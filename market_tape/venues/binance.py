"""Binance USD-M perpetual futures: stream names, message shapes, REST tables and the open-interest lane.

The venue routes streams by URL path, and a connection receives only its own
path's streams, silently dropping the rest (measured 2026-09-25):

| Path | Carries | Feeds here |
| --- | --- | --- |
| `/stream` (path-less, served as `/public`) | `<pair>@bookTicker`, `<pair>@depth<N>@100ms`, `<pair>@trade` | `book:1`, `book:5/10/20`, `trades` |
| `/market/stream` | `<pair>@aggTrade`, `@markPrice@1s`, `@ticker`, `@kline_<i>`, `!forceOrder@arr` | `ticker`, `kline:<i>`, `liquidations` |

`wss://fstream.binance.com/stream` is the URL a live top-of-book reader of the
venue takes, so the tape's top of book is that reader's stream, and `trades`
is `@trade` on the same socket: every fill on the book (`BOOK_TRADE_TYPES`),
rather than `aggTrade`, which only `/market` carries.
`connection_group` names the path and a shard carries one. Streams are asked
for with `SUBSCRIBE` frames on the open socket, 100 streams a frame, so a
symbol listed mid-run joins a live connection.

Venue limits: 1024 streams a connection (`MAX_STREAMS`, which
`topics_per_connection` may not exceed); 10 incoming messages a second, pings
and pongs included (the recorder spaces its frames
`record.LIVE_MESSAGE_SPACING_SECONDS` apart); a ping every 3 minutes, which the
reader answers; a close at every connection's 24-hour mark, after which the
shard reconnects.

Every book row is a whole book (`bookTicker` the top level, `depth<N>` the top
N) and every ticker frame its stream's whole field set, so nothing here needs
a subscription to anchor it: `anchored_topics` is empty and the hourly
re-anchor sends nothing. The 1000-level diff book is not recorded: its deltas
mean something only beside a REST snapshot per symbol per subscribe, which the
venue's request weight (20 of 2,400 a minute) rations to two a second across
the universe. Open interest is a REST poll: no stream pushes it.
"""

from __future__ import annotations

import itertools
import json
import logging
import threading
import time
import urllib.request
from typing import Any, Iterable, Mapping

from market_tape.config import ConfigError, Feed
from market_tape.jsonfast import loads as fast_loads
from market_tape.schema import TICKER_INT_FIELDS, kline_row, liquidation_row, ticker_row, trade_row, typed_book_row
from market_tape.venues import Emit

PUBLIC_USDM_WS = "wss://fstream.binance.com"
PUBLIC_REST = "https://fapi.binance.com"
#: The path of each connection group under `ws_url`.
GROUP_PATHS = {"public": "/stream", "market": "/market/stream"}
#: `book:1` is `bookTicker`; 5, 10 and 20 are the partial-book streams.
BOOK_LEVELS = (1, 5, 10, 20)
LIQUIDATION_STREAM = "!forceOrder@arr"
KLINE_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M")
#: The streams the `/stream` path carries, by suffix; every other stream rides `/market`.
PUBLIC_SUFFIXES = ("bookTicker", "trade")
#: Streams one connection may carry, and streams one `SUBSCRIBE` frame names.
MAX_STREAMS = 1024
STREAMS_PER_FRAME = 100
#: `@trade` types (`X`) that are a taker crossing the book: `MARKET`, and
#: `RPI`, a fill against a retail-price-improvement maker the API book does
#: not show. Every other type is dropped and the first row of each logged:
#: `NA` (measured 2026-09-25) arrives at zero price and size.
BOOK_TRADE_TYPES = frozenset({"MARKET", "RPI"})
OPEN_INTEREST_PAUSE_SECONDS = 0.05

MARK_PRICE_FIELDS = {"p": "mark_price", "i": "index_price", "r": "funding_rate", "T": "next_funding_time_ms"}
DAY_TICKER_FIELDS = {"c": "last_price", "q": "turnover_24h", "v": "volume_24h", "P": "price_change_24h_pct"}
#: Binance states the 24h change in percent; the contract stores a fraction.
PERCENT_FIELDS = frozenset({"price_change_24h_pct"})
PREMIUM_TABLE_FIELDS = ("markPrice", "indexPrice", "lastFundingRate", "nextFundingTime")
DAY_TABLE_FIELDS = ("lastPrice", "quoteVolume", "volume")
BOOK_TABLE_FIELDS = ("bidPrice", "bidQty", "askPrice", "askQty")


def fetch_public_json(url: str, timeout: float = 20.0) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "market-tape"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    if isinstance(payload, Mapping) and "code" in payload and "msg" in payload:
        raise RuntimeError(f"venue refused {url}: {str(payload)[:200]}")
    return payload


def _table(payload: Any) -> list[dict[str, Any]]:
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def fetch_instruments(rest_base: str) -> list[dict[str, Any]]:
    payload = fetch_public_json(f"{rest_base}/fapi/v1/exchangeInfo")
    return _table(payload.get("symbols")) if isinstance(payload, Mapping) else []


def fetch_tickers(rest_base: str) -> list[dict[str, Any]]:
    """One row per symbol, carrying the fields three whole-market tables hold."""

    merged: dict[str, dict[str, Any]] = {}
    for path, fields in (
        ("/fapi/v1/premiumIndex", PREMIUM_TABLE_FIELDS),
        ("/fapi/v1/ticker/24hr", DAY_TABLE_FIELDS),
        ("/fapi/v1/ticker/bookTicker", BOOK_TABLE_FIELDS),
    ):
        for row in _table(fetch_public_json(f"{rest_base}{path}")):
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            target = merged.setdefault(symbol, {"symbol": symbol})
            for name in fields:
                if row.get(name) is not None:
                    target[name] = row[name]
    return [merged[symbol] for symbol in sorted(merged)]


def fetch_open_interest(rest_base: str, symbol: str) -> dict[str, Any]:
    payload = fetch_public_json(f"{rest_base}/fapi/v1/openInterest?symbol={symbol}")
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"open interest for {symbol} is not a table: {str(payload)[:200]}")
    return dict(payload)


def _ns(value: Any) -> int:
    try:
        return int(value or 0) * 1_000_000
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _level(price: Any, size: Any) -> list[list[str]]:
    if price in (None, "") or size in (None, ""):
        return []
    return [[str(price), str(size)]]


def _ticker_values(data: Mapping[str, Any], fields: Mapping[str, str]) -> dict[str, float | int]:
    values: dict[str, float | int] = {}
    for venue_name, stored_name in fields.items():
        number = _float(data.get(venue_name))
        if number is None:
            continue
        values[stored_name] = int(number) if stored_name in TICKER_INT_FIELDS else number
    return values


class BinanceAdapter:
    name = "binance"
    max_topics_per_connection: int | None = MAX_STREAMS

    def __init__(self, *, market: str = "usdm", ws_url: str | None = None, rest_url: str | None = None) -> None:
        if market != "usdm":
            raise ConfigError(f"the Binance recorder records the usdm market, not {market!r}")
        self.market = market
        self.ws_url = (ws_url or PUBLIC_USDM_WS).rstrip("/")
        self.rest_url = (rest_url or PUBLIC_REST).rstrip("/")
        # Written by the writer thread in `normalize`, read by the lane; a
        # float store is one bytecode, so the lane reads a whole value.
        self.marks: dict[str, float] = {}
        self.request_ids = itertools.count(1)
        #: `@trade` types dropped since start, each logged once.
        self.dropped_trade_types: set[str] = set()

    # ---------------------------------------------------------------- feeds

    def validate_feeds(self, feeds: Iterable[Feed]) -> None:
        for feed in feeds:
            if feed.name == "book" and feed.levels not in BOOK_LEVELS:
                raise ConfigError(
                    f"Binance records book levels {BOOK_LEVELS} (bookTicker and the partial books), not {feed.text}"
                )
            if feed.name == "kline" and feed.arg not in KLINE_INTERVALS:
                raise ConfigError(f"Binance kline intervals are {list(KLINE_INTERVALS)}, not {feed.text}")
            if feed.name in ("funding", "account_ratio"):
                raise ConfigError(f"the Binance recorder has no {feed.name} lane; the ticker carries the funding rate")

    def topics(self, symbol: str, feeds: Iterable[Feed]) -> list[str]:
        lower = symbol.lower()
        result = []
        for feed in feeds:
            if feed.name == "book":
                levels = feed.levels
                result.append(f"{lower}@bookTicker" if levels == 1 else f"{lower}@depth{levels}@100ms")
            elif feed.name == "trades":
                result.append(f"{lower}@trade")
            elif feed.name == "ticker":
                result.append(f"{lower}@markPrice@1s")
                result.append(f"{lower}@ticker")
            elif feed.name == "liquidations":
                # One stream carries every symbol's liquidations; the recorder
                # claims a repeated topic once.
                result.append(LIQUIDATION_STREAM)
            elif feed.name == "kline":
                result.append(f"{lower}@kline_{feed.arg}")
        return result

    def connection_group(self, topic: str) -> str:
        suffix = topic.partition("@")[2]
        if suffix in PUBLIC_SUFFIXES or suffix.startswith("depth"):
            return "public"
        return "market"

    def anchored_topics(self, topics: Iterable[str]) -> list[str]:
        return []

    def connection_url(self, topics: list[str]) -> str:
        groups = {self.connection_group(topic) for topic in topics} or {"public"}
        if len(groups) != 1:
            raise ValueError(f"one Binance connection carries one path, got {sorted(groups)}")
        return f"{self.ws_url}{GROUP_PATHS[groups.pop()]}"

    def subscribe_messages(self, topics: list[str]) -> list[str]:
        return self._requests("SUBSCRIBE", topics)

    def add_messages(self, topics: list[str]) -> list[str]:
        return self._requests("SUBSCRIBE", topics)

    def remove_messages(self, topics: list[str]) -> list[str]:
        return self._requests("UNSUBSCRIBE", topics)

    def _requests(self, method: str, topics: list[str]) -> list[str]:
        return [
            json.dumps({"method": method, "params": topics[start : start + STREAMS_PER_FRAME], "id": next(self.request_ids)})
            for start in range(0, len(topics), STREAMS_PER_FRAME)
        ]

    # ----------------------------------------------------------------- lanes

    def start_lanes(
        self, feeds_by_symbol: Mapping[str, tuple[Feed, ...]], emit: Emit, stop: threading.Event
    ) -> list[threading.Thread]:
        by_interval: dict[float, list[str]] = {}
        for symbol, feeds in feeds_by_symbol.items():
            for feed in feeds:
                if feed.name == "open_interest":
                    by_interval.setdefault(feed.seconds, []).append(symbol)
        threads = []
        for seconds, symbols in sorted(by_interval.items()):
            thread = threading.Thread(
                target=self._open_interest_lane,
                args=(sorted(symbols), seconds, emit, stop),
                name=f"tape-binance-oi-{seconds:g}s",
                daemon=True,
            )
            thread.start()
            threads.append(thread)
        return threads

    def _open_interest_lane(self, symbols: list[str], seconds: float, emit: Emit, stop: threading.Event) -> None:
        while not stop.is_set():
            started = time.monotonic()
            for symbol in symbols:
                if stop.is_set():
                    return
                try:
                    emit(self.open_interest_row(symbol))
                except Exception as exc:  # noqa: BLE001 - one symbol's failed poll must not end the lane
                    logging.warning("open interest poll for %s failed: %s", symbol, exc)
                if stop.wait(OPEN_INTEREST_PAUSE_SECONDS):
                    return
            stop.wait(max(0.0, seconds - (time.monotonic() - started)))

    def open_interest_row(self, symbol: str) -> dict[str, Any]:
        payload = fetch_open_interest(self.rest_url, symbol)
        received_ns = time.time_ns()
        count = _float(payload.get("openInterest")) or 0.0
        values: dict[str, float | int] = {"open_interest": count}
        mark = self.marks.get(symbol)
        if mark is not None:
            values["open_interest_value"] = count * mark
        return ticker_row(
            venue=self.name,
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=_ns(payload.get("time")),
            message_type="poll",
            values=values,
        )

    # --------------------------------------------------------------- tables

    def fetch_tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "instruments": fetch_instruments(self.rest_url),
            "tickers": fetch_tickers(self.rest_url),
        }

    def listed_symbols(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> list[str]:
        symbols = set()
        for row in self._trading_rows(instruments, quote=quote):
            if str(row.get("contractType")) != "PERPETUAL":
                continue
            symbol = str(row.get("symbol") or "").upper()
            if symbol and symbol.isalnum():
                symbols.add(symbol)
        return sorted(symbols)

    def excluded_listed(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> dict[str, int]:
        # Binance files its stocks, ETFs and commodities under their own
        # contract type, `TRADIFI_PERPETUAL`, so the perpetual filter is the
        # domain filter; dated futures are counted here too.
        counts: dict[str, int] = {}
        for row in self._trading_rows(instruments, quote=quote):
            label = str(row.get("contractType"))
            if label != "PERPETUAL":
                counts[label] = counts.get(label, 0) + 1
        return dict(sorted(counts.items()))

    @staticmethod
    def _trading_rows(instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> Iterable[Mapping[str, Any]]:
        for row in instruments:
            if not isinstance(row, Mapping):
                continue
            if str(row.get("status")) != "TRADING":
                continue
            if quote is not None and (
                str(row.get("quoteAsset")) != quote or str(row.get("marginAsset", quote)) != quote
            ):
                continue
            yield row

    def turnovers(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        result: dict[str, float] = {}
        for row in tickers:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol") or "").upper()
            turnover = _float(row.get("quoteVolume"))
            if symbol and turnover is not None:
                result[symbol] = turnover
        return result

    def funding_rates(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        rates: dict[str, float] = {}
        for row in tickers:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol") or "").upper()
            rate = _float(row.get("lastFundingRate"))
            if symbol and rate is not None:
                rates[symbol] = rate
        return rates

    # ------------------------------------------------------------- messages

    def normalize(self, raw: str | bytes, received_ns: int, received_mono_ns: int = 0) -> list[dict[str, Any]]:
        message = fast_loads(raw)
        if not isinstance(message, dict):
            return []
        stream = message.get("stream")
        if not isinstance(stream, str):
            if "id" in message:
                self._subscribe_reply(message)
            return []
        data = message.get("data")
        if stream == LIQUIDATION_STREAM:
            return self._liquidations(data, received_ns, received_mono_ns)
        if not isinstance(data, dict):
            return []
        suffix = stream.partition("@")[2]
        if suffix == "bookTicker":
            return self._top_of_book(data, received_ns, received_mono_ns)
        if suffix == "trade":
            return self._trade(data, received_ns, received_mono_ns)
        if suffix.startswith("depth"):
            return self._book(data, suffix, received_ns, received_mono_ns)
        if suffix.startswith("markPrice"):
            return self._ticker(data, MARK_PRICE_FIELDS, received_ns, received_mono_ns)
        if suffix == "ticker":
            return self._ticker(data, DAY_TICKER_FIELDS, received_ns, received_mono_ns)
        if suffix.startswith("kline_"):
            return self._kline(data, suffix[len("kline_") :], received_ns, received_mono_ns)
        return []

    def _subscribe_reply(self, message: Mapping[str, Any]) -> None:
        # `{"result": null, "id": N}` is an accepted request; a refusal carries
        # `error` (or `code` and `msg`), and its streams carry no rows until a
        # later request of them is accepted.
        error = message.get("error")
        if error is None and "code" not in message:
            return
        detail = error if error is not None else {"code": message.get("code"), "msg": message.get("msg")}
        logging.warning("binance refused subscription request %s: %s", message.get("id"), detail)

    def _top_of_book(self, data: Mapping[str, Any], received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        if not symbol:
            return []
        return [
            typed_book_row(
                self.name,
                symbol,
                True,  # snapshot
                1,  # depth
                received_ns,
                received_mono_ns,
                _ns(data.get("E")),
                _ns(data.get("T")),
                _level(data.get("b"), data.get("B")),
                _level(data.get("a"), data.get("A")),
                int(data.get("u") or 0),
                0,  # previous_update_id
                0,  # first_update_id
                0,  # cross_sequence
                0,  # previous_cross_sequence
                False,  # restart_snapshot
                False,  # sequence_gap
            )
        ]

    def _book(
        self, data: Mapping[str, Any], suffix: str, received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        levels = suffix.partition("@")[0][len("depth") :]
        if not symbol or not levels.isdigit():
            return []
        return [
            typed_book_row(
                self.name,
                symbol,
                True,  # snapshot
                int(levels),
                received_ns,
                received_mono_ns,
                _ns(data.get("E")),
                _ns(data.get("T")),
                data.get("b") or [],
                data.get("a") or [],
                int(data.get("u") or 0),
                int(data.get("pu") or 0),
                int(data.get("U") or 0),
                0,  # cross_sequence
                0,  # previous_cross_sequence
                False,  # restart_snapshot
                False,  # sequence_gap
            )
        ]

    def _trade(self, data: Mapping[str, Any], received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        price = _float(data.get("p"))
        qty = _float(data.get("q"))
        kind = data.get("X")
        if kind is not None and kind not in BOOK_TRADE_TYPES:
            if kind not in self.dropped_trade_types:
                self.dropped_trade_types.add(str(kind))
                logging.info("binance @trade type %r is not a print on the book; its rows are dropped", kind)
            return []
        if not symbol or price is None or qty is None:
            return []
        return [
            trade_row(
                venue=self.name,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                local_receive_mono_ns=received_mono_ns,
                exchange_system_ts_ns=_ns(data.get("E")),
                exchange_ts_ns=_ns(data.get("T") or data.get("E")),
                trade_id=str(data.get("t") or ""),
                price=price,
                qty=qty,
                # `m` is "the buyer was the maker", so the aggressor sold.
                side="Sell" if data.get("m") else "Buy",
            )
        ]

    def _ticker(
        self, data: Mapping[str, Any], fields: Mapping[str, str], received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        if not symbol:
            return []
        values = _ticker_values(data, fields)
        for name in PERCENT_FIELDS & values.keys():
            values[name] = float(values[name]) / 100.0
        mark = values.get("mark_price")
        if mark is not None:
            self.marks[symbol] = float(mark)
        return [
            ticker_row(
                venue=self.name,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                local_receive_mono_ns=received_mono_ns,
                exchange_system_ts_ns=_ns(data.get("E")),
                message_type="delta",
                values=values,
            )
        ]

    def _liquidations(self, data: Any, received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        events = data if isinstance(data, list) else [data]
        output = []
        for event in events:
            if not isinstance(event, Mapping):
                continue
            order = event.get("o")
            if not isinstance(order, Mapping):
                continue
            symbol = str(order.get("s") or "").upper()
            # The order closes the position, so the position sat the other way.
            position_side = {"SELL": "Buy", "BUY": "Sell"}.get(str(order.get("S") or "").upper(), "")
            qty = _float(order.get("q"))
            price = _float(order.get("p"))
            if not symbol or not position_side or qty is None or price is None:
                continue
            output.append(
                liquidation_row(
                    venue=self.name,
                    symbol=symbol,
                    local_receive_ts_ns=received_ns,
                    local_receive_mono_ns=received_mono_ns,
                    exchange_system_ts_ns=_ns(event.get("E")),
                    exchange_ts_ns=_ns(order.get("T")),
                    position_side=position_side,
                    qty=qty,
                    bankruptcy_price=price,
                )
            )
        return output

    def _kline(
        self, data: Mapping[str, Any], interval: str, received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        candle = data.get("k")
        if not symbol or not isinstance(candle, Mapping):
            return []
        try:
            return [
                kline_row(
                    venue=self.name,
                    symbol=symbol,
                    interval=str(candle.get("i") or interval),
                    local_receive_ts_ns=received_ns,
                    local_receive_mono_ns=received_mono_ns,
                    exchange_system_ts_ns=_ns(data.get("E")),
                    start_ms=int(candle.get("t") or 0),
                    end_ms=int(candle.get("T") or 0),
                    open=float(candle.get("o") or 0.0),
                    high=float(candle.get("h") or 0.0),
                    low=float(candle.get("l") or 0.0),
                    close=float(candle.get("c") or 0.0),
                    volume=float(candle.get("v") or 0.0),
                    turnover=float(candle.get("q") or 0.0),
                    confirmed=bool(candle.get("x", False)),
                )
            ]
        except (TypeError, ValueError):
            return []
