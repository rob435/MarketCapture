"""Bybit v5 public linear perpetuals: stream names, message shapes, REST tables and lanes.

Streams (one topic per symbol per feed): `orderbook.<levels>.<SYMBOL>` for
levels 1, 50, 200, 500, 1000; `publicTrade.<SYMBOL>`; `tickers.<SYMBOL>`;
`allLiquidation.<SYMBOL>`; `kline.<interval>.<SYMBOL>`. The ticker already
carries open interest, so an `open_interest` poll is refused here. One
connection's subscription list is capped by the venue at 21,000 characters and
sets no topic count; 150 topics of ~22 characters stays well inside.

Two feeds the venue serves over REST only ride a side lane (`RestLane`), one
thread that walks the universe once an hour a few minutes past the boundary
the settlements fall on: `funding` reads `/v5/market/funding/history`, the
settled payments, and `account_ratio` reads `/v5/market/account-ratio`, the
long/short account ratio in five-minute buckets. Both dedupe on the venue's
own stamps, so a row is written once however often it is fetched.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from market_tape.config import ConfigError, Feed
from market_tape.jsonfast import loads as fast_loads
from market_tape.schema import (
    account_ratio_row,
    funding_row,
    kline_row,
    liquidation_row,
    ticker_row,
    trade_row,
    typed_book_row,
)
from market_tape.venues import Emit

PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"
PUBLIC_REST = "https://api.bybit.com"
BOOK_LEVELS = (1, 50, 200, 500, 1000)
#: Bybit lists stocks, ETFs and commodities as `LinearPerpetual` in the same
#: category as crypto and tells them apart by `symbolType`: "" is its ordinary
#: crypto product, "innovation" its innovation zone, and "stock", "ETF",
#: "commodity" are outside the crypto domain the tape records.
CRYPTO_SYMBOL_TYPES = ("", "innovation")
KLINE_INTERVALS = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W", "1M": "M",
}
TICKER_FIELDS = {
    "lastPrice": "last_price",
    "markPrice": "mark_price",
    "indexPrice": "index_price",
    "openInterest": "open_interest",
    "openInterestValue": "open_interest_value",
    "fundingRate": "funding_rate",
    "nextFundingTime": "next_funding_time_ms",
    "bid1Price": "bid_price",
    "bid1Size": "bid_size",
    "ask1Price": "ask_price",
    "ask1Size": "ask_size",
    "turnover24h": "turnover_24h",
    "volume24h": "volume_24h",
    "price24hPcnt": "price_change_24h_pct",
    "prevPrice1h": "prev_price_1h",
    "prevPrice24h": "prev_price_24h",
    "highPrice24h": "high_price_24h",
    "lowPrice24h": "low_price_24h",
}
#: The feeds the REST lane serves: no stream carries them.
LANE_FEEDS = ("funding", "account_ratio")
#: The account-ratio bucket the lane asks for; the venue offers 5min upward.
ACCOUNT_RATIO_PERIOD = "5min"
#: Buckets one hourly pass reads: an hour of five-minute buckets, plus one so a
#: pass that drifts late still overlaps the last one it saw.
ACCOUNT_RATIO_LIMIT = 13
#: Settlements one hourly pass reads: the newest, plus one for a symbol that
#: settles hourly and a pass that ran late.
FUNDING_HISTORY_LIMIT = 2
#: The lane runs once per interval, `LANE_OFFSET_SECONDS` past the boundary:
#: settlements fall on the hour, and the venue's history lists them shortly
#: after.
LANE_INTERVAL_SECONDS = 3_600.0
LANE_OFFSET_SECONDS = 180.0
#: Public REST is limited per IP; five a second walks 520 symbols twice in
#: under four minutes and stays an order of magnitude under the limit.
LANE_REQUESTS_PER_SECOND = 5.0


def fetch_public_json(url: str, timeout: float = 20.0) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": "market-tape"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise RuntimeError(f"venue refused {url}: {str(payload)[:200]}")
    return payload


def fetch_instruments(rest_base: str, category: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = ""
    for _ in range(20):
        params = {"category": category, "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        payload = fetch_public_json(f"{rest_base}/v5/market/instruments-info?{urllib.parse.urlencode(params)}")
        result = payload.get("result") or {}
        page = result.get("list") or []
        rows.extend(row for row in page if isinstance(row, dict))
        cursor = str(result.get("nextPageCursor") or "")
        if not cursor:
            break
    return rows


def fetch_tickers(rest_base: str, category: str) -> list[dict[str, Any]]:
    payload = fetch_public_json(f"{rest_base}/v5/market/tickers?category={category}")
    result = payload.get("result") or {}
    return [row for row in (result.get("list") or []) if isinstance(row, dict)]


def fetch_funding_history(rest_base: str, category: str, symbol: str, limit: int) -> list[dict[str, Any]]:
    """The venue's settled funding payments for one symbol, newest first:
    `{symbol, fundingRate, fundingRateTimestamp}` rows."""

    params = urllib.parse.urlencode({"category": category, "symbol": symbol, "limit": str(limit)})
    payload = fetch_public_json(f"{rest_base}/v5/market/funding/history?{params}")
    result = payload.get("result") or {}
    return [row for row in (result.get("list") or []) if isinstance(row, dict)]


def fetch_account_ratio(rest_base: str, category: str, symbol: str, period: str, limit: int) -> list[dict[str, Any]]:
    """The venue's long/short account ratio for one symbol, newest first:
    `{symbol, buyRatio, sellRatio, timestamp}` rows, one per `period` bucket."""

    params = urllib.parse.urlencode({"category": category, "symbol": symbol, "period": period, "limit": str(limit)})
    payload = fetch_public_json(f"{rest_base}/v5/market/account-ratio?{params}")
    result = payload.get("result") or {}
    return [row for row in (result.get("list") or []) if isinstance(row, dict)]


def funding_rows(
    venue: str, symbol: str, listed: Iterable[Mapping[str, Any]], received_ns: int, *, after_ms: int
) -> list[dict[str, Any]]:
    """Tape rows for the settlements stamped after `after_ms`, oldest first."""

    rows: list[dict[str, Any]] = []
    for entry in listed:
        try:
            stamp = int(entry.get("fundingRateTimestamp") or 0)
            rate = float(entry.get("fundingRate"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if stamp <= after_ms:
            continue
        rows.append(
            funding_row(
                venue=venue, symbol=symbol, local_receive_ts_ns=received_ns, funding_time_ms=stamp, funding_rate=rate
            )
        )
    rows.sort(key=lambda row: row["funding_time_ms"])
    return rows


def account_ratio_rows(
    venue: str, symbol: str, period: str, listed: Iterable[Mapping[str, Any]], received_ns: int, *, after_ms: int
) -> list[dict[str, Any]]:
    """Tape rows for the buckets stamped after `after_ms`, oldest first."""

    rows: list[dict[str, Any]] = []
    for entry in listed:
        try:
            stamp = int(entry.get("timestamp") or 0)
            buy = float(entry.get("buyRatio"))  # type: ignore[arg-type]
            sell = float(entry.get("sellRatio"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if stamp <= after_ms:
            continue
        rows.append(
            account_ratio_row(
                venue=venue,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                period=period,
                ts_ms=stamp,
                buy_ratio=buy,
                sell_ratio=sell,
            )
        )
    rows.sort(key=lambda row: row["ts_ms"])
    return rows


@dataclass
class RestLane:
    """One thread polling the venue's REST history for the feeds no stream carries.

    Once an hour, `offset_seconds` past the boundary, it asks the venue for
    each symbol's settled funding and its long/short account ratio and hands
    every row it has not seen before to the writer through `emit`. The first
    pass takes the newest settlement and the last hour of ratio buckets; every
    later pass takes what is newer than the last stamp it saw. Requests are
    paced so a whole universe is one slow walk rather than a burst; a symbol
    the venue refuses is counted, logged once per pass, and left for the next
    pass; nothing here can stop the tape.
    """

    rest_url: str
    market: str
    emit: Emit
    stop: threading.Event
    funding_symbols: tuple[str, ...] = ()
    ratio_symbols: tuple[str, ...] = ()
    venue: str = "bybit"
    period: str = ACCOUNT_RATIO_PERIOD
    fetch_funding: Callable[[str, str, str, int], list[dict[str, Any]]] = fetch_funding_history
    fetch_ratio: Callable[[str, str, str, str, int], list[dict[str, Any]]] = fetch_account_ratio
    clock_ns: Callable[[], int] = time.time_ns
    sleep: Callable[[float], None] = time.sleep
    requests_per_second: float = LANE_REQUESTS_PER_SECOND
    interval_seconds: float = LANE_INTERVAL_SECONDS
    offset_seconds: float = LANE_OFFSET_SECONDS
    last_funding_ms: dict[str, int] = field(default_factory=dict)
    last_ratio_ms: dict[str, int] = field(default_factory=dict)
    passes: int = 0

    def thread(self) -> threading.Thread:
        return threading.Thread(target=self.run, name="tape-lane-bybit-rest", daemon=True)

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                self.pass_once()
            except Exception:  # the lane outlives anything one pass meets
                logging.exception("bybit rest lane pass failed")
            self.stop.wait(self.seconds_until_next(self.clock_ns() / 1e9))

    def seconds_until_next(self, now_s: float) -> float:
        """Seconds to the next `offset_seconds` past an interval boundary."""

        boundary = (now_s // self.interval_seconds) * self.interval_seconds + self.offset_seconds
        if boundary <= now_s:
            boundary += self.interval_seconds
        return boundary - now_s

    def pass_once(self) -> dict[str, int]:
        """One walk of both universes; returns what it emitted and what failed."""

        counts = {"funding": 0, "account_ratio": 0, "failures": 0}
        first_error: str | None = None
        pause = 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0.0
        for symbol in self.funding_symbols:
            if self.stop.is_set():
                return counts
            try:
                listed = self.fetch_funding(self.rest_url, self.market, symbol, FUNDING_HISTORY_LIMIT)
            except Exception as exc:  # one refused symbol is the next pass's
                counts["failures"] += 1
                first_error = first_error or f"{symbol}: {exc}"
            else:
                # Stamped as the answer arrives: a stamp taken before the fetch
                # sat behind every stream frame the fetch took.
                received_ns = self.clock_ns()
                seen = self.last_funding_ms.get(symbol)
                rows = funding_rows(self.venue, symbol, listed, received_ns, after_ms=-1 if seen is None else seen)
                if seen is None and rows:
                    rows = rows[-1:]
                for row in rows:
                    self.emit(row)
                    self.last_funding_ms[symbol] = int(row["funding_time_ms"])
                counts["funding"] += len(rows)
            if pause:
                self.sleep(pause)
        for symbol in self.ratio_symbols:
            if self.stop.is_set():
                return counts
            try:
                listed = self.fetch_ratio(self.rest_url, self.market, symbol, self.period, ACCOUNT_RATIO_LIMIT)
            except Exception as exc:  # one refused symbol is the next pass's
                counts["failures"] += 1
                first_error = first_error or f"{symbol}: {exc}"
            else:
                received_ns = self.clock_ns()
                seen = self.last_ratio_ms.get(symbol)
                rows = account_ratio_rows(
                    self.venue, symbol, self.period, listed, received_ns, after_ms=-1 if seen is None else seen
                )
                for row in rows:
                    self.emit(row)
                    self.last_ratio_ms[symbol] = int(row["ts_ms"])
                counts["account_ratio"] += len(rows)
            if pause:
                self.sleep(pause)
        self.passes += 1
        if counts["failures"]:
            logging.warning(
                "bybit rest lane: %d of %d symbols refused this pass, first %s",
                counts["failures"],
                len(self.funding_symbols) + len(self.ratio_symbols),
                first_error,
            )
        logging.info(
            "bybit rest lane pass %d: %d funding settlements, %d account-ratio buckets",
            self.passes,
            counts["funding"],
            counts["account_ratio"],
        )
        return counts


@dataclass(slots=True)
class SequenceState:
    update_id: int = 0
    cross_sequence: int = 0
    healthy: bool = False


class BybitAdapter:
    name = "bybit"
    #: The venue caps a connection's subscription list in characters, not topics.
    max_topics_per_connection: int | None = None

    def __init__(self, *, market: str = "linear", ws_url: str | None = None, rest_url: str | None = None) -> None:
        if market != "linear":
            raise ConfigError(f"the Bybit recorder records the linear market, not {market!r}")
        self.market = market
        self.ws_url = ws_url or PUBLIC_LINEAR_WS
        self.rest_url = rest_url or PUBLIC_REST
        self.sequences: dict[str, SequenceState] = {}
        #: Each book topic's depth, read from its name once.
        self.depths: dict[str, int] = {}

    # ---------------------------------------------------------------- feeds

    def validate_feeds(self, feeds: Iterable[Feed]) -> None:
        for feed in feeds:
            if feed.name in LANE_FEEDS:
                continue
            if feed.name == "book" and feed.levels not in BOOK_LEVELS:
                raise ConfigError(f"Bybit offers book levels {BOOK_LEVELS}, not {feed.text}")
            if feed.name == "kline" and feed.arg not in KLINE_INTERVALS:
                raise ConfigError(f"Bybit kline intervals are {sorted(KLINE_INTERVALS)}, not {feed.text}")
            if feed.name == "open_interest":
                raise ConfigError("Bybit pushes open interest on the ticker; drop the open_interest feed")

    def topics(self, symbol: str, feeds: Iterable[Feed]) -> list[str]:
        result = []
        for feed in feeds:
            if feed.name == "book":
                result.append(f"orderbook.{feed.levels}.{symbol}")
            elif feed.name == "trades":
                result.append(f"publicTrade.{symbol}")
            elif feed.name == "ticker":
                result.append(f"tickers.{symbol}")
            elif feed.name == "liquidations":
                result.append(f"allLiquidation.{symbol}")
            elif feed.name == "kline":
                result.append(f"kline.{KLINE_INTERVALS[str(feed.arg)]}.{symbol}")
        return result

    def connection_url(self, topics: list[str]) -> str:
        return self.ws_url

    def connection_group(self, topic: str) -> str:
        return ""

    def anchored_topics(self, topics: Iterable[str]) -> list[str]:
        return [topic for topic in topics if topic.startswith(("orderbook.", "tickers."))]

    def subscribe_messages(self, topics: list[str]) -> list[str]:
        return [json.dumps({"op": "subscribe", "args": topics[start : start + 10]}) for start in range(0, len(topics), 10)]

    def ping_message(self) -> str:
        return json.dumps({"op": "ping"})

    def add_messages(self, topics: list[str]) -> list[str]:
        return self.subscribe_messages(topics)

    def remove_messages(self, topics: list[str]) -> list[str]:
        return [json.dumps({"op": "unsubscribe", "args": topics[start : start + 10]}) for start in range(0, len(topics), 10)]

    def start_lanes(self, feeds_by_symbol: Mapping[str, tuple[Feed, ...]], emit: Emit, stop: threading.Event) -> list[threading.Thread]:
        funding = tuple(sorted(symbol for symbol, feeds in feeds_by_symbol.items() if Feed("funding") in feeds))
        ratio = tuple(sorted(symbol for symbol, feeds in feeds_by_symbol.items() if Feed("account_ratio") in feeds))
        if not funding and not ratio:
            return []
        lane = RestLane(
            rest_url=self.rest_url,
            market=self.market,
            emit=emit,
            stop=stop,
            funding_symbols=funding,
            ratio_symbols=ratio,
            venue=self.name,
        )
        thread = lane.thread()
        thread.start()
        return [thread]

    # --------------------------------------------------------------- tables

    def fetch_tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "instruments": fetch_instruments(self.rest_url, self.market),
            "tickers": fetch_tickers(self.rest_url, self.market),
        }

    def listed_symbols(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> list[str]:
        symbols = set()
        for row in self._trading_perpetuals(instruments, quote=quote):
            if str(row.get("symbolType") or "") not in CRYPTO_SYMBOL_TYPES:
                continue
            symbol = str(row.get("symbol") or "").upper()
            if symbol and symbol.isalnum():
                symbols.add(symbol)
        return sorted(symbols)

    def excluded_listed(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._trading_perpetuals(instruments, quote=quote):
            label = str(row.get("symbolType") or "")
            if label not in CRYPTO_SYMBOL_TYPES:
                counts[label] = counts.get(label, 0) + 1
        return dict(sorted(counts.items()))

    @staticmethod
    def _trading_perpetuals(instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> Iterable[Mapping[str, Any]]:
        for row in instruments:
            if not isinstance(row, Mapping):
                continue
            if str(row.get("status")) != "Trading":
                continue
            if str(row.get("contractType")) != "LinearPerpetual":
                continue
            if quote is not None and (
                str(row.get("quoteCoin")) != quote or str(row.get("settleCoin", quote)) != quote
            ):
                continue
            yield row

    def turnovers(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        result: dict[str, float] = {}
        for row in tickers:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol") or "").upper()
            try:
                turnover = float(row.get("turnover24h") or 0.0)
            except (TypeError, ValueError):
                continue
            if symbol:
                result[symbol] = turnover
        return result

    def funding_rates(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        rates: dict[str, float] = {}
        for row in tickers:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol") or "").upper()
            raw = row.get("fundingRate")
            if not symbol or raw in (None, ""):
                continue
            try:
                rates[symbol] = float(raw)
            except (TypeError, ValueError):
                continue
        return rates

    # ------------------------------------------------------------- messages

    def normalize(self, raw: str | bytes, received_ns: int, received_mono_ns: int = 0) -> list[dict[str, Any]]:
        message = fast_loads(raw)
        if not isinstance(message, (dict, Mapping)):
            return []
        topic = str(message.get("topic") or "")
        if topic.startswith("orderbook."):
            return self._book(message, topic, received_ns, received_mono_ns)
        if topic.startswith("publicTrade."):
            return self._trades(message, received_ns, received_mono_ns)
        if topic.startswith("tickers."):
            return self._ticker(message, received_ns, received_mono_ns)
        if topic.startswith("allLiquidation."):
            return self._liquidations(message, received_ns, received_mono_ns)
        if topic.startswith("kline."):
            return self._klines(message, topic, received_ns, received_mono_ns)
        if message.get("op") == "subscribe":
            self._subscribe_reply(message)
        return []

    def _subscribe_reply(self, message: Mapping[str, Any]) -> None:
        # A live feed's rule for an operation's acknowledgement.
        # A refused subscribe's topics carry no rows until a later subscribe of them is accepted.
        code = message.get("ret_code", message.get("retCode"))
        if message.get("success", code == 0) and code in (None, 0):
            return
        logging.warning(
            "bybit refused a subscription: %s", message.get("ret_msg") or message.get("retMsg") or "no reason given"
        )

    def _book(
        self, message: Mapping[str, Any], topic: str, received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        data = message.get("data")
        if not isinstance(data, (dict, Mapping)):
            return []
        symbol = str(data.get("s") or "").upper()
        if not symbol:
            return []
        depth = self.depths.get(topic)
        if depth is None:
            try:
                depth = int(topic.split(".", 2)[1])
            except (IndexError, ValueError):
                return []
            self.depths[topic] = depth
        update_id = int(data.get("u") or 0)
        cross_sequence = int(data.get("seq") or 0)
        message_type = str(message.get("type") or "").lower()
        # Only "delta" is a delta: the venue's other book types, and a frame
        # with none, carry a whole book. `u == 1` is its service restart.
        snapshot = message_type != "delta" or update_id == 1
        previous = self.sequences.get(topic)
        if previous is None:
            previous = self.sequences[topic] = SequenceState()
        # One topic's deltas are continuous only at `u + 1`; `seq` is the
        # venue's cross-topic order, never a continuity rule. The live feed
        # resyncs on exactly this.
        gap = not snapshot and (
            not previous.healthy or (previous.update_id > 0 and update_id != previous.update_id + 1)
        )
        row = typed_book_row(
            self.name,
            symbol,
            snapshot,
            depth,
            received_ns,
            received_mono_ns,
            int(message.get("ts") or 0) * 1_000_000,
            int(message.get("cts") or 0) * 1_000_000,
            data.get("b") or [],
            data.get("a") or [],
            update_id,
            previous.update_id,
            0,  # first_update_id: Bybit publishes none
            cross_sequence,
            previous.cross_sequence,
            update_id == 1,
            gap,
        )
        previous.update_id = update_id
        previous.cross_sequence = cross_sequence
        previous.healthy = snapshot or not gap
        return [row]

    def _trades(self, message: Mapping[str, Any], received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        rows = message.get("data")
        if not isinstance(rows, list):
            return []
        # The message's own send stamp; every print it carries shares it.
        sent_ns = int(message.get("ts") or 0) * 1_000_000
        output = []
        for trade in rows:
            if not isinstance(trade, (dict, Mapping)):
                continue
            symbol = str(trade.get("s") or "").upper()
            side = str(trade.get("S") or "")
            if not symbol or side not in {"Buy", "Sell"}:
                continue
            output.append(
                trade_row(
                    venue=self.name,
                    symbol=symbol,
                    local_receive_ts_ns=received_ns,
                    local_receive_mono_ns=received_mono_ns,
                    exchange_system_ts_ns=sent_ns,
                    exchange_ts_ns=int(trade.get("T") or message.get("ts") or 0) * 1_000_000,
                    trade_id=str(trade.get("i") or ""),
                    price=float(trade.get("p") or 0.0),
                    qty=float(trade.get("v") or 0.0),
                    side=side,
                )
            )
        return output

    def _ticker(self, message: Mapping[str, Any], received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        data = message.get("data")
        if not isinstance(data, (dict, Mapping)):
            return []
        symbol = str(data.get("symbol") or "").upper()
        if not symbol:
            return []
        values: dict[str, float | int] = {}
        for venue_name, stored_name in TICKER_FIELDS.items():
            raw = data.get(venue_name)
            if raw in (None, ""):
                continue
            try:
                values[stored_name] = int(raw) if venue_name == "nextFundingTime" else float(raw)
            except (TypeError, ValueError):
                continue
        return [
            ticker_row(
                venue=self.name,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                local_receive_mono_ns=received_mono_ns,
                exchange_system_ts_ns=int(message.get("ts") or 0) * 1_000_000,
                message_type=str(message.get("type") or "").lower(),
                cross_sequence=int(message.get("cs") or 0),
                values=values,
            )
        ]

    def _liquidations(
        self, message: Mapping[str, Any], received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        data = message.get("data")
        rows = data if isinstance(data, list) else [data]
        output = []
        for liquidation in rows:
            if not isinstance(liquidation, (dict, Mapping)):
                continue
            symbol = str(liquidation.get("s") or "").upper()
            position_side = str(liquidation.get("S") or "")
            if not symbol or position_side not in {"Buy", "Sell"}:
                continue
            output.append(
                liquidation_row(
                    venue=self.name,
                    symbol=symbol,
                    local_receive_ts_ns=received_ns,
                    local_receive_mono_ns=received_mono_ns,
                    exchange_system_ts_ns=int(message.get("ts") or 0) * 1_000_000,
                    exchange_ts_ns=int(liquidation.get("T") or 0) * 1_000_000,
                    position_side=position_side,
                    qty=float(liquidation.get("v") or 0.0),
                    bankruptcy_price=float(liquidation.get("p") or 0.0),
                )
            )
        return output

    def _klines(
        self, message: Mapping[str, Any], topic: str, received_ns: int, received_mono_ns: int
    ) -> list[dict[str, Any]]:
        parts = topic.split(".", 2)
        if len(parts) != 3:
            return []
        venue_interval, symbol = parts[1], parts[2].upper()
        interval = next((name for name, code in KLINE_INTERVALS.items() if code == venue_interval), venue_interval)
        rows = message.get("data")
        if not isinstance(rows, list):
            return []
        output = []
        for candle in rows:
            if not isinstance(candle, (dict, Mapping)):
                continue
            try:
                output.append(
                    kline_row(
                        venue=self.name,
                        symbol=symbol,
                        interval=interval,
                        local_receive_ts_ns=received_ns,
                        local_receive_mono_ns=received_mono_ns,
                        exchange_system_ts_ns=int(message.get("ts") or 0) * 1_000_000,
                        start_ms=int(candle.get("start") or 0),
                        end_ms=int(candle.get("end") or 0),
                        open=float(candle.get("open") or 0.0),
                        high=float(candle.get("high") or 0.0),
                        low=float(candle.get("low") or 0.0),
                        close=float(candle.get("close") or 0.0),
                        volume=float(candle.get("volume") or 0.0),
                        turnover=float(candle.get("turnover") or 0.0),
                        confirmed=bool(candle.get("confirm", False)),
                    )
                )
            except (TypeError, ValueError):
                continue
        return output
