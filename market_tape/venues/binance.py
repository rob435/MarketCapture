"""Binance USD-M perpetual futures: the adapter, its stream names and message shapes, and its lanes.

Everything asked of the REST API (the tables, the open-interest poll, the
`book:1000` snapshot lane and the hourly lane, and the weight and 429
handling all of them share) is `market_tape/venues/binance_rest.py`; its
public names are importable from here too.

The venue routes streams by URL path, and a connection receives only its own
path's streams, silently dropping the rest (measured 2026-09-25):

| Path | Carries | Feeds here |
| --- | --- | --- |
| `/stream` (path-less, served as `/public`) | `<pair>@bookTicker`, `<pair>@depth<N>@100ms`, `<pair>@depth@100ms`, `<pair>@trade` | `book:1`, `book:5/10/20`, `book:1000`, `trades` |
| `/market/stream` | `<pair>@aggTrade`, `@markPrice@1s`, `@ticker`, `@kline_<i>`, `!forceOrder@arr` | `ticker`, `kline:<i>`, `liquidations` |

(The `/stream` routing of `@depth@100ms` is assumed from its siblings; see
"Measured versus assumed" below.)
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
`recorder.shard.LIVE_MESSAGE_SPACING_SECONDS` apart); a ping every 3 minutes, which the
reader answers; a close at every connection's 24-hour mark, after which the
shard reconnects.

`book:1` and `book:5/10/20` rows are whole books (`bookTicker` the top level,
`depth<N>` the top N) and every ticker frame is its stream's whole field set,
so nothing here needs a subscription to anchor it: `anchored_topics` is empty
and the hourly re-anchor sends nothing. Open interest is a REST poll: no
stream pushes it.

The venue's histories no stream carries ride one hourly REST lane, the walk
Bybit's lane runs (`market_tape/venues/lanes.py`): `funding` (settled
payments, `/fapi/v1/fundingRate`), `account_ratio` (all accounts' long/short
split), `top_account_ratio` and `top_position_ratio` (the top traders'
accounts and positions) and `taker_volume` (taker buy and sell volume), the
last four five-minute statistics from `/futures/data/*`: the channels
tardis.dev records for binance-futures beyond the streams. Its stamps live on
the adapter (`lane_state`), so a lane restarted on a universe change writes
nothing twice; its pacing, and the arithmetic that keeps it and the other
lanes under the venue's per-IP limits, are in `binance_rest.py`.

`book:1000` is the full incremental book, recorded as tardis.dev records it:
the diff stream `<pair>@depth@100ms` (`DIFF_DEPTH_SPEED`, on `/stream`) plus
REST snapshots (`GET /fapi/v1/depth?limit=1000`). Each diff is one
`orderbook_delta` row (`update_id` = `u`, `previous_update_id` = `pu`,
`first_update_id` = `U`) flagged `sequence_gap` when it does not chain onto
the last diff seen for its symbol (`pu != last u`) and on the first diff
after a (re)subscribe, which `chain_reset` arranges in tape order. A gap asks
the snapshot lane (`DepthSnapshotLane`) for a snapshot of that symbol; its
`orderbook_snapshot` row (`update_id` = `lastUpdateId`) lands on the tape
after the diffs it does not cover, and `market_tape/book.py` brackets it. The
lane also takes every diff symbol once per UTC hour (with
`connection.reanchor_each_hour`), so an hour's tar replays alone. A
subscription brings no snapshot here, so a gapped diff book is never
re-subscribed: the recorder accounts its gaps and snapshots in the hour's
coverage record (`rest_based_topics`) and leaves the re-basing to the lane.
The snapshot's weight (20 of the venue's 2,400 a minute per IP, shared with
the open-interest poll and the table reads) is what paces it:
`SNAPSHOT_WEIGHT_PER_MINUTE` is 50 snapshots a minute, so a cold start or a
24-hour reconnect of ~550 diff symbols takes ~11 minutes to base them all,
and the cells read `book_gap` until then.

Measured versus assumed: every other stream above was measured against the
venue; the diff book and the hourly lane were not. This sandbox cannot reach
Binance (HTTP 451), so the diff stream's name, every REST path and answer
shape the two lanes read, the `X-MBX-USED-WEIGHT-1M` header, `Retry-After`,
the per-endpoint limits and the `U`/`u`/`pu` rules follow Binance's published
USD-M documentation ("How to manage a local order book correctly", "Market
Data Endpoints") and are verified only against injected fetches and local
fake servers in the tests.
"""

from __future__ import annotations

import itertools
import json
import logging
import threading
import time
from typing import Any, Callable, Iterable, Mapping

from market_tape.config import ConfigError, Feed
from market_tape.jsonfast import loads as fast_loads
from market_tape.schema import TICKER_INT_FIELDS, kline_row, liquidation_row, ticker_row, trade_row, typed_book_row
from market_tape.venues import Emit
from market_tape.venues.binance_rest import (
    DEPTH_SNAPSHOT_WEIGHT,
    DIFF_BOOK_LEVELS,
    FUNDING_HISTORY_LIMIT,
    FUNDING_HISTORY_MAX_LIMIT,
    FUNDING_PAUSE_SECONDS,
    FUNDING_STEP_MS,
    HOUR_NS,
    LANE_FEEDS,
    LANE_INTERVAL_SECONDS,
    LANE_OFFSET_SECONDS,
    SNAPSHOT_RETRY_MAX_SECONDS,
    SNAPSHOT_RETRY_MIN_SECONDS,
    SNAPSHOT_WEIGHT_PER_MINUTE,
    STATISTICS_LIMIT,
    STATISTICS_MAX_LIMIT,
    STATISTICS_PATHS,
    STATISTICS_PAUSE_SECONDS,
    STATISTICS_STEP_MS,
    VENUE_WEIGHT_PER_MINUTE,
    WEIGHT_THROTTLE_FRACTION,
    DepthSnapshotLane,
    _float,
    _ns,
    fetch_depth,
    fetch_funding_history,
    fetch_instruments,
    fetch_open_interest,
    fetch_statistics,
    fetch_tickers,
    funding_rows,
    statistics_rows,
)
from market_tape.venues.lanes import HourlyLane, LaneFeed, LaneState, RateLimited, RestBudget

# The REST side lives in `binance_rest`; its public names are this module's too.
__all__ = [
    "BOOK_LEVELS",
    "DEPTH_SNAPSHOT_WEIGHT",
    "DIFF_BOOK_LEVELS",
    "HOUR_NS",
    "LANE_FEEDS",
    "SNAPSHOT_RETRY_MAX_SECONDS",
    "SNAPSHOT_RETRY_MIN_SECONDS",
    "SNAPSHOT_WEIGHT_PER_MINUTE",
    "BinanceAdapter",
    "DepthSnapshotLane",
    "RateLimited",
    "fetch_depth",
    "fetch_funding_history",
    "fetch_statistics",
]

PUBLIC_USDM_WS = "wss://fstream.binance.com"
PUBLIC_REST = "https://fapi.binance.com"
#: The path of each connection group under `ws_url`.
GROUP_PATHS = {"public": "/stream", "market": "/market/stream"}
#: The diff stream's update speed: `<pair>@depth@100ms`, the full book's increments.
DIFF_DEPTH_SPEED = "100ms"
#: A diff stream's name after the pair.
DIFF_SUFFIX = f"depth@{DIFF_DEPTH_SPEED}"
#: `book:1` is `bookTicker`; 5, 10 and 20 are the partial-book streams; 1000
#: the diff stream with its REST snapshots.
BOOK_LEVELS = (1, 5, 10, 20, DIFF_BOOK_LEVELS)
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
        #: The writer thread's alone: each diff symbol's last `u`, absent until
        #: its first diff and again after `chain_reset`.
        self.diff_last_u: dict[str, int] = {}
        #: The IP's standing with the venue, shared by every lane: a 429 on one pauses all.
        self.rest_budget = RestBudget(VENUE_WEIGHT_PER_MINUTE, WEIGHT_THROTTLE_FRACTION)
        self.depth_snapshots = DepthSnapshotLane(rest_url=self.rest_url, venue=self.name, budget=self.rest_budget)
        #: The hourly lane's stamps and health: the adapter's, so they outlive a lane restart.
        self.lane_state = LaneState()
        #: The hourly lane's two fetches, looked up at each request (a test swaps them).
        self.fetch_funding: Callable[[str, str, int], tuple[list[dict[str, Any]], int | None]] = fetch_funding_history
        self.fetch_statistics: Callable[[str, str, str, int], tuple[list[dict[str, Any]], int | None]] = fetch_statistics

    def configure(self, config: Any) -> None:
        """The recorder's config, once, before any lane starts: the hourly
        snapshot pass follows `connection.reanchor_each_hour`."""

        self.depth_snapshots.hourly = bool(getattr(config, "reanchor_each_hour", True))

    # ---------------------------------------------------------------- feeds

    def validate_feeds(self, feeds: Iterable[Feed]) -> None:
        for feed in feeds:
            if feed.name == "book" and feed.levels not in BOOK_LEVELS:
                raise ConfigError(
                    f"Binance records book levels {BOOK_LEVELS} (bookTicker, the partial books 5/10/20 "
                    f"and the 1000-level diff book), not {feed.text}"
                )
            if feed.name == "kline" and feed.arg not in KLINE_INTERVALS:
                raise ConfigError(f"Binance kline intervals are {list(KLINE_INTERVALS)}, not {feed.text}")

    def topics(self, symbol: str, feeds: Iterable[Feed]) -> list[str]:
        lower = symbol.lower()
        result = []
        for feed in feeds:
            if feed.name == "book":
                levels = feed.levels
                if levels == 1:
                    result.append(f"{lower}@bookTicker")
                elif levels == DIFF_BOOK_LEVELS:
                    result.append(f"{lower}@{DIFF_SUFFIX}")
                else:
                    result.append(f"{lower}@depth{levels}@100ms")
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

    def rest_based_topics(self, topics: Iterable[str]) -> list[str]:
        """The diff books: a gap re-bases them from the REST lane, never by
        re-subscribing, and a (re)subscribe restarts their chains."""

        return [topic for topic in topics if topic.partition("@")[2] == DIFF_SUFFIX]

    def chain_reset(self, topics: Iterable[str]) -> None:
        """The writer thread's call, in tape order, before the first frame a
        (re)subscribe of `topics` can bring: each one's next diff starts a new
        chain, flagged and snapshotted, whether or not its `pu` happens to
        meet the last diff seen before the subscribe. Without it a symbol whose
        `pu` the venue numbers per connection, or that sent nothing across a
        reconnect, would carry on from a chain the tape cannot vouch for."""

        for topic in topics:
            pair, _, suffix = topic.partition("@")
            if suffix == DIFF_SUFFIX:
                self.diff_last_u.pop(pair.upper(), None)

    def lane_status(self) -> dict[str, Any]:
        return {
            "depth_snapshots": self.depth_snapshots.status(),
            "rest": self.lane_state.status(),
            "rest_budget": self.rest_budget.status(time.time_ns()),
        }

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
        diff_symbols: list[str] = []
        for symbol, feeds in feeds_by_symbol.items():
            for feed in feeds:
                if feed.name == "open_interest":
                    by_interval.setdefault(feed.seconds, []).append(symbol)
                elif feed.name == "book" and feed.levels == DIFF_BOOK_LEVELS:
                    diff_symbols.append(symbol)
        threads = []
        if diff_symbols:
            thread = threading.Thread(
                target=self.depth_snapshots.run,
                args=(diff_symbols, emit, stop),
                name="tape-binance-depth",
                daemon=True,
            )
            thread.start()
            threads.append(thread)
        lane = self.rest_lane(feeds_by_symbol, emit, stop)
        if lane is not None:
            thread = lane.thread()
            thread.start()
            threads.append(thread)
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

    def rest_lane(
        self, feeds_by_symbol: Mapping[str, tuple[Feed, ...]], emit: Emit, stop: threading.Event
    ) -> HourlyLane | None:
        """The hourly lane for the symbols that ask for a lane feed, not started;
        None when none does. Its stamps live in `lane_state` and its pauses in
        `rest_budget`, both the adapter's."""

        feeds = []
        for name in LANE_FEEDS:
            symbols = tuple(sorted(symbol for symbol, wanted in feeds_by_symbol.items() if Feed(name) in wanted))
            if symbols:
                feeds.append(self._lane_feed(name, symbols))
        if not feeds:
            return None
        return HourlyLane(
            venue=self.name,
            feeds=tuple(feeds),
            emit=emit,
            stop=stop,
            state=self.lane_state,
            budget=self.rest_budget,
            interval_seconds=LANE_INTERVAL_SECONDS,
            offset_seconds=LANE_OFFSET_SECONDS,
        )

    def _lane_feed(self, name: str, symbols: tuple[str, ...]) -> LaneFeed:
        if name == "funding":
            return LaneFeed(
                name=name,
                symbols=symbols,
                fetch=lambda symbol, limit: self.fetch_funding(self.rest_url, symbol, limit),
                rows=lambda symbol, listed, received, after: funding_rows(self.name, symbol, listed, received, after_ms=after),
                stamp="funding_time_ms",
                limit=FUNDING_HISTORY_LIMIT,
                step_ms=FUNDING_STEP_MS,
                max_limit=FUNDING_HISTORY_MAX_LIMIT,
                pause=FUNDING_PAUSE_SECONDS,
                newest_only_first=True,
            )
        path = STATISTICS_PATHS[name]
        return LaneFeed(
            name=name,
            symbols=symbols,
            fetch=lambda symbol, limit: self.fetch_statistics(self.rest_url, path, symbol, limit),
            rows=lambda symbol, listed, received, after: statistics_rows(
                name, self.name, symbol, listed, received, after_ms=after
            ),
            stamp="ts_ms",
            limit=STATISTICS_LIMIT,
            step_ms=STATISTICS_STEP_MS,
            max_limit=STATISTICS_MAX_LIMIT,
            pause=STATISTICS_PAUSE_SECONDS,
        )

    def _open_interest_lane(self, symbols: list[str], seconds: float, emit: Emit, stop: threading.Event) -> None:
        while not stop.is_set():
            started = time.monotonic()
            for symbol in symbols:
                if stop.is_set():
                    return
                # A 429 on any lane, or a heavy minute, holds the polls too.
                paused = self.rest_budget.wait_seconds(time.monotonic())
                if paused > 0 and stop.wait(paused):
                    return
                try:
                    emit(self.open_interest_row(symbol))
                except RateLimited as exc:
                    self.rest_budget.note_rate_limited(exc.retry_after, time.monotonic(), time.time_ns())
                    logging.warning("binance open interest polls paused %.0fs: %s", exc.retry_after, exc)
                except Exception as exc:  # noqa: BLE001 - one symbol's failed poll must not end the lane
                    logging.warning("open interest poll for %s failed: %s", symbol, exc)
                if stop.wait(OPEN_INTEREST_PAUSE_SECONDS):
                    return
            stop.wait(max(0.0, seconds - (time.monotonic() - started)))

    def open_interest_row(self, symbol: str) -> dict[str, Any]:
        payload, used = fetch_open_interest(self.rest_url, symbol)
        received_ns = time.time_ns()
        self.rest_budget.note_weight(used, time.monotonic(), received_ns)
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
        if suffix == DIFF_SUFFIX:
            return self._diff(data, received_ns, received_mono_ns)
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

    def _diff(self, data: Mapping[str, Any], received_ns: int, received_mono_ns: int) -> list[dict[str, Any]]:
        symbol = str(data.get("s") or "").upper()
        if not symbol:
            return []
        update_id = int(data.get("u") or 0)
        previous = int(data.get("pu") or 0)
        first = int(data.get("U") or 0)
        last = self.diff_last_u.get(symbol)
        # The venue's rule: each diff names the last one's `u` as its `pu`.
        gap = last is None or previous != last
        self.diff_last_u[symbol] = update_id
        if gap:
            self.depth_snapshots.request(symbol, first)
        return [
            typed_book_row(
                self.name,
                symbol,
                False,  # snapshot
                DIFF_BOOK_LEVELS,
                received_ns,
                received_mono_ns,
                _ns(data.get("E")),
                _ns(data.get("T")),
                data.get("b") or [],
                data.get("a") or [],
                update_id,
                previous,
                first,
                0,  # cross_sequence
                0,  # previous_cross_sequence
                False,  # restart_snapshot
                gap,
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
