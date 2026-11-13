"""Binance USD-M over REST: the tables, the open-interest poll, the `book:1000` snapshot lane and the hourly lane.

`market_tape/venues/binance.py` is the adapter and its streams; this is
everything it asks of `fapi.binance.com`, and what paces it. Every request
goes through `fetch_weighted_json`, which reads the IP's used weight from the
`X-MBX-USED-WEIGHT-1M` header and turns 429 and 418 into `RateLimited`; the
adapter's one `RestBudget` (`market_tape/venues/lanes.py`) carries both to
every lane, so a heavy minute or a 429 met by any of them holds them all.

The hourly lane (`HourlyLane`, shared with Bybit) records the five histories
tardis.dev records for binance-futures beyond the streams, each per symbol,
paced as `FUNDING_PAUSE_SECONDS` states:

| Feed | Path | Tape kind |
| --- | --- | --- |
| `funding` | `/fapi/v1/fundingRate` | `funding_settlement` (`fundingTime`, `fundingRate`) |
| `account_ratio` | `/futures/data/globalLongShortAccountRatio` | `account_ratio` |
| `top_account_ratio` | `/futures/data/topLongShortAccountRatio` | `top_account_ratio` |
| `top_position_ratio` | `/futures/data/topLongShortPositionRatio` | `top_position_ratio` |
| `taker_volume` | `/futures/data/takerlongshortRatio` | `taker_volume` (`buyVol`, `sellVol`) |

The statistics are asked for at `period=5m` and stored with Bybit's period
text, `5min`; a ratio's `longAccount`/`shortAccount` are its
`buy_ratio`/`sell_ratio`.

Measured versus assumed: none of this was measured against the venue. This
sandbox cannot reach Binance (HTTP 451), so every path, parameter and answer
shape here, the `X-MBX-USED-WEIGHT-1M` header, `Retry-After` and the
per-endpoint limits follow Binance's published USD-M documentation and are
verified only against injected fetches and local fake servers in the tests.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from market_tape.schema import (
    account_ratio_row,
    funding_row,
    taker_volume_row,
    top_account_ratio_row,
    top_position_ratio_row,
    typed_book_row,
)
from market_tape.venues import Emit
from market_tape.venues.lanes import (
    LANE_POLL_SECONDS,
    RATE_LIMIT_DEFAULT_SECONDS,
    RateLimited,
    RestBudget,
    header_number,
)

#: The diff book's depth on the tape: the level count of the REST snapshot it rides on.
DIFF_BOOK_LEVELS = 1000
#: The venue's request weight of one 1000-level `/fapi/v1/depth`, and its
#: per-IP weight limit a minute, shared by every REST call from the host.
DEPTH_SNAPSHOT_WEIGHT = 20
VENUE_WEIGHT_PER_MINUTE = 2_400
#: What the snapshot lane spends of it: 50 snapshots a minute, leaving the open-interest poll and the table reads the rest.
SNAPSHOT_WEIGHT_PER_MINUTE = 1_000
#: Past this share of the venue's minute (its `X-MBX-USED-WEIGHT-1M` header),
#: every lane waits out the minute: something else on the host is spending it.
WEIGHT_THROTTLE_FRACTION = 0.75
#: One symbol's failed snapshot is retried after this, doubling to the maximum.
SNAPSHOT_RETRY_MIN_SECONDS = 2.0
SNAPSHOT_RETRY_MAX_SECONDS = 300.0
#: A snapshot older than its chain's first diff is fetched again after this.
STALE_SNAPSHOT_RETRY_SECONDS = 1.0
SNAPSHOT_TIMEOUT_SECONDS = 10.0
HOUR_NS = 3_600 * 1_000_000_000
PREMIUM_TABLE_FIELDS = ("markPrice", "indexPrice", "lastFundingRate", "nextFundingTime")
DAY_TABLE_FIELDS = ("lastPrice", "quoteVolume", "volume")
BOOK_TABLE_FIELDS = ("bidPrice", "bidQty", "askPrice", "askQty")


def fetch_weighted_json(url: str, timeout: float = 20.0) -> tuple[Any, int | None]:
    """One public REST answer, and the IP's weight used this minute as the
    venue's `X-MBX-USED-WEIGHT-1M` header states it (None when absent).

    Raises `RateLimited` on 429 and 418, `RuntimeError` on the venue's
    `{"code", "msg"}` refusal, and whatever else the request met."""

    request = urllib.request.Request(url, headers={"User-Agent": "market-tape"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            used = header_number(getattr(response, "headers", None), "X-MBX-USED-WEIGHT-1M")
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        if exc.code in (418, 429):
            wait = header_number(exc.headers, "Retry-After")
            raise RateLimited(exc.code, wait if wait is not None and wait > 0 else RATE_LIMIT_DEFAULT_SECONDS) from exc
        raise
    if isinstance(payload, Mapping) and "code" in payload and "msg" in payload:
        raise RuntimeError(f"venue refused {url}: {str(payload)[:200]}")
    return payload, None if used is None else int(used)


def fetch_public_json(url: str, timeout: float = 20.0) -> Any:
    return fetch_weighted_json(url, timeout)[0]


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


def fetch_open_interest(rest_base: str, symbol: str) -> tuple[dict[str, Any], int | None]:
    payload, used = fetch_weighted_json(f"{rest_base}/fapi/v1/openInterest?symbol={symbol}")
    if not isinstance(payload, Mapping):
        raise RuntimeError(f"open interest for {symbol} is not a table: {str(payload)[:200]}")
    return dict(payload), used


def fetch_depth(rest_base: str, symbol: str, timeout: float = SNAPSHOT_TIMEOUT_SECONDS) -> tuple[dict[str, Any], int | None]:
    """One symbol's 1000-level REST book, and the IP's weight used this minute
    as the venue's `X-MBX-USED-WEIGHT-1M` header states it (None when absent).

    Raises `RateLimited` on 429 and 418, and whatever else the request met."""

    url = f"{rest_base}/fapi/v1/depth?symbol={symbol}&limit={DIFF_BOOK_LEVELS}"
    payload, used = fetch_weighted_json(url, timeout)
    if not isinstance(payload, Mapping) or "lastUpdateId" not in payload:
        raise RuntimeError(f"venue refused {url}: {str(payload)[:200]}")
    return dict(payload), used


@dataclass
class DepthSnapshotLane:
    """The REST snapshots the `book:1000` diff stream rides on, one thread's work.

    The state lives here, on the adapter, and not in the thread: the recorder
    stops and restarts its lanes whenever the universe changes, and a gap
    asked for, a symbol in backoff or a minute's spent weight must outlive
    that. `run_lock` keeps one thread at a time in `run`, so a restart whose
    old thread is still inside a fetch never has two walking the state.

    Two kinds of work, in this order:

    1. A symbol whose chain gapped, asked for by `request` from the writer
       thread. The request is taken off `wanted` when its fetch starts, so a
       gap that arrives while the fetch is in flight puts it back and costs
       another fetch: that snapshot may land on the tape ahead of the new
       gap's diff and could not re-base it.
    2. Once per UTC hour (`hourly`, the config's
       `connection.reanchor_each_hour`), every diff symbol not yet
       snapshotted in that hour, in sorted order from the hour boundary, so
       one hour's tar replays without the hours before it.

    Pacing: one fetch per `60 * DEPTH_SNAPSHOT_WEIGHT / weight_per_minute`
    seconds, every attempt counted because the venue counts it. `budget` is
    the adapter's `RestBudget`, shared with every other Binance lane: past
    `WEIGHT_THROTTLE_FRACTION` of the venue's minute (what its header says the
    whole IP has spent) the next fetch waits for the next minute, and a 429 or
    418 on any lane pauses this one for `Retry-After`. Any other failure retries
    that symbol with backoff and moves on to the others. A snapshot older than
    its chain's first diff could never be bracketed and is not emitted; one
    the writer's queue had no room for is asked for again. Nothing here
    raises out of `run`, and `run` returns within `LANE_POLL_SECONDS` of
    `stop`, or after the fetch in hand (`SNAPSHOT_TIMEOUT_SECONDS`).
    """

    rest_url: str
    venue: str = "binance"
    fetch: Callable[[str, str], tuple[dict[str, Any], int | None]] = fetch_depth
    wall_ns: Callable[[], int] = time.time_ns
    monotonic: Callable[[], float] = time.monotonic
    weight_per_minute: float = SNAPSHOT_WEIGHT_PER_MINUTE
    hourly: bool = True
    budget: RestBudget = field(default_factory=lambda: RestBudget(VENUE_WEIGHT_PER_MINUTE, WEIGHT_THROTTLE_FRACTION))
    lock: threading.Lock = field(default_factory=threading.Lock)
    run_lock: threading.Lock = field(default_factory=threading.Lock)
    wake: threading.Event = field(default_factory=threading.Event)
    #: Gapped symbols waiting for a snapshot, oldest request first (under `lock`).
    wanted: dict[str, None] = field(default_factory=dict)
    #: Each symbol's current chain's first `U`, written with its request.
    chain_first: dict[str, int] = field(default_factory=dict)
    #: The UTC hour (`ns // HOUR_NS`) of each symbol's last emitted snapshot.
    snapshot_hour: dict[str, int] = field(default_factory=dict)
    retry_at: dict[str, float] = field(default_factory=dict)
    failures_in_row: dict[str, int] = field(default_factory=dict)
    symbols: tuple[str, ...] = ()
    members: frozenset[str] = frozenset()
    next_fetch_at: float = 0.0
    snapshots: int = 0
    failures: int = 0
    stale: int = 0
    dropped: int = 0
    rate_limited: int = 0

    # ------------------------------------------------------------ requests

    def request(self, symbol: str, first_update_id: int) -> None:
        """A chain starting at `first_update_id` needs a snapshot: the writer
        thread's call, on a gap only, so a clean diff never takes the lock."""

        with self.lock:
            self.chain_first[symbol] = first_update_id
            self.wanted[symbol] = None
        self.wake.set()

    def use(self, symbols: Iterable[str]) -> None:
        """The diff symbols this lane serves now; the requests of any other are dropped.

        Only here, as a lane starts: the recorder subscribes a new symbol before
        it restarts the lane, so the outgoing lane meets that symbol's first
        request and must leave it for its successor."""

        self.symbols = tuple(sorted(set(symbols)))
        self.members = frozenset(self.symbols)
        with self.lock:
            for symbol in [symbol for symbol in self.wanted if symbol not in self.members]:
                del self.wanted[symbol]

    # ----------------------------------------------------------------- run

    def run(self, symbols: Iterable[str], emit: Emit, stop: threading.Event) -> None:
        while not self.run_lock.acquire(timeout=LANE_POLL_SECONDS):
            if stop.is_set():
                return
        try:
            self.use(symbols)
            while not stop.is_set():
                try:
                    delay = self.step(emit)
                except Exception:  # the lane outlives anything one step meets
                    logging.exception("binance depth snapshot lane step failed")
                    delay = 1.0
                self.wake.wait(max(0.0, min(delay, LANE_POLL_SECONDS)))
                self.wake.clear()
        finally:
            self.run_lock.release()

    @property
    def spacing_seconds(self) -> float:
        return 60.0 * DEPTH_SNAPSHOT_WEIGHT / self.weight_per_minute if self.weight_per_minute > 0 else 60.0

    def step(self, emit: Emit) -> float:
        """Take at most one snapshot; return the seconds until the next could be due."""

        now = self.monotonic()
        paused = self.budget.wait_seconds(now)
        if paused > 0:
            return paused
        if now < self.next_fetch_at:
            return self.next_fetch_at - now
        symbol, gapped = self._pick(now)
        if symbol is None:
            return 1.0
        self.next_fetch_at = now + self.spacing_seconds
        try:
            payload, used = self.fetch(self.rest_url, symbol)
            received_ns = self.wall_ns()
            row = self.snapshot_row(symbol, payload, received_ns)
        except RateLimited as exc:
            self.rate_limited += 1
            self.budget.note_rate_limited(exc.retry_after, self.monotonic(), self.wall_ns())
            self._again(symbol, gapped)
            logging.warning("binance depth snapshots paused %.0fs: %s", exc.retry_after, exc)
            return exc.retry_after
        except Exception as exc:  # one symbol's failure is retried; the others go on
            self.failures += 1
            count = self.failures_in_row[symbol] = self.failures_in_row.get(symbol, 0) + 1
            delay = min(SNAPSHOT_RETRY_MIN_SECONDS * 2.0 ** (count - 1), SNAPSHOT_RETRY_MAX_SECONDS)
            self.retry_at[symbol] = self.monotonic() + delay
            self._again(symbol, gapped)
            if count == 1:
                logging.warning("binance depth snapshot for %s failed, retrying with backoff: %s", symbol, exc)
            return 0.0
        self.budget.note_weight(used, self.monotonic(), self.wall_ns())
        self.failures_in_row.pop(symbol, None)
        self.retry_at.pop(symbol, None)
        first = self.chain_first.get(symbol)
        if first is not None and row["update_id"] < first:
            # The REST answer predates the chain the tape holds: no diff on the
            # tape can bracket it, so it would only re-base nothing.
            self.stale += 1
            self.retry_at[symbol] = self.monotonic() + STALE_SNAPSHOT_RETRY_SECONDS
            self._again(symbol, gapped)
            return 0.0
        if emit(row) is False:
            self.dropped += 1
            self._again(symbol, True)
            return 0.0
        self.snapshots += 1
        self.snapshot_hour[symbol] = received_ns // HOUR_NS
        return 0.0

    def _pick(self, now: float) -> tuple[str | None, bool]:
        with self.lock:
            for symbol in list(self.wanted):
                if symbol not in self.members or self.retry_at.get(symbol, 0.0) > now:
                    continue
                del self.wanted[symbol]
                return symbol, True
        if not self.hourly:
            return None, False
        hour = self.wall_ns() // HOUR_NS
        for symbol in self.symbols:
            # A symbol no diff has reached yet is asked for by its first diff.
            if self.snapshot_hour.get(symbol) == hour or symbol not in self.chain_first:
                continue
            if self.retry_at.get(symbol, 0.0) > now:
                continue
            return symbol, False
        return None, False

    def _again(self, symbol: str, gapped: bool) -> None:
        # An hourly symbol needs nothing: the hour's walk finds it again.
        if gapped:
            with self.lock:
                self.wanted.setdefault(symbol, None)

    def snapshot_row(self, symbol: str, payload: Mapping[str, Any], received_ns: int) -> dict[str, Any]:
        return typed_book_row(
            self.venue,
            symbol,
            True,  # snapshot
            DIFF_BOOK_LEVELS,
            received_ns,
            0,  # local_receive_mono_ns: no socket read carried it
            _ns(payload.get("E")),
            _ns(payload.get("T")),
            list(payload.get("bids") or []),
            list(payload.get("asks") or []),
            int(payload["lastUpdateId"]),
            0,  # previous_update_id
            0,  # first_update_id
            0,  # cross_sequence
            0,  # previous_cross_sequence
            False,  # restart_snapshot
            False,  # sequence_gap
        )

    def status(self) -> dict[str, Any]:
        with self.lock:
            pending = len(self.wanted)
        budget = self.budget.status(self.wall_ns())
        return {
            "symbols": len(self.symbols),
            "snapshots": self.snapshots,
            "failures": self.failures,
            "stale": self.stale,
            "dropped": self.dropped,
            "rate_limited": self.rate_limited,
            "pending": pending,
            "used_weight_1m": budget["used_weight_1m"],
            "throttled_until_ns": budget["throttled_until_ns"],
        }


# ------------------------------------------------------ the hourly REST lane

#: The hourly lane's feeds, in walk order: settled funding, then the four
#: trading statistics, each from the path below.
LANE_FEEDS = ("funding", "account_ratio", "top_account_ratio", "top_position_ratio", "taker_volume")
STATISTICS_PATHS = {
    "account_ratio": "/futures/data/globalLongShortAccountRatio",
    "top_account_ratio": "/futures/data/topLongShortAccountRatio",
    "top_position_ratio": "/futures/data/topLongShortPositionRatio",
    "taker_volume": "/futures/data/takerlongshortRatio",
}
#: The statistics bucket as the venue names it, and as the tape stores it:
#: Bybit's name, so one period reads the same from either venue.
STATISTICS_PERIOD = "5m"
STORED_PERIOD = "5min"
#: First fetch: the last hour of buckets plus one, the newest settlement plus
#: one. A catch-up reaches back to the last stamp written, in steps of the
#: shortest spacing (five minutes; the hourly settlement some symbols carry),
#: up to the venue's largest page (the statistics keep only 30 days anyway).
STATISTICS_LIMIT = 13
STATISTICS_STEP_MS = 300_000
STATISTICS_MAX_LIMIT = 500
FUNDING_HISTORY_LIMIT = 2
FUNDING_STEP_MS = 3_600_000
FUNDING_HISTORY_MAX_LIMIT = 1_000
#: Once an hour, three minutes past it: settlements fall on the hour and the
#: history lists them shortly after.
LANE_INTERVAL_SECONDS = 3_600.0
LANE_OFFSET_SECONDS = 180.0
# Pacing: each request is followed by its feed's pause. The venue's per-IP
# limits (its published docs): `/fapi/v1/fundingRate` has its own 500
# requests / 5 min (shared with `/fapi/v1/fundingInfo`, which nothing here
# calls); the `/futures/data/*` statistics share their own 1,000 / 5 min.
# For ~550 symbols carrying all five feeds:
#   funding     one request a second at most: 300 / 5 min, 60 % of 500;
#               550 requests walk in ~9.2 min.
#   statistics  one request per 0.5 s at most: 600 / 5 min, 60 % of 1,000;
#               4 x 550 = 2,200 requests walk in ~18.3 min.
# The walk is feed-major, so the two spend their limits in turn: ~27.5 min an
# hour, and no five minutes over 60 % of either limit whatever subset of the
# feeds a config asks for. Request weight, were each to cost one (the docs
# give these their own request limits instead): two a second is 120 of the
# IP's 2,400 a minute, beside the snapshot lane's 1,000
# (`SNAPSHOT_WEIGHT_PER_MINUTE`) and the open-interest poll's one per symbol
# per interval (550 a minute at `open_interest:60`): 1,670, under the 1,800
# (`WEIGHT_THROTTLE_FRACTION`) past which every lane waits out the minute.
FUNDING_PAUSE_SECONDS = 1.0
STATISTICS_PAUSE_SECONDS = 0.5

_RATIO_ROWS = {
    "account_ratio": account_ratio_row,
    "top_account_ratio": top_account_ratio_row,
    "top_position_ratio": top_position_ratio_row,
}


def _rest_table(payload: Any, url: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise RuntimeError(f"venue answered {url} with no table: {str(payload)[:200]}")
    return [row for row in payload if isinstance(row, dict)]


def fetch_funding_history(rest_base: str, symbol: str, limit: int) -> tuple[list[dict[str, Any]], int | None]:
    """The venue's settled funding for one symbol, oldest first: `{symbol,
    fundingTime, fundingRate, markPrice}` rows, and the used weight."""

    url = f"{rest_base}/fapi/v1/fundingRate?{urllib.parse.urlencode({'symbol': symbol, 'limit': limit})}"
    payload, used = fetch_weighted_json(url)
    return _rest_table(payload, url), used


def fetch_statistics(rest_base: str, path: str, symbol: str, limit: int) -> tuple[list[dict[str, Any]], int | None]:
    """One symbol's five-minute trading statistics from `path`, and the used
    weight: `{symbol, longShortRatio, longAccount, shortAccount, timestamp}`
    rows from the three ratio paths, `{buySellRatio, buyVol, sellVol,
    timestamp}` from the taker path."""

    params = urllib.parse.urlencode({"symbol": symbol, "period": STATISTICS_PERIOD, "limit": limit})
    url = f"{rest_base}{path}?{params}"
    payload, used = fetch_weighted_json(url)
    return _rest_table(payload, url), used


def funding_rows(
    venue: str, symbol: str, listed: Iterable[Mapping[str, Any]], received_ns: int, *, after_ms: int
) -> list[dict[str, Any]]:
    """Tape rows for the settlements stamped after `after_ms`, oldest first.

    The `markPrice` the venue lists beside each settlement is not kept: the
    funding row's fields are the contract both venues write, and a field
    added to it would change the block format's fixed record."""

    rows: list[dict[str, Any]] = []
    for entry in listed:
        stamp = _int_or_none(entry.get("fundingTime"))
        rate = _float(entry.get("fundingRate"))
        if stamp is None or rate is None or stamp <= after_ms:
            continue
        rows.append(
            funding_row(venue=venue, symbol=symbol, local_receive_ts_ns=received_ns, funding_time_ms=stamp, funding_rate=rate)
        )
    rows.sort(key=lambda row: row["funding_time_ms"])
    return rows


def statistics_rows(
    feed: str, venue: str, symbol: str, listed: Iterable[Mapping[str, Any]], received_ns: int, *, after_ms: int
) -> list[dict[str, Any]]:
    """Tape rows of one statistics feed for the buckets stamped after
    `after_ms`, oldest first. A ratio's `longAccount` and `shortAccount` are
    its buy and sell ratio (the venue's `longShortRatio` is their quotient);
    taker volume's `buyVol` and `sellVol` its buy and sell volume (its
    `buySellRatio` likewise)."""

    taker = feed == "taker_volume"
    names = ("buyVol", "sellVol") if taker else ("longAccount", "shortAccount")
    rows: list[dict[str, Any]] = []
    for entry in listed:
        stamp = _int_or_none(entry.get("timestamp"))
        buy = _float(entry.get(names[0]))
        sell = _float(entry.get(names[1]))
        if stamp is None or buy is None or sell is None or stamp <= after_ms:
            continue
        if not (math.isfinite(buy) and math.isfinite(sell)):
            continue
        if taker:
            row = taker_volume_row(
                venue=venue,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                period=STORED_PERIOD,
                ts_ms=stamp,
                buy_volume=buy,
                sell_volume=sell,
            )
        else:
            row = _RATIO_ROWS[feed](
                venue=venue,
                symbol=symbol,
                local_receive_ts_ns=received_ns,
                period=STORED_PERIOD,
                ts_ms=stamp,
                buy_ratio=buy,
                sell_ratio=sell,
            )
        rows.append(row)
    rows.sort(key=lambda row: row["ts_ms"])
    return rows


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


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

