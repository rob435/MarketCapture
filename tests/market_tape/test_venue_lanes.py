"""The hourly REST lane both venues run (`market_tape/venues/lanes.py`), and Binance's five lane feeds.

Binance answers this sandbox 451, so every venue answer here is shaped as the
venue's published USD-M docs show it, served by an injected fetch or a fake
`urlopen`: what is verified is the lane's own behaviour against those shapes,
not the venue's."""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from email.message import Message
from typing import Any, Callable

import pytest

from market_tape.config import parse_feed
from market_tape.schema import parse_row
from market_tape.venues import binance as binance_module
from market_tape.venues.binance import BinanceAdapter, RateLimited
from market_tape.venues.binance_rest import (
    STATISTICS_PATHS,
    fetch_funding_history,
    fetch_statistics,
    funding_rows,
    statistics_rows,
)
from market_tape.venues.bybit import BybitAdapter
from market_tape.venues.lanes import HourlyLane, LaneState

SECOND = 1_000_000_000
MINUTE_MS = 60_000
BUCKET_MS = 5 * MINUTE_MS
HOUR_MS = 3_600_000
EIGHT_HOURS_MS = 8 * HOUR_MS
#: An hour boundary two hours after an eight-hour settlement: 2027-01-15T10:00Z.
T0_MS = 1_800_000_000_000 // EIGHT_HOURS_MS * EIGHT_HOURS_MS + 2 * HOUR_MS
STATISTICS_FEEDS = ("account_ratio", "top_account_ratio", "top_position_ratio", "taker_volume")
ALL_LANE_FEEDS = ("funding",) + STATISTICS_FEEDS


def feeds(*texts: str) -> tuple[Any, ...]:
    return tuple(parse_feed(text) for text in texts)


@dataclass
class Clock:
    """A hand-driven wall and monotonic clock; `sleep` moves both unless frozen."""

    wall_ns: int = T0_MS * 1_000_000 + 4 * 60 * SECOND
    mono: float = 1_000.0
    frozen: bool = False
    slept: list[float] = field(default_factory=list)

    def wall(self) -> int:
        return self.wall_ns

    def monotonic(self) -> float:
        return self.mono

    def sleep(self, seconds: float) -> None:
        self.slept.append(round(seconds, 6))
        if not self.frozen:
            self.advance(seconds)

    def advance(self, seconds: float) -> None:
        self.mono += seconds
        self.wall_ns += int(seconds * SECOND)


@dataclass
class StatsVenue:
    """Binance's funding history and five-minute statistics as of the clock's
    wall time, in the docs' shapes: settlements every eight hours, a bucket
    every five minutes, the newest `limit` of each."""

    clock: Clock
    calls: list[tuple[str, str, int]] = field(default_factory=list)
    #: (feed path, symbol) -> what the next request of it raises, once.
    refuse: dict[tuple[str, str], Exception] = field(default_factory=dict)
    used_weight: Callable[[], int | None] = lambda: 40

    def _now_ms(self) -> int:
        return self.clock.wall_ns // 1_000_000

    def fetch_funding(self, rest: str, symbol: str, limit: int) -> tuple[list[dict[str, Any]], int | None]:
        self._call("/fapi/v1/fundingRate", symbol, limit)
        newest = self._now_ms() // EIGHT_HOURS_MS * EIGHT_HOURS_MS
        stamps = [newest - EIGHT_HOURS_MS * back for back in reversed(range(limit))]
        # Oldest first, as the venue lists them when no time window is sent.
        return [
            {"symbol": symbol, "fundingTime": stamp, "fundingRate": f"{0.0001 * (stamp // EIGHT_HOURS_MS % 3 - 1):.8f}", "markPrice": "34287.54619963"}
            for stamp in stamps
        ], self.used_weight()

    def fetch_statistics(self, rest: str, path: str, symbol: str, limit: int) -> tuple[list[dict[str, Any]], int | None]:
        self._call(path, symbol, limit)
        newest = self._now_ms() // BUCKET_MS * BUCKET_MS
        rows = []
        for back in reversed(range(limit)):
            stamp = newest - BUCKET_MS * back
            share = 0.5 + (stamp // BUCKET_MS % 10) / 100
            if path.endswith("takerlongshortRatio"):
                rows.append({"buySellRatio": f"{share / (1 - share):.4f}", "buyVol": f"{share * 1000:.4f}", "sellVol": f"{(1 - share) * 1000:.4f}", "timestamp": stamp})
            else:
                rows.append({"symbol": symbol, "longShortRatio": f"{share / (1 - share):.4f}", "longAccount": f"{share:.4f}", "shortAccount": f"{1 - share:.4f}", "timestamp": str(stamp)})
        return rows, self.used_weight()

    def _call(self, path: str, symbol: str, limit: int) -> None:
        self.calls.append((path, symbol, limit))
        refused = self.refuse.pop((path, symbol), None)
        if refused is not None:
            raise refused


def binance_lane(
    clock: Clock, venue: StatsVenue, wanted: dict[str, tuple[Any, ...]], emit: Callable[[Any], Any], *, adapter: BinanceAdapter | None = None, stop: threading.Event | None = None
) -> tuple[BinanceAdapter, HourlyLane]:
    adapter = adapter or BinanceAdapter(rest_url="http://venue")
    adapter.fetch_funding = venue.fetch_funding
    adapter.fetch_statistics = venue.fetch_statistics
    lane = adapter.rest_lane(wanted, emit, stop or threading.Event())
    assert lane is not None
    lane.clock_ns, lane.monotonic, lane.sleep = clock.wall, clock.monotonic, clock.sleep
    return adapter, lane


def stamp_of(row: dict[str, Any]) -> int:
    return int(row.get("funding_time_ms") or row.get("ts_ms") or 0)


def keys(rows: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
    return [(row["kind"], row["symbol"], stamp_of(row)) for row in rows]


def wait_for(condition: Callable[[], bool], seconds: float = 10.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


# ------------------------------------------------------------- row mappers


def test_funding_rows_are_the_settlements_after_the_last_one_written_oldest_first() -> None:
    listed = [
        {"symbol": "BTCUSDT", "fundingTime": 1698739200000, "fundingRate": "0.00010000", "markPrice": "34287.54619963"},
        {"symbol": "BTCUSDT", "fundingTime": 1698768000000, "fundingRate": "-0.00002015", "markPrice": ""},
        {"symbol": "BTCUSDT", "fundingTime": "1698710400000", "fundingRate": "0.00003000", "markPrice": "34000.1"},
        {"symbol": "BTCUSDT", "fundingTime": None, "fundingRate": "0.1"},
        {"symbol": "BTCUSDT", "fundingTime": 1698796800000, "fundingRate": "n/a"},
    ]
    rows = funding_rows("binance", "BTCUSDT", listed, 7, after_ms=1698710400000)
    assert rows == [
        {"kind": "funding_settlement", "venue": "binance", "symbol": "BTCUSDT", "local_receive_ts_ns": 7, "funding_time_ms": 1698739200000, "funding_rate": 0.0001},
        {"kind": "funding_settlement", "venue": "binance", "symbol": "BTCUSDT", "local_receive_ts_ns": 7, "funding_time_ms": 1698768000000, "funding_rate": -0.00002015},
    ]
    # The mark price is not carried: the row is the contract Bybit's lane writes too.
    assert all(parse_row(row).kind == "funding_settlement" for row in rows)
    assert len(funding_rows("binance", "BTCUSDT", listed, 7, after_ms=-1)) == 3


def test_statistics_rows_map_each_feeds_answer_onto_its_own_kind() -> None:
    ratio = [
        {"symbol": "BTCUSDT", "longShortRatio": "1.8105", "longAccount": "0.6442", "shortAccount": "0.3558", "timestamp": "1583139900000"},
        {"symbol": "BTCUSDT", "longShortRatio": "0.5918", "longAccount": "0.3718", "shortAccount": "0.6282", "timestamp": 1583139600000},
        {"symbol": "BTCUSDT", "longShortRatio": "1", "longAccount": "NaN", "shortAccount": "0.5", "timestamp": 1583140200000},
        {"symbol": "BTCUSDT", "longAccount": "0.5", "timestamp": 1583140500000},
    ]
    for feed in ("account_ratio", "top_account_ratio", "top_position_ratio"):
        rows = statistics_rows(feed, "binance", "BTCUSDT", ratio, 9, after_ms=-1)
        assert [(row["kind"], row["period"], row["ts_ms"], row["buy_ratio"], row["sell_ratio"]) for row in rows] == [
            (feed, "5min", 1583139600000, 0.3718, 0.6282),
            (feed, "5min", 1583139900000, 0.6442, 0.3558),
        ]
        assert [parse_row(row).kind for row in rows] == [feed, feed]
    taker = [
        {"buySellRatio": "1.5586", "buyVol": "387.3300", "sellVol": "248.5030", "timestamp": "1585614900000"},
        {"buySellRatio": "1.1", "buyVol": "1", "sellVol": "", "timestamp": "1585615200000"},
    ]
    (row,) = statistics_rows("taker_volume", "binance", "BTCUSDT", taker, 9, after_ms=-1)
    assert row == {
        "kind": "taker_volume",
        "venue": "binance",
        "symbol": "BTCUSDT",
        "local_receive_ts_ns": 9,
        "period": "5min",
        "ts_ms": 1585614900000,
        "buy_volume": 387.33,
        "sell_volume": 248.503,
    }
    assert statistics_rows("taker_volume", "binance", "BTCUSDT", taker, 9, after_ms=1585614900000) == []


class FakeResponse:
    def __init__(self, payload: Any, headers: dict[str, str] | None = None) -> None:
        self.payload = payload
        self.headers = Message()
        for name, value in (headers or {}).items():
            self.headers[name] = value

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


def test_the_lane_fetches_ask_the_documented_paths_and_read_the_ips_weight(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    answers: list[Any] = [
        FakeResponse([{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "0.0001", "markPrice": "1"}], {"X-MBX-USED-WEIGHT-1M": "12"}),
        FakeResponse([{"buySellRatio": "1", "buyVol": "1", "sellVol": "1", "timestamp": 1}]),
        FakeResponse({"code": -1121, "msg": "Invalid symbol."}),
        FakeResponse({"symbol": "BTCUSDT"}),
    ]

    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        calls.append(request.full_url)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    rows, used = fetch_funding_history("https://fapi.example", "BTCUSDT", 2)
    assert (len(rows), used) == (1, 12)
    rows, used = fetch_statistics("https://fapi.example", STATISTICS_PATHS["taker_volume"], "BTCUSDT", 13)
    assert (len(rows), used) == (1, None)
    assert calls == [
        "https://fapi.example/fapi/v1/fundingRate?symbol=BTCUSDT&limit=2",
        "https://fapi.example/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=13",
    ]
    assert set(STATISTICS_PATHS.values()) == {
        "/futures/data/globalLongShortAccountRatio",
        "/futures/data/topLongShortAccountRatio",
        "/futures/data/topLongShortPositionRatio",
        "/futures/data/takerlongshortRatio",
    }
    # A refusal and an answer that is no table are both the lane's failures, not empty pages.
    with pytest.raises(RuntimeError, match="Invalid symbol"):
        fetch_statistics("https://fapi.example", STATISTICS_PATHS["account_ratio"], "NOPE", 13)
    with pytest.raises(RuntimeError, match="no table"):
        fetch_funding_history("https://fapi.example", "BTCUSDT", 2)

    headers = Message()
    headers["Retry-After"] = "120"
    answers.append(urllib.error.HTTPError("x", 418, "banned", headers, None))
    with pytest.raises(RateLimited) as raised:
        fetch_statistics("https://fapi.example", STATISTICS_PATHS["account_ratio"], "BTCUSDT", 13)
    assert (raised.value.status, raised.value.retry_after) == (418, 120.0)


# ---------------------------------------------------------- the Binance lane


def test_the_binance_lane_walks_feed_major_paced_per_endpoint_and_writes_each_stamp_once() -> None:
    clock = Clock()
    venue = StatsVenue(clock)
    emitted: list[dict[str, Any]] = []
    adapter, lane = binance_lane(
        clock,
        venue,
        {
            "BTCUSDT": feeds(*ALL_LANE_FEEDS, "trades"),
            "ETHUSDT": feeds("account_ratio", "funding"),
            "SOLUSDT": feeds("trades"),
        },
        emitted.append,
    )
    assert lane.thread().name == "tape-lane-binance-rest"

    counts = lane.pass_once()

    # Feed by feed, symbols sorted; the first fetch asks for the last hour of
    # buckets and the newest two settlements, and writes all the buckets but
    # only the newest settlement.
    assert venue.calls == [
        ("/fapi/v1/fundingRate", "BTCUSDT", 2),
        ("/fapi/v1/fundingRate", "ETHUSDT", 2),
        ("/futures/data/globalLongShortAccountRatio", "BTCUSDT", 13),
        ("/futures/data/globalLongShortAccountRatio", "ETHUSDT", 13),
        ("/futures/data/topLongShortAccountRatio", "BTCUSDT", 13),
        ("/futures/data/topLongShortPositionRatio", "BTCUSDT", 13),
        ("/futures/data/takerlongshortRatio", "BTCUSDT", 13),
    ]
    # Each request is followed by its endpoint's pause: 1 s for funding (300 of
    # its 500 a 5 min), 0.5 s for the statistics (600 of their 1,000).
    assert clock.slept == [1.0, 1.0, 0.5, 0.5, 0.5, 0.5, 0.5]
    assert counts == {"funding": 2, "account_ratio": 26, "top_account_ratio": 13, "top_position_ratio": 13, "taker_volume": 13, "failures": 0}
    settlement = T0_MS - 2 * HOUR_MS
    assert [(row["symbol"], row["funding_time_ms"]) for row in emitted if row["kind"] == "funding_settlement"] == [
        ("BTCUSDT", settlement),
        ("ETHUSDT", settlement),
    ]
    buckets = [T0_MS - 12 * BUCKET_MS + BUCKET_MS * index for index in range(13)]
    for kind in STATISTICS_FEEDS:
        assert [row["ts_ms"] for row in emitted if row["kind"] == kind and row["symbol"] == "BTCUSDT"] == buckets
    assert all(row["venue"] == "binance" and parse_row(row) for row in emitted)
    assert {row["period"] for row in emitted if "period" in row} == {"5min"}

    # An hour later: twelve new buckets a feed, asked for by a catch-up that
    # reaches back to the last one written; no settlement fell in the hour.
    del emitted[:], venue.calls[:]
    clock.advance(3_600.0)
    counts = lane.pass_once()
    assert counts == {"funding": 0, "account_ratio": 24, "top_account_ratio": 12, "top_position_ratio": 12, "taker_volume": 12, "failures": 0}
    assert [limit for path, _, limit in venue.calls if path.startswith("/futures")] == [13] * 5
    assert [limit for path, _, limit in venue.calls if path.startswith("/fapi")] == [4, 4]
    assert len(set(keys(emitted))) == len(emitted)

    status = adapter.lane_status()["rest"]
    assert status["passes"] == 2 and status["failures"] == 0 and status["rate_limited"] == 0
    assert status["rows"] == {"funding": 2, "account_ratio": 50, "top_account_ratio": 25, "top_position_ratio": 25, "taker_volume": 25}
    assert adapter.lane_status()["rest_budget"]["used_weight_1m"] == 40


def test_a_refused_symbol_is_retried_next_pass_and_a_late_pass_loses_no_bucket(caplog: pytest.LogCaptureFixture) -> None:
    clock = Clock()
    venue = StatsVenue(clock)
    emitted: list[dict[str, Any]] = []
    adapter, lane = binance_lane(clock, venue, {"BTCUSDT": feeds("account_ratio"), "ETHUSDT": feeds("account_ratio")}, emitted.append)
    lane.pass_once()

    clock.advance(3_600.0)
    venue.refuse[(STATISTICS_PATHS["account_ratio"], "BTCUSDT")] = OSError("connection reset")
    with caplog.at_level(logging.WARNING):
        counts = lane.pass_once()
    assert counts == {"account_ratio": 12, "failures": 1}
    assert "binance rest lane: 1 of 2 symbols refused this pass, first BTCUSDT: connection reset" in caplog.text
    assert adapter.lane_state.status()["last_error"] == "BTCUSDT: connection reset"

    # Two hours of buckets the venue still serves: the next pass asks for them all.
    clock.advance(3_600.0)
    del venue.calls[:]
    lane.pass_once()
    assert venue.calls[0] == (STATISTICS_PATHS["account_ratio"], "BTCUSDT", 25)
    btc = sorted(row["ts_ms"] for row in emitted if row["symbol"] == "BTCUSDT")
    assert btc == list(range(btc[0], btc[-1] + 1, BUCKET_MS)) and len(btc) == 13 + 24
    assert adapter.lane_state.status()["failures"] == 1


def test_a_rate_limit_pauses_every_binance_lane_for_its_retry_after() -> None:
    clock = Clock(frozen=True)
    venue = StatsVenue(clock)
    venue.refuse[(STATISTICS_PATHS["account_ratio"], "BTCUSDT")] = RateLimited(429, 30.0)
    emitted: list[dict[str, Any]] = []
    adapter, lane = binance_lane(clock, venue, {"BTCUSDT": feeds("account_ratio"), "ETHUSDT": feeds("account_ratio")}, emitted.append)

    counts = lane.pass_once()

    # The refused request's pace, then the whole Retry-After before the next one.
    assert clock.slept == [0.5, 30.0, 0.5]
    assert counts == {"account_ratio": 13, "failures": 1}
    assert adapter.rest_budget.throttled_until_ns == clock.wall_ns + 30 * SECOND
    assert adapter.lane_status()["rest"]["rate_limited"] == 1 and adapter.rest_budget.rate_limited == 1
    # The snapshot lane shares the IP's standing: it sends nothing while paused.
    snapshots = adapter.depth_snapshots
    snapshots.monotonic, snapshots.wall_ns = clock.monotonic, clock.wall
    fetched: list[str] = []
    snapshots.fetch = lambda rest, symbol: fetched.append(symbol) or ({"lastUpdateId": 1}, None)  # type: ignore[func-returns-value]
    snapshots.use(["BTCUSDT"])
    snapshots.request("BTCUSDT", 1)
    assert snapshots.step(lambda row: True) == pytest.approx(30.0)
    assert fetched == []
    # The refused symbol is the next pass's: nothing of it was marked.
    assert ("account_ratio", "BTCUSDT") not in adapter.lane_state.last_ms


def test_a_heavy_minute_on_the_ip_holds_the_lane_to_the_next_minute() -> None:
    clock = Clock()
    clock.wall_ns += 45 * SECOND  # 15 s left in the minute
    weights = iter([2_000, 40, 40])
    venue = StatsVenue(clock, used_weight=lambda: next(weights))
    adapter, lane = binance_lane(clock, venue, {symbol: feeds("taker_volume") for symbol in ("AUSDT", "BUSDT", "CUSDT")}, lambda row: None)
    lane.pass_once()
    assert clock.slept == [0.5, 14.5, 0.5, 0.5]
    assert adapter.depth_snapshots.status()["used_weight_1m"] == 40


def test_a_row_the_writer_dropped_is_asked_for_again_and_written_once() -> None:
    clock = Clock()
    venue = StatsVenue(clock)
    emitted: list[dict[str, Any]] = []
    refused = {"left": 1}

    def emit(row: Any) -> bool:
        if len(emitted) == 4 and refused["left"]:
            refused["left"] -= 1
            return False
        emitted.append(dict(row))
        return True

    adapter, lane = binance_lane(clock, venue, {"BTCUSDT": feeds("top_position_ratio")}, emit)
    assert lane.pass_once() == {"top_position_ratio": 4, "failures": 0}
    assert adapter.lane_state.status()["dropped"] == 1
    clock.advance(3_600.0)
    lane.pass_once()
    stamps = [row["ts_ms"] for row in emitted]
    assert stamps == sorted(set(stamps)) and stamps == list(range(stamps[0], stamps[-1] + 1, BUCKET_MS))


def test_the_lane_ends_promptly_on_stop() -> None:
    # Mid-walk: the pass ends at the next symbol and counts no pass.
    clock = Clock()
    stop = threading.Event()
    venue = StatsVenue(clock)
    venue.used_weight = lambda: stop.set() or 40  # type: ignore[func-returns-value]
    adapter, lane = binance_lane(clock, venue, {"AUSDT": feeds("funding"), "BUSDT": feeds("funding")}, lambda row: None, stop=stop)
    assert lane.pass_once() == {"funding": 1, "failures": 0}
    assert len(venue.calls) == 1 and adapter.lane_state.passes == 0

    # A running lane, pausing on `stop` between requests (the default), leaves
    # within a moment of it rather than at the end of its pace or its hour.
    stop = threading.Event()
    venue = StatsVenue(Clock())
    adapter = BinanceAdapter(rest_url="http://venue")
    adapter.fetch_funding = venue.fetch_funding
    lane = adapter.rest_lane({symbol: feeds("funding") for symbol in ("AUSDT", "BUSDT", "CUSDT")}, lambda row: None, stop)
    assert lane is not None and lane.sleep is None
    thread = lane.thread()
    thread.start()
    assert wait_for(lambda: len(venue.calls) >= 1)
    started = time.monotonic()
    stop.set()
    thread.join(2.0)
    assert not thread.is_alive() and time.monotonic() - started < 0.5
    assert len(venue.calls) == 1  # the 1 s pace after the first request was cut short

    # A restarted lane waits for the old one to leave `run` rather than walk the state beside it.
    with adapter.lane_state.run_lock:
        second = lane.thread()
        second.start()
        second.join(1.0)
        assert not second.is_alive()  # stop is set: it gave up waiting


def test_the_open_interest_poll_honours_the_shared_pause_and_reports_a_429(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    headers = Message()
    headers["Retry-After"] = "30"

    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        calls.append(request.full_url)
        raise urllib.error.HTTPError(request.full_url, 429, "too many", headers, None)

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    adapter = BinanceAdapter(rest_url="http://venue")
    stop = threading.Event()
    thread = threading.Thread(target=adapter._open_interest_lane, args=(["AUSDT", "BUSDT"], 60.0, lambda row: None, stop), daemon=True)
    thread.start()
    assert wait_for(lambda: adapter.rest_budget.rate_limited == 1)
    # The next symbol waits out the pause instead of earning the IP a ban.
    time.sleep(0.2)
    assert calls == ["http://venue/fapi/v1/openInterest?symbol=AUSDT"]
    stop.set()
    thread.join(2.0)
    assert not thread.is_alive()


# ---------------------------------------------------- restarts, both venues


def test_a_restarted_binance_lane_resumes_its_slot_and_writes_nothing_twice() -> None:
    """The recorder restarts its lanes on every universe change. The lane's
    stamps are the adapter's: a new lane neither writes a row again nor walks
    a pair the slot already walked; a symbol new to the lane is walked."""

    clock = Clock()
    venue = StatsVenue(clock)
    emitted: list[dict[str, Any]] = []
    wanted = {"BTCUSDT": feeds(*ALL_LANE_FEEDS), "ETHUSDT": feeds("funding", "account_ratio")}
    first_stop = threading.Event()
    adapter, lane = binance_lane(clock, venue, wanted, emitted.append, stop=first_stop)
    thread = lane.thread()
    thread.start()
    assert wait_for(lambda: adapter.lane_state.passes == 1)
    first_stop.set()
    thread.join(2.0)
    assert not thread.is_alive()
    first_calls = len(venue.calls)
    first_rows = len(emitted)

    clock.advance(600.0)  # ten minutes on, the same slot
    second_stop = threading.Event()
    _, restarted = binance_lane(clock, venue, {**wanted, "SOLUSDT": feeds("account_ratio")}, emitted.append, adapter=adapter, stop=second_stop)
    thread = restarted.thread()
    thread.start()
    assert wait_for(lambda: adapter.lane_state.passes == 2)
    second_stop.set()
    thread.join(2.0)
    assert not thread.is_alive()
    assert venue.calls[first_calls:] == [(STATISTICS_PATHS["account_ratio"], "SOLUSDT", 13)]
    assert [row["symbol"] for row in emitted[first_rows:]] == ["SOLUSDT"] * 13

    # The next slot walks everything, and still writes each stamp once.
    clock.advance(3_600.0)
    restarted.pass_once()
    assert len(set(keys(emitted))) == len(emitted)


def _bybit_urlopen(clock: Clock, calls: list[str]) -> Callable[..., FakeResponse]:
    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        url = request.full_url
        calls.append(url)
        query = dict(part.split("=", 1) for part in url.partition("?")[2].split("&"))
        limit, symbol = int(query["limit"]), query["symbol"]
        now_ms = clock.wall_ns // 1_000_000
        if "/funding/history" in url:
            newest = now_ms // EIGHT_HOURS_MS * EIGHT_HOURS_MS
            listed = [
                {"symbol": symbol, "fundingRate": "0.0001", "fundingRateTimestamp": str(newest - EIGHT_HOURS_MS * back)}
                for back in range(limit)
            ]
        else:
            newest = now_ms // BUCKET_MS * BUCKET_MS
            listed = [
                {"symbol": symbol, "buyRatio": "0.6", "sellRatio": "0.4", "timestamp": str(newest - BUCKET_MS * back)}
                for back in range(limit)
            ]
        # Newest first, as Bybit lists them.
        return FakeResponse({"retCode": 0, "retMsg": "OK", "result": {"category": "linear", "list": listed}})

    return urlopen


def test_a_restarted_bybit_lane_writes_nothing_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bug this lane had: each restart built a lane with empty stamps, so
    every universe change wrote the newest settlement and the last hour of
    ratio buckets of every lane symbol again."""

    clock = Clock()
    calls: list[str] = []
    monkeypatch.setattr(urllib.request, "urlopen", _bybit_urlopen(clock, calls))
    adapter = BybitAdapter(rest_url="http://venue")
    emitted: list[dict[str, Any]] = []
    wanted = {"AGIUSDT": feeds("funding", "account_ratio"), "BTCUSDT": feeds("account_ratio")}

    def lane(stop: threading.Event, universe: dict[str, Any]) -> HourlyLane:
        built = adapter.rest_lane(universe, emitted.append, stop)
        assert built is not None and built.state is adapter.lane_state
        built.clock_ns, built.sleep = clock.wall, clock.sleep
        return built

    for round_, universe in enumerate((wanted, {**wanted, "ETHUSDT": feeds("funding")}), start=1):
        stop = threading.Event()
        thread = lane(stop, universe).thread()
        thread.start()
        assert wait_for(lambda: adapter.lane_state.passes == round_)
        stop.set()
        thread.join(2.0)
        assert not thread.is_alive()
        clock.advance(60.0)

    assert len(set(keys(emitted))) == len(emitted)
    assert sum(1 for row in emitted if row["kind"] == "funding_settlement") == 2  # AGI's, then ETH's
    assert sum(1 for row in emitted if row["kind"] == "account_ratio") == 26
    # The restarted lane walked only the symbol new to it.
    assert [url for url in calls[3:]] == ["http://venue/v5/market/funding/history?category=linear&symbol=ETHUSDT&limit=2"]

    # A pass an hour on (11:06), on a lane built yet again, writes only what is
    # new: each symbol's thirteen buckets from 10:05 to 11:05.
    clock.advance(3_600.0)
    before = len(emitted)
    assert lane(threading.Event(), wanted).pass_once() == {"funding": 0, "account_ratio": 26, "failures": 0}
    assert len(set(keys(emitted))) == len(emitted) == before + 26
    assert adapter.lane_status() == {"rest": adapter.lane_state.status()}


def test_start_lanes_runs_the_hourly_lane_beside_the_snapshot_and_poll_lanes(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = BinanceAdapter(rest_url="http://unused")
    monkeypatch.setattr(adapter.depth_snapshots, "run", lambda symbols, emit, stop: None)
    stop = threading.Event()
    stop.set()  # each thread finds it set and leaves before any request
    threads = adapter.start_lanes(
        {"BTCUSDT": feeds("book:1000", "taker_volume", "open_interest:60"), "ETHUSDT": feeds("funding")},
        lambda row: True,
        stop,
    )
    for thread in threads:
        thread.join(5.0)
    assert [thread.name for thread in threads] == ["tape-binance-depth", "tape-lane-binance-rest", "tape-binance-oi-60s"]
    assert not any(thread.is_alive() for thread in threads)
    assert adapter.rest_lane({"BTCUSDT": feeds("trades", "book:1")}, lambda row: True, stop) is None
    assert set(adapter.lane_status()) == {"depth_snapshots", "rest", "rest_budget"}


def test_the_pacing_keeps_a_whole_universe_under_the_venues_limits() -> None:
    """The arithmetic in `binance_rest.py`, checked against its constants: 550
    symbols with every lane feed walk inside the hour, and no five minutes
    spend more than 60 % of either request limit."""

    funding_rate = 300.0 / binance_module.FUNDING_PAUSE_SECONDS
    statistics_rate = 300.0 / binance_module.STATISTICS_PAUSE_SECONDS
    assert funding_rate <= 0.6 * 500 and statistics_rate <= 0.6 * 1_000
    walk = 550 * binance_module.FUNDING_PAUSE_SECONDS + 4 * 550 * binance_module.STATISTICS_PAUSE_SECONDS
    assert walk < 0.5 * binance_module.LANE_INTERVAL_SECONDS
    assert LaneState().status()["passes"] == 0
