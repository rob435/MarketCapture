"""Binance USD-M's full book (`book:1000`): the diff stream, its REST snapshot lane, and the tape it leaves.

Nothing here has met the venue: the sandbox cannot reach Binance (HTTP 451),
so the frames and REST answers follow Binance's published USD-M shapes
(`depthUpdate` with `U`, `u`, `pu`; `/fapi/v1/depth` with `lastUpdateId`,
`E`, `T`). What it holds the adapter, the lane, the recorder and the rebuild
to:

- a diff is one `orderbook_delta` row at depth 1000, flagged `sequence_gap`
  on the first diff of a chain (start, a broken `pu`, a (re)subscribe), and
  each flag asks the lane for that symbol's snapshot;
- the lane takes gaps first and every symbol once an hour, paces itself by
  weight, pauses on 429, backs one symbol off on an error, refetches a
  snapshot too stale to bracket and one the writer's queue dropped, and
  stops when told;
- end to end, against a local venue over real sockets, the tape rebuilds
  into the venue's own book, a dropped diff is a gap the next snapshot
  heals, and the hour's coverage record accounts both.
"""

from __future__ import annotations

import http.server
import json
import queue
import random
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from email.message import Message
from pathlib import Path
from typing import Any, Callable

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import ServerConnection, serve

from market_tape import pack
from market_tape.blocks import BlockReader, BlockWriter
from market_tape.book import Book
from market_tape.config import CaptureConfig, Feed, StorageSettings, Tier, Universe, VenueSettings, parse_feed
from market_tape.coverage import build_ledger
from market_tape.load import HostRoot, iter_coverage, iter_rows
from market_tape.record import CHAIN_RESET, Recorder, Shard
from market_tape.schema import BookRow, parse_row
from market_tape.venues.binance import (
    DIFF_DEPTH_SPEED,
    HOUR_NS,
    SNAPSHOT_RETRY_MIN_SECONDS,
    BinanceAdapter,
    DepthSnapshotLane,
    RateLimited,
    fetch_depth,
)

RECEIVED = 1_790_370_834_300_000_000
MONO = 2_711_837_864_676


def feeds(*texts: str) -> tuple[Any, ...]:
    return tuple(parse_feed(text) for text in texts)


def diff(symbol: str, first: int, last: int, previous: int, bids: Any = (), asks: Any = (), ms: int = 1790370834267) -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@depth@{DIFF_DEPTH_SPEED}",
            "data": {
                "e": "depthUpdate",
                "E": ms,
                "T": ms - 1,
                "s": symbol,
                "U": first,
                "u": last,
                "pu": previous,
                "b": [list(level) for level in bids],
                "a": [list(level) for level in asks],
            },
        }
    )


# --------------------------------------------------------------- the adapter


def test_the_full_book_is_the_diff_stream_on_the_public_path() -> None:
    adapter = BinanceAdapter()
    topics = adapter.topics("BTCUSDT", feeds("book:1000", "book:20", "book:1"))
    assert topics == ["btcusdt@depth@100ms", "btcusdt@depth20@100ms", "btcusdt@bookTicker"]
    assert adapter.connection_group("btcusdt@depth@100ms") == "public"
    assert adapter.connection_url(topics) == "wss://fstream.binance.com/stream"
    # Re-based from REST, not by a subscribe: never anchored, but accounted.
    assert adapter.anchored_topics(topics) == []
    assert adapter.rest_based_topics(topics) == ["btcusdt@depth@100ms"]


def test_a_diff_is_one_delta_row_flagged_where_its_chain_starts_or_breaks() -> None:
    adapter = BinanceAdapter()
    (row,) = adapter.normalize(diff("BTCUSDT", 101, 105, 100, bids=[("100.1", "2")], asks=[("100.2", "0")]), RECEIVED, MONO)
    assert row == {
        "kind": "orderbook_delta",
        "venue": "binance",
        "symbol": "BTCUSDT",
        "depth": 1000,
        "local_receive_ts_ns": RECEIVED,
        "local_receive_mono_ns": MONO,
        "exchange_system_ts_ns": 1790370834267_000_000,
        "exchange_engine_ts_ns": 1790370834266_000_000,
        "bids": [["100.1", "2"]],
        "asks": [["100.2", "0"]],
        "update_id": 105,
        "previous_update_id": 100,
        "first_update_id": 101,
        "cross_sequence": 0,
        "previous_cross_sequence": 0,
        "restart_snapshot": False,
        "sequence_gap": True,  # nothing seen before it
    }
    typed = parse_row(row)
    assert isinstance(typed, BookRow) and not typed.snapshot and typed.depth == 1000
    lane = adapter.depth_snapshots
    assert list(lane.wanted) == ["BTCUSDT"] and lane.chain_first["BTCUSDT"] == 101

    # Chained: no flag, and no request.
    lane.wanted.clear()
    assert adapter.normalize(diff("BTCUSDT", 106, 110, 105), RECEIVED)[0]["sequence_gap"] is False
    assert not lane.wanted
    # A diff the stream lost: the next one names a predecessor the tape never saw.
    assert adapter.normalize(diff("BTCUSDT", 121, 125, 120), RECEIVED)[0]["sequence_gap"] is True
    assert list(lane.wanted) == ["BTCUSDT"] and lane.chain_first["BTCUSDT"] == 121
    # Symbols chain apart.
    assert adapter.normalize(diff("ETHUSDT", 7, 9, 6), RECEIVED)[0]["sequence_gap"] is True
    assert adapter.normalize(diff("BTCUSDT", 126, 130, 125), RECEIVED)[0]["sequence_gap"] is False
    # The partial books are untouched: whole, unflagged, never a request.
    lane.wanted.clear()
    partial = json.dumps({"stream": "btcusdt@depth20@100ms", "data": {"s": "BTCUSDT", "U": 1, "u": 2, "pu": 0, "b": [], "a": []}})
    (whole,) = adapter.normalize(partial, RECEIVED)
    assert (whole["kind"], whole["depth"], whole["sequence_gap"]) == ("orderbook_snapshot", 20, False)
    assert not lane.wanted


def test_a_resubscribe_restarts_the_chain_even_when_pu_happens_to_meet_it() -> None:
    adapter = BinanceAdapter()
    adapter.normalize(diff("BTCUSDT", 1, 5, 0), RECEIVED)
    adapter.normalize(diff("ETHUSDT", 1, 5, 0), RECEIVED)
    adapter.depth_snapshots.wanted.clear()
    # Only the diff topics' chains reset; another stream's name changes nothing.
    adapter.chain_reset(["btcusdt@depth@100ms", "ethusdt@bookTicker", "!forceOrder@arr"])
    assert adapter.normalize(diff("BTCUSDT", 6, 9, 5), RECEIVED)[0]["sequence_gap"] is True
    assert adapter.normalize(diff("ETHUSDT", 6, 9, 5), RECEIVED)[0]["sequence_gap"] is False
    assert list(adapter.depth_snapshots.wanted) == ["BTCUSDT"]


def test_lanes_start_the_snapshot_lane_only_for_a_diff_book(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = BinanceAdapter(rest_url="http://unused")
    ran: list[tuple[list[str], Any]] = []
    monkeypatch.setattr(adapter.depth_snapshots, "run", lambda symbols, emit, stop: ran.append((sorted(symbols), emit)))
    stop = threading.Event()
    assert adapter.start_lanes({"BTCUSDT": feeds("book:1", "trades")}, lambda row: True, stop) == []
    threads = adapter.start_lanes(
        {"BTCUSDT": feeds("book:1000"), "ETHUSDT": feeds("book:1000", "book:1"), "SOLUSDT": feeds("book:20")},
        lambda row: True,
        stop,
    )
    for thread in threads:
        thread.join(5.0)
    assert [thread.name for thread in threads] == ["tape-binance-depth"]
    assert [symbols for symbols, _ in ran] == [["BTCUSDT", "ETHUSDT"]]


def test_configure_carries_the_hourly_re_anchor_switch_to_the_lane(tmp_path: Path) -> None:
    def config(reanchor: bool) -> CaptureConfig:
        return CaptureConfig(
            venue=VenueSettings("binance", "usdm"),
            storage=StorageSettings(root=tmp_path),
            tiers=(Tier("core", feeds("book:1000"), Universe("symbols", symbols=("BTCUSDT",))),),
            reanchor_each_hour=reanchor,
        )

    assert Recorder(config(False), adapter=BinanceAdapter()).adapter.depth_snapshots.hourly is False  # type: ignore[attr-defined]
    assert Recorder(config(True), adapter=BinanceAdapter()).adapter.depth_snapshots.hourly is True  # type: ignore[attr-defined]


# ------------------------------------------------------------- REST snapshot


class FakeResponse:
    def __init__(self, payload: Any, headers: dict[str, str]) -> None:
        self.payload = payload
        self.headers = Message()
        for name, value in headers.items():
            self.headers[name] = value

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


def test_fetch_depth_reads_the_book_and_the_ips_used_weight(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    answer = {"lastUpdateId": 1027024, "E": 1589436922972, "T": 1589436922959, "bids": [["4.0", "431.0"]], "asks": [["4.2", "12.0"]]}

    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        calls.append(request.full_url)
        return FakeResponse(answer, {"x-mbx-used-weight-1m": "41"})

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    assert fetch_depth("https://fapi.example", "BTCUSDT") == (answer, 41)
    assert calls == ["https://fapi.example/fapi/v1/depth?symbol=BTCUSDT&limit=1000"]

    row = DepthSnapshotLane(rest_url="x").snapshot_row("BTCUSDT", answer, RECEIVED)
    assert (row["kind"], row["depth"], row["update_id"], row["sequence_gap"]) == ("orderbook_snapshot", 1000, 1027024, False)
    assert (row["exchange_system_ts_ns"], row["exchange_engine_ts_ns"]) == (1589436922972_000_000, 1589436922959_000_000)
    assert (row["local_receive_ts_ns"], row["local_receive_mono_ns"]) == (RECEIVED, 0)


@pytest.mark.parametrize(("status", "retry_after", "expected"), [(429, "7", 7.0), (418, None, 60.0)])
def test_fetch_depth_turns_a_rate_limit_into_its_wait(
    monkeypatch: pytest.MonkeyPatch, status: int, retry_after: str | None, expected: float
) -> None:
    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        headers = Message()
        if retry_after is not None:
            headers["Retry-After"] = retry_after
        raise urllib.error.HTTPError(request.full_url, status, "limited", headers, None)

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(RateLimited) as raised:
        fetch_depth("https://fapi.example", "BTCUSDT")
    assert (raised.value.status, raised.value.retry_after) == (status, expected)

    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout=None: FakeResponse({"code": -1121, "msg": "Invalid symbol."}, {}))
    with pytest.raises(RuntimeError, match="venue refused"):
        fetch_depth("https://fapi.example", "NOPE")


# ------------------------------------------------------------------ the lane


HOUR_START = 1_790_370_000 * 1_000_000_000 // HOUR_NS * HOUR_NS


@dataclass
class Harness:
    """A lane on a hand-driven clock, a scripted venue and a recording writer."""

    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    mono: float = 1_000.0
    wall: int = HOUR_START + 10 * 1_000_000_000
    #: The next answer per symbol: a lastUpdateId, an exception, or a callable that returns either.
    answers: dict[str, Any] = field(default_factory=dict)
    used_weight: int | None = 40
    fetched: list[str] = field(default_factory=list)
    emitted: list[dict[str, Any]] = field(default_factory=list)
    accept: bool = True
    lane: DepthSnapshotLane = field(init=False)

    def __post_init__(self) -> None:
        self.lane = DepthSnapshotLane(
            rest_url="http://venue", fetch=self.fetch, wall_ns=lambda: self.wall, monotonic=lambda: self.mono
        )
        self.lane.use(self.symbols)

    def fetch(self, rest: str, symbol: str) -> tuple[dict[str, Any], int | None]:
        self.fetched.append(symbol)
        answer = self.answers.get(symbol, 1_000)
        if callable(answer):
            answer = answer()
        if isinstance(answer, Exception):
            raise answer
        return {"lastUpdateId": answer, "E": 1, "T": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]}, self.used_weight

    def emit(self, row: Any) -> bool:
        if self.accept:
            self.emitted.append(dict(row))
        return self.accept

    def step(self, seconds: float | None = None) -> float:
        """One lane step, then the clock moves on by `seconds` (default: what the lane asked to wait)."""

        wait = self.lane.step(self.emit)
        moved = wait if seconds is None else seconds
        self.mono += moved
        self.wall += int(moved * 1e9)
        return wait

    def turn(self) -> None:
        """Step, waiting out the lane's pacing, until it makes its next fetch."""

        before = len(self.fetched)
        for _ in range(10):
            self.step()
            if len(self.fetched) > before:
                return
        raise AssertionError("the lane fetched nothing")

    def seen(self, symbol: str, first: int = 1) -> None:
        """The writer has seen a diff of `symbol` (so the hour's walk knows it), with no request outstanding."""

        self.lane.chain_first[symbol] = first


def test_gaps_go_first_then_the_hour_walks_every_symbol_once_in_order() -> None:
    venue = Harness()
    for symbol in venue.symbols:
        venue.seen(symbol)
    venue.lane.request("SOLUSDT", 1)
    venue.lane.request("ETHUSDT", 1)
    for _ in range(8):
        venue.step()
    # Requests in arrival order, then the hour's walk in sorted order, skipping
    # what this hour already holds; then nothing until the next hour.
    assert venue.fetched == ["SOLUSDT", "ETHUSDT", "BTCUSDT"]
    assert [row["symbol"] for row in venue.emitted] == ["SOLUSDT", "ETHUSDT", "BTCUSDT"]
    assert all(row["depth"] == 1000 and row["update_id"] == 1_000 for row in venue.emitted)

    venue.wall = HOUR_START + HOUR_NS + 1
    for _ in range(6):
        venue.step()
    assert venue.fetched[3:] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    status = venue.lane.status()
    assert (status["snapshots"], status["pending"], status["failures"], status["used_weight_1m"]) == (6, 0, 0, 40)


def test_the_hourly_walk_follows_the_re_anchor_switch_and_skips_symbols_no_diff_reached() -> None:
    venue = Harness()
    venue.seen("ETHUSDT")
    for _ in range(4):
        venue.step()
    assert venue.fetched == ["ETHUSDT"]  # BTC and SOL have sent no diff: their first one asks

    venue.lane.hourly = False
    venue.wall += HOUR_NS
    venue.lane.request("SOLUSDT", 1)
    for _ in range(4):
        venue.step()
    assert venue.fetched == ["ETHUSDT", "SOLUSDT"]


def test_fetches_are_paced_by_the_weight_budget() -> None:
    venue = Harness()
    venue.lane.request("BTCUSDT", 1)
    venue.lane.request("ETHUSDT", 1)
    assert venue.step(0.0) == 0.0
    # 1,000 weight a minute at 20 a snapshot: one every 1.2 s.
    assert venue.step(0.5) == pytest.approx(1.2)
    assert venue.fetched == ["BTCUSDT"]
    assert venue.step(0.0) == pytest.approx(0.7)
    venue.mono += 0.7
    venue.step(0.0)
    assert venue.fetched == ["BTCUSDT", "ETHUSDT"]


def test_a_request_during_the_fetch_costs_another_fetch() -> None:
    venue = Harness()
    venue.seen("BTCUSDT")

    def gap_meanwhile() -> int:
        venue.lane.request("BTCUSDT", 1_500)
        return 1_600

    venue.answers["BTCUSDT"] = gap_meanwhile
    venue.lane.request("BTCUSDT", 1)
    venue.step()
    # That answer may land on the tape ahead of the new gap's diff, so it re-bases nothing.
    assert list(venue.lane.wanted) == ["BTCUSDT"]
    venue.answers["BTCUSDT"] = 1_700
    venue.turn()
    assert venue.fetched == ["BTCUSDT", "BTCUSDT"]
    assert [row["update_id"] for row in venue.emitted] == [1_600, 1_700]
    assert not venue.lane.wanted


def test_a_rate_limit_pauses_the_whole_lane_for_its_retry_after() -> None:
    venue = Harness()
    venue.answers["BTCUSDT"] = RateLimited(429, 30.0)
    venue.lane.request("BTCUSDT", 1)
    venue.lane.request("ETHUSDT", 1)
    assert venue.step(0.0) == 30.0
    assert venue.lane.status()["throttled_until_ns"] == venue.wall + 30 * 1_000_000_000
    venue.answers.pop("BTCUSDT")
    # Nothing goes out while paused, not even another symbol.
    assert venue.step(10.0) == pytest.approx(30.0)
    assert venue.step(0.0) == pytest.approx(20.0)
    assert venue.fetched == ["BTCUSDT"]
    venue.mono += 20.0
    venue.turn()
    venue.turn()
    # The refused symbol went back behind the one that waited with it.
    assert venue.fetched == ["BTCUSDT", "ETHUSDT", "BTCUSDT"]
    assert {row["symbol"] for row in venue.emitted} == {"BTCUSDT", "ETHUSDT"}
    assert venue.lane.status()["rate_limited"] == 1


def test_a_heavy_minute_on_the_ip_waits_for_the_next_minute() -> None:
    venue = Harness(used_weight=2_000)
    venue.wall = HOUR_START + 45 * 1_000_000_000  # 15 s left in the minute
    venue.lane.request("BTCUSDT", 1)
    venue.lane.request("ETHUSDT", 1)
    venue.step(0.0)
    assert venue.step(0.0) == pytest.approx(15.0)
    assert venue.lane.status()["used_weight_1m"] == 2_000
    assert venue.lane.status()["throttled_until_ns"] == HOUR_START + 60 * 1_000_000_000
    assert venue.fetched == ["BTCUSDT"]


def test_a_failing_symbol_backs_off_without_holding_the_others() -> None:
    venue = Harness()
    venue.answers["BTCUSDT"] = OSError("connection reset")
    venue.lane.request("BTCUSDT", 1)
    venue.lane.request("ETHUSDT", 1)
    for _ in range(4):
        venue.step()
    assert venue.fetched == ["BTCUSDT", "ETHUSDT"]
    assert [row["symbol"] for row in venue.emitted] == ["ETHUSDT"]
    assert venue.lane.status()["failures"] == 1 and venue.lane.status()["pending"] == 1
    # Backoff doubles per failure in a row; a success clears it.
    venue.mono += SNAPSHOT_RETRY_MIN_SECONDS
    venue.step()
    assert venue.fetched[-1] == "BTCUSDT"
    assert venue.lane.retry_at["BTCUSDT"] == pytest.approx(venue.mono + 2 * SNAPSHOT_RETRY_MIN_SECONDS - 1.2, abs=1.3)
    venue.answers.pop("BTCUSDT")
    venue.mono += 10.0
    venue.step()
    assert [row["symbol"] for row in venue.emitted] == ["ETHUSDT", "BTCUSDT"]
    assert "BTCUSDT" not in venue.lane.retry_at and "BTCUSDT" not in venue.lane.failures_in_row


def test_a_snapshot_older_than_its_chain_is_not_emitted_and_is_fetched_again() -> None:
    venue = Harness()
    venue.answers["BTCUSDT"] = 400
    venue.lane.request("BTCUSDT", 500)
    venue.step()
    assert venue.emitted == [] and venue.lane.status()["stale"] == 1
    assert list(venue.lane.wanted) == ["BTCUSDT"]
    venue.answers["BTCUSDT"] = 520
    venue.turn()
    assert [row["update_id"] for row in venue.emitted] == [520]


def test_a_snapshot_the_writer_dropped_is_asked_for_again() -> None:
    venue = Harness()
    venue.accept = False
    venue.lane.request("BTCUSDT", 1)
    venue.step()
    assert venue.lane.status()["dropped"] == 1 and list(venue.lane.wanted) == ["BTCUSDT"]
    venue.accept = True
    venue.turn()
    assert [row["symbol"] for row in venue.emitted] == ["BTCUSDT"]
    # A dropped hourly snapshot is the hour's walk's again.
    venue.seen("ETHUSDT")
    venue.accept = False
    venue.turn()
    assert venue.fetched[-1] == "ETHUSDT" and "ETHUSDT" not in venue.lane.snapshot_hour
    venue.accept = True
    venue.turn()
    assert venue.fetched[-1] == "ETHUSDT" and venue.emitted[-1]["symbol"] == "ETHUSDT"


def test_a_request_outside_the_lanes_symbols_waits_for_the_lane_that_serves_it() -> None:
    venue = Harness(symbols=("BTCUSDT",))
    # A symbol subscribed live asks before the recorder restarts the lane with it.
    venue.lane.request("NEWUSDT", 1)
    venue.lane.request("GONEUSDT", 1)
    venue.step()
    assert venue.fetched == [] and list(venue.lane.wanted) == ["NEWUSDT", "GONEUSDT"]
    venue.lane.use(["BTCUSDT", "NEWUSDT"])
    assert list(venue.lane.wanted) == ["NEWUSDT"]
    venue.step()
    assert venue.fetched == ["NEWUSDT"]


def test_the_lane_survives_its_failures_and_ends_when_stopped() -> None:
    stop = threading.Event()
    rows: list[Any] = []
    lane = DepthSnapshotLane(rest_url="http://venue", fetch=lambda rest, symbol: 1 / 0, weight_per_minute=600_000)  # type: ignore[arg-type,return-value]
    lane.request("BTCUSDT", 1)
    thread = threading.Thread(target=lane.run, args=(["BTCUSDT"], rows.append, stop), daemon=True)
    thread.start()
    # Waited for, not slept for: a loaded host may start the thread late.
    assert _wait(lambda: lane.failures >= 1, 10.0)
    assert thread.is_alive()
    started = time.monotonic()
    stop.set()
    thread.join(2.0)
    assert not thread.is_alive() and time.monotonic() - started < 1.0
    assert rows == []

    # A restarted lane waits for the old one to leave `run` rather than walk the state beside it.
    with lane.run_lock:
        second = threading.Thread(target=lane.run, args=(["BTCUSDT"], rows.append, stop), daemon=True)
        second.start()
        second.join(1.0)
        assert not second.is_alive()  # stop is set: it gave up waiting


# ------------------------------------------------------------- the recorder


def _recorder(tmp_path: Path) -> Recorder:
    config = CaptureConfig(
        venue=VenueSettings("binance", "usdm"),
        storage=StorageSettings(root=tmp_path, queue_frames=16, status_interval_seconds=30.0),
        tiers=(Tier("core", feeds("book:1000", "book:1"), Universe("symbols", symbols=("BTCUSDT", "ETHUSDT"))),),
        topics_per_connection=8,
    )
    return Recorder(config, adapter=BinanceAdapter(rest_url="http://unused"))


class FakeLink:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_text(self, text: str) -> None:
        self.sent.append(text)

    def request_close(self) -> None:
        return None


def test_a_diff_book_gap_is_accounted_but_never_re_subscribed(tmp_path: Path) -> None:
    recorder = _recorder(tmp_path)
    adapter = recorder.adapter
    now = time.time_ns()
    (gap,) = adapter.normalize(diff("BTCUSDT", 101, 105, 100), now)
    recorder._note_books([gap], now)
    (top,) = adapter.normalize(
        json.dumps({"stream": "btcusdt@bookTicker", "data": {"s": "BTCUSDT", "u": 1, "b": "1", "B": "1", "a": "2", "A": "1"}}), now
    )
    recorder._note_books([top], now)
    # The gap is open in the hour's record, and nothing waits to be re-subscribed.
    assert recorder.resync_pending == [] and not recorder.resync_now.is_set()
    record = recorder.coverage.close(now + 1)
    assert [(entry["topic"], entry["depth"], entry["to_ns"]) for entry in record["book_gaps"]] == [
        ("btcusdt@depth@100ms", 1000, None)
    ]
    snapshot = adapter.depth_snapshots.snapshot_row("BTCUSDT", {"lastUpdateId": 103, "bids": [], "asks": []}, now + 5)
    recorder._note_books([snapshot], now + 5)
    record = recorder.coverage.close(now + 10)
    assert [(entry["from_ns"], entry["to_ns"]) for entry in record["book_gaps"]] == [(now, now + 5)]
    # Gapped again after it was based: still no re-subscribe.
    (again,) = adapter.normalize(diff("BTCUSDT", 121, 125, 120), now + 20)
    recorder._note_books([again], now + 20)
    assert recorder.resync_pending == []


def test_a_shard_resets_the_diff_chains_in_tape_order_before_it_subscribes(tmp_path: Path) -> None:
    recorder = _recorder(tmp_path)
    adapter = recorder.adapter
    shard = Shard(
        index=0,
        tier="core",
        topics=["btcusdt@bookTicker"],
        adapter=adapter,
        reader=recorder.reader,
        on_frame=recorder._on_frame,
        on_overrun=recorder._on_overrun,
    )
    link = FakeLink()
    shard.socket = link  # type: ignore[assignment]
    shard.connected = True
    adapter.normalize(diff("BTCUSDT", 1, 5, 0), RECEIVED)

    shard.update(["btcusdt@bookTicker", "btcusdt@depth@100ms", "ethusdt@bookTicker"])
    item = recorder.frames.get_nowait()
    assert item[0] == CHAIN_RESET and item[1] == ["btcusdt@depth@100ms"]
    assert recorder.frames.empty()
    assert [json.loads(text)["params"] for text in link.sent] == [["btcusdt@depth@100ms", "ethusdt@bookTicker"]]
    # A shard that adds no diff book queues nothing.
    shard.update(["btcusdt@bookTicker", "btcusdt@depth@100ms", "ethusdt@bookTicker", "solusdt@bookTicker"])
    assert recorder.frames.empty()

    # The writer hands it to the adapter, and writes nothing for it.
    adapter.depth_snapshots.wanted.clear()
    recorder._write_item(item, time.monotonic_ns(), [], [], {}, {})
    assert recorder.written_rows == 0
    assert adapter.normalize(diff("BTCUSDT", 6, 9, 5), RECEIVED)[0]["sequence_gap"] is True


def test_emit_says_whether_the_row_was_queued(tmp_path: Path) -> None:
    recorder = _recorder(tmp_path)
    row = BinanceAdapter().depth_snapshots.snapshot_row("BTCUSDT", {"lastUpdateId": 1, "bids": [], "asks": []}, RECEIVED)
    assert all(recorder.emit(row) for _ in range(16))
    assert recorder.emit(row) is False
    assert recorder.dropped_frames == 1


def test_a_depth_1000_book_row_round_trips_through_the_block_tape(tmp_path: Path) -> None:
    adapter = BinanceAdapter()
    bids = [[f"{100 - level / 100:.2f}", f"{level + 1}.5"] for level in range(1000)]
    asks = [[f"{100 + (level + 1) / 100:.2f}", f"{level + 2}.25"] for level in range(1000)]
    snapshot = adapter.depth_snapshots.snapshot_row(
        "BTCUSDT", {"lastUpdateId": 1027024, "E": 1790370834267, "T": 1790370834266, "bids": bids, "asks": asks}, RECEIVED
    )
    (delta,) = adapter.normalize(diff("BTCUSDT", 1027020, 1027030, 1027019, bids=[("99.99", "0")], asks=[("100.01", "3")]), RECEIVED + 1, MONO)
    # The longest row the feed writes fits the default 64 KB segment buffer.
    assert len(json.dumps(snapshot, separators=(",", ":"))) < 64 * 1024
    path = tmp_path / "segment-000000.lmtb"
    writer = BlockWriter(path, venue="binance", symbol="BTCUSDT", codec="zstd")
    writer.append(snapshot)
    writer.append(delta)
    writer.close()
    with BlockReader(path) as reader:
        assert [row for _, row in reader.rows()] == [snapshot, delta]
        typed = [row for _, row in reader.rows(typed=True)]
    assert typed[1].first_update_id == 1027020 and typed[1].depth == 1000
    book = Book()
    assert book.apply(typed[0]) and book.apply(typed[1])
    assert book.best_bid == (100.0, 1.5) and book.best_ask == (100.01, 3.0)
    assert 99.99 not in dict(book.levels(1000)[0]) and len(book.levels(1000)[0]) == 999


# ------------------------------------------------------------ end to end


SYMBOLS = ("BTCUSDT", "ETHUSDT")
LEVELS = 30


@dataclass
class DiffVenue:
    """A venue whose engine moves each symbol's book every few milliseconds,
    streams the diffs to whoever subscribed, and answers `/fapi/v1/depth` with
    the book as it stands. Every diff it ever made is kept, so a test can
    rebuild the venue's book at any `u`. `drop` names a symbol whose next diff
    the wire loses once."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    stop: threading.Event = field(default_factory=threading.Event)
    rng: random.Random = field(default_factory=lambda: random.Random(7))
    books: dict[str, dict[str, dict[float, str]]] = field(default_factory=dict)
    last_u: dict[str, int] = field(default_factory=dict)
    history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    initial: dict[str, dict[str, dict[float, str]]] = field(default_factory=dict)
    outboxes: dict[str, list[queue.Queue[str]]] = field(default_factory=dict)
    streamed: dict[str, int] = field(default_factory=dict)
    snapshots: dict[str, list[int]] = field(default_factory=dict)
    drop: set[str] = field(default_factory=set)
    dropped_u: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for index, symbol in enumerate(SYMBOLS):
            base = 100.0 * (index + 1)
            book = {
                "bids": {round(base - level * 0.1, 1): "1.0" for level in range(LEVELS)},
                "asks": {round(base + 0.1 + level * 0.1, 1): "1.0" for level in range(LEVELS)},
            }
            self.books[symbol] = book
            self.initial[symbol] = {side: dict(levels) for side, levels in book.items()}
            self.last_u[symbol] = 1_000 * (index + 1)
            self.history[symbol] = []

    def tick(self) -> None:
        with self.lock:
            for symbol in SYMBOLS:
                book = self.books[symbol]
                changes: dict[str, list[list[str]]] = {"bids": [], "asks": []}
                for _ in range(self.rng.randint(1, 3)):
                    side = self.rng.choice(("bids", "asks"))
                    price = self.rng.choice(sorted(book[side]) + [round(min(book[side]) - 0.1, 1)] if side == "bids" else sorted(book[side]) + [round(max(book[side]) + 0.1, 1)])
                    size = "0" if self.rng.random() < 0.3 and len(book[side]) > 5 else f"{self.rng.randint(1, 50) / 10:.1f}"
                    if size == "0":
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size
                    changes[side].append([f"{price:.1f}", size])
                previous = self.last_u[symbol]
                last = previous + self.rng.randint(1, 4)
                event = {"U": previous + 1, "u": last, "pu": previous, "b": changes["bids"], "a": changes["asks"]}
                self.last_u[symbol] = last
                self.history[symbol].append(event)
                if symbol in self.drop:
                    self.drop.discard(symbol)
                    self.dropped_u[symbol] = last
                    continue
                now_ms = time.time_ns() // 1_000_000
                text = json.dumps(
                    {"stream": f"{symbol.lower()}@depth@100ms", "data": {"e": "depthUpdate", "E": now_ms, "T": now_ms - 1, "s": symbol, **event}}
                )
                for outbox in self.outboxes.get(symbol, []):
                    outbox.put(text)
                    self.streamed[symbol] = self.streamed.get(symbol, 0) + 1

    def engine(self) -> None:
        while not self.stop.wait(0.004):
            self.tick()

    def book_at(self, symbol: str, update_id: int) -> tuple[dict[float, float], dict[float, float]]:
        bids = {price: float(size) for price, size in self.initial[symbol]["bids"].items()}
        asks = {price: float(size) for price, size in self.initial[symbol]["asks"].items()}
        with self.lock:
            events = [event for event in self.history[symbol] if event["u"] <= update_id]
        for event in events:
            for side, changes in ((bids, event["b"]), (asks, event["a"])):
                for price, size in changes:
                    if float(size) > 0:
                        side[float(price)] = float(size)
                    else:
                        side.pop(float(price), None)
        return bids, asks

    def depth(self, symbol: str) -> dict[str, Any]:
        with self.lock:
            book = self.books[symbol]
            self.snapshots.setdefault(symbol, []).append(self.last_u[symbol])
            now_ms = time.time_ns() // 1_000_000
            return {
                "lastUpdateId": self.last_u[symbol],
                "E": now_ms,
                "T": now_ms - 1,
                "bids": [[f"{price:.1f}", size] for price, size in sorted(book["bids"].items(), reverse=True)],
                "asks": [[f"{price:.1f}", size] for price, size in sorted(book["asks"].items())],
            }

    def handle(self, connection: ServerConnection) -> None:
        path = connection.request.path if connection.request is not None else ""
        outbox: queue.Queue[str] = queue.Queue()
        mine: set[str] = set()
        closed = threading.Event()

        def listen() -> None:
            try:
                for raw in connection:
                    message = json.loads(raw)
                    params = list(message.get("params") or [])
                    with self.lock:
                        for stream in params:
                            pair, _, kind = stream.partition("@")
                            if path != "/stream" or kind != "depth@100ms":
                                continue
                            symbol = pair.upper()
                            boxes = self.outboxes.setdefault(symbol, [])
                            if message.get("method") == "SUBSCRIBE" and symbol not in mine:
                                boxes.append(outbox)
                                mine.add(symbol)
                            elif message.get("method") == "UNSUBSCRIBE" and symbol in mine:
                                boxes.remove(outbox)
                                mine.discard(symbol)
                    connection.send(json.dumps({"result": None, "id": message["id"]}))
            except ConnectionClosed:
                pass
            finally:
                with self.lock:
                    for symbol in mine:
                        self.outboxes[symbol].remove(outbox)
                closed.set()

        threading.Thread(target=listen, daemon=True).start()
        try:
            while not closed.is_set() and not self.stop.is_set():
                try:
                    connection.send(outbox.get(timeout=0.05))
                except queue.Empty:
                    continue
        except ConnectionClosed:
            pass


def _rest_handler(venue: DiffVenue) -> type[http.server.BaseHTTPRequestHandler]:
    class Rest(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path, _, query = self.path.partition("?")
            params = dict(part.split("=", 1) for part in query.split("&") if "=" in part)
            payload: Any
            if path == "/fapi/v1/depth" and params.get("symbol") in SYMBOLS and params.get("limit") == "1000":
                payload = venue.depth(params["symbol"])
            elif path == "/fapi/v1/exchangeInfo":
                payload = {"symbols": []}
            elif path in ("/fapi/v1/premiumIndex", "/fapi/v1/ticker/24hr", "/fapi/v1/ticker/bookTicker"):
                payload = []
            else:
                payload = {"code": -1, "msg": "no such path"}
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-MBX-USED-WEIGHT-1M", "20")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: Any) -> None:
            return None

    return Rest


def _wait(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def test_the_recorder_records_the_full_book_and_heals_a_lost_diff_with_a_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every assertion below is about one UTC hour: a run that crossed a
    # boundary would split the gaps across two coverage records and take the
    # new hour's re-anchoring snapshots. Start clear of one.
    seconds_left = 3_600 - time.time() % 3_600
    if seconds_left < 60:
        time.sleep(seconds_left + 1)
    venue = DiffVenue()
    threading.Thread(target=venue.engine, daemon=True).start()
    rest = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _rest_handler(venue))
    threading.Thread(target=rest.serve_forever, daemon=True).start()
    root = tmp_path / "forward-market-binance"
    try:
        with serve(venue.handle, "127.0.0.1", 0) as server:
            threading.Thread(target=server.serve_forever, daemon=True).start()
            port = server.socket.getsockname()[1]
            config = CaptureConfig(
                venue=VenueSettings("binance", "usdm"),
                storage=StorageSettings(root=root, segment_max_mb=4.0, status_interval_seconds=0.3, min_free_disk_gb=0.01),
                tiers=(Tier("full", (Feed("book", "1000"),), Universe("symbols", symbols=SYMBOLS)),),
                topics_per_connection=4,
                snapshot_cadence="hour",
            )
            adapter = BinanceAdapter(ws_url=f"ws://127.0.0.1:{port}", rest_url=f"http://127.0.0.1:{rest.server_port}")
            # Ten snapshots a second rather than the venue's pace: the test is about order, not weight.
            adapter.depth_snapshots.weight_per_minute = 12_000
            recorder = Recorder(config, adapter=adapter)
            monkeypatch.setattr(recorder, "_install_signals", lambda: None)
            outcome: dict[str, bool] = {}

            lane = adapter.depth_snapshots

            def script() -> None:
                # Based once from the connect's gap, then a diff lost on the
                # wire, healed by the snapshot that gap asks for. Each wait is
                # for the snapshot handed to the writer (`snapshot_hour`,
                # `snapshots`), not for the venue having served it: the queue
                # is the tape's order and shutdown drains it, whereas a fixed
                # sleep after the venue answered let a starved lane thread
                # queue the basing snapshot behind the lost diff.
                outcome["based"] = _wait(lambda: all(symbol in lane.snapshot_hour for symbol in SYMBOLS), 20.0)
                venue.drop.add("BTCUSDT")
                outcome["healed"] = _wait(lambda: "BTCUSDT" in venue.dropped_u and lane.snapshots >= 3, 20.0)
                # A tail of diffs past the heal, so the book is checked chaining on from it.
                streamed = venue.streamed.get("BTCUSDT", 0)
                _wait(lambda: venue.streamed.get("BTCUSDT", 0) >= streamed + 20, 5.0)
                recorder.stop.set()

            threading.Thread(target=script, daemon=True).start()
            recorder.run()
            venue.stop.set()
            server.shutdown()
    finally:
        venue.stop.set()
        rest.shutdown()
    assert outcome == {"based": True, "healed": True}

    status = json.loads((root / "status.json").read_text(encoding="utf-8"))
    assert status["malformed_frames"] == 0 and status["dropped_frames"] == 0
    lane = status["lanes_status"]["depth_snapshots"]
    assert lane["symbols"] == 2 and lane["snapshots"] >= 3 and lane["failures"] == 0 and lane["used_weight_1m"] == 20
    assert status["shards"][0]["resyncs"] == 0  # never re-subscribed

    recovered = pack.recover_idle_root(pack.Tape("binance-usdm", root, "box:x"), timeout=60.0, dry_run=False)
    assert recovered["failed"] == 0
    source = HostRoot(root)
    hours = source.hours()
    for symbol in SYMBOLS:
        rows = [row for row in iter_rows(source, hours, symbols=[symbol], kinds=["orderbook_snapshot", "orderbook_delta"])]
        assert rows and all(isinstance(row, BookRow) and row.depth == 1000 for row in rows)
        book = Book()
        validity = [book.apply(row) for row in rows]  # type: ignore[arg-type]
        # The tape opens with diffs the first snapshot has not reached (held,
        # not applied), and ends with the venue's own book.
        assert not rows[0].snapshot and rows[0].sequence_gap  # type: ignore[union-attr]
        assert book.valid, symbol
        bids, asks = venue.book_at(symbol, book.last_update_id)
        assert book.levels(1000) == (
            sorted(bids.items(), key=lambda level: -level[0]),
            sorted(asks.items()),
        ), symbol
        gaps = [index for index, row in enumerate(rows) if not row.snapshot and row.sequence_gap]  # type: ignore[union-attr]
        snapshots = [index for index, row in enumerate(rows) if row.snapshot]  # type: ignore[union-attr]
        if symbol == "ETHUSDT":
            assert len(gaps) == 1
            continue
        # BTC: the connect's gap and the lost diff's, each followed by a snapshot that made the book good again.
        assert len(gaps) == 2
        lost = gaps[1]
        # Its `pu` is the lost diff's `u`, which the tape never saw.
        assert rows[lost].previous_update_id == venue.dropped_u["BTCUSDT"]  # type: ignore[union-attr]
        assert validity[lost - 1] and not validity[lost]
        healed = next(index for index in snapshots if index > lost)
        assert validity[healed] and all(validity[healed:])

    # The hour's record: the gaps of the diff book, opened at a flagged diff and
    # closed at the snapshot row, against the diff stream's topic.
    gaps = [gap for coverage in iter_coverage(source, hours) for gap in coverage["book_gaps"]]
    btc = sorted((gap for gap in gaps if gap["symbol"] == "BTCUSDT"), key=lambda gap: gap["from_ns"])
    assert [(gap["topic"], gap["depth"]) for gap in btc] == [("btcusdt@depth@100ms", 1000)] * 2
    assert all(gap["to_ns"] is not None and gap["to_ns"] > gap["from_ns"] for gap in btc)
    btc_rows = list(iter_rows(source, hours, symbols=["BTCUSDT"], kinds=["orderbook_snapshot", "orderbook_delta"], typed=False))
    flagged = [row["local_receive_ts_ns"] for row in btc_rows if row["sequence_gap"]]
    based = [row["local_receive_ts_ns"] for row in btc_rows if row["kind"] == "orderbook_snapshot"]
    assert [gap["from_ns"] for gap in btc] == flagged
    # Closed by the snapshot that followed: a lane row written behind a later
    # stream frame carries that frame's stamp (`SegmentWriter.append`), so at or before it.
    for gap, start in zip(btc, flagged):
        assert start < gap["to_ns"] <= min(stamp for stamp in based if stamp >= start)

    ledger = build_ledger(source, hours, symbols=list(SYMBOLS), feeds=["book:1000"], read_rows=True)
    for cell in ledger.cells:
        assert cell.records > 0
        book_gaps = [reason for reason in cell.reasons if reason.code == "book_gap"]
        assert [reason.detail for reason in book_gaps] == [f"{cell.symbol.lower()}@depth@100ms"] * (2 if cell.symbol == "BTCUSDT" else 1)
        assert set(cell.reason_codes) <= {"recorder_down", "tier_membership_partial", "shard_disconnected", "book_gap"}
