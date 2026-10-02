"""The recorder: tiers of symbols and feeds on one venue, written as the tape.

One process records one venue. It reads the capture config, resolves each
tier's universe, subscribes the union of the tiers' feeds over several
websocket connections (shards), and writes every normalized row through
`storage.SegmentWriter`.

Universes that change while the recorder runs are re-read on every maintenance
tick. The `listed` kind follows the venue's table snapshots; the live kinds
(ranked turnover and movers, funding either side of a line, turnover and
volume surges, price bursts, open-interest jumps) follow the ticker stream the
recorder is already writing, so a name whose funding rate collapses or whose
turnover explodes gets its deep feeds within one tick, not at the next daily
snapshot. Topics are added to and removed from the live
connections in place; a connection only reconnects when the venue drops it.

Every received byte is metered per tier and per feed. With a budget in the
config, the recorder projects a month from its last day of bytes and, when the
projection is over the allowance, gives up the configured `tier:feed` pairs in
order, one an hour, restoring them in reverse once under pace.

Processes and threads: the sockets are read in a process of their own, the
reader (`market_tape.reader`), which connects every shard, keeps it alive,
and stamps every frame as it is read; the writer here parses and serialises
every frame, and on one interpreter lock the two took turns, which on the
capture host cost more CPU than either's work and delayed every stamp by the
writer's turn. In this process: one intake thread moving the reader's
frames into the capture queue, one thread per shard for its connection's
lifetime (open, subscribe, backoff), one writer, one compressor, one
maintainer (status, universes, budget, re-anchoring), one meta writer (the
venue tables and the hourly coverage records), one pruner (retention), one
resyncer (re-subscribing gapped book topics), plus whatever side lanes the
venue adapter starts. Frames cross from the intake to the writer
through one bounded queue, which absorbs a writer slower than the wire for
as long as its bounds allow; past them the reader holds each shard's frames
and, when they have waited too long, drops them, counts them in the status
file, and the shard reconnects for fresh snapshots. A run holds
`<root>/.recorder.lock` from before the compressor starts until after the last
status write, so a second recorder on one root refuses to start rather than
race this one's raw files; `market_tape pack` takes the same lock and finishes
the raw segments only on a root no recorder holds.

Retention and the `_meta` writes have their own threads because
`status.json` is this unit's heartbeat: the maintainer writes it every
`status_interval_seconds`, a pass over a tape holding days of hours across
hundreds of symbols takes longer than the watchdog's freshness limit, and a
table snapshot or coverage record is a REST read or a zstd run and several
fsyncs, each of which waits behind every dirty page the writer has on the
filesystem.
"""

from __future__ import annotations

import ctypes
import dataclasses
import functools
import json
import logging
import os
import queue
import random
import select
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from dataclasses import dataclass, field
from itertools import accumulate
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from market_tape import reader
from market_tape.config import (
    DEFAULT_STICKY_HOURS,
    RANKED_KINDS,
    BudgetSettings,
    CaptureConfig,
    ConfigError,
    Feed,
    Tier,
    Universe,
    load_symbol_file,
    validate_symbols,
)
from market_tape.coverage import CoverageFold
from market_tape.jsonfast import FAST as FAST_JSON
from market_tape.schema import (
    KIND_BOOK_DELTA,
    KIND_BOOK_SNAPSHOT,
    KIND_TICKER,
    KIND_TRADE,
    SCHEMA_VERSION,
    feed_of_row,
)
from market_tape.storage import (
    Compressor,
    CoverageRecords,
    Manifest,
    Retention,
    SegmentWriter,
    Snapshots,
    atomic_json,
    lock_root,
    unlock_root,
    utc_day_hour,
)
from market_tape.venues import VenueAdapter, adapter_for, validate_config

#: A shard's wait before reconnecting: the minimum after a connection that
#: lived `RECONNECT_HEALTHY_SECONDS`, doubling per short-lived one up to the
#: maximum, each wait drawn from its upper half.
RECONNECT_BACKOFF_MIN_SECONDS = 2.0
RECONNECT_BACKOFF_MAX_SECONDS = 60.0
RECONNECT_HEALTHY_SECONDS = 60.0
#: The intake's tick while frames flow, the most one read of the events
#: pipe takes, and how long one offer to a full queue waits before it looks
#: again whether the recorder is closing. Frames carry the reader's stamps,
#: so the tick delays the disk and never the tape's clocks; on the capture
#: host a wake-up costs ~80 µs of CPU, and the pipe and the reader's own
#: buffer hold a tick of a burst many times over.
INTAKE_TICK_SECONDS = 0.02
INTAKE_READ_BYTES = 1024 * 1024
INTAKE_PUT_SECONDS = 0.5
#: What an open waits past the reader's own connect timeout before it gives up on the reader.
OPEN_GRACE_SECONDS = 5.0
#: The pause before a reader that exited unasked is started again, and how
#: long an exited reader has to be reaped before it is killed.
READER_RESPAWN_SECONDS = 1.0
READER_EXIT_SECONDS = 5.0
#: The writer's sleep when it finds the queue empty. It polls rather than
#: waits on the queue, and long enough to find a batch of hundreds: the same
#: frames cost the writer five times the CPU taken twenty at a time as
#: taken a thousand at a time, and frames carry the reader's stamps, so this
#: delays the disk and never the tape's clocks.
WRITE_IDLE_SECONDS = 0.05
#: A frame the writer cannot record is counted every time and logged at most this often.
MALFORMED_LOG_SECONDS = 60.0
#: Subscription frames one shard sends leave this far apart: Binance drops a
#: connection that sends it more than ten messages a second, pings and pongs
#: included, and Bybit refuses a burst. Both venues get this spacing.
LIVE_MESSAGE_SPACING_SECONDS = 0.12
MINUTE_NS = 60 * 1_000_000_000
DAY_NS = 24 * 60 * MINUTE_NS
MONTH_SECONDS = 30 * 86_400
#: An hour of received bytes before a monthly projection means anything.
BUDGET_MIN_WINDOW_NS = 60 * MINUTE_NS
#: Book topics re-anchored per maintenance tick, across every shard. The
#: hourly pass is spread rather than sent at once: a few hundred symbols
#: re-subscribing in one breath is a burst of snapshots and a burst of
#: missing deltas. At 40 a tick and a 30-second tick, 500 topics take about
#: six minutes of the hour.
REANCHOR_TOPICS_PER_TICK = 80
#: Topics dropped and re-taken together. One venue message carries ten, so a
#: chunk is one message each way and a symbol's gap is one round trip.
REANCHOR_CHUNK = 10
#: Seconds between routine retention passes, on the pruner thread. This is the
#: housekeeping cadence only: `min_free_disk_gb` is free space on the whole
#: filesystem, which anything sharing it can cross, and every second the
#: recorder is under that floor is thrown-away tape. So a maintenance tick that
#: finds the disk unwritable sets `prune_now` and the pruner runs at once
#: instead of sleeping out the rest of this interval.
RETENTION_INTERVAL_SECONDS = 300.0
LANES = "lanes"
#: What the writer takes. A socket frame is queued as the reader's event, the
#: events pipe's bytes (`reader.EVENT`, then the payload), one object whose
#: header holds its link and both stamps: a backlog of a million frames is a
#: million of these, and every small object it held besides (a tuple, two
#: stamps) was one more for pymalloc to leave scattered across arenas it
#: cannot give back. `("frame", raw, received_ns, tier, mono_ns)` is a frame
#: handed over whole, `("rows", rows, received_ns, LANES, 0)` rows a side lane
#: built itself. `mono_ns` is the host's monotonic clock at the same instant
#: as `received_ns`; 0 when none was read.
QueueItem = bytes | tuple[str, Any, int, str, int]
#: What a queued event holds beyond its `len()`, as `sys.getsizeof` reads it on
#: CPython 3.11 and 3.12: the bytes object's header 33, up to 15 of the
#: allocator's rounding to 16, and the deque's 8-byte slot.
QUEUE_EVENT_OVERHEAD_BYTES = 56
#: What a queued tuple holds beyond a frame's `len(raw)`, as `sys.getsizeof`
#: reads it on CPython 3.11: the 5-tuple 80, the bytes object's header 33,
#: `received_ns` 36, `mono_ns` 32, and the deque's 8-byte slot, 189 in all
#: (tracemalloc reads 188 per item), rounded up. The kind and tier strings are shared.
QUEUE_ITEM_OVERHEAD_BYTES = 192
#: A lane row's estimate. A one-row funding or account-ratio item, tuple, list,
#: row dict and values, reads 516-537 B under tracemalloc; this plus the item
#: overhead counts it at over twice that, room for a wider row.
QUEUE_ROW_BYTES = 1024
#: A queued event's length, kind and link: what the intake routes it by.
EVENT_HEAD = struct.Struct("<IBI")
#: A queued event's wall stamp, next after `EVENT_HEAD` in `reader.EVENT`.
EVENT_WALL = struct.Struct("<q")


def received_ns_of(item: QueueItem) -> int:
    if isinstance(item, bytes):
        return EVENT_WALL.unpack_from(item, EVENT_HEAD.size)[0]
    return item[2]


def queue_item_bytes(item: QueueItem | None) -> int:
    """The memory one queued item holds, as `FrameQueue`'s byte bound counts it."""

    if item is None:
        return 0
    if isinstance(item, bytes):
        return QUEUE_EVENT_OVERHEAD_BYTES + len(item)
    if item[0] == "frame":
        return QUEUE_ITEM_OVERHEAD_BYTES + len(item[1])
    return QUEUE_ITEM_OVERHEAD_BYTES + QUEUE_ROW_BYTES * len(item[1])


class Overrun(queue.Full):
    """`FrameQueue.put_batch` ran out of time with `dropped` of its items not taken."""

    def __init__(self, dropped: int) -> None:
        super().__init__(dropped)
        self.dropped = dropped


class FrameQueue(queue.Queue[Any]):
    """A bounded FIFO queue that the intake and the side lanes fill and the
    writer drains, a batch at a time.

    Bounded twice: `maxsize` items and `max_bytes` of `queue_item_bytes`, each
    unbounded at 0. A frame's size is the venue's, so an item count alone lets a
    backlog of large book snapshots past the process's memory ceiling."""

    def __init__(self, maxsize: int = 0, max_bytes: int = 0) -> None:
        super().__init__(maxsize)
        self.max_bytes = max_bytes
        #: Maintained under the queue's lock, and only while `max_bytes` bounds.
        self.queued_bytes = 0

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        self.put_batch([item], timeout=timeout if block else 0.0)

    def _get(self) -> Any:
        item = self.queue.popleft()
        if self.max_bytes > 0:
            self.queued_bytes -= queue_item_bytes(item)
        return item

    def put_batch(self, items: list[Any], timeout: float | None = None) -> None:
        """Add a batch in order, as much as has room under each lock acquisition,
        waiting for room for the rest. Room is both bounds at once, except that an
        empty queue always takes the next item, so one frame larger than
        `max_bytes` cannot wedge its shard. A batch may be larger than the queue.

        Raises `Overrun` when `timeout` runs out; the items already taken stay queued."""
        if not items:
            return
        # Summed before the lock: items[a:b] hold ends[b - 1] - ends[a - 1] bytes.
        ends = list(accumulate(map(queue_item_bytes, items))) if self.max_bytes > 0 else None
        with self.not_full:
            endtime = None if timeout is None else time.monotonic() + timeout
            taken = 0
            while True:
                room = len(items) - taken
                if self.maxsize > 0:
                    room = min(room, self.maxsize - len(self.queue))
                if room > 0 and ends is not None:
                    spent = ends[taken - 1] if taken else 0
                    room = bisect_right(ends, spent + self.max_bytes - self.queued_bytes, taken, taken + room) - taken
                    if room == 0 and not self.queue:
                        room = 1
                    if room > 0:
                        self.queued_bytes += ends[taken + room - 1] - spent
                if room > 0:
                    self.queue.extend(items[taken : taken + room])
                    taken += room
                    self.unfinished_tasks += room
                    self.not_empty.notify()
                    if taken == len(items):
                        return
                if endtime is None:
                    self.not_full.wait()
                    continue
                remaining = endtime - time.monotonic()
                if remaining <= 0.0:
                    raise Overrun(len(items) - taken)
                self.not_full.wait(remaining)

    def get_batch(self, max_items: int = 500, timeout: float = 1.0) -> list[Any]:
        with self.not_empty:
            if not self._qsize():
                if timeout is not None:
                    endtime = time.monotonic() + timeout
                    while not self._qsize():
                        remaining = endtime - time.monotonic()
                        if remaining <= 0.0:
                            raise queue.Empty
                        self.not_empty.wait(remaining)
                else:
                    while not self._qsize():
                        self.not_empty.wait()
            items: list[Any] = []
            q = self.queue
            n = min(len(q), max_items)
            if q and q[0] is None:
                items.append(self._get())
            else:
                popleft = q.popleft
                for _ in range(n):
                    item = popleft()
                    if item is None:
                        q.appendleft(None)
                        break
                    items.append(item)
                if self.max_bytes > 0:
                    self.queued_bytes -= sum(map(queue_item_bytes, items))
            # One wake per slot freed, as `Queue.get` gives: a drain of hundreds
            # waking one shard leaves the rest blocked on room that is there.
            if getattr(self.not_full, "_waiters", None):
                self.not_full.notify(len(items))
            return items


def _malloc_trim() -> Callable[[int], int] | None:
    """glibc's `malloc_trim`; None under a C library without one."""

    try:
        trim = ctypes.CDLL(None).malloc_trim
    except (OSError, AttributeError):
        return None
    trim.argtypes = [ctypes.c_size_t]
    trim.restype = ctypes.c_int
    return trim


#: glibc returns a thread's arena to the kernel only from its top, so what a
#: backlog left free in the middle (the intake's megabyte reads, every frame
#: over pymalloc's 512 B) stays resident until trimmed. pymalloc's own
#: arenas are not glibc's, and a trim leaves them.
MALLOC_TRIM = _malloc_trim()


def shard_topics(topics: list[str], per_connection: int, group: Callable[[str], str] | None = None) -> list[list[str]]:
    """Chunk topics into connections; with `group`, topics of different groups
    never share a connection, and each group keeps its order."""

    if per_connection <= 0:
        raise ValueError("topics per connection must be positive")
    grouped: dict[str, list[str]] = {}
    for topic in topics:
        grouped.setdefault(group(topic) if group else "", []).append(topic)
    return [
        members[start : start + per_connection]
        for members in grouped.values()
        for start in range(0, len(members), per_connection)
    ]


# ----------------------------------------------------------------- metering


@dataclass(slots=True)
class ClockWindow:
    """One reading, sampled per row or per frame, summarised over one status interval.

    As the recorder's `clock`: every book, trade, ticker and liquidation frame
    carries the venue's stamp of its send (Bybit `ts`, Binance `E`); the
    difference from the moment this host received it is the venue's own
    dispatch after that stamp, the path one way, and whatever this host's
    clock is wrong by. The path is the venue's distance: a millisecond or two
    to Bybit in Singapore, tens one way to Binance in Tokyo. The
    venue's queue and TCP's recovery of a lost segment sit in the tail and
    drag the mean, so `Recorder._clock_status` gives each feed's median and
    99th percentile too (`Quantiles`): a minimum below zero, or a median
    that moves while the path (`shards[].link`) holds, is the clock, and the
    clock is what every bar built from this tape is cut on.

    As the recorder's `queue_wait`: the monotonic time from a frame's arrival
    (its stamp, taken by the reader) to the writer thread taking it: the
    kernel's hold before the read where the kernel says, the reader's hand,
    the events pipe, the intake's tick, and the capture queue. The row's
    `local_receive_ts_ns` was stamped before that wait, so the tape does not
    carry it; this is where it shows.
    """

    samples: int = 0
    total_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    prefix: str = "skew"

    def take(self, skew_ms: float) -> None:
        if self.samples == 0:
            self.min_ms = self.max_ms = skew_ms
        elif skew_ms < self.min_ms:
            self.min_ms = skew_ms
        elif skew_ms > self.max_ms:
            self.max_ms = skew_ms
        self.samples += 1
        self.total_ms += skew_ms

    def take_all(self, readings: list[float]) -> None:
        if not readings:
            return
        low, high = min(readings), max(readings)
        if self.samples == 0:
            self.min_ms, self.max_ms = low, high
        else:
            if low < self.min_ms:
                self.min_ms = low
            if high > self.max_ms:
                self.max_ms = high
        self.samples += len(readings)
        self.total_ms += sum(readings)

    def merge(self, other: ClockWindow) -> None:
        if other.samples == 0:
            return
        if self.samples == 0:
            self.min_ms = other.min_ms
            self.max_ms = other.max_ms
        else:
            if other.min_ms < self.min_ms:
                self.min_ms = other.min_ms
            if other.max_ms > self.max_ms:
                self.max_ms = other.max_ms
        self.samples += other.samples
        self.total_ms += other.total_ms

    def status(self) -> dict[str, Any]:
        names = (f"{self.prefix}_mean_ms", f"{self.prefix}_min_ms", f"{self.prefix}_max_ms")
        if self.samples == 0:
            return {"samples": 0, **{name: None for name in names}}
        return {
            "samples": self.samples,
            names[0]: round(self.total_ms / self.samples, 3),
            names[1]: round(self.min_ms, 3),
            names[2]: round(self.max_ms, 3),
        }


#: Upper edges, in ms, of `Quantiles`' buckets: a quarter of a millisecond to
#: 20 ms, one to 200 ms, ten to 2 s, a hundred to 20 s. A reading at or below
#: zero counts in the first, one past 20 s in the last.
QUANTILE_EDGES_MS = (
    tuple(quarter / 4 for quarter in range(81))
    + tuple(float(ms) for ms in range(21, 201))
    + tuple(float(ms) for ms in range(210, 2_001, 10))
    + tuple(float(ms) for ms in range(2_100, 20_001, 100))
)
#: The row kinds whose clocks `clock.feeds` reads, by feed class, and the
#: field holding the venue's stamp from before its send stamp.
CLOCK_FEEDS = {
    KIND_BOOK_SNAPSHOT: ("book", "exchange_engine_ts_ns"),
    KIND_BOOK_DELTA: ("book", "exchange_engine_ts_ns"),
    KIND_TRADE: ("trades", "exchange_ts_ns"),
}


@dataclass(slots=True)
class Quantiles:
    """Readings in ms over one status interval, counted in fixed buckets, so a
    quantile is read to the bucket holding it and a tail cannot move it."""

    counts: list[int] = field(default_factory=lambda: [0] * (len(QUANTILE_EDGES_MS) + 1))
    samples: int = 0

    def take_all(self, readings: list[float]) -> None:
        """Count a batch: sorted, a bucket's share is the batch cut at the
        bucket's upper edge, so the cost is the buckets the batch spans, not
        its length."""

        if not readings:
            return
        ordered = sorted(readings)
        edges = QUANTILE_EDGES_MS
        counts = self.counts
        below = 0
        for index in range(bisect_left(edges, ordered[0]), bisect_left(edges, ordered[-1]) + 1):
            upto = bisect_right(ordered, edges[index]) if index < len(edges) else len(ordered)
            counts[index] += upto - below
            below = upto
        self.samples += len(ordered)

    def quantile(self, fraction: float) -> float | None:
        """The upper edge of the bucket holding `fraction` of the readings."""

        if self.samples == 0:
            return None
        rank = fraction * self.samples
        seen = 0
        for index, count in enumerate(self.counts):
            seen += count
            if seen >= rank:
                break
        return QUANTILE_EDGES_MS[min(index, len(QUANTILE_EDGES_MS) - 1)]


class ByteMeter:
    """Bytes received, by key, per minute over the last day, plus lifetime totals."""

    def __init__(self, started_ns: int) -> None:
        self.started_ns = started_ns
        self.lock = threading.Lock()
        self.totals: dict[str, int] = {}
        self.minutes: dict[str, deque[tuple[int, int]]] = {}

    def add(self, key: str, count: int, now_ns: int) -> None:
        minute = now_ns // MINUTE_NS
        with self.lock:
            self.totals[key] = self.totals.get(key, 0) + count
            bucket = self.minutes.setdefault(key, deque())
            if bucket and bucket[-1][0] == minute:
                bucket[-1] = (minute, bucket[-1][1] + count)
            else:
                bucket.append((minute, count))
            while bucket and bucket[0][0] <= minute - 1440:
                bucket.popleft()

    def add_batch(self, counts: Mapping[str, int], now_ns: int) -> None:
        minute = now_ns // MINUTE_NS
        with self.lock:
            for key, count in counts.items():
                self.totals[key] = self.totals.get(key, 0) + count
                bucket = self.minutes.setdefault(key, deque())
                if bucket and bucket[-1][0] == minute:
                    bucket[-1] = (minute, bucket[-1][1] + count)
                else:
                    bucket.append((minute, count))
                while bucket and bucket[0][0] <= minute - 1440:
                    bucket.popleft()

    def last_day(self, key: str, now_ns: int) -> int:
        minute = now_ns // MINUTE_NS
        with self.lock:
            bucket = self.minutes.get(key)
            if not bucket:
                return 0
            return sum(count for stamp, count in bucket if stamp > minute - 1440)

    def window_ns(self, now_ns: int) -> int:
        """How much of the last day this meter has actually seen."""

        return max(0, min(now_ns - self.started_ns, DAY_NS))

    def keys(self, prefix: str) -> list[str]:
        with self.lock:
            return sorted(key for key in self.minutes if key.startswith(prefix))


# --------------------------------------------------------------- live state


@dataclass(slots=True)
class SymbolLive:
    funding_rate: float | None = None
    turnover_24h: float | None = None
    price_change_24h: float | None = None
    price: float | None = None
    open_interest: float | None = None

    def copy(self) -> "SymbolLive":
        return dataclasses.replace(self)


@dataclass(frozen=True, slots=True)
class Sample:
    """One remembered ticker reading, for the windowed universes."""

    ns: int
    turnover_24h: float | None
    price: float | None
    open_interest: float | None


SAMPLE_SPACING_NS = 60 * 1_000_000_000


class LiveState:
    """What the ticker stream, and the last table snapshot, say about every symbol.
    With `history_ns` > 0 it also remembers one sample a minute per symbol that
    far back, so a universe can ask what a name looked like an hour ago."""

    def __init__(self, history_ns: int = 0) -> None:
        self.lock = threading.Lock()
        self.symbols: dict[str, SymbolLive] = {}
        self.history: dict[str, deque[Sample]] = {}
        self.history_ns = max(0, int(history_ns))
        self.baseline_turnover: dict[str, float] = {}

    def observe(self, symbol: str, values: Mapping[str, Any], received_ns: int) -> None:
        with self.lock:
            live = self.symbols.get(symbol)
            if live is None:
                live = self.symbols[symbol] = SymbolLive()
            if "funding_rate" in values:
                live.funding_rate = float(values["funding_rate"])
            if "turnover_24h" in values:
                live.turnover_24h = float(values["turnover_24h"])
            if "price_change_24h_pct" in values:
                live.price_change_24h = float(values["price_change_24h_pct"])
            if "mark_price" in values:
                live.price = float(values["mark_price"])
            elif "last_price" in values and live.price is None:
                live.price = float(values["last_price"])
            if "open_interest" in values:
                live.open_interest = float(values["open_interest"])
            if self.history_ns:
                self._sample(symbol, live, received_ns)

    def _sample(self, symbol: str, live: SymbolLive, now_ns: int) -> None:
        samples = self.history.setdefault(symbol, deque())
        if samples and now_ns - samples[-1].ns < SAMPLE_SPACING_NS:
            return
        samples.append(Sample(now_ns, live.turnover_24h, live.price, live.open_interest))
        # One sample older than the window stays, so a lookback exactly at the window's edge resolves.
        while len(samples) > 1 and samples[1].ns <= now_ns - self.history_ns:
            samples.popleft()

    def earlier(self, symbol: str, at_ns: int) -> Sample | None:
        """The newest remembered sample taken at or before `at_ns`; None when the
        history does not reach back that far."""

        with self.lock:
            samples = self.history.get(symbol)
            if not samples or samples[0].ns > at_ns:
                return None
            found = None
            for sample in samples:
                if sample.ns > at_ns:
                    break
                found = sample
            return found

    def seed(self, funding: Mapping[str, float], turnovers: Mapping[str, float]) -> None:
        """A table snapshot: current funding and turnover for every listed name,
        and the turnover every later surge is measured against."""

        with self.lock:
            for symbol, rate in funding.items():
                self.symbols.setdefault(symbol, SymbolLive()).funding_rate = rate
            for symbol, turnover in turnovers.items():
                self.symbols.setdefault(symbol, SymbolLive()).turnover_24h = turnover
            self.baseline_turnover = dict(turnovers)

    def view(self) -> tuple[dict[str, SymbolLive], dict[str, float]]:
        with self.lock:
            return ({symbol: live.copy() for symbol, live in self.symbols.items()}, dict(self.baseline_turnover))


# ------------------------------------------------------------------- shards


class RemoteLink:
    """One shard's websocket as the recorder holds it. The reader process owns
    the socket (`market_tape.reader`); through this the shard's thread asks
    for text to be sent and for the close, and learns when it opened and
    when it ended."""

    def __init__(self, reader: ReaderProcess, ident: int, *, tier: str, shard: Shard) -> None:
        self.reader = reader
        self.ident = ident
        self.tier = tier
        self.shard = shard
        reader.tiers[ident] = tier
        self.opened = threading.Event()
        #: Set once the socket is closed and every frame read from it is queued
        #: or counted: until then a reconnect's snapshot could reach the writer
        #: ahead of this connection's last deltas.
        self.ended = threading.Event()
        #: Why it never opened, or why the reader ended it unasked.
        self.error = ""
        self.close_requested = False

    def send_text(self, text: str) -> None:
        if self.close_requested or self.ended.is_set():
            raise ConnectionError("the connection is closing")
        self.reader.command({"op": "send", "link": self.ident, "text": text})

    def request_close(self) -> None:
        """Ask the reader to close the socket; `ended` follows once its frames are queued."""

        if self.close_requested or self.ended.is_set():
            return
        self.close_requested = True
        try:
            self.reader.command({"op": "close", "link": self.ident})
        except ConnectionError:
            # The reader is gone, and every link it held ends with it.
            pass


def spawn_reader(commands: int, events: int) -> subprocess.Popen[bytes]:
    """`python -m market_tape.reader` on its ends of the two pipes, which this
    process then closes: the reader's exit is the events pipe's end of file."""

    package_root = str(Path(__file__).resolve().parent.parent)
    search = os.environ.get("PYTHONPATH")
    environment = dict(os.environ, PYTHONPATH=package_root + (os.pathsep + search if search else ""))
    try:
        return subprocess.Popen(
            [sys.executable, "-m", "market_tape.reader", "--commands", str(commands), "--events", str(events)],
            pass_fds=(commands, events),
            stdin=subprocess.DEVNULL,
            env=environment,
        )
    finally:
        os.close(commands)
        os.close(events)


class ReaderProcess:
    """The recorder's side of the reader process.

    It starts the reader, writes it commands, and on the `tape-intake`
    thread moves what the reader read into the capture queue in the order it
    was read. The intake wakes on a tick while frames flow: the frames carry
    the reader's stamps, so the tick delays the queue and never the tape's
    clocks. When the queue has no room the intake waits and the events pipe
    fills; the reader then holds each shard's frames up to
    `reader.SOCKET_QUEUE_FRAMES` and overruns a shard whose oldest has waited
    `reader.QUEUE_PUT_TIMEOUT_SECONDS`, as it reports with `DROPPED`. A reader
    that exits unasked ends every link it held, and is started again.
    """

    def __init__(self, frames: FrameQueue, *, spawn: Callable[[int, int], Any] = spawn_reader) -> None:
        self.frames = frames
        self.spawn = spawn
        #: Guards `links`, `next_ident`, `process`, `events` and `stopping`.
        self.lock = threading.Lock()
        #: Guards `commands`: a command is written whole, and never to a
        #: descriptor a relaunch has closed and the kernel may have reused.
        self.command_lock = threading.Lock()
        self.links: dict[int, RemoteLink] = {}
        #: Every link's tier by its ident, kept past the link: the writer reads
        #: a queued event's tier after the link that read it has ended.
        self.tiers: dict[int, str] = {}
        self.next_ident = 1
        self.process: Any = None
        self.commands = -1
        self.events = -1
        self.stopping = False
        #: Set by `close`: past it, frames the queue has no room for are counted dropped.
        self.close_deadline: float | None = None
        #: The intake's alone: bytes past the last whole event, and how long
        #: they must grow before the next event is whole.
        self.held = bytearray()
        self.need = 0
        self.thread = threading.Thread(target=self._run, name="tape-intake", daemon=True)

    @property
    def pid(self) -> int | None:
        pid = getattr(self.process, "pid", None)
        return pid if isinstance(pid, int) else None

    def start(self) -> None:
        self._launch()
        self.thread.start()

    def alive(self) -> bool:
        return self.thread.is_alive()

    def _launch(self) -> None:
        commands_read, commands_write = os.pipe()
        events_read, events_write = os.pipe()
        reader.size_pipe(events_read)
        os.set_blocking(events_read, False)
        try:
            process = self.spawn(commands_read, events_write)
        except BaseException:
            os.close(commands_write)
            os.close(events_read)
            raise
        with self.lock, self.command_lock:
            self.process, self.commands, self.events = process, commands_write, events_read

    def command(self, message: Mapping[str, Any]) -> None:
        line = memoryview((json.dumps(message, separators=(",", ":")) + "\n").encode())
        with self.command_lock:
            try:
                while line:
                    line = line[os.write(self.commands, line) :]
            except OSError as exc:
                raise ConnectionError(f"the tape reader is gone: {exc}") from exc

    def open(self, url: str, *, tier: str, shard: Shard, ping: str | None = None) -> RemoteLink:
        """Have the reader connect `url` for `shard`; block until it has, and
        return the link. Raises what the connect failed with."""

        with self.lock:
            if self.stopping:
                raise ConnectionError("the recorder is stopping")
            ident = self.next_ident
            self.next_ident += 1
            link = RemoteLink(self, ident, tier=tier, shard=shard)
            self.links[ident] = link
        try:
            self.command({"op": "open", "link": ident, "url": url, "shard": shard.index, "ping": ping})
        except ConnectionError:
            self._forget(link, "the tape reader is gone")
            raise
        if not link.opened.wait(reader.CONNECT_TIMEOUT_SECONDS + OPEN_GRACE_SECONDS):
            link.request_close()
            raise TimeoutError(f"the tape reader did not open {url} in {reader.CONNECT_TIMEOUT_SECONDS:g}s")
        if link.error:
            raise ConnectionError(link.error)
        return link

    def close(self, timeout: float = reader.QUEUE_PUT_TIMEOUT_SECONDS) -> None:
        """Stop the reader. It closes every link and hands back what they
        read; what reaches the queue within `timeout` is kept, the rest is
        counted as dropped; every link ends."""

        with self.lock:
            self.stopping = True
        self.close_deadline = time.monotonic() + timeout
        try:
            self.command({"op": "stop"})
        except ConnectionError:
            pass
        if self.thread.ident is not None:
            # The reader's own patience bounds its shutdown; past this it is stuck.
            self.thread.join(timeout + reader.CONNECT_TIMEOUT_SECONDS + OPEN_GRACE_SECONDS)
            if self.thread.is_alive():
                logging.error("the tape reader did not stop; killing it")
                self._kill()
                self.thread.join()
        else:
            self._kill()
            self._reap()
        with self.lock:
            links = list(self.links.values())
            self.links.clear()
        for link in links:
            link.error = link.error or "the recorder is stopping"
            link.opened.set()
            link.ended.set()

    # -------------------------------------------------------------- intake

    def _run(self) -> None:
        while True:
            try:
                if not self._intake_pass():
                    return
            except Exception:  # every frame reaches the queue through this thread
                logging.exception("tape intake pass failed")
                time.sleep(INTAKE_TICK_SECONDS)

    def _intake_pass(self) -> bool:
        """Read the events pipe dry into the queue; False once the reader has exited on request."""

        descriptor = self.events
        got = False
        while True:
            try:
                chunk = os.read(descriptor, INTAKE_READ_BYTES)
            except (BlockingIOError, InterruptedError):
                break
            if not chunk:
                return self._reader_exited()
            got = True
            self._take(chunk)
            if len(chunk) < INTAKE_READ_BYTES:
                break
        if got:
            time.sleep(INTAKE_TICK_SECONDS)
        else:
            select.select([descriptor], [], [], 1.0)
        return True

    def _take(self, chunk: bytes) -> None:
        if self.held:
            self.held += chunk
            if len(self.held) < self.need:
                return
            data = bytes(self.held)
            self.held.clear()
        else:
            data = chunk
        head = EVENT_HEAD.unpack_from
        header = reader.EVENT.size
        links = self.links
        items: list[QueueItem] = []
        owners: list[RemoteLink] = []
        size = len(data)
        at = 0
        need = 0
        while size - at >= header:
            length, kind, ident = head(data, at)
            event = at
            start = at + header
            end = start + length
            if end > size:
                need = end - at
                break
            at = end
            link = links.get(ident)
            if link is None:
                continue
            if kind == reader.FRAME:
                items.append(data[event:end])
                owners.append(link)
            elif kind == reader.OPENED:
                link.opened.set()
            elif kind == reader.STATS:
                link.shard.link_stats = json.loads(data[start:end])
            else:
                # Every frame read before a drop or an end is queued before it is told.
                self._queue(items, owners)
                items, owners = [], []
                if kind == reader.DROPPED:
                    _, _, _, a, b = reader.EVENT.unpack_from(data, event)
                    link.shard.on_frame(b, a)
                    link.shard.on_overrun(link.shard, a)
                else:
                    self._forget(link, data[start:end].decode(errors="replace") if kind == reader.FAILED else "")
        self._queue(items, owners)
        if at < size:
            self.held += data[at:] if at else data
        self.need = need

    def _queue(self, items: list[QueueItem], owners: list[RemoteLink]) -> None:
        if not items:
            return
        counted: dict[RemoteLink, list[Any]] = {}
        for item, link in zip(items, owners):
            seen = counted.get(link)
            if seen is None:
                counted[link] = [1, item]
            else:
                seen[0] += 1
                seen[1] = item
        for link, (count, newest) in counted.items():
            newest_ns = received_ns_of(newest)
            link.shard.last_message_ns = newest_ns
            link.shard.on_frame(newest_ns, count)
        while items:
            try:
                self.frames.put_batch(items, timeout=INTAKE_PUT_SECONDS)
                return
            except Overrun as exc:
                taken = len(items) - exc.dropped
                items, owners = items[taken:], owners[taken:]
            deadline = self.close_deadline
            if deadline is not None and time.monotonic() >= deadline:
                dropped: dict[RemoteLink, int] = {}
                for link in owners:
                    dropped[link] = dropped.get(link, 0) + 1
                for link, count in dropped.items():
                    link.shard.on_overrun(link.shard, count)
                return

    def _forget(self, link: RemoteLink, error: str) -> None:
        with self.lock:
            self.links.pop(link.ident, None)
        if error:
            link.error = error
        link.opened.set()
        link.ended.set()

    def _reader_exited(self) -> bool:
        with self.lock:
            stopping = self.stopping
            links = list(self.links.values())
            self.links.clear()
        code = self._reap()
        self.held.clear()
        self.need = 0
        with self.lock, self.command_lock:
            os.close(self.commands)
            os.close(self.events)
            self.commands = self.events = -1
        for link in links:
            link.error = link.error or "the tape reader exited"
            link.opened.set()
            link.ended.set()
        if stopping:
            return False
        logging.error("the tape reader exited (%s); its %d connections end and reconnect", code, len(links))
        while True:
            time.sleep(READER_RESPAWN_SECONDS)
            with self.lock:
                if self.stopping:
                    return False
            try:
                self._launch()
                return True
            except OSError as exc:
                logging.error("could not start the tape reader: %s", exc)

    def _reap(self) -> Any:
        process = self.process
        if process is None:
            return None
        try:
            return process.wait(timeout=READER_EXIT_SECONDS)
        except subprocess.TimeoutExpired:
            self._kill()
            return process.wait()

    def _kill(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.kill()


@dataclass
class Shard:
    """One websocket connection carrying one slice of a tier's topic list."""

    index: int
    tier: str
    topics: list[str]
    adapter: VenueAdapter
    #: Called from the intake thread: frames read, and frames dropped on an overrun.
    on_frame: Any
    on_overrun: Any
    #: Called with this shard when its connection opens and when it ends, from
    #: the shard's own thread: the coverage fold's view of connectivity.
    on_connect: Any = None
    on_disconnect: Any = None
    #: The reader process that holds every shard's socket.
    reader: ReaderProcess | None = None
    stop: threading.Event = field(default_factory=threading.Event)
    socket: RemoteLink | None = None
    thread: threading.Thread | None = None
    connected: bool = False
    reconnects: int = 0
    #: Chunks re-anchored, counted for `status.json`.
    reanchors: int = 0
    #: Book topics re-subscribed on a sequence gap, counted for `status.json`.
    resyncs: int = 0
    #: The hour `reanchor_cursor` belongs to, and how many of this shard's
    #: book topics that hour's pass has re-anchored. Together they let the
    #: hourly pass spread over many maintenance ticks and resume where it was.
    reanchor_hour: str = ""
    reanchor_cursor: int = 0
    last_message_ns: int = 0
    #: The reader's last `STATS` for this shard's connection; empty until its first.
    link_stats: dict[str, Any] = field(default_factory=dict)
    backoff_seconds: float = field(default_factory=lambda: RECONNECT_BACKOFF_MIN_SECONDS)

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name=f"tape-shard-{self.index}", daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        link = self.socket
        if link is not None:
            link.request_close()

    def join(self, timeout: float = 10.0) -> None:
        if self.thread is not None:
            self.thread.join(timeout)

    def status(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "tier": self.tier,
            "topics": len(self.topics),
            "connected": self.connected,
            "reconnects": self.reconnects,
            "reanchors": self.reanchors,
            "resyncs": self.resyncs,
            "last_message_ns": self.last_message_ns,
            "link": self.link_stats,
        }

    def coverage(self) -> dict[str, Any]:
        """What the hour's coverage record says about this shard; topics by name,
        because a cell asks whether its own topic was on a shard that failed."""

        return {
            "index": self.index,
            "tier": self.tier,
            "topics": list(self.topics),
            "reconnects": self.reconnects,
            "resyncs": self.resyncs,
            "reanchors": self.reanchors,
        }

    def update(self, topics: list[str]) -> tuple[list[str], list[str]]:
        """Make this shard carry exactly `topics`, changing the live subscription
        in place; a shard that is not connected picks the list up when it connects."""

        current = set(self.topics)
        wanted = set(topics)
        added = [topic for topic in topics if topic not in current]
        removed = [topic for topic in self.topics if topic not in wanted]
        self.topics = list(topics)
        if (added or removed) and self.connected and self.socket is not None:
            self._send_all(self.adapter.remove_messages(removed))
            self._send_all(self.adapter.add_messages(added))
        return added, removed

    def anchored_count(self) -> int:
        return len(self.adapter.anchored_topics(self.topics))

    def mark_anchored(self, hour: str) -> None:
        """Treat this shard's books and tickers as anchored for `hour` without
        sending anything. Connecting subscribes, and subscribing is what
        anchors them, so a shard that just connected is already done for its
        hour."""

        self.reanchor_hour = hour
        self.reanchor_cursor = self.anchored_count()

    def reanchor(self, hour: str, limit: int) -> int:
        """Re-subscribe up to `limit` more of this shard's anchored topics for
        `hour`, and return how many were sent.

        The point is the whole state: a book delta only means something next
        to a snapshot, and a ticker delta only next to the whole ticker its
        subscription brought, so an hour of tape anchored in an earlier hour
        cannot be replayed on its own. The cost is the moment between a
        topic's unsubscribe and its snapshot, so the topics go in small
        chunks that are dropped and re-taken together — one round trip per
        symbol rather than one per shard. The snapshot row marks the seam.
        """

        if self.reanchor_hour != hour:
            self.reanchor_hour = hour
            self.reanchor_cursor = 0
        books = self.adapter.anchored_topics(self.topics)
        if not self.connected or self.socket is None:
            return 0
        sent = 0
        while self.reanchor_cursor < len(books) and sent < limit:
            chunk = books[self.reanchor_cursor : self.reanchor_cursor + REANCHOR_CHUNK]
            if sent:
                # Between chunks, not between a chunk's drop and re-take: the
                # venue's incoming-message rate is what this spacing protects,
                # and the gap it would add sits inside a symbol's blind moment.
                time.sleep(LIVE_MESSAGE_SPACING_SECONDS)
            self._retake(chunk)
            self.reanchor_cursor += len(chunk)
            sent += len(chunk)
            self.reanchors += 1
        return sent

    def reanchored(self, hour: str) -> bool:
        return self.reanchor_hour == hour and self.reanchor_cursor >= self.anchored_count()

    def resync_book(self, topic: str) -> bool:
        """Re-subscribe one gapped book topic, and return whether it was sent.

        A book snapshot comes with a subscription — on Bybit at no other time
        — so a topic that lost continuity stays un-based until it is taken
        again. A shard that is not connected re-bases by connecting.
        """

        if not self.connected or self.socket is None or topic not in self.topics:
            return False
        self._retake([topic])
        self.resyncs += 1
        return True

    def _retake(self, chunk: list[str]) -> None:
        self._send_all(self.adapter.remove_messages(chunk))
        self._send_all(self.adapter.add_messages(chunk))

    def _send_all(self, messages: list[str]) -> None:
        for position, text in enumerate(messages):
            if position:
                time.sleep(LIVE_MESSAGE_SPACING_SECONDS)
            link = self.socket
            if link is None:
                return
            try:
                link.send_text(text)
            except Exception as exc:  # the reconnect resubscribes from self.topics
                logging.warning("shard %d could not send a subscription change: %s", self.index, exc)
                return

    def _note(self, hook: Any) -> None:
        if hook is not None:
            hook(self)

    def _run(self) -> None:
        while not self.stop.is_set():
            opened = time.monotonic()
            self._connect_once()
            self.connected = False
            self._note(self.on_disconnect)
            if self.stop.is_set():
                return
            self.reconnects += 1
            # A connection that lived a minute earned a fresh start; one that
            # died at once backs off, so a venue outage cannot become a storm
            # of reconnects across every shard. The draw spreads the shards a
            # venue dropped together, each of whose reconnects is a subscribe
            # and a snapshot per book.
            lived = time.monotonic() - opened
            self.backoff_seconds = (
                RECONNECT_BACKOFF_MIN_SECONDS
                if lived >= RECONNECT_HEALTHY_SECONDS
                else min(self.backoff_seconds * 2.0, RECONNECT_BACKOFF_MAX_SECONDS)
            )
            delay = self.backoff_seconds * random.uniform(0.5, 1.0)
            logging.warning("shard %d disconnected; reconnecting in %.1fs", self.index, delay)
            self.stop.wait(delay)

    def _connect_once(self) -> None:
        """One connection's lifetime: open, subscribe, wait until it ends.

        The reader process connects the socket, reads it, sends the venue's
        keepalive and closes it after `reader.SILENCE_SECONDS` of silence; this
        thread asks for the connection and the subscriptions. Returns when the
        link has ended — every frame read from it queued or counted — so the
        next connection's snapshots cannot overtake this one's last deltas;
        `_run` owns the backoff and the reconnect.
        """

        topics = list(self.topics)
        ping_getter = getattr(self.adapter, "ping_message", None)
        ping_msg = ping_getter() if callable(ping_getter) else None
        assert self.reader is not None, "a shard reads through the recorder's reader process"
        try:
            link = self.reader.open(self.adapter.connection_url(topics), tier=self.tier, shard=self, ping=ping_msg)
        except Exception as exc:  # a refused or timed-out connect is a reconnect
            if not self.stop.is_set():
                logging.warning("shard %d could not connect: %s", self.index, exc)
            return
        self.socket = link
        self.link_stats = {}
        try:
            if self.stop.is_set():
                return
            self.connected = True
            self._note(self.on_connect)
            self._send_all(self.adapter.subscribe_messages(topics))
            logging.info("shard %d connected with %d topics", self.index, len(topics))
            while not self.stop.is_set() and not link.ended.wait(1.0):
                pass
        finally:
            link.request_close()
            # The reader ends a closed link once its frames are queued or have
            # waited out `reader.QUEUE_PUT_TIMEOUT_SECONDS`, and every link when it stops.
            while not link.ended.wait(1.0):
                if not self.reader.alive():
                    break
            self.socket = None


# ------------------------------------------------------------------- budget


class BudgetController:
    """Sheds and restores `tier:feed` pairs to keep the month's inbound bytes under the allowance.

    The projection is what the pairs still subscribed bring in: a shed pair's
    bytes sit in the trailing window for a day, and counting them would keep
    shedding for a day after the shed that was enough. One action per
    `act_every_minutes`: a shed takes as many pairs, in order, as the
    projection needs; a restore returns the last pair only when its month, as
    measured when it was shed, fits under the restore line beside everything
    still subscribed. Over budget with the list exhausted is said every action.
    """

    def __init__(self, settings: BudgetSettings, meter: ByteMeter) -> None:
        self.settings = settings
        self.meter = meter
        self.shed_active: list[tuple[str, str]] = []
        #: GB/month each shed pair carried when it was shed: what restoring it costs.
        self.shed_gb: dict[tuple[str, str], float] = {}
        self.last_action_ns = 0
        self.projected_gb: float | None = None

    @staticmethod
    def _key(pair: tuple[str, str]) -> str:
        return f"feed:{pair[0]}:{pair[1]}"

    def _window_seconds(self, now_ns: int) -> float | None:
        window_ns = self.meter.window_ns(now_ns)
        if window_ns < BUDGET_MIN_WINDOW_NS:
            return None
        return window_ns / 1e9

    def projection_gb(self, now_ns: int) -> float | None:
        seconds = self._window_seconds(now_ns)
        if seconds is None:
            return None
        received = self.meter.last_day("all", now_ns)
        for pair in self.shed_active:
            received -= self.meter.last_day(self._key(pair), now_ns)
        return max(received, 0) / seconds * MONTH_SECONDS / 1e9

    def pair_gb(self, pair: tuple[str, str], now_ns: int) -> float:
        """One pair's month at its rate over the window."""

        seconds = self._window_seconds(now_ns)
        if seconds is None:
            return 0.0
        return self.meter.last_day(self._key(pair), now_ns) / seconds * MONTH_SECONDS / 1e9

    @property
    def over(self) -> bool:
        return (
            self.settings.monthly_gb is not None
            and self.projected_gb is not None
            and self.projected_gb > self.settings.monthly_gb
        )

    def step(self, now_ns: int) -> bool:
        """Re-project; shed or restore when due. Returns whether the shed set changed."""

        self.projected_gb = self.projection_gb(now_ns)
        limit = self.settings.monthly_gb
        if limit is None or self.projected_gb is None:
            return False
        if self.last_action_ns and now_ns - self.last_action_ns < self.settings.act_every_minutes * MINUTE_NS:
            return False
        if self.projected_gb > limit:
            self.last_action_ns = now_ns
            projected = self.projected_gb
            changed = False
            while projected > limit and len(self.shed_active) < len(self.settings.shed):
                pair = self.settings.shed[len(self.shed_active)]
                gb = self.pair_gb(pair, now_ns)
                self.shed_gb[pair] = gb
                self.shed_active.append(pair)
                projected -= gb
                changed = True
                logging.warning(
                    "over budget (%.0f GB/month projected, %.0f allowed): shedding %s:%s (%.0f GB/month)",
                    self.projected_gb,
                    limit,
                    *pair,
                    gb,
                )
            if projected > limit:
                logging.warning(
                    "over budget with every sheddable feed shed: %.0f GB/month projected against %.0f allowed; the config decides what else goes",
                    projected,
                    limit,
                )
            return changed
        if self.shed_active:
            pair = self.shed_active[-1]
            gb = self.shed_gb.get(pair, 0.0)
            if self.projected_gb + gb < limit * self.settings.restore_below:
                self.shed_active.pop()
                self.last_action_ns = now_ns
                logging.info(
                    "under budget (%.0f GB/month projected, %.0f allowed): restoring %s:%s (%.0f GB/month)",
                    self.projected_gb,
                    limit,
                    *pair,
                    gb,
                )
                return True
        return False

    def status(self) -> dict[str, Any]:
        return {
            "monthly_gb": self.settings.monthly_gb,
            "projected_month_gb": None if self.projected_gb is None else round(self.projected_gb, 1),
            "over": self.over,
            "shed": [f"{tier}:{feed}" for tier, feed in self.shed_active],
            "shed_gb_month": {
                f"{tier}:{feed}": round(self.shed_gb.get((tier, feed), 0.0), 1) for tier, feed in self.shed_active
            },
            "shed_order": [f"{tier}:{feed}" for tier, feed in self.settings.shed],
            "last_action_ns": self.last_action_ns,
        }


# ----------------------------------------------------------------- recorder


class Recorder:
    def __init__(self, config: CaptureConfig, *, root: Path | None = None, adapter: VenueAdapter | None = None) -> None:
        self.config = config
        self.adapter = adapter or adapter_for(
            config.venue.name, market=config.venue.market, ws_url=config.venue.ws_url, rest_url=config.venue.rest_url
        )
        validate_config(self.adapter, config)
        resolved_root = root or config.storage.root
        if resolved_root is None:
            raise ConfigError("the recorder needs a storage root: storage.root in the config or --root")
        self.root = resolved_root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        storage = config.storage
        self.manifest = Manifest(self.root)
        self.writer = SegmentWriter(
            self.root,
            max_bytes=int(storage.segment_max_mb * 1024**2),
            fsync_every=storage.fsync_every_records,
            buffer_bytes=int(storage.segment_buffer_kb * 1024),
        )
        self.compressor = Compressor(
            self.root, self.manifest, backlog_max_bytes=int(storage.compress_backlog_max_mb * 1024**2)
        )
        self.retention = Retention(
            self.root,
            self.manifest,
            retention_days=storage.retention_days,
            max_bytes=int(storage.max_disk_gb * 1024**3),
            min_free_bytes=int(storage.min_free_disk_gb * 1024**3),
        )
        self.snapshots = Snapshots(
            self.root,
            self.manifest,
            venue=self.adapter.name,
            market=self.adapter.market,
            source=self.adapter.rest_url,
            cadence=config.snapshot_cadence,
        )
        self.coverage_records = CoverageRecords(self.root, self.manifest)
        self.frames: FrameQueue = FrameQueue(storage.queue_frames, int(storage.queue_max_mb * 1024**2))
        self.reader = ReaderProcess(self.frames)
        self.stop = threading.Event()
        # Set to run a retention pass before the next routine interval, and on
        # shutdown so the pruner's wait is not what a stop waits out.
        self.prune_now = threading.Event()
        # The `_meta` writes the maintainer hands off, in order; None stops the thread.
        self.meta_jobs: queue.Queue[Callable[[], None] | None] = queue.Queue()
        # A table take is queued or under way: a tick that finds the tables
        # still due does not queue another.
        self.taking_tables = False
        # Book topics whose stream lost continuity, waiting to be re-subscribed;
        # the ones whose re-subscribe has not yet brought a snapshot back; and
        # the ones a snapshot has based at all.
        self.resync_now = threading.Event()
        self.resync_lock = threading.Lock()
        self.resync_pending: list[str] = []
        self.resync_outstanding: set[str] = set()
        self.book_based: set[str] = set()
        self.static_symbols: dict[str, tuple[str, ...]] = {}
        for tier in config.tiers:
            if tier.universe.kind == "symbols":
                self.static_symbols[tier.name] = tier.universe.symbols
            elif tier.universe.kind == "file":
                assert tier.universe.path is not None
                symbols = validate_symbols(load_symbol_file(tier.universe.path))
                if not symbols:
                    raise ConfigError(f"tier {tier.name!r}: {tier.universe.path} names no symbols")
                self.static_symbols[tier.name] = symbols
        self.tables: dict[str, list[dict[str, Any]]] | None = None
        self.live = LiveState(history_ns=int(config.history_hours * 3600 * 1e9 * 1.25))
        # When this process began recording. The watchdog measures silence and
        # socket loss from here, so a recorder younger than its silence limit
        # does not read as a dead venue.
        self.started_at_ns = time.time_ns()
        self.coverage = CoverageFold(
            venue=self.adapter.name,
            market=self.adapter.market,
            pid=os.getpid(),
            started_at_ns=self.started_at_ns,
            feeds={tier.name: tuple(feed.text for feed in tier.feeds) for tier in config.tiers},
            now_ns=self.started_at_ns,
        )
        self.meter = ByteMeter(self.started_at_ns)
        self.budget = BudgetController(config.budget, self.meter)
        self.members: dict[str, set[str]] = {tier.name: set() for tier in config.tiers}
        self.qualified_ns: dict[str, dict[str, int]] = {tier.name: {} for tier in config.tiers}
        self.tier_symbols: dict[str, list[str]] = {tier.name: [] for tier in config.tiers}
        self.tier_topics: dict[str, list[str]] = {tier.name: [] for tier in config.tiers}
        self.tier_shards: dict[str, list[Shard]] = {tier.name: [] for tier in config.tiers}
        self.feeds_by_symbol: dict[str, tuple[Feed, ...]] = {}
        #: A book row's (symbol, depth) as the row carries them, to its anchored topic or "".
        self._book_topic_cache: dict[tuple[Any, Any], str] = {}
        self._feed_keys: dict[tuple[Any, ...], str] = {}
        self._tier_keys: dict[str, str] = {}
        self.lanes: list[threading.Thread] = []
        self.lane_stop = threading.Event()
        self.shard_lock = threading.Lock()
        self.next_shard_index = 0
        self.received_frames = 0
        self.written_rows = 0
        self.dropped_frames = 0
        self.disk_dropped_frames = 0
        #: Frames the writer took and could not record: they did not parse, or
        #: made a row the writer refused.
        self.malformed_frames = 0
        self.malformed_logged_at = float("-inf")
        self.last_receive_ns = 0
        self.last_written_receive_ns = 0
        self.disk_blocked = False
        self.snapshot_failures = 0
        self.clock = ClockWindow()
        #: Per feed class (`CLOCK_FEEDS`): receipt less the venue's send stamp,
        #: and the venue's send stamp less its engine's.
        self.feed_clocks: dict[str, tuple[Quantiles, Quantiles]] = {}
        self.queue_wait = ClockWindow(prefix="wait")
        self.clock_lock = threading.Lock()
        self.worker = threading.Thread(target=self._write_loop, name="tape-writer", daemon=True)
        self.maintainer = threading.Thread(target=self._maintenance_loop, name="tape-maintenance", daemon=True)
        self.meta = threading.Thread(target=self._meta_loop, name="tape-meta", daemon=True)
        self.pruner = threading.Thread(target=self._retention_loop, name="tape-retention", daemon=True)
        self.resyncer = threading.Thread(target=self._resync_loop, name="tape-resync", daemon=True)

    # ------------------------------------------------------------ lifecycle

    def run(self) -> None:
        root_lock = lock_root(self.root)
        if root_lock is None:
            # `Restart=always`: a second recorder on one root shows as a crash
            # loop, which is what it is.
            raise RuntimeError(f"another recorder holds {self.root}")
        self.compressor.start()
        self.worker.start()
        self._install_signals()
        self.reader.start()
        self._refresh(time.time_ns(), restart=False)
        self._reconcile_shards()
        self._start_lanes()
        self.meta.start()
        self.maintainer.start()
        self.pruner.start()
        self.resyncer.start()
        try:
            self.stop.wait()
        finally:
            self.stop.set()
            self.prune_now.set()
            self.resync_now.set()
            self.lane_stop.set()
            for shard in self._all_shards():
                shard.close()
            for shard in self._all_shards():
                shard.join()
            self.reader.close()
            for lane in self.lanes:
                lane.join(10.0)
            self.maintainer.join()
            self.meta_jobs.put(None)
            self.meta.join()
            # A pass mid-walk only unlinks whole compressed files, so shutdown
            # does not wait the length of one out.
            self.pruner.join(10.0)
            self.resyncer.join(10.0)
            self.frames.put(None)
            self.worker.join()
            # The open segments close raw, and the compressor stops after the
            # one in hand: compressing an hour's open segments here held a
            # restart for two minutes of tape. The next start's recovery, or
            # `market_tape pack` on an idle root, compresses them.
            self.writer.close()
            self.compressor.close(drain=False)
            self._close_coverage(time.time_ns())
            self._write_status()
            # A pruner the join above gave up on may still be rewriting the
            # manifest; the next holder of the root appends to it.
            self.manifest.close()
            unlock_root(root_lock)

    def _install_signals(self) -> None:
        def stop(_signum: int, _frame: Any) -> None:
            self.stop.set()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)

    def _all_shards(self) -> list[Shard]:
        with self.shard_lock:
            return [shard for tier in self.config.tiers for shard in self.tier_shards[tier.name]]

    def _new_shard(self, tier: str, topics: list[str]) -> Shard:
        with self.shard_lock:
            index = self.next_shard_index
            self.next_shard_index += 1
        shard = Shard(
            index=index,
            tier=tier,
            topics=list(topics),
            adapter=self.adapter,
            reader=self.reader,
            on_frame=self._on_frame,
            on_overrun=self._on_overrun,
            on_connect=self._on_shard_connect,
            on_disconnect=self._on_shard_disconnect,
        )
        day, hour = utc_day_hour(time.time_ns())
        shard.mark_anchored(f"{day}T{hour}")
        # A shard carries nothing until its socket is open, and the coverage
        # record says so from here rather than from the first failure.
        self._on_shard_disconnect(shard)
        shard.start()
        return shard

    def _reconcile_shards(self) -> None:
        for tier in self.config.tiers:
            self._reconcile_tier(tier.name, self.tier_topics[tier.name])

    def _reconcile_tier(self, tier: str, desired: list[str]) -> None:
        """Make the tier's shards carry exactly `desired`, keeping every topic on
        the connection it already has, filling free room, opening new shards only
        for what does not fit, and closing shards left with nothing."""

        wanted = set(desired)
        room = self.config.topics_per_connection
        group = self.adapter.connection_group
        shards = list(self.tier_shards[tier])
        plans: list[list[str]] = [[topic for topic in shard.topics if topic in wanted] for shard in shards]
        placed = {topic for plan in plans for topic in plan}
        leftover = [topic for topic in desired if topic not in placed]
        for shard, plan in zip(shards, plans):
            anchor = plan[0] if plan else (shard.topics[0] if shard.topics else None)
            if anchor is None:
                continue
            mine = group(anchor)
            take = [topic for topic in leftover if group(topic) == mine][: max(0, room - len(plan))]
            plan.extend(take)
            taken = set(take)
            leftover = [topic for topic in leftover if topic not in taken]
        kept: list[Shard] = []
        for shard, plan in zip(shards, plans):
            if plan:
                shard.update(plan)
                kept.append(shard)
            else:
                # Its topics moved to another connection; the disconnect this
                # shard's thread reports on the way out must not name them.
                shard.topics = []
                shard.close()
        for chunk in shard_topics(leftover, room, group):
            kept.append(self._new_shard(tier, chunk))
        with self.shard_lock:
            self.tier_shards[tier] = kept
        for shard in shards:
            if shard not in kept:
                shard.join()

    def _start_lanes(self) -> None:
        self.lane_stop = threading.Event()
        self.lanes = list(self.adapter.start_lanes(self.feeds_by_symbol, self.emit, self.lane_stop))

    def _restart_lanes(self) -> None:
        self.lane_stop.set()
        for lane in self.lanes:
            lane.join(10.0)
        self._start_lanes()

    def _on_frame(self, received_ns: int, count: int = 1) -> None:
        self.received_frames += count
        self.last_receive_ns = received_ns

    def _on_overrun(self, shard: "Shard | None" = None, frames: int = 1) -> None:
        self.dropped_frames += frames
        if shard is not None:
            self.coverage.overrun(shard.index, time.time_ns())

    def _on_shard_connect(self, shard: Shard) -> None:
        self.coverage.connected(shard.index, shard.tier, list(shard.topics), time.time_ns())

    def _on_shard_disconnect(self, shard: Shard) -> None:
        self.coverage.disconnected(shard.index, shard.tier, list(shard.topics), time.time_ns())

    def emit(self, row: Mapping[str, Any]) -> None:
        """Hand an already normalized row to the writer: what a side lane built."""

        received = int(row.get("local_receive_ts_ns") or time.time_ns())
        self._on_frame(received)
        try:
            self.frames.put_nowait(("rows", [dict(row)], received, LANES, 0))
        except queue.Full:
            self._on_overrun()

    # ------------------------------------------------------------- universe

    def resolve_tiers(
        self, now_ns: int, tables: Mapping[str, list[dict[str, Any]]] | None = None
    ) -> dict[str, list[str]]:
        """Each tier's symbols, in config order. With `tables` given they seed the
        live state first; otherwise the last snapshot and the ticker stream decide."""

        if tables is not None:
            self.tables = dict(tables)
            self._seed_live(tables)
        instruments = list((self.tables or {}).get("instruments") or [])
        live, baseline = self.live.view()
        listed_cache: dict[str | None, list[str]] = {}

        def listed(quote: str | None) -> list[str]:
            if quote not in listed_cache:
                listed_cache[quote] = self.adapter.listed_symbols(instruments, quote=quote) if instruments else []
            return listed_cache[quote]

        def allowed(quote: str | None) -> set[str]:
            if instruments:
                return set(listed(quote))
            # No instrument table yet: the ticker stream is all there is. It
            # carries every symbol the venue streams, so the tier's own quote
            # filter has to be applied by shape here — without it a cold start
            # widens the deep tiers past the quote they asked for and records
            # names like WLDUSDC and ADAUSD_PERP off a USDT universe.
            if quote is None:
                return set(live)
            return {symbol for symbol in live if symbol.isalnum() and symbol.endswith(quote)}

        resolved: dict[str, list[str]] = {}
        for tier in self.config.tiers:
            universe = tier.universe
            if universe.kind in ("symbols", "file"):
                symbols = set(self.static_symbols[tier.name])
            elif universe.kind == "listed":
                symbols = set(listed(universe.quote))
            elif universe.kind in RANKED_KINDS:
                symbols = self._ranked(tier, now_ns, live, allowed(universe.quote))
            else:
                symbols = self._sticky(
                    tier, now_ns, live, baseline, allowed(universe.quote), instruments_known=bool(instruments)
                )
            excluded: set[str] = set()
            for name in universe.exclude_tiers:
                excluded.update(resolved.get(name, []))
            resolved[tier.name] = sorted(symbols - excluded)
        return resolved

    def _ranked(self, tier: Tier, now_ns: int, live: Mapping[str, SymbolLive], allowed: set[str]) -> set[str]:
        universe = tier.universe

        def measure(state: SymbolLive) -> float | None:
            if universe.kind == "top_turnover":
                return state.turnover_24h
            return None if state.price_change_24h is None else abs(state.price_change_24h)

        scored = {symbol: measure(live[symbol]) for symbol in allowed if symbol in live}
        ranked = sorted(
            (symbol for symbol, score in scored.items() if score is not None),
            key=lambda symbol: (-(scored[symbol] or 0.0), symbol),
        )
        rank = {symbol: position + 1 for position, symbol in enumerate(ranked)}
        leave = max(universe.leave_top, universe.top)
        current = self.members[tier.name]
        members = {
            symbol
            for symbol, position in rank.items()
            if position <= universe.top or (symbol in current and position <= leave)
        }
        # The time floor: a name that ranked inside `top` keeps its place for
        # sticky_hours after the last time it did, however far it has fallen.
        stamps = self.qualified_ns[tier.name]
        sticky_ns = int((universe.sticky_hours or 0.0) * 3600 * 1e9)
        if sticky_ns:
            for symbol, position in rank.items():
                if position <= universe.top:
                    stamps[symbol] = now_ns
            for symbol in list(stamps):
                if now_ns - stamps[symbol] >= sticky_ns or symbol not in allowed:
                    del stamps[symbol]
            members |= set(stamps)
        self.members[tier.name] = members
        return members

    def _sticky(
        self,
        tier: Tier,
        now_ns: int,
        live: Mapping[str, SymbolLive],
        baseline: Mapping[str, float],
        allowed: set[str],
        *,
        instruments_known: bool,
    ) -> set[str]:
        universe = tier.universe
        stamps = self.qualified_ns[tier.name]
        window_ns = int(universe.window_hours * 3600 * 1e9)
        for symbol in allowed:
            state = live.get(symbol)
            if state is None:
                continue
            if universe.kind == "funding_below":
                qualifies = state.funding_rate is not None and state.funding_rate * 10_000.0 <= -universe.threshold_bp
            elif universe.kind == "funding_above":
                qualifies = state.funding_rate is not None and state.funding_rate * 10_000.0 >= universe.threshold_bp
            elif universe.kind == "turnover_surge":
                base = baseline.get(symbol)
                qualifies = (
                    state.turnover_24h is not None
                    and base is not None
                    and base > 0.0
                    and state.turnover_24h >= universe.ratio * base
                )
            elif universe.kind == "price_move":
                qualifies = state.price_change_24h is not None and abs(state.price_change_24h) >= universe.pct
            else:
                qualifies = self._windowed(universe, state, self.live.earlier(symbol, now_ns - window_ns), window_ns)
            if qualifies:
                stamps[symbol] = now_ns
        hours = DEFAULT_STICKY_HOURS if universe.sticky_hours is None else universe.sticky_hours
        sticky_ns = int(hours * 3600 * 1e9)
        for symbol in list(stamps):
            if now_ns - stamps[symbol] >= sticky_ns or (instruments_known and symbol not in allowed):
                del stamps[symbol]
        members = set(stamps)
        self.members[tier.name] = members
        return members

    @staticmethod
    def _windowed(universe: Universe, now: SymbolLive, then: Sample | None, window_ns: int) -> bool:
        """The windowed kinds compare the live reading with the sample one window back."""

        if then is None:
            return False
        if universe.kind == "price_burst":
            if now.price is None or then.price is None or then.price <= 0.0:
                return False
            return abs(now.price / then.price - 1.0) >= universe.pct
        if universe.kind == "oi_change":
            if now.open_interest is None or then.open_interest is None or then.open_interest <= 0.0:
                return False
            return abs(now.open_interest / then.open_interest - 1.0) >= universe.pct
        # volume_burst: the growth of the rolling 24h turnover over the window is
        # what the window traded beyond the same window a day earlier.
        if now.turnover_24h is None or then.turnover_24h is None or now.turnover_24h <= 0.0:
            return False
        average_window = now.turnover_24h * window_ns / (24 * 3600 * 1e9)
        return now.turnover_24h - then.turnover_24h >= universe.ratio * average_window

    def _seed_live(self, tables: Mapping[str, list[dict[str, Any]]]) -> None:
        tickers = list(tables.get("tickers") or [])
        self.live.seed(self.adapter.funding_rates(tickers), self.adapter.turnovers(tickers))

    def plan_topics(
        self, resolved: Mapping[str, list[str]], shed: Iterable[tuple[str, str]] | None = None
    ) -> tuple[dict[str, list[str]], dict[str, tuple[Feed, ...]]]:
        """Topics per tier with each venue topic claimed once, and each symbol's
        union of feeds for the side lanes. `shed` names tier:feed pairs to leave out."""

        left_out = set(self.budget.shed_active if shed is None else shed)
        claimed: set[str] = set()
        topics: dict[str, list[str]] = {}
        feeds: dict[str, set[Feed]] = {}
        for tier in self.config.tiers:
            active = tuple(feed for feed in tier.feeds if (tier.name, feed.text) not in left_out)
            mine: list[str] = []
            for symbol in resolved.get(tier.name, []):
                feeds.setdefault(symbol, set()).update(active)
                for topic in self.adapter.topics(symbol, active):
                    if topic in claimed:
                        continue
                    claimed.add(topic)
                    mine.append(topic)
            topics[tier.name] = mine
        return topics, {symbol: tuple(sorted(found, key=lambda feed: feed.text)) for symbol, found in feeds.items()}

    def _take_tables(self, now_ns: int, *, first: bool) -> None:
        try:
            tables = self.adapter.fetch_tables()
            self.snapshots.write(now_ns, tables)
            self.tables = tables
            self._seed_live(tables)
            self._log_listed(tables)
        except Exception as exc:  # the venue's REST is optional to the tape
            self.snapshot_failures += 1
            logging.warning("venue tables unavailable; keeping the last universe: %s", exc)
            if first:
                # Hold the snapshot clock so the next maintenance pass tries
                # again instead of waiting a whole cadence.
                self.snapshots.last_key = None
        finally:
            self.taking_tables = False

    def _log_listed(self, tables: Mapping[str, list[dict[str, Any]]]) -> None:
        instruments = list(tables.get("instruments") or [])
        quotes: list[str | None] = [
            quote for quote in sorted({tier.universe.quote for tier in self.config.tiers if tier.universe.quote})
        ]
        if not quotes:
            quotes.append(None)
        for quote in quotes:
            listed = len(self.adapter.listed_symbols(instruments, quote=quote))
            excluded = self.adapter.excluded_listed(instruments, quote=quote)
            logging.info(
                "venue tables: %d %s perpetuals in the domain; outside it %s",
                listed,
                quote or "all-quote",
                " ".join(f"{label or '(blank)'}={count}" for label, count in excluded.items()) or "none",
            )

    def _refresh(self, now_ns: int, *, restart: bool) -> None:
        """Take the tables if due, then re-resolve every tier; with `restart`
        the live connections and lanes follow the new plan. Once the meta
        thread runs the take is its job, and the plan picks the tables up on
        the tick after they land."""

        if self.snapshots.due(now_ns) and not self.taking_tables:
            self.taking_tables = True
            self._to_meta(functools.partial(self._take_tables, now_ns, first=self.tables is None))
        self._replan(now_ns, apply=restart)

    def _replan(self, now_ns: int, *, apply: bool) -> list[str]:
        resolved = self.resolve_tiers(now_ns)
        topics, feeds_by_symbol = self.plan_topics(resolved)
        changed: list[str] = []
        for tier in self.config.tiers:
            if topics[tier.name] != self.tier_topics[tier.name]:
                changed.append(tier.name)
                logging.info(
                    "tier %s: %d symbols, %d topics (was %d symbols)",
                    tier.name,
                    len(resolved[tier.name]),
                    len(topics[tier.name]),
                    len(self.tier_symbols[tier.name]),
                )
            self.tier_symbols[tier.name] = resolved[tier.name]
            self.tier_topics[tier.name] = topics[tier.name]
            self.coverage.members(tier.name, resolved[tier.name], now_ns)
        lanes_changed = feeds_by_symbol != self.feeds_by_symbol
        self.feeds_by_symbol = feeds_by_symbol
        if apply:
            for name in changed:
                self._reconcile_tier(name, self.tier_topics[name])
            if lanes_changed:
                self._restart_lanes()
        return changed

    # -------------------------------------------------------------- writing

    @staticmethod
    def feed_class(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "control"
        return feed_of_row(rows[0]) or "other"

    def _write_loop(self) -> None:
        next_roll_ns = time.monotonic_ns() + 10_000_000_000
        while True:
            now_ns = time.monotonic_ns()
            if now_ns >= next_roll_ns:
                self._roll_idle()
                next_roll_ns = now_ns + 10_000_000_000
            try:
                items = self.frames.get_batch(max_items=1000, timeout=0.0)
            except queue.Empty:
                # Polled, not woken: a wake per intake tick costs the writer
                # more than the tick's frames do.
                time.sleep(WRITE_IDLE_SECONDS)
                continue
            # The batch's readings, folded into the windows once per batch
            # rather than a call per frame.
            waits: list[float] = []
            skews: list[float] = []
            # Per `CLOCK_FEEDS` row kind: its skews, its venue lags, and the
            # field its venue lag is read from.
            clocks: dict[Any, tuple[list[float], list[float], str]] = {
                kind: ([], [], engine) for kind, (_, engine) in CLOCK_FEEDS.items()
            }
            batch_meters: dict[str, int] = defaultdict(int)
            now_mono_ns = time.monotonic_ns()
            last_received_ns = 0
            stopping = False
            for item in items:
                if item is None:
                    stopping = True
                    break
                last_received_ns = self._write_item(item, now_mono_ns, waits, skews, batch_meters, clocks)
            if waits or skews:
                with self.clock_lock:
                    self.queue_wait.take_all(waits)
                    self.clock.take_all(skews)
                    for kind, (wire, venue, _) in clocks.items():
                        if not wire:
                            continue
                        feed = CLOCK_FEEDS[kind][0]
                        held = self.feed_clocks.get(feed)
                        if held is None:
                            held = self.feed_clocks[feed] = (Quantiles(), Quantiles())
                        held[0].take_all(wire)
                        held[1].take_all(venue)
            if batch_meters and last_received_ns:
                # Every item's bytes are under its tier's key, so "all" is their sum.
                received = sum(count for key, count in batch_meters.items() if key.startswith("tier:"))
                self.meter.add_batch({"all": received, **batch_meters}, last_received_ns)
            if last_received_ns:
                self.last_written_receive_ns = last_received_ns
            if stopping:
                return

    def _tier_key(self, tier: str) -> str:
        """`tier:<tier>`, built once per tier."""

        key = self._tier_keys[tier] = f"tier:{tier}"
        return key

    def _feed_key(self, tier: str, rows: list[dict[str, Any]]) -> str:
        """`feed:<tier>:<feed class>`, built once per tier and row shape."""

        first = rows[0] if rows else None
        shape = (tier, first.get("kind"), first.get("depth"), first.get("interval")) if first is not None else (tier,)
        key = self._feed_keys.get(shape)
        if key is None:
            key = self._feed_keys[shape] = f"feed:{tier}:{self.feed_class(rows)}"
        return key

    def _write_item(
        self,
        item: QueueItem,
        now_mono_ns: int,
        waits: list[float],
        skews: list[float],
        batch_meters: dict[str, int],
        clocks: dict[Any, tuple[list[float], list[float], str]],
    ) -> int:
        """One queued frame or lane row set onto the tape: written whole, or
        counted on exactly one of `disk_dropped_frames` and `malformed_frames`.
        Returns the item's receive stamp.

        The inbound allowance (`budget.monthly_gb`) counts what the venue sent
        whether or not the disk took it; the per-feed split needs the rows."""

        payload: Any
        if isinstance(item, bytes):
            count, _, ident, received_ns, mono_ns = reader.EVENT.unpack_from(item)
            kind, payload, tier = "frame", item[reader.EVENT.size :], self.reader.tiers.get(ident, "")
        else:
            kind, payload, received_ns, tier, mono_ns = item
            count = len(payload)
        if kind == "frame":
            batch_meters[self._tier_keys.get(tier) or self._tier_key(tier)] += count
        if self.disk_blocked:
            self.disk_dropped_frames += 1
            return received_ns
        try:
            if kind == "frame":
                rows = self.adapter.normalize(payload, received_ns, mono_ns)
                batch_meters[self._feed_key(tier, rows)] += count
                if mono_ns:
                    waits.append((now_mono_ns - mono_ns) / 1e6)
            else:
                rows = payload
                count = sum(len(json.dumps(row, separators=(",", ":"))) for row in rows)
                batch_meters[self._tier_keys.get(tier) or self._tier_key(tier)] += count
                batch_meters[self._feed_key(tier, rows)] += count
            append = self.writer.append
            for row in rows:
                row_kind = row.get("kind")
                # This runs for every row; a clean delta, which is most of the
                # tape, never reaches the book bookkeeping, and nor does a book
                # the adapter does not anchor (every Binance book row is a
                # snapshot). A book row is its frame's only row, so each is
                # noted before its frame's append.
                if (
                    row_kind == KIND_BOOK_SNAPSHOT or (row_kind == KIND_BOOK_DELTA and row.get("sequence_gap"))
                ) and self._book_topic(row) is not None:
                    self._note_books([row], received_ns)
                closed = append(row)
                if closed:
                    for segment in closed:
                        self.compressor.submit(segment)
                self.written_rows += 1
                if row_kind == KIND_TICKER:
                    self.live.observe(str(row.get("symbol")), row.get("values") or {}, received_ns)
                stamp = row.get("exchange_system_ts_ns")
                if stamp:
                    skew_ms = (received_ns - stamp) / 1e6
                    skews.append(skew_ms)
                    clocked = clocks.get(row_kind)
                    if clocked is not None:
                        clocked[0].append(skew_ms)
                        engine = row.get(clocked[2])
                        if engine:
                            clocked[1].append((stamp - engine) / 1e6)
        except OSError as exc:
            first = not self.disk_blocked
            self.disk_blocked = True
            self.disk_dropped_frames += 1
            if first:
                self.coverage.disk_blocked(True, time.time_ns())
                self.prune_now.set()
                logging.error("capture storage blocked; frames will be counted but not written: %s", exc)
        except Exception:  # one malformed frame cannot stop the tape
            self.malformed_frames += 1
            now = time.monotonic()
            if now - self.malformed_logged_at >= MALFORMED_LOG_SECONDS:
                self.malformed_logged_at = now
                logging.exception("could not record a frame (%d since start)", self.malformed_frames)
        return received_ns

    def _note_books(self, rows: list[dict[str, Any]], received_ns: int) -> None:
        """Queue a re-subscribe for every book topic whose deltas lost continuity,
        and drop the one a snapshot has just re-based."""

        for row in rows:
            kind = row.get("kind")
            # This runs on the writer thread for every book row; a clean
            # delta, which is most of the tape, never builds a topic name.
            if kind == KIND_BOOK_DELTA:
                if not row.get("sequence_gap"):
                    continue
            elif kind != KIND_BOOK_SNAPSHOT:
                continue
            topic = self._book_topic(row)
            if topic is None:
                continue
            if kind == KIND_BOOK_SNAPSHOT:
                self.coverage.book_snapshot(topic, received_ns)
                with self.resync_lock:
                    self.book_based.add(topic)
                    self.resync_outstanding.discard(topic)
                continue
            self.coverage.book_gap(topic, str(row.get("symbol") or ""), int(row.get("depth") or 0), received_ns)
            with self.resync_lock:
                # A topic no snapshot has based yet is waiting for the one its
                # subscription brings, which lands without a re-subscribe and
                # flags the deltas until it does.
                if topic in self.resync_outstanding or topic not in self.book_based:
                    continue
                self.resync_outstanding.add(topic)
                self.resync_pending.append(topic)
            self.resync_now.set()

    def _book_topic(self, row: Mapping[str, Any]) -> str | None:
        """The row's book topic when the adapter anchors it; None otherwise. A
        book the adapter does not anchor is whole in every row (Binance's), so
        it never gaps and a snapshot of it re-bases nothing."""

        key = (row.get("symbol"), row.get("depth"))
        topic = self._book_topic_cache.get(key)
        if topic is None:
            symbol = str(key[0] or "")
            depth = key[1]
            if not symbol or depth is None:
                return None
            topics = self.adapter.anchored_topics(self.adapter.topics(symbol, (Feed("book", str(int(depth))),)))
            topic = topics[0] if topics else ""
            self._book_topic_cache[key] = topic
        return topic or None

    def _effective_stream_ns(self) -> int:
        """The stream timestamp storage should pace its rolls by.

        If the queue is draining, the tape's cursor is the timestamp of the rows
        just written, not the host's wall clock. Using wall clock across an hour
        boundary while backlog is draining would close segments before their
        hour's rows finish arriving, opening tiny fragments on every tick.
        """
        if self.last_written_receive_ns <= 0:
            return 0
        if not self.frames.empty():
            return self.last_written_receive_ns
        now_ns = time.time_ns()
        if now_ns - self.last_written_receive_ns > 5_000_000_000:
            return now_ns
        return self.last_written_receive_ns

    def _roll_idle(self) -> None:
        stream_ns = self._effective_stream_ns()
        if stream_ns <= 0:
            return
        try:
            for segment in self.writer.roll_idle(stream_ns):
                self.compressor.submit(segment)
        except Exception as exc:  # the writer thread is the tape; a segment still open is retried
            logging.error("could not close an idle segment: %s", exc)
        self.writer.release_cache()

    # ---------------------------------------------------------- maintenance

    def _retention_loop(self) -> None:
        while not self.stop.is_set():
            # A pass that deleted and left the gate shut is owed a successor
            # now, not at the next wake: `_maintenance` arms `prune_now` on a
            # blocked tick, which is a whole `status_interval_seconds` of
            # thrown-away tape, and `_write_loop` never reaches an append to
            # fail on. The retry ends on the pass that deletes nothing, so a
            # disk filled by something other than tape is walked once rather
            # than spun on.
            owed = True
            # What this burst has already unlinked. `prune` reads free space
            # from the kernel, and a successor is owed precisely because the
            # kernel disagreed with what the previous pass unlinked, so an
            # uncredited retry re-derives the same deficit and deletes it
            # again — every few hundred milliseconds, until a floor held by
            # something other than tape has cost the whole tape.
            credit = 0
            while owed and not self.stop.is_set():
                owed = self._retention_pass(free_credit=credit)
                credit += self.retention.last_freed_bytes
            self.prune_now.wait(RETENTION_INTERVAL_SECONDS)
            self.prune_now.clear()

    def _retention_pass(self, free_credit: int = 0) -> bool:
        """One retention pass, on its own thread. A failed pass is the next
        pass's problem: this thread must outlive an unlinkable file.

        Returns whether this pass is owed a successor: it deleted, and the
        writer is still blocked.
        """

        try:
            deleted = self.retention.prune(free_credit=free_credit)
        except OSError as exc:
            logging.error("tape retention pass failed: %s", exc)
            return False
        if not deleted:
            return False
        logging.info("retention removed %d tape files", len(deleted))
        if not self.disk_blocked:
            return False
        # `disk_blocked` gates every frame in `_write_loop`, and the pass that
        # frees room is the only thing that can end the block, so it is what
        # opens the gate. Leaving that to `_maintenance` costs a full
        # `status_interval_seconds` of tape on a disk that already has space.
        #
        # `prune` stops on free space counted from the sizes it unlinked;
        # `writable()` reads the kernel's. The two disagree while the
        # filesystem is still releasing blocks, so a pass can delete, believe
        # it reached the floor, and still leave the gate shut.
        if not self.retention.writable():
            return True
        self.disk_blocked = False
        self.coverage.disk_blocked(False, time.time_ns())
        logging.info("capture storage unblocked; writing resumed")
        return False

    def _meta_loop(self) -> None:
        while (job := self.meta_jobs.get()) is not None:
            job()

    def _to_meta(self, job: Callable[[], None]) -> None:
        """Hand a `_meta` write to its thread; before that thread starts and
        after it stops, the caller runs it. Each job catches its own failure,
        so the thread outlives a refused write."""

        if self.meta.is_alive():
            self.meta_jobs.put(job)
        else:
            job()

    def _resync_loop(self) -> None:
        while not self.stop.is_set():
            self.resync_now.wait()
            self.resync_now.clear()
            if self.stop.is_set():
                return
            self._resync_books()

    def _resync_books(self) -> None:
        """Re-subscribe the queued book topics, one round trip each.

        The hourly re-anchor is the only other thing that re-bases a book, so
        a topic that gapped at the start of an hour would otherwise be
        recorded as deltas against a book nothing can rebuild. One
        re-subscribe is outstanding per topic: the snapshot it brings back is
        what allows the next one.
        """

        while not self.stop.is_set():
            with self.resync_lock:
                if not self.resync_pending:
                    return
                topic = self.resync_pending.pop(0)
            shard = self._shard_for(topic)
            if shard is None or not shard.resync_book(topic):
                with self.resync_lock:
                    self.resync_outstanding.discard(topic)
                continue
            logging.info("re-subscribed %s after a sequence gap", topic)
            with self.resync_lock:
                more = bool(self.resync_pending)
            if more:
                time.sleep(LIVE_MESSAGE_SPACING_SECONDS)

    def _shard_for(self, topic: str) -> Shard | None:
        for shard in self._all_shards():
            if topic in shard.topics:
                return shard
        return None

    def _maintenance_loop(self) -> None:
        while not self.stop.is_set():
            # A full disk refuses the coverage roll and the status write, and
            # the tick is also the only thing that re-reads free space and
            # wakes the pruner: a tick that raised out of this loop left the
            # process running with no heartbeat and the gate shut for good.
            try:
                self._maintenance()
            except Exception:  # the next tick is what reopens the gate
                logging.exception("capture maintenance tick failed")
            self.stop.wait(self.config.storage.status_interval_seconds)

    def _maintenance(self) -> None:
        blocked = not self.retention.writable()
        # Under the free floor every frame is counted and thrown away, so the
        # only thing that frees room runs on every blocked tick, not just the
        # crossing. A credited burst ends on the pass that finds no deficit
        # left, which is the ordinary exit whenever the kernel released the
        # unlinked blocks and another writer on the filesystem took them: the
        # gate is still shut, the tape still holds hours nobody needs, and
        # nothing else wakes the pruner for a whole
        # `RETENTION_INTERVAL_SECONDS`. A pass while blocked is not a repeat
        # of the last one — the tape is not growing, but free space moves
        # under it. The tick itself stays O(1); the pruner owns the walk.
        if blocked:
            self.prune_now.set()
        self.disk_blocked = blocked
        now_ns = time.time_ns()
        self.coverage.disk_blocked(blocked, now_ns)
        if self.stop.is_set():
            return
        self._refresh(now_ns, restart=True)
        if self.budget.step(now_ns):
            self._replan(now_ns, apply=True)
        self.coverage.shed(self.budget.shed_active, now_ns)
        self._reanchor(now_ns)
        self._roll_coverage(now_ns)
        self._write_status()
        if MALLOC_TRIM is not None:
            MALLOC_TRIM(0)

    def _roll_coverage(self, now_ns: int) -> None:
        """Write the coverage record of every hour the clock has left behind."""

        self.coverage.note_shards([shard.coverage() for shard in self._all_shards()])
        self.coverage.note_counts(
            received_frames=self.received_frames,
            written_rows=self.written_rows,
            dropped_frames=self.dropped_frames,
            disk_dropped_frames=self.disk_dropped_frames,
        )
        while now_ns >= self.coverage.hour_end_ns:
            self._to_meta(functools.partial(self._write_coverage, self.coverage.roll(self.coverage.hour_end_ns)))

    def _close_coverage(self, now_ns: int) -> None:
        """The shutdown path: the hours the clock left behind, then the part of
        this one that was recorded."""

        self._roll_coverage(now_ns)
        self._write_coverage(self.coverage.close(now_ns))

    def _write_coverage(self, payload: Mapping[str, Any]) -> None:
        try:
            self.coverage_records.write(payload)
        except Exception as exc:  # the tape outranks its own account of itself
            logging.warning("could not write the coverage record for %s: %s", payload.get("hour"), exc)

    def _reanchor(self, now_ns: int) -> None:
        """Once an hour, re-subscribe every shard's books and tickers so each
        hour of tape opens with a whole book and a whole ticker per symbol.

        The hour is the archive's unit — one directory, one uploaded tar — so
        anchoring on the hour is what makes a single tar replayable. The pass
        is spread over maintenance ticks, `REANCHOR_TOPICS_PER_TICK` topics at
        a time, so a thousand topics take about seven minutes rather than
        re-subscribing at once.
        """

        if not self.config.reanchor_each_hour:
            return
        day, hour = utc_day_hour(now_ns)
        this_hour = f"{day}T{hour}"
        budget = REANCHOR_TOPICS_PER_TICK
        sent = 0
        for shard in self._all_shards():
            if budget <= 0:
                break
            # A shard that is not connected re-anchors by connecting.
            if shard.reanchored(this_hour) or not shard.connected:
                continue
            moved = shard.reanchor(this_hour, budget)
            budget -= moved
            sent += moved
        if sent:
            logging.info("re-anchored %d book and ticker topics for %s", sent, this_hour)

    def _queue_wait_status(self) -> dict[str, Any]:
        with self.clock_lock:
            window, self.queue_wait = self.queue_wait, ClockWindow(prefix="wait")
        return window.status()

    def _clock_status(self) -> dict[str, Any]:
        """The window since the last status write, and a fresh one for the next:
        receipt less the venue's send stamp over every stamped row, and per
        feed class its median and 99th percentile (`skew_*`) beside the
        venue's own lag from its engine's stamp to its send stamp (`venue_*`)."""

        with self.clock_lock:
            window, self.clock = self.clock, ClockWindow()
            feeds, self.feed_clocks = self.feed_clocks, {}
        status = window.status()
        status["feeds"] = {
            feed: {
                "samples": wire.samples,
                "skew_p50_ms": wire.quantile(0.5),
                "skew_p99_ms": wire.quantile(0.99),
                "venue_p50_ms": venue.quantile(0.5),
                "venue_p99_ms": venue.quantile(0.99),
            }
            for feed, (wire, venue) in sorted(feeds.items())
        }
        return status

    def tier_status(self, tier: Tier) -> dict[str, Any]:
        symbols = self.tier_symbols[tier.name]
        status: dict[str, Any] = {
            "name": tier.name,
            "universe": tier.universe.kind,
            "live": tier.universe.live,
            "feeds": [feed.text for feed in tier.feeds],
            "shed": [feed for name, feed in self.budget.shed_active if name == tier.name],
            "symbols": len(symbols),
            "topics": len(self.tier_topics[tier.name]),
        }
        if len(symbols) <= 64:
            status["names"] = list(symbols)
        return status

    def bytes_status(self, now_ns: int) -> dict[str, Any]:
        return {
            "received_total": self.meter.totals.get("all", 0),
            "received_24h": self.meter.last_day("all", now_ns),
            "by_feed_24h": {key[len("feed:") :]: self.meter.last_day(key, now_ns) for key in self.meter.keys("feed:")},
        }

    def _write_status(self) -> None:
        now_ns = time.time_ns()
        budget = self.budget.status()
        compressor = self.compressor.status()
        payload = {
            "kind": "forward_capture_status",
            "schema_version": SCHEMA_VERSION,
            "pid": os.getpid(),
            "reader_pid": self.reader.pid,
            "venue": self.adapter.name,
            "market": self.adapter.market,
            "config": str(self.config.source_path) if self.config.source_path else None,
            "started_at_ns": self.started_at_ns,
            "recorded_at_ns": now_ns,
            "status_interval_seconds": self.config.storage.status_interval_seconds,
            "tiers": [self.tier_status(tier) for tier in self.config.tiers],
            "shards": [shard.status() for shard in self._all_shards()],
            "lanes": len(self.lanes),
            "bytes": self.bytes_status(now_ns),
            "budget": budget,
            "received_frames": self.received_frames,
            "written_rows": self.written_rows,
            "dropped_frames": self.dropped_frames,
            "disk_dropped_frames": self.disk_dropped_frames,
            "malformed_frames": self.malformed_frames,
            "last_receive_ns": self.last_receive_ns,
            "last_snapshot_ns": self.snapshots.last_ns,
            "snapshot_failures": self.snapshot_failures,
            "queued_frames": self.frames.qsize(),
            "queue_capacity": self.config.storage.queue_frames,
            "queued_bytes": self.frames.queued_bytes,
            "queue_byte_capacity": self.frames.max_bytes,
            "disk_blocked": self.disk_blocked,
            "free_disk_bytes": shutil.disk_usage(self.root).free,
            "compressor": compressor,
            "clock": self._clock_status(),
            "queue_wait": self._queue_wait_status(),
            "fast_json": FAST_JSON,
        }
        atomic_json(self.root / "status.json", payload)
        logging.info(
            "capture status frames=%d rows=%d dropped=%d disk_dropped=%d malformed=%d queued=%d queued_bytes=%d "
            "disk_blocked=%s compress_pending=%d compress_failed=%d compress_deferred=%d projected_gb=%s tiers=%s",
            self.received_frames,
            self.written_rows,
            self.dropped_frames,
            self.disk_dropped_frames,
            self.malformed_frames,
            self.frames.qsize(),
            self.frames.queued_bytes,
            self.disk_blocked,
            compressor["pending"],
            compressor["failed"],
            compressor["deferred"],
            budget["projected_month_gb"],
            " ".join(f"{name}:{len(symbols)}" for name, symbols in self.tier_symbols.items()),
        )


def run(config: CaptureConfig, *, root: Path | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    Recorder(config, root=root).run()
    return 0
