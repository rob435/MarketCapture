"""Readings summarised over one status interval: `status.json` `clock` and `queue_wait`.

`ClockWindow` keeps a reading's count, mean, minimum and maximum; `Quantiles`
counts it in fixed buckets, so a median or 99th percentile is read to the
bucket holding it and a tail cannot move it. The venue's queue and TCP's
recovery of a lost segment sit in the tail and drag the mean, which is why
each feed class (`CLOCK_FEEDS`) gets its quantiles beside the window. The
writer folds a batch's readings in at once; the maintainer swaps the windows
out on every status write (`market_tape/recorder/core.py::Recorder::_clock_status`).
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from typing import Any

from market_tape.schema import KIND_BOOK_DELTA, KIND_BOOK_SNAPSHOT, KIND_TRADE


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
