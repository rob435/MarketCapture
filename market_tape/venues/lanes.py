"""The hourly REST walk both venues run for the feeds no stream carries, and the per-IP standing their lanes share.

A venue serves some of what the tape records only as REST history: settled
funding, the long/short ratios, taker volume. `HourlyLane` is one thread
that walks a venue's universe for those feeds once per interval,
`offset_seconds` past the boundary (settlements fall on the hour and the
history lists them shortly after), and hands every row it has not handed
before to the writer. One walk is feed-major: every symbol of the first
feed, then of the next, each request followed by its feed's `pause`, so a
whole universe is one slow walk rather than a burst.

What the walk must not forget lives on the adapter in `LaneState`, not on the
thread: the recorder stops and restarts its lanes whenever the universe
changes (`_restart_lanes`), and a lane that forgot the last stamp it wrote
per (feed, symbol) would write its first pass again on every restart — the
newest settlement and the last hour of buckets, duplicated. A restarted lane
also skips the (feed, symbol) pairs already walked since the current slot
began, so a restart mid-walk resumes it instead of repeating it. The state
is the process's: a recorder restarted writes its first pass again, so a
reader dedupes these kinds on (symbol, the venue's stamp) across a restart.

Per (feed, symbol):

- The first fetch ever asks for `limit` rows and writes them all, or only the
  newest with `newest_only_first` (a settlement history reaches back days).
- Every later fetch writes only rows stamped after the last one written (the
  venue's own stamp, `stamp`), and asks for enough rows to reach back to it,
  `step_ms` apart, up to `max_limit`: a pass that failed or ran late loses
  no bucket the venue still serves.
- A refused request is counted, the pass's first refusal logged, and the pair
  left for the next pass; a row the writer had no room for (`emit` returned
  False) ends that pair's rows for the pass, so its stamp stays where the
  tape is and the next pass asks again. Nothing a pass meets stops the lane.

`RestBudget` is a venue's per-IP standing, shared by every lane of one
adapter: a `RateLimited` answer (HTTP 429, or 418 for an IP banned for having
ignored 429s) pauses every lane for its `Retry-After`, because the venue bans
the IP, not the endpoint; a used-weight header past `throttle_fraction` of
the venue's minute holds every lane to the next minute. Every lane waits on
it before each request. A lane's pauses wait on `stop`, so it ends at once
when told, or after the request in hand.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from market_tape.venues import Emit

#: The lane's longest wait for an outgoing lane to leave `run`, between looks at `stop`.
LANE_POLL_SECONDS = 0.25
#: A 429 or 418 without `Retry-After`: pause every lane this long.
RATE_LIMIT_DEFAULT_SECONDS = 60.0


class RateLimited(RuntimeError):
    """The venue answered 429 (over a rate limit) or 418 (banned for having
    ignored a 429): every request from the IP waits `retry_after`."""

    def __init__(self, status: int, retry_after: float) -> None:
        super().__init__(f"HTTP {status}, retry after {retry_after:g}s")
        self.status = status
        self.retry_after = retry_after


def header_number(headers: Any, name: str) -> float | None:
    """A numeric response header, or None when absent or unreadable."""

    raw = headers.get(name) if headers is not None else None
    try:
        return float(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


@dataclass
class RestBudget:
    """One host's standing against a venue's per-IP REST limits.

    It holds no clock: each lane passes its own readings, so a test drives it
    by hand. `paused_until` is on the monotonic clock, `throttled_until_ns`
    the same instant on the wall clock, for `status.json`."""

    #: The venue's request weight a minute per IP; 0 when it reports none.
    weight_per_minute: int = 0
    #: Past this share of the minute's weight, every lane waits out the minute.
    throttle_fraction: float = 0.75
    lock: threading.Lock = field(default_factory=threading.Lock)
    used_weight: int | None = None
    paused_until: float = 0.0
    throttled_until_ns: int = 0
    rate_limited: int = 0

    def wait_seconds(self, monotonic: float) -> float:
        """How long a request must wait for the budget; 0 when it may go now."""

        return max(0.0, self.paused_until - monotonic)

    def note_weight(self, used: int | None, monotonic: float, wall_ns: int) -> None:
        """The venue's used weight this minute, as its last answer stated it."""

        if used is None:
            return
        with self.lock:
            self.used_weight = used
            if self.weight_per_minute and used > self.throttle_fraction * self.weight_per_minute:
                # The venue's minute is the wall clock's; something on the host is spending it.
                wait = 60.0 - (wall_ns / 1e9) % 60.0
                self.paused_until = max(self.paused_until, monotonic + wait)
                self.throttled_until_ns = max(self.throttled_until_ns, wall_ns + int(wait * 1e9))

    def note_rate_limited(self, retry_after: float, monotonic: float, wall_ns: int) -> None:
        with self.lock:
            self.rate_limited += 1
            self.paused_until = max(self.paused_until, monotonic + retry_after)
            self.throttled_until_ns = max(self.throttled_until_ns, wall_ns + int(retry_after * 1e9))

    def status(self, wall_ns: int) -> dict[str, Any]:
        return {
            "used_weight_1m": self.used_weight,
            "throttled_until_ns": self.throttled_until_ns if self.throttled_until_ns > wall_ns else 0,
            "rate_limited": self.rate_limited,
        }


#: One symbol's venue rows, the newest `limit` of them, and the used weight the
#: answer stated (None when the venue states none).
LaneFetch = Callable[[str, int], "tuple[Sequence[Mapping[str, Any]], int | None]"]
#: (symbol, venue rows, receive stamp, after_ms) -> tape rows stamped after `after_ms`, oldest first.
LaneRows = Callable[[str, "Sequence[Mapping[str, Any]]", int, int], "list[dict[str, Any]]"]


@dataclass(frozen=True)
class LaneFeed:
    """One feed an hourly lane walks, and how to read its venue history."""

    #: The feed as a tier names it; the pass counts its rows under this name.
    name: str
    symbols: tuple[str, ...]
    fetch: LaneFetch
    rows: LaneRows
    #: The tape row's field holding the venue's stamp, in ms: what dedupe keys on.
    stamp: str
    #: Rows the first fetch of a symbol asks for.
    limit: int
    #: The venue's spacing of rows in ms and its largest page, which size a catch-up.
    step_ms: int
    max_limit: int
    #: Seconds after each request: the feed's share of the venue's limits.
    pause: float
    #: The first fetch writes only the newest row.
    newest_only_first: bool = False

    def catch_up(self, seen_ms: int, now_ms: int) -> int:
        """Rows that reach back from now to the last stamp written."""

        missed = (now_ms - seen_ms) // self.step_ms + 1 if self.step_ms > 0 else self.limit
        return int(min(max(self.limit, missed), self.max_limit))


@dataclass
class LaneState:
    """What an adapter's hourly lane keeps across lane restarts, and its health."""

    #: (feed, symbol) -> the newest venue stamp written, in ms.
    last_ms: dict[tuple[str, str], int] = field(default_factory=dict)
    #: (feed, symbol) -> the wall clock of the last fetch whose rows all reached the writer.
    walked_ns: dict[tuple[str, str], int] = field(default_factory=dict)
    #: One thread at a time in `run`: an outgoing lane may still be inside a fetch.
    run_lock: threading.Lock = field(default_factory=threading.Lock)
    lock: threading.Lock = field(default_factory=threading.Lock)
    symbols: dict[str, int] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    passes: int = 0
    failures: int = 0
    rate_limited: int = 0
    dropped: int = 0
    last_pass_ns: int = 0
    last_pass_seconds: float = 0.0
    last_error: str = ""

    def status(self) -> dict[str, Any]:
        with self.lock:
            return {
                "symbols": dict(self.symbols),
                "passes": self.passes,
                "last_pass_ns": self.last_pass_ns,
                "last_pass_seconds": round(self.last_pass_seconds, 3),
                "rows": dict(self.rows),
                "failures": self.failures,
                "rate_limited": self.rate_limited,
                "dropped": self.dropped,
                "last_error": self.last_error,
            }


@dataclass
class HourlyLane:
    """One thread walking a venue's REST history for its lane feeds; see the module doc."""

    venue: str
    feeds: tuple[LaneFeed, ...]
    emit: Emit
    stop: threading.Event
    state: LaneState = field(default_factory=LaneState)
    budget: RestBudget = field(default_factory=RestBudget)
    clock_ns: Callable[[], int] = time.time_ns
    monotonic: Callable[[], float] = time.monotonic
    #: None waits on `stop`, so a pause or a pace ends the moment the lane is told to.
    sleep: Callable[[float], object] | None = None
    interval_seconds: float = 3_600.0
    offset_seconds: float = 180.0

    @property
    def passes(self) -> int:
        return self.state.passes

    def thread(self) -> threading.Thread:
        return threading.Thread(target=self.run, name=f"tape-lane-{self.venue}-rest", daemon=True)

    def _sleep(self, seconds: float) -> None:
        if self.sleep is None:
            self.stop.wait(seconds)
        else:
            self.sleep(seconds)

    def run(self) -> None:
        while not self.state.run_lock.acquire(timeout=LANE_POLL_SECONDS):
            if self.stop.is_set():
                return
        try:
            with self.state.lock:
                self.state.symbols = {feed.name: len(feed.symbols) for feed in self.feeds}
            # The slot this start falls in: what a lane before it walked since
            # the slot began is not walked again.
            since_s = self.slot_start(self.clock_ns() / 1e9)
            while not self.stop.is_set():
                try:
                    self.pass_once(since_ns=int(since_s * 1e9))
                except Exception:  # the lane outlives anything one pass meets
                    logging.exception("%s rest lane pass failed", self.venue)
                now_s = self.clock_ns() / 1e9
                wait = self.seconds_until_next(now_s)
                if self.stop.wait(wait):
                    return
                since_s = now_s + wait
        finally:
            self.state.run_lock.release()

    def slot_start(self, now_s: float) -> float:
        """The latest `offset_seconds` past an interval boundary at or before `now_s`."""

        return ((now_s - self.offset_seconds) // self.interval_seconds) * self.interval_seconds + self.offset_seconds

    def seconds_until_next(self, now_s: float) -> float:
        """Seconds to the next `offset_seconds` past an interval boundary."""

        boundary = (now_s // self.interval_seconds) * self.interval_seconds + self.offset_seconds
        if boundary <= now_s:
            boundary += self.interval_seconds
        return boundary - now_s

    def pass_once(self, since_ns: int | None = None) -> dict[str, int]:
        """One walk of every feed's universe; returns the rows written per feed and
        the refusals. With `since_ns`, a pair whose last whole fetch is at or after
        it is skipped: a restarted lane resumes the slot's walk."""

        state = self.state
        counts = {feed.name: 0 for feed in self.feeds}
        counts["failures"] = 0
        attempted = 0
        first_error: str | None = None
        started_ns = self.clock_ns()
        completed = False
        try:
            for feed in self.feeds:
                for symbol in feed.symbols:
                    if self.stop.is_set():
                        return counts  # a stopped pass is no pass, but its rows count
                    key = (feed.name, symbol)
                    if since_ns is not None and state.walked_ns.get(key, -1) >= since_ns:
                        continue
                    wait = self.budget.wait_seconds(self.monotonic())
                    if wait > 0:
                        self._sleep(wait)
                        if self.stop.is_set():
                            return counts
                    attempted += 1
                    seen = state.last_ms.get(key)
                    limit = feed.limit if seen is None else feed.catch_up(seen, self.clock_ns() // 1_000_000)
                    try:
                        listed, used = feed.fetch(symbol, limit)
                    except RateLimited as exc:
                        self.budget.note_rate_limited(exc.retry_after, self.monotonic(), self.clock_ns())
                        counts["failures"] += 1
                        first_error = first_error or f"{symbol}: {exc}"
                        with state.lock:
                            state.rate_limited += 1
                        logging.warning("%s rest lane paused %.0fs: %s", self.venue, exc.retry_after, exc)
                    except Exception as exc:  # one refused symbol is the next pass's
                        counts["failures"] += 1
                        first_error = first_error or f"{symbol}: {exc}"
                    else:
                        self.budget.note_weight(used, self.monotonic(), self.clock_ns())
                        # Stamped as the answer arrives: a stamp taken before the
                        # fetch sat behind every stream frame the fetch took.
                        received_ns = self.clock_ns()
                        rows = feed.rows(symbol, listed, received_ns, -1 if seen is None else seen)
                        if seen is None and feed.newest_only_first:
                            rows = rows[-1:]
                        whole = True
                        for row in rows:
                            if self.emit(row) is False:
                                whole = False
                                with state.lock:
                                    state.dropped += 1
                                break
                            state.last_ms[key] = int(row[feed.stamp])
                            counts[feed.name] += 1
                        if whole:
                            state.walked_ns[key] = received_ns
                    if feed.pause > 0:
                        self._sleep(feed.pause)
            completed = True
        finally:
            with state.lock:
                state.failures += counts["failures"]
                for feed in self.feeds:
                    state.rows[feed.name] = state.rows.get(feed.name, 0) + counts[feed.name]
                if completed:
                    state.passes += 1
                    state.last_pass_ns = started_ns
                    state.last_pass_seconds = (self.clock_ns() - started_ns) / 1e9
                if first_error:
                    state.last_error = first_error[:200]
        if counts["failures"]:
            logging.warning(
                "%s rest lane: %d of %d symbols refused this pass, first %s",
                self.venue,
                counts["failures"],
                attempted,
                first_error,
            )
        logging.info(
            "%s rest lane pass %d: %s",
            self.venue,
            state.passes,
            ", ".join(f"{counts[feed.name]} {feed.name}" for feed in self.feeds),
        )
        return counts
