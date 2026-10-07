"""What the tape covers: the recorder's per-hour record, and the ledger read back from it.

The recorder folds its own transitions into `CoverageFold` and writes one
record per hour under `<day>/<HH>/_meta/`, so the record rides the hourly tar
and lands in the archive beside the rows it describes. `build_ledger` reads
those records back from any source and answers one question per cell — one
UTC hour, one symbol, one feed:

```text
complete   the whole hour was recorded, granted by a tier, and undisturbed
excluded   granted, with reason codes and the spans that carry them
observed   rows are there and no record says under what conditions
absent     no record grants it and no rows carry it
```

| Reason code | The cell's feed was granted, and |
| --- | --- |
| `recorder_down` | no record window covers that span of the hour |
| `tier_membership_partial` | no tier held the symbol then |
| `shard_disconnected` | the connection carrying its topic was down |
| `book_gap` | the book lost continuity and no snapshot had re-based it |
| `shed` | the budget had given that `tier:feed` up |
| `disk_blocked` | frames were counted and thrown away for want of disk |
| `queue_overrun` | the capture queue overran on the shard carrying its topic |

Every timestamp is integer nanoseconds. A span is `[from_ns, to_ns]` and every
span in a record is clipped to that record's window.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from market_tape.config import VENUES, Feed
from market_tape.load import Source, _hour_key, _hour_text, iter_coverage, iter_rows
from market_tape.schema import coverage_payload, feed_of_row
from market_tape.storage import utc_day_hour
from market_tape.venues import VenueAdapter, adapter_for

if TYPE_CHECKING:
    import polars as pl

__all__ = [
    "Cell",
    "CoverageFold",
    "Ledger",
    "Reason",
    "build_ledger",
]

HOUR_NS = 3_600 * 1_000_000_000

STATUS_COMPLETE = "complete"
STATUS_EXCLUDED = "excluded"
STATUS_OBSERVED = "observed"
STATUS_ABSENT = "absent"

REASON_RECORDER_DOWN = "recorder_down"
REASON_MEMBERSHIP = "tier_membership_partial"
REASON_DISCONNECTED = "shard_disconnected"
REASON_BOOK_GAP = "book_gap"
REASON_SHED = "shed"
REASON_DISK_BLOCKED = "disk_blocked"
REASON_QUEUE_OVERRUN = "queue_overrun"

#: The feed of a cell whose rows were not read: the manifest counts a symbol's
#: rows, not which feed sent them.
ANY_FEED = "*"

Span = tuple[int, int]


def hour_bounds(ns: int) -> tuple[int, int]:
    start = ns - ns % HOUR_NS
    return start, start + HOUR_NS


# ------------------------------------------------------------ span algebra


def _merge(spans: Iterable[Sequence[int]]) -> list[Span]:
    merged: list[Span] = []
    for start, end in sorted((int(span[0]), int(span[1])) for span in spans if int(span[1]) > int(span[0])):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _intersect(left: Iterable[Sequence[int]], right: Iterable[Sequence[int]]) -> list[Span]:
    found: list[Span] = []
    others = _merge(right)
    for start, end in _merge(left):
        for other_start, other_end in others:
            low, high = max(start, other_start), min(end, other_end)
            if high > low:
                found.append((low, high))
    return _merge(found)


def _subtract(base: Iterable[Sequence[int]], cut: Iterable[Sequence[int]]) -> list[Span]:
    found: list[Span] = []
    cuts = _merge(cut)
    for start, end in _merge(base):
        position = start
        for cut_start, cut_end in cuts:
            if cut_end <= position or cut_start >= end:
                continue
            if cut_start > position:
                found.append((position, cut_start))
            position = max(position, cut_end)
            if position >= end:
                break
        if position < end:
            found.append((position, end))
    return found


# ------------------------------------------------------------------- fold


class _SpanSet:
    """The closed spans of one thing in this window, plus the one still open."""

    __slots__ = ("closed", "open_from")

    def __init__(self) -> None:
        self.closed: list[Span] = []
        self.open_from: int | None = None

    def start(self, ns: int) -> None:
        if self.open_from is None:
            self.open_from = ns

    def stop(self, ns: int) -> None:
        if self.open_from is not None:
            self.closed.append((self.open_from, ns))
            self.open_from = None

    def spans(self, window_from: int, window_to: int) -> list[list[int]]:
        found = list(self.closed)
        if self.open_from is not None:
            found.append((self.open_from, window_to))
        return [[start, end] for start, end in _intersect(found, [(window_from, window_to)])]

    def roll(self) -> None:
        self.closed = []

    def spent(self) -> bool:
        return not self.closed and self.open_from is None


class CoverageFold:
    """The recorder's own transitions, folded into one record per hour.

    Every method takes the instant the transition happened, because the thread
    that sees it is not the thread that writes the record: shard threads report
    connectivity, the intake thread overruns, the writer thread book gaps and
    the disk gate, the maintainer
    thread membership, shedding and the roll. The lock is what makes those
    safe together; nothing here touches the filesystem or the clock of the
    caller.
    """

    def __init__(
        self,
        *,
        venue: str,
        market: str,
        pid: int,
        started_at_ns: int,
        feeds: Mapping[str, Sequence[str]],
        now_ns: int,
    ) -> None:
        self.venue = venue
        self.market = market
        self.pid = int(pid)
        self.started_at_ns = int(started_at_ns)
        self.feeds = {tier: tuple(texts) for tier, texts in feeds.items()}
        self._lock = threading.Lock()
        self._window_from_ns = int(now_ns)
        self._hour_start_ns = hour_bounds(int(now_ns))[0]
        self._members: dict[tuple[str, str], _SpanSet] = {}
        self._shed: dict[tuple[str, str], _SpanSet] = {}
        self._disk = _SpanSet()
        self._disconnects: list[dict[str, Any]] = []
        self._disconnect_open: dict[int, dict[str, Any]] = {}
        self._gaps: list[dict[str, Any]] = []
        self._gap_open: dict[str, dict[str, Any]] = {}
        self._overruns: list[dict[str, Any]] = []
        self._shards: dict[int, dict[str, Any]] = {}
        self._counts = {"received_frames": 0, "written_rows": 0, "dropped_frames": 0, "disk_dropped_frames": 0}
        self._counts_at_window = dict(self._counts)

    @property
    def hour_end_ns(self) -> int:
        with self._lock:
            return self._hour_start_ns + HOUR_NS

    # ------------------------------------------------------------ transitions

    def members(self, tier: str, symbols: Iterable[str], now_ns: int) -> None:
        """Make `symbols` the tier's membership as of `now_ns`."""

        wanted = {symbol.upper() for symbol in symbols}
        with self._lock:
            for symbol in wanted:
                self._members.setdefault((tier, symbol), _SpanSet()).start(now_ns)
            for (name, symbol), spans in list(self._members.items()):
                if name == tier and symbol not in wanted:
                    spans.stop(now_ns)
                    if spans.spent():
                        del self._members[(name, symbol)]

    def connected(self, shard: int, tier: str, topics: Sequence[str], now_ns: int) -> None:
        with self._lock:
            self._shard_entry(shard, tier, topics)
            open_span = self._disconnect_open.pop(shard, None)
            if open_span is not None:
                self._disconnects.append({**open_span, "to_ns": now_ns})

    def disconnected(self, shard: int, tier: str, topics: Sequence[str], now_ns: int) -> None:
        with self._lock:
            self._shard_entry(shard, tier, topics)
            if shard not in self._disconnect_open:
                self._disconnect_open[shard] = {
                    "shard": shard,
                    "tier": tier,
                    "topics": list(topics),
                    "from_ns": now_ns,
                }

    def overrun(self, shard: int, now_ns: int) -> None:
        with self._lock:
            self._overruns.append({"shard": shard, "ns": now_ns})

    def book_gap(self, topic: str, symbol: str, depth: int, now_ns: int) -> None:
        with self._lock:
            if topic not in self._gap_open:
                self._gap_open[topic] = {
                    "topic": topic,
                    "symbol": symbol.upper(),
                    "depth": int(depth),
                    "from_ns": now_ns,
                }

    def book_snapshot(self, topic: str, now_ns: int) -> None:
        with self._lock:
            open_gap = self._gap_open.pop(topic, None)
            if open_gap is not None:
                self._gaps.append({**open_gap, "to_ns": now_ns})

    def shed(self, pairs: Iterable[tuple[str, str]], now_ns: int) -> None:
        """Make `pairs` the `tier:feed` set the budget has given up as of `now_ns`."""

        wanted = {(tier, feed) for tier, feed in pairs}
        with self._lock:
            for pair in wanted:
                self._shed.setdefault(pair, _SpanSet()).start(now_ns)
            for pair, spans in list(self._shed.items()):
                if pair not in wanted:
                    spans.stop(now_ns)
                    if spans.spent():
                        del self._shed[pair]

    def disk_blocked(self, blocked: bool, now_ns: int) -> None:
        with self._lock:
            if blocked:
                self._disk.start(now_ns)
            else:
                self._disk.stop(now_ns)

    def note_shards(self, statuses: Iterable[Mapping[str, Any]]) -> None:
        """The shards that exist now, with their topics and counters."""

        with self._lock:
            kept = {int(status["index"]): dict(status) for status in statuses}
            for index in self._disconnect_open:
                if index not in kept and index in self._shards:
                    kept[index] = self._shards[index]
            self._shards = kept

    def note_counts(
        self, *, received_frames: int, written_rows: int, dropped_frames: int, disk_dropped_frames: int
    ) -> None:
        with self._lock:
            self._counts = {
                "received_frames": int(received_frames),
                "written_rows": int(written_rows),
                "dropped_frames": int(dropped_frames),
                "disk_dropped_frames": int(disk_dropped_frames),
            }

    # ---------------------------------------------------------------- records

    def roll(self, hour_end_ns: int) -> dict[str, Any]:
        """Close the hour at its end and return its record; every span still open carries over."""

        with self._lock:
            payload = self._payload(hour_end_ns)
            for spans in list(self._members.values()) + list(self._shed.values()):
                spans.roll()
            self._disk.roll()
            self._disconnects = []
            self._gaps = []
            self._overruns = []
            self._counts_at_window = dict(self._counts)
            self._window_from_ns = hour_end_ns
            self._hour_start_ns = hour_bounds(hour_end_ns)[0]
            return payload

    def close(self, now_ns: int) -> dict[str, Any]:
        """The record for the part of the current hour this process was up for."""

        with self._lock:
            return self._payload(now_ns)

    def _payload(self, window_to: int) -> dict[str, Any]:
        window_from = self._window_from_ns
        hour_start = self._hour_start_ns
        hour_end = hour_start + HOUR_NS
        day, hour = utc_day_hour(hour_start)
        tiers = []
        for tier, feeds in self.feeds.items():
            members = {}
            for (name, symbol), spans in sorted(self._members.items()):
                if name != tier:
                    continue
                found = spans.spans(window_from, window_to)
                if found:
                    members[symbol] = found
            tiers.append({"name": tier, "feeds": list(feeds), "members": members})
        disconnects = []
        for span in self._disconnects + [
            {**open_span, "to_ns": window_to} for open_span in self._disconnect_open.values()
        ]:
            clipped = _intersect([(span["from_ns"], span["to_ns"])], [(window_from, window_to)])
            for start, end in clipped:
                disconnects.append({**span, "from_ns": start, "to_ns": end})
        gaps = []
        for gap in self._gaps:
            clipped = _intersect([(gap["from_ns"], gap["to_ns"])], [(window_from, window_to)])
            for start, end in clipped:
                gaps.append({**gap, "from_ns": start, "to_ns": end})
        for gap in self._gap_open.values():
            gaps.append({**gap, "from_ns": max(gap["from_ns"], window_from), "to_ns": None})
        shed = []
        for (tier, feed), spans in sorted(self._shed.items()):
            for start, end in spans.spans(window_from, window_to):
                shed.append({"tier": tier, "feed": feed, "from_ns": start, "to_ns": end})
        return coverage_payload(
            venue=self.venue,
            market=self.market,
            day=day,
            hour=hour,
            pid=self.pid,
            started_at_ns=self.started_at_ns,
            recorded_at_ns=time.time_ns(),
            window={"from_ns": window_from, "to_ns": window_to},
            hour_span={"from_ns": hour_start, "to_ns": hour_end},
            tiers=tiers,
            shards=[self._shards[index] for index in sorted(self._shards)],
            disconnects=sorted(disconnects, key=lambda span: (span["from_ns"], span["shard"])),
            overruns=[
                event for event in self._overruns if window_from <= event["ns"] <= window_to
            ],
            book_gaps=sorted(gaps, key=lambda gap: (gap["from_ns"], gap["topic"])),
            shed=shed,
            disk_blocked=self._disk.spans(window_from, window_to),
            counts={
                name: value - self._counts_at_window.get(name, 0) for name, value in self._counts.items()
            },
        )

    def _shard_entry(self, shard: int, tier: str, topics: Sequence[str]) -> None:
        entry = self._shards.setdefault(
            shard, {"index": shard, "tier": tier, "topics": [], "reconnects": 0, "resyncs": 0, "reanchors": 0}
        )
        entry["tier"] = tier
        entry["topics"] = list(topics)


# ----------------------------------------------------------------- the ledger


@dataclass(frozen=True, slots=True)
class Reason:
    code: str
    from_ns: int
    to_ns: int
    detail: str = ""


@dataclass(frozen=True, slots=True)
class Cell:
    hour: str
    symbol: str
    feed: str
    status: str
    reasons: tuple[Reason, ...] = ()
    records: int = 0
    first_receive_ns: int | None = None
    last_receive_ns: int | None = None

    @property
    def reason_codes(self) -> list[str]:
        codes: list[str] = []
        for reason in self.reasons:
            if reason.code not in codes:
                codes.append(reason.code)
        return codes


@dataclass
class Ledger:
    cells: list[Cell] = field(default_factory=list)

    def frame(self) -> "pl.DataFrame":
        # The recorder imports this module on the host, whose install holds no
        # polars (no `read` extra); only the ledger's readers need it.
        import polars as pl

        return pl.DataFrame(
            {
                "hour": [cell.hour for cell in self.cells],
                "symbol": [cell.symbol for cell in self.cells],
                "feed": [cell.feed for cell in self.cells],
                "status": [cell.status for cell in self.cells],
                "reason_codes": [cell.reason_codes for cell in self.cells],
                "records": [cell.records for cell in self.cells],
                "first_receive_ns": [cell.first_receive_ns for cell in self.cells],
                "last_receive_ns": [cell.last_receive_ns for cell in self.cells],
            },
            schema={
                "hour": pl.Utf8,
                "symbol": pl.Utf8,
                "feed": pl.Utf8,
                "status": pl.Utf8,
                "reason_codes": pl.List(pl.Utf8),
                "records": pl.Int64,
                "first_receive_ns": pl.Int64,
                "last_receive_ns": pl.Int64,
            },
        )

    def admissible(self, symbol: str, feed: str) -> list[str]:
        """The hours a study may use this (symbol, feed) in: the complete ones."""

        return sorted(
            cell.hour
            for cell in self.cells
            if cell.symbol == symbol.upper() and cell.feed == feed and cell.status == STATUS_COMPLETE
        )

    def contiguous_windows(self, symbol: str, feed: str) -> list[tuple[str, str]]:
        """Runs of consecutive complete hours as (start, end), end exclusive."""

        windows: list[tuple[str, str]] = []
        run_start: int | None = None
        previous: int | None = None
        for hour in self.admissible(symbol, feed):
            key = _hour_key(hour)
            if previous is not None and key == previous + 1:
                previous = key
                continue
            if run_start is not None and previous is not None:
                windows.append((_hour_text(run_start), _hour_text(previous + 1)))
            run_start = key
            previous = key
        if run_start is not None and previous is not None:
            windows.append((_hour_text(run_start), _hour_text(previous + 1)))
        return windows

    def summary(self) -> dict[str, Any]:
        by_reason: dict[str, int] = {}
        counts = {STATUS_COMPLETE: 0, STATUS_EXCLUDED: 0, STATUS_OBSERVED: 0, STATUS_ABSENT: 0}
        for cell in self.cells:
            counts[cell.status] = counts.get(cell.status, 0) + 1
            for code in cell.reason_codes:
                by_reason[code] = by_reason.get(code, 0) + 1
        return {"requested": len(self.cells), **counts, "by_reason": by_reason}


@dataclass
class _Observed:
    records: int = 0
    first_receive_ns: int | None = None
    last_receive_ns: int | None = None

    def add(self, records: int, first: int | None, last: int | None) -> None:
        self.records += records
        if first:
            self.first_receive_ns = first if self.first_receive_ns is None else min(self.first_receive_ns, first)
        if last:
            self.last_receive_ns = last if self.last_receive_ns is None else max(self.last_receive_ns, last)


def build_ledger(
    source: Source,
    hours: Iterable[str],
    *,
    symbols: Iterable[str] | None = None,
    feeds: Iterable[str] | None = None,
    read_rows: bool = False,
) -> Ledger:
    """The coverage of every (hour, symbol, feed) cell the source can speak for.

    `symbols` and `feeds` name the grid to answer for; without them the grid is
    what the records grant and what the receipts carry. `read_rows` walks the
    rows themselves, which is what splits an hour with no coverage record into
    one cell per feed.
    """

    wanted_symbols = {symbol.upper() for symbol in symbols} if symbols else None
    wanted_feeds = list(feeds) if feeds else None
    topics = _Topics()
    cells: list[Cell] = []
    for hour in hours:
        records = list(iter_coverage(source, [hour]))
        observed = _observed_by_symbol(source.hour_manifest(hour))
        rows = _rows_by_feed(source, hour, wanted_symbols) if read_rows else None
        cells.extend(_hour_cells(hour, records, observed, rows, wanted_symbols, wanted_feeds, topics))
    return Ledger(cells)


def _observed_by_symbol(manifest: Mapping[str, Mapping[str, Any]]) -> dict[str, _Observed]:
    found: dict[str, _Observed] = {}
    for path, receipt in manifest.items():
        symbol = str(receipt.get("symbol") or path.split("/", 1)[0]).upper()
        if symbol == "_META" or not receipt.get("records"):
            continue
        found.setdefault(symbol, _Observed()).add(
            int(receipt.get("records") or 0),
            _optional_int(receipt.get("first_receive_ns")),
            _optional_int(receipt.get("last_receive_ns")),
        )
    return found


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _rows_by_feed(source: Source, hour: str, symbols: set[str] | None) -> dict[tuple[str, str], _Observed]:
    found: dict[tuple[str, str], _Observed] = {}
    for row in iter_rows(source, [hour], symbols=sorted(symbols) if symbols else None, typed=False):
        feed = feed_of_row(row)
        if not feed:
            continue
        received = int(row.get("local_receive_ts_ns") or 0)
        found.setdefault((str(row.get("symbol") or "").upper(), feed), _Observed()).add(1, received, received)
    return found


class _Topics:
    """One venue topic per (venue, market, symbol, feed), from the venue's own
    adapter. A venue no adapter records (a converted root's) names none: its
    records carry no shard or disconnect to match one against."""

    def __init__(self) -> None:
        self._adapters: dict[tuple[str, str], VenueAdapter] = {}
        self._topics: dict[tuple[str, str, str, str], str] = {}

    def of(self, venue: str, market: str, symbol: str, feed: str) -> str:
        key = (venue, market, symbol, feed)
        if key not in self._topics and venue not in VENUES:
            self._topics[key] = ""
        if key not in self._topics:
            adapter = self._adapters.get((venue, market))
            if adapter is None:
                adapter = adapter_for(venue, market=market)
                self._adapters[(venue, market)] = adapter
            name, _, arg = feed.partition(":")
            found = adapter.topics(symbol, (Feed(name, arg or None),))
            self._topics[key] = found[0] if found else ""
        return self._topics[key]


def _hour_cells(
    hour: str,
    records: list[dict[str, Any]],
    observed: dict[str, _Observed],
    rows: dict[tuple[str, str], _Observed] | None,
    wanted_symbols: set[str] | None,
    wanted_feeds: list[str] | None,
    topics: _Topics,
) -> list[Cell]:
    granted: set[tuple[str, str]] = set()
    for record in records:
        for tier in record.get("tiers") or []:
            for symbol in tier.get("members") or {}:
                for feed in tier.get("feeds") or []:
                    granted.add((str(symbol).upper(), str(feed)))
    if rows is not None:
        carried = set(rows)
    elif records:
        # A record names the feeds; the receipts only ever counted a symbol.
        carried = set()
    else:
        carried = {(symbol, ANY_FEED) for symbol in observed}
    cells: list[Cell] = []
    for symbol, feed in _grid(granted | carried, wanted_symbols, wanted_feeds):
        counts = rows.get((symbol, feed)) if rows is not None else observed.get(symbol)
        if not records:
            status = STATUS_OBSERVED if counts is not None and counts.records else STATUS_ABSENT
            cells.append(_cell(hour, symbol, feed, status, (), counts))
            continue
        reasons = _reasons(records, symbol, feed, topics)
        if reasons is None:
            cells.append(_cell(hour, symbol, feed, STATUS_ABSENT, (), counts))
            continue
        status = STATUS_EXCLUDED if reasons else STATUS_COMPLETE
        cells.append(_cell(hour, symbol, feed, status, reasons, counts))
    return cells


def _cell(
    hour: str, symbol: str, feed: str, status: str, reasons: Sequence[Reason], counts: _Observed | None
) -> Cell:
    return Cell(
        hour=hour,
        symbol=symbol,
        feed=feed,
        status=status,
        reasons=tuple(reasons),
        records=counts.records if counts else 0,
        first_receive_ns=counts.first_receive_ns if counts else None,
        last_receive_ns=counts.last_receive_ns if counts else None,
    )


def _grid(
    pairs: set[tuple[str, str]], wanted_symbols: set[str] | None, wanted_feeds: list[str] | None
) -> list[tuple[str, str]]:
    seen: dict[str, list[str]] = {}
    for symbol, feed in sorted(pairs):
        seen.setdefault(symbol, []).append(feed)
    names = sorted(wanted_symbols) if wanted_symbols is not None else sorted(seen)
    grid: list[tuple[str, str]] = []
    for symbol in names:
        for feed in wanted_feeds if wanted_feeds is not None else seen.get(symbol) or [ANY_FEED]:
            grid.append((symbol, feed))
    return grid


def _reasons(
    records: list[dict[str, Any]], symbol: str, feed: str, topics: _Topics
) -> list[Reason] | None:
    """Why this cell is not complete; `None` when no record grants it at all."""

    hour_span = (int(records[0]["hour_span"]["from_ns"]), int(records[0]["hour_span"]["to_ns"]))
    covered = _merge(
        (int(record["window"]["from_ns"]), int(record["window"]["to_ns"])) for record in records
    )
    covered = _intersect(covered, [hour_span])
    granted: list[Span] = []
    live: list[Span] = []
    for record in records:
        for tier in record.get("tiers") or []:
            if feed not in (tier.get("feeds") or []):
                continue
            member = _merge((tier.get("members") or {}).get(symbol, []))
            if not member:
                continue
            granted.extend(member)
            shed = [
                (int(span["from_ns"]), int(span["to_ns"]))
                for span in record.get("shed") or []
                if span.get("tier") == tier.get("name") and span.get("feed") == feed
            ]
            live.extend(_subtract(member, shed))
    granted = _intersect(granted, [hour_span])
    if not granted:
        return None
    live = _intersect(live, [hour_span])
    reasons = [Reason(REASON_RECORDER_DOWN, start, end) for start, end in _subtract([hour_span], covered)]
    reasons += [Reason(REASON_MEMBERSHIP, start, end) for start, end in _subtract(covered, granted)]
    reasons += [Reason(REASON_SHED, start, end) for start, end in _subtract(granted, live)]
    for record in records:
        topic = topics.of(str(record["venue"]), str(record.get("market") or ""), symbol, feed)
        window_to = int(record["window"]["to_ns"])
        for span in record.get("disconnects") or []:
            if topic and topic in (span.get("topics") or []):
                for start, end in _intersect([(span["from_ns"], span["to_ns"])], granted):
                    reasons.append(Reason(REASON_DISCONNECTED, start, end, f"shard {span.get('shard')}"))
        for gap in record.get("book_gaps") or []:
            if str(gap.get("symbol") or "").upper() != symbol or feed != f"book:{gap.get('depth')}":
                continue
            end_ns = window_to if gap.get("to_ns") is None else int(gap["to_ns"])
            for start, end in _intersect([(gap["from_ns"], end_ns)], granted):
                reasons.append(Reason(REASON_BOOK_GAP, start, end, str(gap.get("topic") or "")))
        for start, end in _intersect(record.get("disk_blocked") or [], granted):
            reasons.append(Reason(REASON_DISK_BLOCKED, start, end))
        carriers = {
            int(shard["index"])
            for shard in record.get("shards") or []
            if topic and topic in (shard.get("topics") or [])
        }
        for event in record.get("overruns") or []:
            moment = int(event["ns"])
            if int(event.get("shard", -1)) in carriers and any(
                start <= moment <= end for start, end in granted
            ):
                reasons.append(Reason(REASON_QUEUE_OVERRUN, moment, moment, f"shard {event['shard']}"))
    return sorted(reasons, key=lambda reason: (reason.from_ns, reason.code, reason.to_ns))
