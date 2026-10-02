"""What the rows of an hour are worth, per symbol and feed: count, chaining, anchoring, clock.

`coverage` reads the recorder's own account of an hour: what it held and every
span it did not. This reads the rows and says what they carry, which is the
question a study asks before it trusts the hour:

```text
rows, first and last receive   how much, and the span it covers
max_silence_ms                 the longest run of nothing on that feed
skew_*_ms                      local receive minus the venue's own stamp: the
                               host clock against the venue's, plus the wire
clock_steps                    rows on which the wall clock moved against the
                               host's monotonic clock by more than a slew
snapshots, deltas, gaps        the book stream as the recorder marked it
refused, chained               deltas the rebuild could not apply, and whether
                               the book it ends with is the venue's
anchored                       the feed opened on a snapshot (a book) or carried
                               one (a ticker), so the hour replays alone
ticker_fields                  distinct fields the ticker pushed
duplicate_trade_ids            prints repeated inside the last 4,096 of the symbol
```

One pass over `iter_rows`, one polars frame out. A skew that drifts across an
hour, or sits negative, is the host clock; a book that ends unchained with zero
gaps is a delta refused for a reason the recorder did not mark; a ticker with
no snapshot needs the hour before it to know its open interest.

A row stamped with both host clocks (`local_receive_mono_ns` beside
`local_receive_ts_ns`) fixes the wall clock's offset from the monotonic clock
at that instant. Between two consecutive rows of one feed that offset moves
by the clock's slew, at most a few hundred parts per million of the gap; a
move of more than a millisecond plus 0.1 % of the gap is the wall clock being
stepped, and the row it lands on is counted in `clock_steps`. Rows recorded
before the monotonic stamp count no steps. A trade's skew is against the send
stamp of the message that carried it where the row has one, and against the
print's own time on older tape.
"""

from __future__ import annotations

from array import array
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable

from market_tape.book import Book
from market_tape.schema import (
    AccountRatioRow,
    BookRow,
    FundingRow,
    KlineRow,
    LiquidationRow,
    TickerRow,
    TradeRow,
    feed_of_row,
)

__all__ = ["SCHEMA", "build_quality", "summarize"]

#: The prints a symbol's duplicate check looks back over.
TRADE_ID_WINDOW = 4_096
#: A wall-clock move against the monotonic clock is a step past this fixed
#: allowance plus `CLOCK_SLEW` of the gap between the two rows.
CLOCK_STEP_NS = 1_000_000
#: 1,000 ppm: twice the fastest rate a time daemon slews a clock at.
CLOCK_SLEW = 0.001

SCHEMA: dict[str, Any] = {
    "venue": "str",
    "symbol": "str",
    "feed": "str",
    "rows": "int",
    "first_receive_ns": "int",
    "last_receive_ns": "int",
    "max_silence_ms": "float",
    "skew_min_ms": "float",
    "skew_p50_ms": "float",
    "skew_p99_ms": "float",
    "skew_max_ms": "float",
    "snapshots": "int",
    "deltas": "int",
    "sequence_gaps": "int",
    "refused": "int",
    "chained": "bool",
    "anchored": "bool",
    "ticker_fields": "int",
    "duplicate_trade_ids": "int",
    "clock_steps": "int",
}


@dataclass(slots=True)
class _Feed:
    venue: str
    symbol: str
    feed: str
    rows: int = 0
    first_ns: int = 0
    last_ns: int = 0
    max_silence_ns: int = 0
    skews: "array[int]" = field(default_factory=lambda: array("q"))
    snapshots: int = 0
    deltas: int = 0
    sequence_gaps: int = 0
    refused: int = 0
    anchored: bool | None = None
    ticker_fields: set[str] = field(default_factory=set)
    duplicate_trade_ids: int = 0
    clock_steps: int = 0
    last_mono_ns: int = 0

    def take(self, received_ns: int, venue_ns: int, mono_ns: int = 0) -> None:
        if self.rows == 0:
            self.first_ns = received_ns
        elif received_ns - self.last_ns > self.max_silence_ns:
            self.max_silence_ns = received_ns - self.last_ns
        if mono_ns > 0:
            if self.last_mono_ns > 0:
                moved = abs((received_ns - mono_ns) - (self.last_ns - self.last_mono_ns))
                if moved > CLOCK_STEP_NS + int(abs(mono_ns - self.last_mono_ns) * CLOCK_SLEW):
                    self.clock_steps += 1
            self.last_mono_ns = mono_ns
        self.last_ns = received_ns
        self.rows += 1
        if venue_ns > 0:
            self.skews.append((received_ns - venue_ns) // 1_000_000)


def build_quality(rows: Iterable[Any]) -> "Any":
    """One frame row per (symbol, feed) seen, sorted by symbol then feed."""

    feeds: dict[tuple[str, str], _Feed] = {}
    books: dict[tuple[str, int], Book] = {}
    trade_ids: dict[str, tuple[deque[str], set[str]]] = {}

    def cell(row: Any, feed: str) -> _Feed:
        key = (row.symbol, feed)
        found = feeds.get(key)
        if found is None:
            found = feeds[key] = _Feed(row.venue, row.symbol, feed)
        return found

    for row in rows:
        if isinstance(row, BookRow):
            state = cell(row, f"book:{row.depth}")
            state.take(row.local_receive_ts_ns, row.exchange_system_ts_ns, row.local_receive_mono_ns)
            if state.anchored is None:
                state.anchored = row.snapshot
            if row.snapshot:
                state.snapshots += 1
            else:
                state.deltas += 1
                if row.sequence_gap:
                    state.sequence_gaps += 1
            book = books.get((row.symbol, row.depth))
            if book is None:
                book = books[(row.symbol, row.depth)] = Book()
            if not book.apply(row) and not row.snapshot:
                state.refused += 1
        elif isinstance(row, TradeRow):
            state = cell(row, "trades")
            state.take(
                row.local_receive_ts_ns, row.exchange_system_ts_ns or row.exchange_ts_ns, row.local_receive_mono_ns
            )
            recent = trade_ids.get(row.symbol)
            if recent is None:
                recent = trade_ids[row.symbol] = (deque(maxlen=TRADE_ID_WINDOW), set())
            window, seen = recent
            if row.trade_id:
                if row.trade_id in seen:
                    state.duplicate_trade_ids += 1
                else:
                    if len(window) == TRADE_ID_WINDOW:
                        seen.discard(window[0])
                    window.append(row.trade_id)
                    seen.add(row.trade_id)
        elif isinstance(row, TickerRow):
            state = cell(row, "ticker")
            state.take(row.local_receive_ts_ns, row.exchange_system_ts_ns, row.local_receive_mono_ns)
            if row.message_type == "snapshot":
                state.anchored = True
            elif state.anchored is None:
                state.anchored = False
            state.ticker_fields.update(row.values)
        elif isinstance(row, LiquidationRow):
            state = cell(row, "liquidations")
            state.take(row.local_receive_ts_ns, row.exchange_system_ts_ns, row.local_receive_mono_ns)
        elif isinstance(row, KlineRow):
            state = cell(row, feed_of_row({"kind": row.kind, "interval": row.interval}))
            state.take(row.local_receive_ts_ns, row.exchange_system_ts_ns, row.local_receive_mono_ns)
        elif isinstance(row, (FundingRow, AccountRatioRow)):
            # Fetched over REST after the fact: the stamp is the venue's
            # bucket, not a send time, so it says nothing about the clock.
            cell(row, "funding" if isinstance(row, FundingRow) else "account_ratio").take(row.local_receive_ts_ns, 0)

    import polars as pl

    records = []
    for (symbol, feed), state in sorted(feeds.items()):
        book = books.get((symbol, int(feed.split(":", 1)[1]))) if feed.startswith("book:") else None
        skew = _quantiles(state.skews)
        records.append(
            {
                "venue": state.venue,
                "symbol": symbol,
                "feed": feed,
                "rows": state.rows,
                "first_receive_ns": state.first_ns,
                "last_receive_ns": state.last_ns,
                "max_silence_ms": state.max_silence_ns / 1e6,
                "skew_min_ms": skew[0],
                "skew_p50_ms": skew[1],
                "skew_p99_ms": skew[2],
                "skew_max_ms": skew[3],
                "snapshots": state.snapshots,
                "deltas": state.deltas,
                "sequence_gaps": state.sequence_gaps,
                "refused": state.refused,
                "chained": book.valid if book is not None else None,
                "anchored": state.anchored,
                "ticker_fields": len(state.ticker_fields),
                "duplicate_trade_ids": state.duplicate_trade_ids,
                "clock_steps": state.clock_steps,
            }
        )
    schema = {
        name: {"str": pl.Utf8, "int": pl.Int64, "float": pl.Float64, "bool": pl.Boolean}[kind]
        for name, kind in SCHEMA.items()
    }
    return pl.DataFrame(records, schema=schema).sort(["symbol", "feed"])


def _quantiles(values: "array[int]") -> tuple[float | None, float | None, float | None, float | None]:
    if not len(values):
        return None, None, None, None
    ordered = sorted(values)
    count = len(ordered)
    return (
        float(ordered[0]),
        float(ordered[(count - 1) // 2]),
        float(ordered[min(count - 1, (99 * count) // 100)]),
        float(ordered[-1]),
    )


def summarize(frame: "Any") -> dict[str, Any]:
    """Totals by feed, the names that need a second look, and the clock."""

    import polars as pl

    by_feed: dict[str, dict[str, Any]] = {}
    for feed, part in sorted(frame.group_by("feed"), key=lambda item: str(item[0][0])):
        name = str(feed[0])
        entry: dict[str, Any] = {
            "symbols": part.height,
            "rows": int(part["rows"].sum()),
            "skew_p50_ms": _median(part["skew_p50_ms"]),
            "max_silence_ms": float(part["max_silence_ms"].max() or 0.0),
            "clock_steps": int(part["clock_steps"].sum()),
        }
        if name.startswith("book:"):
            entry["sequence_gaps"] = int(part["sequence_gaps"].sum())
            entry["refused"] = int(part["refused"].sum())
            entry["unchained"] = _names(part.filter(pl.col("chained").not_()))
            entry["unanchored"] = _names(part.filter(pl.col("anchored").not_()))
        elif name == "ticker":
            entry["unanchored"] = _names(part.filter(pl.col("anchored").not_()))
        elif name == "trades":
            entry["duplicate_trade_ids"] = int(part["duplicate_trade_ids"].sum())
        by_feed[name] = entry
    skew = frame["skew_p50_ms"].drop_nulls()
    return {
        "symbols": int(frame["symbol"].n_unique()),
        "rows": int(frame["rows"].sum()),
        "skew_p50_ms": _median(skew),
        "skew_p99_ms": float(frame["skew_p99_ms"].drop_nulls().max()) if frame["skew_p99_ms"].drop_nulls().len() else None,
        "clock_steps": int(frame["clock_steps"].sum()),
        "by_feed": by_feed,
    }


def _median(series: "Any") -> float | None:
    values = series.drop_nulls()
    return float(values.median()) if values.len() else None


def _names(part: "Any", limit: int = 20) -> list[str]:
    return sorted(part["symbol"].to_list())[:limit]
