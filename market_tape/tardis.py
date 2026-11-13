"""Export days of tape as Tardis-shaped day files, for a reader that takes Tardis's downloads.

```text
<out>/<exchange>/<type>/<YYYY-MM-DD>/<SYMBOL>.csv.gz     one file per type, UTC day and symbol
<out>/<exchange>/manifest.jsonl                          a receipt per file: path, bytes, sha256, rows, header
<out>/<exchange>/exports.jsonl                           a line per exported symbol-day: its hours, rows, counts
```

`<exchange>` is Tardis's name for the venue (`EXCHANGES`): `bybit`, and
`binance-futures` for Binance USD-M. The headers are Tardis's documented ones
(`COLUMNS`), every time is microseconds since the epoch, `timestamp` the
venue's stamp and `local_timestamp` the recorder's receive stamp, and a day
holds the rows of its 24 hour directories. `liquidity-migration`'s
`ec convert tardis [--venue binance] --root <out>` reads the result as it
reads a Tardis download; its module doc
(`engine/ec-research/src/convert/tardis.rs`) is the contract this follows.

| Type | From the tape | Rule |
| --- | --- | --- |
| `incremental_book_L2` | book rows of one depth (`--book-depth`; Bybit 50, Binance 1000 by default) | A snapshot row is a run of `is_snapshot=true` rows, a delta a run of `false` rows. A book that lost its chain (`market_tape.book`'s rule per venue) writes nothing until the next snapshot re-bases it. Binance's REST snapshot lands on the tape after diffs it does not cover: the diffs it covers are dropped, the rest are written after it, stamped as received with it |
| `book_ticker` | book rows of depth 1 (Bybit `orderbook.1`, Binance `bookTicker`) | One row per top of book the venue pushed; an empty side is empty columns |
| `trades` | `public_trade` | `timestamp` is the trade's own time, `side` the taker's |
| `derivative_ticker` | `ticker` | The tape's ticker rows carry changed fields only; a row here carries every field the day has stated so far, and is written when one of its own columns changed. `predicted_funding_rate` is empty (neither venue states one) |
| `liquidations` | `liquidation` | `side` is the liquidating order's, as Tardis has it: a liquidated long (`position_side` `Buy`) is `sell` |

A price or size is written as the venue's own decimal text where the tape
holds it (book levels of a JSON segment); a value the tape holds as a number
(trades, tickers, a block file's levels) is written as its shortest decimal
that reads back the same, never in exponent form.

A day's file never states a `local_timestamp` behind the row before it or
outside its day: the tape's rows already run in receive order within a
symbol, and a stamp that still falls outside (an hour's first row read a
moment before the boundary, a held Binance diff) is moved to the bound and
counted (`restamped`). A day is exported only when the source holds all 24 of
its hours, unless `allow_partial`; the hours it was cut from are in its
`exports.jsonl` line either way. A symbol-day with a line there is skipped on
a later run unless `overwrite`. Each file is written as `.part`, renamed when
whole, and receipted after; the gzip stream carries no clock, so the same
tape exports to the same bytes.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from market_tape.load import META, Source, iter_rows, open_source
from market_tape.schema import (
    KIND_BOOK_DELTA,
    KIND_BOOK_SNAPSHOT,
    KIND_LIQUIDATION,
    KIND_TICKER,
    KIND_TRADE,
)

__all__ = ["COLUMNS", "EXCHANGES", "TYPES", "ExportError", "day_range", "export_days", "export_symbol_day"]

logger = logging.getLogger(__name__)

#: Tardis's exchange id for each venue the recorder records.
EXCHANGES: Mapping[str, str] = {"bybit": "bybit", "binance": "binance-futures"}
#: The book whose rows become `incremental_book_L2`, per venue, unless the caller names one.
DEFAULT_BOOK_DEPTH: Mapping[str, int] = {"bybit": 50, "binance": 1000}
#: Tardis's documented header of each type (docs.tardis.dev/downloadable-csv-files).
COLUMNS: Mapping[str, tuple[str, ...]] = {
    "incremental_book_L2": ("exchange", "symbol", "timestamp", "local_timestamp", "is_snapshot", "side", "price", "amount"),
    "book_ticker": ("exchange", "symbol", "timestamp", "local_timestamp", "ask_amount", "ask_price", "bid_price", "bid_amount"),
    "trades": ("exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"),
    "derivative_ticker": (
        "exchange",
        "symbol",
        "timestamp",
        "local_timestamp",
        "funding_timestamp",
        "funding_rate",
        "predicted_funding_rate",
        "open_interest",
        "last_price",
        "index_price",
        "mark_price",
    ),
    "liquidations": ("exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"),
}
TYPES: tuple[str, ...] = tuple(COLUMNS)
#: The tape kinds each type is cut from; a read asks for no others.
KINDS: Mapping[str, tuple[str, ...]] = {
    "incremental_book_L2": (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA),
    "book_ticker": (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA),
    "trades": (KIND_TRADE,),
    "derivative_ticker": (KIND_TICKER,),
    "liquidations": (KIND_LIQUIDATION,),
}
#: `derivative_ticker`'s value columns and the tape's ticker field each is.
TICKER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("funding_timestamp", "next_funding_time_ms"),
    ("funding_rate", "funding_rate"),
    ("open_interest", "open_interest"),
    ("last_price", "last_price"),
    ("index_price", "index_price"),
    ("mark_price", "mark_price"),
)
#: Binance diffs held while the book waits for a snapshot it can chain onto,
#: and applied diffs kept so a snapshot landing on a based book can be chained
#: onto: 100 ms diffs, so twenty minutes and two.
HELD_DIFFS = 12_000
RECENT_DIFFS = 1_200
GZIP_LEVEL = 6
US_PER_DAY = 86_400_000_000


class ExportError(RuntimeError):
    """A day that cannot be exported as asked."""


def day_range(start: str, end: str) -> list[str]:
    """Days from start to end as `YYYY-MM-DD`; end is exclusive, and end == start means that one day."""

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first:
        raise ValueError(f"end {end!r} is before start {start!r}")
    if last == first:
        return [first.isoformat()]
    return [(first + timedelta(days=offset)).isoformat() for offset in range((last - first).days)]


def _text(value: Any) -> str:
    """A number as decimal text: the tape's own where it is text, the shortest
    decimal that reads back the same where it is a float, never an exponent."""

    if isinstance(value, str):
        return value
    if value is None:
        return ""
    if isinstance(value, bool):
        raise ValueError(f"a flag is no number: {value!r}")
    if isinstance(value, int):
        return str(value)
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError(f"not a finite number: {value!r}")
    text = repr(value)
    if "e" in text or "E" in text:
        text = format(Decimal(text), "f")
    if text.endswith(".0"):
        text = text[:-2]
    return text


class _Text:
    """Text onto a gzip stream that owns its file: lines are joined a block at
    a time, and closing closes the file under the stream too."""

    BLOCK = 1 << 16

    def __init__(self, path: Path) -> None:
        self.under = path.open("wb")
        # No name and no clock in the gzip header: the same rows are the same bytes.
        self.stream = gzip.GzipFile(filename="", mode="wb", fileobj=self.under, compresslevel=GZIP_LEVEL, mtime=0)
        self.parts: list[str] = []
        self.size = 0

    def write(self, text: str) -> None:
        self.parts.append(text)
        self.size += len(text)
        if self.size >= self.BLOCK:
            self._flush()

    def _flush(self) -> None:
        if self.parts:
            self.stream.write("".join(self.parts).encode("utf-8"))
            self.parts.clear()
            self.size = 0

    def close(self) -> None:
        try:
            self._flush()
            self.stream.close()
        finally:
            self.under.close()


@dataclass(slots=True)
class _Out:
    """One type's day file of one symbol, opened at its first row."""

    path: Path
    data_type: str
    prefix: str
    floor_us: int
    ceiling_us: int
    handle: _Text | None = None
    rows: int = 0
    last_local_us: int = 0
    restamped: int = 0

    def local(self, local_us: int) -> int:
        """`local_us` held inside the day and never behind the row before it."""

        held = min(max(local_us, self.floor_us, self.last_local_us), self.ceiling_us)
        if held != local_us:
            self.restamped += 1
        self.last_local_us = held
        return held

    def write(self, line: str) -> None:
        if self.handle is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            part = self.path.with_name(self.path.name + ".part")
            self.handle = _Text(part)
            self.handle.write(",".join(COLUMNS[self.data_type]) + "\n")
        self.handle.write(self.prefix + line)
        self.rows += 1

    def finish(self) -> dict[str, Any] | None:
        """Close and rename the file; its receipt, or None when it took no row."""

        if self.handle is None:
            return None
        self.handle.close()
        self.handle = None
        part = self.path.with_name(self.path.name + ".part")
        digest = hashlib.sha256()
        with part.open("rb") as written:
            os.fsync(written.fileno())
            for chunk in iter(lambda: written.read(1 << 20), b""):
                digest.update(chunk)
        size = part.stat().st_size
        os.replace(part, self.path)
        return {
            "type": self.data_type,
            "bytes": size,
            "sha256": digest.hexdigest(),
            "rows": self.rows,
            "header": list(COLUMNS[self.data_type]),
            "restamped": self.restamped,
        }

    def discard(self) -> None:
        if self.handle is not None:
            try:
                self.handle.close()
            finally:
                self.handle = None
        self.path.with_name(self.path.name + ".part").unlink(missing_ok=True)


def _stamps(row: Mapping[str, Any], venue_field: str = "exchange_system_ts_ns") -> tuple[int, int]:
    """(`timestamp`, `local_timestamp`) in µs; a row the venue did not stamp
    takes its receive stamp as both."""

    local_us = int(row.get("local_receive_ts_ns") or 0) // 1_000
    venue_ns = int(row.get(venue_field) or 0) or int(row.get("exchange_system_ts_ns") or 0)
    return (venue_ns // 1_000 if venue_ns else local_us), local_us


@dataclass(slots=True)
class _L2:
    """One symbol's book of one depth as `incremental_book_L2` rows.

    The chain rule is `market_tape.book.Book`'s, kept here without the levels:
    this writes messages, and needs to know only whether each one still
    describes the venue's book."""

    out: _Out
    venue: str
    based: bool = False
    last_update_id: int = 0
    #: Binance: the snapshot's id while its first diff is still awaited, else 0.
    awaiting: int = 0
    held: deque[Mapping[str, Any]] = field(default_factory=lambda: deque(maxlen=HELD_DIFFS))
    recent: deque[Mapping[str, Any]] = field(default_factory=lambda: deque(maxlen=RECENT_DIFFS))
    unbased_rows: int = 0
    stale_snapshots: int = 0

    def take(self, row: Mapping[str, Any]) -> None:
        if row.get("kind") == KIND_BOOK_SNAPSHOT:
            if self.venue == "binance":
                self._binance_snapshot(row)
            else:
                self._emit(row, True)
                self.based = True
                self.last_update_id = int(row.get("update_id") or 0)
            return
        if self.venue == "binance":
            self._binance_diff(row)
            return
        if not self.based:
            self.unbased_rows += 1
            return
        update_id = int(row.get("update_id") or 0)
        if row.get("sequence_gap") or update_id != self.last_update_id + 1:
            self.based = False
            self.unbased_rows += 1
            return
        self._emit(row, False)
        self.last_update_id = update_id

    # A Binance snapshot is a REST read taken while the diffs flow, so it
    # lands after diffs it does not cover and may be older than the newest.

    def _binance_snapshot(self, row: Mapping[str, Any]) -> None:
        snapshot_id = int(row.get("update_id") or 0)
        waiting = list(self.recent if self.based else self.held)
        later = [diff for diff in waiting if int(diff.get("update_id") or 0) >= snapshot_id]
        if later and not int(later[0].get("first_update_id") or 0) <= snapshot_id:
            # Older than every diff that could chain onto it. A based book
            # stays the chain's; an unbased one waits for the next snapshot.
            self.stale_snapshots += 1
            return
        _, local_us = _stamps(row)
        self._emit(row, True)
        self.based = True
        self.last_update_id = snapshot_id
        self.awaiting = snapshot_id
        self.held.clear()
        self.recent.clear()
        for position, diff in enumerate(later):
            if not self._binance_diff(diff, local_us=local_us):
                self.held.extend(later[position + 1 :])
                return

    def _binance_diff(self, row: Mapping[str, Any], *, local_us: int | None = None) -> bool:
        if not self.based:
            if row.get("sequence_gap"):
                # A chain broken here: nothing before it can meet a snapshot.
                self.held.clear()
            self.held.append(row)
            self.unbased_rows += 1
            return False
        update_id = int(row.get("update_id") or 0)
        if self.awaiting:
            if update_id < self.awaiting:
                return True
            if not int(row.get("first_update_id") or 0) <= self.awaiting:
                return self._binance_lost(row)
            self.awaiting = 0
        elif row.get("sequence_gap") or int(row.get("previous_update_id") or 0) != self.last_update_id:
            return self._binance_lost(row)
        self._emit(row, False, local_us=local_us)
        self.last_update_id = update_id
        self.recent.append(row)
        return True

    def _binance_lost(self, row: Mapping[str, Any]) -> bool:
        self.based = False
        self.awaiting = 0
        self.held.clear()
        self.recent.clear()
        self.held.append(row)
        self.unbased_rows += 1
        return False

    def _emit(self, row: Mapping[str, Any], snapshot: bool, *, local_us: int | None = None) -> None:
        venue_us, own_local_us = _stamps(row)
        local = self.out.local(own_local_us if local_us is None else max(local_us, own_local_us))
        head = f"{venue_us},{local},{'true' if snapshot else 'false'},"
        write = self.out.write
        for side, levels in (("bid", row.get("bids") or ()), ("ask", row.get("asks") or ())):
            for price, amount in levels:
                write(f"{head}{side},{_text(price)},{_text(amount)}\n")


@dataclass(slots=True)
class _Top:
    """One symbol's depth-1 book as `book_ticker` rows."""

    out: _Out
    venue: str
    bids: dict[float, tuple[str, str]] = field(default_factory=dict)
    asks: dict[float, tuple[str, str]] = field(default_factory=dict)
    based: bool = False
    last_update_id: int = 0
    unbased_rows: int = 0

    def take(self, row: Mapping[str, Any]) -> None:
        update_id = int(row.get("update_id") or 0)
        if row.get("kind") == KIND_BOOK_SNAPSHOT:
            self.bids.clear()
            self.asks.clear()
            self.based = True
        elif not self.based or row.get("sequence_gap") or update_id != self.last_update_id + 1:
            self.based = False
            self.unbased_rows += 1
            return
        self.last_update_id = update_id
        for side, levels in ((self.bids, row.get("bids") or ()), (self.asks, row.get("asks") or ())):
            for price, amount in levels:
                key = float(price)
                if float(amount) > 0:
                    side[key] = (_text(price), _text(amount))
                else:
                    side.pop(key, None)
        venue_us, local_us = _stamps(row)
        bid = self.bids[max(self.bids)] if self.bids else ("", "")
        ask = self.asks[min(self.asks)] if self.asks else ("", "")
        self.out.write(f"{venue_us},{self.out.local(local_us)},{ask[1]},{ask[0]},{bid[0]},{bid[1]}\n")


@dataclass(slots=True)
class _Ticker:
    """One symbol's ticker rows as `derivative_ticker`: the whole state, at each change of it."""

    out: _Out
    state: dict[str, str] = field(default_factory=dict)

    def take(self, row: Mapping[str, Any]) -> None:
        values = row.get("values") or {}
        changed = False
        for column, name in TICKER_COLUMNS:
            value = values.get(name)
            if value is None:
                continue
            text = str(int(value) * 1_000) if column == "funding_timestamp" else _text(value)
            if self.state.get(column) != text:
                self.state[column] = text
                changed = True
        if not changed:
            return
        venue_us, local_us = _stamps(row)
        get = self.state.get
        self.out.write(
            f"{venue_us},{self.out.local(local_us)},{get('funding_timestamp', '')},{get('funding_rate', '')},,"
            f"{get('open_interest', '')},{get('last_price', '')},{get('index_price', '')},{get('mark_price', '')}\n"
        )


def _trade(out: _Out, row: Mapping[str, Any]) -> None:
    venue_us, local_us = _stamps(row, "exchange_ts_ns")
    side = {"Buy": "buy", "Sell": "sell"}.get(str(row.get("side")), "unknown")
    out.write(
        f"{venue_us},{out.local(local_us)},{row.get('trade_id') or ''},{side},{_text(row.get('price'))},{_text(row.get('qty'))}\n"
    )


def _liquidation(out: _Out, row: Mapping[str, Any]) -> None:
    venue_us, local_us = _stamps(row, "exchange_ts_ns")
    # Tardis states the liquidating order's side: a long is closed by a sell.
    side = {"Buy": "sell", "Sell": "buy"}.get(str(row.get("position_side")), "unknown")
    out.write(
        f"{venue_us},{out.local(local_us)},,{side},{_text(row.get('bankruptcy_price'))},{_text(row.get('qty'))}\n"
    )


def _file_symbol(symbol: str) -> str:
    return "".join("-" if char in ':\\/?*<>|"' else char for char in symbol.upper())


def export_symbol_day(
    source: Source,
    day: str,
    symbol: str,
    out: Path,
    *,
    hours: Sequence[str] | None = None,
    types: Iterable[str] = TYPES,
    book_depth: int | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    """Write one symbol's day files and return its `exports.jsonl` line, the
    files' receipts under `files`. Nothing is left behind on a failure."""

    venue = source.venue
    if venue not in EXCHANGES:
        raise ExportError(f"Tardis has no exchange for venue {venue!r}")
    exchange = EXCHANGES[venue]
    wanted = tuple(dict.fromkeys(types))
    unknown = [name for name in wanted if name not in COLUMNS]
    if unknown:
        raise ExportError(f"no such type {unknown[0]!r}; types are {', '.join(TYPES)}")
    depth = DEFAULT_BOOK_DEPTH[venue] if book_depth is None else int(book_depth)
    if hours is None:
        held = set(source.hours())
        hours = [hour for hour in (f"{day}T{index:02d}" for index in range(24)) if hour in held]
    floor_us = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
    file_symbol = _file_symbol(symbol)
    outs = {
        name: _Out(
            path=Path(out) / exchange / name / day / f"{file_symbol}.csv.gz",
            data_type=name,
            prefix=f"{exchange},{symbol},",
            floor_us=floor_us,
            ceiling_us=floor_us + US_PER_DAY - 1,
        )
        for name in wanted
    }
    book = _L2(outs["incremental_book_L2"], venue) if "incremental_book_L2" in outs else None
    top = _Top(outs["book_ticker"], venue) if "book_ticker" in outs and depth != 1 else None
    ticker = _Ticker(outs["derivative_ticker"]) if "derivative_ticker" in outs else None
    trades = outs.get("trades")
    liquidations = outs.get("liquidations")
    kinds = sorted({kind for name in wanted for kind in KINDS[name]})
    skipped_before = source.skipped_rows
    tape_rows = 0
    try:
        for row in iter_rows(source, hours, symbols=[symbol], kinds=kinds, typed=False, strict=strict):
            tape_rows += 1
            kind = row.get("kind")
            if kind == KIND_BOOK_DELTA or kind == KIND_BOOK_SNAPSHOT:
                row_depth = row.get("depth")
                if row_depth == depth:
                    if book is not None:
                        book.take(row)
                elif row_depth == 1 and top is not None:
                    top.take(row)
            elif kind == KIND_TRADE:
                if trades is not None:
                    _trade(trades, row)
            elif kind == KIND_TICKER:
                if ticker is not None:
                    ticker.take(row)
            elif kind == KIND_LIQUIDATION and liquidations is not None:
                _liquidation(liquidations, row)
        files = []
        for name, writer in outs.items():
            receipt = writer.finish()
            if receipt is not None:
                receipt.update(
                    path=str(writer.path.relative_to(out)), exchange=exchange, day=day, symbol=symbol
                )
                files.append(receipt)
    except BaseException:
        for writer in outs.values():
            writer.discard()
        raise
    return {
        "day": day,
        "symbol": symbol,
        "exchange": exchange,
        "hours": list(hours),
        "book_depth": depth,
        "types": list(wanted),
        "tape_rows": tape_rows,
        "skipped_rows": source.skipped_rows - skipped_before,
        "book_unbased_rows": book.unbased_rows if book is not None else 0,
        "book_stale_snapshots": book.stale_snapshots if book is not None else 0,
        "top_unbased_rows": top.unbased_rows if top is not None else 0,
        "restamped": sum(int(receipt["restamped"]) for receipt in files),
        "files": files,
    }


# ------------------------------------------------------------------ the days

#: A worker process's own source: opened once, by `_open_worker`.
_WORKER: dict[str, Any] = {}


def _open_worker(spec: str, cache_dir: Path | None, venue: str | None) -> None:
    _WORKER["source"] = open_source(spec, cache_dir=cache_dir, venue=venue)


def _worker_symbol_day(arguments: tuple[str, str, Path, tuple[str, ...], tuple[str, ...], int | None, bool]) -> dict[str, Any]:
    day, symbol, out, hours, types, book_depth, strict = arguments
    return export_symbol_day(
        _WORKER["source"], day, symbol, out, hours=hours, types=types, book_depth=book_depth, strict=strict
    )


def _done(path: Path) -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
                done.add((str(entry["day"]), str(entry["symbol"])))
            except (ValueError, KeyError, TypeError):
                continue  # a torn last line is no record
    return done


def _append(path: Path, entry: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def export_days(
    spec: str,
    days: Iterable[str],
    out: Path,
    *,
    cache_dir: Path | None = None,
    venue: str | None = None,
    symbols: Iterable[str] | None = None,
    types: Iterable[str] = TYPES,
    book_depth: int | None = None,
    jobs: int = 1,
    allow_partial: bool = False,
    overwrite: bool = False,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Export every symbol of each day from the source `spec` names
    (`market_tape.load.open_source`), `jobs` symbols at a time, and return the
    symbol-days written. A day missing hours is an `ExportError` before
    anything of it is written, unless `allow_partial`."""

    out = Path(out)
    source = open_source(spec, cache_dir=cache_dir, venue=venue)
    if source.venue not in EXCHANGES:
        raise ExportError(f"Tardis has no exchange for venue {source.venue!r}")
    exchange = EXCHANGES[source.venue]
    wanted_types = tuple(dict.fromkeys(types))
    unknown = [name for name in wanted_types if name not in COLUMNS]
    if unknown:
        raise ExportError(f"no such type {unknown[0]!r}; types are {', '.join(TYPES)}")
    wanted_symbols = {symbol.upper() for symbol in symbols} if symbols else None
    held = set(source.hours())
    plan: list[tuple[str, str, Path, tuple[str, ...], tuple[str, ...], int | None, bool]] = []
    exports_path = out / exchange / "exports.jsonl"
    done = set() if overwrite else _done(exports_path)
    for day in days:
        hours = tuple(hour for hour in (f"{day}T{index:02d}" for index in range(24)) if hour in held)
        if len(hours) < 24 and not allow_partial:
            raise ExportError(
                f"{spec} holds {len(hours)} of {day}'s 24 hours; a part of a day is exported only when asked (--allow-partial)"
            )
        names = sorted(
            {member.symbol for hour in hours for member in source.hour_members(hour) if member.symbol != META}
        )
        for symbol in names:
            if wanted_symbols is not None and symbol not in wanted_symbols:
                continue
            if (day, symbol) in done:
                continue
            plan.append((day, symbol, out, hours, wanted_types, book_depth, strict))
    written: list[dict[str, Any]] = []

    def record(entry: dict[str, Any]) -> None:
        for receipt in entry["files"]:
            _append(out / exchange / "manifest.jsonl", receipt)
        _append(exports_path, {**entry, "files": [receipt["path"] for receipt in entry["files"]]})
        written.append(entry)

    if jobs <= 1 or len(plan) <= 1:
        for day, symbol, _, hours, _, _, _ in plan:
            record(
                export_symbol_day(
                    source, day, symbol, out, hours=hours, types=wanted_types, book_depth=book_depth, strict=strict
                )
            )
        return written
    with ProcessPoolExecutor(max_workers=jobs, initializer=_open_worker, initargs=(spec, cache_dir, venue)) as pool:
        # In plan order, so the two ledgers read the same whatever finished first.
        for entry in pool.map(_worker_symbol_day, plan):
            record(entry)
    return written


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="market_tape tardis", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source")
    parser.add_argument("--cache", type=Path, default=None, help="local cache for archives read from rclone")
    parser.add_argument("--venue", default=None, help="the source's venue, when neither status.json nor the source name says")
    parser.add_argument("--days", required=True, help="FROM[..TO], days as YYYY-MM-DD, TO exclusive")
    parser.add_argument("--out", type=Path, required=True, help="the Tardis-shaped root to write under")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--types", nargs="*", default=list(TYPES), help=f"default: {' '.join(TYPES)}")
    parser.add_argument("--book-depth", type=int, default=None, help="the book incremental_book_L2 is cut from; default 50 (Bybit), 1000 (Binance)")
    parser.add_argument("--jobs", type=int, default=1, help="symbols exported at a time, a process each")
    parser.add_argument("--allow-partial", action="store_true", help="export a day the source holds only some hours of")
    parser.add_argument("--overwrite", action="store_true", help="export a symbol-day again though exports.jsonl has it")
    parser.add_argument("--strict", action="store_true", help="refuse a line that does not parse instead of skipping it")
    args = parser.parse_args(argv)
    start, separator, end = args.days.partition("..")
    try:
        written = export_days(
            args.source,
            day_range(start, end if separator else start),
            args.out,
            cache_dir=args.cache,
            venue=args.venue,
            symbols=args.symbols,
            types=args.types,
            book_depth=args.book_depth,
            jobs=args.jobs,
            allow_partial=args.allow_partial,
            overwrite=args.overwrite,
            strict=args.strict,
        )
    except ExportError as exc:
        parser.exit(2, f"market_tape tardis: {exc}\n")
    files = sum(len(entry["files"]) for entry in written)
    rows = sum(int(receipt["rows"]) for entry in written for receipt in entry["files"])
    restamped = sum(int(entry["restamped"]) for entry in written)
    unbased = sum(int(entry["book_unbased_rows"]) for entry in written)
    print(
        f"wrote {files} files, {rows} rows for {len(written)} symbol-days under {args.out} "
        f"(restamped {restamped}, book rows outside a chain {unbased})"
    )
    return 0
