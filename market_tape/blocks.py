"""The tape as fixed-width binary blocks with an index: seek to a moment, skip what a read does not need.

A JSON segment is read whole: to find the rows of one minute, or the trades
without the book, zstd decodes the hour and `json.loads` runs on every line.
A block file holds the same rows, in the same order, so that a reader can
stop before either:

```text
file header      magic, version, codec, one JSON object: schema, venue, symbol, source
block            magic, kind, codec, group, rows, min and max local_receive_ts_ns,
                 raw and stored length, CRC-32C of the stored bytes, then the payload
                 (a string dictionary, fixed-width records, a level table for a book)
...
index            one 48-byte entry per block: offset, length, kind, group, rows,
                 min and max stamp, CRC-32C
trailer          index offset and length, the index's CRC-32C, the block count, magic
```

Rows enter in `local_receive_ts_ns` order and are cut into **groups** of
`rows_per_group` consecutive rows. A group becomes one block per row kind it
holds, so a book block is nothing but book rows and a trade block nothing
but prints: each record is fixed-width and the block's strings (level prices
and sizes as the venue's decimal text, trade ids, sides, symbols) are interned
in a dictionary at the front of the payload, which is what lets zstd fold a
book's repeated prices to nearly nothing. Every record carries its ordinal
inside the group, so a read merges a group's blocks back into the exact order
the recorder wrote, two rows with one stamp included.

A reader opens the trailer, checks the index, and decides from the index
alone which blocks to touch: a kind it does not want is never read, a group
whose stamps lie outside the window is never read, and a block it does read
is checked against its CRC before it is decoded. The rows that come back are
the JSON rows: the same keys, the same values, `parse_row` makes the same
typed rows of them.

The block payload is compressed with the `zstd` command the recorder already
requires (codec 1) or stored raw (codec 0). Every integer is little-endian.
This module is the whole contract; the recorder does not import it, so the
capture host's runtime stays without numpy.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import os
import shutil
import struct
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Iterable, Iterator, Mapping, Sequence

import google_crc32c
import numpy as np

from market_tape.load import BLOCK_SUFFIX
from market_tape.schema import (
    KIND_ACCOUNT_RATIO,
    KIND_BOOK_DELTA,
    KIND_BOOK_SNAPSHOT,
    KIND_FUNDING,
    KIND_KLINE,
    KIND_LIQUIDATION,
    KIND_TICKER,
    KIND_TRADE,
    SCHEMA_VERSION,
    SIDES,
    TICKER_INT_FIELDS,
    TICKER_VALUE_FIELDS,
    parse_row,
)

__all__ = [
    "BLOCK_SUFFIX",
    "BlockCorrupt",
    "BlockEntry",
    "BlockError",
    "BlockReader",
    "BlockSummary",
    "BlockWriter",
    "CODECS",
    "DEFAULT_ROWS_PER_GROUP",
    "RECEIPT_KIND",
    "convert_hours",
    "first_block_venue",
]

logger = logging.getLogger(__name__)

FILE_MAGIC = b"LMTAPE1\0"
INDEX_MAGIC = b"LMTIDX1\0"
BLOCK_MAGIC = b"LMBK"
VERSION = 1
#: The manifest receipt one block file leaves behind.
RECEIPT_KIND = "segment_blocks"
#: Rows per group before a block is cut: about 4 MB of raw book records.
DEFAULT_ROWS_PER_GROUP = 32_768
#: Seconds one zstd call on one block may take.
ZSTD_TIMEOUT_SECONDS = 120.0

CODEC_NONE = 0
CODEC_ZSTD = 1
CODECS = {"none": CODEC_NONE, "zstd": CODEC_ZSTD}
CODEC_NAMES = {code: name for name, code in CODECS.items()}

#: `<4s B B B B I I q q I I I I I 12x`: magic, version, kind, codec, flags,
#: group, rows, min stamp, max stamp, raw length, stored length, CRC-32C of
#: the stored bytes, dictionary length, level count, reserved.
BLOCK_HEADER = struct.Struct("<4sBBBBIIqqIIIII12x")
#: `<8s H B B I`: magic, version, codec, reserved, JSON header length.
FILE_HEADER = struct.Struct("<8sHBBI")
#: `<Q I B 3x I I q q I 4x`: offset, length, kind, group, rows, min stamp,
#: max stamp, CRC-32C, reserved.
INDEX_ENTRY = struct.Struct("<QIB3xIIqqI4x")
#: `<Q I I Q 8s`: index offset, index length, index CRC-32C, block count, magic.
TRAILER = struct.Struct("<QIIQ8s")
assert BLOCK_HEADER.size == 64 and INDEX_ENTRY.size == 48 and TRAILER.size == 32

KIND_CODES = {
    KIND_BOOK_SNAPSHOT: 1,
    KIND_BOOK_DELTA: 2,
    KIND_TRADE: 3,
    KIND_TICKER: 4,
    KIND_LIQUIDATION: 5,
    KIND_KLINE: 6,
    KIND_FUNDING: 7,
    KIND_ACCOUNT_RATIO: 8,
}
KIND_NAMES = {code: kind for kind, code in KIND_CODES.items()}

FLAG_SNAPSHOT = 1
FLAG_RESTART = 2
FLAG_GAP = 4

_COMMON = [
    ("seq", "<u4"),
    ("symbol", "<u4"),
    ("venue", "<u4"),
    ("local_receive_ts_ns", "<i8"),
    ("local_receive_mono_ns", "<i8"),
]
_BOOK = np.dtype(
    _COMMON
    + [
        ("exchange_system_ts_ns", "<i8"),
        ("exchange_engine_ts_ns", "<i8"),
        ("update_id", "<i8"),
        ("previous_update_id", "<i8"),
        ("first_update_id", "<i8"),
        ("cross_sequence", "<i8"),
        ("previous_cross_sequence", "<i8"),
        ("depth", "<u4"),
        ("level_offset", "<u4"),
        ("bids", "<u2"),
        ("asks", "<u2"),
        ("flags", "u1"),
    ]
)
_LEVEL = np.dtype([("price", "<u4"), ("size", "<u4")])
_TRADE = np.dtype(
    _COMMON
    + [
        ("exchange_system_ts_ns", "<i8"),
        ("exchange_ts_ns", "<i8"),
        ("price", "<f8"),
        ("qty", "<f8"),
        ("trade_id", "<u4"),
        ("side", "u1"),
    ]
)
_TICKER = np.dtype(
    _COMMON
    + [
        ("exchange_system_ts_ns", "<i8"),
        ("cross_sequence", "<i8"),
        ("message_type", "<u4"),
        ("present", "<u4"),
        ("values", "<f8", (len(TICKER_VALUE_FIELDS),)),
    ]
)
_LIQUIDATION = np.dtype(
    _COMMON
    + [
        ("exchange_system_ts_ns", "<i8"),
        ("exchange_ts_ns", "<i8"),
        ("qty", "<f8"),
        ("bankruptcy_price", "<f8"),
        ("position_side", "u1"),
    ]
)
_KLINE = np.dtype(
    _COMMON
    + [
        ("exchange_system_ts_ns", "<i8"),
        ("start_ms", "<i8"),
        ("end_ms", "<i8"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("volume", "<f8"),
        ("turnover", "<f8"),
        ("interval", "<u4"),
        ("confirmed", "u1"),
    ]
)
_FUNDING = np.dtype(_COMMON + [("funding_time_ms", "<i8"), ("funding_rate", "<f8")])
_ACCOUNT_RATIO = np.dtype(
    _COMMON + [("ts_ms", "<i8"), ("buy_ratio", "<f8"), ("sell_ratio", "<f8"), ("period", "<u4")]
)
RECORD_DTYPES = {
    KIND_BOOK_SNAPSHOT: _BOOK,
    KIND_BOOK_DELTA: _BOOK,
    KIND_TRADE: _TRADE,
    KIND_TICKER: _TICKER,
    KIND_LIQUIDATION: _LIQUIDATION,
    KIND_KLINE: _KLINE,
    KIND_FUNDING: _FUNDING,
    KIND_ACCOUNT_RATIO: _ACCOUNT_RATIO,
}

_COMMON_KEYS = {"kind", "venue", "symbol", "local_receive_ts_ns", "local_receive_mono_ns"}
#: Every key a row of the kind may carry. A row with a key outside its set is
#: refused: this format holds the contract's fields and never trims a row.
KNOWN_KEYS = {
    KIND_BOOK_SNAPSHOT: _COMMON_KEYS
    | {
        "depth",
        "exchange_system_ts_ns",
        "exchange_engine_ts_ns",
        "bids",
        "asks",
        "update_id",
        "previous_update_id",
        "first_update_id",
        "cross_sequence",
        "previous_cross_sequence",
        "restart_snapshot",
        "sequence_gap",
    },
    KIND_TRADE: _COMMON_KEYS | {"exchange_system_ts_ns", "exchange_ts_ns", "trade_id", "price", "qty", "side"},
    KIND_TICKER: _COMMON_KEYS | {"exchange_system_ts_ns", "message_type", "cross_sequence", "values"},
    KIND_LIQUIDATION: _COMMON_KEYS
    | {"exchange_system_ts_ns", "exchange_ts_ns", "position_side", "qty", "bankruptcy_price"},
    KIND_KLINE: _COMMON_KEYS
    | {
        "interval",
        "exchange_system_ts_ns",
        "start_ms",
        "end_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "confirmed",
    },
    KIND_FUNDING: _COMMON_KEYS | {"funding_time_ms", "funding_rate"},
    KIND_ACCOUNT_RATIO: _COMMON_KEYS | {"period", "ts_ms", "buy_ratio", "sell_ratio"},
}
KNOWN_KEYS[KIND_BOOK_DELTA] = KNOWN_KEYS[KIND_BOOK_SNAPSHOT]


class BlockError(RuntimeError):
    """A block file, block or row this format refuses."""


class BlockCorrupt(BlockError):
    """One block that is not what its index entry and header say it is."""

    def __init__(self, message: str, entry: "BlockEntry") -> None:
        super().__init__(message)
        self.entry = entry


def crc32c(data: bytes | memoryview) -> int:
    return int(google_crc32c.value(bytes(data)))


# ------------------------------------------------------------------- codecs


def _compress(raw: bytes, codec: int) -> bytes:
    if codec == CODEC_NONE:
        return raw
    if codec == CODEC_ZSTD:
        return _zstd(["zstd", "-q", "-3", "-T1", "-c"], raw, "compression")
    raise BlockError(f"unknown block codec {codec}")


def _decompress(stored: bytes, codec: int, raw_len: int) -> bytes:
    if codec == CODEC_NONE:
        raw = stored
    elif codec == CODEC_ZSTD:
        raw = _zstd(["zstd", "-dcq"], stored, "decompression")
    else:
        raise BlockError(f"unknown block codec {codec}")
    if len(raw) != raw_len:
        raise BlockError(f"block payload is {len(raw)} bytes where its header says {raw_len}")
    return raw


def _zstd(argv: list[str], data: bytes, what: str) -> bytes:
    if shutil.which("zstd") is None:
        raise BlockError("zstd is required for a zstd-coded block; write with codec none where it is absent")
    try:
        completed = subprocess.run(argv, input=data, capture_output=True, timeout=ZSTD_TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired as exc:
        raise BlockError(f"zstd {what} of one block did not finish within {ZSTD_TIMEOUT_SECONDS:g}s") from exc
    if completed.returncode != 0:
        said = completed.stderr.decode("utf-8", "replace").strip()
        raise BlockError(f"zstd {what} failed (exit {completed.returncode}): {said}")
    return completed.stdout


# --------------------------------------------------------------- dictionary


class _Interner:
    """The block's strings, each once, by first appearance."""

    def __init__(self) -> None:
        self.ids: dict[str, int] = {}
        self.strings: list[str] = []

    def id(self, text: str) -> int:
        found = self.ids.get(text)
        if found is None:
            found = self.ids[text] = len(self.strings)
            self.strings.append(text)
        return found

    def encode(self) -> bytes:
        parts = [struct.pack("<I", len(self.strings))]
        for text in self.strings:
            raw = text.encode("utf-8")
            if len(raw) > 0xFFFF:
                raise BlockError("a string longer than 65,535 bytes cannot be interned")
            parts.append(struct.pack("<H", len(raw)))
            parts.append(raw)
        return b"".join(parts)


def _decode_dictionary(payload: bytes, length: int) -> list[str]:
    if length < 4 or length > len(payload):
        raise BlockError("block dictionary length lies outside the payload")
    (count,) = struct.unpack_from("<I", payload, 0)
    strings = []
    at = 4
    for _ in range(count):
        if at + 2 > length:
            raise BlockError("block dictionary is cut short")
        (size,) = struct.unpack_from("<H", payload, at)
        at += 2
        if at + size > length:
            raise BlockError("block dictionary is cut short")
        strings.append(payload[at : at + size].decode("utf-8"))
        at += size
    if at != length:
        raise BlockError("block dictionary length does not match its strings")
    return strings


# ------------------------------------------------------------------ encoding


def _int(row: Mapping[str, Any], name: str, default: int = 0) -> int:
    value = row.get(name)
    return default if value is None else int(value)


def _float(row: Mapping[str, Any], name: str) -> float:
    value = row.get(name)
    return 0.0 if value in (None, "") else float(value)


def _encode_records(kind: str, rows: Sequence[tuple[int, Mapping[str, Any]]]) -> tuple[bytes, bytes, int, bytes]:
    """(dictionary, records, level count, levels) for one kind's rows of a group."""

    strings = _Interner()
    levels: list[tuple[int, int]] = []
    records: list[tuple[Any, ...]] = []
    for seq, row in rows:
        unknown = set(row) - KNOWN_KEYS[kind]
        if unknown:
            raise BlockError(f"a {kind} row carries fields this format cannot hold: {sorted(unknown)}")
        common = (
            seq,
            strings.id(str(row.get("symbol") or "")),
            strings.id(str(row.get("venue") or "")),
            _int(row, "local_receive_ts_ns"),
            _int(row, "local_receive_mono_ns"),
        )
        if kind in (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA):
            offset = len(levels)
            bids = row.get("bids") or []
            asks = row.get("asks") or []
            for side in (bids, asks):
                for level in side:
                    levels.append((strings.id(str(level[0])), strings.id(str(level[1]))))
            flags = (
                (FLAG_SNAPSHOT if kind == KIND_BOOK_SNAPSHOT else 0)
                | (FLAG_RESTART if row.get("restart_snapshot") else 0)
                | (FLAG_GAP if row.get("sequence_gap") else 0)
            )
            if len(bids) > 0xFFFF or len(asks) > 0xFFFF:
                raise BlockError("a book row with more than 65,535 levels on one side cannot be held")
            records.append(
                common
                + (
                    _int(row, "exchange_system_ts_ns"),
                    _int(row, "exchange_engine_ts_ns"),
                    _int(row, "update_id"),
                    _int(row, "previous_update_id"),
                    _int(row, "first_update_id"),
                    _int(row, "cross_sequence"),
                    _int(row, "previous_cross_sequence"),
                    _int(row, "depth"),
                    offset,
                    len(bids),
                    len(asks),
                    flags,
                )
            )
        elif kind == KIND_TRADE:
            side = str(row.get("side") or "")
            if side not in SIDES:
                raise BlockError(f"trade side must be Buy or Sell, got {side!r}")
            records.append(
                common
                + (
                    _int(row, "exchange_system_ts_ns"),
                    _int(row, "exchange_ts_ns"),
                    _float(row, "price"),
                    _float(row, "qty"),
                    strings.id(str(row.get("trade_id") or "")),
                    SIDES.index(side),
                )
            )
        elif kind == KIND_TICKER:
            values = row.get("values") or {}
            unknown_values = set(values) - set(TICKER_VALUE_FIELDS)
            if unknown_values:
                raise BlockError(f"ticker values outside the contract: {sorted(unknown_values)}")
            present = 0
            column = [0.0] * len(TICKER_VALUE_FIELDS)
            for index, name in enumerate(TICKER_VALUE_FIELDS):
                if name in values:
                    present |= 1 << index
                    column[index] = float(values[name])
            records.append(
                common
                + (
                    _int(row, "exchange_system_ts_ns"),
                    _int(row, "cross_sequence"),
                    strings.id(str(row.get("message_type") or "")),
                    present,
                    tuple(column),
                )
            )
        elif kind == KIND_LIQUIDATION:
            side = str(row.get("position_side") or "")
            if side not in SIDES:
                raise BlockError(f"liquidation side must be Buy or Sell, got {side!r}")
            records.append(
                common
                + (
                    _int(row, "exchange_system_ts_ns"),
                    _int(row, "exchange_ts_ns"),
                    _float(row, "qty"),
                    _float(row, "bankruptcy_price"),
                    SIDES.index(side),
                )
            )
        elif kind == KIND_KLINE:
            records.append(
                common
                + (
                    _int(row, "exchange_system_ts_ns"),
                    _int(row, "start_ms"),
                    _int(row, "end_ms"),
                    _float(row, "open"),
                    _float(row, "high"),
                    _float(row, "low"),
                    _float(row, "close"),
                    _float(row, "volume"),
                    _float(row, "turnover"),
                    strings.id(str(row.get("interval") or "")),
                    1 if row.get("confirmed") else 0,
                )
            )
        elif kind == KIND_FUNDING:
            records.append(common + (_int(row, "funding_time_ms"), _float(row, "funding_rate")))
        elif kind == KIND_ACCOUNT_RATIO:
            records.append(
                common
                + (
                    _int(row, "ts_ms"),
                    _float(row, "buy_ratio"),
                    _float(row, "sell_ratio"),
                    strings.id(str(row.get("period") or "")),
                )
            )
        else:  # pragma: no cover - the caller routes by KIND_CODES
            raise BlockError(f"unknown row kind {kind!r}")
    array = np.array(records, dtype=RECORD_DTYPES[kind])
    level_array = np.array(levels, dtype=_LEVEL)
    return strings.encode(), array.tobytes(), len(levels), level_array.tobytes()


# ------------------------------------------------------------------ decoding


def _decode_records(kind: str, strings: list[str], records: np.ndarray, levels: np.ndarray) -> list[tuple[int, dict[str, Any]]]:
    out: list[tuple[int, dict[str, Any]]] = []
    if kind in (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA):
        level_list = levels.tolist()
        for record in records.tolist():
            (seq, symbol, venue, received, mono, system, engine, update, previous, first, cross, previous_cross,
             depth, offset, bids, asks, flags) = record
            side_a = [[strings[p], strings[s]] for p, s in level_list[offset : offset + bids]]
            side_b = [[strings[p], strings[s]] for p, s in level_list[offset + bids : offset + bids + asks]]
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "depth": depth,
                        "local_receive_ts_ns": received,
                        "local_receive_mono_ns": mono,
                        "exchange_system_ts_ns": system,
                        "exchange_engine_ts_ns": engine,
                        "bids": side_a,
                        "asks": side_b,
                        "update_id": update,
                        "previous_update_id": previous,
                        "first_update_id": first,
                        "cross_sequence": cross,
                        "previous_cross_sequence": previous_cross,
                        "restart_snapshot": bool(flags & FLAG_RESTART),
                        "sequence_gap": bool(flags & FLAG_GAP),
                    },
                )
            )
    elif kind == KIND_TRADE:
        for seq, symbol, venue, received, mono, system, at, price, qty, trade_id, side in records.tolist():
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "local_receive_ts_ns": received,
                        "local_receive_mono_ns": mono,
                        "exchange_system_ts_ns": system,
                        "exchange_ts_ns": at,
                        "trade_id": strings[trade_id],
                        "price": price,
                        "qty": qty,
                        "side": SIDES[side],
                    },
                )
            )
    elif kind == KIND_TICKER:
        for seq, symbol, venue, received, mono, system, cross, message_type, present, column in records.tolist():
            values: dict[str, float | int] = {}
            for index, name in enumerate(TICKER_VALUE_FIELDS):
                if present & (1 << index):
                    values[name] = int(column[index]) if name in TICKER_INT_FIELDS else column[index]
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "local_receive_ts_ns": received,
                        "local_receive_mono_ns": mono,
                        "exchange_system_ts_ns": system,
                        "message_type": strings[message_type],
                        "cross_sequence": cross,
                        "values": values,
                    },
                )
            )
    elif kind == KIND_LIQUIDATION:
        for seq, symbol, venue, received, mono, system, at, qty, price, side in records.tolist():
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "local_receive_ts_ns": received,
                        "local_receive_mono_ns": mono,
                        "exchange_system_ts_ns": system,
                        "exchange_ts_ns": at,
                        "position_side": SIDES[side],
                        "qty": qty,
                        "bankruptcy_price": price,
                    },
                )
            )
    elif kind == KIND_KLINE:
        for record in records.tolist():
            (seq, symbol, venue, received, mono, system, start, end, open_, high, low, close, volume, turnover,
             interval, confirmed) = record
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "interval": strings[interval],
                        "local_receive_ts_ns": received,
                        "local_receive_mono_ns": mono,
                        "exchange_system_ts_ns": system,
                        "start_ms": start,
                        "end_ms": end,
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                        "turnover": turnover,
                        "confirmed": bool(confirmed),
                    },
                )
            )
    elif kind == KIND_FUNDING:
        for seq, symbol, venue, received, mono, funding_time, rate in records.tolist():
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "local_receive_ts_ns": received,
                        "funding_time_ms": funding_time,
                        "funding_rate": rate,
                    },
                )
            )
    elif kind == KIND_ACCOUNT_RATIO:
        for seq, symbol, venue, received, mono, ts, buy, sell, period in records.tolist():
            out.append(
                (
                    seq,
                    {
                        "kind": kind,
                        "venue": strings[venue],
                        "symbol": strings[symbol],
                        "local_receive_ts_ns": received,
                        "period": strings[period],
                        "ts_ms": ts,
                        "buy_ratio": buy,
                        "sell_ratio": sell,
                    },
                )
            )
    else:  # pragma: no cover - KIND_NAMES routes here
        raise BlockError(f"unknown row kind {kind!r}")
    return out


# -------------------------------------------------------------------- writer


@dataclass(frozen=True, slots=True)
class BlockEntry:
    """One index entry: where a block is and what it holds, read without touching it."""

    offset: int
    length: int
    kind: str
    group: int
    rows: int
    min_ts: int
    max_ts: int
    crc32c: int

    def overlaps(self, start_ns: int | None, end_ns: int | None) -> bool:
        """Whether any of the block's rows can lie in `[start_ns, end_ns)`."""

        if start_ns is not None and self.max_ts < start_ns:
            return False
        if end_ns is not None and self.min_ts >= end_ns:
            return False
        return True


@dataclass(frozen=True, slots=True)
class BlockSummary:
    """What one finished block file holds, for its receipt."""

    path: Path
    rows: int
    blocks: int
    groups: int
    bytes: int
    first_receive_ns: int
    last_receive_ns: int
    sha256: str
    kinds: dict[str, int]


class BlockWriter:
    """Rows in, one block file out. `append` rows in `local_receive_ts_ns`
    order; `close` writes the index and returns the summary. The file is
    written as `<path>.partial` and takes its name only once the trailer is
    down, so a reader never meets a file without an index."""

    def __init__(
        self,
        path: Path,
        *,
        venue: str,
        symbol: str | None = None,
        codec: str = "zstd",
        rows_per_group: int = DEFAULT_ROWS_PER_GROUP,
        source: str | None = None,
    ) -> None:
        if codec not in CODECS:
            raise BlockError(f"codec must be one of {sorted(CODECS)}, got {codec!r}")
        if rows_per_group <= 0:
            raise BlockError("rows_per_group must be positive")
        self.path = Path(path)
        self.venue = venue
        self.symbol = symbol
        self.codec = CODECS[codec]
        self.rows_per_group = rows_per_group
        self.partial = self.path.with_name(self.path.name + ".partial")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle: IO[bytes] | None = self.partial.open("xb")
        self._digest = hashlib.sha256()
        self._entries: list[BlockEntry] = []
        self._buffer: list[dict[str, Any]] = []
        self._group = 0
        self._rows = 0
        self._last_ts: int | None = None
        self._first_ts = 0
        self._kinds: dict[str, int] = {}
        header = json.dumps(
            {
                "format": "lmtb",
                "version": VERSION,
                "schema": SCHEMA_VERSION,
                "venue": venue,
                "symbol": symbol,
                "codec": codec,
                "rows_per_group": rows_per_group,
                "source": source,
                "created_ns": time.time_ns(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self._write(FILE_HEADER.pack(FILE_MAGIC, VERSION, self.codec, 0, len(header)) + header)

    def _write(self, data: bytes) -> None:
        assert self._handle is not None
        self._handle.write(data)
        self._digest.update(data)

    def append(self, row: Mapping[str, Any]) -> None:
        kind = row.get("kind")
        if kind not in KIND_CODES:
            raise BlockError(f"unknown row kind {kind!r}")
        received = _int(row, "local_receive_ts_ns")
        if received <= 0:
            raise BlockError("a row needs a positive local_receive_ts_ns")
        if self._last_ts is not None and received < self._last_ts:
            raise BlockError(f"rows must arrive in local_receive_ts_ns order; {received} follows {self._last_ts}")
        if self.symbol is not None and str(row.get("symbol") or "") != self.symbol:
            raise BlockError(f"this file is {self.symbol}; a {row.get('symbol')!r} row does not belong in it")
        unknown = set(row) - KNOWN_KEYS[str(kind)]
        if unknown:
            raise BlockError(f"a {kind} row carries fields this format cannot hold: {sorted(unknown)}")
        self._buffer.append(dict(row))
        if self._rows == 0:
            self._first_ts = received
        self._last_ts = received
        self._rows += 1
        self._kinds[str(kind)] = self._kinds.get(str(kind), 0) + 1
        if len(self._buffer) >= self.rows_per_group:
            self._flush_group()

    def _flush_group(self) -> None:
        if not self._buffer:
            return
        by_kind: dict[str, list[tuple[int, Mapping[str, Any]]]] = {}
        for seq, row in enumerate(self._buffer):
            by_kind.setdefault(str(row["kind"]), []).append((seq, row))
        assert self._handle is not None
        for kind in sorted(by_kind, key=lambda name: KIND_CODES[name]):
            rows = by_kind[kind]
            dictionary, records, level_count, levels = _encode_records(kind, rows)
            raw = dictionary + records + levels
            stored = _compress(raw, self.codec)
            crc = crc32c(stored)
            stamps = [_int(row, "local_receive_ts_ns") for _, row in rows]
            header = BLOCK_HEADER.pack(
                BLOCK_MAGIC,
                VERSION,
                KIND_CODES[kind],
                self.codec,
                0,
                self._group,
                len(rows),
                min(stamps),
                max(stamps),
                len(raw),
                len(stored),
                crc,
                len(dictionary),
                level_count,
            )
            offset = self._handle.tell()
            self._write(header + stored)
            self._entries.append(
                BlockEntry(
                    offset=offset,
                    length=len(header) + len(stored),
                    kind=kind,
                    group=self._group,
                    rows=len(rows),
                    min_ts=min(stamps),
                    max_ts=max(stamps),
                    crc32c=crc,
                )
            )
        self._group += 1
        self._buffer = []

    def close(self) -> BlockSummary:
        if self._handle is None:
            raise BlockError("this writer is closed")
        self._flush_group()
        index = b"".join(
            INDEX_ENTRY.pack(
                entry.offset,
                entry.length,
                KIND_CODES[entry.kind],
                entry.group,
                entry.rows,
                entry.min_ts,
                entry.max_ts,
                entry.crc32c,
            )
            for entry in self._entries
        )
        index_offset = self._handle.tell()
        self._write(index)
        self._write(TRAILER.pack(index_offset, len(index), crc32c(index), len(self._entries), INDEX_MAGIC))
        self._handle.flush()
        os.fsync(self._handle.fileno())
        size = self._handle.tell()
        self._handle.close()
        self._handle = None
        os.replace(self.partial, self.path)
        return BlockSummary(
            path=self.path,
            rows=self._rows,
            blocks=len(self._entries),
            groups=self._group,
            bytes=size,
            first_receive_ns=self._first_ts,
            last_receive_ns=self._last_ts or 0,
            sha256=self._digest.hexdigest(),
            kinds=dict(sorted(self._kinds.items())),
        )

    def abandon(self) -> None:
        """Drop the partial file after a failure part-way."""

        if self._handle is not None:
            self._handle.close()
            self._handle = None
        self.partial.unlink(missing_ok=True)


# -------------------------------------------------------------------- reader


class BlockReader:
    """One block file: its header, its index, and the blocks a read asks for.

    `source` is a path or a seekable binary file (an entry extracted from a
    tar, say); a path is opened here and closed by `close()`.
    """

    def __init__(self, source: Path | str | IO[bytes], *, label: str | None = None) -> None:
        if isinstance(source, (str, Path)):
            self._file: IO[bytes] = Path(source).open("rb")
            self._owns = True
            self.label = label or str(source)
        else:
            self._file = source
            self._owns = False
            self.label = str(label or getattr(source, "name", "<block file>"))
        self.blocks_read = 0
        self.bytes_read = 0
        try:
            self.header = self._read_header()
            self.entries = self._read_index()
        except BlockError:
            self.close()
            raise

    def __enter__(self) -> "BlockReader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns:
            self._file.close()

    @property
    def venue(self) -> str:
        return str(self.header.get("venue") or "")

    @property
    def symbol(self) -> str | None:
        symbol = self.header.get("symbol")
        return str(symbol) if symbol else None

    @property
    def codec(self) -> str:
        return CODEC_NAMES.get(int(self.header.get("_codec", -1)), "unknown")

    def _read_header(self) -> dict[str, Any]:
        self._file.seek(0)
        fixed = self._file.read(FILE_HEADER.size)
        if len(fixed) < FILE_HEADER.size:
            raise BlockError(f"{self.label}: too short to be a block file")
        magic, version, codec, _, length = FILE_HEADER.unpack(fixed)
        if magic != FILE_MAGIC:
            raise BlockError(f"{self.label}: not a block file (magic {magic!r})")
        if version != VERSION:
            raise BlockError(f"{self.label}: block file version {version} is not {VERSION}")
        if codec not in CODEC_NAMES:
            raise BlockError(f"{self.label}: unknown codec {codec}")
        raw = self._file.read(length)
        if len(raw) < length:
            raise BlockError(f"{self.label}: file header is cut short")
        try:
            header = json.loads(raw)
        except ValueError as exc:
            raise BlockError(f"{self.label}: file header is not JSON: {exc}") from exc
        if not isinstance(header, dict):
            raise BlockError(f"{self.label}: file header is not an object")
        header["_codec"] = codec
        header["_body_offset"] = FILE_HEADER.size + length
        return header

    def _read_index(self) -> list[BlockEntry]:
        self._file.seek(0, os.SEEK_END)
        size = self._file.tell()
        if size < TRAILER.size:
            raise BlockError(f"{self.label}: no trailer; the file was never finished")
        self._file.seek(size - TRAILER.size)
        index_offset, index_len, index_crc, count, magic = TRAILER.unpack(self._file.read(TRAILER.size))
        if magic != INDEX_MAGIC:
            raise BlockError(f"{self.label}: no index trailer; the file was never finished")
        if index_len != count * INDEX_ENTRY.size or index_offset + index_len + TRAILER.size != size:
            raise BlockError(f"{self.label}: index trailer does not describe this file")
        self._file.seek(index_offset)
        index = self._file.read(index_len)
        if len(index) != index_len:
            raise BlockError(f"{self.label}: index is cut short")
        if crc32c(index) != index_crc:
            raise BlockError(f"{self.label}: index CRC does not match")
        entries = []
        for at in range(0, index_len, INDEX_ENTRY.size):
            offset, length, kind, group, rows, min_ts, max_ts, crc = INDEX_ENTRY.unpack_from(index, at)
            if kind not in KIND_NAMES:
                raise BlockError(f"{self.label}: index names unknown kind {kind}")
            entries.append(BlockEntry(offset, length, KIND_NAMES[kind], group, rows, min_ts, max_ts, crc))
        return entries

    # ---------------------------------------------------------------- reads

    def groups(self) -> list[tuple[int, int, int]]:
        """(group, min stamp, max stamp) per group, in file order."""

        seen: dict[int, tuple[int, int]] = {}
        for entry in self.entries:
            low, high = seen.get(entry.group, (entry.min_ts, entry.max_ts))
            seen[entry.group] = (min(low, entry.min_ts), max(high, entry.max_ts))
        return [(group, low, high) for group, (low, high) in sorted(seen.items())]

    def select(
        self, *, kinds: Iterable[str] | None = None, start_ns: int | None = None, end_ns: int | None = None
    ) -> list[BlockEntry]:
        """The blocks a read with these filters touches, decided from the index alone."""

        wanted = set(kinds) if kinds is not None else None
        return [
            entry
            for entry in self.entries
            if (wanted is None or entry.kind in wanted) and entry.overlaps(start_ns, end_ns)
        ]

    def read_block(self, entry: BlockEntry) -> list[tuple[int, dict[str, Any]]]:
        """One block's rows as `(seq, row)`, checked against its CRC."""

        self._file.seek(entry.offset)
        data = self._file.read(entry.length)
        self.blocks_read += 1
        self.bytes_read += len(data)
        where = f"{self.label}: block at {entry.offset}"
        if len(data) != entry.length:
            raise BlockCorrupt(f"{where} is cut short", entry)
        fields = BLOCK_HEADER.unpack_from(data, 0)
        magic, version, kind_code, codec, _, group, rows, min_ts, max_ts, raw_len, stored_len, crc, dict_len, level_count = fields
        if magic != BLOCK_MAGIC or version != VERSION:
            raise BlockCorrupt(f"{where} has no block header", entry)
        if KIND_NAMES.get(kind_code) != entry.kind or group != entry.group or rows != entry.rows:
            raise BlockCorrupt(f"{where} is not the block its index entry describes", entry)
        stored = data[BLOCK_HEADER.size :]
        if len(stored) != stored_len:
            raise BlockCorrupt(f"{where} holds {len(stored)} bytes where its header says {stored_len}", entry)
        if crc32c(stored) != crc or crc != entry.crc32c:
            raise BlockCorrupt(f"{where} fails its CRC-32C", entry)
        try:
            raw = _decompress(stored, codec, raw_len)
            strings = _decode_dictionary(raw, dict_len)
        except BlockError as exc:
            raise BlockCorrupt(f"{where}: {exc}", entry) from exc
        dtype = RECORD_DTYPES[entry.kind]
        records_end = dict_len + rows * dtype.itemsize
        levels_end = records_end + level_count * _LEVEL.itemsize
        if levels_end != len(raw):
            raise BlockCorrupt(f"{where} payload does not match its counts", entry)
        records = np.frombuffer(raw, dtype=dtype, count=rows, offset=dict_len)
        levels = np.frombuffer(raw, dtype=_LEVEL, count=level_count, offset=records_end)
        try:
            decoded = _decode_records(entry.kind, strings, records, levels)
        except (IndexError, ValueError) as exc:
            raise BlockCorrupt(f"{where} names a string its dictionary lacks: {exc}", entry) from exc
        if any(not (min_ts <= row["local_receive_ts_ns"] <= max_ts) for _, row in decoded):
            raise BlockCorrupt(f"{where} holds a row outside its stamped span", entry)
        return decoded

    def rows(
        self,
        *,
        kinds: Iterable[str] | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
        typed: bool = False,
    ) -> Iterator[tuple[int, Any]]:
        """`(local_receive_ts_ns, row)` in the recorder's order, from the blocks the filters select.

        A group's blocks are merged back by each record's ordinal, so kinds
        interleave exactly as they were written. Only groups whose stamps
        meet the window are read, and within them only the kinds asked for.
        """

        wanted = set(kinds) if kinds is not None else None
        by_group: dict[int, list[BlockEntry]] = {}
        for entry in self.select(kinds=wanted, start_ns=start_ns, end_ns=end_ns):
            by_group.setdefault(entry.group, []).append(entry)
        for group in sorted(by_group):
            streams = [iter(self.read_block(entry)) for entry in by_group[group]]
            for _, row in heapq.merge(*streams, key=lambda pair: pair[0]):
                received = int(row["local_receive_ts_ns"])
                if start_ns is not None and received < start_ns:
                    continue
                if end_ns is not None and received >= end_ns:
                    continue
                if typed:
                    yield received, parse_row(row)
                else:
                    yield received, row

    def verify(self) -> int:
        """Read and check every block; the number of rows they hold."""

        return sum(len(self.read_block(entry)) for entry in self.entries)

    def describe(self) -> dict[str, Any]:
        header = {key: value for key, value in self.header.items() if not key.startswith("_")}
        return {
            "header": header,
            "codec": self.codec,
            "blocks": len(self.entries),
            "groups": len(self.groups()),
            "rows": sum(entry.rows for entry in self.entries),
            "first_receive_ns": min((entry.min_ts for entry in self.entries), default=None),
            "last_receive_ns": max((entry.max_ts for entry in self.entries), default=None),
            "rows_by_kind": {
                kind: sum(entry.rows for entry in self.entries if entry.kind == kind)
                for kind in sorted({entry.kind for entry in self.entries}, key=lambda name: KIND_CODES[name])
            },
            "bytes_by_kind": {
                kind: sum(entry.length for entry in self.entries if entry.kind == kind)
                for kind in sorted({entry.kind for entry in self.entries}, key=lambda name: KIND_CODES[name])
            },
        }


def first_block_venue(root: Path) -> str | None:
    """The venue the first block file under a root names, for a root with no status file."""

    for day in sorted(p for p in root.iterdir() if p.is_dir()):
        for hour in sorted(p for p in day.iterdir() if p.is_dir()):
            for symbol in sorted(p for p in hour.iterdir() if p.is_dir()):
                for path in sorted(symbol.iterdir()):
                    if path.name.endswith(BLOCK_SUFFIX):
                        with BlockReader(path) as reader:
                            return reader.venue or None
    return None


# ----------------------------------------------------------------- converter


def convert_hours(
    source: Any,
    hours: Iterable[str],
    out_root: Path,
    *,
    symbols: Iterable[str] | None = None,
    codec: str = "zstd",
    rows_per_group: int = DEFAULT_ROWS_PER_GROUP,
    strict: bool = False,
    copy_meta: bool = True,
) -> list[dict[str, Any]]:
    """Every symbol of the named hours as one block file each under
    `out_root/<day>/<HH>/<SYMBOL>/segment-000000.lmtb`, the hour's `_meta`
    files copied beside them, and one `segment_blocks` receipt per file in
    `out_root/manifest.jsonl`. Returns the receipts written."""

    from market_tape.load import META, HOUR_KEY_RE, iter_rows
    from market_tape.storage import Manifest

    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(out_root)
    wanted = {symbol.upper() for symbol in symbols} if symbols else None
    receipts: list[dict[str, Any]] = []
    for hour in hours:
        match = HOUR_KEY_RE.match(hour)
        if match is None:
            raise BlockError(f"an hour is YYYY-MM-DDTHH, got {hour!r}")
        day, hh = match.group(1), match.group(2)
        members = source.hour_members(hour)
        by_symbol: dict[str, list[Any]] = {}
        for member in members:
            if member.symbol == META:
                if copy_meta:
                    destination = out_root / day / hh / META / Path(member.path).name
                    if not destination.exists():
                        _copy_member(member, destination)
                continue
            if wanted is None or member.symbol in wanted:
                by_symbol.setdefault(member.symbol, []).append(member)
        for symbol in sorted(by_symbol):
            relative = Path(day) / hh / symbol / f"segment-000000{BLOCK_SUFFIX}"
            path = out_root / relative
            if path.exists():
                logger.info("%s already converted; leaving it", relative)
                continue
            writer = BlockWriter(
                path,
                venue=source.venue,
                symbol=symbol,
                codec=codec,
                rows_per_group=rows_per_group,
                source=",".join(member.path for member in by_symbol[symbol]),
            )
            try:
                for row in iter_rows(source, [hour], symbols=[symbol], typed=False, strict=strict):
                    writer.append(row)
                summary = writer.close()
            except BaseException:
                writer.abandon()
                raise
            receipt = {
                "kind": RECEIPT_KIND,
                "recorded_at_ns": time.time_ns(),
                "path": str(relative),
                "symbol": symbol,
                "day": day,
                "hour": hh,
                "records": summary.rows,
                "first_receive_ns": summary.first_receive_ns,
                "last_receive_ns": summary.last_receive_ns,
                "compressed_bytes": summary.bytes,
                "blocks": summary.blocks,
                "groups": summary.groups,
                "rows_by_kind": summary.kinds,
                "sha256": summary.sha256,
                "sources": [member.path for member in by_symbol[symbol]],
            }
            manifest.append(receipt)
            receipts.append(receipt)
    return receipts


def _copy_member(member: Any, destination: Path) -> None:
    """One member's bytes as the source stores them, under the block root."""

    from market_tape.load import FileMember, TarMember

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    if isinstance(member, FileMember):
        shutil.copyfile(member.file_path, temporary)
    elif isinstance(member, TarMember):
        import tarfile

        with tarfile.open(member.archive, "r") as handle:
            extracted = handle.extractfile(member.info if member.info is not None else member.name)
            if extracted is None:
                raise BlockError(f"{member.archive}: {member.name} holds no data")
            with temporary.open("wb") as out:
                shutil.copyfileobj(extracted, out)
    else:
        raise BlockError(f"cannot copy a {type(member).__name__} member byte for byte")
    os.replace(temporary, destination)
