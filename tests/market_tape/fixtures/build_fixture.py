"""Rebuild the fixture hour from the recorded tape sample, then repack and re-derive it.

Run from the repository root:

```text
.venv/bin/python tests/market_tape/fixtures/build_fixture.py SAMPLE.tar.zst
```

The rows are real Bybit linear rows from the recorded sample, each given what
the Bybit normalizer (`market_tape/venues/bybit.py::BybitAdapter`) writes and
the sample lacks: the row's `venue` and a book row's `first_update_id` (0:
Bybit publishes none). Keys are sorted, as in the sample, so every other byte
is the recorder's. The sample holds no `local_receive_mono_ns` and no trade
`exchange_system_ts_ns`, so the rows carry neither and read them as 0. The
`_meta` tables are fetched live from Bybit's public REST and trimmed to four
symbols, so a rebuild changes them while the rows stay fixed. The hour archive
(`market_tape/pack.py::build_archive`), the block files
(`market_tape/blocks.py::convert_hours`, `BLOCK_ROWS_PER_GROUP` rows a group,
no `_meta`) and `expected.json` are derived from the host hour.

The sample archive (`SLICES` names its members) is not in the repository;
without it this script cannot run and the committed fixture is the only copy.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from market_tape.bars import build_bars  # noqa: E402
from market_tape.blocks import convert_hours  # noqa: E402
from market_tape.book import Book  # noqa: E402
from market_tape.load import ArchiveDir, HostRoot, iter_rows  # noqa: E402
from market_tape.pack import Candidate, build_archive, load_capture_manifest  # noqa: E402
from market_tape.schema import KIND_BOOK_DELTA, KIND_BOOK_SNAPSHOT, BookRow, TradeRow  # noqa: E402
from market_tape.storage import Manifest, Snapshots, zstd_compress  # noqa: E402
from market_tape.venues.bybit import PUBLIC_REST, BybitAdapter, fetch_instruments, fetch_tickers  # noqa: E402

FIXTURES = Path(__file__).resolve().parent
HOST = FIXTURES / "host" / "bybit-linear"
ARCHIVE = FIXTURES / "archive" / "bybit-linear"
BLOCKS = FIXTURES / "blocks" / "bybit-linear"

DAY = "2026-08-30"
HOUR = "00"
VENUE = BybitAdapter.name
MARKET = "linear"
META_SYMBOLS = ("BTCUSDT", "PENDLEUSDT", "ETHUSDT", "AGIUSDT")
#: Small enough that BTCUSDT's 1,500 rows span three groups, which a block
#: reader's window tests rely on.
BLOCK_ROWS_PER_GROUP = 512

# Line index of the first depth-50 snapshot in each source segment, then how
# many consecutive lines to keep. Both segments start with a depth-1 snapshot,
# so the slice starts at line 1.
SLICES = (
    ("BTCUSDT", "tape-sample/2026-08-30/BTCUSDT/segment-000002.jsonl.zst", 1, 1500),
    ("PENDLEUSDT", "tape-sample/2026-08-30/PENDLEUSDT/segment-000002.jsonl.zst", 1, 400),
)


def source_lines(archive: Path, member: str) -> list[bytes]:
    listing = subprocess.Popen(["zstd", "-dcq", "--", str(archive)], stdout=subprocess.PIPE)
    assert listing.stdout is not None
    done = subprocess.run(["tar", "-xOf", "-", member], stdin=listing.stdout, capture_output=True, check=True)
    listing.stdout.close()
    listing.wait()
    return subprocess.run(["zstd", "-dcq"], input=done.stdout, capture_output=True, check=True).stdout.splitlines(
        keepends=True
    )


def as_written(line: bytes) -> bytes:
    """One sample line with the fields the Bybit normalizer writes that it lacks; every other key as recorded."""

    row = json.loads(line)
    if json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" != line:
        raise SystemExit("a sample line is not sorted, compact JSON; adding fields would move its other bytes")
    row["venue"] = VENUE
    if row["kind"] in (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA):
        row["first_update_id"] = 0
    return json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def write_segment(manifest: Manifest, symbol: str, lines: list[bytes]) -> None:
    directory = HOST / DAY / HOUR / symbol
    directory.mkdir(parents=True, exist_ok=True)
    raw = directory / "segment-000000.jsonl"
    output = directory / "segment-000000.jsonl.zst"
    output.unlink(missing_ok=True)
    raw.write_bytes(b"".join(lines))
    digest = zstd_compress(raw, output)
    raw.unlink()
    first = json.loads(lines[0])["local_receive_ts_ns"]
    last = json.loads(lines[-1])["local_receive_ts_ns"]
    manifest.append(
        {
            "kind": "segment_compressed",
            "recorded_at_ns": last,
            "path": str(output.relative_to(HOST)),
            "symbol": symbol,
            "day": DAY,
            "hour": HOUR,
            "records": len(lines),
            "first_receive_ns": first,
            "last_receive_ns": last,
            "compressed_bytes": output.stat().st_size,
            "sha256": digest,
        }
    )


def write_meta(manifest: Manifest, now_ns: int) -> None:
    keep = set(META_SYMBOLS)
    tables = {
        "instruments": [row for row in fetch_instruments(PUBLIC_REST, MARKET) if row.get("symbol") in keep],
        "tickers": [row for row in fetch_tickers(PUBLIC_REST, MARKET) if row.get("symbol") in keep],
    }
    for name, rows in tables.items():
        missing = keep - {str(row.get("symbol")) for row in rows}
        if missing:
            raise SystemExit(f"the venue no longer lists {sorted(missing)} in {name}")
    Snapshots(HOST, manifest, venue=VENUE, market=MARKET, source=PUBLIC_REST, cadence="hour").write(now_ns, tables)


def pack() -> None:
    shutil.rmtree(ARCHIVE, ignore_errors=True)
    day_dir = ARCHIVE / DAY.replace("-", "/")
    day_dir.mkdir(parents=True, exist_ok=True)
    candidate = Candidate(f"{DAY}T{HOUR}Z", DAY, HOUR, (HOST / DAY / HOUR,))
    archive, built = build_archive(
        candidate, HOST, day_dir, load_capture_manifest(HOST / "manifest.jsonl"), tape="bybit-linear"
    )
    print(f"packed {archive.relative_to(FIXTURES)} files={built['file_count']} bytes={archive.stat().st_size}")


def blocks() -> None:
    shutil.rmtree(BLOCKS, ignore_errors=True)
    receipts = convert_hours(
        HostRoot(HOST), [f"{DAY}T{HOUR}"], BLOCKS, rows_per_group=BLOCK_ROWS_PER_GROUP, strict=True, copy_meta=False
    )
    print(f"cut {len(receipts)} block files under {BLOCKS.relative_to(FIXTURES)}")


def expectations() -> dict[str, object]:
    host = HostRoot(HOST)
    hours = host.hours()
    counts: dict[str, dict[str, int]] = {}
    spans: dict[str, list[int]] = {}
    first = last = 0
    for row in iter_rows(host, hours):
        counts.setdefault(row.symbol, {}).setdefault(row.kind, 0)
        counts[row.symbol][row.kind] += 1
        span = spans.setdefault(row.symbol, [row.local_receive_ts_ns, row.local_receive_ts_ns])
        span[1] = row.local_receive_ts_ns
        first = first or row.local_receive_ts_ns
        last = row.local_receive_ts_ns

    book = Book()
    for row in iter_rows(host, hours, symbols=["BTCUSDT"], kinds=["orderbook_snapshot", "orderbook_delta"]):
        assert isinstance(row, BookRow)
        if row.depth == 50:
            book.apply(row)

    bars = build_bars(iter_rows(host, hours), interval_seconds=1.0)
    trades = iter_rows(host, hours, symbols=["BTCUSDT"], kinds=["public_trade"])
    volume = sum(row.qty for row in trades if isinstance(row, TradeRow))
    bid, ask = book.depth_within(10)
    return {
        "hours": hours,
        "venue": host.venue,
        "skipped_rows": host.skipped_rows,
        "rows_by_symbol_kind": {symbol: dict(sorted(kinds.items())) for symbol, kinds in sorted(counts.items())},
        "first_receive_ns": first,
        "last_receive_ns": last,
        "span_by_symbol": {symbol: span for symbol, span in sorted(spans.items())},
        "btc_book_depth50": {
            "valid": book.valid,
            "rows_applied": book.rows_applied,
            "last_update_id": book.last_update_id,
            "best_bid": list(book.best_bid) if book.best_bid else None,
            "best_ask": list(book.best_ask) if book.best_ask else None,
            "depth_within_10bp": [bid, ask],
        },
        "one_second_bars": bars.height,
        "btc_trade_volume": volume,
    }


def derive() -> None:
    """The archive, the block files and `expected.json`, from the host hour as it stands."""

    pack()
    blocks()
    expected = FIXTURES / "expected.json"
    expected.write_text(json.dumps(expectations(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"host hours={HostRoot(HOST).hours()} archive hours={ArchiveDir(ARCHIVE).hours()}")


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        raise SystemExit("usage: build_fixture.py SAMPLE.tar.zst")
    archive = Path(argv[0])
    if not archive.is_file():
        raise SystemExit(f"the tape sample is missing: {archive}")
    shutil.rmtree(HOST, ignore_errors=True)
    HOST.mkdir(parents=True)
    manifest = Manifest(HOST)
    started = time.time_ns()
    first_ns = 0
    for symbol, member, start, count in SLICES:
        lines = source_lines(archive, member)[start : start + count]
        if len(lines) != count:
            raise SystemExit(f"{member} holds fewer than {start + count} lines")
        write_segment(manifest, symbol, [as_written(line) for line in lines])
        received = json.loads(lines[0])["local_receive_ts_ns"]
        first_ns = min(first_ns or received, received)
    write_meta(manifest, first_ns)
    derive()
    print(f"rebuilt in {(time.time_ns() - started) / 1e9:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
