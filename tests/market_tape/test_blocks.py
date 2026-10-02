"""The block format: every row kind round-trips in order, the index decides what a read touches,
a corrupt block is caught, and a converted root reads through the same loader as the JSON tape."""

from __future__ import annotations

import json
import tarfile
from pathlib import Path
from typing import Any

import pytest

from market_tape import blocks
from market_tape.__main__ import main as tape_main
from market_tape.blocks import BlockCorrupt, BlockError, BlockReader, BlockWriter, convert_hours
from market_tape.load import ArchiveDir, HostRoot, TapeRowError, iter_rows, iter_snapshots
from market_tape.schema import (
    KIND_BOOK_DELTA,
    KIND_BOOK_SNAPSHOT,
    KIND_TRADE,
    account_ratio_row,
    book_row,
    funding_row,
    kline_row,
    liquidation_row,
    parse_row,
    ticker_row,
    trade_row,
)
from market_tape.storage import read_receipts

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HOST = FIXTURES / "host" / "bybit-linear"
HOUR = "2026-08-30T00"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))

SECOND = 1_000_000_000
MS = 1_000_000
T = 1_800_000_000 * SECOND  # 2027-01-15T08:00:00Z


def _every_kind(symbol: str = "AGIUSDT") -> list[dict[str, Any]]:
    """Rows of every kind, interleaved, two of them sharing one receive stamp."""

    return [
        book_row(
            venue="bybit",
            symbol=symbol,
            snapshot=True,
            depth=50,
            local_receive_ts_ns=T,
            local_receive_mono_ns=10,
            exchange_system_ts_ns=T - 3 * MS,
            exchange_engine_ts_ns=T - 4 * MS,
            bids=[["0.001", "20"], ["0.0009", "1500.5"]],
            asks=[["0.0011", "30"]],
            update_id=10,
            previous_update_id=0,
            cross_sequence=100,
            previous_cross_sequence=0,
            restart_snapshot=True,
        ),
        ticker_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 1 * MS,
            local_receive_mono_ns=11,
            exchange_system_ts_ns=T,
            message_type="snapshot",
            cross_sequence=7,
            values={"last_price": 0.001, "next_funding_time_ms": 1_800_000_000_000, "funding_rate": -0.0001},
        ),
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 2 * MS,
            local_receive_mono_ns=12,
            exchange_system_ts_ns=T + 1 * MS,
            exchange_ts_ns=T + 1 * MS - 500_000,
            trade_id="6d2a1c3e-0001",
            price=0.0011,
            qty=100.0,
            side="Buy",
        ),
        # The same stamp as the print before it: the order they were written in must hold.
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 2 * MS,
            local_receive_mono_ns=12,
            exchange_system_ts_ns=T + 1 * MS,
            exchange_ts_ns=T + 1 * MS,
            trade_id="6d2a1c3e-0002",
            price=0.0010,
            qty=80.0,
            side="Sell",
        ),
        book_row(
            venue="bybit",
            symbol=symbol,
            snapshot=False,
            depth=50,
            local_receive_ts_ns=T + 3 * MS,
            local_receive_mono_ns=13,
            exchange_system_ts_ns=T + 2 * MS,
            exchange_engine_ts_ns=T + 2 * MS,
            bids=[["0.001", "0"]],
            asks=[],
            update_id=11,
            previous_update_id=10,
            cross_sequence=101,
            previous_cross_sequence=100,
            sequence_gap=True,
        ),
        liquidation_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 4 * MS,
            local_receive_mono_ns=14,
            exchange_system_ts_ns=T + 3 * MS,
            exchange_ts_ns=T + 3 * MS,
            position_side="Sell",
            qty=20_000.0,
            bankruptcy_price=0.0009,
        ),
        kline_row(
            venue="bybit",
            symbol=symbol,
            interval="1",
            local_receive_ts_ns=T + 5 * MS,
            local_receive_mono_ns=15,
            exchange_system_ts_ns=T + 4 * MS,
            start_ms=1_800_000_000_000,
            end_ms=1_800_000_059_999,
            open=0.001,
            high=0.0012,
            low=0.0009,
            close=0.0011,
            volume=1234.5,
            turnover=1.3,
            confirmed=False,
        ),
        funding_row(venue="bybit", symbol=symbol, local_receive_ts_ns=T + 6 * MS, funding_time_ms=1, funding_rate=0.0001),
        account_ratio_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 7 * MS,
            period="5min",
            ts_ms=1_800_000_000_000,
            buy_ratio=0.6,
            sell_ratio=0.4,
        ),
        ticker_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=T + 8 * MS,
            local_receive_mono_ns=18,
            exchange_system_ts_ns=T + 7 * MS,
            message_type="delta",
            values={"open_interest": 5.0},
        ),
    ]


@pytest.mark.parametrize("codec", ["none", "zstd"])
def test_every_row_kind_round_trips_in_the_recorders_order(tmp_path: Path, codec: str) -> None:
    rows = _every_kind()
    path = tmp_path / "segment-000000.lmtb"
    # Two groups of six: the second group holds four rows of three kinds.
    writer = BlockWriter(path, venue="bybit", symbol="AGIUSDT", codec=codec, rows_per_group=6)
    for row in rows:
        writer.append(row)
    summary = writer.close()
    assert not path.with_name(path.name + ".partial").exists()
    assert (summary.rows, summary.groups) == (10, 2)
    # Group one: snapshot, ticker, two trades, delta, liquidation = 5 kinds; group two: kline, funding, ratio, ticker = 4.
    assert summary.blocks == 9
    assert summary.kinds == {
        "account_ratio": 1,
        "funding_settlement": 1,
        "kline": 1,
        "liquidation": 1,
        "orderbook_delta": 1,
        "orderbook_snapshot": 1,
        "public_trade": 2,
        "ticker": 2,
    }
    assert (summary.first_receive_ns, summary.last_receive_ns) == (T, T + 8 * MS)

    with BlockReader(path) as reader:
        assert reader.venue == "bybit" and reader.symbol == "AGIUSDT" and reader.codec == codec
        assert reader.header["schema"] == 2 and reader.header["rows_per_group"] == 6
        read = [row for _, row in reader.rows()]
        assert read == rows
        typed = [row for _, row in reader.rows(typed=True)]
        assert typed == [parse_row(row) for row in rows]
        # The ticker's integer field comes back an int, its absent fields absent.
        assert typed[1].values == {"last_price": 0.001, "next_funding_time_ms": 1_800_000_000_000, "funding_rate": -0.0001}
        assert isinstance(typed[1].values["next_funding_time_ms"], int)
        assert reader.verify() == 10
        described = reader.describe()
        assert described["rows"] == 10 and described["blocks"] == 9 and described["groups"] == 2
        assert described["rows_by_kind"]["public_trade"] == 2
        assert described["header"]["venue"] == "bybit" and "_codec" not in described["header"]


def test_the_index_alone_decides_which_blocks_a_filtered_read_touches(tmp_path: Path) -> None:
    """Kinds a read does not want and groups outside its window are never read."""

    path = tmp_path / "segment-000000.lmtb"
    writer = BlockWriter(path, venue="bybit", symbol="AGIUSDT", codec="none", rows_per_group=4)
    rows = []
    for index in range(40):
        at = T + index * SECOND
        if index % 2 == 0:
            rows.append(
                book_row(
                    venue="bybit",
                    symbol="AGIUSDT",
                    snapshot=index == 0,
                    depth=50,
                    local_receive_ts_ns=at,
                    exchange_system_ts_ns=at,
                    exchange_engine_ts_ns=at,
                    bids=[["1", "1"]],
                    asks=[["2", "1"]],
                    update_id=index + 1,
                    previous_update_id=index,
                )
            )
        else:
            rows.append(
                trade_row(
                    venue="bybit",
                    symbol="AGIUSDT",
                    local_receive_ts_ns=at,
                    exchange_ts_ns=at,
                    trade_id=f"t-{index}",
                    price=1.5,
                    qty=1.0,
                    side="Buy",
                )
            )
    for row in rows:
        writer.append(row)
    assert writer.close().groups == 10

    with BlockReader(path) as reader:
        # Ten groups of four rows, two books and two trades each: a book block
        # and a trade block per group, and group 0 holds the snapshot in a
        # block of its own, since a snapshot and a delta are two kinds.
        assert len(reader.entries) == 21
        assert [e.kind for e in reader.entries if e.group == 0] == [KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA, KIND_TRADE]
        assert reader.groups()[0] == (0, T, T + 3 * SECOND)
        # Trades of seconds 13..27: groups 3 (12..15) to 6 (24..27), trade blocks only.
        start, end = T + 13 * SECOND, T + 27 * SECOND
        selected = reader.select(kinds=[KIND_TRADE], start_ns=start, end_ns=end)
        assert [(entry.kind, entry.group) for entry in selected] == [(KIND_TRADE, g) for g in (3, 4, 5, 6)]
        got = [row for _, row in reader.rows(kinds=[KIND_TRADE], start_ns=start, end_ns=end)]
        assert [row["trade_id"] for row in got] == ["t-13", "t-15", "t-17", "t-19", "t-21", "t-23", "t-25"]
        assert reader.blocks_read == 4
        # Snapshots live in group 0 only: one block read for the whole file.
        assert [row["update_id"] for _, row in reader.rows(kinds=[KIND_BOOK_SNAPSHOT])] == [1]
        assert reader.blocks_read == 5
        # An unfiltered read gives every row back in order.
        assert [row for _, row in reader.rows()] == rows
        # A window past the tape reads nothing at all.
        assert reader.select(start_ns=T + 100 * SECOND) == []


def test_a_flipped_byte_fails_its_block_and_a_cut_file_fails_its_trailer(tmp_path: Path) -> None:
    path = tmp_path / "segment-000000.lmtb"
    writer = BlockWriter(path, venue="bybit", symbol="AGIUSDT", codec="none", rows_per_group=6)
    rows = _every_kind()
    for row in rows:
        writer.append(row)
    writer.close()
    raw = bytearray(path.read_bytes())
    with BlockReader(path) as reader:
        # The trade block of group one, a byte inside its payload.
        entry = next(e for e in reader.entries if e.kind == KIND_TRADE)
    at = entry.offset + blocks.BLOCK_HEADER.size + 5
    raw[at] ^= 0xFF
    corrupt = tmp_path / "corrupt.lmtb"
    corrupt.write_bytes(bytes(raw))
    with BlockReader(corrupt) as reader:
        with pytest.raises(BlockCorrupt, match="CRC-32C") as caught:
            reader.read_block(entry)
        assert caught.value.entry.rows == 2
        # Every other block is still whole.
        others = [e for e in reader.entries if e != entry]
        assert sum(len(reader.read_block(e)) for e in others) == 8

    # Through the loader the corrupt block is skipped, counted, and named; strict refuses it.
    root = tmp_path / "root"
    target = root / "2027-01-15" / "08" / "AGIUSDT" / "segment-000000.lmtb"
    target.parent.mkdir(parents=True)
    target.write_bytes(bytes(raw))
    (root / "manifest.jsonl").write_text("")
    source = HostRoot(root, venue="bybit")
    read = [row for row in iter_rows(source, ["2027-01-15T08"], typed=False)]
    assert len(read) == 8 and source.skipped_rows == 2
    assert not any(row["kind"] == KIND_TRADE for row in read)
    with pytest.raises(TapeRowError, match="CRC-32C"):
        list(iter_rows(source, ["2027-01-15T08"], strict=True))

    # A file cut before its trailer was never finished: no index, no guessing.
    cut = tmp_path / "cut.lmtb"
    cut.write_bytes(path.read_bytes()[:-40])
    with pytest.raises(BlockError, match="never finished"):
        BlockReader(cut)
    # And a file that is not a block file at all says so.
    other = tmp_path / "other.lmtb"
    other.write_bytes(b"not a block file, but long enough to hold a trailer's worth of bytes")
    with pytest.raises(BlockError, match="not a block file"):
        BlockReader(other)


def test_the_writer_refuses_what_the_format_cannot_hold(tmp_path: Path) -> None:
    path = tmp_path / "segment-000000.lmtb"
    writer = BlockWriter(path, venue="bybit", symbol="AGIUSDT", codec="none")
    good = trade_row(
        venue="bybit", symbol="AGIUSDT", local_receive_ts_ns=T, exchange_ts_ns=T, trade_id="a", price=1, qty=1, side="Buy"
    )
    writer.append(good)
    with pytest.raises(BlockError, match="unknown row kind"):
        writer.append({**good, "kind": "something_new"})
    with pytest.raises(BlockError, match="cannot hold"):
        writer.append({**good, "local_receive_ts_ns": T + 1, "colour": "red"})
    with pytest.raises(BlockError, match="order"):
        writer.append({**good, "local_receive_ts_ns": T - 1})
    with pytest.raises(BlockError, match="does not belong"):
        writer.append({**good, "symbol": "OTHERUSDT"})
    with pytest.raises(BlockError, match="positive local_receive_ts_ns"):
        writer.append({**good, "local_receive_ts_ns": 0})
    # A refused row is not in the file; the good one is.
    assert writer.close().rows == 1
    with pytest.raises(BlockError, match="codec"):
        BlockWriter(tmp_path / "x.lmtb", venue="bybit", codec="lz4")
    with pytest.raises(BlockError, match="positive"):
        BlockWriter(tmp_path / "y.lmtb", venue="bybit", rows_per_group=0)
    # A writer abandoned part-way leaves no file behind.
    dropped = BlockWriter(tmp_path / "z.lmtb", venue="bybit", codec="none")
    dropped.append(good)
    dropped.abandon()
    assert not (tmp_path / "z.lmtb").exists() and not (tmp_path / "z.lmtb.partial").exists()


def test_the_fixture_hour_converted_to_blocks_reads_back_as_the_same_hour(tmp_path: Path) -> None:
    """The recorded hour, converted, is the same rows to the loader, the book and the tables;
    the converted root names its venue from its block files and receipts its files."""

    source = HostRoot(HOST)
    out = tmp_path / "converted"
    receipts = convert_hours(source, [HOUR], out, rows_per_group=100)
    assert sorted(receipt["symbol"] for receipt in receipts) == ["BTCUSDT", "PENDLEUSDT"]
    for receipt in receipts:
        assert receipt["kind"] == "segment_blocks"
        assert receipt["records"] == sum(EXPECTED["rows_by_symbol_kind"][receipt["symbol"]].values())
        assert receipt["rows_by_kind"] == EXPECTED["rows_by_symbol_kind"][receipt["symbol"]]
        assert receipt["path"] == f"2026-08-30/00/{receipt['symbol']}/segment-000000.lmtb"
        assert receipt["blocks"] > receipt["groups"] >= 2
        assert receipt["sources"] == [f"{receipt['symbol']}/segment-000000.jsonl.zst"]
    assert sorted(p.name for p in (out / "2026-08-30" / "00" / "_meta").iterdir()) == sorted(
        p.name for p in (HOST / "2026-08-30" / "00" / "_meta").iterdir()
    )

    # The directory is named nothing like a venue and has no status file: the block header says.
    converted = HostRoot(out)
    assert converted.venue == "bybit"
    assert converted.hours() == [HOUR]
    # Receipts by path under the root, and by path inside the hour.
    assert converted.hour_manifest(HOUR).keys() == {f"{receipt['symbol']}/segment-000000.lmtb" for receipt in receipts}
    assert read_receipts(out / "manifest.jsonl").keys() == {receipt["path"] for receipt in receipts}

    original = list(iter_rows(source, [HOUR]))
    again = list(iter_rows(converted, [HOUR]))
    assert again == original
    assert converted.skipped_rows == 0
    # Filters ride the index: the same trades, fewer blocks touched.
    trades = list(iter_rows(converted, [HOUR], symbols=["BTCUSDT"], kinds=["public_trade"]))
    assert trades == list(iter_rows(source, [HOUR], symbols=["BTCUSDT"], kinds=["public_trade"]))
    first, last = EXPECTED["span_by_symbol"]["BTCUSDT"]
    middle = (first + last) // 2
    window = list(iter_rows(converted, [HOUR], symbols=["BTCUSDT"], start_ns=middle, end_ns=last))
    assert window == list(iter_rows(source, [HOUR], symbols=["BTCUSDT"], start_ns=middle, end_ns=last))
    assert window and all(middle <= row.local_receive_ts_ns < last for row in window)
    assert list(iter_snapshots(converted, [HOUR])) == list(iter_snapshots(source, [HOUR]))

    # A second run leaves the converted files alone.
    assert convert_hours(source, [HOUR], out, rows_per_group=100) == []

    # Tarred like the storage box, the block files read through the archive source too.
    day = tmp_path / "archive" / "bybit-linear" / "2026" / "08" / "30"
    day.mkdir(parents=True)
    with tarfile.open(day / "2026-08-30T00Z.tar", "w") as archive:
        for path in sorted((out / "2026-08-30" / "00").rglob("*")):
            if path.is_file():
                archive.add(path, arcname=str(path.relative_to(out / "2026-08-30" / "00")))
    archived = ArchiveDir(tmp_path / "archive" / "bybit-linear")
    assert list(iter_rows(archived, [HOUR], symbols=["PENDLEUSDT"])) == list(
        iter_rows(source, [HOUR], symbols=["PENDLEUSDT"])
    )


def test_the_verbs_convert_index_and_read_a_window(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "blocks"
    assert tape_main(["blocks", str(HOST), "--hours", HOUR, "--out", str(out), "--symbols", "BTCUSDT"]) == 0
    printed = capsys.readouterr().out
    assert printed.startswith("wrote 1 block files, ")
    path = out / "2026-08-30" / "00" / "BTCUSDT" / "segment-000000.lmtb"
    assert tape_main(["index", str(path), "--blocks", "--verify"]) == 0
    printed = capsys.readouterr().out
    summary, *entries = printed.split("\n}\n", 1)
    described = json.loads(summary + "\n}")
    assert described["rows"] == described["verified_rows"] == sum(EXPECTED["rows_by_symbol_kind"]["BTCUSDT"].values())
    assert described["rows_by_kind"]["orderbook_delta"] == EXPECTED["rows_by_symbol_kind"]["BTCUSDT"]["orderbook_delta"]
    lines = [json.loads(line) for line in entries[0].splitlines() if line]
    assert len(lines) == described["blocks"]
    assert all(line["min_receive_ns"] <= line["max_receive_ns"] for line in lines)

    first, last = EXPECTED["span_by_symbol"]["BTCUSDT"]
    middle = (first + last) // 2
    assert (
        tape_main(
            [
                "rows",
                str(out),
                "--hours",
                HOUR,
                "--kinds",
                "orderbook_delta",
                "--start-ns",
                str(middle),
                "--end-ns",
                str(last),
            ]
        )
        == 0
    )
    from_blocks = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert tape_main(
        ["rows", str(HOST), "--hours", HOUR, "--symbols", "BTCUSDT", "--kinds", "orderbook_delta",
         "--start-ns", str(middle), "--end-ns", str(last)]
    ) == 0
    from_json = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert from_blocks and all(middle <= row["local_receive_ts_ns"] < last for row in from_blocks)
    assert [parse_row(r) for r in from_blocks] == [
        parse_row(r) for r in from_json
    ]
    assert all(row["kind"] == KIND_BOOK_DELTA for row in from_blocks)


BLOCKS = FIXTURES / "blocks" / "bybit-linear"


def test_the_record_layouts_are_the_sizes_the_rust_reader_expects() -> None:
    """A reader outside Python (the Rust block reader this format was written
    for) reads these records by byte offset; a field added or moved here must
    move there, and this is where it shows."""

    sizes = {kind: dtype.itemsize for kind, dtype in blocks.RECORD_DTYPES.items()}
    assert sizes["orderbook_snapshot"] == sizes["orderbook_delta"] == 97
    assert sizes["public_trade"] == 65
    assert sizes["ticker"] == 196
    assert blocks._LEVEL.itemsize == 8
    assert blocks.BLOCK_HEADER.size == 64 and blocks.FILE_HEADER.size == 16
    assert blocks.INDEX_ENTRY.size == 48 and blocks.TRAILER.size == 32
    book = blocks.RECORD_DTYPES["orderbook_snapshot"]
    assert [book.fields[name][1] for name in ("depth", "level_offset", "bids", "asks", "flags")] == [84, 88, 92, 94, 96]
    trade = blocks.RECORD_DTYPES["public_trade"]
    assert [trade.fields[name][1] for name in ("price", "qty", "trade_id", "side")] == [44, 52, 60, 64]
    ticker = blocks.RECORD_DTYPES["ticker"]
    assert [ticker.fields[name][1] for name in ("cross_sequence", "message_type", "present", "values")] == [36, 44, 48, 52]


def test_the_committed_block_fixture_is_the_recorded_hour() -> None:
    """The block files under `fixtures/blocks/` are cut from the JSON hour
    beside them (`fixtures/build_fixture.py::blocks`). They hold the same rows
    as the hour, and this is where a format change that no longer reads them
    shows."""

    converted = HostRoot(BLOCKS)
    assert converted.venue == "bybit"
    assert list(iter_rows(converted, [HOUR])) == list(iter_rows(HostRoot(HOST), [HOUR]))
    assert converted.skipped_rows == 0
    receipts = read_receipts(BLOCKS / "manifest.jsonl")
    assert {receipt["symbol"]: receipt["records"] for receipt in receipts.values()} == {
        symbol: sum(kinds.values()) for symbol, kinds in EXPECTED["rows_by_symbol_kind"].items()
    }
