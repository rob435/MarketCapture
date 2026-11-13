"""The Binance-only REST lane kinds (`top_account_ratio`, `top_position_ratio`, `taker_volume`) through every reader.

Each kind must come back the same from a JSON segment and from a block file,
typed and untyped, and be a feed of its own in the quality frame and the
coverage ledger: a lane feed with no stream topic, complete while the
recorder is up, as `funding` and `account_ratio` are."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from market_tape.blocks import BlockReader, BlockWriter, convert_hours
from market_tape.coverage import CoverageFold, build_ledger
from market_tape.load import HostRoot, iter_rows
from market_tape.quality import build_quality
from market_tape.schema import (
    TakerVolumeRow,
    TopAccountRatioRow,
    TopPositionRatioRow,
    account_ratio_row,
    funding_row,
    parse_row,
    taker_volume_row,
    top_account_ratio_row,
    top_position_ratio_row,
    trade_row,
)
from market_tape.storage import CoverageRecords, Manifest, utc_day_hour, zstd_compress

SECOND = 1_000_000_000
MINUTE_NS = 60 * SECOND
HOUR_NS = 60 * MINUTE_NS
BASE_NS = 1_800_000_000 * SECOND  # 2027-01-15T08:00:00Z
HOUR = "2027-01-15T08"
BUCKET_MS = 300_000
LANE_FEEDS = ("funding", "account_ratio", "top_account_ratio", "top_position_ratio", "taker_volume")


def _lane_rows(symbol: str = "BTCUSDT") -> list[dict[str, Any]]:
    """One pass of Binance's hourly lane for one symbol, three minutes past the
    hour, behind a print: every lane kind, two buckets of each statistic."""

    at = BASE_NS + 3 * MINUTE_NS
    common = {"venue": "binance", "symbol": symbol, "period": "5min"}
    rows: list[dict[str, Any]] = [
        trade_row(
            venue="binance",
            symbol=symbol,
            local_receive_ts_ns=at - SECOND,
            exchange_ts_ns=at - SECOND,
            trade_id="1",
            price=83_680.0,
            qty=0.001,
            side="Sell",
        ),
        funding_row(venue="binance", symbol=symbol, local_receive_ts_ns=at, funding_time_ms=1_799_971_200_000, funding_rate=-0.00002015),
    ]
    for index in range(2):
        stamp = 1_799_999_400_000 + index * BUCKET_MS
        received = at + (index + 1) * 1_000_000
        rows += [
            account_ratio_row(**common, local_receive_ts_ns=received, ts_ms=stamp, buy_ratio=0.6442, sell_ratio=0.3558),
            top_account_ratio_row(**common, local_receive_ts_ns=received, ts_ms=stamp, buy_ratio=0.5891, sell_ratio=0.4109),
            top_position_ratio_row(**common, local_receive_ts_ns=received, ts_ms=stamp, buy_ratio=0.7012, sell_ratio=0.2988),
            taker_volume_row(**common, local_receive_ts_ns=received, ts_ms=stamp, buy_volume=387.33, sell_volume=248.503),
        ]
    return rows


def _segment(root: Path, symbol: str, rows: list[dict[str, Any]]) -> None:
    day, hour = utc_day_hour(rows[0]["local_receive_ts_ns"])
    directory = root / day / hour / symbol
    directory.mkdir(parents=True, exist_ok=True)
    raw = directory / "segment-000000.jsonl"
    raw.write_bytes(b"".join(json.dumps(row, separators=(",", ":")).encode() + b"\n" for row in rows))
    output = directory / "segment-000000.jsonl.zst"
    digest = zstd_compress(raw, output)
    raw.unlink()
    Manifest(root).append(
        {
            "kind": "segment_compressed",
            "recorded_at_ns": rows[-1]["local_receive_ts_ns"],
            "path": str(output.relative_to(root)),
            "symbol": symbol,
            "day": day,
            "hour": hour,
            "records": len(rows),
            "first_receive_ns": rows[0]["local_receive_ts_ns"],
            "last_receive_ns": rows[-1]["local_receive_ts_ns"],
            "compressed_bytes": output.stat().st_size,
            "sha256": digest,
        }
    )


@pytest.mark.parametrize("codec", ["none", "zstd"])
def test_every_lane_kind_round_trips_through_a_block_file(tmp_path: Path, codec: str) -> None:
    rows = _lane_rows()
    path = tmp_path / "segment-000000.lmtb"
    writer = BlockWriter(path, venue="binance", symbol="BTCUSDT", codec=codec, rows_per_group=4)
    for row in rows:
        writer.append(row)
    summary = writer.close()
    assert summary.kinds == {
        "public_trade": 1,
        "funding_settlement": 1,
        "account_ratio": 2,
        "top_account_ratio": 2,
        "top_position_ratio": 2,
        "taker_volume": 2,
    }
    with BlockReader(path) as reader:
        assert [row for _, row in reader.rows()] == rows
        assert [row for _, row in reader.rows(typed=True)] == [parse_row(row) for row in rows]
        assert reader.verify() == len(rows)
        # The index alone picks a kind: only its blocks are decoded.
        assert [row["kind"] for _, row in reader.rows(kinds=["taker_volume"])] == ["taker_volume"] * 2


def test_a_lane_row_with_a_field_the_format_cannot_hold_is_refused(tmp_path: Path) -> None:
    from market_tape.blocks import BlockError

    row = dict(_lane_rows()[-1], buy_sell_ratio=1.5586)
    writer = BlockWriter(tmp_path / "segment-000000.lmtb", venue="binance", symbol="BTCUSDT", codec="none")
    with pytest.raises(BlockError, match="buy_sell_ratio"):
        writer.append(row)


def test_the_loader_reads_the_lane_kinds_the_same_from_json_and_from_blocks(tmp_path: Path) -> None:
    root = tmp_path / "binance-usdm"
    rows = _lane_rows()
    _segment(root, "BTCUSDT", rows)
    json_source = HostRoot(root, "binance")
    assert list(iter_rows(json_source, [HOUR], typed=False)) == rows
    typed = list(iter_rows(json_source, [HOUR]))
    assert typed == [parse_row(row) for row in rows]
    assert [type(row) for row in typed[-3:]] == [TopAccountRatioRow, TopPositionRatioRow, TakerVolumeRow]
    assert json_source.skipped_rows == 0

    converted = tmp_path / "blocks" / "binance-usdm"
    (receipt,) = convert_hours(json_source, [HOUR], converted, codec="none")
    assert receipt["records"] == len(rows)
    block_source = HostRoot(converted, "binance")
    assert list(iter_rows(block_source, [HOUR], typed=False)) == rows
    assert list(iter_rows(block_source, [HOUR], kinds=["top_position_ratio"])) == [
        row for row in typed if isinstance(row, TopPositionRatioRow)
    ]


def test_each_lane_kind_is_its_own_quality_cell_without_a_clock_reading() -> None:
    frame = build_quality([parse_row(row) for row in _lane_rows()])
    cells = {row["feed"]: row for row in frame.to_dicts()}
    assert set(cells) == {"trades", *LANE_FEEDS}
    for feed in LANE_FEEDS:
        assert cells[feed]["rows"] == (1 if feed == "funding" else 2)
        # The venue's stamp is a bucket, not a send time: no skew, no clock steps.
        assert cells[feed]["skew_p50_ms"] is None and cells[feed]["clock_steps"] == 0
    assert cells["taker_volume"]["max_silence_ms"] == 1.0


def test_a_lane_feed_has_no_topic_and_is_complete_while_the_recorder_is_up(tmp_path: Path) -> None:
    root = tmp_path / "binance-usdm"
    root.mkdir()
    feeds = ("trades", *LANE_FEEDS)
    fold = CoverageFold(venue="binance", market="usdm", pid=7, started_at_ns=BASE_NS, feeds={"all": feeds}, now_ns=BASE_NS)
    fold.members("all", ["BTCUSDT"], BASE_NS)
    fold.connected(0, "all", ["btcusdt@trade"], BASE_NS)
    # The stream drops for ten minutes: the trades cell is excluded, the lane cells are not.
    fold.disconnected(0, "all", ["btcusdt@trade"], BASE_NS + 20 * MINUTE_NS)
    fold.connected(0, "all", ["btcusdt@trade"], BASE_NS + 30 * MINUTE_NS)
    CoverageRecords(root, Manifest(root)).write(fold.roll(BASE_NS + HOUR_NS))
    _segment(root, "BTCUSDT", _lane_rows())

    ledger = build_ledger(HostRoot(root, "binance"), [HOUR], read_rows=True)
    cells = {(cell.symbol, cell.feed): cell for cell in ledger.cells}
    assert sorted(feed for _, feed in cells) == sorted(feeds)
    assert cells[("BTCUSDT", "trades")].status == "excluded"
    for feed in LANE_FEEDS:
        cell = cells[("BTCUSDT", feed)]
        assert cell.status == "complete", feed
        assert cell.records == (1 if feed == "funding" else 2)
    assert ledger.admissible("BTCUSDT", "taker_volume") == [HOUR]
