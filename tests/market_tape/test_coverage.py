"""The coverage ledger: what a recorded hour is admissible for, and why it is not."""

from __future__ import annotations

import json
import shutil
from pathlib import Path


from market_tape import pack
from market_tape.coverage import CoverageFold, build_ledger
from market_tape.load import ArchiveDir, HostRoot, iter_coverage, iter_rows, iter_snapshots
from market_tape.schema import BookRow, LiquidationRow, TickerRow, TradeRow, trade_row
from market_tape.storage import CoverageRecords, Manifest, Snapshots, utc_day_hour, zstd_compress


FIXTURES = Path(__file__).resolve().parent / "fixtures"
ARCHIVE = FIXTURES / "archive" / "bybit-linear"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))
FIXTURE_HOUR = "2026-08-30T00"

MINUTE_NS = 60 * 1_000_000_000
HOUR_NS = 60 * MINUTE_NS
BASE_NS = 1_800_000_000 * 1_000_000_000  # 2027-01-15T08:00:00Z, exactly on the hour
HOUR = "2027-01-15T08"
DAY = "2027-01-15"
FEEDS = ("book:50", "trades")


# ------------------------------------------------------------- the pre-B tape


def test_an_archive_with_no_coverage_record_reads_as_observed_rows(tmp_path: Path) -> None:
    ledger = build_ledger(ArchiveDir(ARCHIVE), [FIXTURE_HOUR])

    spans = EXPECTED["span_by_symbol"]
    assert [
        (cell.symbol, cell.feed, cell.status, cell.records, cell.first_receive_ns, cell.last_receive_ns)
        for cell in ledger.cells
    ] == [
        ("BTCUSDT", "*", "observed", 1500, *spans["BTCUSDT"]),
        ("PENDLEUSDT", "*", "observed", 400, *spans["PENDLEUSDT"]),
    ]
    assert ledger.summary() == {
        "requested": 2,
        "complete": 0,
        "excluded": 0,
        "observed": 2,
        "absent": 0,
        "by_reason": {},
    }
    assert ledger.admissible("BTCUSDT", "*") == []


def test_reading_the_rows_splits_an_observed_hour_by_feed() -> None:
    ledger = build_ledger(ArchiveDir(ARCHIVE), [FIXTURE_HOUR], read_rows=True)

    counted = {(cell.symbol, cell.feed): cell.records for cell in ledger.cells}
    assert {cell.status for cell in ledger.cells} == {"observed"}
    # The tape's two book depths are one pair of kinds; the split is the rows' own.
    derived: dict[tuple[str, str], int] = {}
    for row in iter_rows(ArchiveDir(ARCHIVE), [FIXTURE_HOUR]):
        if isinstance(row, BookRow):
            feed = f"book:{row.depth}"
        elif isinstance(row, TradeRow):
            feed = "trades"
        elif isinstance(row, TickerRow):
            feed = "ticker"
        elif isinstance(row, LiquidationRow):
            feed = "liquidations"
        else:
            continue
        derived[(row.symbol, feed)] = derived.get((row.symbol, feed), 0) + 1
    assert counted == derived
    for symbol, kinds in EXPECTED["rows_by_symbol_kind"].items():
        assert counted[(symbol, "trades")] == kinds["public_trade"]
        assert counted[(symbol, "ticker")] == kinds["ticker"]
        assert counted[(symbol, "book:50")] + counted[(symbol, "book:1")] == (
            kinds["orderbook_delta"] + kinds["orderbook_snapshot"]
        )


# ------------------------------------------------------------ a recorded hour


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "bybit-linear"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _fold(start_ns: int, feeds: tuple[str, ...] = FEEDS) -> CoverageFold:
    return CoverageFold(
        venue="bybit", market="linear", pid=4242, started_at_ns=start_ns, feeds={"deep": feeds}, now_ns=start_ns
    )


def _topics(symbol: str) -> list[str]:
    return [f"orderbook.50.{symbol}", f"publicTrade.{symbol}"]


def _segment(root: Path, symbol: str, at_ns: int, count: int = 3) -> None:
    rows = [
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=at_ns + index,
            exchange_ts_ns=at_ns + index,
            trade_id=str(index),
            price=1.0 + index,
            qty=1.0,
            side="Buy",
        )
        for index in range(count)
    ]
    day, hour = utc_day_hour(at_ns)
    directory = root / day / hour / symbol
    directory.mkdir(parents=True, exist_ok=True)
    raw = directory / "segment-000000.jsonl"
    raw.write_bytes(b"".join(json.dumps(row, sort_keys=True).encode() + b"\n" for row in rows))
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


def _cells(root: Path, hours: list[str]) -> dict[tuple[str, str], object]:
    ledger = build_ledger(HostRoot(root, "bybit"), hours)
    return {(cell.symbol, cell.feed): cell for cell in ledger.cells}


def test_a_shard_disconnect_excludes_the_cells_that_shard_carried(tmp_path: Path) -> None:
    root = _root(tmp_path)
    fold = _fold(BASE_NS)
    fold.members("deep", ["XUSDT", "YUSDT"], BASE_NS)
    fold.connected(0, "deep", _topics("XUSDT"), BASE_NS)
    fold.connected(1, "deep", _topics("YUSDT"), BASE_NS)
    down_from, down_to = BASE_NS + 20 * MINUTE_NS, BASE_NS + 30 * MINUTE_NS
    fold.disconnected(0, "deep", _topics("XUSDT"), down_from)
    fold.connected(0, "deep", _topics("XUSDT"), down_to)
    CoverageRecords(root, Manifest(root)).write(fold.roll(BASE_NS + HOUR_NS))
    _segment(root, "XUSDT", BASE_NS + MINUTE_NS)
    _segment(root, "YUSDT", BASE_NS + MINUTE_NS)

    cells = _cells(root, [HOUR])

    assert sorted(cells) == [("XUSDT", "book:50"), ("XUSDT", "trades"), ("YUSDT", "book:50"), ("YUSDT", "trades")]
    for feed in FEEDS:
        excluded = cells[("XUSDT", feed)]
        assert excluded.status == "excluded"
        assert [(reason.code, reason.from_ns, reason.to_ns) for reason in excluded.reasons] == [
            ("shard_disconnected", down_from, down_to)
        ]
        assert cells[("YUSDT", feed)].status == "complete", "the other shard never dropped"
    assert cells[("XUSDT", "trades")].records == 3, "the receipts still say what the hour holds"


def test_a_symbol_that_joined_the_tier_mid_hour_is_partial(tmp_path: Path) -> None:
    root = _root(tmp_path)
    joined = BASE_NS + 15 * MINUTE_NS
    fold = _fold(BASE_NS)
    fold.members("deep", ["XUSDT"], BASE_NS)
    fold.connected(0, "deep", _topics("XUSDT") + _topics("ZUSDT"), BASE_NS)
    fold.members("deep", ["XUSDT", "ZUSDT"], joined)
    CoverageRecords(root, Manifest(root)).write(fold.roll(BASE_NS + HOUR_NS))
    _segment(root, "ZUSDT", joined)

    cells = _cells(root, [HOUR])

    late = cells[("ZUSDT", "trades")]
    assert late.status == "excluded"
    assert [(reason.code, reason.from_ns, reason.to_ns) for reason in late.reasons] == [
        ("tier_membership_partial", BASE_NS, joined)
    ]
    assert cells[("XUSDT", "trades")].status == "complete"


def test_an_hour_the_recorder_joined_late_is_down_for_the_rest(tmp_path: Path) -> None:
    root = _root(tmp_path)
    hour_start = BASE_NS + HOUR_NS
    started = hour_start + 25 * MINUTE_NS
    fold = _fold(started)
    fold.members("deep", ["XUSDT"], started)
    fold.connected(0, "deep", _topics("XUSDT"), started)
    CoverageRecords(root, Manifest(root)).write(fold.roll(hour_start + HOUR_NS))
    _segment(root, "XUSDT", started + MINUTE_NS)

    cells = _cells(root, ["2027-01-15T09"])

    assert {key: cell.status for key, cell in cells.items()} == {
        ("XUSDT", "book:50"): "excluded",
        ("XUSDT", "trades"): "excluded",
    }
    for cell in cells.values():
        assert [(reason.code, reason.from_ns, reason.to_ns) for reason in cell.reasons] == [
            ("recorder_down", hour_start, started)
        ]


def test_contiguous_windows_stop_at_an_excluded_hour(tmp_path: Path) -> None:
    root = _root(tmp_path)
    records = CoverageRecords(root, Manifest(root))
    fold = _fold(BASE_NS)
    fold.members("deep", ["XUSDT"], BASE_NS)
    fold.connected(0, "deep", _topics("XUSDT"), BASE_NS)
    records.write(fold.roll(BASE_NS + HOUR_NS))
    # The middle hour is dark end to end; the span carries over the roll.
    fold.disconnected(0, "deep", _topics("XUSDT"), BASE_NS + HOUR_NS)
    fold.connected(0, "deep", _topics("XUSDT"), BASE_NS + 2 * HOUR_NS)
    records.write(fold.roll(BASE_NS + 2 * HOUR_NS))
    records.write(fold.roll(BASE_NS + 3 * HOUR_NS))
    for index in range(3):
        _segment(root, "XUSDT", BASE_NS + index * HOUR_NS + MINUTE_NS)

    hours = ["2027-01-15T08", "2027-01-15T09", "2027-01-15T10"]
    ledger = build_ledger(HostRoot(root, "bybit"), hours)

    assert ledger.admissible("XUSDT", "trades") == ["2027-01-15T08", "2027-01-15T10"]
    assert ledger.contiguous_windows("XUSDT", "trades") == [
        ("2027-01-15T08", "2027-01-15T09"),
        ("2027-01-15T10", "2027-01-15T11"),
    ]
    middle = next(cell for cell in ledger.cells if cell.hour == "2027-01-15T09" and cell.feed == "trades")
    assert [(reason.code, reason.from_ns, reason.to_ns) for reason in middle.reasons] == [
        ("shard_disconnected", BASE_NS + HOUR_NS, BASE_NS + 2 * HOUR_NS)
    ]


# -------------------------------------------------------------- what ships


def _recorded_hour(root: Path) -> dict[str, object]:
    """One complete hour on disk: rows, the venue tables, and the coverage record."""

    manifest = Manifest(root)
    fold = _fold(BASE_NS)
    fold.members("deep", ["XUSDT"], BASE_NS)
    fold.connected(0, "deep", _topics("XUSDT"), BASE_NS)
    fold.book_gap("orderbook.50.XUSDT", "XUSDT", 50, BASE_NS + 5 * MINUTE_NS)
    fold.book_snapshot("orderbook.50.XUSDT", BASE_NS + 6 * MINUTE_NS)
    payload = fold.roll(BASE_NS + HOUR_NS)
    CoverageRecords(root, manifest).write(payload)
    _segment(root, "XUSDT", BASE_NS + MINUTE_NS)
    Snapshots(root, manifest, venue="bybit", market="linear", source="http://unused", cadence="hour").write(
        BASE_NS + MINUTE_NS, {"instruments": [{"symbol": "XUSDT"}], "tickers": [{"symbol": "XUSDT"}]}
    )
    return payload


def test_the_coverage_record_rides_the_hourly_tar(tmp_path: Path) -> None:
    root = _root(tmp_path)
    payload = _recorded_hour(root)

    candidate = pack.finished_candidates(root, now=(BASE_NS + 2 * HOUR_NS) / 1e9, grace_seconds=0)
    assert [item.name for item in candidate] == [f"{DAY}T08Z"], "the raw record was unlinked before it was receipted"
    archive, manifest = pack.build_archive(
        candidate[0], root, tmp_path / "staging", pack.load_capture_manifest(root / "manifest.jsonl")
    )
    name = "_meta/coverage-08-20270115T080000Z.json.zst"
    assert name in [row["path"] for row in manifest["files"]]

    day = tmp_path / "archive" / "bybit-linear" / "2027" / "01" / "15"
    day.mkdir(parents=True)
    shutil.copy(archive, day / archive.name)
    source = ArchiveDir(tmp_path / "archive" / "bybit-linear")

    assert name in source.hour_manifest(HOUR)
    assert list(iter_coverage(source, [HOUR])) == [payload]
    assert source.skipped_rows == 0
    assert build_ledger(source, [HOUR]).summary()["complete"] == 1


def test_iter_snapshots_leaves_the_coverage_record_alone(tmp_path: Path) -> None:
    root = _root(tmp_path)
    _recorded_hour(root)
    source = HostRoot(root, "bybit")

    assert [payload["kind"] for payload in iter_snapshots(source, [HOUR])] == [
        "instruments_snapshot",
        "tickers_snapshot",
    ]
    assert source.skipped_rows == 0


def test_a_lane_feed_with_no_topic_is_complete_while_the_recorder_is_up(tmp_path: Path) -> None:
    """`funding` and `account_ratio` ride the REST lane, not a shard: a shard's
    outage says nothing about them, and the recorder being down says everything."""

    root = _root(tmp_path)
    fold = _fold(BASE_NS, feeds=("trades", "funding"))
    fold.members("deep", ["XUSDT"], BASE_NS)
    fold.connected(0, "deep", ["publicTrade.XUSDT"], BASE_NS)
    down_from, down_to = BASE_NS + 20 * MINUTE_NS, BASE_NS + 30 * MINUTE_NS
    fold.disconnected(0, "deep", ["publicTrade.XUSDT"], down_from)
    fold.connected(0, "deep", ["publicTrade.XUSDT"], down_to)
    CoverageRecords(root, Manifest(root)).write(fold.roll(BASE_NS + HOUR_NS))
    _segment(root, "XUSDT", BASE_NS + MINUTE_NS)

    cells = _cells(root, [HOUR])

    assert sorted(cells) == [("XUSDT", "funding"), ("XUSDT", "trades")]
    assert cells[("XUSDT", "trades")].status == "excluded"
    assert cells[("XUSDT", "funding")].status == "complete", "the shard that dropped carried no lane row"

    # A recorder that joined the hour late is down for the lane too.
    late = BASE_NS + HOUR_NS + 25 * MINUTE_NS
    fold = _fold(late, feeds=("trades", "funding"))
    fold.members("deep", ["XUSDT"], late)
    fold.connected(0, "deep", ["publicTrade.XUSDT"], late)
    CoverageRecords(root, Manifest(root)).write(fold.roll(BASE_NS + 2 * HOUR_NS))
    _segment(root, "XUSDT", late + MINUTE_NS)
    cells = _cells(root, ["2027-01-15T09"])
    funding = cells[("XUSDT", "funding")]
    assert funding.status == "excluded"
    assert [(reason.code, reason.from_ns, reason.to_ns) for reason in funding.reasons] == [
        ("recorder_down", BASE_NS + HOUR_NS, late)
    ]
