"""The quality frame says, per symbol and feed, what the rows carry: on the fixture hour and on a stream built to be bad."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from market_tape.__main__ import main as tape_main
from market_tape.load import HostRoot, iter_rows
from market_tape.quality import SCHEMA, build_quality, summarize
from market_tape.schema import book_row, parse_row, ticker_row, trade_row

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HOST = FIXTURES / "host" / "bybit-linear"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))

SECOND = 1_000_000_000
T = 1_700_000_000 * SECOND
MS = 1_000_000


def _typed(raw: dict[str, Any]) -> Any:
    return parse_row(raw)


def _book(symbol: str, received_ns: int, *, snapshot: bool, update_id: int, previous: int, gap: bool = False) -> Any:
    return _typed(
        book_row(
            venue="bybit",
            symbol=symbol,
            snapshot=snapshot,
            depth=50,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns - 7 * MS,
            exchange_engine_ts_ns=received_ns - 9 * MS,
            bids=[["100", "1"]],
            asks=[["101", "1"]],
            update_id=update_id,
            previous_update_id=previous,
            sequence_gap=gap,
        )
    )


def _trade(symbol: str, received_ns: int, trade_id: str) -> Any:
    return _typed(
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_ts_ns=received_ns - 12 * MS,
            trade_id=trade_id,
            price=100.5,
            qty=1.0,
            side="Buy",
        )
    )


def _ticker(symbol: str, received_ns: int, message_type: str, **values: float) -> Any:
    return _typed(
        ticker_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns - 5 * MS,
            message_type=message_type,
            values=values,
        )
    )


def test_the_fixture_hour_reads_as_the_clean_hour_it_is() -> None:
    source = HostRoot(HOST)
    frame = build_quality(iter_rows(source, source.hours()))
    assert list(frame.columns) == list(SCHEMA)
    by_key = {(row["symbol"], row["feed"]): row for row in frame.to_dicts()}
    assert set(by_key) == {
        (symbol, feed) for symbol in ("BTCUSDT", "PENDLEUSDT") for feed in ("book:1", "book:50", "ticker", "trades")
    }
    for symbol, kinds in EXPECTED["rows_by_symbol_kind"].items():
        books = by_key[(symbol, "book:1")]["rows"] + by_key[(symbol, "book:50")]["rows"]
        assert books == kinds["orderbook_delta"] + kinds["orderbook_snapshot"]
        assert by_key[(symbol, "trades")]["rows"] == kinds["public_trade"]
        assert by_key[(symbol, "ticker")]["rows"] == kinds["ticker"]
        # The depth-1 stream is snapshots only; the depth-50 stream chains.
        assert by_key[(symbol, "book:1")]["deltas"] == 0
        for feed in ("book:1", "book:50"):
            cell = by_key[(symbol, feed)]
            assert cell["sequence_gaps"] == 0 and cell["refused"] == 0
            assert cell["chained"] is True and cell["anchored"] is True
        assert by_key[(symbol, "trades")]["duplicate_trade_ids"] == 0
        assert by_key[(symbol, "ticker")]["ticker_fields"] > 0
    btc = by_key[("BTCUSDT", "book:50")]
    assert btc["deltas"] == EXPECTED["btc_book_depth50"]["rows_applied"] - btc["snapshots"]
    # Every row carries the venue's stamp, so the skew is measured everywhere,
    # and on a host a few milliseconds from the venue it is small and positive.
    for cell in by_key.values():
        assert cell["skew_min_ms"] is not None and cell["skew_p50_ms"] is not None
        assert cell["skew_min_ms"] <= cell["skew_p50_ms"] <= cell["skew_p99_ms"] <= cell["skew_max_ms"]
        assert -1_000 < cell["skew_p50_ms"] < 5_000
        assert cell["max_silence_ms"] >= 0.0
        assert cell["first_receive_ns"] <= cell["last_receive_ns"]
    summary = summarize(frame)
    assert summary["symbols"] == 2
    assert summary["rows"] == sum(sum(kinds.values()) for kinds in EXPECTED["rows_by_symbol_kind"].values())
    assert summary["by_feed"]["book:50"]["unchained"] == [] and summary["by_feed"]["book:50"]["unanchored"] == []
    assert summary["by_feed"]["ticker"]["unanchored"] == []
    assert summary["by_feed"]["trades"]["duplicate_trade_ids"] == 0
    assert summary["skew_p50_ms"] is not None and summary["skew_p99_ms"] is not None


def test_a_stream_built_to_be_bad_is_named_cell_by_cell() -> None:
    rows = [
        # ALPHA: opens on a delta (unanchored), chains, then jumps and never re-bases.
        _book("ALPHAUSDT", T, snapshot=False, update_id=10, previous=9),
        _book("ALPHAUSDT", T + 1 * SECOND, snapshot=True, update_id=11, previous=10),
        _book("ALPHAUSDT", T + 2 * SECOND, snapshot=False, update_id=12, previous=11),
        _book("ALPHAUSDT", T + 9 * SECOND, snapshot=False, update_id=14, previous=12, gap=True),
        _book("ALPHAUSDT", T + 10 * SECOND, snapshot=False, update_id=15, previous=14, gap=True),
        # BETA: a clean book, a ticker that never carried a snapshot, and a replayed print.
        _book("BETAUSDT", T, snapshot=True, update_id=1, previous=0),
        _book("BETAUSDT", T + 1 * SECOND, snapshot=False, update_id=2, previous=1),
        _ticker("BETAUSDT", T + 1 * SECOND, "delta", last_price=100.0),
        _ticker("BETAUSDT", T + 2 * SECOND, "delta", open_interest=5.0),
        _trade("BETAUSDT", T + 3 * SECOND, "t-1"),
        _trade("BETAUSDT", T + 4 * SECOND, "t-2"),
        _trade("BETAUSDT", T + 5 * SECOND, "t-1"),
        # GAMMA: a ticker that opened on its snapshot.
        _ticker("GAMMAUSDT", T, "snapshot", last_price=1.0, mark_price=1.0, funding_rate=0.0001),
        _ticker("GAMMAUSDT", T + 30 * SECOND, "delta", last_price=1.01),
    ]
    frame = build_quality(rows)
    cells = {(row["symbol"], row["feed"]): row for row in frame.to_dicts()}

    alpha = cells[("ALPHAUSDT", "book:50")]
    assert (alpha["rows"], alpha["snapshots"], alpha["deltas"]) == (5, 1, 4)
    assert alpha["anchored"] is False
    assert alpha["sequence_gaps"] == 2
    # The first delta had no book to land on, and the two after the jump were refused.
    assert alpha["refused"] == 3
    assert alpha["chained"] is False
    assert alpha["max_silence_ms"] == 7_000.0
    assert alpha["skew_p50_ms"] == 7.0 and alpha["skew_min_ms"] == 7.0 and alpha["skew_max_ms"] == 7.0

    beta_book = cells[("BETAUSDT", "book:50")]
    assert beta_book["chained"] is True and beta_book["anchored"] is True and beta_book["refused"] == 0
    beta_ticker = cells[("BETAUSDT", "ticker")]
    assert beta_ticker["anchored"] is False and beta_ticker["ticker_fields"] == 2 and beta_ticker["skew_p50_ms"] == 5.0
    beta_trades = cells[("BETAUSDT", "trades")]
    assert beta_trades["rows"] == 3 and beta_trades["duplicate_trade_ids"] == 1 and beta_trades["skew_p50_ms"] == 12.0
    assert beta_trades["chained"] is None and beta_trades["anchored"] is None

    gamma = cells[("GAMMAUSDT", "ticker")]
    assert gamma["anchored"] is True and gamma["ticker_fields"] == 3 and gamma["max_silence_ms"] == 30_000.0

    summary = summarize(frame)
    assert summary["by_feed"]["book:50"] == {
        "symbols": 2,
        "rows": 7,
        "skew_p50_ms": 7.0,
        "max_silence_ms": 7_000.0,
        "clock_steps": 0,
        "sequence_gaps": 2,
        "refused": 3,
        "unchained": ["ALPHAUSDT"],
        "unanchored": ["ALPHAUSDT"],
    }
    assert summary["by_feed"]["ticker"]["unanchored"] == ["BETAUSDT"]
    assert summary["by_feed"]["trades"]["duplicate_trade_ids"] == 1
    assert summary["symbols"] == 3 and summary["rows"] == 14


def test_an_empty_stream_is_an_empty_frame() -> None:
    frame = build_quality([])
    assert frame.height == 0 and list(frame.columns) == list(SCHEMA)
    assert summarize(frame) == {"symbols": 0, "rows": 0, "skew_p50_ms": None, "skew_p99_ms": None, "clock_steps": 0, "by_feed": {}}


def test_the_verb_prints_the_summary_and_writes_the_frame(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "quality.parquet"
    assert tape_main(["quality", str(HOST), "--hours", "2026-08-30T00", "--symbols", "BTCUSDT", "--out", str(out)]) == 0
    printed = capsys.readouterr().out
    assert printed.startswith("symbols 1  rows ")
    assert "book:50" in printed and "ticker" in printed and f"wrote 4 rows to {out}" in printed
    import polars as pl

    assert pl.read_parquet(out).height == 4
    assert tape_main(["quality", str(HOST), "--hours", "2026-08-30T00", "--json"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["symbols"] == 2 and set(summary["by_feed"]) == {"book:1", "book:50", "ticker", "trades"}


def test_the_rest_lane_rows_are_counted_per_feed_without_a_clock_reading() -> None:
    from market_tape.schema import account_ratio_row, funding_row

    rows = [
        _typed(funding_row(venue="bybit", symbol="AUSDT", local_receive_ts_ns=T, funding_time_ms=1, funding_rate=0.0001)),
        _typed(funding_row(venue="bybit", symbol="AUSDT", local_receive_ts_ns=T + SECOND, funding_time_ms=2, funding_rate=0.0002)),
        _typed(
            account_ratio_row(
                venue="bybit", symbol="AUSDT", local_receive_ts_ns=T, period="5min", ts_ms=1, buy_ratio=0.6, sell_ratio=0.4
            )
        ),
    ]
    frame = build_quality(rows)
    cells = {row["feed"]: row for row in frame.to_dicts()}
    assert cells["funding"]["rows"] == 2 and cells["funding"]["max_silence_ms"] == 1_000.0
    assert cells["account_ratio"]["rows"] == 1
    for cell in cells.values():
        assert cell["skew_p50_ms"] is None and cell["chained"] is None and cell["anchored"] is None
    summary = summarize(frame)
    assert summary["by_feed"]["funding"] == {
        "symbols": 1, "rows": 2, "skew_p50_ms": None, "max_silence_ms": 1_000.0, "clock_steps": 0
    }


def _stamped_trade(symbol: str, received_ns: int, mono_ns: int, trade_id: str, *, sent_ns: int = 0) -> Any:
    return _typed(
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            local_receive_mono_ns=mono_ns,
            exchange_system_ts_ns=sent_ns,
            exchange_ts_ns=received_ns - 12 * MS,
            trade_id=trade_id,
            price=100.5,
            qty=1.0,
            side="Buy",
        )
    )


def test_a_wall_clock_step_shows_against_the_monotonic_stamp_and_a_slew_does_not() -> None:
    """Rows stamped with both host clocks fix the wall clock's offset from the
    monotonic clock; the offset moving more than a slew between two rows of one
    feed is a step, counted on the row it lands on."""

    mono = 50 * SECOND
    rows = [
        _stamped_trade("AUSDT", T, mono, "t-1"),
        # Ten seconds on, both clocks advanced ten seconds: no move.
        _stamped_trade("AUSDT", T + 10 * SECOND, mono + 10 * SECOND, "t-2"),
        # Ten more seconds, the wall clock 20 ms ahead of where the monotonic
        # clock says it should be. The allowance over a 10 s gap is 1 ms plus
        # 0.1 % of it, 11 ms; this is more, so it is a step.
        _stamped_trade("AUSDT", T + 20 * SECOND + 20 * MS, mono + 20 * SECOND, "t-3"),
        # Half a second on, the wall clock slewed 200 µs: inside the 1.5 ms allowance.
        _stamped_trade("AUSDT", T + 20 * SECOND + 20 * MS + 500 * MS + 200_000, mono + 20 * SECOND + 500 * MS, "t-4"),
        # Then set back 30 ms in the next half second: a second step.
        _stamped_trade("AUSDT", T + 21 * SECOND - 10 * MS, mono + 21 * SECOND, "t-5"),
        # A row from before the monotonic stamp counts nothing and breaks no chain.
        _trade("BUSDT", T, "u-1"),
        _trade("BUSDT", T + SECOND, "u-2"),
    ]
    frame = build_quality(rows)
    cells = {row["symbol"]: row for row in frame.to_dicts()}
    assert cells["AUSDT"]["clock_steps"] == 2
    assert cells["BUSDT"]["clock_steps"] == 0
    summary = summarize(frame)
    assert summary["clock_steps"] == 2 and summary["by_feed"]["trades"]["clock_steps"] == 2


def test_a_print_is_a_clock_reading_against_its_messages_send_stamp_where_it_has_one() -> None:
    """A trade row that carries the send stamp of the message that brought it
    reads its skew against that stamp, as a book row does; older tape, with the
    print's own time only, still reads against that."""

    rows = [
        _stamped_trade("AUSDT", T, 1, "t-1", sent_ns=T - 3 * MS),
        _stamped_trade("AUSDT", T + SECOND, 1 + SECOND, "t-2", sent_ns=T + SECOND - 5 * MS),
        _trade("BUSDT", T, "u-1"),
    ]
    cells = {row["symbol"]: row for row in build_quality(rows).to_dicts()}
    assert (cells["AUSDT"]["skew_min_ms"], cells["AUSDT"]["skew_max_ms"]) == (3.0, 5.0)
    assert cells["BUSDT"]["skew_p50_ms"] == 12.0
