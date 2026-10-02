"""The frozen regression: one real recorded hour, read back and re-derived.

The rows under `fixtures/` are real Bybit linear rows (`fixtures/build_fixture.py`
says how they were cut). `expected.json` holds what the loader, the book, and
the bars make of them. A change to the row contract, the merge order,
the chaining rule, or the bar columns moves one of these numbers, and this test
is where it shows up. Rebuild both with `fixtures/build_fixture.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_tape.bars import build_bars
from market_tape.book import Book
from market_tape.load import ArchiveDir, HostRoot, Source, iter_rows
from market_tape.schema import BookRow, TradeRow

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HOST = FIXTURES / "host" / "bybit-linear"
ARCHIVE = FIXTURES / "archive" / "bybit-linear"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text(encoding="utf-8"))


def _derive(source: Source) -> dict[str, object]:
    hours = source.hours()
    counts: dict[str, dict[str, int]] = {}
    spans: dict[str, list[int]] = {}
    first = last = 0
    for row in iter_rows(source, hours):
        counts.setdefault(row.symbol, {}).setdefault(row.kind, 0)
        counts[row.symbol][row.kind] += 1
        span = spans.setdefault(row.symbol, [row.local_receive_ts_ns, row.local_receive_ts_ns])
        span[1] = row.local_receive_ts_ns
        first = first or row.local_receive_ts_ns
        last = row.local_receive_ts_ns

    book = Book()
    for row in iter_rows(source, hours, symbols=["BTCUSDT"], kinds=["orderbook_snapshot", "orderbook_delta"]):
        assert isinstance(row, BookRow)
        if row.depth == 50:
            book.apply(row)

    bars = build_bars(iter_rows(source, hours), interval_seconds=1.0)
    trades = iter_rows(source, hours, symbols=["BTCUSDT"], kinds=["public_trade"])
    volume = sum(row.qty for row in trades if isinstance(row, TradeRow))
    bid, ask = book.depth_within(10)
    return {
        "hours": hours,
        "venue": source.venue,
        "skipped_rows": source.skipped_rows,
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


@pytest.mark.parametrize("source", [HostRoot(HOST), ArchiveDir(ARCHIVE)], ids=["host", "archive"])
def test_the_fixture_hour_re_derives_what_was_recorded(source: Source) -> None:
    derived = _derive(source)
    assert derived["hours"] == EXPECTED["hours"]
    assert derived["rows_by_symbol_kind"] == EXPECTED["rows_by_symbol_kind"]
    assert derived["first_receive_ns"] == EXPECTED["first_receive_ns"]
    assert derived["last_receive_ns"] == EXPECTED["last_receive_ns"]
    assert derived["span_by_symbol"] == EXPECTED["span_by_symbol"]
    assert derived["one_second_bars"] == EXPECTED["one_second_bars"]
    assert derived["btc_trade_volume"] == pytest.approx(EXPECTED["btc_trade_volume"])
    assert derived["venue"] == EXPECTED["venue"] == "bybit"
    assert derived["skipped_rows"] == EXPECTED["skipped_rows"] == 0

    book = derived["btc_book_depth50"]
    frozen = EXPECTED["btc_book_depth50"]
    assert isinstance(book, dict)
    assert book["valid"] is frozen["valid"] is True
    assert book["rows_applied"] == frozen["rows_applied"]
    assert book["last_update_id"] == frozen["last_update_id"]
    assert book["best_bid"] == pytest.approx(frozen["best_bid"])
    assert book["best_ask"] == pytest.approx(frozen["best_ask"])
    assert book["depth_within_10bp"] == pytest.approx(frozen["depth_within_10bp"])
