"""Rebuilding a book: what chains, what breaks it, and what a broken book says."""

from __future__ import annotations

import pytest

from market_tape.book import Book
from market_tape.schema import BookRow, book_row, parse_row


def _row(
    *,
    venue: str = "bybit",
    symbol: str = "BTCUSDT",
    snapshot: bool = False,
    depth: int = 50,
    received_ns: int = 1_000,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
    update_id: int = 0,
    previous_update_id: int = 0,
    first_update_id: int = 0,
    sequence_gap: bool = False,
) -> BookRow:
    row = parse_row(
        book_row(
            venue=venue,
            symbol=symbol,
            snapshot=snapshot,
            depth=depth,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns,
            exchange_engine_ts_ns=received_ns,
            bids=bids or [],
            asks=asks or [],
            update_id=update_id,
            previous_update_id=previous_update_id,
            first_update_id=first_update_id,
            sequence_gap=sequence_gap,
        ),
    )
    assert isinstance(row, BookRow)
    return row


# ------------------------------------------------------------------- bybit


def test_a_snapshot_makes_the_book_good_and_a_delta_sets_and_deletes() -> None:
    book = Book()
    assert book.apply(
        _row(snapshot=True, update_id=100, bids=[["99", "2"], ["98", "5"]], asks=[["101", "3"], ["102", "1"]])
    )
    assert book.valid
    assert book.best_bid == (99.0, 2.0)
    assert book.best_ask == (101.0, 3.0)
    assert book.mid == 100.0
    assert book.spread_bp == pytest.approx(2.0 / 100.0 * 10_000.0)

    assert book.apply(_row(update_id=101, bids=[["99", "4"], ["98", "0"]], asks=[["101", "0"]]))
    assert book.levels(5) == ([(99.0, 4.0)], [(102.0, 1.0)])
    assert book.last_update_id == 101


def test_a_gap_breaks_the_book_until_the_next_snapshot() -> None:
    book = Book()
    book.apply(_row(snapshot=True, update_id=10, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert not book.apply(_row(update_id=12, bids=[["99", "9"]], sequence_gap=True))
    assert not book.valid
    # While the book is broken every delta is refused, gap flag or not.
    assert not book.apply(_row(update_id=13, bids=[["99", "7"]]))
    assert book.best_bid == (99.0, 1.0)
    assert book.apply(_row(snapshot=True, update_id=20, bids=[["98", "2"]], asks=[["102", "2"]]))
    assert book.valid
    assert book.best_bid == (98.0, 2.0)


def test_a_delta_that_does_not_move_the_id_forward_breaks_the_book() -> None:
    book = Book()
    book.apply(_row(snapshot=True, update_id=10, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert book.apply(_row(update_id=11, bids=[["99", "2"]]))
    assert not book.apply(_row(update_id=11, bids=[["99", "3"]]))
    assert not book.valid
    assert book.best_bid == (99.0, 2.0)


def test_one_book_holds_one_symbol_at_one_depth() -> None:
    book = Book()
    book.apply(_row(snapshot=True, update_id=1, bids=[["99", "1"]], asks=[["101", "1"]]))
    with pytest.raises(ValueError, match="not ETHUSDT"):
        book.apply(_row(symbol="ETHUSDT", snapshot=True, update_id=2))
    with pytest.raises(ValueError, match="depth 1"):
        book.apply(_row(depth=1, snapshot=True, update_id=2))


# ----------------------------------------------------------------- binance


def test_binance_drops_deltas_older_than_the_snapshot() -> None:
    book = Book()
    book.apply(_row(venue="binance", snapshot=True, update_id=1000, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert book.apply(_row(venue="binance", update_id=900, first_update_id=890, bids=[["99", "9"]]))
    assert book.valid
    assert book.best_bid == (99.0, 1.0)


def test_binance_needs_the_first_delta_to_span_the_snapshot_then_chain_on_pu() -> None:
    book = Book()
    book.apply(_row(venue="binance", snapshot=True, update_id=1000, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert book.apply(_row(venue="binance", first_update_id=995, update_id=1005, bids=[["99", "2"]]))
    assert book.apply(_row(venue="binance", previous_update_id=1005, update_id=1010, bids=[["98", "3"]]))
    assert book.levels(5)[0] == [(99.0, 2.0), (98.0, 3.0)]
    assert not book.apply(_row(venue="binance", previous_update_id=1009, update_id=1015, bids=[["97", "4"]]))
    assert not book.valid
    assert book.apply(_row(venue="binance", snapshot=True, update_id=2000, bids=[["97", "5"]], asks=[["103", "5"]]))
    assert book.valid


def test_binance_refuses_a_first_delta_that_leaves_a_hole() -> None:
    book = Book()
    book.apply(_row(venue="binance", snapshot=True, update_id=1000, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert not book.apply(_row(venue="binance", first_update_id=1005, update_id=1010, bids=[["99", "2"]]))
    assert not book.valid


def test_binance_replays_the_diffs_held_before_the_snapshot_landed() -> None:
    book = Book()
    # The recorder fetches the snapshot while the stream already flows, so
    # these two diffs are on the tape ahead of it.
    assert not book.apply(_row(venue="binance", first_update_id=900, update_id=950, bids=[["97", "7"]]))
    assert not book.apply(_row(venue="binance", first_update_id=995, update_id=1005, bids=[["99", "2"]]))
    assert book.held_deltas == 2

    assert book.apply(
        _row(venue="binance", snapshot=True, update_id=1000, bids=[["99", "1"], ["98", "1"]], asks=[["101", "1"]])
    )
    assert book.valid
    assert book.held_deltas == 0
    # The straddling diff is applied; the one that ended before the snapshot is dropped.
    assert book.last_update_id == 1005
    assert book.best_bid == (99.0, 2.0)
    assert 97.0 not in dict(book.levels(10)[0])
    # The chain carries on from the replayed diff.
    assert book.apply(_row(venue="binance", previous_update_id=1005, update_id=1010, bids=[["98", "4"]]))
    assert book.levels(10)[0] == [(99.0, 2.0), (98.0, 4.0)]


def test_binance_holds_the_diffs_when_the_snapshot_is_too_stale() -> None:
    book = Book()
    assert not book.apply(_row(venue="binance", first_update_id=1100, update_id=1150, bids=[["99", "2"]]))
    # Every held diff starts after this snapshot's id, so nothing can chain onto it.
    assert not book.apply(_row(venue="binance", snapshot=True, update_id=1000, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert not book.valid
    assert book.held_deltas == 1
    assert book.best_bid == (99.0, 1.0)

    # A fresher snapshot meets the diff still held, and the book comes good.
    assert book.apply(_row(venue="binance", snapshot=True, update_id=1120, bids=[["99", "1"]], asks=[["101", "1"]]))
    assert book.valid
    assert book.held_deltas == 0
    assert book.last_update_id == 1150
    assert book.best_bid == (99.0, 2.0)


def test_a_bybit_delta_before_a_snapshot_is_not_held() -> None:
    book = Book()
    assert not book.apply(_row(update_id=5, bids=[["99", "1"]]))
    assert book.held_deltas == 0
    assert book.apply(_row(snapshot=True, update_id=10, bids=[["98", "1"]], asks=[["101", "1"]]))
    assert book.last_update_id == 10


# ------------------------------------------------------------- the readings


def test_depth_reads_off_the_rebuilt_book() -> None:
    book = Book()
    book.apply(
        _row(
            snapshot=True,
            update_id=1,
            bids=[["99.95", "1"], ["99.92", "2"], ["95", "10"]],
            asks=[["100.05", "3"], ["100.08", "1"], ["105", "20"]],
        )
    )
    assert book.mid == 100.0
    # 10 bp of a mid of 100 is 0.1, so the two inner levels of each side are inside.
    assert book.depth_within(10) == pytest.approx((99.95 + 2 * 99.92, 3 * 100.05 + 100.08))
    assert book.depth_within(1) == (0.0, 0.0)
    assert book.depth_within(1000) == pytest.approx((99.95 + 2 * 99.92 + 950.0, 3 * 100.05 + 100.08 + 2100.0))


def test_an_empty_book_describes_itself_as_empty() -> None:
    book = Book()
    described = book.describe()
    assert described["symbol"] is None
    assert described["valid"] is False
    assert described["best_bid"] is None
    assert described["mid"] is None
    assert described["spread_bp"] is None
    assert book.depth_within(10) == (0.0, 0.0)
