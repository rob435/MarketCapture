"""Bars from a hand-built stream: every column, two symbols, two buckets."""

from __future__ import annotations

from typing import Any

import pytest

from market_tape.bars import SCHEMA, build_bars
from market_tape.schema import (
    book_row,
    kline_row,
    liquidation_row,
    parse_row,
    ticker_row,
    trade_row,
)

SECOND = 1_000_000_000
T = 1_700_000_000 * SECOND
MS = 1_000_000


def _typed(raw: dict[str, Any]) -> Any:
    return parse_row(raw)


def _book(symbol: str, received_ns: int, depth: int, bids: list[list[str]], asks: list[list[str]], *,
          update_id: int | None = None) -> Any:
    return _typed(
        book_row(
            venue="bybit",
            symbol=symbol,
            snapshot=True,
            depth=depth,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns,
            exchange_engine_ts_ns=received_ns,
            bids=bids,
            asks=asks,
            update_id=received_ns if update_id is None else update_id,
            previous_update_id=0,
        )
    )


def _delta(symbol: str, received_ns: int, depth: int, bids: list[list[str]], asks: list[list[str]], *,
           previous_update_id: int, update_id: int | None = None, sequence_gap: bool = False) -> Any:
    return _typed(
        book_row(
            venue="bybit",
            symbol=symbol,
            snapshot=False,
            depth=depth,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns,
            exchange_engine_ts_ns=received_ns,
            bids=bids,
            asks=asks,
            update_id=previous_update_id + 1 if update_id is None else update_id,
            previous_update_id=previous_update_id,
            sequence_gap=sequence_gap,
        )
    )


def _trade(symbol: str, received_ns: int, price: float, qty: float, side: str) -> Any:
    return _typed(
        trade_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_ts_ns=received_ns,
            trade_id=f"{symbol}-{received_ns}",
            price=price,
            qty=qty,
            side=side,
        )
    )


def _ticker(symbol: str, received_ns: int, values: dict[str, float]) -> Any:
    return _typed(
        ticker_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns,
            message_type="snapshot",
            values=values,
        )
    )


def _liquidation(symbol: str, received_ns: int, qty: float) -> Any:
    return _typed(
        liquidation_row(
            venue="bybit",
            symbol=symbol,
            local_receive_ts_ns=received_ns,
            exchange_system_ts_ns=received_ns,
            exchange_ts_ns=received_ns,
            position_side="Buy",
            qty=qty,
            bankruptcy_price=100.0,
        )
    )


def _stream() -> list[Any]:
    return [
        _book("BTCUSDT", T + 1 * MS, 1, [["100", "2"]], [["102", "3"]]),
        _trade("ETHUSDT", T + 2 * MS, 2000.0, 5.0, "Buy"),
        _trade("BTCUSDT", T + 3 * MS, 100.5, 1.0, "Buy"),
        _book("BTCUSDT", T + 4 * MS, 50, [["99", "10"], ["98", "20"]], [["103", "5"]]),
        _trade("BTCUSDT", T + 5 * MS, 101.5, 2.0, "Sell"),
        _ticker(
            "BTCUSDT",
            T + 6 * MS,
            {"mark_price": 101.0, "index_price": 100.9, "funding_rate": 0.0001, "open_interest": 1234.0},
        ),
        _liquidation("BTCUSDT", T + 7 * MS, 0.5),
        _book("BTCUSDT", T + 8 * MS, 1, [["100.25", "1"]], [["101.75", "4"]]),
        _ticker("ETHUSDT", T + 9 * MS, {"mark_price": 2001.0}),
        _book("BTCUSDT", T + SECOND + 1 * MS, 50, [["95", "1"]], [["96", "2"]]),
        _trade("ETHUSDT", T + SECOND + 2 * MS, 1999.0, 1.0, "Sell"),
        _liquidation("BTCUSDT", T + SECOND + 3 * MS, 0.25),
        _typed(
            kline_row(
                venue="bybit",
                symbol="ETHUSDT",
                interval="1m",
                local_receive_ts_ns=T + SECOND + 4 * MS,
                exchange_system_ts_ns=T + SECOND + 4 * MS,
                start_ms=1,
                end_ms=2,
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=9.0,
                turnover=9.0,
                confirmed=True,
            )
        ),
    ]


def test_every_column_of_every_bar() -> None:
    frame = build_bars(_stream(), interval_seconds=1.0)
    assert list(frame.columns) == list(SCHEMA)
    assert frame.height == 4
    rows = frame.to_dicts()

    btc_first, btc_second, eth_first, eth_second = rows
    assert [(row["symbol"], row["bucket_start_ns"]) for row in rows] == [
        ("BTCUSDT", T),
        ("BTCUSDT", T + SECOND),
        ("ETHUSDT", T),
        ("ETHUSDT", T + SECOND),
    ]

    assert btc_first == {
        "venue": "bybit",
        "symbol": "BTCUSDT",
        "bucket_start_ns": T,
        "trades": 2,
        "volume": 3.0,
        "buy_volume": 1.0,
        "sell_volume": 2.0,
        "notional": pytest.approx(303.5),
        "open": 100.5,
        "high": 101.5,
        "low": 100.5,
        "close": 101.5,
        "vwap": pytest.approx(303.5 / 3.0),
        # The depth-1 feed owns the top of book; the depth-50 row does not touch it.
        "best_bid": 100.25,
        "best_ask": 101.75,
        "mid": 101.0,
        "spread_bp": pytest.approx(1.5 / 101.0 * 10_000.0),
        "book_updates": 3,
        "mark_price": 101.0,
        "index_price": 100.9,
        "funding_rate": 0.0001,
        "open_interest": 1234.0,
        "liquidations": 1,
        "liquidation_qty": 0.5,
    }

    assert btc_second == {
        "venue": "bybit",
        "symbol": "BTCUSDT",
        "bucket_start_ns": T + SECOND,
        "trades": 0,
        "volume": 0.0,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "notional": 0.0,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "vwap": None,
        # No depth-1 row in this bar, so the deeper book's own best levels stand in.
        "best_bid": 95.0,
        "best_ask": 96.0,
        "mid": 95.5,
        "spread_bp": pytest.approx(1.0 / 95.5 * 10_000.0),
        "book_updates": 1,
        # A ticker value is never carried across a bar boundary.
        "mark_price": None,
        "index_price": None,
        "funding_rate": None,
        "open_interest": None,
        "liquidations": 1,
        "liquidation_qty": 0.25,
    }

    assert eth_first == {
        "venue": "bybit",
        "symbol": "ETHUSDT",
        "bucket_start_ns": T,
        "trades": 1,
        "volume": 5.0,
        "buy_volume": 5.0,
        "sell_volume": 0.0,
        "notional": 10_000.0,
        "open": 2000.0,
        "high": 2000.0,
        "low": 2000.0,
        "close": 2000.0,
        "vwap": 2000.0,
        "best_bid": None,
        "best_ask": None,
        "mid": None,
        "spread_bp": None,
        "book_updates": 0,
        "mark_price": 2001.0,
        "index_price": None,
        "funding_rate": None,
        "open_interest": None,
        "liquidations": 0,
        "liquidation_qty": 0.0,
    }

    assert eth_second == {
        "venue": "bybit",
        "symbol": "ETHUSDT",
        "bucket_start_ns": T + SECOND,
        "trades": 1,
        "volume": 1.0,
        "buy_volume": 0.0,
        "sell_volume": 1.0,
        "notional": 1999.0,
        "open": 1999.0,
        "high": 1999.0,
        "low": 1999.0,
        "close": 1999.0,
        "vwap": 1999.0,
        "best_bid": None,
        "best_ask": None,
        "mid": None,
        "spread_bp": None,
        "book_updates": 0,
        "mark_price": None,
        "index_price": None,
        "funding_rate": None,
        "open_interest": None,
        "liquidations": 0,
        "liquidation_qty": 0.0,
    }


def test_a_shorter_interval_cuts_the_same_rows_finer() -> None:
    frame = build_bars(_stream(), interval_seconds=0.5)
    assert frame.height == 4
    assert frame.filter(frame["symbol"] == "BTCUSDT")["bucket_start_ns"].to_list() == [T, T + SECOND]
    frame = build_bars(_stream(), interval_seconds=60.0)
    assert frame.height == 2
    # Buckets are cut against the epoch, so a minute bar need not start on T.
    assert frame["bucket_start_ns"].to_list() == [T - 20 * SECOND, T - 20 * SECOND]
    with pytest.raises(ValueError):
        build_bars(_stream(), interval_seconds=0.0)


def test_no_rows_make_an_empty_frame_with_the_full_schema() -> None:
    frame = build_bars([], interval_seconds=1.0)
    assert frame.height == 0
    assert dict(zip(frame.columns, frame.dtypes)) == SCHEMA


def test_a_delta_moves_the_top_only_as_far_as_the_rebuilt_book_does() -> None:
    snapshot = _book("BTCUSDT", T + 1 * MS, 50, [["100", "2"], ["99", "5"]], [["102", "3"], ["103", "1"]],
                     update_id=10)
    # A size change on the second-best bid is not a new best bid.
    deeper = _delta("BTCUSDT", T + 2 * MS, 50, [["99", "7"]], [], previous_update_id=10)
    bar = build_bars([snapshot, deeper], interval_seconds=1.0).to_dicts()[0]
    assert bar["best_bid"] == 100.0 and bar["best_ask"] == 102.0
    assert bar["book_updates"] == 2

    # Deleting the best bid uncovers the level behind it.
    deleted = _delta("BTCUSDT", T + 3 * MS, 50, [["100", "0"]], [], previous_update_id=11)
    bar = build_bars([snapshot, deeper, deleted], interval_seconds=1.0).to_dicts()[0]
    assert bar["best_bid"] == 99.0 and bar["best_ask"] == 102.0
    assert bar["book_updates"] == 3

    # A gap leaves the book invalid, so the row does not move the reported top.
    gapped = _delta("BTCUSDT", T + 4 * MS, 50, [["90", "1"]], [["91", "1"]],
                    previous_update_id=12, update_id=20, sequence_gap=True)
    bar = build_bars([snapshot, deeper, deleted, gapped], interval_seconds=1.0).to_dicts()[0]
    assert bar["best_bid"] == 99.0 and bar["best_ask"] == 102.0
    assert bar["book_updates"] == 4


def test_the_book_carries_across_a_bar_boundary() -> None:
    rows = [
        _book("BTCUSDT", T + 1 * MS, 1, [["100", "2"], ["99", "5"]], [["102", "3"]], update_id=10),
        _delta("BTCUSDT", T + SECOND + 1 * MS, 1, [["100", "0"]], [], previous_update_id=10),
    ]
    first, second = build_bars(rows, interval_seconds=1.0).to_dicts()
    assert first["best_bid"] == 100.0
    # The delta lands in the next bar and still knows what the snapshot left behind it.
    assert second["best_bid"] == 99.0 and second["best_ask"] == 102.0
