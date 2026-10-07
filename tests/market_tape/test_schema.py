"""The row contract: what each constructor writes, and what the reader gets back."""

from __future__ import annotations

import pytest

from market_tape.schema import (
    BookRow,
    KlineRow,
    LiquidationRow,
    SchemaError,
    TickerRow,
    TradeRow,
    book_row,
    kline_row,
    liquidation_row,
    parse_row,
    ticker_row,
    trade_row,
)


def test_a_trade_round_trips_and_refuses_an_unknown_side() -> None:
    raw = trade_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1_800_000_000_040_000_000,
        exchange_ts_ns=1_800_000_000_039_000_000,
        trade_id="one",
        price=0.0011,
        qty=100,
        side="Buy",
    )

    row = parse_row(raw)
    assert isinstance(row, TradeRow)
    assert (row.side, row.trade_id, row.price, row.qty) == ("Buy", "one", 0.0011, 100.0)

    with pytest.raises(SchemaError):
        trade_row(
            venue="bybit",
            symbol="AGIUSDT",
            local_receive_ts_ns=1,
            exchange_ts_ns=1,
            trade_id="one",
            price=1.0,
            qty=1.0,
            side="buy",
        )
    with pytest.raises(SchemaError):
        parse_row(dict(raw, side="Short"))


def test_a_ticker_round_trips_and_refuses_a_value_outside_the_contract() -> None:
    raw = ticker_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1_800_000_000_010_000_000,
        exchange_system_ts_ns=1_800_000_000_000_000_000,
        message_type="delta",
        values={"mark_price": 0.00105, "next_funding_time_ms": 1_800_003_600_000},
        cross_sequence=42,
    )

    row = parse_row(raw)
    assert isinstance(row, TickerRow)
    assert row.message_type == "delta"
    assert row.cross_sequence == 42
    assert row.values == {"mark_price": 0.00105, "next_funding_time_ms": 1_800_003_600_000}
    assert isinstance(row.values["next_funding_time_ms"], int)

    with pytest.raises(SchemaError):
        ticker_row(
            venue="bybit",
            symbol="AGIUSDT",
            local_receive_ts_ns=1,
            exchange_system_ts_ns=1,
            message_type="delta",
            values={"basis": 1.0},
        )
    with pytest.raises(SchemaError):
        parse_row(dict(raw, values={"basis": 1.0}))
    with pytest.raises(SchemaError):
        parse_row(dict(raw, values=None))


def test_a_row_needs_a_kind_a_venue_a_symbol_and_a_receive_clock() -> None:
    base = {"kind": "public_trade", "venue": "bybit", "symbol": "AGIUSDT", "local_receive_ts_ns": 1, "side": "Buy"}
    with pytest.raises(SchemaError, match="unknown row kind"):
        parse_row(dict(base, kind="open_interest"))
    with pytest.raises(SchemaError, match="lacks a venue"):
        parse_row({key: value for key, value in base.items() if key != "venue"})
    with pytest.raises(SchemaError, match="lacks a symbol"):
        parse_row({key: value for key, value in base.items() if key != "symbol"})
    with pytest.raises(SchemaError, match="lacks local_receive_ts_ns"):
        parse_row({key: value for key, value in base.items() if key != "local_receive_ts_ns"})


@pytest.mark.parametrize(
    ("kind", "fields", "dropped"),
    [
        ("public_trade", {"side": "Buy", "price": 1.5, "qty": 2.0}, "price"),
        ("public_trade", {"side": "Buy", "price": 1.5, "qty": 2.0}, "qty"),
        ("liquidation", {"position_side": "Sell", "qty": 2.0, "bankruptcy_price": 1.5}, "bankruptcy_price"),
        ("kline", {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 3.0, "turnover": 4.0}, "low"),
        ("funding_settlement", {"funding_time_ms": 1, "funding_rate": 0.0001}, "funding_rate"),
        ("account_ratio", {"ts_ms": 1, "period": "5min", "buy_ratio": 0.6, "sell_ratio": 0.4}, "sell_ratio"),
    ],
)
def test_a_number_a_row_always_carries_is_never_read_as_zero(kind: str, fields: dict, dropped: str) -> None:
    """A trade without a price once read as a print at 0.0, which `bars` took
    as the minute's low; the row is the tape's fault and is refused."""

    base = {"kind": kind, "venue": "bybit", "symbol": "AGIUSDT", "local_receive_ts_ns": 1, **fields}
    parse_row(base)
    with pytest.raises(SchemaError, match=f"lacks {dropped}"):
        parse_row({key: value for key, value in base.items() if key != dropped})
    for unreadable in ("x", float("nan"), float("inf")):
        with pytest.raises(SchemaError, match=dropped):
            parse_row(dict(base, **{dropped: unreadable}))


def test_an_unreadable_ticker_value_is_a_schema_error() -> None:
    base = {"kind": "ticker", "venue": "bybit", "symbol": "AGIUSDT", "local_receive_ts_ns": 1}
    with pytest.raises(SchemaError, match="mark_price"):
        parse_row(dict(base, values={"mark_price": "x"}))
    with pytest.raises(SchemaError, match="next_funding_time_ms"):
        parse_row(dict(base, values={"next_funding_time_ms": "soon"}))


def test_a_settled_funding_payment_and_an_account_ratio_bucket_round_trip() -> None:
    from market_tape.schema import (
        KIND_ACCOUNT_RATIO,
        KIND_FUNDING,
        AccountRatioRow,
        FundingRow,
        account_ratio_row,
        feed_of_row,
        funding_row,
    )

    funding = funding_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1_800_000_180_000_000_000,
        funding_time_ms=1_800_000_000_000,
        funding_rate=-0.00031,
    )
    assert funding["kind"] == KIND_FUNDING and feed_of_row(funding) == "funding"
    row = parse_row(funding)
    assert isinstance(row, FundingRow)
    assert (row.symbol, row.funding_time_ms, row.funding_rate) == ("AGIUSDT", 1_800_000_000_000, -0.00031)
    assert row.kind == KIND_FUNDING

    ratio = account_ratio_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1_800_000_180_000_000_000,
        period="5min",
        ts_ms=1_800_000_000_000,
        buy_ratio=0.62,
        sell_ratio=0.38,
    )
    assert ratio["kind"] == KIND_ACCOUNT_RATIO and feed_of_row(ratio) == "account_ratio"
    row = parse_row(ratio)
    assert isinstance(row, AccountRatioRow)
    assert (row.period, row.ts_ms, row.buy_ratio, row.sell_ratio) == ("5min", 1_800_000_000_000, 0.62, 0.38)
    # Both stamps are the venue's and required: a row without one is not a row.
    with pytest.raises(SchemaError):
        parse_row({k: v for k, v in funding.items() if k != "funding_time_ms"})
    with pytest.raises(SchemaError):
        parse_row({k: v for k, v in ratio.items() if k != "ts_ms"})


def test_the_ticker_carries_the_venues_trailing_marks() -> None:
    raw = ticker_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1,
        exchange_system_ts_ns=1,
        message_type="snapshot",
        values={"prev_price_1h": 0.9, "prev_price_24h": 0.8, "high_price_24h": 1.2, "low_price_24h": 0.7},
    )
    row = parse_row(raw)
    assert isinstance(row, TickerRow)
    assert row.values == {"prev_price_1h": 0.9, "prev_price_24h": 0.8, "high_price_24h": 1.2, "low_price_24h": 0.7}


def test_both_host_clocks_and_the_trade_send_stamp_round_trip_and_read_as_zero_on_older_rows() -> None:
    """Every stream row may carry the host's monotonic clock beside its wall
    clock, and a trade row the send stamp of the message that carried it; a row
    written before either existed reads both as 0, never as missing."""

    stamped = trade_row(
        venue="bybit",
        symbol="AGIUSDT",
        local_receive_ts_ns=1_800_000_000_010_000_000,
        local_receive_mono_ns=987_654_321,
        exchange_system_ts_ns=1_800_000_000_009_000_000,
        exchange_ts_ns=1_800_000_000_007_000_000,
        trade_id="one",
        price=0.0011,
        qty=100,
        side="Buy",
    )
    trade = parse_row(stamped)
    assert isinstance(trade, TradeRow)
    assert trade.local_receive_mono_ns == 987_654_321
    assert trade.exchange_system_ts_ns == 1_800_000_000_009_000_000
    assert trade.exchange_ts_ns == 1_800_000_000_007_000_000

    older = dict(stamped)
    del older["local_receive_mono_ns"]
    del older["exchange_system_ts_ns"]
    trade = parse_row(older)
    assert isinstance(trade, TradeRow)
    assert trade.local_receive_mono_ns == 0 and trade.exchange_system_ts_ns == 0

    book = book_row(
        venue="bybit",
        symbol="AGIUSDT",
        snapshot=False,
        depth=50,
        local_receive_ts_ns=1_800_000_000_010_000_000,
        local_receive_mono_ns=5,
        exchange_system_ts_ns=1_800_000_000_000_000_000,
        exchange_engine_ts_ns=1_799_999_999_999_000_000,
        bids=[],
        asks=[["0.0011", "30"]],
        update_id=2,
        previous_update_id=1,
    )
    parsed = parse_row(book)
    assert isinstance(parsed, BookRow) and parsed.local_receive_mono_ns == 5
    ticker = parse_row(
        ticker_row(
            venue="bybit",
            symbol="AGIUSDT",
            local_receive_ts_ns=1,
            local_receive_mono_ns=6,
            exchange_system_ts_ns=1,
            message_type="delta",
            values={"last_price": 1.0},
        ),
    )
    assert isinstance(ticker, TickerRow) and ticker.local_receive_mono_ns == 6
    liquidation = parse_row(
        liquidation_row(
            venue="bybit",
            symbol="AGIUSDT",
            local_receive_ts_ns=1,
            local_receive_mono_ns=7,
            exchange_system_ts_ns=1,
            exchange_ts_ns=1,
            position_side="Sell",
            qty=1.0,
            bankruptcy_price=1.0,
        ),
    )
    assert isinstance(liquidation, LiquidationRow) and liquidation.local_receive_mono_ns == 7
    kline = parse_row(
        kline_row(
            venue="bybit",
            symbol="AGIUSDT",
            interval="1",
            local_receive_ts_ns=1,
            local_receive_mono_ns=8,
            exchange_system_ts_ns=1,
            start_ms=0,
            end_ms=60_000,
            open=1,
            high=1,
            low=1,
            close=1,
            volume=1,
            turnover=1,
            confirmed=True,
        ),
    )
    assert isinstance(kline, KlineRow) and kline.local_receive_mono_ns == 8
