"""The Bybit adapter: stream names, subscribe frames, message shapes, REST tables."""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

import pytest

from market_tape.config import ConfigError, parse_feed
from market_tape.venues.bybit import PUBLIC_LINEAR_WS, BybitAdapter


def rows(adapter: BybitAdapter, message: dict[str, Any], received_ns: int) -> list[dict[str, Any]]:
    return adapter.normalize(json.dumps(message), received_ns)


def book_message(kind: str = "snapshot", update: int = 10, sequence: int = 100) -> dict[str, Any]:
    return {
        "topic": "orderbook.50.AGIUSDT",
        "type": kind,
        "ts": 1_800_000_000_000,
        "cts": 1_799_999_999_999,
        "data": {
            "s": "AGIUSDT",
            "b": [["0.001", "20"]],
            "a": [["0.0011", "30"]],
            "u": update,
            "seq": sequence,
        },
    }


def test_the_normalizer_preserves_book_order_and_public_trade_arrivals() -> None:
    adapter = BybitAdapter()
    snapshot = rows(adapter, book_message(), 1_800_000_000_010_000_000)[0]
    delta = rows(adapter, book_message("delta", 11, 101), 1_800_000_000_020_000_000)[0]
    regression = rows(adapter, book_message("delta", 9, 99), 1_800_000_000_030_000_000)[0]
    trades = rows(
        adapter,
        {
            "topic": "publicTrade.AGIUSDT",
            "ts": 1_800_000_000_040,
            "data": [
                {"s": "AGIUSDT", "S": "Buy", "p": "0.0011", "v": "100", "i": "one"},
                {"s": "AGIUSDT", "S": "Sell", "p": "0.0010", "v": "80", "i": "two"},
            ],
        },
        1_800_000_000_040_000_000,
    )

    assert snapshot["kind"] == "orderbook_snapshot"
    assert snapshot["venue"] == "bybit"
    assert snapshot["depth"] == 50
    assert snapshot["exchange_system_ts_ns"] == 1_800_000_000_000_000_000
    assert snapshot["exchange_engine_ts_ns"] == 1_799_999_999_999_000_000
    assert snapshot["bids"] == [["0.001", "20"]]
    assert snapshot["cross_sequence"] == 100
    assert not snapshot["sequence_gap"]
    assert not snapshot["restart_snapshot"]
    assert delta["kind"] == "orderbook_delta"
    assert delta["previous_update_id"] == 10
    assert delta["previous_cross_sequence"] == 100
    assert not delta["sequence_gap"]
    assert regression["sequence_gap"]
    assert [(row["side"], row["trade_id"]) for row in trades] == [("Buy", "one"), ("Sell", "two")]
    assert len({row["local_receive_ts_ns"] for row in trades}) == 1
    assert trades[0]["exchange_ts_ns"] == 1_800_000_000_040_000_000
    assert trades[0]["venue"] == "bybit"


def test_a_first_update_id_reads_as_a_restart_snapshot() -> None:
    adapter = BybitAdapter()
    row = rows(adapter, book_message("delta", update=1, sequence=1), 1_800_000_000_010_000_000)[0]

    assert row["kind"] == "orderbook_snapshot"
    assert row["restart_snapshot"]
    assert not row["sequence_gap"]


def test_the_normalizer_preserves_ticker_deltas_and_liquidations() -> None:
    adapter = BybitAdapter()
    ticker = rows(
        adapter,
        {
            "topic": "tickers.AGIUSDT",
            "type": "delta",
            "ts": 1_800_000_000_000,
            "cs": 42,
            "data": {
                "symbol": "AGIUSDT",
                "markPrice": "0.00105",
                "openInterestValue": "125000",
                "fundingRate": "-0.0001",
                "nextFundingTime": "1800003600000",
                "indexPrice": "",
                "bid1Price": "not a number",
            },
        },
        1_800_000_000_010_000_000,
    )[0]
    liquidation = rows(
        adapter,
        {
            "topic": "allLiquidation.AGIUSDT",
            "type": "snapshot",
            "ts": 1_800_000_000_020,
            "data": [{"T": 1_800_000_000_019, "s": "AGIUSDT", "S": "Buy", "v": "20000", "p": "0.0009"}],
        },
        1_800_000_000_020_000_000,
    )[0]

    assert ticker == {
        "local_receive_mono_ns": 0,
        "kind": "ticker",
        "venue": "bybit",
        "symbol": "AGIUSDT",
        "local_receive_ts_ns": 1_800_000_000_010_000_000,
        "exchange_system_ts_ns": 1_800_000_000_000_000_000,
        "message_type": "delta",
        "cross_sequence": 42,
        "values": {
            "mark_price": 0.00105,
            "open_interest_value": 125000.0,
            "funding_rate": -0.0001,
            "next_funding_time_ms": 1_800_003_600_000,
        },
    }
    assert liquidation["position_side"] == "Buy"
    assert liquidation["qty"] == 20000.0
    assert liquidation["bankruptcy_price"] == 0.0009
    assert liquidation["exchange_ts_ns"] == 1_800_000_000_019_000_000
    assert liquidation["venue"] == "bybit"


def test_the_normalizer_reads_venue_candles() -> None:
    adapter = BybitAdapter()
    candles = rows(
        adapter,
        {
            "topic": "kline.1.AGIUSDT",
            "type": "snapshot",
            "ts": 1_800_000_060_100,
            "data": [
                {
                    "start": 1_800_000_000_000,
                    "end": 1_800_000_059_999,
                    "open": "1.0",
                    "high": "2.0",
                    "low": "0.5",
                    "close": "1.5",
                    "volume": "100",
                    "turnover": "150",
                    "confirm": True,
                },
                {"start": 1_800_000_060_000, "end": 1_800_000_119_999, "open": "1.5", "confirm": False},
            ],
        },
        1_800_000_060_150_000_000,
    )

    assert [row["kind"] for row in candles] == ["kline", "kline"]
    assert candles[0]["interval"] == "1m"
    assert candles[0]["symbol"] == "AGIUSDT"
    assert candles[0]["confirmed"] and not candles[1]["confirmed"]
    assert (candles[0]["open"], candles[0]["high"], candles[0]["low"], candles[0]["close"]) == (1.0, 2.0, 0.5, 1.5)
    assert (candles[0]["volume"], candles[0]["turnover"]) == (100.0, 150.0)
    assert candles[1]["close"] == 0.0
    assert candles[0]["exchange_system_ts_ns"] == 1_800_000_060_100_000_000


def test_a_control_frame_or_an_unknown_topic_writes_nothing() -> None:
    adapter = BybitAdapter()
    assert rows(adapter, {"success": True, "op": "subscribe"}, 1) == []
    assert rows(adapter, {"topic": "position", "data": {"s": "AGIUSDT"}}, 1) == []
    assert rows(adapter, {"topic": "orderbook.50.AGIUSDT", "data": []}, 1) == []
    assert adapter.normalize(json.dumps([1, 2]), 1) == []


def test_a_refused_subscription_says_what_the_venue_said(caplog: pytest.LogCaptureFixture) -> None:
    adapter = BybitAdapter()
    with caplog.at_level(logging.WARNING):
        accepted = {"success": True, "ret_msg": "", "conn_id": "x", "req_id": "", "op": "subscribe"}
        assert rows(adapter, accepted, 1) == []
        assert rows(adapter, {"success": True, "ret_msg": "pong", "conn_id": "x", "op": "ping"}, 1) == []
        assert caplog.text == ""
        refused = {"success": False, "ret_msg": "Invalid symbol", "conn_id": "x", "req_id": "", "op": "subscribe"}
        assert rows(adapter, refused, 1) == []
        assert rows(adapter, {"retCode": 10404, "retMsg": "op type is not found", "op": "subscribe"}, 1) == []
    assert "bybit refused a subscription: Invalid symbol" in caplog.text
    assert "bybit refused a subscription: op type is not found" in caplog.text


def test_each_feed_names_its_venue_topic() -> None:
    adapter = BybitAdapter()
    feeds = [parse_feed(text) for text in ("book:50", "book:1", "trades", "ticker", "liquidations", "kline:1m")]

    assert adapter.topics("AGIUSDT", feeds) == [
        "orderbook.50.AGIUSDT",
        "orderbook.1.AGIUSDT",
        "publicTrade.AGIUSDT",
        "tickers.AGIUSDT",
        "allLiquidation.AGIUSDT",
        "kline.1.AGIUSDT",
    ]
    assert adapter.topics("AGIUSDT", [parse_feed("kline:1h")]) == ["kline.60.AGIUSDT"]
    assert adapter.topics("AGIUSDT", []) == []


def test_a_feed_bybit_does_not_publish_is_refused() -> None:
    adapter = BybitAdapter()
    adapter.validate_feeds([parse_feed(text) for text in ("book:1", "book:50", "book:1000", "trades", "ticker", "liquidations", "kline:1m")])

    with pytest.raises(ConfigError, match="book levels"):
        adapter.validate_feeds([parse_feed("book:7")])
    with pytest.raises(ConfigError, match="open interest on the ticker"):
        adapter.validate_feeds([parse_feed("open_interest:60")])
    with pytest.raises(ConfigError, match="kline intervals"):
        adapter.validate_feeds([parse_feed("kline:2m")])


def test_topics_are_subscribed_ten_at_a_time() -> None:
    adapter = BybitAdapter()
    topics = [f"tickers.SYM{index}USDT" for index in range(25)]

    messages = adapter.subscribe_messages(topics)

    assert len(messages) == 3
    args = [json.loads(text)["args"] for text in messages]
    assert [len(chunk) for chunk in args] == [10, 10, 5]
    assert [topic for chunk in args for topic in chunk] == topics
    assert {json.loads(text)["op"] for text in messages} == {"subscribe"}
    assert adapter.subscribe_messages([]) == []
    assert adapter.connection_url(topics) == PUBLIC_LINEAR_WS
    assert adapter.start_lanes({}, lambda row: None, threading.Event()) == []


def _perp(symbol: str, *, quote: str = "USDT", status: str = "Trading", contract: str = "LinearPerpetual", **extra: str) -> dict:
    return {"symbol": symbol, "status": status, "quoteCoin": quote, "settleCoin": quote, "contractType": contract, **extra}


def test_the_listed_universe_is_the_trading_crypto_usdt_perpetuals() -> None:
    adapter = BybitAdapter()
    instruments = [
        _perp("BTCUSDT"),
        _perp("MYXUSDT", symbolType="innovation"),
        _perp("ETHPERP", quote="USDC"),
        _perp("BTC-26SEP26", contract="LinearFutures"),
        _perp("OLDUSDT", status="Closed"),
        _perp("solusdt"),
        # Listed as perpetuals in the same category, told apart only by symbolType.
        _perp("NVDAUSDT", symbolType="stock", underlyingTicker="NVDA", marketRegion="US"),
        _perp("SOXLUSDT", symbolType="ETF", underlyingTicker="SOXL"),
        _perp("XAUUSDT", symbolType="commodity"),
        _perp("NEWTHINGUSDT", symbolType="index"),
        "not a row",
    ]

    assert adapter.listed_symbols(instruments, quote="USDT") == ["BTCUSDT", "MYXUSDT", "SOLUSDT"]
    assert adapter.listed_symbols(instruments, quote=None) == ["BTCUSDT", "ETHPERP", "MYXUSDT", "SOLUSDT"]
    assert adapter.listed_symbols([], quote="USDT") == []
    # A label the venue has not used yet is outside the domain too, and is
    # counted so it shows up in the journal instead of vanishing.
    assert adapter.excluded_listed(instruments, quote="USDT") == {"ETF": 1, "commodity": 1, "index": 1, "stock": 1}
    assert adapter.excluded_listed(instruments, quote="USDC") == {}


def test_turnover_ranks_highest_first_and_funding_reads_as_a_fraction() -> None:
    adapter = BybitAdapter()
    tickers = [
        {"symbol": "AGIUSDT", "turnover24h": "1000", "fundingRate": "-0.0012"},
        {"symbol": "BTCUSDT", "turnover24h": "9000000", "fundingRate": "0.0001"},
        {"symbol": "ETHUSDT", "turnover24h": "1000", "fundingRate": ""},
        {"symbol": "BADUSDT", "turnover24h": "n/a", "fundingRate": "n/a"},
        {"symbol": "NEWUSDT"},
        "not a row",
    ]

    assert adapter.turnovers(tickers) == {"AGIUSDT": 1000.0, "BTCUSDT": 9000000.0, "ETHUSDT": 1000.0, "NEWUSDT": 0.0}
    assert adapter.funding_rates(tickers) == {"AGIUSDT": -0.0012, "BTCUSDT": 0.0001}


def test_live_subscription_changes_and_the_24h_change_field() -> None:
    adapter = BybitAdapter()
    topics = [f"publicTrade.S{index}" for index in range(12)]

    added = [json.loads(text) for text in adapter.add_messages(topics)]
    removed = [json.loads(text) for text in adapter.remove_messages(topics[:3])]

    assert [message["op"] for message in added] == ["subscribe", "subscribe"]
    assert [len(message["args"]) for message in added] == [10, 2]
    assert removed == [{"op": "unsubscribe", "args": topics[:3]}]

    rows = adapter.normalize(
        json.dumps(
            {
                "topic": "tickers.AGIUSDT",
                "type": "delta",
                "ts": 1_800_000_000_000,
                "data": {"symbol": "AGIUSDT", "price24hPcnt": "-0.1234", "turnover24h": "42"},
            }
        ),
        7,
    )
    assert rows[0]["values"] == {"price_change_24h_pct": -0.1234, "turnover_24h": 42.0}
    assert adapter.turnovers([{"symbol": "a", "turnover24h": "5"}, {"symbol": "b", "turnover24h": "x"}, {"symbol": "c"}]) == {
        "A": 5.0,
        "C": 0.0,
    }


def test_the_ticker_normalizer_takes_the_trailing_marks() -> None:
    adapter = BybitAdapter()
    ticker = rows(
        adapter,
        {
            "topic": "tickers.AGIUSDT",
            "type": "snapshot",
            "ts": 1_800_000_000_000,
            "data": {
                "symbol": "AGIUSDT",
                "lastPrice": "1.0",
                "prevPrice1h": "0.98",
                "prevPrice24h": "0.90",
                "highPrice24h": "1.05",
                "lowPrice24h": "0.88",
                "tickDirection": "PlusTick",
            },
        },
        1_800_000_000_010_000_000,
    )[0]
    assert ticker["values"] == {
        "last_price": 1.0,
        "prev_price_1h": 0.98,
        "prev_price_24h": 0.90,
        "high_price_24h": 1.05,
        "low_price_24h": 0.88,
    }


def _funding_history(stamps_and_rates: list[tuple[int, str]]) -> list[dict[str, str]]:
    # Newest first, as the venue lists them.
    return [
        {"symbol": "X", "fundingRate": rate, "fundingRateTimestamp": str(stamp)}
        for stamp, rate in sorted(stamps_and_rates, reverse=True)
    ]


def _ratio_history(stamps: list[int]) -> list[dict[str, str]]:
    return [
        {"symbol": "X", "buyRatio": f"{0.5 + index / 100:.2f}", "sellRatio": f"{0.5 - index / 100:.2f}", "timestamp": str(stamp)}
        for index, stamp in enumerate(sorted(stamps, reverse=True))
    ]


def test_the_rest_lane_walks_both_universes_paced_and_writes_each_stamp_once() -> None:
    from market_tape.venues.bybit import ACCOUNT_RATIO_LIMIT, FUNDING_HISTORY_LIMIT, RestLane

    hour = 3_600_000
    emitted: list[dict] = []
    calls: list[tuple] = []
    slept: list[float] = []
    funding_pages = {
        "AAAUSDT": _funding_history([(8 * hour, "0.0001"), (16 * hour, "-0.0002")]),
        "BBBUSDT": _funding_history([(15 * hour, "0.00005"), (16 * hour, "0.00007")]),
    }
    ratio_pages = {
        "AAAUSDT": _ratio_history([16 * hour + 5 * 60_000 * index for index in range(13)]),
    }

    def fetch_funding(rest: str, market: str, symbol: str, limit: int) -> list[dict]:
        calls.append(("funding", symbol, limit))
        return funding_pages[symbol]

    def fetch_ratio(rest: str, market: str, symbol: str, period: str, limit: int) -> list[dict]:
        calls.append(("ratio", symbol, period, limit))
        return ratio_pages[symbol]

    lane = RestLane(
        rest_url="http://unused",
        market="linear",
        emit=emitted.append,
        stop=threading.Event(),
        funding_symbols=("BBBUSDT", "AAAUSDT"),
        ratio_symbols=("AAAUSDT",),
        fetch_funding=fetch_funding,
        fetch_ratio=fetch_ratio,
        clock_ns=lambda: 17 * hour * 1_000_000,
        sleep=slept.append,
        requests_per_second=4.0,
    )

    counts = lane.pass_once()

    # The first pass takes the newest settlement per symbol and the whole
    # hour of ratio buckets; every request is followed by the pace.
    assert counts == {"funding": 2, "account_ratio": 13, "failures": 0}
    assert calls == [
        ("funding", "BBBUSDT", FUNDING_HISTORY_LIMIT),
        ("funding", "AAAUSDT", FUNDING_HISTORY_LIMIT),
        ("ratio", "AAAUSDT", "5min", ACCOUNT_RATIO_LIMIT),
    ]
    assert slept == [0.25, 0.25, 0.25]
    funding = [row for row in emitted if row["kind"] == "funding_settlement"]
    assert [(row["symbol"], row["funding_time_ms"], row["funding_rate"]) for row in funding] == [
        ("BBBUSDT", 16 * hour, 0.00007),
        ("AAAUSDT", 16 * hour, -0.0002),
    ]
    ratios = [row for row in emitted if row["kind"] == "account_ratio"]
    assert [row["ts_ms"] for row in ratios] == [16 * hour + 5 * 60_000 * index for index in range(13)]
    assert ratios[-1]["buy_ratio"] == 0.5 and ratios[-1]["sell_ratio"] == 0.5
    assert all(row["local_receive_ts_ns"] == 17 * hour * 1_000_000 and row["venue"] == "bybit" for row in emitted)

    # The next pass sees the same pages plus one new settlement and one new
    # bucket: only those are written.
    funding_pages["AAAUSDT"] = _funding_history([(16 * hour, "-0.0002"), (24 * hour, "0.0003")])
    ratio_pages["AAAUSDT"] = _ratio_history([16 * hour + 5 * 60_000 * index for index in range(1, 14)])
    del emitted[:]
    counts = lane.pass_once()
    assert counts == {"funding": 1, "account_ratio": 1, "failures": 0}
    assert [(row["kind"], row.get("funding_time_ms") or row.get("ts_ms")) for row in emitted] == [
        ("funding_settlement", 24 * hour),
        ("account_ratio", 16 * hour + 5 * 60_000 * 13),
    ]
    assert lane.passes == 2


def test_the_rest_lane_counts_a_refused_symbol_and_stops_when_told(caplog: pytest.LogCaptureFixture) -> None:
    from market_tape.venues.bybit import RestLane

    emitted: list[dict] = []

    def fetch_funding(rest: str, market: str, symbol: str, limit: int) -> list[dict]:
        if symbol == "BADUSDT":
            raise RuntimeError("venue refused")
        return _funding_history([(3_600_000, "0.0001")])

    stop = threading.Event()
    lane = RestLane(
        rest_url="http://unused",
        market="linear",
        emit=emitted.append,
        stop=stop,
        funding_symbols=("BADUSDT", "OKUSDT"),
        fetch_funding=fetch_funding,
        clock_ns=lambda: 7_200_000_000_000,
        sleep=lambda seconds: None,
    )
    with caplog.at_level(logging.WARNING):
        counts = lane.pass_once()
    assert counts == {"funding": 1, "account_ratio": 0, "failures": 1}
    assert [row["symbol"] for row in emitted] == ["OKUSDT"]
    assert "1 of 2 symbols refused this pass, first BADUSDT: venue refused" in caplog.text

    # A stop set mid-walk ends the pass at the next symbol.
    stop.set()
    assert lane.pass_once() == {"funding": 0, "account_ratio": 0, "failures": 0}

    # The schedule is `offset` past each interval boundary.
    assert lane.seconds_until_next(3_600.0 * 5 + 10.0) == 170.0
    assert lane.seconds_until_next(3_600.0 * 5 + 180.0) == 3_600.0
    assert lane.seconds_until_next(3_600.0 * 5 + 200.0) == 3_580.0


def test_start_lanes_runs_one_thread_for_the_lane_feeds_and_none_otherwise() -> None:
    adapter = BybitAdapter(rest_url="http://unused")
    stop = threading.Event()
    stop.set()  # the thread finds it set and exits before any request
    assert adapter.start_lanes({"AGIUSDT": (parse_feed("trades"),)}, lambda row: None, stop) == []
    threads = adapter.start_lanes(
        {"AGIUSDT": (parse_feed("funding"), parse_feed("trades")), "BTCUSDT": (parse_feed("account_ratio"),)},
        lambda row: None,
        stop,
    )
    assert len(threads) == 1 and threads[0].name == "tape-lane-bybit-rest" and threads[0].daemon
    threads[0].join(5.0)
    assert not threads[0].is_alive()


def test_every_row_of_a_frame_carries_both_host_clocks_and_a_print_carries_its_messages_send_stamp() -> None:
    """The shard reads the wall clock and the monotonic clock together as a
    frame arrives; the adapter puts both on every row it makes of the frame,
    and a trade row carries the send stamp of the message beside the print's
    own time, so a print is a clock reading like a book row."""

    adapter = BybitAdapter()
    wall, mono = 1_800_000_000_010_000_000, 4_321_000_000
    book = adapter.normalize(json.dumps(book_message()), wall, mono)[0]
    assert (book["local_receive_ts_ns"], book["local_receive_mono_ns"]) == (wall, mono)
    trades = adapter.normalize(
        json.dumps(
            {
                "topic": "publicTrade.AGIUSDT",
                "ts": 1_800_000_000_009,
                "data": [
                    {"s": "AGIUSDT", "S": "Buy", "p": "0.0011", "v": "100", "i": "one", "T": 1_800_000_000_007},
                    {"s": "AGIUSDT", "S": "Sell", "p": "0.0010", "v": "80", "i": "two", "T": 1_800_000_000_008},
                ],
            }
        ),
        wall,
        mono,
    )
    assert [row["exchange_ts_ns"] for row in trades] == [1_800_000_000_007_000_000, 1_800_000_000_008_000_000]
    assert {row["exchange_system_ts_ns"] for row in trades} == {1_800_000_000_009_000_000}
    assert {(row["local_receive_ts_ns"], row["local_receive_mono_ns"]) for row in trades} == {(wall, mono)}
    ticker = adapter.normalize(
        json.dumps({"topic": "tickers.AGIUSDT", "type": "snapshot", "ts": 1_800_000_000_009, "data": {"symbol": "AGIUSDT", "lastPrice": "1"}}),
        wall,
        mono,
    )[0]
    liquidation = adapter.normalize(
        json.dumps(
            {"topic": "allLiquidation.AGIUSDT", "ts": 1_800_000_000_009, "data": [{"s": "AGIUSDT", "S": "Buy", "v": "1", "p": "1", "T": 1_800_000_000_009}]}
        ),
        wall,
        mono,
    )[0]
    assert ticker["local_receive_mono_ns"] == liquidation["local_receive_mono_ns"] == mono
    # Without a monotonic reading the rows say so with a zero, never a guess.
    assert adapter.normalize(json.dumps(book_message("delta", 11, 101)), wall)[0]["local_receive_mono_ns"] == 0
