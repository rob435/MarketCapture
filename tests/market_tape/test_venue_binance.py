"""The Binance USD-M adapter: stream names and paths, message shapes, REST tables, the open-interest lane.

The frames are the venue's own, recorded 2026-09-25 (`ps` and `st` included:
fields the adapter does not read must not disturb it)."""

from __future__ import annotations

import json
import logging
import threading
import urllib.request
from typing import Any, Callable

import pytest

from market_tape.config import CaptureConfig, ConfigError, StorageSettings, Tier, Universe, VenueSettings, parse_feed
from market_tape.schema import parse_row
from market_tape.venues import adapter_for, validate_config
from market_tape.venues.binance import MAX_STREAMS, STREAMS_PER_FRAME, BinanceAdapter

RECEIVED = 1_790_370_834_300_000_000
MONO = 2_711_837_864_676

BOOK_TICKER = (
    '{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":11659858833361,"s":"BTCUSDT","ps":"BTCUSDT",'
    '"b":"83680.00","B":"7.816","a":"83680.10","A":"1.566","T":1790370834266,"E":1790370834267,"st":1}}'
)
TRADE = (
    '{"stream":"btcusdt@trade","data":{"e":"trade","E":1790370834849,"T":1790370834848,"s":"BTCUSDT",'
    '"t":8121136411,"p":"83680.00","q":"0.001","X":"MARKET","m":true,"st":1}}'
)
RPI_TRADE = (
    '{"stream":"1000pepeusdt@trade","data":{"e":"trade","E":1790371723022,"T":1790371723021,"s":"1000PEPEUSDT",'
    '"t":2804511605,"p":"0.0044478","q":"10403","X":"RPI","m":false,"st":1}}'
)
NA_TRADE = (
    '{"stream":"dogeusdt@trade","data":{"e":"trade","E":1790371725749,"T":1790371725748,"s":"DOGEUSDT",'
    '"t":3488255295,"p":"0","q":"0","X":"NA","m":true,"st":1}}'
)


def frame(stream: str, data: Any) -> str:
    return json.dumps({"stream": stream, "data": data})


def feeds(*texts: str) -> tuple[Any, ...]:
    return tuple(parse_feed(text) for text in texts)


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


def fake_rest(monkeypatch: pytest.MonkeyPatch, reply: Callable[[str], Any]) -> list[str]:
    """Answer every REST call from `reply`; return the list of URLs called."""

    calls: list[str] = []

    def urlopen(request: Any, timeout: float | None = None) -> FakeResponse:
        calls.append(request.full_url)
        return FakeResponse(reply(request.full_url))

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return calls


# ------------------------------------------------------------ streams and paths


def test_market_and_defaults() -> None:
    adapter = adapter_for("binance", market="usdm")
    assert isinstance(adapter, BinanceAdapter)
    assert (adapter.name, adapter.market) == ("binance", "usdm")
    assert adapter.ws_url == "wss://fstream.binance.com"
    assert adapter.rest_url == "https://fapi.binance.com"
    assert adapter.max_topics_per_connection == 1024
    with pytest.raises(ConfigError):
        BinanceAdapter(market="spot")


def test_the_top_of_book_rides_the_url_a_live_reader_takes() -> None:
    """A live top-of-book reader takes `wss://fstream.binance.com/stream`; the
    tape that grades it must carry the same stream, not a sibling path's."""

    adapter = BinanceAdapter()
    public = adapter.topics("BTCUSDT", feeds("book:1", "trades"))
    assert public == ["btcusdt@bookTicker", "btcusdt@trade"]
    assert adapter.connection_url(public) == "wss://fstream.binance.com/stream"


def test_topics_name_one_stream_per_feed() -> None:
    adapter = BinanceAdapter()
    assert adapter.topics("BTCUSDT", feeds("book:1")) == ["btcusdt@bookTicker"]
    assert adapter.topics("BTCUSDT", feeds("book:5")) == ["btcusdt@depth5@100ms"]
    assert adapter.topics("BTCUSDT", feeds("book:20")) == ["btcusdt@depth20@100ms"]
    assert adapter.topics("BTCUSDT", feeds("trades")) == ["btcusdt@trade"]
    assert adapter.topics("BTCUSDT", feeds("kline:15m")) == ["btcusdt@kline_15m"]
    assert adapter.topics("BTCUSDT", feeds("open_interest:300")) == []
    assert adapter.topics("ETHUSDT", feeds("ticker")) == ["ethusdt@markPrice@1s", "ethusdt@ticker"]
    # One stream carries every symbol's liquidations; the recorder claims it once.
    assert adapter.topics("ETHUSDT", feeds("liquidations")) == adapter.topics("BTCUSDT", feeds("liquidations"))


def test_streams_route_by_path_and_one_connection_carries_one_path() -> None:
    # Measured 2026-09-25: a path-less or /public connection delivers only the
    # depth, bookTicker and trade streams; the rest flow on /market only.
    adapter = BinanceAdapter()
    public = ["btcusdt@depth20@100ms", "btcusdt@bookTicker", "btcusdt@trade"]
    market = ["btcusdt@aggTrade", "btcusdt@markPrice@1s", "btcusdt@ticker", "btcusdt@kline_1m", "!forceOrder@arr"]
    assert {adapter.connection_group(topic) for topic in public} == {"public"}
    assert {adapter.connection_group(topic) for topic in market} == {"market"}
    assert adapter.connection_url(public) == "wss://fstream.binance.com/stream"
    assert adapter.connection_url(market) == "wss://fstream.binance.com/market/stream"
    assert BinanceAdapter(ws_url="ws://127.0.0.1:9/").connection_url(market) == "ws://127.0.0.1:9/market/stream"
    with pytest.raises(ValueError, match="one path"):
        adapter.connection_url(["btcusdt@bookTicker", "btcusdt@markPrice@1s"])


def test_subscriptions_are_frames_on_the_open_socket_a_hundred_streams_each() -> None:
    adapter = BinanceAdapter()
    topics = [f"s{index}usdt@bookTicker" for index in range(250)]

    opened = [json.loads(text) for text in adapter.subscribe_messages(topics)]
    added = [json.loads(text) for text in adapter.add_messages(topics[:3])]
    removed = [json.loads(text) for text in adapter.remove_messages(topics[:2])]

    assert STREAMS_PER_FRAME == 100
    assert [message["method"] for message in opened] == ["SUBSCRIBE"] * 3
    assert [message["params"] for message in opened] == [topics[:100], topics[100:200], topics[200:]]
    assert added == [{"method": "SUBSCRIBE", "params": topics[:3], "id": 4}]
    assert removed == [{"method": "UNSUBSCRIBE", "params": topics[:2], "id": 5}]
    assert adapter.subscribe_messages([]) == []


def test_nothing_is_re_anchored_because_every_row_is_whole() -> None:
    adapter = BinanceAdapter()
    topics = adapter.topics("BTCUSDT", feeds("book:1", "book:20", "trades", "ticker", "liquidations", "kline:1m"))
    assert adapter.anchored_topics(topics) == []


def test_the_venue_refuses_what_it_cannot_record() -> None:
    adapter = BinanceAdapter()
    adapter.validate_feeds(feeds("book:1", "book:5", "book:10", "book:20", "trades", "ticker", "kline:1M", "open_interest:60"))
    for text in ("book:50", "book:1000", "kline:5s", "funding", "account_ratio"):
        with pytest.raises(ConfigError):
            adapter.validate_feeds(feeds(text))

    def config(venue: VenueSettings, per_connection: int) -> CaptureConfig:
        tier = Tier("t", feeds("trades"), Universe("listed", quote="USDT"))
        return CaptureConfig(venue=venue, storage=StorageSettings(), tiers=(tier,), topics_per_connection=per_connection)

    validate_config(adapter, config(VenueSettings("binance", "usdm"), MAX_STREAMS))
    with pytest.raises(ConfigError, match="at most 1024 topics a connection"):
        validate_config(adapter, config(VenueSettings("binance", "usdm"), MAX_STREAMS + 1))
    # Bybit's cap is on the subscription list's characters, not its topic count.
    bybit = adapter_for("bybit", market="linear")
    validate_config(bybit, config(VenueSettings("bybit", "linear"), 5_000))


# -------------------------------------------------------------------- books


def test_top_of_book_is_a_one_level_snapshot_stamped_by_the_venues_two_clocks() -> None:
    adapter = BinanceAdapter()
    (row,) = adapter.normalize(BOOK_TICKER, RECEIVED, MONO)
    assert row == {
        "kind": "orderbook_snapshot",
        "venue": "binance",
        "symbol": "BTCUSDT",
        "depth": 1,
        "local_receive_ts_ns": RECEIVED,
        "local_receive_mono_ns": MONO,
        "exchange_system_ts_ns": 1790370834267_000_000,
        "exchange_engine_ts_ns": 1790370834266_000_000,
        "bids": [["83680.00", "7.816"]],
        "asks": [["83680.10", "1.566"]],
        "update_id": 11659858833361,
        "previous_update_id": 0,
        "first_update_id": 0,
        "cross_sequence": 0,
        "previous_cross_sequence": 0,
        "restart_snapshot": False,
        "sequence_gap": False,
    }
    typed = parse_row(row)
    assert typed.snapshot and typed.bids == ((83680.0, 7.816),) and typed.asks == ((83680.1, 1.566),)
    assert adapter.normalize(BOOK_TICKER.encode(), RECEIVED)[0]["local_receive_mono_ns"] == 0


def test_partial_book_is_a_snapshot_at_its_own_depth() -> None:
    adapter = BinanceAdapter()
    raw = frame(
        "btcusdt@depth20@100ms",
        {
            "e": "depthUpdate",
            "E": 1571889248277,
            "T": 1571889248276,
            "s": "BTCUSDT",
            "U": 390497796,
            "u": 390497878,
            "pu": 390497794,
            "b": [["7403.89", "0.002"], ["7403.90", "3.906"]],
            "a": [["7405.96", "3.340"]],
        },
    )
    (row,) = adapter.normalize(raw, RECEIVED, MONO)
    assert (row["kind"], row["depth"], row["sequence_gap"]) == ("orderbook_snapshot", 20, False)
    assert (row["update_id"], row["previous_update_id"], row["first_update_id"]) == (390497878, 390497794, 390497796)
    assert row["bids"] == [["7403.89", "0.002"], ["7403.90", "3.906"]]
    assert parse_row(row).depth == 20


# ------------------------------------------------------------------- trades


def test_a_print_is_one_trade_row_whose_side_is_the_taker() -> None:
    adapter = BinanceAdapter()
    (row,) = adapter.normalize(TRADE, RECEIVED, MONO)
    assert row == {
        "kind": "public_trade",
        "venue": "binance",
        "symbol": "BTCUSDT",
        "local_receive_ts_ns": RECEIVED,
        "local_receive_mono_ns": MONO,
        "exchange_system_ts_ns": 1790370834849_000_000,
        "exchange_ts_ns": 1790370834848_000_000,
        "trade_id": "8121136411",
        "price": 83680.0,
        "qty": 0.001,
        # `m`: the buyer was the maker, so the taker sold.
        "side": "Sell",
    }
    assert parse_row(row).side == "Sell"
    (rpi,) = adapter.normalize(RPI_TRADE, RECEIVED)
    assert (rpi["symbol"], rpi["side"], rpi["qty"], rpi["trade_id"]) == ("1000PEPEUSDT", "Buy", 10403.0, "2804511605")


def test_a_trade_type_that_is_not_a_print_on_the_book_makes_no_row_and_is_logged_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = BinanceAdapter()
    with caplog.at_level(logging.INFO):
        assert adapter.normalize(NA_TRADE, RECEIVED) == []
        assert adapter.normalize(NA_TRADE, RECEIVED) == []
        adl = json.loads(NA_TRADE)
        adl["data"]["X"] = "ADL"
        assert adapter.normalize(json.dumps(adl), RECEIVED) == []
    said = [record.getMessage() for record in caplog.records if "not a print on the book" in record.getMessage()]
    assert said == [
        "binance @trade type 'NA' is not a print on the book; its rows are dropped",
        "binance @trade type 'ADL' is not a print on the book; its rows are dropped",
    ]
    assert adapter.dropped_trade_types == {"NA", "ADL"}


# ------------------------------------------------------------------ tickers


def test_mark_price_frame_carries_mark_index_and_funding() -> None:
    adapter = BinanceAdapter()
    raw = frame(
        "btcusdt@markPrice@1s",
        {"e": "markPriceUpdate", "E": 1562305380000, "s": "BTCUSDT", "p": "11794.15000000", "i": "11784.62659091",
         "P": "11784.25641265", "r": "0.00038167", "T": 1562306400000},
    )
    (row,) = adapter.normalize(raw, RECEIVED, MONO)
    assert row["kind"] == "ticker" and row["message_type"] == "delta" and row["local_receive_mono_ns"] == MONO
    assert row["values"] == {
        "mark_price": 11794.15,
        "index_price": 11784.62659091,
        "funding_rate": 0.00038167,
        "next_funding_time_ms": 1562306400000,
    }
    assert isinstance(parse_row(row).values["next_funding_time_ms"], int)


def test_day_ticker_carries_last_price_the_two_volumes_and_the_change_as_a_fraction() -> None:
    adapter = BinanceAdapter()
    raw = frame(
        "btcusdt@ticker",
        {"e": "24hrTicker", "E": 1700000000000, "s": "BTCUSDT", "c": "60000", "q": "5e9", "v": "80000", "P": "-2.5"},
    )
    (row,) = adapter.normalize(raw, RECEIVED)
    assert row["values"] == {"last_price": 60000.0, "turnover_24h": 5e9, "volume_24h": 80000.0, "price_change_24h_pct": -0.025}


# ------------------------------------------------------------- liquidations


def force_order_frame(order_side: str) -> str:
    return frame(
        "!forceOrder@arr",
        {"e": "forceOrder", "E": 1568014460893,
         "o": {"s": "BTCUSDT", "S": order_side, "q": "0.014", "p": "9910", "X": "FILLED", "T": 1568014460890}},
    )


def test_liquidation_position_side_is_the_other_side_of_the_order() -> None:
    adapter = BinanceAdapter()
    (row,) = adapter.normalize(force_order_frame("SELL"), RECEIVED, MONO)
    assert row == {
        "kind": "liquidation",
        "venue": "binance",
        "symbol": "BTCUSDT",
        "local_receive_ts_ns": RECEIVED,
        "local_receive_mono_ns": MONO,
        "exchange_system_ts_ns": 1568014460893_000_000,
        "exchange_ts_ns": 1568014460890_000_000,
        "position_side": "Buy",
        "qty": 0.014,
        "bankruptcy_price": 9910.0,
    }
    assert adapter.normalize(force_order_frame("BUY"), RECEIVED)[0]["position_side"] == "Sell"


def test_kline_frame_is_one_candle() -> None:
    adapter = BinanceAdapter()
    raw = frame(
        "btcusdt@kline_1m",
        {"e": "kline", "E": 1638747660000, "s": "BTCUSDT",
         "k": {"t": 1638747660000, "T": 1638747719999, "i": "1m", "o": "0.0010", "c": "0.0020", "h": "0.0025",
               "l": "0.0015", "v": "1000", "x": True, "q": "1.0000"}},
    )
    (row,) = adapter.normalize(raw, RECEIVED, MONO)
    assert (row["interval"], row["open"], row["close"], row["turnover"], row["confirmed"]) == ("1m", 0.001, 0.002, 1.0, True)
    assert row["local_receive_mono_ns"] == MONO
    assert parse_row(row).confirmed


# ----------------------------------------------------------- control frames


def test_subscribe_replies_make_no_rows_and_a_refusal_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    adapter = BinanceAdapter()
    with caplog.at_level(logging.WARNING):
        assert adapter.normalize('{"result":null,"id":1}', RECEIVED) == []
        assert caplog.text == ""
        assert adapter.normalize('{"error":{"code":2,"msg":"Invalid request: unknown stream"},"id":2}', RECEIVED) == []
        assert adapter.normalize('{"code":-1121,"msg":"Invalid symbol.","id":3}', RECEIVED) == []
    assert "refused subscription request 2" in caplog.text and "Invalid symbol." in caplog.text
    assert caplog.text.count("refused subscription request") == 2
    # Not market data, not a reply.
    assert adapter.normalize(json.dumps([1, 2]), RECEIVED) == []
    assert adapter.normalize(frame("btcusdt@unknown", {"s": "BTCUSDT"}), RECEIVED) == []
    assert adapter.normalize(frame("btcusdt@bookTicker", {"u": 1}), RECEIVED) == []


# ------------------------------------------------------------------- tables


INSTRUMENTS = [
    {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
    {"symbol": "ETHUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
    {"symbol": "BTCUSD_240329", "contractType": "CURRENT_QUARTER", "status": "TRADING", "quoteAsset": "USD", "marginAsset": "BTC"},
    {"symbol": "SOLUSDT", "contractType": "PERPETUAL", "status": "SETTLING", "quoteAsset": "USDT", "marginAsset": "USDT"},
    {"symbol": "ADAUSDC", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDC", "marginAsset": "USDC"},
    {"symbol": "BTCUSD", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USD", "marginAsset": "BTC"},
    # Binance files stocks and commodities under their own contract type.
    {"symbol": "NVDAUSDT", "contractType": "TRADIFI_PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
    {"symbol": "XAUUSDT", "contractType": "TRADIFI_PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
    {"symbol": "BTCUSDT_260327", "contractType": "CURRENT_QUARTER", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
]


def test_listed_keeps_the_trading_crypto_perpetuals_of_the_quote() -> None:
    adapter = BinanceAdapter()
    assert adapter.listed_symbols(INSTRUMENTS, quote="USDT") == ["BTCUSDT", "ETHUSDT"]
    assert adapter.listed_symbols(INSTRUMENTS, quote="USDC") == ["ADAUSDC"]
    assert adapter.listed_symbols(INSTRUMENTS, quote=None) == ["ADAUSDC", "BTCUSD", "BTCUSDT", "ETHUSDT"]
    assert adapter.excluded_listed(INSTRUMENTS, quote="USDT") == {"CURRENT_QUARTER": 1, "TRADIFI_PERPETUAL": 2}


def test_turnovers_and_funding_rates_read_the_merged_ticker_table() -> None:
    tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "9000000", "lastFundingRate": "-0.0012"},
        {"symbol": "ETHUSDT", "quoteVolume": "n/a", "lastFundingRate": "0.0001"},
        {"symbol": "XRPUSDT", "lastFundingRate": ""},
    ]
    adapter = BinanceAdapter()
    assert adapter.turnovers(tickers) == {"BTCUSDT": 9_000_000.0}
    assert adapter.funding_rates(tickers) == {"BTCUSDT": -0.0012, "ETHUSDT": 0.0001}


def test_fetch_tables_merges_the_three_ticker_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    replies: dict[str, Any] = {
        "/fapi/v1/exchangeInfo": {"symbols": INSTRUMENTS},
        "/fapi/v1/premiumIndex": [
            {"symbol": "BTCUSDT", "markPrice": "60000.1", "indexPrice": "60001.2", "lastFundingRate": "-0.0012", "nextFundingTime": 1700000000000},
        ],
        "/fapi/v1/ticker/24hr": [{"symbol": "BTCUSDT", "lastPrice": "60000.5", "quoteVolume": "9000000", "volume": "150"}],
        "/fapi/v1/ticker/bookTicker": [{"symbol": "ETHUSDT", "bidPrice": "3000.0", "bidQty": "9", "askPrice": "3000.1", "askQty": "8"}],
    }
    calls = fake_rest(monkeypatch, lambda url: replies[url.partition("fapi.binance.com")[2]])
    tables = BinanceAdapter().fetch_tables()
    assert [url.partition("fapi.binance.com")[2] for url in calls] == list(replies)
    assert tables["instruments"] == INSTRUMENTS
    assert tables["tickers"] == [
        {"symbol": "BTCUSDT", "markPrice": "60000.1", "indexPrice": "60001.2", "lastFundingRate": "-0.0012",
         "nextFundingTime": 1700000000000, "lastPrice": "60000.5", "quoteVolume": "9000000", "volume": "150"},
        {"symbol": "ETHUSDT", "bidPrice": "3000.0", "bidQty": "9", "askPrice": "3000.1", "askQty": "8"},
    ]


def test_a_refused_request_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_rest(monkeypatch, lambda url: {"code": 0, "msg": "Service unavailable from a restricted location"})
    with pytest.raises(RuntimeError, match="restricted location"):
        BinanceAdapter().fetch_tables()


# ------------------------------------------------------ the open interest lane


def test_open_interest_row_is_priced_by_the_last_mark(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_rest(monkeypatch, lambda url: {"symbol": "BTCUSDT", "openInterest": "10659.509", "time": 1589437530011})
    adapter = BinanceAdapter()
    row = adapter.open_interest_row("BTCUSDT")
    assert row["values"] == {"open_interest": 10659.509}
    assert (row["message_type"], row["exchange_system_ts_ns"]) == ("poll", 1589437530011_000_000)

    adapter.normalize(frame("btcusdt@markPrice@1s", {"s": "BTCUSDT", "E": 1, "p": "100"}), RECEIVED)
    priced = adapter.open_interest_row("BTCUSDT")
    assert priced["values"] == {"open_interest": 10659.509, "open_interest_value": 1065950.9}


def test_lanes_run_only_for_the_symbols_that_ask_for_a_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = fake_rest(monkeypatch, lambda url: {"symbol": "X", "openInterest": "1", "time": 1})
    adapter = BinanceAdapter()
    stop = threading.Event()
    stop.set()
    lanes = adapter.start_lanes(
        {"BTCUSDT": feeds("open_interest:300", "trades"), "ETHUSDT": feeds("open_interest:60"), "SOLUSDT": feeds("trades")},
        lambda row: None,
        stop,
    )
    for lane in lanes:
        lane.join(5.0)
    assert [lane.name for lane in lanes] == ["tape-binance-oi-60s", "tape-binance-oi-300s"]
    assert not any(lane.is_alive() for lane in lanes)
    assert calls == []
    assert adapter.start_lanes({"BTCUSDT": feeds("book:1", "trades")}, lambda row: None, stop) == []
