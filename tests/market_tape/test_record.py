"""The recorder's pure parts: universes, live promotion, topic planning, shards, bytes, budget, status;
and the shipped configs through the real `market_tape check`."""

from __future__ import annotations

import dataclasses
import errno
import hashlib
import json
import os
import logging
import queue
import random
import socket as sockets
import subprocess
import sys
import threading
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from market_tape.config import (
    BudgetSettings,
    CaptureConfig,
    ConfigError,
    Feed,
    StorageSettings,
    Tier,
    Universe,
    VenueSettings,
    parse_config,
)
from market_tape import reader, record, storage
from market_tape.record import BudgetController, ByteMeter, Recorder, Shard, shard_topics
from market_tape.schema import SCHEMA_VERSION
from market_tape.venues.bybit import BybitAdapter


HOUR_NS = 3_600 * 1_000_000_000
DAY_NS = 24 * HOUR_NS
BASE_NS = 1_800_000_000 * 1_000_000_000  # 2027-01-15T08:00:00Z

DEEP_FEEDS = (Feed("book", "50"), Feed("book", "1"), Feed("trades"), Feed("ticker"), Feed("liquidations"))
WIDE_FEEDS = (Feed("ticker"), Feed("liquidations"))


def instrument(symbol: str, quote: str = "USDT", symbol_type: str = "") -> dict[str, Any]:
    return {
        "symbol": symbol,
        "status": "Trading",
        "quoteCoin": quote,
        "settleCoin": quote,
        "contractType": "LinearPerpetual",
        "symbolType": symbol_type,
    }


def build(
    tmp_path: Path,
    *tiers: Tier,
    cadence: str = "day",
    per_connection: int = 150,
    budget: BudgetSettings | None = None,
    reanchor: bool = True,
) -> Recorder:
    config = CaptureConfig(
        venue=VenueSettings("bybit", "linear"),
        storage=StorageSettings(root=tmp_path, queue_frames=16, status_interval_seconds=30.0),
        tiers=tiers,
        topics_per_connection=per_connection,
        reanchor_each_hour=reanchor,
        snapshot_cadence=cadence,
        budget=budget or BudgetSettings(),
        source_path=Path("deploy/capture/bybit-linear.toml"),
    )
    return Recorder(config, adapter=BybitAdapter(rest_url="http://unused"))


# ---------------------------------------------------------------- building


def test_a_recorder_refuses_a_root_another_recorder_holds(tmp_path: Path) -> None:
    """`Restart=always`, so a second recorder on one root reads as a crash loop
    rather than two compressors racing for the same raw segment."""

    tier = Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",)))
    recorder = build(tmp_path, tier)
    held = storage.lock_root(recorder.root)
    assert held is not None
    try:
        with pytest.raises(RuntimeError) as refused:
            recorder.run()
    finally:
        storage.unlock_root(held)

    assert str(recorder.root) in str(refused.value)
    assert not recorder.compressor.thread.is_alive()


def test_a_symbol_file_that_names_nothing_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "symbols.txt"
    path.write_text("# nothing yet\n", encoding="utf-8")
    tier = Tier("deep", (Feed("trades"),), Universe("file", path=path))

    with pytest.raises(ConfigError, match="names no symbols"):
        build(tmp_path, tier)

    path.write_text("btcusdt\nETHUSDT, btcusdt\n", encoding="utf-8")
    recorder = build(tmp_path, tier)
    assert recorder.static_symbols["deep"] == ("BTCUSDT", "ETHUSDT")


# ---------------------------------------------------------------- universes


def test_the_listed_universe_takes_the_quote_and_a_dynamic_tier_without_tables_is_empty(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT")))
    tables = {"instruments": [instrument("BTCUSDT"), instrument("ETHPERP", "USDC")], "tickers": []}

    assert recorder.resolve_tiers(BASE_NS, tables) == {"wide": ["BTCUSDT"]}
    recorder.tables = None
    assert recorder.resolve_tiers(BASE_NS, None) == {"wide": []}


def test_a_stock_perpetual_enters_no_tier_however_it_ranks_or_funds(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The venue lists stocks, ETFs and commodities as perpetuals. The tape is
    the crypto domain, so the capture subscribes nothing for them: not the
    ranked tiers, not the funding tiers, not the wide ticker."""

    recorder = build(
        tmp_path,
        Tier("core", DEEP_FEEDS, Universe("top_turnover", top=2, quote="USDT")),
        Tier(
            "crowded",
            (Feed("book", "50"),),
            Universe("funding_below", threshold_bp=8.0, quote="USDT", exclude_tiers=("core",)),
        ),
        Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT", exclude_tiers=("core", "crowded"))),
    )
    tables = {
        "instruments": [
            instrument("BTCUSDT"),
            instrument("MYXUSDT", symbol_type="innovation"),
            instrument("SKHYNIXUSDT", symbol_type="stock"),
            instrument("SOXLUSDT", symbol_type="ETF"),
            instrument("XAUUSDT", symbol_type="commodity"),
        ],
        "tickers": [
            {"symbol": "SKHYNIXUSDT", "turnover24h": "9000000", "fundingRate": "-0.0050"},
            {"symbol": "XAUUSDT", "turnover24h": "8000000", "fundingRate": "-0.0050"},
            {"symbol": "BTCUSDT", "turnover24h": "7000", "fundingRate": "0.0001"},
            {"symbol": "MYXUSDT", "turnover24h": "10", "fundingRate": "-0.0050"},
            {"symbol": "SOXLUSDT", "turnover24h": "5", "fundingRate": "0.0001"},
        ],
    }

    resolved = recorder.resolve_tiers(BASE_NS, tables)

    # The stock and the commodity outrank everything and fund at -50 bp; the
    # crypto names take their places.
    assert resolved == {"core": ["BTCUSDT", "MYXUSDT"], "crowded": [], "wide": []}
    topics, _ = recorder.plan_topics(resolved)
    assert not any("SKHYNIX" in t or "SOXL" in t or "XAU" in t for tier in topics.values() for t in tier)

    with caplog.at_level(logging.INFO):
        recorder._log_listed(tables)
    assert "2 USDT perpetuals in the domain; outside it ETF=1 commodity=1 stock=1" in caplog.text


def test_a_ranked_member_keeps_its_place_for_sticky_hours_after_it_last_ranked_inside_top(tmp_path: Path) -> None:
    """A name that ranked inside the top for a moment can sit far below it a
    few days later, while a position taken on it is still open. The rank
    hysteresis alone drops its book mid-hold; the time floor keeps it."""

    recorder = build(
        tmp_path,
        Tier(
            "core", (Feed("book", "50"),), Universe("top_turnover", top=1, leave_top=2, sticky_hours=4.0, quote="USDT")
        ),
    )
    names = ("AUSDT", "BUSDT", "CUSDT", "PUMPUSDT")
    recorder.tables = {"instruments": [instrument(name) for name in names], "tickers": []}
    for name, turnover in zip(names, (300.0, 200.0, 100.0, 9000.0)):
        recorder.live.observe(name, {"turnover_24h": turnover}, BASE_NS)
    assert recorder.resolve_tiers(BASE_NS + 1)["core"] == ["PUMPUSDT"]

    # The pump fades to rank 4, past leave_top. Rank alone would drop it now.
    recorder.live.observe("PUMPUSDT", {"turnover_24h": 1.0}, BASE_NS + 2)
    assert recorder.resolve_tiers(BASE_NS + 3)["core"] == ["AUSDT", "PUMPUSDT"]
    assert recorder.resolve_tiers(BASE_NS + 4 * HOUR_NS)["core"] == ["AUSDT", "PUMPUSDT"]
    # Four hours after it last ranked inside the top, it goes.
    assert recorder.resolve_tiers(BASE_NS + 1 + 4 * HOUR_NS)["core"] == ["AUSDT"]

    # Without the floor the rank hysteresis alone decides: the default is off.
    bare = build(
        tmp_path / "bare",
        Tier("core", (Feed("book", "50"),), Universe("top_turnover", top=1, leave_top=2, quote="USDT")),
    )
    bare.tables = recorder.tables
    for name, turnover in zip(names, (300.0, 200.0, 100.0, 9000.0)):
        bare.live.observe(name, {"turnover_24h": turnover}, BASE_NS)
    bare.resolve_tiers(BASE_NS + 1)
    bare.live.observe("PUMPUSDT", {"turnover_24h": 1.0}, BASE_NS + 2)
    assert bare.resolve_tiers(BASE_NS + 3)["core"] == ["AUSDT"]


def test_top_turnover_takes_the_ranked_head_of_the_listed_names(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("busy", (Feed("trades"),), Universe("top_turnover", top=2, quote="USDT")))
    tables = {
        "instruments": [instrument("BTCUSDT"), instrument("ETHUSDT"), instrument("AGIUSDT")],
        "tickers": [
            {"symbol": "AGIUSDT", "turnover24h": "10"},
            {"symbol": "BTCUSDT", "turnover24h": "9000"},
            {"symbol": "ETHUSDT", "turnover24h": "500"},
            {"symbol": "SOLUSDT", "turnover24h": "1000000"},
        ],
    }

    # SOL outranks everything but is not in the instrument table, so it is not listed.
    assert recorder.resolve_tiers(BASE_NS, tables) == {"busy": ["BTCUSDT", "ETHUSDT"]}


def test_top_turnover_follows_the_ticker_live_and_leaves_only_below_the_wider_rank(tmp_path: Path) -> None:
    recorder = build(
        tmp_path, Tier("core", (Feed("trades"),), Universe("top_turnover", top=2, leave_top=3, quote="USDT"))
    )
    names = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]
    tables = {
        "instruments": [instrument(name) for name in names],
        "tickers": [
            {"symbol": name, "turnover24h": str(turnover)} for name, turnover in zip(names, (400, 300, 200, 100))
        ],
    }
    assert recorder.resolve_tiers(BASE_NS, tables)["core"] == ["AUSDT", "BUSDT"]

    # D's turnover explodes on the ticker: it enters at once, B slips to rank 3 and stays.
    recorder.live.observe("DUSDT", {"turnover_24h": 350.0}, BASE_NS + 1)
    assert recorder.resolve_tiers(BASE_NS + 2)["core"] == ["AUSDT", "BUSDT", "DUSDT"]

    # C overtakes both: C enters at rank 2, D holds rank 3, B is rank 4, past leave_top, and goes.
    recorder.live.observe("CUSDT", {"turnover_24h": 360.0}, BASE_NS + 3)
    assert recorder.resolve_tiers(BASE_NS + 4)["core"] == ["AUSDT", "CUSDT", "DUSDT"]


def test_a_live_tier_without_an_instrument_table_still_honours_its_quote(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("busy", (Feed("trades"),), Universe("top_turnover", top=5, quote="USDT")))
    # Cold start: no instrument table has landed, so the ticker stream is the only
    # universe there is, and it carries every symbol the venue streams -- other
    # quotes and the venue's own non-perpetual naming included.
    for symbol, turnover in (
        ("BTCUSDT", 900.0),
        ("WLDUSDC", 800.0),
        ("ADAUSD_PERP", 700.0),
        ("ETHUSDT", 600.0),
    ):
        recorder.live.observe(symbol, {"turnover_24h": turnover}, BASE_NS)

    assert recorder.resolve_tiers(BASE_NS + 1)["busy"] == ["BTCUSDT", "ETHUSDT"]


def test_a_crowded_name_keeps_its_tier_for_its_sticky_hours_and_a_delisted_one_drops_at_once(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))),
        Tier(
            "crowded",
            (Feed("book", "50"),),
            Universe("funding_below", threshold_bp=10.0, sticky_hours=48.0, quote="USDT", exclude_tiers=("deep",)),
        ),
    )
    instruments = [instrument(symbol) for symbol in ("BTCUSDT", "AGIUSDT", "SOMIUSDT")]
    days = [
        [{"symbol": "AGIUSDT", "fundingRate": "-0.0020"}, {"symbol": "SOMIUSDT", "fundingRate": "0.0001"}],
        [{"symbol": "AGIUSDT", "fundingRate": "0.0001"}, {"symbol": "SOMIUSDT", "fundingRate": "-0.0030"}],
        [{"symbol": "AGIUSDT", "fundingRate": "0.0001"}, {"symbol": "SOMIUSDT", "fundingRate": "0.0001"}],
    ]

    first = recorder.resolve_tiers(BASE_NS, {"instruments": instruments, "tickers": days[0]})
    assert first["crowded"] == ["AGIUSDT"]

    # Day two: SOMI qualifies, AGI recovered but is inside its 48 hours.
    second = recorder.resolve_tiers(BASE_NS + DAY_NS, {"instruments": instruments, "tickers": days[1]})
    assert second["crowded"] == ["AGIUSDT", "SOMIUSDT"]

    # Day three: AGI's 48 hours are up; SOMI is inside its own, but delisted, so it drops.
    instruments.pop()
    third = recorder.resolve_tiers(BASE_NS + 2 * DAY_NS, {"instruments": instruments, "tickers": days[2]})
    assert third["crowded"] == []


def test_a_crowded_name_at_exactly_the_threshold_qualifies_and_a_shallower_one_does_not(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("crowded", (Feed("book", "50"),), Universe("funding_below", threshold_bp=10.0, quote="USDT")),
    )
    tables = {
        "instruments": [instrument(symbol) for symbol in ("AGIUSDT", "SOMIUSDT", "DOGEUSDT", "BADUSDT", "NEWUSDT")],
        "tickers": [
            {"symbol": "AGIUSDT", "fundingRate": "-0.0012"},
            {"symbol": "SOMIUSDT", "fundingRate": "-0.0010"},
            {"symbol": "DOGEUSDT", "fundingRate": "-0.0009"},
            {"symbol": "BADUSDT", "fundingRate": "n/a"},
            {"symbol": "NEWUSDT"},
        ],
    }

    assert recorder.resolve_tiers(BASE_NS, tables)["crowded"] == ["AGIUSDT", "SOMIUSDT"]


def test_a_funding_collapse_on_the_ticker_promotes_within_the_tick_and_expires_after_sticky_hours(
    tmp_path: Path,
) -> None:
    recorder = build(
        tmp_path,
        Tier(
            "crowded",
            (Feed("book", "50"),),
            Universe("funding_below", threshold_bp=8.0, sticky_hours=2.0, quote="USDT"),
        ),
    )
    tables = {
        "instruments": [instrument("AGIUSDT"), instrument("BTCUSDT")],
        "tickers": [{"symbol": "AGIUSDT", "fundingRate": "0.0001"}],
    }
    assert recorder.resolve_tiers(BASE_NS, tables)["crowded"] == []

    # 14:00: the ticker shows -12 bp; the next tick promotes.
    recorder.live.observe("AGIUSDT", {"funding_rate": -0.0012, "mark_price": 0.42}, BASE_NS + 6 * HOUR_NS)
    assert recorder.resolve_tiers(BASE_NS + 6 * HOUR_NS + 30 * 10**9)["crowded"] == ["AGIUSDT"]

    # The rate recovers; the name stays for its two sticky hours, then goes.
    recorder.live.observe("AGIUSDT", {"funding_rate": 0.0001}, BASE_NS + 7 * HOUR_NS)
    assert recorder.resolve_tiers(BASE_NS + 7 * HOUR_NS)["crowded"] == ["AGIUSDT"]
    assert recorder.resolve_tiers(BASE_NS + 8 * HOUR_NS + 30 * 10**9)["crowded"] == []

    # A ticker for a name the instrument table does not list never promotes it.
    recorder.live.observe("GHOSTUSDT", {"funding_rate": -0.0050}, BASE_NS + 9 * HOUR_NS)
    assert recorder.resolve_tiers(BASE_NS + 9 * HOUR_NS)["crowded"] == []


def test_a_turnover_surge_is_measured_against_the_last_snapshot(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("surging", (Feed("book", "50"),), Universe("turnover_surge", ratio=3.0, sticky_hours=1.0, quote="USDT")),
    )
    tables = {
        "instruments": [instrument("HNTUSDT"), instrument("BTCUSDT")],
        "tickers": [{"symbol": "HNTUSDT", "turnover24h": "1000000"}, {"symbol": "BTCUSDT", "turnover24h": "9e9"}],
    }
    assert recorder.resolve_tiers(BASE_NS, tables)["surging"] == []

    recorder.live.observe("HNTUSDT", {"turnover_24h": 2_999_999.0}, BASE_NS + 1)
    assert recorder.resolve_tiers(BASE_NS + 2)["surging"] == []
    recorder.live.observe("HNTUSDT", {"turnover_24h": 3_000_000.0}, BASE_NS + 3)
    assert recorder.resolve_tiers(BASE_NS + 4)["surging"] == ["HNTUSDT"]

    # The next snapshot raises the baseline to the new level; the surge is over, the name lingers an hour.
    recorder.resolve_tiers(
        BASE_NS + 5,
        {"instruments": tables["instruments"], "tickers": [{"symbol": "HNTUSDT", "turnover24h": "3000000"}]},
    )
    assert recorder.resolve_tiers(BASE_NS + 6)["surging"] == ["HNTUSDT"]
    assert recorder.resolve_tiers(BASE_NS + 4 + HOUR_NS)["surging"] == []


def test_a_price_move_promotes_either_direction(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("movers", (Feed("book", "50"),), Universe("price_move", pct=0.15, sticky_hours=1.0, quote="USDT")),
    )
    recorder.tables = {
        "instruments": [instrument("UPUSDT"), instrument("DOWNUSDT"), instrument("FLATUSDT")],
        "tickers": [],
    }
    recorder.live.observe("UPUSDT", {"price_change_24h_pct": 0.151}, BASE_NS)
    recorder.live.observe("DOWNUSDT", {"price_change_24h_pct": -0.20}, BASE_NS)
    recorder.live.observe("FLATUSDT", {"price_change_24h_pct": 0.149}, BASE_NS)

    assert recorder.resolve_tiers(BASE_NS + 1)["movers"] == ["DOWNUSDT", "UPUSDT"]


# ------------------------------------------------------------------- topics


def test_a_topic_an_earlier_tier_claimed_is_not_subscribed_twice(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("book", "50"), Feed("trades")), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("wide", (Feed("book", "50"),), Universe("listed", quote="USDT")),
    )
    tables = {"instruments": [instrument("BTCUSDT"), instrument("ETHUSDT")], "tickers": []}

    resolved = recorder.resolve_tiers(BASE_NS, tables)
    topics, feeds_by_symbol = recorder.plan_topics(resolved)

    assert resolved == {"deep": ["BTCUSDT"], "wide": ["BTCUSDT", "ETHUSDT"]}
    assert topics == {
        "deep": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"],
        "wide": ["orderbook.50.ETHUSDT"],
    }
    assert [feed.text for feed in feeds_by_symbol["BTCUSDT"]] == ["book:50", "trades"]
    assert [feed.text for feed in feeds_by_symbol["ETHUSDT"]] == ["book:50"]


def test_a_shed_pair_leaves_its_topics_out_of_the_plan(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("book", "50"), Feed("trades")), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("wide", (Feed("book", "1"), Feed("ticker")), Universe("listed", quote="USDT", exclude_tiers=("deep",))),
    )
    tables = {"instruments": [instrument("BTCUSDT"), instrument("ETHUSDT")], "tickers": []}
    resolved = recorder.resolve_tiers(BASE_NS, tables)

    topics, feeds = recorder.plan_topics(resolved, shed=[("wide", "book:1")])

    assert topics == {"deep": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"], "wide": ["tickers.ETHUSDT"]}
    assert [feed.text for feed in feeds["ETHUSDT"]] == ["ticker"]


# ------------------------------------------------------------------- shards


class FakeSocket:
    """A shard's link as the shard uses it: text out, a close asked for."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_text(self, text: str) -> None:
        self.sent.append(text)

    def request_close(self) -> None:
        return None


def unstarted_shard(recorder: Recorder, tier: str, topics: list[str], index: int = 0) -> Shard:
    return Shard(
        index=index,
        tier=tier,
        topics=list(topics),
        adapter=recorder.adapter,
        reader=recorder.reader,
        on_frame=recorder._on_frame,
        on_overrun=recorder._on_overrun,
    )


def test_a_shard_connects_to_its_url_offering_no_compression_and_takes_a_frame_of_any_size(tmp_path: Path) -> None:
    # Pure-Python inflate capped a shard at 143 frames/s on the host, and the
    # library's 1 MiB message default would refuse a large book snapshot.
    from websockets.sync.server import serve

    big = b'{"topic":"publicTrade.BTCUSDT","data":[],"pad":"' + b"x" * (3 * 1024 * 1024) + b'"}'
    seen: dict[str, Any] = {}

    def venue(connection: Any) -> None:
        seen["path"] = connection.request.path
        seen["extensions"] = connection.request.headers.get("Sec-WebSocket-Extensions")
        seen["subscribe"] = json.loads(connection.recv())
        connection.send(big)
        connection.close()

    with serve(venue, "127.0.0.1", 0) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.socket.getsockname()[1]
        config = CaptureConfig(
            venue=VenueSettings("bybit", "linear"),
            storage=StorageSettings(root=tmp_path),
            tiers=(Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),),
        )
        adapter = BybitAdapter(ws_url=f"ws://127.0.0.1:{port}/v5/public/linear", rest_url="http://unused")
        recorder = Recorder(config, adapter=adapter)
        recorder.reader.start()
        shard = unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT"])
        try:
            shard._connect_once()
        finally:
            recorder.reader.close()
            server.shutdown()

    assert seen["path"] == "/v5/public/linear"
    assert seen["extensions"] is None, "the recorder offered permessage-deflate"
    assert seen["subscribe"] == {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}
    assert [item[reader.EVENT.size :] for item in recorder.frames.get_batch(max_items=10, timeout=0.1)] == [big]
    assert recorder.received_frames == 1
    assert shard.socket is None, "the link is dropped once the venue ends it"


def test_a_live_shard_changes_its_subscription_in_place(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    shard = unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT", "publicTrade.ETHUSDT"])
    socket = FakeSocket()
    shard.socket = socket  # type: ignore[assignment]
    shard.connected = True

    added, removed = shard.update(["publicTrade.BTCUSDT", "publicTrade.AGIUSDT"])

    assert (added, removed) == (["publicTrade.AGIUSDT"], ["publicTrade.ETHUSDT"])
    assert shard.topics == ["publicTrade.BTCUSDT", "publicTrade.AGIUSDT"]
    assert [json.loads(text) for text in socket.sent] == [
        {"op": "unsubscribe", "args": ["publicTrade.ETHUSDT"]},
        {"op": "subscribe", "args": ["publicTrade.AGIUSDT"]},
    ]
    # Nothing to change sends nothing.
    assert shard.update(["publicTrade.BTCUSDT", "publicTrade.AGIUSDT"]) == ([], [])
    assert len(socket.sent) == 2


def test_re_anchoring_drops_and_retakes_the_book_and_ticker_topics_in_chunks(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    books = [f"orderbook.50.SYM{index}USDT" for index in range(record.REANCHOR_CHUNK + 2)]
    # The ticker is anchored with the books: its first message after a
    # subscribe is the whole ticker, every later one only what changed. The
    # prints and liquidations are not: each of their rows stands alone.
    topics = [*books[:4], "publicTrade.BTCUSDT", "tickers.BTCUSDT", "allLiquidation.BTCUSDT", *books[4:]]
    anchored = [*books[:4], "tickers.BTCUSDT", *books[4:]]
    shard = unstarted_shard(recorder, "deep", topics)
    socket = FakeSocket()
    shard.socket = socket  # type: ignore[assignment]
    shard.connected = True

    assert shard.reanchor("2027-01-15T08", limit=len(anchored)) == len(anchored)

    # Each chunk is unsubscribed and resubscribed together, so a symbol is
    # gone for one round trip rather than for the whole shard's pass.
    assert [json.loads(text) for text in socket.sent] == [
        {"op": "unsubscribe", "args": anchored[: record.REANCHOR_CHUNK]},
        {"op": "subscribe", "args": anchored[: record.REANCHOR_CHUNK]},
        {"op": "unsubscribe", "args": anchored[record.REANCHOR_CHUNK :]},
        {"op": "subscribe", "args": anchored[record.REANCHOR_CHUNK :]},
    ]
    # The subscription itself is unchanged: this is a re-anchor, not a replan.
    assert shard.topics == topics
    assert shard.reanchored("2027-01-15T08")
    assert not shard.reanchored("2027-01-15T09")


def test_a_shard_with_nothing_anchored_and_a_disconnected_one_re_anchor_nothing(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    prints_only = unstarted_shard(recorder, "wide", ["publicTrade.BTCUSDT", "allLiquidation.BTCUSDT"])
    prints_only.socket = FakeSocket()  # type: ignore[assignment]
    prints_only.connected = True
    assert prints_only.reanchor("2027-01-15T08", limit=40) == 0
    assert prints_only.reanchored("2027-01-15T08"), "nothing to anchor is anchored"

    offline = unstarted_shard(recorder, "wide", ["orderbook.50.BTCUSDT"])
    assert offline.reanchor("2027-01-15T08", limit=40) == 0
    assert not offline.reanchored("2027-01-15T08"), "it re-anchors by connecting"
    assert offline.reanchors == 0


def test_the_hourly_pass_is_bounded_per_tick_and_resumes_where_it_stopped(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    per_shard = record.REANCHOR_TOPICS_PER_TICK
    shards = []
    for index in range(2):
        topics = [f"orderbook.50.S{index}N{n}USDT" for n in range(per_shard)]
        shard = unstarted_shard(recorder, "deep", topics, index=index)
        shard.socket = FakeSocket()  # type: ignore[assignment]
        shard.connected = True
        shards.append(shard)
    recorder.tier_shards["deep"] = shards

    hour = BASE_NS
    recorder._reanchor(hour)
    assert shards[0].reanchor_cursor == per_shard, "the tick's whole budget went to the first shard"
    assert shards[1].reanchor_cursor == 0, "bounded per tick, not all at once"

    recorder._reanchor(hour + 30 * 10**9)
    assert [s.reanchor_cursor for s in shards] == [per_shard, per_shard]
    before = [len(s.socket.sent) for s in shards]  # type: ignore[union-attr]
    recorder._reanchor(hour + 60 * 10**9)
    assert [len(s.socket.sent) for s in shards] == before, "once an hour, not once a tick"  # type: ignore[union-attr]

    # The next UTC hour makes every shard due again.
    recorder._reanchor(hour + HOUR_NS)
    assert shards[0].reanchor_cursor == per_shard and shards[0].reanchor_hour.endswith("T09")


def book_frame(kind: str, update_id: int, depth: int = 50) -> str:
    return json.dumps(
        {
            "topic": f"orderbook.{depth}.BTCUSDT",
            "type": kind,
            "ts": 1_800_000_000_000,
            "cts": 1_799_999_999_999,
            "data": {
                "s": "BTCUSDT",
                "b": [["108420.10", "2.0"]],
                "a": [["108420.50", "1.2"]],
                "u": update_id,
                "seq": 9_000_000 + update_id,
            },
        }
    )


def feed(recorder: Recorder, *frames: str) -> None:
    for frame in frames:
        recorder.frames.put(("frame", frame, BASE_NS, "deep", 0))
    recorder.frames.put(None)
    recorder._write_loop()


def test_a_gapped_book_topic_is_re_subscribed_once_until_its_snapshot_lands(tmp_path: Path) -> None:
    """Bybit sends a book snapshot only to a subscription, so a topic that lost
    continuity would record deltas against a book nothing can rebuild until the
    hourly re-anchor. The live feed drops and refreshes that one topic; so does
    the recorder."""

    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    topics = ["orderbook.50.BTCUSDT", "orderbook.1.BTCUSDT", "publicTrade.BTCUSDT"]
    shard = unstarted_shard(recorder, "deep", topics)
    socket = FakeSocket()
    shard.socket = socket  # type: ignore[assignment]
    shard.connected = True
    recorder.tier_shards["deep"] = [shard]

    feed(recorder, book_frame("snapshot", 100), book_frame("delta", 102), book_frame("delta", 103))

    assert recorder.resync_pending == ["orderbook.50.BTCUSDT"], "one re-subscribe per gap per topic"
    assert recorder.resync_now.is_set(), "and the resyncer is woken for it"
    recorder._resync_books()

    assert [json.loads(text) for text in socket.sent] == [
        {"op": "unsubscribe", "args": ["orderbook.50.BTCUSDT"]},
        {"op": "subscribe", "args": ["orderbook.50.BTCUSDT"]},
    ]
    assert shard.resyncs == 1 and shard.status()["resyncs"] == 1
    assert shard.topics == topics, "the subscription list itself is unchanged"
    assert recorder.resync_outstanding == {"orderbook.50.BTCUSDT"}

    # Nothing more goes out while that re-subscribe is outstanding.
    feed(recorder, book_frame("delta", 104))
    recorder._resync_books()
    assert len(socket.sent) == 2

    # The snapshot it brings back re-bases the topic, and the next gap is taken.
    feed(recorder, book_frame("snapshot", 200))
    assert recorder.resync_outstanding == set()
    feed(recorder, book_frame("delta", 202))
    recorder._resync_books()

    assert [json.loads(text)["args"] for text in socket.sent] == [["orderbook.50.BTCUSDT"]] * 4
    assert shard.resyncs == 2


def test_two_gapped_topics_go_one_at_a_time_at_the_venues_message_spacing(tmp_path: Path, monkeypatch) -> None:
    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    topics = ["orderbook.50.BTCUSDT", "orderbook.1.BTCUSDT"]
    shard = unstarted_shard(recorder, "deep", topics)
    socket = FakeSocket()
    shard.socket = socket  # type: ignore[assignment]
    shard.connected = True
    recorder.tier_shards["deep"] = [shard]

    feed(
        recorder,
        book_frame("snapshot", 100),
        book_frame("snapshot", 100, depth=1),
        book_frame("delta", 102),
        book_frame("delta", 102, depth=1),
    )
    paused: list[float] = []
    monkeypatch.setattr(record.time, "sleep", paused.append)
    recorder._resync_books()

    assert [json.loads(text)["args"] for text in socket.sent] == [
        ["orderbook.50.BTCUSDT"],
        ["orderbook.50.BTCUSDT"],
        ["orderbook.1.BTCUSDT"],
        ["orderbook.1.BTCUSDT"],
    ]
    assert paused == [record.LIVE_MESSAGE_SPACING_SECONDS], "between topics, not inside a topic's round trip"
    assert shard.resyncs == 2


def test_a_gap_before_any_snapshot_waits_for_the_snapshot_already_coming(tmp_path: Path) -> None:
    """A topic no snapshot has based is waiting for the one its subscription
    brings, or — on a venue whose books come over REST — for a fetch already in
    flight. Both flag the deltas until they land, and neither needs a re-take."""

    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    shard = unstarted_shard(recorder, "deep", ["orderbook.50.BTCUSDT"])
    socket = FakeSocket()
    shard.socket = socket  # type: ignore[assignment]
    shard.connected = True
    recorder.tier_shards["deep"] = [shard]

    feed(recorder, book_frame("delta", 99), book_frame("delta", 100))
    recorder._resync_books()

    assert recorder.resync_pending == [] and recorder.resync_outstanding == set()
    assert socket.sent == [] and shard.resyncs == 0


def test_a_gapped_topic_on_a_shard_that_is_gone_waits_for_the_next_gap(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    shard = unstarted_shard(recorder, "deep", ["orderbook.50.BTCUSDT"])
    recorder.tier_shards["deep"] = [shard]

    feed(recorder, book_frame("snapshot", 100), book_frame("delta", 102))
    recorder._resync_books()

    # A disconnected shard re-bases by connecting, and nothing is left holding
    # the topic back from the next attempt.
    assert shard.resyncs == 0
    assert recorder.resync_pending == [] and recorder.resync_outstanding == set()


def test_a_shard_that_is_not_connected_only_records_the_new_list(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    shard = unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT"])

    added, removed = shard.update(["publicTrade.ETHUSDT"])

    assert (added, removed) == (["publicTrade.ETHUSDT"], ["publicTrade.BTCUSDT"])
    assert shard.topics == ["publicTrade.ETHUSDT"]


def test_reconciling_a_tier_keeps_placement_fills_room_and_closes_empty_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = build(
        tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))), per_connection=2
    )
    created: list[Shard] = []

    def quiet_shard(tier: str, topics: list[str]) -> Shard:
        shard = unstarted_shard(recorder, tier, topics, index=len(created))
        created.append(shard)
        return shard

    monkeypatch.setattr(recorder, "_new_shard", quiet_shard)

    recorder._reconcile_tier("deep", ["t1", "t2", "t3"])
    assert [shard.topics for shard in recorder.tier_shards["deep"]] == [["t1", "t2"], ["t3"]]

    # t2 leaves, t4 arrives: t4 takes t2's room; nobody reconnects.
    recorder._reconcile_tier("deep", ["t1", "t3", "t4"])
    assert [shard.topics for shard in recorder.tier_shards["deep"]] == [["t1", "t4"], ["t3"]]
    assert len(created) == 2

    # Only t3 is left: the first shard empties and is closed; the second keeps t3 on its own connection.
    recorder._reconcile_tier("deep", ["t3"])
    assert [shard.topics for shard in recorder.tier_shards["deep"]] == [["t3"]]
    assert created[0].stop.is_set() and not created[1].stop.is_set()

    # More than the room left opens a new shard for the overflow only.
    recorder._reconcile_tier("deep", ["t3", "t5", "t6", "t7"])
    assert [shard.topics for shard in recorder.tier_shards["deep"]] == [["t3", "t5"], ["t6", "t7"]]
    assert len(created) == 3


def test_shard_topics_never_mixes_connection_groups() -> None:
    group = lambda topic: "market" if topic.startswith("m") else "public"  # noqa: E731
    assert shard_topics(["p1", "m1", "p2", "m2", "p3"], 2, group) == [["p1", "p2"], ["p3"], ["m1", "m2"]]
    assert shard_topics(["p1", "m1", "p2"], 2) == [["p1", "m1"], ["p2"]]


# ---------------------------------------------------------------- refreshes


def test_the_first_refresh_fills_the_tiers_and_starts_no_shard(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT", exclude_tiers=("deep",))),
    )
    tables = {"instruments": [instrument("BTCUSDT"), instrument("AGIUSDT")], "tickers": [{"symbol": "AGIUSDT"}]}
    recorder.adapter.fetch_tables = lambda: tables  # type: ignore[method-assign]

    recorder._refresh(BASE_NS, restart=False)

    assert recorder.tier_symbols == {"deep": ["BTCUSDT"], "wide": ["AGIUSDT"]}
    assert recorder.tier_topics == {"deep": ["publicTrade.BTCUSDT"], "wide": ["tickers.AGIUSDT"]}
    assert recorder.tier_shards == {"deep": [], "wide": []}
    assert recorder.lanes == []
    assert sorted(recorder.feeds_by_symbol) == ["AGIUSDT", "BTCUSDT"]
    assert recorder.snapshot_failures == 0
    assert recorder.snapshots.last_ns == BASE_NS
    assert sorted(path.name.split("-")[0] for path in (tmp_path / "2027-01-15" / "08" / "_meta").iterdir()) == [
        "instruments",
        "tickers",
    ]


def test_a_venue_that_will_not_answer_leaves_the_snapshot_clock_open(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT")),
    )

    def refuse() -> dict[str, list[dict[str, Any]]]:
        raise RuntimeError("venue refused")

    recorder.adapter.fetch_tables = refuse  # type: ignore[method-assign]
    recorder._refresh(BASE_NS, restart=False)

    assert recorder.snapshot_failures == 1
    assert recorder.snapshots.last_key is None
    assert recorder.snapshots.due(BASE_NS)
    assert recorder.tables is None
    assert recorder.tier_symbols == {"deep": ["BTCUSDT"], "wide": []}


def test_a_replan_reconciles_only_the_tiers_whose_topics_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("crowded", (Feed("book", "50"),), Universe("funding_below", threshold_bp=8.0, quote="USDT")),
    )
    tables = {"instruments": [instrument("BTCUSDT"), instrument("AGIUSDT")], "tickers": []}
    recorder.adapter.fetch_tables = lambda: tables  # type: ignore[method-assign]
    reconciled: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(recorder, "_reconcile_tier", lambda name, topics: reconciled.append((name, list(topics))))

    recorder._refresh(BASE_NS, restart=True)
    assert reconciled == [("deep", ["publicTrade.BTCUSDT"])]

    recorder.live.observe("AGIUSDT", {"funding_rate": -0.0011}, BASE_NS + 1)
    changed = recorder._replan(BASE_NS + 2, apply=True)
    assert changed == ["crowded"]
    assert reconciled[-1] == ("crowded", ["orderbook.50.AGIUSDT"])


# ---------------------------------------------------------------- the bytes


def test_the_byte_meter_keeps_a_day_per_key_and_the_window_it_has_seen() -> None:
    meter = ByteMeter(BASE_NS)
    meter.add("all", 100, BASE_NS)
    meter.add("all", 50, BASE_NS + 30 * 10**9)
    meter.add("tier:wide", 50, BASE_NS + 30 * 10**9)
    assert meter.last_day("all", BASE_NS + 60 * 10**9) == 150
    assert meter.last_day("tier:wide", BASE_NS + 60 * 10**9) == 50

    # A day later the old minutes fall out of the window as new bytes arrive.
    meter.add("all", 7, BASE_NS + DAY_NS + 60 * 10**9)
    assert meter.totals["all"] == 157
    assert meter.last_day("all", BASE_NS + DAY_NS + 60 * 10**9) == 7
    assert meter.last_day("missing", BASE_NS) == 0
    assert meter.window_ns(BASE_NS + 60 * 10**9) == 60 * 10**9
    assert meter.window_ns(BASE_NS + 3 * DAY_NS) == DAY_NS
    assert meter.keys("tier:") == ["tier:wide"]


def test_frames_are_metered_by_tier_and_feed_class(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    sent_ms = BASE_NS // 1_000_000
    trade = json.dumps(
        {
            "topic": "publicTrade.BTCUSDT",
            "ts": sent_ms,
            "data": [{"s": "BTCUSDT", "S": "Buy", "p": "1", "v": "1", "T": sent_ms, "i": "t1"}],
        }
    ).encode()
    book = json.dumps(
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": sent_ms,
            "data": {"s": "BTCUSDT", "b": [["1", "1"]], "a": [["2", "1"]], "u": 5, "seq": 9},
        }
    ).encode()
    control = b'{"success":true,"op":"subscribe"}'
    recorder.frames.put(("frame", trade, BASE_NS, "deep", 0))
    recorder.frames.put(("frame", book, BASE_NS, "deep", 0))
    recorder.frames.put(("frame", control, BASE_NS, "wide", 0))
    recorder.frames.put(None)

    recorder._write_loop()
    recorder.writer.close()

    assert Recorder.feed_class([{"kind": "orderbook_delta", "depth": 50}]) == "book:50"
    assert Recorder.feed_class([{"kind": "kline", "interval": "1m"}]) == "kline:1m"
    status = recorder.bytes_status(BASE_NS + 1)
    assert status["received_total"] == len(trade) + len(book) + len(control)
    assert status["by_feed_24h"] == {
        "deep:book:50": len(book),
        "deep:trades": len(trade),
        "wide:control": len(control),
    }


def test_a_frame_the_disk_gate_drops_still_counts_against_the_inbound_allowance(tmp_path: Path) -> None:
    """`budget.monthly_gb` is an inbound allowance and `budget.shed` gives up
    subscriptions to stay under it, so the meter must count what the venue
    sent. Metering behind `disk_blocked` measures what the disk kept instead,
    which collapses the projection precisely while a recorder is discarding
    the most and can restore shed feeds — more inbound — during a storage
    incident."""

    recorder = build(tmp_path, Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT")))
    frame = json.dumps(
        {
            "topic": "tickers.AGIUSDT",
            "type": "delta",
            "ts": 1_800_000_000_000,
            "data": {"symbol": "AGIUSDT", "fundingRate": "-0.0015"},
        }
    )
    # What a crossing leaves behind: the gate is shut and the venue keeps sending.
    recorder.disk_blocked = True
    recorder.frames.put(("frame", frame, BASE_NS, "wide", 0))
    recorder.frames.put(None)

    recorder._write_loop()

    assert recorder.written_rows == 0
    assert recorder.disk_dropped_frames == 1
    assert recorder.meter.last_day("all", BASE_NS + 1) == len(frame)
    assert recorder.meter.last_day("tier:wide", BASE_NS + 1) == len(frame)
    # The per-feed split needs the normalized rows, which the gate never produced.
    assert recorder.meter.keys("feed:") == []


def _metered_hour(meter: ByteMeter, at_ns: int, wide_book: int, movers_book: int, rest: int) -> None:
    meter.add("all", wide_book + movers_book + rest, at_ns)
    meter.add("feed:wide:book:1", wide_book, at_ns)
    meter.add("feed:movers:book:50", movers_book, at_ns)
    meter.add("feed:core:trades", rest, at_ns)


def test_the_budget_sheds_what_the_projection_needs_and_counts_shed_pairs_out_of_it() -> None:
    meter = ByteMeter(BASE_NS)
    settings = BudgetSettings(
        monthly_gb=400.0, shed=(("wide", "book:1"), ("movers", "book:50")), restore_below=0.8, act_every_minutes=60
    )
    budget = BudgetController(settings, meter)

    # Less than an hour of history: no projection, no action.
    _metered_hour(meter, BASE_NS + 60 * 10**9, 500_000_000, 300_000_000, 200_000_000)
    assert budget.step(BASE_NS + 60 * 10**9) is False
    assert budget.projected_gb is None

    # One hour in, 1 GB received: 720 GB a month against 400. The first pair
    # alone carries 360 of it, so the first pair is all that goes.
    assert budget.step(BASE_NS + HOUR_NS) is True
    assert budget.shed_active == [("wide", "book:1")]
    assert budget.shed_gb[("wide", "book:1")] == pytest.approx(360.0)
    assert budget.over is True

    # A second hour of traffic without the shed pair. Its first-hour bytes
    # still sit in the window; the projection leaves them out and reads the
    # 360 GB/month that is still subscribed — under the allowance, so nothing
    # more is shed, and the shed pair (360 on top of 360) does not fit under
    # the restore line.
    _metered_hour(meter, BASE_NS + HOUR_NS + 60 * 10**9, 0, 300_000_000, 200_000_000)
    assert budget.step(BASE_NS + 2 * HOUR_NS) is False
    assert budget.projected_gb == pytest.approx(360.0)
    assert budget.over is False
    assert budget.shed_active == [("wide", "book:1")]
    status = budget.status()
    assert status["shed"] == ["wide:book:1"]
    assert status["shed_gb_month"] == {"wide:book:1": 360.0}
    assert status["shed_order"] == ["wide:book:1", "movers:book:50"]


def test_the_budget_sheds_several_pairs_in_one_action_and_says_when_the_list_is_not_enough(
    caplog: pytest.LogCaptureFixture,
) -> None:
    meter = ByteMeter(BASE_NS)
    settings = BudgetSettings(
        monthly_gb=100.0, shed=(("wide", "book:1"), ("movers", "book:50")), restore_below=0.8, act_every_minutes=60
    )
    budget = BudgetController(settings, meter)
    _metered_hour(meter, BASE_NS + 60 * 10**9, 500_000_000, 300_000_000, 200_000_000)

    # 720 against 100: both pairs go at once (720 - 360 - 216 = 144), and
    # what is left is still over, which is said rather than left to status.json.
    with caplog.at_level(logging.WARNING):
        assert budget.step(BASE_NS + HOUR_NS) is True
    assert budget.shed_active == [("wide", "book:1"), ("movers", "book:50")]
    assert [record.getMessage() for record in caplog.records][-1].startswith(
        "over budget with every sheddable feed shed: 144 GB/month projected against 100 allowed"
    )

    # An hour on, the rest still runs at 144 GB/month with nothing left to
    # shed: no change, said again.
    _metered_hour(meter, BASE_NS + HOUR_NS + 60 * 10**9, 0, 0, 200_000_000)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        assert budget.step(BASE_NS + 2 * HOUR_NS) is False
    assert any("every sheddable feed shed" in record.getMessage() for record in caplog.records)
    # Between actions, quiet.
    caplog.clear()
    assert budget.step(BASE_NS + 2 * HOUR_NS + 10 * 60 * 10**9) is False
    assert caplog.records == []


def test_a_shed_pair_is_restored_only_when_its_own_month_fits_under_the_restore_line() -> None:
    meter = ByteMeter(BASE_NS)
    settings = BudgetSettings(
        monthly_gb=400.0, shed=(("wide", "book:1"), ("movers", "book:50")), restore_below=0.8, act_every_minutes=60
    )
    budget = BudgetController(settings, meter)
    _metered_hour(meter, BASE_NS + 60 * 10**9, 500_000_000, 300_000_000, 200_000_000)
    budget.settings = BudgetSettings(monthly_gb=100.0, shed=settings.shed, restore_below=0.8, act_every_minutes=60)
    assert budget.step(BASE_NS + HOUR_NS) is True
    assert budget.shed_active == [("wide", "book:1"), ("movers", "book:50")]
    budget.settings = settings

    # A day later the meter shows almost nothing. The last pair shed carried
    # 216 GB/month: under 320, restored. The first carried 360: over 320, it
    # stays shed however quiet the tape is, because restoring it would put
    # the recorder straight back over.
    late = BASE_NS + 2 * DAY_NS
    meter.add("all", 1, late)
    assert budget.step(late) is True
    assert budget.shed_active == [("wide", "book:1")]
    assert budget.step(late + HOUR_NS) is False
    assert budget.shed_active == [("wide", "book:1")]
    assert budget.over is False


# ------------------------------------------------------------------- status


def test_the_status_file_carries_what_the_host_watchdog_reads(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("deep", (Feed("book", "50"), Feed("trades")), Universe("symbols", symbols=("BTCUSDT",))),
        Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT", exclude_tiers=("deep",))),
        budget=BudgetSettings(monthly_gb=1300.0, shed=(("wide", "ticker"),)),
    )
    recorder.tier_symbols = {"deep": ["BTCUSDT"], "wide": ["AGIUSDT"]}
    recorder.tier_topics = {"deep": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"], "wide": ["tickers.AGIUSDT"]}
    recorder.tier_shards["deep"] = [unstarted_shard(recorder, "deep", recorder.tier_topics["deep"])]
    recorder._on_frame(BASE_NS)
    recorder._on_overrun()
    recorder.disk_dropped_frames = 3
    recorder.disk_blocked = True
    recorder.budget.shed_active = [("wide", "ticker")]
    recorder.frames.put(("frame", b"x" * 100, BASE_NS, "deep", 0))

    recorder._write_status()
    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

    assert payload["kind"] == "forward_capture_status"
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["pid"] == os.getpid()
    # The compressor's own state rides in the heartbeat, so a backlog or a
    # failing zstd is visible while the recorder itself still looks alive.
    assert payload["compressor"] == {
        "pending": 0,
        "pending_bytes": 0,
        "backlog_max_bytes": 4294967296,
        "compressed": 0,
        "failed": 0,
        "deferred": 0,
        "deferred_total": 0,
        "last_error": None,
        "last_error_ns": None,
        "last_deferred_ns": None,
        "alive": False,
    }
    assert payload["venue"] == "bybit" and payload["market"] == "linear"
    assert payload["config"] == "deploy/capture/bybit-linear.toml"
    assert payload["last_receive_ns"] == BASE_NS
    # The watchdog reads silence and socket loss against this, so a recorder
    # seconds old does not page as a dead venue.
    assert payload["started_at_ns"] == recorder.started_at_ns > 0
    assert payload["received_frames"] == 1
    assert payload["dropped_frames"] == 1
    assert payload["disk_dropped_frames"] == 3
    assert payload["disk_blocked"] is True
    assert payload["queued_frames"] == 1 and payload["queue_capacity"] == 16
    assert payload["queued_bytes"] == record.QUEUE_ITEM_OVERHEAD_BYTES + 100
    assert payload["queue_byte_capacity"] == 1024**3
    assert payload["status_interval_seconds"] == 30.0
    assert payload["shards"] == [
        {
            "index": 0,
            "tier": "deep",
            "topics": 2,
            "connected": False,
            "reconnects": 0,
            "reanchors": 0,
            "resyncs": 0,
            "last_message_ns": 0,
            # The reader has not yet reported on a connection it never opened.
            "link": {},
        }
    ]
    assert payload["tiers"] == [
        {
            "name": "deep",
            "universe": "symbols",
            "live": False,
            "feeds": ["book:50", "trades"],
            "shed": [],
            "symbols": 1,
            "topics": 2,
            "names": ["BTCUSDT"],
        },
        {
            "name": "wide",
            "universe": "listed",
            "live": False,
            "feeds": ["ticker"],
            "shed": ["ticker"],
            "symbols": 1,
            "topics": 1,
            "names": ["AGIUSDT"],
        },
    ]
    assert payload["budget"]["monthly_gb"] == 1300.0
    assert payload["budget"]["shed"] == ["wide:ticker"]
    assert payload["budget"]["over"] is False
    assert set(payload["bytes"]) == {"received_total", "received_24h", "by_feed_24h"}


def test_the_maintenance_tick_writes_the_heartbeat_without_walking_the_tape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`status.json` is this unit's heartbeat and the watchdog's silence
    reading both. Retention walks tens of thousands of files on the host, so
    the tick that writes the heartbeat may not wait on a pass; the pruner
    thread owns it."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))

    def refuse() -> dict[str, list[dict[str, Any]]]:
        raise RuntimeError("venue refused")

    recorder.adapter.fetch_tables = refuse  # type: ignore[method-assign]
    monkeypatch.setattr(recorder, "_reconcile_tier", lambda name, topics: None)
    # The free floor is the developer box's, not the host's: pin it so the tick
    # reads `disk_blocked` from the code under test and not from this disk.
    recorder.retention.min_free_bytes = 1
    directory = tmp_path / "2027-01-15" / "10" / "BTCUSDT"
    directory.mkdir(parents=True)
    expired = directory / "segment-000000.jsonl.zst"
    expired.write_bytes(b"long past retention")
    os.utime(expired, (1_000_000, 1_000_000))

    recorder._maintenance()

    assert (tmp_path / "status.json").exists()
    assert recorder.disk_blocked is False
    assert expired.exists()

    recorder._retention_pass()

    assert not expired.exists()


def test_a_disk_under_the_free_floor_prunes_now_instead_of_waiting_out_the_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`min_free_disk_gb` is free space on the whole filesystem, so anything
    sharing it can put the recorder under the floor, and under the floor every
    frame is counted and thrown away. The tick that sees it must start the only
    thing that frees room rather than leave the pruner asleep for the rest of
    `RETENTION_INTERVAL_SECONDS`."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))

    def refuse() -> dict[str, list[dict[str, Any]]]:
        raise RuntimeError("venue refused")

    recorder.adapter.fetch_tables = refuse  # type: ignore[method-assign]
    monkeypatch.setattr(recorder, "_reconcile_tier", lambda name, topics: None)
    # Long enough that a pass inside it can only come from the wake, never the clock.
    monkeypatch.setattr(record, "RETENTION_INTERVAL_SECONDS", 3600.0)

    started = threading.Event()
    woken = threading.Event()
    passes: list[int] = []

    def counted(now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        passes.append(len(passes))
        (started if len(passes) == 1 else woken).set()
        return []

    monkeypatch.setattr(recorder.retention, "prune", counted)
    # A daemon thread, so a failed assertion below reports itself rather than
    # the teardown that a missing wake would trip over.
    pruner = threading.Thread(target=recorder._retention_loop, name="test-retention", daemon=True)
    pruner.start()
    assert started.wait(5.0), "the pruner never made its first pass"

    # A writable disk leaves the pruner on its routine interval.
    recorder.retention.min_free_bytes = 1
    recorder._maintenance()
    assert recorder.disk_blocked is False
    assert not woken.wait(0.5)

    # Now nothing on this filesystem is enough.
    recorder.retention.min_free_bytes = 1 << 62
    recorder._maintenance()
    assert recorder.disk_blocked is True
    assert woken.wait(5.0), "the pruner slept out its interval while the disk was blocked"

    # The level, not only the crossing: a burst ends on the pass that finds no
    # deficit left and that leaves the gate shut, so the still-blocked tick is
    # where the next pass has to come from.
    woken.clear()
    recorder._maintenance()
    assert recorder.disk_blocked is True
    assert woken.wait(5.0), "a blocked tick left the pruner asleep"

    # A stop wakes the pruner: shutdown never waits out a retention interval.
    recorder.stop.set()
    recorder.prune_now.set()
    pruner.join(5.0)
    assert not pruner.is_alive()


def test_a_pass_that_frees_room_opens_the_writer_gate_instead_of_the_next_status_tick(
    tmp_path: Path,
) -> None:
    """`disk_blocked` gates every frame in `_write_loop`, and only a retention
    pass frees room, so the pass that frees it is what must open the gate.
    Leaving that to `_maintenance` throws away a whole
    `status_interval_seconds` of tape onto a disk that already has space."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    directory = tmp_path / "2027-01-15" / "10" / "BTCUSDT"

    def expired(name: str) -> Path:
        # A pass that empties an hour removes the directory too, so each file
        # remakes its own.
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        path.write_bytes(b"long past retention")
        os.utime(path, (1_000_000, 1_000_000))
        return path

    first = expired("segment-000000.jsonl.zst")
    # What a crossing leaves behind: the free-space tick, or the append that
    # failed first, has closed the gate.
    recorder.disk_blocked = True

    # A pass that deletes but leaves the disk under the floor changes nothing:
    # the floor is the developer box's, not the host's, so pin it.
    recorder.retention.min_free_bytes = 1 << 62
    recorder._retention_pass()

    assert not first.exists()
    assert recorder.disk_blocked is True

    # Room is back, so the frame after this pass is tape, not a dropped count.
    expired("segment-000001.jsonl.zst")
    recorder.retention.min_free_bytes = 1
    recorder._retention_pass()

    assert recorder.disk_blocked is False

    row = {"kind": "ticker", "symbol": "BTCUSDT", "values": {}, "local_receive_ts_ns": BASE_NS}
    recorder.frames.put(("rows", [row], BASE_NS, "deep", 0))
    recorder.frames.put(None)
    recorder._write_loop()

    assert recorder.written_rows == 1
    assert recorder.disk_dropped_frames == 0


def test_a_pass_that_deletes_and_leaves_the_gate_shut_passes_again_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`prune` stops on the free space it counts from the sizes it unlinked;
    `writable()` reads the kernel's. While the filesystem is still releasing
    blocks the two disagree, so a pass can delete, believe it reached the
    floor, and leave the gate shut. Nothing else wakes the pruner then —
    `_maintenance` arms `prune_now` on the crossing only, and `_write_loop`
    never reaches an append to fail on — so the pass that fell short must be
    what runs the next one. Sleeping out `RETENTION_INTERVAL_SECONDS` instead
    throws away the tape the pass already made room for."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    # Long enough that a second pass inside it can only come from the first.
    monkeypatch.setattr(record, "RETENTION_INTERVAL_SECONDS", 3600.0)
    # What a crossing leaves behind, and the developer box's own free space
    # pinned under the floor so `writable()` reads the code under test.
    recorder.disk_blocked = True
    recorder.retention.min_free_bytes = 1 << 62

    passes: list[int] = []
    third = threading.Event()

    def short(now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        passes.append(len(passes))
        # Two passes the kernel does not yet agree with, then one it does.
        if len(passes) == 3:
            recorder.retention.min_free_bytes = 1
            third.set()
        return [Path(f"2027-01-15/10/BTCUSDT/segment-{len(passes):06d}.jsonl.zst")]

    monkeypatch.setattr(recorder.retention, "prune", short)
    # A daemon thread, so a failed assertion below reports itself rather than
    # the teardown that a missing pass would trip over.
    pruner = threading.Thread(target=recorder._retention_loop, name="test-retention", daemon=True)
    pruner.start()

    assert third.wait(5.0), f"the pruner slept with the gate shut after {len(passes)} pass(es)"
    deadline = time.monotonic() + 5.0
    while recorder.disk_blocked and time.monotonic() < deadline:
        time.sleep(0.01)
    assert recorder.disk_blocked is False, "the pass that reached the floor did not open the gate"

    # The retry ends where it must: a pass that deletes nothing does not walk
    # the tape again, so a full disk holding no tape is not spun on.
    assert recorder._retention_pass() is False
    monkeypatch.setattr(recorder.retention, "prune", lambda now=None, *, free_credit=0: [])
    recorder.disk_blocked = True
    recorder.retention.min_free_bytes = 1 << 62
    assert recorder._retention_pass() is False

    recorder.stop.set()
    recorder.prune_now.set()
    pruner.join(5.0)
    assert not pruner.is_alive()


def test_a_burst_of_owed_passes_deletes_the_deficit_once_not_the_whole_tape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successor is owed because the kernel's free space disagreed with what
    the pass unlinked, and the retries run back to back with nothing between
    them. So a successor that re-reads that same number derives the whole
    deficit again and deletes it again, pass after pass, until the tape has no
    file left: a floor held by something other than tape costs every hour of
    history the recorder holds. Each pass must credit what the burst already
    unlinked and stop where the first one did."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    # Long enough that every pass inside the burst comes from the burst itself.
    monkeypatch.setattr(record, "RETENTION_INTERVAL_SECONDS", 3600.0)
    recorder.retention.retention_days = 36_500
    recorder.retention.max_bytes = 10**12
    recorder.retention.min_free_bytes = 400
    directory = tmp_path / "2027-01-15" / "10" / "BTCUSDT"
    directory.mkdir(parents=True)
    for index in range(20):
        (directory / f"segment-{index:06d}.jsonl.zst").write_bytes(b"x" * 100)

    # A filesystem that has not released a single unlinked block: free space
    # reads the same under the floor however much the burst deletes, so every
    # pass is owed a successor and `writable()` never opens the gate.
    monkeypatch.setattr(
        "market_tape.storage.shutil.disk_usage",
        lambda path: SimpleNamespace(total=10_000, used=9_800, free=200),
    )
    # What a crossing leaves behind.
    recorder.disk_blocked = True

    real_prune = recorder.retention.prune
    passes: list[int] = []
    settled = threading.Event()

    def counted(now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        passes.append(len(passes))
        deleted = real_prune(now, free_credit=free_credit)
        if not deleted:
            settled.set()
        return deleted

    monkeypatch.setattr(recorder.retention, "prune", counted)
    # A daemon thread, so a failed assertion below reports itself rather than
    # the teardown that a runaway burst would wait on.
    pruner = threading.Thread(target=recorder._retention_loop, name="test-retention", daemon=True)
    pruner.start()

    assert settled.wait(5.0), f"the burst never ended after {len(passes)} pass(es)"
    assert len(passes) == 2, "the successor deleted a second deficit instead of crediting the first"
    assert len(list(directory.glob("*.zst"))) == 17
    assert recorder.disk_blocked is True

    recorder.stop.set()
    recorder.prune_now.set()
    pruner.join(5.0)
    assert not pruner.is_alive()


def test_a_blocked_tick_runs_the_pass_the_credited_burst_stopped_short_of(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A credited burst ends on the pass that finds no deficit left, and it
    ends with the gate still shut whenever the kernel released the unlinked
    blocks and another writer on the filesystem took them — which is what two
    recorders sharing one disk do to each other. The tape still holds hours
    nobody needs and every frame is being counted and dropped, so the next
    blocked status tick is what must run the next pass. Arming `prune_now` on
    the crossing alone leaves the pruner asleep for a whole
    `RETENTION_INTERVAL_SECONDS` there."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))

    def refuse() -> dict[str, list[dict[str, Any]]]:
        raise RuntimeError("venue refused")

    recorder.adapter.fetch_tables = refuse  # type: ignore[method-assign]
    monkeypatch.setattr(recorder, "_reconcile_tier", lambda name, topics: None)
    # Long enough that a pass inside it can only come from a wake, never the clock.
    monkeypatch.setattr(record, "RETENTION_INTERVAL_SECONDS", 3600.0)
    recorder.retention.retention_days = 36_500
    recorder.retention.max_bytes = 10**12
    recorder.retention.min_free_bytes = 400
    directory = tmp_path / "2027-01-15" / "10" / "BTCUSDT"
    directory.mkdir(parents=True)
    for index in range(20):
        (directory / f"segment-{index:06d}.jsonl.zst").write_bytes(b"x" * 100)

    # A filesystem whose free space never moves however much the burst
    # unlinks: the neighbour recorder takes each freed block as it is
    # released. The credit is then an overstatement, so the successor finds no
    # deficit and the burst stops with the disk still under the floor.
    monkeypatch.setattr(
        "market_tape.storage.shutil.disk_usage",
        lambda path: SimpleNamespace(total=10_000, used=9_800, free=200),
    )
    # What a crossing leaves behind.
    recorder.disk_blocked = True

    real_prune = recorder.retention.prune
    passes: list[int] = []
    settled = threading.Event()

    def counted(now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        passes.append(len(passes))
        deleted = real_prune(now, free_credit=free_credit)
        if not deleted:
            settled.set()
        return deleted

    monkeypatch.setattr(recorder.retention, "prune", counted)
    # A daemon thread, so a failed assertion below reports itself rather than
    # the teardown that a missing pass would wait on.
    pruner = threading.Thread(target=recorder._retention_loop, name="test-retention", daemon=True)
    pruner.start()

    assert settled.wait(5.0), f"the burst never ended after {len(passes)} pass(es)"
    assert len(passes) == 2
    assert len(list(directory.glob("*.zst"))) == 17
    assert recorder.disk_blocked is True

    settled.clear()
    recorder._maintenance()

    assert settled.wait(5.0), "a blocked tick left the pruner asleep on a tape it could still trim"
    assert len(passes) == 4
    assert len(list(directory.glob("*.zst"))) == 14

    recorder.stop.set()
    recorder.prune_now.set()
    pruner.join(5.0)
    assert not pruner.is_alive()


def test_a_retention_pass_that_cannot_delete_leaves_the_pruner_thread_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))

    def refuse(now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        raise OSError("read-only file system")

    monkeypatch.setattr(recorder.retention, "prune", refuse)
    with caplog.at_level(logging.ERROR):
        recorder._retention_pass()

    assert "tape retention pass failed" in caplog.text


def test_a_tick_the_full_disk_refuses_leaves_the_maintenance_loop_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A full disk makes the status write raise ENOSPC out of `_maintenance`.
    A maintenance thread that died on it would leave the process running with
    nothing re-reading free space or waking the pruner, and `status.json`
    going stale."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    storage = dataclasses.replace(recorder.config.storage, status_interval_seconds=0.01)
    monkeypatch.setattr(recorder, "config", dataclasses.replace(recorder.config, storage=storage))
    ticks: list[int] = []

    def full_disk_then_stop() -> None:
        ticks.append(len(ticks))
        if len(ticks) >= 3:
            recorder.stop.set()
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(recorder, "_maintenance", full_disk_then_stop)
    maintainer = threading.Thread(target=recorder._maintenance_loop, name="test-maintenance", daemon=True)
    with caplog.at_level(logging.ERROR):
        maintainer.start()
        maintainer.join(5.0)

    assert not maintainer.is_alive()
    assert len(ticks) == 3, "one refused tick ended the maintenance loop"
    assert "capture maintenance tick failed" in caplog.text


def test_the_heartbeat_keeps_its_interval_while_the_disk_holds_every_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An fsync on the filesystem the writer streams to waits for the writer's
    dirty pages: under a burst on a loaded host one took 13 s on the maintenance
    thread and `status.json` stalled with it. The heartbeat takes no flush, and
    the hour's venue tables and coverage record, which take several, wait on
    the meta thread."""

    recorder = build(
        tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))), cadence="hour"
    )
    settings = dataclasses.replace(recorder.config.storage, status_interval_seconds=0.02)
    monkeypatch.setattr(recorder, "config", dataclasses.replace(recorder.config, storage=settings))
    tables = {"instruments": [instrument("BTCUSDT")], "tickers": [{"symbol": "BTCUSDT"}]}
    recorder.adapter.fetch_tables = lambda: tables  # type: ignore[method-assign]
    recorder.retention.min_free_bytes = 1
    monkeypatch.setattr(recorder, "_install_signals", lambda: None)
    monkeypatch.setattr(recorder, "_reconcile_tier", lambda name, topics: None)
    offset = {"ns": 0}
    real_time_ns, real_fsync = time.time_ns, os.fsync
    monkeypatch.setattr(time, "time_ns", lambda: real_time_ns() + offset["ns"])
    held, released = threading.Event(), threading.Event()
    waited: list[str] = []

    def fsync(descriptor: int) -> None:
        if held.is_set() and not released.is_set():
            waited.append(threading.current_thread().name)
            released.wait(10.0)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)
    failures: list[BaseException] = []

    def run() -> None:
        try:
            recorder.run()
        except BaseException as exc:  # reported on the test's thread
            failures.append(exc)

    status = tmp_path / "status.json"
    runner = threading.Thread(target=run, name="test-recorder", daemon=True)
    runner.start()
    try:
        deadline = time.monotonic() + 10.0
        while not status.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert status.exists(), "the recorder never wrote its first status"
        # Every flush now waits, and the clock crosses the hour: the tables and
        # the closed hour's coverage record come due on the next tick.
        held.set()
        offset["ns"] = HOUR_NS
        stamps = set()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            stamps.add(json.loads(status.read_text(encoding="utf-8"))["recorded_at_ns"])
            time.sleep(0.005)
        flushers = set(waited)
    finally:
        released.set()
        recorder.stop.set()
        runner.join(30.0)

    assert not runner.is_alive() and failures == []
    assert "tape-maintenance" not in flushers, "the heartbeat's thread waited on a flush"
    assert "tape-meta" in flushers
    assert len(stamps) >= 5, f"{len(stamps)} status writes in a second at a 20 ms interval"
    # Late, not lost: the start's tables and the stop's partial hour, and
    # between them the tables and the closed hour the held flushes delayed.
    assert len(list(tmp_path.glob("*/*/_meta/instruments-*.json.zst"))) >= 2
    assert len(list(tmp_path.glob("*/*/_meta/coverage-*.json.zst"))) >= 2


def test_a_failed_append_blocks_the_disk_and_asks_for_a_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The append is the first detector of a full disk — it fails milliseconds
    in, where the free-space tick is a status interval behind — so it is also
    what starts the pass that frees room."""

    recorder = build(tmp_path, Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT")))

    def no_space(row: Any) -> list[Any]:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(recorder.writer, "append", no_space)
    frame = json.dumps(
        {
            "topic": "tickers.AGIUSDT",
            "type": "delta",
            "ts": 1_800_000_000_000,
            "data": {"symbol": "AGIUSDT", "fundingRate": "-0.0015"},
        }
    )
    recorder.frames.put(("frame", frame, BASE_NS, "wide", 0))
    recorder.frames.put(None)

    with caplog.at_level(logging.ERROR):
        recorder._write_loop()

    assert recorder.disk_blocked is True
    assert recorder.written_rows == 0
    assert recorder.disk_dropped_frames == 1
    assert "No space left on device" in caplog.text
    assert recorder.prune_now.is_set(), "a full disk left the pruner asleep"


def test_the_writer_feeds_the_live_state_from_ticker_rows(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("wide", (Feed("ticker"),), Universe("listed", quote="USDT")))
    frame = json.dumps(
        {
            "topic": "tickers.AGIUSDT",
            "type": "delta",
            "ts": 1_800_000_000_000,
            "data": {"symbol": "AGIUSDT", "fundingRate": "-0.0015", "turnover24h": "123456", "price24hPcnt": "0.21"},
        }
    )
    recorder.frames.put(("frame", frame, BASE_NS, "wide", 0))
    recorder.frames.put(None)

    recorder._write_loop()

    live, _ = recorder.live.view()
    assert live["AGIUSDT"].funding_rate == pytest.approx(-0.0015)
    assert live["AGIUSDT"].turnover_24h == pytest.approx(123456.0)
    assert live["AGIUSDT"].price_change_24h == pytest.approx(0.21)
    assert recorder.written_rows == 1
    assert recorder.meter.totals["feed:wide:ticker"] == len(frame)
    for segment in recorder.writer.close():
        recorder.compressor.submit(segment)


def test_the_host_config_drives_the_same_recorder(tmp_path: Path) -> None:
    text = """
[venue]
name = "bybit"
market = "linear"

[snapshots]
cadence = "hour"

[budget]
monthly_gb = 100
shed = ["wide:book:1"]

[[tier]]
name = "deep"
feeds = ["book:50", "trades"]
universe = { kind = "symbols", symbols = ["BTCUSDT"] }

[[tier]]
name = "wide"
feeds = ["book:1"]
universe = { kind = "listed", quote = "USDT", exclude_tiers = ["deep"] }
"""
    config = parse_config(tomllib.loads(text), base_dir=tmp_path)
    recorder = Recorder(config, root=tmp_path, adapter=BybitAdapter(rest_url="http://unused"))
    tables = {"instruments": [instrument("BTCUSDT"), instrument("AGIUSDT")], "tickers": []}

    topics = recorder.plan_topics(recorder.resolve_tiers(BASE_NS, tables))[0]

    assert recorder.snapshots.cadence == "hour"
    assert recorder.budget.settings.monthly_gb == 100.0
    assert topics == {"deep": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"], "wide": ["orderbook.1.AGIUSDT"]}

    # Every config the repository ships passes the check a host runs before it
    # installs anything, so a bad one fails here instead.
    repository = Path(record.__file__).resolve().parents[1]
    shipped = sorted(
        [*(repository / "deploy" / "capture").glob("*.toml"), *(repository / "market_tape" / "examples").glob("*.toml")]
    )
    assert shipped
    for path in shipped:
        checked = subprocess.run(
            [sys.executable, "-m", "market_tape", "check", "--config", str(path)],
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
        )
        assert checked.returncode == 0, (path.name, checked.stderr)
        assert checked.stdout.startswith("tier "), (path.name, checked.stdout)


# ---------------------------------------------------- the other live universes


def test_positive_funding_promotes_into_an_overheated_tier(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier(
            "overheated",
            (Feed("book", "50"),),
            Universe("funding_above", threshold_bp=8.0, sticky_hours=1.0, quote="USDT"),
        ),
    )
    recorder.tables = {
        "instruments": [instrument("HOTUSDT"), instrument("COLDUSDT"), instrument("WARMUSDT")],
        "tickers": [],
    }
    recorder.live.observe("HOTUSDT", {"funding_rate": 0.0008}, BASE_NS)
    recorder.live.observe("COLDUSDT", {"funding_rate": -0.0020}, BASE_NS)
    recorder.live.observe("WARMUSDT", {"funding_rate": 0.00079}, BASE_NS)

    assert recorder.resolve_tiers(BASE_NS + 1)["overheated"] == ["HOTUSDT"]

    # The rate cools; the name keeps its tier for the sticky hour, then goes.
    recorder.live.observe("HOTUSDT", {"funding_rate": 0.0001}, BASE_NS + 2)
    assert recorder.resolve_tiers(BASE_NS + HOUR_NS)["overheated"] == ["HOTUSDT"]
    assert recorder.resolve_tiers(BASE_NS + 1 + HOUR_NS)["overheated"] == []


def test_top_movers_ranks_by_the_size_of_the_move_and_keeps_a_member_until_it_falls_below_leave_top(
    tmp_path: Path,
) -> None:
    recorder = build(
        tmp_path, Tier("movers", (Feed("book", "50"),), Universe("top_movers", top=2, leave_top=3, quote="USDT"))
    )
    names = ("AUSDT", "BUSDT", "CUSDT", "DUSDT")
    recorder.tables = {"instruments": [instrument(name) for name in names], "tickers": []}
    for name, change in zip(names, (0.20, -0.30, 0.05, 0.01)):
        recorder.live.observe(name, {"price_change_24h_pct": change}, BASE_NS)
    assert recorder.resolve_tiers(BASE_NS + 1)["movers"] == ["AUSDT", "BUSDT"]

    # C overtakes A; A is now rank 3, inside leave_top, so it stays.
    recorder.live.observe("CUSDT", {"price_change_24h_pct": -0.25}, BASE_NS + 2)
    assert recorder.resolve_tiers(BASE_NS + 3)["movers"] == ["AUSDT", "BUSDT", "CUSDT"]
    # D moves to rank 3: not a member and not in the top two, so it does not enter; A drops to rank 4 and leaves.
    recorder.live.observe("DUSDT", {"price_change_24h_pct": 0.22}, BASE_NS + 4)
    assert recorder.resolve_tiers(BASE_NS + 5)["movers"] == ["BUSDT", "CUSDT"]


def test_a_price_burst_is_measured_against_the_sample_one_window_back(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier(
            "bursting",
            (Feed("book", "50"),),
            Universe("price_burst", pct=0.05, window_hours=1.0, sticky_hours=1.0, quote="USDT"),
        ),
    )
    recorder.tables = {"instruments": [instrument("POPUSDT")], "tickers": []}
    recorder.live.observe("POPUSDT", {"mark_price": 1.00}, BASE_NS)

    # Half an hour in, up six percent: the history does not reach an hour back yet, so nothing.
    recorder.live.observe("POPUSDT", {"mark_price": 1.06}, BASE_NS + HOUR_NS // 2)
    assert recorder.resolve_tiers(BASE_NS + HOUR_NS // 2 + 1)["bursting"] == []

    # An hour and a minute in, still up six percent on the hour: promoted.
    recorder.live.observe("POPUSDT", {"mark_price": 1.06}, BASE_NS + HOUR_NS + 60 * 10**9)
    assert recorder.resolve_tiers(BASE_NS + HOUR_NS + 61 * 10**9)["bursting"] == ["POPUSDT"]

    # Later the price is where it was an hour before, so it no longer qualifies; the name lingers its sticky hour, then goes.
    recorder.live.observe("POPUSDT", {"mark_price": 1.06}, BASE_NS + HOUR_NS + 50 * 60 * 10**9)
    assert recorder.resolve_tiers(BASE_NS + HOUR_NS + 50 * 60 * 10**9 + 1)["bursting"] == ["POPUSDT"]
    assert recorder.resolve_tiers(BASE_NS + 2 * HOUR_NS + 2 * 60 * 10**9)["bursting"] == []


def test_a_volume_burst_is_the_window_trading_beyond_the_same_window_a_day_earlier(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier(
            "flooding",
            (Feed("book", "50"),),
            Universe("volume_burst", ratio=3.0, window_hours=1.0, sticky_hours=1.0, quote="USDT"),
        ),
    )
    recorder.tables = {"instruments": [instrument("HNTUSDT"), instrument("DULLUSDT")], "tickers": []}
    recorder.live.observe("HNTUSDT", {"turnover_24h": 24_000.0}, BASE_NS)
    recorder.live.observe("DULLUSDT", {"turnover_24h": 24_000.0}, BASE_NS)

    # An average hour of 28,000 is 1,167; three of them are 3,500. HNT grew 4,000, DULL 3,000.
    recorder.live.observe("HNTUSDT", {"turnover_24h": 28_000.0}, BASE_NS + HOUR_NS + 60 * 10**9)
    recorder.live.observe("DULLUSDT", {"turnover_24h": 27_000.0}, BASE_NS + HOUR_NS + 60 * 10**9)
    assert recorder.resolve_tiers(BASE_NS + HOUR_NS + 61 * 10**9)["flooding"] == ["HNTUSDT"]


def test_an_open_interest_jump_either_way_promotes(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier(
            "levering",
            (Feed("book", "50"),),
            Universe("oi_change", pct=0.10, window_hours=1.0, sticky_hours=1.0, quote="USDT"),
        ),
    )
    recorder.tables = {
        "instruments": [instrument("UPUSDT"), instrument("DOWNUSDT"), instrument("FLATUSDT")],
        "tickers": [],
    }
    for name in ("UPUSDT", "DOWNUSDT", "FLATUSDT"):
        recorder.live.observe(name, {"open_interest": 100.0}, BASE_NS)
    later = BASE_NS + HOUR_NS + 60 * 10**9
    recorder.live.observe("UPUSDT", {"open_interest": 110.0}, later)
    recorder.live.observe("DOWNUSDT", {"open_interest": 89.0}, later)
    recorder.live.observe("FLATUSDT", {"open_interest": 109.0}, later)

    assert recorder.resolve_tiers(later + 1)["levering"] == ["DOWNUSDT", "UPUSDT"]


def test_the_live_history_keeps_one_sample_a_minute_and_only_as_far_back_as_the_longest_window(tmp_path: Path) -> None:
    recorder = build(
        tmp_path,
        Tier("bursting", (Feed("book", "50"),), Universe("price_burst", pct=0.05, window_hours=1.0, quote="USDT")),
        Tier("levering", (Feed("book", "50"),), Universe("oi_change", pct=0.10, window_hours=2.0, quote="USDT")),
    )
    live = recorder.live
    assert live.history_ns == int(2.0 * 3600 * 1e9 * 1.25)
    for second in range(0, 4 * 3600, 10):
        live.observe("BTCUSDT", {"mark_price": 1.0 + second / 1e6}, BASE_NS + second * 10**9)
    samples = live.history["BTCUSDT"]
    spacing = {(b.ns - a.ns) // 10**9 for a, b in zip(samples, list(samples)[1:])}
    assert spacing == {60}
    assert len(samples) <= 2.5 * 60 + 2
    # The oldest kept sample is the one just past the window, so a lookback at the window's edge resolves.
    edge = samples[-1].ns - live.history_ns
    assert samples[0].ns <= edge < samples[1].ns
    assert live.earlier("BTCUSDT", edge) is samples[0]
    assert live.earlier("BTCUSDT", samples[0].ns - 1) is None
    assert live.earlier("ETHUSDT", edge) is None


def test_continuous_frames_close_a_quiet_symbols_previous_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT")))
    quiet = {"kind": "ticker", "symbol": "QUIETUSDT", "local_receive_ts_ns": BASE_NS}
    active = {"kind": "ticker", "symbol": "BTCUSDT", "local_receive_ts_ns": BASE_NS + HOUR_NS}
    recorder.frames.put(("rows", [quiet], BASE_NS, "wide", 0))
    recorder.frames.put(("rows", [active], BASE_NS + HOUR_NS, "wide", 0))
    recorder.frames.put(None)
    clock = {"wall": BASE_NS, "elapsed": 0}
    get = recorder.frames.get

    def receive(timeout: float) -> Any:
        item = get(timeout=timeout)
        if item is not None:
            clock["wall"] = item[2]
            clock["elapsed"] = item[2] - BASE_NS
        return item

    closed: list[Any] = []
    monkeypatch.setattr(recorder.frames, "get_batch", lambda max_items, timeout: [receive(timeout)])
    monkeypatch.setattr(record.time, "time_ns", lambda: clock["wall"])
    monkeypatch.setattr(record.time, "monotonic_ns", lambda: clock["elapsed"])
    monkeypatch.setattr(recorder.compressor, "submit", closed.append)
    try:
        recorder._write_loop()
        assert [segment.symbol for segment in closed] == ["QUIETUSDT"]
        assert "QUIETUSDT" not in recorder.writer.active
        assert "BTCUSDT" in recorder.writer.active
        assert closed[0].records == 1
        assert json.loads(closed[0].path.read_text()) == quiet
        assert not list(closed[0].path.parent.glob("*.partial"))
    finally:
        recorder.writer.close()


# ----------------------------------------------------------------- coverage


def test_the_hour_roll_writes_the_recorders_own_account_of_the_hour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Everything a cell can be excluded for happens in one hour, from the thread
    that sees it, and lands in `_meta/` where the hourly tar takes it."""

    from market_tape.coverage import CoverageFold
    from market_tape.load import HostRoot, iter_coverage

    recorder = build(
        tmp_path,
        Tier("deep", (Feed("book", "50"), Feed("trades")), Universe("symbols", symbols=("BTCUSDT",))),
        budget=BudgetSettings(monthly_gb=1.0, shed=(("deep", "trades"),)),
    )
    topics = ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"]
    shard = unstarted_shard(recorder, "deep", topics)
    recorder.tier_shards["deep"] = [shard]
    clock = {"now": BASE_NS}
    monkeypatch.setattr(record.time, "time_ns", lambda: clock["now"])
    # The fold opens on the process's own start; this one opens on the test's hour.
    recorder.coverage = CoverageFold(
        venue="bybit",
        market="linear",
        pid=os.getpid(),
        started_at_ns=BASE_NS,
        feeds={"deep": ("book:50", "trades")},
        now_ns=BASE_NS,
    )

    recorder._replan(BASE_NS, apply=False)
    recorder._on_shard_disconnect(shard)
    clock["now"] = BASE_NS + 60 * 10**9
    recorder._on_shard_connect(shard)
    clock["now"] = BASE_NS + 20 * 60 * 10**9
    recorder._on_shard_disconnect(shard)
    clock["now"] = BASE_NS + 25 * 60 * 10**9
    recorder._on_shard_connect(shard)
    clock["now"] = BASE_NS + 30 * 60 * 10**9
    recorder._on_overrun(shard)
    recorder._note_books(
        [{"kind": "orderbook_delta", "symbol": "BTCUSDT", "depth": 50, "sequence_gap": True}],
        BASE_NS + 31 * 60 * 10**9,
    )
    recorder._note_books(
        [{"kind": "orderbook_snapshot", "symbol": "BTCUSDT", "depth": 50}], BASE_NS + 32 * 60 * 10**9
    )
    recorder.coverage.disk_blocked(True, BASE_NS + 40 * 60 * 10**9)
    recorder.coverage.disk_blocked(False, BASE_NS + 41 * 60 * 10**9)
    recorder.meter.add("all", 10**9, BASE_NS + 45 * 60 * 10**9)
    assert recorder.budget.step(BASE_NS + 45 * 60 * 10**9) is True
    recorder.coverage.shed(recorder.budget.shed_active, BASE_NS + 45 * 60 * 10**9)

    clock["now"] = BASE_NS + HOUR_NS
    recorder._roll_coverage(BASE_NS + HOUR_NS)

    written = tmp_path / "2027-01-15" / "08" / "_meta" / "coverage-08-20270115T080000Z.json.zst"
    assert written.is_file() and not list(written.parent.glob("*.json"))
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row for row in receipts if row["kind"] == "coverage_compressed"] == [
        {
            "kind": "coverage_compressed",
            "recorded_at_ns": BASE_NS + HOUR_NS,
            "path": "2027-01-15/08/_meta/coverage-08-20270115T080000Z.json.zst",
            "day": "2027-01-15",
            "hour": "08",
            "from_ns": BASE_NS,
            "to_ns": BASE_NS + HOUR_NS,
            "compressed_bytes": written.stat().st_size,
            "sha256": hashlib.sha256(written.read_bytes()).hexdigest(),
        }
    ]

    (payload,) = list(iter_coverage(HostRoot(tmp_path, "bybit"), ["2027-01-15T08"]))
    assert payload["window"] == {"from_ns": BASE_NS, "to_ns": BASE_NS + HOUR_NS}
    assert payload["hour_span"] == payload["window"]
    assert payload["pid"] == os.getpid() and payload["started_at_ns"] == BASE_NS
    assert payload["tiers"] == [
        {"name": "deep", "feeds": ["book:50", "trades"], "members": {"BTCUSDT": [[BASE_NS, BASE_NS + HOUR_NS]]}}
    ]
    assert payload["shards"] == [
        {"index": 0, "tier": "deep", "topics": topics, "reconnects": 0, "resyncs": 0, "reanchors": 0}
    ]
    assert payload["disconnects"] == [
        {"shard": 0, "tier": "deep", "topics": topics, "from_ns": BASE_NS, "to_ns": BASE_NS + 60 * 10**9},
        {
            "shard": 0,
            "tier": "deep",
            "topics": topics,
            "from_ns": BASE_NS + 20 * 60 * 10**9,
            "to_ns": BASE_NS + 25 * 60 * 10**9,
        },
    ]
    assert payload["overruns"] == [{"shard": 0, "ns": BASE_NS + 30 * 60 * 10**9}]
    assert payload["book_gaps"] == [
        {
            "topic": "orderbook.50.BTCUSDT",
            "symbol": "BTCUSDT",
            "depth": 50,
            "from_ns": BASE_NS + 31 * 60 * 10**9,
            "to_ns": BASE_NS + 32 * 60 * 10**9,
        }
    ]
    assert payload["disk_blocked"] == [[BASE_NS + 40 * 60 * 10**9, BASE_NS + 41 * 60 * 10**9]]
    assert payload["shed"] == [
        {"tier": "deep", "feed": "trades", "from_ns": BASE_NS + 45 * 60 * 10**9, "to_ns": BASE_NS + HOUR_NS}
    ]
    assert payload["counts"] == {
        "received_frames": 0,
        "written_rows": 0,
        "dropped_frames": 1,
        "disk_dropped_frames": 0,
    }

    # The shutdown path writes what it has of the hour it was stopped in.
    clock["now"] = BASE_NS + HOUR_NS + 10 * 60 * 10**9
    recorder._close_coverage(clock["now"])

    (partial,) = list(iter_coverage(HostRoot(tmp_path, "bybit"), ["2027-01-15T09"]))
    assert partial["window"] == {"from_ns": BASE_NS + HOUR_NS, "to_ns": clock["now"]}
    assert partial["hour_span"] == {"from_ns": BASE_NS + HOUR_NS, "to_ns": BASE_NS + 2 * HOUR_NS}
    assert partial["tiers"][0]["members"] == {"BTCUSDT": [[BASE_NS + HOUR_NS, clock["now"]]]}
    assert partial["shed"] == [
        {"tier": "deep", "feed": "trades", "from_ns": BASE_NS + HOUR_NS, "to_ns": clock["now"]}
    ], "a span still open carries into the next hour"
    assert partial["disconnects"] == [] and partial["book_gaps"] == []


def test_the_writer_reads_the_host_clock_against_the_venues_and_the_status_reports_it(tmp_path: Path) -> None:
    """Every stamped frame is one reading of this host's clock against the
    venue's gateway; the status write takes the window and opens a fresh one."""

    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    sent_ms = BASE_NS // 1_000_000
    # The reader's monotonic stamp, far enough behind the writer's own
    # monotonic clock that every frame reads as having waited in the queue.
    mono = time.monotonic_ns() - 20_000_000
    for skew_ms in (4, 9, -2):
        frame = json.dumps(
            {
                "topic": "tickers.BTCUSDT",
                "type": "delta",
                "ts": sent_ms,
                "data": {"symbol": "BTCUSDT", "lastPrice": "100"},
            }
        )
        recorder.frames.put(("frame", frame, BASE_NS + skew_ms * 1_000_000, "wide", mono))
    trade = json.dumps(
        {"topic": "publicTrade.BTCUSDT", "ts": sent_ms, "data": [{"s": "BTCUSDT", "S": "Buy", "p": "1", "v": "1", "i": "t1", "T": sent_ms}]}
    )
    # The message that carries a print has a send stamp of its own: a clock reading too.
    recorder.frames.put(("frame", trade, BASE_NS + 50 * 1_000_000, "wide", mono))
    # A side lane's row was never on a socket: no monotonic stamp, no queue reading.
    recorder.frames.put(("rows", [{"kind": "funding_settlement", "venue": "bybit", "symbol": "BTCUSDT",
                                    "local_receive_ts_ns": BASE_NS, "funding_time_ms": 1, "funding_rate": 0.0}], BASE_NS, "lanes", 0))
    recorder.frames.put(None)
    recorder._write_loop()

    recorder._write_status()
    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    # The print's feed reads on its own: 50 ms from its message's send stamp,
    # and nothing between the print and the send.
    trades = {"samples": 1, "skew_p50_ms": 50.0, "skew_p99_ms": 50.0, "venue_p50_ms": 0.0, "venue_p99_ms": 0.0}
    assert payload["clock"] == {
        "samples": 4, "skew_mean_ms": 15.25, "skew_min_ms": -2.0, "skew_max_ms": 50.0, "feeds": {"trades": trades}
    }
    # Four frames waited; the wait is read on the writer's monotonic clock
    # against the reader's stamp, so it is at least the 20 ms planted.
    wait = payload["queue_wait"]
    assert wait["samples"] == 4
    assert 20.0 <= wait["wait_min_ms"] <= wait["wait_mean_ms"] <= wait["wait_max_ms"] < 20_000.0
    assert payload["fast_json"] in (True, False)
    # The windows were taken; the next status write starts from nothing.
    recorder._write_status()
    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert payload["clock"] == {"samples": 0, "skew_mean_ms": None, "skew_min_ms": None, "skew_max_ms": None, "feeds": {}}
    assert payload["queue_wait"] == {"samples": 0, "wait_mean_ms": None, "wait_min_ms": None, "wait_max_ms": None}


def test_a_socket_frame_reaches_the_writer_with_both_host_clocks_and_every_row_carries_them(tmp_path: Path) -> None:
    """The reader stamps a frame with the wall clock and the monotonic clock;
    the adapter puts both on every row of the frame."""

    recorder = build(tmp_path, Tier("deep", DEEP_FEEDS, Universe("symbols", symbols=("BTCUSDT",))))
    seen: list[dict[str, Any]] = []
    recorder.writer.append = lambda row: seen.append(dict(row)) or []  # type: ignore[method-assign]
    sent_ms = BASE_NS // 1_000_000
    frame = json.dumps(
        {"topic": "publicTrade.BTCUSDT", "ts": sent_ms, "data": [
            {"s": "BTCUSDT", "S": "Buy", "p": "1", "v": "1", "i": "t1", "T": sent_ms - 3},
            {"s": "BTCUSDT", "S": "Sell", "p": "1", "v": "2", "i": "t2", "T": sent_ms - 1},
        ]}
    )
    recorder.frames.put(("frame", frame, BASE_NS + 7_000_000, "deep", 123_456_789))
    recorder.frames.put(None)
    recorder._write_loop()

    assert [row["trade_id"] for row in seen] == ["t1", "t2"]
    for row in seen:
        assert row["local_receive_ts_ns"] == BASE_NS + 7_000_000
        assert row["local_receive_mono_ns"] == 123_456_789
        assert row["exchange_system_ts_ns"] == sent_ms * 1_000_000
    assert [row["exchange_ts_ns"] for row in seen] == [(sent_ms - 3) * 1_000_000, (sent_ms - 1) * 1_000_000]


def test_a_tier_with_a_lane_feed_starts_the_adapters_lane(tmp_path: Path) -> None:
    """The plan hands every symbol's feeds to the adapter; a lane feed with no
    topic subscribes nothing and starts the REST lane instead."""

    recorder = build(
        tmp_path,
        Tier("deep", (Feed("trades"), Feed("funding"), Feed("account_ratio")), Universe("symbols", symbols=("BTCUSDT",))),
    )
    handed: list[tuple[dict[str, tuple[Feed, ...]], threading.Event]] = []

    def start_lanes(feeds_by_symbol: dict[str, tuple[Feed, ...]], emit: Any, stop: threading.Event) -> list[Any]:
        handed.append((dict(feeds_by_symbol), stop))
        assert emit == recorder.emit
        return []

    recorder.adapter.start_lanes = start_lanes  # type: ignore[method-assign]
    recorder._refresh(BASE_NS, restart=False)
    assert recorder.tier_topics["deep"] == ["publicTrade.BTCUSDT"]
    assert recorder.feeds_by_symbol == {"BTCUSDT": (Feed("account_ratio"), Feed("funding"), Feed("trades"))}
    recorder._start_lanes()
    assert handed == [({"BTCUSDT": (Feed("account_ratio"), Feed("funding"), Feed("trades"))}, recorder.lane_stop)]
    assert not recorder.lane_stop.is_set()
    # A replan that changes a symbol's feeds restarts the lane on a fresh stop.
    previous_stop = recorder.lane_stop
    recorder._restart_lanes()
    assert previous_stop.is_set() and not recorder.lane_stop.is_set()
    assert len(handed) == 2 and handed[1][1] is recorder.lane_stop


def test_a_backlog_drained_past_the_hour_closes_each_of_its_segments_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The host clock is already in the next hour while the queue still holds this
    hour's rows: the rolls follow the rows written, so each symbol's hour closes
    once, when the stream reaches the next hour, not on every roll of the drain."""

    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT")))
    symbols = ("AUSDT", "BUSDT", "CUSDT")
    for second in range(14):
        stamp = BASE_NS + HOUR_NS - (14 - second) * 10**9
        rows = [{"kind": "ticker", "symbol": symbol, "local_receive_ts_ns": stamp} for symbol in symbols]
        recorder.frames.put(("rows", rows, stamp, "wide", 0))
    later = BASE_NS + HOUR_NS + 10**9
    recorder.frames.put(("rows", [{"kind": "ticker", "symbol": "AUSDT", "local_receive_ts_ns": later}], later, "wide", 0))
    recorder.frames.put(None)
    clock = {"mono": 0}
    get = recorder.frames.get

    def receive(timeout: float) -> Any:
        clock["mono"] += 11 * 10**9  # a roll is due before every item
        return get(timeout=timeout)

    closed: list[Any] = []
    monkeypatch.setattr(recorder.frames, "get_batch", lambda max_items, timeout: [receive(timeout)])
    monkeypatch.setattr(record.time, "time_ns", lambda: BASE_NS + HOUR_NS + 20 * 60 * 10**9)
    monkeypatch.setattr(record.time, "monotonic_ns", lambda: clock["mono"])
    monkeypatch.setattr(recorder.compressor, "submit", closed.append)
    try:
        recorder._write_loop()
        assert sorted((segment.symbol, segment.hour, segment.records) for segment in closed) == [
            (symbol, "08", 14) for symbol in symbols
        ]
    finally:
        recorder.writer.close()


def test_a_drained_quiet_recorder_closes_the_hour_soon_after_it_ends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing arrives after the hour's last row, so the stream cursor stops there;
    with the queue empty the rolls follow the host clock and the hour still closes."""

    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT")))
    last = BASE_NS + HOUR_NS - 10**9
    recorder.frames.put(("rows", [{"kind": "ticker", "symbol": "AUSDT", "local_receive_ts_ns": last}], last, "wide", 0))
    clock = {"wall": last, "mono": 0}
    get = recorder.frames.get

    def receive(timeout: float) -> Any:
        if not recorder.frames.empty():
            return get(timeout=timeout)
        if clock["wall"] >= BASE_NS + HOUR_NS + 60 * 10**9:
            return None
        clock["wall"] += 10**9
        clock["mono"] += 10**9
        raise queue.Empty

    closed_at: list[int] = []
    monkeypatch.setattr(recorder.frames, "get_batch", lambda max_items, timeout: [receive(timeout)])
    monkeypatch.setattr(record.time, "time_ns", lambda: clock["wall"])
    monkeypatch.setattr(record.time, "monotonic_ns", lambda: clock["mono"])
    monkeypatch.setattr(recorder.compressor, "submit", lambda _segment: closed_at.append(clock["wall"]))
    recorder._write_loop()

    assert len(closed_at) == 1
    assert BASE_NS + HOUR_NS <= closed_at[0] <= BASE_NS + HOUR_NS + 20 * 10**9
    assert recorder.writer.active == {}


def test_one_drain_wakes_every_shard_its_room_admits() -> None:
    """Two shards wait on a full queue and one drain makes room for both: both go
    in at once, neither sits out its timeout beside room that is there."""

    frames = record.FrameQueue(4)
    frames.put_batch([0, 1, 2, 3])
    shards = [threading.Thread(target=frames.put_batch, args=([10 + n, 20 + n], 2.0)) for n in range(2)]
    for shard in shards:
        shard.start()
    deadline = time.monotonic() + 2.0
    while len(frames.not_full._waiters) < 2:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    drained_at = time.monotonic()
    assert frames.get_batch(max_items=10, timeout=0.1) == [0, 1, 2, 3]
    for shard in shards:
        shard.join()
    assert time.monotonic() - drained_at < 1.0
    assert sorted(frames.get_batch(max_items=10, timeout=0.1)) == [10, 11, 20, 21]


def test_a_batch_larger_than_the_queue_goes_in_as_the_writer_makes_room() -> None:
    frames = record.FrameQueue(8)
    taken: list[int] = []
    done = threading.Event()

    def writer() -> None:
        while not done.is_set():
            try:
                taken.extend(frames.get_batch(max_items=3, timeout=0.05))
            except queue.Empty:
                pass

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        frames.put_batch(list(range(20)), timeout=2.0)
        deadline = time.monotonic() + 2.0
        while len(taken) < 20 and time.monotonic() < deadline:
            time.sleep(0.001)
    finally:
        done.set()
        thread.join()
    assert taken == list(range(20))


def test_many_shards_through_a_small_queue_lose_duplicate_and_reorder_nothing() -> None:
    capacity, shards, per_shard = 16, 8, 400
    frames = record.FrameQueue(capacity)
    failures: list[BaseException] = []

    def shard(index: int) -> None:
        sizes = random.Random(index)
        sent = 0
        try:
            while sent < per_shard:
                # Up to 2.5 batches of the whole queue: a shard hands over 64 frames at most.
                size = min(sizes.randint(1, 40), per_shard - sent)
                frames.put_batch([(index, sent + n) for n in range(size)], timeout=5.0)
                sent += size
        except BaseException as exc:  # reported below, on the test's thread
            failures.append(exc)

    threads = [threading.Thread(target=shard, args=(index,), daemon=True) for index in range(shards)]
    for thread in threads:
        thread.start()
    seen: dict[int, list[int]] = {index: [] for index in range(shards)}
    sizes = random.Random(-1)
    received = 0
    deadline = time.monotonic() + 20.0
    while received < shards * per_shard and time.monotonic() < deadline and not failures:
        try:
            batch = frames.get_batch(max_items=sizes.randint(1, 50), timeout=0.5)
        except queue.Empty:
            continue
        assert len(batch) <= capacity
        for index, sequence in batch:
            seen[index].append(sequence)
        received += len(batch)
    for thread in threads:
        thread.join(5.0)
    assert failures == []
    assert not any(thread.is_alive() for thread in threads)
    assert seen == {index: list(range(per_shard)) for index in range(shards)}


def _frame_item(size: int) -> tuple[str, bytes, int, str, int]:
    return ("frame", b"x" * size, BASE_NS, "deep", 1)


#: A queued 1,000-byte frame, as the byte bound counts it.
FRAME_ITEM_BYTES = record.QUEUE_ITEM_OVERHEAD_BYTES + 1_000


def test_an_item_counts_its_payload_and_the_python_that_holds_it() -> None:
    raw = b"x" * 220
    item = ("frame", raw, time.time_ns(), "deep", time.monotonic_ns())
    # The tuple, the bytes header, both clocks and the deque's slot.
    held = sys.getsizeof(item) + sys.getsizeof(raw) - len(raw) + sys.getsizeof(item[2]) + sys.getsizeof(item[4]) + 8
    assert held <= record.QUEUE_ITEM_OVERHEAD_BYTES
    assert record.queue_item_bytes(item) == record.QUEUE_ITEM_OVERHEAD_BYTES + 220
    # The intake's form: the reader's event, header and payload in one bytes
    # object, whose header, allocator rounding and slot are the overhead.
    event = reader.EVENT.pack(len(raw), reader.FRAME, 1, time.time_ns(), time.monotonic_ns()) + raw
    assert sys.getsizeof(event) - len(event) + 15 + 8 <= record.QUEUE_EVENT_OVERHEAD_BYTES
    assert record.queue_item_bytes(event) == record.QUEUE_EVENT_OVERHEAD_BYTES + reader.EVENT.size + 220
    assert record.received_ns_of(event) == reader.EVENT.unpack_from(event)[3]
    rows = [{"kind": "funding_settlement"}, {"kind": "funding_settlement"}]
    assert record.queue_item_bytes(("rows", rows, BASE_NS, record.LANES, 0)) == (
        record.QUEUE_ITEM_OVERHEAD_BYTES + 2 * record.QUEUE_ROW_BYTES
    )
    assert record.queue_item_bytes(None) == 0


def test_the_queue_holds_back_on_bytes_with_frames_to_spare() -> None:
    """Room for a hundred frames and three frames' bytes: of a five-frame batch
    three go in, the other two wait out the timeout and are counted."""

    frames = record.FrameQueue(100, 3 * FRAME_ITEM_BYTES)
    batch = [_frame_item(1_000) for _ in range(5)]
    with pytest.raises(record.Overrun) as overrun:
        frames.put_batch(batch, timeout=0.05)
    assert overrun.value.dropped == 2
    assert frames.qsize() == 3 and frames.queued_bytes == 3 * FRAME_ITEM_BYTES
    assert frames.get_batch(max_items=10, timeout=0.1) == batch[:3]
    assert frames.queued_bytes == 0


def test_a_drain_releases_the_bytes_a_waiting_shard_needs() -> None:
    frames = record.FrameQueue(100, 2 * FRAME_ITEM_BYTES)
    frames.put_batch([_frame_item(1_000), _frame_item(1_000)])
    late = _frame_item(1_000)
    failures: list[BaseException] = []

    def shard() -> None:
        try:
            frames.put_batch([late], timeout=5.0)
        except BaseException as exc:  # reported below, on the test's thread
            failures.append(exc)

    thread = threading.Thread(target=shard)
    thread.start()
    deadline = time.monotonic() + 2.0
    while not frames.not_full._waiters:
        assert time.monotonic() < deadline
        time.sleep(0.001)
    drained_at = time.monotonic()
    assert len(frames.get_batch(max_items=10, timeout=0.1)) == 2
    thread.join(5.0)
    assert failures == [] and time.monotonic() - drained_at < 1.0
    assert frames.qsize() == 1 and frames.queued_bytes == FRAME_ITEM_BYTES
    # `Queue.get` releases what it takes too.
    assert frames.get(timeout=0.1) == late and frames.queued_bytes == 0


def test_a_frame_larger_than_the_byte_bound_goes_into_an_empty_queue_alone() -> None:
    frames = record.FrameQueue(10, FRAME_ITEM_BYTES)
    big = _frame_item(5_000)
    frames.put_batch([big], timeout=0.05)
    assert frames.qsize() == 1 and frames.queued_bytes == record.QUEUE_ITEM_OVERHEAD_BYTES + 5_000
    with pytest.raises(record.Overrun) as overrun:
        frames.put_batch([_frame_item(10)], timeout=0.05)
    assert overrun.value.dropped == 1
    assert frames.get_batch(max_items=10, timeout=0.1) == [big]
    assert frames.queued_bytes == 0


def test_a_lane_row_that_finds_the_byte_bound_full_is_an_overrun(tmp_path: Path) -> None:
    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    recorder.frames = record.FrameQueue(16, FRAME_ITEM_BYTES)
    recorder.frames.put_batch([_frame_item(1_000)])
    row = {"kind": "funding_settlement", "venue": "bybit", "symbol": "BTCUSDT", "local_receive_ts_ns": BASE_NS}

    recorder.emit(row)
    assert recorder.dropped_frames == 1 and recorder.frames.qsize() == 1

    recorder.frames.get_batch(max_items=10, timeout=0.1)
    recorder.emit(row)
    assert recorder.dropped_frames == 1
    assert recorder.frames.queued_bytes == record.QUEUE_ITEM_OVERHEAD_BYTES + record.QUEUE_ROW_BYTES


def test_a_wall_clock_step_during_a_connection_does_not_reset_the_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """How long a connection lived is elapsed time: a clock the host steps an
    hour forward mid-connection is not an hour of healthy stream."""

    recorder = build(tmp_path, Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))))
    shard = unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT"])
    shard.backoff_seconds = 16.0
    wall = {"ns": BASE_NS}

    def connect_while_the_clock_steps() -> None:
        wall["ns"] += HOUR_NS

    class OneWait(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            self.set()
            return True

    shard.stop = OneWait()
    monkeypatch.setattr(record.time, "time_ns", lambda: wall["ns"])
    monkeypatch.setattr(shard, "_connect_once", connect_while_the_clock_steps)
    shard._run()

    assert shard.backoff_seconds == 32.0


def test_an_idle_roll_that_raises_leaves_the_writer_writing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = build(tmp_path, Tier("wide", WIDE_FEEDS, Universe("listed", quote="USDT")))
    clock = {"mono": 0}

    def broken_roll(_now_ns: int) -> list[Any]:
        raise ValueError("I/O operation on closed file")

    items = [("rows", [{"kind": "ticker", "symbol": "AUSDT", "local_receive_ts_ns": BASE_NS + n}], BASE_NS + n, "wide", 0) for n in range(3)]

    def one_at_a_time(max_items: int, timeout: float) -> list[Any]:
        clock["mono"] += 11 * 10**9  # an idle roll is due before every item
        return [items.pop(0) if items else None]

    monkeypatch.setattr(recorder.writer, "roll_idle", broken_roll)
    monkeypatch.setattr(recorder.frames, "get_batch", one_at_a_time)
    monkeypatch.setattr(record.time, "monotonic_ns", lambda: clock["mono"])
    try:
        recorder._write_loop()
        assert recorder.written_rows == 3
    finally:
        recorder.writer.close()


def test_a_shard_the_reader_overruns_is_counted_received_and_dropped_and_ends_once_its_frames_are_queued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader_in_thread: Any
) -> None:
    """A venue flooding a shard while the writer takes nothing: the queue is
    full, the intake waits on it, the reader holds the shard's frames to its
    bound and, past the patience, drops them and closes the connection. Every
    frame it read is counted: queued once the writer makes room, or dropped;
    and the link ends only after the frames read before the drop are in the
    queue."""

    from market_tape import reader as tape_reader
    from websockets.frames import Frame, Opcode
    from websockets.server import ServerProtocol

    payload = b'{"topic":"publicTrade.BTCUSDT","data":[]}' + b" " * 1_000
    frame = Frame(Opcode.TEXT, payload).serialize(mask=False)
    listener = sockets.create_server(("127.0.0.1", 0))
    #: Set when the venue's stream ends, which only the recorder's close does.
    venue_ended = threading.Event()
    ended_with: list[OSError] = []

    def send(end: sockets.socket, data: bytes) -> None:
        view = memoryview(data)
        while view:
            try:
                view = view[end.send(view) :]
            except OSError as exc:
                # A macOS host out of socket buffers refuses a send (ENOBUFS)
                # and keeps none of one this small: a venue's TCP waits, it
                # does not hang up.
                if exc.errno != errno.ENOBUFS:
                    raise

    def venue() -> None:
        try:
            end, _ = listener.accept()
            with end:
                protocol = ServerProtocol()
                while not (requests := protocol.events_received()):
                    data = end.recv(65536)
                    if not data:
                        return
                    protocol.receive_data(data)
                protocol.send_response(protocol.accept(requests[0]))
                send(end, b"".join(protocol.data_to_send()))
                while True:
                    send(end, frame)
        except OSError as exc:
            ended_with.append(exc)
        finally:
            venue_ended.set()

    monkeypatch.setattr(tape_reader, "QUEUE_PUT_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(tape_reader, "SOCKET_QUEUE_FRAMES", 256)
    with listener:
        threading.Thread(target=venue, daemon=True).start()
        port = listener.getsockname()[1]
        config = CaptureConfig(
            venue=VenueSettings("bybit", "linear"),
            storage=StorageSettings(root=tmp_path, queue_frames=16),
            tiers=(Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),),
        )
        recorder = Recorder(config, adapter=BybitAdapter(ws_url=f"ws://127.0.0.1:{port}", rest_url="http://unused"))
        recorder.reader = record.ReaderProcess(recorder.frames, spawn=reader_in_thread)
        recorder.reader.start()
        shard = unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT"])
        recorder.frames.put_batch([("rows", [], BASE_NS, "deep", 0)] * 16)
        taken: list[Any] = []

        def writer_back_once_the_venue_is_cut_off() -> None:
            venue_ended.wait()
            while not shard.stop.is_set():
                try:
                    taken.extend(recorder.frames.get_batch(max_items=1000, timeout=0.05))
                except queue.Empty:
                    pass

        drain = threading.Thread(target=writer_back_once_the_venue_is_cut_off, daemon=True)
        drain.start()
        started = time.monotonic()
        try:
            shard._connect_once()
        finally:
            took = time.monotonic() - started
            shard.stop.set()
            drain.join(5.0)
            recorder.reader.close()
    taken.extend(recorder.frames.get_batch(max_items=10_000, timeout=0.1) if recorder.frames.qsize() else [])

    frames = [item for item in taken if type(item) is bytes]
    assert took < 5.0, f"the overrun's close took {took:.1f}s"
    assert recorder.dropped_frames >= 256, f"the venue's stream ended with {ended_with}"
    assert recorder.received_frames == len(frames) + recorder.dropped_frames
    assert all(item[tape_reader.EVENT.size :] == payload for item in frames)


def test_a_reader_that_exits_unasked_ends_its_links_and_every_shard_reconnects_through_a_new_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Killed, the reader is started again, once past a start that itself fails."""

    import signal as signals

    from websockets.sync.server import serve

    def ticking(connection: Any) -> None:
        connection.recv()
        try:
            for n in range(10_000):
                connection.send(b'{"topic":"publicTrade.BTCUSDT","data":[],"n":%d}' % n)
                time.sleep(0.02)
        except Exception:  # the reader went away
            return

    monkeypatch.setattr(record, "RECONNECT_BACKOFF_MIN_SECONDS", 0.2)
    monkeypatch.setattr(record, "READER_RESPAWN_SECONDS", 0.1)
    with serve(ticking, "127.0.0.1", 0) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.socket.getsockname()[1]
        config = CaptureConfig(
            venue=VenueSettings("bybit", "linear"),
            storage=StorageSettings(root=tmp_path),
            tiers=(Tier("deep", (Feed("trades"),), Universe("symbols", symbols=("BTCUSDT",))),),
        )
        recorder = Recorder(config, adapter=BybitAdapter(ws_url=f"ws://127.0.0.1:{port}", rest_url="http://unused"))
        starts: list[str] = []

        def spawn(commands: int, events: int) -> Any:
            starts.append("start")
            if len(starts) == 2:
                os.close(commands)
                os.close(events)
                raise OSError("fork: Resource temporarily unavailable")
            return record.spawn_reader(commands, events)

        recorder.reader = record.ReaderProcess(recorder.frames, spawn=spawn)
        recorder.reader.start()
        shards = [unstarted_shard(recorder, "deep", ["publicTrade.BTCUSDT"], index=n) for n in range(2)]
        for shard in shards:
            shard.start()
        try:
            first = recorder.reader.pid
            assert first is not None
            deadline = time.monotonic() + 10.0
            while recorder.received_frames < 20 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert recorder.received_frames >= 20
            os.kill(first, signals.SIGKILL)
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline and not (
                all(shard.reconnects >= 1 and shard.connected for shard in shards) and recorder.reader.pid != first
            ):
                time.sleep(0.05)
            assert recorder.reader.pid not in (None, first), "the reader was not started again"
            assert len(starts) == 3, starts
            assert all(shard.reconnects >= 1 and shard.connected for shard in shards)
            before = recorder.received_frames
            deadline = time.monotonic() + 10.0
            while recorder.received_frames < before + 20 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert recorder.received_frames >= before + 20, "frames did not resume through the new reader"
        finally:
            for shard in shards:
                shard.close()
            for shard in shards:
                shard.join()
            recorder.reader.close()
            server.shutdown()
