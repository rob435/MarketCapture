"""The recorder on Binance USD-M, end to end: a scripted venue over real sockets, read back as tape.

The real `Recorder` — its reader process, shards, writer, compressor and
coverage fold — records a local venue that routes streams by URL path as
Binance does (`/stream` carries `bookTicker` and `trade`, `/market/stream`
the rest, each dropping the other's silently), subscribes by frames on the
open socket, refuses a stream it does not list, and serves the REST tables
`listed` resolves from. What it holds the recorder to:

- every listed USDT perpetual's top of book and prints arrive, on the path
  that carries them, subscribed at most 100 streams a frame and no faster
  than the venue's ten messages a second;
- the rows read back through `market_tape.load` as the venue sent them, and
  the coverage ledger answers for them through the Binance adapter's topics;
- a refused subscription is counted in `status.json` and carries no rows.
"""

from __future__ import annotations

import http.server
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import ServerConnection, serve

from market_tape import pack
from market_tape.config import CaptureConfig, Feed, StorageSettings, Tier, Universe, VenueSettings
from market_tape.coverage import build_ledger
from market_tape.load import HostRoot, iter_coverage, iter_rows
from market_tape.record import Recorder, Shard
from market_tape.storage import utc_day_hour
from market_tape.venues.binance import BinanceAdapter


INSTRUMENTS = [
    {"symbol": name, "contractType": kind, "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"}
    for name, kind in (
        ("BTCUSDT", "PERPETUAL"),
        ("ETHUSDT", "PERPETUAL"),
        ("GONEUSDT", "PERPETUAL"),
        ("NVDAUSDT", "TRADIFI_PERPETUAL"),
    )
]
#: Listed, but the venue refuses its streams.
REFUSED = "goneusdt"
PATHS = {"/stream": ("bookTicker", "trade"), "/market/stream": ("markPrice@1s", "ticker")}


@dataclass
class Venue:
    """What the scripted venue saw: every request by path, when it came, and what it streamed."""

    requests: list[tuple[str, float, dict[str, Any]]] = field(default_factory=list)
    sent: dict[str, int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    stop: threading.Event = field(default_factory=threading.Event)

    def handle(self, connection: ServerConnection) -> None:
        path = connection.request.path if connection.request is not None else ""
        carried = PATHS.get(path, ())
        streams: set[str] = set()
        closed = threading.Event()

        def listen() -> None:
            try:
                for raw in connection:
                    message = json.loads(raw)
                    with self.lock:
                        self.requests.append((path, time.monotonic(), message))
                    params = list(message.get("params") or [])
                    if any(stream.startswith(REFUSED) for stream in params):
                        connection.send(json.dumps({"error": {"code": 2, "msg": "Invalid request"}, "id": message["id"]}))
                        continue
                    # A stream this path does not carry is taken and never sent, as the venue does.
                    kept = {stream for stream in params if stream.partition("@")[2] in carried}
                    if message.get("method") == "SUBSCRIBE":
                        streams.update(kept)
                    else:
                        streams.difference_update(params)
                    connection.send(json.dumps({"result": None, "id": message["id"]}))
            except ConnectionClosed:
                pass
            finally:
                closed.set()

        threading.Thread(target=listen, daemon=True).start()
        update = 0
        try:
            while not closed.is_set() and not self.stop.is_set():
                update += 1
                now_ms = time.time_ns() // 1_000_000
                for stream in sorted(streams):
                    pair, _, kind = stream.partition("@")
                    symbol = pair.upper()
                    if kind == "bookTicker":
                        data = {"e": "bookTicker", "u": 1_000_000 + update, "s": symbol, "ps": symbol, "b": f"{100 + update % 7}.10",
                                "B": "1.5", "a": f"{100 + update % 7}.20", "A": "2.5", "T": now_ms - 1, "E": now_ms, "st": 1}
                    else:
                        data = {"e": "trade", "E": now_ms, "T": now_ms - 1, "s": symbol, "t": 500 + update, "p": "100.15",
                                "q": "0.25", "X": "MARKET", "m": update % 2 == 0, "st": 1}
                    connection.send(json.dumps({"stream": stream, "data": data}))
                    with self.lock:
                        self.sent[stream] = self.sent.get(stream, 0) + 1
                time.sleep(0.005)
        except ConnectionClosed:
            pass


class Tables(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.partition("?")[0]
        payload: Any = {
            "/fapi/v1/exchangeInfo": {"symbols": INSTRUMENTS},
            "/fapi/v1/premiumIndex": [{"symbol": row["symbol"], "lastFundingRate": "0.0001"} for row in INSTRUMENTS],
            "/fapi/v1/ticker/24hr": [{"symbol": row["symbol"], "quoteVolume": "1000"} for row in INSTRUMENTS],
            "/fapi/v1/ticker/bookTicker": [],
        }.get(path)
        body = json.dumps(payload if payload is not None else {"code": -1, "msg": "no such path"}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: Any) -> None:
        return None


def test_the_recorder_records_binance_top_of_book_and_prints_on_the_live_readers_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    venue = Venue()
    rest = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Tables)
    threading.Thread(target=rest.serve_forever, daemon=True).start()
    root = tmp_path / "forward-market-binance"
    with serve(venue.handle, "127.0.0.1", 0) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.socket.getsockname()[1]
        config = CaptureConfig(
            venue=VenueSettings("binance", "usdm", ws_url=f"ws://127.0.0.1:{port}", rest_url=f"http://127.0.0.1:{rest.server_port}"),
            storage=StorageSettings(root=root, segment_max_mb=1.0, status_interval_seconds=0.3, min_free_disk_gb=0.01),
            tiers=(Tier("usdt_perps", (Feed("book", "1"), Feed("trades")), Universe("listed", quote="USDT")),),
            topics_per_connection=4,
            snapshot_cadence="hour",
        )
        recorder = Recorder(config)
        monkeypatch.setattr(recorder, "_install_signals", lambda: None)

        def stop_once_recorded() -> None:
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline:
                with venue.lock:
                    streaming = {stream for stream, count in venue.sent.items() if count >= 50}
                if len(streaming) >= 4 and recorder.written_rows >= 400:
                    break
                time.sleep(0.05)
            recorder.stop.set()

        threading.Thread(target=stop_once_recorded, daemon=True).start()
        recorder.run()
        venue.stop.set()
        server.shutdown()
    rest.shutdown()

    # What the venue saw: the listed perpetuals' streams on `/stream` alone,
    # the stock perpetual never, a hundred streams a frame at most, and no
    # connection's frames closer than the venue's ten a second allow.
    paths = {path for path, _, _ in venue.requests}
    assert paths == {"/stream"}
    subscribed = {stream for _, _, message in venue.requests for stream in message["params"]}
    assert subscribed == {f"{pair}@{kind}" for pair in ("btcusdt", "ethusdt", "goneusdt") for kind in ("bookTicker", "trade")}
    assert all(len(message["params"]) <= 100 for _, _, message in venue.requests)
    assert {stream for stream, count in venue.sent.items() if count} == {
        f"{pair}@{kind}" for pair in ("btcusdt", "ethusdt") for kind in ("bookTicker", "trade")
    }

    status = json.loads((root / "status.json").read_text(encoding="utf-8"))
    assert (status["venue"], status["market"]) == ("binance", "usdm")
    assert status["malformed_frames"] == 0 and status["dropped_frames"] == 0
    assert status["tiers"][0]["symbols"] == 3 and status["tiers"][0]["topics"] == 6
    # Two shards of four topics at most, each on one path.
    assert sorted(shard["topics"] for shard in status["shards"]) == [2, 4]

    # The stopped recorder left its open segments raw; the packer's recovery finishes them.
    recovered = pack.recover_idle_root(pack.Tape("binance-usdm", root, "box:x"), timeout=60.0, dry_run=False)
    assert recovered["failed"] == 0
    source = HostRoot(root)
    assert source.venue == "binance"
    hours = source.hours()
    rows = list(iter_rows(source, hours, typed=False))
    books = [row for row in rows if row["kind"] == "orderbook_snapshot"]
    trades = [row for row in rows if row["kind"] == "public_trade"]
    assert {row["symbol"] for row in books} == {row["symbol"] for row in trades} == {"BTCUSDT", "ETHUSDT"}
    assert all(row["depth"] == 1 and not row["sequence_gap"] for row in books)
    for symbol in ("BTCUSDT", "ETHUSDT"):
        ids = [row["update_id"] for row in books if row["symbol"] == symbol]
        assert ids == sorted(ids) and len(set(ids)) == len(ids), symbol
    # Both host clocks, from the reader's socket read, on every row; the venue's two on every book.
    assert all(row["local_receive_mono_ns"] > 0 and row["local_receive_ts_ns"] > 0 for row in rows)
    assert all(row["exchange_engine_ts_ns"] == row["exchange_system_ts_ns"] - 1_000_000 for row in books)
    assert rows == sorted(rows, key=lambda row: row["local_receive_ts_ns"])

    # The ledger answers each (hour, symbol, feed) cell from the recorder's own
    # record, through the Binance topics: granted, and excluded only for the
    # hour the recorder was not up for and the moments before its sockets opened.
    ledger = build_ledger(source, hours, symbols=["BTCUSDT", "ETHUSDT", "GONEUSDT"], feeds=["book:1", "trades"], read_rows=True)
    for cell in ledger.cells:
        assert cell.status == "excluded", cell
        assert set(cell.reason_codes) <= {"recorder_down", "tier_membership_partial", "shard_disconnected"}, cell
    counted = {(cell.symbol, cell.feed): cell.records for cell in ledger.cells}
    assert counted[("BTCUSDT", "book:1")] > 0 and counted[("ETHUSDT", "trades")] > 0
    assert counted[("GONEUSDT", "book:1")] == 0
    for coverage in iter_coverage(source, hours):
        carried = {topic for shard in coverage["shards"] for topic in shard["topics"]}
        assert {"btcusdt@bookTicker", "btcusdt@trade"} <= carried


# ------------------------------------------------------------- shards


class FakeLink:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send_text(self, text: str) -> None:
        self.sent.append(text)

    def request_close(self) -> None:
        return None


def _recorder(tmp_path: Path, *feeds: Feed, per_connection: int = 3) -> Recorder:
    config = CaptureConfig(
        venue=VenueSettings("binance", "usdm"),
        storage=StorageSettings(root=tmp_path, queue_frames=16, status_interval_seconds=30.0),
        tiers=(Tier("core", feeds, Universe("symbols", symbols=("BTCUSDT", "ETHUSDT"))),),
        topics_per_connection=per_connection,
    )
    return Recorder(config, adapter=BinanceAdapter(rest_url="http://unused"))


def _shard(recorder: Recorder, topics: list[str], index: int = 0) -> Shard:
    return Shard(
        index=index,
        tier="core",
        topics=list(topics),
        adapter=recorder.adapter,
        reader=recorder.reader,
        on_frame=recorder._on_frame,
        on_overrun=recorder._on_overrun,
    )


def test_binance_shards_carry_one_path_each_and_live_adds_stay_on_their_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = _recorder(tmp_path, Feed("book", "1"), Feed("trades"), Feed("ticker"))
    created: list[Shard] = []

    def quiet_shard(tier: str, topics: list[str]) -> Shard:
        shard = _shard(recorder, topics, index=len(created))
        created.append(shard)
        return shard

    monkeypatch.setattr(recorder, "_new_shard", quiet_shard)
    topics, _ = recorder.plan_topics({"core": ["BTCUSDT"]})
    recorder._reconcile_tier("core", topics["core"])
    plans = [shard.topics for shard in recorder.tier_shards["core"]]
    assert plans == [["btcusdt@bookTicker", "btcusdt@trade"], ["btcusdt@markPrice@1s", "btcusdt@ticker"]]
    for shard in recorder.tier_shards["core"]:
        recorder.adapter.connection_url(shard.topics)  # one path each, or this raises

    # ETH arrives live: its top of book and prints join the public shard's
    # free room and spill into a new public shard; its market streams fill the market shard.
    topics, _ = recorder.plan_topics({"core": ["BTCUSDT", "ETHUSDT"]})
    recorder._reconcile_tier("core", topics["core"])
    plans = [shard.topics for shard in recorder.tier_shards["core"]]
    assert plans == [
        ["btcusdt@bookTicker", "btcusdt@trade", "ethusdt@bookTicker"],
        ["btcusdt@markPrice@1s", "btcusdt@ticker", "ethusdt@markPrice@1s"],
        ["ethusdt@trade"],
        ["ethusdt@ticker"],
    ]
    assert [recorder.adapter.connection_url(plan) for plan in plans] == [
        "wss://fstream.binance.com/stream",
        "wss://fstream.binance.com/market/stream",
        "wss://fstream.binance.com/stream",
        "wss://fstream.binance.com/market/stream",
    ]


def test_the_hourly_re_anchor_sends_binance_nothing_and_counts_the_hour_done(tmp_path: Path) -> None:
    recorder = _recorder(tmp_path, Feed("book", "1"), Feed("trades"), Feed("ticker"))
    shard = _shard(recorder, ["btcusdt@bookTicker", "btcusdt@trade", "ethusdt@bookTicker"])
    link = FakeLink()
    shard.socket = link  # type: ignore[assignment]
    shard.connected = True
    recorder.tier_shards["core"] = [shard]

    now_ns = time.time_ns()
    day, hour = utc_day_hour(now_ns)

    recorder._reanchor(now_ns)

    assert link.sent == []
    assert shard.reanchors == 0
    assert shard.reanchored(f"{day}T{hour}")


def test_a_live_binance_shard_changes_its_subscription_in_place(tmp_path: Path) -> None:
    recorder = _recorder(tmp_path, Feed("book", "1"))
    shard = _shard(recorder, ["btcusdt@bookTicker", "ethusdt@bookTicker"])
    link = FakeLink()
    shard.socket = link  # type: ignore[assignment]
    shard.connected = True

    assert shard.update(["btcusdt@bookTicker", "solusdt@bookTicker"]) == (["solusdt@bookTicker"], ["ethusdt@bookTicker"])
    assert [json.loads(text) for text in link.sent] == [
        {"method": "UNSUBSCRIBE", "params": ["ethusdt@bookTicker"], "id": 1},
        {"method": "SUBSCRIBE", "params": ["solusdt@bookTicker"], "id": 2},
    ]
