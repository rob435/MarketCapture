"""The Tardis export: the fixture hour as day files, and hand-built tapes for each rule."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from market_tape.book import Book
from market_tape.load import open_source
from market_tape.schema import book_row, liquidation_row, parse_row, ticker_row, trade_row
from market_tape.tardis import (
    COLUMNS,
    TYPES,
    ExportError,
    _text,
    day_range,
    export_days,
    export_symbol_day,
    main,
)

FIXTURES = Path(__file__).parent / "fixtures"
HOST = FIXTURES / "host" / "bybit-linear"
EXPECTED = json.loads((FIXTURES / "expected.json").read_text())
DAY = "2026-08-30"
#: 2026-08-30T00:00:00Z.
DAY_NS = 1_788_048_000_000_000_000
MS = 1_000_000


def _read(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.DictReader(handle))


def _header(path: Path) -> list[str]:
    with gzip.open(path, "rt") as handle:
        return handle.readline().rstrip("\n").split(",")


def _tape(root: Path, symbol: str, rows: list[dict[str, Any]], *, hour: str = "00") -> Path:
    """A recorder root holding `rows` as one symbol's segment of one hour."""

    directory = root / DAY / hour / symbol
    directory.mkdir(parents=True, exist_ok=True)
    raw = directory / "segment-000000.jsonl"
    raw.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
    subprocess.run(["zstd", "-q", "--rm", str(raw)], check=True)
    return root


# ------------------------------------------------------------ the fixture hour


def test_fixture_hour_exports_every_type_the_tape_holds(tmp_path: Path) -> None:
    written = export_days(str(HOST), [DAY], tmp_path, allow_partial=True)

    assert [entry["symbol"] for entry in written] == ["BTCUSDT", "PENDLEUSDT"]
    for entry in written:
        assert entry["hours"] == ["2026-08-30T00"]
        assert entry["restamped"] == 0 and entry["book_unbased_rows"] == 0
        counts = EXPECTED["rows_by_symbol_kind"][entry["symbol"]]
        by_type = {receipt["type"]: receipt for receipt in entry["files"]}
        # No liquidation in the hour: no file, as Tardis serves none.
        assert set(by_type) == {"incremental_book_L2", "book_ticker", "trades", "derivative_ticker"}
        assert by_type["trades"]["rows"] == counts["public_trade"]
        for name, receipt in by_type.items():
            path = tmp_path / receipt["path"]
            assert path == tmp_path / "bybit" / name / DAY / f"{entry['symbol']}.csv.gz"
            assert _header(path) == list(COLUMNS[name]) == receipt["header"]
            rows = _read(path)
            assert len(rows) == receipt["rows"]
            assert path.stat().st_size == receipt["bytes"]
            assert hashlib.sha256(path.read_bytes()).hexdigest() == receipt["sha256"]
            stamps = [int(row["local_timestamp"]) for row in rows]
            assert stamps == sorted(stamps)
            assert all(row["exchange"] == "bybit" and row["symbol"] == entry["symbol"] for row in rows)
    assert not list(tmp_path.rglob("*.part"))


def test_fixture_book_file_rebuilds_the_tapes_book(tmp_path: Path) -> None:
    export_days(str(HOST), [DAY], tmp_path, allow_partial=True, symbols=["BTCUSDT"], types=["incremental_book_L2"])
    rows = _read(tmp_path / "bybit" / "incremental_book_L2" / DAY / "BTCUSDT.csv.gz")

    # As a Tardis reader takes it: a run of snapshot rows replaces the book.
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    in_snapshot = False
    for row in rows:
        snapshot = row["is_snapshot"] == "true"
        if snapshot and not in_snapshot:
            bids.clear()
            asks.clear()
        in_snapshot = snapshot
        side = bids if row["side"] == "bid" else asks
        if float(row["amount"]) > 0:
            side[float(row["price"])] = float(row["amount"])
        else:
            side.pop(float(row["price"]), None)

    expected = EXPECTED["btc_book_depth50"]
    assert [max(bids), bids[max(bids)]] == expected["best_bid"]
    assert [min(asks), asks[min(asks)]] == expected["best_ask"]
    # The venue's own decimal text, not a float's.
    assert all("." in row["price"] and "e" not in row["price"] for row in rows)


def test_export_is_resumed_reproducible_and_the_same_on_many_processes(tmp_path: Path) -> None:
    one, two = tmp_path / "one", tmp_path / "two"
    first = export_days(str(HOST), [DAY], one, allow_partial=True)
    assert export_days(str(HOST), [DAY], one, allow_partial=True) == []
    again = export_days(str(HOST), [DAY], one, allow_partial=True, overwrite=True)
    parallel = export_days(str(HOST), [DAY], two, allow_partial=True, jobs=2)

    assert first == again == parallel
    files = sorted(path.relative_to(one) for path in one.rglob("*.csv.gz"))
    assert files == sorted(path.relative_to(two) for path in two.rglob("*.csv.gz"))
    for relative in files:
        assert (one / relative).read_bytes() == (two / relative).read_bytes()
    manifest = [json.loads(line) for line in (two / "bybit" / "manifest.jsonl").read_text().splitlines()]
    assert sorted(entry["path"] for entry in manifest) == [str(relative) for relative in files]
    exports = [json.loads(line) for line in (two / "bybit" / "exports.jsonl").read_text().splitlines()]
    assert [(entry["day"], entry["symbol"]) for entry in exports] == [(DAY, "BTCUSDT"), (DAY, "PENDLEUSDT")]


def test_a_part_of_a_day_is_refused_unless_asked(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(ExportError, match="1 of 2026-08-30's 24 hours"):
        export_days(str(HOST), [DAY], tmp_path)
    assert not any(tmp_path.iterdir())
    with pytest.raises(SystemExit) as refused:
        main([str(HOST), "--days", DAY, "--out", str(tmp_path)])
    assert refused.value.code == 2
    assert main([str(HOST), "--days", DAY, "--out", str(tmp_path), "--allow-partial", "--types", "trades"]) == 0
    assert "wrote 2 files, 35 rows for 2 symbol-days" in capsys.readouterr().out


def test_unknown_type_and_venue_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ExportError, match="no such type 'quotes'"):
        export_days(str(HOST), [DAY], tmp_path, allow_partial=True, types=["quotes"])
    with pytest.raises(ExportError, match="no exchange for venue 'okx'"):
        export_days(str(HOST), [DAY], tmp_path, allow_partial=True, venue="okx")


# ------------------------------------------------------------------ the rules


def test_text_is_decimal_and_exact() -> None:
    assert _text("0.0100") == "0.0100"
    assert _text(0.1) == "0.1"
    assert _text(1e-07) == "0.0000001"
    assert _text(1.5e16) == "15000000000000000"
    assert _text(78176.0) == "78176"
    assert _text(7) == "7"
    assert _text(None) == ""
    with pytest.raises(ValueError):
        _text(float("nan"))
    assert day_range("2026-08-30", "2026-08-30") == ["2026-08-30"]
    assert day_range("2026-08-30", "2026-09-01") == ["2026-08-30", "2026-08-31"]
    assert TYPES == tuple(COLUMNS)


def _bybit_book(at_ms: int, snapshot: bool, update_id: int, bids: list[list[str]], asks: list[list[str]], *,
                depth: int = 50, gap: bool = False) -> dict[str, Any]:
    return book_row(
        venue="bybit",
        symbol="XUSDT",
        snapshot=snapshot,
        depth=depth,
        local_receive_ts_ns=DAY_NS + at_ms * MS,
        exchange_system_ts_ns=DAY_NS + at_ms * MS - 20 * MS,
        exchange_engine_ts_ns=DAY_NS + at_ms * MS - 25 * MS,
        bids=bids,
        asks=asks,
        update_id=update_id,
        previous_update_id=update_id - 1,
        sequence_gap=gap,
    )


def test_bybit_book_that_lost_its_chain_writes_nothing_until_a_snapshot(tmp_path: Path) -> None:
    root = _tape(
        tmp_path / "bybit-linear",
        "XUSDT",
        [
            _bybit_book(1_000, False, 9, [["1", "1"]], []),  # before any snapshot
            _bybit_book(2_000, True, 10, [["100", "1"]], [["101", "2"]]),
            _bybit_book(3_000, False, 11, [["100", "3"]], []),
            _bybit_book(4_000, False, 13, [["99", "9"]], []),  # 12 never came
            _bybit_book(5_000, False, 14, [["98", "9"]], []),
            _bybit_book(6_000, True, 20, [["100", "5"]], [["101", "5"]]),
            _bybit_book(7_000, False, 21, [], [["101", "0"]]),
        ],
    )
    entry = export_symbol_day(open_source(str(root)), DAY, "XUSDT", tmp_path / "out", types=["incremental_book_L2"])

    rows = _read(tmp_path / "out" / "bybit" / "incremental_book_L2" / DAY / "XUSDT.csv.gz")
    assert [(row["is_snapshot"], row["side"], row["price"], row["amount"]) for row in rows] == [
        ("true", "bid", "100", "1"),
        ("true", "ask", "101", "2"),
        ("false", "bid", "100", "3"),
        ("true", "bid", "100", "5"),
        ("true", "ask", "101", "5"),
        ("false", "ask", "101", "0"),
    ]
    assert entry["book_unbased_rows"] == 3
    # µs: the venue's send stamp and the recorder's receive stamp.
    assert rows[0]["timestamp"] == str((DAY_NS + 1_980 * MS) // 1_000)
    assert rows[0]["local_timestamp"] == str((DAY_NS + 2_000 * MS) // 1_000)


def _binance(at_ms: int, *, snapshot: bool = False, u: int, first: int = 0, previous: int = 0, gap: bool = False,
             bids: list[list[str]] | None = None, asks: list[list[str]] | None = None) -> dict[str, Any]:
    return book_row(
        venue="binance",
        symbol="XUSDT",
        snapshot=snapshot,
        depth=1000,
        local_receive_ts_ns=DAY_NS + at_ms * MS,
        exchange_system_ts_ns=DAY_NS + at_ms * MS - 5 * MS,
        exchange_engine_ts_ns=DAY_NS + at_ms * MS - 6 * MS,
        bids=bids or [],
        asks=asks or [],
        update_id=u,
        previous_update_id=previous,
        first_update_id=first,
        sequence_gap=gap,
    )


def _replay(rows: list[dict[str, str]]) -> tuple[dict[str, str], dict[str, str]]:
    bids: dict[str, str] = {}
    asks: dict[str, str] = {}
    in_snapshot = False
    for row in rows:
        snapshot = row["is_snapshot"] == "true"
        if snapshot and not in_snapshot:
            bids.clear()
            asks.clear()
        in_snapshot = snapshot
        side = bids if row["side"] == "bid" else asks
        if float(row["amount"]) > 0:
            side[row["price"]] = row["amount"]
        else:
            side.pop(row["price"], None)
    return bids, asks


def test_binance_snapshot_lands_after_diffs_and_the_file_reads_in_order(tmp_path: Path) -> None:
    tape = [
        # The first diff after a connect is flagged: nothing came before it.
        _binance(100, u=10, first=6, previous=5, gap=True, bids=[["100", "1"]]),
        _binance(200, u=15, first=11, previous=10, bids=[["99", "2"]]),
        _binance(300, u=20, first=16, previous=15, asks=[["101", "3"]]),
        # The REST snapshot as of update 12: covers the first diff, inside the second.
        _binance(350, snapshot=True, u=12, bids=[["100", "1"], ["98", "7"]], asks=[["102", "4"]]),
        _binance(400, u=25, first=21, previous=20, bids=[["98", "0"]]),
        # The hourly snapshot on a based book, as of update 22: re-bases, and the diff past it is replayed.
        _binance(450, snapshot=True, u=22, bids=[["100", "1"], ["99", "2"], ["98", "7"]], asks=[["101", "3"]]),
        _binance(500, u=30, first=26, previous=25, asks=[["101", "0"]]),
        # A snapshot older than anything still held changes nothing.
        _binance(550, snapshot=True, u=2, bids=[["1", "1"]], asks=[["2", "2"]]),
        # A broken chain: silent until the next snapshot brackets what followed.
        _binance(600, u=40, first=36, previous=33, gap=True, bids=[["97", "1"]]),
        _binance(700, u=45, first=41, previous=40, bids=[["96", "1"]]),
        _binance(800, snapshot=True, u=42, bids=[["100", "1"], ["97", "1"]], asks=[["103", "1"]]),
        # A snapshot newer than every diff seen: diffs it covers are dropped, the one spanning it applies.
        _binance(900, snapshot=True, u=60, bids=[["100", "6"]], asks=[["103", "6"]]),
        _binance(1_000, u=55, first=46, previous=45, bids=[["50", "5"]]),
        _binance(1_100, u=62, first=56, previous=55, asks=[["104", "1"]]),
        _binance(1_200, u=65, first=63, previous=62, bids=[["97", "1"]]),
    ]
    root = _tape(tmp_path / "binance-usdm", "XUSDT", tape)
    source = open_source(str(root))
    entry = export_symbol_day(source, DAY, "XUSDT", tmp_path / "out", types=["incremental_book_L2"])

    path = tmp_path / "out" / "binance-futures" / "incremental_book_L2" / DAY / "XUSDT.csv.gz"
    rows = _read(path)
    stamps = [int(row["local_timestamp"]) for row in rows]
    assert stamps == sorted(stamps)
    assert entry["book_depth"] == 1000 and entry["book_stale_snapshots"] == 1
    # Nothing before the first snapshot; the diffs it does not cover follow it, stamped with it.
    first_snapshot_us = (DAY_NS + 350 * MS) // 1_000
    assert [(row["is_snapshot"], row["side"], row["price"], row["local_timestamp"]) for row in rows[:5]] == [
        ("true", "bid", "100", str(first_snapshot_us)),
        ("true", "bid", "98", str(first_snapshot_us)),
        ("true", "ask", "102", str(first_snapshot_us)),
        ("false", "bid", "99", str(first_snapshot_us)),
        ("false", "ask", "101", str(first_snapshot_us)),
    ]
    assert _replay(rows) == ({"100": "6", "97": "1"}, {"103": "6", "104": "1"})

    # The same tape through `Book`, the tape's own reader, ends on the same book.
    book = Book()
    for raw in tape:
        book.apply(parse_row(raw))
    assert book.valid
    bids, asks = book.levels(10)
    assert {(_text(price), _text(size)) for price, size in bids} == {("100", "6"), ("97", "1")}
    assert {(_text(price), _text(size)) for price, size in asks} == {("103", "6"), ("104", "1")}


def test_top_trades_ticker_and_liquidations(tmp_path: Path) -> None:
    def top(at_ms: int, bid: list[list[str]], ask: list[list[str]]) -> dict[str, Any]:
        return _bybit_book(at_ms, True, at_ms, bid, ask, depth=1)

    def ticker(at_ms: int, **values: float | int) -> dict[str, Any]:
        return ticker_row(
            venue="bybit",
            symbol="XUSDT",
            local_receive_ts_ns=DAY_NS + at_ms * MS,
            exchange_system_ts_ns=DAY_NS + at_ms * MS - 10 * MS,
            message_type="delta",
            values=values,
        )

    rows = [
        # Read a moment before the day began: held at the day's first microsecond.
        top(-5, [["100.5", "2"]], [["100.6", "3"]]),
        ticker(10, mark_price=100.55, index_price=100.5, funding_rate=0.0001, next_funding_time_ms=1_788_076_800_000),
        ticker(20, turnover_24h=5.0),  # none of the file's columns
        ticker(30, open_interest=1234.5, last_price=1e-05),
        ticker(40, mark_price=100.55),  # unchanged
        trade_row(
            venue="bybit",
            symbol="XUSDT",
            local_receive_ts_ns=DAY_NS + 50 * MS,
            exchange_system_ts_ns=DAY_NS + 45 * MS,
            exchange_ts_ns=DAY_NS + 40 * MS,
            trade_id="t-1",
            price=100.6,
            qty=0.25,
            side="Buy",
        ),
        liquidation_row(
            venue="bybit",
            symbol="XUSDT",
            local_receive_ts_ns=DAY_NS + 60 * MS,
            exchange_system_ts_ns=DAY_NS + 55 * MS,
            exchange_ts_ns=DAY_NS + 52 * MS,
            position_side="Buy",
            qty=3.0,
            bankruptcy_price=99.9,
        ),
        top(70, [], [["100.7", "1"]]),
    ]
    root = _tape(tmp_path / "bybit-linear", "XUSDT", rows)
    entry = export_symbol_day(open_source(str(root)), DAY, "XUSDT", tmp_path / "out")
    out = tmp_path / "out" / "bybit"
    day_us = DAY_NS // 1_000

    tops = _read(out / "book_ticker" / DAY / "XUSDT.csv.gz")
    assert tops[0] == {
        "exchange": "bybit",
        "symbol": "XUSDT",
        "timestamp": str(day_us - 25_000),
        "local_timestamp": str(day_us),
        "ask_amount": "3",
        "ask_price": "100.6",
        "bid_price": "100.5",
        "bid_amount": "2",
    }
    assert (tops[1]["bid_price"], tops[1]["bid_amount"], tops[1]["ask_price"]) == ("", "", "100.7")
    assert entry["restamped"] == 1

    tickers = _read(out / "derivative_ticker" / DAY / "XUSDT.csv.gz")
    assert len(tickers) == 2
    assert tickers[0]["funding_timestamp"] == "1788076800000000"
    assert (tickers[0]["funding_rate"], tickers[0]["open_interest"], tickers[0]["last_price"]) == ("0.0001", "", "")
    assert tickers[1]["mark_price"] == "100.55" and tickers[1]["index_price"] == "100.5"
    assert (tickers[1]["open_interest"], tickers[1]["last_price"]) == ("1234.5", "0.00001")
    assert tickers[1]["predicted_funding_rate"] == ""

    (trade,) = _read(out / "trades" / DAY / "XUSDT.csv.gz")
    assert (trade["timestamp"], trade["local_timestamp"]) == (str(day_us + 40_000), str(day_us + 50_000))
    assert (trade["id"], trade["side"], trade["price"], trade["amount"]) == ("t-1", "buy", "100.6", "0.25")

    (liquidation,) = _read(out / "liquidations" / DAY / "XUSDT.csv.gz")
    # A liquidated long is closed by a sell order, which is the side Tardis states.
    assert (liquidation["side"], liquidation["price"], liquidation["amount"]) == ("sell", "99.9", "3")
    assert liquidation["timestamp"] == str(day_us + 52_000)


def test_a_binance_symbol_day_exports_its_ticker_open_interest_liquidations_and_top_of_book(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rows as the Binance adapter makes them, through `export_symbol_day`:
    `@markPrice@1s` and `@ticker` frames and the open-interest poll fold into
    one `derivative_ticker` state, a `!forceOrder@arr` order's side is the
    side Tardis states, and `@bookTicker` is `book_ticker`."""

    import urllib.request

    from market_tape.venues.binance import BinanceAdapter

    adapter = BinanceAdapter(rest_url="http://venue")
    day_ms = DAY_NS // MS

    def frame(at_ms: int, stream: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        return adapter.normalize(json.dumps({"stream": stream, "data": data}), DAY_NS + at_ms * MS, 1)

    class Answer:
        def __init__(self, payload: Any) -> None:
            self.payload = payload

        def read(self) -> bytes:
            return json.dumps(self.payload).encode()

        def __enter__(self) -> Answer:
            return self

        def __exit__(self, *exc: Any) -> bool:
            return False

    rows = frame(
        10,
        "btcusdt@bookTicker",
        {"e": "bookTicker", "u": 400900217, "s": "BTCUSDT", "b": "83680.00", "B": "7.816", "a": "83680.10", "A": "1.566", "T": day_ms + 8, "E": day_ms + 9},
    )
    rows += frame(
        20,
        "btcusdt@markPrice@1s",
        {"e": "markPriceUpdate", "E": day_ms + 18, "s": "BTCUSDT", "p": "83681.20000000", "i": "83675.06511628", "P": "83690.1", "r": "0.00003805", "T": day_ms + 8 * 3_600_000},
    )
    rows += frame(
        30,
        "btcusdt@ticker",
        {"e": "24hrTicker", "E": day_ms + 29, "s": "BTCUSDT", "c": "83680.10", "q": "9000000.5", "v": "107.6", "P": "-1.25"},
    )
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda request, timeout=None: Answer({"symbol": "BTCUSDT", "openInterest": "10659.509", "time": day_ms + 35})
    )
    poll = adapter.open_interest_row("BTCUSDT")
    rows.append({**poll, "local_receive_ts_ns": DAY_NS + 40 * MS})
    rows += frame(
        50,
        "!forceOrder@arr",
        {"e": "forceOrder", "E": day_ms + 49, "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.014", "p": "83500.5", "ap": "83510.0", "X": "FILLED", "l": "0.014", "z": "0.014", "T": day_ms + 48}},
    )
    rows += frame(
        60,
        "!forceOrder@arr",
        {"e": "forceOrder", "E": day_ms + 59, "o": {"s": "BTCUSDT", "S": "BUY", "q": "1", "p": "83900", "T": day_ms + 58}},
    )
    rows += frame(
        70,
        "btcusdt@markPrice@1s",
        {"e": "markPriceUpdate", "E": day_ms + 69, "s": "BTCUSDT", "p": "83681.20000000", "i": "83675.06511628", "r": "0.00003805", "T": day_ms + 8 * 3_600_000},
    )
    assert [row["kind"] for row in rows] == ["orderbook_snapshot", "ticker", "ticker", "ticker", "liquidation", "liquidation", "ticker"]

    root = _tape(tmp_path / "binance-usdm", "BTCUSDT", rows)
    entry = export_symbol_day(
        open_source(str(root)), DAY, "BTCUSDT", tmp_path / "out", types=["derivative_ticker", "liquidations", "book_ticker"]
    )
    out = tmp_path / "out" / "binance-futures"
    day_us = DAY_NS // 1_000
    assert entry["exchange"] == "binance-futures" and entry["restamped"] == 0

    tickers = _read(out / "derivative_ticker" / DAY / "BTCUSDT.csv.gz")
    # The last mark frame changed nothing the file states, so it writes no row.
    assert [(row["timestamp"], row["local_timestamp"]) for row in tickers] == [
        (str(day_us + 18_000), str(day_us + 20_000)),
        (str(day_us + 29_000), str(day_us + 30_000)),
        (str(day_us + 35_000), str(day_us + 40_000)),
    ]
    assert tickers[-1] == {
        "exchange": "binance-futures",
        "symbol": "BTCUSDT",
        "timestamp": str(day_us + 35_000),
        "local_timestamp": str(day_us + 40_000),
        "funding_timestamp": str((day_ms + 8 * 3_600_000) * 1_000),
        "funding_rate": "0.00003805",
        "predicted_funding_rate": "",
        "open_interest": "10659.509",
        "last_price": "83680.1",
        "index_price": "83675.06511628",
        "mark_price": "83681.2",
    }
    assert tickers[0]["open_interest"] == "" and tickers[0]["last_price"] == ""

    liquidations = _read(out / "liquidations" / DAY / "BTCUSDT.csv.gz")
    # A SELL liquidation order closes a long: Tardis states the order's side, as Binance does.
    assert [(row["side"], row["price"], row["amount"], row["timestamp"]) for row in liquidations] == [
        ("sell", "83500.5", "0.014", str(day_us + 48_000)),
        ("buy", "83900", "1", str(day_us + 58_000)),
    ]

    (top,) = _read(out / "book_ticker" / DAY / "BTCUSDT.csv.gz")
    assert (top["timestamp"], top["local_timestamp"]) == (str(day_us + 9_000), str(day_us + 10_000))
    assert (top["bid_price"], top["bid_amount"], top["ask_price"], top["ask_amount"]) == ("83680.00", "7.816", "83680.10", "1.566")
