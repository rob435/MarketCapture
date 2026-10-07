"""The frozen row contract of the market tape.

Every row on the tape is one JSON object on one line. The writer side
(`venues/*.py` normalizers) builds rows only through the constructors here; the
reader side (`load.py`) turns them back into the typed rows here. Changing a
field name or meaning is a schema change: bump SCHEMA_VERSION, and the readers
read every schema a stored row carries.

Every row carries its `venue` and `symbol`. A book row's `first_update_id` is
the venue's first id of the update (Binance `U`; 0 elsewhere) and
`previous_update_id` the venue's own previous id when it publishes one
(Binance `pu`), else the recorder's view of the row before. Two row kinds the
venue serves over REST rather than a stream join the symbol's segment,
`funding_settlement` (one settled funding payment) and `account_ratio` (one
long/short account-ratio bucket). A stream row (book, trade, ticker,
liquidation, kline) may lack `local_receive_mono_ns`, and a trade row
`exchange_system_ts_ns`; absent, both read as 0.

Timestamps are integer nanoseconds. `local_receive_ts_ns` is the recorder
host's wall clock at receipt and is the sort key of the tape.
`local_receive_mono_ns` is the same host's monotonic clock read beside it, by
the recorder's reader process as the read that carried the frame returned, less the kernel's hold of its newest packet, before the frame is queued: a clock steps and drifts, a
monotonic counter does not, so the difference between two rows' monotonic
stamps is the arrival gap the host measured, and a change in
`local_receive_ts_ns - local_receive_mono_ns` between rows is the wall clock
moving. It is comparable only within one recorder process. Both are monotone
within a segment: a row the writer receives behind one it already holds (a
segment TCP recovered, a lane row behind stream frames) is stamped at that
row's instant, both clocks by the same shift. Venue timestamps
keep the venue's names in spirit: `exchange_system_ts_ns` is when the venue's
gateway sent the message (on a trade row, the message that carried the
print), `exchange_engine_ts_ns` when its matching engine produced it,
`exchange_ts_ns` the event's own time (a trade, a liquidation).

Prices and sizes are stored as the venue's decimal strings inside book
levels, so no precision is lost; the typed rows convert to float.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Union

SCHEMA_VERSION = 2

KIND_BOOK_SNAPSHOT = "orderbook_snapshot"
KIND_BOOK_DELTA = "orderbook_delta"
KIND_TRADE = "public_trade"
KIND_TICKER = "ticker"
KIND_LIQUIDATION = "liquidation"
KIND_KLINE = "kline"
#: Served by the venue's REST history rather than a stream, fetched by a side
#: lane once an hour, and written into the symbol's own segment like any row.
KIND_FUNDING = "funding_settlement"
KIND_ACCOUNT_RATIO = "account_ratio"

SNAPSHOT_INSTRUMENTS = "instruments_snapshot"
SNAPSHOT_TICKERS = "tickers_snapshot"
SNAPSHOT_KINDS = (SNAPSHOT_INSTRUMENTS, SNAPSHOT_TICKERS)

#: The recorder's own account of one hour, written beside the table snapshots.
COVERAGE_RECORD = "coverage_record"

SIDES = ("Buy", "Sell")

#: The venue-neutral names a ticker row may carry under `values`. A row carries
#: only the fields the venue pushed in that message; a missing field means
#: "unchanged", never zero.
TICKER_VALUE_FIELDS = (
    "last_price",
    "mark_price",
    "index_price",
    "open_interest",
    "open_interest_value",
    "funding_rate",
    "next_funding_time_ms",
    "bid_price",
    "bid_size",
    "ask_price",
    "ask_size",
    "turnover_24h",
    "volume_24h",
    # The 24h price change as a fraction: 0.05 is up five percent.
    "price_change_24h_pct",
    # The venue's own trailing marks: the price an hour and a day ago, and the
    # day's extremes, so a bar built from the tape can be checked against them.
    "prev_price_1h",
    "prev_price_24h",
    "high_price_24h",
    "low_price_24h",
)
TICKER_INT_FIELDS = frozenset({"next_funding_time_ms"})
_TICKER_VALUE_SET = frozenset(TICKER_VALUE_FIELDS)

Level = tuple[float, float]
RawLevel = list[str] | tuple[str, str]


class SchemaError(ValueError):
    """A row that does not follow the contract."""


# ------------------------------------------------------------- constructors


def book_row(
    *,
    venue: str,
    symbol: str,
    snapshot: bool,
    depth: int,
    local_receive_ts_ns: int,
    exchange_system_ts_ns: int,
    exchange_engine_ts_ns: int,
    bids: Iterable[RawLevel],
    asks: Iterable[RawLevel],
    update_id: int,
    previous_update_id: int,
    cross_sequence: int = 0,
    previous_cross_sequence: int = 0,
    first_update_id: int = 0,
    restart_snapshot: bool = False,
    sequence_gap: bool = False,
    local_receive_mono_ns: int = 0,
) -> dict[str, Any]:
    return typed_book_row(
        venue,
        symbol,
        snapshot,
        int(depth),
        int(local_receive_ts_ns),
        int(local_receive_mono_ns),
        int(exchange_system_ts_ns),
        int(exchange_engine_ts_ns),
        bids,
        asks,
        int(update_id),
        int(previous_update_id),
        int(first_update_id),
        int(cross_sequence),
        int(previous_cross_sequence),
        bool(restart_snapshot),
        bool(sequence_gap),
    )


def typed_book_row(
    venue: str,
    symbol: str,
    snapshot: bool,
    depth: int,
    local_receive_ts_ns: int,
    local_receive_mono_ns: int,
    exchange_system_ts_ns: int,
    exchange_engine_ts_ns: int,
    bids: Iterable[RawLevel],
    asks: Iterable[RawLevel],
    update_id: int,
    previous_update_id: int,
    first_update_id: int,
    cross_sequence: int,
    previous_cross_sequence: int,
    restart_snapshot: bool,
    sequence_gap: bool,
) -> dict[str, Any]:
    """`book_row` by position, for a caller whose integers are ints and flags
    bools already: the same row, key for key, without the coercions. Book
    frames are most of the tape, and the normalizers call this once a frame
    on the writer thread, where a keyword call and its coercions cost more
    than the dict they build."""

    return {
        "kind": KIND_BOOK_SNAPSHOT if snapshot else KIND_BOOK_DELTA,
        "venue": venue,
        "symbol": symbol,
        "depth": depth,
        "local_receive_ts_ns": local_receive_ts_ns,
        "local_receive_mono_ns": local_receive_mono_ns,
        "exchange_system_ts_ns": exchange_system_ts_ns,
        "exchange_engine_ts_ns": exchange_engine_ts_ns,
        "bids": bids if isinstance(bids, list) and (not bids or isinstance(bids[0], list)) else [list(level) for level in bids],
        "asks": asks if isinstance(asks, list) and (not asks or isinstance(asks[0], list)) else [list(level) for level in asks],
        "update_id": update_id,
        "previous_update_id": previous_update_id,
        "first_update_id": first_update_id,
        "cross_sequence": cross_sequence,
        "previous_cross_sequence": previous_cross_sequence,
        "restart_snapshot": restart_snapshot,
        "sequence_gap": sequence_gap,
    }


def trade_row(
    *,
    venue: str,
    symbol: str,
    local_receive_ts_ns: int,
    exchange_ts_ns: int,
    trade_id: str,
    price: float,
    qty: float,
    side: str,
    exchange_system_ts_ns: int = 0,
    local_receive_mono_ns: int = 0,
) -> dict[str, Any]:
    if side not in SIDES:
        raise SchemaError(f"trade side must be Buy or Sell, got {side!r}")
    return {
        "kind": KIND_TRADE,
        "venue": venue,
        "symbol": symbol,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "local_receive_mono_ns": int(local_receive_mono_ns),
        "exchange_system_ts_ns": int(exchange_system_ts_ns),
        "exchange_ts_ns": int(exchange_ts_ns),
        "trade_id": str(trade_id),
        "price": float(price),
        "qty": float(qty),
        "side": side,
    }


def ticker_row(
    *,
    venue: str,
    symbol: str,
    local_receive_ts_ns: int,
    exchange_system_ts_ns: int,
    message_type: str,
    values: Mapping[str, float | int],
    cross_sequence: int = 0,
    local_receive_mono_ns: int = 0,
) -> dict[str, Any]:
    if not _TICKER_VALUE_SET.issuperset(values):
        raise SchemaError(f"ticker values outside the contract: {sorted(set(values) - _TICKER_VALUE_SET)}")
    return {
        "kind": KIND_TICKER,
        "venue": venue,
        "symbol": symbol,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "local_receive_mono_ns": int(local_receive_mono_ns),
        "exchange_system_ts_ns": int(exchange_system_ts_ns),
        "message_type": message_type,
        "cross_sequence": int(cross_sequence),
        "values": dict(values),
    }


def liquidation_row(
    *,
    venue: str,
    symbol: str,
    local_receive_ts_ns: int,
    exchange_system_ts_ns: int,
    exchange_ts_ns: int,
    position_side: str,
    qty: float,
    bankruptcy_price: float,
    local_receive_mono_ns: int = 0,
) -> dict[str, Any]:
    if position_side not in SIDES:
        raise SchemaError(f"liquidation side must be Buy or Sell, got {position_side!r}")
    return {
        "kind": KIND_LIQUIDATION,
        "venue": venue,
        "symbol": symbol,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "local_receive_mono_ns": int(local_receive_mono_ns),
        "exchange_system_ts_ns": int(exchange_system_ts_ns),
        "exchange_ts_ns": int(exchange_ts_ns),
        "position_side": position_side,
        "qty": float(qty),
        "bankruptcy_price": float(bankruptcy_price),
    }


def kline_row(
    *,
    venue: str,
    symbol: str,
    interval: str,
    local_receive_ts_ns: int,
    exchange_system_ts_ns: int,
    start_ms: int,
    end_ms: int,
    open: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    turnover: float,
    confirmed: bool,
    local_receive_mono_ns: int = 0,
) -> dict[str, Any]:
    """One venue candle as pushed; `confirmed` says the interval has closed.
    The venue pushes the open candle repeatedly as it changes; the last row with
    `confirmed` true is the candle."""

    return {
        "kind": KIND_KLINE,
        "venue": venue,
        "symbol": symbol,
        "interval": interval,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "local_receive_mono_ns": int(local_receive_mono_ns),
        "exchange_system_ts_ns": int(exchange_system_ts_ns),
        "start_ms": int(start_ms),
        "end_ms": int(end_ms),
        "open": float(open),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume),
        "turnover": float(turnover),
        "confirmed": bool(confirmed),
    }


def funding_row(
    *,
    venue: str,
    symbol: str,
    local_receive_ts_ns: int,
    funding_time_ms: int,
    funding_rate: float,
) -> dict[str, Any]:
    """One settled funding payment as the venue's history lists it: the rate
    charged at `funding_time_ms`, positive when longs paid shorts. The
    ticker's `funding_rate` is the running rate for the payment still to come;
    this is the one that was taken."""

    return {
        "kind": KIND_FUNDING,
        "venue": venue,
        "symbol": symbol,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "funding_time_ms": int(funding_time_ms),
        "funding_rate": float(funding_rate),
    }


def account_ratio_row(
    *,
    venue: str,
    symbol: str,
    local_receive_ts_ns: int,
    period: str,
    ts_ms: int,
    buy_ratio: float,
    sell_ratio: float,
) -> dict[str, Any]:
    """The venue's long/short account ratio for one `period` bucket stamped
    `ts_ms`: the share of accounts holding the symbol net long and net short,
    which sum to one. Positioning, not size: a thousand small longs and one
    large short read as a crowd long."""

    return {
        "kind": KIND_ACCOUNT_RATIO,
        "venue": venue,
        "symbol": symbol,
        "local_receive_ts_ns": int(local_receive_ts_ns),
        "period": str(period),
        "ts_ms": int(ts_ms),
        "buy_ratio": float(buy_ratio),
        "sell_ratio": float(sell_ratio),
    }


def feed_of_row(row: Mapping[str, Any]) -> str:
    """The feed a row belongs to (`book:50`, `trades`, `ticker`, `liquidations`,
    `kline:1`), as a tier names it; `""` for a kind no feed asks for."""

    kind = row.get("kind")
    if kind in (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA):
        return f"book:{row.get('depth')}"
    if kind == KIND_TRADE:
        return "trades"
    if kind == KIND_TICKER:
        return "ticker"
    if kind == KIND_LIQUIDATION:
        return "liquidations"
    if kind == KIND_KLINE:
        return f"kline:{row.get('interval')}"
    if kind == KIND_FUNDING:
        return "funding"
    if kind == KIND_ACCOUNT_RATIO:
        return "account_ratio"
    return ""


def snapshot_payload(
    *,
    kind: str,
    venue: str,
    market: str,
    recorded_at_ns: int,
    source: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if kind not in SNAPSHOT_KINDS:
        raise SchemaError(f"unknown snapshot kind {kind!r}")
    return {
        "kind": kind,
        "venue": venue,
        "market": market,
        "schema": SCHEMA_VERSION,
        "recorded_at_ns": int(recorded_at_ns),
        "source": source,
        "rows": rows,
    }


def coverage_payload(
    *,
    venue: str,
    market: str,
    day: str,
    hour: str,
    pid: int,
    started_at_ns: int,
    recorded_at_ns: int,
    window: Mapping[str, int],
    hour_span: Mapping[str, int],
    tiers: list[dict[str, Any]],
    shards: list[dict[str, Any]],
    disconnects: list[dict[str, Any]],
    overruns: list[dict[str, Any]],
    book_gaps: list[dict[str, Any]],
    shed: list[dict[str, Any]],
    disk_blocked: list[list[int]],
    counts: Mapping[str, int],
) -> dict[str, Any]:
    """One recorder process's account of one hour: what it was subscribed to, and
    every span in which it was not recording that subscription faithfully.

    `window` is the part of the hour this process was up for; a restart mid-hour
    writes a second record and the hour's time in neither window is
    `recorder_down`. Every span is `[from_ns, to_ns]` clipped to the window,
    except a book gap still open at the window's end, which carries `to_ns:
    null`.
    """

    return {
        "kind": COVERAGE_RECORD,
        "schema": SCHEMA_VERSION,
        "venue": venue,
        "market": market,
        "day": day,
        "hour": hour,
        "pid": int(pid),
        "started_at_ns": int(started_at_ns),
        "recorded_at_ns": int(recorded_at_ns),
        "window": {"from_ns": int(window["from_ns"]), "to_ns": int(window["to_ns"])},
        "hour_span": {"from_ns": int(hour_span["from_ns"]), "to_ns": int(hour_span["to_ns"])},
        "tiers": tiers,
        "shards": shards,
        "disconnects": disconnects,
        "overruns": overruns,
        "book_gaps": book_gaps,
        "shed": shed,
        "disk_blocked": disk_blocked,
        "counts": dict(counts),
    }


# --------------------------------------------------------------- typed rows


@dataclass(frozen=True, slots=True)
class BookRow:
    venue: str
    symbol: str
    snapshot: bool
    depth: int
    local_receive_ts_ns: int
    exchange_system_ts_ns: int
    exchange_engine_ts_ns: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    update_id: int
    previous_update_id: int
    first_update_id: int
    cross_sequence: int
    previous_cross_sequence: int
    restart_snapshot: bool
    sequence_gap: bool
    local_receive_mono_ns: int = 0

    @property
    def kind(self) -> str:
        return KIND_BOOK_SNAPSHOT if self.snapshot else KIND_BOOK_DELTA


@dataclass(frozen=True, slots=True)
class TradeRow:
    venue: str
    symbol: str
    local_receive_ts_ns: int
    exchange_ts_ns: int
    trade_id: str
    price: float
    qty: float
    side: str
    exchange_system_ts_ns: int = 0
    local_receive_mono_ns: int = 0

    kind = KIND_TRADE


@dataclass(frozen=True, slots=True)
class TickerRow:
    venue: str
    symbol: str
    local_receive_ts_ns: int
    exchange_system_ts_ns: int
    message_type: str
    cross_sequence: int
    values: Mapping[str, float | int]
    local_receive_mono_ns: int = 0

    kind = KIND_TICKER


@dataclass(frozen=True, slots=True)
class LiquidationRow:
    venue: str
    symbol: str
    local_receive_ts_ns: int
    exchange_system_ts_ns: int
    exchange_ts_ns: int
    position_side: str
    qty: float
    bankruptcy_price: float
    local_receive_mono_ns: int = 0

    kind = KIND_LIQUIDATION


@dataclass(frozen=True, slots=True)
class KlineRow:
    venue: str
    symbol: str
    interval: str
    local_receive_ts_ns: int
    exchange_system_ts_ns: int
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    confirmed: bool
    local_receive_mono_ns: int = 0

    kind = KIND_KLINE


@dataclass(frozen=True, slots=True)
class FundingRow:
    venue: str
    symbol: str
    local_receive_ts_ns: int
    funding_time_ms: int
    funding_rate: float

    kind = KIND_FUNDING


@dataclass(frozen=True, slots=True)
class AccountRatioRow:
    venue: str
    symbol: str
    local_receive_ts_ns: int
    period: str
    ts_ms: int
    buy_ratio: float
    sell_ratio: float

    kind = KIND_ACCOUNT_RATIO


Row = Union[BookRow, TradeRow, TickerRow, LiquidationRow, KlineRow, FundingRow, AccountRatioRow]


def _int(obj: Mapping[str, Any], name: str, default: int | None = 0) -> int:
    value = obj.get(name)
    if value is None:
        if default is None:
            raise SchemaError(f"row lacks {name}")
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SchemaError(f"{name} is not an integer: {value!r}") from exc


def _number(obj: Mapping[str, Any], name: str) -> float:
    """A number the row's kind always carries: absent, unreadable or not finite
    is the row's fault, never a zero."""

    value = obj.get(name)
    if value is None or isinstance(value, bool):
        raise SchemaError(f"row lacks {name}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SchemaError(f"{name} is not a number: {value!r}") from exc
    if not math.isfinite(number):
        raise SchemaError(f"{name} is not finite: {value!r}")
    return number


def _levels(raw: Any, name: str) -> tuple[Level, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise SchemaError(f"{name} is not a list")
    levels = []
    for level in raw:
        try:
            price, size = level[0], level[1]
            levels.append((float(price), float(size)))
        except (TypeError, ValueError, IndexError) as exc:
            raise SchemaError(f"{name} has a malformed level {level!r}") from exc
    return tuple(levels)


def parse_row(obj: Mapping[str, Any]) -> Row:
    """One JSON object from the tape as a typed row.

    A row with an unknown kind, without a venue or symbol, or without a number
    its kind's constructor always writes (a trade's price, a candle's close)
    raises `SchemaError`; callers decide whether to skip it.
    """

    kind = obj.get("kind")
    venue = str(obj.get("venue") or "")
    if not venue:
        raise SchemaError("row lacks a venue")
    symbol = str(obj.get("symbol") or "")
    if not symbol:
        raise SchemaError("row lacks a symbol")
    received = _int(obj, "local_receive_ts_ns", None)
    mono = _int(obj, "local_receive_mono_ns")
    if kind in (KIND_BOOK_SNAPSHOT, KIND_BOOK_DELTA):
        return BookRow(
            venue=venue,
            symbol=symbol,
            snapshot=kind == KIND_BOOK_SNAPSHOT,
            depth=_int(obj, "depth"),
            local_receive_ts_ns=received,
            exchange_system_ts_ns=_int(obj, "exchange_system_ts_ns"),
            exchange_engine_ts_ns=_int(obj, "exchange_engine_ts_ns"),
            bids=_levels(obj.get("bids"), "bids"),
            asks=_levels(obj.get("asks"), "asks"),
            update_id=_int(obj, "update_id"),
            previous_update_id=_int(obj, "previous_update_id"),
            first_update_id=_int(obj, "first_update_id"),
            cross_sequence=_int(obj, "cross_sequence"),
            previous_cross_sequence=_int(obj, "previous_cross_sequence"),
            restart_snapshot=bool(obj.get("restart_snapshot", False)),
            sequence_gap=bool(obj.get("sequence_gap", False)),
            local_receive_mono_ns=mono,
        )
    if kind == KIND_TRADE:
        side = str(obj.get("side") or "")
        if side not in SIDES:
            raise SchemaError(f"trade side must be Buy or Sell, got {side!r}")
        return TradeRow(
            venue=venue,
            symbol=symbol,
            local_receive_ts_ns=received,
            exchange_ts_ns=_int(obj, "exchange_ts_ns"),
            trade_id=str(obj.get("trade_id") or ""),
            price=_number(obj, "price"),
            qty=_number(obj, "qty"),
            side=side,
            exchange_system_ts_ns=_int(obj, "exchange_system_ts_ns"),
            local_receive_mono_ns=mono,
        )
    if kind == KIND_TICKER:
        raw_values = obj.get("values")
        if not isinstance(raw_values, Mapping):
            raise SchemaError("ticker row lacks values")
        values: dict[str, float | int] = {}
        for name, value in raw_values.items():
            if name not in TICKER_VALUE_FIELDS:
                raise SchemaError(f"ticker value outside the contract: {name}")
            values[name] = _int(raw_values, name, None) if name in TICKER_INT_FIELDS else _number(raw_values, name)
        return TickerRow(
            venue=venue,
            symbol=symbol,
            local_receive_ts_ns=received,
            exchange_system_ts_ns=_int(obj, "exchange_system_ts_ns"),
            message_type=str(obj.get("message_type") or ""),
            cross_sequence=_int(obj, "cross_sequence"),
            values=values,
            local_receive_mono_ns=mono,
        )
    if kind == KIND_LIQUIDATION:
        side = str(obj.get("position_side") or "")
        if side not in SIDES:
            raise SchemaError(f"liquidation side must be Buy or Sell, got {side!r}")
        return LiquidationRow(
            venue=venue,
            symbol=symbol,
            local_receive_ts_ns=received,
            exchange_system_ts_ns=_int(obj, "exchange_system_ts_ns"),
            exchange_ts_ns=_int(obj, "exchange_ts_ns"),
            position_side=side,
            qty=_number(obj, "qty"),
            bankruptcy_price=_number(obj, "bankruptcy_price"),
            local_receive_mono_ns=mono,
        )
    if kind == KIND_KLINE:
        return KlineRow(
            venue=venue,
            symbol=symbol,
            interval=str(obj.get("interval") or ""),
            local_receive_ts_ns=received,
            exchange_system_ts_ns=_int(obj, "exchange_system_ts_ns"),
            start_ms=_int(obj, "start_ms"),
            end_ms=_int(obj, "end_ms"),
            open=_number(obj, "open"),
            high=_number(obj, "high"),
            low=_number(obj, "low"),
            close=_number(obj, "close"),
            volume=_number(obj, "volume"),
            turnover=_number(obj, "turnover"),
            confirmed=bool(obj.get("confirmed", False)),
            local_receive_mono_ns=mono,
        )
    if kind == KIND_FUNDING:
        return FundingRow(
            venue=venue,
            symbol=symbol,
            local_receive_ts_ns=received,
            funding_time_ms=_int(obj, "funding_time_ms", None),
            funding_rate=_number(obj, "funding_rate"),
        )
    if kind == KIND_ACCOUNT_RATIO:
        return AccountRatioRow(
            venue=venue,
            symbol=symbol,
            local_receive_ts_ns=received,
            period=str(obj.get("period") or ""),
            ts_ms=_int(obj, "ts_ms", None),
            buy_ratio=_number(obj, "buy_ratio"),
            sell_ratio=_number(obj, "sell_ratio"),
        )
    raise SchemaError(f"unknown row kind {kind!r}")
