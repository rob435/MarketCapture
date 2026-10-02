"""The capture configuration: one TOML file says what a recorder records.

A recorder records one venue. It records a list of tiers; each tier names a
universe of symbols and the feeds to take for them. A symbol in several tiers
gets the union of their feeds, and each venue topic is subscribed once.

```toml
schema = 1

[venue]
name = "bybit"                 # bybit | binance
market = "linear"              # bybit: linear ; binance: usdm

[storage]
root = "/var/lib/market-capture/bybit-linear"   # `record --root` overrides
segment_max_mb = 64
segment_buffer_kb = 64         # each open segment's write buffer; at least the longest row
queue_frames = 32768            # frames waiting for the writer before a shard
                               # overruns and reconnects for fresh snapshots
queue_max_mb = 1024            # the same queue's bound in memory; a shard
                               # overruns at whichever bound binds first
retention_days = 30
max_disk_gb = 60
min_free_disk_gb = 12

[connection]
topics_per_connection = 150
reanchor_each_hour = true      # re-subscribe books and tickers each UTC hour
                               # so every hour opens with a whole book and a
                               # whole ticker per symbol

[snapshots]
cadence = "day"                # how often the instrument and ticker tables are
                               # written: day | hour

[budget]
monthly_gb = 1300              # inbound allowance for this recorder
shed = ["crowded:trades", "core:book:1"]   # what to give up first when over pace

[[tier]]
name = "core"
feeds = ["book:50", "book:1", "trades", "ticker", "liquidations"]
universe = { kind = "top_turnover", top = 30, leave_top = 45, quote = "USDT" }

[[tier]]
name = "crowded"
feeds = ["book:50", "trades"]
universe = { kind = "funding_below", threshold_bp = 8, sticky_hours = 48, quote = "USDT", exclude_tiers = ["core"] }

[[tier]]
name = "wide"
feeds = ["ticker", "liquidations"]
universe = { kind = "listed", quote = "USDT", exclude_tiers = ["core"] }
```

Feeds: `book:<levels>` (the venue says which level counts it offers; `book:1`
is the top of book), `trades`, `ticker` (last, mark, index, funding, open
interest, best bid and ask, 24h turnover, price change, hour-ago and day-ago
prices and the day's extremes, as the venue pushes them), `liquidations`,
`kline:<interval>` (venue candles, e.g. `kline:1m`), `open_interest:<seconds>`
(a REST poll, for venues that push no open interest), `funding` (the venue's
settled funding payments, over REST once an hour) and `account_ratio` (the
venue's long/short account ratio at five-minute resolution, over REST once an
hour).

Universes:

- `symbols` (an inline list) and `file` (one symbol per line, `#` comments)
  are fixed for the life of the process.
- `listed`: every crypto perpetual the venue lists as trading, optionally filtered by
  `quote`; re-read with each table snapshot.
- `top_turnover`: the `top` names by 24h turnover. A member stays until it
  falls below rank `leave_top` (default one and a half times `top`), so a name
  on the boundary does not flap; with `sticky_hours` it also stays at least
  that long after it last ranked inside `top`, whatever its rank does — the
  floor a strategy's holding period needs.
- `top_movers`: the `top` names by the size of their 24h price change, up or
  down, with the same `leave_top` hysteresis and optional `sticky_hours`.
- `funding_below`: names whose funding rate is at or below `-threshold_bp`.
- `funding_above`: names whose funding rate is at or above `threshold_bp`.
- `turnover_surge`: names whose 24h turnover is at least `ratio` times what
  the last table snapshot showed for them.
- `price_move`: names whose 24h price change is at least `pct` (a fraction:
  0.2 is twenty percent) in either direction.
- `price_burst`: names whose price moved at least `pct` either way over the
  last `window_hours` (default 1), measured from the recorder's own ticker
  history.
- `volume_burst`: names whose 24h turnover grew, over the last `window_hours`
  (default 1), by at least `ratio` times an average window's share of it —
  the last hour traded `ratio` average hours more than the same hour a day
  ago.
- `oi_change`: names whose open interest moved at least `pct` either way over
  the last `window_hours` (default 1). Bybit pushes open interest on the
  ticker; on Binance the name must carry an `open_interest` poll feed.

All but the first three are live: the recorder reads them off the ticker
stream it is already recording and promotes a name within one maintenance tick
of the observation, not at the next daily snapshot. A name that qualified stays
for `sticky_hours` (default 48) after its last qualifying observation; the two
ranked kinds use `leave_top` instead. The windowed kinds compare against a
ticker sample the recorder took `window_hours` earlier, so they see nothing
until the recorder has run that long. The ticker feed on a `listed` tier is
the sensor; without it, a live universe sees only the names some tier already
records. `exclude_tiers` removes names already covered by the named tiers.

Budget: `monthly_gb` is this recorder's inbound allowance. The recorder
projects a month from its last 24 hours of received bytes; when the projection
is over the allowance it gives up the first entry of `shed` (a `tier:feed`
pair), then the next an hour later, and restores them in reverse once the
projection is under `restore_below` (default 0.8) of the allowance. Without a
budget the recorder only measures. The full-universe configuration for a
machine with unbounded bandwidth and disk is one tier: `listed` with every feed
and no budget. See `examples/`.
"""

from __future__ import annotations

import difflib
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable, Mapping

CONFIG_SCHEMA = 1
VENUES = ("bybit", "binance")
FEED_NAMES = ("book", "trades", "ticker", "liquidations", "kline", "open_interest", "funding", "account_ratio")
UNIVERSE_KINDS = (
    "symbols",
    "file",
    "listed",
    "top_turnover",
    "top_movers",
    "funding_below",
    "funding_above",
    "turnover_surge",
    "price_move",
    "price_burst",
    "volume_burst",
    "oi_change",
)
RANKED_KINDS = ("top_turnover", "top_movers")
STICKY_KINDS = ("funding_below", "funding_above", "turnover_surge", "price_move", "price_burst", "volume_burst", "oi_change")
WINDOWED_KINDS = ("price_burst", "volume_burst", "oi_change")
LIVE_KINDS = RANKED_KINDS + STICKY_KINDS
SNAPSHOT_CADENCES = ("day", "hour")
DEFAULT_STICKY_HOURS = 48.0


class ConfigError(ValueError):
    """The configuration cannot be recorded from."""


#: What each table may contain. `[storage]` is read off `StorageSettings`, so
#: a new setting is spellable the moment it exists and no second list can go
#: stale against it.
TABLE_KEYS: Mapping[str, frozenset[str]] = {
    "venue": frozenset({"name", "market", "ws_url", "rest_url"}),
    "connection": frozenset({"topics_per_connection", "reanchor_each_hour"}),
    "snapshots": frozenset({"cadence"}),
    "budget": frozenset({"monthly_gb", "shed", "restore_below", "act_every_minutes"}),
    "tier": frozenset({"name", "feeds", "universe"}),
    "universe": frozenset(
        {
            "kind",
            "exclude_tiers",
            "symbols",
            "path",
            "quote",
            "top",
            "leave_top",
            "sticky_hours",
            "sticky_days",
            "window_hours",
            "threshold_bp",
            "ratio",
            "pct",
        }
    ),
}


def _only_known(table: Mapping[str, Any], section: str, *, known: Iterable[str] | None = None) -> None:
    """Refuse a key the table does not have.

    A mistyped setting that is merely ignored runs the recorder on a default
    nobody chose and says nothing about it. `max_disk_bg = 18` leaves that
    recorder on the 60 GB default while the very file it is written in says
    the two recorders' caps must sum under the filesystem.
    """

    allowed = set(TABLE_KEYS[section] if known is None else known)
    for key in table:
        if key in allowed:
            continue
        near = difflib.get_close_matches(str(key), sorted(allowed), n=1)
        hint = f"did you mean {near[0]!r}?" if near else f"keys are {', '.join(sorted(allowed))}"
        raise ConfigError(f"[{section}] has no key {key!r}; {hint}")


@dataclass(frozen=True, slots=True)
class Feed:
    name: str
    arg: str | None = None

    @property
    def text(self) -> str:
        return self.name if self.arg is None else f"{self.name}:{self.arg}"

    @property
    def levels(self) -> int:
        if self.name != "book" or self.arg is None:
            raise ConfigError(f"{self.text} has no level count")
        return int(self.arg)

    @property
    def seconds(self) -> float:
        if self.name != "open_interest" or self.arg is None:
            raise ConfigError(f"{self.text} has no poll interval")
        return float(self.arg.rstrip("s"))


@dataclass(frozen=True, slots=True)
class Universe:
    kind: str
    symbols: tuple[str, ...] = ()
    path: Path | None = None
    quote: str | None = None
    top: int = 0
    leave_top: int = 0
    threshold_bp: float = 0.0
    ratio: float = 0.0
    pct: float = 0.0
    window_hours: float = 0.0
    #: Unset means the kind's own default: 48 h for the funding and burst kinds,
    #: no time floor for the ranked kinds. The parser always sets it.
    sticky_hours: float | None = None
    exclude_tiers: tuple[str, ...] = ()

    @property
    def live(self) -> bool:
        """Decided from the ticker stream, not from a table snapshot."""

        return self.kind in LIVE_KINDS


@dataclass(frozen=True, slots=True)
class Tier:
    name: str
    feeds: tuple[Feed, ...]
    universe: Universe


@dataclass(frozen=True, slots=True)
class VenueSettings:
    name: str
    market: str
    ws_url: str | None = None
    rest_url: str | None = None


@dataclass(frozen=True, slots=True)
class StorageSettings:
    """The one declaration of the storage defaults; the TOML loader and the capture CLI read them from here.

    `fsync_every_records` is the recovery point objective: a power loss loses at
    most that many acknowledged rows of each symbol's open segment, less one.

    `segment_buffer_kb` is each open segment's write buffer, one per symbol and
    resident for the recorder's life. It must hold the longest row the tiers
    write (`storage.SegmentWriter.append` keeps or refuses a row whole only up
    to it): a book row is 42-54 KB at 1,000 levels, 9-11 KB at 200, 2.5-3 KB at
    50, under 0.5 KB at 1.

    `queue_frames` and `queue_max_mb` bound the capture queue between the shards
    and the writer, in items and in the memory they hold (`record.queue_item_bytes`);
    a shard overruns at whichever binds first. A frame's size is the venue's: a
    quiet day's frame is ~220 B, a volatile day's `book:50` snapshot 3-5 KB.

    `compress_backlog_max_mb` is the compressor's work list ceiling, in raw
    bytes awaiting compression. Above it a closed segment is left as `.jsonl`
    and held in the compressor's deferred FIFO, which its own thread drains
    back into the queue in submission order as room appears: the frame loop
    never waits on compression, no row is dropped, and the queued work list
    stays under the ceiling.
    """

    root: Path | None = None
    segment_max_mb: float = 64.0
    segment_buffer_kb: float = 64.0
    fsync_every_records: int = 1_000
    retention_days: int = 30
    max_disk_gb: float = 60.0
    min_free_disk_gb: float = 12.0
    queue_frames: int = 32_768
    queue_max_mb: float = 1_024.0
    status_interval_seconds: float = 30.0
    compress_backlog_max_mb: float = 4_096.0


@dataclass(frozen=True, slots=True)
class BudgetSettings:
    monthly_gb: float | None = None
    shed: tuple[tuple[str, str], ...] = ()
    restore_below: float = 0.8
    act_every_minutes: float = 60.0


@dataclass(frozen=True, slots=True)
class CaptureConfig:
    venue: VenueSettings
    storage: StorageSettings
    tiers: tuple[Tier, ...]
    topics_per_connection: int = 150
    #: Re-subscribe every book topic once per UTC hour so each hour of tape
    #: opens with a snapshot per symbol. Without it a book delta only means
    #: something next to a snapshot in an earlier hour, and one hour of the
    #: archive cannot be replayed on its own.
    reanchor_each_hour: bool = True
    snapshot_cadence: str = "day"
    budget: BudgetSettings = field(default_factory=BudgetSettings)
    source_path: Path | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def tier(self, name: str) -> Tier:
        for tier in self.tiers:
            if tier.name == name:
                return tier
        raise KeyError(name)

    @property
    def history_hours(self) -> float:
        """How far back the live state must remember ticker samples: the longest window any tier looks over."""

        return max((tier.universe.window_hours for tier in self.tiers), default=0.0)


# ------------------------------------------------------------------ parsing


def parse_feed(text: str) -> Feed:
    name, _, raw_arg = str(text).strip().partition(":")
    if name not in FEED_NAMES:
        raise ConfigError(f"unknown feed {text!r}; feeds are {', '.join(FEED_NAMES)}")
    arg: str | None = raw_arg.strip() or None
    if name == "book":
        if arg is None or not arg.isdigit() or int(arg) <= 0:
            raise ConfigError(f"a book feed names its level count, like book:50, got {text!r}")
    elif name == "kline":
        if not arg:
            raise ConfigError(f"a kline feed names its interval, like kline:1m, got {text!r}")
    elif name == "open_interest":
        if arg is None:
            arg = "60s"
        try:
            if float(arg.rstrip("s")) <= 0:
                raise ValueError
        except ValueError as exc:
            raise ConfigError(f"an open_interest feed names a poll interval in seconds, got {text!r}") from exc
    elif arg is not None:
        raise ConfigError(f"{name} takes no argument, got {text!r}")
    return Feed(name, arg)


def load_symbol_file(path: Path) -> tuple[str, ...]:
    symbols: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.partition("#")[0].strip()
        symbols.update(token.upper() for token in text.replace(",", " ").split() if token)
    return tuple(sorted(symbols))


def validate_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    result = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
    invalid = [symbol for symbol in result if not symbol.isalnum()]
    if invalid:
        raise ConfigError(f"invalid symbols: {invalid}")
    return result


def _sticky_hours(raw: Mapping[str, Any], *, tier: str) -> float:
    if "sticky_hours" in raw and "sticky_days" in raw:
        raise ConfigError(f"tier {tier!r}: give sticky_hours or sticky_days, not both")
    if "sticky_days" in raw:
        days = float(raw["sticky_days"])
        if days <= 0:
            raise ConfigError(f"tier {tier!r}: sticky_days must be positive")
        return days * 24.0
    hours = float(raw.get("sticky_hours", DEFAULT_STICKY_HOURS))
    if hours <= 0:
        raise ConfigError(f"tier {tier!r}: sticky_hours must be positive")
    return hours


def _quote(raw: Mapping[str, Any]) -> str | None:
    quote = raw.get("quote")
    return str(quote).upper() if quote else None


def _universe(raw: Mapping[str, Any], *, tier: str, base_dir: Path) -> Universe:
    if not isinstance(raw, Mapping):
        raise ConfigError(f"tier {tier!r}: universe must be a table")
    _only_known(raw, "universe")
    kind = str(raw.get("kind") or "")
    if kind not in UNIVERSE_KINDS:
        raise ConfigError(f"tier {tier!r}: unknown universe kind {kind!r}; kinds are {', '.join(UNIVERSE_KINDS)}")
    exclude = tuple(str(name) for name in raw.get("exclude_tiers", ()))
    if kind == "symbols":
        symbols = validate_symbols(raw.get("symbols", ()))
        if not symbols:
            raise ConfigError(f"tier {tier!r}: a symbols universe needs at least one symbol")
        return Universe(kind, symbols=symbols, exclude_tiers=exclude)
    if kind == "file":
        text = raw.get("path")
        if not text:
            raise ConfigError(f"tier {tier!r}: a file universe needs a path")
        path = Path(str(text))
        if not path.is_absolute():
            path = base_dir / path
        return Universe(kind, path=path, exclude_tiers=exclude)
    if kind == "listed":
        return Universe(kind, quote=_quote(raw), exclude_tiers=exclude)
    if kind in RANKED_KINDS:
        top = int(raw.get("top") or 0)
        if top <= 0:
            raise ConfigError(f"tier {tier!r}: {kind} needs top > 0")
        leave_top = int(raw.get("leave_top") or round(top * 1.5))
        if leave_top < top:
            raise ConfigError(f"tier {tier!r}: leave_top must be at least top")
        # A time floor on membership, off unless asked for: a strategy that holds
        # a name for days needs its book for days, whatever its rank does.
        sticky = _sticky_hours(raw, tier=tier) if ("sticky_hours" in raw or "sticky_days" in raw) else 0.0
        return Universe(kind, top=top, leave_top=leave_top, sticky_hours=sticky, quote=_quote(raw), exclude_tiers=exclude)
    sticky = _sticky_hours(raw, tier=tier)
    window = 0.0
    if kind in WINDOWED_KINDS:
        window = float(raw.get("window_hours", 1.0))
        if window <= 0:
            raise ConfigError(f"tier {tier!r}: {kind} needs window_hours > 0")
    common: dict[str, Any] = {"sticky_hours": sticky, "window_hours": window, "quote": _quote(raw), "exclude_tiers": exclude}
    if kind in ("funding_below", "funding_above"):
        threshold = float(raw.get("threshold_bp") or 0.0)
        if threshold <= 0:
            raise ConfigError(f"tier {tier!r}: {kind} needs threshold_bp > 0")
        return Universe(kind, threshold_bp=threshold, **common)
    if kind == "turnover_surge":
        ratio = float(raw.get("ratio") or 0.0)
        if ratio <= 1.0:
            raise ConfigError(f"tier {tier!r}: turnover_surge needs ratio > 1")
        return Universe(kind, ratio=ratio, **common)
    if kind == "volume_burst":
        ratio = float(raw.get("ratio") or 0.0)
        if ratio <= 0.0:
            raise ConfigError(f"tier {tier!r}: volume_burst needs ratio > 0 (3 is three average windows of extra turnover)")
        return Universe(kind, ratio=ratio, **common)
    pct = float(raw.get("pct") or 0.0)
    if pct <= 0:
        raise ConfigError(f"tier {tier!r}: {kind} needs pct > 0 (a fraction, 0.2 is twenty percent)")
    return Universe(kind, pct=pct, **common)


def _positive(table: Mapping[str, Any], name: str, default: float, *, section: str) -> float:
    value = table.get(name, default)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{section}.{name} must be a number") from exc
    if number <= 0:
        raise ConfigError(f"{section}.{name} must be positive")
    return number


def _budget(raw: Mapping[str, Any], tiers: Iterable[Tier]) -> BudgetSettings:
    if not isinstance(raw, Mapping):
        raise ConfigError("[budget] must be a table")
    _only_known(raw, "budget")
    monthly = raw.get("monthly_gb")
    monthly_gb = None if monthly is None else _positive(raw, "monthly_gb", 0.0, section="budget")
    restore_below = float(raw.get("restore_below", 0.8))
    if not 0.0 < restore_below < 1.0:
        raise ConfigError("budget.restore_below must be between 0 and 1")
    act_every = _positive(raw, "act_every_minutes", 60.0, section="budget")
    feeds_by_tier = {tier.name: {feed.text for feed in tier.feeds} for tier in tiers}
    shed: list[tuple[str, str]] = []
    for text in raw.get("shed", ()):
        tier_name, separator, feed_text = str(text).partition(":")
        if not separator or tier_name not in feeds_by_tier or feed_text not in feeds_by_tier[tier_name]:
            raise ConfigError(f"budget.shed entry {text!r} must be tier:feed for a tier and feed in this config")
        if (tier_name, feed_text) in shed:
            raise ConfigError(f"budget.shed repeats {text!r}")
        shed.append((tier_name, feed_text))
    if shed and monthly_gb is None:
        raise ConfigError("budget.shed needs budget.monthly_gb")
    return BudgetSettings(monthly_gb=monthly_gb, shed=tuple(shed), restore_below=restore_below, act_every_minutes=act_every)


def parse_config(data: Mapping[str, Any], *, base_dir: Path, source_path: Path | None = None) -> CaptureConfig:
    if int(data.get("schema", CONFIG_SCHEMA)) != CONFIG_SCHEMA:
        raise ConfigError(f"capture config schema {data.get('schema')} is not {CONFIG_SCHEMA}")
    venue_table = data.get("venue")
    if not isinstance(venue_table, Mapping):
        raise ConfigError("config needs a [venue] table")
    _only_known(venue_table, "venue")
    name = str(venue_table.get("name") or "")
    if name not in VENUES:
        raise ConfigError(f"unknown venue {name!r}; venues are {', '.join(VENUES)}")
    market = str(venue_table.get("market") or "")
    if not market:
        raise ConfigError("venue.market is required")
    venue = VenueSettings(
        name=name,
        market=market,
        ws_url=str(venue_table["ws_url"]) if venue_table.get("ws_url") else None,
        rest_url=str(venue_table["rest_url"]) if venue_table.get("rest_url") else None,
    )

    storage_table = data.get("storage") or {}
    if not isinstance(storage_table, Mapping):
        raise ConfigError("[storage] must be a table")
    _only_known(storage_table, "storage", known={field.name for field in fields(StorageSettings)})
    root = storage_table.get("root")
    root_path = Path(str(root)) if root else None
    if root_path is not None and not root_path.is_absolute():
        root_path = base_dir / root_path
    defaults = StorageSettings()
    storage = StorageSettings(
        root=root_path,
        segment_max_mb=_positive(storage_table, "segment_max_mb", defaults.segment_max_mb, section="storage"),
        segment_buffer_kb=_positive(storage_table, "segment_buffer_kb", defaults.segment_buffer_kb, section="storage"),
        fsync_every_records=int(
            _positive(storage_table, "fsync_every_records", defaults.fsync_every_records, section="storage")
        ),
        retention_days=int(_positive(storage_table, "retention_days", defaults.retention_days, section="storage")),
        max_disk_gb=_positive(storage_table, "max_disk_gb", defaults.max_disk_gb, section="storage"),
        min_free_disk_gb=_positive(storage_table, "min_free_disk_gb", defaults.min_free_disk_gb, section="storage"),
        queue_frames=int(_positive(storage_table, "queue_frames", defaults.queue_frames, section="storage")),
        queue_max_mb=_positive(storage_table, "queue_max_mb", defaults.queue_max_mb, section="storage"),
        status_interval_seconds=_positive(
            storage_table, "status_interval_seconds", defaults.status_interval_seconds, section="storage"
        ),
        compress_backlog_max_mb=_positive(
            storage_table, "compress_backlog_max_mb", defaults.compress_backlog_max_mb, section="storage"
        ),
    )

    connection = data.get("connection") or {}
    _only_known(connection, "connection")
    topics_per_connection = int(_positive(connection, "topics_per_connection", 150, section="connection"))
    reanchor = connection.get("reanchor_each_hour", True)
    if not isinstance(reanchor, bool):
        raise ConfigError("connection.reanchor_each_hour must be true or false")

    snapshots = data.get("snapshots") or {}
    _only_known(snapshots, "snapshots")
    cadence = str(snapshots.get("cadence") or "day")
    if cadence not in SNAPSHOT_CADENCES:
        raise ConfigError(f"snapshots.cadence must be one of {SNAPSHOT_CADENCES}, got {cadence!r}")

    raw_tiers = data.get("tier")
    if not isinstance(raw_tiers, list) or not raw_tiers:
        raise ConfigError("config needs at least one [[tier]]")
    tiers: list[Tier] = []
    seen: set[str] = set()
    for raw in raw_tiers:
        if not isinstance(raw, Mapping):
            raise ConfigError("each [[tier]] must be a table")
        _only_known(raw, "tier")
        tier_name = str(raw.get("name") or "")
        if not tier_name or tier_name in seen or ":" in tier_name:
            raise ConfigError(f"tier names must be unique, non-empty, and free of ':', got {tier_name!r}")
        seen.add(tier_name)
        feeds_raw = raw.get("feeds")
        if not isinstance(feeds_raw, list) or not feeds_raw:
            raise ConfigError(f"tier {tier_name!r} needs a feeds list")
        feeds = tuple(parse_feed(text) for text in feeds_raw)
        if len({feed.text for feed in feeds}) != len(feeds):
            raise ConfigError(f"tier {tier_name!r} repeats a feed")
        universe = _universe(raw.get("universe") or {}, tier=tier_name, base_dir=base_dir)
        for excluded in universe.exclude_tiers:
            if excluded not in seen or excluded == tier_name:
                raise ConfigError(f"tier {tier_name!r} excludes {excluded!r}, which is not an earlier tier")
        tiers.append(Tier(tier_name, feeds, universe))

    budget = _budget(data.get("budget") or {}, tiers)
    known = {"schema", "venue", "storage", "connection", "snapshots", "budget", "tier"}
    extra = {key: value for key, value in data.items() if key not in known}
    return CaptureConfig(
        venue=venue,
        storage=storage,
        tiers=tuple(tiers),
        topics_per_connection=topics_per_connection,
        reanchor_each_hour=reanchor,
        snapshot_cadence=cadence,
        budget=budget,
        source_path=source_path,
        extra=extra,
    )


def load_config(path: Path, *, base_dir: Path | None = None) -> CaptureConfig:
    """Read a TOML capture config. Relative paths resolve against `base_dir`
    (default: the current working directory, which on the host is the checkout)."""

    with path.open("rb") as handle:
        data = tomllib.load(handle)
    return parse_config(data, base_dir=(base_dir or Path.cwd()).resolve(), source_path=path.resolve())
