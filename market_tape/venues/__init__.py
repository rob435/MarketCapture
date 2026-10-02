"""What a venue must provide for the recorder to record it.

The recorder (`record.py`) knows tiers, shards, files, bytes, and retention.
It knows nothing about a venue's stream names or message shapes; that is the
adapter's job. One adapter instance serves one recorder process. `normalize`
is called from the single writer thread, so an adapter may keep per-topic
sequence state without locks; side lanes run in their own threads and must
only hand rows to the `emit` callable they were given.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Iterable, Mapping, Protocol

from market_tape.config import CaptureConfig, ConfigError, Feed

Emit = Callable[[Mapping[str, Any]], None]


class VenueAdapter(Protocol):
    name: str
    market: str
    ws_url: str
    rest_url: str
    #: The venue's cap on the topics one connection carries; None when it sets none.
    max_topics_per_connection: int | None

    def validate_feeds(self, feeds: Iterable[Feed]) -> None:
        """Raise ConfigError for a feed this venue cannot record."""

    def topics(self, symbol: str, feeds: Iterable[Feed]) -> list[str]:
        """The venue's stream names for one symbol's feeds, in a stable order."""

    def connection_group(self, topic: str) -> str:
        """Which connection a topic must ride: venues that route streams by URL
        path return the path; a shard carries one group only. "" when all share one."""

    def anchored_topics(self, topics: Iterable[str]) -> list[str]:
        """The subset of `topics` a re-subscribe anchors, in the given order.

        The books, whose subscription brings the snapshot without which a
        delta means nothing, and the tickers, whose first message after a
        subscribe is the whole ticker where every later one carries only the
        fields that changed. Re-taken once an hour, they make an hour of tape
        replayable without the hours before it. A trade or liquidation row
        means the same thing on its own and is never re-taken."""

    def connection_url(self, topics: list[str]) -> str:
        """The websocket URL for one shard's topics (all of one connection group)."""

    def subscribe_messages(self, topics: list[str]) -> list[str]:
        """Text frames to send once the socket is open; empty when the URL subscribes."""

    def add_messages(self, topics: list[str]) -> list[str]:
        """Text frames that subscribe more topics on a live socket."""

    def remove_messages(self, topics: list[str]) -> list[str]:
        """Text frames that unsubscribe topics on a live socket."""

    def normalize(self, raw: str | bytes, received_ns: int, received_mono_ns: int = 0) -> list[dict[str, Any]]:
        """Tape rows for one websocket frame; empty for control frames.

        `received_ns` is the host's wall clock and `received_mono_ns` its
        monotonic clock, both read by the reader process as the read that carried
        the frame returned, less the kernel's hold of its newest packet; every
        row of the frame carries both. 0 says no monotonic
        reading was taken (a row that never came off a socket)."""

    def start_lanes(self, feeds_by_symbol: Mapping[str, tuple[Feed, ...]], emit: Emit, stop: threading.Event) -> list[threading.Thread]:
        """Long-running side lanes (REST polls) for the feeds no stream carries,
        already started; empty when no symbol asks for one. A lane hands rows
        to `emit` and ends when `stop` is set; nothing it meets may stop the tape."""

    def fetch_tables(self) -> dict[str, list[dict[str, Any]]]:
        """The venue's instrument and ticker tables, raw, as `{"instruments": [...], "tickers": [...]}`."""

    def listed_symbols(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> list[str]:
        """Crypto perpetuals the venue lists as trading, filtered by quote asset
        when given. A venue that also lists stocks, ETFs or commodities as
        perpetuals leaves them out here: the tape is the crypto domain."""

    def excluded_listed(self, instruments: Iterable[Mapping[str, Any]], *, quote: str | None) -> dict[str, int]:
        """Trading perpetuals of the quote that `listed_symbols` left out, counted
        by the venue's own label for them. What the recorder logs when it takes
        the tables, so a new label shows up in the journal rather than as a
        silent gap."""

    def turnovers(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        """24h quote turnover per symbol from the ticker table."""

    def funding_rates(self, tickers: Iterable[Mapping[str, Any]]) -> dict[str, float]:
        """Current funding rate per symbol, as a fraction (0.0001 is one basis point)."""


def adapter_for(name: str, *, market: str, ws_url: str | None = None, rest_url: str | None = None) -> VenueAdapter:
    if name == "bybit":
        from market_tape.venues.bybit import BybitAdapter

        return BybitAdapter(market=market, ws_url=ws_url, rest_url=rest_url)
    if name == "binance":
        from market_tape.venues.binance import BinanceAdapter

        return BinanceAdapter(market=market, ws_url=ws_url, rest_url=rest_url)
    raise ValueError(f"no adapter for venue {name!r}")


def validate_config(adapter: VenueAdapter, config: CaptureConfig) -> None:
    """Raise ConfigError for anything in `config` this venue cannot record:
    a feed it does not offer, or more topics a connection than it allows."""

    for tier in config.tiers:
        adapter.validate_feeds(tier.feeds)
    cap = adapter.max_topics_per_connection
    if cap is not None and config.topics_per_connection > cap:
        raise ConfigError(
            f"{adapter.name} carries at most {cap} topics a connection, "
            f"not connection.topics_per_connection = {config.topics_per_connection}"
        )
