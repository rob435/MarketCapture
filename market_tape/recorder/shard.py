"""A shard: one websocket connection carrying one slice of a tier's topic list.

A tier's topics are chunked into connections (`shard_topics`), never mixing
the adapter's connection groups. Each shard has a thread for its
connection's lifetime (open, subscribe, backoff); the reader process holds
the socket (`market_tape/recorder/intake.py`). Topics are added to and
removed from the live connection in place; a connection only reconnects
when the venue drops it. The shard is also what re-subscribes: the hourly
re-anchor of its books and tickers, in small chunks, and one gapped book
topic at a time. Every subscription message leaves
`LIVE_MESSAGE_SPACING_SECONDS` after the one before it, the venues' rate.
"""

from __future__ import annotations

import logging
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from market_tape.recorder.intake import ReaderProcess, RemoteLink
from market_tape.recorder.queue import CHAIN_RESET, LANES
from market_tape.venues import VenueAdapter


#: A shard's wait before reconnecting: the minimum after a connection that
#: lived `RECONNECT_HEALTHY_SECONDS`, doubling per short-lived one up to the
#: maximum, each wait drawn from its upper half.
RECONNECT_BACKOFF_MIN_SECONDS = 2.0
RECONNECT_BACKOFF_MAX_SECONDS = 60.0
RECONNECT_HEALTHY_SECONDS = 60.0
#: Subscription frames one shard sends leave this far apart: Binance drops a
#: connection that sends it more than ten messages a second, pings and pongs
#: included, and Bybit refuses a burst. Both venues get this spacing.
LIVE_MESSAGE_SPACING_SECONDS = 0.12
#: Topics dropped and re-taken together. One venue message carries ten, so a
#: chunk is one message each way and a symbol's gap is one round trip.
REANCHOR_CHUNK = 10


def shard_topics(topics: list[str], per_connection: int, group: Callable[[str], str] | None = None) -> list[list[str]]:
    """Chunk topics into connections; with `group`, topics of different groups
    never share a connection, and each group keeps its order."""

    if per_connection <= 0:
        raise ValueError("topics per connection must be positive")
    grouped: dict[str, list[str]] = {}
    for topic in topics:
        grouped.setdefault(group(topic) if group else "", []).append(topic)
    return [
        members[start : start + per_connection]
        for members in grouped.values()
        for start in range(0, len(members), per_connection)
    ]


@dataclass
class Shard:
    """One websocket connection carrying one slice of a tier's topic list."""

    index: int
    tier: str
    topics: list[str]
    adapter: VenueAdapter
    #: Called from the intake thread: frames read, and frames dropped on an overrun.
    on_frame: Any
    on_overrun: Any
    #: Called with this shard when its connection opens and when it ends, from
    #: the shard's own thread: the coverage fold's view of connectivity.
    on_connect: Any = None
    on_disconnect: Any = None
    #: The reader process that holds every shard's socket.
    reader: ReaderProcess | None = None
    stop: threading.Event = field(default_factory=threading.Event)
    socket: RemoteLink | None = None
    thread: threading.Thread | None = None
    connected: bool = False
    reconnects: int = 0
    #: Chunks re-anchored, counted for `status.json`.
    reanchors: int = 0
    #: Book topics re-subscribed on a sequence gap, counted for `status.json`.
    resyncs: int = 0
    #: The hour `reanchor_cursor` belongs to, and how many of this shard's
    #: book topics that hour's pass has re-anchored. Together they let the
    #: hourly pass spread over many maintenance ticks and resume where it was.
    reanchor_hour: str = ""
    reanchor_cursor: int = 0
    last_message_ns: int = 0
    #: The reader's last `STATS` for this shard's connection; empty until its first.
    link_stats: dict[str, Any] = field(default_factory=dict)
    backoff_seconds: float = field(default_factory=lambda: RECONNECT_BACKOFF_MIN_SECONDS)

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name=f"tape-shard-{self.index}", daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        link = self.socket
        if link is not None:
            link.request_close()

    def join(self, timeout: float = 10.0) -> None:
        if self.thread is not None:
            self.thread.join(timeout)

    def status(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "tier": self.tier,
            "topics": len(self.topics),
            "connected": self.connected,
            "reconnects": self.reconnects,
            "reanchors": self.reanchors,
            "resyncs": self.resyncs,
            "last_message_ns": self.last_message_ns,
            "link": self.link_stats,
        }

    def coverage(self) -> dict[str, Any]:
        """What the hour's coverage record says about this shard; topics by name,
        because a cell asks whether its own topic was on a shard that failed."""

        return {
            "index": self.index,
            "tier": self.tier,
            "topics": list(self.topics),
            "reconnects": self.reconnects,
            "resyncs": self.resyncs,
            "reanchors": self.reanchors,
        }

    def update(self, topics: list[str]) -> tuple[list[str], list[str]]:
        """Make this shard carry exactly `topics`, changing the live subscription
        in place; a shard that is not connected picks the list up when it connects."""

        current = set(self.topics)
        wanted = set(topics)
        added = [topic for topic in topics if topic not in current]
        removed = [topic for topic in self.topics if topic not in wanted]
        self.topics = list(topics)
        if (added or removed) and self.connected and self.socket is not None:
            self._send_all(self.adapter.remove_messages(removed))
            self._reset_chains(added)
            self._send_all(self.adapter.add_messages(added))
        return added, removed

    def _reset_chains(self, topics: list[str]) -> None:
        """Queue a chain reset for those of `topics` the adapter re-bases from
        REST (`rest_based_topics`), ahead of the subscribe that is about to
        go out: no frame of theirs can reach the queue before the venue has
        the subscribe, so the writer meets the reset between the old
        subscription's last frame and the new one's first. A queue with no
        room drops the reset; the frames behind it overrun too, which breaks
        the chain on its own."""

        based = getattr(self.adapter, "rest_based_topics", None)
        if not topics or not callable(based) or self.reader is None:
            return
        chains = list(based(topics))
        if not chains:
            return
        try:
            self.reader.frames.put((CHAIN_RESET, chains, time.time_ns(), LANES, 0), block=False)
        except queue.Full:
            logging.warning("shard %d: no queue room to reset %d book chains", self.index, len(chains))

    def anchored_count(self) -> int:
        return len(self.adapter.anchored_topics(self.topics))

    def mark_anchored(self, hour: str) -> None:
        """Treat this shard's books and tickers as anchored for `hour` without
        sending anything. Connecting subscribes, and subscribing is what
        anchors them, so a shard that just connected is already done for its
        hour."""

        self.reanchor_hour = hour
        self.reanchor_cursor = self.anchored_count()

    def reanchor(self, hour: str, limit: int) -> int:
        """Re-subscribe up to `limit` more of this shard's anchored topics for
        `hour`, and return how many were sent.

        The point is the whole state: a book delta only means something next
        to a snapshot, and a ticker delta only next to the whole ticker its
        subscription brought, so an hour of tape anchored in an earlier hour
        cannot be replayed on its own. The cost is the moment between a
        topic's unsubscribe and its snapshot, so the topics go in small
        chunks that are dropped and re-taken together — one round trip per
        symbol rather than one per shard. The snapshot row marks the seam.
        """

        if self.reanchor_hour != hour:
            self.reanchor_hour = hour
            self.reanchor_cursor = 0
        books = self.adapter.anchored_topics(self.topics)
        if not self.connected or self.socket is None:
            return 0
        sent = 0
        while self.reanchor_cursor < len(books) and sent < limit:
            chunk = books[self.reanchor_cursor : self.reanchor_cursor + REANCHOR_CHUNK]
            if sent:
                # Between chunks, not between a chunk's drop and re-take: the
                # venue's incoming-message rate is what this spacing protects,
                # and the gap it would add sits inside a symbol's blind moment.
                time.sleep(LIVE_MESSAGE_SPACING_SECONDS)
            self._retake(chunk)
            self.reanchor_cursor += len(chunk)
            sent += len(chunk)
            self.reanchors += 1
        return sent

    def reanchored(self, hour: str) -> bool:
        return self.reanchor_hour == hour and self.reanchor_cursor >= self.anchored_count()

    def resync_book(self, topic: str) -> bool:
        """Re-subscribe one gapped book topic, and return whether it was sent.

        A book snapshot comes with a subscription — on Bybit at no other time
        — so a topic that lost continuity stays un-based until it is taken
        again. A shard that is not connected re-bases by connecting.
        """

        if not self.connected or self.socket is None or topic not in self.topics:
            return False
        self._retake([topic])
        self.resyncs += 1
        return True

    def _retake(self, chunk: list[str]) -> None:
        self._send_all(self.adapter.remove_messages(chunk))
        self._send_all(self.adapter.add_messages(chunk))

    def _send_all(self, messages: list[str]) -> None:
        for position, text in enumerate(messages):
            if position:
                time.sleep(LIVE_MESSAGE_SPACING_SECONDS)
            link = self.socket
            if link is None:
                return
            try:
                link.send_text(text)
            except Exception as exc:  # the reconnect resubscribes from self.topics
                logging.warning("shard %d could not send a subscription change: %s", self.index, exc)
                return

    def _note(self, hook: Any) -> None:
        if hook is not None:
            hook(self)

    def _run(self) -> None:
        while not self.stop.is_set():
            opened = time.monotonic()
            self._connect_once()
            self.connected = False
            self._note(self.on_disconnect)
            if self.stop.is_set():
                return
            self.reconnects += 1
            # A connection that lived a minute earned a fresh start; one that
            # died at once backs off, so a venue outage cannot become a storm
            # of reconnects across every shard. The draw spreads the shards a
            # venue dropped together, each of whose reconnects is a subscribe
            # and a snapshot per book.
            lived = time.monotonic() - opened
            self.backoff_seconds = (
                RECONNECT_BACKOFF_MIN_SECONDS
                if lived >= RECONNECT_HEALTHY_SECONDS
                else min(self.backoff_seconds * 2.0, RECONNECT_BACKOFF_MAX_SECONDS)
            )
            delay = self.backoff_seconds * random.uniform(0.5, 1.0)
            logging.warning("shard %d disconnected; reconnecting in %.1fs", self.index, delay)
            self.stop.wait(delay)

    def _connect_once(self) -> None:
        """One connection's lifetime: open, subscribe, wait until it ends.

        The reader process connects the socket, reads it, sends the venue's
        keepalive and closes it after `reader.SILENCE_SECONDS` of silence; this
        thread asks for the connection and the subscriptions. Returns when the
        link has ended — every frame read from it queued or counted — so the
        next connection's snapshots cannot overtake this one's last deltas;
        `_run` owns the backoff and the reconnect.
        """

        topics = list(self.topics)
        ping_getter = getattr(self.adapter, "ping_message", None)
        ping_msg = ping_getter() if callable(ping_getter) else None
        assert self.reader is not None, "a shard reads through the recorder's reader process"
        try:
            link = self.reader.open(self.adapter.connection_url(topics), tier=self.tier, shard=self, ping=ping_msg)
        except Exception as exc:  # a refused or timed-out connect is a reconnect
            if not self.stop.is_set():
                logging.warning("shard %d could not connect: %s", self.index, exc)
            return
        self.socket = link
        self.link_stats = {}
        try:
            if self.stop.is_set():
                return
            self.connected = True
            self._note(self.on_connect)
            self._reset_chains(topics)
            self._send_all(self.adapter.subscribe_messages(topics))
            logging.info("shard %d connected with %d topics", self.index, len(topics))
            while not self.stop.is_set() and not link.ended.wait(1.0):
                pass
        finally:
            link.request_close()
            # The reader ends a closed link once its frames are queued or have
            # waited out `reader.QUEUE_PUT_TIMEOUT_SECONDS`, and every link when it stops.
            while not link.ended.wait(1.0):
                if not self.reader.alive():
                    break
            self.socket = None
