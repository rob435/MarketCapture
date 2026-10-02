"""The recorder under a day like 10 October 2025, end to end.

The real `Recorder` — shards, the reader and its pipes, the intake,
`FrameQueue`, writer, `SegmentWriter`, compressor, maintainer — records a
scripted venue over real TCP connections speaking websockets, handshake
included; the venue's small send buffers leave what the reader does not take
with the venue. The reader runs on a thread of the test's process rather than
in its own, so the host clock the script steps is the reader's clock too. The script runs a steady load, a burst at twenty times it
with every book restarting at once, a disk slow enough that the writer falls
behind and one fsync stall long enough to overrun the queue, an hour boundary
crossed while the queue is full, every socket closed by the venue at once and
refused for a while after, the host clock stepped back across that boundary
and forward again, the free-space floor crossed while the disk refuses writes,
and throughout, truncated frames and book deltas duplicated and out of order.

What it holds the recorder to:

- every frame the reader read is written or counted, exactly once, on one of
  `dropped_frames`, `disk_dropped_frames`, `malformed_frames`;
- every segment it closed reads back line by line, each row under its own
  hour and symbol, each archive with its receipt;
- the frames the reader holds for a shard never exceed
  `reader.SOCKET_QUEUE_FRAMES` by more than one read;
- the status heartbeat never stops, writing resumes after every fault, and
  the run stops when asked;
- a venue that drops every shard is not answered by every shard at once.

`test_a_burst_day_loses_nothing_it_does_not_count` runs in seconds on a small
universe. With `LM_TAPE_SOAK=1` the same script
runs on the host's universe (520 symbols, 2,080 topics, 14 shards) for longer
phases.
"""

from __future__ import annotations

import errno
import io
import json
import os
import random
import shutil
import socket as sockets
import subprocess
import sys
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pytest
from websockets.frames import Frame, Opcode
from websockets.server import ServerProtocol

from market_tape import pack, reader, record, storage
from market_tape.config import CaptureConfig, Feed, StorageSettings, Tier, Universe, VenueSettings
from market_tape.record import Recorder
from market_tape.venues.bybit import BybitAdapter


HOUR_NS = 3_600 * 1_000_000_000
#: The reader's read here: small, so the one read past the read-ahead bound is small too.
READ_BUFFER_BYTES = 16 * 1024
#: The venue's smallest frame on the wire, a subscribe reply.
SMALLEST_FRAME_BYTES = 34
BOUNDARY_NS = 1_800_000_000 * 1_000_000_000 + HOUR_NS  # 2027-01-15T09:00:00Z
FEEDS = (Feed("book", "50"), Feed("trades"), Feed("ticker"), Feed("liquidations"))
GiB = 1024**3
#: The longest the burst's stalled fsync waits for the queue to overrun.
FSYNC_STALL_MAX_SECONDS = 30.0
#: The venue's send buffer and the reader's receive buffer, in bytes: the TCP window.
WIRE_BUFFER_BYTES = 64 * 1024
REAL_TIME_NS = time.time_ns
REAL_FSYNC = os.fsync
REAL_DISK_USAGE = shutil.disk_usage


@dataclass(frozen=True)
class Script:
    symbols: int
    per_connection: int
    #: Frames a socket receives per 5 ms tick in each regime.
    steady: int
    burst: int
    #: Seconds of each phase.
    warm: float
    burst_for: float
    outage: float
    settle: float
    disk_full_for: float
    queue_frames: int
    #: This venue's items run ~375 B: QUICK's byte bound binds before its
    #: frames do, SOAK's after, so each bound overruns under one script.
    queue_max_mb: float
    read_ahead: int


QUICK = Script(
    symbols=24, per_connection=16, steady=1, burst=20, warm=1.0, burst_for=2.0, outage=1.0,
    settle=1.0, disk_full_for=0.8, queue_frames=2_048, queue_max_mb=0.5, read_ahead=256,
)
SOAK = Script(
    symbols=520, per_connection=150, steady=2, burst=40, warm=10.0, burst_for=20.0, outage=3.0,
    settle=10.0, disk_full_for=5.0, queue_frames=65_536, queue_max_mb=64.0, read_ahead=reader.SOCKET_QUEUE_FRAMES,
)


# ------------------------------------------------------------------ the venue


@dataclass
class Tally:
    """What reached the recorder's process, by what it was."""

    data: set[int] = field(default_factory=set)
    control: int = 0
    malformed: int = 0
    anomalies: int = 0

    @property
    def total(self) -> int:
        return len(self.data) + self.control + self.malformed


class VenueSocket:
    """One connection's venue end.

    The recorder's reader connected to it over TCP and completed the
    handshake, so everything between the wire and the queue is the production
    path. The small send buffer is the venue's TCP window: past it the venue
    holds what it has, and a consumer that reads nothing for a second is one
    the venue lets go.
    """

    def __init__(self, venue: "Venue", number: int, end: sockets.socket, protocol: ServerProtocol) -> None:
        self.venue = venue
        self.number = number
        self.end = end
        self.protocol = protocol
        self.lock = threading.Lock()
        #: What the venue has for this socket and has not yet put on the wire.
        self.outbox: deque[bytes] = deque()
        self.wire = b""
        self.topics: list[str] = []
        self.cursor = 0
        #: Data and malformed frames put wholly on the wire.
        self.sent = 0
        #: Frames from this socket that reached the recorder's queue.
        self.read = 0
        self.closed_by_host = False
        self.closed_by_venue = False
        #: The recorder closed this socket on an overrun.
        self.overran = False
        self.paused_since: float | None = None

    def _hear(self) -> None:
        """Read what the recorder sent: subscriptions and the venue heartbeat."""

        while True:
            try:
                data = self.end.recv(65536)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                data = b""
            if not data:
                self.closed_by_host = True
                return
            self.protocol.receive_data(data)
            for frame in self.protocol.events_received():
                if frame.opcode is Opcode.TEXT:
                    self._answer(json.loads(bytes(frame.data)))
            self.protocol.data_to_send()

    def _answer(self, message: dict[str, Any]) -> None:
        op = message.get("op")
        if op == "subscribe":
            for topic in message.get("args") or []:
                if topic not in self.topics:
                    self.topics.append(topic)
                self.outbox.append(self.venue.subscribed_frame(topic, self.number))
            self.outbox.append(b'{"success":true,"ret_msg":"","conn_id":"c%d","op":"subscribe"}' % self.number)
        elif op == "unsubscribe":
            for topic in message.get("args") or []:
                if topic in self.topics:
                    self.topics.remove(topic)
            self.outbox.append(b'{"success":true,"ret_msg":"","conn_id":"c%d","op":"unsubscribe"}' % self.number)
        elif op == "ping":
            self.outbox.append(b'{"success":true,"ret_msg":"pong","conn_id":"c%d","op":"ping"}' % self.number)

    def pump(self, frames: int) -> None:
        with self.lock:
            if self.closed_by_host or self.closed_by_venue:
                return
            self._hear()
            if self.closed_by_host:
                return
            while frames > 0 and self.topics:
                self.outbox.append(self.venue.data_frame(self.topics[self.cursor % len(self.topics)], self.number))
                self.cursor += 1
                frames -= 1
            while self.wire or self.outbox:
                if not self.wire:
                    self.wire = Frame(Opcode.TEXT, self.outbox.popleft()).serialize(mask=False)
                try:
                    wrote = self.end.send(self.wire)
                except (BlockingIOError, InterruptedError):
                    self.paused_since = self.paused_since or time.monotonic()
                    return
                except OSError:
                    self.closed_by_host = True
                    return
                self.wire = self.wire[wrote:]
                if not self.wire:
                    self.sent += 1
            self.paused_since = None

    def drop(self) -> None:
        with self.lock:
            if not self.closed_by_venue:
                self.closed_by_venue = True
                self.end.close()



class Venue:
    """Every connection the recorder opens, and the frames the venue sends on them."""

    def __init__(self, script: Script, seed: int) -> None:
        self.script = script
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.sockets: list[VenueSocket] = []
        self.connects: list[tuple[float, int]] = []
        self.refused_attempts: list[float] = []
        self.tally = Tally()
        self.books: dict[str, int] = {}
        self.next_id = 1
        self.rate = script.steady
        self.refuse = False
        self.malformed_every = 997
        self.anomaly_every = 401
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="stress-venue", daemon=True)
        self.listener = sockets.create_server(("127.0.0.1", 0))
        self.acceptor = threading.Thread(target=self._accept, name="stress-venue-accept", daemon=True)
        #: Which socket each frame went out on: data frames by id, malformed
        #: ones by their bytes; control frames name theirs (`conn_id`).
        self.socket_of: dict[int, int] = {}
        self.malformed_on: dict[bytes, list[int]] = {}
        #: The token each connection's URL carried, and the link each token opened.
        self.socket_of_token: dict[str, int] = {}
        self.token_of_link: dict[int, str] = {}
        #: The most frames the reader held for one link, after any read.
        self.peak = 0

    def start(self) -> None:
        self.acceptor.start()
        self.thread.start()

    def url(self) -> str:
        return f"ws://127.0.0.1:{self.listener.getsockname()[1]}/v5/public/linear"

    def _accept(self) -> None:
        while True:
            try:
                connection, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._handshake, args=(connection,), name="stress-venue-handshake", daemon=True).start()

    def _handshake(self, connection: sockets.socket) -> None:
        with self.lock:
            refuse = self.refuse
            if refuse:
                self.refused_attempts.append(time.monotonic())
        if refuse:
            connection.close()
            return
        protocol = ServerProtocol(max_size=None)
        try:
            connection.settimeout(5.0)
            requests: list[Any] = []
            while not requests:
                data = connection.recv(65536)
                if not data:
                    raise ConnectionError("the reader left during the handshake")
                protocol.receive_data(data)
                requests = protocol.events_received()
            request = requests[0]
            protocol.send_response(protocol.accept(request))
            connection.sendall(b"".join(protocol.data_to_send()))
            connection.setsockopt(sockets.SOL_SOCKET, sockets.SO_SNDBUF, WIRE_BUFFER_BYTES)
            connection.setblocking(False)
        except OSError:
            connection.close()
            return
        token = request.path.rpartition("?c=")[2]
        with self.lock:
            venue_socket = VenueSocket(self, len(self.sockets), connection, protocol)
            self.sockets.append(venue_socket)
            self.connects.append((time.monotonic(), venue_socket.number))
            self.socket_of_token[token] = venue_socket.number

    def took(self, payloads: list[bytes]) -> None:
        """Frames the recorder's intake queued: what the reader read and handed on."""

        for raw in payloads:
            number: int | None = None
            try:
                message = json.loads(raw)
            except ValueError:
                with self.lock:
                    owners = self.malformed_on.get(bytes(raw))
                    number = owners.pop(0) if owners else None
                self.delivered("malformed", 0)
            else:
                topic = str(message.get("topic") or "")
                data = message.get("data")
                frame_id = 0
                if topic.startswith("orderbook."):
                    frame_id = int(data["seq"])
                elif topic.startswith("tickers."):
                    frame_id = int(message["cs"])
                elif topic.startswith("publicTrade."):
                    frame_id = int(data[0]["i"])
                elif topic.startswith("allLiquidation."):
                    frame_id = int(data[0]["T"])
                if frame_id:
                    self.delivered("data", frame_id)
                    number = self.socket_of.get(frame_id)
                else:
                    self.delivered("control", 0)
                    number = int(str(message.get("conn_id") or "c-1")[1:])
            if number is not None and number >= 0:
                with self.lock:
                    self.sockets[number].read += 1

    def delivered(self, kind: str, frame_id: int) -> None:
        with self.lock:
            if kind == "data":
                self.tally.data.add(frame_id)
            elif kind == "malformed":
                self.tally.malformed += 1
            else:
                self.tally.control += 1

    def _id(self) -> int:
        with self.lock:
            frame_id = self.next_id
            self.next_id += 1
            return frame_id

    def subscribed_frame(self, topic: str, number: int) -> bytes:
        frame_id = self._id()
        kind, _, symbol = topic.rpartition(".")
        with self.lock:
            self.socket_of[frame_id] = number
        if topic.startswith("orderbook."):
            u = self.books.setdefault(topic, 100)
            return self._book(topic, symbol, frame_id, "snapshot", u)
        if topic.startswith("tickers."):
            return self._ticker(topic, symbol, frame_id, "snapshot")
        return b'{"success":true,"op":"subscribe","conn_id":"c%d"}' % number

    def data_frame(self, topic: str, number: int) -> bytes:
        frame_id = self._id()
        with self.lock:
            self.socket_of[frame_id] = number
        symbol = topic.rpartition(".")[2]
        if topic.startswith("orderbook."):
            u = self.books.get(topic, 100) + 1
            if frame_id % self.anomaly_every == 0:
                with self.lock:
                    self.tally.anomalies += 1
                # A duplicate one time, a delta from ahead of the stream the next.
                u = u - 1 if (frame_id // self.anomaly_every) % 2 else u + 1
            self.books[topic] = max(self.books.get(topic, 100), u)
            raw = self._book(topic, symbol, frame_id, "delta", u)
        elif topic.startswith("publicTrade."):
            ms = REAL_TIME_NS() // 1_000_000
            raw = (
                '{"topic":"%s","type":"snapshot","ts":%d,"data":[{"T":%d,"s":"%s","S":"Buy","v":"0.1","p":"50000.5","i":"%d"}]}'
                % (topic, ms, ms, symbol, frame_id)
            ).encode()
        elif topic.startswith("tickers."):
            raw = self._ticker(topic, symbol, frame_id, "delta")
        else:
            raw = (
                '{"topic":"%s","type":"snapshot","ts":%d,"data":[{"T":%d,"s":"%s","S":"Sell","v":"12","p":"0.5"}]}'
                % (topic, REAL_TIME_NS() // 1_000_000, frame_id, symbol)
            ).encode()
        if frame_id % self.malformed_every == 0:
            with self.lock:
                self.malformed_on.setdefault(raw[: len(raw) // 2], []).append(number)
            return raw[: len(raw) // 2]
        return raw

    @staticmethod
    def _book(topic: str, symbol: str, frame_id: int, kind: str, u: int) -> bytes:
        levels = 50 if kind == "snapshot" else 3
        bids = ",".join('["%d.5","%d"]' % (50_000 - n, n + 1) for n in range(levels))
        asks = ",".join('["%d.5","%d"]' % (50_001 + n, n + 1) for n in range(levels))
        return (
            '{"topic":"%s","type":"%s","ts":%d,"data":{"s":"%s","b":[%s],"a":[%s],"u":%d,"seq":%d},"cts":%d}'
            % (topic, kind, REAL_TIME_NS() // 1_000_000, symbol, bids, asks, u, frame_id, frame_id)
        ).encode()

    @staticmethod
    def _ticker(topic: str, symbol: str, frame_id: int, kind: str) -> bytes:
        return (
            '{"topic":"%s","type":"%s","cs":%d,"ts":%d,"data":{"symbol":"%s","lastPrice":"1.5","markPrice":"1.5",'
            '"fundingRate":"0.0001","turnover24h":"1000000","openInterest":"50"}}'
            % (topic, kind, frame_id, REAL_TIME_NS() // 1_000_000, symbol)
        ).encode()

    def _run(self) -> None:
        while not self.stop.is_set():
            with self.lock:
                sockets = list(self.sockets)
            for socket in sockets:
                socket.pump(self.rate)
                # A consumer that reads nothing for a second is one the venue lets go.
                if socket.paused_since is not None and time.monotonic() - socket.paused_since > 1.0:
                    socket.drop()
            time.sleep(0.005)

    def restart_books(self) -> None:
        """The venue's book service restarting: a `u = 1` snapshot for every book on every socket."""

        for socket in self.open_sockets():
            with socket.lock:
                for topic in socket.topics:
                    if topic.startswith("orderbook."):
                        self.books[topic] = 1
                        frame_id = self._id()
                        with self.lock:
                            self.socket_of[frame_id] = socket.number
                        socket.outbox.append(self._book(topic, topic.rpartition(".")[2], frame_id, "snapshot", 1))

    def drop_all(self) -> None:
        with self.lock:
            sockets = list(self.sockets)
        for socket in sockets:
            socket.drop()

    def open_sockets(self) -> list[VenueSocket]:
        with self.lock:
            return [s for s in self.sockets if not (s.closed_by_host or s.closed_by_venue)]

    def unread(self) -> int:
        """Frames the venue put on a wire that never reached the recorder's
        queue. A socket the reader closed is left out: what the kernel still
        held for it was never read, and what it read and dropped is counted."""

        with self.lock:
            sockets = list(self.sockets)
        return sum(
            socket.sent - socket.read for socket in sockets if not (socket.overran or socket.closed_by_host)
        )


# ------------------------------------------------------------------ the host


class Host:
    """The capture host around the recorder: its wall clock, its disk, its fsync."""

    def __init__(self) -> None:
        self.offset_ns = 0
        self.free_bytes = 100 * GiB
        self.refuse_writes = False
        self.torn = False
        self.write_delay = 0.0
        #: Given, the writer's next fsync waits for it (the reader's first
        #: overrun), at most `FSYNC_STALL_MAX_SECONDS`.
        self.fsync_stall: threading.Event | None = None

    def time_ns(self) -> int:
        return REAL_TIME_NS() + self.offset_ns

    def fsync(self, descriptor: int) -> None:
        # The slow disk is the tape's: the writer thread's fsyncs.
        if threading.current_thread().name == "tape-writer":
            stall, self.fsync_stall = self.fsync_stall, None
            if stall is not None:
                stall.wait(FSYNC_STALL_MAX_SECONDS)
            time.sleep(self.write_delay)
        REAL_FSYNC(descriptor)

    def disk_usage(self, root: Path) -> Any:
        real = REAL_DISK_USAGE(root)
        return real._replace(free=self.free_bytes)


class _HostRaw(io.RawIOBase):
    def __init__(self, inner: Any, host: Host) -> None:
        self.inner = inner
        self.host = host

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        if self.host.write_delay:
            time.sleep(self.host.write_delay)
        if self.host.refuse_writes:
            if not self.host.torn and len(data) > 1:
                self.host.torn = True
                return int(self.inner.write(bytes(data[: len(data) // 2])))
            raise OSError(errno.ENOSPC, "No space left on device")
        return int(self.inner.write(data))

    def fileno(self) -> int:
        return int(self.inner.fileno())

    def close(self) -> None:
        self.inner.close()
        super().close()


class CountingAdapter(BybitAdapter):
    """The Bybit adapter, counting the frames it took that carried no row, and
    answering the table fetch without the network."""

    def __init__(self, symbols: list[str], url: str) -> None:
        super().__init__(ws_url=url, rest_url="http://unused")
        self.symbols = symbols
        self.empty = 0
        self.connections = 0

    def connection_url(self, topics: list[str]) -> str:
        """The venue's URL with a token naming this connection, so the venue
        can tell which of its sockets a link the reader overran was."""

        self.connections += 1
        return f"{self.ws_url}?c={self.connections}"

    def normalize(self, raw: str | bytes, received_ns: int, received_mono_ns: int = 0) -> list[dict[str, Any]]:
        rows = super().normalize(raw, received_ns, received_mono_ns)
        if not rows:
            self.empty += 1
        return rows

    def fetch_tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "instruments": [
                {"symbol": s, "status": "Trading", "quoteCoin": "USDT", "settleCoin": "USDT",
                 "contractType": "LinearPerpetual", "symbolType": ""}
                for s in self.symbols
            ],
            "tickers": [{"symbol": s, "fundingRate": "0.0001", "turnover24h": "1000000"} for s in self.symbols],
        }


def symbols_of(count: int) -> list[str]:
    return [f"S{n:03d}USDT" for n in range(count)]


# ------------------------------------------------------------------ the run


@dataclass
class Observed:
    status_writes: list[float] = field(default_factory=list)
    queue_peak: int = 0
    byte_peak: int = 0
    phases: dict[str, float] = field(default_factory=dict)
    rows_at: dict[str, int] = field(default_factory=dict)


def _watch(recorder: Recorder, root: Path, observed: Observed, stop: threading.Event) -> None:
    last = None
    while not stop.is_set():
        try:
            stamp = (root / "status.json").stat().st_mtime_ns
        except FileNotFoundError:
            stamp = None
        # The heartbeat of a running recorder: once stopped it joins the
        # maintainer first and writes its last status after the compressor has
        # drained, which on a loaded host is tens of seconds of shutdown.
        if stamp is not None and stamp != last and not recorder.stop.is_set():
            observed.status_writes.append(time.monotonic())
            last = stamp
        observed.queue_peak = max(observed.queue_peak, recorder.frames.qsize())
        observed.byte_peak = max(observed.byte_peak, recorder.frames.queued_bytes)
        time.sleep(0.01)


def _wait_until(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


def _stacks() -> str:
    frames = sys._current_frames()
    return "\n".join(
        f"--- {thread.name}\n" + "".join(traceback.format_stack(frames[thread.ident]))
        for thread in threading.enumerate()
        if thread.ident in frames and thread.name.startswith("tape")
    )


def run_burst_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, script: Script, spawn: Callable[[int, int], Any], seed: int = 11
) -> dict[str, Any]:
    root = tmp_path / "tape"
    symbols = symbols_of(script.symbols)
    venue = Venue(script, seed)
    host = Host()
    # The hour ends while the burst has the queue full.
    host.offset_ns = BOUNDARY_NS - REAL_TIME_NS() - int((script.warm + script.burst_for / 2) * 1e9)

    queued_by_intake = record.ReaderProcess._queue

    def queued(intake: record.ReaderProcess, items: list[Any], owners: list[Any]) -> None:
        venue.took([item[reader.EVENT.size :] for item in items])
        queued_by_intake(intake, items, owners)

    monkeypatch.setattr(record.ReaderProcess, "_queue", queued)
    commanded = record.ReaderProcess.command

    def command(intake: record.ReaderProcess, message: Any) -> None:
        if message.get("op") == "open":
            venue.token_of_link[int(message["link"])] = str(message["url"]).rpartition("?c=")[2]
        commanded(intake, message)

    monkeypatch.setattr(record.ReaderProcess, "command", command)
    cut_by_reader = reader.Reader._frames

    def cut(loop: reader.Reader, link: reader.Link, data: bytes, now_ns: int, mono_ns: int) -> None:
        cut_by_reader(loop, link, data, now_ns, mono_ns)
        venue.peak = max(venue.peak, len(link.pending))

    monkeypatch.setattr(reader.Reader, "_frames", cut)
    # The reader's own overrun, when it drops a link's held frames: the
    # recorder hears of it only once the frames ahead of it in the pipe are
    # queued, which a stalled writer holds up.
    overrun = threading.Event()
    overrun_by_reader = reader.Reader._overrun

    def overrun_in_reader(loop: reader.Reader, link: reader.Link) -> None:
        overrun.set()
        overrun_by_reader(loop, link)

    monkeypatch.setattr(reader.Reader, "_overrun", overrun_in_reader)
    monkeypatch.setattr(reader, "READ_BUFFER_BYTES", READ_BUFFER_BYTES)
    monkeypatch.setattr(reader, "QUEUE_PUT_TIMEOUT_SECONDS", 0.25)
    monkeypatch.setattr(reader, "SOCKET_QUEUE_FRAMES", script.read_ahead)
    monkeypatch.setattr(record, "random", random.Random(seed))
    monkeypatch.setattr(record, "RECONNECT_BACKOFF_MIN_SECONDS", 0.3)
    monkeypatch.setattr(record, "RECONNECT_HEALTHY_SECONDS", 0.5)
    monkeypatch.setattr(time, "time_ns", host.time_ns)
    monkeypatch.setattr(os, "fsync", host.fsync)
    monkeypatch.setattr(shutil, "disk_usage", host.disk_usage)

    config = CaptureConfig(
        venue=VenueSettings("bybit", "linear"),
        storage=StorageSettings(
            root=root,
            segment_max_mb=1.0,
            fsync_every_records=500,
            queue_frames=script.queue_frames,
            queue_max_mb=script.queue_max_mb,
            status_interval_seconds=0.2,
            min_free_disk_gb=1.0,
            max_disk_gb=1_000.0,
        ),
        tiers=(Tier("crypto_perps", FEEDS, Universe("listed", quote="USDT")),),
        topics_per_connection=script.per_connection,
        reanchor_each_hour=True,
        snapshot_cadence="hour",
    )
    adapter = CountingAdapter(symbols, venue.url())
    recorder = Recorder(config, adapter=adapter)
    recorder.reader = record.ReaderProcess(recorder.frames, spawn=spawn)
    monkeypatch.setattr(recorder, "_install_signals", lambda: None)
    overran_by_reader = recorder._on_overrun

    def overran(shard: Any = None, frames: int = 1) -> None:
        link = getattr(shard, "socket", None)
        token = venue.token_of_link.get(link.ident) if link is not None else None
        number = venue.socket_of_token.get(token) if token is not None else None
        if number is not None:
            venue.sockets[number].overran = True
        overran_by_reader(shard, frames)

    monkeypatch.setattr(recorder, "_on_overrun", overran)
    prunes: list[float] = []
    monkeypatch.setattr(recorder.retention, "prune", lambda now=None, *, free_credit=0: prunes.append(time.monotonic()) or [])
    opened = recorder.writer._open

    def open_on_the_host_disk(symbol: str, day: str, hour: str) -> Any:
        segment = opened(symbol, day, hour)
        segment.handle = io.BufferedWriter(_HostRaw(segment.handle.detach(), host), recorder.writer.buffer_bytes)
        return segment

    monkeypatch.setattr(recorder.writer, "_open", open_on_the_host_disk)

    failures: list[BaseException] = []

    def run() -> None:
        try:
            recorder.run()
        except BaseException as exc:  # reported on the test's thread
            failures.append(exc)

    observed = Observed()
    watching = threading.Event()
    watcher = threading.Thread(target=_watch, args=(recorder, root, observed, watching), daemon=True)
    runner = threading.Thread(target=run, name="stress-recorder", daemon=True)
    venue.start()
    runner.start()
    watcher.start()
    try:
        assert _wait_until(lambda: len(venue.open_sockets()) == len(recorder._all_shards()) > 0, 10.0)
        shards = len(recorder._all_shards())
        time.sleep(script.warm)

        # The burst: every book restarts at once, the wire runs at twenty times
        # the steady rate, the disk is slow, and one fsync stalls until the
        # reader overruns. The hour ends in the middle of it. The burst lasts
        # until that overrun too: on a starved host the venue fills the queue,
        # the pipe and the reader's holds more slowly than the script's clock.
        observed.phases["burst"] = time.monotonic()
        venue.restart_books()
        venue.rate = script.burst
        host.write_delay = 0.0005
        host.fsync_stall = overrun
        time.sleep(script.burst_for)
        assert overrun.wait(FSYNC_STALL_MAX_SECONDS), "the burst never overran the queue"
        venue.rate = script.steady
        host.write_delay = 0.0
        observed.rows_at["burst_end"] = recorder.written_rows

        # Every socket dropped by the venue at once, and refused for a while.
        observed.phases["outage"] = time.monotonic()
        venue.refuse = True
        venue.drop_all()
        time.sleep(script.outage)
        venue.refuse = False
        observed.phases["reopen"] = time.monotonic()
        assert _wait_until(lambda: len(venue.open_sockets()) == shards, 20.0), "shards did not come back"
        observed.rows_at["reopened"] = recorder.written_rows
        assert _wait_until(lambda: recorder.written_rows > observed.rows_at["reopened"], 5.0), (
            "writing never resumed after the shards came back"
        )

        # The host clock stepped back across the hour, then forward again.
        step = host.time_ns() - (BOUNDARY_NS - 1_000_000_000)
        host.offset_ns -= step
        time.sleep(0.5)
        host.offset_ns += step
        time.sleep(0.5)

        # The disk refuses writes before free space says so, while the clock
        # steps back across the hour so every symbol has a segment to close;
        # then the free-space floor too.
        observed.phases["disk_full"] = time.monotonic()
        host.refuse_writes = True
        step = host.time_ns() - (BOUNDARY_NS - 1_000_000_000)
        host.offset_ns -= step
        time.sleep(script.disk_full_for / 2)
        host.free_bytes = GiB // 2
        time.sleep(script.disk_full_for / 2)
        observed.rows_at["disk_full_end"] = recorder.written_rows
        host.refuse_writes = False
        host.torn = False
        host.free_bytes = 100 * GiB
        host.offset_ns += step
        observed.phases["disk_back"] = time.monotonic()
        assert _wait_until(lambda: not recorder.disk_blocked, 5.0), "the disk gate never reopened"
        assert _wait_until(lambda: recorder.written_rows > observed.rows_at["disk_full_end"], 5.0), (
            "writing never resumed after the disk came back"
        )

        time.sleep(script.settle)
        # Quiet the wire and let everything that reached the host be read.
        venue.stop.set()
        venue.thread.join(5.0)
        venue.listener.close()
        assert _wait_until(lambda: venue.unread() == 0 and recorder.frames.qsize() == 0, 30.0), (
            f"the recorder stopped reading: {venue.unread()} unread, {recorder.frames.qsize()} queued\n{_stacks()}"
        )
    finally:
        venue.stop.set()
        venue.listener.close()
        recorder.stop.set()
        runner.join(60.0)
        watching.set()
        watcher.join(5.0)
    assert not runner.is_alive(), f"the recorder did not stop:\n{_stacks()}"
    assert failures == [], failures
    return {
        "root": root,
        "venue": venue,
        "recorder": recorder,
        "adapter": adapter,
        "observed": observed,
        "shards": shards,
        "prunes": prunes,
    }


# ------------------------------------------------------------- the readback


def _frame_id(row: dict[str, Any]) -> int:
    kind = row["kind"]
    if kind in ("orderbook_snapshot", "orderbook_delta"):
        return int(row["exchange_engine_ts_ns"]) // 1_000_000
    if kind == "public_trade":
        return int(row["trade_id"])
    if kind == "ticker":
        return int(row["cross_sequence"])
    return int(row["exchange_ts_ns"]) // 1_000_000


def read_back(root: Path) -> tuple[list[int], dict[str, int]]:
    """Every row of every closed segment, checked line by line against the
    directory it sits in and the receipt it was given."""

    receipts = storage.read_receipts(root / "manifest.jsonl")
    ids: list[int] = []
    segments: dict[str, int] = {}
    for archive in sorted(root.rglob("segment-*.jsonl.zst")):
        day, hour, symbol = storage.segment_identity(archive, root)
        text = subprocess.run(["zstd", "-dcq", str(archive)], check=True, capture_output=True).stdout
        assert text.endswith(b"\n"), f"{archive} ends mid-line"
        lines = text.splitlines()
        for line in lines:
            row = json.loads(line)
            assert row["symbol"] == symbol
            assert storage.utc_day_hour(int(row["local_receive_ts_ns"])) == (day, hour), f"{archive}: a row of another hour"
            ids.append(_frame_id(row))
        relative = str(archive.relative_to(root))
        assert receipts[relative]["records"] == len(lines), f"{archive}: its receipt counts another file"
        key = f"{day}T{hour}/{symbol}"
        segments[key] = segments.get(key, 0) + 1
    return ids, segments


def assert_burst_day(outcome: dict[str, Any], script: Script) -> None:
    root: Path = outcome["root"]
    venue: Venue = outcome["venue"]
    recorder: Recorder = outcome["recorder"]
    adapter: CountingAdapter = outcome["adapter"]
    observed: Observed = outcome["observed"]
    tally = venue.tally

    # Every frame the venue put on a socket the reader kept open reached the queue.
    assert venue.unread() == 0, "frames reached the host and never reached the queue"

    # The stop closed every open segment raw and left it: nothing half-written.
    assert not list(root.rglob("*.partial")) and not list(root.rglob("*.tmp"))
    raw = list(root.rglob("segment-*.jsonl"))
    assert raw, "the stop compressed the open segments it should have left for recovery"
    # What an idle root's hourly `market_tape pack` does with them.
    recovered = pack.recover_idle_root(pack.Tape("stress", root, "unused:"), timeout=120.0, dry_run=False)
    assert recovered == {"raw": len(raw), "recovered": len(raw), "failed": 0, "held": False}, recovered
    left = [p for p in root.rglob("segment-*") if not p.name.endswith(".zst")] + list(root.rglob("*.tmp"))
    assert left == []
    assert not [p for p in (root.rglob("_meta/*")) if not p.name.endswith(".zst")]
    for line in (root / "manifest.jsonl").read_bytes().splitlines():
        json.loads(line)

    ids, segments = read_back(root)
    written = set(ids)
    assert len(ids) == len(written), "a frame was written twice"
    assert written <= tally.data, "the tape holds a frame the venue never sent"

    # Every frame the reader read reached the queue or was counted dropped, and
    # every frame the queue took is on the tape or on one counter.
    assert recorder.received_frames == tally.total + recorder.dropped_frames
    assert recorder.malformed_frames <= tally.malformed
    accounted = len(written) + adapter.empty + recorder.malformed_frames + recorder.disk_dropped_frames
    assert accounted == tally.total, {
        "queued": tally.total,
        "written": len(written),
        "empty": adapter.empty,
        "malformed": recorder.malformed_frames,
        "dropped": recorder.dropped_frames,
        "disk_dropped": recorder.disk_dropped_frames,
    }
    status = json.loads((root / "status.json").read_text())
    for name in ("received_frames", "dropped_frames", "disk_dropped_frames", "malformed_frames"):
        assert status[name] == getattr(recorder, name)

    # The script did what it says: the burst overran, the disk dropped, frames
    # were malformed, and the hour turned with rows on both sides.
    assert recorder.dropped_frames > 0, "the burst never overran the queue"
    assert recorder.disk_dropped_frames > 0, "the full disk dropped nothing"
    assert recorder.malformed_frames > 0 and tally.anomalies > 0
    hours = {key.split("/")[0] for key in segments}
    assert len(hours) == 2, hours
    # An hour turned under a full queue and a clock stepped across it costs a
    # symbol a few segments, not one per roll.
    assert max(segments.values()) <= 8, max(segments.items(), key=lambda item: item[1])

    # Memory: the read-ahead held to its bound, the queue to both of its bounds,
    # and the per-topic state to the topics.
    assert venue.peak <= script.read_ahead + READ_BUFFER_BYTES // SMALLEST_FRAME_BYTES
    assert observed.queue_peak <= script.queue_frames
    assert observed.byte_peak <= recorder.frames.max_bytes
    assert recorder.frames.queued_bytes == 0, "the drained queue still counts bytes"
    topics = 4 * script.symbols
    assert len(adapter.sequences) <= script.symbols
    assert len(recorder.resync_pending) + len(recorder.resync_outstanding) <= topics

    # Never wedged: the heartbeat kept its cadence through every fault.
    gaps = [b - a for a, b in zip(observed.status_writes, observed.status_writes[1:])]
    assert gaps and max(gaps) < 3.0, f"the status heartbeat stalled for {max(gaps):.1f}s"

    # The venue's drop was not answered all at once, nor retried in a storm.
    reopened = sorted(at for at, _ in venue.connects if at >= observed.phases["reopen"])[: outcome["shards"]]
    assert len(reopened) == outcome["shards"]
    assert reopened[-1] - reopened[0] >= 0.1 * record.RECONNECT_BACKOFF_MIN_SECONDS, "every shard reconnected at once"
    attempts = [at for at in venue.refused_attempts if observed.phases["outage"] <= at <= observed.phases["reopen"]]
    # Backoff doubles from 0.3 s: over the outage a shard tries a handful of times.
    assert len(attempts) <= outcome["shards"] * 4, f"{len(attempts)} refused attempts from {outcome['shards']} shards"


def test_a_burst_day_loses_nothing_it_does_not_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader_in_thread: Any
) -> None:
    outcome = run_burst_day(tmp_path, monkeypatch, QUICK, reader_in_thread)
    assert_burst_day(outcome, QUICK)


@pytest.mark.skipif(not os.environ.get("LM_TAPE_SOAK"), reason="the host-sized burst day runs with LM_TAPE_SOAK=1")
def test_a_burst_day_on_the_hosts_universe_loses_nothing_it_does_not_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader_in_thread: Any
) -> None:
    outcome = run_burst_day(tmp_path, monkeypatch, SOAK, reader_in_thread)
    assert_burst_day(outcome, SOAK)
