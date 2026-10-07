"""The capture reader: every shard's websocket, read in a process of its own.

The recorder (`market_tape.record`) starts one reader beside itself
(`python -m market_tape.reader`) and speaks to it over two pipes. The reader
owns every socket: it connects, completes TLS and the websocket handshake,
sends what the recorder asks, keeps each connection alive, reads every frame,
stamps it, and hands the frames back through the events pipe. It parses no
frame's JSON and knows no venue: the recorder names the URL and the keepalive
message, and the recorder's writer normalizes what comes back.

Why a process: the recorder's writer parses and serialises every frame under
the interpreter lock, and a reader thread on that lock waited out the
writer's turn before every read. On the capture host the lock's hand-offs
between the two cost more CPU than either thread's work, and every stamp
waited with them. This interpreter runs nothing but the reader.

Commands, recorder to reader, one JSON object a line:

- `{"op": "open", "link": N, "url": ..., "shard": I, "ping": text|null}`
- `{"op": "send", "link": N, "text": ...}`
- `{"op": "close", "link": N}`
- `{"op": "stop"}`: close every link, hand back what they read, exit.

End of file on the commands pipe is the recorder gone: the reader exits at
once. SIGTERM and SIGINT are the recorder's to act on, and ignored here, so a
unit stop does not cut off frames the recorder is still owed.

Events, reader to recorder: `EVENT` then `length` bytes of body.

- `FRAME`: a message's payload, with the wall and monotonic stamps.
- `OPENED`: the link is open; frames and sends may follow.
- `FAILED`: the link never opened; the body says why.
- `DROPPED`: the recorder did not take this link's frames within
  `QUEUE_PUT_TIMEOUT_SECONDS`; `count` of them are gone and the link closes.
- `ENDED`: the socket is closed and every frame read from it is in the
  events pipe ahead of this.
- `STATS`: every `STATS_SECONDS`, the link's account of itself as a JSON
  object: `tcp`, the kernel's `TCP_INFO_FIELDS` for the socket; `ping_rtt_ms`,
  the protocol ping's round trip to the answer's kernel receive stamp (null
  for a venue with a heartbeat message of its own, or before an answer);
  and, since the last, `reads` that completed a message, the `frames` they
  completed (the frames of one read share its stamp, so `frames` over
  `reads` is how many one stamp covers), and `dwell_max_ms`, the longest
  the kernel held a read's newest packet before the read (zero while the
  kernel stamps nothing).

Stamps: each frame carries the wall clock and the monotonic clock of the
read that completed it. On Linux every socket asks the kernel for its receive
time (`SO_TIMESTAMPNS`): a read reports when the newest packet it took
arrived, and the frames it completed are stamped that much earlier than the
read. The reader reads a socket as soon as the kernel says it holds bytes, so
the frames one read carries are those that landed while the pass before it
ran, and share the newest one's stamp. A read whose newest packet the kernel
stamped before the read before it (a segment TCP recovered behind the one that
overtook it) is stamped at that earlier read's instant, both clocks by the same
shift: on one link the stamps never step back, which is the tape's sort key.
"""

from __future__ import annotations

import argparse
import fcntl
import heapq
import json
import logging
import os
import select
import selectors
import signal
import socket as sockets
import ssl
import struct
import sys
import threading
import time
from typing import Any

from websockets.client import ClientProtocol
from websockets.frames import Frame, Opcode
from websockets.http11 import USER_AGENT
from websockets.protocol import State
from websockets.uri import parse_uri

#: One read's most; every read lands in the reader's one buffer of this size.
READ_BUFFER_BYTES = 256 * 1024
#: A TLS record carries at most 16 KiB of plaintext. Asked for more, the
#: `ssl` module allocates the whole request for every record it returns.
TLS_RECORD_BYTES = 16 * 1024
#: Frames the reader holds for one link that the recorder has not taken. At
#: it the reader stops reading that socket and TCP holds the rest, one read's
#: frames past it at most.
SOCKET_QUEUE_FRAMES = 4096
#: How long a held frame waits for the recorder before its link overruns:
#: its frames are dropped and counted, and the link closes so the shard
#: reconnects for fresh snapshots.
QUEUE_PUT_TIMEOUT_SECONDS = 5.0
#: Events not yet in the pipe past which no more frames are encoded; held
#: frames stay with their link, where the overrun bound counts them.
EVENTS_BUFFER_BYTES = 1024 * 1024
#: The events pipe takes at most one write this often, unless the events
#: waiting fill `EVENTS_FLUSH_BYTES`: a write a pass was a syscall a pass,
#: and the recorder takes them on its own tick (`record.INTAKE_TICK_SECONDS`).
#: A pass after a quiet spell writes at once. Frames carry their stamps, so
#: this delays the queue and never the tape's clocks; a stopping reader
#: writes at once.
EVENTS_FLUSH_SECONDS = 0.005
EVENTS_FLUSH_BYTES = 64 * 1024
_EVENTS_FLUSH_NS = int(EVENTS_FLUSH_SECONDS * 1e9)
#: What the events pipe holds, where the kernel allows it (Linux's
#: `fs.pipe-max-size` is 1 MiB by default): the recorder drains it on a tick.
PIPE_BYTES = 1024 * 1024
#: The wait while frames are held for a recorder that has no room.
QUEUE_RETRY_SECONDS = 0.01
#: TCP connect, TLS and the websocket handshake, together.
CONNECT_TIMEOUT_SECONDS = 10.0
#: A link's keepalive cadence, and the silence after which it closes.
PING_INTERVAL_SECONDS = 20.0
SILENCE_SECONDS = 60.0
KEEPALIVE_CHECK_SECONDS = 1.0
#: A close the venue does not read ends the socket anyway after this.
CLOSE_TIMEOUT_SECONDS = 5.0
#: A handshake response longer than this is not one.
HANDSHAKE_MAX_BYTES = 64 * 1024
#: A link's `STATS` cadence, from its open.
STATS_SECONDS = 10.0

#: `(length, kind, link, a, b)`: a frame's `a` and `b` are its wall and
#: monotonic stamps; a drop's are the count and the newest dropped frame's
#: wall stamp.
EVENT = struct.Struct("<IBIqq")
FRAME, OPENED, FAILED, DROPPED, ENDED, STATS = range(6)

#: What `STATS` carries of the kernel's `struct tcp_info`
#: (include/uapi/linux/tcp.h), as (name, byte offset, format), all there
#: from Linux 5.4. Round trips are the socket's own estimates in µs: `rtt_us`
#: and `min_rtt_us` (the least over the kernel's last five minutes) from the
#: acknowledgements of what this end sends, pings and subscriptions;
#: `rcv_rtt_us` from the receive side's timestamps. `rcv_ooopack` counts the
#: packets that arrived behind a hole (a loss or a reordering on the way in),
#: of `data_segs_in`: a round trip's packets for each loss, every one held
#: until the retransmission lands.
TCP_INFO_FIELDS = (
    ("rtt_us", 68, "=I"),
    ("rttvar_us", 72, "=I"),
    ("rcv_rtt_us", 92, "=I"),
    ("min_rtt_us", 148, "=I"),
    ("data_segs_in", 152, "=I"),
    ("rcv_ooopack", 224, "=I"),
)
TCP_INFO_BYTES = 256

#: Linux's `SO_TIMESTAMPNS`, which the socket module does not name: the
#: kernel's receive time of a read's newest packet, as a `struct timespec`.
SO_TIMESTAMPNS = 35 if sys.platform.startswith("linux") else None
TIMESPEC = struct.Struct("@qq")
ANCILLARY_BYTES = sockets.CMSG_SPACE(TIMESPEC.size) if SO_TIMESTAMPNS is not None else 0
#: A kernel stamp further from the read than this is a clock step, not a wait.
MAX_DWELL_NS = 10 * 1_000_000_000

_U16 = struct.Struct("!H").unpack_from
_U64 = struct.Struct("!Q").unpack_from
#: The protocol ping's payload: its monotonic send time, which the answer echoes.
_PING = struct.Struct("!q")
_CLOSE_NORMAL = b"\x03\xe8"


class ProtocolError(Exception):
    """The venue broke the websocket framing."""


def _client_frame(opcode: Opcode, payload: bytes) -> bytes:
    return Frame(opcode, payload).serialize(mask=True)


class Link:
    """One shard's websocket after its handshake, the reader's alone.

    TLS runs in memory (`ssl.MemoryBIO`): the socket is read raw, as much as
    the kernel holds in one call, and the records are decrypted here. Read
    through `SSLSocket`, OpenSSL fetched each record from the kernel itself,
    and Bybit sends each message as its own record: two syscalls a message.
    Frames are cut here too; the websocket library does the handshake only.
    """

    def __init__(
        self,
        ident: int,
        sock: sockets.socket,
        *,
        shard: Any = None,
        ping: str | None = None,
        tls: ssl.SSLObject | None = None,
        incoming: ssl.MemoryBIO | None = None,
        outgoing: ssl.MemoryBIO | None = None,
        plain: bytes = b"",
    ) -> None:
        self.ident = ident
        self.sock = sock
        self.shard = shard
        self.ping = ping
        self.tls = tls
        self.incoming = incoming
        self.outgoing = outgoing
        #: Plaintext past the last whole frame, and how long it must grow before
        #: the next frame is whole (0 while its header is not).
        self.plain = bytearray(plain)
        self.need = 0
        self.fragments: list[bytes] | None = None
        #: `(payload, wall_ns, mono_ns)` read and not yet handed to the recorder.
        self.pending: list[tuple[bytes, int, int]] = []
        #: Bytes for the socket, after TLS.
        self.wire = bytearray()
        self.stamps = False
        self.open = True
        self.reading = True
        self.closing = False
        self.closing_since_ns = 0
        self.registered = 0
        now_ns = time.monotonic_ns()
        self.last_data_mono_ns = now_ns
        #: The wall stamp of this link's last read; a later read never stamps
        #: behind it.
        self.last_wall_ns = 0
        self.next_ping_ns = now_ns + int(PING_INTERVAL_SECONDS * 1e9)
        #: The protocol ping awaiting its answer (its monotonic send time, 0
        #: for none), and the last answer's round trip.
        self.ping_sent_ns = 0
        self.ping_rtt_ns = 0
        #: `STATS`' counters since the last.
        self.next_stats_ns = now_ns + int(STATS_SECONDS * 1e9)
        self.reads = 0
        self.frames = 0
        self.dwell_max_ns = 0

    def decrypt(self, raw: memoryview) -> tuple[bytes, bool]:
        """The plaintext `raw` completes, and whether the venue closed TLS."""

        tls, incoming = self.tls, self.incoming
        if tls is None or incoming is None:
            return bytes(raw), False
        incoming.write(raw)
        first = b""
        parts: list[bytes] | None = None
        closed = False
        # Asked only while there are bytes to decrypt: a read that finds none
        # raises, and building the exception costs more than the record.
        while incoming.pending or tls.pending():
            try:
                part = tls.read(TLS_RECORD_BYTES)
            except ssl.SSLWantReadError:
                break
            except ssl.SSLZeroReturnError:
                closed = True
                break
            if not part:
                closed = True
                break
            if not first:
                first = part
            elif parts is None:
                parts = [first, part]
            else:
                parts.append(part)
        return (b"".join(parts) if parts is not None else first), closed

    def encrypt(self, plaintext: bytes) -> bytes:
        """What goes on the wire for `plaintext`, and whatever TLS itself owes the venue."""

        if self.tls is None or self.outgoing is None:
            return plaintext
        if plaintext:
            self.tls.write(plaintext)
        return self.outgoing.read()

    def send(self, opcode: Opcode, payload: bytes) -> None:
        self.wire += self.encrypt(_client_frame(opcode, payload))


def _ask_for_stamps(sock: sockets.socket) -> bool:
    if SO_TIMESTAMPNS is None or sock.family not in (sockets.AF_INET, sockets.AF_INET6):
        return False
    try:
        sock.setsockopt(sockets.SOL_SOCKET, SO_TIMESTAMPNS, 1)
    except OSError:
        return False
    return True


def tcp_info(sock: sockets.socket) -> dict[str, int]:
    """`TCP_INFO_FIELDS` as the kernel holds them for `sock`; empty where it keeps none."""

    if sys.platform != "linux":
        return {}
    try:
        raw = sock.getsockopt(sockets.IPPROTO_TCP, sockets.TCP_INFO, TCP_INFO_BYTES)
    except OSError:
        return {}
    return {
        name: struct.unpack_from(layout, raw, offset)[0]
        for name, offset, layout in TCP_INFO_FIELDS
        if offset + struct.calcsize(layout) <= len(raw)
    }


def open_link(
    url: str,
    *,
    ident: int = 0,
    shard: Any = None,
    ping: str | None = None,
    timeout: float = CONNECT_TIMEOUT_SECONDS,
) -> Link:
    """Connect and complete TLS and the websocket handshake, blocking the
    caller's thread. No extensions are offered: permessage-deflate is
    pure-Python zlib, measured at 90.8x the frames' cost on the capture host,
    and no message is too large."""

    uri = parse_uri(url)
    deadline = time.monotonic() + timeout

    def remaining() -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"connecting to {uri.host} took over {timeout:g}s")
        return left

    sock = sockets.create_connection((uri.host, uri.port), timeout=timeout)
    try:
        sock.setsockopt(sockets.IPPROTO_TCP, sockets.TCP_NODELAY, 1)
        tls = incoming = outgoing = None
        if uri.secure:
            incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
            tls = ssl.create_default_context().wrap_bio(incoming, outgoing, server_hostname=uri.host)
        link = Link(ident, sock, shard=shard, ping=ping, tls=tls, incoming=incoming, outgoing=outgoing)

        def exchange() -> bytes:
            """Send what TLS owes the venue, then wait for the venue's next bytes."""

            owed = link.encrypt(b"")
            if owed:
                sock.settimeout(remaining())
                sock.sendall(owed)
            sock.settimeout(remaining())
            data = sock.recv(READ_BUFFER_BYTES)
            if not data:
                raise ConnectionError(f"{uri.host} closed the connection while it was being opened")
            # The last of these reads stamps the frames that came with the headers.
            link.last_wall_ns, link.last_data_mono_ns = time.time_ns(), time.monotonic_ns()
            return data

        if tls is not None and incoming is not None:
            while True:
                try:
                    # Verifies the certificate and the host name, as the context says.
                    tls.do_handshake()
                    break
                except ssl.SSLWantReadError:
                    incoming.write(exchange())
        protocol = ClientProtocol(uri, max_size=None)
        request = protocol.connect()
        request.headers["User-Agent"] = USER_AGENT
        protocol.send_request(request)
        sock.settimeout(remaining())
        sock.sendall(link.encrypt(b"".join(protocol.data_to_send())))
        response = b""
        while (end := response.find(b"\r\n\r\n")) < 0:
            if len(response) > HANDSHAKE_MAX_BYTES:
                raise ConnectionError(f"{uri.host} answered the websocket handshake with no end of headers")
            response += link.decrypt(memoryview(exchange()))[0]
        # The protocol reads the headers only: what follows them is frames,
        # and those are this module's to cut.
        protocol.receive_data(response[: end + 4])
        rest = response[end + 4 :]
        if protocol.state is State.CONNECTING and protocol.handshake_exc is None:
            # A refusal can carry a body; it is read so the reason can be told.
            protocol.receive_data(rest)
            while protocol.state is State.CONNECTING and protocol.handshake_exc is None:
                try:
                    more = exchange()
                except ConnectionError:
                    protocol.receive_eof()
                    break
                protocol.receive_data(link.decrypt(memoryview(more))[0])
        if protocol.handshake_exc is not None:
            raise protocol.handshake_exc
        if protocol.state is not State.OPEN:
            raise ConnectionError(f"{uri.host} closed the connection during the websocket handshake")
        link.plain += rest
        link.stamps = _ask_for_stamps(sock)
        sock.setblocking(False)
        return link
    except BaseException:
        sock.close()
        raise


def _descriptor(fileobj: Any) -> int:
    return fileobj if isinstance(fileobj, int) else int(fileobj.fileno())


class Poller:
    """The readiness calls the reader's loop makes, on epoll where the kernel
    has it and `selectors` elsewhere (macOS, where the tests also run).
    `selectors` looks up a key and builds a tuple for every event, and the
    loop passes once for nearly every packet the venues send. `select` hands
    back what each ready descriptor was registered with; like `selectors`,
    a descriptor registers once, and one never registered is a `KeyError`."""

    def __init__(self) -> None:
        self.epoll: Any = None
        self.fallback: selectors.BaseSelector | None = None
        if sys.platform == "linux":
            self.epoll = select.epoll()
        else:
            self.fallback = selectors.DefaultSelector()
        self.data: dict[int, Any] = {}

    @staticmethod
    def _mask(events: int) -> int:
        if sys.platform != "linux":
            return events
        return (select.EPOLLIN if events & selectors.EVENT_READ else 0) | (
            select.EPOLLOUT if events & selectors.EVENT_WRITE else 0
        )

    def register(self, fileobj: Any, events: int, data: Any) -> None:
        descriptor = _descriptor(fileobj)
        if descriptor in self.data:
            raise KeyError(f"{fileobj!r} is already registered")
        if self.epoll is not None:
            self.epoll.register(descriptor, self._mask(events))
        else:
            assert self.fallback is not None
            self.fallback.register(fileobj, events, data)
        self.data[descriptor] = data

    def modify(self, fileobj: Any, events: int, data: Any) -> None:
        descriptor = _descriptor(fileobj)
        if descriptor not in self.data:
            raise KeyError(f"{fileobj!r} is not registered")
        if self.epoll is not None:
            self.epoll.modify(descriptor, self._mask(events))
        else:
            assert self.fallback is not None
            self.fallback.modify(fileobj, events, data)
        self.data[descriptor] = data

    def unregister(self, fileobj: Any) -> None:
        descriptor = _descriptor(fileobj)
        del self.data[descriptor]
        if self.epoll is not None:
            try:
                self.epoll.unregister(descriptor)
            except OSError:
                # Closed already, which leaves epoll's set.
                pass
        else:
            assert self.fallback is not None
            self.fallback.unregister(fileobj)

    def select(self, timeout: float) -> list[Any]:
        if self.epoll is None:
            assert self.fallback is not None
            return [key.data for key, _events in self.fallback.select(timeout)]
        data = self.data
        return [data[descriptor] for descriptor, _events in self.epoll.poll(timeout, len(data) or 1) if descriptor in data]

    def close(self) -> None:
        if self.epoll is not None:
            self.epoll.close()
        elif self.fallback is not None:
            self.fallback.close()
        self.data.clear()


class Reader:
    """The reader's loop: commands in, sockets read, events out."""

    def __init__(self, commands: int, events: int) -> None:
        self.commands = commands
        self.events = events
        os.set_blocking(commands, False)
        os.set_blocking(events, False)
        self.selector = Poller()
        self.selector.register(commands, selectors.EVENT_READ, "commands")
        self.wake_read, self.wake_write = sockets.socketpair()
        self.wake_read.setblocking(False)
        self.wake_write.setblocking(False)
        self.selector.register(self.wake_read, selectors.EVENT_READ, "wake")
        self.events_registered = False
        #: The last write to the events pipe found it full.
        self.events_blocked = False
        #: When the events pipe last took all of `out`, on the monotonic clock.
        self.flushed_ns = 0
        self.lock = threading.Lock()
        #: Under `lock`: opens that finished, `(ident, link or None, why not)`.
        self.arriving: list[tuple[int, Link | None, str]] = []
        #: The reader's alone.
        self.links: dict[int, Link] = {}
        self.opening: set[int] = set()
        self.cancelled: set[int] = set()
        self.command_bytes = bytearray()
        self.out = bytearray()
        self.buffer = bytearray(READ_BUFFER_BYTES)
        self.view = memoryview(self.buffer)
        self.stopping = False
        self.orphaned = False
        self.next_keepalive_ns = 0
        #: A link has closed since `_retire` last found every closed link retired.
        self.shut = False
        #: Some link holds frames not yet in `out`: set by whatever appends to
        #: a link's `pending`, recomputed by `_deliver`. A pass reads it where
        #: it would otherwise walk every link.
        self.holding = False

    # ---------------------------------------------------------------- loop

    def run(self) -> None:
        while not self.orphaned and not (self.stopping and self._finished()):
            try:
                self._pass()
            except Exception:  # every shard reads through this loop; it outlives any one fault
                logging.exception("tape reader pass failed")
                time.sleep(QUEUE_RETRY_SECONDS)
        for link in list(self.links.values()):
            self._shut(link)
        self.selector.close()
        self.wake_read.close()
        self.wake_write.close()

    def _finished(self) -> bool:
        return not self.links and not self.opening and not self.out

    def _pass(self) -> None:
        self._admit()
        timeout = QUEUE_RETRY_SECONDS if self.holding or self.out else KEEPALIVE_CHECK_SECONDS
        if self.out and not self.events_blocked:
            # Events held back for the next write wait no longer than it is due.
            due_ns = self.flushed_ns + _EVENTS_FLUSH_NS - time.monotonic_ns()
            timeout = min(timeout, max(0.0, due_ns / 1e9))
        for data in self.selector.select(timeout):
            # A link first: nearly every event is one.
            if isinstance(data, Link):
                if data.reading:
                    self._read(data)
            elif data == "commands":
                self._commands()
            elif data == "wake":
                self._drain_wake()
        for link in self.links.values():
            if link.wire and link.open:
                self._flush(link)
        now_ns = time.monotonic_ns()
        self._deliver(now_ns)
        self._flush_events(now_ns)
        if now_ns >= self.next_keepalive_ns:
            self.next_keepalive_ns = now_ns + int(KEEPALIVE_CHECK_SECONDS * 1e9)
            self._keepalive(now_ns)
        self._retire()
        self._register()

    def wake(self) -> None:
        try:
            self.wake_write.send(b"\0")
        except OSError:
            # Full: a wake is already waiting to be read.
            pass

    def _drain_wake(self) -> None:
        while True:
            try:
                if not self.wake_read.recv(4096):
                    return
            except OSError:
                return

    # ------------------------------------------------------------ commands

    def _commands(self) -> None:
        while True:
            try:
                chunk = os.read(self.commands, 65536)
            except (BlockingIOError, InterruptedError):
                break
            if not chunk:
                logging.warning("the recorder is gone; the tape reader exits")
                self.orphaned = True
                return
            self.command_bytes += chunk
        while (end := self.command_bytes.find(b"\n")) >= 0:
            line = bytes(self.command_bytes[:end])
            del self.command_bytes[: end + 1]
            try:
                self.command(json.loads(line))
            except Exception:  # one bad command costs that command
                logging.exception("tape reader could not act on a command")

    def command(self, message: dict[str, Any]) -> None:
        op = message.get("op")
        if op == "stop":
            self.stopping = True
            for link in list(self.links.values()):
                self._close(link)
            self.cancelled |= self.opening
            return
        ident = int(message["link"])
        if op == "open":
            if self.stopping:
                self._event(FAILED, ident, body=b"the reader is stopping")
                return
            self.opening.add(ident)
            threading.Thread(
                target=self._open,
                args=(ident, str(message["url"]), message.get("shard"), message.get("ping")),
                name=f"tape-reader-open-{ident}",
                daemon=True,
            ).start()
            return
        target = self.links.get(ident)
        if op == "close":
            if target is not None:
                self._close(target)
            elif ident in self.opening:
                self.cancelled.add(ident)
            return
        if op == "send":
            if target is not None and target.open and not target.closing:
                target.send(Opcode.TEXT, str(message["text"]).encode())
            return
        raise ValueError(f"unknown command {op!r}")

    def _open(self, ident: int, url: str, shard: Any, ping: str | None) -> None:
        try:
            link: Link | None = open_link(url, ident=ident, shard=shard, ping=ping)
            why = ""
        except Exception as exc:  # a refused or timed-out connect is the recorder's reconnect
            link, why = None, str(exc) or type(exc).__name__
        with self.lock:
            self.arriving.append((ident, link, why))
        self.wake()

    def _admit(self) -> None:
        # Unlocked: an open that lands after this look wakes the next pass.
        if not self.arriving:
            return
        with self.lock:
            arriving, self.arriving = self.arriving, []
        for ident, link, why in arriving:
            self.opening.discard(ident)
            if link is None:
                self._event(FAILED, ident, body=why.encode())
                continue
            if ident in self.cancelled:
                self.cancelled.discard(ident)
                link.sock.close()
                self._event(FAILED, ident, body=b"closed before it opened")
                continue
            self.links[ident] = link
            self._event(OPENED, ident)
            # Frames the handshake's last read carried past its headers, and
            # whatever else TLS or the kernel already holds.
            if link.plain:
                plain, link.plain = bytes(link.plain), bytearray()
                try:
                    self._frames(link, plain, link.last_wall_ns, link.last_data_mono_ns)
                except Exception as exc:  # one connection's fault ends that connection
                    self._say(link, f"websocket stream failed: {exc}")
                    self._shut(link)
                    continue
            self._read(link)

    # ------------------------------------------------------------- reading

    def _read(self, link: Link) -> None:
        """Read one socket until the kernel has nothing more for it or it holds
        `SOCKET_QUEUE_FRAMES`.

        Each read's frames carry the clocks read as that read returned, less
        how long the kernel held the read's newest packet."""

        view = self.view
        size = len(view)
        while link.reading and len(link.pending) < SOCKET_QUEUE_FRAMES:
            ancillary: list[tuple[int, int, bytes]] = []
            try:
                if link.stamps:
                    count, ancillary, _flags, _address = link.sock.recvmsg_into([view], ANCILLARY_BYTES)
                else:
                    count = link.sock.recv_into(view)
            except (BlockingIOError, InterruptedError):
                break
            except OSError as exc:
                self._ended(link, f"socket read failed: {exc}")
                count = -1
            now_ns = time.time_ns()
            mono_ns = time.monotonic_ns()
            for level, kind, stamp in ancillary:
                if level == sockets.SOL_SOCKET and kind == SO_TIMESTAMPNS and len(stamp) >= TIMESPEC.size:
                    seconds, nanoseconds = TIMESPEC.unpack_from(stamp)
                    # Against the wall reading the frames are stamped from: a
                    # later clock read would stamp them early by however long
                    # this process ran or waited since.
                    dwell = now_ns - (seconds * 1_000_000_000 + nanoseconds)
                    if 0 < dwell < MAX_DWELL_NS:
                        now_ns -= dwell
                        mono_ns -= dwell
                        if dwell > link.dwell_max_ns:
                            link.dwell_max_ns = dwell
            if now_ns < link.last_wall_ns:
                # TCP hands a recovered segment over behind the one that
                # overtook it, so its kernel stamp is older than the read
                # before's: nothing could have read it sooner. Stamped at that
                # read's instant, both clocks by the same shift.
                shift = link.last_wall_ns - now_ns
                now_ns += shift
                mono_ns += shift
            link.last_wall_ns = now_ns
            if count <= 0:
                if not count:
                    self._ended(link, "the venue closed the connection (end of stream)")
                self._shut(link)
                break
            link.last_data_mono_ns = mono_ns
            held = len(link.pending)
            try:
                data, closed = link.decrypt(view[:count])
                if link.outgoing is not None and link.outgoing.pending:
                    # What TLS owes the venue for what it read (a key update's answer).
                    link.wire += link.outgoing.read()
                if data:
                    self._frames(link, data, now_ns, mono_ns)
            except Exception as exc:  # one connection's fault ends that connection
                self._say(link, f"websocket stream failed: {exc}")
                self._shut(link)
                break
            completed = len(link.pending) - held
            if completed:
                link.reads += 1
                link.frames += completed
            if closed:
                if link.reading:
                    self._ended(link, "the venue closed TLS")
                self._shut(link)
                break
            if count < size:
                # Short: the kernel had no more, and asking again to be told
                # so is a syscall and an exception per socket per pass.
                break

    def _frames(self, link: Link, data: bytes, now_ns: int, mono_ns: int) -> None:
        """Cut `data`, after whatever the link held, into messages; the last
        partial frame waits in `link.plain`."""

        if link.plain:
            link.plain += data
            if len(link.plain) < link.need:
                return
            data = bytes(link.plain)
            link.plain.clear()
        pending = link.pending
        append = pending.append
        size = len(data)
        at = 0
        need = 0
        while size - at >= 2:
            first = data[at]
            length = data[at + 1]
            if length < 126:
                head = at + 2
            elif length == 126:
                if size - at < 4:
                    break
                length = _U16(data, at + 2)[0]
                head = at + 4
            elif length == 127:
                if size - at < 10:
                    break
                length = _U64(data, at + 2)[0]
                head = at + 10
            else:
                raise ProtocolError("the venue masked a frame")
            end = head + length
            if end > size:
                need = end - at
                break
            if first == 0x81 and link.fragments is None:
                # A whole text message, as nearly every frame is.
                append((data[head:end], now_ns, mono_ns))
                at = end
                continue
            if first & 0x70:
                raise ProtocolError("the venue set a reserved bit; no extension was agreed")
            opcode = first & 0x0F
            if opcode == 1 or opcode == 2:
                if link.fragments is not None:
                    raise ProtocolError("a message began inside another")
                if first & 0x80:
                    pending.append((data[head:end], now_ns, mono_ns))
                else:
                    link.fragments = [data[head:end]]
            elif opcode == 0:
                if link.fragments is None:
                    raise ProtocolError("a continuation with no message")
                link.fragments.append(data[head:end])
                if first & 0x80:
                    pending.append((b"".join(link.fragments), now_ns, mono_ns))
                    link.fragments = None
            elif opcode == 9:
                if not link.closing:
                    link.send(Opcode.PONG, data[head:end])
            elif opcode == 10:
                if length == _PING.size and link.ping_sent_ns and _PING.unpack_from(data, head)[0] == link.ping_sent_ns:
                    link.ping_rtt_ns = mono_ns - link.ping_sent_ns
                    link.ping_sent_ns = 0
            elif opcode == 8:
                # The venue's close: answered with its own code, and the socket
                # ends once the answer is written.
                if not link.closing:
                    code = _U16(data, head)[0] if length >= 2 else None
                    reason = bytes(data[head + 2 : end]).decode(errors="replace") if length > 2 else ""
                    self._ended(link, f"the venue closed the websocket: code {code}, reason {reason!r}")
                    link.send(Opcode.CLOSE, data[head : head + 2] if length >= 2 else b"")
                    link.closing = True
                    link.closing_since_ns = mono_ns
                link.reading = False
                at = end
                break
            else:
                raise ProtocolError(f"the venue sent opcode {opcode}")
            at = end
        if at < size:
            link.plain += data[at:] if at else data
        link.need = need
        if pending:
            self.holding = True

    # ------------------------------------------------------------- writing

    def _flush(self, link: Link) -> None:
        while link.wire:
            try:
                wrote = link.sock.send(link.wire)
            except (BlockingIOError, InterruptedError):
                break
            except OSError as exc:
                self._say(link, f"socket write failed: {exc}")
                self._shut(link)
                return
            if not wrote:
                break
            del link.wire[:wrote]
        if link.closing and not link.wire:
            self._shut(link)

    def _close(self, link: Link) -> None:
        """Send the venue a close and drop the socket once it is written. A
        link asked to close reads no more: what it would bring is for a
        connection that is over."""

        if link.closing or not link.open:
            link.reading = False
            return
        try:
            link.send(Opcode.CLOSE, _CLOSE_NORMAL)
        except ssl.SSLError as exc:
            self._say(link, f"TLS write failed: {exc}")
            self._shut(link)
            return
        link.closing = True
        link.closing_since_ns = time.monotonic_ns()
        link.reading = False

    def _event(self, kind: int, ident: int, a: int = 0, b: int = 0, body: bytes = b"") -> None:
        self.out += EVENT.pack(len(body), kind, ident, a, b)
        if body:
            self.out += body

    def _deliver(self, mono_ns: int) -> None:
        """Encode held frames for the recorder in the order the host received
        them, whichever link carried each, while the events buffer has room;
        overrun a link whose oldest held frame has waited out the recorder's
        patience."""

        ready = [link for link in self.links.values() if link.pending]
        if not ready:
            self.holding = False
            return
        out = self.out
        pack = EVENT.pack
        limit = EVENTS_BUFFER_BYTES
        if len(ready) == 1:
            # One link's frames are in arrival order already; most passes read one socket.
            link = ready[0]
            pending, ident = link.pending, link.ident
            at, end = 0, len(pending)
            while at < end and len(out) < limit:
                payload, wall_ns, stamp_ns = pending[at]
                out += pack(len(payload), FRAME, ident, wall_ns, stamp_ns)
                out += payload
                at += 1
            taken = [at]
            heads = []
        else:
            # Each link's frames are already in arrival order; merged on the
            # monotonic stamp they reach the recorder, and the tape, as the host
            # received them. Handed over a link at a time they would trail each
            # other by up to a pass: tens of milliseconds on a busy feed, every
            # one a row behind the row before it. The merge takes the
            # earliest of the links' next frames by `(mono, wall, link)`, and with
            # it the frames behind it on its link that share its stamps.
            taken = [0] * len(ready)
            heads = [(link.pending[0][2], link.pending[0][1], link.ident, index) for index, link in enumerate(ready)]
            heapq.heapify(heads)
        while heads and len(out) < limit:
            frame_mono_ns, now_ns, ident, index = heads[0]
            pending = ready[index].pending
            at = taken[index]
            end = len(pending)
            while True:
                payload = pending[at][0]
                out += pack(len(payload), FRAME, ident, now_ns, frame_mono_ns)
                out += payload
                at += 1
                if at == end or len(out) >= limit:
                    break
                _payload, wall_ns, stamp_ns = pending[at]
                if stamp_ns != frame_mono_ns or wall_ns != now_ns:
                    break
            taken[index] = at
            if at == end:
                heapq.heappop(heads)
            else:
                _payload, wall_ns, stamp_ns = pending[at]
                heapq.heapreplace(heads, (stamp_ns, wall_ns, ident, index))
        for link, count in zip(ready, taken):
            if count == len(link.pending):
                link.pending = []
            elif count:
                del link.pending[:count]
        patience = mono_ns - int(QUEUE_PUT_TIMEOUT_SECONDS * 1e9)
        holding = False
        for link in ready:
            if link.pending and link.pending[0][2] <= patience:
                self._overrun(link)
            link.reading = link.open and not link.closing and len(link.pending) < SOCKET_QUEUE_FRAMES
            if link.pending:
                holding = True
        self.holding = holding

    def _overrun(self, link: Link) -> None:
        dropped = len(link.pending)
        newest_ns = link.pending[-1][1]
        link.pending = []
        link.fragments = None
        logging.error("shard %s overran the capture queue; reconnecting for fresh snapshots", link.shard)
        self._event(DROPPED, link.ident, dropped, newest_ns)
        self._close(link)

    def _flush_events(self, now_ns: int = 0) -> None:
        """Write `out` to the events pipe when a write is due
        (`EVENTS_FLUSH_SECONDS`, `EVENTS_FLUSH_BYTES`); at once when `now_ns`
        is 0, the reader is stopping, or the last write found the pipe full."""

        out = self.out
        if not out:
            return
        if (
            now_ns
            and now_ns - self.flushed_ns < _EVENTS_FLUSH_NS
            and len(out) < EVENTS_FLUSH_BYTES
            and not self.stopping
            and not self.events_blocked
        ):
            return
        while out:
            try:
                wrote = os.write(self.events, out)
            except (BlockingIOError, InterruptedError):
                self.events_blocked = True
                return
            except BrokenPipeError:
                logging.warning("the recorder stopped reading; the tape reader exits")
                self.orphaned = True
                return
            del out[:wrote]
        self.events_blocked = False
        self.flushed_ns = now_ns or time.monotonic_ns()

    # --------------------------------------------------------- housekeeping

    def _keepalive(self, now_ns: int) -> None:
        silence = int(SILENCE_SECONDS * 1e9)
        for link in self.links.values():
            if not link.open:
                continue
            if link.closing:
                if now_ns - link.closing_since_ns > CLOSE_TIMEOUT_SECONDS * 1e9:
                    self._shut(link)
                continue
            if now_ns - link.last_data_mono_ns > silence:
                logging.warning(
                    "shard %s received no data or pong in %.0fs; reconnecting", link.shard, SILENCE_SECONDS
                )
                self._close(link)
            elif now_ns >= link.next_ping_ns:
                # The venue's own heartbeat where it has one: Bybit drops a
                # socket that sends it nothing, and does not reliably answer
                # the protocol's ping under load.
                if link.ping:
                    link.send(Opcode.TEXT, link.ping.encode())
                else:
                    # Written now, so its answer times the round trip.
                    link.ping_sent_ns = time.monotonic_ns()
                    link.send(Opcode.PING, _PING.pack(link.ping_sent_ns))
                    self._flush(link)
                link.next_ping_ns = now_ns + int(PING_INTERVAL_SECONDS * 1e9)
            if link.open and not link.closing and now_ns >= link.next_stats_ns:
                link.next_stats_ns = now_ns + int(STATS_SECONDS * 1e9)
                self._report(link)

    def _report(self, link: Link) -> None:
        body = {
            "tcp": tcp_info(link.sock),
            "ping_rtt_ms": round(link.ping_rtt_ns / 1e6, 3) if link.ping_rtt_ns else None,
            "reads": link.reads,
            "frames": link.frames,
            "dwell_max_ms": round(link.dwell_max_ns / 1e6, 3),
        }
        link.reads = link.frames = link.dwell_max_ns = 0
        self._event(STATS, link.ident, body=json.dumps(body, separators=(",", ":")).encode())

    def _retire(self) -> None:
        if not self.shut:
            return
        for ident, link in list(self.links.items()):
            if not link.open and not link.pending:
                del self.links[ident]
                self._event(ENDED, ident)
        self.shut = any(not link.open for link in self.links.values())

    def _register(self) -> None:
        for link in self.links.values():
            if not link.open:
                continue
            wanted = (selectors.EVENT_READ if link.reading else 0) | (selectors.EVENT_WRITE if link.wire else 0)
            if wanted == link.registered:
                continue
            if not wanted:
                self.selector.unregister(link.sock)
            elif not link.registered:
                self.selector.register(link.sock, wanted, link)
            else:
                self.selector.modify(link.sock, wanted, link)
            link.registered = wanted
        blocked = self.events_blocked and bool(self.out)
        if blocked != self.events_registered:
            if blocked:
                self.selector.register(self.events, selectors.EVENT_WRITE, "events")
            else:
                self.selector.unregister(self.events)
            self.events_registered = blocked

    def _shut(self, link: Link) -> None:
        if not link.open:
            return
        link.open = False
        link.reading = False
        self.shut = True
        if link.registered:
            try:
                self.selector.unregister(link.sock)
            except (KeyError, ValueError):
                pass
            link.registered = 0
        try:
            link.sock.close()
        except OSError:
            pass

    @staticmethod
    def _say(link: Link, what: str) -> None:
        logging.warning("shard %s %s", link.shard, what)

    def _ended(self, link: Link, how: str) -> None:
        """How the venue's side ended a link, and what the reader held as it
        did: frames the recorder had not yet taken (`SOCKET_QUEUE_FRAMES` is
        the most, past which the link was no longer read), and the longest
        the kernel held a read's newest packet since the link's last `STATS`.
        A close the reader was asked for says nothing here."""

        self._say(
            link,
            f"{how}; frames held for the recorder: {len(link.pending)}, "
            f"the kernel's longest hold of a read since the last stats: {link.dwell_max_ns / 1e6:.0f} ms",
        )


def size_pipe(descriptor: int) -> None:
    """Grow a pipe to `PIPE_BYTES` where the kernel allows it."""

    setting = getattr(fcntl, "F_SETPIPE_SZ", None)
    if setting is None:
        return
    try:
        fcntl.fcntl(descriptor, setting, PIPE_BYTES)
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m market_tape.reader", description="The capture reader the recorder starts beside itself."
    )
    parser.add_argument("--commands", type=int, required=True, help="descriptor the recorder writes commands to")
    parser.add_argument("--events", type=int, required=True, help="descriptor this reader writes events to")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    Reader(args.commands, args.events).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
