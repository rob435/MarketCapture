"""The capture reader (`market_tape.reader`) through its pipes.

The ways it can fail, and where each is held:

- a message lost, duplicated, reordered or cut wrong between the wire and the
  events pipe: however the venue frames it (small, 16-bit and 64-bit lengths,
  fragments with a ping between them, one message larger than a read), and
  whatever arrived with the handshake's response;
- a frame stamped other than when its read's newest segment arrived, however
  long the reader waited between its clock readings, or behind the frame
  before it on its link, the handshake's own read included;
- a frame the venue broke that does not end its link;
- a TLS socket read one record at a time, which fell seconds behind the feed;
- a refusal reported as something else, a venue ping left unanswered, a
  silent socket kept, a keepalive off its cadence;
- a link the venue ended (its websocket close, the end of the stream) that
  says neither how nor what the reader held for the recorder as it did;
- a connection's account of itself (`STATS`) read from the wrong bytes of
  the kernel's `tcp_info`, or a ping's answer left untimed;
- a link whose frames the recorder does not take held without bound, or
  dropped without being counted;
- a stop that exits before handing back what was read, a reader that
  outlives the recorder.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import select
import shutil
import socket as sockets
import ssl
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest
from websockets.frames import Frame, Opcode
from websockets.protocol import State
from websockets.server import ServerProtocol
from websockets.sync.server import serve

from market_tape import reader, record

Event = tuple[int, int, int, int, bytes]


class Driver:
    """The recorder's end of a reader's two pipes."""

    def __init__(self, spawn: Callable[[int, int], Any]) -> None:
        commands_read, self.commands = os.pipe()
        self.events, events_write = os.pipe()
        self.process = spawn(commands_read, events_write)
        self.held = b""
        self.log: list[Event] = []
        self.eof = False

    def command(self, **message: Any) -> None:
        os.write(self.commands, (json.dumps(message) + "\n").encode())

    def read(self, until: Callable[[list[Event]], bool], timeout: float = 10.0) -> list[Event]:
        deadline = time.monotonic() + timeout
        while not until(self.log):
            left = deadline - time.monotonic()
            assert left > 0 and not self.eof, f"gave up waiting; events so far: {[e[:4] for e in self.log][-10:]}"
            if not select.select([self.events], [], [], min(left, 0.5))[0]:
                continue
            chunk = os.read(self.events, 1 << 20)
            if not chunk:
                self.eof = True
                continue
            data = self.held + chunk
            at = 0
            while len(data) - at >= reader.EVENT.size:
                length, kind, ident, a, b = reader.EVENT.unpack_from(data, at)
                end = at + reader.EVENT.size + length
                if end > len(data):
                    break
                self.log.append((kind, ident, a, b, data[at + reader.EVENT.size : end]))
                at = end
            self.held = data[at:]
        return self.log

    def of(self, kind: int, ident: int = 1) -> list[Event]:
        return [event for event in self.log if event[0] == kind and event[1] == ident]

    def payloads(self, ident: int = 1) -> list[bytes]:
        return [event[4] for event in self.of(reader.FRAME, ident)]

    def stop(self) -> None:
        self.command(op="stop")
        self.read(lambda _log: self.eof, timeout=20.0)
        self.process.wait(timeout=10.0)
        os.close(self.commands)
        os.close(self.events)


def _served(handler: Callable[[Any], None], **options: Any) -> Any:
    # IPv4, as the reader dials (`reader.connect_ipv4`); the name stays
    # localhost for the certificate.
    server = serve(handler, "127.0.0.1", 0, **options)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_every_message_crosses_whole_and_in_order_however_the_venue_frames_it(reader_in_thread: Any) -> None:
    small = [b'{"n":%d}' % n for n in range(400)]
    medium = b"m" * 300  # a 16-bit length
    large = b"l" * 70_000  # a 64-bit length
    huge = b"h" * (3 * 1024 * 1024)  # more than one read, and more than the intake's read
    seen: dict[str, Any] = {}

    def venue(connection: Any) -> None:
        seen["extensions"] = connection.request.headers.get("Sec-WebSocket-Extensions")
        seen["subscribe"] = connection.recv()
        for payload in small[:200]:
            connection.send(payload)
        connection.send(medium)
        connection.send(large)
        # A fragmented message, and the venue's ping while it is still open.
        connection.send([b"frag", b"men", b"ted"])
        pong = connection.ping(b"are you there")
        for payload in small[200:]:
            connection.send(payload)
        connection.send(huge)
        seen["pong"] = pong.wait(5.0)
        try:
            connection.recv()
        except Exception:  # the reader closed it
            return

    driver = Driver(reader_in_thread)
    with _served(venue) as server:
        driver.command(op="open", link=1, url=f"ws://localhost:{server.socket.getsockname()[1]}/v5", shard=3, ping=None)
        driver.read(lambda log: bool(driver.of(reader.OPENED)))
        driver.command(op="send", link=1, text='{"op":"subscribe"}')
        expected = small[:200] + [medium, large, b"fragmented"] + small[200:] + [huge]
        driver.read(lambda log: len(driver.of(reader.FRAME)) == len(expected))
        driver.command(op="close", link=1)
        driver.read(lambda log: bool(driver.of(reader.ENDED)))
        driver.stop()

    assert driver.payloads() == expected
    assert seen["extensions"] is None, "the reader offered permessage-deflate"
    assert seen["subscribe"] == '{"op":"subscribe"}'
    assert seen["pong"], "the venue's ping went unanswered"
    stamps = [(event[2], event[3]) for event in driver.of(reader.FRAME)]
    assert all(wall > 0 and mono > 0 for wall, mono in stamps)
    assert [kind for kind, *_ in driver.log if kind != reader.FRAME] == [reader.OPENED, reader.ENDED]


def test_a_tls_socket_is_read_to_empty_in_one_read_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A TLS read returns one record, and Bybit sends each message as its own:
    a reader that stopped at a short read took one message a socket a pass,
    and on the capture host fell seconds behind the feed within minutes."""

    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is not installed")
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(cert),
         "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
        check=True,
        capture_output=True,
    )
    served = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    served.load_cert_chain(cert, key)
    trusted = ssl.create_default_context(cafile=str(cert))
    monkeypatch.setattr(reader.ssl, "create_default_context", lambda: trusted)
    opened, sent = threading.Event(), threading.Event()

    def venue(connection: Any) -> None:
        # As a venue, nothing until the link is open: frames that ride in with
        # the handshake's answer wait in `link.plain` for the next socket read,
        # which this test never makes.
        opened.wait(5.0)
        for n in range(200):
            connection.send(b'{"n":%d}' % n)
        sent.set()
        try:
            connection.recv()
        except Exception:  # the reader closed it
            return

    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    with _served(venue, ssl=served) as server:
        link = reader.open_link(f"wss://localhost:{server.socket.getsockname()[1]}", ident=1)
        opened.set()
        try:
            assert sent.wait(5.0)
            deadline = time.monotonic() + 5.0
            while not select.select([link.sock], [], [], 0)[0] and time.monotonic() < deadline:
                time.sleep(0.01)
            time.sleep(0.2)
            loop.links[1] = link
            loop._read(link)
            assert [payload for payload, _wall, _mono in link.pending] == [b'{"n":%d}' % n for n in range(200)]
        finally:
            link.sock.close()
            for descriptor in (commands, commands_write, events_read, events):
                os.close(descriptor)


def _raw_venue(
    answer: Callable[[ServerProtocol, Any], bytes], later: bytes = b"", hang_up: bool = False
) -> tuple[sockets.socket, threading.Thread]:
    """A venue on a bare socket: it reads the handshake and writes `answer`'s
    bytes in one send, and `later` 50 ms after; with `hang_up`, it then closes
    the connection at once."""

    listener = sockets.create_server(("127.0.0.1", 0))

    def serve_one() -> None:
        connection, _ = listener.accept()
        with connection:
            protocol = ServerProtocol()
            while True:
                protocol.receive_data(connection.recv(65536))
                requests = protocol.events_received()
                if requests:
                    break
            connection.sendall(answer(protocol, requests[0]))
            if later:
                time.sleep(0.05)
                connection.sendall(later)
            if hang_up:
                return
            connection.settimeout(5.0)
            try:
                while connection.recv(65536):
                    pass
            except OSError:
                pass

    thread = threading.Thread(target=serve_one, daemon=True)
    thread.start()
    return listener, thread


def test_frames_the_handshakes_own_read_carried_are_not_lost(reader_in_thread: Any) -> None:
    def answer(protocol: ServerProtocol, request: Any) -> bytes:
        protocol.send_response(protocol.accept(request))
        response = b"".join(protocol.data_to_send())
        return response + Frame(Opcode.TEXT, b'{"first":1}').serialize(mask=False) + b"\x81\x05{\"se"

    listener, thread = _raw_venue(answer)
    driver = Driver(reader_in_thread)
    try:
        driver.command(op="open", link=1, url=f"ws://127.0.0.1:{listener.getsockname()[1]}", shard=0, ping=None)
        driver.read(lambda log: bool(driver.of(reader.FRAME)))
        assert driver.payloads() == [b'{"first":1}']
        assert driver.of(reader.OPENED) and driver.log.index(driver.of(reader.OPENED)[0]) == 0
    finally:
        driver.stop()
        listener.close()


@contextlib.contextmanager
def _receive_stamping() -> Iterator[None]:
    """The kernel stamping every segment as it arrives: it starts host-wide from
    a work item after the first socket asks, and until then stamps a segment
    when it is read."""

    listener = sockets.create_server(("127.0.0.1", 0))
    client = sockets.create_connection(listener.getsockname())
    venue, _ = listener.accept()
    try:
        assert reader._ask_for_stamps(client)
        deadline = time.monotonic() + 10.0
        while True:
            venue.sendall(b"x")
            sent_ns = time.time_ns()
            time.sleep(0.05)
            _data, ancillary, _flags, _address = client.recvmsg(64, reader.ANCILLARY_BYTES)
            stamps = [reader.TIMESPEC.unpack_from(stamp) for _level, _kind, stamp in ancillary]
            if stamps and stamps[0][0] * 10**9 + stamps[0][1] - sent_ns < 25_000_000:
                break
            assert time.monotonic() < deadline, "the kernel never stamped a segment when it arrived"
        yield
    finally:
        for sock in (listener, client, venue):
            sock.close()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the kernel's receive stamps are Linux's")
def test_frames_the_handshakes_read_carried_are_stamped_with_it_and_never_after_the_next_read(
    reader_in_thread: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A frame riding in with the handshake's answer was read with it and
    carries that read's stamps; the frame the kernel held behind it, stamped
    when it arrived, does not step back behind it. Here the loop takes the link
    300 ms after its handshake, and the venue's next frame lands 50 ms after
    its answer."""

    def answer(protocol: ServerProtocol, request: Any) -> bytes:
        protocol.send_response(protocol.accept(request))
        return b"".join(protocol.data_to_send()) + Frame(Opcode.TEXT, b"with the answer").serialize(mask=False)

    opened = reader.open_link

    def taken_late(*args: Any, **options: Any) -> reader.Link:
        link = opened(*args, **options)
        time.sleep(0.3)
        return link

    monkeypatch.setattr(reader, "open_link", taken_late)
    listener, _thread = _raw_venue(answer, later=Frame(Opcode.TEXT, b"behind it").serialize(mask=False))
    with _receive_stamping():
        driver = Driver(reader_in_thread)
        try:
            asked_ns = time.time_ns()
            driver.command(op="open", link=1, url=f"ws://127.0.0.1:{listener.getsockname()[1]}", shard=0, ping=None)
            driver.read(lambda log: len(driver.of(reader.FRAME)) == 2)
        finally:
            driver.stop()
            listener.close()
    (_, _, first_ns, _, first), (_, _, second_ns, _, second) = driver.of(reader.FRAME)
    assert (first, second) == (b"with the answer", b"behind it")
    assert first_ns <= second_ns, f"the link's stamps stepped back {(first_ns - second_ns) / 1e6:.1f} ms"
    assert first_ns - asked_ns < 250_000_000, "stamped when the loop took the link, not when it was read"


def test_a_frame_broken_in_the_handshakes_own_read_ends_its_link(
    reader_in_thread: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """A frame the venue masked, riding in with its handshake's answer, ends
    the link at once, as a broken frame read later does. Raised out of the
    pass instead, it left the link open on a stream cut mid-frame, and lost
    every other open that pass was taking: never answered, and still opening
    when a stop waited for them."""

    def answer(protocol: ServerProtocol, request: Any) -> bytes:
        protocol.send_response(protocol.accept(request))
        return b"".join(protocol.data_to_send()) + Frame(Opcode.TEXT, b"masked").serialize(mask=True)

    listener, _thread = _raw_venue(answer)
    driver = Driver(reader_in_thread)
    try:
        driver.command(op="open", link=1, url=f"ws://127.0.0.1:{listener.getsockname()[1]}", shard=0, ping=None)
        # The venue itself hangs up 5 s on.
        driver.read(lambda log: bool(driver.of(reader.ENDED)), timeout=3.0)
        assert [kind for kind, *_ in driver.log] == [reader.OPENED, reader.ENDED]
        assert "tape reader pass failed" not in caplog.text
    finally:
        driver.stop()
        listener.close()


@pytest.mark.parametrize("ending", ["websocket close", "end of stream"])
def test_a_link_the_venue_ends_says_how_and_what_the_reader_held(
    ending: str, reader_in_thread: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """A venue that drops a connection is answered by a reconnect, and the
    recorder's line says only that: which end hung up and why, and whether
    the reader was holding frames the recorder had not taken (a slow
    consumer is ours, not the venue's), must come from here."""

    def answer(protocol: ServerProtocol, request: Any) -> bytes:
        protocol.send_response(protocol.accept(request))
        sent = b"".join(protocol.data_to_send()) + Frame(Opcode.TEXT, b'{"n":1}').serialize(mask=False)
        if ending == "websocket close":
            sent += Frame(Opcode.CLOSE, struct.pack("!H", 1008) + b"slow consumer").serialize(mask=False)
        return sent

    listener, _thread = _raw_venue(answer, hang_up=ending == "end of stream")
    driver = Driver(reader_in_thread)
    try:
        with caplog.at_level(logging.WARNING):
            driver.command(op="open", link=1, url=f"ws://127.0.0.1:{listener.getsockname()[1]}", shard=7, ping=None)
            driver.read(lambda log: bool(driver.of(reader.ENDED)))
        # The venue's frame rode in the read that carried the close: still in the reader's hand.
        said = [record.getMessage() for record in caplog.records if record.getMessage().startswith("shard 7 ")]
        if ending == "websocket close":
            assert said == [
                "shard 7 the venue closed the websocket: code 1008, reason 'slow consumer'; "
                "frames held for the recorder: 1, the kernel's longest hold of a read since the last stats: 0 ms"
            ]
        else:
            # The hang-up is a read of its own: a pass stops at the short read
            # that carried the frame, so whether the frame was handed on before
            # the next pass read the end is the scheduler's.
            assert len(said) == 1 and re.fullmatch(
                r"shard 7 the venue closed the connection \(end of stream\); "
                r"frames held for the recorder: [01], the kernel's longest hold of a read since the last stats: 0 ms",
                said[0],
            ), said
        assert driver.payloads() == [b'{"n":1}']
    finally:
        driver.stop()
        listener.close()


def test_a_refused_handshake_fails_the_open_with_the_venues_answer(reader_in_thread: Any) -> None:
    listener, thread = _raw_venue(lambda protocol, request: b"HTTP/1.1 403 Forbidden\r\nContent-Length: 12\r\n\r\nno thank you")
    driver = Driver(reader_in_thread)
    try:
        driver.command(op="open", link=1, url=f"ws://127.0.0.1:{listener.getsockname()[1]}", shard=0, ping=None)
        driver.read(lambda log: bool(driver.of(reader.FAILED)))
        assert b"403" in driver.of(reader.FAILED)[0][4]
        assert not driver.of(reader.OPENED)
    finally:
        driver.stop()
        listener.close()


def _venue_frames(end: sockets.socket) -> list[Frame]:
    """Every frame the reader has written to a socketpair's venue end."""

    protocol = ServerProtocol(state=State.OPEN, max_size=None)
    end.setblocking(False)
    while True:
        try:
            data = end.recv(65536)
        except BlockingIOError:
            break
        if not data:
            break
        protocol.receive_data(data)
    return [event for event in protocol.events_received() if isinstance(event, Frame)]


@pytest.mark.parametrize("socket_state", ["quiet", "flood", "silent"])
def test_the_venue_heartbeat_holds_its_cadence_whatever_the_socket_carries(socket_state: str) -> None:
    """The venue's ping every 20 s, on a socket carrying nothing but the
    pongs and on one flooding frames; a socket that carries nothing at all
    for 60 s, pongs included, is taken for dead and closed."""

    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    host, venue = sockets.socketpair()
    host.setblocking(False)
    link = reader.Link(1, host, ping='{"op":"ping"}')
    link.last_data_mono_ns = 0
    link.next_ping_ns = int(reader.PING_INTERVAL_SECONDS * 1e9)
    loop.links[1] = link
    pings: list[int] = []
    closed_at = None
    try:
        for second in range(1, 101):
            now_ns = second * 10**9
            if socket_state == "flood":
                link.last_data_mono_ns = now_ns
            loop._keepalive(now_ns)
            if link.open and link.wire:
                loop._flush(link)
            for frame in _venue_frames(venue):
                if frame.opcode is Opcode.TEXT and frame.data == b'{"op":"ping"}':
                    pings.append(now_ns)
                    if socket_state != "silent":
                        # The venue's answer lands 30 ms later.
                        link.last_data_mono_ns = now_ns + 30_000_000
                elif frame.opcode is Opcode.CLOSE and closed_at is None:
                    closed_at = now_ns
    finally:
        host.close()
        venue.close()
        for descriptor in (commands, commands_write, events_read, events):
            os.close(descriptor)

    if socket_state == "silent":
        assert [ping // 10**9 for ping in pings] == [20, 40, 60]
        assert closed_at is not None and 60 * 10**9 < closed_at <= 62 * 10**9
        return
    assert closed_at is None, "a live socket was closed"
    assert [ping // 10**9 for ping in pings] == [20, 40, 60, 80, 100]


def test_a_link_reports_its_path_its_ping_and_its_reads(reader_in_thread: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """What the recorder is told of a connection (`STATS`): the kernel's round
    trips and inbound segments, the protocol ping's round trip to its
    answer's receive stamp, and the frames its reads completed."""

    monkeypatch.setattr(reader, "PING_INTERVAL_SECONDS", 0.2)
    monkeypatch.setattr(reader, "STATS_SECONDS", 1.5)
    monkeypatch.setattr(reader, "KEEPALIVE_CHECK_SECONDS", 0.05)

    def venue(connection: Any) -> None:
        for n in range(100):
            connection.send(b'{"n":%d}' % n)
            time.sleep(0.002)
        try:
            connection.recv()
        except Exception:  # the reader closed it
            return

    driver = Driver(reader_in_thread)
    with _served(venue) as server:
        driver.command(op="open", link=1, url=f"ws://localhost:{server.socket.getsockname()[1]}", shard=0, ping=None)
        driver.read(lambda log: bool(driver.of(reader.STATS)), timeout=10.0)
        driver.command(op="close", link=1)
        driver.read(lambda log: bool(driver.of(reader.ENDED)))
        driver.stop()

    body = json.loads(driver.of(reader.STATS)[0][4])
    if sys.platform.startswith("linux"):
        assert body["tcp"]["data_segs_in"] > 0 and body["tcp"]["min_rtt_us"] > 0, body
    else:  # only Linux keeps a `struct tcp_info` the socket module can read
        assert body["tcp"] == {}, body
    assert body["ping_rtt_ms"] is not None and 0 < body["ping_rtt_ms"] < 1_000, body
    assert 0 < body["reads"] <= body["frames"] <= 100, body


def test_a_link_the_recorder_does_not_drain_is_held_to_its_bound_then_dropped_and_counted(
    reader_in_thread: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A venue flooding a link whose frames the recorder does not take: the
    reader holds `SOCKET_QUEUE_FRAMES` of them, one read past it at most,
    leaves the rest with TCP, and once the oldest has waited out the
    recorder's patience drops every one it held, says how many, and closes."""

    monkeypatch.setattr(reader, "SOCKET_QUEUE_FRAMES", 64)
    monkeypatch.setattr(reader, "QUEUE_PUT_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(reader, "EVENTS_BUFFER_BYTES", 4096)
    payload = b'{"topic":"publicTrade.BTCUSDT","data":[],"pad":"' + b" " * 100 + b'","n":%d}'

    def flood(connection: Any) -> None:
        try:
            for n in range(100_000):
                connection.send(payload % n)
        except Exception:  # the reader closed it
            return

    driver = Driver(reader_in_thread)
    with _served(flood) as server:
        driver.command(op="open", link=1, url=f"ws://localhost:{server.socket.getsockname()[1]}", shard=5, ping=None)
        # The recorder is busy: nothing is read from the events pipe for a while.
        time.sleep(1.5)
        driver.read(lambda log: bool(driver.of(reader.ENDED)), timeout=20.0)
        driver.stop()

    frames = [json.loads(p)["n"] for p in driver.payloads()]
    assert frames == list(range(len(frames))), "the frames handed over are not the venue's, in order"
    (drop,) = driver.of(reader.DROPPED)
    one_read = reader.READ_BUFFER_BYTES // len(payload % 0) + 1
    assert 64 <= drop[2] <= 64 + one_read, drop[2]
    assert drop[3] > 0, "the drop carries no stamp"
    kinds = [kind for kind, *_ in driver.log if kind != reader.FRAME]
    assert kinds == [reader.OPENED, reader.DROPPED, reader.ENDED]


def test_frames_reach_the_recorder_in_arrival_order_across_links(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two sockets read in one pass: the recorder, and so the tape, sees their
    frames as the host received them, not one socket's batch and then the
    other's; a full events buffer takes the oldest first and leaves each link
    the rest of its own."""

    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    a_sock, b_sock = sockets.socketpair()
    a, b = reader.Link(1, a_sock), reader.Link(2, b_sock)
    loop.links = {1: a, 2: b}
    try:
        a.pending = [(b"a1", 1_000, 100), (b"a2", 1_003, 130), (b"a3", 1_004, 140)]
        b.pending = [(b"b1", 1_001, 110), (b"b2", 1_002, 120)]
        loop._deliver(mono_ns=10_000)
        assert _delivered(loop.out) == [
            (1, 1_000, 100, b"a1"),
            (2, 1_001, 110, b"b1"),
            (2, 1_002, 120, b"b2"),
            (1, 1_003, 130, b"a2"),
            (1, 1_004, 140, b"a3"),
        ]
        assert (a.pending, b.pending) == ([], [])
        assert a.reading and b.reading

        loop.out = bytearray()
        monkeypatch.setattr(reader, "EVENTS_BUFFER_BYTES", 2 * (reader.EVENT.size + 2))
        a.pending = [(b"a1", 1_000, 100), (b"a2", 1_003, 130)]
        b.pending = [(b"b1", 1_001, 110), (b"b2", 1_002, 120)]
        loop._deliver(mono_ns=10_000)
        assert [frame[3] for frame in _delivered(loop.out)] == [b"a1", b"b1"]
        assert (a.pending, b.pending) == ([(b"a2", 1_003, 130)], [(b"b2", 1_002, 120)])
    finally:
        for sock in (a_sock, b_sock):
            sock.close()
        for descriptor in (commands, commands_write, events_read, events):
            os.close(descriptor)


def _delivered(out: bytes) -> list[tuple[int, int, int, bytes]]:
    """The frame events in a reader's events buffer: `(link, wall_ns, mono_ns, payload)`."""

    frames = []
    offset = 0
    while offset < len(out):
        size, kind, ident, now_ns, mono_ns = reader.EVENT.unpack_from(out, offset)
        offset += reader.EVENT.size
        assert kind == reader.FRAME
        frames.append((ident, now_ns, mono_ns, bytes(out[offset : offset + size])))
        offset += size
    return frames


def test_a_stop_hands_back_every_frame_read_and_the_reader_exits(tmp_path: Path) -> None:
    sent = threading.Event()

    def venue(connection: Any) -> None:
        for n in range(1000):
            connection.send(b'{"n":%d}' % n)
        sent.set()
        try:
            connection.recv()
        except Exception:  # the reader closed it
            return

    driver = Driver(record.spawn_reader)
    with _served(venue) as server:
        driver.command(op="open", link=1, url=f"ws://localhost:{server.socket.getsockname()[1]}", shard=0, ping=None)
        driver.read(lambda log: bool(driver.of(reader.OPENED)))
        assert sent.wait(5.0)
        time.sleep(0.2)
        driver.stop()

    assert driver.payloads() == [b'{"n":%d}' % n for n in range(1000)]
    assert [kind for kind, *_ in driver.log if kind != reader.FRAME] == [reader.OPENED, reader.ENDED]
    assert driver.process.returncode == 0


def test_a_reader_whose_recorder_is_gone_exits() -> None:
    commands_read, commands = os.pipe()
    events, events_write = os.pipe()
    process = record.spawn_reader(commands_read, events_write)
    os.close(commands)
    try:
        assert process.wait(timeout=10.0) == 0
        assert os.read(events, 1) == b""
    finally:
        os.close(events)


class _Aged:
    """A socket whose receive stamps read `by_ns` older than the kernel's."""

    def __init__(self, sock: sockets.socket, by_ns: int) -> None:
        self.sock = sock
        self.by_ns = by_ns

    def recvmsg_into(self, buffers: list[Any], size: int) -> tuple[int, list[tuple[int, int, bytes]], int, Any]:
        count, ancillary, flags, address = self.sock.recvmsg_into(buffers, size)
        aged = []
        for level, kind, data in ancillary:
            seconds, nanoseconds = reader.TIMESPEC.unpack_from(data)
            stamp = seconds * 10**9 + nanoseconds - self.by_ns
            aged.append((level, kind, reader.TIMESPEC.pack(*divmod(stamp, 10**9))))
        return count, aged, flags, address

    def __getattr__(self, name: str) -> Any:
        return getattr(self.sock, name)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the kernel's receive stamps are Linux's")
def test_a_segment_the_kernel_stamped_before_the_read_before_it_is_stamped_at_that_read(tmp_path: Path) -> None:
    """TCP hands a recovered segment over behind the one that overtook it, so
    the kernel's stamp on a later read is older than the read before's. The
    tape's sort key is per link: the read is stamped at the earlier read's
    instant, both clocks by the same shift. Here the kernel's stamp on the
    second read is made to read 200 ms older than it is."""

    listener = sockets.create_server(("127.0.0.1", 0))
    client = sockets.create_connection(listener.getsockname())
    venue, _ = listener.accept()
    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    link = reader.Link(1, client)
    link.stamps = reader._ask_for_stamps(client)
    client.setblocking(False)
    loop.links[1] = link
    try:
        assert link.stamps
        venue.sendall(Frame(Opcode.TEXT, b"first").serialize(mask=False))
        time.sleep(0.05)
        loop._read(link)
        venue.sendall(Frame(Opcode.TEXT, b"recovered").serialize(mask=False))
        time.sleep(0.05)
        link.sock = _Aged(client, 200_000_000)
        loop._read(link)
        (first, wall_1, mono_1), (second, wall_2, mono_2) = link.pending
        assert (first, second) == (b"first", b"recovered")
        assert wall_2 == wall_1, (wall_2 - wall_1) / 1e6
        assert abs(mono_2 - mono_1) < 5_000_000, (mono_2 - mono_1) / 1e6
    finally:
        for sock in (listener, client, venue):
            sock.close()
        for descriptor in (commands, commands_write, events_read, events):
            os.close(descriptor)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the kernel's receive stamps are Linux's")
def test_a_frame_the_kernel_held_is_stamped_when_it_arrived_not_when_it_was_read(tmp_path: Path) -> None:
    listener = sockets.create_server(("127.0.0.1", 0))
    client = sockets.create_connection(listener.getsockname())
    venue, _ = listener.accept()
    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    link = reader.Link(1, client)
    link.stamps = reader._ask_for_stamps(client)
    client.setblocking(False)
    loop.links[1] = link
    try:
        assert link.stamps
        # The kernel turns receive stamping on host-wide from a work item after
        # the first socket asks (net_enable_timestamp), and stamps a segment
        # that arrived before then when it is read. On a loaded host that item
        # runs late, so the frame under test waits for a warm-up frame stamped
        # when it arrived.
        deadline = time.monotonic() + 10.0
        while True:
            venue.sendall(Frame(Opcode.TEXT, b"warm").serialize(mask=False))
            warm_sent_ns = time.time_ns()
            time.sleep(0.05)
            loop._read(link)
            ((_warm, warm_ns, _warm_mono_ns),) = link.pending
            link.pending.clear()
            if warm_ns - warm_sent_ns < 25_000_000:
                break
            assert time.monotonic() < deadline, "the kernel never stamped a segment when it arrived"
        venue.sendall(Frame(Opcode.TEXT, b"held").serialize(mask=False))
        arrived_ns = time.time_ns()
        time.sleep(0.3)
        loop._read(link)
        ((payload, wall_ns, _mono_ns),) = link.pending
        assert payload == b"held"
        assert abs(wall_ns - arrived_ns) < 50_000_000, (wall_ns - arrived_ns) / 1e6
    finally:
        for sock in (listener, client, venue):
            sock.close()
        for descriptor in (commands, commands_write, events_read, events):
            os.close(descriptor)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the kernel's receive stamps are Linux's")
def test_a_read_held_up_after_it_reads_the_wall_clock_is_stamped_when_its_segment_arrived(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kernel's hold is measured against the wall reading the frame is
    stamped from, so a reader preempted after that reading still stamps the
    frame when it arrived. Measured against a later reading of the clock, the
    hold took in the preemption and the frame was stamped that much before it
    arrived: the tape's sort key early by however long the host kept the
    reader waiting. Here the realtime clock reads 20 ms on after the wall
    reading, as a 20 ms preemption leaves it."""

    listener = sockets.create_server(("127.0.0.1", 0))
    client = sockets.create_connection(listener.getsockname())
    venue, _ = listener.accept()
    commands, commands_write = os.pipe()
    events_read, events = os.pipe()
    loop = reader.Reader(commands, events)
    link = reader.Link(1, client)
    link.stamps = reader._ask_for_stamps(client)
    client.setblocking(False)
    loop.links[1] = link
    real_clock = time.clock_gettime_ns
    try:
        assert link.stamps
        with _receive_stamping():
            venue.sendall(Frame(Opcode.TEXT, b"held").serialize(mask=False))
            arrived_ns = time.time_ns()
            time.sleep(0.1)
            monkeypatch.setattr(reader.time, "clock_gettime_ns", lambda clock: real_clock(clock) + 20_000_000)
            loop._read(link)
        ((payload, wall_ns, _mono_ns),) = link.pending
        assert payload == b"held"
        assert abs(wall_ns - arrived_ns) < 5_000_000, (wall_ns - arrived_ns) / 1e6
    finally:
        for sock in (listener, client, venue):
            sock.close()
        for descriptor in (commands, commands_write, events_read, events):
            os.close(descriptor)
