"""The recorder's side of the reader process (`market_tape.reader`), and the intake.

The sockets are read in a process of their own, which connects every shard,
keeps it alive, and stamps every frame as it is read; the writer parses and
serialises every frame, and on one interpreter lock the two took turns, which
on the capture host cost more CPU than either's work and delayed every stamp
by the writer's turn. Here: `spawn_reader` starts it, `ReaderProcess` writes
it commands and, on the `tape-intake` thread, moves the frames it read into
the capture queue (`market_tape/recorder/queue.py`) in the order they were
read, and `RemoteLink` is one shard's websocket as a shard's thread holds it.
A reader that exits unasked ends every link it held, and is started again.
"""

from __future__ import annotations

import json
import logging
import os
import select
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping

from market_tape import reader
from market_tape.recorder.queue import EVENT_HEAD, FrameQueue, Overrun, QueueItem, received_ns_of

if TYPE_CHECKING:
    from market_tape.recorder.shard import Shard


#: The intake's tick while frames flow, the most one read of the events
#: pipe takes, and how long one offer to a full queue waits before it looks
#: again whether the recorder is closing. Frames carry the reader's stamps,
#: so the tick delays the disk and never the tape's clocks; on the capture
#: host a wake-up costs ~80 µs of CPU, and the pipe and the reader's own
#: buffer hold a tick of a burst many times over.
INTAKE_TICK_SECONDS = 0.02
INTAKE_READ_BYTES = 1024 * 1024
INTAKE_PUT_SECONDS = 0.5
#: What an open waits past the reader's own connect timeout before it gives up on the reader.
OPEN_GRACE_SECONDS = 5.0
#: The pause before a reader that exited unasked is started again, and how
#: long an exited reader has to be reaped before it is killed.
READER_RESPAWN_SECONDS = 1.0
READER_EXIT_SECONDS = 5.0


class RemoteLink:
    """One shard's websocket as the recorder holds it. The reader process owns
    the socket (`market_tape.reader`); through this the shard's thread asks
    for text to be sent and for the close, and learns when it opened and
    when it ended."""

    def __init__(self, reader: ReaderProcess, ident: int, *, tier: str, shard: Shard) -> None:
        self.reader = reader
        self.ident = ident
        self.tier = tier
        self.shard = shard
        reader.tiers[ident] = tier
        self.opened = threading.Event()
        #: Set once the socket is closed and every frame read from it is queued
        #: or counted: until then a reconnect's snapshot could reach the writer
        #: ahead of this connection's last deltas.
        self.ended = threading.Event()
        #: Why it never opened, or why the reader ended it unasked.
        self.error = ""
        self.close_requested = False

    def send_text(self, text: str) -> None:
        if self.close_requested or self.ended.is_set():
            raise ConnectionError("the connection is closing")
        self.reader.command({"op": "send", "link": self.ident, "text": text})

    def request_close(self) -> None:
        """Ask the reader to close the socket; `ended` follows once its frames are queued."""

        if self.close_requested or self.ended.is_set():
            return
        self.close_requested = True
        try:
            self.reader.command({"op": "close", "link": self.ident})
        except ConnectionError:
            # The reader is gone, and every link it held ends with it.
            pass


def spawn_reader(commands: int, events: int) -> subprocess.Popen[bytes]:
    """`python -m market_tape.reader` on its ends of the two pipes, which this
    process then closes: the reader's exit is the events pipe's end of file."""

    package_root = str(Path(__file__).resolve().parents[2])
    search = os.environ.get("PYTHONPATH")
    environment = dict(os.environ, PYTHONPATH=package_root + (os.pathsep + search if search else ""))
    try:
        return subprocess.Popen(
            [sys.executable, "-m", "market_tape.reader", "--commands", str(commands), "--events", str(events)],
            pass_fds=(commands, events),
            stdin=subprocess.DEVNULL,
            env=environment,
        )
    finally:
        os.close(commands)
        os.close(events)


class ReaderProcess:
    """The recorder's side of the reader process.

    It starts the reader, writes it commands, and on the `tape-intake`
    thread moves what the reader read into the capture queue in the order it
    was read. The intake wakes on a tick while frames flow: the frames carry
    the reader's stamps, so the tick delays the queue and never the tape's
    clocks. When the queue has no room the intake waits and the events pipe
    fills; the reader then holds each shard's frames up to
    `reader.SOCKET_QUEUE_FRAMES` and overruns a shard whose oldest has waited
    `reader.QUEUE_PUT_TIMEOUT_SECONDS`, as it reports with `DROPPED`. A reader
    that exits unasked ends every link it held, and is started again.
    """

    def __init__(self, frames: FrameQueue, *, spawn: Callable[[int, int], Any] = spawn_reader) -> None:
        self.frames = frames
        self.spawn = spawn
        #: Guards `links`, `next_ident`, `process`, `events` and `stopping`.
        self.lock = threading.Lock()
        #: Guards `commands`: a command is written whole, and never to a
        #: descriptor a relaunch has closed and the kernel may have reused.
        self.command_lock = threading.Lock()
        self.links: dict[int, RemoteLink] = {}
        #: Every link's tier by its ident, kept past the link: the writer reads
        #: a queued event's tier after the link that read it has ended.
        self.tiers: dict[int, str] = {}
        self.next_ident = 1
        self.process: Any = None
        self.commands = -1
        self.events = -1
        self.stopping = False
        #: Set by `close`: past it, frames the queue has no room for are counted dropped.
        self.close_deadline: float | None = None
        #: The intake's alone: bytes past the last whole event, and how long
        #: they must grow before the next event is whole.
        self.held = bytearray()
        self.need = 0
        self.thread = threading.Thread(target=self._run, name="tape-intake", daemon=True)

    @property
    def pid(self) -> int | None:
        pid = getattr(self.process, "pid", None)
        return pid if isinstance(pid, int) else None

    def start(self) -> None:
        self._launch()
        self.thread.start()

    def alive(self) -> bool:
        return self.thread.is_alive()

    def _launch(self) -> None:
        commands_read, commands_write = os.pipe()
        events_read, events_write = os.pipe()
        reader.size_pipe(events_read)
        os.set_blocking(events_read, False)
        try:
            process = self.spawn(commands_read, events_write)
        except BaseException:
            os.close(commands_write)
            os.close(events_read)
            raise
        with self.lock, self.command_lock:
            self.process, self.commands, self.events = process, commands_write, events_read

    def command(self, message: Mapping[str, Any]) -> None:
        line = memoryview((json.dumps(message, separators=(",", ":")) + "\n").encode())
        with self.command_lock:
            try:
                while line:
                    line = line[os.write(self.commands, line) :]
            except OSError as exc:
                raise ConnectionError(f"the tape reader is gone: {exc}") from exc

    def open(self, url: str, *, tier: str, shard: Shard, ping: str | None = None) -> RemoteLink:
        """Have the reader connect `url` for `shard`; block until it has, and
        return the link. Raises what the connect failed with."""

        with self.lock:
            if self.stopping:
                raise ConnectionError("the recorder is stopping")
            ident = self.next_ident
            self.next_ident += 1
            link = RemoteLink(self, ident, tier=tier, shard=shard)
            self.links[ident] = link
        try:
            self.command({"op": "open", "link": ident, "url": url, "shard": shard.index, "ping": ping})
        except ConnectionError:
            self._forget(link, "the tape reader is gone")
            raise
        if not link.opened.wait(reader.CONNECT_TIMEOUT_SECONDS + OPEN_GRACE_SECONDS):
            link.request_close()
            raise TimeoutError(f"the tape reader did not open {url} in {reader.CONNECT_TIMEOUT_SECONDS:g}s")
        if link.error:
            raise ConnectionError(link.error)
        return link

    def close(self, timeout: float = reader.QUEUE_PUT_TIMEOUT_SECONDS) -> None:
        """Stop the reader. It closes every link and hands back what they
        read; what reaches the queue within `timeout` is kept, the rest is
        counted as dropped; every link ends."""

        with self.lock:
            self.stopping = True
        self.close_deadline = time.monotonic() + timeout
        try:
            self.command({"op": "stop"})
        except ConnectionError:
            pass
        if self.thread.ident is not None:
            # The reader's own patience bounds its shutdown; past this it is stuck.
            self.thread.join(timeout + reader.CONNECT_TIMEOUT_SECONDS + OPEN_GRACE_SECONDS)
            if self.thread.is_alive():
                logging.error("the tape reader did not stop; killing it")
                self._kill()
                self.thread.join()
        else:
            self._kill()
            self._reap()
        with self.lock:
            links = list(self.links.values())
            self.links.clear()
        for link in links:
            link.error = link.error or "the recorder is stopping"
            link.opened.set()
            link.ended.set()

    # -------------------------------------------------------------- intake

    def _run(self) -> None:
        while True:
            try:
                if not self._intake_pass():
                    return
            except Exception:  # every frame reaches the queue through this thread
                logging.exception("tape intake pass failed")
                time.sleep(INTAKE_TICK_SECONDS)

    def _intake_pass(self) -> bool:
        """Read the events pipe dry into the queue; False once the reader has exited on request."""

        descriptor = self.events
        got = False
        while True:
            try:
                chunk = os.read(descriptor, INTAKE_READ_BYTES)
            except (BlockingIOError, InterruptedError):
                break
            if not chunk:
                return self._reader_exited()
            got = True
            self._take(chunk)
            if len(chunk) < INTAKE_READ_BYTES:
                break
        if got:
            time.sleep(INTAKE_TICK_SECONDS)
        else:
            select.select([descriptor], [], [], 1.0)
        return True

    def _take(self, chunk: bytes) -> None:
        if self.held:
            self.held += chunk
            if len(self.held) < self.need:
                return
            data = bytes(self.held)
            self.held.clear()
        else:
            data = chunk
        head = EVENT_HEAD.unpack_from
        header = reader.EVENT.size
        links = self.links
        items: list[QueueItem] = []
        owners: list[RemoteLink] = []
        size = len(data)
        at = 0
        need = 0
        while size - at >= header:
            length, kind, ident = head(data, at)
            event = at
            start = at + header
            end = start + length
            if end > size:
                need = end - at
                break
            at = end
            link = links.get(ident)
            if link is None:
                continue
            if kind == reader.FRAME:
                items.append(data[event:end])
                owners.append(link)
            elif kind == reader.OPENED:
                link.opened.set()
            elif kind == reader.STATS:
                link.shard.link_stats = json.loads(data[start:end])
            else:
                # Every frame read before a drop or an end is queued before it is told.
                self._queue(items, owners)
                items, owners = [], []
                if kind == reader.DROPPED:
                    _, _, _, a, b = reader.EVENT.unpack_from(data, event)
                    link.shard.on_frame(b, a)
                    link.shard.on_overrun(link.shard, a)
                else:
                    self._forget(link, data[start:end].decode(errors="replace") if kind == reader.FAILED else "")
        self._queue(items, owners)
        if at < size:
            self.held += data[at:] if at else data
        self.need = need

    def _queue(self, items: list[QueueItem], owners: list[RemoteLink]) -> None:
        if not items:
            return
        counted: dict[RemoteLink, list[Any]] = {}
        for item, link in zip(items, owners):
            seen = counted.get(link)
            if seen is None:
                counted[link] = [1, item]
            else:
                seen[0] += 1
                seen[1] = item
        for link, (count, newest) in counted.items():
            newest_ns = received_ns_of(newest)
            link.shard.last_message_ns = newest_ns
            link.shard.on_frame(newest_ns, count)
        while items:
            try:
                self.frames.put_batch(items, timeout=INTAKE_PUT_SECONDS)
                return
            except Overrun as exc:
                taken = len(items) - exc.dropped
                items, owners = items[taken:], owners[taken:]
            deadline = self.close_deadline
            if deadline is not None and time.monotonic() >= deadline:
                dropped: dict[RemoteLink, int] = {}
                for link in owners:
                    dropped[link] = dropped.get(link, 0) + 1
                for link, count in dropped.items():
                    link.shard.on_overrun(link.shard, count)
                return

    def _forget(self, link: RemoteLink, error: str) -> None:
        with self.lock:
            self.links.pop(link.ident, None)
        if error:
            link.error = error
        link.opened.set()
        link.ended.set()

    def _reader_exited(self) -> bool:
        with self.lock:
            stopping = self.stopping
            links = list(self.links.values())
            self.links.clear()
        code = self._reap()
        self.held.clear()
        self.need = 0
        with self.lock, self.command_lock:
            os.close(self.commands)
            os.close(self.events)
            self.commands = self.events = -1
        for link in links:
            link.error = link.error or "the tape reader exited"
            link.opened.set()
            link.ended.set()
        if stopping:
            return False
        logging.error("the tape reader exited (%s); its %d connections end and reconnect", code, len(links))
        while True:
            time.sleep(READER_RESPAWN_SECONDS)
            with self.lock:
                if self.stopping:
                    return False
            try:
                self._launch()
                return True
            except OSError as exc:
                logging.error("could not start the tape reader: %s", exc)

    def _reap(self) -> Any:
        process = self.process
        if process is None:
            return None
        try:
            return process.wait(timeout=READER_EXIT_SECONDS)
        except subprocess.TimeoutExpired:
            self._kill()
            return process.wait()

    def _kill(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.kill()
