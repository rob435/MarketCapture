"""Files on the recording host: segments, receipts, compression, retention, snapshots.

Layout under the root:

```text
<day>/<HH>/<SYMBOL>/segment-NNNNNN.jsonl.zst   one symbol, one UTC hour (rolled at the size cap)
<day>/<HH>/_meta/instruments-<stamp>.json.zst  the venue's instrument table, as of that moment
<day>/<HH>/_meta/tickers-<stamp>.json.zst      the venue's ticker table, as of that moment
manifest.jsonl                                 a receipt per file the root holds, a row per deletion (`Manifest`)
status.json                                    the recorder's own health, rewritten on a timer
.recorder.lock                                 whoever holds it owns the raw files under this root
```

An hour's segment is written as `.jsonl.partial`, renamed to `.jsonl` when it
closes, and compressed to `.jsonl.zst` by a background thread that verifies the
archive before deleting the raw file. A restart finishes whatever was open.

Durability of the open segment (the recovery point objective): rows are fsynced
every `fsync_every` records per symbol, so a power loss can lose up to
`fsync_every - 1` acknowledged rows of each symbol's open segment. A process
crash loses only what is still in that segment's write buffer
(`SegmentWriter.buffer_bytes`), since the kernel keeps what was written
through it. A closed segment is fsynced whole before it is renamed; the rename
itself is made durable by the compressor's directory sync, and a rename a power
loss undoes leaves a `.partial` that recovery finishes.

`SegmentWriter.append` either keeps a row no longer than that buffer or raises
having kept none of it, so the caller's loss count is exact; a longer row is
written past the buffer in pieces, and a refused piece leaves part of it on
disk. A write the disk refuses stays in the segment's buffer, which a later
flush completes in order, and the segment stays open until its close succeeds:
a full disk delays a segment, it never tears one.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import logging
import os
import queue
import selectors
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Container, Iterable, Mapping

from market_tape.jsonfast import dumps_line
from market_tape.schema import SNAPSHOT_INSTRUMENTS, SNAPSHOT_TICKERS, snapshot_payload

#: The per-hour directory holding the venue's instrument and ticker tables and
#: the recorder's coverage records.
META_DIRECTORY = "_meta"

#: Receipt kinds in `manifest.jsonl` that name a file the tape still holds.
FILE_RECEIPT_KINDS = ("segment_compressed", "snapshot_compressed", "coverage_compressed", "segment_blocks")
#: Row kinds in `manifest.jsonl` that say a receipted file has left the root.
DELETION_KINDS = ("segment_deleted", "snapshot_deleted")
#: A retention pass rewrites the manifest as its live receipts once the file is
#: past this and past twice what the last rewrite left, so it holds at most
#: about twice the receipts of what the root holds.
MANIFEST_COMPACT_BYTES = 4 * 1024 * 1024

#: One holder at a time per tape root. The recorder holds it for the life of a
#: run; `market_tape pack` takes it to finish the raw segments of a root no
#: recorder is running on.
ROOT_LOCK_NAME = ".recorder.lock"

#: How far past `min_free_bytes` a pass driven by free space keeps deleting,
#: as a fraction of that floor. `writable()` lets the writer run again the
#: moment free space reaches the floor, so a pass that stops on the floor
#: hands the writer no room at all: it re-crosses within one status interval
#: and every frame in between is discarded. The gap between the two thresholds
#: is what makes a crossing resolve instead of repeat.
FREE_HEADROOM_FRACTION = 0.05

#: Seconds one zstd call (compress or verify) may take on one segment before it
#: is a failure: a stuck disk must not hold the compressor, or a stop, forever.
ZSTD_TIMEOUT_SECONDS = 600.0
#: Seconds `Compressor.close()` waits for the queue to drain before reporting
#: what it left behind.
COMPRESSOR_STOP_TIMEOUT_SECONDS = 900.0
PAGE_BYTES = os.sysconf("SC_PAGE_SIZE")


def discard_file_cache(handle: Any, length: int = 0) -> None:
    """Release a file's clean cached pages, the first `length` bytes' or all of them at 0.

    Linux starts writeback of a dirty page in the range and keeps it; a later
    call drops it. Replay reads archived files, not hot writes.
    """

    advise = getattr(os, "posix_fadvise", None)
    dont_need = getattr(os, "POSIX_FADV_DONTNEED", None)
    if advise is None or dont_need is None:
        return
    try:
        advise(handle.fileno(), 0, length, dont_need)
    except OSError:
        # Cache eviction is a throughput hint. Durability already came from
        # fsync, so an unsupported filesystem must not stop the tape.
        return


def adopt_owner(path: Path) -> None:
    """Give `path` its parent directory's uid and gid, when this process is root.

    `market_tape pack` runs as root and the recorder as its own user: a
    root-owned 0640 file under the recorder's root is one the recorder can
    neither read nor replace.
    """

    if os.geteuid() != 0:
        return
    directory = path.parent.stat()
    current = path.stat()
    if current.st_uid != directory.st_uid or current.st_gid != directory.st_gid:
        os.chown(path, directory.st_uid, directory.st_gid)


def lock_root(root: Path) -> int | None:
    """Take the tape root's exclusive lock; `None` when another process holds it.

    Read-only: `flock` needs an open file, not a writable one.
    """

    path = root / ROOT_LOCK_NAME
    descriptor = os.open(path, os.O_RDONLY | os.O_CREAT, 0o640)
    try:
        adopt_owner(path)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(descriptor)
        if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
            return None
        raise
    return descriptor


def unlock_root(descriptor: int) -> None:
    os.close(descriptor)


HOUR_NS = 3_600_000_000_000
_last_hour: tuple[int | None, tuple[str, str]] = (None, ("", ""))


def utc_day_hour(ns: int) -> tuple[str, str]:
    global _last_hour
    hour_bin = ns // HOUR_NS
    cached_bin, day_hour = _last_hour
    if hour_bin != cached_bin:
        # From the integer hour: `ns / 1e9` rounds the last microsecond of an hour into the next.
        moment = datetime.fromtimestamp(hour_bin * 3_600, tz=timezone.utc)
        day_hour = (moment.date().isoformat(), f"{moment.hour:02d}")
        _last_hour = (hour_bin, day_hour)
    return day_hour


@dataclass(slots=True)
class ActiveSegment:
    symbol: str
    day: str
    hour: str
    path: Path
    handle: Any
    #: The hour `day` and `hour` name, as `utc_day_hour` cuts it: `[hour_start_ns, hour_end_ns)`.
    hour_start_ns: int = 0
    hour_end_ns: int = 0
    bytes_written: int = 0
    records: int = 0
    first_receive_ns: int = 0
    last_receive_ns: int = 0
    unsynced: int = 0


@dataclass(frozen=True, slots=True)
class ClosedSegment:
    path: Path
    symbol: str
    day: str
    hour: str
    records: int
    first_receive_ns: int
    last_receive_ns: int


def segment_identity(path: Path, root: Path) -> tuple[str, str, str]:
    """(day, hour, symbol) for a segment path, `<day>/<HH>/<SYMBOL>/segment-*`."""

    parts = path.resolve().relative_to(root.resolve()).parts
    if len(parts) == 4 and len(parts[1]) == 2 and parts[1].isdigit():
        return parts[0], parts[1], parts[2].upper()
    raise ValueError(f"not a capture segment path: {path}")


class SegmentWriter:
    def __init__(self, root: Path, max_bytes: int, fsync_every: int, buffer_bytes: int = 64 * 1024) -> None:
        self.root = root
        self.max_bytes = max_bytes
        self.fsync_every = fsync_every
        self.buffer_bytes = buffer_bytes
        self.active: dict[str, ActiveSegment] = {}

    def append(self, row: Mapping[str, Any]) -> list[ClosedSegment]:
        received_ns = int(row.get("local_receive_ts_ns") or 0)
        symbol = str(row.get("symbol") or "").upper()
        if received_ns <= 0:
            raise ValueError("capture row has no receive timestamp")
        if not symbol:
            raise ValueError("capture row has no symbol")
        segment = self.active.get(symbol)
        if segment is not None and received_ns < segment.last_receive_ns:
            # The sort key holds per segment. A row that reaches the writer
            # behind one it already holds (a lane row behind stream frames,
            # one shard's frames behind another's) could not have been read
            # before it: stamped at its instant, both clocks by the same shift.
            shift = segment.last_receive_ns - received_ns
            received_ns = segment.last_receive_ns
            row = {**row, "local_receive_ts_ns": received_ns}
            mono_ns = int(row.get("local_receive_mono_ns") or 0)
            if mono_ns > 0:
                row["local_receive_mono_ns"] = mono_ns + shift
        payload = dumps_line(row)
        closed: list[ClosedSegment] = []
        if segment is not None and (
            not segment.hour_start_ns <= received_ns < segment.hour_end_ns
            or segment.bytes_written + len(payload) > self.max_bytes
            or segment.handle.closed
        ):
            closed.append(self._close(symbol))
            segment = None
        if segment is None:
            segment = self._open(symbol, *utc_day_hour(received_ns))
        elif segment.unsynced >= self.fsync_every:
            # The sync after the previous row failed; this row waits on it.
            self._sync(segment)
        written = segment.handle.write(payload)
        if written != len(payload):
            raise OSError("short tape write")
        segment.bytes_written += written
        segment.records += 1
        segment.first_receive_ns = segment.first_receive_ns or received_ns
        segment.last_receive_ns = received_ns
        segment.unsynced += 1
        if segment.unsynced >= self.fsync_every:
            try:
                self._sync(segment)
            except OSError:
                # The row is buffered and kept; the segment's next row retries
                # the sync first and raises before it buffers anything.
                pass
        return closed

    @staticmethod
    def _sync(segment: ActiveSegment) -> None:
        segment.handle.flush()
        os.fsync(segment.handle.fileno())
        discard_file_cache(segment.handle)
        segment.unsynced = 0

    def release_cache(self) -> None:
        """Drop the cached whole pages of every open segment.

        systemd charges the tape's pages to the recorder's unit, and a segment
        can stay open for its whole hour: left alone, the open segments' pages
        hold the unit at its MemoryMax and the kernel reclaims inside the
        recorder's own allocations. Called every few seconds, this keeps the
        cache at about two calls' writes. The last partial page stays: an append
        into a dropped one reads it back from disk first.
        """

        for segment in self.active.values():
            if segment.handle.closed:
                continue
            try:
                whole = os.fstat(segment.handle.fileno()).st_size // PAGE_BYTES * PAGE_BYTES
            except OSError:
                continue
            if whole:
                discard_file_cache(segment.handle, whole)

    def roll_idle(self, now_ns: int) -> list[ClosedSegment]:
        """Close every segment whose hour has passed, so a quiet symbol's hour still ships on time."""

        day, hour = utc_day_hour(now_ns)
        closed: list[ClosedSegment] = []
        for symbol, segment in list(self.active.items()):
            if (segment.day, segment.hour) < (day, hour):
                closed.append(self._close(symbol))
        return closed

    def _open(self, symbol: str, day: str, hour: str) -> ActiveSegment:
        directory = self.root / day / hour / symbol
        directory.mkdir(parents=True, exist_ok=True)
        indices = []
        for path in directory.glob("segment-*"):
            try:
                indices.append(int(path.name.split("-", 1)[1].split(".", 1)[0]))
            except (IndexError, ValueError):
                continue
        index = max(indices, default=-1) + 1
        path = directory / f"segment-{index:06d}.jsonl.partial"
        handle = path.open("xb", buffering=self.buffer_bytes)
        os.chmod(path, 0o640)
        start = int(datetime.fromisoformat(f"{day}T{hour}:00:00+00:00").timestamp()) * 1_000_000_000
        segment = ActiveSegment(
            symbol=symbol, day=day, hour=hour, path=path, handle=handle, hour_start_ns=start, hour_end_ns=start + HOUR_NS
        )
        self.active[symbol] = segment
        return segment

    def _close(self, symbol: str) -> ClosedSegment:
        segment = self.active[symbol]
        # A close whose rename failed is retried from the rename.
        if not segment.handle.closed:
            self._sync(segment)
            segment.handle.close()
        final = segment.path.with_suffix("")
        os.replace(segment.path, final)
        del self.active[symbol]
        return ClosedSegment(
            path=final,
            symbol=segment.symbol,
            day=segment.day,
            hour=segment.hour,
            records=segment.records,
            first_receive_ns=segment.first_receive_ns,
            last_receive_ns=segment.last_receive_ns,
        )

    def close(self) -> list[ClosedSegment]:
        """Close every open segment. One the disk refuses is left as its
        `.partial`, whole lines of which the next start's recovery keeps."""

        closed: list[ClosedSegment] = []
        for symbol in list(self.active):
            try:
                closed.append(self._close(symbol))
            except OSError as exc:
                segment = self.active.pop(symbol)
                logging.error("tape segment left for recovery: %s: %s", segment.path, exc)
                try:
                    segment.handle.close()
                except OSError:
                    pass
        return closed


class Manifest:
    """The root's `manifest.jsonl`: a receipt as each file is written, a
    deletion row as each leaves. `kinds` are the receipt kinds that name a
    file the root holds; a store that keeps its own files' receipts in a
    manifest of this shape names its kinds here.

    This process appends under `lock`. `market_tape pack` appends deletion rows
    from its own process, lock or none, and receipts only while it holds the
    root lock, when no recorder runs. `compact` replaces the file, so only the
    root lock's holder calls it, and `close` ends it before that lock goes. A
    deletion row `pack` appends to the file a rewrite is replacing is lost:
    its file is already gone, so the rewrite either dropped its receipt or
    kept it for the next rewrite to drop.
    """

    def __init__(self, root: Path, kinds: Iterable[str] = FILE_RECEIPT_KINDS) -> None:
        self.path = root / "manifest.jsonl"
        self.kinds = tuple(kinds)
        self.lock = threading.Lock()
        self._kept_bytes = 0
        self._closed = False

    def append(self, row: Mapping[str, Any]) -> None:
        self.extend((row,))

    def extend(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Every row in one write and one fsync."""

        payload = b"".join(_receipt_line(row) for row in rows)
        if not payload:
            return
        with self.lock:
            created = not self.path.exists()
            with self.path.open("ab+") as handle:
                # An append the disk cut short left a torn line; a receipt
                # written onto its end would be unreadable with it.
                end = handle.seek(0, os.SEEK_END)
                if end:
                    handle.seek(end - 1)
                    if handle.read(1) != b"\n":
                        payload = b"\n" + payload
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            if created:
                adopt_owner(self.path)

    def compact_if_grown(self) -> bool:
        """`compact` once the file is past `MANIFEST_COMPACT_BYTES` and twice what the last one left."""

        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return False
        if size <= max(2 * self._kept_bytes, MANIFEST_COMPACT_BYTES):
            return False
        return self.compact()

    def compact(self) -> bool:
        """Rewrite the file as the receipts of the files the root still holds; whether it did.

        The read and the stats run without `lock`, so appends go on meanwhile;
        under it, what they added since is folded in and the rewrite replaces
        the file in one rename.
        """

        try:
            receipts, offset = _live_receipts(self.path, self.kinds)
        except FileNotFoundError:
            return False
        with self.lock:
            if self._closed:
                return False
            with self.path.open("rb") as handle:
                handle.seek(offset)
                for line in handle:
                    _fold_receipt(line, receipts, self.kinds)
                previous = os.fstat(handle.fileno())
            temporary = self.path.with_name(f".{self.path.name}.compact")
            try:
                with temporary.open("wb") as handle:
                    for row in receipts.values():
                        handle.write(_receipt_line(row))
                    handle.flush()
                    os.fsync(handle.fileno())
                    kept = handle.tell()
                os.chmod(temporary, previous.st_mode & 0o7777)
                adopt_owner(temporary)
                os.replace(temporary, self.path)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
            sync_directory(self.path.parent)
            self._kept_bytes = kept
        return True

    def close(self) -> None:
        """No `compact` from here on: the caller is letting go of the root lock."""

        with self.lock:
            self._closed = True


def _receipt_line(row: Mapping[str, Any]) -> bytes:
    return (json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _fold_receipt(line: bytes, receipts: dict[str, dict[str, Any]], kinds: Container[str]) -> None:
    """One manifest line into `receipts`: a receipt of `kinds` names its file,
    a deletion row forgets it, a torn line is nothing."""

    try:
        row = json.loads(line)
    except ValueError:
        return
    if not isinstance(row, dict) or not row.get("path"):
        return
    if row.get("kind") in kinds:
        receipts[str(row["path"])] = row
    elif row.get("kind") in DELETION_KINDS:
        receipts.pop(str(row["path"]), None)


def _live_receipts(path: Path, kinds: Container[str]) -> tuple[dict[str, dict[str, Any]], int]:
    """The receipts of `kinds` in `path`, up to its last whole line, whose
    files exist under its directory; and where that line ends."""

    receipts: dict[str, dict[str, Any]] = {}
    offset = 0
    with path.open("rb") as handle:
        end = os.fstat(handle.fileno()).st_size
        for line in handle:
            if offset + len(line) > end or not line.endswith(b"\n"):
                break
            offset += len(line)
            _fold_receipt(line, receipts, kinds)
    root = path.parent
    return {name: row for name, row in receipts.items() if (root / name).exists()}, offset


def read_receipts(path: Path, kinds: Iterable[str] = FILE_RECEIPT_KINDS) -> dict[str, dict[str, Any]]:
    """A `manifest.jsonl`'s file receipts by their path under the root: row
    counts, time spans and digests, for whoever needs them (`pack`, the
    coverage ledger). A file with a deletion row after its receipt is gone
    and has none."""

    rows: dict[str, dict[str, Any]] = {}
    wanted = frozenset(kinds)
    if not path.exists():
        return rows
    with path.open("rb") as handle:
        for line in handle:
            _fold_receipt(line, rows, wanted)
    return rows


def inspect_jsonl(path: Path, root: Path) -> ClosedSegment | None:
    records = 0
    first = 0
    last = 0
    day, hour, symbol = segment_identity(path, root)
    last_line: bytes | None = None
    with path.open("rb") as handle:
        for raw in handle:
            if not raw.endswith(b"\n"):
                break
            if records == 0:
                try:
                    row = json.loads(raw)
                    first = int(row.get("local_receive_ts_ns") or 0)
                except (ValueError, TypeError):
                    return None
            records += 1
            last_line = raw
    if records == 0 or last_line is None:
        return None
    try:
        last_row = json.loads(last_line)
        last = int(last_row.get("local_receive_ts_ns") or 0)
    except (ValueError, TypeError):
        return None
    return ClosedSegment(path, symbol, day, hour, records, first, last)


#: A unit's stop sends these to every process in it, and the recorder's stop
#: compresses its open segments before it exits: a zstd started with them
#: blocked (a child inherits its parent thread's signal mask) finishes the
#: segment the stop is waiting on. `ZSTD_TIMEOUT_SECONDS` still bounds it,
#: with SIGKILL.
STOP_SIGNALS = frozenset({signal.SIGTERM, signal.SIGINT})


def _outliving_stop(command: list[str], **options: Any) -> subprocess.Popen[bytes]:
    held = signal.pthread_sigmask(signal.SIG_BLOCK, STOP_SIGNALS)
    try:
        return subprocess.Popen(command, **options)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, held)


def zstd_compress(source: Path, output: Path, *, timeout: float = ZSTD_TIMEOUT_SECONDS) -> str:
    """Compress source to output atomically, verify, and return the output's SHA-256.

    The compressed bytes are hashed as zstd produces them, so the archive is
    written once and read once (by the verification), never a third time.
    """

    temporary = output.with_suffix(output.suffix + ".tmp")
    hasher = hashlib.sha256()
    try:
        with temporary.open("xb") as handle:
            process = _outliving_stop(
                ["zstd", "-q", "-3", "-T1", "-c", "--", str(source)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            assert process.stdout is not None and process.stderr is not None
            deadline = time.monotonic() + timeout
            try:
                # A blocking read would wait on a zstd that writes nothing for
                # as long as it hangs; the deadline has to bound the wait itself.
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0.0:
                            raise subprocess.TimeoutExpired(process.args, timeout)
                        if not selector.select(remaining):
                            continue
                        block = os.read(process.stdout.fileno(), 1024 * 1024)
                        if not block:
                            break
                        hasher.update(block)
                        handle.write(block)
                returncode = process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except BaseException as exc:
                # Before anything waits on its pipes: a zstd nobody reads blocks
                # on a full stdout and never closes them.
                process.kill()
                process.wait()
                process.stdout.close()
                process.stderr.close()
                if isinstance(exc, subprocess.TimeoutExpired):
                    raise RuntimeError(f"zstd compression of {source} did not finish within {timeout:g}s") from None
                raise
            said = process.stderr.read().decode(errors="replace").strip()
            process.stdout.close()
            process.stderr.close()
            if returncode != 0:
                raise RuntimeError(f"zstd compression failed for {source} (exit {returncode}): {said}")
            handle.flush()
            os.fsync(handle.fileno())
            # Durable, and nothing on this host reads it again: replay reads
            # archived files, not the pages compression just dirtied.
            discard_file_cache(handle)
        checking = _outliving_stop(
            ["zstd", "-q", "-t", "--", str(temporary)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        try:
            _, complaint = checking.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            checking.kill()
            checking.communicate()
            raise RuntimeError(f"zstd verification of {source} did not finish within {timeout:g}s") from None
        if checking.returncode != 0:
            said = complaint.decode(errors="replace").strip()
            raise RuntimeError(f"zstd verification failed for {source} (exit {checking.returncode}): {said}")
        # The verification read the archive back into the cache.
        with temporary.open("rb") as verified:
            discard_file_cache(verified)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, output)
    adopt_owner(output)
    sync_directory(output.parent)
    return hasher.hexdigest()


class Compressor:
    """Compresses closed segments on its own thread and says how it is doing.

    A segment that fails to compress is left as `.jsonl` for the next start's
    recovery, counted on `failed`, and named in `last_error`; the thread goes
    on to the next segment, because one bad file must not stop the tape. The
    recorder publishes `status()` so the watchdog sees a compressor that is
    failing or falling behind while the recorder's heartbeat is still fresh.

    The work list is bounded by `backlog_max_bytes` of raw segment, not by a
    count of segments: what a backlog costs is disk, and segments differ in
    size. Above the ceiling a closed segment is **deferred** — left as
    `.jsonl` where it is and held in a FIFO that this thread drains back into
    the queue as room appears, in submission order. Deferring rather than
    blocking is what keeps the ceiling honest: `submit` is called from the
    writer, so a blocking put would stall it until the capture queue filled
    and rows the venue is still sending were lost, whereas a deferred segment
    has already been written and fsynced and loses nothing at all. A segment larger than the
    ceiling on its own is taken when the queue is empty, so it cannot starve.
    """

    def __init__(self, root: Path, manifest: Manifest, backlog_max_bytes: int = 0) -> None:
        self.root = root
        self.manifest = manifest
        self.backlog_max_bytes = max(0, int(backlog_max_bytes))
        self.pending: queue.Queue[ClosedSegment | None] = queue.Queue()
        self.thread = threading.Thread(target=self._run, name="tape-compressor", daemon=True)
        self.error: BaseException | None = None
        self.failed = 0
        self.compressed = 0
        self.deferred_total = 0
        self.last_error: str | None = None
        self.last_error_ns = 0
        self.last_deferred_ns = 0
        self.current: ClosedSegment | None = None
        self._submitted = 0
        self._taken = 0
        self._queued_bytes = 0
        self._done_bytes = 0
        # Guards the deferred FIFO and the queued-side counters, which the
        # frame loop and the compressor thread both move.
        self._lock = threading.Lock()
        self._deferred: deque[tuple[ClosedSegment, int]] = deque()
        self._deferred_run = 0
        self._deferred_since_ns = 0
        #: Raw segments the start's walk named, which this thread reads for
        #: their receipts and queues before anything else; set once it has.
        self._recovering: list[Path] = []
        self.recovered = threading.Event()
        #: Set by `close(drain=False)`: stop after the segment in hand.
        self._halt = False

    def start(self) -> None:
        if shutil.which("zstd") is None:
            raise RuntimeError("zstd is required to record the tape")
        self._recovering = self._walk()
        self.thread.start()

    def submit(self, segment: ClosedSegment) -> bool:
        """Queue a closed segment, or defer it when the backlog is at its ceiling.

        `False` means deferred: the segment stays raw on disk and waits in the
        deferred FIFO, which the compressor thread drains as room appears.
        Never blocks, so the frame loop that calls this is never held.
        """

        try:
            size = segment.path.stat().st_size
        except OSError:
            # Gone or unreadable between closing and queueing. The worker
            # reports what it finds; the ceiling just cannot count this one.
            size = 0
        with self._lock:
            # Anything already deferred goes first, or a submission that fits
            # would overtake it.
            if not self._deferred and not self._over_ceiling(size):
                self._enqueue(segment, size)
                return True
            first = not self._deferred
            if first:
                self._deferred_run = 0
                self._deferred_since_ns = time.time_ns()
            self._deferred.append((segment, size))
            self._deferred_run += 1
            self.deferred_total += 1
            self.last_deferred_ns = time.time_ns()
        if first:
            logging.warning(
                "compression backlog is at its %.1f MiB ceiling: %s waits, with whatever follows it, "
                "until the queue drains",
                self.backlog_max_bytes / 1024 / 1024,
                segment.path,
            )
        return False

    def deferred_waiting(self) -> int:
        """Segments held out of the queue by the ceiling right now."""

        with self._lock:
            return len(self._deferred)

    def _over_ceiling(self, size: int) -> bool:
        return bool(self.backlog_max_bytes) and self.backlog_bytes() + size > self.backlog_max_bytes

    def _enqueue(self, segment: ClosedSegment, size: int) -> None:
        self._submitted += 1
        self._queued_bytes += size
        self.pending.put(segment)

    def _requeue_deferred(self) -> None:
        """Move deferred segments back into the queue while they fit under the ceiling."""

        while True:
            drained = False
            with self._lock:
                if not self._deferred:
                    return
                segment, size = self._deferred[0]
                # A segment over the ceiling on its own only ever fits when
                # nothing else is queued; refusing it there would starve it.
                if self._over_ceiling(size) and self.depth() > 0:
                    return
                self._deferred.popleft()
                self._enqueue(segment, size)
                if not self._deferred:
                    drained = True
                    count = self._deferred_run
                    waited = (time.time_ns() - self._deferred_since_ns) / 1e9
            if drained:
                logging.info(
                    "compression backlog is under its ceiling again: %d segment(s) queued after waiting %.1fs",
                    count,
                    waited,
                )

    def depth(self) -> int:
        """Segments waiting, the one being compressed included."""

        return self._submitted - self._taken

    def backlog_bytes(self) -> int:
        """Raw bytes waiting, the segment being compressed included."""

        return max(0, self._queued_bytes - self._done_bytes)

    def status(self) -> dict[str, Any]:
        return {
            "pending": self.depth() + len(self._recovering),
            "pending_bytes": self.backlog_bytes(),
            "backlog_max_bytes": self.backlog_max_bytes or None,
            "compressed": self.compressed,
            "failed": self.failed,
            "deferred": self.deferred_waiting(),
            "deferred_total": self.deferred_total,
            "last_error": self.last_error,
            "last_error_ns": self.last_error_ns or None,
            "last_deferred_ns": self.last_deferred_ns or None,
            "alive": self.thread.is_alive(),
        }

    def _walk(self) -> list[Path]:
        """Drop torn compressions, close partials, and name every raw segment.

        Called before anything writes under the root, so a partial it finds is
        a crashed run's, never this run's open segment. Reading what it names
        is `_recover`'s, on this compressor's thread: a stop leaves every open
        segment raw, and reading gigabytes of them here held the recorder's
        start."""

        temporaries: list[Path] = []
        partials: list[Path] = []
        raw: list[Path] = []
        for directory, _, names in os.walk(self.root):
            for name in names:
                path = Path(directory) / name
                if name.endswith(".zst.tmp"):
                    temporaries.append(path)
                elif name.endswith(".jsonl.partial"):
                    partials.append(path)
                elif name.startswith("segment-") and name.endswith(".jsonl"):
                    raw.append(path)
        for temporary in temporaries:
            temporary.unlink(missing_ok=True)
        for partial in partials:
            truncate_partial_line(partial)
            if partial.stat().st_size == 0:
                partial.unlink()
                continue
            final = partial.with_suffix("")
            os.replace(partial, final)
            raw.append(final)
        return sorted(set(raw))

    def _recover(self) -> None:
        """Queue every raw segment the walk named, with its receipt."""

        while self._recovering and not self._halt:
            path = self._recovering[0]
            try:
                segment = inspect_jsonl(path, self.root)
            except ValueError:
                logging.warning("leaving an unrecognised capture file alone: %s", path)
                segment = None
            except FileNotFoundError:
                # Retention or `market_tape pack` took it after the walk.
                segment = None
            else:
                if segment is None:
                    path.unlink(missing_ok=True)
            if segment is not None:
                self.submit(segment)
            self._recovering.pop(0)

    def _run(self) -> None:
        try:
            self._recover()
        except Exception:  # what recovery did not queue stays raw for the next start
            logging.exception("tape segment recovery failed")
        finally:
            self.recovered.set()
        while not self._halt:
            try:
                segment = self.pending.get(timeout=1.0)
            except queue.Empty:
                self._requeue_deferred()
                continue
            if segment is None or self._halt:
                return
            self.current = segment
            try:
                size = segment.path.stat().st_size
            except OSError:
                size = 0
            try:
                self._compress(segment)
                self.compressed += 1
            except FileNotFoundError:
                # `Retention.prune` and `market_tape pack` unlink raw segments
                # too. One taken before this thread reached it is not a
                # failure: nobody is waiting for those bytes any more.
                logging.warning("tape segment was removed before compression: %s", segment.path)
            except BaseException as exc:  # surfaced through status() and close()
                self.error = exc
                self.failed += 1
                self.last_error = f"{segment.path.relative_to(self.root)}: {exc}"
                self.last_error_ns = time.time_ns()
                logging.exception("tape segment compression failed: %s", segment.path)
            finally:
                self.current = None
                self._taken += 1
                # Off the backlog either way: a failed segment is left raw for
                # recovery, so it is no longer work this process will do.
                self._done_bytes += size
            self._requeue_deferred()

    def _compress(self, segment: ClosedSegment) -> None:
        if not segment.path.exists():
            raise FileNotFoundError(errno.ENOENT, "taken before compression", str(segment.path))
        output = segment.path.with_suffix(segment.path.suffix + ".zst")
        digest = zstd_compress(segment.path, output)
        # Retention may have taken the raw file while zstd read it; the archive
        # is verified either way and keeps its receipt. The receipt's fsync
        # carries the unlink on a journaling filesystem; an unlink a power loss
        # undoes leaves a raw segment beside its archive, which the next start
        # compresses and receipts again.
        segment.path.unlink(missing_ok=True)
        self.manifest.append(
            {
                "kind": "segment_compressed",
                "recorded_at_ns": time.time_ns(),
                "path": str(output.relative_to(self.root)),
                "symbol": segment.symbol,
                "day": segment.day,
                "hour": segment.hour,
                "records": segment.records,
                "first_receive_ns": segment.first_receive_ns,
                "last_receive_ns": segment.last_receive_ns,
                "compressed_bytes": output.stat().st_size,
                "sha256": digest,
            }
        )

    def close(self, timeout: float = COMPRESSOR_STOP_TIMEOUT_SECONDS, *, drain: bool = True) -> None:
        """Stop. Drained, the queue and the deferred FIFO go first; undrained,
        the thread stops after the segment in hand. Raw segments left behind,
        by a failure, the deadline or the stop itself, stay on disk for the
        next start's recovery or an idle root's `market_tape pack`; a failure
        or a drain cut short raises and says so."""

        deadline = time.monotonic() + timeout
        if not drain:
            self._halt = True
            self.pending.put(None)
            self.thread.join(max(0.0, deadline - time.monotonic()))
            if self.thread.is_alive():
                raise RuntimeError(f"tape compressor did not stop within {timeout:g}s")
            if self.error is not None:
                raise RuntimeError(f"{self.failed} tape segment(s) did not compress; last: {self.last_error}") from self.error
            return
        self.recovered.wait(max(0.0, deadline - time.monotonic()))
        while self.thread.is_alive() and self.deferred_waiting():
            if time.monotonic() >= deadline:
                break
            self._requeue_deferred()
            time.sleep(0.02)
        self.pending.put(None)
        self.thread.join(max(0.0, deadline - time.monotonic()))
        left = self.depth() + self.deferred_waiting() + len(self._recovering)
        if self.thread.is_alive():
            raise RuntimeError(f"tape compressor did not stop within {timeout:g}s; {left} segment(s) left raw for recovery")
        if left:
            # A deadline that passed before the start's recovery queued what
            # it found put the stop ahead of those segments.
            raise RuntimeError(f"tape compressor stopped within {timeout:g}s; {left} segment(s) left raw for recovery")
        waiting = self.deferred_waiting()
        if waiting:
            raise RuntimeError(f"tape compressor stopped with {waiting} segment(s) deferred and left raw for recovery")
        if self.error is not None:
            raise RuntimeError(f"{self.failed} tape segment(s) did not compress; last: {self.last_error}") from self.error


class Retention:
    """Every byte the tape holds has an owner here: the compressed archives and
    the closed raw segments a compressor did not take, under one age rule and
    one pressure rule."""

    def __init__(self, root: Path, manifest: Manifest, retention_days: int, max_bytes: int, min_free_bytes: int) -> None:
        self.root = root
        self.manifest = manifest
        self.retention_days = retention_days
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_bytes
        #: Bytes the last pass unlinked. A successor pass credits them: the
        #: kernel's statvfs need not show a deleted file's blocks yet.
        self.last_freed_bytes = 0
        #: Files the last pass could not stat and so could not consider.
        self.last_unstatable = 0

    def _inventory(self) -> list[tuple[Path, bool]]:
        """Every file a pass may delete, and whether it is still raw.

        The raw half is what a stopped or backed-up compressor leaves: without
        it those bytes answer to no limit at all. The writer's open segment
        (`.jsonl.partial`) and a compression in flight (`.tmp`) are somebody
        else's.
        """

        found: list[tuple[Path, bool]] = []
        pending = [self.root]
        while pending:
            directory = pending.pop()
            try:
                with os.scandir(directory) as scanned:
                    entries = list(scanned)
            except OSError:
                # `market_tape pack` removes shipped hours from its own process.
                continue
            for entry in entries:
                name = entry.name
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                if name.endswith(".zst"):
                    found.append((Path(entry.path), False))
                elif name.startswith("segment-") and name.endswith(".jsonl"):
                    found.append((Path(entry.path), True))
        return found

    def prune(self, now: float | None = None, *, free_credit: int = 0) -> list[Path]:
        """Delete what is expired, then what the disk has no room for.

        Archives and closed raw segments are one list, oldest mtime first, under
        the same two rules: a recorder that is stopped, or a compressor that
        failed, must not leave bytes no limit can reach.

        A pass walks the whole tape, so it stats each file once and reads the
        filesystem's free space once, carrying both forward as it deletes. On
        a host holding days of hours across hundreds of symbols the walk is
        tens of thousands of files: a stat or a statvfs per file per pass is
        the difference between seconds and minutes. Free space is tracked by
        the sizes unlinked rather than re-read, which is also the truer
        number — a filesystem need not release a deleted file's blocks by the
        time the next statvfs returns.

        A pass that deletes for room frees past the floor by
        `FREE_HEADROOM_FRACTION`, so the writer it unblocks has somewhere to
        write; deleting for `max_bytes` or for age stops where it always did.

        `free_credit` is what earlier passes in the same burst unlinked and the
        statvfs below has not shown yet. A retry runs precisely because those
        two numbers disagreed, so a successor that trusts the statvfs alone
        derives the whole deficit a second time and deletes it a second time,
        once per retry and with no delay between them.

        The manifest is tape bytes too: a pass ends with
        `Manifest.compact_if_grown`, on this thread and never the writer's.
        """

        now = time.time() if now is None else now
        # The floor is what `writable()` blocks on; this is what a pass frees to.
        free_target = self.min_free_bytes + int(self.min_free_bytes * FREE_HEADROOM_FRACTION)
        self.last_freed_bytes = 0
        self.last_unstatable = 0
        found: list[tuple[int, str, Path, int, float, bool]] = []
        first_unstatable: str | None = None
        for path, raw in self._inventory():
            try:
                stat = path.stat()
            except FileNotFoundError:
                # `market_tape pack` shipped it, or the compressor replaced it,
                # between the walk and the stat.
                continue
            except OSError as exc:
                self.last_unstatable += 1
                if first_unstatable is None:
                    first_unstatable = f"{path}: {exc}"
                continue
            found.append((stat.st_mtime_ns, str(path), path, stat.st_size, stat.st_mtime, raw))
        if self.last_unstatable:
            logging.warning(
                "tape retention could not stat %d file(s) and cannot retain them; first: %s",
                self.last_unstatable,
                first_unstatable,
            )
        files = sorted(found, key=lambda item: (item[0], item[1]))
        total = sum(item[3] for item in files)
        free = shutil.disk_usage(self.root).free + free_credit
        cutoff = now - self.retention_days * 86_400
        deleted: list[Path] = []
        for _, _, path, size, mtime, raw in files:
            # A venue table snapshot is the point-in-time reference for every
            # hour after it and weighs kilobytes: it goes with age, never for room.
            snapshot = path.parent.name == META_DIRECTORY
            expired = mtime < cutoff
            pressured = total > self.max_bytes or free < free_target
            if not expired and not (pressured and not snapshot):
                continue
            relative = path.relative_to(self.root)
            try:
                path.unlink()
            except FileNotFoundError:
                # `market_tape pack` deletes shipped hours from its own process;
                # a file it took between this pass's stat and this unlink is
                # not this pass's room, and it must not end the pass.
                total -= size
                continue
            total -= size
            free += size
            self.last_freed_bytes += size
            deleted.append(relative)
            receipt: dict[str, Any] = {
                "kind": "snapshot_deleted" if snapshot else "segment_deleted",
                "recorded_at_ns": time.time_ns(),
                "path": str(relative),
                "reason": "age" if expired else "disk_limit",
            }
            if raw:
                receipt["raw"] = True
                receipt["raw_bytes"] = size
            else:
                receipt["compressed_bytes"] = size
            self.manifest.append(receipt)
        if deleted:
            remove_empty_directories(self.root)
        try:
            self.manifest.compact_if_grown()
        except OSError as exc:
            # The file stays whole and a later pass rewrites it; what this
            # pass deleted must still reach the caller.
            logging.error("tape manifest rewrite failed: %s", exc)
        return deleted

    def writable(self) -> bool:
        """Is there room to keep writing: one statvfs, no filesystem walk.

        The recorder asks this on the tick that writes its heartbeat, so this
        must stay O(1). `prune` is the housekeeping and runs on its own thread.
        """

        return shutil.disk_usage(self.root).free >= self.min_free_bytes


class Snapshots:
    """The venue's instrument and ticker tables, written as of one moment, at a cadence."""

    def __init__(self, root: Path, manifest: Manifest, *, venue: str, market: str, source: str, cadence: str) -> None:
        self.root = root
        self.manifest = manifest
        self.venue = venue
        self.market = market
        self.source = source
        self.cadence = cadence
        self.last_key: tuple[str, ...] | None = None
        self.last_ns = 0

    def _key(self, now_ns: int) -> tuple[str, ...]:
        day, hour = utc_day_hour(now_ns)
        return (day, hour) if self.cadence == "hour" else (day,)

    def due(self, now_ns: int) -> bool:
        return self.last_key != self._key(now_ns)

    def write(self, now_ns: int, tables: Mapping[str, list[dict[str, Any]]]) -> None:
        day, hour = utc_day_hour(now_ns)
        stamp = datetime.fromtimestamp(now_ns / 1_000_000_000, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        directory = self.root / day / hour / META_DIRECTORY
        directory.mkdir(parents=True, exist_ok=True)
        for name, kind in (("instruments", SNAPSHOT_INSTRUMENTS), ("tickers", SNAPSHOT_TICKERS)):
            rows = list(tables.get(name) or [])
            raw = directory / f"{name}-{stamp}.json"
            output = directory / f"{name}-{stamp}.json.zst"
            payload = snapshot_payload(
                kind=kind,
                venue=self.venue,
                market=self.market,
                recorded_at_ns=now_ns,
                source=self.source,
                rows=rows,
            )
            digest = write_compressed_json(raw, output, payload)
            self.manifest.append(
                {
                    "kind": "snapshot_compressed",
                    "recorded_at_ns": time.time_ns(),
                    "path": str(output.relative_to(self.root)),
                    "snapshot": name,
                    "day": day,
                    "hour": hour,
                    "rows": len(rows),
                    "compressed_bytes": output.stat().st_size,
                    "sha256": digest,
                }
            )
        self.last_key = self._key(now_ns)
        self.last_ns = now_ns


class CoverageRecords:
    """The recorder's per-hour coverage record, written where the hourly tar takes it.

    Raw, compressed, unlinked, receipted — the same order as `Snapshots`,
    because an hour holding one non-`.zst` file never packs.
    """

    def __init__(self, root: Path, manifest: Manifest) -> None:
        self.root = root
        self.manifest = manifest

    def write(self, payload: Mapping[str, Any]) -> Path:
        day = str(payload["day"])
        hour = str(payload["hour"])
        window = payload["window"]
        stamp = datetime.fromtimestamp(int(window["from_ns"]) / 1_000_000_000, tz=timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        directory = self.root / day / hour / META_DIRECTORY
        directory.mkdir(parents=True, exist_ok=True)
        raw = directory / f"coverage-{hour}-{stamp}.json"
        output = directory / f"{raw.name}.zst"
        digest = write_compressed_json(raw, output, payload)
        self.manifest.append(
            {
                "kind": "coverage_compressed",
                "recorded_at_ns": time.time_ns(),
                "path": str(output.relative_to(self.root)),
                "day": day,
                "hour": hour,
                "from_ns": int(window["from_ns"]),
                "to_ns": int(window["to_ns"]),
                "compressed_bytes": output.stat().st_size,
                "sha256": digest,
            }
        )
        return output


def write_compressed_json(raw: Path, output: Path, payload: Mapping[str, Any]) -> str:
    """Write `payload` as `raw`, compress it to `output`, drop `raw`; the archive's SHA-256.

    A failure leaves no `raw` behind: one non-`.zst` file under an hour keeps
    the whole hour from packing, and nothing else ever removes it.
    """

    try:
        with raw.open("xb") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(raw, 0o640)
        return zstd_compress(raw, output)
    finally:
        raw.unlink(missing_ok=True)


def truncate_partial_line(path: Path) -> None:
    """Cut `path` back to its last whole line, reading back from its end: a
    crashed writer's partials are the recorder's whole open hour, and the
    start waits on this."""

    with path.open("rb+") as handle:
        position = handle.seek(0, os.SEEK_END)
        end = 0
        while position > 0:
            start = max(0, position - 65536)
            handle.seek(start)
            newline = handle.read(position - start).rfind(b"\n")
            if newline >= 0:
                end = start + newline + 1
                break
            position = start
        handle.truncate(end)
        handle.flush()
        os.fsync(handle.fileno())


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish `payload` at `path` in one rename; a second writer can never share the temporary.

    Never flushed: this is the recorder's heartbeat, rewritten every status
    interval, and a reader needs a whole file, which the rename gives. An
    fsync here waits behind every dirty page the tape writer has on the
    filesystem, and a heartbeat that waits on the writer's disk is not one.
    """

    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_empty_directories(root: Path) -> None:
    """Drop the empty hour and symbol directories a prune leaves; a directory
    that is not empty or is already gone is the expected case and says nothing."""

    failures = 0
    first: str | None = None
    for directory, _, _ in os.walk(root, topdown=False):
        path = Path(directory)
        if path == root:
            continue
        try:
            path.rmdir()
        except OSError as exc:
            if exc.errno in (errno.ENOTEMPTY, errno.ENOENT, errno.EEXIST):
                continue
            failures += 1
            if first is None:
                first = f"{path}: {exc}"
    if failures:
        logging.warning("could not remove %d empty tape directory(ies); first: %s", failures, first)
