"""Files on the recording host: segments, receipts, compression, retention, snapshots."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import io
import json
import logging
import mmap
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from market_tape.schema import SCHEMA_VERSION
from market_tape import storage
from market_tape.storage import (
    Compressor,
    Manifest,
    Retention,
    SegmentWriter,
    Snapshots,
    atomic_json,
    segment_identity,
    utc_day_hour,
    zstd_compress,
)

HOUR_10 = 1_788_256_800_000_000_000  # 2026-09-01T10:00:00Z
HOUR = 3_600_000_000_000


def trade(received_ns: int, symbol: str = "AGIUSDT") -> dict[str, object]:
    return {"kind": "public_trade", "symbol": symbol, "local_receive_ts_ns": received_ns}


def test_a_row_behind_the_one_before_it_is_stamped_at_that_instant_on_both_clocks(tmp_path: Path) -> None:
    """A segment TCP recovered behind the one that overtook it, or a lane row
    queued behind stream frames, reaches the writer stamped earlier than the
    row before it. The sort key is per segment: the row takes the instant it
    could first have been read, and its monotonic stamp moves by the same
    shift, so the two clocks keep their offset."""

    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    writer.append({**trade(HOUR_10 + 1_000_000), "local_receive_mono_ns": 5_000_000})
    writer.append({**trade(HOUR_10 + 662_311), "local_receive_mono_ns": 4_662_311})  # 337,689 ns back on both
    writer.append({**trade(HOUR_10 + 800_000), "local_receive_mono_ns": 4_800_000})
    writer.append(trade(HOUR_10 + 2_000_000))
    (closed,) = writer.append(trade(HOUR_10 + HOUR))

    rows = [json.loads(line) for line in closed.path.read_text().splitlines()]
    assert [(row["local_receive_ts_ns"], row.get("local_receive_mono_ns")) for row in rows] == [
        (HOUR_10 + 1_000_000, 5_000_000),
        (HOUR_10 + 1_000_000, 5_000_000),
        (HOUR_10 + 1_000_000, 5_000_000),
        (HOUR_10 + 2_000_000, None),
    ]
    assert (closed.first_receive_ns, closed.last_receive_ns, closed.records) == (HOUR_10 + 1_000_000, HOUR_10 + 2_000_000, 4)


def test_segments_roll_on_the_hour_and_idle_hours_close(tmp_path: Path) -> None:
    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    assert utc_day_hour(HOUR_10) == ("2026-09-01", "10")

    assert writer.append(trade(HOUR_10 + 5)) == []
    assert writer.append(trade(HOUR_10 + 3_599_000_000_000)) == []
    closed = writer.append(trade(HOUR_10 + HOUR))

    assert [(segment.day, segment.hour, segment.records) for segment in closed] == [("2026-09-01", "10", 2)]
    assert closed[0].path == tmp_path / "2026-09-01" / "10" / "AGIUSDT" / "segment-000000.jsonl"
    assert closed[0].first_receive_ns == HOUR_10 + 5
    assert closed[0].last_receive_ns == HOUR_10 + 3_599_000_000_000
    # A quiet symbol's open hour closes when the clock passes it, without a new row.
    assert writer.roll_idle(HOUR_10 + HOUR + 1) == []
    idle = writer.roll_idle(HOUR_10 + 2 * HOUR)
    assert [(segment.hour, segment.records) for segment in idle] == [("11", 1)]
    assert writer.active == {}


def test_a_row_in_the_last_microsecond_of_an_hour_is_filed_in_that_hour(tmp_path: Path) -> None:
    """The hour is the stamp's integer hour, as the coverage record cuts it. The
    label is cached per hour, so a wrong one for the hour's last microsecond
    would also be every later row's of that hour."""

    midnight = HOUR_10 + 14 * HOUR
    assert utc_day_hour(HOUR_10 + HOUR) == ("2026-09-01", "11")
    assert utc_day_hour(HOUR_10 + HOUR - 1) == ("2026-09-01", "10")
    assert utc_day_hour(HOUR_10 + HOUR // 2) == ("2026-09-01", "10")
    assert utc_day_hour(midnight - 1) == ("2026-09-01", "23")
    assert utc_day_hour(midnight) == ("2026-09-02", "00")
    assert utc_day_hour(-1) == ("1969-12-31", "23")

    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    utc_day_hour(HOUR_10 + HOUR)
    assert writer.append(trade(HOUR_10 + HOUR - 1)) == []
    assert writer.append(trade(HOUR_10 + HOUR - 1_000_000)) == []
    closed = writer.close()
    assert [(segment.day, segment.hour, segment.records) for segment in closed] == [("2026-09-01", "10", 2)]


def test_a_segment_rolls_at_the_size_cap_and_numbers_the_next_one(tmp_path: Path) -> None:
    writer = SegmentWriter(tmp_path, max_bytes=80, fsync_every=1)
    row = trade(HOUR_10 + 1)
    assert writer.append(row) == []
    closed = writer.append(trade(HOUR_10 + 2))

    assert [segment.path.name for segment in closed] == ["segment-000000.jsonl"]
    assert writer.active["AGIUSDT"].path.name == "segment-000001.jsonl.partial"
    assert writer.close()[0].records == 1


def cached_pages(path: Path) -> int:
    """Pages of `path` in the page cache: mincore over a shared read-only map, which faults none in."""

    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    size = path.stat().st_size
    vector = (ctypes.c_ubyte * -(-size // mmap.PAGESIZE))()
    with path.open("rb") as handle:
        address = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, handle.fileno(), 0)
        if address == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_errno(), "mmap")
        try:
            if libc.mincore(address, size, vector) != 0:
                raise OSError(ctypes.get_errno(), "mincore")
        finally:
            libc.munmap(address, size)
    return sum(byte & 1 for byte in vector)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="posix_fadvise and mincore are Linux's")
def test_an_open_segments_written_pages_leave_the_cache_before_it_closes(tmp_path: Path) -> None:
    # Below the fsync cadence and the size cap, as most of an hour's segments
    # are: nothing but `release_cache` drops what they wrote.
    writer = SegmentWriter(tmp_path, max_bytes=64 * 1024 * 1024, fsync_every=1_000_000)
    for index in range(2_000):
        writer.append({**trade(HOUR_10 + index), "pad": "x" * 4_000})
    segment = writer.active["AGIUSDT"]
    assert cached_pages(segment.path) > 1_000

    # The first call starts the dirty pages' writeback; a later one drops them.
    deadline = time.monotonic() + 10.0
    while True:
        writer.release_cache()
        left = cached_pages(segment.path)
        if left <= 1 or time.monotonic() > deadline:
            break
        time.sleep(0.1)
    assert left <= 1, "only the partial page appends still land in stays"

    writer.append(trade(HOUR_10 + 2_000))
    closed = writer.close()
    stamps = [json.loads(line)["local_receive_ts_ns"] for line in closed[0].path.read_bytes().splitlines()]
    assert stamps == [HOUR_10 + index for index in range(2_001)]


def test_a_row_needs_a_symbol_and_a_receive_clock(tmp_path: Path) -> None:
    writer = SegmentWriter(tmp_path, max_bytes=1024, fsync_every=1)
    with pytest.raises(ValueError, match="receive timestamp"):
        writer.append({"kind": "public_trade", "symbol": "AGIUSDT"})
    with pytest.raises(ValueError, match="no symbol"):
        writer.append({"kind": "public_trade", "local_receive_ts_ns": HOUR_10})


def test_segment_identity_reads_the_day_hour_and_symbol_of_a_path(tmp_path: Path) -> None:
    segment = tmp_path / "2026-09-01" / "10" / "agiusdt" / "segment-000000.jsonl.zst"

    assert segment_identity(segment, tmp_path) == ("2026-09-01", "10", "AGIUSDT")
    with pytest.raises(ValueError, match="not a capture segment"):
        segment_identity(tmp_path / "manifest.jsonl", tmp_path)


def burst(tmp_path: Path, symbols: tuple[str, ...] = ("AAAUSDT", "BBBUSDT", "CCCUSDT")) -> list[storage.ClosedSegment]:
    """One closed segment per symbol, all the same size: the names are equally long."""

    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    for symbol in symbols:
        writer.append(trade(1_800_000_000_000_000_000, symbol))
    return writer.close()


def test_a_backlog_at_its_ceiling_keeps_the_segment_out_of_the_queue(tmp_path: Path) -> None:
    # A ceiling of one byte: every closed segment is over it.
    manifest = Manifest(tmp_path)
    compressor = Compressor(tmp_path, manifest, backlog_max_bytes=1)
    writer = SegmentWriter(tmp_path, max_bytes=1024, fsync_every=1)
    for _ in range(3):
        writer.append(trade(1_800_000_000_000_000_000))
    closed = writer.close()
    assert closed, "the writer closed a segment to defer"

    assert [compressor.submit(segment) for segment in closed] == [False] * len(closed)
    assert compressor.depth() == 0, "a deferred segment is not queued"
    assert compressor.backlog_bytes() == 0
    status = compressor.status()
    assert status["deferred"] == len(closed) and status["deferred_total"] == len(closed)
    assert status["last_deferred_ns"] > 0

    # Nothing is lost while it waits: the rows are on disk as raw segments.
    raw = list(tmp_path.rglob("segment-*.jsonl"))
    assert len(raw) == len(closed)
    assert sum(len(path.read_bytes().splitlines()) for path in raw) == 3


def test_a_deferred_burst_is_drained_by_the_running_compressor(tmp_path: Path) -> None:
    # The ceiling holds one segment, the burst is three: the second and third
    # are deferred, and this process compresses them.
    manifest = Manifest(tmp_path)
    closed = burst(tmp_path)
    assert len(closed) == 3
    sizes = {segment.path.stat().st_size for segment in closed}
    assert len(sizes) == 1, "the burst's segments are the same size"
    compressor = Compressor(tmp_path, manifest, backlog_max_bytes=sizes.pop())

    assert [compressor.submit(segment) for segment in closed] == [True, False, False]
    compressor.thread.start()
    compressor.close()

    assert not list(tmp_path.rglob("segment-*.jsonl"))
    assert len(list(tmp_path.rglob("segment-*.jsonl.zst"))) == 3
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [receipt["kind"] for receipt in receipts] == ["segment_compressed"] * 3
    # Submission order survives the ceiling.
    assert [receipt["symbol"] for receipt in receipts] == ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    status = compressor.status()
    assert status["deferred"] == 0 and status["deferred_total"] == 2
    assert status["compressed"] == 3 and status["failed"] == 0


def test_a_segment_larger_than_the_ceiling_alone_is_still_compressed(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    compressor = Compressor(tmp_path, manifest, backlog_max_bytes=1)
    closed = burst(tmp_path, symbols=("AAAUSDT",))

    assert compressor.submit(closed[0]) is False
    compressor.thread.start()
    compressor.close()

    assert not list(tmp_path.rglob("segment-*.jsonl"))
    assert len(list(tmp_path.rglob("segment-*.jsonl.zst"))) == 1
    assert compressor.status()["deferred"] == 0 and compressor.status()["deferred_total"] == 1


def test_recovery_compresses_a_raw_pile_larger_than_the_ceiling_on_one_start(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    closed = burst(tmp_path)
    assert len(list(tmp_path.rglob("segment-*.jsonl"))) == len(closed)

    compressor = Compressor(tmp_path, manifest, backlog_max_bytes=1)
    compressor.start()
    compressor.close()

    assert not list(tmp_path.rglob("segment-*.jsonl"))
    assert len(list(tmp_path.rglob("segment-*.jsonl.zst"))) == len(closed)
    assert compressor.status()["compressed"] == len(closed)


def test_a_start_reads_the_raw_pile_on_the_compressors_thread_not_the_callers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stop leaves every open segment raw, and the recorder starts writing
    as soon as `start` returns: reading gigabytes of raw segments for their
    receipts there held the start as long as compressing them held the stop."""

    manifest = Manifest(tmp_path)
    closed = burst(tmp_path)
    readers: list[str] = []
    inspected = storage.inspect_jsonl

    def inspect(path: Path, root: Path) -> Any:
        readers.append(threading.current_thread().name)
        return inspected(path, root)

    monkeypatch.setattr(storage, "inspect_jsonl", inspect)
    compressor = Compressor(tmp_path, manifest)
    compressor.start()
    compressor.close()

    assert readers and set(readers) == {"tape-compressor"}
    assert len(list(tmp_path.rglob("segment-*.jsonl.zst"))) == len(closed)
    assert not list(tmp_path.rglob("segment-*.jsonl"))


def test_an_undrained_close_stops_after_the_segment_in_hand_and_the_next_start_takes_the_rest(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path)
    compressor = Compressor(tmp_path, manifest)
    closed = burst(tmp_path)
    for segment in closed:
        compressor.submit(segment)
    compressor.start()
    compressor.close(drain=False)

    done = len(list(tmp_path.rglob("segment-*.jsonl.zst")))
    assert done < len(closed), "the undrained close compressed the whole queue"
    assert done + len(list(tmp_path.rglob("segment-*.jsonl"))) == len(closed)

    successor = Compressor(tmp_path, manifest)
    successor.start()
    successor.close()
    assert len(list(tmp_path.rglob("segment-*.jsonl.zst"))) == len(closed)
    assert not list(tmp_path.rglob("segment-*.jsonl"))


def test_closed_segment_is_verified_before_raw_bytes_are_removed(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    compressor = Compressor(tmp_path, manifest)
    compressor.start()
    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    for _ in range(3):
        assert writer.append(trade(1_800_000_000_000_000_000)) == []
    for segment in writer.close():
        compressor.submit(segment)
    compressor.close()

    compressed = list(tmp_path.rglob("segment-*.jsonl.zst"))
    assert len(compressed) == 1
    assert not list(tmp_path.rglob("segment-*.jsonl"))
    assert subprocess.run(["zstd", "-q", "-t", str(compressed[0])], check=False).returncode == 0
    receipt = json.loads((tmp_path / "manifest.jsonl").read_text(encoding="utf-8"))
    assert receipt["kind"] == "segment_compressed"
    assert receipt["records"] == 3
    assert receipt["symbol"] == "AGIUSDT"
    # Hashed as zstd produced it, and it is the file's digest.
    assert receipt["sha256"] == hashlib.sha256(compressed[0].read_bytes()).hexdigest()
    assert compressor.status() == {
        "pending": 0,
        "pending_bytes": 0,
        "backlog_max_bytes": None,
        "compressed": 1,
        "failed": 0,
        "deferred": 0,
        "deferred_total": 0,
        "last_error": None,
        "last_error_ns": None,
        "last_deferred_ns": None,
        "alive": False,
    }


def _fake_zstd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    binary = tmp_path / "bin" / "zstd"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("#!/bin/sh\n" + body + "\n", encoding="utf-8")
    binary.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binary.parent}{os.pathsep}{os.environ['PATH']}")


def test_a_zstd_that_hangs_is_killed_and_leaves_no_temporary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_zstd(tmp_path, monkeypatch, "sleep 30")
    source = tmp_path / "segment-000000.jsonl"
    source.write_bytes(b'{"a":1}\n')
    output = source.with_suffix(".jsonl.zst")
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not finish within 0.2s"):
        zstd_compress(source, output, timeout=0.2)
    assert time.monotonic() - started < 10.0, "the deadline waited out a zstd that wrote nothing"
    assert not output.exists()
    assert not output.with_suffix(".zst.tmp").exists()
    assert source.exists(), "the raw segment is kept for the next attempt"


def test_a_zstd_the_units_stop_signals_finishes_the_segment_the_stop_waits_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A unit's stop sends SIGTERM to every process in it, and the recorder's
    stop compresses its open segments before it exits. A zstd that died of
    that signal left its segment raw and failed the recorder's exit; one
    started now finishes."""

    source = tmp_path / "segment-000000.jsonl"
    source.write_bytes(b"".join(b'{"n":%d,"pad":"%s"}\n' % (n, b"x" * (n % 97)) for n in range(20_000)))
    output = source.with_suffix(".jsonl.zst")
    started: list[Any] = []
    popen = subprocess.Popen

    def signalled(*args: Any, **kwargs: Any) -> Any:
        process = popen(*args, **kwargs)
        if args and list(args[0])[:1] == ["zstd"]:
            # The unit's stop, as the process starts.
            process.send_signal(signal.SIGTERM)
            started.append(process)
        return process

    monkeypatch.setattr(storage.subprocess, "Popen", signalled)
    digest = zstd_compress(source, output)
    monkeypatch.undo()

    assert len(started) == 2, "the compression and its verification were both signalled"
    assert hashlib.sha256(output.read_bytes()).hexdigest() == digest
    restored = subprocess.run(["zstd", "-dcq", str(output)], check=True, capture_output=True).stdout
    assert restored == source.read_bytes()


def test_a_zstd_that_fails_says_what_it_said(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_zstd(tmp_path, monkeypatch, 'echo "disk on fire" >&2; exit 7')
    source = tmp_path / "segment-000000.jsonl"
    source.write_bytes(b'{"a":1}\n')
    with pytest.raises(RuntimeError, match=r"compression failed .* \(exit 7\): disk on fire"):
        zstd_compress(source, source.with_suffix(".jsonl.zst"))
    assert not source.with_suffix(".jsonl.zst.tmp").exists()


def test_a_segment_that_will_not_compress_is_counted_and_the_next_one_still_ships(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # zstd refuses this one symbol and works on the other.
    _fake_zstd(
        tmp_path,
        monkeypatch,
        f'case "$*" in *AAAUSDT*) echo "disk on fire" >&2; exit 7 ;; esac\nexec {shutil.which("zstd")} "$@"',
    )
    manifest = Manifest(tmp_path)
    compressor = Compressor(tmp_path, manifest)
    compressor.start()
    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=1)
    writer.append(trade(1_800_000_000_000_000_000, "AAAUSDT"))
    writer.append(trade(1_800_000_000_000_000_000, "BBBUSDT"))
    closed = {segment.symbol: segment for segment in writer.close()}
    compressor.submit(closed["AAAUSDT"])
    compressor.submit(closed["BBBUSDT"])
    with pytest.raises(RuntimeError, match=r"1 tape segment\(s\) did not compress; last: .*AAAUSDT/segment-000000.jsonl"):
        compressor.close()

    status = compressor.status()
    assert status["failed"] == 1 and status["compressed"] == 1 and status["pending"] == 0
    assert "AAAUSDT/segment-000000.jsonl" in status["last_error"]
    assert status["last_error_ns"] is not None
    assert (closed["BBBUSDT"].path.with_suffix(".jsonl.zst")).exists(), "the failure did not stop the queue"


def test_atomic_json_never_shares_a_temporary_and_leaves_none_behind(tmp_path: Path) -> None:
    import threading

    path = tmp_path / "status.json"
    seen: list[str] = []
    real_mkstemp = storage.tempfile.mkstemp

    def recorded(*args: Any, **kwargs: Any) -> tuple[int, str]:
        descriptor, name = real_mkstemp(*args, **kwargs)
        seen.append(name)
        return descriptor, name

    storage.tempfile.mkstemp = recorded  # type: ignore[assignment]
    try:
        workers = [threading.Thread(target=lambda i=i: [atomic_json(path, {"n": i, "k": j}) for j in range(50)]) for i in range(4)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
    finally:
        storage.tempfile.mkstemp = real_mkstemp  # type: ignore[assignment]
    assert len(seen) == 200 and len(set(seen)) == 200, "every write had its own temporary"
    assert json.loads(path.read_text(encoding="utf-8"))["k"] == 49
    assert [p.name for p in tmp_path.iterdir()] == ["status.json"]
    assert oct(path.stat().st_mode & 0o777) == "0o644"


def test_retention_names_the_files_it_could_not_stat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    manifest = Manifest(tmp_path)
    directory = tmp_path / "2026-08-01" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    good = directory / "segment-000000.jsonl.zst"
    bad = directory / "segment-000001.jsonl.zst"
    good.write_bytes(b"x" * 10)
    bad.write_bytes(b"x" * 10)
    old = time.time() - 400 * 86_400
    os.utime(good, (old, old))
    os.utime(bad, (old, old))
    real_stat = Path.stat

    def stat(self: Path, *args: Any, **kwargs: Any) -> os.stat_result:
        if self == bad:
            raise PermissionError(13, "Permission denied")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)
    retention = Retention(tmp_path, manifest, retention_days=30, max_bytes=10**12, min_free_bytes=1)
    with caplog.at_level("WARNING"):
        deleted = retention.prune()
    assert deleted == [good.relative_to(tmp_path)]
    assert retention.last_unstatable == 1
    assert any("could not stat 1 file(s)" in record.getMessage() and str(bad) in record.getMessage() for record in caplog.records)


def test_restart_keeps_only_complete_json_lines_and_receipts_the_segment_in_place(tmp_path: Path) -> None:
    directory = tmp_path / "2027-01-15" / "13" / "AGIUSDT"
    directory.mkdir(parents=True)
    partial = directory / "segment-000002.jsonl.partial"
    complete = trade(1_800_000_000_000_000_000)
    partial.write_bytes(json.dumps(complete).encode() + b"\n" + b'{"kind":"torn"')

    compressor = Compressor(tmp_path, Manifest(tmp_path))
    compressor.start()
    compressor.close()

    decoded = subprocess.run(
        ["zstd", "-dcq", str(directory / "segment-000002.jsonl.zst")],
        check=True,
        capture_output=True,
    ).stdout
    assert decoded == json.dumps(complete).encode() + b"\n"
    assert not partial.exists()
    receipt = json.loads((tmp_path / "manifest.jsonl").read_text(encoding="utf-8"))
    assert receipt["day"] == "2027-01-15" and receipt["hour"] == "13" and receipt["symbol"] == "AGIUSDT"
    assert receipt["records"] == 1


def test_restart_drops_an_empty_partial_and_leaves_stray_temporaries_nowhere(tmp_path: Path) -> None:
    directory = tmp_path / "2027-01-15" / "13" / "AGIUSDT"
    directory.mkdir(parents=True)
    empty = directory / "segment-000000.jsonl.partial"
    empty.write_bytes(b'{"torn":')
    stray = directory / "segment-000001.jsonl.zst.tmp"
    stray.write_bytes(b"half a compression")

    compressor = Compressor(tmp_path, Manifest(tmp_path))
    compressor.start()
    compressor.close()

    assert not empty.exists()
    assert not stray.exists()
    assert not (tmp_path / "manifest.jsonl").exists()


def test_a_source_another_cleanup_took_is_not_a_compression_failure(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Retention and `market_tape pack` unlink raw segments too. One gone before
    the compressor reached it is nobody's failure."""

    compressor = Compressor(tmp_path, Manifest(tmp_path))
    [segment] = burst(tmp_path, ("AAAUSDT",))
    compressor.submit(segment)
    segment.path.unlink()

    compressor.start()
    with caplog.at_level("WARNING"):
        compressor.close()

    assert compressor.failed == 0
    assert compressor.compressed == 0
    assert compressor.error is None
    assert compressor.last_error is None
    assert not (tmp_path / "manifest.jsonl").exists()
    assert [record.getMessage() for record in caplog.records if str(segment.path) in record.getMessage()]


def test_a_source_taken_while_zstd_read_it_still_gets_its_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The archive was written and verified before the raw file went; the
    receipt is what `pack` reads the row count and time span from."""

    real = storage.zstd_compress

    def take_source_after(source: Path, output: Path, **kwargs: object) -> str:
        digest = real(source, output, **kwargs)  # type: ignore[arg-type]
        source.unlink()
        return digest

    monkeypatch.setattr(storage, "zstd_compress", take_source_after)
    compressor = Compressor(tmp_path, Manifest(tmp_path))
    [segment] = burst(tmp_path, ("AAAUSDT",))
    compressor.submit(segment)
    compressor.start()
    compressor.close()

    assert compressor.compressed == 1
    assert compressor.failed == 0
    assert segment.path.with_suffix(".jsonl.zst").exists()
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["kind"] for row in receipts] == ["segment_compressed"]
    assert receipts[0]["records"] == segment.records


def test_retention_deletes_oldest_complete_segments_and_receipts_it(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    old = directory / "segment-000000.jsonl.zst"
    newer = directory / "segment-000001.jsonl.zst"
    partial = directory / "segment-000002.jsonl.partial"
    old.write_bytes(b"old")
    newer.write_bytes(b"newer")
    partial.write_bytes(b"still open")
    now = time.time()
    os.utime(old, (now - 40 * 86_400, now - 40 * 86_400))
    os.utime(newer, (now, now))

    retention = Retention(tmp_path, manifest, retention_days=30, max_bytes=1024, min_free_bytes=1)
    deleted = retention.prune(now)

    assert deleted == [old.relative_to(tmp_path)]
    assert not old.exists()
    assert newer.exists()
    assert partial.exists()
    receipt = json.loads((tmp_path / "manifest.jsonl").read_text(encoding="utf-8"))
    assert receipt["kind"] == "segment_deleted"
    assert receipt["reason"] == "age"


def test_retention_deletes_closed_raw_segments_and_leaves_the_open_one(tmp_path: Path) -> None:
    """A stopped recorder's raw segments are bytes like any other; the writer's
    `.jsonl.partial` is not this pass's to take."""

    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    raw = directory / "segment-000000.jsonl"
    partial = directory / "segment-000001.jsonl.partial"
    raw.write_bytes(b"raw rows\n")
    partial.write_bytes(b"still open\n")
    now = time.time()
    for path in (raw, partial):
        os.utime(path, (now - 40 * 86_400, now - 40 * 86_400))

    retention = Retention(tmp_path, manifest, retention_days=30, max_bytes=10**12, min_free_bytes=1)
    deleted = retention.prune(now)

    assert deleted == [raw.relative_to(tmp_path)]
    assert partial.exists()
    receipt = json.loads((tmp_path / "manifest.jsonl").read_text(encoding="utf-8"))
    assert receipt["kind"] == "segment_deleted"
    assert receipt["reason"] == "age"
    assert receipt["raw"] is True
    assert receipt["raw_bytes"] == len(b"raw rows\n")
    assert "compressed_bytes" not in receipt


def test_disk_pressure_takes_the_oldest_file_raw_or_compressed(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    raw = directory / "segment-000000.jsonl"
    archive = directory / "segment-000001.jsonl.zst"
    raw.write_bytes(b"raw-bytes")
    archive.write_bytes(b"archived")
    os.utime(raw, (1_000_000, 1_000_000))
    os.utime(archive, (1_000_001, 1_000_001))

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=10, min_free_bytes=1)
    assert retention.prune(1_000_100.0) == [raw.relative_to(tmp_path)]
    assert archive.exists()

    # The same two files the other way round: the archive is now the oldest.
    later = directory / "segment-000002.jsonl"
    later.write_bytes(b"raw-two")
    os.utime(archive, (1_000_000, 1_000_000))
    os.utime(later, (1_000_002, 1_000_002))

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=7, min_free_bytes=1)
    assert retention.prune(1_000_100.0) == [archive.relative_to(tmp_path)]
    assert later.exists()
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [(receipt.get("raw"), receipt["reason"]) for receipt in receipts] == [(True, "disk_limit"), (None, "disk_limit")]
    assert receipts[1]["compressed_bytes"] == len(b"archived")


def test_disk_pressure_spares_the_venue_table_snapshots_and_age_names_them(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    hour = tmp_path / "2027-01-15" / "10"
    (hour / "AGIUSDT").mkdir(parents=True)
    (hour / "_meta").mkdir()
    snapshot = hour / "_meta" / "instruments-20270115T100000Z.json.zst"
    segment = hour / "AGIUSDT" / "segment-000000.jsonl.zst"
    snapshot.write_bytes(b"tables")
    segment.write_bytes(b"segment")
    # The snapshot is the older file; pressure would take it first by age.
    os.utime(snapshot, (1_000_000, 1_000_000))
    os.utime(segment, (1_000_001, 1_000_001))

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=8, min_free_bytes=1)
    deleted = retention.prune(1_000_100.0)
    assert deleted == [segment.relative_to(tmp_path)]
    assert snapshot.exists()

    aged = Retention(tmp_path, manifest, retention_days=1, max_bytes=10**12, min_free_bytes=1)
    deleted = aged.prune(1_000_000.0 + 2 * 86_400)
    assert deleted == [snapshot.relative_to(tmp_path)]
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [(receipt["kind"], receipt["reason"]) for receipt in receipts] == [("segment_deleted", "disk_limit"), ("snapshot_deleted", "age")]


def test_writable_asks_the_free_space_question_and_walks_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`writable()` is read on the tick that writes the recorder's heartbeat,
    so it may not walk the tape or delete anything on the way."""

    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    expired = directory / "segment-000000.jsonl.zst"
    expired.write_bytes(b"old")
    os.utime(expired, (1_000_000, 1_000_000))
    walked = 0
    original = Path.rglob

    def counted(self: Path, pattern: str) -> Any:
        nonlocal walked
        walked += 1
        return original(self, pattern)

    monkeypatch.setattr(Path, "rglob", counted)

    retention = Retention(tmp_path, manifest, retention_days=1, max_bytes=10**12, min_free_bytes=1)
    assert retention.writable() is True

    assert walked == 0
    assert expired.exists()
    assert not (tmp_path / "manifest.jsonl").exists()


def test_disk_pressure_stops_once_the_unlinked_bytes_clear_the_free_floor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Free space is carried forward by what was unlinked, so a pass under the
    free floor deletes what it needs and stops — it does not empty the tape."""

    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    for index in range(4):
        path = directory / f"segment-{index:06d}.jsonl.zst"
        path.write_bytes(b"x" * 100)
        os.utime(path, (1_000_000 + index, 1_000_000 + index))
    monkeypatch.setattr(
        "market_tape.storage.shutil.disk_usage",
        lambda path: SimpleNamespace(total=1_000, used=150, free=850),
    )

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=10**12, min_free_bytes=1_000)
    deleted = retention.prune(1_000_100.0)

    assert [path.name for path in deleted] == ["segment-000000.jsonl.zst", "segment-000001.jsonl.zst"]
    assert (directory / "segment-000002.jsonl.zst").exists()


def test_disk_pressure_leaves_the_writer_room_above_the_floor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A pass that stops exactly on `min_free_bytes` unblocks the writer onto
    no room at all: `writable()` returns True, the next segments cross the
    floor again, and the recorder blocks for another interval with every frame
    in between discarded. The pass must free past the floor."""

    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    for index in range(20):
        path = directory / f"segment-{index:06d}.jsonl.zst"
        path.write_bytes(b"x" * 50)
        os.utime(path, (1_000_000 + index, 1_000_000 + index))

    # Free space is what the tape does not hold: deleting a file returns its
    # bytes, writing one takes them, exactly as the filesystem behaves.
    def usage(path: Any) -> Any:
        held = sum(item.stat().st_size for item in tmp_path.rglob("*.zst"))
        return SimpleNamespace(total=3_000, used=1_100 + held, free=1_900 - held)

    monkeypatch.setattr("market_tape.storage.shutil.disk_usage", usage)

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=10**12, min_free_bytes=1_000)
    assert retention.writable() is False

    deleted = retention.prune(1_000_100.0)

    assert retention.writable() is True
    # One more rolled segment must not put the recorder back under the floor.
    (directory / "segment-000099.jsonl.zst").write_bytes(b"x" * 50)
    assert retention.writable() is True
    assert len(deleted) == 3


def test_snapshots_write_the_venue_tables_with_their_own_payload(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    snapshots = Snapshots(
        tmp_path,
        manifest,
        venue="bybit",
        market="linear",
        source="https://api.bybit.com",
        cadence="day",
    )
    tables = {"instruments": [{"symbol": "AGIUSDT"}, {"symbol": "BTCUSDT"}], "tickers": [{"symbol": "AGIUSDT"}]}

    assert snapshots.due(HOUR_10)
    snapshots.write(HOUR_10, tables)

    meta = tmp_path / "2026-09-01" / "10" / "_meta"
    written = sorted(path.name for path in meta.iterdir())
    assert written == ["instruments-20260901T100000Z.json.zst", "tickers-20260901T100000Z.json.zst"]
    payload = json.loads(subprocess.run(["zstd", "-dcq", str(meta / written[0])], check=True, capture_output=True).stdout)
    assert payload["kind"] == "instruments_snapshot"
    assert payload["venue"] == "bybit"
    assert payload["market"] == "linear"
    assert payload["schema"] == SCHEMA_VERSION
    assert payload["source"] == "https://api.bybit.com"
    assert payload["recorded_at_ns"] == HOUR_10
    assert payload["rows"] == tables["instruments"]
    tickers = json.loads(subprocess.run(["zstd", "-dcq", str(meta / written[1])], check=True, capture_output=True).stdout)
    assert tickers["kind"] == "tickers_snapshot"
    assert tickers["rows"] == tables["tickers"]
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["snapshot"] for row in receipts] == ["instruments", "tickers"]
    assert [row["rows"] for row in receipts] == [2, 1]
    assert all(row["day"] == "2026-09-01" and row["hour"] == "10" for row in receipts)
    assert snapshots.last_ns == HOUR_10


def test_a_daily_cadence_waits_for_the_day_and_an_hourly_one_for_the_hour(tmp_path: Path) -> None:
    tables: dict[str, list[dict[str, object]]] = {"instruments": [], "tickers": []}
    daily = Snapshots(tmp_path / "day", Manifest(tmp_path), venue="bybit", market="linear", source="x", cadence="day")
    hourly = Snapshots(tmp_path / "hour", Manifest(tmp_path), venue="bybit", market="linear", source="x", cadence="hour")
    for snapshots in (daily, hourly):
        snapshots.root.mkdir(parents=True)
        snapshots.write(HOUR_10, tables)

    assert not daily.due(HOUR_10 + HOUR)
    assert daily.due(HOUR_10 + 24 * HOUR)
    assert hourly.due(HOUR_10 + HOUR)


def test_a_pass_survives_a_file_another_process_unlinked_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`market_tape pack` deletes shipped hours from its own process. A file it
    takes between this pass's stat and unlink must cost the pass nothing but
    that file: the pass goes on, and the receipt is not written twice."""

    manifest = Manifest(tmp_path)
    directory = tmp_path / "2027-01-15" / "10" / "AGIUSDT"
    directory.mkdir(parents=True)
    taken = directory / "segment-000000.jsonl.zst"
    ours = directory / "segment-000001.jsonl.zst"
    taken.write_bytes(b"gone-first")
    ours.write_bytes(b"ours")
    os.utime(taken, (1_000_000, 1_000_000))
    os.utime(ours, (1_000_001, 1_000_001))
    real_unlink = Path.unlink

    def unlink(self: Path, missing_ok: bool = False) -> None:
        if self == taken:
            real_unlink(self)  # the other process gets there first
            raise FileNotFoundError(str(self))
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)

    retention = Retention(tmp_path, manifest, retention_days=36_500, max_bytes=0, min_free_bytes=1)
    deleted = retention.prune(1_000_100.0)

    assert deleted == [ours.relative_to(tmp_path)]
    assert not taken.exists() and not ours.exists()
    receipts = [json.loads(line) for line in (tmp_path / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [receipt["path"] for receipt in receipts] == [str(ours.relative_to(tmp_path))]


def test_a_root_process_gives_what_it_writes_to_the_tape_roots_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`market_tape pack` runs as root; a root-owned file under the recorder's
    root is one the recorder can neither read nor replace."""

    path = tmp_path / "manifest.jsonl"
    path.write_text("{}\n", encoding="utf-8")
    real_stat = Path.stat

    def stat(self: Path, **kwargs: Any) -> Any:
        if self == path:
            return SimpleNamespace(st_uid=0, st_gid=0)
        if self == tmp_path:
            return SimpleNamespace(st_uid=1234, st_gid=5678)
        return real_stat(self, **kwargs)

    chowned: list[tuple[Path, int, int]] = []
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(os, "chown", lambda target, uid, gid: chowned.append((target, uid, gid)))

    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    storage.adopt_owner(path)
    assert chowned == []

    monkeypatch.setattr(os, "geteuid", lambda: 0)
    storage.adopt_owner(path)
    assert chowned == [(path, 1234, 5678)]

    # Already the directory's owner: nothing to do.
    monkeypatch.setattr(Path, "stat", lambda self, **kwargs: SimpleNamespace(st_uid=1234, st_gid=5678))
    storage.adopt_owner(path)
    assert len(chowned) == 1


def test_every_file_written_under_the_root_is_offered_to_its_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adopted: list[str] = []
    monkeypatch.setattr(storage, "adopt_owner", lambda path: adopted.append(path.name))
    burst(tmp_path, ("AAAUSDT",))

    compressor = Compressor(tmp_path, Manifest(tmp_path))
    compressor.start()
    compressor.close()
    descriptor = storage.lock_root(tmp_path)
    assert descriptor is not None
    storage.unlock_root(descriptor)

    assert adopted == ["segment-000000.jsonl.zst", "manifest.jsonl", storage.ROOT_LOCK_NAME]


def test_one_process_at_a_time_holds_a_tape_root(tmp_path: Path) -> None:
    first = storage.lock_root(tmp_path)

    assert first is not None
    assert (tmp_path / storage.ROOT_LOCK_NAME).exists()
    assert storage.lock_root(tmp_path) is None

    storage.unlock_root(first)
    second = storage.lock_root(tmp_path)
    assert second is not None
    storage.unlock_root(second)


# ------------------------------------------------------------- a full disk


class DiskFull:
    """What the segment files' disk will take: everything, or once `full`, what
    fits of the first write and nothing after it."""

    def __init__(self) -> None:
        self.full = False
        self.torn = False


class _FillableRaw(io.RawIOBase):
    def __init__(self, inner: Any, disk: DiskFull) -> None:
        self.inner = inner
        self.disk = disk

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        if self.disk.full:
            if not self.disk.torn and len(data) > 1:
                self.disk.torn = True
                return int(self.inner.write(bytes(data[: len(data) // 2])))
            raise OSError(errno.ENOSPC, "No space left on device")
        return int(self.inner.write(data))

    def fileno(self) -> int:
        return int(self.inner.fileno())

    def close(self) -> None:
        self.inner.close()
        super().close()


def fillable_writer(root: Path, disk: DiskFull, *, fsync_every: int) -> SegmentWriter:
    writer = SegmentWriter(root, max_bytes=1024 * 1024 * 1024, fsync_every=fsync_every)
    opened = writer._open

    def open_on_the_fillable_disk(symbol: str, day: str, hour: str) -> Any:
        segment = opened(symbol, day, hour)
        segment.handle = io.BufferedWriter(_FillableRaw(segment.handle.detach(), disk), writer.buffer_bytes)
        return segment

    writer._open = open_on_the_fillable_disk  # type: ignore[method-assign]
    return writer


def test_a_segment_the_full_disk_will_not_close_stays_open_and_closes_whole_later(tmp_path: Path) -> None:
    disk = DiskFull()
    writer = fillable_writer(tmp_path, disk, fsync_every=10**6)
    rows = [trade(HOUR_10 + n) for n in range(1, 101)]
    for row in rows:
        writer.append(row)
    disk.full = True

    with pytest.raises(OSError):
        writer.roll_idle(HOUR_10 + HOUR)
    assert "AGIUSDT" in writer.active, "a refused close dropped the segment it was closing"
    assert not list(tmp_path.rglob("*.jsonl"))

    disk.full = False
    closed = writer.roll_idle(HOUR_10 + HOUR)
    assert [segment.records for segment in closed] == [100]
    assert [json.loads(line) for line in closed[0].path.read_bytes().splitlines()] == rows
    assert not list(tmp_path.rglob("*.partial"))


def test_a_row_the_full_disk_refuses_is_either_kept_or_raised_never_both(tmp_path: Path) -> None:
    """The writer counts a frame lost when `append` raises, so a raise must mean
    nothing of the row reached the segment, and a row that did not raise must
    reach it once the disk has room, in order."""

    disk = DiskFull()
    writer = fillable_writer(tmp_path, disk, fsync_every=100)
    kept: list[dict[str, object]] = []
    refused = 0
    for n in range(3_000):
        disk.full = 1_000 <= n < 2_000
        row = trade(HOUR_10 + n + 1)
        try:
            writer.append(row)
        except OSError:
            refused += 1
            continue
        kept.append(row)
    closed = writer.close()

    assert refused > 0
    assert [json.loads(line) for line in closed[0].path.read_bytes().splitlines()] == kept
    assert closed[0].records == len(kept)


def test_a_shutdown_on_a_full_disk_closes_what_it_can_and_leaves_the_rest_for_recovery(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    disk = DiskFull()
    writer = fillable_writer(tmp_path, disk, fsync_every=10**6)
    writer.append(trade(HOUR_10 + 1, "AGIUSDT"))
    writer.append(trade(HOUR_10 + 2, "BTCUSDT"))
    disk.full = True

    with caplog.at_level(logging.ERROR):
        closed = writer.close()

    assert closed == [] and writer.active == {}
    assert len(list(tmp_path.rglob("*.jsonl.partial"))) == 2
    assert "left for recovery" in caplog.text


def test_closing_a_segment_syncs_its_file_and_leaves_the_rename_to_the_compressor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every symbol's hour closes at once on the boundary, on the writer thread.
    The compressor's directory sync persists the rename with its archive, and a
    rename a power loss undoes leaves a `.partial` recovery finishes."""

    directories: list[Path] = []
    monkeypatch.setattr(storage, "sync_directory", directories.append)
    writer = SegmentWriter(tmp_path, max_bytes=1024 * 1024, fsync_every=10**6)
    for symbol in ("AGIUSDT", "BTCUSDT", "ETHUSDT"):
        writer.append(trade(HOUR_10 + 1, symbol))

    assert len(writer.roll_idle(HOUR_10 + HOUR)) == 3
    assert directories == []


def test_a_meta_table_the_disk_refuses_leaves_no_raw_file_to_hold_the_hour_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`pack` never ships an hour holding a file that is not `.zst`, and nothing
    else removes a raw `_meta` table: one left behind strands the whole hour."""

    def no_space(source: Path, output: Path, **_: Any) -> str:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(storage, "zstd_compress", no_space)
    manifest = Manifest(tmp_path)
    snapshots = Snapshots(tmp_path, manifest, venue="bybit", market="linear", source="x", cadence="hour")
    with pytest.raises(OSError):
        snapshots.write(HOUR_10, {"instruments": [], "tickers": []})
    coverage = storage.CoverageRecords(tmp_path, manifest)
    with pytest.raises(OSError):
        coverage.write({"day": "2026-09-01", "hour": "10", "window": {"from_ns": HOUR_10, "to_ns": HOUR_10 + 1}})

    meta = tmp_path / "2026-09-01" / "10" / "_meta"
    assert sorted(path.name for path in meta.iterdir()) == []


def test_a_receipt_after_a_torn_append_starts_its_own_line(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    manifest.append({"kind": "segment_compressed", "path": "a.zst"})
    with (tmp_path / "manifest.jsonl").open("ab") as handle:
        handle.write(b'{"kind":"segment_compressed","pa')
    manifest.append({"kind": "segment_compressed", "path": "b.zst"})

    assert sorted(storage.read_receipts(tmp_path / "manifest.jsonl")) == ["a.zst", "b.zst"]


def _receipt(path: str, kind: str = "segment_compressed") -> dict[str, object]:
    return {"kind": kind, "path": path, "records": 1, "sha256": "0" * 64}


def _on_disk(root: Path, path: str) -> dict[str, object]:
    (root / path).parent.mkdir(parents=True, exist_ok=True)
    (root / path).write_bytes(b"archived")
    return _receipt(path, "snapshot_compressed" if "/_meta/" in path else "segment_compressed")


def test_a_retention_pass_leaves_the_manifest_the_receipts_of_the_files_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A month of an hourly tape is hundreds of thousands of receipts for files
    long shipped and as many deletion rows, and `pack` parses the file every hour."""

    monkeypatch.setattr(storage, "MANIFEST_COMPACT_BYTES", 0, raising=False)
    manifest = Manifest(tmp_path)
    shipped = [_receipt(f"2027-01-14/{hour:02d}/AGIUSDT/segment-000000.jsonl.zst") for hour in range(20)]
    # Gone without its deletion row: a row `pack` appended to a file being replaced.
    vanished = _receipt("2027-01-13/00/AGIUSDT/segment-000000.jsonl.zst")
    live = [_on_disk(tmp_path, f"2027-01-15/10/SYM{index}USDT/segment-000000.jsonl.zst") for index in range(3)]
    live.append(_on_disk(tmp_path, "2027-01-15/10/_meta/instruments-20270115T100000Z.json.zst"))
    expired = _on_disk(tmp_path, "2027-01-15/09/AGIUSDT/segment-000000.jsonl.zst")
    now = time.time()
    for path in [tmp_path / str(row["path"]) for row in live]:
        os.utime(path, (now, now))
    os.utime(tmp_path / str(expired["path"]), (now - 40 * 86_400, now - 40 * 86_400))
    for row in [*shipped, vanished, *live, expired]:
        manifest.append(row)
    for row in shipped:
        manifest.append({"kind": "segment_deleted", "path": row["path"], "reason": "shipped"})

    # A deletion row forgets its receipt as the file is read, rewritten or not.
    assert set(storage.read_receipts(manifest.path)) == {row["path"] for row in [vanished, *live, expired]}

    retention = Retention(tmp_path, manifest, retention_days=30, max_bytes=10**12, min_free_bytes=1)
    assert retention.prune(now) == [Path(str(expired["path"]))]

    assert [json.loads(line) for line in manifest.path.read_bytes().splitlines()] == live
    assert set(storage.read_receipts(manifest.path)) == {row["path"] for row in live}


def test_what_lands_while_the_manifest_is_rewritten_stays_in_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The rewrite reads the file and stats every receipted path without the
    lock, so the recorder's threads append meanwhile and `pack` appends
    deletion rows from its own process, which takes no lock at all."""

    manifest = Manifest(tmp_path)
    kept = _on_disk(tmp_path, "2027-01-15/10/AGIUSDT/segment-000000.jsonl.zst")
    taken = _on_disk(tmp_path, "2027-01-15/10/BTCUSDT/segment-000000.jsonl.zst")
    gone = _receipt("2027-01-14/10/AGIUSDT/segment-000000.jsonl.zst")
    for row in (gone, kept, taken):
        manifest.append(row)
    compressed = "2027-01-15/11/AGIUSDT/segment-000000.jsonl.zst"
    walk = storage._live_receipts

    def meanwhile(path: Path, kinds: Any) -> tuple[dict[str, dict[str, Any]], int]:
        found = walk(path, kinds)
        # The compressor receipts a segment it just finished.
        manifest.append(_on_disk(tmp_path, compressed))
        # `pack` ships an hour: the file goes, then its row, appended as `append_ledger` does.
        (tmp_path / str(taken["path"])).unlink()
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"kind": "segment_deleted", "path": taken["path"], "reason": "shipped"}) + "\n")
        return found

    monkeypatch.setattr(storage, "_live_receipts", meanwhile)
    assert manifest.compact() is True

    assert [json.loads(line)["path"] for line in manifest.path.read_bytes().splitlines()] == [kept["path"], compressed]

    # Once the recorder lets go of the root, a rewrite could replace a receipt
    # the next holder appends: `close` ends rewrites before the lock goes.
    monkeypatch.setattr(storage, "_live_receipts", walk)
    manifest.close()
    manifest.append(gone)
    assert manifest.compact() is False
    assert json.loads(manifest.path.read_bytes().splitlines()[-1]) == gone


def test_a_compression_the_disk_refuses_mid_stream_ends_instead_of_waiting_on_zstd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zstd blocks on a full stdout pipe once nobody reads it, so a failure
    between reads must stop it before anything waits on its pipes."""

    _fake_zstd(tmp_path, monkeypatch, "exec head -c 8000000 /dev/zero")

    class RefusingHash:
        def update(self, _block: bytes) -> None:
            raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(storage.hashlib, "sha256", RefusingHash)
    source = tmp_path / "segment-000000.jsonl"
    source.write_bytes(b'{"a":1}\n')
    raised: list[BaseException] = []

    def compress() -> None:
        try:
            zstd_compress(source, source.with_suffix(".jsonl.zst"), timeout=60.0)
        except BaseException as exc:  # asserted below
            raised.append(exc)

    worker = threading.Thread(target=compress, daemon=True)
    worker.start()
    worker.join(10.0)
    assert not worker.is_alive(), "the compression waited on a zstd blocked writing to it"
    assert isinstance(raised[0], OSError)
    assert not source.with_suffix(".jsonl.zst.tmp").exists()
