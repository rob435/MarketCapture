"""Pack each finished hour of a tape into one archive and upload it to the storage box.

The recorder writes one compressed file per symbol per UTC hour under
`<root>/<day>/<HH>/<SYMBOL>/` plus the table snapshots under `<day>/<HH>/_meta/`.
That is hundreds of files an hour, and a cloud folder full of them is a mess
nobody can browse. This job takes every hour that has finished (its end is
more than `--grace-seconds` ago and nothing under it is still open or
uncompressed), writes one uncompressed tar of its already-compressed files
with a `MANIFEST.json` first, and beside it an index of where each member's
bytes sit in the tar (`<day>T<HH>Z.tar.index.json`, `build_index`), so a
reader on a slow line range-reads one symbol's members rather than the hour
(`market_tape fetch`). It uploads the index, then the tar, as
`<remote>/<YYYY>/<MM>/<DD>/<day>T<HH>Z.tar[.index.json]`, reads the remote's
own size and MD5 of both back (the storage box runs `md5sum` over the stored
file) to prove the bytes landed, and records the hour in a local ledger, with
the box that confirmed it, so it is never packed twice for that box. The index
lands first, so a tar on the box without one was packed before indexes
existed. The box is what the rclone config's section
for the remote names (`box_of`): when the config names another box, every hour
whose segments are still on disk ships to it. An hour another box confirmed and
the window has since emptied holds only its `_meta` here; its tape is on that
box, and it is not packed again as if it were the whole hour.

Several tapes ship in one run: `--tape NAME=ROOT` repeated, each landing under
`--remote-base/NAME`. The single-tape form `--root ROOT --remote REMOTE` is the
same thing with one tape.

A root whose recorder is stopped still holds whatever that recorder left raw,
and a raw file under an hour is what keeps the hour from packing at all. So
before it looks for finished hours, each run takes the root's
`.recorder.lock`: a recorder holding it owns its own raw segments and this run
says so and moves on; on an idle root the run finishes the job itself, with the
recorder's own recovery — partials truncated to whole lines and renamed, torn
`.zst.tmp` dropped, every raw segment compressed, verified and receipted in
`manifest.jsonl` — bounded by `--recover-timeout`.

The archive on the storage box is the copy that lasts. The local tape is a
sliding window: once an hour is in the ledger for the box the config names
now and has been over for
`--keep-hours`, the same run deletes its segments from the recorder's root, so
the disk holds the last `--keep-hours` of tape plus whatever has not shipped
yet. An hour that is not in the ledger is never deleted here, whatever its age;
the recorder's own `retention_days` / `max_disk_gb` / `min_free_disk_gb` are the
backstop for a tape the storage box is not taking. The `_meta` table snapshots
stay: with a daily cadence the day's first hour holds the snapshot every later
local hour reads against, and the recorder prunes them by age.
"""

from __future__ import annotations

import argparse
import configparser
import fcntl
import hashlib
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Any, Iterable, Mapping, cast

from market_tape.storage import (
    ZSTD_TIMEOUT_SECONDS,
    Compressor,
    Manifest,
    discard_file_cache,
    lock_root,
    read_receipts,
    unlock_root,
)

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
HOUR_RE = re.compile(r"^\d{2}$")
#: The recorder's `UMask=0027`. `main` runs under 0o077 for its own state — the
#: ledger, the lock, staging — but what it writes under a tape root belongs to
#: the tape.
RECOVERY_UMASK = 0o027
#: Deadline for an rclone call that carries no file: `about`, and a listing,
#: for which the box hashes each file the listing names (an hour's tar and index).
RCLONE_TIMEOUT_SECONDS = 1800.0
#: The slowest one-stream upload rate to the box a deadline allows for. An
#: upload's deadline is its length at this rate, never under
#: RCLONE_TIMEOUT_SECONDS: an hour's tar grows with the market, rclone keeps
#: nothing of an upload it did not finish, and an hour that could not meet a
#: fixed deadline would fail every run and stop every later hour behind it.
UPLOAD_FLOOR_BYTES_PER_S = 100 * 1000
#: The index sidecar's suffix after the tar's own name, and its shape.
INDEX_SUFFIX = ".index.json"
INDEX_KIND = "market_tape_hour_index"
INDEX_SCHEMA = 1
#: Nothing on the host reads a segment again once it is in the tar, nor the
#: tar once it is uploaded, and every page this job reads or writes is charged
#: to its `MemoryMax`: the tar is synced and dropped from the page cache every
#: this many bytes as it is written, and while rclone reads it (from 1.74 rclone
#: no longer drops what it reads) its pages are dropped every interval.
SHED_BYTES = 64 * 1024 * 1024
SHED_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class Candidate:
    """One archive to build: a finished hour."""

    name: str
    day: str
    hour: str
    directories: tuple[Path, ...]

    @property
    def remote_name(self) -> str:
        year, month, day = self.day.split("-")
        return f"{year}/{month}/{day}/{self.name}.tar"

    @property
    def remote_index_name(self) -> str:
        return f"{self.remote_name}{INDEX_SUFFIX}"


@dataclass(frozen=True)
class Tape:
    """One recorder root and the remote folder its archives land in."""

    name: str
    root: Path
    remote: str


def _raw_files_under(directory: Path) -> bool:
    for path in directory.rglob("*"):
        if path.is_file() and not path.name.endswith(".zst"):
            return True
    return False


def _has_archives(directory: Path) -> bool:
    return any(path.is_file() and path.name.endswith(".zst") for path in directory.rglob("*"))


def _raw_work(root: Path) -> int:
    """Segments under the root that no compression has taken: the closed raw
    files, and the `.jsonl.partial` a writer had open."""

    closed = sum(1 for path in root.rglob("segment-*.jsonl") if path.is_file())
    return closed + sum(1 for path in root.rglob("*.jsonl.partial") if path.is_file())


def recover_idle_root(tape: Tape, *, timeout: float, dry_run: bool) -> dict[str, Any]:
    """Compress what a stopped recorder left raw, on a root no recorder holds.

    The lock is the whole decision: a live recorder's compressor owns its raw
    files, deferred ones included, and two compressors on one root would race
    for the same segment. On an idle root nothing else will ever take them —
    the recorder's recovery runs at start, and the hours stay unpackable and
    unshipped until it does.
    """

    raw = _raw_work(tape.root)
    descriptor = lock_root(tape.root)
    if descriptor is None:
        if raw and dry_run:
            print(f"would recover {raw} raw segment(s) under {tape.root} (recorder holds the root)")
        elif raw:
            print(
                f"market tape: {tape.name}: a recorder holds {tape.root}; "
                f"its compressor owns the {raw} raw segment(s)"
            )
        return {"raw": raw, "recovered": 0, "failed": 0, "held": True}
    try:
        if not raw:
            return {"raw": 0, "recovered": 0, "failed": 0, "held": False}
        if dry_run:
            print(f"would recover {raw} raw segment(s) under {tape.root} (root idle)")
            return {"raw": raw, "recovered": 0, "failed": 0, "held": False}
        started = time.monotonic()
        umask = os.umask(RECOVERY_UMASK)
        try:
            compressor = Compressor(tape.root, Manifest(tape.root), backlog_max_bytes=0)
            compressor.start()
            try:
                compressor.close(timeout=timeout)
            except RuntimeError as exc:
                # Whatever is still raw simply does not finish this run; the
                # hours it holds stay on disk and pack on a later one. The
                # thread is still working, though, and the lock this holds is
                # the promise that nothing else writes raw files under the
                # root: drop what it has not started and wait for the one
                # `zstd` call in flight, which `ZSTD_TIMEOUT_SECONDS` bounds.
                print(f"market tape: {tape.name}: {exc}", file=sys.stderr)
                while True:
                    try:
                        compressor.pending.get_nowait()
                    except queue.Empty:
                        break
                compressor.pending.put(None)
                compressor.thread.join(ZSTD_TIMEOUT_SECONDS)
        finally:
            os.umask(umask)
        print(
            f"market tape: {tape.name}: recovered {raw} raw segment(s) under {tape.root} "
            f"in {time.monotonic() - started:.0f}s compressed={compressor.compressed} failed={compressor.failed}"
        )
        return {"raw": raw, "recovered": compressor.compressed, "failed": compressor.failed, "held": False}
    finally:
        unlock_root(descriptor)


def finished_candidates(root: Path, *, now: float, grace_seconds: float) -> list[Candidate]:
    """Hours whose files are all closed and whose time has passed."""

    candidates: list[Candidate] = []
    for day_dir in sorted(path for path in root.iterdir() if path.is_dir() and DAY_RE.match(path.name)):
        day = day_dir.name
        for child in sorted(path for path in day_dir.iterdir() if path.is_dir() and HOUR_RE.match(path.name)):
            hour_end = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(hours=int(child.name) + 1)
            if now < hour_end.timestamp() + grace_seconds:
                continue
            if _raw_files_under(child) or not _has_archives(child):
                continue
            candidates.append(Candidate(f"{day}T{child.name}Z", day, child.name, (child,)))
    return candidates


def box_of(config: Path, remote: str) -> str:
    """The store an rclone remote reaches, as its section in `config` names it.

    Another box under the same remote name is another store: what one box
    confirmed says nothing about the next."""

    parser = configparser.RawConfigParser()
    parser.read(config, encoding="utf-8")
    name = remote.split(":", 1)[0]
    if not parser.has_section(name):
        return name
    fields = parser[name]
    return f"{fields.get('type', '')}://{fields.get('user', '')}@{fields.get('host', '')}:{fields.get('port', '')}"


def load_ledger(path: Path, boxes: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    """Archives the box each remote reaches now confirmed, keyed by remote path,
    so several tapes share one ledger. `boxes` maps a remote's name to its
    `box_of`; a row another box confirmed does not count."""

    rows: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        remote_path = str(row.get("remote_path") or "")
        if remote_path and row.get("box") == boxes.get(remote_path.split(":", 1)[0]):
            rows[remote_path] = row
    return rows


def confirmed_elsewhere(path: Path, boxes: Mapping[str, str]) -> set[str]:
    """Remote paths a box other than the one each remote reaches now confirmed."""

    if not path.exists():
        return set()
    paths: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        remote_path = str(row.get("remote_path") or "")
        if remote_path and row.get("box") != boxes.get(remote_path.split(":", 1)[0]):
            paths.add(remote_path)
    return paths


def bytes_uploaded_since(ledger: Mapping[str, Mapping[str, Any]], since: float) -> int:
    """Outbound bytes the ledger records after `since` (Unix seconds): the month's upload cost."""

    total = 0
    for row in ledger.values():
        stamp = str(row.get("uploaded_at") or "")
        try:
            uploaded = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
        if uploaded >= since:
            total += int(row.get("bytes") or 0)
    return total


def append_ledger(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_capture_manifest(path: Path) -> dict[str, dict[str, Any]]:
    """The recorder's own receipts, by relative path, for row counts and time spans."""

    return read_receipts(path)


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
        discard_file_cache(handle)
    return hasher.hexdigest()


def _md5(path: Path) -> str:
    hasher = hashlib.md5()  # noqa: S324 - the remote reports MD5; this only compares against it
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


class _ShedWriter:
    """The tar's file as `tarfile` writes it, never read back: every byte also
    feeds `digest`, and every `SHED_BYTES` what is written is synced and dropped
    from the page cache. `shed` at the end is the tar's fsync."""

    def __init__(self, handle: IO[bytes], digest: Any) -> None:
        self._handle = handle
        self._digest = digest
        self._written = 0
        self._unshed = 0

    def write(self, data: bytes) -> int:
        self._handle.write(data)
        if self._digest is not None:
            self._digest.update(data)
        self._written += len(data)
        self._unshed += len(data)
        if self._unshed >= SHED_BYTES:
            self.shed()
        return len(data)

    def tell(self) -> int:
        return self._written

    def shed(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())
        discard_file_cache(self._handle)
        self._unshed = 0


def build_archive(
    candidate: Candidate,
    root: Path,
    staging: Path,
    receipts: dict[str, dict[str, Any]],
    *,
    tape: str | None = None,
    digest: Any = None,
) -> tuple[Path, dict[str, Any]]:
    """Write <staging>/<name>.tar with MANIFEST.json first; return its path and manifest.

    `digest` (a `hashlib` object) takes every byte of the tar as it is written,
    so the caller has the tar's hash without reading it back."""

    files: list[dict[str, Any]] = []
    members: list[tuple[Path, str]] = []
    for directory in candidate.directories:
        for path in sorted(p for p in directory.rglob("*") if p.is_file() and p.name.endswith(".zst")):
            relative_to_root = str(path.relative_to(root))
            arcname = str(path.relative_to(directory))
            receipt = receipts.get(relative_to_root, {})
            sha256_digest = str(receipt.get("sha256") or "")
            if not sha256_digest:
                sha256_digest = _sha256(path)
            files.append(
                {
                    "path": arcname,
                    "bytes": path.stat().st_size,
                    "sha256": sha256_digest,
                    "records": receipt.get("records"),
                    "first_receive_ns": receipt.get("first_receive_ns"),
                    "last_receive_ns": receipt.get("last_receive_ns"),
                    "symbol": receipt.get("symbol"),
                    "snapshot": receipt.get("snapshot"),
                }
            )
            members.append((path, arcname))
    manifest = {
        "kind": "market_tape_hour",
        "name": candidate.name,
        "tape": tape,
        "day": candidate.day,
        "hour": candidate.hour,
        "created_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": files,
        "file_count": len(files),
        "bytes": sum(int(row["bytes"]) for row in files),
        "symbols": sorted({row["symbol"] for row in files if row.get("symbol")}),
    }
    staging.mkdir(parents=True, exist_ok=True)
    output = staging / f"{candidate.name}.tar"
    temporary = staging / f".{candidate.name}.tar.tmp"
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    try:
        with temporary.open("wb") as output_handle:
            sink = _ShedWriter(output_handle, digest)
            with tarfile.open(fileobj=cast(IO[bytes], sink), mode="w", format=tarfile.PAX_FORMAT) as archive:
                info = tarfile.TarInfo("MANIFEST.json")
                info.size = len(manifest_bytes)
                info.mtime = int(time.time())
                info.mode = 0o644
                archive.addfile(info, fileobj=_Bytes(manifest_bytes))
                for path, arcname in members:
                    info = archive.gettarinfo(str(path), arcname=arcname)
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mode = 0o644
                    with path.open("rb") as handle:
                        archive.addfile(info, fileobj=handle)
                        discard_file_cache(handle)
            sink.shed()
    except BaseException:
        # Staging is on the filesystem the recorders' `min_free_disk_gb` floor
        # guards and outside both tape roots, so `Retention.prune` — which
        # walks `<tape root>/**/*.zst` — can neither see a partial archive nor
        # delete it. Left behind, it is disk the recorders pay for forever.
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, output)
    return output, manifest


def build_index(archive: Path, manifest: Mapping[str, Any], *, tape: str | None = None) -> tuple[Path, dict[str, Any]]:
    """Write `<archive>.index.json` beside the tar: every member's data offset
    and size in it, with the MANIFEST's digest and symbol; return its path and payload.

    `offset` and `bytes` are the member's data, past its tar headers (PAX
    included), so `rclone cat --offset O --count N` of the tar is the file.
    `MANIFEST.json` is hashed from the tar; a segment the recorder never
    receipted takes its symbol from its directory; `_meta` members have none.
    """

    files = {str(row["path"]): row for row in manifest.get("files") or []}
    members: list[dict[str, Any]] = []
    with archive.open("rb") as raw, tarfile.open(fileobj=raw, mode="r") as handle:
        for info in handle.getmembers():
            if not info.isfile():
                continue
            row = files.get(info.name) or {}
            digest = str(row.get("sha256") or "")
            if not digest:
                source = handle.extractfile(info)
                digest = hashlib.sha256(source.read() if source is not None else b"").hexdigest()
            directory, separator, _ = info.name.partition("/")
            symbol = row.get("symbol") or (directory if separator and directory != "_meta" else None)
            members.append(
                {"path": info.name, "offset": info.offset_data, "bytes": info.size, "sha256": digest, "symbol": symbol}
            )
        discard_file_cache(raw)
    index = {
        "kind": INDEX_KIND,
        "schema": INDEX_SCHEMA,
        "name": manifest.get("name"),
        "tape": tape,
        "tar_bytes": archive.stat().st_size,
        "members": members,
    }
    output = archive.with_name(archive.name + INDEX_SUFFIX)
    temporary = output.with_name(f".{output.name}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(json.dumps(index, separators=(",", ":"), sort_keys=True).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    os.replace(temporary, output)
    return output, index


def sweep_staging(staging: Path) -> int:
    """Delete archives an earlier run left behind; return the bytes reclaimed.

    Staging holds one archive at a time and nothing reads it across runs, so
    anything here when a run starts is the remains of a run that was killed
    before its `finally` (`TimeoutStartSec`, `MemoryMax`, reboot). The caller
    holds the exclusive upload lock, so no live run owns these bytes.
    """

    if not staging.is_dir():
        return 0
    reclaimed = 0
    for path in sorted(staging.iterdir()):
        if not path.is_file() or not path.name.endswith((".tar", ".tar.tmp", INDEX_SUFFIX, f"{INDEX_SUFFIX}.tmp")):
            continue
        try:
            size = path.stat().st_size
            path.unlink()
        except OSError as exc:
            print(f"market tape: could not remove stale staging archive {path.name}: {exc}", file=sys.stderr)
            continue
        reclaimed += size
        print(f"market tape: removed stale staging archive {path.name} bytes={size}")
    return reclaimed


class _Bytes:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._data) - self._offset
        chunk = self._data[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def _shed_until(handle: IO[bytes], done: threading.Event) -> None:
    while not done.wait(SHED_INTERVAL_SECONDS):
        discard_file_cache(handle)


class Rclone:
    def __init__(self, binary: str, config: Path) -> None:
        self.binary = binary
        self.config = config

    def run(
        self, *args: str, capture: bool = False, timeout: float = RCLONE_TIMEOUT_SECONDS
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.binary, *args, "--config", str(self.config)],
            check=True,
            text=True,
            capture_output=capture,
            timeout=timeout,
        )

    def upload(self, local: Path, remote_path: str) -> None:
        with local.open("rb") as handle:
            done = threading.Event()
            shedder = threading.Thread(target=_shed_until, args=(handle, done), daemon=True)
            shedder.start()
            try:
                self.run(
                    "copyto",
                    str(local),
                    remote_path,
                    "--retries",
                    "5",
                    "--low-level-retries",
                    "10",
                    timeout=max(RCLONE_TIMEOUT_SECONDS, local.stat().st_size / UPLOAD_FLOOR_BYTES_PER_S),
                )
            finally:
                done.set()
                shedder.join()
                discard_file_cache(handle)

    def remote_listing(self, remote_dir: str, names: Iterable[str]) -> dict[str, tuple[str | None, int | None]]:
        """The named files in one remote directory: each one's MD5 as the remote computes it, and its size.

        The box reads a file whole for every hash type a listing asks of it, and
        a day's directory holds every tar of that day, so the listing names only
        these files and asks for MD5 alone.
        """

        filters = [argument for name in names for argument in ("--include", f"/{name}")]
        done = self.run("lsjson", remote_dir, "--hash", "--hash-type", "md5", "--files-only", *filters, capture=True)
        found: dict[str, tuple[str | None, int | None]] = {}
        for row in json.loads(done.stdout or "[]"):
            hashes = row.get("Hashes") or {}
            found[str(row.get("Name"))] = (hashes.get("md5") or hashes.get("MD5"), row.get("Size"))
        return found

    def free_bytes(self, remote: str) -> int | None:
        try:
            done = self.run("about", remote.split(":", 1)[0] + ":", "--json", capture=True)
            value = json.loads(done.stdout or "{}").get("free")
            return int(value) if isinstance(value, int) else None
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError):
            return None


def write_stamp(path: Path, values: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={'' if value is None else value}\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def ship(
    candidates: Iterable[Candidate],
    *,
    root: Path,
    remote: str,
    staging: Path,
    ledger_path: Path,
    rclone: Rclone,
    box: str,
    tape: str | None = None,
) -> list[dict[str, Any]]:
    receipts = load_capture_manifest(root / "manifest.jsonl")
    shipped: list[dict[str, Any]] = []
    for candidate in candidates:
        tar_md5 = hashlib.md5()  # noqa: S324 - the remote reports MD5; this only compares against it
        archive, manifest = build_archive(candidate, root, staging, receipts, tape=tape, digest=tar_md5)
        index = archive.with_name(archive.name + INDEX_SUFFIX)
        try:
            remote_path = f"{remote}/{candidate.remote_name}"
            index, _ = build_index(archive, manifest, tape=tape)
            local_md5 = tar_md5.hexdigest()
            size = archive.stat().st_size
            uploaded = {archive.name: (local_md5, size), index.name: (_md5(index), index.stat().st_size)}
            # The index first: a reader that finds the tar finds its index.
            rclone.upload(index, f"{remote}/{candidate.remote_index_name}")
            rclone.upload(archive, remote_path)
            # One listing proves both, as one proved the tar alone: the box
            # hashes every file a listing names.
            listing = rclone.remote_listing(remote_path.rpartition("/")[0], uploaded)
            for name, (md5, expected_size) in uploaded.items():
                remote_md5, remote_size = listing.get(name, (None, None))
                if remote_size != expected_size or (remote_md5 is not None and remote_md5.lower() != md5):
                    raise RuntimeError(
                        f"{candidate.name}: the remote holds {name} as size={remote_size} md5={remote_md5}, "
                        f"not the uploaded size={expected_size} md5={md5}"
                    )
            row = {
                "name": candidate.name,
                "tape": tape,
                "remote_path": remote_path,
                "box": box,
                "bytes": size,
                "md5": local_md5,
                "file_count": manifest["file_count"],
                "uploaded_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            append_ledger(ledger_path, row)
            shipped.append(row)
            print(f"market tape: shipped {candidate.name} files={manifest['file_count']} bytes={size} -> {remote_path}")
        finally:
            archive.unlink(missing_ok=True)
            index.unlink(missing_ok=True)
    return shipped


def candidate_end(candidate: Candidate) -> float:
    """Unix seconds at which the candidate's hour was over."""

    start = datetime.fromisoformat(candidate.day).replace(tzinfo=timezone.utc)
    span = timedelta(hours=int(candidate.hour) + 1)
    return (start + span).timestamp()


def _segments_under(directory: Path) -> list[Path]:
    """The hour's segment files: every `.zst` under it that is not a `_meta` snapshot."""

    return sorted(p for p in directory.rglob("*.zst") if p.is_file() and p.parent.name != "_meta")


def shipped_to_prune(
    root: Path,
    ledger: Mapping[str, Mapping[str, Any]],
    *,
    remote: str,
    now: float,
    keep_hours: float,
    grace_seconds: float,
) -> list[Candidate]:
    """Ledgered hours that ended more than `keep_hours` ago and still hold segments locally."""

    return [
        candidate
        for candidate in finished_candidates(root, now=now, grace_seconds=grace_seconds)
        if f"{remote}/{candidate.remote_name}" in ledger
        and now >= candidate_end(candidate) + keep_hours * 3600.0
        and any(_segments_under(directory) for directory in candidate.directories)
    ]


def prune_shipped(
    root: Path,
    ledger: Mapping[str, Mapping[str, Any]],
    *,
    remote: str,
    now: float,
    keep_hours: float,
    grace_seconds: float,
) -> list[dict[str, Any]]:
    """Delete the local segments of every ledgered hour that ended more than
    `keep_hours` ago; return one row per hour pruned.

    Membership in the ledger is the only licence to delete: a row is written
    after the remote's own size and MD5 matched the upload, so an hour missing
    from it — never shipped, or shipped and rejected — stays on disk for the
    recorder's retention to judge. `_meta` snapshots are left in place and the
    hour directory with them; an hour whose segments are already gone is a
    no-op, so a run over an old window is idempotent.

    The recorder's `Retention.prune` may be unlinking on its own thread; a file
    gone between the listing and the unlink is counted as not ours.
    """

    pruned: list[dict[str, Any]] = []
    manifest_path = root / "manifest.jsonl"
    for candidate in shipped_to_prune(root, ledger, remote=remote, now=now, keep_hours=keep_hours, grace_seconds=grace_seconds):
        remote_path = f"{remote}/{candidate.remote_name}"
        files = 0
        size_total = 0
        # The recorder's manifest is the tape's receipt trail; a deletion made
        # from this process belongs in it too. Each row reaches the file as its
        # unlink is made, and the hour's rows are synced once, after its last.
        trail = manifest_path.open("a", encoding="utf-8") if manifest_path.exists() else None
        try:
            for directory in candidate.directories:
                for path in _segments_under(directory):
                    try:
                        size = path.stat().st_size
                        path.unlink()
                    except FileNotFoundError:
                        continue
                    files += 1
                    size_total += size
                    if trail is not None:
                        row = {
                            "kind": "segment_deleted",
                            "recorded_at_ns": time.time_ns(),
                            "path": str(path.relative_to(root)),
                            "compressed_bytes": size,
                            "reason": "shipped",
                            "remote_path": remote_path,
                        }
                        trail.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
                        trail.flush()
                for empty, _, _ in os.walk(directory, topdown=False):
                    try:
                        Path(empty).rmdir()
                    except OSError:
                        pass
            if trail is not None:
                os.fsync(trail.fileno())
        finally:
            if trail is not None:
                trail.close()
        if not files:
            continue
        try:
            (root / candidate.day).rmdir()
        except OSError:
            pass
        pruned.append({"name": candidate.name, "remote_path": remote_path, "file_count": files, "bytes": size_total})
        print(f"market tape: pruned shipped {candidate.name} files={files} bytes={size_total}")
    return pruned


def _usage_error(message: str) -> SystemExit:
    print(f"market tape: {message}", file=sys.stderr)
    return SystemExit(2)


def parse_tapes(args: argparse.Namespace) -> list[Tape]:
    tapes: list[Tape] = []
    if args.tape:
        if not args.remote_base or ":" not in args.remote_base:
            raise _usage_error("--tape needs --remote-base as an rclone remote (remote:path)")
        base = args.remote_base.rstrip("/")
        for text in args.tape:
            name, separator, root = str(text).partition("=")
            if not separator or not name or not root or "/" in name:
                raise _usage_error(f"--tape wants NAME=ROOT, got {text!r}")
            tapes.append(Tape(name, Path(root).resolve(), f"{base}/{name}"))
    if args.root is not None or args.remote is not None:
        if args.root is None or args.remote is None or ":" not in args.remote:
            raise _usage_error("--root and --remote (remote:path) go together")
        remote = args.remote.rstrip("/")
        tapes.append(Tape(remote.rsplit("/", 1)[-1], Path(args.root).resolve(), remote))
    if not tapes:
        raise _usage_error("name at least one tape with --tape NAME=ROOT or --root/--remote")
    return tapes


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="market_tape pack", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tape", action="append", default=[], help="NAME=ROOT; repeat for several tapes")
    parser.add_argument("--remote-base", default=None, help="rclone folder the tapes land under, remote:path")
    parser.add_argument("--root", type=Path, default=None, help="single-tape form: the recorder's root directory")
    parser.add_argument("--remote", default=None, help="single-tape form: rclone destination, remote:path")
    parser.add_argument("--state-dir", type=Path, required=True, help="ledger, lock and staging")
    parser.add_argument("--stamp-file", type=Path, required=True, help="receipt written after a fully successful run")
    parser.add_argument("--grace-seconds", type=float, default=300.0, help="how long after an hour ends before it is packed")
    parser.add_argument(
        "--recover-timeout",
        type=float,
        default=1500.0,
        help="seconds this run may spend compressing what a stopped recorder left raw under a tape root",
    )
    parser.add_argument(
        "--keep-hours",
        type=float,
        default=24.0,
        help="local sliding window: a shipped hour's segments are deleted once it has been over this long",
    )
    parser.add_argument("--rclone", default=os.environ.get("RCLONE_BIN") or "/usr/bin/rclone")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(os.environ.get("RCLONE_CONFIG") or "/etc/market-capture/rclone.conf"),
        help="rclone config, read where it lies (default: $RCLONE_CONFIG, else the host's)",
    )
    parser.add_argument("--now", type=float, default=None, help="override the clock (tests)")
    parser.add_argument("--dry-run", action="store_true", help="list what would be packed and stop")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.keep_hours < 0:
        raise _usage_error(f"--keep-hours must be zero or more, got {args.keep_hours}")
    tapes = parse_tapes(args)
    present = [tape for tape in tapes if tape.root.is_dir()]
    for tape in tapes:
        if tape not in present:
            print(f"market tape: {tape.name} has no root yet at {tape.root}; skipping it", file=sys.stderr)
    if not present:
        print("market tape: no tape root exists", file=sys.stderr)
        return 2
    now = args.now if args.now is not None else time.time()
    os.umask(0o077)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = args.state_dir / "uploaded-tapes.jsonl"
    boxes = {tape.remote.split(":", 1)[0]: box_of(args.config, tape.remote) for tape in present}
    ledger = load_ledger(ledger_path, boxes)
    elsewhere = confirmed_elsewhere(ledger_path, boxes)
    # An hour holding one raw file is refused by `finished_candidates`, so this
    # comes first: what it compresses ships in this same run.
    recovered = sum(
        int(recover_idle_root(tape, timeout=args.recover_timeout, dry_run=args.dry_run)["recovered"])
        for tape in present
    )
    pending: list[tuple[Tape, Candidate]] = []
    for tape in present:
        for candidate in finished_candidates(tape.root, now=now, grace_seconds=args.grace_seconds):
            remote_path = f"{tape.remote}/{candidate.remote_name}"
            if remote_path in ledger:
                continue
            if remote_path in elsewhere and not any(_segments_under(d) for d in candidate.directories):
                continue
            pending.append((tape, candidate))
    if args.dry_run:
        for tape, candidate in pending:
            print(f"would pack {tape.name} {candidate.name} -> {tape.remote}/{candidate.remote_name}")
        stale = 0
        for tape in present:
            for candidate in shipped_to_prune(
                tape.root, ledger, remote=tape.remote, now=now, keep_hours=args.keep_hours, grace_seconds=args.grace_seconds
            ):
                stale += 1
                print(f"would prune {tape.name} {candidate.name} (shipped, over {args.keep_hours:g}h ago)")
        print(f"market tape: {len(pending)} pending, {len(ledger)} already shipped, {stale} shipped hours past the window")
        return 0
    with (args.state_dir / "upload.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("market tape: another run owns the upload lock")
            return 0
        sweep_staging(args.state_dir / "staging")
        if not shutil.which(args.rclone) and not os.access(args.rclone, os.X_OK):
            print(f"market tape: rclone is not executable: {args.rclone}", file=sys.stderr)
            return 2
        if not args.config.exists():
            raise SystemExit(f"rclone config is missing: {args.config}")
        rclone = Rclone(args.rclone, args.config)
        shipped: list[dict[str, Any]] = []
        for tape in present:
            shipped.extend(
                ship(
                    [candidate for owner, candidate in pending if owner is tape],
                    root=tape.root,
                    remote=tape.remote,
                    staging=args.state_dir / "staging",
                    ledger_path=ledger_path,
                    rclone=rclone,
                    box=boxes[tape.remote.split(":", 1)[0]],
                    tape=tape.name,
                )
            )
        # The window is judged against the ledger as it stands after this run's
        # uploads, so an hour shipped a moment ago and already older than the
        # window goes in the same run.
        ledger = load_ledger(ledger_path, boxes)
        pruned: list[dict[str, Any]] = []
        for tape in present:
            pruned.extend(
                prune_shipped(
                    tape.root,
                    ledger,
                    remote=tape.remote,
                    now=now,
                    keep_hours=args.keep_hours,
                    grace_seconds=args.grace_seconds,
                )
            )
        destination = args.remote_base.rstrip("/") if args.remote_base else present[0].remote
        free = rclone.free_bytes(destination)
        write_stamp(
            args.stamp_file,
            {
                "uploaded_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "archives": ",".join(f"{row['tape']}/{row['name']}" if row.get("tape") else row["name"] for row in shipped) or "none",
                "file_count": sum(int(row["file_count"]) for row in shipped),
                "bytes": sum(int(row["bytes"]) for row in shipped),
                "bytes_30d": bytes_uploaded_since(ledger, now - 30 * 86_400),
                "destination": destination,
                "tapes": ",".join(tape.name for tape in present),
                "remote_free_bytes": free,
                "keep_hours": args.keep_hours,
                "recovered_segments": recovered,
                "pruned_hours": len(pruned),
                "pruned_bytes": sum(int(row["bytes"]) for row in pruned),
            },
        )
    print(
        f"market tape: shipped {len(shipped)} archives to {destination}; {len(pending) - len(shipped)} left; "
        f"pruned {len(pruned)} shipped hours older than {args.keep_hours:g}h"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
