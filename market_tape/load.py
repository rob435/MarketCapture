"""Read a recorded tape back, wherever it is stored.

Three sources answer the same three questions — which hours do you hold, which
files are in an hour, and give me the bytes of one file:

```text
HostRoot      a recorder root:             <day>/<HH>/<SYMBOL>/segment-*.jsonl.zst
ArchiveDir    an archive-shaped directory: YYYY/MM/DD/<day>T<HH>Z.tar
RcloneRemote  the storage box (any rclone remote in that layout), through a local cache of those tars
```

`iter_rows` merges an hour's symbols into one stream ordered by
`local_receive_ts_ns`, which is the order the recorder saw them. Each segment
is already in that order and a symbol's segments run in sequence, so the merge
is a heap over one open file per symbol and never holds an hour in memory.

A segment is JSON lines under zstd as the recorder wrote it, or the same rows
as a block file (`segment-*.lmtb`, `market_tape.blocks`), which any source may
hold beside or instead of the JSON: `python -m market_tape blocks` converts an
hour. A block file is read through its index, so a read filtered to some kinds
or to a window of receive time (`iter_rows(..., kinds=, start_ns=, end_ns=)`)
decodes only the blocks that can hold what it asks for; on a JSON segment the
same filters apply line by line and the read stops at the window's end.
`market_tape.blocks` needs numpy and is imported only when a block file is
met, so a host without it still reads and records JSON tape.

Every row names its own venue. A source names one too, `source.venue`: the
venue a block file cut from it carries in its header and a caller picks its
defaults by. It comes from the recorder's `status.json`, the source's own name
(`bybit-linear`, `binance-usdm`), a block file's header or the caller; a
source that says none of those is refused rather than guessed.

Decompression runs through the `zstd` command line tool; there is no zstd
Python module on the recording host.
"""

from __future__ import annotations

import fcntl
import hashlib
import heapq
import json
import logging
import os
import re
import shutil
import subprocess
import tarfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, IO, Iterable, Iterator, Mapping, Protocol, Sequence, cast

from market_tape.schema import COVERAGE_RECORD, Row, SchemaError, SNAPSHOT_KINDS, parse_row
from market_tape.storage import read_receipts

__all__ = [
    "ArchiveDir",
    "CacheError",
    "HostRoot",
    "RcloneRemote",
    "RemoteCommandError",
    "RemoteError",
    "RemoteTimeout",
    "SourceError",
    "TapeRowError",
    "hour_range",
    "iter_coverage",
    "iter_rows",
    "iter_snapshots",
    "open_source",
]

logger = logging.getLogger(__name__)

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
HOUR_RE = re.compile(r"^\d{2}$")
HOUR_KEY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2})$")
ARCHIVE_NAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2})Z\.tar$")
YEAR_RE = re.compile(r"^\d{4}$")
#: A block file's suffix; `market_tape.blocks` owns the format and takes the name from here.
BLOCK_SUFFIX = ".lmtb"

META = "_meta"
#: `_meta` members whose name begins with this are coverage records, not tables.
COVERAGE_PREFIX = "coverage-"
DEFAULT_CACHE = Path.home() / ".cache" / "market-tape"

#: Seconds an rclone listing may take before it is a `RemoteTimeout`.
LIST_TIMEOUT_SECONDS = 300.0
#: Seconds one hour archive may take to arrive in the cache.
COPY_TIMEOUT_SECONDS = 3_600.0


class SourceError(RuntimeError):
    """A source that cannot be read as a tape: bad metadata, a bad file, a failed transfer."""


class TapeRowError(SourceError):
    """A line a strict read could not parse; names the member and the line."""


class RemoteError(SourceError):
    """An rclone operation that did not answer."""

    def __init__(self, operation: str, remote_path: str, detail: str) -> None:
        super().__init__(f"rclone {operation} on {remote_path}: {detail}")
        self.operation = operation
        self.remote_path = remote_path


class RemoteTimeout(RemoteError):
    pass


class RemoteCommandError(RemoteError):
    def __init__(self, operation: str, remote_path: str, returncode: int, stderr: str) -> None:
        super().__init__(operation, remote_path, f"exit {returncode}: {stderr.strip() or 'no output'}")
        self.returncode = returncode
        self.stderr = stderr


class CacheError(RemoteError):
    """A fetched archive that is not the archive the remote lists."""


def hour_range(start: str, end: str) -> list[str]:
    """Hours from start to end as `YYYY-MM-DDTHH`; end is exclusive, and end == start means that one hour."""

    first, last = _hour_key(start), _hour_key(end)
    if last < first:
        raise ValueError(f"end {end!r} is before start {start!r}")
    if last == first:
        return [_hour_text(first)]
    hours = []
    moment = first
    while moment < last:
        hours.append(_hour_text(moment))
        moment += 1
    return hours


def _hour_key(text: str) -> int:
    """One hour as a count of hours, so arithmetic on it needs no calendar."""

    match = HOUR_KEY_RE.match(text)
    if match is None:
        raise ValueError(f"an hour is YYYY-MM-DDTHH, got {text!r}")
    day = datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
    return int(day.timestamp()) // 3600 + int(match.group(2))


def _hour_text(key: int) -> str:
    moment = datetime.fromtimestamp(key * 3600, tz=timezone.utc)
    return f"{moment.date().isoformat()}T{moment.hour:02d}"


def _current_hour() -> str:
    return _hour_text(int(time.time()) // 3600)


# ----------------------------------------------------------------- the bytes


def _zstd_lines(
    argv: Sequence[str], source: IO[bytes] | None = None, owns: Any = None, label: str = ""
) -> Generator[bytes, None, None]:
    """One line of the decompressed file per item; `source` feeds a stream instead of a path.

    A read error on `source` is the source's failure, not zstd's: zstd would
    see a clean end of input and could exit 0 on a frame boundary, so the
    feeder's error is checked after zstd's exit status regardless.
    """

    process = subprocess.Popen(
        list(argv),
        stdin=subprocess.PIPE if source is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    feeder: threading.Thread | None = None
    failure: list[BaseException] = []
    if source is not None:
        feeder = threading.Thread(target=_feed, args=(process, source, failure), daemon=True)
        feeder.start()
    stdout, stderr = process.stdout, process.stderr
    assert stdout is not None and stderr is not None
    try:
        yield from stdout
        returncode = process.wait()
        if feeder is not None:
            feeder.join(timeout=5)
        if failure:
            raise SourceError(f"reading {label or ' '.join(argv)} failed: {failure[0]}") from failure[0]
        if returncode != 0:
            # A reader that stops early leaves zstd shouting about a broken
            # pipe, so its words are only worth repeating when it truly failed.
            said = (stderr.read() or b"").decode(errors="replace").strip()
            raise SourceError(f"zstd exit {returncode} on {label or ' '.join(argv)}: {said}")
    finally:
        if process.poll() is None:
            process.kill()
        stdout.close()
        stderr.close()
        process.wait()
        if feeder is not None:
            feeder.join(timeout=5)
        if owns is not None:
            owns.close()


def _feed(process: subprocess.Popen[bytes], source: IO[bytes], failure: list[BaseException]) -> None:
    stdin = process.stdin
    assert stdin is not None
    try:
        shutil.copyfileobj(source, stdin)
    except BrokenPipeError:
        # zstd went away first: the reader stopped early, or zstd failed and
        # says so itself through its exit status.
        pass
    except (OSError, tarfile.TarError) as exc:
        failure.append(exc)
    finally:
        try:
            stdin.close()
        except OSError:
            pass
        source.close()


class Member(Protocol):
    """One compressed file inside an hour."""

    @property
    def symbol(self) -> str:
        """The symbol whose rows it holds, or `_meta` for a table snapshot."""

    @property
    def path(self) -> str:
        """Where it sits inside the hour, as `<SYMBOL>/segment-NNNNNN.jsonl.zst`."""

    def open(self) -> Generator[bytes, None, None]:
        """The decompressed content, one line per item."""

    def binary(self) -> IO[bytes]:
        """The file's bytes as stored, seekable, for a reader that indexes into them (a block file)."""


@dataclass(frozen=True)
class FileMember:
    symbol: str
    path: str
    file_path: Path

    def open(self) -> Generator[bytes, None, None]:
        return _zstd_lines(["zstd", "-dcq", "--", str(self.file_path)], label=str(self.file_path))

    def binary(self) -> IO[bytes]:
        return self.file_path.open("rb")


class _TarBinary:
    """One tar entry as a seekable binary file; closing it closes the archive it came from."""

    def __init__(self, entry: IO[bytes], archive: tarfile.TarFile, name: str) -> None:
        self._entry = entry
        self._archive = archive
        self.name = name

    def read(self, size: int = -1) -> bytes:
        return self._entry.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._entry.seek(offset, whence)

    def tell(self) -> int:
        return self._entry.tell()

    def close(self) -> None:
        self._entry.close()
        self._archive.close()


@dataclass(frozen=True)
class TarMember:
    symbol: str
    path: str
    archive: Path
    name: str
    #: The entry as the archive listed it; reading it needs no second scan of the tar.
    info: tarfile.TarInfo | None = field(default=None, compare=False, repr=False)

    def open(self) -> Generator[bytes, None, None]:
        handle = tarfile.open(self.archive, "r")
        source = handle.extractfile(self.info if self.info is not None else self.name)
        if source is None:
            handle.close()
            raise SourceError(f"{self.archive}: {self.name} holds no data")
        return _zstd_lines(["zstd", "-dcq"], source=source, owns=handle, label=f"{self.archive}:{self.name}")

    def binary(self) -> IO[bytes]:
        handle = tarfile.open(self.archive, "r")
        source = handle.extractfile(self.info if self.info is not None else self.name)
        if source is None:
            handle.close()
            raise SourceError(f"{self.archive}: {self.name} holds no data")
        return cast(IO[bytes], _TarBinary(source, handle, f"{self.archive}:{self.name}"))


def _symbol_of(relative: str) -> str:
    head = relative.split("/", 1)[0]
    return head if head == META else head.upper()


def _is_coverage(member: "Member") -> bool:
    return member.symbol == META and member.path.rsplit("/", 1)[-1].startswith(COVERAGE_PREFIX)


# --------------------------------------------------------------- the sources


class Source(Protocol):
    venue: str
    skipped_rows: int

    def hours(self) -> list[str]:
        """Hours as `YYYY-MM-DDTHH`."""

    def hour_members(self, hour: str) -> list[Member]:
        """The compressed files of one hour, symbol members and `_meta` alike."""

    def hour_manifest(self, hour: str) -> dict[str, dict[str, Any]]:
        """The receipt of every file of that hour, keyed by the member path: row
        counts, receive spans and digests as the writer recorded them. Empty
        when the source carries no receipts for the hour."""


def _venue_from_name(name: str) -> str | None:
    """`bybit-linear` and `binance-usdm` name the venue before the dash; anything else names none."""

    head, dash, _ = name.partition("-")
    return head if dash and head.isalpha() else None


def _remote_name(remote_path: str) -> str:
    """The last component of an rclone `remote:path`: `bybit-linear` of
    `box:bybit-linear` as of `box:tapes/bybit-linear`."""

    return remote_path.rsplit("/", 1)[-1].rsplit(":", 1)[-1]


def _venue_or_refuse(explicit: str | None, inferred: str | None, what: str) -> str:
    venue = explicit or inferred
    if not venue:
        raise SourceError(f"{what} names no venue; pass one explicitly (open_source(..., venue=...) or --venue)")
    return venue


def _block_venue(root: Path) -> str | None:
    """The venue the first block file under a converted root names, when nothing else does."""

    if not root.is_dir():
        return None
    try:
        from market_tape.blocks import BlockError, first_block_venue
    except ImportError:  # a host without numpy holds no block files of its own
        return None
    try:
        return first_block_venue(root)
    except (BlockError, OSError) as exc:
        raise SourceError(f"{root}: a block file under it is not readable: {exc}") from exc


def _status_venue(root: Path) -> str | None:
    """The venue the recorder's status file names; a status file that cannot be read is refused."""

    status = root / "status.json"
    if not status.is_file():
        return None
    try:
        payload = json.loads(status.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SourceError(f"{status} is not readable JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise SourceError(f"{status} is not a JSON object")
    venue = payload.get("venue")
    if venue is None:
        return None
    if not isinstance(venue, str) or not venue:
        raise SourceError(f"{status} names venue {venue!r}, which is not a venue")
    return venue


class HostRoot:
    """A recorder root on the host that wrote it.

    Members of a finished hour are listed once and remembered; the hour the
    wall clock is in is still being written, so it is listed on every call.
    `refresh()` forgets everything remembered.
    """

    def __init__(self, path: Path, venue: str | None = None) -> None:
        self.root = Path(path)
        self.skipped_rows = 0
        self.venue = _venue_or_refuse(
            venue,
            _status_venue(self.root) or _venue_from_name(self.root.name) or _block_venue(self.root),
            str(self.root),
        )
        self._members: dict[str, list[Member]] = {}
        self._receipts: dict[str, dict[str, Any]] = {}
        self._receipt_key: tuple[int, int] | None = None

    @staticmethod
    def looks_like(path: Path) -> bool:
        if (path / "manifest.jsonl").is_file() or (path / "status.json").is_file():
            return True
        for day in path.iterdir():
            if not day.is_dir() or not DAY_RE.match(day.name):
                continue
            if any(child.is_dir() and HOUR_RE.match(child.name) for child in day.iterdir()):
                return True
        return False

    def refresh(self) -> None:
        self._members.clear()
        self._receipt_key = None

    def hours(self) -> list[str]:
        found: set[str] = set()
        for day_dir in self.root.iterdir():
            if not day_dir.is_dir() or not DAY_RE.match(day_dir.name):
                continue
            for child in day_dir.iterdir():
                if child.is_dir() and HOUR_RE.match(child.name):
                    found.add(f"{day_dir.name}T{child.name}")
        return sorted(found)

    def hour_members(self, hour: str) -> list[Member]:
        remembered = self._members.get(hour)
        if remembered is not None:
            return list(remembered)
        directory = self._directory(hour)
        members: list[Member] = []
        if directory.is_dir():
            for symbol_dir in directory.iterdir():
                if not symbol_dir.is_dir():
                    continue
                for path in symbol_dir.iterdir():
                    if path.is_file() and (path.name.endswith(".zst") or path.name.endswith(BLOCK_SUFFIX)):
                        relative = f"{symbol_dir.name}/{path.name}"
                        members.append(FileMember(_symbol_of(relative), relative, path))
            members.sort(key=lambda member: member.path)
        if hour < _current_hour():
            self._members[hour] = members
        return list(members)

    def hour_manifest(self, hour: str) -> dict[str, dict[str, Any]]:
        prefix = f"{str(self._directory(hour).relative_to(self.root))}/"
        return {
            path[len(prefix) :]: row for path, row in self._all_receipts().items() if path.startswith(prefix)
        }

    def _all_receipts(self) -> dict[str, dict[str, Any]]:
        path = self.root / "manifest.jsonl"
        try:
            stat = path.stat()
        except OSError:
            return {}
        key = (stat.st_size, stat.st_mtime_ns)
        if self._receipt_key != key:
            self._receipts = read_receipts(path)
            self._receipt_key = key
        return self._receipts

    def _directory(self, hour: str) -> Path:
        match = HOUR_KEY_RE.match(hour)
        if match is None:
            raise ValueError(f"an hour is YYYY-MM-DDTHH, got {hour!r}")
        return self.root / match.group(1) / match.group(2)


def _tar_members(archive: Path) -> list[Member]:
    members: list[Member] = []
    try:
        with tarfile.open(archive, "r") as handle:
            for info in handle.getmembers():
                name = info.name
                if not (name.endswith(".zst") or name.endswith(BLOCK_SUFFIX)):
                    continue
                members.append(TarMember(_symbol_of(name), name, archive, name, info))
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise SourceError(f"{archive} is not a readable tar archive: {exc}") from exc
    members.sort(key=lambda member: member.path)
    return members


def _tar_manifest(archive: Path) -> dict[str, dict[str, Any]]:
    """The receipts `MANIFEST.json` carries, by the member path they name."""

    try:
        with tarfile.open(archive, "r") as handle:
            try:
                source = handle.extractfile("MANIFEST.json")
            except KeyError:
                return {}
            if source is None:
                return {}
            payload = json.loads(source.read())
    except (tarfile.TarError, EOFError, OSError, ValueError) as exc:
        raise SourceError(f"{archive} has no readable MANIFEST.json: {exc}") from exc
    if not isinstance(payload, Mapping):
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for row in payload.get("files") or []:
        if isinstance(row, Mapping) and row.get("path"):
            rows[str(row["path"])] = dict(row)
    return rows


class _TarIndex:
    """The member lists of archives already opened, keyed by the file's identity on disk."""

    def __init__(self) -> None:
        self._members: dict[tuple[Path, int, int], list[Member]] = {}
        self._manifests: dict[tuple[Path, int, int], dict[str, dict[str, Any]]] = {}

    def members(self, archive: Path) -> list[Member]:
        stat = archive.stat()
        key = (archive, stat.st_size, stat.st_mtime_ns)
        found = self._members.get(key)
        if found is None:
            found = _tar_members(archive)
            self._members = {k: v for k, v in self._members.items() if k[0] != archive}
            self._members[key] = found
        return list(found)

    def manifest(self, archive: Path) -> dict[str, dict[str, Any]]:
        stat = archive.stat()
        key = (archive, stat.st_size, stat.st_mtime_ns)
        found = self._manifests.get(key)
        if found is None:
            found = _tar_manifest(archive)
            self._manifests = {k: v for k, v in self._manifests.items() if k[0] != archive}
            self._manifests[key] = found
        return dict(found)

    def clear(self) -> None:
        self._members.clear()
        self._manifests.clear()


class ArchiveDir:
    """A directory holding hour archives in the storage box's own layout."""

    def __init__(self, path: Path, venue: str | None = None) -> None:
        self.root = Path(path)
        self.skipped_rows = 0
        self.venue = _venue_or_refuse(venue, _venue_from_name(self.root.name), str(self.root))
        self._index = _TarIndex()

    @staticmethod
    def looks_like(path: Path) -> bool:
        return any(child.is_dir() and YEAR_RE.match(child.name) for child in path.iterdir())

    def refresh(self) -> None:
        self._index.clear()

    def hours(self) -> list[str]:
        return sorted(self._archives())

    def hour_members(self, hour: str) -> list[Member]:
        archive = self._archives().get(hour)
        if archive is None:
            return []
        return self._index.members(archive)

    def hour_manifest(self, hour: str) -> dict[str, dict[str, Any]]:
        archive = self._archives().get(hour)
        if archive is None:
            return {}
        return self._index.manifest(archive)

    def _archives(self) -> dict[str, Path]:
        found: dict[str, Path] = {}
        for path in self.root.glob("*/*/*/*.tar"):
            match = ARCHIVE_NAME_RE.match(path.name)
            if match is None:
                continue
            day, hour = match.group(1), match.group(2)
            found[f"{day}T{hour}"] = path
        return found


def _cache_key(remote_path: str) -> str:
    """One cache directory per remote spec: the spec made safe for a filename, kept distinct by its digest."""

    safe = re.sub(r"[^A-Za-z0-9._-]", "_", remote_path)[:64]
    return f"{safe}-{hashlib.sha256(remote_path.encode()).hexdigest()[:32]}"


@dataclass(frozen=True)
class _RemoteArchive:
    relative: str
    size: int | None


class RcloneRemote:
    """An rclone remote holding hour archives, read through a local cache of whole ones.

    Each remote spec owns a directory under the cache, so two remotes that
    hold the same hour never read each other's tar. Files sitting directly in
    the cache root belong to no remote and are never read.

    An archive arrives as `<name>.partial`, is checked against the size the
    remote listed and opened as a tar before it is renamed into place, so an
    interrupted copy is never mistaken for the hour. A cached archive whose
    size no longer matches the listing is fetched again; size is the identity
    the listing carries, so a re-pack that lands on the same tar size is
    served from the cache until `refresh()` and a cleared cache directory.

    Processes sharing a cache take an exclusive `flock` on `<name>.lock`
    beside the archive while they check and fetch it: the first fetches, the
    rest wait and read what it left. The lock file stays.
    """

    def __init__(
        self,
        remote_path: str,
        cache_dir: Path | None = None,
        *,
        venue: str | None = None,
        list_timeout: float = LIST_TIMEOUT_SECONDS,
        copy_timeout: float = COPY_TIMEOUT_SECONDS,
    ) -> None:
        self.remote_path = remote_path.rstrip("/")
        self.cache = Path(cache_dir) if cache_dir is not None else DEFAULT_CACHE
        self.root = self.cache / _cache_key(self.remote_path)
        self.binary = os.environ.get("RCLONE_BIN") or "rclone"
        self.skipped_rows = 0
        self.venue = _venue_or_refuse(venue, _venue_from_name(_remote_name(self.remote_path)), self.remote_path)
        self.list_timeout = list_timeout
        self.copy_timeout = copy_timeout
        self._listing: dict[str, _RemoteArchive] | None = None
        self._index = _TarIndex()

    def hours(self) -> list[str]:
        return sorted(self._remote_archives())

    def refresh(self) -> None:
        """Forget the remote listing; the next call asks the remote what it holds."""

        self._listing = None
        self._index.clear()

    def hour_members(self, hour: str) -> list[Member]:
        local = self._cached(hour)
        return [] if local is None else self._index.members(local)

    def hour_manifest(self, hour: str) -> dict[str, dict[str, Any]]:
        local = self._cached(hour)
        return {} if local is None else self._index.manifest(local)

    def _cached(self, hour: str) -> Path | None:
        """The hour's archive in the local cache, fetched if it is not there yet."""

        remote = self._remote_archives().get(hour)
        if remote is None:
            return None
        local = self.root / remote.relative
        local.parent.mkdir(parents=True, exist_ok=True)
        with open(local.with_name(local.name + ".lock"), "a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if local.is_file() and remote.size is not None and local.stat().st_size != remote.size:
                logger.warning("cached %s is %d bytes, the remote lists %d; fetching again", local, local.stat().st_size, remote.size)
                local.unlink()
            if not local.is_file():
                self._fetch(remote, local)
        return local

    def _fetch(self, remote: _RemoteArchive, local: Path) -> None:
        partial = local.with_name(local.name + ".partial")
        partial.unlink(missing_ok=True)
        try:
            self._run("copyto", f"{self.remote_path}/{remote.relative}", str(partial), timeout=self.copy_timeout)
            if not partial.is_file():
                raise CacheError("copyto", self.remote_path, f"{remote.relative} did not arrive")
            size = partial.stat().st_size
            if remote.size is not None and size != remote.size:
                raise CacheError(
                    "copyto", self.remote_path, f"{remote.relative} arrived as {size} bytes, the remote lists {remote.size}"
                )
            try:
                _tar_members(partial)
            except SourceError as exc:
                raise CacheError("copyto", self.remote_path, f"{remote.relative} is not a tar archive: {exc}") from exc
            with partial.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(partial, local)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

    def _remote_archives(self) -> dict[str, _RemoteArchive]:
        if self._listing is not None:
            return self._listing
        done = self._run("lsjson", self.remote_path, "--recursive", "--files-only", timeout=self.list_timeout)
        try:
            rows = json.loads(done.stdout or "[]")
        except ValueError as exc:
            raise RemoteCommandError("lsjson", self.remote_path, 0, f"unparseable listing: {exc}") from exc
        found: dict[str, _RemoteArchive] = {}
        for row in rows:
            relative = str(row.get("Path") or "")
            match = ARCHIVE_NAME_RE.match(relative.rsplit("/", 1)[-1])
            if match is None:
                continue
            day, hour = match.group(1), match.group(2)
            size = row.get("Size")
            found[f"{day}T{hour}"] = _RemoteArchive(relative, int(size) if isinstance(size, int) and size >= 0 else None)
        self._listing = found
        return found

    def _run(self, operation: str, *args: str, timeout: float) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run([self.binary, operation, *args], check=True, text=True, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise RemoteTimeout(operation, self.remote_path, f"no answer within {timeout:g}s") from exc
        except subprocess.CalledProcessError as exc:
            raise RemoteCommandError(operation, self.remote_path, exc.returncode, exc.stderr or "") from exc
        except OSError as exc:
            raise RemoteCommandError(operation, self.remote_path, 127, f"{self.binary}: {exc}") from exc


def open_source(spec: str, *, cache_dir: Path | None = None, venue: str | None = None) -> Source:
    """A source from what the operator typed: `rclone:<remote:path>`, a recorder root, or an archive-shaped directory."""

    if spec.startswith("rclone:"):
        return RcloneRemote(spec[len("rclone:") :], cache_dir, venue=venue)
    path = Path(spec)
    if not path.is_dir():
        raise ValueError(f"not a tape source: {spec}")
    if HostRoot.looks_like(path):
        return HostRoot(path, venue)
    if ArchiveDir.looks_like(path):
        return ArchiveDir(path, venue)
    raise ValueError(f"{spec} is neither a recorder root nor a directory of hour archives")


# ----------------------------------------------------------------- the rows


def iter_rows(
    source: Source,
    hours: Iterable[str],
    *,
    symbols: Iterable[str] | None = None,
    kinds: Iterable[str] | None = None,
    typed: bool = True,
    strict: bool = False,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Iterator[Any]:
    """Every row of the named hours in `local_receive_ts_ns` order, symbols merged.

    Streams enter the merge in member path order, which is symbol order, and a
    symbol's segments in segment order, so rows stamped the same nanosecond
    come out in that order. A line that does not parse is counted on
    `source.skipped_rows`, logged once per member with its first cause, and
    skipped; with `strict` it is a `TapeRowError` instead. `start_ns` and
    `end_ns` bound the rows by receive stamp, `end_ns` exclusive: a block
    member reads only the blocks that can hold the window, a JSON member
    stops at its end.
    """

    wanted = {symbol.upper() for symbol in symbols} if symbols else None
    kept = set(kinds) if kinds else None
    for hour in hours:
        members = [
            member
            for member in source.hour_members(hour)
            if member.symbol != META and (wanted is None or member.symbol in wanted)
        ]
        by_symbol: dict[str, list[Member]] = {}
        for member in members:
            by_symbol.setdefault(member.symbol, []).append(member)
        streams = [_symbol_rows(source, group, kept, typed, strict, start_ns, end_ns) for group in by_symbol.values()]
        try:
            for _, row in heapq.merge(*streams, key=lambda pair: pair[0]):
                yield row
        finally:
            for stream in streams:
                stream.close()


def _symbol_rows(
    source: Source,
    members: Sequence[Member],
    kinds: set[str] | None,
    typed: bool,
    strict: bool,
    start_ns: int | None,
    end_ns: int | None,
) -> Generator[tuple[int, Any], None, None]:
    """One symbol's segments end to end; the next is opened only once the one before it runs out."""

    for member in members:
        yield from _member_rows(source, member, kinds, typed, strict, start_ns, end_ns)


class _Skips:
    """What one member's unreadable lines cost, said once when the member is done."""

    def __init__(self, source: Source, member: Member, strict: bool) -> None:
        self.source = source
        self.member = member
        self.strict = strict
        self.count = 0
        self.first: str | None = None

    def skip(self, line_number: int, exc: BaseException) -> None:
        if self.strict:
            raise TapeRowError(f"{self.member.path} line {line_number}: {type(exc).__name__}: {exc}") from exc
        self.source.skipped_rows += 1
        self.count += 1
        if self.first is None:
            self.first = f"line {line_number}: {type(exc).__name__}: {exc}"

    def report(self) -> None:
        if self.count:
            logger.warning("%s: skipped %d unreadable line(s); first at %s", self.member.path, self.count, self.first)


def _member_rows(
    source: Source,
    member: Member,
    kinds: set[str] | None,
    typed: bool,
    strict: bool,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Generator[tuple[int, Any], None, None]:
    if member.path.endswith(BLOCK_SUFFIX):
        yield from _block_member_rows(source, member, kinds, typed, strict, start_ns, end_ns)
        return
    stream = member.open()
    skips = _Skips(source, member, strict)
    try:
        for line_number, raw in enumerate(stream, start=1):
            if not raw.strip():
                continue
            try:
                obj = json.loads(raw)
                if not isinstance(obj, Mapping):
                    raise SchemaError("a tape line is not an object")
                if kinds is not None and obj.get("kind") not in kinds:
                    continue
                received = int(obj["local_receive_ts_ns"])
                if start_ns is not None and received < start_ns:
                    continue
                if end_ns is not None and received >= end_ns:
                    # The segment is in receive order: nothing after this line is inside the window.
                    break
                row: Row | dict[str, Any] = parse_row(obj) if typed else dict(obj)
            except (KeyError, TypeError, ValueError, SchemaError) as exc:
                skips.skip(line_number, exc)
                continue
            yield received, row
        skips.report()
    finally:
        stream.close()


def _block_member_rows(
    source: Source,
    member: Member,
    kinds: set[str] | None,
    typed: bool,
    strict: bool,
    start_ns: int | None,
    end_ns: int | None,
) -> Generator[tuple[int, Any], None, None]:
    """A block member's rows through its index: only the blocks the filters select are read.

    A block that fails its checks is skipped whole, its rows counted on
    `source.skipped_rows`, and the member's rows after it are still read;
    with `strict` it is a `TapeRowError`. A file without a valid index is the
    member's failure: skipped and logged, or refused under `strict`.
    """

    from market_tape.blocks import BlockCorrupt, BlockError, BlockReader

    handle = member.binary()
    skips = _Skips(source, member, strict)
    try:
        try:
            reader = BlockReader(handle, label=member.path)
        except BlockError as exc:
            if strict:
                raise TapeRowError(f"{member.path}: {exc}") from exc
            logger.warning("%s: skipped; %s", member.path, exc)
            return
        by_group: dict[int, list[Any]] = {}
        for entry in reader.select(kinds=kinds, start_ns=start_ns, end_ns=end_ns):
            by_group.setdefault(entry.group, []).append(entry)
        ordinal = 0
        for group in sorted(by_group):
            streams = []
            for entry in by_group[group]:
                try:
                    streams.append(iter(reader.read_block(entry)))
                except BlockCorrupt as exc:
                    if strict:
                        raise TapeRowError(f"{member.path}: {exc}") from exc
                    source.skipped_rows += exc.entry.rows
                    logger.warning("%s: skipped one %s block of %d rows; %s", member.path, entry.kind, entry.rows, exc)
            for _, obj in heapq.merge(*streams, key=lambda pair: pair[0]):
                ordinal += 1
                received = int(obj["local_receive_ts_ns"])
                if start_ns is not None and received < start_ns:
                    continue
                if end_ns is not None and received >= end_ns:
                    continue
                row: Row | dict[str, Any]
                if typed:
                    try:
                        row = parse_row(obj)
                    except SchemaError as exc:
                        skips.skip(ordinal, exc)
                        continue
                else:
                    row = obj
                yield received, row
        skips.report()
    finally:
        handle.close()


def iter_snapshots(source: Source, hours: Iterable[str], *, strict: bool = False) -> Iterator[dict[str, Any]]:
    """The venue's instrument and ticker tables as they were recorded in those hours.

    A `_meta` line that is not a snapshot payload is counted on
    `source.skipped_rows` and logged, or refused with `strict`.
    """

    for hour in hours:
        for member in source.hour_members(hour):
            if member.symbol != META or _is_coverage(member):
                continue
            stream = member.open()
            skips = _Skips(source, member, strict)
            try:
                for line_number, raw in enumerate(stream, start=1):
                    if not raw.strip():
                        continue
                    try:
                        payload = json.loads(raw)
                        if not isinstance(payload, dict) or payload.get("kind") not in SNAPSHOT_KINDS:
                            raise SchemaError("a _meta line is not an instruments or tickers snapshot")
                    except (ValueError, SchemaError) as exc:
                        skips.skip(line_number, exc)
                        continue
                    yield payload
                skips.report()
            finally:
                stream.close()


def iter_coverage(source: Source, hours: Iterable[str], *, strict: bool = False) -> Iterator[dict[str, Any]]:
    """The recorder's own coverage records for those hours, in member order.

    One record per (hour, recorder process): a restart mid-hour leaves two, and
    the hour's time inside neither window is `recorder_down`.
    """

    for hour in hours:
        for member in source.hour_members(hour):
            if not _is_coverage(member):
                continue
            stream = member.open()
            skips = _Skips(source, member, strict)
            try:
                for line_number, raw in enumerate(stream, start=1):
                    if not raw.strip():
                        continue
                    try:
                        payload = json.loads(raw)
                        if not isinstance(payload, dict) or payload.get("kind") != COVERAGE_RECORD:
                            raise SchemaError("a coverage member line is not a coverage record")
                    except (ValueError, SchemaError) as exc:
                        skips.skip(line_number, exc)
                        continue
                    yield payload
                skips.report()
            finally:
                stream.close()
