"""`python -m market_tape fetch`: the symbol-hours a plan names, range-read from the storage box.

The recorders run on the capture host and ship each finished hour as one tar
(`market_tape pack`); a host that reads tape but records none pulls back only
what it will read. The line to the box is slow (~0.4 MB/s a stream) and an
hour's tar is hundreds of megabytes, so each hour's index sidecar
(`<day>T<HH>Z.tar.index.json`, `pack.build_index`) says where every member's
bytes sit, and each wanted member is read by range: `rclone cat --offset O
--count N` of the tar. Wanted members closer together than
`COALESCE_GAP_BYTES` share one read, so one rclone start serves a symbol's
consecutive segments.

```text
python -m market_tape fetch --remote-base REMOTE --plan PATH|- --root TAPE=DIR [--root ...]
    --state-dir DIR [--keep-hours N] [--owner USER:GROUP] [--timeout SECONDS]
    [--max-stream-bytes N] [--bwlimit-kibps N] [--rclone PATH]
```

`--bwlimit-kibps` hands every rclone call `--bwlimit=<N>K --buffer-size=0
--sftp-disable-concurrent-reads`: the host that fetches also receives its
market data on the same line, and a read at the line's rate queues that data
behind it.

The plan, from whatever reader wants the hours:
`{"schema": 1, "tapes": {"<tape>": [{"day": "YYYY-MM-DD", "hour": "HH", "symbols": [...]}]}}`.
Each planned hour of a tape lands under that tape's root as
`<root>/<day>/<HH>/<SYMBOL>/segment-*.jsonl.zst` (and `.lmtb` where the tar
holds one), with the hour's coverage records under `_meta/`: a recorder root's
layout, which every reader takes as a SOURCE. A root named after its tape
(`.../bybit-linear`) names its venue. Newest hours go first.

| Hour status | When |
| --- | --- |
| `done` | every wanted member is on disk, verified; `missing` names planned symbols the tar holds nothing for |
| `pending` | the tar is not on the box yet (it lands ~10 minutes after the hour) |
| `failed` | a member failed its size or SHA-256, a read failed, the index is unreadable, or the plan's tape has no `--root` |
| `skipped` | `no_index_too_large`: packed before indexes, and bigger than `--max-stream-bytes`; `past_keep_hours`: `--keep-hours` would delete it |
| `left` | the `--timeout` budget ran out before the hour finished |

An hour without an index streams the whole tar through `rclone cat` into
`tarfile`, writing only wanted members (verified against `MANIFEST.json`) and
stopping once it has them. Every member is written atomically (`.<name>.part`,
fsync, rename; directories 0750, files 0640, chowned to `--owner`) and
recorded in `<state-dir>/fetched-members.jsonl`; a member on disk at the
index's size whose SHA-256 that ledger holds is not read again. An index is
kept in `<state-dir>/indexes/` and read from there on later runs, until a
member it names fails. `--keep-hours` deletes each root's hours that ended
longer ago than that, with their ledger rows and kept indexes.

Stdout is one JSON line: `{"fetched", "skipped", "failed", "bytes"}` count
members (a `failed` hour that never listed its members counts one),
`"pending"`, `"left"` and `"pruned"` count hours, `"seconds"` is the run's wall
time, and `"hours"` holds one `{"tape", "day", "hour", "status", "reason",
"fetched", "skipped", "failed", "bytes", "missing"}` per planned hour. The exit
is 0 once the run starts, whatever its hours did; 2 for arguments or a plan
it cannot read.
"""

from __future__ import annotations

import argparse
import fcntl
import grp
import hashlib
import io
import json
import os
import pwd
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Callable, Iterable, Mapping

from market_tape.pack import INDEX_KIND, INDEX_SCHEMA, INDEX_SUFFIX

PLAN_SCHEMA = 1
DEFAULT_TIMEOUT_SECONDS = 1800.0
DEFAULT_MAX_STREAM_BYTES = 2 * 1024**3
#: Two wanted members at most this far apart are read in one range: at
#: ~0.4 MB/s the gap costs less than another rclone start and SSH handshake.
COALESCE_GAP_BYTES = 256 * 1024
CHUNK_BYTES = 1024 * 1024
LEDGER_NAME = "fetched-members.jsonl"
LOCK_NAME = "fetch.lock"
INDEX_CACHE = "indexes"
DIRECTORY_MODE = 0o750
FILE_MODE = 0o640
#: rclone's exit status for a directory that does not exist.
RCLONE_DIRECTORY_NOT_FOUND = 3
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
HOUR_RE = re.compile(r"^(?:[01]\d|2[0-3])$")
SYMBOL_RE = re.compile(r"^[A-Z0-9]+$")
TAPE_RE = re.compile(r"^[A-Za-z0-9_-]+$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
#: The members a fetch writes: a symbol's segments, and the hour's coverage records.
SEGMENT_RE = re.compile(r"^([A-Z0-9]+)/segment-\d{6}\.(?:jsonl\.zst|lmtb)$")
COVERAGE_RE = re.compile(r"^_meta/coverage-\d{2}-\d{8}T\d{6}Z\.json\.zst$")

STATUS_DONE = "done"
STATUS_PENDING = "pending"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUS_LEFT = "left"


class UsageError(Exception):
    """The run cannot start: an argument or the plan is not usable."""


class OutOfTime(Exception):
    """The run's `--timeout` budget is spent."""


class RcloneError(Exception):
    def __init__(self, operation: str, returncode: int, stderr: str) -> None:
        super().__init__(f"rclone {operation} exit {returncode}: {stderr.strip()[-400:] or 'no output'}")
        self.returncode = returncode


# ------------------------------------------------------------------ the plan


@dataclass(frozen=True)
class PlannedHour:
    tape: str
    day: str
    hour: str
    symbols: frozenset[str]

    @property
    def name(self) -> str:
        return f"{self.day}T{self.hour}Z"

    @property
    def end(self) -> float:
        """Unix seconds at which the hour was over."""

        start = datetime.fromisoformat(self.day).replace(tzinfo=timezone.utc).timestamp()
        return start + (int(self.hour) + 1) * 3600.0

    @property
    def remote_dir(self) -> str:
        year, month, day = self.day.split("-")
        return f"{year}/{month}/{day}"


def parse_plan(payload: Any) -> list[PlannedHour]:
    """The plan's hours, one per (tape, day, hour), symbols merged; UsageError when it is not a plan."""

    if not isinstance(payload, Mapping) or payload.get("schema") != PLAN_SCHEMA:
        raise UsageError(f"the plan is not a schema {PLAN_SCHEMA} tape plan")
    tapes = payload.get("tapes")
    if not isinstance(tapes, Mapping):
        raise UsageError("the plan has no tapes table")
    merged: dict[tuple[str, str, str], set[str]] = {}
    for tape, hours in tapes.items():
        if not isinstance(tape, str) or not TAPE_RE.match(tape) or not isinstance(hours, list):
            raise UsageError(f"the plan's tape {tape!r} is not a tape name with a list of hours")
        for entry in hours:
            if not isinstance(entry, Mapping):
                raise UsageError(f"the plan's {tape} entry {entry!r} is not an hour")
            day, hour, symbols = entry.get("day"), entry.get("hour"), entry.get("symbols")
            if not isinstance(day, str) or not DAY_RE.match(day) or not isinstance(hour, str) or not HOUR_RE.match(hour):
                raise UsageError(f"the plan's {tape} entry names day {day!r} hour {hour!r}, not YYYY-MM-DD and HH")
            try:
                datetime.fromisoformat(day)
            except ValueError as exc:
                raise UsageError(f"the plan's {tape} day {day!r} is not a date") from exc
            if not isinstance(symbols, list) or not all(isinstance(s, str) and SYMBOL_RE.match(s) for s in symbols):
                raise UsageError(f"the plan's {tape} {day}T{hour} symbols are not a list of symbols: {symbols!r}")
            merged.setdefault((tape, day, hour), set()).update(symbols)
    return [
        PlannedHour(tape, day, hour, frozenset(symbols))
        for (tape, day, hour), symbols in sorted(merged.items(), key=lambda item: (item[0][1], item[0][2], item[0][0]), reverse=True)
    ]


# ------------------------------------------------------------------ members


@dataclass(frozen=True)
class Member:
    """One file in an hour's tar: where its bytes sit, and what they hash to.
    `offset` is None for a member known only from `MANIFEST.json`."""

    path: str
    offset: int | None
    size: int
    sha256: str

    @property
    def symbol(self) -> str | None:
        match = SEGMENT_RE.match(self.path)
        return match.group(1) if match else None


def wanted(path: str, symbols: frozenset[str]) -> bool:
    match = SEGMENT_RE.match(path)
    if match is not None:
        return match.group(1) in symbols
    return COVERAGE_RE.match(path) is not None


def parse_index(payload: Any, planned: PlannedHour) -> list[Member]:
    """The index's members, checked against the hour it must describe; ValueError when it is not that index."""

    if not isinstance(payload, Mapping) or payload.get("kind") != INDEX_KIND or payload.get("schema") != INDEX_SCHEMA:
        raise ValueError(f"not a schema {INDEX_SCHEMA} {INDEX_KIND}")
    if payload.get("name") != planned.name or payload.get("tape") not in (None, planned.tape):
        raise ValueError(f"it describes {payload.get('tape')}/{payload.get('name')}, not {planned.tape}/{planned.name}")
    total = payload.get("tar_bytes")
    rows = payload.get("members")
    if not isinstance(total, int) or not isinstance(rows, list):
        raise ValueError("it has no tar_bytes or no members list")
    members: list[Member] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError(f"member {row!r} is not a table")
        path, offset, size, digest = row.get("path"), row.get("offset"), row.get("bytes"), row.get("sha256")
        if (
            not isinstance(path, str)
            or not isinstance(offset, int)
            or not isinstance(size, int)
            or not isinstance(digest, str)
            or offset < 0
            or size < 0
            or offset + size > total
            or not SHA256_RE.match(digest)
        ):
            raise ValueError(f"member {row!r} is not a path, an offset and size inside the tar, and a SHA-256")
        members.append(Member(path, offset, size, digest))
    return members


# ------------------------------------------------------------------ the box


class Rclone:
    """The rclone calls a fetch makes, each bounded by the run's deadline and
    capped at `bwlimit_kibps` when one is set. rclone reads its config from
    `RCLONE_CONFIG`, as the unit sets it."""

    def __init__(self, binary: str, bwlimit_kibps: int | None = None) -> None:
        self.binary = binary
        # rclone charges a read to the cap as it hands it on, above two
        # read-aheads that fill at the line's rate: the transfer's buffer
        # (`--buffer-size`, 16 MiB) and the SFTP reader's window of concurrent
        # requests (64 of 32 KiB, run on past a range's end until it closes).
        # Capped, it reads one 32 KiB request at a time as the copy asks.
        self.flags = (
            [f"--bwlimit={bwlimit_kibps}K", "--buffer-size=0", "--sftp-disable-concurrent-reads"]
            if bwlimit_kibps
            else []
        )

    def listing(self, remote_dir: str, deadline: float) -> dict[str, int | None] | None:
        """File name to size in one remote directory; None when the directory is not there."""

        try:
            done = self._run("lsjson", remote_dir, "--files-only", deadline=deadline)
        except RcloneError as exc:
            if exc.returncode == RCLONE_DIRECTORY_NOT_FOUND:
                return None
            raise
        rows = json.loads(done or b"[]")
        return {
            str(row.get("Name")): (row["Size"] if isinstance(row.get("Size"), int) and row["Size"] >= 0 else None)
            for row in rows
            if isinstance(row, dict)
        }

    def cat(self, remote_path: str, deadline: float) -> bytes:
        return self._run("cat", remote_path, deadline=deadline)

    def _run(self, operation: str, *args: str, deadline: float) -> bytes:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OutOfTime()
        try:
            done = subprocess.run(
                [self.binary, operation, *args, *self.flags],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                timeout=remaining,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise OutOfTime() from exc
        except OSError as exc:
            raise RcloneError(operation, 127, f"{self.binary}: {exc}") from exc
        if done.returncode != 0:
            raise RcloneError(operation, done.returncode, done.stderr.decode(errors="replace"))
        return done.stdout

    def stream(self, args: list[str], deadline: float, reader: Callable[[IO[bytes]], bool]) -> None:
        """Run `rclone <args>` and hand its stdout to `reader`, killing it at the deadline.

        `reader` returns True when it has what it wants before the output
        ends: the process is then killed and its exit is not an error.
        OutOfTime when the deadline cut the output short; RcloneError when
        rclone failed; whatever `reader` raised otherwise."""

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OutOfTime()
        with tempfile.TemporaryFile() as errors:
            try:
                process = subprocess.Popen(
                    [self.binary, *args, *self.flags], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=errors
                )
            except OSError as exc:
                raise RcloneError(args[0], 127, f"{self.binary}: {exc}") from exc
            expired = threading.Event()

            def expire() -> None:
                expired.set()
                process.kill()

            timer = threading.Timer(remaining, expire)
            timer.daemon = True
            timer.start()
            failure: BaseException | None = None
            early = finished = False
            try:
                assert process.stdout is not None
                early = reader(process.stdout)
                finished = True
                # A reader that stopped where the bytes did not (a tar read
                # to a header it cannot parse ends as if at the archive's end)
                # stopped early all the same.
                early = early or bool(process.stdout.read(1))
            except (OSError, EOFError, tarfile.TarError, ValueError) as exc:
                failure = exc
            finally:
                # Anything but a reader that read to the end leaves rclone
                # writing into a pipe nobody reads.
                if early or not finished:
                    process.kill()
                code = process.wait()
                timer.cancel()
                assert process.stdout is not None
                process.stdout.close()
            if expired.is_set():
                raise OutOfTime()
            if code != 0 and not early:
                errors.seek(0)
                raise RcloneError(args[0], code, errors.read().decode(errors="replace"))
            if failure is not None:
                raise failure


# ----------------------------------------------------------------- the disk


def resolve_owner(text: str | None) -> tuple[int, int] | None:
    """`USER:GROUP`, by name or number, as `(uid, gid)`; UsageError when either is unknown."""

    if text is None:
        return None
    user, separator, group = text.partition(":")
    if not separator or not user or not group:
        raise UsageError(f"--owner wants USER:GROUP, got {text!r}")
    try:
        uid = int(user) if user.isdigit() else pwd.getpwnam(user).pw_uid
        gid = int(group) if group.isdigit() else grp.getgrnam(group).gr_gid
    except KeyError as exc:
        raise UsageError(f"--owner {text!r}: no such user or group") from exc
    return uid, gid


@dataclass
class Disk:
    """Where a fetch writes: the modes and owner of everything it creates."""

    owner: tuple[int, int] | None = None

    def directory(self, path: Path, top: Path) -> None:
        """Make `path` and every missing directory between it and `top` (itself made if missing)."""

        missing: list[Path] = []
        probe = path
        while not probe.is_dir():
            missing.append(probe)
            if probe == top:
                break
            probe = probe.parent
        for directory in reversed(missing):
            try:
                directory.mkdir(mode=DIRECTORY_MODE)
            except FileExistsError:
                continue
            self._own(directory, DIRECTORY_MODE)

    def _own(self, path: Path, mode: int) -> None:
        os.chmod(path, mode)
        if self.owner is not None:
            os.chown(path, *self.owner)


class MemberFile:
    """One member on its way to disk: a `.part` beside its final name, hashed as
    it is written, renamed into place only whole and matching its digest."""

    def __init__(self, member: Member, target: Path, disk: Disk) -> None:
        self.member = member
        self.target = target
        self.temporary = target.with_name(f".{target.name}.part")
        self.disk = disk
        self.hasher = hashlib.sha256()
        self.written = 0
        self.descriptor = os.open(self.temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, FILE_MODE)

    @property
    def remaining(self) -> int:
        return self.member.size - self.written

    def write(self, data: bytes | memoryview) -> None:
        if len(data) > self.remaining:
            raise OSError(f"{self.member.path}: more bytes than its {self.member.size}")
        self.hasher.update(data)
        view = memoryview(data)
        while view:
            view = view[os.write(self.descriptor, view) :]
        self.written += len(data)

    def finish(self) -> str | None:
        """Rename into place; the reason it was refused otherwise, the part removed."""

        try:
            if self.written != self.member.size:
                return f"{self.written} of {self.member.size} bytes arrived"
            digest = self.hasher.hexdigest()
            if digest != self.member.sha256:
                return f"sha256 {digest} is not the index's {self.member.sha256}"
            os.fsync(self.descriptor)
            os.fchmod(self.descriptor, FILE_MODE)
            if self.disk.owner is not None:
                os.fchown(self.descriptor, *self.disk.owner)
            os.close(self.descriptor)
            self.descriptor = -1
            os.replace(self.temporary, self.target)
            directory = os.open(self.target.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return None
        finally:
            self.discard()

    def discard(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1
        self.temporary.unlink(missing_ok=True)


# ------------------------------------------------------------------ the run


@dataclass
class HourReport:
    tape: str
    day: str
    hour: str
    status: str = STATUS_DONE
    reason: str | None = None
    fetched: int = 0
    skipped: int = 0
    failed: int = 0
    bytes: int = 0
    missing: list[str] = field(default_factory=list)

    def fail(self, reason: str) -> None:
        self.status = STATUS_FAILED
        self.reason = self.reason or reason

    def row(self) -> dict[str, Any]:
        return {
            "tape": self.tape,
            "day": self.day,
            "hour": self.hour,
            "status": self.status,
            "reason": self.reason,
            "fetched": self.fetched,
            "skipped": self.skipped,
            "failed": self.failed,
            "bytes": self.bytes,
            "missing": self.missing,
        }


def _say(message: str) -> None:
    print(f"market tape fetch: {message}", file=sys.stderr)


class Fetch:
    """One run: the plan's hours from `remote_base` into `roots`, within `deadline`."""

    def __init__(
        self,
        *,
        remote_base: str,
        roots: Mapping[str, Path],
        state_dir: Path,
        rclone: Rclone,
        deadline: float,
        disk: Disk,
        max_stream_bytes: int = DEFAULT_MAX_STREAM_BYTES,
        keep_hours: float | None = None,
        now: float | None = None,
    ) -> None:
        self.remote_base = remote_base.rstrip("/")
        self.roots = dict(roots)
        self.state_dir = state_dir
        self.rclone = rclone
        self.deadline = deadline
        self.disk = disk
        self.max_stream_bytes = max_stream_bytes
        self.keep_hours = keep_hours
        self.now = time.time() if now is None else now
        self.ledger_path = state_dir / LEDGER_NAME
        self.ledger: dict[tuple[str, str], str] = {}
        self.listings: dict[tuple[str, str], dict[str, int | None] | None] = {}

    # ------------------------------------------------------------ ledger

    def load_ledger(self) -> None:
        self.ledger = {}
        if not self.ledger_path.exists():
            return
        for line in self.ledger_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and isinstance(row.get("path"), str) and isinstance(row.get("sha256"), str):
                self.ledger[(str(row.get("tape")), row["path"])] = row["sha256"]

    def _record(self, tape: str, relative: str, member: Member) -> None:
        row = {
            "tape": tape,
            "path": relative,
            "bytes": member.size,
            "sha256": member.sha256,
            "fetched_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self.ledger[(tape, relative)] = member.sha256

    def _present(self, tape: str, relative: str, member: Member, root: Path) -> bool:
        try:
            size = (root / relative).stat().st_size
        except OSError:
            return False
        return size == member.size and self.ledger.get((tape, relative)) == member.sha256

    # ------------------------------------------------------------- prune

    def past_window(self, day: str, hour: str) -> bool:
        if self.keep_hours is None:
            return False
        end = datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() + (int(hour) + 1) * 3600.0
        return self.now >= end + self.keep_hours * 3600.0

    def prune(self) -> int:
        """Delete every root's hours past `--keep-hours`, their ledger rows and kept indexes; the hours deleted."""

        if self.keep_hours is None:
            return 0
        gone: set[tuple[str, str]] = set()
        for tape, root in self.roots.items():
            if not root.is_dir():
                continue
            for day_dir in sorted(path for path in root.iterdir() if path.is_dir() and DAY_RE.match(path.name)):
                for hour_dir in sorted(path for path in day_dir.iterdir() if path.is_dir() and HOUR_RE.match(path.name)):
                    if self.past_window(day_dir.name, hour_dir.name):
                        shutil.rmtree(hour_dir)
                        gone.add((tape, f"{day_dir.name}/{hour_dir.name}/"))
                try:
                    day_dir.rmdir()
                except OSError:
                    pass
        cache = self.state_dir / INDEX_CACHE
        for kept in sorted(cache.glob(f"*/*{INDEX_SUFFIX}")) if cache.is_dir() else []:
            match = re.match(r"^(\d{4}-\d{2}-\d{2})T(\d{2})Z\.tar", kept.name)
            if match and self.past_window(match.group(1), match.group(2)):
                kept.unlink(missing_ok=True)
        if gone:
            self._compact(gone)
        return len(gone)

    def _compact(self, gone: set[tuple[str, str]]) -> None:
        if not self.ledger_path.exists():
            return
        kept_lines = []
        for line in self.ledger_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            path = str(row.get("path") or "") if isinstance(row, dict) else ""
            if any(row.get("tape") == tape and path.startswith(prefix) for tape, prefix in gone):
                continue
            kept_lines.append(line)
        temporary = self.ledger_path.with_name(f".{self.ledger_path.name}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n" for line in kept_lines))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.ledger_path)
        self.load_ledger()

    # ------------------------------------------------------------- hours

    def run(self, plan: list[PlannedHour]) -> list[HourReport]:
        reports = [HourReport(hour.tape, hour.day, hour.hour) for hour in plan]
        for planned, report in zip(plan, reports):
            if time.monotonic() >= self.deadline:
                report.status = STATUS_LEFT
                continue
            try:
                self._hour(planned, report)
            except OutOfTime:
                report.status = STATUS_LEFT
            except (RcloneError, OSError, ValueError, EOFError, tarfile.TarError) as exc:
                report.fail(str(exc) or type(exc).__name__)
                report.failed = max(report.failed, 1)
        return reports

    def _hour(self, planned: PlannedHour, report: HourReport) -> None:
        if self.past_window(planned.day, planned.hour):
            report.status, report.reason = STATUS_SKIPPED, "past_keep_hours"
            return
        root = self.roots.get(planned.tape)
        if root is None:
            report.fail("no_root")
            report.failed = 1
            return
        tar = f"{self.remote_base}/{planned.tape}/{planned.remote_dir}/{planned.name}.tar"
        cached = self.state_dir / INDEX_CACHE / planned.tape / f"{planned.name}.tar{INDEX_SUFFIX}"
        members: list[Member] | None = None
        if cached.is_file():
            try:
                members = parse_index(json.loads(cached.read_bytes()), planned)
            except ValueError as exc:
                _say(f"{planned.tape} {planned.name}: the kept index is refused ({exc}); reading the box's")
                cached.unlink(missing_ok=True)
        if members is None:
            listing = self._listing(planned)
            if listing is None or f"{planned.name}.tar" not in listing:
                report.status = STATUS_PENDING
                return
            if f"{planned.name}.tar{INDEX_SUFFIX}" not in listing:
                self._streamed(planned, report, root, tar, listing.get(f"{planned.name}.tar"))
                return
            raw = self.rclone.cat(f"{tar}{INDEX_SUFFIX}", self.deadline)
            try:
                members = parse_index(json.loads(raw), planned)
            except ValueError as exc:
                report.fail(f"index: {exc}")
                report.failed = 1
                return
            _keep_index(cached, raw)
        self._indexed(planned, report, root, tar, members)
        if report.status == STATUS_FAILED:
            # A kept index that led to a failure is read from the box again next run.
            cached.unlink(missing_ok=True)

    def _listing(self, planned: PlannedHour) -> dict[str, int | None] | None:
        key = (planned.tape, planned.remote_dir)
        if key not in self.listings:
            self.listings[key] = self.rclone.listing(
                f"{self.remote_base}/{planned.tape}/{planned.remote_dir}", self.deadline
            )
        return self.listings[key]

    def _plan_members(
        self, planned: PlannedHour, report: HourReport, root: Path, members: Iterable[Member]
    ) -> list[Member]:
        """The wanted members not already on disk; counts the skipped and names the missing."""

        held = {member.symbol for member in members if member.symbol}
        report.missing = sorted(planned.symbols - held)
        todo: list[Member] = []
        for member in members:
            if not wanted(member.path, planned.symbols):
                continue
            if self._present(planned.tape, self._relative(planned, member), member, root):
                report.skipped += 1
            else:
                todo.append(member)
        return todo

    @staticmethod
    def _relative(planned: PlannedHour, member: Member) -> str:
        return f"{planned.day}/{planned.hour}/{member.path}"

    def _open(self, planned: PlannedHour, member: Member, root: Path) -> MemberFile:
        target = root / self._relative(planned, member)
        self.disk.directory(target.parent, root)
        return MemberFile(member, target, self.disk)

    def _landed(self, planned: PlannedHour, report: HourReport, handle: MemberFile) -> None:
        refused = handle.finish()
        if refused is None:
            self._record(planned.tape, self._relative(planned, handle.member), handle.member)
            report.fetched += 1
            report.bytes += handle.member.size
            return
        report.failed += 1
        report.fail(f"{handle.member.path}: {refused}")
        _say(f"{planned.tape} {planned.name} {handle.member.path}: refused, {refused}")

    # ------------------------------------------------------ by the index

    def _indexed(self, planned: PlannedHour, report: HourReport, root: Path, tar: str, members: list[Member]) -> None:
        todo = sorted(self._plan_members(planned, report, root, members), key=lambda member: member.offset or 0)
        runs: list[list[Member]] = []
        for member in todo:
            if runs:
                last = runs[-1][-1]
                assert last.offset is not None and member.offset is not None
                if member.offset - (last.offset + last.size) <= COALESCE_GAP_BYTES:
                    runs[-1].append(member)
                    continue
            runs.append([member])
        for run in runs:
            self._range(planned, report, root, tar, run)

    def _range(self, planned: PlannedHour, report: HourReport, root: Path, tar: str, run: list[Member]) -> None:
        """One `rclone cat --offset --count` over consecutive members, each cut to its own file."""

        first, last = run[0], run[-1]
        assert first.offset is not None and last.offset is not None
        start, end = first.offset, last.offset + last.size
        pending = list(run)
        handle: MemberFile | None = None
        position = start

        def take(stream: IO[bytes]) -> bool:
            nonlocal handle, position
            while pending:
                member = pending[0]
                assert member.offset is not None
                if handle is None:
                    if position < member.offset:
                        gap = stream.read(min(CHUNK_BYTES, member.offset - position))
                        if not gap:
                            return False
                        position += len(gap)
                        continue
                    handle = self._open(planned, member, root)
                if handle.remaining:
                    chunk = stream.read(min(CHUNK_BYTES, handle.remaining))
                    if not chunk:
                        return False
                    handle.write(chunk)
                    position += len(chunk)
                    continue
                self._landed(planned, report, handle)
                handle = None
                pending.pop(0)
            # The range ends with its last member: rclone is done too.
            return False

        try:
            if end > start:
                self.rclone.stream(["cat", "--offset", str(start), "--count", str(end - start), tar], self.deadline, take)
            else:
                take(io.BytesIO())
        except RcloneError as exc:
            report.fail(str(exc))
        finally:
            if handle is not None:
                handle.discard()
                handle = None
        # What the read did not bring whole is refused, and read again next run.
        for member in pending:
            report.failed += 1
            report.fail(f"{member.path}: the read ended before it")

    # --------------------------------------------------- without an index

    def _streamed(self, planned: PlannedHour, report: HourReport, root: Path, tar: str, size: int | None) -> None:
        """An hour packed before indexes: the whole tar through `tarfile`, the wanted members kept."""

        if size is None or size > self.max_stream_bytes:
            report.status, report.reason = STATUS_SKIPPED, "no_index_too_large"
            return
        todo: dict[str, Member] = {}
        seen_manifest = False

        def take(stream: IO[bytes]) -> bool:
            nonlocal seen_manifest
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                for info in archive:
                    source = archive.extractfile(info) if info.isfile() else None
                    if not seen_manifest:
                        if info.name != "MANIFEST.json" or source is None:
                            raise ValueError("the tar does not open on MANIFEST.json")
                        seen_manifest = True
                        members = _manifest_members(json.loads(source.read()))
                        todo.update((member.path, member) for member in self._plan_members(planned, report, root, members))
                        if not todo:
                            return True
                        continue
                    member = todo.get(info.name)
                    if member is None or source is None:
                        continue
                    handle = self._open(planned, member, root)
                    try:
                        while chunk := source.read(CHUNK_BYTES):
                            handle.write(chunk)
                    except BaseException:
                        handle.discard()
                        raise
                    self._landed(planned, report, handle)
                    del todo[info.name]
                    if not todo:
                        return True
            return False

        try:
            self.rclone.stream(["cat", tar], self.deadline, take)
        except RcloneError as exc:
            report.fail(str(exc))
        if not seen_manifest:
            report.fail("the tar's MANIFEST.json never arrived")
            report.failed = max(report.failed, 1)
        for path in sorted(todo):
            report.failed += 1
            report.fail(f"{path}: not in the stream")


def _keep_index(cached: Path, raw: bytes) -> None:
    cached.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    temporary = cached.with_name(f".{cached.name}.tmp")
    temporary.write_bytes(raw)
    os.replace(temporary, cached)


def _manifest_members(payload: Any) -> list[Member]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("files"), list):
        raise ValueError("MANIFEST.json lists no files")
    members = []
    for row in payload["files"]:
        if not isinstance(row, Mapping):
            continue
        path, size, digest = row.get("path"), row.get("bytes"), row.get("sha256")
        if isinstance(path, str) and isinstance(size, int) and isinstance(digest, str) and SHA256_RE.match(digest):
            members.append(Member(path, None, size, digest))
    return members


# ------------------------------------------------------------------ the CLI


def parse_roots(texts: list[str]) -> dict[str, Path]:
    roots: dict[str, Path] = {}
    for text in texts:
        tape, separator, directory = text.partition("=")
        if not separator or not TAPE_RE.match(tape) or not directory:
            raise UsageError(f"--root wants TAPE=DIR, got {text!r}")
        if tape in roots:
            raise UsageError(f"--root names {tape} twice")
        roots[tape] = Path(directory).resolve()
    if not roots:
        raise UsageError("name at least one --root TAPE=DIR")
    return roots


def read_plan(text: str) -> list[PlannedHour]:
    try:
        raw = sys.stdin.read() if text == "-" else Path(text).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise UsageError(f"the plan {text} is not readable JSON: {exc}") from exc
    return parse_plan(payload)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="market_tape fetch", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--remote-base", required=True, help="rclone folder the tapes sit under, remote:path")
    parser.add_argument("--plan", required=True, help="the tape plan's JSON file, or - for stdin")
    parser.add_argument("--root", action="append", default=[], help="TAPE=DIR, where a tape's hours land; repeat per tape")
    parser.add_argument("--state-dir", type=Path, required=True, help="the fetched-member ledger, kept indexes and lock")
    parser.add_argument("--keep-hours", type=float, default=None, help="delete local hours that ended longer ago than this")
    parser.add_argument("--owner", default=None, help="USER:GROUP that owns what the fetch writes")
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="seconds the whole run may take; what is left is reported"
    )
    parser.add_argument(
        "--max-stream-bytes",
        type=int,
        default=DEFAULT_MAX_STREAM_BYTES,
        help="the largest tar without an index that is streamed whole (default 2 GiB)",
    )
    parser.add_argument(
        "--bwlimit-kibps", type=int, default=None, help="cap every rclone read at this many KiB/s (default: no cap)"
    )
    parser.add_argument("--rclone", default="rclone", help="the rclone binary; its config comes from RCLONE_CONFIG")
    parser.add_argument("--now", type=float, default=None, help=argparse.SUPPRESS)
    return parser


def _lock(state_dir: Path, deadline: float) -> IO[str] | None:
    """The state directory's lock, waited for until the deadline; None when it never came."""

    handle = (state_dir / LOCK_NAME).open("w")
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except OSError:
            if time.monotonic() + 1.0 >= deadline:
                handle.close()
                return None
            time.sleep(1.0)


def main(argv: list[str] | None = None) -> int:
    started = time.monotonic()
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        if ":" not in args.remote_base:
            raise UsageError("--remote-base must be an rclone remote, remote:path")
        if args.keep_hours is not None and args.keep_hours < 0:
            raise UsageError(f"--keep-hours must be zero or more, got {args.keep_hours}")
        if args.timeout <= 0 or args.max_stream_bytes < 0:
            raise UsageError("--timeout must be positive and --max-stream-bytes zero or more")
        if args.bwlimit_kibps is not None and args.bwlimit_kibps <= 0:
            raise UsageError(f"--bwlimit-kibps must be a positive whole number of KiB/s, got {args.bwlimit_kibps}")
        roots = parse_roots(args.root)
        disk = Disk(resolve_owner(args.owner))
        plan = read_plan(args.plan)
        args.state_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
        for root in roots.values():
            root.parent.mkdir(parents=True, exist_ok=True)
            disk.directory(root, root)
    except (UsageError, OSError) as exc:
        print(f"market tape fetch: {exc}", file=sys.stderr)
        return 2
    deadline = started + args.timeout
    fetch = Fetch(
        remote_base=args.remote_base,
        roots=roots,
        state_dir=args.state_dir,
        rclone=Rclone(args.rclone, args.bwlimit_kibps),
        deadline=deadline,
        disk=disk,
        max_stream_bytes=args.max_stream_bytes,
        keep_hours=args.keep_hours,
        now=args.now,
    )
    pruned = 0
    lock = _lock(args.state_dir, deadline)
    if lock is None:
        _say("another fetch holds the state directory's lock for the whole budget")
        reports = [HourReport(hour.tape, hour.day, hour.hour, status=STATUS_LEFT) for hour in plan]
    else:
        try:
            fetch.load_ledger()
            pruned = fetch.prune()
            reports = fetch.run(plan)
        finally:
            lock.close()
    summary = {
        "fetched": sum(report.fetched for report in reports),
        "skipped": sum(report.skipped for report in reports),
        "failed": sum(report.failed for report in reports),
        "pending": sum(1 for report in reports if report.status == STATUS_PENDING),
        "left": sum(1 for report in reports if report.status == STATUS_LEFT),
        "pruned": pruned,
        "bytes": sum(report.bytes for report in reports),
        "seconds": round(time.monotonic() - started, 3),
        "hours": [report.row() for report in reports],
    }
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
