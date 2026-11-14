"""Which of an upload's connections to the storage box has stopped moving.

A connection to the box keeps the pace it opened with. Some live for hours at
tens of KiB/s, the host retransmitting about a segment a second, while the next
one opened often carries several hundred. `rclone --timeout` cannot see that:
the connection is never idle. And rclone keeps nothing of an upload it did not
finish, so an hour's tar that crawls for the whole of its deadline is lost
whole. `pack` therefore runs each upload as a child in a session of its own
(`market_tape/pack.py::Rclone.upload`), looks at the child's connections every
few seconds, and stops it when one is a crawl; the upload then goes again on a
fresh connection.

A connection is a crawl when bytes written to it have waited unsent at every
look through `Watch.window` seconds while the box acknowledged fewer than
`Watch.floor` bytes a second of them. A connection with nothing unsent is
waiting on the box (rclone's listing, the box's hashing), never a crawl, however
long that lasts; a connection that is sending at a good pace is never stopped for
its length. The facts come from `ss -tinp` (iproute2): `bytes_acked` is what the
box has acknowledged on the connection so far, `notsent` what has been written
and not yet sent.

| Condition | Result |
| --- | --- |
| a connection held unsent bytes through the window under the floor | `CrawlJudge.look` returns why, once; the caller stops the upload |
| a connection with nothing unsent, or one at or over the floor | never judged a crawl |
| `Watch.window` of 0 | `Watch.enabled` is false; nothing is read |
| `ss` not installed, fails, or does not answer within `SS_TIMEOUT_SECONDS` | `connections` raises `OSError`; the caller runs the upload unwatched |
| a process of the session that has already exited | its connections are not listed |

Standard library only.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

#: A look at `ss` that has not answered by then is a failed one.
SS_TIMEOUT_SECONDS = 10.0
#: How long a stopped upload has to leave on SIGTERM before SIGKILL.
STOP_GRACE_SECONDS = 30.0
#: One TCP connection's reading: (bytes the box acknowledged, bytes written and not yet sent).
Reading = tuple[int, int]


@dataclass(frozen=True)
class Watch:
    """How an upload attempt is watched: a connection that held unsent bytes
    through `window` seconds while the box acknowledged under `floor` bytes a
    second is a crawl. A `window` of 0 watches nothing."""

    window: float = 0.0
    floor: float = 100 * 1024.0

    @property
    def enabled(self) -> bool:
        return self.window > 0

    @property
    def look_seconds(self) -> float:
        """How often the connections are read: a quarter window, at most 5 s."""

        return min(5.0, self.window / 4)


def parse_ss(text: str, owned: Callable[[int], bool]) -> dict[str, Reading]:
    """Each connection `ss -tinp` listed that a process `owned` holds, keyed by
    its local and peer address, with its (bytes_acked, notsent).

    A connection is two lines: the socket's (state, queues, addresses and the
    `users:(("rclone",pid=N,fd=M))` that hold it), then the indented one of
    `tcp_info` counters. A counter `ss` leaves out reads 0.
    """

    found: dict[str, Reading] = {}
    name: str | None = None
    for line in text.splitlines():
        if not line[:1].isspace():
            words = line.split()
            mine = any(owned(int(pid)) for pid in re.findall(r"pid=(\d+)", line))
            name = " ".join(words[3:5]) if mine and len(words) >= 5 else None
        elif name:
            counts = dict(re.findall(r"\b(bytes_acked|notsent):(\d+)", line))
            found[name] = (int(counts.get("bytes_acked", 0)), int(counts.get("notsent", 0)))
    return found


def connections(session: int) -> dict[str, Reading]:
    """Each TCP connection a process of `session` holds, read now.

    OSError when `ss` is missing, exits nonzero or does not answer in time.
    """

    def owned(pid: int) -> bool:
        try:
            return os.getsid(pid) == session
        except OSError:
            return False

    try:
        shown = subprocess.run(["ss", "-tinp"], capture_output=True, timeout=SS_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as exc:
        raise OSError("ss did not answer") from exc
    if shown.returncode != 0:
        raise OSError(f"ss exited {shown.returncode}")
    return parse_ss(shown.stdout.decode("utf-8", "replace"), owned)


@dataclass
class CrawlJudge:
    """The decision, with the clock and the readings handed in: `look` is
    called at each look with the time and what `connections` read."""

    watch: Watch
    _waiting: dict[str, list[tuple[float, int]]] = field(default_factory=dict)

    def look(self, now: float, seen: Mapping[str, Reading]) -> str | None:
        """Why the upload is a crawl, or None.

        A connection is tracked from the look it first held unsent bytes; a look
        at which it holds none, or at which it is gone, forgets it. The pace is
        the box's acknowledgements across the tracked looks, the oldest dropped
        once it is a whole window behind the newest.
        """

        for name in list(self._waiting):
            if seen.get(name, (0, 0))[1] == 0:
                del self._waiting[name]
        for name, (acked, unsent) in seen.items():
            if not unsent:
                continue
            looks = self._waiting.setdefault(name, [])
            looks.append((now, acked))
            while len(looks) > 1 and looks[1][0] <= now - self.watch.window:
                looks.pop(0)
            since, then = looks[0]
            pace = (acked - then) / max(now - since, 1e-9)
            if now - since >= self.watch.window and pace < self.watch.floor:
                return f"a connection crawled at {pace / 1024:.0f} KiB/s for {now - since:.0f} s"
        return None


def stop(child: subprocess.Popen[bytes], grace: float = STOP_GRACE_SECONDS) -> None:
    """End the child's whole session: SIGTERM, SIGKILL after `grace` seconds,
    and a last SIGKILL for any process of the group that outlived its leader."""

    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=wait)
            break
        except subprocess.TimeoutExpired:
            continue
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

