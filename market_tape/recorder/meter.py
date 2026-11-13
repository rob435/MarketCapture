"""Every received byte, metered per tier and per feed.

Bytes are counted by key (`all`, `tier:<tier>`, `feed:<tier>:<feed class>`)
per minute over the last day, plus lifetime totals: the day is what the
budget projects a month from (`market_tape/recorder/budget.py`) and what
`status.json` `bytes` reports.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Mapping


MINUTE_NS = 60 * 1_000_000_000
DAY_NS = 24 * 60 * MINUTE_NS


class ByteMeter:
    """Bytes received, by key, per minute over the last day, plus lifetime totals."""

    def __init__(self, started_ns: int) -> None:
        self.started_ns = started_ns
        self.lock = threading.Lock()
        self.totals: dict[str, int] = {}
        self.minutes: dict[str, deque[tuple[int, int]]] = {}

    def add(self, key: str, count: int, now_ns: int) -> None:
        minute = now_ns // MINUTE_NS
        with self.lock:
            self.totals[key] = self.totals.get(key, 0) + count
            bucket = self.minutes.setdefault(key, deque())
            if bucket and bucket[-1][0] == minute:
                bucket[-1] = (minute, bucket[-1][1] + count)
            else:
                bucket.append((minute, count))
            while bucket and bucket[0][0] <= minute - 1440:
                bucket.popleft()

    def add_batch(self, counts: Mapping[str, int], now_ns: int) -> None:
        minute = now_ns // MINUTE_NS
        with self.lock:
            for key, count in counts.items():
                self.totals[key] = self.totals.get(key, 0) + count
                bucket = self.minutes.setdefault(key, deque())
                if bucket and bucket[-1][0] == minute:
                    bucket[-1] = (minute, bucket[-1][1] + count)
                else:
                    bucket.append((minute, count))
                while bucket and bucket[0][0] <= minute - 1440:
                    bucket.popleft()

    def last_day(self, key: str, now_ns: int) -> int:
        minute = now_ns // MINUTE_NS
        with self.lock:
            bucket = self.minutes.get(key)
            if not bucket:
                return 0
            return sum(count for stamp, count in bucket if stamp > minute - 1440)

    def window_ns(self, now_ns: int) -> int:
        """How much of the last day this meter has actually seen."""

        return max(0, min(now_ns - self.started_ns, DAY_NS))

    def keys(self, prefix: str) -> list[str]:
        with self.lock:
            return sorted(key for key in self.minutes if key.startswith(prefix))
