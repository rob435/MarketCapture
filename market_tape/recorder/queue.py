"""The capture queue: what the intake and the side lanes fill and the writer drains.

Frames cross from the intake to the writer through one bounded queue
(`FrameQueue`), which absorbs a writer slower than the wire for as long as its
bounds allow; past them the reader holds each shard's frames and, when they
have waited too long, drops them, counts them in the status file, and the
shard reconnects for fresh snapshots. Bounded twice, in items and in the
memory they hold (`queue_item_bytes`): a frame's size is the venue's, so an
item count alone lets a backlog of large book snapshots past the process's
memory ceiling. A socket frame is queued as the reader's event, the events
pipe's own bytes, so a backlog of a million frames is a million objects and
not several million.
"""

from __future__ import annotations

import queue
import struct
import time
from bisect import bisect_right
from itertools import accumulate
from typing import Any


LANES = "lanes"
#: A queue item's kind that carries no rows: `(CHAIN_RESET, topics, ns, LANES, 0)`,
#: queued by a shard before it (re)subscribes `topics`, so the writer tells the
#: adapter (`chain_reset`) in tape order: after the last frame of the old
#: subscription, ahead of the first of the new.
CHAIN_RESET = "chain_reset"
#: A queued chain reset's estimate per topic name.
QUEUE_TOPIC_BYTES = 64
#: What the writer takes. A socket frame is queued as the reader's event, the
#: events pipe's bytes (`reader.EVENT`, then the payload), one object whose
#: header holds its link and both stamps: a backlog of a million frames is a
#: million of these, and every small object it held besides (a tuple, two
#: stamps) was one more for pymalloc to leave scattered across arenas it
#: cannot give back. `("frame", raw, received_ns, tier, mono_ns)` is a frame
#: handed over whole, `("rows", rows, received_ns, LANES, 0)` rows a side lane
#: built itself. `mono_ns` is the host's monotonic clock at the same instant
#: as `received_ns`; 0 when none was read.
QueueItem = bytes | tuple[str, Any, int, str, int]
#: What a queued event holds beyond its `len()`, as `sys.getsizeof` reads it on
#: CPython 3.11 and 3.12: the bytes object's header 33, up to 15 of the
#: allocator's rounding to 16, and the deque's 8-byte slot.
QUEUE_EVENT_OVERHEAD_BYTES = 56
#: What a queued tuple holds beyond a frame's `len(raw)`, as `sys.getsizeof`
#: reads it on CPython 3.11: the 5-tuple 80, the bytes object's header 33,
#: `received_ns` 36, `mono_ns` 32, and the deque's 8-byte slot, 189 in all
#: (tracemalloc reads 188 per item), rounded up. The kind and tier strings are shared.
QUEUE_ITEM_OVERHEAD_BYTES = 192
#: A lane row's estimate. A one-row funding or account-ratio item, tuple, list,
#: row dict and values, reads 516-537 B under tracemalloc; this plus the item
#: overhead counts it at over twice that, room for a wider row.
QUEUE_ROW_BYTES = 1024
#: A queued event's length, kind and link: what the intake routes it by.
EVENT_HEAD = struct.Struct("<IBI")
#: A queued event's wall stamp, next after `EVENT_HEAD` in `reader.EVENT`.
EVENT_WALL = struct.Struct("<q")


def received_ns_of(item: QueueItem) -> int:
    if isinstance(item, bytes):
        return EVENT_WALL.unpack_from(item, EVENT_HEAD.size)[0]
    return item[2]


def queue_item_bytes(item: QueueItem | None) -> int:
    """The memory one queued item holds, as `FrameQueue`'s byte bound counts it."""

    if item is None:
        return 0
    if isinstance(item, bytes):
        return QUEUE_EVENT_OVERHEAD_BYTES + len(item)
    if item[0] == "frame":
        return QUEUE_ITEM_OVERHEAD_BYTES + len(item[1])
    if item[0] == CHAIN_RESET:
        return QUEUE_ITEM_OVERHEAD_BYTES + QUEUE_TOPIC_BYTES * len(item[1])
    return QUEUE_ITEM_OVERHEAD_BYTES + QUEUE_ROW_BYTES * len(item[1])


class Overrun(queue.Full):
    """`FrameQueue.put_batch` ran out of time with `dropped` of its items not taken."""

    def __init__(self, dropped: int) -> None:
        super().__init__(dropped)
        self.dropped = dropped


class FrameQueue(queue.Queue[Any]):
    """A bounded FIFO queue that the intake and the side lanes fill and the
    writer drains, a batch at a time.

    Bounded twice: `maxsize` items and `max_bytes` of `queue_item_bytes`, each
    unbounded at 0. A frame's size is the venue's, so an item count alone lets a
    backlog of large book snapshots past the process's memory ceiling."""

    def __init__(self, maxsize: int = 0, max_bytes: int = 0) -> None:
        super().__init__(maxsize)
        self.max_bytes = max_bytes
        #: Maintained under the queue's lock, and only while `max_bytes` bounds.
        self.queued_bytes = 0

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        self.put_batch([item], timeout=timeout if block else 0.0)

    def _get(self) -> Any:
        item = self.queue.popleft()
        if self.max_bytes > 0:
            self.queued_bytes -= queue_item_bytes(item)
        return item

    def put_batch(self, items: list[Any], timeout: float | None = None) -> None:
        """Add a batch in order, as much as has room under each lock acquisition,
        waiting for room for the rest. Room is both bounds at once, except that an
        empty queue always takes the next item, so one frame larger than
        `max_bytes` cannot wedge its shard. A batch may be larger than the queue.

        Raises `Overrun` when `timeout` runs out; the items already taken stay queued."""
        if not items:
            return
        # Summed before the lock: items[a:b] hold ends[b - 1] - ends[a - 1] bytes.
        ends = list(accumulate(map(queue_item_bytes, items))) if self.max_bytes > 0 else None
        with self.not_full:
            endtime = None if timeout is None else time.monotonic() + timeout
            taken = 0
            while True:
                room = len(items) - taken
                if self.maxsize > 0:
                    room = min(room, self.maxsize - len(self.queue))
                if room > 0 and ends is not None:
                    spent = ends[taken - 1] if taken else 0
                    room = bisect_right(ends, spent + self.max_bytes - self.queued_bytes, taken, taken + room) - taken
                    if room == 0 and not self.queue:
                        room = 1
                    if room > 0:
                        self.queued_bytes += ends[taken + room - 1] - spent
                if room > 0:
                    self.queue.extend(items[taken : taken + room])
                    taken += room
                    self.unfinished_tasks += room
                    self.not_empty.notify()
                    if taken == len(items):
                        return
                if endtime is None:
                    self.not_full.wait()
                    continue
                remaining = endtime - time.monotonic()
                if remaining <= 0.0:
                    raise Overrun(len(items) - taken)
                self.not_full.wait(remaining)

    def get_batch(self, max_items: int = 500, timeout: float = 1.0) -> list[Any]:
        with self.not_empty:
            if not self._qsize():
                if timeout is not None:
                    endtime = time.monotonic() + timeout
                    while not self._qsize():
                        remaining = endtime - time.monotonic()
                        if remaining <= 0.0:
                            raise queue.Empty
                        self.not_empty.wait(remaining)
                else:
                    while not self._qsize():
                        self.not_empty.wait()
            items: list[Any] = []
            q = self.queue
            n = min(len(q), max_items)
            if q and q[0] is None:
                items.append(self._get())
            else:
                popleft = q.popleft
                for _ in range(n):
                    item = popleft()
                    if item is None:
                        q.appendleft(None)
                        break
                    items.append(item)
                if self.max_bytes > 0:
                    self.queued_bytes -= sum(map(queue_item_bytes, items))
            # One wake per slot freed, as `Queue.get` gives: a drain of hundreds
            # waking one shard leaves the rest blocked on room that is there.
            if getattr(self.not_full, "_waiters", None):
                self.not_full.notify(len(items))
            return items
