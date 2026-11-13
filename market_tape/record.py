"""The recorder: tiers of symbols and feeds on one venue, written as the tape.

One process records one venue. It reads the capture config, resolves each
tier's universe, subscribes the union of the tiers' feeds over several
websocket connections (shards), and writes every normalized row through
`storage.SegmentWriter`.

Universes that change while the recorder runs are re-read on every maintenance
tick. The `listed` kind follows the venue's table snapshots; the live kinds
(ranked turnover and movers, funding either side of a line, turnover and
volume surges, price bursts, open-interest jumps) follow the ticker stream the
recorder is already writing, so a name whose funding rate collapses or whose
turnover explodes gets its deep feeds within one tick, not at the next daily
snapshot. Topics are added to and removed from the live
connections in place; a connection only reconnects when the venue drops it.

Every received byte is metered per tier and per feed. With a budget in the
config, the recorder projects a month from its last day of bytes and, when the
projection is over the allowance, gives up the configured `tier:feed` pairs in
order, one an hour, restoring them in reverse once under pace.

Processes and threads: the sockets are read in a process of their own, the
reader (`market_tape.reader`), which connects every shard, keeps it alive,
and stamps every frame as it is read; the writer here parses and serialises
every frame, and on one interpreter lock the two took turns, which on the
capture host cost more CPU than either's work and delayed every stamp by the
writer's turn. In this process: one intake thread moving the reader's
frames into the capture queue, one thread per shard for its connection's
lifetime (open, subscribe, backoff), one writer, one compressor, one
maintainer (status, universes, budget, re-anchoring), one meta writer (the
venue tables and the hourly coverage records), one pruner (retention), one
resyncer (re-subscribing gapped book topics), plus whatever side lanes the
venue adapter starts. Frames cross from the intake to the writer
through one bounded queue, which absorbs a writer slower than the wire for
as long as its bounds allow; past them the reader holds each shard's frames
and, when they have waited too long, drops them, counts them in the status
file, and the shard reconnects for fresh snapshots. A run holds
`<root>/.recorder.lock` from before the compressor starts until after the last
status write, so a second recorder on one root refuses to start rather than
race this one's raw files; `market_tape pack` takes the same lock and finishes
the raw segments only on a root no recorder holds.

Retention and the `_meta` writes have their own threads because
`status.json` is this unit's heartbeat: the maintainer writes it every
`status_interval_seconds`, a pass over a tape holding days of hours across
hundreds of symbols takes longer than the watchdog's freshness limit, and a
table snapshot or coverage record is a REST read or a zstd run and several
fsyncs, each of which waits behind every dirty page the writer has on the
filesystem.

Modules, under `market_tape/recorder/`: `queue.py` (the capture queue and
what an item holds), `intake.py` (the reader process, its links, the intake
thread), `shard.py` (one connection's lifetime, re-anchoring, resyncs),
`clocks.py` (`status.json` `clock` and `queue_wait`), `meter.py` (bytes per
tier and feed), `budget.py` (shed and restore), `live.py` (the ticker
stream's view of every symbol), and `core.py` (`Recorder`: universes,
writing, book resync, retention, maintenance, status). This module is the
entry point (`python -m market_tape record`) and keeps every name the
recorder has ever exported importable from here.
"""

from __future__ import annotations

from market_tape.recorder.budget import (
    BUDGET_MIN_WINDOW_NS,
    MONTH_SECONDS,
    BudgetController,
)
from market_tape.recorder.clocks import (
    CLOCK_FEEDS,
    QUANTILE_EDGES_MS,
    ClockWindow,
    Quantiles,
)
from market_tape.recorder.core import (
    MALFORMED_LOG_SECONDS,
    MALLOC_TRIM,
    REANCHOR_TOPICS_PER_TICK,
    RETENTION_INTERVAL_SECONDS,
    SIGNAL_POLL_SECONDS,
    WRITE_IDLE_SECONDS,
    Recorder,
    _malloc_trim,
    run,
)
from market_tape.recorder.intake import (
    INTAKE_PUT_SECONDS,
    INTAKE_READ_BYTES,
    INTAKE_TICK_SECONDS,
    OPEN_GRACE_SECONDS,
    READER_EXIT_SECONDS,
    READER_RESPAWN_SECONDS,
    ReaderProcess,
    RemoteLink,
    spawn_reader,
)
from market_tape.recorder.live import (
    SAMPLE_SPACING_NS,
    LiveState,
    Sample,
    SymbolLive,
)
from market_tape.recorder.meter import (
    DAY_NS,
    MINUTE_NS,
    ByteMeter,
)
from market_tape.recorder.queue import (
    CHAIN_RESET,
    EVENT_HEAD,
    EVENT_WALL,
    LANES,
    QUEUE_EVENT_OVERHEAD_BYTES,
    QUEUE_ITEM_OVERHEAD_BYTES,
    QUEUE_ROW_BYTES,
    QUEUE_TOPIC_BYTES,
    FrameQueue,
    Overrun,
    QueueItem,
    queue_item_bytes,
    received_ns_of,
)
from market_tape.recorder.shard import (
    LIVE_MESSAGE_SPACING_SECONDS,
    REANCHOR_CHUNK,
    RECONNECT_BACKOFF_MAX_SECONDS,
    RECONNECT_BACKOFF_MIN_SECONDS,
    RECONNECT_HEALTHY_SECONDS,
    Shard,
    shard_topics,
)

__all__ = [
    "BUDGET_MIN_WINDOW_NS",
    "MONTH_SECONDS",
    "BudgetController",
    "CLOCK_FEEDS",
    "QUANTILE_EDGES_MS",
    "ClockWindow",
    "Quantiles",
    "MALFORMED_LOG_SECONDS",
    "MALLOC_TRIM",
    "REANCHOR_TOPICS_PER_TICK",
    "RETENTION_INTERVAL_SECONDS",
    "SIGNAL_POLL_SECONDS",
    "WRITE_IDLE_SECONDS",
    "Recorder",
    "_malloc_trim",
    "run",
    "INTAKE_PUT_SECONDS",
    "INTAKE_READ_BYTES",
    "INTAKE_TICK_SECONDS",
    "OPEN_GRACE_SECONDS",
    "READER_EXIT_SECONDS",
    "READER_RESPAWN_SECONDS",
    "ReaderProcess",
    "RemoteLink",
    "spawn_reader",
    "SAMPLE_SPACING_NS",
    "LiveState",
    "Sample",
    "SymbolLive",
    "DAY_NS",
    "MINUTE_NS",
    "ByteMeter",
    "CHAIN_RESET",
    "EVENT_HEAD",
    "EVENT_WALL",
    "LANES",
    "QUEUE_EVENT_OVERHEAD_BYTES",
    "QUEUE_ITEM_OVERHEAD_BYTES",
    "QUEUE_ROW_BYTES",
    "QUEUE_TOPIC_BYTES",
    "FrameQueue",
    "Overrun",
    "QueueItem",
    "queue_item_bytes",
    "received_ns_of",
    "LIVE_MESSAGE_SPACING_SECONDS",
    "REANCHOR_CHUNK",
    "RECONNECT_BACKOFF_MAX_SECONDS",
    "RECONNECT_BACKOFF_MIN_SECONDS",
    "RECONNECT_HEALTHY_SECONDS",
    "Shard",
    "shard_topics",
]
