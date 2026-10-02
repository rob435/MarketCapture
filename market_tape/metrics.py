"""One sample a minute of each market recorder, kept on disk and pushed to a metrics sink.

A recorder holds one artifact, its `status.json`. The `lm_recorder` line read
off it is a fixed contract, field for field, shared with a sampler in Rust
(the Rust sampler below) that writes the same line for a recorder, so one
dashboard series reads either: `tests/market_tape/fixtures/recorder_metrics_oracle.json`
pins it. What the
status file says beyond that contract, and what the host says about the
recorder process and the hourly upload, rides on a second line, `lm_tape`,
which only this sampler writes.

```text
python -m market_tape metrics --status REALM=ROOT/status.json [--status REALM=...] --state-dir DIR [--upload-stamp PATH]
python -m market_tape metrics --status ROOT/status.json [--realm bybit] --state-dir DIR [--upload-stamp PATH]
```

Each `--status` is one recorder, and its realm is the label its lines carry
(`lm_recorder{realm="binance"}`); a bare path takes `--realm`. Each sample is
appended to its own `<state-dir>/recorder-<realm>-<YYYY-MM>.jsonl` first,
whatever the sink does: the host file is the record and the sink is a view of
it. Every recorder's lines go in one push. An append the disk refuses still
pushes, since a full disk is what the view is for. The one upload stamp is the
pack run that ships every tape, so each recorder's `lm_tape` line carries it.
`METRICS_PUSH_URL`, `METRICS_PUSH_USER` and `METRICS_PUSH_TOKEN` name the
sink; any of the three empty means local only.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import math
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

__all__ = [
    "KIND",
    "MAX_LINE_BYTES",
    "RECORDER_MAX_AGE_MS",
    "TAPE_FIELDS",
    "Sink",
    "append",
    "execute",
    "line_protocol",
    "main",
    "parse_recorders",
    "read_sample",
    "tape_fields",
]

KIND = "recorder"
#: The recorder's status expires after this; the same limit the Rust sampler
#: and the host watchdog apply (`freshness_limits_ms.recorder` in the oracle).
RECORDER_MAX_AGE_MS = 120_000
#: One appended line is one atomic write; a longer sample is refused.
MAX_LINE_BYTES = 4096
PUSH_TIMEOUT_SECONDS = 10.0
#: The fields the sample never pushes: the line's own coordinates and text.
_NOT_A_FIELD = frozenset({"ts_ms", "realm", "kind", "state", "error", "tape"})
#: The byte meter's feed classes, as `lm_tape_bytes_24h_<class>`. A class the
#: recorder reports outside this list is summed into `other`.
_FEED_CLASSES = ("book", "trades", "ticker", "liquidations", "kline", "control")
#: The feed classes whose clocks `status.json` `clock.feeds` reads (`market_tape/record.py::CLOCK_FEEDS`).
_CLOCK_FEEDS = ("book", "trades")
#: `<key in the upload stamp>` -> `<lm_tape field>`.
_UPLOAD_STAMP_FIELDS = {
    "bytes": "upload_bytes",
    "bytes_30d": "upload_bytes_30d",
    "file_count": "upload_files",
    "pruned_hours": "upload_pruned_hours",
    "pruned_bytes": "upload_pruned_bytes",
    "recovered_segments": "upload_recovered_segments",
    "remote_free_bytes": "archive_free_bytes",
}
#: Every field the `lm_tape` line may carry; a dashboard charts no other.
TAPE_FIELDS = (
    "uptime_s",
    "resyncs",
    "reanchors",
    "symbols",
    "topics",
    "lanes",
    "snapshot_age_ms",
    "compressor_pending",
    "compressor_pending_bytes",
    "compressor_compressed",
    "compressor_failed",
    "compressor_deferred",
    "compressor_deferred_total",
    "compressor_alive",
    "received_total_bytes",
    *(f"bytes_24h_{name}" for name in (*_FEED_CLASSES, "other")),
    "skew_samples",
    "skew_mean_ms",
    "skew_min_ms",
    "skew_max_ms",
    *(f"{reading}_{feed}_{quantile}_ms" for reading in ("skew", "venue") for feed in _CLOCK_FEEDS for quantile in ("p50", "p99")),
    "link_min_rtt_ms",
    "link_rtt_ms",
    "link_ping_rtt_ms",
    "link_out_of_order",
    "reader_frames_per_read",
    "reader_dwell_max_ms",
    "queue_wait_samples",
    "queue_wait_mean_ms",
    "queue_wait_max_ms",
    "fast_json",
    "rss_bytes",
    "cpu_seconds",
    "reader_cpu_seconds",
    "threads",
    "upload_age_ms",
    *_UPLOAD_STAMP_FIELDS.values(),
    "root_free_bytes",
)


# ------------------------------------------------------------ the numbers


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _count(value: Any) -> int:
    return len(value) if isinstance(value, (list, dict)) else 0


def _rounded(value: float, digits: int) -> float:
    # Fixed formatting rounds the binary value, ties to even, as the Rust
    # sampler's `format!("{value:.digits$}")` does.
    return float(f"{value:.{digits}f}")


def _rounded_age(value: float | None) -> int | None:
    if value is None:
        return None
    rounded = round(value)
    if not -(2**63) <= rounded < 2**63:
        return None
    return int(rounded)


def _age_ms(now_ms: int, nanoseconds: Any) -> int | None:
    stamp = _number(nanoseconds)
    if stamp is None or stamp == 0.0:
        return None
    return _rounded_age(now_ms - stamp / 1e6)


def _ratio(part: Any, whole: Any) -> float | None:
    numerator, denominator = _number(part), _number(whole)
    if numerator is None or denominator is None or denominator <= 0.0:
        return None
    return _rounded(numerator / denominator, 6)


def _float_text(value: float) -> str:
    return repr(float(value))


def _refuse_constant(token: str) -> Any:
    # Bare NaN and Infinity are not JSON; the Rust sampler refuses them and so
    # does this one, so no invalid token is ever republished.
    raise ValueError(f"invalid JSON token {token}")


# ------------------------------------------------------------ the sample


def read_sample(path: Path, *, realm: str, now: Callable[[], int]) -> dict[str, Any]:
    """One observation of the recorder's status file, in the Rust sampler's shape.

    The clock is read after the file, so the sample's `ts_ms` is when the
    reading was made and not when the batch began.
    """

    sample, _ = _read(path, realm=realm, now=now)
    return sample


def _read(path: Path, *, realm: str, now: Callable[[], int]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """The sample, and the live payload it was read from (None unless the sample is live)."""

    try:
        raw = path.read_text(encoding="utf-8")
        error: OSError | None = None
    except OSError as exc:
        raw, error = "", exc
    now_ms = now()
    sample: dict[str, Any] = {"ts_ms": now_ms, "realm": realm, "kind": KIND}
    if error is not None:
        sample["state"] = "absent" if isinstance(error, FileNotFoundError) else "unreadable"
        sample["error"] = f"{path}: {error}"
        return sample, None
    try:
        payload = json.loads(raw, parse_constant=_refuse_constant)
    except ValueError as exc:
        sample["state"] = "unreadable"
        sample["error"] = str(exc)
        return sample, None
    if not isinstance(payload, dict):
        sample["state"] = "unreadable"
        sample["error"] = "status is not an object"
        return sample, None
    stamp = payload.get("recorded_at_ns")
    millis: float | None
    if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
        millis = None
    elif isinstance(stamp, int):
        millis = float(stamp // 1_000_000) if stamp > 0 else None
    else:
        millis = math.floor(stamp / 1_000_000) if stamp > 0.0 and math.isfinite(stamp) else None
    if millis is None or millis > now_ms:
        sample["state"] = "unreadable"
        sample["error"] = "source timestamp is missing, invalid, or in the future"
        return sample, None
    elapsed = now_ms - millis
    if elapsed > RECORDER_MAX_AGE_MS:
        sample["state"] = "stale"
        sample["status_age_ms"] = _rounded_age(elapsed)
        return sample, None
    sample["state"] = "live"
    _recorder_fields(sample, payload, now_ms)
    return sample, payload


def _recorder_fields(out: dict[str, Any], status: Mapping[str, Any], now_ms: int) -> None:
    """The `lm_recorder` contract: what the Rust sampler reads off a status file."""

    budget = status.get("budget")
    budget = budget if isinstance(budget, dict) else {}
    shards = [shard for shard in status.get("shards") or [] if isinstance(shard, dict)]
    out["venue"] = status.get("venue")
    out["status_age_ms"] = _age_ms(now_ms, status.get("recorded_at_ns"))
    out["receive_age_ms"] = _age_ms(now_ms, status.get("last_receive_ns"))
    for destination, source in (
        ("projected_month_gb", "projected_month_gb"),
        ("monthly_gb", "monthly_gb"),
        ("budget_over", "over"),
    ):
        out[destination] = _number(budget.get(source))
    out["shed_feeds"] = _count(budget.get("shed"))
    for key in (
        "received_frames",
        "written_rows",
        "queued_frames",
        "dropped_frames",
        "disk_dropped_frames",
        "malformed_frames",
        "snapshot_failures",
        "free_disk_bytes",
        "queue_capacity",
    ):
        out[key] = _number(status.get(key))
    out["disk_blocked"] = 1.0 if status.get("disk_blocked") is True else 0.0
    # A shard overruns at whichever of the queue's two bounds binds first.
    fills = [
        fill
        for fill in (
            _ratio(status.get("queued_frames"), status.get("queue_capacity")),
            _ratio(status.get("queued_bytes"), status.get("queue_byte_capacity")),
        )
        if fill is not None
    ]
    out["queue_fill"] = max(fills) if fills else None
    out["shards"] = len(shards)
    out["shards_connected"] = sum(1 for shard in shards if shard.get("connected") is True)
    out["reconnects"] = _sum_counter(shards, "reconnects")
    received = status.get("bytes")
    out["bytes_24h"] = _number(received.get("received_24h")) if isinstance(received, dict) else None


def _sum_counter(shards: list[dict[str, Any]], key: str) -> int | None:
    """Since-boot counters summed over shards, as an integer; None past the sampler's own range."""

    total = 0
    for shard in shards:
        count = _number(shard.get(key)) or 0.0
        if not -(2**127) <= count < 2**127:
            return None
        total += int(count)
        if not -(2**127) <= total < 2**127:
            return None
    return total


def tape_fields(
    status: Mapping[str, Any] | None,
    *,
    now_ms: int,
    upload_stamp: Path | None = None,
    proc: Path = Path("/proc"),
    root: Path | None = None,
) -> dict[str, float]:
    """The `lm_tape` line: the status file past the recorder contract, the
    recorder process as the kernel sees it, the hourly upload's receipt, and
    free space under the recorder's root.

    `status` is the live payload, or None when the recorder is down; the upload
    fields and free space are read either way, because a stalled upload or a
    full disk with a dead recorder is exactly the hour to notice. The status
    file's own `free_disk_bytes` is as old as the status, and a full disk is
    what stops the recorder writing it.
    """

    fields: dict[str, float] = {}
    if status is not None:
        _status_tape_fields(fields, status, now_ms)
        # The recorder and the reader process it runs beside itself: one
        # unit's memory and threads, and each process's CPU on a counter of
        # its own. The reader exits before the recorder on a stop and can be
        # started again under it, so a sum of the two would fall mid-run and
        # read as a reset. A reader between restarts is absent.
        recorder, reader = (
            _process_fields(proc / str(pid)) if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0 else {}
            for pid in (status.get("pid"), status.get("reader_pid"))
        )
        if recorder:
            fields["cpu_seconds"] = recorder["cpu_seconds"]
            for name in ("rss_bytes", "threads"):
                fields[name] = recorder.get(name, 0.0) + reader.get(name, 0.0)
        if reader:
            fields["reader_cpu_seconds"] = reader["cpu_seconds"]
    if upload_stamp is not None:
        fields.update(_upload_fields(upload_stamp, now_ms))
    if root is not None:
        try:
            fields["root_free_bytes"] = float(shutil.disk_usage(root).free)
        except OSError:
            pass
    return {name: value for name, value in fields.items() if value is not None and math.isfinite(value)}


def _status_tape_fields(fields: dict[str, float], status: Mapping[str, Any], now_ms: int) -> None:
    def put(name: str, value: Any) -> None:
        number = _number(value)
        if number is not None:
            fields[name] = number

    uptime = _age_ms(now_ms, status.get("started_at_ns"))
    if uptime is not None:
        fields["uptime_s"] = float(uptime // 1000)
    shards = [shard for shard in status.get("shards") or [] if isinstance(shard, dict)]
    for name in ("resyncs", "reanchors"):
        put(name, _sum_counter(shards, name))
    tiers = [tier for tier in status.get("tiers") or [] if isinstance(tier, dict)]
    for name in ("symbols", "topics"):
        put(name, sum(int(_number(tier.get(name)) or 0.0) for tier in tiers))
    put("lanes", status.get("lanes"))
    put("snapshot_age_ms", _age_ms(now_ms, status.get("last_snapshot_ns")))
    compressor = status.get("compressor")
    if isinstance(compressor, dict):
        for name in ("pending", "pending_bytes", "compressed", "failed", "deferred", "deferred_total"):
            put(f"compressor_{name}", compressor.get(name))
        put("compressor_alive", 1.0 if compressor.get("alive") is True else 0.0)
    clock = status.get("clock")
    if isinstance(clock, dict):
        put("skew_samples", clock.get("samples"))
        for name in ("skew_mean_ms", "skew_min_ms", "skew_max_ms"):
            put(name, clock.get(name))
        feeds = clock.get("feeds")
        for feed in _CLOCK_FEEDS:
            reading = feeds.get(feed) if isinstance(feeds, dict) else None
            if isinstance(reading, dict):
                for name in ("skew", "venue"):
                    for quantile in ("p50", "p99"):
                        put(f"{name}_{feed}_{quantile}_ms", reading.get(f"{name}_{quantile}_ms"))
    _link_fields(fields, [shard["link"] for shard in shards if shard.get("connected") and isinstance(shard.get("link"), dict)])
    queue_wait = status.get("queue_wait")
    if isinstance(queue_wait, dict):
        put("queue_wait_samples", queue_wait.get("samples"))
        put("queue_wait_mean_ms", queue_wait.get("wait_mean_ms"))
        put("queue_wait_max_ms", queue_wait.get("wait_max_ms"))
    put("fast_json", 1.0 if status.get("fast_json") is True else 0.0 if "fast_json" in status else None)
    received = status.get("bytes")
    if isinstance(received, dict):
        put("received_total_bytes", received.get("received_total"))
        by_feed = received.get("by_feed_24h")
        if isinstance(by_feed, dict):
            totals = {name: 0.0 for name in (*_FEED_CLASSES, "other")}
            for key, value in by_feed.items():
                number = _number(value)
                if number is None:
                    continue
                # The meter keys `<tier>:<feed class>`, and the class carries
                # its depth (`book:50`): the split here is by class alone.
                feed_class = str(key).split(":", 1)[1].split(":", 1)[0] if ":" in str(key) else str(key)
                totals[feed_class if feed_class in totals else "other"] += number
            for name, total in totals.items():
                fields[f"bytes_24h_{name}"] = total


def _link_fields(fields: dict[str, float], links: list[Mapping[str, Any]]) -> None:
    """The connected shards' last reader `STATS`, across shards: the path's
    least round trip, the slowest connection's round trip and ping, the share
    of inbound packets that came in behind a hole since each connection
    opened, and the reader's frames per read and its longest wait."""

    def each(key: str, inner: str | None = None) -> list[float]:
        values = []
        for link in links:
            source = link.get(inner) if inner else link
            value = _number(source.get(key)) if isinstance(source, Mapping) else None
            if value is not None:
                values.append(value)
        return values

    if min_rtt := each("min_rtt_us", "tcp"):
        fields["link_min_rtt_ms"] = min(min_rtt) / 1000.0
    if rtt := each("rtt_us", "tcp"):
        fields["link_rtt_ms"] = max(rtt) / 1000.0
    if ping := each("ping_rtt_ms"):
        fields["link_ping_rtt_ms"] = max(ping)
    segments = sum(each("data_segs_in", "tcp"))
    if segments > 0:
        fields["link_out_of_order"] = _rounded(sum(each("rcv_ooopack", "tcp")) / segments, 6)
    reads = sum(each("reads"))
    if reads > 0:
        fields["reader_frames_per_read"] = _rounded(sum(each("frames")) / reads, 3)
    if dwell := each("dwell_max_ms"):
        fields["reader_dwell_max_ms"] = max(dwell)


def _process_fields(process: Path) -> dict[str, float]:
    """RSS, CPU time and thread count of one process, from its /proc entry.

    Empty when the entry cannot be read: the sampler is another user, and a
    unit with `ProtectProc=invisible` would see nothing here.
    """

    fields: dict[str, float] = {}
    try:
        stat = (process / "stat").read_text(encoding="utf-8")
        tail = stat[stat.rindex(")") + 2 :].split()
        ticks = os.sysconf("SC_CLK_TCK")
        # Fields 14 and 15 of /proc/<pid>/stat are utime and stime; the split
        # starts at field 3, so they are indices 11 and 12; num_threads is 20.
        fields["cpu_seconds"] = (int(tail[11]) + int(tail[12])) / float(ticks)
        fields["threads"] = float(int(tail[17]))
        for line in (process / "status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                fields["rss_bytes"] = float(int(line.split()[1])) * 1024.0
                break
    except (OSError, ValueError, IndexError):
        return {}
    return fields


def _upload_fields(stamp: Path, now_ms: int) -> dict[str, float]:
    """The hourly pack's receipt: how long since it last succeeded, and what that run counted."""

    fields: dict[str, float] = {}
    try:
        modified_ms = stamp.stat().st_mtime * 1000.0
        text = stamp.read_text(encoding="utf-8")
    except OSError:
        return fields
    age = _rounded_age(max(0.0, now_ms - modified_ms))
    if age is not None:
        fields["upload_age_ms"] = float(age)
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        name = _UPLOAD_STAMP_FIELDS.get(key.strip())
        if not separator or name is None:
            continue
        try:
            fields[name] = float(value.strip())
        except ValueError:
            continue
    return fields


# ------------------------------------------------------------ the record


def sample_path(state_dir: Path, sample: Mapping[str, Any]) -> Path:
    month = datetime.fromtimestamp(int(sample["ts_ms"]) / 1000.0, tz=timezone.utc).strftime("%Y-%m")
    return state_dir / f"{sample['kind']}-{sample['realm']}-{month}.jsonl"


def append(state_dir: Path, sample: Mapping[str, Any]) -> Path:
    """One line per sample, in the Rust sampler's file, refused before the file is opened when over the cap."""

    path = sample_path(state_dir, sample)
    line = json.dumps(sample, separators=(",", ":"), sort_keys=True, ensure_ascii=True) + "\n"
    if len(line.encode("utf-8")) > MAX_LINE_BYTES:
        raise ValueError(f"sample is {len(line)} bytes, over the {MAX_LINE_BYTES} byte append cap")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
    return path


def _escape_tag(value: str) -> str:
    return value.replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def line_protocol(sample: Mapping[str, Any]) -> list[str]:
    """The `lm_recorder` line the Rust sampler would write for this sample, then
    the `lm_tape` line when the sample carries tape fields."""

    realm = _escape_tag(str(sample["realm"]))
    stamp = int(sample["ts_ms"]) * 1_000_000
    fields: dict[str, float] = {"up": 1.0 if sample.get("state") == "live" else 0.0}
    for key, value in sample.items():
        if key in _NOT_A_FIELD:
            continue
        number = _number(value)
        if number is not None and math.isfinite(number):
            fields[key] = number
    lines = [_line(f"lm_{sample['kind']}", realm, fields, stamp)]
    tape = sample.get("tape")
    if isinstance(tape, dict):
        numbers = {key: n for key, value in tape.items() if (n := _number(value)) is not None and math.isfinite(n)}
        if numbers:
            lines.append(_line("lm_tape", realm, numbers, stamp))
    return lines


def _line(measurement: str, realm: str, fields: Mapping[str, float], stamp: int) -> str:
    text = ",".join(f"{key}={_float_text(value)}" for key, value in sorted(fields.items()))
    return f"{measurement},realm={realm} {text} {stamp}"


# ------------------------------------------------------------ the push


@dataclass(frozen=True, slots=True)
class Sink:
    url: str
    user: str
    token: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] = os.environ) -> "Sink | None":
        url = environment.get("METRICS_PUSH_URL", "").strip()
        user = environment.get("METRICS_PUSH_USER", "").strip()
        token = environment.get("METRICS_PUSH_TOKEN", "").strip()
        if not (url and user and token):
            return None
        return cls(url, user, token)

    def push(self, body: str) -> None:
        auth = base64.b64encode(f"{self.user}:{self.token}".encode("utf-8")).decode("ascii")
        request = urllib.request.Request(
            self.url,
            data=body.encode("utf-8"),
            method="POST",
            headers={"Authorization": f"Basic {auth}", "Content-Type": "text/plain; charset=utf-8"},
        )
        # Direct, as the Rust sampler is: no proxy from the environment and no
        # redirect followed, so the token goes to the configured host alone.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        with opener.open(request, timeout=PUSH_TIMEOUT_SECONDS) as response:
            response.read()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        raise urllib.error.HTTPError(req.full_url, code, f"redirect to {newurl} refused", headers, fp)


# ------------------------------------------------------------ the run


#: A `--status REALM=PATH` realm is a bare label; text whose part before its first `=` is not one is a path.
_REALM = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class Options:
    #: `(realm, status.json)` per recorder, sampled in this order.
    recorders: tuple[tuple[str, Path], ...]
    state_dir: Path
    upload_stamp: Path | None = None
    proc: Path = Path("/proc")


@dataclass(frozen=True, slots=True)
class Output:
    message: str
    warning: str | None = None


def execute(options: Options, sink: Sink | None, now: Callable[[], int]) -> Output:
    """Read and append each recorder's sample, then push them all at once. The
    two fail apart: a sink that fails leaves the records written and warns, and
    a record the disk refuses is still pushed and warns, because a full disk is
    the sample the dashboard most needs."""

    lines: list[str] = []
    unrecorded: list[str] = []
    for realm, status_path in options.recorders:
        sample, status = _read(status_path, realm=realm, now=now)
        tape = tape_fields(
            status,
            now_ms=int(sample["ts_ms"]),
            upload_stamp=options.upload_stamp,
            proc=options.proc,
            root=status_path.parent,
        )
        if tape:
            sample["tape"] = tape
        try:
            options.state_dir.mkdir(parents=True, exist_ok=True)
            append(options.state_dir, sample)
        except (OSError, ValueError) as exc:
            unrecorded.append(f"WARNING: {realm} sample not recorded on the host: {exc}")
        lines.extend(line_protocol(sample))
    samples = len(options.recorders)
    counted = f"{samples} sample{'' if samples == 1 else 's'} ({len(lines)} lines)"
    warning = "\n".join(unrecorded) if unrecorded else None
    if sink is None:
        if len(unrecorded) == samples:
            return Output("", warning)
        if unrecorded:
            return Output(f"recorded {samples - len(unrecorded)} of {counted}; no metrics sink configured", warning)
        return Output(f"recorded {counted}; no metrics sink configured")
    try:
        sink.push("\n".join(lines) + "\n")
    except (urllib.error.URLError, OSError, ValueError, http.client.HTTPException) as exc:
        failed = f"WARNING: metrics push failed: {exc}"
        return Output("", failed if warning is None else f"{warning}\n{failed}")
    if unrecorded:
        missed = "it" if samples == 1 else f"{len(unrecorded)} of them"
        return Output(f"pushed {counted} without recording {missed}", warning)
    return Output(f"recorded and pushed {counted}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="market_tape metrics",
        description="One sample of the recorder's status, appended to the host record and pushed to the metrics sink.",
    )
    parser.add_argument(
        "--status",
        action="append",
        required=True,
        metavar="[REALM=]PATH",
        help="a recorder's status.json, as REALM=PATH or a bare PATH that takes --realm; repeat per recorder",
    )
    parser.add_argument("--state-dir", type=Path, required=True, help="where recorder-<realm>-<YYYY-MM>.jsonl is appended")
    parser.add_argument("--realm", default="bybit", help="the realm of a bare --status PATH (default: bybit)")
    parser.add_argument(
        "--upload-stamp", type=Path, default=None, help="the hourly pack's receipt, for the upload's age and counts"
    )
    parser.add_argument("--proc", type=Path, default=Path("/proc"), help=argparse.SUPPRESS)
    return parser


def parse_recorders(texts: list[str], *, realm: str) -> tuple[tuple[str, Path], ...]:
    """`--status` values as `(realm, path)`: `REALM=PATH`, or a bare path under `realm`.
    A realm named twice is refused: two recorders would write one record file."""

    recorders: list[tuple[str, Path]] = []
    for text in texts:
        label, separator, path = text.partition("=")
        if separator and _REALM.match(label) and path:
            recorders.append((label, Path(path)))
        else:
            recorders.append((realm, Path(text)))
    realms = [label for label, _ in recorders]
    repeated = sorted({label for label in realms if realms.count(label) > 1})
    if repeated:
        raise ValueError(f"--status names realm {', '.join(repeated)} more than once")
    return tuple(recorders)


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        recorders = parse_recorders(args.status, realm=args.realm)
    except ValueError as exc:
        parser.error(str(exc))
    options = Options(recorders=recorders, state_dir=args.state_dir, upload_stamp=args.upload_stamp, proc=args.proc)
    output = execute(options, Sink.from_environment(), lambda: time.time_ns() // 1_000_000)
    if output.message:
        print(output.message)
    if output.warning:
        print(output.warning, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
