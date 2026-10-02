"""The recorder sampler says what the oracle pins, field for field, and what only it can."""

from __future__ import annotations

import contextlib
import http.client
import http.server
import json
import os
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from market_tape import metrics
from market_tape.__main__ import main as tape_main

ORACLE = json.loads((Path(__file__).resolve().parent / "fixtures" / "recorder_metrics_oracle.json").read_text(encoding="utf-8"))
NOW_MS = 1_788_000_000_000

#: A status file as `Recorder._write_status` shapes it, one minute into an hour.
STATUS: dict[str, Any] = {
    "kind": "forward_capture_status",
    "schema_version": 2,
    "pid": 4242,
    "venue": "bybit",
    "market": "linear",
    "started_at_ns": (NOW_MS - 3_600_000) * 1_000_000,
    "recorded_at_ns": (NOW_MS - 1_000) * 1_000_000,
    "last_receive_ns": (NOW_MS - 500) * 1_000_000,
    "last_snapshot_ns": (NOW_MS - 600_000) * 1_000_000,
    "status_interval_seconds": 30.0,
    "tiers": [{"name": "crypto_perps", "symbols": 520, "topics": 2080, "feeds": ["book:50"], "shed": []}],
    "shards": [
        {
            "index": 0, "connected": True, "reconnects": 2, "reanchors": 14, "resyncs": 3,
            "link": {
                "tcp": {"rtt_us": 72_500, "min_rtt_us": 70_100, "data_segs_in": 90_000, "rcv_ooopack": 45},
                "ping_rtt_ms": 81.2, "reads": 4_000, "frames": 6_000, "dwell_max_ms": 3.5,
            },
        },
        {
            "index": 1, "connected": True, "reconnects": 0, "reanchors": 13, "resyncs": 0,
            "link": {
                "tcp": {"rtt_us": 75_000, "min_rtt_us": 69_900, "data_segs_in": 10_000, "rcv_ooopack": 5},
                "ping_rtt_ms": None, "reads": 1_000, "frames": 1_000, "dwell_max_ms": 0.0,
            },
        },
        # A shard that is down reports the connection it lost; none of it is read.
        {
            "index": 2, "connected": False, "reconnects": 5, "reanchors": 0, "resyncs": 1,
            "link": {"tcp": {"rtt_us": 900_000, "min_rtt_us": 1}, "ping_rtt_ms": 999.0},
        },
    ],
    "lanes": 0,
    "bytes": {
        "received_total": 9_000_000_000,
        "received_24h": 5_000_000_000,
        "by_feed_24h": {
            "crypto_perps:book:50": 4_000_000_000,
            "crypto_perps:trades": 600_000_000,
            "crypto_perps:ticker": 300_000_000,
            "crypto_perps:liquidations": 1_000_000,
            "crypto_perps:control": 99_000_000,
            "crypto_perps:mystery": 7,
        },
    },
    "budget": {"monthly_gb": 5000.0, "projected_month_gb": 1980.4, "over": False, "shed": []},
    "received_frames": 1_234_567,
    "written_rows": 2_345_678,
    "dropped_frames": 0,
    "disk_dropped_frames": 0,
    "malformed_frames": 5,
    "snapshot_failures": 0,
    "queued_frames": 131,
    "queue_capacity": 262_144,
    "queued_bytes": 54_000,
    "queue_byte_capacity": 1_073_741_824,
    "disk_blocked": False,
    "free_disk_bytes": 60_000_000_000,
    "compressor": {
        "pending": 3,
        "pending_bytes": 150_000_000,
        "compressed": 812,
        "failed": 0,
        "deferred": 0,
        "deferred_total": 2,
        "alive": True,
    },
    "clock": {
        "samples": 48_210, "skew_mean_ms": 6.4, "skew_min_ms": -1.2, "skew_max_ms": 212.0,
        "feeds": {
            "book": {"samples": 40_000, "skew_p50_ms": 3.25, "skew_p99_ms": 41.0, "venue_p50_ms": 1.0, "venue_p99_ms": 6.0},
            "trades": {"samples": 8_000, "skew_p50_ms": 4.0, "skew_p99_ms": 60.0, "venue_p50_ms": 0.5, "venue_p99_ms": 3.0},
        },
    },
    "queue_wait": {"samples": 31_004, "wait_mean_ms": 0.9, "wait_min_ms": 0.0, "wait_max_ms": 41.7},
    "fast_json": True,
}


def _write(path: Path, payload: Any) -> Path:
    path.write_text(json.dumps(payload) if not isinstance(payload, str) else payload, encoding="utf-8")
    return path


def _proc(tmp_path: Path, pid: int) -> Path:
    process = tmp_path / "proc" / str(pid)
    process.mkdir(parents=True)
    # utime 4200 ticks, stime 800 ticks, 17 threads, in the kernel's own field order.
    tail = ["S", "1", "1", "1", "0", "-1", "4194560", "0", "0", "0", "0", "4200", "800", "0", "0", "32", "12", "17"]
    (process / "stat").write_text(f"{pid} (python3) " + " ".join(tail) + " 0 0\n", encoding="utf-8")
    (process / "status").write_text("Name:\tpython3\nVmRSS:\t  524288 kB\nThreads:\t17\n", encoding="utf-8")
    return tmp_path / "proc"


def _stamp(tmp_path: Path, age_ms: int = 2_000) -> Path:
    stamp = tmp_path / "market-tape-upload.last-success"
    stamp.write_text(
        "uploaded_at=2026-09-20T15:10:41Z\narchives=bybit-linear/2026-09-20T14Z.tar\nfile_count=522\n"
        "bytes=1234567890\nbytes_30d=98765432100\ndestination=storagebox:market-capture/market-tape\n"
        "tapes=bybit-linear\nremote_free_bytes=4000000000000\nkeep_hours=6\nrecovered_segments=0\n"
        "pruned_hours=1\npruned_bytes=1200000000\n",
        encoding="utf-8",
    )
    seconds = (NOW_MS - age_ms) / 1000.0
    os.utime(stamp, (seconds, seconds))
    return stamp


# --------------------------------------------------------- the contract


def test_the_recorder_line_is_the_rust_samplers_line_for_every_oracle_case(tmp_path: Path) -> None:
    cases = [case for case in ORACLE["cases"] if case["kind"] == "recorder"]
    assert cases, "the oracle carries recorder cases"
    for case in cases:
        path = _write(tmp_path / f"{case['name']}.json", case["input"])
        sample = metrics.read_sample(path, realm=case["realm"], now=lambda: case["now_ms"])
        lines = metrics.line_protocol(sample)
        assert lines == [case["line_protocol"]], case["name"]
        # The host record is the same JSON, key for key.
        assert sample == json.loads(case["sample_json"]), case["name"]
        assert metrics.sample_path(tmp_path, sample).name == case["filename"]


def test_rounding_is_the_rust_samplers_rounding() -> None:
    for case in ORACLE["rounding"]:
        assert metrics._rounded(case["value"], case["digits"]) == case["expected"], case
    assert metrics.RECORDER_MAX_AGE_MS == ORACLE["freshness_limits_ms"]["recorder"]


def test_a_source_that_cannot_be_read_is_a_down_sample_with_its_reason(tmp_path: Path) -> None:
    sample = metrics.read_sample(tmp_path / "missing", realm="bybit", now=lambda: NOW_MS)
    assert sample["state"] == "absent" and "missing" in sample["error"]
    assert metrics.line_protocol(sample) == [f"lm_recorder,realm=bybit up=0.0 {NOW_MS * 1_000_000}"]
    assert metrics.read_sample(tmp_path, realm="bybit", now=lambda: NOW_MS)["state"] == "unreadable"
    for raw in ("{ broken", "[]", "null", "true", '{"recorded_at_ns": NaN}', '{"recorded_at_ns": Infinity}'):
        path = _write(tmp_path / "status.json", raw)
        sample = metrics.read_sample(path, realm="bybit", now=lambda: NOW_MS)
        assert sample["state"] == "unreadable", raw
        assert "error" in sample and "received_frames" not in sample


def test_the_clock_is_read_after_the_file(tmp_path: Path) -> None:
    path = _write(tmp_path / "status.json", {"recorded_at_ns": 1_000_000_000})

    def clock() -> int:
        path.write_text("invalid", encoding="utf-8")
        return 1_001

    sample = metrics.read_sample(path, realm="bybit", now=clock)
    assert sample["state"] == "live" and sample["ts_ms"] == 1_001 and sample["status_age_ms"] == 1


# --------------------------------------------------------- the tape line


def test_tape_fields_read_the_status_past_the_contract_the_process_and_the_upload(tmp_path: Path) -> None:
    proc = _proc(tmp_path, 4242)
    _proc(tmp_path, 4343)
    fields = metrics.tape_fields(
        {**STATUS, "reader_pid": 4343}, now_ms=NOW_MS, upload_stamp=_stamp(tmp_path), proc=proc, root=tmp_path
    )
    assert fields["uptime_s"] == 3_600.0
    assert fields["resyncs"] == 4.0 and fields["reanchors"] == 27.0
    assert fields["symbols"] == 520.0 and fields["topics"] == 2_080.0 and fields["lanes"] == 0.0
    assert fields["snapshot_age_ms"] == 600_000.0
    assert fields["compressor_pending"] == 3.0 and fields["compressor_pending_bytes"] == 150_000_000.0
    assert fields["compressor_compressed"] == 812.0 and fields["compressor_deferred_total"] == 2.0
    assert fields["compressor_alive"] == 1.0
    assert fields["received_total_bytes"] == 9_000_000_000.0
    # Receipt against the venue's send stamp over the last status interval,
    # and each feed's quantiles beside the venue's own lag to its send stamp.
    assert fields["skew_samples"] == 48_210.0 and fields["skew_mean_ms"] == 6.4
    assert fields["skew_min_ms"] == -1.2 and fields["skew_max_ms"] == 212.0
    assert (fields["skew_book_p50_ms"], fields["skew_book_p99_ms"]) == (3.25, 41.0)
    assert (fields["skew_trades_p50_ms"], fields["skew_trades_p99_ms"]) == (4.0, 60.0)
    assert (fields["venue_book_p50_ms"], fields["venue_trades_p99_ms"]) == (1.0, 3.0)
    # The connected shards' paths: the least round trip of any, the slowest
    # connection's, the one ping answered, inbound packets behind a hole.
    assert fields["link_min_rtt_ms"] == 69.9 and fields["link_rtt_ms"] == 75.0
    assert fields["link_ping_rtt_ms"] == 81.2
    assert fields["link_out_of_order"] == 0.0005
    assert fields["reader_frames_per_read"] == 1.4 and fields["reader_dwell_max_ms"] == 3.5
    # How long the capture queue held a frame, over the same interval.
    assert fields["queue_wait_samples"] == 31_004.0
    assert fields["queue_wait_mean_ms"] == 0.9 and fields["queue_wait_max_ms"] == 41.7
    assert fields["fast_json"] == 1.0
    # The meter's `<tier>:<feed>` keys fold to feed classes, depth dropped.
    assert fields["bytes_24h_book"] == 4_000_000_000.0
    assert fields["bytes_24h_trades"] == 600_000_000.0
    assert fields["bytes_24h_ticker"] == 300_000_000.0
    assert fields["bytes_24h_liquidations"] == 1_000_000.0
    assert fields["bytes_24h_control"] == 99_000_000.0
    assert fields["bytes_24h_kline"] == 0.0 and fields["bytes_24h_other"] == 7.0
    # 5,000 ticks of the kernel's clock and half a gigabyte resident, each process.
    assert fields["cpu_seconds"] == fields["reader_cpu_seconds"] == 5_000 / os.sysconf("SC_CLK_TCK")
    assert fields["threads"] == 34.0 and fields["rss_bytes"] == 2 * 536_870_912.0
    assert fields["upload_age_ms"] == 2_000.0
    assert fields["upload_files"] == 522.0 and fields["upload_bytes"] == 1_234_567_890.0
    assert fields["upload_bytes_30d"] == 98_765_432_100.0 and fields["archive_free_bytes"] == 4_000_000_000_000.0
    assert fields["upload_pruned_hours"] == 1.0 and fields["upload_pruned_bytes"] == 1_200_000_000.0
    assert fields["upload_recovered_segments"] == 0.0
    assert fields["root_free_bytes"] > 0.0
    # Every name this line can carry is declared, and every declared name is
    # carried by a full status: the dashboard charts against the declaration.
    assert set(fields) == set(metrics.TAPE_FIELDS)
    assert len(set(metrics.TAPE_FIELDS)) == len(metrics.TAPE_FIELDS)


def test_a_reader_that_comes_and_goes_never_moves_the_recorders_cpu_counter(tmp_path: Path) -> None:
    # A recorder's stop: the reader exits first, and a summed counter falls
    # by the reader's share, which `rate` reads as a reset: dozens of cores.
    proc = _proc(tmp_path, 4242)
    _proc(tmp_path, 4343)
    ticks = os.sysconf("SC_CLK_TCK")
    both = metrics.tape_fields({**STATUS, "reader_pid": 4343}, now_ms=NOW_MS, proc=proc)
    alone = metrics.tape_fields({**STATUS, "reader_pid": 9999}, now_ms=NOW_MS, proc=proc)
    assert both["cpu_seconds"] == alone["cpu_seconds"] == 5_000 / ticks
    assert both["reader_cpu_seconds"] == 5_000 / ticks and "reader_cpu_seconds" not in alone
    # Memory and threads are the unit's: whatever of it is running.
    assert both["threads"] == 34.0 and both["rss_bytes"] == 2 * 536_870_912.0
    assert alone["threads"] == 17.0 and alone["rss_bytes"] == 536_870_912.0


def test_the_upload_is_read_while_the_recorder_is_down_and_a_hidden_process_is_left_out(tmp_path: Path) -> None:
    fields = metrics.tape_fields(None, now_ms=NOW_MS, upload_stamp=_stamp(tmp_path, age_ms=9_000_000))
    assert fields == {
        "upload_age_ms": 9_000_000.0,
        "upload_files": 522.0,
        "upload_bytes": 1_234_567_890.0,
        "upload_bytes_30d": 98_765_432_100.0,
        "archive_free_bytes": 4_000_000_000_000.0,
        "upload_recovered_segments": 0.0,
        "upload_pruned_hours": 1.0,
        "upload_pruned_bytes": 1_200_000_000.0,
    }
    assert metrics.tape_fields(None, now_ms=NOW_MS, upload_stamp=tmp_path / "absent") == {}
    # A /proc the sampler cannot see (ProtectProc=invisible, or a dead pid)
    # costs the process readings and nothing else.
    fields = metrics.tape_fields(STATUS, now_ms=NOW_MS, proc=tmp_path / "no-proc")
    assert {"rss_bytes", "cpu_seconds", "reader_cpu_seconds", "threads"}.isdisjoint(fields)
    assert fields["resyncs"] == 4.0


def test_the_sample_pushes_two_lines_and_records_one(tmp_path: Path) -> None:
    path = _write(tmp_path / "status.json", STATUS)
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=_proc(tmp_path, 4242))
    output = metrics.execute(options, None, lambda: NOW_MS)
    assert output == metrics.Output("recorded 1 sample (2 lines); no metrics sink configured")
    record = (tmp_path / "equity" / "recorder-bybit-2026-08.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(record) == 1 and len(record[0]) <= metrics.MAX_LINE_BYTES
    sample = json.loads(record[0])
    assert sample["state"] == "live" and sample["shards_connected"] == 2 and sample["queue_fill"] == 0.0005
    assert sample["tape"]["cpu_seconds"] == 5_000 / os.sysconf("SC_CLK_TCK")
    recorder, tape = metrics.line_protocol(sample)
    assert recorder.startswith("lm_recorder,realm=bybit budget_over=0.0,bytes_24h=5000000000.0,")
    assert ",up=1.0,written_rows=2345678.0 " in recorder and "tape" not in recorder
    # The three loss counters travel together, on the line both samplers write.
    for counter in ("dropped_frames=0.0,", "disk_dropped_frames=0.0,", "malformed_frames=5.0,"):
        assert counter in recorder and counter not in tape, counter
    assert tape.startswith("lm_tape,realm=bybit bytes_24h_book=4000000000.0,")
    assert tape.endswith(f",venue_trades_p99_ms=3.0 {NOW_MS * 1_000_000}")


def test_the_queue_fill_is_the_fuller_of_its_two_bounds(tmp_path: Path) -> None:
    def fill(**queue: Any) -> Any:
        status = {key: value for key, value in {**STATUS, **queue}.items() if value is not None}
        return metrics.read_sample(_write(tmp_path / "status.json", status), realm="bybit", now=lambda: NOW_MS)["queue_fill"]

    # A quarter of the frames holding three quarters of the bytes: volatile book snapshots.
    assert fill(queued_frames=65_536, queued_bytes=805_306_368) == 0.75
    assert fill(queued_frames=196_608, queued_bytes=268_435_456) == 0.75
    # A status without the byte bound reads on frames alone, as the oracle's do.
    assert fill(queued_frames=65_536, queued_bytes=None, queue_byte_capacity=None) == 0.25
    assert fill(queued_frames=None, queue_capacity=None, queued_bytes=None, queue_byte_capacity=None) is None


def test_a_sample_over_the_line_cap_is_refused_before_the_file_is_opened(tmp_path: Path) -> None:
    sample = {"ts_ms": NOW_MS, "realm": "bybit", "kind": "recorder", "state": "live", "venue": "x" * 5_000}
    with pytest.raises(ValueError, match="append cap"):
        metrics.append(tmp_path, sample)
    assert not (tmp_path / "recorder-bybit-2026-08.jsonl").exists()


def test_a_sample_over_the_line_cap_is_still_pushed_and_warned_about(tmp_path: Path) -> None:
    """The cap refuses the record, as a full disk does; the run goes on to the
    push and the warning, and never to a traceback that ends the sampler."""

    path = _write(tmp_path / "status.json", {**STATUS, "venue": "x" * 5_000})
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=tmp_path / "no-proc")
    with _sink() as (url, received):
        output = metrics.execute(options, metrics.Sink(url=url, user="u", token="t"), lambda: NOW_MS)
    assert output.message == "pushed 1 sample (2 lines) without recording it"
    assert output.warning is not None and output.warning.startswith("WARNING: bybit sample not recorded on the host: sample is ")
    assert "append cap" in output.warning
    assert not list((tmp_path / "equity").glob("*.jsonl"))
    assert received[0]["body"].startswith("lm_recorder,realm=bybit ")


# --------------------------------------------------------- two recorders


def _binance_status() -> dict[str, Any]:
    return {
        **STATUS,
        "pid": 4343,
        "venue": "binance",
        "market": "usdm",
        "tiers": [{"name": "usdt_perps", "symbols": 530, "topics": 1060, "feeds": ["book:1", "trades"], "shed": []}],
        "bytes": {
            "received_total": 3_000_000_000,
            "received_24h": 2_700_000_000,
            "by_feed_24h": {"usdt_perps:book:1": 2_300_000_000, "usdt_perps:trades": 400_000_000},
        },
    }


def test_one_run_samples_each_recorder_under_its_own_realm_and_pushes_them_together(tmp_path: Path) -> None:
    bybit = _write(tmp_path / "bybit.json", STATUS)
    binance = _write(tmp_path / "binance.json", _binance_status())
    options = metrics.Options(
        recorders=(("bybit", bybit), ("binance", binance)),
        state_dir=tmp_path / "equity",
        upload_stamp=_stamp(tmp_path),
        proc=tmp_path / "no-proc",
    )
    with _sink() as (url, received):
        output = metrics.execute(options, metrics.Sink(url=url, user="u", token="t"), lambda: NOW_MS)

    assert output == metrics.Output("recorded and pushed 2 samples (4 lines)")
    assert len(received) == 1, "one push carries every recorder"
    lines = received[0]["body"].splitlines()
    assert [line.split(" ", 1)[0] for line in lines] == [
        "lm_recorder,realm=bybit",
        "lm_tape,realm=bybit",
        "lm_recorder,realm=binance",
        "lm_tape,realm=binance",
    ]
    # The bybit line is the line a single-recorder run writes, field for field:
    # the oracle pins that one.
    alone = metrics.read_sample(bybit, realm="bybit", now=lambda: NOW_MS)
    assert lines[0] == metrics.line_protocol(alone)[0]
    assert "bytes_24h=2700000000.0" in lines[2] and ",venue" not in lines[2]
    assert "bytes_24h_book=2300000000.0" in lines[3] and "upload_age_ms=2000.0" in lines[3]
    for realm in ("bybit", "binance"):
        record = (tmp_path / "equity" / f"recorder-{realm}-2026-08.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(record) == 1 and json.loads(record[0])["realm"] == realm


def test_a_recorder_that_is_down_is_sampled_down_beside_one_that_is_live(tmp_path: Path) -> None:
    bybit = _write(tmp_path / "bybit.json", STATUS)
    options = metrics.Options(
        recorders=(("bybit", bybit), ("binance", tmp_path / "absent" / "status.json")),
        state_dir=tmp_path / "equity",
        proc=tmp_path / "no-proc",
    )
    assert metrics.execute(options, None, lambda: NOW_MS) == metrics.Output(
        "recorded 2 samples (3 lines); no metrics sink configured"
    )
    down = json.loads((tmp_path / "equity" / "recorder-binance-2026-08.jsonl").read_text(encoding="utf-8"))
    assert down["state"] == "absent"
    assert json.loads((tmp_path / "equity" / "recorder-bybit-2026-08.jsonl").read_text(encoding="utf-8"))["state"] == "live"


def test_status_arguments_name_a_realm_or_take_the_default_and_never_one_realm_twice() -> None:
    assert metrics.parse_recorders(["/var/lib/tape/status.json"], realm="bybit") == (
        ("bybit", Path("/var/lib/tape/status.json")),
    )
    assert metrics.parse_recorders(["bybit=/a/status.json", "binance=/b/status.json"], realm="bybit") == (
        ("bybit", Path("/a/status.json")),
        ("binance", Path("/b/status.json")),
    )
    # A path whose text before its first `=` is not a bare label is a path.
    assert metrics.parse_recorders(["/srv/x=y/status.json"], realm="bybit") == (("bybit", Path("/srv/x=y/status.json")),)
    with pytest.raises(ValueError, match="bybit more than once"):
        metrics.parse_recorders(["/a/status.json", "bybit=/b/status.json"], realm="bybit")


def test_the_verb_takes_one_status_per_recorder(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bybit = _write(tmp_path / "bybit.json", STATUS)
    binance = _write(tmp_path / "binance.json", _binance_status())
    common = ["--state-dir", str(tmp_path / "equity"), "--proc", str(tmp_path / "no-proc")]
    assert tape_main(["metrics", "--status", f"bybit={bybit}", "--status", f"binance={binance}", *common]) == 0
    assert "recorded 2 samples (4 lines); no metrics sink configured" in capsys.readouterr().out
    assert sorted(path.name.split("-")[1] for path in (tmp_path / "equity").glob("recorder-*.jsonl")) == ["binance", "bybit"]
    with pytest.raises(SystemExit) as refused:
        tape_main(["metrics", "--status", str(bybit), "--status", f"bybit={binance}", *common])
    assert refused.value.code == 2
    assert "more than once" in capsys.readouterr().err


# --------------------------------------------------------- the sink


@contextlib.contextmanager
def _sink(status: int = 204) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    received: list[dict[str, Any]] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            received.append(
                {
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "content_type": self.headers.get("Content-Type"),
                    "body": self.rfile.read(length).decode("utf-8"),
                }
            )
            self.send_response(status)
            self.end_headers()

        def log_message(self, *_args: Any) -> None:
            return None

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/api/v1/push/influx/write", received
    finally:
        server.shutdown()
        server.server_close()


def test_the_push_carries_basic_auth_and_both_lines(tmp_path: Path) -> None:
    path = _write(tmp_path / "status.json", STATUS)
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=tmp_path / "no-proc")
    with _sink() as (url, received):
        sink = metrics.Sink(url=url, user="123456", token="glc_secret")
        output = metrics.execute(options, sink, lambda: NOW_MS)
    assert output == metrics.Output("recorded and pushed 1 sample (2 lines)")
    assert len(received) == 1
    assert received[0]["path"] == "/api/v1/push/influx/write"
    assert received[0]["authorization"] == "Basic MTIzNDU2OmdsY19zZWNyZXQ="
    assert received[0]["content_type"] == "text/plain; charset=utf-8"
    lines = received[0]["body"].splitlines()
    assert [line.split(",", 1)[0] for line in lines] == ["lm_recorder", "lm_tape"]


def test_a_refused_push_warns_after_the_record_is_written(tmp_path: Path) -> None:
    path = _write(tmp_path / "status.json", STATUS)
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=tmp_path / "no-proc")
    with _sink(status=401) as (url, _received):
        output = metrics.execute(options, metrics.Sink(url=url, user="u", token="t"), lambda: NOW_MS)
    assert output.message == "" and output.warning is not None
    assert output.warning.startswith("WARNING: metrics push failed: HTTP Error 401")
    assert (tmp_path / "equity" / "recorder-bybit-2026-08.jsonl").exists()


def test_a_sink_that_closes_mid_answer_is_a_warned_push_and_not_a_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A body cut short raises http.client's own error, which is no OSError."""

    path = _write(tmp_path / "status.json", STATUS)
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=tmp_path / "no-proc")

    def cut_short(_self: metrics.Sink, _body: str) -> None:
        raise http.client.IncompleteRead(b"")

    monkeypatch.setattr(metrics.Sink, "push", cut_short)
    output = metrics.execute(options, metrics.Sink(url="http://sink.invalid/write", user="u", token="t"), lambda: NOW_MS)
    assert output.message == "" and output.warning is not None
    assert output.warning.startswith("WARNING: metrics push failed: IncompleteRead")
    assert (tmp_path / "equity" / "recorder-bybit-2026-08.jsonl").exists()


def test_a_record_the_full_disk_refuses_is_still_pushed_with_the_free_space(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On a full disk the append raises ENOSPC before the push. The push must
    still go, or the dashboard's Tape row reads No data for the whole outage
    instead of a recorder that is down on a full disk."""

    stale = dict(STATUS, recorded_at_ns=(NOW_MS - 44_519_000) * 1_000_000)
    path = _write(tmp_path / "status.json", stale)
    options = metrics.Options(recorders=(("bybit", path),), state_dir=tmp_path / "equity", proc=tmp_path / "no-proc")

    def no_space(_state_dir: Path, _sample: Any) -> Path:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(metrics, "append", no_space)
    with _sink() as (url, received):
        output = metrics.execute(options, metrics.Sink(url=url, user="u", token="t"), lambda: NOW_MS)
    assert output.message == "pushed 1 sample (2 lines) without recording it"
    assert output.warning == "WARNING: bybit sample not recorded on the host: [Errno 28] No space left on device"
    recorder, tape = received[0]["body"].splitlines()
    assert recorder.startswith("lm_recorder,realm=bybit ") and ",up=0.0 " in recorder
    assert tape.startswith("lm_tape,realm=bybit ") and "root_free_bytes=" in tape
    # Nothing to push to: the refusal is the whole report.
    assert metrics.execute(options, None, lambda: NOW_MS) == metrics.Output("", output.warning)


def test_the_sink_is_the_three_variables_or_nothing() -> None:
    assert metrics.Sink.from_environment({}) is None
    assert metrics.Sink.from_environment({"METRICS_PUSH_URL": "https://x", "METRICS_PUSH_USER": "1"}) is None
    sink = metrics.Sink.from_environment(
        {"METRICS_PUSH_URL": " https://x/write ", "METRICS_PUSH_USER": "1", "METRICS_PUSH_TOKEN": "t"}
    )
    assert sink == metrics.Sink("https://x/write", "1", "t")


# --------------------------------------------------------- the CLI


def test_the_verb_runs_from_the_package_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _write(tmp_path / "status.json", STATUS)
    assert (
        tape_main(
            [
                "metrics",
                "--status",
                str(path),
                "--state-dir",
                str(tmp_path / "equity"),
                "--upload-stamp",
                str(_stamp(tmp_path)),
                "--proc",
                str(tmp_path / "no-proc"),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "no metrics sink configured" in out
    record = (tmp_path / "equity").glob("recorder-bybit-*.jsonl")
    sample = json.loads(next(record).read_text(encoding="utf-8"))
    # The status was written a minute before this test's clock, whichever
    # minute that is, so the sample is stale and the upload and the disk
    # still speak.
    assert sample["state"] == "stale"
    assert set(sample["tape"]) == {
        "root_free_bytes",
        "upload_age_ms",
        "upload_files",
        "upload_bytes",
        "upload_bytes_30d",
        "archive_free_bytes",
        "upload_recovered_segments",
        "upload_pruned_hours",
        "upload_pruned_bytes",
    }
    with pytest.raises(SystemExit) as done:
        tape_main(["metrics", "--help"])
    assert done.value.code == 0
