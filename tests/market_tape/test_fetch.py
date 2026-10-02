"""`market_tape fetch`: a plan's symbol-hours range-read from the box that `market_tape pack` filled.

Every test ships real hours with the real packer into a directory the fake
rclone (`fake_rclone.py`) serves as the storage box, honouring `cat --offset
--count` as rclone does, then runs the fetch verb as the host runs it: its own
process, its one JSON line on stdout. What can go wrong, and the test that
holds each:

| Fault | Test |
| --- | --- |
| a member lands short, altered, or at the wrong path; a symbol's run costs a read per segment; the whole tar is read for one symbol | `test_a_planned_hour_is_range_read_member_by_member` |
| a second run reads what it holds again; a kept index goes stale unnoticed | `test_a_second_run_reads_only_what_it_does_not_hold` |
| a tar not shipped yet reads as a failure | `test_an_hour_not_on_the_box_yet_is_pending` |
| an hour packed before indexes is unreadable, or streamed past its bound | `test_an_hour_packed_before_indexes_streams_its_tar_within_the_bound` |
| a corrupted member is kept, or leaves a part file | `test_a_member_that_does_not_match_its_digest_is_refused_and_leaves_nothing` |
| a file the ledger does not vouch for is trusted | `test_a_file_the_ledger_does_not_vouch_for_is_read_again` |
| the window deletes too much or too little, or its ledger rows linger | `test_the_window_deletes_old_hours_and_skips_planned_ones_it_would_delete` |
| a slow box overruns the unit's timeout | `test_the_budget_ends_the_run_and_says_what_is_left` |
| a read runs at the line's full rate, queueing the host's market data behind it | `test_a_capped_run_hands_every_rclone_call_the_cap` |
| a bad invocation runs anyway, or prints a summary | `test_a_run_that_cannot_start_exits_2_and_prints_nothing` |
| an index names a path outside the tape layout; an index lies about the tar | `test_an_index_cannot_write_outside_the_tape_or_past_the_tar` |
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from market_tape.load import iter_rows, open_source
from market_tape.schema import book_row, trade_row
from tests.market_tape import fake_rclone

ROOT = Path(__file__).resolve().parents[2]
REMOTE_BASE = "storagebox:market-capture/market-tape"
DAY, HOUR = "2026-09-25", "13"
COVERAGE = "_meta/coverage-13-20260925T130000Z.json.zst"
INSTRUMENTS = "_meta/instruments-20260925T130000Z.json.zst"


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


def _environment(tmp_path: Path, **extra: str) -> dict[str, str]:
    return {
        **os.environ,
        "FAKE_REMOTE_DIR": str(tmp_path / "box"),
        "FAKE_RCLONE_LOG": str(tmp_path / "rclone.log"),
        **extra,
    }


def _hour(root: Path, files: dict[str, bytes], day: str = DAY, hour: str = HOUR) -> None:
    for relative, payload in files.items():
        path = root / day / hour / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)


def _pack(tmp_path: Path, tape: str, root: Path, now: str) -> None:
    """Ship every finished hour of `root` to the box, as the capture host's timer does."""

    config = tmp_path / "rclone.conf"
    config.write_text("[storagebox]\ntype = sftp\nhost = u1.box.example\nuser = u1\nport = 23\n", encoding="utf-8")
    done = subprocess.run(
        [
            sys.executable, "-m", "market_tape", "pack",
            "--tape", f"{tape}={root}", "--remote-base", REMOTE_BASE,
            "--state-dir", str(tmp_path / "pack-state"), "--stamp-file", str(tmp_path / "pack-stamp"),
            "--rclone", str(fake_rclone.install(tmp_path)), "--config", str(config), "--now", str(_epoch(now)),
        ],
        cwd=ROOT, capture_output=True, text=True, env=_environment(tmp_path), check=False,
    )
    assert done.returncode == 0, done.stderr


def _plan(tmp_path: Path, tapes: dict[str, list[dict[str, Any]]], name: str = "plan") -> Path:
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps({"schema": 1, "tapes": tapes}), encoding="utf-8")
    return path


def _fetch(
    tmp_path: Path, plan: Path, *extra: str, now: str = "2026-09-25T14:30:00", env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    done = subprocess.run(
        [
            sys.executable, "-m", "market_tape", "fetch",
            "--remote-base", REMOTE_BASE, "--plan", str(plan),
            "--root", f"bybit-linear={tmp_path / 'local' / 'bybit-linear'}",
            "--state-dir", str(tmp_path / "local" / ".state"),
            "--rclone", str(fake_rclone.install(tmp_path)), "--now", str(_epoch(now)),
            # A hang reads as `left` in seconds, not as a stuck test; a test's own --timeout comes later and wins.
            "--timeout", "60", *extra,
        ],
        cwd=ROOT, capture_output=True, text=True, env=env or _environment(tmp_path), check=False, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    lines = done.stdout.splitlines()
    assert len(lines) == 1, done.stdout
    return done, json.loads(lines[0])


def _calls(tmp_path: Path, verb: str) -> list[str]:
    log = tmp_path / "rclone.log"
    return [line for line in log.read_text().splitlines() if line.startswith(verb)] if log.exists() else []


def _zst(rows: list[dict[str, Any]]) -> bytes:
    lines = b"".join(json.dumps(row, separators=(",", ":")).encode() + b"\n" for row in rows)
    return subprocess.run(["zstd", "-q", "-c"], input=lines, capture_output=True, check=True).stdout


def _rows(symbol: str, start_ns: int, count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for step in range(count):
        at = start_ns + step * 1_000_000
        rows.append(
            book_row(venue="bybit", symbol=symbol, snapshot=True, depth=1, local_receive_ts_ns=at, exchange_system_ts_ns=at,
                     exchange_engine_ts_ns=at, bids=[["100.0", "1"]], asks=[["100.1", "2"]], update_id=step + 1,
                     previous_update_id=step)
        )
        rows.append(
            trade_row(venue="bybit", symbol=symbol, local_receive_ts_ns=at + 1, exchange_ts_ns=at, trade_id=f"{symbol}{step}",
                      price=100.05, qty=0.5, side="Buy")
        )
    return rows


def _standard_hour(tmp_path: Path) -> dict[str, bytes]:
    """BTC in two segments, ETH in one, SOL big enough that skipping it is a second range, `_meta`."""

    files = {
        "BTCUSDT/segment-000000.jsonl.zst": os.urandom(3_000),
        "BTCUSDT/segment-000001.jsonl.zst": os.urandom(1_111),
        "ETHUSDT/segment-000000.jsonl.zst": os.urandom(2_222),
        "SOLUSDT/segment-000000.jsonl.zst": os.urandom(600_000),
        COVERAGE: os.urandom(333),
        INSTRUMENTS: os.urandom(4_444),
    }
    _hour(tmp_path / "tape", files)
    _pack(tmp_path, "bybit-linear", tmp_path / "tape", now="2026-09-25T14:10:00")
    return files


def _box_tar(tmp_path: Path, tape: str = "bybit-linear", day: str = DAY, hour: str = HOUR) -> Path:
    return tmp_path / "box" / "market-capture" / "market-tape" / tape / day.replace("-", "/") / f"{day}T{hour}Z.tar"


def _wanted(symbols: list[str], day: str = DAY, hour: str = HOUR) -> dict[str, list[dict[str, Any]]]:
    return {"bybit-linear": [{"day": day, "hour": hour, "symbols": symbols}]}


def _local(tmp_path: Path, relative: str, day: str = DAY, hour: str = HOUR) -> Path:
    return tmp_path / "local" / "bybit-linear" / day / hour / relative


def _parts(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "local").rglob("*.part"))


# ------------------------------------------------------------------ by index


def test_a_planned_hour_is_range_read_member_by_member(tmp_path: Path) -> None:
    files = _standard_hour(tmp_path)
    os.umask(0o022)

    _done, summary = _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT", "XRPUSDT"])))

    wanted = ["BTCUSDT/segment-000000.jsonl.zst", "BTCUSDT/segment-000001.jsonl.zst", "ETHUSDT/segment-000000.jsonl.zst", COVERAGE]
    assert summary["hours"] == [
        {"tape": "bybit-linear", "day": DAY, "hour": HOUR, "status": "done", "reason": None, "fetched": 4, "skipped": 0,
         "failed": 0, "bytes": sum(len(files[path]) for path in wanted), "missing": ["XRPUSDT"]}
    ]
    assert (summary["fetched"], summary["skipped"], summary["failed"], summary["pending"], summary["left"]) == (4, 0, 0, 0, 0)
    for relative in wanted:
        local = _local(tmp_path, relative)
        assert local.read_bytes() == files[relative], relative
        assert stat.S_IMODE(local.stat().st_mode) == 0o640, relative
    # Not planned: another symbol's segments and the venue tables.
    assert not _local(tmp_path, "SOLUSDT").exists() and not _local(tmp_path, INSTRUMENTS).exists()
    for directory in (_local(tmp_path, ""), _local(tmp_path, "BTCUSDT"), _local(tmp_path, "_meta"), tmp_path / "local" / "bybit-linear"):
        assert stat.S_IMODE(directory.stat().st_mode) == 0o750, directory
    assert _parts(tmp_path) == []
    # BTC's run and ETH's share one range; SOL's 600 KB is skipped and the
    # coverage record after it is a second; nothing reads the whole tar.
    reads = [call.split() for call in _calls(tmp_path, "cat --offset")]
    assert len(reads) == 2
    assert sum(int(call[4]) for call in reads) < _box_tar(tmp_path).stat().st_size - 590_000


def test_a_fetched_root_reads_back_as_the_tape_it_came_from(tmp_path: Path) -> None:
    start = int(_epoch(f"{DAY}T{HOUR}:00:00") * 1e9)
    source_rows = {symbol: _rows(symbol, start + offset, 50) for offset, symbol in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT"))}
    _hour(tmp_path / "tape", {f"{symbol}/segment-000000.jsonl.zst": _zst(rows) for symbol, rows in source_rows.items()})
    _pack(tmp_path, "bybit-linear", tmp_path / "tape", now="2026-09-25T14:10:00")

    _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT"])))

    source = open_source(str(tmp_path / "local" / "bybit-linear"))
    assert source.venue == "bybit"
    read = list(iter_rows(source, [f"{DAY}T{HOUR}"], typed=False))
    expected = sorted(source_rows["BTCUSDT"] + source_rows["ETHUSDT"], key=lambda row: row["local_receive_ts_ns"])
    assert read == expected


def test_a_second_run_reads_only_what_it_does_not_hold(tmp_path: Path) -> None:
    files = _standard_hour(tmp_path)
    plan = _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT"]))
    _fetch(tmp_path, plan)
    calls_before = len((tmp_path / "rclone.log").read_text().splitlines())

    _done, again = _fetch(tmp_path, plan)

    assert (again["fetched"], again["skipped"], again["failed"]) == (0, 4, 0)
    assert again["hours"][0]["status"] == "done"
    # The kept index answered: no listing, no index read, no range.
    assert len((tmp_path / "rclone.log").read_text().splitlines()) == calls_before

    # A symbol the next plan adds costs exactly its own range.
    _done, more = _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT", "SOLUSDT"])))
    assert (more["fetched"], more["skipped"]) == (1, 4)
    new_calls = (tmp_path / "rclone.log").read_text().splitlines()[calls_before:]
    assert len(new_calls) == 1 and new_calls[0].startswith("cat --offset")
    assert _local(tmp_path, "SOLUSDT/segment-000000.jsonl.zst").read_bytes() == files["SOLUSDT/segment-000000.jsonl.zst"]


def test_an_hour_not_on_the_box_yet_is_pending(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    plan = _plan(
        tmp_path,
        {
            "bybit-linear": [
                {"day": DAY, "hour": "14", "symbols": ["BTCUSDT"]},  # its day is on the box, its tar is not
                {"day": "2026-09-26", "hour": "00", "symbols": ["BTCUSDT"]},  # no day directory at all
            ]
        },
    )

    _done, summary = _fetch(tmp_path, plan, now="2026-09-26T00:30:00")

    assert [(row["day"], row["hour"], row["status"]) for row in summary["hours"]] == [
        ("2026-09-26", "00", "pending"),
        (DAY, "14", "pending"),
    ]
    assert (summary["pending"], summary["failed"], summary["fetched"]) == (2, 0, 0)
    assert not (tmp_path / "local" / "bybit-linear" / DAY / "14").exists()


def test_a_member_that_does_not_match_its_digest_is_refused_and_leaves_nothing(tmp_path: Path) -> None:
    files = _standard_hour(tmp_path)
    tar = _box_tar(tmp_path)
    index = json.loads(tar.with_name(tar.name + ".index.json").read_bytes())
    target = next(member for member in index["members"] if member["path"] == "BTCUSDT/segment-000001.jsonl.zst")
    original = tar.read_bytes()
    damaged = bytearray(original)
    damaged[target["offset"] + 10] ^= 0xFF
    tar.write_bytes(bytes(damaged))
    plan = _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT"]))

    _done, summary = _fetch(tmp_path, plan)

    hour = summary["hours"][0]
    assert (hour["status"], hour["fetched"], hour["failed"]) == ("failed", 3, 1)
    assert "BTCUSDT/segment-000001.jsonl.zst: sha256" in hour["reason"]
    assert not _local(tmp_path, "BTCUSDT/segment-000001.jsonl.zst").exists()
    assert _local(tmp_path, "BTCUSDT/segment-000000.jsonl.zst").read_bytes() == files["BTCUSDT/segment-000000.jsonl.zst"]
    assert _parts(tmp_path) == []
    ledger = (tmp_path / "local" / ".state" / "fetched-members.jsonl").read_text(encoding="utf-8")
    assert "segment-000001" not in ledger
    # The kept index that led to the failure is dropped, and read from the box again next run.
    assert not list((tmp_path / "local" / ".state" / "indexes").rglob("*.index.json"))

    tar.write_bytes(original)
    _done, again = _fetch(tmp_path, plan)
    assert (again["hours"][0]["status"], again["fetched"], again["skipped"]) == ("done", 1, 3)
    assert _local(tmp_path, "BTCUSDT/segment-000001.jsonl.zst").read_bytes() == files["BTCUSDT/segment-000001.jsonl.zst"]


def test_a_file_the_ledger_does_not_vouch_for_is_read_again(tmp_path: Path) -> None:
    files = _standard_hour(tmp_path)
    impostor = _local(tmp_path, "ETHUSDT/segment-000000.jsonl.zst")
    impostor.parent.mkdir(parents=True)
    impostor.write_bytes(b"x" * len(files["ETHUSDT/segment-000000.jsonl.zst"]))

    _done, summary = _fetch(tmp_path, _plan(tmp_path, _wanted(["ETHUSDT"])))

    assert (summary["fetched"], summary["skipped"]) == (2, 0)
    assert impostor.read_bytes() == files["ETHUSDT/segment-000000.jsonl.zst"]


# --------------------------------------------------------- without an index


def test_an_hour_packed_before_indexes_streams_its_tar_within_the_bound(tmp_path: Path) -> None:
    files = _standard_hour(tmp_path)
    tar = _box_tar(tmp_path)
    tar.with_name(tar.name + ".index.json").unlink()
    plan = _plan(tmp_path, _wanted(["BTCUSDT"]))

    # Over the bound: said, and nothing read.
    _done, bounded = _fetch(tmp_path, plan, "--max-stream-bytes", str(tar.stat().st_size - 1))
    assert bounded["hours"][0]["status"] == "skipped" and bounded["hours"][0]["reason"] == "no_index_too_large"
    assert _calls(tmp_path, "cat") == []
    assert not _local(tmp_path, "BTCUSDT").exists()

    _done, summary = _fetch(tmp_path, plan)

    assert (summary["hours"][0]["status"], summary["fetched"], summary["failed"]) == ("done", 3, 0)
    for relative in ("BTCUSDT/segment-000000.jsonl.zst", "BTCUSDT/segment-000001.jsonl.zst", COVERAGE):
        assert _local(tmp_path, relative).read_bytes() == files[relative]
    assert not _local(tmp_path, "ETHUSDT").exists()
    # One whole-tar stream, no range.
    assert [call.split()[:2] for call in _calls(tmp_path, "cat")] == [["cat", f"{REMOTE_BASE}/bybit-linear/2026/09/25/{DAY}T{HOUR}Z.tar"]]
    assert _parts(tmp_path) == []

    # A tar that breaks mid-stream fails its hour, and the run still reports.
    with tarfile.open(tar) as archive:
        header = archive.getmember("BTCUSDT/segment-000001.jsonl.zst").offset
    damaged = bytearray(tar.read_bytes())
    damaged[header : header + 512] = b"\x7f" * 512
    tar.write_bytes(bytes(damaged))
    _done, broken = _fetch(tmp_path, _plan(tmp_path, _wanted(["ETHUSDT"])))
    assert broken["hours"][0]["status"] == "failed" and broken["failed"] >= 1
    assert not _local(tmp_path, "ETHUSDT").exists() and _parts(tmp_path) == []


# ------------------------------------------------------------ the window


def test_the_window_deletes_old_hours_and_skips_planned_ones_it_would_delete(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    plan = _plan(
        tmp_path,
        {"bybit-linear": [{"day": DAY, "hour": HOUR, "symbols": ["ETHUSDT"]}, {"day": "2026-09-24", "hour": "10", "symbols": ["ETHUSDT"]}]},
        name="window",
    )
    _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT"])))
    old = tmp_path / "local" / "bybit-linear" / "2026-09-23" / "01" / "BTCUSDT" / "segment-000000.jsonl.zst"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    ledger = tmp_path / "local" / ".state" / "fetched-members.jsonl"
    with ledger.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"tape": "bybit-linear", "path": "2026-09-23/01/BTCUSDT/segment-000000.jsonl.zst", "bytes": 3, "sha256": "0" * 64}) + "\n")

    # 14:30 on the 25th with a 24 h window: the 23rd's hour 01 is long gone, the
    # 24th's hour 10 (over at 11:00) ended 27.5 h ago, the 25th's 13 an hour and a half ago.
    _done, summary = _fetch(tmp_path, plan, "--keep-hours", "24")

    assert summary["pruned"] == 1
    assert not (tmp_path / "local" / "bybit-linear" / "2026-09-23").exists()
    assert _local(tmp_path, "BTCUSDT/segment-000000.jsonl.zst").exists()
    by_hour = {(row["day"], row["hour"]): row for row in summary["hours"]}
    assert (by_hour[("2026-09-24", "10")]["status"], by_hour[("2026-09-24", "10")]["reason"]) == ("skipped", "past_keep_hours")
    assert by_hour[(DAY, HOUR)]["status"] == "done"
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]
    assert rows and not any(row["path"].startswith("2026-09-23/") for row in rows)

    # Once the 25th's hour 13 is past the window too, it goes with its kept index.
    _done, later = _fetch(tmp_path, _plan(tmp_path, {"bybit-linear": []}), "--keep-hours", "24", now="2026-09-26T15:00:00")
    assert later["pruned"] == 1 and later["hours"] == []
    assert not (tmp_path / "local" / "bybit-linear" / DAY).exists()
    assert not list((tmp_path / "local" / ".state" / "indexes").rglob("*.index.json"))
    assert ledger.read_text(encoding="utf-8") == ""


# ------------------------------------------------------------ the budget


def test_the_budget_ends_the_run_and_says_what_is_left(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    plan = _plan(
        tmp_path,
        {"bybit-linear": [{"day": DAY, "hour": HOUR, "symbols": ["BTCUSDT"]}, {"day": DAY, "hour": "12", "symbols": ["BTCUSDT"]}]},
    )
    started = time.monotonic()

    _done, summary = _fetch(tmp_path, plan, "--timeout", "1.5", env=_environment(tmp_path, FAKE_RCLONE_CAT_SLEEP="30"))

    assert time.monotonic() - started < 15.0
    assert [row["status"] for row in summary["hours"]] == ["left", "left"]
    assert (summary["left"], summary["failed"], summary["fetched"]) == (2, 0, 0)
    assert _parts(tmp_path) == []
    assert not _local(tmp_path, "BTCUSDT").exists()


def test_a_capped_run_hands_every_rclone_call_the_cap(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    before = len((tmp_path / "rclone.log").read_text().splitlines())

    _done, summary = _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT", "ETHUSDT"])), "--bwlimit-kibps", "192")

    assert summary["hours"][0]["status"] == "done"
    calls = (tmp_path / "rclone.log").read_text().splitlines()[before:]
    # The listing, the index and the ranges: every read the run made.
    assert {call.split()[0] for call in calls} == {"lsjson", "cat"}, calls
    # rclone paces a read as it hands it on, above its read-ahead buffer and
    # the SFTP reader's window of concurrent requests, so both go with the cap
    # or each read still lands at the line's rate.
    capped = ["--bwlimit=192K", "--buffer-size=0", "--sftp-disable-concurrent-reads"]
    assert all(call.split()[-3:] == capped for call in calls), calls


# -------------------------------------------------------------- refusals


def test_a_run_that_cannot_start_exits_2_and_prints_nothing(tmp_path: Path) -> None:
    good = _plan(tmp_path, _wanted(["BTCUSDT"]))
    broken = tmp_path / "broken.json"
    broken.write_text("{ not json", encoding="utf-8")
    wrong = tmp_path / "wrong.json"
    wrong.write_text(json.dumps({"schema": 2, "tapes": {}}), encoding="utf-8")
    bad_hour = tmp_path / "bad-hour.json"
    bad_hour.write_text(json.dumps({"schema": 1, "tapes": {"bybit-linear": [{"day": DAY, "hour": "24", "symbols": []}]}}), encoding="utf-8")
    bad_symbol = tmp_path / "bad-symbol.json"
    bad_symbol.write_text(json.dumps({"schema": 1, "tapes": {"bybit-linear": [{"day": DAY, "hour": "01", "symbols": ["../x"]}]}}), encoding="utf-8")
    base = [sys.executable, "-m", "market_tape", "fetch", "--rclone", str(fake_rclone.install(tmp_path)), "--state-dir", str(tmp_path / "state")]
    root = ["--root", f"bybit-linear={tmp_path / 'local'}"]
    cases = [
        (["--remote-base", REMOTE_BASE, "--plan", str(broken), *root], "not readable JSON"),
        (["--remote-base", REMOTE_BASE, "--plan", str(tmp_path / "absent.json"), *root], "not readable JSON"),
        (["--remote-base", REMOTE_BASE, "--plan", str(wrong), *root], "schema 1 tape plan"),
        (["--remote-base", REMOTE_BASE, "--plan", str(bad_hour), *root], "not YYYY-MM-DD and HH"),
        (["--remote-base", REMOTE_BASE, "--plan", str(bad_symbol), *root], "not a list of symbols"),
        (["--remote-base", "no-colon", "--plan", str(good), *root], "rclone remote"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good)], "--root TAPE=DIR"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), "--root", "a/b=/x"], "TAPE=DIR"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), *root, *root], "twice"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), *root, "--owner", "no-such-user-here:root"], "no such user"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), *root, "--owner", "root"], "USER:GROUP"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), *root, "--keep-hours", "-1"], "zero or more"),
        (["--remote-base", REMOTE_BASE, "--plan", str(good), *root, "--bwlimit-kibps", "0"], "whole number of KiB/s"),
    ]
    for arguments, said in cases:
        done = subprocess.run([*base, *arguments], cwd=ROOT, capture_output=True, text=True, env=_environment(tmp_path), check=False)
        assert done.returncode == 2, (arguments, done.stderr)
        assert done.stdout == "", arguments
        assert said in done.stderr, (arguments, done.stderr)
    assert not (tmp_path / "rclone.log").exists(), "a run that could not start touched the box"


def test_a_planned_tape_without_a_root_fails_alone(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    plan = _plan(tmp_path, {**_wanted(["BTCUSDT"]), "binance-usdm": [{"day": DAY, "hour": HOUR, "symbols": ["BTCUSDT"]}]})

    _done, summary = _fetch(tmp_path, plan)

    by_tape = {row["tape"]: row for row in summary["hours"]}
    assert (by_tape["binance-usdm"]["status"], by_tape["binance-usdm"]["reason"]) == ("failed", "no_root")
    assert by_tape["bybit-linear"]["status"] == "done"
    assert summary["failed"] == 1


def test_an_index_cannot_write_outside_the_tape_or_past_the_tar(tmp_path: Path) -> None:
    _standard_hour(tmp_path)
    tar = _box_tar(tmp_path)
    index_path = tar.with_name(tar.name + ".index.json")
    index = json.loads(index_path.read_bytes())
    btc = next(member for member in index["members"] if member["path"] == "BTCUSDT/segment-000000.jsonl.zst")
    index["members"].append({**btc, "path": "BTCUSDT/../../../escape/segment-000000.jsonl.zst"})
    index_path.write_text(json.dumps(index), encoding="utf-8")

    _done, summary = _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT"])))

    assert summary["hours"][0]["status"] == "done"
    assert not list(tmp_path.rglob("escape"))

    lying = dict(index, members=[{**btc, "offset": index["tar_bytes"]}])
    index_path.write_text(json.dumps(lying), encoding="utf-8")
    shutil.rmtree(tmp_path / "local" / ".state" / "indexes")
    _done, refused = _fetch(tmp_path, _plan(tmp_path, _wanted(["ETHUSDT"])))
    assert refused["hours"][0]["status"] == "failed" and refused["hours"][0]["reason"].startswith("index: member")


@pytest.mark.skipif(os.geteuid() != 0, reason="handing files to another user needs root")
def test_what_the_fetch_writes_belongs_to_the_owner_it_names(tmp_path: Path) -> None:
    _standard_hour(tmp_path)

    _fetch(tmp_path, _plan(tmp_path, _wanted(["BTCUSDT"])), "--owner", "65534:65534")

    for path in (tmp_path / "local" / "bybit-linear", _local(tmp_path, ""), _local(tmp_path, "BTCUSDT/segment-000000.jsonl.zst")):
        assert (path.stat().st_uid, path.stat().st_gid) == (65534, 65534), path


def test_two_tapes_fetch_newest_hour_first_into_their_own_roots(tmp_path: Path) -> None:
    bybit = {"BTCUSDT/segment-000000.jsonl.zst": b"bybit-13"}
    _hour(tmp_path / "bybit", bybit)
    _hour(tmp_path / "bybit", {"BTCUSDT/segment-000000.jsonl.zst": b"bybit-12"}, hour="12")
    _hour(tmp_path / "binance", {"BTCUSDT/segment-000000.jsonl.zst": b"binance-13"})
    _pack(tmp_path, "bybit-linear", tmp_path / "bybit", now="2026-09-25T14:10:00")
    _pack(tmp_path, "binance-usdm", tmp_path / "binance", now="2026-09-25T14:10:00")
    plan = _plan(
        tmp_path,
        {
            "bybit-linear": [{"day": DAY, "hour": "12", "symbols": ["BTCUSDT"]}, {"day": DAY, "hour": "13", "symbols": ["BTCUSDT"]}],
            "binance-usdm": [{"day": DAY, "hour": "13", "symbols": ["BTCUSDT"]}],
        },
    )

    _done, summary = _fetch(tmp_path, plan, "--root", f"binance-usdm={tmp_path / 'local' / 'binance-usdm'}")

    assert [(row["tape"], row["hour"], row["status"]) for row in summary["hours"]] == [
        ("bybit-linear", "13", "done"),
        ("binance-usdm", "13", "done"),
        ("bybit-linear", "12", "done"),
    ]
    assert _local(tmp_path, "BTCUSDT/segment-000000.jsonl.zst").read_bytes() == b"bybit-13"
    assert _local(tmp_path, "BTCUSDT/segment-000000.jsonl.zst", hour="12").read_bytes() == b"bybit-12"
    binance = tmp_path / "local" / "binance-usdm" / DAY / "13" / "BTCUSDT" / "segment-000000.jsonl.zst"
    assert binance.read_bytes() == b"binance-13"
    assert hashlib.sha256(binance.read_bytes()).hexdigest() in (tmp_path / "local" / ".state" / "fetched-members.jsonl").read_text()
