"""The hourly packer: what it packs, what it leaves, and what it proves landed."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from market_tape import pack
from market_tape.storage import lock_root, unlock_root
from tests.market_tape import fake_rclone
from tests.market_tape.test_tape_storage import cached_pages


ROOT = Path(pack.__file__).resolve().parents[1]
REMOTE = "storagebox:market-capture/market-tape/bybit-linear"
REMOTE_BASE = "storagebox:market-capture/market-tape"

def _fake_rclone(tmp_path: Path) -> Path:
    return fake_rclone.install(tmp_path)


def _segment(root: Path, day: str, hour: str, symbol: str, index: int, payload: bytes = b"zst-bytes") -> Path:
    path = root / day / hour / symbol / f"segment-{index:06d}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _raw_segment(root: Path, day: str, hour: str, symbol: str, index: int, rows: int = 2) -> Path:
    """What a stopped recorder leaves: a closed segment nothing compressed."""

    path = root / day / hour / symbol / f"segment-{index:06d}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        b"".join(
            json.dumps(
                {"kind": "public_trade", "symbol": symbol, "local_receive_ts_ns": 1_788_000_000_000_000_000 + offset}
            ).encode()
            + b"\n"
            for offset in range(rows)
        )
    )
    return path


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


def _run(
    tmp_path: Path, *tape_arguments: str, now: str, corrupt: bool | str = False, box: str = "u1.box.example"
) -> subprocess.CompletedProcess[str]:
    """One pack run against `box`: a box of its own name keeps its own remote directory."""

    env = {
        **os.environ,
        "FAKE_RCLONE_LOG": str(tmp_path / "rclone.log"),
        "FAKE_RCLONE_HASH_LOG": str(tmp_path / "hashed.log"),
        "FAKE_REMOTE_DIR": str(tmp_path / ("remote" if box == "u1.box.example" else f"remote-{box}")),
        "FAKE_RCLONE_CORRUPT": corrupt if isinstance(corrupt, str) else "1" if corrupt else "0",
    }
    config = tmp_path / "rclone.conf"
    config.write_text(f"[storagebox]\ntype = sftp\nhost = {box}\nuser = u1\nport = 23\n", encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "market_tape",
            "pack",
            *tape_arguments,
            "--state-dir",
            str(tmp_path / "state"),
            "--stamp-file",
            str(tmp_path / "receipts" / "market-tape-upload.last-success"),
            "--rclone",
            str(_fake_rclone(tmp_path)),
            "--config",
            str(config),
            "--now",
            str(_epoch(now)),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def _single_tape(tmp_path: Path) -> tuple[str, ...]:
    return ("--root", str(tmp_path / "tape"), "--remote", REMOTE)


def _ledger(tmp_path: Path) -> list[dict[str, object]]:
    path = tmp_path / "state" / "uploaded-tapes.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_only_hours_that_ended_and_closed_are_packed(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0)
    _segment(root, "2026-09-02", "10", "ETHUSDT", 0)
    (root / "2026-09-02" / "10" / "_meta").mkdir()
    (root / "2026-09-02" / "10" / "_meta" / "instruments-x.json.zst").write_bytes(b"meta")
    # Hour 11 has a segment still open: not finished.
    _segment(root, "2026-09-02", "11", "BTCUSDT", 0)
    (root / "2026-09-02" / "11" / "BTCUSDT" / "segment-000001.jsonl.partial").write_bytes(b"open")
    # Hour 12 is the current hour.
    _segment(root, "2026-09-02", "12", "BTCUSDT", 0)

    now = _epoch("2026-09-02T12:20:00")
    names = [c.name for c in pack.finished_candidates(root, now=now, grace_seconds=300)]

    assert names == ["2026-09-02T10Z"]
    # Hour 11 ended at 12:00 but is still open; with the partial gone it packs.
    (root / "2026-09-02" / "11" / "BTCUSDT" / "segment-000001.jsonl.partial").unlink()
    names = [c.name for c in pack.finished_candidates(root, now=now, grace_seconds=300)]
    assert names == ["2026-09-02T10Z", "2026-09-02T11Z"]
    # Inside the grace window the hour is not yet packed.
    names = [c.name for c in pack.finished_candidates(root, now=_epoch("2026-09-02T12:03:00"), grace_seconds=300)]
    assert "2026-09-02T11Z" not in names


def test_an_hour_ships_as_one_archive_with_a_manifest_and_is_ledgered(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-0")
    _segment(root, "2026-09-02", "10", "BTCUSDT", 1, b"btc-1")
    _segment(root, "2026-09-02", "10", "ETHUSDT", 0, b"eth-0")
    (root / "manifest.jsonl").write_text(
        json.dumps(
            {
                "kind": "segment_compressed",
                "path": "2026-09-02/10/BTCUSDT/segment-000000.jsonl.zst",
                "symbol": "BTCUSDT",
                "records": 42,
                "first_receive_ns": 1,
                "last_receive_ns": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    remote_tar = tmp_path / "remote" / "market-capture/market-tape/bybit-linear/2026/09/02/2026-09-02T10Z.tar"
    assert remote_tar.exists()
    # Every rclone call reads the config it was given, where it is.
    calls = (tmp_path / "rclone.log").read_text().splitlines()
    assert calls and all(call.endswith(f"--config {tmp_path / 'rclone.conf'}") for call in calls)
    # The index lands beside the tar, and first: a reader that finds the tar finds its index.
    uploads = [call.split()[2] for call in calls if call.startswith("copyto")]
    assert uploads == [f"{REMOTE}/2026/09/02/2026-09-02T10Z.tar.index.json", f"{REMOTE}/2026/09/02/2026-09-02T10Z.tar"]
    index = json.loads(remote_tar.with_name("2026-09-02T10Z.tar.index.json").read_bytes())
    assert index["tar_bytes"] == remote_tar.stat().st_size
    tar_bytes = remote_tar.read_bytes()
    by_path = {member["path"]: member for member in index["members"]}
    assert tar_bytes[by_path["BTCUSDT/segment-000001.jsonl.zst"]["offset"] :][:5] == b"btc-1"
    with tarfile.open(remote_tar) as archive:
        names = archive.getnames()
        assert names[0] == "MANIFEST.json"
        assert set(names[1:]) == {
            "BTCUSDT/segment-000000.jsonl.zst",
            "BTCUSDT/segment-000001.jsonl.zst",
            "ETHUSDT/segment-000000.jsonl.zst",
        }
        manifest = json.load(archive.extractfile("MANIFEST.json"))
        assert archive.extractfile("BTCUSDT/segment-000001.jsonl.zst").read() == b"btc-1"
    assert manifest["kind"] == "market_tape_hour"
    assert manifest["day"] == "2026-09-02" and manifest["hour"] == "10"
    # The single-tape form takes its tape name from the last part of the remote.
    assert manifest["tape"] == "bybit-linear"
    assert manifest["symbols"] == ["BTCUSDT"]  # only the receipted file names its symbol
    first = next(row for row in manifest["files"] if row["path"] == "BTCUSDT/segment-000000.jsonl.zst")
    assert first["records"] == 42
    assert first["sha256"] == hashlib.sha256(b"btc-0").hexdigest()
    ledger = _ledger(tmp_path)
    assert [row["name"] for row in ledger] == ["2026-09-02T10Z"]
    assert ledger[0]["tape"] == "bybit-linear"
    assert ledger[0]["remote_path"] == f"{REMOTE}/2026/09/02/2026-09-02T10Z.tar"
    assert ledger[0]["md5"] == hashlib.md5(remote_tar.read_bytes()).hexdigest()
    stamp = (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()
    assert "archives=bybit-linear/2026-09-02T10Z" in stamp
    assert "file_count=3" in stamp
    assert f"remote_free_bytes={4 * 1024**4}" in stamp
    assert f"destination={REMOTE}" in stamp
    # The staged tar is gone; the hour is inside the default 24 h window, so its segments stay.
    assert not list((tmp_path / "state" / "staging").glob("*.tar"))
    assert (root / "2026-09-02" / "10" / "BTCUSDT" / "segment-000000.jsonl.zst").exists()
    # A second run ships nothing new and does not re-upload.
    log_before = (tmp_path / "rclone.log").read_text()
    again = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:20:00")
    assert again.returncode == 0, again.stderr
    assert (tmp_path / "rclone.log").read_text().count("copyto") == log_before.count("copyto")


def test_a_listing_hashes_only_the_hour_it_proves(tmp_path: Path) -> None:
    """The box reads a file whole for every hash a listing asks of it, and a
    day's directory holds every tar of that day: a listing of the directory
    re-read each earlier hour of the day on every run."""

    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-10")
    assert _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00").returncode == 0
    _segment(root, "2026-09-02", "11", "BTCUSDT", 0, b"btc-11")
    (tmp_path / "hashed.log").unlink()

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T12:10:00")

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "hashed.log").read_text().split() == ["2026-09-02T11Z.tar", "2026-09-02T11Z.tar.index.json"]
    assert [row["name"] for row in _ledger(tmp_path)] == ["2026-09-02T10Z", "2026-09-02T11Z"]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="posix_fadvise and mincore are Linux's")
def test_a_packed_hours_segments_leave_the_page_cache(tmp_path: Path) -> None:
    # The run's cgroup pays for every page it reads, and nothing on the host
    # reads a segment again once it is in the tar.
    root = tmp_path / "tape"
    segments = [_segment(root, "2026-09-02", "10", f"SYM{index}USDT", 0, os.urandom(256 * 1024)) for index in range(4)]
    for path in segments:
        # Durable and cached, as the recorder leaves a segment it just compressed.
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    assert all(cached_pages(path) for path in segments)

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    assert [row["name"] for row in _ledger(tmp_path)] == ["2026-09-02T10Z"]
    assert [cached_pages(path) for path in segments] == [0] * len(segments)


def test_an_idle_root_is_finished_by_the_run_and_the_hour_ships(tmp_path: Path) -> None:
    """The recorder is stopped, so nobody else will ever compress these; one raw
    file under the hour is what kept the whole hour from packing."""

    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-0")
    raw = _raw_segment(root, "2026-09-02", "10", "BTCUSDT", 1)

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    assert not raw.exists()
    assert raw.with_name(raw.name + ".zst").exists()
    assert f"recovered 1 raw segment(s) under {root.resolve()}" in result.stdout
    assert "compressed=1 failed=0" in result.stdout
    remote_tar = tmp_path / "remote" / "market-capture/market-tape/bybit-linear/2026/09/02/2026-09-02T10Z.tar"
    with tarfile.open(remote_tar) as archive:
        assert set(archive.getnames()) == {
            "MANIFEST.json",
            "BTCUSDT/segment-000000.jsonl.zst",
            "BTCUSDT/segment-000001.jsonl.zst",
        }
    receipt = json.loads((root / "manifest.jsonl").read_text(encoding="utf-8"))
    assert receipt["kind"] == "segment_compressed" and receipt["records"] == 2
    stamp = (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()
    assert "recovered_segments=1" in stamp


def test_a_recorder_holding_the_root_keeps_its_own_raw_segments(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-0")
    raw = _raw_segment(root, "2026-09-02", "10", "BTCUSDT", 1)

    held = lock_root(root)
    assert held is not None
    try:
        result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00")
    finally:
        unlock_root(held)

    assert result.returncode == 0, result.stderr
    assert raw.exists()
    assert f"a recorder holds {root.resolve()}; its compressor owns the 1 raw segment(s)" in result.stdout
    assert "shipped 0 archives" in result.stdout
    assert not (tmp_path / "state" / "uploaded-tapes.jsonl").exists()
    assert not list((tmp_path / "remote").rglob("*.tar"))


def test_a_recovery_that_runs_out_of_time_lets_go_of_nothing_it_still_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The lock says nobody else writes raw files under this root, so the run
    may not walk away with its compressor still working."""

    binary = tmp_path / "bin" / "zstd"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\nsleep 1\nexit 7\n", encoding="utf-8")
    binary.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binary.parent}{os.pathsep}{os.environ['PATH']}")
    root = tmp_path / "tape"
    raw = _raw_segment(root, "2026-09-02", "10", "BTCUSDT", 0)

    result = pack.recover_idle_root(pack.Tape("bybit-linear", root, REMOTE), timeout=0.0, dry_run=False)

    assert [thread for thread in threading.enumerate() if thread.name == "tape-compressor"] == []
    assert result["recovered"] == 0
    assert raw.exists(), "the segment stays raw for a later run"
    assert "did not stop within 0s" in capsys.readouterr().err


def test_a_dry_run_names_what_it_would_recover(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    raw = _raw_segment(root, "2026-09-02", "10", "BTCUSDT", 0)

    result = _run(tmp_path, *_single_tape(tmp_path), "--dry-run", now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    assert f"would recover 1 raw segment(s) under {root.resolve()} (root idle)" in result.stdout
    assert raw.exists()


def test_a_corrupted_upload_is_not_ledgered_and_leaves_no_receipt(tmp_path: Path) -> None:
    _segment(tmp_path / "tape", "2026-09-02", "10", "BTCUSDT", 0)

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00", corrupt=True)

    assert result.returncode != 0
    assert "not the uploaded" in result.stderr
    assert not (tmp_path / "state" / "uploaded-tapes.jsonl").exists()
    assert not (tmp_path / "receipts" / "market-tape-upload.last-success").exists()
    assert not list((tmp_path / "state" / "staging").iterdir())


def test_an_index_that_does_not_land_fails_the_hour_as_a_tar_would(tmp_path: Path) -> None:
    """The tar arrived whole; its index did not. The hour is not shipped: a
    reader on the slow line has nothing to range-read it by."""

    _segment(tmp_path / "tape", "2026-09-02", "10", "BTCUSDT", 0)

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:10:00", corrupt="index")

    assert result.returncode != 0
    assert "2026-09-02T10Z.tar.index.json as size=17" in result.stderr
    assert not (tmp_path / "state" / "uploaded-tapes.jsonl").exists()
    assert not (tmp_path / "receipts" / "market-tape-upload.last-success").exists()
    assert not list((tmp_path / "state" / "staging").iterdir())
    # A run whose index lands ships the hour, index and tar again.
    again = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:20:00")
    assert again.returncode == 0, again.stderr
    assert [row["name"] for row in _ledger(tmp_path)] == ["2026-09-02T10Z"]


def test_the_index_gives_every_members_data_offset_and_digest(tmp_path: Path) -> None:
    """A range read at the index's offset and size is the member's file, byte
    for byte: past its tar header and past a PAX header when a long name needs one."""

    root = tmp_path / "tape"
    long_symbol = "A" * 120 + "USDT"  # past ustar's 100-byte name: a PAX header precedes it
    files = {
        "BTCUSDT/segment-000000.jsonl.zst": os.urandom(1_000),
        "BTCUSDT/segment-000001.jsonl.zst": os.urandom(513),
        f"{long_symbol}/segment-000000.jsonl.zst": os.urandom(7),
        "ETHUSDT/segment-000000.jsonl.zst": b"",
        "_meta/coverage-10-20260902T100000Z.json.zst": os.urandom(64),
    }
    for relative, payload in files.items():
        path = root / "2026-09-02" / "10" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    receipts = {
        "2026-09-02/10/BTCUSDT/segment-000000.jsonl.zst": {
            "symbol": "BTCUSDT",
            "sha256": hashlib.sha256(files["BTCUSDT/segment-000000.jsonl.zst"]).hexdigest(),
        }
    }
    candidate = pack.Candidate("2026-09-02T10Z", "2026-09-02", "10", (root / "2026-09-02" / "10",))
    archive, manifest = pack.build_archive(candidate, root, tmp_path / "staging", receipts, tape="bybit-linear")

    path, index = pack.build_index(archive, manifest, tape="bybit-linear")

    assert path == archive.with_name("2026-09-02T10Z.tar.index.json")
    assert json.loads(path.read_bytes()) == index
    assert (index["kind"], index["schema"], index["name"], index["tape"]) == (
        "market_tape_hour_index", 1, "2026-09-02T10Z", "bybit-linear"
    )
    assert index["tar_bytes"] == archive.stat().st_size
    data = archive.read_bytes()
    members = {member["path"]: member for member in index["members"]}
    assert set(members) == {"MANIFEST.json", *files}
    for relative, payload in files.items():
        member = members[relative]
        assert data[member["offset"] : member["offset"] + member["bytes"]] == payload, relative
        assert member["sha256"] == hashlib.sha256(payload).hexdigest(), relative
    manifest_member = members["MANIFEST.json"]
    manifest_bytes = data[manifest_member["offset"] : manifest_member["offset"] + manifest_member["bytes"]]
    assert json.loads(manifest_bytes)["name"] == "2026-09-02T10Z"
    assert manifest_member["sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
    # The receipt's symbol, else the segment's directory; nothing for `_meta` or the manifest.
    assert members["BTCUSDT/segment-000000.jsonl.zst"]["symbol"] == "BTCUSDT"
    assert members[f"{long_symbol}/segment-000000.jsonl.zst"]["symbol"] == long_symbol
    assert members["_meta/coverage-10-20260902T100000Z.json.zst"]["symbol"] is None
    assert manifest_member["symbol"] is None


def test_two_tapes_ship_under_their_own_folders_and_the_ledger_keys_by_remote_path(tmp_path: Path) -> None:
    bybit = tmp_path / "bybit"
    binance = tmp_path / "binance"
    binance.mkdir(parents=True)
    _segment(bybit, "2026-09-02", "10", "BTCUSDT", 0, b"bybit-btc")
    tapes = ("--tape", f"bybit-linear={bybit}", "--tape", f"binance-usdm={binance}", "--remote-base", REMOTE_BASE)

    first = _run(tmp_path, *tapes, now="2026-09-02T11:10:00")
    assert first.returncode == 0, first.stderr

    # The same hour on the second tape ships after the first tape's hour is ledgered.
    _segment(binance, "2026-09-02", "10", "BTCUSDT", 0, b"binance-btc")
    second = _run(tmp_path, *tapes, now="2026-09-02T11:20:00")
    assert second.returncode == 0, second.stderr

    remote = tmp_path / "remote" / "market-capture" / "market-tape"
    bybit_tar = remote / "bybit-linear" / "2026/09/02/2026-09-02T10Z.tar"
    binance_tar = remote / "binance-usdm" / "2026/09/02/2026-09-02T10Z.tar"
    assert bybit_tar.exists() and binance_tar.exists()
    with tarfile.open(binance_tar) as archive:
        manifest = json.load(archive.extractfile("MANIFEST.json"))
        assert archive.extractfile("BTCUSDT/segment-000000.jsonl.zst").read() == b"binance-btc"
    assert manifest["tape"] == "binance-usdm"
    ledger = _ledger(tmp_path)
    assert [row["name"] for row in ledger] == ["2026-09-02T10Z", "2026-09-02T10Z"]
    assert [row["tape"] for row in ledger] == ["bybit-linear", "binance-usdm"]
    assert [row["remote_path"] for row in ledger] == [
        f"{REMOTE_BASE}/bybit-linear/2026/09/02/2026-09-02T10Z.tar",
        f"{REMOTE_BASE}/binance-usdm/2026/09/02/2026-09-02T10Z.tar",
    ]
    # Two hours, each its index and its tar.
    assert (tmp_path / "rclone.log").read_text().count("copyto") == 4
    stamp = (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()
    assert "archives=binance-usdm/2026-09-02T10Z" in stamp
    assert "tapes=bybit-linear,binance-usdm" in stamp
    assert f"destination={REMOTE_BASE}" in stamp

    # A third run has nothing left for either tape.
    third = _run(tmp_path, *tapes, now="2026-09-02T11:30:00")
    assert third.returncode == 0, third.stderr
    assert (tmp_path / "rclone.log").read_text().count("copyto") == 4
    assert "archives=none" in (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()


def test_a_tape_whose_root_is_missing_is_noted_and_the_others_ship(tmp_path: Path) -> None:
    bybit = tmp_path / "bybit"
    _segment(bybit, "2026-09-02", "10", "BTCUSDT", 0)
    absent = tmp_path / "binance"

    result = _run(
        tmp_path,
        "--tape",
        f"bybit-linear={bybit}",
        "--tape",
        f"binance-usdm={absent}",
        "--remote-base",
        REMOTE_BASE,
        now="2026-09-02T11:10:00",
    )

    assert result.returncode == 0, result.stderr
    assert "binance-usdm has no root yet" in result.stderr
    assert (tmp_path / "remote" / "market-capture/market-tape/bybit-linear/2026/09/02/2026-09-02T10Z.tar").exists()
    assert [row["tape"] for row in _ledger(tmp_path)] == ["bybit-linear"]
    assert "tapes=bybit-linear" in (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()


def test_no_tape_root_at_all_ships_nothing(tmp_path: Path) -> None:
    result = _run(tmp_path, "--tape", f"bybit-linear={tmp_path / 'nowhere'}", "--remote-base", REMOTE_BASE, now="2026-09-02T11:10:00")

    assert result.returncode == 2
    assert "no tape root exists" in result.stderr
    assert not (tmp_path / "receipts" / "market-tape-upload.last-success").exists()


def test_a_tape_needs_a_name_a_root_and_a_remote(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def namespace(**values: object) -> argparse.Namespace:
        return argparse.Namespace(**{"tape": [], "remote_base": None, "root": None, "remote": None, **values})

    def refused(text: str, **values: object) -> None:
        # A malformed invocation exits 2 and says why on stderr.
        with pytest.raises(SystemExit) as excinfo:
            pack.parse_tapes(namespace(**values))
        assert excinfo.value.code == 2
        assert text in capsys.readouterr().err

    refused("at least one tape")
    refused("remote-base", tape=["bybit=/var/tape"])
    refused("NAME=ROOT", tape=["/var/tape"], remote_base=REMOTE_BASE)
    refused("NAME=ROOT", tape=["a/b=/var/tape"], remote_base=REMOTE_BASE)
    refused("go together", root=Path("/var/tape"))

    both = pack.parse_tapes(namespace(tape=["bybit=/var/tape"], remote_base=REMOTE_BASE + "/", root=Path("/var/other"), remote=REMOTE))
    assert [(tape.name, tape.root, tape.remote) for tape in both] == [
        ("bybit", Path("/var/tape").resolve(), f"{REMOTE_BASE}/bybit"),
        ("bybit-linear", Path("/var/other").resolve(), REMOTE),
    ]


def test_the_stamp_sums_the_last_thirty_days_of_uploads() -> None:
    ledger = {
        "r/a": {"uploaded_at": "2026-09-01T10:00:00Z", "bytes": 100},
        "r/b": {"uploaded_at": "2026-08-01T10:00:00Z", "bytes": 1000},
        "r/c": {"uploaded_at": "2026-09-02T00:00:00Z", "bytes": 5},
        "r/d": {"uploaded_at": "garbage", "bytes": 7},
    }
    since = datetime(2026, 8, 3, tzinfo=timezone.utc).timestamp()
    assert pack.bytes_uploaded_since(ledger, since) == 105
    assert pack.bytes_uploaded_since({}, since) == 0


def test_a_failed_archive_build_leaves_no_partial_archive_in_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partial `.tar.tmp` is disk no retention pass can see: staging is outside
    both tape roots, on the filesystem the recorders' free-space floor guards."""

    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0)
    staging = tmp_path / "state" / "staging"
    candidate = pack.Candidate("2026-09-02T10Z", "2026-09-02", "10", (root / "2026-09-02" / "10",))
    added = 0
    real_addfile = tarfile.TarFile.addfile

    def full_disk(self, tarinfo, fileobj=None):  # noqa: ANN001, ANN202 - test double
        nonlocal added
        added += 1
        if added > 1:  # the manifest lands, then the disk fills
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_addfile(self, tarinfo, fileobj)

    monkeypatch.setattr(tarfile.TarFile, "addfile", full_disk)

    with pytest.raises(OSError):
        pack.build_archive(candidate, root, staging, {})

    assert list(staging.iterdir()) == []


def test_a_run_reclaims_the_staging_a_killed_run_left_behind(tmp_path: Path) -> None:
    _segment(tmp_path / "tape", "2026-09-02", "10", "BTCUSDT", 0)
    staging = tmp_path / "state" / "staging"
    staging.mkdir(parents=True)
    orphans = (
        staging / "2026-09-02T09Z.tar",
        staging / ".2026-09-02T08Z.tar.tmp",
        staging / "2026-09-02T09Z.tar.index.json",
        staging / ".2026-09-02T09Z.tar.index.json.tmp",
    )
    for orphan in orphans:
        orphan.write_bytes(b"x" * 1024)
    keep = staging / "notes.txt"
    keep.write_bytes(b"not an archive")

    result = _run(tmp_path, *_single_tape(tmp_path), now="2026-09-02T11:20:00")

    assert result.returncode == 0, result.stderr
    assert "removed stale staging archive 2026-09-02T09Z.tar bytes=1024" in result.stdout
    assert "removed stale staging archive .2026-09-02T08Z.tar.tmp bytes=1024" in result.stdout
    for orphan in orphans:
        assert not orphan.exists()
    assert keep.exists()
    # The run still ships its own hour.
    assert [row["name"] for row in _ledger(tmp_path)] == ["2026-09-02T10Z"]


def test_a_shipped_hour_leaves_the_disk_once_it_is_older_than_the_window(tmp_path: Path) -> None:
    """The local tape is a sliding window over what the storage box already holds:
    a ledgered hour's segments go once the hour has been over `--keep-hours`,
    its `_meta` snapshots stay, and an hour inside the window is untouched."""

    root = tmp_path / "tape"
    old_segment = _segment(root, "2026-09-02", "08", "BTCUSDT", 0, b"btc-08")
    _segment(root, "2026-09-02", "08", "ETHUSDT", 0, b"eth-08")
    meta = root / "2026-09-02" / "08" / "_meta" / "instruments-20260902T080000Z.json.zst"
    meta.parent.mkdir()
    meta.write_bytes(b"tables")
    recent_segment = _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-10")
    (root / "manifest.jsonl").write_text("", encoding="utf-8")

    # 11:10: hour 08 has been over for 2h10m, hour 10 for 10 minutes.
    result = _run(tmp_path, *_single_tape(tmp_path), "--keep-hours", "1", now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    assert [row["name"] for row in _ledger(tmp_path)] == ["2026-09-02T08Z", "2026-09-02T10Z"]
    assert "pruned shipped 2026-09-02T08Z files=2 bytes=12" in result.stdout
    assert not old_segment.exists()
    assert not (root / "2026-09-02" / "08" / "ETHUSDT").exists()
    assert meta.exists()
    assert recent_segment.exists()
    receipts = [json.loads(line) for line in (root / "manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {(row["kind"], row["reason"]) for row in receipts} == {("segment_deleted", "shipped")}
    assert {row["path"] for row in receipts} == {
        "2026-09-02/08/BTCUSDT/segment-000000.jsonl.zst",
        "2026-09-02/08/ETHUSDT/segment-000000.jsonl.zst",
    }
    assert all(row["remote_path"] == f"{REMOTE}/2026/09/02/2026-09-02T08Z.tar" for row in receipts)
    stamp = (tmp_path / "receipts" / "market-tape-upload.last-success").read_text()
    assert "keep_hours=1.0" in stamp
    assert "pruned_hours=1" in stamp
    assert "pruned_bytes=12" in stamp

    # 12:20: hour 10 has now been over for 1h20m. It goes without a re-upload,
    # and the hour that only holds `_meta` is not pruned twice.
    copies_before = (tmp_path / "rclone.log").read_text().count("copyto")
    again = _run(tmp_path, *_single_tape(tmp_path), "--keep-hours", "1", now="2026-09-02T12:20:00")

    assert again.returncode == 0, again.stderr
    assert (tmp_path / "rclone.log").read_text().count("copyto") == copies_before
    assert "pruned shipped 2026-09-02T10Z files=1 bytes=6" in again.stdout
    assert "pruned shipped 2026-09-02T08Z" not in again.stdout
    assert not recent_segment.exists()
    assert not (root / "2026-09-02" / "10").exists()
    assert meta.exists()



def test_a_new_box_gets_every_hour_still_on_disk_before_the_window_takes_it(tmp_path: Path) -> None:
    """The ledger licenses a delete only for the box that confirmed the hour:
    once the rclone config names another box, each hour still on disk ships to
    that box, and the window takes an hour only after that box confirmed it."""

    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "06", "BTCUSDT", 0, b"btc-06")
    meta = root / "2026-09-02" / "06" / "_meta" / "instruments-20260902T060000Z.json.zst"
    meta.parent.mkdir()
    meta.write_bytes(b"tables")
    old_hour = _segment(root, "2026-09-02", "08", "BTCUSDT", 0, b"btc-08")
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0, b"btc-10")

    # 11:10 on the old box: all three hours ship; 06 is 4h10m over and leaves
    # the disk but for its `_meta`, the other two are inside the window.
    first = _run(tmp_path, *_single_tape(tmp_path), "--keep-hours", "3", now="2026-09-02T11:10:00")
    assert first.returncode == 0, first.stderr
    assert "pruned shipped 2026-09-02T06Z" in first.stdout
    assert old_hour.exists() and meta.exists()

    # 12:20 on the new box, which refuses what it is sent: hour 08 is 3h20m
    # over, and the old box's word does not take it.
    refused = _run(
        tmp_path, *_single_tape(tmp_path), "--keep-hours", "3", now="2026-09-02T12:20:00",
        box="u2.box.example", corrupt=True,
    )
    assert refused.returncode != 0
    assert old_hour.exists()

    # 12:30 on the new box, taking uploads: both hours ship to it, then 08 goes.
    second = _run(
        tmp_path, *_single_tape(tmp_path), "--keep-hours", "3", now="2026-09-02T12:30:00", box="u2.box.example"
    )
    assert second.returncode == 0, second.stderr
    # Hour 06's tape is on the old box alone: its `_meta` does not ship as the hour.
    new_box = tmp_path / "remote-u2.box.example" / "market-capture" / "market-tape" / "bybit-linear" / "2026/09/02"
    assert sorted(path.name for path in new_box.iterdir()) == [
        "2026-09-02T08Z.tar",
        "2026-09-02T08Z.tar.index.json",
        "2026-09-02T10Z.tar",
        "2026-09-02T10Z.tar.index.json",
    ]
    assert "pruned shipped 2026-09-02T08Z files=1" in second.stdout
    assert not old_hour.exists()
    assert [(row["name"], row["box"]) for row in _ledger(tmp_path)] == [
        ("2026-09-02T06Z", "sftp://u1@u1.box.example:23"),
        ("2026-09-02T08Z", "sftp://u1@u1.box.example:23"),
        ("2026-09-02T10Z", "sftp://u1@u1.box.example:23"),
        ("2026-09-02T08Z", "sftp://u1@u2.box.example:23"),
        ("2026-09-02T10Z", "sftp://u1@u2.box.example:23"),
    ]

def test_the_window_only_deletes_what_the_ledger_says_the_store_holds(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    unshipped = _segment(root, "2026-08-01", "00", "BTCUSDT", 0)
    shipped = _segment(root, "2026-08-01", "01", "BTCUSDT", 0)
    ledger = {f"{REMOTE}/2026/08/01/2026-08-01T01Z.tar": {"name": "2026-08-01T01Z"}}
    now = _epoch("2026-09-02T12:00:00")

    # A month old and never ledgered: the recorder's retention decides, not this.
    pruned = pack.prune_shipped(root, {}, remote=REMOTE, now=now, keep_hours=0, grace_seconds=300)
    assert pruned == []
    assert unshipped.exists() and shipped.exists()

    # Ledgered but still inside the window: untouched.
    pruned = pack.prune_shipped(root, ledger, remote=REMOTE, now=now, keep_hours=24 * 40, grace_seconds=300)
    assert pruned == []
    assert shipped.exists()

    # Ledgered and past the window: only that hour goes.
    pruned = pack.prune_shipped(root, ledger, remote=REMOTE, now=now, keep_hours=24, grace_seconds=300)
    assert [(row["name"], row["file_count"]) for row in pruned] == [("2026-08-01T01Z", 1)]
    assert unshipped.exists()
    assert not shipped.exists()
    assert not (root / "2026-08-01" / "01").exists()
    assert (root / "2026-08-01").exists()


def test_a_dry_run_names_the_shipped_hours_the_window_would_take(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    _segment(root, "2026-09-02", "08", "BTCUSDT", 0)
    _segment(root, "2026-09-02", "10", "BTCUSDT", 0)
    state = tmp_path / "state"
    state.mkdir()
    (state / "uploaded-tapes.jsonl").write_text(
        json.dumps(
            {
                "name": "2026-09-02T08Z",
                "remote_path": f"{REMOTE}/2026/09/02/2026-09-02T08Z.tar",
                "box": "sftp://u1@u1.box.example:23",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = _run(tmp_path, *_single_tape(tmp_path), "--keep-hours", "1", "--dry-run", now="2026-09-02T11:10:00")

    assert result.returncode == 0, result.stderr
    assert "would pack bybit-linear 2026-09-02T10Z" in result.stdout
    assert "would prune bybit-linear 2026-09-02T08Z (shipped, over 1h ago)" in result.stdout
    assert "1 pending, 1 already shipped, 1 shipped hours past the window" in result.stdout
    assert (root / "2026-09-02" / "08" / "BTCUSDT" / "segment-000000.jsonl.zst").exists()
