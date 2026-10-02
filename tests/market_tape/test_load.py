"""Reading a tape back: hours, sources, venues, the rclone cache, filters, and bad lines."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from market_tape import load
from market_tape.load import (
    ArchiveDir,
    CacheError,
    HostRoot,
    RcloneRemote,
    RemoteCommandError,
    RemoteTimeout,
    SourceError,
    TapeRowError,
    hour_range,
    iter_rows,
    iter_snapshots,
    open_source,
)
from market_tape.schema import book_row, trade_row
from market_tape.storage import zstd_compress

FIXTURES = Path(__file__).resolve().parent / "fixtures"
HOST = FIXTURES / "host" / "bybit-linear"
ARCHIVE = FIXTURES / "archive" / "bybit-linear"
HOUR = "2026-08-30T00"

FAKE_RCLONE = '''#!/usr/bin/env python3
"""A stand-in rclone: lsjson lists FAKE_REMOTE_DIR recursively, copyto copies out of it.

FAKE_RCLONE_SLEEP holds every call that many seconds; FAKE_RCLONE_FAIL makes
every call exit 3; FAKE_RCLONE_TRUNCATE makes copyto deliver half the bytes and
still exit 0, the way an interrupted transfer that rclone did not notice would.
"""
import json, os, shutil, sys, time
args = sys.argv[1:]
root = os.environ["FAKE_REMOTE_DIR"]
with open(os.environ["FAKE_RCLONE_LOG"], "a") as log:
    log.write(" ".join(args) + "\\n")
if os.environ.get("FAKE_RCLONE_SLEEP"):
    time.sleep(float(os.environ["FAKE_RCLONE_SLEEP"]))
if os.environ.get("FAKE_RCLONE_FAIL"):
    print("fake rclone: " + os.environ["FAKE_RCLONE_FAIL"], file=sys.stderr)
    sys.exit(3)
def local(remote):
    return os.path.join(root, remote.split(":", 1)[1])
if args[0] == "lsjson":
    base = local(args[1])
    rows = []
    for directory, _, names in os.walk(base):
        for name in sorted(names):
            path = os.path.join(directory, name)
            rows.append({"Path": os.path.relpath(path, base), "Size": os.path.getsize(path)})
    print(json.dumps(sorted(rows, key=lambda row: row["Path"])))
elif args[0] == "copyto":
    os.makedirs(os.path.dirname(args[2]), exist_ok=True)
    if os.environ.get("FAKE_RCLONE_TRUNCATE"):
        data = open(local(args[1]), "rb").read()
        open(args[2], "wb").write(data[: len(data) // 2])
    else:
        shutil.copyfile(local(args[1]), args[2])
else:
    sys.exit(f"fake rclone got {args[0]}")
'''


def _write_segment(root: Path, relative: str, rows: list[dict]) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = path.with_suffix("")
    raw.write_bytes(b"".join(json.dumps(row, sort_keys=True).encode() + b"\n" for row in rows))
    zstd_compress(raw, path)
    raw.unlink()


def _fake_rclone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put the stand-in rclone on PATH; returns the log of calls it writes."""
    binary = tmp_path / "bin" / "rclone"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text(FAKE_RCLONE, encoding="utf-8")
    binary.chmod(0o755)
    log = tmp_path / "rclone.log"
    monkeypatch.setenv("PATH", f"{binary.parent}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_REMOTE_DIR", str(tmp_path / "remote"))
    monkeypatch.setenv("FAKE_RCLONE_LOG", str(log))
    monkeypatch.delenv("RCLONE_BIN", raising=False)
    return log


def _calls(log: Path, verb: str) -> int:
    if not log.is_file():
        return 0
    return sum(1 for line in log.read_text(encoding="utf-8").splitlines() if line.startswith(verb))


def _hour_archive(tar_path: Path, build_dir: Path, rows: list[dict]) -> None:
    """One hour archive holding a single BTCUSDT segment."""
    name = "BTCUSDT/segment-000000.jsonl.zst"
    _write_segment(build_dir, name, rows)
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w") as handle:
        handle.add(build_dir / name, arcname=name)


def _bybit_book(symbol: str, received_ns: int, update_id: int) -> dict:
    return book_row(
        venue="bybit",
        symbol=symbol,
        snapshot=True,
        depth=1,
        local_receive_ts_ns=received_ns,
        exchange_system_ts_ns=received_ns - 1000,
        exchange_engine_ts_ns=received_ns - 2000,
        bids=[["100.5", "2"]],
        asks=[["100.6", "3"]],
        update_id=update_id,
        previous_update_id=0,
    )


def _bybit_trade(symbol: str, received_ns: int, price: float) -> dict:
    return trade_row(
        venue="bybit",
        symbol=symbol,
        local_receive_ts_ns=received_ns,
        exchange_ts_ns=received_ns - 1000,
        trade_id=f"t{received_ns}",
        price=price,
        qty=1.0,
        side="Buy",
    )


# ------------------------------------------------------------------- hours


def test_hour_range_is_start_inclusive_end_exclusive() -> None:
    assert hour_range("2026-08-30T00", "2026-08-30T00") == ["2026-08-30T00"]
    assert hour_range("2026-08-30T22", "2026-08-31T01") == [
        "2026-08-30T22",
        "2026-08-30T23",
        "2026-08-31T00",
    ]
    with pytest.raises(ValueError):
        hour_range("2026-08-30T02", "2026-08-30T01")
    with pytest.raises(ValueError):
        hour_range("2026-08-30", "2026-08-31")


# --------------------------------------------------------------- host root


def test_rows_are_merged_in_receive_order_across_symbols() -> None:
    source = HostRoot(HOST)
    received = [row.local_receive_ts_ns for row in iter_rows(source, [HOUR])]
    assert received == sorted(received)
    assert len(received) == 1900
    assert source.skipped_rows == 0


def test_meta_snapshots_come_back_as_payloads() -> None:
    payloads = {payload["kind"]: payload for payload in iter_snapshots(HostRoot(HOST), [HOUR])}
    assert set(payloads) == {"instruments_snapshot", "tickers_snapshot"}
    assert payloads["instruments_snapshot"]["venue"] == "bybit"
    assert payloads["instruments_snapshot"]["market"] == "linear"
    symbols = {row["symbol"] for row in payloads["tickers_snapshot"]["rows"]}
    assert symbols == {"BTCUSDT", "PENDLEUSDT", "ETHUSDT", "AGIUSDT"}


def test_the_recorder_status_names_the_venue_and_a_broken_one_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "tape"
    root.mkdir()
    (root / "manifest.jsonl").touch()
    # No status, no venue in the name, nothing from the caller: refused, not guessed.
    with pytest.raises(SourceError, match="names no venue"):
        HostRoot(root)
    assert HostRoot(root, venue="binance").venue == "binance"
    (root / "status.json").write_text(json.dumps({"venue": "binance", "market": "usdm"}), encoding="utf-8")
    assert HostRoot(root).venue == "binance"
    # The caller's word wins over the file.
    assert HostRoot(root, venue="bybit").venue == "bybit"
    # A status file that exists but cannot be read is a broken root, whatever the name says.
    (root / "status.json").write_text("half a line", encoding="utf-8")
    with pytest.raises(SourceError, match="not readable JSON"):
        HostRoot(root)
    (root / "status.json").write_text(json.dumps({"venue": 7}), encoding="utf-8")
    with pytest.raises(SourceError, match="not a venue"):
        HostRoot(root)
    named = tmp_path / "binance-usdm"
    named.mkdir()
    (named / "manifest.jsonl").touch()
    assert HostRoot(named).venue == "binance"


def test_a_directory_that_merely_holds_dated_folders_is_not_a_tape(tmp_path: Path) -> None:
    stranger = tmp_path / "reports"
    (stranger / "2026-09-01" / "notes").mkdir(parents=True)
    (stranger / "2026-09-01" / "notes" / "summary.txt").touch()
    assert not HostRoot.looks_like(stranger)
    with pytest.raises(ValueError, match="neither a recorder root"):
        open_source(str(stranger))
    tape = tmp_path / "bybit-linear"
    _write_segment(tape, "2026-09-01/07/BTCUSDT/segment-000000.jsonl.zst", [_bybit_book("BTCUSDT", 1_000, 5)])
    assert HostRoot.looks_like(tape)
    assert isinstance(open_source(str(tape)), HostRoot)
    assert open_source(str(tape), venue="binance").venue == "binance"


def test_a_finished_hour_is_listed_once_and_the_current_hour_every_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "bybit-linear"
    _write_segment(root, "2026-09-01/07/AAAUSDT/segment-000000.jsonl.zst", [_bybit_trade("AAAUSDT", 100, 1.0)])
    source = HostRoot(root)
    assert len(source.hour_members("2026-09-01T07")) == 1
    _write_segment(root, "2026-09-01/07/AAAUSDT/segment-000001.jsonl.zst", [_bybit_trade("AAAUSDT", 200, 2.0)])
    assert len(source.hour_members("2026-09-01T07")) == 1, "a finished hour is remembered"
    source.refresh()
    assert len(source.hour_members("2026-09-01T07")) == 2

    monkeypatch.setattr(load, "_current_hour", lambda: "2026-09-01T07")
    fresh = HostRoot(root)
    assert len(fresh.hour_members("2026-09-01T07")) == 2
    _write_segment(root, "2026-09-01/07/AAAUSDT/segment-000002.jsonl.zst", [_bybit_trade("AAAUSDT", 300, 3.0)])
    assert len(fresh.hour_members("2026-09-01T07")) == 3, "the hour being written is listed afresh"


def test_a_strict_read_refuses_the_first_bad_line_and_a_lax_one_says_what_it_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "bybit-linear"
    path = root / "2026-09-01" / "07" / "BTCUSDT" / "segment-000000.jsonl.zst"
    path.parent.mkdir(parents=True)
    raw = path.with_suffix("")
    good = _bybit_trade("BTCUSDT", 2_000, 100.0)
    raw.write_bytes(b"\n".join([json.dumps(good).encode(), b"{not json", b"[1, 2]"]) + b"\n")
    zstd_compress(raw, path)
    raw.unlink()
    meta = root / "2026-09-01" / "07" / "_meta" / "instruments-20260901T070000Z.json.zst"
    meta.parent.mkdir(parents=True)
    meta_raw = meta.with_suffix("")
    meta_raw.write_bytes(json.dumps({"kind": "not_a_snapshot"}).encode() + b"\n")
    zstd_compress(meta_raw, meta)
    meta_raw.unlink()

    with pytest.raises(TapeRowError, match=r"BTCUSDT/segment-000000.jsonl.zst line 2: JSONDecodeError"):
        list(iter_rows(HostRoot(root), ["2026-09-01T07"], strict=True))
    with pytest.raises(TapeRowError, match=r"_meta/instruments-.* line 1: SchemaError"):
        list(iter_snapshots(HostRoot(root), ["2026-09-01T07"], strict=True))

    source = HostRoot(root)
    with caplog.at_level("WARNING", logger="market_tape.load"):
        assert len(list(iter_rows(source, ["2026-09-01T07"]))) == 1
        assert list(iter_snapshots(source, ["2026-09-01T07"])) == []
    assert source.skipped_rows == 3
    said = [record.getMessage() for record in caplog.records]
    assert any("skipped 2 unreadable line(s); first at line 2: JSONDecodeError" in line for line in said), said
    assert any("_meta/instruments" in line and "skipped 1" in line for line in said), said


def test_a_source_read_that_fails_mid_stream_is_the_sources_error_not_a_clean_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cut:
        """A member stream that dies after the first bytes, as a bad disk or a torn tar would."""

        def __init__(self, real: object) -> None:
            self.real = real
            self.reads = 0

        def read(self, size: int = -1) -> bytes:
            self.reads += 1
            if self.reads > 1:
                raise OSError(5, "Input/output error")
            return self.real.read(4096)  # type: ignore[attr-defined]

        def close(self) -> None:
            self.real.close()  # type: ignore[attr-defined]

    real_extract = tarfile.TarFile.extractfile
    monkeypatch.setattr(tarfile.TarFile, "extractfile", lambda self, member: Cut(real_extract(self, member)))
    with pytest.raises(SourceError, match=r"reading .*2026-08-30T00Z.tar:.* failed: .*Input/output error"):
        list(iter_rows(ArchiveDir(ARCHIVE), [HOUR]))


def test_stopping_early_says_nothing_and_a_corrupt_file_says_what_broke(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    source = ArchiveDir(ARCHIVE)
    rows = iter_rows(source, [HOUR])
    assert next(rows)
    rows.close()
    assert capfd.readouterr().err == ""

    root = tmp_path / "tape"
    path = root / "2026-09-01" / "07" / "BTCUSDT" / "segment-000000.jsonl.zst"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"this is not a zstd frame")
    with pytest.raises(SourceError, match="zstd exit"):
        list(iter_rows(HostRoot(root, venue="bybit"), ["2026-09-01T07"]))


# -------------------------------------------------------------- archive dir


def test_archive_dir_yields_the_same_rows_as_the_host_root() -> None:
    host = list(iter_rows(HostRoot(HOST), [HOUR], typed=False))
    archive_source = ArchiveDir(ARCHIVE)
    assert archive_source.venue == "bybit"
    assert archive_source.hours() == [HOUR]
    assert list(iter_rows(archive_source, [HOUR], typed=False)) == host


def test_archive_dir_filters_and_reads_meta_out_of_the_tar() -> None:
    source = ArchiveDir(ARCHIVE)
    trades = list(iter_rows(source, [HOUR], symbols=["BTCUSDT"], kinds=["public_trade"]))
    assert len(trades) == 28
    kinds = {payload["kind"] for payload in iter_snapshots(source, [HOUR])}
    assert kinds == {"instruments_snapshot", "tickers_snapshot"}


def test_archive_dir_takes_the_venue_from_the_folder_name_or_the_caller(tmp_path: Path) -> None:
    for name, venue in (("binance-usdm", "binance"), ("bybit-linear", "bybit")):
        (tmp_path / name / "2026" / "08" / "30").mkdir(parents=True)
        assert ArchiveDir(tmp_path / name).venue == venue
    (tmp_path / "tapes" / "2026" / "08" / "30").mkdir(parents=True)
    with pytest.raises(SourceError, match="names no venue"):
        ArchiveDir(tmp_path / "tapes")
    assert ArchiveDir(tmp_path / "tapes", "binance").venue == "binance"


# ------------------------------------------------------------- open_source


def test_open_source_detects_what_it_was_handed(tmp_path: Path) -> None:
    assert isinstance(open_source(str(HOST)), HostRoot)
    assert isinstance(open_source(str(ARCHIVE)), ArchiveDir)
    remote = open_source("rclone:storagebox:tapes/bybit-linear", cache_dir=tmp_path / "cache")
    assert isinstance(remote, RcloneRemote)
    assert remote.venue == "bybit"
    with pytest.raises(SourceError, match="names no venue"):
        open_source("rclone:storagebox:tapes/everything", cache_dir=tmp_path / "cache")
    assert open_source("rclone:storagebox:tapes/everything", cache_dir=tmp_path / "cache", venue="binance").venue == "binance"
    (tmp_path / "empty").mkdir()
    with pytest.raises(ValueError):
        open_source(str(tmp_path / "empty"))
    with pytest.raises(ValueError):
        open_source(str(tmp_path / "missing"))


# ------------------------------------------------------------ rclone remote


def test_rclone_remote_caches_the_hour_and_reads_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = _fake_rclone(tmp_path, monkeypatch)
    remote_dir = tmp_path / "remote" / "tapes" / "bybit-linear" / "2026" / "08" / "30"
    remote_dir.mkdir(parents=True)
    archive = ARCHIVE / "2026" / "08" / "30" / "2026-08-30T00Z.tar"
    (remote_dir / archive.name).write_bytes(archive.read_bytes())

    cache = tmp_path / "cache"
    source = RcloneRemote("storagebox:tapes/bybit-linear", cache)
    assert source.venue == "bybit"
    assert source.hours() == [HOUR]
    assert len(list(iter_rows(source, [HOUR], kinds=["public_trade"]))) == 35
    # The remote spec owns a directory under the cache, and the hour keeps its own path inside it.
    assert source.root.parent == cache
    assert (source.root / "2026" / "08" / "30" / archive.name).is_file()

    # A second read serves the cached tar and never downloads it again.
    assert len(list(iter_rows(source, [HOUR], kinds=["public_trade"]))) == 35
    assert _calls(log, "copyto") == 1


def test_two_remotes_that_hold_the_same_hour_do_not_share_a_cached_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_rclone(tmp_path, monkeypatch)
    hour_path = Path("2026") / "08" / "30" / "2026-08-30T00Z.tar"
    remote = tmp_path / "remote" / "tapes"
    _hour_archive(remote / "bybit-linear" / hour_path, tmp_path / "build-bybit",
                  [_bybit_trade("BTCUSDT", 1_000, 100.0)])
    _hour_archive(remote / "binance-usdm" / hour_path, tmp_path / "build-binance",
                  [_bybit_trade("BTCUSDT", 2_000, 200.0)])

    cache = tmp_path / "cache"
    bybit = RcloneRemote("storagebox:tapes/bybit-linear", cache)
    binance = RcloneRemote("storagebox:tapes/binance-usdm", cache)
    assert [row.price for row in iter_rows(bybit, [HOUR])] == [100.0]
    assert [row.price for row in iter_rows(binance, [HOUR])] == [200.0]
    assert _calls(log, "copyto") == 2
    assert bybit.root != binance.root


def test_the_remote_listing_is_read_once_until_refresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = _fake_rclone(tmp_path, monkeypatch)
    _hour_archive(
        tmp_path / "remote" / "tapes" / "bybit-linear" / "2026" / "08" / "30" / "2026-08-30T00Z.tar",
        tmp_path / "build", [_bybit_trade("BTCUSDT", 1_000, 100.0)],
    )
    source = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache")
    for _ in range(25):
        assert len(source.hour_members(HOUR)) == 1
    assert _calls(log, "lsjson") == 1
    source.refresh()
    assert source.hours() == [HOUR]
    assert _calls(log, "lsjson") == 2


def test_a_symbols_segments_are_read_one_after_another(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "tape"
    _write_segment(root, "2026-09-01/07/AAAUSDT/segment-000000.jsonl.zst",
                   [_bybit_trade("AAAUSDT", 100, 1.0), _bybit_trade("AAAUSDT", 300, 3.0)])
    _write_segment(root, "2026-09-01/07/AAAUSDT/segment-000001.jsonl.zst",
                   [_bybit_trade("AAAUSDT", 400, 4.0)])
    _write_segment(root, "2026-09-01/07/BBBUSDT/segment-000000.jsonl.zst",
                   [_bybit_trade("BBBUSDT", 100, 10.0), _bybit_trade("BBBUSDT", 500, 50.0)])

    live = {"open": 0, "peak": 0, "total": 0}
    real = load._zstd_lines

    def counted(argv, source=None, owns=None, label=""):  # type: ignore[no-untyped-def]
        live["total"] += 1
        live["open"] += 1
        live["peak"] = max(live["peak"], live["open"])
        try:
            yield from real(argv, source, owns, label)
        finally:
            live["open"] -= 1

    monkeypatch.setattr(load, "_zstd_lines", counted)
    rows = list(iter_rows(HostRoot(root, venue="bybit"), ["2026-09-01T07"]))
    assert [(row.symbol, row.local_receive_ts_ns) for row in rows] == [
        ("AAAUSDT", 100), ("BBBUSDT", 100), ("AAAUSDT", 300), ("AAAUSDT", 400), ("BBBUSDT", 500)
    ]
    assert live["total"] == 3  # every segment is read
    assert live["peak"] == 2  # one open file per symbol, not per segment
    assert live["open"] == 0


def test_an_interrupted_copy_never_becomes_the_cached_hour(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = _fake_rclone(tmp_path, monkeypatch)
    hour_path = Path("2026") / "08" / "30" / "2026-08-30T00Z.tar"
    _hour_archive(tmp_path / "remote" / "tapes" / "bybit-linear" / hour_path, tmp_path / "build",
                  [_bybit_trade("BTCUSDT", 1_000, 100.0)])
    source = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache")

    monkeypatch.setenv("FAKE_RCLONE_TRUNCATE", "1")
    with pytest.raises(CacheError, match="arrived as .* bytes, the remote lists"):
        source.hour_members(HOUR)
    cached = source.root / hour_path
    assert not cached.exists()
    assert not cached.with_name(cached.name + ".partial").exists(), "no half a tar is left behind"

    # Told the whole size but handed a file that is not a tar: refused the same way.
    monkeypatch.delenv("FAKE_RCLONE_TRUNCATE")
    (tmp_path / "remote" / "tapes" / "bybit-linear" / hour_path).write_bytes(b"x" * 10_240)
    source.refresh()
    with pytest.raises(CacheError, match="is not a tar archive"):
        source.hour_members(HOUR)
    assert not cached.exists()

    # The real archive arrives whole and is read; two failed attempts cost two copies, no more.
    _hour_archive(tmp_path / "remote" / "tapes" / "bybit-linear" / hour_path, tmp_path / "build-2",
                  [_bybit_trade("BTCUSDT", 1_000, 100.0)])
    source.refresh()
    assert [row.price for row in iter_rows(source, [HOUR])] == [100.0]
    assert cached.is_file()
    assert _calls(log, "copyto") == 3


def test_a_cached_archive_that_no_longer_matches_the_listing_is_fetched_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_rclone(tmp_path, monkeypatch)
    hour_path = Path("2026") / "08" / "30" / "2026-08-30T00Z.tar"
    remote = tmp_path / "remote" / "tapes" / "bybit-linear" / hour_path
    _hour_archive(remote, tmp_path / "build", [_bybit_trade("BTCUSDT", 1_000, 100.0)])
    source = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache")
    assert [row.price for row in iter_rows(source, [HOUR])] == [100.0]

    # The hour was re-packed on the storage box with far more trades: a tar of a different size.
    many = [_bybit_trade("BTCUSDT", 1_000 + i, 100.0 + i) for i in range(2_000)]
    _hour_archive(remote, tmp_path / "build-2", many)
    source.refresh()
    assert len(list(iter_rows(source, [HOUR]))) == 2_000
    assert _calls(log, "copyto") == 2


def test_a_slash_less_remote_names_its_venue_by_the_path_after_the_colon(tmp_path: Path) -> None:
    assert RcloneRemote("box:bybit-linear", tmp_path / "cache").venue == "bybit"
    assert open_source("rclone:box:binance-usdm", cache_dir=tmp_path / "cache").venue == "binance"
    with pytest.raises(SourceError, match="names no venue"):
        RcloneRemote("box:", tmp_path / "cache")


_FETCH_ONE_HOUR = """
import sys
from pathlib import Path
from market_tape.load import RcloneRemote
source = RcloneRemote("storagebox:tapes/bybit-linear", Path(sys.argv[1]))
print(len(source.hour_members(sys.argv[2])))
"""


def test_processes_sharing_a_cache_fetch_one_uncached_hour_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two workers opening one uncached hour at once: without the lock each
    copies it, and the second's fresh `.partial` is unlinked under the first's
    copy, so one fails or both pay the transfer. With it the second waits,
    finds the archive whole and reads it."""

    log = _fake_rclone(tmp_path, monkeypatch)
    _hour_archive(
        tmp_path / "remote" / "tapes" / "bybit-linear" / "2026" / "08" / "30" / "2026-08-30T00Z.tar",
        tmp_path / "build", [_bybit_trade("BTCUSDT", 1_000, 100.0)],
    )
    monkeypatch.setenv("FAKE_RCLONE_SLEEP", "1")
    root = Path(__file__).resolve().parents[2]
    workers = [
        subprocess.Popen(
            [sys.executable, "-c", _FETCH_ONE_HOUR, str(tmp_path / "cache"), HOUR],
            cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for _ in range(2)
    ]
    results = [worker.communicate(timeout=60) for worker in workers]
    assert [worker.returncode for worker in workers] == [0, 0], [err for _, err in results]
    assert [out.strip() for out, _ in results] == ["1", "1"]
    assert _calls(log, "copyto") == 1
    assert _calls(log, "lsjson") == 2


def test_rclone_that_hangs_or_fails_is_a_typed_error_naming_the_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_rclone(tmp_path, monkeypatch)
    _hour_archive(
        tmp_path / "remote" / "tapes" / "bybit-linear" / "2026" / "08" / "30" / "2026-08-30T00Z.tar",
        tmp_path / "build", [_bybit_trade("BTCUSDT", 1_000, 100.0)],
    )
    # Two handles on one cache directory, so they are one remote as far as the
    # files on disk are concerned. Only the call that is SUPPOSED to time out
    # gets a budget a call cannot meet: 0.2 s is not enough to spawn a shell
    # script on a loaded CI runner, so a success or a refusal measured against
    # it reports a timeout that says nothing about the code under test.
    hangs = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache", list_timeout=0.2, copy_timeout=0.2)
    source = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache", copy_timeout=0.2)

    monkeypatch.setenv("FAKE_RCLONE_SLEEP", "5")
    with pytest.raises(RemoteTimeout, match=r"rclone lsjson on storagebox:tapes/bybit-linear: no answer within 0.2s"):
        hangs.hours()
    monkeypatch.delenv("FAKE_RCLONE_SLEEP")

    # A refusal, and it has to surface as the venue's exit code rather than as
    # however long the machine took to deliver it.
    monkeypatch.setenv("FAKE_RCLONE_FAIL", "couldn't find remote")
    with pytest.raises(RemoteCommandError, match=r"rclone lsjson on .*: exit 3: fake rclone: couldn't find remote") as failed:
        source.hours()
    assert failed.value.returncode == 3
    monkeypatch.delenv("FAKE_RCLONE_FAIL")

    # Listed once and remembered, which is what leaves the copy below as the
    # only call the next timeout can be measuring.
    assert source.hours() == [HOUR]
    monkeypatch.setenv("FAKE_RCLONE_SLEEP", "5")
    with pytest.raises(RemoteTimeout, match="rclone copyto"):
        source.hour_members(HOUR)
    assert not (source.root / "2026" / "08" / "30" / "2026-08-30T00Z.tar.partial").exists()

    monkeypatch.setenv("RCLONE_BIN", str(tmp_path / "no-such-rclone"))
    missing = RcloneRemote("storagebox:tapes/bybit-linear", tmp_path / "cache")
    with pytest.raises(RemoteCommandError, match="exit 127"):
        missing.hours()


def test_cache_directories_are_told_apart_by_a_long_digest(tmp_path: Path) -> None:
    prefix = "storagebox:" + "a" * 70
    first = RcloneRemote(prefix + "/bybit-linear", tmp_path / "cache")
    second = RcloneRemote(prefix + "/bybit-linear-2", tmp_path / "cache")
    assert first.root != second.root
    assert len(first.root.name.rsplit("-", 1)[1]) == 32
