"""The audit: the fixture's three sources clean, then each way a file can stop being its receipt's."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from market_tape.load import open_source
from market_tape.verify import audit_hour, audit_hours, main, summarize

FIXTURES = Path(__file__).parent / "fixtures"
HOUR = "2026-08-30T00"
BTC = "BTCUSDT/segment-000000.jsonl.zst"


@pytest.fixture
def host(tmp_path: Path) -> Path:
    root = tmp_path / "bybit-linear"
    shutil.copytree(FIXTURES / "host" / "bybit-linear", root)
    return root


def _findings(root: Path, **options: object) -> list[tuple[str, str]]:
    audit = audit_hour(open_source(str(root)), HOUR, **options)  # type: ignore[arg-type]
    return [(name, path) for name, path, _ in audit.findings]


@pytest.mark.parametrize("kind", ["host", "archive"])
def test_fixture_sources_hold_the_files_their_receipts_state(kind: str) -> None:
    source = open_source(str(FIXTURES / kind / "bybit-linear"))
    (audit,) = audit_hours(source, [HOUR], rows=True)

    # The fixture hour predates coverage records: a warning, and nothing else.
    assert [(name, path) for name, path, _ in audit.findings] == [("no_coverage", "_meta/")]
    assert audit.sound and not audit.ok
    assert (audit.files, audit.rows) == (4, 1900)


def test_an_hour_the_source_does_not_hold_is_absent_not_a_finding(host: Path) -> None:
    audits = audit_hours(open_source(str(host)), [HOUR, "2026-08-30T01"])
    assert [audit.present for audit in audits] == [True, False]
    assert audits[1].ok
    assert summarize(audits)["absent"] == ["2026-08-30T01"]


def test_changed_bytes_are_a_digest_finding(host: Path) -> None:
    segment = host / "2026-08-30" / "00" / BTC
    data = bytearray(segment.read_bytes())
    data[-1] ^= 0xFF
    segment.write_bytes(bytes(data))
    assert ("digest", BTC) in _findings(host)


def test_a_receipted_file_gone_is_missing_and_a_stray_one_unreceipted(host: Path) -> None:
    hour = host / "2026-08-30" / "00"
    (hour / BTC).rename(hour / "BTCUSDT" / "segment-000009.jsonl.zst")
    found = _findings(host)
    assert ("missing", BTC) in found
    assert ("unreceipted", "BTCUSDT/segment-000009.jsonl.zst") in found
    # Narrowed to another symbol, neither is this audit's.
    assert _findings(host, symbols=["PENDLEUSDT"]) == [("no_coverage", "_meta/")]


def _rewrite(host: Path, edit: object) -> None:
    """The BTC segment with its lines edited, receipted as the new bytes: only its rows disagree."""

    import hashlib

    segment = host / "2026-08-30" / "00" / BTC
    lines = subprocess.run(["zstd", "-dcq", str(segment)], check=True, capture_output=True).stdout.splitlines(keepends=True)
    raw = segment.with_suffix("")
    raw.write_bytes(b"".join(edit(lines)))  # type: ignore[operator]
    segment.unlink()
    subprocess.run(["zstd", "-q", "--rm", str(raw)], check=True)
    digest = hashlib.sha256(segment.read_bytes()).hexdigest()
    manifest = host / "manifest.jsonl"
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    for row in rows:
        if row["path"].endswith(BTC):
            row["sha256"] = digest
    manifest.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_rows_are_checked_against_the_receipt_only_when_asked(host: Path) -> None:
    _rewrite(host, lambda lines: lines[:-10])
    assert _findings(host) == [("no_coverage", "_meta/")]
    audit = audit_hour(open_source(str(host)), HOUR, rows=True)
    details = [detail for name, path, detail in audit.findings if name == "rows" and path == BTC]
    assert "1490 rows, the receipt states 1500" in details
    assert any(detail.startswith("last_receive_ns") for detail in details)
    assert not audit.sound


def test_a_row_behind_the_one_before_it_and_a_torn_line(host: Path) -> None:
    _rewrite(host, lambda lines: [lines[1], lines[0], b"{torn\n", *lines[2:]])
    found = _findings(host, rows=True)
    assert ("order", BTC) in found
    assert ("rows", BTC) in found


def test_cli_exit_status_and_json(host: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([str(host), "--hours", HOUR, "--rows"]) == 0
    assert "findings: no_coverage=1" in capsys.readouterr().out
    assert main([str(host), "--hours", HOUR, "--strict"]) == 1
    capsys.readouterr()
    (host / "2026-08-30" / "00" / BTC).write_bytes(b"not zstd")
    assert main([str(host), "--hours", HOUR, "--rows", "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["findings"]["digest"] == 1 and payload["findings"]["unreadable"] == 1
    with pytest.raises(SystemExit) as refused:
        main([str(host / "nowhere"), "--hours", HOUR])
    assert refused.value.code == 2
