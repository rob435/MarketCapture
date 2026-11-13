"""Audit a tape against its own receipts: every file of the named hours, wherever it is stored.

```text
python -m market_tape verify SOURCE --hours FROM[..TO] [--rows] [--symbols ...] [--json]
```

The recorder receipts every file it finishes with the SHA-256 of its bytes
(`manifest.jsonl` on a recorder root, `MANIFEST.json` inside an hour's tar),
and a segment's receipt also states its row count and the first and last
receive stamps. This reads the files back and says whether they are still
those files: on the host that wrote them, in an archive directory, or on the
storage box through `rclone:` (`market_tape.load.open_source`).

| Finding | Means |
| --- | --- |
| `digest` | the bytes are not the receipt's: the file changed after it was receipted |
| `missing` | a receipt names a file the hour does not hold (on a recorder root, an hour inside `pack`'s sliding window has been deleted whole and has no receipts left, so this is a file lost alone) |
| `unreceipted` | a file no receipt names: on a recorder root, the open hour's or a crashed run's; in an archive, a file the pack did not account for |
| `rows` (`--rows`) | a segment decompresses to a different row count, or a first or last receive stamp, than its receipt states, or a line of it is not a tape row |
| `order` (`--rows`) | a row's receive stamp is behind the row before it in its segment, which the writer never writes |
| `no_coverage` | an hour without a coverage record: the recorder's account of the hour is absent, so `coverage` reads it from the rows alone |
| `unreadable` | the file could not be read or decompressed at all |

Without `--rows` only the stored bytes are hashed, at disk speed; `--rows`
also decompresses every JSON segment and walks its lines (a block file states
its own CRCs: `index --verify`). An hour the source does not hold is reported
as `absent` and is not a finding: `coverage` is what says which hours should
exist. `unreceipted` and `no_coverage` are warnings: printed, and a failure
only with `--strict`. The exit status is 0 when nothing failed, 1 otherwise, 2
for a source or argument that cannot be read.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from market_tape.load import BLOCK_SUFFIX, COVERAGE_PREFIX, META, Member, Source, SourceError

__all__ = ["FINDINGS", "HourAudit", "audit_hour", "audit_hours", "main"]

FINDINGS = ("digest", "missing", "unreceipted", "rows", "order", "no_coverage", "unreadable")
#: Findings a healthy tape can show (the open hour's files, an hour written
#: before coverage records): printed, and a failure only under `strict`.
WARNINGS = frozenset({"unreceipted", "no_coverage"})
#: Receipts that describe a file the hour holds; a `*_deleted` row describes one it no longer does.
HELD_RECEIPTS = ("segment_compressed", "snapshot_compressed", "coverage_compressed", "segment_blocks")
READ_BYTES = 1 << 20


@dataclass(slots=True)
class HourAudit:
    hour: str
    present: bool = True
    files: int = 0
    bytes: int = 0
    rows: int = 0
    #: (finding, member path, detail), in member order.
    findings: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """No finding at all."""

        return not self.findings

    @property
    def sound(self) -> bool:
        """No finding but `WARNINGS`."""

        return all(name in WARNINGS for name, _, _ in self.findings)

    def payload(self) -> dict[str, Any]:
        return {
            "hour": self.hour,
            "present": self.present,
            "files": self.files,
            "bytes": self.bytes,
            "rows": self.rows,
            "findings": [{"finding": name, "path": path, "detail": detail} for name, path, detail in self.findings],
        }


def _digest(member: Member) -> tuple[str, int]:
    hasher = hashlib.sha256()
    size = 0
    handle = member.binary()
    try:
        for chunk in iter(lambda: handle.read(READ_BYTES), b""):
            hasher.update(chunk)
            size += len(chunk)
    finally:
        handle.close()
    return hasher.hexdigest(), size


def _walk_rows(member: Member, receipt: Mapping[str, Any]) -> tuple[int, list[tuple[str, str]]]:
    """A JSON segment's rows against its receipt: the count, and what is wrong with them."""

    found: list[tuple[str, str]] = []
    count = 0
    first = last = 0
    disorder = 0
    bad_lines = 0
    stream = member.open()
    try:
        for line_number, raw in enumerate(stream, start=1):
            if not raw.strip():
                continue
            try:
                received = int(json.loads(raw)["local_receive_ts_ns"])
            except (KeyError, TypeError, ValueError):
                bad_lines += 1
                if bad_lines == 1:
                    found.append(("rows", f"line {line_number} is not a tape row"))
                continue
            if count == 0:
                first = received
            elif received < last:
                disorder += 1
                if disorder == 1:
                    found.append(("order", f"line {line_number} is stamped {last - received} ns behind the line before it"))
            last = max(last, received)
            count += 1
    finally:
        stream.close()
    stated = receipt.get("records")
    if stated is not None and int(stated) != count + bad_lines:
        found.append(("rows", f"{count + bad_lines} rows, the receipt states {int(stated)}"))
    for name, value in (("first_receive_ns", first), ("last_receive_ns", last)):
        stated = receipt.get(name)
        if stated is not None and count and not disorder and int(stated) != value:
            found.append(("rows", f"{name} is {value}, the receipt states {int(stated)}"))
    return count, found


def audit_hour(source: Source, hour: str, *, rows: bool = False, symbols: Iterable[str] | None = None) -> HourAudit:
    """One hour's files against their receipts. `symbols` narrows the audit to
    those symbols' segments; the hour's `_meta` files are audited either way."""

    wanted = {symbol.upper() for symbol in symbols} if symbols else None
    audit = HourAudit(hour)
    members = [
        member
        for member in source.hour_members(hour)
        if wanted is None or member.symbol == META or member.symbol in wanted
    ]
    receipts = {
        path: receipt
        for path, receipt in source.hour_manifest(hour).items()
        if receipt.get("kind") in HELD_RECEIPTS or "kind" not in receipt
    }
    if not members and not receipts:
        audit.present = False
        return audit
    seen: set[str] = set()
    coverage = False
    for member in members:
        seen.add(member.path)
        audit.files += 1
        if member.symbol == META and member.path.rsplit("/", 1)[-1].startswith(COVERAGE_PREFIX):
            coverage = True
        try:
            digest, size = _digest(member)
        except (OSError, SourceError) as exc:
            audit.findings.append(("unreadable", member.path, str(exc)[:200]))
            continue
        audit.bytes += size
        receipt = receipts.get(member.path)
        if receipt is None:
            audit.findings.append(("unreceipted", member.path, f"{size} bytes"))
        elif receipt.get("sha256") and receipt["sha256"] != digest:
            audit.findings.append(("digest", member.path, f"sha256 {digest}, the receipt states {receipt['sha256']}"))
        if not rows or member.symbol == META or member.path.endswith(BLOCK_SUFFIX):
            continue
        try:
            count, found = _walk_rows(member, receipt or {})
        except (OSError, SourceError) as exc:
            audit.findings.append(("unreadable", member.path, str(exc)[:200]))
            continue
        audit.rows += count
        audit.findings.extend((name, member.path, detail) for name, detail in found)
    for path in sorted(set(receipts) - seen):
        symbol = path.split("/", 1)[0]
        if wanted is None or symbol == META or symbol in wanted:
            audit.findings.append(("missing", path, "receipted, and the hour does not hold it"))
    if not coverage:
        audit.findings.append(("no_coverage", f"{META}/", "the hour holds no coverage record"))
    return audit


def audit_hours(
    source: Source, hours: Iterable[str], *, rows: bool = False, symbols: Iterable[str] | None = None
) -> list[HourAudit]:
    wanted = list(symbols) if symbols else None
    return [audit_hour(source, hour, rows=rows, symbols=wanted) for hour in hours]


def summarize(audits: Sequence[HourAudit]) -> dict[str, Any]:
    by_finding: dict[str, int] = {}
    for audit in audits:
        for name, _, _ in audit.findings:
            by_finding[name] = by_finding.get(name, 0) + 1
    return {
        "hours": len(audits),
        "absent": [audit.hour for audit in audits if not audit.present],
        "clean": sum(1 for audit in audits if audit.present and audit.ok),
        "files": sum(audit.files for audit in audits),
        "bytes": sum(audit.bytes for audit in audits),
        "rows": sum(audit.rows for audit in audits),
        "findings": dict(sorted(by_finding.items())),
    }


def main(argv: Sequence[str] | None = None) -> int:
    import argparse
    from pathlib import Path

    from market_tape.load import hour_range, open_source

    parser = argparse.ArgumentParser(prog="market_tape verify", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source")
    parser.add_argument("--cache", type=Path, default=None, help="local cache for archives read from rclone")
    parser.add_argument("--venue", default=None, help="the source's venue, when neither status.json nor the source name says")
    parser.add_argument("--hours", required=True, help="FROM[..TO], hours as YYYY-MM-DDTHH, TO exclusive")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--rows", action="store_true", help="also decompress each JSON segment and check its rows against its receipt")
    parser.add_argument("--strict", action="store_true", help="fail on the warnings too: unreceipted, no_coverage")
    parser.add_argument("--json", action="store_true", help="print the summary and every hour's findings as JSON")
    args = parser.parse_args(argv)
    start, separator, end = args.hours.partition("..")
    try:
        source = open_source(args.source, cache_dir=args.cache, venue=args.venue)
        audits = audit_hours(source, hour_range(start, end if separator else start), rows=args.rows, symbols=args.symbols)
    except (SourceError, ValueError) as exc:
        parser.exit(2, f"market_tape verify: {exc}\n")
    summary = summarize(audits)
    if args.json:
        print(json.dumps({**summary, "audits": [audit.payload() for audit in audits if not audit.ok]}, indent=2, sort_keys=True))
    else:
        for audit in audits:
            for name, path, detail in audit.findings:
                print(f"{audit.hour} {name:<12} {path} {detail}")
        counted = "  ".join(f"{name}={count}" for name, count in summary["findings"].items()) or "none"
        print(
            f"hours {summary['hours']}  clean {summary['clean']}  absent {len(summary['absent'])}  "
            f"files {summary['files']}  bytes {summary['bytes']}  rows {summary['rows']}  findings: {counted}"
        )
    return 0 if all(audit.ok if args.strict else audit.sound for audit in audits) else 1
