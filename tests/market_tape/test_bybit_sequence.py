"""The Bybit orderbook sequence contract, on the fixture a live feed reads too.

`tests/fixtures/bybit_orderbook_sequence.jsonl` is one raw venue message per
line with the verdict the recorder, the book rebuild and a live feed must all
reach: `apply`, `rebase`, `gap`, `before_snapshot`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from market_tape.book import Book
from market_tape.schema import BookRow, parse_row
from market_tape.venues.bybit import BybitAdapter

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "bybit_orderbook_sequence.jsonl"


def cases() -> list[tuple[dict[str, Any], str]]:
    rows = [json.loads(line) for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [(row["frame"], row["expect"]) for row in rows]


def recorder_verdict(row: dict[str, Any], based: set[str]) -> str:
    """What the recorder's row says happened to the topic."""

    topic = f"orderbook.{row['depth']}.{row['symbol']}"
    if row["kind"] == "orderbook_snapshot":
        based.add(topic)
        return "rebase"
    if row["sequence_gap"]:
        return "gap" if topic in based else "before_snapshot"
    return "apply"


def test_the_recorder_and_the_rebuild_read_the_fixture_the_same_way() -> None:
    adapter = BybitAdapter()
    books: dict[str, Book] = {}
    based: set[str] = set()

    for line, (frame, expect) in enumerate(cases(), start=1):
        rows = adapter.normalize(json.dumps(frame), 1_800_000_000_000_000_000 + line)
        assert len(rows) == 1, f"line {line}"
        row = rows[0]
        assert row["cross_sequence"] == frame["data"]["seq"], f"line {line}: seq is recorded"
        assert recorder_verdict(row, based) == expect, f"line {line}: the recorder"

        # The rebuild reaches the verdict from the ids alone: tape written
        # before this rule carries a `sequence_gap` decided by the old one.
        typed = parse_row({**row, "sequence_gap": False})
        assert isinstance(typed, BookRow)
        book = books.setdefault(frame["topic"], Book())
        good = book.apply(typed)
        assert good == (expect in {"apply", "rebase"}), f"line {line}: the rebuild"
        assert book.valid == good
        if good:
            assert book.last_update_id == frame["data"]["u"]
