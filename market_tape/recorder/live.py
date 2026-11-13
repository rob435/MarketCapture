"""What the ticker stream, and the last table snapshot, say about every symbol.

The live universe kinds (ranked turnover and movers, funding either side of a
line, turnover and volume surges, price bursts, open-interest jumps) follow
the ticker stream the recorder is already writing, so a name whose funding
rate collapses or whose turnover explodes gets its deep feeds within one
maintenance tick, not at the next daily snapshot. The writer thread observes
every ticker row; the maintainer reads a copy (`LiveState.view`). The
windowed kinds ask what a name looked like a window ago, so with a history
the state keeps one sample a minute per symbol that far back.
"""

from __future__ import annotations

import dataclasses
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(slots=True)
class SymbolLive:
    funding_rate: float | None = None
    turnover_24h: float | None = None
    price_change_24h: float | None = None
    price: float | None = None
    open_interest: float | None = None

    def copy(self) -> "SymbolLive":
        return dataclasses.replace(self)


@dataclass(frozen=True, slots=True)
class Sample:
    """One remembered ticker reading, for the windowed universes."""

    ns: int
    turnover_24h: float | None
    price: float | None
    open_interest: float | None


SAMPLE_SPACING_NS = 60 * 1_000_000_000


class LiveState:
    """What the ticker stream, and the last table snapshot, say about every symbol.
    With `history_ns` > 0 it also remembers one sample a minute per symbol that
    far back, so a universe can ask what a name looked like an hour ago."""

    def __init__(self, history_ns: int = 0) -> None:
        self.lock = threading.Lock()
        self.symbols: dict[str, SymbolLive] = {}
        self.history: dict[str, deque[Sample]] = {}
        self.history_ns = max(0, int(history_ns))
        self.baseline_turnover: dict[str, float] = {}

    def observe(self, symbol: str, values: Mapping[str, Any], received_ns: int) -> None:
        with self.lock:
            live = self.symbols.get(symbol)
            if live is None:
                live = self.symbols[symbol] = SymbolLive()
            if "funding_rate" in values:
                live.funding_rate = float(values["funding_rate"])
            if "turnover_24h" in values:
                live.turnover_24h = float(values["turnover_24h"])
            if "price_change_24h_pct" in values:
                live.price_change_24h = float(values["price_change_24h_pct"])
            if "mark_price" in values:
                live.price = float(values["mark_price"])
            elif "last_price" in values and live.price is None:
                live.price = float(values["last_price"])
            if "open_interest" in values:
                live.open_interest = float(values["open_interest"])
            if self.history_ns:
                self._sample(symbol, live, received_ns)

    def _sample(self, symbol: str, live: SymbolLive, now_ns: int) -> None:
        samples = self.history.setdefault(symbol, deque())
        if samples and now_ns - samples[-1].ns < SAMPLE_SPACING_NS:
            return
        samples.append(Sample(now_ns, live.turnover_24h, live.price, live.open_interest))
        # One sample older than the window stays, so a lookback exactly at the window's edge resolves.
        while len(samples) > 1 and samples[1].ns <= now_ns - self.history_ns:
            samples.popleft()

    def earlier(self, symbol: str, at_ns: int) -> Sample | None:
        """The newest remembered sample taken at or before `at_ns`; None when the
        history does not reach back that far."""

        with self.lock:
            samples = self.history.get(symbol)
            if not samples or samples[0].ns > at_ns:
                return None
            found = None
            for sample in samples:
                if sample.ns > at_ns:
                    break
                found = sample
            return found

    def seed(self, funding: Mapping[str, float], turnovers: Mapping[str, float]) -> None:
        """A table snapshot: current funding and turnover for every listed name,
        and the turnover every later surge is measured against."""

        with self.lock:
            for symbol, rate in funding.items():
                self.symbols.setdefault(symbol, SymbolLive()).funding_rate = rate
            for symbol, turnover in turnovers.items():
                self.symbols.setdefault(symbol, SymbolLive()).turnover_24h = turnover
            self.baseline_turnover = dict(turnovers)

    def view(self) -> tuple[dict[str, SymbolLive], dict[str, float]]:
        with self.lock:
            return ({symbol: live.copy() for symbol, live in self.symbols.items()}, dict(self.baseline_turnover))
