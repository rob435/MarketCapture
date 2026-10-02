"""Rebuild one symbol's order book from the tape, and read the shapes off it.

Feed a `Book` the book rows of one symbol at one depth, oldest first. It says
after every row whether the book it now holds is the venue's book or a guess.

A snapshot replaces the book and makes it good. A delta is applied only when it
chains onto what is already there; when it does not, the book is bad until the
next snapshot, because a missed delta leaves levels that no later message will
ever correct.

What chaining means is the venue's own rule:

- Bybit numbers every book message per topic; a delta chains when its
  `update_id` is exactly one above the last one applied, and `seq` orders
  topics against each other rather than deltas within one.
- Binance's snapshot is fetched over REST while the deltas already flow over
  the socket, so on the tape the snapshot row lands after diffs it does not
  cover and may itself be a little stale. Diffs that arrive while the book has
  no snapshot are held; when a snapshot lands, the held diffs older than its
  `lastUpdateId` are dropped, the first one kept must span that id
  (`U <= lastUpdateId <= u`), and every diff after it must name the last one
  applied as its own predecessor (`pu`). A snapshot too stale to meet the held
  diffs leaves the book bad and the diffs held for the next snapshot.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from market_tape.schema import BookRow

#: Diffs held while a Binance book waits for a snapshot it can chain onto.
HELD_DELTAS = 5_000


class Book:
    def __init__(self) -> None:
        self.symbol: str | None = None
        self.venue: str | None = None
        self.depth: int | None = None
        self.last_update_id = 0
        self.snapshot_update_id = 0
        self.rows_applied = 0
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._valid = False
        self._awaiting_first_delta = False
        self._held: deque[BookRow] = deque(maxlen=HELD_DELTAS)

    @property
    def held_deltas(self) -> int:
        return len(self._held)

    # ------------------------------------------------------------ the stream

    def apply(self, row: BookRow) -> bool:
        """Take one row; return whether the book is the venue's book afterwards."""

        if self.symbol is None:
            self.symbol = row.symbol
            self.venue = row.venue
            self.depth = row.depth
        elif row.symbol != self.symbol:
            raise ValueError(f"this book is {self.symbol}, not {row.symbol}")
        elif row.depth != self.depth:
            raise ValueError(f"this book is depth {self.depth}, not depth {row.depth}; filter by depth first")
        if row.snapshot:
            return self._take_snapshot(row)
        return self._take_delta(row)

    def _take_snapshot(self, row: BookRow) -> bool:
        self._bids = {price: size for price, size in row.bids if size > 0}
        self._asks = {price: size for price, size in row.asks if size > 0}
        self.last_update_id = row.update_id
        self.snapshot_update_id = row.update_id
        self._awaiting_first_delta = row.venue == "binance"
        self._valid = True
        self.rows_applied += 1
        if row.venue == "binance":
            self._replay()
        return self._valid

    def _take_delta(self, row: BookRow) -> bool:
        if row.venue == "binance":
            if not self._valid:
                self._held.append(row)
                return False
            applied = self._binance_delta(row)
            if not applied and not self._valid:
                self._held.append(row)
            return applied
        if not self._valid:
            return False
        if row.sequence_gap or row.update_id != self.last_update_id + 1:
            self._valid = False
            return False
        self._change(row)
        return True

    def _replay(self) -> None:
        """The held diffs against the snapshot that just landed."""

        held = list(self._held)
        self._held.clear()
        for index, row in enumerate(held):
            if not self._binance_delta(row) and not self._valid:
                self._held.extend(held[index:])
                return

    def _binance_delta(self, row: BookRow) -> bool:
        if row.update_id < self.snapshot_update_id:
            return True
        if self._awaiting_first_delta:
            # The recorder flags the first diff after a connect as a gap because
            # it saw nothing before it; the snapshot's bracket is the authority here.
            if not row.first_update_id <= self.snapshot_update_id <= row.update_id:
                self._valid = False
                return False
            self._awaiting_first_delta = False
        elif row.sequence_gap or row.previous_update_id != self.last_update_id:
            self._valid = False
            return False
        self._change(row)
        return True

    def _change(self, row: BookRow) -> None:
        for side, changes in ((self._bids, row.bids), (self._asks, row.asks)):
            for price, size in changes:
                if size > 0:
                    side[price] = size
                else:
                    side.pop(price, None)
        self.last_update_id = row.update_id
        self.rows_applied += 1

    # ------------------------------------------------------------- the shape

    @property
    def valid(self) -> bool:
        return self._valid

    @property
    def best_bid(self) -> tuple[float, float] | None:
        if not self._bids:
            return None
        price = max(self._bids)
        return price, self._bids[price]

    @property
    def best_ask(self) -> tuple[float, float] | None:
        if not self._asks:
            return None
        price = min(self._asks)
        return price, self._asks[price]

    @property
    def mid(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid[0] + ask[0]) / 2.0

    @property
    def spread_bp(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        mid = self.mid
        if bid is None or ask is None or not mid:
            return None
        return (ask[0] - bid[0]) / mid * 10_000.0

    def levels(self, n: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self._bids.items(), key=lambda level: -level[0])[:n]
        asks = sorted(self._asks.items())[:n]
        return bids, asks

    def depth_within(self, bp: float) -> tuple[float, float]:
        """Quote-currency size resting within `bp` basis points of the mid, bids then asks."""

        mid = self.mid
        if not mid:
            return 0.0, 0.0
        floor = mid * (1.0 - bp / 10_000.0)
        ceiling = mid * (1.0 + bp / 10_000.0)
        bid = sum(price * size for price, size in self._bids.items() if price >= floor)
        ask = sum(price * size for price, size in self._asks.items() if price <= ceiling)
        return bid, ask

    def describe(self, levels: int = 5) -> dict[str, Any]:
        bids, asks = self.levels(levels)
        best_bid, best_ask = self.best_bid, self.best_ask
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "depth": self.depth,
            "valid": self._valid,
            "rows_applied": self.rows_applied,
            "held_deltas": len(self._held),
            "last_update_id": self.last_update_id,
            "snapshot_update_id": self.snapshot_update_id,
            "best_bid": list(best_bid) if best_bid is not None else None,
            "best_ask": list(best_ask) if best_ask is not None else None,
            "mid": self.mid,
            "spread_bp": self.spread_bp,
            "bids": [list(level) for level in bids],
            "asks": [list(level) for level in asks],
        }
