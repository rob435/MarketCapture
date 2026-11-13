"""The inbound budget: shed and restore `tier:feed` pairs to keep the month under the allowance.

With a budget in the config, the recorder projects a month from its last
day of bytes (`market_tape/recorder/meter.py`) and, when the projection is
over the allowance, gives up the configured `tier:feed` pairs in order, one
action per `act_every_minutes` (an hour by default), restoring them in reverse
once under pace. The maintainer steps it every tick and re-plans the tiers'
topics when the shed set changes.
"""

from __future__ import annotations

import logging
from typing import Any

from market_tape.config import BudgetSettings
from market_tape.recorder.meter import MINUTE_NS, ByteMeter


MONTH_SECONDS = 30 * 86_400
#: An hour of received bytes before a monthly projection means anything.
BUDGET_MIN_WINDOW_NS = 60 * MINUTE_NS


class BudgetController:
    """Sheds and restores `tier:feed` pairs to keep the month's inbound bytes under the allowance.

    The projection is what the pairs still subscribed bring in: a shed pair's
    bytes sit in the trailing window for a day, and counting them would keep
    shedding for a day after the shed that was enough. One action per
    `act_every_minutes`: a shed takes as many pairs, in order, as the
    projection needs; a restore returns the last pair only when its month, as
    measured when it was shed, fits under the restore line beside everything
    still subscribed. Over budget with the list exhausted is said every action.
    """

    def __init__(self, settings: BudgetSettings, meter: ByteMeter) -> None:
        self.settings = settings
        self.meter = meter
        self.shed_active: list[tuple[str, str]] = []
        #: GB/month each shed pair carried when it was shed: what restoring it costs.
        self.shed_gb: dict[tuple[str, str], float] = {}
        self.last_action_ns = 0
        self.projected_gb: float | None = None

    @staticmethod
    def _key(pair: tuple[str, str]) -> str:
        return f"feed:{pair[0]}:{pair[1]}"

    def _window_seconds(self, now_ns: int) -> float | None:
        window_ns = self.meter.window_ns(now_ns)
        if window_ns < BUDGET_MIN_WINDOW_NS:
            return None
        return window_ns / 1e9

    def projection_gb(self, now_ns: int) -> float | None:
        seconds = self._window_seconds(now_ns)
        if seconds is None:
            return None
        received = self.meter.last_day("all", now_ns)
        for pair in self.shed_active:
            received -= self.meter.last_day(self._key(pair), now_ns)
        return max(received, 0) / seconds * MONTH_SECONDS / 1e9

    def pair_gb(self, pair: tuple[str, str], now_ns: int) -> float:
        """One pair's month at its rate over the window."""

        seconds = self._window_seconds(now_ns)
        if seconds is None:
            return 0.0
        return self.meter.last_day(self._key(pair), now_ns) / seconds * MONTH_SECONDS / 1e9

    @property
    def over(self) -> bool:
        return (
            self.settings.monthly_gb is not None
            and self.projected_gb is not None
            and self.projected_gb > self.settings.monthly_gb
        )

    def step(self, now_ns: int) -> bool:
        """Re-project; shed or restore when due. Returns whether the shed set changed."""

        self.projected_gb = self.projection_gb(now_ns)
        limit = self.settings.monthly_gb
        if limit is None or self.projected_gb is None:
            return False
        if self.last_action_ns and now_ns - self.last_action_ns < self.settings.act_every_minutes * MINUTE_NS:
            return False
        if self.projected_gb > limit:
            self.last_action_ns = now_ns
            projected = self.projected_gb
            changed = False
            while projected > limit and len(self.shed_active) < len(self.settings.shed):
                pair = self.settings.shed[len(self.shed_active)]
                gb = self.pair_gb(pair, now_ns)
                self.shed_gb[pair] = gb
                self.shed_active.append(pair)
                projected -= gb
                changed = True
                logging.warning(
                    "over budget (%.0f GB/month projected, %.0f allowed): shedding %s:%s (%.0f GB/month)",
                    self.projected_gb,
                    limit,
                    *pair,
                    gb,
                )
            if projected > limit:
                logging.warning(
                    "over budget with every sheddable feed shed: %.0f GB/month projected against %.0f allowed; the config decides what else goes",
                    projected,
                    limit,
                )
            return changed
        if self.shed_active:
            pair = self.shed_active[-1]
            gb = self.shed_gb.get(pair, 0.0)
            if self.projected_gb + gb < limit * self.settings.restore_below:
                self.shed_active.pop()
                self.last_action_ns = now_ns
                logging.info(
                    "under budget (%.0f GB/month projected, %.0f allowed): restoring %s:%s (%.0f GB/month)",
                    self.projected_gb,
                    limit,
                    *pair,
                    gb,
                )
                return True
        return False

    def status(self) -> dict[str, Any]:
        return {
            "monthly_gb": self.settings.monthly_gb,
            "projected_month_gb": None if self.projected_gb is None else round(self.projected_gb, 1),
            "over": self.over,
            "shed": [f"{tier}:{feed}" for tier, feed in self.shed_active],
            "shed_gb_month": {
                f"{tier}:{feed}": round(self.shed_gb.get((tier, feed), 0.0), 1) for tier, feed in self.shed_active
            },
            "shed_order": [f"{tier}:{feed}" for tier, feed in self.settings.shed],
            "last_action_ns": self.last_action_ns,
        }
