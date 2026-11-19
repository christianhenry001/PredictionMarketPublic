"""
Per-market order-flow metrics for the single-venue maker bot (added
2026-09-28, "Item 0: baseline metrics"): how much the bot posts and
cancels relative to what fills, how long orders rest, which ladder rungs
actually fill, and how the mid moves after our fills.

One OrderMetrics per quoter (SingleVenueQuoter.metrics). The quoter feeds
it every post, cancel and quote fill, and the external mid on every
reprice and watchdog tick (the markouts need a mid sampled AFTER a fill,
which on a quiet book only the watchdog provides). Nothing here makes a
trading decision. Emitted by log_periodic_instrumentation and once more at
shutdown.

Definitions (also what analyze_order_metrics.py computes from old logs):
  - posts / cancels / fills per hour: counts since the quoter started,
    divided by elapsed hours. A fill is one fill event (partial fills count
    separately); fill contracts are reported alongside.
  - cancel-to-fill ratio: cancels / fill events. None while nothing filled.
  - resting time: from our post call returning to the order's FIRST cancel
    or FIRST fill, whichever came first -- one sample per order. Reported
    combined, plus split by how the order ended.
  - fill rate per rung (ladder): of the orders posted at rung k, the share
    that got at least one fill, and filled contracts / posted contracts.
    Single-order mode reports everything as rung 0.
  - markouts at +5s / +30s / +120s, in cents per contract, size-weighted,
    + = good for us. Two numbers per horizon:
      mid move: the external mid (outside liquidity only, our own quotes
        subtracted) at t+h minus the mid at the fill, signed from our side.
      vs fill price: the mid at t+h minus our fill price, signed -- the
        convention analyze_fills.py uses (spread captured + mid move).
    Fills whose horizon hasn't elapsed yet are pending, not counted.
"""

from __future__ import annotations

import json
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from decimal import Decimal

MARKOUT_HORIZONS = (5.0, 30.0, 120.0)
# Bound on per-order bookkeeping (post time, rung) -- oldest dropped first,
# same pattern as MAX_ORDER_CONTEXT in single_venue_maker_bot.py.
MAX_TRACKED_ORDERS = 5000
MAX_SAMPLES = 5000


def quantile(values, p: float) -> float | None:
    """Nearest-rank quantile (same convention as RollingLatencyStats and
    analyze_latency._pct). None for no samples."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[min(len(ordered) - 1, int(p * len(ordered)))]


@dataclass
class _PendingMarkout:
    at: float
    side: str
    size: Decimal
    price: Decimal  # our side's own terms
    mid_at_fill: Decimal | None  # yes-denominated
    rung: int
    results: dict[float, tuple[float, float] | None] = field(default_factory=dict)  # horizon -> (mid move c, vs price c)


class OrderMetrics:
    def __init__(self, started_at: float | None = None):
        self.started_at = time.time() if started_at is None else started_at
        self.posts = 0
        self.cancels = 0
        self.fills = 0
        self.filled_contracts = Decimal(0)
        self.cancels_by_reason: Counter[str] = Counter()
        # A post at the same side and price as a cancel in the same reprice
        # batch: the order lost its queue place for nothing.
        self.same_price_reposts = 0
        # Size-down modifies (USE_MODIFY_FOR_SIZE_DOWN): "sent", then how each
        # ended -- "preserved" / "reset" / "unknown" (queue priority after the
        # exchange confirmed it), "rejected", or "failed" (the request itself).
        self.modifies: Counter[str] = Counter()
        self._orders: dict[str, tuple[float, int, int]] = {}  # order_id -> (posted_at, rung, size)
        self._order_queue: deque[str] = deque()
        self._ended: set[str] = set()  # orders already counted in a resting-time sample
        self._rest_cancel: deque[float] = deque(maxlen=MAX_SAMPLES)
        self._rest_fill: deque[float] = deque(maxlen=MAX_SAMPLES)
        self.rung_posts: Counter[int] = Counter()
        self.rung_posted_contracts: Counter[int] = Counter()
        self.rung_filled_orders: Counter[int] = Counter()
        self.rung_filled_contracts: Counter[int] = Counter()
        self._filled_orders: set[str] = set()
        self._pending: list[_PendingMarkout] = []
        self._done: deque[_PendingMarkout] = deque(maxlen=MAX_SAMPLES)

    # ---- feeding ---------------------------------------------------------

    def on_post(self, order_id: str, rung: int | None, size: int, now: float | None = None) -> None:
        now = time.time() if now is None else now
        rung = rung or 0
        self.posts += 1
        self.rung_posts[rung] += 1
        self.rung_posted_contracts[rung] += size
        self._orders[order_id] = (now, rung, size)
        self._order_queue.append(order_id)
        while len(self._order_queue) > MAX_TRACKED_ORDERS:
            old = self._order_queue.popleft()
            self._orders.pop(old, None)
            self._ended.discard(old)
            self._filled_orders.discard(old)

    def on_cancel(self, order_id: str, reason: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self.cancels += 1
        self.cancels_by_reason[reason] += 1
        info = self._orders.get(order_id)
        if info is not None and order_id not in self._ended:
            self._ended.add(order_id)
            self._rest_cancel.append(now - info[0])

    def on_modify(self, outcome: str) -> None:
        self.modifies[outcome] += 1

    def on_batch(self, cancelled: list[tuple[str, Decimal]], posted: list[tuple[str, Decimal]]) -> None:
        """One reprice batch's cancels and posts as (side, price)."""
        remaining = Counter(cancelled)
        for key in posted:
            if remaining[key] > 0:
                remaining[key] -= 1
                self.same_price_reposts += 1

    def on_fill(self, order_id: str | None, side: str, size: Decimal, price: Decimal,
                mid_at_fill: Decimal | None, now: float | None = None) -> None:
        """A fill on one of our quotes (never an unwind)."""
        now = time.time() if now is None else now
        self.fills += 1
        self.filled_contracts += size
        info = self._orders.get(order_id) if order_id is not None else None
        rung = info[1] if info is not None else 0
        if info is not None:
            if order_id not in self._ended:
                self._ended.add(order_id)
                self._rest_fill.append(now - info[0])
            if order_id not in self._filled_orders:
                self._filled_orders.add(order_id)
                self.rung_filled_orders[rung] += 1
        self.rung_filled_contracts[rung] += size
        self._pending.append(_PendingMarkout(now, side, Decimal(size), price, mid_at_fill, rung))

    def on_mid(self, mid: Decimal | None, now: float | None = None) -> None:
        """Latest external mid (yes terms). Resolves every pending markout
        whose horizon has passed with this mid -- sampled at most one
        watchdog tick (0.25s) late."""
        if mid is None or not self._pending:
            return
        now = time.time() if now is None else now
        still: list[_PendingMarkout] = []
        for m in self._pending:
            for h in MARKOUT_HORIZONS:
                if h not in m.results and now - m.at >= h:
                    m.results[h] = _markout(m, mid)
            if len(m.results) == len(MARKOUT_HORIZONS):
                self._done.append(m)
            else:
                still.append(m)
        self._pending = still

    # ---- reporting -------------------------------------------------------

    def fill_markouts(self) -> list[_PendingMarkout]:
        """Every quote fill's markout record (resolved horizons in
        .results), oldest first -- for per-rung analysis (replay_session.py)."""
        return sorted(list(self._done) + list(self._pending), key=lambda m: m.at)

    def snapshot(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        hours = max(now - self.started_at, 1e-9) / 3600.0
        rest_all = list(self._rest_cancel) + list(self._rest_fill)
        markouts = {}
        for h in MARKOUT_HORIZONS:
            vals = [(m.results[h], m.size) for m in list(self._done) + self._pending if m.results.get(h) is not None]
            weight = sum(float(s) for _, s in vals)
            markouts[f"{h:g}s"] = None if not vals else {
                "n": len(vals),
                "mid_move_c": round(sum(r[0] * float(s) for r, s in vals) / weight, 3),
                "vs_fill_c": round(sum(r[1] * float(s) for r, s in vals) / weight, 3),
                # Summed over contracts, not averaged: fill P&L in cents at
                # this horizon (before rebates) -- what an A/B compares when
                # one side fills less often.
                "vs_fill_total_c": round(sum(r[1] * float(s) for r, s in vals), 2),
            }
        rungs = {}
        for k in sorted(set(self.rung_posts) | set(self.rung_filled_contracts)):
            posted = self.rung_posts[k]
            rungs[str(k)] = {
                "posts": posted,
                "filled_orders": self.rung_filled_orders[k],
                "order_fill_rate": round(self.rung_filled_orders[k] / posted, 4) if posted else None,
                "contract_fill_rate": (round(float(self.rung_filled_contracts[k]) / self.rung_posted_contracts[k], 4)
                                       if self.rung_posted_contracts[k] else None),
            }
        return {
            "elapsed_h": round(hours, 4),
            "posts": self.posts, "cancels": self.cancels, "fills": self.fills,
            "fill_contracts": str(self.filled_contracts),
            "posts_per_h": round(self.posts / hours, 1),
            "cancels_per_h": round(self.cancels / hours, 1),
            "fills_per_h": round(self.fills / hours, 1),
            "cancel_to_fill": round(self.cancels / self.fills, 2) if self.fills else None,
            "rest_s": _rest_summary(rest_all),
            "rest_to_cancel_s": _rest_summary(self._rest_cancel),
            "rest_to_fill_s": _rest_summary(self._rest_fill),
            "rungs": rungs,
            "markouts": markouts,
            "markouts_pending": len(self._pending),
            "same_price_reposts": self.same_price_reposts,
            "modifies": dict(self.modifies),
            "cancels_by_reason": dict(self.cancels_by_reason.most_common()),
        }

    def log_line(self, now: float | None = None) -> str:
        """One parseable line: `order metrics <json>`."""
        return "order metrics " + json.dumps(self.snapshot(now), sort_keys=False)


# Same-side fills closer than this are one fill EPISODE in the per-rung report:
# deep rungs fill in a few sweeps, and those, not fills, are the independent samples.
EPISODE_GAP_SECONDS = 10.0


def per_rung_pnl(marks, gap: float | None = None) -> dict:
    """Per posted rung: fills, contracts, fill episodes it took part in
    (same-side fills under EPISODE_GAP_SECONDS apart are one episode), and
    P&L vs fill price summed over contracts at +30s / +120s (cents, before
    rebates; a fill too late in the tape for a horizon is left out of it and
    counted in "unresolved_120s"). "episode_pnl_120s" lists each episode's
    +120s total for this rung, for spotting a result carried by one or two.
    `marks`: objects with .at, .side, .size, .rung and .results {horizon:
    (mid move c, vs fill c) or None} -- _PendingMarkout, or the same shape
    built from a log (analyze_order_metrics.py --per-rung)."""
    gap = EPISODE_GAP_SECONDS if gap is None else gap
    last: dict[str, float] = {}
    current: dict[str, int] = {}
    episode_of: list[int] = []
    n_episodes = 0
    marks = sorted(marks, key=lambda m: m.at)
    for m in marks:
        if m.side not in last or m.at - last[m.side] >= gap:
            n_episodes += 1
            current[m.side] = n_episodes
        last[m.side] = m.at
        episode_of.append(current[m.side])
    out: dict[str, dict] = {}
    for m, ep in zip(marks, episode_of):
        r = out.setdefault(str(m.rung), {"fills": 0, "contracts": 0.0, "episodes": set(), "pnl_30s_c": 0.0,
                                         "pnl_120s_c": 0.0, "contracts_120s": 0.0, "unresolved_120s": 0,
                                         "episode_pnl_120s": {}})
        size = float(m.size)
        r["fills"] += 1
        r["contracts"] += size
        r["episodes"].add(ep)
        if m.results.get(30) is not None:
            r["pnl_30s_c"] += m.results[30][1] * size
        if m.results.get(120) is not None:
            r["pnl_120s_c"] += m.results[120][1] * size
            r["contracts_120s"] += size
            r["episode_pnl_120s"][ep] = r["episode_pnl_120s"].get(ep, 0.0) + m.results[120][1] * size
        else:
            r["unresolved_120s"] += 1
    for r in out.values():
        r["episodes"] = len(r["episodes"])
        r["pnl_30s_c"] = round(r["pnl_30s_c"], 2)
        r["pnl_120s_c"] = round(r["pnl_120s_c"], 2)
        r["pnl_120s_per_ct"] = round(r["pnl_120s_c"] / r["contracts_120s"], 3) if r["contracts_120s"] else None
        r["episode_pnl_120s"] = [round(v, 2) for v in r["episode_pnl_120s"].values()]
    return dict(sorted(out.items()))

def _rest_summary(samples) -> dict | None:
    samples = list(samples)
    if not samples:
        return None
    return {"n": len(samples), "p50": round(quantile(samples, 0.5), 3), "p90": round(quantile(samples, 0.9), 3)}


def _markout(m: _PendingMarkout, mid_later: Decimal) -> tuple[float, float] | None:
    """(mid move, vs fill price) in cents per contract, + = good for us. A
    "yes" fill bought yes, so the yes mid rising is good; a "no" fill sold
    yes (bought no), so the yes mid falling is good."""
    sign = 1 if m.side == "yes" else -1
    yes_fill = m.price if m.side == "yes" else Decimal(1) - m.price
    vs_fill = float(sign * (mid_later - yes_fill) * 100)
    if m.mid_at_fill is None:
        return None
    return float(sign * (mid_later - m.mid_at_fill) * 100), vs_fill
