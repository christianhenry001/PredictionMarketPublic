"""
Position reconciliation for the single-venue maker bot -- the no-hedge
equivalent of maker_bot.py's PositionReconciler, but comparing this bot's
LOCAL PositionTracker belief against Polymarket's own REAL reported
position for the same market, not two venues' positions against each
other (there's only one venue here).

IMPORTANT DIFFERENCE from the cross-venue reconciler, worth being precise
about rather than assumed identical: maker_bot.py's PositionReconciler was
rebuilt 2026-09-16 SPECIFICALLY because comparing Polymarket's real
position against PositionTracker's local belief was WRONG there -- a
Kalshi hedge doesn't reduce the Polymarket position, so the two were never
meant to converge to the same number, and that comparison would have
halted the bot on its own first successful trade. That bug does NOT apply
here: single_venue_maker_bot.py's PositionTracker only ever changes on a
real fill (see dispatch_fill) or an unwind fill (see InventoryUnwinder),
and nothing else ever touches Polymarket's real reported position either
-- the two ARE meant to be identical at all times in this design. So the
comparison the cross-venue bot's FIRST, buggy attempt used is exactly the
RIGHT one here. Don't port the cross-venue rebuild's conclusion without
re-deriving it -- it was correct for that bot's own specific reason, which
doesn't hold in this one.

WHY THE FIX IS SIMPLER HERE than on the cross-venue bot: there, a real
mismatch meant genuine unhedged exposure needing an active corrective
order -- fixing the local tracker's belief alone would not have reduced
any real risk, since the risk lived on a SEPARATE venue's books regardless
of what our number said. Here, the position already exists wherever it
actually is (on Polymarket, the only venue) -- a mismatch means our
BOOKKEEPING was wrong (the same root-cause shape as the real 2026-09-16
incident this all traces back to: a fill-listener gap), not that risk is
unaccounted for somewhere else. So the fix IS correcting the local
tracker to match the real reported value -- once corrected, every
downstream consumer that reads from PositionTracker (SingleVenueQuoter's
inventory skew, quote_prices()'s max_inventory cap check,
InventoryUnwinder's trigger) automatically sees the true number on its
very next cycle. No separate corrective-order path needed.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

log = logging.getLogger("single_venue_reconciler")

# Duplicated from single_venue_maker_bot.py rather than imported -- same
# reasoning as single_venue_pricing.py's is_quotable and
# single_venue_unwind.py's _fair_value duplication: a small, stable value,
# and importing it would create a circular import (single_venue_maker_bot.py
# is the one that imports PositionReconciler, not the reverse).
POLYMARKET_OPEN_STATE = "MARKET_STATE_OPEN"

# Same value and same real-incident-derived reasoning as maker_bot.py's
# PositionReconciler.refresh_interval: 30s was never chosen for this
# purpose (inherited from BalanceMonitor's cadence), and live evidence
# showed a real correction can be too late by then. Positions/portfolio
# reads are cheap relative to order placement, confirmed on Kalshi's own
# endpoint-cost listing and this project's own live traffic all night.
DEFAULT_REFRESH_INTERVAL = 3.0

# Contracts -- allows for one fill genuinely in flight between our local
# update and the next real API read, not real drift. Same value as the
# cross-venue reconciler's own tolerance.
DEFAULT_TOLERANCE = Decimal("0.5")

# Don't correct toward a venue position that changed this recently -- the
# fill notification that explains it may still be in transit. Found live
# 2026-09-24 (Astros/Mariners, 01:07:47): a positions read landed 2ms after
# a real fill, the reconciler corrected the tracker 0 -> 2, then the fill
# notification arrived 148ms later and added 2 more -> tracker at 4 for 3s
# until the next read undid it. The DEFAULT_TOLERANCE above can't cover
# that once quotes are 2+ contracts. Fill notifications have taken well
# under 1s all night; 2s leaves margin and the local clock's ~0.15s lag
# only makes this wait slightly longer (the safe direction). A genuinely
# missed fill still gets corrected on the first read after its change
# settles -- within about SETTLE + refresh_interval (~5s).
DEFAULT_SETTLE_SECONDS = 2.0


def _parse_iso_epoch(s: str | None) -> float | None:
    """Venue ISO-8601 timestamp (nanosecond precision, 'Z') -> epoch."""
    if not s:
        return None
    try:
        s = s.rstrip("Z")
        base, _, frac = s.partition(".")
        return datetime.fromisoformat(base).replace(tzinfo=timezone.utc).timestamp() + float("0." + (frac or "0"))
    except (ValueError, TypeError):
        return None


@dataclass
class PositionReconciler:
    poly_client: object
    positions: object  # single_venue_maker_bot.PositionTracker -- not imported, avoids a circular import
    store: object  # single_venue_maker_bot.BookStore
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL
    tolerance: Decimal = DEFAULT_TOLERANCE
    settle_seconds: float = DEFAULT_SETTLE_SECONDS
    budget: object = None  # rate_limit.ActionBudget: each positions read counts against the per-key limit
    _last_checked_at: float = field(default=0.0)
    # Pair names whose mismatch was seen on the previous read -- only used
    # when the venue gives no usable updateTime (see _change_is_settled).
    _unconfirmed: set = field(default_factory=set)

    def maybe_check(self, pairs: list) -> None:
        if time.time() - self._last_checked_at < self.refresh_interval:
            return
        self._last_checked_at = time.time()

        if self.budget is not None:
            self.budget.consume(None, 1)
        try:
            resp = self.poly_client.portfolio.positions()
        except Exception:
            log.exception("position reconciliation: failed to fetch real Polymarket US positions -- skipping this cycle")
            return

        pos_by_slug = resp.get("positions") if isinstance(resp, dict) else getattr(resp, "positions", None)
        if not isinstance(pos_by_slug, dict):
            log.error("position reconciliation: unexpected Polymarket US response shape, cannot verify this cycle: %s", resp)
            return

        for pair in pairs:
            if self._market_finished(pair):
                # No real financial action is ever taken by this
                # reconciler (unlike the cross-venue one's corrective
                # hedge), so a closed market isn't the safety hazard it
                # was there -- skipped anyway for cleanliness, since
                # there's nothing meaningful left to correct toward once
                # the bot will never quote this pair again either way.
                continue

            entry = pos_by_slug.get(pair.poly_market_slug)
            raw_net = (entry.get("netPosition") if isinstance(entry, dict) else getattr(entry, "netPosition", None)) if entry is not None else None
            # Kept as Decimal, not int -- found live 2026-09-22: Polymarket
            # US can report a genuinely fractional real position (a real
            # partial-fill artifact, not a display quirk -- see
            # PositionTracker's own docstring for the same finding on the
            # local-tracking side). Truncating the REAL reported value here
            # would have meant this safety net could itself introduce a
            # small error while "correcting" one.
            real_net = Decimal(str(raw_net)) if raw_net is not None else Decimal("0")

            local_net = self.positions.get(pair.name)
            if abs(real_net - local_net) <= self.tolerance:
                self._unconfirmed.discard(pair.name)
                continue

            update_time = (entry.get("updateTime") if isinstance(entry, dict) else getattr(entry, "updateTime", None)) if entry is not None else None
            if not self._change_is_settled(pair.name, update_time):
                log.info(
                    "%s: position mismatch (local %s, venue %s) but the venue position changed moments ago -- "
                    "waiting for the fill notification before correcting",
                    pair.name, local_net, real_net,
                )
                continue
            self._unconfirmed.discard(pair.name)

            log.critical(
                "%s: POSITION DRIFT -- local tracker believed %s, Polymarket US actually reports %s. "
                "Correcting local belief to the real value; this fixes what SingleVenueQuoter's inventory "
                "skew and max_inventory checks see on their very next cycle, and what InventoryUnwinder's "
                "trigger reads on its very next check.",
                pair.name, local_net, real_net,
            )
            self.positions.set(pair.name, real_net)

    def _change_is_settled(self, pair_name: str, update_time: str | None) -> bool:
        """True when a mismatch should be corrected now. With the venue's
        position updateTime: settled once it's at least settle_seconds old.
        Without one (no entry -- e.g. a position that went to zero -- or an
        unparseable value): fall back to requiring the mismatch on two
        consecutive reads."""
        changed_at = _parse_iso_epoch(update_time)
        if changed_at is not None:
            return time.time() - changed_at >= self.settle_seconds
        if pair_name in self._unconfirmed:
            return True
        self._unconfirmed.add(pair_name)
        return False

    def _market_finished(self, pair) -> bool:
        poly_book = self.store.books.get(("polymarket_us", pair.poly_market_slug))
        return poly_book is not None and poly_book.market_state is not None and poly_book.market_state != POLYMARKET_OPEN_STATE
