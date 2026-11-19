"""
Dry-run-only instrumentation: measures how fast a detected edge decays.

Never places an order and never touches OrderExecutor/LiveExecutor. It runs
its own ArbDetector instance purely for the math (evaluate_direction /
check_all_opportunities), independent of the trading-facing detector in
arb_bot.py, so it can log opportunities that detector would ignore (below
pair.min_edge, or on a book the trading path's freshness guard would refuse
to act on — see arbitrage.py's ArbDetector.check() vs check_all_opportunities()).

For every opportunity whose net edge clears EDGE_LOG_THRESHOLD (0.0 by
default — i.e. essentially every crossed-book event, not just ones that
would actually clear a pair's real min_edge), writes one JSON-lines record
per reading: an initial one at detection time, then more at +200ms, +1s,
and +5s re-evaluating the SAME direction fresh against current books. All
four share an `event_id` so they can be joined back together for analysis.
`net_edge`/`gross_edge`/etc. are null in a later reading if the opportunity
had already fully disappeared (book no longer crossed) by that offset.

Each book snapshot carries two timestamps: `local_receipt_time` (when this
process actually processed that update — Book.updated_at) and
`venue_event_time` (the exchange's own event time, where the venue exposes
one). Kalshi's orderbook_snapshot/orderbook_delta messages don't carry one
in what's been observed, so it's null for Kalshi; Polymarket US's
market_data does (`transactTime`), so `feed_latency_s` is only ever
computed for that side today.

CAVEAT (found 2026-09-14, not yet resolved): `feed_latency_s` came out
negative on real captured data, which isn't physically sensible for a
receipt-minus-sent-time delta. Traced to the local machine's clock, not a
parsing bug — an NTP query (`w32tm /stripchart /computer:time.windows.com`)
showed a real ~0.5s offset, comparable to or larger than the latency being
measured, and a `w32tm /resync` to fix it was refused for lack of admin
rights in the session that found this. Until that's actually resynced (or
this machine's clock is otherwise confirmed accurate), treat
`feed_latency_s` as unreliable in absolute terms — relative comparisons
across readings should still be fine, since a constant offset cancels out.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from arbitrage import ArbDetector, ArbOpportunity
from market_pairs import MarketPair
from orderbook_feed import Book, BookStore

log = logging.getLogger("instrumentation")

EDGE_LOG_THRESHOLD = Decimal("0.0")
RESAMPLE_OFFSETS_MS = (0, 200, 1000, 5000)  # 0 = the initial detection itself

_event_ids = itertools.count(1)


def _parse_iso8601_epoch(ts: str | None) -> float | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _book_snapshot(book: Book | None) -> dict | None:
    if book is None:
        return None
    bb, ba = book.best_bid(), book.best_ask()
    venue_epoch = _parse_iso8601_epoch(book.venue_event_time)
    return {
        "venue": book.venue,
        "symbol": book.symbol,
        "bid_price": str(bb[0]) if bb else None,
        "bid_size": str(bb[1]) if bb else None,
        "ask_price": str(ba[0]) if ba else None,
        "ask_size": str(ba[1]) if ba else None,
        "local_receipt_time": book.updated_at,
        "venue_event_time": book.venue_event_time,
        "feed_latency_s": (book.updated_at - venue_epoch) if venue_epoch is not None else None,
    }


def _opportunity_fields(opp: ArbOpportunity | None) -> dict:
    if opp is None:
        return {"gross_edge": None, "fees_per_contract": None, "net_edge": None, "contracts": None}
    return {
        "gross_edge": str(opp.gross_edge),
        "fees_per_contract": str(opp.fees_per_contract),
        "net_edge": str(opp.net_edge),
        "contracts": opp.max_contracts,
    }


@dataclass
class _PendingEvent:
    event_id: int
    pair: MarketPair
    direction: str


class EdgeDecayLogger:
    """Wire alongside (not instead of) the trading-facing ArbDetector via
    check_and_log() on every book update. Dry-run only by construction —
    arb_bot.py refuses to attach this when --live is set."""

    def __init__(
        self,
        store: BookStore,
        pairs: list[MarketPair],
        out_path: str | Path,
        threshold: Decimal = EDGE_LOG_THRESHOLD,
    ):
        self.store = store
        self.pairs = pairs
        self.out_path = Path(out_path)
        self.threshold = threshold
        # A detector purely for its math — its own on_opportunity callback
        # is never used, since we call evaluate_direction/check_all_opportunities
        # directly instead of relying on its check_all() trading path.
        self._detector = ArbDetector(store, pairs, lambda opp: None)
        self._inflight: set[tuple[str, str]] = set()

    def check_and_log(self) -> None:
        for pair in self.pairs:
            for opp in self._detector.check_all_opportunities(pair, self.threshold):
                key = (pair.name, opp.direction)
                if key in self._inflight:
                    continue  # already tracking a resample sequence for this pair+direction
                self._inflight.add(key)
                event = _PendingEvent(event_id=next(_event_ids), pair=pair, direction=opp.direction)
                asyncio.create_task(self._run_sequence(event, key))

    async def _run_sequence(self, event: _PendingEvent, key: tuple[str, str]) -> None:
        try:
            self._write(event, offset_ms=0)
            prev_ms = 0
            for offset_ms in RESAMPLE_OFFSETS_MS[1:]:
                await asyncio.sleep((offset_ms - prev_ms) / 1000)
                prev_ms = offset_ms
                self._write(event, offset_ms=offset_ms)
        except Exception:
            log.exception("edge-decay logging sequence failed for event %d", event.event_id)
        finally:
            self._inflight.discard(key)

    def _write(self, event: _PendingEvent, offset_ms: int) -> None:
        pair = event.pair
        opp = self._detector.evaluate_direction(pair, event.direction)
        kalshi_book = self.store.books.get(("kalshi", pair.kalshi_ticker))
        poly_book = self.store.books.get(("polymarket_us", pair.poly_market_slug))
        record = {
            "event_id": event.event_id,
            "offset_ms": offset_ms,
            "wall_time": time.time(),
            "pair": pair.name,
            "direction": event.direction,
            "kalshi": _book_snapshot(kalshi_book),
            "polymarket_us": _book_snapshot(poly_book),
            **_opportunity_fields(opp),
        }
        try:
            with self.out_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except OSError:
            log.exception("could not write edge-decay record to %s", self.out_path)
