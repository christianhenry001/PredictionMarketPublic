"""
Paper trading: realistic simulated fills against real live order-book
depth, with a persisted running ledger. Built because Polymarket US has no
sandbox/testnet at all (confirmed 2026-09-14) -- this is the practical
substitute for exercising order-execution logic against live conditions
without ever sending a real order, on either venue.

Two distinct simulation problems, because taker and maker fills work
differently:

  Taker (PaperExecutor): you decided to trade because you saw a specific
  price and size available RIGHT NOW. Simulating the fill just means
  recording it at that price/size and computing the real fee -- there's no
  timing uncertainty, because a taker order (in the model this project
  already uses) is assumed to execute against the depth you just observed.
  This also covers the maker bot's HEDGE leg (still a taker order on
  Kalshi) -- PaperKalshiHedgeClient walks REAL live Kalshi depth the same
  way, which is arguably more realistic than even a real demo fill would
  be, since it's matched against genuine live liquidity.

  Maker (PaperFillSimulator): a resting quote's fate depends on what OTHER
  participants do after you post it -- unanswerable from a single book
  snapshot. This uses Polymarket US's trade tape (Book.last_trade_price /
  Book.cumulative_volume) to ask "did new volume just print at a price
  that would cross one of my resting quotes?" and synthesizes a MakerFill
  when it does, at the QUOTE'S OWN price (a resting order fills at what it
  was posted at, not at the taker's price).

  QUEUE POSITION (added 2026-09-15, after the naive version was caught
  being systematically too optimistic): PaperFillSimulator tracks an
  assumed queue position per quote, using the real displayed size at your
  exact price when the quote first goes live as "ahead of you" -- the
  standard conservative assumption for price-time-priority venues, since
  you join at the back. Crossing volume clears that ahead-of-you amount
  first; only the remainder fills you. Still an approximation, stated
  plainly: it can't see cancellations by orders ahead of you (real venues
  free up queue position that way; this doesn't, so it stays somewhat
  pessimistic about how fast you'd move up), and a batch of trades at
  different prices between two of your book updates gets attributed to a
  single crossing price rather than reconstructed individually, since the
  feed's aggregated stats don't have that resolution. Net effect versus no
  queue model at all: fills come slower and less often, which is the
  direction reality points -- but this still isn't a guarantee. Treat
  paper P&L from the maker bot as a more-informed estimate, not a
  prediction.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Callable

from executor import Fill
from fees import kalshi_taker_fee, polymarket_taker_fee
from maker_executor import MakerFill, RestingQuote
from orderbook_feed import Book

log = logging.getLogger("paper_trading")


# --------------------------------------------------------------------------
# Shared ledger
# --------------------------------------------------------------------------


def _resting(quoter) -> list:
    """Every order a quoter has resting. The single-venue quoter can rest
    several per side (resting_quotes()); maker_bot's MakerQuoter has one per
    side in active_quotes."""
    if hasattr(quoter, "resting_quotes"):
        return quoter.resting_quotes()
    return [q for q in quoter.active_quotes.values() if q is not None]


@dataclass
class PaperLedger:
    """Running totals across paper fills, optionally persisted as one
    JSON-lines record per event. Realized P&L is recorded explicitly by
    callers once they know a hedge pair actually completed (arb_bot.py's
    ArbBot after both legs succeed; maker_bot.py's HedgeCoordinator after a
    successful hedge) -- this module doesn't try to infer P&L from
    individual fills on its own, since a single leg's cash flow isn't
    profit or loss by itself."""

    out_path: Path | None = None
    # Log-line prefix. maker_bot --live --ledger passes "LIVE": its real fills logged as [PAPER]
    # (until 2026-11-08), and the log analyzers read [PAPER] lines as a simulated run.
    label: str = "PAPER"
    fill_count: int = 0
    total_fees: Decimal = field(default_factory=lambda: Decimal("0"))
    realized_pnl: Decimal = field(default_factory=lambda: Decimal("0"))

    def record_fill(self, venue: str, symbol: str, side: str, price: Decimal, count: Decimal, fee: Decimal) -> None:
        self.fill_count += 1
        self.total_fees += fee
        log.info("[%s] fill: %s %s buy %s %s @ %.4f (fee $%.2f)", self.label, venue, symbol, count, side, price, fee)
        self._write({
            "type": "fill", "venue": venue, "symbol": symbol, "side": side,
            "price": str(price), "count": str(count), "fee": str(fee),
        })

    def record_realized_pnl(self, pair_name: str, amount: Decimal) -> None:
        self.realized_pnl += amount
        log.info("[%s] realized P&L on %s: $%.2f (cumulative $%.2f)", self.label, pair_name, amount, self.realized_pnl)
        self._write({"type": "realized_pnl", "pair": pair_name, "amount": str(amount), "cumulative": str(self.realized_pnl)})

    def _write(self, record: dict) -> None:
        if self.out_path is None:
            return
        record = {"wall_time": time.time(), **record}
        try:
            with self.out_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except OSError:
            log.exception("could not write paper trading record to %s", self.out_path)

    def summary(self) -> str:
        return f"{self.fill_count} fills, ${self.total_fees:.2f} total fees, ${self.realized_pnl:.2f} realized P&L"


# --------------------------------------------------------------------------
# Taker-side: PaperExecutor (arb_bot.py) and the maker bot's hedge leg
# --------------------------------------------------------------------------


class PaperExecutor:
    """OrderExecutor-compatible (see executor.py). Records a fill at the
    given price/count -- the price you already observed when deciding to
    trade -- computes the real fee, and logs it to the ledger. Never sends
    a real order."""

    def __init__(self, ledger: PaperLedger):
        self.ledger = ledger

    def buy_kalshi(self, ticker: str, side: str, price: Decimal, count: int) -> Fill:
        fee = kalshi_taker_fee(price, count)
        self.ledger.record_fill("kalshi", ticker, side, price, count, fee)
        return Fill("kalshi", ticker, side, price, count, order_id=f"paper-{self.ledger.fill_count}", dry_run=True)

    def buy_polymarket(self, market_slug: str, side: str, price: Decimal, count: int) -> Fill:
        fee = polymarket_taker_fee(price, count)
        self.ledger.record_fill("polymarket_us", market_slug, side, price, count, fee)
        return Fill("polymarket_us", market_slug, side, price, count, order_id=f"paper-{self.ledger.fill_count}", dry_run=True)


def _capped_kalshi_fills(book: Book, side: str, count: int, cap: Decimal) -> list[tuple[Decimal, Decimal]]:
    """(price, qty) fills for buying `side` on Kalshi, walking real book
    depth up to `count`, keeping only the best-price-first prefix at `cap`
    or better -- mirrors exactly what a real IOC order at that cap fills."""
    if side == "yes":
        raw = book.walk_asks(Decimal(count))
    elif side == "no":
        raw = [(Decimal(1) - p, q) for p, q in book.walk_bids(Decimal(count))]
    else:
        raise ValueError(f"side must be 'yes' or 'no', got {side!r}")

    kept = []
    for price, qty in raw:
        if price > cap:
            break
        kept.append((price, qty))
    return kept


class PaperKalshiHedgeClient:
    """Drop-in for executor.KalshiOrderClient's place_ioc_hedge, matching
    its NORMALIZED response shape (flat, `{"fill_count": N, "avg_price":
    ..., "avg_fee_per_contract": ..., ...}` -- see
    KalshiOrderClient._normalize_response, updated 2026-09-15 for the V2
    migration) so maker_bot.py's HedgeCoordinator needs no changes to use
    this instead of the real client. avg_price/avg_fee_per_contract are
    already in this call's own `side` convention here (this client works
    entirely in that convention already, unlike the real client which has
    to convert Kalshi's YES-denominated wire format back into it).
    HedgeCoordinator uses those fields to compute realized paper P&L; this
    class doesn't touch the ledger's realized-P&L bookkeeping itself, so
    all of it for one fill-and-hedge event lives in one place.

    Simulates the fill against REAL live Kalshi depth, which is why this
    needs the ledger anyway: to record the Kalshi-side fill/fee, just not
    to compute the cross-venue P&L."""

    def __init__(self, ledger: PaperLedger, book_lookup: Callable[[str], Book | None]):
        self.ledger = ledger
        self.book_lookup = book_lookup  # ticker -> current Kalshi Book, or None
        self._next_id = 0

    def place_ioc_hedge(self, ticker: str, side: str, action: str, max_price: Decimal, count: int) -> dict:
        self._next_id += 1
        order_id = f"paper-hedge-{self._next_id}"
        book = self.book_lookup(ticker)
        empty = {"order_id": order_id, "fill_count": 0, "remaining_count": count, "avg_price": None, "avg_fee_per_contract": None}
        if book is None:
            return empty

        fills = _capped_kalshi_fills(book, side, count, max_price)
        filled = sum((q for _, q in fills), Decimal(0))
        if filled <= 0:
            return empty

        vwap = (sum((p * q for p, q in fills), Decimal(0)) / filled).quantize(Decimal("0.0001"))
        fee_total = kalshi_taker_fee(vwap, filled)
        fee_per_contract = (fee_total / filled).quantize(Decimal("0.0001"))
        self.ledger.record_fill("kalshi", ticker, side, vwap, filled, fee_total)
        return {
            "order_id": order_id,
            "fill_count": int(filled),
            "remaining_count": int(count - filled),
            "avg_price": vwap,
            "avg_fee_per_contract": fee_per_contract,
        }


# --------------------------------------------------------------------------
# Maker-side: quote posting/canceling and the trade-tape fill simulator
# --------------------------------------------------------------------------


class PaperMakerQuoteClient:
    """MakerOrderClient-compatible (see maker_executor.py). Tracks a virtual
    resting order and logs it -- identical mechanically to
    ShadowMakerExecutor, kept as its own class for clearer [PAPER] log
    labeling. Fills are decided separately, by PaperFillSimulator below."""

    def __init__(self) -> None:
        self._next_id = 0

    def post_quote(self, market_slug: str, side: str, price: Decimal, size: int) -> RestingQuote:
        self._next_id += 1
        order_id = f"paper-quote-{self._next_id}"
        log.info("[PAPER] quoting %s %d @ %.4f on polymarket_us:%s (%s)", side, size, price, market_slug, order_id)
        return RestingQuote(order_id=order_id, market_slug=market_slug, side=side, price=price, size=size)

    def cancel_quote(self, quote: RestingQuote) -> None:
        log.info("[PAPER] canceling %s (%s %d @ %.4f on %s)", quote.order_id, quote.side, quote.size, quote.price, quote.market_slug)

    def modify_quote(self, quote: RestingQuote, new_size: int) -> None:
        """Size-down modify: same order id, so PaperFillSimulator keeps its
        queue position (the size ahead of it doesn't change)."""
        log.info("[PAPER] modifying %s (%s @ %.4f on %s) size %d -> %d", quote.order_id, quote.side, quote.price,
                 quote.market_slug, quote.size, new_size)

    # Batch forms (USE_BATCH_ENDPOINTS): one entry at a time, so paper runs
    # take the bot's batch path with the same bookkeeping.
    def post_quotes(self, orders: list[tuple[str, str, Decimal, int]]) -> list[RestingQuote]:
        return [self.post_quote(*o) for o in orders]

    def cancel_quotes(self, quotes: list[RestingQuote]) -> None:
        for q in quotes:
            self.cancel_quote(q)

    def modify_quotes(self, items: list[tuple[RestingQuote, int]]) -> None:
        for q, n in items:
            self.modify_quote(q, n)


class PaperFillSimulator:
    """Watches Polymarket US book updates for trade-tape crossings against
    currently active paper quotes (read from the given MakerQuoter
    instances) and calls `on_fill` with a synthesized MakerFill when one
    would plausibly have been hit.

    Tracks an assumed QUEUE POSITION per quote rather than assuming your
    order is always first in line: the first time a quote is observed, the
    size already displayed at its exact price in the real book is recorded
    as "ahead of you" (the standard conservative assumption -- you join at
    the back of a price-time-priority queue). Crossing trade volume is
    applied against that ahead-of-you amount first; only volume beyond it
    is credited as your fill. Reset automatically when a quote is replaced
    (a new post gets a new order_id, so it starts a fresh queue position).

    Still an approximation, stated plainly:
      - Can't see cancellations by orders ahead of you -- real venues open
        up queue position that way; this doesn't, so it stays somewhat
        pessimistic about how fast you'd move up the queue.
      - If multiple trades at different prices land inside one update batch,
        the feed's aggregated stats (one cumulative volume, one last price)
        don't have the resolution to attribute volume across levels
        precisely -- this assumes it all happened at the crossing price.
      - A quote's queue position is captured from whatever book snapshot
        first reveals it as active; if a quote is posted in the very same
        update as a crossing trade, that snapshot may already be
        post-trade, very slightly overstating the size ahead of you in
        that narrow, rare timing coincidence.
    Net effect versus having no queue model at all: fills should now come
    slower and less often, which is the direction reality points."""

    def __init__(self, quoters: dict[str, object], pairs_by_slug: dict[str, object], on_fill: Callable[[object, MakerFill], None]):
        self.quoters = quoters  # pair name -> MakerQuoter
        self.pairs_by_slug = pairs_by_slug  # poly_market_slug -> MakerPair
        self.on_fill = on_fill
        self._last_volume: dict[str, Decimal] = {}
        self._queue_ahead: dict[str, Decimal] = {}  # order_id -> assumed size still ahead of us

    def on_polymarket_update(self, book: Book) -> None:
        pair = self.pairs_by_slug.get(book.symbol)
        if pair is None:
            return

        quoter = self.quoters.get(pair.name)
        if quoter is None:
            return
        self._prune_stale_queue_entries()

        # Establish a queue position for any quote we haven't seen yet,
        # from whatever book snapshot first reveals it -- independent of
        # whether trade-tape volume-delta history exists yet. Doing this
        # only inside the volume-delta branch below would miss a quote's
        # TRUE starting depth: the branch bails out entirely on the first
        # ever observation of a market (no prior volume to diff against),
        # and capturing the baseline one update late means we'd use
        # whatever the book happened to look like AFTER that missed
        # update instead of when the quote actually went live.
        for quote in _resting(quoter):
            if quote.order_id not in self._queue_ahead:
                self._queue_ahead[quote.order_id] = self._displayed_size_at(book, quote.side, quote.price)

        prev_volume = self._last_volume.get(book.symbol)
        if book.cumulative_volume is not None:
            self._last_volume[book.symbol] = book.cumulative_volume

        if prev_volume is None or book.cumulative_volume is None or book.last_trade_price is None:
            return  # first observation for this market, or venue gave us no trade-tape data yet

        new_volume = book.cumulative_volume - prev_volume
        if new_volume <= 0:
            return

        trade_price = book.last_trade_price

        # Most aggressive first on each side, and one print's volume is used
        # up as it goes: with several resting orders on a side (the single-
        # venue ladder) a print must not fill every one of them in full.
        # With one quote per side this is exactly the old behavior.
        volume_left = {"yes": new_volume, "no": new_volume}
        for quote in sorted(_resting(quoter), key=lambda q: q.price, reverse=True):
            side = quote.side
            if volume_left[side] <= 0:
                continue

            # side "yes": our bid at quote.price is hit by a sell printing
            # at or below it. side "no": our synthesized sell-YES ask at
            # (1 - quote.price) is hit by a buy printing at or above that,
            # i.e. trade_price >= 1 - quote.price <=> (1-trade_price) <= quote.price.
            crossed = (
                (side == "yes" and trade_price <= quote.price)
                or (side == "no" and (Decimal(1) - trade_price) <= quote.price)
            )
            if not crossed:
                continue

            ahead = self._queue_ahead[quote.order_id]
            self._queue_ahead[quote.order_id] = max(ahead - volume_left[side], Decimal(0))
            available = volume_left[side] - ahead
            if available <= 0:
                continue  # the whole print was absorbed by orders ahead of us in the queue

            fill_size = min(available, Decimal(quote.size))
            volume_left[side] -= fill_size
            self.on_fill(pair, MakerFill(
                market_slug=pair.poly_market_slug, side=side, price=quote.price,
                size=fill_size, order_id=quote.order_id,
            ))

    def _displayed_size_at(self, book: Book, side: str, quote_price: Decimal) -> Decimal:
        """Real resting size at the exact price our quote sits at, used as
        the assumed queue position when a quote is first observed."""
        if side == "yes":
            return book.bids.get(quote_price, Decimal(0))
        return book.asks.get(Decimal(1) - quote_price, Decimal(0))

    def _prune_stale_queue_entries(self) -> None:
        """Drop queue-position tracking for quotes that are no longer
        active anywhere (replaced or canceled), so this doesn't grow
        unbounded. Must check ALL quoters -- order_ids are unique across
        the whole run (one shared order client issues them), but this
        dict holds entries for every pair at once, not just whichever
        pair triggered the current update."""
        live_ids = {q.order_id for quoter in self.quoters.values() for q in _resting(quoter)}
        for order_id in list(self._queue_ahead):
            if order_id not in live_ids:
                del self._queue_ahead[order_id]
