"""
Cross-venue arbitrage detection for a binary market listed on both Kalshi
and Polymarket US.

The trade: buy YES on one venue and buy NO on the other. If the two costs
sum to less than $1.00 per contract, the pair pays out exactly $1.00 no
matter how the market resolves — the gap (less fees) is locked-in profit.
There are two directions, and either, both, or neither may be open at a
given moment:

    A) buy YES on Kalshi + buy NO on Polymarket US
    B) buy YES on Polymarket US + buy NO on Kalshi

Both venues turn out to price a binary market the same way underneath: one
order book, denominated as probability-of-YES. Neither venue exposes buying
NO as a real second order book:
  - Kalshi only ever has YES-side interest; orderbook_feed.KalshiFeed
    synthesizes book.asks (YES ask) from resting NO bids, so book.bids/asks
    already give "buy YES" directly, and "buy NO" is the complement of the
    best YES bid.
  - Polymarket US's book is the real thing (bids/asks are genuine resting
    orders), but "buy NO at price p" is submitted as ORDER_INTENT_BUY_SHORT
    at price p and executed by the platform as selling YES at (1-p) — see
    https://docs.polymarket.us/concepts/orders. So again, "buy NO" cost is
    the complement of the best YES bid, not a separate ask.
That symmetry means the same walk/fee logic applies to both venues' Book
objects unchanged.

Sizing walks each leg's full depth (Book.walk_asks/walk_bids), not just the
top level — `pair.max_contracts` can exceed what's resting at the best
price, in which case the trade sweeps into worse levels on that leg, and
fees are computed per fill (see fees.py) rather than once on a single
blended price, since both venues charge per fill.

Three independent gates apply before an opportunity is usable for real
trading (see `check()`): the pair's own `min_edge`, a plausibility ceiling
(`MAX_PLAUSIBLE_NET_EDGE`) that refuses an implausibly large edge as a
likely bad tick or data glitch rather than real free money, and a book-
freshness check (`max_book_age`) that refuses to act on a stale-looking
book even if the math says there's an edge. None of these gates apply to
`check_all_opportunities()`, which exists for instrumentation — see
instrumentation.py — and deliberately wants to see edges the trading path
would refuse to act on.

Note there's no equivalent plausibility cap in maker_pricing.py for the
market-maker strategy, deliberately: max_safe_quote_price() always SOLVES
for min_edge given whatever hedge_cost currently is, rather than observing
an edge that a bad tick could inflate. A glitched hedge_cost shifts where
it quotes, not how large an edge that quote implies -- so a cap here isn't
redundant the way it would be there.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

from fees import kalshi_taker_fees_for_fills, polymarket_taker_fees_for_fills
from market_pairs import MarketPair
from orderbook_feed import Book, BookStore

log = logging.getLogger("arb")

DEFAULT_MAX_BOOK_AGE = 0.5  # seconds; see ArbDetector's max_book_age

# A genuine cross-venue arb on two reasonably efficient markets should be a
# few cents at most -- both venues observed staying within a couple cents
# of each other all session. An edge far beyond that is much more likely a
# bad tick / stale cache / decimal-place error than real free money.
MAX_PLAUSIBLE_NET_EDGE = Decimal("0.15")

# direction -> (venue buying YES, venue buying NO)
DIRECTIONS: dict[str, tuple[str, str]] = {
    "kalshi_yes+poly_no": ("kalshi", "polymarket_us"),
    "poly_yes+kalshi_no": ("polymarket_us", "kalshi"),
}


@dataclass
class ArbOpportunity:
    pair: MarketPair
    direction: str  # "kalshi_yes+poly_no" or "poly_yes+kalshi_no"
    buy_a_venue: str
    buy_a_price: Decimal  # volume-weighted average cost across whatever levels were walked
    buy_b_venue: str
    buy_b_price: Decimal
    max_contracts: int  # actual sized contracts: min(depth on each leg, pair.max_contracts)
    gross_edge: Decimal  # per contract, before fees
    fees_per_contract: Decimal
    net_edge: Decimal  # per contract, after fees
    fills_a: list[tuple[Decimal, Decimal]]  # (price, qty) levels actually walked on leg A
    fills_b: list[tuple[Decimal, Decimal]]

    def __str__(self) -> str:
        return (
            f"ARB [{self.pair.name}] {self.direction}: "
            f"buy {self.buy_a_venue}@{self.buy_a_price:.4f} + "
            f"buy {self.buy_b_venue}@{self.buy_b_price:.4f} "
            f"= {self.buy_a_price + self.buy_b_price:.4f}/contract (VWAP), "
            f"net edge {self.net_edge:.4f} x {self.max_contracts} contracts "
            f"= ${(self.net_edge * self.max_contracts):.2f}"
        )


def _yes_depth(book: Book) -> Decimal:
    return sum((s for s in book.asks.values() if s > 0), Decimal(0))


def _no_depth(book: Book) -> Decimal:
    return sum((s for s in book.bids.values() if s > 0), Decimal(0))


def _walk_yes(book: Book, target: int) -> list[tuple[Decimal, Decimal]]:
    """Fills to buy YES: straight off the ask side, cheapest first."""
    return book.walk_asks(Decimal(target))


def _walk_no(book: Book, target: int) -> list[tuple[Decimal, Decimal]]:
    """Fills to buy NO: the complement of the best-first bid walk (crossing
    the YES bid directly on Kalshi; via ORDER_INTENT_BUY_SHORT on Polymarket
    US, which the platform executes the same way under the hood)."""
    return [(Decimal(1) - price, qty) for price, qty in book.walk_bids(Decimal(target))]


def _fees_for(venue: str, fills: list[tuple[Decimal, Decimal]]) -> Decimal:
    if venue == "kalshi":
        return kalshi_taker_fees_for_fills(fills)
    return polymarket_taker_fees_for_fills(fills)


class ArbDetector:
    """
    Evaluates MarketPairs against the current BookStore state.

    `check()` is the trading-facing entry point: applies both `pair.min_edge`
    and the `max_book_age` freshness guard, returns the better direction if
    either clears both. `check_all_opportunities()` and `evaluate_direction()`
    exist for instrumentation and bypass both gates deliberately — see
    instrumentation.py.
    """

    def __init__(
        self,
        store: BookStore,
        pairs: list[MarketPair],
        on_opportunity: Callable[[ArbOpportunity], None],
        max_book_age: float = DEFAULT_MAX_BOOK_AGE,
    ):
        self.store = store
        self.pairs = pairs
        self.on_opportunity = on_opportunity
        self.max_book_age = max_book_age

    def check_all(self) -> None:
        for pair in self.pairs:
            opp = self.check(pair)
            if opp is not None:
                self.on_opportunity(opp)

    def check(self, pair: MarketPair) -> ArbOpportunity | None:
        """The trading decision: best opportunity that clears both
        pair.min_edge and the book-freshness guard, or None."""
        best: ArbOpportunity | None = None
        for direction in DIRECTIONS:
            opp = self.evaluate_direction(pair, direction)
            if opp is None or opp.net_edge < pair.min_edge:
                continue
            if opp.net_edge > MAX_PLAUSIBLE_NET_EDGE:
                log.warning(
                    "%s: %s net edge %.4f exceeds the plausible cap (%.4f) -- treating as a likely bad "
                    "tick/data glitch rather than a real opportunity, refusing to act",
                    pair.name, direction, opp.net_edge, MAX_PLAUSIBLE_NET_EDGE,
                )
                continue
            if not self._books_fresh(pair):
                log.warning(
                    "%s: %s clears min_edge but a book is stale (>%.3fs old) — refusing to act",
                    pair.name, direction, self.max_book_age,
                )
                continue
            if best is None or opp.net_edge > best.net_edge:
                best = opp
        return best

    def check_all_opportunities(self, pair: MarketPair, threshold: Decimal) -> list[ArbOpportunity]:
        """Every direction whose net edge clears `threshold`, ignoring
        pair.min_edge and the freshness guard. For instrumentation only —
        never wire this to an executor."""
        results = []
        for direction in DIRECTIONS:
            opp = self.evaluate_direction(pair, direction)
            if opp is not None and opp.net_edge >= threshold:
                results.append(opp)
        return results

    def evaluate_direction(self, pair: MarketPair, direction: str) -> ArbOpportunity | None:
        """Re-evaluate one specific direction fresh against current books.
        No threshold or freshness filtering — pure math. Used by check(),
        check_all_opportunities(), and instrumentation's resampling (which
        needs to re-check the SAME direction over time, not whichever
        direction happens to be best later)."""
        books = self._books(pair)
        venue_a, venue_b = DIRECTIONS[direction]
        book_a, book_b = books.get(venue_a), books.get(venue_b)
        if not book_a or not book_b:
            return None
        return self._evaluate(pair, direction, venue_a, book_a, venue_b, book_b)

    def _books(self, pair: MarketPair) -> dict[str, Book | None]:
        return {
            "kalshi": self.store.books.get(("kalshi", pair.kalshi_ticker)),
            "polymarket_us": self.store.books.get(("polymarket_us", pair.poly_market_slug)),
        }

    def _books_fresh(self, pair: MarketPair) -> bool:
        now = time.time()
        return all(
            book is not None and (now - book.updated_at) <= self.max_book_age
            for book in self._books(pair).values()
        )

    def _evaluate(
        self, pair: MarketPair, direction: str, venue_a: str, book_a: Book, venue_b: str, book_b: Book,
    ) -> ArbOpportunity | None:
        # venue_a always buys YES, venue_b always buys NO, per DIRECTIONS.
        depth_a = _yes_depth(book_a)
        depth_b = _no_depth(book_b)
        contracts = int(min(depth_a, depth_b, Decimal(pair.max_contracts)))
        if contracts <= 0:
            return None

        fills_a = _walk_yes(book_a, contracts)
        fills_b = _walk_no(book_b, contracts)
        if not fills_a or not fills_b:
            return None

        total_cost_a = sum((p * q for p, q in fills_a), Decimal(0))
        total_cost_b = sum((p * q for p, q in fills_b), Decimal(0))
        gross_profit = Decimal(contracts) - total_cost_a - total_cost_b
        if gross_profit <= 0:
            return None

        fee_a = _fees_for(venue_a, fills_a)
        fee_b = _fees_for(venue_b, fills_b)
        net_profit = gross_profit - fee_a - fee_b

        gross_edge = (gross_profit / contracts).quantize(Decimal("0.0001"))
        fees_per_contract = ((fee_a + fee_b) / contracts).quantize(Decimal("0.0001"))
        net_edge = (net_profit / contracts).quantize(Decimal("0.0001"))

        return ArbOpportunity(
            pair=pair,
            direction=direction,
            buy_a_venue=venue_a,
            buy_a_price=(total_cost_a / contracts).quantize(Decimal("0.0001")),
            buy_b_venue=venue_b,
            buy_b_price=(total_cost_b / contracts).quantize(Decimal("0.0001")),
            max_contracts=contracts,
            gross_edge=gross_edge,
            fees_per_contract=fees_per_contract,
            net_edge=net_edge,
            fills_a=fills_a,
            fills_b=fills_b,
        )
