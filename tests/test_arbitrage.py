"""
Unit tests for ArbDetector.check() / check_all_opportunities() / evaluate_direction().
Expected values were computed by running the code and hand-verifying the
arithmetic (see fees.py's formulas), not guessed.
"""

import sys
import time
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from arbitrage import MAX_PLAUSIBLE_NET_EDGE, ArbDetector
from market_pairs import MarketPair
from orderbook_feed import BookStore

D = Decimal


def _fresh_pair(name="test", **kwargs) -> tuple[BookStore, MarketPair]:
    store = BookStore()
    defaults = dict(kalshi_ticker=f"KX-{name}", poly_market_slug=f"poly-{name}", min_edge=D("0.02"), max_contracts=50)
    defaults.update(kwargs)
    pair = MarketPair(name=name, **defaults)
    return store, pair


def _touch(book, bids: dict, asks: dict) -> None:
    """Populate a book and mark it fresh, as a real feed would via publish()."""
    book.bids.update(bids)
    book.asks.update(asks)
    book.updated_at = time.time()


class TestWorkedExample(unittest.TestCase):
    """The exact scenario from the README / the Task 1 conversation."""

    def setUp(self):
        self.store, self.pair = _fresh_pair(min_edge=D("0.0"))
        kalshi_book = self.store.get("kalshi", self.pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.44"): D("120")}, asks={D("0.47"): D("75")})
        poly_book = self.store.get("polymarket_us", self.pair.poly_market_slug)
        _touch(poly_book, bids={D("0.52"): D("60")}, asks={D("0.55"): D("40")})
        self.detector = ArbDetector(self.store, [self.pair], lambda o: None)

    def test_direction_and_sizing(self):
        opp = self.detector.check(self.pair)
        self.assertIsNotNone(opp)
        self.assertEqual(opp.direction, "kalshi_yes+poly_no")
        self.assertEqual(opp.max_contracts, 50)  # capped by pair.max_contracts, not the deeper book

    def test_exact_edge_numbers(self):
        opp = self.detector.check(self.pair)
        self.assertEqual(opp.gross_edge, D("0.0500"))
        # Corrected 2026-09-23 for the real 0.0695 Polymarket taker rate
        # (was 0.06 -- confirmed stale against docs.polymarket.us/fees, see
        # fees.py's module docstring). fee_a = kalshi_taker_fee(0.47,50) =
        # 0.88; fee_b = polymarket_taker_fee(1-0.52,50) = polymarket_taker_
        # fee(0.48,50) = 0.87 -> fees_per_contract = (0.88+0.87)/50 = 0.0350
        self.assertEqual(opp.fees_per_contract, D("0.0350"))
        self.assertEqual(opp.net_edge, D("0.0150"))

    def test_reverse_direction_has_no_edge(self):
        # buy YES poly (0.55) + buy NO kalshi (1-0.44=0.56) = 1.11 > 1.00
        opp = self.detector.evaluate_direction(self.pair, "poly_yes+kalshi_no")
        self.assertIsNone(opp)


class TestMultiLevelFill(unittest.TestCase):
    """max_contracts exceeds top-of-book depth on one leg, forcing a sweep
    into a second price level -- exercises per-fill fee rounding on both
    venues within a single opportunity."""

    def setUp(self):
        self.store, self.pair = _fresh_pair(name="multi", min_edge=D("0.0"), max_contracts=30)
        kalshi_book = self.store.get("kalshi", self.pair.kalshi_ticker)
        # Only 10 available at the best ask; the remaining 20 needed comes
        # from the next level up.
        _touch(kalshi_book, bids={D("0.30"): D("100")}, asks={D("0.40"): D("10"), D("0.42"): D("40")})
        poly_book = self.store.get("polymarket_us", self.pair.poly_market_slug)
        _touch(poly_book, bids={D("0.55"): D("100")}, asks={D("0.90"): D("10")})
        self.detector = ArbDetector(self.store, [self.pair], lambda o: None)

    def test_walks_both_kalshi_ask_levels(self):
        opp = self.detector.check(self.pair)
        self.assertIsNotNone(opp)
        self.assertEqual(opp.fills_a, [(D("0.40"), D("10")), (D("0.42"), D("20"))])
        self.assertEqual(opp.fills_b, [(D("0.45"), D("30"))])  # 1 - 0.55 = 0.45, one level covers it

    def test_exact_edge_numbers(self):
        opp = self.detector.check(self.pair)
        # cost_a = 0.40*10 + 0.42*20 = 12.40 ; cost_b = 0.45*30 = 13.50
        # gross_profit = 30 - 12.40 - 13.50 = 4.10 -> gross_edge = 4.10/30 = 0.1367 (rounded)
        self.assertEqual(opp.gross_edge, D("0.1367"))
        # Corrected 2026-09-23 for the real 0.0695 Polymarket taker rate
        # (was 0.06 -- confirmed stale against docs.polymarket.us/fees, see
        # fees.py's module docstring).
        # fee_a = kalshi_taker_fee(0.40,10) + kalshi_taker_fee(0.42,20) = 0.17 + 0.35 = 0.52
        # fee_b = polymarket_taker_fee(0.45,30) = 0.52
        # fees_per_contract = (0.52+0.52)/30 = 0.0347 (rounded)
        self.assertEqual(opp.fees_per_contract, D("0.0347"))
        # net_profit = 4.10 - 0.52 - 0.52 = 3.06 -> net_edge = 3.06/30 = 0.1020 (rounded)
        self.assertEqual(opp.net_edge, D("0.1020"))


class TestThresholdDecoupling(unittest.TestCase):
    """check() enforces pair.min_edge; check_all_opportunities() doesn't --
    this is what lets instrumentation see edges the trading path ignores."""

    def setUp(self):
        # net edge here is small (see TestWorkedExample) -- 0.0174. Set
        # min_edge above that so check() should refuse it, but it should
        # still show up via check_all_opportunities(threshold=0.0).
        self.store, self.pair = _fresh_pair(name="threshold", min_edge=D("0.05"))
        kalshi_book = self.store.get("kalshi", self.pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.44"): D("120")}, asks={D("0.47"): D("75")})
        poly_book = self.store.get("polymarket_us", self.pair.poly_market_slug)
        _touch(poly_book, bids={D("0.52"): D("60")}, asks={D("0.55"): D("40")})
        self.detector = ArbDetector(self.store, [self.pair], lambda o: None)

    def test_check_refuses_below_min_edge(self):
        self.assertIsNone(self.detector.check(self.pair))

    def test_check_all_opportunities_ignores_min_edge(self):
        opps = self.detector.check_all_opportunities(self.pair, D("0.0"))
        directions = {o.direction for o in opps}
        self.assertIn("kalshi_yes+poly_no", directions)


class TestStalenessGuard(unittest.TestCase):
    """check() refuses to act on a stale book even when the math otherwise
    qualifies; the ungated methods don't apply this guard at all."""

    def setUp(self):
        self.store, self.pair = _fresh_pair(name="stale", min_edge=D("0.0"))
        kalshi_book = self.store.get("kalshi", self.pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.44"): D("120")}, asks={D("0.47"): D("75")})
        poly_book = self.store.get("polymarket_us", self.pair.poly_market_slug)
        _touch(poly_book, bids={D("0.52"): D("60")}, asks={D("0.55"): D("40")})
        # Backdate one book well past the default 0.5s staleness threshold.
        kalshi_book.updated_at = time.time() - 10.0
        self.detector = ArbDetector(self.store, [self.pair], lambda o: None)

    def test_check_refuses_stale_book(self):
        self.assertIsNone(self.detector.check(self.pair))

    def test_evaluate_direction_ignores_staleness(self):
        opp = self.detector.evaluate_direction(self.pair, "kalshi_yes+poly_no")
        self.assertIsNotNone(opp)

    def test_check_all_opportunities_ignores_staleness(self):
        opps = self.detector.check_all_opportunities(self.pair, D("0.0"))
        self.assertTrue(any(o.direction == "kalshi_yes+poly_no" for o in opps))

    def test_freshening_the_book_lets_check_succeed_again(self):
        self.store.books[("kalshi", self.pair.kalshi_ticker)].updated_at = time.time()
        self.assertIsNotNone(self.detector.check(self.pair))


class TestPlausibilityCap(unittest.TestCase):
    """Guard added 2026-09-15: an edge far beyond what two reasonably
    efficient venues would ever leave on the table is more likely a bad
    tick or data glitch than real free money. check() must refuse to act
    on it; the ungated methods must still report it (instrumentation
    deliberately wants to see this)."""

    def setUp(self):
        self.store, self.pair = _fresh_pair(name="glitch", min_edge=D("0.0"))
        kalshi_book = self.store.get("kalshi", self.pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.28"): D("100")}, asks={D("0.30"): D("100")})
        poly_book = self.store.get("polymarket_us", self.pair.poly_market_slug)
        _touch(poly_book, bids={D("0.50"): D("100")}, asks={D("0.55"): D("100")})
        self.detector = ArbDetector(self.store, [self.pair], lambda o: None)

    def test_evaluate_direction_reports_the_implausible_edge(self):
        opp = self.detector.evaluate_direction(self.pair, "kalshi_yes+poly_no")
        self.assertIsNotNone(opp)
        # Corrected 2026-09-23 for the real 0.0695 Polymarket taker rate
        # (was 0.06 -- confirmed stale against docs.polymarket.us/fees).
        self.assertEqual(opp.net_edge, D("0.1678"))
        self.assertGreater(opp.net_edge, MAX_PLAUSIBLE_NET_EDGE)

    def test_check_refuses_to_act_on_it(self):
        self.assertIsNone(self.detector.check(self.pair))

    def test_check_all_opportunities_still_reports_it(self):
        opps = self.detector.check_all_opportunities(self.pair, D("0.0"))
        self.assertTrue(any(o.direction == "kalshi_yes+poly_no" for o in opps))

    def test_edge_just_under_the_cap_is_not_blocked(self):
        # Sanity check the cap isn't overly aggressive against a real,
        # merely-generous opportunity.
        store, pair = _fresh_pair(name="generous", min_edge=D("0.0"))
        kalshi_book = store.get("kalshi", pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.40"): D("100")}, asks={D("0.42"): D("100")})
        poly_book = store.get("polymarket_us", pair.poly_market_slug)
        _touch(poly_book, bids={D("0.50"): D("100")}, asks={D("0.55"): D("100")})
        detector = ArbDetector(store, [pair], lambda o: None)
        opp = detector.check(pair)
        self.assertIsNotNone(opp)
        self.assertLess(opp.net_edge, MAX_PLAUSIBLE_NET_EDGE)


class TestNoArbitrage(unittest.TestCase):
    def test_no_opportunity_when_costs_exceed_one_dollar(self):
        store, pair = _fresh_pair(name="noarb", min_edge=D("0.0"))
        kalshi_book = store.get("kalshi", pair.kalshi_ticker)
        _touch(kalshi_book, bids={D("0.30"): D("50")}, asks={D("0.60"): D("50")})
        poly_book = store.get("polymarket_us", pair.poly_market_slug)
        _touch(poly_book, bids={D("0.30"): D("50")}, asks={D("0.60"): D("50")})
        detector = ArbDetector(store, [pair], lambda o: None)
        self.assertIsNone(detector.check(pair))
        self.assertEqual(detector.check_all_opportunities(pair, D("0.0")), [])


if __name__ == "__main__":
    unittest.main()
