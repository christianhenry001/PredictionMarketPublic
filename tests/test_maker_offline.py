"""Offline maker test harness: the queue models, the policies, and the scoring."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from maker_offline.fills import CONSERVATIVE, OPTIMISTIC, FillEngine  # noqa: E402
from maker_offline.offline_fees import MAKER_REBATE, taker_fee  # noqa: E402
from maker_offline.tape import Book, Game, Trade, infer_tick  # noqa: E402


def book(t, bids, asks, state="MARKET_STATE_OPEN"):
    return Book(t, dict(bids), dict(asks), state)


def engine(model=CONSERVATIVE, latency=0.0):
    fills = []
    e = FillEngine(model, fills.append, latency, latency)
    e.on_book(book(0.0, {0.50: 100, 0.49: 50}, {0.52: 80}))
    return e, fills


def sell(t, price, qty):
    return Trade(t, price, qty, True)


class QueueModelTests(unittest.TestCase):
    def test_conservative_fills_only_after_the_queue_ahead_trades(self):
        e, fills = engine()
        o = e.post(0.0, "bid", 0.50, 5)
        e.advance(0.0)
        e.on_trade(sell(1, 0.50, 60))
        self.assertEqual(fills, [])
        e.on_trade(sell(2, 0.50, 42))
        self.assertEqual(fills[0].size, 2)
        self.assertEqual(o.left, 3)

    def test_optimistic_fills_at_the_front(self):
        e, fills = engine(OPTIMISTIC)
        e.post(0.0, "bid", 0.50, 5)
        e.advance(0.0)
        e.on_trade(sell(1, 0.50, 3))
        self.assertEqual(fills[0].size, 3)

    def test_a_trade_through_our_price_fills_in_full(self):
        e, fills = engine()
        e.post(0.0, "bid", 0.50, 5)
        e.advance(0.0)
        e.on_trade(sell(1, 0.49, 1))
        self.assertEqual(fills[0].size, 5)
        self.assertTrue(fills[0].through)

    def test_a_shrinking_level_moves_us_up_only_below_our_estimate(self):
        e, _ = engine()
        o = e.post(0.0, "bid", 0.50, 5)
        e.advance(0.0)
        self.assertEqual(o.ahead, 100)
        e.on_book(book(1, {0.50: 150, 0.49: 50}, {0.52: 80}))
        self.assertEqual(o.ahead, 100)
        e.on_book(book(2, {0.50: 30, 0.49: 50}, {0.52: 80}))
        self.assertEqual(o.ahead, 30)

    def test_a_vanished_level_clears_the_queue(self):
        e, _ = engine()
        o = e.post(0.0, "bid", 0.50, 5)
        e.advance(0.0)
        e.on_book(book(1, {0.49: 50, 0.48: 10}, {0.52: 80}))
        self.assertEqual(o.ahead, 0)

    def test_an_order_past_the_recorded_levels_is_flagged(self):
        e, _ = engine()
        o = e.post(0.0, "bid", 0.40, 5)
        e.advance(0.0)
        self.assertFalse(o.queue_known)
        self.assertEqual(o.ahead, 50)

    def test_a_new_level_inside_the_range_has_no_queue(self):
        e, _ = engine()
        o = e.post(0.0, "bid", 0.51, 5)
        e.advance(0.0)
        self.assertTrue(o.queue_known)
        self.assertEqual(o.ahead, 0)

    def test_a_crossing_post_is_rejected(self):
        e, _ = engine()
        e.post(0.0, "bid", 0.52, 5)
        e.advance(0.0)
        self.assertEqual(e.counters.rejected, 1)
        self.assertEqual(e.orders, {})

    def test_a_cancel_can_lose_the_race(self):
        e, fills = engine(OPTIMISTIC, latency=0.028)
        o = e.post(0.0, "bid", 0.50, 5)
        e.advance(0.1)
        e.cancel(1.0, o.id)
        e.on_trade(sell(1.01, 0.50, 5))
        self.assertEqual(len(fills), 1)
        self.assertEqual(e.counters.lost_cancel_races, 1)

    def test_a_cancel_that_arrived_in_time_avoids_the_fill(self):
        e, fills = engine(OPTIMISTIC, latency=0.028)
        o = e.post(0.0, "bid", 0.50, 5)
        e.advance(0.1)
        e.cancel(1.0, o.id)
        e.on_trade(sell(1.05, 0.50, 5))
        self.assertEqual(fills, [])

    def test_one_trade_is_used_up_across_orders(self):
        e, fills = engine(OPTIMISTIC)
        e.post(0.0, "bid", 0.50, 2)
        e.post(0.0, "bid", 0.49, 2)
        e.advance(0.0)
        e.on_trade(sell(1, 0.49, 3))
        self.assertEqual([f.size for f in fills], [2, 1])

    def test_taker_buy_hits_our_ask(self):
        e, fills = engine()
        e.post(0.0, "ask", 0.52, 1)
        e.advance(0.0)
        e.on_trade(Trade(1, 0.52, 81, False))
        self.assertEqual(fills[0].side, "ask")


def game(events, start=None, tick=0.01):
    return Game("aec-nba-a-b-2026-11-05", "NBA", "full", start, tick, events, None)


TOUCH = ({0.50: 100, 0.49: 50}, {0.52: 80, 0.53: 40})


class FeeTests(unittest.TestCase):
    def test_no_maker_rebate(self):
        self.assertEqual(MAKER_REBATE, 0.0)

    def test_taker_fee_matches_our_own_fill(self):
        self.assertEqual(taker_fee(742.53, 0.044), 2.17)  # our 2026-11-03 execution

    def test_rounding(self):
        self.assertEqual(taker_fee(100, 0.5), 1.74)
        self.assertEqual(taker_fee(1, 0.5), 0.02)


class TapeTests(unittest.TestCase):
    def test_tick(self):
        self.assertEqual(infer_tick([book(0, {0.50: 1, 0.49: 1}, {0.51: 1})]), 0.01)
        self.assertEqual(infer_tick([book(0, {0.455: 1}, {0.46: 1})]), 0.005)
        self.assertEqual(infer_tick([book(0, {0.044: 1}, {0.046: 1})]), 0.001)


if __name__ == "__main__":
    unittest.main()
