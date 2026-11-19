"""
Baseline order-flow metrics (order_metrics.py, "Item 0", added 2026-09-28)
and post round-trip timing (audit item #21).
"""

import sys
import unittest
import unittest.mock
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from order_metrics import MARKOUT_HORIZONS, OrderMetrics, quantile

D = Decimal


class TestCountsAndRates(unittest.TestCase):
    def test_per_hour_rates_and_cancel_to_fill(self):
        m = OrderMetrics(started_at=0.0)
        for i in range(6):
            m.on_post(f"o{i}", None, 2, now=1.0)
        for i in range(4):
            m.on_cancel(f"o{i}", "FV_OUT_OF_BAND", now=2.0)
        m.on_fill("o4", "yes", D("2"), D("0.45"), D("0.50"), now=3.0)
        m.on_fill("o5", "no", D("1"), D("0.45"), D("0.50"), now=3.0)
        snap = m.snapshot(now=1800.0)  # half an hour
        self.assertEqual((snap["posts_per_h"], snap["cancels_per_h"], snap["fills_per_h"]), (12.0, 8.0, 4.0))
        self.assertEqual(snap["cancel_to_fill"], 2.0)
        self.assertEqual(snap["cancels_by_reason"], {"FV_OUT_OF_BAND": 4})
        self.assertEqual(snap["fill_contracts"], "3")

    def test_no_fills_means_no_ratio(self):
        m = OrderMetrics(started_at=0.0)
        m.on_post("a", None, 1, now=0.0)
        m.on_cancel("a", "STALE_FEED", now=1.0)
        self.assertIsNone(m.snapshot(now=10.0)["cancel_to_fill"])


class TestRestingTime(unittest.TestCase):
    def test_one_sample_per_order_at_its_first_cancel_or_fill(self):
        m = OrderMetrics(started_at=0.0)
        m.on_post("a", None, 2, now=0.0)
        m.on_post("b", None, 2, now=0.0)
        m.on_post("c", None, 2, now=0.0)
        m.on_fill("a", "yes", D("1"), D("0.4"), D("0.5"), now=1.0)
        m.on_fill("a", "yes", D("1"), D("0.4"), D("0.5"), now=5.0)  # second partial: no new sample
        m.on_cancel("a", "FILL_SIDE_HOLD", now=6.0)  # already ended by its fill
        m.on_cancel("b", "TARGET_IMPROVED", now=2.0)
        m.on_cancel("c", "TARGET_IMPROVED", now=10.0)
        snap = m.snapshot(now=20.0)
        self.assertEqual(snap["rest_to_fill_s"], {"n": 1, "p50": 1.0, "p90": 1.0})
        self.assertEqual(snap["rest_to_cancel_s"], {"n": 2, "p50": 10.0, "p90": 10.0})
        self.assertEqual(snap["rest_s"]["n"], 3)

    def test_quantile_is_nearest_rank(self):
        self.assertEqual(quantile([1, 2, 3, 4], 0.5), 3)
        self.assertEqual(quantile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.9), 10)
        self.assertIsNone(quantile([], 0.5))


class TestPerRungFillRate(unittest.TestCase):
    def test_order_and_contract_fill_rates_per_rung(self):
        m = OrderMetrics(started_at=0.0)
        m.on_post("t1", 0, 2, now=0.0)
        m.on_post("t2", 0, 2, now=0.0)
        m.on_post("d1", 1, 1, now=0.0)
        m.on_post("d2", 2, 1, now=0.0)
        m.on_fill("t1", "yes", D("2"), D("0.45"), D("0.5"), now=1.0)
        m.on_fill("d1", "yes", D("1"), D("0.44"), D("0.5"), now=1.0)
        rungs = m.snapshot(now=10.0)["rungs"]
        self.assertEqual(rungs["0"], {"posts": 2, "filled_orders": 1, "order_fill_rate": 0.5, "contract_fill_rate": 0.5})
        self.assertEqual(rungs["1"]["order_fill_rate"], 1.0)
        self.assertEqual(rungs["2"]["order_fill_rate"], 0.0)


class TestMarkouts(unittest.TestCase):
    def test_signed_from_our_side_at_each_horizon(self):
        m = OrderMetrics(started_at=0.0)
        m.on_post("y", None, 2, now=0.0)
        m.on_post("n", None, 2, now=0.0)
        m.on_fill("y", "yes", D("2"), D("0.45"), D("0.50"), now=100.0)  # bought yes at 0.45
        m.on_fill("n", "no", D("2"), D("0.45"), D("0.50"), now=100.0)  # sold yes at 0.55
        m.on_mid(D("0.52"), now=104.0)
        self.assertIsNone(m.snapshot(now=104.0)["markouts"]["5s"])  # not due yet
        m.on_mid(D("0.52"), now=105.1)
        snap = m.snapshot(now=105.1)["markouts"]["5s"]
        # yes: mid +2c (good), vs fill 0.52 - 0.45 = +7c; no: mid +2c is bad (-2c), vs fill 0.55 - 0.52 = +3c
        self.assertEqual(snap, {"n": 2, "mid_move_c": 0.0, "vs_fill_c": 5.0, "vs_fill_total_c": 20.0})  # 7x2 + 3x2
        m.on_mid(D("0.40"), now=130.0)
        m.on_mid(D("0.40"), now=220.0)
        snap = m.snapshot(now=220.0)
        self.assertEqual(snap["markouts"]["30s"], {"n": 2, "mid_move_c": 0.0, "vs_fill_c": 5.0, "vs_fill_total_c": 20.0})
        self.assertEqual(snap["markouts_pending"], 0)
        self.assertEqual(len(MARKOUT_HORIZONS), 3)


class TestSamePriceReposts(unittest.TestCase):
    def test_counts_posts_matching_a_cancel_in_the_same_batch(self):
        m = OrderMetrics()
        m.on_batch([("yes", D("0.45")), ("yes", D("0.44")), ("no", D("0.40"))],
                   [("yes", D("0.45")), ("no", D("0.41")), ("yes", D("0.45"))])
        self.assertEqual(m.same_price_reposts, 1)


if __name__ == "__main__":
    unittest.main()
