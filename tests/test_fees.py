"""
Unit tests for fees.py. Every expected value here was computed by first
running the function under test and hand-verifying the arithmetic by hand
(see the module docstrings in fees.py for the formulas) — not guessed,
and not asserted as a range.
"""

import sys
import unittest
from unittest import mock
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fees import (
    kalshi_maker_fee,
    kalshi_taker_fee,
    kalshi_taker_fees_for_fills,
    polymarket_maker_rebate,
    polymarket_round_trip_cost_rate,
    polymarket_taker_fee,
    polymarket_taker_fees_for_fills,
)

D = Decimal


class TestSingleFillFees(unittest.TestCase):
    def test_kalshi_taker_worked_example(self):
        # raw = 0.07*50*0.47*0.53 = 0.87185 -> 87.185c -> round UP -> 88c
        self.assertEqual(kalshi_taker_fee(D("0.47"), 50), D("0.88"))

    def test_polymarket_taker_worked_example(self):
        # raw = 0.0695*50*0.48*0.52 = 0.86736 -> 86.736c -> round half-even -> 87c
        self.assertEqual(polymarket_taker_fee(D("0.48"), 50), D("0.87"))

    def test_kalshi_taker_midprice(self):
        # peak of the fee curve: raw = 0.07*1*0.5*0.5 = 0.0175 -> 1.75c -> UP -> 2c
        self.assertEqual(kalshi_taker_fee(D("0.50"), 1), D("0.02"))

    def test_polymarket_taker_midprice_rounds_up(self):
        # raw = 0.0695*1*0.5*0.5 = 0.017375 -> 1.7375c -> not a tie -> rounds up to 2c
        self.assertEqual(polymarket_taker_fee(D("0.50"), 1), D("0.02"))

    def test_polymarket_taker_midprice_rounds_down(self):
        # raw = 0.0695*3*0.5*0.5 = 0.052125 -> 5.2125c -> not a tie -> rounds down to 5c
        self.assertEqual(polymarket_taker_fee(D("0.50"), 3), D("0.05"))

    def test_kalshi_taker_tail(self):
        # raw = 0.07*13*0.1*0.9 = 0.0819 -> 8.19c -> UP -> 9c
        self.assertEqual(kalshi_taker_fee(D("0.10"), 13), D("0.09"))

    def test_polymarket_taker_tail(self):
        # raw = 0.0695*13*0.1*0.9 = 0.081315 -> 8.1315c -> half-even -> 8c (not a tie)
        self.assertEqual(polymarket_taker_fee(D("0.10"), 13), D("0.08"))

    def test_kalshi_maker_uses_lower_coefficient(self):
        # raw = 0.0175*100*0.5*0.5 = 0.4375 -> 43.75c -> UP -> 44c
        self.assertEqual(kalshi_maker_fee(D("0.50"), 100), D("0.44"))

    @mock.patch("fees.POLYMARKET_MAKER_REBATE_RATE", Decimal("0.0125"))  # the pre-2026-10-28 rate these cases were worked at
    def test_polymarket_maker_rebate(self):
        # raw = 0.0125*100*0.5*0.5 = 0.3125 -> 31.25c -> half-even -> 31c (not a tie)
        self.assertEqual(polymarket_maker_rebate(D("0.50"), 100), D("0.31"))

    def test_zero_or_negative_contracts_is_zero_fee(self):
        self.assertEqual(kalshi_taker_fee(D("0.50"), 0), D("0"))
        self.assertEqual(polymarket_taker_fee(D("0.50"), 0), D("0"))


class TestPerOrderFees(unittest.TestCase):
    def test_kalshi_multi_fill_is_plain_sum_no_cap(self):
        # kalshi_taker_fee(0.47,30) = 0.53 ; kalshi_taker_fee(0.50,20) = 0.35
        fills = [(D("0.47"), D(30)), (D("0.50"), D(20))]
        self.assertEqual(kalshi_taker_fees_for_fills(fills), D("0.53") + D("0.35"))
        self.assertEqual(kalshi_taker_fees_for_fills(fills), D("0.88"))

    def test_polymarket_cumulative_cap_reduces_total(self):
        # Two fills of 1 contract each at 0.50: each fill's exact fee is
        # 1.7375c, and naive per-fill rounding sends BOTH to 2c (not a tie,
        # just a normal round-up), for a naive sum of 4c. But the true
        # cumulative exact fee is 3.475c, which rounds DOWN to 3c, so the
        # cap knocks the second fill's charge down from 2c to 1c, landing
        # on 3c total -- not 4c.
        fills = [(D("0.5"), D(1)), (D("0.5"), D(1))]
        naive_sum = polymarket_taker_fee(D("0.5"), 1) * 2
        self.assertEqual(naive_sum, D("0.04"))  # what you'd get without the cap
        self.assertEqual(polymarket_taker_fees_for_fills(fills), D("0.03"))

    def test_polymarket_cumulative_cap_is_a_noop_when_not_needed(self):
        # Two identical fills whose naive per-fill rounding already equals
        # the rounded cumulative exact total -- the cap changes nothing.
        fills = [(D("0.1"), D(13)), (D("0.1"), D(13))]
        naive_sum = polymarket_taker_fee(D("0.1"), 13) * 2
        self.assertEqual(naive_sum, D("0.16"))
        self.assertEqual(polymarket_taker_fees_for_fills(fills), D("0.16"))

    def test_empty_fills_is_zero(self):
        self.assertEqual(kalshi_taker_fees_for_fills([]), D("0.00"))
        self.assertEqual(polymarket_taker_fees_for_fills([]), D("0.00"))


class TestPolymarketRoundTripCostRate(unittest.TestCase):
    @mock.patch("fees.POLYMARKET_MAKER_REBATE_RATE", Decimal("0.0125"))  # the pre-2026-10-28 rate these cases were worked at
    def test_taker_exit_worked_example_at_midprice(self):
        # entry (maker): -0.0125*0.5*0.5 = -0.003125
        # exit (taker):   +0.0695*0.5*0.5 =  0.017375
        # total = 0.01425
        self.assertEqual(polymarket_round_trip_cost_rate(D("0.5"), D("0.5"), exit_is_taker=True), D("0.01425"))

    @mock.patch("fees.POLYMARKET_MAKER_REBATE_RATE", Decimal("0.0125"))  # the pre-2026-10-28 rate these cases were worked at
    def test_maker_exit_is_a_net_credit_at_midprice(self):
        # entry (maker): -0.003125 ; exit (maker): -0.003125 -> total -0.00625 (net rebate, real revenue)
        self.assertEqual(polymarket_round_trip_cost_rate(D("0.5"), D("0.5"), exit_is_taker=False), D("-0.00625"))

    @mock.patch("fees.POLYMARKET_MAKER_REBATE_RATE", Decimal("0.0125"))  # the pre-2026-10-28 rate these cases were worked at
    def test_taker_exit_at_band_edge_worked_example(self):
        # p(1-p) at 0.15 = 0.1275
        # entry (maker): -0.0125*0.1275 = -0.00159375
        # exit (taker):   0.0695*0.1275 =  0.00886125
        # total = 0.0072675
        self.assertEqual(polymarket_round_trip_cost_rate(D("0.15"), D("0.15"), exit_is_taker=True), D("0.0072675"))

    @mock.patch("fees.POLYMARKET_MAKER_REBATE_RATE", Decimal("0.0125"))  # the pre-2026-10-28 rate these cases were worked at
    def test_different_entry_and_exit_prices(self):
        # entry at 0.40 (maker): -0.0125*0.4*0.6 = -0.003
        # exit at 0.60 (taker):   0.0695*0.6*0.4 =  0.01668
        # total = 0.01368
        self.assertEqual(polymarket_round_trip_cost_rate(D("0.40"), D("0.60"), exit_is_taker=True), D("0.01368"))

    def test_taker_exit_always_costs_more_than_maker_exit_same_prices(self):
        taker = polymarket_round_trip_cost_rate(D("0.3"), D("0.7"), exit_is_taker=True)
        maker = polymarket_round_trip_cost_rate(D("0.3"), D("0.7"), exit_is_taker=False)
        self.assertGreater(taker, maker)


if __name__ == "__main__":
    unittest.main()
