"""
Unit tests for executor.py / maker_executor.py's price-sanity guard, added
2026-09-15 as a last line of defense immediately before any order-placing
API call. Each order-placing method's guard is confirmed to fire BEFORE
any network/SDK call would happen, by constructing the client via
__new__() (bypassing __init__, so no real credentials/network setup is
needed) and confirming the ValueError still fires from a bare instance.
"""

import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from executor import KalshiOrderClient, LiveExecutor, PolymarketUSOrderClient, assert_sane_price
from maker_executor import LiveMakerExecutor

D = Decimal


class TestAssertSanePrice(unittest.TestCase):
    def test_valid_prices_pass(self):
        for price in (D("0.01"), D("0.5"), D("0.99"), D("0.0001")):
            assert_sane_price(price, "test")  # must not raise

    def test_zero_is_rejected(self):
        with self.assertRaises(ValueError):
            assert_sane_price(D("0"), "test")

    def test_one_is_rejected(self):
        with self.assertRaises(ValueError):
            assert_sane_price(D("1"), "test")

    def test_negative_is_rejected(self):
        with self.assertRaises(ValueError):
            assert_sane_price(D("-0.01"), "test")

    def test_above_one_is_rejected(self):
        with self.assertRaises(ValueError):
            assert_sane_price(D("1.01"), "test")


class TestGuardFiresBeforeAnyNetworkCall(unittest.TestCase):
    """Each of these constructs the client via __new__() (skipping
    __init__ entirely, so no real auth/SDK/network setup exists) to prove
    the price check happens before self._client or self.auth is ever
    touched -- if it didn't, these would fail with AttributeError instead
    of the expected ValueError."""

    def test_kalshi_place_limit_order(self):
        client = KalshiOrderClient.__new__(KalshiOrderClient)
        with self.assertRaises(ValueError):
            client.place_limit_order("KX-TEST", "yes", "buy", D("1.5"), 10)

    def test_kalshi_place_ioc_hedge(self):
        client = KalshiOrderClient.__new__(KalshiOrderClient)
        with self.assertRaises(ValueError):
            client.place_ioc_hedge("KX-TEST", "yes", "buy", D("-0.1"), 10)

    def test_polymarket_us_order_client_buy(self):
        client = PolymarketUSOrderClient.__new__(PolymarketUSOrderClient)
        with self.assertRaises(ValueError):
            client.buy("poly-test", "yes", D("0"), 10)

    def test_live_maker_executor_post_quote(self):
        client = LiveMakerExecutor.__new__(LiveMakerExecutor)
        with self.assertRaises(ValueError):
            client.post_quote("poly-test", "yes", D("2"), 10)


class TestKalshiBuildBody(unittest.TestCase):
    """Kalshi's V2 order API (migrated 2026-09-15) has no separate yes/no
    fields -- side is bid/ask and always refers to the YES leg. These
    confirm _build_body's yes/no -> bid/ask + price/1-price mapping,
    against the exact schema read from docs.kalshi.com/openapi.yaml."""

    def test_yes_side_maps_to_bid_at_same_price(self):
        body = KalshiOrderClient._build_body("KX-TEST", "yes", "buy", D("0.42"), 10, time_in_force="good_till_canceled")
        self.assertEqual(body["side"], "bid")
        self.assertEqual(body["price"], "0.42")
        self.assertEqual(body["count"], "10")
        self.assertEqual(body["time_in_force"], "good_till_canceled")
        self.assertEqual(body["self_trade_prevention_type"], "taker_at_cross")

    def test_no_side_maps_to_ask_at_complement_price(self):
        body = KalshiOrderClient._build_body("KX-TEST", "no", "buy", D("0.42"), 10, time_in_force="immediate_or_cancel")
        self.assertEqual(body["side"], "ask")
        self.assertEqual(body["price"], "0.58")

    def test_sell_action_is_rejected(self):
        with self.assertRaises(ValueError):
            KalshiOrderClient._build_body("KX-TEST", "yes", "sell", D("0.42"), 10, time_in_force="good_till_canceled")

    def test_invalid_side_is_rejected(self):
        with self.assertRaises(ValueError):
            KalshiOrderClient._build_body("KX-TEST", "sideways", "buy", D("0.42"), 10, time_in_force="good_till_canceled")

    def test_yes_side_subcent_price_rounds_down_to_the_cent(self):
        # Reproduces the exact live failure 2026-09-16: a computed hedge
        # cap of 0.5050 (not a whole cent) was sent as-is and rejected by
        # Kalshi with {"code": "invalid_price"} -- this market's real
        # price grid is whole cents. Rounds DOWN for a "yes" bid since
        # that's the more conservative (stricter max-buy-price) direction.
        body = KalshiOrderClient._build_body("KX-TEST", "yes", "buy", D("0.5050"), 10, time_in_force="immediate_or_cancel")
        self.assertEqual(body["price"], "0.50")

    def test_no_side_subcent_price_rounds_up_after_complement(self):
        # "no" at 0.4250 -> complement 0.5750 -> must round UP to 0.58 (not
        # down to 0.57): a higher ask is economically a LOWER effective NO
        # cost, so rounding up keeps this at least as conservative as the
        # price that was actually computed, never looser.
        body = KalshiOrderClient._build_body("KX-TEST", "no", "buy", D("0.4250"), 10, time_in_force="immediate_or_cancel")
        self.assertEqual(body["price"], "0.58")

    def test_already_whole_cent_price_is_unaffected(self):
        body = KalshiOrderClient._build_body("KX-TEST", "yes", "buy", D("0.50"), 10, time_in_force="good_till_canceled")
        self.assertEqual(body["price"], "0.50")


class TestKalshiNormalizeResponse(unittest.TestCase):
    """Confirms the response side of the same V2 mapping: average_fill_price
    comes back YES-denominated always (per Kalshi's own BookSide schema
    description) and must be converted back to the caller's side for a NO
    hedge, and average_fee_paid is documented as PER CONTRACT, kept
    separate from the total so callers can't accidentally treat it as one."""

    def test_yes_side_price_passes_through_unchanged(self):
        raw = {"order_id": "o1", "fill_count": "10.00", "remaining_count": "0.00",
               "average_fill_price": "0.4200", "average_fee_paid": "0.0100"}
        norm = KalshiOrderClient._normalize_response(raw, "yes")
        self.assertEqual(norm["order_id"], "o1")
        self.assertEqual(norm["fill_count"], 10)
        self.assertEqual(norm["remaining_count"], 0)
        self.assertEqual(norm["avg_price"], D("0.4200"))
        self.assertEqual(norm["avg_fee_per_contract"], D("0.0100"))

    def test_no_side_price_is_converted_from_yes_complement(self):
        # We bought NO at what we consider 0.58 -- Kalshi reports that same
        # fill as a YES-side average_fill_price of 0.42 (1 - 0.58).
        raw = {"order_id": "o1", "fill_count": "10.00", "remaining_count": "0.00",
               "average_fill_price": "0.4200", "average_fee_paid": "0.0100"}
        norm = KalshiOrderClient._normalize_response(raw, "no")
        self.assertEqual(norm["avg_price"], D("0.5800"))

    def test_zero_fill_reports_no_price_or_fee(self):
        raw = {"order_id": "o1", "fill_count": "0.00", "remaining_count": "10.00"}
        norm = KalshiOrderClient._normalize_response(raw, "yes")
        self.assertEqual(norm["fill_count"], 0)
        self.assertIsNone(norm["avg_price"])
        self.assertIsNone(norm["avg_fee_per_contract"])

    def test_fractional_fill_count_rounds_up(self):
        # This project only ever REQUESTS whole-contract counts, but the
        # response is documented to support fractional contracts down to
        # 0.01 -- round up (same ROUND_CEILING convention as Polymarket's
        # fractional fills elsewhere) rather than silently truncate.
        raw = {"order_id": "o1", "fill_count": "4.30", "remaining_count": "0.70"}
        norm = KalshiOrderClient._normalize_response(raw, "yes")
        self.assertEqual(norm["fill_count"], 5)
        self.assertEqual(norm["remaining_count"], 1)


class _FakeKalshiClientForBuy:
    """Stub in place of the real KalshiOrderClient for testing
    LiveExecutor.buy_kalshi's fill-count/cancel behavior in isolation --
    place_limit_order returns an already-NORMALIZED response, matching
    what the real client hands back after _normalize_response."""

    def __init__(self, fill_count: int, remaining_count: int):
        self._response = {
            "order_id": "order-1", "fill_count": fill_count, "remaining_count": remaining_count,
            "avg_price": None, "avg_fee_per_contract": None,
        }
        self.cancel_calls = []
        self.cancel_should_raise = False

    def place_limit_order(self, ticker, side, action, price, count):
        return self._response

    def cancel_order(self, order_id, ticker):
        self.cancel_calls.append((order_id, ticker))
        if self.cancel_should_raise:
            raise RuntimeError("cancel failed")
        return {"order_id": order_id, "reduced_by": self._response["remaining_count"], "ts_ms": 123}


class TestLiveExecutorBuyKalshiFillHandling(unittest.TestCase):
    """Added 2026-09-15 alongside the real Cancel Order endpoint: a GTC
    leg's unfilled remainder used to just rest on Kalshi forever, and the
    returned Fill always reported the REQUESTED count regardless of what
    actually filled -- these confirm the fix, not just that a fill
    happened at all."""

    def test_full_fill_does_not_cancel_and_reports_requested_count(self):
        kalshi = _FakeKalshiClientForBuy(fill_count=10, remaining_count=0)
        executor = LiveExecutor(kalshi, polymarket=None)
        fill = executor.buy_kalshi("KX-TEST", "yes", D("0.42"), 10)
        self.assertEqual(fill.count, 10)
        self.assertEqual(kalshi.cancel_calls, [])

    def test_partial_fill_cancels_remainder_and_reports_actual_count(self):
        kalshi = _FakeKalshiClientForBuy(fill_count=6, remaining_count=4)
        executor = LiveExecutor(kalshi, polymarket=None)
        fill = executor.buy_kalshi("KX-TEST", "yes", D("0.42"), 10)
        self.assertEqual(fill.count, 6)  # actual fill, not the requested 10
        self.assertEqual(kalshi.cancel_calls, [("order-1", "KX-TEST")])

    def test_zero_fill_cancels_remainder_and_raises(self):
        kalshi = _FakeKalshiClientForBuy(fill_count=0, remaining_count=10)
        executor = LiveExecutor(kalshi, polymarket=None)
        with self.assertRaises(RuntimeError):
            executor.buy_kalshi("KX-TEST", "yes", D("0.42"), 10)
        self.assertEqual(kalshi.cancel_calls, [("order-1", "KX-TEST")])

    def test_cancel_failure_does_not_mask_the_fill_result(self):
        # A failed cleanup attempt must not prevent ArbBot._execute from
        # still learning about the (possibly zero) real fill.
        kalshi = _FakeKalshiClientForBuy(fill_count=6, remaining_count=4)
        kalshi.cancel_should_raise = True
        executor = LiveExecutor(kalshi, polymarket=None)
        fill = executor.buy_kalshi("KX-TEST", "yes", D("0.42"), 10)  # must not raise despite the failed cancel
        self.assertEqual(fill.count, 6)


if __name__ == "__main__":
    unittest.main()
