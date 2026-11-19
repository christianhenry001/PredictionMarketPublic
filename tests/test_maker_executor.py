"""
Unit tests for maker_executor.py's PolymarketFillListener, focused on the
fill-deduplication guard added 2026-09-15: a duplicate or replayed WS
notification (reconnect replay, at-least-once delivery) must not trigger a
second hedge for a fill that only really happened once.
"""

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decimal import Decimal

from maker_executor import MAX_SEEN_EXECUTIONS, LiveMakerExecutor, PolymarketFillListener, RestingQuote


def _order_update(exec_id=None, trade_id=None, market_slug="poly-test", price="0.50", size="5", intent="ORDER_INTENT_BUY_LONG", avg_px=None):
    order = {"marketSlug": market_slug, "intent": intent, "id": "order-1"}
    if avg_px is not None:
        order["avgPx"] = {"value": avg_px, "currency": "USD"}
    execution = {
        "type": "EXECUTION_TYPE_FILL",
        # "intent" (not "side" -- that field is the order's buy/sell
        # DIRECTION, "ORDER_SIDE_BUY"/"ORDER_SIDE_SELL", confirmed against
        # a real captured order 2026-09-17, not our yes/no outcome side at
        # all) is the field _on_order_update actually reads now, via the
        # same _INTENT mapping already confirmed correct for order
        # creation. Default "ORDER_INTENT_BUY_LONG" -> "yes".
        "order": order,
        # "lastPx" here is the FALLBACK only -- see order.avgPx's
        # docstring in _on_order_update for why lastPx alone was wrong.
        "lastPx": {"value": price, "currency": "USD"},
        "lastShares": size,
    }
    if exec_id is not None:
        execution["id"] = exec_id
    if trade_id is not None:
        execution["tradeId"] = trade_id
    return {"orderSubscriptionUpdate": {"execution": execution}}


class TestFillDeduplication(unittest.TestCase):
    def setUp(self):
        self.fills = []
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=self.fills.append)

    def test_first_fill_is_processed(self):
        self.listener._on_order_update(_order_update(exec_id="exec-1"))
        self.assertEqual(len(self.fills), 1)

    def test_duplicate_execution_id_is_ignored(self):
        self.listener._on_order_update(_order_update(exec_id="exec-1"))
        self.listener._on_order_update(_order_update(exec_id="exec-1"))
        self.assertEqual(len(self.fills), 1)

    def test_falls_back_to_trade_id_when_no_id(self):
        self.listener._on_order_update(_order_update(trade_id="trade-1"))
        self.listener._on_order_update(_order_update(trade_id="trade-1"))
        self.assertEqual(len(self.fills), 1)

    def test_different_execution_ids_both_processed(self):
        self.listener._on_order_update(_order_update(exec_id="exec-1"))
        self.listener._on_order_update(_order_update(exec_id="exec-2"))
        self.assertEqual(len(self.fills), 2)

    def test_missing_both_ids_still_processes_the_fill(self):
        # Can't dedupe without an id, but a real fill must never be
        # silently dropped just because it can't be deduped.
        self.listener._on_order_update(_order_update())
        self.assertEqual(len(self.fills), 1)

    def test_seen_ids_bounded_evicts_oldest(self):
        for i in range(MAX_SEEN_EXECUTIONS + 10):
            self.listener._on_order_update(_order_update(exec_id=f"exec-{i}"))
        self.assertLessEqual(len(self.listener._seen_execution_ids), MAX_SEEN_EXECUTIONS)
        # The very first id should have been evicted, so it's treated as
        # "new" again if it somehow reappeared.
        fills_before = len(self.fills)
        self.listener._on_order_update(_order_update(exec_id="exec-0"))
        self.assertEqual(len(self.fills), fills_before + 1)


class TestFillSideComesFromIntent(unittest.TestCase):
    """Added 2026-09-17 after a real, repeating live crash: _on_order_update
    used to read order.get("side"), guessed since the exact field was
    unconfirmed -- but the real "side" field is the order's buy/sell
    DIRECTION ("ORDER_SIDE_BUY"/"ORDER_SIDE_SELL"), not our yes/no outcome
    side at all. That wrong value flowed into MakerFill.side, and
    downstream MakerQuoter.clear_filled_quote's self.active_quotes[side]
    lookup (only "yes"/"no" keys) raised a real KeyError -- which crashed
    this listener's ENTIRE message loop, forcing a reconnect, on every
    single real fill, every time, all night (and likely every prior
    --live run that ever got a real fill, since paper trading can't
    exercise this listener at all). The reconciler backstop caught every
    affected fill within ~3s regardless, but the fast direct hedge path
    -- the whole point of the night's fill-to-hedge latency work -- had
    never once actually fired. Fixed by reading "intent" instead, the
    field this project has already confirmed correct (it's what
    _build_body sends to create real orders that posted successfully all
    night) via the same _INTENT mapping, reversed."""

    def setUp(self):
        self.fills = []
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=self.fills.append)

    def test_buy_long_intent_maps_to_yes(self):
        self.listener._on_order_update(_order_update(exec_id="e1", intent="ORDER_INTENT_BUY_LONG"))
        self.assertEqual(self.fills[0].side, "yes")

    def test_buy_short_intent_maps_to_no(self):
        self.listener._on_order_update(_order_update(exec_id="e1", intent="ORDER_INTENT_BUY_SHORT"))
        self.assertEqual(self.fills[0].side, "no")

    def test_unrecognized_intent_is_dropped_not_crashed(self):
        # Reproduces the exact live crash: the real (wrong) field value
        # this bug used to pass through, "ORDER_SIDE_BUY", is not a valid
        # intent either -- must be safely dropped, never raise.
        self.listener._on_order_update(_order_update(exec_id="e1", intent="ORDER_SIDE_BUY"))
        self.assertEqual(self.fills, [])

    def test_missing_intent_is_dropped_not_crashed(self):
        execution = {
            "type": "EXECUTION_TYPE_FILL",
            "order": {"marketSlug": "poly-test", "id": "order-1"},  # no intent field at all
            "lastPx": {"value": "0.50", "currency": "USD"},
            "lastShares": "5",
            "id": "e1",
        }
        self.listener._on_order_update({"orderSubscriptionUpdate": {"execution": execution}})
        self.assertEqual(self.fills, [])


class TestFillPriceComesFromAvgPxNotLastPx(unittest.TestCase):
    """Added 2026-09-18 after a real live incident: a "no" quote resting at
    limit price 0.39 later showed a fill whose hedge-pricing log (built
    from execution.lastPx) reported reference_price 0.54 -- 15c WORSE
    than the order's own limit price, which a real resting limit order
    can never do. lastPx most likely reflects the market's last trade
    price generally, not this specific execution. order.avgPx is a real,
    documented SDK field (Order.avgPx) matching the same convention
    already used correctly for Kalshi's avg_fill_price -- now preferred,
    with lastPx kept only as a fallback if avgPx is ever absent.

    These "yes"-side cases (the default intent) are unaffected by the
    2026-09-19 yes/no price-conversion fix below -- avgPx is used as-is
    only when side == "yes"."""

    def setUp(self):
        self.fills = []
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=self.fills.append)

    def test_avg_px_is_used_when_present(self):
        self.listener._on_order_update(
            _order_update(exec_id="e1", price="0.54", avg_px="0.39")
        )
        self.assertEqual(self.fills[0].price, Decimal("0.39"))

    def test_falls_back_to_last_px_when_avg_px_absent(self):
        # Existing behavior/tests (no avgPx key at all) must be unaffected.
        self.listener._on_order_update(_order_update(exec_id="e1", price="0.50"))
        self.assertEqual(self.fills[0].price, Decimal("0.50"))


class TestNoSideFillPriceIsConvertedFromYesTerms(unittest.TestCase):
    """Added 2026-09-19 after a real, deliberate, isolated live test: a 1-
    contract "no" order submitted at 0.05 (meant to be unfillable) filled
    INSTANTLY at a real cost of 0.865. Root cause: Polymarket US has ONE
    single book, always denominated in probability-of-YES (confirmed,
    see PolymarketUSFeed's docstring and _quotes_would_cross's model of
    this) -- avgPx/lastPx for a "no" (BUY_SHORT) fill are reported in
    that SAME yes-denominated space, not in our own "cost of no" terms.
    0.05 was read by the exchange as "sell YES for at least 0.05" --
    absurdly below the real yes price (~0.135 at the time) -- so it
    matched instantly, at a real yes-sell price of ~0.135, i.e. a real
    "no" cost of (1 - 0.135) = 0.865, exactly matching the confirmed
    fill. This was true of EVERY "no" fill this project has ever
    received, including the 2026-09-18 "impossible" incident above --
    that fill's avgPx=0.39 was also yes-denominated, so the real "no"
    cost was actually (1 - 0.39) = 0.61, not 0.39."""

    def setUp(self):
        self.fills = []
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=self.fills.append)

    def test_no_side_fill_price_is_one_minus_avg_px(self):
        self.listener._on_order_update(
            _order_update(exec_id="e1", intent="ORDER_INTENT_BUY_SHORT", price="0.54", avg_px="0.39")
        )
        self.assertEqual(self.fills[0].price, Decimal("0.61"))

    def test_reproduces_the_real_live_incident_exactly(self):
        # The actual confirmed numbers: submitted 0.05, real yes price
        # ~0.135 at fill time, real position entry recorded as 0.865.
        self.listener._on_order_update(
            _order_update(exec_id="e1", intent="ORDER_INTENT_BUY_SHORT", avg_px="0.135")
        )
        self.assertEqual(self.fills[0].price, Decimal("0.865"))

    def test_yes_side_fill_price_is_unaffected_by_the_conversion(self):
        self.listener._on_order_update(
            _order_update(exec_id="e1", intent="ORDER_INTENT_BUY_LONG", avg_px="0.39")
        )
        self.assertEqual(self.fills[0].price, Decimal("0.39"))


class _FakePolyOrdersResource:
    def __init__(self):
        self.cancel_calls = []
        self.create_calls = []

    def cancel(self, order_id, params):
        self.cancel_calls.append((order_id, params))

    def create(self, params):
        self.create_calls.append(params)
        return {"id": "order-created", "executions": []}


class TestLiveMakerExecutorPostQuotePriceConversion(unittest.TestCase):
    """Added 2026-09-19 after a real, deliberate, isolated live test proved
    Polymarket US's "price" field for a "no" (BUY_SHORT) order is
    yes-denominated, not in our own "cost of no" terms -- see
    TestNoSideFillPriceIsConvertedFromYesTerms's docstring for the full
    incident. post_quote must submit (1 - price) for "no" so the
    exchange receives the correct yes-denominated value; "yes" quotes
    are unaffected (Polymarket's book is already yes-denominated)."""

    def setUp(self):
        self.executor = LiveMakerExecutor.__new__(LiveMakerExecutor)  # skip __init__, no real credentials needed
        self.fake_orders = _FakePolyOrdersResource()
        self.executor._client = type("_FakeClient", (), {"orders": self.fake_orders})()

    def test_no_quote_submits_one_minus_price(self):
        self.executor.post_quote("poly-test", "no", Decimal("0.72"), 2)
        self.assertEqual(self.fake_orders.create_calls[0]["price"]["value"], "0.28")

    def test_yes_quote_submits_price_unchanged(self):
        self.executor.post_quote("poly-test", "yes", Decimal("0.18"), 2)
        self.assertEqual(self.fake_orders.create_calls[0]["price"]["value"], "0.18")

    def test_returned_resting_quote_still_tracks_our_own_native_price(self):
        # Internal bookkeeping (deadband comparisons, logs, position math)
        # must keep using OUR "no" price -- only the wire value changes.
        quote = self.executor.post_quote("poly-test", "no", Decimal("0.72"), 2)
        self.assertEqual(quote.price, Decimal("0.72"))

    def test_reproduces_the_real_live_incident_submission(self):
        # The actual test order: "no" at 0.05, meant to be unfillable.
        # The real bug: this got submitted as-is (0.05) instead of 0.95,
        # an absurdly cheap yes-sell floor that matched instantly.
        self.executor.post_quote("poly-test", "no", Decimal("0.05"), 1)
        self.assertEqual(self.fake_orders.create_calls[0]["price"]["value"], "0.95")


class TestLiveMakerExecutorCancel(unittest.TestCase):
    """Regression test for a bug found 2026-09-15 by reading the installed
    polymarket-us SDK source directly: orders.cancel() requires a second
    positional arg, {"marketSlug": ...} (CancelOrderParams, not optional)
    -- calling it with just the order_id (the original code) raises
    TypeError on every single cancel in live mode, which means every
    safety guard that pulls a quote (staleness, self-crossing, halt,
    market-close) would fail to actually cancel anything."""

    def test_cancel_quote_passes_market_slug(self):
        executor = LiveMakerExecutor.__new__(LiveMakerExecutor)  # skip __init__, no real credentials needed
        fake_orders = _FakePolyOrdersResource()
        executor._client = type("_FakeClient", (), {"orders": fake_orders})()
        quote = RestingQuote(order_id="order-1", market_slug="poly-test", side="yes", price=Decimal("0.42"), size=5)

        executor.cancel_quote(quote)

        self.assertEqual(fake_orders.cancel_calls, [("order-1", {"marketSlug": "poly-test"})])


class TestHeartbeatCountsAsProofOfLife(unittest.TestCase):
    """Added 2026-09-16 after the first real --live run showed the fill
    listener reconnecting ~18 times in 32 minutes despite constant order
    activity via REST -- strong evidence a real server heartbeat was being
    received but not counted, since only _on_order_update refreshed
    _last_msg_at. A heartbeat now counts too, decoupling "the connection
    is alive" from "an order-related event happened recently.\""""

    def setUp(self):
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=lambda f: None)

    def test_heartbeat_refreshes_last_msg_at(self):
        self.listener._last_msg_at = 0.0
        self.listener._on_heartbeat()
        self.assertGreater(self.listener._last_msg_at, 0.0)

    def test_heartbeat_alone_is_enough_to_avoid_staleness(self):
        # Simulates what run() actually checks: as long as SOMETHING (an
        # order update OR a heartbeat) has refreshed _last_msg_at within
        # stale_after, the connection must not be treated as dead.
        import time
        self.listener._on_heartbeat()
        elapsed = time.time() - self.listener._last_msg_at
        self.assertLess(elapsed, self.listener.stale_after)


class TestCloseAndErrorAreTheRealDisconnectSignal(unittest.TestCase):
    """Added 2026-09-17: tuning stale_after (15 -> 45) never converged --
    live data showed reconnects clustering almost exactly at whatever the
    current value was, both times, immediately after each change. Root
    cause was that the app-level staleness check was never actually
    measuring connection health -- the underlying `websockets` library
    already does real protocol-level ping/pong (confirmed via its real
    installed defaults, ping_interval=20/ping_timeout=20) and already
    emits "close"/"error" on a genuine failure (BaseWebSocket._message_loop)
    -- this listener just never listened for them. These tests confirm
    run() now reacts to those real events instead of only inferring
    staleness from app-level message silence."""

    def setUp(self):
        self.listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=lambda f: None)
        # run() creates this fresh each call; tests simulate that setup
        # directly rather than driving the full async run() loop.
        self.listener._closed_event = asyncio.Event()

    def test_on_close_sets_the_event_and_a_reason(self):
        self.listener._on_close()
        self.assertTrue(self.listener._closed_event.is_set())
        self.assertEqual(self.listener._closed_reason, "connection closed")

    def test_on_error_sets_the_event_and_includes_the_error(self):
        self.listener._on_error(RuntimeError("boom"))
        self.assertTrue(self.listener._closed_event.is_set())
        self.assertIn("boom", self.listener._closed_reason)

    def test_stale_after_is_now_a_generous_last_resort_not_a_tuned_cadence(self):
        # No specific value is "correct" anymore since this is a fallback,
        # not the primary signal -- just confirm it's generous enough to
        # not fire during ordinary quiet periods that used to trip the
        # old 15s/45s values (the actual bug that motivated this fix).
        self.assertGreaterEqual(self.listener.stale_after, 60.0)


if __name__ == "__main__":
    unittest.main()


class TestRejectionRouting(unittest.TestCase):
    """Real EXECUTION_TYPE_REJECTED payload shape, captured live 2026-09-23
    from a post-only order placed through the best ask (aec-nhl-la-ana)."""

    REJECTED = {
        "requestId": "po-capture",
        "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
        "orderSubscriptionUpdate": {"execution": {
            "id": "", "type": "EXECUTION_TYPE_REJECTED",
            "text": "Order may participate but not initiate in the market",
            "orderRejectReason": "ORD_REJECT_REASON_EXCHANGE_OPTION",
            "lastShares": "0", "aggressor": False,
            "order": {"id": "CP8CR2NDAXAV", "marketSlug": "poly-test", "state": "ORDER_STATE_REJECTED",
                      "intent": "ORDER_INTENT_BUY_LONG", "price": {"value": "0.48", "currency": "USD"},
                      "cumQuantity": 0, "leavesQuantity": 0},
        }},
    }

    def test_rejection_goes_to_on_reject_not_on_fill(self):
        fills, rejects = [], []
        listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=fills.append,
                                          on_reject=lambda *a: rejects.append(a))
        listener._on_order_update(self.REJECTED)
        self.assertEqual(fills, [])
        self.assertEqual(rejects, [("CP8CR2NDAXAV", "poly-test", "Order may participate but not initiate in the market")])

    def test_rejection_without_on_reject_is_ignored(self):
        # maker_bot.py's listener passes no on_reject -- must not crash.
        fills = []
        listener = PolymarketFillListener(["poly-test"], "key", "secret", on_fill=fills.append)
        listener._on_order_update(self.REJECTED)
        self.assertEqual(fills, [])
