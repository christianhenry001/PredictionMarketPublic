"""
Unit tests for arb_bot.py's balance-monitor hard gate, added alongside
balance_monitor.py to guard against capital
drifting out of balance between the two venues. Unlike the maker bot's
hedge leg (real exposure already exists by the time that check runs), both
legs here are still uncommitted at the gate point, so refusing the whole
trade is a clean no-op -- these tests focus on proving neither leg fires
when either venue's balance is insufficient, and that both fire normally
when balance_monitor is absent (dry-run/paper) or sufficient.
"""

import asyncio
import sys
import unittest
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import arb_bot as arb_bot_module
from arb_bot import ArbBot
from arbitrage import ArbOpportunity
from balance_monitor import BalanceMonitor
from market_pairs import MarketPair
from orderbook_feed import BookStore

D = Decimal


class _RecordingExecutor:
    def __init__(self):
        self.kalshi_calls = []
        self.poly_calls = []

    def buy_kalshi(self, ticker, side, price, count):
        self.kalshi_calls.append((ticker, side, price, count))
        return {"venue": "kalshi", "filled": count}

    def buy_polymarket(self, market_slug, side, price, count):
        self.poly_calls.append((market_slug, side, price, count))
        return {"venue": "polymarket_us", "filled": count}


def _pair(**overrides) -> MarketPair:
    defaults = dict(
        name="test", kalshi_ticker="KX-TEST", poly_market_slug="poly-test",
        min_edge=D("0.02"), max_contracts=10,
    )
    defaults.update(overrides)
    return MarketPair(**defaults)


def _opp(pair, direction="kalshi_yes+poly_no", buy_a_price=D("0.40"), buy_b_price=D("0.50"), max_contracts=5) -> ArbOpportunity:
    return ArbOpportunity(
        pair=pair,
        direction=direction,
        buy_a_venue="kalshi" if direction == "kalshi_yes+poly_no" else "polymarket_us",
        buy_a_price=buy_a_price,
        buy_b_venue="polymarket_us" if direction == "kalshi_yes+poly_no" else "kalshi",
        buy_b_price=buy_b_price,
        max_contracts=max_contracts,
        gross_edge=D("0.10"),
        fees_per_contract=D("0.01"),
        net_edge=D("0.09"),
        fills_a=[(buy_a_price, Decimal(max_contracts))],
        fills_b=[(buy_b_price, Decimal(max_contracts))],
    )


def _monitor(kalshi_balance, poly_balance) -> BalanceMonitor:
    monitor = BalanceMonitor(kalshi_get_balance_fn=lambda: kalshi_balance, poly_get_balance_fn=lambda: poly_balance)
    monitor.refresh()
    return monitor


class TestBalanceGateBeforeExecution(unittest.TestCase):
    def _bot(self, balance_monitor):
        store = BookStore()
        executor = _RecordingExecutor()
        bot = ArbBot(store, [_pair()], executor, live=True, max_notional=D("1000"), balance_monitor=balance_monitor)
        return bot, executor

    def test_both_legs_skipped_when_kalshi_leg_unaffordable(self):
        # direction kalshi_yes+poly_no: kalshi cost = 0.40*5=2.00, poly cost = 0.50*5=2.50
        monitor = _monitor(kalshi_balance=D("1.00"), poly_balance=D("100.00"))
        bot, executor = self._bot(monitor)
        opp = _opp(_pair(), direction="kalshi_yes+poly_no", buy_a_price=D("0.40"), buy_b_price=D("0.50"), max_contracts=5)

        bot._on_opportunity(opp)

        self.assertEqual(executor.kalshi_calls, [])
        self.assertEqual(executor.poly_calls, [])
        self.assertEqual(bot._inflight, set())

    def test_both_legs_skipped_when_poly_leg_unaffordable(self):
        monitor = _monitor(kalshi_balance=D("100.00"), poly_balance=D("1.00"))
        bot, executor = self._bot(monitor)
        opp = _opp(_pair(), direction="kalshi_yes+poly_no", buy_a_price=D("0.40"), buy_b_price=D("0.50"), max_contracts=5)

        bot._on_opportunity(opp)

        self.assertEqual(executor.kalshi_calls, [])
        self.assertEqual(executor.poly_calls, [])

    def test_trade_proceeds_when_balance_sufficient(self):
        # _on_opportunity schedules a real asyncio task once it decides to
        # proceed, so this needs a running loop -- _inflight is populated
        # synchronously before that task is created, so no need to await it.
        # COOLDOWN_SECONDS is patched to 0 so the scheduled _execute task's
        # finally-block sleep doesn't make this test take 5 real seconds.
        async def run():
            monitor = _monitor(kalshi_balance=D("100.00"), poly_balance=D("100.00"))
            bot, executor = self._bot(monitor)
            opp = _opp(_pair(), direction="kalshi_yes+poly_no", buy_a_price=D("0.40"), buy_b_price=D("0.50"), max_contracts=5)
            bot._on_opportunity(opp)
            self.assertIn("test", bot._inflight)
            await asyncio.sleep(0)  # let the scheduled _execute task run so it doesn't warn on teardown

        original = arb_bot_module.COOLDOWN_SECONDS
        arb_bot_module.COOLDOWN_SECONDS = 0
        try:
            asyncio.run(run())
        finally:
            arb_bot_module.COOLDOWN_SECONDS = original

    def test_direction_is_respected_when_attributing_cost_to_venue(self):
        # poly_yes+kalshi_no: buy_a is polymarket_us, buy_b is kalshi.
        # Plenty of kalshi balance but not enough polymarket_us balance
        # should still block it -- proves cost isn't attributed backwards.
        monitor = _monitor(kalshi_balance=D("100.00"), poly_balance=D("1.00"))
        bot, executor = self._bot(monitor)
        opp = _opp(_pair(), direction="poly_yes+kalshi_no", buy_a_price=D("0.50"), buy_b_price=D("0.40"), max_contracts=5)

        bot._on_opportunity(opp)

        self.assertEqual(executor.kalshi_calls, [])
        self.assertEqual(executor.poly_calls, [])

    def test_no_balance_monitor_means_no_gate_at_all(self):
        async def run():
            store = BookStore()
            executor = _RecordingExecutor()
            bot = ArbBot(store, [_pair()], executor, live=True, max_notional=D("1000"), balance_monitor=None)
            opp = _opp(_pair(), direction="kalshi_yes+poly_no", buy_a_price=D("0.40"), buy_b_price=D("0.50"), max_contracts=5)
            bot._on_opportunity(opp)
            self.assertIn("test", bot._inflight)
            await asyncio.sleep(0)

        original = arb_bot_module.COOLDOWN_SECONDS
        arb_bot_module.COOLDOWN_SECONDS = 0
        try:
            asyncio.run(run())
        finally:
            arb_bot_module.COOLDOWN_SECONDS = original


if __name__ == "__main__":
    unittest.main()
