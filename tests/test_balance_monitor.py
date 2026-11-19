"""
Unit tests for balance_monitor.py. Covers handling capital
drift between the two venues: there's no
programmatic way to move money between them, so this is about alerting
and gating trades on cached balance rather than solving rebalancing itself.
"""

import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from balance_monitor import BalanceMonitor, kalshi_get_balance, polymarket_get_balance

D = Decimal


class TestKalshiGetBalance(unittest.TestCase):
    def test_parses_cents_to_dollars(self):
        result = kalshi_get_balance(lambda path, params: {"balance": 123456})
        self.assertEqual(result, D("1234.56"))

    def test_missing_field_returns_none(self):
        result = kalshi_get_balance(lambda path, params: {"unexpected": 1})
        self.assertIsNone(result)

    def test_exception_returns_none(self):
        def raising_get(path, params):
            raise ConnectionError("down")
        self.assertIsNone(kalshi_get_balance(raising_get))


class _FakePolyAccount:
    """`response` is whatever client.account.balances() itself returns --
    the real shape (confirmed 2026-09-15 against the installed SDK's
    GetAccountBalancesResponse) is an envelope dict {"balances": [...]},
    not a bare list, so callers below wrap their fixture data accordingly."""

    def __init__(self, response=None, raises=False):
        self._response = response
        self._raises = raises

    def balances(self):
        if self._raises:
            raise RuntimeError("boom")
        return self._response


class _FakePolyClient:
    def __init__(self, account=None):
        self.account = account


class TestPolymarketGetBalance(unittest.TestCase):
    def test_finds_buying_power(self):
        account = _FakePolyAccount(response={"balances": [{"currency": "USD", "buyingPower": "500.25"}]})
        client = _FakePolyClient(account=account)
        self.assertEqual(polymarket_get_balance(client), D("500.25"))

    def test_falls_back_to_current_balance(self):
        account = _FakePolyAccount(response={"balances": [{"currency": "USD", "currentBalance": "300.00"}]})
        client = _FakePolyClient(account=account)
        self.assertEqual(polymarket_get_balance(client), D("300.00"))

    def test_ignores_non_usd_entries(self):
        account = _FakePolyAccount(response={"balances": [
            {"currency": "EUR", "buyingPower": "999.00"},
            {"currency": "USD", "buyingPower": "42.00"},
        ]})
        client = _FakePolyClient(account=account)
        self.assertEqual(polymarket_get_balance(client), D("42.00"))

    def test_missing_balances_envelope_returns_none(self):
        # The bug found 2026-09-15: the original code treated a response
        # with no "balances" key as a single flat entry instead of failing
        # to find the list at all -- this is the regression test for it.
        account = _FakePolyAccount(response={"currency": "USD", "buyingPower": "10.00"})
        client = _FakePolyClient(account=account)
        self.assertIsNone(polymarket_get_balance(client))

    def test_no_account_resource_returns_none(self):
        client = _FakePolyClient(account=None)
        self.assertIsNone(polymarket_get_balance(client))

    def test_no_balances_method_returns_none(self):
        class _EmptyAccount:
            pass
        client = _FakePolyClient(account=_EmptyAccount())
        self.assertIsNone(polymarket_get_balance(client))

    def test_method_raising_returns_none(self):
        account = _FakePolyAccount(raises=True)
        client = _FakePolyClient(account=account)
        self.assertIsNone(polymarket_get_balance(client))

    def test_no_recognizable_field_returns_none(self):
        account = _FakePolyAccount(response={"balances": [{"currency": "USD", "somethingElse": "1"}]})
        client = _FakePolyClient(account=account)
        self.assertIsNone(polymarket_get_balance(client))


class TestBalanceMonitor(unittest.TestCase):
    def test_refresh_populates_both_balances(self):
        monitor = BalanceMonitor(
            kalshi_get_balance_fn=lambda: D("100.00"),
            poly_get_balance_fn=lambda: D("50.00"),
        )
        monitor.refresh()
        self.assertEqual(monitor.kalshi_balance, D("100.00"))
        self.assertEqual(monitor.poly_balance, D("50.00"))

    def test_can_afford_true_when_sufficient(self):
        monitor = BalanceMonitor(lambda: D("100.00"), lambda: D("50.00"))
        monitor.refresh()
        self.assertTrue(monitor.can_afford("kalshi", D("99.99")))
        self.assertTrue(monitor.can_afford("polymarket_us", D("50.00")))  # exact match is affordable

    def test_can_afford_false_when_insufficient(self):
        monitor = BalanceMonitor(lambda: D("100.00"), lambda: D("50.00"))
        monitor.refresh()
        self.assertFalse(monitor.can_afford("kalshi", D("100.01")))
        self.assertFalse(monitor.can_afford("polymarket_us", D("50.01")))

    def test_unknown_balance_treated_as_insufficient(self):
        monitor = BalanceMonitor(lambda: None, lambda: D("50.00"))
        monitor.refresh()
        self.assertFalse(monitor.can_afford("kalshi", D("0.01")))  # can't verify -> fail-safe

    def test_can_afford_uses_cached_value_not_a_fresh_fetch(self):
        calls = []

        def kalshi_fn():
            calls.append(1)
            return D("100.00")

        monitor = BalanceMonitor(kalshi_fn, lambda: D("50.00"))
        monitor.refresh()
        monitor.can_afford("kalshi", D("1"))
        monitor.can_afford("kalshi", D("1"))
        self.assertEqual(len(calls), 1)  # only the one refresh() call fetched

    def test_maybe_refresh_respects_interval(self):
        calls = []
        monitor = BalanceMonitor(
            lambda: (calls.append(1), D("100.00"))[1],
            lambda: D("50.00"),
            refresh_interval=1000.0,
        )
        monitor.maybe_refresh()
        monitor.maybe_refresh()
        self.assertEqual(len(calls), 1)  # second call skipped, well within the interval

    def test_low_balance_alert_logged(self):
        monitor = BalanceMonitor(lambda: D("5.00"), lambda: D("50.00"), min_balance=D("10.00"))
        with self.assertLogs("balance_monitor", level="CRITICAL") as cm:
            monitor.refresh()
        self.assertTrue(any("LOW BALANCE" in line and "kalshi" in line for line in cm.output))

    def test_imbalance_alert_logged(self):
        monitor = BalanceMonitor(lambda: D("10.00"), lambda: D("990.00"), min_balance_share=D("0.2"))
        with self.assertLogs("balance_monitor", level="CRITICAL") as cm:
            monitor.refresh()
        self.assertTrue(any("BALANCE IMBALANCE" in line and "kalshi" in line for line in cm.output))

    def test_balanced_venues_do_not_alert(self):
        monitor = BalanceMonitor(lambda: D("500.00"), lambda: D("500.00"), min_balance=D("10"), min_balance_share=D("0.2"))
        monitor.refresh()  # should not raise / should produce no CRITICAL logs
        self.assertEqual(monitor.kalshi_balance, D("500.00"))


if __name__ == "__main__":
    unittest.main()
