"""
Rate-limit handling (rate_limit.py, audit item #20, added 2026-09-28):
  - a 429 with Retry-After waits (with jitter) and retries; the budget then
    holds everything but risk cancels until the Retry-After has passed
  - an exhausted budget still lets retreat/risk cancels through and defers
    advances (and reposts) to the watchdog's retry
"""

import random
import sys
import unittest
import unittest.mock
from decimal import Decimal
from email.utils import format_datetime
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from polymarket_us.errors import BadRequestError, RateLimitError

import rate_limit
from maker_executor import LiveMakerExecutor
from rate_limit import (
    ActionBudget, Priority, RateLimitedError, TokenBucket, call_with_429_retry, retry_after_seconds,
)

D = Decimal


def _429(retry_after: str | None = "0.5") -> RateLimitError:
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    response = httpx.Response(429, headers=headers, request=httpx.Request("POST", "https://api.polymarket.us/v1/orders"))
    return RateLimitError("Too Many Requests", response=response)


class _Flaky:
    """Raises the given exceptions in turn, then returns 'ok'."""

    def __init__(self, *errors):
        self.errors = list(errors)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "ok"


class TestTokenBucket(unittest.TestCase):
    def test_refills_to_capacity_and_can_go_into_debt(self):
        b = TokenBucket(capacity=10, rate_per_s=5)
        self.assertEqual(b.available(0.0), 10)
        b.consume(13, 0.0)
        self.assertEqual(b.available(0.0), -3)
        self.assertEqual(b.available(1.0), 2)
        self.assertEqual(b.available(100.0), 10)

    def test_worst_case_one_second_window_leaves_headroom_below_the_documented_limit(self):
        # Budgeted calls may use at most C + r in any second; keep >= 4/s of the
        # documented 20 for the calls the budget always lets through (risk
        # cancels, reads, unwinds, retries).
        for capacity, rate in ((rate_limit.GLOBAL_BURST, rate_limit.GLOBAL_RATE_PER_S),
                               (rate_limit.MARKET_BURST, rate_limit.MARKET_RATE_PER_S)):
            self.assertLessEqual(capacity + rate, rate_limit.DOCUMENTED_REQUESTS_PER_SECOND - 4)


class TestRetryOn429(unittest.TestCase):
    def test_a_429_with_retry_after_waits_that_long_plus_jitter_then_retries(self):
        sleeps = []
        budget = ActionBudget(clock=lambda: 100.0)
        fn = _Flaky(_429("0.5"))
        with self.assertLogs("rate_limit", level="WARNING") as cm:
            result = call_with_429_retry(fn, "create", budget, sleep=sleeps.append, rng=random.Random(1))
        self.assertEqual(result, "ok")
        self.assertEqual(fn.calls, 2)
        self.assertEqual(len(sleeps), 1)
        self.assertGreaterEqual(sleeps[0], 0.5)
        self.assertLessEqual(sleeps[0], 0.5 * (1 + rate_limit.RETRY_JITTER_FRACTION))
        self.assertTrue(any("429" in line and "retrying" in line for line in cm.output))
        self.assertEqual(budget.rate_limited, 1)
        self.assertEqual(budget.cooloff_until, 100.5)

    def test_without_retry_after_waits_one_second_as_documented(self):
        # docs.polymarket.us: "Wait 1 second before retrying"; no Retry-After header is documented.
        sleeps = []
        fn = _Flaky(_429(None))
        self.assertEqual(call_with_429_retry(fn, "cancel", sleep=sleeps.append, rng=random.Random(0)), "ok")
        self.assertEqual(fn.calls, 2)
        self.assertGreaterEqual(sleeps[0], 1.0)
        self.assertLessEqual(sleeps[0], 1.0 * (1 + rate_limit.RETRY_JITTER_FRACTION))

    def test_repeated_429s_back_off_exponentially_in_the_budget(self):
        clock = {"t": 0.0}
        budget = ActionBudget(clock=lambda: clock["t"])
        waits = []
        for _ in range(4):
            budget.note_rate_limited(None)
            waits.append(budget.cooloff_until - clock["t"])
            clock["t"] = budget.cooloff_until
        self.assertEqual(waits, [1.0, 2.0, 4.0, 8.0])
        clock["t"] += rate_limit.BACKOFF_WINDOW_SECONDS  # a quiet minute resets it
        budget.note_rate_limited(None)
        self.assertEqual(budget.cooloff_until - clock["t"], 1.0)

    def test_http_date_retry_after_is_understood(self):
        when = datetime(2026, 9, 28, 12, 0, 3, tzinfo=timezone.utc)
        exc = _429(format_datetime(when, usegmt=True))
        self.assertAlmostEqual(retry_after_seconds(exc, now=when.timestamp() - 3), 3.0)

    def test_gives_up_after_the_last_attempt(self):
        fn = _Flaky(*[_429("0.1")] * 5)
        with self.assertLogs("rate_limit", level="ERROR"):
            with self.assertRaises(RateLimitedError):
                call_with_429_retry(fn, "create", sleep=lambda s: None)
        self.assertEqual(fn.calls, rate_limit.RETRY_ATTEMPTS)

    def test_a_long_retry_after_is_not_slept_out_inline_but_holds_the_budget(self):
        sleeps = []
        budget = ActionBudget(clock=lambda: 0.0)
        with self.assertLogs("rate_limit", level="ERROR"):
            with self.assertRaises(RateLimitedError):
                call_with_429_retry(_Flaky(_429("30")), "create", budget, sleep=sleeps.append)
        self.assertEqual(sleeps, [])
        self.assertFalse(budget.allows("m", 1, Priority.CANCEL))
        self.assertTrue(budget.allows("m", 1, Priority.RISK_CANCEL))

    def test_other_errors_pass_straight_through(self):
        response = httpx.Response(400, request=httpx.Request("POST", "https://x"))
        fn = _Flaky(BadRequestError("bad", response=response))
        with self.assertRaises(BadRequestError):
            call_with_429_retry(fn, "create", sleep=lambda s: None)
        self.assertEqual(fn.calls, 1)


class TestLiveExecutorRetries(unittest.TestCase):
    def _executor(self, orders):
        ex = LiveMakerExecutor.__new__(LiveMakerExecutor)
        ex._client = type("C", (), {"orders": orders})()
        return ex

    def test_post_and_cancel_retry_through_a_429(self):
        class Orders:
            def __init__(self):
                self.creates = _Flaky(_429("0.01"))
                self.cancels = _Flaky(_429("0.01"))

            def create(self, params):
                self.creates()
                return {"id": "o1"}

            def cancel(self, order_id, params):
                self.cancels()

        orders = Orders()
        ex = self._executor(orders)
        with unittest.mock.patch("time.sleep"), self.assertLogs("rate_limit", level="WARNING"):
            quote = ex.post_quote("m", "yes", D("0.45"), 1)
            ex.cancel_quote(quote)
        self.assertEqual((orders.creates.calls, orders.cancels.calls), (2, 2))

    def test_a_429_that_outlasts_the_retries_is_a_clean_failure_not_an_unknown_order(self):
        class Orders:
            def create(self, params):
                raise _429("30")

        with self.assertLogs("rate_limit", level="ERROR"):
            with self.assertRaises(RateLimitedError):
                self._executor(Orders()).post_quote("m", "yes", D("0.45"), 1)


if __name__ == "__main__":
    unittest.main()
