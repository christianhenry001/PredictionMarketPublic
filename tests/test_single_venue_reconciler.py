"""
Unit tests for single_venue_reconciler.py. Key invariants under test:
  - A local/real mismatch beyond tolerance corrects the LOCAL tracker to
    the real value (not the other way around, and not a corrective order
    the way the cross-venue reconciler places one -- see the module's own
    docstring for why that's the right fix here specifically).
  - A mismatch within tolerance is left alone (one fill genuinely in
    flight shouldn't trigger a correction).
  - refresh_interval rate-limits real API calls.
  - A finished market is skipped.
  - A failed/malformed real API response never crashes and never corrects
    off bad data (fail-safe: do nothing this cycle, not "assume zero").
"""

import sys
import unittest
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orderbook_feed import BookStore
from single_venue_reconciler import DEFAULT_TOLERANCE, PositionReconciler

D = Decimal
OLD_UPDATE_TIME = "2026-01-01T00:00:00.000000000Z"


def _iso_seconds_ago(seconds: float) -> str:
    import time
    from datetime import datetime, timezone
    return datetime.fromtimestamp(time.time() - seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"


@dataclass
class _Pair:
    name: str
    poly_market_slug: str


class _FakePositions:
    def __init__(self, net: int = 0):
        self._net = {}
        self._net["test"] = net
        self.set_calls = []

    def get(self, name):
        return self._net.get(name, 0)

    def set(self, name, value):
        self._net[name] = value
        self.set_calls.append((name, value))


class _FakePolyPortfolio:
    def __init__(self, positions_by_slug=None, raise_on_call=False):
        self._positions_by_slug = positions_by_slug or {}
        self.raise_on_call = raise_on_call
        self.call_count = 0

    def positions(self):
        self.call_count += 1
        if self.raise_on_call:
            raise RuntimeError("simulated API failure")
        # Real entries always carry updateTime; default to a long-settled
        # one so tests not about the settle window exercise the normal
        # correction path. Tests about it pass their own.
        return {"positions": {
            slug: {"updateTime": OLD_UPDATE_TIME, **entry} if isinstance(entry, dict) else entry
            for slug, entry in self._positions_by_slug.items()
        }}


class _FakePolyClient:
    def __init__(self, positions=None, raise_on_call=False):
        self.portfolio = _FakePolyPortfolio(positions, raise_on_call)


def _reconciler(poly_client, positions, **overrides) -> PositionReconciler:
    defaults = dict(poly_client=poly_client, positions=positions, store=BookStore(), refresh_interval=0.0)
    defaults.update(overrides)
    return PositionReconciler(**defaults)


class TestPositionDriftCorrection(unittest.TestCase):
    def test_real_position_ahead_of_local_belief_corrects_upward(self):
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "9"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 9)
        self.assertEqual(positions.set_calls, [("test", 9)])

    def test_real_fractional_position_is_corrected_exactly_not_truncated(self):
        # Found live 2026-09-22: Polymarket US can report a genuinely
        # fractional real netPosition. Truncating it here (int(...)) would
        # mean this safety net could itself introduce a small error while
        # "correcting" a real drift -- must correct to the exact value.
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "4.75"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), D("4.75"))

    def test_real_position_behind_local_belief_corrects_downward(self):
        positions = _FakePositions(net=9)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "2"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 2)

    def test_negative_real_position_corrects_correctly(self):
        positions = _FakePositions(net=0)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "-5"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), -5)

    def test_no_real_entry_at_all_treated_as_real_zero(self):
        # No entry -> no updateTime -> needs the mismatch on two reads.
        positions = _FakePositions(net=4)
        poly_client = _FakePolyClient(positions={})  # slug never traded, no entry
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 4)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 0)


class TestFillInFlight(unittest.TestCase):
    """Live 2026-09-24 01:07:47 (Astros/Mariners): a positions read landed
    2ms after a real fill; correcting on it and then applying the fill
    notification 148ms later double-counted the fill (tracker 4, real 2)."""

    PAIR = _Pair(name="test", poly_market_slug="poly-test")

    def test_fresh_venue_change_is_not_corrected(self):
        positions = _FakePositions(net=0)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "2", "updateTime": _iso_seconds_ago(0.1)}})
        _reconciler(poly_client, positions).maybe_check([self.PAIR])
        self.assertEqual(positions.set_calls, [])

    def test_real_sequence_never_double_counts(self):
        positions = _FakePositions(net=0)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "2", "updateTime": _iso_seconds_ago(0.002)}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([self.PAIR])       # read lands just after the fill
        positions._net["test"] += 2               # fill notification arrives
        poly_client.portfolio._positions_by_slug["poly-test"]["updateTime"] = _iso_seconds_ago(3.0)
        reconciler.maybe_check([self.PAIR])       # next read, 3s later
        self.assertEqual(positions.get("test"), 2)
        self.assertEqual(positions.set_calls, [])

    def test_genuinely_missed_fill_is_corrected_once_settled(self):
        positions = _FakePositions(net=0)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "2", "updateTime": _iso_seconds_ago(0.1)}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([self.PAIR])
        self.assertEqual(positions.set_calls, [])
        poly_client.portfolio._positions_by_slug["poly-test"]["updateTime"] = _iso_seconds_ago(2.5)
        reconciler.maybe_check([self.PAIR])
        self.assertEqual(positions.set_calls, [("test", 2)])

    def test_unparseable_update_time_falls_back_to_two_reads(self):
        positions = _FakePositions(net=0)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "2", "updateTime": "not-a-time"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([self.PAIR])
        self.assertEqual(positions.set_calls, [])
        reconciler.maybe_check([self.PAIR])
        self.assertEqual(positions.set_calls, [("test", 2)])

    def test_two_read_fallback_resets_when_the_mismatch_clears(self):
        positions = _FakePositions(net=4)
        poly_client = _FakePolyClient(positions={})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([self.PAIR])       # mismatch seen once
        positions._net["test"] = 0                # the fill arrives, now agrees
        reconciler.maybe_check([self.PAIR])
        positions._net["test"] = 4                # a new, separate mismatch
        reconciler.maybe_check([self.PAIR])       # must NOT correct on its first read
        self.assertEqual(positions.set_calls, [])


class TestWithinTolerance(unittest.TestCase):
    def test_mismatch_within_tolerance_is_not_corrected(self):
        positions = _FakePositions(net=5)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "5"}})  # exact match
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.set_calls, [])

    def test_mismatch_of_one_full_contract_is_corrected(self):
        # DEFAULT_TOLERANCE is 0.5 -- a real 1-contract gap must still trigger.
        positions = _FakePositions(net=5)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "6"}})
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.set_calls, [("test", 6)])


class TestRefreshInterval(unittest.TestCase):
    def test_second_call_within_interval_skips_the_real_api_call(self):
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "9"}})
        reconciler = _reconciler(poly_client, positions, refresh_interval=60.0)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(poly_client.portfolio.call_count, 1)


class TestFailureModes(unittest.TestCase):
    def test_api_exception_does_not_crash_and_does_not_correct(self):
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(raise_on_call=True)
        reconciler = _reconciler(poly_client, positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])  # should not raise
        self.assertEqual(positions.get("test"), 3)  # untouched

    def test_malformed_response_shape_does_not_crash_or_correct(self):
        positions = _FakePositions(net=3)

        class _BadPortfolio:
            def positions(self):
                return {"unexpected": "shape"}

        class _BadClient:
            portfolio = _BadPortfolio()

        reconciler = _reconciler(_BadClient(), positions)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 3)


class TestFinishedMarketSkipped(unittest.TestCase):
    def test_a_closed_market_is_not_corrected(self):
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "9"}})
        store = BookStore()
        book = store.get("polymarket_us", "poly-test")
        book.market_state = "MARKET_STATE_RESOLVED"
        reconciler = _reconciler(poly_client, positions, store=store)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.set_calls, [])

    def test_an_open_market_is_still_corrected(self):
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "9"}})
        store = BookStore()
        book = store.get("polymarket_us", "poly-test")
        book.market_state = "MARKET_STATE_OPEN"
        reconciler = _reconciler(poly_client, positions, store=store)
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 9)

    def test_no_cached_book_yet_is_not_treated_as_finished(self):
        # Uncertainty (no book cached at all) must default to "try to
        # verify", the same fail-safe posture as the cross-venue
        # reconciler's own _either_market_finished.
        positions = _FakePositions(net=3)
        poly_client = _FakePolyClient(positions={"poly-test": {"netPosition": "9"}})
        reconciler = _reconciler(poly_client, positions, store=BookStore())
        reconciler.maybe_check([_Pair(name="test", poly_market_slug="poly-test")])
        self.assertEqual(positions.get("test"), 9)


if __name__ == "__main__":
    unittest.main()
