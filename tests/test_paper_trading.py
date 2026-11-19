"""
Unit tests for paper_trading.py. Expected values computed by running the
code and hand-verifying against fees.py's formulas, same discipline as the
rest of this project's tests.
"""

import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from maker_executor import MakerFill, RestingQuote
from orderbook_feed import BookStore
from paper_trading import PaperExecutor, PaperFillSimulator, PaperKalshiHedgeClient, PaperLedger, _capped_kalshi_fills

D = Decimal


class TestPaperLedger(unittest.TestCase):
    def test_record_fill_accumulates_fees_and_count(self):
        ledger = PaperLedger()
        ledger.record_fill("kalshi", "KX-TEST", "yes", D("0.5"), D("10"), D("0.18"))
        ledger.record_fill("polymarket_us", "poly-test", "no", D("0.4"), D("10"), D("0.15"))
        self.assertEqual(ledger.fill_count, 2)
        self.assertEqual(ledger.total_fees, D("0.33"))

    def test_record_realized_pnl_accumulates(self):
        ledger = PaperLedger()
        ledger.record_realized_pnl("pair-a", D("0.87"))
        ledger.record_realized_pnl("pair-a", D("0.41"))
        self.assertEqual(ledger.realized_pnl, D("1.28"))

    def test_live_ledger_lines_are_not_labelled_paper(self):
        with self.assertLogs("paper_trading", level="INFO") as logs:
            PaperLedger(label="LIVE").record_fill("polymarket_us", "poly-test", "no", D("0.22"), D("6.15"), D("0"))
            PaperLedger(label="LIVE").record_realized_pnl("pair-a", D("0.10"))
            PaperLedger().record_realized_pnl("pair-a", D("0.10"))
        self.assertTrue(logs.output[0].endswith("[LIVE] fill: polymarket_us poly-test buy 6.15 no @ 0.2200 (fee $0.00)"))
        self.assertIn("[LIVE] realized P&L", logs.output[1])
        self.assertIn("[PAPER] realized P&L", logs.output[2])

    def test_persists_jsonl_when_path_given(self):
        import json
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ledger.jsonl"
            ledger = PaperLedger(out_path=path)
            ledger.record_fill("kalshi", "KX-TEST", "yes", D("0.5"), D("10"), D("0.18"))
            lines = path.read_text().strip().splitlines()
            self.assertEqual(len(lines), 1)
            record = json.loads(lines[0])
            self.assertEqual(record["type"], "fill")
            self.assertEqual(record["fee"], "0.18")

    def test_no_path_means_no_file_write_but_still_tracks(self):
        ledger = PaperLedger()  # out_path=None
        ledger.record_fill("kalshi", "KX-TEST", "yes", D("0.5"), D("10"), D("0.18"))
        self.assertEqual(ledger.fill_count, 1)  # doesn't raise, just skips persistence


class TestPaperExecutor(unittest.TestCase):
    def test_buy_kalshi_computes_real_fee_and_records(self):
        ledger = PaperLedger()
        executor = PaperExecutor(ledger)
        fill = executor.buy_kalshi("KX-TEST", "yes", D("0.47"), 50)
        self.assertEqual(fill.venue, "kalshi")
        self.assertEqual(fill.price, D("0.47"))
        self.assertEqual(fill.count, 50)
        self.assertTrue(fill.dry_run)
        # kalshi_taker_fee(0.47, 50) == 0.88, per tests/test_fees.py
        self.assertEqual(ledger.total_fees, D("0.88"))

    def test_buy_polymarket_computes_real_fee_and_records(self):
        ledger = PaperLedger()
        executor = PaperExecutor(ledger)
        executor.buy_polymarket("poly-test", "no", D("0.48"), 50)
        # polymarket_taker_fee(0.48, 50) == 0.87, per tests/test_fees.py --
        # corrected 2026-09-23 for the real 0.0695 Polymarket taker rate
        # (was 0.06, confirmed stale against docs.polymarket.us/fees).
        self.assertEqual(ledger.total_fees, D("0.87"))


class TestCappedKalshiFills(unittest.TestCase):
    def setUp(self):
        store = BookStore()
        self.book = store.get("kalshi", "KX-TEST")
        self.book.asks[D("0.40")] = D("10")
        self.book.asks[D("0.45")] = D("20")
        self.book.asks[D("0.60")] = D("50")
        self.book.bids[D("0.55")] = D("5")
        self.book.bids[D("0.50")] = D("30")

    def test_yes_side_stops_at_cap(self):
        fills = _capped_kalshi_fills(self.book, "yes", 100, D("0.45"))
        self.assertEqual(fills, [(D("0.40"), D("10")), (D("0.45"), D("20"))])

    def test_no_side_stops_at_cap(self):
        # NO cost from bid 0.55 -> 0.45 (within cap); from bid 0.50 -> 0.50 (excluded, over cap 0.48)
        fills = _capped_kalshi_fills(self.book, "no", 100, D("0.48"))
        self.assertEqual(fills, [(D("0.45"), D("5"))])

    def test_target_smaller_than_depth_stops_at_target(self):
        fills = _capped_kalshi_fills(self.book, "yes", 5, D("0.99"))
        self.assertEqual(fills, [(D("0.40"), D("5"))])

    def test_invalid_side_raises(self):
        with self.assertRaises(ValueError):
            _capped_kalshi_fills(self.book, "sideways", 1, D("0.5"))


class TestPaperKalshiHedgeClient(unittest.TestCase):
    def test_full_fill_within_cap(self):
        store = BookStore()
        book = store.get("kalshi", "KX-TEST")
        book.asks[D("0.40")] = D("100")
        ledger = PaperLedger()
        client = PaperKalshiHedgeClient(ledger, book_lookup=lambda t: store.books.get(("kalshi", t)))

        resp = client.place_ioc_hedge("KX-TEST", "yes", "buy", D("0.50"), 10)
        # Flat shape matching executor.KalshiOrderClient._normalize_response
        # (V2 migration, 2026-09-15) -- no more {"order": {...}} wrapper,
        # and avg_price/avg_fee_per_contract are real Decimal objects, not
        # strings (this client already works in its own side convention,
        # unlike the real client which converts from Kalshi's wire format).
        self.assertEqual(resp["fill_count"], 10)
        self.assertEqual(resp["avg_price"], D("0.4000"))
        self.assertEqual(ledger.fill_count, 1)

    def test_no_book_returns_zero_fill(self):
        ledger = PaperLedger()
        client = PaperKalshiHedgeClient(ledger, book_lookup=lambda t: None)
        resp = client.place_ioc_hedge("KX-MISSING", "yes", "buy", D("0.50"), 10)
        self.assertEqual(resp["fill_count"], 0)
        self.assertIsNone(resp["avg_price"])

    def test_price_above_cap_yields_zero_fill(self):
        store = BookStore()
        book = store.get("kalshi", "KX-TEST")
        book.asks[D("0.90")] = D("100")
        ledger = PaperLedger()
        client = PaperKalshiHedgeClient(ledger, book_lookup=lambda t: store.books.get(("kalshi", t)))
        resp = client.place_ioc_hedge("KX-TEST", "yes", "buy", D("0.50"), 10)
        self.assertEqual(resp["fill_count"], 0)
        self.assertEqual(ledger.fill_count, 0)


class _FakeQuoter:
    """Minimal stand-in for maker_bot.MakerQuoter -- only active_quotes is used by PaperFillSimulator."""
    def __init__(self, active_quotes):
        self.active_quotes = active_quotes


class _FakePair:
    def __init__(self, name, poly_market_slug):
        self.name = name
        self.poly_market_slug = poly_market_slug


class TestPaperFillSimulator(unittest.TestCase):
    def setUp(self):
        self.pair = _FakePair("test-pair", "poly-slug")
        self.quote_yes = RestingQuote(order_id="q1", market_slug="poly-slug", side="yes", price=D("0.60"), size=10)
        self.quoter = _FakeQuoter({"yes": self.quote_yes, "no": None})
        self.fills: list[MakerFill] = []
        self.sim = PaperFillSimulator(
            quoters={"test-pair": self.quoter},
            pairs_by_slug={"poly-slug": self.pair},
            on_fill=lambda pair, fill: self.fills.append(fill),
        )

    def _book(self, trade_price, cumulative_volume, bids=None, asks=None):
        store = BookStore()
        book = store.get("polymarket_us", "poly-slug")
        book.last_trade_price = D(trade_price) if trade_price is not None else None
        book.cumulative_volume = D(cumulative_volume) if cumulative_volume is not None else None
        book.bids.update(bids or {})
        book.asks.update(asks or {})
        return book

    def test_first_observation_never_fills_no_baseline_yet(self):
        self.sim.on_polymarket_update(self._book("0.55", "100"))
        self.assertEqual(self.fills, [])

    def test_no_new_volume_no_fill(self):
        self.sim.on_polymarket_update(self._book("0.55", "100"))
        self.sim.on_polymarket_update(self._book("0.55", "100"))  # unchanged
        self.assertEqual(self.fills, [])

    def test_trade_crossing_yes_quote_fills(self):
        self.sim.on_polymarket_update(self._book("0.65", "100"))  # baseline, above our 0.60 bid, no cross
        self.sim.on_polymarket_update(self._book("0.55", "108"))  # prints AT/below our bid -> crosses
        self.assertEqual(len(self.fills), 1)
        fill = self.fills[0]
        self.assertEqual(fill.side, "yes")
        self.assertEqual(fill.price, D("0.60"))  # fills at OUR price, not the trade's price
        self.assertEqual(fill.size, D("8"))  # min(new_volume=8, quote.size=10)

    def test_fill_size_capped_at_quote_size(self):
        self.sim.on_polymarket_update(self._book("0.65", "100"))
        self.sim.on_polymarket_update(self._book("0.55", "150"))  # 50 new volume, quote size is only 10
        self.assertEqual(len(self.fills), 1)
        self.assertEqual(self.fills[0].size, D("10"))

    def test_trade_not_crossing_no_fill(self):
        self.sim.on_polymarket_update(self._book("0.70", "100"))
        self.sim.on_polymarket_update(self._book("0.70", "110"))  # new volume, but price never reaches our 0.60 bid
        self.assertEqual(self.fills, [])

    def test_queue_ahead_fully_absorbs_a_smaller_print(self):
        # 20 already displayed at our exact price when the quote is first
        # observed -- that's assumed ahead of us in the queue.
        self.sim.on_polymarket_update(self._book("0.65", "100", bids={D("0.60"): D("20")}))
        self.sim.on_polymarket_update(self._book("0.55", "115"))  # 15 new volume, crosses, but < 20 ahead
        self.assertEqual(self.fills, [])
        self.assertEqual(self.sim._queue_ahead["q1"], D("5"))  # 20 - 15 remaining ahead of us

    def test_queue_ahead_partially_consumed_then_fills_the_remainder(self):
        self.sim.on_polymarket_update(self._book("0.65", "100", bids={D("0.60"): D("20")}))
        self.sim.on_polymarket_update(self._book("0.55", "115"))  # 15 volume, absorbed, none for us yet
        self.sim.on_polymarket_update(self._book("0.55", "125"))  # 10 more volume: 5 clears the queue, 5 is ours
        self.assertEqual(len(self.fills), 1)
        self.assertEqual(self.fills[0].size, D("5"))
        self.assertEqual(self.sim._queue_ahead["q1"], D("0"))

    def test_queue_position_captured_once_not_recomputed_each_update(self):
        self.sim.on_polymarket_update(self._book("0.65", "100", bids={D("0.60"): D("20")}))
        self.assertEqual(self.sim._queue_ahead["q1"], D("20"))
        # Even if displayed size at our price changes later, our tracked
        # position shouldn't reset to the new value.
        self.sim.on_polymarket_update(self._book("0.65", "105", bids={D("0.60"): D("999")}))
        self.assertEqual(self.sim._queue_ahead["q1"], D("20"))

    def test_stale_queue_entries_pruned_when_quote_replaced(self):
        self.sim.on_polymarket_update(self._book("0.65", "100", bids={D("0.60"): D("20")}))
        self.assertIn("q1", self.sim._queue_ahead)
        # Simulate MakerQuoter replacing the quote with a new order_id.
        self.quoter.active_quotes["yes"] = RestingQuote(order_id="q2", market_slug="poly-slug", side="yes", price=D("0.61"), size=10)
        self.sim.on_polymarket_update(self._book("0.65", "105"))
        self.assertNotIn("q1", self.sim._queue_ahead)

    def test_no_quote_active_no_fill(self):
        empty_quoter = _FakeQuoter({"yes": None, "no": None})
        sim = PaperFillSimulator({"test-pair": empty_quoter}, {"poly-slug": self.pair}, lambda p, f: self.fills.append(f))
        sim.on_polymarket_update(self._book("0.65", "100"))
        sim.on_polymarket_update(self._book("0.50", "110"))
        self.assertEqual(self.fills, [])

    def test_unrecognized_slug_ignored(self):
        store = BookStore()
        book = store.get("polymarket_us", "unknown-slug")
        book.last_trade_price = D("0.5")
        book.cumulative_volume = D("10")
        self.sim.on_polymarket_update(book)  # should not raise
        self.assertEqual(self.fills, [])


if __name__ == "__main__":
    unittest.main()
