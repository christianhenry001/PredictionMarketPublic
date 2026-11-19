"""KalshiFeed's feed-level liveness stamp (Book.alive_at, added 2026-11-08)."""
import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orderbook_feed import BookStore, KalshiFeed  # noqa: E402


def snapshot(seq, ticker):
    return {"type": "orderbook_snapshot", "seq": seq, "msg": {"market_ticker": ticker, "yes_dollars_fp": [["0.40", "10"]], "no_dollars_fp": []}}


def delta(seq, ticker):
    return {"type": "orderbook_delta", "seq": seq, "msg": {"market_ticker": ticker, "price_dollars": "0.40", "delta_fp": "1", "side": "yes"}}


class _Ws:
    async def send(self, _):
        pass


class AliveStamp(unittest.TestCase):
    def setUp(self):
        self.store = BookStore()
        self.feed = KalshiFeed(auth=None, tickers=["A", "B", "C"], store=self.store)
        self.feed._sid = 1
        self.run_msgs = lambda *ms: [asyncio.run(self.feed._handle(_Ws(), m)) for m in ms]

    def test_a_message_on_one_ticker_marks_every_snapshotted_book_alive(self):
        self.run_msgs(snapshot(1, "A"), snapshot(2, "B"))
        a, b = self.store.get("kalshi", "A"), self.store.get("kalshi", "B")
        a.alive_at = b.alive_at = 0.0
        self.run_msgs(delta(3, "B"))
        self.assertGreater(a.alive_at, 0)
        self.assertGreater(b.alive_at, 0)

    def test_a_book_without_a_snapshot_on_this_connection_is_not_marked(self):
        self.run_msgs(snapshot(1, "A"), delta(2, "A"))
        self.assertEqual(self.store.get("kalshi", "C").alive_at, 0.0)

    def test_a_sequence_gap_unmarks_until_the_next_snapshot(self):
        self.run_msgs(snapshot(1, "A"), snapshot(2, "B"), delta(5, "A"))
        b = self.store.get("kalshi", "B")
        b.alive_at = 0.0
        self.run_msgs(snapshot(6, "A"), delta(7, "A"))
        self.assertEqual(b.alive_at, 0.0)


if __name__ == "__main__":
    unittest.main()
