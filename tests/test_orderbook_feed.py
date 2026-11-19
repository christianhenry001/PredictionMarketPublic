"""
Unit tests for orderbook_feed.run_forever's backoff-reset fix, added
2026-09-16 after a live run showed the fill listener stuck reconnecting
with a 30-second delay for the rest of a run, following one early
disconnect -- delay was only ever reset on a "clean exit" that none of
this project's actual feed loops (all `while True`, only ever exit via an
exception) can produce. Real time.sleep/asyncio.sleep are mocked out so
these run instantly regardless of the real delay values involved.
"""

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import orderbook_feed as ob


class _FakeClock:
    def __init__(self, start: float = 0.0):
        self.t = start

    def time(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class _ScriptedFactory:
    """Each call returns a fresh coroutine simulating a connection that
    stays "up" for `durations[i]` seconds (advancing the fake clock) before
    failing. The last entry raises CancelledError instead, to give the
    `while True` loop in run_forever a defined stopping point for the test."""

    def __init__(self, fake_clock: _FakeClock, durations: list[float]):
        self.fake_clock = fake_clock
        self.durations = durations
        self.call_count = 0

    def __call__(self):
        i = self.call_count
        self.call_count += 1
        dt = self.durations[i]

        async def _run():
            self.fake_clock.advance(dt)
            if i == len(self.durations) - 1:
                raise asyncio.CancelledError()
            raise RuntimeError(f"simulated disconnect #{i}")

        return _run()


class TestRunForeverBackoffReset(unittest.TestCase):
    def _run_and_capture_delays(self, factory: _ScriptedFactory, fake_clock: _FakeClock) -> list[float]:
        recorded: list[float] = []

        async def fake_sleep(delay):
            recorded.append(delay)

        with patch.object(ob.time, "time", fake_clock.time), patch("asyncio.sleep", fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(ob.run_forever("test", factory))
        return recorded

    def test_rapid_repeated_failures_keep_climbing(self):
        # Every "connection" only stays up 0.1s -- well under the 60s
        # reset threshold -- so backoff should climb normally: 1, 2, 4, 8.
        fake_clock = _FakeClock()
        factory = _ScriptedFactory(fake_clock, durations=[0.1, 0.1, 0.1, 0.1])
        delays = self._run_and_capture_delays(factory, fake_clock)
        self.assertEqual(delays, [1.0, 2.0, 4.0])  # 4th call raises CancelledError before another sleep

    def test_backoff_caps_at_thirty_without_a_sustained_uptime(self):
        fake_clock = _FakeClock()
        factory = _ScriptedFactory(fake_clock, durations=[0.1] * 8)
        delays = self._run_and_capture_delays(factory, fake_clock)
        self.assertEqual(delays, [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0])

    def test_sustained_uptime_resets_backoff(self):
        # First three disconnects are rapid (climbs to 4.0), then one
        # connection stays up 90s (over the 60s threshold) before failing
        # -- the NEXT delay should be back down to 1.0, not 8.0.
        fake_clock = _FakeClock()
        factory = _ScriptedFactory(fake_clock, durations=[0.1, 0.1, 0.1, 90.0, 0.1])
        delays = self._run_and_capture_delays(factory, fake_clock)
        self.assertEqual(delays, [1.0, 2.0, 4.0, 1.0])

    def test_brief_uptime_just_under_threshold_does_not_reset(self):
        fake_clock = _FakeClock()
        factory = _ScriptedFactory(fake_clock, durations=[0.1, 0.1, 59.9, 0.1])
        delays = self._run_and_capture_delays(factory, fake_clock)
        self.assertEqual(delays, [1.0, 2.0, 4.0])  # not reset -- 59.9s is under the 60.0s threshold


class _FakeReader:
    def __init__(self):
        self.finished = False

    def done(self) -> bool:
        return self.finished


class _FakeMarketsWS:
    """Stands in for the SDK's MarketsWebSocket: `open`/`reader` are flipped
    by the test to simulate the keepalive closing the socket, or the SDK's
    reader task ending with the socket still open."""

    def __init__(self):
        self.open = True
        self._message_task = _FakeReader()
        self.handlers = {}
        self.closed_by_feed = False

    @property
    def is_connected(self) -> bool:
        return self.open

    def on(self, event, cb):
        self.handlers[event] = cb
        return self

    async def connect(self):
        pass

    async def subscribe(self, *args):
        pass

    async def close(self):
        self.closed_by_feed = True


class _FakeClient:
    def __init__(self, ws):
        self.ws = type("WsNamespace", (), {"markets": lambda _self: ws})()


class TestPolymarketFeedConnectionState(unittest.TestCase):
    """Added 2026-09-25 with the single-venue bot's quiet-market fix: the
    quoter no longer treats a few seconds of silence as stale, so the feed
    itself has to notice a dead connection promptly and say so on the
    Book (feed_connected) rather than leave it to a 90s silence timeout."""

    SLUG = "poly-test"

    def _market_data(self):
        return {"marketData": {"marketSlug": self.SLUG,
                               "bids": [{"px": {"value": "0.49"}, "qty": "10"}],
                               "offers": [{"px": {"value": "0.51"}, "qty": "10"}]}}

    def _run_until_raised(self, break_connection):
        store = ob.BookStore()
        feed = ob.PolymarketUSFeed([self.SLUG], store, "k", "s")
        feed.LIVENESS_CHECK_INTERVAL = 0.01
        ws = _FakeMarketsWS()

        async def scenario():
            with patch("polymarket_us.AsyncPolymarketUS", lambda **kw: _FakeClient(ws)):
                task = asyncio.ensure_future(feed.run())
                await asyncio.sleep(0.02)
                ws.handlers["market_data"](self._market_data())
                book = store.books[(ob.PolymarketUSFeed.VENUE, self.SLUG)]
                self.assertTrue(book.feed_connected)
                break_connection(ws)
                with self.assertRaises(RuntimeError):
                    await asyncio.wait_for(task, timeout=1.0)
                return book

        book = asyncio.run(scenario())
        self.assertTrue(ws.closed_by_feed)
        return book

    def test_socket_closed_raises_and_marks_books_disconnected(self):
        def close(ws):
            ws.open = False
        book = self._run_until_raised(close)
        self.assertFalse(book.feed_connected)

    def test_reader_task_ending_with_socket_open_is_caught(self):
        def kill_reader(ws):
            ws._message_task.finished = True
        book = self._run_until_raised(kill_reader)
        self.assertFalse(book.feed_connected)

    def test_next_message_after_reconnect_marks_connected_again(self):
        store = ob.BookStore()
        feed = ob.PolymarketUSFeed([self.SLUG], store, "k", "s")
        feed._on_market_data(self._market_data())
        feed._mark_disconnected()
        book = store.books[(ob.PolymarketUSFeed.VENUE, self.SLUG)]
        self.assertFalse(book.feed_connected)
        feed._on_market_data(self._market_data())
        self.assertTrue(book.feed_connected)


if __name__ == "__main__":
    unittest.main()
