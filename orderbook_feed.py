"""
Dual-venue WebSocket order book feed: Kalshi + Polymarket US.

Maintains local order books from streaming updates and normalizes both venues
into a common probability view so cross-venue comparison is a dict lookup.

Both venues turn out to price a binary market the same way underneath: one
order book, denominated as probability-of-YES, where "buy NO" isn't a
separate instrument but the complementary side of the same book (see
market_pairs.py's docstring). That's why one Book shape serves both venues.

Note: this targets Polymarket US (docs.polymarket.us, CFTC-regulated, one
book per market slug, key_id/secret_key auth) — NOT the original crypto/
on-chain Polymarket.com CLOB (separate YES/NO ERC-1155 tokens, wallet
signing). Those are different products with incompatible APIs; don't mix up
example code you find for one with the other.

Install:
    pip install websockets cryptography polymarket-us python-dotenv

Kalshi credentials:
    Generate an API key in the Kalshi UI. You get a key ID (a UUID) and download
    an RSA private key as a .pem file. Set:
        export KALSHI_KEY_ID=...
        export KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem

Polymarket US credentials:
    Apply for API access at polymarket.us/developer. REST market data
    (book/bbo) is public, but the WebSocket feed used here rejects
    connections without credentials even though it only reads:
        export POLYMARKET_KEY_ID=...
        export POLYMARKET_SECRET_KEY_ID=...

Run:
    python orderbook_feed.py
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger("feed")

KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_WS_DEMO = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
KALSHI_WS_PATH = "/trade-api/ws/v2"

# If nothing arrives on a connection for this long, assume it is wedged and
# tear it down. Polymarket in particular can accept a subscription and then
# silently deliver nothing while the socket stays OPEN.
STALE_AFTER = 45.0


# --------------------------------------------------------------------------
# Book state
# --------------------------------------------------------------------------


@dataclass
class Book:
    """One side-agnostic book. Prices and sizes are Decimals in [0, 1] / contracts."""

    venue: str
    symbol: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)  # price -> size
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    seq: int | None = None
    updated_at: float = 0.0
    # Exchange-provided event time for the most recent update, as given by
    # the venue (raw ISO 8601 string) — None where the venue doesn't expose
    # one. Kalshi's orderbook_snapshot/orderbook_delta messages don't carry
    # one in what's been observed; Polymarket US's market_data does
    # (`transactTime`). Distinct from `updated_at`, which is always our own
    # local receipt time regardless of venue.
    venue_event_time: str | None = None
    # Trade tape, where the venue exposes one -- used by paper_trading.py to
    # infer whether a hypothetical resting order would plausibly have been
    # hit by real market activity. Polymarket US's market_data carries
    # cumulative `sharesTraded` and `lastTradePx` in its stats block; Kalshi
    # doesn't expose an equivalent in what's been observed, so both stay
    # None there.
    last_trade_price: Decimal | None = None
    cumulative_volume: Decimal | None = None
    # Venue-reported market lifecycle state (e.g. "MARKET_STATE_OPEN"),
    # confirmed present in a real captured Polymarket US market_data
    # message. Kalshi's orderbook messages don't carry an equivalent field
    # in what's been observed, so this stays None on the Kalshi side --
    # used by maker_bot.py to proactively pull quotes on a closed/resolved
    # market rather than rely on depth happening to go to zero.
    market_state: str | None = None
    # False once the feed that fills this book has lost its connection,
    # until its first message after reconnecting. Lets a quoter tell "the
    # connection is down" (pull now) apart from "the market is just quiet"
    # (keep quoting) -- updated_at alone can't, since Polymarket US only
    # pushes market_data when the book actually changes and sends no
    # heartbeats (confirmed live 2026-09-25: 45s probe, zero heartbeat
    # messages). Only PolymarketUSFeed maintains it; Kalshi books stay True.
    feed_connected: bool = True
    # Kalshi only (added 2026-11-08): last time an in-sequence message arrived on the
    # subscription this book has been snapshotted on. Kalshi's seq is per subscription, so
    # any in-order message proves this book missed nothing, even when it hasn't changed itself.
    alive_at: float = 0.0

    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        live = [(p, s) for p, s in self.bids.items() if s > 0]
        return max(live, key=lambda x: x[0]) if live else None

    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        live = [(p, s) for p, s in self.asks.items() if s > 0]
        return min(live, key=lambda x: x[0]) if live else None

    def walk_asks(self, target: Decimal) -> list[tuple[Decimal, Decimal]]:
        """Consume resting asks cheapest-first up to `target` total size.
        Returns the (price, size) fills actually available — the total may
        come up short of `target` if depth runs out."""
        fills: list[tuple[Decimal, Decimal]] = []
        remaining = target
        for price in sorted(p for p, s in self.asks.items() if s > 0):
            if remaining <= 0:
                break
            size = self.asks[price]
            take = min(size, remaining)
            fills.append((price, take))
            remaining -= take
        return fills

    def walk_bids(self, target: Decimal) -> list[tuple[Decimal, Decimal]]:
        """Consume resting bids best-first (highest price) up to `target`
        total size. Returns raw bid-side (price, size) pairs — NOT the
        complementary "buy NO" cost; callers doing a NO-side walk convert
        each price to (1 - price) themselves."""
        fills: list[tuple[Decimal, Decimal]] = []
        remaining = target
        for price in sorted((p for p, s in self.bids.items() if s > 0), reverse=True):
            if remaining <= 0:
                break
            size = self.bids[price]
            take = min(size, remaining)
            fills.append((price, take))
            remaining -= take
        return fills

    def touch(self) -> str:
        bb, ba = self.best_bid(), self.best_ask()
        b = f"{bb[0]:.4f}x{bb[1]}" if bb else "-"
        a = f"{ba[0]:.4f}x{ba[1]}" if ba else "-"
        return f"[{self.venue}] {self.symbol}  bid {b}  ask {a}"

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.seq = None


class BookStore:
    """Shared in-memory cache. Both feeds write here; your strategy reads it."""

    def __init__(self) -> None:
        self.books: dict[tuple[str, str], Book] = {}
        self.on_update: Callable[[Book], None] | None = None

    def get(self, venue: str, symbol: str) -> Book:
        key = (venue, symbol)
        if key not in self.books:
            self.books[key] = Book(venue=venue, symbol=symbol)
        return self.books[key]

    def publish(self, book: Book) -> None:
        book.updated_at = time.time()
        if self.on_update:
            try:
                self.on_update(book)
            except Exception:
                log.exception("on_update callback raised")


# --------------------------------------------------------------------------
# Reconnect wrapper
# --------------------------------------------------------------------------


async def run_forever(name: str, coro_factory: Callable[[], object]) -> None:
    """Run a feed coroutine, restarting with capped exponential backoff.

    Delay resets to 1.0 not just on a clean return -- every real caller in
    this project (KalshiFeed.run, PolymarketUSFeed.run,
    PolymarketFillListener.run) is a `while True` loop that only ever
    exits via an exception, so that branch is effectively dead code for
    all of them -- but also after a connection stays up for a meaningful
    stretch before failing again. Without this second path, delay only
    ever climbs, PERMANENTLY, to the 30s cap after the very first
    disconnect of the whole process's life, regardless of how healthy
    every later reconnect is. Confirmed live 2026-09-16: the maker bot's
    fill listener reconnected repeatedly with delay stuck at the 30s cap,
    accounting for over 20% of the run's total elapsed time as "listener
    down" -- one early transient hiccup had permanently degraded reconnect
    speed for the rest of the run, on the channel this project can least
    afford to have blind (a missed fill defeats the whole point of
    hedging immediately)."""
    delay = 1.0
    MIN_UPTIME_TO_RESET_BACKOFF = 60.0  # stayed up this long -> treat the next failure as fresh, not a continuation
    while True:
        started_at = time.time()
        try:
            await coro_factory()
            delay = 1.0  # clean exit, reset (rare in practice, but still correct if it ever happens)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("%s disconnected: %s — reconnecting in %.1fs", name, exc, delay)
            if time.time() - started_at >= MIN_UPTIME_TO_RESET_BACKOFF:
                delay = 1.0
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30.0)


# --------------------------------------------------------------------------
# Kalshi
# --------------------------------------------------------------------------


class KalshiAuth:
    def __init__(self, key_id: str, private_key_path: str) -> None:
        self.key_id = key_id
        with open(private_key_path, "rb") as fh:
            self.private_key = serialization.load_pem_private_key(fh.read(), password=None)

    def headers(self, method: str = "GET", path: str = KALSHI_WS_PATH) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        message = (ts + method + path.split("?")[0]).encode()
        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }


class KalshiFeed:
    """
    Subscribes to orderbook_delta and maintains books.

    Kalshi expresses the book as BIDS ONLY on both sides: yes_dollars_fp is
    buy-YES interest, no_dollars_fp is buy-NO interest. A resting NO bid at
    0.54 is economically a YES offer at 0.46, so we synthesize the YES ask
    side as (1 - no_bid_price). That gives a conventional two-sided book in
    YES terms, which is what you want for cross-venue comparison.
    """

    VENUE = "kalshi"

    def __init__(self, auth: KalshiAuth, tickers: list[str], store: BookStore, url: str = KALSHI_WS):
        self.auth = auth
        self.tickers = tickers
        self.store = store
        self.url = url
        self._msg_id = 0
        self._sid: int | None = None
        self._expected_seq: int | None = None
        self._synced: set[str] = set()

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def run(self) -> None:
        headers = self.auth.headers()
        # websockets >=14 uses additional_headers; older releases use extra_headers.
        try:
            ctx = websockets.connect(self.url, additional_headers=headers, ping_interval=20)
        except TypeError:  # pragma: no cover
            ctx = websockets.connect(self.url, extra_headers=headers, ping_interval=20)

        self._synced: set[str] = set()  # tickers snapshotted on this connection (see Book.alive_at)
        async with ctx as ws:
            await ws.send(json.dumps({
                "id": self._next_id(),
                "cmd": "subscribe",
                "params": {"channels": ["orderbook_delta"], "market_tickers": self.tickers},
            }))
            log.info("kalshi: subscribed to %d markets", len(self.tickers))

            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=STALE_AFTER)
                await self._handle(ws, json.loads(raw))

    async def _handle(self, ws, data: dict) -> None:
        mtype = data.get("type")

        if mtype == "subscribed":
            self._sid = data.get("msg", {}).get("sid", data.get("sid"))
            return

        if mtype == "error":
            raise RuntimeError(f"kalshi error {data.get('msg')}")

        if mtype not in ("orderbook_snapshot", "orderbook_delta"):
            return

        seq = data.get("seq")
        if mtype == "orderbook_snapshot":
            # A snapshot resets the sequence baseline for this subscription.
            self._expected_seq = seq
        elif self._expected_seq is not None and seq != self._expected_seq + 1:
            # Gap. seq is per-subscription, not per-market, so a gap invalidates
            # every book in this sid. Ask for fresh snapshots rather than
            # tearing down the connection.
            log.warning("kalshi: seq gap (expected %s, got %s) — resyncing",
                        self._expected_seq + 1, seq)
            await self._request_snapshot(ws)
            self._expected_seq = None
            self._synced.clear()
            return
        else:
            self._expected_seq = seq

        msg = data["msg"]
        book = self.store.get(self.VENUE, msg["market_ticker"])

        if mtype == "orderbook_snapshot":
            self._synced.add(msg["market_ticker"])
            book.clear()
            for price, size in msg.get("yes_dollars_fp", []) or []:
                book.bids[Decimal(price)] = Decimal(size)
            for price, size in msg.get("no_dollars_fp", []) or []:
                # NO bid at p == YES offer at 1 - p
                book.asks[Decimal(1) - Decimal(price)] = Decimal(size)
        else:
            price = Decimal(msg["price_dollars"])
            delta = Decimal(msg["delta_fp"])
            if msg["side"] == "yes":
                book.bids[price] = book.bids.get(price, Decimal(0)) + delta
                if book.bids[price] <= 0:
                    book.bids.pop(price, None)
            else:
                ask_price = Decimal(1) - price
                book.asks[ask_price] = book.asks.get(ask_price, Decimal(0)) + delta
                if book.asks[ask_price] <= 0:
                    book.asks.pop(ask_price, None)

        book.seq = seq
        now = time.time()
        for ticker in self._synced:
            self.store.get(self.VENUE, ticker).alive_at = now
        self.store.publish(book)

    async def _request_snapshot(self, ws) -> None:
        """get_snapshot re-sends snapshots without modifying the subscription."""
        if self._sid is None:
            raise RuntimeError("kalshi: cannot resync without a sid")
        await ws.send(json.dumps({
            "id": self._next_id(),
            "cmd": "update_subscription",
            "params": {
                "sids": [self._sid],
                "action": "get_snapshot",
                "market_tickers": self.tickers,
            },
        }))


# --------------------------------------------------------------------------
# Polymarket US
# --------------------------------------------------------------------------


class PolymarketUSFeed:
    """
    Market data over the official `polymarket-us` SDK's async WebSocket
    client. One book per market slug, priced as probability-of-YES — same
    shape as Kalshi's book, no YES/NO token split.

    REST market data (gateway.polymarket.us) is public, but the WebSocket
    client rejects connection without key_id/secret_key — confirmed by
    actually running this against the live service; Polymarket US's own
    docs summary claimed the WS was public too, and it isn't. This feed
    needs credentials even though it never trades.

    The SDK's WS client is callback-driven (register handlers, then keep the
    coroutine alive) rather than an async iterator, so staleness can't be
    detected via a recv() timeout like the Kalshi feed does. Instead this
    tracks the last message time itself and raises if it goes quiet, which
    run_forever() turns into a reconnect.

    Confirmed against a live message on 2026-09-11 — the payload really does
    look like the snippet above (top-level marketData.marketSlug/bids/offers,
    each level as {"px": {"value": ..., "currency": "USD"}, "qty": ...}).

    Uses its own staleness threshold rather than the shared STALE_AFTER:
    a live run showed this market going >45s between market_data pushes
    under completely normal conditions (no error, book just wasn't moving),
    which made the shared 45s threshold fire reconnects on a healthy
    connection. Kalshi's own recv-timeout staleness check is unaffected and
    stays at STALE_AFTER — it was observed streaming steadily at that cadence.
    """

    VENUE = "polymarket_us"
    DEFAULT_STALE_AFTER = 90.0
    LIVENESS_CHECK_INTERVAL = 1.0

    def __init__(
        self, slugs: list[str], store: BookStore, key_id: str, secret_key: str,
        stale_after: float = DEFAULT_STALE_AFTER, on_trade: Callable[[dict], None] | None = None,
    ):
        self.slugs = slugs
        self.store = store
        self.key_id = key_id
        self.secret_key = secret_key
        self.stale_after = stale_after
        self._last_msg_at = 0.0
        # Trade stream (added 2026-09-29 for the single-venue sweep pause):
        # subscribed on the same connection only when a callback is given,
        # so a caller without one gets exactly the old subscription. The
        # callback gets each raw "trade" message.
        self.on_trade = on_trade

    async def run(self) -> None:
        from polymarket_us import AsyncPolymarketUS

        client = AsyncPolymarketUS(key_id=self.key_id, secret_key=self.secret_key)
        ws = client.ws.markets()
        ws.on("market_data", self._on_market_data)
        if self.on_trade is not None:
            ws.on("trade", self._on_trade)

        ws.on("error", lambda exc: log.warning("polymarket_us: websocket error: %s", exc))

        await ws.connect()
        try:
            await ws.subscribe("md-sub-1", "SUBSCRIPTION_TYPE_MARKET_DATA", self.slugs)
            if self.on_trade is not None:
                await ws.subscribe("tr-sub-1", "SUBSCRIPTION_TYPE_TRADE", self.slugs)
            log.info("polymarket_us: subscribed to %d markets%s", len(self.slugs),
                     " (book + trades)" if self.on_trade is not None else "")
            self._last_msg_at = time.time()

            while True:
                await asyncio.sleep(self.LIVENESS_CHECK_INTERVAL)
                # Changed 2026-09-25: this loop used to check silence only,
                # so a connection the websockets keepalive had already
                # closed (ping_interval/ping_timeout 20s each by default)
                # went unnoticed for up to stale_after (90s). The SDK's
                # reader task also ENDS -- with the socket still open -- if
                # any market_data callback raises, since its _message_loop
                # catches the exception, emits "error" and returns. Nothing
                # would arrive after that, and nothing would say why.
                if not self._connection_alive(ws):
                    raise RuntimeError("polymarket_us: websocket closed or reader stopped")
                if time.time() - self._last_msg_at > self.stale_after:
                    raise RuntimeError("polymarket_us: no market_data received recently")
        finally:
            self._mark_disconnected()
            await ws.close()

    @staticmethod
    def _connection_alive(ws) -> bool:
        # _message_task is SDK-private (polymarket-us 0.1.2, websocket/base.py);
        # there's no public way to see the reader has stopped.
        reader = getattr(ws, "_message_task", None)
        return ws.is_connected and not (reader is not None and reader.done())

    def _mark_disconnected(self) -> None:
        for slug in self.slugs:
            book = self.store.books.get((self.VENUE, slug))
            if book is not None:
                book.feed_connected = False

    def _on_trade(self, data: dict) -> None:
        # Never let a callback error reach the SDK's reader: it would stop
        # the reader (see run()'s liveness comment) and the book with it.
        try:
            self.on_trade(data)
        except Exception:
            log.exception("polymarket_us: trade callback failed on %s", data)

    def _on_market_data(self, data: dict) -> None:
        self._last_msg_at = time.time()
        log.debug("polymarket_us raw market_data: %s", data)

        market_data = data.get("marketData", data)
        slug = market_data.get("slug") or market_data.get("marketSlug") or data.get("slug")
        if not slug:
            log.warning("polymarket_us: market_data with no identifiable slug: %s", data)
            return

        book = self.store.get(self.VENUE, slug)
        book.feed_connected = True
        book.clear()
        for level in market_data.get("bids", []) or []:
            price = Decimal(level["px"]["value"])
            size = Decimal(level["qty"])
            book.bids[price] = size
        for level in market_data.get("offers", []) or []:
            price = Decimal(level["px"]["value"])
            size = Decimal(level["qty"])
            book.asks[price] = size
        book.venue_event_time = market_data.get("transactTime") or data.get("transactTime")
        book.market_state = market_data.get("state") or book.market_state  # keep the last-known state if this update omits it

        stats = market_data.get("stats") or {}
        last_trade_px = stats.get("lastTradePx", {}).get("value")
        if last_trade_px is not None:
            book.last_trade_price = Decimal(last_trade_px)
        shares_traded = stats.get("sharesTraded")
        if shares_traded is not None:
            book.cumulative_volume = Decimal(shares_traded)

        self.store.publish(book)


# --------------------------------------------------------------------------
# Wiring
# --------------------------------------------------------------------------


def make_printer(store: BookStore) -> Callable[[Book], None]:
    last: dict[tuple[str, str], str] = {}

    def on_update(book: Book) -> None:
        line = book.touch()
        key = (book.venue, book.symbol)
        if last.get(key) != line:  # only print when top-of-book actually moves
            last[key] = line
            # flush explicitly: this is a long-running process, and stdout is
            # fully block-buffered (not line-buffered) whenever it's not a
            # real terminal — e.g. redirected to a log file — so without this
            # lines can sit invisible in the buffer indefinitely instead of
            # showing up as they happen.
            print(line, flush=True)

    return on_update


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    store = BookStore()
    store.on_update = make_printer(store)

    tasks = []

    key_id = os.environ.get("KALSHI_KEY_ID")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    kalshi_tickers = ["KXFED-26DEC-T3.00"]  # replace with live tickers

    if key_id and key_path:
        auth = KalshiAuth(key_id, key_path)
        feed = KalshiFeed(auth, kalshi_tickers, store)
        tasks.append(run_forever("kalshi", feed.run))
    else:
        log.warning("KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH unset — skipping Kalshi")

    poly_key_id = os.environ.get("POLYMARKET_KEY_ID")
    poly_secret_key = os.environ.get("POLYMARKET_SECRET_KEY_ID")
    poly_slugs: list[str] = []  # fill with market slugs from gateway.polymarket.us
    if poly_slugs and poly_key_id and poly_secret_key:
        feed = PolymarketUSFeed(poly_slugs, store, poly_key_id, poly_secret_key)
        tasks.append(run_forever("polymarket_us", feed.run))
    else:
        log.warning("no Polymarket US market slugs/credentials configured — skipping Polymarket US")

    if not tasks:
        log.error("nothing to run; configure at least one venue")
        return

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
