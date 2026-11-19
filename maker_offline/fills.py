"""Decides which virtual resting orders fill, trade by trade, under two queue models.

  conservative  we join behind everything visible at our price when the order reaches the venue. A trade
                at our price works through the queue first; we fill only from what's left. A trade through
                our price means the level emptied, so we fill in full.
  optimistic    front of the queue: any trade at or through our price fills us, up to its size.

The queue ahead of us can only shrink: by trades at our price, and when the level's visible size drops
below it (cancels ahead of us). Cancels behind us can't be told apart, so a shrinking level moves us only
once it falls under our estimate, which stays on the pessimistic side.

An order placed deeper than the recorded levels has no visible queue. It is counted as behind the deepest
recorded level's size and flagged queue_known=False, so reports can split those fills out.

Requests take effect after a latency, so a cancel can lose the race with a trade already on its way.
Virtual orders never change the book, so this can't show our own impact: negligible at 1-5 contracts
against touches of thousands, but not zero on thin books.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from maker_offline.tape import Book, Trade

CONSERVATIVE, OPTIMISTIC = "conservative", "optimistic"
EPS = 1e-9


@dataclass
class Order:
    id: str
    side: str                    # "bid" or "ask", on the yes book
    price: float
    size: float
    tag: str = ""
    sent: float = 0.0
    live_at: float = 0.0
    left: float = 0.0
    ahead: float = 0.0
    ahead_at_place: float = 0.0
    queue_known: bool = True
    active: bool = False
    cancel_at: float | None = None
    filled_any: bool = False


@dataclass
class MakerFill:
    t: float
    order_id: str
    side: str
    price: float
    size: float
    tag: str
    ahead_at_place: float
    ahead_at_fill: float
    queue_known: bool
    through: bool                # the trade went through our price


@dataclass
class Counters:
    posts: int = 0
    cancels: int = 0
    rejected: int = 0
    lost_cancel_races: int = 0   # filled while a cancel was on its way
    extra: dict = field(default_factory=dict)


class FillEngine:
    def __init__(self, model: str, on_fill: Callable[[MakerFill], None], cancel_latency: float = 0.028,
                 post_latency: float = 0.028) -> None:
        if model not in (CONSERVATIVE, OPTIMISTIC):
            raise ValueError(model)
        self.model, self.on_fill = model, on_fill
        self.cancel_latency, self.post_latency = cancel_latency, post_latency
        self.orders: dict[str, Order] = {}
        self.book: Book | None = None
        self.counters = Counters()
        self.on_cancelled: Callable[[Order], None] | None = None
        self._next = 0

    def post(self, t: float, side: str, price: float, size: float, tag: str = "", order_id: str | None = None) -> Order:
        self._next += 1
        o = Order(order_id or f"o{self._next}", side, round(price, 4), size, tag, t, t + self.post_latency, size)
        self.orders[o.id] = o
        self.counters.posts += 1
        return o

    def cancel(self, t: float, order_id: str) -> None:
        o = self.orders.get(order_id)
        if o is not None and o.cancel_at is None:
            o.cancel_at = t + self.cancel_latency
            self.counters.cancels += 1

    def resting(self, side: str | None = None) -> list[Order]:
        return [o for o in self.orders.values() if (side is None or o.side == side) and o.cancel_at is None]

    def _activate(self, o: Order) -> None:
        book = self.book
        levels = (book.bids if o.side == "bid" else book.asks) if book else {}
        opposite = (book.best_ask if o.side == "bid" else book.best_bid) if book else None
        if opposite is not None and (o.price >= opposite if o.side == "bid" else o.price <= opposite):
            self.orders.pop(o.id, None)      # post-only: it would have crossed
            self.counters.rejected += 1
            if self.on_cancelled is not None:
                self.on_cancelled(o)
            return
        o.active = True
        if o.price in levels:
            o.ahead = levels[o.price]
        elif not levels:
            o.ahead, o.queue_known = 0.0, False
        else:
            deepest = min(levels) if o.side == "bid" else max(levels)
            beyond = o.price < deepest if o.side == "bid" else o.price > deepest
            if beyond:
                o.ahead, o.queue_known = levels[deepest], False
            else:
                o.ahead = 0.0            # inside the recorded range with nothing there: a new level
        o.ahead_at_place = o.ahead

    def advance(self, t: float) -> None:
        """Applies every post and cancel that has reached the venue by t."""
        for o in list(self.orders.values()):
            if not o.active and o.live_at <= t and o.id in self.orders:
                self._activate(o)
            if o.id in self.orders and o.cancel_at is not None and o.cancel_at <= t:
                self.orders.pop(o.id)
                if self.on_cancelled is not None:
                    self.on_cancelled(o)

    def on_book(self, book: Book) -> None:
        self.advance(book.t)
        self.book = book
        for o in self.orders.values():
            if not o.active or self.model == OPTIMISTIC:
                continue
            levels = book.bids if o.side == "bid" else book.asks
            if o.price in levels:
                if levels[o.price] < o.ahead:
                    o.ahead = levels[o.price]
                    o.queue_known = True
            elif levels:
                deepest = min(levels) if o.side == "bid" else max(levels)
                inside = o.price >= deepest if o.side == "bid" else o.price <= deepest
                if inside:               # the level is gone, so nothing is left ahead of us
                    o.ahead = 0.0

    def on_trade(self, tr: Trade) -> None:
        self.advance(tr.t)
        side = "bid" if tr.taker_sell else "ask"
        ours = [o for o in self.orders.values() if o.active and o.side == side]
        ours.sort(key=lambda o: o.price, reverse=side == "bid")
        vol = tr.qty
        for o in ours:
            if vol <= EPS:
                break
            reached = tr.price <= o.price if side == "bid" else tr.price >= o.price
            if not reached:
                continue
            through = tr.price != o.price
            if self.model == OPTIMISTIC:
                n = min(o.left, vol)
                vol -= n
            elif through:
                n = o.left               # the taker walked past our level, which must have cleared
            else:
                used = min(o.ahead, vol)
                o.ahead -= used
                vol -= used
                n = min(o.left, vol)
                vol -= n
            if n <= EPS:
                continue
            ahead_at_fill = o.ahead
            o.left -= n
            o.filled_any = True
            if o.cancel_at is not None:
                self.counters.lost_cancel_races += 1
            if o.left <= EPS:
                self.orders.pop(o.id)
            self.on_fill(MakerFill(tr.t, o.id, side, o.price, n, o.tag, o.ahead_at_place, ahead_at_fill, o.queue_known, through))
