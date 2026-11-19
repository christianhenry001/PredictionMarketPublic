"""
Order posting, cancellation, and fill notification for the market-maker
strategy. Separate from executor.py (the taker bot's executor) because the
shapes involved are genuinely different: resting orders that need
cancel/replace, and a fill listener that pushes notifications asynchronously
rather than something you poll after sending an order.

Safety model — same spirit as executor.py, extended for a strategy that
sits exposed on the book continuously rather than acting instantaneously:

  * ShadowMakerExecutor is the default. It computes what it WOULD quote,
    logs it, and tracks a simulated resting order locally -- nothing is
    ever sent to Polymarket US. This is how the pricing/quoting logic gets
    validated against real, moving books before a single real order goes out.
  * LiveMakerExecutor actually posts/cancels resting orders and requires
    the same two independent opt-ins as the taker bot's LiveExecutor
    (--live and ARB_BOT_CONFIRM_LIVE=yes) — see maker_bot.py.
  * UNTESTED AGAINST THE LIVE SERVICE as of this writing: order posting/
    cancellation and the private fill-notification WebSocket below are
    built from Polymarket US's documented request/response shapes, the
    same way orderbook_feed.py's PolymarketUSFeed was before its market_data
    parsing was verified against a real captured message. That verification
    hasn't been repeated here yet — do not trust this path for real money
    without watching its first live run closely, the same way the market
    data feed was watched.

Kalshi's hedge leg reuses executor.py's KalshiOrderClient.place_ioc_hedge
directly — no new Kalshi-side code needed here.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Protocol

from rate_limit import RateLimitedError, call_with_429_retry

log = logging.getLogger("maker_executor")

# Bound on how many execution IDs the fill listener remembers for dedup --
# oldest evicted first once exceeded, so a long-running process doesn't
# grow this unboundedly. Fills are rare relative to this size in practice.
MAX_SEEN_EXECUTIONS = 10_000

_INTENT = {"yes": "ORDER_INTENT_BUY_LONG", "no": "ORDER_INTENT_BUY_SHORT"}
_REVERSE_INTENT = {v: k for k, v in _INTENT.items()}
_GTC = "TIME_IN_FORCE_GOOD_TILL_CANCEL"


def venue_price(side: str, price: Decimal) -> Decimal:
    """Our side's price -> the yes-denominated price the venue takes (see
    LiveMakerExecutor.post_quote for why a "no" order is sent as 1 - p)."""
    return price if side == "yes" else Decimal(1) - price


def create_order_params(market_slug: str, side: str, price: Decimal, size: int, post_only: bool) -> dict:
    """Body of one resting-quote create: orders.create(), or one entry of
    POST /v1/orders/batched (same shape, per the docs)."""
    if side not in _INTENT:
        raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
    from executor import POLYMARKET_MANUAL_ORDER_INDICATOR
    params = {
        "marketSlug": market_slug,
        "intent": _INTENT[side],
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": str(venue_price(side, price)), "currency": "USD"},
        "quantity": size,
        "tif": _GTC,
        "manualOrderIndicator": POLYMARKET_MANUAL_ORDER_INDICATOR,
    }
    if post_only:
        params["participateDontInitiate"] = True
    return params


def modify_order_params(market_slug: str, side: str, price: Decimal, quantity: int, post_only: bool) -> dict:
    """Body of a size-down modify: orders.modify(), or one entry of POST
    /v1/orders/batched/modify (which adds orderId). Repeats the order's
    price, tif and post-only flag rather than sending only the quantity:
    the venue forwards a modify to the exchange as a cancel-replace (docs,
    2026-09-29), and whether an omitted field keeps its old value or falls
    back to a default isn't documented. The price is the order's own
    unchanged price -- a price change loses time priority anyway, so the bot
    only modifies size. scripts/verify_modify_priority.py sends exactly this
    body, so what it verifies is what the bot sends.

    `quantity` is the new TOTAL order quantity (FIX OrderQty: filled plus
    remaining). The bot only modifies orders with no fills, where total and
    remaining are the same number, so the two readings can't disagree."""
    if side not in _INTENT:
        raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
    params = {
        "marketSlug": market_slug,
        "price": {"value": str(venue_price(side, price)), "currency": "USD"},
        "quantity": quantity,
        "tif": _GTC,
    }
    if post_only:
        params["participateDontInitiate"] = True
    return params


@dataclass
class RestingQuote:
    order_id: str
    market_slug: str
    side: str  # "yes" or "no"
    price: Decimal
    size: int
    # Queue priority (added 2026-09-29, audit items #6/#7): "reset" for a new
    # order (it joined the back of its price level), "preserved" once a
    # size-down modify is confirmed without the exchange re-stamping the
    # order's insertTime, "reset" again if it was re-stamped, "unknown" if the
    # confirmation carried no times. Informational: nothing trades on it.
    queue_priority: str = "reset"
    insert_time: str | None = None  # venue insertTime, once any venue record of the order has shown it


@dataclass
class MakerFill:
    market_slug: str
    side: str
    price: Decimal
    size: Decimal
    order_id: str
    # Stamped the instant this object is constructed -- for a real fill
    # that's the moment PolymarketFillListener's WS callback learns about
    # it (see _on_order_update below), which is exactly the start point
    # HedgeCoordinator.handle_fill needs to measure real fill-to-hedge
    # latency (added 2026-09-17, see README's lever-1 section). Not the
    # exchange's own execution timestamp -- that field isn't confirmed to
    # exist in the real WS payload yet (see the module docstring's other
    # unconfirmed-field notes), so this measures our own reaction time
    # from first learning about the fill, not exchange-side delivery lag.
    detected_at: float = field(default_factory=time.time)


@dataclass
class OrderEvent:
    """A non-fill event from the private order stream (added 2026-09-28):
    the order is gone -- EXECUTION_TYPE_CANCELED / _EXPIRED / _DONE_FOR_DAY
    (kind "gone") -- or was replaced with a new price/size (kind
    "replaced"), or (added 2026-09-29) the exchange accepted it
    (EXECUTION_TYPE_NEW, kind "accepted"), or a REQUEST on a still-working order
    was rejected -- EXECUTION_TYPE_REJECTED whose order is still in a
    working state, e.g. a rejected modify, after which "the existing order
    remains working with prior attributes" (docs) (kind "request_rejected").
    `price` is in our own side's terms (see post_quote on the
    yes-denominated book); side/price/leaves are None when the message
    doesn't carry them. The venue times are the order's insertTime /
    createTime and the execution's transactTime, as sent."""
    kind: str
    exec_type: str
    order_id: str
    market_slug: str | None
    side: str | None
    price: Decimal | None
    leaves: Decimal | None
    reason: str
    state: str | None = None
    insert_time: str | None = None
    create_time: str | None = None
    transact_time: str | None = None


_GONE_EXECUTION_TYPES = frozenset({"EXECUTION_TYPE_CANCELED", "EXECUTION_TYPE_EXPIRED", "EXECUTION_TYPE_DONE_FOR_DAY"})
_FILL_EXECUTION_TYPES = frozenset({"EXECUTION_TYPE_FILL", "EXECUTION_TYPE_PARTIAL_FILL"})
# Order states in which the order is still on the book (or about to be), per
# the docs' ORDER_STATE_* table. A REJECTED execution on an order in one of
# these rejected a request ON the order (modify, cancel), not the order.
WORKING_ORDER_STATES = frozenset({
    "ORDER_STATE_NEW", "ORDER_STATE_PARTIALLY_FILLED", "ORDER_STATE_PENDING_REPLACE",
    "ORDER_STATE_PENDING_CANCEL", "ORDER_STATE_PENDING_NEW", "ORDER_STATE_PENDING_RISK",
})


def _field(obj, name: str):
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def parse_venue_order(order) -> tuple[str | None, str | None, Decimal | None, Decimal | None]:
    """(order_id, our side, price in our side's terms, leaves quantity) from
    a venue Order object -- an order-stream execution's `order` or an
    orders.list() entry. Side comes from `intent` (the field confirmed live,
    see _on_order_update); a "no" order's yes-denominated price is flipped
    to 1 - price, the same conversion post_quote applies going out."""
    order_id = _field(order, "id") or _field(order, "orderId")
    side = _REVERSE_INTENT.get(_field(order, "intent"))
    raw_price = _field(order, "price")
    raw_price = _field(raw_price, "value") if raw_price is not None and not isinstance(raw_price, (str, int, float)) else raw_price
    price = None
    if raw_price is not None and side is not None:
        price = Decimal(str(raw_price))
        if side == "no":
            price = Decimal(1) - price
    leaves = _field(order, "leavesQuantity")
    return (str(order_id) if order_id is not None else None, side, price,
            Decimal(str(leaves)) if leaves is not None else None)


class AmbiguousOrderError(Exception):
    """create-order failed in a way that doesn't prove the order WASN'T
    placed (added 2026-09-28): a timeout or transport error after the
    request may have been sent, or a 5xx. The SDK raises APITimeoutError on
    an httpx read timeout -- i.e. after sending -- and lets other httpx
    transport errors (ReadError, RemoteProtocolError) through unwrapped, so
    the server may have accepted the order. Polymarket US's create-order
    has no client order id to look it up by (checked 2026-09-28), so the
    caller must find it among open orders by side, price, size and time."""

    def __init__(self, market_slug: str, side: str, price: Decimal, size: int, cause: BaseException):
        super().__init__(f"create order outcome unknown ({type(cause).__name__}: {cause})")
        self.market_slug = market_slug
        self.side = side
        self.price = price
        self.size = size
        self.cause = cause
        self.attempted_at = time.time()


def is_ambiguous_order_failure(exc: BaseException) -> bool:
    """True if a failed create-order call may still have placed the order:
    timeouts and connection/transport errors, and 5xx responses. A 4xx is a
    clean rejection -- the order was not placed."""
    try:
        from polymarket_us.errors import APIConnectionError, APIStatusError
    except ImportError:  # SDK not installed (tests); fall through to httpx
        APIConnectionError = APIStatusError = ()
    if APIStatusError and isinstance(exc, APIStatusError):
        return exc.status_code >= 500
    if APIConnectionError and isinstance(exc, APIConnectionError):
        return True
    try:
        import httpx
    except ImportError:
        return False
    return isinstance(exc, httpx.TransportError)


# --------------------------------------------------------------------------
# Batch endpoints (added 2026-09-29, audit item #15)
# --------------------------------------------------------------------------
# POST /v1/orders/batched, /batched/cancel, /batched/modify (docs, read
# 2026-09-29; no SDK version wraps them -- 0.1.2 and 2.0.0 both checked -- so
# they're called through the SDK client's own signed post()):
#   - up to 20 entries; each entry has the single call's body shape (cancel
#     and modify add orderId)
#   - if ANY entry fails request-shape validation, the gateway rejects the
#     whole batch (400) before anything reaches the exchange
#   - per-entry outcomes come on the order stream, not in the response:
#     createdOrderIds are real ids in request order, but canceledOrderIds /
#     modifiedOrderIds only echo the request, and the exchange silently
#     ignores unknown order ids
BATCH_MAX_ENTRIES = 20
_BATCH_PATHS = {"create": "/v1/orders/batched", "cancel": "/v1/orders/batched/cancel",
                "modify": "/v1/orders/batched/modify"}
_INTENTS = frozenset({"ORDER_INTENT_BUY_LONG", "ORDER_INTENT_SELL_LONG", "ORDER_INTENT_BUY_SHORT", "ORDER_INTENT_SELL_SHORT"})
_TIFS = frozenset({"TIME_IN_FORCE_DAY", "TIME_IN_FORCE_GOOD_TILL_CANCEL", "TIME_IN_FORCE_GOOD_TILL_DATE",
                   "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", "TIME_IN_FORCE_FILL_OR_KILL"})


class BatchRejectedError(Exception):
    """A whole batch was refused before reaching the exchange -- by our own
    shape check (`local`), or by the gateway with a 4xx. Nothing in it was
    acted on, so the caller may send the entries as single calls."""

    def __init__(self, kind: str, detail: str, local: bool, cause: BaseException | None = None):
        super().__init__(f"batched {kind} rejected {'locally' if local else 'by the gateway'}: {detail}")
        self.kind = kind
        self.local = local
        self.cause = cause


class AmbiguousBatchError(Exception):
    """A batch request failed in a way that doesn't prove it WASN'T acted on
    (timeout, transport error, 5xx, or a 200 without the created ids). For
    creates, every entry may be live; cancels and modifies are safe to
    resend."""

    def __init__(self, kind: str, cause: BaseException | str):
        super().__init__(f"batched {kind} outcome unknown ({cause})")
        self.kind = kind
        self.cause = cause
        self.attempted_at = time.time()


def _amount_problem(amount) -> str | None:
    if not isinstance(amount, dict) or not isinstance(amount.get("currency"), str):
        return "price must be {value, currency}"
    try:
        value = Decimal(str(amount.get("value")))
    except ArithmeticError:
        return f"price value {amount.get('value')!r} is not a decimal"
    if not isinstance(amount.get("value"), str) or not (0 < value < 1):
        return f"price value {amount.get('value')!r} must be a decimal string in (0, 1)"
    return None


def batch_shape_problem(kind: str, entries: list[dict]) -> str | None:
    """Our own copy of the gateway's request-shape rules for a batch (the
    documented schema), so a bad entry is caught before the request and the
    rest of the batch isn't lost with it. None if the batch looks valid."""
    if not 1 <= len(entries) <= BATCH_MAX_ENTRIES:
        return f"{len(entries)} entries (must be 1-{BATCH_MAX_ENTRIES})"
    for i, e in enumerate(entries):
        where = f"entry {i}"
        if not isinstance(e.get("marketSlug"), str) or not e["marketSlug"]:
            return f"{where}: marketSlug missing"
        if kind in ("cancel", "modify") and (not isinstance(e.get("orderId"), str) or not e["orderId"]):
            return f"{where}: orderId missing"
        if kind == "create":
            if e.get("intent") not in _INTENTS:
                return f"{where}: intent {e.get('intent')!r}"
            if e.get("type") != "ORDER_TYPE_LIMIT":
                return f"{where}: type {e.get('type')!r} (resting quotes are limit orders)"
        if kind in ("create", "modify"):
            if kind == "create" or "price" in e:
                problem = _amount_problem(e.get("price"))
                if problem:
                    return f"{where}: {problem}"
            if kind == "create" or "quantity" in e:
                q = e.get("quantity")
                if isinstance(q, bool) or not isinstance(q, (int, float)) or q <= 0:
                    return f"{where}: quantity {q!r} must be a positive number"
            if "tif" in e and e["tif"] not in _TIFS:
                return f"{where}: tif {e['tif']!r}"
    return None


class MakerOrderClient(Protocol):
    def post_quote(self, market_slug: str, side: str, price: Decimal, size: int) -> RestingQuote: ...
    def cancel_quote(self, quote: RestingQuote) -> None: ...
    # Optional (added 2026-09-29): reduce a resting order's quantity in place.
    # A client whose `confirms_via_stream` is True only SENDS the modify; the
    # order stream's REPLACE confirms it. Otherwise it's done on return.
    # def modify_quote(self, quote: RestingQuote, new_size: int) -> None: ...
    # Optional batch forms (added 2026-09-29): post_quotes / cancel_quotes /
    # modify_quotes, one request each -- see LiveMakerExecutor.


# --------------------------------------------------------------------------
# Shadow (default) — no real order ever posted
# --------------------------------------------------------------------------


class ShadowMakerExecutor:
    """Simulates resting-order bookkeeping locally. Never sends anything to
    Polymarket US. A shadow quote never fills on its own -- if you want to
    exercise the hedge path in shadow mode too, that requires deliberately
    synthesizing a fill, which maker_bot.py does not do automatically."""

    def __init__(self) -> None:
        self._next_id = 0

    def post_quote(self, market_slug: str, side: str, price: Decimal, size: int) -> RestingQuote:
        self._next_id += 1
        order_id = f"shadow-{self._next_id}"
        log.info("[SHADOW] would quote %s %d @ %.4f on polymarket_us:%s (order %s)", side, size, price, market_slug, order_id)
        return RestingQuote(order_id=order_id, market_slug=market_slug, side=side, price=price, size=size)

    def cancel_quote(self, quote: RestingQuote) -> None:
        log.info("[SHADOW] would cancel %s (%s %d @ %.4f on %s)", quote.order_id, quote.side, quote.size, quote.price, quote.market_slug)

    def modify_quote(self, quote: RestingQuote, new_size: int) -> None:
        log.info("[SHADOW] would modify %s (%s @ %.4f on %s) size %d -> %d", quote.order_id, quote.side, quote.price,
                 quote.market_slug, quote.size, new_size)

    # Batch forms: the same bookkeeping one entry at a time (nothing is sent).
    def post_quotes(self, orders: list[tuple[str, str, Decimal, int]]) -> list[RestingQuote]:
        return [self.post_quote(*o) for o in orders]

    def cancel_quotes(self, quotes: list[RestingQuote]) -> None:
        for q in quotes:
            self.cancel_quote(q)

    def modify_quotes(self, items: list[tuple[RestingQuote, int]]) -> None:
        for q, n in items:
            self.modify_quote(q, n)


# --------------------------------------------------------------------------
# Live — Polymarket US resting orders
# --------------------------------------------------------------------------


class LiveMakerExecutor:
    """Posts and cancels real resting orders on Polymarket US. See this
    module's docstring — the request/response shapes here haven't been
    verified against the live service the way the market data feed was."""

    post_only = False  # class default, so instances built without __init__ (tests) stay off
    # Our quotes really rest on the venue's book, so the book we read back
    # contains them. The single-venue quoter subtracts them before pricing
    # off that book (see SingleVenueQuoter.external_book). Paper/shadow
    # clients don't set this, since their quotes never reach the real book.
    posts_to_real_book = True

    budget = None  # rate_limit.ActionBudget, told about every 429 (class default for instances built without __init__)

    def __init__(self, key_id: str, secret_key: str, post_only: bool = False, budget=None):
        """`post_only` (added 2026-09-23, default off so maker_bot.py is
        unchanged) sets participateDontInitiate on every quote: the
        exchange REJECTS an order that would cross instead of filling it
        as the taker. Needed because a crossing guard run against our own
        copy of the book can't be trusted in a fast move -- live Mets run
        1 posted a bid at 0.425 into a real 0.42 ask off a stale local book.

        Verified live the same night (aec-nhl-la-ana-2026-09-23, 1
        contract): a non-crossing post-only order rests normally; a
        crossing one is rejected instantly with zero fill. The create()
        call does NOT raise -- it returns a normal {"id", "executions": []}
        -- and the rejection arrives on the private order stream as
        execution.type EXECUTION_TYPE_REJECTED ("Order may participate
        but not initiate in the market"). Callers must therefore listen
        for it (PolymarketFillListener's on_reject) or they'll believe a
        rejected quote is resting. Canceling an already-rejected order
        returns normally, so a race there is harmless."""
        try:
            from polymarket_us import PolymarketUS
        except ImportError as e:
            raise RuntimeError("pip install polymarket-us to enable live market-making") from e
        self._client = PolymarketUS(key_id=key_id, secret_key=secret_key)
        self.post_only = post_only
        self.budget = budget

    def post_quote(self, market_slug: str, side: str, price: Decimal, size: int) -> RestingQuote:
        if side not in _INTENT:
            raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
        from executor import POLYMARKET_MANUAL_ORDER_INDICATOR, assert_sane_price
        assert_sane_price(price, f"polymarket_us post_quote {market_slug} {side}")
        # Confirmed live 2026-09-19 via a deliberate, isolated test order (1
        # contract, "no", submitted at 0.05 -- an absurdly cheap price meant
        # to be unfillable): it filled INSTANTLY at an effective "no" cost
        # of 0.865. Polymarket US has ONE single book, always denominated
        # in probability-of-YES (see PolymarketUSFeed's docstring, and
        # _quotes_would_cross's already-correct model of this) -- the API's
        # "price" field for a BUY_SHORT ("no") order is in THAT same
        # yes-denominated space, not in our own "cost of no" terms. What we
        # submitted (0.05) was interpreted as "sell YES for at least 0.05"
        # -- absurdly far below the real yes price (~0.135 at the time),
        # so it matched immediately, at a real economic "no" cost of
        # (1 - 0.135) = 0.865, matching the confirmed fill exactly. Every
        # "no" quote this project has EVER posted submitted the wrong
        # number as a direct result -- this explains both last night's
        # "impossible" fill-worse-than-limit incident AND the 28-fill
        # sweep far better than the crossing-guard fix alone did (that fix
        # is still correct and still needed -- see its own docstring --
        # but it was unknowingly comparing against a HYPOTHETICAL
        # correctly-converted submission that this bug meant never
        # actually happened, until now).
        # LOAD-BEARING for outward-rounding safety, not just unit
        # conversion: `price` here has already been floored to the tick
        # grid (single_venue_maker_bot.py's _apply(), via quantize_to_tick)
        # in NO-terms -- this complement conversion is what turns that
        # floor into a CEILING on the real yes-denominated ask actually
        # submitted (verified 2026-09-24: `1 - floor(x, tick) ==
        # ceil(1-x, tick)` exactly, for every real tick size in use --
        # see quantize_to_tick's own docstring for the full derivation and
        # TestOutwardRounding for the locked-in regression test). If this
        # conversion is ever changed, re-verify that identity still holds.
        submitted_price = venue_price(side, price)
        params = create_order_params(market_slug, side, price, size, self.post_only)
        assert params["manualOrderIndicator"] == POLYMARKET_MANUAL_ORDER_INDICATOR
        try:
            # A 429 is a clean rejection (nothing was placed), so retrying the
            # create is safe -- see rate_limit.call_with_429_retry.
            resp = call_with_429_retry(lambda: self._client.orders.create(params),
                                       f"polymarket_us create {side} {size} @ {price} on {market_slug}", self.budget)
        except RateLimitedError:
            raise
        except Exception as exc:
            if is_ambiguous_order_failure(exc):
                log.error(
                    "polymarket_us: create order outcome UNKNOWN (%s: %s) -- %s %d @ %.4f (submitted %.4f) on %s may be live",
                    type(exc).__name__, exc, side, size, price, submitted_price, market_slug,
                )
                raise AmbiguousOrderError(market_slug, side, price, size, exc) from exc
            raise
        order_id = resp.get("orderId", resp.get("id", "unknown"))
        log.info("polymarket_us quote posted: %s (submitted price %.4f for our %s price %.4f)", resp, submitted_price, side, price)
        return RestingQuote(order_id=order_id, market_slug=market_slug, side=side, price=price, size=size)

    def cancel_quote(self, quote: RestingQuote) -> None:
        # cancel() requires marketSlug as a second positional arg (confirmed
        # against the installed polymarket-us SDK source, v0.1.2,
        # CancelOrderParams -- not optional, a plain call with just the
        # order_id raises TypeError). Found 2026-09-15 by reading the SDK
        # source directly rather than guessing further -- this would have
        # crashed on literally every cancel in live mode, which means every
        # safety guard that pulls a quote (staleness, self-crossing, halt,
        # market-close) would have failed to actually cancel anything.
        call_with_429_retry(lambda: self._client.orders.cancel(quote.order_id, {"marketSlug": quote.market_slug}),
                            f"polymarket_us cancel {quote.order_id} on {quote.market_slug}", self.budget)
        log.info("polymarket_us quote canceled: %s", quote.order_id)

    # A modify returns an empty 200; whether it took effect -- and whether the
    # order kept its time priority -- comes on the order stream (REPLACE or
    # REJECTED). See SingleVenueQuoter's pending-modify handling.
    confirms_via_stream = True

    def modify_quote(self, quote: RestingQuote, new_size: int) -> None:
        """Size-down modify at the order's own price (added 2026-09-29,
        audit items #6/#7; see modify_order_params for the body). Raises on
        failure: a 4xx means the venue refused it (the order keeps its old
        size); a timeout / transport error / 5xx leaves the outcome unknown
        (maker_executor.is_ambiguous_order_failure)."""
        params = modify_order_params(quote.market_slug, quote.side, quote.price, new_size, self.post_only)
        call_with_429_retry(lambda: self._client.orders.modify(quote.order_id, params),
                            f"polymarket_us modify {quote.order_id} size {quote.size} -> {new_size} on {quote.market_slug}",
                            self.budget)
        log.info("polymarket_us quote modify sent: %s size %d -> %d (%s)", quote.order_id, quote.size, new_size, params)

    # ---- batch forms (see BATCH_MAX_ENTRIES) ------------------------------

    def _batch(self, kind: str, entries: list[dict], what: str) -> dict:
        """One batch request. Raises BatchRejectedError (our shape check or a
        4xx other than 429: nothing was acted on), AmbiguousBatchError
        (timeout / transport / 5xx: it may have been), RateLimitedError."""
        problem = batch_shape_problem(kind, entries)
        if problem is not None:
            raise BatchRejectedError(kind, problem, local=True)
        try:
            from polymarket_us.errors import APIStatusError
        except ImportError:  # SDK not installed (tests)
            APIStatusError = ()
        try:
            resp = call_with_429_retry(
                lambda: self._client.post(_BATCH_PATHS[kind], body={"orders": entries}, authenticated=True),
                what, self.budget)
        except RateLimitedError:
            raise
        except Exception as exc:
            if APIStatusError and isinstance(exc, APIStatusError) and exc.status_code < 500:
                raise BatchRejectedError(kind, f"{exc.status_code}: {exc}", local=False, cause=exc) from exc
            if is_ambiguous_order_failure(exc):
                raise AmbiguousBatchError(kind, exc) from exc
            raise
        return resp if isinstance(resp, dict) else {}

    def post_quotes(self, orders: list[tuple[str, str, Decimal, int]]) -> list[RestingQuote]:
        """Creates up to BATCH_MAX_ENTRIES quotes in one request (POST
        /v1/orders/batched). Returns them with the venue's ids, in request
        order. Accepted-or-rejected per order comes on the order stream, as
        for a single create."""
        from executor import assert_sane_price
        for slug, side, price, _ in orders:
            assert_sane_price(price, f"polymarket_us post_quotes {slug} {side}")
        entries = [create_order_params(slug, side, price, size, self.post_only) for slug, side, price, size in orders]
        resp = self._batch("create", entries, f"polymarket_us batched create of {len(entries)}")
        ids = resp.get("createdOrderIds")
        if not isinstance(ids, list) or len(ids) != len(entries) or not all(isinstance(i, str) and i for i in ids):
            raise AmbiguousBatchError("create", f"200 without {len(entries)} createdOrderIds: {resp!r}")
        log.info("polymarket_us batched create: %d quotes posted: %s", len(ids), ids)
        return [RestingQuote(order_id=oid, market_slug=slug, side=side, price=price, size=size)
                for oid, (slug, side, price, size) in zip(ids, orders)]

    def cancel_quotes(self, quotes: list[RestingQuote]) -> None:
        """Cancels up to BATCH_MAX_ENTRIES quotes in one request (POST
        /v1/orders/batched/cancel). The response only echoes the ids: the
        order stream's CANCELED events confirm them."""
        entries = [{"orderId": q.order_id, "marketSlug": q.market_slug} for q in quotes]
        self._batch("cancel", entries, f"polymarket_us batched cancel of {len(entries)}")
        log.info("polymarket_us batched cancel sent: %s", [q.order_id for q in quotes])

    def modify_quotes(self, items: list[tuple[RestingQuote, int]]) -> None:
        """Size-down modifies of up to BATCH_MAX_ENTRIES quotes in one
        request (POST /v1/orders/batched/modify; each entry is forwarded as a
        cancel-replace). Confirmed per order by the stream (REPLACE /
        REJECTED)."""
        entries = [{"orderId": q.order_id, **modify_order_params(q.market_slug, q.side, q.price, n, self.post_only)}
                   for q, n in items]
        self._batch("modify", entries, f"polymarket_us batched modify of {len(entries)}")
        log.info("polymarket_us batched modify sent: %s", [(q.order_id, q.size, n) for q, n in items])


# --------------------------------------------------------------------------
# Fill notifications — private WebSocket
# --------------------------------------------------------------------------


class PolymarketFillListener:
    """Subscribes to SUBSCRIPTION_TYPE_ORDER on the private WebSocket and
    calls `on_fill` for every EXECUTION_TYPE_FILL / _PARTIAL_FILL event on
    the given market slugs. Requires credentials -- same client model as
    PolymarketUSFeed's market-data feed, just the private channel instead
    of the public one.

    Callback-driven the same way PolymarketUSFeed is; run() blocks until
    canceled or the connection drops, wrap it with orderbook_feed.run_forever
    for reconnect + backoff."""

    def __init__(
        self, slugs: list[str], key_id: str, secret_key: str, on_fill: Callable[[MakerFill], None],
        on_reject: Callable[[str, str, str], None] | None = None,
        on_order_event: Callable[[OrderEvent], None] | None = None,
    ):
        """`on_reject(order_id, market_slug, reason_text)`, optional (added
        2026-09-23 for post-only quoting -- see LiveMakerExecutor), is
        called for every EXECUTION_TYPE_REJECTED on the subscribed
        markets. Shape confirmed from a live capture.

        `on_order_event(OrderEvent)`, optional (added 2026-09-28), gets
        every cancel / expire / done-for-day / replace execution, including
        unsolicited ones, so a gone order isn't left looking resting. Any
        other unrecognized execution type is logged at WARNING."""
        self.slugs = slugs
        self.key_id = key_id
        self.secret_key = secret_key
        self.on_fill = on_fill
        self.on_reject = on_reject
        self.on_order_event = on_order_event
        self._last_msg_at = 0.0
        # History: was 15.0, then 45.0 (2026-09-17), each time raised after
        # live data showed reconnects clustering almost exactly at the
        # current value -- 15 produced ~15-20s cycles, 45 produced ~45s
        # cycles, on the SAME run, immediately after being raised. That
        # pattern means this was never really a "heartbeat interval"
        # problem: tuning it up only ever moved where the same self-
        # inflicted disconnect happened, it never converged. Root cause
        # (confirmed 2026-09-17 by reading the actual installed `websockets`
        # library, v17.1): the real WS connection already gets protocol-
        # level ping/pong every 20s automatically (`websockets.connect`'s
        # own defaults, ping_interval=20/ping_timeout=20 -- polymarket_us's
        # BaseWebSocket.connect() never overrides them), so a genuinely
        # dead connection would surface via a real `ConnectionClosed`
        # within ~40s regardless of anything measured here. This staleness
        # check was conflating "no APPLICATION-level order/heartbeat
        # message recently" (common and harmless -- most quiet periods
        # have neither) with "the connection might be dead" (a question
        # the library's own ping/pong already answers, more accurately,
        # on its own). See run()'s real close/error handling below, which
        # is now the primary disconnect signal. This is kept only as a
        # generous last-resort catch-all for a failure mode the library's
        # own detection somehow doesn't surface (belt-and-suspenders, not
        # expected to fire in practice) -- not tuned to any particular
        # cadence, since it's no longer trying to guess one.
        #
        # Also, the original worry this constant encoded ("a real fill
        # sitting unhedged for up to N seconds defeats hedging entirely")
        # no longer fully holds regardless of this value's size:
        # PositionReconciler now independently re-verifies both venues'
        # real positions every 3s regardless of this listener's state (see
        # maker_bot.py), so a missed fill here is caught within ~3s either
        # way.
        self.stale_after = 180.0
        self._seen_execution_ids: set[str] = set()
        self._seen_execution_order: deque[str] = deque()
        self._closed_event: object = None  # asyncio.Event, created fresh per run()
        self._closed_reason: str | None = None

    async def run(self) -> None:
        import asyncio

        from polymarket_us import AsyncPolymarketUS

        client = AsyncPolymarketUS(key_id=self.key_id, secret_key=self.secret_key)
        ws = client.ws.private()
        ws.on("order_update", self._on_order_update)
        # Found live 2026-09-16: without this, the listener treated "no
        # order-related event recently" as "the connection might be dead,"
        # but a healthy connection with genuinely no order activity (the
        # common case -- most of a maker bot's own REST-placed quote
        # posts/cancels apparently don't themselves generate a push here)
        # looks IDENTICAL to that from _last_msg_at's perspective alone.
        # The base websocket class already distinguishes a real server
        # heartbeat from an order update (see BaseWebSocket._handle_message);
        # counting it too separates "proof the connection is alive" from
        # "an order-related event happened," which is what stale_after is
        # actually supposed to be measuring. Confirmed live: reconnected
        # ~18 times in the first 32 minutes of the first real --live run,
        # despite constant quote churn via REST and zero actual problems --
        # strong evidence the old check was a false-positive generator, not
        # a real connectivity signal.
        ws.on("heartbeat", self._on_heartbeat)
        # Added 2026-09-17, now the PRIMARY disconnect signal (see
        # stale_after's docstring in __init__ for the full story): the
        # library already emits real "close"/"error" events from its own
        # protocol-level ping/pong detection (BaseWebSocket._message_loop)
        # -- this listener just never listened for them before, relying
        # entirely on a cruder app-level silence heuristic instead. These
        # fire immediately on an actual disconnect, not after waiting up
        # to stale_after seconds to notice.
        self._closed_event = asyncio.Event()
        self._closed_reason = None
        ws.on("close", self._on_close)
        ws.on("error", self._on_error)

        await ws.connect()
        try:
            await ws.subscribe("fill-sub-1", "SUBSCRIPTION_TYPE_ORDER", self.slugs)
            log.info("polymarket_us: subscribed to fills for %d markets", len(self.slugs))
            self._last_msg_at = time.time()
            while True:
                try:
                    await asyncio.wait_for(self._closed_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise RuntimeError(f"polymarket_us fill listener: connection closed ({self._closed_reason})")
                if time.time() - self._last_msg_at > self.stale_after:
                    raise RuntimeError(
                        "polymarket_us fill listener: no order updates received recently "
                        "(long-idle fallback -- the real close/error signal above never fired)"
                    )
        finally:
            await ws.close()

    def _on_close(self) -> None:
        self._closed_reason = "connection closed"
        self._closed_event.set()

    def _on_error(self, err: object) -> None:
        self._closed_reason = f"error: {err}"
        self._closed_event.set()

    def _on_order_update(self, data: dict) -> None:
        self._last_msg_at = time.time()
        log.debug("polymarket_us raw order_update: %s", data)

        execution = data.get("orderSubscriptionUpdate", {}).get("execution")
        if not execution:
            return
        exec_type = execution.get("type", "")
        if exec_type == "EXECUTION_TYPE_REJECTED" and (execution.get("order") or {}).get("state") in WORKING_ORDER_STATES:
            # The order itself is still working: this rejected a request on
            # it (a modify, say), not the order. on_reject would forget it.
            self._dispatch_order_event(data, execution, exec_type)
            return
        if exec_type == "EXECUTION_TYPE_REJECTED":
            order = execution.get("order", {})
            order_id = order.get("id") or order.get("orderId")
            market_slug = order.get("marketSlug") or data.get("marketSlug")
            if self.on_reject is not None and order_id:
                self.on_reject(order_id, market_slug, execution.get("text") or execution.get("orderRejectReason") or "")
            return
        if exec_type in _GONE_EXECUTION_TYPES or exec_type in ("EXECUTION_TYPE_REPLACE", "EXECUTION_TYPE_NEW"):
            self._dispatch_order_event(data, execution, exec_type)
            return
        if exec_type not in _FILL_EXECUTION_TYPES:
            log.warning("polymarket_us: unhandled order-stream execution type %r -- ignoring: %s", exec_type, execution)
            return

        # Dedup on the execution's own id (falling back to tradeId) -- a
        # duplicate or replayed WS notification (reconnect replay, at-
        # least-once delivery, etc.) would otherwise double-hedge a fill
        # that only really happened once. If neither field is present
        # (shouldn't happen per the documented schema, but unconfirmed
        # against a live message), process it anyway rather than drop a
        # real fill just because it can't be deduped.
        exec_id = execution.get("id") or execution.get("tradeId")
        if exec_id is not None:
            if exec_id in self._seen_execution_ids:
                log.info("polymarket_us: duplicate fill notification for execution %s -- ignoring", exec_id)
                return
            self._seen_execution_ids.add(exec_id)
            self._seen_execution_order.append(exec_id)
            if len(self._seen_execution_order) > MAX_SEEN_EXECUTIONS:
                oldest = self._seen_execution_order.popleft()
                self._seen_execution_ids.discard(oldest)
        else:
            log.warning("polymarket_us: fill event has no execution id/tradeId to dedupe on -- processing anyway: %s", data)

        order = execution.get("order", {})
        market_slug = order.get("marketSlug") or data.get("marketSlug")
        # Found live 2026-09-17 (WNBA PHX@POR run): this used to read
        # order.get("side"), guessed here since the docstring flagged the
        # exact field as unconfirmed -- but the real "side" field is the
        # order's buy/sell DIRECTION ("ORDER_SIDE_BUY"/"ORDER_SIDE_SELL",
        # confirmed against a real captured order blob when diagnosing a
        # stray resting order earlier the same night), not our internal
        # yes/no outcome side at all. That wrong value flowed straight
        # into MakerFill.side, and downstream clear_filled_quote's
        # self.active_quotes[side] lookup (only "yes"/"no" keys) crashed
        # with a real KeyError -- which crashed this listener's entire
        # message loop, forcing a reconnect, on EVERY real fill. This
        # bug had been completely latent until tonight: paper trading
        # can't exercise this listener at all, and no prior --live run
        # had ever gotten enough real fills to hit it. The reconciler
        # backstop (see maker_bot.py's PositionReconciler) caught every
        # affected fill within ~3s regardless, but that means the FAST,
        # direct hedge path -- the entire point of tonight's fill-to-
        # hedge latency work -- had never actually fired successfully,
        # for any fill, ever. `intent` is the field this project has
        # already confirmed correct (it's what _build_body sends to
        # CREATE real orders that have posted successfully all night),
        # so reversing that same confirmed mapping is used here instead
        # of guessing at a new field name.
        side = _REVERSE_INTENT.get(order.get("intent"))
        # Found live 2026-09-18: execution.lastPx reported a price 15c
        # WORSE than our own resting order's own limit price for a fill
        # that was confirmed to match that exact order -- impossible on a
        # real exchange (a resting limit order can never fill worse than
        # its own limit). lastPx most likely reflects the market's last
        # trade generally, not specifically this execution. order.avgPx
        # is a real, documented field on Order (matches the same
        # convention Kalshi's avg_fill_price already uses, which this
        # project handles correctly there) -- preferred here, with
        # lastPx kept only as a fallback if avgPx is ever absent. Logging
        # the raw execution alongside this at WARNING (not DEBUG) so if
        # avgPx is ALSO wrong, the very next incident gives concrete
        # proof instead of a fourth round of guessing.
        avg_px = order.get("avgPx", {}).get("value")
        price = avg_px if avg_px is not None else execution.get("lastPx", {}).get("value")
        log.warning(
            "polymarket_us: fill price source check -- using %s (avgPx=%s, lastPx=%s) -- raw execution: %s",
            "avgPx" if avg_px is not None else "lastPx (avgPx missing)",
            avg_px, execution.get("lastPx", {}).get("value"), execution,
        )
        size = execution.get("lastShares")
        order_id = order.get("id") or order.get("orderId")

        if side is None:
            log.warning(
                "polymarket_us: fill event has an unrecognized/missing order intent %r -- cannot "
                "determine yes/no side, dropping (PositionReconciler will still catch the real "
                "exposure this caused): %s",
                order.get("intent"), data,
            )
            return

        if not (market_slug and price is not None and size is not None):
            log.warning("polymarket_us: fill event missing expected fields: %s", data)
            return

        # Confirmed live 2026-09-19 (see post_quote's matching docstring):
        # avgPx/lastPx are reported in Polymarket US's single yes-
        # denominated book, same as the "price" field we submit. For a
        # "no" fill, the real economic cost of "no" is (1 - reported
        # price), not the reported price directly -- mirrors the exact
        # conversion post_quote now applies going the other direction.
        reported_price = Decimal(str(price))
        real_price = reported_price if side == "yes" else Decimal(1) - reported_price

        self.on_fill(MakerFill(
            market_slug=market_slug,
            side=side,  # guaranteed "yes" or "no" -- the None case already returned above
            price=real_price,
            size=Decimal(str(size)),
            order_id=str(order_id),
        ))

    def _dispatch_order_event(self, data: dict, execution: dict, exec_type: str) -> None:
        """Cancel / expire / done-for-day / replace. The docs name no
        cancel-reason field, so the reason is whichever text field is
        present and the raw execution is logged for the record."""
        order = execution.get("order", {}) or {}
        order_id, side, price, leaves = parse_venue_order(order)
        market_slug = order.get("marketSlug") or data.get("marketSlug")
        reason = (execution.get("text") or execution.get("orderRejectReason") or order.get("cancelReason")
                  or order.get("text") or "")
        # NEW (the exchange accepted the order) isn't in the docs' execution
        # type table but arrives for nearly every create -- 1,265 of 1,273
        # posts on the 2026-09-28 NFL run, 40ms p50 after our create returned
        # -- and was logged as "unhandled" at WARNING until 2026-09-29. Only
        # an id and times matter in it, so it logs at DEBUG.
        (log.debug if exec_type == "EXECUTION_TYPE_NEW" else log.info)(
            "polymarket_us: order-stream %s for order %s on %s (reason %r) -- raw execution: %s",
            exec_type, order_id, market_slug, reason, execution)
        if not order_id:
            log.warning("polymarket_us: %s event without an order id -- cannot apply it: %s", exec_type, data)
            return
        kind = {"EXECUTION_TYPE_REPLACE": "replaced", "EXECUTION_TYPE_REJECTED": "request_rejected",
                "EXECUTION_TYPE_NEW": "accepted"}.get(exec_type, "gone")
        event = OrderEvent(
            kind=kind, exec_type=exec_type,
            order_id=order_id, market_slug=market_slug, side=side, price=price, leaves=leaves, reason=reason,
            state=order.get("state"), insert_time=order.get("insertTime"), create_time=order.get("createTime"),
            transact_time=execution.get("transactTime"),
        )
        if self.on_order_event is not None:
            self.on_order_event(event)

    def _on_heartbeat(self) -> None:
        self._last_msg_at = time.time()
        log.debug("polymarket_us fill listener: heartbeat")
