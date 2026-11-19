"""
Order execution for both venues, behind one interface.

Safety model — read this before wiring anything to `--live`:

  * DryRunExecutor is the default everywhere in this project. It logs what
    it *would* send and returns a fake fill. Nothing reaches either venue.
  * LiveExecutor actually places orders and requires:
      - explicit `--live` on the CLI (see arb_bot.py), AND
      - the env var ARB_BOT_CONFIRM_LIVE=yes set in the shell that launches it
    Both gates exist so a stray flag or a copy-pasted command can't arm real
    trading by accident.
  * There is no partial-fill reconciliation, no retry logic, and no position
    reconciliation against what the venues actually confirm. A cross-venue
    arb leg can fill on one side and miss on the other (one book moved
    between your quote read and your order hitting the matching engine) —
    that leaves you with a naked, unhedged position, not a locked-in profit.
    Treat LiveExecutor as a starting point to harden, not a finished bot.

Kalshi order placement uses the same RSA-PSS request signing as the
WebSocket auth in orderbook_feed.py (KalshiAuth), extended to REST.

Polymarket US order placement uses the official `polymarket_us` package
(pip install polymarket-us) — NOT `py_clob_client`, which is for the
original crypto/on-chain Polymarket.com CLOB, a different product with
incompatible auth (wallet signing vs. key_id/secret_key here).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_UP, Decimal
from typing import Protocol

from orderbook_feed import KalshiAuth

log = logging.getLogger("executor")

KALSHI_REST_BASE = os.environ.get("KALSHI_REST_BASE", "https://external-api.kalshi.com/trade-api/v2")
# The legacy /portfolio/orders endpoint carried a deprecation notice ("no
# earlier than May 6, 2026") in Kalshi's own OpenAPI spec, confirmed past
# due as of 2026-09-15 and confirmed replaced (via
# Kalshi's changelog) by this V2 path -- migrated 2026-09-15. V2 uses a
# flat bid/ask side (YES-denominated only; NO is expressed as ask at
# 1-price, mirroring how Polymarket US already works and how this
# project's own orderbook_feed.KalshiFeed already synthesizes NO asks) and
# fixed-point DOLLAR STRINGS for price/count, not integer cents -- see
# KalshiOrderClient._build_body's docstring for the full mapping. Schema
# read directly from https://docs.kalshi.com/openapi.yaml
# (CreateOrderV2Request/CreateOrderV2Response), not inferred from a
# rendered docs page.
KALSHI_ORDERS_PATH = "/trade-api/v2/portfolio/events/orders"


class KalshiRateLimitedError(RuntimeError):
    """Raised specifically for an HTTP 429 from Kalshi's order endpoint,
    distinct from a generic order rejection -- callers (e.g.
    maker_bot.py's HedgeCoordinator) can special-case this to back off
    before retrying instead of immediately hitting the same limit again."""


@dataclass
class Fill:
    venue: str
    ticker_or_slug: str
    side: str  # "yes" or "no" on both venues now
    price: Decimal
    count: int
    order_id: str
    dry_run: bool


class OrderExecutor(Protocol):
    def buy_kalshi(self, ticker: str, side: str, price: Decimal, count: int) -> Fill: ...
    def buy_polymarket(self, market_slug: str, side: str, price: Decimal, count: int) -> Fill: ...


# --------------------------------------------------------------------------
# Dry run (default)
# --------------------------------------------------------------------------


class DryRunExecutor:
    def buy_kalshi(self, ticker: str, side: str, price: Decimal, count: int) -> Fill:
        log.info("[DRY RUN] would buy %d %s @ %.4f on kalshi:%s", count, side, price, ticker)
        return Fill("kalshi", ticker, side, price, count, order_id="dryrun", dry_run=True)

    def buy_polymarket(self, market_slug: str, side: str, price: Decimal, count: int) -> Fill:
        log.info("[DRY RUN] would buy %d %s @ %.4f on polymarket_us:%s", count, side, price, market_slug)
        return Fill("polymarket_us", market_slug, side, price, count, order_id="dryrun", dry_run=True)


# --------------------------------------------------------------------------
# Live — Kalshi
# --------------------------------------------------------------------------


# Every order this project creates on Polymarket US is placed by a program,
# so it is flagged AUTOMATIC (create-order's manualOrderIndicator field,
# MANUAL_ORDER_INDICATOR_MANUAL / _AUTOMATIC). The docs name no default for
# an omitted field (checked 2026-09-28), so it is always sent explicitly.
POLYMARKET_MANUAL_ORDER_INDICATOR = "MANUAL_ORDER_INDICATOR_AUTOMATIC"


def assert_sane_price(price: Decimal, context: str) -> None:
    """Last line of defense immediately before any order-placing API call:
    reject a nonsensical price outright rather than let it reach a real
    exchange. Cheap, and doesn't rely on every upstream code path (fee
    formulas, fixed-point solves, book walks) having gotten its math right
    in every possible market condition -- belt and suspenders, not a
    substitute for those being correct."""
    if not (Decimal("0") < price < Decimal("1")):
        raise ValueError(f"refusing to place order with insane price {price} ({context})")


class KalshiOrderClient:
    def __init__(self, auth: KalshiAuth, base_url: str = KALSHI_REST_BASE):
        self.auth = auth
        self.base_url = base_url.rstrip("/")

    @staticmethod
    def _build_body(ticker: str, side: str, action: str, price: Decimal, count: int, time_in_force: str) -> dict:
        """Maps this project's venue-neutral (side: yes/no, action: buy)
        vocabulary onto Kalshi V2's request shape, confirmed against
        CreateOrderV2Request in the real OpenAPI schema (docs.kalshi.com/openapi.yaml):

        V2 has no separate yes/no fields at all -- `side` is `bid`/`ask` and
        always refers to the YES leg (`BookSide`'s own description:
        "bid means buy YES, ask means sell YES... but this endpoint quotes
        everything from the YES side"). So buying NO at price `p` is placed
        as `side=ask, price=(1-p)` -- the same complement-price convention
        `orderbook_feed.KalshiFeed` already uses to synthesize NO asks when
        READING the book, now applied to PLACING one too.

        This project only ever buys (never sells) on Kalshi in both bots --
        action='sell' is rejected outright rather than silently mismapped,
        since a sell would need the opposite bid/ask assignment and hasn't
        been reasoned through or tested.

        price/count go over the wire as fixed-point decimal STRINGS
        (FixedPointDollars/FixedPointCount), not integer cents -- another
        real format change in V2, confirmed from the same schema.

        self_trade_prevention_type is a new required V2 field with no
        default; 'taker_at_cross' is used since every order this project
        places on Kalshi is inherently a taker order (neither bot ever
        rests an order on Kalshi itself -- only Polymarket US quotes rest),
        so there is no resting order of ours to prefer keeping alive.

        book_price is quantized to the nearest cent before being sent --
        found live 2026-09-16: a corrective hedge was rejected outright
        with `{"code":"invalid_price"}` because its computed cap (0.5050)
        wasn't a whole cent. The API's own docs say FixedPointDollars
        "accepts 2-4 decimal places," which is about STRING FORMAT, not
        this market's actual price grid (every real Kalshi market seen
        so far uses whole-cent steps) -- this project's own price inputs
        can easily land on a finer value than that grid allows, since
        PositionReconciler's reference price comes from Polymarket US
        (which ticks in half-cents for at least this market) and hedge
        cost is a VWAP division that isn't guaranteed to land on a cent.
        Rounds in whichever direction keeps the EFFECTIVE cap at least as
        conservative as what was computed, never looser: DOWN for a
        direct "yes" bid (a lower cap is a stricter max-buy-price), UP for
        the "no" case's post-complement ask (a higher ask is economically
        a LOWER effective NO cost, same direction of conservatism)."""
        if action != "buy":
            raise ValueError(f"KalshiOrderClient only supports action='buy', got {action!r}")
        if side == "yes":
            book_side, book_price = "bid", price.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        elif side == "no":
            book_side, book_price = "ask", (Decimal("1") - price).quantize(Decimal("0.01"), rounding=ROUND_UP)
        else:
            raise ValueError(f"side must be 'yes' or 'no', got {side!r}")

        return {
            "ticker": ticker,
            "client_order_id": os.urandom(8).hex(),
            "side": book_side,
            "count": str(count),
            "price": str(book_price),
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
        }

    def place_limit_order(self, ticker: str, side: str, action: str, price: Decimal, count: int) -> dict:
        """side: 'yes' or 'no'. action: 'buy' only (see _build_body). price
        is 0-1 in YES-equivalent probability terms regardless of side --
        the NO-side complement conversion happens inside _build_body.
        No time limit is set beyond good-till-canceled, matching the V1
        endpoint's implicit default: an unfilled remainder still rests.
        Returns a response NORMALIZED back into the caller's own side
        convention -- see _normalize_response."""
        assert_sane_price(price, f"kalshi place_limit_order {ticker} {side}")
        body = self._build_body(ticker, side, action, price, count, time_in_force="good_till_canceled")
        return self._normalize_response(self._post_order(body), side)

    def place_ioc_hedge(self, ticker: str, side: str, action: str, max_price: Decimal, count: int) -> dict:
        """Aggressive, bounded-price hedge: an immediate-or-cancel limit at
        the worst price you're willing to accept. Fills whatever's
        available at `max_price` or better right now and cancels the rest.
        Kalshi has no true market order type (confirmed against its docs),
        so this is the standard way to get an immediate, price-bounded fill
        — used by the market-maker strategy (maker_bot.py) to hedge a
        Polymarket US maker fill without unbounded slippage risk. Returns
        a response NORMALIZED back into the caller's own side convention
        -- see _normalize_response."""
        assert_sane_price(max_price, f"kalshi place_ioc_hedge {ticker} {side}")
        body = self._build_body(ticker, side, action, max_price, count, time_in_force="immediate_or_cancel")
        return self._normalize_response(self._post_order(body), side)

    @staticmethod
    def _normalize_response(raw: dict, side: str) -> dict:
        """CreateOrderV2Response is flat (order_id at the top level, no
        {"order": {...}} wrapper) and quotes average_fill_price in
        YES-equivalent terms always, same as the request -- so a NO-side
        hedge's fill price needs converting back to NO terms the same way
        _build_body converts the request, or callers reasoning in this
        project's yes/no convention (everywhere outside this one class)
        would silently misprice a NO fill by (1 - true_price).

        average_fee_paid is documented as PER CONTRACT
        ("Volume-weighted average fee paid per contract"), returned here
        as `avg_fee_per_contract` (not multiplied by fill count) so the
        naming makes that explicit at the call site -- multiply it out
        there, don't accumulate it directly as a total.

        fill_count/remaining_count are fixed-point decimal strings
        (fractional Kalshi contracts are technically supported by V2, down
        to 0.01) rounded UP to a whole int here, same ROUND_CEILING
        convention already used for Polymarket's fractional fills
        elsewhere in this project (erring toward slightly more hedged
        exposure tracked, not less) -- this project still only ever
        REQUESTS whole-contract counts, so a fractional response would be
        unexpected, but the rounding direction stays safe if one occurs."""
        def _to_int(raw_value) -> int:
            if raw_value is None:
                return 0
            return int(Decimal(str(raw_value)).to_integral_value(rounding=ROUND_CEILING))

        fill_count = _to_int(raw.get("fill_count"))
        avg_price = None
        avg_fee_per_contract = None
        if fill_count > 0:
            raw_price = raw.get("average_fill_price")
            if raw_price is not None:
                yes_price = Decimal(str(raw_price))
                avg_price = yes_price if side == "yes" else (Decimal("1") - yes_price)
            raw_fee = raw.get("average_fee_paid")
            if raw_fee is not None:
                avg_fee_per_contract = Decimal(str(raw_fee))

        return {
            "order_id": raw.get("order_id", "unknown"),
            "fill_count": fill_count,
            "remaining_count": _to_int(raw.get("remaining_count")),
            "avg_price": avg_price,
            "avg_fee_per_contract": avg_fee_per_contract,
            "raw": raw,
        }

    def _post_order(self, body: dict) -> dict:
        payload = json.dumps(body).encode()
        headers = self.auth.headers(method="POST", path=KALSHI_ORDERS_PATH)
        headers["Content-Type"] = "application/json"

        req = urllib.request.Request(
            self.base_url + "/portfolio/events/orders", data=payload, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            if e.code == 429:
                raise KalshiRateLimitedError(f"kalshi rate-limited (429): {detail}") from e
            raise RuntimeError(f"kalshi order rejected ({e.code}): {detail}") from e

    def cancel_order(self, order_id: str, ticker: str) -> dict:
        """DELETE /portfolio/events/orders/{order_id} -- schema read
        directly from docs.kalshi.com/openapi.yaml (CancelOrderV2), added
        2026-09-15 to clean up a GTC leg's unfilled remainder (see
        LiveExecutor.buy_kalshi) rather than leaving it resting on Kalshi
        indefinitely, unaccounted for by this project.

        `market_ticker` is passed as a query param for auto-routing --
        confirmed required by the spec's own warning: "an order_id alone
        cannot identify the exchange shard." No request body; DELETE with
        query params only.

        A 404 (already fully filled or already canceled -- e.g. a race
        between this call and the exchange finishing the fill) is treated
        as a harmless no-op, same defensive posture as
        maker_bot.HedgeCoordinator's "cancel regardless, ignore if it's
        already gone" pattern for the Polymarket US side."""
        path = f"/portfolio/events/orders/{order_id}"
        query = urllib.parse.urlencode({"market_ticker": ticker})
        headers = self.auth.headers(method="DELETE", path=f"/trade-api/v2{path}")
        req = urllib.request.Request(f"{self.base_url}{path}?{query}", headers=headers, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            if e.code == 429:
                raise KalshiRateLimitedError(f"kalshi rate-limited (429) on cancel: {detail}") from e
            if e.code == 404:
                log.info("kalshi: cancel for order %s got 404 (already gone) -- treating as a no-op", order_id)
                return {"order_id": order_id, "reduced_by": 0, "ts_ms": None}
            raise RuntimeError(f"kalshi order cancel rejected ({e.code}): {detail}") from e

        reduced_by_raw = raw.get("reduced_by")
        reduced_by = int(Decimal(str(reduced_by_raw)).to_integral_value(rounding=ROUND_CEILING)) if reduced_by_raw is not None else 0
        return {"order_id": raw.get("order_id", order_id), "reduced_by": reduced_by, "ts_ms": raw.get("ts_ms")}


def kalshi_rest_get(auth: KalshiAuth, path: str, params: dict, base_url: str = KALSHI_REST_BASE) -> dict:
    """Shared helper for one-off authenticated Kalshi REST GETs that don't
    fit KalshiOrderClient's order-placing surface (startup reconciliation,
    balance checks). Both arb_bot.py and maker_bot.py use this same
    function so there's exactly one place that builds the auth headers for
    a GET request."""
    import urllib.parse

    query = urllib.parse.urlencode(params)
    url = f"{base_url}{path}?{query}"
    headers = auth.headers(method="GET", path=f"/trade-api/v2{path}")
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


# --------------------------------------------------------------------------
# Live — Polymarket US
# --------------------------------------------------------------------------


class PolymarketUSOrderClient:
    """Thin wrapper around the official `polymarket_us` SDK. Imported lazily
    so the rest of this project works without the dependency installed
    until you actually go live on Polymarket US."""

    _INTENT = {"yes": "ORDER_INTENT_BUY_LONG", "no": "ORDER_INTENT_BUY_SHORT"}

    def __init__(self, key_id: str, secret_key: str):
        try:
            from polymarket_us import PolymarketUS
        except ImportError as e:
            raise RuntimeError("pip install polymarket-us to enable live Polymarket US execution") from e

        self._client = PolymarketUS(key_id=key_id, secret_key=secret_key)

    def buy(self, market_slug: str, side: str, price: Decimal, count: int) -> dict:
        if side not in self._INTENT:
            raise ValueError(f"side must be 'yes' or 'no', got {side!r}")
        assert_sane_price(price, f"polymarket_us buy {market_slug} {side}")
        return self._client.orders.create({
            "marketSlug": market_slug,
            "intent": self._INTENT[side],
            "type": "ORDER_TYPE_LIMIT",
            "price": {"value": str(price), "currency": "USD"},
            "quantity": count,
            "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "manualOrderIndicator": POLYMARKET_MANUAL_ORDER_INDICATOR,
        })


# --------------------------------------------------------------------------
# Live executor combining both
# --------------------------------------------------------------------------


class LiveExecutor:
    def __init__(self, kalshi: KalshiOrderClient, polymarket: PolymarketUSOrderClient):
        self.kalshi = kalshi
        self.polymarket = polymarket

    def buy_kalshi(self, ticker: str, side: str, price: Decimal, count: int) -> Fill:
        """Places a GTC limit order (see KalshiOrderClient.place_limit_order)
        and, since GTC means any unfilled remainder simply rests on Kalshi
        rather than being canceled by the exchange the way an IOC order's
        remainder is, cleans that remainder up immediately with a real
        Cancel Order call (added 2026-09-15) instead of leaving it resting
        indefinitely, unaccounted for by this project. Reports the ACTUAL
        filled count back to the caller (previously this returned the
        REQUESTED count regardless of what really filled, which would have
        silently treated a partial or even a zero-fill GTC leg as if it
        had fully filled -- ArbBot._execute's one-leg-filled/one-leg-failed
        detection only ever catches an outright exception, so a 0-fill
        leg is raised here explicitly to make sure that detection actually
        fires instead of believing a ghost fill occurred).

        Polymarket US's leg (buy_polymarket, below) has no equivalent
        fill-count/cancellation handling yet -- out of scope for this
        change, which was specifically about the newly-available Kalshi
        Cancel Order endpoint, not a full partial-fill audit of both
        venues."""
        resp = self.kalshi.place_limit_order(ticker, side=side, action="buy", price=price, count=count)
        # V2's CreateOrderV2Response is flat (order_id at the top level),
        # unlike the legacy endpoint's {"order": {...}} wrapper. Already
        # normalized by KalshiOrderClient (fill_count/remaining_count as
        # real ints) -- see _normalize_response.
        order_id = resp.get("order_id", "unknown")
        filled = resp.get("fill_count", 0)
        remaining = resp.get("remaining_count", 0)
        log.info("kalshi order placed: %s", resp)

        if remaining > 0:
            log.warning(
                "kalshi: order %s for %s only filled %d/%d immediately -- canceling the resting remainder "
                "rather than leaving it live on the exchange unaccounted for",
                order_id, ticker, filled, count,
            )
            try:
                cancel_resp = self.kalshi.cancel_order(order_id, ticker)
                log.info("kalshi: canceled resting remainder of order %s: %s", order_id, cancel_resp)
            except Exception:
                # The leg already has a real (possibly zero) fill regardless
                # of whether this cleanup call itself succeeds -- log loudly
                # but don't let a failed cancel attempt mask the fill result
                # below, which is what ArbBot._execute actually needs to act on.
                log.exception(
                    "kalshi: failed to cancel resting remainder of order %s -- it may still be live on "
                    "Kalshi; check manually",
                    order_id,
                )

        if filled == 0:
            raise RuntimeError(f"kalshi order {order_id} for {ticker} did not fill at all (0/{count})")

        return Fill("kalshi", ticker, side, price, filled, order_id=order_id, dry_run=False)

    def buy_polymarket(self, market_slug: str, side: str, price: Decimal, count: int) -> Fill:
        resp = self.polymarket.buy(market_slug, side, price, count)
        order_id = resp.get("orderId", resp.get("id", "unknown"))
        log.info("polymarket_us order placed: %s", resp)
        return Fill("polymarket_us", market_slug, side, price, count, order_id=order_id, dry_run=False)


def build_live_executor() -> LiveExecutor:
    """Reads credentials from env and constructs a LiveExecutor. Raises if
    anything required is missing rather than silently falling back."""
    key_id = os.environ["KALSHI_KEY_ID"]
    key_path = os.environ["KALSHI_PRIVATE_KEY_PATH"]
    poly_key_id = os.environ["POLYMARKET_KEY_ID"]
    poly_secret_key = os.environ["POLYMARKET_SECRET_KEY_ID"]

    kalshi = KalshiOrderClient(KalshiAuth(key_id, key_path))
    polymarket = PolymarketUSOrderClient(poly_key_id, poly_secret_key)
    return LiveExecutor(kalshi, polymarket)
