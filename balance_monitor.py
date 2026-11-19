"""
Tracks available capital on both venues and gates trades on it.

Kalshi and Polymarket US are two separate regulated institutions with
separate cash rails -- there is no way to net collateral between them
programmatically, and this project deliberately never requests
withdrawal-capable API access for either venue (a much bigger attack
surface than trading-only access: a compromised trading-only key can place
bad orders, a compromised withdrawal-capable key can drain the account).
So actually moving money between venues to rebalance stays a manual,
human-initiated action, always -- this module's job is narrower: make sure
you find out about an imbalance from a clear alert, not from a rejected
order mid-trade. For the market-maker's hedge leg specifically, a rejected
order isn't just an inconvenience -- it's exactly the naked-position
scenario HedgeCoordinator's halt system exists to catch, so gating on
balance closes a real gap upstream of it.

Kalshi's GET /portfolio/balance is a confirmed real endpoint
(docs.kalshi.com), reporting balance in integer CENTS (matching the
price-in-cents convention used elsewhere in Kalshi's API) -- the "balance"
field name is confirmed, the cents unit is a reasonable inference from
that convention, not independently confirmed for this specific field.

Polymarket US's GET /v1/account/balances is also a confirmed real REST
endpoint (docs.polymarket.us). This project talks to Polymarket US only
through the official SDK, never hand-rolled REST + signing, deliberately,
given no sandbox exists to verify signing against -- but the SDK method
name/response shape below is no longer a guess: corrected 2026-09-15 by
reading the actual installed `polymarket-us` SDK source (v0.1.2,
site-packages), not by trial and error against the live service. The real
call is `client.account.balances()`, returning
`{"balances": [{"currency": ..., "buyingPower": ..., "currentBalance":
..., ...}, ...]}` -- the original guess treated the whole envelope as a
single flat entry (no unwrapping of the "balances" key at all), which
would have made every real balance check report "no recognizable balance
field," which -- since an unknown balance is treated as insufficient
(fail-safe, same posture as maker_bot.py's check_for_existing_state) --
would have made the bot refuse to trade AT ALL despite a perfectly healthy
account. This is source-verified against v0.1.2, not a live-service
confirmation -- re-check against the installed version if it's ever
upgraded.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable

log = logging.getLogger("balance_monitor")

DEFAULT_REFRESH_INTERVAL = 30.0  # seconds -- balances don't need sub-second freshness


def kalshi_get_balance(kalshi_get: Callable[[str, dict], dict]) -> Decimal | None:
    """`kalshi_get(path, params) -> dict` is the same injectable shape as
    maker_bot.check_for_existing_state's kalshi_get (see
    executor._kalshi_rest_get / maker_bot._kalshi_rest_get for the real
    implementation) so this can share it and be tested with fakes."""
    try:
        resp = kalshi_get("/portfolio/balance", {})
    except Exception:
        log.exception("kalshi: balance check failed")
        return None
    cents = resp.get("balance") if isinstance(resp, dict) else getattr(resp, "balance", None)
    if cents is None:
        log.error("kalshi: balance response missing expected 'balance' field: %s", resp)
        return None
    return (Decimal(cents) / 100).quantize(Decimal("0.01"))


def polymarket_get_balance(poly_client) -> Decimal | None:
    """See module docstring -- source-verified against SDK v0.1.2, not a
    live-service confirmation. Prefers 'buyingPower' (how much can
    actually be deployed right now) over 'currentBalance' (which may
    include funds tied up in open positions), and prefers the USD entry if
    the response has multiple currencies."""
    account = getattr(poly_client, "account", None)
    if account is None:
        log.error("polymarket_us: client has no 'account' resource -- cannot check balance")
        return None

    method = getattr(account, "balances", None)
    if method is None:
        log.error(
            "polymarket_us: client.account has no 'balances' method -- this was source-verified against "
            "SDK v0.1.2, so a missing method here means the installed SDK version has changed; re-check "
            "against the currently installed version rather than assuming this is still correct"
        )
        return None
    try:
        resp = method()
    except Exception:
        log.exception("polymarket_us: client.account.balances() failed")
        return None

    # {"balances": [{"currency": ..., "buyingPower": ..., ...}, ...]} --
    # confirmed against GetAccountBalancesResponse in the installed SDK.
    # NOT itself a list -- unwrap the envelope before iterating, unlike the
    # original guess which treated the whole response as one flat entry.
    balances = resp.get("balances") if isinstance(resp, dict) else getattr(resp, "balances", None)
    if not isinstance(balances, list):
        log.error("polymarket_us: balances() response missing expected 'balances' list: %s", resp)
        return None

    for entry in balances:
        currency = entry.get("currency") if isinstance(entry, dict) else getattr(entry, "currency", None)
        if currency is not None and currency != "USD":
            continue
        value = entry.get("buyingPower") if isinstance(entry, dict) else getattr(entry, "buyingPower", None)
        if value is None:
            value = entry.get("currentBalance") if isinstance(entry, dict) else getattr(entry, "currentBalance", None)
        if value is not None:
            return Decimal(str(value))

    log.error("polymarket_us: balances() response had no recognizable balance field: %s", resp)
    return None


@dataclass
class BalanceMonitor:
    """Periodically refreshes both venues' available balance and caches
    the result for cheap, frequent pre-trade checks -- doesn't hit the
    balance API on every single order decision, since balance doesn't move
    that fast and both venues' order endpoints have their own rate limits
    to respect too (see maker_bot.py's MIN_ORDER_ACTION_INTERVAL).

    `min_balance` (an absolute floor per venue) and `min_balance_share`
    (this venue's share of combined capital, e.g. 0.2 = alert if one venue
    ever holds less than 20% of the combined total) both drive alerts;
    `can_afford()` is the hard-gate check callers use before committing to
    a trade."""

    kalshi_get_balance_fn: Callable[[], Decimal | None]
    poly_get_balance_fn: Callable[[], Decimal | None]
    min_balance: Decimal = Decimal("0")
    min_balance_share: Decimal = Decimal("0")
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL
    kalshi_balance: Decimal | None = None
    poly_balance: Decimal | None = None
    _last_refreshed_at: float = 0.0

    def refresh(self) -> None:
        self.kalshi_balance = self.kalshi_get_balance_fn()
        self.poly_balance = self.poly_get_balance_fn()
        self._last_refreshed_at = time.time()
        self._check_alerts()

    def maybe_refresh(self) -> None:
        if time.time() - self._last_refreshed_at >= self.refresh_interval:
            self.refresh()

    def _check_alerts(self) -> None:
        for venue, balance in (("kalshi", self.kalshi_balance), ("polymarket_us", self.poly_balance)):
            if balance is not None and balance < self.min_balance:
                log.critical(
                    "LOW BALANCE: %s has $%.2f, below the floor $%.2f -- rebalancing needed",
                    venue, balance, self.min_balance,
                )

        if self.kalshi_balance is None or self.poly_balance is None or self.min_balance_share <= 0:
            return
        total = self.kalshi_balance + self.poly_balance
        if total <= 0:
            return
        shares = {"kalshi": self.kalshi_balance / total, "polymarket_us": self.poly_balance / total}
        for venue, share in shares.items():
            if share < self.min_balance_share:
                other = "polymarket_us" if venue == "kalshi" else "kalshi"
                log.critical(
                    "BALANCE IMBALANCE: %s holds %.1f%% of combined capital (floor %.1f%%) -- "
                    "consider withdrawing from %s and depositing to %s",
                    venue, share * 100, self.min_balance_share * 100, other, venue,
                )

    def can_afford(self, venue: str, cost: Decimal) -> bool:
        """Hard-gate check against the last REFRESHED (cached) balance --
        call maybe_refresh() on your own cadence first (both bots' periodic
        watchdogs already do). Unknown balance (couldn't verify) is treated
        as insufficient -- fail-safe, consistent with
        maker_bot.check_for_existing_state's design."""
        balance = self.kalshi_balance if venue == "kalshi" else self.poly_balance
        if balance is None:
            log.warning("%s: balance unknown -- treating as insufficient (fail-safe)", venue)
            return False
        return balance >= cost
