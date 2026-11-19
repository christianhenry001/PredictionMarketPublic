"""
Per-venue fee models for arbitrage-edge calculations.

Verified facts:

  Kalshi taker:  ceil(0.07 * C * p * (1-p)) rounded UP to the cent, per fill.
      Confirmed 2026-09-21
      against Kalshi's own real fee-schedule PDF.
  Kalshi maker:  same formula, coefficient 0.0175, same per-fill round-UP
      rule — also confirmed against the same real PDF (2026-09-21), and
      note it only applies to specific listed series (e.g. KXMLBGAME,
      KXNFLGAME) per that PDF's Non-Standard Fees table; the default
      maker multiplier elsewhere on Kalshi is 0 (free).
  Polymarket US taker: 0.0695 * C * p * (1-p), banker's rounding (round-
      half-to-even) to the nearest cent, per fill. Corrected 2026-09-23 —
      real value confirmed directly against docs.polymarket.us/fees
      ("Taker Fee: 0.0695 (6.95%)"); this module previously assumed 0.06.
  Polymarket US maker: no rebate since 2026-10-28 (see POLYMARKET_MAKER_REBATE_RATE). Before then a
      REBATE of 0.0125 * C * p * (1-p), paid to the maker
      (a credit, not a charge). Confirmed 2026-09-23 directly against
      docs.polymarket.us/fees ("Maker Rebate: -0.0125 (1.25% rebate)") —
      this rate itself was already correct; only the taker rate above was
      wrong. Rounding rule for the rebate itself still isn't separately
      confirmed; this module applies the same banker's rounding as the
      taker fee as the best available assumption.

  Both venues charge per FILL, not per order — an order that sweeps several
  price levels accrues a separately-computed fee at each level, not one fee
  on the blended average price. Polymarket additionally caps the running
  total commission across one order's fills at the banker's-rounded
  cumulative EXACT (unrounded) fee-so-far; that cap can only ever REDUCE a
  fill's charge relative to its own naive per-fill rounding, never increase
  it. This corrects for repeated per-fill round-up-ish drift across many
  small fills. Kalshi has no such cap in the facts given — fees there are a
  plain sum of independently-rounded per-fill charges.

Still just a model, not authoritative pricing — re-verify against
https://kalshi.com/docs/fees and https://docs.polymarket.us/fees before
sizing real trades; fee schedules move.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, ROUND_UP, Decimal
from typing import Iterable

Fill = tuple[Decimal, Decimal]  # (price, quantity)

KALSHI_TAKER_FEE_RATE = Decimal("0.07")
KALSHI_MAKER_FEE_RATE = Decimal("0.0175")
POLYMARKET_TAKER_FEE_RATE = Decimal("0.0695")
# 0.0125 until 2026-10-28 22:00 ET; since then Polymarket US pays no maker rebate on
# accounts under $10M monthly volume. Zeroed 2026-11-08.
POLYMARKET_MAKER_REBATE_RATE = Decimal("0")


def _round_up_to_cent(dollars: Decimal) -> Decimal:
    cents = (dollars * 100).to_integral_value(rounding=ROUND_UP)
    return (cents / 100).quantize(Decimal("0.01"))


def _round_half_even_to_cent(dollars: Decimal) -> Decimal:
    cents = (dollars * 100).to_integral_value(rounding=ROUND_HALF_EVEN)
    return (cents / 100).quantize(Decimal("0.01"))


# --------------------------------------------------------------------------
# Single-fill fees
# --------------------------------------------------------------------------


def kalshi_taker_fee(price: Decimal, contracts: Decimal | int) -> Decimal:
    """Fee in dollars for one Kalshi taker fill of `contracts` at `price` (0-1)."""
    if contracts <= 0:
        return Decimal("0")
    raw = KALSHI_TAKER_FEE_RATE * contracts * price * (Decimal(1) - price)
    return _round_up_to_cent(raw)


def kalshi_maker_fee(price: Decimal, contracts: Decimal | int) -> Decimal:
    """Fee in dollars for one Kalshi maker fill. Unused by this bot today
    (it only takes liquidity) — present for when resting orders are added."""
    if contracts <= 0:
        return Decimal("0")
    raw = KALSHI_MAKER_FEE_RATE * contracts * price * (Decimal(1) - price)
    return _round_up_to_cent(raw)


def polymarket_taker_fee(price: Decimal, contracts: Decimal | int) -> Decimal:
    """Fee in dollars for one Polymarket US taker fill, no cumulative cap
    applied (that only makes sense across multiple fills of one order — see
    polymarket_taker_fees_for_fills)."""
    if contracts <= 0:
        return Decimal("0")
    raw = POLYMARKET_TAKER_FEE_RATE * contracts * price * (Decimal(1) - price)
    return _round_half_even_to_cent(raw)


def polymarket_maker_rebate(price: Decimal, contracts: Decimal | int) -> Decimal:
    """Rebate in dollars PAID TO a Polymarket US maker fill (a credit).
    Unused by this bot today — present for when resting orders are added."""
    if contracts <= 0:
        return Decimal("0")
    raw = POLYMARKET_MAKER_REBATE_RATE * contracts * price * (Decimal(1) - price)
    return _round_half_even_to_cent(raw)


# --------------------------------------------------------------------------
# Per-order fees: walk each fill, apply per-fill rounding, apply the cap
# --------------------------------------------------------------------------


def kalshi_taker_fees_for_fills(fills: Iterable[Fill]) -> Decimal:
    """Total taker commission across all fills of one Kalshi order. Plain
    sum of independently-rounded per-fill fees — no cumulative cap."""
    total = Decimal("0")
    for price, qty in fills:
        total += kalshi_taker_fee(price, qty)
    return total.quantize(Decimal("0.01"))


def polymarket_round_trip_cost_rate(entry_price: Decimal, exit_price: Decimal, exit_is_taker: bool) -> Decimal:
    """Real per-contract cost RATE (in probability-point terms, directly
    comparable to a half_spread) of one round trip on Polymarket US:
    entry is ALWAYS a maker fill here (single_venue_maker_bot.py only
    ever enters via a resting quote) -- entry_cost = -MAKER_REBATE_RATE *
    entry_price*(1-entry_price) (negative = a real rebate, i.e. revenue).
    Exit is either the DESIGNED, common case (a passive fill on the
    OPPOSITE resting quote -- also a real maker fill, since both yes and
    no quotes rest simultaneously) or the FALLBACK case (InventoryUnwinder
    actively crossing the spread when the passive exit doesn't happen in
    time -- a real taker fill). Added 2026-09-24 for the fee-floor check
    (single_venue_maker_bot.py's check_fee_floor).

    Uses the EXACT continuous rate formula, not the cent-rounded per-trade
    dollar functions elsewhere in this module -- this is a STRUCTURAL
    "does the spread cover realistic costs" comparison, not a specific
    trade's exact realized P&L, so rounding a single trade's dollar amount
    would just add noise without changing the real answer."""
    entry_cost = -POLYMARKET_MAKER_REBATE_RATE * entry_price * (Decimal(1) - entry_price)
    if exit_is_taker:
        exit_cost = POLYMARKET_TAKER_FEE_RATE * exit_price * (Decimal(1) - exit_price)
    else:
        exit_cost = -POLYMARKET_MAKER_REBATE_RATE * exit_price * (Decimal(1) - exit_price)
    return entry_cost + exit_cost


def polymarket_taker_fees_for_fills(fills: Iterable[Fill]) -> Decimal:
    """Total taker commission across all fills of one Polymarket US order,
    applying the cumulative cap: the running charged total is held at or
    below the banker's-rounded cumulative EXACT (unrounded) fee-so-far. The
    cap only ever reduces a fill's charge below its own naive per-fill
    rounding — it never tops a fill up."""
    running_exact = Decimal("0")
    running_charged = Decimal("0")
    for price, qty in fills:
        if qty <= 0:
            continue
        exact = POLYMARKET_TAKER_FEE_RATE * qty * price * (Decimal(1) - price)
        running_exact += exact
        naive = _round_half_even_to_cent(exact)
        cap = _round_half_even_to_cent(running_exact)
        allowed = cap - running_charged
        charged = naive if naive <= allowed else max(allowed, Decimal("0"))
        running_charged += charged
    return running_charged.quantize(Decimal("0.01"))
