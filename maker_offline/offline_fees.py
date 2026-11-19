"""Polymarket US fees for offline replays: no maker rebate, taker 0.0695 (schedule of 2026-10-28)."""
from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal

MAKER_REBATE = 0.0
TAKER_COEFFICIENT = Decimal("0.0695")


def taker_fee(contracts: float, price: float) -> float:
    """0.0695 x C x p(1-p), rounded per fill to the cent with ties to even, as the fee page states."""
    c, p = Decimal(str(contracts)), Decimal(str(price))
    return float((TAKER_COEFFICIENT * c * p * (1 - p)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))
