"""
Manual mapping of equivalent markets across Kalshi and Polymarket US.

Both venues price a binary market the same way: one order book, denominated
as probability-of-YES (0-1), where "buy NO" is executed as the complement of
the YES side rather than through a separate instrument:
  - Kalshi: no direct NO asks exist; buying NO crosses the best YES bid at
    the complementary price (synthesized in orderbook_feed.KalshiFeed).
  - Polymarket US: "buy NO at $0.40" is submitted as-is (ORDER_INTENT_BUY_SHORT,
    price 0.40) and the platform executes it as selling YES at $0.60
    internally — see https://docs.polymarket.us/concepts/orders.
So a pair only needs ONE identifier per venue, not separate YES/NO ids.

How to find the IDs:

  Kalshi ticker:
    GET https://external-api.kalshi.com/trade-api/v2/markets?series_ticker=...
    (or read it off the market's URL / the Kalshi UI).

  Polymarket US market slug:
    GET https://gateway.polymarket.us/v1/markets?slug=<slug>  (public, no auth)
    or read it off the market's URL on polymarket.us.

Only add a pair here once you've manually confirmed both venues are actually
resolving the same underlying event, on the same terms, with the same
deadline. A near-miss (different strike, different expiry, different
settlement source) will look like free money and isn't.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class MarketPair:
    name: str  # human-readable label for logs
    kalshi_ticker: str
    poly_market_slug: str
    min_edge: Decimal = Decimal("0.02")  # net-of-fees edge required to act, in probability points
    max_contracts: int = 10  # hard per-opportunity cap; keep small until the pipeline is proven out


# Example pairs only: both markets settled in September 2026. Replace with live ones.
PAIRS: list[MarketPair] = [
    MarketPair(
        name="NYC HIGH TEMP SEPT 16 77F-78F",
        kalshi_ticker="KXHIGHNY-26SEP16-B77.5",
        poly_market_slug="tc-temp-nychigh-2026-09-16-gte77lt78f",
        min_edge=Decimal("0.02"),
        max_contracts=10,
    ),
    MarketPair(
        # Verified 2026-09-15: Kalshi YES 0.23/0.24, Polymarket US YES
        # 0.23/0.25 -- closely aligned already. Same cross-venue
        # settlement-source caveat as the pair above (Kalshi: The Weather
        # Company; Polymarket US: NWS Climatological Report, station KNYC).
        name="NYC HIGH TEMP SEPT 16 79F-80F",
        kalshi_ticker="KXHIGHNY-26SEP16-B79.5",
        poly_market_slug="tc-temp-nychigh-2026-09-16-gte79lt80f",
        min_edge=Decimal("0.02"),
        max_contracts=10,
    ),
]
