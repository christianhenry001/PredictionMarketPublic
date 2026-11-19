"""
Request budget and 429 handling for Polymarket US (added 2026-09-28,
audit item #20; limits re-checked and lowered the same day).

Documented limits (docs.polymarket.us, re-read 2026-09-28):
  - Retail REST API (what this bot uses): 20 requests per second PER API KEY,
    across ALL authenticated endpoints -- order creates and cancels, and the
    open-order and position reads the reconcilers make. No per-endpoint
    limits and no burst allowance are documented for it.
  - On excess: 429 {"code": 8, "message": "rate limit exceeded"}. No
    Retry-After header is documented; the guidance is "Stop making requests
    immediately. Wait 1 second before retrying. Implement exponential
    backoff for repeated 429s." (We still honor a Retry-After if one comes.)
  - The per-endpoint tables on the trader-guide page (InsertOrder 30/s,
    SearchOrders 12/min, ...) are for the institutional REST/gRPC/FIX APIs,
    not this one. "Global Rate Limit Exceeded" rejects after 5s of latency
    are a latency stopgap, not a rate limit -- don't throttle on them.
So one budget is shared by every caller in the process: a global bucket,
plus one bucket per market so a single busy market can't take all of it.

Token buckets: a bucket of capacity C refilling at r tokens/s lets through
at most C + r calls in any one-second window (C + 5r in five seconds).
  global  C=10, r=6/s  -> <= 16 budgeted calls in any 1s
  market  C=10, r=5/s  -> <= 15 budgeted calls in any 1s
That leaves >= 4/s below the documented 20 for the calls the budget always
lets through: risk cancels (sent even with no tokens, see Priority), the
reconcilers' reads (positions every 3s, open orders every 15s), unwinds,
self-match checks and 429 retries. A hard guarantee would need risk cancels
to wait for the window, which this deliberately doesn't do. C=10 still fits
a whole two-sided 3-rung ladder post (6 orders + the 2-token reserve).

Measured (2026-09-28): replaying recorded sessions showed unbudgeted bursts
well above the documented limit; with this budget the busiest second stays
at or under 16 calls. The one block this account has hit was not a
request-rate limit: a Cloudflare 403 on repeated cancels of a single order,
while other calls kept working.

Priorities when tokens are short:
  RISK_CANCEL     always sent -- a cancel that reduces risk now (retreat,
                  pull, guard); it still consumes tokens, going into debt
                  that later actions repay
  CANCEL          any other cancel (an advance's cancel, a rung reshuffle):
                  needs its tokens
  POST            a post that doesn't improve price (a repost after a
                  retreat, a same-price top-up): needs its tokens plus
                  POST_RESERVE left over
  IMPROVING_POST  a post more aggressive than what was resting, or a fresh
                  side: needs its tokens plus IMPROVING_POST_RESERVE
The reserves keep headroom for the cancels that may have to follow. After a
429, only RISK_CANCEL goes out until the back-off (see note_rate_limited)
has passed.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from enum import IntEnum

log = logging.getLogger("rate_limit")

DOCUMENTED_REQUESTS_PER_SECOND = 20
GLOBAL_BURST = 10.0
GLOBAL_RATE_PER_S = 6.0
MARKET_BURST = 10.0
MARKET_RATE_PER_S = 5.0
# Tokens a post must leave behind: headroom for the cancels a later reprice
# may need. Small, because risk cancels are sent even into debt anyway.
POST_RESERVE = 1.0
IMPROVING_POST_RESERVE = 2.0

# 429 handling, per the documented guidance: wait 1s (RETRY_BASE_SECONDS, or
# the Retry-After if the venue sends one) plus up to RETRY_JITTER_FRACTION,
# then retry once (RETRY_ATTEMPTS = 2 calls). Order calls are synchronous in
# the event loop, so nothing longer than MAX_INLINE_WAIT_SECONDS is slept out
# inline: the call fails with RateLimitedError instead. Every 429 also starts
# a budget back-off that doubles for repeated 429s within BACKOFF_WINDOW_
# SECONDS (1s, 2s, 4s ... up to MAX_BACKOFF_SECONDS), holding everything but
# risk cancels.
RETRY_ATTEMPTS = 2
RETRY_BASE_SECONDS = 1.0
RETRY_JITTER_FRACTION = 0.25
MAX_INLINE_WAIT_SECONDS = 2.0
BACKOFF_WINDOW_SECONDS = 60.0
MAX_BACKOFF_SECONDS = 30.0


class Priority(IntEnum):
    RISK_CANCEL = 0
    CANCEL = 1
    POST = 2
    IMPROVING_POST = 3


_RESERVE = {Priority.CANCEL: 0.0, Priority.POST: POST_RESERVE, Priority.IMPROVING_POST: IMPROVING_POST_RESERVE}


@dataclass
class TokenBucket:
    capacity: float
    rate_per_s: float
    tokens: float = None  # starts full
    updated: float | None = None

    def __post_init__(self):
        if self.tokens is None:
            self.tokens = self.capacity

    def _refill(self, now: float) -> None:
        if self.updated is not None and now > self.updated:
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate_per_s)
        if self.updated is None or now > self.updated:
            self.updated = now

    def available(self, now: float) -> float:
        self._refill(now)
        return self.tokens

    def consume(self, n: float, now: float) -> None:
        """Always takes the tokens -- may go negative (debt)."""
        self._refill(now)
        self.tokens -= n


@dataclass
class ActionBudget:
    """Shared by every quoter and every REST caller in one process. The
    caller asks `allows()` before sending deferrable actions, and calls
    `consume()` for every request actually sent (order actions and reads
    alike). Thread-safe (added 2026-09-29): with ASYNC_ORDER_CALLS the order
    calls -- and call_with_429_retry's note_rate_limited / consume -- run in
    worker threads while the event loop keeps asking allows()."""

    global_bucket: TokenBucket = field(default_factory=lambda: TokenBucket(GLOBAL_BURST, GLOBAL_RATE_PER_S))
    market_burst: float = MARKET_BURST
    market_rate_per_s: float = MARKET_RATE_PER_S
    clock: object = time.monotonic
    markets: dict = field(default_factory=dict)
    cooloff_until: float = 0.0
    deferred: Counter = field(default_factory=Counter)  # Priority name -> actions refused (a retried action counts again)
    refusals: int = 0  # allows() calls answered False
    forced_over_budget: int = 0  # risk cancels sent with no tokens left
    rate_limited: int = 0  # 429 responses seen
    recent_429s: list = field(default_factory=list)  # times of 429s within BACKOFF_WINDOW_SECONDS
    consumed: int = 0
    _lock: object = field(default_factory=threading.RLock, repr=False, compare=False)

    def _market(self, slug: str | None) -> TokenBucket | None:
        if slug is None:
            return None
        if slug not in self.markets:
            self.markets[slug] = TokenBucket(self.market_burst, self.market_rate_per_s)
        return self.markets[slug]

    def tokens(self, slug: str | None = None) -> float:
        with self._lock:
            return self._tokens(slug)

    def _tokens(self, slug: str | None) -> float:
        now = self.clock()
        available = self.global_bucket.available(now)
        market = self._market(slug)
        if market is not None:
            available = min(available, market.available(now))
        return available

    def allows(self, slug: str | None, n: int, priority: Priority, committed: int = 0) -> bool:
        """True if `n` more requests at `priority` may go now, on top of
        `committed` already approved in the same batch but not yet sent.
        RISK_CANCEL is always True (counted in forced_over_budget if tokens
        were short). A False is counted in `deferred` -- the caller must not
        send."""
        if n <= 0:
            return True
        with self._lock:
            return self._allows(slug, n, priority, committed)

    def _allows(self, slug: str | None, n: int, priority: Priority, committed: int) -> bool:
        available = self._tokens(slug) - committed
        if priority == Priority.RISK_CANCEL:
            if available < n:
                self.forced_over_budget += 1
            return True
        if self.clock() < self.cooloff_until or available < n + _RESERVE[priority]:
            self.deferred[priority.name] += n
            self.refusals += 1
            return False
        return True

    def consume(self, slug: str | None, n: int = 1) -> None:
        with self._lock:
            self._consume(slug, n)

    def _consume(self, slug: str | None, n: int) -> None:
        now = self.clock()
        self.global_bucket.consume(n, now)
        market = self._market(slug)
        if market is not None:
            market.consume(n, now)
        self.consumed += n

    def note_rate_limited(self, retry_after: float | None) -> None:
        """A 429 came back: hold all but risk cancels for the Retry-After, or
        without one for RETRY_BASE_SECONDS doubled per earlier 429 within
        BACKOFF_WINDOW_SECONDS (1s, 2s, 4s ... MAX_BACKOFF_SECONDS)."""
        with self._lock:
            self._note_rate_limited(retry_after)

    def _note_rate_limited(self, retry_after: float | None) -> None:
        now = self.clock()
        self.rate_limited += 1
        self.recent_429s = [t for t in self.recent_429s if now - t < BACKOFF_WINDOW_SECONDS] + [now]
        # Exponent capped: 2 ** n overflows float past ~1000 429s in the window.
        backoff = min(MAX_BACKOFF_SECONDS, RETRY_BASE_SECONDS * 2 ** min(len(self.recent_429s) - 1, 30))
        wait = retry_after if retry_after is not None else backoff
        self.cooloff_until = max(self.cooloff_until, now + wait)

    def summary(self) -> str:
        with self._lock:  # consistent counters; dict(deferred) never iterates mid-update
            return (f"budget: {self.consumed} requests, {self.refusals} refusals (actions refused incl. retries "
                    f"{dict(self.deferred) or 0}), risk cancels sent over budget {self.forced_over_budget}, "
                    f"429s {self.rate_limited}")


class RateLimitedError(Exception):
    """A request got 429 and wasn't retried (attempts used up, or a
    Retry-After too long to sleep out inline). Nothing was done by the
    venue: a 429 is a clean rejection."""

    def __init__(self, what: str, retry_after: float | None, cause: BaseException):
        super().__init__(f"{what}: rate limited (429, Retry-After {retry_after}) -- {cause}")
        self.retry_after = retry_after
        self.cause = cause


def is_rate_limited(exc: BaseException) -> bool:
    return getattr(exc, "status_code", None) == 429


def retry_after_seconds(exc: BaseException, now: float | None = None) -> float | None:
    """Retry-After from the 429 response: delta-seconds or an HTTP date.
    None if absent or unparseable."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    raw = headers.get("Retry-After") if headers is not None else None
    if raw is None:
        return None
    raw = str(raw).strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = time.time() if now is None else now
    return max(0.0, when.timestamp() - now)


def call_with_429_retry(fn, what: str, budget: ActionBudget | None = None, attempts: int = RETRY_ATTEMPTS,
                        sleep=time.sleep, rng: random.Random | None = None):
    """Calls fn(); on a 429 waits (Retry-After, else exponential backoff,
    plus jitter) and retries, up to `attempts` calls. Every 429 is logged
    and reported to `budget` (which then holds deferrable actions for the
    Retry-After), and each retry is counted against it. Raises
    RateLimitedError when out of attempts or when the wait would exceed
    MAX_INLINE_WAIT_SECONDS; any other exception passes through unchanged."""
    rng = rng or random
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if not is_rate_limited(exc):
                raise
            retry_after = retry_after_seconds(exc)
            if budget is not None:
                budget.note_rate_limited(retry_after)
            base = retry_after if retry_after is not None else RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            wait = base * (1 + rng.uniform(0, RETRY_JITTER_FRACTION))
            if attempt == attempts or wait > MAX_INLINE_WAIT_SECONDS:
                log.error("%s: rate limited (429, Retry-After %s) on attempt %d/%d -- giving up%s",
                          what, retry_after, attempt, attempts,
                          f" (would have to wait {wait:.2f}s inline)" if wait > MAX_INLINE_WAIT_SECONDS else "")
                raise RateLimitedError(what, retry_after, exc) from exc
            log.warning("%s: rate limited (429, Retry-After %s) on attempt %d/%d -- retrying in %.2fs",
                        what, retry_after, attempt, attempts, wait)
            sleep(wait)
            if budget is not None:
                budget.consume(None, 1)  # the retry is one more request; the caller counted the first
    raise AssertionError("unreachable")
