"""
Append-only order audit log (added 2026-09-28, audit item #35): one JSON
line per order action -- every create, cancel request, and every exchange
answer or event about one of our orders -- separate from the app log, with
a reason code from the fixed ReasonCode enum on every record.

File: <dir>/order_audit_<UTC date>.jsonl, opened in append mode only and
never truncated or rewritten; a record dated on a new UTC day goes to that
day's file. Several runs on one day share a file -- each record carries a
`run` id (the app log's name, or the process start time).

Record fields (always present; null when unknown or not applicable):
  ts_ns        wall clock, time.time_ns()
  mono_ns      time.monotonic_ns() -- for ordering/latency within one run
  run, mode    run id; "live" / "paper" / "shadow" / "replay"
  action       create | create_ack | create_failed | create_unknown |
               cancel_request | cancel_ack | cancel_failed | cancel_all |
               fill | reject | exchange_cancel | replace | adopt |
               modify | modify_ack | modify_failed
               (modify = a size-down modify request, reason SIZE_REDUCE;
               modify_ack = the venue accepted the REQUEST -- the order
               stream's replace / reject record says what happened to it)
  reason       a ReasonCode value. For an ack/failure: the reason of the
               request it answers. For an exchange-originated event: the
               EXCHANGE_* code.
  market, pair market slug and pair name
  order_id     venue order id (null on a create before the venue answers)
  client_id    always null: Polymarket US create-order has no client order
               id or idempotency key (docs checked 2026-09-28)
  side, price, size, rung
               price in OUR side's terms (a "no" order at p rests on the
               yes-denominated book as an ask at 1 - p)
  fair_value   the quoter's external fair value at decision time
  book         raw book top at decision time: {"bid": [p, size], "ask": [p, size]}
  old_target, new_target
               reprices: the resting price being replaced and the new target
               (null new_target = the side is being pulled)
  latency_ms   round trip for *_ack / *_failed records
  detail       free text

Every cancel has a reason: SingleVenueQuoter's cancel primitives take the
reason as a required keyword argument and refuse anything that isn't a
ReasonCode (tests/test_order_audit.py enforces both, plus a source scan that
no other code path calls the order client's cancel).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path

log = logging.getLogger("order_audit")


class ReasonCode(str, Enum):
    # --- why a resting order is cancelled / a side is pulled ---------------
    # The resting price is now too aggressive: inside MIN_EDGE of fair
    # value, through it, or further ahead of its target than the hold band
    # allows (a retreat) -- for a ladder, ahead of the new top rung or of the
    # new target of the rung it serves. Also the ladder freeze breach (fair
    # value reached the deepest frozen rung).
    FV_OUT_OF_BAND = "FV_OUT_OF_BAND"
    # The target moved toward the market by more than the hold band (an
    # advance) -- the old order is replaced by a more aggressive one.
    TARGET_IMPROVED = "TARGET_IMPROVED"
    # Another fill on this side would take inventory past max_inventory (or
    # the ladder's inventory allowance for the side is zero).
    INVENTORY_LIMIT = "INVENTORY_LIMIT"
    # A ladder side's allowed total shrank (inventory or volatility), or an
    # oversized order at a wanted price can't be absorbed by deeper rungs.
    SIZE_REDUCE = "SIZE_REDUCE"
    # Ladder only: a DEEP rung's target moved toward the market (the ladder
    # re-formed closer in, e.g. after the top filled or the mid moved up), so
    # the order is replaced by a more aggressive one -- the deep-rung
    # analogue of TARGET_IMPROVED. (A deep rung left MORE aggressive than its
    # target is FV_OUT_OF_BAND.)
    RUNG_PROMOTION = "RUNG_PROMOTION"
    FILL_SIDE_HOLD = "FILL_SIDE_HOLD"  # same-side hold after a fill (single-order mode)
    # A taker order swept through the touch on this side (sweep_detector.py);
    # the side stays off for the pair's sweep_pause_seconds.
    SWEEP_PAUSE = "SWEEP_PAUSE"
    STALE_FEED = "STALE_FEED"  # feed disconnected or silent past MAKER_QUOTE_MAX_SILENCE
    FV_UNAVAILABLE = "FV_UNAVAILABLE"  # no outside bid or ask besides our own quotes
    MARKET_CLOSED = "MARKET_CLOSED"  # venue market state not OPEN
    EXTREME_GUARD = "EXTREME_GUARD"  # mid outside the tradeable probability band
    UNQUOTABLE_PRICE = "UNQUOTABLE_PRICE"  # target outside (0, 1) after spread/skew/tick rounding
    CHURN_BREAKER = "CHURN_BREAKER"  # requote-churn circuit breaker tripped
    SELF_MATCH_GUARD = "SELF_MATCH_GUARD"  # cleared out of our own unwind/flatten order's reach
    RECONCILE_UNTRACKED = "RECONCILE_UNTRACKED"  # open on the venue, unknown to us
    RECONCILE_MISMATCH = "RECONCILE_MISMATCH"  # venue shows a different side than we tracked
    RECONCILE_DUPLICATE = "RECONCILE_DUPLICATE"  # a found unknown order duplicates a side that already has one
    SHUTDOWN = "SHUTDOWN"
    RISK_GUARD_FILL_VELOCITY = "RISK_GUARD_FILL_VELOCITY"
    RISK_GUARD_SELF_CROSS = "RISK_GUARD_SELF_CROSS"  # our yes and no quotes would cross each other
    RISK_GUARD_REAL_CROSS = "RISK_GUARD_REAL_CROSS"  # target marketable against the real book, no valid clamp
    RISK_GUARD_BALANCE = "RISK_GUARD_BALANCE"  # can't afford the quote
    RISK_GUARD_WEATHER_DEAD = "RISK_GUARD_WEATHER_DEAD"
    RISK_GUARD_HALT = "RISK_GUARD_HALT"  # a per-pair halt()
    # --- why an order is created ------------------------------------------
    QUOTE_NEW = "QUOTE_NEW"  # nothing was resting on that side/rung
    QUOTE_REPLACE = "QUOTE_REPLACE"  # replaces an order cancelled in the same reprice
    QUOTE_TOP_UP = "QUOTE_TOP_UP"  # tops up size behind a kept order at the same price
    UNWIND = "UNWIND"  # active inventory unwind (taker IOC)
    FLATTEN = "FLATTEN"  # shutdown flatten (taker IOC)
    # --- exchange-originated events ---------------------------------------
    EXCHANGE_FILL = "EXCHANGE_FILL"
    EXCHANGE_REJECT = "EXCHANGE_REJECT"  # e.g. post-only would have crossed
    EXCHANGE_CANCELED = "EXCHANGE_CANCELED"  # cancel / expire / done-for-day on the order stream
    EXCHANGE_REPLACED = "EXCHANGE_REPLACED"
    RECONCILE_ADOPT = "RECONCILE_ADOPT"  # took the venue's price/size, or adopted an unknown order


# Cancels that reduce risk right now. The rate-limit budget always lets
# these through (rate_limit.Priority.RISK_CANCEL); every other cancel reason
# can wait for budget.
RISK_CANCEL_REASONS = frozenset({
    ReasonCode.FV_OUT_OF_BAND, ReasonCode.INVENTORY_LIMIT, ReasonCode.SIZE_REDUCE,
    ReasonCode.FILL_SIDE_HOLD, ReasonCode.SWEEP_PAUSE, ReasonCode.STALE_FEED, ReasonCode.FV_UNAVAILABLE, ReasonCode.MARKET_CLOSED,
    ReasonCode.EXTREME_GUARD, ReasonCode.UNQUOTABLE_PRICE, ReasonCode.CHURN_BREAKER, ReasonCode.SELF_MATCH_GUARD,
    ReasonCode.RECONCILE_UNTRACKED, ReasonCode.RECONCILE_MISMATCH, ReasonCode.RECONCILE_DUPLICATE,
    ReasonCode.SHUTDOWN, ReasonCode.RISK_GUARD_FILL_VELOCITY, ReasonCode.RISK_GUARD_SELF_CROSS,
    ReasonCode.RISK_GUARD_REAL_CROSS, ReasonCode.RISK_GUARD_BALANCE, ReasonCode.RISK_GUARD_WEATHER_DEAD,
    ReasonCode.RISK_GUARD_HALT,
})

CANCEL_REASONS = RISK_CANCEL_REASONS | {ReasonCode.TARGET_IMPROVED, ReasonCode.RUNG_PROMOTION}

ACTIONS = frozenset({
    "create", "create_ack", "create_failed", "create_unknown",
    "cancel_request", "cancel_ack", "cancel_failed", "cancel_all",
    "fill", "reject", "exchange_cancel", "replace", "adopt", "modify", "modify_ack", "modify_failed",
})

_FIELDS = ("market", "pair", "order_id", "client_id", "side", "price", "size", "rung", "fair_value", "book",
           "old_target", "new_target", "latency_ms", "detail")


def require_reason(reason, allowed: frozenset | None = None) -> ReasonCode:
    """Raises TypeError unless `reason` is a ReasonCode (a bare string is
    refused -- the enum is the fixed vocabulary), and ValueError if it isn't
    in `allowed`."""
    if not isinstance(reason, ReasonCode):
        raise TypeError(f"reason must be a ReasonCode, got {reason!r}")
    if allowed is not None and reason not in allowed:
        raise ValueError(f"{reason.value} is not a valid reason here")
    return reason


def _jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


class OrderAuditLog:
    """Append-only JSONL writer, rotated by UTC date. `directory=None` keeps
    records in memory only (`self.records`, bounded) -- for tests and for
    replays that don't want files. A write failure is logged (once per
    file) and counted, never raised: the audit must not stop the bot from
    cancelling.

    Thread-safe (2026-09-29): with ASYNC_ORDER_CALLS the order calls run in
    worker threads. Every record is written today from the event loop
    thread only (tests pin that), but one lock around the row, the counter
    and the file write means a future caller on another thread can't
    interleave half-lines or race the date rotation."""

    MEMORY_LIMIT = 100_000

    def __init__(self, directory: str | os.PathLike | None, run: str | None = None, mode: str = "shadow",
                 clock_ns=time.time_ns, mono_ns=time.monotonic_ns):
        self.directory = Path(directory) if directory is not None else None
        self.run = run or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.mode = mode
        self._clock_ns = clock_ns
        self._mono_ns = mono_ns
        self._file = None
        self._day: str | None = None
        self.records: list[dict] = []
        self.write_errors = 0
        self.count = 0
        self._lock = threading.Lock()
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, day: str) -> Path | None:
        return None if self.directory is None else self.directory / f"order_audit_{day}.jsonl"

    def record(self, action: str, reason: ReasonCode, **fields) -> dict:
        if action not in ACTIONS:
            raise ValueError(f"unknown audit action {action!r}")
        require_reason(reason)
        unknown = set(fields) - set(_FIELDS)
        if unknown:
            raise ValueError(f"unknown audit field(s) {sorted(unknown)}")
        values = {name: _jsonable(fields.get(name)) for name in _FIELDS}
        with self._lock:
            ts_ns = self._clock_ns()
            row = {"ts_ns": ts_ns, "mono_ns": self._mono_ns(), "run": self.run, "mode": self.mode,
                   "action": action, "reason": reason.value, **values}
            self.count += 1
            if self.directory is None:
                if len(self.records) < self.MEMORY_LIMIT:
                    self.records.append(row)
                return row
            self._write(row, ts_ns)
        return row

    def _write(self, row: dict, ts_ns: int) -> None:
        day = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        try:
            if day != self._day or self._file is None:
                if self._file is not None:
                    self._file.close()
                self._file = open(self.path_for(day), "a", encoding="utf-8")
                self._day = day
            self._file.write(json.dumps(row, separators=(",", ":")) + "\n")
            self._file.flush()
        except OSError as exc:
            self.write_errors += 1
            if self.write_errors == 1:
                log.error("order audit: could not write %s: %s: %s -- continuing without the audit record",
                          self.path_for(day), type(exc).__name__, exc)

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None
