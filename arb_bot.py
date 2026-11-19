"""
Cross-venue arbitrage bot: Kalshi <-> Polymarket US.

Wires orderbook_feed's BookStore/KalshiFeed/PolymarketUSFeed to ArbDetector
to an OrderExecutor. Defaults to a fully passive dry run; real orders
require two independent, explicit opt-ins (see LiveExecutor's docstring in
executor.py).

Loads API_Setup/.env on startup (pip install python-dotenv) so KALSHI_KEY_ID,
KALSHI_PRIVATE_KEY_PATH, POLYMARKET_KEY_ID, and POLYMARKET_SECRET_KEY_ID can
live there instead of being exported by hand every session.

Usage:
    python arb_bot.py                          # dry run, prints opportunities
    python arb_bot.py --min-edge 0.03          # raise the bar for a hit
    python arb_bot.py --log-edges edges.jsonl  # also record edge-decay instrumentation (dry-run only)
    python arb_bot.py --max-book-age 0.25      # tighten the staleness guard (default 0.5s)
    python arb_bot.py --paper logs/paper.jsonl # simulate realistic fills + a running P&L ledger, still no real orders
    ARB_BOT_CONFIRM_LIVE=yes python arb_bot.py --live --max-notional 50
                                                # places real orders, capped
                                                # at $50 total committed capital
                                                # for this run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

from arbitrage import DEFAULT_MAX_BOOK_AGE, ArbDetector, ArbOpportunity
from balance_monitor import BalanceMonitor, kalshi_get_balance, polymarket_get_balance
from executor import DryRunExecutor, OrderExecutor, build_live_executor, kalshi_rest_get
from instrumentation import EdgeDecayLogger
from market_pairs import PAIRS, MarketPair
from orderbook_feed import BookStore, KalshiAuth, KalshiFeed, PolymarketUSFeed, make_printer, run_forever
from paper_trading import PaperExecutor, PaperLedger

load_dotenv(Path(__file__).parent / "API_Setup" / ".env")

log = logging.getLogger("arb_bot")

# Minimum time to leave a pair "cooling down" after acting on it, so a burst
# of book updates while the order is in flight doesn't fire duplicate orders
# on the same opportunity before your own fill is reflected anywhere.
COOLDOWN_SECONDS = 5.0


class ArbBot:
    def __init__(
        self,
        store: BookStore,
        pairs: list[MarketPair],
        executor: OrderExecutor,
        live: bool,
        max_notional: Decimal | None,
        max_book_age: float = DEFAULT_MAX_BOOK_AGE,
        ledger: PaperLedger | None = None,
        balance_monitor: BalanceMonitor | None = None,
    ):
        self.store = store
        self.pairs = pairs
        self.executor = executor
        self.live = live
        self.max_notional = max_notional
        self.ledger = ledger  # set only in --paper mode; records realized P&L once both legs succeed
        self.balance_monitor = balance_monitor  # set only in --live mode; see _on_opportunity's gate
        self.committed_notional = Decimal(0)
        self._inflight: set[str] = set()
        self.detector = ArbDetector(store, pairs, self._on_opportunity, max_book_age=max_book_age)

    def _on_opportunity(self, opp: ArbOpportunity) -> None:
        log.info(str(opp))
        if not (self.live or self.ledger is not None):
            return  # plain dry run: log only, no simulated or real execution
        if opp.pair.name in self._inflight:
            return

        cost = (opp.buy_a_price + opp.buy_b_price) * opp.max_contracts
        if self.max_notional is not None and self.committed_notional + cost > self.max_notional:
            log.warning(
                "skipping %s: would commit $%.2f, over remaining budget $%.2f",
                opp.pair.name, cost, self.max_notional - self.committed_notional,
            )
            return

        if self.balance_monitor is not None:
            # Hard gate, checked before EITHER leg commits -- unlike the
            # maker bot's hedge leg (real exposure already exists there by
            # the time balance matters), neither leg here has fired yet at
            # this point, so refusing the whole trade is a clean no-op, not
            # a worse-than-nothing outcome. maybe_refresh() piggybacks on
            # opportunity checks rather than needing a separate periodic
            # task, since this bot has none today.
            self.balance_monitor.maybe_refresh()
            if opp.direction == "kalshi_yes+poly_no":
                kalshi_cost, poly_cost = opp.buy_a_price * opp.max_contracts, opp.buy_b_price * opp.max_contracts
            else:
                poly_cost, kalshi_cost = opp.buy_a_price * opp.max_contracts, opp.buy_b_price * opp.max_contracts
            if not self.balance_monitor.can_afford("kalshi", kalshi_cost) or not self.balance_monitor.can_afford(
                "polymarket_us", poly_cost
            ):
                log.warning(
                    "%s: skipping -- insufficient balance for one or both legs (need kalshi $%.2f, "
                    "polymarket_us $%.2f; have kalshi %s, polymarket_us %s)",
                    opp.pair.name, kalshi_cost, poly_cost,
                    self.balance_monitor.kalshi_balance, self.balance_monitor.poly_balance,
                )
                return

        self._inflight.add(opp.pair.name)
        self.committed_notional += cost
        asyncio.create_task(self._execute(opp))

    async def _execute(self, opp: ArbOpportunity) -> None:
        try:
            if opp.direction == "kalshi_yes+poly_no":
                leg_a = asyncio.to_thread(
                    self.executor.buy_kalshi, opp.pair.kalshi_ticker, "yes", opp.buy_a_price, opp.max_contracts
                )
                leg_b = asyncio.to_thread(
                    self.executor.buy_polymarket, opp.pair.poly_market_slug, "no", opp.buy_b_price, opp.max_contracts
                )
            else:  # poly_yes+kalshi_no
                leg_a = asyncio.to_thread(
                    self.executor.buy_polymarket, opp.pair.poly_market_slug, "yes", opp.buy_a_price, opp.max_contracts
                )
                leg_b = asyncio.to_thread(
                    self.executor.buy_kalshi, opp.pair.kalshi_ticker, "no", opp.buy_b_price, opp.max_contracts
                )

            result_a, result_b = await asyncio.gather(leg_a, leg_b, return_exceptions=True)
            fail_a, fail_b = isinstance(result_a, Exception), isinstance(result_b, Exception)

            if fail_a and fail_b:
                log.error("%s: both legs failed — no position taken. a=%s b=%s", opp.pair.name, result_a, result_b)
            elif fail_a or fail_b:
                filled, failed = (result_b, result_a) if fail_a else (result_a, result_b)
                log.critical(
                    "%s: ONE LEG FILLED, ONE FAILED — you are holding an UNHEDGED position. "
                    "filled=%s failed=%s. Close this out manually now.",
                    opp.pair.name, filled, failed,
                )
            else:
                log.info("%s: both legs filled. a=%s b=%s", opp.pair.name, result_a, result_b)
                if self.ledger is not None:
                    self.ledger.record_realized_pnl(opp.pair.name, opp.net_edge * opp.max_contracts)
        finally:
            await asyncio.sleep(COOLDOWN_SECONDS)
            self._inflight.discard(opp.pair.name)

    def on_book_update(self, book) -> None:
        self.detector.check_all()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="place real orders instead of dry-run logging")
    parser.add_argument("--min-edge", type=Decimal, default=None, help="override min_edge for every configured pair")
    parser.add_argument(
        "--max-notional", type=Decimal, default=None,
        help="hard cap on total $ committed to live trades this run (recommended whenever --live is set)",
    )
    parser.add_argument(
        "--max-book-age", type=float, default=DEFAULT_MAX_BOOK_AGE,
        help=f"refuse to act on a book older than this many seconds (default {DEFAULT_MAX_BOOK_AGE})",
    )
    parser.add_argument(
        "--log-edges", type=str, default=None, metavar="PATH",
        help="write edge-decay instrumentation (JSON lines) to PATH. Dry-run only — ignored with --live.",
    )
    parser.add_argument(
        "--paper", type=str, default=None, metavar="PATH",
        help="simulate realistic fills against live book depth with a running P&L ledger, written to PATH. "
             "Still no real orders — mutually exclusive with --live.",
    )
    parser.add_argument(
        "--min-balance", type=Decimal, default=Decimal("0"),
        help="live mode only: log a CRITICAL alert if either venue's available balance drops below this "
             "many dollars (default 0 -- no alert). Does not by itself block trading.",
    )
    parser.add_argument(
        "--min-balance-share", type=Decimal, default=Decimal("0"),
        help="live mode only: log a CRITICAL alert if either venue's balance falls below this fraction "
             "(e.g. 0.2 = 20%%) of combined capital across both venues (default 0 -- no alert)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.paper and args.live:
        log.error("--paper and --live are mutually exclusive")
        return

    if not PAIRS:
        log.error("market_pairs.PAIRS is empty — add at least one MarketPair before running")
        return

    pairs = PAIRS
    if args.min_edge is not None:
        pairs = [
            MarketPair(p.name, p.kalshi_ticker, p.poly_market_slug, args.min_edge, p.max_contracts)
            for p in pairs
        ]

    if args.live:
        if os.environ.get("ARB_BOT_CONFIRM_LIVE") != "yes":
            log.error(
                "refusing to run --live without ARB_BOT_CONFIRM_LIVE=yes set in the environment. "
                "This is deliberate — read executor.py's module docstring first."
            )
            return
        if args.max_notional is None:
            log.error("refusing to run --live without --max-notional set. Pick a number you're OK losing.")
            return
        executor: OrderExecutor = build_live_executor()
        ledger = None
        # Balance monitoring is live-mode only -- paper/dry-run never place
        # a real order, so there's no real balance to protect. Reuses the
        # same KalshiAuth / PolymarketUS SDK client build_live_executor()
        # already constructed, rather than opening a second connection.
        balance_monitor: BalanceMonitor | None = BalanceMonitor(
            kalshi_get_balance_fn=lambda: kalshi_get_balance(lambda path, params: kalshi_rest_get(executor.kalshi.auth, path, params)),
            poly_get_balance_fn=lambda: polymarket_get_balance(executor.polymarket._client),
            min_balance=args.min_balance,
            min_balance_share=args.min_balance_share,
        )
        balance_monitor.refresh()  # populate before trading starts, not just on the first opportunity
        if balance_monitor.kalshi_balance is None or balance_monitor.poly_balance is None:
            log.error("refusing to start live: could not confirm starting balance on one or both venues (see error above)")
            return
        log.info(
            "starting balances: kalshi $%.2f, polymarket_us $%.2f",
            balance_monitor.kalshi_balance, balance_monitor.poly_balance,
        )
        log.warning("LIVE TRADING ARMED — real orders will be placed, capped at $%s total", args.max_notional)
    elif args.paper:
        ledger = PaperLedger(out_path=Path(args.paper))
        executor = PaperExecutor(ledger)
        balance_monitor = None
        log.info("paper trading — simulated fills against live book depth, logged to %s", args.paper)
    else:
        executor = DryRunExecutor()
        ledger = None
        balance_monitor = None
        log.info("dry run — no real orders will be placed")

    store = BookStore()
    printer = make_printer(store)
    bot = ArbBot(
        store, pairs, executor, live=args.live, max_notional=args.max_notional,
        max_book_age=args.max_book_age, ledger=ledger, balance_monitor=balance_monitor,
    )

    edge_logger: EdgeDecayLogger | None = None
    if args.log_edges:
        if args.live:
            log.error("--log-edges is dry-run-only instrumentation — ignoring it since --live is set")
        else:
            edge_logger = EdgeDecayLogger(store, pairs, args.log_edges)
            log.info("edge-decay instrumentation enabled -> %s", args.log_edges)

    def on_update(book) -> None:
        printer(book)
        bot.on_book_update(book)
        if edge_logger is not None:
            edge_logger.check_and_log()

    store.on_update = on_update

    tasks = []

    key_id = os.environ.get("KALSHI_KEY_ID")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    kalshi_tickers = sorted({p.kalshi_ticker for p in pairs})
    if key_id and key_path:
        auth = KalshiAuth(key_id, key_path)
        feed = KalshiFeed(auth, kalshi_tickers, store)
        tasks.append(run_forever("kalshi", feed.run))
    else:
        log.error("KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH unset — cannot subscribe to Kalshi books")
        return

    poly_key_id = os.environ.get("POLYMARKET_KEY_ID")
    poly_secret_key = os.environ.get("POLYMARKET_SECRET_KEY_ID")
    poly_slugs = sorted({p.poly_market_slug for p in pairs})
    if poly_key_id and poly_secret_key:
        feed = PolymarketUSFeed(poly_slugs, store, poly_key_id, poly_secret_key)
        tasks.append(run_forever("polymarket_us", feed.run))
    else:
        log.error("POLYMARKET_KEY_ID / POLYMARKET_SECRET_KEY_ID unset — cannot subscribe to Polymarket US books")
        return

    log.info("watching %d pair(s): %s", len(pairs), ", ".join(p.name for p in pairs))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import contextlib

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
