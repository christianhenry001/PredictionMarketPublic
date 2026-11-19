# Prediction Market Trading Infrastructure

Python infrastructure for trading and research on two regulated US prediction-market exchanges, **Kalshi** and **Polymarket US**. The code covers live market data, order execution with layered safety controls, position reconciliation, fee modeling, paper trading, an offline replay engine, and long-running data recorders. It has run against real money on both venues since September 2026.

This is the public subset of a larger private project. The strategy layer (quoting logic, market selection, signal models and research findings) and all recorded market data are kept private. What's here is the engineering underneath it.

## Highlights

- **One order-book model for two venues.** Kalshi exposes only YES-side interest, while Polymarket US runs a single book where "buy NO" executes as a YES sell at the complement price. [orderbook_feed.py](orderbook_feed.py) streams both over WebSockets and normalizes them into one probability-of-YES `Book`, so cross-venue comparison is a dictionary lookup.
- **Safety first.** Every executor runs in dry-run mode by default. A live order needs two separate opt-ins: a `--live` flag and the environment variable `ARB_BOT_CONFIRM_LIVE=yes`. On top of that sit hard capital caps, balance gating, price sanity checks, book-staleness guards and a plausibility ceiling that rejects too-good-to-be-true opportunities as likely bad ticks.
- **Reconciliation against the venue.** [single_venue_reconciler.py](single_venue_reconciler.py) compares the bot's local position against the venue's own reported position and corrects drift. It was built after a live incident in which a gap in the fill listener let the local tracker drift.
- **Exact fee modeling.** [fees.py](fees.py) charges fees per fill, not per order, and walks the book across price levels. It applies each venue's own rounding: round-up on Kalshi; banker's rounding on Polymarket US, with a cap on the cumulative fee. Each formula is checked against the venue's published schedule, and the module was updated as those schedules changed.
- **Rate-limit budgeting.** [rate_limit.py](rate_limit.py) uses token buckets (global and per market) with priority classes. Risk-reducing cancels always go out, while new posts must leave headroom. 429s get exponential backoff and honor `Retry-After`.
- **Order audit trail.** [order_audit.py](order_audit.py) writes an append-only JSONL record of every create, cancel, ack and fill. Each record carries a reason code from a fixed enum, both a wall clock and a monotonic clock, and the decision context. [order_metrics.py](order_metrics.py) tracks cancel-to-fill ratios, resting times, fill rates per price level and post-fill markouts.
- **Paper trading with a queue model.** Polymarket US has no sandbox, so [paper_trading.py](paper_trading.py) simulates taker fills against real live depth. Maker fills come from the live trade tape, with an estimated position in the queue.
- **Offline replay engine.** [maker_offline/](maker_offline/) replays recorded books and trades on the exchange's clock. It keeps two clocks per event (when it happened at the venue, and when we saw it) and fills virtual orders under conservative or optimistic queue models. Request latency is modeled too, so a cancel can lose a race against a trade.
- **Data recorders.** Long-running async recorders for weather observations, NWS forecasts and climate reports, alongside market books ([weather_recorder.py](weather_recorder.py)), and for MLB live game feeds ([sports_feed/mlb_recorder.py](sports_feed/mlb_recorder.py)). Output is daily JSONL files ([jsonl_logs.py](jsonl_logs.py)). Finished days are gzipped and verified byte-for-byte before the original is deleted ([scripts/compress_logs.py](scripts/compress_logs.py)). A disk guard stops recorders cleanly before the volume fills ([scripts/disk_guard.py](scripts/disk_guard.py)).

## Layout

| Module | Purpose |
|---|---|
| [orderbook_feed.py](orderbook_feed.py) | WebSocket book feeds for both venues, Kalshi RSA-PSS auth, shared `BookStore`, watchdog for wedged connections |
| [executor.py](executor.py) | Taker order execution (Kalshi REST V2, Polymarket US SDK), dry-run / live executors, IOC hedge orders |
| [maker_executor.py](maker_executor.py) | Resting-order management: post, cancel/replace, batch validation, private fill-notification stream with de-duplication |
| [paper_trading.py](paper_trading.py) | Simulated taker and maker fills against live depth, persistent P&L ledger |
| [balance_monitor.py](balance_monitor.py) | Cached per-venue balances; gates trades on available capital |
| [single_venue_reconciler.py](single_venue_reconciler.py) | Local-vs-venue position reconciliation |
| [fees.py](fees.py) | Per-fill fee models for both venues |
| [rate_limit.py](rate_limit.py) | Token-bucket request budget and 429 retry |
| [order_audit.py](order_audit.py), [order_metrics.py](order_metrics.py) | Order audit log and order-flow metrics |
| [arbitrage.py](arbitrage.py), [arb_bot.py](arb_bot.py), [market_pairs.py](market_pairs.py), [instrumentation.py](instrumentation.py) | A complete first-generation bot: textbook cross-venue YES+NO detection with depth walking and fees, the four safety gates, and instrumentation for how fast an opportunity decays |
| [maker_offline/](maker_offline/) | Offline tape loader and queue-model fill engine |
| [weather_recorder.py](weather_recorder.py), [sports_feed/](sports_feed/) | Data recorders |
| [scripts/](scripts/) | Log compression, disk guard, desktop notifications |
| [tests/](tests/) | 255 unit tests |

## Running

```bash
pip install -r requirements.txt
python -m pytest            # no credentials or network needed
```

To connect to the venues, copy `.env.example` to `.env` and fill in your own API credentials. Kalshi uses a key ID plus an RSA private key file. Polymarket US uses a key ID and a secret. Everything defaults to dry-run:

```bash
python orderbook_feed.py                       # stream and print books
python arb_bot.py                              # dry run: detect and log only
python arb_bot.py --paper logs/paper.jsonl     # simulated fills with a running ledger
```

Live trading needs both `--live` and `ARB_BOT_CONFIRM_LIVE=yes`.

## How the research is run

The code is half of the project. The other half is a research process built to avoid fooling ourselves:

- **Pre-registered decisions.** Before any forward data is scored, each go/no-go test records its sample size, its statistic, and its pass and kill thresholds.
- **No pooling across market types.** Sports, politics, weather and crypto markets behave differently, so each is tested on its own.
- **Staged promotion.** An idea moves from historical replay to paper trading to a small live probe. At each stage, paper results are compared with real fills.
- **Negative results are kept.** Retired hypotheses are written up with their evidence, just like the ones that passed.

## Engineering notes

Many design choices in this code came from live incidents rather than up-front planning. The comments document them where they apply. Examples include position drift between the local tracker and the venue, a NO-side price inversion, off-grid price rejections, settlement-timing cases in reconciliation, silent feed stalls, and a local clock running half a second off. Each incident was traced to its root cause, fixed, and covered by a regression test.
