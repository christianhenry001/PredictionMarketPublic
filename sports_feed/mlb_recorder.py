"""
Read-only recorder for MLB's free Stats API live game feed. Places NO
orders and touches no trading venue.

What it is for: measuring how quickly a free, public score feed reports a
play relative to the exchange's own book updates. Run it on the same
machine as an order-book recorder so both share a clock.

Sources, all public and unauthenticated:
  - linescore  /api/v1/game/{pk}/linescore (~1.6 KB), polled every
               --linescore-seconds (default 0.5): score, hits, errors,
               inning, outs, count, runners. The fast path for a score
               change.
  - feed       /api/v1.1/game/{pk}/feed/live (200-800 KB), fetched when
               the linescore changes, a push arrives, or every
               FEED_FALLBACK_SECONDS: every play and pitch with MLB's own
               startTime/endTime, so each event also carries MLB's clock.
  - push       the Gameday websocket (wss://ws.statsapi.mlb.com/...),
               logged raw. Each message also triggers a feed fetch.
               Unverified: on 2026-09-29 it accepted a connection but sent
               nothing in 2 minutes before first pitch.
Every request carries a random cache-busting query parameter: the CDN
caches these endpoints for 10s (max-age=10), and a plain poll read up to
10s late. With the parameter the response is a MISS, age 0, 30-80ms. The
parameter NAME matters: the CDN ignores "cb" (still a HIT, age 12s, on
2026-09-29) but not "rq", "x" or "nocache". Each response's x-cache/age
headers are checked; hits are counted in the stats rows and warned about.

Records, to <out>/<kind>_<UTC date>.jsonl (recv_time on every row; for
polled rows sent_time/resp_time bracket the request -- use resp_time):
  - linescore:   the linescore state whenever it changes.
  - feed_update: each new feed timecode (metaData.timeStamp) and MLB's own
                 gameEvents/logicalEvents tags for it.
  - play_event:  each pitch/action when first seen or when it changes.
  - at_bat:      each plate appearance's result when first seen or when it
                 changes (isComplete, event, score). lag_vs_end_s = our
                 resp_time minus MLB's endTime for that play.
  - status:      game status changes.
  - push / push_status: websocket messages and connects.
  - stats:       per-source request counts, errors and latency every 60s.
  - health:      failed requests.
Rows from the first feed fetch have backfill=true (seen before we started).

Usage:
  py -m sports_feed.mlb_recorder --team NYY             # today's NYY game
  py -m sports_feed.mlb_recorder --game-pk 849851 --out logs/mlb_feed/g1
It stops by itself 60s after the game goes Final (or after --minutes).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import statistics
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import httpx

from weather_recorder import JsonlWriter, describe_error

log = logging.getLogger("sports_feed.mlb_recorder")

API = "https://statsapi.mlb.com"
WS_URL = "wss://ws.statsapi.mlb.com/api/v1/game/push/subscribe/gameday/{pk}"
WS_HEADERS = {"Origin": "https://www.mlb.com"}

LINESCORE_SECONDS = 0.5
FEED_MIN_GAP_SECONDS = 0.5
FEED_FALLBACK_SECONDS = 10.0
FINAL_GRACE_SECONDS = 60.0
STATS_SECONDS = 60.0
MAX_BACKOFF_SECONDS = 30.0
REQUEST_TIMEOUT_SECONDS = 5.0
CACHE_BUST_PARAM = "rq"  # not "cb": the CDN ignores that one (see the docstring)

REQUEST_ERRORS = (httpx.HTTPError, ValueError)


def cache_busted(url: str) -> str:
    return f"{url}{'&' if '?' in url else '?'}{CACHE_BUST_PARAM}={random.getrandbits(48):x}"


def backoff_seconds(failures: int) -> float:
    return min(MAX_BACKOFF_SECONDS, 0.5 * 2 ** min(failures, 10))


def iso_epoch(value: str | None) -> float | None:
    """MLB's "2026-09-29T23:58:50.228Z" -> epoch seconds."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def linescore_state(ls: dict[str, Any]) -> dict[str, Any]:
    """The fields of /linescore that change during play. Runners appear
    under offense.first/second/third only while the base is occupied."""
    teams = ls.get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    offense = ls.get("offense") or {}
    return {
        "inning": ls.get("currentInning"),
        "inning_state": ls.get("inningState"),
        "is_top": ls.get("isTopInning"),
        "away_runs": away.get("runs"),
        "home_runs": home.get("runs"),
        "away_hits": away.get("hits"),
        "home_hits": home.get("hits"),
        "away_errors": away.get("errors"),
        "home_errors": home.get("errors"),
        "outs": ls.get("outs"),
        "balls": ls.get("balls"),
        "strikes": ls.get("strikes"),
        "on_1b": "first" in offense,
        "on_2b": "second" in offense,
        "on_3b": "third" in offense,
        "batter_id": (offense.get("batter") or {}).get("id"),
    }


def runs_changed(prev: dict[str, Any] | None, cur: dict[str, Any]) -> bool:
    return prev is not None and (prev["away_runs"], prev["home_runs"]) != (cur["away_runs"], cur["home_runs"])


def play_rows(feed: dict[str, Any], seen: dict[tuple, tuple]) -> list[dict[str, Any]]:
    """New or changed plays in a live feed, as rows with a "kind" key
    ("play_event" or "at_bat"). `seen` maps each key to the signature last
    emitted and is updated in place, so a later feed emits only changes."""
    rows: list[dict[str, Any]] = []
    plays = ((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or []
    for play in plays:
        about = play.get("about") or {}
        result = play.get("result") or {}
        ab = about.get("atBatIndex")
        for ev in play.get("playEvents") or []:
            d = ev.get("details") or {}
            key = ("ev", ab, ev.get("index"))
            sig = (d.get("description"), d.get("code"), d.get("isInPlay"), d.get("awayScore"),
                   d.get("homeScore"), d.get("isScoringPlay"), ev.get("endTime"))
            if seen.get(key) == sig:
                continue
            rows.append({
                "kind": "play_event",
                "first_seen": key not in seen,
                "at_bat": ab,
                "index": ev.get("index"),
                "event_kind": ev.get("type"),
                "description": d.get("description"),
                "event": d.get("event"),
                "code": d.get("code"),
                "is_in_play": d.get("isInPlay"),
                "is_scoring_play": d.get("isScoringPlay"),
                "away_score": d.get("awayScore"),
                "home_score": d.get("homeScore"),
                "count": ev.get("count"),
                "start_time": ev.get("startTime"),
                "end_time": ev.get("endTime"),
            })
            seen[key] = sig
        key = ("ab", ab)
        sig = (about.get("isComplete"), result.get("event"), result.get("awayScore"),
               result.get("homeScore"), about.get("isScoringPlay"))
        if seen.get(key) == sig:
            continue
        rows.append({
            "kind": "at_bat",
            "first_seen": key not in seen,
            "at_bat": ab,
            "inning": about.get("inning"),
            "half": about.get("halfInning"),
            "is_complete": about.get("isComplete"),
            "is_scoring_play": about.get("isScoringPlay"),
            "event": result.get("event"),
            "event_type": result.get("eventType"),
            "description": result.get("description"),
            "rbi": result.get("rbi"),
            "away_score": result.get("awayScore"),
            "home_score": result.get("homeScore"),
            "start_time": about.get("startTime"),
            "end_time": about.get("endTime"),
        })
        seen[key] = sig
    return rows


def pick_game(schedule: dict[str, Any], team: str) -> dict[str, Any]:
    """The game in a /schedule response involving `team` (abbreviation,
    case-insensitive). With two (a doubleheader), the first not Final."""
    team = team.upper()
    games = [g for d in schedule.get("dates") or [] for g in d.get("games") or []
             if team in {(g["teams"][s]["team"].get("abbreviation") or "").upper() for s in ("away", "home")}]
    if not games:
        raise SystemExit(f"no game today for {team}")
    live = [g for g in games if (g.get("status") or {}).get("abstractGameState") != "Final"]
    return (live or games)[0]


def is_cache_hit(headers: httpx.Headers) -> bool:
    """True when the CDN served a stored copy: x-cache lists one verdict per
    cache layer ("MISS, MISS"), and age is how long it has been stored."""
    if "HIT" in (headers.get("x-cache") or "").upper():
        return True
    try:
        return int(headers.get("age") or 0) > 0
    except ValueError:
        return False


def summarize_ms(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    return {"n": len(ordered), "p50_ms": round(statistics.median(ordered), 1),
            "p90_ms": round(ordered[int(0.9 * (len(ordered) - 1))], 1), "max_ms": round(ordered[-1], 1)}


class MlbRecorder:
    def __init__(self, client: httpx.AsyncClient, writer: JsonlWriter, game_pk: int, *,
                 linescore_seconds: float = LINESCORE_SECONDS, use_push: bool = True,
                 clock: Callable[[], float] = time.time) -> None:
        self.client = client
        self.writer = writer
        self.pk = game_pk
        self.linescore_seconds = linescore_seconds
        self.use_push = use_push
        self.clock = clock
        self.seen: dict[tuple, tuple] = {}
        self.last_linescore: dict[str, Any] | None = None
        self.last_status: tuple | None = None
        self.last_timecode: str | None = None
        self.first_feed = True
        self.final_at: float | None = None
        self.feed_wanted = asyncio.Event()
        self.latency: dict[str, list[float]] = {"linescore": [], "feed": []}
        self.errors: dict[str, int] = {"linescore": 0, "feed": 0, "push": 0}
        self.cache_hits: dict[str, int] = {"linescore": 0, "feed": 0}
        self.pushes = 0

    async def get_json(self, source: str, url: str) -> tuple[Any, float, float]:
        sent = self.clock()
        resp = await self.client.get(cache_busted(url), timeout=REQUEST_TIMEOUT_SECONDS)
        received = self.clock()
        resp.raise_for_status()
        self.latency[source].append((received - sent) * 1000)
        if is_cache_hit(resp.headers):
            if self.cache_hits[source] == 0:
                log.warning("%s served from the CDN cache (x-cache %s, age %s): cache-busting failed",
                            source, resp.headers.get("x-cache"), resp.headers.get("age"))
            self.cache_hits[source] += 1
        return resp.json(), sent, received

    def _failure(self, source: str, exc: BaseException) -> None:
        self.errors[source] += 1
        self.writer.write("health", {"source": source, **describe_error(exc)})
        log.warning("%s request failed: %s", source, describe_error(exc))

    # --- linescore -------------------------------------------------------

    def handle_linescore(self, ls: dict[str, Any], sent: float, received: float) -> None:
        state = linescore_state(ls)
        if state == self.last_linescore:
            return
        prev, self.last_linescore = self.last_linescore, state
        self.writer.write("linescore", {"sent_time": sent, "resp_time": received, **state})
        if runs_changed(prev, state):
            log.info("SCORE (linescore) away %s - home %s, inning %s %s",
                     state["away_runs"], state["home_runs"], state["inning_state"], state["inning"])
        self.feed_wanted.set()

    async def poll_linescore(self) -> None:
        url = f"{API}/api/v1/game/{self.pk}/linescore"
        failures = 0
        next_at = self.clock()
        while True:
            try:
                ls, sent, received = await self.get_json("linescore", url)
                failures = 0
                self.handle_linescore(ls, sent, received)
            except REQUEST_ERRORS as exc:
                failures += 1
                self._failure("linescore", exc)
                await asyncio.sleep(backoff_seconds(failures))
            # Fixed cadence; after a stall, resume from now instead of bursting.
            next_at = max(next_at + self.linescore_seconds, self.clock())
            await asyncio.sleep(max(0.0, next_at - self.clock()))

    # --- full feed -------------------------------------------------------

    def handle_feed(self, feed: dict[str, Any], sent: float, received: float) -> None:
        meta = feed.get("metaData") or {}
        timecode = meta.get("timeStamp")
        backfill, self.first_feed = self.first_feed, False
        if timecode != self.last_timecode:
            self.last_timecode = timecode
            self.writer.write("feed_update", {
                "sent_time": sent, "resp_time": received, "timecode": timecode,
                "game_events": meta.get("gameEvents"), "logical_events": meta.get("logicalEvents"),
                "wait": meta.get("wait"),
            })
        status = (feed.get("gameData") or {}).get("status") or {}
        key = (status.get("abstractGameState"), status.get("detailedState"))
        if key != self.last_status:
            self.last_status = key
            self.writer.write("status", {"resp_time": received, "abstract": key[0], "detailed": key[1],
                                         "code": status.get("statusCode")})
            log.info("status: %s / %s", *key)
            if key[0] == "Final" and self.final_at is None:
                self.final_at = received
        for row in play_rows(feed, self.seen):
            kind = row.pop("kind")
            end = iso_epoch(row.get("end_time"))
            row.update({"sent_time": sent, "resp_time": received, "timecode": timecode, "backfill": backfill,
                        "lag_vs_end_s": round(received - end, 3) if end is not None else None})
            self.writer.write(kind, row)
            if kind == "at_bat" and row["is_scoring_play"] and row["is_complete"] and not backfill:
                log.info("SCORE (feed) %s: away %s - home %s, %.1fs after MLB endTime",
                         row["event"], row["away_score"], row["home_score"], row["lag_vs_end_s"] or float("nan"))

    async def poll_feed(self) -> None:
        url = f"{API}/api/v1.1/game/{self.pk}/feed/live"
        failures = 0
        last_fetch = float("-inf")
        while True:
            try:
                await asyncio.wait_for(self.feed_wanted.wait(), timeout=FEED_FALLBACK_SECONDS)
            except asyncio.TimeoutError:
                pass
            gap = FEED_MIN_GAP_SECONDS - (self.clock() - last_fetch)
            if gap > 0:
                await asyncio.sleep(gap)
            # Cleared before the request: a trigger during it fetches again.
            self.feed_wanted.clear()
            last_fetch = self.clock()
            try:
                feed, sent, received = await self.get_json("feed", url)
                failures = 0
                self.handle_feed(feed, sent, received)
            except REQUEST_ERRORS as exc:
                failures += 1
                self._failure("feed", exc)
                await asyncio.sleep(backoff_seconds(failures))

    # --- push ------------------------------------------------------------

    def handle_push(self, message: str | bytes, received: float) -> None:
        self.pushes += 1
        try:
            body: Any = json.loads(message)
        except ValueError:
            body = str(message)[:2000]
        self.writer.write("push", {"resp_time": received, "body": body})
        self.feed_wanted.set()

    async def listen_push(self) -> None:
        try:
            import websockets
        except ImportError:
            log.warning("websockets not installed; push disabled")
            return
        url = WS_URL.format(pk=self.pk)
        failures = 0
        while True:
            try:
                async with websockets.connect(url, additional_headers=WS_HEADERS, open_timeout=10) as ws:
                    self.writer.write("push_status", {"state": "connected"})
                    failures = 0
                    async for message in ws:
                        self.handle_push(message, self.clock())
                self.writer.write("push_status", {"state": "closed"})
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # connection errors of every kind: log and reconnect
                failures += 1
                self._failure("push", exc)
            await asyncio.sleep(backoff_seconds(failures + 1))

    # --- lifecycle -------------------------------------------------------

    def write_stats(self) -> None:
        row = {source: {**summarize_ms(values), "errors": self.errors[source], "cache_hits": self.cache_hits[source]}
               for source, values in self.latency.items()}
        row["push"] = {"messages": self.pushes, "errors": self.errors["push"]}
        self.writer.write("stats", row)
        log.info("last %.0fs: linescore %s, feed %s, push %s", STATS_SECONDS, row["linescore"], row["feed"], row["push"])
        for values in self.latency.values():
            values.clear()

    async def run(self, deadline: float | None = None) -> None:
        tasks = [asyncio.create_task(self.poll_linescore()), asyncio.create_task(self.poll_feed())]
        if self.use_push:
            tasks.append(asyncio.create_task(self.listen_push()))
        self.feed_wanted.set()
        next_stats = self.clock() + STATS_SECONDS
        try:
            while True:
                await asyncio.sleep(1.0)
                now = self.clock()
                if now >= next_stats:
                    self.write_stats()
                    next_stats = now + STATS_SECONDS
                if self.final_at is not None and now - self.final_at >= FINAL_GRACE_SECONDS:
                    log.info("game Final %.0fs ago; stopping", now - self.final_at)
                    return
                if deadline is not None and now >= deadline:
                    log.info("--minutes reached; stopping")
                    return
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.write_stats()


async def amain(args: argparse.Namespace) -> None:
    async with httpx.AsyncClient(headers={"User-Agent": "mlb-feed-recorder/1"}) as client:
        game_pk = args.game_pk
        if game_pk is None:
            date = args.date or datetime.now().date().isoformat()
            resp = await client.get(cache_busted(f"{API}/api/v1/schedule?sportId=1&date={date}&hydrate=team"),
                                    timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            game = pick_game(resp.json(), args.team)
            game_pk = game["gamePk"]
            label = f"{game['teams']['away']['team']['abbreviation']}-{game['teams']['home']['team']['abbreviation']}"
            info = {"game_pk": game_pk, "game_date": game.get("gameDate"), "description": game.get("description"),
                    "away": game["teams"]["away"]["team"]["abbreviation"], "home": game["teams"]["home"]["team"]["abbreviation"]}
        else:
            label, info = str(game_pk), {"game_pk": game_pk}
        out = Path(args.out or f"logs/mlb_feed/{datetime.now():%Y-%m-%d_%H%M%S}_{game_pk}_{label}")
        writer = JsonlWriter(out)
        handler = logging.FileHandler(out / "recorder.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logging.getLogger().addHandler(handler)
        writer.write("session", {**info, "linescore_seconds": args.linescore_seconds, "push": not args.no_push})
        log.info("recording game %s (%s) to %s", game_pk, info.get("description") or label, out)
        recorder = MlbRecorder(client, writer, game_pk, linescore_seconds=args.linescore_seconds,
                               use_push=not args.no_push)
        deadline = time.time() + args.minutes * 60 if args.minutes else None
        try:
            await recorder.run(deadline)
        finally:
            writer.close()
            log.info("rows written: %s", writer.counts)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--game-pk", type=int, help="MLB gamePk (from /api/v1/schedule)")
    target.add_argument("--team", help="team abbreviation, e.g. NYY: today's game for that team")
    ap.add_argument("--date", help="schedule date YYYY-MM-DD for --team (default: today, local time)")
    ap.add_argument("--out", help="output directory (default logs/mlb_feed/<time>_<pk>_<AWAY-HOME>)")
    ap.add_argument("--linescore-seconds", type=float, default=LINESCORE_SECONDS, help="linescore poll interval")
    ap.add_argument("--no-push", action="store_true", help="skip the Gameday websocket")
    ap.add_argument("--minutes", type=float, help="stop after this many minutes (default: 60s after Final)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one INFO line per request otherwise
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
