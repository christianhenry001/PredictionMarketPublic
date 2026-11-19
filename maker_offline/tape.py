"""Recorded games as one time-ordered stream of book and trade events.

Three sources, one shape:
  session tapes   logs/aws/sessions, logs/paper_sessions: top 3 levels. A live run's own orders are
                  taken back out of the books so a replay doesn't queue behind its predecessor.
  xsports slices  logs/xsports pbook + ptrade: top 5 levels. Older games stop 15 minutes after start,
                  so they are labelled early-game. extract_slices() cuts them to per-game files once.
  xsports depth   logs/xsports pdepth + ptrade, full games from 2026-11-04 22:59 ET: the whole book.

Two clocks per event, both on the exchange's: t is when it happened at the venue, and seen is when we had
it. Fills are decided in t order and the bot reacts at seen, so a replayed cancel can't beat a trade it only
learned of from that trade's own book. Our PC's clock runs up to 0.5 s off, so seen is our receive time less
a per-tape offset (the 10th percentile of receive minus exchange time, less a nominal 20 ms path).
Prices are yes-denominated.
"""
from __future__ import annotations

import bisect
import glob
import gzip
import heapq
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import jsonl_logs  # noqa: E402

SLICES = ROOT / "logs" / "maker_offline_slices"
XSPORTS = ROOT / "logs" / "xsports"
SPORTS = {"mlb": "MLB", "nfl": "NFL", "cfb": "CFB", "wnba": "WNBA", "nba": "NBA", "nhl": "NHL", "atp": "tennis", "wta": "tennis",
          "itfwo": "tennis", "itfmen": "tennis", "cnl": "soccer", "lmx": "soccer"}
SLICE_BEFORE, SLICE_AFTER = 600.0, 900.0
NOMINAL_PATH = 0.02
TICKS = (0.001, 0.005, 0.01)


def iso(s: str | None) -> float | None:
    if not s:
        return None
    s = s.rstrip("Z")
    if s.endswith("+00:00"):
        s = s[:-6]
    if "." in s:
        a, b = s.split(".")
        s = a + "." + b[:6]
    return datetime.fromisoformat(s + "+00:00").timestamp()


@dataclass
class Book:
    t: float
    bids: dict[float, float]
    asks: dict[float, float]
    state: str | None = None
    tt: str | None = None
    seen: float | None = None

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return (b + a) / 2 if b is not None and a is not None else None


@dataclass
class Trade:
    t: float
    price: float
    qty: float
    taker_sell: bool
    id: str | None = None
    tt: str | None = None
    seen: float | None = None


@dataclass
class Game:
    slug: str
    sport: str
    source: str                  # "tape", "early-game", "full"
    start: float | None
    tick: float
    events: list                 # Book and Trade, by time
    depth_levels: int | None     # recorded levels per side; None = the whole book
    live_log: str | None = None
    session: dict | None = None
    _mt: list[float] = field(default_factory=list, repr=False)
    _mv: list[float] = field(default_factory=list, repr=False)
    _end: float | None = field(default=None, repr=False)
    _settled: float | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self._restamp()
        for e in self.events:
            if isinstance(e, Book) and e.mid is not None:
                self._mt.append(e.t)
                self._mv.append(e.mid)

    def _restamp(self) -> None:
        """Events arrive with t = our receive time. Moves t to the venue's time and seen to our receive
        time on the venue's clock."""
        lags = sorted(e.t - v for e in self.events if isinstance(e, Book) and (v := iso(e.tt)) is not None)
        self.clock_offset = lags[len(lags) // 10] - NOMINAL_PATH if lags else 0.0
        for e in self.events:
            seen = e.t - self.clock_offset
            venue = iso(e.tt) or seen
            e.seen, e.t = max(seen, venue), venue
        self.events.sort(key=lambda e: e.t)

    def mid_at(self, t: float) -> float | None:
        i = bisect.bisect_right(self._mt, t) - 1
        return self._mv[i] if i >= 0 else None

    @property
    def end(self) -> float:
        return self._end if self._end is not None else self.events[-1].t

    @property
    def last_mid(self) -> float | None:
        return self._mv[-1] if self._mv else None

    def settled(self) -> float | None:
        """1 or 0 if the tape runs to the market all but resolved, else None."""
        if self._settled is not None:
            return self._settled
        m = self.last_mid
        return None if m is None or 0.03 < m < 0.97 else float(m >= 0.97)


def sport_of(slug: str) -> str:
    parts = slug.split("-")
    return SPORTS.get(parts[1], parts[1]) if len(parts) > 1 else slug


def by_time(game: "Game", lo: float, hi: float):
    """Each event twice, in time order: ("venue", e) when it happens, ("seen", e) when we have it."""
    ev = [e for e in game.events if lo <= e.t <= hi]
    venue = (("venue", e.t, e) for e in ev)
    seen = (("seen", e.seen, e) for e in sorted(ev, key=lambda x: x.seen))
    return heapq.merge(venue, seen, key=lambda x: (x[1], x[0] != "venue"))


def infer_tick(events: list) -> float:
    """The largest recorded tick every price sits on."""
    prices = set()
    for e in events:
        if isinstance(e, Book):
            prices.update(e.bids)
            prices.update(e.asks)
            if len(prices) > 400:
                break
    for tick in reversed(TICKS):
        if all(abs(p / tick - round(p / tick)) < 1e-6 for p in prices):
            return tick
    return TICKS[0]


def _book(t: float, bids, asks, state=None, tt=None) -> Book:
    return Book(t, {round(float(p), 4): float(q) for p, q in bids or []}, {round(float(p), 4): float(q) for p, q in asks or []}, state, tt)


def load_session(session_dir: str | Path, subtract_own: bool = True) -> list[Game]:
    """One Game per market in a session directory (session.json + rec/)."""
    from replay_session import OwnOrders, resting_rungs
    d = Path(session_dir)
    session = json.loads((d / "session.json").read_text(encoding="utf-8"))
    live_log = d / "live.log"
    own_by_slug = None
    if subtract_own and live_log.exists() and session.get("kind", "").startswith("live"):
        own_by_slug = OwnOrders(resting_rungs(live_log, session.get("pair_names", {})))
    books: dict[str, list] = {}
    for p in sorted(glob.glob(str(d / "rec" / "book_*.jsonl"))):
        with open(p, encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("kind") == "book":
                    books.setdefault(r["slug"], []).append(r)
    trades: dict[str, dict] = {}
    for p in sorted(glob.glob(str(d / "rec" / "trade_*.jsonl"))):
        with open(p, encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                tr = (r.get("payload") or {}).get("trade")
                if tr:
                    trades.setdefault(tr["marketSlug"], {})[tr["id"]] = Trade(
                        r["recv_time"], round(float(tr["price"]["value"]), 4), float(tr["quantity"]["value"]),
                        tr["taker"]["side"] == "ORDER_SIDE_SELL", tr["id"], tr.get("tradeTime"))
    games = []
    from decimal import Decimal as D
    for slug, rows in books.items():
        rows.sort(key=lambda r: r["recv_time"])
        ev: list = []
        for r in rows:
            bids, asks = r.get("bids"), r.get("asks")
            if own_by_slug is not None:
                b = {D(p): D(q) for p, q in bids or []}
                a = {D(p): D(q) for p, q in asks or []}
                own_by_slug.advance(r["recv_time"])
                own_by_slug.subtract(slug, b, a)
                bids = [(p, q) for p, q in b.items() if q > 0]
                asks = [(p, q) for p, q in a.items() if q > 0]
            ev.append(_book(r["recv_time"], bids, asks, r.get("state"), r.get("transact_time")))
        ev += trades.get(slug, {}).values()
        ev.sort(key=lambda e: e.t)
        m = (session.get("markets") or {}).get(slug) or {}
        tick = float(m["tick_size"]) if m.get("tick_size") else infer_tick(ev)
        games.append(Game(slug, sport_of(slug), "tape", iso(m.get("start_time")), tick, ev, 3,
                          str(live_log) if live_log.exists() else None, session))
        if own_by_slug is not None:   # OwnOrders sweeps forward only, so one market per pass
            own_by_slug = OwnOrders(resting_rungs(live_log, session.get("pair_names", {})))
    return games


def load_pairs() -> dict[str, dict]:
    pairs: dict[str, dict] = {}
    for f in jsonl_logs.daily_files(XSPORTS, "pair"):
        with jsonl_logs.open_log(f) as h:
            for line in h:
                r = json.loads(line)
                if r.get("poly_slug"):
                    pairs[r["poly_slug"]] = r
    return pairs


def extract_slices(leagues=("nba", "nhl", "nfl", "cfb"), until: float | None = None, out: Path = SLICES) -> dict[str, int]:
    """Cuts every game's window out of the daily pbook and ptrade files, once, into per-game gzip files.
    A single pass per file, so it takes minutes and flat memory. Games starting at or after `until` are
    left out: those are the held-out test set."""
    pairs = {s: p for s, p in load_pairs().items() if p.get("league") in leagues and p.get("game_start")}
    starts = {s: iso(p["game_start"]) for s, p in pairs.items()}
    if until is not None:
        starts = {s: t for s, t in starts.items() if t < until}
    out.mkdir(parents=True, exist_ok=True)
    rx = re.compile(r'"slug": "([^"]+)"')
    rt = re.compile(r'"recv_time": ([\d.]+)')
    handles: dict[str, object] = {}
    counts: dict[str, int] = {}
    try:
        for kind in ("pbook", "ptrade"):
            for f in jsonl_logs.daily_files(XSPORTS, kind):
                with jsonl_logs.open_log(f) as h:
                    for line in h:
                        m = rx.search(line)
                        if not m or m.group(1) not in starts:
                            continue
                        slug = m.group(1)
                        t = float(rt.search(line).group(1))
                        if not starts[slug] - SLICE_BEFORE <= t <= starts[slug] + SLICE_AFTER:
                            continue
                        w = handles.get(slug)
                        if w is None:
                            w = handles[slug] = gzip.open(out / f"{slug}.jsonl.gz", "at", encoding="utf-8")
                        w.write(line)
                        counts[slug] = counts.get(slug, 0) + 1
    finally:
        for w in handles.values():
            w.close()
    return counts


def _xsports_trade(r: dict) -> Trade:
    return Trade(r["recv_time"], round(float(r["price"]), 4), float(r["qty"]), r["taker_side"] == "ORDER_SIDE_SELL", r.get("id"), r.get("trade_time"))


def load_slice(path: str | Path, pairs: dict[str, dict] | None = None) -> Game:
    """An early-game slice from extract_slices()."""
    slug = os.path.basename(str(path)).replace(".jsonl.gz", "")
    ev: list = []
    seen: set[str] = set()
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r["kind"] == "pbook":
                if not r.get("gap"):
                    ev.append(_book(r["recv_time"], r.get("bids"), r.get("asks"), r.get("state"), r.get("transact_time")))
            elif r.get("id") not in seen:
                seen.add(r.get("id"))
                ev.append(_xsports_trade(r))
    ev.sort(key=lambda e: e.t)
    p = (pairs or {}).get(slug) or {}
    return Game(slug, sport_of(slug), "early-game", iso(p.get("game_start")), infer_tick(ev), ev, 5)


def load_full_game(slug: str, days: list[str], pairs: dict[str, dict] | None = None) -> Game:
    """A full game from the depth tape: rebuilds the whole book at every pdepth row."""
    from xsports.recorder import apply_depth
    rx = f'"slug": "{slug}"'
    ev: list = []
    book: dict = {}
    for f in jsonl_logs.daily_files(XSPORTS, "pdepth"):
        if jsonl_logs.file_day(f) not in days:
            continue
        with jsonl_logs.open_log(f) as h:
            for line in h:
                if rx not in line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if not r.get("snap") and not r.get("gap") and not book:
                    continue
                apply_depth(book, r)
                if not r.get("gap"):
                    ev.append(_book(r["recv_time"], book["bids"].items(), book["asks"].items(), book.get("state"), r.get("transact_time")))
    seen: set[str] = set()
    for f in jsonl_logs.daily_files(XSPORTS, "ptrade"):
        if jsonl_logs.file_day(f) not in days:
            continue
        with jsonl_logs.open_log(f) as h:
            for line in h:
                if rx in line:
                    r = json.loads(line)
                    if r.get("id") not in seen:
                        seen.add(r.get("id"))
                        ev.append(_xsports_trade(r))
    ev.sort(key=lambda e: e.t)
    p = (pairs or {}).get(slug) or {}
    return Game(slug, sport_of(slug), "full", iso(p.get("game_start")), infer_tick(ev), ev, None)
