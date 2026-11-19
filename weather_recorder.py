"""
Read-only weather data recorder (started 2026-09-24). Places NO orders and never touches the
trading client's order endpoints.

For every city with a Polymarket US daily-high market, it records, with
OUR receive time on every row:
  - obs:     each new surface observation, from two sources
               awc -- aviationweather.gov METAR API (all stations, one call)
               nws -- api.weather.gov stations/{id}/observations/latest
             First-seen rows only, so recv_time - obs_time is each source's
             real delivery lag and the awc-vs-nws gap is directly comparable.
  - cli:     each NWS Climatological Report (Daily) as it is issued: the
             afternoon preliminary ("VALID TODAY AS OF 0400 PM") and the
             after-midnight final these markets settle against.
  - book:    top-of-book changes for every bucket market (websocket)
  - trade:   raw trade messages for every bucket market (websocket)
  - market:  bucket market metadata, once per newly discovered slug
  - forecast: the NWS forecast for each station's grid point, whenever
             its updateTime changes -- hourly temps for the next 48h, the
             max of those per LST day, and the NWS forecast daily high.

What it is for: measuring each public data source's delivery latency and
recording market data alongside it on one clock, for offline analysis.

Facts confirmed live 2026-09-24 that shape this file:
  - Markets settle on the NWS CLI report for one station per city. The
    CLI's "OBSERVED TIME" column is labeled (LST), and the Polymarket
    events start at local-STANDARD-time midnight (temp-nychigh-2026-09-24
    starts 05:00Z = 00:00 EST = 01:00 EDT), so the climate day is
    midnight-to-midnight LST, i.e. 1am-1am local clock time during DST.
    Event slug dates are therefore computed in LST, not local clock time.

Output: logs/weather/<kind>_<UTC date>.jsonl, one JSON object per line.

Usage:
  py weather_recorder.py                 # all cities
  py weather_recorder.py --cities nyc,mia
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import httpx

log = logging.getLogger("weather_recorder")

USER_AGENT = "PredictionMarkets weather recorder (research, read-only)"
AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"
NWS_LATEST_URL = "https://api.weather.gov/stations/{station}/observations/latest"
NWS_CLI_LIST_URL = "https://api.weather.gov/products/types/CLI/locations/{location}"
NWS_PRODUCT_URL = "https://api.weather.gov/products/{id}"
NWS_STATION_URL = "https://api.weather.gov/stations/{station}"
NWS_POINTS_URL = "https://api.weather.gov/points/{lat:.4f},{lon:.4f}"

AWC_POLL_SECONDS = 30.0
NWS_POLL_SECONDS = 60.0
CLI_POLL_SECONDS = 300.0
# The NWS forecast updates roughly hourly; a new row is written only when
# its updateTime changes, so this just bounds how late we notice.
FORECAST_POLL_SECONDS = 600.0
FORECAST_HOURS_KEPT = 48
GRID_MAX_PERIODS_KEPT = 3
DISCOVERY_SECONDS = 900.0
# gateway.polymarket.us Cloudflare-bans the IP after a ~20-call burst (see
# the project's rate-limit notes), and a ban would also block the live bot.
DISCOVERY_CALL_SPACING_SECONDS = 2.0
# Weather buckets can sit untouched for many minutes; a short threshold
# would reconnect a healthy socket over and over.
WS_STALE_AFTER = 900.0
BOOK_LEVELS = 3
CLI_BACKFILL_PER_LOCATION = 4


@dataclass(frozen=True)
class City:
    code: str
    station: str           # ICAO id the CLI report and markets settle on
    event_prefix: str      # Polymarket US event slug prefix
    cli_location: str      # api.weather.gov CLI product location id
    std_utc_offset_hours: int  # local STANDARD time; the climate day is LST


# Confirmed live 2026-09-24: each event's market description names this
# station, and each CLI location returns reports from the matching office.
CITIES: list[City] = [
    City("nyc", "KNYC", "temp-nychigh", "NYC", -5),
    City("mia", "KMIA", "temp-miahigh", "MIA", -5),
    City("chi", "KMDW", "temp-mdwhigh", "MDW", -6),
    City("lax", "KLAX", "temp-laxhigh", "LAX", -8),
    City("sfo", "KSFO", "temp-sfohigh", "SFO", -8),
]


# --------------------------------------------------------------------------
# Pure parsing
# --------------------------------------------------------------------------


def _signed_tenths(sign: str, digits: str) -> Decimal:
    value = Decimal(digits) / 10
    return -value if sign == "1" else value


def c_to_f(celsius: Decimal | None) -> Decimal | None:
    if celsius is None:
        return None
    return (celsius * 9 / 5 + 32).quantize(Decimal("0.01"))


def parse_metar_groups(raw: str) -> dict[str, Decimal | None]:
    """Temperature fields from a raw METAR, in Celsius.

    temp_c comes from the remarks T-group (tenths) when present, since the
    body group (e.g. 17/06) is whole degrees. The 6-hour and 24-hour
    max/min groups only appear in remarks, so they are only searched for
    after RMK -- that keeps body groups like wind or visibility from being
    misread as a 1snTTT group.
    """
    out: dict[str, Decimal | None] = {
        "temp_c": None, "max6h_c": None, "min6h_c": None, "max24h_c": None, "min24h_c": None,
    }
    body, _, remarks = raw.partition(" RMK ")
    t = re.search(r"\bT([01])(\d{3})([01])(\d{3})\b", remarks)
    if t:
        out["temp_c"] = _signed_tenths(t.group(1), t.group(2))
    else:
        m = re.search(r"\s(M?\d{2})/(M?\d{2})?\s", f" {body} ")
        if m:
            v = m.group(1)
            out["temp_c"] = -Decimal(v[1:]) if v.startswith("M") else Decimal(v)
    for key, pattern in (("max6h_c", r"(?<!\S)1([01])(\d{3})(?!\S)"), ("min6h_c", r"(?<!\S)2([01])(\d{3})(?!\S)")):
        g = re.search(pattern, remarks)
        if g:
            out[key] = _signed_tenths(g.group(1), g.group(2))
    g24 = re.search(r"(?<!\S)4([01])(\d{3})([01])(\d{3})(?!\S)", remarks)
    if g24:
        out["max24h_c"] = _signed_tenths(g24.group(1), g24.group(2))
        out["min24h_c"] = _signed_tenths(g24.group(3), g24.group(4))
    return out


def parse_cli_text(text: str) -> dict[str, Any]:
    """The settlement-relevant fields of a CLI product.

    preliminary is True for the afternoon "VALID TODAY AS OF ..." issue; the
    after-midnight issue for the same date has no such line and is the one
    the markets settle on. max_f is None when the report shows MM (missing).
    """
    summary = re.search(r"CLIMATE SUMMARY FOR ([A-Z]+ \d{1,2} \d{4})", text)
    summary_date = None
    if summary:
        try:
            summary_date = datetime.strptime(summary.group(1).title(), "%B %d %Y").date().isoformat()
        except ValueError:
            summary_date = None
    valid_as_of = re.search(r"VALID (?:TODAY )?AS OF (\d{3,4} [AP]M)", text)
    max_f = max_time = None
    temp_section = text.split("TEMPERATURE (F)", 1)
    if len(temp_section) == 2:
        m = re.search(r"^\s+MAXIMUM\s+(-?\d+|MM)\*?\s+(\d{1,2}:?\d{2} [AP]M)?", temp_section[1], re.MULTILINE)
        if m:
            max_f = None if m.group(1) == "MM" else int(m.group(1))
            max_time = m.group(2)
    return {
        "summary_date": summary_date,
        "preliminary": valid_as_of is not None,
        "valid_as_of": valid_as_of.group(1) if valid_as_of else None,
        "max_f": max_f,
        "max_time_lst": max_time,
    }


def climate_dates(city: City, now_utc: datetime) -> list[str]:
    """LST dates whose events are worth watching: yesterday (settling),
    today (live) and tomorrow (already listed and trading)."""
    lst_today = (now_utc + timedelta(hours=city.std_utc_offset_hours)).date()
    return [(lst_today + timedelta(days=d)).isoformat() for d in (-1, 0, 1)]


def lst_day_bounds(city: City, lst_date: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(lst_date).replace(tzinfo=timezone.utc) - timedelta(hours=city.std_utc_offset_hours)
    return start, start + timedelta(days=1)


def hourly_day_max(periods: list[dict], city: City, lst_date: str) -> int | None:
    """Highest hourly-forecast temperature (F) whose hour starts inside the
    LST climate day. None when the forecast doesn't cover any of it --
    including when the day is partly over, since the hourly forecast only
    runs forward and so is then the max of the REMAINING hours only."""
    start, end = lst_day_bounds(city, lst_date)
    temps = [p["temperature"] for p in periods
             if p.get("temperature") is not None and start <= datetime.fromisoformat(p["startTime"]) < end]
    return max(temps) if temps else None


def forecast_record(city: City, hourly_props: dict, grid_props: dict, now_utc: datetime) -> dict[str, Any]:
    """One snapshot of the NWS forecast for a station's grid point.

    hourly_max_f[date] is the max over the hourly forecast for the rest of
    that LST day (see hourly_day_max). grid_max_f is the NWS forecast daily
    high itself (gridpoint maxTemperature, published in C over a daytime
    window like 12Z/PT13H), converted to F.
    """
    periods = hourly_props.get("periods") or []
    _, today, tomorrow = climate_dates(city, now_utc)
    grid_max = ((grid_props.get("maxTemperature") or {}).get("values") or [])[:GRID_MAX_PERIODS_KEPT]
    return {
        "city": city.code, "station": city.station,
        "update_time": hourly_props.get("updateTime"),
        "grid_update_time": grid_props.get("updateTime"),
        "hourly_max_f": {d: hourly_day_max(periods, city, d) for d in (today, tomorrow)},
        "grid_max_f": [[v.get("validTime"), str(c_to_f(Decimal(str(v["value"]))))] for v in grid_max if v.get("value") is not None],
        "hourly": [[p.get("startTime"), p.get("temperature")] for p in periods[:FORECAST_HOURS_KEPT]],
    }


def top_levels(levels: list[dict] | None, descending: bool, n: int = BOOK_LEVELS) -> list[list[str]]:
    parsed = [(Decimal(l["px"]["value"]), l["qty"]) for l in (levels or [])]
    parsed.sort(key=lambda x: x[0], reverse=descending)
    return [[str(p), str(q)] for p, q in parsed[:n]]


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------


class JsonlWriter:
    """Appends one JSON object per line to logs/weather/<kind>_<UTC date>.jsonl,
    flushing every line so a crash loses nothing already seen."""

    def __init__(self, directory: Path, clock: Callable[[], float] = time.time) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._files: dict[tuple[str, str], Any] = {}
        self.counts: dict[str, int] = {}

    def write(self, kind: str, record: dict[str, Any]) -> None:
        now = self.clock()
        day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
        key = (kind, day)
        f = self._files.get(key)
        if f is None:
            for old in [k for k in self._files if k[0] == kind]:
                self._files.pop(old).close()
            f = open(self.directory / f"{kind}_{day}.jsonl", "a", encoding="utf-8")
            self._files[key] = f
        row = {"kind": kind, "recv_time": now, "recv_iso": datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="milliseconds")}
        row.update(record)
        f.write(json.dumps(row, default=str) + "\n")
        f.flush()
        self.counts[kind] = self.counts.get(kind, 0) + 1

    def close(self) -> None:
        for f in self._files.values():
            f.close()
        self._files.clear()


# --------------------------------------------------------------------------
# Observation sources
# --------------------------------------------------------------------------


def describe_error(exc: BaseException) -> dict[str, Any]:
    """Type, HTTP status and a short body for a failed poll. str() alone is
    empty for httpx timeouts, which left a 2026-09-25 AWC failure with no
    recorded cause."""
    info: dict[str, Any] = {"error_type": type(exc).__name__, "error": str(exc)[:300]}
    response = getattr(exc, "response", None)
    if isinstance(exc, httpx.HTTPStatusError) and response is not None:
        info["status"] = response.status_code
        info["body"] = response.text[:300]
    return info


def record_failure(writer: JsonlWriter, source: str, target: str, exc: BaseException) -> None:
    """Logs a failed poll and writes it as a `health` row, so the analysis
    can tell a quiet source from one we couldn't reach."""
    info = describe_error(exc)
    log.warning("%s poll failed for %s: %s%s %s", source, target, info["error_type"],
                f" (HTTP {info['status']})" if "status" in info else "", info["error"])
    writer.write("health", {"source": source, "target": target, **info})


class ObservationDeduper:
    """First-seen tracking per (source, station, report). The first poll of
    each source is marked backfill=True: those rows were published before
    the recorder started, so their lag says nothing about the source."""

    def __init__(self) -> None:
        self._seen: set[tuple[str, str, str]] = set()
        self._polled_once: set[str] = set()

    def is_new(self, source: str, station: str, key: str) -> bool:
        k = (source, station, key)
        if k in self._seen:
            return False
        self._seen.add(k)
        return True

    def backfill(self, source: str) -> bool:
        return source not in self._polled_once

    def mark_polled(self, source: str) -> None:
        self._polled_once.add(source)


def obs_record(source: str, station: str, obs_time: str, raw: str, feed_temp_c: Any, extra: dict[str, Any], backfill: bool) -> dict[str, Any]:
    """has_raw=False marks a reading with no METAR behind it. Seen live
    2026-09-25 from api.weather.gov for KMIA/KMDW/KLAX at :50 and :00 --
    between the hourly METARs, whole-degree C, so likely the stations'
    5-minute data. temp_c then falls back to the feed's own value."""
    groups = parse_metar_groups(raw) if raw else {
        "temp_c": None, "max6h_c": None, "min6h_c": None, "max24h_c": None, "min24h_c": None,
    }
    if groups["temp_c"] is None and feed_temp_c is not None:
        groups["temp_c"] = Decimal(str(feed_temp_c))
    rec: dict[str, Any] = {"source": source, "station": station, "obs_time": obs_time, "backfill": backfill,
                           "has_raw": bool(raw), "raw": raw, "feed_temp_c": feed_temp_c}
    for k, v in groups.items():
        rec[k] = str(v) if v is not None else None
        rec[k[:-2] + "_f"] = str(c_to_f(v)) if v is not None else None
    rec.update(extra)
    return rec


def awc_records(payload: list[dict], deduper: ObservationDeduper, backfill: bool) -> list[dict[str, Any]]:
    out = []
    for m in payload:
        station, raw = m.get("icaoId"), m.get("rawOb") or ""
        if not station or not raw or not deduper.is_new("awc", station, raw):
            continue
        obs_epoch = m.get("obsTime")
        obs_time = datetime.fromtimestamp(obs_epoch, timezone.utc).isoformat() if obs_epoch else None
        out.append(obs_record("awc", station, obs_time, raw, m.get("temp"), {
            "metar_type": m.get("metarType"),
            "source_receipt_time": m.get("receiptTime"),
        }, backfill))
    return out


def nws_record(station: str, payload: dict, deduper: ObservationDeduper, backfill: bool) -> dict[str, Any] | None:
    props = payload.get("properties") or {}
    ts = props.get("timestamp")
    if not ts or not deduper.is_new("nws", station, ts):
        return None
    temp = (props.get("temperature") or {}).get("value")
    return obs_record("nws", station, ts, props.get("rawMessage") or "", temp, {}, backfill)


async def poll_awc(http: httpx.AsyncClient, cities: list[City], writer: JsonlWriter, deduper: ObservationDeduper) -> None:
    ids = ",".join(c.station for c in cities)
    while True:
        try:
            r = await http.get(AWC_METAR_URL, params={"ids": ids, "format": "json", "hours": 3})
            r.raise_for_status()
            backfill = deduper.backfill("awc")
            for rec in awc_records(r.json() or [], deduper, backfill):
                writer.write("obs", rec)
            deduper.mark_polled("awc")
        except Exception as exc:
            record_failure(writer, "awc", ids, exc)
        await asyncio.sleep(AWC_POLL_SECONDS)


async def poll_nws(http: httpx.AsyncClient, cities: list[City], writer: JsonlWriter, deduper: ObservationDeduper) -> None:
    while True:
        backfill = deduper.backfill("nws")
        for c in cities:
            try:
                r = await http.get(NWS_LATEST_URL.format(station=c.station))
                r.raise_for_status()
                rec = nws_record(c.station, r.json(), deduper, backfill)
                if rec:
                    writer.write("obs", rec)
            except Exception as exc:
                record_failure(writer, "nws", c.station, exc)
        deduper.mark_polled("nws")
        await asyncio.sleep(NWS_POLL_SECONDS)


async def poll_cli(http: httpx.AsyncClient, cities: list[City], writer: JsonlWriter) -> None:
    seen: set[str] = set()
    first = True
    while True:
        for c in cities:
            try:
                r = await http.get(NWS_CLI_LIST_URL.format(location=c.cli_location))
                r.raise_for_status()
                products = (r.json() or {}).get("@graph") or []
                if first:
                    for p in products[CLI_BACKFILL_PER_LOCATION:]:
                        seen.add(p["id"])
                for p in products:
                    if p["id"] in seen:
                        continue
                    pr = await http.get(NWS_PRODUCT_URL.format(id=p["id"]))
                    pr.raise_for_status()
                    text = (pr.json() or {}).get("productText") or ""
                    seen.add(p["id"])
                    rec = {"city": c.code, "station": c.station, "product_id": p["id"],
                           "issuance_time": p.get("issuanceTime"), "office": p.get("issuingOffice"),
                           "backfill": first, "text": text}
                    rec.update(parse_cli_text(text))
                    writer.write("cli", rec)
                    if not first:
                        log.info("CLI %s %s: max %s F (%s)", c.code, rec["summary_date"], rec["max_f"],
                                 "preliminary" if rec["preliminary"] else "final")
            except Exception as exc:
                record_failure(writer, "cli", c.cli_location, exc)
        first = False
        await asyncio.sleep(CLI_POLL_SECONDS)


async def resolve_forecast_urls(http: httpx.AsyncClient, city: City) -> tuple[str, str]:
    """Station -> its coordinates -> the NWS grid point covering them."""
    r = await http.get(NWS_STATION_URL.format(station=city.station))
    r.raise_for_status()
    lon, lat = r.json()["geometry"]["coordinates"]
    r = await http.get(NWS_POINTS_URL.format(lat=lat, lon=lon))
    r.raise_for_status()
    props = r.json()["properties"]
    return props["forecastHourly"], props["forecastGridData"]


async def poll_forecasts(http: httpx.AsyncClient, cities: list[City], writer: JsonlWriter) -> None:
    urls: dict[str, tuple[str, str]] = {}
    last_update: dict[str, str] = {}
    while True:
        for c in cities:
            try:
                if c.code not in urls:
                    urls[c.code] = await resolve_forecast_urls(http, c)
                hourly_url, grid_url = urls[c.code]
                r = await http.get(hourly_url)
                r.raise_for_status()
                hourly = r.json()["properties"]
                if hourly.get("updateTime") == last_update.get(c.code):
                    continue
                g = await http.get(grid_url)
                g.raise_for_status()
                rec = forecast_record(c, hourly, g.json()["properties"], datetime.now(timezone.utc))
                writer.write("forecast", rec)
                last_update[c.code] = hourly.get("updateTime")
                log.info("forecast %s updated %s: hourly max %s", c.code, rec["update_time"], rec["hourly_max_f"])
            except Exception as exc:
                record_failure(writer, "forecast", c.code, exc)
        await asyncio.sleep(FORECAST_POLL_SECONDS)


# --------------------------------------------------------------------------
# Polymarket US: discovery + websocket
# --------------------------------------------------------------------------


def event_markets(resp: Any) -> tuple[dict, list[dict]]:
    event = resp.get("event", resp) if isinstance(resp, dict) else {}
    return event, [m for m in (event.get("markets") or []) if isinstance(m, dict)]


class MarketFeed:
    """Book + trade websocket for the current slug set. Records top-of-book
    only when it changes, plus every trade message raw (its payload shape
    was not captured before this was written, so nothing is assumed)."""

    def __init__(self, slugs: list[str], writer: JsonlWriter, key_id: str, secret_key: str) -> None:
        self.slugs = slugs
        self.writer = writer
        self.key_id = key_id
        self.secret_key = secret_key
        self._last_top: dict[str, tuple] = {}
        self._last_msg_at = 0.0

    async def run(self) -> None:
        from polymarket_us import AsyncPolymarketUS

        client = AsyncPolymarketUS(key_id=self.key_id, secret_key=self.secret_key)
        ws = client.ws.markets()
        ws.on("market_data", self.on_market_data)
        ws.on("trade", self.on_trade)
        await ws.connect()
        try:
            await ws.subscribe("wx-md", "SUBSCRIPTION_TYPE_MARKET_DATA", self.slugs)
            await ws.subscribe("wx-tr", "SUBSCRIPTION_TYPE_TRADE", self.slugs)
            log.info("polymarket_us: recording %d bucket markets", len(self.slugs))
            self._last_msg_at = time.time()
            while True:
                await asyncio.sleep(5)
                if time.time() - self._last_msg_at > WS_STALE_AFTER:
                    raise RuntimeError("no market data received recently")
        finally:
            await ws.close()

    def on_market_data(self, data: dict) -> None:
        self._last_msg_at = time.time()
        md = data.get("marketData", data)
        slug = md.get("marketSlug") or md.get("slug")
        if not slug:
            return
        stats = md.get("stats") or {}
        bids = top_levels(md.get("bids"), descending=True)
        asks = top_levels(md.get("offers"), descending=False)
        state = md.get("state")
        volume = stats.get("sharesTraded")
        last_px = (stats.get("lastTradePx") or {}).get("value")
        top = (json.dumps(bids), json.dumps(asks), state, volume, last_px)
        if self._last_top.get(slug) == top:
            return
        self._last_top[slug] = top
        self.writer.write("book", {"slug": slug, "transact_time": md.get("transactTime") or data.get("transactTime"),
                                   "state": state, "bids": bids, "asks": asks,
                                   "shares_traded": volume, "last_trade_px": last_px})

    def on_trade(self, data: dict) -> None:
        self._last_msg_at = time.time()
        self.writer.write("trade", {"payload": data})


async def discover(poly_client: Any, cities: list[City], writer: JsonlWriter, known: dict[str, dict]) -> set[str]:
    """Open bucket slugs for yesterday/today/tomorrow (LST) in every city.
    Sync SDK calls run in a thread, spaced out for the gateway rate limit."""
    now = datetime.now(timezone.utc)
    slugs: set[str] = set()
    for c in cities:
        for d in climate_dates(c, now):
            event_slug = f"{c.event_prefix}-{d}"
            try:
                resp = await asyncio.to_thread(poly_client.events.retrieve_by_slug, event_slug)
            except Exception as exc:
                log.debug("event %s not available: %s", event_slug, exc)
                await asyncio.sleep(DISCOVERY_CALL_SPACING_SECONDS)
                continue
            _, markets = event_markets(resp)
            for m in markets:
                slug = m.get("slug")
                if not slug or m.get("closed"):
                    continue
                slugs.add(slug)
                if slug not in known:
                    known[slug] = m
                    writer.write("market", {"city": c.code, "station": c.station, "event_slug": event_slug,
                                            "climate_date": d, "slug": slug,
                                            "question": m.get("question"), "description": m.get("description"),
                                            "tick_size": m.get("orderPriceMinTickSize"),
                                            "game_start_time": m.get("gameStartTime"), "end_date": m.get("endDate")})
            await asyncio.sleep(DISCOVERY_CALL_SPACING_SECONDS)
    return slugs


async def run_markets(cities: list[City], writer: JsonlWriter, key_id: str, secret_key: str) -> None:
    from polymarket_us import PolymarketUS
    from orderbook_feed import run_forever

    poly_client = PolymarketUS(key_id=key_id, secret_key=secret_key)
    known: dict[str, dict] = {}
    current: set[str] = set()
    feed_task: asyncio.Task | None = None
    while True:
        try:
            slugs = await discover(poly_client, cities, writer, known)
        except Exception as exc:
            record_failure(writer, "discovery", "polymarket_us", exc)
            slugs = current
        if slugs and slugs != current:
            if feed_task:
                feed_task.cancel()
                try:
                    await feed_task
                except (asyncio.CancelledError, Exception):
                    pass
            current = slugs
            feed = MarketFeed(sorted(current), writer, key_id, secret_key)
            feed_task = asyncio.create_task(run_forever("weather market feed", feed.run))
        elif not slugs:
            log.warning("no open weather bucket markets found")
        await asyncio.sleep(DISCOVERY_SECONDS)


async def heartbeat(writer: JsonlWriter) -> None:
    while True:
        await asyncio.sleep(300)
        log.info("recorded so far: %s", ", ".join(f"{k} {v}" for k, v in sorted(writer.counts.items())) or "nothing")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cities", default=",".join(c.code for c in CITIES), help="comma-separated city codes")
    parser.add_argument("--out", default=str(Path(__file__).parent / "logs" / "weather"))
    parser.add_argument("--no-markets", action="store_true", help="weather sources only, no Polymarket connection")
    args = parser.parse_args()

    Path(args.out).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(Path(args.out) / "recorder.log", encoding="utf-8")])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    wanted = {c.strip() for c in args.cities.split(",") if c.strip()}
    cities = [c for c in CITIES if c.code in wanted]
    if not cities:
        parser.error(f"no known cities in {args.cities!r}; known: {', '.join(c.code for c in CITIES)}")

    writer = JsonlWriter(Path(args.out))
    deduper = ObservationDeduper()
    log.info("weather recorder (read-only) -- cities %s -> %s", ", ".join(c.code for c in cities), args.out)
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}, timeout=15.0) as http:
        tasks = [poll_awc(http, cities, writer, deduper), poll_nws(http, cities, writer, deduper),
                 poll_cli(http, cities, writer), poll_forecasts(http, cities, writer), heartbeat(writer)]
        if not args.no_markets:
            from dotenv import load_dotenv
            load_dotenv(Path(__file__).parent / "API_Setup" / ".env")
            tasks.append(run_markets(cities, writer, os.environ["POLYMARKET_KEY_ID"], os.environ["POLYMARKET_SECRET_KEY_ID"]))
        try:
            await asyncio.gather(*tasks)
        finally:
            writer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
