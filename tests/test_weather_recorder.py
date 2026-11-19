"""
Tests for weather_recorder.py's pure parsing and recording logic. Every
METAR and CLI string here is real, captured live 2026-09-24/25.
"""

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import weather_recorder as wr

KNYC_2351 = "KNYC 242351Z AUTO 05010G20KT 10SM CLR 17/06 A3031 RMK AO2 SLP253 T01720056 10189 20172 58006"
KNYC_0051 = "METAR KNYC 250051Z AUTO 05010G20KT 020V080 10SM CLR 17/06 A3031 RMK AO2 SLP254 T01670056"
KSFO_0056 = "METAR KSFO 250056Z 28017G25KT 10SM SCT180 BKN200 20/09 A2988 RMK AO2 PK WND 28032/0032 SLP117 T02000094 $"

CLI_FINAL = """CDUS41 KOKX 240617
CLINYC

CLIMATE REPORT
NATIONAL WEATHER SERVICE NEW YORK, NY
217 AM EDT THU SEP 24 2026

...THE CENTRAL PARK NY CLIMATE SUMMARY FOR SEPTEMBER 23 2026...

WEATHER ITEM   OBSERVED TIME   RECORD YEAR NORMAL DEPARTURE LAST
                VALUE   (LST)  VALUE       VALUE  FROM      YEAR
TEMPERATURE (F)
 YESTERDAY
  MAXIMUM         67    342 PM  97    1895  74     -7       83
  MINIMUM         55   1159 PM  41    1947  60     -5       65
"""

CLI_PRELIM = """CDUS41 KOKX 242037
CLINYC

...THE CENTRAL PARK NY CLIMATE SUMMARY FOR SEPTEMBER 24 2026...
VALID TODAY AS OF 0400 PM LOCAL TIME.

TEMPERATURE (F)
 TODAY
  MAXIMUM         66    259 PM  91    2017  73     -7       80
  MINIMUM         53    644 AM  40    1963  60     -7       70
"""


class TestMetarParsing(unittest.TestCase):
    def test_tenths_and_six_hour_groups(self):
        g = wr.parse_metar_groups(KNYC_2351)
        self.assertEqual(g["temp_c"], Decimal("17.2"))
        self.assertEqual(g["max6h_c"], Decimal("18.9"))
        self.assertEqual(g["min6h_c"], Decimal("17.2"))
        self.assertIsNone(g["max24h_c"])

    def test_hourly_without_extremes(self):
        g = wr.parse_metar_groups(KNYC_0051)
        self.assertEqual(g["temp_c"], Decimal("16.7"))
        self.assertIsNone(g["max6h_c"])
        self.assertIsNone(g["min6h_c"])

    def test_body_groups_are_not_misread_as_extremes(self):
        # 10SM (visibility) and 28017G25KT sit in the body; only remarks count.
        g = wr.parse_metar_groups(KSFO_0056)
        self.assertEqual(g["temp_c"], Decimal("20.0"))
        self.assertIsNone(g["max6h_c"])

    def test_negative_and_24_hour_groups(self):
        raw = "KMDW 010551Z 00000KT 10SM CLR M02/M05 A3010 RMK AO2 T10221050 11011 21033 400111033"
        g = wr.parse_metar_groups(raw)
        self.assertEqual(g["temp_c"], Decimal("-2.2"))
        self.assertEqual(g["max6h_c"], Decimal("-1.1"))
        self.assertEqual(g["min6h_c"], Decimal("-3.3"))
        self.assertEqual(g["max24h_c"], Decimal("1.1"))
        self.assertEqual(g["min24h_c"], Decimal("-3.3"))

    def test_body_temperature_fallback_without_t_group(self):
        g = wr.parse_metar_groups("KMIA 250053Z 09008KT 10SM FEW025 28/M01 A3001")
        self.assertEqual(g["temp_c"], Decimal("28"))

    def test_c_to_f(self):
        self.assertEqual(wr.c_to_f(Decimal("18.9")), Decimal("66.02"))
        self.assertIsNone(wr.c_to_f(None))


class TestCliParsing(unittest.TestCase):
    def test_final_report(self):
        p = wr.parse_cli_text(CLI_FINAL)
        self.assertEqual(p["summary_date"], "2026-09-23")
        self.assertFalse(p["preliminary"])
        self.assertEqual(p["max_f"], 67)
        self.assertEqual(p["max_time_lst"], "342 PM")

    def test_preliminary_report(self):
        p = wr.parse_cli_text(CLI_PRELIM)
        self.assertEqual(p["summary_date"], "2026-09-24")
        self.assertTrue(p["preliminary"])
        self.assertEqual(p["valid_as_of"], "0400 PM")
        self.assertEqual(p["max_f"], 66)

    def test_colon_time_format(self):
        # KMFL (Miami) writes times as "2:59 PM", not "259 PM".
        p = wr.parse_cli_text(CLI_PRELIM.replace("66    259 PM", "91   2:59 PM"))
        self.assertEqual(p["max_f"], 91)
        self.assertEqual(p["max_time_lst"], "2:59 PM")

    def test_missing_maximum(self):
        p = wr.parse_cli_text(CLI_FINAL.replace("MAXIMUM         67    342 PM", "MAXIMUM         MM"))
        self.assertIsNone(p["max_f"])


class TestClimateDates(unittest.TestCase):
    def test_lst_not_clock_time(self):
        nyc = wr.CITIES[0]
        # 04:30Z = 00:30 EDT on the 25th, but still 23:30 EST on the 24th.
        now = datetime(2026, 9, 25, 4, 30, tzinfo=timezone.utc)
        self.assertEqual(wr.climate_dates(nyc, now), ["2026-09-23", "2026-09-24", "2026-09-25"])
        now = datetime(2026, 9, 25, 5, 30, tzinfo=timezone.utc)
        self.assertEqual(wr.climate_dates(nyc, now)[1], "2026-09-25")


class TestForecast(unittest.TestCase):
    # Real shapes from api.weather.gov gridpoints/OKX/34,45, 2026-09-25.
    PERIODS = [
        {"startTime": "2026-09-24T23:00:00-04:00", "temperature": 61},  # 22:00 EST, the 24th
        {"startTime": "2026-09-25T00:00:00-04:00", "temperature": 60},  # 23:00 EST, still the 24th
        {"startTime": "2026-09-25T01:00:00-04:00", "temperature": 59},  # 00:00 EST, the 25th
        {"startTime": "2026-09-25T15:00:00-04:00", "temperature": 69},
        {"startTime": "2026-09-26T00:30:00-04:00", "temperature": 72},  # 23:30 EST, still the 25th
        {"startTime": "2026-09-26T01:00:00-04:00", "temperature": 80},  # the 26th
    ]

    def test_hourly_day_max_uses_lst_day(self):
        nyc = wr.CITIES[0]
        self.assertEqual(wr.hourly_day_max(self.PERIODS, nyc, "2026-09-24"), 61)
        self.assertEqual(wr.hourly_day_max(self.PERIODS, nyc, "2026-09-25"), 72)
        self.assertIsNone(wr.hourly_day_max(self.PERIODS, nyc, "2026-09-28"))

    def test_forecast_record(self):
        nyc = wr.CITIES[0]
        hourly = {"updateTime": "2026-09-25T01:58:19+00:00", "periods": self.PERIODS}
        grid = {"updateTime": "2026-09-25T01:58:19+00:00", "maxTemperature": {"uom": "wmoUnit:degC", "values": [
            {"validTime": "2026-09-24T19:00:00+00:00/PT6H", "value": 20},
            {"validTime": "2026-09-25T12:00:00+00:00/PT13H", "value": 20.555555555555557}]}}
        now = datetime(2026, 9, 25, 2, 5, tzinfo=timezone.utc)  # 21:05 EST on the 24th
        rec = wr.forecast_record(nyc, hourly, grid, now)
        self.assertEqual(rec["hourly_max_f"], {"2026-09-24": 61, "2026-09-25": 72})
        self.assertEqual(rec["grid_max_f"][1], ["2026-09-25T12:00:00+00:00/PT13H", "69.00"])
        self.assertEqual(len(rec["hourly"]), 6)


class TestObservationRecords(unittest.TestCase):
    def test_awc_dedupe_and_backfill(self):
        d = wr.ObservationDeduper()
        payload = [{"icaoId": "KNYC", "rawOb": KNYC_0051, "obsTime": 1790297460, "metarType": "METAR",
                    "receiptTime": "2026-09-25T00:54:01.000Z", "temp": 16.7}]
        first = wr.awc_records(payload, d, d.backfill("awc"))
        d.mark_polled("awc")
        self.assertEqual(len(first), 1)
        self.assertTrue(first[0]["backfill"])
        self.assertEqual(first[0]["temp_c"], "16.7")
        self.assertEqual(first[0]["obs_time"], "2026-09-25T00:51:00+00:00")
        self.assertEqual(wr.awc_records(payload, d, d.backfill("awc")), [])
        newer = [dict(payload[0], rawOb=KNYC_0051.replace("250051Z", "250151Z"))]
        second = wr.awc_records(newer, d, d.backfill("awc"))
        self.assertFalse(second[0]["backfill"])

    def test_nws_record(self):
        d = wr.ObservationDeduper()
        payload = {"properties": {"timestamp": "2026-09-24T23:51:00+00:00", "rawMessage": KNYC_2351,
                                  "temperature": {"value": 17.2}}}
        rec = wr.nws_record("KNYC", payload, d, True)
        self.assertEqual(rec["max6h_f"], "66.02")
        self.assertIsNone(wr.nws_record("KNYC", payload, d, False))

    def test_nws_reading_without_metar(self):
        # Real shape seen for KMIA at 00:50Z: no rawMessage, whole-degree C.
        d = wr.ObservationDeduper()
        payload = {"properties": {"timestamp": "2026-09-25T00:50:00+00:00", "rawMessage": "",
                                  "temperature": {"value": 28}}}
        rec = wr.nws_record("KMIA", payload, d, False)
        self.assertFalse(rec["has_raw"])
        self.assertEqual(rec["temp_c"], "28")
        self.assertEqual(rec["temp_f"], "82.40")
        self.assertIsNone(rec["max6h_f"])


class TestFailureRecording(unittest.TestCase):
    def test_timeout_has_a_type_even_with_empty_message(self):
        info = wr.describe_error(wr.httpx.ReadTimeout(""))
        self.assertEqual(info["error_type"], "ReadTimeout")

    def test_http_status_and_body_captured(self):
        req = wr.httpx.Request("GET", wr.AWC_METAR_URL)
        resp = wr.httpx.Response(502, request=req, text="Bad Gateway")
        exc = wr.httpx.HTTPStatusError("502", request=req, response=resp)
        info = wr.describe_error(exc)
        self.assertEqual((info["status"], info["body"]), (502, "Bad Gateway"))

    def test_failure_written_as_health_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            w = wr.JsonlWriter(Path(tmp), clock=lambda: 1790297460.0)
            with self.assertLogs("weather_recorder", level="WARNING") as logs:
                wr.record_failure(w, "awc", "KNYC", wr.httpx.ConnectTimeout(""))
            w.close()
            row = json.loads((Path(tmp) / "health_2026-09-25.jsonl").read_text())
            self.assertEqual((row["source"], row["error_type"]), ("awc", "ConnectTimeout"))
            self.assertIn("ConnectTimeout", logs.output[0])


class TestMarketRecording(unittest.TestCase):
    def test_top_levels_sorted(self):
        levels = [{"px": {"value": "0.30"}, "qty": "5"}, {"px": {"value": "0.32"}, "qty": "7"},
                  {"px": {"value": "0.31"}, "qty": "1"}, {"px": {"value": "0.29"}, "qty": "2"}]
        self.assertEqual(wr.top_levels(levels, descending=True), [["0.32", "7"], ["0.31", "1"], ["0.30", "5"]])
        self.assertEqual(wr.top_levels(None, descending=False), [])

    def test_book_written_only_on_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            w = wr.JsonlWriter(Path(tmp), clock=lambda: 1790297460.0)
            feed = wr.MarketFeed(["s"], w, "k", "s")
            msg = {"marketData": {"marketSlug": "s", "bids": [{"px": {"value": "0.30"}, "qty": "26"}],
                                  "offers": [{"px": {"value": "0.31"}, "qty": "32"}], "state": "MARKET_STATE_OPEN",
                                  "stats": {"sharesTraded": "100", "lastTradePx": {"value": "0.30"}}}}
            feed.on_market_data(msg)
            feed.on_market_data(msg)
            msg["marketData"]["stats"]["sharesTraded"] = "103"
            feed.on_market_data(msg)
            feed.on_trade({"trade": {"x": 1}})
            w.close()
            rows = [json.loads(l) for l in (Path(tmp) / "book_2026-09-25.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["shares_traded"], "103")
            self.assertEqual(rows[0]["bids"], [["0.30", "26"]])
            self.assertEqual(w.counts, {"book": 2, "trade": 1})

    def test_event_markets_unwraps(self):
        ev, ms = wr.event_markets({"event": {"slug": "e", "markets": [{"slug": "a"}, "junk"]}})
        self.assertEqual(ev["slug"], "e")
        self.assertEqual(ms, [{"slug": "a"}])


if __name__ == "__main__":
    unittest.main()
