"""sports_feed.mlb_recorder: linescore/feed parsing, change-only emission,
game lookup, cache-hit detection, and that each source triggers a feed
fetch. No network."""

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sports_feed import mlb_recorder as m
from weather_recorder import JsonlWriter


def linescore(away=0, home=0, outs=0, balls=0, strikes=0, bases=(), batter=1):
    offense = {"batter": {"id": batter}, "onDeck": {"id": 2}}
    for base in bases:
        offense[base] = {"id": 9}
    return {"currentInning": 1, "inningState": "Top", "isTopInning": True,
            "teams": {"away": {"runs": away, "hits": 0, "errors": 0}, "home": {"runs": home, "hits": 0, "errors": 0}},
            "offense": offense, "outs": outs, "balls": balls, "strikes": strikes}


def feed(plays, state="Live", detailed="In Progress", timecode="20260930_000100"):
    return {"metaData": {"timeStamp": timecode, "gameEvents": [], "logicalEvents": [], "wait": 10},
            "gameData": {"status": {"abstractGameState": state, "detailedState": detailed, "statusCode": "I"}},
            "liveData": {"plays": {"allPlays": plays}}}


def play(ab, events, complete=False, scoring=False, away=0, home=0, event=None, end="2026-09-30T00:01:00.000Z"):
    return {"about": {"atBatIndex": ab, "inning": 1, "halfInning": "top", "isComplete": complete,
                      "isScoringPlay": scoring, "startTime": "2026-09-30T00:00:00.000Z", "endTime": end},
            "result": {"event": event, "eventType": None, "description": "", "rbi": 0,
                       "awayScore": away, "homeScore": home},
            "playEvents": events}


def pitch(i, desc="Ball", end="2026-09-30T00:00:30.000Z"):
    return {"index": i, "type": "pitch", "details": {"description": desc, "code": "B", "isInPlay": False},
            "count": {"balls": 1, "strikes": 0, "outs": 0}, "startTime": "2026-09-30T00:00:25.000Z", "endTime": end}


class ParsingTests(unittest.TestCase):
    def test_linescore_state_reads_runners_from_offense_keys(self):
        s = m.linescore_state(linescore(away=2, bases=("first", "third"), batter=7))
        self.assertEqual((s["away_runs"], s["home_runs"]), (2, 0))
        self.assertEqual((s["on_1b"], s["on_2b"], s["on_3b"]), (True, False, True))
        self.assertEqual(s["batter_id"], 7)

    def test_runs_changed(self):
        a, b = m.linescore_state(linescore()), m.linescore_state(linescore(home=1))
        self.assertTrue(m.runs_changed(a, b))
        self.assertFalse(m.runs_changed(a, m.linescore_state(linescore(outs=1))))
        self.assertFalse(m.runs_changed(None, b))

    def test_play_rows_emit_only_new_or_changed(self):
        seen = {}
        rows = m.play_rows(feed([play(0, [pitch(0)])]), seen)
        self.assertEqual([r["kind"] for r in rows], ["play_event", "at_bat"])
        self.assertTrue(all(r["first_seen"] for r in rows))
        self.assertEqual(m.play_rows(feed([play(0, [pitch(0)])]), seen), [])
        # A new pitch and the at-bat completing as a scoring play.
        rows = m.play_rows(feed([play(0, [pitch(0), pitch(1, "In play, run(s)")], complete=True,
                                      scoring=True, away=1, event="Single")]), seen)
        self.assertEqual([(r["kind"], r["first_seen"]) for r in rows], [("play_event", True), ("at_bat", False)])
        self.assertEqual((rows[1]["away_score"], rows[1]["is_scoring_play"]), (1, True))

    def test_pick_game_by_team_prefers_not_final(self):
        def game(pk, away, home, state):
            return {"gamePk": pk, "status": {"abstractGameState": state},
                    "teams": {"away": {"team": {"abbreviation": away}}, "home": {"team": {"abbreviation": home}}}}
        sched = {"dates": [{"games": [game(1, "BOS", "NYY", "Final"), game(2, "BOS", "NYY", "Live"),
                                      game(3, "CHC", "SD", "Preview")]}]}
        self.assertEqual(m.pick_game(sched, "nyy")["gamePk"], 2)
        self.assertEqual(m.pick_game(sched, "SD")["gamePk"], 3)
        with self.assertRaises(SystemExit):
            m.pick_game(sched, "LAD")

    def test_cache_hit_detection_and_param(self):
        self.assertTrue(m.is_cache_hit(httpx.Headers({"x-cache": "HIT, MISS", "age": "0"})))
        self.assertTrue(m.is_cache_hit(httpx.Headers({"x-cache": "MISS, MISS", "age": "4"})))
        self.assertFalse(m.is_cache_hit(httpx.Headers({"x-cache": "MISS, MISS", "age": "0"})))
        self.assertFalse(m.is_cache_hit(httpx.Headers({})))
        url = m.cache_busted("https://x.test/a?b=1")
        self.assertIn("&rq=", url)
        self.assertNotIn("cb=", url)

    def test_iso_epoch(self):
        self.assertAlmostEqual(m.iso_epoch("2026-09-30T00:00:01.500Z") % 60, 1.5)
        self.assertIsNone(m.iso_epoch(None))
        self.assertIsNone(m.iso_epoch("garbage"))


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.writer = JsonlWriter(Path(self.tmp.name), clock=lambda: 1000.0)

    def tearDown(self):
        self.writer.close()
        self.tmp.cleanup()

    def recorder(self):
        return m.MlbRecorder(None, self.writer, 1, clock=lambda: 1000.0)

    def test_linescore_written_on_change_only_and_triggers_feed(self):
        async def go():
            r = self.recorder()
            r.handle_linescore(linescore(), 1.0, 1.1)
            self.assertTrue(r.feed_wanted.is_set())
            r.feed_wanted.clear()
            r.handle_linescore(linescore(), 2.0, 2.1)
            self.assertFalse(r.feed_wanted.is_set())
            r.handle_linescore(linescore(home=1), 3.0, 3.1)
            self.assertTrue(r.feed_wanted.is_set())
        asyncio.run(go())
        self.assertEqual(self.writer.counts["linescore"], 2)

    def test_first_feed_is_backfill_and_final_sets_stop_time(self):
        async def go():
            r = self.recorder()
            r.handle_feed(feed([play(0, [pitch(0)])]), 1.0, 1.2)
            r.handle_feed(feed([play(0, [pitch(0), pitch(1)])], state="Final", detailed="Final",
                               timecode="20260930_030000"), 2.0, 2.2)
            return r
        r = asyncio.run(go())
        self.assertEqual(r.final_at, 2.2)
        self.writer.close()
        rows = [__import__("json").loads(line) for f in Path(self.tmp.name).glob("play_event_*.jsonl")
                for line in f.read_text().splitlines()]
        self.assertEqual([row["backfill"] for row in rows], [True, False])
        self.assertEqual(self.writer.counts["feed_update"], 2)
        self.assertEqual(self.writer.counts["status"], 2)

    def test_push_logged_and_triggers_feed(self):
        async def go():
            r = self.recorder()
            r.handle_push('{"gameEvents": ["strikeout"]}', 5.0)
            r.handle_push(b"not json", 6.0)
            self.assertTrue(r.feed_wanted.is_set())
            return r
        self.assertEqual(asyncio.run(go()).pushes, 2)
        self.assertEqual(self.writer.counts["push"], 2)


if __name__ == "__main__":
    unittest.main()
