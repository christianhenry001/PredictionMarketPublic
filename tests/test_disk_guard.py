"""Disk guard: which processes it may touch, when it acts, and a real stop of a throwaway process."""
import subprocess
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import disk_guard as dg  # noqa: E402


class IsRecorderTests(unittest.TestCase):
    def test_recorder_command_lines_match(self):
        for c in ('"C:\\Windows\\py.exe" -m crypto_updown.recorder', "python.exe -m xsports.recorder --minutes 5",
                  '"C:\\Windows\\py.exe" weather_recorder.py', "python.exe B:\\PredictionMarkets\\weather_recorder.py"):
            self.assertTrue(dg.is_recorder(c), c)

    def test_bots_and_mentions_do_not_match(self):
        for c in ("py single_venue_maker_bot.py --live", "py -m crypto_updown.r6_live", "py analyze_xsports.recorder_stats.py",
                  'py -c "import xsports.recorder_x"', "notepad logs\\weather_recorder.py.bak", "notepad weather_recorder.py",
                  "pythonw.exe scripts/disk_guard.py --match xsports.recorder", "py tool.py xsports.recorder", "", None):
            self.assertFalse(dg.is_recorder(c), c)


class DecideTests(unittest.TestCase):
    def test_lines(self):
        self.assertEqual(dg.decide(14.9, 15, 25, 0, 1e6), (True, True))
        self.assertEqual(dg.decide(15.0, 15, 25, 0, 1e6), (False, True))
        self.assertEqual(dg.decide(25.0, 15, 25, 0, 1e6), (False, False))

    def test_warning_repeats_at_most_hourly(self):
        self.assertEqual(dg.decide(20, 15, 25, 1e6 - 60, 1e6), (False, False))
        self.assertEqual(dg.decide(20, 15, 25, 1e6 - 3600, 1e6), (False, True))
        self.assertEqual(dg.decide(10, 15, 25, 1e6 - 60, 1e6), (True, True))


# the flag clear is for test runners started with Ctrl+C ignored, which a child would inherit
DUMMY = """
import ctypes, sys, time
ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False)
try:
    while True:
        time.sleep(0.2)
except KeyboardInterrupt:
    open(sys.argv[2], "w").write("clean")
"""


class RealStopTests(unittest.TestCase):
    """A throwaway process on its own console, stopped the way a recorder would be."""

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.script = self.tmp / "dummy_recorder_for_guard_test.py"
        self.script.write_text(DUMMY, encoding="utf-8")
        self.flag = self.tmp / "clean.txt"

    def test_ctrl_c_reaches_it_and_it_exits_cleanly(self):
        p = subprocess.Popen([sys.executable, str(self.script), "x", str(self.flag)], creationflags=subprocess.CREATE_NEW_CONSOLE)
        time.sleep(1.5)
        try:
            result = dg.stop_recorders(("dummy_recorder_for_guard_test.py",), grace=15.0)
            p.wait(timeout=5)
            self.assertEqual(result["clean"], [p.pid])
            self.assertEqual(result["forced"], [])
            self.assertEqual(self.flag.read_text(), "clean")
        finally:
            if p.poll() is None:
                p.kill()

    def test_shared_console_with_a_non_recorder_is_not_signalled(self):
        # a second process on the console that isn't on the list: Ctrl+C must not be sent
        p = subprocess.Popen([sys.executable, str(self.script), "x", str(self.flag)], creationflags=subprocess.CREATE_NEW_CONSOLE)
        time.sleep(1.5)
        try:
            code = dg.send_ctrl_c(p.pid, set())
            self.assertEqual(code, 3)
            time.sleep(1.0)
            self.assertIsNone(p.poll())
        finally:
            p.kill()


class FreeSpaceTests(unittest.TestCase):
    """B: is a thin Storage Space: the pool can be full while the volume shows tens of GB free
    (2026-11-08). The guard must go by the smaller figure."""

    def test_a_full_pool_wins_over_a_roomy_volume(self):
        real = dg.pool_free_gb
        try:
            dg.pool_free_gb = lambda: 0.25
            self.assertEqual(dg.free_gb(), 0.25)
        finally:
            dg.pool_free_gb = real

    def test_an_unreadable_pool_falls_back_to_the_volume(self):
        real = dg.pool_free_gb
        try:
            dg.pool_free_gb = lambda: None
            self.assertGreater(dg.free_gb(), 0)
        finally:
            dg.pool_free_gb = real


if __name__ == "__main__":
    unittest.main()
