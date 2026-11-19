"""jsonl_logs (reading plain and gzipped daily logs) and scripts/compress_logs.py
(which day files get compressed, byte-exact round trip, recovery from a half-done run)."""

import gzip
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import compress_logs as cl
from jsonl_logs import daily_files, file_day, open_log

NOW = 1793059200.0          # 2026-10-27 00:00 UTC
OLD = NOW - 3 * 3600        # last written well over an hour ago


def write(d: Path, name: str, text: str = '{"a": 1}\n', mtime: float = OLD) -> Path:
    p = d / name
    data = text.encode("utf-8")      # bytes, so Windows doesn't turn \n into \r\n
    p.write_bytes(gzip.compress(data) if name.endswith(".gz") else data)
    os.utime(p, (mtime, mtime))
    return p


class DailyFilesTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_mixes_plain_and_gzipped_days_in_date_order(self):
        write(self.dir, "book_2026-10-24.jsonl.gz")
        write(self.dir, "book_2026-10-26.jsonl")
        write(self.dir, "book_2026-10-25.jsonl.gz")
        self.assertEqual([os.path.basename(p) for p in daily_files(self.dir, "book")],
                         ["book_2026-10-24.jsonl.gz", "book_2026-10-25.jsonl.gz", "book_2026-10-26.jsonl"])

    def test_kind_does_not_match_longer_kinds(self):
        write(self.dir, "index_2026-10-24.jsonl")
        write(self.dir, "index_fast_2026-10-24.jsonl.gz")
        write(self.dir, "index2_2026-10-24.jsonl")
        self.assertEqual([os.path.basename(p) for p in daily_files(self.dir, "index")], ["index_2026-10-24.jsonl"])
        self.assertEqual([os.path.basename(p) for p in daily_files(self.dir, "index_fast")], ["index_fast_2026-10-24.jsonl.gz"])

    def test_plain_file_wins_when_a_day_has_both(self):
        write(self.dir, "trade_2026-10-24.jsonl")
        write(self.dir, "trade_2026-10-24.jsonl.gz")
        self.assertEqual([os.path.basename(p) for p in daily_files(self.dir, "trade")], ["trade_2026-10-24.jsonl"])

    def test_file_day_and_open_log_read_both_forms(self):
        a = write(self.dir, "index2_fast_2026-10-24.jsonl", '{"x": 1}\n{"x": 2}\n')
        b = write(self.dir, "index2_fast_2026-10-25.jsonl.gz", '{"x": 3}\n')
        self.assertEqual([file_day(str(a)), file_day(str(b))], ["2026-10-24", "2026-10-25"])
        lines = [line for p in (a, b) for line in open_log(str(p))]
        self.assertEqual(lines, ['{"x": 1}\n', '{"x": 2}\n', '{"x": 3}\n'])


class CompressTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def names(self, paths):
        return [p.name for p in paths]

    def test_candidates_skip_recent_days_busy_files_and_other_files(self):
        write(self.dir, "book_2026-10-24.jsonl")
        write(self.dir, "book_2026-10-25.jsonl")
        write(self.dir, "book_2026-10-26.jsonl")                       # yesterday: kept with keep_days=2
        write(self.dir, "book_2026-10-27.jsonl", mtime=NOW)            # today
        write(self.dir, "trade_2026-10-25.jsonl", mtime=NOW - 600)     # old day but written 10 min ago
        write(self.dir, "market_2026-10-23.jsonl.gz")                  # already done
        write(self.dir, "recorder.log")
        write(self.dir, "early_entry.csv")
        self.assertEqual(self.names(cl.candidates(self.dir, 2, NOW)), ["book_2026-10-24.jsonl", "book_2026-10-25.jsonl"])
        self.assertEqual(self.names(cl.candidates(self.dir, 1, NOW)),
                         ["book_2026-10-24.jsonl", "book_2026-10-25.jsonl", "book_2026-10-26.jsonl"])

    def test_compress_round_trips_exactly_and_removes_the_original(self):
        text = "".join(f'{{"recv_time": {i}, "bids": [["0.5{i % 10}", "12"]]}}\n' for i in range(5000))
        src = write(self.dir, "book_2026-10-24.jsonl", text)
        freed = cl.compress(src)
        gz = self.dir / "book_2026-10-24.jsonl.gz"
        self.assertFalse(src.exists())
        self.assertFalse((self.dir / "book_2026-10-24.jsonl.gz.tmp").exists())
        self.assertGreater(freed, 0)
        with gzip.open(gz, "rt", encoding="utf-8", newline="") as f:
            self.assertEqual(f.read(), text)

    def test_finishes_a_run_that_could_not_delete_the_original(self):
        src = write(self.dir, "ktop_2026-10-24.jsonl", '{"tk": "A"}\n' * 100)
        with gzip.open(self.dir / "ktop_2026-10-24.jsonl.gz", "wb") as f:
            f.write(src.read_bytes())
        cl.compress(src)
        self.assertFalse(src.exists())
        self.assertEqual(gzip.decompress((self.dir / "ktop_2026-10-24.jsonl.gz").read_bytes()), b'{"tk": "A"}\n' * 100)

    def test_redoes_a_mismatched_leftover_archive(self):
        src = write(self.dir, "ktop_2026-10-24.jsonl", '{"tk": "A"}\n' * 100)
        with gzip.open(self.dir / "ktop_2026-10-24.jsonl.gz", "wb") as f:
            f.write(b"truncated")
        cl.compress(src)
        self.assertFalse(src.exists())
        self.assertEqual(gzip.decompress((self.dir / "ktop_2026-10-24.jsonl.gz").read_bytes()), b'{"tk": "A"}\n' * 100)

    def test_main_dry_run_changes_nothing(self):
        src = write(self.dir, "book_2026-10-24.jsonl")
        old_dirs = cl.DIRS
        cl.DIRS = [self.dir]
        try:
            self.assertEqual(cl.main(["--dry-run"]), 0)
        finally:
            cl.DIRS = old_dirs
        self.assertTrue(src.exists())
        self.assertFalse((self.dir / "book_2026-10-24.jsonl.gz").exists())

    def test_main_compresses_and_clears_stale_tmp(self):
        write(self.dir, "book_2026-01-01.jsonl")
        (self.dir / "book_2026-01-02.jsonl.gz.tmp").write_bytes(b"partial")
        old_dirs = cl.DIRS
        cl.DIRS = [self.dir]
        try:
            self.assertEqual(cl.main([]), 0)
        finally:
            cl.DIRS = old_dirs
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()), ["book_2026-01-01.jsonl.gz"])


if __name__ == "__main__":
    unittest.main()
