"""Daily recorder logs: <kind>_<UTC date>.jsonl, and the gzipped archives of past days.

scripts/compress_logs.py gzips finished days in logs/crypto and logs/weather in place
(<kind>_<date>.jsonl -> <kind>_<date>.jsonl.gz), so code that reads those folders lists
a kind's days with daily_files() and opens each one with open_log().
"""
from __future__ import annotations

import glob
import gzip
import os
from pathlib import Path


def daily_files(directory: Path | str, kind: str) -> list[str]:
    """Paths of <kind>_<date>.jsonl or .jsonl.gz in directory, one per day, in date order.
    The [0-9] keeps kind "index" from matching index_fast_*. If a day has both files (only
    for a moment, while compress_logs.py is replacing it) the plain one is returned."""
    by_day = {p[:-3]: p for p in glob.glob(os.path.join(str(directory), f"{kind}_[0-9]*.jsonl.gz"))}
    by_day.update((p, p) for p in glob.glob(os.path.join(str(directory), f"{kind}_[0-9]*.jsonl")))
    return [by_day[k] for k in sorted(by_day)]


def file_day(path: str) -> str:
    """The YYYY-MM-DD in a daily file's name."""
    name = os.path.basename(path)
    return name[name.rindex("_") + 1:][:10]


def open_log(path: str):
    """Text-mode handle on a daily file, gzipped or not."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")
