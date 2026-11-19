"""Gzips finished days of the recorder logs in place, to keep B: from filling up.

The BTC, Kalshi and weather recorders write ~1.5 GB a day of <kind>_<UTC date>.jsonl into
logs/crypto and logs/weather; the cross-venue sports recorder (xsports) ~1.6 GB a day into
logs/xsports. A full disk killed all three silently on 2026-10-25 (see
logs/recording_gaps.md). The files gzip ~15x. Readers go through jsonl_logs, which opens
either form, so nothing downstream changes.

A day is compressed only once it is at least --keep-days old (UTC) and its file has not been
written for an hour, so the recorders never have a file pulled out from under them. Each file
is written to <name>.jsonl.gz.tmp, decompressed again and checked byte for byte (size and
SHA-256) against the original, renamed to .jsonl.gz, and only then is the original deleted. If
the original can't be deleted (Windows: a reader has it open), it stays and the next run
finishes the job. Files are done one at a time, streamed, so memory stays flat.

Usage:
  py scripts\\compress_logs.py --dry-run     # list what would be compressed
  py scripts\\compress_logs.py               # compress everything finished
  py scripts\\compress_logs.py --log logs\\compress_logs.log
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIRS = [ROOT / "logs" / "crypto", ROOT / "logs" / "weather", ROOT / "logs" / "xsports", ROOT / "logs" / "bdl", ROOT / "logs" / "xvenue"]
DAILY = re.compile(r"^[a-z0-9_]+_(\d{4}-\d{2}-\d{2})\.jsonl$")
CHUNK = 16 * 1024 * 1024
QUIET_SECONDS = 3600
LEVEL = 6

log = logging.getLogger("compress_logs")


def candidates(directory: Path, keep_days: int, now: float) -> list[Path]:
    """Daily files in directory old enough to compress, oldest first."""
    cutoff = (datetime.fromtimestamp(now, timezone.utc).date() - timedelta(days=keep_days - 1)).isoformat()
    out = []
    for p in sorted(directory.iterdir()):
        m = DAILY.match(p.name)
        if m and p.is_file() and m.group(1) < cutoff and now - p.stat().st_mtime >= QUIET_SECONDS:
            out.append(p)
    return out


def _digest(f) -> tuple[int, str]:
    h, n = hashlib.sha256(), 0
    while chunk := f.read(CHUNK):
        h.update(chunk)
        n += len(chunk)
    return n, h.hexdigest()


def _matches(src: Path, gz: Path) -> bool:
    with open(src, "rb") as a, gzip.open(gz, "rb") as b:
        return _digest(a) == _digest(b)


def _remove_original(src: Path) -> bool:
    try:
        src.unlink()
        return True
    except PermissionError as e:
        log.warning("%s: compressed and verified, but the original is in use (%s); the next run deletes it", src.name, e)
        return False


def compress(src: Path) -> int:
    """Compresses one file, verifies it, deletes the original. Returns bytes freed (0 if skipped)."""
    gz = src.with_name(src.name + ".gz")
    if gz.exists():                      # left by a run that couldn't delete the original
        if _matches(src, gz):
            saved = src.stat().st_size - gz.stat().st_size
            return saved if _remove_original(src) else 0
        log.warning("%s: existing %s does not match the original; redoing it", src.name, gz.name)
        gz.unlink()
    before = src.stat()
    tmp = src.with_name(src.name + ".gz.tmp")
    h, n = hashlib.sha256(), 0
    with open(src, "rb") as fin, gzip.open(tmp, "wb", compresslevel=LEVEL) as fout:
        while chunk := fin.read(CHUNK):
            h.update(chunk)
            n += len(chunk)
            fout.write(chunk)
    after = src.stat()
    with gzip.open(tmp, "rb") as f:
        check = _digest(f)
    if (after.st_size, after.st_mtime) != (before.st_size, before.st_mtime) or n != after.st_size \
            or check != (n, h.hexdigest()):
        tmp.unlink()
        log.error("%s: changed during compression or failed verification; left as is", src.name)
        return 0
    os.replace(tmp, gz)
    saved = n - gz.stat().st_size
    if not _remove_original(src):
        return 0
    log.info("%s: %.1f MB -> %.1f MB", src.name, n / 1e6, (n - saved) / 1e6)
    return saved


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--keep-days", type=int, default=2, help="leave the newest N UTC days plain (default 2: today and yesterday)")
    ap.add_argument("--dry-run", action="store_true", help="list what would be compressed, change nothing")
    ap.add_argument("--log", help="append the log to this file as well as the console")
    args = ap.parse_args(argv)
    if args.keep_days < 1:
        ap.error("--keep-days must be at least 1 (today's files are still being written)")
    handlers = [logging.StreamHandler(sys.stdout)] + ([logging.FileHandler(args.log, encoding="utf-8")] if args.log else [])
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)

    now = time.time()
    dirs = [d for d in DIRS if d.is_dir()]   # a retired recorder's folder may be deleted
    for d in dirs:
        for tmp in d.glob("*.jsonl.gz.tmp"):     # from a run that was killed mid-file
            tmp.unlink()
    todo = [p for d in dirs for p in candidates(d, args.keep_days, now)]
    total = sum(p.stat().st_size for p in todo)
    log.info("%d files, %.1f GB to compress%s", len(todo), total / 1e9, " (dry run)" if args.dry_run else "")
    if args.dry_run:
        for p in todo:
            log.info("  %s/%s  %.1f MB", p.parent.name, p.name, p.stat().st_size / 1e6)
        return 0
    freed = 0
    for p in todo:
        freed += compress(p)
    log.info("done: freed %.1f GB", freed / 1e9)
    return 0


if __name__ == "__main__":
    sys.exit(main())
