"""Shows a Windows desktop notification, for one-off reminders from a scheduled task.

Usage: pyw scripts\\notify.py "Title" "Body"
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import disk_guard  # noqa: E402

if __name__ == "__main__":
    logging.basicConfig(filename=disk_guard.LOG, encoding="utf-8", level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    disk_guard.alert(sys.argv[1], sys.argv[2])
