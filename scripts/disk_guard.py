"""Stops the recorders before B: fills up, and raises a desktop alert.

A full disk killed every recorder silently on 2026-10-25 (logs/recording_gaps.md). Meant to run
from a scheduled task every few minutes:

  free < --warn-gb (25)   desktop alert, at most once per ALERT_REPEAT_SECONDS
  free < --stop-gb (15)   Ctrl+C to each recorder, a forced stop for any still up after
                          STOP_GRACE_SECONDS, a desktop alert, and an entry in recording_gaps.md

Only processes running one of RECORDERS are touched, never a bot. Ctrl+C reaches a whole console,
so it is sent only if every process on that console is a recorder; otherwise that recorder gets
the forced stop alone. A forced stop is safe for the data because JsonlWriter flushes every line:
at worst the last row is cut short. Nothing restarts the recorders: free space, then restart by
hand (recipes in logs/recording_gaps.md).

Usage:
  pyw scripts\\disk_guard.py                     # one check, as the scheduled task runs it
  py scripts\\disk_guard.py --dry-run            # say what it would do, change nothing
  py scripts\\disk_guard.py --test-alert         # show a desktop alert and exit
"""
from __future__ import annotations

import argparse
import ctypes
import json
import logging
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = ROOT / "logs" / "disk_guard.log"
STATE = ROOT / "logs" / "disk_guard_state.json"
GAPS = ROOT / "logs" / "recording_gaps.md"
RECORDERS = ("crypto_updown.recorder", "crypto_updown.kalshi_recorder", "weather_recorder.py", "xsports.recorder",
             "sports_feed.mlb_recorder", "sports_feed.bdl_recorder", "weather_taker.accuweather_recorder", "xvenue.recorder")
STOP_GRACE_SECONDS = 60.0
ALERT_REPEAT_SECONDS = 3600.0
NO_WINDOW = 0x08000000

log = logging.getLogger("disk_guard")


# B: is a thin Storage Space: a 500 GiB volume on a 465 GiB pool. The volume's own free space
# overstates what can really be written by about 37 GB, so the pool's free space is what counts.
# Found 2026-11-08 02:00 ET: the volume showed 36 GB free while the pool had 0.25 GB, every
# recorder's writes failed, and this guard never fired (it read the volume). Almost certainly the
# 2026-10-25 outage too. Get-StoragePool needs no admin rights.
POOL_QUERY = ("Get-StoragePool -IsPrimordial $false | "
              "ForEach-Object { [math]::Round(($_.Size - $_.AllocatedSize) / 1e9, 3) }")


def pool_free_gb() -> float | None:
    """Real free space left in the storage pool(s) under B:, or None if it can't be read."""
    try:
        out = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", POOL_QUERY],
                             capture_output=True, text=True, timeout=60, creationflags=NO_WINDOW)
        values = [float(x) for x in out.stdout.split()]
    except (OSError, subprocess.SubprocessError, ValueError):
        log.exception("could not read the storage pool's free space")
        return None
    return min(values) if values else None


def free_gb(path: Path = ROOT) -> float:
    """The smaller of the volume's and the pool's free space (see POOL_QUERY)."""
    volume = shutil.disk_usage(path).free / 1e9
    pool = pool_free_gb()
    return volume if pool is None else min(volume, pool)


def is_recorder(cmdline: str | None, names: tuple[str, ...] = RECORDERS) -> bool:
    """True if the command line is Python running one of names as its module or script, not
    merely mentioning it in an argument."""
    parts = [p.strip('"') for p in shlex.split((cmdline or "").replace("\\", "/"), posix=False)]
    if not parts or not parts[0].rsplit("/", 1)[-1].lower().startswith("py"):
        return False
    args = iter(parts[1:])
    for p in args:
        if p == "-m":
            return next(args, None) in names
        if not p.startswith("-"):
            return p.rsplit("/", 1)[-1] in names
    return False


def recorder_pids(names: tuple[str, ...] = RECORDERS) -> dict[int, str]:
    out = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command",
                          "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"],
                         capture_output=True, text=True, timeout=120, creationflags=NO_WINDOW, check=True).stdout
    return {r["ProcessId"]: r["CommandLine"] for r in json.loads(out) if is_recorder(r.get("CommandLine"), names)}


def _ctrl_c_child(pid: int, allowed: set[int]) -> int:
    """Runs in its own short-lived process, since it has to give up its console. Exit 0 sent,
    2 no console to attach to, 3 the console holds something other than recorders."""
    k = ctypes.windll.kernel32
    k.FreeConsole()
    if not k.AttachConsole(pid):
        return 2
    buf = (ctypes.c_uint32 * 256)()
    n = k.GetConsoleProcessList(buf, 256)
    me = k.GetCurrentProcessId()
    if not 0 < n <= 256 or {buf[i] for i in range(n)} - allowed - {me}:
        return 3
    k.SetConsoleCtrlHandler(None, True)     # keep the event from ending this process first
    k.GenerateConsoleCtrlEvent(0, 0)
    time.sleep(1.0)
    k.FreeConsole()
    return 0


def send_ctrl_c(pid: int, allowed: set[int]) -> int:
    return subprocess.run([sys.executable, __file__, "--ctrl-c", str(pid), "--allowed", ",".join(map(str, allowed))],
                          creationflags=NO_WINDOW, timeout=30).returncode


def force_stop(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, creationflags=NO_WINDOW, timeout=30)


def stop_recorders(names: tuple[str, ...] = RECORDERS, grace: float = STOP_GRACE_SECONDS) -> dict[str, list[int]]:
    """Asks every recorder to stop, then forces any still running. Returns which pids went which way."""
    pids = recorder_pids(names)
    allowed = set(pids)
    # py.exe and its python.exe child share a console, and one Ctrl+C reaches both
    signalled: set[int] = set()
    for pid in pids:
        if pid not in signalled:
            code = send_ctrl_c(pid, allowed)
            log.info("Ctrl+C to %d (%s): exit %d", pid, pids[pid][:80], code)
            if code == 0:
                signalled.add(pid)
    deadline = time.time() + grace
    left = recorder_pids(names)
    while left and time.time() < deadline:
        time.sleep(2.0)
        left = recorder_pids(names)
    for pid in left:
        log.warning("forcing %d (%s)", pid, left[pid][:80])
        force_stop(pid)
    return {"clean": sorted(allowed - set(left)), "forced": sorted(left)}


def alert(title: str, body: str) -> None:
    """A Windows desktop notification, plus the log line, so it survives a missed toast."""
    log.warning("ALERT %s: %s", title, body)
    esc = lambda s: s.replace("&", "&amp;").replace("<", "&lt;").replace("'", "''")
    script = ("[Windows.UI.Notifications.ToastNotificationManager,Windows.UI.Notifications,ContentType=WindowsRuntime]|Out-Null;"
              "[Windows.Data.Xml.Dom.XmlDocument,Windows.Data.Xml.Dom,ContentType=WindowsRuntime]|Out-Null;"
              "$x=New-Object Windows.Data.Xml.Dom.XmlDocument;"
              f"$x.LoadXml('<toast scenario=\"reminder\"><visual><binding template=\"ToastGeneric\"><text>{esc(title)}</text><text>{esc(body)}</text></binding></visual>"
              "<actions><action content=\"OK\" arguments=\"ok\" activationType=\"system\"/></actions></toast>');"
              "$t=[Windows.UI.Notifications.ToastNotification]::new($x);"
              "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("
              "'{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\\WindowsPowerShell\\v1.0\\powershell.exe').Show($t)")
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True,
                           text=True, timeout=60, creationflags=NO_WINDOW)
        if r.returncode:
            log.error("desktop alert failed: %s", r.stderr[:300])
    except Exception as exc:
        log.error("desktop alert failed: %s", exc)


def load_state(path: Path = STATE) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def decide(free: float, stop_gb: float, warn_gb: float, last_alert: float, now: float) -> tuple[bool, bool]:
    """(stop the recorders, send an alert) for this reading. A stop always alerts."""
    if free < stop_gb:
        return True, True
    return False, free < warn_gb and now - last_alert >= ALERT_REPEAT_SECONDS


def check(stop_gb: float, warn_gb: float, dry_run: bool = False, names: tuple[str, ...] = RECORDERS,
          state_path: Path = STATE, gaps_path: Path = GAPS, grace: float = STOP_GRACE_SECONDS) -> int:
    free, now = free_gb(), time.time()
    state = load_state(state_path)
    stop, send = decide(free, stop_gb, warn_gb, state.get("last_alert", 0.0), now)
    log.info("B: %.1f GB free (warn %.0f, stop %.0f)%s", free, warn_gb, stop_gb, " -> STOP" if stop else "")
    if dry_run:
        log.info("dry run: stop=%s alert=%s recorders=%s", stop, send, recorder_pids(names))
        return 0
    if stop:
        running = recorder_pids(names)
        if running:
            result = stop_recorders(names, grace)
            when = time.strftime("%Y-%m-%d %H:%M:%S %Z")
            with open(gaps_path, "a", encoding="utf-8") as f:
                f.write(f"\n## {when} -- disk guard stopped the recorders\n\n"
                        f"- {free:.1f} GB free on B:, below the {stop_gb:.0f} GB line. Clean: {result['clean']}. Forced: {result['forced']}.\n"
                        f"- Recording is off from here until someone frees space and restarts them.\n")
            alert("RECORDERS STOPPED: disk nearly full",
                  f"B: has {free:.1f} GB free. {len(running)} recorder processes stopped. Free space, then restart them.")
            send = False
            state["last_alert"] = now
        elif send and now - state.get("last_alert", 0.0) < ALERT_REPEAT_SECONDS:
            send = False                 # already stopped and already said so
    if send:
        alert("Disk low on B:", f"{free:.1f} GB free. Recorders stop at {stop_gb:.0f} GB.")
        state["last_alert"] = now
    state.update(last_check=now, last_free_gb=round(free, 2))
    state_path.write_text(json.dumps(state), encoding="utf-8")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--stop-gb", type=float, default=15.0)
    ap.add_argument("--warn-gb", type=float, default=25.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--test-alert", action="store_true")
    ap.add_argument("--no-alert", action="store_true", help="log alerts without showing them (for testing)")
    ap.add_argument("--match", help="comma-separated recorder names to use instead of the built-in list (for testing)")
    ap.add_argument("--grace", type=float, default=STOP_GRACE_SECONDS)
    ap.add_argument("--state", default=str(STATE))
    ap.add_argument("--gaps", default=str(GAPS))
    ap.add_argument("--ctrl-c", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--allowed", default="", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.ctrl_c is not None:
        return _ctrl_c_child(args.ctrl_c, {int(x) for x in args.allowed.split(",") if x})
    handlers: list[logging.Handler] = [logging.FileHandler(LOG, encoding="utf-8")]
    if sys.stdout is not None:           # pyw has none
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)
    if args.no_alert:
        globals()["alert"] = lambda title, body: log.warning("ALERT (not shown) %s: %s", title, body)
    if args.test_alert:
        alert("Disk guard test", "This is a test of the recorder disk alert. Nothing was stopped.")
        return 0
    names = tuple(x.strip() for x in args.match.split(",")) if args.match else RECORDERS
    try:
        return check(args.stop_gb, args.warn_gb, args.dry_run, names, Path(args.state), Path(args.gaps), args.grace)
    except Exception:
        log.exception("disk guard check failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
