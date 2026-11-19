"""
Order audit log (order_audit.py, audit item #35, added 2026-09-28):
  - every record has both timestamps, the order fields, the decision-time
    fair value and book top, and a ReasonCode
  - the file is append-only and rotates by UTC date
  - EVERY cancel carries a reason code: the cancel primitives refuse a
    missing or non-enum reason before anything is sent, and a source scan
    checks nothing else in the quoter calls the order client's cancel
"""

import ast
import json
import sys
import tempfile
import unittest
import unittest.mock
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from order_audit import CANCEL_REASONS, RISK_CANCEL_REASONS, OrderAuditLog, ReasonCode, require_reason

D = Decimal
BOOK = dict(bids={D("0.49"): D("10")}, asks={D("0.51"): D("10")})


class TestRecordFormat(unittest.TestCase):
    def test_a_record_has_every_field_and_both_clocks(self):
        audit = OrderAuditLog(None, run="r1", mode="paper", clock_ns=lambda: 1_790_000_000_000_000_000, mono_ns=lambda: 42)
        row = audit.record("cancel_request", ReasonCode.FV_OUT_OF_BAND, market="m", pair="p", order_id="o1",
                           side="no", price=D("0.47"), size=3, rung=1, fair_value=D("0.5012"),
                           book={"bid": [D("0.49"), D("10")], "ask": [D("0.51"), D("7")]},
                           old_target=D("0.47"), new_target=D("0.46"))
        self.assertEqual(row["ts_ns"], 1_790_000_000_000_000_000)
        self.assertEqual(row["mono_ns"], 42)
        self.assertEqual(row["reason"], "FV_OUT_OF_BAND")
        self.assertEqual(row["price"], "0.47")
        self.assertEqual(row["book"], {"bid": ["0.49", "10"], "ask": ["0.51", "7"]})
        self.assertIsNone(row["client_id"])  # the venue has no client order id
        for key in ("run", "mode", "action", "market", "pair", "order_id", "side", "size", "rung", "fair_value",
                    "old_target", "new_target", "latency_ms", "detail"):
            self.assertIn(key, row)

    def test_a_bare_string_or_missing_reason_is_refused(self):
        audit = OrderAuditLog(None)
        with self.assertRaises(TypeError):
            audit.record("cancel_request", "FV_OUT_OF_BAND")
        with self.assertRaises(TypeError):
            audit.record("cancel_request", None)

    def test_unknown_action_or_field_is_refused(self):
        audit = OrderAuditLog(None)
        with self.assertRaises(ValueError):
            audit.record("teleport", ReasonCode.SHUTDOWN)
        with self.assertRaises(ValueError):
            audit.record("cancel_request", ReasonCode.SHUTDOWN, colour="red")


class TestAppendOnlyAndRotation(unittest.TestCase):
    def test_appends_across_instances_and_rotates_by_utc_date(self):
        day1 = 1_790_035_200 * 10**9  # 2026-09-22 00:00:00 UTC
        with tempfile.TemporaryDirectory() as d:
            clock = {"ns": day1 + 10**9}
            a = OrderAuditLog(d, run="a", clock_ns=lambda: clock["ns"])
            a.record("create", ReasonCode.QUOTE_NEW, order_id="1")
            a.close()
            b = OrderAuditLog(d, run="b", clock_ns=lambda: clock["ns"])
            b.record("create", ReasonCode.QUOTE_NEW, order_id="2")  # same day: appended, not rewritten
            clock["ns"] = day1 + 86_400 * 10**9 + 5
            b.record("cancel_request", ReasonCode.SHUTDOWN, order_id="2")  # next UTC day: new file
            b.close()
            files = sorted(p.name for p in Path(d).iterdir())
            self.assertEqual(files, ["order_audit_2026-09-22.jsonl", "order_audit_2026-09-23.jsonl"])
            first = [json.loads(l) for l in (Path(d) / files[0]).read_text().splitlines()]
            self.assertEqual([(r["run"], r["order_id"]) for r in first], [("a", "1"), ("b", "2")])
            second = [json.loads(l) for l in (Path(d) / files[1]).read_text().splitlines()]
            self.assertEqual([r["action"] for r in second], ["cancel_request"])

    def test_the_writer_never_opens_a_file_for_writing_except_in_append_mode(self):
        source = (ROOT / "order_audit.py").read_text(encoding="utf-8")
        modes = [node.args[1].value for node in ast.walk(ast.parse(source))
                 if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "open" and len(node.args) > 1]
        self.assertEqual(modes, ["a"])

    def test_a_write_failure_does_not_raise(self):
        with tempfile.TemporaryDirectory() as d:
            audit = OrderAuditLog(d)
            with unittest.mock.patch("builtins.open", side_effect=OSError("disk full")):
                with self.assertLogs("order_audit", level="ERROR"):
                    audit.record("create", ReasonCode.QUOTE_NEW)
            self.assertEqual(audit.write_errors, 1)


class TestReasonSets(unittest.TestCase):
    def test_non_risk_cancels_are_only_advances_and_rung_reshuffles(self):
        self.assertEqual(CANCEL_REASONS - RISK_CANCEL_REASONS, {ReasonCode.TARGET_IMPROVED, ReasonCode.RUNG_PROMOTION})

    def test_require_reason(self):
        self.assertIs(require_reason(ReasonCode.SHUTDOWN, CANCEL_REASONS), ReasonCode.SHUTDOWN)
        with self.assertRaises(ValueError):
            require_reason(ReasonCode.QUOTE_NEW, CANCEL_REASONS)


if __name__ == "__main__":
    unittest.main()
