"""Brief receipts project real offline records without trimming machine evidence."""

import contextlib
import copy
import io
import json
from pathlib import Path
import unittest

from taskflow import execution
from taskflow.execution_quality import run_quality_task
from taskflow.execution_tasks import brief_task_receipt
from tests import test_execution_quality as quality_fixtures


class BriefReceiptTests(unittest.TestCase):
    def setUp(self):
        self.case = quality_fixtures.ExplicitQualityTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        record = self.case.register("accepted", identity="brief-quality")
        self.record = run_quality_task(self.case.store, record["flow_id"], True)

    def cli(self, action, *options):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            status = execution.main(["--db", str(self.case.store.path), "quality", action,
                                     self.record["flow_id"], *options])
        return status, json.loads(output.getvalue()), len(output.getvalue().encode("utf-8"))

    def test_real_status_and_collect_brief_preserve_default_machine_record_and_originals(self):
        runtime = Path(self.record["state"]["invocations"][0]["runtime"])
        originals = {path: path.read_bytes() for path in runtime.rglob("*") if path.is_file()}
        expected = copy.deepcopy(self.case.store.get_flow(self.record["flow_id"]))
        default_code, full, full_size = self.cli("status")
        brief_code, brief, brief_size = self.cli("status", "--brief")
        expanded_code, expanded, _ = self.cli("status", "--full")
        self.assertEqual((0, 0, 0), (default_code, brief_code, expanded_code))
        self.assertEqual(expected, full)
        self.assertEqual(full, expanded)
        self.assertEqual("accepted", brief["state"]["outcome"])
        self.assertLess(brief_size, full_size / 3)
        self.assertNotIn("definition", brief)
        self.assertNotIn("sha256", json.dumps(brief))
        self.assertNotIn("manifest_sha256", json.dumps(brief))
        self.assertIn("--full", brief["details"]["argv"])
        _, collected, _ = self.cli("collect", "--brief")
        self.assertEqual(brief["state"], collected["state"])
        self.assertEqual(expected, self.case.store.get_flow(self.record["flow_id"]))
        self.assertEqual(originals, {path: path.read_bytes() for path in originals})

    def test_stage_results_and_blocking_error_counts_are_retained(self):
        record = {"flow_id": "review", "state": {"execution": "unknown", "delivery": "invalid",
            "errors": [{"code": "blocked", "message": "Exact problem" * 1000, "correctable": False}] * 5,
            "results": {"facts": {"execution": "completed", "delivery": "valid", "outcome": "complete"}},
            "invocations": {"facts": [{"work_id": "facts-1", "sid": "original"}], "score": []},
            "facts_seal": {"path": "/private/tmp/facts-seal.json", "sha256": "machine-only"}}}
        original = copy.deepcopy(record)
        brief = brief_task_receipt(record, database=self.case.store.path, operation="review")
        self.assertEqual(5, brief["state"]["error_count"])
        self.assertEqual(3, len(brief["state"]["errors"]))
        self.assertLess(len(brief["state"]["errors"][0]["message"]), 550)
        self.assertEqual("unknown", brief["state"]["execution"])
        self.assertEqual("complete", brief["state"]["results"]["facts"]["outcome"])
        self.assertEqual("original", brief["invocations"][0]["sid"])
        self.assertEqual(original, record)

    def test_brief_and_full_are_explicit_and_mutually_exclusive(self):
        for operation in ("revision", "quality", "solver", "review"):
            for action in ("collect", "status"):
                argv = [operation, action, "neutral"]
                if operation == "review" and action == "collect":
                    argv += ["--stage", "facts"]
                self.assertTrue(execution.parser().parse_args([*argv, "--brief"]).brief)
                self.assertFalse(execution.parser().parse_args(argv).brief)
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    execution.parser().parse_args([*argv, "--brief", "--full"])


if __name__ == "__main__":
    unittest.main()
