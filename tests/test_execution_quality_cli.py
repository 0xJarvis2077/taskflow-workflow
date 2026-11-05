"""The quality CLI exposes role tasks, not automatic business continuations."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution


class QualityCliTests(unittest.TestCase):
    def test_quality_actions_are_explicit_and_model_launch_is_opt_in(self):
        for action in ("prepare", "collect", "status", "correct", "run"):
            argv = ["quality", action, "neutral-quality"]
            if action == "correct":
                argv += ["--record", "correction.json"]
            args = execution.parser().parse_args(argv)
            self.assertEqual("quality", args.operation)
            self.assertEqual(action, args.quality_action)
            if action == "run":
                self.assertFalse(args.allow_model)

    def test_old_automatic_quality_and_incident_commands_are_not_exposed(self):
        for command in ("flow-register", "flow-run", "flow-resume",
                        "flow-inconclusive-inspect", "flow-inconclusive-continue",
                        "flow-rejected-quality-inspect", "flow-rejected-quality-continue",
                        "flow-repair-capacity-prepare"):
            with self.subTest(command=command), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    execution.parser().parse_args([command])
                self.assertEqual(2, error.exception.code)

    def test_help_creates_no_database(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "unused.sqlite3"
            result = subprocess.run(
                [sys.executable, "-B", "-m", "taskflow", "execution", "--db",
                 str(database), "quality", "--help"],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("correct", result.stdout)
            self.assertFalse(database.exists())

    def test_submit_calls_only_registration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spec.json"
            document = {"definition": {"flow_id": "neutral"}, "evidence": []}
            path.write_text(json.dumps(document), encoding="utf-8")
            with patch("taskflow.execution_store.ExecutionStore") as store, patch(
                "taskflow.execution_quality.register_quality_task", return_value={"state": {}}
            ) as submit, contextlib.redirect_stdout(io.StringIO()):
                code = execution.main(["quality", "submit", "--spec", str(path)])
            self.assertEqual(0, code)
            submit.assert_called_once_with(store.return_value, document["definition"], [])

    def test_invalid_delivery_has_actionable_exit_without_becoming_business_failure(self):
        invalid = {"state": {"execution": "completed", "delivery": "invalid",
                              "outcome": "revision_required", "errors": [{"code": "target_scope"}]}}
        with patch("taskflow.execution_store.ExecutionStore"), patch(
            "taskflow.execution_quality.collect_quality_task", return_value=invalid
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            code = execution.main(["quality", "collect", "neutral"])
        self.assertEqual(2, code)
        self.assertEqual("completed", json.loads(output.getvalue())["state"]["execution"])

    def test_failed_or_unknown_execution_exits_without_a_business_conclusion(self):
        for state in ("failed", "unknown"):
            result = {"state": {"execution": state, "delivery": "pending", "outcome": None}}
            with self.subTest(state=state), patch("taskflow.execution_store.ExecutionStore"), patch(
                "taskflow.execution_quality.collect_quality_task", return_value=result
            ), contextlib.redirect_stdout(io.StringIO()):
                code = execution.main(["quality", "collect", "neutral"])
            self.assertEqual(2, code)

    def test_business_revision_is_a_normal_completed_result(self):
        result = {"state": {"execution": "completed", "delivery": "valid",
                             "outcome": "revision_required"}}
        with patch("taskflow.execution_store.ExecutionStore"), patch(
            "taskflow.execution_quality.run_quality_task", return_value=result
        ) as run, contextlib.redirect_stdout(io.StringIO()):
            code = execution.main(["quality", "run", "neutral"])
        self.assertEqual(0, code)
        run.assert_called_once()
        self.assertFalse(run.call_args.kwargs["allow_model"])


if __name__ == "__main__":
    unittest.main()
