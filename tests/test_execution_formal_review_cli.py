"""Explicit formal/facts/score CLI, retired loops cannot dispatch."""
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


class FormalReviewCliTests(unittest.TestCase):
    def test_parser_requires_explicit_stage_and_model_opt_in(self):
        for role in ("solver", "review"):
            command = [role, "run", "neutral"] + (["--stage", "facts"] if role == "review" else [])
            args = execution.parser().parse_args(command)
            self.assertFalse(args.allow_model)
        for action in ("prepare", "collect", "run", "correct"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                execution.parser().parse_args(["review", action, "neutral"])
        self.assertEqual("seal", execution.parser().parse_args(["review", "seal", "neutral"]).review_action)

    def test_retired_automatic_experiments_and_rounds_are_removed(self):
        for command in ("experiment-start", "batch-experiment-start", "round-bind", "round-start"):
            with self.subTest(command=command), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                execution.parser().parse_args([command])

    def test_help_never_creates_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            for role in ("solver", "review"):
                path = Path(temporary) / (role + ".sqlite3")
                result = subprocess.run([sys.executable, "-B", "-m", "taskflow", "execution", "--db", str(path), role, "--help"],
                                        capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertFalse(path.exists())

    def test_submit_registers_only_explicit_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            record = {"definition": {"flow_id": "neutral"}, "evidence": []}
            path.write_text(json.dumps(record))
            for role, module, function in (("solver", "execution_solver", "register_solver_task"),
                                           ("review", "execution_assessment", "register_assessment_task")):
                with patch("taskflow.execution_store.ExecutionStore") as store, patch(
                    "taskflow." + module + "." + function, return_value={"state": {}}
                ) as submit, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(0, execution.main([role, "submit", "--spec", str(path)]))
                submit.assert_called_once_with(store.return_value, record["definition"], [])

    def test_review_run_and_seal_are_separate_operations(self):
        result = {"state": {"execution": "completed", "delivery": "valid", "outcome": "complete"}}
        with patch("taskflow.execution_store.ExecutionStore") as store, patch(
            "taskflow.execution_assessment.run_assessment_task", return_value=result
        ) as run, patch("taskflow.execution_assessment.seal_assessment_facts") as seal, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, execution.main(["review", "run", "neutral", "--stage", "facts"]))
            run.assert_called_once_with(store.return_value, "neutral", stage="facts", allow_model=False)
            seal.assert_not_called()

    def test_invalid_and_unknown_return_actionable_exit_without_replaying(self):
        for stage in ("facts", "score"):
            for fields in ({"execution": "completed", "delivery": "invalid"}, {"execution": "unknown", "delivery": "pending"}):
                with patch("taskflow.execution_store.ExecutionStore"), patch(
                    "taskflow.execution_assessment.collect_assessment_task", return_value={"state": fields}
                ), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(2, execution.main(["review", "collect", "neutral", "--stage", stage]))

    def test_old_material_loops_are_rejected_before_store_creation(self):
        commands = ("author-start", "teacher-successor-start", "batch-author-start",
                    "batch-baseline-quality-start", "lifecycle-register", "lifecycle-run",
                    "lifecycle-resume", "author-completion-observe", "controller-finding-reopen")
        for command in commands:
            with self.subTest(command=command), patch(
                "taskflow.execution_store.ExecutionStore"
            ) as store, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    execution.main([command])
                self.assertEqual(2, error.exception.code)
                store.assert_not_called()

    def test_history_queries_remain_read_only(self):
        history = {"tasks": [], "attempts": [], "work": [], "scopes": []}
        with patch("taskflow.execution_store.ExecutionStore") as store, contextlib.redirect_stdout(io.StringIO()):
            store.return_value.dashboard.return_value = history
            store.return_value.list_flows.return_value = [{"flow_id": "old", "state": {"status": "accepted"}}]
            store.return_value.events.return_value = [{"kind": "old_immutable_evidence"}]
            self.assertEqual(0, execution.main(["status"]))
            self.assertEqual(0, execution.main(["events", "--after", "3"]))
            store.return_value.events.assert_called_once_with(after=3)
            store.return_value.update_flow.assert_not_called()
            store.return_value.claim_next.assert_not_called()


if __name__ == "__main__":
    unittest.main()
