"""Revision CLI never starts work from registration, preparation or queries."""

import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution


class RevisionCliTests(unittest.TestCase):
    def test_public_reconstruction_cannot_launch_the_informed_native_quality_carrier(self):
        from taskflow.contracts import TaskflowError
        from tests.test_execution_quality import ExplicitQualityTests

        case = ExplicitQualityTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        spec = copy.deepcopy(case.public_spec)
        spec["work"].update(work_id="unsafe-public", intent_id="unsafe-public", model_slot="public_rebuild")
        spec["runner"].update(intent_id="unsafe-public", model_slot="public_rebuild", mode="native_role")
        before = case.store.events()
        with patch("taskflow.execution_preparation.prepare_or_resume", side_effect=AssertionError("no preparation")):
            with self.assertRaisesRegex(TaskflowError, "public-only isolated adapter"):
                execution.enqueue_spec(case.store, spec, case.root / "unsafe-public-runtime")
        self.assertEqual(before, case.store.events())
        self.assertEqual([], case.store.list_attempts("unsafe-public"))

    def test_actions_and_explicit_model_opt_in(self):
        for action in ("prepare", "collect", "status", "run"):
            args = execution.parser().parse_args(["revision", action, "revision-01"])
            self.assertEqual(action, args.revision_action)
            if action == "run":
                self.assertFalse(args.allow_model)

    def test_help_does_not_create_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "unused.sqlite3"
            result = subprocess.run([sys.executable, "-B", "-m", "taskflow", "execution", "--db",
                                     str(path), "revision", "--help"], capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertFalse(path.exists())

    def test_submit_is_only_registration(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            document = {"definition": {"flow_id": "revision-01"}, "evidence": []}
            path.write_text(json.dumps(document), encoding="utf-8")
            with patch("taskflow.execution_store.ExecutionStore") as store, patch(
                "taskflow.execution_revision.register_revision_task", return_value={"state": {}}
            ) as submit, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, execution.main(["revision", "submit", "--spec", str(path)]))
            submit.assert_called_once_with(store.return_value, document["definition"], [])

    def test_collection_preserves_invalid_and_unknown_results(self):
        for state in ({"execution": "completed", "delivery": "invalid"},
                      {"execution": "unknown", "delivery": "pending"}):
            with patch("taskflow.execution_store.ExecutionStore"), patch(
                "taskflow.execution_revision.collect_revision_task", return_value={"state": state}
            ), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(2, execution.main(["revision", "collect", "revision-01"]))

    def test_no_change_is_a_normal_result(self):
        with patch("taskflow.execution_store.ExecutionStore"), patch(
            "taskflow.execution_revision.run_revision_task",
            return_value={"state": {"execution": "not_started", "delivery": "valid", "outcome": "no_change"}}
        ) as run, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, execution.main(["revision", "run", "revision-01"]))
            run.assert_called_once()
            self.assertFalse(run.call_args.kwargs["allow_model"])


if __name__ == "__main__":
    unittest.main()
