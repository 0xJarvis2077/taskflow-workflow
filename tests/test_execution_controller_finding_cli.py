"""Retired CLI rejection and immutable historical controller contract."""

import contextlib
import io
import json
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_controller_finding import reopen_controller_public_finding
from tests.test_execution_controller_finding import make_controller_finding_fixture


class ControllerFindingCLITests(unittest.TestCase):
    def call(self, fixture, arguments):
        path = fixture["author"].root / "controller-operation.json"
        write_json(path, arguments)
        try:
            return 0, reopen_controller_public_finding(fixture["store"], **arguments), None
        except TaskflowError as error:
            return 2, None, {"error": str(error)}

    def test_historical_backend_reopens_same_registered_source_once_and_preserves_hold(self):
        value = make_controller_finding_fixture(self)
        code, result, error = self.call(value, value["args"])
        self.assertEqual(0, code, error)
        self.assertEqual("reopened", result["status"])
        self.assertEqual(0, result["models_dispatched"])
        self.assertEqual(value["experiment"], value["store"].get_flow(value["experiment"]["flow_id"]))
        before = value["store"].events()
        code, result, error = self.call(value, value["args"])
        self.assertEqual(0, code, error)
        self.assertEqual("already_registered", result["status"])
        self.assertEqual(before, value["store"].events())

    def test_historical_backend_wrong_controller_is_readable_and_does_not_open_a_defect(self):
        value = make_controller_finding_fixture(self)
        before = value["store"].events()
        code, _, error = self.call(value, {**value["args"], "controller": "foreign-controller"})
        self.assertEqual(2, code)
        self.assertIn("owner", error["error"])
        self.assertEqual(before, value["store"].events())
    def test_retired_command_cannot_reopen_or_create_a_database(self):
        value = make_controller_finding_fixture(self)
        before = value["store"].events()
        with patch("taskflow.execution_store.ExecutionStore") as store, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                execution.main(["controller-finding-reopen", "--record", "unused.json"])
        self.assertEqual(2, error.exception.code)
        store.assert_not_called()
        self.assertEqual(before, value["store"].events())



if __name__ == "__main__":
    unittest.main()
