"""Actual command boundary around the neutral registered controller finding."""

import contextlib
import io
import json
import unittest

from taskflow import execution
from taskflow.contracts import write_json
from tests.test_execution_controller_finding import make_controller_finding_fixture


class ControllerFindingCLITests(unittest.TestCase):
    def call(self, fixture, arguments):
        path = fixture["author"].root / "controller-operation.json"
        write_json(path, arguments)
        out, error = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(error):
            code = execution.main(["--db", str(fixture["store"].path), "controller-finding-reopen", "--record", str(path)])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, json.loads(error.getvalue()) if error.getvalue() else None

    def test_command_reopens_same_registered_source_once_and_preserves_hold(self):
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

    def test_command_wrong_controller_is_readable_and_does_not_open_a_defect(self):
        value = make_controller_finding_fixture(self)
        before = value["store"].events()
        code, _, error = self.call(value, {**value["args"], "controller": "foreign-controller"})
        self.assertEqual(2, code)
        self.assertIn("owner", error["error"])
        self.assertEqual(before, value["store"].events())


if __name__ == "__main__":
    unittest.main()
