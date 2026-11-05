"""Locked direct role maps preserve the ordinary author successor contract."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_author import _roles, _template
from tests import test_execution_author as author_tests


class DirectRoleMapTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name).resolve() / "roles.json"
        self.roles = {name: {"scope": "whole_package", "runner": {"role": name}}
                      for name in ("quality", "repair", "recheck", "public_rebuild", "recheck_public")}

    def definition(self, value, **extra):
        self.path.write_text(json.dumps(value), encoding="utf-8")
        return {"role_template_ref": {"path": str(self.path),
                "sha256": sha256(self.path.read_bytes()), **extra}}

    def test_direct_map_and_wrapped_workflow_preserve_identical_roles(self):
        direct = self.definition(self.roles)
        original = self.path.read_bytes()
        self.assertEqual({"roles": self.roles}, _template(direct))
        self.assertEqual(original, self.path.read_bytes())
        wrapped = self.definition({"workflows": [{"roles": self.roles}]})
        self.assertEqual(_template(wrapped)["roles"], self.roles)

    def test_incomplete_extra_and_nonwhole_direct_maps_are_rejected(self):
        missing = copy.deepcopy(self.roles)
        missing.pop("recheck")
        nonwhole = copy.deepcopy(self.roles)
        nonwhole["quality"]["scope"] = "affected"
        for value in (missing, {**self.roles, "unknown": {}}, nonwhole):
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                _template(self.definition(value))

    def test_named_workflow_selection_cannot_fall_back_to_direct_map(self):
        with self.assertRaises(TaskflowError):
            _template(self.definition(self.roles, flow_id="named-workflow"))

    def test_changed_locked_bytes_are_still_rejected(self):
        definition = self.definition(self.roles)
        self.path.write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "template changed"):
            _template(definition)

    def test_normal_role_scrubbing_is_unchanged_for_direct_map(self):
        case = author_tests.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        wrapped = json.loads(case.template.read_text())
        roles = wrapped["workflows"][0]["definition"]["roles"]
        definition = {"task_id": "neutral", "policy_id": "fixture-v1",
                      "role_template_ref": {"path": str(case.template),
                                            "sha256": sha256(case.template.read_bytes())}}
        expected = _roles(case.store, definition)
        case.template.write_text(json.dumps(roles))
        definition["role_template_ref"]["sha256"] = sha256(case.template.read_bytes())
        self.assertEqual(expected, _roles(case.store, definition))


if __name__ == "__main__":
    unittest.main()
