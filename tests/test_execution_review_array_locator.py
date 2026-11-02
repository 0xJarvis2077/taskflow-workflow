"""A signed complete-array count retains its exact frozen-source meaning."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution_review
from taskflow.contracts import TaskflowError
from taskflow.execution_review import _local_assessment, _review_source_alias
from taskflow.execution_runner import _info


class CompleteReviewArrayLocatorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.package_path = "draft/evaluation/criteria.json"
        self.alias = "normative/" + self.package_path
        self.source = self.root / "snapshot" / self.package_path
        self.target = self.root / "inputs" / self.alias
        for path in (self.source, self.target):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps([{"id": "one"}, {"id": "two"}]), encoding="utf-8")
        binding = {"task_id": "neutral", "content_digest": "c" * 64,
                   "input_digest": "i" * 64, "evaluation_digest": "e" * 64}
        self.snapshot = {**binding, "snapshot_digest": "s" * 64,
                         "files": [{"path": self.package_path, "role": "rubric", **_info(self.source)}]}
        self.run = {"run_digest": "r" * 64, "snapshot_digest": "s" * 64,
                    "input_digest": binding["input_digest"], "evaluation_digest": binding["evaluation_digest"]}
        entry = {"source": str(self.source), "target": self.alias, "kind": "teacher", **_info(self.source)}
        self.context = {"cwd": self.root / "inputs", "manifest": {
            "candidate_binding": binding, "scoring_source_allowlist": [self.package_path],
            "scoring_projection": {"source": str(self.root / "snapshot/task.json")}, "inputs": [entry]}}

    def resolve(self, locator="all 2 criteria", *, context=None, snapshot=None):
        return _review_source_alias(self.alias, locator, context or self.context,
                                    self.run, snapshot or self.snapshot)

    def assessment(self):
        configuration = self.root / "inputs/original/configuration.json"
        configuration.parent.mkdir(parents=True)
        configuration.write_text('{"model":"neutral-fixture"}', encoding="utf-8")
        self.context["plan"] = {"sources": [
            {"destination": str(configuration), "artifact_id": "actual_configuration", **_info(configuration)}]}
        self.run["artifacts"] = [{"id": "actual_configuration", **_info(configuration)}]
        return {"execution_binding": {key: self.run[key] for key in (
                    "run_digest", "snapshot_digest", "input_digest", "evaluation_digest")},
                "experiment_validity": {"local_profile": "current_registered_local_profile",
                    "local_eligible": True, "served_identity_unknown_disclosed": True,
                    "substantive_disputes": [], "reason": "Neutral frozen sources only.",
                    "evidence": [{"path": "original/configuration.json", "locator": "/model"},
                                 {"path": self.alias, "locator": "all 2 criteria"}]},
                "artifact_status": {"artifacts_complete": True, "reason": "Neutral original retained.",
                    "evidence": [{"path": self.alias, "locator": "all 2 criteria"}]}}

    def test_exact_count_selects_the_entire_bound_array_and_preserves_signed_text(self):
        original = self.target.read_bytes()
        result = self.resolve()
        self.assertEqual("all 2 criteria", result["locator"])
        self.assertEqual("", result["json_pointer"])
        self.assertEqual(2, result["json_array_length"])
        self.assertEqual("entire_json_array", result["locator_type"])
        self.assertEqual(_info(self.source)["sha256"], result["sha256"])
        self.assertEqual(original, self.target.read_bytes())

    def test_wrong_count_shape_syntax_or_non_json_remains_invalid(self):
        for locator in ("all 1 criteria", "all 3 criteria", "all 0 criteria", "all 02 criteria",
                        "all criteria", "all 2 criteria and more", "all two criteria"):
            with self.subTest(locator=locator), self.assertRaises(TaskflowError):
                self.resolve(locator)
        for raw in (b'{"criteria":[1,2]}', b'"two"', b'[]', b'[1,', b'[{"a":1,"a":2},3]'):
            self.source.write_bytes(raw)
            self.target.write_bytes(raw)
            self.context["manifest"]["inputs"][0].update(_info(self.source))
            self.snapshot["files"][0].update(_info(self.source))
            with self.subTest(raw=raw), self.assertRaises(TaskflowError):
                self.resolve()

    def test_count_cannot_bypass_allowlist_source_bytes_role_or_frozen_version(self):
        wrong = copy.deepcopy(self.context)
        wrong["manifest"]["scoring_source_allowlist"] = []
        self.assertIsNone(self.resolve(context=wrong))
        changes = [("kind", "trace"), ("source", str(self.root / "foreign.json")),
                   ("sha256", "f" * 64), ("size", 1)]
        for key, value in changes:
            wrong = copy.deepcopy(self.context)
            wrong["manifest"]["inputs"][0][key] = value
            with self.subTest(input_key=key), self.assertRaises(TaskflowError):
                self.resolve(context=wrong)
        for key in ("content_digest", "input_digest", "evaluation_digest"):
            wrong = copy.deepcopy(self.context)
            wrong["manifest"]["candidate_binding"][key] = "f" * 64
            with self.subTest(binding_key=key), self.assertRaises(TaskflowError):
                self.resolve(context=wrong)
        wrong = copy.deepcopy(self.snapshot)
        wrong["files"][0]["role"] = "input"
        with self.assertRaises(TaskflowError):
            self.resolve(snapshot=wrong)
        self.source.write_text("[1,2,3]", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            self.resolve()

    def test_local_assessment_checks_every_reference_and_exact_run_without_resigning(self):
        score = self.assessment()
        original = copy.deepcopy(score)
        result = _local_assessment(score, self.context, self.run, self.snapshot)
        self.assertTrue(result["signed_binding_valid"])
        self.assertTrue(result["local_eligible"])
        self.assertEqual(3, len(result["evidence"]))
        self.assertEqual(original, score)
        wrong_run = {**self.run, "run_digest": "x" * 64}
        result = _local_assessment(score, self.context, wrong_run, self.snapshot)
        self.assertFalse(result["signed_binding_valid"])
        self.assertIn("signed_local_assessment_has_no_exact_run_snapshot_input_evaluation_binding",
                      result["structural_reasons"])
        score["artifact_status"]["evidence"].append({"path": self.alias, "locator": "/missing"})
        result = _local_assessment(score, self.context, self.run, self.snapshot)
        self.assertFalse(result["signed_binding_valid"])
        self.assertIn("artifact_status_locators_not_bound_to_actual_run", result["structural_reasons"])

    def test_changed_staged_array_cannot_reuse_the_earlier_frozen_sha(self):
        original_expect = execution_review._expect

        def change_after_source_check(path, expected):
            result = original_expect(path, expected)
            if path == self.source:
                self.target.write_text("[1,2,3]", encoding="utf-8")
            return result

        with patch.object(execution_review, "_expect", side_effect=change_after_source_check):
            with self.assertRaisesRegex(TaskflowError, "changed during locator validation"):
                self.resolve("all 3 criteria")


if __name__ == "__main__":
    unittest.main()
