"""Repair defect names normalize mechanically; authors and quality stay distinct."""

import copy
import json
import unittest

from pathlib import Path

from taskflow.contracts import TaskflowError
from tests import test_execution_flow_contracts as fixtures
from taskflow import execution_flow_contracts as contracts


class RepairDefectIdentityTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FlowContractTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.child, self.record = self.case.repair_fixture()
        self.record["role"] = "repair"
        self.record["covered_defect_ids"] = self.record.pop("defect_ids")

    def test_original_alias_returns_canonical_identity_without_rewriting(self):
        value = self.case.repair(self.child, self.record)
        self.assertEqual(["defect-a"], value["defect_ids"])
        self.assertEqual(self.record, json.loads(self.case.repair_path.read_text()))

    def test_both_fields_must_independently_name_the_exact_unique_string_set(self):
        self.record["defect_ids"] = ["defect-a"]
        self.assertEqual(["defect-a"], self.case.repair(self.child, self.record)["defect_ids"])
        for key in ("defect_ids", "covered_defect_ids"):
            for wrong in (None, [], ["other"], ["defect-a", "extra"], ["defect-a", "defect-a"], [1], [" "]):
                value = {**self.record, key: wrong}
                with self.subTest(key=key, wrong=wrong), self.assertRaises(TaskflowError):
                    self.case.repair(self.child, value)

    def test_alias_needs_repair_role_explicit_false_and_actual_changes(self):
        for changes in ({"role": "quality"}, {"role": None}, {"needs_business_decision": None},
                        {"needs_business_decision": 0}, {"needs_business_decision": "false"},
                        {"needs_business_decision": True}, {"changes": []}):
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                self.case.repair(self.child, {**self.record, **changes})
        missing = copy.deepcopy(self.record)
        del missing["role"]
        with self.assertRaises(TaskflowError):
            self.case.repair(self.child, missing)
        child = {**self.child, "files": self.case.candidate["files"]}
        with self.assertRaisesRegex(TaskflowError, "no actual"):
            self.case.repair(child, self.record)

    def test_current_candidate_drift_and_incomplete_diff_still_reject(self):
        for change in ({"parent_id": "other"}, {"candidate_id": "other"},
                       {"changes": self.record["changes"][:-1]}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.case.repair(self.child, {**self.record, **change})
        path = self.case.root / "revised/draft/input/prompt.md"
        path.write_text("Uncaptured later source.")
        with self.assertRaisesRegex(TaskflowError, "bytes changed"):
            self.case.repair(self.child, self.record)


class RepairIdentityFieldTests(unittest.TestCase):
    """Pure identity contract; these tests do not stand in for sealed IO tests."""

    def check(self, record, expected=None):
        return contracts.repair_defect_ids(record, ["issue-b", "issue-a"] if expected is None else expected)

    def test_historical_strict_lookup_rejects_the_same_neutral_alias_identity(self):
        record = {"role": "repair", "covered_defect_ids": ["issue-a", "issue-b"]}
        # This is the old validator's exact field lookup, without any IO mocks.
        with self.assertRaisesRegex(TaskflowError, "^repair defect_ids: list required$"):
            contracts._ids(record.get("defect_ids"), "repair defect_ids", nonempty=True)
        self.assertEqual(["issue-b", "issue-a"], self.check(record))

    def test_alias_and_dual_fields_preserve_the_assigned_order_without_mutation(self):
        for record in ({"role": "repair", "covered_defect_ids": ["issue-a", "issue-b"]},
                       {"role": "repair", "covered_defect_ids": ["issue-a", "issue-b"],
                        "defect_ids": ["issue-b", "issue-a"]},
                       {"defect_ids": ["issue-a", "issue-b"]}):
            original = copy.deepcopy(record)
            self.assertEqual(["issue-b", "issue-a"], self.check(record))
            self.assertEqual(original, record)

    def test_wrong_role_and_every_invalid_identity_list_reject(self):
        for key in ("defect_ids", "covered_defect_ids"):
            for value in (None, (), "issue-a", [], ["issue-a"], ["issue-a", "issue-b", "extra"],
                          ["issue-a", "issue-a"], [1, "issue-b"], [True, "issue-b"], ["", "issue-b"]):
                with self.subTest(key=key, value=value), self.assertRaises(TaskflowError):
                    self.check({"role": "repair", key: value})
        for role in (None, "quality", "author", False):
            with self.subTest(role=role), self.assertRaises(TaskflowError):
                self.check({"role": role, "covered_defect_ids": ["issue-a", "issue-b"]})
        for record in ({}, {"covered_defect_ids": ["issue-a", "issue-b"]},
                       {"role": "quality", "defect_ids": ["issue-a", "issue-b"]},
                       {"role": "repair", "defect_ids": ["issue-a", "issue-b"], "covered_defect_ids": None}):
            with self.subTest(record=record), self.assertRaises(TaskflowError):
                self.check(record)

    def test_assigned_ids_are_also_unique_nonempty_strings(self):
        for expected in (None, [], ["x", "x"], [1], "x"):
            with self.subTest(expected=expected), self.assertRaises(TaskflowError):
                contracts.repair_defect_ids({"defect_ids": ["x"]}, expected)

    def test_future_producer_requests_canonical_role_identity(self):
        request = contracts.repair_instructions({"candidate_id": "parent", "revised_candidate_id": "child", "defect_ids": ["issue-a"]})
        self.assertIn('"role": "repair"', request)
        self.assertIn('"defect_ids"', request)


class RepairIdentityGuardTests(unittest.TestCase):
    def test_noncompleted_or_foreign_author_is_rejected_before_io(self):
        attempt = {"attempt_id": "original", "state": "completed", "role": "repair", "process_exit": 0,
                   "response_complete": True, "artifacts_complete": True, "technical_accepted": True,
                   "cli_sid": "original-sid", "candidate_id": "parent", "task_id": "neutral"}
        parent = {"candidate_id": "parent", "task_id": "neutral"}
        child = {"parent_id": "parent", "candidate_id": "child", "author_attempt": "original"}
        for change in ({"state": "running"}, {"role": "quality"}, {"process_exit": True}, {"process_exit": 1},
                       {"response_complete": 1}, {"artifacts_complete": False}, {"technical_accepted": None},
                       {"cli_sid": ""}, {"candidate_id": "foreign"}, {"task_id": "foreign"}):
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact completed managed author"):
                contracts.validate_completed_repair_identity(Path("never-read"), attempt={**attempt, **change},
                    parent=parent, candidate=child, parent_root=Path("parent"), candidate_root=Path("child"),
                    archive=Path("archive"), defect_ids=["issue-a"])
        for change in ({"author_attempt": "foreign"}, {"parent_id": "foreign"}):
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact completed managed author"):
                contracts.validate_completed_repair_identity(Path("never-read"), attempt=attempt,
                    parent=parent, candidate={**child, **change}, parent_root=Path("parent"), candidate_root=Path("child"),
                    archive=Path("archive"), defect_ids=["issue-a"])

    def test_unknown_dependency_and_new_business_hold_are_rejected_before_io(self):
        from taskflow.execution_lifecycle import _resolved_repair_identity_dependency
        parent = {"definition": {}, "state": {}}
        reasons = ({}, {"stage": "child_quality", "reason": "Unknown decision"},
                   {"stage": "child_quality", "reason": {"reason": "other", "work_id": "node"}},
                   {"stage": "child_quality", "reason": {"reason": "repair defect_ids: list required", "work_id": 1}})
        for previous in reasons:
            with self.subTest(previous=previous), self.assertRaisesRegex(TaskflowError, "exact resolved repair identity"):
                _resolved_repair_identity_dependency(None, parent, previous, active_only=True)
