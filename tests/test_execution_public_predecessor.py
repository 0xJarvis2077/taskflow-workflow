"""Reuse public originals for new private bytes without transferring quality."""

import copy
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_flow import _predecessor, advance_workflow
from taskflow.execution_public_predecessor import BINDING, KIND
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as fixture


class PublicPredecessorTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.ContinuationTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        case = self.case
        self.definition = case.definition("neutral", outcome="accepted")
        self.attempt = case.predecessor(self.definition, completed=True)
        self.old = case.store.get_candidate(self.attempt["candidate_id"])
        self.target_root = case.root / "private-revision"
        shutil.copytree(Path(self.definition["initial"]["candidate_root"]), self.target_root)
        (self.target_root / "draft/evaluation/answer.md").write_text("New private support awaiting whole review.\n")
        self.target = execution.capture_candidate(case.store, self.target_root, "new-private-root")
        self.assertEqual(self.old["input_digest"], self.target["input_digest"])
        self.assertNotEqual(self.old["content_digest"], self.target["content_digest"])
        self.packet_path = case.root / "public-reuse.json"
        self.packet = {"schema_version": 1, "kind": KIND, "controller": self.definition["owner"],
            "task_id": self.definition["task_id"],
            "predecessor": {key: self.attempt[key] for key in
                            ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id", "cli_sid")},
            "source_candidate_binding": {key: self.old[key] for key in BINDING},
            "target_candidate_binding": {key: self.target[key] for key in BINDING},
            "quality_transfer": False, "reason": "Only private support changed; fresh whole review required.",
            "evidence": case.refs}
        self.definition["initial"].update(candidate_id=self.target["candidate_id"],
            candidate_root=str(self.target_root), expected_digests={key: self.target[key] for key in
            ("input_digest", "content_digest", "evaluation_digest")})
        self.publish()

    def publish(self):
        write_json(self.packet_path, self.packet)
        self.definition["initial"]["public_predecessor_reuse"] = evidence_ref(
            self.packet_path, "Explicit identical-public source reuse, without quality transfer")

    def test_real_fixture_public_archive_then_fresh_whole_review_of_new_private_root(self):
        case = self.case
        before = case.store.get_attempt(self.attempt["attempt_id"])
        record = case.register(self.definition)
        initialized = advance_workflow(case.store, record["flow_id"])
        self.assertTrue(initialized["state"]["initialized"])
        history = initialized["state"]["history"][0]
        self.assertEqual(self.attempt["cli_sid"], history["cli_sid"])
        self.assertEqual(self.target["candidate_id"], history["target_candidate_binding"]["candidate_id"])
        self.assertFalse(history["quality_transfer"])
        archive = Path(history["archive"])
        self.assertTrue((archive / "supplemental/receipt.json").exists())
        old_manifest = json.loads((archive / "manifest.json").read_text())
        self.assertEqual(self.old["candidate_id"], old_manifest["candidate_binding"]["candidate_id"])
        result = case.run_flow([record["flow_id"]])["flows"][0]
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(self.target["candidate_id"], result["state"]["candidate_id"])
        attempts = case.store.dashboard()["attempts"]
        self.assertEqual(2, len(attempts))
        whole = next(row for row in attempts if row["attempt_id"] != self.attempt["attempt_id"])
        self.assertNotEqual(whole["cli_sid"], self.attempt["cli_sid"])
        self.assertEqual(self.target["candidate_id"], whole["candidate_id"])
        self.assertEqual(before, case.store.get_attempt(self.attempt["attempt_id"]))
        self.assertIsNone(self.target["author_attempt"])
        self.assertIsNone(self.target["parent_id"])

    def test_default_predecessor_contract_remains_same_candidate(self):
        self.definition["initial"].pop("public_predecessor_reuse")
        with self.assertRaisesRegex(TaskflowError, "task/candidate binding"):
            self.case.register(self.definition)

    def test_packet_cannot_transfer_quality_or_change_source_target_controller_or_sid(self):
        original = copy.deepcopy(self.packet)
        for change in ({"quality_transfer": True}, {"controller": "other"}, {"reason": ""},
                       {"source_candidate_binding": {**original["source_candidate_binding"], "content_digest": "f"*64}},
                       {"target_candidate_binding": {**original["target_candidate_binding"], "evaluation_digest": "f"*64}},
                       {"predecessor": {**original["predecessor"], "cli_sid": "foreign"}}):
            self.packet = {**original, **change}
            self.publish()
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.case.register(self.definition)
        self.assertEqual([], self.case.store.list_flows())

    def test_equal_digest_does_not_replace_member_comparison(self):
        actual_get = self.case.store.get_candidate
        changed = copy.deepcopy(self.target)
        public = next(row for row in changed["files"] if row["role"] == "prompt")
        public["sha256"] = "f"*64
        def get(identity):
            return changed if identity == self.target["candidate_id"] else actual_get(identity)
        with patch.object(self.case.store, "get_candidate", side_effect=get), self.assertRaises(TaskflowError):
            _predecessor(self.case.store, self.definition)

    def test_changed_public_target_cannot_reuse_old_attempt(self):
        public = next(row for row in self.target["files"] if row["role"] == "prompt")
        (self.target_root / public["package_path"]).write_text("Changed public task.")
        changed = execution.capture_candidate(self.case.store, self.target_root, "different-public-root")
        self.definition["initial"]["candidate_id"] = changed["candidate_id"]
        self.packet["target_candidate_binding"] = {key: changed[key] for key in BINDING}
        self.publish()
        with self.assertRaises(TaskflowError):
            self.case.register(self.definition)

    def test_unfinished_or_nonpublic_attempt_never_reuses(self):
        for change in ({"state": "running"}, {"role": "repair"}, {"technical_accepted": False},
                       {"artifacts_complete": False}, {"response_complete": False}):
            with self.subTest(change=change), patch.object(self.case.store, "get_attempt", return_value={**self.attempt, **change}), self.assertRaises(TaskflowError):
                self.case.register(self.definition)

    def test_sealed_collection_and_original_input_are_rechecked(self):
        self.case.register(self.definition)
        collection = Path(self.attempt["runtime_path"]) / "collection.json"
        collection.write_text(collection.read_text() + " ")
        result = advance_workflow(self.case.store, self.definition["flow_id"])
        self.assertEqual("exception", result["state"]["status"])
        self.assertEqual(1, len(self.case.store.dashboard()["attempts"]))

    def test_reuse_cannot_skip_attempt_or_fresh_whole_quality(self):
        initial = self.definition["initial"]
        attempt_id = initial.pop("predecessor_attempt")
        with self.assertRaisesRegex(TaskflowError, "exact original attempt"):
            self.case.register(self.definition)
        initial["predecessor_attempt"] = attempt_id
        role = self.definition["roles"]["quality"]
        role["scope"] = "affected"
        with self.assertRaisesRegex(TaskflowError, "fresh whole-package"):
            self.case.register(self.definition)
        role["scope"] = "whole_package"
        role["runner"]["resume_sid"] = "old-context"
        with self.assertRaisesRegex(TaskflowError, "fresh whole-package"):
            self.case.register(self.definition)

    def test_real_role_slot_cannot_be_inferred_from_quality_role(self):
        import taskflow.execution_public_predecessor as module

        root = Path(self.attempt["runtime_path"])
        manifest = json.loads((root / "manifest.json").read_text())
        with patch.object(module, "_validate", return_value={**manifest, "mode": "native_role"}), self.assertRaisesRegex(TaskflowError, "public reconstruction role"):
            self.case.register(self.definition)
