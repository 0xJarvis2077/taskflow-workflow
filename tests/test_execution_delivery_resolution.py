"""A real neutral repair may lack only an unrequested narrative handoff."""

import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from taskflow import execution
from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


SCRIPT = r'''
import hashlib,json,pathlib,shutil,sys
source,defect,mode=sys.argv[1:]
root=pathlib.Path('revised-task/neutral');shutil.copytree(source,root)
target=root/'draft/evaluation/answer.md';before=hashlib.sha256(target.read_bytes()).hexdigest()
target.write_text('Revised actual private support.\n')
if mode=='partial':(root/'draft/evaluation/rubric.md').unlink()
if mode!='no-record':
 pathlib.Path('repair-record.json').write_text(json.dumps({'parent_id':'parent','candidate_id':'child','defect_ids':[defect],'needs_business_decision':False,'changes':[{'path':'draft/evaluation/answer.md','before_sha256':before,'after_sha256':hashlib.sha256(target.read_bytes()).hexdigest()}]}))
print(json.dumps({'type':'thread.started','thread_id':'12345678-1234-4567-8123-123456789012'}))
if mode!='no-final':print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Complete revised package and repair manifest delivered; independent recheck remains.'}}))
if mode!='no-turn':print(json.dumps({'type':'turn.completed'}))
'''


class DeliveryResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.task = make_task(self.root)
        self.parent = execution.capture_candidate(self.store, self.task, "parent")
        self.authority = self.root / "authority.json"
        self.authority.write_text('{"nature":"neutral explicit omission resolution"}')
        self.refs = [evidence_ref(self.authority, "neutral technical acceptance")]
        self.script = self.root / "repair.py"
        self.script.write_text(SCRIPT)
        self.runtime = self.root / "runtime"
        self.work = self.root / "work"
        self.work.mkdir()
        self.history = self.root / "handoff.md"
        self.history.write_text("Historical author evidence, not a required output.")
        self.open_defect()

    def open_defect(self):
        self.store.enqueue("quality", "neutral", "parent", "quality", "fixture-v1",
                           intent_id="quality-intent", purpose="neutral independent fixture",
                           request_digest=sha256(b"neutral quality"))
        attempt = self.store.claim("fixture-quality")
        aid = attempt["attempt_id"]
        self.store.mark_starting(aid)
        self.store.record_process(aid, 123, "neutral fixture quality", str(uuid.uuid4()))
        self.store.mark_collecting(aid)
        self.store.finish_attempt(aid, process_exit=0, response_complete=True,
                                  artifacts_complete=True, technical_accepted=True,
                                  evidence=self.refs)
        self.store.record_review("prior-review", "parent", aid, verdict="defects",
                                 scope="neutral private standard", whole_package=True,
                                 evidence=self.refs)
        self.store.open_defect("private-defect", "parent", "prior-review",
                               responsibility="actual private support differs", kind="reference",
                               evidence=[evidence_ref(self.task / "draft/evaluation/answer.md", "line 1")])

    def failed_delivery(self, mode="complete", instruction=None):
        runner = {"intent_id": "repair-intent", "policy_version": "fixture-v1",
                  "mode": "test_fixture", "role": "repair", "model": "fixture-cli",
                  "reasoning_effort": "fixture", "timeout_seconds": 15,
                  "cwd": str(self.work), "argv": [str(Path(sys.executable).resolve()),
                    str(self.script), str(self.task), "private-defect", mode],
                  "dependencies": [str(self.script)],
                  "inputs": [{"path": str(self.history), "kind": "history",
                              "target": "author-history/handoff.md"}],
                  "outputs": ["repair-record.json", "handoff.md"],
                  "output_trees": ["revised-task"],
                  "request_sections": [{"kind": "operation", "text": instruction or
                    "Deliver a complete revised-task candidate and repair-record.json."}]}
        execution.enqueue_spec(self.store, {"work": {
            "work_id": "repair", "task_id": "neutral", "candidate_id": "parent",
            "role": "repair", "policy_id": "fixture-v1", "intent_id": "repair-intent",
            "purpose": "neutral actual repair fixture", "evidence": self.refs},
            "runner": runner}, self.runtime)
        self.attempt = execution.run_worker(self.store, "fixture-author")
        self.assertEqual("failed", self.attempt["state"])
        return self.attempt

    def adopt(self, omitted=None):
        return self.store.accept_delivery_resolution(
            self.attempt["attempt_id"], omitted_outputs=["handoff.md"] if omitted is None else omitted,
            evidence=self.refs)

    def test_accepts_auxiliary_omission_without_changing_failure_or_collection(self):
        old = self.failed_delivery()
        original = {n: (self.runtime / n).read_bytes() for n in
                    ("manifest.json", "request.md", "trace.jsonl", "collection.json", "collection-lock.json")}
        result = self.adopt()
        for key in ("state", "technical_accepted", "artifacts_complete", "cli_sid", "process_exit"):
            self.assertEqual(old[key], result[key])
        self.assertEqual("accepted_auxiliary_omission", result["delivery_resolution"]["status"])
        self.assertEqual(["handoff.md"], result["delivery_resolution"]["omitted_outputs"])
        self.assertIsNone(self.store.claim("never-retry"))
        self.assertEqual(result, self.adopt())
        for name, raw in original.items():
            self.assertEqual(raw, (self.runtime / name).read_bytes())
        self.assertFalse((self.runtime / "artifacts/handoff.md").exists())

    def test_same_actual_author_can_register_and_repair_after_resolution(self):
        self.failed_delivery()
        with self.assertRaises(TaskflowError):
            execution.capture_candidate(self.store, self.runtime / "artifacts/revised-task/neutral",
                                        "child", parent_id="parent", author_attempt=self.attempt["attempt_id"])
        self.adopt()
        child = execution.capture_candidate(self.store, self.runtime / "artifacts/revised-task/neutral",
                                            "child", parent_id="parent", author_attempt=self.attempt["attempt_id"])
        repair = self.store.record_repair("private-defect", "child", disposition="private_repair",
                                          evidence=self.refs)
        self.assertEqual(self.attempt["attempt_id"], child["author_attempt"])
        self.assertEqual(self.attempt["attempt_id"], repair["author_attempt"])
        self.assertEqual("repaired", repair["state"])

    def test_resolved_author_cannot_register_another_candidate_identity(self):
        self.failed_delivery()
        self.adopt()
        with self.assertRaisesRegex(TaskflowError, "another candidate or version"):
            execution.capture_candidate(self.store, self.runtime / "artifacts/revised-task/neutral",
                                        "other-child", parent_id="parent", author_attempt=self.attempt["attempt_id"])

    def test_missing_turn_or_final_is_not_an_auxiliary_omission(self):
        self.failed_delivery("no-turn")
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_missing_final_response_is_not_an_auxiliary_omission(self):
        self.failed_delivery("no-final")
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_missing_genuine_payload_is_rejected(self):
        self.failed_delivery("partial")
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_missing_repair_record_is_rejected(self):
        self.failed_delivery("no-record")
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_explicit_requested_output_is_not_waived(self):
        self.failed_delivery(instruction="Deliver repair-record.json and handoff.md.")
        with self.assertRaisesRegex(TaskflowError, "explicitly requested"):
            self.adopt()

    def test_arbitrary_or_substantive_omissions_are_rejected(self):
        self.failed_delivery()
        for names in ([], ["repair-record.json"], ["revised-task"], ["handoff.md", "handoff.md"], "handoff.md"):
            with self.subTest(names=names), self.assertRaises(TaskflowError):
                self.adopt(names)

    def test_changed_sealed_artifact_rejects_acceptance_and_later_registration(self):
        self.failed_delivery()
        self.adopt()
        target = self.runtime / "artifacts/revised-task/neutral/draft/evaluation/answer.md"
        target.write_text("Changed after the actual author delivery.")
        with self.assertRaises(TaskflowError):
            self.adopt()
        with self.assertRaises(TaskflowError):
            execution.capture_candidate(self.store, self.runtime / "artifacts/revised-task/neutral",
                                        "child", parent_id="parent", author_attempt=self.attempt["attempt_id"])

    def test_changed_collection_or_acceptance_evidence_cannot_be_resealed(self):
        self.failed_delivery()
        self.adopt()
        self.authority.write_text("different technical acceptance")
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_sealed_failed_collection_bytes_are_not_replaced(self):
        self.failed_delivery()
        self.adopt()
        collection = self.runtime / "collection.json"
        collection.write_bytes(collection.read_bytes() + b" ")
        with self.assertRaisesRegex(TaskflowError, "immutable digest"):
            self.adopt()


if __name__ == "__main__":
    unittest.main()
