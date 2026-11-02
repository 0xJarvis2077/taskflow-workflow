"""Optional native candidates are adopted only from sealed actual author work."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

from taskflow import execution
from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_runner import archive_full_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


SCRIPT = r'''
import json,pathlib,shutil,sys
source,mode=sys.argv[1:]
if mode not in ('ended','missing'):
 root=pathlib.Path('next-task/neutral');shutil.copytree(source,root)
 (root/'draft/evaluation/answer.md').write_text('New optional candidate actual private support.\n')
 (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')
 if mode=='partial':(root/'draft/evaluation/rubric.md').unlink()
handoff={'schema_version':1,'task_id':'neutral','disposition':'candidate_complete','candidate_subpath':'next-task/neutral','reason':'Actual constructed candidate available for new independent quality.','needs_business_decision':False,'source_evidence':[{'path':source,'location':'neutral actual baseline'}],'independent_quality_passed':False,'new_formal_score':None}
if mode=='ended':handoff.update(disposition='research_ended',candidate_subpath=None)
if mode=='decision':handoff.update(disposition='needs_business_decision',needs_business_decision=True)
if mode=='foreign':handoff['task_id']='other'
if mode=='selfsigned':handoff['independent_quality_passed']=True
for name in ('research.md','candidate-decision.md','handoff.md'):pathlib.Path(name).write_text('Actual neutral author result.')
pathlib.Path('issues.json').write_text('{"issues":[]}')
pathlib.Path('handoff.json').write_text(json.dumps(handoff))
print(json.dumps({'type':'thread.started','thread_id':'12345678-1234-4567-8123-123456789012'}))
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Actual core research and conditional candidate delivered.'}}))
if mode!='no-turn':print(json.dumps({'type':'turn.completed'}))
'''


class OptionalCandidateDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.task = make_task(self.root)
        self.parent = execution.capture_candidate(self.store, self.task, "parent")
        self.authority = self.root / "authority.json"
        self.authority.write_text('{"nature":"neutral explicit optional candidate authority"}')
        self.refs = [evidence_ref(self.authority, "neutral authorized complete-candidate branch")]
        self.script = self.root / "author.py"
        self.script.write_text(SCRIPT)
        self.work = self.root / "work"
        self.work.mkdir()
        self.runtime, self.archive = self.root / "runtime", self.root / "archive"

    def run_author(self, mode="complete"):
        runner = {"intent_id": "author-intent", "policy_version": "fixture-v1",
                  "mode": "test_fixture", "role": "author", "model": "fixture-cli",
                  "reasoning_effort": "fixture", "timeout_seconds": 15,
                  "cwd": str(self.work), "argv": [str(Path(sys.executable).resolve()),
                    str(self.script), str(self.task), mode], "dependencies": [str(self.script)],
                  "inputs": [], "outputs": ["research.md", "candidate-decision.md", "handoff.md",
                                               "handoff.json", "issues.json"], "output_trees": [],
                  "request_sections": [{"kind": "operation", "text":
                    "Return the five core research files and, only if formed, a complete next-task candidate."}]}
        execution.enqueue_spec(self.store, {"work": {
            "work_id": "author", "task_id": "neutral", "candidate_id": "parent",
            "role": "author", "policy_id": "fixture-v1", "intent_id": "author-intent",
            "purpose": "neutral actual optional candidate", "evidence": self.refs},
            "runner": runner}, self.runtime)
        self.attempt = execution.run_worker(self.store, "fixture-author")
        if mode != "no-turn":
            self.assertEqual("completed", self.attempt["state"])
        return self.attempt

    def archive_author(self):
        return archive_full_attempt(self.runtime, self.archive)

    def adopt(self, **changes):
        return self.store.record_optional_candidate_delivery(
            self.attempt["attempt_id"], archive=changes.get("archive", self.archive),
            candidate_subpath=changes.get("candidate_subpath", "next-task/neutral"), evidence=self.refs)

    def test_full_archive_optional_candidate_extends_only_actual_declared_delivery(self):
        old = self.run_author()
        original = {name: (self.runtime / name).read_bytes() for name in
                    ("manifest.json", "request.md", "collection.json", "collection-lock.json")}
        self.archive_author()
        result = self.adopt()
        delivery = result["supplemental_delivery"]
        self.assertEqual("accepted_optional_candidate", delivery["status"])
        self.assertEqual(old["state"], result["state"])
        self.assertTrue(result["technical_accepted"])
        self.assertEqual(result, self.adopt())
        candidate = Path(delivery["candidate_root"])
        self.assertTrue(candidate.is_relative_to(self.archive))
        self.assertEqual(len(self.parent["files"]) + 1, len(delivery["file_refs"]))
        delivered = self.store._delivered_artifacts(result)
        self.assertNotIn(sha256((candidate / "unlisted-extra.txt").read_bytes()), delivered)
        self.assertIn(sha256((candidate / "task.json").read_bytes()), delivered)
        child = execution.capture_candidate(self.store, candidate, "child", parent_id="parent",
                                            author_attempt=self.attempt["attempt_id"])
        self.assertEqual(self.attempt["attempt_id"], child["author_attempt"])
        self.assertEqual([], child["external_author_sids"])
        for name, raw in original.items():
            self.assertEqual(raw, (self.runtime / name).read_bytes())
        self.assertIsNone(self.store.claim("no-redraw"))

    def test_work_candidate_cannot_bypass_supplemental_registration(self):
        self.run_author()
        with self.assertRaises(TaskflowError):
            execution.capture_candidate(self.store, self.work / "next-task/neutral", "child",
                                        parent_id="parent", author_attempt=self.attempt["attempt_id"])

    def test_research_ended_is_not_manufactured_into_a_candidate(self):
        self.run_author("ended")
        self.archive_author()
        with self.assertRaisesRegex(TaskflowError, "does not declare"):
            self.adopt()

    def test_business_decision_branch_cannot_adopt_a_candidate(self):
        self.run_author("decision")
        self.archive_author()
        with self.assertRaisesRegex(TaskflowError, "does not declare"):
            self.adopt()

    def test_foreign_handoff_task_is_rejected(self):
        self.run_author("foreign")
        self.archive_author()
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_author_self_signed_quality_is_not_an_unsigned_candidate(self):
        self.run_author("selfsigned")
        self.archive_author()
        with self.assertRaisesRegex(TaskflowError, "independently unsigned"):
            self.adopt()

    def test_other_attempt_full_archive_cannot_supply_candidate_delivery(self):
        self.run_author()
        other = OptionalCandidateDeliveryTests("test_foreign_handoff_task_is_rejected")
        other.setUp()
        try:
            other.run_author()
            other.archive_author()
            with self.assertRaisesRegex(TaskflowError, "exact original attempt"):
                self.adopt(archive=other.archive)
        finally:
            other.doCleanups()

    def test_missing_or_partial_declared_payload_is_rejected(self):
        self.run_author("partial")
        self.archive_author()
        with self.assertRaisesRegex(TaskflowError, "incomplete"):
            self.adopt()

    def test_completed_handoff_without_candidate_files_is_rejected(self):
        self.run_author("missing")
        self.archive_author()
        with self.assertRaises(TaskflowError):
            self.adopt()

    def test_unterminated_turn_never_acquires_optional_delivery(self):
        self.run_author("no-turn")
        self.archive_author()
        with self.assertRaisesRegex(TaskflowError, "completed author"):
            self.adopt()

    def test_changed_actual_terminal_work_is_rejected_before_and_after_acceptance(self):
        self.run_author()
        self.archive_author()
        self.adopt()
        (self.work / "next-task/neutral/draft/evaluation/answer.md").write_text("Later unbound change")
        with self.assertRaisesRegex(TaskflowError, "terminal work"):
            self.adopt()
        with self.assertRaises(TaskflowError):
            self.store._delivered_artifacts(self.store.get_attempt(self.attempt["attempt_id"]))

    def test_changed_archive_or_arbitrary_candidate_path_is_rejected(self):
        self.run_author()
        self.archive_author()
        for path in ("next-task/other", "../outside", "/outside", "next-task/neutral/subdir"):
            with self.subTest(path=path), self.assertRaises(TaskflowError):
                self.adopt(candidate_subpath=path)
        target = self.archive / "supplemental/trees/work/next-task/neutral/draft/evaluation/answer.md"
        target.write_text("Tampered archive payload")
        with self.assertRaises(TaskflowError):
            self.adopt()


if __name__ == "__main__":
    unittest.main()
