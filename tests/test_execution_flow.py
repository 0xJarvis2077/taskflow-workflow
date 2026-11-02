"""One registration drives real neutral role subprocesses and result receipt."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_flow import (
    _consume_review, _consumer_lock, _publish_once, _review_metadata, advance_workflow, register_workflow, resume_workflow, run_workflows,
)
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


ROLE_FIXTURE = r'''
import argparse,hashlib,json,pathlib,shutil,sys,time,uuid
p=argparse.ArgumentParser();p.add_argument('--context');p.add_argument('--inputs');p.add_argument('--action');p.add_argument('--scope');p.add_argument('--outcome');p.add_argument('--delay',type=float,default=0)
a=p.parse_args();request=sys.stdin.read();ctx=json.loads(pathlib.Path(a.context).read_text());inputs=pathlib.Path(a.inputs)
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p=pathlib.Path(p);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v))
sid=str(uuid.uuid4());print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
time.sleep(a.delay)
if a.action=='repair':
 task=json.loads((inputs/'execution/task.json').read_text());root=pathlib.Path('revised-task')/ctx['task_id'];root.mkdir(parents=True)
 save(root/'task.json',task)
 for item in task['files']:
  target=root/item['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(inputs/item['path'],target)
 changed='draft/evaluation/answer.md';before=digest(root/changed);(root/changed).write_text('Actual repaired private support.\n')
 save('repair-record.json',{'parent_id':ctx['candidate_id'],'candidate_id':ctx['revised_candidate_id'],'defect_ids':ctx['defect_ids'],'needs_business_decision':False,'changes':[{'path':changed,'before_sha256':before,'after_sha256':digest(root/changed)}]})
 pathlib.Path('handoff.md').write_text('Fixture actual material delivery, not a business/model result.')
else:
 binding={k:ctx[k] for k in ['candidate_id','intent_id','content_digest','input_digest','evaluation_digest']}
 defects=[];blocks=[]
 if a.outcome=='revision_required':
  name='draft/evaluation/answer.md';blocks=['PRIVATE-ONE'];defects=[{'id':blocks[0],'kind':'reference','responsibility':'The private support differs from the published rule.','affected_paths':[name],'evidence':[{'path':name,'sha256':digest(inputs/name),'location':'line1'}]}]
 route={'schema_version':1,**binding,'signer_role':'independent_quality','independent_of_current_author':True,'participated_in_production':False,'scope':a.scope,'outcome':a.outcome,'blocking_issue_ids':blocks,'covered_defect_ids':ctx['defect_ids'] if a.outcome=='accepted' else [],'reason':'Neutral signed fixture outcome.'}
 save('adoption-readiness.json',{'execution_routing':route});save('issues.json',{'execution_binding':binding,'issues':defects});pathlib.Path('intake.md').write_text('Complete neutral fixture examination.')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral fixture delivery.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
'''


class ContinuationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.script = self.root / "role.py"
        self.script.write_text(ROLE_FIXTURE)
        self.authority = self.root / "authorization.json"
        write_json(self.authority, {"nature": "test_fixture", "scope": "neutral continuation"})
        self.refs = [evidence_ref(self.authority, "neutral fixture authorization")]

    def definition(self, identity="first", outcome="revision_required", delay=0):
        task = make_task(self.root, identity)
        base = {"mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture",
                "timeout_seconds": 15, "inputs": [], "request_sections": [],
                "dependencies": [str(self.script)], "cwd": "{{work}}"}
        def role(action, result, scope):
            runner = copy.deepcopy(base)
            runner.update(role="repair" if action == "repair" else "quality",
                          argv=[str(Path(sys.executable).resolve()), str(self.script),
                                "--context", "{{runtime}}/inputs/execution/current.json",
                                "--inputs", "{{runtime}}/inputs", "--action", action,
                                "--scope", scope, "--outcome", result, "--delay", str(delay)],
                          outputs=["repair-record.json", "handoff.md"] if action == "repair"
                          else ["intake.md", "issues.json", "adoption-readiness.json"],
                          output_trees=["revised-task"] if action == "repair" else [])
            value = {"runner": runner, "purpose": "preauthorized neutral " + action, "scope": scope}
            if action == "repair":
                value["candidate_subpath"] = "revised-task/" + identity
            return value
        return {"schema_version": 1, "flow_id": "flow-" + identity, "task_id": identity,
                "owner": "fixture-controller", "policy_id": "fixture-v1",
                "directory": str(self.root / ("flow-data-" + identity)),
                "initial": {"candidate_root": str(task), "candidate_id": identity + "-initial"},
                "accepted_use": "retain bounded neutral fixture only; never a target experiment",
                "roles": {"quality": role("quality", outcome, "whole_package"),
                          "repair": role("repair", "unused", "affected"),
                          "recheck": role("recheck", "accepted", "affected")}}

    def register(self, definition):
        return register_workflow(self.store, definition, self.refs)

    def run_flow(self, identities=None):
        return run_workflows(self.store, identities, allow_model=True, poll_seconds=.01)

    def predecessor(self, definition, *, completed=False):
        initial = definition["initial"]
        candidate = execution.capture_candidate(self.store, Path(initial["candidate_root"]), initial["candidate_id"])
        work = self.root / (definition["task_id"] + "-external-work")
        work.mkdir()
        script = self.root / (definition["task_id"] + "-external.py")
        script.write_text(
            "import json,pathlib\n"
            "pathlib.Path('delivery.txt').write_text('neutral external original')\n"
            "print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000001'}))\n"
            "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'neutral'}}))\n"
            "print(json.dumps({'type':'turn.completed'}))\n"
        )
        runtime = self.root / (definition["task_id"] + "-external-runtime")
        work_id = definition["task_id"] + "-external"
        spec = {"work": {"work_id": work_id, "task_id": candidate["task_id"],
                         "candidate_id": candidate["candidate_id"], "intent_id": work_id + "-intent",
                         "role": "quality", "policy_id": "fixture-v1", "purpose": "neutral external fixture",
                         "request_digest": "0" * 64, "evidence": self.refs},
                "runner": {"intent_id": work_id + "-intent", "role": "quality", "policy_version": "fixture-v1",
                           "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture",
                           "timeout_seconds": 15, "cwd": str(work), "inputs": [], "request_sections": [],
                           "dependencies": [str(script)], "argv": [str(Path(sys.executable).resolve()), str(script)],
                           "outputs": ["delivery.txt"]}}
        execution.enqueue_spec(self.store, spec, runtime)
        if completed:
            execution.run_worker(self.store, "external-fixture", True, [work_id])
            attempt = next(a for a in self.store.dashboard()["attempts"] if a["work_id"] == work_id)
        else:
            attempt = self.store.claim("external-fixture", [work_id])
        initial["predecessor_attempt"] = attempt["attempt_id"]
        initial["predecessor_binding"] = {key: attempt[key] for key in
                                          ("work_id", "intent_id", "candidate_id", "task_id")}
        initial["expected_digests"] = {key: candidate[key] for key in
                                       ("input_digest", "content_digest", "evaluation_digest")}
        return attempt

    def test_running_predecessor_wait_is_stable_without_process_recovery(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition)
        self.register(definition)
        with patch("taskflow.execution.reconcile", side_effect=AssertionError("must not recover external worker")), \
             patch("taskflow.execution_runner.archive_full_attempt", side_effect=AssertionError("must not archive external worker")):
            first = advance_workflow(self.store, definition["flow_id"])
            again = advance_workflow(self.store, definition["flow_id"])
        self.assertEqual("active", first["state"]["status"])
        self.assertFalse(first["state"]["initialized"])
        self.assertEqual(attempt["attempt_id"], first["state"]["waiting_predecessor"]["attempt_id"])
        self.assertEqual(first["revision"], again["revision"])
        self.assertEqual("claimed", self.store.get_attempt(attempt["attempt_id"])["state"])

    def test_predecessor_registration_rejects_wrong_intent_version_or_reused_work(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition)
        changes = [
            {"first_work_id": attempt["work_id"]},
            {"initial": {**definition["initial"], "predecessor_binding": {
                **definition["initial"]["predecessor_binding"], "intent_id": "foreign-intent"}}},
            {"initial": {**definition["initial"], "expected_digests": {
                **definition["initial"]["expected_digests"], "evaluation_digest": "f" * 64}}},
        ]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.register({**definition, **change})
        self.assertEqual([], self.store.list_flows())

    def test_completed_legacy_predecessor_waits_for_seal_then_initializes_once(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition, completed=True)
        definition["initial"].pop("predecessor_binding")
        self.register(definition)
        seal = Path(attempt["runtime_path"]) / "collection-lock.json"
        raw = seal.read_bytes()
        seal.unlink()
        with patch("taskflow.execution_runner.archive_full_attempt", side_effect=AssertionError("unsealed")):
            waiting = advance_workflow(self.store, definition["flow_id"])
        self.assertEqual("collection_not_sealed", waiting["state"]["waiting_predecessor"]["reason"])
        seal.write_bytes(raw)
        initialized = advance_workflow(self.store, definition["flow_id"])
        self.assertTrue(initialized["state"]["initialized"])
        self.assertNotIn("waiting_predecessor", initialized["state"])
        self.assertEqual(attempt["cli_sid"], initialized["state"]["history"][0]["cli_sid"])
        result = self.run_flow([definition["flow_id"]])
        self.assertEqual("accepted", result["flows"][0]["state"]["status"])
        self.run_flow([definition["flow_id"]])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_terminal_predecessor_stops_only_its_flow_without_recovery_or_reclaim(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition)
        self.register(definition)
        for status in ("failed", "uncertain", "cancelled"):
            with self.subTest(status=status), \
                 patch.object(self.store, "get_attempt", return_value={**attempt, "state": status}), \
                 patch.object(self.store, "claim", side_effect=AssertionError("never reclaim predecessor")), \
                 patch("taskflow.execution.reconcile", side_effect=AssertionError("never recover predecessor")), \
                 patch("taskflow.execution_runner.archive_full_attempt", side_effect=AssertionError("never archive failed predecessor")):
                record = self.store.get_flow(definition["flow_id"])
                if record["state"]["status"] == "exception":
                    state = {**record["state"], "status": "active"}
                    self.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "fixture_reset"})
                failed = advance_workflow(self.store, definition["flow_id"])
                self.assertEqual("exception", failed["state"]["status"])
                self.assertIn(status, failed["state"]["exception"]["reason"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_completion_rechecks_predecessor_work_binding_before_adoption(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition, completed=True)
        self.register(definition)
        changed = {**self.store.get_work(attempt["work_id"]), "intent_id": "foreign-intent"}
        with patch.object(self.store, "get_work", return_value=changed), \
             patch("taskflow.execution_runner.archive_full_attempt", side_effect=AssertionError("foreign intent")):
            failed = advance_workflow(self.store, definition["flow_id"])
        self.assertEqual("exception", failed["state"]["status"])
        self.assertIn("work/attempt binding", failed["state"]["exception"]["reason"])

    def test_independent_review_rejects_both_public_predecessor_sid_kinds(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition, completed=True)
        for kind in ("adopted_predecessor", "public_reconstruction"):
            record = {"definition": {}, "state": {"history": [{"kind": kind, "cli_sid": attempt["cli_sid"]}]}}
            with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "reused a public predecessor"):
                _consume_review(self.store, record, attempt, {})

    def test_one_registration_runs_quality_actual_repair_recheck_receipt(self):
        definition = self.definition()
        self.register(definition)
        result = self.run_flow()
        state = result["flows"][0]["state"]
        self.assertEqual("accepted", state["status"], state)
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(["quality", "repair", "quality"], [item["role"] for item in attempts])
        self.assertEqual(3, len({item["cli_sid"] for item in attempts}))
        self.assertEqual("closed", self.store.get_defect(state["repaired_defects"][0])["state"])
        self.assertEqual("accepted", read_json(Path(state["receipt"]))["status"])
        self.assertEqual("Actual repaired private support.\n", (Path(state["candidate_root"]) / "draft/evaluation/answer.md").read_text())
        self.run_flow()
        self.assertEqual(3, len(self.store.dashboard()["attempts"]))

    def test_pass_branch_does_not_manufacture_repair_or_solver(self):
        self.register(self.definition(outcome="accepted"))
        state = self.run_flow()["flows"][0]["state"]
        self.assertEqual("accepted", state["status"], state)
        self.assertEqual(["quality"], [a["role"] for a in self.store.dashboard()["attempts"]])
        self.assertFalse(read_json(Path(state["receipt"]))["human_read_claimed"])

    def test_inconclusive_one_topic_does_not_block_other_ready_topic(self):
        self.register(self.definition("one", "inconclusive", .2))
        self.register(self.definition("two", "accepted", .2))
        result = self.run_flow()
        self.assertEqual({"exception", "accepted"}, {r["state"]["status"] for r in result["flows"]})
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(2, len(attempts))
        self.assertTrue(all(a["state"] == "completed" for a in attempts))
        self.assertLess(max(a["start_intent_at"] for a in attempts), min(a["ended_at"] for a in attempts))

    def test_no_model_authorization_does_not_claim_ready_job(self):
        self.register(self.definition(outcome="accepted"))
        result = run_workflows(self.store, allow_model=False, poll_seconds=.01)
        self.assertEqual("ready_requires_explicit_model_authorization", result["state"])
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_immutable_registration_retry_and_changed_definition_rejected(self):
        definition = self.definition(outcome="accepted")
        first = self.register(definition)
        second = self.register(definition)
        self.assertEqual(first["definition_digest"], second["definition_digest"])
        definition["accepted_use"] = "another purpose"
        with self.assertRaises(TaskflowError):
            self.register(definition)

    def test_publish_after_file_before_cursor_is_reused_not_overwritten(self):
        path = self.root / "once.json"
        _publish_once(path, {"state": "first"})
        inode = path.stat().st_ino
        _publish_once(path, {"state": "first"})
        self.assertEqual(inode, path.stat().st_ino)
        with self.assertRaises(TaskflowError):
            _publish_once(path, {"state": "changed"})

    def test_receipt_publication_before_db_commit_recovers_once(self):
        definition = self.definition(outcome="accepted")
        self.register(definition)
        run_workflows(self.store, allow_model=False, poll_seconds=.01)
        execution.run_worker(self.store, "fixture", True)
        identity = definition["flow_id"]
        advance_workflow(self.store, identity)
        original = self.store.update_flow
        def crash(flow_id, revision, state, event):
            if event["kind"] == "workflow_result_received":
                raise KeyboardInterrupt("fixture crash after receipt publish")
            return original(flow_id, revision, state, event)
        with patch.object(self.store, "update_flow", side_effect=crash):
            with self.assertRaises(KeyboardInterrupt):
                advance_workflow(self.store, identity)
        self.assertTrue((Path(definition["directory"]) / "result.json").is_file())
        self.assertFalse(self.store.get_flow(identity)["state"].get("receipt"))
        self.run_flow()
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertTrue(self.store.get_flow(identity)["state"]["receipt"])

    def test_consumer_mutex_is_not_a_second_mutable_state_source(self):
        with _consumer_lock(self.store):
            with self.assertRaises(TaskflowError):
                with _consumer_lock(self.store):
                    self.fail("second concurrent consumer acquired the same lock")

    def failed_preflight(self):
        definition = self.definition(outcome="accepted")
        self.register(definition)
        runtime = Path(definition["directory"]) / "nodes/0-quality/runtime"
        def fail_enqueue(**kwargs):
            write_json(runtime / "native-probe-first.json", {"passed": False, "error": "fixture preflight"})
            raise TaskflowError("fixture preflight failed before enqueue")
        with patch.object(self.store, "enqueue", side_effect=fail_enqueue):
            result = self.run_flow()
        self.assertEqual("exception", result["flows"][0]["state"]["status"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        return definition, runtime, result["flows"][0]

    def test_explicit_resume_retains_zero_model_failure_originals_and_same_definition(self):
        definition, runtime, failed = self.failed_preflight()
        receipt = Path(failed["state"]["receipt"])
        originals = {p: (p.stat().st_ino, sha256(p.read_bytes())) for p in
                     [receipt, runtime / "native-probe-first.json", runtime / "manifest.json",
                      runtime.parent / "spec.json", runtime.parent / "current.json"]}
        resumed = resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        again = resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        self.assertEqual(resumed["revision"], again["revision"])
        self.assertEqual(failed["definition_digest"], resumed["definition_digest"])
        self.assertEqual([sha256((runtime / "native-probe-first.json").read_bytes())],
                         resumed["state"]["probe_retry_authorizations"][str(runtime)])
        result = self.run_flow()["flows"][0]
        self.assertEqual("accepted", result["state"]["status"], result["state"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(1, len(result["state"]["resolved_exceptions"]))
        for p, expected in originals.items():
            self.assertEqual(expected, (p.stat().st_ino, sha256(p.read_bytes())))

    def test_probe_retry_grant_matches_exact_failed_reports_and_cannot_repeat_after_new_failure(self):
        definition, runtime, failed = self.failed_preflight()
        resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        grant = self.store.get_flow(definition["flow_id"])["state"]["probe_retry_authorizations"][str(runtime)]
        spec = read_json(runtime.parent / "spec.json")
        manifest = read_json(runtime / "manifest.json")
        manifest["mode"] = "native_role"
        def fail_probe(path):
            write_json(path / "native-probe-second.json", {"passed": False, "error": "new fixture failure"})
            return {"passed": False}
        with patch("taskflow.execution_runner._validate", return_value=manifest), \
             patch("taskflow.execution_runner.probe_native_attempt", side_effect=fail_probe) as probe:
            with self.assertRaisesRegex(TaskflowError, "previous native probe"):
                execution.enqueue_spec(self.store, spec, runtime, resume_prepared=True, probe_native=True)
            self.assertEqual(0, probe.call_count)
            with self.assertRaisesRegex(TaskflowError, "current native probe failed"):
                execution.enqueue_spec(self.store, spec, runtime, resume_prepared=True, probe_native=True,
                                       retry_failed_probes=grant)
            with self.assertRaisesRegex(TaskflowError, "previous native probe"):
                execution.enqueue_spec(self.store, spec, runtime, resume_prepared=True, probe_native=True,
                                       retry_failed_probes=grant)
            self.assertEqual(1, probe.call_count)
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_resume_refuses_uncertain_actual_attempt_without_clearing_occupancy(self):
        definition = self.definition(outcome="accepted")
        self.register(definition)
        run_workflows(self.store, allow_model=False, poll_seconds=.01)
        attempt = self.store.claim("fixture-crashed-worker")
        self.store.mark_uncertain(attempt["attempt_id"], "fixture launch status unknown")
        record = self.store.get_flow(definition["flow_id"])
        state = {**record["state"], "status": "exception", "exception": {"reason": "fixture"}}
        self.store.update_flow(definition["flow_id"], record["revision"], state, {"kind": "fixture_exception"})
        with self.assertRaisesRegex(TaskflowError, "cannot retry"):
            resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        self.assertEqual("uncertain", self.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual("exception", self.store.get_flow(definition["flow_id"])["state"]["status"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_resume_refuses_actual_failed_role_instead_of_requeueing_it(self):
        definition = self.definition(outcome="accepted")
        definition["roles"]["quality"]["runner"]["argv"].append("--fixture-force-cli-error")
        self.register(definition)
        failed = self.run_flow()["flows"][0]
        self.assertEqual("exception", failed["state"]["status"])
        self.assertEqual("failed", self.store.dashboard()["attempts"][0]["state"])
        with self.assertRaisesRegex(TaskflowError, "cannot retry"):
            resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_reason_or_evidence_location_change_cannot_reuse_a_consumed_resolution(self):
        definition, runtime, _ = self.failed_preflight()
        resume_workflow(self.store, definition["flow_id"], reason="fixture engineering fix", evidence=self.refs)
        record = self.store.get_flow(definition["flow_id"])
        state = {**record["state"], "status": "exception", "exception": {"reason": "new fixture failure"}}
        self.store.update_flow(definition["flow_id"], record["revision"], state, {"kind": "fixture_new_exception"})
        modified_refs = copy.deepcopy(self.refs)
        modified_refs[0]["location"] = "changed explanation, same actual evidence"
        with self.assertRaisesRegex(TaskflowError, "previous resolution evidence"):
            resume_workflow(self.store, definition["flow_id"], reason="changed wording", evidence=modified_refs)

    def test_review_declaration_alias_requires_locked_source_staged_bytes_and_candidate_version(self):
        definition = self.definition(outcome="accepted")
        self.register(definition)
        run_workflows(self.store, allow_model=False, poll_seconds=.01)
        record = self.store.get_flow(definition["flow_id"])
        work = self.store.get_work(record["state"]["current_work_id"])
        candidate = self.store.get_candidate(record["state"]["candidate_id"])
        source = Path(record["state"]["candidate_root"]) / "task.json"
        expected = {"execution/task.json": {"path": str(source), "sha256": sha256(source.read_bytes())}}
        self.assertEqual(expected, _review_metadata(record, candidate, work))
        runtime = Path(work["runtime_path"])
        manifest = read_json(runtime / "manifest.json")
        foreign = copy.deepcopy(manifest)
        next(item for item in foreign["inputs"] if item["target"] == "execution/task.json")["source"] = str(self.authority)
        with patch("taskflow.execution_runner._validate", return_value=foreign):
            with self.assertRaisesRegex(TaskflowError, "foreign source"):
                _review_metadata(record, candidate, work)
        changed_binding = copy.deepcopy(manifest)
        changed_binding["candidate_binding"]["evaluation_digest"] = "a" * 64
        with patch("taskflow.execution_runner._validate", return_value=changed_binding):
            with self.assertRaisesRegex(TaskflowError, "different candidate"):
                _review_metadata(record, candidate, work)
        changed_candidate = copy.deepcopy(candidate)
        changed_candidate["evaluation_digest"] = "a" * 64
        with patch("taskflow.execution_runner._validate", return_value=changed_binding):
            with self.assertRaisesRegex(TaskflowError, "not the current candidate"):
                _review_metadata(record, changed_candidate, work)
        staged = runtime / "inputs/execution/task.json"
        staged.chmod(0o600)
        staged.write_text("changed declaration")
        with patch("taskflow.execution_runner._validate", return_value=manifest):
            with self.assertRaisesRegex(TaskflowError, "source/staged bytes changed"):
                _review_metadata(record, candidate, work)
        self.assertEqual([], self.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
