"""Actual neutral author outcomes advance through the existing ledger only."""

import copy
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import _candidate_structure, _public_root, advance_author
from taskflow.execution_flow import _public_prepare
from taskflow.execution_lifecycle import register_lifecycle
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.packages import check_task
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


AUTHOR_SCRIPT = r'''
import json,pathlib,shutil,sys,time
source,mode,delay,repository=sys.argv[1:]
print(json.dumps({'type':'thread.started','thread_id':'12345678-1234-4567-8123-123456789012'}),flush=True)
time.sleep(float(delay))
handoff={'schema_version':1,'task_id':'neutral','disposition':mode,'candidate_subpath':None,'reason':'Bounded neutral research outcome.','needs_business_decision':mode=='needs_business_decision','source_evidence':[{'path':source,'location':'neutral original source'}],'independent_quality_passed':False,'new_formal_score':None}
if mode=='candidate_complete':
 root=pathlib.Path('next-task/neutral');shutil.copytree(source,root)
 (root/'draft/evaluation/answer.md').write_text('Actual new private reference, pending independent quality.\n')
 sys.path.insert(0,repository)
 from taskflow.packages import check_task
 task=json.loads((root/'task.json').read_text());check=check_task(root,write_report=False)
 for spec in task['reviews']:
  target=root/spec['path'];target.parent.mkdir(parents=True,exist_ok=True)
  target.write_text(json.dumps({'schema_version':1,'review_id':spec['id'],'kind':spec['kind'],'status':'inconclusive','content_digest':check['content_digest'],'input_digest':check['input_digest'],'scope':'Current candidate; no independent review yet.','limitations':['Neutral test fixture, not signed quality.'],'evidence':['actual author candidate']}))
 handoff['candidate_subpath']='next-task/neutral'
if mode.startswith('bad-'):
 handoff['disposition']='research_ended'
 if mode=='bad-schema':handoff['schema_version']=True
 if mode=='bad-quality':handoff['independent_quality_passed']=True
 if mode=='bad-score':handoff['new_formal_score']=1
 if mode=='bad-decision':handoff['needs_business_decision']='false'
if mode=='no-turn':handoff['disposition']='research_ended'
for name in ('research.md','candidate-decision.md','handoff.md'):pathlib.Path(name).write_text('Actual neutral author narrative.')
pathlib.Path('handoff.json').write_text(json.dumps(handoff))
pathlib.Path('issues.json').write_text('{"issues":[]}')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral complete author outcome retained.'}}),flush=True)
if mode!='no-turn':print(json.dumps({'type':'turn.completed'}),flush=True)
'''


PUBLIC_SCRIPT = r'''
import json,pathlib
pathlib.Path('deliverables').mkdir()
pathlib.Path('deliverables/quality-note.md').write_text('Neutral public fixture delivery, not a model business judgment.')
print(json.dumps({'type':'thread.started','thread_id':'87654321-4321-4567-8123-123456789012'}))
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral public fixture completed.'}}))
print(json.dumps({'type':'turn.completed'}))
'''


class AuthorSuccessorTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.task = make_task(self.root)
        task = read_json(self.task / "task.json")
        task["reviews"].append({"id": "calculation", "kind": "calculation",
                                "path": "reports/calculation.json", "required": True})
        write_json(self.task / "task.json", task)
        self.parent = execution.capture_candidate(self.store, self.task, "baseline")
        self.authority = self.root / "authority.json"
        write_json(self.authority, {"controller": "fixture-controller", "task_id": "neutral"})
        self.refs = [evidence_ref(self.authority, "authorized neutral author outcome branches")]
        self.script, self.public_script = self.root / "author.py", self.root / "public.py"
        self.script.write_text(AUTHOR_SCRIPT)
        self.public_script.write_text(PUBLIC_SCRIPT)
        self.work, self.runtime = self.root / "author-work", self.root / "author-runtime"
        self.work.mkdir()
        self.required = ["research.md", "candidate-decision.md", "handoff.md", "handoff.json", "issues.json"]
        self.template = self.root / "role-template.json"
        roles = {}
        for name in ("quality", "repair", "recheck", "public_rebuild", "recheck_public"):
            role = "repair" if name == "repair" else "quality"
            runner = {"role": role, "mode": "existing_isolated" if name == "public_rebuild" else "test_fixture",
                      "model": "OLD_MODEL", "reasoning_effort": "OLD_EFFORT", "timeout_seconds": 99,
                      "cwd": "{{work}}", "inputs": [{"path": "/OLD_TASK/private", "kind": "teacher"}],
                      "request_sections": [{"kind": "operation", "text": "OLD_BUSINESS_SCOPE"}],
                      "dependencies": [str(self.public_script)], "outputs": ["deliverables/quality-note.md"],
                      "output_trees": [], "argv": [str(Path(sys.executable).resolve()), str(self.public_script)]}
            roles[name] = {"runner": runner, "purpose": "OLD_BUSINESS_SCOPE", "scope": "whole_package",
                           "candidate_subpath": "revised-task/OLD_TASK"}
            if name == "public_rebuild":
                roles[name]["prepare"] = {"adapter": str(self.public_script), "sha256": sha256(self.public_script.read_bytes()),
                                          "checks": [], "launcher_python": str(Path(sys.executable).resolve()),
                                          "config": {"model": "OLD_MODEL", "reasoning_effort": "OLD_EFFORT"}}
        write_json(self.template, {"workflows": [{"definition": {"flow_id": "legacy-fixture", "roles": roles}}]})

    def prepare_source(self, mode="research_ended", delay=0):
        runner = {"intent_id": "author-source-intent", "policy_version": "fixture-v1", "role": "author",
                  "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture",
                  "timeout_seconds": 15, "cwd": str(self.work),
                  "argv": [str(Path(sys.executable).resolve()), str(self.script), str(self.task), mode,
                           str(delay), str(Path(__file__).resolve().parents[1])],
                  "dependencies": [str(self.script)], "inputs": [], "outputs": self.required,
                  "output_trees": [], "request_sections": [{"kind": "operation", "text": "Produce the actual five core outcome documents."}]}
        execution.enqueue_spec(self.store, {"work": {"work_id": "author-source", "task_id": "neutral",
            "candidate_id": "baseline", "role": "author", "policy_id": "fixture-v1",
            "intent_id": "author-source-intent", "purpose": "neutral preauthorized research", "evidence": self.refs,
            "dispatch_owner": "fixture-controller"},
            "runner": runner}, self.runtime)

    def run_source(self, mode="research_ended"):
        self.prepare_source(mode)
        self.attempt = execution.run_worker(self.store, "neutral-author")
        return self.attempt

    def register(self, **changes):
        binding = {key: self.attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
        definition = {"schema_version": 1, "kind": "author_quality_v1", "flow_id": "neutral-author-tail",
                      "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
                      "directory": str(self.root / "author-tail"), "authorization": self.refs[0],
                      "source_author_attempt": self.attempt["attempt_id"], "source_author_binding": binding,
                      "source_author_work": str(self.work), "role_template_ref": {"path": str(self.template),
                        "sha256": sha256(self.template.read_bytes()), "flow_id": "legacy-fixture"},
                      "business_item": {"task_id": "neutral", "intent_id": self.attempt["intent_id"], "direction": "Neutral current responsibility."},
                      "author_contract": {"required_outputs": self.required},
                      "next": {"candidate_complete": "Retain an independently qualified research asset; no target experiment.",
                               "research_ended": "Archive the actual bounded research end.",
                               "needs_business_decision": "Keep the exact substantive choice pending."}, **changes}
        return register_lifecycle(self.store, definition, self.refs)

    def test_running_source_waits_idempotently_without_archiving_or_capturing(self):
        from taskflow.execution_runner import run_attempt

        def fixture_process_identity(root, on_process=None, **kwargs):
            # Process execution is real; only OS startup metadata is adapted
            # because sandboxed macOS cannot execute ps for this neutral test.
            def observed(identity):
                on_process({**identity, "start_identity": "neutral actual subprocess fixture"})
            return run_attempt(root, on_process=observed, **kwargs)

        self.prepare_source(delay=1)
        with patch("taskflow.execution_runner.run_attempt", side_effect=fixture_process_identity), \
             ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(execution.run_worker, self.store, "neutral-author")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                attempts = self.store.dashboard()["attempts"]
                if attempts and attempts[0]["state"] == "running":
                    self.attempt = self.store.get_attempt(attempts[0]["attempt_id"])
                    break
                time.sleep(.01)
            else:
                self.fail("Neutral source did not reach its actual running window")
            record = self.register()
            with patch("taskflow.execution_author.archive_full_attempt", side_effect=AssertionError("inflight archive")), \
                 patch("taskflow.execution_author.capture_candidate", side_effect=AssertionError("inflight capture")), \
                 patch("taskflow.execution_author._public_prepare", side_effect=AssertionError("inflight prepare")):
                first = advance_author(self.store, record)
                again = advance_author(self.store, first)
            self.assertEqual(first["revision"], again["revision"])
            self.assertEqual("source_author", again["state"]["waiting"]["stage"])
            self.assertEqual(1, len(self.store.dashboard()["attempts"]))
            self.assertEqual("completed", future.result(timeout=5)["state"])

    def test_research_ended_receives_full_archive_without_new_work(self):
        self.run_source()
        record = advance_author(self.store, self.register())
        self.assertEqual("author_handoff", record["state"]["stage"])
        result = advance_author(self.store, record)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual("research_ended", result["state"]["result"]["outcome"])
        self.assertFalse(result["state"]["result"]["candidate_created"])
        self.assertFalse(result["state"]["result"]["new_model_dispatched"])
        self.assertTrue((Path(result["state"]["author_archive"]) / "supplemental/receipt.json").is_file())
        self.assertEqual(result, advance_author(self.store, result))
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_business_decision_retains_specific_exception_and_originals(self):
        self.run_source("needs_business_decision")
        record = advance_author(self.store, self.register())
        result = advance_author(self.store, record)
        self.assertEqual("exception", result["state"]["status"])
        self.assertEqual("author_handoff", result["state"]["exception"]["stage"])
        self.assertIn("Bounded neutral", result["state"]["exception"]["reason"])
        self.assertTrue(result["state"]["exception"]["source_evidence"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_source_binding_and_core_output_contract_reject_wrong_original(self):
        self.run_source()
        binding = {key: self.attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
        with self.assertRaises(TaskflowError):
            self.register(source_author_binding={**binding, "intent_id": "other-intent"})
        record = self.register()
        record = copy.deepcopy(record)
        record["definition"]["source_author_binding"]["intent_id"] = "other-intent"
        with self.assertRaisesRegex(TaskflowError, "different original"):
            advance_author(self.store, record)
        self.assertFalse((Path(record["definition"]["directory"]) / "archives").exists())

    def test_missing_required_output_contract_cannot_broaden_source(self):
        self.run_source()
        record = self.register(author_contract={"required_outputs": ["handoff.json"]})
        with self.assertRaises(TaskflowError):
            advance_author(self.store, record)

    def test_handoff_schema_and_role_flags_are_not_truthy_coercions(self):
        for mode in ("bad-schema", "bad-quality", "bad-score", "bad-decision"):
            case = AuthorSuccessorTests("test_research_ended_receives_full_archive_without_new_work")
            case.setUp()
            try:
                case.run_source(mode)
                record = advance_author(case.store, case.register())
                with self.subTest(mode=mode), self.assertRaises(TaskflowError):
                    advance_author(case.store, record)
                self.assertEqual(1, len(case.store.dashboard()["attempts"]))
            finally:
                case.doCleanups()

    def test_failed_original_is_never_redrawn(self):
        self.run_source("no-turn")
        record = self.register()
        with self.assertRaisesRegex(TaskflowError, "never redraw"):
            advance_author(self.store, record)
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles(self):
        self.run_source("candidate_complete")
        record = advance_author(self.store, self.register())
        self.addCleanup(shutil.rmtree, _public_root(record), True)
        record = advance_author(self.store, record)
        self.assertEqual("public_prepare", record["state"]["stage"])
        candidate = self.store.get_candidate(record["state"]["candidate_id"])
        self.assertEqual(self.attempt["attempt_id"], candidate["author_attempt"])
        self.assertEqual("baseline", candidate["parent_id"])
        self.assertEqual([], candidate["external_author_sids"])
        self.assertTrue(self.store.get_attempt(self.attempt["attempt_id"])["supplemental_delivery"])
        inner = self.root / "stub-public"
        (inner / "work").mkdir(parents=True)

        def enqueue_fixture(store, spec, runtime, **kwargs):
            spec = copy.deepcopy(spec)
            config = spec["runner"]
            config.update(mode="test_fixture", cwd=str(inner / "work"),
                          argv=[str(Path(sys.executable).resolve()), str(self.public_script)],
                          dependencies=[str(self.public_script)])
            config.pop("isolated", None)
            return execution.enqueue_spec(store, spec, runtime, **kwargs)

        with patch("taskflow.execution_author._public_prepare", return_value={"runtime": str(inner), "flavor": "quality"}) as prepare, \
             patch("taskflow.execution_author.enqueue_spec", side_effect=enqueue_fixture):
            record = advance_author(self.store, record)
        self.assertEqual(1, prepare.call_count)
        self.assertEqual("public_result", record["state"]["stage"])
        public = execution.run_worker(self.store, "neutral-public")
        self.assertEqual("completed", public["state"])
        self.assertNotEqual(self.attempt["cli_sid"], public["cli_sid"])
        record = advance_author(self.store, record)
        self.assertEqual("child_quality", record["state"]["stage"])
        with patch("taskflow.execution_author.advance_workflow", side_effect=lambda store, identity: store.get_flow(identity)):
            record = advance_author(self.store, record)
        child = self.store.get_flow(record["state"]["child_flow_id"])["definition"]
        self.assertEqual(public["attempt_id"], child["initial"]["predecessor_attempt"])
        self.assertEqual({key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")},
                         child["initial"]["predecessor_binding"])
        self.assertEqual(candidate["candidate_id"], child["initial"]["candidate_id"])
        serialized = json.dumps(child)
        for old in ("OLD_BUSINESS_SCOPE", "/OLD_TASK/private", "OLD_MODEL", "OLD_EFFORT", "revised-task/OLD_TASK"):
            self.assertNotIn(old, serialized)
        self.assertEqual("revised-task/neutral", child["roles"]["repair"]["candidate_subpath"])
        self.assertEqual("whole_package", child["roles"]["quality"]["scope"])
        self.assertEqual("affected", child["roles"]["recheck"]["scope"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def write_placeholders(self, style="flags"):
        declaration = read_json(self.task / "task.json")
        for spec in declaration["reviews"]:
            report = {"schema_version": 1, "review_id": spec["id"], "kind": spec["kind"],
                      "status": "inconclusive"}
            if style == "flags":
                report.update(independent_quality_passed=False, new_formal_score=None,
                              authority="author only; independent review pending",
                              local_checks="Scoped author evidence, not an independent review decision")
            else:
                report.update(scope="whole_current_candidate", content_digest=None, input_digest=None,
                              evaluation_digest=None, independent_signer=None, signed_at_utc=None,
                              formal_score=None, limitations=["Author construction only; independent review pending."])
            write_json(self.task / spec["path"], report)

    def test_explicit_unsigned_author_placeholders_remain_unreviewed_without_rewriting_reports(self):
        for style in ("flags", "null-signature"):
            with self.subTest(style=style):
                self.write_placeholders(style)
                declaration = read_json(self.task / "task.json")
                before = {row["path"]: (self.task / row["path"]).read_bytes() for row in declaration["reviews"]}
                actual = _candidate_structure(self.task, "neutral")
                self.assertEqual([], actual["errors"])
                self.assertEqual({"stale"}, {row["state"] for row in actual["reviews"]})
                self.assertEqual(before, {name: (self.task / name).read_bytes() for name in before})

    def test_unsigned_placeholders_reject_false_claims_stale_digests_and_wrong_identity(self):
        changes = [{"schema_version": True}, {"status": "passed"}, {"status": "failed"},
                   {"review_id": "other"}, {"kind": "other"}, {"task_id": "other"},
                   {"independent_quality_passed": True}, {"independent_quality_passed": "false"},
                   {"new_formal_score": 0}, {"formal_score": 0}, {"independent_signer": "someone"},
                   {"signed_at_utc": "2026-11-02T00:00:00Z"}, {"content_digest": "0" * 64},
                   {"native_signoff": {"dimensions": "passed"}}]
        for change in changes:
            self.write_placeholders()
            report = read_json(self.task / "reports/business.json")
            report.update(change)
            write_json(self.task / "reports/business.json", report)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                _candidate_structure(self.task, "neutral")
        for style, key in (("flags", "new_formal_score"), ("null-signature", "independent_signer")):
            self.write_placeholders(style)
            report = read_json(self.task / "reports/business.json")
            del report[key]
            write_json(self.task / "reports/business.json", report)
            with self.subTest(style=style), self.assertRaises(TaskflowError):
                _candidate_structure(self.task, "neutral")

    def test_technical_same_byte_candidate_alias_keeps_original_managed_author(self):
        self.run_source("candidate_complete")
        record = advance_author(self.store, self.register())
        source = Path(record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        alias = self.root / "technical-same-byte-alias"
        shutil.copytree(source, alias)
        record["state"]["technical_candidate_root"] = str(alias)
        record = advance_author(self.store, record)
        self.assertEqual(str(alias), record["state"]["candidate_root"])
        candidate = self.store.get_candidate(record["state"]["candidate_id"])
        self.assertEqual(self.attempt["attempt_id"], candidate["author_attempt"])
        self.assertEqual("baseline", candidate["parent_id"])
        self.assertTrue(all(Path(row["path"]).is_relative_to(alias) for row in candidate["files"]))

    def test_external_public_root_runs_actual_zero_model_prepare_without_touching_old_failure(self):
        from tests import test_isolated_quality as isolated_fixture

        fixture = isolated_fixture.IsolatedQualityTests(
            "test_prepare_copies_only_bound_public_bytes_and_has_no_inference")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        record = {"flow_id": "neutral-public", "definition_digest": sha256(str(self.root).encode())}
        root = _public_root(record)
        self.addCleanup(shutil.rmtree, root, True)
        self.assertEqual(root, _public_root(dict(record)))
        self.assertNotEqual(root, _public_root({**record, "definition_digest": "0" * 64}))
        self.assertFalse(root.is_relative_to(fixture.repository))
        self.assertFalse(fixture.repository.is_relative_to(root))
        root.mkdir()
        old_failure = fixture.repository / "lifecycle/public"
        old_failure.mkdir(parents=True)
        sentinel = old_failure / "original-failed-preparation.json"
        sentinel.write_text('{"error":"Runtime must be outside the repository"}')
        original = sentinel.read_bytes()
        adapter = Path(__file__).resolve().parents[1] / "tools/isolated_quality.py"
        actual = check_task(fixture.task, write_report=False)
        context = {"candidate_root": str(fixture.task), "runtime": str(root / "runtime"),
                   "input_digest": actual["input_digest"], "private_paths": []}
        role = {"runner": {"model": "neutral-model", "reasoning_effort": "high", "timeout_seconds": 99},
                "prepare": {"adapter": str(adapter), "sha256": sha256(adapter.read_bytes()),
                            "launcher_python": str(Path(sys.executable).resolve()), "config": fixture.config, "checks": []}}

        def zero_model_cli(argv, **kwargs):
            if argv[1:2] == ["sandbox"]:
                return fixture.probe_ok(argv, **kwargs)
            return fixture.cli_evidence(argv, **kwargs)

        with patch("subprocess.run", side_effect=zero_model_cli), \
             patch("subprocess.Popen", side_effect=AssertionError("no model launch during public preparation")), \
             patch.object(Path, "home", return_value=fixture.base / "home"), \
             patch.dict(os.environ, {}, clear=True):
            prepared = _public_prepare(role, context)
            repeated = _public_prepare(role, context)
        self.assertEqual(prepared, repeated)
        inner = Path(prepared["runtime"])
        self.assertTrue(inner.is_relative_to(root))
        self.assertEqual(str(inner), str(root / "runtime.public"))
        self.assertFalse((inner / "work/draft/evaluation").exists())
        self.assertTrue((inner / "work/draft/input/prompt.md").is_file())
        self.assertTrue((inner / "ready.json").is_file())
        self.assertEqual(original, sentinel.read_bytes())


if __name__ == "__main__":
    unittest.main()
