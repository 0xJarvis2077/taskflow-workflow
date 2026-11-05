"""One bounded fresh quality role after an actual local transport-failure fixture."""

import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow import execution, execution_author, execution_runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import _identity, validate_technical_quality_successor
from taskflow.execution_experiment import _author_source, _packet
from taskflow.execution_lifecycle import advance_lifecycle, register_lifecycle, run_lifecycle
from taskflow.execution_runner import RunnerError, archive_full_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as authors
from tests import test_execution_experiment_author as experiments
from tests import test_execution_flow as flows
from tools.execution_simulation import fixture_policy


class QualityTechnicalSuccessorTests(unittest.TestCase):
    def setUp(self):
        case = authors.AuthorSuccessorTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        script = case.root / "quality-role.py"
        script.write_text('''import json,pathlib,sys,uuid
manifest=json.loads(pathlib.Path(sys.argv[1]).read_text());sys.argv.pop(1)
if manifest['model']['requested']=='fixture-cli':
 pathlib.Path('public-responsibility-first.json').write_text('OLD_FAILED_CHECKPOINT_SENTINEL')
 print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
 message='stream disconnected before completion: Transport error: network error: local fixture'
 print(json.dumps({'type':'error','message':message}),flush=True)
 print(json.dumps({'type':'turn.failed','error':{'message':message}}),flush=True)
 sys.exit(1)
assert manifest['resume_sid'] is None
assert not pathlib.Path('public-responsibility-first.json').exists()
assert all('OLD_FAILED_CHECKPOINT_SENTINEL' not in p.read_text() for p in pathlib.Path(sys.argv[sys.argv.index('--inputs')+1]).rglob('*.json'))
''' + flows.ROLE_FIXTURE)
        template = read_json(case.template)
        runner = template["workflows"][0]["definition"]["roles"]["quality"]["runner"]
        runner.update(argv=[str(Path(sys.executable).resolve()), str(script), "{{runtime}}/manifest.json",
                      "--context", "{{runtime}}/inputs/execution/current.json", "--inputs", "{{runtime}}/inputs",
                      "--action", "quality", "--scope", "whole_package", "--outcome", "accepted", "--delay", "0"],
                      dependencies=[str(script)], outputs=["intake.md", "issues.json", "adoption-readiness.json"])
        write_json(case.template, template)
        case.run_source("candidate_complete")
        original_roles = execution_author._roles

        def fixture_roles(store, definition):
            roles = original_roles(store, definition)
            for role in roles.values():
                for item in role.get("predecessor_inputs", []):
                    if item.get("target") == "actual-public-trace.jsonl":
                        item["path"] = "{{public_archive}}/trace.jsonl"
            return roles

        roles_double = patch("taskflow.execution_author._roles", side_effect=fixture_roles)
        roles_double.start()
        self.addCleanup(roles_double.stop)
        source = case.register()
        for _ in range(2):
            source = advance_lifecycle(case.store, source["flow_id"])
        inner = case.root / "public-inner"
        (inner / "work").mkdir(parents=True)
        (inner / "scratch").mkdir()

        def public_enqueue(store, spec, runtime, **kwargs):
            prepared = copy.deepcopy(spec)
            prepared["runner"].update(mode="test_fixture", cwd=str(inner / "work"),
                                      argv=[str(Path(sys.executable).resolve()), str(case.public_script)])
            prepared["runner"].pop("isolated", None)
            return execution.enqueue_spec(store, prepared, runtime, **kwargs)

        with patch("taskflow.execution_author._public_prepare", return_value={"runtime":str(inner), "flavor":"quality"}), \
             patch("taskflow.execution_author.enqueue_spec", side_effect=public_enqueue):
            source = advance_lifecycle(case.store, source["flow_id"])
        self.addCleanup(__import__('shutil').rmtree, Path(case.store.get_work(source['state']['current_work_id'])['runtime_path']).parent, True)
        execution.run_worker(case.store, "public-fixture", False, [source["state"]["current_work_id"]])
        original_roots = execution_runner._full_source_roots

        def fixture_public_roots(manifest):
            # Explicit local adapter double; archive the actual fixture trees in
            # the existing isolated public layout without a native model call.
            if manifest["cwd"] == str(inner / "work"):
                return {"inner":inner}, {"inner":{"source":"inner", "relative":".", "path":str(inner)}}
            return original_roots(manifest)

        archive_double = patch("taskflow.execution_runner._full_source_roots", side_effect=fixture_public_roots)
        archive_double.start()
        self.addCleanup(archive_double.stop)
        source = advance_lifecycle(case.store, source["flow_id"])
        source = advance_lifecycle(case.store, source["flow_id"])
        for _ in range(3):
            if source["state"].get("current_work_id"):
                break
            source = advance_lifecycle(case.store, source["flow_id"])
        self.assertIsInstance(source["state"]["current_work_id"], str, source)
        failed = execution.run_worker(case.store, "failed-quality-fixture", False, [source["state"]["current_work_id"]])
        self.assertEqual("failed", failed["state"])
        for _ in range(3):
            source = advance_lifecycle(case.store, source["flow_id"])
        self.source = source
        self.failed = failed
        self.child = case.store.get_flow(source["state"]["child_flow_id"])
        archive_full_attempt(Path(failed["runtime_path"]), Path(self.child["definition"]["directory"]) / "archives" / failed["attempt_id"])
        policy = fixture_policy()
        for setting in policy["roles"].values():
            setting["model"] = "future-fixture-cli"
        case.store.register_policy("fixture-future", policy)
        self.definition = copy.deepcopy(source["definition"])
        self.definition.update(flow_id=_identity("neutral", "quality-technical-" + failed["attempt_id"]),
                               policy_id="fixture-future", directory=str(case.root / "technical-quality"))
        technical = {"source_parent_flow_id":source["flow_id"], "source_parent_definition_digest":source["definition_digest"],
                     "source_child_flow_id":self.child["flow_id"], "source_child_definition_digest":self.child["definition_digest"],
                     "failed_attempt_id":failed["attempt_id"]}
        candidate = case.store.get_candidate(source["state"]["candidate_id"])
        binding = {key:candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        decision = case.root / "decision.json"
        write_json(decision, {"maximum_new_quality_attempts":1, "predecessor_attempt":failed["attempt_id"],
                   "predecessor_actual_sid":failed["cli_sid"], "candidate_binding":binding,
                   "requested_model":"future-fixture-cli", "reasoning_effort":"fixture"})
        selection = case.root / "model-choice.json"
        write_json(selection, {"nature":"explicit fixture model choice, no business model authority"})
        self.authority_path = case.root / "technical-authority.json"
        self.authority = {"action":"prepare_failed_initial_whole_quality_successor_once", "controller":"fixture-controller",
                          "task_id":"neutral", "maximum_new_quality_attempts":1, "technical_source":technical,
                          "candidate_binding":binding, "original_author_attempt":case.attempt["attempt_id"],
                          "original_public_attempt":source["state"]["public_attempt_id"], "target_policy_id":"fixture-future",
                          "requested_model":"future-fixture-cli", "reasoning_effort":"fixture",
                          "decision":evidence_ref(decision, "bounded neutral fixture decision"),
                          "model_selection":evidence_ref(selection, "neutral fixture selection")}
        self.refresh_authority()
        self.protected = {name:(Path(failed["runtime_path"]) / name).read_bytes()
                          for name in ("manifest.json", "trace.jsonl", "collection.json", "collection-lock.json")}

    def refresh_authority(self):
        write_json(self.authority_path, self.authority)
        self.definition["technical_quality_successor"] = {
            **self.authority["technical_source"], "authority":evidence_ref(self.authority_path, "one bounded fixture successor")}

    def register(self):
        reference = self.definition["technical_quality_successor"]["authority"]
        return register_lifecycle(self.case.store, self.definition, [*self.case.refs, reference])

    def test_historical_backend_reuses_actual_author_public_and_one_fresh_review_then_source_binds(self):
        before = len(self.case.store.dashboard()["attempts"])
        first = self.register()
        self.assertEqual(first, self.register())
        self.assertEqual(before, len(self.case.store.dashboard()["attempts"]))
        result = run_lifecycle(self.case.store, first["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual("accepted", result["state"]["status"], result)
        self.assertEqual(before + 1, len(self.case.store.dashboard()["attempts"]))
        review = self.case.store.get_attempt(self.case.store.quality_gate(result["state"]["result"]["candidate_id"])["attempt_id"])
        self.assertNotIn(review["cli_sid"], result["state"]["technical_quality_binding"]["excluded_sids"])
        original = copy.deepcopy(self.source)
        self.assertEqual(original, self.case.store.get_flow(original["flow_id"]))
        for name, raw in self.protected.items():
            self.assertEqual(raw, (Path(self.failed["runtime_path"]) / name).read_bytes())
        helper = experiments.AuthorExperimentTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        experiment = helper.definition(self.case, result)
        experiment["policy_id"] = "fixture-future"
        packet = _packet({"definition":experiment})
        bound = _author_source(self.case.store, {"definition":experiment, "state":{}}, packet, require_candidate=True)
        self.assertEqual(self.case.attempt["attempt_id"], bound["binding"]["source_author_attempt"])
        with self.assertRaises(TaskflowError):
            _author_source(self.case.store, {"definition":experiment, "state":{}},
                           {**packet, "source_author_attempt":review["attempt_id"]}, require_candidate=True)
        self.assertEqual(result, run_lifecycle(self.case.store, first["flow_id"], allow_model=True, poll_seconds=.001))
        self.assertEqual(before + 1, len(self.case.store.dashboard()["attempts"]))

    def test_wrong_identity_budget_policy_and_partial_signature_are_rejected(self):
        for field, value in (("flow_id", "another-id"), ("policy_id", "fixture-v1")):
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_technical_quality_successor(self.case.store, {**self.definition, field:value})
        self.authority["maximum_new_quality_attempts"] = 2
        self.refresh_authority()
        with self.assertRaises(TaskflowError):
            self.register()
        self.authority["maximum_new_quality_attempts"] = 1
        self.refresh_authority()
        partial = Path(self.failed["runtime_path"]) / "artifacts/intake.md"
        partial.parent.mkdir(exist_ok=True)
        partial.write_text("Partial output is not a missing review.")
        with self.assertRaises((TaskflowError, RunnerError)):
            self.register()
        partial.unlink()
        self.assertEqual(3, len(self.case.store.dashboard()["attempts"]))

    def test_changed_policy_infrastructure_model_and_boolean_budget_are_rejected(self):
        original_policy = self.case.store.get_policy
        for field, value in (("model", "other-fixture"), ("timeout_seconds", 101)):
            def policy(identity):
                result = copy.deepcopy(original_policy(identity))
                if identity == "fixture-future":
                    result["config"]["roles"]["quality"][field] = value
                return result
            with self.subTest(field=field), patch.object(self.case.store, "get_policy", side_effect=policy):
                with self.assertRaises(TaskflowError):
                    self.register()
        for value in (True, 2):
            self.authority["maximum_new_quality_attempts"] = value
            self.refresh_authority()
            with self.subTest(budget=value), self.assertRaises(TaskflowError):
                self.register()

    def test_active_uncertain_or_recursive_source_is_rejected(self):
        original_get = self.case.store.get_attempt
        for state in ("running", "uncertain", "completed"):
            def actor(identity):
                value = original_get(identity)
                return {**value, "state":state} if identity == self.failed["attempt_id"] else value
            with self.subTest(state=state), patch.object(self.case.store, "get_attempt", side_effect=actor), self.assertRaises(TaskflowError):
                self.register()
        original_flow = self.case.store.get_flow
        def recursive(identity):
            value = original_flow(identity)
            if identity == self.source["flow_id"]:
                value = copy.deepcopy(value)
                value["definition"]["technical_quality_successor"] = {"previous":True}
            return value
        with patch.object(self.case.store, "get_flow", side_effect=recursive), self.assertRaises(TaskflowError):
            self.register()


if __name__ == "__main__":
    unittest.main()
