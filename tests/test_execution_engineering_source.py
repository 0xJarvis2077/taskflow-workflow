"""Engineering roots use actual public originals and fresh native quality sources."""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_engineering_source import (
    KIND, BINDING, PUBLIC_IDENTITY, engineering_packet, engineering_source, validate_frozen_engineering_source,
)
from taskflow.execution_experiment import _packet, _source
from taskflow.execution_formal import native_quality_handoff, validate_normative_certification, projected_quality_reports
from taskflow.execution_runner import archive_full_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_normative_certification as normative_fixture
from tools.execution_simulation import fixture_policy


class EngineeringSourceTests(unittest.TestCase):
    def setUp(self):
        case = normative_fixture.NormativeCertificationTests()
        self.source = case.fixture()
        self.normative_case = case
        self.addCleanup(case.doCleanups)
        s = self.source
        ledger = ExecutionStore(s.root / "public-ledger.sqlite3")
        self.ledger = ledger
        ledger.register_policy("fixture-v1", fixture_policy())
        execution.capture_candidate(ledger, s.task, s.candidate["candidate_id"])
        script = s.root / "public.py"
        script.write_text("import json,pathlib\npathlib.Path('public.txt').write_text('Neutral public reconstruction, zero models.')\n"
            "print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000088'}))\n"
            "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral public original.'}}))\n"
            "print(json.dumps({'type':'turn.completed'}))\n")
        work, runtime = s.root / "public-work", s.root / "public-runtime"
        work.mkdir()
        refs = [evidence_ref(script, "neutral public process fixture")]
        execution.enqueue_spec(ledger, {"work": {"work_id": "public-source", "task_id": "neutral",
            "candidate_id": s.candidate["candidate_id"], "role": "quality", "policy_id": "fixture-v1",
            "intent_id": "public-intent", "purpose": "Neutral actual fixture public predecessor", "evidence": refs},
            "runner": {"intent_id": "public-intent", "role": "quality", "policy_version": "fixture-v1",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), str(script)],
                "dependencies": [str(script)], "inputs": [], "request_sections": [], "outputs": ["public.txt"]}}, runtime)
        self.public = execution.run_worker(ledger, "public-fixture")
        archive = s.root / "engineering-quality/archives" / self.public["attempt_id"]
        archive_full_attempt(runtime, archive)
        s.attempts[self.public["attempt_id"]] = self.public
        s.store.get_work = ledger.get_work
        s.store.get_policy = ledger.get_policy
        s.store.get_review = lambda key: s.current
        s.child["definition"].update(flow_id=s.child_id, policy_id="fixture-v1", directory=str(s.root / "engineering-quality"),
            initial={"candidate_id": s.candidate["candidate_id"], "candidate_root": str(s.task),
                     "predecessor_attempt": self.public["attempt_id"],
                     "predecessor_binding": {key: self.public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")},
                     "expected_digests": {key: s.candidate[key] for key in ("content_digest", "input_digest", "evaluation_digest")}},
            roles={"quality": {"scope": "whole_package", "runner": {"role": "quality", "mode": "test_fixture"}}})
        s.child["state"]["predecessor_archive"] = str(archive)
        origin = s.root / "engineering-origin.json"
        write_json(origin, {"schema_version": 1, "kind": "engineering_candidate_origin_v1",
            "candidate_created_by": "engineering", "candidate_binding": {key: s.candidate[key] for key in BINDING},
            "author_attempt": None, "parent_id": None, "reason": "Actual neutral engineering root.", "evidence": refs})
        self.item = {"task_id": "neutral", "source_kind": KIND, "source_quality_flow": s.child_id,
            "initial_candidate_binding": {key: s.candidate[key] for key in BINDING},
            "public_predecessor": {key: self.public[key] for key in PUBLIC_IDENTITY},
            "engineering_origin": evidence_ref(origin, "explicit honest engineering origin"),
            "first_experiment_business_unknown": "Neutral substantive question", "version_id": "neutral-v1",
            "first_run_id": "neutral-run", "first_evaluation_id": "neutral-score",
            "conditional_one_confirmation_run_id": "neutral-confirm", "conditional_one_confirmation_evaluation_id": "neutral-confirm-score"}
        write_json(s.authority, {"schema_version": 1, "controller": "neutral-controller", "items": [self.item],
            "preauthorized_normal_chain": "Normal exact candidate experiment", "prerequisites": ["Independent quality required"],
            "confirmation": {"maximum_additional_runs_per_first": 1, "only_if": "only when total <= 1 / 10"}})
        self.authorization = evidence_ref(s.authority, "explicit independent engineering-root experiment authority")
        self.definition = {"kind": "experiment_v1", "flow_id": "engineering-experiment", "source_kind": KIND,
            "source_flow_id": s.child_id, "task_id": "neutral", "owner": "neutral-controller", "policy_id": "fixture-v1",
            "authorization": self.authorization, "business_packet_ref": self.authorization,
            "business_packet_item": {"task_id": "neutral", "source_quality_flow": s.child_id},
            "technical": {}}
        self.native = lambda: native_quality_handoff(s.store, candidate_id=s.candidate["candidate_id"],
                                authorization=self.authorization, source_flow_id=s.child_id)

    def test_source_projection_and_offline_score_certification_keep_no_author_alias(self):
        s = self.source
        bound = engineering_source(s.store, self.definition, self.item, require_candidate=True)
        self.assertIsNone(bound["initial"]["author_attempt"])
        self.assertIsNone(bound["initial"]["parent_id"])
        proof = bound["binding"]["engineering_source"]
        validate_frozen_engineering_source(proof)
        native = self.native()
        self.assertEqual(proof, native["certification"]["engineering_source"])
        cert = {"schema_version": 1, "kind": "native_quality_handoff",
                "candidate_binding": {key: s.candidate[key] for key in BINDING},
                "source_quality_flow_id": s.child_id, "authorization": self.authorization,
                "native_quality_handoff": native}
        self.assertEqual(native["normative_sources"], validate_normative_certification(candidate=s.candidate, certification=cert))
        self.assertEqual(native["normative_sources"], validate_normative_certification(candidate=s.candidate, certification=cert, store=s.store))
        projected = projected_quality_reports(s.store, candidate_id=s.candidate["candidate_id"],
            source_refs=native["source_refs"], protocol_map=native["protocol_map"], destination=s.root / "engineering-projection")
        self.assertEqual({"business", "artifact", "calculation"}, set(projected))
        record = {"definition": self.definition, "state": {"history": []}}
        packet = _packet(record)
        with patch("taskflow.execution_experiment._save", side_effect=lambda store, record, state, *args: state):
            result = _source(s.store, record, packet)
        self.assertEqual("release", result["stage"])
        self.assertIn("engineering_source_binding", result)
        self.assertNotIn("author_source_binding", result)
        self.assertIn(self.public["cli_sid"], result["excluded_sids"])
        self.assertEqual(["draft/evaluation/answer.md", "draft/evaluation/rubric.md"], native["scoring_source_allowlist"])

    def test_private_revision_uses_explicit_old_public_archive_and_new_complete_quality(self):
        import shutil

        s = self.source
        old = s.candidate
        new_root = s.root / "new-private-root"
        shutil.copytree(s.task, new_root)
        (new_root / "draft/evaluation/answer.md").write_text("New complete private support for independent whole review.\n")
        current = execution.capture_candidate(self.ledger, new_root, "engineering-private-v2")
        self.assertEqual(old["input_digest"], current["input_digest"])
        self.assertNotEqual(old["content_digest"], current["content_digest"])
        s.candidates[current["candidate_id"]] = current
        s.candidate, s.task = current, new_root
        s.child["definition"]["initial"].update(candidate_id=current["candidate_id"], candidate_root=str(new_root),
            expected_digests={key: current[key] for key in ("input_digest", "content_digest", "evaluation_digest")})
        reuse = s.root / "explicit-public-reuse.json"
        write_json(reuse, {"schema_version": 1, "kind": "identical_public_predecessor_reuse_v1",
            "controller": "neutral-controller", "task_id": "neutral", "quality_transfer": False,
            "predecessor": {key: self.public[key] for key in PUBLIC_IDENTITY},
            "source_candidate_binding": {key: old[key] for key in BINDING},
            "target_candidate_binding": {key: current[key] for key in BINDING},
            "reason": "Explicit old public reconstruction for unchanged public bytes only.",
            "evidence": [evidence_ref(new_root / "task.json", "new private root declaration")]})
        s.child["definition"]["initial"]["public_predecessor_reuse"] = evidence_ref(reuse, "actual explicit same-public reuse")
        s.child["state"].update(candidate_id=current["candidate_id"], candidate_root=str(new_root))
        material = s.source(current, "new-quality", coverage="whole_package", outcome="accepted")
        self.normative_case.complete_source(s, material)
        s.current = {"review_id": "new-accepted", "attempt_id": "new-quality", "whole_package": True}
        s.bind_receipt("new-accepted", "new-quality")
        origin = s.root / "new-engineering-origin.json"
        write_json(origin, {"schema_version": 1, "kind": "engineering_candidate_origin_v1",
            "candidate_created_by": "engineering", "candidate_binding": {key: current[key] for key in BINDING},
            "author_attempt": None, "parent_id": None, "reason": "Honest private reference revision.",
            "evidence": [evidence_ref(new_root / "task.json", "actual new root")]})
        self.item.update(initial_candidate_binding={key: current[key] for key in BINDING},
                         engineering_origin=evidence_ref(origin, "new actual engineering origin"))
        packet = read_json(s.authority);packet["items"] = [self.item];write_json(s.authority, packet)
        self.authorization = evidence_ref(s.authority, "new private root actual formal authority")
        self.definition.update(authorization=self.authorization, business_packet_ref=self.authorization)
        bound = engineering_source(s.store, self.definition, self.item, require_candidate=True)
        self.assertEqual(old["candidate_id"], bound["binding"]["engineering_source"]["public_source_binding"]["candidate_id"])
        self.assertEqual(current["candidate_id"], bound["initial"]["candidate_id"])
        native = self.native()
        self.assertEqual("new-quality", native["certification"]["fresh"]["attempt_id"])
        self.assertEqual(current["candidate_id"], native["certification"]["engineering_source"]["initial_binding"]["candidate_id"])
        self.assertEqual(self.public, self.ledger.get_attempt(self.public["attempt_id"]))

    def test_factory_registers_new_source_selector_without_starting_consumer(self):
        from taskflow.execution_experiment_factory import start_experiments_from_packet

        s = self.source
        config = s.root / "engineering-factory.json"
        write_json(config, {"schema_version": 1, "policy_id": "fixture-v1", "repository": str(s.root),
            "lifecycle_root": str(s.root / "lifecycles"), "formal_runtime_root": str(s.root / "formal"),
            "publication_root": str(s.root / "publications"), "technical": {"real_repository": str(s.root),
                "formal_config_base": {}, "review_runner": {}}})
        captured = []
        def register(store, definition, evidence):
            _packet({"definition": definition})
            captured.append(definition)
            return {"flow_id": definition["flow_id"], "revision": 1}
        with patch("taskflow.execution_experiment_factory.register_lifecycle", side_effect=register), \
             patch("taskflow.execution_experiment_factory.launch_lifecycle_consumer", side_effect=AssertionError("no model")):
            result = start_experiments_from_packet(s.store, packet_ref=self.authorization,
                factory_config_ref=evidence_ref(config, "normal neutral engineering factory"), allow_model=False)
        self.assertEqual(0, result["consumers_started"])
        self.assertFalse(result["attention"])
        self.assertEqual(s.child_id, captured[0]["source_flow_id"])
        self.assertEqual(KIND, captured[0]["source_kind"])
        self.assertNotIn("source_author_flow_id", captured[0])

    def test_score_preparation_transports_new_source_field_and_exact_normative_cert(self):
        from taskflow.execution_experiment import _advance

        s = self.source
        s.store.list_flows = lambda: []
        definition = copy.deepcopy(self.definition)
        definition.update(directory=str(s.root / "experiment"))
        definition["technical"].update(formal_runtime_root=str(s.root / "formal"), real_repository=str(s.root), review_runner={})
        record = {"flow_id": definition["flow_id"], "definition": definition, "state": {
            "stage": "score_prepare", "candidate_id": s.candidate["candidate_id"], "run_index": 0,
            "facts_attempt": {"work_id": "facts-work", "runtime_path": str(s.root / "facts")},
            "imported_run": {"run_path": str(s.root / "run"), "first_review_index": "index", "first_review_inputs": "inputs"},
            "excluded_sids": [self.public["cli_sid"]], "facts_prepared": {"review_binding": {}},
            "release": {"snapshot": {"path": str(s.task)}}}}
        captured = []
        def prepare(**kwargs):
            captured.append(kwargs["config"])
            return {"prepared": True}
        with patch("taskflow.execution_experiment.prepare_score_runner", side_effect=prepare), \
             patch("taskflow.execution_experiment._score_request_sections", return_value=[]), \
             patch("taskflow.execution_experiment._save", side_effect=lambda store, record, state, *args: state):
            result = _advance(s.store, record)
        self.assertEqual("score_wait", result["stage"])
        cert = captured[0]["normative_certification"]
        self.assertEqual(s.child_id, cert["source_quality_flow_id"])
        self.assertNotIn("source_author_flow_id", cert)
        self.assertNotIn("parent_flow_id", cert["native_quality_handoff"]["certification"])
        self.assertEqual(cert["native_quality_handoff"]["normative_sources"],
                         validate_normative_certification(candidate=s.candidate, certification=cert))

    def test_packet_never_borrows_author_identity_or_another_source(self):
        original = read_json(self.source.authority)
        for change in ({"source_author_attempt": "invented"}, {"source_quality_flow": "foreign"},
                       {"source_kind": "author_quality_v1"}, {"first_experiment_business_unknown": ""}):
            packet = {**original, "items": [{**self.item, **change}]}
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                engineering_packet(self.definition, packet)

    def test_root_binding_public_identity_and_real_origin_cannot_be_substituted(self):
        for change in ({"initial_candidate_binding": {}}, {"public_predecessor": {}}, {"source_quality_flow": "foreign"}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                engineering_source(self.source.store, self.definition, {**self.item, **change}, require_candidate=True)
        origin = Path(self.item["engineering_origin"]["path"])
        value = read_json(origin)
        value["author_attempt"] = "invented"
        write_json(origin, value)
        item = {**self.item, "engineering_origin": evidence_ref(origin, "wrong engineering origin")}
        with self.assertRaisesRegex(TaskflowError, "invented author/parent"):
            engineering_source(self.source.store, self.definition, item)

    def test_active_quality_can_wait_but_exception_or_unaccepted_native_source_cannot_pass(self):
        self.source.child["state"]["status"] = "active"
        bound = engineering_source(self.source.store, self.definition, self.item)
        self.assertIsNone(bound["binding"]["engineering_source"])
        with self.assertRaisesRegex(TaskflowError, "not been independently accepted"):
            self.native()
        self.source.child["state"]["status"] = "exception"
        with self.assertRaisesRegex(TaskflowError, "unresolved exception"):
            engineering_source(self.source.store, self.definition, self.item)

    def test_frozen_proof_cannot_replace_public_collection_or_initial_binding(self):
        bound = engineering_source(self.source.store, self.definition, self.item, require_candidate=True)
        proof = bound["binding"]["engineering_source"]
        for key, value in (("kind", "author_quality_v1"), ("public_collection", proof["quality_receipt"]),
                           ("initial_binding", {**proof["initial_binding"], "input_digest": "f"*64})):
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                validate_frozen_engineering_source({**proof, key: value})

    def test_real_public_mode_requires_the_original_confirmed_role_slot(self):
        from taskflow.execution_runner import _read

        manifest = _read(Path(self.public["runtime_path"]) / "manifest.json")
        with patch("taskflow.execution_flow._predecessor", return_value=(self.public, {**manifest, "mode": "native_role"})), self.assertRaisesRegex(TaskflowError, "confirmed public role"):
            engineering_source(self.source.store, self.definition, self.item)

    def test_formal_authority_keeps_baseline_model_timeout_and_exact_new_selector(self):
        from taskflow.execution_formal import validate_formal_authority
        from taskflow.execution_runner import _info

        s = self.source
        baseline = s.root / "baseline-plan.json"
        baseline.write_text("{}")
        authority = read_json(s.authority)
        authority["formal_baseline"] = {"model_requested": "fixture-cli", "timeout_seconds": 15,
                                        "path": str(baseline), "sha256": _info(baseline)["sha256"]}
        write_json(s.authority, authority)
        ref = evidence_ref(s.authority, "explicit new-source formal authority")
        config = {"authorization": ref, "authorization_item": self.definition["business_packet_item"],
                  "run_id": self.item["first_run_id"], "model": "fixture-cli", "timeout_seconds": 15,
                  "adapter": {"files": {"baseline-plan.json": _info(baseline)["sha256"]}}}
        self.assertEqual("neutral", validate_formal_authority(config=config, candidate=s.candidate)["task_id"])
        for change in ({"model": "another-model"}, {"timeout_seconds": 99}, {"run_id": "unapproved-run"},
                       {"authorization_item": {**config["authorization_item"], "source_author_quality_flow": "fake"}}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                validate_formal_authority(config={**config, **change}, candidate=s.candidate)

    def test_changed_root_failed_public_missing_archive_and_fabricated_author_reject(self):
        s = self.source
        s.child["definition"]["roles"]["quality"]["runner"]["resume_sid"] = "old-quality-context"
        with self.assertRaisesRegex(TaskflowError, "honest root"):
            engineering_source(s.store, self.definition, self.item)
        s.child["definition"]["roles"]["quality"]["runner"].pop("resume_sid")
        original = s.candidate.get("author_attempt")
        s.candidate["author_attempt"] = "invented"
        with self.assertRaisesRegex(TaskflowError, "honest root"):
            engineering_source(s.store, self.definition, self.item)
        s.candidate["author_attempt"] = original
        s.attempts[self.public["attempt_id"]] = {**self.public, "state": "failed"}
        with self.assertRaisesRegex(TaskflowError, "completed same-public"):
            engineering_source(s.store, self.definition, self.item)
        s.attempts[self.public["attempt_id"]] = self.public
        archive = Path(s.child["state"]["predecessor_archive"])
        receipt = archive / "supplemental/receipt.json"
        raw = receipt.read_bytes()
        receipt.unlink()
        with self.assertRaisesRegex(TaskflowError, "existing complete full archive"):
            engineering_source(s.store, self.definition, self.item)
        receipt.write_bytes(raw)
        (s.task / "draft/evaluation/answer.md").write_text("Changed after initial capture")
        with self.assertRaisesRegex(TaskflowError, "root bytes changed"):
            engineering_source(s.store, self.definition, self.item)

    def test_quality_context_cannot_reuse_public_sid(self):
        history = self.source.child["state"]["history"]
        history[0]["cli_sid"] = self.public["cli_sid"]
        path = Path(self.source.child["state"]["receipt"])
        body = read_json(path);body["history"] = history;write_json(path, body)
        with self.assertRaisesRegex(TaskflowError, "fresh source"):
            self.native()
