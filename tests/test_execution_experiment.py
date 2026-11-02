"""A bounded packet reaches one real fixture queue without dispatching a model.

Quality provenance and detached publication are explicit simulated adapters in
this state-machine suite. It does not certify native quality; the separate
release-report binding tests exercise real canonical production/validation.
"""

import copy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import PUBLIC_ROLES, read_json, write_json
from taskflow.execution_experiment import advance_experiment, _confirmation_limit, _required_release_reports
from taskflow.execution_runner import _info
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.packages import freeze_task
from tests.helpers import add_reviews, make_task
from tools.execution_simulation import fixture_policy, perform
from taskflow.records import import_run
from taskflow import execution_review
from datetime import datetime, timezone


class ExperimentBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.task = make_task(self.root)
        add_reviews(self.task)
        self.candidate = execution.capture_candidate(self.store, self.task, "neutral-initial")
        quality = perform(self.store, self.root / "quality", "neutral-initial", "quality", "source-quality", {
            "adoption-readiness.json": '{"execution_routing":{"scope":"whole_package","outcome":"accepted"}}',
            "issues.json": '{"issues":[]}', "intake.md": "Neutral fixture scope only, not real business/model quality.\n"})
        self.quality = quality
        self.refs = [evidence_ref(Path(quality["runtime_path"]) / "artifacts/adoption-readiness.json", "neutral fixture acceptance")]
        self.store.record_review("source-review", "neutral-initial", quality["attempt_id"], verdict="accepted",
                                scope="whole neutral fixture", evidence=self.refs, whole_package=True)
        receipt = self.root / "source-result.json"
        write_json(receipt, {"nature": "test_fixture", "status": "accepted"})
        self.store.register_flow("source-flow", {"policy_id": "fixture-v1"}, self.refs,
            {"status": "accepted", "candidate_id": "neutral-initial", "candidate_root": str(self.task),
             "accepted_review_id": "source-review", "receipt": str(receipt), "history": []})
        self.snapshot = Path(freeze_task(self.task, "fixture-v1")["path"])
        self.authority = self.root / "business.json"
        packet = {"controller": "fixture-controller", "task_id": "neutral", "source_flow_id": "source-flow",
                  "initial_candidate_id": "neutral-initial", "initial_expected_digests": {key: self.candidate[key] for key in (
                      "input_digest", "content_digest", "evaluation_digest")},
                  "business_investment": {"action": "one neutral fixture experiment", "unresolved_business_question": "Exercise the neutral packet handoff without a model."},
                  "release": {"version_id": "fixture-v1"}, "formal": {"first_run_id": "fixture-01"},
                  "independent_review": {"score_id": "fixture-eval-01", "teacher_file_ids": ["answer", "rubric"]},
                  "single_confirmation": {"authorized_if": "First signed original<=3/7, only when exact local evidence is eligible.",
                       "max_additional_attempts": 1, "run_id": "fixture-02", "score_id": "fixture-eval-02"}}
        write_json(self.authority, packet)
        authority = evidence_ref(self.authority, "neutral single-purpose packet, never real model authority")
        self.definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "neutral-experiment",
             "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
             "authorization": authority, "business_packet_ref": authority, "source_flow_id": "source-flow",
             "directory": str(self.root / "lifecycle"),
             "technical": {"publication_root": str(self.root / "publication/neutral"),
                           "formal_runtime_root": str(self.root / "formal"),
                           "formal_config_base": {"model": "fixture-cli", "reasoning_effort": "fixture", "reasoning_context": "all_turns", "reasoning_selection": "verified_client_default"},
                           "quality_projection_map": {}, "review_runner": {}, "real_repository": str(self.root)}}
        self.record = self.store.register_flow("neutral-experiment", self.definition, [authority],
            {"status": "active", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [], "waiting": None})

    def advance(self):
        return advance_experiment(self.store, self.store.get_flow(self.record["flow_id"]))

    def release(self, *args, **kwargs):
        self.assertEqual(_required_release_reports(read_json(self.task / "task.json"), self.fixture_projections),
                         kwargs["quality_reports"])
        binding = {key: self.candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        return {"candidate_binding": binding, "snapshot": {"path": str(self.snapshot)},
                "publication_root": str(self.task), "quality_evidence": self.refs, "receipt": self.refs[0]}

    def prepare(self, **kwargs):
        self.assertEqual("fixture-01", kwargs["config"]["run_id"])
        self.assertEqual(self.candidate, kwargs["candidate"])
        cwd = kwargs["runtime"] / "work"
        cwd.mkdir(parents=True)
        script = cwd.parent / "local-fixture.py"
        script.write_text("import pathlib,json\npathlib.Path('answer.md').write_text('A neutral result.')\n" +
            "print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000001'}))\n" +
            "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral local delivery.'}}))\n" +
            "print(json.dumps({'type':'turn.completed'}))\n", encoding="utf-8")
        return {"runner": {"intent_id": kwargs["config"]["intent_id"], "policy_version": "fixture-v1", "role": "solver",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "cwd": str(cwd), "argv": [str(Path(sys.executable).resolve()), str(script)], "dependencies": [str(script)],
                "inputs": [], "request_sections": [{"kind": "operation", "text": "Neutral local fixture only."}], "outputs": ["answer.md"]}}

    def ready(self):
        self.assertEqual("release", self.advance()["state"]["stage"])
        # This suite's source is an actual test_fixture worker, not a native
        # independent quality signer. Never relabel its sealed manifest or
        # fabricate a passed signature to get through the production adapter.
        # Simulate only that provenance/IO boundary, retaining actual canonical
        # kind paths and the real pure required-ID selection contract.
        def project_fixture(store, *, candidate_id, source_refs, protocol_map, destination):
            self.assertIs(self.store, store)
            self.assertEqual(self.candidate["candidate_id"], candidate_id)
            self.assertEqual({"adoption", "issues", "intake"}, set(source_refs))
            self.assertEqual(self.definition["technical"]["quality_projection_map"], protocol_map)
            self.fixture_projections = {}
            for kind in ("business", "artifact", "calculation"):
                path = destination / (kind + ".json")
                write_json(path, {"schema_version": 1, "nature": "test_fixture", "kind": kind,
                    "candidate_id": candidate_id, "source_refs": source_refs,
                    "scope": "simulated state-machine quality adapter; no native or business signoff"})
                self.fixture_projections[kind] = path
            return self.fixture_projections

        def select_fixture(store, *, candidate_id, source_root, reports):
            self.assertIs(self.store, store)
            self.assertEqual(self.candidate["candidate_id"], candidate_id)
            self.assertEqual(self.task, source_root)
            self.assertEqual(self.fixture_projections, reports)
            self.assertEqual({"business", "artifact", "calculation"}, set(reports))
            for kind, path in reports.items():
                report = read_json(path)
                self.assertEqual(kind, report["kind"])
                self.assertEqual("test_fixture", report["nature"])
                self.assertNotEqual("passed", report.get("status"))
            return _required_release_reports(read_json(source_root / "task.json"), reports)

        with patch("taskflow.execution_experiment.projected_quality_reports", side_effect=project_fixture) as project, \
             patch("taskflow.execution_experiment._release_quality_reports", side_effect=select_fixture) as select, \
             patch("taskflow.execution_experiment.prepare_release", side_effect=self.release):
            record = self.advance()
        self.assertEqual("formal_prepare", record["state"]["stage"])
        self.assertEqual({"adoption", "issues", "intake"}, set(project.call_args.kwargs["source_refs"]))
        select.assert_called_once()
        with patch("taskflow.execution_experiment.prepare_formal", side_effect=self.prepare):
            return self.advance()

    def test_source_release_prepare_reaches_one_exact_ready_work_without_launch(self):
        record = self.ready()
        self.assertEqual("formal_wait", record["state"]["stage"], record)
        work = self.store.get_work(record["state"]["current_work_id"])
        self.assertEqual("ready", work["state"])
        self.assertEqual("solver", work["role"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        decisions = self.store.dashboard()["decisions"]
        self.assertEqual(1, len(decisions))
        self.assertTrue(decisions[0]["authorized"])
        self.assertEqual(decisions[0]["experiment_purpose"], work["purpose"])
        self.assertFalse((Path(work["runtime_path"]) / "process.json").exists())

    def test_claimed_original_wait_is_stable_and_uncertain_stops_without_redraw(self):
        record = self.ready()
        attempt = self.store.claim("fixture-original", [record["state"]["current_work_id"]])
        with patch("taskflow.execution_experiment.archive_full_attempt", side_effect=AssertionError("not completed")):
            waiting = self.advance()
            self.assertEqual(waiting["revision"], self.advance()["revision"])
        self.assertEqual("original_worker_owns_completion", waiting["state"]["waiting"]["reason"])
        self.store.mark_uncertain(attempt["attempt_id"], "neutral launch identity unresolved")
        failed = self.advance()
        self.assertEqual("exception", failed["state"]["status"])
        self.assertIn("no automatic redraw", failed["state"]["exception"]["reason"])
        self.assertEqual("uncertain", self.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_source_pending_waits_without_dispatch(self):
        source = self.store.get_flow("source-flow")
        self.store.update_flow(source["flow_id"], source["revision"], {**source["state"], "status": "active"}, {"kind": "fixture_pending"})
        waiting = self.advance()
        self.assertEqual("source_quality_not_accepted", waiting["state"]["waiting"]["reason"])
        self.assertEqual(waiting["revision"], self.advance()["revision"])
        self.assertEqual(1, len(self.store.dashboard()["work"]))

    def intake_state(self):
        record = self.ready()
        self.definition = copy.deepcopy(record["definition"])
        self.definition["technical"]["formal_config_base"] = {
            "model": "fixture-cli", "reasoning_effort": "fixture", "reasoning_context": "all_turns",
            "reasoning_selection": "verified_client_default"}
        # The registered definition is immutable; this fixture creates its
        # complete technical config before simulating the intake transition.
        flow_id = "intake-experiment"
        authority = self.definition["authorization"]
        self.definition["flow_id"] = flow_id
        state = {**record["state"], "stage": "evaluation_intake", "current_work_id": None,
                 "formal_attempt": {"attempt_id": "fixture-solver-attempt", "cli_sid": "00000000-0000-4000-8000-000000000001"},
                 "facts_attempt": {"runtime_path": str(self.root / "facts")},
                 "score_attempt": {"runtime_path": str(self.root / "score")},
                 "facts_prepared": {"review_binding": str(self.root / "facts-binding.json")},
                 "score_prepared": {"review_binding": str(self.root / "score-binding.json")},
                 "facts_seal": {"sid": "00000000-0000-4000-8000-000000000002"},
                 "imported_run": {"run_path": str(self.root / "original-run")}}
        # Use another fixture packet, so the unique once-only authority gate
        # remains active rather than being bypassed by the test.
        packet = read_json(self.authority)
        new_authority = self.root / "intake-authority.json"
        write_json(new_authority, packet)
        authority = evidence_ref(new_authority, "separate neutral intake fixture")
        self.definition.update(authorization=authority, business_packet_ref=authority)
        self.store.register_flow(flow_id, self.definition, [authority], state)
        self.record = self.store.get_flow(flow_id)
        original = self.root / "original-run"
        original.mkdir()
        write_json(original / "record.original.json", {
            "requested_config": {"model": "fixture-cli", "reasoning_selection": "verified_client_default", "request_reasoning_override": None},
            "observed_config": {"model": None, "client_resolved": "fixture-cli",
                                "client_resolved_reasoning": {"effort": "fixture", "context": "all_turns"}}})
        result = {"accepted": True, "run": str(original), "evaluation_id": "fixture-eval-01", "source_sha256": "a" * 64,
                  "signer_sid": "00000000-0000-4000-8000-000000000002", "score": 2,
                  "review_archive": {"path": str(self.root / "raw")},
                  "local_profile_assessment": {"signed_binding_valid": True, "structural_reasons": [],
                       "local_eligible": True, "artifacts_complete": True, "served_identity_unknown_disclosed": True,
                       "substantive_disputes": []}}
        observed = {"completion": "completed", "input_binding": "matched", "gaps": ["observed_config.model: unknown"],
                    "model_validity": "not_determined"}
        return result, observed

    def test_only_signed_exact_local_profile_can_authorize_one_confirmation(self):
        result, observed = self.intake_state()
        with patch("taskflow.execution_experiment.ingest_signed_evaluation", return_value=result), \
             patch("taskflow.execution_experiment.verify_run", return_value=observed):
            next_record = self.advance()
        self.assertEqual("formal_prepare", next_record["state"]["stage"], next_record)
        self.assertEqual(1, next_record["state"]["run_index"])
        self.assertTrue(next_record["state"]["confirmation_authorized"])
        self.assertTrue(next_record["state"]["confirmation_basis"]["eligible"])
        self.assertIn(result["signer_sid"], next_record["state"]["excluded_sids"])
        state = {**next_record["state"], "stage": "evaluation_intake"}
        self.store.update_flow(next_record["flow_id"], next_record["revision"], state, {"kind": "fixture_second_intake"})
        with patch("taskflow.execution_experiment.ingest_signed_evaluation", return_value=result), \
             patch("taskflow.execution_experiment.verify_run", return_value=observed):
            final = self.advance()
        self.assertEqual("accepted", final["state"]["status"])
        self.assertIn("single_confirmation_already_used", final["state"]["result"]["confirmation_eligibility"]["reasons"])
        self.assertEqual("not_determined", final["state"]["result"]["model_validity"])

    def test_unknown_signed_validity_or_real_gap_returns_controller_without_confirmation(self):
        result, observed = self.intake_state()
        result["local_profile_assessment"]["local_eligible"] = "unknown"
        observed["gaps"].append("actual_input_digest: mismatch with selected snapshot")
        with patch("taskflow.execution_experiment.ingest_signed_evaluation", return_value=result), \
             patch("taskflow.execution_experiment.verify_run", return_value=observed):
            final = self.advance()
        self.assertEqual("accepted", final["state"]["status"])
        self.assertFalse(final["state"]["result"]["confirmation_dispatched"])
        reasons = final["state"]["result"]["confirmation_eligibility"]["reasons"]
        self.assertIn("signed_local_eligible_unknown", reasons)
        self.assertTrue(any("actual_input_digest" in item for item in reasons))

    def test_confirmation_bound_uses_explicit_packet_scale(self):
        self.assertEqual(3, _confirmation_limit(read_json(self.authority), 7))
        packet = read_json(self.authority)
        packet["single_confirmation"]["authorized_if"] = "Not authorized by a numeric bounded condition."
        self.assertIsNone(_confirmation_limit(packet, 7))

    def test_modified_packet_does_not_dispatch(self):
        packet = read_json(self.authority)
        packet["controller"] = "another-controller"
        write_json(self.authority, packet)
        failed = self.advance()
        self.assertEqual("exception", failed["state"]["status"])
        self.assertIn("evidence changed", failed["state"]["exception"]["reason"])
        self.assertEqual(1, len(self.store.dashboard()["work"]))

    def import_original_fixture(self, *, attempt, archive, snapshot, run_id):
        incoming = self.root / "incoming" / run_id
        incoming.mkdir(parents=True)
        manifest = read_json(archive / "manifest.json")
        shutil.copyfile(Path(manifest["cwd"]) / "answer.md", incoming / "answer.md")
        shutil.copyfile(archive / "trace.jsonl", incoming / "trace.jsonl")
        artifacts = [{"id": "answer", "path": "answer.md", "kind": "answer"},
                     {"id": "trace", "path": "trace.jsonl", "kind": "trace"}]
        for item in self.candidate["files"]:
            if item["role"] not in PUBLIC_ROLES:
                continue
            target = incoming / "work" / item["package_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item["path"], target)
            artifacts.append({"id": "public-" + target.name.replace(".", "-"), "path": str(target.relative_to(incoming)), "kind": "evidence"})
        rows = [{"id": item["id"], "incoming_path": item["path"],
                 "visibility": "actual_trace" if item["id"] == "trace" else "submission" if item["id"] == "answer" else "public",
                 **_info(incoming / item["path"])} for item in artifacts]
        index = incoming / "indexes/first_review_index.json"
        write_json(index, {"allowed_direct_artifacts": rows})
        artifacts.append({"id": "first_review_index", "path": "indexes/first_review_index.json", "kind": "evidence"})
        now = datetime.now(timezone.utc).isoformat()
        write_json(incoming / "run.json", {"schema_version": 1, "run_id": run_id, "nature": "test_fixture",
            "status": "completed", "actual_input_digest": self.candidate["input_digest"],
            "requested_config": {"model": "fixture-cli", "tools": [], "reasoning_selection": "verified_client_default", "request_reasoning_override": None},
            "observed_config": {"model": "fixture-cli", "tools": [], "client_resolved": "fixture-cli",
                                "client_resolved_reasoning": {"effort": "fixture", "context": "all_turns"}},
            "started_at": now, "ended_at": now, "interventions": [], "exceptions": [],
            "input_evidence": [rows[-1]["id"] + "#public"], "isolation_evidence": ["trace#fixture-only"], "artifacts": artifacts})
        imported = import_run(snapshot, incoming / "run.json")
        inputs, _ = execution_review._first_inputs({"first_review_index": str(index)}, imported)
        return {"run_path": imported["path"], "run_digest": imported["run_digest"],
                "first_review_index": str(index), "first_review_inputs": inputs}

    def fixture_role(self, *, store, config, runtime, scoring=False):
        work = config["work"]
        runtime.parent.mkdir(parents=True, exist_ok=True)
        cwd = runtime.parent / "work"
        cwd.mkdir()
        run = read_json(Path(config["run_directory"]) / "run.json")
        inputs, selector = execution_review._first_inputs(config, run)
        public = [item for item in self.candidate["files"] if item["role"] in PUBLIC_ROLES]
        binding = {"schema_version": 1, "run_directory": config["run_directory"], "run_digest": run["run_digest"],
            "candidate_binding": {key: self.candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
            "first_review_inputs": inputs, "selector": selector, "public_sources": public,
            "excluded_sids": [*config["excluded_sids"], "00000000-0000-4000-8000-000000000001"]}
        payload = {"facts.json": '{"scope":"complete neutral submission and original trace"}'}
        extra = {}
        if scoring:
            facts = execution_review.seal_existing_facts(config={"facts_attempt": config["facts_attempt"], "review_binding": config["facts_binding"]})["seal"]
            normative = [item for item in self.candidate["files"] if item["package_path"] in config["scoring_source_allowlist"]]
            declaration = next(item for item in self.candidate["evidence"] if item["location"] == "explicit package declaration")
            projection = runtime.parent / "task-scoring.json"
            write_json(projection, read_json(Path(declaration["path"]))["scoring"])
            projection_ref = {"source": declaration["path"], "source_sha256": declaration["sha256"], "path": str(projection),
                              "target": "normative/task-scoring.json", "sha256": _info(projection)["sha256"]}
            binding.update(facts_seal=facts, scoring_sources=normative, scoring_source_allowlist=config["scoring_source_allowlist"], scoring_projection=projection_ref)
            inputs = [*inputs, {"path": facts["path"], "kind": "submission", "target": "sealed/facts.json"},
                      *[{"path": item["path"], "kind": "teacher", "target": item["package_path"]} for item in normative],
                      {"path": str(projection), "kind": "teacher", "target": projection_ref["target"]}]
            public_name = next(item["package_path"] for item in public)
            score = {"max_score": 7, "total_score": 7, "criteria": [{"id": "C1", "max_score": 7, "score": 7,
                    "reason": "Neutral fixture original result retained.", "public_evidence": [{"path": public_name, "locator": "1"}],
                    "submission_evidence": [{"path": "original/answer/answer.md", "locator": "1"}]}]}
            payload = {"score.json": json.dumps(score), "explanation.md": "Whole neutral original examination.\n"}
            extra = {"facts_attempt": config["facts_attempt"], "resume_sid": facts["sid"],
                     "scoring_source_allowlist": config["scoring_source_allowlist"], "scoring_projection": projection_ref}
        binding_path = runtime.parent / "review-binding.json"
        write_json(binding_path, binding)
        payload_file = runtime.parent / "payload.json"
        write_json(payload_file, payload)
        script = Path(__file__).resolve().parents[1] / "tools/mock_role_cli.py"
        runner = {"role": work["role"], "intent_id": work["intent_id"], "policy_version": "fixture-v1", "mode": "test_fixture",
                  "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15, "cwd": str(cwd),
                  "argv": [str(Path(sys.executable).resolve()), str(script), "--payload", str(payload_file), "--sid", "00000000-0000-4000-8000-000000000002"],
                  "dependencies": [str(binding_path), str(script), str(payload_file)], "inputs": inputs,
                  "request_sections": [{"kind": "operation", "text": "Neutral zero-model subprocess fixture."}], "outputs": list(payload), **extra}
        prepared = execution.enqueue_spec(store, {"work": work, "runner": runner}, runtime)
        return {**prepared, "review_binding": str(binding_path), "run_digest": run["run_digest"]}

    def test_real_fixture_workers_traverse_import_facts_seal_score_and_original_intake(self):
        record = self.ready()
        execution.run_worker(self.store, "fixture-original", False, [record["state"]["current_work_id"]])
        with patch("taskflow.execution_experiment.import_formal", side_effect=self.import_original_fixture):
            record = self.advance()
        self.assertEqual("facts_prepare", record["state"]["stage"], record)
        with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=self.fixture_role):
            record = self.advance()
        self.assertEqual("facts_wait", record["state"]["stage"], record)
        execution.run_worker(self.store, "fixture-facts", False, [record["state"]["current_work_id"]])
        record = self.advance()
        self.assertEqual("score_prepare", record["state"]["stage"], record)
        facts = Path(record["state"]["facts_attempt"]["runtime_path"])
        self.assertTrue((facts / "facts-seal.json").is_file())
        with patch("taskflow.execution_experiment.prepare_score_runner", side_effect=lambda **kwargs: self.fixture_role(scoring=True, **kwargs)):
            record = self.advance()
        self.assertEqual("score_wait", record["state"]["stage"], record)
        execution.run_worker(self.store, "fixture-score", False, [record["state"]["current_work_id"]])
        record = self.advance()
        self.assertEqual("evaluation_intake", record["state"]["stage"], record)
        record = self.advance()
        self.assertEqual("accepted", record["state"]["status"], record)
        result = record["state"]["result"]["runs"][0]
        self.assertEqual("test_fixture", result["nature"])
        self.assertEqual(7, result["score"])
        self.assertEqual("not_determined", result["model_validity"])
        self.assertEqual(4, len(self.store.dashboard()["attempts"]))
        self.assertEqual(record, self.advance())


if __name__ == "__main__":
    unittest.main()
