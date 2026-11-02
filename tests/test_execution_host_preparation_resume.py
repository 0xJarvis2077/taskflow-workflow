"""Lifecycle recovery consumes a read-only host proof without preparing/spawning."""

from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_lifecycle as fixtures


class HostPreparationResumeTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.FormalPreparationResumeTests("test_exact_unclaimed_formal_guard_resumes_once_and_preserves_definition_run_decision_receipts")
        case.setUp()
        self.addCleanup(case.doCleanups)
        record, receipt = case.failure()
        self.case, self.store, self.root = case, case.store, case.root
        exception = {"stage": "formal_prepare", "reason": "existing formal preparation failed; original runtime/reservation retained: neutral original EPERM listener.bind failure"}
        write_json(receipt, {"flow_id": record["flow_id"], "status": "exception", "exception": exception})
        self.record = self.store.update_flow(record["flow_id"], record["revision"],
            {**record["state"], "exception": exception}, {"kind": "neutral_original_host_failure"})
        definition, state = self.record["definition"], self.record["state"]
        self.work_id = _node(self.record, "solver")
        self.inner = Path(definition["technical"]["formal_runtime_root"]) / "fixture-01"
        self.inner.mkdir(parents=True)
        config = {**definition["technical"]["formal_config_base"], "run_id": "fixture-01",
                  "intent_id": self.work_id + "-intent", "policy_version": definition["policy_id"]}
        self.identity = {"config_digest": sha256(canonical(config)), "release_receipt": state["release"]["receipt"],
                         "candidate_binding": state["release"]["candidate_binding"], "runtime": str(self.inner)}
        self.intent = self.inner.with_name(self.inner.name + ".formal-intent.json")
        write_json(self.intent, self.identity)
        failure = self.inner / "preparation-failure.json"
        write_json(failure, {"nature": "neutral old preparation interface fixture", "type": "PermissionError", "model_calls": 0})
        self.proof = {"binding": self.identity, "run_id": "fixture-01", "runtime": str(self.inner), "model_calls": 0,
                      "previous_runtime": str(self.root / "retained-old-preparation")}
        for name in ("plan", "acceptance", "reservation", "recovery_process"):
            path = self.root / (name + ".json")
            write_json(path, {"nature": "neutral read-only helper boundary", "run_id": "fixture-01", "model_calls": 0})
            self.proof[name] = evidence_ref(path, "neutral existing " + name)
        self.proof["intent"] = evidence_ref(self.intent, "original same-run formal intent")
        recovery = self.root / "host-recovery.json"
        write_json(recovery, {"nature": "neutral validated host-proof interface fixture", "model_calls": 0})
        self.resolution = {"action": "resume_validated_host_formal_preparation", "reason": "Consume an already validated same-run zero-model host preparation.",
            "evidence": self.record["evidence"], "preparation": evidence_ref(recovery, "neutral actual recovery receipt")}
        self.originals = {path: path.read_bytes() for path in (receipt, failure, self.intent, Path(self.proof["reservation"]["path"]))}

    def validator(self, **kwargs):
        return patch("taskflow.execution_formal.validate_formal_preparation_recovery", create=True, **kwargs)

    def test_validated_existing_preparation_resumes_same_stage_and_never_runs_prepare_or_model(self):
        before = self.store.dashboard()
        with self.validator(return_value=self.proof) as validator, \
             patch("taskflow.execution_formal.prepare_formal", side_effect=AssertionError("must not prepare")), \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("must not dispatch")):
            result = resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("formal_prepare", result["state"]["stage"])
        self.assertEqual(self.record["definition"], result["definition"])
        self.assertEqual(self.record["state"]["run_index"], result["state"]["run_index"])
        self.assertEqual(self.identity["config_digest"], sha256(canonical(validator.call_args.kwargs["config"])))
        self.assertEqual(self.inner, validator.call_args.kwargs["runtime"])
        self.assertFalse(result["state"]["history"][-1]["new_model_dispatched"])
        self.assertEqual(before["attempts"], self.store.dashboard()["attempts"])
        self.assertEqual(before["decisions"], self.store.dashboard()["decisions"])
        for path, raw in self.originals.items():
            self.assertEqual(raw, path.read_bytes())
        with self.validator(return_value=self.proof), self.assertRaisesRegex(TaskflowError, "received original"):
            resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def test_wrong_binding_run_nonzero_model_calls_or_failed_host_gates_cannot_resume(self):
        for change in ({"run_id": "another-run"}, {"binding": {"config_digest": "0" * 64}}, {"model_calls": 1}, {"model_calls": False}):
            with self.subTest(change=change), self.validator(return_value={**self.proof, **change}), \
                 self.assertRaisesRegex(TaskflowError, "exact zero-model"):
                resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        with self.validator(side_effect=TaskflowError("original failure is not EPERM or current native host probes did not pass")), \
             self.assertRaisesRegex(TaskflowError, "did not pass"):
            resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))

    def test_execution_markers_outer_runtime_or_ready_claimed_uncertain_solver_prevent_resume(self):
        outer = Path(self.record["definition"]["directory"]) / "runtimes" / self.work_id
        for path in (self.inner / "execution-claim.json", outer):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Original execution/prepared work evidence must not be overwritten.")
            with self.subTest(path=path), self.validator(return_value=self.proof), self.assertRaises(TaskflowError):
                resume_path_transport(self.store, self.record["flow_id"], self.resolution)
            self.assertTrue(path.exists())
            path.unlink()
        candidate = self.case.case.candidate
        decision = self.store.record_decision("host-negative-decision", candidate["task_id"], candidate["candidate_id"], action="experiment",
            rationale="Neutral original unrelated solver record.", evidence=self.record["evidence"],
            experiment_purpose="Neutral original solver fixture, no process/model launched.", authorized=True)
        work = self.store.enqueue(work_id="existing-related-solver", task_id=candidate["task_id"], candidate_id=candidate["candidate_id"],
            role="solver", policy_id="fixture-v1", intent_id="existing-related-intent", purpose=decision["experiment_purpose"],
            request_digest="0" * 64, evidence=self.record["evidence"])
        for status in ("ready", "claimed", "uncertain"):
            if status == "claimed":
                attempt = self.store.claim("original-solver-owner", [work["work_id"]])
            elif status == "uncertain":
                self.store.mark_uncertain(attempt["attempt_id"], "Retain this neutral original uncertain intent.")
            before = self.store.dashboard()["attempts"]
            with self.subTest(status=status), self.validator(return_value=self.proof), self.assertRaisesRegex(TaskflowError, "related work/attempt"):
                resume_path_transport(self.store, self.record["flow_id"], self.resolution)
            self.assertEqual(before, self.store.dashboard()["attempts"])
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
