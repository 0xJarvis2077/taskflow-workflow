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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_wrong_binding_run_nonzero_model_calls_or_failed_host_gates_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_execution_markers_outer_runtime_or_ready_claimed_uncertain_solver_prevent_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
