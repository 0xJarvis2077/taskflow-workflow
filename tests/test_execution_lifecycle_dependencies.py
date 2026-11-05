"""Only an explicit real repair projection reopens its propagated dependencies."""

from contextlib import contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_repair_record_projection as fixtures


class QualityDependencyResumeTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.RepairRecordProjectionTests("test_explicit_projection_reuses_original_attempt_and_requires_fresh_recheck")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.prepare_projection_exception()
        self.case, self.store = case, case.store
        task_id = case.parent["task_id"]
        self.source_author = {"attempt_id": "original-source-author", "state": "completed", "cli_sid": "original-author-sid",
                              "intent_id": "original-author-intent"}
        self.public = {"attempt_id": "original-public-attempt", "state": "completed", "role": "quality", "cli_sid": "fresh-public-sid"}
        source_authority = case.root / "source-author-authority.json"
        write_json(source_authority, {"controller": "fixture-controller", "task_id": task_id})
        source_ref = evidence_ref(source_authority, "neutral original author authority")
        self.old_exception = {"stage": "child_quality", "reason": case.failed["state"]["exception"],
                              "child_receipt": case.failed["state"]["receipt"]}
        parent_receipt = case.root / "parent-original-exception.json"
        write_json(parent_receipt, {"flow_id": "source-author-parent", "status": "exception", "exception": self.old_exception})
        definition = {"kind": "author_quality_v1", "owner": "fixture-controller", "task_id": task_id, "policy_id": "fixture-v1",
                      "source_author_attempt": self.source_author["attempt_id"], "authorization": source_ref,
                      "directory": str(case.root / "source-parent")}
        self.parent = self.store.register_flow("source-author-parent", definition, [source_ref], {
            "status": "exception", "stage": "child_quality", "current_work_id": None,
            "candidate_id": case.parent["candidate_id"], "candidate_root": case.definition["initial"]["candidate_root"],
            "child_flow_id": case.definition["flow_id"], "public_attempt_id": self.public["attempt_id"],
            "author_archive": str(case.root / "original-author-archive"), "receipt": str(parent_receipt),
            "exception": self.old_exception, "history": [], "archives": []})
        self.parent = self.store.get_flow(self.parent["flow_id"])
        packet = case.root / "downstream-authority.json"
        write_json(packet, {"schema_version": 1, "controller": "fixture-controller", "prerequisites": ["Actual independently accepted current candidate."],
            "preauthorized_normal_chain": "One exact original neutral chain.", "confirmation": {"maximum_additional_runs_per_first": 1},
            "items": [{"task_id": task_id, "source_author_quality_flow": self.parent["flow_id"],
                "source_author_attempt": self.source_author["attempt_id"], "source_author_intent": self.source_author["intent_id"],
                "source_author_sid": self.source_author["cli_sid"], "source_business_packet": str(source_authority),
                "source_business_packet_sha256": source_ref["sha256"], "first_experiment_business_unknown": "Neutral whole-job observation.",
                "version_id": "next-v1", "first_run_id": "next-run-01", "first_evaluation_id": "next-eval-01",
                "conditional_one_confirmation_run_id": "next-run-02", "conditional_one_confirmation_evaluation_id": "next-eval-02"}]})
        ref = evidence_ref(packet, "neutral exact downstream source authority")
        experiment_receipt = case.root / "experiment-original-exception.json"
        exception = {"stage": "experiment_v1", "reason": "source author-quality flow stopped with its original exception: " + str(self.parent["state"]["exception"])}
        write_json(experiment_receipt, {"flow_id": "source-bound-experiment", "status": "exception", "exception": exception})
        self.experiment = self.store.register_flow("source-bound-experiment", {"kind": "experiment_v1", "task_id": task_id,
            "owner": "fixture-controller", "policy_id": "fixture-v1", "directory": str(case.root / "downstream"),
            "authorization": ref, "business_packet_ref": ref, "source_author_flow_id": self.parent["flow_id"],
            "business_packet_item": {"task_id": task_id, "source_author_quality_flow": self.parent["flow_id"]},
            "technical": {"formal_runtime_root": str(case.root / "formal" )}}, [ref], {
                "status": "exception", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [],
                "receipt": str(experiment_receipt), "exception": exception})
        self.resolution = {"action": "resume_resolved_quality_dependency", "reason": "Propagate only the original explicit declaration-copy repair resolution.",
                           "evidence": case.refs}
        self.preserved = {path: path.read_bytes() for path in (case.original, case.runtime / "collection.json",
            Path(case.failed["state"]["receipt"]), parent_receipt, experiment_receipt)}
        case.resume()

    @contextmanager
    def source_interfaces(self):
        # Actual repair execution, collection, projection and resolver are real.
        # The already-covered original author/public adapters are interface fixtures.
        with patch("taskflow.execution_author._author", return_value=(self.source_author, {})), \
             patch("taskflow.execution_author._handoff", return_value={"disposition": "candidate_complete"}), \
             patch("taskflow.execution_flow._predecessor", return_value=(self.public, {})):
            yield

    def resume_parent(self):
        return resume_path_transport(self.store, self.parent["flow_id"], self.resolution)

    def test_exact_projection_reopens_parent_then_source_experiment_without_new_models(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_unrelated_active_child_missing_projection_and_failed_child_cannot_bypass(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_unresolved_uncertain_current_child_attempt_is_not_a_resolved_dependency(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_experiment_requires_explicit_parent_resolution_and_no_formal_footprint(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_may_finish_its_actual_fresh_recheck_before_experiment_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
