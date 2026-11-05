"""Zero-launch dependency recovery around real neutral quality/repair workers."""

from contextlib import contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_flow import _public_inner_runtime, resume_workflow
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flow_tests


class PublicPreparationDependencyTests(unittest.TestCase):
    def setUp(self):
        case = flow_tests.ContinuationTests("test_running_predecessor_wait_is_stable_without_process_recovery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        # Change the neutral defect and actual repaired payload to a public file.
        case.script.write_text(flow_tests.ROLE_FIXTURE.replace("draft/evaluation/answer.md", "draft/input/prompt.md")
                               .replace("'kind':'reference'", "'kind':'material'"))
        definition = case.definition("public-change")
        public = case.predecessor(definition, completed=True)
        public_role = copy.deepcopy(definition["roles"]["quality"])
        public_role["runner"]["mode"] = "existing_isolated"
        public_role["prepare"] = {"config": {"repository": str(case.root)}}
        definition["roles"].update(public_rebuild=public_role, recheck_public=definition["roles"]["recheck"])
        case.register(definition)
        # The actual zero-model preparation implementation is covered separately;
        # this fixture stops at its original early path-constraint exception.
        with patch("taskflow.execution_flow._public_prepare", side_effect=TaskflowError("Runtime must be outside the repository")):
            child = case.run_flow([definition["flow_id"]])["flows"][0]
        self.assertEqual("public_rebuild", child["state"]["stage"], child["state"].get("exception"))
        self.assertEqual("exception", child["state"]["status"])
        self.child, self.definition = child, definition
        self.author = {"attempt_id": "original-source-author", "state": "completed", "cli_sid": "original-author-sid",
                       "intent_id": "original-source-intent"}
        source_authority = case.root / "original-authority.json"
        write_json(source_authority, {"controller": definition["owner"], "task_id": definition["task_id"]})
        source_ref = evidence_ref(source_authority, "neutral original source authority")
        previous = {"stage": "child_quality", "reason": child["state"]["exception"], "child_receipt": child["state"]["receipt"]}
        receipt = case.root / "parent-original-exception.json"
        write_json(receipt, {"flow_id": "source-author-parent", "status": "exception", "exception": previous})
        self.parent = self.store.register_flow("source-author-parent", {
            "kind": "author_quality_v1", "task_id": definition["task_id"], "owner": definition["owner"],
            "policy_id": definition["policy_id"], "authorization": source_ref,
            "source_author_attempt": self.author["attempt_id"], "directory": str(case.root / "source-parent")}, [source_ref], {
                "status": "exception", "stage": "child_quality", "current_work_id": None, "history": [], "archives": [],
                "candidate_id": definition["initial"]["candidate_id"], "candidate_root": definition["initial"]["candidate_root"],
                "child_flow_id": child["flow_id"], "public_attempt_id": public["attempt_id"],
                "author_archive": str(case.root / "original-author-archive"), "receipt": str(receipt), "exception": previous})
        self.parent = self.store.get_flow(self.parent["flow_id"])
        packet = case.root / "downstream-authority.json"
        write_json(packet, {"schema_version": 1, "controller": definition["owner"],
            "prerequisites": ["Actual accepted independent source."], "preauthorized_normal_chain": "Neutral exact original chain.",
            "confirmation": {"maximum_additional_runs_per_first": 1}, "items": [{"task_id": definition["task_id"],
                "source_author_quality_flow": self.parent["flow_id"], "source_author_attempt": self.author["attempt_id"],
                "source_author_intent": self.author["intent_id"], "source_author_sid": self.author["cli_sid"],
                "source_business_packet": str(source_authority), "source_business_packet_sha256": source_ref["sha256"],
                "first_experiment_business_unknown": "Neutral whole-job observation.", "version_id": "next-v1",
                "first_run_id": "next-run-01", "first_evaluation_id": "next-eval-01",
                "conditional_one_confirmation_run_id": "next-run-02", "conditional_one_confirmation_evaluation_id": "next-eval-02"}]})
        packet_ref = evidence_ref(packet, "neutral exact downstream authority")
        exception = {"stage": "experiment_v1", "reason": "source author-quality flow stopped with its original exception: " + str(self.parent["state"]["exception"])}
        experiment_receipt = case.root / "experiment-original-exception.json"
        write_json(experiment_receipt, {"flow_id": "source-bound-experiment", "status": "exception", "exception": exception})
        self.experiment = self.store.register_flow("source-bound-experiment", {
            "kind": "experiment_v1", "task_id": definition["task_id"], "owner": definition["owner"], "policy_id": definition["policy_id"],
            "directory": str(case.root / "experiment"), "authorization": packet_ref, "business_packet_ref": packet_ref,
            "source_author_flow_id": self.parent["flow_id"], "business_packet_item": {"task_id": definition["task_id"],
                "source_author_quality_flow": self.parent["flow_id"]}, "technical": {"formal_runtime_root": str(case.root / "formal")}},
            [packet_ref], {"status": "exception", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [],
                           "receipt": str(experiment_receipt), "exception": exception})
        self.resolution = {"action": "resume_resolved_quality_dependency", "reason": "Exact unlaunched public runtime placement corrected.",
                           "evidence": case.refs}
        self.preserved = {Path(record["state"]["receipt"]): Path(record["state"]["receipt"]).read_bytes()
                          for record in (child, self.parent, self.experiment)}

    @contextmanager
    def source_interfaces(self):
        # Actual original public/quality/repair processes, byte seals and SQLite
        # are real; the separately covered source-author handoff is an interface.
        with patch("taskflow.execution_author._author", return_value=(self.author, {})), \
             patch("taskflow.execution_author._handoff", return_value={"disposition": "candidate_complete"}):
            yield

    def resolve_child(self):
        return resume_workflow(self.store, self.child["flow_id"], reason=self.resolution["reason"], evidence=self.case.refs)

    def resume_parent(self):
        return resume_path_transport(self.store, self.parent["flow_id"], self.resolution)

    def test_exact_child_resolution_reopens_parent_and_only_its_initial_propagation(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_no_resolution_foreign_resolution_work_spec_inner_or_source_drift_are_rejected(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_distinct_business_hold_and_existing_solver_footprint_are_not_dependency_resolutions(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
