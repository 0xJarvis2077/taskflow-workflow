"""Store/resume integration uses actual neutral actors and a typed-adapter boundary."""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_composite_quality as fixtures
from tools.execution_simulation import perform


class SupplementCompositionResumeTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.CompositeQualityTests("test_exact_private_repair_composes_without_new_whole_signature")
        value = case.make_closed_flow(extra=True, bad_sha=True)
        self.addCleanup(case.doCleanups)
        self.value, self.store, self.root = value, value["store"], value["case"].root
        self.candidate = self.store.get_candidate(value["candidate"])
        self.parent = self.store.get_candidate(self.candidate["parent_id"])
        self.fresh = self.store.get_attempt(self.store.get_review(value["recheck"])["attempt_id"])
        self.prior_report = Path(self.fresh["runtime_path"]) / "artifacts/adoption-readiness.json"
        old = {row["package_path"]: row for row in self.parent["files"]}
        current = {row["package_path"]: row for row in self.candidate["files"]}
        changed = [name for name in sorted(current) if current[name]["sha256"] != old[name]["sha256"]]
        self.missing_path = changed[-1]
        rows = [{"path": name, "old": old[name]["sha256"], "new": current[name]["sha256"]} for name in changed]
        citations = [{"path": name, "sha256": current[name]["sha256"], "location": "Exact neutral source judgment."} for name in changed]
        self.canonical_report = {"defect_resolution": {"affected_paths": changed, "evidence": citations}, "coverage": {"changed_files": rows}}
        raw = {"coverage": {"changed_files": rows}, "defect_resolution": {"affected_paths": changed, "evidence": [
            {**row, "package_path": row["path"], "path": current[row["path"]]["path"]} for row in citations]}}
        self.attempt = perform(self.store, self.root / "supplement", self.candidate["candidate_id"], "quality", "new-supplement", {
            "adoption-readiness.json": json.dumps(raw), "issues.json": "[]", "intake.md": "Neutral bounded supplement interface fixture."})
        runtime = Path(self.attempt["runtime_path"])
        self.spec = {"schema_version": 1, "attempt_id": self.attempt["attempt_id"], "base_recheck_id": value["recheck"],
            "report": evidence_ref(runtime / "artifacts/adoption-readiness.json", "actual neutral supplement"),
            "issues": evidence_ref(runtime / "artifacts/issues.json", "actual neutral empty issue array"),
            "original_recheck_report": evidence_ref(self.prior_report, "original affected signature preserved")}
        sources = {}
        for index, name in enumerate(changed):
            for pointer, actual in ((f"/defect_resolution/evidence/{index}", raw["defect_resolution"]["evidence"][index]),
                                    (f"/defect_resolution/affected_paths/{index}", name),
                                    (f"/coverage/changed_files/{index}", rows[index])):
                sources[pointer] = [{"report": self.spec["report"], "json_pointer": pointer, "actual_value": actual}]
        self.transport = {"report": self.canonical_report, "sources": sources,
                          "binding": {"attempt_id": self.attempt["attempt_id"], "sid": self.attempt["cli_sid"], "report": self.spec["report"]}}
        self.originals = {path: path.read_bytes() for path in [self.prior_report, runtime / "collection.json", runtime / "trace.jsonl"]}

    def adapter(self, **kwargs):
        # The typed validator has its own raw-schema/independence tests. This
        # boundary verifies store use of actual actor, files and provenance.
        self.assertEqual(self.spec, kwargs["supplement"])
        self.assertEqual(self.attempt["attempt_id"], kwargs["attempt"]["attempt_id"])
        self.assertTrue(kwargs["parent_aliases"])
        return copy.deepcopy(self.transport)

    def compose(self):
        return self.store.compose_repaired_quality(self.value["candidate"], self.value["whole"], self.value["recheck"],
            self.value["defects"], evidence=self.value["refs"], supplement=self.spec)

    def test_explicit_supplement_preserves_closure_scope_originals_and_actual_completion_order(self):
        with self.assertRaisesRegex(TaskflowError, "explicit current SHA"):
            self.store.compose_repaired_quality(self.value["candidate"], self.value["whole"], self.value["recheck"], self.value["defects"], evidence=self.value["refs"])
        closures = [self.store.get_defect(identity) for identity in self.value["defects"]]
        with patch("taskflow.execution_recheck_supplement.validate_recheck_supplement", side_effect=self.adapter, create=True):
            result = self.compose()
            self.assertEqual(result, self.store.quality_gate(self.value["candidate"]))
        graph = result["composite_quality_coverage"]
        self.assertEqual(self.spec, graph["recheck_supplement"]["definition"])
        self.assertEqual(self.value["recheck"], graph["recheck_id"])
        self.assertFalse(result["whole_package"])
        self.assertEqual(self.attempt["last_seq"], result["completed_seq"])
        self.assertEqual(self.fresh["attempt_id"], result["attempt_id"])
        self.assertEqual(closures, [self.store.get_defect(identity) for identity in self.value["defects"]])
        for path, raw in self.originals.items():
            self.assertEqual(raw, path.read_bytes())

    def test_missing_canonical_provenance_or_author_context_cannot_be_accepted(self):
        missing = copy.deepcopy(self.transport)
        missing["sources"] = {}
        with patch("taskflow.execution_recheck_supplement.validate_recheck_supplement", return_value=missing, create=True), \
             self.assertRaisesRegex(TaskflowError, "exact raw provenance"):
            self.compose()
        original_load = ExecutionStore._load
        def load(db, table, identity):
            value = original_load(db, table, identity)
            if table == "attempts" and identity == self.attempt["attempt_id"]:
                return {**value, "cli_sid": self.fresh["cli_sid"]}
            return value
        with patch.object(ExecutionStore, "_load", side_effect=load), \
             patch("taskflow.execution_recheck_supplement.validate_recheck_supplement", side_effect=AssertionError("independence must fail first"), create=True), \
             self.assertRaisesRegex(TaskflowError, "reused an author/public/prior-quality"):
            self.compose()

    def lifecycle(self, result):
        flow = self.value["flow"]
        source_receipt = self.root / "accepted-author-result.json"
        write_json(source_receipt, {"outcome": "candidate_quality_accepted"})
        parent = self.store.register_flow("source-author", {"kind": "author_quality_v1"}, self.value["refs"], {
            "status": "accepted", "receipt": str(source_receipt), "result": {"outcome": "candidate_quality_accepted"}})
        authority = self.root / "supplement-source-authority.json"
        task_id = self.candidate["task_id"]
        write_json(authority, {"schema_version": 1, "controller": "fixture-controller", "prerequisites": ["Exact accepted quality."],
            "preauthorized_normal_chain": "One neutral original chain.", "confirmation": {}, "items": [{"task_id": task_id,
                "source_author_quality_flow": parent["flow_id"], "source_author_intent": "author-intent", "source_author_attempt": "author-attempt",
                "source_author_sid": "author-sid", "source_business_packet": str(source_receipt), "source_business_packet_sha256": "a" * 64,
                "first_experiment_business_unknown": "Neutral whole task.", "version_id": "neutral-v1", "first_run_id": "neutral-01",
                "first_evaluation_id": "neutral-eval-01", "conditional_one_confirmation_run_id": "neutral-02", "conditional_one_confirmation_evaluation_id": "neutral-eval-02"}]})
        ref = evidence_ref(authority, "neutral explicit exact source item")
        receipt = self.root / "original-source-exception.json"
        exception = {"stage": "experiment_v1", "reason": "Fresh recheck has no explicit current SHA evidence for changed path: " + self.missing_path}
        write_json(receipt, {"flow_id": "supplement-source", "status": "exception", "exception": exception})
        record = self.store.register_flow("supplement-source", {"kind": "experiment_v1", "task_id": task_id,
            "owner": "fixture-controller", "policy_id": "fixture-v1", "authorization": ref, "business_packet_ref": ref,
            "source_author_flow_id": parent["flow_id"], "business_packet_item": {"task_id": task_id, "source_author_quality_flow": parent["flow_id"]},
            "directory": str(self.root / "experiment"), "technical": {"formal_runtime_root": str(self.root / "formal")}}, [ref], {
                "status": "exception", "stage": "experiment_v1", "current_work_id": None, "receipt": str(receipt), "exception": exception, "history": [], "archives": []})
        resolution = {"action": "resume_validated_quality_composition", "reason": "Consume only the already validated bounded supplement.",
            "evidence": [ref], "composite_review_id": result["review_id"], "supplement": self.spec}
        bound = {"source": flow, "initial": self.parent}
        return record, resolution, bound, receipt

    def test_resume_only_consumes_validated_composite_and_keeps_original_source_ids_and_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_resume_rejects_wrong_composite_or_existing_formal_intent(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
