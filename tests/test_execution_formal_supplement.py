"""Formal consumption of a store-validated bounded third source.

These are neutral raw-file/collection fixtures. The store graph and _validate
seam are doubles; this does not establish native permissions or typed admission.
"""

import copy
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import (
    native_quality_handoff, prepare_release, projected_quality_reports,
)
from taskflow.execution_runner import _info, _save_collection
from taskflow.execution_store import ExecutionStore
from tests import test_execution_composite_projection as composite_fixture
from tests import test_execution_formal as native_fixture


class FormalSupplementTests(unittest.TestCase):
    def setUp(self):
        case = composite_fixture.CompositeProjectionTests(
            "test_two_original_sources_project_and_release_without_fresh_whole_claim")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.root = case, case.root
        self.store, self.candidate = case.store, case.candidate
        self.graph = case.accepted["composite_quality_coverage"]
        self.graph["recheck_id"] = "original-affected-review"
        root = self.root / "supplement-native-schema"
        (root / "artifacts").mkdir(parents=True)
        self.runtime = root
        prompt = next(row for row in self.candidate["files"] if row["role"] == "prompt")
        report = {"native_signoff": {"coverage": "affected", "dimensions": {
            kind: {"affirmed": True, "evidence": [{"path": prompt["package_path"], "sha256": prompt["sha256"],
                                                   "location": "line 1"}],
                   "limitations": ["Bounded neutral supplemental source only; no new whole judgment."]}
            for kind in ("business", "artifact", "calculation")}}}
        write_json(root / "artifacts/adoption-readiness.json", report)
        write_json(root / "artifacts/issues.json", [])
        (root / "artifacts/intake.md").write_text("Original bounded supplemental examination.\n")
        manifest = {"intent_id": "supplement-intent", "role": "quality", "mode": "native_role",
                    "nature": "real_cli_attempt", "model": {"requested": "gpt-6.1-sol", "reasoning_effort": "xhigh"},
                    "candidate_binding": {key: self.candidate[key] for key in
                                          ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                    "outputs": ["adoption-readiness.json", "issues.json", "intake.md"]}
        write_json(root / "manifest.json", manifest)
        self.attempt = {"attempt_id": "supplement-native-attempt", "state": "completed", "role": "quality",
                        "candidate_id": self.candidate["candidate_id"], "cli_sid": "new-supplement-schema-sid",
                        "intent_id": "supplement-intent", "runtime_path": str(root)}
        original_get = self.store.get_attempt
        self.store.get_attempt = lambda key: self.attempt if key == self.attempt["attempt_id"] else original_get(key)
        self.seal()
        self.spec = {"schema_version": 1, "attempt_id": self.attempt["attempt_id"],
                     "base_recheck_id": self.graph["recheck_id"],
                     "report": {"path": str(root / "artifacts/adoption-readiness.json"), **_info(root / "artifacts/adoption-readiness.json")},
                     "issues": {"path": str(root / "artifacts/issues.json"), **_info(root / "artifacts/issues.json")},
                     "original_recheck_report": case.refs["adoption"]}
        self.graph["recheck_supplement"] = {"definition": self.spec, "transport": {"fixture_store_validation_contract": True}}
        self.refs = {**case.refs, **{"supplement_" + key: {"path": str(root / "artifacts" / name), **_info(root / "artifacts" / name)}
                    for key, name in (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md"))}}
        self.protocol = copy.deepcopy(case.protocol)
        for kind, dimension in self.protocol["dimensions"].items():
            prefix = "/native_signoff/dimensions/" + kind
            dimension["affirmations"].append({"source": "supplement_adoption", "pointer": prefix + "/affirmed", "equals": True})
            dimension["evidence"].append({"source": "supplement_adoption", "pointer": prefix + "/evidence", "nonempty": True})
            dimension["limitations"].append({"source": "supplement_adoption", "pointer": prefix + "/limitations", "nonempty": True})

    def seal(self):
        for name in ("collection-lock.json", "collection.json"):
            (self.runtime / name).unlink(missing_ok=True)
        manifest = read_json(self.runtime / "manifest.json")
        _save_collection(self.runtime, {"intent_id": manifest["intent_id"], "sid": self.attempt["cli_sid"],
                         "technical_accepted": True, "process_ended": True,
                         "artifacts": {name: _info(self.runtime / "artifacts" / name) for name in manifest["outputs"]}})

    def project(self, *, refs=None, protocol=None, label="projection"):
        with patch("taskflow.execution_formal._validate", side_effect=lambda root: read_json(Path(root) / "manifest.json")):
            return projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"],
                source_refs=refs or self.refs, protocol_map=protocol or self.protocol, destination=self.root / label)

    def test_three_original_sources_project_and_release_without_scope_upgrade(self):
        reports = self.project()
        derived = read_json(reports["business"])
        self.assertFalse(derived["whole_package"])
        self.assertEqual("explicit_repaired_quality_coverage", derived["scope"])
        self.assertEqual({"whole", "affected", "supplement"},
                         set(derived["derived_quality_projection"]["source_groups"].values()))
        quality = {spec["id"]: reports[spec["kind"]] for spec in read_json(self.case.case.task / "task.json")["reviews"]}
        with patch("taskflow.execution_formal._validate", side_effect=lambda root: read_json(Path(root) / "manifest.json")):
            released = prepare_release(self.store, candidate_id=self.candidate["candidate_id"],
                source_root=self.case.case.task, destination=self.root / "publication/neutral", version="supplement-v1", quality_reports=quality)
        actual = read_json(Path(released["publication_root"]) / "reports/business.json")
        self.assertFalse(actual["whole_package"])
        self.assertEqual(self.graph, actual["composite_quality_coverage"])
        self.assertEqual("original-affected-review", self.graph["recheck_id"])

    def test_every_dimension_requires_supplemental_affirmation_evidence_and_limits(self):
        for kind in ("business", "artifact", "calculation"):
            for field in ("affirmations", "evidence", "limitations"):
                protocol = copy.deepcopy(self.protocol)
                protocol["dimensions"][kind][field] = [row for row in protocol["dimensions"][kind][field]
                                                      if row["source"] != "supplement_adoption"]
                with self.subTest(kind=kind, field=field), self.assertRaisesRegex(TaskflowError, "supplemental affirmation/evidence/limits"):
                    self.project(protocol=protocol, label=kind + "-missing-" + field)

    def test_same_byte_foreign_source_reused_sid_wrong_candidate_and_collection_drift_are_rejected(self):
        foreign = self.root / "foreign-same-byte-report.json"
        foreign.write_bytes(Path(self.refs["supplement_adoption"]["path"]).read_bytes())
        references = {**self.refs, "supplement_adoption": {"path": str(foreign), **_info(foreign)}}
        with self.assertRaisesRegex(TaskflowError, "outside its explicit actual"):
            self.project(refs=references, label="foreign")
        original_sid = self.attempt["cli_sid"]
        self.attempt["cli_sid"] = self.graph["affected_source"]["sid"]
        self.seal()
        with self.assertRaisesRegex(TaskflowError, "independent same-candidate"):
            self.project(label="reused-sid")
        self.attempt["cli_sid"] = original_sid
        self.seal()
        manifest = read_json(self.runtime / "manifest.json")
        changed = copy.deepcopy(manifest)
        changed["candidate_binding"]["candidate_id"] = "foreign-candidate"
        write_json(self.runtime / "manifest.json", changed)
        self.seal()
        with self.assertRaisesRegex(TaskflowError, "independent same-candidate"):
            self.project(label="foreign-candidate")
        write_json(self.runtime / "manifest.json", manifest)
        self.seal()
        (self.runtime / "artifacts/intake.md").write_text("Drift after actual collection")
        with self.assertRaises(TaskflowError):
            self.project(label="collection-drift")

    def test_native_handoff_appends_third_certification_but_retains_fresh_normative(self):
        case = native_fixture.NativeQualityHandoffTests(
            "test_private_repair_uses_original_unchanged_and_fresh_affected_scope")
        case.setUp()
        self.addCleanup(case.doCleanups)
        old_task = case.root / "old-task"
        shutil.copytree(case.task, old_task)
        (old_task / "draft/evaluation/rubric.md").write_text("Actual old private reference.\n")
        old = capture_candidate(ExecutionStore(case.root / "old.sqlite3"), old_task, "native-old")
        case.candidates[old["candidate_id"]] = old
        old_root = case.source(old, "old-quality", coverage="unchanged_scope", outcome="revision_required")
        fresh_root = case.source(case.candidate, "fresh-quality", coverage="affected", outcome="accepted")
        case.bind_receipt("fresh-accepted", "fresh-quality")
        graph = {"recheck_id": "fresh-accepted", "whole_source": {"candidate_id": old["candidate_id"], "attempt_id": "old-quality",
                    "sid": case.attempts["old-quality"]["cli_sid"], "report": {"path": str(old_root / "artifacts/adoption-readiness.json"), **_info(old_root / "artifacts/adoption-readiness.json")}},
                 "affected_source": {"candidate_id": case.candidate["candidate_id"], "attempt_id": "fresh-quality",
                    "sid": case.attempts["fresh-quality"]["cli_sid"], "report": {"path": str(fresh_root / "artifacts/adoption-readiness.json"), **_info(fresh_root / "artifacts/adoption-readiness.json")}},
                 "input_digest": case.candidate["input_digest"], "closed_defect_ids": ["closed-defect"],
                 "private_changes": [{"path": "draft/evaluation/rubric.md"}], "source_limits": {"whole": {}, "affected": {}}}
        case.current = {"review_id": "composed", "attempt_id": "fresh-quality", "whole_package": False,
                        "snapshot_bound_acceptance": True, "composite_quality_coverage": graph}
        with patch("taskflow.execution_formal._validate", side_effect=lambda root: read_json(Path(root) / "manifest.json")):
            original = case.handoff()
        third = case.source(case.candidate, "supplement-quality", coverage="affected", outcome="accepted")
        case.attempts["supplement-quality"]["cli_sid"] = "00000000-0000-4000-8000-000000000003"
        (third / "collection.json").unlink()
        (third / "collection-lock.json").unlink()
        case.seal(third)
        spec = {"schema_version": 1, "attempt_id": "supplement-quality", "base_recheck_id": "fresh-accepted",
                "report": {"path": str(third / "artifacts/adoption-readiness.json"), **_info(third / "artifacts/adoption-readiness.json")},
                "issues": {"path": str(third / "artifacts/issues.json"), **_info(third / "artifacts/issues.json")},
                "original_recheck_report": graph["affected_source"]["report"]}
        graph["recheck_supplement"] = {"definition": spec, "transport": {"fixture_store_validation_contract": True}}
        with patch("taskflow.execution_formal._validate", side_effect=lambda root: read_json(Path(root) / "manifest.json")):
            received = native_quality_handoff(case.store, candidate_id=case.candidate["candidate_id"],
                authorization=case.authorization, source_flow_id=case.parent_id)
        self.assertEqual(original["scoring_source_allowlist"], received["scoring_source_allowlist"])
        self.assertEqual(original["normative_sources"], received["normative_sources"])
        self.assertEqual("affected", received["certification"]["supplement"]["scope"])
        self.assertEqual("fresh-accepted", received["certification"]["supplement"]["original_affected_source_retained"])
        self.assertTrue({"supplement_adoption", "supplement_issues", "supplement_intake"} <= set(received["source_refs"]))
        for dimension in received["protocol_map"]["dimensions"].values():
            for field in ("affirmations", "evidence", "limitations"):
                self.assertTrue(any(row["source"] == "supplement_adoption" for row in dimension[field]))


if __name__ == "__main__":
    unittest.main()
