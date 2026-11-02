"""Explicit two-source coverage remains scoped through projection and intake."""

import copy
import json
import unittest
from pathlib import Path
from types import SimpleNamespace

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_formal import projected_quality_reports, prepare_release
from taskflow.execution_experiment import advance_experiment, _normative_paths
from taskflow.execution_lifecycle import register_lifecycle
from taskflow.execution_runner import _info, _save_collection
from taskflow.execution_store import evidence_ref
from taskflow.packages import freeze_task
from tests import test_execution_formal as formal_fixtures
from tests import test_execution_composite_quality as composition_fixtures
from tests.helpers import add_reviews


class CompositeProjectionTests(unittest.TestCase):
    def setUp(self):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.root, self.candidate = case, case.root, case.candidate
        old = {**self.candidate, "candidate_id": "previous-neutral", "content_digest": "a" * 64}
        candidates = {self.candidate["candidate_id"]: self.candidate, old["candidate_id"]: old}
        attempts, refs, groups = {}, {}, {}
        for group, bound, sid in (("whole", old, "whole-schema-signer"), ("affected", self.candidate, "fresh-schema-signer")):
            root = self.root / (group + "-schema-original")
            (root / "artifacts").mkdir(parents=True)
            report = {"execution_routing": {"scope": "whole_package" if group == "whole" else "affected",
                       "outcome": "revision_required" if group == "whole" else "accepted",
                       "covered_defect_ids": [] if group == "whole" else ["private-defect"]},
                      "business": {"status": "accepted"}, "artifact": {"readback": True},
                      "calculation": {"exact": True}, "coverage": {"changed_files": ["private-path"]},
                      "limitations": ["Neutral schema-only source; remote production unknown."]}
            write_json(root / "artifacts/adoption-readiness.json", report)
            write_json(root / "artifacts/issues.json", {"issues": []})
            (root / "artifacts/intake.md").write_text("Explicit neutral signed source passage.\n")
            manifest = {"intent_id": group + "-intent", "role": "quality", "nature": "real_cli_attempt",
                        "candidate_binding": {key: bound[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                        "outputs": ["adoption-readiness.json", "intake.md", "issues.json"]}
            write_json(root / "manifest.json", manifest)
            _save_collection(root, {"intent_id": manifest["intent_id"], "sid": sid,
                                   "technical_accepted": True, "process_ended": True,
                                   "artifacts": {name: _info(root / "artifacts" / name) for name in manifest["outputs"]}})
            aid = group + "-schema-attempt"
            attempts[aid] = {"attempt_id": aid, "state": "completed", "role": "quality", "candidate_id": bound["candidate_id"],
                             "cli_sid": sid, "intent_id": manifest["intent_id"], "runtime_path": str(root)}
            prefix = "whole_" if group == "whole" else ""
            for name, filename in (("adoption", "adoption-readiness.json"), ("intake", "intake.md"), ("issues", "issues.json")):
                refs[prefix + name] = {"path": str(root / "artifacts" / filename), **_info(root / "artifacts" / filename)}
            groups[group] = {"candidate_id": bound["candidate_id"], "attempt_id": aid, "sid": sid,
                             "report": refs[prefix + "adoption"]}
        graph = {"whole_source": groups["whole"], "affected_source": groups["affected"],
                 "input_digest": self.candidate["input_digest"], "closed_defect_ids": ["private-defect"],
                 "private_changes": [{"path": "private-path"}],
                 "source_limits": {"whole": {"limits": ["Unknown original remote boundary"]}, "affected": {"limits": ["Affected only"]}}}
        self.accepted = {"review_id": "composed-schema-coverage", "attempt_id": groups["affected"]["attempt_id"],
                         "whole_package": False, "snapshot_bound_acceptance": True, "evidence": [], "composite_quality_coverage": graph}
        self.store = SimpleNamespace(get_candidate=lambda identity: candidates[identity], get_attempt=lambda identity: attempts[identity],
                                     quality_gate=lambda identity: self.accepted)
        authority = self.root / "composite-map-authority.json"
        write_json(authority, {"task_id": "neutral", "controller": "neutral schema mapping fixture"})
        def dimension(pointer, expected):
            return {"affirmations": [{"source": "whole_adoption", "pointer": pointer, "equals": expected},
                                     {"source": "adoption", "pointer": "/execution_routing/outcome", "equals": "accepted"}],
                    "evidence": [{"source": "adoption", "pointer": "/coverage/changed_files", "nonempty": True}],
                    "limitations": [{"source": "whole_adoption", "pointer": "/limitations", "nonempty": True},
                                    {"source": "adoption", "pointer": "/limitations", "nonempty": True}]}
        self.refs = refs
        self.protocol = {"authorization": {"path": str(authority), **_info(authority)},
                         "scope": {"source": "whole_adoption", "pointer": "/execution_routing/scope", "equals": "whole_package"},
                         "dimensions": {"business": dimension("/business/status", "accepted"),
                                        "artifact": dimension("/artifact/readback", True),
                                        "calculation": dimension("/calculation/exact", True)}}

    def test_two_original_sources_project_and_release_without_fresh_whole_claim(self):
        reports = projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=self.refs,
                    protocol_map=self.protocol, destination=self.root / "composite-projection")
        projected = read_json(reports["business"])
        self.assertFalse(projected["whole_package"])
        self.assertEqual("explicit_repaired_quality_coverage", projected["scope"])
        self.assertEqual({"whole", "affected"}, set(projected["derived_quality_projection"]["source_groups"].values()))
        quality = {spec["id"]: reports[spec["kind"]] for spec in read_json(self.case.task / "task.json")["reviews"]}
        released = prepare_release(self.store, candidate_id=self.candidate["candidate_id"], source_root=self.case.task,
                                   destination=self.root / "publication-composite/neutral", version="composite-v1", quality_reports=quality)
        report = read_json(Path(released["publication_root"]) / "reports/business.json")
        self.assertFalse(report["whole_package"])
        self.assertEqual(self.accepted["composite_quality_coverage"], report["composite_quality_coverage"])

    def test_composite_mapping_refuses_foreign_source_or_missing_old_dimension(self):
        foreign = self.root / "same-bytes-foreign.json"
        foreign.write_bytes(Path(self.refs["whole_adoption"]["path"]).read_bytes())
        refs = {**self.refs, "whole_adoption": {"path": str(foreign), **_info(foreign)}}
        with self.assertRaisesRegex(TaskflowError, "outside its two"):
            projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=refs,
                                      protocol_map=self.protocol, destination=self.root / "bad-foreign")
        protocol = copy.deepcopy(self.protocol)
        protocol["dimensions"]["business"]["affirmations"] = protocol["dimensions"]["business"]["affirmations"][1:]
        with self.assertRaisesRegex(TaskflowError, "original affirmative"):
            projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=self.refs,
                                      protocol_map=protocol, destination=self.root / "bad-incomplete")


class CompositeExperimentTests(unittest.TestCase):
    def setup_source(self):
        helper = composition_fixtures.CompositeQualityTests("test_exact_private_repair_composes_without_new_whole_signature")
        self.addCleanup(helper.doCleanups)
        value = helper.make_closed_flow(extra=True)
        store, source = value["store"], value["flow"]
        initial = store.get_candidate(store.get_review(value["whole"])["candidate_id"])
        packet = {"task_id": "neutral", "controller": "fixture-controller", "source_flow_id": source["flow_id"],
                  "initial_candidate_id": initial["candidate_id"],
                  "initial_expected_digests": {key: initial[key] for key in ("input_digest", "content_digest", "evaluation_digest")},
                  "business_investment": {"unresolved_business_question": "Independent neutral purpose, never auto inferred."},
                  "quality_and_candidate_gate": {"required": "Exact old whole finding plus fresh affected closure and signed linked changes.",
                                                 "no_implicit_experiment": "Separate attributed purpose is supplied."}}
        authority = value["case"].root / "composite-business.json"
        write_json(authority, packet)
        ref = evidence_ref(authority, "neutral bounded composite-source authorization")
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "composite-experiment",
                      "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
                      "directory": str(value["case"].root / "experiment"), "authorization": ref, "business_packet_ref": ref,
                      "source_flow_id": source["flow_id"], "technical": {"quality_coverage": {
                        "whole_review_id": value["whole"], "recheck_id": value["recheck"], "closed_defect_ids": value["defects"]}}}
        return value, definition, ref

    def test_source_uses_composite_gate_without_rewriting_actual_affected_signature(self):
        value, definition, ref = self.setup_source()
        record = register_lifecycle(value["store"], definition, [ref])
        result = advance_experiment(value["store"], record)
        self.assertEqual("release", result["state"]["stage"], result["state"])
        self.assertNotEqual(value["recheck"], result["state"]["accepted_review_id"])
        self.assertEqual(value["recheck"], value["store"].get_flow(value["flow"]["flow_id"])["state"]["accepted_review_id"])
        self.assertEqual({"adoption", "intake", "issues", "whole_adoption", "whole_intake", "whole_issues"},
                         set(result["state"]["quality_sources"]))
        self.assertEqual(3, len(value["store"].dashboard()["attempts"]))
        self.assertEqual([], value["store"].dashboard()["decisions"])

    def test_source_rejects_recheck_not_equal_to_actual_source_receipt(self):
        value, definition, ref = self.setup_source()
        definition["technical"]["quality_coverage"]["recheck_id"] = value["whole"]
        record = register_lifecycle(value["store"], definition, [ref])
        result = advance_experiment(value["store"], record)
        self.assertEqual("exception", result["state"]["status"])
        self.assertIn("authorized source flow", result["state"]["exception"]["reason"])
        self.assertEqual(value["recheck"], value["store"].get_flow(value["flow"]["flow_id"])["state"]["accepted_review_id"])


class NormativePrefixTests(unittest.TestCase):
    def setUp(self):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        task = read_json(case.task / "task.json")
        for identity, name in (("normative", "draft/evaluation/reference/normal.txt"),
                               ("history", "draft/evaluation/history/old.txt")):
            path = case.task / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Neutral " + identity)
            row = {**task["files"][1], "id": identity, "path": name, "role": "teacher"}
            task["files"].append(row)
        write_json(case.task / "task.json", task)
        add_reviews(case.task)
        self.snapshot = Path(freeze_task(case.task, "prefix-v1")["path"])
        self.declaration = read_json(self.snapshot / "task.json")

    def test_explicit_private_prefix_adds_normative_files_without_history(self):
        review = {"teacher_file_ids": ["answer", "rubric"], "teacher_path_prefixes": ["draft/evaluation/reference/"]}
        paths = _normative_paths(self.snapshot, self.declaration, review)
        self.assertEqual(["draft/evaluation/answer.md", "draft/evaluation/rubric.md", "draft/evaluation/reference/normal.txt"], paths)
        self.assertNotIn("draft/evaluation/history/old.txt", paths)
        self.assertEqual(["draft/evaluation/rubric.md", "draft/evaluation/answer.md"],
                         _normative_paths(self.snapshot, self.declaration, {"teacher_file_ids": ["rubric", "answer"]}))

    def test_unsafe_public_prefix_and_changed_frozen_source_are_rejected(self):
        for prefixes in (["../outside/"], ["draft/evaluation/reference"], ["draft/input/"]):
            with self.subTest(prefixes=prefixes), self.assertRaises(TaskflowError):
                _normative_paths(self.snapshot, self.declaration, {"teacher_file_ids": ["answer"], "teacher_path_prefixes": prefixes})
        (self.snapshot / "draft/evaluation/reference/normal.txt").write_text("Changed frozen normative body")
        with self.assertRaises(TaskflowError):
            _normative_paths(self.snapshot, self.declaration, {"teacher_file_ids": ["answer"], "teacher_path_prefixes": ["draft/evaluation/reference/"]})


if __name__ == "__main__":
    unittest.main()
