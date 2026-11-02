"""Preserve explicit reference/rubric observations; no model or permission claims.

The existing fixture provides native-shaped metadata and real neutral
subprocess output. It is a schema/transport fixture, not a real independent
model signature or a proof of native sandbox permissions.
"""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_flow_contracts import (
    NATIVE_SIGNOFF_PROTOCOL_SHA256, _nonblocking_observation, parse_review,
    request_has_native_signoff_protocol,
)
from taskflow.execution_formal import prepare_release, projected_quality_reports
from taskflow import execution_runner as runner
from tests import test_execution_native_observation_acceptance as fixtures


PROTOCOL_SHA256 = "7db19b34c646c80c0f6d8b8a101d5b111f154bec650f792453da48e7c16b197b"


class ObservationPredicateTests(unittest.TestCase):
    def test_explicit_private_observations_and_legacy_claims_keep_their_conditions(self):
        for kind in ("reference", "rubric"):
            row = {"id": "OBS", "kind": kind, "blocking": False, "status": "open_nonblocking",
                   "nonblocking_reason": "A concrete bounded observation, not a repair requirement."}
            before = copy.deepcopy(row)
            self.assertTrue(_nonblocking_observation(row, []))
            self.assertEqual(before, row)
            for changes in ({"blocking": True}, {"blocking": 0}, {"blocking": "false"},
                            {"status": "open"}, {"nonblocking_reason": " "}, {"nonblocking_reason": []},
                            {"needs_business_decision": True}, {"disposition": "unresolved"},
                            {"kind": "material"}, {"kind": "unknown"}, {"id": ""}):
                with self.subTest(kind=kind, changes=changes):
                    self.assertFalse(_nonblocking_observation({**row, **changes}, []))
            for key in ("blocking", "status", "nonblocking_reason"):
                missing = {name: value for name, value in row.items() if name != key}
                self.assertFalse(_nonblocking_observation(missing, []))
            self.assertFalse(_nonblocking_observation(row, ["OBS"]))
        self.assertTrue(_nonblocking_observation({"id": "OLD", "kind": "claim"}, []))
        self.assertTrue(_nonblocking_observation({"id": "OLD", "kind": "claim", "blocking": False}, []))
        self.assertFalse(_nonblocking_observation({"id": "OLD", "kind": "claim", "blocking": True}, []))
        for invalid in (None, [], {}, {"id": None, "kind": "claim"}):
            self.assertFalse(_nonblocking_observation(invalid, []))


class TypedObservationTransportTests(unittest.TestCase):
    def source(self, kind):
        source = fixtures.native_observation_fixture(self)
        issues = read_json(source.material / "artifacts/issues.json")
        row = issues["issues"][0]
        row.pop("disposition")
        row.update(kind=kind, blocking=False, status="open_nonblocking",
                   nonblocking_reason="The candidate is explicitly accepted; this bounded private observation remains open.",
                   affected_paths=["draft/evaluation/answer.md"])
        basis = next(item for item in source.candidate["files"] if item["package_path"] == "draft/evaluation/answer.md")
        row["evidence"].append({"path": basis["package_path"], "sha256": basis["sha256"], "location": "line 1"})
        self.reseal(source, issues=issues)
        return source

    def reseal(self, source, *, report=None, issues=None):
        root = source.material
        if report is not None:
            write_json(root / "artifacts/adoption-readiness.json", report)
        if issues is not None:
            write_json(root / "artifacts/issues.json", issues)
        (root / "collection.json").unlink()
        (root / "collection-lock.json").unlink()
        source.seal(root)

    def parse(self, source, *, aliases=None):
        manifest = runner._validate(source.material)
        if aliases is None:
            aliases = {entry["target"]: {"path": entry["source"], "sha256": entry["sha256"], "kind": entry["kind"]}
                       for entry in manifest["inputs"] if entry["target"].startswith("actual-public-work/")}
        return parse_review(source.material / "artifacts/adoption-readiness.json", source.material / "artifacts/issues.json",
                            candidate=source.candidate, intent_id=manifest["intent_id"], scope="whole_package",
                            observed_aliases=aliases)

    def test_reference_and_rubric_preserve_kind_open_status_false_and_original_bytes(self):
        for kind in ("reference", "rubric"):
            with self.subTest(kind=kind):
                source = self.source(kind)
                root = source.material
                paths = [root / name for name in ("manifest.json", "manifest-lock.json", "request.md", "collection.json",
                         "collection-lock.json", "artifacts/adoption-readiness.json", "artifacts/issues.json")]
                original = {path: path.read_bytes() for path in paths}
                original_issue = read_json(root / "artifacts/issues.json")["issues"][0]
                parsed = self.parse(source)
                self.assertEqual("accepted", parsed["outcome"])
                self.assertEqual([], parsed["defects"])
                self.assertEqual([], parsed["covered_defect_ids"])
                handoff = source.handoff()
                acceptance = handoff["protocol_map"]["candidate_acceptance"]
                self.assertFalse(acceptance["original_dimensions"]["calculation"]["affirmed"])
                self.assertEqual(original_issue, acceptance["observations"][0]["actual_value"])
                self.assertEqual(kind, acceptance["observations"][0]["actual_value"]["kind"])
                self.assertEqual("open_nonblocking", acceptance["observations"][0]["actual_value"]["status"])
                self.assertEqual([], acceptance["blocking_issue_ids"])
                reports = projected_quality_reports(source.store, candidate_id=source.candidate["candidate_id"],
                    source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=source.root / "projected")
                calculation = read_json(reports["calculation"])
                self.assertFalse(calculation["original_dimension"]["affirmed"])
                self.assertEqual("candidate_routing_acceptance", calculation["status_basis"])
                self.assertEqual(acceptance["observations"], calculation["nonblocking_observations"])
                self.assertEqual(acceptance["original_dimensions"]["calculation"]["limitations"],
                                 calculation["original_dimension"]["limitations"])
                released = prepare_release(source.store, candidate_id=source.candidate["candidate_id"], source_root=source.task,
                    destination=source.root / "publication/neutral", version="neutral-v1", quality_reports=reports)
                published = read_json(Path(released["publication_root"]) / "reports/calculation.json")
                self.assertFalse(published["original_dimension"]["affirmed"])
                self.assertEqual(acceptance["observations"], published["nonblocking_observations"])
                self.assertEqual(original, {path: path.read_bytes() for path in original})
                self.assertEqual(PROTOCOL_SHA256, NATIVE_SIGNOFF_PROTOCOL_SHA256)
                self.assertTrue(request_has_native_signoff_protocol((root / "request.md").read_bytes()))

    def test_missing_reason_status_explicit_false_or_business_hold_cannot_use_observed_citations(self):
        source = self.source("reference")
        original = read_json(source.material / "artifacts/issues.json")
        variants = [{"status": "open"}, {"nonblocking_reason": ""}, {"nonblocking_reason": "  "},
                    {"blocking": True}, {"needs_business_decision": True}, {"disposition": "unresolved"},
                    {"kind": "material"}, {"kind": "unknown"}]
        for change in variants:
            issues = copy.deepcopy(original)
            issues["issues"][0].update(change)
            self.reseal(source, issues=issues)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.parse(source)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                source.handoff()
        for key in ("blocking", "status", "nonblocking_reason"):
            issues = copy.deepcopy(original)
            issues["issues"][0].pop(key)
            self.reseal(source, issues=issues)
            with self.subTest(missing=key), self.assertRaises(TaskflowError):
                self.parse(source)

    def test_observed_bytes_cannot_become_private_observation_targets_or_blocking_findings(self):
        for kind in ("reference", "rubric"):
            source = self.source(kind)
            original_issues = read_json(source.material / "artifacts/issues.json")
            report = read_json(source.material / "artifacts/adoption-readiness.json")
            alias = original_issues["issues"][0]["evidence"][0]["path"]
            issues = copy.deepcopy(original_issues)
            issues["issues"][0]["affected_paths"] = [alias]
            self.reseal(source, issues=issues)
            with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "not repair targets"):
                self.parse(source)
            report["execution_routing"].update(outcome="revision_required", blocking_issue_ids=[original_issues["issues"][0]["id"]])
            self.reseal(source, report=report, issues=original_issues)
            with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "Observed evidence"):
                self.parse(source)
            with self.assertRaises(TaskflowError):
                source.handoff()

    def test_observed_citation_cannot_replace_current_candidate_or_bound_metadata_basis(self):
        for kind in ("reference", "rubric"):
            source = self.source(kind)
            issues = read_json(source.material / "artifacts/issues.json")
            issues["issues"][0]["evidence"] = [row for row in issues["issues"][0]["evidence"]
                                               if row["path"].startswith("actual-public-work/")]
            self.assertTrue(issues["issues"][0]["evidence"])
            self.reseal(source, issues=issues)
            with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "supplemental"):
                self.parse(source)
            with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "supplemental"):
                source.handoff()

    def test_source_sha_stage_bytes_and_alias_permissions_remain_strict(self):
        source = self.source("rubric")
        original = read_json(source.material / "artifacts/issues.json")
        issues = copy.deepcopy(original)
        issues["issues"][0]["evidence"][0]["sha256"] = "0" * 64
        self.reseal(source, issues=issues)
        with self.assertRaises(TaskflowError):
            self.parse(source)
        self.reseal(source, issues=original)
        manifest = runner._validate(source.material)
        entry = next(row for row in manifest["inputs"] if row["target"].startswith("actual-public-work/"))
        for kind in ("teacher", "public", "source_metadata"):
            aliases = {entry["target"]: {"path": entry["source"], "sha256": entry["sha256"], "kind": kind}}
            with self.subTest(alias_kind=kind), self.assertRaises(TaskflowError):
                self.parse(source, aliases=aliases)
        original_path = Path(entry["source"])
        original_path.write_bytes(original_path.read_bytes() + b"changed after seal")
        with self.assertRaises(TaskflowError):
            source.handoff()
        source = self.source("reference")
        entry = next(row for row in runner._validate(source.material)["inputs"] if row["target"].startswith("actual-public-work/"))
        staged = source.material / "inputs" / entry["target"]
        staged.write_bytes(staged.read_bytes() + b"changed staged bytes")
        with self.assertRaises(TaskflowError):
            source.handoff()

    def test_false_dimension_requires_original_limits_and_evidence_link(self):
        source = self.source("rubric")
        original = read_json(source.material / "artifacts/adoption-readiness.json")
        for change in ("limitations", "evidence"):
            report = copy.deepcopy(original)
            dimension = report["native_signoff"]["dimensions"]["calculation"]
            dimension[change] = ([] if change == "limitations"
                                 else report["native_signoff"]["dimensions"]["business"]["evidence"])
            self.reseal(source, report=report)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                source.handoff()


if __name__ == "__main__":
    unittest.main()
