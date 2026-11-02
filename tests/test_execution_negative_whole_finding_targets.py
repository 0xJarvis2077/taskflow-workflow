"""Retain explicit whole negative finding targets without restamping a pass.

Native ledger identity is the named contract double from the adjacent suite;
source/candidate files, original role output subprocess, request and collection
seals are real neutral fixtures. No model/provider call is made.
"""

import copy
import shutil
import unittest

from taskflow.contracts import TaskflowError, read_json
from taskflow.execution_formal import (
    _whole_finding_locations, projected_quality_reports, validate_normative_certification,
)
from taskflow import execution_runner as runner
from tests import test_execution_native_prior_findings as prior_fixtures
from tests import test_execution_normative_certification as certificates


def negative_whole_targets_fixture(owner):
    case, graph, prior_root, fresh_root = prior_fixtures.prior_findings_fixture(owner)
    raw = read_json(prior_root / "artifacts/adoption-readiness.json")
    targets = list(read_json(prior_root / "artifacts/issues.json")["issues"][0]["affected_paths"])
    def original_finding_locations(report):
        report["native_signoff"].update(affected_paths=targets,
            coverage_basis="Whole is the actual reviewed scope; these paths locate blocking findings, not exclusions or a pass.")
        report["native_signoff"]["dimensions"]["artifact"]["affirmed"] = True
    case.amend_source(original_finding_locations, prior_root)
    graph["whole_source"]["report"].update(runner._info(prior_root / "artifacts/adoption-readiness.json"))
    return case, graph, prior_root, fresh_root, targets


class NegativeWholeFindingTargetsTests(unittest.TestCase):
    def setUp(self):
        self.case, self.graph, self.prior_root, self.fresh_root, self.targets = negative_whole_targets_fixture(self)

    def amend(self, change):
        self.case.amend_source(change, self.prior_root)
        self.graph["whole_source"]["report"].update(runner._info(self.prior_root / "artifacts/adoption-readiness.json"))

    def test_original_whole_negative_targets_are_preserved_in_certificate_and_projection(self):
        raw = (self.prior_root / "artifacts/adoption-readiness.json").read_bytes()
        native = self.case.handoff()
        prior = native["protocol_map"]["repaired_candidate_acceptance"]["prior_whole_findings"]
        self.assertEqual("whole_package", prior["coverage"])
        self.assertEqual("revision_required", prior["outcome"])
        self.assertEqual(self.targets, prior["affected_paths"])
        self.assertEqual(read_json(self.prior_root / "artifacts/adoption-readiness.json")["native_signoff"]["dimensions"],
                         prior["original_dimensions"])
        self.assertFalse(prior["original_dimensions"]["business"]["affirmed"])
        self.assertTrue(prior["original_dimensions"]["artifact"]["affirmed"])
        witness = prior["finding_locations"]
        self.assertEqual("original_negative_whole_finding_locations", witness["kind"])
        self.assertEqual(self.targets, witness["actual_value"])
        self.assertEqual("/native_signoff/affected_paths", witness["pointer"])
        self.assertEqual(prior, native["certification"]["whole"]["prior_whole_findings"])
        for row in witness["targets"]:
            self.assertEqual(row["source"]["path"], row["input"]["source"])
            self.assertEqual({k: row["source"][k] for k in ("sha256", "size")},
                             {k: row["staged"][k] for k in ("sha256", "size")})
            self.assertEqual("/issues/0/affected_paths", row["issue_targets"][0]["pointer"])
        reports = projected_quality_reports(self.case.store, candidate_id=self.case.candidate["candidate_id"],
            source_refs=native["source_refs"], protocol_map=native["protocol_map"], destination=self.case.root / "projections")
        projected = read_json(reports["business"])
        self.assertFalse(projected["whole_package"])
        self.assertFalse(projected["prior_dimension"]["affirmed"])
        self.assertTrue(projected["current_dimension"]["affirmed"])
        self.assertEqual(prior, projected["repaired_candidate_acceptance"]["prior_whole_findings"])
        self.assertEqual(raw, (self.prior_root / "artifacts/adoption-readiness.json").read_bytes())

    def test_wrong_targets_unknown_coverage_or_positive_whole_paths_are_rejected(self):
        original = read_json(self.prior_root / "artifacts/adoption-readiness.json")
        for change in (lambda r: r["native_signoff"].update(affected_paths=["draft/evaluation/answer.md"]),
                       lambda r: r["native_signoff"].update(affected_paths=self.targets + self.targets),
                       lambda r: r["native_signoff"].update(coverage="whole"),
                       lambda r: r["execution_routing"].update(outcome="accepted"),
                       lambda r: [row.update(affirmed=True) for row in r["native_signoff"]["dimensions"].values()]):
            self.amend(lambda r: r.update(copy.deepcopy(original)))
            self.amend(change)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.case.handoff()

    def test_original_unclosed_or_mismatched_finding_graph_cannot_be_accepted(self):
        original = copy.deepcopy(self.graph)
        for change in (lambda g: g.update(closed_defect_ids=[]),
                       lambda g: g["original_issue_ids"].update({"closed-defect": "UNSIGNED"}),
                       lambda g: g["whole_source"].update(whole_package=False),
                       lambda g: g["affected_source"].update(whole_package=True)):
            self.graph.clear();self.graph.update(copy.deepcopy(original));change(self.graph)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.case.handoff()

    def test_target_source_must_be_the_original_captured_payload_and_staged_bytes(self):
        case = self.case
        old = case.candidates[self.graph["whole_source"]["candidate_id"]]
        manifest = runner._validate(self.prior_root)
        report = read_json(self.prior_root / "artifacts/adoption-readiness.json")
        issues = read_json(self.prior_root / "artifacts/issues.json")["issues"]
        refs = {"adoption": self.graph["whole_source"]["report"], "issues": self.graph["whole_source"]["issues"]}
        forged = copy.deepcopy(manifest)
        row = next(item for item in forged["inputs"] if item["target"] == self.targets[0])
        foreign = case.root / "same-byte-decoy.txt"
        shutil.copyfile(row["source"], foreign)
        row["source"] = str(foreign)
        with self.assertRaisesRegex(TaskflowError, "captured candidate payload"):
            _whole_finding_locations(report, self.targets, forged, self.prior_root, refs, issues, old)
        staged = self.prior_root / "inputs" / self.targets[0]
        staged.chmod(0o644)
        staged.write_bytes(staged.read_bytes() + b"drift")
        with self.assertRaises(TaskflowError):
            self.case.handoff()

    def test_offline_rebuild_keeps_raw_scope_targets_and_rejects_forged_transport(self):
        helper = certificates.NormativeCertificationTests()
        helper.complete_source(self.case, self.fresh_root)
        certificate = helper.certification(self.case)
        self.assertEqual(certificate["native_quality_handoff"]["normative_sources"],
                         validate_normative_certification(candidate=self.case.candidate, certification=certificate))
        for field in ("paths", "pointer", "source", "false", "coverage", "closure"):
            forged = copy.deepcopy(certificate)
            native = forged["native_quality_handoff"]
            prior = native["protocol_map"]["repaired_candidate_acceptance"]["prior_whole_findings"]
            if field == "paths":prior["affected_paths"] = []
            elif field == "pointer":prior["finding_locations"]["pointer"] = "/execution_routing/scope"
            elif field == "source":prior["finding_locations"]["targets"][0]["source"]["sha256"] = "0" * 64
            elif field == "false":prior["original_dimensions"]["business"]["affirmed"] = True
            elif field == "coverage":prior["coverage"] = "unchanged_scope"
            else:native["certification"]["composite_quality_coverage"]["closed_defect_ids"] = []
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=self.case.candidate, certification=forged)

    def test_legacy_empty_whole_targets_do_not_gain_the_new_witness(self):
        case, _, _, _ = prior_fixtures.prior_findings_fixture(self)
        old = case.handoff()["protocol_map"]["repaired_candidate_acceptance"]["prior_whole_findings"]
        self.assertNotIn("finding_locations", old)
        self.assertNotIn("affected_paths", old)
        self.assertNotIn("coverage_basis", old)


if __name__ == "__main__":
    unittest.main()
