"""False native dimensions remain false in an accepted, nonblocking transport.

The source has native-shaped, byte-sealed metadata and actual neutral subprocess
output; this does not claim native permission proof or an external model call.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow import execution_runner as runner
from taskflow.execution_formal import (
    prepare_release, projected_quality_reports, validate_normative_certification,
)
from tests import test_execution_formal as fixtures
from tests import test_execution_normative_certification as certificate_fixtures
from tests.helpers import make_task


def observation_source(source):
    """Attach a complete, explicitly nonblocking observation to a neutral source."""
    root = source.material
    original = source.root / "public-observation.txt"
    original.write_text("A supporting prose total differs from its unchanged data table.\n", encoding="utf-8")
    alias = "actual-public-work/support.txt"
    staged = root / "inputs" / alias
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(original.read_bytes())
    manifest = read_json(root / "manifest.json")
    manifest["inputs"].append({"source": str(original), "target": alias, "kind": "submission", **runner._info(staged)})
    write_json(root / "manifest.json", manifest)
    write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
    report = read_json(root / "artifacts/adoption-readiness.json")
    report["execution_routing"]["reason"] = "Current candidate accepted; supporting public observation retained without repair."
    citation = {"path": alias, "sha256": runner._info(original)["sha256"], "location": "line 1"}
    dimension = report["native_signoff"]["dimensions"]["calculation"]
    dimension.update(affirmed=False, evidence=[citation],
                     limitations=["No blanket affirmation of the supporting public prose; current candidate remains accepted."])
    issue = {"id": "OBS-ONE", "kind": "claim", "blocking": False, "disposition": "observed_unrepaired",
             "responsibility": "Keep the supporting explanation consistent with its complete data.",
             "affected_paths": [alias], "evidence": [citation],
             "finding": "Neutral supporting prose discrepancy remains unchanged.",
             "business_consequence": "The actual independent candidate acceptance remains explicitly separate."}
    binding = {key: report["execution_routing"][key] for key in
               ("candidate_id", "intent_id", "content_digest", "input_digest", "evaluation_digest")}
    write_json(root / "artifacts/adoption-readiness.json", report)
    write_json(root / "artifacts/issues.json", {"execution_binding": binding, "blocking_issue_ids": [], "issues": [issue]})
    (root / "collection.json").unlink();(root / "collection-lock.json").unlink()
    source.seal(root)
    return {"alias": alias, "issue": issue, "report": report}


def native_observation_fixture(owner):
    """Return the existing isolated fixture class with all three declared reviews."""
    def three_reviews(root, task_id="neutral", total=7):
        task = make_task(root, task_id, total)
        declaration = read_json(task / "task.json")
        declaration["reviews"].append({"id": "calculation", "kind": "calculation",
                                       "path": "reports/calculation.json", "required": True})
        write_json(task / "task.json", declaration)
        return task
    source = fixtures.NativeQualityHandoffTests(
        "test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic")
    with patch("tests.test_execution_formal.make_task", side_effect=three_reviews):
        source.setUp()
    owner.addCleanup(source.doCleanups)
    observation_source(source)
    return source


class NativeObservationAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.source = native_observation_fixture(self)

    def rewrite(self, report=None, issues=None):
        root = self.source.material
        if report is not None:
            write_json(root / "artifacts/adoption-readiness.json", report)
        if issues is not None:
            write_json(root / "artifacts/issues.json", issues)
        (root / "collection.json").unlink();(root / "collection-lock.json").unlink()
        self.source.seal(root)

    def test_false_dimension_is_preserved_through_handoff_projection_and_release(self):
        source = self.source
        raw = (source.material / "artifacts/adoption-readiness.json").read_bytes()
        handoff = source.handoff()
        acceptance = handoff["protocol_map"]["candidate_acceptance"]
        self.assertFalse(acceptance["original_dimensions"]["calculation"]["affirmed"])
        self.assertEqual("accepted", acceptance["outcome"])
        self.assertEqual([], acceptance["blocking_issue_ids"])
        self.assertEqual("observed_unrepaired", acceptance["observations"][0]["actual_value"]["disposition"])
        self.assertFalse(handoff["protocol_map"]["dimensions"]["calculation"]["affirmations"][0]["equals"])
        self.assertEqual(acceptance, handoff["certification"]["fresh"]["candidate_acceptance"])
        reports = projected_quality_reports(source.store, candidate_id=source.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=source.root / "projected")
        report = read_json(reports["calculation"])
        self.assertEqual("passed", report["status"])
        self.assertEqual("candidate_routing_acceptance", report["status_basis"])
        self.assertFalse(report["original_dimension"]["affirmed"])
        self.assertEqual(acceptance["observations"], report["nonblocking_observations"])
        self.assertIn("does not assert a true dimension", report["interpretation"])
        destination = source.root / "publication/neutral"
        released = prepare_release(source.store, candidate_id=source.candidate["candidate_id"],
            source_root=source.task, destination=destination, version="neutral-v1", quality_reports=reports)
        published = read_json(destination / "reports/calculation.json")
        self.assertFalse(published["original_dimension"]["affirmed"])
        self.assertEqual("candidate_routing_acceptance", published["status_basis"])
        self.assertEqual(report, published["original_report"])
        self.assertEqual(acceptance["observations"], published["nonblocking_observations"])
        self.assertEqual(source.candidate["input_digest"], released["solver"]["input_digest"])
        self.assertEqual(raw, (source.material / "artifacts/adoption-readiness.json").read_bytes())

    def test_blocking_unresolved_or_unbound_observations_cannot_use_the_new_branch(self):
        original_report = read_json(self.source.material / "artifacts/adoption-readiness.json")
        original_issues = read_json(self.source.material / "artifacts/issues.json")
        cases = [
            (lambda r, i: r["execution_routing"].update(blocking_issue_ids=["OBS-ONE"])),
            (lambda r, i: i.update(blocking_issue_ids=["OBS-ONE"])),
            (lambda r, i: i["issues"][0].update(blocking=True)),
            (lambda r, i: i["issues"][0].pop("blocking")),
            (lambda r, i: i["issues"][0].update(disposition="unresolved")),
            (lambda r, i: i["issues"][0].update(kind="reference")),
            (lambda r, i: i["issues"][0].update(needs_business_decision=True)),
            (lambda r, i: i["execution_binding"].update(candidate_id="foreign")),
            (lambda r, i: i["issues"][0]["evidence"][0].update(sha256="0" * 64)),
            (lambda r, i: i.update(issues=[])),
            (lambda r, i: r["native_signoff"]["responsibility_analysis"][0].update(disposition="unresolved")),
            (lambda r, i: r["native_signoff"]["normative"].update(status="needs_business_decision")),
            (lambda r, i: r["native_signoff"]["dimensions"]["calculation"].update(affirmed="false")),
            (lambda r, i: r["native_signoff"]["dimensions"]["calculation"].update(limitations=[])),
            (lambda r, i: r["native_signoff"]["dimensions"]["calculation"].update(
                evidence=r["native_signoff"]["dimensions"]["business"]["evidence"])),
        ]
        for change in cases:
            report, issues = copy.deepcopy(original_report), copy.deepcopy(original_issues)
            change(report, issues);self.rewrite(report, issues)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.source.handoff()

    def test_online_and_offline_certificates_revalidate_false_and_observations(self):
        source = self.source
        helper = certificate_fixtures.NormativeCertificationTests()
        helper.complete_source(source, source.material)
        certification = helper.certification(source)
        with patch("subprocess.run", side_effect=AssertionError("validation must remain readonly")):
            offline = validate_normative_certification(candidate=source.candidate, certification=certification)
            online = validate_normative_certification(candidate=source.candidate, certification=certification, store=source.store)
        self.assertEqual(offline, online)
        for field in ("false", "observation", "selector"):
            forged = copy.deepcopy(certification)
            protocol = forged["native_quality_handoff"]["protocol_map"]
            if field == "false":
                protocol["candidate_acceptance"]["original_dimensions"]["calculation"]["affirmed"] = True
            elif field == "observation":
                protocol["candidate_acceptance"]["observations"] = []
            else:
                protocol["dimensions"]["calculation"]["affirmations"][0]["equals"] = True
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=source.candidate, certification=forged)

    def test_tampered_projection_or_source_collection_cannot_drop_the_observation(self):
        source = self.source
        handoff = source.handoff()
        reports = projected_quality_reports(source.store, candidate_id=source.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=source.root / "projected")
        report = read_json(reports["calculation"])
        report["original_dimension"]["affirmed"] = True
        write_json(reports["calculation"], report)
        with self.assertRaisesRegex(TaskflowError, "source chain"):
            prepare_release(source.store, candidate_id=source.candidate["candidate_id"], source_root=source.task,
                            destination=source.root / "tampered/neutral", version="neutral-v1", quality_reports=reports)
        path = source.material / "artifacts/issues.json"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaises(TaskflowError):
            source.handoff()


if __name__ == "__main__":
    unittest.main()
