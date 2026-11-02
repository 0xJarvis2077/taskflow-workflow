"""Native source transport of blocking whole findings and validated closure.

The quality gate below is an explicit contract double. Store deep graph tests
own defect/repair provenance; this seam uses real captured bytes, neutral role
subprocess outputs and sealed manifests/collections, with zero model calls.
"""

import copy
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow import execution_runner as runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import prepare_release, projected_quality_reports, validate_normative_certification
from taskflow.execution_store import ExecutionStore
from tests import test_execution_formal as fixtures
from tests import test_execution_normative_certification as certificates


def prior_findings_fixture(owner, *, inherited=False):
    case = fixtures.NativeQualityHandoffTests(
        "test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic")
    case.setUp();owner.addCleanup(case.doCleanups)
    old_task = case.root / "prior-task"
    shutil.copytree(case.task, old_task)
    (old_task / "draft/evaluation/rubric.md").write_text("Actual old private standard requiring repair.\n")
    old = capture_candidate(ExecutionStore(case.root / "old.sqlite3"), old_task, "prior-candidate")
    case.candidates[old["candidate_id"]] = old
    prior_root = case.source(old, "whole-findings", coverage="whole_package", outcome="revision_required")
    fresh_root = case.source(case.candidate, "fresh-affected", coverage="affected", outcome="accepted")
    prior = read_json(prior_root / "artifacts/adoption-readiness.json")
    prior["execution_routing"].update(reason="Actual whole findings require the bounded private repair.", blocking_issue_ids=["RAW-DEFECT"])
    for dimension in prior["native_signoff"]["dimensions"].values():dimension["affirmed"] = False
    prior["native_signoff"]["responsibility_analysis"][0]["blocking_issue_ids"] = ["RAW-DEFECT"]
    private = next(row for row in old["files"] if row["role"] == "rubric")
    issue = {"id": "RAW-DEFECT", "kind": "reference", "blocking": True,
             "responsibility": "The declared private support requires an actual bounded correction.",
             "affected_paths": [private["package_path"]],
             "evidence": [{"path": private["package_path"], "sha256": private["sha256"], "location": "line 1"}]}
    binding = {key: prior["execution_routing"][key] for key in
               ("candidate_id", "intent_id", "content_digest", "input_digest", "evaluation_digest")}
    write_json(prior_root / "artifacts/adoption-readiness.json", prior)
    write_json(prior_root / "artifacts/issues.json", {"execution_binding": binding, "issues": [issue]})
    closed = ["closed-defect"] + (["inherited-a", "inherited-b"] if inherited else [])
    fresh = read_json(fresh_root / "artifacts/adoption-readiness.json")
    fresh["execution_routing"].update(covered_defect_ids=closed, reason="Accepted only the actual affected repair and its associations.")
    write_json(fresh_root / "artifacts/adoption-readiness.json", fresh)
    for root in (prior_root, fresh_root):
        (root / "collection.json").unlink();(root / "collection-lock.json").unlink();case.seal(root)
    case.bind_receipt("fresh-review", "fresh-affected")
    graph = {"whole_review_id": "whole-review", "recheck_id": "fresh-review",
        "whole_source": {"candidate_id": old["candidate_id"], "attempt_id": "whole-findings", "sid": case.attempts["whole-findings"]["cli_sid"],
            "report": {"path": str(prior_root / "artifacts/adoption-readiness.json"), **runner._info(prior_root / "artifacts/adoption-readiness.json")},
            "issues": {"path": str(prior_root / "artifacts/issues.json"), **runner._info(prior_root / "artifacts/issues.json")},
            "scope": "whole_package", "whole_package": True, "verdict": "defects"},
        "affected_source": {"candidate_id": case.candidate["candidate_id"], "attempt_id": "fresh-affected", "sid": case.attempts["fresh-affected"]["cli_sid"],
            "report": {"path": str(fresh_root / "artifacts/adoption-readiness.json"), **runner._info(fresh_root / "artifacts/adoption-readiness.json")},
            "issues": {"path": str(fresh_root / "artifacts/issues.json"), **runner._info(fresh_root / "artifacts/issues.json")},
            "scope": "affected", "whole_package": False},
        "input_digest": case.candidate["input_digest"], "closed_defect_ids": closed,
        "original_issue_ids": {"closed-defect": "RAW-DEFECT"},
        "private_changes": [{"path": private["package_path"], "old_sha256": private["sha256"],
                             "new_sha256": next(row["sha256"] for row in case.candidate["files"] if row["package_path"] == private["package_path"])}],
        "source_limits": {"whole": {"scope": "original whole findings, not unchanged positive"}, "affected": {"scope": "actual affected closure"}}}
    if inherited:
        graph.update(coverage_mode="whole_findings_with_inherited_affected_closure_v1",
            inherited_closure_chains=[{"schema_version": 1, "kind": "whole_then_same_public_affected", "defect_id": identity,
                "original_review_id": "previous-" + identity, "repair_review_id": "whole-review", "final_review_id": "fresh-review"}
                for identity in ("inherited-a", "inherited-b")])
    case.current = {"review_id": "composed-current", "attempt_id": "fresh-affected", "whole_package": False,
                    "snapshot_bound_acceptance": True, "composite_quality_coverage": graph}
    return case, graph, prior_root, fresh_root


class NativePriorFindingsTests(unittest.TestCase):
    def setUp(self):
        self.case, self.graph, self.prior_root, self.fresh_root = prior_findings_fixture(self)

    def test_actual_whole_false_findings_remain_original_through_projection_and_release(self):
        case = self.case
        raw = (self.prior_root / "artifacts/adoption-readiness.json").read_bytes()
        handoff = case.handoff()
        self.assertNotIn("candidate_acceptance", handoff["protocol_map"])
        repaired = handoff["protocol_map"]["repaired_candidate_acceptance"]
        prior = repaired["prior_whole_findings"]
        self.assertEqual("whole_package", prior["coverage"])
        self.assertEqual("revision_required", prior["outcome"])
        self.assertEqual(["RAW-DEFECT"], prior["blocking_issue_ids"])
        self.assertTrue(all(row["affirmed"] is False for row in prior["original_dimensions"].values()))
        self.assertEqual("affected", repaired["current_affected_closure"]["coverage"])
        self.assertEqual(["closed-defect"], repaired["current_affected_closure"]["covered_defect_ids"])
        reports = projected_quality_reports(case.store, candidate_id=case.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=case.root / "prior-projections")
        result = read_json(reports["business"])
        self.assertFalse(result["whole_package"])
        self.assertFalse(result["prior_dimension"]["affirmed"])
        self.assertTrue(result["current_dimension"]["affirmed"])
        self.assertEqual("verified_repaired_candidate_acceptance", result["status_basis"])
        self.assertIn("no unchanged-scope affirmation", result["interpretation"])
        quality = {row["id"]: reports[row["kind"]] for row in read_json(case.task / "task.json")["reviews"] if row["required"]}
        release = prepare_release(case.store, candidate_id=case.candidate["candidate_id"], source_root=case.task,
            destination=case.root / "publication/neutral", version="repaired-v1", quality_reports=quality)
        published = read_json(Path(release["publication_root"]) / "reports/business.json")
        self.assertFalse(published["prior_dimension"]["affirmed"])
        self.assertFalse(published["whole_package"])
        self.assertEqual(raw, (self.prior_root / "artifacts/adoption-readiness.json").read_bytes())

    def test_missing_graph_closure_source_or_necessary_association_stays_rejected(self):
        original = copy.deepcopy(self.graph)
        changes = [lambda g: g.update(closed_defect_ids=[]), lambda g: g.update(original_issue_ids={}),
                   lambda g: g["original_issue_ids"].update({"closed-defect": "OTHER"}),
                   lambda g: g["whole_source"].update(scope=None),
                   lambda g: g["whole_source"].update(verdict="passed"),
                   lambda g: g["affected_source"].update(whole_package=True),
                   lambda g: g["affected_source"]["issues"].update(sha256="0" * 64)]
        for change in changes:
            self.graph.clear();self.graph.update(copy.deepcopy(original));change(self.graph)
            with self.subTest(change=change), self.assertRaises(TaskflowError):self.case.handoff()
        self.graph.clear();self.graph.update(original)
        self.case.amend_source(lambda report: report["native_signoff"].update(affected_paths=["draft/evaluation/answer.md"]), self.fresh_root)
        self.graph["affected_source"]["report"].update(runner._info(self.fresh_root / "artifacts/adoption-readiness.json"))
        with self.assertRaisesRegex(TaskflowError, "association"):self.case.handoff()

    def test_no_graph_unclosed_fresh_or_unknown_current_normative_cannot_admit(self):
        original = self.case.current
        self.case.current = {key: value for key, value in original.items() if key != "composite_quality_coverage"}
        with self.assertRaises(TaskflowError):self.case.handoff()
        self.case.current = original
        raw = read_json(self.fresh_root / "artifacts/adoption-readiness.json")
        for change in (lambda r: r["execution_routing"].update(covered_defect_ids=[]),
                       lambda r: r["native_signoff"]["normative"].update(status="needs_business_decision"),
                       lambda r: r["native_signoff"].update(coverage="whole_package")):
            self.case.amend_source(lambda r: r.update(copy.deepcopy(raw)), self.fresh_root)
            self.case.amend_source(change, self.fresh_root)
            self.graph["affected_source"]["report"].update(runner._info(self.fresh_root / "artifacts/adoption-readiness.json"))
            with self.subTest(change=change), self.assertRaises(TaskflowError):self.case.handoff()

    def test_inherited_closure_requires_each_exact_current_anchor(self):
        case, graph, _, _ = prior_findings_fixture(self, inherited=True)
        repaired = case.handoff()["protocol_map"]["repaired_candidate_acceptance"]
        self.assertEqual(2, len(repaired["inherited_closure_chains"]))
        original = copy.deepcopy(graph)
        changes = [lambda g: g.pop("coverage_mode"), lambda g: g["inherited_closure_chains"].pop(),
                   lambda g: g["inherited_closure_chains"][0].update(repair_review_id="foreign"),
                   lambda g: g["inherited_closure_chains"][0].update(final_review_id="foreign"),
                   lambda g: g["inherited_closure_chains"][0].update(schema_version=True)]
        for change in changes:
            graph.clear();graph.update(copy.deepcopy(original));change(graph)
            with self.subTest(change=change), self.assertRaises(TaskflowError):case.handoff()

    def test_offline_normative_source_cannot_relabel_prior_false_or_fresh_scope(self):
        source = self.case
        helper = certificates.NormativeCertificationTests()
        helper.complete_source(source, self.fresh_root)
        certification = helper.certification(source)
        self.assertEqual(certification["native_quality_handoff"]["normative_sources"],
                         validate_normative_certification(candidate=source.candidate, certification=certification))
        for field in ("false", "prior_scope", "fresh_scope", "graph", "private_changes", "anchors"):
            forged = copy.deepcopy(certification)
            native = forged["native_quality_handoff"]
            repaired = native["protocol_map"]["repaired_candidate_acceptance"]
            if field == "false":repaired["prior_whole_findings"]["original_dimensions"]["business"]["affirmed"] = True
            elif field == "prior_scope":repaired["prior_whole_findings"]["coverage"] = "unchanged_scope"
            elif field == "fresh_scope":repaired["current_affected_closure"]["scope"] = "whole_package"
            elif field == "graph":native["certification"]["composite_quality_coverage"]["closed_defect_ids"] = []
            elif field == "private_changes":repaired["private_changes"] = []
            else:repaired["whole_review_id"] = "foreign"
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=source.candidate, certification=forged)

    def test_release_retains_exact_evaluator_empty_record_declaration(self):
        original = fixtures.make_task
        empty = {"kind": "zero_record_jsonl", "reason": "Neutral control has no records.", "evidence": ["S1"]}

        def task_with_empty_record(root):
            task = original(root)
            declaration = read_json(task / "task.json")
            (task / "draft/evaluation/zero.jsonl").write_bytes(b"")
            declaration["files"].append({"id": "EMPTY", "path": "draft/evaluation/zero.jsonl", "role": "teacher",
                                         "empty_content": empty})
            write_json(task / "task.json", declaration)
            return task

        case = fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        with patch.object(fixtures, "make_task", task_with_empty_record):case.setUp()
        self.addCleanup(case.doCleanups)
        first = case.release()
        self.assertEqual(first, case.release())
        self.assertEqual(empty, next(row for row in first["evaluator"]["files"] if row["id"] == "EMPTY")["empty_content"])
        self.assertFalse(any(row["id"] == "EMPTY" for row in first["solver"]["files"]))
        exported = Path(first["evaluator"]["path"]) / "package.json"
        data = read_json(exported)
        next(row for row in data["files"] if row["id"] == "EMPTY")["empty_content"]["reason"] = "Different metadata."
        write_json(exported, data)
        with self.assertRaisesRegex(TaskflowError, "export differs"):
            case.release()


if __name__ == "__main__":unittest.main()
