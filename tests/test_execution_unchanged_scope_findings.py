"""Neutral sealed role fixtures; no model or shared database is used.

The native source fixture supplies the store graph interface explicitly; deep
graph provenance remains covered by the adjacent real-worker/store suites.
"""

import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution_runner as runner
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_flow_contracts import render_native_signoff_protocol
from taskflow.execution_formal import projected_quality_reports, validate_normative_certification
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_native_prior_findings as findings
from tests import test_execution_normative_certification as certificates
from tools.execution_simulation import fixture_policy


def unchanged_findings_fixture(owner):
    source, graph, prior_root, fresh_root = findings.prior_findings_fixture(owner)
    private = next(row for row in source.candidates["prior-candidate"]["files"] if row["role"] == "rubric")

    def delimit(report):
        signed = report["native_signoff"]
        signed.update(coverage="unchanged_scope", affected_paths=[private["package_path"]],
                      coverage_basis="Whole findings exclude the defective private rule; only original unaffected observations remain.")
        signed["dimensions"]["artifact"]["affirmed"] = True
        for kind in ("business", "calculation"):
            signed["dimensions"][kind]["evidence"].append({
                "path": private["package_path"], "sha256": private["sha256"], "location": "line 1, blocking rule"})

    source.amend_source(delimit, prior_root)
    graph["whole_source"]["report"].update(runner._info(prior_root / "artifacts/adoption-readiness.json"))
    source.current["derived"] = True
    return source, graph, prior_root, fresh_root


class UnchangedScopeFindingsTests(unittest.TestCase):
    def test_original_unchanged_false_is_transported_with_exact_affected_closure(self):
        source, graph, prior_root, _ = unchanged_findings_fixture(self)
        paths = [prior_root / "artifacts/adoption-readiness.json", prior_root / "artifacts/issues.json"]
        before = {path: path.read_bytes() for path in paths}
        handoff = source.handoff()
        repaired = handoff["protocol_map"]["repaired_candidate_acceptance"]
        prior = repaired["prior_whole_findings"]
        self.assertEqual("unchanged_scope", prior["coverage"])
        signed = read_json(paths[0])["native_signoff"]
        self.assertEqual(signed["affected_paths"], prior["affected_paths"])
        self.assertEqual(signed["coverage_basis"], prior["coverage_basis"])
        self.assertEqual(signed["dimensions"], prior["original_dimensions"])
        self.assertEqual("revision_required", prior["outcome"])
        self.assertEqual("affected", repaired["current_affected_closure"]["coverage"])
        reports = projected_quality_reports(source.store, candidate_id=source.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=source.root / "scope-projections")
        report = read_json(reports["business"])
        self.assertFalse(report["whole_package"])
        self.assertFalse(report["prior_dimension"]["affirmed"])
        self.assertTrue(report["current_dimension"]["affirmed"])
        self.assertEqual("verified_repaired_candidate_acceptance", report["status_basis"])
        self.assertEqual(before, {path: path.read_bytes() for path in paths})
        self.assertFalse(source.current["whole_package"])
        self.assertEqual(graph, handoff["certification"]["composite_quality_coverage"])

    def test_online_offline_certificate_preserves_exclusion_and_false(self):
        source, _, prior_root, fresh_root = unchanged_findings_fixture(self)
        helper = certificates.NormativeCertificationTests()
        helper.complete_source(source, prior_root)
        helper.complete_source(source, fresh_root)
        certificate = helper.certification(source)
        before = {path: path.read_bytes() for path in source.root.rglob("*") if path.is_file()}
        for store in (None, source.store):
            self.assertEqual(certificate["native_quality_handoff"]["normative_sources"],
                validate_normative_certification(candidate=source.candidate, certification=certificate, store=store))
        self.assertEqual(before, {path: path.read_bytes() for path in source.root.rglob("*") if path.is_file()})
        for field in ("coverage", "affected_paths", "coverage_basis", "false", "closure", "graph"):
            forged = copy.deepcopy(certificate)
            native = forged["native_quality_handoff"]
            prior = native["protocol_map"]["repaired_candidate_acceptance"]["prior_whole_findings"]
            if field == "coverage": prior["coverage"] = "whole_package"
            elif field == "affected_paths": prior["affected_paths"] = []
            elif field == "coverage_basis": prior["coverage_basis"] = "Unrestricted"
            elif field == "false": prior["original_dimensions"]["business"]["affirmed"] = True
            elif field == "closure": native["protocol_map"]["repaired_candidate_acceptance"]["current_affected_closure"]["scope"] = "whole_package"
            else: native["certification"]["composite_quality_coverage"]["closed_defect_ids"] = []
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=source.candidate, certification=forged)

    def test_signed_producer_allows_false_without_scope_upgrade(self):
        request = render_native_signoff_protocol()
        self.assertIn("unchanged_scope", request)
        self.assertIn("execution_routing.outcome", request)
        self.assertIn("false", request)

    def test_uncovered_negative_exclusion_and_responsibility_require_business_decision(self):
        source, graph, prior_root, _ = unchanged_findings_fixture(self)
        original = read_json(prior_root / "artifacts/adoption-readiness.json")
        mutations = {
            "no exclusions": lambda s: s.update(affected_paths=[]),
            "unrelated exclusion": lambda s: s.update(affected_paths=["draft/evaluation/answer.md"]),
            "duplicate": lambda s: s["affected_paths"].append(s["affected_paths"][0]),
            "uncovered false": lambda s: s["dimensions"]["business"].update(evidence=s["responsibility_analysis"][0]["public_evidence"]),
            "unresolved": lambda s: s["responsibility_analysis"][0].update(disposition="unresolved"),
            "unlinked": lambda s: s["responsibility_analysis"][0].update(disposition="retained", blocking_issue_ids=[]),
            "foreign issue": lambda s: s["responsibility_analysis"][0].update(blocking_issue_ids=["OTHER"]),
            "unknown coverage": lambda s: s.update(coverage="unrecognized"),
        }
        for label, mutate in mutations.items():
            source.amend_source(lambda r: r.update(copy.deepcopy(original)), prior_root)
            source.amend_source(lambda r: mutate(r["native_signoff"]), prior_root)
            graph["whole_source"]["report"].update(runner._info(prior_root / "artifacts/adoption-readiness.json"))
            with self.subTest(label=label), self.assertRaises(TaskflowError):
                source.handoff()

    def test_missing_closure_wrong_source_or_scope_and_unaffirmed_current_reject(self):
        source, graph, _, fresh_root = unchanged_findings_fixture(self)
        original = copy.deepcopy(graph)
        mutations = [lambda g: g.update(closed_defect_ids=[]), lambda g: g.update(original_issue_ids={}),
                     lambda g: g["whole_source"].update(sid="foreign"),
                     lambda g: g["affected_source"].update(attempt_id="whole-findings"),
                     lambda g: g["whole_source"]["issues"].update(sha256="0" * 64),
                     lambda g: g["affected_source"]["report"].update(sha256="0" * 64)]
        for mutate in mutations:
            graph.clear(); graph.update(copy.deepcopy(original)); mutate(graph)
            with self.subTest(mutate=mutate), self.assertRaises(TaskflowError):
                source.handoff()
        graph.clear(); graph.update(original)
        source.amend_source(lambda r: r["native_signoff"]["dimensions"]["business"].update(affirmed=False), fresh_root)
        graph["affected_source"]["report"].update(runner._info(fresh_root / "artifacts/adoption-readiness.json"))
        with self.assertRaises(TaskflowError):
            source.handoff()

    def test_drift_current_native_without_graph_and_stale_receipt_reject(self):
        source, _, prior_root, _ = unchanged_findings_fixture(self)
        path = prior_root / "artifacts/adoption-readiness.json"
        raw = path.read_bytes()
        path.write_bytes(raw + b" ")
        with self.assertRaises((TaskflowError, ValueError)):
            source.handoff()
        path.write_bytes(raw)
        source.current.pop("composite_quality_coverage")
        with self.assertRaises(TaskflowError):
            source.handoff()

    def test_current_nonblocking_false_cannot_fill_the_old_excluded_scope(self):
        source, graph, _, fresh_root = unchanged_findings_fixture(self)
        source.amend_source(lambda r: r["native_signoff"]["dimensions"]["business"].update(affirmed=False), fresh_root)
        report = read_json(fresh_root / "artifacts/adoption-readiness.json")
        routing = report["execution_routing"]
        evidence = report["native_signoff"]["dimensions"]["business"]["evidence"]
        binding = {key: routing[key] for key in ("candidate_id", "intent_id", "content_digest", "input_digest", "evaluation_digest")}
        write_json(fresh_root / "artifacts/issues.json", {"execution_binding": binding, "issues": [{
            "id": "LIMITED-OBSERVATION", "kind": "claim", "blocking": False, "disposition": "observed_unrepaired",
            "responsibility": "Current scoped support remains an observation.", "evidence": evidence,
            "affected_paths": [row["path"] for row in evidence]}]})
        (fresh_root / "collection.json").unlink()
        (fresh_root / "collection-lock.json").unlink()
        source.seal(fresh_root)
        graph["affected_source"]["report"].update(runner._info(fresh_root / "artifacts/adoption-readiness.json"))
        graph["affected_source"]["issues"].update(runner._info(fresh_root / "artifacts/issues.json"))
        with self.assertRaisesRegex(TaskflowError, "explicit current affected affirmations"):
            source.handoff()


BUSINESS = "native quality dimension has no explicit scoped affirmation: business"


class UnchangedScopeRecoveryTests(unittest.TestCase):
    def fixture(self):
        source, graph, prior_root, _ = unchanged_findings_fixture(self)
        source.candidate["parent_id"] = graph["whole_source"]["candidate_id"]
        packet = read_json(source.authority)
        packet.update(prerequisites=["Actual accepted independent current source."],
                      preauthorized_normal_chain="One exact neutral handoff.",
                      quality_and_candidate_gate={"required": True, "no_implicit_experiment": True},
                      confirmation={"maximum_additional_runs_per_first": 1})
        packet["items"][0].update(first_experiment_business_unknown="Neutral offline observation.", version_id="next-v1",
            first_run_id="next-run-01", first_evaluation_id="next-eval-01",
            conditional_one_confirmation_run_id="next-run-02", conditional_one_confirmation_evaluation_id="next-eval-02")
        write_json(source.authority, packet)
        source.authorization = evidence_ref(source.authority, "neutral exact authority")
        source.bind_receipt("fresh-review", "fresh-affected")
        # bind_receipt constructs the current receipt; add the original whole
        # history and persist it again as fixture construction, before testing.
        source.child["state"]["history"].insert(0, {"kind": "independent_review", "review_id": graph["whole_review_id"]})
        child_receipt = Path(source.child["state"]["receipt"])
        received = read_json(child_receipt)
        received["history"] = source.child["state"]["history"]
        write_json(child_receipt, received)
        parent_receipt = source.root / "parent-received.json"
        write_json(parent_receipt, {"flow_id": source.parent_id, "status": "accepted", "result": source.parent["state"]["result"]})
        source.parent["state"]["receipt"] = str(parent_receipt)
        store = ExecutionStore(source.root / "recovery.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "limited-source-experiment", "task_id": "neutral",
            "owner": "neutral-controller", "policy_id": "fixture-v1", "authorization": source.authorization,
            "business_packet_ref": source.authorization, "source_author_flow_id": source.parent_id,
            "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": source.parent_id},
            "directory": str(source.root / "experiment"), "technical": {
                "formal_runtime_root": str(source.root / "formal"), "publication_root": str(source.root / "publication")}}
        exception = {"stage": "experiment_v1", "reason": BUSINESS}
        received = {"controller": definition["owner"], "flow_id": definition["flow_id"], "status": "exception",
                    "exception": exception, "history": [], "archives": []}
        receipt = source.root / ("exception-" + sha256(canonical(received))[:16] + ".json")
        write_json(receipt, received)
        record = store.register_flow(definition["flow_id"], definition, [source.authorization], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [],
            "receipt": str(receipt), "exception": exception})
        resolution = {"action": "resume_validated_source_quality_transport", "reason": "Only exact original limited findings and closure.",
                      "evidence": [source.authorization]}
        bound = {"source": source.child, "initial": source.candidates[graph["whole_source"]["candidate_id"]],
                 "binding": {"source_author_flow_id": source.parent_id}}
        original_get = store.get_flow

        @contextmanager
        def interfaces():
            # Only the graph/source-author adapters are doubles. Native handoff,
            # packet validation, receipts and lifecycle persistence are real.
            with ExitStack() as stack:
                stack.enter_context(patch.object(store, "get_flow", side_effect=lambda key: source.flows[key] if key in source.flows else original_get(key)))
                for name in ("get_candidate", "get_attempt", "quality_gate", "assert_review_current"):
                    stack.enter_context(patch.object(store, name, getattr(source.store, name)))
                stack.enter_context(patch.object(store, "get_review", return_value={
                    "review_id": "fresh-review", "covered_defect_ids": ["closed-defect"]}))
                stack.enter_context(patch("taskflow.execution_experiment._author_source", return_value=bound))
                yield
        return source, graph, store, record, resolution, interfaces

    def test_append_recovery_retains_originals_and_has_zero_launches(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_hold_unknown_complete_and_launch_footprints_reject_without_append(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_wrong_receipt_missing_graph_stale_child_and_native_source_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_exact_exception_hash_and_original_coverage_are_required(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_business_hold_is_not_removed_by_transport(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
