"""Source-bound unsigned alias dependency recovery; no model calls."""

import copy
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import advance_author
from taskflow.execution_lifecycle import _declaration_transport_reviews, advance_lifecycle, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as author_fixtures
from tests import test_execution_declaration_transport_resume as declaration_fixtures
from tests import test_execution_optional_candidate_report_layout as layout_fixtures


class UnsignedDependencyResumeTests(unittest.TestCase):
    downstream = declaration_fixtures.DeclarationTransportResumeTests.downstream

    def setUp(self):
        case = author_fixtures.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        case.script.write_text(case.script.read_text().replace("'status':'inconclusive'", "'status':'passed'"))
        case.run_source("candidate_complete")
        record = case.register()
        advance_lifecycle(self.store, record["flow_id"])
        advance_lifecycle(self.store, record["flow_id"])
        self.record = advance_lifecycle(self.store, record["flow_id"])
        self.attempt = self.store.get_attempt(case.attempt["attempt_id"])
        self.material = self.store._validated_optional_candidate_delivery(self.attempt)
        self.source = Path(self.material["candidate_root"])
        self.target = case.root / "unsigned-alias"
        shutil.copytree(self.source, self.target)
        for row in read_json(self.target / "task.json")["reviews"]:
            source = self.source / row["path"]
            write_json(self.target / row["path"], {"schema_version": 1, "review_id": row["id"], "kind": row["kind"],
                "status": "inconclusive", "task_id": "neutral", "independent_quality_passed": False, "new_formal_score": None,
                "local_checks": {"original_author_report": read_json(source), "original_relative_path": row["path"],
                                 "original_sha256": sha256(source.read_bytes())}})
        self.resolution = {"action": "accept_unsigned_author_placeholders", "reason": "Preserve the unsigned original in a source-bound wrapper.",
                           "candidate_root": str(self.target), "evidence": case.refs}

    def resume_parent(self):
        return resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def resume_child(self, record, resolution):
        return resume_path_transport(self.store, record["flow_id"], {**resolution, "action": "resume_resolved_author_preparation_dependency"})

    def test_actual_alias_preserves_full_raw_report_and_only_restores_downstream_wait(self):
        downstream, resolution = self.downstream()
        originals = {path: path.read_bytes() for path in [self.source / "task.json", Path(self.record["state"]["receipt"]),
                     *[self.source / row["path"] for row in read_json(self.source / "task.json")["reviews"]]]}
        self.resume_parent()
        result = self.resume_child(downstream, resolution)
        waiting = advance_lifecycle(self.store, result["flow_id"])
        self.assertEqual("active", waiting["state"]["status"])
        self.assertEqual("source_author_quality_not_received", waiting["state"]["waiting"]["reason"])
        self.assertNotIn("accepted_review_id", waiting["state"])
        self.assertIsNone(waiting["state"]["current_work_id"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(self.attempt, self.store.get_attempt(self.attempt["attempt_id"]))
        for path, raw in originals.items():
            self.assertEqual(raw, path.read_bytes())

    def test_changed_raw_json_locator_sha_or_payload_cannot_resume_dependency(self):
        downstream, resolution = self.downstream()
        self.resume_parent()
        report = self.target / "reports/business.json"
        wrapper = read_json(report)
        mutations = [lambda local: local.update(original_sha256="0" * 64),
                     lambda local: local.update(original_relative_path="reports/artifact.json"),
                     lambda local: local["original_author_report"].update(status="inconclusive")]
        for mutate in mutations:
            changed = copy.deepcopy(wrapper)
            mutate(changed["local_checks"])
            write_json(report, changed)
            with self.subTest(mutate=mutate), self.assertRaisesRegex(TaskflowError, "exact original author report"):
                self.resume_child(downstream, resolution)
        write_json(report, wrapper)
        public = self.target / "draft/input/prompt.md"
        public.write_bytes(public.read_bytes() + b"Changed public bytes")
        with self.assertRaisesRegex(TaskflowError, "payload byte"):
            self.resume_child(downstream, resolution)
        self.assertEqual(downstream, self.store.get_flow(downstream["flow_id"]))

    def test_only_explicit_unsigned_resolution_and_typed_layout_transport_are_admitted(self):
        downstream, resolution = self.downstream()
        parent = self.resume_parent()
        state = copy.deepcopy(parent["state"])
        state["history"][-1]["resolution"]["action"] = "accept_path_only_candidate_transport"
        self.store.update_flow(parent["flow_id"], parent["revision"], state, {"kind": "neutral unrelated resolution"})
        with self.assertRaisesRegex(TaskflowError, "explicit optional"):
            self.resume_child(downstream, resolution)
        # This isolates lifecycle consumption of the store validator's typed
        # return. The store's layout/source/byte validation has its own tests.
        relocated = self.target / "draft/evaluation/original-business.json"
        shutil.copyfile(self.source / "reports/business.json", relocated)
        report = self.target / "reports/business.json"
        projection = {"definition": {"kind": "declared_unsigned_report_layout_v1"},
            "source_candidate_root": str(self.source), "projection": {"unsigned_reports": [{
                "original": evidence_ref(self.source / "reports/business.json", "actual raw source"),
                "relocated": evidence_ref(relocated, "same raw bytes retained"),
                "wrapper": evidence_ref(report, "unsigned wrapper"), "original_json_preserved": True}]}}
        material = {"candidate_root": str(self.target), "declaration_transport": projection}
        _declaration_transport_reviews(material, "neutral")
        projection["projection"]["unsigned_reports"][0]["original_json_preserved"] = False
        with self.assertRaisesRegex(TaskflowError, "original unsigned report preservation"):
            _declaration_transport_reviews(material, "neutral")


class LayoutDependencyResumeTests(unittest.TestCase):
    downstream = declaration_fixtures.DeclarationTransportResumeTests.downstream

    def test_real_layout_validator_resumes_parent_and_dependency_without_upgrading_report(self):
        case = author_fixtures.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        case.script.write_text(layout_fixtures.source_script().replace("source,mode=sys.argv[1:]", "source,mode,*_=sys.argv[1:]"))
        case.run_source("candidate_complete")
        self.attempt = self.store.get_attempt(case.attempt["attempt_id"])
        record = case.register()
        advance_lifecycle(self.store, record["flow_id"])
        advance_lifecycle(self.store, record["flow_id"])
        self.record = advance_lifecycle(self.store, record["flow_id"])
        source = Path(self.record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        target = case.root / "unsigned-layout"
        spec = layout_fixtures.project_unsigned_reports(source, target, case.refs)
        downstream, resolution = self.downstream()
        parent = resume_path_transport(self.store, self.record["flow_id"], {
            "action": "accept_declaration_only_candidate_transport", "reason": "Keep three original unsigned reports and explicit zero-record metadata.",
            "declaration_transport": spec, "evidence": case.refs})
        self.assertEqual(self.attempt, self.store.get_attempt(self.attempt["attempt_id"]))
        resumed = resume_path_transport(self.store, downstream["flow_id"], resolution)
        self.assertEqual("active", resumed["state"]["status"])
        self.assertEqual("experiment_v1", resumed["state"]["stage"])
        transport = resumed["state"]["history"][-1]["declaration_transport"]
        self.assertEqual(3, len(transport["projection"]["unsigned_reports"]))
        self.assertEqual(1, len(transport["projection"]["empty_content"]))
        captured = advance_author(self.store, parent)
        self.assertEqual("public_prepare", captured["state"]["stage"])
        self.assertEqual(self.attempt["attempt_id"], self.store.get_candidate(captured["state"]["candidate_id"])["author_attempt"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        for row in transport["projection"]["unsigned_reports"]:
            original = read_json(Path(row["original"]["path"]))
            wrapper = read_json(Path(row["wrapper"]["path"]))
            self.assertEqual(original, wrapper["local_checks"]["original_author_report"])
            self.assertEqual("inconclusive", wrapper["status"])
            self.assertFalse(wrapper["independent_quality_passed"])

