"""Resume one sealed rubric-role declaration transport without model calls."""

import copy
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import _author, _identity, advance_author
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as author_fixtures
from tests import test_execution_optional_candidate_declaration_transport as transport_fixtures


class DeclarationTransportResumeTests(unittest.TestCase):
    def setUp(self):
        case = author_fixtures.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        case.script.write_text(transport_fixtures.author_script().replace("source,mode=sys.argv[1:]", "source,mode,*_=sys.argv[1:]"))
        case.run_source("candidate_complete")
        self.attempt = self.store.get_attempt(case.attempt["attempt_id"])
        record = case.register()
        advance_lifecycle(self.store, record["flow_id"])
        advance_lifecycle(self.store, record["flow_id"])
        self.record = advance_lifecycle(self.store, record["flow_id"])
        self.source = Path(self.record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        self.target = case.root / "declaration-candidate"
        shutil.copytree(self.source, self.target)
        declaration = read_json(self.target / "task.json")
        next(row for row in declaration["files"] if row["id"] == declaration["scoring"]["rubric_file"])["role"] = "rubric"
        write_json(self.target / "task.json", declaration)
        self.declaration = declaration
        self.spec = {"schema_version": 1, "kind": "referenced_rubric_role_v1",
            "original": evidence_ref(self.source / "task.json", "actual sealed original author declaration"),
            "projected": evidence_ref(self.target / "task.json", "explicit derived rubric role label"), "evidence": case.refs}
        self.resolution = {"action": "accept_declaration_only_candidate_transport", "reason": "Use the already referenced unchanged private rubric.",
                           "declaration_transport": self.spec, "evidence": case.refs}
        self.originals = {path: path.read_bytes() for path in [self.source / "task.json", Path(self.record["state"]["receipt"]),
            *[case.runtime / name for name in ("manifest.json", "request.md", "collection.json", "collection-lock.json", "trace.jsonl")]]}

    def resume(self, resolution=None):
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("resume cannot dispatch")):
            return resume_path_transport(self.store, self.record["flow_id"], resolution or self.resolution)

    def rewritten(self, declaration):
        write_json(self.target / "task.json", declaration)
        spec = {**self.spec, "projected": evidence_ref(self.target / "task.json", "explicit derived rubric role label")}
        return {**self.resolution, "declaration_transport": spec}

    def downstream(self):
        flow_id = "declaration-experiment"
        authority = self.case.root / "downstream-authority.json"
        item = {"task_id": "neutral", "source_author_quality_flow": self.record["flow_id"],
            "source_author_intent": self.attempt["intent_id"], "source_author_attempt": self.attempt["attempt_id"],
            "source_author_sid": self.attempt["cli_sid"], "source_business_packet": self.case.refs[0]["path"],
            "source_business_packet_sha256": self.case.refs[0]["sha256"], "first_experiment_business_unknown": "Neutral authorized future question.",
            "version_id": "neutral-v1", "first_run_id": "neutral-01", "first_evaluation_id": "neutral-eval-01",
            "conditional_one_confirmation_run_id": "neutral-02", "conditional_one_confirmation_evaluation_id": "neutral-eval-02"}
        write_json(authority, {"schema_version": 1, "controller": "fixture-controller", "items": [item],
            "preauthorized_normal_chain": "Existing bounded chain only.", "prerequisites": ["Actual independent quality required."], "confirmation": {}})
        ref = evidence_ref(authority, "explicit neutral downstream authority")
        record = self.store.register_flow(flow_id, {"schema_version": 1, "flow_id": flow_id, "kind": "experiment_v1",
            "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1", "authorization": ref,
            "business_packet_ref": ref, "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": self.record["flow_id"]},
            "source_author_flow_id": self.record["flow_id"], "directory": str(self.case.root / "experiment"),
            "technical": {"formal_runtime_root": str(self.case.root / "formal")}}, [ref],
            {"status": "exception", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [],
             "exception": {"stage": "experiment_v1", "reason": "source author-quality flow stopped with its original exception: " + str(self.record["state"]["exception"])}})
        record = advance_lifecycle(self.store, record["flow_id"])
        resolution = {"action": "resume_resolved_declaration_dependency", "reason": "Wait on the same technically resumed source.", "evidence": [ref]}
        return record, resolution

    def test_explicit_resume_preserves_original_actor_then_normal_handler_captures_same_author(self):
        resumed = self.resume()
        self.assertEqual("active", resumed["state"]["status"])
        self.assertEqual("author_handoff", resumed["state"]["stage"])
        self.assertEqual(self.spec, resumed["state"]["optional_candidate_declaration_transport"])
        self.assertEqual(self.record["definition"], resumed["definition"])
        self.assertEqual(self.attempt, self.store.get_attempt(self.attempt["attempt_id"]))
        self.assertNotIn("candidate_id", resumed["state"])
        captured = advance_author(self.store, resumed)
        self.assertEqual("public_prepare", captured["state"]["stage"])
        candidate = self.store.get_candidate(captured["state"]["candidate_id"])
        self.assertEqual(self.attempt["attempt_id"], candidate["author_attempt"])
        self.assertEqual([], candidate["external_author_sids"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        for path, content in self.originals.items():
            self.assertEqual(content, path.read_bytes())

    def test_legacy_wrong_link_extra_role_new_payload_public_change_and_foreign_source_reject(self):
        for action in ("accept_path_only_candidate_transport", "accept_unsigned_author_placeholders"):
            with self.subTest(action=action), self.assertRaises(TaskflowError):
                self.resume({**self.resolution, "action": action})
        mutations = [lambda value: value["scoring"].update(rubric_file="answer"),
                     lambda value: value["files"][0].update(role="teacher"),
                     lambda value: value["files"].append({"id": "extra", "path": "checks/extra.txt", "role": "teacher"})]
        (self.target / "checks").mkdir(exist_ok=True)
        (self.target / "checks/extra.txt").write_text("Not original payload")
        for mutation in mutations:
            declaration = copy.deepcopy(self.declaration)
            mutation(declaration)
            with self.subTest(mutation=mutation), self.assertRaises(TaskflowError):
                self.resume(self.rewritten(declaration))
        self.rewritten(self.declaration)
        public = self.target / "draft/input/prompt.md"
        content = public.read_bytes()
        public.write_bytes(content + b"Changed public requirement")
        with self.assertRaisesRegex(TaskflowError, "payload bytes"):
            self.resume()
        public.write_bytes(content)
        foreign = self.case.root / "foreign-task.json"
        shutil.copyfile(self.source / "task.json", foreign)
        for spec in ({**self.spec, "original": evidence_ref(foreign, "foreign same-byte source")},
                     {key: value for key, value in self.spec.items() if key != "original"}):
            with self.subTest(spec=spec), self.assertRaises(TaskflowError):
                self.resume({**self.resolution, "declaration_transport": spec})
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))

    def test_nonterminal_signed_or_changed_metadata_successor_and_business_hold_reject(self):
        actual = _author(self.store, self.record["definition"])
        with patch("taskflow.execution_author._author", return_value=({**actual[0], "state": "running"}, actual[1])):
            with self.assertRaisesRegex(TaskflowError, "same completed original"):
                self.resume()
        report = self.target / self.declaration["reviews"][0]["path"]
        original_report = report.read_bytes()
        for changes in ({"status": "passed"}, {"reason": "Changed unsigned metadata"}):
            write_json(report, {**read_json(self.source / self.declaration["reviews"][0]["path"]), **changes})
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                self.resume()
        report.write_bytes(original_report)
        self.store.register_flow(_identity(self.record["flow_id"], "quality-flow"), {}, self.case.refs,
                                 {"status": "active", "stage": "quality", "current_work_id": None})
        with self.assertRaisesRegex(TaskflowError, "existing candidate or public/quality"):
            self.resume()
        blocked = self.store.register_flow("business-held-composition", {"kind": "experiment_v1"}, self.case.refs,
            {"status": "exception", "stage": "experiment_v1", "pending_business_scope": {"reason": "Fresh public responsibility remains unsigned."}})
        with self.assertRaisesRegex(TaskflowError, "pending business scope"):
            resume_path_transport(self.store, blocked["flow_id"], {"action": "resume_validated_quality_composition",
                "reason": "Technical composition cannot clear business hold.", "evidence": self.case.refs})
        self.assertEqual(blocked, self.store.get_flow(blocked["flow_id"]))

    def test_downstream_resumes_same_initial_stage_and_only_waits_for_existing_quality(self):
        downstream, resolution = self.downstream()
        receipt = Path(downstream["state"]["receipt"])
        original = receipt.read_bytes()
        self.resume()
        attempts = copy.deepcopy(self.store.dashboard()["attempts"])
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("no dispatch")):
            resumed = resume_path_transport(self.store, downstream["flow_id"], resolution)
            waiting = advance_lifecycle(self.store, resumed["flow_id"])
        self.assertEqual("active", waiting["state"]["status"])
        self.assertEqual("experiment_v1", waiting["state"]["stage"])
        self.assertEqual("source_author_quality_not_received", waiting["state"]["waiting"]["reason"])
        self.assertIsNone(waiting["state"]["current_work_id"])
        self.assertNotIn("accepted_review_id", waiting["state"])
        self.assertNotIn("candidate_id", waiting["state"])
        self.assertEqual(attempts, self.store.dashboard()["attempts"])
        self.assertEqual(original, receipt.read_bytes())
        self.assertEqual(downstream["definition"], resumed["definition"])

    def test_downstream_rejects_unresolved_parent_drift_existing_intent_and_other_exception(self):
        downstream, resolution = self.downstream()
        with self.assertRaisesRegex(TaskflowError, "technically resumed active"):
            resume_path_transport(self.store, downstream["flow_id"], resolution)
        self.resume()
        path = self.target / "task.json"
        original = path.read_bytes()
        path.write_bytes(original + b" ")
        with self.assertRaises(TaskflowError):
            resume_path_transport(self.store, downstream["flow_id"], resolution)
        path.write_bytes(original)
        intent = self.case.root / "formal/neutral-01.formal-intent.json"
        intent.parent.mkdir()
        intent.write_text("Existing formal intent must remain unchanged.")
        with self.assertRaisesRegex(TaskflowError, "existing solver"):
            resume_path_transport(self.store, downstream["flow_id"], resolution)
        self.assertEqual(downstream, self.store.get_flow(downstream["flow_id"]))
        intent.unlink()
        changed = {**downstream["state"], "exception": {"stage": "experiment_v1", "reason": "A different source business issue."}}
        self.store.update_flow(downstream["flow_id"], downstream["revision"], changed, {"kind": "neutral different exception"})
        with self.assertRaisesRegex(TaskflowError, "one exact resolution"):
            resume_path_transport(self.store, downstream["flow_id"], resolution)

