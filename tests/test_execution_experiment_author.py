"""A shared authority binds each experiment to one actual author-quality result."""

import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _native_handoff, _native_normative, _packet, _source, advance_experiment
from taskflow.execution_lifecycle import advance_lifecycle
from taskflow.execution_store import evidence_ref
from taskflow.execution_runner import _info
from tests import test_execution_author as author_fixtures
from tests import test_execution_composite_quality as composite_fixtures
from tools.execution_simulation import perform


class AuthorExperimentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def definition(self, case, source):
        attempt = case.attempt
        original = source["definition"]["authorization"]
        item = {"task_id": "neutral", "source_author_quality_flow": source["flow_id"],
                "source_author_intent": attempt["intent_id"], "source_author_attempt": attempt["attempt_id"],
                "source_author_sid": attempt["cli_sid"], "source_business_packet": original["path"],
                "source_business_packet_sha256": original["sha256"],
                "first_experiment_business_unknown": "A new neutral whole-job observation; no model in this fixture.",
                "version_id": "new-v1", "first_run_id": "new-run-01", "first_evaluation_id": "new-eval-01",
                "conditional_one_confirmation_run_id": "new-run-02",
                "conditional_one_confirmation_evaluation_id": "new-eval-02"}
        other = {**item, "task_id": "other-topic", "source_author_quality_flow": "other-author-flow"}
        path = self.root / "shared-business.json"
        write_json(path, {"schema_version": 1, "controller": "fixture-controller", "items": [item, other],
                          "prerequisites": ["Actual complete candidate and independently signed quality."],
                          "preauthorized_normal_chain": "Retain the exact neutral whole-job observation chain.",
                          "confirmation": {"maximum_additional_runs_per_first": 1, "only_if": "Signed original<=3/7 with exact eligible evidence."}})
        ref = evidence_ref(path, "neutral explicit multi-item authority, no real model authorization")
        return {"schema_version": 1, "kind": "experiment_v1", "flow_id": "next-experiment", "task_id": "neutral",
                "owner": "fixture-controller", "policy_id": "fixture-v1", "authorization": ref,
                "business_packet_ref": ref, "source_author_flow_id": source["flow_id"],
                "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": source["flow_id"]},
                "directory": str(self.root / "downstream"),
                "technical": {"independent_review": {"teacher_file_ids": ["answer", "rubric"]}}}

    def author(self, disposition="research_ended", terminal=True):
        case = author_fixtures.AuthorSuccessorTests("test_research_ended_receives_full_archive_without_new_work")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.run_source(disposition)
        source = case.register()
        if terminal:
            for _ in range(3):
                source = advance_lifecycle(case.store, source["flow_id"])
        return case, source

    def register(self, case, definition):
        return case.store.register_flow(definition["flow_id"], definition, [definition["authorization"]], {
            "status": "active", "stage": "experiment_v1", "current_work_id": None,
            "history": [], "archives": [], "waiting": None})

    def candidate_source(self):
        case, source = self.author("candidate_complete", terminal=False)
        source = advance_lifecycle(case.store, source["flow_id"])
        source = advance_lifecycle(case.store, source["flow_id"])
        initial = case.store.get_candidate(source["state"]["candidate_id"])
        public = perform(case.store, case.root / "actual-public", initial["candidate_id"], "quality", "fresh-public", {
            "public.md": "Actual neutral public reconstruction, not real business quality."})
        quality = perform(case.store, case.root / "actual-quality", initial["candidate_id"], "quality", "fresh-quality", {
            "adoption-readiness.json": '{"execution_routing":{"scope":"whole_package","outcome":"accepted"}}',
            "intake.md": "Actual neutral whole signature fixture.", "issues.json": '{"issues":[]}'})
        review = case.store.record_review("current-whole", initial["candidate_id"], quality["attempt_id"],
            verdict="accepted", scope="whole neutral fixture", evidence=[evidence_ref(
                Path(quality["runtime_path"]) / "artifacts/adoption-readiness.json", "actual neutral source")], whole_package=True)
        child_state = {"status": "accepted", "candidate_id": initial["candidate_id"],
                       "candidate_root": source["state"]["candidate_root"], "accepted_review_id": review["review_id"],
                       "history": [{"review_id": review["review_id"], "cli_sid": quality["cli_sid"]}]}
        child_receipt = case.root / "child-result.json"
        write_json(child_receipt, {"flow_id": "actual-child", "controller": "fixture-controller", **child_state})
        child_state["receipt"] = str(child_receipt)
        case.store.register_flow("actual-child", {"task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
            "initial": {"candidate_id": initial["candidate_id"], "expected_digests": {key: initial[key] for key in (
                "input_digest", "content_digest", "evaluation_digest")}, "predecessor_attempt": public["attempt_id"],
                "predecessor_binding": {key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}}}, case.refs, child_state)
        state = {**source["state"], "status": "accepted", "stage": "child_quality", "current_work_id": None,
                 "public_attempt_id": public["attempt_id"], "child_flow_id": "actual-child", "result": {
                     "outcome": "candidate_quality_accepted", "candidate_id": initial["candidate_id"],
                     "candidate_root": child_state["candidate_root"], "accepted_review_id": review["review_id"],
                     "child_flow_id": "actual-child", "child_receipt": str(child_receipt)}}
        source = case.store.update_flow(source["flow_id"], source["revision"], state, {"kind": "neutral_signed_fixture_received"})
        source = advance_lifecycle(case.store, source["flow_id"])
        return case, source, initial

    def native_interface_fixture(self, store, record, candidate_id):
        # The native signoff protocol has separate formal-helper tests. This
        # fixture checks only its interface with actual author/quality binding.
        current = store.quality_gate(candidate_id)
        refs = {}
        groups = [("", current["attempt_id"])]
        if current.get("composite_quality_coverage"):
            groups.append(("whole_", current["composite_quality_coverage"]["whole_source"]["attempt_id"]))
        for prefix, identity in groups:
            root = Path(store.get_attempt(identity)["runtime_path"]) / "artifacts"
            refs.update({prefix + key: evidence_ref(root / name, "actual neutral interface source") for key, name in (
                ("adoption", "adoption-readiness.json"), ("intake", "intake.md"), ("issues", "issues.json"))})
        return {"source_refs": refs, "protocol_map": {"nature": "test_fixture_interface"},
                "normative_sources": [], "scoring_source_allowlist": [], "certification": {"nature": "test_fixture_interface"}}

    def test_active_author_quality_waits_idempotently_and_other_item_does_not_block(self):
        case, source = self.author(terminal=False)
        definition = self.definition(case, source)
        other = copy.deepcopy(definition)
        other.update(flow_id="other-experiment", task_id="other-topic", source_author_flow_id="other-author-flow")
        other["business_packet_item"] = {"task_id": "other-topic", "source_author_quality_flow": "other-author-flow"}
        self.register(case, other)
        record = self.register(case, definition)
        count = len(case.store.dashboard()["work"])
        record = advance_experiment(case.store, record)
        self.assertEqual("source_author_quality_not_received", record["state"]["waiting"]["reason"])
        again = advance_experiment(case.store, record)
        self.assertEqual(record["revision"], again["revision"])
        self.assertEqual(count, len(case.store.dashboard()["work"]))
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))

    def test_research_ended_receives_original_result_without_target_work(self):
        case, source = self.author()
        record = advance_experiment(case.store, self.register(case, self.definition(case, source)))
        self.assertEqual("accepted", record["state"]["status"])
        self.assertEqual("research_ended", record["state"]["result"]["outcome"])
        self.assertEqual("not_dispatched", record["state"]["result"]["formal_experiment"])
        self.assertEqual(source["flow_id"], record["state"]["author_source_binding"]["source_author_flow_id"])
        self.assertEqual(1, len(case.store.dashboard()["work"]))
        self.assertEqual(record, advance_experiment(case.store, record))

    def test_inflight_author_sid_is_not_required_before_original_collection(self):
        case, source = self.author(terminal=False)
        definition = self.definition(case, source)
        record = self.register(case, definition)
        # Actual process/collection behavior is tested by the author fixtures;
        # this checks the store's normal pre-collection missing-SID shape.
        with patch("taskflow.execution_author._author", return_value=({**case.attempt,
                   "state": "running", "cli_sid": None}, {})):
            result = advance_experiment(case.store, record)
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("source_author_quality_not_received", result["state"]["waiting"]["reason"])
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))

    def test_substantive_author_decision_keeps_specific_exception(self):
        case, source = self.author("needs_business_decision")
        record = advance_experiment(case.store, self.register(case, self.definition(case, source)))
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("Bounded neutral research outcome", record["state"]["exception"]["reason"])
        self.assertEqual(1, len(case.store.dashboard()["work"]))

    def test_actual_complete_candidate_binds_exact_whole_quality_child(self):
        case, source, initial = self.candidate_source()
        definition = self.definition(case, source)
        normalized = _packet({"definition": definition})
        self.assertEqual("new-eval-01", normalized["independent_review"]["score_id"])
        self.assertEqual(["answer", "rubric"], normalized["independent_review"]["teacher_file_ids"])
        with patch("taskflow.execution_experiment._native_handoff", side_effect=self.native_interface_fixture):
            record = advance_experiment(case.store, self.register(case, definition))
        self.assertEqual("release", record["state"]["stage"], record["state"])
        self.assertEqual(initial["candidate_id"], record["state"]["candidate_id"])
        self.assertEqual("current-whole", record["state"]["accepted_review_id"])
        self.assertEqual("actual-child", record["state"]["author_source_binding"]["child_flow_id"])
        self.assertIn(case.attempt["cli_sid"], record["state"]["excluded_sids"])
        self.assertEqual(3, len(case.store.dashboard()["attempts"]))

    def test_wrong_selector_ambiguous_item_and_foreign_author_sid_are_rejected(self):
        case, source = self.author()
        definition = self.definition(case, source)
        wrong = copy.deepcopy(definition)
        wrong["business_packet_item"]["source_author_quality_flow"] = "foreign-flow"
        with self.assertRaisesRegex(TaskflowError, "selector"):
            _packet({"definition": wrong})
        path = Path(definition["authorization"]["path"])
        packet = read_json(path)
        packet["items"].append(copy.deepcopy(packet["items"][0]))
        write_json(path, packet)
        ref = evidence_ref(path, "neutral exact authority")
        definition.update(authorization=ref, business_packet_ref=ref)
        with self.assertRaisesRegex(TaskflowError, "ambiguous"):
            _packet({"definition": definition})
        packet["items"].pop()
        packet["items"][0]["source_author_sid"] = "foreign-sid"
        write_json(path, packet)
        ref = evidence_ref(path, "neutral exact authority")
        definition.update(authorization=ref, business_packet_ref=ref)
        record = advance_experiment(case.store, self.register(case, definition))
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("attempt/intent/SID", record["state"]["exception"]["reason"])

    def test_duplicate_same_item_cannot_consume_under_another_lifecycle(self):
        case, source = self.author()
        definition = self.definition(case, source)
        self.register(case, {**definition, "flow_id": "duplicate-experiment"})
        record = advance_experiment(case.store, self.register(case, definition))
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("packet item is already registered", record["state"]["exception"]["reason"])
        self.assertEqual(1, len(case.store.dashboard()["work"]))

    def test_child_receipt_cannot_substitute_another_candidate(self):
        case, source, _ = self.candidate_source()
        definition = self.definition(case, source)
        path = Path(source["state"]["result"]["child_receipt"])
        receipt = read_json(path)
        receipt["candidate_id"] = "foreign-candidate"
        write_json(path, receipt)
        record = advance_experiment(case.store, self.register(case, definition))
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("child receipt differs", record["state"]["exception"]["reason"])
        self.assertEqual(3, len(case.store.dashboard()["attempts"]))

    def test_fresh_affected_child_composes_only_its_actual_initial_whole_findings(self):
        case = composite_fixtures.CompositeQualityTests("test_exact_private_repair_composes_without_new_whole_signature")
        value = case.make_closed_flow()
        self.addCleanup(case.doCleanups)
        store, source = value["store"], value["flow"]
        initial = store.get_candidate(source["definition"]["initial"]["candidate_id"])
        definition = {"flow_id": "future-neutral-experiment", "source_author_flow_id": "actual-author-flow",
                      "task_id": "neutral", "technical": {}, "authorization": value["refs"][0]}
        record = store.register_flow(definition["flow_id"], definition, value["refs"], {
            "status": "active", "stage": "experiment_v1", "history": [], "archives": [], "waiting": None})
        bound = {"source": source, "initial": initial, "author_sids": {"actual-source-author"},
                 "binding": {"source_author_flow_id": "actual-author-flow"}}
        with patch("taskflow.execution_experiment._author_source", return_value=bound), \
             patch("taskflow.execution_experiment._native_handoff", side_effect=self.native_interface_fixture):
            result = _source(store, record, {"quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}})
        review = store.get_review(result["state"]["accepted_review_id"])
        self.assertFalse(review["whole_package"])
        self.assertEqual(value["recheck"], review["composite_quality_coverage"]["recheck_id"])
        self.assertEqual(value["whole"], review["composite_quality_coverage"]["whole_review_id"])
        self.assertEqual({"adoption", "intake", "issues", "whole_adoption", "whole_intake", "whole_issues"},
                         set(result["state"]["quality_sources"]))
        self.assertEqual(value["recheck"], store.get_flow(source["flow_id"])["state"]["accepted_review_id"])

    def test_native_certification_is_rechecked_without_replacing_original_binding(self):
        value = {"source_refs": {}, "protocol_map": {"native": True}, "scoring_source_allowlist": ["draft/evaluation/answer.md"]}
        record = {"definition": {"source_author_flow_id": "exact-author-flow", "authorization": {"fixture": True}},
                  "state": {"native_quality_handoff": value}}
        with patch("taskflow.execution_formal.native_quality_handoff", return_value=copy.deepcopy(value), create=True) as helper:
            self.assertEqual(value, _native_handoff(object(), record, "exact-candidate"))
            self.assertEqual("exact-author-flow", helper.call_args.kwargs["source_flow_id"])
            helper.return_value["scoring_source_allowlist"].append("draft/evaluation/old-history.md")
            with self.assertRaisesRegex(TaskflowError, "differs from the bound original"):
                _native_handoff(object(), record, "exact-candidate")
        self.assertEqual(["draft/evaluation/answer.md"], value["scoring_source_allowlist"])

    def test_exact_native_normative_files_match_frozen_bytes_and_exclude_public(self):
        private, public = "draft/evaluation/answer.md", "draft/input/brief.md"
        for name in (private, public):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Neutral exact reference." if name == private else "Public task.")
        declaration = {"files": [{"path": private, "role": "answer"}, {"path": public, "role": "input"}]}
        signed = {"scoring_source_allowlist": [private], "normative_sources": [{"package_path": private,
                  "path": str(self.root / private), "role": "answer", **_info(self.root / private)}]}
        self.assertEqual([private], _native_normative(self.root, declaration, signed))
        bad = copy.deepcopy(signed)
        bad["normative_sources"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(TaskflowError, "frozen normative source differs"):
            _native_normative(self.root, declaration, bad)
        bad = {"scoring_source_allowlist": [public], "normative_sources": [{"package_path": public,
               "role": "input", **_info(self.root / public)}]}
        with self.assertRaisesRegex(TaskflowError, "frozen normative source differs"):
            _native_normative(self.root, declaration, bad)


if __name__ == "__main__":
    unittest.main()
