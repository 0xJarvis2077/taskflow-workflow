"""Finite source-transport recovery, with explicit fixture interface boundaries."""

from contextlib import ExitStack, contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_claim_scope_composition as claim_fixtures
from tests import test_execution_native_observation_acceptance as native_fixtures
from tools.execution_simulation import fixture_policy


CALCULATION = "native quality dimension has no explicit scoped affirmation: calculation"
COMPOSITION = "Composition defect lacks exact repair/closure identity and actual ordering"
ALIAS = "Issue evidence is absent from current candidate or has wrong SHA"


class SourceQualityTransportTests(unittest.TestCase):
    def native_case(self):
        # The real native helper reads actual request/input/output/collection
        # seals around a neutral local subprocess, never a real model call.
        source = native_fixtures.native_observation_fixture(self)
        packet = read_json(source.authority)
        packet.update(prerequisites=["Actual accepted independent current source."],
                      preauthorized_normal_chain="One exact neutral handoff.",
                      confirmation={"maximum_additional_runs_per_first": 1})
        packet["items"][0].update(first_experiment_business_unknown="Neutral offline observation.", version_id="next-v1",
                                  first_run_id="next-run-01", first_evaluation_id="next-eval-01",
                                  conditional_one_confirmation_run_id="next-run-02",
                                  conditional_one_confirmation_evaluation_id="next-eval-02")
        write_json(source.authority, packet)
        source.authorization = evidence_ref(source.authority, "neutral exact source experiment authority")
        parent_receipt = source.root / "parent-result.json"
        write_json(parent_receipt, {"flow_id": source.parent_id, "status": "accepted",
                                   "result": source.parent["state"]["result"]})
        source.parent["state"]["receipt"] = str(parent_receipt)
        store = ExecutionStore(source.root / "recovery.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "source-experiment", "task_id": "neutral",
                      "owner": "neutral-controller", "policy_id": "fixture-v1", "authorization": source.authorization,
                      "business_packet_ref": source.authorization, "source_author_flow_id": source.parent_id,
                      "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": source.parent_id},
                      "directory": str(source.root / "experiment"), "technical": {
                          "formal_runtime_root": str(source.root / "formal"), "publication_root": str(source.root / "publication")}}
        record, resolution = self.exception(store, definition, CALCULATION, source.root)
        original_get = store.get_flow
        bound = {"source": source.child, "initial": source.candidate, "author_sids": {source.item["source_author_sid"]},
                 "binding": {"source_author_flow_id": source.parent_id, "source_author_attempt": "author-attempt"}}

        @contextmanager
        def interfaces():
            # Source-author ancestry has its own real author tests. The native
            # signature validator is deliberately not mocked in this fixture.
            with ExitStack() as stack:
                stack.enter_context(patch.object(store, "get_flow", side_effect=lambda key: source.flows[key] if key in source.flows else original_get(key)))
                for name in ("get_candidate", "get_attempt", "quality_gate", "assert_review_current"):
                    stack.enter_context(patch.object(store, name, getattr(source.store, name)))
                stack.enter_context(patch("taskflow.execution_experiment._author_source", return_value=bound))
                yield
        return source, store, record, resolution, interfaces

    def exception(self, store, definition, reason, root):
        exception = {"stage": "experiment_v1", "reason": reason}
        receipt = root / (definition["flow_id"] + "-original-exception.json")
        write_json(receipt, {"flow_id": definition["flow_id"], "status": "exception", "exception": exception})
        record = store.register_flow(definition["flow_id"], definition, [definition["authorization"]], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [], "archives": [], "receipt": str(receipt), "exception": exception})
        return record, {"action": "resume_validated_source_quality_transport", "reason": "Validate only the original corrected source transport.",
                        "evidence": [definition["authorization"]]}

    def test_actual_native_nonblocking_signature_reopens_without_rewriting_false_or_sources(self):
        source, store, record, resolution, interfaces = self.native_case()
        paths = [source.material / "artifacts/adoption-readiness.json", source.material / "artifacts/issues.json",
                 source.material / "collection.json", Path(record["state"]["receipt"])]
        before = {path: path.read_bytes() for path in paths}
        with interfaces():
            result = resume_path_transport(store, record["flow_id"], resolution)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        typed = result["state"]["history"][-1]["native_certification"]["fresh"]["candidate_acceptance"]
        self.assertIs(typed["original_dimensions"]["calculation"]["affirmed"], False)
        self.assertFalse(result["state"]["history"][-1]["new_model_dispatched"])
        self.assertEqual([], store.dashboard()["work"])
        self.assertEqual([], store.dashboard()["attempts"])
        for path, raw in before.items():
            self.assertEqual(raw, path.read_bytes())

    def test_unrelated_hold_receipt_pending_business_and_existing_launch_footprints_reject(self):
        source, store, record, resolution, interfaces = self.native_case()
        original_get = store.get_flow
        for change in ({"exception": {"stage": "experiment_v1", "reason": "Specific public attribution requires a substantive disposition"}},
                       {"pending_business_scope": {"reason": "Unresolved business choice"}}, {"current_work_id": "running-work"},
                       {"candidate_id": source.candidate["candidate_id"]}, {"run_index": 1}):
            wrong = {**record, "state": {**record["state"], **change}}
            with self.subTest(change=list(change)), interfaces(), \
                 patch.object(store, "get_flow", side_effect=lambda key: wrong if key == record["flow_id"] else source.flows[key]), \
                 self.assertRaisesRegex(TaskflowError, "exact received"):
                resume_path_transport(store, record["flow_id"], resolution)
        for name in ("formal", "publication"):
            path = source.root / name
            path.mkdir()
            try:
                with self.subTest(path=name), interfaces(), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    resume_path_transport(store, record["flow_id"], resolution)
            finally:
                path.rmdir()
        import json
        for status in ("ready", "running", "uncertain", "failed"):
            row = {"work_id": "some-solver", "role": "solver", "state": status,
                   "candidate_id": source.candidate["candidate_id"]}
            identity = "existing-solver-" + status
            with store._db(True) as db:
                db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (identity, "neutral", source.candidate["candidate_id"],
                    identity + "-intent", status, json.dumps(row)))
            try:
                with self.subTest(status=status), interfaces(), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    resume_path_transport(store, record["flow_id"], resolution)
            finally:
                with store._db(True) as db:
                    db.execute("DELETE FROM work WHERE id=?", (identity,))
        receipt = Path(record["state"]["receipt"])
        raw = receipt.read_bytes()
        changed = read_json(receipt)
        changed["exception"]["reason"] = "Another exception"
        write_json(receipt, changed)
        try:
            with interfaces(), self.assertRaisesRegex(TaskflowError, "original exception receipt"):
                resume_path_transport(store, record["flow_id"], resolution)
        finally:
            receipt.write_bytes(raw)
        self.assertEqual(record, original_get(record["flow_id"]))

    def test_native_source_drift_or_flipping_the_original_false_is_not_a_resolution(self):
        source, store, record, resolution, interfaces = self.native_case()
        intake = source.material / "artifacts/intake.md"
        raw = intake.read_bytes()
        intake.write_bytes(raw + b"drift")
        try:
            with interfaces(), self.assertRaises(ValueError):
                resume_path_transport(store, record["flow_id"], resolution)
        finally:
            intake.write_bytes(raw)
        handoff = source.handoff()
        wrong = copy.deepcopy(handoff)
        wrong["certification"]["fresh"]["candidate_acceptance"]["original_dimensions"]["calculation"]["affirmed"] = True
        with interfaces(), patch("taskflow.execution_experiment._native_handoff", return_value=wrong), \
             self.assertRaisesRegex(TaskflowError, "original limited"):
            resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_composite_errors_require_an_existing_real_canonical_graph_and_preserve_scope(self):
        value = claim_fixtures.make_claim_scope_fixture(self)
        store, child = value["store"], value["flow"]
        initial = store.get_candidate(child["definition"]["initial"]["candidate_id"])
        root = store.path.parent
        parent = store.register_flow("source-author-parent", {"kind": "author_quality_v1"}, value["refs"], {
            "status": "accepted", "receipt": child["state"]["receipt"], "result": {"outcome": "candidate_quality_accepted"}})
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "composite-experiment", "task_id": "neutral",
                      "source_author_flow_id": parent["flow_id"], "authorization": value["refs"][0], "directory": str(root / "experiment"),
                      "technical": {"formal_runtime_root": str(root / "formal"), "publication_root": str(root / "publication")}}
        record, resolution = self.exception(store, definition, COMPOSITION, root)
        bound = {"source": child, "initial": initial, "binding": {"source_author_flow_id": parent["flow_id"]}}
        packet = {"formal": {"first_run_id": "next-run-01"}, "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}}
        with patch("taskflow.execution_experiment._packet", return_value=packet), \
             patch("taskflow.execution_experiment._author_source", return_value=bound):
            with self.assertRaisesRegex(TaskflowError, "whole-package"):
                resume_path_transport(store, record["flow_id"], resolution)
            review = store.compose_repaired_quality(value["candidate"], value["whole"], value["recheck"], value["defects"], evidence=value["refs"])
            graph = review["composite_quality_coverage"]
            # Composite native protocol validation is independently covered by
            # formal tests; this boundary fixture retains its real graph exactly.
            handoff = {"certification": {"fresh": {"attempt_id": review["attempt_id"]}, "composite_quality_coverage": graph}}
            before_reviews = store.get_review(review["review_id"])
            with patch("taskflow.execution_experiment._native_handoff", return_value=handoff):
                result = resume_path_transport(store, record["flow_id"], resolution)
                definition = {**definition, "flow_id": "alias-experiment"}
                alias, alias_resolution = self.exception(store, definition, ALIAS, root)
                alias_result = resume_path_transport(store, alias["flow_id"], alias_resolution)
            self.assertEqual("active", result["state"]["status"])
            self.assertEqual("active", alias_result["state"]["status"])
            self.assertFalse(before_reviews["whole_package"])
            self.assertEqual(before_reviews, store.get_review(review["review_id"]))
            self.assertEqual(value["recheck"], child["state"]["accepted_review_id"])


if __name__ == "__main__":
    unittest.main()
