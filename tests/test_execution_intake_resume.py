"""Resume only a completed, already imported typed-locator original intake."""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution, execution_review
from taskflow.contracts import TaskflowError, canonical, read_json, write_json
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as fixtures


class EvaluationIntakeResumeTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.ExperimentBoundaryTests("test_real_fixture_workers_traverse_import_facts_seal_score_and_original_intake")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        record = case.ready()
        execution.run_worker(case.store, "neutral-original", False, [record["state"]["current_work_id"]])
        with patch("taskflow.execution_experiment.import_formal", side_effect=case.import_original_fixture):
            record = case.advance()
        with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=case.fixture_role):
            record = case.advance()
        execution.run_worker(case.store, "neutral-facts", False, [record["state"]["current_work_id"]])
        record = case.advance()

        def typed_payload(path, value):
            if Path(path).name == "payload.json" and "score.json" in value:
                value = copy.deepcopy(value)
                score = json.loads(value["score.json"])
                for row in score["criteria"]:
                    for field in ("public_evidence", "submission_evidence"):
                        for citation in row[field]:
                            citation["locator"] = {"type": "lines", "start": 1, "end": 1}
                value["score.json"] = json.dumps(score)
            write_json(path, value)

        def prepare_score(**kwargs):
            with patch("tests.test_execution_experiment.write_json", side_effect=typed_payload):
                return case.fixture_role(scoring=True, **kwargs)

        with patch("taskflow.execution_experiment.prepare_score_runner", side_effect=prepare_score):
            record = case.advance()
        execution.run_worker(case.store, "neutral-score", False, [record["state"]["current_work_id"]])
        record = case.advance()
        self.assertEqual("evaluation_intake", record["state"]["stage"])
        with patch("taskflow.execution_review._locators", side_effect=TaskflowError("signed evidence path/location is missing")):
            record = advance_lifecycle(case.store, record["flow_id"])
        record = advance_lifecycle(case.store, record["flow_id"])
        self.record = record
        state = record["state"]
        config = {"schema_version": 1, "mode": "managed_two_stage", "run_directory": state["imported_run"]["run_path"],
            "snapshot_directory": state["release"]["snapshot"]["path"], "evaluation_id": "fixture-eval-01",
            "facts_attempt": state["facts_attempt"]["runtime_path"], "facts_binding": state["facts_prepared"]["review_binding"],
            "score_attempt": state["score_attempt"]["runtime_path"], "score_binding": state["score_prepared"]["review_binding"],
            "expected_sid": state["facts_seal"]["sid"], "score_file": "score.json", "explanation_file": "explanation.md",
            "evidence": [record["definition"]["authorization"]]}
        destination = Path(record["definition"]["directory"]) / "evaluations/fixture-01"
        result = execution_review.ingest_signed_evaluation(config=config, destination=destination)
        self.assertTrue(result["accepted"])
        self.intake_receipt = Path(result["receipt"])
        self.resolution = {"action": "resume_validated_evaluation_intake", "reason": "Only consume the already accepted original typed-locator transport.",
            "evidence": [record["definition"]["authorization"]], "receipt": evidence_ref(self.intake_receipt, "existing exact accepted intake")}
        self.originals = {path: path.read_bytes() for path in (Path(state["receipt"]),
            Path(state["score_attempt"]["runtime_path"]) / "artifacts/score.json",
            Path(state["score_attempt"]["runtime_path"]) / "first-score-seal.json",
            Path(state["score_attempt"]["runtime_path"]) / "collection.json",
            Path(state["facts_attempt"]["runtime_path"]) / "facts-seal.json")}

    def test_existing_native_intake_resumes_same_stage_without_reingest_or_model(self):
        attempts = self.store.dashboard()["attempts"]
        decisions = self.store.dashboard()["decisions"]
        with patch("taskflow.execution_review.ingest_signed_evaluation", side_effect=AssertionError("resume must not reingest")), \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("resume must not dispatch")):
            result = resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("evaluation_intake", result["state"]["stage"])
        self.assertEqual(self.record["definition"], result["definition"])
        self.assertEqual(self.record["state"]["run_index"], result["state"]["run_index"])
        self.assertEqual(attempts, self.store.dashboard()["attempts"])
        self.assertEqual(decisions, self.store.dashboard()["decisions"])
        self.assertFalse(result["state"]["history"][-1]["new_model_dispatched"])
        for path, raw in self.originals.items():
            self.assertEqual(raw, path.read_bytes())
        with self.assertRaisesRegex(TaskflowError, "exact received"):
            resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def test_foreign_receipt_and_changed_accepted_source_hash_or_sid_are_rejected(self):
        other = self.case.root / "foreign-receipt.json"
        other.write_bytes(self.intake_receipt.read_bytes())
        with self.assertRaisesRegex(TaskflowError, "fixed destination"):
            resume_path_transport(self.store, self.record["flow_id"], {**self.resolution,
                "receipt": evidence_ref(other, "same bytes at a foreign destination")})
        original = self.intake_receipt.read_bytes()
        for field in ("source_sha256", "signer_sid"):
            changed = json.loads(original)
            changed[field] = "wrong-original-binding"
            write_json(self.intake_receipt, changed)
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "differs from the original"):
                resume_path_transport(self.store, self.record["flow_id"], {**self.resolution,
                    "receipt": evidence_ref(self.intake_receipt, "negative accepted receipt fixture")})
        self.intake_receipt.write_bytes(original)
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))

    def test_projection_drift_and_uncertain_original_attempt_cannot_resume(self):
        verification = self.intake_receipt.parent / "evaluation-transport-verification.json"
        original = verification.read_bytes()
        value = read_json(verification)
        value["bindings"][0]["projected_locator"] = "999999"
        write_json(verification, value)
        with self.assertRaisesRegex(TaskflowError, "traceable typed-locator"):
            resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        verification.write_bytes(original)
        from taskflow.execution_completion import completion_owner

        score = self.record["state"]["score_attempt"]
        originals = {}
        # A sealed ended attempt can now be received legitimately. Keep this
        # negative as an unresolved original still owned by its collector.
        with self.store._db(True) as db:
            for table, identity in (("attempts", score["attempt_id"]), ("work", score["work_id"])):
                row = db.execute("SELECT state,data FROM " + table + " WHERE id=?", (identity,)).fetchone()
                originals[table] = (identity, row["state"], row["data"])
                value = json.loads(row["data"])
                value["state"] = "uncertain"
                db.execute("UPDATE " + table + " SET state=?,data=? WHERE id=?",
                           ("uncertain", canonical(value).decode(), identity))
        original_lookup = self.store.list_attempts
        requested = []

        def original_score_lookup(work_id):
            requested.append(work_id)
            rows = original_lookup(work_id)
            if work_id == score["work_id"]:
                self.assertEqual([score["attempt_id"]], [row["attempt_id"] for row in rows])
                self.assertEqual("uncertain", rows[0]["state"])
            return rows

        try:
            with completion_owner(Path(score["runtime_path"])) as owned:
                self.assertTrue(owned)
                with patch.object(self.store, "list_attempts", side_effect=original_score_lookup), self.assertRaisesRegex(TaskflowError, "no automatic redraw"):
                    resume_path_transport(self.store, self.record["flow_id"], self.resolution)
            self.assertIn(score["work_id"], requested)
        finally:
            with self.store._db(True) as db:
                for table, (identity, state, data) in originals.items():
                    db.execute("UPDATE " + table + " SET state=?,data=? WHERE id=?", (state, data, identity))
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
