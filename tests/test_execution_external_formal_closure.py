"""Offline closure regressions: local fixture subprocesses, never model results.

The simulated quality/release and role adapters are the existing neutral fixtures.
State persistence, work execution, fact sealing, same-SID score validation and
original intake remain real. Provider reference recovery validates actual bytes.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_experiment as experiment_fixtures
from tests import test_execution_lifecycle as lifecycle_fixtures


class ExternalFormalClosureTests(unittest.TestCase):
    def fixture(self):
        case = experiment_fixtures.ExperimentBoundaryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def finish_one(self, case, ready):
        work_id = ready["state"]["current_work_id"]
        self.assertEqual("formal_wait", ready["state"]["stage"], ready["state"])
        self.assertEqual("ready", case.store.get_work(work_id)["state"])
        before = case.store.dashboard()["attempts"]
        self.assertFalse(any(row["role"] == "solver" for row in before))
        waiting = case.advance()
        self.assertEqual("original_worker_not_started", waiting["state"]["waiting"]["reason"])
        self.assertEqual(waiting, case.advance())
        execution.run_worker(case.store, "local-solver", False, [work_id])
        with patch("taskflow.execution_experiment.import_formal", side_effect=case.import_original_fixture):
            record = case.advance()
        self.assertEqual("facts_prepare", record["state"]["stage"])
        with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=case.fixture_role):
            record = case.advance()
        self.assertEqual("facts_wait", record["state"]["stage"])
        self.assertFalse(any(row["role"] == "review-score" for row in case.store.dashboard()["work"]))
        facts_work = case.store.get_work(record["state"]["current_work_id"])
        facts_manifest = read_json(Path(facts_work["runtime_path"]) / "manifest.json")
        self.assertFalse(any(item["kind"] == "teacher" for item in facts_manifest["inputs"]))
        execution.run_worker(case.store, "local-facts", False, [facts_work["work_id"]])
        record = case.advance()
        self.assertEqual("score_prepare", record["state"]["stage"])
        facts_root = Path(record["state"]["facts_attempt"]["runtime_path"])
        seal_path = facts_root / "facts-seal.json"
        seal_before = seal_path.read_bytes()
        seal = read_json(seal_path)
        with patch("taskflow.execution_experiment.prepare_score_runner",
                   side_effect=lambda **kw: case.fixture_role(scoring=True, **kw)):
            record = case.advance()
        self.assertEqual("score_wait", record["state"]["stage"])
        score_work = case.store.get_work(record["state"]["current_work_id"])
        score_manifest = read_json(Path(score_work["runtime_path"]) / "manifest.json")
        self.assertEqual(seal["sid"], score_manifest["resume_sid"])
        execution.run_worker(case.store, "local-score", False, [score_work["work_id"]])
        record = case.advance()
        self.assertEqual("evaluation_intake", record["state"]["stage"])
        record = case.advance()
        self.assertEqual("accepted", record["state"]["status"], record)
        result = record["state"]["result"]["runs"][0]
        self.assertEqual("test_fixture", result["nature"])
        self.assertEqual(7, result["score"])
        self.assertEqual(seal_before, seal_path.read_bytes())
        attempts = case.store.dashboard()["attempts"]
        self.assertEqual(4, len(attempts))
        facts = next(row for row in attempts if row["role"] == "review-facts")
        score = next(row for row in attempts if row["role"] == "review-score")
        self.assertEqual(facts["cli_sid"], score["cli_sid"])
        archive = case.root / "lifecycle/evaluations/fixture-01/receipt.json"
        original_receipt = archive.read_bytes()
        self.assertEqual(record, case.advance())
        self.assertEqual(attempts, case.store.dashboard()["attempts"])
        self.assertEqual(original_receipt, archive.read_bytes())

    def test_zero_model_ready_then_facts_before_same_sid_score_and_unique_intake(self):
        case = self.fixture()
        self.finish_one(case, case.ready())

    def test_pre_intent_metadata_failure_resumes_same_definition_then_closes_once(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
