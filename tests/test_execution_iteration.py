"""Current dispatch uses coordinator intake; replay never calls a model."""

import copy
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import apply_record
from taskflow.execution_iteration import explicit_solver_decision, validate_intake, validate_purpose_kind
from taskflow.execution_solver import prepare_solver_task, register_solver_task
from taskflow.execution_store import evidence_ref
from tests import test_execution_solver as solver_fixtures


experiment_intake = solver_fixtures.experiment_intake
from tools.execution_replay import replay_decisions


class IterationContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        source = self.root / "source.txt"
        source.write_text("Explicit offline decision evidence. A lawful script remains valid.\n", encoding="utf-8")
        self.refs = [evidence_ref(source, "L1")]

    def test_experiment_requires_consequential_unknown_not_a_quality_pass_alone(self):
        value = experiment_intake(self.refs)
        self.assertEqual(value, validate_intake(value, action="experiment"))
        for change in (
            lambda row: row.update(experiment=None),
            lambda row: row["experiment"].update(important_unknown=""),
            lambda row: row["experiment"].update(outcomes=[row["experiment"]["outcomes"][0]]),
            lambda row: row["experiment"]["outcomes"].__setitem__(1, copy.deepcopy(row["experiment"]["outcomes"][0])),
            lambda row: row["quality"].update(conclusion="revision_required"),
            lambda row: row["quality"].update(evidence=[]),
            lambda row: row["difficulty"].update(limitations=""),
            lambda row: row["difficulty"].update(status="observed"),
            lambda row: row.update(purpose_kind="quality_repair"),
        ):
            with self.subTest(change=change):
                invalid = copy.deepcopy(value)
                change(invalid)
                with self.assertRaises(TaskflowError):
                    validate_intake(invalid, action="experiment")
        with self.assertRaises(TaskflowError):
            validate_purpose_kind("guessed")
        with self.assertRaises(TaskflowError):
            explicit_solver_decision({"action": "retain", "authorized": True, "intake": value})

    def test_known_quality_and_difficulty_are_separate_from_retention(self):
        value = experiment_intake(self.refs)
        value.update(purpose_kind="mechanism_diagnosis", experiment=None)
        self.assertEqual(value, validate_intake(value, action="retain"))
        value["quality"].update(conclusion="unknown", evidence=[])
        self.assertEqual("unknown", validate_intake(value, action="research")["quality"]["conclusion"])
        invalid = copy.deepcopy(value)
        invalid["difficulty"]["evidence"] = None
        with self.assertRaises(TaskflowError):
            validate_intake(invalid, action="research")
        invalid["difficulty"]["evidence"] = [{**self.refs[0], "sha256": "0" * 64}]
        with self.assertRaises(TaskflowError):
            validate_intake(invalid, action="research")

    def test_malformed_intake_is_rejected_without_inventing_facts(self):
        mutations = [
            lambda value: value.update(quality=[]),
            lambda value: value["quality"].update(conclusion={}),
            lambda value: value["quality"].update(evidence=None),
            lambda value: value.update(difficulty={}),
            lambda value: value["experiment"]["outcomes"][0].update(next_action="stop-because-script"),
            lambda value: value["experiment"]["outcomes"].__setitem__(0, {}),
        ]
        for mutation in mutations:
            value = experiment_intake(self.refs)
            mutation(value)
            with self.subTest(mutation=mutation), self.assertRaises(TaskflowError):
                validate_intake(value, action="experiment")
        with self.assertRaises(TaskflowError):
            validate_intake(None, action="experiment")
        with self.assertRaises(TaskflowError):
            validate_intake(experiment_intake(self.refs), action="research")

    def test_replay_rejects_ambiguous_cases_before_creating_output(self):
        for index, value in enumerate(([], {"schema_version": 1, "cases": [{}]},
                {"schema_version": 1, "cases": [{"id": "missing-choices"}]})):
            path, output = self.root / (str(index) + ".json"), self.root / ("bad-" + str(index))
            write_json(path, value)
            with self.assertRaises(TaskflowError):
                replay_decisions(path, output)
            self.assertFalse(output.exists())

    def test_offline_replay_changes_quality_only_call_and_preserves_normal_confirmation(self):
        formal = {"action": "experiment", "rationale": "Resolve a remaining important unknown.",
                  "authorized": True, "experiment_purpose": "Neutral replay purpose"}
        retention = experiment_intake(self.refs)
        retention.update(purpose_kind="mechanism_diagnosis", experiment=None)
        spec = {"schema_version": 1, "cases": [
            {"id": "quality-alone", "sources": self.refs, "before": formal,
             "after": {"action": "retain", "rationale": "No consequential unknown remains.", "intake": retention},
             "expected_changed": True, "expected_before_accepted": False},
            {"id": "lawful-script-confirmation", "sources": self.refs,
             "before": {**formal, "intake": experiment_intake(self.refs)},
             "after": {**formal, "intake": experiment_intake(self.refs)},
             "expected_changed": False, "expected_before_accepted": True}]}
        path = self.root / "replay.json"
        write_json(path, spec)
        original = Path(self.refs[0]["path"]).read_bytes()
        result = replay_decisions(path, self.root / "replayed")
        self.assertTrue(result["passed"], result)
        self.assertEqual(0, result["real_model_calls"])
        self.assertEqual(original, Path(self.refs[0]["path"]).read_bytes())
        self.assertEqual(result, read_json(self.root / "replayed/decision-verification.json"))


class IterationDispatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture = solver_fixtures.SolverTaskTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_legacy_quality_only_decision_cannot_enter_current_solver(self):
        case = self.fixture
        original = case.store.record_decision("no-question", "neutral", "candidate-neutral",
            action="experiment", rationale="Quality passed.", authorized=True,
            experiment_purpose=case.purpose, evidence=case.case.refs)
        spec = case.spec()
        spec["decision_id"] = original["decision_id"]
        before = len(case.store.dashboard()["work"])
        with self.assertRaisesRegex(TaskflowError, "intake"):
            register_solver_task(case.store, spec, case.case.refs)
        self.assertEqual(before, len(case.store.dashboard()["work"]))

    def test_current_coordinator_write_cannot_omit_separate_quality_and_difficulty(self):
        case = self.fixture
        for action in ("repair", "research", "retain", "reconstruct", "environment"):
            with self.subTest(action=action), self.assertRaisesRegex(TaskflowError, "intake"):
                apply_record(case.store, {"operation": "record_decision", "arguments": {
                    "decision_id": "without-intake-" + action, "task_id": "neutral",
                    "candidate_id": "candidate-neutral", "action": action,
                    "rationale": "No intake supplied.", "evidence": case.case.refs}})

    def test_apply_intake_is_locked_before_call_without_private_solver_inputs(self):
        case = self.fixture
        choice = apply_record(case.store, {"operation": "record_decision", "arguments": {
            "decision_id": "explicit-intake", "task_id": "neutral", "candidate_id": "candidate-neutral",
            "action": "experiment", "rationale": "Actual normal entry with a remaining question.",
            "authorized": True, "experiment_purpose": case.purpose, "evidence": case.case.refs,
            "intake": experiment_intake(case.case.refs)}})["result"]
        spec = case.spec()
        spec["decision_id"] = choice["decision_id"]
        registered = register_solver_task(case.store, spec, case.case.refs)
        prepared = prepare_solver_task(case.store, registered["flow_id"])
        runtime = Path(prepared["state"]["invocation"]["runtime"])
        manifest = read_json(runtime / "manifest.json")
        source = prepared["state"]["invocation"]["decision_source"]
        self.assertEqual(choice, read_json(Path(source["path"])))
        self.assertIn(source["path"], manifest["dependencies"])
        self.assertTrue(all(row["kind"] == "public" for row in manifest["inputs"]))
        request = (runtime / "request.md").read_text(encoding="utf-8")
        self.assertNotIn(choice["intake"]["experiment"]["important_unknown"], request)


if __name__ == "__main__":
    unittest.main()
