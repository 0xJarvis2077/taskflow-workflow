"""Scheduling metadata does not require a full historical quality projection."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_author import _active_attempt, _factory_owner
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_author as author_fixtures
from tests.helpers import make_task


class DispatchMetadataTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        source = self.root / "authority.json"
        write_json(source, {"nature": "neutral metadata authorization"})
        self.refs = [evidence_ref(source, "neutral controller evidence")]

    def observe(self, task, owner, date):
        return self.store.observe(task, self.refs[0], "observed", owner, date, {})

    def test_target_snapshot_preserves_dated_latest_and_factory_transfer_without_quality(self):
        self.observe("neutral", "current-controller", "2026-01-02T00:00:00Z")
        self.observe("neutral", "older-controller", "2026-01-01T00:00:00Z")
        self.observe("unrelated", "another-controller", "2026-01-03T00:00:00Z")
        expected = next(row for row in self.store.dashboard()["observations"]
                        if row["task_id"] == "neutral")
        with patch.object(self.store, "dashboard", side_effect=AssertionError("no dashboard")), \
             patch.object(ExecutionStore, "_assert_quality", side_effect=AssertionError("no global quality scan")):
            context = self.store.dispatch_ownership_snapshot("neutral")
            self.assertIsNone(context["ownership"])
            self.assertEqual(expected, context["observation"])
            before = self.store.events()
            with self.assertRaisesRegex(TaskflowError, "observed controller"):
                _factory_owner(self.store, "neutral", "older-controller", self.refs)
            self.assertEqual(before, self.store.events())
            _factory_owner(self.store, "neutral", "current-controller", self.refs)
            owned = self.store.dispatch_ownership_snapshot("neutral")["ownership"]
            self.assertEqual("current-controller", owned["owner"])
            self.assertEqual("future_dispatch", owned["scope"])
            before = self.store.events()
            _factory_owner(self.store, "neutral", "current-controller", self.refs)
            self.assertEqual(before, self.store.events())
            with self.assertRaisesRegex(TaskflowError, "another controller"):
                _factory_owner(self.store, "neutral", "foreign-controller", self.refs)

    def test_empty_and_work_specific_ownership_do_not_become_general_ownership(self):
        self.assertEqual({"ownership": None, "observation": None},
                         self.store.dispatch_ownership_snapshot("neutral"))
        before = self.store.events()
        _factory_owner(self.store, "neutral", "controller", self.refs)
        self.assertEqual(before, self.store.events())
        self.observe("neutral", "controller", "2026-01-01T00:00:00Z")
        self.store.transfer_owner("neutral", "controller", "specific-controller", self.refs,
                                  scope="work_item", work_id="future-work")
        snapshot = self.store.dispatch_ownership_snapshot("neutral")
        self.assertIsNone(snapshot["ownership"])
        self.assertEqual("controller", snapshot["observation"]["owner"])

    def test_task_filter_does_not_interpret_unrelated_observations(self):
        self.observe("neutral", "controller", "2026-01-01T00:00:00Z")
        self.observe("unrelated", "other", "2026-01-02T00:00:00Z")
        from taskflow import execution_store
        original = execution_store._observation_time

        def checked(value):
            if value.get("task_id") == "unrelated":
                raise AssertionError("unrelated observation must not be selected")
            return original(value)

        with patch("taskflow.execution_store._observation_time", side_effect=checked):
            snapshot = self.store.dispatch_ownership_snapshot("neutral")
        self.assertEqual("controller", snapshot["observation"]["owner"])

    def test_exact_attempt_and_decision_reads_do_not_validate_unrelated_quality(self):
        case = author_fixtures.AuthorSuccessorTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        attempt = case.run_source("research_ended")
        unrelated = make_task(case.root, "unrelated")
        execution.capture_candidate(case.store, unrelated, "unrelated-candidate")
        with self.assertRaises(TaskflowError):
            case.store.quality_gate("unrelated-candidate")
        decision = case.store.record_decision("metadata-decision", "neutral", "baseline",
            action="research", rationale="Neutral metadata lookup", evidence=case.refs)
        with patch.object(case.store, "dashboard", side_effect=AssertionError("no dashboard")), \
             patch.object(ExecutionStore, "_assert_quality", side_effect=AssertionError("no global quality scan")):
            self.assertEqual([attempt], case.store.list_attempts(attempt["work_id"]))
            self.assertEqual([], case.store.list_attempts("another-work"))
            self.assertEqual(attempt, _active_attempt(case.store, attempt["work_id"]))
            self.assertIsNone(_active_attempt(case.store, "another-work"))
            self.assertEqual(decision, case.store.get_decision("metadata-decision"))
            with self.assertRaises(TaskflowError):
                case.store.get_decision("missing-decision")


if __name__ == "__main__":
    unittest.main()
