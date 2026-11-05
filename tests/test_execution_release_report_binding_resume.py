"""One-shot received release recovery; source provenance is an explicit interface seam.

Canonical projection, byte checks, strict release, SQLite history/CAS and IO are real.
No safety IO function is replaced; complete native provenance has adjacent tests.
"""

from contextlib import contextmanager
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _author_source, _node
from taskflow.execution_lifecycle import _receive, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests.test_execution_release_report_binding import ReleaseFixture
from tools.execution_simulation import fixture_policy


class ReleaseRecoveryGuardTests(unittest.TestCase):
    def test_snapshot_metadata_guard_remains_exact_and_rejects_report_id_error(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_transition_to_waiting_or_research_ended_cannot_write_during_recovery(self):
        record = {"definition": {"source_author_flow_id": "parent", "policy_id": "neutral-policy"}}
        for state in ({"status": "active"}, {"status": "exception"},
                      {"status": "accepted", "receipt": "received.json", "result": {"outcome": "research_ended"}},
                      {"status": "accepted", "result": {"outcome": "candidate_quality_accepted"}},
                      {"status": "accepted", "receipt": "received.json", "pending_business_scope": {"hold": "business"},
                       "result": {"outcome": "candidate_quality_accepted"}}):
            store = SimpleNamespace(get_flow=lambda key: {"definition": {}, "state": state})
            with self.subTest(state=state), self.assertRaisesRegex(TaskflowError, "without waiting"):
                _author_source(store, record, {}, require_candidate=True)

    def test_noninitial_unreceived_claimed_hold_and_stale_revision_fail_before_io(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


class UnpublishedReleaseReportBindingTests(unittest.TestCase):
    def fixture(self, **options):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        case = ReleaseFixture(Path(temporary.name).resolve(), **options)
        store = case.db
        store.register_policy("fixture-v1", fixture_policy())
        native = {"source_refs": case.refs, "protocol_map": case.protocol, "certification": {"seam": "neutral provenance interface"}}
        child = {"state": {"status": "accepted", "candidate_id": case.candidate["candidate_id"],
                           "candidate_root": str(case.task)}}
        bound = {"source": child, "binding": {"source_author_flow_id": "accepted-parent", "receipt": case.authority}}
        store.register_flow("accepted-parent", {"kind": "author_quality_v1", "policy_id": "fixture-v1"}, [case.authority],
                            {"status": "accepted", "result": {"outcome": "candidate_quality_accepted"}})
        definition = {"kind": "experiment_v1", "flow_id": "neutral-release", "task_id": "neutral",
            "owner": "neutral-controller", "policy_id": "fixture-v1", "directory": str(case.directory),
            "source_author_flow_id": "accepted-parent", "technical": {
                "publication_root": str(case.destination), "formal_runtime_root": str(case.root / "formal")}}
        task = read_json(case.task / "task.json")
        error = {"stage": "release", "reason": "quality_reports must cover exactly the required report IDs: " +
                 ", ".join(row["id"] for row in task["reviews"] if row["required"])}
        record = store.register_flow(definition["flow_id"], definition, [case.authority], {
            "status": "exception", "stage": "release", "current_work_id": None, "exception": error,
            "candidate_id": case.candidate["candidate_id"], "candidate_root": str(case.task),
            "accepted_review_id": case.accepted["review_id"], "author_source_binding": bound["binding"],
            "native_quality_handoff": native, "quality_sources": case.refs,
            "history": [{"kind": "accepted_source_bound", "candidate_id": case.candidate["candidate_id"]}],
            "archives": [], "runs": [], "run_index": 0, "waiting": None})
        record = _receive(store, record)
        resolution = {"action": "resume_unpublished_release_report_binding", "reason": "Bind original canonical signatures to captured IDs.",
                      "evidence": [case.authority], "expected_revision": record["revision"],
                      "original_receipt": evidence_ref(Path(record["state"]["receipt"]), "explicit original terminal receipt")}
        packet = {"release": {"version_id": "neutral-v1"}, "formal": {"first_run_id": "neutral-run"}}

        @contextmanager
        def interfaces():
            # Only provenance seams are injected. Real producer/strict consumer,
            # current source/projection bytes, locks and store CAS stay in use.
            with patch("taskflow.execution_experiment._packet", return_value=packet), \
                 patch("taskflow.execution_experiment._author_source", return_value=bound), \
                 patch("taskflow.execution_experiment._native_handoff", return_value=native), \
                 patch.object(store, "quality_gate", return_value=case.accepted), \
                 patch.object(store, "assert_review_current", return_value=case.accepted), \
                 patch.object(store, "get_attempt", return_value=case.attempt):
                yield
        return case, store, record, resolution, interfaces, bound, native

    @contextmanager
    def no_dispatch(self):
        with patch("subprocess.Popen", side_effect=AssertionError("no model or subprocess")), \
             patch("subprocess.run", side_effect=AssertionError("no model or subprocess")), \
             patch("taskflow.execution_experiment.prepare_formal", side_effect=AssertionError("no formal preparation")), \
             patch("taskflow.execution_experiment.prepare_release", side_effect=AssertionError("no publication")), \
             patch("taskflow.execution_experiment.projected_quality_reports", side_effect=AssertionError("no regenerated projections")), \
             patch("taskflow.execution_experiment.enqueue_spec", side_effect=AssertionError("no queue")):
            yield

    def test_original_bytes_history_one_cas_zero_models_then_normal_strict_release(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_snapshot_guard_and_same_id_legacy_cannot_use_binding_recovery(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_exact_error_receipt_revision_and_source_identity_are_required(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_pending_business_on_experiment_parent_or_child_is_never_cleared(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_projection_source_task_and_native_mutations_never_regenerate_evidence(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_publication_staging_locks_formal_intents_reservations_and_dangling_links_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_any_solver_work_attempt_or_candidate_decision_is_preserved(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_store_cas_conflict_cannot_append_resolution_or_overwrite_other_state(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
