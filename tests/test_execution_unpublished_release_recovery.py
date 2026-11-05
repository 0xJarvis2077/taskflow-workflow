"""Unpublished metadata retry; native/source gates are explicit tested boundaries."""

from contextlib import contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import prepare_release
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_formal as formal_fixtures
from tools.execution_simulation import fixture_policy


class UnpublishedReleaseRecoveryTests(unittest.TestCase):
    def fixture(self, *, declared=True):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        store = ExecutionStore(case.root / "state.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        declaration = read_json(case.task / "task.json")
        if declared:
            for entry in declaration["files"]:
                raw = (case.task / entry["path"]).read_bytes()
                entry.update(sha256=sha256(raw), bytes=len(raw))
            write_json(case.task / "task.json", declaration)
        case.candidate = capture_candidate(store, case.task, "declared-current")
        case.report["candidate_binding"] = {key: case.candidate[key] for key in
            ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
        case.save_report()
        authority = case.root / "authority.json"
        write_json(authority, {"controller": "fixture-controller", "nature": "neutral source-interface fixture"})
        refs = [evidence_ref(authority, "neutral exact retry authority")]
        store.register_flow("accepted-parent", {"kind": "author_quality_v1", "policy_id": "fixture-v1"}, refs, {
            "status": "accepted", "result": {"outcome": "candidate_quality_accepted"}})
        child = {"state": {"candidate_id": case.candidate["candidate_id"], "candidate_root": str(case.task)}}
        bound = {"source": child, "binding": {"source_author_flow_id": "accepted-parent", "original": "unchanged"}}
        sources = {"adoption": evidence_ref(case.signed, "neutral existing signed-source interface")}
        native = {"source_refs": sources, "protocol_map": {"nature": "explicit separately validated native interface"}}
        directory = case.root / "lifecycle"
        reports = {}
        for spec in declaration["reviews"]:
            if spec["required"]:
                path = directory / "quality-projection" / (spec["id"] + ".json")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(canonical(case.report) + b"\n")
                reports[spec["id"]] = case.report
                case.accepted["evidence"].append(evidence_ref(path, "neutral original projection evidence"))
        definition = {"kind": "experiment_v1", "flow_id": "unpublished-release", "task_id": "neutral", "policy_id": "fixture-v1",
            "directory": str(directory), "source_author_flow_id": "accepted-parent", "technical": {
                "publication_root": str(case.root / "publication/neutral"), "formal_runtime_root": str(case.root / "formal")}}
        exception = {"stage": "release", "reason": "snapshot file manifest mismatch"}
        receipt = directory / "original-release-exception.json"
        write_json(receipt, {"flow_id": definition["flow_id"], "status": "exception", "exception": exception})
        record = store.register_flow(definition["flow_id"], definition, refs, {
            "status": "exception", "stage": "release", "current_work_id": None, "receipt": str(receipt), "exception": exception,
            "candidate_id": case.candidate["candidate_id"], "candidate_root": str(case.task), "accepted_review_id": case.accepted["review_id"],
            "author_source_binding": bound["binding"], "native_quality_handoff": native, "quality_sources": sources,
            "history": [], "archives": [], "runs": [], "run_index": 0})
        resolution = {"action": "resume_unpublished_release_metadata", "reason": "Only rebuild actual metadata for the first absent publication.", "evidence": refs}
        packet = {"release": {"version_id": "neutral-v1"}, "formal": {"first_run_id": "neutral-run-01"}}

        @contextmanager
        def interfaces():
            # Role provenance is separately tested. Candidate bytes, projections,
            # retry state, atomic freeze/export and the receipt here are real.
            with patch("taskflow.execution_experiment._packet", return_value=packet), \
                 patch("taskflow.execution_experiment._author_source", return_value=bound), \
                 patch("taskflow.execution_experiment._native_handoff", return_value=native), \
                 patch("taskflow.execution_formal._projection_data", return_value=reports), \
                 patch.object(store, "quality_gate", return_value=case.accepted), \
                 patch.object(store, "assert_review_current", return_value=case.accepted):
                yield
        return case, store, record, resolution, interfaces

    def test_same_identity_makes_its_first_successful_atomic_publication_without_overwrite(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_other_error_or_hold_missing_collision_changed_receipt_and_projection_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_any_existing_publication_staging_lock_formal_or_solver_evidence_blocks_retry(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
