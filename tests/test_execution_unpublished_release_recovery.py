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
            # Role provenance is separately tested, and the actual Q5 guard is
            # independently checked read-only. Candidate bytes, projections,
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
        case, store, record, resolution, interfaces = self.fixture()
        original_receipt = Path(record["state"]["receipt"]).read_bytes()
        raw = (case.task / "task.json").read_bytes()
        destination = Path(record["definition"]["technical"]["publication_root"])
        self.assertFalse(destination.exists())
        with interfaces():
            resumed = resume_path_transport(store, record["flow_id"], resolution)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual("release", resumed["state"]["stage"])
        identity = resumed["state"]["history"][-1]["first_publication_identity"]
        reports = {key: Path(row["path"]) for key, row in identity["reports"].items()}
        released = prepare_release(case.store, candidate_id=case.candidate["candidate_id"], source_root=case.task,
                                   destination=destination, version="neutral-v1", quality_reports=reports)
        self.assertEqual(identity, read_json(destination / "release-receipt.json")["binding"])
        self.assertEqual(raw, (Path(released["snapshot"]["path"]) / "task.json").read_bytes())
        self.assertEqual(raw, (case.task / "task.json").read_bytes())
        self.assertEqual(original_receipt, Path(record["state"]["receipt"]).read_bytes())
        self.assertEqual([], store.dashboard()["attempts"])

    def test_other_error_or_hold_missing_collision_changed_receipt_and_projection_reject(self):
        case, store, record, resolution, interfaces = self.fixture()
        original = store.get_flow
        for change in ({"exception": {"stage": "release", "reason": "A substantive hold"}},
                       {"pending_business_scope": {"reason": "Needs controller choice"}}, {"release": {"receipt": "already-published"}},
                       {"run_index": 1}, {"author_source_binding": {"source": "different"}}):
            wrong = {**record, "state": {**record["state"], **change}}
            with self.subTest(change=list(change)), interfaces(), \
                 patch.object(store, "get_flow", side_effect=lambda key: wrong if key == record["flow_id"] else original(key)), \
                 self.assertRaises(TaskflowError):
                resume_path_transport(store, record["flow_id"], resolution)
        projection = Path(record["definition"]["directory"]) / "quality-projection/business.json"
        raw = projection.read_bytes()
        projection.write_bytes(raw + b"\n")
        try:
            with interfaces(), self.assertRaisesRegex(TaskflowError, "projected source bytes"):
                resume_path_transport(store, record["flow_id"], resolution)
        finally:
            projection.write_bytes(raw)
        receipt = Path(record["state"]["receipt"])
        raw = receipt.read_bytes()
        changed = read_json(receipt)
        changed["exception"]["reason"] = "Unrelated original"
        write_json(receipt, changed)
        try:
            with interfaces(), self.assertRaisesRegex(TaskflowError, "original exception receipt"):
                resume_path_transport(store, record["flow_id"], resolution)
        finally:
            receipt.write_bytes(raw)
        _, other_store, other_record, other_resolution, other_interfaces = self.fixture(declared=False)
        with other_interfaces(), self.assertRaisesRegex(TaskflowError, "no original declared"):
            resume_path_transport(other_store, other_record["flow_id"], other_resolution)

    def test_any_existing_publication_staging_lock_formal_or_solver_evidence_blocks_retry(self):
        case, store, record, resolution, interfaces = self.fixture()
        publication = Path(record["definition"]["technical"]["publication_root"])
        publication.parent.mkdir()
        paths = (publication, publication.with_name("." + publication.name + ".lock"),
                 publication.with_name("." + publication.name + ".staging-original"), case.root / "formal")
        for path in paths:
            path.mkdir()
            try:
                with self.subTest(path=path.name), interfaces(), self.assertRaisesRegex(TaskflowError, "existing publication"):
                    resume_path_transport(store, record["flow_id"], resolution)
            finally:
                path.rmdir()
        board = store.dashboard()
        for status in ("ready", "running", "failed", "uncertain"):
            wrong = copy.deepcopy(board)
            wrong["work"].append({"work_id": "already-selected", "role": "solver", "state": status,
                                   "candidate_id": case.candidate["candidate_id"]})
            with self.subTest(status=status), interfaces(), patch.object(store, "dashboard", return_value=wrong), \
                 self.assertRaisesRegex(TaskflowError, "existing publication"):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual(record, store.get_flow(record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
