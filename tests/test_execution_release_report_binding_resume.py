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
from taskflow.execution_lifecycle import (
    _receive, _resume_unpublished_release_metadata, _resume_unpublished_release_report_binding,
    resume_path_transport,
)
from taskflow.execution_store import evidence_ref
from tests.test_execution_release_report_binding import ReleaseFixture
from tools.execution_simulation import fixture_policy


class ReleaseRecoveryGuardTests(unittest.TestCase):
    def test_snapshot_metadata_guard_remains_exact_and_rejects_report_id_error(self):
        record = {"definition": {"kind": "experiment_v1", "source_author_flow_id": "parent"},
                  "state": {"status": "exception", "stage": "release", "receipt": "original.json",
                    "exception": {"stage": "release", "reason": "quality_reports must cover exactly the required report IDs: a, b"}}}
        with self.assertRaisesRegex(TaskflowError, "exact received"):
            _resume_unpublished_release_metadata(None, record, {}, [])

    def test_source_transition_to_waiting_or_research_ended_cannot_write_during_recovery(self):
        record = {"definition": {"source_author_flow_id": "parent"}}
        for state in ({"status": "active"}, {"status": "exception"},
                      {"status": "accepted", "receipt": "received.json", "result": {"outcome": "research_ended"}},
                      {"status": "accepted", "result": {"outcome": "candidate_quality_accepted"}},
                      {"status": "accepted", "receipt": "received.json", "pending_business_scope": {"hold": "business"},
                       "result": {"outcome": "candidate_quality_accepted"}}):
            store = SimpleNamespace(get_flow=lambda key: {"definition": {}, "state": state})
            with self.subTest(state=state), self.assertRaisesRegex(TaskflowError, "without waiting"):
                _author_source(store, record, {}, require_candidate=True)

    def test_noninitial_unreceived_claimed_hold_and_stale_revision_fail_before_io(self):
        base = {"revision": 5, "definition": {"kind": "experiment_v1", "source_author_flow_id": "parent"},
                "state": {"status": "exception", "stage": "release", "receipt": "original.json"}}
        changes = ({"status": "active"}, {"stage": "formal_prepare"}, {"receipt": None},
                   {"current_work_id": "claimed"}, {"pending_business_scope": {"hold": "business"}},
                   {"waiting": {"reason": "business"}}, {"release": {"receipt": "published"}},
                   {"runs": [{"run": "prior"}]}, {"run_index": 1})
        for change in changes:
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                _resume_unpublished_release_report_binding(None, {**base, "state": {**base["state"], **change}},
                                                           {"expected_revision": 5}, [])
        for revision in (None, True, 4, 6):
            with self.subTest(revision=revision), self.assertRaisesRegex(TaskflowError, "revision"):
                _resume_unpublished_release_report_binding(None, base, {"expected_revision": revision}, [])


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
        case, store, record, resolution, interfaces, _, _ = self.fixture()
        originals = {str(path): path.read_bytes() for path in case.root.rglob("*")
                     if path.is_file() and path.suffix != ".sqlite3"}
        board = store.dashboard()
        with interfaces(), self.no_dispatch():
            resumed = resume_path_transport(store, record["flow_id"], resolution)
            with self.assertRaises(TaskflowError):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual(record["revision"] + 1, resumed["revision"])
        self.assertEqual("active", resumed["state"]["status"])
        self.assertEqual("release", resumed["state"]["stage"])
        self.assertEqual(record["state"]["history"], resumed["state"]["history"][:-1])
        self.assertEqual(board, store.dashboard())
        proof = resumed["state"]["history"][-1]
        self.assertEqual(record["state"]["exception"], proof["previous_exception"])
        self.assertEqual(resolution["original_receipt"]["sha256"], proof["previous_receipt"]["sha256"])
        self.assertFalse(proof["new_model_dispatched"])
        self.assertFalse(case.destination.exists())
        for name, raw in originals.items():
            self.assertEqual(raw, Path(name).read_bytes())
        case.release()
        self.assertEqual(proof["first_publication_identity"], read_json(case.destination / "release-receipt.json")["binding"])
        self.assertEqual(resumed, store.get_flow(record["flow_id"]))

    def test_snapshot_guard_and_same_id_legacy_cannot_use_binding_recovery(self):
        case, store, record, resolution, interfaces, _, _ = self.fixture()
        with interfaces(), self.assertRaisesRegex(TaskflowError, "exact received"):
            resume_path_transport(store, record["flow_id"], {**resolution, "action": "resume_unpublished_release_metadata"})
        _, store, record, resolution, interfaces, _, _ = self.fixture(legacy=True)
        with interfaces(), self.assertRaisesRegex(TaskflowError, "no original"):
            resume_path_transport(store, record["flow_id"], resolution)

    def test_exact_error_receipt_revision_and_source_identity_are_required(self):
        case, store, record, resolution, interfaces, bound, native = self.fixture()
        get_flow = store.get_flow
        changes = ({"exception": {"stage": "release", "reason": "snapshot file manifest mismatch"}},
                   {"exception": {"stage": "release", "reason": record["state"]["exception"]["reason"] + " "}},
                   {"accepted_review_id": "another-signature"}, {"author_source_binding": {"other": "source"}},
                   {"native_quality_handoff": {"other": "native"}}, {"quality_sources": {"other": "raw"}})
        for change in changes:
            wrong = {**record, "state": {**record["state"], **change}}
            with self.subTest(change=change), interfaces(), \
                 patch.object(store, "get_flow", side_effect=lambda key: wrong if key == record["flow_id"] else get_flow(key)), \
                 self.assertRaises(TaskflowError):
                resume_path_transport(store, record["flow_id"], resolution)
        for changed_resolution in ({**resolution, "expected_revision": record["revision"] - 1},
                                   {**resolution, "original_receipt": {**resolution["original_receipt"], "sha256": "0" * 64}}):
            with interfaces(), self.assertRaises(TaskflowError):
                resume_path_transport(store, record["flow_id"], changed_resolution)
        receipt = Path(record["state"]["receipt"])
        raw = receipt.read_bytes()
        receipt.write_bytes(raw + b"\n")
        amended = {**resolution, "original_receipt": evidence_ref(receipt, "changed receipt cannot be adopted")}
        with interfaces(), self.assertRaisesRegex(TaskflowError, "received state"):
            resume_path_transport(store, record["flow_id"], amended)
        receipt.write_bytes(raw)
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_pending_business_on_experiment_parent_or_child_is_never_cleared(self):
        for where in ("experiment", "parent", "child"):
            with self.subTest(where=where):
                _, store, record, resolution, interfaces, bound, _ = self.fixture()
                get_flow = store.get_flow
                if where == "child":
                    bound["source"]["state"]["pending_business_scope"] = {"business": "unresolved"}
                    replacement = None
                else:
                    target = record if where == "experiment" else get_flow("accepted-parent")
                    replacement = {**target, "state": {**target["state"], "pending_business_scope": {"business": "unresolved"}}}
                with interfaces(), patch.object(store, "get_flow", side_effect=lambda key:
                        replacement if replacement and key == replacement["flow_id"] else get_flow(key)), \
                     self.assertRaises(TaskflowError):
                    resume_path_transport(store, record["flow_id"], resolution)
                self.assertEqual(record, get_flow(record["flow_id"]))

    def test_projection_source_task_and_native_mutations_never_regenerate_evidence(self):
        for where in ("projection-whitespace", "projection-kind", "raw-source", "declaration", "candidate-file", "native"):
            with self.subTest(where=where):
                case, store, record, resolution, interfaces, _, native = self.fixture()
                if where.startswith("projection"):
                    path = case.projections["business"]
                    if where == "projection-whitespace":
                        path.write_bytes(path.read_bytes() + b"\n")
                    else:
                        report = read_json(path)
                        report["kind"] = "artifact"
                        write_json(path, report)
                elif where == "raw-source":
                    (case.root / "signed-source/artifacts/intake.md").write_text("Changed original.\n", encoding="utf-8")
                elif where == "declaration":
                    path = case.task / "task.json"
                    task = read_json(path)
                    task["reviews"][0]["id"] = "other-id"
                    write_json(path, task)
                elif where == "candidate-file":
                    (case.task / "draft/input/prompt.md").write_text("Changed candidate.\n", encoding="utf-8")
                else:
                    native["certification"] = {"changed": "handoff"}
                with interfaces(), self.no_dispatch(), self.assertRaises(TaskflowError):
                    resume_path_transport(store, record["flow_id"], resolution)
                self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_publication_staging_locks_formal_intents_reservations_and_dangling_links_reject(self):
        for where in ("publication", "staging", "lock", "formal", "inner", "outer", "intent", "reservation", "symlink"):
            with self.subTest(where=where):
                case, store, record, resolution, interfaces, _, _ = self.fixture()
                formal = case.root / "formal"
                inner = formal / "neutral-run"
                paths = {"publication": case.destination, "staging": case.destination.with_name(".neutral.staging-old"),
                    "lock": case.destination.with_name(".neutral.lock"), "formal": formal, "inner": inner,
                    "outer": case.directory / "runtimes" / _node(record, "solver"),
                    "intent": inner.with_name(inner.name + ".formal-intent.json"),
                    "reservation": case.destination / "reports/runtime-reservations/neutral-run.json", "symlink": case.destination}
                path = paths[where]
                path.parent.mkdir(parents=True, exist_ok=True)
                if where == "symlink":
                    path.symlink_to(case.root / "absent")
                else:
                    path.write_text("Original production footprint.\n", encoding="utf-8")
                with interfaces(), self.assertRaisesRegex(TaskflowError, "existing publication"):
                    resume_path_transport(store, record["flow_id"], resolution)
                self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_any_solver_work_attempt_or_candidate_decision_is_preserved(self):
        case, store, record, resolution, interfaces, _, _ = self.fixture()
        board = store.dashboard()
        rows = [("work", {"candidate_id": case.candidate["candidate_id"], "role": "solver", "state": state})
                for state in ("ready", "claimed", "completed", "failed", "uncertain")]
        rows += [("attempts", {"intent_id": _node(record, "solver") + "-intent"}),
                 ("work", {"runtime_path": str(case.root / "formal/neutral-run")}),
                 ("work", {"work_id": _node(record, "solver")}),
                 ("decisions", {"candidate_id": case.candidate["candidate_id"]}),
                 ("decisions", {"decision_id": _node(record, "experiment-decision")})]
        for collection, row in rows:
            wrong = copy.deepcopy(board)
            wrong[collection].append(row)
            with self.subTest(row=row), interfaces(), patch.object(store, "dashboard", return_value=wrong), \
                 self.assertRaisesRegex(TaskflowError, "existing publication"):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_store_cas_conflict_cannot_append_resolution_or_overwrite_other_state(self):
        _, store, record, resolution, interfaces, _, _ = self.fixture()
        update = store.update_flow
        def race(identity, revision, state, event):
            concurrent = update(identity, revision, {**record["state"], "concurrent_observation": "preserve"}, {"kind": "neutral-race"})
            self.assertEqual(revision + 1, concurrent["revision"])
            return update(identity, revision, state, event)
        with interfaces(), patch.object(store, "update_flow", side_effect=race), \
             self.assertRaisesRegex(TaskflowError, "revision conflict"):
            resume_path_transport(store, record["flow_id"], resolution)
        current = store.get_flow(record["flow_id"])
        self.assertEqual("preserve", current["state"]["concurrent_observation"])
        self.assertEqual(record["state"]["history"], current["state"]["history"])
        self.assertEqual(record["state"]["receipt"], current["state"]["receipt"])


if __name__ == "__main__":
    unittest.main()
