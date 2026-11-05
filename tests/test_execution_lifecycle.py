"""Durable terminal intake uses the existing ledger without a model launch."""

from contextlib import redirect_stderr, redirect_stdout
import copy
import io
import json
from pathlib import Path
import select
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow import execution
from taskflow.execution_flow import run_workflows
from taskflow.execution_lifecycle import _lifecycle_lock, advance_lifecycle, register_lifecycle, resume_path_transport, run_lifecycle
from taskflow.execution_store import ExecutionStore, evidence_ref
from tools.execution_simulation import fixture_policy
from tests.helpers import make_task


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.authority = self.root / "authority.json"
        write_json(self.authority, {"controller": "fixture-controller", "task_id": "neutral"})
        self.refs = [evidence_ref(self.authority, "neutral zero-model authorization")]
        self.definition = {"schema_version": 1, "kind": "evaluation_tail_v1", "flow_id": "neutral-tail",
                           "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
                           "directory": str(self.root / "tail"), "authorization": self.refs[0],
                           "review": {"nature": "fixture"}}

    def test_waiting_is_specific_and_repeated_poll_does_not_write_event(self):
        register_lifecycle(self.store, self.definition, self.refs)
        with patch("taskflow.execution_review.ingest_signed_evaluation", return_value={
            "accepted": False, "state": "waiting", "reason": "stage2 original termination absent"
        }):
            first = advance_lifecycle(self.store, "neutral-tail")
            second = advance_lifecycle(self.store, "neutral-tail")
        self.assertEqual(first["revision"], second["revision"])
        self.assertIn("termination", second["state"]["waiting"]["reason"])
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_original_is_adopted_and_unique_receipt_reentry_has_no_second_intake(self):
        register_lifecycle(self.store, self.definition, self.refs)
        value = {"accepted": True, "evaluation_id": "neutral-original", "source_sha256": "a" * 64,
                 "signer_sid": "fixture-signer", "review_archive": {"path": str(self.root / "archive")}}
        with patch("taskflow.execution_review.ingest_signed_evaluation", return_value=value) as ingest:
            first = run_lifecycle(self.store, "neutral-tail", poll_seconds=.001)
            again = run_lifecycle(self.store, "neutral-tail", poll_seconds=.001)
        self.assertEqual(1, ingest.call_count)
        self.assertEqual(first["revision"], again["revision"])
        receipt = read_json(Path(first["state"]["receipt"]))
        self.assertFalse(receipt["human_read_claimed"])
        self.assertFalse(receipt["experiment_validity_inferred"])
        self.assertEqual("accepted", receipt["status"])
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_changed_authority_or_definition_cannot_replace_registered_intent(self):
        register_lifecycle(self.store, self.definition, self.refs)
        changed = {**self.definition, "review": {"nature": "another intent"}}
        with self.assertRaises(TaskflowError):
            register_lifecycle(self.store, changed, self.refs)
        write_json(self.authority, {"controller": "other", "task_id": "neutral"})
        with self.assertRaises(TaskflowError):
            register_lifecycle(self.store, self.definition, self.refs)

    def test_verified_path_transport_resumes_original_completed_author_without_relaunch(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_path_transport_resolution_cannot_resume_other_kinds_or_unspecific_action(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def preparation_exception(self, identity, stage):
        work = self.enqueue(identity + "-author", "author")
        execution.run_worker(self.store, "neutral-author", False, [work["work_id"]])
        attempt = next(row for row in self.store.dashboard()["attempts"] if row["work_id"] == work["work_id"])
        definition = {**self.definition, "kind": "author_quality_v1", "flow_id": identity,
            "source_author_attempt": attempt["attempt_id"],
            "source_author_binding": {key: attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}}
        current = register_lifecycle(self.store, definition, self.refs)
        receipt = self.root / (identity + "-original-exception.json")
        write_json(receipt, {"reason": "Original bounded preparation transport exception."})
        state = {**current["state"], "status": "exception", "stage": stage, "author_archive": str(self.root / "archive"),
                 "receipt": str(receipt), "exception": {"reason": "Original bounded preparation transport exception."}}
        if stage == "public_prepare":
            state["candidate_id"] = self.candidate["candidate_id"]
        current = self.store.update_flow(identity, current["revision"], state, {"kind": "neutral_preparation_exception"})
        source = make_task(self.root / (identity + "-delivery"))
        declaration = read_json(source / "task.json")
        declaration["reviews"].append({"id": "calculation", "kind": "calculation",
                                       "path": "reports/calculation.json", "required": True})
        write_json(source / "task.json", declaration)
        for review in declaration["reviews"]:
            write_json(source / review["path"], {"schema_version": 1, "review_id": review["id"],
                "kind": review["kind"], "status": "inconclusive", "scope": "Unsigned neutral author placeholder.",
                "content_digest": None, "input_digest": None, "evaluation_digest": None,
                "independent_quality_passed": False, "new_formal_score": None})
        files = [{"package_path": name, "role": role, "size": len((source / name).read_bytes()),
                  **evidence_ref(source / name, "neutral verified delivery payload")}
                 for name, role in {"task.json": "source_metadata", **{row["path"]: row["role"] for row in declaration["files"]}}.items()]
        material = {"archive": state["author_archive"], "candidate_root": str(source), "file_refs": files}
        return current, attempt, material, receipt

    def test_unclaimed_public_preparation_resumes_without_changing_original_author_or_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_public_preparation_rejects_current_or_previously_enqueued_work_and_wrong_action(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_typed_alias_preserves_delivered_payloads_and_only_accepts_unsigned_report_metadata(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_typed_alias_rejects_payload_declaration_and_signed_report_drift_without_state_change(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_intake_failure_retains_exception_and_does_not_retry(self):
        register_lifecycle(self.store, self.definition, self.refs)
        with patch("taskflow.execution_review.ingest_signed_evaluation",
                   side_effect=TaskflowError("sealed original has incompatible standards")) as ingest:
            result = run_lifecycle(self.store, "neutral-tail", poll_seconds=.001)
            run_lifecycle(self.store, "neutral-tail", poll_seconds=.001)
        self.assertEqual(1, ingest.call_count)
        self.assertEqual("exception", result["state"]["status"])
        self.assertIn("incompatible", result["state"]["exception"]["reason"])

    def enqueue(self, identity, role="research"):
        if not hasattr(self, "candidate"):
            task = make_task(self.root)
            self.candidate = execution.capture_candidate(self.store, task, "neutral-initial")
        script = self.root / "local-role.py"
        if not script.exists():
            script.write_text(
                "import json,pathlib,uuid\n"
                "pathlib.Path('delivery.txt').write_text('Actual neutral subprocess delivery.')\n"
                "print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}))\n"
                "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral fixture result.'}}))\n"
                "print(json.dumps({'type':'turn.completed'}))\n", encoding="utf-8")
        cwd = self.root / (identity + "-work")
        cwd.mkdir()
        work = {"work_id": identity, "task_id": "neutral", "candidate_id": "neutral-initial", "role": role,
                "intent_id": identity + "-intent", "policy_id": "fixture-v1", "dispatch_owner": "fixture-controller",
                "purpose": "neutral local fixture", "request_digest": "0" * 64, "evidence": self.refs}
        runner = {"intent_id": work["intent_id"], "role": role, "policy_version": "fixture-v1", "mode": "test_fixture",
                  "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                  "cwd": str(cwd), "argv": [str(Path(sys.executable).resolve()), str(script)], "dependencies": [str(script)],
                  "inputs": [], "request_sections": [{"kind": "operation", "text": "Neutral local fixture only."}], "outputs": ["delivery.txt"]}
        return execution.enqueue_spec(self.store, {"work": work, "runner": runner}, self.root / (identity + "-runtime"))["work"]

    def experiment(self):
        write_json(self.authority, {"controller": "fixture-controller", "task_id": "neutral", "source_flow_id": "neutral-source"})
        self.refs = [evidence_ref(self.authority, "neutral bounded lifecycle authorization")]
        definition = {**self.definition, "kind": "experiment_v1", "flow_id": "neutral-experiment",
                      "source_flow_id": "neutral-source", "authorization": self.refs[0]}
        definition.pop("review")
        return register_lifecycle(self.store, definition, self.refs)

    def role_handler(self, identity):
        def advance(store, record):
            work = store.get_work(identity)
            state = dict(record["state"])
            if work["state"] == "completed":
                state.update(status="accepted", stage="fixture_completed", current_work_id=None,
                             result={"nature": "test_fixture", "work_id": identity})
            else:
                if state.get("current_work_id") == identity:
                    return record
                state.update(stage="fixture_worker", current_work_id=identity)
            return store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_fixture_transition"})
        return advance

    def cli(self, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = execution.main(["--db", str(self.store.path), *arguments])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, json.loads(err.getvalue()) if err.getvalue() else None

    def test_exact_ready_work_is_claimed_once_and_unselected_work_remains_ready(self):
        record = self.experiment()
        selected, other = self.enqueue("selected"), self.enqueue("unselected")
        worker = execution.run_worker
        with patch("taskflow.execution_experiment.advance_experiment", side_effect=self.role_handler(selected["work_id"])), \
             patch("taskflow.execution.run_worker", wraps=worker) as run:
            result = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
            again = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(result, again)
        run.assert_called_once_with(self.store, "lifecycle-consumer", True, [selected["work_id"]])
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(1, len(attempts))
        self.assertEqual("selected", attempts[0]["work_id"])
        self.assertEqual("completed", attempts[0]["state"])
        self.assertTrue((Path(selected["runtime_path"]) / "artifacts/delivery.txt").is_file())
        self.assertEqual("ready", self.store.get_work(other["work_id"])["state"])

    def test_no_allow_does_not_claim_and_same_pid_later_allow_preserves_both_receipts(self):
        record = self.experiment()
        selected = self.enqueue("permission-bound")
        with patch("taskflow.execution_experiment.advance_experiment", side_effect=self.role_handler(selected["work_id"])):
            with patch("taskflow.execution.run_worker", side_effect=AssertionError("not authorized")):
                waiting = run_lifecycle(self.store, record["flow_id"], allow_model=False, poll_seconds=.001)
            self.assertEqual("ready_requires_explicit_model_permission", waiting["execution"])
            self.assertEqual([], self.store.dashboard()["attempts"])
            self.assertEqual("ready", self.store.get_work(selected["work_id"])["state"])
            result = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual("accepted", result["state"]["status"])
        consumers = [read_json(path) for path in Path(self.definition["directory"]).glob("consumer-*.json")]
        self.assertEqual({False, True}, {item["allow_model"] for item in consumers})
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_worker_uncertainty_keeps_original_attempt_and_receives_exception_without_retry(self):
        record = self.experiment()
        selected, other = self.enqueue("uncertain"), self.enqueue("still-unselected")
        (Path(selected["runtime_path"]) / "manifest.json").unlink()
        with patch("taskflow.execution_experiment.advance_experiment", side_effect=self.role_handler(selected["work_id"])):
            first = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
            again = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual("exception", first["state"]["status"])
        self.assertEqual(first, again)
        self.assertEqual("uncertain", self.store.dashboard()["attempts"][0]["state"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertEqual("ready", self.store.get_work(other["work_id"])["state"])
        self.assertFalse((Path(selected["runtime_path"]) / "process.json").exists())
        self.assertEqual("exception", read_json(Path(first["state"]["receipt"]))["status"])

    def test_legacy_default_flow_run_skips_lifecycles_and_wrong_entry_does_not_mutate(self):
        lifecycle = register_lifecycle(self.store, self.definition, self.refs)
        self.assertEqual([], run_workflows(self.store, allow_model=False, poll_seconds=.001)["flows"])
        with self.assertRaisesRegex(TaskflowError, "lifecycle-run"):
            run_workflows(self.store, [lifecycle["flow_id"]], allow_model=False, poll_seconds=.001)
        old_definition = {"schema_version": 1, "policy_id": "fixture-v1", "directory": str(self.root / "old-flow")}
        old_state = {"status": "active", "stage": "quality", "current_work_id": None, "history": [], "archives": []}
        old = self.store.register_flow("old-flow", old_definition, self.refs, old_state)
        for call in (advance_lifecycle, run_lifecycle):
            with self.subTest(entry=call.__name__), self.assertRaises(TaskflowError):
                call(self.store, "old-flow")
            self.assertEqual(old, self.store.get_flow("old-flow"))
        self.assertFalse(Path(old_definition["directory"]).exists())
        self.assertEqual(lifecycle, self.store.get_flow(lifecycle["flow_id"]))

    def test_author_registration_binds_exact_original_and_dispatches_only_its_handler(self):
        work = self.enqueue("original-author", role="author")
        author = self.store.claim("original-author-worker", [work["work_id"]])
        definition = {**self.definition, "kind": "author_quality_v1", "flow_id": "author-tail",
                      "source_author_attempt": author["attempt_id"],
                      "source_author_binding": {key: author[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}}
        bad = copy.deepcopy(definition)
        bad["source_author_binding"]["intent_id"] = "another-author-intent"
        with self.assertRaisesRegex(TaskflowError, "exact authorized original"):
            register_lifecycle(self.store, bad, self.refs)
        registered = register_lifecycle(self.store, definition, self.refs)
        with patch("taskflow.execution_author.advance_author", return_value=registered) as author_handler, \
             patch("taskflow.execution_experiment.advance_experiment", side_effect=AssertionError("wrong kind")), \
             patch("taskflow.execution_review.ingest_signed_evaluation", side_effect=AssertionError("wrong kind")):
            self.assertEqual(registered, advance_lifecycle(self.store, registered["flow_id"]))
        author_handler.assert_called_once_with(self.store, registered)
        self.assertEqual("claimed", self.store.get_attempt(author["attempt_id"])["state"])

    def test_registration_rejects_schema_owner_task_missing_authority_and_source_mismatch(self):
        alternate = self.root / "alternate.json"
        write_json(alternate, {"nature": "another neutral reference"})
        wrong_refs = [evidence_ref(alternate, "another source")]
        changes = [{"schema_version": True}, {"kind": "unknown_kind"}, {"directory": "relative"},
                   {"owner": "another-controller"}, {"task_id": "another-task"},
                   {"kind": "experiment_v1", "source_flow_id": "unapproved-source"}]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                register_lifecycle(self.store, {**self.definition, **change}, self.refs)
        with self.assertRaisesRegex(TaskflowError, "immutable evidence"):
            register_lifecycle(self.store, self.definition, wrong_refs)
        first = register_lifecycle(self.store, self.definition, self.refs)
        self.assertEqual(first, register_lifecycle(self.store, self.definition, self.refs))

    def test_actual_other_process_lock_excludes_same_lifecycle_but_allows_another(self):
        identity = "locked-lifecycle"
        path = self.store.path.with_name(self.store.path.name + ".lifecycle-" + identity + ".lock")
        code = "import fcntl,sys; f=open(sys.argv[1],'a+b'); fcntl.flock(f,fcntl.LOCK_EX); print('locked',flush=True); sys.stdin.readline()"
        process = subprocess.Popen([sys.executable, "-c", code, str(path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            self.assertEqual("locked", process.stdout.readline().strip())
            with self.assertRaisesRegex(TaskflowError, "already has a consumer"):
                with _lifecycle_lock(self.store, identity):
                    self.fail("second consumer acquired the original lifecycle")
            with _lifecycle_lock(self.store, "different-lifecycle"):
                self.assertIsNone(process.poll())
        finally:
            process.communicate("\n", timeout=5)

    def test_retired_evaluation_lifecycle_cli_never_changes_original_receipt(self):
        registered = register_lifecycle(self.store, self.definition, self.refs)
        before = self.store.events()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            execution.main(["--db", str(self.store.path), "lifecycle-run", "--flow-id", registered["flow_id"]])
        self.assertEqual(2, error.exception.code)
        self.assertEqual(before, self.store.events())
        self.assertEqual(registered, self.store.get_flow(registered["flow_id"]))
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_retired_author_cli_rejects_both_model_flags_without_dispatch(self):
        before = self.store.events()
        for flag in ([], ["--allow-model"]):
            with self.subTest(flag=flag), patch("taskflow.execution_author.start_author_from_packet") as start, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    execution.main(["--db", str(self.store.path), "author-start", "--packet", "unused.json", "--factory", "unused.json", *flag])
                self.assertEqual(2, error.exception.code)
                start.assert_not_called()
        self.assertEqual(before, self.store.events())


class FormalPreparationResumeTests(unittest.TestCase):
    def setUp(self):
        from tests import test_execution_experiment as fixtures

        case = fixtures.ExperimentBoundaryTests("test_source_release_prepare_reaches_one_exact_ready_work_without_launch")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store, self.root = case, case.store, case.root
        self.resolution = {"action": "resume_unclaimed_formal_preparation",
            "reason": "Resolve the exact multi-item authority projection before any solver enqueue.",
            "evidence": [evidence_ref(case.authority, "neutral immutable authority and technical resolution") ]}

    def failure(self, *, enqueued=False):
        record = self.case.ready() if enqueued else self.case.advance()
        binding = {key: self.case.candidate[key] for key in
                   ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
        source_report = self.case.refs[0]
        report = {"path": source_report["path"], "sha256": source_report["sha256"],
                  "size": len(Path(source_report["path"]).read_bytes())}
        release_receipt = self.root / "release-resume-receipt.json"
        write_json(release_receipt, {"binding": {"candidate_binding": binding, "accepted_review_id": "source-review",
            "source_root": str(self.case.task), "version": "fixture-v1", "reports": {"business": report}}})
        release = {"candidate_binding": binding, "snapshot": {"path": str(self.case.snapshot)},
                   "receipt": {"path": str(release_receipt), "sha256": sha256(release_receipt.read_bytes()),
                               "size": len(release_receipt.read_bytes())}, "quality_evidence": [report]}
        receipt = self.root / "original-formal-exception.json"
        exception = {"stage": "formal_prepare", "reason": "formal configuration is outside the explicit business packet"}
        write_json(receipt, {"flow_id": record["flow_id"], "status": "exception", "exception": exception})
        state = {**record["state"], "status": "exception", "stage": "formal_prepare", "current_work_id": None,
                 "release": release, "receipt": str(receipt), "exception": exception}
        record = self.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_formal_guard_fixture"})
        return record, receipt

    def validate_authority(self):
        return patch("taskflow.execution_formal.validate_formal_authority", return_value={"controller": "fixture-controller"})

    def test_exact_unclaimed_formal_guard_resumes_once_and_preserves_definition_run_decision_receipts(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_existing_ready_claimed_and_uncertain_solver_are_never_requeued(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_any_inner_outer_or_formal_intent_footprint_blocks_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_current_payload_or_release_binding_and_authority_drift_block_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_new_author_mode_passes_only_immutable_item_selector_and_rechecks_native_quality(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
