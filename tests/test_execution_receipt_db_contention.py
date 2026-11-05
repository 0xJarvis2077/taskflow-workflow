"""Real SQLite contention at receipt boundaries, with actual neutral CLI IO.

No external model, production database, timeout-policy edit or warning filter is
used. Only OS startup identity metadata unavailable in a sandbox is substituted.
"""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256
from taskflow.execution_completion import completion_owner
from taskflow.execution_author import advance_author
from taskflow import execution_runner as runner
from taskflow.execution_flow import advance_workflow
from tests import test_execution as cli_fixtures
from tests import test_execution_flow as flow_fixtures
from tests import test_execution_author as author_fixtures


def operational_error(code, text="database is locked"):
    error = sqlite3.OperationalError(text)
    error.sqlite_errorcode = code
    return error


class ReceiptDBContentionTests(unittest.TestCase):
    def queued(self):
        case = cli_fixtures.ExecutionCLITests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        spec = case.spec("receipt-original")
        original = spec["runner"]["argv"][1]
        script = case.root / "counted-fake-cli.py"
        script.write_text(
            "import pathlib,runpy,sys\n"
            "with pathlib.Path('model-invocations.txt').open('a') as fp:fp.write('CALL\\n')\n"
            "sys.argv[0]=" + repr(original) + "\n"
            "runpy.run_path(sys.argv[0],run_name='__main__')\n")
        spec["runner"]["argv"][1] = str(script)
        spec["runner"]["dependencies"].append(str(script))
        runtime = case.root / "receipt-runtime"
        execution.enqueue_spec(case.store, spec, runtime)
        return case, runtime, spec["work"]["work_id"]

    def ended(self):
        case, runtime, work_id = self.queued()
        attempt = case.store.claim("original-fake-cli", [work_id])
        case.store.mark_starting(attempt["attempt_id"])

        def on_process(value):
            case.store.record_process(attempt["attempt_id"], value["pid"],
                value.get("start_identity") or "neutral real subprocess pid=" + str(value["pid"]))

        result = runner.run_attempt(runtime, on_process=on_process)
        self.assertTrue(result["technical_accepted"])
        self.assertTrue((runtime / "collection.json").is_file())
        self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
        return case, runtime, case.store.get_attempt(attempt["attempt_id"]), result

    @contextmanager
    def writer(self, database):
        ready, release = threading.Event(), threading.Event()
        errors = []

        def own_write_reservation():
            db = sqlite3.connect(database, timeout=0, isolation_level=None)
            try:
                db.execute("BEGIN IMMEDIATE")
                ready.set()
                if not release.wait(10):
                    raise AssertionError("test failed to release its own SQLite reservation")
                db.rollback()
            except BaseException as error:
                errors.append(error)
                ready.set()
            finally:
                db.close()

        thread = threading.Thread(target=own_write_reservation, name="neutral-sqlite-writer")
        thread.start()
        self.assertTrue(ready.wait(3))
        try:
            self.assertEqual([], errors)
            yield release
        finally:
            release.set()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual([], errors)

    def receive(self, case, runtime, attempt, result, **options):
        with completion_owner(runtime) as owned:
            self.assertTrue(owned)
            return execution._receive_ended_result(case.store, attempt, result, **options)

    def assert_one_terminal(self, case, attempt):
        actual = case.store.get_attempt(attempt["attempt_id"])
        self.assertEqual("completed", actual["state"])
        self.assertEqual(0, actual["process_exit"])
        self.assertTrue(actual["response_complete"])
        self.assertTrue(actual["artifacts_complete"])
        self.assertTrue(actual["technical_accepted"])
        self.assertEqual(1, len(case.store.list_attempts(attempt["work_id"])))
        finishes = [event for event in case.store.events()
                    if event["kind"] == "attempt_finished" and event["data"]["attempt_id"] == attempt["attempt_id"]]
        self.assertEqual(1, len(finishes))
        return actual

    def assert_pending(self, runtime, attempt, caught):
        self.assertIsInstance(caught, RuntimeError)
        self.assertNotIsInstance(caught, TaskflowError)
        files = list(runtime.glob("receipt-write-pending-*.json"))
        self.assertEqual(1, len(files))
        self.assertEqual(str(files[0]), caught.receipt["path"])
        self.assertEqual(sha256(files[0].read_bytes()), caught.receipt["sha256"])
        self.assertEqual(files[0].stat().st_size, caught.receipt["size"])
        self.assertTrue(caught.receipt["location"].strip())
        note = read_json(files[0])
        self.assertEqual("terminal_receipt_write_pending", note["kind"])
        for key in ("attempt_id", "work_id", "intent_id", "runtime_path", "request_digest"):
            self.assertEqual(attempt[key], note[key])
        self.assertFalse(note["automatic_retry"])
        self.assertEqual(0, note["new_model_calls"])
        self.assertTrue(note["busy_errors"])
        for name in ("manifest.json", "termination.json", "collection.json"):
            binding = note["bindings"][name]
            self.assertEqual(str(runtime / name), binding["path"])
            self.assertEqual(sha256((runtime / name).read_bytes()), binding["sha256"])
            self.assertEqual((runtime / name).stat().st_size, binding["size"])
        return files[0]

    def test_real_short_write_lock_releases_and_receives_same_original_once(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db) as release:
            timer = threading.Timer(.12, release.set)
            timer.start()
            try:
                with patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("no repeated model")):
                    received = self.receive(case, runtime, attempt, result, wait_seconds=.8)
            finally:
                timer.cancel()
                timer.join()
        self.assertEqual(received, self.assert_one_terminal(case, attempt))
        self.assertEqual("CALL\n", (Path(runner._validate(runtime)["cwd"]) / "model-invocations.txt").read_text())

    def test_collecting_commit_then_finish_lock_does_not_mark_collecting_twice(self):
        case, runtime, attempt, result = self.ended()
        mark = case.store.mark_collecting
        state = {"lock": None, "timer": None}

        def mark_then_lock(*args, **kwargs):
            current = mark(*args, **kwargs)
            state["lock"] = self.writer(case.db)
            release = state["lock"].__enter__()
            state["timer"] = threading.Timer(.12, release.set)
            state["timer"].start()
            return current

        try:
            with patch.object(case.store, "mark_collecting", side_effect=mark_then_lock) as marks:
                self.receive(case, runtime, attempt, result, mark_collecting=True, wait_seconds=.8)
            self.assertEqual(1, marks.call_count)
        finally:
            if state["timer"] is not None:
                state["timer"].cancel()
                state["timer"].join()
            if state["lock"] is not None:
                state["lock"].__exit__(None, None, None)
        self.assert_one_terminal(case, attempt)

    def test_committed_finish_then_busy_is_received_without_duplicate_finish(self):
        case, runtime, attempt, result = self.ended()
        finish = case.store.finish_attempt

        def committed_then_busy(*args, **kwargs):
            finish(*args, **kwargs)
            raise operational_error(sqlite3.SQLITE_BUSY)

        with patch.object(case.store, "finish_attempt", side_effect=committed_then_busy) as finishes:
            self.receive(case, runtime, attempt, result, wait_seconds=0)
        self.assertEqual(1, finishes.call_count)
        self.assert_one_terminal(case, attempt)
        self.assertEqual([], list(runtime.glob("receipt-write-pending-*.json")))

    def test_only_busy_locked_primary_codes_are_retried(self):
        for code in (sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_BUSY | (2 << 8), None):
            with self.subTest(code=code):
                case, runtime, attempt, result = self.ended()
                finish = case.store.finish_attempt
                calls = []

                def first_busy(*args, **kwargs):
                    calls.append(kwargs.get("busy_timeout_seconds"))
                    if len(calls) == 1:
                        if code is None:
                            # Python3.10 SQLite exceptions lack errorcode.
                            raise sqlite3.OperationalError("database is locked")
                        raise operational_error(code, "controlled extended SQLite lock fault")
                    return finish(*args, **kwargs)

                with patch.object(case.store, "finish_attempt", side_effect=first_busy):
                    self.receive(case, runtime, attempt, result, wait_seconds=.5)
                self.assertEqual(2, len(calls))
                self.assertTrue(all(0 <= value <= .5 for value in calls))
                self.assert_one_terminal(case, attempt)

    def test_nonbusy_sqlite_error_is_not_retried_or_relabelled_pending(self):
        case, runtime, attempt, result = self.ended()
        before = case.store.events()
        error = operational_error(sqlite3.SQLITE_IOERR, "database is locked")
        with patch.object(case.store, "finish_attempt", side_effect=error) as finishes:
            with self.assertRaises(sqlite3.OperationalError) as caught:
                self.receive(case, runtime, attempt, result, wait_seconds=.5)
        self.assertIs(error, caught.exception)
        self.assertEqual(1, finishes.call_count)
        self.assertEqual(before, case.store.events())
        self.assertEqual([], list(runtime.glob("receipt-write-pending-*.json")))

    def test_builtin_oserror_subclass_proves_supervisor_did_not_start(self):
        case, runtime, work_id = self.queued()
        attempt = case.store.claim("fixture-worker", [work_id])
        case.store.mark_starting(attempt["attempt_id"])
        with patch.object(runner.subprocess, "Popen", side_effect=BlockingIOError("fixture resource unavailable")):
            result = runner.run_attempt(runtime)
        self.assertEqual("BlockingIOError", result["termination"]["exception"]["type"])
        actual = self.receive(case, runtime, case.store.get_attempt(attempt["attempt_id"]), result)
        self.assertEqual("failed", actual["state"])
        self.assertEqual("not_started", actual["process_state"])
        self.assertEqual(1, len(case.store.list_attempts(work_id)))
        self.assertEqual(actual, execution.reconcile(case.store, attempt["attempt_id"]))

    def test_unrecognized_oserror_does_not_resolve_pending_or_repeat_finish(self):
        class NeutralCustomOSError(OSError):
            pass

        case, runtime, work_id = self.queued()
        attempt = case.store.claim("fixture-worker", [work_id])
        case.store.mark_starting(attempt["attempt_id"])
        with patch.object(runner.subprocess, "Popen", side_effect=NeutralCustomOSError("fixture failure")):
            result = runner.run_attempt(runtime)
        attempt = case.store.get_attempt(attempt["attempt_id"])
        with self.writer(case.db):
            with self.assertRaises(execution.ReceiptWritePending):
                self.receive(case, runtime, attempt, result, wait_seconds=.05)
        for _ in range(2):
            with self.assertRaises(execution.ReceiptResultUncertain):
                execution.reconcile(case.store, attempt["attempt_id"])
        current = case.store.get_attempt(attempt["attempt_id"])
        self.assertEqual("uncertain", current["state"])
        self.assertEqual("unknown", current["process_state"])
        self.assertEqual([], list(runtime.glob("receipt-write-received-*.json")))
        finished = [event for event in case.store.events() if event["kind"] == "attempt_finished"
                    and event["data"]["attempt_id"] == attempt["attempt_id"]]
        self.assertEqual(1, len(finished))
        self.assertEqual(1, len(case.store.list_attempts(work_id)))

    def test_persistent_lock_has_bounded_pending_then_canonical_reconcile_recovers(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db):
            started = time.monotonic()
            with self.assertRaises(execution.ReceiptWritePending) as caught:
                self.receive(case, runtime, attempt, result, mark_collecting=True, wait_seconds=.08)
            self.assertLess(time.monotonic() - started, 2)
            pending = self.assert_pending(runtime, attempt, caught.exception)
        original_pending = pending.read_bytes()
        self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
        with patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("never redraw the original")):
            actual = execution.reconcile(case.store, attempt["attempt_id"])
            self.assertEqual(actual, execution.reconcile(case.store, attempt["attempt_id"]))
        self.assertEqual(actual, self.assert_one_terminal(case, attempt))
        self.assertEqual(original_pending, pending.read_bytes())
        self.assertEqual(1, len(list(runtime.glob("receipt-write-received-*.json"))))

    def test_later_reconcile_revalidates_collection_bytes_not_pending_claims(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db):
            with self.assertRaises(execution.ReceiptWritePending) as caught:
                self.receive(case, runtime, attempt, result, wait_seconds=.05)
            pending = self.assert_pending(runtime, attempt, caught.exception)
        before = pending.read_bytes()
        delivery = runtime / "artifacts/delivery.md"
        delivery.chmod(0o600)
        delivery.write_bytes(delivery.read_bytes() + b"Drift after terminal collection")
        with patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("no new process")):
            with self.assertRaises((runner.RunnerError, TaskflowError)):
                execution.reconcile(case.store, attempt["attempt_id"])
        self.assertEqual(before, pending.read_bytes())
        self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual([], list(runtime.glob("receipt-write-received-*.json")))

    def test_existing_terminal_result_must_match_the_same_original(self):
        case, runtime, attempt, result = self.ended()
        actual = self.receive(case, runtime, attempt, result, wait_seconds=.2)
        for field, value in (("sid", "foreign-reviewer"), ("technical_accepted", False)):
            changed = {**result, field: value}
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "original sealed result"):
                self.receive(case, runtime, attempt, changed, wait_seconds=.2)
        self.assertEqual(actual, self.assert_one_terminal(case, attempt))

    def test_invalid_budget_or_unproved_result_is_not_a_receipt_retry(self):
        case, runtime, attempt, result = self.ended()
        for budget in (True, -1, float("nan"), float("inf"), "90"):
            with self.subTest(budget=budget), self.assertRaises(TaskflowError):
                self.receive(case, runtime, attempt, result, wait_seconds=budget)
        for change in ({"process_ended": False}, {"intent_id": "foreign-original"}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.receive(case, runtime, attempt, {**result, **change}, wait_seconds=.2)
        self.assertEqual([], list(runtime.glob("receipt-write-pending-*.json")))

    def test_automatic_author_observer_does_not_open_a_second_receipt_budget(self):
        case = author_fixtures.AuthorSuccessorTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.prepare_source("research_ended")
        attempt = case.store.claim("actual-original-author", ["author-source"])
        case.store.mark_starting(attempt["attempt_id"])

        def on_process(value):
            case.store.record_process(attempt["attempt_id"], value["pid"],
                value.get("start_identity") or "neutral author original pid=" + str(value["pid"]))

        result = runner.run_attempt(case.runtime, on_process=on_process)
        attempt = case.store.get_attempt(attempt["attempt_id"])
        with self.writer(case.store.path):
            with self.assertRaises(execution.ReceiptWritePending):
                self.receive(case, case.runtime, attempt, result, mark_collecting=True, wait_seconds=.05)
        case.attempt = attempt
        record = case.register()
        before = case.store.events()
        started = time.monotonic()
        with patch.object(case.store, "finish_attempt", side_effect=AssertionError("automatic receipt budget must not restart")), \
             patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("no author redraw")):
            with self.assertRaises(execution.ReceiptWritePending):
                advance_author(case.store, record)
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(before, case.store.events())
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))
        self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual(1, len(list(case.runtime.glob("receipt-write-pending-*.json"))))

    def test_resolution_io_failure_preserves_terminal_and_later_reconcile_closes_once(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db):
            with self.assertRaises(execution.ReceiptWritePending) as caught:
                self.receive(case, runtime, attempt, result, wait_seconds=.05)
            pending = self.assert_pending(runtime, attempt, caught.exception)
        original_pending = pending.read_bytes()
        publish = runner._publish

        def fail_only_resolution(path, value):
            if path.name.startswith("receipt-write-received-"):
                raise PermissionError("neutral receipt diagnostic write fault")
            return publish(path, value)

        with patch("taskflow.execution_runner._publish", side_effect=fail_only_resolution), \
             patch.object(case.store, "mark_uncertain", side_effect=AssertionError("terminal must remain terminal")):
            with self.assertRaises(execution.ReceiptEvidenceError) as error:
                execution.reconcile(case.store, attempt["attempt_id"])
        self.assertTrue(error.exception.terminal_received)
        actual = self.assert_one_terminal(case, attempt)
        self.assertEqual(original_pending, pending.read_bytes())
        self.assertEqual([], list(runtime.glob("receipt-write-received-*.json")))
        with patch.object(case.store, "finish_attempt", side_effect=AssertionError("already committed")), \
             patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("no CLI retry")):
            self.assertEqual(actual, execution.reconcile(case.store, attempt["attempt_id"]))
            received = list(runtime.glob("receipt-write-received-*.json"))
            self.assertEqual(1, len(received))
            resolved_bytes = received[0].read_bytes()
            self.assertEqual(actual, execution.reconcile(case.store, attempt["attempt_id"]))
        self.assertEqual(received, list(runtime.glob("receipt-write-received-*.json")))
        self.assertEqual(resolved_bytes, received[0].read_bytes())
        self.assertEqual(original_pending, pending.read_bytes())

    def test_tampered_pending_identity_binding_path_or_membership_cannot_receive(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db):
            with self.assertRaises(execution.ReceiptWritePending) as caught:
                self.receive(case, runtime, attempt, result, wait_seconds=.05)
            pending = self.assert_pending(runtime, attempt, caught.exception)
        raw = pending.read_bytes()
        original = json.loads(raw)
        foreign = case.root / "same-byte-foreign-collection.json"
        foreign.write_bytes((runtime / "collection.json").read_bytes())
        changes = [
            lambda value: value.update(role="foreign-role"),
            lambda value: value.update(cli_sid="foreign-sid"),
            lambda value: value.update(attempt_id="foreign-attempt"),
            lambda value: value["bindings"]["collection.json"].update(path=str(foreign)),
            lambda value: value["bindings"].pop("manifest-lock.json"),
        ]
        try:
            for change in changes:
                value = json.loads(raw)
                change(value)
                pending.chmod(0o600)
                pending.write_text(json.dumps(value))
                with self.subTest(change=change), \
                     patch.object(case.store, "finish_attempt", side_effect=AssertionError("invalid pending source")), \
                     self.assertRaises((TaskflowError, runner.RunnerError)):
                    execution.reconcile(case.store, attempt["attempt_id"])
                self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
        finally:
            pending.chmod(0o600)
            pending.write_bytes(raw)
        self.assertEqual(original, read_json(pending))

    def test_nonregular_pending_note_is_rejected_with_a_real_cli_timeout_bound(self):
        case, runtime, attempt, result = self.ended()
        with self.writer(case.db):
            with self.assertRaises(execution.ReceiptWritePending) as caught:
                self.receive(case, runtime, attempt, result, wait_seconds=.05)
            pending = self.assert_pending(runtime, attempt, caught.exception)
        saved = pending.with_name("original-pending-note.json")
        pending.rename(saved)
        try:
            for kind in ("symlink", "fifo"):
                if kind == "symlink":
                    pending.symlink_to(saved)
                else:
                    os.mkfifo(pending)
                try:
                    completed = subprocess.run([sys.executable, "-B", "-m", "taskflow", "execution",
                        "--db", str(case.db), "reconcile", attempt["attempt_id"]],
                        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=3)
                    self.assertEqual(2, completed.returncode, completed.stderr)
                    self.assertTrue(json.loads(completed.stderr)["error"])
                    self.assertEqual("running", case.store.get_attempt(attempt["attempt_id"])["state"])
                finally:
                    pending.unlink()
        finally:
            saved.rename(pending)

    def test_normal_flow_cli_exits_pending_without_uncertain_flow_or_second_model(self):
        case = flow_fixtures.ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.script.write_text("import pathlib\nwith pathlib.Path('model-invocations.txt').open('a') as fp:fp.write('CALL\\n')\n" + case.script.read_text())
        definition = case.definition("receipt-flow", outcome="accepted")
        case.register(definition)
        advance_workflow(case.store, definition["flow_id"])
        ready = advance_workflow(case.store, definition["flow_id"])
        runtime = Path(ready["state"]["current_runtime"])
        marker, held = case.root / "actual-cli-ended", case.root / "sqlite-reserved"
        program = r'''
import pathlib,sys,time
from unittest.mock import patch
from taskflow import execution,execution_runner
from taskflow.execution_flow import run_workflows
from taskflow.execution_store import ExecutionStore
import json
actual_run=execution_runner.run_attempt
actual_receive=execution._receive_ended_result
marker,held=map(pathlib.Path,sys.argv[1:3])
def ended(root,on_process=None,**kwargs):
 def identity(value):
  value={**value,'start_identity':value.get('start_identity') or 'neutral real CLI subprocess'}
  if on_process:on_process(value)
 result=actual_run(root,on_process=identity,**kwargs)
 marker.write_text('actual CLI and collection completed')
 deadline=time.monotonic()+6
 while not held.exists():
  if time.monotonic()>deadline:raise RuntimeError('test writer handshake not received')
  time.sleep(.01)
 return result
def bounded(store,attempt,result,**kwargs):
 return actual_receive(store,attempt,result,**{**kwargs,'wait_seconds':.1})
with patch.object(execution_runner,'run_attempt',side_effect=ended),patch.object(execution,'_receive_ended_result',side_effect=bounded):
 try:
  result=run_workflows(ExecutionStore(pathlib.Path(sys.argv[3])),[sys.argv[4]],allow_model=True)
 except execution.ReceiptWritePending as error:
  print(json.dumps({'error':str(error),'state':'terminal_receipt_pending','receipt':error.receipt,'automatic_retry':False}),file=sys.stderr)
  raise SystemExit(2)
 print(json.dumps(result))
'''
        process = subprocess.Popen([sys.executable, "-B", "-c", program, str(marker), str(held),
            str(case.store.path), definition["flow_id"]], cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 8
            while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(marker.is_file(), "real CLI must end before test writer acquires its reservation")
            with self.writer(case.store.path):
                held.write_text("the test owns BEGIN IMMEDIATE")
                started = time.monotonic()
                stdout, stderr = process.communicate(timeout=4)
                self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(2, process.returncode, (stdout, stderr))
            error = json.loads(stderr)
            self.assertEqual("terminal_receipt_pending", error["state"])
            self.assertFalse(error["automatic_retry"])
            attempts = case.store.list_attempts(ready["state"]["current_work_id"])
            self.assertEqual(1, len(attempts))
            attempt = attempts[0]
            self.assertEqual("running", attempt["state"])
            current = case.store.get_flow(definition["flow_id"])
            self.assertEqual("active", current["state"]["status"])
            self.assertIsNone(current["state"].get("exception"))
            self.assertFalse(any(event["kind"] == "uncertain" and event["data"].get("attempt_id") == attempt["attempt_id"]
                                 for event in case.store.events()))
            self.assertEqual("CALL\n", (runtime.parent / "work/model-invocations.txt").read_text())
            self.assertEqual(1, len(list(runtime.glob("receipt-write-pending-*.json"))))
            self.assertEqual("waiting", execution.run_worker(case.store, "must-not-redraw", True,
                [attempt["work_id"]])["state"])
            with patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("reconcile is receipt only")):
                execution.reconcile(case.store, attempt["attempt_id"])
            self.assert_one_terminal(case, attempt)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=3)

    def test_cold_normal_consumer_default_budget_receives_after_real_short_lock(self):
        case = flow_fixtures.ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        marker, held = case.root / "fake-cli-before-terminal", case.root / "parent-reserved-db"
        fixture = case.script.read_text()
        terminal = "print(json.dumps({'type':'turn.completed'}),flush=True)"
        self.assertEqual(1, fixture.count(terminal))
        handshake = ("pathlib.Path(" + repr(str(marker)) + ").write_text('ready for terminal boundary')\n"
                     "deadline=time.monotonic()+8\n"
                     "while not pathlib.Path(" + repr(str(held)) + ").exists():\n"
                     " if time.monotonic()>deadline:raise RuntimeError('parent test handshake absent')\n"
                     " time.sleep(.01)\n")
        case.script.write_text("import pathlib\nwith pathlib.Path('model-invocations.txt').open('a') as fp:fp.write('CALL\\n')\n"
                               + fixture.replace(terminal, handshake + terminal))
        definition = case.definition("cold-receipt", outcome="accepted")
        case.register(definition)
        advance_workflow(case.store, definition["flow_id"])
        ready = advance_workflow(case.store, definition["flow_id"])
        runtime = Path(ready["state"]["current_runtime"])
        work_id = ready["state"]["current_work_id"]
        request_before = (runtime / "request.md").read_bytes()
        manifest_before = (runtime / "manifest.json").read_bytes()
        repository = Path(__file__).resolve().parents[1]
        source_names = ("execution.py", "execution_store.py", "execution_flow.py",
                        "execution_completion.py", "execution_runner.py")
        source_before = {name: sha256((repository / "taskflow" / name).read_bytes()) for name in source_names}
        program = ("import json,sys; from pathlib import Path; "
                   "from taskflow.execution_store import ExecutionStore; "
                   "from taskflow.execution_flow import run_workflows; "
                   "print(json.dumps(run_workflows(ExecutionStore(Path(sys.argv[1])),"
                   "[sys.argv[2]],allow_model=True)))")
        argv = [sys.executable, "-B", "-c", program, str(case.store.path), definition["flow_id"]]
        process = subprocess.Popen(argv, cwd=repository, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pid = process.pid
        try:
            deadline = time.monotonic() + 8
            attempt = None
            while process.poll() is None and time.monotonic() < deadline:
                attempts = case.store.list_attempts(work_id)
                if marker.exists() and len(attempts) == 1 and attempts[0]["state"] in ("running", "uncertain"):
                    attempt = attempts[0]
                    break
                time.sleep(.01)
            self.assertIsNotNone(attempt, "fake CLI must reach terminal boundary with its original managed launch retained")
            with self.writer(case.store.path):
                held.write_text("actual parent BEGIN IMMEDIATE acquired")
                deadline = time.monotonic() + 5
                while not (runtime / "collection.json").exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue((runtime / "collection.json").is_file())
                self.assertEqual(attempt["state"], case.store.get_attempt(attempt["attempt_id"])["state"])
                # The real default receiver is currently contending on this
                # reservation. No monkeypatch or timeout policy is installed.
                time.sleep(.12)
            stdout, stderr = process.communicate(timeout=8)
            self.assertEqual(0, process.returncode, (stdout, stderr))
            self.assertEqual("", stderr)
            output = json.loads(stdout)
            final = output["flows"][0]
            self.assertEqual("accepted", final["state"]["status"])
            actual = self.assert_one_terminal(case, attempt)
            self.assertEqual(1, len(case.store.list_attempts(work_id)))
            self.assertEqual("CALL\n", (runtime.parent / "work/model-invocations.txt").read_text())
            self.assertEqual(request_before, (runtime / "request.md").read_bytes())
            self.assertEqual(manifest_before, (runtime / "manifest.json").read_bytes())
            self.assertEqual([], list(runtime.glob("receipt-write-pending-*.json")))
            source_after = {name: sha256((repository / "taskflow" / name).read_bytes()) for name in source_names}
            self.assertEqual(source_before, source_after)
            self.cold_adoption_proof = {"nature": "neutral_fake_cli_not_external_model", "argv": argv, "consumer_pid": pid,
                "cli_exit_code": process.returncode, "stdout": stdout, "stderr": stderr,
                "receiver_budget": "unchanged actual default90; no function, timeout or environment patch",
                "actual_sqlite_lock": "independent writer BEGIN IMMEDIATE held until sealed collection, then .12 seconds, released",
                "original_work_id": work_id, "original_attempt_id": actual["attempt_id"], "original_cli_sid": actual["cli_sid"],
                "original_process_receipt": read_json(runtime / "process.json"),
                "original_pre_receipt_state": attempt["state"],
                "startup_identity_limit": "Unmodified _identity may retain unknown ps identity in a restricted sandbox; the bound original termination/collection still receives the same attempt. No identity is substituted in this cold test.",
                "original_request_sha256": sha256(request_before), "manifest_sha256": sha256(manifest_before),
                "terminal_attempt_events": 1, "attempts_for_original_work": 1, "fake_cli_invocations": 1,
                "terminal_flow": {"flow_id": final["flow_id"], "status": final["state"]["status"], "receipt": final["state"]["receipt"]},
                "source_five_hashes_before": source_before, "source_five_hashes_after": source_after,
                "source_bytes_unchanged": True, "production_database_writes": 0, "external_model_calls": 0}
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
