"""Neutral regressions from the independent receipt code review; no models."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution, execution_flow
from taskflow.contracts import canonical
from taskflow.execution_store import ExecutionStore


class ReceiptReviewRegressions(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("neutral-policy", {"concurrency": 1, "roles": {
            "research": {"model": "unused-neutral-fixture", "reasoning_effort": "fixture", "timeout_seconds": 1}}})

    def test_runtime_less_terminal_attempt_remains_idempotent_in_api_and_cli(self):
        for state in ("completed", "failed", "cancelled"):
            with self.subTest(state=state):
                identity = "old-" + state
                original = {"attempt_id": identity, "work_id": identity + "-work",
                            "task_id": "neutral", "state": state, "modifies": False, "runtime_path": None}
                with self.store._db(True) as db:
                    db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?)", (
                        identity, original["work_id"], "neutral", state, 0, canonical(original).decode()))
                before = self.store.events()
                self.assertEqual(original, execution.reconcile(self.store, identity))
                stdout, stderr = io.StringIO(), io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = execution.main(["--db", str(self.store.path), "reconcile", identity])
                self.assertEqual(0, code, stderr.getvalue())
                self.assertEqual(original, json.loads(stdout.getvalue()))
                self.assertEqual("", stderr.getvalue())
                self.assertEqual(before, self.store.events())
                self.assertEqual(original, self.store.get_attempt(identity))

    def test_another_ready_workflow_cannot_hide_a_pending_receipt(self):
        with self.store._db(True) as db:
            for identity, status in (("pending-flow", "running"), ("ready-flow", "ready")):
                work_id = identity + "-work"
                state = {"status": "active", "initialized": True, "current_work_id": work_id,
                         "stage": "quality", "history": [], "archives": []}
                definition = canonical({"policy_id": "neutral-policy"})
                db.execute("INSERT INTO execution_flows VALUES(?,?,?,?,?,?,?)", (
                    identity, definition.decode(), hashlib.sha256(definition).hexdigest(),
                    canonical(state).decode(), 0, 0, "[]"))
                work = {"work_id": work_id, "task_id": identity, "state": status}
                db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (
                    work_id, identity, "unused-candidate", work_id, status, canonical(work).decode()))
        before = [self.store.get_flow(identity) for identity in ("pending-flow", "ready-flow")]
        actual_advance = execution_flow.advance_workflow
        note = {"path": str(self.root / "pending-original.json")}
        for signal in (execution.ReceiptWritePending(note),
                       execution.ReceiptEvidenceError(self.root / "resolution.json", OSError("disk busy"),
                                                      terminal_received=True)):
            with self.subTest(signal=type(signal).__name__):
                def advance_boundary(store, identity):
                    if identity == "pending-flow":
                        raise signal
                    return actual_advance(store, identity)

                stdout, stderr = io.StringIO(), io.StringIO()
                with patch.object(execution_flow, "advance_workflow", side_effect=advance_boundary), \
                        patch.object(execution, "run_worker", side_effect=AssertionError("no model permission")), \
                        redirect_stdout(stdout), redirect_stderr(stderr):
                    code = execution.main(["--db", str(self.store.path), "flow-run",
                                           "--flow-id", "pending-flow", "--flow-id", "ready-flow"])
                self.assertEqual(2, code)
                self.assertEqual("", stdout.getvalue())
                error = json.loads(stderr.getvalue())
                self.assertFalse(error["automatic_retry"])
                if isinstance(signal, execution.ReceiptWritePending):
                    self.assertEqual("terminal_receipt_pending", error["state"])
                    self.assertEqual(note, error["receipt"])
                else:
                    self.assertEqual("terminal_receipt_evidence_error", error["state"])
                    self.assertTrue(error["terminal_received"])
                self.assertEqual(before, [self.store.get_flow(identity)
                                          for identity in ("pending-flow", "ready-flow")])
                self.assertEqual("ready", self.store.get_work("ready-flow-work")["state"])
                self.assertEqual([], self.store.list_attempts("ready-flow-work"))

    def test_externally_running_workflow_cannot_hide_a_pending_receipt(self):
        with self.store._db(True) as db:
            for identity in ("pending-flow", "external-flow"):
                work_id = identity + "-work"
                state = {"status": "active", "initialized": True, "current_work_id": work_id,
                         "stage": "quality", "history": [], "archives": []}
                definition = canonical({"policy_id": "neutral-policy"})
                db.execute("INSERT INTO execution_flows VALUES(?,?,?,?,?,?,?)", (
                    identity, definition.decode(), hashlib.sha256(definition).hexdigest(),
                    canonical(state).decode(), 0, 0, "[]"))
                work = {"work_id": work_id, "task_id": identity, "state": "running"}
                db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (
                    work_id, identity, "unused-candidate", work_id, "running", canonical(work).decode()))
        before = [self.store.get_flow(identity) for identity in ("pending-flow", "external-flow")]
        note = {"path": str(self.root / "pending-original.json")}

        def advance_boundary(store, identity):
            if identity == "pending-flow":
                raise execution.ReceiptWritePending(note)
            return store.get_flow(identity)

        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(execution_flow, "advance_workflow", side_effect=advance_boundary), \
                patch.object(execution_flow.time, "sleep", side_effect=AssertionError("must report pending")), \
                patch.object(execution, "run_worker", side_effect=AssertionError("no new process")), \
                redirect_stdout(stdout), redirect_stderr(stderr):
            code = execution.main(["--db", str(self.store.path), "flow-run", "--allow-model",
                                   "--flow-id", "pending-flow", "--flow-id", "external-flow"])
        self.assertEqual(2, code)
        self.assertEqual("", stdout.getvalue())
        error = json.loads(stderr.getvalue())
        self.assertEqual("terminal_receipt_pending", error["state"])
        self.assertEqual(note, error["receipt"])
        self.assertEqual(before, [self.store.get_flow(identity) for identity in ("pending-flow", "external-flow")])

    def test_pending_receipt_reports_after_other_ready_work_cannot_be_claimed(self):
        with self.store._db(True) as db:
            for identity, status in (("pending-flow", "running"), ("ready-flow", "ready")):
                work_id = identity + "-work"
                state = {"status": "active", "initialized": True, "current_work_id": work_id,
                         "stage": "quality", "history": [], "archives": []}
                definition = canonical({"policy_id": "neutral-policy"})
                db.execute("INSERT INTO execution_flows VALUES(?,?,?,?,?,?,?)", (
                    identity, definition.decode(), hashlib.sha256(definition).hexdigest(),
                    canonical(state).decode(), 0, 0, "[]"))
                work = {"work_id": work_id, "task_id": identity, "state": status}
                db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (
                    work_id, identity, "unused-candidate", work_id, status, canonical(work).decode()))
        note = {"path": str(self.root / "pending-original.json")}

        def advance_boundary(store, identity):
            if identity == "pending-flow":
                raise execution.ReceiptWritePending(note)
            return store.get_flow(identity)

        with patch.object(execution_flow, "advance_workflow", side_effect=advance_boundary), \
                patch.object(execution, "run_worker", return_value={"state": "waiting"}) as worker:
            with self.assertRaises(execution.ReceiptWritePending):
                execution_flow.run_workflows(self.store, ["pending-flow", "ready-flow"],
                                             allow_model=True, poll_seconds=.001)
        worker.assert_called_once_with(self.store, "continuation-consumer", True, ["ready-flow-work"])
        self.assertEqual("ready", self.store.get_work("ready-flow-work")["state"])
        self.assertEqual([], self.store.list_attempts("ready-flow-work"))

    def test_pending_receipt_allows_another_ready_work_to_finish(self):
        with self.store._db(True) as db:
            for identity, status in (("pending-flow", "running"), ("ready-flow", "ready")):
                work_id = identity + "-work"
                state = {"status": "active", "initialized": True, "current_work_id": work_id,
                         "stage": "quality", "history": [], "archives": []}
                definition = canonical({"policy_id": "neutral-policy"})
                db.execute("INSERT INTO execution_flows VALUES(?,?,?,?,?,?,?)", (
                    identity, definition.decode(), hashlib.sha256(definition).hexdigest(),
                    canonical(state).decode(), 0, 0, "[]"))
                work = {"work_id": work_id, "task_id": identity, "state": status}
                db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (
                    work_id, identity, "unused-candidate", work_id, status, canonical(work).decode()))
        note = {"path": str(self.root / "pending-original.json")}

        def advance_boundary(store, identity):
            if identity == "pending-flow":
                raise execution.ReceiptWritePending(note)
            return store.get_flow(identity)

        def finish_other(store, worker_id, allow_model, work_ids):
            state = store.get_flow("ready-flow")["state"]
            state.update(status="accepted", receipt="fixture-receipt", current_work_id=None)
            with store._db(True) as db:
                db.execute("UPDATE execution_flows SET state=? WHERE flow_id=?",
                           (canonical(state).decode(), "ready-flow"))
            return {"state": "completed"}

        with patch.object(execution_flow, "advance_workflow", side_effect=advance_boundary), \
                patch.object(execution, "run_worker", side_effect=finish_other) as worker:
            with self.assertRaises(execution.ReceiptWritePending):
                execution_flow.run_workflows(self.store, ["pending-flow", "ready-flow"],
                                             allow_model=True, poll_seconds=.001)
        worker.assert_called_once_with(self.store, "continuation-consumer", True, ["ready-flow-work"])
        self.assertEqual("accepted", self.store.get_flow("ready-flow")["state"]["status"])
