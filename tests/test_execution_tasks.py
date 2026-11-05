"""Shared task primitives: no model, network, business choice or global state."""
import copy
import fcntl
import json
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import Mock, patch

from taskflow.contracts import TaskflowError
from taskflow import execution_tasks as tasks
from taskflow.execution_runner import prepare_attempt
from tests import test_execution_role_session as session_fixtures


class TaskPrimitivesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()

    def test_render_expands_keys_values_and_rejects_unknown_or_colliding_keys(self):
        original = {"{{work}}": {"path": ["{{work}}/answer.json", 3]}}
        before = copy.deepcopy(original)
        self.assertEqual({"/owned/work": {"path": ["/owned/work/answer.json", 3]}},
                         tasks.render(original, {"work": "/owned/work"}))
        self.assertEqual(before, original)
        with self.assertRaisesRegex(TaskflowError, "unknown"):
            tasks.render({"{{missing}}": "read"}, {})
        with self.assertRaisesRegex(TaskflowError, "collide"):
            tasks.render({"{{work}}": "read", "/owned/work": "write"}, {"work": "/owned/work"})

    def test_publish_is_idempotent_and_never_replaces_original(self):
        path = self.root / "new/spec.json"
        tasks.publish_once(path, {"value": 1})
        before = path.read_bytes()
        tasks.publish_once(path, {"value": 1})
        with self.assertRaises(TaskflowError):
            tasks.publish_once(path, {"value": 2})
        self.assertEqual(before, path.read_bytes())
        with self.assertRaises(TaskflowError):
            tasks.publish_once(path, {"value": True})
        raw = self.root / "bytes/source.bin"
        tasks.publish_copy(raw, b"original")
        tasks.publish_copy(raw, b"original")
        with self.assertRaises(TaskflowError):
            tasks.publish_copy(raw, b"different")
        self.assertEqual(b"original", raw.read_bytes())

    def test_invocations_distinguish_real_turn_and_stage(self):
        spec = {"flow_id": "neutral", "directory": str(self.root)}
        first = tasks.invocation(spec, "facts")
        self.assertEqual(first, tasks.invocation(spec, "facts"))
        for other in (tasks.invocation(spec, "facts", 1), tasks.invocation(spec, "score")):
            self.assertNotEqual(first["work_id"], other["work_id"])
            self.assertNotEqual(first["runtime"], other["runtime"])

    def test_role_operations_share_existing_install_barrier_but_can_run_in_parallel(self):
        store = Mock(path=self.root / "state.sqlite3")
        barrier = Path(str(store.path) + ".continuation.lock")
        with tasks.task_lock(self.root / "a", store=store), tasks.task_lock(self.root / "b", store=store):
            with barrier.open("a+b") as install:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(install, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with barrier.open("a+b") as install:
            fcntl.flock(install, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(install, fcntl.LOCK_UN)
        forbidden = self.root / "symlink-task"
        forbidden.mkdir()
        (forbidden / "role-task.lock").symlink_to(barrier)
        with self.assertRaises(TaskflowError):
            with tasks.task_lock(forbidden):
                self.fail("symlink lock admitted")

    def test_get_and_original_attempt_do_not_create_or_redraw(self):
        store = Mock()
        store.get_flow.return_value = {"definition": {"kind": "solver_task_v1"}}
        tasks.get_task(store, "one", "solver_task_v1")
        with self.assertRaises(TaskflowError):
            tasks.get_task(store, "one", "assessment_task_v1")
        current = {"work_id": "work"}
        store.list_attempts.return_value = []
        self.assertIsNone(tasks.original_attempt(store, current))
        store.list_attempts.return_value = [{"state": "completed"}]
        self.assertEqual({"state": "completed"}, tasks.original_attempt(store, current))
        store.list_attempts.return_value = [{"state": "running", "attempt_id": "actual"}]
        with patch("taskflow.execution_tasks.reconcile", return_value={"state": "uncertain"}) as reconcile:
            self.assertEqual({"state": "uncertain"}, tasks.original_attempt(store, current))
            reconcile.assert_called_once_with(store, "actual", resume_pending=False)
        store.list_attempts.return_value = [{}, {}]
        with self.assertRaises(TaskflowError):
            tasks.original_attempt(store, current)

    def test_native_view_rejects_other_batches_control_and_scratch_redirection(self):
        task = self.root / "task"
        public = task / "public"
        work = task / "invocations/facts-000/work"
        scratch = work.parent / "scratch"
        home = self.root / "role-home"
        private = self.root / "other-batch-private"
        for path in (public, work, scratch, home, private):
            path.mkdir(parents=True, exist_ok=True)
        source = public / "prompt.md"
        source.write_text("Neutral public input.")
        baseline = {":root": "deny", ":minimal": "read", ":tmpdir": "deny", ":slash_tmp": "deny",
                    "/private/tmp": "deny", str(public): "read", str(work): "write", str(scratch): "write",
                    str(home): "deny", str(Path(sys.base_prefix).resolve()): "read"}
        permissions = {"repository": str(public), "filesystem": baseline.copy(),
                       "python": str(Path(sys.executable).resolve()), "scratch": str(scratch),
                       "probe_sources": [str(source)], "readonly_probe": str(source)}
        runner = {"mode": "native_role", "cli": str(Path(sys.executable).resolve()),
                  "cwd": str(work), "native_permissions": permissions, "role_session": {"home": str(home)}}
        tasks.check_native_view(runner, public, control_root=task, forbidden=[])
        for name in (str(private), str(task / "invocations/facts-000/runtime"), ":user_home"):
            permissions["filesystem"] = {**baseline, name: "read"}
            with self.subTest(grant=name), self.assertRaises(TaskflowError):
                tasks.check_native_view(runner, public, control_root=task, forbidden=[])
        permissions["filesystem"] = {**baseline, str(private): "write"}
        permissions["scratch"] = str(private)
        with self.assertRaisesRegex(TaskflowError, "scratch"):
            tasks.check_native_view(runner, public, control_root=task, forbidden=[])
        permissions["filesystem"] = baseline.copy()
        permissions["scratch"] = str(scratch)
        scratch.rmdir()
        scratch.symlink_to(private, target_is_directory=True)
        with self.assertRaisesRegex(TaskflowError, "canonical"):
            tasks.check_native_view(runner, public, control_root=task, forbidden=[])
        scratch.unlink()
        scratch.mkdir()
        permissions["python"] = str(self.root / "another-interpreter")
        with self.assertRaisesRegex(TaskflowError, "interpreter"):
            tasks.check_native_view(runner, public, control_root=task, forbidden=[])

    def test_prepare_recovers_enqueue_before_state_commit_without_second_enqueue(self):
        case = session_fixtures.RoleSessionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        runtime, runner = case.config()
        manifest = prepare_attempt(runtime, runner)
        current = {"root": str(case.base / "task"), "runtime": str(runtime),
                   "work_id": "neutral-work", "intent_id": runner["intent_id"]}
        definition = {"task_id": "neutral-task", "candidate_id": "neutral-candidate", "policy_id": "policy-v1",
                      "owner": "neutral-owner", "purpose": "explicit neutral test"}
        record = {"definition": definition, "evidence": []}
        existing = {"work_id": current["work_id"], "intent_id": current["intent_id"],
                    "task_id": definition["task_id"], "candidate_id": definition["candidate_id"],
                    "policy_id": definition["policy_id"], "purpose": definition["purpose"],
                    "dispatch_owner": definition["owner"], "role": "quality", "runtime_path": str(runtime),
                    "request_digest": manifest["request"]["sha256"]}
        store = Mock()
        store.get_work.return_value = existing
        with patch("taskflow.execution_tasks.enqueue_spec", side_effect=AssertionError("must not enqueue twice")):
            self.assertEqual(existing, tasks.prepare_work(store, record, current, runner)["work"])
            store.get_work.return_value = {**existing, "policy_id": "changed"}
            with self.assertRaisesRegex(TaskflowError, "policy_id"):
                tasks.prepare_work(store, record, current, runner)


if __name__ == "__main__":
    unittest.main()
