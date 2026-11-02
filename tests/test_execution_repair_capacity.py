"""Isolated transactional tests; source proof is stubbed, never a model result."""
import copy
import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

from taskflow.contracts import TaskflowError, canonical, sha256, write_json
from taskflow.execution_repair_capacity import (CAPACITY, _proof, _retry_work_id, _validate_retry_manifest, _validate_capacity_trace,
    _validate_prepared_witness, advance_failed_capacity_retry, prepare_repair_capacity_budget, retry_claim_guard, RetryCapacityBusy)
from taskflow.execution_store import ExecutionStore, evidence_ref


class GuardedRepairPreparationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.source = self.root / "source.json"
        write_json(self.source, {"neutral": "evidence"})
        self.ref = evidence_ref(self.source, "neutral test evidence")
        self.store.register_flow("parent", {"owner": "controller"}, [self.ref], {"status": "exception"})
        self.store.register_flow("child", {"task_id": "task", "directory": str(self.root / "child")}, [self.ref], {
            "status": "exception", "stage": "repair", "step": 5, "current_work_id": "failed-work",
            "exception": {"reason": "failed"}, "receipt": str(self.source), "archives": [], "history": [], "defects": ["untouched"]})
        self.resolution = {"reason": "one authorized neutral recovery", "authority": self.ref,
                           "failed_attempt_id": "failed"}
        self.proof = {"kind": "bounded_repair_capacity_v1", "failed_attempt_id": "failed",
            "new_work_id": "new-work", "parent_flow_id": "parent", "dependent_revisions": {}, "failed_collection": self.ref,
            "candidate_binding": {"task_id": "task", "candidate_id": "candidate"},
            "previous_child_receipt": self.ref, "previous_parent_receipt": self.ref,
            "direct_user_evidence": self.ref, "failed_manifest": self.ref, "failed_trace": self.ref}

    def prepare(self, resolution=None):
        # Real SQLite/locks and state transitions; proof and archive are isolated
        # boundaries here. This test does not claim the source proof passed.
        with patch("taskflow.execution_repair_capacity._proof", return_value=self.proof), \
             patch("taskflow.execution_repair_capacity._validate_prepared_witness"), \
             patch("taskflow.execution_public_continuation._no_formal"), \
             patch.object(self.store, "get_attempt", return_value={"runtime_path": str(self.root / "runtime")}), \
             patch("taskflow.execution_repair_capacity.archive_full_attempt", return_value={"path": str(self.root / "archive")}):
            return prepare_repair_capacity_budget(self.store, "parent", "child", resolution or self.resolution)

    def test_originals_held_once_and_reentry_preserves_human_resume(self):
        original = self.store.get_flow("child")
        prepared = self.prepare()
        state = prepared["state"]
        self.assertEqual(("active", "repair", 6), (state["status"], state["stage"], state["step"]))
        self.assertEqual(["untouched"], state["defects"])
        self.assertEqual(original["state"]["exception"], state["resolved_exceptions"][0]["exception"])
        self.assertEqual(original["state"]["receipt"], state["resolved_exceptions"][0]["receipt"])
        with self.store._db() as db:
            control = json.loads(db.execute("SELECT data FROM controls WHERE scope=?", ("work:new-work",)).fetchone()["data"])
            self.assertEqual("pause", control["action"])
            self.assertIsNone(db.execute("SELECT 1 FROM work WHERE id='new-work'").fetchone())
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])
        self.store.set_control("work:new-work", "resume", "explicit later user decision")
        self.assertEqual(prepared, self.prepare())
        with self.store._db() as db:
            control = json.loads(db.execute("SELECT data FROM controls WHERE scope=?", ("work:new-work",)).fetchone()["data"])
            self.assertEqual("resume", control["action"])
        self.assertEqual("exception", self.store.get_flow("parent")["state"]["status"])

    def test_concurrent_preparation_is_single_and_conflicting_authority_rejects(self):
        def invoke():
            try:
                return prepare_repair_capacity_budget(self.store, "parent", "child", self.resolution)
            except TaskflowError:
                return None  # An existing consumer lock is a safe fail-closed outcome.
        with patch("taskflow.execution_repair_capacity._proof", return_value=self.proof), \
             patch("taskflow.execution_repair_capacity._validate_prepared_witness"), \
             patch("taskflow.execution_public_continuation._no_formal"), \
             patch.object(self.store, "get_attempt", return_value={"runtime_path": str(self.root / "runtime")}), \
             patch("taskflow.execution_repair_capacity.archive_full_attempt", return_value={"path": str(self.root / "archive")}), \
             ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: invoke(), range(2)))
        self.assertTrue(any(results))
        final = self.store.get_flow("child")
        self.assertEqual(1, len(final["state"]["history"]))
        changed = {**self.resolution, "reason": "another authorization"}
        with self.assertRaises(TaskflowError):
            self.prepare(changed)
        self.assertEqual(final, self.store.get_flow("child"))

    def test_conflicting_control_fails_without_flow_transition(self):
        self.store.set_control("work:new-work", "resume", "unrelated existing decision")
        initial = self.store.get_flow("child")
        with self.assertRaises(TaskflowError):
            self.prepare()
        self.assertEqual(initial, self.store.get_flow("child"))

    def test_interruption_rolls_back_control_and_exact_retry_can_be_prepared_once(self):
        initial = self.store.get_flow("child")
        event = self.store._event
        def fail_on_transition(db, kind, data):
            if kind == "flow_updated":
                raise RuntimeError("neutral interruption after control insertion")
            return event(db, kind, data)
        with patch.object(self.store, "_event", side_effect=fail_on_transition), self.assertRaises(RuntimeError):
            self.prepare()
        self.assertEqual(initial, self.store.get_flow("child"))
        with self.store._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM controls WHERE scope='work:new-work'").fetchone())
        prepared = self.prepare()
        self.assertEqual(1, len(prepared["state"]["history"]))
        self.assertEqual(prepared, self.prepare())


class RelatedExperimentGuardTests(unittest.TestCase):
    def test_active_or_formally_advanced_dependent_fails_before_source_or_dispatch(self):
        parent = {"flow_id": "parent", "definition": {"kind": "author_quality_v1", "task_id": "task",
                  "owner": "controller", "policy_id": "policy"},
                  "state": {"status": "exception", "stage": "child_quality", "receipt": "old",
                            "pending_business_scope": None, "child_flow_id": "child",
                            "candidate_id": "candidate", "candidate_root": "/candidate"}}
        child = {"flow_id": "child", "definition": {"task_id": "task", "owner": "controller", "policy_id": "policy",
                 "initial": {"candidate_id": "candidate", "candidate_root": "/candidate"}}, "state": {}}
        definition = {"kind": "experiment_v1", "source_author_flow_id": "parent"}
        for state in ({"status": "active", "stage": "experiment_v1", "current_work_id": None, "receipt": "unexpected"},
                      {"status": "exception", "stage": "experiment_v1", "receipt": "old", "current_work_id": "solver"},
                      {"status": "exception", "stage": "release", "receipt": "old", "current_work_id": None},
                      {"status": "exception", "stage": "experiment_v1", "receipt": "old", "release": {"exists": True}}):
            store = Mock()
            store.list_flows.return_value = [{"flow_id": "experiment", "definition": definition, "state": state}]
            with self.subTest(state=state), patch("taskflow.execution_repair_capacity._actual") as actual, \
                    self.assertRaisesRegex(TaskflowError, "dependent experiment"):
                _proof(store, parent, child, "failed", {})
            actual.assert_not_called()


class CapacityTraceTests(unittest.TestCase):
    def test_only_one_matching_terminal_capacity_failure_is_eligible(self):
        start = {"type": "thread.started", "thread_id": "neutral-sid"}
        error = {"type": "error", "message": CAPACITY}
        failed = {"type": "turn.failed", "error": {"message": CAPACITY}}
        encode = lambda rows: ("\n".join(json.dumps(row) for row in rows) + "\n").encode()
        _validate_capacity_trace(encode([start, error, failed]), "neutral-sid")
        for events in ([error, failed], [start, error, failed, {"type": "turn.completed"}],
                       [start, error, failed, failed], [start, failed],
                       [start, {"type": "error", "message": "different"}, failed],
                       [start, error, {"type": "turn.failed", "error": {"message": "different"}}],
                       [start, error, failed, {"type": "item.completed"}]):
            with self.subTest(events=events), self.assertRaises(TaskflowError):
                _validate_capacity_trace(encode(events), "neutral-sid")
        with self.assertRaises(TaskflowError):
            _validate_capacity_trace(encode([start, error, failed]), "different-sid")


class RetryBudgetStateTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        p = self.root / "evidence.json"
        write_json(p, {"fixture": "neutral"})
        ref = evidence_ref(p, "neutral fixture")
        parent = self.store.register_flow("parent", {}, [ref], {"status": "exception"})
        self.proof = {"initial_step": 0, "maximum_additional_attempts": 10, "parent_flow_id": "parent",
                      "parent_revision": parent["revision"], "parent_received_event": {"event_seq": parent["event_seq"]},
                      "dependent_revisions": {}, "candidate_binding": {"task_id": "task", "candidate_id": "candidate"},
                      "author_sid": "author", "failed_sid": "original"}
        self.proof["new_work_id"] = _retry_work_id({"flow_id": "retry-flow"}, self.proof, 1)
        self.store.register_flow("retry-flow", {"directory": str(self.root / "flow"), "task_id": "task"}, [ref], {
            "status": "active", "stage": "repair", "step": 1, "repair_capacity_attempt_ordinal": 1,
            "repair_capacity_continuation": self.proof, "history": [], "archives": []})

    def fail_once(self, ordinal, *, message=CAPACITY, timed_out=False, attempt_state="failed"):
        record = self.store.get_flow("retry-flow")
        work_id = _retry_work_id(record, self.proof, ordinal)
        state = {**record["state"], "current_work_id": work_id, "current_node_id": work_id}
        record = self.store.update_flow("retry-flow", record["revision"], state, {"kind": "neutral_work_prepared"})
        runtime = self.root / ("runtime-" + str(ordinal))
        runtime.mkdir()
        write_json(runtime / "manifest.json", {"fixture": "neutral"})
        write_json(runtime / "collection.json", {"fixture": "neutral"})
        sid = "sid-" + str(ordinal)
        events = [{"type": "thread.started", "thread_id": sid}, {"type": "error", "message": message},
                  {"type": "turn.failed", "error": {"message": message}}]
        (runtime / "trace.jsonl").write_text("\n".join(json.dumps(row) for row in events) + "\n")
        attempt = {"attempt_id": "attempt-" + str(ordinal), "work_id": work_id, "intent_id": work_id,
                   "state": attempt_state, "cli_sid": sid, "process_exit": 1}
        collected = {"nature": "real_cli_attempt", "process_ended": True, "model_completed": False,
                     "delivery_complete": False, "technical_accepted": False,
                     "termination": {"spawn_state": "started", "timed_out": timed_out, "returncode": 1}}
        with patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch"), \
             patch("taskflow.execution_repair_capacity._actual", return_value=(runtime, {}, collected)), \
             patch("taskflow.execution_repair_capacity._validate_retry_manifest"), \
             patch("taskflow.execution_batch_binding.validate_batch_source", return_value=True), \
             patch.object(self.store, "list_attempts", return_value=[attempt]), \
             patch("taskflow.execution_repair_capacity.archive_full_attempt", return_value={"path": str(runtime / "archive")}):
            return advance_failed_capacity_retry(self.store, record, attempt)

    def test_ten_bounded_failures_advance_once_and_tenth_stops(self):
        for ordinal in range(1, 11):
            result = self.fail_once(ordinal)
            self.assertEqual(ordinal, len(result["state"]["history"]))
            if ordinal < 10:
                self.assertEqual("active", result["state"]["status"])
                self.assertEqual(ordinal + 1, result["state"]["repair_capacity_attempt_ordinal"])
                self.assertIsNone(result["state"]["current_work_id"])
            else:
                self.assertEqual("exception", result["state"]["status"])
                self.assertEqual("authorized repair capacity retries exhausted", result["state"]["exception"]["reason"])

    def test_noncapacity_or_timeout_never_advances_budget(self):
        for message, timed_out, attempt_state in (("other failure", False, "failed"),
                                                   (CAPACITY, True, "failed"), (CAPACITY, False, "uncertain")):
            with self.subTest(message=message, timed_out=timed_out, state=attempt_state):
                # Separate fixture per case preserves every original attempt.
                case = RetryBudgetStateTests()
                case.setUp()
                try:
                    with self.assertRaises(TaskflowError):
                        case.fail_once(1, message=message, timed_out=timed_out, attempt_state=attempt_state)
                    state = case.store.get_flow("retry-flow")["state"]
                    self.assertEqual((1, []), (state["repair_capacity_attempt_ordinal"], state["history"]))
                finally:
                    case.doCleanups()

    def ready_retry(self, *, cursor=True):
        record = self.store.get_flow("retry-flow")
        work = _retry_work_id(record, self.proof, 1)
        state = {**record["state"], "current_work_id": work if cursor else None,
                 "repair_capacity_continuation": self.proof}
        record = self.store.update_flow("retry-flow", record["revision"], state,
                                        {"kind": "workflow_repair_capacity_continuation_prepared", "issued_work_id": work})
        with self.store._db(True) as db:
            binding = {"flow_id": "retry-flow", "proof_digest": sha256(canonical(self.proof)), "ordinal": 1,
                       "issued_event_seq": record["event_seq"], "task_id": "task", "candidate_id": "candidate", "role": "repair"}
            db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (work, canonical(binding).decode()))
            item = {"work_id": work, "task_id": "task", "candidate_id": "candidate", "intent_id": work,
                    "state": "ready", "role": "repair", "runtime_path": str(self.root / "runtime"),
                    "repair_capacity_binding": binding}
            db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (work, "task", "candidate", work, "ready", canonical(item).decode()))
            self.store._event(db, "dispatch_intent", item)
        return record, work, binding

    def test_claim_holds_lifecycle_locks_and_rechecks_before_yield(self):
        from taskflow.execution_lifecycle import _lifecycle_lock

        evidence = evidence_ref(self.root / "evidence.json", "neutral fixture")
        dependent = self.store.register_flow("dependent", {}, [evidence], {"status": "exception"})
        self.proof["dependent_revisions"] = {"dependent": dependent["revision"]}
        record, work, binding = self.ready_retry()
        attempt = {"attempt_id": "claimed", "work_id": work, "state": "claimed"}
        with patch("taskflow.execution_runner._validate", return_value={"fixture": "neutral"}), \
             patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch") as validate, \
             patch.object(self.store, "claim", return_value=attempt):
            with retry_claim_guard(self.store, "neutral", [work]) as (selected, protected):
                self.assertEqual(attempt, selected)
                self.assertEqual(binding, protected["binding"])
                validate.assert_called_once_with(self.store, record, {"fixture": "neutral"})
                script = ("from pathlib import Path\nfrom taskflow.execution_store import ExecutionStore\n"
                          "from taskflow.execution_lifecycle import _lifecycle_lock\n"
                          "from taskflow.contracts import TaskflowError\nimport sys\n"
                          "s=ExecutionStore(Path(sys.argv[1]))\n"
                          "try:\n with _lifecycle_lock(s,sys.argv[2]): print('UNEXPECTED')\n"
                          "except TaskflowError: print('LOCKED')\n")
                for identity in ("parent", "dependent"):
                    with self.assertRaises(TaskflowError):
                        with _lifecycle_lock(self.store, identity):
                            self.fail("source lock must remain held")
                    child = subprocess.run([sys.executable, "-c", script, str(self.store.path), identity],
                                           capture_output=True, text=True, timeout=10, check=True)
                    self.assertEqual("LOCKED", child.stdout.strip())
        with _lifecycle_lock(self.store, "parent"), _lifecycle_lock(self.store, "dependent"):
            pass
        with patch("taskflow.execution_runner._validate", return_value={}), \
             patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch"), \
             patch.object(self.store, "claim", return_value=attempt):
            with self.assertRaisesRegex(RuntimeError, "neutral worker interrupted"):
                with retry_claim_guard(self.store, "neutral", [work]):
                    raise RuntimeError("neutral worker interrupted")
        with _lifecycle_lock(self.store, "parent"), _lifecycle_lock(self.store, "dependent"):
            pass

    def test_cursor_or_proof_loss_cannot_be_downgraded_to_normal_work(self):
        record, work, _ = self.ready_retry(cursor=False)
        with patch("taskflow.execution_runner._validate") as validate, self.assertRaises(TaskflowError):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("unrecorded work must never be claimed")
        validate.assert_not_called()
        changed = dict(record["state"])
        changed.pop("repair_capacity_continuation")
        self.store.update_flow("retry-flow", record["revision"], changed, {"kind": "neutral_damage"})
        with self.assertRaises(TaskflowError):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("missing proof must not become ordinary work")
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_preparation_rejects_missing_or_changed_issuing_proof_before_files(self):
        from taskflow.execution_flow import _prepare_node_checked

        record, work, _ = self.ready_retry(cursor=False)
        record["definition"].update({"roles": {"repair": {"runner": {}}}, "policy_id": "policy"})
        record["state"]["candidate_id"] = "candidate"
        for change in ("missing", "ordinal"):
            damaged = copy.deepcopy(record)
            if change == "missing":
                damaged["state"].pop("repair_capacity_continuation")
            else:
                damaged["state"]["repair_capacity_attempt_ordinal"] = 2
            with self.subTest(change=change), \
                 patch.object(self.store, "get_candidate", return_value={}), \
                 patch.object(self.store, "get_policy", return_value={"config": {}}), \
                 patch("taskflow.execution_batch.validate_batch_policy", return_value=None), \
                 patch("taskflow.execution_batch.policy_setting", return_value={}), \
                 self.assertRaisesRegex(TaskflowError, "lost its current issuing proof"):
                _prepare_node_checked(self.store, damaged)
            self.assertFalse((self.root / "flow" / "nodes").exists())
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_missing_work_binding_is_rejected_before_enqueue_or_claim(self):
        record = self.store.get_flow("retry-flow")
        work = _retry_work_id(record, self.proof, 1)
        record = self.store.update_flow("retry-flow", record["revision"], record["state"],
                                        {"kind": "workflow_repair_capacity_continuation_prepared", "issued_work_id": work})
        binding = {"flow_id": "retry-flow", "proof_digest": sha256(canonical(self.proof)), "ordinal": 1,
                   "issued_event_seq": record["event_seq"], "task_id": "task", "candidate_id": "candidate", "role": "repair"}
        with self.store._db(True) as db:
            db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (work, canonical(binding).decode()))
        with self.assertRaisesRegex(TaskflowError, "issuing-event binding"):
            self.store.enqueue(work, "task", "candidate", "repair", "policy", intent_id=work,
                               purpose="neutral", request_digest="a" * 64)
        with self.store._db(True) as db:
            db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (work, "task", "candidate", work, "ready",
                canonical({"work_id": work, "task_id": "task", "role": "repair", "state": "ready"}).decode()))
        with self.assertRaisesRegex(TaskflowError, "immutable retry"):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("missing immutable binding cannot become ordinary work")
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_issued_id_cannot_be_reused_by_another_role_task_or_candidate(self):
        record = self.store.get_flow("retry-flow")
        work = _retry_work_id(record, self.proof, 1)
        record = self.store.update_flow("retry-flow", record["revision"], record["state"],
                                        {"kind": "workflow_repair_capacity_continuation_prepared", "issued_work_id": work})
        binding = {"flow_id": "retry-flow", "proof_digest": sha256(canonical(self.proof)), "ordinal": 1,
                   "issued_event_seq": record["event_seq"], "task_id": "task", "candidate_id": "candidate", "role": "repair"}
        with self.store._db(True) as db:
            db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (work, canonical(binding).decode()))
        for field, value in (("role", "quality"), ("task_id", "other-task"),
                             ("candidate_id", "other-candidate"), ("intent_id", "other-intent")):
            args = {"work_id": work, "task_id": "task", "candidate_id": "candidate", "role": "repair",
                    "policy_id": "policy", "intent_id": work, "purpose": "neutral", "request_digest": "a" * 64,
                    "repair_capacity_binding": binding}
            args[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "role, task, candidate or intent"):
                self.store.enqueue(**args)
        item = {"work_id": work, "task_id": "other-task", "candidate_id": "candidate", "intent_id": work,
                "state": "ready", "role": "quality", "repair_capacity_binding": binding}
        with self.store._db(True) as db:
            db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (work, "other-task", "candidate", work, "ready", canonical(item).decode()))
            self.store._event(db, "dispatch_intent", item)
        with self.assertRaisesRegex(TaskflowError, "role, task, candidate or intent"):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("wrong-role or wrong-task issued work cannot be claimed")
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_busy_parent_is_waiting_without_claim_or_state_change(self):
        from taskflow import execution
        from taskflow.execution_lifecycle import _lifecycle_lock

        record, work, _ = self.ready_retry()
        with _lifecycle_lock(self.store, "parent"):
            result = execution.run_worker(self.store, "neutral", True, [work])
        self.assertEqual("waiting", result["state"])
        self.assertEqual(record, self.store.get_flow("retry-flow"))
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])
            self.assertEqual("ready", db.execute("SELECT state FROM work WHERE id=?", (work,)).fetchone()[0])

    def test_missing_index_blocks_only_its_issued_work(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        _, work, _ = self.ready_retry()
        with self.store._db(True) as db:
            db.execute("DELETE FROM repair_capacity_issuance WHERE work_id=?", (work,))
        with self.assertRaisesRegex(TaskflowError, "index is missing"):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("missing issuance index cannot downgrade to ordinary work")
        # The unrelated normal task remains independently runnable.
        self.store.register_policy("fixture", fixture_policy())
        task = make_task(self.root, "neutral")
        execution.capture_candidate(self.store, task, "neutral-candidate")
        self.store.enqueue("ordinary", "neutral", "neutral-candidate", "quality", "fixture",
                           intent_id="ordinary", purpose="neutral fixture", request_digest="a" * 64)
        with retry_claim_guard(self.store, "neutral", None) as (attempt, binding):
            self.assertIsNone(binding)
            self.assertEqual("ordinary", attempt["work_id"])

    def test_unrelated_malformed_dispatch_event_does_not_block_retry(self):
        _, work, _ = self.ready_retry()
        with self.store._db(True) as db:
            db.execute("INSERT INTO events(at,kind,data) VALUES(?,?,?)", (1, "dispatch_intent", '{"unrelated": broken'))
        attempt = {"attempt_id": "claimed", "work_id": work, "state": "claimed"}
        with patch("taskflow.execution_runner._validate", return_value={}), \
             patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch"), \
             patch.object(self.store, "claim", return_value=attempt):
            with retry_claim_guard(self.store, "neutral", [work]) as (selected, binding):
                self.assertEqual(attempt, selected)
                self.assertIsNotNone(binding)

    def test_invalid_retry_runtime_does_not_block_other_ready_work(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        for damage in ("missing_file", "changed_manifest"):
            with self.subTest(damage=damage):
                case = RetryBudgetStateTests()
                case.setUp()
                try:
                    _, retry, _ = case.ready_retry()
                    if damage == "changed_manifest":
                        runtime = case.root / "runtime"
                        runtime.mkdir()
                        write_json(runtime / "manifest.json", {"fixture": "neutral"})
                        write_json(runtime / "manifest-lock.json", {"sha256": "wrong", "size": 0})
                    case.store.register_policy("fixture", fixture_policy())
                    task = make_task(case.root, "neutral")
                    execution.capture_candidate(case.store, task, "neutral-candidate")
                    case.store.enqueue("ordinary", "neutral", "neutral-candidate", "quality", "fixture",
                                       intent_id="ordinary", purpose="neutral fixture", request_digest="a" * 64)
                    with retry_claim_guard(case.store, "neutral", None) as (attempt, binding):
                        self.assertEqual("ordinary", attempt["work_id"])
                        self.assertIsNone(binding)
                    with self.assertRaisesRegex(TaskflowError, "evidence cannot be validated"):
                        with retry_claim_guard(case.store, "neutral", [retry]):
                            self.fail("damaged retry cannot be claimed")
                    self.assertEqual([], case.store.list_attempts(retry))
                finally:
                    case.doCleanups()

    def test_corrupt_ready_work_and_scoped_control_do_not_block_other_tasks(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        for damage in ("work", "control", "global_control", "flow"):
            with self.subTest(damage=damage):
                case = RetryBudgetStateTests()
                case.setUp()
                try:
                    _, retry, _ = case.ready_retry()
                    case.store.register_policy("fixture", fixture_policy())
                    task = make_task(case.root, "neutral")
                    execution.capture_candidate(case.store, task, "neutral-candidate")
                    case.store.enqueue("ordinary", "neutral", "neutral-candidate", "quality", "fixture",
                                       intent_id="ordinary", purpose="neutral fixture", request_digest="a" * 64)
                    with case.store._db(True) as db:
                        if damage == "work":
                            db.execute("UPDATE work SET data=? WHERE id=?", ('{broken', retry))
                        elif damage == "flow":
                            db.execute("UPDATE execution_flows SET state=? WHERE flow_id=?", ('{broken', "retry-flow"))
                        else:
                            scope = "all" if damage == "global_control" else "work:" + retry
                            db.execute("INSERT INTO controls VALUES(?,?)", (scope, '{broken'))
                    if damage == "global_control":
                        with self.assertRaisesRegex(TaskflowError, "global claim control is malformed"):
                            with retry_claim_guard(case.store, "neutral", None):
                                self.fail("global control must fail closed")
                        self.assertEqual([], case.store.list_attempts("ordinary"))
                    else:
                        with retry_claim_guard(case.store, "neutral", None) as (attempt, binding):
                            self.assertEqual("ordinary", attempt["work_id"])
                            self.assertIsNone(binding)
                        expected = ("repair retry flow cannot be validated" if damage == "flow"
                                    else "ready work or its scoped control is malformed")
                        with self.assertRaisesRegex(TaskflowError, expected):
                            with retry_claim_guard(case.store, "neutral", [retry]):
                                self.fail("corrupt retry cannot be claimed")
                        self.assertEqual([], case.store.list_attempts(retry))
                finally:
                    case.doCleanups()

    def test_claim_isolates_local_policy_and_dependency_but_not_global_active_policy(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        for damage in ("policy", "dependency", "active_policy"):
            with self.subTest(damage=damage):
                case = RetryBudgetStateTests()
                case.setUp()
                try:
                    case.store.register_policy("fixture", fixture_policy())
                    task = make_task(case.root, "neutral")
                    execution.capture_candidate(case.store, task, "neutral-candidate")
                    for identity in ("first", "second"):
                        case.store.enqueue(identity, "neutral", "neutral-candidate", "quality", "fixture",
                                           intent_id=identity, purpose="neutral fixture", request_digest="a" * 64)
                    with case.store._db(True) as db:
                        if damage == "active_policy":
                            active = {"attempt_id": "active", "policy_id": "missing", "state": "running"}
                            db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?)",
                                       ("active", "other", "other", "running", 0, canonical(active).decode()))
                        else:
                            row = db.execute("SELECT data FROM work WHERE id='first'").fetchone()
                            work = json.loads(row["data"])
                            work["policy_id" if damage == "policy" else "dependencies"] = (
                                "missing" if damage == "policy" else ["missing"])
                            db.execute("UPDATE work SET data=? WHERE id='first'", (canonical(work).decode(),))
                    if damage == "active_policy":
                        with self.assertRaises(TaskflowError):
                            with retry_claim_guard(case.store, "neutral", None):
                                self.fail("unknown global active policy must fail closed")
                        self.assertEqual([], case.store.list_attempts("second"))
                    else:
                        with retry_claim_guard(case.store, "neutral", None) as (attempt, binding):
                            self.assertEqual("second", attempt["work_id"])
                            self.assertIsNone(binding)
                        self.assertEqual([], case.store.list_attempts("first"))
                finally:
                    case.doCleanups()

    def test_corrupt_exact_issuing_event_never_becomes_ordinary_work(self):
        record, work, _ = self.ready_retry()
        with self.store._db(True) as db:
            db.execute("UPDATE events SET data=? WHERE seq=?",
                       ('{"transition":{"issued_work_id":' + json.dumps(work) + ',broken', record["event_seq"]))
        with self.assertRaisesRegex(TaskflowError, "issuing evidence is malformed"):
            with retry_claim_guard(self.store, "neutral", [work]):
                self.fail("corrupt issuing evidence cannot permit a claim")
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_unrelated_malformed_flow_event_does_not_block_normal_enqueue(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        self.store.register_policy("fixture", fixture_policy())
        task = make_task(self.root, "neutral")
        execution.capture_candidate(self.store, task, "neutral-candidate")
        with self.store._db(True) as db:
            db.execute("INSERT INTO events(at,kind,data) VALUES(?,?,?)", (1, "flow_updated", '{"unrelated": broken'))
        result = self.store.enqueue("ordinary", "neutral", "neutral-candidate", "quality", "fixture",
                                    intent_id="ordinary", purpose="neutral fixture", request_digest="a" * 64)
        self.assertEqual("ready", result["state"])

    def test_busy_parent_during_preparation_leaves_flow_active_unchanged(self):
        from taskflow.execution_flow import advance_workflow
        from taskflow.execution_lifecycle import _lifecycle_lock

        record = self.store.get_flow("retry-flow")
        record = self.store.update_flow("retry-flow", record["revision"],
            {**record["state"], "initialized": True, "current_work_id": None}, {"kind": "neutral_wait"})
        with _lifecycle_lock(self.store, "parent"):
            result = advance_workflow(self.store, "retry-flow")
        self.assertEqual(record, result)
        self.assertEqual(record, self.store.get_flow("retry-flow"))
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_busy_dependent_releases_parent_and_does_not_claim(self):
        from taskflow import execution
        from taskflow.execution_lifecycle import _lifecycle_lock

        evidence = evidence_ref(self.root / "evidence.json", "neutral fixture")
        dependent = self.store.register_flow("dependent", {}, [evidence], {"status": "exception"})
        self.proof["dependent_revisions"] = {"dependent": dependent["revision"]}
        record, work, _ = self.ready_retry()
        with _lifecycle_lock(self.store, "dependent"):
            result = execution.run_worker(self.store, "neutral", True, [work])
            with _lifecycle_lock(self.store, "parent"):
                pass
        self.assertEqual("waiting", result["state"])
        self.assertEqual(record, self.store.get_flow("retry-flow"))
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM attempts").fetchone()[0])

    def test_two_consumers_cannot_consume_same_retry_twice(self):
        record = self.store.get_flow("retry-flow")
        work = _retry_work_id(record, self.proof, 1)
        record = self.store.update_flow("retry-flow", record["revision"],
            {**record["state"], "current_work_id": work, "current_node_id": work}, {"kind": "neutral_prepared"})
        runtime = self.root / "race-runtime"
        runtime.mkdir()
        write_json(runtime / "manifest.json", {"neutral": True})
        write_json(runtime / "collection.json", {"neutral": True})
        events = [{"type": "thread.started", "thread_id": "fresh"}, {"type": "error", "message": CAPACITY},
                  {"type": "turn.failed", "error": {"message": CAPACITY}}]
        (runtime / "trace.jsonl").write_text("\n".join(json.dumps(row) for row in events) + "\n")
        attempt = {"attempt_id": "attempt", "work_id": work, "intent_id": work, "state": "failed",
                   "cli_sid": "fresh", "process_exit": 1}
        result = {"nature": "real_cli_attempt", "process_ended": True, "model_completed": False,
                  "delivery_complete": False, "technical_accepted": False,
                  "termination": {"spawn_state": "started", "timed_out": False, "returncode": 1}}
        def invoke():
            try:
                return advance_failed_capacity_retry(self.store, record, attempt)
            except (TaskflowError, RetryCapacityBusy):
                return None
        with patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch"), \
             patch("taskflow.execution_repair_capacity._actual", return_value=(runtime, {}, result)), \
             patch("taskflow.execution_repair_capacity._validate_retry_manifest"), \
             patch("taskflow.execution_batch_binding.validate_batch_source", return_value=True), \
             patch.object(self.store, "list_attempts", return_value=[attempt]), \
             patch("taskflow.execution_repair_capacity.archive_full_attempt", return_value={"path": str(runtime / "archive")}), \
             ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: invoke(), range(2)))
        self.assertEqual(1, sum(result is not None for result in results))
        state = self.store.get_flow("retry-flow")["state"]
        self.assertEqual(2, state["repair_capacity_attempt_ordinal"])
        self.assertEqual(1, len(state["history"]))


class MixedQueueFairnessTests(unittest.TestCase):
    def test_paused_or_dependency_blocked_first_work_does_not_hide_later_ready(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        for first_block in ("paused", "dependency"):
            with self.subTest(first_block=first_block), tempfile.TemporaryDirectory(dir="/private/tmp") as name:
                root = Path(name).resolve()
                store = ExecutionStore(root / "state.sqlite3")
                store.register_policy("fixture", fixture_policy())
                task = make_task(root, "neutral")
                execution.capture_candidate(store, task, "candidate")
                def enqueue(identity, dependencies=None):
                    return store.enqueue(identity, "neutral", "candidate", "quality", "fixture", intent_id=identity,
                        purpose="neutral queue fixture", request_digest="a" * 64, dependencies=dependencies)
                if first_block == "dependency":
                    enqueue("dependency")
                    store.set_control("work:dependency", "pause", "neutral fixture")
                    enqueue("first", ["dependency"])
                else:
                    enqueue("first")
                    store.set_control("work:first", "pause", "neutral fixture")
                enqueue("later")
                with retry_claim_guard(store, "neutral-worker", None) as (attempt, binding):
                    self.assertIsNone(binding)
                    self.assertEqual("later", attempt["work_id"])

    def test_blocked_protected_retry_does_not_hide_later_ready_work(self):
        from taskflow import execution
        from tests.helpers import make_task
        from tools.execution_simulation import fixture_policy

        with tempfile.TemporaryDirectory(dir="/private/tmp") as name:
            root = Path(name).resolve()
            store = ExecutionStore(root / "state.sqlite3")
            store.register_policy("fixture", fixture_policy())
            task = make_task(root, "neutral")
            execution.capture_candidate(store, task, "candidate")
            evidence = root / "evidence.json"
            write_json(evidence, {"fixture": "neutral"})
            ref = evidence_ref(evidence, "neutral")
            parent = store.register_flow("parent", {}, [ref], {"status": "exception"})
            proof = {"parent_flow_id": "parent", "dependent_revisions": {}, "initial_step": 0,
                     "maximum_additional_attempts": 10, "candidate_binding": {"task_id": "neutral", "candidate_id": "candidate"}}
            work = _retry_work_id({"flow_id": "retry-flow"}, proof, 1)
            proof["new_work_id"] = work
            flow = store.register_flow("retry-flow", {"task_id": "neutral"}, [ref], {"status": "active", "stage": "repair",
                "step": 1, "current_work_id": work, "repair_capacity_attempt_ordinal": 1, "repair_capacity_continuation": proof})
            flow = store.update_flow("retry-flow", flow["revision"], flow["state"],
                                     {"kind": "workflow_repair_capacity_continuation_prepared", "issued_work_id": work})
            binding = {"flow_id": "retry-flow", "proof_digest": sha256(canonical(proof)), "ordinal": 1,
                       "issued_event_seq": flow["event_seq"], "task_id": "neutral", "candidate_id": "candidate", "role": "repair"}
            with store._db(True) as db:
                db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (work, canonical(binding).decode()))
            store.enqueue("dependency", "neutral", "candidate", "quality", "fixture", intent_id="dependency",
                          purpose="neutral", request_digest="a" * 64)
            store.set_control("work:dependency", "pause", "neutral")
            runtime = root / "runtime"
            runtime.mkdir()
            store.enqueue(work, "neutral", "candidate", "repair", "fixture", intent_id=work,
                          purpose="neutral", request_digest="a" * 64, dependencies=["dependency"],
                          runtime_path=str(runtime), repair_capacity_binding=binding)
            store.enqueue("later", "neutral", "candidate", "quality", "fixture", intent_id="later",
                          purpose="neutral", request_digest="a" * 64)
            with patch("taskflow.execution_runner._validate", return_value={}), \
                 patch("taskflow.execution_repair_capacity.validate_repair_capacity_before_dispatch"):
                with retry_claim_guard(store, "neutral-worker", None) as (attempt, protected):
                    self.assertIsNone(protected)
                    self.assertEqual("later", attempt["work_id"])
            self.assertEqual([], store.list_attempts(work))
            self.assertEqual("exception", store.get_flow("parent")["state"]["status"])


class RetryRequestBindingTests(unittest.TestCase):
    def test_only_path_and_new_identity_changes_are_permitted(self):
        from taskflow.execution_flow import _identity

        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            base = Path(directory).resolve()
            old = base / "old" / "runtime"
            root = base / "flow/nodes/1-repair/runtime"
            for path in (old, root, root / "inputs/execution"):
                path.mkdir(parents=True, exist_ok=True)
            cli = base / "neutral-cli"
            cli.write_text("neutral executable bytes")
            record = {"flow_id": "retry-flow", "definition": {"directory": str(base / "flow")}}
            proof = {"failed_attempt_id": "failed", "initial_step": 0, "maximum_additional_attempts": 10,
                     "candidate_binding": {"candidate_id": "c"}}
            new_id = _retry_work_id(record, proof, 1)
            old_context = {"node_id": "old-node", "intent_id": "old-node", "runtime": str(old),
                "work": str(old.parent / "work"), "scratch": str(old.parent / "scratch"),
                "inputs_root": str(old / "inputs"), "revised_candidate_id": "old-candidate", "defects": ["D1"]}
            context = {**old_context, "node_id": new_id, "intent_id": new_id, "runtime": str(root),
                "work": str(root.parent / "work"), "scratch": str(root.parent / "scratch"),
                "inputs_root": str(root / "inputs"), "revised_candidate_id": _identity("retry-flow", new_id + "-candidate")}
            write_json(old.parent / "current.json", old_context)
            write_json(root.parent / "current.json", context)
            write_json(root / "inputs/execution/current.json", context)
            (old / "request.md").write_text("Repair D1 from " + str(old / "inputs") + " at old-node")
            (root / "request.md").write_text("Repair D1 from " + str(root / "inputs") + " at " + new_id)
            shared = {"kind": "teacher", "target": "answer.md", "source": "/captured/answer", "sha256": "sha", "size": 7}
            base_manifest = {"mode": "native_role", "argv": [str(cli), "-C", str(old.parent / "work")],
                "native": {"scratch": str(old.parent / "scratch")}, "model": {"requested": "neutral", "reasoning_effort": "high"},
                "model_slot": "repair", "reasoning_selection": "explicit", "reasoning_context": None,
                "timeout_seconds": 30, "batch_binding": {"setting": {"model": "neutral"}},
                "inputs": [{"target": "execution/current.json"}, shared], "outputs": ["repair.json"], "output_trees": ["task"],
                "output_tree_contract_version": 2}
            manifest = {**base_manifest, "argv": [str(cli), "-C", str(root.parent / "work")],
                "native": {"scratch": str(root.parent / "scratch")}, "intent_id": new_id, "role": "repair",
                "resume_sid": None, "candidate_binding": proof["candidate_binding"]}
            store = Mock()
            store.get_attempt.return_value = {"policy_id": "policy"}
            runner_binding = {"model_slot": "repair", "policy_id": "policy"}
            with patch("taskflow.execution_repair_capacity._actual", return_value=(old, base_manifest, {})), \
                 patch("taskflow.execution_runner._validate", return_value=manifest), \
                 patch("taskflow.execution_batch_binding.validate_batch_runner", return_value=runner_binding):
                _validate_retry_manifest(store, record, proof, manifest, 1)
                (root / "request.md").write_text("Altered responsibility")
                with self.assertRaisesRegex(TaskflowError, "request"):
                    _validate_retry_manifest(store, record, proof, manifest, 1)


class OriginalArchiveWitnessTests(unittest.TestCase):
    def test_full_original_archive_and_preparation_event_are_revalidated(self):
        from taskflow.execution_runner import archive_full_attempt, run_attempt
        from tests.test_execution_runner import ExecutionRunnerTests

        fixture = ExecutionRunnerTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.prepare()
        self.assertTrue(run_attempt(fixture.root)["technical_accepted"])
        archive = fixture.base / "archive"
        archive_full_attempt(fixture.root, archive)
        store = ExecutionStore(fixture.base / "state.sqlite3")
        proof = {"child_revision": 0, "child_event_seq": 0, "failed_work_id": "intent-1",
                 "failed_attempt_id": "attempt-1", "failed_archive": str(archive)}
        with store._db(True) as db:
            store._event(db, "flow_updated", {"flow_id": "child", "previous_revision": 0, "revision": 1,
                         "transition": {"kind": "workflow_repair_capacity_continuation_prepared", "binding": proof},
                         "state": {"repair_capacity_continuation": proof}})
        with patch.object(store, "get_attempt", return_value={"runtime_path": str(fixture.root)}):
            _validate_prepared_witness(store, "child", proof)
            from taskflow.execution_runner import _info

            binding_path = archive / "archive-binding.json"
            original_binding = binding_path.read_bytes()
            extra = archive / "injected.txt"
            extra.write_text("coordinated fake archive member")
            binding = json.loads(original_binding)
            binding["files"]["injected.txt"] = _info(extra)
            write_json(binding_path, binding)
            with self.assertRaises((TaskflowError, OSError, ValueError)):
                _validate_prepared_witness(store, "child", proof)
            extra.unlink()
            binding_path.write_bytes(original_binding)
            target = archive / "trace.jsonl"
            target.write_text("changed", encoding="utf-8")
            with self.assertRaises((TaskflowError, OSError, ValueError)):
                _validate_prepared_witness(store, "child", proof)


class SimulatedRetryIntegrationTests(unittest.TestCase):
    def test_actual_fixture_subprocess_failures_then_success_without_extra_calls(self):
        """Neutral subprocesses and real SQLite; source/profile checks are stubs."""
        from taskflow import execution
        from taskflow.execution_flow import advance_workflow
        from taskflow.execution_public_continuation import _actual as actual
        from tests.test_execution_flow import ContinuationTests, ROLE_FIXTURE

        case = ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        marker = case.root / "neutral-failure-count"
        injection = """
if a.action=='repair':
 marker=pathlib.Path(%r)
 number=int(marker.read_text())+1 if marker.exists() else 1
 marker.write_text(str(number))
 if number <= 3:
  print(json.dumps({'type':'error','message':%r}),flush=True)
  print(json.dumps({'type':'turn.failed','error':{'message':%r}}),flush=True)
  raise SystemExit(1)
""" % (str(marker), CAPACITY, CAPACITY)
        case.script.write_text(ROLE_FIXTURE.replace("time.sleep(a.delay)", "time.sleep(a.delay)\n" + injection))
        definition = case.definition()
        child = case.register(definition)
        original = case.run_flow([child["flow_id"]])["flows"][0]
        self.assertEqual("exception", original["state"]["status"])
        failed = case.store.list_attempts(original["state"]["current_work_id"])[0]
        self.assertEqual("failed", failed["state"])
        authority = case.root / "retry-authority.json"
        write_json(authority, {"fixture": "simulation"})
        ref = evidence_ref(authority, "neutral fixture")
        parent = case.store.register_flow("parent", {}, case.refs, {"status": "exception"})
        parent = case.store.update_flow("parent", parent["revision"], {"status": "exception", "stage": "child_quality",
            "receipt": str(authority), "child_flow_id": original["flow_id"], "pending_business_scope": None},
            {"kind": "lifecycle_result_received"})
        candidate = case.store.get_candidate(original["state"]["candidate_id"])
        candidate_binding = {key: candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        proof = {"kind": "bounded_repair_capacity_v1", "failed_attempt_id": failed["attempt_id"],
            "failed_work_id": failed["work_id"], "failed_sid": failed["cli_sid"], "author_sid": "fixture-author",
            "parent_flow_id": "parent", "parent_revision": parent["revision"],
            "parent_received_event": {"event_seq": parent["event_seq"]}, "dependent_revisions": {},
            "child_revision": original["revision"], "child_event_seq": original["event_seq"],
            "initial_step": original["state"]["step"], "maximum_additional_attempts": 10,
            "candidate_binding": candidate_binding,
            "previous_child_receipt": ref, "previous_parent_receipt": ref, "direct_user_evidence": ref,
            "failed_manifest": ref, "failed_collection": ref, "failed_trace": ref}
        proof["new_work_id"] = _retry_work_id(original, proof, 1)
        resolution = {"reason": "neutral simulation", "authority": ref, "failed_attempt_id": failed["attempt_id"]}
        with patch("taskflow.execution_repair_capacity._proof", return_value=proof), \
             patch("taskflow.execution_public_continuation._no_formal"):
            child = prepare_repair_capacity_budget(case.store, "parent", original["flow_id"], resolution)
        case.store.set_control("work:" + proof["new_work_id"], "resume", "neutral authorized fixture")
        def fixture_actual(store, attempt):
            root, manifest, collected = actual(store, attempt)
            return root, manifest, {**collected, "nature": "real_cli_attempt"}
        with patch("taskflow.execution_repair_capacity._proof", return_value=proof), \
             patch("taskflow.execution_repair_capacity._actual", side_effect=fixture_actual), \
             patch("taskflow.execution_repair_capacity._validate_retry_manifest"), \
             patch("taskflow.execution_repair_capacity.validate_repair_capacity_result"), \
             patch("taskflow.execution_batch_binding.validate_batch_source", return_value=True):
            child = advance_workflow(case.store, child["flow_id"])
            first_retry = execution.run_worker(case.store, "neutral", True, [child["state"]["current_work_id"]])
            self.assertEqual("failed", first_retry["state"])
            retained = case.store.get_flow(child["flow_id"])
            event = case.store._event
            def interrupt_after_archive(db, kind, data):
                if kind == "flow_updated" and data.get("transition", {}).get("kind") == "workflow_repair_capacity_failure_retained":
                    raise KeyboardInterrupt("neutral crash before durable transition")
                return event(db, kind, data)
            with patch.object(case.store, "_event", side_effect=interrupt_after_archive), self.assertRaises(KeyboardInterrupt):
                advance_workflow(case.store, child["flow_id"])
            self.assertEqual(retained, case.store.get_flow(child["flow_id"]))
            self.assertTrue((Path(child["definition"]["directory"]) / "archives" /
                             first_retry["attempt_id"] / "supplemental/receipt.json").is_file())
            child = case.run_flow([child["flow_id"]])["flows"][0]
        self.assertEqual("accepted", child["state"]["status"], child["state"])
        self.assertEqual(2, sum(row.get("kind") == "repair_capacity_failed_retry" for row in child["state"]["history"]))
        self.assertEqual("4", marker.read_text())  # Original failure, two retries, then success.
        self.assertEqual(3, child["state"]["repair_capacity_attempt_ordinal"])
        self.assertEqual("exception", case.store.get_flow("parent")["state"]["status"])
