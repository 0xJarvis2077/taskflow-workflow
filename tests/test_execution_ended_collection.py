"""Real neutral CLI termination survives an interrupted original consumer.

Only sandbox-unavailable startup identity metadata is adapted. No external model
or native sandbox proof is claimed; collection, SQLite and workflow IO are real.
"""

import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import _public_root, advance_author
from taskflow.execution_completion import completion_owner, reconcile_ended_attempt
from taskflow.execution_experiment import _completed
from taskflow.execution_flow import _identity, advance_workflow, register_workflow
from taskflow.execution_lifecycle import run_lifecycle
from taskflow.execution_runner import reconcile_attempt, run_attempt
from tests import test_execution_author as author_fixtures
from tests import test_execution_flow as flow_fixtures
from tools.execution_simulation import perform


class EndedCollectionTests(unittest.TestCase):
    def source(self):
        case = author_fixtures.AuthorSuccessorTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def interrupt_worker(self, store, work_id):
        def interrupted(root, on_process=None, **kwargs):
            def identity(value):
                # The CLI and supervisor are actual subprocesses. This string
                # only substitutes ps metadata denied in the test sandbox.
                on_process({**value, "start_identity": value.get("start_identity")
                            or "neutral actual subprocess pid=" + str(value["pid"])})

            run_attempt(root, fault="after_start")
            deadline = time.monotonic() + 8
            while not (root / "process.json").exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue((root / "process.json").is_file())
            identity(read_json(root / "process.json"))
            raise KeyboardInterrupt("neutral consumer interrupted before collection")

        with patch("taskflow.execution_runner.run_attempt", side_effect=interrupted), \
             self.assertRaises(KeyboardInterrupt):
            execution.run_worker(store, "interrupted-neutral-consumer", True, [work_id])
        with store._db() as db:
            import json
            rows = [json.loads(row["data"]) for row in db.execute(
                "SELECT data FROM attempts WHERE work_id=?", (work_id,))]
        self.assertEqual(1, len(rows))
        self.assertEqual("running", rows[0]["state"])
        return rows[0]

    def ended(self, attempt):
        root = Path(attempt["runtime_path"])
        deadline = time.monotonic() + 8
        while not (root / "termination.json").exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue((root / "termination.json").is_file())
        self.assertEqual(0, read_json(root / "termination.json")["returncode"])
        self.assertFalse((root / "collection.json").exists())

    def test_source_author_restart_collects_the_same_original_once(self):
        case = self.source()
        case.prepare_source("research_ended")
        attempt = self.interrupt_worker(case.store, "author-source")
        case.attempt = attempt
        record = case.register()
        self.ended(attempt)
        record = advance_author(case.store, record)
        self.assertEqual("completed", case.store.get_attempt(attempt["attempt_id"])["state"])
        received = run_lifecycle(case.store, record["flow_id"], allow_model=False)
        self.assertEqual("accepted", received["state"]["status"])
        self.assertEqual("research_ended", received["state"]["result"]["outcome"])
        actual = case.store.get_attempt(attempt["attempt_id"])
        self.assertEqual("completed", actual["state"])
        before = case.store.events()
        self.assertEqual(received, run_lifecycle(case.store, record["flow_id"], allow_model=False))
        self.assertEqual(before, case.store.events())
        self.assertEqual([actual], case.store.dashboard()["attempts"])

    def public_ready(self, case):
        case.run_source("candidate_complete")
        record = advance_author(case.store, case.register())
        self.addCleanup(shutil.rmtree, _public_root(record), True)
        record = advance_author(case.store, record)
        inner = case.root / "public-fixture"
        (inner / "work").mkdir(parents=True)

        def enqueue(store, spec, runtime, **kwargs):
            converted = copy.deepcopy(spec)
            converted["runner"].update(mode="test_fixture", cwd=str(inner / "work"),
                argv=[str(Path(sys.executable).resolve()), str(case.public_script)],
                dependencies=[str(case.public_script)])
            converted["runner"].pop("isolated", None)
            return execution.enqueue_spec(store, converted, runtime, **kwargs)

        with patch("taskflow.execution_author._public_prepare", return_value={
                "runtime": str(inner), "flavor": "quality"}), \
             patch("taskflow.execution_author.enqueue_spec", side_effect=enqueue):
            record = advance_author(case.store, record)
        self.assertEqual("public_result", record["state"]["stage"])
        return record

    def test_public_restart_collects_same_attempt_without_reusing_author_sid(self):
        case = self.source()
        record = self.public_ready(case)
        attempt = self.interrupt_worker(case.store, record["state"]["public_work_id"])
        self.ended(attempt)
        record = advance_author(case.store, record)
        self.assertEqual("child_quality", record["state"]["stage"])
        actual = case.store.get_attempt(attempt["attempt_id"])
        self.assertEqual("completed", actual["state"])
        self.assertNotEqual(case.attempt["cli_sid"], actual["cli_sid"])
        self.assertEqual(attempt["attempt_id"], record["state"]["public_attempt_id"])
        self.assertEqual(2, len(case.store.dashboard()["attempts"]))

    def child_repair_ready(self, case):
        record = self.public_ready(case)
        public = execution.run_worker(case.store, "neutral-public", True,
                                      [record["state"]["public_work_id"]])
        record = advance_author(case.store, record)
        chain = case.root / "child-fixture"
        chain.mkdir()
        script = chain / "roles.py"
        script.write_text(flow_fixtures.ROLE_FIXTURE, encoding="utf-8")
        definition = flow_fixtures.ContinuationTests.definition(
            SimpleNamespace(root=chain, script=script), "neutral")
        definition.update(flow_id=_identity(record["flow_id"], "quality-flow"),
                          directory=str(Path(record["definition"]["directory"]) / "quality"))
        definition["initial"] = {
            "candidate_id": record["state"]["candidate_id"],
            "candidate_root": record["state"]["candidate_root"],
            "predecessor_attempt": public["attempt_id"],
            "predecessor_binding": {key: public[key] for key in
                                    ("work_id", "intent_id", "candidate_id", "task_id")}}
        register_workflow(case.store, definition, case.refs)
        ready = run_lifecycle(case.store, record["flow_id"], allow_model=False)
        quality_id = ready["state"]["current_work_id"]
        self.assertEqual("quality", case.store.get_work(quality_id)["role"])
        execution.run_worker(case.store, "neutral-original-quality", True, [quality_id])
        ready = run_lifecycle(case.store, record["flow_id"], allow_model=False)
        repair_id = ready["state"]["current_work_id"]
        self.assertEqual("repair", case.store.get_work(repair_id)["role"])
        return ready, repair_id

    def test_parent_restart_receives_ended_repair_and_prepares_independent_recheck(self):
        case = self.source()
        record, repair_id = self.child_repair_ready(case)
        attempt = self.interrupt_worker(case.store, repair_id)
        self.ended(attempt)
        before = len(case.store.dashboard()["attempts"])
        with patch.object(case.store, "dashboard", side_effect=AssertionError(
                "a same-work restart must not inspect unrelated quality histories")):
            record = advance_author(case.store, record)
            self.assertEqual("completed", case.store.get_attempt(attempt["attempt_id"])["state"])
            restarted = run_lifecycle(case.store, record["flow_id"], allow_model=False)
        actual = case.store.get_attempt(attempt["attempt_id"])
        self.assertEqual("completed", actual["state"])
        current = case.store.get_work(restarted["state"]["current_work_id"])
        self.assertEqual("quality", current["role"])
        self.assertEqual("ready", current["state"])
        child = case.store.get_flow(restarted["state"]["child_flow_id"])
        self.assertEqual("recheck", child["state"]["stage"])
        self.assertNotEqual(attempt["intent_id"], current["intent_id"])
        self.assertEqual(before, len(case.store.dashboard()["attempts"]))
        for _ in range(2):
            run_lifecycle(case.store, record["flow_id"], allow_model=False)
        self.assertEqual(before, len(case.store.dashboard()["attempts"]))
        with case.store._db() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM attempts WHERE work_id=?",
                                          (repair_id,)).fetchone()[0])

    def test_experiment_completion_collects_original_review_without_claiming(self):
        case = self.source()
        with patch("tools.execution_simulation.run_worker", side_effect=lambda *a, **k:
                   self.interrupt_worker(case.store, "neutral-facts")):
            attempt = perform(case.store, case.root / "facts", "baseline", "review-facts",
                              "neutral-facts", {"facts.json": "{}"})
        self.ended(attempt)
        record = {"state": {"current_work_id": "neutral-facts", "candidate_id": "baseline"}}
        with patch.object(case.store, "dashboard", side_effect=AssertionError(
                "completion lookup must not inspect unrelated quality histories")):
            actual, reason = _completed(case.store, record)
        self.assertEqual("completed", reason)
        self.assertEqual(attempt["attempt_id"], actual["attempt_id"])
        self.assertEqual((actual, reason), _completed(case.store, record))
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))

    def test_experiment_ready_or_claimed_without_termination_waits_without_dispatch(self):
        case = self.source()
        case.prepare_source()
        record = {"state": {"current_work_id": "author-source", "candidate_id": "baseline"}}
        self.assertEqual((None, "original_worker_not_started"), _completed(case.store, record))
        attempt = case.store.claim("neutral-never-launched", ["author-source"])
        self.assertEqual((None, "original_worker_owns_completion"), _completed(case.store, record))
        case.store.mark_uncertain(attempt["attempt_id"], "No bound termination")
        with self.assertRaisesRegex(TaskflowError, "no automatic redraw"):
            _completed(case.store, record)
        self.assertEqual(1, len(case.store.list_attempts(attempt["work_id"])))

    def test_alive_or_unproved_attempt_is_not_collected_or_redrawn(self):
        case = self.source()
        case.prepare_source("research_ended", delay=1)
        attempt = self.interrupt_worker(case.store, "author-source")
        case.attempt = attempt
        record = case.register()
        before = case.store.events()
        self.assertEqual(attempt, reconcile_ended_attempt(case.store, attempt))
        self.assertEqual(before, case.store.events())
        waited = advance_author(case.store, record)
        self.assertEqual("source_author", waited["state"]["waiting"]["stage"])
        uncertain = case.store.mark_uncertain(attempt["attempt_id"], "Original process state not proved")
        self.assertEqual(uncertain, reconcile_ended_attempt(case.store, uncertain))
        with self.assertRaisesRegex(TaskflowError, "never redraw"):
            advance_author(case.store, waited)
        self.ended(uncertain)
        self.assertEqual("completed", reconcile_ended_attempt(case.store, uncertain)["state"])
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))

    def test_missing_or_nonregular_termination_never_starts_reconciliation(self):
        case = self.source()
        case.prepare_source()
        attempt = case.store.claim("neutral-never-launched", ["author-source"])
        with patch("taskflow.execution.reconcile", side_effect=AssertionError("must not reconcile")):
            self.assertIsNone(reconcile_ended_attempt(case.store, None))
            self.assertEqual(attempt, reconcile_ended_attempt(case.store, attempt))
            for kind in ("directory", "symlink"):
                path = case.runtime / "termination.json"
                if kind == "directory":
                    path.mkdir()
                else:
                    path.symlink_to(case.runtime / "manifest.json")
                try:
                    with self.subTest(kind=kind), self.assertRaisesRegex(TaskflowError, "ordinary"):
                        reconcile_ended_attempt(case.store, attempt)
                finally:
                    path.rmdir() if kind == "directory" else path.unlink()
            terminal = {**attempt, "state": "failed"}
            self.assertEqual(terminal, reconcile_ended_attempt(case.store, terminal))

    def test_foreign_or_inconclusive_termination_does_not_prove_completion(self):
        case = self.source()
        case.prepare_source("research_ended")
        attempt = self.interrupt_worker(case.store, "author-source")
        self.ended(attempt)
        path = Path(attempt["runtime_path"]) / "termination.json"
        original = path.read_bytes()
        receipt = read_json(path)
        try:
            write_json(path, {**receipt, "intent_id": "another-intent"})
            before = case.store.events()
            with self.assertRaisesRegex(ValueError, "another dispatch intent"):
                reconcile_ended_attempt(case.store, attempt)
            self.assertEqual(before, case.store.events())
            write_json(path, {**receipt, "returncode": None})
            unresolved = reconcile_ended_attempt(case.store, attempt)
            self.assertEqual("uncertain", unresolved["state"])
            self.assertFalse((path.parent / "collection.json").exists())
            self.assertEqual(1, len(case.store.list_attempts(attempt["work_id"])))
        finally:
            path.write_bytes(original)
        self.assertEqual("completed", reconcile_ended_attempt(case.store, unresolved)["state"])

    def test_live_worker_owns_termination_to_collection_and_finish_window(self):
        case = self.source()
        case.prepare_source("research_ended")
        at_collection, release = threading.Event(), threading.Event()

        def paused(root, fault=None):
            self.assertTrue((root / "termination.json").exists())
            at_collection.set()
            self.assertTrue(release.wait(8))
            return reconcile_attempt(root, fault=fault)

        with patch("taskflow.execution_runner.reconcile_attempt", side_effect=paused) as collector, \
             patch.object(case.store, "finish_attempt", wraps=case.store.finish_attempt) as finish, \
             ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(execution.run_worker, case.store, "original-live-consumer", True,
                                 ["author-source"])
            try:
                self.assertTrue(at_collection.wait(8))
                attempt = case.store.list_attempts("author-source")[0]
                before = case.store.events()
                self.assertEqual(attempt, reconcile_ended_attempt(case.store, attempt))
                self.assertEqual(before, case.store.events())
                self.assertEqual(1, collector.call_count)
                self.assertEqual(0, finish.call_count)
                self.assertFalse((case.runtime / "collection.json").exists())
            finally:
                release.set()
            actual = future.result(timeout=8)
            self.assertEqual("completed", actual["state"])
            self.assertEqual(1, finish.call_count)
            self.assertEqual(actual, reconcile_ended_attempt(case.store, actual))
            self.assertEqual(1, finish.call_count)

    def test_dead_consumer_process_releases_completion_owner_for_one_original_collection(self):
        case = self.source()
        case.prepare_source("research_ended")
        marker = case.runtime.with_name(case.runtime.name + ".collection-barrier")
        repository = Path(__file__).resolve().parents[1]
        code = r'''
import os,pathlib,sys
from unittest.mock import patch
from taskflow import execution
from taskflow.execution_store import ExecutionStore
store=ExecutionStore(sys.argv[1]); marker=pathlib.Path(sys.argv[2])
def stop_before_collection(root, fault=None):
    assert (root/'termination.json').is_file()
    marker.write_text('Original CLI ended; original consumer still owns completion.')
    sys.stdin.buffer.read(1)
    os._exit(73)
with patch('taskflow.execution_runner.reconcile_attempt',side_effect=stop_before_collection):
    execution.run_worker(store,'actual-neutral-consumer-process',True,['author-source'])
'''
        process = subprocess.Popen([sys.executable, "-c", code, str(case.store.path), str(marker)],
            cwd=repository, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 8
            while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(marker.is_file())
            attempt = case.store.list_attempts("author-source")[0]
            self.assertEqual(0, read_json(case.runtime / "termination.json")["returncode"])
            before = case.store.events()
            self.assertEqual(attempt, reconcile_ended_attempt(case.store, attempt))
            self.assertEqual(before, case.store.events())
            self.assertFalse((case.runtime / "collection.json").exists())
            out, error = process.communicate(input=b"release", timeout=8)
            self.assertEqual(73, process.returncode, error.decode())
            with patch.object(case.store, "finish_attempt", wraps=case.store.finish_attempt) as finish:
                actual = reconcile_ended_attempt(case.store, attempt)
                self.assertEqual("completed", actual["state"])
                self.assertEqual(attempt["attempt_id"], actual["attempt_id"])
                self.assertEqual(actual, reconcile_ended_attempt(case.store, actual))
                self.assertEqual(1, finish.call_count)
            self.assertEqual(1, len(case.store.list_attempts("author-source")))
        finally:
            if process.poll() is None:
                process.terminate()
            process.communicate(timeout=8)

    def test_completion_owner_held_before_worker_launch_does_not_start_or_redraw(self):
        case = self.source()
        case.prepare_source()
        with completion_owner(case.runtime) as owned:
            self.assertTrue(owned)
            attempt = execution.run_worker(case.store, "neutral-held-owner", True, ["author-source"])
            self.assertEqual("claimed", attempt["state"])
            self.assertEqual(attempt, execution.reconcile(case.store, attempt["attempt_id"]))
            self.assertFalse((case.runtime / "dispatch.json").exists())
        self.assertEqual("waiting", execution.run_worker(case.store, "no-replacement", True,
                                                         ["author-source"])["state"])
        self.assertEqual(1, len(case.store.list_attempts("author-source")))

    def test_unprepared_original_claim_is_retained_uncertain_without_launch(self):
        case = self.source()
        case.store.enqueue("unprepared", "neutral", "baseline", "author", "fixture-v1",
            intent_id="unprepared-intent", purpose="Neutral missing prepared runtime",
            request_digest="0" * 64, modifies=True, evidence=case.refs)
        with self.assertRaisesRegex(TaskflowError, "no prepared runtime"):
            execution.run_worker(case.store, "neutral-unprepared", True, ["unprepared"])
        attempts = case.store.list_attempts("unprepared")
        self.assertEqual(1, len(attempts))
        self.assertEqual("uncertain", attempts[0]["state"])

    def test_two_observers_recheck_terminal_state_inside_completion_mutex(self):
        case = self.source()
        case.prepare_source()
        attempt = self.interrupt_worker(case.store, "author-source")
        self.ended(attempt)
        initial_read, continue_owner = threading.Event(), threading.Event()
        delayed_thread = []

        @contextmanager
        def delayed(root):
            if threading.get_ident() in delayed_thread:
                initial_read.set()
                self.assertTrue(continue_owner.wait(8))
            with completion_owner(root) as owned:
                yield owned

        def observe():
            delayed_thread.append(threading.get_ident())
            return execution.reconcile(case.store, attempt["attempt_id"])

        with patch("taskflow.execution_completion.completion_owner", side_effect=delayed), \
             patch.object(case.store, "finish_attempt", wraps=case.store.finish_attempt) as finish, \
             ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(observe)
            try:
                self.assertTrue(initial_read.wait(8))
                actual = execution.reconcile(case.store, attempt["attempt_id"])
            finally:
                continue_owner.set()
            self.assertEqual(actual, future.result(timeout=8))
            self.assertEqual("completed", actual["state"])
            self.assertEqual(1, finish.call_count)

    def test_nonregular_completion_mutex_does_not_consume_the_attempt(self):
        case = self.source()
        case.prepare_source()
        lock = case.runtime.with_name(case.runtime.name + ".completion-owner.lock")
        os.mkfifo(lock)
        try:
            with self.assertRaisesRegex(TaskflowError, "ordinary"):
                with completion_owner(case.runtime):
                    self.fail("FIFO cannot be a completion owner")
        finally:
            lock.unlink()
        self.assertEqual([], case.store.list_attempts("author-source"))

    def test_uncertain_flow_receipt_arrival_cannot_collect_outside_completion_owner(self):
        case = self.source()
        parent, repair_id = self.child_repair_ready(case)
        attempt = self.interrupt_worker(case.store, repair_id)
        case.store.mark_uncertain(attempt["attempt_id"], "Original consumer lost its process observation")
        self.ended(attempt)
        child_id = parent["state"]["child_flow_id"]
        before = case.store.get_flow(child_id)
        termination = Path(attempt["runtime_path"]) / "termination.json"
        exists = Path.exists
        checks = []

        def arrives(path):
            if path == termination:
                checks.append(path)
                if len(checks) == 1:
                    return False
            return exists(path)

        with completion_owner(Path(attempt["runtime_path"])) as owned:
            self.assertTrue(owned)
            with patch.object(Path, "exists", new=arrives):
                waiting = advance_workflow(case.store, child_id)
            self.assertEqual(before, waiting)
            self.assertEqual("active", waiting["state"]["status"])
            self.assertFalse((Path(attempt["runtime_path"]) / "collection.json").exists())
        self.assertEqual(1, len(checks))
        advance_workflow(case.store, child_id)
        self.assertEqual("completed", case.store.get_attempt(attempt["attempt_id"])["state"])
        resumed = run_lifecycle(case.store, parent["flow_id"], allow_model=False)
        self.assertEqual("quality", case.store.get_work(resumed["state"]["current_work_id"])["role"])
        self.assertEqual(1, len(case.store.list_attempts(repair_id)))

    def test_uncertain_flow_late_terminal_receipt_is_received_as_completion(self):
        case = self.source()
        parent, repair_id = self.child_repair_ready(case)
        attempt = self.interrupt_worker(case.store, repair_id)
        case.store.mark_uncertain(attempt["attempt_id"], "Original process observation lost")
        self.ended(attempt)
        termination = Path(attempt["runtime_path"]) / "termination.json"
        exists = Path.exists
        checks = []

        def arrives(path):
            if path == termination:
                checks.append(path)
                if len(checks) == 1:
                    return False
            return exists(path)

        with patch.object(Path, "exists", new=arrives), \
             patch.object(case.store, "finish_attempt", wraps=case.store.finish_attempt) as finish:
            child = advance_workflow(case.store, parent["state"]["child_flow_id"])
        self.assertEqual("active", child["state"]["status"])
        self.assertEqual("completed", case.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual(1, finish.call_count)
        self.assertGreaterEqual(len(checks), 2)
        self.assertEqual(1, len(case.store.list_attempts(repair_id)))


if __name__ == "__main__":
    unittest.main()
