"""Real local workers retain completion ownership while successors wait."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Event
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow import execution_flow
from taskflow.execution_flow import advance_workflow
from taskflow.execution_runner import archive_full_attempt
from tests import test_execution_flow as flow_tests


class PredecessorConcurrencyTests(unittest.TestCase):
    # Borrow fixture construction without inheriting the other module's tests.
    setUp = flow_tests.ContinuationTests.setUp
    definition = flow_tests.ContinuationTests.definition
    register = flow_tests.ContinuationTests.register
    run_flow = flow_tests.ContinuationTests.run_flow
    predecessor = flow_tests.ContinuationTests.predecessor

    def paused_original_worker(self, attempt, reached, release):
        """Continue the already claimed fixture using the actual worker/runner."""
        from taskflow.execution_repair_capacity import retry_claim_guard

        original_record = execution._record_result

        @contextmanager
        def claim_guard(store, worker_id, work_ids=None):
            if worker_id == "external-fixture":
                self.assertEqual([attempt["work_id"]], work_ids)
                yield attempt, None
            else:
                with retry_claim_guard(store, worker_id, work_ids) as selection:
                    yield selection

        def record(store, current, result, **kwargs):
            if current["attempt_id"] == attempt["attempt_id"]:
                runtime = Path(current["runtime_path"])
                self.assertTrue((runtime / "termination.json").is_file())
                self.assertTrue((runtime / "collection-lock.json").is_file())
                self.assertEqual("collecting", store.get_attempt(current["attempt_id"])["state"])
                reached.set()
                if not release.wait(10):
                    raise AssertionError("test did not release original worker")
            return original_record(store, current, result, **kwargs)

        # The fixture helper already performed the durable claim. Only hand that
        # existing claim back to its owner; process launch and collection are real.
        return patch("taskflow.execution_repair_capacity.retry_claim_guard", side_effect=claim_guard), patch(
            "taskflow.execution._record_result", side_effect=record
        )

    def test_sealed_collecting_predecessor_remains_with_original_worker(self):
        definition = self.definition(outcome="accepted")
        attempt = self.predecessor(definition)
        self.register(definition)
        reached, release = Event(), Event()
        claim_patch, record_patch = self.paused_original_worker(attempt, reached, release)
        with claim_patch, record_patch, ThreadPoolExecutor(max_workers=1) as pool:
            worker = pool.submit(execution.run_worker, self.store, "external-fixture", True, [attempt["work_id"]])
            try:
                self.assertTrue(reached.wait(10), "actual worker did not reach sealed collecting window")
                current = self.store.get_attempt(attempt["attempt_id"])
                with patch("taskflow.execution.reconcile", side_effect=AssertionError("original worker owns completion")), \
                     patch("taskflow.execution_runner.archive_full_attempt", side_effect=AssertionError("original is still collecting")):
                    first = advance_workflow(self.store, definition["flow_id"])
                    again = advance_workflow(self.store, definition["flow_id"])
                self.assertEqual("collecting", first["state"]["waiting_predecessor"]["state"])
                self.assertEqual("original_worker_owns_completion", first["state"]["waiting_predecessor"]["reason"])
                self.assertFalse(first["state"]["initialized"])
                self.assertEqual(first["revision"], again["revision"])
                self.assertEqual(current, self.store.get_attempt(attempt["attempt_id"]))
                self.assertEqual(1, len(self.store.dashboard()["attempts"]))
            finally:
                release.set()
            completed = worker.result(timeout=10)
        self.assertEqual("completed", completed["state"])
        result = self.run_flow([definition["flow_id"]])
        state = result["flows"][0]["state"]
        self.assertEqual("accepted", state["status"], state)
        self.assertEqual(completed["cli_sid"], state["history"][0]["cli_sid"])
        self.assertEqual("adopted_predecessor", state["history"][0]["kind"])
        self.run_flow([definition["flow_id"]])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_waiting_flow_does_not_block_ready_flow_and_wakes_once(self):
        waiting = self.definition("waiting", outcome="accepted")
        attempt = self.predecessor(waiting)
        ready = self.definition("ready", outcome="accepted")
        self.register(waiting)
        self.register(ready)
        reached, release, ready_received = Event(), Event(), Event()
        claim_patch, record_patch = self.paused_original_worker(attempt, reached, release)
        original_receipt = execution_flow._receipt

        def receipt(store, record):
            result = original_receipt(store, record)
            if record["flow_id"] == ready["flow_id"] and result["state"].get("receipt"):
                ready_received.set()
            return result

        def archive(runtime, destination):
            if runtime == Path(attempt["runtime_path"]):
                self.assertEqual("completed", self.store.get_attempt(attempt["attempt_id"])["state"])
            return archive_full_attempt(runtime, destination)

        with claim_patch, record_patch, \
             patch("taskflow.execution_flow._receipt", side_effect=receipt), \
             patch("taskflow.execution.reconcile", side_effect=AssertionError("never recover the live original worker")), \
             patch("taskflow.execution_runner.archive_full_attempt", side_effect=archive), \
             ThreadPoolExecutor(max_workers=2) as pool:
            worker = pool.submit(execution.run_worker, self.store, "external-fixture", True, [attempt["work_id"]])
            consumer = None
            try:
                self.assertTrue(reached.wait(10), "original worker did not reach collecting")
                consumer = pool.submit(self.run_flow, [waiting["flow_id"], ready["flow_id"]])
                self.assertTrue(ready_received.wait(10), "waiting predecessor blocked an unrelated ready flow")
                self.assertFalse(consumer.done())
                blocked = self.store.get_flow(waiting["flow_id"])
                self.assertEqual("active", blocked["state"]["status"])
                self.assertEqual("collecting", blocked["state"]["waiting_predecessor"]["state"])
                self.assertEqual("accepted", self.store.get_flow(ready["flow_id"])["state"]["status"])
                self.assertEqual("collecting", self.store.get_attempt(attempt["attempt_id"])["state"])
                self.assertEqual(2, len(self.store.dashboard()["attempts"]))
            finally:
                release.set()
            self.assertEqual("completed", worker.result(timeout=10)["state"])
            self.assertIsNotNone(consumer)
            result = consumer.result(timeout=10)
        self.assertTrue(all(item["state"]["status"] == "accepted" for item in result["flows"]), result)
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(3, len(attempts))
        self.assertEqual(1, sum(item["work_id"] == attempt["work_id"] for item in attempts))
        self.assertEqual(2, sum(item["task_id"] == "waiting" for item in attempts))
        self.run_flow([waiting["flow_id"], ready["flow_id"]])
        self.assertEqual(attempts, self.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
