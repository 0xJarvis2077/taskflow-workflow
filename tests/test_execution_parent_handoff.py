"""Zero-model evidence for an explicit parent-consumer handoff, not a new API."""

import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import read_json
from taskflow.execution_author import _author
from taskflow.execution_lifecycle import _lifecycle_lock, advance_lifecycle, run_lifecycle
from taskflow.execution_runner import run_attempt
from tests import test_execution_author as author_fixtures


UPGRADE_REASON = "Explicit technical producer upgrade; original author continues unchanged."


class ParentConsumerHandoffTests(unittest.TestCase):
    def fixture(self):
        case = author_fixtures.AuthorSuccessorTests("test_research_ended_receives_full_archive_without_new_work")
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def wait(self, predicate):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.01)
        self.fail("Neutral handoff did not reach its expected bounded state")

    @staticmethod
    def actual_process(root, on_process=None, **kwargs):
        # The subprocess and collection are real. Only startup identity is
        # adapted because this sandbox cannot use macOS ps.
        def observed(identity):
            on_process({**identity, "start_identity": "neutral actual subprocess fixture"})

        return run_attempt(root, on_process=observed, **kwargs)

    @staticmethod
    def stop_parent(case):
        current = case.store.get_flow("neutral-author-tail")
        state = copy.deepcopy(current["state"])
        state.update(status="exception", exception={"stage": "author_quality_v1", "reason": UPGRADE_REASON})
        return case.store.update_flow(current["flow_id"], current["revision"], state,
                                      {"kind": "explicit_test_only_parent_handoff"})

    def test_old_parent_returns_but_original_worker_collects_and_new_parent_never_reclaims(self):
        case = self.fixture()
        gate = case.root / "finish-author"
        source = case.script.read_text().replace(
            "time.sleep(float(delay))", "while not pathlib.Path(delay).exists(): time.sleep(.01)")
        case.script.write_text(source)
        case.prepare_source(delay=str(gate))
        old_loop_returned = threading.Event()

        with patch("taskflow.execution_runner.run_attempt", side_effect=self.actual_process), \
                ThreadPoolExecutor(max_workers=3) as pool:
            worker = pool.submit(execution.run_worker, case.store, "original-neutral-author")
            self.addCleanup(gate.touch)
            try:
                case.attempt = self.wait(lambda: next((item for item in case.store.dashboard()["attempts"]
                                                       if item["state"] == "running"), None))
                original_id = case.attempt["attempt_id"]
                registered = case.register()

                def old_factory_main():
                    received = run_lifecycle(case.store, registered["flow_id"], poll_seconds=.005)
                    old_loop_returned.set()
                    worker.result(timeout=5)
                    return received

                old_parent = pool.submit(old_factory_main)
                self.wait(lambda: case.store.get_flow(registered["flow_id"])["state"].get("waiting"))
                self.stop_parent(case)
                self.assertTrue(old_loop_returned.wait(5))
                self.assertFalse(worker.done())
                self.assertFalse(old_parent.done())

                # This is an explicit test-only transition. No production
                # resume helper is introduced or implicitly authorized here.
                with _lifecycle_lock(case.store, registered["flow_id"]):
                    stopped = case.store.get_flow(registered["flow_id"])
                    self.assertEqual(UPGRADE_REASON, stopped["state"]["exception"]["reason"])
                    old_receipt = Path(stopped["state"]["receipt"])
                    old_bytes = old_receipt.read_bytes()
                    self.assertEqual(stopped["state"]["exception"], read_json(old_receipt)["exception"])
                    state = copy.deepcopy(stopped["state"])
                    state["history"].append({"kind": "test_only_parent_handoff", "previous_receipt": str(old_receipt)})
                    state.update(status="active", waiting=None)
                    state.pop("exception")
                    state.pop("receipt")
                    case.store.update_flow(stopped["flow_id"], stopped["revision"], state,
                                           {"kind": "explicit_test_only_parent_resumed"})

                new_parent = pool.submit(run_lifecycle, case.store, registered["flow_id"], poll_seconds=.005)
                waiting = self.wait(lambda: case.store.get_flow(registered["flow_id"])["state"].get("waiting"))
                self.assertEqual({"stage": "source_author", "state": "running"}, waiting)
                self.assertEqual(1, len(case.store.dashboard()["attempts"]))
                self.assertFalse((Path(registered["definition"]["directory"]) / "archives").exists())
                gate.touch()
                original_result = worker.result(timeout=5)
                old_result = old_parent.result(timeout=5)
                new_result = new_parent.result(timeout=5)
                self.assertEqual("completed", original_result["state"])
                self.assertEqual("exception", old_result["state"]["status"])
                self.assertEqual("research_ended", new_result["state"]["result"]["outcome"])
                self.assertEqual("accepted", case.store.get_flow(registered["flow_id"])["state"]["status"])
                self.assertEqual([original_id], [row["attempt_id"] for row in case.store.dashboard()["attempts"]])
                self.assertEqual(1, len(case.store.dashboard()["work"]))
                self.assertEqual(old_bytes, old_receipt.read_bytes())
            finally:
                gate.touch()

    def test_stale_advance_can_overwrite_stop_reason_so_exact_receipt_guard_must_hold(self):
        case = self.fixture()
        case.run_source()
        record = case.register()
        original = _author(case.store, record["definition"])

        def concurrent_stop(*args):
            self.stop_parent(case)
            return original

        with patch("taskflow.execution_author._author", side_effect=concurrent_stop):
            raced = advance_lifecycle(case.store, record["flow_id"])
        self.assertEqual("exception", raced["state"]["status"])
        self.assertNotEqual(UPGRADE_REASON, raced["state"]["exception"]["reason"])
        self.assertIn("revision", raced["state"]["exception"]["reason"])
        self.assertIsNone(raced["state"]["current_work_id"])
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))
        self.assertEqual("completed", case.store.get_attempt(case.attempt["attempt_id"])["state"])
        self.assertEqual("author_quality_v1", raced["state"]["stage"])

