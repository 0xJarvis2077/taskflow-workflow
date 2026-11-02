"""Opt-in producer startup and the collector's actual isolated trace contract.

No external model is called. The isolated adapter is a local Python script with
empty stdout and three actual inner events. Other subprocess seams are labeled.
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import shutil
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow import execution_experiment_factory as factory
from taskflow import execution_runner as runner
from taskflow.contracts import PUBLIC_ROLES, TaskflowError, read_json, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _receive, run_lifecycle
from taskflow.execution_original_pause import original_pause_grant, validated_original
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment_factory as factories
from tests.test_execution_original_pause import controls, metadata, register_pause, source_fixture


SID = "00000000-0000-4000-8000-000000000001"


def isolated_solver(case, **kwargs):
    inner = kwargs["runtime"]; inner.mkdir(parents=True)
    work = inner / "public-work"; work.mkdir()
    (inner / "solver-scratch").mkdir()
    for row in case.candidate["files"]:
        if row["role"] in PUBLIC_ROLES:
            target = work / row["package_path"]; target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(row["path"], target)
    for name in ("runtime_gate.py", "file_evidence.py", "source-lock.json", "baseline-plan.json"):
        (inner / name).write_text("{}")
    for name in ("shell-probes.json", "exec-context.json"):
        write_json(inner / name, {"passed": True})
    (inner / "request.md").write_text("Neutral local adapter, no provider/model call.")
    plan = {"cwd": str(work), "root": str(inner), "run_id": kwargs["config"]["run_id"], "task_id": "neutral",
        "requested_model": "fixture-cli", "argv": ["fixture-not-executed", "-c", 'model_reasoning_effort="fixture"'],
        "launcher_python": sys.executable, "timeout_seconds": 15,
        "input_digest": case.candidate["input_digest"], "public_tree": {
            str(path.relative_to(work)): runner._info(path) for path in work.rglob("*") if path.is_file()},
        "frozen_binding": {"snapshot_directory": str(case.snapshot), "taskflow_input_digest": case.candidate["input_digest"]},
        "source_lock_sha256": runner._info(inner / "source-lock.json")["sha256"],
        "request_sha256": runner._info(inner / "request.md")["sha256"],
        "probe_evidence_hashes": {name: runner._info(inner / name)["sha256"] for name in ("shell-probes.json", "exec-context.json")}}
    write_json(inner / "execution-plan.json", plan)
    digest = runner._info(inner / "execution-plan.json")["sha256"]
    write_json(inner / "plan-lock.json", {"sha256": digest})
    write_json(inner / "launch-review.json", {"approved": True, "reviewer": "neutral schema fixture only",
        "plan_sha256": digest, "input_digest": case.candidate["input_digest"]})
    script = """
import datetime,json,pathlib,sys
r=pathlib.Path(__file__).parent
if '--execute' in sys.argv:
 out=r/'real-run';out.mkdir();(out/'file-change-evidence').mkdir()
 events=[{'type':'thread.started','thread_id':SID},{'type':'item.completed','item':{'type':'agent_message','text':'Neutral actual inner delivery.'}},{'type':'turn.completed'}]
 trace='\\n'.join(json.dumps(e) for e in events)+'\\n'
 (out/'trace.jsonl').write_text(trace);(out/'request.md').write_bytes((r/'request.md').read_bytes())
 now=datetime.datetime.now(datetime.timezone.utc).isoformat()
 attempt={'run_id':RUN,'status':'process_exited','returncode':0,'exceptions':[],'interventions':[],'started_at':now,'ended_at':now}
 (out/'attempt.json').write_text(json.dumps(attempt));(r/'execution-attempt.json').write_text(json.dumps(attempt))
 (out/'file-change-evidence/summary.json').write_text(json.dumps({'observer_stopped':True,'trace_bytes_at_stop':len(trace.encode())}))
 (r/'post-run-input-readback.json').write_text(json.dumps({'matches':True}))
 pathlib.Path('answer.md').write_text('A neutral result.')
""".replace("SID", repr(SID)).replace("RUN", repr(kwargs["config"]["run_id"]))
    (inner / "execute_diagnostic.py").write_text(script)
    return {"runner": {"intent_id": kwargs["config"]["intent_id"], "policy_version": "fixture-v1", "role": "solver",
        "mode": "existing_isolated", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
        "cwd": str(work), "argv": [sys.executable], "inputs": [], "outputs": ["answer.md"], "request_sections": [],
        "isolated": {"runtime": str(inner), "flavor": "formal"}}}


def isolated_import(case, **kwargs):
    # The existing state-machine import adapter copied trace.jsonl by default.
    # Select the actual collector trace; never rewrite either sealed trace.
    actual_copy = shutil.copyfile
    archive = kwargs["archive"]

    def copy(source, target, **options):
        if Path(source) == archive / "trace.jsonl":
            source = archive / "observed-trace.jsonl"
        return actual_copy(source, target, **options)

    with patch("tests.test_execution_experiment.shutil.copyfile", side_effect=copy):
        return case.import_original_fixture(**kwargs)


class OriginalPauseProducerTests(unittest.TestCase):
    def guard_fixture(self):
        case, source = source_fixture(self, stop="unprepared")
        follower = register_pause(case, source)
        work_id = _node({**source, "state": {**source["state"], "run_index": 1}}, "solver")
        # An ordinary ready work item exercises the real claim transaction;
        # its exact ID is the only aspect relevant to the guard.
        case.store.enqueue(work_id, "neutral", "neutral-initial", "quality", "fixture-v1",
            intent_id="guard-race-intent", purpose="Neutral guard concurrency fixture",
            request_digest="0" * 64)
        scope = "work:" + work_id
        def prepare():
            return case.store.prepare_original_pause_control(
                follower["flow_id"], follower["definition_digest"], scope, "Neutral guard fixture")
        return case, follower, work_id, scope, prepare

    def test_initial_guard_fails_closed_after_claim_and_terminal_attempt(self):
        for state in ("claimed", "completed", "ready"):
            with self.subTest(state=state):
                case, follower, work_id, scope, prepare = self.guard_fixture()
                attempt = case.store.claim("guard-race-worker", [work_id])
                self.assertIsNotNone(attempt)
                if state != "claimed":
                    # Preserve the attempt row while simulating a historical terminal
                    # or inconsistent ready work state; neither can be claimed anew.
                    with case.store._db(True) as db:
                        db.execute("UPDATE attempts SET state='completed' WHERE work_id=?", (work_id,))
                        db.execute("UPDATE work SET state=? WHERE id=?", (state, work_id))
                with self.assertRaisesRegex(TaskflowError, "too late"):
                    prepare()
                self.assertEqual([], controls(case.store))
                self.assertEqual(follower, case.store.get_flow(follower["flow_id"]))

    def test_guard_and_claim_are_serialized_and_prepared_reentry_preserves_resume(self):
        for _ in range(4):
            case, follower, work_id, scope, prepare = self.guard_fixture()
            barrier = threading.Barrier(2)
            def guarded():
                barrier.wait()
                try:
                    return prepare()
                except TaskflowError as exc:
                    return exc
            def claim():
                barrier.wait()
                return case.store.claim("guard-race-worker", [work_id])
            with ThreadPoolExecutor(max_workers=2) as pool:
                guard_future, claim_future = pool.submit(guarded), pool.submit(claim)
                guard, attempt = guard_future.result(), claim_future.result()
            if isinstance(guard, TaskflowError):
                self.assertIn("too late", str(guard))
                self.assertIsNotNone(attempt)
                self.assertEqual([], controls(case.store))
            else:
                self.assertIsNone(attempt)
                self.assertEqual("pause", controls(case.store)[0]["action"])
                case.store.set_control(scope, "resume", "Later explicit human decision")
                self.assertIsNotNone(case.store.claim("after-resume", [work_id]))
                self.assertEqual(guard, prepare())
                self.assertEqual("resume", controls(case.store)[0]["action"])

    def test_prepared_and_accepted_reentry_do_not_replace_later_resume(self):
        case, follower, work_id, scope, prepare = self.guard_fixture()
        prepared = prepare()
        case.store.set_control(scope, "resume", "Later explicit human decision")
        self.assertIsNotNone(case.store.claim("after-resume", [work_id]))
        self.assertEqual(prepared, prepare())
        accepted = case.store.update_flow(follower["flow_id"], prepared["revision"],
            {**prepared["state"], "status": "accepted"}, {"kind": "neutral terminal fixture"})
        self.assertEqual(accepted, prepare())
        self.assertEqual("resume", controls(case.store)[0]["action"])

    def factory_case(self, *, grant=None):
        case = factories.ExperimentFactoryTests(); case.setUp(); self.addCleanup(case.doCleanups)
        packet = read_json(case.packet_path); packet["items"] = packet["items"][:1]
        if grant is not None:
            packet["original_score_pause"] = grant
        write_json(case.packet_path, packet)
        case.packet_ref = evidence_ref(case.packet_path, "explicit neutral future experiment packet")
        return case

    def mock_processes(self):
        launched = []

        def spawn(argv, **kwargs):
            launched.append(argv)
            return SimpleNamespace(pid=24000 + len(launched))

        return launched, patch.object(factory.subprocess, "Popen", side_effect=spawn)

    def test_legacy_packet_keeps_old_register_only_behavior_without_any_controls(self):
        case = self.factory_case()
        with patch.object(factory.subprocess, "Popen") as spawn:
            first, second = case.start(False), case.start(False)
        self.assertEqual(first, second); spawn.assert_not_called()
        self.assertEqual([], controls(case.store))
        self.assertEqual(1, len(case.store.list_flows()))
        self.assertNotIn("original_pause", first["items"][0])
        self.assertNotIn("control_consumers_started", first)

    def test_grant_register_only_starts_actual_control_child_waiting_without_any_model_work(self):
        case = self.factory_case(grant={"round_scope": "this_round_original", "first_run_only": True})
        actual, children = factory.subprocess.Popen, []

        def observe(*args, **kwargs):
            process = actual(*args, **kwargs)
            if isinstance(args[0], list) and "lifecycle-run" in args[0]: children.append(process)
            return process

        with patch.object(factory.subprocess, "Popen", side_effect=observe):
            result = case.start(False)
        pause = result["items"][0]["original_pause"]
        self.assertEqual(0, result["consumers_started"])
        self.assertEqual(1, result["control_consumers_started"])
        self.assertEqual(1, len(children))
        follower = case.store.get_flow(pause["flow_id"])
        source = case.store.get_flow(follower["definition"]["source_flow_id"])
        self.assertTrue(follower["state"]["confirmation_control_prepared"])
        second = _node({**source, "state": {**source["state"], "run_index": 1}}, "solver")
        self.assertEqual("work:" + second, controls(case.store)[0]["scope"])
        intent = read_json(Path(follower["definition"]["directory"]) / "consumer-launch-intent.json")
        self.assertFalse(intent["allow_model"]); self.assertNotIn("--allow-model", intent["argv"])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            follower = case.store.get_flow(pause["flow_id"])
            if follower["state"].get("waiting") == "first_original_not_received": break
            time.sleep(.05)
        self.assertEqual("first_original_not_received", follower["state"]["waiting"])
        self.assertEqual([], metadata(case.store)["attempts"])
        self.assertEqual([], metadata(case.store)["work"])
        # End this fixture-only observer normally, without inventing a score or
        # sending a signal. Real accepted-score consumption is tested separately.
        case.store.update_flow(follower["flow_id"], follower["revision"],
            {**follower["state"], "status": "accepted", "result": {"nature": "test_fixture", "outcome": "observer test finished"}},
            {"kind": "neutral observer fixture terminal"})
        self.assertEqual(0, children[0].wait(timeout=10))
        self.assertIsNone(case.store.get_flow(source["flow_id"])["state"]["current_work_id"])

    def test_two_calls_reuse_one_control_and_start_it_before_any_opted_in_model_consumer(self):
        case = self.factory_case(grant={"round_scope": "this_round_original", "first_run_only": True})
        launched, spawn = self.mock_processes()
        with spawn, patch.object(factory, "_process_identity", side_effect=lambda pid: {"pid": pid, "start_identity": str(pid)}), \
                patch.object(factory.os, "getpgid", side_effect=lambda pid: pid):
            first = case.start(True)
            with patch.object(factory, "_process_status", return_value=("alive", "neutral exact PID fixture")):
                second = case.start(True)
        self.assertFalse(first["attention"], first); self.assertFalse(second["attention"], second)
        self.assertEqual(2, len(launched)); self.assertNotIn("--allow-model", launched[0]); self.assertIn("--allow-model", launched[1])
        self.assertEqual(1, first["consumers_started"]); self.assertEqual(1, first["control_consumers_started"])
        self.assertEqual(2, len(case.store.list_flows())); self.assertEqual(1, len(controls(case.store)))
        follower = case.store.get_flow(first["items"][0]["original_pause"]["flow_id"])
        _receive(case.store, case.store.update_flow(follower["flow_id"], follower["revision"],
            {**follower["state"], "status": "accepted", "result": {"nature": "test_fixture", "outcome": "terminal producer reuse boundary"}},
            {"kind": "neutral accepted-followup branch fixture"}))
        case.store.set_control(controls(case.store)[0]["scope"], "resume", "Later explicit human decision.")
        before = controls(case.store)
        with patch.object(factory.subprocess, "Popen") as prohibited:
            resumed = case.start(False)
        prohibited.assert_not_called(); self.assertEqual(before, controls(case.store))
        self.assertEqual("terminal_reused", resumed["items"][0]["original_pause"]["control_consumer"]["status"])

    def test_wrong_typed_grant_is_rejected_before_any_registration_or_launch(self):
        case = self.factory_case()
        original = read_json(case.packet_path)
        for grant in (True, None, {}, {"round_scope": "this_round_original", "first_run_only": 1},
                      {"round_scope": "baseline", "first_run_only": True},
                      {"round_scope": "this_round_original", "first_run_only": True, "after_score": "pause"}):
            packet = {**original, "original_score_pause": grant}; write_json(case.packet_path, packet)
            case.packet_ref = evidence_ref(case.packet_path, "explicit malformed grant fixture")
            with self.subTest(grant=grant), patch.object(factory.subprocess, "Popen") as spawn, self.assertRaises(TaskflowError):
                case.start(False)
            spawn.assert_not_called(); self.assertEqual([], case.store.list_flows()); self.assertEqual([], controls(case.store))
        self.assertIsNone(original_pause_grant(original))

    def test_existing_isolated_empty_stdout_uses_actual_observed_events_through_original_pause(self):
        case, source = source_fixture(self, solver_prepare=isolated_solver, original_import=isolated_import, allow_solver=True)
        root = Path(source["state"]["formal_attempt"]["runtime_path"])
        self.assertEqual(b"", (root / "trace.jsonl").read_bytes())
        self.assertEqual(3, len((root / "observed-trace.jsonl").read_text().splitlines()))
        before = metadata(case.store)
        original = validated_original(case.store, source)
        follower = register_pause(case, source)
        result = run_lifecycle(case.store, follower["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(SID, original["solver_sid"])
        self.assertEqual(before, metadata(case.store)); self.assertEqual("pause", controls(case.store)[0]["action"])
        self.assertEqual(b"", (root / "trace.jsonl").read_bytes())


if __name__ == "__main__":
    unittest.main()
