"""Synthetic round binding/consumer regressions; no provider calls or real scores."""

from pathlib import Path
import os
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution import parser
from taskflow.execution_experiment_factory import launch_lifecycle_consumer
from taskflow.execution_lifecycle import run_lifecycle
from taskflow.execution_rounds import advance_round_plan, bind_round_from_packet
from taskflow.execution_store import evidence_ref
from tests import test_execution_rounds as rounds


class RoundContinuationTests(unittest.TestCase):
    setUp = rounds.BoundedRoundTests.setUp
    register = rounds.BoundedRoundTests.register
    next_packet = rounds.BoundedRoundTests.next_packet
    simulated_factory = rounds.BoundedRoundTests.simulated_factory
    bind = rounds.BoundedRoundTests.bind

    def synthetic_spawn(self, argv, **kwargs):
        record = self.store.get_flow(argv[-2])
        directory = Path(record["definition"]["directory"])
        paths = [directory / "consumer-launch-intent.json"]
        paths.extend(sorted((directory / "consumer-generations").glob("*/consumer-launch-intent.json")))
        intent = paths[-1]
        write_json(intent.parent / ("consumer-started-" + str(os.getpid()) + ".json"), {
            "process": {"pid": os.getpid(), "start_identity": "synthetic-start", "identity_source": "test_fixture"},
            "launch_intent": evidence_ref(intent, "original consumer launch intent")})
        return SimpleNamespace(pid=os.getpid(), poll=lambda: None)

    def production_packet(self, plan):
        from tests.test_execution_experiment_factory import ExperimentFactoryTests

        fixture = ExperimentFactoryTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        packet = read_json(fixture.packet_path)
        packet.update(batch_id=plan["definition"]["batch_id"], bounded_round={
            "plan_id": plan["flow_id"], "ordinal": 2,
            "root_intent": plan["definition"]["root_intent"],
            "predecessor_digest": sha256(canonical(plan["state"]["completed"][-1])),
            "simulation_only": True})
        packet["items"] = [dict(packet["items"][0], task_id="neutral")]
        packet["confirmation"]["maximum_additional_runs_per_first"] = 0
        path = self.case.root / "production-shaped-packet.json"
        write_json(path, packet)
        config = read_json(fixture.config_path)
        for key in ("lifecycle_root", "formal_runtime_root", "publication_root"):
            config[key] = str(self.case.root / key)
        config_path = self.case.root / "production-factory.json"
        write_json(config_path, config)
        return path, config_path

    def test_real_factory_rejection_preserves_reason_and_rolls_back(self):
        plan = advance_round_plan(self.store, self.register())
        packet, config = self.production_packet(plan)
        raw = read_json(packet)
        del raw["items"][0]["source_author_attempt"]
        write_json(packet, raw)
        before = self.store.list_flows()
        with self.assertRaisesRegex(TaskflowError, "factory rejected packet:.*original identity"):
            bind_round_from_packet(self.store, plan["flow_id"], ordinal=2,
                packet_ref=evidence_ref(packet, "synthetic packet"), factory_config_ref=evidence_ref(config, "synthetic factory"))
        self.assertEqual(before, self.store.list_flows())
        self.assertFalse((self.case.root / "lifecycle_root").exists())

    def test_real_factory_and_packet_binding_with_explicit_synthetic_source_boundary(self):
        plan = advance_round_plan(self.store, self.register())
        packet, config = self.production_packet(plan)
        candidate = plan["state"]["completed"][0]["proof"]["candidate_id"]
        def source(store, record, normalized):
            self.assertEqual(0, normalized["single_confirmation"]["max_additional_attempts"])
            return store.update_flow(record["flow_id"], record["revision"], {
                **record["state"], "stage": "release", "candidate_id": candidate, "excluded_sids": []},
                {"kind": "synthetic_source_boundary"})
        refs = dict(packet_ref=evidence_ref(packet, "synthetic packet"), factory_config_ref=evidence_ref(config, "synthetic factory"))
        with patch("taskflow.execution_experiment._source", side_effect=source), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
            result = bind_round_from_packet(self.store, plan["flow_id"], ordinal=2, **refs)
            self.assertEqual(result, bind_round_from_packet(self.store, plan["flow_id"], ordinal=2, **refs))
        spawn.assert_not_called()
        with self.store._db() as db:
            self.assertEqual(3, db.execute("SELECT count(*) FROM bounded_round_work").fetchone()[0])
        self.assertFalse((self.case.root / "lifecycle_root").exists())

    def test_persistent_wait_retains_lock_until_explicit_control(self):
        plan = self.register()
        def control(_):
            from taskflow.execution_lifecycle import _lifecycle_lock
            with self.assertRaisesRegex(TaskflowError, "already has a consumer"):
                with _lifecycle_lock(self.store, plan["flow_id"]):
                    pass
            self.store.set_control("task:neutral", "pause", "synthetic control")
        with patch("taskflow.execution_lifecycle.time.sleep", side_effect=control):
            result = run_lifecycle(self.store, plan["flow_id"], wait_for_round_binding=True)
        self.assertEqual("existing_control_blocks_rounds", result["execution"])
        self.assertEqual(1, len(result["state"]["completed"]))

    def test_persistent_mode_cannot_be_used_for_other_lifecycles(self):
        with self.assertRaisesRegex(TaskflowError, "bounded round plan"):
            run_lifecycle(self.store, self.source["flow_id"], wait_for_round_binding=True)

    def test_retired_round_cli_does_not_start_a_consumer(self):
        import contextlib
        import io
        for command in ("round-bind", "round-start"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser().parse_args([command])

    def test_normal_wait_launcher_rejects_missing_exit_proof(self):
        plan = self.register()
        repository = Path(__file__).resolve().parents[1]
        process = {"pid": os.getpid(), "start_identity": "synthetic-start", "identity_source": "test_fixture"}
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=self.synthetic_spawn), \
             patch("taskflow.execution_experiment_factory._process_identity", return_value=process), \
             patch("taskflow.execution_experiment_factory.os.getpgid", return_value=os.getpid()):
            first = launch_lifecycle_consumer(self.store, plan, repository, resume_normal_wait=True)
        self.assertTrue(first["consumer_spawned"])
        with patch("taskflow.execution_experiment_factory._process_status", return_value=("dead", "synthetic dead")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
            with self.assertRaises((TaskflowError, OSError)):
                launch_lifecycle_consumer(self.store, plan, repository, resume_normal_wait=True)
        spawn.assert_not_called()

    def test_proven_normal_wait_appends_one_generation_and_unknown_never_spawns(self):
        plan = self.register()
        repository = Path(__file__).resolve().parents[1]
        process = {"pid": os.getpid(), "start_identity": "synthetic-start", "identity_source": "test_fixture"}
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=self.synthetic_spawn), \
             patch("taskflow.execution_experiment_factory._process_identity", return_value=process), \
             patch("taskflow.execution_experiment_factory.os.getpgid", return_value=os.getpid()):
            launch_lifecycle_consumer(self.store, plan, repository, resume_normal_wait=True)
            waiting = run_lifecycle(self.store, plan["flow_id"], poll_seconds=0)
            directory = Path(waiting["definition"]["directory"])
            self.assertTrue((directory / "consumer-wait-exit.json").exists(),
                            (directory / "consumer.stdout.log").read_text())
            original = (directory / "consumer-launch.json").read_bytes()
            with patch("taskflow.execution_experiment_factory._process_status", return_value=("unknown", "synthetic unknown")), \
                 patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
                result = launch_lifecycle_consumer(self.store, waiting, repository, resume_normal_wait=True)
                self.assertEqual("attention", result["status"])
                spawn.assert_not_called()
            with patch("taskflow.execution_experiment_factory._process_status", return_value=("dead", "synthetic dead")):
                result = launch_lifecycle_consumer(self.store, waiting, repository, resume_normal_wait=True)
            self.assertEqual(1, result["generation"])
            self.assertEqual(original, (directory / "consumer-launch.json").read_bytes())
            self.assertTrue((directory / "consumer-generations/000001/consumer-launch.json").exists())

    def test_actual_local_consumer_seals_normal_wait_and_resumes_without_model_work(self):
        plan = self.register()
        repository = Path(__file__).resolve().parents[1]
        before = self.store.dashboard()["attempts"]
        children = []
        original_popen = subprocess.Popen
        def spawn(argv, **kwargs):
            process = original_popen(argv, **kwargs)
            if "lifecycle-run" in argv:
                children.append(process)
            return process
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=spawn):
            first = launch_lifecycle_consumer(self.store, plan, repository, resume_normal_wait=True)
            self.assertTrue(first["consumer_spawned"], first)
            returncode = children[-1].wait(timeout=15)
            if not first.get("process", {}).get("start_identity"):
                self.skipTest("native process identity unavailable; normal-exit continuation remains blocked")
            self.assertEqual(0, returncode)
            waiting = self.store.get_flow(plan["flow_id"])
            directory = Path(waiting["definition"]["directory"])
            self.assertTrue((directory / "consumer-wait-exit.json").exists())
            second = launch_lifecycle_consumer(self.store, waiting, repository, resume_normal_wait=True)
            self.assertEqual(1, second["generation"])
            self.assertEqual(0, children[-1].wait(timeout=15))
        self.assertEqual(1, len(self.store.get_flow(plan["flow_id"])["state"]["completed"]))
        self.assertEqual(before, self.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
