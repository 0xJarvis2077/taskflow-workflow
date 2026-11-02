"""Bootstrap existing consumers once; never turn process uncertainty into retry."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment_factory import _process_status, launch_lifecycle_consumer, start_experiments_from_packet
from taskflow.execution_lifecycle import _lifecycle_lock
from taskflow.execution_store import ExecutionStore, evidence_ref
from tools.execution_simulation import fixture_policy


class ExperimentFactoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repository = Path(__file__).resolve().parents[1]
        self.store = ExecutionStore(self.root / "execution.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        source = self.root / "author-business.json"
        write_json(source, {"controller": "fixture-controller", "nature": "neutral bootstrap fixture"})
        source_ref = evidence_ref(source, "neutral original author authority")
        items = []
        for index in range(2):
            items.append({"task_id": f"topic-{index}", "source_author_quality_flow": f"author-flow-{index}",
                "source_author_intent": f"author-intent-{index}", "source_author_attempt": f"author-attempt-{index}",
                "source_author_sid": f"known-original-sid-{index}", "source_business_packet": str(source),
                "source_business_packet_sha256": source_ref["sha256"],
                "first_experiment_business_unknown": "A specific neutral whole-job observation; no target model used.",
                "version_id": f"version-{index}", "first_run_id": f"run-{index}-01",
                "first_evaluation_id": f"eval-{index}-01", "conditional_one_confirmation_run_id": f"run-{index}-02",
                "conditional_one_confirmation_evaluation_id": f"eval-{index}-02"})
        self.packet_path = self.root / "packet.json"
        write_json(self.packet_path, {"schema_version": 1, "controller": "fixture-controller", "items": items,
            "prerequisites": ["An actual independently accepted current candidate."],
            "preauthorized_normal_chain": "Retain the exact neutral original experiment chain.",
            "confirmation": {"maximum_additional_runs_per_first": 1, "only_if": "Actual eligible original<=3/7."}})
        self.config_path = self.root / "factory.json"
        write_json(self.config_path, {"schema_version": 1, "policy_id": "fixture-v1", "repository": str(self.repository),
            "lifecycle_root": str(self.root / "lifecycles"), "formal_runtime_root": str(self.root / "formal"),
            "publication_root": str(self.root / "publications"), "technical": {
                "real_repository": str(self.repository), "formal_config_base": {"model": "fixture-cli", "authorization": {"old": True}},
                "local_profile_policy": {"authorization": {"old": True}}, "review_runner": {"mode": "test_fixture"}}})
        self.packet_ref = evidence_ref(self.packet_path, "neutral exact shared authority")
        self.config_ref = evidence_ref(self.config_path, "neutral shared execution configuration")

    def start(self, allow=False):
        return start_experiments_from_packet(self.store, packet_ref=self.packet_ref,
            factory_config_ref=self.config_ref, allow_model=allow)

    def process(self, pid):
        return {"pid": pid, "start_identity": "bound-start-" + str(pid), "identity_source": "mock_boundary"}

    def spawn_fixture(self, argv, **kwargs):
        directory = Path(next(f["definition"]["directory"] for f in self.store.list_flows() if f["flow_id"] == argv[-2]))
        self.assertTrue((directory / "consumer-launch-intent.json").exists())
        self.assertEqual(subprocess.DEVNULL, kwargs["stdin"])
        self.assertTrue(kwargs["start_new_session"])
        self.assertEqual(self.repository, kwargs["cwd"])
        self.assertEqual([str(Path(sys.executable).resolve()), "-m", "taskflow", "execution", "--db", str(self.store.path)], argv[:6])
        self.assertEqual("lifecycle-run", argv[6])
        self.assertEqual("--allow-model", argv[-1])
        self.assertEqual("xb", kwargs["stdout"].mode)
        return SimpleNamespace(pid=12000 + len(self.spawned))

    def mocked_launch(self):
        self.spawned = []
        def spawn(argv, **kwargs):
            result = self.spawn_fixture(argv, **kwargs)
            self.spawned.append(argv)
            return result
        return patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=spawn)

    def test_register_only_is_idempotent_and_replaces_all_inherited_authority(self):
        with patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
            first, second = self.start(), self.start()
        self.assertFalse(first["attention"], first)
        self.assertEqual(first, second)
        self.assertEqual(2, len(self.store.list_flows()))
        self.assertEqual(0, first["consumers_started"])
        spawn.assert_not_called()
        for flow in self.store.list_flows():
            definition = flow["definition"]
            self.assertEqual("experiment_v1", definition["kind"])
            technical = definition["technical"]
            self.assertEqual(self.packet_ref, technical["formal_config_base"]["authorization"])
            self.assertEqual(self.packet_ref, technical["local_profile_policy"]["authorization"])
            self.assertIn(flow["flow_id"], technical["formal_runtime_root"])
            self.assertEqual(definition["task_id"], Path(technical["publication_root"]).name)
            self.assertFalse(Path(definition["directory"]).exists())
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_each_existing_consumer_launches_once_and_matching_live_process_reuses(self):
        with self.mocked_launch() as spawn, \
             patch("taskflow.execution_experiment_factory._process_identity", side_effect=self.process), \
             patch("taskflow.execution_experiment_factory.os.getpgid", side_effect=lambda pid: pid):
            first = self.start(True)
            with patch("taskflow.execution_experiment_factory._process_status", return_value=("alive", "bound original process")):
                second = self.start(True)
        self.assertEqual(2, first["consumers_started"], first)
        self.assertEqual({"alive_reused"}, {row["status"] for row in second["items"]})
        self.assertEqual(2, spawn.call_count)
        for flow in self.store.list_flows():
            directory = Path(flow["definition"]["directory"])
            receipt = read_json(directory / "consumer-launch.json")
            self.assertEqual(receipt["process"]["pid"], receipt["pgid"])
            self.assertTrue(read_json(directory / "consumer-launch-intent.json")["source_refs"])
        self.assertEqual([], self.store.dashboard()["work"])

    def test_missing_receipt_unknown_identity_and_dead_nonterminal_never_respawn(self):
        with self.mocked_launch() as spawn, \
             patch("taskflow.execution_experiment_factory._process_identity", side_effect=self.process), \
             patch("taskflow.execution_experiment_factory.os.getpgid", side_effect=lambda pid: pid):
            self.start(True)
            flow = self.store.list_flows()[0]
            directory = Path(flow["definition"]["directory"])
            receipt_path = directory / "consumer-launch.json"
            original = receipt_path.read_bytes()
            receipt_path.unlink()
            missing = launch_lifecycle_consumer(self.store, flow, self.repository)
            self.assertIn("no PID receipt", missing["reason"])
            receipt_path.write_bytes(original)
            for status in ("unknown", "dead"):
                with patch("taskflow.execution_experiment_factory._process_status", return_value=(status, status + " original")):
                    value = launch_lifecycle_consumer(self.store, flow, self.repository)
                self.assertEqual("attention", value["status"])
                self.assertEqual(status, value["process_status"])
            self.assertEqual(2, spawn.call_count)
            self.assertTrue((directory / "consumer-launch-intent.json").exists())

    def test_terminal_result_reuses_without_launch_and_shared_config_change_cannot_replace_definition(self):
        self.start()
        flow = self.store.list_flows()[0]
        receipt = self.root / "terminal.json"
        write_json(receipt, {"status": "exception", "reason": "Original bounded decision retained."})
        self.store.update_flow(flow["flow_id"], flow["revision"], {**flow["state"], "status": "exception",
            "receipt": str(receipt)}, {"kind": "neutral_terminal_fixture"})
        with patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
            value = launch_lifecycle_consumer(self.store, self.store.get_flow(flow["flow_id"]), self.repository)
        self.assertEqual("terminal_reused", value["status"])
        spawn.assert_not_called()
        config = read_json(self.config_path)
        config["formal_runtime_root"] = str(self.root / "other-formal")
        write_json(self.config_path, config)
        self.config_ref = evidence_ref(self.config_path, "changed neutral configuration")
        changed = self.start()
        self.assertTrue(changed["attention"])
        self.assertEqual(2, len(self.store.list_flows()))

    def test_popen_failure_retains_exclusive_intent_and_does_not_retry(self):
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=OSError("neutral spawn denied")) as spawn:
            first, second = self.start(True), self.start(True)
        self.assertTrue(first["attention"])
        self.assertTrue(second["attention"])
        self.assertEqual(2, spawn.call_count)
        for flow in self.store.list_flows():
            directory = Path(flow["definition"]["directory"])
            self.assertTrue((directory / "consumer-launch-error.json").exists())
            self.assertTrue((directory / "consumer-launch-intent.json").exists())

    def test_an_existing_lifecycle_lock_prevents_an_untracked_second_consumer(self):
        self.start()
        flow = self.store.list_flows()[0]
        with _lifecycle_lock(self.store, flow["flow_id"]), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen") as spawn:
            result = launch_lifecycle_consumer(self.store, flow, self.repository)
        self.assertEqual("attention", result["status"])
        self.assertIn("owns its lock", result["reason"])
        self.assertFalse((Path(flow["definition"]["directory"]) / "consumer-launch-intent.json").exists())
        spawn.assert_not_called()

    def test_process_reuse_requires_matching_live_identity_and_missing_pid_is_unknown(self):
        identity = self.process(os.getpid())
        with patch("taskflow.execution_experiment_factory._process_identity", return_value=identity):
            self.assertEqual("alive", _process_status(identity)[0])
            self.assertEqual("dead", _process_status({**identity, "start_identity": "another-start"})[0])
            self.assertEqual("unknown", _process_status({"pid": os.getpid()})[0])
        self.assertEqual("unknown", _process_status(None)[0])
        self.assertEqual("unknown", _process_status({"pid": True})[0])
        with patch("taskflow.execution_experiment_factory.os.kill", side_effect=ProcessLookupError):
            self.assertEqual("dead", _process_status(identity)[0])
        with patch("taskflow.execution_experiment_factory.os.kill", side_effect=PermissionError):
            self.assertEqual("unknown", _process_status(identity)[0])

    def test_duplicate_authority_items_are_rejected_and_long_source_prefixes_do_not_collide(self):
        packet = read_json(self.packet_path)
        packet["items"][1]["source_author_quality_flow"] = packet["items"][0]["source_author_quality_flow"]
        packet["items"][1]["task_id"] = packet["items"][0]["task_id"]
        write_json(self.packet_path, packet)
        self.packet_ref = evidence_ref(self.packet_path, "duplicate neutral authority fixture")
        with self.assertRaisesRegex(TaskflowError, "duplicate item selectors"):
            self.start()
        self.assertEqual([], self.store.list_flows())
        for index, item in enumerate(packet["items"]):
            item["source_author_quality_flow"] = "same-prefix-" + "x" * 45 + str(index)
        write_json(self.packet_path, packet)
        self.packet_ref = evidence_ref(self.packet_path, "distinct long source flow fixture")
        result = self.start()
        self.assertFalse(result["attention"], result)
        self.assertEqual(2, len({row["flow_id"] for row in result["items"]}))

    def test_exported_launcher_reuses_existing_author_lifecycle_without_new_definition(self):
        definition = {"kind": "author_quality_v1", "directory": str(self.root / "existing-author"),
                      "task_id": "topic-0", "owner": "fixture-controller", "policy_id": "fixture-v1"}
        record = self.store.register_flow("existing-author-flow", definition, [self.packet_ref], {
            "status": "active", "stage": "author_handoff", "history": [], "archives": []})
        with self.mocked_launch(), \
             patch("taskflow.execution_experiment_factory._process_identity", side_effect=self.process), \
             patch("taskflow.execution_experiment_factory.os.getpgid", side_effect=lambda pid: pid):
            result = launch_lifecycle_consumer(self.store, record, self.repository)
        self.assertTrue(result["consumer_spawned"], result)
        self.assertEqual(1, len(self.store.list_flows()))
        self.assertEqual([], self.store.dashboard()["work"])

    def test_real_neutral_child_survives_bootstrap_parent_exit_in_its_own_session(self):
        self.start()
        flow = self.store.list_flows()[0]
        child_evidence = self.root / "independent-child.json"
        neutral = self.root / "neutral-child.py"
        neutral.write_text("import json,os,pathlib,sys,time\n"
            "original_parent=int(sys.argv[2]);deadline=time.monotonic()+5\n"
            "while os.getppid()==original_parent and time.monotonic()<deadline:time.sleep(.01)\n"
            "pathlib.Path(sys.argv[1]).write_text(json.dumps({'pid':os.getpid(),'ppid':os.getppid(),'pgid':os.getpgrp(),"
            "'original_parent':original_parent,'model_calls':0,'stdin_eof':sys.stdin.read()==''}))\n", encoding="utf-8")
        parent = self.root / "bootstrap-parent.py"
        parent.write_text("import os,pathlib,subprocess,sys\nfrom unittest.mock import patch\n"
            "from taskflow.execution_store import ExecutionStore\nfrom taskflow import execution_experiment_factory as factory\n"
            "real_popen=subprocess.Popen\n"
            "def neutral_launch(argv,**kwargs):\n"
            " return real_popen([sys.executable,sys.argv[3],sys.argv[4],str(os.getpid())],**kwargs)\n"
            "store=ExecutionStore(pathlib.Path(sys.argv[1]));record=store.get_flow(sys.argv[2])\n"
            "with patch.object(factory.subprocess,'Popen',side_effect=neutral_launch),patch.object(factory,'_process_identity',side_effect=lambda pid:{'pid':pid,'start_identity':None}):\n"
            " result=factory.launch_lifecycle_consumer(store,record,pathlib.Path.cwd())\n"
            " if not result['consumer_spawned']:raise RuntimeError(result)\n", encoding="utf-8")
        finished = subprocess.run([str(Path(sys.executable).resolve()), str(parent), str(self.store.path), flow["flow_id"],
                                  str(neutral), str(child_evidence)], cwd=self.repository, capture_output=True, text=True,
                                  timeout=10, env={**os.environ, "PYTHONPATH": str(self.repository)})
        self.assertEqual(0, finished.returncode, finished.stderr)
        deadline = time.monotonic() + 5
        while not child_evidence.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(child_evidence.exists(), finished.stderr)
        observed = read_json(child_evidence)
        self.detached_probe = observed
        self.assertEqual(observed["pid"], observed["pgid"])
        self.assertNotEqual(observed["original_parent"], observed["ppid"])
        self.assertTrue(observed["stdin_eof"])
        self.assertEqual(0, observed["model_calls"])


if __name__ == "__main__":
    unittest.main()
