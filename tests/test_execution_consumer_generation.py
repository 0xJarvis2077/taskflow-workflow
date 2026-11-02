"""Explicit dead consumer generations use neutral processes, never model CLIs."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment_factory import _source_refs, launch_lifecycle_consumer
from taskflow.execution_lifecycle import _lifecycle_lock
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment_factory as factory_fixtures


class ConsumerGenerationTests(unittest.TestCase):
    def setUp(self):
        case = factory_fixtures.ExperimentFactoryTests("test_register_only_is_idempotent_and_replaces_all_inherited_authority")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.start()
        self.case, self.root, self.store, self.repository = case, case.root, case.store, case.repository
        self.flow = self.store.list_flows()[0]
        self.directory = Path(self.flow["definition"]["directory"])
        self.processes = []
        self.original_popen = subprocess.Popen
        self.addCleanup(self.stop_neutral_children)
        module = self.repository / "taskflow/execution_experiment_factory.py"
        preserved = self.root / "previous-launcher.py"
        preserved.write_bytes(module.read_bytes())
        review = self.root / "mechanical-engineering-proof.json"
        write_json(review, {"scope": "neutral source publication fixture; no business/model judgment"})
        self.publication_path = self.root / "publication.json"
        write_json(self.publication_path, {"schema_version": 1, "kind": "validated_execution_source_publication", "accepted": True,
            "before": [{"path": str(module), **_info(module), "preserved_source": self.ref(preserved)}],
            "after": [{"path": str(module), **_info(module)},
                      {"path": str(Path(__file__).resolve()), **_info(Path(__file__))}],
            "evidence": [self.ref(review)]})
        self.recovery = {"publication": self.ref(self.publication_path), "source_refs": _source_refs(self.repository)}

    @staticmethod
    def ref(path):
        return evidence_ref(path, "neutral exact retained consumer evidence")

    def finish(self, process):
        if process.poll() is None:
            process.neutral_stop.write_text("stop", encoding="utf-8")
        process.wait(timeout=5)

    def stop_neutral_children(self):
        for process in self.processes:
            self.finish(process)

    @staticmethod
    def identity(pid):
        # The PID is an actual child. Start-identity discovery is an explicit
        # boundary double so this unit suite needs no sandbox host-ps privilege.
        return {"pid": pid, "start_identity": "neutral-fixture-start-" + str(pid), "identity_source": "neutral_process_boundary"}

    def spawn_neutral(self, argv, **kwargs):
        self.assertEqual(self.flow["flow_id"], argv[-2])
        self.assertEqual("--allow-model", argv[-1])
        self.assertTrue(kwargs["start_new_session"])
        stop = self.root / ("stop-neutral-" + str(len(self.processes)))
        code = "import pathlib,sys,time; stop=pathlib.Path(sys.argv[1])\nwhile not stop.exists(): time.sleep(.01)\n"
        process = self.original_popen([str(Path(sys.executable).resolve()), "-c", code, str(stop)], **kwargs)
        process.neutral_stop = stop
        self.processes.append(process)
        return process

    def launch(self, **kwargs):
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=self.spawn_neutral), \
             patch("taskflow.execution_experiment_factory._process_identity", side_effect=self.identity):
            return launch_lifecycle_consumer(self.store, self.store.get_flow(self.flow["flow_id"]), self.repository, **kwargs)

    def first_dead(self):
        result = self.launch()
        self.assertEqual("started", result["status"], result)
        self.finish(self.processes[-1])
        return result

    def test_explicit_generation_preserves_original_and_default_never_restarts_dead(self):
        self.first_dead()
        originals = {name: (self.directory / name).read_bytes() for name in
                     ("consumer-launch-intent.json", "consumer-launch.json", "consumer.stdout.log", "consumer.stderr.log")}
        flow_before = self.store.get_flow(self.flow["flow_id"])
        result = self.launch()
        self.assertEqual("attention", result["status"])
        self.assertEqual(1, len(self.processes))
        resumed = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertEqual("started", resumed["status"], resumed)
        self.assertEqual(1, resumed["generation"])
        self.assertEqual(2, len(self.processes))
        generation = self.directory / "consumer-generations/000001"
        self.assertTrue((generation / "consumer-launch.json").is_file())
        intent = read_json(generation / "consumer-launch-intent.json")
        self.assertEqual(self.recovery["publication"], intent["recovery"]["publication"])
        self.assertTrue(intent["old_source_refs_are_historical"])
        self.assertEqual(flow_before, self.store.get_flow(self.flow["flow_id"]))
        self.assertEqual(originals, {name: (self.directory / name).read_bytes() for name in originals})
        # The latest generation, rather than the dead original PID, is reused.
        alive = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertEqual("alive_reused", alive["status"], alive)
        self.assertEqual(2, len(self.processes))

    def test_historical_loaded_sources_are_not_claimed_as_current_and_fresh_inventory_is_exact(self):
        old = self.root / "neutral-old-code.py"
        old.write_text("# old retained launch source\n", encoding="utf-8")
        with patch("taskflow.execution_experiment_factory._source_refs", return_value=[self.ref(old)]):
            self.first_dead()
        old.write_text("# subsequently changed source path; original receipt remains historical\n", encoding="utf-8")
        intent_before = (self.directory / "consumer-launch-intent.json").read_bytes()
        resumed = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertEqual("started", resumed["status"], resumed)
        self.assertEqual(intent_before, (self.directory / "consumer-launch-intent.json").read_bytes())
        self.finish(self.processes[-1])
        for recovery in ({**self.recovery, "source_refs": self.recovery["source_refs"][:-1]},
                         {**self.recovery, "source_refs": [*self.recovery["source_refs"], self.recovery["source_refs"][0]]}):
            with self.subTest(inventory=len(recovery["source_refs"])):
                with self.assertRaisesRegex(TaskflowError, "complete exact current"):
                    self.launch(resume_dead=True, recovery=recovery)
        wrong_size = copy.deepcopy(self.recovery)
        next(row for row in wrong_size["source_refs"] if Path(row["path"]).name == "__init__.py")["size"] = False
        with self.assertRaisesRegex(TaskflowError, "source size differs"):
            self.launch(resume_dead=True, recovery=wrong_size)
        self.assertEqual(2, len(self.processes))

    def test_next_dead_generation_binds_previous_receipt_and_equal_byte_alias_is_rejected(self):
        self.first_dead()
        first = self.launch(resume_dead=True, recovery=self.recovery)
        self.finish(self.processes[-1])
        second = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertEqual(2, second["generation"])
        self.finish(self.processes[-1])
        directory = self.directory / "consumer-generations/000002"
        intent_path, receipt_path = directory / "consumer-launch-intent.json", directory / "consumer-launch.json"
        intent, receipt = read_json(intent_path), read_json(receipt_path)
        self.assertEqual(first["launch_receipt"]["path"], intent["previous_launch"]["receipt"]["path"])
        decoy = self.root / "equal-byte-original-intent.json"
        decoy.write_bytes((self.directory / "consumer-launch-intent.json").read_bytes())
        intent["original_launch"]["intent"] = self.ref(decoy)
        write_json(intent_path, intent)
        receipt["launch_intent"] = evidence_ref(intent_path, "original consumer launch intent")
        write_json(receipt_path, receipt)
        rejected = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("preceding/original", rejected["reason"])
        self.assertEqual(3, len(self.processes))
        self.assertFalse((self.directory / "consumer-generations/000003").exists())

    def test_unproven_new_identity_is_retained_and_explicit_flags_are_not_coerced(self):
        self.first_dead()
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=self.spawn_neutral), \
             patch("taskflow.execution_experiment_factory._process_identity", side_effect=lambda pid: {"pid": pid, "start_identity": None}):
            unknown = launch_lifecycle_consumer(self.store, self.store.get_flow(self.flow["flow_id"]), self.repository,
                                               resume_dead=True, recovery=self.recovery)
        self.assertTrue(unknown["consumer_spawned"])
        self.assertEqual("attention", unknown["status"])
        self.finish(self.processes[-1])
        again = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("start identity", again["reason"])
        self.assertEqual(2, len(self.processes))
        with self.assertRaises(TaskflowError):
            self.launch(resume_dead=1, recovery=self.recovery)
        with self.assertRaises(TaskflowError):
            self.launch(recovery=self.recovery)

    def test_unknown_alive_missing_identity_missing_receipt_and_business_hold_do_not_spawn(self):
        self.first_dead()
        for status in ("alive", "unknown"):
            with self.subTest(status=status), patch("taskflow.execution_experiment_factory._process_status", return_value=(status, "neutral status")):
                result = self.launch(resume_dead=True, recovery=self.recovery)
                self.assertFalse(result["consumer_spawned"])
        receipt_path = self.directory / "consumer-launch.json"
        raw = receipt_path.read_bytes()
        receipt = read_json(receipt_path)
        receipt["process"]["start_identity"] = None
        write_json(receipt_path, receipt)
        self.assertIn("start identity", self.launch(resume_dead=True, recovery=self.recovery)["reason"])
        receipt_path.write_bytes(raw)
        receipt_path.unlink()
        self.assertIn("no PID receipt", self.launch(resume_dead=True, recovery=self.recovery)["reason"])
        receipt_path.write_bytes(raw)
        record = self.store.get_flow(self.flow["flow_id"])
        self.store.update_flow(record["flow_id"], record["revision"], {**record["state"], "pending_business_scope": {}}, {"kind": "neutral_business_hold"})
        held = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("business scope", held["reason"])
        self.assertEqual(1, len(self.processes))

    def test_exact_authority_argv_database_and_publication_drift_fail_closed(self):
        self.first_dead()
        intent_path = self.directory / "consumer-launch-intent.json"
        receipt_path = self.directory / "consumer-launch.json"
        original_intent, original_receipt = intent_path.read_bytes(), receipt_path.read_bytes()
        for key, value in (("argv", ["another-command"]), ("cwd", str(self.root)), ("database", str(self.root / "other.sqlite3"))):
            intent = read_json(intent_path)
            intent[key] = value
            write_json(intent_path, intent)
            receipt = read_json(receipt_path)
            receipt["launch_intent"] = evidence_ref(intent_path, "original consumer launch intent")
            write_json(receipt_path, receipt)
            with self.subTest(field=key):
                self.assertIn("binding differs", self.launch(resume_dead=True, recovery=self.recovery)["reason"])
            intent_path.write_bytes(original_intent)
            receipt_path.write_bytes(original_receipt)
        proof = read_json(self.publication_path)
        for mutate in (lambda x: x.update(accepted=False),
                       lambda x: x["after"][0].update(sha256="0" * 64),
                       lambda x: x["after"][0].update(size=False),
                       lambda x: x["after"].pop(0)):
            changed = copy.deepcopy(proof)
            mutate(changed)
            write_json(self.publication_path, changed)
            with self.subTest(publication=changed):
                with self.assertRaises(TaskflowError):
                    self.launch(resume_dead=True, recovery={**self.recovery, "publication": self.ref(self.publication_path)})
        write_json(self.publication_path, proof)
        authority = self.case.packet_path
        authority.write_bytes(authority.read_bytes() + b" ")
        with self.assertRaisesRegex(TaskflowError, "evidence changed"):
            self.launch(resume_dead=True, recovery=self.recovery)
        self.assertEqual(1, len(self.processes))

    def test_lifecycle_lock_and_unknown_generation_keep_all_prior_records(self):
        self.first_dead()
        with _lifecycle_lock(self.store, self.flow["flow_id"]):
            blocked = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("owns its lock", blocked["reason"])
        self.assertEqual(1, len(self.processes))
        resumed = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertTrue(resumed["consumer_spawned"])
        self.finish(self.processes[-1])
        generation = self.directory / "consumer-generations/000001"
        (generation / "consumer-launch.json").unlink()
        unknown = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("no PID receipt", unknown["reason"])
        self.assertEqual(2, len(self.processes))
        self.assertFalse((self.directory / "consumer-generations/000002").exists())

    def test_spawn_failure_retains_exclusive_generation_and_missing_original_never_launches(self):
        missing = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("no original launch intent", missing["reason"])
        self.assertEqual([], self.processes)
        self.first_dead()
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=OSError("neutral spawn failure")):
            failed = launch_lifecycle_consumer(self.store, self.store.get_flow(self.flow["flow_id"]), self.repository,
                                             resume_dead=True, recovery=self.recovery)
        self.assertEqual("attention", failed["status"])
        generation = self.directory / "consumer-generations/000001"
        self.assertTrue((generation / "consumer-launch-intent.json").exists())
        self.assertTrue((generation / "consumer-launch-error.json").exists())
        again = self.launch(resume_dead=True, recovery=self.recovery)
        self.assertIn("no PID receipt", again["reason"])
        self.assertEqual(1, len(self.processes))
        self.assertFalse((self.directory / "consumer-generations/000002").exists())


if __name__ == "__main__":
    unittest.main()
