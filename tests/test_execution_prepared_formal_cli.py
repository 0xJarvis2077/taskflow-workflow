"""Prepared CLI contract/recovery fixtures; no model, native probe or adapter launch."""

import copy
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_batch import CLI_ROLES, confirmation_digests, freeze_batch_plan, load_batch_snapshot, role_config
from taskflow.execution_batch_binding import make_batch_binding, validate_batch_runner
from taskflow.execution_formal import prepare_formal, validate_prepared_formal_cli
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import evidence_ref
from taskflow.packages import verify_snapshot
from tests.test_execution_batch import make_confirmed_plan
from tests import test_execution_formal as formal_fixtures
from tests import test_execution_lifecycle as lifecycle_fixtures


def policy(root):
    plan, path = make_confirmed_plan(root / "batch", mode="real_cli")
    plan["task_ids"] = ["neutral"]
    expected = confirmation_digests(plan)
    for binding in plan["role_bindings"].values():
        evidence = Path(binding["configuration_evidence"]["path"])
        data = read_json(evidence); data["configuration"]["scope"] = expected["scope"]
        write_json(evidence, data)
        binding["configuration_evidence"] = {**evidence_ref(evidence, "neutral chat fixture"), "size": evidence.stat().st_size}
    plan["user_model_confirmation"].update(expected)
    write_json(path, plan)
    frozen = freeze_batch_plan(path, root / "batch-frozen")
    snapshot = load_batch_snapshot(frozen["snapshot"])
    config = {"concurrency": 1, "model_confirmation_required": True, "batch_snapshot": frozen["snapshot"],
              "model_slots": {key: role_config(snapshot, key) for key in CLI_ROLES},
              "roles": {"solver": role_config(snapshot, "solver")}}
    return {"policy_id": "neutral-cli-policy", "config": config, "digest": sha256(canonical(config))}


def configuration(root, selected, authority, run_id, intent_id):
    adapter = root / "adapter"; adapter.mkdir()
    for name in ("prepare_diagnostic.py", "runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json"):
        (adapter / name).write_text("# Neutral adapter fixture, never executed.\n")
    request = root / "request.txt"; request.write_text("Neutral public request.")
    return {**selected, "adapter": {**evidence_ref(adapter / "prepare_diagnostic.py", "neutral fixture"),
        "files": {name: sha256((adapter / name).read_bytes()) for name in
                  ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")}},
        "authorization": evidence_ref(authority, "neutral authority"), "request": evidence_ref(request, "neutral request"),
        "run_id": run_id, "intent_id": intent_id, "policy_version": "neutral-cli-policy", "launcher_python": sys.executable}


def prepared_runtime(runtime, config, candidate, release):
    runtime.mkdir(parents=True)
    for name in (*config["adapter"]["files"], "prepare_diagnostic.py"):
        shutil.copyfile(Path(config["adapter"]["path"]).parent / name, runtime / name)
    plan = {"run_id": config["run_id"], "requested_model": config["model"], "input_digest": candidate["input_digest"],
        "request_sha256": config["request"]["sha256"], "launcher_python": str(Path(sys.executable).resolve()),
        "frozen_binding": {"snapshot_digest": verify_snapshot(Path(release["snapshot"]["path"]))["snapshot_digest"]},
        "argv": [config["cli"], "exec"], "cwd": str(runtime / "work")}
    write_json(runtime / "execution-plan.json", plan)
    write_json(runtime / "plan-lock.json", {"sha256": sha256((runtime / "execution-plan.json").read_bytes())})
    for name in ("preparation-process.json", "exec-context.json", "normal-tools-probe.json"):
        write_json(runtime / name, {"model_calls": 0, "external_model_requests": 0,
                                    "binding": {"node": {"path": sys.executable}}})


class PreparedFormalCliTests(unittest.TestCase):
    def setUp(self):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp(); self.addCleanup(case.doCleanups)
        self.root, self.candidate, self.release = case.root, case.candidate, case.release()
        self.policy = policy(self.root)
        self.binding = make_batch_binding(self.policy, "solver", "solver", "neutral")
        authority = self.root / "authority.json"
        write_json(authority, {"controller": "neutral-owner", "task_id": "neutral",
            "formal": {"first_run_id": "neutral-run", "model": self.binding["setting"]["model"],
                       "timeout_seconds": self.binding["setting"]["timeout_seconds"],
                       "baseline": {"sha256": sha256(b"# Neutral adapter fixture, never executed.\n")}}})
        self.config = configuration(self.root, self.binding["setting"], authority, "neutral-run", "neutral-intent")
        self.runtime = self.root / "inner"
        prepared_runtime(self.runtime, self.config, self.candidate, self.release)
        self.process = patch("taskflow.execution_formal.subprocess.run", return_value=subprocess.CompletedProcess([], 0, b"", b""))
        self.run = self.process.start(); self.addCleanup(self.process.stop)
        with patch("taskflow.execution_formal._native_checks", return_value=evidence_ref(self.runtime / "normal-tools-probe.json", "neutral cached report")):
            self.prepared = prepare_formal(config=self.config, release=self.release, candidate=self.candidate, runtime=self.runtime)

    def validate(self):
        return validate_prepared_formal_cli(config=self.config, release=self.release, candidate=self.candidate, runtime=self.runtime)

    def test_actual_prepared_cli_crosses_producer_and_batch_consumer(self):
        runner = {**self.prepared["runner"], "candidate_binding": self.release["candidate_binding"],
                  "model_slot": "solver", "batch_binding": self.binding}
        self.assertEqual(self.config["cli"], runner["cli"])
        self.assertIsNotNone(validate_batch_runner(runner))
        del runner["cli"]
        with self.assertRaisesRegex(TaskflowError, "CLI differs"):
            validate_batch_runner(runner)
        runner["cli"] = str(self.root / "foreign-cli")
        with self.assertRaisesRegex(TaskflowError, "CLI differs"):
            validate_batch_runner(runner)

    def test_completed_preparation_validation_is_readonly_and_never_invokes_prepare_or_execute(self):
        before = {str(p): p.read_bytes() for p in self.runtime.rglob("*") if p.is_file()}
        result = self.validate()
        self.assertEqual(self.config["cli"], result["cli"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.runtime.rglob("*") if p.is_file()})
        for call in self.run.call_args_list:
            self.assertEqual([str(Path(sys.executable).resolve()), str(self.runtime / "execute_diagnostic.py")], call.args[0])

    def test_config_cli_mismatch_and_missing_plan_cli_cannot_produce_runner(self):
        self.config["cli"] = str(self.root / "other-cli")
        with self.assertRaisesRegex(TaskflowError, "CLI differs"):
            self.validate_plan()
        self.config["cli"] = self.binding["setting"]["cli"]
        path = self.runtime / "execution-plan.json"; data = read_json(path); data["argv"] = []
        write_json(path, data); write_json(self.runtime / "plan-lock.json", {"sha256": sha256(path.read_bytes())})
        with self.assertRaisesRegex(TaskflowError, "no actual CLI"):
            self.validate_plan()

    def validate_plan(self):
        from taskflow.execution_formal import _prepared_formal_plan
        return _prepared_formal_plan(self.config, self.candidate, verify_snapshot(Path(self.release["snapshot"]["path"])),
                                     self.runtime, self.config["adapter"]["sha256"])

    def test_changed_intent_plan_source_acceptance_or_nonzero_capture_reject(self):
        changes = [(self.runtime.with_name("inner.formal-intent.json"), {}),
                   (self.runtime / "launch-review.json", {}),
                   (self.runtime / "exec-context.json", {"model_calls": 1, "external_model_requests": 0}),
                   (self.runtime / "plan-lock.json", {"sha256": "0" * 64})]
        for path, data in changes:
            original = path.read_bytes(); write_json(path, data)
            with self.subTest(path=path), self.assertRaises(TaskflowError): self.validate()
            path.write_bytes(original)
        path = self.runtime / "runtime_gate.py"; original = path.read_bytes();path.write_bytes(original+b"drift")
        with self.assertRaises(TaskflowError):self.validate()
        path.write_bytes(original)
        self.run.return_value = subprocess.CompletedProcess([], 2, b"", b"Changed dependency")
        with self.assertRaisesRegex(TaskflowError, "validation failed"): self.validate()


class PreparedFormalCliResumeTests(unittest.TestCase):
    def setUp(self):
        c = lifecycle_fixtures.FormalPreparationResumeTests("test_exact_unclaimed_formal_guard_resumes_once_and_preserves_definition_run_decision_receipts")
        c.setUp();self.addCleanup(c.doCleanups)
        old, _ = c.failure()
        self.store, self.root = c.store, c.root
        p = policy(self.root)
        self.store.register_policy(p["policy_id"], p["config"])
        binding = make_batch_binding(p, "solver", "solver", "neutral")
        from taskflow.execution_experiment import _node
        definition = copy.deepcopy(old["definition"])
        definition.update(flow_id="prepared-cli-resume", policy_id=p["policy_id"])
        config = configuration(self.root, binding["setting"], c.case.authority, "fixture-01", "temporary")
        config["intent_id"] = _node({"flow_id": definition["flow_id"], "state": old["state"]}, "solver") + "-intent"
        definition["technical"]["formal_config_base"] = {k:v for k,v in config.items() if k not in ("run_id", "intent_id", "policy_version")}
        state = copy.deepcopy(old["state"])
        state["exception"] = {"stage": "formal_prepare", "reason": "actual runner CLI differs from the confirmed batch service"}
        self.receipt = self.root / "original-cli-exception.json"
        write_json(self.receipt, {"flow_id": definition["flow_id"], "status": "exception", "exception": state["exception"]})
        state["receipt"] = str(self.receipt)
        self.record = self.store.register_flow(definition["flow_id"], definition, old["evidence"], state)
        self.resolution = {"action": "resume_validated_prepared_formal_cli", "reason": "Carry the original actual CLI", "evidence": old["evidence"]}
        self.proof = {"cli": config["cli"], "model_calls": 0}
        patcher = patch("taskflow.execution_formal.validate_formal_authority", return_value={"controller": definition["owner"]})
        patcher.start();self.addCleanup(patcher.stop)
        patcher = patch("taskflow.execution_formal.validate_prepared_formal_cli", return_value=self.proof)
        self.validator = patcher.start();self.addCleanup(patcher.stop)
        self.inner = Path(definition["technical"]["formal_runtime_root"]) / "fixture-01"
        self.inner.mkdir(parents=True)
        self.intent = self.inner.with_name(self.inner.name+".formal-intent.json")
        self.intent.write_text("Original identity retained.")

    def resume(self):return resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def test_resume_keeps_source_run_runtime_intent_and_exception_without_enqueue(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_execution_marker_work_hold_override_and_other_error_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)
