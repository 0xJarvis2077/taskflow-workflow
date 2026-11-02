"""Neutral preparation receipts exercise a read-only recovery validator."""

import json
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_formal import validate_formal_preparation_recovery
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_formal as formal_fixtures


class FormalPreparationRecoveryTests(unittest.TestCase):
    def setUp(self):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.root, self.candidate = case.root, case.candidate
        self.release = case.release()
        self.runtime = self.root / "formal/neutral-01"
        self.runtime.mkdir(parents=True)
        self.incident = self.root / "incident"
        self.incident.mkdir()
        adapter = self.root / "approved-fixture-adapter"
        adapter.mkdir()
        # A neutral approved read-only gate, rather than executing host probes.
        # The real unchanged adapter/probe proof is separately integration-tested.
        gate = """import json
from pathlib import Path
def validate(plan, require_review=False):
 root=Path(__file__).parent
 if json.loads((root/'execution-plan.json').read_text())!=plan:
  raise ValueError('fixture plan drift')
 if require_review and json.loads((root/'launch-review.json').read_text()).get('approved') is not True:
  raise ValueError('fixture approval missing')
 return (root/'request.md').read_bytes(), None
"""
        for name in ("prepare_diagnostic.py", "execute_diagnostic.py", "file_evidence.py"):
            (adapter / name).write_text("# Neutral approved source fixture; no process/model.\n", encoding="utf-8")
        (adapter / "runtime_gate.py").write_text(gate, encoding="utf-8")
        write_json(adapter / "baseline-plan.json", {"nature": "neutral zero-model baseline fixture"})
        self.request = self.root / "neutral-request.md"
        self.request.write_text("Read only the neutral public prompt and deliver the requested result.\n", encoding="utf-8")
        authority = self.root / "authority.json"
        write_json(authority, {"controller": "fixture-controller", "task_id": "neutral", "formal": {
            "first_run_id": "neutral-01", "model": "gpt-5.6-terra", "timeout_seconds": 3600,
            "baseline": {"sha256": _info(adapter / "baseline-plan.json")["sha256"]}}})
        self.config = {"model": "gpt-5.6-terra", "timeout_seconds": 3600, "run_id": "neutral-01", "intent_id": "neutral-first-intent",
            "policy_version": "neutral-policy", "launcher_python": str(Path(sys.executable).resolve()), "deny_read_probes": [],
            "adapter": {"path": str(adapter / "prepare_diagnostic.py"), **_info(adapter / "prepare_diagnostic.py"),
                        "files": {name: _info(adapter / name)["sha256"] for name in
                                  ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")}},
            "request": self.ref(self.request), "authorization": self.ref(authority)}
        self.binding = {"config_digest": sha256(canonical(self.config)), "release_receipt": self.release["receipt"],
                        "candidate_binding": self.release["candidate_binding"], "runtime": str(self.runtime)}
        self.intent = self.runtime.with_name(self.runtime.name + ".formal-intent.json")
        write_json(self.intent, self.binding)
        token = self.runtime.with_name(self.runtime.name + ".host-preparation-recovery.json")
        write_json(token, {"schema_version": 1, "kind": "exclusive_zero_inference_preparation_recovery", "binding": self.binding,
                           "run_id": "neutral-01", "runtime": str(self.runtime), "model_calls": 0})
        self.retained = self.runtime.with_name(self.runtime.name + ".failed-preparation-" + _info(token)["sha256"][:12])
        self.archive = self.incident / "previous-runtime"
        for source in adapter.iterdir():
            shutil.copyfile(source, self.runtime / source.name)
        shutil.copyfile(self.request, self.runtime / "request.md")
        source_names = ["baseline-plan.json", "runtime_gate.py", "prepare_diagnostic.py", "execute_diagnostic.py", "file_evidence.py"]
        write_json(self.runtime / "source-lock.json", {"no_repository_dynamic_imports": True,
            "files": [{"path": str(self.runtime / name), **_info(self.runtime / name)} for name in source_names]})
        argv = [self.config["launcher_python"], str(self.runtime / "prepare_diagnostic.py"), "--solver", self.release["solver"]["path"],
                "--evaluation-directory", str(Path(self.release["snapshot"]["path"]) / "draft/evaluation"), "--run-id", "neutral-01",
                "--runtime-directory", str(self.runtime), "--task-directory", self.release["publication_root"], "--in-runtime"]
        for reference in self.release["quality_evidence"]:
            argv += ["--quality-evidence", reference["path"]]
        failure = "[Errno 1] Operation not permitted"
        write_json(self.runtime / "preparation-failure.json", {"nature": "preparation_failure_no_inference", "run_id": "neutral-01",
                   "type": "PermissionError", "message": failure, "model_calls": 0})
        write_json(self.runtime / "preparation-process.json", {"argv": argv, "exit_code": 1, "stdout": "",
                   "stderr": "listener.bind(address)\nPermissionError: " + failure, "model_calls": 0})
        write_json(self.runtime / "shell-probes.json", {"nature": "no_inference_existing_target_boundary_probes", "model_calls": 0,
                   "passed": False, "tcp": {"listener_created": False}, "exception": {"type": "PermissionError", "message": failure}})
        shutil.copytree(self.runtime, self.archive)
        shutil.copytree(self.runtime, self.retained)
        manifest = self.incident / "previous-runtime.manifest.json"
        write_json(manifest, {"files": {path.name: _info(path) for path in self.archive.iterdir()}})
        work = self.root / "new-public-context/solver"
        shutil.copytree(self.release["solver"]["path"], work)
        node = self.root / "dependencies/node/bin/node"
        node.parent.mkdir(parents=True)
        node.write_text("Neutral dependency byte fixture.\n", encoding="utf-8")
        self.node = node
        snapshot = read_json(Path(self.release["snapshot"]["path"]) / "manifest.json")
        frozen = {"snapshot_digest": snapshot["snapshot_digest"], "taskflow_input_digest": self.candidate["input_digest"]}
        reservation = Path(self.release["publication_root"]) / "reports/runtime-reservations/neutral-01.json"
        reservation.parent.mkdir(parents=True)
        write_json(reservation, {"run_id": "neutral-01", "runtime_directory": str(self.runtime), "frozen_binding": frozen,
                                "nature": "preparation_only_zero_inference_not_a_model_attempt"})
        self.plan = {"run_id": "neutral-01", "task_id": "neutral", "requested_model": "gpt-5.6-terra", "timeout_seconds": 3600,
            "input_digest": self.candidate["input_digest"], "request_sha256": _info(self.request)["sha256"],
            "launcher_python": self.config["launcher_python"], "run_reservation": str(reservation), "run_reservation_sha256": _info(reservation)["sha256"],
            "baseline_plan_sha256": self.config["adapter"]["files"]["baseline-plan.json"], "source_solver": self.release["solver"]["path"],
            "private_directory": str(Path(self.release["snapshot"]["path"]) / "draft/evaluation"), "frozen_binding": frozen, "cwd": str(work),
            "argv": ["neutral-cli", 'shell_environment_policy.set={"PATH"=' + json.dumps(str(node.parent)) + '}', "exec"]}
        write_json(self.runtime / "execution-plan.json", self.plan)
        plan_ref = self.ref(self.runtime / "execution-plan.json")
        write_json(self.runtime / "plan-lock.json", {"sha256": plan_ref["sha256"]})
        public = next(row for row in read_json(work / "package.json")["files"] if row["role"] == "prompt")
        self.normal_path = self.runtime / "normal-tools-probe.json"
        write_json(self.normal_path, {"binding": {"plan_sha256": plan_ref["sha256"], "node": {"path": str(node), **_info(node)}, "public_sha256": public["sha256"]},
            "returncode": 0, "model_calls": 0, "external_model_requests": 0,
            "observed": {"openssl_conf": None, "private_denied": True, "deny_code": "EACCES", "public_sha256": public["sha256"],
                         "own_write": True, "fork_ipc": True, "sqlite": True}})
        self.acceptance_path = self.runtime / "launch-review.json"
        write_json(self.acceptance_path, {"approved": True, "plan_sha256": plan_ref["sha256"], "input_digest": self.candidate["input_digest"],
            "preparation_binding": self.binding, "authorization": self.config["authorization"], "normal_tools": self.ref(self.normal_path)})
        process = self.incident / "recovery-process.json"
        write_json(process, {"argv": argv, "returncode": 0, "stdout": "Neutral zero-inference preparation complete.", "stderr": "", "model_calls": 0})
        self.proof_path = self.incident / "completed-recovery.json"
        self.proof = {"schema_version": 1, "kind": "validated_zero_inference_host_preparation_recovery", "binding": self.binding,
            "run_id": "neutral-01", "runtime": str(self.runtime), "intent": self.ref(self.intent), "reservation": self.ref(reservation),
            "previous_runtime": {"path": str(self.archive), "manifest": self.ref(manifest), "retained_path": str(self.retained)},
            "recovery_token": self.ref(token), "recovery_process": self.ref(process), "plan": plan_ref,
            "acceptance": self.ref(self.acceptance_path), "completed": True, "model_calls": 0}
        self.publish()

    @staticmethod
    def ref(path):
        return evidence_ref(path, "neutral recovery fixture evidence")

    def publish(self):
        write_json(self.proof_path, self.proof)

    def validate(self):
        return validate_formal_preparation_recovery(config=self.config, release=self.release, candidate=self.candidate,
            runtime=self.runtime, recovery=self.ref(self.proof_path))

    def test_completed_same_run_recovery_is_read_only_and_keeps_both_original_trees(self):
        before = {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        with patch("taskflow.execution_formal.subprocess.run", side_effect=AssertionError("readonly validator spawned a process")) as process:
            result = self.validate()
        process.assert_not_called()
        self.assertEqual(self.binding, result["binding"])
        self.assertEqual("neutral-01", result["run_id"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(self.proof["previous_runtime"], result["previous_runtime"])
        self.assertEqual(before, {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()})

    def test_original_archive_reservation_intent_and_plan_drift_are_rejected(self):
        for path in (self.archive / "request.md", self.retained / "preparation-process.json", self.intent,
                     Path(self.proof["reservation"]["path"]), self.runtime / "execution-plan.json"):
            old = path.read_bytes()
            with self.subTest(path=path.name):
                path.write_bytes(old + b"\n")
                with self.assertRaises(TaskflowError):
                    self.validate()
                path.write_bytes(old)
        extra = self.archive / "unexpected.json"
        extra.write_text("{}")
        with self.assertRaisesRegex(TaskflowError, "membership or bytes"):
            self.validate()

    def test_nonzero_or_executed_recovery_and_foreign_bound_native_binary_are_rejected(self):
        for value in (1, None, False):
            with self.subTest(model_calls=value):
                self.proof["model_calls"] = value
                self.publish()
                with self.assertRaisesRegex(TaskflowError, "zero-inference preparation"):
                    self.validate()
        self.proof["model_calls"] = 0
        self.publish()
        process_path = Path(self.proof["recovery_process"]["path"])
        original_process = read_json(process_path)
        for field in ("model_calls", "returncode"):
            with self.subTest(process_field=field):
                write_json(process_path, {**original_process, field: 1})
                self.proof["recovery_process"] = self.ref(process_path)
                self.publish()
                with self.assertRaisesRegex(TaskflowError, "same zero-inference preparation"):
                    self.validate()
        write_json(process_path, original_process)
        self.proof["recovery_process"] = self.ref(process_path)
        self.publish()
        claim = self.runtime / "execution-claim.json"
        claim.write_text("{}")
        with self.assertRaisesRegex(TaskflowError, "executed or uncertain"):
            self.validate()
        claim.unlink()
        foreign = self.root / "foreign-node"
        foreign.write_bytes(self.node.read_bytes())
        normal = read_json(self.normal_path)
        normal["binding"]["node"] = {"path": str(foreign), **_info(foreign)}
        write_json(self.normal_path, normal)
        accepted = read_json(self.acceptance_path)
        accepted["normal_tools"] = self.ref(self.normal_path)
        write_json(self.acceptance_path, accepted)
        self.proof["acceptance"] = self.ref(self.acceptance_path)
        self.publish()
        with self.assertRaisesRegex(TaskflowError, "normal-tools runtime bytes"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
