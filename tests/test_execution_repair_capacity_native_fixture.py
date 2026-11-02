"""Neutral fake native CLI and confirmed batch; no model or real permission proof.

The permission probe is deliberately substituted. CLI invocation, batch
binding, worker, terminal collection and full archival are actual local IO.
"""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_batch import CHAT_ROLES, CLI_ROLES, ROLE_KEYS, confirmation_digests, freeze_batch_plan, load_batch_snapshot, role_config
from taskflow.execution_batch_binding import validate_batch_source
from taskflow.execution_flow import _binding, _identity
from taskflow.execution_public_continuation import _actual
from taskflow.execution_repair_capacity import CAPACITY, _validate_capacity_trace, _validate_full_archive, _validate_retry_manifest
from taskflow.execution_runner import _info, _publish, archive_full_attempt
from taskflow.execution_store import ExecutionStore
from tests.test_execution_batch import file_ref, make_confirmed_plan
from tools.execution_simulation import fixture_policy
from tests.helpers import make_task


class NeutralNativeBatchCapacityTests(unittest.TestCase):
    def test_confirmed_batch_native_fake_cli_failure_and_original_archive(self):
        with tempfile.TemporaryDirectory(prefix="native-batch-capacity-", dir="/private/tmp") as directory:
            root = Path(directory).resolve()
            plan, plan_path = make_confirmed_plan(root / "batch", mode="real_cli")
            plan["task_ids"] = ["neutral"]
            cli = Path(plan["roles"]["repair"]["runtime"]["cli"])
            catalog_path = root / "batch/original-catalog.json"
            catalog = read_json(catalog_path)
            for item in catalog["models"]:
                item["slug"] = item["id"]
                item["supported_reasoning_levels"] = [{"effort": "minimal"}, {"effort": "deep"}]
            write_json(catalog_path, catalog)
            code = '''#!%s
import json,sys,uuid
catalog=%r
if '--version' in sys.argv:
 print('neutral fixture CLI 1');raise SystemExit(0)
if 'models' in sys.argv or 'catalog' in sys.argv:
 print(catalog);raise SystemExit(0)
sys.stdin.read()
print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
print(json.dumps({'type':'error','message':%r}),flush=True)
print(json.dumps({'type':'turn.failed','error':{'message':%r}}),flush=True)
raise SystemExit(1)
''' % (str(Path(sys.executable).resolve()), json.dumps(catalog), CAPACITY, CAPACITY)
            cli.write_text(code, encoding="utf-8")
            cli.chmod(0o755)
            for role in CLI_ROLES:
                runtime = plan["roles"][role]["runtime"]
                runtime["reasoning_context"] = None
                support_path = Path(runtime["support_evidence"]["path"])
                support = read_json(support_path)
                support["cli"] = file_ref(cli)
                support["output"] = file_ref(catalog_path)
                write_json(support_path, support)
                runtime["support_evidence"] = file_ref(support_path)
            confirmed = confirmation_digests(plan)
            for role in CHAT_ROLES:
                binding = plan["role_bindings"][role]
                evidence_path = Path(binding["configuration_evidence"]["path"])
                data = read_json(evidence_path)
                data["configuration"]["scope"] = confirmed["scope"]
                write_json(evidence_path, data)
                binding["configuration_evidence"] = file_ref(evidence_path)
            plan["user_model_confirmation"].update(confirmed)
            write_json(plan_path, plan)
            frozen = freeze_batch_plan(plan_path, root / "snapshot")
            snapshot = load_batch_snapshot(frozen["snapshot"])
            config = {**fixture_policy(), "model_confirmation_required": True, "batch_snapshot": frozen["snapshot"],
                      "model_slots": {key: role_config(snapshot, key) for key in ROLE_KEYS if key in CLI_ROLES}}
            store = ExecutionStore(root / "state.sqlite3")
            store.register_policy("native-batch", config)
            task = make_task(root)
            candidate = execution.capture_candidate(store, task, "candidate")
            flow_root = root / "flow"
            old_node = flow_root / "nodes/1-repair"
            work, scratch, runtime = old_node / "work", old_node / "scratch", old_node / "runtime"
            work.mkdir(parents=True); scratch.mkdir()
            old_id = _identity("flow", "1-repair")
            context = {**_binding(candidate), "flow_id": "flow", "node_id": old_id, "intent_id": old_id,
                       "runtime": str(runtime), "work": str(work), "scratch": str(scratch),
                       "inputs_root": str(runtime / "inputs"),
                       "revised_candidate_id": _identity("flow", old_id + "-candidate"),
                       "candidate_root": str(task), "defect_ids": ["D1"], "defects": [{"defect_id": "D1"}]}
            write_json(old_node / "current.json", context)
            readonly = root / "readonly.md"
            readonly.write_text("neutral read only source")
            setting = role_config(snapshot, "repair")
            runner = {key: setting[key] for key in ("cli", "model", "reasoning_effort", "reasoning_selection", "reasoning_context", "timeout_seconds")}
            runner.update({"mode": "native_role", "role": "repair", "model_slot": "repair", "intent_id": old_id,
                "policy_version": "native-batch", "cwd": str(work),
                "inputs": [{"path": str(old_node / "current.json"), "kind": "source_metadata", "target": "execution/current.json"},
                           {"path": str(readonly), "kind": "teacher", "target": "draft/evaluation/reference.md"}],
                "outputs": ["required.txt"],
                "request_sections": [{"kind": "operation", "text": "Neutral fixture at " + str(runtime) + "; no model."}],
                "native_permissions": {"profile": "neutral", "repository": str(root), "scratch": str(scratch),
                    "python": str(Path(sys.executable).resolve()), "probe_sources": [str(readonly)], "readonly_probe": str(readonly),
                    "filesystem": {":root": "deny", ":minimal": "read", str(root): "read", str(work): "write", str(scratch): "write"}}})
            spec = {"work": {"work_id": old_id, "task_id": "neutral", "candidate_id": "candidate", "role": "repair",
                              "model_slot": "repair", "policy_id": "native-batch", "intent_id": old_id, "purpose": "neutral simulation"},
                    "runner": runner}
            def simulated_probe(path):
                bound = _info(path / "manifest.json")
                report = {"passed": True, "model_calls": 0, "manifest": bound, "scope": "simulated permission probe"}
                _publish(path / "neutral-probe.json", report)
                _publish(path / "native-probe-ready.json", {"manifest": bound, "report_file": "neutral-probe.json", "report": _info(path / "neutral-probe.json")})
                return report
            with patch("taskflow.execution_runner.probe_native_attempt", side_effect=simulated_probe):
                execution.enqueue_spec(store, spec, runtime, probe_native=True)
            attempt = execution.run_worker(store, "neutral", allow_model=True, work_ids=[old_id])
            self.assertEqual("failed", attempt["state"])
            actual_root, manifest, collected = _actual(store, attempt)
            self.assertTrue(validate_batch_source(attempt, manifest, store))
            self.assertEqual("native_role", manifest["mode"])
            self.assertFalse(collected["model_completed"])
            _validate_capacity_trace((runtime / "trace.jsonl").read_bytes(), attempt["cli_sid"])
            archive = root / "archive"
            archive_full_attempt(runtime, archive)
            _validate_full_archive(archive, old_id, actual_root)
            new_node = flow_root / "nodes/2-repair"
            new_work, new_scratch, new_runtime = new_node / "work", new_node / "scratch", new_node / "runtime"
            new_work.mkdir(parents=True); new_scratch.mkdir()
            new_id = _identity("flow", "2-repair")
            changed = {"node_id": new_id, "intent_id": new_id, "runtime": str(new_runtime), "work": str(new_work),
                       "scratch": str(new_scratch), "inputs_root": str(new_runtime / "inputs"),
                       "revised_candidate_id": _identity("flow", new_id + "-candidate")}
            write_json(new_node / "current.json", {**context, **changed})
            new_runner = {**runner, "intent_id": new_id, "cwd": str(new_work),
                "inputs": [{"path": str(new_node / "current.json"), "kind": "source_metadata", "target": "execution/current.json"},
                           {"path": str(readonly), "kind": "teacher", "target": "draft/evaluation/reference.md"}],
                "request_sections": [{"kind": "operation", "text": "Neutral fixture at " + str(new_runtime) + "; no model."}],
                "native_permissions": {**runner["native_permissions"], "scratch": str(new_scratch),
                    "filesystem": {":root": "deny", ":minimal": "read", str(root): "read", str(new_work): "write", str(new_scratch): "write"}}}
            new_spec = {"work": {**spec["work"], "work_id": new_id, "intent_id": new_id}, "runner": new_runner}
            with patch("taskflow.execution_runner.probe_native_attempt", side_effect=simulated_probe):
                execution.enqueue_spec(store, new_spec, new_runtime, probe_native=True)
            new_manifest = read_json(new_runtime / "manifest.json")
            proof = {"failed_attempt_id": attempt["attempt_id"], "initial_step": 1,
                     "maximum_additional_attempts": 10, "candidate_binding": _binding(candidate)}
            _validate_retry_manifest(store, {"flow_id": "flow", "definition": {"directory": str(flow_root)}}, proof, new_manifest, 1)
            self.assertEqual([], store.list_attempts(new_id))  # The replacement is prepared only, never launched.
            foreign_node = flow_root / "nodes/3-repair"
            foreign_work, foreign_scratch, foreign_runtime = foreign_node / "work", foreign_node / "scratch", foreign_node / "runtime"
            foreign_work.mkdir(parents=True); foreign_scratch.mkdir()
            foreign_id = _identity("flow", "3-repair")
            foreign_changes = {"node_id": foreign_id, "intent_id": foreign_id, "runtime": str(foreign_runtime),
                "work": str(foreign_work), "scratch": str(foreign_scratch), "inputs_root": str(foreign_runtime / "inputs"),
                "revised_candidate_id": _identity("flow", foreign_id + "-candidate")}
            write_json(foreign_node / "current.json", {**context, **foreign_changes})
            foreign_runner = {**runner, "intent_id": foreign_id, "cwd": str(foreign_work),
                "inputs": [{"path": str(foreign_node / "current.json"), "kind": "source_metadata", "target": "execution/current.json"},
                           {"path": str(readonly), "kind": "teacher", "target": "draft/evaluation/reference.md"}],
                "request_sections": [{"kind": "operation", "text": "Changed responsibility at " + str(foreign_runtime)}],
                "native_permissions": {**runner["native_permissions"], "scratch": str(foreign_scratch),
                    "filesystem": {":root": "deny", ":minimal": "read", str(root): "read", str(foreign_work): "write", str(foreign_scratch): "write"}}}
            foreign_spec = {"work": {**spec["work"], "work_id": foreign_id, "intent_id": foreign_id}, "runner": foreign_runner}
            with patch("taskflow.execution_runner.probe_native_attempt", side_effect=simulated_probe):
                execution.enqueue_spec(store, foreign_spec, foreign_runtime, probe_native=True)
            with self.assertRaisesRegex(TaskflowError, "request"):
                _validate_retry_manifest(store, {"flow_id": "flow", "definition": {"directory": str(flow_root)}},
                                         proof, read_json(foreign_runtime / "manifest.json"), 2)
            self.assertEqual([], store.list_attempts(foreign_id))
