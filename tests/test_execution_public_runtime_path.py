"""Isolated public inner paths stay external; no model is invoked."""

import copy
import os
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_flow import _prepare_node, _public_inner_runtime, _public_prepare
from taskflow.packages import check_task
from tests import test_execution_flow as flow_fixtures
from tests import test_isolated_quality as isolated_fixtures
from tools import isolated_quality


class PublicInnerRuntimeTests(unittest.TestCase):
    def setUp(self):
        case = isolated_fixtures.IsolatedQualityTests("test_prepare_copies_only_bound_public_bytes_and_has_no_inference")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        adapter = Path(__file__).resolve().parents[1] / "tools/isolated_quality.py"
        self.role = {"runner": {"model": "neutral-model", "reasoning_effort": "high", "timeout_seconds": 99},
            "prepare": {"adapter": str(adapter), "sha256": sha256(adapter.read_bytes()),
                        "launcher_python": str(Path(sys.executable).resolve()), "config": case.config, "checks": []}}
        actual = check_task(case.task, write_report=False)
        self.context = {"runtime": str(case.repository / "very-long-workflow-node/public/runtime"),
            "candidate_root": str(case.task), "candidate_id": "neutral-candidate", "intent_id": "public-rebuild-intent",
            "private_paths": [], **{key: actual[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")}}

    def test_existing_adapter_prepares_short_external_inner_once_with_no_inference(self):
        case = self.case
        legacy = Path(self.context["runtime"] + ".public")
        with self.assertRaisesRegex(ValueError, "Runtime must be outside"):
            isolated_quality.prepare_draft(case.task, legacy, case.config)
        self.assertFalse(legacy.exists())
        inner = _public_inner_runtime(self.role, self.context)
        self.addCleanup(shutil.rmtree, inner, True)
        self.assertFalse(inner.is_relative_to(case.repository))
        self.assertLess(len(str(inner)), 80)
        original = copy.deepcopy(self.role)

        def zero_model(argv, **kwargs):
            return case.probe_ok(argv, **kwargs) if argv[1:2] == ["sandbox"] else case.cli_evidence(argv, **kwargs)

        with patch("subprocess.run", side_effect=zero_model), \
                patch("subprocess.Popen", side_effect=AssertionError("no model during preparation")), \
                patch.object(Path, "home", return_value=case.base / "home"), patch.dict(os.environ, {}, clear=True):
            first = _public_prepare(self.role, self.context)
            plan_bytes = (inner / "plan.json").read_bytes()
            repeated = _public_prepare(self.role, self.context)
        self.assertEqual(first, repeated)
        self.assertEqual(str(inner), first["runtime"])
        self.assertEqual(plan_bytes, (inner / "plan.json").read_bytes())
        self.assertEqual(original, self.role)
        self.assertFalse((inner / "work/draft/evaluation").exists())
        self.assertTrue((inner / "work/draft/input/prompt.md").exists())
        self.assertFalse(legacy.exists())

    def test_binding_changes_isolate_nodes_and_existing_inner_evidence_is_never_migrated(self):
        current = _public_inner_runtime(self.role, self.context)
        self.assertEqual(current, _public_inner_runtime(self.role, dict(self.context)))
        for key, value in (("intent_id", "other-public-intent"), ("input_digest", "0" * 64),
                           ("evaluation_digest", "1" * 64), ("candidate_id", "other-candidate")):
            with self.subTest(key=key):
                self.assertNotEqual(current, _public_inner_runtime(self.role, {**self.context, key: value}))
        external = {**self.context, "runtime": str(self.case.base / "already-external/runtime")}
        legacy = Path(external["runtime"] + ".public")
        legacy.mkdir(parents=True)
        proof = legacy / "original-plan.json"
        proof.write_bytes(b"Original already prepared external evidence")
        self.assertEqual(legacy, _public_inner_runtime(self.role, external))
        self.assertEqual(b"Original already prepared external evidence", proof.read_bytes())
        occupied = Path(self.context["runtime"] + ".public")
        occupied.mkdir(parents=True)
        receipt = occupied / "unknown-original-receipt.json"
        receipt.write_bytes(b"Preserve occupied inner instead of redrawing")
        with self.assertRaisesRegex(TaskflowError, "already has evidence"):
            _public_inner_runtime(self.role, self.context)
        self.assertEqual(b"Preserve occupied inner instead of redrawing", receipt.read_bytes())


class PublicNodeArgvTests(unittest.TestCase):
    def setUp(self):
        case = flow_fixtures.ContinuationTests("test_one_registration_runs_quality_actual_repair_recheck_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store

    def record(self, stage, isolated):
        definition = self.case.definition("neutral")
        candidate = execution.capture_candidate(self.store, Path(definition["initial"]["candidate_root"]), "neutral-current")
        role = copy.deepcopy(definition["roles"]["quality"])
        if isolated:
            role["runner"].update(mode="existing_isolated", argv=[str(Path(sys.executable).resolve()),
                "{{runtime}}.public/runner.py", "execute", "{{runtime}}.public"])
            role["prepare"] = {"launcher_python": str(Path(sys.executable).resolve()),
                               "config": {"repository": str(self.case.root)}}
        definition["roles"][stage] = role
        record = self.store.register_flow(definition["flow_id"], definition, self.case.refs,
            {"status": "active", "stage": stage, "step": 2, "candidate_id": candidate["candidate_id"],
             "candidate_root": definition["initial"]["candidate_root"], "defects": [], "repaired_defects": []})
        return record, role

    def test_isolated_public_rebuild_uses_actual_inner_argv_and_reuses_frozen_spec(self):
        record, role = self.record("public_rebuild", True)
        specs = []

        def prepare(actual_role, context):
            return {"runtime": str(_public_inner_runtime(actual_role, context)), "flavor": "quality",
                    "python": actual_role["prepare"]["launcher_python"]}

        with patch("taskflow.execution_flow._public_prepare", side_effect=prepare) as public, \
                patch("taskflow.execution.enqueue_spec", side_effect=lambda store, spec, *args, **kwargs: specs.append(copy.deepcopy(spec))):
            result = _prepare_node(self.store, record)
            path = Path(record["definition"]["directory"]) / "nodes/2-public_rebuild/spec.json"
            original = path.read_bytes()
            _prepare_node(self.store, result)
        self.assertEqual(1, public.call_count)
        self.assertEqual(2, len(specs))
        self.assertEqual(specs[0], specs[1])
        self.assertEqual(original, path.read_bytes())
        runner = specs[0]["runner"]
        inner = Path(runner["isolated"]["runtime"])
        self.assertFalse(inner.is_relative_to(self.case.root))
        self.assertEqual([role["prepare"]["launcher_python"], str(inner / "runner.py"), "execute", str(inner)], runner["argv"])
        self.assertEqual(str(inner / "work"), runner["cwd"])
        self.assertEqual("{{runtime}}.public/runner.py", role["runner"]["argv"][1])
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_native_recheck_public_does_not_use_isolated_preparer(self):
        record, role = self.record("recheck_public", False)
        specs = []
        with patch("taskflow.execution_flow._public_prepare", side_effect=AssertionError("native recheck has no public adapter")), \
                patch("taskflow.execution.enqueue_spec", side_effect=lambda store, spec, *args, **kwargs: specs.append(copy.deepcopy(spec))):
            _prepare_node(self.store, record)
        self.assertNotIn("isolated", specs[0]["runner"])
        self.assertEqual(role["runner"]["mode"], specs[0]["runner"]["mode"])
        self.assertEqual(role["runner"]["argv"][0], specs[0]["runner"]["argv"][0])
        self.assertFalse(any(".public/runner.py" in value for value in specs[0]["runner"]["argv"]))
        self.assertEqual([], self.store.dashboard()["attempts"])

