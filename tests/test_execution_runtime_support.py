"""Explicit support refresh preserves model choices and frozen historical bytes."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_batch import confirmation_digests, freeze_batch_plan, validate_batch_policy
from taskflow.execution_batch_factory import compile_batch_configuration
from taskflow.execution_runtime_support import original_support, validate_support_refresh
from taskflow.execution_solver_profile import validated_profile
from taskflow.execution_store import evidence_ref
from tests import test_execution_solver_profile as fixture
from tests.test_execution_batch import file_ref
from tools.build_formal_profile_adapter import build_formal_profile_adapter


class RuntimeSupportTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.SolverProfileTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        c = self.case
        self.profile = read_json(c.packet)
        plan = read_json(c.root / "updated-plan.json")
        cli = Path(plan["roles"]["solver"]["runtime"]["cli"])
        cli.write_text("# Neutral upgraded CLI fixture, never executed.\n")
        capture = c.root / "current-host.json"
        write_json(capture, {"model_calls": 0, "exit_code": 0,
            "cli": {key: file_ref(cli)[key] for key in ("path", "sha256", "size")},
            "argv": [str(cli), "catalog"]})
        self.capture_ref = evidence_ref(capture, "zero-model current host observation")
        for key, row in plan["roles"].items():
            runtime = row.get("runtime") or {}
            if runtime.get("carrier") != "cli":
                continue
            proof = read_json(Path(runtime["support_evidence"]["path"]))
            proof.update(cli=file_ref(cli), catalog_capture=self.capture_ref)
            path = c.root / (key + "-current-support.json")
            write_json(path, proof)
            runtime["support_evidence"] = file_ref(path)
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        plan_path = c.root / "current-plan.json"
        write_json(plan_path, plan)
        frozen = freeze_batch_plan(plan_path, c.root / "current/confirmation")
        adapter = build_formal_profile_adapter(adapter_ref=c.original_adapter,
            snapshot_ref=frozen["snapshot"], destination=c.root / "current-adapter")
        infra = read_json(c.root / "updated-infrastructure.json")
        infra["policy_id"] = "current-host-policy"
        infra["experiment_factory"]["technical"]["formal_config_base"]["adapter"] = adapter
        ip = c.root / "current-infrastructure.json"
        write_json(ip, infra)
        current = compile_batch_configuration(plan_path, ip, c.root / "current")
        self.profile.update(target_policy_record=current["policy"], target_factory=current["experiment_factory"])
        self.old = read_json(Path(self.profile["source_policy_record"]["path"]))["arguments"]
        self.new = read_json(Path(self.profile["target_policy_record"]["path"]))["arguments"]
        self.before = validate_batch_policy(self.old["config"])
        self.after = validate_batch_policy(self.new["config"])
        roles = [key for key, row in self.before["roles"].items() if
                 (row.get("runtime") or {}).get("support_evidence") !=
                 (self.after["roles"][key].get("runtime") or {}).get("support_evidence")]
        self.refresh = {"schema_version": 1, "kind": "same_catalog_runtime_support_refresh_v1",
            "source_policy_record": self.profile["source_policy_record"],
            "target_policy_record": self.profile["target_policy_record"],
            "role_keys": sorted(roles), "observations": [self.capture_ref],
            "reason": "Exact same catalog, explicit current host proof replacement."}
        self.refresh_path = c.root / "host-refresh.json"
        self.publish()

    def publish(self):
        write_json(self.refresh_path, self.refresh)
        self.profile["runtime_support_refresh"] = evidence_ref(self.refresh_path, "explicit host support refresh")
        path = self.case.root / "current-profile.json"
        write_json(path, self.profile)
        return evidence_ref(path, "solver update plus exact host metadata refresh")

    def validate(self):
        return validate_support_refresh(self.profile, self.old, self.new, self.before, self.after)

    def test_full_profile_accepts_exact_current_host_refresh_and_preserves_old_snapshot(self):
        old = Path(self.old["config"]["batch_snapshot"]["path"]).read_bytes()
        result = validated_profile(self.publish())
        self.assertEqual("current-host-policy", result["new"]["policy_id"])
        self.assertEqual("minimal", result["selected"]["reasoning_effort"])
        self.assertEqual(old, Path(self.old["config"]["batch_snapshot"]["path"]).read_bytes())
        for key in self.before["roles"]:
            if key == "solver":
                continue
            row = copy.deepcopy(self.after["roles"][key])
            row["runtime"] = original_support(row["runtime"], self.before["roles"][key]["runtime"], self.validate())
            self.assertEqual(self.before["roles"][key], row)

    def test_profile_keeps_strict_default_without_explicit_refresh(self):
        self.profile.pop("runtime_support_refresh")
        path = self.case.root / "unreceived-profile.json"
        write_json(path, self.profile)
        with self.assertRaisesRegex(TaskflowError, "non-solver role configuration"):
            validated_profile(evidence_ref(path, "no host refresh reception"))
        self.assertEqual([], validate_support_refresh({}, self.old, self.new, self.before, self.after))

    def test_refresh_requires_exact_roles_policies_and_host_observations(self):
        old = copy.deepcopy(self.refresh)
        for change in ({"schema_version": True}, {"role_keys": []}, {"observations": []},
                       {"reason": ""}, {"source_policy_record": self.profile["target_policy_record"]}):
            self.refresh = {**old, **change}
            self.publish()
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.validate()

    def test_catalog_change_cli_drift_and_nonzero_inference_observation_reject(self):
        import taskflow.execution_runtime_support as module
        real_frozen = module._frozen
        def altered(reference, snapshot, item):
            proof = real_frozen(reference, snapshot, item)
            if snapshot is self.after:
                proof["output"]["sha256"] = "f"*64
            return proof
        with patch.object(module, "_frozen", side_effect=altered), self.assertRaisesRegex(TaskflowError, "catalog bytes"):
            self.validate()
        cli = Path(self.after["roles"]["solver"]["runtime"]["cli"])
        cli.write_text("# Later unobserved host change")
        with self.assertRaisesRegex(TaskflowError, "current CLI"):
            self.validate()

    def test_observed_model_calls_must_be_zero_and_match_original_command(self):
        import taskflow.execution_runtime_support as module
        read = module._read
        for change in ({"model_calls": 1}, {"model_calls": False}, {"exit_code": 1}, {"argv": ["different"]}):
            def observed(path):
                value = read(path)
                return {**value, **change} if path == Path(self.capture_ref["path"]) else value
            with self.subTest(change=change), patch.object(module, "_read", side_effect=observed), self.assertRaisesRegex(TaskflowError, "zero-model"):
                self.validate()

    def test_provider_native_carrier_or_cli_changes_are_not_support_refresh(self):
        for change in ({"provider": "foreign"}, {"carrier": "chat"}, {"cli": "/different"},
                       {"native_provider": {"new": True}}):
            after = copy.deepcopy(self.after)
            after["roles"]["author"]["runtime"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "carrier/provider/CLI"):
                validate_support_refresh(self.profile, self.old, self.new, self.before, after)

    def test_normalization_never_hides_model_effort_or_permission_changes(self):
        pairs = self.validate()
        a = self.before["roles"]["author"]["runtime"]
        b = {**self.after["roles"]["author"]["runtime"], "reasoning_effort": "minimal", "extra_tool": True}
        normalized = original_support(b, a, pairs)
        self.assertEqual("minimal", normalized["reasoning_effort"])
        self.assertTrue(normalized["extra_tool"])
        self.assertNotEqual(a, normalized)
        self.assertIsNone(original_support(None, None, pairs))

    def test_changed_frozen_support_is_rejected(self):
        ref = self.after["roles"]["author"]["runtime"]["support_evidence"]
        entry = next(row for row in self.after["evidence_copies"] if row["original"]["path"] == ref["path"])
        path = Path(self.new["config"]["batch_snapshot"]["path"]).parent / entry["relative_path"]
        path.write_text("{}")
        with self.assertRaisesRegex(TaskflowError, "frozen evidence changed"):
            self.validate()
