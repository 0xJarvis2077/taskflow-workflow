"""Focused batch binding boundaries; fixtures never call an external model.

Real local fixture subprocesses are used only for facts sealing. Synthetic
native/isolated manifests below test metadata and adapter readback, not native
permissions, real-provider support or a real model's identity.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_batch import CLI_ROLES, freeze_batch_plan, load_batch_snapshot, role_config
from taskflow.execution_batch_binding import (
    make_batch_binding, validate_batch_runner, validate_batch_source,
)
from taskflow.execution_runner import RunnerError, prepare_attempt, run_attempt, seal_facts
from tests import test_execution_batch_dispatch as fixtures
from tests.test_execution_batch import make_confirmed_plan


class BatchBindingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.BatchDispatchTests(
            "test_empty_template_cli_reports_all_missing_without_database")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.compiled, self.store, self.task = self.fixture.configured()
        self.policy = self.store.get_policy("neutral-batch-v1")
        self.candidate = self.store.get_candidate("seed")

    def config(self, name="binding", role="quality", slot="quality", **extra):
        spec = self.fixture.specification(self.store, "seed", role, slot, name,
                                          {"out.md": "neutral binding output"}, extra=extra)
        value = spec["runner"]
        value["batch_binding"] = make_batch_binding(self.policy, role, slot, "neutral-task")
        value["candidate_binding"] = {key: self.candidate[key] for key in
            ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        return value

    def test_direct_prepare_rejects_wrong_candidate_scope_before_any_runtime(self):
        config = self.config()
        config["candidate_binding"]["task_id"] = "foreign-task"
        runtime = self.root / "blocked-scope"
        with self.assertRaisesRegex(TaskflowError, "candidate task"):
            prepare_attempt(runtime, config)
        self.assertFalse(runtime.exists())
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_public_batch_check_rejects_nonobject_json_without_creating_a_database(self):
        path = self.root / "nonobject.json"
        for value in ([], None, "plain text"):
            write_json(path, value)
            with self.subTest(value=value):
                code, result, error = self.fixture.call("batch-check", "--plan", str(path))
                self.assertEqual(2, code)
                self.assertIsNone(result)
                self.assertIn("expected a JSON object", error)
                self.assertNotIn("Traceback", error)
                self.assertFalse((self.root / "never.sqlite3").exists())

    def test_direct_prepare_rejects_original_policy_slot_and_model_drift(self):
        original = self.config()
        edits = [
            ("policy", lambda v: v.update(policy_version="foreign-policy")),
            ("slot", lambda v: v.update(model_slot="recheck")),
            ("role", lambda v: v.update(role="repair")),
            ("model", lambda v: v.update(model="unselected")),
            ("effort", lambda v: v.update(reasoning_effort="unselected")),
            ("timeout", lambda v: v.update(timeout_seconds=11)),
            ("batch", lambda v: v["batch_binding"].update(batch_id="foreign")),
            ("policy_bytes", lambda v: v["batch_binding"]["policy_config"].update(concurrency=999)),
            ("setting", lambda v: v["batch_binding"]["setting"].update(model="unselected")),
        ]
        for name, edit in edits:
            value = copy.deepcopy(original); edit(value)
            runtime = self.root / ("blocked-" + name)
            with self.subTest(drift=name), self.assertRaises(TaskflowError):
                prepare_attempt(runtime, value)
            self.assertFalse(runtime.exists())
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_manifest_scope_policy_and_requested_values_are_revalidated(self):
        runtime = self.root / "prepared"
        manifest = prepare_attempt(runtime, self.config())
        self.assertIsNotNone(validate_batch_runner(manifest, manifest=True))
        originals = (runtime / "manifest.json").read_bytes()
        edits = [
            lambda m: m.update(role="solver"),
            lambda m: m.update(policy_version="foreign-policy"),
            lambda m: m["model"].update(requested="unselected"),
            lambda m: m["candidate_binding"].update(task_id="foreign-task"),
            lambda m: m.update(reasoning_context="foreign-context"),
        ]
        for index, edit in enumerate(edits):
            value = copy.deepcopy(manifest); edit(value)
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                validate_batch_runner(value, manifest=True)
        value = copy.deepcopy(manifest)
        value["batch_binding"]["policy_config"]["batch_snapshot"]["sha256"] = "0" * 64
        value["batch_binding"]["policy_digest"] = sha256(canonical(value["batch_binding"]["policy_config"]))
        with self.assertRaisesRegex(TaskflowError, "evidence bytes differ"):
            validate_batch_runner(value, manifest=True)
        self.assertEqual(originals, (runtime / "manifest.json").read_bytes())

    def test_snapshot_and_captured_evidence_drift_prevent_child_launch(self):
        runtime = self.root / "prepared"
        manifest = prepare_attempt(runtime, self.config())
        snapshot = self.compiled["snapshot"]
        data = load_batch_snapshot(snapshot)
        selected = [Path(snapshot["path"]), Path(snapshot["path"]).parent /
                    data["evidence_copies"][0]["relative_path"]]
        for source in selected:
            self.assertIn(str(source), manifest["dependencies"])
            raw = source.read_bytes()
            try:
                source.write_bytes(raw + b"drift")
                with self.subTest(source=source.name), \
                        patch("taskflow.execution_runner.subprocess.Popen") as child, \
                        self.assertRaises((RunnerError, TaskflowError)):
                    run_attempt(runtime)
                child.assert_not_called()
                self.assertFalse((runtime / "dispatch.json").exists())
                self.assertFalse((runtime / "process.json").exists())
            finally:
                source.write_bytes(raw)

    def test_same_reviewer_and_sid_do_not_allow_a_different_original_policy(self):
        facts = self.fixture.perform(self.store, "seed", "review-facts", "reviewer", "facts-original",
                                      {"facts.md": "Neutral sealed complete observations."})
        facts_root = Path(facts["runtime_path"])
        seal_facts(facts_root, facts_root / "artifacts/facts.md")
        config = self.config("score-binding", "review-score", "reviewer",
                             facts_attempt=str(facts_root), resume_sid=facts["cli_sid"])
        other = self.store.register_policy("same-model-new-policy", self.policy["config"])
        changed = copy.deepcopy(config)
        changed["policy_version"] = other["policy_id"]
        changed["batch_binding"] = make_batch_binding(other, "review-score", "reviewer", "neutral-task")
        self.assertEqual(config["model"], changed["model"])
        self.assertEqual(config["resume_sid"], changed["resume_sid"])
        with self.assertRaisesRegex(TaskflowError, "same original batch reviewer model and policy"):
            prepare_attempt(self.root / "blocked-new-policy", changed)
        self.assertFalse((self.root / "blocked-new-policy").exists())
        wrong_sid = copy.deepcopy(config); wrong_sid["resume_sid"] = "different-sid"
        with self.assertRaisesRegex(RunnerError, "same review SID"):
            prepare_attempt(self.root / "blocked-new-sid", wrong_sid)
        # The original policy/model/SID remains a valid preparation.
        actual = prepare_attempt(self.root / "valid-original-score", config)
        self.assertEqual(facts["cli_sid"], actual["resume_sid"])
        self.assertEqual("reviewer", actual["model_slot"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def native_metadata(self, *, context=None):
        # These are neutral input records, not an executed native model probe.
        plan, path = make_confirmed_plan(self.root / "native-metadata", mode="real_cli")
        for key in CLI_ROLES:
            plan["roles"][key]["runtime"]["reasoning_context"] = context
        from taskflow.execution_batch import confirmation_digests
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        write_json(path, plan)
        frozen = freeze_batch_plan(path, self.root / "native-frozen")
        snapshot = load_batch_snapshot(frozen["snapshot"])
        config = {"concurrency": 1, "model_confirmation_required": True, "batch_snapshot": frozen["snapshot"],
                  "model_slots": {key: role_config(snapshot, key) for key in CLI_ROLES},
                  "roles": {"quality": role_config(snapshot, "quality")}}
        policy = {"policy_id": "neutral-native-metadata", "config": config, "digest": sha256(canonical(config))}
        binding = make_batch_binding(policy, "quality", "quality", "neutral-task")
        value = {**binding["setting"], "batch_binding": binding, "role": "quality",
                 "model_slot": "quality", "policy_version": policy["policy_id"],
                 "mode": "native_role", "candidate_binding": self.config("native-candidate")["candidate_binding"]}
        return policy, value

    def test_native_and_public_adapter_reasoning_and_cli_mismatch_fail_before_execution(self):
        _, value = self.native_metadata(context="all_turns")
        for mode in ("native_role", "existing_isolated"):
            changed = copy.deepcopy(value); changed["mode"] = mode
            with self.subTest(mode=mode), self.assertRaisesRegex(TaskflowError, "unsupported"):
                prepare_attempt(self.root / ("blocked-" + mode), changed)
            self.assertFalse((self.root / ("blocked-" + mode)).exists())

    def test_native_cli_and_manifest_adapter_readback_use_confirmed_executable(self):
        policy, value = self.native_metadata()
        self.assertIsNotNone(validate_batch_runner(value))
        value["cli"] = str(self.root / "different-cli")
        with self.assertRaisesRegex(TaskflowError, "CLI differs"):
            prepare_attempt(self.root / "blocked-cli", value)
        self.assertFalse((self.root / "blocked-cli").exists())
        binding = make_batch_binding(policy, "solver", "solver", "neutral-task")
        inner = self.root / "synthetic-inner"; inner.mkdir()
        write_json(inner / "execution-plan.json", {"argv": [binding["setting"]["cli"], "exec"]})
        manifest = {**binding["setting"], "batch_binding": binding, "role": "solver",
                    "model_slot": "solver", "policy_version": policy["policy_id"],
                    "model": {"requested": binding["setting"]["model"],
                              "reasoning_effort": binding["setting"]["reasoning_effort"]},
                    "candidate_binding": value["candidate_binding"], "mode": "existing_isolated",
                    "isolated": {"flavor": "formal", "runtime": str(inner)}}
        self.assertIsNotNone(validate_batch_runner(manifest, manifest=True))
        write_json(inner / "execution-plan.json", {"argv": [str(self.root / "foreign-cli")]})
        with self.assertRaisesRegex(TaskflowError, "CLI differs"):
            validate_batch_runner(manifest, manifest=True)

    def test_source_requires_original_policy_slot_and_registered_bytes(self):
        attempt = self.fixture.perform(self.store, "seed", "quality", "quality", "actual-neutral-source",
                                       {"review.md": "Neutral bound original source."})
        manifest = read_json(Path(attempt["runtime_path"]) / "manifest.json")
        self.assertTrue(validate_batch_source(attempt, manifest, self.store))
        for field in ("model_slot", "policy_id", "policy_digest", "task_id"):
            value = copy.deepcopy(attempt); value[field] = "foreign"
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "source attempt"):
                validate_batch_source(value, manifest, self.store)
        with patch.object(self.store, "get_policy", return_value={
                **self.policy, "digest": "0" * 64}), self.assertRaisesRegex(TaskflowError, "registered"):
            validate_batch_source(attempt, manifest, self.store)
        self.assertFalse(validate_batch_source({}, {"role": "quality"}))
        with self.assertRaisesRegex(TaskflowError, "lost"):
            validate_batch_source({"model_slot": "quality"}, {"role": "quality"})


if __name__ == "__main__":
    unittest.main()

