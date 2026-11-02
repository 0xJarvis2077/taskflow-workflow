"""Neutral batch contracts only: no external models or production execution DB."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution_batch import (
    CHAT_ROLES, CLI_ROLES, ROLE_KEYS, check_batch_plan, confirmation_digests,
    freeze_batch_plan, load_batch_snapshot, policy_setting, role_config,
    validate_batch_policy,
)


def write_json(path, value):
    path.write_bytes(canonical(value) + b"\n")


def file_ref(path, location="neutral structured evidence"):
    raw = path.read_bytes()
    return {"path": str(path), "sha256": sha256(raw), "size": len(raw), "location": location}


def make_confirmed_plan(root, *, mode="test_fixture", disabled=()):
    """Reusable machine fixture; its labels are not a real-provider support proof."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    cli = root / "neutral-cli.py"
    cli.write_text("# Neutral fake CLI for model-support evidence; never calls a model.\n", encoding="utf-8")
    models = {key: "other-provider/" + key for key in ROLE_KEYS}
    catalog = root / "original-catalog.json"
    write_json(catalog, {"models": [{"id": models[key], "efforts": ["minimal", "deep"],
                                    "contexts": ["all_turns"], "default_effort": "minimal"}
                                   for key in ROLE_KEYS if key in CLI_ROLES]})
    plan = {"schema_version": 1, "architecture_version": "high-value-task-v1",
            "batch_id": "neutral-batch", "task_ids": ["neutral-task"],
            "project_label": "A display label", "execution_mode": mode,
            "roles": {}, "role_bindings": {}, "naming": {"chat_pattern": "display only"}}
    model_index = 0
    for key in ROLE_KEYS:
        if key in disabled:
            plan["roles"][key] = {"enabled": False, "model": None, "runtime": None}
            continue
        runtime = {"carrier": "chat" if key in CHAT_ROLES else "cli",
                   "provider": "neutral-provider"}
        if key in CLI_ROLES:
            support = root / (key + "-support.json")
            write_json(support, {
                "schema_version": 1, "kind": "cli_model_support",
                "origin": "test_fixture" if mode == "test_fixture" else "original_cli_observation",
                "provider": "neutral-provider", "model": models[key], "supported": True,
                "cli": file_ref(cli), "argv": [str(cli), "catalog"],
                "returncode": 0, "output": file_ref(catalog),
                "model_pointer": "/models/" + str(model_index) + "/id",
                "efforts_pointer": "/models/" + str(model_index) + "/efforts",
                "contexts_pointer": "/models/" + str(model_index) + "/contexts",
            })
            runtime.update(cli=str(cli), support_evidence=file_ref(support),
                           reasoning_selection="explicit", reasoning_effort="deep",
                           reasoning_context="all_turns", timeout_seconds=10)
            model_index += 1
        plan["roles"][key] = {"enabled": True, "model": models[key], "runtime": runtime}
    # Disabled CLI slots still occur in the catalog; use their actual stable index.
    catalog_keys = [key for key in ROLE_KEYS if key in CLI_ROLES]
    for key in CLI_ROLES - set(disabled):
        path = root / (key + "-support.json")
        support = json.loads(path.read_bytes())
        index = catalog_keys.index(key)
        for field, suffix in (("model_pointer", "id"), ("efforts_pointer", "efforts"),
                              ("contexts_pointer", "contexts")):
            support[field] = "/models/" + str(index) + "/" + suffix
        write_json(path, support)
        plan["roles"][key]["runtime"]["support_evidence"] = file_ref(path)
    expected = confirmation_digests(plan)
    for key in CHAT_ROLES - set(disabled):
        config = root / (key + "-actual-chat.json")
        write_json(config, {"configuration": {
            "origin": "actual_chat_configuration", "role_key": key,
            "provider": "neutral-provider", "thread_id": "thread-" + key,
            "host": "local", "model": models[key], "scope": expected["scope"]}})
        plan["role_bindings"][key] = {
            "carrier": "chat", "thread_id": "thread-" + key, "host": "local",
            "actual_model": models[key], "configuration_evidence": file_ref(config),
            "configuration_pointer": "/configuration"}
    choice = root / "direct-user-choice.json"
    write_json(choice, {"actor": "user", "message_id": "neutral-message",
                        "text": "Use the separately specified models for this batch.",
                        "structured_selection": expected["expanded_model_map"]})
    plan["user_model_confirmation"] = {"origin": "direct_user", "source": file_ref(choice), **expected}
    path = root / "plan.json"
    write_json(path, plan)
    return plan, path


class BatchPlanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="u021-", dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def plan(self, **kwargs):
        return make_confirmed_plan(self.root, **kwargs)

    def check(self, plan, path):
        write_json(path, plan)
        return check_batch_plan(path)

    def frozen(self, **kwargs):
        plan, path = self.plan(**kwargs)
        frozen = freeze_batch_plan(path, self.root / "frozen")
        return plan, path, frozen

    def policy(self, frozen):
        snapshot = load_batch_snapshot(frozen["snapshot"])
        return {"concurrency": 3, "model_confirmation_required": True,
                "batch_snapshot": frozen["snapshot"],
                "roles": {"quality": {"model": "must-not-be-used"}},
                "model_slots": {key: role_config(snapshot, key) for key in ROLE_KEYS
                                if key in CLI_ROLES and snapshot["roles"][key]["enabled"]}}

    def test_empty_template_reports_all_models_without_preparing_anything(self):
        path = self.root / "empty.json"
        write_json(path, {"schema_version": 1, "architecture_version": "high-value-task-v1",
                         "batch_id": None, "task_ids": [], "user_model_confirmation": None,
                         "roles": {key: {"enabled": True, "model": None} for key in ROLE_KEYS}})
        before = sorted(self.root.iterdir())
        result = check_batch_plan(path)
        self.assertFalse(result["ready"])
        self.assertEqual(list(ROLE_KEYS), result["missing_models"])
        self.assertEqual("awaiting_user_models", result["status"])
        self.assertEqual(before, sorted(self.root.iterdir()))
        with self.assertRaisesRegex(TaskflowError, "blocked"):
            freeze_batch_plan(path, self.root / "forbidden")
        self.assertFalse((self.root / "forbidden").exists())

    def test_distinct_slots_freeze_and_same_reviewer_is_resolved_twice(self):
        plan, path, frozen = self.frozen()
        self.assertTrue(check_batch_plan(path)["ready"])
        self.assertEqual("neutral-batch", frozen["batch_id"])
        snapshot = load_batch_snapshot(frozen["snapshot"])
        self.assertEqual(plan["roles"]["quality"]["model"], role_config(snapshot, "quality")["model"])
        self.assertNotEqual(role_config(snapshot, "public_rebuild"), role_config(snapshot, "recheck"))
        policy = self.policy(frozen)
        self.assertEqual(policy_setting(policy, "review-facts", task_id="neutral-task"),
                         policy_setting(policy, "review-score", task_id="neutral-task"))
        self.assertEqual(role_config(snapshot, "author"), policy_setting(policy, "author"))
        self.assertEqual(frozen, freeze_batch_plan(path, self.root / "frozen"))

    def test_rename_is_presentation_only_not_confirmation_or_route(self):
        plan, path, frozen = self.frozen()
        expected = confirmation_digests(plan)
        plan["project_label"] = "Renamed batch label"
        plan["naming"] = {"chat_pattern": "Renamed chat title"}
        plan["roles"]["coordinator"]["display_name"] = "Renamed coordinator"
        plan["role_bindings"]["coordinator"]["title"] = "A renamed window"
        self.assertTrue(self.check(plan, path)["ready"])
        self.assertEqual(expected, confirmation_digests(plan))
        self.assertEqual(frozen, freeze_batch_plan(path, self.root / "frozen"))
        with self.assertRaisesRegex(TaskflowError, "unknown batch role"):
            role_config(frozen, "Renamed coordinator")

    def test_models_runtime_enabled_scope_and_direct_source_cannot_drift(self):
        plan, path = self.plan()
        variants = []
        for model in (None, "", " ", "auto", "DEFAULT", "unselected-model"):
            changed = copy.deepcopy(plan); changed["roles"]["author"]["model"] = model; variants.append(changed)
        for field, value in (("batch_id", "foreign-batch"), ("task_ids", ["foreign-task"])):
            changed = copy.deepcopy(plan); changed[field] = value; variants.append(changed)
        changed = copy.deepcopy(plan); changed["roles"]["author"]["runtime"]["timeout_seconds"] = 11; variants.append(changed)
        changed = copy.deepcopy(plan); changed["roles"]["author"] = {"enabled": False, "model": None, "runtime": None}; variants.append(changed)
        changed = copy.deepcopy(plan); changed["user_model_confirmation"]["origin"] = "assistant"; variants.append(changed)
        changed = copy.deepcopy(plan); changed["user_model_confirmation"]["scope_digest"] = "0" * 64; variants.append(changed)
        for index, value in enumerate(variants):
            with self.subTest(index=index):
                self.assertFalse(self.check(value, path)["ready"])
        write_json(path, plan)
        Path(plan["user_model_confirmation"]["source"]["path"]).write_text("changed", encoding="utf-8")
        self.assertIn("evidence bytes differ", check_batch_plan(path)["errors"][0])

    def test_chat_config_unknown_wrong_id_model_scope_and_title_only_are_rejected(self):
        plan, path = self.plan()
        for field, value in (("actual_model", "unknown"), ("thread_id", "foreign-thread"),
                             ("host", "foreign-host"), ("configuration_pointer", "/missing")):
            changed = copy.deepcopy(plan); changed["role_bindings"]["coordinator"][field] = value
            with self.subTest(field=field):
                self.assertFalse(self.check(changed, path)["ready"])
        changed = copy.deepcopy(plan); changed["role_bindings"].pop("coordinator")
        result = self.check(changed, path)
        self.assertEqual("awaiting_role_bindings", result["status"])
        self.assertIn("coordinator", result["unknown_chat_roles"])
        changed = copy.deepcopy(plan); changed["role_bindings"] = {"A chat title": {"model": "anything"}}
        self.assertFalse(self.check(changed, path)["ready"])
        config = self.root / "coordinator-actual-chat.json"
        document = json.loads(config.read_bytes()); document["configuration"]["scope"]["batch_id"] = "foreign"
        write_json(config, document)
        plan["role_bindings"]["coordinator"]["configuration_evidence"] = file_ref(config)
        self.assertFalse(self.check(plan, path)["ready"])

    def test_support_is_bound_to_original_cli_provider_output_and_capabilities(self):
        plan, path = self.plan()
        support_path = self.root / "author-support.json"
        original = json.loads(support_path.read_bytes())
        for field, value in (("origin", "arbitrary-self-report"), ("model", "foreign"),
                             ("provider", "foreign"), ("supported", False), ("returncode", True),
                             ("model_pointer", "/models/99/id"), ("efforts_pointer", "/models/0/id"),
                             ("contexts_pointer", "/models/0/id"), ("argv", [])):
            with self.subTest(field=field):
                document = copy.deepcopy(original); document[field] = value
                write_json(support_path, document)
                changed = copy.deepcopy(plan)
                changed["roles"]["author"]["runtime"]["support_evidence"] = file_ref(support_path)
                changed["user_model_confirmation"].update(confirmation_digests(changed))
                self.assertFalse(self.check(changed, path)["ready"])
        write_json(support_path, original)
        # Fixture observations never count as original real-CLI support.
        plan["execution_mode"] = "real_cli"
        self.assertFalse(self.check(plan, path)["ready"])

    def test_verified_client_default_requires_actual_default_locator(self):
        plan, path = self.plan()
        runtime = plan["roles"]["author"]["runtime"]
        runtime.update(reasoning_selection="verified_client_default", reasoning_effort="minimal")
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        self.assertFalse(self.check(plan, path)["ready"])
        support_path = self.root / "author-support.json"; support = json.loads(support_path.read_bytes())
        support.update(observation_kind="same_argv_no_inference", default_effort_pointer="/models/0/default_effort")
        write_json(support_path, support); runtime["support_evidence"] = file_ref(support_path)
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        self.assertTrue(self.check(plan, path)["ready"])

    def test_capability_locators_cannot_borrow_a_different_model_entry(self):
        plan, path = self.plan()
        support_path = self.root / "author-support.json"
        support = json.loads(support_path.read_bytes())
        support["efforts_pointer"] = "/models/1/efforts"
        write_json(support_path, support)
        plan["roles"]["author"]["runtime"]["support_evidence"] = file_ref(support_path)
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        result = self.check(plan, path)
        self.assertFalse(result["ready"])
        self.assertIn("same selected model", result["errors"][0])

    def test_partial_batch_policy_never_falls_back_and_all_slots_must_match(self):
        _, _, frozen = self.frozen()
        valid = self.policy(frozen)
        for field in ("model_confirmation_required", "batch_snapshot", "model_slots"):
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_batch_policy({field: valid[field]})
        for edit in ("missing", "extra", "different"):
            value = copy.deepcopy(valid)
            if edit == "missing":value["model_slots"].pop("author")
            elif edit == "extra":value["model_slots"]["foreign"] = value["model_slots"]["author"]
            else:value["model_slots"]["author"]["model"] = "foreign"
            with self.subTest(edit=edit), self.assertRaisesRegex(TaskflowError, "complete confirmed"):
                validate_batch_policy(value)
        with self.assertRaisesRegex(TaskflowError, "logical model_slot"):
            policy_setting(valid, "quality")
        with self.assertRaisesRegex(TaskflowError, "logical model_slot"):
            policy_setting(valid, "quality", "solver")
        with self.assertRaisesRegex(TaskflowError, "task_id"):
            policy_setting(valid, "author", task_id="foreign")
        self.assertEqual("other-provider/recheck", policy_setting(valid, "quality", "recheck")["model"])
        old = {"roles": {"quality": {"model": "legacy-exact", "reasoning_effort": "old"}}}
        self.assertIsNone(validate_batch_policy(old))
        self.assertEqual(old["roles"]["quality"], policy_setting(old, "quality"))
        with self.assertRaisesRegex(TaskflowError, "legacy policy"):
            policy_setting(old, "quality", "quality")

    def test_disabled_slot_is_not_implicitly_enabled_and_cli_ids_are_not_invented(self):
        plan, path, frozen = self.frozen(disabled=("repair",))
        policy = self.policy(frozen)
        self.assertNotIn("repair", policy["model_slots"])
        with self.assertRaisesRegex(TaskflowError, "disabled"):
            policy_setting(policy, "repair")
        plan["role_bindings"]["author"] = {"carrier": "cli", "work_id": None, "attempt_id": None, "sid": None}
        self.assertTrue(self.check(plan, path)["ready"])
        plan["role_bindings"]["author"]["sid"] = "invented-sid"
        self.assertFalse(self.check(plan, path)["ready"])

    def test_frozen_copy_survives_old_source_changes_but_its_own_drift_is_rejected(self):
        plan, path, frozen = self.frozen()
        source = Path(plan["user_model_confirmation"]["source"]["path"])
        source.write_text("changed external user capture", encoding="utf-8")
        self.assertFalse(check_batch_plan(path)["ready"])
        self.assertEqual(frozen["batch_id"], load_batch_snapshot(frozen["snapshot"])["batch_id"])
        copy_path = self.root / "frozen" / frozen["evidence_copies"][0]["relative_path"]
        copy_path.write_text("changed frozen copy", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "frozen evidence"):
            load_batch_snapshot(frozen["snapshot"])

    def test_freeze_refuses_existing_different_version_and_midcopy_changes(self):
        plan, path, frozen = self.frozen()
        plan["batch_id"] = "new-version"
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        # Change chat configuration scope honestly so the new plan is otherwise valid.
        for key in CHAT_ROLES:
            p = self.root / (key + "-actual-chat.json"); value = json.loads(p.read_bytes())
            value["configuration"]["scope"] = plan["user_model_confirmation"]["scope"]
            write_json(p, value)
            plan["role_bindings"][key]["configuration_evidence"] = file_ref(p)
        write_json(path, plan)
        with self.assertRaisesRegex(TaskflowError, "existing frozen batch"):
            freeze_batch_plan(path, self.root / "frozen")
        import taskflow.execution_batch as batch
        original = batch._write_new
        def write_then_drift(target, raw):
            original(target, raw)
            if target.name == "batch-snapshot.json":
                value = json.loads(path.read_bytes()); value["roles"]["author"]["runtime"]["timeout_seconds"] = 99
                write_json(path, value)
        with patch.object(batch, "_write_new", side_effect=write_then_drift), self.assertRaises(TaskflowError):
            freeze_batch_plan(path, self.root / "interrupted")
        self.assertFalse((self.root / "interrupted").exists())

    def test_malformed_runtime_schema_references_and_snapshot_are_rejected(self):
        plan, path = self.plan()
        for key, value in (("timeout_seconds", True), ("reasoning_selection", "auto"),
                           ("cli", "relative-cli"), ("provider", " "), ("foreign", True)):
            changed = copy.deepcopy(plan); changed["roles"]["author"]["runtime"][key] = value
            with self.subTest(key=key):
                self.assertFalse(self.check(changed, path)["ready"])
        changed = copy.deepcopy(plan); changed["schema_version"] = True
        self.assertFalse(self.check(changed, path)["ready"])
        write_json(path, plan); frozen = freeze_batch_plan(path, self.root / "frozen")
        manifest = Path(frozen["snapshot"]["path"]); raw = manifest.read_bytes()
        document = json.loads(raw); document["roles"]["author"]["model"] = "tampered"
        write_json(manifest, document)
        with self.assertRaises(TaskflowError):load_batch_snapshot(frozen["snapshot"])
        with self.assertRaises(TaskflowError):load_batch_snapshot(file_ref(manifest))
        manifest.write_bytes(raw)
        link = self.root / "linked"; link.symlink_to(self.root / "frozen", target_is_directory=True)
        with self.assertRaisesRegex(TaskflowError, "symlink"):
            freeze_batch_plan(path, link)


if __name__ == "__main__":
    unittest.main()

