"""Confirmed slots reach author/public/quality work without changing old intents.

Machine attribution uses neutral local evidence. Native preparation is converted
only at the existing test-fixture seam; no real-provider or model proof is claimed.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution_author
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import _roles, start_author_from_packet
from taskflow.execution_batch import (
    CHAT_ROLES, CLI_ROLES, ROLE_KEYS, confirmation_digests, freeze_batch_plan,
    load_batch_snapshot, role_config,
)
from taskflow.execution_experiment_factory import start_experiments_from_packet
from taskflow.execution_flow import register_workflow, run_workflows
from taskflow.execution_runner import _validate
from taskflow.execution_store import evidence_ref
from tests import test_execution_author_factory as author_fixtures
from tests import test_execution_batch as batch_fixtures
from tests import test_execution_experiment_factory as experiment_fixtures
from tests import test_execution_flow as flow_fixtures
from tools.execution_simulation import fixture_policy


def batch_policy(root, tasks):
    plan, path = batch_fixtures.make_confirmed_plan(root / "batch-evidence")
    plan["task_ids"] = tasks
    confirmed = confirmation_digests(plan)
    for role in CHAT_ROLES:
        binding = plan["role_bindings"][role]
        source = Path(binding["configuration_evidence"]["path"])
        payload = read_json(source)
        payload["configuration"]["scope"] = confirmed["scope"]
        write_json(source, payload)
        binding["configuration_evidence"] = batch_fixtures.file_ref(source)
    plan["user_model_confirmation"].update(confirmed)
    write_json(path, plan)
    frozen = freeze_batch_plan(path, root / "frozen-batch")
    snapshot = load_batch_snapshot(frozen["snapshot"])
    config = {**fixture_policy(), "model_confirmation_required": True,
              "batch_snapshot": frozen["snapshot"],
              "model_slots": {key: role_config(snapshot, key) for key in ROLE_KEYS
                              if key in CLI_ROLES and snapshot["roles"][key]["enabled"]}}
    registration = root / "batch-policy-record.json"
    write_json(registration, {"operation": "register_policy",
                             "arguments": {"policy_id": "batch-v1", "config": config}})
    return config, snapshot, evidence_ref(registration, "neutral original batch policy registration")


class BatchAuthorTests(unittest.TestCase):
    def author(self, *, batch=True):
        case = author_fixtures.AuthorFactoryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.factory()
        if batch:
            config, snapshot, record = batch_policy(case.root, ["neutral"])
            case.store.register_policy("batch-v1", config)
            case.config.update(policy_id="batch-v1", batch_snapshot=config["batch_snapshot"], policy_record_ref=record)
            case.packet_data["batch_id"] = snapshot["batch_id"]
            write_json(case.packet, case.packet_data)
            case.packet_ref = evidence_ref(case.packet, "neutral once-only batch author packet")
            case.snapshot = snapshot
        return case

    def ready(self, case, **overrides):
        kwargs = case.kwargs()
        kwargs.update(overrides)
        with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("ready call cannot dispatch")):
            return start_author_from_packet(case.store, **kwargs)

    def test_batch_author_slot_and_original_prepared_request_are_retained(self):
        case = self.author()
        first = self.ready(case)
        paths = [case.directory / name for name in ("factory-intent.json", "spec.json", "runtime/manifest.json", "runtime/request.md")]
        originals = {path: path.read_bytes() for path in paths}
        second = self.ready(case)
        self.assertEqual(first, second)
        spec = read_json(case.directory / "spec.json")
        self.assertEqual("author", spec["work"]["model_slot"])
        self.assertEqual("author", spec["runner"]["model_slot"])
        self.assertEqual("other-provider/author", spec["runner"]["model"])
        self.assertEqual("all_turns", spec["runner"]["reasoning_context"])
        manifest = _validate(Path(first["runtime"]))
        self.assertEqual("author", manifest["model_slot"])
        self.assertEqual("other-provider/author", manifest["model"]["requested"])
        self.assertEqual(case.store.get_policy("batch-v1")["digest"], manifest["batch_binding"]["policy_digest"])
        self.assertEqual("author", case.store.get_work(first["work_id"])["model_slot"])
        self.assertEqual([], case.store.dashboard()["attempts"])
        self.assertEqual(1, len(case.native_specs))
        self.assertEqual(originals, {path: path.read_bytes() for path in paths})

    def test_new_author_missing_factory_never_creates_an_intent(self):
        case = self.author(batch=False)
        before = case.store.dashboard()
        with self.assertRaisesRegex(TaskflowError, "explicit factory"):
            start_author_from_packet(case.store, **{**case.kwargs(), "factory_config": None})
        self.assertFalse(case.directory.exists())
        self.assertEqual(before, case.store.dashboard())

    def test_batch_factory_policy_packet_and_task_scope_fail_before_any_intent(self):
        for variant in ("snapshot", "policy_record", "missing_batch", "wrong_batch", "task"):
            with self.subTest(variant=variant):
                case = self.author()
                before = case.store.dashboard()
                config = copy.deepcopy(case.config)
                if variant == "snapshot":config.pop("batch_snapshot")
                elif variant == "policy_record":config["policy_record_ref"] = case.packet_ref
                elif variant == "missing_batch":case.packet_data.pop("batch_id")
                elif variant == "wrong_batch":case.packet_data["batch_id"] = "foreign-batch"
                else:case.packet_data["items"][0]["task_id"] = "outside-confirmed-scope"
                if variant in {"missing_batch", "wrong_batch", "task"}:
                    write_json(case.packet, case.packet_data)
                    case.packet_ref = evidence_ref(case.packet, "neutral deliberately mismatched packet")
                with self.assertRaises(TaskflowError):
                    self.ready(case, factory_config=config)
                self.assertFalse(case.directory.exists())
                self.assertEqual(before, case.store.dashboard())

    def test_only_an_existing_exact_legacy_factory_can_use_none_and_preserve_bytes(self):
        case = self.author(batch=False)
        first = self.ready(case)
        paths = [case.directory / name for name in ("factory-intent.json", "spec.json", "runtime/manifest.json", "runtime/request.md")]
        originals = {path: path.read_bytes() for path in paths}
        actual_read = execution_author._read
        default = Path(execution_author.__file__).resolve().parents[1] / "config/execution-author-factory-v1.json"
        def read(path):
            return copy.deepcopy(case.config) if Path(path) == default else actual_read(path)
        with patch("taskflow.execution_author._read", side_effect=read):
            second = self.ready(case, factory_config=None)
        self.assertEqual(first, second)
        self.assertNotIn("model_slot", read_json(case.directory / "spec.json")["runner"])
        self.assertEqual(originals, {path: path.read_bytes() for path in paths})
        changed = {**case.config, "policy_id": "other-policy"}
        with patch("taskflow.execution_author._read", side_effect=lambda path: changed if Path(path) == default else actual_read(path)), \
             self.assertRaisesRegex(TaskflowError, "original author factory configuration"):
            self.ready(case, factory_config=None)
        self.assertEqual(originals, {path: path.read_bytes() for path in paths})

    def test_logical_public_quality_recheck_and_repair_slots_stay_distinct(self):
        case = self.author()
        definition = {"task_id": "neutral", "policy_id": "batch-v1", "role_template_ref": case.config["role_template_ref"]}
        roles = _roles(case.store, definition)
        expected = {"public_rebuild": "public_rebuild", "quality": "quality", "repair": "repair",
                    "recheck": "recheck", "recheck_public": "quality"}
        for name, slot in expected.items():
            self.assertEqual(slot, roles[name]["runner"]["model_slot"])
            self.assertEqual("other-provider/" + slot, roles[name]["runner"]["model"])
            self.assertEqual("all_turns", roles[name]["runner"]["reasoning_context"])
        self.assertEqual("other-provider/public_rebuild", roles["public_rebuild"]["prepare"]["config"]["model"])
        self.assertNotEqual(roles["quality"]["runner"]["model"], roles["recheck"]["runner"]["model"])
        with self.assertRaisesRegex(TaskflowError, "scope"):
            _roles(case.store, {**definition, "task_id": "foreign-task"})

    def test_actual_neutral_quality_repair_recheck_workers_lock_their_own_slots(self):
        case = flow_fixtures.ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        config, _, _ = batch_policy(case.root, ["neutral"])
        case.store.register_policy("batch-v1", config)
        definition = case.definition("neutral")
        definition["policy_id"] = "batch-v1"
        for name, role in definition["roles"].items():
            role["runner"]["model_slot"] = name
        register_workflow(case.store, definition, case.refs)
        run_workflows(case.store, [definition["flow_id"]], allow_model=True, poll_seconds=.01)
        result = case.store.get_flow(definition["flow_id"])
        self.assertEqual("accepted", result["state"]["status"], result["state"])
        attempts = case.store.dashboard()["attempts"]
        self.assertEqual({"quality", "repair", "recheck"}, {row["model_slot"] for row in attempts})
        self.assertEqual(3, len({row["cli_sid"] for row in attempts}))
        for row in attempts:
            manifest = _validate(Path(row["runtime_path"]))
            self.assertEqual(row["model_slot"], manifest["model_slot"])
            self.assertEqual("other-provider/" + row["model_slot"], manifest["model"]["requested"])

    def test_experiment_factory_checks_every_item_and_runtime_before_registration(self):
        for variant in ("valid", "snapshot", "record", "batch", "task", "runtime"):
            with self.subTest(variant=variant):
                case = experiment_fixtures.ExperimentFactoryTests()
                case.setUp()
                self.addCleanup(case.doCleanups)
                policy, snapshot, record = batch_policy(case.root, ["topic-0", "topic-1"])
                case.store.register_policy("batch-v1", policy)
                config = read_json(case.config_path)
                config.update(policy_id="batch-v1", batch_snapshot=policy["batch_snapshot"], policy_record_ref=record)
                for slot, target in (("solver", config["technical"]["formal_config_base"]), ("reviewer", config["technical"]["review_runner"])):
                    target.update(role_config(snapshot, slot))
                packet = read_json(case.packet_path)
                packet["batch_id"] = snapshot["batch_id"]
                if variant == "snapshot":config.pop("batch_snapshot")
                elif variant == "record":config["policy_record_ref"] = case.packet_ref
                elif variant == "batch":packet["batch_id"] = "other-batch"
                elif variant == "task":packet["items"][-1]["task_id"] = "outside-scope"
                elif variant == "runtime":config["technical"]["review_runner"]["model"] = "unconfirmed-model"
                write_json(case.config_path, config);write_json(case.packet_path, packet)
                case.config_ref = evidence_ref(case.config_path, "neutral batch factory")
                case.packet_ref = evidence_ref(case.packet_path, "neutral batch packet")
                with patch("taskflow.execution_experiment_factory.launch_lifecycle_consumer", side_effect=AssertionError("no launch")):
                    if variant == "valid":
                        result = case.start(False)
                        self.assertFalse(result["attention"], result)
                        self.assertEqual(2, len(case.store.list_flows()))
                    else:
                        with self.assertRaises(TaskflowError):case.start(False)
                        self.assertEqual([], case.store.list_flows())
                        self.assertFalse((case.root / "lifecycles").exists())


if __name__ == '__main__':
    unittest.main()
