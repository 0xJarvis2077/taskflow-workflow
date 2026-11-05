"""Confirmed slots reach author/public/quality work without changing old intents.

Machine attribution uses neutral local evidence. Native preparation is converted
only at the existing test-fixture seam; no real-provider or model proof is claimed.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution_author
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
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

    def failed_author_probe(self):
        """Prepare a neutral locked runtime; probe and native environment are doubles."""
        case = self.author()
        runtime = case.directory / "runtime"
        calls = []

        def prepare(root, runner):
            root.mkdir(parents=True)
            manifest = {"mode": "native_role", "preparation_config_digest": sha256(canonical(runner)),
                        "request": {"sha256": "a" * 64}}
            write_json(root / "manifest.json", manifest)
            write_json(root / "manifest-lock.json", execution_author._info(root / "manifest.json"))
            return manifest

        def validate(root, **kwargs):
            if read_json(root / "manifest-lock.json") != execution_author._info(root / "manifest.json"):
                raise TaskflowError("fixture locked manifest changed")
            if kwargs.get("before_run") and not (root / "native-probe-ready.json").exists():
                raise TaskflowError("fixture native seal missing")
            return read_json(root / "manifest.json")

        def probe(root):
            calls.append(root)
            passed = len(calls) > 1
            report = {"passed": passed, "model_calls": 0,
                      "manifest": execution_author._info(root / "manifest.json")}
            write_json(root / ("native-probe-success.json" if passed else "native-probe-first.json"), report)
            if passed:
                write_json(root / "native-probe-ready.json", {"passed": True})
            return report

        for target, value in (("taskflow.execution_runner.prepare_attempt", prepare),
                              ("taskflow.execution_runner._validate", validate),
                              ("taskflow.execution_runner.probe_native_attempt", probe)):
            active = patch(target, side_effect=value); active.start(); self.addCleanup(active.stop)
        for target in ("taskflow.execution._check_work_binding", "taskflow.execution_batch_binding.validate_batch_runner"):
            active = patch(target); active.start(); self.addCleanup(active.stop)
        active = patch("taskflow.execution_author.run_worker", side_effect=AssertionError("no model dispatch"))
        active.start(); self.addCleanup(active.stop)
        with self.assertRaisesRegex(TaskflowError, "current native probe failed"):
            start_author_from_packet(case.store, **case.kwargs())
        self.assertEqual([], case.store.dashboard()["attempts"])
        failure = runtime / "native-probe-first.json"
        return case, runtime, calls, failure

    def test_author_probe_retry_requires_exact_failure_and_preserves_originals(self):
        case, runtime, calls, failure = self.failed_author_probe()
        original = {path: path.read_bytes() for path in (
            failure, runtime / "manifest.json", runtime / "manifest-lock.json", case.directory / "spec.json")}
        digest = sha256(failure.read_bytes())
        with self.assertRaisesRegex(TaskflowError, "previous native probe"):
            start_author_from_packet(case.store, **case.kwargs())
        for grant in (["0" * 64], [], [digest, digest]):
            with self.subTest(grant=grant), self.assertRaises(TaskflowError):
                start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=grant)
        with self.assertRaisesRegex(TaskflowError, "must not dispatch"):
            start_author_from_packet(case.store, **case.kwargs(True), retry_failed_probes=[digest])
        self.assertEqual(1, len(calls))
        result = start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])
        self.assertEqual("ready", result["state"])
        self.assertEqual(2, len(calls))
        self.assertEqual([], case.store.dashboard()["attempts"])
        self.assertEqual(original, {path: path.read_bytes() for path in original})
        self.assertFalse(runtime.with_name("runtime.preparation-originals").exists())
        records = list((case.directory / "probe-retry-authorizations").glob("*.json"))
        self.assertEqual(1, len(records))
        self.assertEqual(digest, read_json(records[0])["reports"][0]["sha256"])
        with self.assertRaisesRegex(TaskflowError, "unqueued"):
            start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])

    def test_author_probe_retry_rejects_new_or_missing_reports_and_manifest_drift(self):
        for change in ("missing", "added", "manifest", "factory", "batch", "attempt", "ready", "not-failed"):
            with self.subTest(change=change):
                case, runtime, calls, failure = self.failed_author_probe()
                digest = sha256(failure.read_bytes())
                kwargs = case.kwargs()
                if change == "missing":
                    failure.unlink()
                elif change == "added":
                    write_json(runtime / "native-probe-added.json", read_json(failure))
                elif change == "manifest":
                    (runtime / "manifest.json").write_text("{}")
                elif change == "factory":
                    kwargs["factory_config"] = {**case.config, "research_root": str(case.root / "other")}
                elif change == "batch":
                    case.packet_data["batch_id"] = "different-batch"
                    write_json(case.packet, case.packet_data)
                    kwargs["packet_ref"] = evidence_ref(case.packet, "changed neutral packet")
                elif change == "attempt":
                    active = patch.object(case.store, "list_attempts", return_value=[{"state": "uncertain"}])
                    active.start(); self.addCleanup(active.stop)
                elif change == "ready":
                    write_json(runtime / "native-probe-ready.json", {"passed": True})
                else:
                    report = read_json(failure); report["passed"] = True
                    write_json(failure, report)
                    digest = sha256(failure.read_bytes())
                with self.assertRaises(TaskflowError):
                    start_author_from_packet(case.store, **kwargs, retry_failed_probes=[digest])
                self.assertEqual(1, len(calls))
                self.assertEqual([], case.store.dashboard()["attempts"])
                self.assertEqual([], case.store.dashboard()["work"])

    def test_new_failed_probe_requires_new_explicit_complete_sha_set(self):
        case, runtime, calls, failure = self.failed_author_probe()
        digest = sha256(failure.read_bytes())
        def fail_again(root):
            calls.append(root)
            report = {"passed": False, "model_calls": 0,
                      "manifest": execution_author._info(root / "manifest.json")}
            write_json(root / "native-probe-second.json", report)
            return report
        with patch("taskflow.execution_runner.probe_native_attempt", side_effect=fail_again):
            with self.assertRaisesRegex(TaskflowError, "current native probe failed"):
                start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])
            with self.assertRaisesRegex(TaskflowError, "match all"):
                start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])
        self.assertEqual(2, len(calls))
        self.assertEqual([], case.store.dashboard()["attempts"])
        self.assertEqual([], case.store.dashboard()["work"])

    def test_author_preparation_lock_excludes_same_intent_only(self):
        case, runtime, calls, failure = self.failed_author_probe()
        digest = sha256(failure.read_bytes())
        other = case.root / "different-intent"
        other.mkdir()
        with execution_author._author_prepare_lock(case.directory):
            with execution_author._author_prepare_lock(other):
                pass
            for grant in (None, [digest]):
                with self.subTest(grant=grant), self.assertRaisesRegex(TaskflowError, "already active"):
                    start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=grant)
        self.assertEqual(1, len(calls))
        self.assertEqual([], case.store.dashboard()["work"])
        self.assertEqual([], case.store.dashboard()["attempts"])
        self.assertFalse((case.directory / "probe-retry-authorizations").exists())

    def test_interrupted_probe_cannot_reuse_consumed_authorization(self):
        case, runtime, calls, failure = self.failed_author_probe()
        digest = sha256(failure.read_bytes())
        with patch("taskflow.execution_runner.probe_native_attempt", side_effect=PermissionError("fixture interruption")):
            with self.assertRaises(PermissionError):
                start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])
            with self.assertRaisesRegex(TaskflowError, "already consumed"):
                start_author_from_packet(case.store, **case.kwargs(), retry_failed_probes=[digest])
        self.assertEqual([], case.store.dashboard()["work"])
        self.assertEqual([], case.store.dashboard()["attempts"])

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
