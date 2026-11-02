"""Confirmed batches use the real local queue/collector with neutral subprocesses."""

import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, write_json
from taskflow.execution_batch import load_batch_snapshot, policy_setting
from taskflow.execution_batch_binding import make_batch_binding, validate_batch_runner, validate_batch_source
from taskflow.execution_batch_factory import checked_batch_factory, compile_batch_configuration
from taskflow.execution_runner import RunnerError, seal_facts
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.test_execution_batch import ROLE_KEYS, make_confirmed_plan
from tools.execution_simulation import make_package, package_reviews


class BatchDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="batch-dispatch-", dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.plan, self.plan_path = make_confirmed_plan(self.root / "choices")
        self.infra = self.root / "infrastructure.json"
        write_json(self.infra, {"schema_version": 1, "policy_id": "neutral-batch-v1",
                               "policy": {"concurrency": 3}})

    def call(self, *argv):
        out, error = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(error):
            code = execution.main(["--db", str(self.root / "never.sqlite3"), *argv])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, error.getvalue()

    def configured(self):
        result = compile_batch_configuration(self.plan_path, self.infra, self.root / "configuration")
        policy = read_json(Path(result["policy"]["path"]))["arguments"]
        store = ExecutionStore(self.root / "temporary.sqlite3")
        store.register_policy(**policy)
        task = make_package(self.root)
        data = read_json(task / "task.json")
        data["task_id"] = "neutral-task"
        write_json(task / "task.json", data)
        execution.capture_candidate(store, task, "seed")
        return result, store, task

    def specification(self, store, candidate, low_role, slot, name, payload, *, sid=None, extra=None):
        setting = policy_setting(store.get_policy("neutral-batch-v1")["config"], low_role, slot, "neutral-task")
        work = self.root / "work" / name
        work.mkdir(parents=True)
        source = self.root / (name + "-payload.json")
        write_json(source, payload)
        cli = Path(__file__).resolve().parents[1] / "tools/mock_role_cli.py"
        runner = {key: setting[key] for key in ("model", "reasoning_effort", "reasoning_selection", "reasoning_context", "timeout_seconds")}
        runner.update(intent_id=name, policy_version="neutral-batch-v1", role=low_role, model_slot=slot,
                      mode="test_fixture", cwd=str(work), outputs=list(payload), inputs=[],
                      argv=[str(Path(sys.executable).resolve()), str(cli), "--payload", str(source), "--sid", sid or str(uuid.uuid4())],
                      dependencies=[str(source), str(cli)], request_sections=[{"kind": "operation", "text": "Neutral local fixture, no model."}])
        runner.update(extra or {})
        return {"work": {"work_id": name, "task_id": "neutral-task", "candidate_id": candidate, "role": low_role,
                         "model_slot": slot, "policy_id": "neutral-batch-v1", "intent_id": name,
                         "purpose": "neutral batch lifecycle"}, "runner": runner}

    def perform(self, store, candidate, role, slot, name, payload, **kwargs):
        spec = self.specification(store, candidate, role, slot, name, payload, **kwargs)
        runtime = self.root / "runtimes" / name
        execution.enqueue_spec(store, spec, runtime)
        result = execution.run_worker(store, "neutral-batch", work_ids=[name])
        self.assertEqual("completed", result["state"], result)
        manifest = read_json(runtime / "manifest.json")
        self.assertTrue(validate_batch_source(result, manifest, store))
        self.assertEqual(self.plan["roles"][slot]["model"], manifest["model"]["requested"])
        return result

    def test_empty_template_cli_reports_all_missing_without_database(self):
        empty = self.root / "empty.json"
        write_json(empty, {"schema_version": 1, "architecture_version": "high-value-task-v1", "batch_id": None,
                           "task_ids": [], "user_model_confirmation": None,
                           "roles": {key: {"enabled": True, "model": None} for key in ROLE_KEYS}})
        code, output, error = self.call("batch-check", "--plan", str(empty))
        self.assertEqual(2, code, error)
        self.assertEqual(list(ROLE_KEYS), output["missing_models"])
        self.assertEqual("awaiting_user_models", output["status"])
        self.assertFalse((self.root / "never.sqlite3").exists())
        code, _, _ = self.call("batch-config", "--plan", str(empty), "--infrastructure", str(self.infra), "--output", str(self.root / "blocked"))
        self.assertEqual(2, code)
        self.assertFalse((self.root / "blocked").exists())

    def test_compile_does_not_take_models_from_infrastructure_and_old_choice_source_can_change(self):
        base = read_json(self.infra)
        base["policy"]["roles"] = {"author": {"model": "historical-default", "reasoning_effort": "old", "timeout_seconds": 99}}
        write_json(self.infra, base)
        result, store, _ = self.configured()
        policy = store.get_policy("neutral-batch-v1")
        self.assertEqual("other-provider/author", policy["config"]["roles"]["author"]["model"])
        frozen = Path(result["snapshot"]["path"]).read_bytes()
        Path(self.plan["user_model_confirmation"]["source"]["path"]).write_text("changed later choice source")
        self.assertEqual("other-provider/quality", load_batch_snapshot(result["snapshot"])["roles"]["quality"]["model"])
        self.assertEqual(frozen, Path(result["snapshot"]["path"]).read_bytes())

    def test_wrong_runner_slot_scope_and_model_are_blocked_before_runtime(self):
        _, store, _ = self.configured()
        original = self.specification(store, "seed", "quality", "recheck", "bad", {"out.md": "neutral"})
        for change in ({"model": "historical-default"}, {"model_slot": "quality"}, {"reasoning_effort": "unconfirmed"}):
            spec = copy.deepcopy(original)
            spec["runner"].update(change)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                execution.enqueue_spec(store, spec, self.root / "never-runtime")
            self.assertFalse((self.root / "never-runtime").exists())
        with self.assertRaisesRegex(TaskflowError, "scope"):
            make_batch_binding(store.get_policy("neutral-batch-v1"), "author", "author", "foreign-task")
        self.assertEqual([], store.dashboard()["attempts"])

    def test_native_fixture_and_unsupported_reasoning_cannot_be_used_for_real_calls(self):
        _, store, _ = self.configured()
        binding = make_batch_binding(store.get_policy("neutral-batch-v1"), "quality", "quality", "neutral-task")
        value = {**binding["setting"], "role": "quality", "policy_version": "neutral-batch-v1", "model_slot": "quality",
                 "mode": "native_role", "batch_binding": binding}
        with self.assertRaisesRegex(TaskflowError, "fixture"):
            validate_batch_runner(value)
        value["batch_binding"] = None
        with self.assertRaisesRegex(TaskflowError, "original batch"):
            validate_batch_runner(value)

    def test_new_batch_entry_rejects_missing_snapshot_before_store_and_historical_factory_has_no_default(self):
        factory, packet = self.root / "factory.json", self.root / "packet.json"
        write_json(factory, {"schema_version": 1, "policy_id": "old"})
        write_json(packet, {"items": [{"task_id": "neutral-task"}]})
        code, _, _ = self.call("batch-author-start", "--factory", str(factory), "--packet", str(packet))
        self.assertEqual(2, code)
        self.assertFalse((self.root / "never.sqlite3").exists())
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            execution.parser().parse_args(["author-start", "--packet", str(packet)])

    def factory_infrastructure(self):
        template = self.root / "generic-template.json"
        write_json(template, {"description": "Neutral infrastructure fixture; no model permission"})
        ref = evidence_ref(template, "neutral generic template bytes")
        base = read_json(self.infra)
        base["author_factory"] = {"schema_version": 1, "template_spec_ref": ref, "role_template_ref": ref,
                                  "research_root": str(self.root / "research"), "lifecycle_root": str(self.root / "lifecycles")}
        base["experiment_factory"] = {"schema_version": 1, "repository": str(self.root),
            "formal_runtime_root": str(self.root / "formal"), "lifecycle_root": str(self.root / "lifecycles"),
            "publication_root": str(self.root / "publications"), "technical": {
                "formal_config_base": {"model": "old-default"}, "review_runner": {"model": "old-default"},
                "local_profile_policy": {"required_model": "old-default"}}}
        write_json(self.infra, base)

    def test_factories_take_confirmed_slots_and_foreign_batch_same_task_is_rejected_before_store(self):
        self.factory_infrastructure()
        result = compile_batch_configuration(self.plan_path, self.infra, self.root / "configuration")
        factory = Path(result["author_factory"]["path"])
        experiment = read_json(Path(result["experiment_factory"]["path"]))
        self.assertEqual("other-provider/solver", experiment["technical"]["formal_config_base"]["model"])
        self.assertEqual("other-provider/reviewer", experiment["technical"]["review_runner"]["model"])
        packet = self.root / "packet.json"
        write_json(packet, {"batch_id": "foreign-batch", "items": [{"task_id": "neutral-task"}]})
        code, _, error = self.call("batch-author-start", "--factory", str(factory), "--packet", str(packet))
        self.assertEqual(2, code)
        self.assertIn("batch_id", error)
        self.assertFalse((self.root / "never.sqlite3").exists())
        write_json(packet, {"batch_id": "neutral-batch", "items": [{"task_id": "foreign-task"}]})
        with self.assertRaisesRegex(TaskflowError, "scope"):
            checked_batch_factory(factory, packet)

    def test_batch_policy_registration_reuses_exact_bytes_and_cli_reentry_is_idempotent(self):
        self.factory_infrastructure()
        result = compile_batch_configuration(self.plan_path, self.infra, self.root / "configuration")
        factory, packet = Path(result["author_factory"]["path"]), self.root / "packet.json"
        # The author interface is a deliberate no-dispatch double here. The
        # separate full-chain test executes actual fixture workers/collectors.
        with patch("taskflow.execution_author.start_author_from_packet", return_value={"state": "fixture_ready", "model_calls": 0}) as prepare:
            for intent in ("first", "first", "second"):
                write_json(packet, {"batch_id": "neutral-batch", "items": [{"task_id": "neutral-task", "intent_id": intent}]})
                code, output, error = self.call("batch-author-start", "--factory", str(factory), "--packet", str(packet))
                self.assertEqual(0, code, error)
                self.assertEqual("fixture_ready", output["state"])
            self.assertEqual(3, prepare.call_count)
        store = ExecutionStore(self.root / "never.sqlite3")
        original = store.get_policy("neutral-batch-v1")
        self.assertEqual(1, len([event for event in store.events() if event["kind"] == "policy_registered"]))
        changed = copy.deepcopy(original["config"])
        changed["concurrency"] = 4
        with self.assertRaisesRegex(TaskflowError, "different original"):
            store.register_policy("neutral-batch-v1", changed)
        self.assertEqual(original, store.get_policy("neutral-batch-v1"))

    def test_invalid_infrastructure_fails_before_freezing(self):
        base = read_json(self.infra)
        base["experiment_factory"] = {"schema_version": 1, "technical": []}
        write_json(self.infra, base)
        with self.assertRaisesRegex(TaskflowError, "infrastructure"):
            compile_batch_configuration(self.plan_path, self.infra, self.root / "forbidden")
        self.assertFalse((self.root / "forbidden").exists())

    def test_confirmed_models_execute_full_neutral_quality_repair_and_same_sid_review(self):
        _, store, task = self.configured()
        author = self.perform(store, "seed", "author", "author", "author", {"prompt.md": "Capacity is 3. Allocate 2 units. Draft selects A.\n"})
        shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", task / "draft/input/prompt.md")
        execution.capture_candidate(store, task, "v1", "seed", author["attempt_id"])
        self.perform(store, "v1", "quality", "public_rebuild", "public", {"work.md": "Independent public result"})
        quality = self.perform(store, "v1", "quality", "quality", "quality", {"review.md": "Remove supplied recommendation; preserve capacity 3 and demand 2."})
        ref = [evidence_ref(Path(quality["runtime_path"]) / "artifacts/review.md", "neutral actual finding")]
        store.record_review("q1", "v1", quality["attempt_id"], verdict="defects", scope="whole neutral", evidence=ref, whole_package=True)
        store.open_defect("D1", "v1", "q1", responsibility="restore choice", evidence=ref)
        repair = self.perform(store, "v1", "repair", "repair", "repair", {"prompt.md": "Capacity is 3. A and B are available. Allocate 2 units and justify.\n"})
        shutil.copyfile(Path(repair["runtime_path"]) / "artifacts/prompt.md", task / "draft/input/prompt.md")
        execution.capture_candidate(store, task, "v2", "v1", repair["attempt_id"])
        store.record_repair("D1", "v2", evidence=[evidence_ref(task / "draft/input/prompt.md", "real changed neutral input")])
        recheck = self.perform(store, "v2", "quality", "recheck", "recheck", {"review.md": "Whole package and linked conditions verified; no supplied choice."})
        ref = [evidence_ref(Path(recheck["runtime_path"]) / "artifacts/review.md", "neutral independent recheck")]
        store.record_review("q2", "v2", recheck["attempt_id"], verdict="accepted", scope="whole neutral and related criteria", evidence=ref, whole_package=True)
        store.close_defect("D1", "q2", evidence=ref)
        self.assertEqual("q2", store.quality_gate("v2")["review_id"])
        package_reviews(task, Path(recheck["runtime_path"]) / "artifacts/review.md")
        release = execution.release_candidate(store, task, "v2", "neutral-v2")
        self.assertTrue(Path(release["snapshot"]["path"]).is_dir())
        store.record_decision("formal", "neutral-task", "v2", action="experiment", rationale="neutral simulation only", evidence=ref,
                              experiment_purpose="neutral batch lifecycle", authorized=True)
        solver = self.perform(store, "v2", "solver", "solver", "solver", {"answer.md": "Allocate 2 to A, 2 <= 3."})
        facts = self.perform(store, "v2", "review-facts", "reviewer", "facts", {"facts.md": "Whole answer chooses A and checks capacity."},
                             extra={"inputs": [{"path": str(Path(solver["runtime_path"]) / "artifacts/answer.md"), "kind": "submission"}]})
        root = Path(facts["runtime_path"])
        seal_facts(root, root / "artifacts/facts.md")
        with self.assertRaisesRegex(RunnerError, "same review SID"):
            spec = self.specification(store, "v2", "review-score", "reviewer", "bad-sid", {"score.json": "{}"},
                                      extra={"facts_attempt": str(root), "resume_sid": str(uuid.uuid4())})
            execution.enqueue_spec(store, spec, self.root / "bad-sid-runtime")
        with self.assertRaisesRegex(TaskflowError, "runner"):
            spec = self.specification(store, "v2", "review-score", "reviewer", "bad-model", {"score.json": "{}"},
                                      extra={"facts_attempt": str(root), "resume_sid": facts["cli_sid"], "model": "other-provider/quality"})
            execution.enqueue_spec(store, spec, self.root / "bad-model-runtime")
        self.assertFalse((self.root / "bad-model-runtime").exists())
        score = self.perform(store, "v2", "review-score", "reviewer", "score", {"score.json": json.dumps({"score": 7, "nature": "test_fixture"})},
                             sid=facts["cli_sid"], extra={"facts_attempt": str(root), "resume_sid": facts["cli_sid"]})
        self.assertEqual(facts["cli_sid"], score["cli_sid"])
        self.assertEqual(store.get_work("facts")["model_slot"], store.get_work("score")["model_slot"])
        self.assertEqual("closed", store.get_defect("D1")["state"])


if __name__ == "__main__":
    unittest.main()
