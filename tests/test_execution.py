"""The opt-in CLI is exercised with real neutral subprocess deliveries."""

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_runner import RunnerError, prepare_attempt, run_attempt, seal_facts
from taskflow.execution_store import ExecutionStore, evidence_ref
from tools.execution_simulation import fixture_policy, make_package, package_reviews, perform


class ExecutionCLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "execution.sqlite3"
        self.store = ExecutionStore(self.db)
        self.count = 0
        self.task = make_package(self.root)
        self.command("register_policy", {"policy_id": "fixture-v1", "config": fixture_policy()})
        code, candidate, _ = self.call("capture", str(self.task), "--candidate-id", "neutral-a")
        self.assertEqual(0, code, candidate)

    def call(self, *arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = execution.main(["--db", str(self.db), *arguments])
        output = stdout.getvalue()
        try:
            output = json.loads(output)
        except ValueError:
            pass
        return code, output, stderr.getvalue()

    def command(self, operation, arguments, expected=0):
        self.count += 1
        path = self.root / f"command-{self.count}.json"
        write_json(path, {"operation": operation, "arguments": arguments})
        result = self.call("apply", "--record", str(path))
        self.assertEqual(expected, result[0], result)
        return result

    def spec(self, identity="role-01", role="research", policy="fixture-v1"):
        cwd = self.root / "work" / identity
        cwd.mkdir(parents=True)
        payload = self.root / f"{identity}-payload.json"
        write_json(payload, {"delivery.md": "Complete neutral fixture delivery."})
        script = Path(__file__).resolve().parents[1] / "tools/mock_role_cli.py"
        return {"work": {
            "work_id": identity, "task_id": "neutral", "candidate_id": "neutral-a",
            "role": role, "policy_id": policy, "intent_id": identity,
            "purpose": "CLI lifecycle fixture",
        }, "runner": {
            "intent_id": identity, "policy_version": policy, "role": role,
            "mode": "test_fixture", "model": "fixture-cli",
            "reasoning_effort": "fixture", "timeout_seconds": 15,
            "cwd": str(cwd), "argv": [str(Path(sys.executable).resolve()), str(script), "--payload", str(payload), "--sid", str(uuid.uuid4())],
            "inputs": [], "outputs": ["delivery.md"],
            "request_sections": [{"kind": "operation", "text": "Neutral fixture; deliver declared files."}],
            "dependencies": [str(payload), str(script)],
        }}

    def enqueue(self, identity="role-01", role="research"):
        spec = self.spec(identity, role)
        path = self.root / f"{identity}-spec.json"
        write_json(path, spec)
        runtime = self.root / "runtimes" / identity
        code, result, error = self.call("enqueue", "--spec", str(path), "--runtime", str(runtime))
        self.assertEqual(0, code, error)
        return runtime, result

    def accept_quality(self):
        runtime, _ = self.enqueue("quality", "quality")
        code, attempt, error = self.call("worker", "--worker-id", "quality-worker")
        self.assertEqual(0, code, error)
        self.assertEqual("completed", attempt["state"])
        ref = evidence_ref(runtime / "artifacts/delivery.md", "fixture quality report")
        self.command("record_review", {
            "review_id": "quality-accepted", "candidate_id": "neutral-a",
            "attempt_id": attempt["attempt_id"], "verdict": "accepted",
            "scope": "whole neutral package and linked private standard",
            "whole_package": True, "evidence": [ref],
        })
        return runtime

    def test_real_enqueue_worker_reconcile_and_events(self):
        runtime, queued = self.enqueue()
        self.assertEqual("ready", queued["work"]["state"])
        code, attempt, error = self.call("worker", "--worker-id", "one")
        self.assertEqual(0, code, error)
        self.assertEqual("completed", attempt["state"])
        self.assertTrue(attempt["response_complete"])
        self.assertTrue(attempt["artifacts_complete"])
        self.assertTrue((runtime / "trace.jsonl").is_file())
        self.assertTrue((runtime / "process.json").is_file())
        code, reconciled, _ = self.call("reconcile", attempt["attempt_id"])
        self.assertEqual(0, code)
        self.assertEqual(attempt, reconciled)
        code, waiting, _ = self.call("worker", "--worker-id", "one")
        self.assertEqual("waiting", waiting["state"])
        code, events, _ = self.call("events", "--after", "0")
        self.assertEqual(0, code)
        self.assertIn("dispatch_intent", {item["kind"] for item in events["events"]})
        self.assertIn("attempt_finished", {item["kind"] for item in events["events"]})

    def test_release_requires_quality_and_actual_byte_binding(self):
        code, _, error = self.call("release", str(self.task), "--candidate-id", "neutral-a", "--version", "fixture-v1")
        self.assertEqual(2, code)
        self.assertIn("quality", error)
        runtime = self.accept_quality()
        package_reviews(self.task, runtime / "artifacts/delivery.md")
        code, released, error = self.call("release", str(self.task), "--candidate-id", "neutral-a", "--version", "fixture-v1")
        self.assertEqual(0, code, error)
        self.assertTrue(Path(released["snapshot"]["path"]).is_dir())
        self.assertTrue(Path(released["solver"]["path"]).is_dir())
        original = Path(released["snapshot"]["path"]) / "draft/input/prompt.md"
        saved = original.read_bytes()
        (self.task / "draft/input/prompt.md").write_text("different public input", encoding="utf-8")
        code, _, error = self.call("release", str(self.task), "--candidate-id", "neutral-a", "--version", "fixture-v2")
        self.assertEqual(2, code)
        self.assertIn("changed", error)
        self.assertEqual(saved, original.read_bytes())

    def test_capture_invalid_package_and_immutable_identity(self):
        (self.task / "draft/input/prompt.md").unlink()
        code, _, error = self.call("capture", str(self.task), "--candidate-id", "bad")
        self.assertEqual(2, code)
        self.assertIn("structure invalid", error)

    def test_apply_rejects_unknown_operations_missing_methods_and_wrong_types(self):
        for record in ({}, {"operation": "finish_attempt", "arguments": {}}, {"operation": "observe", "arguments": []}):
            with self.assertRaises(TaskflowError):
                execution.apply_record(self.store, record)
        with self.assertRaisesRegex(TaskflowError, "not implemented"):
            execution.apply_record(object(), {"operation": "observe", "arguments": {}})
        self.command("unknown", {}, expected=2)

    def test_policy_role_and_intent_mismatches_refuse_before_runtime(self):
        for key, value in (("intent_id", "another"), ("role", "author"), ("policy_version", "another"), ("model", "different")):
            spec = self.spec("mismatch-" + key)
            spec["runner"][key] = value
            runtime = self.root / ("runtime-" + key)
            with self.assertRaises(TaskflowError):
                execution.enqueue_spec(self.store, spec, runtime)
            self.assertFalse(runtime.exists())

    def test_missing_runtime_is_uncertain_and_not_reclaimed(self):
        self.command("enqueue", {
            "work_id": "no-runtime", "task_id": "neutral", "candidate_id": "neutral-a",
            "role": "research", "policy_id": "fixture-v1", "intent_id": "no-runtime",
            "purpose": "explicit missing-runtime fixture", "request_digest": sha256(b"none"),
        })
        code, _, error = self.call("worker", "--worker-id", "worker")
        self.assertEqual(2, code)
        self.assertIn("no prepared runtime", error)
        self.assertEqual("uncertain", self.store.dashboard()["attempts"][0]["state"])
        self.assertEqual("waiting", self.call("worker", "--worker-id", "worker")[1]["state"])

    def test_direct_queue_cannot_relabel_author_runtime_as_quality(self):
        spec = self.spec("quality-intent", "quality")
        spec["runner"].update(role="author", intent_id="actual-author")
        runtime = self.root / "misbound-runtime"
        manifest = prepare_attempt(runtime, spec["runner"])
        self.store.enqueue(**spec["work"], runtime_path=str(runtime), request_digest=manifest["request"]["sha256"])
        code, _, error = self.call("worker", "--worker-id", "worker")
        self.assertEqual(2, code)
        self.assertIn("intent/role/policy", error)
        self.assertFalse((runtime / "dispatch.json").exists())
        self.assertFalse((runtime / "process.json").exists())
        self.assertEqual("uncertain", self.store.dashboard()["attempts"][0]["state"])

    def test_foreign_isolated_input_cannot_execute_for_named_candidate(self):
        # An internally prepared runtime is deliberately rebound to a foreign
        # bridge. The worker must reject before consulting or executing it.
        runtime, _ = self.enqueue("foreign-runtime", "quality")
        manifest = read_json(runtime / "manifest.json")
        manifest.update(mode="existing_isolated", isolated={
            "source_binding": {"input_digest": sha256(b"other public version")}
        })
        write_json(runtime / "manifest.json", manifest)
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = execution.main(["--db", str(self.db), "worker", "--worker-id", "worker", "--allow-model"])
        self.assertEqual(2, code)
        self.assertIn("public input differs", stderr.getvalue())
        self.assertFalse((runtime / "dispatch.json").exists())
        self.assertFalse((runtime / "process.json").exists())

    def test_native_handoff_requires_actual_candidate_public_files(self):
        candidate = self.store.get_candidate("neutral-a")
        self.assertTrue(candidate["files"])
        work = self.spec()["work"]
        manifest = {"intent_id": work["intent_id"], "role": work["role"], "policy_version": work["policy_id"],
                    "mode": "native_role", "inputs": [], "candidate_binding": {key: candidate[key] for key in
                    ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
        with self.assertRaisesRegex(TaskflowError, "captured candidate public bytes"):
            execution._check_work_binding(work, candidate, manifest)
        manifest["inputs"] = [{"source": item["path"], "sha256": item["sha256"], "kind": "public"}
                              for item in candidate["files"] if item["role"] in ("prompt", "input", "template")]
        execution._check_work_binding(work, candidate, manifest)

    def test_quality_foreign_rubric_refused_without_calling_fixture(self):
        spec = self.spec("foreign-quality", "quality")
        foreign = self.root / "foreign-rubric.md"
        foreign.write_text("A different private scoring obligation.", encoding="utf-8")
        spec["runner"]["inputs"] = [{"path": str(foreign), "kind": "teacher"}]
        runtime = self.root / "foreign-quality-runtime"
        with self.assertRaisesRegex(TaskflowError, "foreign reference/rubric"):
            execution.enqueue_spec(self.store, spec, runtime)
        self.assertFalse((runtime / "dispatch.json").exists())
        self.assertFalse((runtime / "process.json").exists())
        self.assertFalse((Path(spec["runner"]["cwd"]) / "delivery.md").exists())

    def test_same_sid_does_not_allow_facts_a_to_score_public_version_b(self):
        root = self.root / "two-stage-version-check"
        facts = perform(self.store, root, "neutral-a", "review-facts", "facts-a", {"facts.md": "Facts about version A."})
        runtime = Path(facts["runtime_path"])
        seal_facts(runtime, runtime / "artifacts/facts.md")
        other = make_package(self.root / "other")
        (other / "draft/input/prompt.md").write_text("Capacity 10; choose an allocation of 4.", encoding="utf-8")
        execution.capture_candidate(self.store, other, "neutral-b")
        with self.assertRaisesRegex(RunnerError, "candidate|version"):
            perform(self.store, root, "neutral-b", "review-score", "score-b", {"score.json": "{}"},
                    sid=facts["cli_sid"], extra={"facts_attempt": str(runtime), "resume_sid": facts["cli_sid"]})
        self.assertFalse((root / "attempts/score-b/dispatch.json").exists())
        self.assertFalse((root / "work/score-b/score.json").exists())

    def test_score_same_candidate_cannot_consume_foreign_private_rules(self):
        root = self.root / "private-version-check"
        facts = perform(self.store, root, "neutral-a", "review-facts", "facts-private-a", {"facts.md": "Facts about submitted A."})
        runtime = Path(facts["runtime_path"])
        seal_facts(runtime, runtime / "artifacts/facts.md")
        foreign = self.root / "foreign-private-rule.md"
        foreign.write_text("Foreign version B gives a different score.", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "foreign reference/rubric"):
            perform(self.store, root, "neutral-a", "review-score", "score-foreign-private", {"score.json": "6"},
                    sid=facts["cli_sid"], extra={"facts_attempt": str(runtime), "resume_sid": facts["cli_sid"],
                    "inputs": [{"path": str(foreign), "kind": "teacher"}]})
        self.assertFalse((root / "attempts/score-foreign-private/dispatch.json").exists())
        self.assertFalse((root / "work/score-foreign-private/score.json").exists())

    def test_prepared_policy_drift_and_missing_model_approval_do_not_launch(self):
        for name, change, expected in (
            ("drift", lambda manifest: manifest["model"].update(requested="another-model"), "policy"),
            ("no-approval", lambda manifest: manifest.update(mode="native_role"), "--allow-model"),
        ):
            runtime, _ = self.enqueue(name)
            manifest = read_json(runtime / "manifest.json")
            change(manifest)
            write_json(runtime / "manifest.json", manifest)
            code, _, error = self.call("worker", "--worker-id", name)
            self.assertEqual(2, code)
            self.assertIn(expected, error)
            self.assertFalse((runtime / "dispatch.json").exists())
        self.assertTrue(all(item["state"] == "uncertain" for item in self.store.dashboard()["attempts"]))

    def test_reconcile_before_start_preserves_intent_without_second_process(self):
        runtime, _ = self.enqueue()
        attempt = self.store.claim("worker")
        self.store.mark_starting(attempt["attempt_id"])
        run_attempt(runtime, fault="before_start")
        code, reconciled, error = self.call("reconcile", attempt["attempt_id"])
        self.assertEqual(0, code, error)
        self.assertEqual("uncertain", reconciled["state"])
        self.assertFalse((runtime / "process.json").exists())
        self.assertEqual("waiting", self.call("worker", "--worker-id", "worker")[1]["state"])

    def test_status_outputs_are_database_projections(self):
        path = self.root / "dashboard.json"
        code, board, error = self.call("status", "--output", str(path))
        self.assertEqual(0, code, error)
        self.assertEqual(board, read_json(path))
        text_path = self.root / "dashboard.md"
        code, text, error = self.call("status", "--format", "markdown", "--output", str(text_path))
        self.assertEqual(0, code, error)
        self.assertIn("neutral", text)
        self.assertEqual(text.strip(), text_path.read_text(encoding="utf-8").strip())
        self.assertIn("SQLite", execution.dashboard_markdown({}))

    def test_six_independent_tasks_one_paused_five_real_deliveries(self):
        for index in range(6):
            identity = f"item-{index}"
            task = identity
            self.store.register_candidate(task, identity, input_digest=sha256(identity.encode()), content_digest=sha256((identity + "all").encode()), evaluation_digest=sha256((identity + "private").encode()), evidence=[evidence_ref(self.task / "task.json", "neutral source")])
            spec = self.spec(identity)
            spec["work"].update(task_id=task, candidate_id=identity)
            execution.enqueue_spec(self.store, spec, self.root / "runtimes" / identity)
        self.store.set_control("task:item-0", "pause", "single task awaiting source evidence")
        completed = []
        for index in range(5):
            completed.append(self.call("worker", "--worker-id", f"worker-{index}")[1])
        self.assertTrue(all(item["state"] == "completed" for item in completed))
        self.assertEqual({f"item-{index}" for index in range(1, 6)}, {item["task_id"] for item in completed})
        self.assertEqual("ready", self.store.get_work("item-0")["state"])
        self.assertEqual("waiting", self.call("worker", "--worker-id", "last")[1]["state"])

    def test_simulate_full_original_records_and_same_sid_two_stage(self):
        destination = self.root / "simulation"
        code, result, error = self.call("simulate", "--output", str(destination))
        self.assertEqual(0, code, error)
        self.assertEqual(0, result["real_model_calls"])
        self.assertEqual("test_fixture", result["nature"])
        self.assertEqual({"completed"}, set(result["stages"].values()))
        self.assertTrue(result["same_review_sid"])
        self.assertEqual({"author_self_review", "unclosed_defect"}, set(result["rejected"]))
        self.assertTrue(Path(result["original_run"]["path"]).is_dir())
        self.assertTrue(Path(result["original_evaluation"]["path"]).is_dir())
        self.assertEqual("research", result["dashboard"]["decisions"][-1]["action"])

    def test_opt_in_execution_module_help(self):
        result = subprocess.run([sys.executable, "-m", "taskflow.execution", "--help"], capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("capture", result.stdout)
        self.assertIn("reconcile", result.stdout)

    def test_root_execution_help_and_legacy_init_check_remain_compatible(self):
        result = subprocess.run([sys.executable, "-m", "taskflow", "execution", "--help"], capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("capture", result.stdout)
        legacy_root = self.root / "legacy"
        result = subprocess.run([sys.executable, "-m", "taskflow", "init", "legacy-neutral", "--root", str(legacy_root)], capture_output=True, text=True, check=False)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue((legacy_root / "legacy-neutral/task.json").is_file())
        result = subprocess.run([sys.executable, "-m", "taskflow", "check", str(legacy_root / "legacy-neutral"), "--no-write"], capture_output=True, text=True, check=False)
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertEqual("failed", json.loads(result.stdout)["status"])


if __name__ == "__main__":
    unittest.main()
