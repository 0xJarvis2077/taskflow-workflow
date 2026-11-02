"""Neutral real subprocess receipts exercise signed-original score transport."""

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import errno
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
from threading import Barrier
import unittest
from unittest.mock import patch

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, read_json, write_json
from taskflow.execution_review import ingest_signed_evaluation
from taskflow import execution_review
from taskflow.execution_runner import _info, prepare_attempt, run_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.packages import freeze_task, verify_snapshot
from taskflow.records import import_run, verify_evaluation, verify_run
from tests.helpers import add_reviews, make_task
from tools.execution_simulation import fixture_policy


SID = "00000000-0000-4000-8000-000000000002"
SOLVER_SID = "00000000-0000-4000-8000-000000000001"


def now():
    return datetime.now(timezone.utc).isoformat()


class SignedEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        task = make_task(self.root)
        add_reviews(task)
        self.snapshot = Path(freeze_task(task, "fixture-v1")["path"])
        self.incoming = self.root / "incoming"
        self.incoming.mkdir()
        (self.incoming / "work/draft/input").mkdir(parents=True)
        shutil.copyfile(self.snapshot / "draft/input/prompt.md", self.incoming / "work/draft/input/prompt.md")
        (self.incoming / "work/answer.md").write_text("Choose two units.\n2 <= 3.\n", encoding="utf-8")
        self.trace(self.incoming / "trace.jsonl", SOLVER_SID)
        run_record = {"schema_version": 1, "run_id": "fixture-run", "nature": "test_fixture",
                      "actual_input_digest": read_json(self.snapshot / "manifest.json")["input_digest"],
                      "status": "completed", "requested_config": {"model": "local-fixture", "tools": []},
                      "observed_config": {"model": "local-fixture", "tools": []},
                      "started_at": now(), "ended_at": now(), "interventions": [], "exceptions": [],
                      "input_evidence": ["public#line-1"], "isolation_evidence": ["trace#fixture-only"],
                      "artifacts": [{"id": "answer", "path": "work/answer.md", "kind": "answer"},
                                    {"id": "public", "path": "work/draft/input/prompt.md", "kind": "evidence"},
                                    {"id": "trace", "path": "trace.jsonl", "kind": "trace"}]}
        write_json(self.incoming / "run.json", run_record)
        self.run = Path(import_run(self.snapshot, self.incoming / "run.json")["path"])
        self.review = self.root / "legacy-review"
        self.review.mkdir()
        self.runtime = self.root / "legacy-runtime"
        self.cwd = self.runtime / "review"
        for name in ("review/stage1-output", "review/stage2-output", "review/sealed", "scratch/empty", "teacher", "control"):
            (self.runtime / name).mkdir(parents=True)
        self.sources = []
        for source, target in (
                (self.snapshot / "draft/input/prompt.md", self.cwd / "draft/input/prompt.md"),
                (self.incoming / "work/answer.md", self.cwd / "submission/work/answer.md"),
                (self.incoming / "trace.jsonl", self.cwd / "evidence/trace.jsonl")):
            self.sources.append(self.copy_source(source, target))
        teacher = [self.copy_source(self.snapshot / item["path"], self.runtime / "teacher" / item["path"])
                   for item in read_json(self.snapshot / "task.json")["files"]]
        scoring = self.runtime / "teacher/task-scoring.json"
        write_json(scoring, {"scoring": read_json(self.snapshot / "task.json")["scoring"]})
        teacher.append({"source": str(self.snapshot / "task.json"), "destination": str(scoring),
                        "source_json_pointer": "/scoring", "source_info": _info(self.snapshot / "task.json"), **_info(scoring)})
        for name in ("rules.md", "visible.json", "stage1-request.md", "stage2-request.md"):
            (self.review / name).write_text("Neutral fixture only.\n", encoding="utf-8")
        argv = ["local-fixture", "-c", 'model_reasoning_effort="xhigh"', "exec", "-m", "gpt-6.1-sol"]
        self.plan = {"task_id": "neutral", "version_id": "fixture-v1", "submission_id": "fixture-run",
                     "root": str(self.runtime), "cwd": str(self.cwd),
                     "snapshot_digest": read_json(self.snapshot / "manifest.json")["snapshot_digest"],
                     "snapshot_manifest": {"path": str(self.snapshot / "manifest.json"), **_info(self.snapshot / "manifest.json")},
                     "sources": self.sources, "stage2_teacher": teacher,
                     "stage1_argv": argv,
                     "stage2_argv_template": [*argv, "resume", "<EXACT_NEW_STAGE1_SESSION_ID>"],
                     "request_files": {str(stage): {"path": str(self.review / f"stage{stage}-request.md"),
                                                     **_info(self.review / f"stage{stage}-request.md")} for stage in (1, 2)},
                     "neutral_rules": {"path": str(self.review / "rules.md"), **_info(self.review / "rules.md")},
                     "visible_manifest": {"path": str(self.review / "visible.json"), **_info(self.review / "visible.json")}}
        self.lock_plan()
        facts = self.cwd / "stage1-output/facts-review.md"
        facts.write_text("Actual whole submission chooses two units and checks capacity.\n", encoding="utf-8")
        self.stage(1, [facts])
        self.facts = {"nature": "stage1_facts_frozen_before_teacher_release", "sealed_at": now(),
                      **_info(facts), "thread_id": SID, "original_path": str(facts),
                      "stage1_trace": _info(self.review / "stage1-run/trace.jsonl"),
                      "stage1_request": self.plan["request_files"]["1"]}
        shutil.copyfile(facts, self.review / "stage1-facts-review.md")
        shutil.copyfile(facts, self.cwd / "sealed/facts-review.md")
        write_json(self.review / "stage1-facts-seal.json", self.facts)
        write_json(self.cwd / "sealed/facts-seal.json", self.facts)
        self.score = {"schema_version": 1, "review_stage": 2, "total_score": 7, "max_score": 7,
                      "criteria": [{"id": "C1", "max_score": 7, "score": 7,
                                    "reason": "The actual submission satisfies the frozen criterion.",
                                    "public_evidence": [{"path": "draft/input/prompt.md", "locator": "第1行"}],
                                    "submission_evidence": [{"path": "submission/work/answer.md", "locator": "1-2"}],
                                    "stage1_evidence": [{"path": "sealed/facts-review.md", "locator": "whole result"}]}]}
        self.reseal_score()
        self.authority = self.root / "authorization.json"
        write_json(self.authority, {"nature": "neutral test fixture", "scope": "transport only; no model calls"})
        self.config = {"schema_version": 1, "review_directory": str(self.review), "run_directory": str(self.run),
                       "snapshot_directory": str(self.snapshot), "evaluation_id": "fixture-original",
                       "expected_sid": SID, "expected_facts_sha256": self.facts["sha256"],
                       "expected_score_sha256": self.seal["score"]["sha256"],
                       "excluded_sids": [SOLVER_SID], "evidence": [evidence_ref(self.authority, "neutral once-only authority")]}
        self.destination = self.root / "received"

    def trace(self, target, sid):
        target.parent.mkdir(parents=True, exist_ok=True)
        code = "import json;" + ";".join("print(json.dumps(" + repr(event) + "))" for event in (
            {"type": "thread.started", "thread_id": sid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "Neutral local fixture delivery."}},
            {"type": "turn.completed"}))
        result = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)
        target.write_bytes(result.stdout)

    def copy_source(self, source, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return {"source": str(source), "destination": str(target), **_info(target)}

    def lock_plan(self):
        write_json(self.review / "launch-plan.json", self.plan)
        write_json(self.review / "launch-plan-lock.json", _info(self.review / "launch-plan.json"))

    def stage(self, number, outputs):
        root = self.review / f"stage{number}-run"
        root.mkdir(exist_ok=True)
        started = now()
        self.trace(root / "trace.jsonl", SID)
        ended = now()
        snapshots = []
        for source in outputs:
            target = root / "output-snapshots" / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            snapshots.append({"path": source.name, **_info(source), "stable_during_read": True})
        write_json(root / "attempt.json", {"stage": number, "status": "process_exited", "returncode": 0,
                   "started_at": started, "ended_at": ended, "exceptions": [], "input_readback_passed": True,
                   "output_snapshots": {"files": snapshots, "errors": []}})
        argv = self.plan["stage1_argv"] if number == 1 else [SID if x == "<EXACT_NEW_STAGE1_SESSION_ID>" else x
                                                            for x in self.plan["stage2_argv_template"]]
        write_json(root / "argv.json", {"argv": argv})
        shutil.copyfile(self.review / f"stage{number}-request.md", root / "request.md")

    def reseal_score(self):
        score = self.cwd / "stage2-output/first-score.json"
        write_json(score, self.score)
        explanation = self.cwd / "stage2-output/scoring-review.md"
        explanation.write_text("The original signed reasons remain unchanged.\n", encoding="utf-8")
        self.stage(2, [score, explanation])
        self.seal = {"nature": "first_independent_score_before_author_comparison", "sealed_at": now(),
                     "facts_sha256": self.facts["sha256"], "thread_id": SID,
                     "score": {"path": str(score), **_info(score)},
                     "explanation": {"path": str(explanation), **_info(explanation)}}
        write_json(self.review / "first-score-seal.json", self.seal)
        if hasattr(self, "config"):
            self.config["expected_score_sha256"] = self.seal["score"]["sha256"]

    def ingest(self, config=None):
        return ingest_signed_evaluation(config=config or self.config, destination=self.destination)

    def test_terminal_original_transport_preserves_reasons_full_archive_and_idempotency(self):
        outside = self.root / "outside-secret"
        outside.write_text("Not followed.\n", encoding="utf-8")
        (self.runtime / "scratch/link").symlink_to(outside)
        result = self.ingest()
        self.assertTrue(result["accepted"])
        self.assertEqual("test_fixture", result["nature"])
        self.assertEqual(7, result["score"])
        record = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(self.score, record["signed_original_score"])
        self.assertEqual(self.score["criteria"][0]["reason"], record["items"][0]["reason"])
        self.assertEqual(["public#第1行", "answer#1-2"], record["items"][0]["evidence"])
        archive = Path(result["review_archive"]["path"])
        self.assertTrue((archive / "trees/runtime/scratch/empty").is_dir())
        self.assertFalse((archive / "trees/runtime/scratch/link").exists())
        self.assertEqual("symlink", read_json(archive / "manifest.json")["snapshot"]["runtime"]["special"]["scratch/link"]["kind"])
        self.assertEqual([], verify_evaluation(Path(result["evaluation"]))["validation_issues"])
        self.assertEqual(result, self.ingest())
        self.assertEqual(1, len(list((self.run / "evaluations").glob("*/evaluation.json"))))

    def test_running_or_unsealed_score_waits_without_reading_score_or_writing(self):
        attempt = self.review / "stage2-run/attempt.json"
        original = attempt.read_bytes()
        attempt.unlink()
        with patch("taskflow.execution_review._project", side_effect=AssertionError("must not read unsealed score")):
            self.assertEqual("original_stage2_not_terminal", self.ingest()["reason"])
            attempt.write_bytes(original)
            (self.review / "first-score-seal.json").unlink()
            self.assertEqual("original_first_score_not_sealed", self.ingest()["reason"])
        self.assertFalse(self.destination.exists())

    def test_changed_score_or_wrong_sid_is_rejected(self):
        config = {**self.config, "expected_sid": SOLVER_SID}
        with self.assertRaisesRegex(TaskflowError, "SID"):
            self.ingest(config)
        Path(self.seal["score"]["path"]).write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "byte drift"):
            self.ingest()
        self.assertFalse(self.destination.exists())

    def test_facts_after_scoring_or_failed_original_terminal_is_rejected(self):
        seal = copy.deepcopy(self.facts)
        seal["sealed_at"] = "2099-01-01T00:00:00+00:00"
        write_json(self.review / "stage1-facts-seal.json", seal)
        with self.assertRaisesRegex(TaskflowError, "before original scoring"):
            self.ingest()
        write_json(self.review / "stage1-facts-seal.json", self.facts)
        attempt = read_json(self.review / "stage2-run/attempt.json")
        attempt["returncode"] = 1
        write_json(self.review / "stage2-run/attempt.json", attempt)
        with self.assertRaisesRegex(TaskflowError, "terminal/input"):
            self.ingest()

    def test_wrong_frozen_weight_or_out_of_range_locator_has_no_native_original(self):
        self.score["criteria"][0]["max_score"] = 8
        self.reseal_score()
        with self.assertRaisesRegex(TaskflowError, "criterion weight"):
            self.ingest()
        self.score["criteria"][0]["max_score"] = 7
        self.score["criteria"][0]["submission_evidence"][0]["locator"] = "99"
        self.reseal_score()
        with self.assertRaisesRegex(TaskflowError, "outside the actual source"):
            self.ingest()
        self.assertFalse((self.run / "evaluations/fixture-original").exists())

    def test_legacy_items_and_signed_aliases_use_the_same_transport(self):
        row = self.score.pop("criteria")[0]
        row.pop("public_evidence")
        row.pop("submission_evidence")
        row["public_responsibility_evidence"] = ["draft/input/prompt.md:1"]
        row["final_submission_evidence"] = ["A:1-2"]
        self.score.update(items=[row], evidence_aliases={"A": "submission/work/answer.md"})
        self.reseal_score()
        result = self.ingest()
        self.assertEqual(7, result["score"])
        record = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(self.score, record["signed_original_score"])
        self.assertEqual(["public#1", "answer#1-2"], record["items"][0]["evidence"])

    def test_reentry_rejects_raw_archive_drift_and_new_original_identity(self):
        result = self.ingest()
        raw = Path(result["review_archive"]["path"]) / "trees/runtime/scratch/new-file"
        raw.write_text("Unbound new bytes.\n", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "archived review bytes"):
            self.ingest()
        raw.unlink()
        config = {**self.config, "evaluation_id": "another-original"}
        with self.assertRaisesRegex(TaskflowError, "another original judgment"):
            ingest_signed_evaluation(config=config, destination=self.root / "different-receive")
        self.assertFalse((self.run / "evaluations/another-original").exists())

    def test_concurrent_different_ids_import_only_one_original_for_the_run(self):
        ready = Barrier(2)
        archive = execution_review._archive

        def synchronized_archive(roots, destination):
            result = archive(roots, destination)
            ready.wait(timeout=5)
            return result

        with patch("taskflow.execution_review._archive", side_effect=synchronized_archive), \
             ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(ingest_signed_evaluation,
                        config={**self.config, "evaluation_id": identity}, destination=self.root / identity)
                       for identity in ("one-original", "two-original")]
            successes, failures = [], []
            for future in futures:
                try:
                    successes.append(future.result(timeout=10))
                except TaskflowError as exc:
                    failures.append(str(exc))
        self.assertEqual(1, len(successes))
        self.assertEqual(1, len(failures))
        self.assertIn("another original judgment", failures[0])
        self.assertEqual(1, len(list((self.run / "evaluations").glob("*/evaluation.json"))))
        self.assertEqual("not_determined", successes[0]["model_validity"])

    def test_archive_score_cannot_change_between_validation_and_transport(self):
        archive = execution_review._archive

        def changed_archive(roots, destination):
            result = archive(roots, destination)
            (destination / "trees/runtime/review/stage2-output/first-score.json").write_text("{}", encoding="utf-8")
            return result

        with patch("taskflow.execution_review._archive", side_effect=changed_archive), \
             self.assertRaisesRegex(TaskflowError, "byte drift"):
            self.ingest()
        self.assertFalse((self.run / "evaluations/fixture-original").exists())
        self.assertFalse((self.destination / "receipt.json").exists())


class CleanReviewPreparationTests(unittest.TestCase):
    trace = SignedEvaluationTests.trace
    copy_source = SignedEvaluationTests.copy_source
    lock_plan = SignedEvaluationTests.lock_plan
    stage = SignedEvaluationTests.stage
    reseal_score = SignedEvaluationTests.reseal_score

    def setUp(self):
        task_factory = make_task

        def task_with_teacher_notes(*args, **kwargs):
            task = task_factory(*args, **kwargs)
            notes = task / "draft/evaluation/author-notes.md"
            notes.write_text("Teacher-side author hypotheses and previous score must stay out.\n", encoding="utf-8")
            declaration = read_json(task / "task.json")
            declaration["files"].append({"id": "author_notes", "path": "draft/evaluation/author-notes.md", "role": "teacher"})
            write_json(task / "task.json", declaration)
            return task

        with patch("tests.test_execution_review.make_task", side_effect=task_with_teacher_notes):
            SignedEvaluationTests.setUp(self)
        self.store = ExecutionStore(self.root / "review-state.sqlite3")
        policy = fixture_policy()
        for role in policy["roles"].values():
            role.update(model="gpt-6.1-sol", reasoning_effort="xhigh", timeout_seconds=3600)
        self.store.register_policy("review-fixture-v1", policy)
        from taskflow.execution import capture_candidate

        self.candidate = capture_candidate(self.store, self.snapshot, "neutral-current")
        selector = self.incoming / "indexes/first_review_index.json"
        selector.parent.mkdir()
        rows = [{"id": row["id"], "incoming_path": row["source_path"],
                 "visibility": "actual_trace" if row["id"] == "trace" else "public" if row["id"] == "public" else "submission",
                 "size": row["size"], "sha256": row["sha256"]}
                for row in read_json(self.run / "run.json")["artifacts"]]
        configuration = self.incoming / "indexes/actual-configuration.json"
        write_json(configuration, {"nature": "neutral actual-config fixture", "requested_model": "local-fixture", "served_model": None})
        rows.append({"id": "actual_configuration", "incoming_path": "indexes/actual-configuration.json",
                     "visibility": "actual_configuration", **_info(configuration)})
        write_json(selector, {"allowed_direct_artifacts": rows})
        record = read_json(self.incoming / "run.json")
        record["run_id"] = "fixture-clean-run"
        record["artifacts"].append({"id": "first_review_index", "path": "indexes/first_review_index.json", "kind": "evidence"})
        record["artifacts"].append({"id": "actual_configuration", "path": "indexes/actual-configuration.json", "kind": "trace"})
        write_json(self.incoming / "clean-run.json", record)
        self.run = Path(import_run(self.snapshot, self.incoming / "clean-run.json")["path"])
        (self.root / "AGENTS.md").write_text("Private fixture repository rules.\n", encoding="utf-8")
        self.cli = Path("/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex")
        self.runtime = self.root / "clean-facts-runtime"
        self.clean_config = {"run_directory": str(self.run), "real_repository": str(self.root),
            "first_review_index": str(selector), "excluded_sids": ["00000000-0000-4000-8000-000000000003"],
            "runtime_read_paths": ["/opt/homebrew/Cellar"],
            "work": {"work_id": "clean-facts", "task_id": "neutral", "candidate_id": "neutral-current",
                     "role": "review-facts", "policy_id": "review-fixture-v1", "intent_id": "clean-facts-intent",
                     "purpose": "neutral observed run factual review", "request_digest": "0" * 64,
                     "evidence": [evidence_ref(self.authority, "neutral test preparation authority")]},
            "runner": {"intent_id": "clean-facts-intent", "role": "review-facts", "policy_version": "review-fixture-v1",
                       "mode": "native_role", "model": "gpt-6.1-sol", "reasoning_effort": "xhigh", "timeout_seconds": 3600,
                       "cli": str(self.cli), "outputs": ["facts.json"], "inputs": [],
                       "request_sections": [{"kind": "operation", "text": "Read the entire actual submission and visible trace; save your own complete factual findings in facts.json."}],
                       "native_permissions": {"profile": "clean-review-fixture", "python": str(Path(sys.executable).resolve()),
                                              "direct_network": {"tcp": "deny", "unix": "deny"}}}}

    def fixture_facts(self, sid=SID, payload=None):
        from taskflow.execution_review import _first_inputs

        inputs, selector = _first_inputs(self.clean_config, read_json(self.run / "run.json"))
        binding = {"schema_version": 1, "run_directory": str(self.run),
                   "run_digest": read_json(self.run / "run.json")["run_digest"],
                   "candidate_binding": {key: self.candidate[key] for key in (
                       "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
                   "first_review_inputs": inputs, "selector": selector, "excluded_sids": [SOLVER_SID],
                   "public_sources": [item for item in self.candidate["files"] if item["role"] in {"input", "prompt", "template"}]}
        binding_file = self.root / "fixture-review-binding.json"
        write_json(binding_file, binding)
        cwd = self.root / "fixture-facts-work"
        cwd.mkdir()
        script = self.root / "fixture-facts.py"
        facts_data = {"observation": "whole actual answer checked"} if payload is None else payload
        script.write_text("import pathlib,json\npathlib.Path('facts.json').write_text(" + repr(json.dumps(facts_data)) + ")\n" +
                          "\n".join("print(json.dumps(" + repr(event) + "))" for event in (
                              {"type": "thread.started", "thread_id": sid},
                              {"type": "item.completed", "item": {"type": "agent_message", "text": "Complete neutral facts saved."}},
                              {"type": "turn.completed"})), encoding="utf-8")
        runtime = self.root / "fixture-facts-attempt"
        prepare_attempt(runtime, {"intent_id": "neutral-facts", "role": "review-facts", "policy_version": "fixture-v1",
            "mode": "test_fixture", "model": "local-fixture", "reasoning_effort": "fixture", "timeout_seconds": 10,
            "candidate_binding": binding["candidate_binding"], "cwd": str(cwd), "argv": [sys.executable, str(script)],
            "dependencies": [str(binding_file), str(script)], "outputs": ["facts.json"],
            "inputs": [*inputs, *[{"path": item["path"], "kind": "public", "target": item["package_path"]}
                                  for item in binding["public_sources"]]],
            "request_sections": [{"kind": "operation", "text": "Neutral factual examination fixture."}]})
        self.assertTrue(run_attempt(runtime)["technical_accepted"])
        return {"facts_attempt": str(runtime), "review_binding": str(binding_file)}

    def test_actual_collected_facts_seal_is_same_run_idempotent_and_rejects_reused_sid(self):
        config = self.fixture_facts()
        seal = execution_review.seal_existing_facts(config=config)
        self.assertEqual(SID, seal["sid"])
        self.assertEqual(seal, execution_review.seal_existing_facts(config=config))
        source = Path(config["facts_attempt"]) / "artifacts/facts.json"
        source.write_text("Changed facts.\n", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "drifted"):
            execution_review.seal_existing_facts(config=config)

    def test_solver_context_cannot_be_sealed_as_independent_facts(self):
        config = self.fixture_facts(SOLVER_SID)
        with self.assertRaisesRegex(TaskflowError, "reused an author/solver"):
            execution_review.seal_existing_facts(config=config)
        self.assertFalse((Path(config["facts_attempt"]) / "facts-seal.json").exists())

    def test_first_review_selection_cannot_be_truncated_or_relabel_control(self):
        selector = Path(self.clean_config["first_review_index"])
        value = read_json(selector)
        value["allowed_direct_artifacts"] = value["allowed_direct_artifacts"][-1:]
        write_json(selector, value)
        with self.assertRaisesRegex(TaskflowError, "byte drift"):
            execution_review.prepare_facts_runner(store=self.store, config=self.clean_config, runtime=self.runtime)
        self.assertEqual([], self.store.dashboard()["attempts"])
        self.assertFalse(self.runtime.exists())

    @unittest.skipUnless(sys.platform == "darwin", "requires the actual macOS native sandbox; calls no model")
    def test_native_prepare_actually_denies_teacher_reads_before_enqueue(self):
        self.native_probe_available()
        result = execution_review.prepare_facts_runner(store=self.store, config=self.clean_config, runtime=self.runtime)
        self.assertEqual("ready", result["work"]["state"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        manifest = read_json(self.runtime / "manifest.json")
        self.assertEqual("deny", manifest["native"]["filesystem"][str(self.root)])
        self.assertNotIn(SOLVER_SID, (self.runtime / "request.md").read_text())
        self.assertTrue(all(item["kind"] != "teacher" for item in manifest["inputs"]))
        proof = read_json(self.runtime / "review-boundary-probe.json")
        self.assertEqual(0, proof["model_calls"])
        self.assertIn(str(self.snapshot / "draft/evaluation/answer.md"), proof["forbidden_reads_denied"])
        self.assertEqual(result, execution_review.prepare_facts_runner(store=self.store, config=self.clean_config, runtime=self.runtime))

    def native_probe_available(self):
        if not self.cli.exists():
            self.skipTest("local bundled CLI unavailable")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            try:
                listener.bind(("127.0.0.1", 0))
            except OSError as exc:
                if exc.errno in (errno.EPERM, errno.EACCES):
                    self.skipTest("host disallows local probe listeners; native test requires host execution")
                raise

    def test_runtime_reads_cannot_expose_other_contexts_or_internal_metadata(self):
        for value in ("/private/tmp", "/private/tmp/unrelated-review", str(self.root),
                      str(self.runtime), str(self.runtime.parent / (self.runtime.name + "-review")),
                      "/home/neutral-reviewer/.codex"):
            with self.subTest(path=value), self.assertRaisesRegex(TaskflowError, "runtime reads cannot expose"):
                execution_review.prepare_facts_runner(store=self.store,
                    config={**self.clean_config, "runtime_read_paths": [value]}, runtime=self.runtime)
        self.assertEqual([], self.store.dashboard()["attempts"])

    @unittest.skipUnless(sys.platform == "darwin", "requires the actual macOS native sandbox; calls no model")
    def test_score_prepared_inputs_are_only_normative_plus_scoring_projection(self):
        self.native_probe_available()
        facts = self.fixture_facts()
        config = copy.deepcopy(self.clean_config)
        config.update(facts_attempt=facts["facts_attempt"], facts_binding=facts["review_binding"],
                      scoring_source_allowlist=["draft/evaluation/answer.md", "draft/evaluation/rubric.md"])
        config["work"].update(role="review-score", work_id="clean-score", intent_id="clean-score-intent")
        config["runner"].update(role="review-score", intent_id="clean-score-intent", outputs=["first-score.json"])
        runtime = self.root / "clean-score-runtime"
        result = execution_review.prepare_score_runner(store=self.store, config=config, runtime=runtime)
        manifest = read_json(runtime / "manifest.json")
        teachers = {item["source"] for item in manifest["inputs"] if item["kind"] == "teacher"}
        projection = manifest["scoring_projection"]
        self.assertEqual({str(self.snapshot / "draft/evaluation/answer.md"),
                          str(self.snapshot / "draft/evaluation/rubric.md"), projection["path"]}, teachers)
        self.assertNotIn(str(self.snapshot / "draft/evaluation/author-notes.md"), teachers)
        self.assertEqual(read_json(self.snapshot / "task.json")["scoring"], read_json(Path(projection["path"])))
        self.assertEqual(SID, manifest["resume_sid"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        bad = copy.deepcopy(manifest)
        notes = self.root / "foreign-author-notes.md"
        notes.write_text("Unrelated private author research.", encoding="utf-8")
        bad["inputs"].append({"source": str(notes), "kind": "teacher", "target": "foreign-notes.md", **_info(notes)})
        from taskflow.execution import _check_work_binding

        with self.assertRaisesRegex(TaskflowError, "foreign reference/rubric"):
            _check_work_binding(result["work"], self.candidate, bad)

    def fixture_managed_score(self, sid=SID, *, smuggle_notes=False, extra_scoring_field=False, local_assessment=False, review_aliases=False, normative_locators=False):
        facts_config = self.fixture_facts(payload={"reviewed_files": ["actual answer"], "facts": [{"correct": True}, {"omission": "none observed"}]} if review_aliases else None)
        facts = execution_review.seal_existing_facts(config=facts_config)["seal"]
        binding = read_json(Path(facts_config["review_binding"]))
        normative = [item for item in self.candidate["files"] if item["role"] in {"answer", "rubric"}]
        projection = self.root / "managed-task-scoring.json"
        scoring_data = read_json(self.snapshot / "task.json")["scoring"]
        if extra_scoring_field:
            scoring_data["author_opinion"] = "Unapproved extra content."
        write_json(projection, scoring_data)
        scoring_projection = {"source": str(self.snapshot / "task.json"), "source_sha256": _info(self.snapshot / "task.json")["sha256"],
                              "path": str(projection), "target": "normative/task-scoring.json", "sha256": _info(projection)["sha256"]}
        binding.update(facts_seal=facts, scoring_sources=normative,
                       scoring_source_allowlist=sorted(item["package_path"] for item in normative),
                       scoring_projection=scoring_projection)
        if smuggle_notes:
            binding["scoring_sources"] = [*normative, next(item for item in self.candidate["files"] if item["role"] == "teacher")]
        binding_path = self.root / "managed-score-binding.json"
        write_json(binding_path, binding)
        cwd = self.root / "managed-score-work"
        cwd.mkdir()
        score = {"schema_version": 1, "total_score": 7, "max_score": 7,
                 "criteria": [{"id": "C1", "max_score": 7, "score": 7, "reason": "The original complete answer is correct.",
                               "public_evidence": [{"path": "draft/input/prompt.md", "locator": "1"}],
                               "submission_evidence": [{"path": "original/answer/answer.md", "locator": "1-2"}]}]}
        if local_assessment or review_aliases:
            run = read_json(self.run / "run.json")
            score.update(execution_binding={"run_digest": run["run_digest"], "snapshot_digest": run["snapshot_digest"],
                                            "input_digest": run["input_digest"], "evaluation_digest": run["evaluation_digest"]},
                         experiment_validity={"local_profile": "current_registered_local_profile", "local_eligible": True,
                             "served_identity_unknown_disclosed": True, "substantive_disputes": [], "reason": "The actual local fixture configuration is recorded.",
                             "evidence": [{"path": "original/actual_configuration/actual-configuration.json", "locator": "requested_model"}]},
                         artifact_status={"artifacts_complete": True, "reason": "Both actual submitted lines are preserved.",
                             "evidence": [{"path": "original/answer/answer.md", "locator": "1-2"}]})
            if review_aliases:
                score["experiment_validity"]["local_eligible"] = "unknown"
                score["experiment_validity"]["evidence"].append({"path": "normative/task-scoring.json", "locator": "json_pointer:/criteria"})
                score["artifact_status"]["evidence"] += [
                    {"path": "sealed/facts.json", "locator": "json_pointer:/reviewed_files"},
                    {"path": "sealed/facts.json", "locator": "json_pointer:/facts/1"}]
            if normative_locators:
                score["experiment_validity"]["evidence"] += [
                    {"path": "normative/task-scoring.json", "locator": "/criteria; /total"},
                    {"path": "normative/draft/evaluation/answer.md", "locator": "line 1"},
                    {"path": "normative/draft/evaluation/rubric.md", "locator": "entire document"}]
                score["artifact_status"]["evidence"].append(
                    {"path": "sealed/facts.json", "locator": "/reviewed_files; /facts/1"})
        script = self.root / "managed-score.py"
        script.write_text("import pathlib,json\npathlib.Path('first-score.json').write_text(" + repr(json.dumps(score)) + ")\n"
                          "pathlib.Path('scoring-review.md').write_text('Original independent reasons retained.')\n" +
                          "\n".join("print(json.dumps(" + repr(event) + "))" for event in (
                              {"type": "thread.started", "thread_id": sid},
                              {"type": "item.completed", "item": {"type": "agent_message", "text": "Signed original saved."}},
                              {"type": "turn.completed"})), encoding="utf-8")
        runtime = self.root / "managed-score-attempt"
        inputs = [*binding["first_review_inputs"], *[{"path": item["path"], "kind": "public", "target": item["package_path"]}
                                                  for item in binding["public_sources"]],
                  {"path": facts["path"], "kind": "submission", "target": "sealed/facts.json"},
                  *[{"path": item["path"], "kind": "teacher", "target": ("normative/" if normative_locators else "") + item["package_path"]}
                    for item in binding["scoring_sources"]],
                  {"path": str(projection), "kind": "teacher", "target": scoring_projection["target"]}]
        prepare_attempt(runtime, {"intent_id": "managed-score", "role": "review-score", "policy_version": "fixture-v1",
            "mode": "test_fixture", "model": "local-fixture", "reasoning_effort": "fixture", "timeout_seconds": 10,
            "candidate_binding": binding["candidate_binding"], "cwd": str(cwd), "argv": [sys.executable, str(script)],
            "dependencies": [str(binding_path), str(script)], "outputs": ["first-score.json", "scoring-review.md"],
            "inputs": inputs, "request_sections": [{"kind": "operation", "text": "Neutral original scoring fixture."}],
            "facts_attempt": facts_config["facts_attempt"], "resume_sid": SID,
            "scoring_source_allowlist": binding["scoring_source_allowlist"], "scoring_projection": scoring_projection})
        collection = run_attempt(runtime)
        self.assertEqual(sid == SID, collection["technical_accepted"])
        config = {"schema_version": 1, "mode": "managed_two_stage", "facts_attempt": facts_config["facts_attempt"],
                  "facts_binding": facts_config["review_binding"], "score_attempt": str(runtime), "score_binding": str(binding_path),
                  "run_directory": str(self.run), "snapshot_directory": str(self.snapshot), "expected_sid": SID,
                  "evaluation_id": "managed-original", "evidence": [evidence_ref(self.authority, "neutral managed intake") ]}
        return config, score

    def test_managed_collected_score_preserves_original_once_and_full_archives(self):
        config, score = self.fixture_managed_score()
        result = ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertTrue(result["accepted"])
        self.assertEqual("test_fixture", result["nature"])
        self.assertEqual("not_determined", result["model_validity"])
        record = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(score, record["signed_original_score"])
        self.assertIn("local-fixture/fixture", record["evaluator"])
        self.assertNotIn("gpt-6.1-sol/xhigh", record["evaluator"])
        self.assertEqual([], verify_evaluation(Path(result["evaluation"]))["validation_issues"])
        self.assertTrue((Path(result["review_archive"]["score"]["path"]) / "first-score-seal.json").is_file())
        self.assertEqual(result, ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption"))

    def test_managed_score_mismatched_actual_sid_cannot_become_original(self):
        config, _ = self.fixture_managed_score(SOLVER_SID)
        with self.assertRaisesRegex(TaskflowError, "SID, actual run or frozen standards"):
            ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertFalse((self.run / "evaluations/managed-original").exists())

    def test_managed_score_waits_for_collection_lock_without_reading_unsealed_original(self):
        config, _ = self.fixture_managed_score()
        (Path(config["score_attempt"]) / "collection-lock.json").unlink()
        with patch("taskflow.execution_review._project", side_effect=AssertionError("not sealed")):
            result = ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertFalse(result["accepted"])
        self.assertEqual("original_core_score_not_collected", result["reason"])

    def test_observed_run_cannot_accept_fixture_facts_and_score_as_real_review(self):
        record = read_json(self.incoming / "clean-run.json")
        record.update(run_id="observed-run", nature="observed_run")
        write_json(self.incoming / "observed-run.json", record)
        self.run = Path(import_run(self.snapshot, self.incoming / "observed-run.json")["path"])
        config, _ = self.fixture_managed_score()
        with self.assertRaisesRegex(TaskflowError, "fixtures cannot sign"):
            ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertFalse((self.run / "evaluations/managed-original").exists())

    def test_intake_rechecks_frozen_allowlist_instead_of_trusting_binding_expected_sources(self):
        config, _ = self.fixture_managed_score(smuggle_notes=True)
        with self.assertRaisesRegex(TaskflowError, "foreign reference/rubric"):
            ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertFalse((self.run / "evaluations/managed-original").exists())

    def test_intake_rejects_self_consistent_projection_with_extra_author_content(self):
        config, _ = self.fixture_managed_score(extra_scoring_field=True)
        with self.assertRaisesRegex(TaskflowError, "scoring projection"):
            ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        self.assertFalse((self.run / "evaluations/managed-original").exists())

    def test_original_same_sid_local_profile_verdict_is_bound_and_preserved_without_official_claim(self):
        config, score = self.fixture_managed_score(local_assessment=True)
        result = ingest_signed_evaluation(config=config, destination=self.root / "managed-adoption")
        assessment = result["local_profile_assessment"]
        self.assertTrue(assessment["signed_binding_valid"], assessment)
        self.assertTrue(assessment["local_eligible"])
        self.assertEqual("not_determined", result["model_validity"])
        self.assertEqual("actual_configuration", assessment["evidence"][0]["artifact_id"])
        original = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(score, original["signed_original_score"])

    def test_exact_scoring_and_facts_aliases_resolve_without_changing_signed_unknown(self):
        config, score = self.fixture_managed_score(review_aliases=True)
        result = ingest_signed_evaluation(config=config, destination=self.root / "alias-adoption")
        assessment = result["local_profile_assessment"]
        self.assertTrue(assessment["signed_binding_valid"], assessment)
        self.assertEqual("unknown", assessment["local_eligible"])
        sources = [item for item in assessment["evidence"] if item.get("review_source")]
        self.assertEqual(["normative/task-scoring.json", "sealed/facts.json", "sealed/facts.json"], [item["review_source"] for item in sources])
        self.assertEqual(["/criteria", "/reviewed_files", "/facts/1"], [item["json_pointer"] for item in sources])
        self.assertTrue(all("artifact_id" not in item for item in sources))
        self.assertEqual(score, read_json(Path(result["evaluation"]) / "record.original.json")["signed_original_score"])

    def test_review_aliases_reject_other_teacher_wrong_digest_sid_and_nonexistent_pointer(self):
        config, _ = self.fixture_managed_score(review_aliases=True)
        manifest = read_json(Path(config["score_attempt"]) / "manifest.json")
        context = {"manifest": manifest, "facts_seal": read_json(Path(config["facts_attempt"]) / "facts-seal.json"),
                   "cwd": Path(config["score_attempt"]) / "inputs"}
        run, snapshot = verify_run(self.run), verify_snapshot(self.snapshot)
        self.assertIsNone(execution_review._review_source_alias("normative/author-notes.md", "/opinion", context, run, snapshot))
        self.assertIsNone(execution_review._review_source_alias("review-binding.json", "/authorization", context, run, snapshot))
        wrong = copy.deepcopy(context)
        wrong["manifest"]["scoring_projection"]["sha256"] = "f" * 64
        with self.assertRaisesRegex(TaskflowError, "exact frozen scoring"):
            execution_review._review_source_alias("normative/task-scoring.json", "/criteria", wrong, run, snapshot)
        wrong = copy.deepcopy(context)
        wrong["manifest"]["resume_sid"] = SOLVER_SID
        with self.assertRaisesRegex(TaskflowError, "same-SID sealed facts"):
            execution_review._review_source_alias("sealed/facts.json", "/facts/1", wrong, run, snapshot)
        for path, pointer in (("normative/task-scoring.json", "/missing"), ("sealed/facts.json", "/facts/99")):
            with self.subTest(path=path), self.assertRaisesRegex(TaskflowError, "does not exist"):
                execution_review._review_source_alias(path, pointer, context, run, snapshot)

    def test_semicolon_pointers_and_actual_selected_normative_preserve_original_unknown(self):
        config, score = self.fixture_managed_score(review_aliases=True, normative_locators=True)
        result = ingest_signed_evaluation(config=config, destination=self.root / "normative-adoption")
        assessment = result["local_profile_assessment"]
        self.assertTrue(assessment["signed_binding_valid"], assessment)
        self.assertEqual("unknown", assessment["local_eligible"])
        evidence = assessment["evidence"]
        self.assertIn(["/criteria", "/total"], [item.get("json_pointers") for item in evidence])
        self.assertIn(["/reviewed_files", "/facts/1"], [item.get("json_pointers") for item in evidence])
        normative = [item for item in evidence if item.get("source_type") == "selected_frozen_normative"]
        self.assertEqual(2, len(normative))
        self.assertTrue(all("artifact_id" not in item for item in normative))
        self.assertEqual(score, read_json(Path(result["evaluation"]) / "record.original.json")["signed_original_score"])

    def test_selected_normative_requires_allowlist_frozen_bytes_version_and_real_locator(self):
        config, _ = self.fixture_managed_score(review_aliases=True, normative_locators=True)
        manifest = read_json(Path(config["score_attempt"]) / "manifest.json")
        context = {"manifest": manifest, "facts_seal": read_json(Path(config["facts_attempt"]) / "facts-seal.json"),
                   "cwd": Path(config["score_attempt"]) / "inputs"}
        run, snapshot = verify_run(self.run), verify_snapshot(self.snapshot)
        path = "normative/draft/evaluation/answer.md"
        for pointer in ("/criteria; /missing", "/criteria;", "/criteria; /total/no-scalar-child"):
            with self.subTest(pointer=pointer), self.assertRaises(TaskflowError):
                execution_review._review_source_alias("normative/task-scoring.json", pointer, context, run, snapshot)
        for locator in ("lines 1-99999", "lines 2-1", "whatever the scorer intended"):
            with self.subTest(locator=locator), self.assertRaises(TaskflowError):
                execution_review._review_source_alias(path, locator, context, run, snapshot)
        wrong = copy.deepcopy(context)
        wrong["manifest"]["scoring_source_allowlist"].remove("draft/evaluation/answer.md")
        self.assertIsNone(execution_review._review_source_alias(path, "line 1", wrong, run, snapshot))
        for field, value in (("source", str(self.root / "foreign-answer.md")), ("kind", "trace")):
            wrong = copy.deepcopy(context)
            next(item for item in wrong["manifest"]["inputs"] if item["target"] == path)[field] = value
            with self.subTest(entry_field=field), self.assertRaisesRegex(TaskflowError, "same frozen version"):
                execution_review._review_source_alias(path, "line 1", wrong, run, snapshot)
        for field in ("content_digest", "input_digest", "evaluation_digest"):
            wrong = copy.deepcopy(context)
            wrong["manifest"]["candidate_binding"][field] = "f" * 64
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "same frozen version"):
                execution_review._review_source_alias(path, "line 1", wrong, run, snapshot)
        wrong_snapshot = copy.deepcopy(snapshot)
        next(item for item in wrong_snapshot["files"] if item["path"] == "draft/evaluation/answer.md")["sha256"] = "f" * 64
        with self.assertRaisesRegex(TaskflowError, "same frozen version"):
            execution_review._review_source_alias(path, "line 1", context, run, wrong_snapshot)
        target = Path(config["score_attempt"]) / "inputs" / path
        target.chmod(0o600)
        target.write_text("unsealed replacement", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "byte drift"):
            execution_review._review_source_alias(path, "line 1", context, run, snapshot)

    def test_profile_definition_selects_one_exact_multi_item_task_and_run(self):
        run, snapshot = verify_run(self.run), verify_snapshot(self.snapshot)
        policy = fixture_policy()
        policy["roles"]["solver"].update(model="local-fixture", reasoning_effort="fixture", reasoning_context="all_turns")
        self.store.register_policy("multi-profile-v1", policy)
        packet = self.root / "multi-profile-business.json"
        item = {"task_id": "neutral", "first_run_id": run["run_id"],
                "conditional_one_confirmation_run_id": "neutral-confirmation"}
        body = {"controller": "fixture-controller", "formal_baseline": {"model_requested": "local-fixture"},
                "items": [{"task_id": "other", "first_run_id": "other-run"}, item]}
        config = copy.deepcopy(self.clean_config)
        config["work"].update(policy_id="multi-profile-v1", dispatch_owner="fixture-controller")
        config["local_profile_policy"] = {"schema_version": 1, "scope": "local_fixed_client_profile",
            "allow_served_identity_unknown": True, "required_model": "local-fixture",
            "required_client_reasoning": {"effort": "fixture", "context": "all_turns"},
            "requires_frozen_input_and_reference_match": True, "requires_complete_capture": True,
            "requires_no_substantive_dispute": True, "official_provider_identity_claimed": False}

        def authorize(value):
            write_json(packet, value)
            ref = evidence_ref(packet, "multi-item exact authority")
            config["work"]["evidence"] = [ref]
            config["local_profile_policy"]["authorization"] = ref

        authorize(body)
        result = execution_review._profile_definition(self.store, config, run, snapshot, self.candidate, self.root / "multi-profile")
        self.assertEqual(run["run_digest"], read_json(Path(result["path"]))["execution_binding"]["run_digest"])
        for wrong in ({**body, "items": [item, item]},
                      {**body, "items": [{**item, "task_id": "other"}]},
                      {**body, "items": [{**item, "first_run_id": "other-run"}]}):
            authorize(wrong)
            with self.assertRaisesRegex(TaskflowError, "one exact authorized"):
                execution_review._profile_definition(self.store, config, run, snapshot, self.candidate, self.root / "rejected-multi")

    def test_explicit_profile_definition_is_sanitized_bound_and_has_no_default_permission(self):
        target = self.root / "clean-policy"
        run, snapshot = verify_run(self.run), verify_snapshot(self.snapshot)
        self.assertIsNone(execution_review._profile_definition(self.store, self.clean_config, run, snapshot, self.candidate, target))
        self.assertFalse(target.exists())
        policy = fixture_policy()
        policy["roles"]["solver"].update(model="local-fixture", reasoning_effort="fixture", reasoning_context="all_turns")
        self.store.register_policy("local-profile-v1", policy)
        packet = self.root / "profile-business-packet.json"
        write_json(packet, {"controller": "fixture-controller", "task_id": "neutral", "formal": {"first_run_id": run["run_id"], "model": "local-fixture"},
                            "business_investment": "BUSINESS_SECRET", "old_score": 98})
        authority = evidence_ref(packet, "explicit local profile authority")
        config = copy.deepcopy(self.clean_config)
        config["work"].update(policy_id="local-profile-v1", dispatch_owner="fixture-controller", evidence=[authority])
        config["local_profile_policy"] = {"schema_version": 1, "authorization": authority, "scope": "local_fixed_client_profile",
            "allow_served_identity_unknown": True, "required_model": "local-fixture", "required_client_reasoning": {"effort": "fixture", "context": "all_turns"},
            "requires_frozen_input_and_reference_match": True, "requires_complete_capture": True,
            "requires_no_substantive_dispute": True, "official_provider_identity_claimed": False,
            "experiment_purpose": "PURPOSE_SECRET", "score_threshold": 50, "author_notes": "AUTHOR_SECRET"}
        ref = execution_review._profile_definition(self.store, config, run, snapshot, self.candidate, target)
        document = read_json(Path(ref["path"]))
        raw = json.dumps(document)
        for secret in ("BUSINESS_SECRET", "PURPOSE_SECRET", "AUTHOR_SECRET", str(packet), "authorization", "score_threshold", "old_score"):
            self.assertNotIn(secret, raw)
        self.assertEqual(run["run_digest"], document["execution_binding"]["run_digest"])
        self.assertEqual("actual_configuration", document["actual_configuration"]["id"])
        self.assertFalse(document["requirements"]["official_provider_identity_claimed"])
        self.assertIn("scoring fact", document["judgment"]["input_reference_match"])
        self.assertIn("not whether every business delivery", document["judgment"]["artifact_completeness"])
        self.assertEqual("local-profile-definition.json", ref["target"])
        context = {"manifest": {"inputs": [{"source": ref["path"], "target": ref["target"], "kind": "trace",
                                            **{key: ref[key] for key in ("sha256", "size")}}]},
                   "cwd": target, "local_profile_definition": ref}
        alias = execution_review._review_source_alias(ref["target"], "/scope; /execution_binding; /requirements", context, run, snapshot)
        self.assertEqual(["/scope", "/execution_binding", "/requirements"], alias["json_pointers"])
        self.assertNotIn("artifact_id", alias)
        with self.assertRaisesRegex(TaskflowError, "does not exist"):
            execution_review._review_source_alias(ref["target"], "/scope; /nonexistent", context, run, snapshot)
        bad = copy.deepcopy(config)
        bad["local_profile_policy"]["required_model"] = "another-model"
        with self.assertRaisesRegex(TaskflowError, "registered target"):
            execution_review._profile_definition(self.store, bad, run, snapshot, self.candidate, self.root / "bad-policy")


if __name__ == "__main__":
    unittest.main()
