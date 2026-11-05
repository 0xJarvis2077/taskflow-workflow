"""Real neutral subprocess tests; never call a model or native permission service."""

import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

from taskflow import execution_runner as runner


SID = "12345678-1234-4567-8123-123456789012"


class ExecutionRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.work = self.base / "work"
        self.work.mkdir()
        self.public = self.base / "source.md"
        self.public.write_text("Neutral public facts.\n", encoding="utf-8")
        self.script = self.base / "fake.py"
        self.script.write_text(
            "import json,sys,time\n"
            "from pathlib import Path\n"
            "mode=sys.argv[1]\n"
            "request=sys.stdin.read()\n"
            "Path('seen.txt').write_text(request)\n"
            "if mode=='slow':time.sleep(.2)\n"
            "if mode=='timeout':time.sleep(5)\n"
            "print(json.dumps({'type':'thread.started','thread_id':'" + SID + "'}),flush=True)\n"
            "if mode!='no-file':Path('answer.md').write_text('An actual delivery.')\n"
            "if mode!='no-response':\n"
            " print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Delivered.'}}),flush=True)\n"
            "if mode!='no-turn':print(json.dumps({'type':'turn.completed'}),flush=True)\n",
            encoding="utf-8")
        self.config = {"intent_id": "intent-1", "policy_version": "policy-v1",
                       "mode": "test_fixture", "role": "author", "model": "neutral",
                       "reasoning_effort": "high", "cwd": str(self.work),
                       "argv": [sys.executable, str(self.script), "okay"],
                       "dependencies": [str(self.script)], "outputs": ["answer.md"],
                       "inputs": [{"path": str(self.public), "kind": "public"}],
                       "request_sections": [{"kind": "operation", "text": "Produce answer.md."}]}
        self.root = self.base / "attempt"

    def prepare(self, **changes) -> dict:
        config = {**self.config, **changes}
        if config["mode"] == "native_role" and "native_permissions" not in changes:
            repository, scratch = self.base / "project", self.base / "scratch"
            repository.mkdir(exist_ok=True)
            scratch.mkdir(exist_ok=True)
            readonly = repository / "readonly.md"
            readonly.write_text("Neutral read-only project fixture")
            cwd = str(Path(config["cwd"]).resolve())
            config["native_permissions"] = {
                "profile": "neutral-native", "repository": str(repository), "scratch": str(scratch),
                "python": str(Path(sys.executable).resolve()), "probe_sources": [str(readonly)],
                "readonly_probe": str(readonly), "filesystem": {":root": "deny", ":minimal": "read",
                  str(repository): "read", cwd: "write", str(scratch): "write"}}
        return runner.prepare_attempt(self.root, config)

    def test_real_process_request_delivery_and_immutable_archive(self) -> None:
        manifest = self.prepare()
        self.public.write_text("Source changed later", encoding="utf-8")
        observed = []
        result = runner.run_attempt(self.root, on_process=observed.append)
        self.assertEqual(observed[0]["intent_id"], "intent-1")
        self.assertGreater(observed[0]["pid"], 0)
        self.assertTrue(result["technical_accepted"])
        self.assertEqual(result["nature"], "test_fixture")
        self.assertEqual(result["business_quality"], "unresolved")
        self.assertEqual((self.root / "artifacts/answer.md").read_text(), "An actual delivery.")
        self.assertIn(str(self.root / "inputs/0000-source.md"), (self.work / "seen.txt").read_text())
        self.assertEqual((self.root / "inputs/0000-source.md").read_text(), "Neutral public facts.\n")
        self.assertIsNone(manifest["model"]["served"])
        destination = self.base / "archive"
        interrupted = runner.archive_attempt(self.root, destination, fault="archiving")
        self.assertFalse(interrupted["archived"])
        self.assertFalse(destination.exists())
        self.assertTrue(runner.archive_attempt(self.root, destination)["archived"])
        self.assertTrue(runner.archive_attempt(self.root, destination)["archived"])
        (destination / "archive-binding.json").write_text('{}')
        with self.assertRaises(runner.RunnerError):
            runner.archive_attempt(self.root, destination)

    def test_exit_zero_without_turn_response_or_delivery_is_incomplete(self) -> None:
        for index, mode in enumerate(("no-turn", "no-response", "no-file")):
            with self.subTest(mode=mode):
                self.root = self.base / f"attempt-{index}"
                (self.work / "answer.md").unlink(missing_ok=True)
                self.prepare(argv=[sys.executable, str(self.script), mode])
                result = runner.run_attempt(self.root)
                self.assertEqual(result["termination"]["returncode"], 0)
                self.assertFalse(result["technical_accepted"])
                self.assertEqual(result["status"], "incomplete")

    def test_before_start_occupied_intent_remains_uncertain(self) -> None:
        self.prepare()
        result = runner.run_attempt(self.root, fault="before_start")
        self.assertEqual(result["status"], "uncertain")
        self.assertFalse((self.work / "seen.txt").exists())
        with self.assertRaises(FileExistsError):
            runner.run_attempt(self.root)
        with self.assertRaises(runner.RunnerError):
            runner.archive_attempt(self.root, self.base / "archive")

    def test_parent_loses_pid_receipt_supervisor_finishes_without_reinvocation(self) -> None:
        self.prepare(argv=[sys.executable, str(self.script), "slow"])
        self.assertEqual(runner.run_attempt(self.root, fault="before_pid")["status"], "uncertain")
        deadline = time.monotonic() + 5
        while not (self.root / "termination.json").exists() and time.monotonic() < deadline:
            time.sleep(.02)
        result = runner.reconcile_attempt(self.root)
        self.assertTrue(result["technical_accepted"])
        self.assertTrue((self.root / "supervisor.json").exists())
        self.assertFalse((self.root / "launcher.json").exists())
        with self.assertRaises(FileExistsError):
            runner.run_attempt(self.root)

    def test_collection_resume_does_not_run_again(self) -> None:
        self.prepare()
        result = runner.run_attempt(self.root, fault="collecting")
        self.assertEqual(result["status"], "collecting")
        trace = (self.root / "trace.jsonl").read_bytes()
        result = runner.reconcile_attempt(self.root)
        self.assertTrue(result["technical_accepted"])
        self.assertEqual((self.root / "trace.jsonl").read_bytes(), trace)
        self.assertEqual(runner.reconcile_attempt(self.root), result)

    def test_input_request_and_dependency_drift_reject(self) -> None:
        for index, name in enumerate(("request", "input", "dependency", "manifest")):
            with self.subTest(name=name):
                self.root = self.base / f"drift-{index}"
                self.prepare()
                target = {"request": self.root / "request.md", "input": self.root / "inputs/0000-source.md",
                          "dependency": self.script, "manifest": self.root / "manifest.json"}[name]
                old = target.read_bytes()
                target.chmod(0o644)
                target.write_bytes(old + b" ")
                with self.assertRaises(runner.RunnerError):
                    runner.run_attempt(self.root)
                target.write_bytes(old)

    def test_blind_roles_reject_teacher_targets_history_and_arbitrary_injection(self) -> None:
        for role in ("solver", "review-facts"):
            for kind in ("teacher", "reference", "target", "history_score"):
                with self.subTest(role=role, kind=kind):
                    with self.assertRaises(runner.RunnerError):
                        self.prepare(role=role, request_sections=[{"kind": kind, "text": "hidden"}])
                    self.assertFalse(self.root.exists())
            with self.assertRaises(runner.RunnerError):
                self.prepare(role=role, request_text="untyped text")
        with self.assertRaises(runner.RunnerError):
            self.prepare(role="solver", mode="native_role")

    def test_teacher_roles_can_receive_teacher_materials(self) -> None:
        self.prepare(role="quality", inputs=[{"path": str(self.public), "kind": "teacher"}],
                     request_text="Evaluate actual inputs without assuming their conclusions.")
        self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])

    def test_two_stage_facts_seal_order_same_sid_and_immutable_facts(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.prepare(role="review-score", facts_attempt=str(self.base / "absent"), resume_sid=SID)
        self.prepare(role="review-facts")
        runner.run_attempt(self.root)
        facts = self.root / "artifacts/answer.md"
        seal = runner.seal_facts(self.root, facts)
        self.assertEqual(seal["sid"], SID)
        first = self.root
        self.root = self.base / "score-attempt"
        score_work = self.base / "score-work"
        score_work.mkdir()
        manifest = self.prepare(role="review-score", facts_attempt=str(first), resume_sid=SID, cwd=str(score_work),
                                inputs=[{"path": str(self.public), "kind": "teacher"}])
        self.assertEqual(manifest["facts_seal"]["facts"], seal["facts"])
        self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])
        self.root = self.base / "bad-score"
        with self.assertRaises(runner.RunnerError):
            self.prepare(role="review-score", facts_attempt=str(first), resume_sid="wrong")
        facts.write_text("Changed facts")
        with self.assertRaises(runner.RunnerError):
            self.prepare(role="review-score", facts_attempt=str(first), resume_sid=SID)

    def test_timeout_is_environment_failure_not_low_score(self) -> None:
        self.prepare(argv=[sys.executable, str(self.script), "timeout"], timeout_seconds=.05)
        result = runner.run_attempt(self.root)
        self.assertTrue(result["termination"]["timed_out"])
        self.assertFalse(result["technical_accepted"])
        self.assertEqual(result["formal_experiment_validity"], "unresolved")
        self.assertNotIn("score", result)

    def test_missing_executable_preserves_failed_attempt(self) -> None:
        self.prepare()
        # Failure at actual spawn cannot be manufactured by changing locked argv;
        # instead exercise the supervisor's real OSError path with a nonexecutable file.
        self.root = self.base / "not-executable"
        binary = self.base / "binary"
        binary.write_text("not executable")
        self.prepare(argv=[str(binary)])
        result = runner.run_attempt(self.root)
        self.assertEqual(result["termination"]["exception"]["type"], "PermissionError")
        self.assertFalse(result["technical_accepted"])

    def test_paths_outputs_selection_and_symlinks_are_validated(self) -> None:
        for change in ({"model": "default"}, {"policy_version": ""}, {"timeout_seconds": False},
                       {"role": "unknown"}, {"mode": "unknown"}, {"outputs": ["../bad"]}):
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                self.prepare(**change)
            if self.root.exists():
                self.root = self.base / ("next-" + str(len(list(self.base.iterdir()))))
        link = self.base / "link"
        link.symlink_to(self.public)
        with self.assertRaises(runner.RunnerError):
            self.prepare(inputs=[{"path": str(link), "kind": "public"}])

    def test_native_role_catalog_and_generated_fresh_cli(self) -> None:
        evidence = {"model_calls": 0, "commands": [
            {"argv": [str(self.script), "--version"], "returncode": 0, "stdout": "fixture"},
            {"argv": [str(self.script), "--no-daemon", "debug", "models", "--bundled"],
             "returncode": 0, "stdout": json.dumps({"models": [{"slug": "neutral",
                  "supported_reasoning_levels": [{"effort": "high"}]}]})}]}
        def capture(root, cli):
            runner._publish(root / "cli-evidence.json", evidence)
            return evidence
        with mock.patch("tools.isolated_quality.capture_cli_evidence", side_effect=capture):
            manifest = self.prepare(mode="native_role", cli=str(self.script))
        self.assertIn("--ephemeral", manifest["argv"])
        self.assertIn("--ignore-user-config", manifest["argv"])
        self.assertIn('model_reasoning_effort="high"', manifest["argv"])
        self.assertIsNone(manifest["model"]["served"])

    def test_existing_solver_requires_probe_and_revalidates_bridge(self) -> None:
        runtime = self.base / "isolated"
        runtime.mkdir()
        work = runtime / "work"
        work.mkdir()
        probe = runtime / "probe.json"
        probe.write_text('{"passed":false}')
        (runtime / "source-lock.json").write_text('{}')
        ready = {"probe_file": "probe.json", "probe": runner._info(probe),
                 "source_lock": runner._info(runtime / "source-lock.json")}
        (runtime / "ready.json").write_text(json.dumps(ready))
        plan = {"model": {"requested": "neutral", "reasoning_effort": "high"}}
        (runtime / "plan.json").write_text(json.dumps(plan))
        bridge = {"runtime": str(runtime)}
        with self.assertRaises(runner.RunnerError):
            self.prepare(mode="existing_isolated", role="solver", isolated=bridge, cwd=str(work))
        self.root = self.base / "solver-ready"
        probe.write_text('{"passed":true}')
        ready["probe"] = runner._info(probe)
        (runtime / "ready.json").write_text(json.dumps(ready))
        for name in ("plan-lock.json", "prepared-lock.json"):
            (runtime / name).write_text('{}')
        (runtime / "request.md").write_text("Actual neutral solver request")
        (runtime / "runner.py").write_text("raise SystemExit(1)")
        manifest = self.prepare(mode="existing_isolated", role="solver", isolated=bridge, cwd=str(work))
        self.assertEqual(manifest["argv"][1:], [str(runtime / "runner.py"), "execute", str(runtime)])
        with self.assertRaises(runner.RunnerError):
            runner.run_attempt(self.root)

    def test_existing_delivery_is_not_accepted_as_this_attempt(self) -> None:
        (self.work / "answer.md").write_text("Unrelated earlier answer")
        with self.assertRaises(runner.RunnerError):
            self.prepare()

    def test_running_receipt_and_unknown_identity_are_distinct(self) -> None:
        self.prepare(argv=[sys.executable, str(self.script), "slow"])
        runner.run_attempt(self.root, fault="after_start")
        deadline = time.monotonic() + 5
        while not (self.root / "process.json").exists() and time.monotonic() < deadline:
            time.sleep(.01)
        observed = {"start_identity": "start"}
        with mock.patch.object(runner, "_identity", return_value=observed):
            receipt = json.loads((self.root / "process.json").read_text())
            receipt["start_identity"] = "start"
            (self.root / "process.json").write_text(json.dumps(receipt))
            self.assertEqual(runner.reconcile_attempt(self.root)["status"], "running")
        with mock.patch.object(runner, "_identity", return_value={"start_identity": None}):
            self.assertEqual(runner.reconcile_attempt(self.root)["status"], "uncertain")
        while not (self.root / "termination.json").exists() and time.monotonic() < deadline:
            time.sleep(.01)
        runner.reconcile_attempt(self.root)

    def test_collection_rejects_symlink_and_partial_archive_drift(self) -> None:
        self.prepare()
        runner.run_attempt(self.root, fault="collecting")
        answer = self.work / "answer.md"
        answer.unlink()
        answer.symlink_to(self.public)
        result = runner.reconcile_attempt(self.root)
        self.assertFalse(result["delivery_complete"])
        archive = self.base / "archive"
        runner.archive_attempt(self.root, archive, fault="archiving")
        (archive.with_name("archive.staging") / "trace.jsonl").write_text("changed")
        with self.assertRaises(runner.RunnerError):
            runner.archive_attempt(self.root, archive)

    def test_formal_bridge_preserves_plan_and_checks_actual_readback(self) -> None:
        runtime = self.base / "formal-runtime"
        runtime.mkdir()
        plan = {"cwd": str(self.work), "requested_model": "neutral",
                "argv": ["codex", "-c", 'model_reasoning_effort="high"'],
                "launcher_python": sys.executable}
        for name in ("runtime_gate.py", "file_evidence.py", "source-lock.json", "baseline-plan.json"):
            (runtime / name).write_text("{}")
        for name in ("shell-probes.json", "exec-context.json"):
            (runtime / name).write_text('{"passed":true}')
        (runtime / "request.md").write_text("Actual isolated public request")
        plan.update(source_lock_sha256=runner._info(runtime / "source-lock.json")["sha256"],
                    input_digest="neutral-input-digest", timeout_seconds=3600,
                    frozen_binding={"taskflow_input_digest": "neutral-input-digest"},
                    request_sha256=runner._info(runtime / "request.md")["sha256"],
                    probe_evidence_hashes={name: runner._info(runtime / name)["sha256"]
                                           for name in ("shell-probes.json", "exec-context.json")})
        (runtime / "execution-plan.json").write_text(json.dumps(plan))
        plan_sha = runner._info(runtime / "execution-plan.json")["sha256"]
        (runtime / "plan-lock.json").write_text(json.dumps({"sha256": plan_sha}))
        (runtime / "launch-review.json").write_text(json.dumps({"approved": True, "reviewer": "neutral-reviewer",
                                  "plan_sha256": plan_sha, "input_digest": "neutral-input-digest"}))
        script = ("import sys,json\nfrom pathlib import Path\n"
                  "r=Path(__file__).parent\n"
                  "if '--execute' in sys.argv:\n"
                  " (r/'real-run').mkdir()\n"
                  " events=[{'type':'thread.started','thread_id':'" + SID + "'},"
                  "{'type':'item.completed','item':{'type':'agent_message','text':'Done.'}},"
                  "{'type':'turn.completed'}]\n"
                  " (r/'real-run/trace.jsonl').write_text('\\n'.join(json.dumps(e) for e in events))\n"
                  " (r/'real-run/attempt.json').write_text(json.dumps({'status':'process_exited','returncode':0,'exceptions':[]}))\n"
                  " (r/'post-run-input-readback.json').write_text(json.dumps({'matches':True}))\n"
                  " Path('answer.md').write_text('Formal fixture delivery')\n")
        (runtime / "execute_diagnostic.py").write_text(script)
        bridge = {"runtime": str(runtime), "flavor": "formal"}
        original = {name: (runtime / name).read_bytes() for name in
                    ("execution-plan.json", "plan-lock.json", "launch-review.json", "shell-probes.json")}
        for index, failure in enumerate(("probe", "approval", "binding", "reasoning", "model")):
            self.root = self.base / f"bad-formal-{index}"
            if failure == "probe":
                (runtime / "shell-probes.json").write_text('{"passed":false}')
            elif failure == "approval":
                (runtime / "launch-review.json").write_text('{"approved":false}')
            else:
                altered = json.loads(original["execution-plan.json"])
                if failure == "binding":
                    altered["frozen_binding"] = {"taskflow_input_digest": "other-version"}
                elif failure == "reasoning":
                    altered["argv"] = ["codex"]
                else:
                    altered["requested_model"] = "other-model"
                (runtime / "execution-plan.json").write_text(json.dumps(altered))
                changed_sha = runner._info(runtime / "execution-plan.json")["sha256"]
                (runtime / "plan-lock.json").write_text(json.dumps({"sha256": changed_sha}))
                review = json.loads(original["launch-review.json"])
                review["plan_sha256"] = changed_sha
                (runtime / "launch-review.json").write_text(json.dumps(review))
            with self.subTest(failure=failure), self.assertRaises(runner.RunnerError):
                self.prepare(mode="existing_isolated", role="solver", isolated=bridge)
            for name, data in original.items():
                (runtime / name).write_bytes(data)
        self.root = self.base / "valid-formal"
        manifest = self.prepare(mode="existing_isolated", role="solver", isolated=bridge)
        self.assertEqual(manifest["argv"][-1], "--execute")
        result = runner.run_attempt(self.root)
        self.assertTrue(result["technical_accepted"])
        self.assertEqual(result["formal_experiment_validity"], "unresolved")
        self.assertEqual((self.root / "artifacts/answer.md").read_text(), "Formal fixture delivery")

    def test_formal_bridge_rejects_implicit_reasoning(self) -> None:
        # Test the requested-model contract before any external execute command.
        runtime = self.base / "formal-runtime"
        runtime.mkdir()
        plan = {"cwd": str(self.work), "requested_model": "neutral", "argv": ["codex"]}
        (runtime / "execution-plan.json").write_text(json.dumps(plan))
        (runtime / "plan-lock.json").write_text('{"sha256":"changed"}')
        with self.assertRaisesRegex(runner.RunnerError, "plan digest"):
            self.prepare(mode="existing_isolated", role="solver",
                         isolated={"runtime": str(runtime), "flavor": "formal"})

    def test_native_review_command_is_resumable_and_same_sid(self) -> None:
        evidence = {"model_calls": 0, "commands": [
            {"argv": [str(self.script), "--version"], "returncode": 0, "stdout": "fixture"},
            {"argv": [str(self.script), "--no-daemon", "debug", "models", "--bundled"],
             "returncode": 0, "stdout": json.dumps({"models": [{"slug": "neutral",
                  "supported_reasoning_levels": [{"effort": "high"}]}]})}]}
        def capture(root, cli):
            runner._publish(root / "cli-evidence.json", evidence)
            return evidence
        with mock.patch("tools.isolated_quality.capture_cli_evidence", side_effect=capture):
            manifest = self.prepare(mode="native_role", role="review-facts", cli=str(self.script))
        self.assertNotIn("--ephemeral", manifest["argv"])
        # Use a real neutral facts attempt to exercise the resume command gate.
        self.root = self.base / "real-facts"
        self.prepare(role="review-facts")
        runner.run_attempt(self.root)
        runner.seal_facts(self.root, self.root / "artifacts/answer.md")
        first = self.root
        self.root = self.base / "native-score"
        work = self.base / "score-work"
        work.mkdir()
        with mock.patch("tools.isolated_quality.capture_cli_evidence", side_effect=capture):
            manifest = self.prepare(mode="native_role", role="review-score", cwd=str(work),
                                    cli=str(self.script), facts_attempt=str(first), resume_sid=SID)
        self.assertIn("resume", manifest["argv"])
        self.assertIn(SID, manifest["argv"])
        self.assertIn("--ignore-user-config", manifest["argv"])

    def test_collection_partial_bytes_resume_and_source_drift_refuses(self) -> None:
        self.prepare()
        result = runner.run_attempt(self.root, fault="collecting")
        self.assertEqual(result["status"], "collecting")
        self.assertTrue((self.root / "collection-sources.json").exists())
        partial = self.root / "artifacts/.capture-interrupted"
        self.assertTrue(partial.exists())
        self.assertFalse((self.root / "artifacts/answer.md").exists())
        (self.work / "answer.md").write_text("Changed after interruption")
        result = runner.reconcile_attempt(self.root)
        self.assertFalse(result["technical_accepted"])
        self.assertIn("changed after interrupted", result["artifact_errors"][0]["error"])
        self.assertFalse((self.root / "artifacts/answer.md").exists())
        self.assertFalse(partial.exists())

    def test_archive_partial_copy_resumes_from_bound_original(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / "archive"
        result = runner.archive_attempt(self.root, target, fault="archiving")
        self.assertFalse(result["archived"])
        stage = target.with_name(target.name + ".staging")
        self.assertTrue((stage / "archive-binding.json").exists())
        self.assertTrue(list(stage.rglob(".capture-interrupted")))
        self.assertLess(len([p for p in stage.rglob('*') if p.is_file()]), len(result["files"]))
        runner.archive_attempt(self.root, target)
        self.assertFalse(list(target.rglob(".capture-*")))
        self.assertEqual((target / "trace.jsonl").read_bytes(), (self.root / "trace.jsonl").read_bytes())

    def test_large_executable_dependency_is_streamed_not_input_limited(self) -> None:
        dependency = self.base / "large-runtime"
        with dependency.open('wb') as handle:
            handle.truncate(17 * 1024 * 1024)
        manifest = self.prepare(dependencies=[str(self.script), str(dependency)])
        self.assertEqual(manifest["dependencies"][str(dependency)]["size"], 17 * 1024 * 1024)
        self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])

    def test_facts_cannot_seal_external_or_later_uncollected_text(self) -> None:
        self.prepare(role="review-facts")
        runner.run_attempt(self.root)
        external = self.base / "replacement-facts.md"
        external.write_text("Replacement facts, never delivered by the CLI")
        with self.assertRaisesRegex(runner.RunnerError, "actual collected"):
            runner.seal_facts(self.root, external)
        later = self.root / "artifacts/later-facts.md"
        later.write_text("Added after collection")
        with self.assertRaises(runner.RunnerError):
            runner.seal_facts(self.root, later)
        self.assertFalse((self.root / "facts-seal.json").exists())

    def test_artifact_or_trace_drift_rejects_cached_success_and_archive(self) -> None:
        self.prepare()
        original = runner.run_attempt(self.root)
        self.assertTrue(original["technical_accepted"])
        artifact = self.root / "artifacts/answer.md"
        saved_artifact = artifact.read_bytes()
        artifact.write_text("Changed after completed collection")
        with self.assertRaisesRegex(runner.RunnerError, "drifted"):
            runner.reconcile_attempt(self.root)
        with self.assertRaises(runner.RunnerError):
            runner.archive_attempt(self.root, self.base / "archive")
        self.assertFalse((self.base / "archive").exists())
        self.assertEqual(json.loads((self.root / "collection.json").read_text()), original)
        artifact.write_bytes(saved_artifact)
        trace = self.root / "trace.jsonl"
        trace.write_text('{"type":"turn.failed"}\n')
        with self.assertRaisesRegex(runner.RunnerError, "drifted"):
            runner.reconcile_attempt(self.root)
        with self.assertRaises(runner.RunnerError):
            runner.archive_attempt(self.root, self.base / "archive")

    def test_collection_conclusion_tampering_is_not_used_as_cached_acceptance(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        result = json.loads((self.root / "collection.json").read_text())
        result["artifacts"]["answer.md"]["sha256"] = "arbitrary-replacement"
        (self.root / "collection.json").write_text(json.dumps(result))
        with self.assertRaisesRegex(runner.RunnerError, "immutable digest"):
            runner.reconcile_attempt(self.root)

    def test_proven_cli_exec_failure_is_terminal_evidence_not_ambiguous_launch(self) -> None:
        binary = self.base / "nonexecutable"
        binary.write_text("Cannot exec")
        self.prepare(argv=[str(binary)])
        result = runner.run_attempt(self.root)
        self.assertEqual(result["status"], "not_started")
        self.assertEqual(result["process_state"], "not_started")
        self.assertEqual(result["termination"]["spawn_failure_origin"], "cli_popen")
        self.assertTrue(result["process_ended"])
        self.assertFalse(result["technical_accepted"])
        self.assertFalse((self.root / "process.json").exists())
        self.assertEqual(runner.reconcile_attempt(self.root), result)
        with self.assertRaises(FileExistsError):
            runner.run_attempt(self.root)

    def test_proven_supervisor_exec_failure_is_distinct_from_unobserved_window(self) -> None:
        self.prepare()
        with mock.patch.object(runner.subprocess, "Popen", side_effect=PermissionError("blocked before exec")):
            result = runner.run_attempt(self.root)
        self.assertEqual(result["process_state"], "not_started")
        self.assertEqual(result["termination"]["spawn_failure_origin"], "supervisor_popen")
        self.assertFalse((self.root / "process.json").exists())
        self.assertFalse((self.root / "supervisor.json").exists())

    def test_no_receipt_or_old_failure_receipt_does_not_prove_absence(self) -> None:
        self.prepare()
        runner.run_attempt(self.root, fault="before_start")
        result = runner.reconcile_attempt(self.root)
        self.assertEqual(result["status"], "uncertain")
        self.assertNotIn("process_state", result)
        self.assertFalse(result["process_ended"])

    def test_legacy_oserror_without_proven_spawn_origin_stays_uncertain(self) -> None:
        self.prepare()
        runner.run_attempt(self.root, fault="before_start")
        runner._publish(self.root / "termination.json", {"intent_id": "intent-1", "returncode": None,
                         "exception": {"type": "PermissionError", "message": "unknown phase"}, "timed_out": False})
        result = runner.reconcile_attempt(self.root)
        self.assertEqual(result["status"], "uncertain")
        self.assertFalse(result["process_ended"])
        self.assertFalse((self.root / "collection.json").exists())

    def test_conflicting_process_receipt_cannot_claim_not_started(self) -> None:
        self.prepare()
        runner.run_attempt(self.root, fault="before_start")
        runner._publish(self.root / "process.json", {"pid": 123, "intent_id": "intent-1"})
        runner._publish(self.root / "termination.json", {"intent_id": "intent-1", "returncode": None,
                         "exception": {"type": "PermissionError", "message": "conflicting note"},
                         "spawn_state": "not_started", "spawn_failure_origin": "cli_popen", "timed_out": False})
        with self.assertRaisesRegex(runner.RunnerError, "Conflicting evidence"):
            runner.reconcile_attempt(self.root)

    def test_fifo_directory_and_device_reads_reject_with_bounded_subprocesses(self) -> None:
        fifo = self.base / "no-writer-fifo"
        os.mkfifo(fifo)
        directory = self.base / "artifact-directory"
        directory.mkdir()
        code = (
            "import sys\nfrom pathlib import Path\n"
            "from taskflow.execution_runner import _regular, _info, RunnerError\n"
            "for reader in (_regular, _info):\n"
            " try:reader(Path(sys.argv[1]))\n"
            " except (RunnerError,OSError):pass\n"
            " else:raise AssertionError('special file was accepted')\n"
            "print('rejected')\n")
        for path in (fifo, directory, Path('/dev/null')):
            with self.subTest(path=path):
                observed = subprocess.run([sys.executable, "-c", code, str(path)],
                                          cwd=Path(__file__).resolve().parents[1],
                                          capture_output=True, text=True, timeout=3, check=False)
                self.assertEqual(observed.returncode, 0, observed.stderr)
                self.assertEqual(observed.stdout.strip(), "rejected")

    def test_fifo_model_delivery_finishes_collection_as_incomplete(self) -> None:
        fifo_cli = self.base / "fifo_cli.py"
        fifo_cli.write_text(
            "import sys,os,json\n"
            "sys.stdin.read()\n"
            "os.mkfifo('answer.md')\n"
            "events=[{'type':'thread.started','thread_id':'" + SID + "'},"
            "{'type':'item.completed','item':{'type':'agent_message','text':'Claims complete.'}},"
            "{'type':'turn.completed'}]\n"
            "for event in events:print(json.dumps(event),flush=True)\n")
        self.prepare(argv=[sys.executable, str(fifo_cli)], dependencies=[str(fifo_cli)])
        code = (
            "import sys,json\nfrom pathlib import Path\n"
            "from taskflow.execution_runner import run_attempt\n"
            "print(json.dumps(run_attempt(Path(sys.argv[1]))))\n")
        observed = subprocess.run([sys.executable, "-c", code, str(self.root)],
                                  cwd=Path(__file__).resolve().parents[1],
                                  capture_output=True, text=True, timeout=3, check=False)
        self.assertEqual(observed.returncode, 0, observed.stderr)
        result = json.loads(observed.stdout)
        self.assertTrue(result["model_completed"])
        self.assertFalse(result["delivery_complete"])
        self.assertFalse(result["technical_accepted"])
        self.assertEqual(result["status"], "incomplete")
        self.assertIn("Not a regular file", result["artifact_errors"][0]["error"])
        self.assertFalse((self.root / "artifacts/answer.md").exists())

    def test_blind_render_omits_dispatch_intent_policy_and_target_metadata(self) -> None:
        intent = "ARBITRARY_INTENT_SENTINEL_731"
        policy = "ARBITRARY_POLICY_SENTINEL_472"
        target = "ARBITRARY_TARGET_SENTINEL_319"
        for role in ("solver", "review-facts"):
            with self.subTest(role=role):
                self.root = self.base / (role + "-attempt")
                work = self.base / (role + "-work")
                work.mkdir()
                manifest = self.prepare(role=role, cwd=str(work), intent_id=intent,
                                        policy_version=policy, purpose=target,
                                        experiment_target={"unrendered": target})
                request = (self.root / "request.md").read_text()
                for sentinel in (intent, policy, target):
                    self.assertNotIn(sentinel, request)
                self.assertEqual(manifest["intent_id"], intent)
                self.assertEqual(manifest["policy_version"], policy)
                self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])
                self.assertEqual((work / "seen.txt").read_text(), request)
        # Teacher-side roles retain the explicit intent and typed source links.
        self.root = self.base / "teacher-attempt"
        work = self.base / "teacher-work"
        work.mkdir()
        self.prepare(role="quality", cwd=str(work), intent_id=intent,
                     inputs=[{"path": str(self.public), "kind": "teacher"}])
        request = (self.root / "request.md").read_text()
        self.assertIn(intent, request)
        self.assertIn("Input (teacher):", request)
        self.assertIn(str(self.root / "inputs/0000-source.md"), request)

    def verified_default_runtime(self) -> tuple[Path, dict, dict]:
        runtime = self.base / "verified-default-runtime"
        runtime.mkdir()
        for name in ("runtime_gate.py", "file_evidence.py", "source-lock.json", "baseline-plan.json"):
            (runtime / name).write_text("{}")
        (runtime / "request.md").write_text("Neutral public request, no teacher metadata.")
        (runtime / "shell-probes.json").write_text('{"passed":true}')
        argv = ["neutral-cli", "-c", 'default_permissions="neutral-profile"',
                "exec", "-m", "neutral", "--json", "-"]
        plan = {"cwd": str(self.work), "requested_model": "neutral", "argv": argv,
                "launcher_python": sys.executable, "timeout_seconds": 3600,
                "source_lock_sha256": runner._info(runtime / "source-lock.json")["sha256"],
                "input_digest": "neutral-input", "public_tree_digest": "neutral-public-tree",
                "frozen_binding": {"taskflow_input_digest": "neutral-input"},
                "request_sha256": runner._info(runtime / "request.md")["sha256"]}
        overrides = ["-c", 'model_provider="audit-fixed"', "-c",
                     'model_providers.audit-fixed.name="Local no inference fixture"', "-c",
                     'model_providers.audit-fixed.base_url="http://127.0.0.1:23456/v1"', "-c",
                     'model_providers.audit-fixed.wire_api="responses"', "-c",
                     "model_providers.audit-fixed.requires_openai_auth=false"]
        report = {"nature": "current_local_fixed_exec_context_not_production_payload",
                  "model_calls": 0, "external_model_requests": 0, "tool_calls_returned": 0,
                  "exit_code": 0, "public_tree_unchanged": True, "passed": True,
                  "only_extra_argv": overrides, "argv": argv[:3] + overrides + argv[3:],
                  "bound": {"plan_core_sha256": runner._digest(runner._bytes(plan)),
                            "cwd": plan["cwd"], "profile": "neutral-profile",
                            "argv_sha256": runner._digest(runner._bytes(argv)),
                            "public_tree_digest": plan["public_tree_digest"],
                            "input_digest": plan["input_digest"], "request_sha256": plan["request_sha256"]},
                  "observations": [{"path": "/v1/responses", "model": "neutral",
                                    "reasoning": {"effort": "medium", "context": "all_turns"},
                                    "input": [{"role": "user", "content": [{"type": "input_text",
                                               "text": (runtime / "request.md").read_text()}]}]}]}
        (runtime / "execute_diagnostic.py").write_text(
            "import sys,json\nfrom pathlib import Path\nr=Path(__file__).parent\n"
            "if '--execute' in sys.argv:\n"
            " (r/'real-run').mkdir()\n"
            " events=[{'type':'thread.started','thread_id':'" + SID + "'},"
            "{'type':'item.completed','item':{'type':'agent_message','text':'Fixture delivery'}},"
            "{'type':'turn.completed'}]\n"
            " (r/'real-run/trace.jsonl').write_text('\\n'.join(json.dumps(e) for e in events))\n"
            " (r/'real-run/attempt.json').write_text(json.dumps({'status':'process_exited','returncode':0,'exceptions':[]}))\n"
            " (r/'post-run-input-readback.json').write_text(json.dumps({'matches':True}))\n"
            " Path('answer.md').write_text('Fixture default-mode delivery')\n")
        return runtime, plan, report

    def save_default_runtime(self, runtime: Path, plan: dict, report: dict) -> None:
        (runtime / "exec-context.json").write_text(json.dumps(report))
        plan["probe_evidence_hashes"] = {name: runner._info(runtime / name)["sha256"]
                                         for name in ("shell-probes.json", "exec-context.json")}
        (runtime / "execution-plan.json").write_text(json.dumps(plan))
        plan_sha = runner._info(runtime / "execution-plan.json")["sha256"]
        (runtime / "plan-lock.json").write_text(json.dumps({"sha256": plan_sha}))
        (runtime / "launch-review.json").write_text(json.dumps({"approved": True, "reviewer": "neutral",
                            "plan_sha256": plan_sha, "input_digest": plan["input_digest"]}))

    def test_verified_client_default_preserves_argv_and_records_only_local_proof(self) -> None:
        runtime, plan, report = self.verified_default_runtime()
        self.save_default_runtime(runtime, plan, report)
        manifest = self.prepare(mode="existing_isolated", role="solver", reasoning_effort="medium",
                                reasoning_selection="verified_client_default", reasoning_context="all_turns",
                                isolated={"runtime": str(runtime), "flavor": "formal"})
        model = manifest["model"]
        self.assertEqual(model["reasoning_selection"], "verified_client_default")
        self.assertIsNone(model["request_reasoning_override"])
        self.assertEqual(model["client_resolved"], "neutral")
        self.assertEqual(model["client_resolved_reasoning"], {"effort": "medium", "context": "all_turns"})
        self.assertIsNone(model["served"])
        self.assertIn("production payload", model["client_resolution_scope"])
        self.assertFalse(any(arg.startswith("model_reasoning_effort=") for arg in plan["argv"]))
        self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])

    def test_verified_default_rejects_forged_passed_changed_request_and_inference(self) -> None:
        runtime, plan, original = self.verified_default_runtime()
        mutations = ("passed-only", "effort", "context", "model", "request", "argv", "binding",
                     "model-calls", "external-calls", "tool-calls", "wrong-provider")
        for index, failure in enumerate(mutations):
            report = json.loads(json.dumps(original))
            self.root = self.base / f"bad-client-default-{index}"
            if failure == "passed-only":
                report = {"passed": True}
            elif failure in {"effort", "context"}:
                report["observations"][0]["reasoning"][failure] = "different"
            elif failure == "model":
                report["observations"][0]["model"] = "different"
            elif failure == "request":
                report["observations"][0]["input"][-1]["content"][0]["text"] = "different"
            elif failure == "argv":
                report["argv"] = ["different"]
            elif failure == "binding":
                report["bound"]["input_digest"] = "other-version"
            elif failure == "wrong-provider":
                report["only_extra_argv"][5] = 'model_providers.audit-fixed.base_url="https://external.invalid/v1"'
            else:
                key = {"model-calls": "model_calls", "external-calls": "external_model_requests",
                       "tool-calls": "tool_calls_returned"}[failure]
                report[key] = 1
            self.save_default_runtime(runtime, plan, report)
            with self.subTest(failure=failure), self.assertRaises(runner.RunnerError):
                self.prepare(mode="existing_isolated", role="solver", reasoning_effort="medium",
                             reasoning_selection="verified_client_default", reasoning_context="all_turns",
                             isolated={"runtime": str(runtime), "flavor": "formal"})

    def test_archive_rechecks_sealed_bytes_after_reconcile_before_copy(self) -> None:
        self.prepare()
        original = runner.run_attempt(self.root)
        real_reconcile = runner.reconcile_attempt
        def change_after_reconcile(root, **kwargs):
            result = real_reconcile(root, **kwargs)
            (root / "artifacts/answer.md").write_text("Raced after cached acceptance")
            return result
        target = self.base / "racing-archive"
        with mock.patch.object(runner, "reconcile_attempt", side_effect=change_after_reconcile):
            with self.assertRaises(runner.RunnerError):
                runner.archive_attempt(self.root, target)
        self.assertFalse(target.exists())
        self.assertTrue(target.with_name(target.name + ".staging").exists())
        self.assertEqual(json.loads((self.root / "collection.json").read_text()), original)

    def test_archive_rechecks_staged_bytes_after_member_copy(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / "racing-staged-archive"
        actual_publish = runner._publish_bytes
        changed = []
        def change_one_copy(path, data):
            actual_publish(path, data)
            if ".staging" in str(path) and path.name == "request.md" and not changed:
                path.write_text("Staging changed after atomic member capture")
                changed.append(str(path))
        with mock.patch.object(runner, "_publish_bytes", side_effect=change_one_copy):
            with self.assertRaises(runner.RunnerError):
                runner.archive_attempt(self.root, target)
        self.assertTrue(changed)
        self.assertFalse(target.exists())
        self.assertTrue(target.with_name(target.name + ".staging").exists())

    def test_trace_and_metadata_fifos_reject_in_bounded_child(self) -> None:
        fifo = self.base / "metadata-or-trace-fifo"
        os.mkfifo(fifo)
        code = (
            "import sys\nfrom pathlib import Path\n"
            "from taskflow.execution_runner import _read,_trace,RunnerError\n"
            "p=Path(sys.argv[1])\n"
            "try:_read(p)\n"
            "except (RunnerError,OSError):pass\n"
            "else:raise AssertionError('FIFO metadata accepted')\n"
            "result=_trace(p)\n"
            "assert result['model_completed'] is False\n"
            "print('rejected')\n")
        observed = subprocess.run([sys.executable, "-c", code, str(fifo)],
                                  cwd=Path(__file__).resolve().parents[1],
                                  capture_output=True, text=True, timeout=3, check=False)
        self.assertEqual(observed.returncode, 0, observed.stderr)
        self.assertEqual(observed.stdout.strip(), "rejected")

    def test_facts_and_score_bind_same_candidate_public_and_private_version(self) -> None:
        binding = {"task_id": "neutral", "candidate_id": "candidate-v1", "input_digest": "public-a",
                   "content_digest": "complete-a", "evaluation_digest": "private-a"}
        manifest = self.prepare(role="review-facts", candidate_binding=binding)
        self.assertEqual(manifest["candidate_binding"], binding)
        runner.run_attempt(self.root)
        seal = runner.seal_facts(self.root, self.root / "artifacts/answer.md")
        self.assertEqual(seal["candidate_binding"], binding)
        facts_root = self.root
        for index, field in enumerate(("input_digest", "evaluation_digest", "candidate_id")):
            self.root = self.base / f"cross-version-score-{index}"
            wrong = {**binding, field: "different"}
            with self.subTest(field=field), self.assertRaises(runner.RunnerError):
                self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                             candidate_binding=wrong)
        self.root = self.base / "bound-score"
        work = self.base / "bound-score-work"
        work.mkdir()
        score = self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                             candidate_binding=binding, cwd=str(work))
        self.assertEqual(score["candidate_binding"], binding)

    def test_native_profile_preserves_network_and_shared_project_read_only(self) -> None:
        evidence = {"model_calls": 0, "commands": [
            {"argv": [str(self.script), "--version"], "returncode": 0, "stdout": "fixture"},
            {"argv": [str(self.script), "--no-daemon", "debug", "models", "--bundled"],
             "returncode": 0, "stdout": json.dumps({"models": [{"slug": "neutral",
                  "supported_reasoning_levels": [{"effort": "high"}]}]})}]}
        def capture(root, cli):
            runner._publish(root / "cli-evidence.json", evidence)
            return evidence
        with mock.patch("tools.isolated_quality.capture_cli_evidence", side_effect=capture):
            manifest = self.prepare(mode="native_role", cli=str(self.script))
        argv = manifest["argv"]
        self.assertNotIn("-s", argv)
        for setting in ('permissions.neutral-native.network.enabled=true',
                        'permissions.neutral-native.network.domains={"*"="allow"}',
                        'features.network_proxy=true', 'web_search="live"'):
            self.assertIn(setting, argv)
            self.assertIn(setting, manifest["native"]["sandbox_argv"])
        self.assertEqual(manifest["native"]["filesystem"][str(self.base / "project")], "read")
        self.assertEqual(manifest["native"]["filesystem"][str(self.work)], "write")
        with self.assertRaises(FileNotFoundError):
            runner.run_attempt(self.root)
        self.assertFalse((self.root / "dispatch.json").exists())
        native = manifest["native"]
        expected = [*("read:" + path for path in native["probe_sources"]), "project_write_denied",
                    "write:" + manifest["cwd"], "write:" + native["scratch"], "tcp:deny", "unix:deny"]
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.recv.return_value = b"NATIVE"
        listener = mock.MagicMock()
        listener.getsockname.return_value = ("127.0.0.1", 23456)
        listener.accept.return_value = (connection, None)
        observed = subprocess.CompletedProcess([], 0, json.dumps(expected), "")
        with mock.patch.object(runner.socket, "socket", return_value=listener), \
                mock.patch.object(runner.subprocess, "run", return_value=observed):
            report = runner.probe_native_attempt(self.root)
        self.assertTrue(report["passed"])
        self.assertEqual(report["model_calls"], 0)
        self.assertEqual(runner._validate(self.root, before_run=True)["native"], native)
        ready = json.loads((self.root / "native-probe-ready.json").read_text())
        (self.root / ready["report_file"]).write_text('{"passed":true}')
        with self.assertRaises(runner.RunnerError):
            runner._validate(self.root, before_run=True)

    def test_native_profile_rejects_project_write_or_project_child_read_denial(self) -> None:
        evidence = {"model_calls": 0, "commands": [
            {"argv": [str(self.script), "--version"], "returncode": 0, "stdout": "fixture"},
            {"argv": [str(self.script), "--no-daemon", "debug", "models", "--bundled"],
             "returncode": 0, "stdout": json.dumps({"models": [{"slug": "neutral",
                  "supported_reasoning_levels": [{"effort": "high"}]}]})}]}
        def capture(root, cli):
            runner._publish(root / "cli-evidence.json", evidence)
            return evidence
        repository, scratch = self.base / "project", self.base / "scratch"
        repository.mkdir(); scratch.mkdir()
        readonly = repository / "file.md"
        readonly.write_text("Neutral")
        for index, mode in enumerate(("project-write", "project-deny", "extra-write")):
            self.root = self.base / f"invalid-native-{index}"
            filesystem = {str(repository): "read", str(self.work): "write", str(scratch): "write"}
            if mode == "project-write":filesystem[str(repository)] = "write"
            if mode == "project-deny":filesystem[str(readonly)] = "deny"
            if mode == "extra-write":filesystem[str(self.base)] = "write"
            permissions = {"profile": "neutral", "repository": str(repository), "scratch": str(scratch),
                           "python": sys.executable, "probe_sources": [str(readonly)],
                           "readonly_probe": str(readonly), "filesystem": filesystem}
            with self.subTest(mode=mode), mock.patch("tools.isolated_quality.capture_cli_evidence", side_effect=capture):
                with self.assertRaises(runner.RunnerError):
                    self.prepare(mode="native_role", cli=str(self.script), native_permissions=permissions)

    def prepare_tree(self, action: str, filename: str = "consolidated.md") -> dict:
        script = self.base / "tree_cli.py"
        script.write_text(
            "import json,sys,os\nfrom pathlib import Path\nsys.stdin.read()\n"
            "action,name=sys.argv[1:]\nd=Path('deliverables');d.mkdir()\n"
            "if action!='empty':\n"
            " p=d/name;p.parent.mkdir(parents=True,exist_ok=True)\n"
            " if action=='fifo':os.mkfifo(p)\n"
            " elif action=='symlink':p.symlink_to(" + repr(str(self.public)) + ")\n"
            " else:p.write_text('An actual independently named delivery.');(d/'__init__.py').touch();(d/'empty/nested').mkdir(parents=True)\n"
            "events=[{'type':'thread.started','thread_id':'" + SID + "'},"
            "{'type':'item.completed','item':{'type':'agent_message','text':'Delivered named files.'}},"
            "{'type':'turn.completed'}]\n"
            "for event in events:print(json.dumps(event),flush=True)\n")
        return self.prepare(outputs=[], output_trees=["deliverables"],
                            argv=[sys.executable, str(script), action, filename],
                            dependencies=[str(script)])

    def test_output_tree_accepts_different_legitimate_names_and_empty_support_file(self) -> None:
        for index, name in enumerate(("consolidated.md", "nested/results.json")):
            self.root = self.base / f"tree-attempt-{index}"
            self.work = self.base / f"tree-work-{index}"
            self.work.mkdir()
            self.config["cwd"] = str(self.work)
            manifest = self.prepare_tree("normal", name)
            self.assertEqual(manifest["outputs"], [])
            self.assertEqual(manifest["output_trees"], ["deliverables"])
            result = runner.run_attempt(self.root)
            self.assertTrue(result["technical_accepted"])
            self.assertIn("deliverables/" + name, result["artifacts"])
            self.assertEqual(result["artifacts"]["deliverables/__init__.py"]["size"], 0)
            self.assertTrue((self.root / "artifacts/deliverables" / name).exists())
            index_record = json.loads((self.root / "collection-sources.json").read_text())
            self.assertIn("deliverables/" + name, index_record["output_tree_members"]["deliverables"])
            self.assertIn("deliverables/empty/nested", index_record["output_tree_directories"]["deliverables"])
            self.assertTrue((self.root / "artifacts/deliverables/empty/nested").is_dir())
            archived = self.base / f"tree-archive-{index}"
            runner.archive_attempt(self.root, archived)
            self.assertTrue((archived / "artifacts/deliverables/empty/nested").is_dir())
            archive_binding = json.loads((archived / "archive-binding.json").read_text())
            self.assertIn("artifacts/deliverables/empty/nested", archive_binding["directories"])

    def test_empty_fifo_and_symlink_output_directories_are_incomplete(self) -> None:
        for index, action in enumerate(("empty", "fifo", "symlink")):
            self.root = self.base / f"invalid-tree-{index}"
            self.work = self.base / f"invalid-tree-work-{index}"
            self.work.mkdir()
            self.config["cwd"] = str(self.work)
            self.prepare_tree(action)
            result = runner.run_attempt(self.root)
            self.assertTrue(result["model_completed"])
            self.assertFalse(result["technical_accepted"])
            self.assertFalse(result["delivery_complete"])
            self.assertFalse((self.root / "artifacts/deliverables/consolidated.md").exists())

    def test_output_tree_membership_drift_after_interruption_is_refused(self) -> None:
        self.prepare_tree("normal", "own-name.md")
        runner.run_attempt(self.root, fault="collecting")
        trace = (self.root / "trace.jsonl").read_bytes()
        (self.work / "deliverables/later.md").write_text("Added after source index was sealed")
        with self.assertRaisesRegex(runner.RunnerError, "membership changed"):
            runner.reconcile_attempt(self.root)
        self.assertEqual((self.root / "trace.jsonl").read_bytes(), trace)
        self.assertFalse((self.root / "collection.json").exists())
        with self.assertRaises(FileExistsError):
            runner.run_attempt(self.root)

    def test_output_tree_changed_bytes_after_interruption_do_not_replace_original_index(self) -> None:
        self.prepare_tree("normal", "own-name.md")
        runner.run_attempt(self.root, fault="collecting")
        index = (self.root / "collection-sources.json").read_bytes()
        (self.work / "deliverables/own-name.md").write_text("Changed after source freeze")
        result = runner.reconcile_attempt(self.root)
        self.assertFalse(result["technical_accepted"])
        self.assertEqual((self.root / "collection-sources.json").read_bytes(), index)
        self.assertFalse((self.root / "artifacts/deliverables/own-name.md").exists())

    def test_output_tree_contract_rejects_traversal_existing_and_nested_roots(self) -> None:
        for index, trees in enumerate((["../outside"], ["deliverables", "deliverables/nested"])):
            self.root = self.base / f"bad-tree-path-{index}"
            with self.assertRaises(runner.RunnerError):
                self.prepare(outputs=[], output_trees=trees)
        (self.work / "already-there").mkdir()
        self.root = self.base / "existing-tree"
        with self.assertRaises(runner.RunnerError):
            self.prepare(outputs=[], output_trees=["already-there"])

    def test_empty_directory_deletion_during_collection_is_detected(self) -> None:
        self.prepare_tree("normal", "named.md")
        runner.run_attempt(self.root, fault="collecting")
        (self.work / "deliverables/empty/nested").rmdir()
        with self.assertRaisesRegex(runner.RunnerError, "membership changed"):
            runner.reconcile_attempt(self.root)
        self.assertTrue((self.root / "artifacts/deliverables/empty/nested").is_dir())
        self.assertFalse((self.root / "collection.json").exists())

    def test_empty_directory_addition_during_collection_is_detected(self) -> None:
        self.prepare_tree("normal", "named.md")
        runner.run_attempt(self.root, fault="collecting")
        (self.work / "deliverables/new-empty").mkdir()
        with self.assertRaisesRegex(runner.RunnerError, "membership changed"):
            runner.reconcile_attempt(self.root)
        self.assertFalse((self.root / "artifacts/deliverables/new-empty").exists())

    def test_collected_and_archived_empty_directory_drift_is_rejected(self) -> None:
        self.prepare_tree("normal", "named.md")
        result = runner.run_attempt(self.root)
        self.assertTrue(result["technical_accepted"])
        self.assertIn("artifacts/deliverables/empty/nested", result["artifact_directories"])
        archive = self.base / "archive-with-empty-directories"
        runner.archive_attempt(self.root, archive)
        (archive / "artifacts/deliverables/empty/nested").rmdir()
        with self.assertRaisesRegex(runner.RunnerError, "directory structure"):
            runner.archive_attempt(self.root, archive)
        (self.root / "artifacts/deliverables/empty/nested").rmdir()
        with self.assertRaisesRegex(runner.RunnerError, "directory structure"):
            runner.reconcile_attempt(self.root)

    def test_score_request_rejects_target_history_author_and_unbound_facts_kinds(self) -> None:
        self.prepare(role="review-facts")
        runner.run_attempt(self.root)
        runner.seal_facts(self.root, self.root / "artifacts/answer.md")
        facts_root = self.root
        for index, kind in enumerate(("target", "history_score", "history", "author_conclusion", "facts")):
            self.root = self.base / f"bad-score-input-{index}"
            with self.subTest(kind=kind), self.assertRaises(runner.RunnerError):
                self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                             inputs=[{"path": str(self.public), "kind": kind}])
            with self.subTest(section_kind=kind), self.assertRaises(runner.RunnerError):
                self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                             request_sections=[{"kind": kind, "text": "Untrusted historical conclusion"}])
        self.root = self.base / "untyped-score"
        with self.assertRaises(runner.RunnerError):
            self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                         request_text="Unlabelled prior conclusion")

    def test_score_header_is_neutral_and_same_version_teacher_sources_remain(self) -> None:
        self.prepare(role="review-facts")
        runner.run_attempt(self.root)
        runner.seal_facts(self.root, self.root / "artifacts/answer.md")
        facts_root = self.root
        self.root = self.base / "neutral-score"
        work = self.base / "neutral-score-work"
        work.mkdir()
        intent = "ARBITRARY_SCORE_INTENT_741"
        policy = "ARBITRARY_SCORE_POLICY_916"
        manifest = self.prepare(role="review-score", facts_attempt=str(facts_root), resume_sid=SID,
                                cwd=str(work), intent_id=intent, policy_version=policy,
                                inputs=[{"path": str(self.public), "kind": "teacher"}])
        request = (self.root / "request.md").read_text()
        self.assertNotIn(intent, request)
        self.assertNotIn(policy, request)
        self.assertIn("Input (teacher):", request)
        self.assertEqual(manifest["intent_id"], intent)
        self.assertTrue(runner.run_attempt(self.root)["technical_accepted"])
        self.assertEqual((work / "seen.txt").read_text(), request)

    def test_preparation_config_digest_binds_exact_configuration(self) -> None:
        binding = {"candidate_id": "neutral-a", "task_id": "neutral", "input_digest": "public-a",
                   "content_digest": "whole-a", "evaluation_digest": "private-a"}
        first = self.prepare(candidate_binding=binding)
        expected = runner._digest(runner._bytes({**self.config, "candidate_binding": binding}))
        self.assertEqual(first["preparation_config_digest"], expected)
        self.root = self.base / "different-config"
        second = self.prepare(candidate_binding={**binding, "evaluation_digest": "private-b"})
        self.assertNotEqual(first["preparation_config_digest"], second["preparation_config_digest"])

    def test_full_archive_preserves_external_work_all_files_directories_and_types(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        (self.work / "analysis.py").write_text("print('not a declared output')")
        (self.work / "empty/nested").mkdir(parents=True)
        os.mkfifo(self.work / "no-writer-fifo")
        (self.work / "private-link").symlink_to(self.public)
        (self.work / ".capture-legitimate").write_text("A real final file with a reserved-looking name")
        target = self.base / "full-archive"
        result = runner.archive_full_attempt(self.root, target)
        self.assertTrue(result["archived"])
        self.assertEqual(result["special_entries_indexed"], 2)
        self.assertEqual((target / "supplemental/trees/work/analysis.py").read_text(), "print('not a declared output')")
        self.assertTrue((target / "supplemental/trees/work/empty/nested").is_dir())
        self.assertFalse((target / "supplemental/trees/work/private-link").exists())
        intent = json.loads((self.root / "supplemental-intent.json").read_text())
        self.assertEqual(intent["snapshot"]["work"]["special"]["no-writer-fifo"]["kind"], "fifo")
        self.assertEqual(intent["snapshot"]["work"]["special"]["private-link"]["kind"], "symlink")
        manifest = json.loads((target / "supplemental/manifest.json").read_text())
        self.assertIn(".capture-legitimate", manifest["snapshot"]["work"]["files"])
        self.assertTrue(list((target / "supplemental/indexed-files/work").glob('*.bin')))
        self.assertTrue((self.work / "analysis.py").exists())
        self.assertEqual(runner.archive_full_attempt(self.root, target)["snapshot_digest"], result["snapshot_digest"])

    def test_full_archive_source_changes_never_reseal_new_bytes(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / "full-archive"
        runner.archive_full_attempt(self.root, target)
        original = (self.root / "supplemental-intent.json").read_bytes()
        (self.work / "new-after-archive.py").write_text("new")
        with self.assertRaisesRegex(runner.RunnerError, "sources changed"):
            runner.archive_full_attempt(self.root, target)
        self.assertEqual((self.root / "supplemental-intent.json").read_bytes(), original)
        self.assertFalse((target / "supplemental/trees/work/new-after-archive.py").exists())

    def test_full_archive_partial_capture_resumes_original_intent(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        (self.work / "extra.py").write_text("original extra")
        actual_publish = runner._publish_bytes
        interrupted = []
        def stop_mid_file(path, data):
            if 'supplemental.staging' in str(path) and path.name == 'extra.py' and not interrupted:
                path.parent.mkdir(parents=True, exist_ok=True)
                (path.parent / '.capture-test-interruption').write_bytes(data[:3])
                interrupted.append(True)
                raise RuntimeError('simulated host interruption during snapshot write')
            return actual_publish(path, data)
        target = self.base / "full-archive"
        with mock.patch.object(runner, '_publish_bytes', side_effect=stop_mid_file):
            with self.assertRaises(RuntimeError):
                runner.archive_full_attempt(self.root, target)
        intent = (self.root / 'supplemental-intent.json').read_bytes()
        self.assertTrue((self.root / 'supplemental.staging').exists())
        self.assertFalse(target.exists())
        result = runner.archive_full_attempt(self.root, target)
        self.assertTrue(result['archived'])
        self.assertEqual((self.root / 'supplemental-intent.json').read_bytes(), intent)
        self.assertEqual((target / 'supplemental/trees/work/extra.py').read_text(), 'original extra')
        self.assertFalse(list((target / 'supplemental').rglob('.capture-*')))

    def test_full_archive_drift_during_capture_is_rejected_and_keeps_intent(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        (self.work / 'extra.py').write_text('original')
        actual_publish = runner._publish_bytes
        changed = []
        def drift_after_copy(path, data):
            actual_publish(path, data)
            if 'supplemental.staging' in str(path) and path.name == 'extra.py' and not changed:
                (self.work / 'extra.py').write_text('changed during capture')
                changed.append(True)
        target = self.base / 'full-archive'
        with mock.patch.object(runner, '_publish_bytes', side_effect=drift_after_copy):
            with self.assertRaisesRegex(runner.RunnerError, 'source membership or bytes changed'):
                runner.archive_full_attempt(self.root, target)
        self.assertFalse(target.exists())
        self.assertFalse((self.root / 'supplemental').exists())
        with self.assertRaisesRegex(runner.RunnerError, 'sources changed'):
            runner.archive_full_attempt(self.root, target)

    def test_full_archive_refuses_running_and_source_or_destination_overlap(self) -> None:
        self.prepare()
        runner.run_attempt(self.root, fault='before_start')
        with self.assertRaisesRegex(runner.RunnerError, 'proved termination'):
            runner.archive_full_attempt(self.root, self.base / 'archive')
        self.root = self.base / 'actual-attempt'
        self.prepare()
        runner.run_attempt(self.root)
        for destination in (self.work / 'archive', self.root / 'archive', self.base):
            with self.subTest(destination=destination), self.assertRaises(runner.RunnerError):
                runner.archive_full_attempt(self.root, destination)
        link = self.base / 'destination-link'
        link.symlink_to(self.base / 'outside')
        with self.assertRaises(runner.RunnerError):
            runner.archive_full_attempt(self.root, link / 'archive')

    def test_full_archive_final_supplemental_or_receipt_tampering_refuses(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / 'full-archive'
        runner.archive_full_attempt(self.root, target)
        (self.root / 'supplemental/trees/work/answer.md').write_text('changed captured bytes')
        with self.assertRaises(runner.RunnerError):
            runner.archive_full_attempt(self.root, target)

    def test_full_native_archive_includes_external_scratch(self) -> None:
        self.script.write_text('#!' + str(Path(sys.executable).resolve()) + '\n' + self.script.read_text())
        self.script.chmod(0o755)
        evidence = {"model_calls": 0, "commands": [
            {"argv": [str(self.script), "--version"], "returncode": 0, "stdout": "fixture"},
            {"argv": [str(self.script), "--no-daemon", "debug", "models", "--bundled"],
             "returncode": 0, "stdout": json.dumps({"models": [{"slug": "neutral",
                  "supported_reasoning_levels": [{"effort": "high"}]}]})}]}
        def capture(root, cli):
            runner._publish(root / 'cli-evidence.json', evidence)
            return evidence
        with mock.patch('tools.isolated_quality.capture_cli_evidence', side_effect=capture):
            manifest = self.prepare(mode='native_role', cli=str(self.script))
        native = manifest['native']
        expected = [*("read:" + path for path in native['probe_sources']), 'project_write_denied',
                    'write:' + manifest['cwd'], 'write:' + native['scratch'], 'tcp:deny', 'unix:deny']
        listener = mock.MagicMock()
        listener.getsockname.return_value = ('127.0.0.1', 23456)
        with mock.patch.object(runner.socket, 'socket', return_value=listener), \
                mock.patch.object(runner.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(expected), '')):
            runner.probe_native_attempt(self.root)
        self.assertTrue(runner.run_attempt(self.root)['technical_accepted'])
        scratch = Path(native['scratch'])
        (scratch / 'retained-code.py').write_text('print(42)')
        (scratch / 'empty/nested').mkdir(parents=True)
        result = runner.archive_full_attempt(self.root, self.base / 'native-full-archive')
        self.assertIn('scratch', result['source_roots'])
        archived = Path(result['path'])
        self.assertEqual((archived / 'supplemental/trees/scratch/retained-code.py').read_text(), 'print(42)')
        self.assertTrue((archived / 'supplemental/trees/scratch/empty/nested').is_dir())
        self.assertTrue((archived / 'trace.jsonl').exists())
        self.assertTrue((archived / 'request.md').exists())

    def test_full_quality_bridge_archive_contains_inner_raw_run_work_and_scratch(self) -> None:
        inner = self.base / 'inner-quality'
        inner.mkdir()
        for name in ('work','scratch','control'):(inner / name).mkdir()
        (inner / 'source-lock.json').write_text('{}')
        (inner / 'probe.json').write_text('{"passed":true}')
        ready = {'probe_file':'probe.json', 'probe':runner._info(inner / 'probe.json'),
                 'source_lock':runner._info(inner / 'source-lock.json')}
        (inner / 'ready.json').write_text(json.dumps(ready))
        (inner / 'plan.json').write_text(json.dumps({'model':{'requested':'neutral','reasoning_effort':'high'}}))
        for name in ('plan-lock.json','prepared-lock.json'):(inner / name).write_text('{}')
        (inner / 'request.md').write_text('Actual locked inner public request')
        (inner / 'runner.py').write_text(
            "import sys,json\nfrom pathlib import Path\nr=Path(__file__).parent\n"
            "if sys.argv[1]=='execute':\n"
            " (r/'run').mkdir()\n"
            " events=[{'type':'thread.started','thread_id':'" + SID + "'},"
            "{'type':'item.completed','item':{'type':'agent_message','text':'Actual inner neutral result'}},"
            "{'type':'turn.completed'}]\n"
            " (r/'run/trace.jsonl').write_text('\\n'.join(json.dumps(e) for e in events))\n"
            " (r/'run/attempt.json').write_text(json.dumps({'status':'completed','returncode':0}))\n"
            " (r/'run/request.md').write_text((r/'request.md').read_text())\n"
            " Path('answer.md').write_text('Inner actual output')\n"
            " Path('unlisted-helper.py').write_text('print(1)')\n"
            " Path('empty/nested').mkdir(parents=True)\n"
            " (r/'scratch/temp-code.py').write_text('print(2)')\n")
        self.prepare(mode='existing_isolated', role='quality', cwd=str(inner / 'work'),
                     isolated={'runtime':str(inner),'flavor':'quality'})
        self.assertTrue(runner.run_attempt(self.root)['technical_accepted'])
        result = runner.archive_full_attempt(self.root, self.base / 'quality-full-archive')
        self.assertEqual(result['source_roots'], {'inner':str(inner)})
        archived = Path(result['path']) / 'supplemental/trees/inner'
        self.assertEqual((archived / 'work/unlisted-helper.py').read_text(), 'print(1)')
        self.assertEqual((archived / 'scratch/temp-code.py').read_text(), 'print(2)')
        self.assertEqual((archived / 'run/request.md').read_text(), 'Actual locked inner public request')
        self.assertTrue((archived / 'run/trace.jsonl').exists())
        self.assertTrue((archived / 'work/empty/nested').is_dir())

    def test_full_archive_receipt_before_supplemental_publication_recovers(self) -> None:
        from taskflow import contracts
        self.prepare()
        runner.run_attempt(self.root)
        actual_publish = contracts._publish_no_replace
        def interrupted(source, destination):
            if destination == self.root / 'supplemental':
                raise RuntimeError('host stopped after sealed supplemental receipt')
            return actual_publish(source, destination)
        target = self.base / 'full-archive'
        with mock.patch.object(contracts, '_publish_no_replace', side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                runner.archive_full_attempt(self.root, target)
        self.assertTrue((self.root / 'supplemental.staging/receipt.json').exists())
        self.assertFalse((self.root / 'supplemental').exists())
        self.assertTrue(runner.archive_full_attempt(self.root, target)['archived'])

    def test_full_archive_external_drift_during_archive_copy_prevents_final_publication(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        actual_archive = runner.archive_attempt
        def changed_after_archive(root, destination, fault=None, **kwargs):
            result = actual_archive(root, destination, fault=fault, **kwargs)
            (self.work / 'late.py').write_text('late writer')
            return result
        target = self.base / 'full-archive'
        with mock.patch.object(runner, 'archive_attempt', side_effect=changed_after_archive):
            with self.assertRaisesRegex(runner.RunnerError, 'sources changed during final archive'):
                runner.archive_full_attempt(self.root, target)
        self.assertFalse(target.exists())
        self.assertTrue(target.with_name(target.name + '.full-pending').exists())
        with self.assertRaisesRegex(runner.RunnerError, 'sources changed'):
            runner.archive_full_attempt(self.root, target)

    def test_full_archive_final_publication_interrupt_recovers_without_recopy(self) -> None:
        from taskflow import contracts
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / 'full-archive'
        actual_publish = contracts._publish_no_replace
        def interrupted(source, destination):
            if destination == target:
                raise RuntimeError('host stopped before final full archive rename')
            return actual_publish(source, destination)
        with mock.patch.object(contracts, '_publish_no_replace', side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                runner.archive_full_attempt(self.root, target)
        pending = target.with_name(target.name + '.full-pending')
        self.assertTrue(pending.exists())
        original = (pending / 'archive-binding.json').read_bytes()
        result = runner.archive_full_attempt(self.root, target)
        self.assertTrue(result['archived'])
        self.assertEqual((target / 'archive-binding.json').read_bytes(), original)
        self.assertFalse(pending.exists())

    def test_full_archive_retains_existing_128mib_file_bound(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        with (self.work / 'too-large.bin').open('wb') as handle:
            handle.truncate(128 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(runner.RunnerError, 'existing 128 MiB'):
            runner.archive_full_attempt(self.root, self.base / 'full-archive')
        self.assertFalse((self.root / 'supplemental').exists())

    def test_full_archive_idempotence_check_detects_concurrent_external_change(self) -> None:
        self.prepare()
        runner.run_attempt(self.root)
        target = self.base / 'full-archive'
        runner.archive_full_attempt(self.root, target)
        original = (target / 'archive-binding.json').read_bytes()
        actual_archive = runner.archive_attempt
        def changed_after_validation(root, destination, fault=None, **kwargs):
            result = actual_archive(root, destination, fault=fault, **kwargs)
            (self.work / 'changed-while-verifying.py').write_text('new')
            return result
        with mock.patch.object(runner, 'archive_attempt', side_effect=changed_after_validation):
            with self.assertRaisesRegex(runner.RunnerError, 'sources changed while verifying'):
                runner.archive_full_attempt(self.root, target)
        self.assertEqual((target / 'archive-binding.json').read_bytes(), original)

    def long_unix_probe_fixture(self, suffix: str, expectation: str) -> tuple[Path, dict]:
        root = self.base / ('unix-probe-' + suffix)
        work = self.base / ('unix-work-' + suffix)
        work.mkdir()
        scratch = self.base / ('long-scratch-' + 'x' * 70) / ('nested-' + suffix + '-' + 'y' * 35)
        scratch.mkdir(parents=True)
        self.assertGreater(len(str(scratch)), 104)
        repository = self.base / 'unix-project'
        repository.mkdir(exist_ok=True)
        source = repository / 'readonly.txt'
        source.write_text('Neutral native permission fixture')
        permissions = {'profile':'long-unix-' + suffix,'repository':str(repository),
                       'scratch':str(scratch),'python':str(Path(sys.executable).resolve()),
                       'filesystem':{':root':'deny',':minimal':'read',str(repository):'read',
                                     str(work):'write',str(scratch):'write'},
                       'readonly_probe':str(source),'probe_sources':[str(source)],
                       'direct_network':{'tcp':'deny','unix':expectation}}
        evidence = {'model_calls':0,'commands':[
            {'argv':[str(self.script),'--version'],'returncode':0,'stdout':'fixture'},
            {'argv':[str(self.script),'--no-daemon','debug','models','--bundled'],
             'returncode':0,'stdout':json.dumps({'models':[{'slug':'neutral','supported_reasoning_levels':[{'effort':'high'}]}]})}]}
        def capture(where, cli):
            runner._publish(where / 'cli-evidence.json', evidence)
            return evidence
        with mock.patch('tools.isolated_quality.capture_cli_evidence',side_effect=capture):
            manifest = runner.prepare_attempt(root,{**self.config,'cwd':str(work),'mode':'native_role',
                                                'cli':str(self.script),'native_permissions':permissions})
        return root,manifest

    def simulated_native_with_real_relative_unix(self, actual_run):
        # Only the native sandbox transport is simulated. The production UNIX
        # branch runs in a real child, against the real short-basename helper.
        def invoke(argv, **kwargs):
            code = argv[-1]
            unix = code[code.index('os.chdir('):code.index('print(json.dumps(rows))')]
            denied = "assert observed=='deny'" in unix
            prefix = 'import os,errno,socket,json\nrows=[]\n'
            if denied:
                prefix += ("real_socket=socket.socket\n"
                           "class DeniedSocket:\n"
                           " def __init__(self,*args):self.socket=real_socket(*args)\n"
                           " def settimeout(self,value):self.socket.settimeout(value)\n"
                           " def connect(self,address):raise PermissionError(errno.EPERM,'simulated native direct denial')\n"
                           " def close(self):self.socket.close()\n"
                           "socket.socket=DeniedSocket\n")
            result = actual_run([sys.executable,'-B','-c',prefix + unix + 'print(json.dumps(rows))\n'],
                                cwd=kwargs['cwd'],capture_output=True,text=True,timeout=5,check=False)
            if result.returncode:
                return result
            import ast
            sources_line = next(line for line in code.splitlines() if line.startswith('for name,expected in '))
            sources = ast.literal_eval(sources_line.removeprefix('for name,expected in ').removesuffix('.items():'))
            directories_line = next(line for line in code.splitlines() if line.startswith('for directory in '))
            directories = ast.literal_eval(directories_line.removeprefix('for directory in ').removesuffix(':'))
            rows = [*("read:" + name for name in sources),'project_write_denied',
                    *("write:" + name for name in directories),'tcp:deny',*json.loads(result.stdout)]
            return subprocess.CompletedProcess(argv,0,json.dumps(rows),'')
        return invoke

    def test_native_long_unix_relative_helper_allows_and_denies_without_parent_chdir(self) -> None:
        actual_run = subprocess.run
        cwd = Path.cwd()
        for expectation in ('deny','allow'):
            root,manifest = self.long_unix_probe_fixture(expectation,expectation)
            old = root / 'native-probe-old-failure.json'
            old.write_text('{"passed":false,"error":"AF_UNIX path too long"}')
            original_manifest = (root / 'manifest.json').read_bytes()
            original_worker = (root / 'worker.py').read_bytes()
            with mock.patch.object(runner.subprocess,'run',side_effect=self.simulated_native_with_real_relative_unix(actual_run)), \
                    mock.patch.object(runner.os,'chdir',side_effect=AssertionError('parent cwd changed')):
                report = runner.probe_native_attempt(root)
            self.assertTrue(report['passed'], report)
            self.assertTrue(report['unix_listener']['ready'])
            self.assertEqual(report['unix_listener']['returncode'],0)
            self.assertGreater(report['unix_listener']['scratch_path_bytes'],104)
            self.assertLess(report['unix_listener']['bind_argument_bytes'],104)
            self.assertEqual(report['unix_listener']['cwd'],manifest['native']['scratch'])
            self.assertEqual((root / 'manifest.json').read_bytes(),original_manifest)
            self.assertEqual((root / 'worker.py').read_bytes(),original_worker)
            self.assertEqual(old.read_text(),'{"passed":false,"error":"AF_UNIX path too long"}')
            self.assertFalse(list(Path(manifest['native']['scratch']).glob('np-*.sock')))
            self.assertEqual(Path.cwd(),cwd)

    def test_native_concurrent_long_unix_helpers_are_independent(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        fixtures = [self.long_unix_probe_fixture('parallel-a','allow'),
                    self.long_unix_probe_fixture('parallel-b','allow')]
        actual_run = subprocess.run
        cwd = Path.cwd()
        with mock.patch.object(runner.subprocess,'run',side_effect=self.simulated_native_with_real_relative_unix(actual_run)), \
                mock.patch.object(runner.os,'chdir',side_effect=AssertionError('parent cwd changed')):
            with ThreadPoolExecutor(max_workers=2) as pool:
                reports = list(pool.map(runner.probe_native_attempt,[root for root,_ in fixtures]))
        self.assertTrue(all(report['passed'] for report in reports),reports)
        self.assertEqual(len({report['unix_listener']['pid'] for report in reports}),2)
        self.assertEqual(Path.cwd(),cwd)
        for _,manifest in fixtures:
            self.assertFalse(list(Path(manifest['native']['scratch']).glob('np-*.sock')))
