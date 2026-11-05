"""Neutral offline regression tests; no model or native sandbox is invoked."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from taskflow.contracts import read_json, write_json
from taskflow.packages import export_snapshot, freeze_task
from tests.helpers import add_reviews, make_task
from tools import isolated_quality as quality


THREAD = "12345678-1234-4567-8123-123456789012"


class IsolatedQualityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        (self.base / "home/.codex").mkdir(parents=True)
        self.repository = self.base / "repository"
        self.repository.mkdir()
        self.task = make_task(self.repository)
        add_reviews(self.task)
        freeze_task(self.task, "v1")
        self.snapshot = self.task / "versions/v1"
        self.solver = self.task / "exports/v1-solver"
        export_snapshot(self.snapshot, "solver")
        self.root = self.base / "runtime"
        self.bin = self.base / "bin"
        self.bin.mkdir()
        for name in ("cli", "rtk", "python"):
            (self.bin / name).write_text("NEUTRAL_BINARY_FIXTURE\n", encoding="utf-8")
        self.dependencies = self.base / "dependencies"
        self.dependencies.mkdir()
        self.config = {"repository": str(self.repository),
                       "dependencies": str(self.dependencies),
                       "model": "neutral-model", "reasoning_effort": "high",
                       **{name: str(self.bin / name) for name in ("cli", "rtk", "python")}}
        self.catalog = {"models": [
            {"slug": "neutral-model", "supported_reasoning_levels": [
                {"effort": "medium"}, {"effort": "high"}]},
            {"slug": "neutral-other", "supported_reasoning_levels": [{"effort": "low"}]},
        ]}
        cli_run = mock.patch.object(quality.subprocess, "run", side_effect=self.cli_evidence)
        self.cli_run = cli_run.start()
        self.addCleanup(cli_run.stop)

    def cli_evidence(self, argv, **kwargs) -> subprocess.CompletedProcess:
        self.assertEqual(argv[0], self.config["cli"])
        if argv[1:] == ["--version"]:
            return subprocess.CompletedProcess(argv, 0, "codex-cli fixture\n", "")
        self.assertEqual(argv[1:], ["--no-daemon", "debug", "models", "--bundled"])
        return subprocess.CompletedProcess(argv, 0, json.dumps(self.catalog), "")

    def prepare(self, **kwargs) -> dict:
        with mock.patch.object(Path, "home", return_value=self.base / "home"):
            with mock.patch.dict(os.environ, {}, clear=True):
                return quality.prepare(self.solver, self.snapshot, self.root, self.config, **kwargs)

    def draft(self, task: Path, **kwargs) -> dict:
        with mock.patch.object(Path, "home", return_value=self.base / "home"):
            with mock.patch.dict(os.environ, {}, clear=True):
                return quality.prepare_draft(task, self.root, self.config, **kwargs)

    def probe_ok(self, argv: list[str], **kwargs) -> subprocess.CompletedProcess:
        # Let the implementation produce its actual probe specifications; model
        # native results only at the subprocess boundary, never bypass the gate.
        import shlex
        code = shlex.split(argv[-1])[-1]
        specs_text = code.split("for spec in ", 1)[1].split(":\n", 1)[0]
        import ast
        specs = ast.literal_eval(specs_text)
        rows = [{"path": spec["path"], "read": spec["read"], "write": spec["write"],
                 **({"read_errno": 1} if not spec["read"] else {}),
                 **({"write_errno": 1} if not spec["write"] else {}),
                 **({"sha256": spec["sha256"]} if "sha256" in spec else {})}
                for spec in specs]
        return subprocess.CompletedProcess(argv, 0, json.dumps(rows), "")

    def ready(self) -> None:
        self.prepare()
        with mock.patch.object(quality.subprocess, "run", side_effect=self.probe_ok):
            quality.probe(self.root)

    def mutate_plan(self, change) -> None:
        plan = quality.load(self.root / "plan.json")
        change(plan)
        # Even if an accidental caller regenerates the direct plan lock, the
        # independently locked source set must reject a changed command.
        (self.root / "plan.json").write_bytes(quality.canonical(plan) + b"\n")
        (self.root / "plan-lock.json").write_bytes(quality.canonical(quality.info(self.root / "plan.json")))

    def test_prepare_copies_only_bound_public_bytes_and_has_no_inference(self) -> None:
        with mock.patch.object(quality.subprocess, "Popen") as popen:
            result = self.prepare()
        self.assertEqual(result["model_calls"], 0)
        self.assertFalse(popen.called)
        plan = quality.validate(self.root, require_probe=False)
        package = quality.load(self.root / "work/package.json")
        self.assertEqual(package["audience"], "solver")
        self.assertEqual(set(plan["public_tree"]), {"package.json", "draft/input/prompt.md"})
        self.assertFalse((self.root / "work/draft/evaluation").exists())
        self.assertIn("--ephemeral", plan["argv"])
        self.assertIn("--ignore-user-config", plan["argv"])
        self.assertEqual(plan["argv"][plan["argv"].index("-m") + 1], "neutral-model")
        self.assertIn('model_reasoning_effort="high"', plan["argv"])
        self.assertEqual(plan["model"]["requested"], "neutral-model")
        self.assertEqual(plan["model"]["reasoning_effort"], "high")
        self.assertIsNone(plan["model"]["client_resolved"])
        self.assertIsNone(plan["model"]["served"])
        self.assertEqual(plan["cli"]["version"], "codex-cli fixture")
        self.assertEqual(quality.load(self.root / "cli-evidence.json")["model_calls"], 0)
        self.assertIn("skip_host_skill_discovery", " ".join(plan["argv"]))
        self.assertEqual((self.root / "work/draft/input/prompt.md").stat().st_mode & 0o777, 0o644)
        self.assertNotIn("answer.md", (self.root / "request.md").read_text())
        with self.assertRaises(FileExistsError):
            self.prepare()

    def test_prepare_denies_a_historical_run_in_flat_or_grouped_packages(self) -> None:
        for relative in ("tasks/flat", "tasks/topic/packages/grouped"):
            with self.subTest(package=relative):
                package = self.repository / relative
                package.mkdir(parents=True)
                (package / "task.json").write_text("{}\n", encoding="utf-8")
                run = package / "runs" / "historical" / "run.json"
                run.parent.mkdir(parents=True)
                run.write_text("{}\n", encoding="utf-8")
                self.prepare()
                plan = quality.validate(self.root, require_probe=False)

                self.assertIn(
                    {"path": str(run), "kind": "file"}, plan["denied"]
                )
                self.root = self.base / "runtime-grouped"
                (package / "task.json").unlink()

    def cli_args(self, action: str) -> list[str]:
        sources = ({"solver": self.solver, "snapshot": self.snapshot}
                   if action == "prepare" else {"task": self.task})
        paths = {**sources, "output": self.root,
                 **{key: self.config[key] for key in
                    ("cli", "python", "dependencies", "rtk", "repository")}}
        return [action, *[part for key, value in paths.items() for part in ("--" + key, str(value))]]

    def test_both_cli_prepares_require_each_selection_and_forward_values(self) -> None:
        for action in ("prepare", "prepare-draft"):
            for selection in ([], ["--model", "neutral-model"], ["--reasoning-effort", "high"]):
                with self.subTest(action=action, selection=selection):
                    with contextlib.redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit) as error:
                            quality.main([*self.cli_args(action), *selection])
                    self.assertEqual(error.exception.code, 2)
                    self.assertFalse(self.root.exists())
            self.assertFalse(self.cli_run.called)
            self.root = self.base / action
            with mock.patch.object(Path, "home", return_value=self.base / "home"):
                with mock.patch.dict(os.environ, {}, clear=True):
                    with contextlib.redirect_stdout(io.StringIO()):
                        result = quality.main([*self.cli_args(action), "--model", "neutral-other",
                                               "--reasoning-effort", "low"])
            self.assertEqual(result, 0)
            plan = quality.validate(self.root, require_probe=False)
            self.assertEqual(plan["model"]["requested"], "neutral-other")
            self.assertEqual(plan["model"]["reasoning_effort"], "low")
            self.assertEqual(plan["argv"][plan["argv"].index("-m") + 1], "neutral-other")
            self.assertIn('model_reasoning_effort="low"', plan["argv"])
            self.root = self.base / (action + "-unused")
            self.cli_run.reset_mock()

    def test_empty_default_or_missing_selections_reject_both_prepare_modes(self) -> None:
        original = self.config.copy()
        for mode in (self.prepare, lambda: self.draft(self.task)):
            for key in ("model", "reasoning_effort"):
                for value in (None, "", " ", "default", "auto", " high "):
                    with self.subTest(mode=mode, key=key, value=value):
                        self.config = original.copy()
                        if value is None:
                            self.config.pop(key)
                        else:
                            self.config[key] = value
                        with self.assertRaisesRegex(ValueError, "Explicit"):
                            mode()
                        self.assertFalse(self.root.exists())
        self.assertFalse(self.cli_run.called)

    def test_catalog_limits_model_effort_pair_and_preserves_failed_preparation(self) -> None:
        for index, (model, effort) in enumerate((("unknown-model", "high"),
                                                ("neutral-model", "low"),
                                                ("neutral-model", "invented"))):
            self.root = self.base / f"unsupported-{index}"
            self.config.update(model=model, reasoning_effort=effort)
            with self.assertRaisesRegex(ValueError, "catalog"):
                self.prepare()
            evidence = quality.load(self.root / "cli-evidence.json")
            self.assertEqual(len(evidence["commands"]), 2)
            self.assertFalse((self.root / "plan.json").exists())
            self.assertFalse((self.root / "claim.json").exists())

    def test_cli_evidence_failure_is_retained_without_ready_plan_or_attempt(self) -> None:
        fixtures = [OSError("neutral"), subprocess.TimeoutExpired("neutral", 30),
                    subprocess.CompletedProcess([], 1, "", "NEUTRAL_FAILURE"),
                    subprocess.CompletedProcess([], 0, "INVALID", ""),
                    subprocess.CompletedProcess([], 0, '{"models": []}', "")]
        for index, fixture in enumerate(fixtures):
            self.root = self.base / f"evidence-failure-{index}"
            def broken_catalog(argv, **kwargs):
                if argv[1:] == ["--version"]:
                    return self.cli_evidence(argv, **kwargs)
                if isinstance(fixture, Exception):
                    raise fixture
                return fixture
            with mock.patch.object(quality.subprocess, "run", side_effect=broken_catalog):
                with self.assertRaisesRegex(ValueError, "catalog"):
                    self.prepare()
            evidence = quality.load(self.root / "cli-evidence.json")
            self.assertEqual(evidence["model_calls"], 0)
            self.assertFalse((self.root / "plan.json").exists())
            self.assertFalse((self.root / "claim.json").exists())
        self.root = self.base / "version-failure"
        empty = subprocess.CompletedProcess([], 0, "", "")
        with mock.patch.object(quality.subprocess, "run", return_value=empty):
            with self.assertRaisesRegex(ValueError, "version"):
                self.prepare()
        self.assertTrue((self.root / "cli-evidence.json").is_file())

    def relock_plan(self, plan: dict) -> None:
        """Exercise semantic validation even with internally consistent byte locks."""
        (self.root / "plan.json").write_bytes(quality.canonical(plan) + b"\n")
        (self.root / "plan-lock.json").write_bytes(
            quality.canonical(quality.info(self.root / "plan.json")))
        source_lock = quality.load(self.root / "source-lock.json")
        for entry in source_lock["files"]:
            if Path(entry["path"]).name in ("plan.json", "plan-lock.json"):
                entry.update(quality.info(Path(entry["path"])))
        (self.root / "source-lock.json").write_bytes(quality.canonical(source_lock))
        (self.root / "prepared-lock.json").write_bytes(
            quality.canonical(quality.info(self.root / "source-lock.json")))

    def test_missing_default_and_mismatched_metadata_or_argv_refuse_launch(self) -> None:
        self.prepare()
        files = ("plan.json", "plan-lock.json", "source-lock.json", "prepared-lock.json")
        originals = {name: (self.root / name).read_bytes() for name in files}
        def replace_value(plan, old, new):
            plan["argv"][plan["argv"].index(old)] = new
        def remove_selection(plan, value):
            position = plan["argv"].index(value)
            del plan["argv"][position - 1:position + 1]
        reasoning = 'model_reasoning_effort="high"'
        mutations = [
            lambda p: p.pop("model"),
            lambda p: p["model"].pop("requested"),
            lambda p: p["model"].update(requested=None),
            lambda p: p["model"].update(requested="default"),
            lambda p: p["model"].update(requested="neutral-other"),
            lambda p: p["model"].pop("reasoning_effort"),
            lambda p: p["model"].update(reasoning_effort="medium"),
            lambda p: p["model"].update(served="fabricated-identity"),
            lambda p: p["config"].pop("model"),
            lambda p: p["config"].update(model="default"),
            lambda p: p["config"].pop("reasoning_effort"),
            lambda p: p["cli"].update(version="fabricated-version"),
            lambda p: replace_value(p, "neutral-model", "neutral-other"),
            lambda p: remove_selection(p, "neutral-model"),
            lambda p: replace_value(p, reasoning, 'model_reasoning_effort="medium"'),
            lambda p: remove_selection(p, reasoning),
            lambda p: p["argv"].extend(["-m", "neutral-other"]),
            lambda p: p["argv"].extend(["-c", 'model_reasoning_effort="medium"']),
        ]
        for index, change in enumerate(mutations):
            with self.subTest(mutation=index):
                plan = quality.load(self.root / "plan.json")
                change(plan)
                self.relock_plan(plan)
                with mock.patch.object(quality.subprocess, "Popen") as popen:
                    with self.assertRaises(ValueError):
                        quality.validate(self.root, require_probe=False)
                    with self.assertRaises(ValueError):
                        quality.execute(self.root)
                self.assertFalse(popen.called)
                self.assertFalse((self.root / "claim.json").exists())
                for name, raw in originals.items():
                    (self.root / name).write_bytes(raw)
        path = self.root / "cli-evidence.json"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "Locked bytes"):
            quality.validate(self.root, require_probe=False)

    def test_rejects_private_export_and_snapshot_mismatch(self) -> None:
        evaluator = self.task / "exports/v1-evaluator"
        export_snapshot(self.snapshot, "evaluator")
        with self.assertRaises(ValueError):
            quality.prepare(evaluator, self.snapshot, self.root, self.config)
        second = make_task(self.repository, "other")
        add_reviews(second)
        freeze_task(second, "v2")
        with self.assertRaisesRegex(ValueError, "does not match"):
            quality.prepare(self.solver, second / "versions/v2", self.root, self.config)

    def test_rejects_source_symlink_extra_member_and_instruction_member(self) -> None:
        linked = self.base / "linked"
        linked.symlink_to(self.solver, target_is_directory=True)
        with self.assertRaises(ValueError):
            quality.prepare(linked, self.snapshot, self.root, self.config)
        (self.solver / "teacher.txt").write_text("NEUTRAL_PRIVATE", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.prepare()
        (self.solver / "teacher.txt").unlink()
        value = read_json(self.task / "task.json")
        value["files"][0]["path"] = "draft/input/AGENTS.md"
        (self.task / "draft/input/AGENTS.md").write_text("NEUTRAL_AUTOMATIC_RULE", encoding="utf-8")
        write_json(self.task / "task.json", value)
        add_reviews(self.task)
        freeze_task(self.task, "v2")
        other = self.task / "exports/v2-solver"
        export_snapshot(self.task / "versions/v2", "solver")
        with self.assertRaisesRegex(ValueError, "automatic instruction"):
            quality.prepare(other, self.task / "versions/v2", self.root, self.config)

    def test_rejects_dependency_read_exceptions_inside_protected_trees(self) -> None:
        for protected in (self.repository, self.root):
            dependency = protected / "leaky-dependency"
            dependency.mkdir(parents=True, exist_ok=True)
            with self.subTest(protected=protected):
                config = {**self.config, "dependencies": str(dependency)}
                with self.assertRaisesRegex(ValueError, "Unsafe"):
                    quality.prepare(self.solver, self.snapshot, self.base / "another-runtime",
                                    config if protected == self.repository else {**config, "repository": str(self.root)})
        with self.assertRaisesRegex(ValueError, "outside"):
            quality.prepare(self.solver, self.snapshot, self.repository / "runtime", self.config)

    def test_rejects_unapproved_global_instructions_or_rtk(self) -> None:
        home = self.base / "home/.codex"
        home.mkdir(parents=True, exist_ok=True)
        (home / "AGENTS.md").write_text("NEUTRAL_UNAPPROVED_RULE", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Global AGENTS"):
            self.prepare()
        (home / "AGENTS.md").write_text("@" + str(home / "RTK.md"), encoding="utf-8")
        (home / "RTK.md").write_text("NEUTRAL_UNAPPROVED_RTK", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "neutral digest"):
            self.prepare()
        self.prepare(neutral_rtk_sha256=quality.info(home / "RTK.md")["sha256"])
        (home / "RTK.md").write_text("CHANGED", encoding="utf-8")
        with self.assertRaises(ValueError):
            quality.validate(self.root, require_probe=False)

    def test_new_automatic_instructions_block_later_probe_and_execution(self) -> None:
        self.prepare()
        for path in (self.root / "AGENTS.md", self.root / "work/AGENTS.override.md",
                     self.base / "home/.codex/AGENTS.override.md"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("NEUTRAL_UNAPPROVED", encoding="utf-8")
            with self.subTest(path=path):
                with mock.patch.object(quality.subprocess, "Popen") as popen:
                    with self.assertRaises(ValueError):
                        quality.execute(self.root)
                    self.assertFalse(popen.called)
            path.unlink()

    def test_native_probe_is_required_and_records_original_results(self) -> None:
        self.prepare()
        with self.assertRaises(FileNotFoundError):
            quality.execute(self.root)
        with mock.patch.object(quality.subprocess, "run", side_effect=self.probe_ok) as run:
            result = quality.probe(self.root)
        self.assertEqual(result["model_calls"], 0)
        self.assertTrue((self.root / "ready.json").is_file())
        self.assertFalse((self.root / "work/denied-symlink").exists())
        self.assertIn("sandbox", run.call_args.args[0])
        self.assertIn("/bin/zsh", run.call_args.args[0])
        quality.validate(self.root)
        with self.assertRaises(ValueError):
            quality.probe(self.root)

    def test_probe_failure_or_timeout_never_creates_ready_or_claim(self) -> None:
        self.prepare()
        for result in (subprocess.CompletedProcess([], 0, "[]", ""),
                       subprocess.CompletedProcess([], 1, "", "NEUTRAL_FAILURE"),
                       subprocess.TimeoutExpired("native-fixture", 50)):
            with self.subTest(result=type(result).__name__):
                with mock.patch.object(quality.subprocess, "run") as run:
                    if isinstance(result, Exception):
                        run.side_effect = result
                    else:
                        run.return_value = result
                    with self.assertRaisesRegex(ValueError, "probe failed"):
                        quality.probe(self.root)
                self.assertFalse((self.root / "ready.json").exists())
                self.assertFalse((self.root / "claim.json").exists())
                self.assertFalse((self.root / "work/denied-symlink").exists())

    def test_missing_denied_target_or_nonpermission_error_is_not_isolation(self) -> None:
        self.prepare()
        control = self.root / "control/denied.txt"
        control.unlink()
        with mock.patch.object(quality.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "absent"):
                quality.probe(self.root)
            self.assertFalse(run.called)
        control.write_text("NEUTRAL_CONTROL\n", encoding="utf-8")
        def missing_result(argv, **kwargs):
            result = self.probe_ok(argv, **kwargs)
            rows = json.loads(result.stdout)
            for row in rows:
                if not row["read"]:
                    row["read_errno"] = 2
            return subprocess.CompletedProcess(argv, 0, json.dumps(rows), "")
        with mock.patch.object(quality.subprocess, "run", side_effect=missing_result):
            with self.assertRaisesRegex(ValueError, "probe failed"):
                quality.probe(self.root)
        self.assertFalse((self.root / "ready.json").exists())

    def test_mutation_and_prepopulated_outputs_block_before_model(self) -> None:
        self.ready()
        path = self.root / "work/draft/input/prompt.md"
        original = path.read_bytes()
        for member in (path, self.root / "request.md", self.root / "runner.py"):
            raw = member.read_bytes()
            member.write_bytes(raw + b"CHANGED")
            with mock.patch.object(quality.subprocess, "Popen") as popen:
                with self.assertRaises(ValueError):
                    quality.execute(self.root)
                self.assertFalse(popen.called)
            member.write_bytes(raw)
        self.assertEqual(path.read_bytes(), original)
        path.chmod(0o444)
        with self.assertRaisesRegex(ValueError, "POSIX"):
            quality.execute(self.root)
        path.chmod(0o644)
        (self.root / "scratch/preexisting.txt").write_text("NEUTRAL", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "start empty"):
            quality.execute(self.root)

    def test_symlink_in_output_is_not_followed_or_copied(self) -> None:
        self.ready()
        private = self.base / "private.txt"
        private.write_text("NEUTRAL_PRIVATE", encoding="utf-8")
        (self.root / "work/deliverables/link.txt").symlink_to(private)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            quality.snapshot_outputs(self.root, self.root / "copied")
        self.assertFalse((self.root / "copied").exists())
        for member in ("../private", "/private", "a//b", "a\\b", "./a", ""):
            with self.assertRaises(ValueError):
                quality.relative(member)

    def test_plan_probe_and_interpreter_tampering_fail_closed(self) -> None:
        self.ready()
        readiness = quality.load(self.root / "ready.json")
        probe = self.root / readiness["probe_file"]
        raw = probe.read_bytes()
        probe.write_bytes(raw + b" ")
        with self.assertRaisesRegex(ValueError, "Locked bytes"):
            quality.validate(self.root)
        probe.write_bytes(raw)
        with mock.patch.object(quality.sys, "executable", "/wrong/python"):
            with self.assertRaisesRegex(ValueError, "locked launcher"):
                quality.validate(self.root)
        self.mutate_plan(lambda plan: plan["argv"].append("--dangerously-bypass-approvals-and-sandbox"))
        with self.assertRaisesRegex(ValueError, "Locked bytes"):
            quality.validate(self.root)

    def fake_process(self, argv, **kwargs):
        output = kwargs["stdout"]
        events = [{"type": "thread.started", "thread_id": THREAD},
                  {"type": "item.completed", "item": {"type": "agent_message", "text": "NEUTRAL_FINAL"}},
                  {"type": "turn.completed"}]
        output.write(b"".join(quality.canonical(event) + b"\n" for event in events))
        output.flush()
        (self.root / "work/deliverables/result.txt").write_text("NEUTRAL_RESULT", encoding="utf-8")
        process = mock.Mock(returncode=0, pid=12345)
        process.communicate.return_value = (None, None)
        return process

    def test_explicit_execute_retains_final_answer_artifacts_and_one_claim(self) -> None:
        self.ready()
        with mock.patch.object(quality.subprocess, "Popen", side_effect=self.fake_process) as popen:
            result = quality.execute(self.root)
            self.assertEqual(popen.call_count, 1)
            with self.assertRaises((ValueError, FileExistsError)):
                quality.execute(self.root)
            self.assertEqual(popen.call_count, 1)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["thread_id"], THREAD)
        self.assertEqual((self.root / "run/answer.md").read_text(), "NEUTRAL_FINAL")
        self.assertEqual((self.root / "run/artifacts/result.txt").read_text(), "NEUTRAL_RESULT")
        self.assertTrue((self.root / "run/trace-summary.json").is_file())
        self.assertEqual(result["model"]["requested"], "neutral-model")
        self.assertEqual(result["model"]["reasoning_effort"], "high")
        self.assertIsNone(result["model"]["served"])
        self.assertEqual(result["cli"]["version"], "codex-cli fixture")
        self.assertNotIn("NEUTRAL_FINAL", json.dumps(result))

    def test_failed_launch_is_recorded_and_cannot_be_retried(self) -> None:
        self.ready()
        with mock.patch.object(quality.subprocess, "Popen", side_effect=OSError("NEUTRAL_FAILURE")) as popen:
            result = quality.execute(self.root)
            self.assertEqual(result["status"], "launch_failed")
            self.assertEqual(result["exceptions"], ["OSError"])
            self.assertEqual(result["model"]["requested"], "neutral-model")
            self.assertEqual(result["model"]["reasoning_effort"], "high")
            self.assertEqual(result["cli"]["version"], "codex-cli fixture")
            with self.assertRaises(FileExistsError):
                quality.execute(self.root)
            self.assertEqual(popen.call_count, 1)

    def test_timeout_preserves_evidence_and_terminates_without_retry(self) -> None:
        self.ready()
        process = mock.Mock(returncode=-15, pid=12345)
        process.communicate.side_effect = subprocess.TimeoutExpired("neutral", 1)
        with mock.patch.object(quality.subprocess, "Popen", return_value=process) as popen:
            with mock.patch.object(quality.os, "killpg") as terminate:
                result = quality.execute(self.root)
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(popen.call_count, 1)
        terminate.assert_called_once()

    def test_trace_exit_is_not_a_completed_solution(self) -> None:
        path = self.base / "trace.jsonl"
        fixtures = [b"", b"INVALID\n", b"[]\n",
                    quality.canonical({"type": "thread.started", "thread_id": "bad"}) + b"\n",
                    quality.canonical({"type": "turn.failed"}) + b"\n",
                    quality.canonical({"type": "error"}) + b"\n"]
        for raw in fixtures:
            path.write_bytes(raw)
            summary, _ = quality.trace_summary(path)
            self.assertFalse(summary["complete"])

    def test_bounds_and_cli_missing_evidence(self) -> None:
        raw = self.base / "large"
        raw.write_bytes(b"1234")
        with self.assertRaises(ValueError):
            quality.read_regular(self.base, "large", limit=3)
        with self.assertRaises(ValueError):
            self.prepare(timeout=0)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(quality.main(["validate", str(self.root)]), 2)
        self.ready()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(quality.main(["validate", str(self.root)]), 0)

    def test_dependency_hash_streams_past_public_limit_without_relaxing_inputs(self) -> None:
        binary = self.base / "large-dependency"
        with binary.open("wb") as handle:
            handle.truncate(quality.MAX_FILE + 1)
        observed = quality.info(binary)
        self.assertEqual(observed["size"], quality.MAX_FILE + 1)
        with self.assertRaises(ValueError):
            quality.read_regular(self.base, binary.name)
        with mock.patch.object(quality, "MAX_DEPENDENCY", 4):
            with self.assertRaisesRegex(ValueError, "bounded ordinary dependency"):
                quality.info(binary)

    def test_draft_without_reviews_prepares_without_freezing_or_exporting(self) -> None:
        task = make_task(self.repository, "draft-only")
        source_files = quality.file_tree(task)
        with mock.patch.object(quality.subprocess, "Popen") as popen:
            result = self.draft(task)
        self.assertFalse(popen.called)
        self.assertEqual(result["source_kind"], "draft_candidate")
        self.assertEqual(quality.file_tree(task), source_files)
        self.assertEqual(list((task / "versions").iterdir()), [])
        self.assertEqual(list((task / "exports").iterdir()), [])
        self.assertEqual(list((task / "reports").iterdir()), [])
        plan = quality.validate(self.root, require_probe=False)
        binding = plan["source_binding"]
        self.assertEqual(binding["kind"], "draft_candidate")
        self.assertFalse(binding["reviews_required_for_preparation"])
        self.assertIn("content_digest", binding)
        package = quality.load(self.root / "work/package.json")
        self.assertEqual(set(package), {"schema_version", "audience", "task_id", "version_id", "input_digest", "files"})
        self.assertTrue(package["version_id"].startswith("candidate-"))
        self.assertNotIn("content_digest", package)
        self.assertNotIn("scoring", package)
        self.assertNotIn("answer", json.dumps(package))
        self.assertEqual(set(plan["public_tree"]), {"package.json", "draft/input/prompt.md"})

    def test_draft_invalid_path_and_symlink_do_not_prepare(self) -> None:
        task = make_task(self.repository, "invalid-draft")
        value = read_json(task / "task.json")
        original = (task / "task.json").read_bytes()
        value["files"][0]["path"] = "../outside.txt"
        write_json(task / "task.json", value)
        with self.assertRaisesRegex(ValueError, "structural capture failed"):
            self.draft(task)
        self.assertFalse(self.root.exists())
        (task / "task.json").write_bytes(original)
        prompt = task / "draft/input/prompt.md"
        prompt.unlink()
        prompt.symlink_to(task / "draft/evaluation/answer.md")
        with self.assertRaisesRegex(ValueError, "structural capture failed"):
            self.draft(task)
        self.assertFalse(self.root.exists())

    def test_draft_capture_rejects_change_between_checks(self) -> None:
        from taskflow import packages
        task = make_task(self.repository, "moving-draft")
        original = packages.check_task
        calls = 0
        def changing_check(root, **kwargs):
            nonlocal calls
            result = original(root, **kwargs)
            calls += 1
            if calls == 1:
                (task / "draft/input/prompt.md").write_text("NEUTRAL_CHANGED_INPUT\n", encoding="utf-8")
            return result
        with mock.patch.object(packages, "check_task", side_effect=changing_check):
            with self.assertRaisesRegex(ValueError, "changed between"):
                self.draft(task)
        self.assertFalse(self.root.exists())

    def test_draft_source_drift_blocks_launch_but_does_not_change_copied_binding(self) -> None:
        task = make_task(self.repository, "source-draft")
        self.draft(task)
        copied = (self.root / "work/draft/input/prompt.md").read_bytes()
        (task / "draft/input/prompt.md").write_text("NEUTRAL_CHANGED_INPUT\n", encoding="utf-8")
        with mock.patch.object(quality.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(ValueError, "source is stale"):
                quality.execute(self.root)
            self.assertFalse(popen.called)
        plan = quality.validate(self.root, require_probe=False, before_run=False)
        self.assertEqual(quality.source_freshness(plan["source_binding"])["state"], "stale")
        self.assertEqual((self.root / "work/draft/input/prompt.md").read_bytes(), copied)

    def test_draft_change_during_run_is_source_staleness_not_input_mismatch(self) -> None:
        task = make_task(self.repository, "running-draft")
        self.draft(task)
        with mock.patch.object(quality.subprocess, "run", side_effect=self.probe_ok):
            quality.probe(self.root)
        def mutate_source(argv, **kwargs):
            process = self.fake_process(argv, **kwargs)
            (task / "draft/input/prompt.md").write_text("NEUTRAL_CHANGED_INPUT\n", encoding="utf-8")
            return process
        with mock.patch.object(quality.subprocess, "Popen", side_effect=mutate_source):
            result = quality.execute(self.root)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["input_readback"], "matched")
        self.assertEqual(result["source_freshness"]["state"], "stale")
        self.assertFalse(result["current_source_eligible"])


if __name__ == "__main__":
    unittest.main()
