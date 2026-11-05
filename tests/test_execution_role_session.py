"""Explicit role-home/session tests with neutral local subprocesses only."""

import copy
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution_role_session as sessions
from taskflow import execution_runner as runner
from taskflow.execution_codex_environment import process_environment
from tests import test_execution_codex_provider as provider_fixtures


SID = "12345678-1234-4567-8123-123456789012"
BINDING = {"candidate_id": "neutral-candidate", "task_id": "neutral-task",
           "input_digest": "public-bytes", "content_digest": "candidate-bytes",
           "evaluation_digest": "reference-bytes"}


def reference(path: Path) -> dict:
    return {"path": str(path), **runner._info(path), "location": "neutral original"}


class RoleSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.home = self.base / "role-home"
        self.home.mkdir()
        self.public = self.base / "public.md"
        self.public.write_text("Neutral public facts.\n", encoding="utf-8")
        self.cli = self.base / "neutral.py"
        self.cli.write_text(
            "#!" + sys.executable + "\n"
            "import json,os,sys\n"
            "from pathlib import Path\n"
            "request=sys.stdin.read()\n"
            "sid='87654321-4321-4567-8123-123456789012' if 'WRONG_SID' in request else '" + SID + "'\n"
            "Path('answer.json').write_text(json.dumps({'home':os.environ.get('CODEX_HOME')}))\n"
            "print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)\n"
            "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Synthetic delivery.'}}),flush=True)\n"
            "print(json.dumps({'type':'turn.completed'}),flush=True)\n", encoding="utf-8")
        self.cli.chmod(0o755)
        self.number = 0

    def config(self, mode: str = "fresh_persistent", previous: dict | None = None) -> tuple[Path, dict]:
        self.number += 1
        work = self.base / ("work-" + str(self.number))
        work.mkdir()
        root = self.base / ("attempt-" + str(self.number))
        return root, {"intent_id": "intent-" + str(self.number), "policy_version": "policy-v1",
                      "role": "quality", "mode": "test_fixture", "model": "neutral",
                      "reasoning_effort": "high", "cwd": str(work),
                      "argv": [str(self.cli)], "dependencies": [str(self.cli)],
                      "candidate_binding": copy.deepcopy(BINDING), "outputs": ["answer.json"],
                      "inputs": [{"path": str(self.public), "target": "public/original.md", "kind": "public"}],
                      "request_sections": [{"kind": "operation", "text": "Create answer.json."}],
                      "role_session": {"mode": mode, "home": str(self.home), "previous": previous}}

    def completed(self, mode: str = "fresh_persistent") -> tuple[Path, dict]:
        root, config = self.config(mode)
        runner.prepare_attempt(root, config)
        result = runner.run_attempt(root)
        self.assertTrue(result["technical_accepted"], result)
        return root, reference(root / "collection.json")

    @staticmethod
    def snapshot(root: Path) -> dict:
        return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}

    def test_completed_source_resumes_exact_sid_in_new_invocation_without_changing_original(self) -> None:
        original, previous = self.completed()
        before = self.snapshot(original)
        root, config = self.config("resume_existing", previous)
        config["request_sections"][0]["text"] = "Correct only the declared format in answer.json."
        manifest = runner.prepare_attempt(root, config)
        self.assertEqual(SID, manifest["resume_sid"])
        self.assertEqual(previous, manifest["role_session"]["previous"])
        self.assertNotEqual(original, root)
        result = runner.run_attempt(root)
        self.assertEqual(SID, result["sid"])
        self.assertTrue(result["technical_accepted"])
        output = json.loads((root / "artifacts/answer.json").read_text(encoding="utf-8"))
        self.assertEqual(str(self.home), output["home"])
        self.assertEqual(before, self.snapshot(original))
        invocation = json.loads((root / "role-session-invocation.json").read_text(encoding="utf-8"))
        self.assertEqual(config["intent_id"], invocation["intent_id"])
        self.assertEqual(SID, invocation["resume_sid"])
        self.assertEqual(result, runner.reconcile_attempt(root))
        with self.assertRaises(runner.RunnerError):
            runner.run_attempt(root)

    def test_resume_rejects_changed_home_model_candidate_policy_input_or_caller_sid(self) -> None:
        _, previous = self.completed()
        other = self.base / "other-home"
        other.mkdir()
        for field in ("home", "model", "candidate", "policy", "input", "sid"):
            root, config = self.config("resume_existing", previous)
            if field == "home":
                config["role_session"]["home"] = str(other)
            elif field == "candidate":
                config["candidate_binding"]["candidate_id"] = "different"
            elif field == "policy":
                config["policy_version"] = "different"
            elif field == "input":
                config["inputs"][0]["target"] = "public/different.md"
            elif field == "sid":
                config["resume_sid"] = "different"
            else:
                config["model"] = "different"
            with self.subTest(field=field), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_old_ephemeral_and_uncompleted_contexts_cannot_resume(self) -> None:
        _, previous = self.completed("fresh_ephemeral")
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "persistent"):
            runner.prepare_attempt(root, config)
        root, config = self.config("resume_existing", {**previous, "path": str(self.base / "absent/collection.json")})
        with self.assertRaises((OSError, runner.RunnerError)):
            runner.prepare_attempt(root, config)

    def test_source_reference_and_locked_original_drift_are_rejected(self) -> None:
        original, previous = self.completed()
        root, config = self.config("resume_existing", {**previous, "sha256": "wrong"})
        with self.assertRaisesRegex(runner.RunnerError, "reference changed"):
            runner.prepare_attempt(root, config)
        locked = original / "inputs/public/original.md"
        locked.chmod(0o644)
        locked.write_text("Changed", encoding="utf-8")
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "staged input changed"):
            runner.prepare_attempt(root, config)

    def test_changed_original_trace_is_rejected_before_a_resume(self) -> None:
        original, previous = self.completed()
        with (original / "trace.jsonl").open("ab") as handle:
            handle.write(b'{"type":"foreign-turn"}\n')
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "drifted"):
            runner.prepare_attempt(root, config)

    def test_same_path_executable_change_cannot_resume_the_original_runtime(self) -> None:
        _, previous = self.completed()
        with self.cli.open("a", encoding="utf-8") as handle:
            handle.write("\n# A different executable at the same path.\n")
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "capabilities"):
            runner.prepare_attempt(root, config)

    def test_original_copied_helper_change_cannot_be_used_as_resume_evidence(self) -> None:
        original, previous = self.completed()
        with (original / "role_session.py").open("a", encoding="utf-8") as handle:
            handle.write("\n# Changed copied runtime helper.\n")
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "runtime dependency changed"):
            runner.prepare_attempt(root, config)

    def test_changed_sid_in_actual_resume_is_not_technically_accepted(self) -> None:
        _, previous = self.completed()
        root, config = self.config("resume_existing", previous)
        config["request_sections"][0]["text"] = "WRONG_SID neutral negative fixture."
        runner.prepare_attempt(root, config)
        result = runner.run_attempt(root)
        self.assertFalse(result["technical_accepted"])
        self.assertFalse(result["model_completed"])

    def test_noncompleted_original_cannot_supply_a_resume(self) -> None:
        root, config = self.config()
        config["request_sections"][0]["text"] = "WRONG_SID neutral fixture."
        # An explicit expected SID is unsupported for fresh mode. Instead the
        # neutral process produces no terminal event, leaving a sealed failure.
        self.cli.write_text(self.cli.read_text(encoding="utf-8").replace(
            "print(json.dumps({'type':'turn.completed'}),flush=True)", "pass"), encoding="utf-8")
        runner.prepare_attempt(root, config)
        result = runner.run_attempt(root)
        self.assertFalse(result["technical_accepted"])
        next_root, next_config = self.config("resume_existing", reference(root / "collection.json"))
        with self.assertRaisesRegex(runner.RunnerError, "technically completed"):
            runner.prepare_attempt(next_root, next_config)

    def test_distinct_resume_branches_cannot_consume_the_same_original_twice(self) -> None:
        _, previous = self.completed()
        first, first_config = self.config("resume_existing", previous)
        second, second_config = self.config("resume_existing", previous)
        runner.prepare_attempt(first, first_config)
        runner.prepare_attempt(second, second_config)
        self.assertTrue(runner.run_attempt(first)["technical_accepted"])
        with self.assertRaisesRegex(runner.RunnerError, "occupied invocation"):
            runner.run_attempt(second)
        self.assertFalse((second / "dispatch.json").exists())

    def test_completed_successor_does_not_allow_path_alias_or_copied_source_consumption(self) -> None:
        original, previous = self.completed()
        first, first_config = self.config("resume_existing", previous)
        runner.prepare_attempt(first, first_config)
        self.assertTrue(runner.run_attempt(first)["technical_accepted"])
        alias = {**previous, "path": str(original) + "/./collection.json"}
        copied = self.base / "copied-original"
        shutil.copytree(original, copied)
        copied_ref = {**previous, "path": str(copied / "collection.json")}
        for source in (alias, copied_ref):
            root, config = self.config("resume_existing", source)
            with self.subTest(path=source["path"]), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)
            self.assertFalse((root / "dispatch.json").exists())

    def test_unknown_launch_remains_occupied_and_does_not_start_again(self) -> None:
        root, config = self.config()
        runner.prepare_attempt(root, config)
        self.assertEqual("uncertain", runner.run_attempt(root, fault="before_start")["status"])
        with self.assertRaises(runner.RunnerError):
            runner.run_attempt(root)
        self.assertFalse((Path(config["cwd"]) / "answer.json").exists())

    def test_fresh_home_is_bound_once_and_rejects_prior_or_ambient_files(self) -> None:
        root, config = self.config()
        runner.prepare_attempt(root, config)
        second, config2 = self.config()
        with self.assertRaisesRegex(runner.RunnerError, "empty"):
            runner.prepare_attempt(second, config2)
        (self.home / "unexpected-config.toml").write_text("fixture", encoding="utf-8")
        with self.assertRaisesRegex(runner.RunnerError, "changed before"):
            runner.run_attempt(root)

    def test_declared_home_cannot_be_global_relative_symlink_or_inside_work(self) -> None:
        link = self.base / "link-home"
        link.symlink_to(self.home)
        for value in ("relative", str(link), str(Path.home() / ".codex")):
            root, config = self.config()
            config["role_session"]["home"] = value
            with self.subTest(home=value), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)
        root, config = self.config()
        config["role_session"]["home"] = config["cwd"]
        with self.assertRaisesRegex(runner.RunnerError, "separate"):
            runner.prepare_attempt(root, config)

    def test_schema_is_explicit_quality_only_and_cannot_mix_old_sessions(self) -> None:
        for change in ({"role": "author"}, {"mode": "existing_isolated"}, {"quality_session": {}},
                       {"candidate_binding": {}}, {"resume_sid": "last"},
                       {"role_session": {"mode": "last", "home": str(self.home), "previous": None}}):
            root, config = self.config()
            config.update(change)
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_home_mutex_rejects_another_live_owner(self) -> None:
        root, config = self.config()
        manifest = runner.prepare_attempt(root, config)
        with sessions.execution_lock(manifest):
            with self.assertRaisesRegex(sessions.RoleSessionError, "active invocation"):
                with sessions.execution_lock(manifest):
                    self.fail("mutex admitted a second owner")
        with sessions.execution_lock({}):
            pass

    def test_original_role_home_marker_cannot_be_changed_or_replaced_by_a_symlink(self) -> None:
        _, previous = self.completed()
        marker = self.home / sessions.BINDING_NAME
        original = marker.read_bytes()
        marker.write_text("{}", encoding="utf-8")
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "binding changed"):
            runner.prepare_attempt(root, config)
        saved = self.base / "saved-marker.json"
        saved.write_bytes(original)
        marker.unlink()
        marker.symlink_to(saved)
        root, config = self.config("resume_existing", previous)
        with self.assertRaisesRegex(runner.RunnerError, "ordinary file"):
            runner.prepare_attempt(root, config)

    def test_process_environment_binds_explicit_home_and_selected_credential_only(self) -> None:
        selected = {"codex_home": str(self.home), "credential_env": "OPENAI_API_KEY"}
        with patch.dict(os.environ, {"OPENAI_API_KEY": "neutral-key", "OTHER_API_KEY": "neutral-other"}):
            environment = process_environment(selected, role_home=str(self.home))
        self.assertEqual(str(self.home), environment["CODEX_HOME"])
        self.assertEqual("neutral-key", environment["OPENAI_API_KEY"])
        self.assertNotIn("OTHER_API_KEY", environment)
        with self.assertRaisesRegex(ValueError, "differs"):
            process_environment(selected, role_home=str(self.base))

    def test_nonreview_identity_has_no_unrelated_review_run_binding_field(self) -> None:
        for role in ("quality", "solver"):
            root, config = self.config()
            home = self.base / (role + "-specific-home")
            home.mkdir()
            config["role"] = role
            config["role_session"]["home"] = str(home)
            manifest = runner.prepare_attempt(root, config)
            identity = sessions.identity(manifest, root)
            with self.subTest(role=role):
                self.assertNotIn("run_binding", identity)
                self.assertEqual(identity, sessions.identity({**manifest, "run_binding": None}, root))
                self.assertEqual(sessions._digest(identity), manifest["role_session_binding"]["identity"])
                self.assertEqual(manifest, runner._validate(root))

    def test_normalization_does_not_move_a_sibling_read_permission(self) -> None:
        first = {"candidate_binding": BINDING, "role": "quality", "mode": "test_fixture", "policy_version": "v1",
                 "model": {"requested": "neutral", "reasoning_effort": "high"}, "cwd": "/a/work",
                 "inputs": [], "outputs": ["answer.json"], "argv": ["/a/work-sibling/cli"]}
        second = {**first, "cwd": "/b/work", "argv": ["/b/work-sibling/cli"]}
        self.assertNotEqual(sessions.identity(first, Path("/a/run")), sessions.identity(second, Path("/b/run")))


class NativeRoleSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = provider_fixtures.CodexProviderTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.config = {**self.case.config, "role": "quality", "candidate_binding": copy.deepcopy(BINDING),
                       "role_session": {"mode": "fresh_persistent", "home": str(self.case.home), "previous": None}}

    def test_native_persistent_omits_ephemeral_and_home_is_actual_provider_home(self) -> None:
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=self.case.capture):
            manifest = runner.prepare_attempt(self.case.case.root, self.config)
        self.assertNotIn("--ephemeral", manifest["argv"])
        environment = runner._process_environment(manifest["native"], manifest["role_session"])
        self.assertEqual(str(self.case.home), environment["CODEX_HOME"])
        # This checks only generation of the CLI command. No permission probe,
        # CLI invocation, session original or provider connection is fabricated.
        root = self.case.root / "resume-command"
        root.mkdir()
        config = copy.deepcopy(self.config)
        config["role_session"]["mode"] = "resume_existing"
        config["resume_sid"] = SID
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=self.case.capture):
            arguments, _, _ = runner._native_command(config, root)
        self.assertIn("resume", arguments)
        self.assertIn(SID, arguments)
        self.assertNotIn("--ephemeral", arguments)

    def test_native_ephemeral_selects_flag_and_provider_home_mismatch_is_rejected(self) -> None:
        self.config["role_session"]["mode"] = "fresh_ephemeral"
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=self.case.capture):
            manifest = runner.prepare_attempt(self.case.case.root, self.config)
        self.assertIn("--ephemeral", manifest["argv"])
        other = self.case.root / "other-home"
        other.mkdir()
        config = copy.deepcopy(self.config)
        config["role_session"]["home"] = str(other)
        config["native_permissions"]["filesystem"][str(other)] = "deny"
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=self.case.capture):
            with self.assertRaisesRegex(runner.RunnerError, "provider Codex home differs"):
                runner.prepare_attempt(self.case.root / "other-runtime", config)


if __name__ == "__main__":
    unittest.main()
