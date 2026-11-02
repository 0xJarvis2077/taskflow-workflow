"""Offline publication tests: temporary files and a local bare Git transport."""

import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools import github_sync as sync


IGNORE = """*
!/.gitignore
!/AGENTS.md
!/README.md
!/pyproject.toml
!/knowledge/
/knowledge/*
!/knowledge/index.md
!/skills/
/skills/*
!/skills/high-value-task-workflow/
/skills/high-value-task-workflow/*
!/skills/high-value-task-workflow/SKILL.md
!/tools/
/tools/*
!/tools/*.py
!/tests/
/tests/*
!/tests/*.py
!/docs/
/docs/*
!/docs/*.md
!/config/
/config/*
!/config/examples/
/config/examples/*
!/config/examples/*.json
"""


class TemporaryProject(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source"
        self.source.mkdir()
        for name in sync.REQUIRED_FILES:
            self.write(name, "# Public workflow\n" if name.endswith(".md") else "")
        self.write(".gitignore", IGNORE)
        self.write("tests/test_smoke.py", "import unittest\nclass Smoke(unittest.TestCase):\n def test_ok(self): self.assertTrue(True)\n")
        self.config_path = self.root / "local.json"
        self.raw_config = {
            "source": str(self.source), "mirror": str(self.root / "mirror"),
            "repository": "example/workflow", "branch": "main",
            "state_file": str(self.root / "private" / "state.json"),
            "identity": {"name": "Workflow publisher", "email": "workflow@example.invalid"},
        }
        self.config_path.write_text(json.dumps(self.raw_config))
        self.config = sync.load_config(self.config_path)

    def write(self, name, content):
        path = self.source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content if isinstance(content, bytes) else content.encode())
        return path

    def update_config(self, **changes):
        self.raw_config.update(changes)
        self.config_path.write_text(json.dumps(self.raw_config))
        return sync.load_config(self.config_path)

    def local_transport(self):
        """Map only remote commands to a local bare repository; never network."""
        bare = self.root / "remote.git"
        sync._run(["git", "init", "--bare", "--quiet", str(bare)])
        original = sync._git

        def git(mirror, *args, check=True):
            if args and args[0] in {"push", "ls-remote"}:
                args = tuple(str(bare) if x == "origin" else x for x in args)
            return original(mirror, *args, check=check)

        return bare, patch.object(sync, "_git", side_effect=git)

    def runner(self):
        return patch.object(sync, "_test_snapshot", return_value={"tests": 1, "log": "private-test-log"})


class PublicBoundaryTests(TemporaryProject):
    def test_collect_only_ignored_allowed_files(self):
        self.write("tasks/private.md", "private task")
        self.write("knowledge/reviews/private.md", "private review")
        self.write("docs/guide.md", "# Guide\n[Workflow](../README.md#public-workflow)\n")
        self.write("config/examples/basic.json", '{"enabled": true}')
        self.write("tools/example.py", "x = 1\n")
        files = sync.collect_public(self.source)
        self.assertIn("docs/guide.md", files)
        self.assertNotIn("tasks/private.md", files)
        self.assertFalse((self.source / ".git").exists())
        self.assertEqual(sync._digest(files), sync._digest(dict(reversed(list(files.items())))))

    def test_allowlist_is_independent(self):
        for name in ("tasks/answer.md", "AGENTS.local.md", "docs/private.local.md",
                     "knowledge/sources/raw.md", "tools/nested/helper.py", "../README.md", "/README.md", "tools\\x.py"):
            with self.subTest(name=name):
                self.assertFalse(sync.allowed_path(name))
        self.write(".gitignore", "")
        self.write("tasks/answer.md", "private")
        with self.assertRaisesRegex(sync.SyncError, "outside_public_scope:tasks/answer.md"):
            sync.collect_public(self.source)

    def test_secret_rejection_does_not_echo_secret(self):
        samples = {
            "personal_home": "/" + "Users/employee/project",
            "private_meeting": "https://tenant." + "feishu.cn/docx/private",
            "chat_identity": "01abcdef" + "-1234-5678-9abc-0123456789ab",
            "private_key": "-----BEGIN " + "PRIVATE KEY-----",
            "credential": "ghp" + "_" + "A" * 32,
        }
        for kind, value in samples.items():
            with self.subTest(kind=kind):
                self.write("README.md", value)
                with self.assertRaises(sync.SyncError) as caught:
                    sync.collect_public(self.source)
                self.assertIn(kind, str(caught.exception))
                self.assertNotIn(value, str(caught.exception))

    def test_current_home_rejected(self):
        self.write("README.md", "operator-home-location")
        with patch.object(sync.Path, "home", return_value=Path("operator-home-location")):
            with self.assertRaisesRegex(sync.SyncError, "current_home"):
                sync.collect_public(self.source)

    def test_non_utf8_syntax_symlinks_and_size(self):
        for name, value, reason in (
            ("README.md", b"\xff", "non_utf8"),
            ("tools/broken.py", "def (", "invalid_syntax"),
            ("config/examples/broken.json", "{", "invalid_syntax"),
        ):
            with self.subTest(reason=reason):
                path = self.write(name, value)
                with self.assertRaisesRegex(sync.SyncError, reason):
                    sync.collect_public(self.source)
                path.unlink()
                self.write("README.md", "# Public workflow\n")
        large = self.write("tools/large.py", b" " * (5 * 1024 * 1024 + 1))
        with self.assertRaisesRegex(sync.SyncError, "invalid_public_file"):
            sync.collect_public(self.source)
        large.unlink()
        link = self.source / "tools/link.py"
        link.symlink_to(self.source / "tests/test_smoke.py")
        with self.assertRaisesRegex(sync.SyncError, "symlink"):
            sync.collect_public(self.source)

    def test_required_files_and_file_count(self):
        (self.source / "AGENTS.md").unlink()
        with self.assertRaisesRegex(sync.SyncError, "incomplete_or_oversized"):
            sync.collect_public(self.source)
        with patch.object(sync, "_run", side_effect=[subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0, stdout=b"x\0" * 2001)]):
            with self.assertRaisesRegex(sync.SyncError, "incomplete_or_oversized"):
                sync.collect_public(self.source)

    def test_links_and_duplicate_anchors(self):
        self.write("README.md", "# Public workflow\n# Public workflow\n<a id='custom'></a>\n[Again](#public-workflow-1)\n[Custom](#custom)\n[Web](https://example.invalid)\n")
        sync.collect_public(self.source)
        for link, reason in (("missing.md", "broken_link"), ("#unknown", "broken_link"),
                             ("../outside.md", "outside_link")):
            self.write("README.md", f"[Bad]({link})\n")
            with self.assertRaisesRegex(sync.SyncError, reason):
                sync.collect_public(self.source)


class ConfigAndCommandTests(TemporaryProject):
    def test_invalid_config_values(self):
        for changes, reason in (({"repository": "bad"}, "invalid_repository"),
                ({"repository": []}, "invalid_repository"),
                ({"branch": "main..bad"}, "invalid_branch"),
                ({"branch": None}, "invalid_branch"),
                ({"identity": []}, "invalid_identity"),
                ({"identity": {"name": ""}}, "invalid_identity"),
                ({"mirror": ""}, "missing_config:mirror")):
            original = dict(self.raw_config)
            with self.subTest(changes=changes), self.assertRaisesRegex(sync.SyncError, reason):
                self.update_config(**changes)
            self.raw_config = original
        self.config_path.write_text("[]")
        with self.assertRaisesRegex(sync.SyncError, "invalid_config"):
            sync.load_config(self.config_path)

    def test_unsafe_config_scopes(self):
        for path in (self.source, self.source / "tools/shadow", self.source / ".git"):
            with self.subTest(path=path), self.assertRaises(sync.SyncError):
                self.update_config(mirror=str(path))
        self.raw_config["mirror"] = str(self.root / "mirror")
        link = self.root / "link"
        link.symlink_to(self.source, target_is_directory=True)
        with self.assertRaisesRegex(sync.SyncError, "unsafe_config_path"):
            self.update_config(mirror=str(link))

    def test_preview_does_not_initialize_or_run_tests(self):
        with patch.object(sync, "_test_snapshot") as runner:
            result = sync.synchronize(self.config)
        self.assertEqual(result["status"], "preview")
        runner.assert_not_called()
        self.assertFalse(self.config["mirror"].exists())
        self.assertFalse(self.config["state_file"].exists())

    def test_main_preview_and_safe_errors(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(sync.main(["--config", str(self.config_path)]), 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "preview")
        self.config_path.write_text("invalid")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(sync.main(["--config", str(self.config_path)]), 2)
        self.assertEqual(json.loads(output.getvalue())["error"], "JSONDecodeError")

    def test_command_failure_redacts_process_output(self):
        result = subprocess.CompletedProcess(["git"], 7, b"private", b"credential")
        with patch.object(sync.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(sync.SyncError, "^command_failed:git:7$"):
                sync._run(["git", "status"])

    def test_state_symlink_refused(self):
        target = self.root / "data"
        target.write_text("untouched")
        link = self.root / "state-link"
        link.symlink_to(target)
        with self.assertRaisesRegex(sync.SyncError, "unsafe_state_file"):
            sync._save(link, {})
        self.assertEqual(target.read_text(), "untouched")


class MirrorFlowTests(TemporaryProject):
    def test_push_update_delete_and_no_change(self):
        self.write("tools/removable.py", "x = 1\n")
        bare, transport = self.local_transport()
        with transport, self.runner() as runner:
            first = sync.synchronize(self.config, push=True)
            self.assertEqual(first["status"], "pushed")
            self.assertEqual(sync.synchronize(self.config, push=True)["status"], "no_changes")
            runner.assert_called_once()
            (self.source / "tools/removable.py").unlink()
            self.write("README.md", "# Public workflow\nUpdated.\n")
            second = sync.synchronize(self.config, push=True)
        self.assertNotEqual(first["commit"], second["commit"])
        self.assertFalse((self.config["mirror"] / "tools/removable.py").exists())
        remote = sync._run(["git", "--git-dir", str(bare), "rev-parse", "refs/heads/main"]).stdout.decode().strip()
        self.assertEqual(remote, second["commit"])
        self.assertFalse((self.source / ".git").exists())

    def test_failed_push_retries_exact_commit_without_tests(self):
        _, transport = self.local_transport()
        with transport, self.runner() as runner:
            original_push = sync._push
            with patch.object(sync, "_push", side_effect=sync.SyncError("transport_unavailable")):
                with self.assertRaisesRegex(sync.SyncError, "transport_unavailable"):
                    sync.synchronize(self.config, push=True)
            state = json.loads(self.config["state_file"].read_text())
            pending = state["pending_commit"]
            self.write("README.md", "# Public workflow\nNewer local edit.\n")
            with patch.object(sync, "_push", wraps=original_push) as push:
                result = sync.synchronize(self.config, push=True)
                push.assert_called_once_with(self.config, pending)
            self.assertEqual(result["commit"], pending)
            self.assertTrue(result["source_has_newer_changes"])
            self.assertEqual(runner.call_count, 1)
            count = sync._git(self.config["mirror"], "rev-list", "--count", "HEAD")
            self.assertEqual(count, "1")

    def test_already_remote_pending_commit_is_verified_without_repush(self):
        _, transport = self.local_transport()
        original = sync._push
        def uploaded_but_unverified(config, commit):
            original(config, commit)
            raise sync.SyncError("remote_verification_failed")
        with transport, self.runner(), patch.object(sync, "_push", side_effect=uploaded_but_unverified):
            with self.assertRaises(sync.SyncError):
                sync.synchronize(self.config, push=True)
            with patch.object(sync, "_push") as push:
                result = sync.synchronize(self.config, push=True)
                push.assert_not_called()
            self.assertEqual(result["status"], "pushed_pending_commit")

    def test_tests_failure_and_source_change_block_commit(self):
        _, transport = self.local_transport()
        with transport:
            with patch.object(sync, "_test_snapshot", side_effect=sync.SyncError("tests_failed_or_empty")):
                with self.assertRaisesRegex(sync.SyncError, "tests_failed"):
                    sync.synchronize(self.config, push=True)
            def changed(snapshot, log):
                self.write("README.md", "# Public workflow\nChanged during tests.\n")
                return {"tests": 1, "log": str(log)}
            with patch.object(sync, "_test_snapshot", side_effect=changed):
                with self.assertRaisesRegex(sync.SyncError, "source_changed_during_validation"):
                    sync.synchronize(self.config, push=True)
            self.assertFalse(sync._git(self.config["mirror"], "rev-parse", "--verify", "HEAD", check=False))

    def test_origin_mismatch_dirty_tree_and_branch_mismatch(self):
        _, transport = self.local_transport()
        with transport, self.runner():
            sync.synchronize(self.config, push=True)
            mirror = self.config["mirror"]
            sync._git(mirror, "remote", "set-url", "origin", "https://github.com/other/repo.git")
            with self.assertRaisesRegex(sync.SyncError, "origin_mismatch"):
                sync.synchronize(self.config, push=True)
            sync._git(mirror, "remote", "set-url", "origin", "https://github.com/example/workflow.git")
            (mirror / "stray").write_text("private")
            with self.assertRaisesRegex(sync.SyncError, "mirror_has_local_changes"):
                sync.synchronize(self.config, push=True)
            (mirror / "stray").unlink()
            sync._git(mirror, "checkout", "-b", "other")
            with self.assertRaisesRegex(sync.SyncError, "mirror_branch_mismatch"):
                sync.synchronize(self.config, push=True)

    def test_remote_ahead_blocks_without_reset(self):
        bare, transport = self.local_transport()
        with transport, self.runner():
            result = sync.synchronize(self.config, push=True)
            other = self.root / "other"
            sync._run(["git", "clone", "--quiet", "--branch", "main", str(bare), str(other)])
            sync._git(other, "config", "user.name", "Another publisher")
            sync._git(other, "config", "user.email", "other@example.invalid")
            (other / "README.md").write_text("# Remote update\n")
            sync._git(other, "commit", "-am", "Remote change")
            sync._run(["git", "-C", str(other), "push", "--quiet", str(bare), "main"])
            with self.assertRaisesRegex(sync.SyncError, "remote_head_changed"):
                sync.synchronize(self.config, push=True)
            self.assertEqual(sync._git(self.config["mirror"], "rev-parse", "HEAD"), result["commit"])

    def test_nonempty_new_mirror_and_invalid_identity(self):
        self.config["mirror"].mkdir()
        (self.config["mirror"] / "keep").write_text("keep")
        with self.assertRaisesRegex(sync.SyncError, "mirror_not_empty"):
            sync._mirror_ready(self.config)
        (self.config["mirror"] / "keep").unlink()
        self.config["identity"] = {"invalid": "value"}
        with self.assertRaisesRegex(sync.SyncError, "invalid_identity"):
            sync._mirror_ready(self.config)

    def test_pending_commit_mismatch_and_conflicting_remote(self):
        _, transport = self.local_transport()
        with transport, self.runner(), patch.object(sync, "_push", side_effect=sync.SyncError("offline")):
            with self.assertRaises(sync.SyncError):
                sync.synchronize(self.config, push=True)
            state = json.loads(self.config["state_file"].read_text())
            original = state["pending_tree"]
            state["pending_tree"] = "invalid"
            sync._save(self.config["state_file"], state)
            with self.assertRaisesRegex(sync.SyncError, "pending_commit_mismatch"):
                sync.synchronize(self.config, push=True)
            state["pending_tree"] = original
            state["pending_parent"] = "unexpected"
            sync._save(self.config["state_file"], state)
            with self.assertRaisesRegex(sync.SyncError, "remote_head_changed"):
                sync.synchronize(self.config, push=True)

    def test_lock_prevents_overlapping_push(self):
        state = self.config["state_file"]
        state.parent.mkdir()
        with state.with_suffix(".json.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(sync.SyncError, "sync_already_running"):
                sync.synchronize(self.config, push=True)

    def test_push_requires_matching_remote_receipt(self):
        with patch.object(sync, "_check_origin"), patch.object(sync, "_git", side_effect=["", "different refs/heads/main"]):
            with self.assertRaisesRegex(sync.SyncError, "remote_verification_failed"):
                sync._push(self.config, "expected")

    def test_push_url_and_push_rewrite_are_checked_before_sending(self):
        _, transport = self.local_transport()
        with transport, self.runner():
            sync.synchronize(self.config, push=True)
            mirror = self.config["mirror"]
            sync._git(mirror, "config", "remote.origin.pushurl", "https://github.com/other/repo.git")
            with patch.object(sync, "_push") as push:
                with self.assertRaisesRegex(sync.SyncError, "origin_mismatch"):
                    sync.synchronize(self.config, push=True)
                push.assert_not_called()
            with self.assertRaisesRegex(sync.SyncError, "origin_mismatch"):
                sync._push(self.config, "unused")
            sync._git(mirror, "config", "--unset", "remote.origin.pushurl")
            sync._git(mirror, "config", "url.https://github.com/other/.pushInsteadOf", "https://github.com/example/")
            with self.assertRaisesRegex(sync.SyncError, "origin_mismatch"):
                sync._mirror_ready(self.config)

    def test_git_location_and_config_environment_are_scrubbed(self):
        outside = self.root / "outside-repository"
        sync._run(["git", "init", "--quiet", str(outside)])
        before = {name: (outside / ".git" / name).read_bytes() for name in ("config", "HEAD")}
        dirty_env = {"GIT_DIR": str(outside / ".git"), "GIT_WORK_TREE": str(outside),
                     "GIT_INDEX_FILE": str(outside / ".git" / "foreign-index"),
                     "GIT_OBJECT_DIRECTORY": str(outside / ".git" / "objects"),
                     "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "user.name",
                     "GIT_CONFIG_VALUE_0": "Injected"}
        _, transport = self.local_transport()
        with transport, self.runner(), patch.dict(os.environ, dirty_env):
            self.assertEqual(sync.synchronize(self.config, push=True)["status"], "pushed")
        for name, data in before.items():
            self.assertEqual((outside / ".git" / name).read_bytes(), data)
        self.assertFalse((outside / ".git" / "foreign-index").exists())
        self.assertFalse((outside / "README.md").exists())

    def test_external_gitdir_pointer_symlink_and_worktree_refused(self):
        mirror = self.config["mirror"]
        mirror.mkdir()
        pointer = mirror / ".git"
        pointer.write_text("gitdir: ../outside\n")
        with self.assertRaisesRegex(sync.SyncError, "mirror_external_gitdir"):
            sync._mirror_ready(self.config)
        pointer.unlink()
        pointer.symlink_to(self.root / "outside")
        with self.assertRaisesRegex(sync.SyncError, "mirror_external_gitdir"):
            sync._mirror_ready(self.config)
        pointer.unlink()
        _, transport = self.local_transport()
        with transport:
            sync._mirror_ready(self.config)
            outside = self.root / "outside"
            outside.mkdir()
            sync._git(mirror, "config", "core.worktree", str(outside))
            with self.assertRaisesRegex(sync.SyncError, "mirror_external_gitdir"):
                sync._mirror_ready(self.config)

    def test_staging_refuses_unsafe_tracked_paths_and_symlinks(self):
        mirror = self.config["mirror"]
        mirror.mkdir()
        sync._run(["git", "init", "--quiet", str(mirror)])
        (mirror / "private").write_text("private")
        sync._git(mirror, "add", "private")
        with self.assertRaisesRegex(sync.SyncError, "unsafe_tracked_file"):
            sync._stage(self.config, self.source, sync.collect_public(self.source))
        sync._git(mirror, "rm", "--cached", "private")
        (mirror / "private").unlink()
        target = self.root / "outside"
        target.write_text("keep")
        (mirror / "README.md").symlink_to(target)
        sync._git(mirror, "add", "README.md")
        with self.assertRaisesRegex(sync.SyncError, "mirror_symlink"):
            sync._stage(self.config, self.source, sync.collect_public(self.source))
        self.assertEqual(target.read_text(), "keep")

    def test_staged_manifest_and_bytes_must_match_snapshot(self):
        payloads = {"README.md": b"expected"}
        with patch.object(sync, "_git", side_effect=["", "", "README.md\0extra.py"]):
            with self.assertRaisesRegex(sync.SyncError, "staged_members_mismatch"):
                sync._stage(self.config, self.source, payloads)
        with patch.object(sync, "_git", side_effect=["", "", "README.md"]), patch.object(
                sync, "_run", return_value=subprocess.CompletedProcess([], 0, stdout=b"changed")):
            with self.assertRaisesRegex(sync.SyncError, "staged_bytes_changed"):
                sync._stage(self.config, self.source, payloads)


class SnapshotRunnerTests(TemporaryProject):
    def test_real_offline_runner_and_environment(self):
        log = self.root / "test.log"
        optional = {"TASKFLOW_TEST_FORMAL_ADAPTER": "must-not-propagate",
                    "TASKFLOW_TEST_PUBLIC_PROBE_ADAPTER_ROOT": "must-not-propagate"}
        with patch.dict(os.environ, optional):
            result = sync._test_snapshot(self.source, log)
        self.assertEqual(result["tests"], 1)
        self.assertIn(b"OK", log.read_bytes())
        self.assertFalse(list(self.source.rglob("__pycache__")))
        with patch.dict(os.environ, optional), patch.object(sync.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, b"", b"Ran 1 test\nOK")) as run:
            sync._test_snapshot(self.source, log)
            self.assertNotIn("TASKFLOW_TEST_FORMAL_ADAPTER", run.call_args.kwargs["env"])
            self.assertNotIn("TASKFLOW_TEST_PUBLIC_PROBE_ADAPTER_ROOT", run.call_args.kwargs["env"])
            self.assertEqual(run.call_args.kwargs["env"]["PYTHONDONTWRITEBYTECODE"], "1")

    def test_failed_empty_and_timed_out_tests(self):
        log = self.root / "test.log"
        for status, stderr in ((1, b"Ran 1 test\nFAILED"), (0, b"Ran 0 tests\nOK"), (0, b"unknown")):
            with patch.object(sync.subprocess, "run", return_value=subprocess.CompletedProcess([], status, b"", stderr)):
                with self.assertRaisesRegex(sync.SyncError, "tests_failed_or_empty"):
                    sync._test_snapshot(self.source, log)
        with patch.object(sync.subprocess, "run", side_effect=subprocess.TimeoutExpired("python", 1800, b"partial", b"failure")):
            with self.assertRaisesRegex(sync.SyncError, "tests_timed_out"):
                sync._test_snapshot(self.source, log)
        self.assertEqual(log.read_bytes(), b"partialfailure")


if __name__ == "__main__":
    unittest.main()
