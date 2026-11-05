"""Neutral subprocess checks for fresh solvers and staged same-SID reviewers."""

import copy
import json
from pathlib import Path
from unittest.mock import patch
import unittest

from taskflow import execution_runner as runner
from taskflow import execution_role_session as sessions
from tests import test_execution_role_session as fixtures
from tests import test_execution_codex_provider as provider_fixtures


class FormalRoleRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = fixtures.RoleSessionTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.cli.write_text(
            "#!" + fixtures.sys.executable + "\n"
            "import json,os,sys\nfrom pathlib import Path\n"
            "request=sys.stdin.read()\n"
            "name='facts.json' if request.startswith('Role: review-facts.') else "
            "'score.json' if request.startswith('Role: review-score.') else 'answer.json'\n"
            "Path(name).write_text(json.dumps({'stage':name,'home':os.environ.get('CODEX_HOME')}))\n"
            "print(json.dumps({'type':'thread.started','thread_id':'" + fixtures.SID + "'}),flush=True)\n"
            "print(json.dumps({'type':'item.completed','item':"
            "{'type':'agent_message','text':'Synthetic role result.'}}),flush=True)\n"
            "print(json.dumps({'type':'turn.completed'}),flush=True)\n", encoding="utf-8")
        self.teacher = self.case.base / "teacher.json"
        self.teacher.write_text('{"private":"TEACHER_PRIVATE"}', encoding="utf-8")
        self.source = self.case.base / "source-binding.json"
        self.source.write_text('{"run_id":"neutral-run","private":"MANAGER_PRIVATE"}', encoding="utf-8")
        self.binding = {"source": fixtures.reference(self.source), "teacher_inputs": [
            {**fixtures.reference(self.teacher), "kind": "teacher", "target": "teacher/rubric.json"}]}

    def config(self, role: str, previous: dict | None = None) -> tuple[Path, dict]:
        mode = "resume_existing" if previous else "fresh_persistent"
        root, config = self.case.config(mode, previous)
        config["role"] = role
        config["outputs"] = ["facts.json" if role == "review-facts" else "score.json"]
        config["run_binding"] = copy.deepcopy(self.binding)
        return root, config

    def facts(self, *, seal: bool = True) -> tuple[Path, dict, dict | None]:
        root, config = self.config("review-facts")
        runner.prepare_attempt(root, config)
        self.assertTrue(runner.run_attempt(root)["technical_accepted"])
        sealed = runner.seal_facts(root, root / "artifacts/facts.json") if seal else None
        return root, fixtures.reference(root / "collection.json"), sealed

    def score_config(self, root: Path, previous: dict, seal: dict) -> tuple[Path, dict]:
        score_root, config = self.config("review-score", previous)
        config["inputs"] += copy.deepcopy(self.binding["teacher_inputs"])
        config["inputs"].append({"path": seal["path"], "kind": "submission",
                                 "target": "execution/sealed-facts.json"})
        return score_root, config

    def test_facts_to_score_uses_original_seal_and_sid_with_new_invocation(self) -> None:
        facts, previous, seal = self.facts()
        before = self.case.snapshot(facts)
        root, config = self.score_config(facts, previous, seal)
        manifest = runner.prepare_attempt(root, config)
        self.assertEqual(str(facts), manifest["facts_attempt"])
        self.assertEqual(seal, manifest["facts_seal"])
        self.assertEqual(fixtures.SID, manifest["resume_sid"])
        self.assertEqual(self.binding, manifest["run_binding"])
        result = runner.run_attempt(root)
        self.assertTrue(result["technical_accepted"])
        self.assertEqual(fixtures.SID, result["sid"])
        self.assertEqual(before, self.case.snapshot(facts))
        self.assertTrue((root / "role-session-invocation.json").is_file())

    def test_run_binding_and_teacher_are_manager_only_until_score(self) -> None:
        facts, _, seal = self.facts()
        request = (facts / "request.md").read_text(encoding="utf-8")
        for private in ("TEACHER_PRIVATE", "MANAGER_PRIVATE", str(self.teacher), str(self.source)):
            self.assertNotIn(private, request)
        manifest = runner._read(facts / "manifest.json")
        self.assertIn(str(self.teacher), manifest["dependencies"])
        self.assertIn(str(self.source), manifest["dependencies"])
        self.assertEqual(self.binding, seal["run_binding"])

    def test_explicit_facts_seal_is_a_validated_appendix_to_an_immutable_prior_archive(self) -> None:
        facts, _, _ = self.facts(seal=False)
        archive = self.case.base / "facts-archive"
        original = runner.archive_full_attempt(facts, archive)
        before = self.case.snapshot(archive)
        supplemental = self.case.snapshot(Path(original["supplemental"]))
        seal = runner.seal_facts(facts, facts / "artifacts/facts.json")
        observed = runner.archive_full_attempt(facts, archive)
        self.assertEqual(fixtures.reference(facts / "facts-seal.json")["sha256"],
                         observed["facts_seal_appendix"]["sha256"])
        self.assertNotIn("facts-seal.json", observed["files"])
        self.assertEqual(before, self.case.snapshot(archive))
        self.assertEqual(supplemental, self.case.snapshot(Path(observed["supplemental"])))
        self.assertEqual(seal, runner._validated_facts_seal(facts))
        seal_bytes = (facts / "facts-seal.json").read_bytes()
        self.assertEqual(seal, runner.seal_facts(facts, facts / "artifacts/facts.json"))
        self.assertEqual(seal_bytes, (facts / "facts-seal.json").read_bytes())
        self.assertEqual(observed, runner.archive_full_attempt(facts, archive))
        link = facts / "untracked-directory-link"
        link.symlink_to(self.case.base, target_is_directory=True)
        with self.assertRaisesRegex(runner.RunnerError, "only addition"):
            runner.archive_full_attempt(facts, archive)
        link.unlink()
        (facts / "unexpected.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(runner.RunnerError, "only addition"):
            runner.archive_full_attempt(facts, archive)
        self.assertEqual(before, self.case.snapshot(archive))

    def test_score_requires_a_real_seal_and_fresh_persistent_facts(self) -> None:
        facts, previous, _ = self.facts(seal=False)
        root, config = self.config("review-score", previous)
        with self.assertRaisesRegex(runner.RunnerError, "sealed facts"):
            runner.prepare_attempt(root, config)
        root, config = self.config("review-facts")
        config["role_session"]["mode"] = "fresh_ephemeral"
        with self.assertRaisesRegex(runner.RunnerError, "persistent"):
            runner.prepare_attempt(root, config)

    def test_score_rejects_added_removed_teacher_history_or_replaced_sealed_facts(self) -> None:
        facts, previous, seal = self.facts()
        copied = self.case.base / "same-facts.json"
        copied.write_bytes(Path(seal["path"]).read_bytes())
        for change in ("missing_teacher", "missing_public", "extra_history", "facts_copy", "wrong_facts_root"):
            root, config = self.score_config(facts, previous, seal)
            if change == "missing_teacher":
                config["inputs"] = [item for item in config["inputs"] if item["kind"] != "teacher"]
            elif change == "missing_public":
                config["inputs"] = [item for item in config["inputs"] if item["kind"] != "public"]
            elif change == "extra_history":
                config["inputs"].append({"path": str(self.source), "kind": "trace", "target": "history.json"})
            elif change == "facts_copy":
                config["inputs"][-1]["path"] = str(copied)
            else:
                config["facts_attempt"] = str(self.case.base / "other")
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_score_rejects_changed_run_model_policy_home_or_candidate(self) -> None:
        facts, previous, seal = self.facts()
        other_source = self.case.base / "other-run.json"
        other_source.write_text('{"run_id":"different"}', encoding="utf-8")
        home = self.case.base / "other-home"
        home.mkdir()
        for change in ("run", "model", "policy", "home", "candidate"):
            root, config = self.score_config(facts, previous, seal)
            if change == "run":
                config["run_binding"]["source"] = fixtures.reference(other_source)
            elif change == "model":
                config["model"] = "different-model"
            elif change == "policy":
                config["policy_version"] = "different-policy"
            elif change == "home":
                config["role_session"]["home"] = str(home)
            else:
                config["candidate_binding"]["candidate_id"] = "different-candidate"
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_same_phase_facts_identity_still_binds_the_original_formal_run(self) -> None:
        _, previous, _ = self.facts(seal=False)
        other = self.case.base / "other-factual-run.json"
        other.write_text('{"run_id":"different"}', encoding="utf-8")
        root, config = self.config("review-facts", previous)
        config["run_binding"]["source"] = fixtures.reference(other)
        with self.assertRaisesRegex(runner.RunnerError, "capabilities"):
            runner.prepare_attempt(root, config)

    def test_facts_correction_then_score_correction_retain_one_original_facts_seal(self) -> None:
        first, previous, _ = self.facts(seal=False)
        corrected, config = self.config("review-facts", previous)
        runner.prepare_attempt(corrected, config)
        self.assertTrue(runner.run_attempt(corrected)["technical_accepted"])
        seal = runner.seal_facts(corrected, corrected / "artifacts/facts.json")
        score, config = self.score_config(corrected, fixtures.reference(corrected / "collection.json"), seal)
        runner.prepare_attempt(score, config)
        self.assertTrue(runner.run_attempt(score)["technical_accepted"])
        next_score, config = self.score_config(corrected, fixtures.reference(score / "collection.json"), seal)
        manifest = runner.prepare_attempt(next_score, config)
        self.assertEqual(str(corrected), manifest["facts_attempt"])
        self.assertEqual(seal, manifest["facts_seal"])
        self.assertTrue(runner.run_attempt(next_score)["technical_accepted"])
        self.assertFalse((first / "facts-seal.json").exists())

    def test_same_facts_collection_has_only_one_score_successor(self) -> None:
        facts, previous, seal = self.facts()
        first, first_config = self.score_config(facts, previous, seal)
        second, second_config = self.score_config(facts, previous, seal)
        runner.prepare_attempt(first, first_config)
        runner.prepare_attempt(second, second_config)
        self.assertTrue(runner.run_attempt(first)["technical_accepted"])
        with self.assertRaisesRegex(runner.RunnerError, "occupied invocation"):
            runner.run_attempt(second)

    def test_facts_rejects_teacher_metadata_or_teacher_inline_sections(self) -> None:
        for change in ("teacher", "binding", "inline"):
            root, config = self.config("review-facts")
            if change == "inline":
                config["request_sections"].append({"kind": "teacher", "text": "Private rubric"})
            else:
                config["inputs"].append({"path": str(self.teacher if change == "teacher" else self.source),
                                         "kind": "public", "target": "leaked.json"})
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_review_binding_rejects_missing_changed_or_unsafe_frozen_dependencies(self) -> None:
        for change in ("missing", "changed", "duplicate_target", "unsafe_target", "wrong_kind", "wrong_output"):
            root, config = self.config("review-facts")
            if change == "missing":
                config.pop("run_binding")
            elif change == "changed":
                config["run_binding"]["source"]["sha256"] = "different"
            elif change == "duplicate_target":
                config["run_binding"]["teacher_inputs"] *= 2
            elif change == "unsafe_target":
                config["run_binding"]["teacher_inputs"][0]["target"] = "../teacher.json"
            elif change == "wrong_kind":
                config["run_binding"]["teacher_inputs"][0]["kind"] = "public"
            else:
                config["outputs"] = ["facts.json", "extra.json"]
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_fixed_metadata_is_not_normalized_as_a_moving_work_path(self) -> None:
        first = {"candidate_binding": {**fixtures.BINDING, "candidate_id": "/first/work"},
                 "role": "review-facts", "mode": "test_fixture", "policy_version": "/first/work",
                 "model": {"requested": "neutral", "reasoning_effort": "high"}, "cwd": "/first/work",
                 "inputs": [], "outputs": ["facts.json"], "argv": ["/neutral/cli"]}
        moved = {**first, "cwd": "/second/work"}
        self.assertEqual(sessions.identity(first, Path("/first/runtime")),
                         sessions.identity(moved, Path("/second/runtime")))
        for field in ("policy_version", "candidate_binding"):
            changed = {**moved, field: "/second/work" if field == "policy_version"
                       else {**fixtures.BINDING, "candidate_id": "/second/work"}}
            with self.subTest(field=field):
                self.assertNotEqual(sessions.identity(first, Path("/first/runtime")),
                                    sessions.identity(changed, Path("/second/runtime")))

    def test_solver_is_public_fresh_and_cannot_reuse_a_completed_context(self) -> None:
        root, config = self.case.config("fresh_ephemeral")
        config["role"] = "solver"
        runner.prepare_attempt(root, config)
        self.assertTrue(runner.run_attempt(root)["technical_accepted"])
        previous = fixtures.reference(root / "collection.json")
        next_root, next_config = self.case.config("resume_existing", previous)
        next_config["role"] = "solver"
        with self.assertRaisesRegex(runner.RunnerError, "fresh independent"):
            runner.prepare_attempt(next_root, next_config)
        next_root, next_config = self.case.config("fresh_ephemeral")
        next_config["role"] = "solver"
        next_config["inputs"].append({"path": str(self.teacher), "kind": "teacher", "target": "answer-key.json"})
        with self.assertRaisesRegex(runner.RunnerError, "forbidden material"):
            runner.prepare_attempt(next_root, next_config)

    def test_native_solver_is_direct_cli_without_inner_isolated_carrier(self) -> None:
        case = provider_fixtures.CodexProviderTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        config = {**case.config, "role": "solver", "candidate_binding": copy.deepcopy(fixtures.BINDING),
                  "role_session": {"mode": "fresh_ephemeral", "home": str(case.home), "previous": None}}
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=case.capture):
            manifest = runner.prepare_attempt(case.case.root, config)
        self.assertEqual("native_role", manifest["mode"])
        self.assertFalse(manifest["isolated"])
        self.assertIn("--ephemeral", manifest["argv"])
        self.assertEqual(str(case.case.script.resolve()), manifest["argv"][0])


if __name__ == "__main__":
    unittest.main()
