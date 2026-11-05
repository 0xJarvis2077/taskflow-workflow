"""Offline native-session transition contracts; all CLI events are fixtures."""

import copy
import json
from pathlib import Path
import unittest

from taskflow import execution_role_session as sessions
from taskflow import execution_runner as runner
from tests import test_execution_role_session as fixtures


BINDING, SID, reference = fixtures.BINDING, fixtures.SID, fixtures.reference


class RoleVersionTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = fixtures.RoleSessionTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def config(self, role: str, mode="fresh_persistent", previous=None):
        root, config = self.case.config(mode, previous)
        config["role"] = role
        if role == "repair":
            config["inputs"][0]["target"] = "parent/public/original.md"
        config["role_lineage"] = {"logical_role": "author" if role == "repair" else "quality",
                                  "owner": "neutral-owner", "task_id": BINDING["task_id"]}
        return root, config

    def completed(self, role: str):
        root, config = self.config(role)
        manifest = runner.prepare_attempt(root, config)
        self.assertTrue(runner.run_attempt(root)["technical_accepted"])
        return root, manifest, reference(root / "collection.json")

    def successor(self, role: str, source: Path, previous: dict, candidate="neutral-child"):
        root, config = self.config(role, "resume_existing", previous)
        original = runner._read(source / "manifest.json")
        config["candidate_binding"].update(candidate_id=candidate, content_digest=candidate + "-bytes")
        updated = self.case.base / (candidate + ".md")
        updated.write_text("Current affected business unit: " + candidate, encoding="utf-8")
        config["inputs"][0]["path"] = str(updated)
        diff = self.case.base / (candidate + "-diff.json")
        diff.write_text(json.dumps({"kind": "material_revision_diff_v1", "schema_version": 1,
            "parent_binding": original["candidate_binding"], "candidate_binding": config["candidate_binding"],
            "changes": [{"path": "public/original.md", "before": {"role": "input", **runner._info(source / "inputs" / original["inputs"][0]["target"])},
                         "after": {"role": "input", **runner._info(updated)}}]}), encoding="utf-8")
        config["role_transition"] = {
            "kind": "author_revision_v1" if role == "repair" else "quality_recheck_v1",
            "source_binding": original["candidate_binding"], "target_binding": config["candidate_binding"],
            "revision": reference(diff),
            "input_changes": sessions.input_changes(original["inputs"], config["inputs"])}
        return root, config

    def test_author_and_qa_parent_child_resume_preserve_sid_and_immutable_origin(self) -> None:
        for role in ("repair", "quality"):
            with self.subTest(role=role):
                if role == "quality":
                    home = self.case.base / "quality-home"
                    home.mkdir()
                    self.case.home = home
                original, manifest, previous = self.completed(role)
                snapshot = self.case.snapshot(original)
                marker = (self.case.home / sessions.BINDING_NAME).read_bytes()
                first, config = self.successor(role, original, previous, role + "-child")
                child_manifest = runner.prepare_attempt(first, config)
                self.assertNotEqual(manifest["role_session_binding"]["identity"], child_manifest["role_session_binding"]["identity"])
                self.assertEqual(SID, runner.run_attempt(first)["sid"])
                second, config2 = self.successor(role, first, reference(first / "collection.json"), role + "-grandchild")
                runner.prepare_attempt(second, config2)
                self.assertTrue(runner.run_attempt(second)["technical_accepted"])
                self.assertEqual(marker, (self.case.home / sessions.BINDING_NAME).read_bytes())
                self.assertEqual(snapshot, self.case.snapshot(original))
                self.assertNotEqual(manifest["intent_id"], child_manifest["intent_id"])
                self.assertEqual(SID, child_manifest["resume_sid"])
                if role == "quality":
                    correction, correction_config = self.config(role, "resume_existing", reference(second / "collection.json"))
                    correction_config["candidate_binding"] = copy.deepcopy(config2["candidate_binding"])
                    correction_config["inputs"] = copy.deepcopy(config2["inputs"])
                    runner.prepare_attempt(correction, correction_config)
                    self.assertEqual(SID, runner.run_attempt(correction)["sid"])

    def test_unrelated_task_wrong_author_or_missing_migration_is_rejected(self) -> None:
        original, _, previous = self.completed("repair")
        for field in ("owner", "task", "missing", "role", "inputs", "model", "policy", "diff"):
            root, config = self.successor("repair", original, previous, "child-" + field)
            if field == "owner":
                config["role_lineage"]["owner"] = "another-author"
            elif field == "task":
                config["candidate_binding"]["task_id"] = "unrelated-task"
                config["role_lineage"]["task_id"] = "unrelated-task"
            elif field == "missing":
                del config["role_transition"]
            elif field == "role":
                config["role"] = "quality"
                config["role_lineage"]["logical_role"] = "quality"
                config["role_transition"]["kind"] = "quality_recheck_v1"
            elif field == "inputs":
                config["role_transition"]["input_changes"] = []
            elif field == "diff":
                config["role_transition"]["revision"]["sha256"] = "wrong"
            elif field == "policy":
                config["policy_version"] = "another-policy"
            else:
                config["model"] = "another-model"
            with self.subTest(field=field), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)
            self.assertFalse((root / "dispatch.json").exists())

    def test_migration_rejects_illegal_typed_material_or_false_relation(self) -> None:
        original, _, previous = self.completed("quality")
        for field in ("material", "relation", "kind", "fresh"):
            root, config = self.successor("quality", original, previous, "child-" + field)
            if field == "material":
                config["inputs"][0]["kind"] = "target"
                config["role_transition"]["input_changes"] = sessions.input_changes(
                    runner._read(original / "manifest.json")["inputs"], config["inputs"])
            elif field == "relation":
                config["role_transition"]["source_binding"] = {**BINDING, "candidate_id": "unrelated"}
            elif field == "kind":
                config["role_transition"]["kind"] = "solver_continue_v1"
            else:
                config["role_session"]["mode"] = "fresh_persistent"
            with self.subTest(field=field), self.assertRaises(runner.RunnerError):
                runner.prepare_attempt(root, config)

    def test_version_migration_retains_unique_successor_and_unknown_launch_claim(self) -> None:
        original, _, previous = self.completed("repair")
        first, config = self.successor("repair", original, previous, "one-child")
        second, config2 = self.successor("repair", original, previous, "other-child")
        runner.prepare_attempt(first, config)
        runner.prepare_attempt(second, config2)
        self.assertEqual("uncertain", runner.run_attempt(first, fault="before_start")["status"])
        with self.assertRaises(runner.RunnerError):
            runner.run_attempt(first)
        with self.assertRaisesRegex(runner.RunnerError, "occupied invocation"):
            runner.run_attempt(second)
        self.assertFalse((second / "dispatch.json").exists())

    def test_payload_kinds_and_new_teacher_require_the_actual_revision(self) -> None:
        original, _, previous = self.completed("quality")
        for field in ("extra-teacher", "relabel-history", "relabel-teacher", "stale-payload"):
            root, config = self.successor("quality", original, previous, "kind-" + field)
            if field == "extra-teacher":
                config["inputs"].append({"path": str(self.case.public), "kind": "teacher", "target": "unrelated-private.md"})
            elif field == "stale-payload":
                config["inputs"][0]["path"] = str(self.case.public)
            else:
                config["inputs"][0]["kind"] = "history" if field == "relabel-history" else "teacher"
            config["role_transition"]["input_changes"] = sessions.input_changes(
                runner._read(original / "manifest.json")["inputs"], config["inputs"])
            with self.subTest(field=field), self.assertRaisesRegex(runner.RunnerError, "actual revision|actual role and bytes"):
                runner.prepare_attempt(root, config)

        root, config = self.successor("quality", original, previous, "legal-role-change")
        config["inputs"][0]["kind"] = "teacher"
        path = Path(config["role_transition"]["revision"]["path"])
        diff = runner._read(path)
        diff["changes"][0]["after"]["role"] = "answer"
        path.write_text(json.dumps(diff), encoding="utf-8")
        config["role_transition"]["revision"] = reference(path)
        config["role_transition"]["input_changes"] = sessions.input_changes(
            runner._read(original / "manifest.json")["inputs"], config["inputs"])
        runner.prepare_attempt(root, config)
        self.assertEqual(SID, runner.run_attempt(root)["sid"])

    def test_read_repository_migration_cannot_grant_write_or_move_home(self) -> None:
        original, _, previous = self.completed("quality")
        root, config = self.successor("quality", original, previous)
        manifest = runner.prepare_attempt(root, config)
        source = runner._role_session_source(previous)
        source["manifest"] = copy.deepcopy(source["manifest"])
        current = copy.deepcopy(manifest)
        old_root = self.case.base / "old-read"
        new_root = self.case.base / "new-read"
        old_root.mkdir()
        new_root.mkdir()
        for item, directory in ((source["manifest"], old_root), (current, new_root)):
            item["native"] = {"repository": str(directory), "filesystem": {str(directory): "read"},
                              "scratch": str(self.case.base / "scratch"), "profile": "neutral",
                              "python": item["argv"][0], "sandbox_argv": []}
            item["argv"] = [item["argv"][0], "exec"]
        current["role_transition"]["read_root_transition"] = {"before": str(old_root), "after": str(new_root)}
        sessions._version_transition(current, root, source)
        current["native"]["filesystem"][str(new_root)] = "write"
        with self.assertRaisesRegex(sessions.RoleSessionError, "read-only repository"):
            sessions._version_transition(current, root, source)


if __name__ == "__main__":
    unittest.main()
