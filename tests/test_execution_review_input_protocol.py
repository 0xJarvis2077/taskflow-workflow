"""Actual neutral enqueue/readback for future review input protocols; no models."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import PUBLIC_ROLES, TaskflowError, read_json, sha256, write_json
from taskflow.execution_flow import _prepare_node, advance_workflow
from taskflow.execution_runner import _validate
from tests import test_execution_flow as fixture


class ReviewInputProtocolTests(unittest.TestCase):
    def setUp(self):
        case = fixture.ContinuationTests("test_running_predecessor_wait_is_stable_without_process_recovery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.root, self.store = case, case.root, case.store
        self.definition = case.definition("neutral", outcome="accepted")
        self.task = Path(self.definition["initial"]["candidate_root"])
        declaration = read_json(self.task / "task.json")
        for index in range(5):
            name = "draft/evaluation/SECRET_TEACHER_" + str(index) + ".txt"
            (self.task / name).write_text("Actual parent private bytes " + str(index) + "\n")
            declaration["files"].append({"id": "private_" + str(index), "path": name,
                                         "role": "teacher", "references": ["S1"]})
        write_json(self.task / "task.json", declaration)
        for name in ("quality", "recheck"):
            self.definition["roles"][name]["runner"]["request_sections"] = [
                {"kind": "operation", "text": "OLD_ROLE_SECTION_MARKER: later informed examination."}]
        case.register(self.definition)
        self.initialized = advance_workflow(self.store, self.definition["flow_id"])
        self.parent = self.store.get_candidate(self.initialized["state"]["candidate_id"])

    def prepare(self, record=None):
        result = _prepare_node(self.store, record or self.initialized)
        runtime = Path(result["state"]["current_runtime"])
        return result, runtime, _validate(runtime)

    def revision(self, mutate=None):
        current = self.root / "current-revision"
        shutil.copytree(self.task, current)
        if mutate is None:
            for row in self.parent["files"]:
                if row["role"] not in PUBLIC_ROLES:
                    path = current / row["package_path"]
                    path.write_bytes(path.read_bytes() + b"Current repaired support.\n")
        else:
            mutate(current)
        # This is an actual neutral subprocess emitting a synthetic complete
        # protocol trace, explicitly not an external model or business author.
        sid = "12345678-1234-4567-8123-123456789012"
        program = "import json\nprint(json.dumps({'type':'thread.started','thread_id':" + repr(sid) + "}))\nprint(json.dumps({'type':'turn.completed'}))\n"
        process = subprocess.run([sys.executable, "-c", program], capture_output=True, check=True)
        trace = self.root / "neutral-external-author.jsonl"
        trace.write_bytes(process.stdout)
        candidate = execution.capture_candidate(self.store, current, "current-revision", parent_id=self.parent["candidate_id"],
            external_author_sids=[sid], external_author_traces=[trace])
        record = self.store.get_flow(self.definition["flow_id"])
        state = {**record["state"], "candidate_id": candidate["candidate_id"], "candidate_root": str(current),
                 "stage": "recheck", "step": 1, "current_work_id": None}
        updated = self.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_fixture_revision"})
        return updated, candidate, current

    def test_whole_enqueue_locks_only_public_index_and_precedes_existing_role_sections(self):
        from taskflow.execution_flow_contracts import public_first_instructions

        result, runtime, manifest = self.prepare()
        context = read_json(runtime.parent / "current.json")
        index = read_json(runtime / "inputs/execution/public-inputs.json")
        self.assertEqual(self.parent["input_digest"], context["public_first"]["input_digest"])
        self.assertEqual("public-responsibility-first.json", context["public_first"]["checkpoint"])
        self.assertEqual({"schema_version", "input_digest", "files"}, set(index))
        public = [row for row in self.parent["files"] if row["role"] in PUBLIC_ROLES]
        self.assertEqual({row["package_path"] for row in public}, {row["path"] for row in index["files"]})
        for row in index["files"]:
            self.assertEqual({"path", "role", "sha256", "size"}, set(row))
            self.assertEqual(sha256((runtime / "inputs" / row["path"]).read_bytes()), row["sha256"])
        text = json.dumps(index)
        for secret in ("SECRET_TEACHER", "rubric", "scoring", "criteria", "evaluation_digest"):
            self.assertNotIn(secret, text)
        locked = next(row for row in manifest["inputs"] if row["target"] == "execution/public-inputs.json")
        self.assertEqual("source_metadata", locked["kind"])
        self.assertEqual(sha256((runtime / "inputs/execution/public-inputs.json").read_bytes()), locked["sha256"])
        request = (runtime / "request.md").read_text()
        self.assertLess(request.index(public_first_instructions(context)), request.index("OLD_ROLE_SECTION_MARKER"))
        self.assertNotIn(context["public_first"]["checkpoint"], manifest["outputs"])
        self.assertEqual("ready", self.store.get_work(result["state"]["current_work_id"])["state"])
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_affected_enqueue_locks_eight_actual_changes_and_historical_parent_bytes(self):
        record, candidate, current = self.revision()
        self.assertEqual((self.task / "task.json").read_bytes(), (current / "task.json").read_bytes())
        result, runtime, manifest = self.prepare(record)
        index = read_json(runtime / "inputs/execution/repair-diff.json")
        context = read_json(runtime.parent / "current.json")
        self.assertTrue(context["repair_diff"]["available"])
        self.assertTrue(index["available"])
        self.assertEqual(8, len(index["changed"]))
        self.assertNotIn("task.json", {row["path"] for row in index["changed"]})
        self.assertEqual(self.parent["candidate_id"], index["parent_binding"]["candidate_id"])
        self.assertEqual(candidate["content_digest"], index["current_binding"]["content_digest"])
        self.assertNotIn("public_first", context)
        inputs = {row["target"]: row for row in manifest["inputs"]}
        for row in index["changed"]:
            old = inputs[row["before_staged_path"]]
            new = inputs[row["current_staged_path"]]
            self.assertEqual("history", old["kind"])
            self.assertEqual("teacher", new["kind"])
            self.assertEqual(row["before_input_source"], old["source"])
            self.assertEqual(row["before_source"], next(item["path"] for item in self.parent["files"] if item["package_path"] == row["path"]))
            self.assertEqual(row["old_sha256"], old["sha256"])
            self.assertEqual(row["new_sha256"], new["sha256"])
            self.assertEqual((self.task / row["path"]).read_bytes(), (runtime / "inputs" / row["before_staged_path"]).read_bytes())
        self.assertIn("execution/repair-diff.json", (runtime / "request.md").read_text())
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_actual_role_addition_and_removal_are_part_of_the_declared_diff(self):
        def mutate(current):
            task = read_json(current / "task.json")
            first = next(row for row in task["files"] if row["id"] == "private_0")
            first["role"] = "verification"
            removed = next(row for row in task["files"] if row["id"] == "private_1")
            (current / removed["path"]).unlink()
            task["files"].remove(removed)
            added = {"id": "added_private", "path": "checks/new-association.txt", "role": "verification"}
            (current / "checks").mkdir(exist_ok=True)
            (current / added["path"]).write_text("Actual new member.\n")
            task["files"].append(added)
            write_json(current / "task.json", task)
        record, _, _ = self.revision(mutate)
        _, runtime, _ = self.prepare(record)
        rows = {row["path"]: row for row in read_json(runtime / "inputs/execution/repair-diff.json")["changed"]}
        self.assertEqual(3, len(rows))
        role = rows["draft/evaluation/SECRET_TEACHER_0.txt"]
        self.assertEqual(role["old_sha256"], role["new_sha256"])
        self.assertEqual(("teacher", "verification"), (role["old_role"], role["new_role"]))
        self.assertIsNone(rows["draft/evaluation/SECRET_TEACHER_1.txt"]["current_staged_path"])
        self.assertIsNone(rows["checks/new-association.txt"]["before_staged_path"])

    def test_missing_parent_or_source_is_explicitly_unavailable_without_old_hash_guesses(self):
        record = self.store.get_flow(self.definition["flow_id"])
        state = {**record["state"], "stage": "recheck", "step": 1}
        record = self.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_missing_parent"})
        _, runtime, manifest = self.prepare(record)
        value = read_json(runtime / "inputs/execution/repair-diff.json")
        self.assertFalse(value["available"])
        self.assertEqual("no_parent_candidate", value["reason"])
        self.assertEqual([], value["changed"])
        self.assertFalse(any(row["target"].startswith("execution/parent-inputs/") for row in manifest["inputs"]))
        case = ReviewInputProtocolTests()
        case.setUp()
        try:
            record, _, _ = case.revision()
            (case.task / "draft/evaluation/answer.md").unlink()
            _, runtime, _ = case.prepare(record)
            value = read_json(runtime / "inputs/execution/repair-diff.json")
            self.assertFalse(value["available"])
            self.assertEqual("parent_source_unavailable", value["reason"])
            self.assertEqual([], value["changed"])
        finally:
            case.doCleanups()

    def test_captured_public_or_parent_byte_drift_is_rejected_before_enqueue(self):
        public = self.task / "draft/input/prompt.md"
        public.write_bytes(public.read_bytes() + b"Drift")
        with self.assertRaisesRegex(TaskflowError, "bytes drifted"):
            self.prepare()
        self.assertEqual([], self.store.dashboard()["work"])
        case = ReviewInputProtocolTests()
        case.setUp()
        try:
            record, _, _ = case.revision()
            parent = case.task / "draft/evaluation/answer.md"
            parent.write_bytes(parent.read_bytes() + b"Unbound old bytes")
            with self.assertRaisesRegex(TaskflowError, "bytes drifted"):
                case.prepare(record)
            self.assertEqual([], case.store.dashboard()["work"])
        finally:
            case.doCleanups()

    def test_existing_prepared_spec_and_locked_request_are_reused_without_regeneration(self):
        with patch.object(self.store, "enqueue", side_effect=TaskflowError("neutral failure after actual preparation")):
            with self.assertRaisesRegex(TaskflowError, "after actual preparation"):
                self.prepare()
        root = Path(self.definition["directory"]) / "nodes/0-quality"
        paths = [root / "spec.json", root / "current.json", root / "public-inputs.json",
                 root / "runtime/manifest.json", root / "runtime/request.md",
                 root / "runtime/inputs/execution/public-inputs.json"]
        originals = {path: path.read_bytes() for path in paths}
        with patch("taskflow.execution_flow._review_input_protocol", side_effect=AssertionError("no protocol regeneration")):
            result, runtime, _ = self.prepare()
        self.assertEqual("ready", self.store.get_work(result["state"]["current_work_id"])["state"])
        for path, raw in originals.items():
            self.assertEqual(raw, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
