"""Explicit private zero-record fixtures; no business source or model calls."""

import copy
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, validate_task, write_json
from taskflow.packages import check_task, export_snapshot, freeze_task, verify_export, verify_snapshot
from tests.helpers import add_reviews, make_task


class EmptyContentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.task = make_task(self.root)
        self.path = "draft/evaluation/control-stream.jsonl"
        self.entry = {"id": "zero_records", "path": self.path, "role": "teacher", "references": ["S1"],
                      "empty_content": {"kind": "zero_record_jsonl", "reason": "Constructed control has no records.",
                                        "evidence": ["S1", "prompt"]}}

    def declare(self, entry=None, data=b"", task=None):
        task = task or self.task
        entry = copy.deepcopy(self.entry if entry is None else entry)
        source = task / entry["path"]
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(data)
        declaration = read_json(task / "task.json")
        declaration["files"].append(entry)
        write_json(task / "task.json", declaration)
        return source

    def test_private_zero_records_round_trip_with_evidence_and_private_digests(self):
        for role in ("teacher", "verification"):
            with self.subTest(role=role):
                task = make_task(self.root, role)
                before = check_task(task, False)
                entry = {**self.entry, "role": role}
                self.declare(entry, task=task)
                captured = check_task(task, False)
                self.assertEqual("passed", captured["status"])
                self.assertEqual(before["input_digest"], captured["input_digest"])
                self.assertNotEqual(before["evaluation_digest"], captured["evaluation_digest"])
                declaration = read_json(task / "task.json")
                declaration["files"][-1]["empty_content"]["reason"] += " Explicit context."
                write_json(task / "task.json", declaration)
                changed = check_task(task, False)
                self.assertEqual(captured["input_digest"], changed["input_digest"])
                self.assertNotEqual(captured["content_digest"], changed["content_digest"])
                self.assertNotEqual(captured["evaluation_digest"], changed["evaluation_digest"])
                add_reviews(task)
                snapshot = Path(freeze_task(task, "v1")["path"])
                self.assertEqual(0, next(row for row in verify_snapshot(snapshot)["files"] if row["id"] == "zero_records")["size"])
                evaluator = export_snapshot(snapshot, "evaluator")
                exported = next(row for row in evaluator["files"] if row["id"] == "zero_records")
                self.assertEqual(declaration["files"][-1]["empty_content"], exported["empty_content"])
                self.assertEqual(0, (Path(evaluator["path"]) / self.path).stat().st_size)
                self.assertEqual(evaluator["files"], verify_export(Path(evaluator["path"]))["files"])
                solver = export_snapshot(snapshot, "solver")
                self.assertFalse(any("empty_content" in row for row in solver["files"]))
                self.assertFalse((Path(solver["path"]) / self.path).exists())
                self.assertTrue(all(set(row) == {"id", "path", "role", "size", "sha256"} for row in solver["files"]))

    def test_only_exact_typed_private_jsonl_declarations_are_valid(self):
        self.declare()
        original = read_json(self.task / "task.json")
        mutations = {
            "null": lambda row: row.update(empty_content=None),
            "extra field": lambda row: row["empty_content"].update(allow=True),
            "wrong kind": lambda row: row["empty_content"].update(kind="allow_empty"),
            "blank reason": lambda row: row["empty_content"].update(reason=" \n"),
            "empty evidence": lambda row: row["empty_content"].update(evidence=[]),
            "duplicate evidence": lambda row: row["empty_content"].update(evidence=["S1", "S1"]),
            "unknown evidence": lambda row: row["empty_content"].update(evidence=["unknown"]),
            "untyped evidence": lambda row: row["empty_content"].update(evidence=[{"id": "S1"}]),
            "untyped role": lambda row: row.update(role=[]),
            "non-jsonl": lambda row: row.update(path="draft/evaluation/control.json"),
        }
        for role in ("prompt", "input", "template", "answer", "rubric", "reference"):
            mutations[role] = lambda row, role=role: row.update(role=role)
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                declaration = copy.deepcopy(original)
                mutate(declaration["files"][-1])
                self.assertTrue(validate_task(declaration))

    def test_exact_zero_bytes_and_safe_existing_file_are_required(self):
        path = self.declare()
        for data in (b"\n", b" ", b"{}\n", b"\xef\xbb\xbf"):
            with self.subTest(data=data):
                path.write_bytes(data)
                self.assertIn("exactly zero bytes", str(check_task(self.task, False)["errors"]))
        path.write_bytes(b"")
        declaration = read_json(self.task / "task.json")
        del declaration["files"][-1]["empty_content"]
        write_json(self.task / "task.json", declaration)
        self.assertIn("empty file", str(check_task(self.task, False)["errors"]))
        declaration["files"][-1] = self.entry
        write_json(self.task / "task.json", declaration)
        path.unlink()
        self.assertEqual("failed", check_task(self.task, False)["status"])
        path.mkdir()
        self.assertEqual("failed", check_task(self.task, False)["status"])
        path.rmdir()
        other = self.task / "draft/evaluation/other-empty.jsonl"
        other.write_bytes(b"")
        path.symlink_to(other)
        self.assertIn("symlink", str(check_task(self.task, False)["errors"]))

    def test_snapshot_and_export_cannot_drop_or_forge_the_empty_declaration(self):
        self.declare()
        add_reviews(self.task)
        snapshot = Path(freeze_task(self.task, "v1")["path"])
        evaluator = Path(export_snapshot(snapshot, "evaluator")["path"])
        solver = Path(export_snapshot(snapshot, "solver")["path"])
        original = read_json(evaluator / "package.json")
        for label in ("removed", "unknown evidence", "wrong role", "nonzero"):
            with self.subTest(label=label):
                manifest = copy.deepcopy(original)
                entry = next(row for row in manifest["files"] if row["id"] == "zero_records")
                if label == "removed":
                    del entry["empty_content"]
                elif label == "unknown evidence":
                    entry["empty_content"]["evidence"] = ["missing"]
                elif label == "wrong role":
                    entry["role"] = "answer"
                else:
                    (evaluator / self.path).write_bytes(b"\n")
                    entry.update(size=1, sha256=sha256(b"\n"))
                write_json(evaluator / "package.json", manifest)
                with self.assertRaisesRegex(TaskflowError, "export file contract"):
                    verify_export(evaluator)
                (evaluator / self.path).write_bytes(b"")
        manifest = read_json(solver / "package.json")
        manifest["files"][0]["empty_content"] = self.entry["empty_content"]
        write_json(solver / "package.json", manifest)
        with self.assertRaisesRegex(TaskflowError, "metadata"):
            verify_export(solver)
        declaration = read_json(snapshot / "task.json")
        del declaration["files"][-1]["empty_content"]
        write_json(snapshot / "task.json", declaration)
        manifest = read_json(snapshot / "manifest.json")
        del manifest["files"][-1]["empty_content"]
        raw = (snapshot / "task.json").read_bytes()
        manifest.update(task_size=len(raw), task_sha256=sha256(raw))
        manifest["snapshot_digest"] = sha256(canonical({key: value for key, value in manifest.items() if key != "snapshot_digest"}))
        write_json(snapshot / "manifest.json", manifest)
        with self.assertRaisesRegex(TaskflowError, "snapshot file contract.*empty file"):
            verify_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
