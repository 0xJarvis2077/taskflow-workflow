"""Shallow navigation preserves material bytes and rejects unsafe index outputs."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from tools.task_catalog import CatalogError, MARKER, build_catalog, main


class TaskCatalogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve() / "tasks"
        self.root.mkdir()

    def file(self, relative: str, content: bytes = b"original") -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def original_bytes(self) -> dict[Path, bytes]:
        return {path: path.read_bytes() for path in self.root.rglob("*")
                if path.is_file() and not path.is_symlink() and path.name != "INDEX.md"}

    def test_storage_instructions_are_linked_without_reading_or_modifying_them(self):
        self.file("Topic/packages/pack/task.json")
        storage = self.file("Topic/STORAGE.md", b"restore instructions")
        build_catalog(self.root, write=True)
        self.assertIn("[STORAGE.md](STORAGE.md)",
                      (self.root / "Topic/INDEX.md").read_text())
        self.assertEqual(storage.read_bytes(), b"restore instructions")

    def test_flat_grouped_research_and_prototypes_without_nested_mirrors(self):
        self.file("README.md")
        self.file("Topic/README.md")
        self.file("Topic/packages/pack/task.json", b"not parsed")
        self.file("Topic/packages/pack/draft/input/prompt.md")
        self.file("Topic/packages/pack/draft/evaluation/answer.md")
        self.file("Topic/packages/pack/versions/v1/task.json")
        self.file("Topic/packages/pack/exports/v1-solver/package.json")
        self.file("Topic/packages/pack/runs/run1/evaluation.json")
        self.file("Topic/packages/pack/incoming/run1/result.json")
        self.file("Topic/packages/pack/runtime-preparation/run1/request.md")
        self.file("Topic/packages/pack/reports/past/work/copy/task.json")
        self.file("Topic/research/study/README.md")
        self.file("Topic/prototypes/idea/task.json")
        self.file("ResearchOnly/research/study/notes.md")
        self.file("Flat/task.json")
        self.file("Flat/record.md")
        self.file("history/saved/task.json")
        self.file("noise/README.md")
        before = self.original_bytes()

        result = build_catalog(self.root, write=True)

        self.assertEqual(3, result["questions"])
        self.assertEqual(2, result["packages"])
        self.assertEqual(2, result["research"])
        self.assertEqual(1, result["prototypes"])
        self.assertEqual(4, result["indices_written"])
        self.assertEqual(before, self.original_bytes())
        self.assertFalse((self.root / "Topic/packages/pack/INDEX.md").exists())
        self.assertFalse((self.root / "history/INDEX.md").exists())
        catalog = (self.root / "INDEX.md").read_text(encoding="utf-8")
        self.assertTrue(catalog.startswith(MARKER + "\n"))
        self.assertNotIn("history", catalog)
        question = (self.root / "Topic/INDEX.md").read_text(encoding="utf-8")
        for value in ("当前草稿入口", "冻结版本", "导出", "运行与评分", "研究", "试作",
                      "用途/采用状态 unknown", "README.md（原文保留）"):
            self.assertIn(value, question)

    def test_default_cli_is_readonly_and_does_not_parse_material_contents(self):
        self.file("Topic/packages/pack/task.json", b"invalid json\xff")
        self.file("Topic/packages/pack/reports/run/trace.jsonl", b"\xff")
        self.file("Topic/packages/pack/reports/run/runtime/script.py", b"raise RuntimeError()")
        before = self.original_bytes()
        output = io.StringIO()

        with redirect_stdout(output):
            self.assertEqual(0, main(["--root", str(self.root)]))

        self.assertEqual(before, self.original_bytes())
        self.assertFalse((self.root / "INDEX.md").exists())
        result = json.loads(output.getvalue())
        self.assertFalse(result["write"])
        self.assertEqual(0, result["indices_written"])
        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, main(["--root", str(self.root), "--write"]))

    def test_report_classification_uses_actual_subdirectories_and_stays_shallow(self):
        prefix = "Topic/packages/pack"
        self.file(prefix + "/task.json")
        self.file(prefix + "/reports/accepted-latest-100.json", b"not a score receipt")
        self.file(prefix + "/reports/runtime-sounding/note.md")
        self.file(prefix + "/reports/review/runtime/hidden-file.json")
        self.file(prefix + "/reports/build/work/copied-task/task.json")
        self.file(prefix + "/reports/inspect/scratch/hidden-script.py")
        self.file(prefix + "/reports/scratch/hidden.txt")

        build_catalog(self.root, write=True)

        text = (self.root / "Topic/INDEX.md").read_text(encoding="utf-8")
        for name in ("review", "build", "inspect", "scratch"):
            self.assertIn(f"[{name}](packages/pack/reports/{name})：含执行原件", text)
        self.assertIn("runtime-sounding)：审查/记录（用途未核，unknown）", text)
        self.assertIn("accepted-latest-100.json)：审查/记录（用途未核，unknown）", text)
        self.assertNotIn("hidden-file", text)
        self.assertNotIn("copied-task", text)
        self.assertIn("物理存在不代表验收状态", text)
        self.assertIn("最新已接收结果及用途未由本工具判定：unknown", text)

    def test_user_index_anywhere_aborts_before_writing_other_indices(self):
        self.file("A/task.json")
        self.file("B/task.json")
        self.file("INDEX.md", (MARKER + "\nold generated content\n").encode())
        user_index = self.file("B/INDEX.md", b"user authored index\n")
        before = {path: path.read_bytes() for path in self.root.rglob("*") if path.is_file()}

        with self.assertRaisesRegex(CatalogError, "拒绝覆盖"):
            build_catalog(self.root, write=True)

        self.assertEqual(before, {path: path.read_bytes() for path in self.root.rglob("*") if path.is_file()})
        self.assertEqual(b"user authored index\n", user_index.read_bytes())
        self.assertFalse((self.root / "A/INDEX.md").exists())

    def test_symlink_sources_and_execution_evidence_are_excluded(self):
        outside = self.root.parent / "outside"
        outside.mkdir()
        (outside / "task.json").write_text("{}", encoding="utf-8")
        (self.root / "LinkedQuestion").symlink_to(outside, target_is_directory=True)
        self.file("Topic/packages/pack/task.json")
        (self.root / "Topic/packages/alias").symlink_to(outside, target_is_directory=True)
        reports = self.root / "Topic/packages/pack/reports"
        reports.mkdir()
        (reports / "alias").symlink_to(outside, target_is_directory=True)
        (reports / "review").mkdir()
        (reports / "review/work").symlink_to(outside, target_is_directory=True)
        (self.root / "Topic/research").symlink_to(outside, target_is_directory=True)
        self.file("MarkerLink/task.json", b"temporary marker").unlink()
        (self.root / "MarkerLink/task.json").symlink_to(outside / "task.json")

        result = build_catalog(self.root, write=True)

        self.assertEqual(1, result["questions"])
        self.assertEqual(1, result["packages"])
        self.assertEqual(0, result["research"])
        self.assertEqual(["task.json"], sorted(path.name for path in outside.iterdir()))
        text = (self.root / "Topic/INDEX.md").read_text(encoding="utf-8")
        self.assertNotIn("alias", text)
        self.assertIn("review)：审查/记录（用途未核，unknown）", text)

    def test_root_symlink_or_symlink_ancestor_is_rejected(self):
        alias = self.root.parent / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        (self.root / "collection").mkdir()
        for root in (alias, alias / "collection"):
            with self.subTest(root=root), self.assertRaisesRegex(CatalogError, "root 含符号链接"):
                build_catalog(root, write=True)
        self.assertFalse((self.root / "INDEX.md").exists())

    def test_symlink_index_and_directory_index_are_rejected(self):
        self.file("A/task.json")
        target = self.root.parent / "untouched.md"
        target.write_bytes(b"external")
        index = self.root / "A/INDEX.md"
        index.symlink_to(target)
        with self.assertRaisesRegex(CatalogError, "符号链接"):
            build_catalog(self.root, write=True)
        self.assertFalse((self.root / "INDEX.md").exists())
        self.assertEqual(b"external", target.read_bytes())
        index.unlink()
        index.mkdir()
        with self.assertRaisesRegex(CatalogError, "不是普通文件"):
            build_catalog(self.root, write=True)

    def test_repeated_write_is_byte_and_mtime_idempotent(self):
        self.file("A/task.json")
        first = build_catalog(self.root, write=True)
        self.assertEqual(2, first["indices_written"])
        indices = [self.root / "INDEX.md", self.root / "A/INDEX.md"]
        before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in indices]

        second = build_catalog(self.root, write=True)

        self.assertEqual(0, second["indices_written"])
        self.assertEqual(before, [(path.read_bytes(), path.stat().st_mtime_ns) for path in indices])
        self.file("A/reports/new.json")
        self.assertEqual(1, build_catalog(self.root, write=True)["indices_written"])
        self.assertFalse(any(path.name.startswith(".INDEX.md.") for path in self.root.rglob("*")))

    def test_links_encode_unicode_spaces_parentheses_and_escape_labels(self):
        self.file("题目 (甲)/packages/包 [乙]/task.json")
        self.file("题目 (甲)/packages/包 [乙]/reports/检查 (终).json")

        build_catalog(self.root, write=True)

        catalog = (self.root / "INDEX.md").read_text(encoding="utf-8")
        self.assertIn("%E9%A2%98%E7%9B%AE%20%28%E7%94%B2%29/INDEX.md", catalog)
        question = (self.root / "题目 (甲)/INDEX.md").read_text(encoding="utf-8")
        self.assertIn("%E5%8C%85%20%5B%E4%B9%99%5D/task.json", question)
        self.assertIn("%E6%A3%80%E6%9F%A5%20%28%E7%BB%88%29.json", question)
        self.assertIn("包 \\[乙\\]", question)

    def test_large_categories_are_bounded_and_empty_sections_remain_visible(self):
        self.file("A/packages/pack/task.json")
        for number in range(35):
            self.file(f"A/packages/pack/reports/report-{number:02}.json")
            self.file(f"A/packages/pack/versions/v{number:02}/task.json")
            self.file(f"A/research/study-{number:02}/notes.md")
        (self.root / "Empty/prototypes").mkdir(parents=True)

        build_catalog(self.root, write=True)

        text = (self.root / "A/INDEX.md").read_text(encoding="utf-8")
        self.assertIn("35 个直接条目", text)
        self.assertIn("另 5 项", text)
        self.assertNotIn("report-34", text)
        self.assertNotIn("v34", text)
        self.assertNotIn("study-34", text)
        empty = (self.root / "Empty/INDEX.md").read_text(encoding="utf-8")
        self.assertIn("题包：未见", empty)

    def test_missing_root_and_unowned_index_return_actionable_cli_errors(self):
        for root in (self.root / "missing", self.file("not-a-directory")):
            with self.subTest(root=root), redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(2, main(["--root", str(root), "--write"]))
                self.assertIn("root 不是普通目录", errors.getvalue())
        self.file("INDEX.md", b"user index")
        with redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(2, main(["--root", str(self.root), "--write"]))
        self.assertIn("拒绝覆盖", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
