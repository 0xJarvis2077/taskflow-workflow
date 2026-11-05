import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from taskflow.__main__ import main


class TopicInitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def invoke(self, *arguments):
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(list(arguments))
        return code, output.getvalue(), errors.getvalue()

    def test_topic_places_package_and_preserves_existing_versions(self):
        code, output, errors = self.invoke(
            "init", "draft-02", "--root", str(self.root), "--topic", "T1"
        )
        self.assertEqual(code, 0, errors)
        package = self.root / "T1/packages/draft-02"
        self.assertEqual(Path(json.loads(output)["path"]), package)
        self.assertTrue((package / "draft/input/prompt.md").is_file())
        self.assertIn("evaluations/", (package / "README.md").read_text())
        before = (package / "task.json").read_bytes()
        code, _, errors = self.invoke(
            "init", "draft-02", "--root", str(self.root), "--topic", "T1"
        )
        self.assertEqual(code, 2)
        self.assertIn("refusing overwrite", errors)
        self.assertEqual((package / "task.json").read_bytes(), before)

    def test_grouped_collection_requires_explicit_topic(self):
        (self.root / "T1/packages").mkdir(parents=True)
        code, _, errors = self.invoke("init", "scattered", "--root", str(self.root))
        self.assertEqual(code, 2)
        self.assertIn("--topic", errors)
        self.assertFalse((self.root / "scattered").exists())
        code, _, errors = self.invoke(
            "init", "also-scattered", "--root", str(self.root / "T1")
        )
        self.assertEqual(code, 2)
        self.assertIn("--topic", errors)
        self.assertFalse((self.root / "T1/also-scattered").exists())
        code, _, errors = self.invoke(
            "init", "scoped", "--root", str(self.root / "T1/packages")
        )
        self.assertEqual(code, 0, errors)
        self.assertTrue((self.root / "T1/packages/scoped/task.json").is_file())

    def test_flat_root_remains_compatible(self):
        for name in ("packages", "flat"):
            code, _, errors = self.invoke("init", name, "--root", str(self.root))
            self.assertEqual(code, 0, errors)
        self.assertTrue((self.root / "flat/task.json").is_file())

    def test_topic_cannot_escape_or_follow_symlink(self):
        code, _, _ = self.invoke(
            "init", "draft", "--root", str(self.root), "--topic", "../escape"
        )
        self.assertEqual(code, 2)
        outside = self.root / "outside"
        outside.mkdir()
        (self.root / "T1").symlink_to(outside, target_is_directory=True)
        code, _, errors = self.invoke(
            "init", "draft", "--root", str(self.root), "--topic", "T1"
        )
        self.assertEqual(code, 2)
        self.assertIn("symlink", errors)
        self.assertFalse((outside / "packages").exists())


if __name__ == "__main__":
    unittest.main()
