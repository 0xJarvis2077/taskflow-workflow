"""Neutral collection layouts; no task content or execution is inspected."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from taskflow.task_directories import grouped_tasks_root, iter_task_packages


class TaskDirectoriesTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.root = self.base / "tasks"
        self.root.mkdir()

    def package(self, path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        (path / "task.json").write_text("not parsed by discovery", encoding="utf-8")
        return path

    def test_flat_and_grouped_packages_are_sorted_and_unique(self) -> None:
        flat = self.package(self.root / "z-flat")
        topic = self.package(self.root / "a-topic")
        grouped = self.package(topic / "packages" / "b-package")
        another = self.package(topic / "packages" / "a-package")

        packages = list(iter_task_packages(self.root))

        self.assertEqual(packages, sorted([flat, topic, grouped, another]))
        self.assertEqual(len(packages), len(set(packages)))
        self.assertTrue(grouped_tasks_root(self.root))

    def test_only_the_two_supported_depths_are_discovered(self) -> None:
        package = self.package(self.root / "topic" / "packages" / "package")
        self.package(package / "versions" / "v1")
        for directory in ("versions", "reports", "research", "runtime"):
            self.package(self.root / "topic" / directory / "mirror")
        self.package(self.root / "other" / "deeper" / "package")

        self.assertEqual(list(iter_task_packages(self.root)), [package])

    def test_empty_ordinary_packages_directory_identifies_a_grouped_root(self) -> None:
        (self.root / "topic" / "packages").mkdir(parents=True)

        self.assertTrue(grouped_tasks_root(self.root))
        self.assertEqual(list(iter_task_packages(self.root)), [])

    def test_missing_or_non_directory_collection_is_empty(self) -> None:
        regular_file = self.base / "file"
        regular_file.write_text("ordinary file", encoding="utf-8")
        for root in (self.base / "missing", regular_file):
            with self.subTest(root=root):
                self.assertEqual(list(iter_task_packages(root)), [])
                self.assertFalse(grouped_tasks_root(root))

    def test_directory_and_task_marker_symlinks_are_excluded(self) -> None:
        outside = self.package(self.base / "outside")
        (self.root / "linked-flat").symlink_to(outside, target_is_directory=True)
        (self.root / "topic").mkdir()
        (self.root / "topic" / "packages").symlink_to(
            outside, target_is_directory=True
        )
        packages = self.root / "ordinary-topic" / "packages"
        packages.mkdir(parents=True)
        (packages / "linked-package").symlink_to(outside, target_is_directory=True)
        marker = self.root / "linked-marker"
        marker.mkdir()
        (marker / "task.json").symlink_to(outside / "task.json")
        (self.root / "broken").symlink_to(self.base / "absent", target_is_directory=True)
        alias = self.base / "alias"
        alias.symlink_to(self.root, target_is_directory=True)

        self.assertEqual(list(iter_task_packages(self.root)), [])
        self.assertEqual(list(iter_task_packages(alias)), [])
        self.assertFalse(grouped_tasks_root(alias))
        self.assertTrue(grouped_tasks_root(self.root))

    def test_grouped_root_requires_ordinary_topic_and_packages_directories(self) -> None:
        outside = self.base / "outside"
        (outside / "packages").mkdir(parents=True)
        (self.root / "linked-topic").symlink_to(outside, target_is_directory=True)
        topic = self.root / "ordinary-topic"
        topic.mkdir()
        (topic / "packages").symlink_to(outside / "packages", target_is_directory=True)

        self.assertFalse(grouped_tasks_root(self.root))

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires FIFO support")
    def test_special_entries_are_never_treated_as_packages(self) -> None:
        os.mkfifo(self.root / "special")
        ordinary = self.root / "ordinary"
        ordinary.mkdir()
        os.mkfifo(ordinary / "task.json")
        (self.root / "directory-marker" / "task.json").mkdir(parents=True)
        topic = self.root / "topic"
        topic.mkdir()
        os.mkfifo(topic / "packages")

        self.assertEqual(list(iter_task_packages(self.root)), [])
        self.assertFalse(grouped_tasks_root(self.root))

    def test_filesystem_errors_are_not_reported_as_empty_collections(self) -> None:
        with mock.patch.object(Path, "lstat", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                list(iter_task_packages(self.root))
            with self.assertRaises(PermissionError):
                grouped_tasks_root(self.root)


if __name__ == "__main__":
    unittest.main()
