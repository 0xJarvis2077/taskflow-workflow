"""Source immutability inventory supports both local package layouts."""

from pathlib import Path
import tempfile
import unittest

from tools.validate_local import _source_files


class ValidateLocalSourcesTests(unittest.TestCase):
    def test_checks_from_flat_and_grouped_packages_join_the_source_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()

            def write(relative: str) -> Path:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# neutral source\n", encoding="utf-8")
                return path

            expected = [write(f"{directory}/source.py")
                        for directory in ("taskflow", "tools", "tests")]
            for package in ("tasks/flat", "tasks/topic/packages/grouped"):
                write(f"{package}/task.json")
                expected.append(write(f"{package}/checks/verify.py"))
            write("tasks/flat/versions/v1/task.json")
            write("tasks/flat/versions/v1/checks/verify.py")
            write("tasks/topic/research/mirror/task.json")
            write("tasks/topic/research/mirror/checks/verify.py")
            write("tasks/without-marker/checks/verify.py")
            (root / "tasks/linked").symlink_to(root / "tasks/flat", target_is_directory=True)

            self.assertEqual(_source_files(root), sorted(expected))


if __name__ == "__main__":
    unittest.main()
