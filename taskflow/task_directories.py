"""Discover task packages in flat and topic-grouped local collections."""

from pathlib import Path
import stat
from typing import Iterator


def _is_kind(path: Path, kind: int) -> bool:
    try:
        return stat.S_IFMT(path.lstat().st_mode) == kind
    except (FileNotFoundError, NotADirectoryError):
        return False


def _directories(root: Path) -> Iterator[Path]:
    if _is_kind(root, stat.S_IFDIR):
        for path in sorted(root.iterdir()):
            if _is_kind(path, stat.S_IFDIR):
                yield path


def iter_task_packages(root: Path) -> Iterator[Path]:
    """Yield sorted ordinary packages at the two supported collection depths.

    Only ``root/<package>/task.json`` and
    ``root/<topic>/packages/<package>/task.json`` identify a package. Directory
    symlinks and non-regular task markers are excluded without reading content.
    Missing collections yield nothing; other filesystem errors remain visible.
    """
    packages = set()
    for directory in _directories(root):
        if _is_kind(directory / "task.json", stat.S_IFREG):
            packages.add(directory)
        for package in _directories(directory / "packages"):
            if _is_kind(package / "task.json", stat.S_IFREG):
                packages.add(package)
    yield from sorted(packages)


def grouped_tasks_root(root: Path) -> bool:
    """Whether an ordinary topic has an ordinary, possibly empty packages dir."""
    return any(
        _is_kind(directory / "packages", stat.S_IFDIR)
        for directory in _directories(root)
    )
