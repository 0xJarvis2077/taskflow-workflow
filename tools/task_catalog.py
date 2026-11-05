"""Build shallow teacher navigation without interpreting task acceptance or scores."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import quote

from taskflow.task_directories import iter_task_packages


MARKER = "<!-- taskflow-task-catalog:v1 -->"
MAX_ENTRIES = 30
SECTIONS = ("packages", "research", "prototypes")
EXECUTION_DIRECTORIES = ("runtime", "work", "scratch")


class CatalogError(ValueError):
    """A catalog source or output cannot be used safely."""


@dataclass(frozen=True)
class Question:
    directory: Path
    packages: tuple[Path, ...]
    research: tuple[Path, ...]
    prototypes: tuple[Path, ...]


def _root_directory(root: Path) -> Path:
    absolute = root.absolute()
    if any(path.is_symlink() for path in (absolute, *absolute.parents)):
        raise CatalogError(f"root 含符号链接：{root}")
    if not absolute.is_dir():
        raise CatalogError(f"root 不是普通目录：{root}")
    return absolute.resolve()


def _safe(path: Path, boundary: Path) -> bool:
    if not path.is_relative_to(boundary):
        return False
    current = path
    while current != boundary:
        if current.is_symlink():
            return False
        current = current.parent
    return path.resolve().is_relative_to(boundary)


def _children(directory: Path, boundary: Path) -> tuple[Path, ...]:
    if not _safe(directory, boundary) or not directory.is_dir():
        return ()
    return tuple(
        path for path in sorted(directory.iterdir(), key=lambda path: path.name)
        if _safe(path, boundary) and (path.is_dir() or path.is_file())
    )


def _directories(directory: Path, boundary: Path) -> tuple[Path, ...]:
    return tuple(path for path in _children(directory, boundary) if path.is_dir())


def collect_catalog(root: Path) -> tuple[Path, tuple[Question, ...]]:
    """Inspect only question roots, package roots and named category children."""
    root = _root_directory(root)
    packages = tuple(iter_task_packages(root))
    questions = []
    for directory in _directories(root, root):
        selected = tuple(
            package for package in packages
            if (package == directory or package.parent == directory / "packages")
            and _safe(package, root)
        )
        sections = {
            name: _directories(directory / name, root)
            for name in ("research", "prototypes")
        }
        has_section = any(
            _safe(directory / name, root) and (directory / name).is_dir()
            for name in SECTIONS
        )
        if selected or has_section:
            questions.append(Question(directory, selected, **sections))
    return root, tuple(questions)


def _label(value: str) -> str:
    value = " ".join(value.splitlines())
    for character in ("\\", "[", "]", "|", "<", ">", "`", "*", "_"):
        value = value.replace(character, "\\" + character)
    return value


def _link(path: Path, base: Path, label: str | None = None) -> str:
    relative = path.relative_to(base).as_posix()
    return f"[{_label(label or path.name)}]({quote(relative, safe='/.-_~')})"


def _existing_link(path: Path, base: Path, boundary: Path, label: str) -> str:
    if _safe(path, boundary) and (path.is_file() or path.is_dir()):
        return _link(path, base, label)
    return "未见普通文件/目录"


def _listing(directory: Path, base: Path, boundary: Path) -> str:
    entries = _children(directory, boundary)
    if not entries:
        return "未见直接条目"
    links = "、".join(_link(entry, base) for entry in entries[:MAX_ENTRIES])
    if len(entries) > MAX_ENTRIES:
        links += f"；另 {len(entries) - MAX_ENTRIES} 项见目录"
    return f"{len(entries)} 项：{links}"


def _report_lines(package: Path, base: Path, boundary: Path) -> list[str]:
    reports = package / "reports"
    entries = _children(reports, boundary)
    if not entries:
        return ["报告：未见直接条目。", ""]
    lines = [f"报告：{_link(reports, base)}，{len(entries)} 个直接条目。", ""]
    for entry in entries[:MAX_ENTRIES]:
        contains_execution = entry.is_dir() and (
            entry.name in EXECUTION_DIRECTORIES or any(
                _safe(entry / name, boundary) and (entry / name).is_dir()
                for name in EXECUTION_DIRECTORIES
            )
        )
        purpose = "含执行原件" if contains_execution else "审查/记录（用途未核，unknown）"
        lines.append(f"- {_link(entry, base)}：{purpose}。")
    if len(entries) > MAX_ENTRIES:
        lines.append(f"- 另 {len(entries) - MAX_ENTRIES} 项见报告目录；未展开或判定用途。")
    return [*lines, ""]


def render_question(question: Question, boundary: Path) -> str:
    """Render physical navigation; never infer a current winner or accepted result."""
    base = question.directory
    lines = [MARKER, f"# {_label(base.name)} · 分类导航", "",
             "教师侧目录；物理存在不代表验收状态、当前采用版本或真实得分。",
             "当前采用包、最新已接收结果及用途未由本工具判定：unknown。",
             "仅列固定层级；单类最多展示前 30 个按名称排序的条目，不读取正文或执行脚本。", "",
             "历史说明读取入口：" + _existing_link(base / "README.md", base, boundary, "README.md（原文保留）") + "。", ""]
    storage = base / "STORAGE.md"
    if _safe(storage, boundary) and storage.is_file():
        lines.extend(["存储清理与恢复：" + _link(storage, base) + "。", ""])
    if not question.packages:
        lines.extend(["题包：未见带普通 task.json 的固定层级题包。", ""])
    for package in question.packages:
        lines.extend([f"## 包：{_label(package.name)}", "", "| 类别 | 入口 |", "| --- | --- |"])
        for label, path in (("题包契约", package / "task.json"),
                            ("历史说明", package / "README.md"),
                            ("原记录（未核）", package / "record.md"),
                            ("当前草稿入口 · 公开候选", package / "draft" / "input"),
                            ("当前草稿入口 · 私有参考", package / "draft" / "evaluation")):
            lines.append(f"| {label} | {_existing_link(path, base, boundary, path.name)} |")
        for label, name in (("冻结版本", "versions"), ("导出", "exports"),
                            ("运行与评分 · runs", "runs"), ("运行与评分 · incoming", "incoming"),
                            ("运行与评分 · runtime-preparation", "runtime-preparation")):
            path = package / name
            entry = _existing_link(path, base, boundary, name)
            lines.append(f"| {label} | {entry}；{_listing(path, base, boundary)} |")
        lines.extend(["", *_report_lines(package, base, boundary)])
    for title, entries in (("研究", question.research), ("试作", question.prototypes)):
        lines.extend([f"## {title}", ""])
        if not entries:
            lines.extend(["未见普通目录。", ""])
            continue
        for entry in entries[:MAX_ENTRIES]:
            readme = _existing_link(entry / "README.md", base, boundary, "历史说明 README.md")
            lines.append(f"- {_link(entry, base)}：用途/采用状态 unknown；{readme}。")
        if len(entries) > MAX_ENTRIES:
            lines.append(f"- 另 {len(entries) - MAX_ENTRIES} 项未展开。")
        lines.append("")
    return "\n".join(lines)


def render_catalog(root: Path, questions: tuple[Question, ...]) -> str:
    """Render one compact row per question without inspecting material contents."""
    lines = [MARKER, "# 任务分类导航", "",
             "教师侧目录；物理存在不代表验收状态、当前采用版本或真实得分。",
             "只枚举一层题根及 packages/research/prototypes；平铺题包也可列入。",
             "未读取任务、报告或 trace 正文；当前采用与最新得分保持 unknown。", "",
             "历史说明读取入口：" + _existing_link(root / "README.md", root, root, "README.md（原文保留）") + "。", "",
             "| 题目分类入口 | 题包 | 研究 | 试作 |", "| --- | ---: | ---: | ---: |"]
    for question in questions:
        lines.append(f"| {_link(question.directory / 'INDEX.md', root, question.directory.name)} | "
                     f"{len(question.packages)} | {len(question.research)} | {len(question.prototypes)} |")
    return "\n".join([*lines, ""])


def _validate_target(path: Path, boundary: Path) -> None:
    if not _safe(path, boundary):
        raise CatalogError(f"INDEX 输出含符号链接或越界：{path}")
    if path.exists():
        if not path.is_file():
            raise CatalogError(f"INDEX 输出不是普通文件：{path}")
        with path.open("r", encoding="utf-8") as stream:
            if stream.read(len(MARKER) + 1) != MARKER + "\n":
                raise CatalogError(f"拒绝覆盖非本工具生成的 INDEX：{path}")


def _write_index(path: Path, content: str, boundary: Path) -> bool:
    _validate_target(path, boundary)
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False
    descriptor, temporary = tempfile.mkstemp(prefix=".INDEX.md.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        _validate_target(path, boundary)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return True


def build_catalog(root: Path, *, write: bool = False) -> dict[str, int | bool]:
    """Return shallow counts; optionally replace only this tool's root indices."""
    root, questions = collect_catalog(root)
    outputs = [(root / "INDEX.md", render_catalog(root, questions))]
    outputs.extend((question.directory / "INDEX.md", render_question(question, root))
                   for question in questions)
    written = 0
    if write:
        for path, _ in outputs:
            _validate_target(path, root)
        for path, content in outputs:
            written += _write_index(path, content, root)
    return {"questions": len(questions), "packages": sum(len(q.packages) for q in questions),
            "research": sum(len(q.research) for q in questions),
            "prototypes": sum(len(q.prototypes) for q in questions),
            "write": write, "indices_written": written}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("tasks"))
    parser.add_argument("--write", action="store_true", help="仅生成总根及题根的 INDEX.md")
    args = parser.parse_args(argv)
    try:
        result = build_catalog(args.root, write=args.write)
    except (CatalogError, OSError, UnicodeError) as exc:
        print(f"task catalog: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
