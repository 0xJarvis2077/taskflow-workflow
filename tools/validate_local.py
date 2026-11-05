"""Run offline unittest and standard-library executable-line coverage.

Run from the workspace: python3 -m tools.validate_local
No dependencies are installed. This reports line coverage, not branch coverage.
"""

import json
import hashlib
import re
import sys
import trace
import unittest
from pathlib import Path

from taskflow.task_directories import iter_task_packages


def run_suite() -> unittest.TestResult:
    suite = unittest.defaultTestLoader.discover("tests")
    return unittest.TextTestRunner(verbosity=2).run(suite)


def _source_files(root: Path) -> list[Path]:
    return sorted(set(
        list((root / "taskflow").glob("*.py"))
        + list((root / "tests").glob("*.py"))
        + list((root / "tools").glob("*.py"))
        + [path for package in iter_task_packages(root / "tasks")
           for path in (package / "checks").glob("*.py")]
    ))


def main() -> int:
    root = Path.cwd()
    output = root / ".local-validation" / "coverage"
    output.mkdir(parents=True, exist_ok=True)
    sources = _source_files(root)
    def hashes() -> dict[str, str]:
        return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    source_hashes = hashes()
    # trace caches ignored decisions by basename. Ignoring the stdlib's
    # __main__.py also hides our CLI. Trace all, then keep our source files.
    tracer = trace.Trace(count=True, trace=False)
    outcome = tracer.runfunc(run_suite)
    sources_unchanged = hashes() == source_hashes
    counts = {
        (filename, line): count
        for (filename, line), count in tracer.results().counts.items()
        if Path(filename).is_relative_to(root)
    }
    result = trace.CoverageResults(counts=counts)
    result.write_results(show_missing=True, summary=True, coverdir=str(output))
    modules = []
    for source in sorted((root / "taskflow").glob("*.py")):
        report = output / f"taskflow.{source.stem}.cover"
        hit = missing = 0
        if report.is_file():
            for line in report.read_text(encoding="utf-8").splitlines():
                if re.match(r"\s*\d+:", line):
                    hit += 1
                elif line.startswith(">>>>>>"):
                    missing += 1
        total = hit + missing
        modules.append({
            "module": f"taskflow.{source.stem}", "hit": hit, "executable": total,
            "line_coverage": round(hit / total * 100, 2) if total else 0,
        })
    summary = {
        "method": "Python stdlib trace executable-line coverage; no branch coverage",
        "tests_run": outcome.testsRun,
        "tests_passed": outcome.wasSuccessful(),
        "python": sys.version,
        "source_hashes": source_hashes,
        "sources_unchanged_during_tests": sources_unchanged,
        "modules": modules,
        "gate_percent_per_module": 80,
        "coverage_gate_passed": all(m["line_coverage"] >= 80 for m in modules),
    }
    (root / ".local-validation" / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if outcome.wasSuccessful() and sources_unchanged and summary["coverage_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
