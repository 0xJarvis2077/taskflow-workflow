"""Generate teacher-side reports without executing or changing a model task."""

import argparse
import json
from pathlib import Path
import sys

from taskflow.contracts import TaskflowError
from taskflow.stress_report import build_report, write_report


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read original solver/review evidence and write a derived report")
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--solver", required=True)
    parser.add_argument("--review")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rules", type=Path)
    parser.add_argument("--compare", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        rules = json.loads(args.rules.read_text(encoding="utf-8")) if args.rules else None
        report = build_report(args.db, args.solver, assessment_id=args.review, rules=rules, compare=args.compare)
        output = write_report(args.output, report)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, TaskflowError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
