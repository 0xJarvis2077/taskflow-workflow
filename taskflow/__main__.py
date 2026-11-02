"""Archive commands are local; opt-in execution workers may launch role CLIs."""

import argparse
import json
import sys
from pathlib import Path

from taskflow import __version__
from taskflow.contracts import TaskflowError
from taskflow.packages import check_task, export_snapshot, freeze_task, init_task


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Local task packages and evidence records")
    result.add_argument("--version", action="version", version=__version__)
    commands = result.add_subparsers(dest="command", required=True)
    execution = commands.add_parser("execution", add_help=False,
                                    help="persistent opt-in role execution (execution --help)")
    execution.add_argument("arguments", nargs=argparse.REMAINDER)
    init = commands.add_parser("init", help="create a draft; never overwrite")
    init.add_argument("task_id")
    init.add_argument("--root", type=Path, default=Path("tasks"))
    check = commands.add_parser("check", help="check structure, bytes and report binding")
    check.add_argument("task", type=Path)
    check.add_argument("--no-write", action="store_true")
    freeze = commands.add_parser("freeze", help="freeze reviewed captured bytes once")
    freeze.add_argument("task", type=Path)
    freeze.add_argument("--version", required=True)
    export = commands.add_parser("export", help="export an explicit solver/evaluator directory")
    export.add_argument("snapshot", type=Path)
    export.add_argument("--audience", choices=("solver", "evaluator"), required=True)
    export.add_argument("--output", type=Path)
    run = commands.add_parser("run", help="archive observed run records, including failures")
    run_commands = run.add_subparsers(dest="operation", required=True)
    run_import = run_commands.add_parser("import")
    run_import.add_argument("snapshot", type=Path)
    run_import.add_argument("--record", type=Path, required=True)
    evaluation = commands.add_parser("evaluation", help="append original/correction/rejudgment evaluations")
    evaluation_commands = evaluation.add_subparsers(dest="operation", required=True)
    evaluation_import = evaluation_commands.add_parser("import")
    evaluation_import.add_argument("run", type=Path)
    evaluation_import.add_argument("--record", type=Path, required=True)
    status = commands.add_parser("status", help="show evidence, gaps and stale checks")
    status.add_argument("task", type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    supplied = sys.argv[1:] if argv is None else argv
    if supplied and supplied[0] == "execution":
        from taskflow.execution import main as execution_main

        return execution_main(supplied[1:])
    args = parser().parse_args(argv)
    try:
        if args.command == "init":
            output = init_task(args.root / args.task_id, args.task_id)
        elif args.command == "check":
            output = check_task(args.task, write_report=not args.no_write)
        elif args.command == "freeze":
            output = freeze_task(args.task, args.version)
        elif args.command == "export":
            output = export_snapshot(args.snapshot, args.audience, args.output)
        else:
            from taskflow.records import import_evaluation, import_run, status

            if args.command == "run":
                output = import_run(args.snapshot, args.record)
            elif args.command == "evaluation":
                output = import_evaluation(args.run, args.record)
            else:
                output = status(args.task)
        print(json.dumps(output, ensure_ascii=False, indent=2, allow_nan=False))
        return 1 if output.get("status") == "failed" and args.command == "check" else 0
    except (OSError, TaskflowError, KeyError, TypeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
