"""Explicit ChatGPT environment at a normal Python taskflow startup boundary.

This preserves the original consumer command and immutable worker. It does not
qualify ancestor/tool isolation or authorize business execution by itself.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

from taskflow.execution_codex_environment import process_environment


def start(argv: list[str], *, cwd: Path, receipt: Path, execute: bool = False) -> dict:
    if (len(argv) < 4 or Path(argv[0]).resolve() != Path(sys.executable).resolve()
            or argv[1:4] != ["-m", "taskflow", "execution"]):
        raise ValueError("only the existing Python taskflow execution entry is supported")
    if cwd.resolve() != Path(__file__).resolve().parents[1]:
        raise ValueError("consumer repository differs from the running management package")
    environment = process_environment(None, authentication="chatgpt")
    observation = {"kind": "explicit_chatgpt_consumer_environment", "argv": argv,
        "cwd": str(cwd.resolve()), "authentication": "existing ChatGPT home",
        "retained_environment_names": sorted(environment), "credential_values_recorded": False,
        "arbitrary_tool_or_ancestor_isolation_claimed": False,
        "business_launch_permission_granted": False}
    if not execute:
        return {**observation, "process_started": False}
    # This opt-in is only a launch mechanism. Existing taskflow holds and owner
    # conditions are still required; no task or work identity is rebuilt here.
    receipt.parent.mkdir(parents=True, exist_ok=True)
    with receipt.open("x", encoding="utf-8") as stream:
        json.dump({**observation, "started_at": datetime.now(timezone.utc).isoformat(),
                   "process_started": "unknown", "automatic_retry": False}, stream, indent=2)
    stdout = receipt.with_suffix(receipt.suffix+".stdout.log")
    stderr = receipt.with_suffix(receipt.suffix+".stderr.log")
    with stdout.open("xb") as out, stderr.open("xb") as err:
        process = subprocess.Popen(argv, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
                                   stdout=out, stderr=err, start_new_session=True)
    value = {**observation, "process_started": True, "pid": process.pid,
             "stdout": str(stdout), "stderr": str(stderr), "automatic_retry": False}
    result = receipt.with_suffix(receipt.suffix+".started.json")
    with result.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    print(json.dumps(start(command, cwd=args.cwd, receipt=args.receipt, execute=args.execute)))
