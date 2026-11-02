"""Neutral subprocess fixture, never a real model or a target experiment."""

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--sid", required=True)
    parser.add_argument("--incomplete", action="store_true")
    args = parser.parse_args()
    request = sys.stdin.read()
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    for name, text in payload.items():
        target = Path(name)
        if target.is_absolute() or ".." in target.parts:
            raise ValueError("fixture output must be relative")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    print(json.dumps({"type": "thread.started", "thread_id": args.sid}), flush=True)
    if not args.incomplete:
        print(json.dumps({"type": "item.completed", "item": {
            "type": "agent_message", "text": "test_fixture delivery; received " + str(len(request)) + " request characters"
        }}), flush=True)
        print(json.dumps({"type": "turn.completed"}), flush=True)


if __name__ == "__main__":
    main()
