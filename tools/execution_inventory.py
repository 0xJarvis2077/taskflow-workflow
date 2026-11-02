"""Append source-timed legacy observations without taking dispatch ownership."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from taskflow.contracts import read_json, sha256, write_json
from taskflow.execution_store import ExecutionStore, evidence_ref


def _role_evidence(role: object) -> dict:
    if not isinstance(role, dict):
        return {"verification": "no_current_role_declared"}
    result = {"declared": role, "process_state": "unknown", "model_response": "unknown",
              "delivery": "not_technically_accepted_by_this_layer"}
    runtime = role.get("runtime", role.get("raw_records"))
    if not isinstance(runtime, str):
        return result
    directory = Path(runtime)
    trace = directory / "trace.jsonl"
    if trace.is_file():
        ids, terminals = [], []
        for line in trace.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
                if event.get("type") == "thread.started":
                    ids.append(event.get("thread_id"))
                if event.get("type") in ("turn.completed", "turn.failed"):
                    terminals.append(event["type"])
            except (ValueError, AttributeError):
                continue
        result.update(trace=evidence_ref(trace, "actual visible CLI events"),
                      observed_sids=ids, terminal_events=terminals,
                      model_response="completed" if terminals == ["turn.completed"] else "not_proved_complete")
    for name in ("attempt.json", "execution.json", "execution-attempt.json"):
        receipt = directory / name
        if receipt.is_file():
            value = read_json(receipt)
            result["termination_receipt"] = evidence_ref(receipt, "legacy process observation")
            result["recorded_process_status"] = value.get("status")
            result["recorded_returncode"] = value.get("returncode")
            if value.get("ended_at") and isinstance(value.get("returncode"), int):
                result["process_state"] = "recorded_ended"
            break
    return result


def observe_controller(db: Path, source: Path, snapshots: Path, owner: str) -> dict:
    """Capture exact source bytes first; mutable source is never the DB truth."""
    raw = source.read_bytes()
    value = json.loads(raw)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory = snapshots / stamp
    directory.mkdir(parents=True, exist_ok=False)
    snapshot = directory / source.name
    snapshot.write_bytes(raw)
    store = ExecutionStore(db)
    observations = []
    for question in value["questions"]:
        observations.append(store.observe(
            question["task_id"], evidence_ref(snapshot, f"questions.{question['id']}"),
            question["status"], owner, datetime.now(timezone.utc).isoformat(),
            {"reported_at": value.get("updated_at"), "source_path": str(source.resolve()),
             "source_sha256": sha256(raw), "question_id": question["id"],
             "next_action_claim": question.get("next_action"),
             "current_role_evidence": _role_evidence(question.get("current_active_role")),
             "dispatch_owner_unchanged": True}
        ))
    result = {"observations": observations, "source_snapshot": str(snapshot),
              "scope": "read-only evidence intake; no dispatch ownership transfer"}
    write_json(directory / "intake.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--owner", required=True)
    args = parser.parse_args()
    result = observe_controller(args.db, args.source, args.snapshots, args.owner)
    print(json.dumps({"observations": len(result["observations"]), "source_snapshot": result["source_snapshot"]}))


if __name__ == "__main__":
    main()
