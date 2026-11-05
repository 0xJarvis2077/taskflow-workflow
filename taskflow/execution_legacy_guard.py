"""Deterministic refusal of retired real dispatch; no lifecycle is executed."""

import json
from pathlib import Path

from taskflow.contracts import TaskflowError


KINDS = {"evaluation_tail_v1", "experiment_v1", "author_quality_v1", "teacher_successor_v1", "original_score_pause_v1", "baseline_quality_v1", "bounded_original_rounds_v1"}


def assert_legacy_dispatch_allowed(db, work: dict) -> None:
    """Keep old actual role calls retired while retaining neutral replay tests."""
    from taskflow.execution_runner import _read
    if not work.get("runtime_path"):
        # The generic worker retains an unprepared claim as uncertain and cannot
        # launch it. Do not replace that durable no-replay boundary here.
        return
    manifest_path = Path(work["runtime_path"]) / "manifest.json"
    if not manifest_path.exists():
        return  # An absent manifest cannot launch; the worker records uncertainty.
    bound = work["role"] == "author"
    for row in db.execute("SELECT definition,state FROM execution_flows"):
        try:
            definition = json.loads(row["definition"])
        except (TypeError, ValueError):
            continue
        if not isinstance(definition, dict):
            continue
        try:
            state = json.loads(row["state"])
        except (TypeError, ValueError):
            state = {}
        if not isinstance(state, dict):
            state = {}
        kind = definition.get("kind")
        legacy = (isinstance(kind, str) and kind in KINDS) or (
            definition.get("kind") is None and "initial" in definition and "roles" in definition)
        if not legacy:
            continue
        identities = [state.get("current_work_id"), state.get("public_work_id"),
                      definition.get("author_work_id"), definition.get("first_work_id")]
        source = definition.get("source_author_binding")
        if isinstance(source, dict):
            identities.append(source.get("work_id"))
        if work["work_id"] in identities:
            bound = True
            break
    if bound and (not work.get("runtime_path")
                  or _read(manifest_path).get("mode") != "test_fixture"):
        raise TaskflowError("legacy author/lifecycle model dispatch is retired; use an explicit role task")

