"""Append-only interpretation of sealed author transport-completion evidence.

This module never dispatches, retries, rewrites a collection, or changes terminal
state. Consumers must validate the receipt and its originals on every use.
"""

from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_runner import (
    RunnerError, _author_host_executables, _info, _read, _recovered_author_collection, _validate,
    _validated_collection,
)


IDENTITY = ("attempt_id", "work_id", "intent_id", "task_id", "candidate_id",
            "policy_id", "runtime_path", "request_digest", "cli_sid")


def observation_material(attempt: dict) -> dict:
    """Validate the original failed author and derive an exact read-only receipt."""
    if (attempt.get("state") != "failed" or attempt.get("role") != "author"
            or type(attempt.get("process_exit")) is not int or attempt["process_exit"] != 0
            or attempt.get("response_complete") is not False
            or attempt.get("artifacts_complete") is not True
            or attempt.get("technical_accepted") is not False or not attempt.get("cli_sid")):
        raise TaskflowError("Completion observation requires the original trace-only failed author")
    root = Path(attempt["runtime_path"])
    try:
        manifest = _read(root / "manifest.json")
        collection = _validated_collection(root, manifest)
        interpreted = _recovered_author_collection(root, manifest, collection)
        if (manifest.get("intent_id") != attempt["intent_id"]
                or manifest.get("policy_version") != attempt["policy_id"]
                or manifest.get("request", {}).get("sha256") != attempt["request_digest"]
                or any(manifest.get("candidate_binding", {}).get(key) != attempt[key] for key in
                       ("candidate_id", "task_id", "input_digest", "content_digest"))
                or collection.get("sid") != attempt["cli_sid"]):
            raise TaskflowError("Completion observation differs from its original attempt binding")
        changes = {}
        for name, expected in manifest["dependencies"].items():
            actual = _info(Path(name))
            if actual != expected:
                if name not in _author_host_executables(manifest):
                    raise TaskflowError("Completion observation cannot accept changed input or dependency")
                changes[name] = {"before": expected, "after": actual}
        _validate(root, received_dependencies=changes)
        originals = {name: _info(root / name) for name in
                     ("manifest.json", "manifest-lock.json", "request.md", "termination.json",
                      "trace.jsonl", "collection.json", "collection-lock.json")}
    except (RunnerError, OSError, KeyError, TypeError) as exc:
        raise TaskflowError("Invalid original author completion evidence: " + str(exc)) from exc
    return {"schema_version": 1, "kind": "same_attempt_transport_completion_v1",
            "identity": {key: attempt[key] for key in IDENTITY},
            "original_state": "failed", "originals": originals,
            "historical_dependencies": changes,
            "reconnect_progress": interpreted["reconnect_progress"],
            "technical_completion_observed": True, "automatic_model_retry": False,
            "business_quality": "unresolved", "formal_score": None}


def validated_observation(attempt: dict) -> dict:
    receipt = attempt.get("completion_observation")
    if not isinstance(receipt, dict):
        raise TaskflowError("Author has no same-attempt completion observation")
    from taskflow.execution_store import validate_evidence

    validate_evidence(receipt.get("evidence"))
    material = observation_material(attempt)
    if receipt != {**material, "evidence": receipt["evidence"]}:
        raise TaskflowError("Author completion observation or its original evidence changed")
    return receipt


def author_technically_complete(attempt: dict) -> bool:
    if attempt.get("state") == "completed":
        return True
    if attempt.get("completion_observation") is None:
        return False
    validated_observation(attempt)
    return True


def received_dependencies(attempt: dict) -> dict | None:
    if attempt.get("completion_observation") is None:
        return None
    return validated_observation(attempt)["historical_dependencies"]


def author_collection(attempt: dict, root: Path) -> dict:
    """Return a derived view; the sealed collection remains byte-identical."""
    manifest = _read(root / "manifest.json")
    original = _validated_collection(root, manifest)
    if attempt.get("completion_observation") is None:
        return original
    receipt = validated_observation(attempt)
    for name, expected in receipt["originals"].items():
        if _info(root / name) != expected:
            raise TaskflowError("Author archive differs from observed original completion")
    return _recovered_author_collection(root, manifest, original)
