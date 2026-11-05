"""Explicit reuse of a completed public reconstruction for identical public bytes."""

from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError
from taskflow.execution_runner import _info, _read, _validate, _validated_collection
from taskflow.execution_store import validate_evidence


KIND = "identical_public_predecessor_reuse_v1"
BINDING = ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")


def _binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in BINDING}


def _public_members(candidate: dict) -> list[tuple]:
    members = []
    for row in candidate["files"]:
        if row["role"] not in PUBLIC_ROLES:
            continue
        actual = _info(Path(row["path"]))
        if (actual["sha256"] != row["sha256"]
                or ("size" in row and row["size"] != actual["size"])):
            raise TaskflowError("Public predecessor reuse requires identical captured public bytes and actual member sizes")
        members.append((row["package_path"], row["role"], actual["sha256"], actual["size"]))
    return sorted(members)


def validate_public_reuse(store: object, definition: dict, attempt: dict, target: dict) -> dict:
    """Return the actual source candidate without rebinding the original attempt."""
    initial = definition["initial"]
    quality = definition["roles"]["quality"]
    runner = quality["runner"]
    if (quality.get("scope") != "whole_package" or runner.get("resume_sid")
            or (runner.get("quality_session") or {}).get("previous")):
        raise TaskflowError("Public predecessor reuse requires a fresh whole-package quality context")
    return validate_public_reuse_reference(store, initial["public_predecessor_reuse"],
        owner=definition["owner"], task_id=definition["task_id"], attempt=attempt, target=target)


def validate_public_reuse_reference(store: object, reference: dict, *, owner: str, task_id: str,
                                    attempt: dict, target: dict) -> dict:
    """Authenticate one applicability record while preserving the old original identity."""
    ref = validate_evidence([reference])[0]
    packet = _read(Path(ref["path"]))
    source = store.get_candidate(attempt["candidate_id"])
    identity = {key: attempt[key] for key in ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id", "cli_sid")}
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or packet.get("kind") != KIND or packet.get("controller") != owner
            or packet.get("task_id") != task_id
            or packet.get("predecessor") != identity
            or packet.get("source_candidate_binding") != _binding(source)
            or packet.get("target_candidate_binding") != _binding(target)
            or packet.get("quality_transfer") is not False
            or not isinstance(packet.get("reason"), str) or not packet["reason"].strip()
            or source["task_id"] != target["task_id"] or source["task_id"] != task_id
            or source["candidate_id"] == target["candidate_id"]
            or not _public_members(source) or _public_members(source) != _public_members(target)
            or attempt.get("role") != "quality" or attempt.get("state") != "completed"
            or not attempt.get("cli_sid")
            or attempt.get("technical_accepted") is not True
            or attempt.get("response_complete") is not True or attempt.get("artifacts_complete") is not True):
        raise TaskflowError("Public predecessor reuse requires exact old/new bindings and identical captured public bytes")
    validate_evidence(packet.get("evidence"))
    root = Path(attempt["runtime_path"])
    manifest = _validate(root)
    if manifest.get("candidate_binding") != _binding(source):
        raise TaskflowError("Public predecessor reuse source differs from original manifest")
    # Native/isolated real calls must retain their original public role slot.
    # Neutral fixture runs remain explicitly marked test_fixture.
    if manifest.get("mode") != "test_fixture":
        from taskflow.execution_batch_binding import validate_batch_source

        if (attempt.get("model_slot") != "public_rebuild"
                or not validate_batch_source(attempt, manifest, store)):
            raise TaskflowError("Public predecessor reuse requires the actual confirmed public reconstruction role")
    collection = _validated_collection(root, manifest)
    if (collection.get("status") != "completed" or collection.get("technical_accepted") is not True
            or collection.get("model_completed") is not True or collection.get("delivery_complete") is not True
            or collection.get("process_ended") is not True or collection.get("sid") != attempt["cli_sid"]
            or collection.get("intent_id") != attempt["intent_id"]):
        raise TaskflowError("Public predecessor reuse lacks original sealed successful collection")
    return source
