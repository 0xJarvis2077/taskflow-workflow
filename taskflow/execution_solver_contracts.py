"""Authenticity checks for one explicit public-only solver task.

These checks bind declared evidence. They do not assess an answer or choose
whether another business iteration should run.
"""

from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError
from taskflow.execution_quality_contracts import (
    candidate_binding, candidate_files, inspect_delivery, public_predecessor,
)
from taskflow.execution_runner import (
    _full_verify, _info, _read, _regular, _validate, _validated_collection, archive_attempt,
)
from taskflow.execution_store import validate_evidence


def completed_original(store, current: dict, candidate: dict, role: str) -> tuple[dict, dict, dict]:
    """Authenticate one completed work/attempt/CLI collection tuple."""
    attempt = store.get_attempt(current["attempt_id"])
    work = store.get_work(current["work_id"])
    root = Path(current["runtime"])
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    if (attempt["state"] != "completed" or attempt["role"] != role
            or attempt["candidate_id"] != candidate["candidate_id"]
            or attempt["task_id"] != candidate["task_id"]
            or manifest.get("role") != role
            or manifest.get("candidate_binding") != candidate_binding(candidate)
            or manifest["intent_id"] != attempt["intent_id"]
            or manifest["policy_version"] != attempt["policy_id"]
            or collection.get("technical_accepted") is not True
            or collection.get("status") != "completed"
            or not collection.get("sid") or collection["sid"] != attempt.get("cli_sid")
            or current["work_id"] != attempt["work_id"]
            or str(root) != attempt["runtime_path"]
            or any(work[key] != attempt[key] for key in (
                "task_id", "candidate_id", "intent_id", "runtime_path", "policy_id", "request_digest"))):
        raise TaskflowError("role source is not its exact completed original")
    return attempt, manifest, collection


def verified_full_archive(root: Path, archive: Path) -> dict:
    """Use the existing durable archive verifier, including all supplemental bytes."""
    if not archive.is_dir() or not (archive / "supplemental-intent.json").is_file():
        raise TaskflowError("role source requires its original complete archive")
    archive_attempt(root, archive)
    intent = _read(root / "supplemental-intent.json")
    _full_verify(archive / "supplemental", intent, True)
    return intent


def quality_source(store, spec: dict, *, _check_gate: bool = True, _allow_findings: bool = False) -> dict:
    """Validate a named v38 quality task and its actual accepted original."""
    candidate = store.get_candidate(spec["candidate_id"])
    files = candidate_files(candidate, Path(spec["candidate_root"]))
    record = store.get_flow(spec["quality_task_id"])
    definition, state = record["definition"], record["state"]
    if (definition.get("kind") != "quality_task_v1"
            or any(definition.get(key) != spec[key] for key in ("candidate_id", "task_id"))
            or definition.get("candidate_root") != spec["candidate_root"]
            or state.get("execution") != "completed" or state.get("delivery") != "valid"
            or state.get("outcome") not in (("accepted", "revision_required") if _allow_findings else ("accepted",))
            or (_check_gate and state.get("quality_gate", {}).get("accepted") is not True)):
        raise TaskflowError("solver requires an accepted explicit quality task for its exact candidate and policy")
    current = state["invocations"][state["current_index"]]
    attempt, manifest, collection = completed_original(store, current, candidate, "quality")
    if attempt["policy_id"] != definition["policy_id"] or manifest["policy_version"] != definition["policy_id"]:
        raise TaskflowError("quality original differs from its declared exact policy")
    if spec["runner"].get("mode") == "native_role" and manifest.get("mode") != "native_role":
        raise TaskflowError("native solver cannot promote simulated quality into a real business source")
    root = Path(current["runtime"])
    archive = Path(current["root"]) / "archive"
    verified_full_archive(root, archive)
    predecessor, _ = public_predecessor(store, definition, candidate)
    original_root = Path(state["invocations"][0]["runtime"])
    scope = definition.get("scope", "whole_package")
    coverage = None
    if scope == "affected":
        from taskflow.execution_quality_coverage import coverage_plan

        coverage = coverage_plan(store, definition, candidate)
    checked = inspect_delivery(candidate=candidate, attempt=attempt, root=root,
        archive=Path(definition["predecessor_archive"]), report_file=definition["report_file"],
        issues_file=definition["issues_file"], original_public_root=original_root, scope=scope, coverage=coverage, defect_ids=definition.get("defect_ids", []))
    if checked["errors"] or checked["outcome"] != state["outcome"]:
        raise TaskflowError("quality original is not a currently valid accepted delivery")
    review = store.get_review(current["review_id"])
    if (review["attempt_id"] != attempt["attempt_id"] or review["candidate_id"] != candidate["candidate_id"]
            or review["verdict"] != {"accepted": "accepted", "revision_required": "defects"}[state["outcome"]]
            or review.get("whole_package") is not (scope == "whole_package")
            or review.get("scope") != record["flow_id"]):
        raise TaskflowError("quality task review does not identify its accepted original")
    validate_evidence(review["evidence"])
    if _check_gate:
        store.quality_gate(candidate["candidate_id"])
    excluded = set(candidate.get("external_author_sids", [])) | {predecessor["cli_sid"], collection["sid"]}
    excluded.update(item["sid"] for item in state["invocations"] if item.get("sid"))
    if coverage is not None:
        excluded.update(coverage["parent_source"]["excluded_sids"])
    if candidate.get("author_attempt"):
        author_sid = store.get_attempt(candidate["author_attempt"])["cli_sid"]
        if author_sid:
            excluded.add(author_sid)
    return {"candidate": candidate, "files": files, "quality": record, "attempt": attempt,
            "manifest": manifest, "collection": collection, "review": review,
            "excluded_sids": sorted(excluded), "archive": archive, "coverage": coverage}


def solver_decision(store, spec: dict) -> dict:
    """A concrete solver request still needs the coordinator's explicit authorization."""
    decision = store.get_decision(spec["decision_id"])
    if (any(decision.get(key) != spec[key] for key in ("candidate_id", "task_id"))
            or decision.get("action") != "experiment" or decision.get("authorized") is not True
            or decision.get("experiment_purpose") != spec["purpose"]):
        raise TaskflowError("solver decision must authorize this exact candidate and purpose")
    validate_evidence(decision["evidence"])
    return decision


def check_public_inputs(manifest: dict, files: dict) -> None:
    """Require every actual public file and no private/control material."""
    expected = sorted((name, "public", row["sha256"], row["size"])
                      for name, row in files.items() if row["role"] in PUBLIC_ROLES)
    actual = sorted((row["target"], row["kind"], row["sha256"], row["size"])
                    for row in manifest["inputs"])
    if not expected or actual != expected:
        raise TaskflowError("solver inputs differ from the complete exact public candidate view")


def check_public_view(directory: Path, files: dict) -> None:
    """A readable public repository must not contain undeclared extra members."""
    expected = {name: row for name, row in files.items() if row["role"] in PUBLIC_ROLES}
    actual = {}
    for path in directory.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            continue
        name = str(path.relative_to(directory))
        _regular(path)
        actual[name] = _info(path)
    if (set(actual) != set(expected)
            or any(actual[name] != {key: expected[name][key] for key in ("sha256", "size")} for name in actual)):
        raise TaskflowError("solver public-only repository differs from the complete exact public view")


def check_solver_permissions(runner: dict, public_root: Path, files: dict,
                             quality_root: Path, candidate_root: Path) -> None:
    """Use the common positive material/runtime view for this formal role."""
    from taskflow.execution_tasks import check_native_view

    forbidden = [Path(row["source"]) for row in files.values() if row["role"] not in PUBLIC_ROLES]
    forbidden.extend((quality_root, candidate_root))
    check_native_view(runner, public_root, control_root=public_root.parent, forbidden=forbidden)
