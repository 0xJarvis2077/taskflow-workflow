"""Append-only local evidence records; no model execution or validity certification."""

import fcntl
import math
import os
import stat
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from taskflow.contracts import (
    TaskflowError,
    atomic_directory,
    canonical,
    parse_json,
    safe_path,
    safe_read,
    sha256,
    utc_now,
    validate_id,
    write_json,
)
from taskflow.packages import check_task, verify_snapshot


def _sealed(value: dict[str, Any], key: str) -> dict[str, Any]:
    return {**value, key: sha256(canonical(value))}


def _source_record(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = safe_read(path.parent, path.name)
    return raw, parse_json(raw, str(path))


def _capture_artifacts(
    source: Path, destination: Path, declared: Any
) -> tuple[list[dict[str, Any]], list[str]]:
    artifacts: list[dict[str, Any]] = []
    issues: list[str] = []
    if not isinstance(declared, list):
        return [], ["artifacts: missing or not a list"]
    seen: set[str] = set()
    for index, item in enumerate(declared):
        try:
            if not isinstance(item, dict):
                raise TaskflowError("entry must be an object")
            artifact_id = validate_id(item.get("id"), "artifact.id")
            if artifact_id in seen:
                raise TaskflowError(f"duplicate artifact id: {artifact_id}")
            seen.add(artifact_id)
            path = safe_path(source, item.get("path"))
            data = safe_read(source, item.get("path"))
            relative = f"artifacts/{index:04d}-{artifact_id}/{path.name}"
            target = safe_path(destination, relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            artifacts.append(
                {
                    "id": artifact_id,
                    "path": relative,
                    "kind": item.get("kind"),
                    "source_path": item.get("path"),
                    "size": len(data),
                    "sha256": sha256(data),
                }
            )
        except (TaskflowError, OSError, TypeError) as exc:
            issues.append(f"artifacts[{index}]: {exc}")
    return artifacts, issues


def _references(
    value: Any, artifacts: list[dict[str, Any]], label: str
) -> list[str]:
    if not isinstance(value, list) or not value:
        return [f"{label}: evidence references missing"]
    ids = {item["id"] for item in artifacts}
    issues = []
    for reference in value:
        if not isinstance(reference, str) or reference.split("#", 1)[0] not in ids:
            issues.append(f"{label}: unresolved artifact reference {reference!r}")
    return issues


def _run_gaps(
    record: dict[str, Any], reference: dict[str, Any], artifacts: list[dict[str, Any]]
) -> list[str]:
    gaps: list[str] = []
    if record.get("schema_version", 1) != 1:
        gaps.append("schema_version: unsupported record schema; retained for review")
    if record.get("actual_input_digest") is None:
        gaps.append("actual_input_digest: unknown")
    elif record["actual_input_digest"] != reference["input_digest"]:
        gaps.append("actual_input_digest: mismatch with selected snapshot")
    if record.get("status") != "completed":
        gaps.append(f"run status: {record.get('status', 'unknown')}")
    for key in ("requested_config", "observed_config"):
        config = record.get(key)
        if (
            not isinstance(config, dict)
            or not isinstance(config.get("model"), str)
            or not config["model"]
        ):
            gaps.append(f"{key}.model: unknown")
        if not isinstance(config, dict) or not isinstance(config.get("tools"), (list, dict)):
            gaps.append(f"{key}.tools: unknown")
    times = []
    for key in ("started_at", "ended_at"):
        try:
            value = record[key]
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone missing")
            times.append(parsed)
        except (KeyError, AttributeError, TypeError, ValueError):
            gaps.append(f"{key}: unknown or invalid timezone-aware time")
    if len(times) == 2 and times[1] < times[0]:
        gaps.append("ended_at: earlier than started_at")
    for key in ("interventions", "exceptions"):
        if not isinstance(record.get(key), list):
            gaps.append(f"{key}: unknown; use an explicit list when observed")
    gaps.extend(_references(record.get("input_evidence"), artifacts, "input_evidence"))
    gaps.extend(_references(record.get("isolation_evidence"), artifacts, "isolation_evidence"))
    if not any(item.get("kind") == "answer" for item in artifacts):
        gaps.append("answer artifact: missing")
    if record.get("nature") not in ("observed_run", "test_fixture"):
        gaps.append("nature: unknown")
    return gaps


def _classify_run(record: dict, reference: dict, gaps: list[str]) -> str:
    if record.get("nature") == "test_fixture":
        return "test_fixture"
    if record.get("actual_input_digest") not in (None, reference["input_digest"]):
        return "input_mismatch"
    if record.get("status") in ("failed", "timeout", "cancelled", "incomplete"):
        return "incomplete"
    return "unknown" if gaps else "evidence_complete"


def import_run(snapshot: Path, record_path: Path) -> dict[str, Any]:
    """Archive supplied observations, including mismatches and partial failures."""
    snapshot, record_path = Path(snapshot).absolute(), Path(record_path).absolute()
    reference = verify_snapshot(snapshot)
    raw, record = _source_record(record_path)
    run_id = validate_id(record.get("run_id"), "run_id")
    destination = snapshot.parent.parent / "runs" / run_id
    with atomic_directory(destination) as staging:
        (staging / "record.original.json").write_bytes(raw)
        write_json(staging / "snapshot.reference.json", reference)
        artifacts, issues = _capture_artifacts(
            record_path.parent, staging, record.get("artifacts")
        )
        gaps = _run_gaps(record, reference, artifacts)
        gaps.extend(issues)
        classification = _classify_run(record, reference, gaps)
        actual_input = record.get("actual_input_digest")
        input_binding = (
            "unknown" if actual_input is None
            else "matched" if actual_input == reference["input_digest"]
            else "mismatched"
        )
        files = [
            {
                "path": name,
                "size": (staging / name).stat().st_size,
                "sha256": sha256((staging / name).read_bytes()),
            }
            for name in ("record.original.json", "snapshot.reference.json")
        ]
        manifest = _sealed(
            {
                "schema_version": 1,
                "run_id": run_id,
                "task_id": reference["task_id"],
                "version_id": reference["version_id"],
                "snapshot_path": str(snapshot),
                "snapshot_digest": reference["snapshot_digest"],
                "input_digest": reference["input_digest"],
                "evaluation_digest": reference["evaluation_digest"],
                "actual_input_digest": record.get("actual_input_digest"),
                "input_binding": input_binding,
                "completion": record.get("status", "unknown"),
                "imported_at": utc_now(),
                "nature": record.get("nature", "unknown"),
                "classification": classification,
                "gaps": gaps,
                "model_validity": "not_determined",
                "isolation": "evidence_recorded_only; independent review required",
                "files": files,
                "artifacts": artifacts,
            },
            "run_digest",
        )
        write_json(staging / "run.json", manifest)
        if verify_snapshot(snapshot)["snapshot_digest"] != reference["snapshot_digest"]:
            raise TaskflowError("snapshot changed during run import")
    return {**manifest, "path": str(destination)}


def verify_run(run: Path) -> dict[str, Any]:
    """Verify the archived bytes before evaluating or reporting a run."""
    manifest = parse_json(safe_read(run, "run.json"), str(run / "run.json"))
    expected = manifest.get("run_digest")
    core = {key: value for key, value in manifest.items() if key != "run_digest"}
    if expected != sha256(canonical(core)):
        raise TaskflowError(f"run manifest digest mismatch: {run}")
    for item in manifest.get("files", []) + manifest.get("artifacts", []):
        data = safe_read(run, item["path"])
        if len(data) != item["size"] or sha256(data) != item["sha256"]:
            raise TaskflowError(f"run file changed: {item['path']}")
    return manifest


def verify_evaluation(evaluation: Path) -> dict[str, Any]:
    """Verify one saved assessment without replacing its original judgment."""
    result = parse_json(
        safe_read(evaluation, "evaluation.json"), str(evaluation / "evaluation.json")
    )
    core = {key: value for key, value in result.items() if key != "record_digest"}
    if result.get("record_digest") != sha256(canonical(core)):
        raise TaskflowError(f"evaluation manifest digest mismatch: {evaluation}")
    for item in result["files"]:
        if sha256(safe_read(evaluation, item["path"])) != item["sha256"]:
            raise TaskflowError(f"evaluation file changed: {item['path']}")
    return result


def _evaluation_issues(
    record: dict[str, Any], rubric: dict[str, Any], run_manifest: dict[str, Any]
) -> list[str]:
    issues: list[str] = []
    if record.get("schema_version", 1) != 1:
        issues.append("schema_version: unsupported evaluation schema; retained for review")
    criteria = rubric.get("scoring", {}).get("criteria", [])
    configured = {item["id"]: item for item in criteria}
    items = record.get("items")
    if not isinstance(items, list):
        return ["items: must list each configured criterion"]
    seen = set()
    total = 0
    artifact_ids = {item["id"] for item in run_manifest["artifacts"]}
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            issues.append(f"items[{index}]: must be an object")
            continue
        criterion_id = item.get("id")
        if (
            not isinstance(criterion_id, str)
            or criterion_id not in configured
            or criterion_id in seen
        ):
            issues.append(f"items[{index}]: unknown or duplicate criterion {criterion_id!r}")
            continue
        seen.add(criterion_id)
        score = item.get("score")
        maximum = configured[criterion_id]["max_score"]
        if (
            isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not 0 <= score <= maximum
        ):
            issues.append(f"items[{index}].score: outside criterion range")
        else:
            total += score
        refs = item.get("evidence")
        if not isinstance(refs, list) or not refs:
            issues.append(f"items[{index}].evidence: missing")
        elif any(
            not isinstance(ref, str) or ref.split("#", 1)[0] not in artifact_ids
            for ref in refs
        ):
            issues.append(f"items[{index}].evidence: unresolved run artifact")
    if seen != set(configured):
        issues.append("items: criterion coverage incomplete")
    declared_total = record.get("total")
    total_matches = (
        not isinstance(declared_total, bool)
        and isinstance(declared_total, (int, float))
    )
    try:
        total_matches = (
            total_matches
            and math.isfinite(declared_total)
            and math.isclose(declared_total, total, rel_tol=1e-10, abs_tol=1e-9)
        )
    except OverflowError:
        total_matches = False
    if not total_matches:
        issues.append("total: does not equal accepted item scores")
    if not isinstance(record.get("evaluator"), str) or not record["evaluator"]:
        issues.append("evaluator: unknown")
    if record.get("review_status") not in ("unreviewed", "reviewed"):
        issues.append("review_status: unknown")
    return issues


@contextmanager
def _evaluation_sequence(run: Path) -> Iterator[None]:
    """Serialize assessment decisions; keep the lock inode stable after release."""
    name = ".evaluation-sequence"
    safe_path(run, name)
    root_descriptor = os.open(run, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
        try:
            descriptor = os.open(
                name, flags | os.O_CREAT | os.O_EXCL, 0o600,
                dir_fd=root_descriptor,
            )
        except FileExistsError:
            descriptor = os.open(name, flags, dir_fd=root_descriptor)
    finally:
        os.close(root_descriptor)
    with os.fdopen(descriptor, "rb+") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise TaskflowError("evaluation sequence lock must be a regular file")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def import_evaluation(run: Path, record_path: Path) -> dict[str, Any]:
    """Append assessments, serializing parent and original-judgment decisions."""
    run, record_path = Path(run).absolute(), Path(record_path).absolute()
    verify_run(run)
    with _evaluation_sequence(run):
        return _import_evaluation_locked(run, record_path)


def _import_evaluation_locked(run: Path, record_path: Path) -> dict[str, Any]:
    run_manifest = verify_run(run)
    raw, record = _source_record(record_path)
    evaluation_id = validate_id(record.get("evaluation_id"), "evaluation_id")
    kind = record.get("kind")
    if kind not in ("original", "correction", "rejudgment"):
        raise TaskflowError("kind: expected original, correction or rejudgment")
    rubric_path = record.get("rubric_snapshot", run_manifest["snapshot_path"])
    if not isinstance(rubric_path, str):
        raise TaskflowError("rubric_snapshot: expected snapshot path")
    rubric_snapshot = Path(rubric_path)
    if not rubric_snapshot.is_absolute():
        rubric_snapshot = safe_path(record_path.parent, rubric_path)
    rubric = verify_snapshot(rubric_snapshot)
    issues = _evaluation_issues(record, rubric, run_manifest)
    if rubric["task_id"] != run_manifest["task_id"]:
        issues.append("rubric_snapshot: different task")
    parent_id = record.get("parent_evaluation_id")
    if kind != "original":
        try:
            parent_id = validate_id(parent_id, "parent_evaluation_id")
            parent = verify_evaluation(safe_path(run, f"evaluations/{parent_id}"))
            if parent["run_digest"] != run_manifest["run_digest"]:
                issues.append("parent_evaluation_id: assessment belongs to a different run")
            if kind == "correction" and parent["evaluation_digest"] != rubric["evaluation_digest"]:
                issues.append("correction: rubric changed; use rejudgment")
            if kind == "rejudgment" and parent["evaluation_digest"] == rubric["evaluation_digest"]:
                issues.append("rejudgment: rubric unchanged; use correction")
        except (TaskflowError, OSError, KeyError) as exc:
            issues.append(f"parent_evaluation_id: {exc}")
    else:
        if rubric["evaluation_digest"] != run_manifest["evaluation_digest"]:
            issues.append("original: rubric differs from original run standard")
        for path in (run / "evaluations").glob("*/evaluation.json"):
            previous = verify_evaluation(path.parent)
            if previous["kind"] == "original":
                issues.append("original: an original judgment is already archived")
                break
    if record.get("input_digest") != run_manifest.get("actual_input_digest"):
        issues.append("input_digest: differs from observed run input")
    exploratory = rubric["input_digest"] != run_manifest.get("actual_input_digest")
    if exploratory:
        issues.append("rubric depends on different public input; exploratory only")
    destination = run / "evaluations" / evaluation_id
    with atomic_directory(destination) as staging:
        (staging / "record.original.json").write_bytes(raw)
        write_json(staging / "rubric.reference.json", rubric)
        files = [
            {"path": name, "sha256": sha256((staging / name).read_bytes())}
            for name in ("record.original.json", "rubric.reference.json")
        ]
        result = _sealed(
            {
                "schema_version": 1,
                "evaluation_id": evaluation_id,
                "run_id": run_manifest["run_id"],
                "run_digest": run_manifest["run_digest"],
                "kind": kind,
                "parent_evaluation_id": parent_id,
                "evaluation_digest": rubric["evaluation_digest"],
                "rubric_input_digest": rubric["input_digest"],
                "actual_input_digest": run_manifest.get("actual_input_digest"),
                "imported_at": utc_now(),
                "nature": run_manifest["nature"],
                "run_classification": run_manifest["classification"],
                "model_validity": run_manifest["model_validity"],
                "validation_issues": issues,
                "classification": "exploratory" if exploratory else "recorded",
                "files": files,
            },
            "record_digest",
        )
        write_json(staging / "evaluation.json", result)
        if verify_run(run)["run_digest"] != run_manifest["run_digest"]:
            raise TaskflowError("run changed during evaluation import")
        if verify_snapshot(rubric_snapshot)["snapshot_digest"] != rubric["snapshot_digest"]:
            raise TaskflowError("rubric snapshot changed during evaluation import")
    return {**result, "path": str(destination)}


def status(task: Path) -> dict[str, Any]:
    """Summarize current evidence without inferring platform or model success."""
    task = Path(task).absolute()
    try:
        check = check_task(task, write_report=False)
    except (TaskflowError, OSError) as exc:
        check = {
            "status": "failed",
            "errors": [f"current draft could not be checked: {exc}"],
            "gaps": [],
            "reviews": [],
        }
    check_history = []
    for path in sorted((task / "reports").glob("check-*/report.json")):
        try:
            report = parse_json(safe_read(path.parent, path.name), str(path))
            if not isinstance(report.get("content_digest"), str):
                state = "invalid"
            elif not check.get("content_digest"):
                state = "unknown_current_input"
            else:
                state = (
                    "current" if report["content_digest"] == check["content_digest"]
                    else "stale"
                )
            check_history.append({"path": str(path), "state": state})
        except (TaskflowError, OSError, ValueError) as exc:
            check_history.append({"path": str(path), "state": "invalid", "error": str(exc)})
    interrupted_operations = []
    for directory in ("versions", "runs", "exports", "reports"):
        root = safe_path(task, directory)
        for path in sorted(root.rglob(".*")):
            if path.name.endswith(".lock") or ".staging-" in path.name:
                interrupted_operations.append(
                    {"path": str(path), "state": "pending_or_interrupted"}
                )
    versions = []
    for path in sorted((task / "versions").glob("*")):
        if not path.is_dir() or path.name.startswith("."):
            continue
        try:
            snapshot = verify_snapshot(path)
            versions.append(
                {
                    "version_id": snapshot["version_id"],
                    "integrity": "verified",
                    "snapshot_digest": snapshot["snapshot_digest"],
                    "input_digest": snapshot["input_digest"],
                    "evaluation_digest": snapshot["evaluation_digest"],
                }
            )
        except (TaskflowError, OSError, KeyError, ValueError) as exc:
            versions.append({"version_id": path.name, "integrity": "failed", "error": str(exc)})
    runs = []
    for path in sorted((task / "runs").glob("*")):
        if not path.is_dir() or path.name.startswith("."):
            continue
        try:
            run = verify_run(path)
            evaluations = []
            for evaluation_path in sorted((path / "evaluations").glob("*")):
                if not evaluation_path.is_dir() or evaluation_path.name.startswith("."):
                    continue
                try:
                    evaluation = verify_evaluation(evaluation_path)
                    if evaluation["run_digest"] != run["run_digest"]:
                        raise TaskflowError("assessment belongs to a different run")
                    rubric = parse_json(
                        safe_read(evaluation_path, "rubric.reference.json"),
                        str(evaluation_path / "rubric.reference.json"),
                    )
                    evaluations.append(
                        {
                            "evaluation_id": evaluation["evaluation_id"],
                            "kind": evaluation["kind"],
                            "parent_evaluation_id": evaluation.get("parent_evaluation_id"),
                            "classification": evaluation.get("classification"),
                            "rubric_version_id": rubric.get("version_id"),
                            "evaluation_digest": evaluation.get("evaluation_digest"),
                            "rubric_input_digest": evaluation.get("rubric_input_digest"),
                            "actual_input_digest": evaluation.get("actual_input_digest"),
                            "integrity": "verified",
                            "validation_issues": evaluation["validation_issues"],
                        }
                    )
                except (TaskflowError, OSError, KeyError, ValueError) as exc:
                    evaluations.append(
                        {
                            "evaluation_id": evaluation_path.name,
                            "integrity": "failed",
                            "error": str(exc),
                        }
                    )
            runs.append(
                {
                    "run_id": run["run_id"],
                    "version_id": run.get("version_id"),
                    "snapshot_digest": run.get("snapshot_digest"),
                    "input_digest": run.get("input_digest"),
                    "actual_input_digest": run.get("actual_input_digest"),
                    "input_binding": run.get("input_binding"),
                    "evaluation_digest": run.get("evaluation_digest"),
                    "completion": run.get("completion"),
                    "nature": run["nature"],
                    "classification": run["classification"],
                    "model_validity": run.get("model_validity"),
                    "integrity": "verified",
                    "gaps": run["gaps"],
                    "evaluations": evaluations,
                }
            )
        except (TaskflowError, OSError, KeyError, ValueError) as exc:
            runs.append({"run_id": path.name, "integrity": "failed", "error": str(exc)})
    return {
        "task": str(task),
        "check": check,
        "check_history": check_history,
        "interrupted_operations": interrupted_operations,
        "versions": versions,
        "runs": runs,
        "platform_status": "unknown; no platform receipt imported by this tool",
        "model_validity": "not_determined; package separation does not establish isolation",
    }
