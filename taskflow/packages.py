"""Capture, check, freeze and export explicit task files without executing them."""

import io
import os
import stat
import uuid
import zipfile
from pathlib import Path

from taskflow import __version__
from taskflow.contracts import (
    PRIVATE_ROLES, PUBLIC_ROLES, ROLES, TaskflowError, atomic_directory, canonical,
    parse_json, relative_path, safe_path, safe_read, sha256, utc_now, validate_id,
    validate_empty_content, validate_task, write_json,
)

MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 2000
MAX_ARCHIVE_DEPTH = 3


def init_task(destination: Path, task_id: str) -> dict:
    """Create an honest draft requiring business configuration before freeze."""
    validate_id(task_id, "task_id")
    task = {
        "schema_version": 1, "task_id": task_id, "title": "",
        "project": {"name": None, "phase": None, "rule_sources": []},
        "sources": [],
        "files": [
            {"id": "prompt", "path": "draft/input/prompt.md", "role": "prompt", "references": []},
            {"id": "answer", "path": "draft/evaluation/answer.md", "role": "answer", "references": ["prompt"]},
            {"id": "rubric", "path": "draft/evaluation/rubric.md", "role": "rubric", "references": ["prompt"]},
            {"id": "record", "path": "record.md", "role": "teacher", "references": []},
        ],
        "scoring": {"total": None, "rubric_file": "rubric", "criteria": []},
        "reviews": [
            {"id": "business", "path": "reports/business.json", "kind": "business", "required": True},
            {"id": "artifact", "path": "reports/artifact.json", "kind": "artifact", "required": True},
        ],
    }
    with atomic_directory(destination) as stage:
        for name in ("checks", "reports", "versions", "runs", "exports"):
            (stage / name).mkdir()
        for entry in task["files"]:
            target = stage / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                "# 待填写\n\n尚未完成设计、核算或业务审查。\n", encoding="utf-8"
            )
        write_json(stage / "task.json", task)
    return {"path": str(destination), "task_id": task_id, "state": "draft"}


def _archive_paths(data: bytes, depth: int = 0, prefix: str = "", budget: dict | None = None) -> set[str]:
    if depth > MAX_ARCHIVE_DEPTH:
        raise TaskflowError("archive nesting exceeds limit")
    if budget is None:
        budget = {"bytes": 0, "members": 0}
    result: set[str] = set()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            local = set()
            file_names = {entry.filename for entry in archive.infolist() if not entry.is_dir()}
            for name in file_names:
                if any(parent.as_posix() in file_names for parent in Path(name).parents if parent.as_posix() != "."):
                    raise TaskflowError(f"archive file/directory collision: {name}")
            for entry in archive.infolist():
                name = entry.filename.rstrip("/")
                relative_path(name)
                if "!/" in name:
                    raise TaskflowError(f"archive member uses reserved nested separator: {name}")
                if name in local:
                    raise TaskflowError(f"archive duplicate member: {name}")
                local.add(name)
                if entry.flag_bits & 1:
                    raise TaskflowError(f"encrypted archive member: {name}")
                mode = entry.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise TaskflowError(f"archive symlink: {name}")
                budget["members"] += 1
                budget["bytes"] += entry.file_size
                if budget["members"] > MAX_ARCHIVE_MEMBERS or budget["bytes"] > MAX_ARCHIVE_BYTES:
                    raise TaskflowError("archive expanded size/member limit exceeded")
                if entry.is_dir():
                    continue
                key = prefix + name
                result.add(key)
                content = archive.read(entry)  # CRC verified; no files extracted.
                if name.lower().endswith(".zip"):
                    result.update(_archive_paths(content, depth + 1, key + "!/", budget))
                elif name.lower().endswith((".rar", ".7z", ".tar", ".gz")):
                    raise TaskflowError(f"unsupported nested archive: {key}; unpack and declare files")
    except (zipfile.BadZipFile, RuntimeError, NotImplementedError) as exc:
        raise TaskflowError(f"unreadable ZIP archive: {exc}") from exc
    return result


def _empty_content_errors(entry: dict, data: bytes, known: set[str] | None = None) -> list[str]:
    errors = validate_empty_content(entry, known)
    if "empty_content" in entry:
        if data:
            errors.append(f"{entry['path']}: zero_record_jsonl requires exactly zero bytes")
    elif not data:
        errors.append(f"{entry['path']}: empty file")
    return errors


def _inspect_file(entry: dict, data: bytes) -> tuple[list[str], list[str]]:
    errors = _empty_content_errors(entry, data)
    gaps: list[str] = []
    suffix = Path(entry["path"]).suffix.lower()
    if not data:
        return errors, gaps
    if suffix == ".zip":
        try:
            actual = _archive_paths(data)
            members = entry.get("archive_members")
            if not isinstance(members, list) or not all(isinstance(m, dict) for m in members):
                raise TaskflowError("ZIP requires explicit archive_members [{path,role}]")
            declared = {m.get("path"): m.get("role") for m in members if isinstance(m.get("path"), str)}
            if len(declared) != len(members) or set(declared) != actual:
                raise TaskflowError("archive_members must exactly match all files (nested paths use !/)")
            for name, role in declared.items():
                if not isinstance(role, str) or role not in ROLES:
                    raise TaskflowError(f"invalid archive member role: {name}")
                if entry["role"] in PUBLIC_ROLES and role not in PUBLIC_ROLES:
                    raise TaskflowError(f"public archive contains private member: {name}")
        except TaskflowError as exc:
            errors.append(f"{entry['path']}: {exc}")
    elif suffix in {".rar", ".7z", ".tar", ".gz"}:
        errors.append(f"{entry['path']}: unsupported archive; unpack and declare files for verification")
    elif suffix in {".docx", ".xlsx", ".pptx"}:
        try:
            _archive_paths(data)
        except TaskflowError as exc:
            errors.append(f"{entry['path']}: {exc}")
        gaps.append(f"{entry['path']}: OOXML container inspected; content/formulas/layout require artifact review")
    elif suffix in {".md", ".txt", ".csv", ".tsv", ".json", ".html", ".py"}:
        try:
            data.decode("utf-8-sig")
        except UnicodeError:
            errors.append(f"{entry['path']}: expected UTF-8 text")
    else:
        gaps.append(f"{entry['path']}: bytes captured; format/content not automatically verified")
    return errors, gaps


def _metadata(entry: dict, data: bytes) -> dict:
    return {**entry, "size": len(data), "sha256": sha256(data)}


def _capture(root: Path) -> tuple[dict, bytes, dict[str, bytes], list[str], list[str]]:
    raw = safe_read(root, "task.json")
    task = parse_json(raw, "task.json")
    errors = validate_task(task)
    if errors:
        return task, raw, {}, errors, []
    captured = {}
    gaps = []
    identities = {}
    for entry in task["files"]:
        path = entry["path"]
        try:
            data = safe_read(root, path)
            captured[path] = data
            info = safe_path(root, path).stat()
            identity = (info.st_dev, info.st_ino)
            prior = identities.get(identity)
            if prior and ((prior in PUBLIC_ROLES) != (entry["role"] in PUBLIC_ROLES)):
                errors.append(f"{path}: same filesystem file declared public and private")
            identities[identity] = entry["role"]
            file_errors, file_gaps = _inspect_file(entry, data)
            errors.extend(file_errors)
            gaps.extend(file_gaps)
        except (OSError, TaskflowError) as exc:
            errors.append(f"{path}: {exc}")
    if safe_read(root, "task.json") != raw:
        errors.append("task.json changed during capture")
    for path, data in captured.items():
        try:
            if safe_read(root, path) != data:
                errors.append(f"{path}: source changed during capture")
        except (OSError, TaskflowError) as exc:
            errors.append(f"{path}: changed during capture: {exc}")
    return task, raw, captured, errors, gaps


def _digests(task: dict, raw: bytes, captured: dict[str, bytes]) -> dict:
    files = [_metadata(entry, captured[entry["path"]]) for entry in task["files"] if entry["path"] in captured]
    public = [f for f in files if f["role"] in PUBLIC_ROLES]
    private = [f for f in files if f["role"] in PRIVATE_ROLES]
    return {
        "content_digest": sha256(canonical({"task_sha256": sha256(raw), "files": files})),
        "input_digest": sha256(canonical([
            {key: entry[key] for key in ("id", "path", "role", "size", "sha256")}
            for entry in public
        ])),
        "evaluation_digest": sha256(canonical({"files": private, "scoring": task.get("scoring")})),
    }


def _review_status(root: Path, task: dict, digests: dict) -> tuple[list[dict], dict[str, bytes]]:
    reviews = []
    captured = {}
    for spec in task.get("reviews", []):
        result = {**spec, "state": "missing"}
        try:
            raw = safe_read(root, spec["path"])
            captured[spec["path"]] = raw
            report = parse_json(raw, spec["path"])
            result.update(
                reported_status=report.get("status"),
                scope=report.get("scope"),
                limitations=report.get("limitations"),
                evidence_count=(
                    len(report["evidence"])
                    if isinstance(report.get("evidence"), list) else None
                ),
            )
            if report.get("content_digest") != digests["content_digest"] or report.get("input_digest") != digests["input_digest"]:
                result["state"] = "stale"
            elif report.get("review_id") != spec["id"] or report.get("kind") != spec["kind"]:
                result["state"] = "identity_mismatch"
            elif report.get("schema_version") != 1 or not report.get("scope") or not isinstance(report.get("limitations"), list) or not report.get("evidence"):
                result["state"] = "invalid_report"
            elif report.get("status") in ("passed", "failed", "inconclusive"):
                result["state"] = report["status"]
            else:
                result["state"] = "invalid_report"
        except (OSError, TaskflowError) as exc:
            result["detail"] = str(exc)
        reviews.append(result)
    return reviews, captured


def _check_capture(root: Path) -> tuple[dict, dict, bytes, dict, dict]:
    task, raw, captured, errors, gaps = _capture(root)
    digests = _digests(task, raw, captured) if not errors else {}
    reviews, reports = _review_status(root, task, digests) if digests else ([], {})
    result = {
        "schema_version": 1, "tool_version": __version__, "created_at": utc_now(),
        "task_id": task.get("task_id"), "status": "failed" if errors else "passed",
        **digests, "errors": errors, "gaps": gaps, "reviews": reviews,
        "scope": "manifest, references, scoring structure, bytes, archive members and report binding",
        "limitations": ["program checks do not establish business correctness or platform approval", "file packages do not establish model access isolation"],
    }
    return result, task, raw, captured, reports


def check_task(root: Path, write_report: bool = True) -> dict:
    result, _, _, _, _ = _check_capture(root)
    if write_report:
        destination = root / "reports" / f"check-{uuid.uuid4().hex}"
        with atomic_directory(destination) as stage:
            write_json(stage / "report.json", result)
        result["report_path"] = str(destination / "report.json")
    return result


def freeze_task(root: Path, version_id: str) -> dict:
    """Freeze the captured bytes only when all required reviews match them."""
    validate_id(version_id, "version_id")
    destination = safe_path(root, f"versions/{version_id}")
    with atomic_directory(destination) as stage:
        result, task, raw, captured, reports = _check_capture(root)
        failures = list(result["errors"])
        failures.extend(f"review {r['id']}: {r['state']}" for r in result["reviews"] if r["required"] and r["state"] != "passed")
        if failures:
            raise TaskflowError("cannot freeze: " + "; ".join(failures))
        for path, data in {**captured, **reports, "task.json": raw}.items():
            target = stage / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        write_json(stage / "structural-check.json", result)
        check_bytes = (stage / "structural-check.json").read_bytes()
        manifest = {
            "schema_version": 1, "tool_version": __version__, "task_id": task["task_id"],
            "version_id": version_id, "created_at": utc_now(),
            **{key: result[key] for key in ("content_digest", "input_digest", "evaluation_digest")},
            "task_sha256": sha256(raw), "task_size": len(raw),
            "files": [_metadata(entry, captured[entry["path"]]) for entry in task["files"]],
            "reviews": [_metadata(spec, reports[spec["path"]]) for spec in task["reviews"] if spec["path"] in reports],
            "check": {"path": "structural-check.json", "sha256": sha256(check_bytes), "size": len(check_bytes)},
            "scoring": task["scoring"], "project": task["project"], "sources": task.get("sources", []),
        }
        manifest["snapshot_digest"] = sha256(canonical(manifest))
        write_json(stage / "manifest.json", manifest)
        verify_snapshot(stage)
    return {"path": str(destination), **manifest}


def verify_snapshot(root: Path) -> dict:
    manifest = parse_json(safe_read(root, "manifest.json"), "manifest.json")
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("check"), dict):
        raise TaskflowError("snapshot manifest schema/check invalid")
    for key in ("files", "reviews"):
        if not isinstance(manifest.get(key), list) or not all(isinstance(v, dict) for v in manifest[key]):
            raise TaskflowError(f"snapshot manifest {key}: expected objects")
    for entry in manifest["files"] + manifest["reviews"] + [manifest["check"]]:
        if not all(key in entry for key in ("path", "size", "sha256")):
            raise TaskflowError("snapshot file entry needs path, size and sha256")
    digest = manifest.get("snapshot_digest")
    if digest != sha256(canonical({k: v for k, v in manifest.items() if k != "snapshot_digest"})):
        raise TaskflowError("snapshot manifest digest mismatch")
    raw = safe_read(root, "task.json")
    if sha256(raw) != manifest.get("task_sha256") or len(raw) != manifest.get("task_size"):
        raise TaskflowError("snapshot task.json integrity mismatch")
    task = parse_json(raw, "snapshot task.json")
    errors = validate_task(task)
    if errors:
        raise TaskflowError("snapshot contract: " + "; ".join(errors))
    captured = {}
    expected = {"manifest.json", "task.json", "structural-check.json"}
    if manifest.get("scoring") != task["scoring"] or manifest.get("task_id") != task["task_id"]:
        raise TaskflowError("snapshot task metadata mismatch")
    if manifest.get("project") != task["project"] or manifest.get("sources") != task.get("sources", []):
        raise TaskflowError("snapshot source metadata mismatch")
    declared = manifest.get("files", [])
    expected_files = [_metadata(entry, safe_read(root, entry["path"])) for entry in task["files"]]
    for entry, actual in zip(declared, expected_files):
        if entry.get("path") == actual["path"] and any(entry.get(key) != actual[key] for key in ("sha256", "size")):
            raise TaskflowError(f"snapshot integrity mismatch: {actual['path']}")
    if canonical(declared) != canonical(expected_files):
        raise TaskflowError("snapshot file manifest mismatch")
    expected_reviews = [r for r in task["reviews"] if any(m.get("path") == r["path"] for m in manifest["reviews"])]
    if [{k: v for k, v in r.items() if k not in {"sha256", "size"}} for r in manifest["reviews"]] != expected_reviews:
        raise TaskflowError("snapshot review manifest mismatch")
    if manifest["check"]["path"] != "structural-check.json":
        raise TaskflowError("snapshot check path mismatch")
    for entry in declared + manifest.get("reviews", []) + [manifest["check"]]:
        path = relative_path(entry["path"])
        if path in expected and path != "structural-check.json":
            raise TaskflowError(f"snapshot duplicate path: {path}")
        data = safe_read(root, path)
        if len(data) != entry["size"] or sha256(data) != entry["sha256"]:
            raise TaskflowError(f"snapshot integrity mismatch: {path}")
        expected.add(path)
        if entry in declared:
            errors = _empty_content_errors(entry, data)
            if errors:
                raise TaskflowError("snapshot file contract: " + "; ".join(errors))
            captured[path] = data
    for key, value in _digests(task, raw, captured).items():
        if manifest.get(key) != value:
            raise TaskflowError(f"snapshot {key} mismatch")
    check = parse_json(safe_read(root, "structural-check.json"), "structural-check.json")
    if (
        check.get("schema_version") != 1 or check.get("status") != "passed"
        or check.get("errors") != [] or not check.get("scope")
        or any(check.get(key) != manifest[key] for key in ("content_digest", "input_digest", "evaluation_digest"))
    ):
        raise TaskflowError("snapshot structural check does not bind the captured content")
    reviews, _ = _review_status(root, task, manifest)
    if any(r["required"] and r["state"] != "passed" for r in reviews):
        raise TaskflowError("snapshot required reviews invalid")
    actual = set()
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(parent) / name
            if path.is_symlink():
                raise TaskflowError(f"snapshot symlink: {path}")
        actual.update((Path(parent) / name).relative_to(root).as_posix() for name in files)
    if actual != expected:
        raise TaskflowError(f"snapshot unexpected/missing files: {sorted(actual ^ expected)}")
    return manifest


def export_snapshot(root: Path, audience: str, destination: Path | None = None) -> dict:
    if audience not in {"solver", "evaluator"}:
        raise TaskflowError("audience must be solver or evaluator")
    manifest = verify_snapshot(root)
    if destination is None:
        destination = root.parent.parent / "exports" / f"{manifest['version_id']}-{audience}"
    selected = [entry for entry in manifest["files"] if audience == "evaluator" or entry["role"] in PUBLIC_ROLES]
    # Solver metadata is deliberately constructed from public facts, never filtered
    # from a full private manifest. It has no teacher names, targets or review data.
    export_manifest = {
        "schema_version": 1, "audience": audience,
        "task_id": manifest["task_id"], "version_id": manifest["version_id"],
        "input_digest": manifest["input_digest"],
        "files": [{**{k: entry[k] for k in ("id", "path", "role", "sha256", "size")},
                   **({"empty_content": entry["empty_content"]}
                      if audience == "evaluator" and "empty_content" in entry else {})} for entry in selected],
    }
    if audience == "evaluator":
        export_manifest.update(
            scoring=manifest["scoring"], sources=manifest["sources"], project=manifest["project"],
            evaluation_digest=manifest["evaluation_digest"], snapshot_digest=manifest["snapshot_digest"],
        )
    with atomic_directory(destination) as stage:
        for entry in selected:
            data = safe_read(root, entry["path"])
            if sha256(data) != entry["sha256"]:
                raise TaskflowError(f"snapshot changed during export: {entry['path']}")
            target = stage / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        write_json(stage / "package.json", export_manifest)
        verify_export(stage)
        verify_snapshot(root)  # Detect a concurrent change before publication.
    return {"path": str(destination), **export_manifest}


def verify_export(root: Path) -> dict:
    """Re-read every exported byte and exact membership before consumption."""
    manifest = parse_json(safe_read(root, "package.json"), "package.json")
    if manifest.get("schema_version") != 1 or manifest.get("audience") not in ("solver", "evaluator"):
        raise TaskflowError("export package schema/audience invalid")
    if manifest["audience"] == "solver" and set(manifest) != {
        "schema_version", "audience", "task_id", "version_id", "input_digest", "files"
    }:
        raise TaskflowError("solver export contains unexpected/private metadata")
    files = manifest.get("files")
    if not isinstance(files, list) or not all(isinstance(f, dict) for f in files):
        raise TaskflowError("export package files: expected objects")
    expected = {"package.json"}
    public = []
    known = {entry["id"] for entry in files if isinstance(entry.get("id"), str)}
    sources = manifest.get("sources", [])
    known.update(source["id"] for source in (sources if isinstance(sources, list) else [])
                 if isinstance(source, dict) and isinstance(source.get("id"), str))
    for entry in files:
        fields = {"id", "path", "role", "size", "sha256"}
        if manifest["audience"] == "evaluator" and "empty_content" in entry:
            fields.add("empty_content")
        if set(entry) != fields:
            raise TaskflowError("export file metadata must contain five public fields and only an explicit evaluator empty_content declaration")
        path = relative_path(entry.get("path"))
        role = entry.get("role")
        if path in expected or not isinstance(role, str) or role not in ROLES:
            raise TaskflowError(f"export duplicate path or invalid role: {path}")
        if manifest["audience"] == "solver" and role not in PUBLIC_ROLES:
            raise TaskflowError(f"export public/private role conflict: {path}")
        data = safe_read(root, path)
        if sha256(data) != entry.get("sha256") or len(data) != entry.get("size"):
            raise TaskflowError(f"export file integrity mismatch: {path}")
        errors = _empty_content_errors(entry, data, known)
        if errors:
            raise TaskflowError("export file contract: " + "; ".join(errors))
        expected.add(path)
        if role in PUBLIC_ROLES:
            public.append(entry)
    if sha256(canonical(public)) != manifest.get("input_digest"):
        raise TaskflowError("export public input digest mismatch")
    actual = set()
    for parent, dirs, filenames in os.walk(root, followlinks=False):
        for name in dirs + filenames:
            if (Path(parent) / name).is_symlink():
                raise TaskflowError("export contains a symlink")
        actual.update((Path(parent) / name).relative_to(root).as_posix() for name in filenames)
    if actual != expected:
        raise TaskflowError(f"export unexpected/missing members: {sorted(actual ^ expected)}")
    return manifest
