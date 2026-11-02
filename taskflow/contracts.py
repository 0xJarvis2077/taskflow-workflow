"""Portable task contracts and safe local filesystem primitives."""

import hashlib
import ctypes
import errno
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterator

PUBLIC_ROLES = frozenset({"prompt", "input", "template"})
PRIVATE_ROLES = frozenset({"answer", "rubric", "teacher", "verification"})
ROLES = PUBLIC_ROLES | PRIVATE_ROLES
REVIEW_KINDS = frozenset({"business", "artifact", "calculation"})
ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}\Z")


class TaskflowError(ValueError):
    """An invalid contract or unsafe operation with an actionable message."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise TaskflowError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse_json(data: bytes, label: str = "JSON") -> dict:
    try:
        value = json.loads(data, object_pairs_hook=_unique_object)
        if not isinstance(value, dict):
            raise TaskflowError(f"{label}: expected a JSON object")
        canonical(value)  # Reject non-finite numbers accepted by json.loads.
        return value
    except (UnicodeError, ValueError, TypeError) as exc:
        raise TaskflowError(f"{label}: {exc}") from exc


def read_json(path: Path) -> dict:
    return parse_json(path.read_bytes(), str(path))


def write_json(path: Path, value: object) -> None:
    """Write inside a private staging directory; publication is separate."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(canonical(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def validate_id(value: object, label: str = "id") -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
        raise TaskflowError(f"{label}: use 1–100 ASCII letters, digits, _, . or -")
    if value in {".", ".."}:
        raise TaskflowError(f"{label}: invalid identifier")
    return value


def relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise TaskflowError(f"unsafe relative path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in {"", ".", ".."} for p in value.split("/")):
        raise TaskflowError(f"unsafe relative path: {value!r}")
    if ":" in path.parts[0]:
        raise TaskflowError(f"unsafe relative path: {value!r}")
    return value


def safe_path(root: Path, rel: str) -> Path:
    relative_path(rel)
    if root.is_symlink():
        raise TaskflowError(f"symlink root: {root}")
    current = root
    for part in PurePosixPath(rel).parts:
        current = current / part
        if current.is_symlink():
            raise TaskflowError(f"symlink is not allowed: {rel}")
    return current


def safe_read(root: Path, rel: str) -> bytes:
    """Read through directory descriptors, refusing symlinks and special files."""
    safe_path(root, rel)
    parts = PurePosixPath(rel).parts
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            next_descriptor = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        file_descriptor = os.open(
            parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=descriptor,
        )
        with os.fdopen(file_descriptor, "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise TaskflowError(f"not a regular file: {rel}")
            return handle.read()
    except OSError as exc:
        raise TaskflowError(f"cannot read {rel}: {exc}") from exc
    finally:
        os.close(descriptor)


def _publish_no_replace(source: Path, destination: Path) -> None:
    """Use kernel no-replace rename, including against non-cooperating writers."""
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin" and hasattr(library, "renamex_np"):
        operation = library.renamex_np
        operation.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(os.fsencode(source), os.fsencode(destination), 4)  # RENAME_EXCL
    elif sys.platform.startswith("linux") and hasattr(library, "renameat2"):
        operation = library.renameat2
        operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    else:
        raise TaskflowError("atomic no-replace directory publication requires macOS/Linux support")
    if result != 0:
        code = ctypes.get_errno()
        if code in (errno.EEXIST, errno.ENOTEMPTY):
            raise TaskflowError(f"already exists; refusing overwrite: {destination}")
        raise OSError(code, os.strerror(code), str(destination))


@contextmanager
def atomic_directory(destination: Path) -> Iterator[Path]:
    """Publish a complete directory once; never overwrite an existing version.

    An exclusive sibling lock serializes cooperating processes. A killed process
    may leave a lock/staging directory; those are evidence, never valid releases.
    """
    if destination.parent.is_symlink():
        raise TaskflowError(f"symlink parent: {destination.parent}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    lock = destination.with_name(f".{destination.name}.lock")
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise TaskflowError(f"operation locked: {lock}; inspect before removing") from exc
    staging = None
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(f"pid={os.getpid()}\ncreated_at={utc_now()}\n")
        if destination.exists() or destination.is_symlink():
            raise TaskflowError(f"already exists; refusing overwrite: {destination}")
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
        yield staging
        if destination.exists() or destination.is_symlink():
            raise TaskflowError(f"destination appeared during publication: {destination}")
        _publish_no_replace(staging, destination)
        staging = None
    finally:
        if staging is not None:
            shutil.rmtree(staging)
        lock.unlink(missing_ok=True)


def _known_references(value: object, known: set[str], label: str, errors: list[str]) -> None:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        errors.append(f"{label}: expected a list of IDs")
    elif set(value) - known:
        errors.append(f"{label}: unknown references {sorted(set(value) - known)}")


def validate_empty_content(entry: dict, known: set[str] | None = None) -> list[str]:
    """Validate an explicit zero-record fixture declaration, not its business use."""
    if "empty_content" not in entry:
        return []
    errors: list[str] = []
    label = str(entry.get("path", "file")) + ".empty_content"
    value = entry["empty_content"]
    if not isinstance(value, dict) or set(value) != {"kind", "reason", "evidence"}:
        return [f"{label}: expected exactly kind, reason and evidence"]
    if value["kind"] != "zero_record_jsonl":
        errors.append(f"{label}.kind: expected zero_record_jsonl")
    role = entry.get("role")
    if not isinstance(role, str) or role not in {"teacher", "verification"} or Path(str(entry.get("path", ""))).suffix.lower() != ".jsonl":
        errors.append(f"{label}: only private teacher/verification .jsonl fixtures may declare zero records")
    if not isinstance(value["reason"], str) or not value["reason"].strip():
        errors.append(f"{label}.reason: nonblank explanation required")
    evidence = value["evidence"]
    if (not isinstance(evidence, list) or not evidence
            or not all(isinstance(item, str) and item for item in evidence)
            or len(set(evidence)) != len(evidence)):
        errors.append(f"{label}.evidence: nonempty unique declared file/source IDs required")
    elif known is not None:
        _known_references(evidence, known, label + ".evidence", errors)
    return errors


def _number(value: object) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_task(task: dict) -> list[str]:
    """Validate program-readable structure; this does not judge business quality."""
    errors: list[str] = []
    if task.get("schema_version") != 1:
        errors.append("schema_version: expected 1")
    try:
        validate_id(task.get("task_id"), "task_id")
    except TaskflowError as exc:
        errors.append(str(exc))
    if not isinstance(task.get("title"), str) or not task["title"].strip():
        errors.append("title: required")
    project = task.get("project")
    if (
        not isinstance(project, dict)
        or not isinstance(project.get("name"), str) or not project["name"].strip()
        or not isinstance(project.get("rule_sources"), list)
        or not project["rule_sources"]
        or not all(isinstance(s, str) and s for s in project["rule_sources"])
    ):
        errors.append("project: name and rule_sources required (phase may remain unknown)")
    sources = task.get("sources", [])
    if not isinstance(sources, list) or not all(isinstance(s, dict) for s in sources):
        errors.append("sources: expected objects")
        sources = []
    source_ids = [s.get("id") for s in sources]
    for source in sources:
        try:
            validate_id(source.get("id"), "source.id")
        except TaskflowError as exc:
            errors.append(str(exc))
        if not all(isinstance(source.get(k), str) and source[k] for k in ("id", "kind", "reference")):
            errors.append("sources: each needs id, kind and reference")
    files = task.get("files")
    if not isinstance(files, list) or not files or not all(isinstance(f, dict) for f in files):
        errors.append("files: expected a nonempty list of objects")
        files = []
    ids = [f.get("id") for f in files] + source_ids
    if not all(isinstance(i, str) and i for i in ids) or len(set(str(i) for i in ids)) != len(ids):
        errors.append("files/sources: IDs must be nonempty and unique")
    known = {i for i in ids if isinstance(i, str)}
    paths: set[str] = set()
    roles = {}
    for index, item in enumerate(files):
        label = f"files[{index}]"
        try:
            validate_id(item.get("id"), f"{label}.id")
        except TaskflowError as exc:
            errors.append(str(exc))
        try:
            path = relative_path(item.get("path"))
            if not path.startswith(("draft/", "checks/")) and path != "record.md":
                errors.append(f"{label}.path: use draft/, checks/ or record.md")
            if path in paths:
                errors.append(f"{label}.path: duplicate {path}")
            paths.add(path)
        except TaskflowError as exc:
            errors.append(f"{label}: {exc}")
        role = item.get("role")
        if not isinstance(role, str) or role not in ROLES:
            errors.append(f"{label}.role: expected one of {sorted(ROLES)}")
        roles[str(item.get("id"))] = role
        _known_references(item.get("references", []), known, f"{label}.references", errors)
        errors.extend(validate_empty_content(item, known))
    for item in files:
        if isinstance(item.get("role"), str) and item["role"] in PUBLIC_ROLES:
            for ref in item.get("references", []) if isinstance(item.get("references", []), list) else []:
                if isinstance(roles.get(str(ref)), str) and roles[str(ref)] in PRIVATE_ROLES:
                    errors.append(f"public file {item.get('id')} references private file {ref}")
    for role in ("prompt", "answer", "rubric"):
        if role not in roles.values():
            errors.append(f"files: missing {role} role")
    scoring = task.get("scoring", {})
    if not isinstance(scoring, dict):
        scoring = {}
    total = scoring.get("total")
    if not _number(total) or total <= 0:
        errors.append("scoring.total: positive finite configured score required")
    if roles.get(str(scoring.get("rubric_file"))) != "rubric":
        errors.append("scoring.rubric_file: must reference a rubric file ID")
    criteria = scoring.get("criteria")
    if not isinstance(criteria, list) or not criteria or not all(isinstance(c, dict) for c in criteria):
        errors.append("scoring.criteria: nonempty objects required")
        criteria = []
    criterion_ids = [c.get("id") for c in criteria]
    if len(set(str(i) for i in criterion_ids)) != len(criterion_ids):
        errors.append("scoring.criteria: duplicate ID")
    score_sum = 0.0
    for index, criterion in enumerate(criteria):
        label = f"scoring.criteria[{index}]"
        try:
            validate_id(criterion.get("id"), f"{label}.id")
        except TaskflowError as exc:
            errors.append(str(exc))
        for key in ("id", "dimension", "condition", "allowed", "tolerance", "evidence"):
            if not criterion.get(key):
                errors.append(f"{label}.{key}: required")
        for key in ("dimension", "condition", "tolerance"):
            if not isinstance(criterion.get(key), str):
                errors.append(f"{label}.{key}: expected text")
        allowed = criterion.get("allowed")
        if not isinstance(allowed, str) and not (
            isinstance(allowed, list) and all(isinstance(a, str) or _number(a) for a in allowed)
        ):
            errors.append(f"{label}.allowed: expected text or list of text/finite numbers")
        points = criterion.get("max_score")
        if not _number(points) or points <= 0:
            errors.append(f"{label}.max_score: positive finite number required")
        else:
            score_sum += points
        _known_references(criterion.get("evidence"), known, f"{label}.evidence", errors)
    if _number(total) and not math.isclose(score_sum, total, rel_tol=1e-10, abs_tol=1e-9):
        errors.append(f"scoring.total: configured {total}, criteria sum {score_sum}")
    reviews = task.get("reviews", [])
    if not isinstance(reviews, list) or not all(isinstance(r, dict) for r in reviews):
        errors.append("reviews: expected objects")
        reviews = []
    review_ids = []
    for review in reviews:
        review_ids.append(review.get("id"))
        try:
            validate_id(review.get("id"), "review.id")
            path = relative_path(review.get("path"))
            if not path.startswith("reports/") or path in paths:
                errors.append("reviews.path: unique reports/ path required; exclude reports from files")
            paths.add(path)
        except TaskflowError as exc:
            errors.append(str(exc))
        if not isinstance(review.get("kind"), str) or review["kind"] not in REVIEW_KINDS or not isinstance(review.get("required"), bool):
            errors.append("reviews: valid kind and boolean required needed")
    if len(set(str(i) for i in review_ids)) != len(review_ids):
        errors.append("reviews: duplicate ID")
    for kind in ("business", "artifact"):
        if not any(r.get("kind") == kind and r.get("required") is True for r in reviews):
            errors.append(f"reviews: required {kind} review must be configured")
    return errors
