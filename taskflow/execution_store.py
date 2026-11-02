"""Transactional work ownership and evidence gates for local role execution.

SQLite owns mutable scheduling state. Evidence files remain in the existing
filesystem; JSON returned here is a view, never a second editable state store.
Business verdicts are attributed inputs, not conclusions made by this module.
"""

import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from taskflow.contracts import (
    PUBLIC_ROLES, TaskflowError, canonical, relative_path, safe_read, sha256,
    validate_id,
)

ACTIVE = ("claimed", "starting", "running", "collecting", "uncertain")
TERMINAL = ("completed", "failed", "cancelled")
DECISIONS = ("research", "repair", "reconstruct", "experiment", "retain", "environment")


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError(f"{label}: non-empty text required")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise TaskflowError(f"{label}: SHA256 required")
    try:
        int(value, 16)
    except ValueError as exc:
        raise TaskflowError(f"{label}: SHA256 required") from exc
    return value


def _iso_time(value: Any, label: str) -> datetime:
    _required_text(value, label)
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("timezone required")
        return instant.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise TaskflowError(f"{label}: valid ISO time with timezone required") from exc


def _observation_time(value: dict[str, Any]) -> tuple[datetime, str, str | None]:
    observed = _iso_time(value.get("observed_at"), "observed_at")
    details = value.get("details", {})
    if "reported_at" in details:
        try:
            return _iso_time(details["reported_at"], "details.reported_at"), "reported_at", None
        except TaskflowError as exc:
            return observed, "observed_at_fallback", str(exc)
    return observed, "observed_at", None


def _external_authors(sids: Any, evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if sids is None:
        return []
    if not isinstance(sids, list) or any(not isinstance(sid, str) for sid in sids) or len(sids) != len(set(sids)):
        raise TaskflowError("external_author_sids: unique UUID string list required")
    for sid in sids:
        try:
            if not isinstance(sid, str) or str(uuid.UUID(sid)) != sid:
                raise ValueError("non-canonical UUID")
        except (ValueError, AttributeError) as exc:
            raise TaskflowError("external_author_sids: canonical UUID required") from exc
    validated: dict[str, dict[str, Any]] = {}
    for source in evidence:
        path = _evidence_path(source["path"])
        raw = safe_read(Path(path.anchor), str(path.relative_to(path.anchor)))
        if sha256(raw) != source["sha256"]:
            raise TaskflowError(f"external author trace changed: {path}")
        starts, terminals = [], []
        try:
            for line in raw.decode("utf-8").splitlines():
                if not line.strip():
                    continue
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError("trace event must be an object")
                if event.get("type") == "thread.started":
                    starts.append(event.get("thread_id"))
                if event.get("type") in ("turn.completed", "turn.failed", "error"):
                    terminals.append(event["type"])
        except (ValueError, UnicodeError):
            # Most candidate evidence is not a CLI trace. It remains ordinary
            # evidence but cannot authenticate an external execution context.
            continue
        if len(starts) == 1 and terminals == ["turn.completed"] and starts[0] in sids:
            validated[starts[0]] = {"cli_sid": starts[0], "trace": source, "source": "external_cli_trace", "engine_attempt_id": None}
    if set(validated) != set(sids):
        raise TaskflowError("external author requires complete actual CLI trace for every declared SID")
    return [validated[sid] for sid in sids]


def validate_evidence(evidence: Any) -> list[dict[str, Any]]:
    """Read evidence bytes and retain an explicit location in each source."""
    if not isinstance(evidence, list) or not evidence:
        raise TaskflowError("evidence: non-empty list required")
    result = []
    for item in evidence:
        if not isinstance(item, dict):
            raise TaskflowError("evidence item: object required")
        path = _evidence_path(_required_text(item.get("path"), "evidence.path"))
        expected = _digest(item.get("sha256"), "evidence.sha256")
        location = _required_text(item.get("location"), "evidence.location")
        # Canonicalize OS path aliases (macOS /tmp and /var) before descriptor
        # traversal. The referenced final file must not itself be a symlink.
        raw = safe_read(Path(path.anchor), str(path.relative_to(path.anchor)))
        if sha256(raw) != expected:
            raise TaskflowError(f"evidence changed: {path}")
        result.append({**item, "path": str(path), "location": location})
    return result


def evidence_ref(path: Path, location: str) -> dict[str, Any]:
    """Capture the current bytes of one source for a later attributed record."""
    path = _evidence_path(path)
    raw = safe_read(Path(path.anchor), str(path.relative_to(path.anchor)))
    return {"path": str(path), "sha256": sha256(raw), "location": _required_text(location, "location")}


def _evidence_path(value: str | Path) -> Path:
    path = Path(value).absolute()
    if path.is_symlink():
        raise TaskflowError(f"symlink evidence is not allowed: {path}")
    return path.parent.resolve(strict=True) / path.name


def _data(row: sqlite3.Row | None, label: str = "record") -> dict[str, Any]:
    if row is None:
        raise TaskflowError(f"unknown {label}")
    return json.loads(row["data"])


class ExecutionStore:
    """One-machine scheduler with conservative crash reconciliation.

    A claim never expires into another model invocation. An ambiguous attempt
    holds its slot until explicit reconciliation with process and file evidence.
    """

    def __init__(self, path: Path):
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS policies (
                    id TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS candidates (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    parent_id TEXT, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS work (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL, intent_id TEXT UNIQUE NOT NULL,
                    state TEXT NOT NULL, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS attempts (
                    id TEXT PRIMARY KEY, work_id TEXT UNIQUE NOT NULL,
                    task_id TEXT NOT NULL, state TEXT NOT NULL,
                    modifies INTEGER NOT NULL, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reviews (
                    id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL,
                    data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS defects (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    state TEXT NOT NULL, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS decisions (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                    data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS controls (
                    scope TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS observations (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS ownership (
                    task_id TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS execution_flows (
                    flow_id TEXT PRIMARY KEY, definition TEXT NOT NULL,
                    definition_digest TEXT NOT NULL, state TEXT NOT NULL,
                    revision INTEGER NOT NULL, event_seq INTEGER NOT NULL,
                    evidence TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    at REAL NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL);
            """)

    @contextmanager
    def _db(self, write: bool = False) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=15000")
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except sqlite3.IntegrityError as exc:
            if write:
                db.rollback()
            raise TaskflowError(f"immutable identity already exists: {exc}") from exc
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _event(db: sqlite3.Connection, kind: str, data: dict[str, Any]) -> int:
        cursor = db.execute(
            "INSERT INTO events(at,kind,data) VALUES(?,?,?)",
            (time.time(), kind, canonical(data).decode()),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _load(db: sqlite3.Connection, table: str, identity: str) -> dict[str, Any]:
        # table names are internal constants, never CLI input.
        return _data(db.execute(f"SELECT data FROM {table} WHERE id=?", (identity,)).fetchone(), table)

    def register_policy(self, policy_id: str, config: dict[str, Any]) -> dict[str, Any]:
        validate_id(policy_id, "policy_id")
        if not isinstance(config, dict):
            raise TaskflowError("policy config: object required")
        limit = config.get("concurrency")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise TaskflowError("policy.concurrency: positive integer required")
        roles = config.get("roles")
        if not isinstance(roles, dict) or not roles:
            raise TaskflowError("policy.roles: role configuration required")
        for role, setting in roles.items():
            validate_id(role, "role")
            if not isinstance(setting, dict):
                raise TaskflowError("role setting: object required")
            for key in ("model", "reasoning_effort"):
                _required_text(setting.get(key), key)
            timeout = setting.get("timeout_seconds")
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
                raise TaskflowError("role.timeout_seconds: positive number required")
        from taskflow.execution_batch import validate_batch_policy

        validate_batch_policy(config)
        value = {"policy_id": policy_id, "config": config, "digest": sha256(canonical(config))}
        with self._db(True) as db:
            if config.get("model_confirmation_required") is True:
                previous = db.execute("SELECT data FROM policies WHERE id=?", (policy_id,)).fetchone()
                if previous is not None:
                    original = _data(previous)
                    if canonical(original) != canonical(value):
                        raise TaskflowError("existing batch policy ID has different original configuration")
                    return original
            db.execute("INSERT INTO policies VALUES(?,?)", (policy_id, canonical(value).decode()))
            self._event(db, "policy_registered", value)
        return value

    def register_candidate(
        self, task_id: str, candidate_id: str, *, input_digest: str,
        content_digest: str, evaluation_digest: str,
        author_attempt: str | None = None, parent_id: str | None = None,
        evidence: list[dict[str, Any]],
        external_author_sids: list[str] | None = None,
        files: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        for key, value in (("task_id", task_id), ("candidate_id", candidate_id)):
            validate_id(value, key)
        for key, value in (("input", input_digest), ("content", content_digest), ("evaluation", evaluation_digest)):
            _digest(value, key)
        refs = validate_evidence(evidence)
        captured_files = validate_evidence(files) if files else []
        seen_files = set()
        for item in captured_files:
            package_path = relative_path(item.get("package_path"))
            if package_path in seen_files or item.get("role") not in PUBLIC_ROLES | {"answer", "rubric", "teacher", "verification"}:
                raise TaskflowError("candidate files require unique package_path and declared role")
            seen_files.add(package_path)
        external = _external_authors(external_author_sids, refs)
        with self._db(True) as db:
            parent = None
            if parent_id:
                parent = self._load(db, "candidates", parent_id)
                if parent["task_id"] != task_id:
                    raise TaskflowError("candidate parent belongs to another task")
                if not author_attempt and not external:
                    raise TaskflowError("revision requires an author attempt")
            if author_attempt:
                author = self._load(db, "attempts", author_attempt)
                if author["task_id"] != task_id or author["role"] not in ("author", "repair"):
                    raise TaskflowError("candidate requires a completed author/repair attempt for this task")
                if author["state"] != "completed":
                    resolved = self._validated_delivery_resolution(author)["candidate"]
                    if any(resolved[key] != value for key, value in (
                            ("candidate_id", candidate_id), ("task_id", task_id),
                            ("input_digest", input_digest), ("content_digest", content_digest),
                            ("evaluation_digest", evaluation_digest))):
                        raise TaskflowError("Resolved author delivery cannot register another candidate or version")
                if author.get("supplemental_delivery") is not None:
                    supplied = self._validated_optional_candidate_delivery(author)["candidate_binding"]
                    if any(supplied[key] != value for key, value in (
                            ("task_id", task_id), ("input_digest", input_digest),
                            ("content_digest", content_digest), ("evaluation_digest", evaluation_digest))):
                        raise TaskflowError("Optional author delivery belongs to another candidate version")
                if parent_id and author["candidate_id"] != parent_id:
                    raise TaskflowError("author attempt did not receive the revision parent")
                if parent is None:
                    parent_id = author["candidate_id"]
                    parent = self._load(db, "candidates", parent_id)
                self._validate_revision_delivery(parent, captured_files, refs, author)
            value = {
                "candidate_id": candidate_id, "task_id": task_id, "parent_id": parent_id,
                "input_digest": input_digest, "content_digest": content_digest,
                "evaluation_digest": evaluation_digest, "author_attempt": author_attempt,
                "external_author_sids": [item["cli_sid"] for item in external],
                "external_provenance": external,
                "files": captured_files,
                "evidence": refs,
            }
            value["created_seq"] = self._event(db, "candidate_registered", value)
            db.execute("INSERT INTO candidates VALUES(?,?,?,?)", (candidate_id, task_id, parent_id, canonical(value).decode()))
        return value

    @staticmethod
    def _validate_revision_delivery(parent: dict[str, Any], files: list[dict[str, Any]],
                                    evidence: list[dict[str, Any]], author: dict[str, Any]) -> None:
        if not files:
            raise TaskflowError("managed revision requires captured candidate files and actual author delivery")
        prior = {item["package_path"]: item for item in parent.get("files", [])}
        current = {item["package_path"]: item for item in files}
        changed = [item for key, item in current.items() if prior.get(key, {}).get("sha256") != item["sha256"]]
        delivered = ExecutionStore._delivered_artifacts(author)
        if any(item["sha256"] not in delivered for item in changed):
            raise TaskflowError("changed candidate files must match this author's actually collected delivery SHA256")
        old_declaration = next((item for item in parent["evidence"] if Path(item["path"]).name == "task.json"), None)
        new_declaration = next((item for item in evidence if Path(item["path"]).name == "task.json"), None)
        declaration_changed = old_declaration is not None and new_declaration is not None and old_declaration["sha256"] != new_declaration["sha256"]
        if declaration_changed and new_declaration["sha256"] not in delivered:
            raise TaskflowError("changed package declaration must match the author's actual delivery SHA256")
        if set(prior) - set(current) and not declaration_changed:
            raise TaskflowError("removed candidate files require the author's changed package declaration")

    def enqueue(
        self, work_id: str, task_id: str, candidate_id: str, role: str,
        policy_id: str, *, intent_id: str, purpose: str, request_digest: str,
        modifies: bool = False, dependencies: list[str] | None = None,
        runtime_path: str | None = None,
        evidence: list[dict[str, Any]] | None = None,
        dispatch_owner: str | None = None,
        model_slot: str | None = None,
    ) -> dict[str, Any]:
        for key, value in (("work_id", work_id), ("intent_id", intent_id), ("role", role)):
            validate_id(value, key)
        _required_text(purpose, "purpose")
        _digest(request_digest, "request_digest")
        if not isinstance(modifies, bool):
            raise TaskflowError("modifies: boolean required")
        if role in ("author", "repair"):
            modifies = True
        dependencies = dependencies or []
        refs = validate_evidence(evidence) if evidence is not None else []
        if runtime_path is not None:
            runtime_path = str(Path(runtime_path).absolute())
            if not Path(runtime_path).is_dir() or Path(runtime_path).is_symlink():
                raise TaskflowError("runtime_path must be an existing locked runtime directory")
        if not isinstance(dependencies, list):
            raise TaskflowError("dependencies: list required")
        with self._db(True) as db:
            candidate = self._load(db, "candidates", candidate_id)
            policy = self._load(db, "policies", policy_id)
            ownership = self._dispatch_ownership(db, task_id, work_id)
            if ownership is not None and dispatch_owner != ownership["owner"]:
                raise TaskflowError("dispatch_owner must match the scoped controller handoff receipt")
            if candidate["task_id"] != task_id or role not in policy["config"]["roles"]:
                raise TaskflowError("candidate task or policy role mismatch")
            from taskflow.execution_batch_binding import make_batch_binding

            batch = make_batch_binding(policy, role, model_slot, task_id)
            if modifies and role not in ("author", "repair"):
                raise TaskflowError("only author/repair can claim mutable ownership")
            for dependency in dependencies:
                self._load(db, "work", dependency)
            experiment_decision = None
            if role == "solver":
                self._assert_quality(db, candidate_id)
                experiment_decision = self._assert_experiment(db, candidate_id, purpose)
            value = {
                "work_id": work_id, "task_id": task_id, "candidate_id": candidate_id,
                "role": role, "policy_id": policy_id, "policy_digest": policy["digest"],
                "intent_id": intent_id, "purpose": purpose, "request_digest": request_digest,
                "modifies": modifies, "dependencies": dependencies, "state": "ready",
                "input_digest": candidate["input_digest"], "content_digest": candidate["content_digest"],
                "runtime_path": runtime_path, "evidence": refs,
                "experiment_decision_id": experiment_decision["decision_id"] if experiment_decision else None,
                "dispatch_owner": dispatch_owner,
                "ownership_event_seq": ownership["event_seq"] if ownership else None,
            }
            if batch is not None:
                value["model_slot"] = batch["model_slot"]
            db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", (work_id, task_id, candidate_id, intent_id, "ready", canonical(value).decode()))
            self._event(db, "dispatch_intent", value)
        return value

    def set_control(self, scope: str, action: str, reason: str) -> None:
        """Pause/resume future claims or cancel ready work, never rewrite requests."""
        if action not in ("pause", "resume", "cancel"):
            raise TaskflowError("control action must be pause/resume/cancel")
        _required_text(scope, "scope")
        _required_text(reason, "reason")
        value = {"scope": scope, "action": action, "reason": reason, "applies_to": "future_claims"}
        with self._db(True) as db:
            db.execute("INSERT INTO controls VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET data=excluded.data", (scope, canonical(value).decode()))
            if action == "cancel":
                for row in db.execute("SELECT data FROM work WHERE state='ready'").fetchall():
                    work = _data(row)
                    if scope in ("all", f"task:{work['task_id']}", f"work:{work['work_id']}"):
                        work["state"] = "cancelled"
                        db.execute("UPDATE work SET state='cancelled',data=? WHERE id=?", (canonical(work).decode(), work["work_id"]))
            self._event(db, "control", value)

    @staticmethod
    def _blocked(db: sqlite3.Connection, work: dict[str, Any]) -> bool:
        scopes = ("all", f"task:{work['task_id']}", f"work:{work['work_id']}")
        return any(
            json.loads(row["data"])["action"] != "resume"
            for row in db.execute("SELECT data FROM controls WHERE scope IN (?,?,?)", scopes)
        )

    @staticmethod
    def _dispatch_ownership(db: sqlite3.Connection, task_id: str, work_id: str) -> dict[str, Any] | None:
        observed = db.execute("SELECT 1 FROM observations WHERE task_id=? LIMIT 1", (task_id,)).fetchone()
        if observed is None:
            return None
        specific = db.execute("SELECT data FROM ownership WHERE task_id=?", (f"{task_id}::{work_id}",)).fetchone()
        general = db.execute("SELECT data FROM ownership WHERE task_id=?", (task_id,)).fetchone()
        receipt = specific or general
        if receipt is None:
            raise TaskflowError("observed task remains with its controller; scoped ownership receipt required")
        return _data(receipt)

    @staticmethod
    def _check_work_ownership(db: sqlite3.Connection, work: dict[str, Any]) -> None:
        ownership = ExecutionStore._dispatch_ownership(db, work["task_id"], work["work_id"])
        if ownership is not None and (
            work.get("dispatch_owner") != ownership["owner"]
            or work.get("ownership_event_seq") != ownership["event_seq"]
        ):
            raise TaskflowError("scoped dispatch ownership changed; old queued intent requires explicit handoff")

    def claim(self, worker_id: str, work_ids: list[str] | None = None) -> dict[str, Any] | None:
        _required_text(worker_id, "worker_id")
        eligible = None
        if work_ids is not None:
            if not isinstance(work_ids, list):
                raise TaskflowError("claim work_ids: list or None required")
            eligible = {validate_id(identity, "claim work_id") for identity in work_ids}
            if not eligible:
                return None
        with self._db(True) as db:
            active = [_data(row) for row in db.execute("SELECT data FROM attempts WHERE state IN ('claimed','starting','running','collecting','uncertain')")]
            for row in db.execute("SELECT data FROM work WHERE state='ready' ORDER BY rowid").fetchall():
                work = _data(row)
                if eligible is not None and work["work_id"] not in eligible:
                    continue
                if self._blocked(db, work):
                    continue
                try:
                    self._check_work_ownership(db, work)
                except TaskflowError:
                    continue
                policy = self._load(db, "policies", work["policy_id"])
                # The most restrictive active policy remains effective; updating a
                # policy does not silently expand old in-flight concurrency.
                limits = [policy["config"]["concurrency"]]
                limits.extend(self._load(db, "policies", item["policy_id"])["config"]["concurrency"] for item in active)
                if len(active) >= min(limits):
                    continue
                if work["modifies"] and any(item["task_id"] == work["task_id"] and item["modifies"] for item in active):
                    continue
                if any(self._load(db, "work", dep)["state"] != "completed" for dep in work["dependencies"]):
                    continue
                if work["role"] == "solver":
                    try:
                        self._assert_quality(db, work["candidate_id"])
                    except TaskflowError:
                        continue
                attempt = {
                    **work, "attempt_id": uuid.uuid4().hex, "worker_id": worker_id,
                    "state": "claimed", "claimed_at": time.time(), "pid": None,
                    "process_identity": None, "cli_sid": None, "process_exit": None,
                    "response_complete": False, "artifacts_complete": False,
                    "technical_accepted": False, "evidence": [],
                }
                attempt["claimed_seq"] = self._event(db, "claimed", attempt)
                db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?)", (attempt["attempt_id"], work["work_id"], work["task_id"], "claimed", int(work["modifies"]), canonical(attempt).decode()))
                work["state"] = "claimed"
                db.execute("UPDATE work SET state='claimed',data=? WHERE id=?", (canonical(work).decode(), work["work_id"]))
                return attempt
        return None

    def _update_attempt(self, db: sqlite3.Connection, value: dict[str, Any], kind: str) -> None:
        value["last_seq"] = self._event(db, kind, value)
        db.execute("UPDATE attempts SET state=?,data=? WHERE id=?", (value["state"], canonical(value).decode(), value["attempt_id"]))
        work = self._load(db, "work", value["work_id"])
        work["state"] = value["state"]
        db.execute("UPDATE work SET state=?,data=? WHERE id=?", (work["state"], canonical(work).decode(), work["work_id"]))

    def mark_starting(self, attempt_id: str) -> dict[str, Any]:
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] != "claimed":
                raise TaskflowError("starting requires claimed attempt")
            attempt.update(state="starting", start_intent_at=time.time())
            self._update_attempt(db, attempt, "starting")
            return attempt

    def record_process(self, attempt_id: str, pid: int, process_identity: str, cli_sid: str | None = None) -> dict[str, Any]:
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise TaskflowError("pid: positive integer required")
        _required_text(process_identity, "process_identity")
        if cli_sid is not None:
            _required_text(cli_sid, "cli_sid")
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] not in ("starting", "uncertain"):
                raise TaskflowError("process recording requires starting/uncertain attempt")
            attempt.update(state="running", pid=pid, process_identity=process_identity, cli_sid=cli_sid)
            self._update_attempt(db, attempt, "process_recorded")
            return attempt

    def mark_collecting(self, attempt_id: str) -> dict[str, Any]:
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] not in ("running", "uncertain"):
                raise TaskflowError("collecting requires running/uncertain attempt")
            attempt["state"] = "collecting"
            self._update_attempt(db, attempt, "collecting")
            return attempt

    def finish_attempt(self, attempt_id: str, *, process_exit: int | None, response_complete: bool, artifacts_complete: bool, technical_accepted: bool, evidence: list[dict[str, Any]], cli_sid: str | None = None, process_state: str = "unknown") -> dict[str, Any]:
        refs = validate_evidence(evidence)
        if process_exit is not None and (isinstance(process_exit, bool) or not isinstance(process_exit, int)):
            raise TaskflowError("process_exit: integer or unknown required")
        if any(not isinstance(flag, bool) for flag in (response_complete, artifacts_complete, technical_accepted)):
            raise TaskflowError("completion observations: booleans required")
        if cli_sid is not None:
            _required_text(cli_sid, "cli_sid")
        if process_state not in ("unknown", "not_started"):
            raise TaskflowError("process_state: unknown/not_started required")
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] not in ACTIVE:
                raise TaskflowError("terminal attempts are immutable; append another intent")
            complete = process_exit == 0 and response_complete and artifacts_complete and technical_accepted
            proven_not_started = False
            if process_state == "not_started":
                for source in refs:
                    source_path = Path(source["path"])
                    runtime = attempt.get("runtime_path")
                    if (source_path.name not in ("termination.json", "collection.json")
                            or not runtime or source_path.parent.resolve() != Path(runtime).resolve()):
                        continue
                    try:
                        receipt = json.loads(source_path.read_text(encoding="utf-8"))
                        dispatch = json.loads((source_path.parent / "dispatch.json").read_text(encoding="utf-8"))
                        manifest_raw = (source_path.parent / "manifest.json").read_bytes()
                        manifest = json.loads(manifest_raw)
                    except (OSError, ValueError, UnicodeError):
                        continue
                    termination = receipt.get("termination", receipt) if isinstance(receipt, dict) else {}
                    if (dispatch.get("intent_id") == attempt["intent_id"]
                            and dispatch.get("manifest", {}).get("sha256") == sha256(manifest_raw)
                            and manifest.get("intent_id") == attempt["intent_id"]
                            and manifest.get("role") == attempt["role"]
                            and manifest.get("request", {}).get("sha256") == attempt["request_digest"]
                            and termination.get("intent_id") == attempt["intent_id"]
                            and termination.get("spawn_state") == "not_started"
                            and termination.get("spawn_failure_origin") in ("cli_popen", "supervisor_popen")
                            and isinstance(termination.get("exception"), dict)
                            and termination["exception"].get("type") in ("OSError", "PermissionError", "FileNotFoundError", "NotADirectoryError", "IsADirectoryError")
                            and termination.get("returncode") is None
                            and not (source_path.parent / "process.json").exists()):
                        proven_not_started = True
            attempt.update(
                state="failed" if proven_not_started else "uncertain" if process_exit is None else "completed" if complete else "failed", process_exit=process_exit,
                response_complete=response_complete, artifacts_complete=artifacts_complete,
                technical_accepted=technical_accepted, evidence=refs,
                cli_sid=cli_sid or attempt["cli_sid"], ended_at=time.time() if process_exit is not None or proven_not_started else None,
                process_state="not_started" if proven_not_started else "unknown",
            )
            self._update_attempt(db, attempt, "attempt_finished")
            return attempt

    @staticmethod
    def _delivery_resolution_material(attempt: dict[str, Any], omitted: list[str]) -> dict:
        """Read a sealed failed delivery; never recollect, edit or run its role."""
        from taskflow.execution_runner import (
            RunnerError, _info, _read, _regular, _trace, _validate,
            _validated_collection,
        )
        from taskflow.packages import check_task

        if (omitted != ["handoff.md"] or attempt.get("state") != "failed"
                or attempt.get("role") not in ("author", "repair")
                or type(attempt.get("process_exit")) is not int or attempt["process_exit"] != 0
                or attempt.get("response_complete") is not True
                or attempt.get("artifacts_complete") is not False
                or attempt.get("technical_accepted") is not False):
            raise TaskflowError("Only a completed author turn missing its unrequested auxiliary handoff can be adopted")
        root = Path(_required_text(attempt.get("runtime_path"), "runtime_path"))
        try:
            manifest = _validate(root)
            collected = _validated_collection(root, manifest)
            termination = _read(root / "termination.json")
            trace = _trace(root / "trace.jsonl", attempt.get("cli_sid"))
            if (manifest.get("role") != attempt["role"]
                    or manifest.get("intent_id") != attempt["intent_id"]
                    or manifest.get("mode") not in ("native_role", "test_fixture")
                    or manifest["request"]["sha256"] != attempt["request_digest"]
                    or termination.get("intent_id") != attempt["intent_id"]
                    or termination.get("returncode") != 0
                    or termination.get("spawn_state") != "started"
                    or termination.get("timed_out") is not False
                    or termination.get("exception") is not None
                    or collected.get("termination") != termination
                    or collected.get("process_ended") is not True
                    or collected.get("model_completed") is not True
                    or collected.get("delivery_complete") is not False
                    or collected.get("technical_accepted") is not False
                    or collected.get("errors") != [] or not trace["model_completed"]
                    or not attempt.get("cli_sid") or trace["sid"] != attempt["cli_sid"]
                    or collected.get("sid") != trace["sid"]
                    or collected.get("final_response") != trace["final_response"]):
                raise TaskflowError("Original author turn, termination or sealed collection is incomplete")
            errors = [{"path": "artifacts/" + name,
                       "error": f"[Errno 2] No such file or directory: {name!r}"}
                      for name in omitted]
            if collected.get("artifact_errors") != errors:
                raise TaskflowError("Only the exact missing auxiliary output error can be resolved")
            outputs = manifest.get("outputs", [])
            artifacts = collected.get("artifacts", {})
            if ("repair-record.json" not in outputs or not manifest.get("output_trees")
                    or any(name not in outputs or name in artifacts
                           or (Path(manifest["cwd"]) / name).exists()
                           or (Path(manifest["cwd"]) / name).is_symlink() for name in omitted)
                    or any(name not in artifacts for name in outputs if name not in omitted)):
                raise TaskflowError("Cannot omit substantive outputs or absent candidate trees")
            input_lines = {f"Input ({item['kind']}): {root / 'inputs' / item['target']}"
                           for item in manifest["inputs"]}
            instructions = "\n".join(line for line in _regular(root / "request.md").decode("utf-8").splitlines()
                                     if line not in input_lines)
            if any(name.casefold() in instructions.casefold() for name in omitted):
                raise TaskflowError("An explicitly requested output cannot be omitted")
            declarations = [name for name in artifacts if name.endswith("/task.json")
                            and any(name.startswith(tree + "/") for tree in manifest["output_trees"])]
            if len(declarations) != 1:
                raise TaskflowError("One complete declared revised candidate is required")
            declaration_name = declarations[0]
            source = root / "artifacts" / Path(declaration_name).parent
            check = check_task(source, write_report=False)
            if check["errors"] or check["task_id"] != attempt["task_id"]:
                raise TaskflowError("Revised candidate is incomplete or belongs to another task")
            declaration = _read(source / "task.json")
            prefix = str(Path(declaration_name).parent) + "/"
            files = []
            for item in declaration["files"]:
                name = prefix + relative_path(item["path"])
                if name not in artifacts or _info(root / "artifacts" / name) != artifacts[name]:
                    raise TaskflowError("A declared revised candidate file was not actually collected")
                files.append({"package_path": item["path"], "path": str(source / item["path"]),
                              "role": item["role"], "sha256": artifacts[name]["sha256"]})
            record_path = root / "artifacts/repair-record.json"
            record = _read(record_path)
            if record.get("parent_id") != attempt["candidate_id"]:
                raise TaskflowError("Repair record does not name the actual author parent")
            child = {"candidate_id": validate_id(record.get("candidate_id"), "candidate_id"),
                     "parent_id": attempt["candidate_id"], "task_id": check["task_id"],
                     **{key: check[key] for key in ("input_digest", "content_digest", "evaluation_digest")},
                     "files": files}
            names = ["manifest.json", "manifest-lock.json", "request.md", "termination.json",
                     "trace.jsonl", "collection.json", "collection-lock.json", "collection-sources.json",
                     "artifacts/repair-record.json", "artifacts/" + declaration_name]
            refs = [evidence_ref(root / name, "original failed delivery resolution source") for name in names]
            return {"manifest": manifest, "collection": collected, "record_path": record_path,
                    "record": record, "candidate": child, "original_refs": refs}
        except (OSError, RunnerError, ValueError, UnicodeError, KeyError, TypeError) as error:
            raise TaskflowError(f"Cannot adopt failed delivery: {error}") from error

    @staticmethod
    def _validated_delivery_resolution(attempt: dict[str, Any]) -> dict:
        resolution = attempt.get("delivery_resolution")
        if not isinstance(resolution, dict) or resolution.get("status") != "accepted_auxiliary_omission":
            raise TaskflowError("Failed author needs a valid append-only delivery resolution")
        validate_evidence(resolution.get("evidence"))
        material = ExecutionStore._delivery_resolution_material(attempt, resolution.get("omitted_outputs"))
        binding = {key: material["candidate"][key] for key in
                   ("candidate_id", "parent_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        if (resolution.get("original_refs") != material["original_refs"]
                or resolution.get("candidate_binding") != binding
                or resolution.get("artifact_errors") != material["collection"]["artifact_errors"]):
            raise TaskflowError("Delivery resolution no longer matches its immutable originals")
        return material

    def accept_delivery_resolution(self, attempt_id: str, *, omitted_outputs: list[str],
                                   evidence: list[dict[str, Any]]) -> dict[str, Any]:
        """Append acceptance of an unrequested narrative omission, retaining failure."""
        from taskflow.execution_flow_contracts import validate_repair

        refs = validate_evidence(evidence)
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            material = self._delivery_resolution_material(attempt, omitted_outputs)
            if attempt.get("delivery_resolution") is not None:
                self._validated_delivery_resolution(attempt)
                if attempt["delivery_resolution"]["evidence"] != refs:
                    raise TaskflowError("Delivery resolution cannot replace its original acceptance evidence")
                return attempt
            parent = self._load(db, "candidates", attempt["candidate_id"])
            if material["manifest"].get("candidate_binding") != {
                    key: parent[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}:
                raise TaskflowError("Author delivery was not bound to the actual parent candidate")
            validation = validate_repair(material["record_path"], parent=parent,
                                         candidate=material["candidate"],
                                         defect_ids=material["record"].get("defect_ids"))
            attempt["delivery_resolution"] = {
                "schema_version": 1, "status": "accepted_auxiliary_omission", "at": time.time(),
                "omitted_outputs": omitted_outputs, "evidence": refs,
                "original_refs": material["original_refs"],
                "artifact_errors": material["collection"]["artifact_errors"],
                "candidate_binding": {key: material["candidate"][key] for key in
                                      ("candidate_id", "parent_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
                "changes": validation["changes"],
            }
            self._update_attempt(db, attempt, "delivery_resolution_accepted")
            return attempt

    @staticmethod
    def _rubric_declaration_transport(attempt: dict[str, Any], candidate: Path,
                                      declaration: dict, check: dict, file_refs: list[dict],
                                      specification: dict) -> dict:
        """Verify one explicit role-label correction, preserving every author byte."""
        from taskflow.contracts import parse_json
        from taskflow.execution_runner import _info, _regular
        from taskflow.packages import check_task

        if (not isinstance(specification, dict)
                or set(specification) != {"schema_version", "kind", "original", "projected", "evidence"}
                or type(specification.get("schema_version")) is not int
                or specification["schema_version"] != 1
                or specification.get("kind") != "referenced_rubric_role_v1"):
            raise TaskflowError("Explicit referenced_rubric_role_v1 declaration transport required")
        original, projected = validate_evidence([specification["original"], specification["projected"]])
        refs = validate_evidence(specification["evidence"])
        if original["path"] != str(candidate / "task.json") or original["sha256"] != file_refs[0]["sha256"]:
            raise TaskflowError("Declaration transport original must be this exact full-archive task.json")
        target = Path(projected["path"])
        if target.name != "task.json" or target == candidate / "task.json":
            raise TaskflowError("Declaration transport projected source must be a distinct task.json")
        transported = target.parent
        target_raw = _regular(target)
        if sha256(target_raw) != projected["sha256"]:
            raise TaskflowError("Declaration transport projected task.json changed")
        projected_declaration = parse_json(target_raw, "declaration transport task.json")
        source_files, target_files = declaration["files"], projected_declaration.get("files")
        reference = declaration.get("scoring", {}).get("rubric_file")
        matches = [(index, row) for index, row in enumerate(source_files) if row.get("id") == reference]
        if len(matches) != 1 or matches[0][1].get("role") != "teacher":
            raise TaskflowError("Declaration transport requires the existing referenced private teacher rubric")
        index, referenced = matches[0]
        if (not isinstance(target_files, list) or len(target_files) != len(source_files)
                or not isinstance(target_files[index], dict) or target_files[index].get("role") != "rubric"):
            raise TaskflowError("Declaration transport must correct only the referenced teacher role to rubric")
        normalized = {**projected_declaration, "files": [dict(row) for row in target_files]}
        normalized["files"][index]["role"] = "teacher"
        if canonical(normalized) != canonical(declaration):
            raise TaskflowError("Declaration transport may change only the referenced private rubric role")
        allowed_errors = ["scoring.rubric_file: must reference a rubric file ID"]
        if not any(row.get("role") == "rubric" for row in source_files):
            allowed_errors.insert(0, "files: missing rubric role")
        if check["task_id"] != attempt["task_id"] or check["errors"] != allowed_errors:
            raise TaskflowError("Original optional candidate has errors beyond the referenced rubric role")
        rubric_path = candidate / relative_path(referenced["path"])
        rubric = parse_json(_regular(rubric_path), "original referenced rubric JSON")
        scoring = declaration["scoring"]
        criteria, metadata_criteria = rubric.get("criteria"), scoring.get("criteria")
        if (type(rubric.get("schema_version")) is not int or rubric["schema_version"] != 1
                or canonical(rubric.get("total")) != canonical(scoring.get("total"))
                or not isinstance(criteria, list) or not criteria
                or not isinstance(metadata_criteria, list) or len(criteria) != len(metadata_criteria)):
            raise TaskflowError("Referenced rubric total and complete criteria must mirror task scoring")
        for row, metadata in zip(criteria, metadata_criteria):
            if not isinstance(row, dict) or not isinstance(metadata, dict):
                raise TaskflowError("Referenced rubric criteria must remain objects")
            mirrored = dict(metadata)
            if "evidence" not in row:
                mirrored.pop("evidence", None)
            if canonical(mirrored) != canonical(row):
                raise TaskflowError("Referenced rubric complete criteria differ from task scoring")
        target_check = check_task(transported, write_report=False)
        if target_check["errors"] or target_check["task_id"] != attempt["task_id"]:
            raise TaskflowError("Declaration transported candidate is incomplete or belongs to another task")
        target_info = {"sha256": sha256(target_raw), "size": len(target_raw)}
        target_refs = [{**projected, "size": len(target_raw), "package_path": "task.json", "role": "source_metadata"}]
        for source, target_row, source_ref in zip(source_files, target_files, file_refs[1:]):
            name = relative_path(source["path"])
            path = transported / name
            expected = {key: source_ref[key] for key in ("sha256", "size")}
            if _info(path) != expected:
                raise TaskflowError("Declaration transport payload bytes differ from the original full archive")
            ref = evidence_ref(path, "verified byte-identical declaration-transported candidate payload")
            if ref["sha256"] != expected["sha256"]:
                raise TaskflowError("Declaration transport changed during validation")
            target_refs.append({**ref, "size": expected["size"], "package_path": name, "role": target_row["role"]})
        # Re-read both sides and the explicit authority after constructing every
        # reference, so a partial or concurrent rewrite cannot acquire a seal.
        for ref in file_refs + target_refs:
            if _info(Path(ref["path"])) != {key: ref[key] for key in ("sha256", "size")}:
                raise TaskflowError("Declaration transport changed during validation")
        validate_evidence([original, projected, *refs])
        definition = {**specification, "original": original, "projected": projected, "evidence": refs}
        transport = {"schema_version": 1, "nature": "explicit_referenced_private_rubric_role_transport",
                     "definition": definition, "source_candidate_root": str(candidate), "source_files": file_refs,
                     "source_task": {key: file_refs[0][key] for key in ("path", "sha256", "size")},
                     "candidate_root": str(transported), "task": {"path": str(target), **target_info},
                     "change": {"json_pointer": f"/files/{index}/role", "file_id": reference,
                                "package_path": referenced["path"], "old": "teacher", "new": "rubric"},
                     "rubric_mirror": {"source": {key: file_refs[index + 1][key] for key in ("path", "sha256", "size")},
                                       "total_identical": True, "criteria_identical": True,
                                       "metadata_extra_fields": ["evidence"]}}
        return {"candidate": transported, "check": target_check, "file_refs": target_refs,
                "declaration_transport": transport}

    @staticmethod
    def _unsigned_report_layout_transport(attempt: dict[str, Any], candidate: Path,
                                          declaration: dict, check: dict, file_refs: list[dict],
                                          specification: dict) -> dict:
        """Separate declared unsigned originals from their derived review metadata."""
        from taskflow.contracts import PRIVATE_ROLES, parse_json, validate_empty_content
        from taskflow.execution_runner import _info, _regular
        from taskflow.packages import check_task

        if (not isinstance(specification, dict)
                or set(specification) != {"schema_version", "kind", "original", "projected", "evidence"}
                or type(specification.get("schema_version")) is not int
                or specification["schema_version"] != 1
                or specification.get("kind") != "declared_unsigned_report_layout_v1"):
            raise TaskflowError("Explicit declared_unsigned_report_layout_v1 transport required")
        original, projected = validate_evidence([specification["original"], specification["projected"]])
        refs = validate_evidence(specification["evidence"])
        if original["path"] != str(candidate / "task.json") or original["sha256"] != file_refs[0]["sha256"]:
            raise TaskflowError("Unsigned layout original must be this exact full-archive task.json")
        target = Path(projected["path"])
        if target.name != "task.json" or target == candidate / "task.json":
            raise TaskflowError("Unsigned layout projected source must be a distinct task.json")
        transported = target.parent
        raw = _regular(target)
        if sha256(raw) != projected["sha256"]:
            raise TaskflowError("Unsigned layout task.json changed")
        after = parse_json(raw, "unsigned layout task.json")
        source_files, target_files = declaration["files"], after.get("files")
        before_reviews, after_reviews = declaration.get("reviews"), after.get("reviews")
        if (not isinstance(target_files, list) or not all(isinstance(row, dict) for row in target_files)
                or len(source_files) != len(target_files) or not isinstance(before_reviews, list)
                or not isinstance(after_reviews, list) or len(before_reviews) != len(after_reviews)
                or not all(isinstance(row, dict) for row in after_reviews)):
            raise TaskflowError("Unsigned layout must preserve declaration membership and order")
        required = [(index, row) for index, row in enumerate(before_reviews) if row.get("required", True)]
        if (len(required) != 3 or {row.get("kind") for index, row in required} != {"business", "artifact", "calculation"}
                or len({row.get("id") for index, row in required}) != 3):
            raise TaskflowError("Unsigned layout requires the three unique required review identities")
        normalized = {**after, "files": [dict(row) for row in target_files],
                      "reviews": [dict(row) for row in after_reviews]}
        original_by_path = {row["path"]: (index, row) for index, row in enumerate(source_files)}
        if len(original_by_path) != len(source_files):
            raise TaskflowError("Unsigned layout source declarations must have unique paths")
        changes, report_rows, moved = [], [], set()
        wrapper_fields = {"schema_version", "review_id", "kind", "status", "task_id",
                          "independent_quality_passed", "new_formal_score", "authority", "local_checks", "limitations"}
        raw_fields = {"schema_version", "review_id", "task_id", "kind", "status", "role", "independent",
                      "author_checked", "summary", "reason", "evidence", "limitations",
                      "independent_quality_passed", "new_formal_score", "formal_score"}
        for review_index, review in required:
            old_name = relative_path(review["path"])
            member = original_by_path.get(old_name)
            if member is None or member[1].get("role") not in PRIVATE_ROLES:
                raise TaskflowError("Unsigned review original must be its declared private payload")
            index, source = member
            current_review, current_file = after_reviews[review_index], target_files[index]
            if current_review.get("id") != review["id"] or current_review.get("kind") != review["kind"]:
                raise TaskflowError("Unsigned layout cannot replace a review identity or kind")
            new_name, wrapper_name = relative_path(current_file.get("path")), relative_path(current_review.get("path"))
            if (index in moved or new_name == old_name
                    or not new_name.startswith("draft/evaluation/author-provenance/")
                    or Path(new_name).name != Path(old_name).name
                    or not wrapper_name.startswith("reports/") or Path(wrapper_name).name != Path(old_name).name
                    or wrapper_name in {row.get("path") for row in target_files}):
                raise TaskflowError("Unsigned layout may move only the corresponding private originals and review wrappers")
            moved.add(index)
            normalized["files"][index]["path"] = old_name
            normalized["reviews"][review_index]["path"] = old_name
            changes.extend([{"json_pointer": f"/files/{index}/path", "before": old_name, "after": new_name},
                            {"json_pointer": f"/reviews/{review_index}/path", "before": old_name, "after": wrapper_name}])
            original_raw = _regular(candidate / old_name)
            original_json = parse_json(original_raw, "original unsigned author report")
            if (not set(original_json).issubset(raw_fields)
                    or type(original_json.get("schema_version")) is not int or original_json["schema_version"] != 1
                    or original_json.get("kind") != review["kind"] or original_json.get("status") != "inconclusive"
                    or original_json.get("independent_quality_passed") is not False
                    or not any(key in original_json for key in ("formal_score", "new_formal_score"))
                    or any(original_json.get(key) is not None for key in ("formal_score", "new_formal_score"))
                    or ("review_id" in original_json and original_json["review_id"] != review["id"])
                    or ("task_id" in original_json and original_json["task_id"] != attempt["task_id"])
                    or ("role" in original_json and original_json["role"] != "informed_author")
                    or ("independent" in original_json and original_json["independent"] is not False)
                    or ("author_checked" in original_json and original_json["author_checked"] is not True)):
                raise TaskflowError("Unsigned layout requires explicit original author-only inconclusive reports")
            wrapper_raw = _regular(transported / wrapper_name)
            wrapper = parse_json(wrapper_raw, "derived unsigned review wrapper")
            if (set(wrapper) != wrapper_fields or type(wrapper.get("schema_version")) is not int
                    or wrapper["schema_version"] != 1 or wrapper.get("review_id") != review["id"]
                    or wrapper.get("kind") != review["kind"] or wrapper.get("task_id") != attempt["task_id"]
                    or wrapper.get("status") != "inconclusive" or wrapper.get("independent_quality_passed") is not False
                    or wrapper.get("new_formal_score") is not None
                    or not isinstance(wrapper.get("authority"), str) or not wrapper["authority"].strip()
                    or not isinstance(wrapper.get("limitations"), list) or not wrapper["limitations"]
                    or not all(isinstance(item, str) and item.strip() for item in wrapper["limitations"])):
                raise TaskflowError("Derived unsigned wrappers cannot claim a signature, score or independent pass")
            local = wrapper.get("local_checks")
            if (not isinstance(local, dict)
                    or set(local) != {"original_author_report", "original_relative_path", "original_sha256"}
                    or canonical(local.get("original_author_report")) != canonical(original_json)
                    or local.get("original_relative_path") != old_name
                    or local.get("original_sha256") != sha256(original_raw)):
                raise TaskflowError("Derived unsigned wrapper must preserve the complete original JSON, path and SHA")
            original_ref = file_refs[index + 1]
            relocated_ref = {**evidence_ref(transported / new_name, "byte-identical original unsigned author payload"),
                             "size": _info(transported / new_name)["size"]}
            wrapper_ref = {**evidence_ref(transported / wrapper_name, "derived unsigned review metadata; not raw author output"),
                           "size": len(wrapper_raw)}
            if wrapper_ref["sha256"] != sha256(wrapper_raw):
                raise TaskflowError("Unsigned wrapper changed during validation")
            report_rows.append({"review_id": review["id"], "kind": review["kind"],
                                "original": {key: original_ref[key] for key in ("path", "sha256", "size", "location")},
                                "relocated": relocated_ref, "wrapper": wrapper_ref,
                                "local_checks": {key: local[key] for key in ("original_relative_path", "original_sha256")},
                                "original_json_preserved": True})
        empty_rows = []
        for index, (source, current) in enumerate(zip(source_files, target_files)):
            if "empty_content" in current and "empty_content" not in source:
                source_ref = file_refs[index + 1]
                if (index in moved or source_ref["size"] != 0 or source_ref["sha256"] != sha256(b"")
                        or current.get("path") != source["path"] or validate_empty_content(current)):
                    raise TaskflowError("Unsigned layout empty_content requires an unchanged original private zero-record JSONL")
                normalized["files"][index].pop("empty_content")
                changes.append({"json_pointer": f"/files/{index}/empty_content", "before_present": False,
                                "before": None, "after": current["empty_content"]})
                empty_rows.append({"package_path": source["path"],
                                   "source": {key: source_ref[key] for key in ("path", "sha256", "size", "location")},
                                   "metadata": current["empty_content"]})
        if canonical(normalized) != canonical(declaration):
            raise TaskflowError("Unsigned layout may change only its exact report paths and typed zero-record metadata")
        if check["task_id"] != attempt["task_id"]:
            raise TaskflowError("Unsigned layout source belongs to another task")
        target_check = check_task(transported, write_report=False)
        if target_check["errors"] or target_check["task_id"] != attempt["task_id"]:
            raise TaskflowError("Unsigned layout candidate remains structurally incomplete: " + "; ".join(target_check["errors"]))
        target_refs = [{**projected, "size": len(raw), "package_path": "task.json", "role": "source_metadata"}]
        for source, current, source_ref in zip(source_files, target_files, file_refs[1:]):
            path = transported / relative_path(current["path"])
            expected = {key: source_ref[key] for key in ("sha256", "size")}
            if _info(path) != expected:
                raise TaskflowError("Unsigned layout payload bytes differ from the original full archive")
            ref = evidence_ref(path, "verified byte-identical unsigned-layout candidate payload")
            if ref["sha256"] != expected["sha256"]:
                raise TaskflowError("Unsigned layout payload changed during validation")
            target_refs.append({**ref, "size": expected["size"], "package_path": current["path"], "role": current["role"]})
        final_refs = [*file_refs, *target_refs,
                      *[row[key] for row in report_rows for key in ("relocated", "wrapper")]]
        for ref in final_refs:
            if _info(Path(ref["path"])) != {key: ref[key] for key in ("sha256", "size")}:
                raise TaskflowError("Unsigned layout changed during validation")
        validate_evidence([original, projected, *refs])
        transport = {"schema_version": 1, "nature": "explicit_declared_unsigned_report_layout_transport",
                     "definition": {**specification, "original": original, "projected": projected, "evidence": refs},
                     "source_candidate_root": str(candidate), "source_files": file_refs,
                     "source_task": {key: file_refs[0][key] for key in ("path", "sha256", "size")},
                     "candidate_root": str(transported), "task": {"path": str(target), "sha256": sha256(raw), "size": len(raw)},
                     "projection": {"declaration_changes": changes, "unsigned_reports": report_rows,
                                    "empty_content": empty_rows}}
        return {"candidate": transported, "check": target_check, "file_refs": target_refs,
                "declaration_transport": transport}

    @staticmethod
    def _optional_candidate_material(attempt: dict[str, Any], archive: Path,
                                     candidate_subpath: str, *,
                                     transport_candidate: Path | None = None,
                                     transport_evidence: list[dict[str, Any]] | None = None,
                                     declaration_transport: dict | None = None) -> dict:
        """Bind only a declared optional candidate to the unchanged terminal work."""
        from taskflow.execution_runner import (
            RunnerError, _full_source_roots, _full_tree, _full_verify, _info,
            _read, _regular, _trace, _validate, _validated_collection,
        )
        from taskflow.packages import check_task

        if declaration_transport is not None and (transport_candidate is not None or transport_evidence is not None):
            raise TaskflowError("Declaration transport cannot be combined with path-only transport")
        if (attempt.get("state") != "completed" or attempt.get("role") not in ("author", "repair")
                or type(attempt.get("process_exit")) is not int or attempt["process_exit"] != 0
                or any(attempt.get(key) is not True for key in
                       ("response_complete", "artifacts_complete", "technical_accepted"))):
            raise TaskflowError("Optional candidate requires the actual completed author/repair attempt")
        candidate_subpath = relative_path(candidate_subpath)
        if candidate_subpath != "next-task/" + attempt["task_id"]:
            raise TaskflowError("Optional candidate must use its exact next-task task path")
        root = Path(_required_text(attempt.get("runtime_path"), "runtime_path"))
        archive = Path(archive).absolute()
        try:
            manifest = _validate(root)
            collected = _validated_collection(root, manifest)
            termination = _read(root / "termination.json")
            trace = _trace(root / "trace.jsonl", attempt.get("cli_sid"))
            if (manifest.get("role") != attempt["role"]
                    or manifest.get("intent_id") != attempt["intent_id"]
                    or manifest.get("mode") not in ("native_role", "test_fixture")
                    or manifest["request"]["sha256"] != attempt["request_digest"]
                    or manifest.get("output_trees")
                    or not {"research.md", "candidate-decision.md", "handoff.md", "handoff.json", "issues.json"}.issubset(manifest["outputs"])
                    or collected.get("technical_accepted") is not True
                    or collected.get("delivery_complete") is not True
                    or collected.get("process_ended") is not True
                    or collected.get("errors") != [] or collected.get("artifact_errors") != []
                    or termination.get("intent_id") != attempt["intent_id"]
                    or termination.get("returncode") != 0 or termination.get("spawn_state") != "started"
                    or termination.get("timed_out") is not False or termination.get("exception") is not None
                    or collected.get("termination") != termination or not trace["model_completed"]
                    or not attempt.get("cli_sid") or trace["sid"] != attempt["cli_sid"]
                    or collected.get("sid") != trace["sid"]
                    or collected.get("final_response") != trace["final_response"]):
                raise TaskflowError("Optional candidate lacks its original complete turn and core collection")
            handoff = _read(root / "artifacts/handoff.json")
            if (type(handoff.get("schema_version")) is not int or handoff["schema_version"] != 1
                    or handoff.get("task_id") != attempt["task_id"]
                    or handoff.get("disposition") != "candidate_complete"
                    or handoff.get("candidate_subpath") != candidate_subpath
                    or handoff.get("needs_business_decision") is not False
                    or handoff.get("independent_quality_passed") is not False
                    or "new_formal_score" not in handoff or handoff["new_formal_score"] is not None
                    or not isinstance(handoff.get("source_evidence"), (str, list, dict))
                    or not handoff["source_evidence"]):
                raise TaskflowError("Collected handoff does not declare an independently unsigned complete candidate")
            _required_text(handoff.get("reason"), "candidate handoff reason")
            binding = _read(archive / "archive-binding.json")
            tree = _full_tree(archive)
            actual_files = {name: {key: row[key] for key in ("sha256", "size")}
                            for name, row in tree["files"].items() if name != "archive-binding.json"}
            directories = sorted(name.rstrip("/") for name in tree["directories"] if name != ".")
            if (tree["special"] or binding.get("intent_id") != attempt["intent_id"]
                    or binding.get("files") != actual_files or binding.get("directories") != directories
                    or binding.get("collection") != _info(root / "collection.json")
                    or _info(archive / "collection.json") != _info(root / "collection.json")
                    or _info(archive / "manifest.json") != _info(root / "manifest.json")):
                raise TaskflowError("Full archive does not contain this exact original attempt and collection")
            _validated_collection(archive, manifest)
            intent = _read(archive / "supplemental-intent.json")
            sources, aliases = _full_source_roots(manifest)
            if (intent.get("intent_id") != attempt["intent_id"]
                    or intent.get("manifest") != _info(root / "manifest.json")
                    or intent.get("collection") != _info(root / "collection.json")
                    or intent.get("sources") != {name: str(path) for name, path in sources.items()}
                    or intent.get("aliases") != aliases
                    or _info(root / "supplemental-intent.json") != _info(archive / "supplemental-intent.json")
                    or intent.get("snapshot") != {name: _full_tree(path) for name, path in sources.items()}):
                raise TaskflowError("Original terminal work or its full-archive source binding changed")
            receipt = _full_verify(archive / "supplemental", intent, True)
            alias = aliases["work"]
            work = archive / "supplemental/trees" / alias["source"]
            if alias["relative"] != ".":
                work /= relative_path(alias["relative"])
            candidate = work / candidate_subpath
            declaration_raw = _regular(candidate / "task.json")
            declaration = json.loads(declaration_raw)
            if not isinstance(declaration, dict):
                raise TaskflowError("Optional candidate task.json must be an object")
            check = check_task(candidate, write_report=False)
            if (transport_candidate is None and declaration_transport is None
                    and (check["errors"] or check["task_id"] != attempt["task_id"])):
                raise TaskflowError("Optional candidate is incomplete or belongs to another task")
            members = [("task.json", "source_metadata"),
                       *[(row["path"], row["role"]) for row in declaration["files"]]]
            file_refs = []
            for name, role in members:
                path = candidate / relative_path(name)
                payload = str(path.relative_to(archive / "supplemental"))
                expected = receipt["files"].get(payload)
                if expected != _info(path):
                    raise TaskflowError("Optional candidate member is not bound to the full-archive receipt")
                ref = evidence_ref(path, "actual declared optional candidate delivery")
                if ref["sha256"] != expected["sha256"] or _info(path) != expected:
                    raise TaskflowError("Original optional candidate member changed during validation")
                file_refs.append({**ref, "size": expected["size"], "package_path": name, "role": role})
            if {"sha256": sha256(declaration_raw), "size": len(declaration_raw)} != {
                    key: file_refs[0][key] for key in ("sha256", "size")}:
                raise TaskflowError("Original optional candidate declaration changed during validation")
            transport, declaration_material = None, None
            if declaration_transport is not None:
                validator = (ExecutionStore._unsigned_report_layout_transport
                             if isinstance(declaration_transport, dict)
                             and declaration_transport.get("kind") == "declared_unsigned_report_layout_v1"
                             else ExecutionStore._rubric_declaration_transport)
                declaration_material = validator(
                    attempt, candidate, declaration, check, file_refs, declaration_transport)
                candidate, check, file_refs = (declaration_material[key] for key in ("candidate", "check", "file_refs"))
            elif transport_candidate is not None:
                refs = validate_evidence(transport_evidence)
                transported = Path(transport_candidate).absolute()
                target_raw = _regular(transported / "task.json")
                transported_declaration = json.loads(target_raw)
                if not isinstance(transported_declaration, dict):
                    raise TaskflowError("Path transport task.json must remain an object")
                source_files = declaration["files"]
                target_files = transported_declaration.get("files")
                if not isinstance(target_files, list) or len(target_files) != len(source_files):
                    raise TaskflowError("Path transport must preserve every declaration in its original order")
                normalized = {**transported_declaration, "files": []}
                mapping, allowed_errors, target_refs = [], [], []
                target_task = {"sha256": sha256(target_raw), "size": len(target_raw)}
                target_refs.append({**evidence_ref(transported / "task.json", "verified path-only transported task metadata"),
                                    "size": target_task["size"], "package_path": "task.json", "role": "source_metadata"})
                for index, (source, target) in enumerate(zip(source_files, target_files)):
                    if not isinstance(target, dict):
                        raise TaskflowError("Path transport declarations must remain objects")
                    source_name = relative_path(source["path"])
                    target_name = relative_path(target.get("path"))
                    normalized["files"].append({**target, "path": source_name})
                    if source_name != target_name:
                        if (source.get("role") != "teacher" or not source_name.startswith("record/")
                                or not target_name.startswith(("draft/", "checks/"))):
                            raise TaskflowError("Path transport may move only teacher record/ declarations to draft/ or checks/")
                        allowed_errors.append(f"files[{index}].path: use draft/, checks/ or record.md")
                        mapping.append({"id": source["id"], "role": source["role"],
                                        "source_path": source_name, "package_path": target_name,
                                        **_info(candidate / source_name)})
                    expected = _info(candidate / source_name)
                    if _info(transported / target_name) != expected:
                        raise TaskflowError("Path transport payload bytes differ from the original full archive")
                    ref = evidence_ref(transported / target_name, "verified byte-identical transported candidate payload")
                    if ref["sha256"] != expected["sha256"]:
                        raise TaskflowError("Path transport changed during validation")
                    target_refs.append({**ref, "size": expected["size"],
                                        "package_path": target_name, "role": source["role"]})
                if not mapping or canonical(normalized) != canonical(declaration):
                    raise TaskflowError("Path transport may change only files[].path of the permitted declarations")
                if check["task_id"] != attempt["task_id"] or check["errors"] != allowed_errors:
                    raise TaskflowError("Original optional candidate has errors beyond the explicitly transported record/ paths")
                transported_check = check_task(transported, write_report=False)
                if transported_check["errors"] or transported_check["task_id"] != attempt["task_id"]:
                    raise TaskflowError("Transported optional candidate is incomplete or belongs to another task")
                if target_refs[0]["sha256"] != target_task["sha256"]:
                    raise TaskflowError("Path transport declaration changed during validation")
                for ref in file_refs + target_refs:
                    if _info(Path(ref["path"])) != {key: ref[key] for key in ("sha256", "size")}:
                        raise TaskflowError("Path transport changed during validation")
                transport = {"schema_version": 1, "nature": "explicit_path_only_transport",
                             "source_candidate_root": str(candidate), "source_files": file_refs,
                             "source_task": {"path": str(candidate / "task.json"), **_info(candidate / "task.json")},
                             "candidate_root": str(transported),
                             "task": {"path": str(transported / "task.json"), **target_task},
                             "mapping": mapping, "evidence": refs}
                candidate, check, file_refs = transported, transported_check, target_refs
            elif transport_evidence is not None:
                raise TaskflowError("Path transport evidence requires an explicit transport candidate")
            elif check["errors"] or check["task_id"] != attempt["task_id"]:
                raise TaskflowError("Optional candidate is incomplete or belongs to another task")
            names = ["manifest.json", "manifest-lock.json", "request.md", "termination.json", "trace.jsonl",
                     "collection.json", "collection-lock.json", "artifacts/handoff.json", "supplemental-intent.json"]
            original_refs = [evidence_ref(root / name, "original optional candidate author source") for name in names]
            original_refs += [evidence_ref(archive / name, "sealed full-archive candidate source") for name in
                              ("archive-binding.json", "supplemental-intent.json", "supplemental/manifest.json", "supplemental/receipt.json")]
            material = {"archive": str(archive), "candidate_subpath": candidate_subpath,
                    "candidate_root": str(candidate), "snapshot_digest": receipt["snapshot_digest"],
                    "candidate_binding": {key: check[key] for key in
                                          ("task_id", "input_digest", "content_digest", "evaluation_digest")},
                    "file_refs": file_refs, "original_refs": original_refs}
            if transport is not None:
                material["transport"] = transport
            if declaration_material is not None:
                material["declaration_transport"] = declaration_material["declaration_transport"]
            return material
        except (OSError, RunnerError, ValueError, UnicodeError, KeyError, TypeError) as error:
            raise TaskflowError(f"Cannot adopt optional candidate delivery: {error}") from error

    @staticmethod
    def _validated_optional_candidate_delivery(attempt: dict[str, Any]) -> dict:
        delivery = attempt.get("supplemental_delivery")
        if not isinstance(delivery, dict) or delivery.get("status") != "accepted_optional_candidate":
            raise TaskflowError("Optional candidate has no valid supplemental delivery")
        validate_evidence(delivery.get("evidence"))
        transport = delivery.get("transport")
        if transport is not None and not isinstance(transport, dict):
            raise TaskflowError("Optional candidate transport metadata must be an object")
        declaration_transport = delivery.get("declaration_transport")
        if declaration_transport is not None and not isinstance(declaration_transport, dict):
            raise TaskflowError("Optional candidate declaration transport metadata must be an object")
        material = ExecutionStore._optional_candidate_material(
            attempt, Path(delivery["archive"]), delivery["candidate_subpath"],
            transport_candidate=Path(transport["candidate_root"]) if transport else None,
            transport_evidence=transport["evidence"] if transport else None,
            declaration_transport=declaration_transport["definition"] if declaration_transport else None)
        if any(delivery.get(key) != value for key, value in material.items()):
            raise TaskflowError("Optional candidate delivery differs from its immutable originals")
        return material

    def record_optional_candidate_delivery(self, attempt_id: str, *, archive: Path,
                                          candidate_subpath: str,
                                          evidence: list[dict[str, Any]],
                                          transport_candidate: Path | None = None,
                                          transport_evidence: list[dict[str, Any]] | None = None,
                                          declaration_transport: dict | None = None) -> dict[str, Any]:
        """Append only the complete candidate actually declared in collected handoff."""
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt.get("supplemental_delivery") is not None:
                self._validated_optional_candidate_delivery(attempt)
                locked_transport = attempt["supplemental_delivery"].get("transport")
                locked_declaration = attempt["supplemental_delivery"].get("declaration_transport")
                if ((locked_transport and declaration_transport is not None)
                        or (locked_declaration and (transport_candidate is not None or transport_evidence is not None))):
                    raise TaskflowError("Optional candidate delivery cannot replace its original transport kind")
                if locked_declaration and declaration_transport is None:
                    declaration_transport = locked_declaration["definition"]
                if locked_transport and transport_candidate is None:
                    transport_candidate = Path(locked_transport["candidate_root"])
                    if transport_evidence is None:
                        transport_evidence = locked_transport["evidence"]
            material = self._optional_candidate_material(
                attempt, archive, candidate_subpath, transport_candidate=transport_candidate,
                transport_evidence=transport_evidence, declaration_transport=declaration_transport)
            if attempt.get("supplemental_delivery") is not None:
                if any(attempt["supplemental_delivery"].get(key) != value for key, value in material.items()) or attempt["supplemental_delivery"]["evidence"] != refs:
                    raise TaskflowError("Optional candidate delivery cannot replace its original source/evidence")
                return attempt
            parent = self._load(db, "candidates", attempt["candidate_id"])
            from taskflow.execution_runner import _read

            if _read(Path(attempt["runtime_path"]) / "manifest.json").get("candidate_binding") != {
                    key: parent[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}:
                raise TaskflowError("Optional candidate author did not receive the actual registered baseline")
            attempt["supplemental_delivery"] = {"schema_version": 1, "status": "accepted_optional_candidate",
                                                 "at": time.time(), "evidence": refs, **material}
            self._update_attempt(db, attempt, "optional_candidate_delivery_recorded")
            return attempt

    def mark_uncertain(self, attempt_id: str, reason: str) -> dict[str, Any]:
        _required_text(reason, "reason")
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] not in ACTIVE:
                raise TaskflowError("terminal attempt cannot be made uncertain")
            attempt.update(state="uncertain", uncertainty=reason)
            self._update_attempt(db, attempt, "uncertain")
            return attempt

    def reconcile(self, attempt_id: str, observation: dict[str, Any]) -> dict[str, Any]:
        """Persist observed recovery evidence; never infer permission to relaunch."""
        refs = validate_evidence(observation.get("evidence"))
        with self._db(True) as db:
            attempt = self._load(db, "attempts", attempt_id)
            if attempt["state"] not in ACTIVE:
                raise TaskflowError("terminal attempt requires no recovery")
            state = observation.get("process_state")
            if state not in ("alive", "dead", "unknown", "not_started"):
                raise TaskflowError("process_state: alive/dead/unknown/not_started required")
            if state == "alive" and (not attempt["process_identity"] or observation.get("process_identity") != attempt["process_identity"]):
                raise TaskflowError("live process startup identity mismatch")
            attempt["recovery"] = {**observation, "evidence": refs}
            attempt["state"] = "running" if state == "alive" else "uncertain"
            self._update_attempt(db, attempt, "reconciled")
            return attempt

    def record_review(self, review_id: str, candidate_id: str, attempt_id: str, *, verdict: str, scope: str, evidence: list[dict[str, Any]], whole_package: bool = False, covered_defect_ids: list[str] | None = None) -> dict[str, Any]:
        validate_id(review_id, "review_id")
        if verdict not in ("accepted", "defects", "inconclusive"):
            raise TaskflowError("review verdict: accepted/defects/inconclusive required")
        _required_text(scope, "scope")
        if not isinstance(whole_package, bool):
            raise TaskflowError("whole_package: boolean required")
        covered = [] if covered_defect_ids is None else covered_defect_ids
        if not isinstance(covered, list) or any(not isinstance(identity, str) for identity in covered) or len(covered) != len(set(covered)):
            raise TaskflowError("covered_defect_ids: unique ID list required")
        for identity in covered:
            validate_id(identity, "covered defect ID")
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            candidate = self._load(db, "candidates", candidate_id)
            attempt = self._load(db, "attempts", attempt_id)
            if any(_data(row)["attempt_id"] == attempt_id for row in db.execute("SELECT data FROM reviews")):
                raise TaskflowError("quality attempt already signed a review; cannot restamp an old verdict")
            if attempt["state"] != "completed" or attempt["role"] != "quality" or attempt["candidate_id"] != candidate_id:
                raise TaskflowError("review requires completed quality attempt on this exact candidate")
            for ancestor_id in self._quality_scope(db, candidate_id):
                ancestor = self._load(db, "candidates", ancestor_id)
                if attempt["cli_sid"] in ancestor.get("external_author_sids", []):
                    raise TaskflowError("external author context cannot independently review its candidate")
                author = ancestor.get("author_attempt")
                if author:
                    author_row = self._load(db, "attempts", author)
                    if attempt_id == author or (attempt["cli_sid"] and attempt["cli_sid"] == author_row["cli_sid"]):
                        raise TaskflowError("author context cannot independently review its revision")
            if not attempt["cli_sid"]:
                raise TaskflowError("independent quality context SID required")
            candidate_ancestors = self._ancestors(db, candidate_id)
            for identity in covered:
                defect = self._load(db, "defects", identity)
                if defect["state"] != "repaired" or defect.get("repair_candidate_id") not in candidate_ancestors:
                    raise TaskflowError("covered defect must identify this actual repaired candidate or its ancestor")
                if attempt["claimed_seq"] <= defect["repair_seq"]:
                    raise TaskflowError("covered recheck must start after the recorded actual repair")
            value = {
                "review_id": review_id, "candidate_id": candidate_id, "attempt_id": attempt_id,
                "input_digest": candidate["input_digest"], "content_digest": candidate["content_digest"],
                "verdict": verdict, "scope": scope, "whole_package": whole_package,
                "evidence": refs,
                "completed_seq": attempt["last_seq"],
                "covered_defect_ids": covered,
            }
            value["created_seq"] = self._event(db, "review_recorded", value)
            db.execute("INSERT INTO reviews VALUES(?,?,?)", (review_id, candidate_id, canonical(value).decode()))
            return value

    def import_external_review(
        self, review_id: str, candidate_id: str, *, snapshot_directory: str,
        report: dict[str, Any], trace: dict[str, Any],
        runtime_attempt: dict[str, Any], runtime_spec: dict[str, Any],
        archive_index: dict[str, Any], policy_id: str,
        supporting_evidence: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Adopt an existing independently signed role report without rerunning it.

        This migration adapter reads the existing adoption-readiness protocol.
        It preserves affected-scope/reused evidence and accepts only the exact
        already-frozen package. It creates no managed attempt or new judgment.
        """
        from taskflow.packages import verify_snapshot

        validate_id(review_id, "review_id")
        refs = validate_evidence([report, trace, runtime_attempt, runtime_spec, archive_index] + (supporting_evidence or []))
        report_ref, trace_ref, attempt_ref, spec_ref, index_ref = refs[:5]
        source = json.loads(Path(report_ref["path"]).read_text(encoding="utf-8"))
        attempt = json.loads(Path(attempt_ref["path"]).read_text(encoding="utf-8"))
        spec = json.loads(Path(spec_ref["path"]).read_text(encoding="utf-8"))
        index = json.loads(Path(index_ref["path"]).read_text(encoding="utf-8"))
        if any(not isinstance(value, dict) for value in (source, attempt, spec, index)):
            raise TaskflowError("external review sources must be JSON objects")
        signer = source.get("signer", {})
        sid = signer.get("sid")
        _external_authors([sid], [trace_ref])
        if (source.get("quality_pass_within_declared_offline_scope") is not True
                or source.get("blocking_issue_ids") != []
                or source.get("requires_author_repair") is not False
                or not (signer.get("independent_of_current_author_repair") is True
                        or signer.get("independent_of_current_authoring") is True)):
            raise TaskflowError("external report does not independently accept the declared package scope")
        scope = _required_text(source.get("quality_scope"), "external quality_scope")
        limitations = source.get("source_scope_limits")
        if not isinstance(limitations, list):
            raise TaskflowError("external scope limitations must be preserved as a list")
        if (attempt.get("status") != "process_exited" or attempt.get("returncode") != 0
                or attempt.get("exceptions") != [] or attempt.get("nature") == "test_fixture"
                or spec.get("mode") == "test_fixture" or spec.get("nature") == "test_fixture"
                or attempt.get("nature") != spec.get("role")):
            raise TaskflowError("external quality requires its original completed role runtime")
        started = _iso_time(attempt.get("started_at"), "external started_at")
        ended = _iso_time(attempt.get("ended_at"), "external ended_at")
        signed = _iso_time(source.get("signed_at_utc"), "external signed_at_utc")
        archived = _iso_time(index.get("at"), "external archive time")
        if not started <= signed <= ended <= archived:
            raise TaskflowError("external quality signature/runtime/archive ordering is inconsistent")
        indexed = {(item.get("destination"), item.get("sha256")) for item in index.get("files", []) if isinstance(item, dict)}
        if index.get("source_readback_matched") is not True or any((item["path"], item["sha256"]) not in indexed for item in refs[:4]):
            raise TaskflowError("external archived report/trace/runtime source bytes do not match transport evidence")
        snapshot = Path(snapshot_directory).resolve()
        frozen = verify_snapshot(snapshot)
        frozen_at = _iso_time(frozen.get("created_at"), "snapshot created_at")
        if archived > frozen_at:
            raise TaskflowError("external source archive postdates the supplied frozen acceptance")
        fingerprint = sha256(canonical(sorted(
            [{key: item[key] for key in ("path", "size", "sha256")} for item in frozen["files"]],
            key=lambda item: item["path"],
        )))
        source_candidate = source.get("candidate", {})
        if (source_candidate.get("task_id") != frozen["task_id"]
                or source_candidate.get("task_json_sha256") != frozen["task_sha256"]
                or source_candidate.get("independent_declared_files_fingerprint") != fingerprint):
            raise TaskflowError("external report declaration fingerprint does not match the actual snapshot")
        authorities = []
        for entry in frozen["reviews"]:
            if entry.get("kind") == "business":
                business = json.loads(safe_read(snapshot, entry["path"]))
                authorities.append(business.get("signature_authority", {}))
        if not any(authority.get("sid") == sid and authority.get("sha256") == report_ref["sha256"] and _evidence_path(authority.get("path", "")) == Path(report_ref["path"]) for authority in authorities):
            raise TaskflowError("frozen business report does not bind this independent source signature")
        setting = self.get_policy(policy_id)["config"]["roles"]["quality"]
        reused_origins = self._validate_reused_quality_graph(
            report_ref, source, frozen, index, refs[5:], setting,
        )
        with self._db(True) as db:
            candidate = self._load(db, "candidates", candidate_id)
            if any(candidate[key] != frozen[key] or source_candidate.get(key) != frozen[key] for key in ("input_digest", "content_digest", "evaluation_digest")):
                raise TaskflowError("external quality is for different candidate bytes")
            if candidate["task_id"] != frozen["task_id"]:
                raise TaskflowError("external quality task mismatch")
            setting = self._load(db, "policies", policy_id)["config"]["roles"]["quality"]
            external_roles = setting.get("external_roles", [
                "quality", "independent_quality",
                "independent_affected_quality_recheck_not_author_not_solver",
            ])
            if not isinstance(external_roles, list) or spec.get("role") not in external_roles:
                raise TaskflowError("external runtime role is not an authorized quality role")
            argv = spec.get("argv", [])
            models = [argv[index + 1] for index, value in enumerate(argv[:-1]) if value in ("-m", "--model")]
            reasoning = [json.loads(value.split("=", 1)[1]) for value in argv if isinstance(value, str) and value.startswith("model_reasoning_effort=")]
            if (models != [setting["model"]] or reasoning != [setting["reasoning_effort"]]
                    or attempt.get("requested_model") != setting["model"]
                    or attempt.get("reasoning_effort") != setting["reasoning_effort"]):
                raise TaskflowError("external quality requested configuration differs from its versioned project policy")
            for identity in self._quality_scope(db, candidate_id):
                ancestor = self._load(db, "candidates", identity)
                authors = set(ancestor.get("external_author_sids", []))
                if ancestor.get("author_attempt"):
                    authors.add(self._load(db, "attempts", ancestor["author_attempt"])["cli_sid"])
                if sid in authors:
                    raise TaskflowError("external author context cannot sign independent quality")
            for row in db.execute("SELECT data FROM reviews"):
                old = _data(row)
                old_sid = old.get("external_cli_sid")
                if old.get("attempt_id"):
                    old_sid = self._load(db, "attempts", old["attempt_id"])["cli_sid"]
                if old_sid == sid:
                    raise TaskflowError("external quality context already has a registered judgment")
            value = {
                "review_id": review_id, "candidate_id": candidate_id, "attempt_id": None,
                "engine_attempt_id": None, "external_cli_sid": sid,
                "source_kind": "external_independent_quality", "verdict": "accepted",
                "scope": scope, "limitations": limitations, "whole_package": False,
                "snapshot_bound_acceptance": True, "snapshot_directory": str(snapshot),
                "snapshot_digest": frozen["snapshot_digest"],
                "input_digest": candidate["input_digest"], "content_digest": candidate["content_digest"],
                "evaluation_digest": candidate["evaluation_digest"],
                "source_signed_at": signed.isoformat(), "source_completed_at": ended.isoformat(),
                "source_report": report_ref, "supporting_evidence": refs[5:], "evidence": refs,
                "closed_issue_ids": source.get("closed_issue_ids", []),
                "technical_acceptance_conditions": source.get("technical_acceptance_conditions", []),
                "served_model": None, "served_revision": None,
                "runtime_role": spec["role"], "quality_role_policy": external_roles,
                "reused_origin_evidence": reused_origins,
            }
            value["created_seq"] = self._event(db, "external_review_imported", value)
            db.execute("INSERT INTO reviews VALUES(?,?,?)", (review_id, candidate_id, canonical(value).decode()))
            return value

    @staticmethod
    def _validate_reused_quality_graph(report_ref: dict[str, Any], source: dict[str, Any],
                                       target: dict[str, Any], archive: dict[str, Any],
                                       supporting: list[dict[str, Any]], setting: dict[str, Any]) -> list[dict[str, Any]]:
        """Resolve reused signatures to original role evidence, without signing business judgments."""
        from taskflow.packages import check_task

        seen_paths = {report_ref["path"]}
        seen_hashes = {report_ref["sha256"]}
        seen_sids = {source["signer"]["sid"]}
        origins = []

        def visit(parent_ref, parent, current, parent_archive, declared_sources, depth):
            if depth > 32:
                raise TaskflowError("reused quality source graph exceeds bounded traversal")
            expects_sources = parent.get("affected_independent_recheck_completed") is True and parent.get("verification", {}).get("unchanged_finite_groups_reused", 0)
            if expects_sources and not declared_sources:
                raise TaskflowError("affected-scope acceptance requires its actual reused quality sources")
            if not isinstance(declared_sources, list):
                raise TaskflowError("reused quality supporting_evidence must be a list")
            try:
                declared_refs = validate_evidence(declared_sources) if declared_sources else []
            except (OSError, TaskflowError) as exc:
                raise TaskflowError("reused quality graph references a missing earlier source") from exc
            for item in declared_refs:
                prior = json.loads(Path(item["path"]).read_text(encoding="utf-8"))
                if not isinstance(prior, dict):
                    raise TaskflowError("reused quality source must be a report object")
                signer = prior.get("signer", {})
                prior_sid = signer.get("sid")
                if item["path"] in seen_paths or item["sha256"] in seen_hashes or prior_sid in seen_sids:
                    raise TaskflowError("reused quality sources cannot cite themselves or cycle through the same path/hash/SID")
                seen_paths.add(item["path"])
                seen_hashes.add(item["sha256"])
                seen_sids.add(prior_sid)
                prior_signed = _iso_time(prior.get("signed_at_utc"), "reused source signed_at_utc")
                if prior_signed >= _iso_time(parent.get("signed_at_utc"), "parent signed_at_utc") or not (
                    signer.get("independent_of_current_author_repair") is True
                    or signer.get("independent_of_current_authoring") is True
                ):
                    raise TaskflowError("reused quality source must be an earlier independent signature")
                business = prior.get("business_quality")
                if (not isinstance(business, dict) or any(not isinstance(business.get(key), str) or not business[key].strip() for key in ("references", "scoring", "public_responsibility"))
                        or not isinstance(prior.get("source_scope_limits"), list)):
                    raise TaskflowError("reused source must carry structured business quality scope and limitations")
                candidate = prior.get("candidate", {})
                if candidate.get("input_digest") != current["input_digest"] or not set(prior.get("blocking_issue_ids", [])) <= set(parent.get("closed_issue_ids", [])):
                    raise TaskflowError("reused quality source has different public bytes or unresolved findings")
                root = Path(_required_text(candidate.get("root"), "reused candidate root")).resolve()
                checked = check_task(root, write_report=False)
                if checked["errors"] or any(checked.get(key) != candidate.get(key) for key in ("task_id", "input_digest", "content_digest", "evaluation_digest")):
                    raise TaskflowError("reused quality candidate bytes do not match its original signature")
                declaration = json.loads(safe_read(root, "task.json"))
                rows = []
                for entry in declaration["files"]:
                    raw = safe_read(root, entry["path"])
                    rows.append({"path": entry["path"], "size": len(raw), "sha256": sha256(raw), "role": entry["role"]})
                fingerprint = sha256(canonical(sorted([{key: row[key] for key in ("path", "size", "sha256")} for row in rows], key=lambda row: row["path"])))
                if fingerprint != candidate.get("independent_declared_files_fingerprint"):
                    raise TaskflowError("reused quality declaration fingerprint does not bind its actual reviewed scope")
                descriptor = {**checked, "files": rows, "task_sha256": sha256(safe_read(root, "task.json")), "root": str(root)}
                origin = item.get("origin")
                if origin is None:
                    archive_root = Path(item["path"]).parent.parent
                    try:
                        origin = {
                            "trace": evidence_ref(archive_root / "runtime/trace.jsonl", "original reused quality CLI trace"),
                            "runtime_attempt": evidence_ref(archive_root / "runtime/attempt.json", "original reused quality runtime"),
                            "runtime_spec": evidence_ref(archive_root / "runtime/spec.json", "original reused quality request"),
                            "archive_index": evidence_ref(archive_root / "full-archive-index.json", "original reused quality archive"),
                        }
                    except (OSError, TaskflowError) as exc:
                        raise TaskflowError("reused quality source lacks its original runtime/trace/archive evidence") from exc
                validated = ExecutionStore._validate_quality_origin(item, prior, origin, setting)
                if _iso_time(validated["completed_at"], "reused completed_at") >= _iso_time(parent["signed_at_utc"], "parent signed_at_utc"):
                    raise TaskflowError("reused quality role did not finish before the later signature")
                origins.append({"report": item, "runtime": validated, "scope": business, "limitations": prior["source_scope_limits"]})
                ExecutionStore._validate_private_reuse_mapping(parent_ref, parent, parent_archive, descriptor, current)
                visit(item, prior, descriptor, validated["archive"], prior.get("supporting_evidence", []), depth + 1)

        visit(report_ref, source, target, archive, supporting, 0)
        return origins

    @staticmethod
    def _validate_quality_origin(report_ref, report, origin, setting):
        if not isinstance(origin, dict) or any(key not in origin for key in ("trace", "runtime_attempt", "runtime_spec", "archive_index")):
            raise TaskflowError("reused quality source lacks its original role evidence")
        refs = validate_evidence([origin[key] for key in ("trace", "runtime_attempt", "runtime_spec", "archive_index")])
        trace_ref, attempt_ref, spec_ref, index_ref = refs
        sid = report["signer"]["sid"]
        _external_authors([sid], [trace_ref])
        attempt = json.loads(Path(attempt_ref["path"]).read_text(encoding="utf-8"))
        spec = json.loads(Path(spec_ref["path"]).read_text(encoding="utf-8"))
        archive = json.loads(Path(index_ref["path"]).read_text(encoding="utf-8"))
        allowed = setting.get("reusable_quality_roles", ["quality", "independent_quality", "independent_affected_quality_recheck_not_author_not_solver", "independent_teacher_informed_complete_quality_intake_not_solver"])
        if (not isinstance(allowed, list) or spec.get("role") not in allowed or attempt.get("nature") != spec.get("role")
                or attempt.get("status") != "process_exited" or attempt.get("returncode") != 0
                or attempt.get("exceptions") != []):
            raise TaskflowError("reused source did not originate in a completed business quality role")
        argv = spec.get("argv", [])
        models = [argv[index + 1] for index, value in enumerate(argv[:-1]) if value in ("-m", "--model")]
        reasoning = [json.loads(value.split("=", 1)[1]) for value in argv if isinstance(value, str) and value.startswith("model_reasoning_effort=")]
        if (models != [setting["model"]] or reasoning != [setting["reasoning_effort"]]
                or attempt.get("requested_model") != setting["model"] or attempt.get("reasoning_effort") != setting["reasoning_effort"]):
            raise TaskflowError("reused source requested configuration differs from quality policy")
        started = _iso_time(attempt.get("started_at"), "reused started_at")
        ended = _iso_time(attempt.get("ended_at"), "reused ended_at")
        signed = _iso_time(report.get("signed_at_utc"), "reused signed_at_utc")
        archived = _iso_time(archive.get("at"), "reused archive time")
        if not started <= signed <= ended <= archived:
            raise TaskflowError("reused quality runtime/signature/archive ordering differs")
        indexed = {(item.get("destination"), item.get("sha256")) for item in archive.get("files", []) if isinstance(item, dict)}
        if archive.get("source_readback_matched") is not True or any((item["path"], item["sha256"]) not in indexed for item in [report_ref, *refs[:3]]):
            raise TaskflowError("reused quality original role/report bytes lack archive binding")
        return {"completed_at": ended.isoformat(), "role": spec["role"], "sid": sid, "evidence": refs, "archive": archive}

    @staticmethod
    def _validate_private_reuse_mapping(report_ref, report, archive, prior, current):
        if prior["evaluation_digest"] == current["evaluation_digest"]:
            return
        old = {row["path"]: row for row in prior["files"]}
        new = {row["path"]: row for row in current["files"]}
        changed = {path for path in old.keys() | new.keys() if old.get(path, {}).get("sha256") != new.get(path, {}).get("sha256") and (old.get(path, new.get(path)).get("role") not in PUBLIC_ROLES)}
        if prior["task_sha256"] != current["task_sha256"]:
            changed.add("task.json")
        indexed = {(item.get("destination"), item.get("sha256")) for item in archive.get("files", []) if isinstance(item, dict)}
        expected_rows = sorted([{key: row[key] for key in ("path", "size", "sha256")} for row in current["files"]], key=lambda row: row["path"])
        for reference in report.get("evidence", []):
            if not isinstance(reference, str):
                continue
            try:
                path = Path(report_ref["path"]).parent / relative_path(reference)
                raw = safe_read(Path(report_ref["path"]).parent, reference)
                mapping = json.loads(raw)
            except (OSError, TaskflowError, ValueError, UnicodeError):
                continue
            if (str(path) , sha256(raw)) not in indexed or not isinstance(mapping, dict):
                continue
            if (mapping.get("reviewer_sid") != report["signer"]["sid"]
                    or any(mapping.get("reported_taskflow_digests", {}).get(key) != current[key] for key in ("input_digest", "content_digest", "evaluation_digest"))
                    or mapping.get("unlisted_candidate_changes") != []
                    or mapping.get("task_json", {}).get("sha256") != current["task_sha256"]):
                continue
            observed_rows = sorted([{key: row.get(key) for key in ("path", "size", "sha256")} for row in mapping.get("declared_files", [])], key=lambda row: row["path"])
            if observed_rows != expected_rows:
                continue
            baseline = {row["path"]: row for row in mapping.get("source_QA_design_baseline", [])}
            if any(baseline.get(str(Path(prior["root"]) / row["path"]), {}).get("sha256") != row["sha256"] for row in prior["files"]):
                continue
            if baseline.get(str(Path(prior["root"]) / "task.json"), {}).get("sha256") != prior["task_sha256"]:
                continue
            changes = mapping.get("author_change_details", [])
            if all(any((item.get("path") == name or item.get("path", "").endswith("/" + name)) and item.get("before_hash_matches") is True and item.get("after_hash_matches") is True for item in changes) for name in changed):
                return
        raise TaskflowError("different private standard requires the signed affected-scope before/after mapping, not only matching public bytes")

    def open_defect(self, defect_id: str, candidate_id: str, review_id: str, *, responsibility: str, evidence: list[dict[str, Any]], kind: str = "material") -> dict[str, Any]:
        validate_id(defect_id, "defect_id")
        _required_text(responsibility, "responsibility")
        if kind not in ("material", "reference", "rubric", "claim", "environment"):
            raise TaskflowError("unknown defect kind")
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            candidate = self._load(db, "candidates", candidate_id)
            review = self._load(db, "reviews", review_id)
            if review["candidate_id"] != candidate_id:
                raise TaskflowError("defect review candidate mismatch")
            value = {"defect_id": defect_id, "candidate_id": candidate_id, "task_id": candidate["task_id"], "review_id": review_id, "responsibility": responsibility, "kind": kind, "state": "open", "evidence": refs}
            value["opened_seq"] = self._event(db, "defect_opened", value)
            db.execute("INSERT INTO defects VALUES(?,?,?,?)", (defect_id, candidate["task_id"], "open", canonical(value).decode()))
            return value

    def record_repair(self, defect_id: str, candidate_id: str, *, evidence: list[dict[str, Any]], disposition: str = "material_repair", prior_decision_id: str | None = None) -> dict[str, Any]:
        refs = validate_evidence(evidence)
        if disposition not in ("material_repair", "scope_adjustment", "private_repair"):
            raise TaskflowError("unknown repair disposition")
        with self._db(True) as db:
            defect = self._load(db, "defects", defect_id)
            original = self._load(db, "candidates", defect["candidate_id"])
            candidate = self._load(db, "candidates", candidate_id)
            if defect["state"] != "open":
                raise TaskflowError("repair requires open defect")
            if defect["kind"] == "material" and disposition == "private_repair":
                raise TaskflowError("material responsibility needs public repair or explicit scope adjustment")
            if defect["kind"] in ("reference", "rubric") and disposition == "material_repair":
                raise TaskflowError("private-standard defect needs private repair or explicit scope adjustment")
            ancestors = self._ancestors(db, candidate_id)
            if original["candidate_id"] not in ancestors or candidate_id == original["candidate_id"]:
                raise TaskflowError("repair must create a descendant revision")
            if candidate["created_seq"] <= defect["opened_seq"]:
                raise TaskflowError("repair revision predates defect discovery")
            author_id = candidate.get("author_attempt")
            if not author_id:
                raise TaskflowError("defect repair requires a captured actual author attempt and its delivered bytes")
            author = self._load(db, "attempts", author_id)
            if author["claimed_seq"] <= defect["opened_seq"]:
                authorized_prior = False
                if prior_decision_id:
                    decision = self._load(db, "decisions", prior_decision_id)
                    defect_sources = {(item["path"], item["sha256"]) for item in defect["evidence"]}
                    decision_sources = {(item["path"], item["sha256"]) for item in decision["evidence"]}
                    authorized_prior = (
                        decision["action"] == "repair" and decision["authorized"] is True
                        and decision["candidate_id"] == author["candidate_id"]
                        and decision["created_seq"] < author["claimed_seq"]
                        and decision["rationale"] == author["purpose"]
                        and bool(defect_sources & decision_sources)
                    )
                if not authorized_prior:
                    raise TaskflowError("author attempt predates finding; explicit prior authorized repair purpose required")
            if author["last_seq"] <= defect["opened_seq"] and not prior_decision_id:
                raise TaskflowError("repair author completion predates finding")
            if disposition == "material_repair" and candidate["input_digest"] == original["input_digest"]:
                raise TaskflowError("report/template-only changes cannot repair material defect")
            if disposition == "private_repair" and candidate["evaluation_digest"] == original["evaluation_digest"]:
                raise TaskflowError("private repair requires changed reference/rubric bytes")
            if disposition == "scope_adjustment" and candidate["content_digest"] == original["content_digest"]:
                raise TaskflowError("scope adjustment requires an actual recorded revision")
            original_files = {item["package_path"]: item for item in original.get("files", [])}
            changed = [item for item in candidate.get("files", []) if original_files.get(item["package_path"], {}).get("sha256") != item["sha256"]]
            if disposition == "material_repair":
                changed = [item for item in changed if item["role"] in PUBLIC_ROLES]
            elif disposition == "private_repair":
                changed = [item for item in changed if item["role"] not in PUBLIC_ROLES]
            delivered = self._delivered_artifacts(author)
            if not changed or any(item["sha256"] not in delivered for item in changed):
                raise TaskflowError("changed candidate files must match this author's actually collected delivery SHA256")
            defect.update(state="repaired", repair_candidate_id=candidate_id, disposition=disposition, repair_evidence=refs)
            defect["author_attempt"] = author_id
            defect["changed_files"] = changed
            defect["prior_decision_id"] = prior_decision_id
            defect["repair_seq"] = self._event(db, "defect_repaired", defect)
            db.execute("UPDATE defects SET state='repaired',data=? WHERE id=?", (canonical(defect).decode(), defect_id))
            return defect

    @staticmethod
    def _delivered_artifacts(attempt: dict[str, Any]) -> set[str]:
        if attempt.get("state") == "failed":
            material = ExecutionStore._validated_delivery_resolution(attempt)
            return {item["sha256"] for item in material["collection"]["artifacts"].values()}
        delivered = set()
        for source in validate_evidence(attempt["evidence"]):
            if source.get("kind") == "delivery":
                delivered.add(source["sha256"])
            try:
                receipt = json.loads(Path(source["path"]).read_text(encoding="utf-8"))
            except (ValueError, UnicodeError):
                continue
            if (not isinstance(receipt, dict) or receipt.get("intent_id") != attempt["intent_id"]
                    or receipt.get("sid") != attempt["cli_sid"]
                    or receipt.get("technical_accepted") is not True):
                continue
            for name, info in receipt.get("artifacts", {}).items():
                name = relative_path(name)
                raw = safe_read(Path(source["path"]).parent, "artifacts/" + name)
                if sha256(raw) != info.get("sha256") or len(raw) != info.get("size"):
                    raise TaskflowError("author collected artifact changed after delivery")
                delivered.add(info["sha256"])
        if attempt.get("supplemental_delivery") is not None:
            material = ExecutionStore._validated_optional_candidate_delivery(attempt)
            delivered.update(item["sha256"] for item in material["file_refs"])
        return delivered

    def close_defect(self, defect_id: str, review_id: str, *, evidence: list[dict[str, Any]]) -> dict[str, Any]:
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            defect = self._load(db, "defects", defect_id)
            review = self._load(db, "reviews", review_id)
            if defect["state"] != "repaired":
                raise TaskflowError("closure requires independent review of actual repair revision")
            if review["candidate_id"] != defect["repair_candidate_id"] and (
                defect["repair_candidate_id"] not in self._ancestors(db, review["candidate_id"])
                or defect_id not in review.get("covered_defect_ids", [])
            ):
                raise TaskflowError("descendant closure requires explicit defect coverage and actual repair ancestry")
            if review["verdict"] != "accepted" or review["created_seq"] <= defect["repair_seq"]:
                raise TaskflowError("closure requires a subsequent accepted independent review")
            if not review["whole_package"]:
                original = self._load(db, "candidates", defect["candidate_id"])
                repaired = self._load(db, "candidates", review["candidate_id"])
                if (defect["kind"] == "material"
                        or defect_id not in review.get("covered_defect_ids", [])
                        or repaired["input_digest"] != original["input_digest"]):
                    raise TaskflowError("closure requires whole-package review or explicit covered same-public private recheck")
                attempt = self._load(db, "attempts", review["attempt_id"])
                if attempt["claimed_seq"] <= defect["repair_seq"]:
                    raise TaskflowError("private recheck predates recorded actual repair")
            self._assert_no_newer_negative(db, review, defect_id=defect_id)
            defect.update(state="closed", closure_review_id=review_id, closure_evidence=refs)
            defect["closed_seq"] = self._event(db, "defect_closed", defect)
            db.execute("UPDATE defects SET state='closed',data=? WHERE id=?", (canonical(defect).decode(), defect_id))
            return defect

    @staticmethod
    def _closure_coverage_chain_material(db: sqlite3.Connection, defect: dict,
                                         repair_review_id: str, final_review_id: str, *,
                                         current_acceptance: dict | None = None) -> dict:
        """Recheck a real whole repair judgment followed by private-only closure."""
        from taskflow.execution_flow_contracts import _files, validate_repair
        from taskflow.execution_runner import _info, _validate, _validated_collection
        from taskflow.packages import check_task

        anchor = ExecutionStore._load(db, "reviews", repair_review_id)
        final = ExecutionStore._load(db, "reviews", final_review_id)
        if (defect["state"] not in {"repaired", "closed"} or anchor.get("whole_package") is not True
                or anchor["verdict"] not in {"defects", "accepted"} or final.get("whole_package") is not False
                or final["verdict"] != "accepted" or defect["defect_id"] not in anchor.get("covered_defect_ids", [])
                or defect["defect_id"] not in final.get("covered_defect_ids", [])):
            raise TaskflowError("coverage chain requires exact whole-covered and accepted affected defect signatures")
        original = ExecutionStore._load(db, "candidates", defect["candidate_id"])
        original_review = ExecutionStore._load(db, "reviews", defect["review_id"])
        if original_review.get("whole_package") is not True or original_review["verdict"] != "defects":
            raise TaskflowError("coverage chain needs the original actual whole blocking finding")
        origin = ExecutionStore._composition_review(db, original_review, "whole_package")
        original_issues = [row for row in origin["parsed"]["defects"] if row["kind"] == defect["kind"]
                           and row["responsibility"] == defect["responsibility"] and row["evidence"] == defect["evidence"]]
        if len(original_issues) != 1:
            raise TaskflowError("coverage chain defect differs from its original signed blocking issue")
        whole = ExecutionStore._composition_review(db, anchor, "whole_package")
        fresh = ExecutionStore._composition_review(db, final, "affected")
        parent, candidate = whole["candidate"], fresh["candidate"]
        if (candidate.get("parent_id") != parent["candidate_id"] or parent["input_digest"] != candidate["input_digest"]
                or defect["repair_candidate_id"] != parent["candidate_id"]
                or parent.get("parent_id") != original["candidate_id"]
                or original["candidate_id"] not in ExecutionStore._ancestors(db, parent["candidate_id"])
                or (defect["kind"] != "material" and original["input_digest"] == candidate["input_digest"])):
            raise TaskflowError("coverage chain lacks the renewed whole judgment and direct same-public private continuation")
        if (len({origin["attempt"]["cli_sid"], whole["attempt"]["cli_sid"], fresh["attempt"]["cli_sid"]}) != 3
                or whole["attempt"]["claimed_seq"] <= defect["repair_seq"]
                or fresh["attempt"]["claimed_seq"] <= anchor["created_seq"]
                or original_issues[0]["id"] in whole["report"]["execution_routing"]["blocking_issue_ids"]):
            raise TaskflowError("coverage chain review identities or actual ordering differ")

        first_author = ExecutionStore._load(db, "attempts", parent["author_attempt"])
        first_defects = [_data(row) for row in db.execute("SELECT data FROM defects")
                         if _data(row).get("review_id") == defect["review_id"]]
        if (first_author["state"] != "completed" or first_author["role"] != "repair"
                or first_author["candidate_id"] != original["candidate_id"] or not first_defects
                or first_author["cli_sid"] in {origin["attempt"]["cli_sid"], whole["attempt"]["cli_sid"], fresh["attempt"]["cli_sid"]}
                or any(row.get("repair_candidate_id") != parent["candidate_id"]
                       or row.get("author_attempt") != first_author["attempt_id"]
                       or row.get("state") not in {"repaired", "closed"}
                       or row.get("disposition") not in {"material_repair", "scope_adjustment", "private_repair"}
                       or (row["kind"] == "material" and row.get("disposition") == "private_repair")
                       or (row["kind"] in {"reference", "rubric"} and row.get("disposition") == "material_repair")
                       or not original_review["created_seq"] < row["opened_seq"] < first_author["claimed_seq"]
                       or not first_author["last_seq"] < parent["created_seq"] < row["repair_seq"] < whole["attempt"]["claimed_seq"]
                       for row in first_defects)):
            raise TaskflowError("coverage chain original public repair lacks its exact managed author and ordering")
        first_root = Path(first_author["runtime_path"])
        first_manifest = _validate(first_root)
        first_collected = _validated_collection(first_root, first_manifest)
        first_record = first_root / "artifacts/repair-record.json"
        if (first_manifest.get("candidate_binding") != {key: original[key] for key in
                ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or first_manifest.get("request", {}).get("sha256") != first_author["request_digest"]
                or first_collected.get("sid") != first_author["cli_sid"]
                or first_collected.get("technical_accepted") is not True
                or first_collected.get("artifacts", {}).get("repair-record.json") != _info(first_record)):
            raise TaskflowError("coverage chain original repair is not its sealed actual delivery")
        validate_repair(first_record, parent=original, candidate=parent,
                        defect_ids=sorted(row["defect_id"] for row in first_defects))
        ExecutionStore._validate_revision_delivery(original, parent["files"], parent["evidence"], first_author)
        for row in first_defects:
            validate_evidence(row["evidence"])
            validate_evidence(row["repair_evidence"])
            if not any(ref["path"] == str(first_record) and ref["sha256"] == _info(first_record)["sha256"]
                       for ref in row["repair_evidence"]):
                raise TaskflowError("coverage chain original responsibility lacks its actual repair-record reference")
            if len([issue for issue in origin["parsed"]["defects"] if issue["kind"] == row["kind"]
                    and issue["responsibility"] == row["responsibility"] and issue["evidence"] == row["evidence"]]) != 1:
                raise TaskflowError("coverage chain original repair includes an unsigned responsibility")
        if len(first_defects) != len(origin["parsed"]["defects"]):
            raise TaskflowError("coverage chain original whole blocking findings remain unassigned")

        def citations(rows: Any, current: dict, *, public: bool = False) -> list[dict]:
            files = _files(current)
            if not isinstance(rows, list) or not rows:
                raise TaskflowError("coverage chain needs explicit current signed evidence")
            found_public = False
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("location"), str) or not row["location"].strip():
                    raise TaskflowError("coverage chain evidence needs an exact source location")
                path = relative_path(row.get("path"))
                if path not in files or row.get("sha256") != files[path]["sha256"] or _info(Path(files[path]["path"]))["sha256"] != row["sha256"]:
                    raise TaskflowError("coverage chain evidence is not the exact current candidate byte")
                found_public |= files[path]["role"] in PUBLIC_ROLES
            if public and not found_public:
                raise TaskflowError("material coverage chain requires current public evidence")
            return rows

        anchor_rows = whole["report"].get("rechecked_defects", [])
        if not isinstance(anchor_rows, list):
            raise TaskflowError("whole coverage chain retained judgments must be explicit rows")
        matched = [(i, row) for i, row in enumerate(anchor_rows) if isinstance(row, dict) and row.get("defect_id") == defect["defect_id"]]
        if len(matched) != 1:
            raise TaskflowError("whole coverage chain has no unique explicit retained defect judgment")
        anchor_index, anchor_row = matched[0]
        if any(not isinstance(anchor_row.get(key), str) or not anchor_row[key].strip() for key in ("finding", "actual_coverage", "limit")):
            raise TaskflowError("whole retained judgment needs its actual coverage and limits")
        citations(anchor_row.get("evidence"), parent, public=defect["kind"] == "material")
        detail = fresh["report"].get("defect_resolution", {})
        rows = detail.get("per_defect", []) if isinstance(detail, dict) else []
        matched = [(i, row) for i, row in enumerate(rows) if isinstance(row, dict) and row.get("defect_id") == defect["defect_id"]]
        if len(matched) != 1 or matched[0][1].get("disposition") != "resolved_retained":
            raise TaskflowError("affected coverage chain has no explicit resolved_retained judgment")
        final_index, final_row = matched[0]
        citations(final_row.get("evidence"), candidate, public=defect["kind"] == "material")
        before, after = _files(parent), _files(candidate)
        if set(before) != set(after) or any(before[key]["role"] != after[key]["role"] for key in before):
            raise TaskflowError("coverage chain must preserve the renewed whole file/role set")
        declarations = []
        for current in (original, parent, candidate):
            refs = [ref for ref in current["evidence"] if ref.get("location") == "explicit package declaration"]
            validate_evidence(refs)
            if len(refs) != 1:
                raise TaskflowError("coverage chain needs the actual captured declarations")
            actual = check_task(Path(refs[0]["path"]).parent, write_report=False)
            if actual["errors"] or any(actual[key] != current[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
                raise TaskflowError("coverage chain captured candidate has drifted")
            declarations.append(refs[0])
        if declarations[1]["sha256"] != declarations[2]["sha256"]:
            raise TaskflowError("coverage chain cannot inherit changed scoring/declaration metadata")
        changed = sorted(key for key in before if before[key]["sha256"] != after[key]["sha256"])
        if not changed or any(after[key]["role"] in PUBLIC_ROLES for key in changed):
            raise TaskflowError("coverage chain requires an actual private-only same-public continuation")
        author = ExecutionStore._load(db, "attempts", candidate["author_attempt"])
        if (author["state"] != "completed" or author["role"] != "repair" or author["candidate_id"] != parent["candidate_id"]
                or not anchor["created_seq"] < author["claimed_seq"] < author["last_seq"] < candidate["created_seq"] < fresh["attempt"]["claimed_seq"]
                or fresh["attempt"]["cli_sid"] == author["cli_sid"] or whole["attempt"]["cli_sid"] == author["cli_sid"]):
            raise TaskflowError("coverage chain private repair is not its actual ordered managed author")
        root = Path(author["runtime_path"])
        manifest = _validate(root)
        collected = _validated_collection(root, manifest)
        if (manifest.get("candidate_binding") != {key: parent[key] for key in
                ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or collected.get("sid") != author["cli_sid"] or collected.get("technical_accepted") is not True
                or "repair-record.json" not in collected["artifacts"]):
            raise TaskflowError("coverage chain private repair source is not its sealed actual delivery")
        record_path = root / "artifacts/repair-record.json"
        own = [_data(row) for row in db.execute("SELECT data FROM defects") if _data(row).get("review_id") == repair_review_id]
        own_ids = {row["defect_id"] for row in own}
        expected = own_ids | set(anchor["covered_defect_ids"])
        if (not own_ids or set(final["covered_defect_ids"]) != expected
                or any(row.get("repair_candidate_id") != candidate["candidate_id"] or row.get("author_attempt") != author["attempt_id"] for row in own)):
            raise TaskflowError("coverage chain final signature omits or adds renewed whole responsibilities")
        if len(own) != len(whole["parsed"]["defects"]) or any(len([
                issue for issue in whole["parsed"]["defects"] if issue["kind"] == row["kind"]
                and issue["responsibility"] == row["responsibility"] and issue["evidence"] == row["evidence"]]) != 1 for row in own):
            raise TaskflowError("coverage chain renewed whole responsibilities differ from actual blocking issues")
        validate_repair(record_path, parent=parent, candidate=candidate, defect_ids=sorted(own_ids))
        ExecutionStore._validate_revision_delivery(parent, candidate["files"], candidate["evidence"], author)
        coverage = fresh["report"].get("coverage", {})
        if (not isinstance(coverage, dict)
                or coverage.get("parent_binding") != {key: parent[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or coverage.get("current_binding") != {key: candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}):
            raise TaskflowError("coverage chain current/parent signed bindings differ")
        pairs, signed_evidence, paths = coverage.get("changed_files"), detail.get("evidence"), detail.get("affected_paths")
        if not isinstance(pairs, list) or not isinstance(paths, list) or not isinstance(signed_evidence, list):
            raise TaskflowError("coverage chain needs complete changed associations and current evidence")
        if {row.get("path") for row in pairs if isinstance(row, dict)} != set(changed) or len(pairs) != len(changed):
            raise TaskflowError("coverage chain changed pairs do not match all actual private differences")
        changes = []
        for path in changed:
            matches = [(i, row) for i, row in enumerate(pairs) if isinstance(row, dict) and row.get("path") == path
                       and row.get("old") == before[path]["sha256"] and row.get("new") == after[path]["sha256"]]
            ev = [(i, row) for i, row in enumerate(signed_evidence) if isinstance(row, dict) and row.get("path") == path
                  and row.get("sha256") == after[path]["sha256"]]
            if len(matches) != 1 or len(ev) != 1 or path not in paths:
                raise TaskflowError("coverage chain private change lacks exact fresh pair/evidence/necessary association")
            citations([ev[0][1]], candidate)
            changes.append({"path": path, "old_sha256": before[path]["sha256"], "new_sha256": after[path]["sha256"],
                            "pair_pointer": f"/coverage/changed_files/{matches[0][0]}",
                            "evidence_pointer": f"/defect_resolution/evidence/{ev[0][0]}"})
        ExecutionStore._assert_no_newer_negative(db, final, defect_id=defect["defect_id"],
                                                current_acceptance=current_acceptance)
        if defect["state"] == "closed" and (defect.get("closure_review_id") != final_review_id or defect.get("closed_seq", 0) <= final["created_seq"]):
            raise TaskflowError("coverage chain closed defect belongs to another result or ordering")
        return {"schema_version": 1, "kind": "whole_then_same_public_affected", "defect_id": defect["defect_id"],
                "original_review_id": defect["review_id"], "repair_review_id": repair_review_id, "final_review_id": final_review_id,
                "original_source": {"candidate_id": original["candidate_id"], "attempt_id": origin["attempt"]["attempt_id"],
                    "sid": origin["attempt"]["cli_sid"], "report": origin["report_ref"], "issues": origin["issues_ref"],
                    "issue_id": original_issues[0]["id"], "repair_author_attempt": first_author["attempt_id"],
                    "repair_record": evidence_ref(first_record, "actual coverage-chain original public repair record")},
                "whole_source": {"candidate_id": parent["candidate_id"], "attempt_id": whole["attempt"]["attempt_id"],
                    "sid": whole["attempt"]["cli_sid"], "report": whole["report_ref"], "issues": whole["issues_ref"],
                    "json_pointer": f"/rechecked_defects/{anchor_index}", "actual_value": anchor_row, "verdict": anchor["verdict"]},
                "affected_source": {"candidate_id": candidate["candidate_id"], "attempt_id": fresh["attempt"]["attempt_id"],
                    "sid": fresh["attempt"]["cli_sid"], "report": fresh["report_ref"], "issues": fresh["issues_ref"],
                    "json_pointer": f"/defect_resolution/per_defect/{final_index}", "actual_value": final_row, "whole_package": False},
                "parent_binding": coverage["parent_binding"], "current_binding": coverage["current_binding"],
                "repair_author_attempt": author["attempt_id"], "repair_record": evidence_ref(record_path, "actual coverage-chain private repair record"),
                "private_changes": changes, "source_limits": {"whole": anchor_row["limit"], "affected": final_row.get("limits", [])},
                "completed_seq": final["completed_seq"]}

    def close_defect_with_coverage_chain(self, defect_id: str, final_review_id: str, *,
                                         repair_review_id: str, evidence: list[dict[str, Any]]) -> dict:
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            defect = self._load(db, "defects", defect_id)
            material = self._closure_coverage_chain_material(db, defect, repair_review_id, final_review_id)
            if defect["state"] == "closed":
                if defect.get("closure_coverage_chain") != material or defect.get("closure_evidence") != refs:
                    raise TaskflowError("closed coverage chain cannot be replaced")
                return defect
            defect.update(state="closed", closure_review_id=final_review_id, closure_evidence=refs,
                          closure_coverage_chain=material)
            defect["closed_seq"] = self._event(db, "defect_closed_with_coverage_chain", defect)
            db.execute("UPDATE defects SET state='closed',data=? WHERE id=?", (canonical(defect).decode(), defect_id))
            return defect

    def validate_defect_coverage_chain(self, defect_id: str) -> dict:
        with self._db() as db:
            defect = self._load(db, "defects", defect_id)
            chain = defect.get("closure_coverage_chain")
            if defect["state"] != "closed" or not isinstance(chain, dict):
                raise TaskflowError("defect has no accepted typed coverage closure chain")
            material = self._closure_coverage_chain_material(db, defect, chain["repair_review_id"], chain["final_review_id"])
            if material != chain:
                raise TaskflowError("coverage closure chain no longer matches its original signed sources")
            return material

    @staticmethod
    def _ancestors(db: sqlite3.Connection, candidate_id: str) -> set[str]:
        result = set()
        current = candidate_id
        while current:
            if current in result:
                raise TaskflowError("candidate lineage cycle")
            result.add(current)
            current = ExecutionStore._load(db, "candidates", current)["parent_id"]
        return result

    @staticmethod
    def _quality_scope(db: sqlite3.Connection, candidate_id: str) -> set[str]:
        """Follow lineage and exact-byte aliases without reusing their verdicts."""
        candidate = ExecutionStore._load(db, "candidates", candidate_id)
        all_candidates = [_data(row) for row in db.execute("SELECT data FROM candidates WHERE task_id=?", (candidate["task_id"],))]
        scope = ExecutionStore._ancestors(db, candidate_id)
        while True:
            fingerprints = {
                tuple(ExecutionStore._load(db, "candidates", identity)[key] for key in ("input_digest", "content_digest", "evaluation_digest"))
                for identity in scope
            }
            related = set(scope)
            for item in all_candidates:
                if tuple(item[key] for key in ("input_digest", "content_digest", "evaluation_digest")) in fingerprints:
                    related.update(ExecutionStore._ancestors(db, item["candidate_id"]))
            if related == scope:
                return scope
            scope = related

    @staticmethod
    def _review_time(db: sqlite3.Connection, review: dict[str, Any]) -> float:
        if review.get("source_completed_at"):
            return _iso_time(review["source_completed_at"], "review source_completed_at").timestamp()
        if review.get("attempt_id"):
            attempt = ExecutionStore._load(db, "attempts", review["attempt_id"])
            for source in validate_evidence(attempt["evidence"]):
                path = Path(source["path"])
                if path.name not in ("collection.json", "result.json"):
                    continue
                try:
                    receipt = json.loads(path.read_text(encoding="utf-8"))
                except (ValueError, UnicodeError):
                    continue
                if (not isinstance(receipt, dict) or receipt.get("intent_id") != attempt["intent_id"]
                        or receipt.get("sid") != attempt["cli_sid"]
                        or not isinstance(receipt.get("termination"), dict)):
                    continue
                termination = receipt["termination"]
                if termination.get("returncode") == attempt["process_exit"] and termination.get("ended_at"):
                    return _iso_time(termination["ended_at"], "actual quality termination ended_at").timestamp()
            if isinstance(attempt.get("ended_at"), (int, float)):
                return float(attempt["ended_at"])
        event = db.execute("SELECT at FROM events WHERE seq=?", (review["created_seq"],)).fetchone()
        return float(event["at"])

    @staticmethod
    def _review_scopes_overlap(accepted: dict[str, Any], later: dict[str, Any]) -> bool:
        if (accepted.get("whole_package") or later.get("whole_package")
                or accepted.get("snapshot_bound_acceptance") or later.get("snapshot_bound_acceptance")):
            return True
        accepted_ids = set(accepted.get("covered_defect_ids", []))
        later_ids = set(later.get("covered_defect_ids", []))
        if accepted_ids and later_ids:
            return bool(accepted_ids & later_ids)
        # Flow scope IDs are assigned by the controller; no semantic keyword
        # inference is made from a model's descriptive business text.
        return accepted.get("scope") == later.get("scope")

    @staticmethod
    def _assert_no_newer_negative(db: sqlite3.Connection, review: dict[str, Any],
                                  defect_id: str | None = None, *,
                                  current_acceptance: dict | None = None) -> None:
        accepted_time = ExecutionStore._review_time(db, review)
        reviewed_scope = ExecutionStore._quality_scope(db, review["candidate_id"])
        for row in db.execute("SELECT data FROM reviews"):
            later = _data(row)
            if later["verdict"] == "accepted":
                continue
            if defect_id is None:
                overlaps = ExecutionStore._review_scopes_overlap(review, later)
            else:
                overlaps = later.get("whole_package") or defect_id in later.get("covered_defect_ids", [])
            if not overlaps or ExecutionStore._review_time(db, later) <= accepted_time:
                continue
            related = later["candidate_id"] in reviewed_scope or review["candidate_id"] in ExecutionStore._quality_scope(db, later["candidate_id"])
            if related:
                validate_evidence(later["evidence"])
                if current_acceptance is not None:
                    # Only archival alias validation supplies this context.
                    # Recheck the original signature as well as the exact graph
                    # that consumed this particular intermediate finding.
                    ExecutionStore._composition_review(db, review,
                        "whole_package" if review["whole_package"] else "affected")
                    ExecutionStore._assert_consumed_quality_negative(db, later, current_acceptance)
                    continue
                raise TaskflowError("later independent rejection/conflict covers the accepted scope; old acceptance is not current")

    @staticmethod
    def _review_acceptance_material(db: sqlite3.Connection, review_id: str) -> dict[str, Any]:
        """Validate acceptance identity and evidence separately from freshness."""
        review = ExecutionStore._load(db, "reviews", review_id)
        if review["verdict"] != "accepted":
            raise TaskflowError("current acceptance requires an accepted independent review")
        if review.get("composite_quality_coverage"):
            ExecutionStore._validate_repaired_quality(db, review)
        candidate = ExecutionStore._load(db, "candidates", review["candidate_id"])
        if any(review.get(key) != candidate[key] for key in ("input_digest", "content_digest")) or (
            "evaluation_digest" in review and review["evaluation_digest"] != candidate["evaluation_digest"]
        ):
            raise TaskflowError("accepted review does not bind the current candidate version")
        validate_evidence(review["evidence"])
        if review.get("attempt_id"):
            attempt = ExecutionStore._load(db, "attempts", review["attempt_id"])
            if (attempt["state"] != "completed" or attempt["role"] != "quality"
                    or attempt["candidate_id"] != review["candidate_id"]):
                raise TaskflowError("accepted review source attempt no longer matches its signed candidate")
            validate_evidence(attempt["evidence"])
        for origin in review.get("reused_origin_evidence", []):
            validate_evidence(origin["runtime"]["evidence"])
        return review

    @staticmethod
    def _assert_review_current(db: sqlite3.Connection, review_id: str) -> dict[str, Any]:
        review = ExecutionStore._review_acceptance_material(db, review_id)
        ExecutionStore._assert_no_newer_negative(db, review)
        return review

    def assert_review_current(self, review_id: str) -> dict[str, Any]:
        """Assert scoped acceptance freshness without claiming whole-package quality."""
        validate_id(review_id, "review_id")
        with self._db() as db:
            return self._assert_review_current(db, review_id)

    @staticmethod
    def _composition_observations(db: sqlite3.Connection, review: dict, candidate: dict,
                                  attempt: dict, manifest: dict, root: Path, issues: dict) -> dict:
        """Resolve cited observations through this signature's recorded public role."""
        from taskflow.execution_flow import _review_observations
        from taskflow.execution_runner import _full_verify, _info, _read, _regular, _validate, _validated_collection

        rows = issues.get("issues", [])
        rows = rows if isinstance(rows, list) else []
        used = {entry["path"] for row in rows if isinstance(row, dict) and isinstance(row.get("evidence"), list)
                for entry in row["evidence"] if isinstance(entry, dict) and isinstance(entry.get("path"), str)}
        used.update(path for row in rows if isinstance(row, dict) and isinstance(row.get("affected_paths"), list)
                    for path in row["affected_paths"] if isinstance(path, str))
        observed = [entry for entry in manifest["inputs"]
                    if entry["kind"] in {"submission", "trace"} and entry["target"] in used]
        if not observed:
            return {}
        matches = []
        for row in db.execute("SELECT flow_id,definition,state FROM execution_flows"):
            definition, state = json.loads(row["definition"]), json.loads(row["state"])
            for signed in state.get("history", []):
                if signed.get("kind") == "independent_review" and signed.get("review_id") == review["review_id"]:
                    matches.append((row["flow_id"], definition, state, signed))
        if len(matches) != 1:
            raise TaskflowError("Composition observations need one actual independent-review history")
        identity, definition, state, signed = matches[0]
        controls = [entry for entry in manifest["inputs"] if entry["target"] == "execution/current.json"]
        if len(controls) != 1 or controls[0]["kind"] != "source_metadata":
            raise TaskflowError("Composition observations need the actual locked workflow context")
        control = controls[0]
        staged = root / "inputs/execution/current.json"
        context = _read(staged)
        if (control["source"] != str(root.parent / "current.json")
                or _info(staged) != {key: control[key] for key in ("sha256", "size")}
                or _regular(staged) != _regular(Path(control["source"]))
                or context.get("flow_id") != identity or context.get("intent_id") != attempt["intent_id"]
                or any(context.get(key) != candidate[key] for key in
                       ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest"))):
            raise TaskflowError("Composition observation context has a foreign workflow, intent or candidate")
        expected_scope = "whole_package" if review["whole_package"] else "affected"
        scopes = {role.get("scope_id", identity + ".authorized-quality")
                  for name, role in definition.get("roles", {}).items()
                  if name in {"quality", "recheck", "recheck_public"} and isinstance(role, dict)
                  and role.get("scope") == expected_scope}
        if (any(signed.get(key) != value for key, value in {
                "attempt_id": attempt["attempt_id"], "cli_sid": attempt["cli_sid"],
                "candidate_id": candidate["candidate_id"]}.items())
                or definition.get("flow_id") != identity
                or definition.get("task_id") != candidate["task_id"]
                or definition.get("owner") != attempt["dispatch_owner"]
                or definition.get("policy_id") != attempt["policy_id"] or review["scope"] not in scopes):
            raise TaskflowError("Composition observation history has a foreign role, signer or candidate")
        archives = state.get("archives", [])
        containing = {name for name in archives if isinstance(name, str)
                      and all(Path(entry["source"]).is_relative_to(Path(name)) for entry in observed)}
        if len(containing) != 1:
            raise TaskflowError("Composition observations need one recorded original public archive")
        archive_name = next(iter(containing))
        public_history = [row for row in state["history"]
                          if row.get("kind") in {"adopted_predecessor", "public_reconstruction"}
                          and row.get("archive") == archive_name]
        if len(public_history) != 1:
            raise TaskflowError("Composition observation archive has no unique actual public role")
        predecessor = ExecutionStore._load(db, "attempts", public_history[0]["attempt_id"])
        if (predecessor.get("state") != "completed" or predecessor.get("role") != "quality"
                or not predecessor.get("cli_sid") or predecessor["cli_sid"] != public_history[0].get("cli_sid")
                or predecessor["cli_sid"] == attempt["cli_sid"]
                or predecessor["task_id"] != candidate["task_id"] or predecessor["input_digest"] != candidate["input_digest"]
                or predecessor["last_seq"] >= attempt["claimed_seq"]):
            raise TaskflowError("Composition observation predecessor is not its prior completed independent public role")
        archive, original_root = Path(archive_name), Path(predecessor["runtime_path"])
        public_manifest = _validate(original_root)
        original_collection = _validated_collection(original_root, public_manifest)
        archived_manifest = _read(archive / "manifest.json")
        archived_collection = _validated_collection(archive, archived_manifest)
        archive_binding = _read(archive / "archive-binding.json")
        intent = _read(archive / "supplemental-intent.json")
        if (_regular(archive / "manifest.json") != _regular(original_root / "manifest.json")
                or public_manifest.get("intent_id") != predecessor["intent_id"]
                or public_manifest.get("role") != "quality"
                or any(public_manifest.get("candidate_binding", {}).get(key) != candidate[key] for key in ("task_id", "input_digest"))
                or original_collection != archived_collection or original_collection.get("technical_accepted") is not True
                or original_collection.get("process_ended") is not True or original_collection.get("sid") != predecessor["cli_sid"]
                or archive_binding.get("intent_id") != predecessor["intent_id"]
                or archive_binding.get("collection") != _info(original_root / "collection.json")
                or any(intent.get(key) != expected for key, expected in {
                    "intent_id": predecessor["intent_id"], "manifest": _info(original_root / "manifest.json"),
                    "collection": _info(original_root / "collection.json")}.items())
                or any(archive_binding.get("files", {}).get(name) != _info(archive / name)
                       or _regular(archive / name) != _regular(original_root / name)
                       for name in ("manifest.json", "collection.json", "supplemental-intent.json"))):
            raise TaskflowError("Composition public archive differs from its original sealed role")
        _full_verify(archive / "supplemental", intent, True)
        context = {"state": {"public_archive": archive_name, "archives": archives}}
        aliases = _review_observations(context, candidate, attempt)
        return {entry["target"]: aliases[entry["target"]] for entry in observed}

    @staticmethod
    def _composition_review(db: sqlite3.Connection, review: dict[str, Any], scope: str) -> dict:
        from taskflow.execution_flow_contracts import parse_review, review_metadata_aliases
        from taskflow.execution_runner import _info, _read, _validate, _validated_collection

        if review.get("snapshot_bound_acceptance") or review.get("composite_quality_coverage"):
            raise TaskflowError("Composition requires original managed independent signatures")
        candidate = ExecutionStore._load(db, "candidates", review["candidate_id"])
        attempt = ExecutionStore._load(db, "attempts", review["attempt_id"])
        if (attempt["state"] != "completed" or attempt["role"] != "quality"
                or attempt["candidate_id"] != candidate["candidate_id"] or not attempt.get("cli_sid")
                or review.get("completed_seq") != attempt["last_seq"]):
            raise TaskflowError("Composition source is not its actual completed independent quality attempt")
        for identity in ExecutionStore._quality_scope(db, candidate["candidate_id"]):
            ancestor = ExecutionStore._load(db, "candidates", identity)
            authors = set(ancestor.get("external_author_sids", []))
            if ancestor.get("author_attempt"):
                authors.add(ExecutionStore._load(db, "attempts", ancestor["author_attempt"])["cli_sid"])
            if attempt["cli_sid"] in authors:
                raise TaskflowError("Composition source signer participated in candidate production")
        validate_evidence(review["evidence"])
        validate_evidence(attempt["evidence"])
        runtime = attempt.get("runtime_path")
        if not isinstance(runtime, str) or not runtime:
            raise TaskflowError("Composition source lacks its original managed runtime")
        root = Path(runtime)
        manifest = _validate(root)
        collected = _validated_collection(root, manifest)
        report, issues = root / "artifacts/adoption-readiness.json", root / "artifacts/issues.json"
        if (collected.get("technical_accepted") is not True or collected.get("sid") != attempt["cli_sid"]
                or manifest.get("intent_id") != attempt["intent_id"]
                or manifest.get("role") != "quality"
                or manifest.get("candidate_binding") != {key: candidate[key] for key in
                                                         ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or any(_info(path) != collected["artifacts"].get(path.name) for path in (report, issues))
                or not all(any(ref["path"] == str(path) and ref["sha256"] == _info(path)["sha256"]
                               for ref in review["evidence"]) for path in (report, issues))):
            raise TaskflowError("Composition signature is not bound to its sealed actual report and issues")
        parsed = parse_review(report, issues, candidate=candidate, intent_id=attempt["intent_id"], scope=scope,
                              metadata_aliases=review_metadata_aliases(candidate, manifest, root),
                              observed_aliases=ExecutionStore._composition_observations(
                                  db, review, candidate, attempt, manifest, root, _read(issues)))
        expected = "revision_required" if review["verdict"] == "defects" else review["verdict"]
        if (parsed["outcome"] != expected or parsed["whole_package"] != review["whole_package"]
                or set(parsed["covered_defect_ids"]) != set(review.get("covered_defect_ids", []))):
            raise TaskflowError("Stored review and its actual independent routing disagree")
        return {"attempt": attempt, "candidate": candidate, "report": _read(report),
                "issues": _read(issues), "parsed": parsed,
                "report_ref": evidence_ref(report, "original independent composition source"),
                "issues_ref": evidence_ref(issues, "original independently located defects")}

    @staticmethod
    def _scoring_metadata_mirror(old: dict, fresh: dict, original: dict, current: dict,
                                 old_decl: dict, new_decl: dict, private_changes: list[dict]) -> dict:
        """Bind changed scoring to one explicitly selected current rubric object."""
        # This reader has no store callback: unlike native_quality_handoff it
        # cannot recurse through quality_gate. Formal has no top-level store import.
        from taskflow.execution_formal import _native_normative
        from taskflow.execution_flow_contracts import (
            NATIVE_SIGNOFF_PROTOCOL_SHA256, request_has_native_signoff_protocol, review_metadata_aliases,
        )
        from taskflow.execution_runner import _info, _read, _regular, _validate

        before, after = original.get("scoring"), current.get("scoring")
        if (not isinstance(before, dict) or not isinstance(after, dict) or canonical(before) == canonical(after)
                or canonical({key: value for key, value in original.items() if key not in {"files", "scoring"}})
                != canonical({key: value for key, value in current.items() if key not in {"files", "scoring"}})):
            raise TaskflowError("Scoring mirror requires only an explicit scoring-object metadata change")
        root = Path(fresh["attempt"]["runtime_path"])
        manifest = _validate(root)
        aliases = review_metadata_aliases(fresh["candidate"], manifest, root)
        if (set(aliases) != {"execution/task.json"} or aliases["execution/task.json"]["path"] != new_decl["path"]
                or aliases["execution/task.json"]["sha256"] != new_decl["sha256"]):
            raise TaskflowError("Scoring mirror needs its actual locked current declaration alias")
        declaration_input = next(entry for entry in manifest["inputs"] if entry["target"] == "execution/task.json")
        declaration_alias = {"manifest": {**evidence_ref(root / "manifest.json", "actual fresh role input manifest"),
                                          **_info(root / "manifest.json")},
            "manifest_input": declaration_input,
            "source": {**evidence_ref(Path(new_decl["path"]), "actual current declaration input source"),
                       **_info(Path(new_decl["path"]))},
            "staged": {**evidence_ref(root / "inputs/execution/task.json", "actual locked staged declaration"),
                       **_info(root / "inputs/execution/task.json")},
            "candidate_binding": manifest["candidate_binding"]}
        signoff = fresh["report"].get("native_signoff", {})
        if (type(signoff.get("schema_version")) is not int or signoff["schema_version"] != 1
                or signoff.get("protocol_sha256") != NATIVE_SIGNOFF_PROTOCOL_SHA256
                or not request_has_native_signoff_protocol(_regular(root / "request.md"))):
            raise TaskflowError("Scoring mirror needs the actual requested native normative signoff")
        normative = _native_normative({"candidate": fresh["candidate"], "manifest": manifest,
                                       "root": root, "signoff": signoff})
        matches = []
        for row in normative:
            if row["purpose"] != "rubric" or row["role"] not in {"teacher", "rubric"}:
                continue
            try:
                value = json.loads(_regular(Path(row["path"])).decode("utf-8"))
            except (ValueError, UnicodeError):
                continue  # A selected prose rubric is not a JSON scoring mirror.
            if isinstance(value, dict) and canonical(value) == canonical(after):
                matches.append(row)
        if len(matches) != 1:
            raise TaskflowError("Scoring mirror needs one exact independently selected current rubric JSON object")
        selected = matches[0]
        path = selected["package_path"]
        previous = [row for row in old["candidate"]["files"] if row["package_path"] == path]
        changed = [row for row in private_changes if row["path"] == path]
        if (len(previous) != 1 or previous[0]["role"] != selected["role"]
                or len(changed) != 1 or changed[0].get("original_issue_affected") is not True
                or changed[0]["old_sha256"] != previous[0]["sha256"]
                or changed[0]["new_sha256"] != selected["sha256"]
                or signoff.get("coverage") != "affected" or path not in signoff.get("affected_paths", [])):
            raise TaskflowError("Scoring mirror source is outside the actual original defect and affected repair coverage")
        prior_path = Path(previous[0]["path"])
        if (_info(prior_path)["sha256"] != previous[0]["sha256"]
                or canonical(_read(prior_path)) != canonical(before)):
            raise TaskflowError("Original scoring object is not its exact same-path declared rubric JSON")
        rows = signoff["normative"]["files"]
        index = next(index for index, row in enumerate(rows) if row["package_path"] == path)
        signed = [{"report": fresh["report_ref"], "json_pointer": f"/native_signoff/normative/files/{index}",
                   "actual_value": rows[index]}, *changed[0]["fresh_signed_evidence"]]
        declaration_sources = []
        def citations(value: Any, pointer: str) -> None:
            if isinstance(value, dict):
                if (value.get("path") == "execution/task.json" and value.get("sha256") == new_decl["sha256"]
                        and isinstance(value.get("location"), str) and value["location"].strip()):
                    declaration_sources.append({"report": fresh["report_ref"], "json_pointer": pointer, "actual_value": value})
                for key, item in value.items():
                    citations(item, pointer + "/" + key.replace("~", "~0").replace("/", "~1"))
            elif isinstance(value, list):
                for number, item in enumerate(value):
                    citations(item, pointer + "/" + str(number))
        citations(fresh["report"], "")
        if not declaration_sources:
            raise TaskflowError("Scoring mirror needs the independent current declaration citation")
        changes = []
        def difference(left: Any, right: Any, pointer: str) -> None:
            if canonical(left) == canonical(right):
                return
            if isinstance(left, dict) and isinstance(right, dict) and set(left) == set(right):
                for key in sorted(left):
                    difference(left[key], right[key], pointer + "/" + key.replace("~", "~0").replace("/", "~1"))
            elif isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
                for number, (a, b) in enumerate(zip(left, right)):
                    difference(a, b, pointer + "/" + str(number))
            else:
                changes.append({"json_pointer": pointer, "before": left, "after": right})
        difference(before, after, "/scoring")
        return {"schema_version": 1, "kind": "deterministic_signed_scoring_mirror_v1", "json_pointer": "/scoring",
                "before": before, "after": after, "changes": changes,
                "original_declaration": old_decl, "current_declaration": new_decl,
                "declaration_alias": declaration_alias,
                "source": {"package_path": path, "role": selected["role"],
                    "original": {**evidence_ref(prior_path, "original declared scoring mirror; not a positive old judgment"), **_info(prior_path)},
                    "current": {**evidence_ref(Path(selected["path"]), "current independently selected rubric mirror"),
                                **_info(Path(selected["path"]))}},
                "signed_evidence": [*signed, *declaration_sources],
                "original_scope": {"coverage": old["report"].get("native_signoff", {}).get("coverage"),
                    "affected_paths": old["report"].get("native_signoff", {}).get("affected_paths"),
                    "outcome": old["report"]["execution_routing"]["outcome"]}}

    @staticmethod
    def _repaired_quality_material(db: sqlite3.Connection, candidate_id: str, whole_review_id: str,
                                   recheck_id: str, closed_defect_ids: list[str], *,
                                   supplement: dict[str, Any] | None = None) -> dict:
        from taskflow.execution_flow_contracts import _files, validate_repair
        from taskflow.execution_runner import _info, _read, _regular, _validate, _validated_collection
        from taskflow.packages import check_task

        if (not isinstance(closed_defect_ids, list) or not closed_defect_ids
                or any(not isinstance(item, str) for item in closed_defect_ids)
                or len(closed_defect_ids) != len(set(closed_defect_ids))):
            raise TaskflowError("Composition requires an exact nonempty unique closed defect list")
        for identity in closed_defect_ids:
            validate_id(identity, "closed defect ID")
        candidate = ExecutionStore._load(db, "candidates", candidate_id)
        whole = ExecutionStore._load(db, "reviews", whole_review_id)
        recheck = ExecutionStore._load(db, "reviews", recheck_id)
        if (whole["verdict"] != "defects" or whole.get("whole_package") is not True
                or recheck["verdict"] != "accepted" or recheck.get("whole_package") is not False
                or recheck["candidate_id"] != candidate_id or candidate.get("parent_id") != whole["candidate_id"]
                or set(recheck.get("covered_defect_ids", [])) != set(closed_defect_ids)):
            raise TaskflowError("Composition needs the exact whole finding and its direct repaired affected closure")
        old = ExecutionStore._composition_review(db, whole, "whole_package")
        fresh = ExecutionStore._composition_review(db, recheck, "affected")
        parent = old["candidate"]
        if candidate["input_digest"] != parent["input_digest"]:
            raise TaskflowError("Changed public input cannot reuse whole quality through private repair")
        before, after = _files(parent), _files(candidate)
        added = sorted(set(after) - set(before))
        if (set(before) - set(after) or any(before[key]["role"] != after[key]["role"] for key in before)
                or any(after[key]["role"] not in {"teacher", "verification"} for key in added)):
            raise TaskflowError("Composition only supports unchanged original file/roles and added private validation evidence")
        for item in (parent, candidate):
            declarations = [ref for ref in item["evidence"] if ref.get("location") == "explicit package declaration"]
            if len(declarations) != 1:
                raise TaskflowError("Composition needs each actual captured package declaration")
            validate_evidence(declarations)
            actual = check_task(Path(declarations[0]["path"]).parent, write_report=False)
            if actual["errors"] or any(actual[key] != item[key] for key in
                                       ("task_id", "content_digest", "input_digest", "evaluation_digest")):
                raise TaskflowError("Composition candidate bytes do not match its captured version")
        old_decl = next(ref for ref in parent["evidence"] if ref.get("location") == "explicit package declaration")
        new_decl = next(ref for ref in candidate["evidence"] if ref.get("location") == "explicit package declaration")
        declaration_delta, scoring_mirror = None, None
        if old_decl["sha256"] != new_decl["sha256"]:
            original, current = _read(Path(old_decl["path"])), _read(Path(new_decl["path"]))
            originals = {row["path"]: row for row in original["files"]}
            retained, metadata_updates = [], []
            for index, entry in enumerate(current["files"]):
                key = entry["path"]
                if key not in before:
                    for field, value in (("sha256", after[key]["sha256"]), ("bytes", _info(Path(after[key]["path"]))["size"])):
                        if field in entry and (entry[field] != value or (field == "bytes" and type(entry[field]) is not int)):
                            raise TaskflowError("Added private validation declaration does not bind its actual bytes")
                    continue
                restored = dict(entry)
                for field, info_key in (("sha256", "sha256"), ("bytes", "size")):
                    prior = originals[key]
                    if entry.get(field) != prior.get(field):
                        if (field not in entry or field not in prior or before[key]["sha256"] == after[key]["sha256"]
                                or after[key]["role"] in PUBLIC_ROLES
                                or (field == "bytes" and (type(prior[field]) is not int or type(entry[field]) is not int))
                                or prior[field] != _info(Path(before[key]["path"]))[info_key]
                                or entry[field] != _info(Path(after[key]["path"]))[info_key]):
                            raise TaskflowError("Composition declaration byte metadata is not an exact private-file update")
                        metadata_updates.append({"json_pointer": f"/files/{index}/{field}", "path": key,
                                                 "before": prior[field], "after": entry[field]})
                        restored[field] = prior[field]
                retained.append(restored)
            restored = {**current, "files": retained}
            if canonical(restored) != canonical(original) or not added:
                if (canonical(original.get("scoring")) == canonical(current.get("scoring"))
                        or canonical({**restored, "scoring": original.get("scoring")}) != canonical(original)
                        or not isinstance(fresh["report"].get("native_signoff"), dict)
                        or not isinstance(fresh["report"]["native_signoff"].get("normative"), dict)):
                    raise TaskflowError("Composition cannot silently inherit changed package/scoring metadata")
                scoring_mirror = (original, current, old_decl, new_decl)
            declaration_delta = {"kind": "private_validation_evidence_addition" if added else "signed_scoring_mirror_transport",
                "original": old_decl, "current": new_decl,
                "file_metadata_updates": metadata_updates,
                "added_files": [{"json_pointer": f"/files/{index}", "declaration": row,
                                 "sha256": after[row["path"]]["sha256"]}
                                for index, row in enumerate(current["files"]) if row["path"] in added]}
        changed = sorted(key for key in after if key not in before or before[key]["sha256"] != after[key]["sha256"])
        if not changed or any(after[key]["role"] in PUBLIC_ROLES for key in changed):
            raise TaskflowError("Composition requires actual private-only byte changes")
        defects = [_data(row) for row in db.execute("SELECT data FROM defects")
                   if _data(row)["review_id"] == whole_review_id]
        own_ids = {row["defect_id"] for row in defects}
        inherited_ids = set(closed_defect_ids) - own_ids
        inherited = []
        if inherited_ids:
            if not own_ids <= set(closed_defect_ids) or set(whole.get("covered_defect_ids", [])) != inherited_ids:
                raise TaskflowError("Composition inherited responsibilities must be exactly the whole signature's retained IDs")
            for identity in sorted(inherited_ids):
                defect = ExecutionStore._load(db, "defects", identity)
                chain = defect.get("closure_coverage_chain")
                if (defect["state"] != "closed" or not isinstance(chain, dict)
                        or chain.get("repair_review_id") != whole_review_id or chain.get("final_review_id") != recheck_id
                        or defect.get("closure_review_id") != recheck_id):
                    raise TaskflowError("Composition inherited responsibility lacks its exact typed whole/affected closure chain")
                validated = ExecutionStore._closure_coverage_chain_material(db, defect, whole_review_id, recheck_id)
                if (chain != validated or chain["current_binding"]["candidate_id"] != candidate_id
                        or chain["parent_binding"]["candidate_id"] != parent["candidate_id"]):
                    raise TaskflowError("Composition inherited closure chain differs from its actual current sources")
                validate_evidence(defect["closure_evidence"])
                inherited.append(validated)
        elif own_ids != set(closed_defect_ids):
            raise TaskflowError("Composition must cover every and only the source whole-review defect")
        repair_ids = [identity for identity in closed_defect_ids if identity in own_ids]
        original_paths, original_ids = set(), {}
        remaining = list(old["parsed"]["defects"])
        author_id = candidate.get("author_attempt")
        if not author_id:
            raise TaskflowError("Composition requires the actual managed repair author")
        author = ExecutionStore._load(db, "attempts", author_id)
        for defect in defects:
            supported = ((defect["kind"] in ("reference", "rubric") and defect.get("disposition") == "private_repair")
                         or (defect["kind"] == "claim" and defect.get("disposition") == "scope_adjustment"))
            if not supported:
                raise TaskflowError("Composition does not support this defect kind/repair disposition: "
                                    + str(defect["kind"]) + "/" + str(defect.get("disposition")))
            if (defect["candidate_id"] != parent["candidate_id"] or defect["state"] != "closed"
                    or defect.get("repair_candidate_id") != candidate_id
                    or defect.get("closure_review_id") != recheck_id
                    or defect.get("author_attempt") != author_id
                    or not whole["created_seq"] < defect["opened_seq"] < author["claimed_seq"]
                    or not author["last_seq"] < candidate["created_seq"] < defect["repair_seq"] < fresh["attempt"]["claimed_seq"]
                    or not recheck["created_seq"] < defect["closed_seq"]):
                raise TaskflowError("Composition defect lacks exact repair/closure identity and actual ordering")
            validate_evidence(defect["evidence"])
            validate_evidence(defect["repair_evidence"])
            validate_evidence(defect["closure_evidence"])
            matches = [row for row in remaining if row["kind"] == defect["kind"]
                       and row["responsibility"] == defect["responsibility"] and row["evidence"] == defect["evidence"]]
            if len(matches) != 1:
                raise TaskflowError("Closed defect does not match one original signed blocking issue")
            row = matches[0]
            if defect["kind"] == "claim" and (not row["affected_paths"] or any(
                    path not in before or before[path]["role"] in PUBLIC_ROLES for path in row["affected_paths"])):
                raise TaskflowError("Claim scope composition requires exclusively original signed private affected paths")
            remaining.remove(row)
            original_paths.update(row["affected_paths"])
            original_ids[defect["defect_id"]] = row["id"]
        if remaining or not defects or author["candidate_id"] != parent["candidate_id"]:
            raise TaskflowError("Original whole findings or actual repair parent remain uncovered")
        if (fresh["attempt"]["cli_sid"] in (old["attempt"]["cli_sid"], author.get("cli_sid"))
                or fresh["attempt"]["claimed_seq"] <= author["last_seq"]):
            raise TaskflowError("Composition requires a fresh independent post-repair context")
        delivered = ExecutionStore._delivered_artifacts(author)
        root = Path(author["runtime_path"])
        if author["state"] == "failed":
            material = ExecutionStore._validated_delivery_resolution(author)
            record_path = material["record_path"]
        elif author["state"] == "completed":
            manifest = _validate(root)
            collected = _validated_collection(root, manifest)
            if "repair-record.json" not in collected["artifacts"]:
                raise TaskflowError("Composition lacks the actual collected repair manifest")
            record_path = root / "artifacts/repair-record.json"
        else:
            raise TaskflowError("Composition repair author is not terminally delivered")
        projection = None
        typed = [[ref for ref in defect["repair_evidence"] if ref.get("kind") == "repair_record_projection"]
                 for defect in defects]
        if any(typed):
            from taskflow.execution_flow_contracts import resolve_repair_record_projection

            if any(len(rows) != 1 for rows in typed):
                raise TaskflowError("Composition requires one consistent typed repair projection for every closed defect")
            pairs, bindings = [], []
            for rows in typed:
                reference = rows[0]
                pair = reference.get("repair_record_projection")
                if not isinstance(pair, dict) or set(pair) != {"original", "projected"}:
                    raise TaskflowError("Composition repair projection needs its exact original/projected references")
                validate_evidence([pair["original"], pair["projected"]])
                if any(reference.get(key) != pair["projected"].get(key) for key in ("path", "sha256")):
                    raise TaskflowError("Composition derived evidence does not bind its projected repair record")
                pairs.append(pair)
                bindings.append({name: {key: pair[name][key] for key in ("path", "sha256")}
                                 for name in ("original", "projected")})
            if any(value != bindings[0] for value in bindings):
                raise TaskflowError("Composition closed defects reference conflicting repair projections")
            projection = resolve_repair_record_projection(pairs[0], attempt=author, parent=parent,
                                                          candidate=candidate, defect_ids=repair_ids)
            record_path = Path(projection["record_path"])
        repair = validate_repair(record_path, parent=parent, candidate=candidate, defect_ids=repair_ids)
        if any(after[key]["sha256"] not in delivered for key in changed):
            raise TaskflowError("Composition changes are absent from the actual repair delivery")
        supplemental = None
        completed_seq = recheck["completed_seq"]
        completed_at = ExecutionStore._review_time(db, recheck)
        report = fresh["report"]
        if supplement is not None:
            from taskflow.execution_recheck_supplement import validate_recheck_supplement

            if not isinstance(supplement, dict) or supplement.get("base_recheck_id") != recheck_id:
                raise TaskflowError("Recheck supplement must bind this exact original closure review")
            supplemental_attempt = ExecutionStore._load(db, "attempts", supplement["attempt_id"])
            excluded = {old["attempt"]["cli_sid"], fresh["attempt"]["cli_sid"]}
            ancestors = ExecutionStore._ancestors(db, candidate_id)
            for identity in ancestors:
                ancestor = ExecutionStore._load(db, "candidates", identity)
                excluded.update(ancestor.get("external_author_sids", []))
                if ancestor.get("author_attempt"):
                    excluded.add(ExecutionStore._load(db, "attempts", ancestor["author_attempt"])["cli_sid"])
            for row in db.execute("SELECT * FROM execution_flows"):
                flow = ExecutionStore._flow_value(row)
                if flow["definition"].get("task_id") != candidate["task_id"]:
                    continue
                initial = flow["definition"].get("initial") or {}
                ids = []
                if initial.get("candidate_id") in ancestors and initial.get("predecessor_attempt"):
                    ids.append(initial["predecessor_attempt"])
                if flow["state"].get("candidate_id") in ancestors and flow["state"].get("public_attempt_id"):
                    ids.append(flow["state"]["public_attempt_id"])
                for identity in ids:
                    excluded.add(ExecutionStore._load(db, "attempts", identity)["cli_sid"])
            if not supplemental_attempt.get("cli_sid") or supplemental_attempt["cli_sid"] in excluded:
                raise TaskflowError("Recheck supplement reused an author/public/prior-quality context")
            parent_files = {item["path"]: item for item in parent["files"]}
            parent_aliases = {}
            for item in _validate(root)["inputs"]:
                captured = parent_files.get(item["source"])
                if captured is None:
                    continue
                staged = root / "inputs" / relative_path(item["target"])
                expected = _info(Path(item["source"]))
                if (expected["sha256"] != captured["sha256"]
                        or ("size" in captured and expected["size"] != captured["size"])
                        or {key: item[key] for key in ("sha256", "size")} != expected
                        or _info(staged) != expected or _info(Path(item["source"])) != expected
                        or _regular(staged) != _regular(Path(item["source"]))):
                    raise TaskflowError("Recheck supplement parent alias differs from the locked repair input bytes")
                parent_aliases[str(staged)] = item["source"]
            transport = validate_recheck_supplement(supplement=supplement, attempt=supplemental_attempt,
                prior=fresh, parent=parent, candidate=candidate, closed_defect_ids=closed_defect_ids,
                changed_paths=changed, original_paths=original_paths, parent_aliases=parent_aliases)
            supplemental = {"definition": supplement, "transport": transport}
            report = transport["report"]
            completed_seq = max(completed_seq, supplemental_attempt["last_seq"])
            completed_at = max(completed_at, ExecutionStore._review_time(db,
                {"attempt_id": supplemental_attempt["attempt_id"], "created_seq": supplemental_attempt["last_seq"]}))
        detail = report.get("defect_resolution", {})
        evidence = detail.get("evidence", []) if isinstance(detail, dict) else []
        extra_paths = detail.get("affected_paths", []) if isinstance(detail, dict) else []
        coverage = report.get("coverage", {})
        rows = coverage.get("changed_files", []) if isinstance(coverage, dict) else []
        if not all(isinstance(value, list) for value in (evidence, extra_paths, rows)):
            raise TaskflowError("Fresh recheck changed-association evidence must use explicit lists")
        def signed_sources(pointer: str, value: Any) -> list[dict]:
            if supplemental is None:
                return [{"report": fresh["report_ref"], "json_pointer": pointer, "actual_value": value}]
            sources = supplemental["transport"]["sources"].get(pointer)
            if not isinstance(sources, list) or not sources:
                raise TaskflowError("Recheck supplement lacks exact raw provenance for canonical pointer: " + pointer)
            return sources
        graph = []
        for key in changed:
            matching = [(index, row) for index, row in enumerate(evidence) if isinstance(row, dict)
                        and row.get("path") == key and row.get("sha256") == after[key]["sha256"]
                        and isinstance(row.get("location"), str) and row["location"].strip()]
            if not matching:
                raise TaskflowError("Fresh recheck has no explicit current SHA evidence for changed path: " + key)
            sources = list(signed_sources(f"/defect_resolution/evidence/{matching[0][0]}", matching[0][1]))
            if key not in original_paths:
                signed = [(index, row) for index, row in enumerate(rows) if isinstance(row, dict)
                          and row.get("path") == key and "old" in row
                          and row.get("old") == before.get(key, {}).get("sha256")
                          and row.get("new") == after[key]["sha256"]]
                if key not in extra_paths or len(signed) != 1:
                    raise TaskflowError("Changed path is outside original issues and exact signed necessary associations: " + key)
                sources += signed_sources(f"/defect_resolution/affected_paths/{extra_paths.index(key)}", key)
                sources += signed_sources(f"/coverage/changed_files/{signed[0][0]}", signed[0][1])
            graph.append({"path": key, "old_sha256": before.get(key, {}).get("sha256"), "new_sha256": after[key]["sha256"],
                          "role": after[key]["role"], "original_issue_affected": key in original_paths,
                          "original_signed_evidence": [
                              {"issues": old["issues_ref"], "json_pointer": f"/issues/{index}/affected_paths/{paths.index(key)}", "actual_value": key}
                              for index, issue in enumerate(old["issues"]["issues"])
                              if issue["id"] in original_ids.values() and key in (paths := issue["affected_paths"])],
                          "fresh_signed_evidence": sources})
        if scoring_mirror:
            scoring_mirror = ExecutionStore._scoring_metadata_mirror(old, fresh, *scoring_mirror, graph)
        old_order = (ExecutionStore._review_time(db, whole), whole["completed_seq"])
        quality_scope = ExecutionStore._quality_scope(db, candidate_id)
        for row in db.execute("SELECT data FROM defects WHERE task_id=?", (candidate["task_id"],)):
            other = _data(row)
            if other["candidate_id"] in quality_scope and other["state"] != "closed":
                raise TaskflowError("An additional open responsibility prevents repaired quality composition")
        for row in db.execute("SELECT data FROM reviews"):
            other = _data(row)
            if other["verdict"] == "accepted" or other["review_id"] == whole_review_id:
                continue
            related = other["candidate_id"] in ExecutionStore._quality_scope(db, candidate_id)
            related = related or candidate_id in ExecutionStore._quality_scope(db, other["candidate_id"])
            order = (ExecutionStore._review_time(db, other), other.get("completed_seq", other["created_seq"]))
            if related and order > old_order:
                validate_evidence(other["evidence"])
                raise TaskflowError("Additional later independent negative/conflict prevents this bounded composition")
        ExecutionStore._assert_review_current(db, recheck_id)
        limits = lambda source: {key: source[key] for key in
                                ("limits", "limitations", "source_scope_limits", "accepted_use") if key in source}
        return {"whole_review_id": whole_review_id, "recheck_id": recheck_id,
                "whole_source": {"candidate_id": parent["candidate_id"], "attempt_id": old["attempt"]["attempt_id"],
                                 "sid": old["attempt"]["cli_sid"], "scope": whole["scope"], "report": old["report_ref"],
                                 "issues": old["issues_ref"], "whole_package": True, "verdict": "defects"},
                "affected_source": {"candidate_id": candidate_id, "attempt_id": fresh["attempt"]["attempt_id"],
                                    "sid": fresh["attempt"]["cli_sid"], "scope": recheck["scope"],
                                    "report": fresh["report_ref"], "issues": fresh["issues_ref"], "whole_package": False},
                "closed_defect_ids": sorted(closed_defect_ids), "original_issue_ids": original_ids,
                "private_changes": graph, "repair_record": evidence_ref(record_path,
                    "validated derived repair manifest" if projection else "actual repair byte manifest"),
                **({"coverage_mode": "whole_findings_with_inherited_affected_closure_v1",
                    "inherited_closure_chains": inherited} if inherited else {}),
                 **({"declaration_delta": declaration_delta} if declaration_delta else {}),
                **({"scoring_metadata_mirror": scoring_mirror} if scoring_mirror else {}),
                **({"repair_record_projection": projection} if projection else {}),
                **({"recheck_supplement": supplemental} if supplemental else {}),
                "repair_changes": repair["changes"], "input_digest": candidate["input_digest"],
                "unchanged_files": [{"path": key, "sha256": after[key]["sha256"], "role": after[key]["role"]}
                                    for key in sorted(after) if key not in changed],
                "source_limits": {"whole": limits(old["report"]), "affected": limits(fresh["report"])},
                "completed_seq": completed_seq, "source_completed_at": datetime.fromtimestamp(completed_at, timezone.utc).isoformat()}

    @staticmethod
    def _validate_repaired_quality(db: sqlite3.Connection, review: dict[str, Any]) -> None:
        validate_evidence(review["evidence"])
        graph = review["composite_quality_coverage"]
        current = ExecutionStore._repaired_quality_material(db, review["candidate_id"], graph["whole_review_id"],
            graph["recheck_id"], graph["closed_defect_ids"],
            supplement=graph.get("recheck_supplement", {}).get("definition"))
        if (current != graph or review.get("whole_package") is not False
                or review.get("snapshot_bound_acceptance") is not True
                or review.get("completed_seq") != graph["completed_seq"]
                or review.get("source_completed_at") != graph["source_completed_at"]
                or review.get("attempt_id") != graph["affected_source"]["attempt_id"]):
            raise TaskflowError("Derived repaired-quality coverage changed or upgraded an original scope")

    def compose_repaired_quality(self, candidate_id: str, whole_review_id: str, recheck_id: str,
                                 closed_defect_ids: list[str], *, evidence: list[dict[str, Any]],
                                 supplement: dict[str, Any] | None = None) -> dict:
        """Record bounded combined coverage, never a new whole-package signature."""
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            graph = self._repaired_quality_material(db, candidate_id, whole_review_id, recheck_id, closed_defect_ids,
                                                    supplement=supplement)
            candidate = self._load(db, "candidates", candidate_id)
            identity_binding = {"candidate": candidate_id, "whole": whole_review_id,
                                "recheck": recheck_id, "defects": sorted(closed_defect_ids)}
            if supplement is not None:
                identity_binding["supplement"] = supplement
            if graph.get("inherited_closure_chains"):
                identity_binding["inherited_closure_chains"] = graph["inherited_closure_chains"]
            identity = "composed-" + sha256(canonical(identity_binding))[:32]
            existing = db.execute("SELECT data FROM reviews WHERE id=?", (identity,)).fetchone()
            if existing:
                value = _data(existing)
                self._validate_repaired_quality(db, value)
                if value["evidence"] != refs:
                    raise TaskflowError("Composite quality cannot replace its original acceptance evidence")
                return value
            value = {"review_id": identity, "candidate_id": candidate_id,
                     "attempt_id": graph["affected_source"]["attempt_id"],
                     **{key: candidate[key] for key in ("input_digest", "content_digest", "evaluation_digest")},
                     "verdict": "accepted", "scope": "explicit_repaired_quality_coverage",
                     "whole_package": False, "snapshot_bound_acceptance": True,
                     "covered_defect_ids": sorted(closed_defect_ids), "evidence": refs,
                     "composite_quality_coverage": graph, "completed_seq": graph["completed_seq"],
                     "source_completed_at": graph["source_completed_at"], "derived": True}
            value["created_seq"] = self._event(db, "repaired_quality_composed", value)
            db.execute("INSERT INTO reviews VALUES(?,?,?)", (identity, candidate_id, canonical(value).decode()))
            return value

    @staticmethod
    def _assert_closed_quality_record(db: sqlite3.Connection, defect: dict) -> None:
        """A closed flag must retain its exact append-only repair/closure events."""
        base = ("defect_id", "candidate_id", "review_id", "repair_candidate_id", "author_attempt")
        for sequence, kinds, fields in (
            (defect.get("repair_seq"), {"defect_repaired"},
             (*base, "disposition", "changed_files", "repair_evidence")),
            (defect.get("closed_seq"), {"defect_closed", "defect_closed_with_coverage_chain"},
             (*base, "repair_seq", "closure_review_id", "closure_evidence", "closure_coverage_chain")),
        ):
            event = db.execute("SELECT kind,data FROM events WHERE seq=?", (sequence,)).fetchone()
            if (event is None or event["kind"] not in kinds
                    or any(_data(event).get(key) != defect.get(key) for key in fields)):
                raise TaskflowError("quality closure differs from its original repair/closure events")

    @staticmethod
    def _consumed_quality_defect_ids(negative: dict, current: dict, candidate: dict,
                                     issues: list[dict], defects: list[dict], author: dict,
                                     fresh: dict, original: dict) -> list[str]:
        """Match every actual whole finding to its ordered current whole closure."""
        ids = [row["defect_id"] for row in defects]
        covered = current.get("covered_defect_ids", [])
        if (not ids or len(ids) != len(set(ids)) or len(covered) != len(set(covered))
                or set(ids) != set(covered) or candidate.get("parent_id") != negative["candidate_id"]
                or current["candidate_id"] != candidate["candidate_id"]
                or original["candidate_id"] != negative["candidate_id"]
                or original["task_id"] != candidate["task_id"]
                or author.get("state") != "completed" or author.get("role") != "repair"
                or author.get("candidate_id") != original["candidate_id"]
                or candidate.get("author_attempt") != author["attempt_id"]
                or not author.get("cli_sid") or author["cli_sid"] == fresh["cli_sid"]
                or fresh["last_seq"] != current["completed_seq"]):
            raise TaskflowError("historical negative lacks exact current whole repair coverage")
        remaining = list(issues)
        for defect in defects:
            matches = [row for row in remaining if row["kind"] == defect["kind"]
                       and row["responsibility"] == defect["responsibility"] and row["evidence"] == defect["evidence"]]
            if (len(matches) != 1 or defect["state"] != "closed"
                    or defect["review_id"] != negative["review_id"]
                    or defect["candidate_id"] != original["candidate_id"]
                    or defect.get("repair_candidate_id") != candidate["candidate_id"]
                    or defect.get("closure_review_id") != current["review_id"]
                    or defect.get("author_attempt") != author["attempt_id"]
                    or defect.get("disposition") not in (
                        {"private_repair", "scope_adjustment"} if defect["kind"] in {"reference", "rubric"}
                        else {"material_repair", "scope_adjustment"} if defect["kind"] == "material"
                        else {"scope_adjustment"})
                    or (defect["kind"] == "material" and original["input_digest"] == candidate["input_digest"])
                    or (defect.get("disposition") == "private_repair"
                        and original["evaluation_digest"] == candidate["evaluation_digest"])
                    or not negative["created_seq"] < defect["opened_seq"] < author["claimed_seq"]
                    < author["last_seq"] < candidate["created_seq"] < defect["repair_seq"]
                    < fresh["claimed_seq"] < fresh["last_seq"] < current["created_seq"] < defect["closed_seq"]):
                raise TaskflowError("historical negative has an unmatched or unordered current closure")
            remaining.remove(matches[0])
        if remaining:
            raise TaskflowError("historical negative has uncovered actual findings")
        return sorted(ids)

    @staticmethod
    def _assert_consumed_quality_negative(db: sqlite3.Connection, negative: dict, current: dict) -> None:
        """Admit only the exact finding consumed by a validated current repair."""
        from taskflow.execution_flow_contracts import validate_repair
        from taskflow.execution_runner import _validate, _validated_collection

        accepted = ExecutionStore._assert_review_current(db, current["review_id"])
        if (accepted != current or negative["verdict"] != "defects" or negative.get("whole_package") is not True
                or ExecutionStore._review_time(db, negative) >= ExecutionStore._review_time(db, current)):
            raise TaskflowError("historical negative is not an earlier consumed whole finding")
        graph = current.get("composite_quality_coverage")
        if graph:
            # assert_review_current rebuilt the entire canonical graph, including
            # its original issues, actual changes, sources and exact closures.
            if graph["whole_review_id"] != negative["review_id"]:
                raise TaskflowError("historical negative is outside the current repaired quality graph")
            return
        if (current.get("whole_package") is not True or current.get("derived")
                or current.get("snapshot_bound_acceptance")):
            raise TaskflowError("historical negative requires a current original whole acceptance")
        old = ExecutionStore._composition_review(db, negative, "whole_package")
        fresh = ExecutionStore._composition_review(db, current, "whole_package")
        candidate = fresh["candidate"]
        if not candidate.get("author_attempt"):
            raise TaskflowError("historical negative requires its actual current repair author")
        author = ExecutionStore._load(db, "attempts", candidate["author_attempt"])
        defects = [_data(row) for row in db.execute("SELECT data FROM defects")
                   if _data(row)["review_id"] == negative["review_id"]]
        ids = ExecutionStore._consumed_quality_defect_ids(negative, current, candidate,
            old["parsed"]["defects"], defects, author, fresh["attempt"], old["candidate"])
        if old["attempt"]["cli_sid"] in {author["cli_sid"], fresh["attempt"]["cli_sid"]}:
            raise TaskflowError("historical negative repair reused an independent source context")
        for defect in defects:
            ExecutionStore._assert_closed_quality_record(db, defect)
            for key in ("evidence", "repair_evidence", "closure_evidence"):
                validate_evidence(defect[key])
        root = Path(author["runtime_path"])
        manifest = _validate(root)
        collected = _validated_collection(root, manifest)
        if (manifest.get("candidate_binding") != {key: old["candidate"][key] for key in
                ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or manifest.get("role") != author["role"] or manifest.get("intent_id") != author["intent_id"]
                or collected.get("sid") != author["cli_sid"] or collected.get("technical_accepted") is not True
                or "repair-record.json" not in collected["artifacts"]):
            raise TaskflowError("historical negative repair lacks its sealed actual author delivery")
        validate_repair(root / "artifacts/repair-record.json", parent=old["candidate"], candidate=candidate, defect_ids=ids)
        ExecutionStore._validate_revision_delivery(old["candidate"], candidate["files"], candidate["evidence"], author)

    @staticmethod
    def _assert_repair_alias_current(db: sqlite3.Connection, defect: dict, quality_scope: set[str],
                                     latest: dict) -> None:
        """Validate the captured bytes and original closure behind alias ancestry."""
        fingerprints = {}
        for identity in quality_scope:
            item = ExecutionStore._load(db, "candidates", identity)
            validate_evidence(item["evidence"])
            if not item["files"]:
                raise TaskflowError("repair alias requires captured candidate file members")
            files = validate_evidence(item["files"])
            fingerprint = tuple(item[key] for key in ("input_digest", "content_digest", "evaluation_digest"))
            members = sorted((row["package_path"], row.get("id"), row["role"], row["sha256"], row.get("size"))
                             for row in files)
            if fingerprint in fingerprints and fingerprints[fingerprint] != members:
                raise TaskflowError("repair alias candidate file members differ despite matching digests")
            fingerprints[fingerprint] = members
        validate_evidence(defect["repair_evidence"])
        validate_evidence(defect["closure_evidence"])
        ExecutionStore._assert_closed_quality_record(db, defect)
        author = ExecutionStore._load(db, "attempts", defect["author_attempt"])
        delivered = ExecutionStore._delivered_artifacts(author)
        if any(row["sha256"] not in delivered for row in validate_evidence(defect["changed_files"])):
            raise TaskflowError("repair alias differs from the original author's actual delivery")
        closure = ExecutionStore._review_acceptance_material(db, defect["closure_review_id"])
        repair = ExecutionStore._load(db, "candidates", defect["repair_candidate_id"])
        if (repair.get("author_attempt") != author["attempt_id"]
                or defect["candidate_id"] not in ExecutionStore._ancestors(db, repair["candidate_id"])
                or repair["candidate_id"] not in ExecutionStore._ancestors(db, closure["candidate_id"])
                or (repair["candidate_id"] != closure["candidate_id"]
                    and defect["defect_id"] not in closure.get("covered_defect_ids", []))
                or not defect["opened_seq"] < repair["created_seq"] < defect["repair_seq"]
                < closure["created_seq"] < defect["closed_seq"]):
            raise TaskflowError("repair alias lacks its original ordered closure identity")
        chain = defect.get("closure_coverage_chain")
        if chain:
            material = ExecutionStore._closure_coverage_chain_material(db, defect,
                chain["repair_review_id"], closure["review_id"], current_acceptance=latest)
            if material != chain:
                raise TaskflowError("repair alias original closure coverage chain changed")
        elif not closure["whole_package"]:
            original = ExecutionStore._load(db, "candidates", defect["candidate_id"])
            repaired = ExecutionStore._load(db, "candidates", closure["candidate_id"])
            source = ExecutionStore._load(db, "attempts", closure["attempt_id"])
            if (defect["kind"] == "material" or defect["defect_id"] not in closure.get("covered_defect_ids", [])
                    or original["input_digest"] != repaired["input_digest"] or source["claimed_seq"] <= defect["repair_seq"]):
                raise TaskflowError("repair alias has no original same-public affected closure")
        ExecutionStore._assert_no_newer_negative(db, closure, current_acceptance=latest)
        closed = db.execute("SELECT at FROM events WHERE seq=?", (defect["closed_seq"],)).fetchone()["at"]
        if max(closed, ExecutionStore._review_time(db, closure)) > ExecutionStore._review_time(db, latest):
            raise TaskflowError("repair alias closure postdates current independent quality")

    @staticmethod
    def _assert_quality(db: sqlite3.Connection, candidate_id: str, *, require_repair_alias: bool = False) -> dict[str, Any]:
        candidate = ExecutionStore._load(db, "candidates", candidate_id)
        ancestors = ExecutionStore._ancestors(db, candidate_id)
        quality_scope = ExecutionStore._quality_scope(db, candidate_id)
        reviews = [_data(row) for row in db.execute("SELECT data FROM reviews WHERE candidate_id=?", (candidate_id,))]
        current = [row for row in reviews if row["verdict"] == "accepted" and (row["whole_package"] or row.get("snapshot_bound_acceptance")) and row["content_digest"] == candidate["content_digest"]]
        if not current:
            raise TaskflowError("current whole-package independent quality review missing/stale")
        latest = max(current, key=lambda row: (ExecutionStore._review_time(db, row), row["created_seq"]))
        if latest.get("composite_quality_coverage"):
            ExecutionStore._validate_repaired_quality(db, latest)
        latest_order = ExecutionStore._review_time(db, latest)
        alias_repair = False
        for row in db.execute("SELECT data FROM defects WHERE task_id=?", (candidate["task_id"],)):
            defect = _data(row)
            source = ExecutionStore._load(db, "candidates", defect["candidate_id"])
            digest_key = "input_digest" if defect["kind"] == "material" else "evaluation_digest" if defect["kind"] in ("reference", "rubric") else "content_digest"
            affected = defect["candidate_id"] in quality_scope or any(ExecutionStore._load(db, "candidates", identity)[digest_key] == source[digest_key] for identity in quality_scope)
            if affected and defect["state"] != "closed":
                raise TaskflowError(f"unclosed responsibility defect: {defect['defect_id']}")
            if affected and defect.get("repair_candidate_id") not in ancestors:
                if defect.get("repair_candidate_id") not in quality_scope:
                    raise TaskflowError("defect was repaired only in a different/later revision")
                ExecutionStore._assert_repair_alias_current(db, defect, quality_scope, latest)
                alias_repair = True
            finding_time = db.execute("SELECT at FROM events WHERE seq=?", (defect["opened_seq"],)).fetchone()["at"]
            if affected and finding_time > latest_order:
                raise TaskflowError("new finding reopened affected quality conclusion")
        related_reviews = [_data(row) for row in db.execute("SELECT data FROM reviews") if _data(row)["candidate_id"] in quality_scope]
        if any(ExecutionStore._review_time(db, row) > latest_order and row["verdict"] != "accepted" for row in related_reviews):
            raise TaskflowError("later independent review supersedes accepted conclusion")
        if require_repair_alias and not alias_repair:
            raise TaskflowError("current quality has no closed repair through exact-byte aliases")
        return latest

    def quality_gate(self, candidate_id: str, *, require_repair_alias: bool = False) -> dict[str, Any]:
        with self._db() as db:
            return self._assert_quality(db, candidate_id, require_repair_alias=require_repair_alias)

    @staticmethod
    def _assert_experiment(db: sqlite3.Connection, candidate_id: str, purpose: str) -> dict[str, Any]:
        decisions = [_data(row) for row in db.execute("SELECT data FROM decisions ORDER BY rowid DESC")]
        for decision in decisions:
            if (decision["candidate_id"] == candidate_id
                    and decision["action"] == "experiment"
                    and decision["authorized"] is True
                    and decision["experiment_purpose"] == purpose):
                return decision
        raise TaskflowError("solver requires an authorized experiment decision for this candidate and exact purpose")

    def record_decision(self, decision_id: str, task_id: str, candidate_id: str, *, action: str, rationale: str, evidence: list[dict[str, Any]], experiment_purpose: str | None = None, authorized: bool = False) -> dict[str, Any]:
        validate_id(decision_id, "decision_id")
        if action not in DECISIONS:
            raise TaskflowError("unknown next investment action")
        _required_text(rationale, "rationale")
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            candidate = self._load(db, "candidates", candidate_id)
            if candidate["task_id"] != task_id:
                raise TaskflowError("decision task mismatch")
            if action == "experiment":
                self._assert_quality(db, candidate_id)
                _required_text(experiment_purpose, "experiment_purpose")
                if authorized is not True:
                    raise TaskflowError("experiment requires explicit authorization evidence")
            value = {"decision_id": decision_id, "task_id": task_id, "candidate_id": candidate_id, "action": action, "rationale": rationale, "evidence": refs, "experiment_purpose": experiment_purpose, "authorized": authorized}
            value["created_seq"] = self._event(db, "next_investment", value)
            db.execute("INSERT INTO decisions VALUES(?,?,?)", (decision_id, task_id, canonical(value).decode()))
            return value

    def get_attempt(self, attempt_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "attempts", attempt_id)

    def get_work(self, work_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "work", work_id)

    def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "candidates", candidate_id)

    def get_policy(self, policy_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "policies", policy_id)

    def get_review(self, review_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "reviews", review_id)

    def get_defect(self, defect_id: str) -> dict[str, Any]:
        with self._db() as db:
            return self._load(db, "defects", defect_id)

    @staticmethod
    def _flow_value(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            raise TaskflowError("unknown execution flow")
        return {
            "flow_id": row["flow_id"], "definition": json.loads(row["definition"]),
            "definition_digest": row["definition_digest"], "state": json.loads(row["state"]),
            "revision": row["revision"], "event_seq": row["event_seq"],
            "evidence": json.loads(row["evidence"]),
        }

    def register_flow(self, flow_id: str, definition: dict[str, Any],
                      evidence: list[dict[str, Any]],
                      initial_state: dict[str, Any] | None = None) -> dict[str, Any]:
        """Register one immutable flow definition; the caller owns business semantics."""
        validate_id(flow_id, "flow_id")
        if not isinstance(definition, dict):
            raise TaskflowError("flow definition: object required")
        if initial_state is None:
            initial_state = definition.get("initial_state", {})
        if not isinstance(initial_state, dict):
            raise TaskflowError("flow initial_state: object required")
        refs = validate_evidence(evidence)
        raw_definition = canonical(definition).decode()
        digest = sha256(raw_definition.encode())
        raw_state = canonical(initial_state).decode()
        policy_ids = definition.get("policy_ids", [])
        if not isinstance(policy_ids, list):
            raise TaskflowError("flow definition.policy_ids: list required")
        if "policy_id" in definition:
            policy_ids = [definition["policy_id"], *policy_ids]
        with self._db(True) as db:
            for policy_id in policy_ids:
                validate_id(policy_id, "flow policy reference")
                self._load(db, "policies", policy_id)
            event_seq = self._event(db, "flow_registered", {
                "flow_id": flow_id, "definition_digest": digest,
                "revision": 0, "state": initial_state, "evidence": refs,
            })
            db.execute("INSERT INTO execution_flows VALUES(?,?,?,?,?,?,?)", (
                flow_id, raw_definition, digest, raw_state, 0, event_seq,
                canonical(refs).decode(),
            ))
            return self._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (flow_id,)).fetchone())

    def get_flow(self, flow_id: str) -> dict[str, Any]:
        validate_id(flow_id, "flow_id")
        with self._db() as db:
            return self._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (flow_id,)).fetchone())

    def list_flows(self) -> list[dict[str, Any]]:
        with self._db() as db:
            return [self._flow_value(row) for row in db.execute("SELECT * FROM execution_flows ORDER BY rowid")]

    def update_flow(self, flow_id: str, expected_revision: int, state: dict[str, Any],
                    event: dict[str, Any]) -> dict[str, Any]:
        """CAS the SQLite state and append its event in the same transaction."""
        validate_id(flow_id, "flow_id")
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int) or expected_revision < 0:
            raise TaskflowError("flow expected_revision: nonnegative integer required")
        if not isinstance(state, dict) or not isinstance(event, dict):
            raise TaskflowError("flow state and event: objects required")
        _required_text(event.get("kind"), "flow event.kind")
        transition = dict(event)
        if "evidence" in transition:
            transition["evidence"] = validate_evidence(transition["evidence"])
        raw_state = canonical(state).decode()
        canonical(transition)
        with self._db(True) as db:
            current = self._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (flow_id,)).fetchone())
            if current["revision"] != expected_revision:
                raise TaskflowError(f"flow revision conflict: expected {expected_revision}, current {current['revision']}")
            if "accepted_review_id" in transition:
                review_id = validate_id(transition["accepted_review_id"], "flow accepted_review_id")
                accepted = self._assert_review_current(db, review_id)
                if state.get("candidate_id") != accepted["candidate_id"]:
                    raise TaskflowError("accepted flow state must name the review's exact candidate")
            sequence = self._event(db, "flow_updated", {
                "flow_id": flow_id, "previous_revision": expected_revision,
                "revision": expected_revision + 1, "state": state, "transition": transition,
            })
            cursor = db.execute("UPDATE execution_flows SET state=?,revision=?,event_seq=? WHERE flow_id=? AND revision=?", (raw_state, expected_revision + 1, sequence, flow_id, expected_revision))
            if cursor.rowcount != 1:
                raise TaskflowError("flow revision conflict; no state was overwritten")
            return self._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (flow_id,)).fetchone())

    def observe(self, task_id: str, source: dict[str, Any], observed_status: str,
                owner: str, observed_at: str, details: dict[str, Any]) -> dict[str, Any]:
        """Append a time-bound read-only observation without assuming ownership."""
        validate_id(task_id, "task_id")
        for key, value in (("observed_status", observed_status), ("owner", owner), ("observed_at", observed_at)):
            _required_text(value, key)
        if not isinstance(details, dict):
            raise TaskflowError("observation details: object required")
        effective, time_basis, time_issue = _observation_time({"observed_at": observed_at, "details": details})
        refs = validate_evidence([source])
        value = {"task_id": task_id, "source": refs[0], "observed_status": observed_status, "owner": owner, "observed_at": observed_at, "details": details, "mode": "observed", "effective_at": effective.isoformat(), "time_basis": time_basis, "time_issue": time_issue}
        with self._db(True) as db:
            value["event_seq"] = self._event(db, "observed", value)
            db.execute("INSERT INTO observations(task_id,data) VALUES(?,?)", (task_id, canonical(value).decode()))
        return value

    @staticmethod
    def _latest_observations(db: sqlite3.Connection) -> list[dict[str, Any]]:
        latest: dict[str, tuple[datetime, int, dict[str, Any]]] = {}
        for row in db.execute("SELECT seq,data FROM observations ORDER BY seq"):
            value = _data(row)
            try:
                instant, basis, issue = _observation_time(value)
            except TaskflowError:
                # Preserve old invalid records in history, but do not let an
                # unorderable timestamp displace a dated current observation.
                instant = datetime.min.replace(tzinfo=timezone.utc)
                basis, issue = "invalid_legacy_time", "legacy observation time is invalid"
            view = {**value, "effective_at": instant.isoformat(), "time_basis": basis, "time_issue": issue}
            candidate = (instant, int(value.get("event_seq", row["seq"])), view)
            previous = latest.get(value["task_id"])
            if previous is None or candidate[:2] > previous[:2]:
                latest[value["task_id"]] = candidate
        return [latest[task_id][2] for task_id in sorted(latest)]

    def transfer_owner(self, task_id: str, previous_owner: str, new_owner: str,
                       evidence: list[dict[str, Any]], scope: str = "future_dispatch",
                       work_id: str | None = None) -> dict[str, Any]:
        """Register a documented handoff; the receipt is an attributed judgment.

        This gate checks the receipt bytes and consistency with the current
        recorded owner. It does not impersonate the previous controller or
        prove a human statement's authenticity through hashes alone.
        """
        validate_id(task_id, "task_id")
        _required_text(previous_owner, "previous_owner")
        _required_text(new_owner, "new_owner")
        same_owner = previous_owner == new_owner
        if work_id is not None:
            validate_id(work_id, "work_id")
            scope = "work_item"
        if scope not in ("future_dispatch", "work_item") or (scope == "work_item" and not work_id):
            raise TaskflowError("ownership scope: future_dispatch or one explicit work_item")
        if same_owner and scope != "future_dispatch":
            raise TaskflowError("same owner registration is only for first observed-to-managed future dispatch")
        ownership_key = task_id if work_id is None else f"{task_id}::{work_id}"
        refs = validate_evidence(evidence)
        with self._db(True) as db:
            row = db.execute("SELECT data FROM ownership WHERE task_id=?", (ownership_key,)).fetchone()
            if same_owner and row is not None:
                raise TaskflowError("same owner cannot restamp existing managed dispatch ownership")
            if row is None and work_id is not None:
                # A newly delegated work item inherits the current task-level
                # dispatch owner, rather than an older observational owner.
                row = db.execute("SELECT data FROM ownership WHERE task_id=?", (task_id,)).fetchone()
            if row is not None:
                current = _data(row)["owner"]
            else:
                observed = next((value for value in self._latest_observations(db) if value["task_id"] == task_id), None)
                if observed is None:
                    raise TaskflowError("unknown prior owner observation")
                current = observed["owner"]
            if current != previous_owner:
                raise TaskflowError("previous_owner does not match current evidence")
            active = [_data(row) for row in db.execute("SELECT data FROM attempts WHERE task_id=? AND state IN ('claimed','starting','running','collecting','uncertain')", (task_id,))]
            if work_id is not None:
                active = [item for item in active if item["work_id"] == work_id]
            # A future-dispatch transfer leaves running attempts with their
            # original owner. A named work item cannot be transferred in flight.
            if active and scope == "work_item":
                raise TaskflowError("ownership handoff requires no unresolved managed attempt")
            value = {"task_id": task_id, "previous_owner": previous_owner, "owner": new_owner, "evidence": refs, "mode": "managed_work_item" if work_id else "managed_dispatch", "scope": scope, "work_id": work_id, "applies_to": "new_work_only", "transition_kind": "observed_to_managed_future_dispatch" if same_owner else "owner_transfer"}
            value["event_seq"] = self._event(db, "owner_transferred", value)
            db.execute("INSERT INTO ownership VALUES(?,?) ON CONFLICT(task_id) DO UPDATE SET data=excluded.data", (ownership_key, canonical(value).decode()))
        return value

    def events(self, after: int = 0) -> list[dict[str, Any]]:
        with self._db() as db:
            return [{"seq": row["seq"], "at": row["at"], "kind": row["kind"], "data": json.loads(row["data"])} for row in db.execute("SELECT * FROM events WHERE seq>? ORDER BY seq", (after,))]

    def dashboard(self) -> dict[str, Any]:
        with self._db() as db:
            candidates = [_data(row) for row in db.execute("SELECT data FROM candidates ORDER BY rowid")]
            work = [_data(row) for row in db.execute("SELECT data FROM work ORDER BY rowid")]
            attempts = [_data(row) for row in db.execute("SELECT data FROM attempts ORDER BY rowid")]
            decisions = [_data(row) for row in db.execute("SELECT data FROM decisions ORDER BY rowid")]
            observations = self._latest_observations(db)
            observation_history = [_data(row) for row in db.execute("SELECT data FROM observations ORDER BY seq")]
            ownership = [_data(row) for row in db.execute("SELECT data FROM ownership ORDER BY task_id")]
            quality = []
            for candidate in candidates:
                try:
                    review = self._assert_quality(db, candidate["candidate_id"])
                    quality.append({"candidate_id": candidate["candidate_id"], "state": "accepted_in_scope", "review_id": review["review_id"]})
                except TaskflowError as exc:
                    quality.append({"candidate_id": candidate["candidate_id"], "state": "unresolved", "reason": str(exc)})
            ownership_by_task = {item["task_id"]: item for item in ownership if item.get("scope") != "work_item"}
            observed_by_task = {item["task_id"]: item for item in observations}
            task_ids = sorted({item["task_id"] for item in candidates + observations + ownership})
            tasks = []
            for task_id in task_ids:
                task_work = [item for item in work if item["task_id"] == task_id]
                active = [item for item in attempts if item["task_id"] == task_id and item["state"] in ACTIVE]
                waiting = []
                for item in task_work:
                    if item["state"] != "ready":
                        continue
                    reason = "ready"
                    try:
                        self._check_work_ownership(db, item)
                    except TaskflowError as exc:
                        reason = str(exc)
                    if reason != "ready":
                        pass
                    elif self._blocked(db, item):
                        reason = "paused_or_cancelled_for_future_claims"
                    elif any(self._load(db, "work", dependency)["state"] != "completed" for dependency in item["dependencies"]):
                        reason = "dependency_not_completed"
                    elif item["modifies"] and any(attempt["modifies"] for attempt in active):
                        reason = "writer_owned_by_active_attempt"
                    elif item["role"] == "solver":
                        try:
                            self._assert_quality(db, item["candidate_id"])
                        except TaskflowError as exc:
                            reason = str(exc)
                    waiting.append({"work_id": item["work_id"], "role": item["role"], "reason": reason})
                observed = observed_by_task.get(task_id)
                owner = ownership_by_task.get(task_id)
                managed_items = [item for item in ownership if item["task_id"] == task_id and item.get("scope") == "work_item"]
                tasks.append({
                    "task_id": task_id,
                    "mode": "managed_dispatch" if owner else "managed_work_item" if managed_items else "observed" if observed else "local_unclaimed",
                    "owner": owner["owner"] if owner else observed["owner"] if observed else None,
                    "managed_work_items": managed_items,
                    "observation": observed,
                    "active_attempts": [{"attempt_id": item["attempt_id"], "state": item["state"], "role": item["role"], "runtime_path": item.get("runtime_path")} for item in active],
                    "waiting": waiting,
                    "latest_work_state": task_work[-1]["state"] if task_work else None,
                    "current_state": active[-1]["state"] if active else task_work[-1]["state"] if task_work else observed["observed_status"] if observed else "unknown",
                    "current_state_source": "managed_attempt" if active else "managed_work" if task_work else "observation" if observed else "unknown",
                    "next_role": waiting[0]["role"] if waiting else None,
                    "latest_decision": next((item for item in reversed(decisions) if item["task_id"] == task_id), None),
                })
            return {"schema_version": 1, "source": str(self.path), "tasks": tasks, "work": work, "attempts": attempts, "quality": quality, "decisions": decisions, "observations": observations, "observation_history": observation_history, "ownership": ownership, "platform_status": "unknown", "difficulty_status": "requires_valid_experiment_evidence"}
