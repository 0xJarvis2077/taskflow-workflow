"""Preauthorized quality/repair continuations on the existing execution queue.

Definitions are immutable, and SQLite owns the cursor.  This is a foreground
consumer of the existing workers, not a service or a second scheduler.  Business
outcomes come only from the collected independent role's signed report.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import copy
import fcntl
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import time
from typing import Any, Iterator

from taskflow.contracts import (
    PUBLIC_ROLES, TaskflowError, canonical, relative_path, sha256, validate_id,
)
from taskflow.execution_runner import _publish, _publish_bytes, _read, _regular
from taskflow.execution_store import ACTIVE, ExecutionStore, evidence_ref, validate_evidence


def _identity(flow_id: str, label: str) -> str:
    return flow_id[:48] + "." + label[:24] + "." + sha256(label.encode())[:12]


def _publish_once(path: Path, value: Any) -> None:
    expected = canonical(value) + b"\n"
    if path.exists():
        if _regular(path) != expected:
            raise TaskflowError("published continuation evidence differs: " + str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        _publish(path, value)
    except FileExistsError:
        if _regular(path) != expected:
            raise TaskflowError("concurrent continuation evidence differs: " + str(path))


def _render(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {_render(key, context): _render(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [_render(item, context) for item in value]
    if not isinstance(value, str):
        return value

    def replacement(match: re.Match) -> str:
        name = match.group(1)
        if name not in context:
            raise TaskflowError(f"unbound workflow value: {name}")
        return str(context[name])

    return re.sub(r"\{\{([a-z_]+)\}\}", replacement, value)


@contextmanager
def _consumer_lock(store: ExecutionStore) -> Iterator[None]:
    # This OS mutex only excludes simultaneous consumers. Persistent decisions
    # and launch uncertainty stay in SQLite and the existing attempt receipts.
    path = store.path.with_name(store.path.name + ".continuation.lock")
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskflowError("an execution continuation consumer is already active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def register_workflow(store: ExecutionStore, definition: dict, evidence: list[dict]) -> dict:
    """Register one authorized loop; registration itself never calls a model."""
    if not isinstance(definition, dict) or definition.get("schema_version") != 1:
        raise TaskflowError("workflow schema_version=1 required")
    for key in ("flow_id", "task_id", "owner", "policy_id"):
        validate_id(definition.get(key), key)
    refs = validate_evidence(evidence)
    initial = definition.get("initial")
    roles = definition.get("roles")
    if not isinstance(initial, dict) or not isinstance(roles, dict):
        raise TaskflowError("workflow initial and roles objects required")
    excluded = initial.get("excluded_review_sids", [])
    if (not isinstance(excluded, list) or any(not isinstance(sid, str) or not sid for sid in excluded)
            or len(excluded) != len(set(excluded))):
        raise TaskflowError("excluded_review_sids requires unique nonempty identity strings")
    if not initial.get("candidate_root") or not initial.get("candidate_id"):
        raise TaskflowError("workflow initial source and stable candidate identity required")
    if not Path(definition.get("directory", "")).is_absolute() or not definition.get("accepted_use"):
        raise TaskflowError("workflow evidence directory and bounded accepted use required")
    directory, source = Path(definition["directory"]), Path(initial["candidate_root"])
    if directory.is_relative_to(source) or source.is_relative_to(directory):
        raise TaskflowError("workflow evidence and original candidate must not overlap")
    if "quality" not in roles or "repair" not in roles or "recheck" not in roles:
        raise TaskflowError("authorized quality, repair and recheck definitions required")
    for name, role in roles.items():
        if name not in ("quality", "repair", "recheck", "recheck_public", "public_rebuild"):
            raise TaskflowError("unknown workflow role")
        if not isinstance(role, dict) or not isinstance(role.get("runner"), dict):
            raise TaskflowError("each workflow role needs an existing runner template")
        expected = "repair" if name == "repair" else "quality"
        if role["runner"].get("role") != expected:
            raise TaskflowError("workflow runner role mismatch")
    store.get_policy(definition["policy_id"])
    if initial.get("public_predecessor_reuse") is not None and not initial.get("predecessor_attempt"):
        raise TaskflowError("public predecessor reuse requires its exact original attempt")
    if initial.get("predecessor_attempt"):
        _predecessor(store, definition)
    state = {"status": "active", "stage": "quality", "step": 0,
             "candidate_id": initial["candidate_id"], "candidate_root": initial["candidate_root"],
             "initialized": False, "current_work_id": None, "history": [],
             "defects": [], "repaired_defects": [], "seen_findings": [], "archives": []}
    record = _get_or_none(store.get_flow, definition["flow_id"])
    if record is None:
        record = store.register_flow(definition["flow_id"], definition, refs, state)
    elif record["definition_digest"] != sha256(canonical(definition)) or record["evidence"] != refs:
        raise TaskflowError("workflow registration cannot replace its original definition/evidence")
    # A controller moving its own previously observed task to the unified entry
    # needs a single future-dispatch receipt, not a human handoff at every node.
    dispatch = store.dispatch_ownership_snapshot(definition["task_id"])
    general = dispatch["ownership"]
    if general is None:
        observed = dispatch["observation"]
        if observed is not None:
            store.transfer_owner(definition["task_id"], observed["owner"], definition["owner"],
                                 refs, scope="future_dispatch")
    elif general["owner"] != definition["owner"]:
        raise TaskflowError("workflow owner differs from existing future dispatch owner")
    return record


def _save(store: ExecutionStore, record: dict, state: dict, kind: str) -> dict:
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": kind})


def _completed_repair_identity(store: ExecutionStore, record: dict, work_id: str) -> dict:
    """Bind the one original alias rejection to its node, delivery and sources."""
    from taskflow.execution_flow_contracts import _ids, validate_completed_repair_identity

    definition, state = record["definition"], record["state"]
    attempts = store.list_attempts(work_id)
    if len(attempts) != 1 or state.get("pending_business_scope"):
        raise TaskflowError("repair identity needs one original attempt and no business hold")
    attempt = attempts[0]
    runtime = Path(attempt["runtime_path"])
    label = runtime.parent.name
    node_id = _identity(record["flow_id"], label)
    work = store.get_work(work_id)
    if _regular(runtime.parent / "current.json") != _regular(runtime / "inputs/execution/current.json"):
        raise TaskflowError("repair identity context differs from its original locked node input")
    context = _read(runtime.parent / "current.json")
    parent = store.get_candidate(attempt["candidate_id"])
    candidate_id = _identity(record["flow_id"], node_id + "-candidate")
    candidate = store.get_candidate(candidate_id)
    source = runtime / "artifacts" / relative_path(definition["roles"]["repair"]["candidate_subpath"])
    archive = Path(definition["directory"]) / "archives" / attempt["attempt_id"]
    if (not label.endswith("-repair") or work_id != node_id
            or runtime != Path(definition["directory"]) / "nodes" / label / "runtime"
            or work.get("role") != "repair" or work.get("state") != "completed"
            or any(work.get(key) != attempt.get(key) for key in
                   ("intent_id", "task_id", "candidate_id", "runtime_path", "request_digest"))
            or attempt["intent_id"] != node_id or attempt["task_id"] != definition["task_id"]
            or any(context.get(key) != value for key, value in _binding(parent).items())
            or context.get("node_id") != node_id or context.get("intent_id") != node_id
            or context.get("revised_candidate_id") != candidate_id or context.get("runtime") != str(runtime)
            or state.get("candidate_id") not in {parent["candidate_id"], candidate_id}
            or state.get("candidate_root") != (context["candidate_root"] if state["candidate_id"] == parent["candidate_id"] else str(source))
            or str(archive) not in state.get("archives", [])):
        raise TaskflowError("repair identity node, candidate lineage or original archive differs")
    ids = [row["defect_id"] for row in state["defects"]]
    if (set(_ids(context.get("defect_ids"), "repair node defect_ids", nonempty=True)) != set(ids)
            or context.get("defects") != state["defects"]):
        raise TaskflowError("repair identity assigned node defects differ")
    for issue in state["defects"]:
        defect = store.get_defect(issue["defect_id"])
        if (defect["candidate_id"] != parent["candidate_id"]
                or any(defect.get(key) != issue.get(key) for key in ("responsibility", "kind", "evidence"))
                or defect.get("review_id") != issue.get("source_review_id")
                or defect["state"] not in {"open", "repaired", "closed"}
                or (defect["state"] != "open" and (defect.get("repair_candidate_id") != candidate_id
                    or defect.get("author_attempt") != attempt["attempt_id"]))):
            raise TaskflowError("repair identity responsibility or actual repair binding differs")
    return validate_completed_repair_identity(
        runtime / "artifacts" / definition["roles"]["repair"].get("repair_file", "repair-record.json"),
        attempt=attempt, parent=parent, candidate=candidate,
        parent_root=Path(context["candidate_root"]), candidate_root=source, archive=archive, defect_ids=ids)


def resume_workflow(store: ExecutionStore, flow_id: str, *, reason: str, evidence: list[dict],
                    repair_record_projection: dict | None = None) -> dict:
    """Resolve an exception explicitly, retaining every original and launch identity.

    This authorizes one replacement zero-model preflight, or consumption of an
    already completed attempt. It never authorizes a replacement model attempt.
    """
    from taskflow.execution import reconcile

    if not isinstance(reason, str) or not reason.strip():
        raise TaskflowError("workflow resolution needs a nonempty reason")
    refs = validate_evidence(evidence)
    resolution = {"reason": reason, "evidence": refs}
    projection_refs = []
    if repair_record_projection is not None:
        if not isinstance(repair_record_projection, dict) or set(repair_record_projection) != {"original", "projected"}:
            raise TaskflowError("repair_record_projection requires exactly original and projected references")
        projection_refs = validate_evidence([repair_record_projection[key] for key in ("original", "projected")])
        repair_record_projection = dict(zip(("original", "projected"), projection_refs))
        resolution["repair_record_projection"] = repair_record_projection
    resolution_digest = sha256(canonical(resolution))
    evidence_digest = sha256(canonical(sorted({item["sha256"] for item in refs + projection_refs})))
    with _consumer_lock(store):
        record = store.get_flow(flow_id)
        state = dict(record["state"])
        previous = state.get("last_resolution", {})
        if previous.get("digest") == resolution_digest:
            if state["status"] == "active":
                return record
            raise TaskflowError("this resolution was already consumed; a new exception needs new evidence")
        if state["status"] != "exception":
            raise TaskflowError("only an exception can be explicitly resumed")
        if any(item.get("evidence_digest") == evidence_digest for item in state.get("resolved_exceptions", [])):
            raise TaskflowError("previous resolution evidence cannot authorize another exception")
        definition = record["definition"]
        from taskflow.execution_quality_reading import layout, validate_history

        reading = layout(record)
        label = f"{state['step']}-{state['stage']}"
        if reading is not None:
            label = reading["label"]
        node_id = _identity(flow_id, label)
        work_id = (definition.get("first_work_id") if state["step"] == 0 else None) or node_id
        runtime = (reading["root"] if reading is not None else Path(definition["directory"]) / "nodes" / label) / "runtime"
        if state.get("current_work_id") not in (None, work_id):
            raise TaskflowError("exception current work differs from the immutable workflow node")
        work = _get_or_none(store.get_work, work_id)
        if work and (work["intent_id"] != node_id or work["candidate_id"] != state["candidate_id"]
                     or work["runtime_path"] != str(runtime)):
            raise TaskflowError("exception node has a different launch binding")
        attempts = store.list_attempts(work_id)
        if reading is not None:
            validate_history(store, record)
            if not attempts:
                if (work is not None and work["state"] != "ready"
                        or state.get("current_node_id") not in (None, node_id)
                        or state.get("current_runtime") not in (None, str(runtime))
                        or state.get("exception", {}).get("work_id") not in (None, work_id)):
                    raise TaskflowError("serial quality resume lost its zero-model preflight binding")
            else:
                from taskflow.execution_runner import _info, _validated_collection

                if (state.get("current_node_id") != node_id or state.get("current_runtime") != str(runtime)
                        or state.get("current_work_id") != work_id or work is None
                        or state.get("exception", {}).get("work_id") != work_id
                        or len(attempts) != 1 or attempts[0]["state"] != "completed"):
                    raise TaskflowError("serial quality resume requires the exact completed original turn")
                original = attempts[0]
                manifest = _read(runtime / "manifest.json")
                session = manifest.get("quality_session", {})
                collected = _validated_collection(runtime, manifest)
                expected_previous = state["serial_quality_reading"]["turns"][-1]["seal"] if reading["index"] else None
                if (_read(runtime / "manifest-lock.json") != _info(runtime / "manifest.json")
                        or original["runtime_path"] != str(runtime) or original["role"] != "quality"
                        or original["candidate_id"] != state["candidate_id"] or not original["cli_sid"]
                        or manifest.get("intent_id") != node_id or manifest.get("role") != "quality"
                        or manifest.get("candidate_binding") != state["serial_quality_reading"]["candidate_binding"]
                        or session.get("flow_id") != flow_id or session.get("index") != reading["index"]
                        or session.get("phase") != reading["phase"] or session.get("plan") != reading["reference"]
                        or session.get("previous") != expected_previous
                        or collected.get("sid") != original["cli_sid"] or collected.get("status") != "completed"
                        or collected.get("technical_accepted") is not True):
                    raise TaskflowError("serial quality resume lost its locked manifest, collection or cursor binding")
        projection = None
        identity_proof = None
        if state.get("exception", {}).get("reason") == "repair defect_ids: list required":
            if (repair_record_projection is not None or state["stage"] != "repair"
                    or state.get("current_node_id") != node_id or state.get("current_work_id") != work_id
                    or state.get("exception") != {"reason": "repair defect_ids: list required", "work_id": work_id}
                    or not state.get("receipt")):
                raise TaskflowError("repair identity resume requires the exact original completed repair exception")
            received = _read(Path(state["receipt"]))
            if (received.get("flow_id") != flow_id or received.get("status") != "exception"
                    or received.get("exception") != state["exception"]
                    or received.get("candidate_id") != state["candidate_id"]
                    or received.get("history") != state["history"]):
                raise TaskflowError("repair identity original exception receipt differs")
            identity_proof = _completed_repair_identity(store, record, work_id)
        if repair_record_projection is not None:
            from taskflow.execution_flow_contracts import resolve_repair_record_projection

            if (state["stage"] != "repair" or state.get("current_node_id") != node_id
                    or state.get("exception", {}).get("reason") != "Repair changes duplicate or do not reflect actual differences"
                    or len(attempts) != 1 or attempts[0]["state"] != "completed"):
                raise TaskflowError("repair projection requires this exact completed repair and original diff exception")
            parent = store.get_candidate(state["candidate_id"])
            candidate = store.get_candidate(_identity(flow_id, node_id + "-candidate"))
            projection = resolve_repair_record_projection(
                repair_record_projection, attempt=attempts[0], parent=parent, candidate=candidate,
                defect_ids=[item["defect_id"] for item in state["defects"]])
        for attempt in attempts:
            if attempt["state"] in ACTIVE:
                reconcile(store, attempt["attempt_id"], resume_pending=False)
                attempt = store.get_attempt(attempt["attempt_id"])
            adopted_repair = (state["stage"] == "repair" and attempt["state"] == "failed"
                              and (attempt.get("delivery_resolution") or {}).get("status") == "accepted_auxiliary_omission")
            if adopted_repair:
                store._validated_delivery_resolution(attempt)
            if attempt["state"] != "completed" and not adopted_repair:
                raise TaskflowError("resume cannot retry a failed or uncertain model attempt: " + attempt["attempt_id"])
        if work and not attempts and work["state"] != "ready":
            raise TaskflowError("exception work is not an unclaimed ready intent")
        reports = sorted(runtime.glob("native-probe-*.json")) if runtime.exists() else []
        failed_reports = []
        if not (runtime / "native-probe-ready.json").exists():
            for path in reports:
                raw = _regular(path)
                if json.loads(raw).get("passed") is True:
                    raise TaskflowError("successful probe without ready seal needs reconciliation")
                failed_reports.append({"path": str(path), "sha256": sha256(raw)})
        resolved = {"exception": state.get("exception"), "receipt": state.get("receipt"),
                    "reason": reason, "evidence": refs, "revision": record["revision"],
                    "probe_reports": failed_reports, "evidence_digest": evidence_digest}
        if reading is not None:
            resolved["serial_quality_node"] = {
                "plan": reading["reference"], "index": reading["index"], "phase": reading["phase"],
                "node_id": node_id, "work_id": work_id, "runtime": str(runtime),
                "attempt_id": attempts[0]["attempt_id"] if attempts else None,
                "action": "consume_completed_original" if attempts else "retry_zero_model_preflight"}
        if identity_proof is not None:
            resolved["repair_defect_identity"] = identity_proof
            existing = state.get("repair_defect_identities", {}).get(identity_proof["attempt_id"])
            if existing is not None and existing != identity_proof:
                raise TaskflowError("repair identity cannot replace its already bound original")
            state["repair_defect_identities"] = {**state.get("repair_defect_identities", {}),
                                                 identity_proof["attempt_id"]: identity_proof}
        if projection is not None:
            resolved["repair_record_projection"] = projection
            projections = dict(state.get("repair_record_projections", {}))
            existing = projections.get(projection["attempt_id"])
            if existing is not None and existing != projection:
                raise TaskflowError("repair projection cannot replace an already bound original")
            projections[projection["attempt_id"]] = projection
            state["repair_record_projections"] = projections
        state["resolved_exceptions"] = [*state.get("resolved_exceptions", []), resolved]
        authorizations = dict(state.get("probe_retry_authorizations", {}))
        if failed_reports:
            authorizations[str(runtime)] = sorted(item["sha256"] for item in failed_reports)
        state.update(status="active", receipt_pending=False,
                     probe_retry_authorizations=authorizations,
                     last_resolution={"digest": resolution_digest, **resolution})
        state.pop("exception", None)
        state.pop("receipt", None)
        return store.update_flow(flow_id, record["revision"], state,
                                 {"kind": "workflow_exception_resolved", **resolved})


def _get_or_none(method: Any, identity: str) -> dict | None:
    try:
        return method(identity)
    except TaskflowError as exc:
        if "unknown" not in str(exc) and "not found" not in str(exc):
            raise
        return None


def _binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in (
        "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest"
    )}


def _predecessor(store: ExecutionStore, definition: dict) -> tuple[dict, dict]:
    """Check the external worker's immutable intent without recovering its process."""
    initial = definition["initial"]
    attempt = store.get_attempt(initial["predecessor_attempt"])
    work = store.get_work(attempt["work_id"])
    candidate = store.get_candidate(initial["candidate_id"])
    source_candidate = candidate
    if initial.get("public_predecessor_reuse") is not None:
        from taskflow.execution_public_predecessor import validate_public_reuse

        source_candidate = validate_public_reuse(store, definition, attempt, candidate)
    declared = initial.get("predecessor_binding")
    identity = {key: attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
    if declared is not None and declared != identity:
        raise TaskflowError("workflow predecessor declared intent binding differs")
    if (attempt["task_id"] != definition["task_id"]
            or attempt["candidate_id"] != source_candidate["candidate_id"]
            or candidate["task_id"] != definition["task_id"]):
        raise TaskflowError("workflow predecessor task/candidate binding differs")
    for key in ("task_id", "candidate_id", "intent_id", "runtime_path", "policy_id", "request_digest"):
        if work[key] != attempt[key]:
            raise TaskflowError("workflow predecessor work/attempt binding differs: " + key)
    for key in ("input_digest", "content_digest"):
        if work[key] != source_candidate[key] or attempt[key] != source_candidate[key]:
            raise TaskflowError("workflow predecessor candidate version differs: " + key)
    for key, digest in initial.get("expected_digests", {}).items():
        if candidate[key] != digest:
            raise TaskflowError("workflow predecessor expected version differs: " + key)
    first_work = definition.get("first_work_id") or _identity(definition["flow_id"], "0-quality")
    if first_work == attempt["work_id"]:
        raise TaskflowError("workflow successor cannot reuse the predecessor work item")
    root = Path(attempt["runtime_path"])
    raw = _regular(root / "manifest.json")
    if _read(root / "manifest-lock.json") != {"sha256": sha256(raw), "size": len(raw)}:
        raise TaskflowError("workflow predecessor manifest is not sealed")
    manifest = json.loads(raw)
    if (manifest.get("candidate_binding") != _binding(source_candidate)
            or manifest.get("intent_id") != attempt["intent_id"]
            or manifest.get("policy_version") != attempt["policy_id"]
            or manifest.get("role") != attempt["role"]
            or manifest.get("request", {}).get("sha256") != attempt["request_digest"]):
        raise TaskflowError("workflow predecessor runtime intent/version binding differs")
    return attempt, manifest


def _wait_predecessor(store: ExecutionStore, record: dict, attempt: dict, reason: str) -> dict:
    state = dict(record["state"])
    waiting = {key: attempt[key] for key in ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id")}
    waiting.update(state=attempt["state"], reason=reason)
    if state.get("waiting_predecessor") == waiting:
        return record
    state["waiting_predecessor"] = waiting
    return _save(store, record, state, "workflow_waiting_predecessor")


def _initialize(store: ExecutionStore, record: dict) -> dict:
    from taskflow.execution import capture_candidate
    from taskflow.execution_runner import archive_full_attempt
    from taskflow.packages import check_task

    definition, state = record["definition"], dict(record["state"])
    initial = definition["initial"]
    predecessor = initial.get("predecessor_attempt")
    attempt = None
    predecessor_sid = None
    if predecessor:
        attempt, manifest = _predecessor(store, definition)
        if attempt["state"] in ("claimed", "starting", "running", "collecting"):
            return _wait_predecessor(store, record, attempt, "original_worker_owns_completion")
        if attempt["state"] != "completed":
            raise TaskflowError("workflow predecessor failed/uncertain/cancelled: " + attempt["state"])
        root = Path(attempt["runtime_path"])
        if not (root / "collection.json").exists() or not (root / "collection-lock.json").exists():
            return _wait_predecessor(store, record, attempt, "collection_not_sealed")
        raw = _regular(root / "collection.json")
        if _read(root / "collection-lock.json") != {"sha256": sha256(raw), "size": len(raw)}:
            raise TaskflowError("workflow predecessor collection seal differs")
        collection = json.loads(raw)
        if (collection.get("intent_id") != manifest["intent_id"]
                or not isinstance(collection.get("evidence_files"), dict)):
            raise TaskflowError("workflow predecessor collection belongs to a different intent")
        predecessor_sid = collection.get("sid")
        if not predecessor_sid or (attempt.get("cli_sid") and attempt["cli_sid"] != predecessor_sid):
            raise TaskflowError("workflow predecessor completed CLI identity is absent or differs")
    if initial.get("evidence"):
        validate_evidence(initial["evidence"])
    actual = check_task(Path(initial["candidate_root"]), write_report=False)
    if actual["errors"]:
        raise TaskflowError("initial workflow package has structural errors")
    for key, digest in initial.get("expected_digests", {}).items():
        if actual[key] != digest:
            raise TaskflowError("initial workflow source version mismatch: " + key)
    candidate = _get_or_none(store.get_candidate, initial["candidate_id"])
    if candidate is None:
        candidate = capture_candidate(
            store, Path(initial["candidate_root"]), initial["candidate_id"],
            external_author_sids=initial.get("external_author_sids"),
            external_author_traces=[Path(name) for name in initial.get("external_author_traces", [])],
        )
    if any(candidate[key] != actual[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("registered candidate differs from actual workflow source")
    if predecessor:
        # Recheck after inspecting the package, before adopting any original.
        current, _ = _predecessor(store, definition)
        if current != attempt:
            raise TaskflowError("workflow predecessor changed during initialization")
        destination = Path(definition["directory"]) / "archives" / predecessor
        archived = archive_full_attempt(Path(attempt["runtime_path"]), destination)
        state["archives"] = [archived["path"]]
        state["predecessor_archive"] = archived["path"]
        state["public_archive"] = archived["path"]
        state["history"] = [{"kind": "adopted_predecessor", "attempt_id": predecessor,
                             "cli_sid": predecessor_sid, "archive": archived["path"],
                             "business_verdict": "not_inferred"}]
        if initial.get("public_predecessor_reuse") is not None:
            state["history"][0]["public_predecessor_reuse"] = validate_evidence([initial["public_predecessor_reuse"]])[0]
            state["history"][0]["target_candidate_binding"] = _binding(candidate)
            state["history"][0]["quality_transfer"] = False
    state["initialized"] = True
    state.pop("waiting_predecessor", None)
    return _save(store, record, state, "workflow_source_bound")


def _public_inner_runtime(role: dict, context: dict) -> Path:
    """Resolve one unprepared public adapter without moving prior evidence."""
    legacy = Path(context["runtime"]).absolute().with_name(Path(context["runtime"]).name + ".public")
    configuration = _render(role["prepare"]["config"], context)
    repository = Path(configuration["repository"]).resolve()
    resolved = legacy.resolve()
    if not resolved.is_relative_to(repository) and not repository.is_relative_to(resolved):
        return legacy
    if legacy.exists() or legacy.is_symlink():
        raise TaskflowError("repository public runtime already has evidence; preserve it instead of relocating")
    identity = {"outer_runtime": str(Path(context["runtime"]).absolute()), "intent_id": context["intent_id"],
                "candidate_binding": {key: context[key] for key in
                                      ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}}
    external = Path("/tmp").resolve() / ("tf-public-" + sha256(canonical(identity))[:24])
    if external.is_relative_to(repository) or repository.is_relative_to(external):
        raise TaskflowError("configured repository overlaps the external short public runtime")
    return external


def _public_prepare(role: dict, context: dict) -> dict:
    """Use the existing approved, hash-bound public adapter; never a new solver."""
    setup = role["prepare"]
    source = Path(setup["adapter"])
    if sha256(_regular(source)) != setup["sha256"]:
        raise TaskflowError("public preparation adapter changed")
    spec = importlib.util.spec_from_file_location("taskflow_bound_public_preparer", source)
    if spec is None or spec.loader is None:
        raise TaskflowError("public preparation adapter unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    inner = _public_inner_runtime(role, context)
    if not (inner / "plan.json").exists():
        module.prepare_draft(Path(context["candidate_root"]), inner,
                             _render(setup["config"], context),
                             denied=[Path(name) for name in context.get("private_paths", [])],
                             timeout=role["runner"]["timeout_seconds"])
    plan = _read(inner / "plan.json")
    if (plan["source_binding"]["input_digest"] != context["input_digest"]
            or plan["model"]["requested"] != role["runner"]["model"]
            or plan["model"]["reasoning_effort"] != role["runner"]["reasoning_effort"]):
        raise TaskflowError("prepared public runtime belongs to a different candidate/configuration")
    if not (inner / "ready.json").exists():
        module.probe(inner)
    module.validate(inner)
    # Current tool/context checks are zero-inference, hash-bound existing local
    # helpers. Their exact report predicates are registered by the controller.
    for index, check in enumerate(setup.get("checks", [])):
        helper = Path(check["source"])
        if sha256(_regular(helper)) != check["sha256"]:
            raise TaskflowError("registered public tool/context checker changed")
        output = inner / ("continuation-check-" + str(index))
        if not (output / "report.json").exists():
            subprocess.run([setup["launcher_python"], str(helper), str(inner), str(output)],
                           check=True, timeout=90, capture_output=True)
        report = _read(output / "report.json")
        if report.get("returncode") != 0 or report.get("private_unchanged") is not True:
            raise TaskflowError("current public tool/context check failed")
        requests = report.get("requests", [])
        if not requests:
            raise TaskflowError("current public context capture missing")
        body = requests[0]["body"]
        if body.get("model") != role["runner"]["model"]:
            raise TaskflowError("public context requested model mismatch")
        if "<skills_instructions>" in json.dumps(body.get("input", [])):
            raise TaskflowError("public context contains personal skill catalog")
        node = next((item for item in _json_objects(requests[1:])
                     if item.get("case") == "default_node_actual_tool"), None)
        package_hash = sha256(_regular(inner / "work/package.json"))
        if (not node or node.get("openssl_conf") is not None or node.get("private_denied") is not True
                or node.get("deny_code") not in ("EPERM", "EACCES") or node.get("own_write") is not True
                or node.get("package_sha256") != package_hash):
            raise TaskflowError("actual default Node/private-boundary tool result missing or failed")
    return {"runtime": str(inner), "flavor": "quality", "python": setup["launcher_python"]}


def _json_objects(value: Any, depth: int = 0) -> Iterator[dict]:
    if depth > 12:
        return
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _json_objects(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            yield from _json_objects(item, depth + 1)
    elif isinstance(value, str):
        for line in value.splitlines():
            try:
                decoded = json.loads(line)
            except ValueError:
                continue
            if not isinstance(decoded, str):
                yield from _json_objects(decoded, depth + 1)


def _review_files(candidate: dict, *, public_only: bool = False) -> dict:
    """Read actual declared bytes against their captured identity and role."""
    from taskflow.execution_runner import _info

    result = {}
    for row in candidate.get("files", []):
        if public_only and row["role"] not in PUBLIC_ROLES:
            continue
        name = relative_path(row["package_path"])
        if name in result:
            raise TaskflowError("review input candidate has duplicate declared paths: " + name)
        observed = _info(Path(row["path"]))
        if observed["sha256"] != row["sha256"] or ("size" in row and observed["size"] != row["size"]):
            raise TaskflowError("review input candidate bytes drifted from captured source: " + name)
        result[name] = {"path": name, "role": row["role"], "source": row["path"], **observed}
    return result


def _review_input_protocol(store: ExecutionStore, candidate: dict, root: Path,
                           *, whole: bool) -> tuple[dict, list[dict]]:
    """Publish neutral process metadata, never a new permission or business gate."""
    if whole:
        public = _review_files(candidate, public_only=True)
        if not public:
            raise TaskflowError("public-first review requires actual captured public inputs")
        index = {"schema_version": 1, "input_digest": candidate["input_digest"],
                 "files": [{key: row[key] for key in ("path", "role", "sha256", "size")}
                           for name, row in sorted(public.items())]}
        path = root / "public-inputs.json"
        _publish_once(path, index)
        return {"public_first": {"index": "execution/public-inputs.json",
                                 "checkpoint": "public-responsibility-first.json",
                                 "input_digest": candidate["input_digest"]}}, [
            {"path": str(path), "kind": "source_metadata", "target": "execution/public-inputs.json"}]
    current = _review_files(candidate)
    parent_id = candidate.get("parent_id")
    parent = _get_or_none(store.get_candidate, parent_id) if parent_id else None
    index = {"schema_version": 1, "available": False, "parent_candidate_id": parent_id,
             "candidate_id": candidate["candidate_id"], "parent_binding": _binding(parent) if parent else None,
             "current_binding": _binding(candidate), "changed": [],
             "reason": "parent_candidate_unavailable" if parent_id else "no_parent_candidate"}
    historical = []
    if parent is not None and parent["task_id"] != candidate["task_id"]:
        raise TaskflowError("affected review parent belongs to another task")
    if parent is not None and not parent.get("files"):
        index["reason"] = "parent_source_unavailable"
    elif parent is not None:
        try:
            before = _review_files(parent)
        except (FileNotFoundError, PermissionError):
            index["reason"] = "parent_source_unavailable"
        else:
            index.update(available=True, reason=None)
            for name in sorted(set(before) | set(current)):
                old, new = before.get(name), current.get(name)
                if (old is not None and new is not None
                        and (old["sha256"], old["role"]) == (new["sha256"], new["role"])):
                    continue
                target = "execution/parent-inputs/" + name if old else None
                snapshot = root / "parent-inputs" / name if old else None
                if old:
                    raw = _regular(Path(old["source"]))
                    if sha256(raw) != old["sha256"] or len(raw) != old["size"]:
                        raise TaskflowError("historical parent bytes changed while capturing: " + name)
                    if snapshot.exists():
                        if _regular(snapshot) != raw:
                            raise TaskflowError("historical parent snapshot cannot be replaced: " + name)
                    else:
                        _publish_bytes(snapshot, raw)
                index["changed"].append({"path": name,
                    "old_sha256": old["sha256"] if old else None, "new_sha256": new["sha256"] if new else None,
                    "old_role": old["role"] if old else None, "new_role": new["role"] if new else None,
                    "old_size": old["size"] if old else None, "new_size": new["size"] if new else None,
                    "before_source": old["source"] if old else None, "current_source": new["source"] if new else None,
                    "before_input_source": str(snapshot) if snapshot else None,
                    "before_staged_path": target, "current_staged_path": name if new else None})
                if old:
                    historical.append({"path": str(snapshot), "kind": "history", "target": target})
    path = root / "repair-diff.json"
    _publish_once(path, index)
    return {"repair_diff": {"index": "execution/repair-diff.json", "available": index["available"],
                             "parent_candidate_id": parent_id, "candidate_id": candidate["candidate_id"],
                             "reason": index["reason"]}}, [
        {"path": str(path), "kind": "source_metadata", "target": "execution/repair-diff.json"}, *historical]


def _target_preflight_context(candidate, state, inputs, work):
    """Describe the same bound predecessor aliases for a read-only self-check."""
    archive_name = state.get("public_archive")
    candidate_paths = set(_review_files(candidate))
    observed_paths, source_errors = [], []
    if archive_name:
        archive = Path(archive_name)
        if archive_name not in state.get("archives", []):
            source_errors.append("public observation archive is outside the recorded provenance")
        else:
            try:
                predecessor = _read(archive / "manifest.json").get("candidate_binding", {})
                if any(predecessor.get(key) != candidate[key] for key in ("task_id", "input_digest")):
                    source_errors.append("public observation archive belongs to another public candidate")
            except (OSError, ValueError, TaskflowError) as error:
                source_errors.append("public observation archive manifest is unavailable: " + str(error))
        seen_targets = set()
        for item in inputs:
            if item.get("kind") not in ("submission", "trace"):
                continue
            target = item.get("target", "")
            source = Path(item["path"])
            if not source.is_relative_to(archive):
                source_errors.append("public observation source is outside the exact public archive: " + target)
            elif target in seen_targets:
                source_errors.append("ambiguous public observation alias: " + target)
            elif target in candidate_paths or not ((item["kind"] == "submission" and target.startswith(
                    ("actual-public-work/", "actual-public-scratch/")))
                    or (item["kind"] == "trace" and target == "actual-public-trace.jsonl")):
                source_errors.append("Unsupported public-predecessor observation alias: " + target)
            else:
                try:
                    _regular(source)
                except (OSError, TaskflowError) as error:
                    source_errors.append("public observation source cannot be read safely: " + target + ": " + str(error))
                else:
                    observed_paths.append(target)
            seen_targets.add(target)
    targets = {"candidate_paths": sorted(candidate_paths), "observed_paths": observed_paths,
               "source_errors": source_errors, "work": str(work)}
    return targets


def _prepare_node(store: ExecutionStore, record: dict) -> dict:
    proof = record["state"].get("repair_capacity_continuation")
    if (proof is not None and record["state"].get("stage") == "repair"
            and record["state"].get("step") == proof["initial_step"] + record["state"].get("repair_capacity_attempt_ordinal", 0)):
        from taskflow.execution_repair_capacity import capacity_lifecycle_locks, validate_repair_capacity_before_dispatch

        with capacity_lifecycle_locks(store, proof):
            current = store.get_flow(record["flow_id"])
            if current != record:
                raise TaskflowError("repair continuation changed before preparation")
            validate_repair_capacity_before_dispatch(store, record)
            return _prepare_node_checked(store, record)
    return _prepare_node_checked(store, record)


def _prepare_node_checked(store: ExecutionStore, record: dict) -> dict:
    from taskflow.execution import enqueue_spec
    from taskflow.execution_batch import policy_setting, validate_batch_policy
    from taskflow.execution_flow_contracts import public_first_instructions, repair_instructions, review_instructions

    definition, state = record["definition"], dict(record["state"])
    stage, number = state["stage"], state["step"]
    role = copy.deepcopy(definition["roles"][stage])
    candidate = store.get_candidate(state["candidate_id"])
    policy = store.get_policy(definition["policy_id"])["config"]
    snapshot = validate_batch_policy(policy)
    slot = role["runner"].get("model_slot")
    setting = policy_setting(policy, "repair" if stage == "repair" else "quality", slot, definition["task_id"])
    if snapshot is not None:
        from taskflow.execution_codex_provider import copy_provider_setting

        copy_provider_setting(role["runner"], setting)
        role["runner"].update({key: setting[key] for key in
                              ("cli", "model", "reasoning_effort", "timeout_seconds", "reasoning_selection", "reasoning_context")})
        if stage == "public_rebuild":
            copy_provider_setting(role["prepare"]["config"], setting)
            role["prepare"]["config"].update({key: setting[key] for key in
                                             ("cli", "model", "reasoning_effort", "reasoning_selection", "reasoning_context")})
    label = f"{number}-{stage}"
    from taskflow.execution_quality_reading import layout, context_metadata, configure_runner

    reading = layout(record)
    if reading is not None:
        from taskflow.execution_quality_reading import validate_history

        validate_history(store, record)
        label = reading["label"]
    node_id = _identity(record["flow_id"], label)
    work_id = definition.get("first_work_id") if number == 0 else None
    work_id = work_id or node_id
    with store._db() as db:
        capacity_binding = store._capacity_binding_for_work(db, work_id)
    if capacity_binding is not None:
        proof = state.get("repair_capacity_continuation")
        if (stage != "repair" or not isinstance(proof, dict)
                or sha256(canonical(proof)) != capacity_binding["proof_digest"]
                or state.get("repair_capacity_attempt_ordinal") != capacity_binding["ordinal"]
                or record["flow_id"] != capacity_binding["flow_id"]):
            raise TaskflowError("repair retry lost its current issuing proof")
    if state.get("repair_capacity_continuation") is not None and stage == "repair" and number == state["repair_capacity_continuation"]["initial_step"] + state["repair_capacity_attempt_ordinal"]:
        if capacity_binding is None:
            raise TaskflowError("repair continuation lacks its immutable issuing event")
    root = Path(definition["directory"]) / "nodes" / label
    runtime, work, scratch = root / "runtime", root / "work", root / "scratch"
    if reading is not None:
        root = reading["root"]
        runtime, work, scratch = root / "runtime", reading["work"], reading["scratch"]
    existing = _get_or_none(store.get_work, work_id)
    if existing is not None:
        if (existing["intent_id"] != node_id or existing["candidate_id"] != candidate["candidate_id"]
                or existing["runtime_path"] != str(runtime) or existing.get("model_slot") != slot
                or existing.get("repair_capacity_binding") != capacity_binding):
            raise TaskflowError("workflow node already exists with different launch identity")
        state.update(current_work_id=work_id, current_runtime=str(runtime), current_node_id=node_id)
        return _save(store, record, state, "workflow_existing_node_recovered")
    if role["runner"].get("public_first_initial_listing") is not None and (
            stage != "quality" or role.get("scope") != "whole_package" or reading is not None):
        raise TaskflowError("neutral initial listing supports only the fresh ordinary whole-quality stage")
    frozen_spec = root / "spec.json"
    if frozen_spec.exists():
        specification = _read(frozen_spec)
        expected = {"work_id": work_id, "task_id": definition["task_id"], "candidate_id": candidate["candidate_id"],
                    "role": "repair" if stage == "repair" else "quality", "policy_id": definition["policy_id"],
                    "intent_id": node_id, "dispatch_owner": definition["owner"]}
        if snapshot is not None:
            expected["model_slot"] = slot
        if capacity_binding is not None:
            expected["repair_capacity_binding"] = capacity_binding
        if any(specification.get("work", {}).get(key) != value for key, value in expected.items()):
            raise TaskflowError("prepared workflow specification has a different launch identity")
        enqueue_spec(store, specification, runtime, resume_prepared=True, probe_native=True,
                     retry_failed_probes=state.get("probe_retry_authorizations", {}).get(str(runtime)))
        state.update(current_work_id=work_id, current_runtime=str(runtime), current_node_id=node_id)
        return _save(store, record, state, "workflow_prepared_specification_reused")
    if (runtime / "manifest.json").exists():
        raise TaskflowError("prepared workflow runtime has no immutable original specification")
    work.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    context = {**_binding(candidate), "flow_id": record["flow_id"], "node_id": node_id,
               "intent_id": node_id, "runtime": str(runtime), "work": str(work),
               "revised_candidate_id": _identity(record["flow_id"], node_id + "-candidate"),
               "scratch": str(scratch), "candidate_root": state["candidate_root"],
               "inputs_root": str(runtime / "inputs"),
               "private_paths": [item["path"] for item in candidate.get("files", []) if item["role"] not in PUBLIC_ROLES],
               "predecessor_archive": state.get("predecessor_archive", ""),
               "public_archive": state.get("public_archive", ""),
               "defect_ids": ([item["defect_id"] for item in state["defects"]]
                              if stage == "repair" else state["repaired_defects"]),
               "defects": state["defects"]}
    continuation = next((row for row in reversed(state.get("history", []))
                         if row.get("kind") in {"workflow_inconclusive_continued", "workflow_rejected_quality_continued"}
                         and row.get("new_work_id") == (_identity(record["flow_id"], reading["canonical_label"])
                                                       if reading is not None else node_id)), None)
    if continuation is not None:
        context["quality_continuation"] = continuation
    if reading is not None:
        context["serial_quality_reading"] = context_metadata(record, reading)
    process_inputs = []
    # Existing prepared specifications keep their own exact request and inputs;
    # new protocol material is produced only before the first specification.
    if stage != "repair" and role["runner"].get("mode") != "existing_isolated":
        process, process_inputs = _review_input_protocol(
            store, candidate, root, whole=role["scope"] == "whole_package")
        context.update(process)
    control = root / "current.json"
    _publish_once(control, context)
    runner = _render(role["runner"], context)
    runner.update(intent_id=node_id, policy_version=definition["policy_id"])
    if runner.get("mode") == "existing_isolated":
        runner["isolated"] = _public_prepare(role, context)
        runner["cwd"] = str(Path(runner["isolated"]["runtime"]) / "work")
        runner["argv"] = [role["prepare"]["launcher_python"],
                          str(Path(runner["isolated"]["runtime"]) / "runner.py"),
                          "execute", runner["isolated"]["runtime"]]
    else:
        runner["cwd"] = str(work)
        runner.setdefault("inputs", []).append({"path": str(control), "kind": "source_metadata",
                                                "target": "execution/current.json"})
        runner["inputs"].append({"path": str(Path(state["candidate_root"]) / "task.json"),
                                 "kind": "source_metadata", "target": "execution/task.json"})
        runner["inputs"].extend(process_inputs)
        if context.get("public_first") and (reading is None or reading["index"] == 0):
            runner["request_sections"] = [{"kind": "operation", "text": public_first_instructions(context)},
                                          *runner.get("request_sections", [])]
        for item in _render(role.get("predecessor_inputs", []), context):
            if "tree" in item:
                source = Path(item["tree"])
                if not source.is_dir() or source.is_symlink():
                    raise TaskflowError("complete predecessor input tree unavailable")
                for path in sorted(source.rglob("*")):
                    if path.is_file():
                        _regular(path)
                        runner["inputs"].append({"path": str(path), "kind": item["kind"],
                                                 "target": item["target"] + "/" + str(path.relative_to(source))})
            else:
                runner["inputs"].append(item)
        for index, reference in enumerate(state.get("last_review_evidence", [])):
            suffix = (".md" if continuation is not None
                      and continuation["kind"] == "workflow_rejected_quality_continued" and index == 2 else ".json")
            runner["inputs"].append({"path": reference["path"], "kind": "history",
                                     "target": f"execution/previous-review-{index}{suffix}"})
        if stage == "repair":
            present = {item["path"] for item in runner["inputs"]}
            runner["inputs"] += [{"path": item["path"], "kind": "teacher", "target": item["package_path"]}
                                 for item in candidate.get("files", [])
                                 if item["role"] not in PUBLIC_ROLES and item["path"] not in present]
        instructions = (repair_instructions(context) if stage == "repair" else
                        review_instructions(context, role["scope"]))
        instructions += ("\nRead the authoritative current package under " + str(runtime / "inputs")
                         + "; execution/task.json is its declaration. Original source paths are provenance, not a request to bypass permissions. "
                         "covered_defect_ids uses only the current execution defect_ids supplied here; historical issue IDs remain historical evidence.\n")
        if continuation is not None:
            source_description = ("a rejected signed quality report" if continuation["kind"] == "workflow_rejected_quality_continued"
                                  else "an inconclusive signature")
            instructions += ("\nThis is one fresh independent whole review after " + source_description + ". "
                "For compact pre-checkpoint routing metadata, use json.load on execution/current.json and "
                'output only current["public_first"], current["inputs_root"], current["work"] and current["scratch"]. '
                "Do not output other current fields before the checkpoint. "
                "Before reading the informed continuation purpose, complete the public-first checkpoint, "
                "save it, and print its actual SHA256 anchor. Only afterward use json.load on execution/current.json "
                'and read exactly current["quality_continuation"]["purpose"]. Print only that field, in pages of at most '
                "6000 UTF-8 bytes per tool response; if longer, continue at the next complete UTF-8 character without "
                "omitting any part of the purpose. Do not dump the whole current object, task declaration or history. "
                "Use the post-checkpoint purpose to locate the concrete unread duties and material entry points. "
                "execution/current.json binds the preserved original, explicit purpose and current candidate; "
                "execution/previous-review-0.json and previous-review-1.json are historical evidence. "
                "Complete the previously unread source and public reconstruction scope as well as the whole current package. "
                "Do not infer acceptance from the continuation authorization, source identity or previous partial checks. "
                "Explicitly examine every supplied current repaired defect. Sign your own actual accepted, revision_required "
                "or inconclusive outcome; retained limitations remain limitations.\n")
            if continuation["kind"] == "workflow_rejected_quality_continued":
                instructions += "The original unreceived intake is execution/previous-review-2.md, also historical evidence.\n"
        runner.setdefault("request_sections", []).append({"kind": "operation", "text": instructions})
        runner["request_sections"].append({"kind": "operation", "text":
            "Required final delivery files: " + ", ".join(runner.get("outputs", []))
            + ". Required complete output directories: " + ", ".join(runner.get("output_trees", []))
            + ". Deliver every declared file; a final message does not replace a requested file."})
        if reading is not None:
            configure_runner(record, reading, runner, context)
        if stage != "repair" and (reading is None or reading.get("phase") == "final"):
            # The same pure target policy used by intake, staged read-only in
            # this attempt. This is a self-check, never a new acceptance gate.
            import shlex
            import sys

            targets = _target_preflight_context(candidate, state, runner["inputs"], work)
            target_context = root / "quality-target-paths.json"
            _publish_once(target_context, targets)
            helper = Path(__file__).with_name("execution_quality_progress_contract.py")
            runner["inputs"].extend([
                {"path": str(helper), "kind": "source_metadata", "target": "execution/quality-target-preflight.py"},
                {"path": str(target_context), "kind": "source_metadata", "target": "execution/quality-target-paths.json"},
            ])
            python = runner.get("native_permissions", {}).get("python", sys.executable)
            command = shlex.join(["rtk", "proxy", python, str(runtime / "inputs/execution/quality-target-preflight.py"),
                                  "--target-scope-only", "--context", str(runtime / "inputs/execution/quality-target-paths.json")])
            runner["request_sections"].append({"kind": "operation", "text":
                "Before ending this same quality turn, run the read-only local target-scope check: " + command
                + ". If it identifies an issue and affected path, independently clarify the actual responsibility "
                  "and correct the signature or keep a truthful inconclusive result in this same turn. "
                  "Do not weaken a genuine blocking finding or presume acceptance. The check does not decide quality, "
                  "authenticate sources, add a review, or replace the original intake."})
    specification = {"work": {"work_id": work_id, "task_id": definition["task_id"],
                     "candidate_id": candidate["candidate_id"], "policy_id": definition["policy_id"],
                     "intent_id": node_id, "role": "repair" if stage == "repair" else "quality",
                     "purpose": role["purpose"], "dispatch_owner": definition["owner"],
                     "evidence": record["evidence"]}, "runner": runner}
    if snapshot is not None:
        specification["work"]["model_slot"] = slot
    if capacity_binding is not None:
        specification["work"]["repair_capacity_binding"] = capacity_binding
    _publish_once(root / "spec.json", specification)
    enqueue_spec(store, specification, runtime, resume_prepared=True, probe_native=True,
                 retry_failed_probes=state.get("probe_retry_authorizations", {}).get(str(runtime)))
    state.update(current_work_id=work_id, current_runtime=str(runtime), current_node_id=node_id)
    return _save(store, record, state, "workflow_node_enqueued")


def _review_metadata(record: dict, candidate: dict, attempt: dict, *, received_dependencies=None) -> dict[str, dict]:
    """Expose only the factory's locked, current candidate declaration alias."""
    from taskflow.execution_flow_contracts import review_metadata_aliases
    from taskflow.execution_runner import _validate
    from taskflow.packages import check_task

    runtime = Path(attempt["runtime_path"])
    manifest = (_validate(runtime) if received_dependencies is None
                else _validate(runtime, received_dependencies=received_dependencies))
    if manifest["candidate_binding"] != _binding(candidate) or manifest["intent_id"] != attempt["intent_id"]:
        raise TaskflowError("review metadata belongs to a different candidate or intent")
    aliases = (review_metadata_aliases(candidate, manifest, runtime) if received_dependencies is None
               else review_metadata_aliases(candidate, manifest, runtime, received_dependencies=received_dependencies))
    if not aliases or aliases["execution/task.json"]["path"] != str(Path(record["state"]["candidate_root"]) / "task.json"):
        raise TaskflowError("review declaration alias has a foreign source")
    return aliases


def _review_observations(record: dict, candidate: dict, attempt: dict, *, received_dependencies=None) -> dict[str, dict]:
    """Resolve only locked inputs from this candidate's actual public archive."""
    from taskflow.execution_runner import _validate

    archive_name = record["state"].get("public_archive")
    if not archive_name:
        return {}
    archive = Path(archive_name)
    if archive_name not in record["state"]["archives"]:
        raise TaskflowError("public observation archive is outside the recorded provenance")
    archived_manifest = _read(archive / "manifest.json")
    old_binding = archived_manifest.get("candidate_binding", {})
    if any(old_binding.get(key) != candidate[key] for key in ("task_id", "input_digest")):
        raise TaskflowError("public observation archive belongs to another public candidate")
    runtime = Path(attempt["runtime_path"])
    manifest = (_validate(runtime) if received_dependencies is None
                else _validate(runtime, received_dependencies=received_dependencies))
    if manifest["candidate_binding"] != _binding(candidate) or manifest["intent_id"] != attempt["intent_id"]:
        raise TaskflowError("public observation inputs have a foreign candidate/intent")
    aliases = {}
    for item in manifest["inputs"]:
        if item["kind"] not in ("submission", "trace"):
            continue
        source = Path(item["source"])
        if not source.is_relative_to(archive):
            raise TaskflowError("public observation source is outside the exact public archive")
        raw = _regular(source)
        if (raw != _regular(runtime / "inputs" / relative_path(item["target"]))
                or sha256(raw) != item["sha256"] or len(raw) != item["size"]):
            raise TaskflowError("public observation source/staged SHA differs")
        if item["target"] in aliases:
            raise TaskflowError("ambiguous public observation alias")
        aliases[item["target"]] = {"path": str(source), "sha256": item["sha256"], "kind": item["kind"]}
    return aliases


def _close_accepted_defect(store: ExecutionStore, record: dict, defect: dict, review: dict,
                           evidence: list[dict]) -> dict:
    """Use only this flow's explicit whole repair signature across public input."""
    candidate = store.get_candidate(review["candidate_id"])
    original = store.get_candidate(defect["candidate_id"])
    if review.get("whole_package") or (defect["kind"] != "material" and original["input_digest"] == candidate["input_digest"]):
        return store.close_defect(defect["defect_id"], review["review_id"], evidence=evidence)
    lineage, seen = set(), set()
    current = candidate
    while True:
        identity = current["candidate_id"]
        if identity in seen:
            raise TaskflowError("whole repair signature candidate lineage is cyclic")
        seen.add(identity)
        lineage.add(identity)
        if identity == defect.get("repair_candidate_id"):
            break
        if not current.get("parent_id"):
            raise TaskflowError("whole repair signature is outside the defect's actual repair lineage")
        current = store.get_candidate(current["parent_id"])
    matches = {}
    for entry in record["state"]["history"]:
        if entry.get("kind") != "independent_review" or not entry.get("review_id"):
            continue
        signed = store.get_review(entry["review_id"])
        if (signed.get("whole_package") is not True or signed.get("verdict") not in {"accepted", "defects"}
                or signed["candidate_id"] not in lineage or defect["defect_id"] not in signed.get("covered_defect_ids", [])):
            continue
        anchor = store.get_candidate(signed["candidate_id"])
        if anchor["input_digest"] != candidate["input_digest"]:
            continue
        source = store.get_attempt(signed["attempt_id"])
        if (entry.get("attempt_id") != source["attempt_id"] or entry.get("candidate_id") != anchor["candidate_id"]
                or entry.get("cli_sid") != source.get("cli_sid") or source.get("state") != "completed"
                or source.get("role") != "quality" or source.get("candidate_id") != anchor["candidate_id"]):
            raise TaskflowError("whole repair signature differs from the workflow's actual independent history")
        matches[signed["review_id"]] = signed
    if len(matches) != 1:
        raise TaskflowError("public-version defect needs one exact whole repair signature from this workflow")
    return store.close_defect_with_coverage_chain(defect["defect_id"], review["review_id"],
        repair_review_id=next(iter(matches)), evidence=evidence)


def _consume_review(store: ExecutionStore, record: dict, attempt: dict, archive: dict) -> dict:
    from taskflow.execution_flow_contracts import parse_review

    definition, state = record["definition"], dict(record["state"])
    prior_sids = {item.get("cli_sid") for item in state["history"]
                  if item.get("kind") in ("adopted_predecessor", "public_reconstruction")}
    prior_sids.update(definition.get("initial", {}).get("excluded_review_sids", []))
    if attempt.get("cli_sid") and attempt["cli_sid"] in prior_sids:
        raise TaskflowError("independent quality reused a public predecessor CLI identity")
    rejected_sids = {item.get("source", {}).get("cli_sid") for item in state["history"]
                     if item.get("kind") == "workflow_rejected_quality_continued"}
    if attempt.get("cli_sid") and attempt["cli_sid"] in rejected_sids:
        raise TaskflowError("independent quality reused a rejected quality CLI identity")
    role = definition["roles"][state["stage"]]
    candidate = store.get_candidate(state["candidate_id"])
    runtime = Path(attempt["runtime_path"])
    report = runtime / "artifacts" / role.get("report_file", "adoption-readiness.json")
    issues = runtime / "artifacts" / role.get("issues_file", "issues.json")
    verdict = parse_review(report, issues, candidate=candidate,
                           intent_id=attempt["intent_id"], scope=role["scope"],
                           metadata_aliases=_review_metadata(record, candidate, attempt),
                           observed_aliases=_review_observations(record, candidate, attempt))
    references = [evidence_ref(report, "actual collected independent role routing attestation"),
                  evidence_ref(issues, "same-role current blocking issue details")]
    review_id = _identity(record["flow_id"], state["current_node_id"] + "-review")
    stored = _get_or_none(store.get_review, review_id)
    if stored is None:
        stored = store.record_review(
            review_id, candidate["candidate_id"], attempt["attempt_id"],
            verdict={"accepted": "accepted", "revision_required": "defects", "inconclusive": "inconclusive"}[verdict["outcome"]],
            scope=role.get("scope_id", record["flow_id"] + ".authorized-quality"), evidence=references,
            whole_package=verdict["whole_package"], covered_defect_ids=verdict["covered_defect_ids"],
        )
    if verdict["outcome"] == "inconclusive":
        raise TaskflowError("independent role signed inconclusive: " + verdict["reason"])
    history = {"kind": "independent_review", "attempt_id": attempt["attempt_id"],
               "cli_sid": attempt["cli_sid"], "review_id": review_id,
               "candidate_id": candidate["candidate_id"], "outcome": verdict["outcome"],
               "archive": archive["path"], "evidence": references}
    state["last_review_evidence"] = references
    if not any(item.get("attempt_id") == attempt["attempt_id"] for item in state["history"]):
        state["history"] = [*state["history"], history]
    if verdict["outcome"] == "accepted":
        store.assert_review_current(review_id)
        for defect_id in state["repaired_defects"]:
            defect = store.get_defect(defect_id)
            if defect["state"] == "closed":
                if defect.get("closure_review_id") != review_id:
                    raise TaskflowError("defect was closed by another result")
            else:
                _close_accepted_defect(store, record, defect, stored, references)
        state.update(status="accepted", current_work_id=None, receipt_pending=True,
                     accepted_review_id=review_id)
        return _save(store, record, state, "workflow_business_accepted")
    pending = []
    signatures = list(state["seen_findings"])
    for issue in verdict["defects"]:
        signature = issue["id"]
        if signature in signatures or signature in state["repaired_defects"]:
            raise TaskflowError("independent recheck repeated an unresolved responsibility; controller decision required")
        defect_id = _identity(record["flow_id"], issue["id"] + "-" + str(state["step"]))
        if _get_or_none(store.get_defect, defect_id) is None:
            store.open_defect(defect_id, candidate["candidate_id"], review_id,
                              responsibility=issue["responsibility"], evidence=issue["evidence"], kind=issue["kind"])
        pending.append({**issue, "defect_id": defect_id, "source_review_id": review_id})
        signatures.append(signature)
    if not set(state["repaired_defects"]).issubset(verdict["covered_defect_ids"]):
        raise TaskflowError("independent recheck did not close previously repaired responsibilities")
    state.update(stage="repair", step=state["step"] + 1, current_work_id=None,
                 defects=pending, seen_findings=signatures,
                 predecessor_archive=archive["path"])
    return _save(store, record, state, "workflow_real_defects_routed_to_repair")


def _consume_repair(store: ExecutionStore, record: dict, attempt: dict, archive: dict) -> dict:
    from taskflow.execution import capture_candidate
    from taskflow.execution_flow_contracts import resolve_repair_record_projection, validate_repair
    from taskflow.execution_repair_capacity import validate_repair_capacity_result

    definition, state = record["definition"], dict(record["state"])
    validate_repair_capacity_result(store, record, attempt)
    role = definition["roles"]["repair"]
    if state.get("pending_business_scope"):
        raise TaskflowError("repair consumption cannot clear a business hold")
    runtime = Path(attempt["runtime_path"])
    source = runtime / "artifacts" / relative_path(role["candidate_subpath"])
    identity = _identity(record["flow_id"], state["current_node_id"] + "-candidate")
    parent = store.get_candidate(state["candidate_id"])
    candidate = _get_or_none(store.get_candidate, identity)
    if candidate is None:
        candidate = capture_candidate(store, source, identity,
                                      parent_id=parent["candidate_id"], author_attempt=attempt["attempt_id"])
    manifest = runtime / "artifacts" / role.get("repair_file", "repair-record.json")
    refs = [evidence_ref(manifest, "actual independent repair manifest and before/after bytes")]
    projection = state.get("repair_record_projections", {}).get(attempt["attempt_id"])
    if projection is not None:
        validated = resolve_repair_record_projection(
            {key: projection[key] for key in ("original", "projected")}, attempt=attempt,
            parent=parent, candidate=candidate, defect_ids=[item["defect_id"] for item in state["defects"]])
        if validated != projection or Path(validated["original"]["path"]) != manifest:
            raise TaskflowError("repair projection differs from its explicit immutable resolution")
        refs.append({**validated["projected"], "kind": "repair_record_projection",
                     "repair_record_projection": {key: validated[key] for key in ("original", "projected")}})
    else:
        validate_repair(manifest, parent=parent, candidate=candidate,
                        defect_ids=[item["defect_id"] for item in state["defects"]])
    identity_proof = state.get("repair_defect_identities", {}).get(attempt["attempt_id"])
    if identity_proof is not None or (projection is None and "covered_defect_ids" in _read(manifest)):
        actual_identity = _completed_repair_identity(store, record, attempt["work_id"])
        if identity_proof is not None and actual_identity != identity_proof:
            raise TaskflowError("repair defect identity differs from its immutable resolution")
    for issue in state["defects"]:
        existing = store.get_defect(issue["defect_id"])
        if existing["state"] == "open":
            disposition = ("material_repair" if issue["kind"] == "material" else
                           "scope_adjustment" if issue["kind"] == "claim" else "private_repair")
            store.record_repair(issue["defect_id"], identity, evidence=refs, disposition=disposition)
        elif existing.get("repair_candidate_id") != identity:
            raise TaskflowError("repair effect already bound to a different candidate")
    changed_public = parent["input_digest"] != candidate["input_digest"]
    next_stage = "public_rebuild" if changed_public else "recheck"
    if next_stage not in definition["roles"]:
        raise TaskflowError("authorized public-change reconstruction route missing")
    state.update(candidate_id=identity, candidate_root=str(source), stage=next_stage,
                 step=state["step"] + 1, current_work_id=None, predecessor_archive=archive["path"],
                 repaired_defects=list(dict.fromkeys(state["repaired_defects"] + [item["defect_id"] for item in state["defects"]])))
    history = {"kind": "actual_repair", "attempt_id": attempt["attempt_id"],
                          "cli_sid": attempt["cli_sid"], "candidate_id": identity,
                          "public_changed": changed_public, "archive": archive["path"]}
    if projection is not None:
        history["repair_record_projection"] = projection
    state["history"] = [*state["history"], history]
    return _save(store, record, state, "workflow_actual_repair_bound")


def _receipt(store: ExecutionStore, record: dict) -> dict:
    from taskflow.execution_controller_finding import current_after_controller_reopen

    current = current_after_controller_reopen(store, record)
    if current is not None:
        return current
    state = dict(record["state"])
    if state.get("receipt"):
        _read(Path(state["receipt"]))
        if state["status"] == "accepted":
            store.assert_review_current(state["accepted_review_id"])
        return record
    if state["status"] == "accepted":
        store.assert_review_current(state["accepted_review_id"])
    value = {"flow_id": record["flow_id"], "status": state["status"],
             "candidate_id": state["candidate_id"], "history": state["history"],
             "archives": state["archives"], "exception": state.get("exception"),
             "receiver": "execution consumer on behalf of registered controller",
             "controller": record["definition"]["owner"], "human_read_claimed": False,
             "asset_use": record["definition"]["accepted_use"],
              "resolved_exceptions": state.get("resolved_exceptions", []),
             "formal_experiment": "not_dispatched", "difficulty": "not_inferred"}
    if state["status"] == "accepted":
        name = "result-" + sha256(canonical(value))[:16] + ".json" if state.get("prior_receipts") else "result.json"
    else:
        name = "exception-" + sha256(canonical(value))[:16] + ".json"
    target = Path(record["definition"]["directory"]) / name
    _publish_once(target, value)
    state.update(receipt=str(target), receipt_pending=False)
    event = {"kind": "workflow_result_received"}
    if state["status"] == "accepted":
        event["accepted_review_id"] = state["accepted_review_id"]
    return store.update_flow(record["flow_id"], record["revision"], state, event)


def advance_workflow(store: ExecutionStore, flow_id: str) -> dict:
    """Consume at most one durable node transition, without launching a model."""
    from taskflow.execution import reconcile
    from taskflow.execution_runner import archive_full_attempt
    from taskflow.execution_repair_capacity import RetryCapacityBusy

    record = store.get_flow(flow_id)
    state = dict(record["state"])
    try:
        if state["status"] in ("accepted", "exception"):
            return _receipt(store, record)
        if not state["initialized"]:
            return _initialize(store, record)
        if not state["current_work_id"]:
            return _prepare_node(store, record)
        work = store.get_work(state["current_work_id"])
        attempts = store.list_attempts(work["work_id"])
        if not attempts:
            return record
        attempt = attempts[-1]
        if attempt["state"] in ACTIVE:
            root = Path(attempt["runtime_path"])
            if (root / "termination.json").exists():
                reconcile(store, attempt["attempt_id"], resume_pending=False)
            elif attempt["state"] == "uncertain":
                from taskflow.execution import _receive_ended_result
                from taskflow.execution_completion import completion_owner
                from taskflow.execution_runner import reconcile_attempt

                with completion_owner(root) as owned:
                    if not owned:
                        return record
                    current = store.get_attempt(attempt["attempt_id"])
                    if current["state"] not in ACTIVE:
                        return record
                    observed = reconcile_attempt(root)
                    if observed.get("process_ended"):
                        _receive_ended_result(store, current, observed, resume_pending=False)
                    elif observed.get("status") != "running":
                        raise TaskflowError("launch remains uncertain after original-process reconciliation")
            return record
        adopted_repair = (state["stage"] == "repair" and attempt["state"] == "failed"
                          and (attempt.get("delivery_resolution") or {}).get("status") == "accepted_auxiliary_omission")
        if adopted_repair:
            store._validated_delivery_resolution(attempt)
        if attempt["state"] != "completed" and not adopted_repair:
            if state["stage"] == "repair" and state.get("repair_capacity_continuation") and attempt["state"] == "failed":
                from taskflow.execution_repair_capacity import advance_failed_capacity_retry

                return advance_failed_capacity_retry(store, record, attempt)
            raise TaskflowError("role technical failure/uncertainty retained: " + attempt["attempt_id"])
        destination = Path(record["definition"]["directory"]) / "archives" / attempt["attempt_id"]
        from taskflow.execution_quality_reading import layout, receive_progress
        from taskflow.execution_quality_session import seal_turn

        reading = layout(record)
        turn_seal = seal_turn(Path(attempt["runtime_path"])) if reading is not None else None
        archive = archive_full_attempt(Path(attempt["runtime_path"]), destination)
        if archive["path"] not in state["archives"]:
            state["archives"] = [*state["archives"], archive["path"]]
            record = _save(store, record, state, "workflow_full_originals_archived")
        if reading is not None and reading["phase"] == "progress":
            return receive_progress(store, record, attempt, archive, turn_seal)
        if state["stage"] == "repair":
            return _consume_repair(store, record, attempt, archive)
        if state["stage"] == "public_rebuild":
            state = dict(record["state"])
            state.update(stage="recheck_public", step=state["step"] + 1,
                         current_work_id=None, predecessor_archive=archive["path"], public_archive=archive["path"])
            state["history"] = [*state["history"], {"kind": "public_reconstruction", "attempt_id": attempt["attempt_id"],
                                "cli_sid": attempt["cli_sid"], "archive": archive["path"], "business_verdict": "not_inferred"}]
            return _save(store, record, state, "workflow_public_reconstruction_received")
        return _consume_review(store, record, attempt, archive)
    except RetryCapacityBusy:
        return store.get_flow(flow_id)
    except (TaskflowError, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        latest = store.get_flow(flow_id)
        failed = dict(latest["state"])
        if failed.get("receipt"):
            failed["superseded_receipt"] = failed.pop("receipt")
        failed.update(status="exception", exception={"reason": str(exc), "work_id": failed.get("current_work_id")},
                      receipt_pending=True)
        return _save(store, latest, failed, "workflow_substantive_or_technical_exception")


def run_workflows(store: ExecutionStore, flow_ids: list[str] | None = None,
                  *, allow_model: bool = False, poll_seconds: float = 1) -> dict:
    """One invocation advances all selected loops and their existing workers."""
    from taskflow.execution import ReceiptEvidenceError, ReceiptResultUncertain, ReceiptWritePending, run_worker

    identities = flow_ids or [item["flow_id"] for item in store.list_flows()
                              if not item["definition"].get("kind")]
    for identity in identities:
        if store.get_flow(identity)["definition"].get("kind"):
            raise TaskflowError("complete handoff records use execution lifecycle-run")
    limits = [store.get_policy(store.get_flow(identity)["definition"]["policy_id"])["config"]["concurrency"]
              for identity in identities]
    workers = min(limits) if limits else 1
    with _consumer_lock(store), ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        pending = {}
        stalled_ready = set()
        while True:
            newly_stalled = set()
            made_progress = False
            for future, work_id in list(futures.items()):
                if future.done():
                    try:
                        result = future.result()
                        if result.get("state") == "waiting":
                            newly_stalled.add(work_id)
                        else:
                            made_progress = True
                    except (ReceiptWritePending, ReceiptEvidenceError, ReceiptResultUncertain) as exc:
                        pending[work_id] = exc
                    except Exception as exc:
                        # Worker boundaries retain their attempt; only its own
                        # authorized continuation stops. No model is retried.
                        for identity in identities:
                            current = store.get_flow(identity)
                            if current["state"].get("current_work_id") == work_id:
                                failed = dict(current["state"])
                                failed.update(status="exception", exception={"reason": str(exc), "work_id": work_id})
                                _save(store, current, failed, "workflow_worker_exception")
                    del futures[future]
            if made_progress:
                stalled_ready.clear()
            stalled_ready.update(newly_stalled)
            for identity in identities:
                current = store.get_flow(identity)
                # The current one-shot worker owns its launch/collection
                # window. Recovery logic is for a worker no longer managed by
                # this invocation, never a reason to race its normal result.
                work_id = current["state"].get("current_work_id")
                if work_id not in futures.values() and work_id not in pending:
                    try:
                        advance_workflow(store, identity)
                    except (ReceiptWritePending, ReceiptEvidenceError, ReceiptResultUncertain) as exc:
                        pending[work_id] = exc
            records = [store.get_flow(identity) for identity in identities]
            if pending and all(item["state"].get("current_work_id") in pending
                               or item["state"]["status"] in ("accepted", "exception") and item["state"].get("receipt")
                               for item in records):
                raise next(iter(pending.values()))
            if all(item["state"]["status"] in ("accepted", "exception") and item["state"].get("receipt") for item in records):
                return {"flows": records, "normal_nodes_require_manual_dispatch": False}
            ready = [item["state"]["current_work_id"] for item in records
                     if item["state"]["status"] == "active" and item["state"].get("current_work_id")
                     and item["state"]["current_work_id"] not in pending
                     and (not pending or item["state"]["current_work_id"] not in stalled_ready)
                     and store.get_work(item["state"]["current_work_id"])["state"] == "ready"]
            # Continue runnable work, but surface a pending receipt when the
            # remaining flows are external or workers could not claim them.
            if pending and not futures and not ready:
                raise next(iter(pending.values()))
            if not allow_model and ready:
                if pending:
                    raise next(iter(pending.values()))
                return {"flows": records, "state": "ready_requires_explicit_model_authorization"}
            for work_id in ready:
                if work_id in futures.values():
                    continue
                if len(futures) >= workers:
                    break
                future = pool.submit(run_worker, store, "continuation-consumer", allow_model, [work_id])
                futures[future] = work_id
            time.sleep(poll_seconds)
