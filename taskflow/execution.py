"""Opt-in local execution CLI; legacy archive commands remain independent.

SQLite owns current scheduling state. Runner receipts and original artifacts are
append-only evidence, and status documents are disposable projections.
"""

import argparse
import json
import math
import sqlite3
import sys
import time
import uuid
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, read_json, relative_path, sha256


OPERATIONS = frozenset({
    "register_policy", "register_candidate", "enqueue", "record_review",
    "open_defect", "record_repair", "close_defect", "record_decision",
    "observe", "transfer_owner", "set_control", "import_external_review",
})


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Persistent local role execution; model calls occur only in worker"
    )
    result.add_argument("--db", type=Path, default=Path("execution/state.sqlite3"))
    commands = result.add_subparsers(dest="operation", required=True)
    apply = commands.add_parser("apply", help="validate and append a typed command record")
    apply.description = "Allowed record operations: " + ", ".join(sorted(OPERATIONS))
    apply.add_argument("--record", type=Path, required=True)
    controller_finding = commands.add_parser("controller-finding-reopen", help="receive an authorized public finding at an accepted idle source; no model")
    controller_finding.add_argument("--record", type=Path, required=True)
    enqueue = commands.add_parser("enqueue", help="lock runner request and enqueue one intent")
    enqueue.add_argument("--spec", type=Path, required=True)
    enqueue.add_argument("--runtime", type=Path, required=True)
    capture = commands.add_parser("capture", help="register actual captured package bytes")
    capture.add_argument("task", type=Path)
    capture.add_argument("--candidate-id", required=True)
    capture.add_argument("--parent-id")
    capture.add_argument("--author-attempt")
    capture.add_argument("--external-author-sid", action="append", default=[])
    capture.add_argument("--external-author-trace", type=Path, action="append", default=[])
    release = commands.add_parser("release", help="quality gate, freeze and export a managed candidate")
    release.add_argument("task", type=Path)
    release.add_argument("--candidate-id", required=True)
    release.add_argument("--version", required=True)
    worker = commands.add_parser("worker", help="claim and execute at most one ready intent")
    worker.add_argument("--worker-id", required=True)
    worker.add_argument("--allow-model", action="store_true",
                        help="required to execute a non-fixture prepared request")
    worker.add_argument("--work-id", action="append",
                        help="claim only these exact work IDs; repeat for a whitelist")
    flows = commands.add_parser("flow-register", help="register preauthorized quality/repair loops once")
    flows.add_argument("--spec", type=Path, required=True)
    consume = commands.add_parser("flow-run", help="advance registered loops through existing workers")
    consume.add_argument("--flow-id", action="append")
    consume.add_argument("--allow-model", action="store_true")
    resume = commands.add_parser("flow-resume", help="append an evidenced technical resolution; never retry a model")
    resume.add_argument("--flow-id", action="append", required=True)
    resume.add_argument("--record", type=Path, required=True,
                        help="JSON object with reason and evidence references for this resolution")
    repair_capacity = commands.add_parser("flow-repair-capacity-prepare", help="prepare a bounded, initially paused repair capacity continuation")
    repair_capacity.add_argument("--parent-flow-id", required=True)
    repair_capacity.add_argument("--flow-id", required=True)
    repair_capacity.add_argument("--record", type=Path, required=True)
    lifecycle = commands.add_parser("lifecycle-register", help="register one explicitly authorized complete handoff")
    lifecycle.add_argument("--spec", type=Path, required=True)
    lifecycle_run = commands.add_parser("lifecycle-run", help="consume one exact registered complete handoff")
    lifecycle_run.add_argument("--flow-id", required=True)
    lifecycle_run.add_argument("--allow-model", action="store_true")
    lifecycle_resume = commands.add_parser("lifecycle-resume", help="resume verified preparation, resolved dependency or validated intake; never retry a model")
    lifecycle_resume.add_argument("--flow-id", required=True)
    lifecycle_resume.add_argument("--record", type=Path, required=True)
    experiment_start = commands.add_parser("experiment-start", help="historical explicit-factory compatibility; new batches use batch-experiment-start")
    experiment_start.add_argument("--packet", type=Path, required=True)
    experiment_start.add_argument("--factory", type=Path, required=True, help="explicit original historical factory; never inferred")
    experiment_start.add_argument("--allow-model", action="store_true")
    author_start = commands.add_parser("author-start", help="historical explicit-factory compatibility; new batches use batch-author-start")
    author_start.add_argument("--packet", type=Path, required=True)
    author_start.add_argument("--factory", type=Path, required=True, help="explicit original historical factory; never inferred")
    author_start.add_argument("--allow-model", action="store_true")
    teacher_start = commands.add_parser("teacher-successor-start", help="bind explicit accepted-original teacher successors once through the existing author factory")
    teacher_start.add_argument("--packet", type=Path, required=True)
    teacher_start.add_argument("--directory", type=Path, required=True, help="separate receipt directory for these once-only links")
    teacher_start.add_argument("--allow-model", action="store_true")
    batch_check = commands.add_parser("batch-check", help="read-only new batch model/scope/binding check; no database or model")
    batch_check.add_argument("--plan", type=Path, required=True)
    batch_freeze = commands.add_parser("batch-freeze", help="freeze confirmed choices and evidence without a database or model")
    batch_freeze.add_argument("--plan", type=Path, required=True)
    batch_freeze.add_argument("--output", type=Path, required=True)
    batch_config = commands.add_parser("batch-config", help="compile a confirmed batch with explicit runtime infrastructure; no model")
    batch_config.add_argument("--plan", type=Path, required=True)
    batch_config.add_argument("--infrastructure", type=Path, required=True)
    batch_config.add_argument("--output", type=Path, required=True)
    for name in ("batch-author-start", "batch-experiment-start", "batch-baseline-quality-start"):
        start = commands.add_parser(name, help="new batch only: validate confirmed snapshot before store/preparation/dispatch")
        start.add_argument("--packet", type=Path, required=True)
        start.add_argument("--factory", type=Path, required=True)
        start.add_argument("--allow-model", action="store_true")
        if name == "batch-author-start":
            start.add_argument("--retry-failed-native-probe-sha256", action="append", metavar="SHA256",
                               help="authorize one zero-model retry for the exact set of failed native reports")
    reconcile = commands.add_parser("reconcile", help="collect evidence; never relaunch a model")
    reconcile.add_argument("attempt_id")
    status = commands.add_parser("status", help="derive dashboard from database evidence")
    status.add_argument("--format", choices=("json", "markdown"), default="json")
    status.add_argument("--output", type=Path)
    events = commands.add_parser("events", help="append-only dispatch and transition history")
    events.add_argument("--after", type=int, default=0)
    simulate = commands.add_parser("simulate", help="neutral end-to-end subprocess fixture; no model")
    simulate.add_argument("--output", type=Path, required=True)
    return result


def apply_record(store: object, record: dict) -> dict:
    operation = record.get("operation")
    arguments = record.get("arguments")
    if operation not in OPERATIONS or not isinstance(arguments, dict):
        raise TaskflowError("command needs an allowed operation and arguments object")
    method = getattr(store, operation, None)
    if method is None:
        raise TaskflowError(f"operation is not implemented: {operation}")
    result = method(**arguments)
    return {"operation": operation, "result": result}


def enqueue_spec(store: object, spec: dict, runtime: Path, *,
                 resume_prepared: bool = False, probe_native: bool = False,
                 retry_failed_probes: list[str] | None = None) -> dict:
    from taskflow.execution_runner import _regular, _validate, prepare_attempt, probe_native_attempt

    work = dict(spec["work"])
    runner = dict(spec["runner"])
    with store._db() as db:
        has_rounds = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bounded_round_work'").fetchone()
        if has_rounds:
            from taskflow.execution_rounds import decorate_round_runner

            runner = decorate_round_runner(db, work, runner)
    if work["intent_id"] != runner["intent_id"] or work["role"] != runner["role"]:
        raise TaskflowError("work and runner intent/role must match")
    if work["policy_id"] != runner["policy_version"]:
        raise TaskflowError("work and runner policy must match")
    from taskflow.execution_batch import policy_setting
    from taskflow.execution_batch_binding import make_batch_binding, validate_batch_runner

    policy = store.get_policy(work["policy_id"])
    slot = work.get("model_slot")
    if runner.get("model_slot") != slot:
        raise TaskflowError("work and runner logical model slot must match")
    setting = policy_setting(policy["config"], work["role"], slot, work["task_id"])
    batch = make_batch_binding(policy, work["role"], slot, work["task_id"])
    if batch is not None:
        work["model_slot"] = runner["model_slot"] = batch["model_slot"]
        if runner.get("batch_binding") not in (None, batch):
            raise TaskflowError("runner has a foreign batch confirmation")
        runner["batch_binding"] = batch
    elif runner.get("batch_binding") is not None:
        raise TaskflowError("legacy policy cannot accept a batch runner binding")
    if any(runner.get(key) != setting[key] for key in ("model", "reasoning_effort", "timeout_seconds")):
        raise TaskflowError("runner differs from the versioned project policy")
    for key in ("reasoning_selection", "reasoning_context"):
        if key in setting and runner.get(key) != setting[key]:
            raise TaskflowError("runner reasoning selection differs from versioned policy")
    candidate = store.get_candidate(work["candidate_id"])
    selected_private = None
    projection_input = None
    if work["role"] == "review-score":
        selected_private, projection_input = _scoring_sources(candidate, runner)
    elif runner.get("scoring_source_allowlist") is not None or runner.get("scoring_projection") is not None:
        raise TaskflowError("normative scoring selection is only supported for review-score")
    runner["candidate_binding"] = {key: candidate[key] for key in (
        "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest"
    )}
    validate_batch_runner(runner)
    if runner.get("mode", "native_role") != "existing_isolated":
        # The role may read additional authorized sources, but it must receive
        # the actual candidate public bytes under review, never a named alias.
        inputs = list(runner.get("inputs", []))
        present = {str(Path(item["path"]).resolve()) for item in inputs}
        for item in candidate.get("files", []):
            needed = (item.get("role") in PUBLIC_ROLES or work["role"] == "quality"
                      or (work["role"] == "review-score" and item in selected_private))
            if needed and item["path"] not in present:
                kind = "public" if item["role"] in PUBLIC_ROLES else "teacher"
                inputs.append({"path": item["path"], "kind": kind, "target": item["package_path"]})
        if projection_input and projection_input["path"] not in present:
            inputs.append(projection_input)
        runner["inputs"] = inputs
    if runtime.exists() and resume_prepared:
        manifest = _validate(runtime)
        if manifest.get("preparation_config_digest") != sha256(canonical(runner)):
            raise TaskflowError("prepared runtime belongs to a different continuation configuration")
    else:
        manifest = prepare_attempt(runtime, runner)
    _check_work_binding(work, candidate, manifest)
    if probe_native and manifest["mode"] == "native_role":
        if not (runtime / "native-probe-ready.json").exists():
            reports = sorted(sha256(_regular(path)) for path in runtime.glob("native-probe-*.json"))
            if reports and reports != retry_failed_probes:
                raise TaskflowError("previous native probe did not succeed; original retained")
            if probe_native_attempt(runtime).get("passed") is not True:
                raise TaskflowError("current native probe failed; work was not enqueued")
        _validate(runtime, before_run=True)
    work.update(runtime_path=str(runtime.resolve()),
                request_digest=manifest["request"]["sha256"])
    if not work["request_digest"]:
        work["request_digest"] = sha256((runtime / "request.md").read_bytes())
    return {"work": store.enqueue(**work), "runtime": str(runtime.resolve())}


def _scoring_sources(candidate: dict, runner: dict) -> tuple[list[dict], dict | None]:
    """Resolve a same-candidate normative subset and its exact scoring projection."""
    from taskflow.execution_runner import _info, _regular

    private = [item for item in candidate.get("files", []) if item["role"] not in PUBLIC_ROLES]
    selection = runner.get("scoring_source_allowlist")
    projection = runner.get("scoring_projection")
    if selection is None:
        if projection is not None:
            raise TaskflowError("scoring projection requires an explicit normative allowlist")
        return private, None
    if (not isinstance(selection, list) or not selection or not all(isinstance(value, str) for value in selection)
            or len(set(selection)) != len(selection)):
        raise TaskflowError("normative scoring allowlist must contain unique package paths")
    names = {relative_path(value) for value in selection}
    chosen = [item for item in private if item["package_path"] in names]
    if {item["package_path"] for item in chosen} != names or not {"answer", "rubric"} <= {item["role"] for item in chosen}:
        raise TaskflowError("normative selection must contain actual candidate answer and rubric and no foreign paths")
    if not isinstance(projection, dict) or projection.get("target") != "normative/task-scoring.json":
        raise TaskflowError("normative scoring requires the exact task-scoring projection")
    declarations = [item for item in candidate["evidence"] if item.get("location") == "explicit package declaration"]
    if (len(declarations) != 1 or projection.get("source") != declarations[0]["path"]
            or projection.get("source_sha256") != declarations[0]["sha256"]):
        raise TaskflowError("scoring projection has a foreign candidate declaration")
    raw = _regular(Path(projection["source"]))
    declaration = json.loads(raw)
    projected = _regular(Path(projection["path"]))
    if (sha256(raw) != projection["source_sha256"] or declaration["task_id"] != candidate["task_id"]
            or sha256(projected) != projection.get("sha256") or json.loads(projected) != declaration["scoring"]):
        raise TaskflowError("scoring projection differs from the signed candidate task.scoring")
    if projection.get("normative_certification") is not None:
        from taskflow.execution_formal import validate_normative_certification

        certified = validate_normative_certification(
            candidate=candidate, certification=projection["normative_certification"])
        def certified_row(item: dict) -> dict:
            observed = _info(Path(item["path"]))
            if observed["sha256"] != item["sha256"]:
                raise TaskflowError("certified normative source changed after capture")
            return {key: item[key] for key in ("path", "package_path", "role")} | observed

        observed = {item["package_path"]: certified_row(item) for item in chosen}
        expected = {item["package_path"]: certified_row(item) for item in certified}
        if observed != expected:
            raise TaskflowError("scoring selection differs from the actual independent normative certification")
    return chosen, {"path": projection["path"], "kind": "teacher", "target": projection["target"]}


def _check_work_binding(work: dict, candidate: dict, manifest: dict) -> None:
    if (manifest["intent_id"] != work["intent_id"] or manifest["role"] != work["role"]
            or manifest["policy_version"] != work["policy_id"]):
        raise TaskflowError("runtime intent/role/policy differs from the work item")
    binding = {key: candidate[key] for key in (
        "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest"
    )}
    if manifest.get("candidate_binding") != binding:
        raise TaskflowError("runtime candidate public/private version binding differs from work")
    if work["role"] == "review-score":
        if (manifest.get("facts_seal") or {}).get("candidate_binding") != binding:
            raise TaskflowError("facts and scoring must use the same candidate and frozen standards")
    if manifest["mode"] == "existing_isolated":
        binding = manifest["isolated"]["source_binding"]
        actual = binding.get("input_digest") or (binding.get("source_binding") or {}).get("input_digest")
        if actual != candidate["input_digest"]:
            raise TaskflowError("isolated runner public input differs from the managed candidate")
        frozen = binding.get("frozen_binding") or {}
        snapshot = frozen.get("snapshot_directory")
        if work["role"] == "solver" and not snapshot:
            raise TaskflowError("formal solver requires the exact frozen public/private version")
        if snapshot:
            from taskflow.packages import verify_snapshot

            declared = verify_snapshot(Path(snapshot))
            if any(declared[key] != candidate[key] for key in (
                    "task_id", "input_digest", "content_digest", "evaluation_digest")):
                raise TaskflowError("isolated runner frozen standards differ from managed candidate")
    else:
        public = [item for item in candidate.get("files", []) if item.get("role") in PUBLIC_ROLES]
        if not public and manifest["mode"] == "native_role":
            raise TaskflowError("native role requires captured candidate public file provenance")
        provided = {(item["source"], item["sha256"]) for item in manifest["inputs"] if item["kind"] == "public"}
        if any((item["path"], item["sha256"]) not in provided for item in public):
            raise TaskflowError("native role does not contain the captured candidate public bytes")
        if public and provided != {(item["path"], item["sha256"]) for item in public}:
            raise TaskflowError("public handoff includes another candidate; classify unrelated research separately")
        if work["role"] in ("quality", "review-score"):
            private = [item for item in candidate.get("files", []) if item.get("role") not in PUBLIC_ROLES]
            projected = None
            if work["role"] == "review-score":
                private, projected = _scoring_sources(candidate, manifest)
            teachers = {(item["source"], item["sha256"]) for item in manifest["inputs"] if item["kind"] == "teacher"}
            expected = {(item["path"], item["sha256"]) for item in private}
            if projected:
                expected.add((projected["path"], manifest["scoring_projection"]["sha256"]))
            if not expected <= teachers:
                raise TaskflowError("quality/scoring review lacks current candidate reference/rubric bytes")
            if teachers != expected:
                raise TaskflowError("quality/scoring review includes foreign reference/rubric bytes")


def capture_candidate(store: object, task: Path, candidate_id: str,
                      parent_id: str | None = None,
                      author_attempt: str | None = None,
                      external_author_sids: list[str] | None = None,
                      external_author_traces: list[Path] | None = None) -> dict:
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import check_task

    result = check_task(task, write_report=False)
    if result["errors"]:
        raise TaskflowError("candidate structure invalid: " + "; ".join(result["errors"]))
    declaration = read_json(task / "task.json")
    files = [{**evidence_ref(task / item["path"], "actual candidate file"),
              "package_path": item["path"], "role": item["role"]} for item in declaration["files"]]
    after = check_task(task, write_report=False)
    if any(after[key] != result[key] for key in ("input_digest", "content_digest", "evaluation_digest")):
        raise TaskflowError("candidate changed while collecting file provenance")
    return store.register_candidate(
        result["task_id"], candidate_id,
        **{key: result[key] for key in ("input_digest", "content_digest", "evaluation_digest")},
        parent_id=parent_id, author_attempt=author_attempt,
        external_author_sids=external_author_sids,
        files=files,
        evidence=[evidence_ref(task / "task.json", "explicit package declaration")]
        + [evidence_ref(path, "external completed author trace") for path in external_author_traces or []],
    )


def release_candidate(store: object, task: Path, candidate_id: str, version: str) -> dict:
    from taskflow.packages import check_task, export_snapshot, freeze_task

    captured = check_task(task, write_report=False)
    candidate = store.get_candidate(candidate_id)
    if any(captured[key] != candidate[key] for key in ("input_digest", "content_digest", "evaluation_digest")):
        raise TaskflowError("managed candidate changed; capture a revision and independently review it")
    store.quality_gate(candidate_id)
    frozen = freeze_task(task, version)
    if any(frozen[key] != candidate[key] for key in ("input_digest", "content_digest", "evaluation_digest")):
        raise TaskflowError("published snapshot differs from managed review; retained for investigation, no export")
    snapshot = Path(frozen["path"])
    return {"snapshot": frozen, "solver": export_snapshot(snapshot, "solver"),
            "evaluator": export_snapshot(snapshot, "evaluator")}


def _record_result(store: object, attempt: dict, result: dict, *,
                   busy_timeout_seconds: float | None = None) -> dict:
    """Receive technical evidence without turning it into a business verdict."""
    from taskflow.execution_store import evidence_ref

    runtime = Path(attempt["runtime_path"])
    receipt = runtime / "result.json"
    if not receipt.is_file():
        receipt = runtime / "collection.json"
    evidence = [evidence_ref(receipt, "technical result")] if receipt.is_file() else []
    options = {} if busy_timeout_seconds is None else {"busy_timeout_seconds": busy_timeout_seconds}
    return store.finish_attempt(
        attempt["attempt_id"],
        process_exit=result.get("termination", {}).get("returncode"),
        response_complete=bool(result.get("model_completed")),
        artifacts_complete=bool(result.get("delivery_complete")),
        technical_accepted=bool(result.get("technical_accepted")),
        process_state=result.get("process_state", "unknown"),
        evidence=evidence, cli_sid=result.get("cli_sid", result.get("sid")), **options,
    )


class ReceiptWritePending(RuntimeError):
    """A sealed result still needs its database receipt; no model retry."""

    def __init__(self, receipt: dict):
        self.receipt = receipt
        super().__init__("terminal receipt database wait exhausted; retained " + receipt["path"])

class ReceiptEvidenceError(RuntimeError):
    """Report diagnostic I/O without changing a terminal attempt to uncertain."""

    def __init__(self, path: Path, error: Exception, *, terminal_received: bool):
        self.path = str(path)
        self.terminal_received = terminal_received
        super().__init__("terminal receipt diagnostic could not be published: " + str(path) + ": " + str(error))

class ReceiptResultUncertain(RuntimeError):
    """The sealed observation does not establish a terminal attempt."""

    def __init__(self, attempt_id: str):
        self.attempt_id = attempt_id
        super().__init__("original result remains uncertain: " + attempt_id)

def _receipt_busy(error: sqlite3.OperationalError) -> bool:
    code = getattr(error, "sqlite_errorcode", None)
    if isinstance(code, int):
        return (code & 255) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
    # Python3.10 does not expose sqlite_errorcode on exceptions.
    return str(error).lower().split(":", 1)[0] in {
        "database is locked", "database table is locked", "database schema is locked",
    }

def _receipt_reference(path: Path, location: str) -> dict:
    from taskflow.execution_runner import _info

    return {"path": str(path), **_info(path), "location": location}

def _same_terminal_result(attempt: dict, original: dict, result: dict) -> None:
    for key in ("attempt_id", "work_id", "intent_id", "runtime_path", "request_digest", "policy_id", "role"):
        if attempt.get(key) != original.get(key):
            raise TaskflowError("terminal receipt attempt identity changed: " + key)
    expected = {"process_exit": result.get("termination", {}).get("returncode"),
                "response_complete": bool(result.get("model_completed")),
                "artifacts_complete": bool(result.get("delivery_complete")),
                "technical_accepted": bool(result.get("technical_accepted"))}
    sid = result.get("cli_sid", result.get("sid"))
    if sid:
        expected["cli_sid"] = sid
    if any(attempt.get(key) != value for key, value in expected.items()):
        raise TaskflowError("terminal attempt does not match its original sealed result")

def _pending_receipt_refs(runtime: Path, attempt: dict, result: dict) -> list[dict]:
    from taskflow.execution_runner import _regular
    from taskflow.execution_store import validate_evidence

    references = []
    for path in sorted(runtime.glob("receipt-write-pending-*.json")):
        note = json.loads(_regular(path))
        if (not isinstance(note, dict) or note.get("schema_version") != 1
                or note.get("kind") != "terminal_receipt_write_pending"
                or any(note.get(key) != attempt.get(key) for key in
                       ("attempt_id", "work_id", "intent_id", "runtime_path", "request_digest", "role"))
                or note.get("cli_sid") != result.get("cli_sid", result.get("sid"))):
            raise TaskflowError("pending terminal receipt belongs to another original attempt")
        bindings = note.get("bindings")
        names = {"manifest.json", "manifest-lock.json", "termination.json", "collection.json", "collection-lock.json"}
        if not isinstance(bindings, dict) or set(bindings) != names:
            raise TaskflowError("pending terminal receipt lacks original byte bindings")
        if any(not isinstance(value, dict) or value.get("path") != str(runtime / name)
               for name, value in bindings.items()):
            raise TaskflowError("pending terminal receipt points outside its original runtime")
        validate_evidence(list(bindings.values()))
        references.append(_receipt_reference(path, "previous bounded terminal receipt wait"))
    return references

def _receive_ended_result(store: object, attempt: dict, result: dict, *,
                          mark_collecting: bool = False, wait_seconds: float = 90,
                          resume_pending: bool = True) -> dict:
    """Receive one ended original under its caller's completion-owner mutex.

    Only SQLite BUSY/LOCKED is retried. Each database call waits at most one
    second and at most the remaining budget; original file validation is never
    skipped or interrupted. No claim, process launch or model call occurs here.
    """
    from taskflow.execution_runner import _publish
    from taskflow.execution_store import TERMINAL

    if (isinstance(wait_seconds, bool) or not isinstance(wait_seconds, (int, float))
            or not math.isfinite(wait_seconds) or wait_seconds < 0):
        raise TaskflowError("terminal receipt wait must be finite and nonnegative")
    if not result.get("process_ended") or result.get("intent_id", attempt["intent_id"]) != attempt["intent_id"]:
        raise TaskflowError("terminal receipt requires this original proved termination")
    runtime = Path(attempt["runtime_path"])
    pending = _pending_receipt_refs(runtime, attempt, result)
    started = time.monotonic()
    deadline = started + wait_seconds
    errors = []

    def timeout() -> float:
        return max(0, min(1, deadline - time.monotonic()))

    def received(current: dict) -> dict:
        if current["state"] not in TERMINAL:
            raise ReceiptResultUncertain(attempt["attempt_id"])
        if pending:
            note = {"schema_version": 1, "kind": "pending_terminal_receipt_received",
                    "attempt_id": attempt["attempt_id"], "intent_id": attempt["intent_id"],
                    "pending_receipts": pending, "terminal_attempt": current,
                    "automatic_retry": False, "new_model_calls": 0}
            path = runtime / ("receipt-write-received-" + sha256(canonical(note)) + ".json")
            try:
                from taskflow.execution_runner import _regular

                try:
                    _publish(path, note)
                except FileExistsError:
                    if json.loads(_regular(path)) != note:
                        raise TaskflowError("terminal receipt resolution bytes changed")
            except (OSError, TaskflowError, ValueError) as error:
                raise ReceiptEvidenceError(path, error, terminal_received=True) from error
        return current

    if pending and not resume_pending:
        # A supervising author/lifecycle may observe a worker's just-exhausted
        # budget. It must stop too, not start another budget in its polling loop.
        try:
            current = store.get_attempt(attempt["attempt_id"], busy_timeout_seconds=0)
        except sqlite3.OperationalError as error:
            if not _receipt_busy(error):
                raise
        else:
            if current["state"] in TERMINAL:
                _same_terminal_result(current, attempt, result)
                return received(current)
        raise ReceiptWritePending(pending[-1])

    while True:
        stage = "read_original_attempt"
        try:
            current = store.get_attempt(attempt["attempt_id"], busy_timeout_seconds=timeout())
            for key in ("attempt_id", "work_id", "intent_id", "runtime_path", "request_digest", "policy_id", "role"):
                if current.get(key) != attempt.get(key):
                    raise TaskflowError("original attempt changed during terminal receipt wait: " + key)
            if current["state"] in TERMINAL:
                _same_terminal_result(current, attempt, result)
                return received(current)
            # A startup-recording interruption may leave starting/claimed. The
            # canonical finisher accepts their proved original termination too.
            if mark_collecting and current["state"] in ("running", "uncertain"):
                stage = "mark_collecting"
                store.mark_collecting(attempt["attempt_id"], busy_timeout_seconds=timeout())
            stage = "finish_attempt"
            return received(_record_result(store, attempt, result, busy_timeout_seconds=timeout()))
        except sqlite3.OperationalError as error:
            if not _receipt_busy(error):
                raise
            errors.append({"stage": stage, "message": str(error),
                           "sqlite_errorcode": getattr(error, "sqlite_errorcode", None),
                           "elapsed_seconds": time.monotonic() - started})
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # A commit followed by an error must not be received twice.
                try:
                    current = store.get_attempt(attempt["attempt_id"], busy_timeout_seconds=0)
                except sqlite3.OperationalError as final_error:
                    if not _receipt_busy(final_error):
                        raise
                else:
                    if current["state"] in TERMINAL:
                        _same_terminal_result(current, attempt, result)
                        return received(current)
                break
            time.sleep(min(0.05, remaining))

    path = runtime / ("receipt-write-pending-" + uuid.uuid4().hex + ".json")
    try:
        bindings = {name: _receipt_reference(runtime / name, "original terminal receipt bytes")
                    for name in ("manifest.json", "manifest-lock.json", "termination.json", "collection.json", "collection-lock.json")}
        _publish(path, {"schema_version": 1, "kind": "terminal_receipt_write_pending",
                       **{key: attempt.get(key) for key in
                          ("attempt_id", "work_id", "intent_id", "runtime_path", "request_digest", "role")},
                       "cli_sid": result.get("cli_sid", result.get("sid")), "bindings": bindings,
                       "lock_budget_seconds": wait_seconds, "elapsed_seconds": time.monotonic() - started,
                       "busy_errors": errors, "automatic_retry": False, "new_model_calls": 0,
                       "next_action": "Revalidate and reconcile this same original attempt; never redraw the model."})
    except (OSError, TaskflowError, ValueError) as error:
        raise ReceiptEvidenceError(path, error, terminal_received=False) from error
    raise ReceiptWritePending(_receipt_reference(path, "original terminal result pending database receipt"))


def run_worker(store: object, worker_id: str, allow_model: bool = False,
               work_ids: list[str] | None = None) -> dict:
    from taskflow.execution_repair_capacity import RetryCapacityBusy, retry_claim_guard

    try:
        with retry_claim_guard(store, worker_id, work_ids) as (attempt, capacity):
            return _run_worker_claimed(store, attempt, allow_model, capacity)
    except RetryCapacityBusy:
        return {"state": "waiting", "reason": "capacity source lifecycle is busy; no claim occurred"}


def _run_worker_claimed(store: object, attempt: dict | None, allow_model: bool,
                        capacity: dict | None) -> dict:
    from taskflow.execution_runner import run_attempt
    from taskflow.execution_completion import completion_owner

    if attempt is None:
        return {"state": "waiting", "reason": "no eligible ready work"}
    # Claim is a durable attempt. Failure here preserves it; it is never silently
    # returned to the ready queue or interpreted as an unused model call.
    if not attempt.get("runtime_path"):
        reason = "claimed work has no prepared runtime; preserve and reconcile"
        store.mark_uncertain(attempt["attempt_id"], reason)
        raise TaskflowError(reason)
    runtime = Path(attempt["runtime_path"])
    with completion_owner(runtime) as owned:
        if not owned:
            return store.get_attempt(attempt["attempt_id"])
        current = store.get_attempt(attempt["attempt_id"])
        if current["state"] != "claimed":
            return current
        try:
            manifest = read_json(runtime / "manifest.json")
            if manifest.get("mode") != "test_fixture" and not allow_model:
                raise TaskflowError("non-fixture execution requires --allow-model")
            policy = store.get_policy(attempt["policy_id"])
            _check_work_binding(attempt, store.get_candidate(attempt["candidate_id"]), manifest)
            from taskflow.execution_batch import policy_setting
            from taskflow.execution_batch_binding import validate_batch_source

            setting = policy_setting(policy["config"], attempt["role"], attempt.get("model_slot"), attempt["task_id"])
            validate_batch_source(attempt, manifest, store)
            if (manifest["model"]["requested"] != setting["model"]
                    or manifest["model"]["reasoning_effort"] != setting["reasoning_effort"]
                    or manifest["timeout_seconds"] != setting["timeout_seconds"]
                    or manifest["request"]["sha256"] != attempt["request_digest"]
                    or manifest["policy_version"] != attempt["policy_id"]):
                raise TaskflowError("prepared request differs from the claimed versioned policy")
            for key in ("reasoning_selection", "reasoning_context"):
                if key in setting and manifest.get(key) != setting[key]:
                    raise TaskflowError("prepared reasoning selection differs from the claimed policy")
            if setting.get("formal_baseline_sha256"):
                baseline = str(Path(manifest["isolated"]["runtime"]) / "baseline-plan.json")
                if manifest["dependencies"].get(baseline, {}).get("sha256") != setting["formal_baseline_sha256"]:
                    raise TaskflowError("formal baseline differs from the claimed versioned policy")
            if capacity is not None:
                from taskflow.execution_repair_capacity import validate_repair_capacity_before_dispatch

                if attempt.get("repair_capacity_binding") != capacity["binding"]:
                    raise TaskflowError("claimed retry lost its immutable issuing-event binding")
                current_flow = store.get_flow(capacity["flow_id"])
                validate_repair_capacity_before_dispatch(store, current_flow, manifest)
            elif attempt.get("repair_capacity_binding") is not None:
                raise TaskflowError("retry cannot dispatch without its protected pre-claim locks")
            store.mark_starting(attempt["attempt_id"])
            def on_process(identity: dict) -> None:
                if identity.get("start_identity"):
                    store.record_process(attempt["attempt_id"], identity["pid"], identity["start_identity"])
                else:
                    store.mark_uncertain(attempt["attempt_id"], "process started; startup identity unavailable")

            with store._db() as db:
                reserved = db.execute("SELECT 1 FROM events WHERE kind='bounded_round_work_reserved' "
                    "AND CASE WHEN json_valid(data) THEN json_extract(data,'$.work_id')=? ELSE 0 END",
                    (attempt["work_id"],)).fetchone()
            if reserved:
                from taskflow.execution_rounds import round_launch_guard

                result = run_attempt(runtime, on_process=on_process,
                                     launch_guard=lambda: round_launch_guard(store, attempt))
            else:
                result = run_attempt(runtime, on_process=on_process)
            if not result.get("process_ended"):
                return store.mark_uncertain(attempt["attempt_id"], "runner has no proved termination")
            return _receive_ended_result(store, attempt, result, mark_collecting=True, resume_pending=False)
        except (OSError, TaskflowError, ValueError) as exc:
            store.mark_uncertain(attempt["attempt_id"], str(exc))
            raise


def reconcile(store: object, attempt_id: str, *, resume_pending: bool = True) -> dict:
    from taskflow.execution_runner import reconcile_attempt
    from taskflow.execution_completion import completion_owner

    attempt = store.get_attempt(attempt_id)
    runtime_path = attempt.get("runtime_path")
    pending = bool(runtime_path) and any(Path(runtime_path).glob("receipt-write-pending-*.json"))
    if attempt["state"] in ("completed", "failed", "cancelled") and not pending:
        return attempt
    with completion_owner(Path(attempt["runtime_path"])) as owned:
        if not owned:
            return store.get_attempt(attempt_id)
        attempt = store.get_attempt(attempt_id)
        if attempt["state"] in ("completed", "failed", "cancelled") and not pending:
            return attempt
        result = reconcile_attempt(Path(attempt["runtime_path"]))
        if result.get("process_ended"):
            return _receive_ended_result(store, attempt, result, resume_pending=resume_pending)
        return store.mark_uncertain(attempt_id, result.get("reason", "process or result unproved"))


def dashboard_markdown(dashboard: dict) -> str:
    def cell(value: object) -> str:
        return str(value if value is not None else "unknown").replace("|", "\\|").replace("\n", " ")

    lines = ["# Execution evidence", "", "Generated from SQLite. Observed legacy work remains with its recorded owner."]
    if dashboard.get("flows"):
        work_by_id = {item["work_id"]: item for item in dashboard.get("work", [])}
        attempts = dashboard.get("attempts", [])
        attempt_by_id = {item["attempt_id"]: item for item in attempts}
        lines += ["", "## Current registered flows", "",
                  "Flow revisions and event sequences identify the stored state. Work and attempt states come from this same projection; observations below retain their own times and sources.", "",
                  "| Task | Flow / kind | Revision / event | Status / stage | Work / actual attempt evidence | Next responsibility | Receipt / recorded result |",
                  "|---|---|---|---|---|---|---|"]
        for flow in dashboard["flows"]:
            definition, state = flow["definition"], flow["state"]
            owner = definition.get("owner") or "unknown controller"
            work_id = state.get("current_work_id")
            work = work_by_id.get(work_id, {})
            matching = [item for item in attempts if work_id and item.get("work_id") == work_id]
            attempt = matching[-1] if matching else {}
            evidence = [f"current work={work_id}; queue={work.get('state', 'unknown')}" if work_id else "no current work"]
            if not work_id:
                source_author = definition.get("source_author_attempt")
                runtime = state.get("current_runtime")
                if source_author:
                    evidence.append(f"source author attempt={source_author}")
                    attempt = attempt_by_id.get(source_author, {})
                elif runtime:
                    evidence.append(f"last runtime={runtime}")
                    matching = [item for item in attempts if item.get("runtime_path") == runtime]
                    attempt = matching[-1] if matching else {}
                elif isinstance(state.get("formal_attempt"), dict):
                    identity = state["formal_attempt"].get("attempt_id")
                    evidence.append(f"last formal attempt={identity}")
                    attempt = attempt_by_id.get(identity, {})
            if attempt:
                evidence.append(f"attempt={attempt['attempt_id']}; state={attempt.get('state', 'unknown')}; "
                                f"sid={attempt.get('cli_sid') or 'unknown'}; response={attempt.get('response_complete')}; "
                                f"delivery={attempt.get('artifacts_complete')}; technical={attempt.get('technical_accepted')}")
            elif work_id:
                evidence.append("no attempt recorded")
            status, stage = state.get("status"), state.get("stage")
            if status == "accepted":
                next_role = f"{owner}: business decision for further work"
            elif status == "exception":
                next_role = f"{owner}: resolve recorded exception"
            elif attempt.get("state") in ("uncertain", "starting", "claimed"):
                next_role = f"{owner}: reconcile existing attempt; no relaunch"
            elif attempt.get("state") in ("running", "collecting"):
                next_role = f"{owner}: observe and collect existing attempt"
            elif attempt:
                next_role = f"{owner}: consume existing evidence for recorded stage"
            elif work_id:
                next_role = f"{owner}: follow registered work state"
            else:
                next_role = f"{owner}: follow recorded stage; next work unknown"
            result, exception, waiting = state.get("result") or {}, state.get("exception") or {}, state.get("waiting") or {}
            receipt = [f"receipt={state.get('receipt') or 'not recorded'}"]
            if result.get("outcome"):
                receipt.append(f"outcome={result['outcome']}")
            if exception.get("reason"):
                receipt.append(f"exception={exception['reason']}")
            if waiting.get("reason"):
                receipt.append(f"waiting={waiting['reason']}")
            kind = definition.get("kind") or ("quality workflow (kind undeclared)" if definition.get("roles") else "unknown")
            row = [definition.get("task_id"), f"{flow['flow_id']} / {kind}",
                   f"rev={flow.get('revision')}; event={flow.get('event_seq')}",
                   f"{status or 'unknown'} / {stage or 'unknown'}", "; ".join(evidence), next_role, "; ".join(receipt)]
            lines.append("| " + " | ".join(cell(value) for value in row) + " |")
    lines += ["", "## Task observations and managed scopes", "",
             "| Task | Mode | Owner / scope | State | Actual response evidence | Next responsibility | Observed at |",
             "|---|---|---|---|---|---|---|"]
    for task in dashboard.get("tasks", []):
        observation = task.get("observation") or {}
        details = observation.get("details", {})
        role = details.get("current_role_evidence", {})
        # A work-scoped handoff does not transfer the other R&D responsibilities.
        # Keep the source observation separate from each managed work item.
        scoped = task.get("managed_work_items", [])
        state = (observation.get("observed_status") if scoped else task.get("current_state")) or "waiting"
        next_role = details.get("next_action_claim") if scoped else task.get("next_role") or details.get("next_action_claim")
        source = observation.get("source") or {}
        observed = (f"{observation.get('observed_at') or 'unknown'}; source={source.get('path') or 'unknown'}; "
                    f"sha256={source.get('sha256') or 'unknown'}")
        row = [task.get("task_id"), "observed R&D" if scoped else task.get("mode"),
               task.get("owner"), state, role.get("model_response", "unknown"),
               next_role or "controller decision pending", observed]
        lines.append("| " + " | ".join(cell(value) for value in row) + " |")
        for scope in scoped:
            work_id = scope["work_id"]
            work = next((item for item in dashboard.get("work", []) if item["work_id"] == work_id), {})
            attempts = [item for item in dashboard.get("attempts", []) if item["work_id"] == work_id]
            attempt = attempts[-1] if attempts else {}
            state = attempt.get("state") or work.get("state") or "handoff accepted; not enqueued"
            response = (f"response={attempt.get('response_complete')}; delivery={attempt.get('artifacts_complete')}; "
                        f"technical={attempt.get('technical_accepted')}") if attempt else "no attempt"
            next_role = f"{scope['owner']}: {work.get('role', 'prepare handed-off work')}"
            if state in ("uncertain", "starting", "claimed"):
                next_role = f"{scope['owner']}: reconcile existing attempt; no relaunch"
            elif state in ("running", "collecting"):
                next_role = f"{scope['owner']}: observe and collect existing attempt"
            elif state in ("completed", "failed", "cancelled"):
                next_role = f"{task.get('owner')}: accept evidence and decide next work"
            row = [work_id, "managed work item", scope["owner"], state, response,
                   next_role, attempt.get("ended_at") or attempt.get("updated_at") or work.get("created_at")]
            lines.append("| " + " | ".join(cell(value) for value in row) + " |")
    if not dashboard.get("tasks") and not dashboard.get("flows"):
        lines.append(json.dumps(dashboard, ensure_ascii=False, indent=2))
    lines += ["", "Technical completion does not establish business quality, experiment validity, difficulty or platform acceptance."]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.operation == "batch-check":
            from taskflow.execution_batch import check_batch_plan

            output = check_batch_plan(args.plan)
        elif args.operation == "batch-freeze":
            from taskflow.execution_batch import freeze_batch_plan

            output = freeze_batch_plan(args.plan, args.output)
        elif args.operation == "batch-config":
            from taskflow.execution_batch_factory import compile_batch_configuration

            output = compile_batch_configuration(args.plan, args.infrastructure, args.output)
        elif args.operation == "simulate":
            from tools.execution_simulation import simulate

            output = simulate(args.output)
        else:
            from taskflow.execution_store import ExecutionStore

            batch_policy = None
            if args.operation in ("batch-author-start", "batch-experiment-start", "batch-baseline-quality-start"):
                from taskflow.execution_batch_factory import checked_batch_factory

                _, batch_policy = checked_batch_factory(args.factory, args.packet)
            store = ExecutionStore(args.db)
            if batch_policy is not None:
                store.register_policy(batch_policy["policy_id"], batch_policy["config"])
            if args.operation == "apply":
                output = apply_record(store, read_json(args.record))
            elif args.operation == "controller-finding-reopen":
                from taskflow.execution_controller_finding import reopen_controller_public_finding

                output = reopen_controller_public_finding(store, **read_json(args.record))
            elif args.operation == "enqueue":
                output = enqueue_spec(store, read_json(args.spec), args.runtime)
            elif args.operation == "worker":
                output = run_worker(store, args.worker_id, args.allow_model, args.work_id)
            elif args.operation == "flow-register":
                from taskflow.execution_flow import register_workflow

                document = read_json(args.spec)
                output = {"flows": [register_workflow(store, item["definition"], item["evidence"])
                                    for item in document["workflows"]]}
            elif args.operation == "flow-run":
                from taskflow.execution_flow import run_workflows

                output = run_workflows(store, args.flow_id, allow_model=args.allow_model)
            elif args.operation == "flow-resume":
                from taskflow.execution_flow import resume_workflow

                document = read_json(args.record)
                output = {"flows": [resume_workflow(store, identity, **document)
                                    for identity in args.flow_id]}
            elif args.operation == "flow-repair-capacity-prepare":
                from taskflow.execution_repair_capacity import prepare_repair_capacity_budget

                output = prepare_repair_capacity_budget(store, args.parent_flow_id, args.flow_id, read_json(args.record))
            elif args.operation == "lifecycle-register":
                from taskflow.execution_lifecycle import register_lifecycle

                document = read_json(args.spec)
                output = register_lifecycle(store, document["definition"], document["evidence"])
            elif args.operation == "lifecycle-run":
                from taskflow.execution_lifecycle import run_lifecycle

                output = run_lifecycle(store, args.flow_id, allow_model=args.allow_model)
            elif args.operation == "lifecycle-resume":
                from taskflow.execution_lifecycle import resume_path_transport

                output = resume_path_transport(store, args.flow_id, read_json(args.record))
            elif args.operation == "batch-baseline-quality-start":
                from taskflow.execution_baseline_quality import start_baseline_quality_from_packet
                from taskflow.execution_store import evidence_ref

                output = start_baseline_quality_from_packet(store,
                    packet_ref=evidence_ref(args.packet.resolve(), "explicit research-ended baseline quality purpose"),
                    factory_config_ref=evidence_ref(args.factory.resolve(), "original confirmed quality infrastructure"),
                    allow_model=args.allow_model)
            elif args.operation in ("experiment-start", "batch-experiment-start"):
                from taskflow.execution_experiment_factory import start_experiments_from_packet
                from taskflow.execution_store import evidence_ref

                output = start_experiments_from_packet(store,
                    packet_ref=evidence_ref(args.packet.resolve(), "separate substantive downstream experiment authority"),
                    factory_config_ref=evidence_ref(args.factory.resolve(), "shared parameterized experiment preparation"),
                    allow_model=args.allow_model)
            elif args.operation in ("author-start", "batch-author-start"):
                from taskflow.execution_author import start_author_from_packet
                from taskflow.execution_store import evidence_ref

                factory = read_json(args.factory)
                packet = read_json(args.packet)
                if not isinstance(packet.get("items"), list) or len(packet["items"]) != 1:
                    raise TaskflowError("author-start accepts one concretely authorized item per packet")
                output = start_author_from_packet(store,
                    packet_ref=evidence_ref(args.packet.resolve(), "once-only substantive author direction and normal successors"),
                    template_spec_ref=factory["template_spec_ref"],
                    directory=Path(factory["research_root"]) / packet["items"][0]["intent_id"],
                    allow_model=args.allow_model, factory_config=factory,
                    retry_failed_probes=getattr(args, "retry_failed_native_probe_sha256", None))
            elif args.operation == "teacher-successor-start":
                from taskflow.execution_teacher_successor import start_teacher_successors_from_packet
                from taskflow.execution_store import evidence_ref

                output = start_teacher_successors_from_packet(store,
                    packet_ref=evidence_ref(args.packet.resolve(), "explicit bounded post-original teacher business successors"),
                    directory=args.directory.resolve(), allow_model=args.allow_model)
            elif args.operation == "capture":
                output = capture_candidate(store, args.task, args.candidate_id,
                                           args.parent_id, args.author_attempt,
                                           args.external_author_sid, args.external_author_trace)
            elif args.operation == "release":
                output = release_candidate(store, args.task, args.candidate_id, args.version)
            elif args.operation == "reconcile":
                output = reconcile(store, args.attempt_id)
            elif args.operation == "events":
                output = {"events": store.events(after=args.after)}
            else:
                output = store.dashboard()
                output["flows"] = store.list_flows()
                if args.format == "markdown":
                    output = dashboard_markdown(output)
                if args.output:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(
                        output if isinstance(output, str) else json.dumps(output, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
        print(output if isinstance(output, str) else json.dumps(output, ensure_ascii=False, indent=2, allow_nan=False))
        return 2 if args.operation == "batch-check" and not output["ready"] else 0
    except ReceiptWritePending as exc:
        print(json.dumps({"error": str(exc), "state": "terminal_receipt_pending",
                          "receipt": exc.receipt, "automatic_retry": False}, ensure_ascii=False), file=sys.stderr)
        return 2
    except ReceiptEvidenceError as exc:
        print(json.dumps({"error": str(exc), "state": "terminal_receipt_evidence_error",
                          "path": exc.path, "terminal_received": exc.terminal_received,
                          "automatic_retry": False}, ensure_ascii=False), file=sys.stderr)
        return 2
    except ReceiptResultUncertain as exc:
        print(json.dumps({"error": str(exc), "state": "original_result_uncertain",
                          "attempt_id": exc.attempt_id, "automatic_retry": False}, ensure_ascii=False), file=sys.stderr)
        return 2
    except (OSError, TaskflowError, KeyError, TypeError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
