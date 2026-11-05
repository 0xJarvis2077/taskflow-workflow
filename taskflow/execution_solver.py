"""An explicitly authorized public-only formal answer, without a business loop."""

import copy
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, relative_path, validate_id
from taskflow.execution import run_worker
from taskflow.execution_iteration import explicit_solver_decision
from taskflow.execution_quality_contracts import candidate_binding, delivery_error
from taskflow.execution_runner import (
    RunnerError, _info, _read, _regular, archive_full_attempt,
)
from taskflow.execution_solver_contracts import (
    check_public_inputs, check_public_view, check_solver_permissions, completed_original,
    quality_source, solver_decision, verified_full_archive,
)
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.execution_tasks import (
    execution_state, get_task, invocation, original_attempt, prepare_work,
    publish_copy, publish_once, render, save_task, task_lock,
)


KIND = "solver_task_v1"


def _get(store, identity: str) -> dict:
    record = get_task(store, identity, KIND)
    expected = invocation(record["definition"], "solver")
    if any(record["state"]["invocation"].get(key) != value for key, value in expected.items()):
        raise TaskflowError("solver invocation differs from its exact registered identity and paths")
    return record


def _sources(store, spec: dict) -> dict:
    source = quality_source(store, spec)
    decision = solver_decision(store, spec)
    explicit_solver_decision(decision)
    return {**source, "decision": decision}


def register_solver_task(store, spec: dict, evidence: list[dict]) -> dict:
    """Register one explicit solver purpose; never release or execute a model."""
    spec = copy.deepcopy(spec)
    if type(spec.get("schema_version")) is not int or spec["schema_version"] != 1:
        raise TaskflowError("solver schema_version must be 1")
    for key in ("flow_id", "task_id", "owner", "policy_id", "candidate_id", "quality_task_id", "decision_id"):
        validate_id(spec.get(key), key)
    for key in ("directory", "candidate_root"):
        if not isinstance(spec.get(key), str) or not Path(spec[key]).is_absolute():
            raise TaskflowError("solver " + key + " must be absolute")
    if not isinstance(spec.get("purpose"), str) or not spec["purpose"].strip():
        raise TaskflowError("solver purpose required")
    runner = spec.get("runner")
    if not isinstance(runner, dict) or runner.get("mode") not in {"native_role", "test_fixture"}:
        raise TaskflowError("solver requires an explicit native_role runner or neutral fixture")
    if runner.get("inputs") or runner.get("request_text"):
        raise TaskflowError("solver inputs are derived only from its exact public candidate")
    if any(key in runner for key in ("resume_sid", "quality_session", "facts_attempt", "public_first_initial_listing",
                                     "scoring_source_allowlist", "scoring_projection")):
        raise TaskflowError("solver cannot reuse another role or receive scoring context")
    if any(not isinstance(section, dict) or section.get("kind") != "operation"
           for section in runner.get("request_sections", [])):
        raise TaskflowError("solver request sections must be typed operations")
    session = runner.get("role_session")
    if (not isinstance(session, dict) or session.get("mode") not in {"fresh_persistent", "fresh_ephemeral"}
            or session.get("previous") is not None or not isinstance(session.get("home"), str)
            or not Path(session["home"]).is_absolute()):
        raise TaskflowError("solver requires a fresh independent role_session and own absolute home")
    outputs = runner.get("outputs", [])
    trees = runner.get("output_trees", [])
    if (not isinstance(outputs, list) or not isinstance(trees, list) or not (outputs or trees)
            or any(not isinstance(path, str) for path in outputs + trees)):
        raise TaskflowError("solver requires explicit relative delivery outputs or output trees")
    for name in outputs + trees:
        relative_path(name)
    refs = validate_evidence(evidence)
    if not refs:
        raise TaskflowError("solver requires actual authorization/source evidence")
    spec["kind"] = KIND
    _sources(store, spec)
    store.get_policy(spec["policy_id"])
    with task_lock(Path(spec["directory"]), store=store):
        try:
            current = store.get_flow(spec["flow_id"])
        except TaskflowError:
            current = None
        if current is not None:
            if current["definition"] != spec or current["evidence"] != refs:
                raise TaskflowError("solver cannot replace its original definition or authority")
            return current
        return store.register_flow(spec["flow_id"], spec, refs, {
            "status": "ready", "execution": "not_started", "delivery": "pending", "outcome": None,
            "errors": [], "candidate_id": spec["candidate_id"], "invocation": invocation(spec, "solver")})


def _prepare(store, record: dict) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    source = _sources(store, spec)
    current = state["invocation"]
    if current.get("prepared"):
        check_public_view(Path(spec["directory"]) / "public", source["files"])
        return record
    root, work, scratch = (Path(current[key]) for key in ("root", "work", "scratch"))
    for path in (root, work, scratch):
        path.mkdir(parents=True, exist_ok=True)
    public_root = Path(spec["directory"]) / "public"
    for name, row in source["files"].items():
        if row["role"] in PUBLIC_ROLES:
            publish_copy(public_root / name, _regular(Path(row["source"])))
    check_public_view(public_root, source["files"])
    context = {**candidate_binding(source["candidate"]), **current, "public_root": str(public_root),
               "inputs_root": str(Path(current["runtime"]) / "inputs")}
    runner = render(copy.deepcopy(spec["runner"]), context)
    # Management-only intake is locked with this call, never sent to the solver.
    decision_path = Path(spec["directory"]) / "solver-decision.json"
    publish_once(decision_path, source["decision"])
    runner["dependencies"] = [*runner.get("dependencies", []), str(decision_path)]
    runner.update(role="solver", intent_id=current["intent_id"], policy_version=spec["policy_id"],
                  cwd=str(work), inputs=[{"path": row["source"], "kind": "public", "target": name}
                      for name, row in sorted(source["files"].items()) if row["role"] in PUBLIC_ROLES])
    runner["request_sections"] = [*runner.get("request_sections", []), {"kind": "operation", "text":
        "Complete this formal answer independently using only the declared public inputs and your own work. "
        "Do not seek author material, references, scoring standards or other sessions. "
        "Write the required deliverables in your current work directory. Retain uncertainty truthfully; "
        "do not start another model or a reviewer."}]
    check_solver_permissions(runner, public_root, source["files"],
        Path(source["quality"]["definition"]["directory"]), Path(spec["candidate_root"]))
    prepare_work(store, record, current, runner)
    current["prepared"] = True
    current["decision_source"] = evidence_ref(decision_path, "coordinator intake locked before formal dispatch")
    current["quality_source"] = {"task_id": spec["quality_task_id"],
        "attempt_id": source["attempt"]["attempt_id"], "review_id": source["review"]["review_id"],
        "manifest": _info(Path(source["attempt"]["runtime_path"]) / "manifest.json"),
        "collection": _info(Path(source["attempt"]["runtime_path"]) / "collection.json")}
    return save_task(store, record, state, "solver_task_prepared")


def prepare_solver_task(store, identity: str) -> dict:
    """Prepare exactly one public-only work item, without starting a model."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        return _prepare(store, _get(store, identity))


def _receipt(record: dict, source: dict, attempt: dict, manifest: dict, collection: dict, archive: Path) -> dict:
    root = Path(record["state"]["invocation"]["runtime"])
    return {"schema_version": 1, "kind": "solver_delivery_v1", "solver_task_id": record["flow_id"],
            "candidate_binding": candidate_binding(source["candidate"]), "policy_id": record["definition"]["policy_id"],
            "decision_id": record["definition"]["decision_id"], "quality_task_id": record["definition"]["quality_task_id"],
            "quality_source": record["state"]["invocation"]["quality_source"],
            "work_id": attempt["work_id"], "attempt_id": attempt["attempt_id"], "intent_id": manifest["intent_id"],
            "sid": collection["sid"], "manifest": evidence_ref(root / "manifest.json", "original solver manifest"),
            "collection": evidence_ref(root / "collection.json", "original solver collection"),
            "archive": str(archive), "archive_binding": evidence_ref(archive / "archive-binding.json", "original full solver archive"),
            "supplemental_intent": evidence_ref(root / "supplemental-intent.json", "original complete solver sources"),
            "supplemental_receipt": evidence_ref(archive / "supplemental/receipt.json", "original complete solver capture"),
            "execution": "completed", "delivery": "valid", "outcome": None}


def _collect(store, record: dict) -> dict:
    state = copy.deepcopy(record["state"])
    current = state["invocation"]
    attempt = original_attempt(store, current)
    if attempt is None:
        return record
    current["attempt_id"] = attempt["attempt_id"]
    state.update(execution=execution_state(attempt), status=execution_state(attempt))
    if state["execution"] != "completed":
        return save_task(store, record, state, "solver_task_execution_observed")
    errors = []
    try:
        source = _sources(store, record["definition"])
        check_public_view(Path(record["definition"]["directory"]) / "public", source["files"])
        attempt, manifest, collection = completed_original(store, current, source["candidate"], "solver")
        if attempt["policy_id"] != record["definition"]["policy_id"] or manifest["policy_version"] != record["definition"]["policy_id"]:
            raise TaskflowError("solver original differs from its declared exact policy")
        check_public_inputs(manifest, source["files"])
        if collection["sid"] in source["excluded_sids"]:
            raise TaskflowError("solver reused an author, public reconstruction or quality context")
        archive = Path(current["root"]) / "archive"
        current["archive"] = archive_full_attempt(Path(current["runtime"]), archive)
        received = _receipt({**record, "state": state}, source, attempt, manifest, collection, archive)
        receipt = Path(record["definition"]["directory"]) / "solver-receipt.json"
        publish_once(receipt, received)
        state["receipt"] = evidence_ref(receipt, "original explicit solver delivery")
        current["sid"] = collection["sid"]
    except (TaskflowError, RunnerError, OSError, KeyError, TypeError, ValueError) as error:
        errors.append(delivery_error("solver.invalid_source_or_delivery", str(error), correctable=False))
    state.update(status="completed", execution="completed", delivery="invalid" if errors else "valid", errors=errors)
    return save_task(store, record, state, "solver_task_delivery_received")


def collect_solver_task(store, identity: str) -> dict:
    """Receive the original solver; never infer correctness or start a reviewer."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity))
    return _with_report(store, record)


def _with_report(store, record: dict) -> dict:
    """Expose measurements after termination, without changing the original run."""
    if record["state"]["execution"] not in {"completed", "failed", "unknown"}:
        return record
    try:
        from taskflow.stress_report import build_report, write_report

        report = build_report(store.path, record["flow_id"])
        output = write_report(Path(record["definition"]["directory"]) / "reports", report)
        return {**record, "report": {"status": "available", **output}}
    except Exception as error:  # A derived report cannot change received work.
        return {**record, "report": {"status": "unavailable", "error": str(error)}}


def run_solver_task(store, identity: str, allow_model: bool = False) -> dict:
    """Run the current explicitly authorized solver invocation at most once."""
    record = prepare_solver_task(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity))
        if record["state"]["execution"] == "not_started" and allow_model:
            run_worker(store, "explicit-solver-task", True, [record["state"]["invocation"]["work_id"]])
            record = _collect(store, _get(store, identity))
    return _with_report(store, record)


def validate_solver_source(store, identity: str) -> dict:
    """Return only an authenticated complete solver delivery for a later reviewer."""
    record = _get(store, identity)
    spec, state = record["definition"], record["state"]
    if state.get("execution") != "completed" or state.get("delivery") != "valid":
        raise TaskflowError("review requires a completed valid explicit solver delivery")
    source = _sources(store, spec)
    check_public_view(Path(spec["directory"]) / "public", source["files"])
    current = state["invocation"]
    attempt, manifest, collection = completed_original(store, current, source["candidate"], "solver")
    if attempt["policy_id"] != spec["policy_id"] or manifest["policy_version"] != spec["policy_id"]:
        raise TaskflowError("solver original differs from its declared exact policy")
    check_public_inputs(manifest, source["files"])
    if collection["sid"] in source["excluded_sids"]:
        raise TaskflowError("solver context is not independent of its source roles")
    archive = Path(current["root"]) / "archive"
    verified_full_archive(Path(current["runtime"]), archive)
    reference = state["receipt"]
    validate_evidence([reference])
    path = Path(spec["directory"]) / "solver-receipt.json"
    if Path(reference["path"]) != path:
        raise TaskflowError("solver receipt reference moved outside its original task")
    expected = _receipt(record, source, attempt, manifest, collection, archive)
    if _read(path) != expected:
        raise TaskflowError("solver receipt differs from its exact original invocation")
    return {**source, "record": record, "attempt": attempt, "manifest": manifest, "collection": collection,
            "archive": archive, "receipt": expected, "receipt_reference": reference,
            "excluded_sids": sorted({*source["excluded_sids"], collection["sid"]})}
