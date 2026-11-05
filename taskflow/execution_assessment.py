"""Explicit factual review, sealing and same-reviewer original scoring.

The coordinator submits stages. Python never interprets an answer, chooses a
score, starts a business successor or repairs a model's original files.
"""

import copy
import json
from pathlib import Path
import shlex
import sys

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, relative_path, validate_id
from taskflow.execution import run_worker
from taskflow.execution_assessment_contracts import (
    AssessmentContractError, _location, accumulate_business_payload, business_payload, check_delivery, number, validate_business_values,
)
from taskflow.execution_quality_contracts import candidate_binding, delivery_error, observation_inputs
from taskflow.execution_runner import (
    RunnerError, _info, _read, _regular, _validate, _validated_collection,
    _validated_facts_seal, archive_full_attempt, seal_facts,
)
from taskflow.execution_solver import validate_solver_source
from taskflow.execution_solver_contracts import check_public_view, completed_original
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.execution_tasks import (
    check_native_view, execution_state, get_task, invocation, original_attempt, prepare_work,
    publish_copy, publish_once, render, save_task, task_lock,
)


KIND = "assessment_task_v1"
STAGES = ("facts", "score")


def _reference(path: Path, location: str) -> dict:
    return {**evidence_ref(path, location), **_info(path)}


def _directory(path: Path) -> None:
    if (not path.is_absolute() or path.resolve() != path or path.is_symlink()
            or (path.exists() and not path.is_dir())):
        raise TaskflowError("assessment owned directories must be canonical ordinary absolute paths")
    path.mkdir(parents=True, exist_ok=True)


def _get(store, identity: str) -> dict:
    return get_task(store, identity, KIND)


def _stage(value: str) -> str:
    if value not in STAGES:
        raise TaskflowError("assessment stage must be facts or score")
    return value


def _current(state: dict, stage: str) -> dict:
    entries = state["invocations"][stage]
    if not entries:
        raise TaskflowError("assessment stage has not been explicitly prepared: " + stage)
    return entries[-1]


def _public_sources(spec: dict, source: dict) -> list[dict]:
    archive = source["archive"]
    result = []
    for row in source["manifest"]["inputs"]:
        path = archive / "inputs" / row["target"]
        if _info(path) != {key: row[key] for key in ("sha256", "size")}:
            raise TaskflowError("original solver public input changed in its archive")
        result.append({"path": source["files"][row["target"]]["source"], "target": row["target"], "kind": "public"})
    for row in observation_inputs(archive):
        result.append({**row, "target": row["target"].replace("actual-public-", "actual-solver-", 1)})
    for name in ("request.md", "process.json", "termination.json"):
        path = archive / name
        _regular(path)
        result.append({"path": str(path), "kind": "trace", "target": "actual-solver-" + name})
    intent = _read(archive / "supplemental-intent.json")
    membership = {"schema_version": 1, "kind": "actual_solver_workspace_membership_v1", "workspaces": {}}
    for area in ("work", "scratch"):
        alias = intent["aliases"].get(area)
        if alias is None:
            continue
        tree = intent["snapshot"][alias["source"]]
        prefix = "" if alias["relative"] == "." else alias["relative"].rstrip("/") + "/"
        membership["workspaces"][area] = {
            key: {name[len(prefix):]: value for name, value in tree[key].items()
                  if not prefix or name.startswith(prefix)}
            for key in ("files", "directories", "special")}
    index = Path(spec["directory"]) / "sources/solver-membership.json"
    publish_once(index, membership)
    result.append({"path": str(index), "kind": "trace", "target": "actual-solver-workspace-membership.json"})
    names = [row["target"] for row in result]
    if len(names) != len(set(names)):
        raise TaskflowError("solver source view has colliding public/submission targets")
    return sorted(result, key=lambda row: row["target"])


def _standards(spec: dict, source: dict) -> list[dict]:
    selected = spec.get("standards")
    files = source["files"]
    if (not isinstance(selected, list) or not selected or not all(isinstance(name, str) for name in selected)
            or len(selected) != len(set(selected))):
        raise TaskflowError("assessment needs explicit unique current standards paths")
    quality = source["quality"]
    current = quality["state"]["invocations"][quality["state"]["current_index"]]
    report = _read(Path(current["runtime"]) / "artifacts" / quality["definition"]["report_file"])
    normative = report["native_signoff"]["normative"]
    if normative["status"] != "determined" or set(selected) != {row["package_path"] for row in normative["files"]}:
        raise TaskflowError("scoring standards must exactly retain the accepted quality role's frozen normative selection")
    result = []
    for name in selected:
        name = relative_path(name)
        row = files.get(name)
        if row is None or row["role"] in PUBLIC_ROLES:
            raise TaskflowError("scoring standard is not an actual current private candidate member")
        result.append({"path": row["source"], "target": name, "kind": "teacher", **_info(Path(row["source"]))})
    if not {"answer", "rubric"} <= {files[row["target"]]["role"] for row in result}:
        raise TaskflowError("scoring needs explicit current answer and rubric sources")
    return sorted(result, key=lambda row: row["target"])


def _criteria(spec: dict, source: dict, standards: list[dict]) -> list[dict]:
    declared = _read(Path(spec["candidate_root"]) / "task.json")["scoring"]
    expected = {row["id"]: row["max_score"] for row in declared["criteria"]}
    rows = spec.get("criteria")
    if not isinstance(rows, list) or len(rows) != len(expected) or not rows:
        raise TaskflowError("explicit criteria must match the frozen scoring declaration")
    seen = set()
    allowed = {row["target"]: row for row in standards if source["files"][row["target"]]["role"] == "rubric"}
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {"id", "max_points", "source"}
                or not isinstance(row["id"], str) or row["id"] not in expected or row["id"] in seen
                or not number(row["max_points"]) or row["max_points"] != expected[row["id"]]):
            raise TaskflowError("criterion identity/maximum differs from the current frozen rubric declaration")
        reference = row["source"]
        selected = allowed.get(reference.get("path")) if isinstance(reference, dict) and isinstance(reference.get("path"), str) else None
        if (selected is None or set(reference) != {"path", "sha256", "location"}
                or reference["sha256"] != selected["sha256"]):
            raise TaskflowError("criterion needs its current selected rubric path/SHA/location")
        _location(reference["location"], {**selected, "raw": _regular(Path(selected["path"]))}, "criteria/source/location")
        seen.add(row["id"])
    return copy.deepcopy(rows)


def _sources(store, spec: dict) -> dict:
    source = validate_solver_source(store, spec["solver_task_id"])
    if spec["facts_runner"]["mode"] == "native_role" and source["manifest"]["mode"] == "test_fixture":
        raise TaskflowError("real review cannot consume a simulated solver as actual business execution")
    original = source["record"]["definition"]
    if any(original[key] != spec[key] for key in ("candidate_id", "task_id", "candidate_root")):
        raise TaskflowError("assessment belongs to another formal candidate or source")
    _directory(Path(spec["directory"]) / "sources")
    source["first_inputs"] = _public_sources(spec, source)
    source["standards"] = _standards(spec, source)
    source["criteria"] = _criteria(spec, source, source["standards"])
    declarations = [row for row in source["candidate"]["evidence"] if row.get("location") == "explicit package declaration"]
    if len(declarations) != 1:
        raise TaskflowError("review requires the actual captured scoring declaration")
    declaration = Path(declarations[0]["path"])
    validate_evidence(declarations)
    source["scoring_declaration"] = _reference(declaration, "actual captured scoring declaration")
    source["scoring"] = _read(declaration)["scoring"]
    projection = Path(spec["directory"]) / "sources/task-scoring.json"
    publish_once(projection, source["scoring"])
    source["scoring_projection"] = {"path": str(projection), "target": "normative/task-scoring.json", "kind": "teacher", **_info(projection)}
    source["teacher_inputs"] = [*source["standards"], source["scoring_projection"]]
    return source


def _initial_result() -> dict:
    return {"execution": "not_started", "delivery": "pending", "outcome": None, "errors": []}


def register_assessment_task(store, spec: dict, evidence: list[dict]) -> dict:
    """Register one original reviewer for one explicit completed formal source."""
    spec = copy.deepcopy(spec)
    if type(spec.get("schema_version")) is not int or spec["schema_version"] != 1:
        raise TaskflowError("assessment schema_version must be 1")
    for key in ("flow_id", "task_id", "owner", "policy_id", "candidate_id", "solver_task_id"):
        validate_id(spec.get(key), key)
    for key in ("directory", "candidate_root"):
        if not isinstance(spec.get(key), str) or not Path(spec[key]).is_absolute():
            raise TaskflowError("assessment " + key + " must be absolute")
    if Path(spec["directory"]).resolve() != Path(spec["directory"]):
        raise TaskflowError("assessment directory must be canonical before registration")
    if not isinstance(spec.get("purpose"), str) or not spec["purpose"].strip():
        raise TaskflowError("assessment purpose required")
    limit = spec.setdefault("correction_limit", 0)
    if type(limit) is not int or limit < 0:
        raise TaskflowError("correction_limit must be a nonnegative integer")
    for stage in STAGES:
        runner = spec.get(stage + "_runner")
        if (not isinstance(runner, dict) or runner.get("mode") not in {"native_role", "test_fixture"}
                or runner.get("inputs") or runner.get("request_sections") or runner.get("request_text")
                or runner.get("output_trees") or any(key in runner for key in (
                    "facts_attempt", "resume_sid", "quality_session", "run_binding", "public_first_initial_listing",
                    "scoring_source_allowlist", "scoring_projection"))):
            raise TaskflowError("assessment runners declare runtime only; stage materials/instructions are derived")
        session = runner.get("role_session")
        if (not isinstance(session, dict) or session.get("mode") != "fresh_persistent"
                or session.get("previous") is not None or not isinstance(session.get("home"), str)
                or not Path(session["home"]).is_absolute()):
            raise TaskflowError("assessment requires the explicit same persistent reviewer home")
    a, b = spec["facts_runner"], spec["score_runner"]
    if any(a.get(key) != b.get(key) for key in ("mode", "model", "reasoning_effort", "cli", "native_provider", "role_session")):
        raise TaskflowError("facts and scoring must retain one reviewer model/provider/profile/home")
    refs = validate_evidence(evidence)
    if not refs:
        raise TaskflowError("assessment needs actual authority/source evidence")
    spec["kind"] = KIND
    with task_lock(Path(store.path).parent / ".assessment-registration", store=store):
        _sources(store, spec)
        store.get_policy(spec["policy_id"])
        for current in store.list_flows():
            if current["flow_id"] == spec["flow_id"]:
                if current["definition"] != spec or current["evidence"] != refs:
                    raise TaskflowError("assessment cannot replace its original definition or authority")
                return current
            if (current["definition"].get("kind") == KIND
                    and current["definition"].get("solver_task_id") == spec["solver_task_id"]):
                raise TaskflowError("this formal solver already has its original assessment task")
        return store.register_flow(spec["flow_id"], spec, refs, {
            "status": "ready", "stage": "facts", **_initial_result(),
            "results": {stage: _initial_result() for stage in STAGES},
            "invocations": {"facts": [invocation(spec, "facts")], "score": []}, "corrections_used": 0})


def _binding(source: dict) -> dict:
    return {**candidate_binding(source["candidate"]), "solver_attempt_id": source["attempt"]["attempt_id"],
            "solver_collection_sha256": source["receipt"]["collection"]["sha256"]}


def _index(inputs: list[dict]) -> list[dict]:
    return [{"path": row["target"], **_info(Path(row["path"]))} for row in inputs]


def _shared(spec: dict, source: dict) -> tuple[list[dict], dict]:
    shared = Path(spec["directory"]) / "sources"
    context = {"stage": "facts", "binding": _binding(source), "sources": _index(source["first_inputs"])}
    publish_once(shared / "facts-context.json", context)
    helper = Path(__file__).with_name("execution_assessment_contracts.py")
    publish_copy(shared / "assessment-preflight.py", _regular(helper))
    binding = {"schema_version": 1, "kind": "review_run_binding_v1", "candidate_binding": candidate_binding(source["candidate"]),
               "solver_receipt": source["receipt_reference"], "solver_attempt_id": source["attempt"]["attempt_id"],
               "solver_collection": source["receipt"]["collection"], "first_inputs": source["first_inputs"],
               "standards": source["standards"], "criteria": source["criteria"],
               "scoring_declaration": source["scoring_declaration"], "scoring_projection": source["scoring_projection"]}
    publish_once(shared / "source-binding.json", binding)
    inputs = [*source["first_inputs"],
              {"path": str(shared / "facts-context.json"), "target": "execution/review-context.json", "kind": "operation"},
              {"path": str(shared / "assessment-preflight.py"), "target": "execution/assessment-preflight.py", "kind": "operation"}]
    run_binding = {"source": _reference(shared / "source-binding.json", "explicit original review source and frozen standards"),
                   "teacher_inputs": source["teacher_inputs"]}
    return inputs, run_binding


def _sealed(record: dict) -> tuple[dict, Path]:
    reference = record["state"].get("facts_seal")
    if not reference:
        raise TaskflowError("scoring requires explicitly sealed complete original facts")
    validate_evidence([reference])
    facts = _current(record["state"], "facts")
    root = Path(facts["runtime"])
    if Path(reference["path"]) != root / "facts-seal.json":
        raise TaskflowError("facts seal does not identify the current original factual turn")
    return _validated_facts_seal(root), root


def _context(spec: dict, source: dict, current: dict, stage: str, seal: dict | None = None) -> dict:
    rows = list(source["first_inputs"])
    result = {"stage": stage, "binding": {**_binding(source), "intent_id": current["intent_id"]},
              "inputs_root": str(Path(current["runtime"]) / "inputs")}
    if stage == "score":
        rows.extend(source["teacher_inputs"])
        rows.append({"path": seal["path"], "target": "execution/sealed-facts.json", "kind": "submission"})
        result.update(criteria=source["criteria"], scoring=source["scoring"],
                      scoring_projection={"path": source["scoring_projection"]["target"], "sha256": source["scoring_projection"]["sha256"]},
                      facts_sha256=seal["facts"]["sha256"])
        if "business_lock" in current:
            validate_evidence(current["business_lock_sources"])
            retained = {}
            for reference in current["business_lock_sources"]:
                retained = accumulate_business_payload(retained, _read(Path(reference["path"])), result)
            if retained != current["business_lock"]:
                raise TaskflowError("scoring correction lock differs from its original signed source turns")
            result["business_lock"] = copy.deepcopy(current["business_lock"])
    result["sources"] = _index(rows)
    return result


def _permissions(runner: dict, source: dict, public_root: Path, spec: dict) -> None:
    forbidden = [Path(spec["candidate_root"]), Path(spec["directory"]) / "sources",
                 Path(source["record"]["definition"]["directory"]),
                 Path(source["quality"]["definition"]["directory"])]
    forbidden.extend(Path(row["source"]) for row in source["files"].values() if row["role"] not in PUBLIC_ROLES)
    forbidden.extend(Path(row["path"]) for row in source["teacher_inputs"])
    check_native_view(runner, public_root, control_root=Path(spec["directory"]), forbidden=forbidden)


def _prepare(store, record: dict, stage: str) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    source = _sources(store, spec)
    public_root = Path(spec["directory"]) / "public"
    if stage == "score" and not state["invocations"][stage]:
        _sealed(record)
        state["invocations"][stage].append(invocation(spec, stage))
    current = _current(state, stage)
    root, work, scratch = (Path(current[key]) for key in ("root", "work", "scratch"))
    for path in (Path(spec["directory"]), root, work, scratch, public_root):
        _directory(path)
    if current.get("prepared"):
        check_public_view(public_root, source["files"])
        return record
    inputs, run_binding = _shared(spec, source)
    seal, facts_root = _sealed(record) if stage == "score" else (None, None)
    if stage == "facts" and (state.get("facts_seal") or state["invocations"]["score"]):
        raise TaskflowError("sealed factual review cannot be reopened")
    for row in source["first_inputs"]:
        if row["kind"] == "public":
            publish_copy(public_root / row["target"], _regular(Path(row["path"])))
    check_public_view(public_root, source["files"])
    context = _context(spec, source, current, stage, seal)
    runner = render(copy.deepcopy(spec[stage + "_runner"]), {**current, "public_root": str(public_root),
        "inputs_root": context["inputs_root"], **candidate_binding(source["candidate"])})
    if runner.get("mode") == "native_role":
        supplied = Path(runner.get("native_permissions", {}).get("scratch", ""))
        if supplied != scratch or supplied.resolve() != scratch:
            raise TaskflowError("review native scratch must be this invocation's own canonical scratch directory")
    session = copy.deepcopy(runner["role_session"])
    if current.get("previous"):
        session.update(mode="resume_existing", previous=current["previous"])
    elif stage == "score":
        session.update(mode="resume_existing", previous=_reference(facts_root / "collection.json", "exact sealed factual turn"))
    runner.update(role="review-" + stage, intent_id=current["intent_id"], policy_version=spec["policy_id"],
                  cwd=str(work), role_session=session, inputs=inputs, outputs=[stage + ".json"], output_trees=[],
                  candidate_binding=candidate_binding(source["candidate"]), run_binding=run_binding)
    if stage == "score":
        runner["facts_attempt"] = str(facts_root)
        runner["inputs"] += [{key: row[key] for key in ("path", "target", "kind")} for row in source["teacher_inputs"]]
        runner["inputs"].append({"path": seal["path"], "target": "execution/sealed-facts.json", "kind": "submission"})
    context_path = work / ".taskflow-assessment-context.json"
    publish_once(context_path, context)
    runner["dependencies"] = [*runner.get("dependencies", []), str(context_path)]
    command = ["rtk", "proxy", runner.get("native_permissions", {}).get("python", sys.executable),
               str(Path(current["runtime"]) / "inputs/execution/assessment-preflight.py"),
               "--directory", str(work), "--intent-id", current["intent_id"], "--inputs-root", context["inputs_root"],
               "--context", str(context_path)]
    if current.get("correction_errors"):
        instructions = ("Continue only your original " + stage + " stage in this exact reviewer context. "
            "Preserve true substantive findings and the original " + stage + ".json; sign a distinct corrected delivery "
            "with the current binding/intent. Read the current locked context at " + str(context_path)
            + " as needed, preserving every business_lock field exactly. Correct only these actual contract errors, "
            "without assuming acceptance, changing standards or adding new judgments: "
            + json.dumps(current["correction_errors"], ensure_ascii=False) + ". ")
    elif stage == "facts":
        instructions = ("Independently read every actual public input and every formal work/scratch/trace/process source. "
            "Do not seek references, rubric, author research, expected scores or other sessions. "
            "Write facts.json with exactly schema_version=1,kind=review_facts_v1,binding,status,coverage,facts,limitations. "
            "Use binding from execution/review-context.json plus this invocation intent_id. "
            "status is complete or incomplete. coverage has every context.sources path/SHA exactly once, each row "
            "{path,sha256,status:read|inspected|unresolved,location,limitation}; never hide unread sources. "
            "facts is a list of {id,statement,evidence:[{path,sha256,location}]}. Retain actual uncertainty. ")
    else:
        instructions = ("Continue your own original factual review in this same reviewer context. The factual file is sealed: "
            "do not rewrite it. Read the explicit current frozen standards now added and determine the original score yourself. "
            "Write score.json with exactly schema_version=1,kind=review_score_v1,binding,outcome,facts_sha256,validity,criteria,total,limitations. "
            "outcome is scored or inconclusive. validity={local_eligible:true|false|null,reason,evidence:[citation]}; "
            "do not invent official certification or served model identity. Each criterion is {id,points,max_points,reason,evidence:[citation]}. "
            "Use every frozen criterion for a scored result; total must equal your original points sum. Inconclusive keeps total=null. "
            "Do not adjust standards or choose a target score. Read the exact current binding, criteria, scoring and source locators "
            "from " + str(context_path) + " without rewriting that dependency. ")
    runner["request_sections"] = [{"kind": "operation", "text": instructions
        + "All citations/coverage locations use actual L1[-L2] lines or half-open B0-B1 byte ranges. "
        + "This invocation intent_id=" + current["intent_id"] + ". Locked inputs: " + context["inputs_root"]
        + ". Read the locked context file as needed; do not modify or remove it. "
        + "Use your own current work/scratch only. Do not start another model or next stage. "
        + "Before finishing run this read-only structure preflight and independently correct your own delivery if needed: "
        + shlex.join(command) + ". It never chooses a score or certifies substantive truth."}]
    _permissions(runner, source, public_root, spec)
    prepare_work(store, record, current, runner)
    current["prepared"] = True
    state.update(stage=stage, status="ready", **_initial_result())
    return save_task(store, record, state, "assessment_" + stage + "_prepared")


def prepare_assessment_task(store, identity: str, stage: str = "facts") -> dict:
    """Prepare exactly one requested phase; scoring is never automatic."""
    stage = _stage(stage)
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        return _prepare(store, _get(store, identity), stage)


def _collect(store, record: dict, stage: str) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    current = _current(state, stage)
    attempt = original_attempt(store, current)
    if attempt is None:
        return record
    current["attempt_id"] = attempt["attempt_id"]
    result = {**_initial_result(), "execution": execution_state(attempt)}
    if result["execution"] == "completed":
        try:
            source = _sources(store, spec)
            attempt, manifest, collection = completed_original(store, current, source["candidate"], "review-" + stage)
            current["sid"] = collection["sid"]
            current["artifact"] = evidence_ref(Path(current["runtime"]) / "artifacts" / (stage + ".json"), "original " + stage)
            archive = Path(current["root"]) / "archive"
            current["archive"] = archive_full_attempt(Path(current["runtime"]), archive)
            if collection["sid"] in source["excluded_sids"]:
                raise TaskflowError("reviewer reused an author/quality/solver context")
            if stage == "score":
                seal, _ = _sealed(record)
                if collection["sid"] != seal["sid"]:
                    raise TaskflowError("scoring does not continue the actual sealed factual reviewer")
            else:
                seal = None
            context = _context(spec, source, current, stage, seal)
            if stage == "score":
                signed = _read(Path(current["runtime"]) / "artifacts/score.json")
                business_payload(signed, context)
                if "business_lock" in current:
                    validate_business_values(signed, context)
            checked = check_delivery(context, Path(current["runtime"]) / "artifacts")
            document = _read(Path(current["runtime"]) / "artifacts" / (stage + ".json"))
            result.update(delivery="valid", outcome=checked["outcome"])
            if stage == "score":
                receipt = {"schema_version": 1, "kind": "original_score_receipt_v1", "assessment_task_id": record["flow_id"],
                    "solver_receipt": source["receipt_reference"], "binding": context["binding"],
                    "facts_seal": state["facts_seal"], "sid": collection["sid"], "attempt_id": attempt["attempt_id"],
                    "manifest": evidence_ref(Path(current["runtime"]) / "manifest.json", "original scoring manifest"),
                    "collection": evidence_ref(Path(current["runtime"]) / "collection.json", "original scoring collection"),
                    "score": current["artifact"], "outcome": document["outcome"], "total": document["total"], "validity": document["validity"]}
                path = Path(spec["directory"]) / "score-receipt.json"
                publish_once(path, receipt)
                state["score_receipt"] = evidence_ref(path, "unique original score receipt")
        except (TaskflowError, RunnerError, OSError, KeyError, TypeError, ValueError) as error:
            detail = error.as_dict() if isinstance(error, AssessmentContractError) else delivery_error(
                "assessment.invalid_source_or_delivery", str(error), correctable=False)
            correctable = (isinstance(error, AssessmentContractError)
                           and detail.get("code") not in {"assessment.signed_business_changed", "assessment.ambiguous_business_payload"})
            path = Path(current["runtime"]) / "artifacts" / (stage + ".json")
            try:
                document = _read(path)
                result["outcome"] = document.get("status" if stage == "facts" else "outcome")
            except (OSError, ValueError, TypeError):
                if stage == "score":
                    correctable = False
                    detail = {"code": "assessment.unparseable_original_score", "field": "delivery",
                              "message": "unparseable original score cannot authenticate its signed scalar values"}
            result.update(delivery="invalid", errors=[{**detail, "correctable": correctable}])
    current["result"] = copy.deepcopy(result)
    state["results"][stage] = result
    state.update(stage=stage, status=result["execution"], **result)
    return save_task(store, record, state, "assessment_" + stage + "_received")


def collect_assessment_task(store, identity: str, stage: str = "facts") -> dict:
    """Receive only this phase's original attempt, without choosing another turn."""
    stage = _stage(stage)
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity), stage)
    return _with_report(store, record, stage)


def _with_report(store, record: dict, stage: str) -> dict:
    """Expose a derived report after terminal scoring; never change the score."""
    if stage != "score" or record["state"]["results"][stage]["execution"] not in {"completed", "failed", "unknown"}:
        return record
    try:
        from taskflow.stress_report import build_report, write_report

        spec = record["definition"]
        report = build_report(store.path, spec["solver_task_id"], assessment_id=record["flow_id"])
        output = write_report(Path(spec["directory"]) / "reports", report)
        return {**record, "report": {"status": "available", **output}}
    except Exception as error:  # Presentation failure must not undo received scores.
        return {**record, "report": {"status": "unavailable", "error": str(error)}}


def seal_assessment_facts(store, identity: str) -> dict:
    """Explicitly seal a valid complete factual delivery; never start scoring."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _get(store, identity)
        if record["state"].get("facts_seal"):
            _, root = _sealed(record)
            archive_full_attempt(root, Path(_current(record["state"], "facts")["root"]) / "archive")
            return record
        record = _collect(store, record, "facts")
        state = copy.deepcopy(record["state"])
        result = state["results"]["facts"]
        if (tuple(result[key] for key in ("execution", "delivery", "outcome")) != ("completed", "valid", "complete")
                or result["errors"]):
            raise TaskflowError("only valid complete original facts can be sealed")
        root = Path(_current(state, "facts")["runtime"])
        seal_facts(root, root / "artifacts/facts.json")
        state["facts_seal"] = evidence_ref(root / "facts-seal.json", "sealed original complete factual review")
        return save_task(store, record, state, "assessment_facts_sealed")


def correct_assessment_task(store, identity: str, stage: str = "facts", evidence: list[dict] | None = None) -> dict:
    """Reserve one authorized same-phase format correction; do not run a model."""
    stage = _stage(stage)
    refs = validate_evidence(evidence or [])
    if not refs:
        raise TaskflowError("assessment correction requires actual authorization evidence")
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _get(store, identity)
        current = _current(record["state"], stage)
        if current.get("correction_evidence") == refs and not current.get("result"):
            return record
        record = _collect(store, record, stage)
        state = copy.deepcopy(record["state"])
        result = state["results"][stage]
        current = _current(state, stage)
        if (result["execution"] != "completed" or result["delivery"] != "invalid" or not result["errors"]
                or any(not error["correctable"] for error in result["errors"])
                or state.get("score_receipt") or (stage == "facts" and (state.get("facts_seal")
                    or (Path(current["runtime"]) / "facts-seal.json").exists()))):
            raise TaskflowError("only completed persistent invalid delivery can receive a same-phase correction")
        if state["corrections_used"] >= record["definition"]["correction_limit"]:
            raise TaskflowError("assessment correction allowance exhausted")
        following = invocation(record["definition"], stage, len(state["invocations"][stage]),
            previous=_reference(Path(current["runtime"]) / "collection.json", "exact previous completed review turn"),
            correction_errors=copy.deepcopy(result["errors"]), correction_evidence=refs)
        if stage == "score":
            source = _sources(store, record["definition"])
            seal, _ = _sealed(record)
            original = Path(current["runtime"]) / "artifacts/score.json"
            try:
                document = _read(original)
            except (ValueError, OSError) as error:
                raise TaskflowError("unparseable original score cannot authenticate its signed scalar values") from error
            context = _context(record["definition"], source, current, stage, seal)
            validate_business_values(document, context)
            following["business_lock"] = accumulate_business_payload(current.get("business_lock", {}), document,
                context)
            following["business_lock_sources"] = [*current.get("business_lock_sources", []),
                _reference(original, "original signed scalar values retained during delivery correction")]
        state["invocations"][stage].append(following)
        state["corrections_used"] += 1
        state["results"][stage] = _initial_result()
        state.update(stage=stage, status="ready", **_initial_result())
        return save_task(store, record, state, "assessment_" + stage + "_correction_reserved")


def run_assessment_task(store, identity: str, stage: str = "facts", allow_model: bool = False) -> dict:
    """Run at most the current explicitly selected review phase invocation."""
    stage = _stage(stage)
    record = prepare_assessment_task(store, identity, stage)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity), stage)
        if record["state"]["results"][stage]["execution"] == "not_started" and allow_model:
            source = _sources(store, record["definition"])
            check_public_view(Path(record["definition"]["directory"]) / "public", source["files"])
            run_worker(store, "explicit-assessment-task", True, [_current(record["state"], stage)["work_id"]])
            record = _collect(store, _get(store, identity), stage)
    return _with_report(store, record, stage)
