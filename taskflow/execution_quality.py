"""One explicit independent quality task, with bounded same-reviewer corrections.

No business repair loop, parent lifecycle, source discovery or model retry is
created here. Every model turn keeps a separate work, intent, runtime and attempt.
"""

import copy
import json
from pathlib import Path
import re
import shlex
import sys

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, relative_path, sha256, validate_id
from taskflow.execution import enqueue_spec, reconcile, run_worker
from taskflow.execution_flow_contracts import NATIVE_SIGNOFF_PROTOCOL_SHA256, render_native_signoff_protocol, review_instructions
from taskflow.execution_quality_contracts import (
    candidate_binding, candidate_files, delivery_error, inspect_delivery,
    observation_inputs, public_predecessor,
)
from taskflow.execution_runner import RunnerError, _info, _publish, _publish_bytes, _read, _regular, _validate, _validated_collection, archive_full_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref, validate_evidence
from taskflow.execution_tasks import task_lock


KIND = "quality_task_v1"
ACTIVE = {"claimed", "starting", "running", "collecting", "uncertain"}
REPORT_FILES = {"report_file": "adoption-readiness.json", "issues_file": "issues.json", "intake_file": "intake.md"}


def _get(store: ExecutionStore, identity: str) -> dict:
    record = store.get_flow(identity)
    if record["definition"].get("kind") != KIND:
        raise TaskflowError("not an explicit quality task: " + identity)
    return record


def _save(store: ExecutionStore, record: dict, state: dict, event: str) -> dict:
    if state == record["state"]:
        return record
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": event})


def _publish_once(path: Path, value: object) -> None:
    if path.exists():
        if _read(path) != value:
            raise TaskflowError("quality original cannot be replaced: " + str(path))
        return
    _publish(path, value)


def _publish_copy(path: Path, raw: bytes) -> None:
    if path.exists():
        if _regular(path) != raw:
            raise TaskflowError("quality source cannot be replaced: " + str(path))
        return
    _publish_bytes(path, raw)


def _invocation(spec: dict, ordinal: int, **extra) -> dict:
    identity = "quality-" + sha256(canonical([spec["flow_id"], ordinal]))[:24]
    root = Path(spec["directory"]) / "invocations" / f"{ordinal:03d}"
    return {"ordinal": ordinal, "work_id": identity, "intent_id": identity,
            "root": str(root), "runtime": str(root / "runtime"),
            "work": str(root / "work"), "scratch": str(root / "scratch"), **extra}


def _sources(store: ExecutionStore, spec: dict) -> tuple[dict, dict, dict]:
    candidate = store.get_candidate(spec["candidate_id"])
    files = candidate_files(candidate, Path(spec["candidate_root"]))
    if candidate["task_id"] != spec["task_id"]:
        raise TaskflowError("quality task belongs to another captured candidate")
    try:
        predecessor, _ = public_predecessor(store, spec, candidate)
    except RunnerError as error:
        raise TaskflowError(str(error)) from error
    return candidate, files, predecessor


def register_quality_task(store: ExecutionStore, spec: dict, evidence: list[dict]) -> dict:
    """Register one immutable whole-quality purpose; never launches a model."""
    spec = copy.deepcopy(spec)
    if type(spec.get("schema_version")) is not int or spec["schema_version"] != 1:
        raise TaskflowError("quality schema_version must be 1")
    for key in ("flow_id", "task_id", "owner", "policy_id", "candidate_id", "predecessor_attempt"):
        validate_id(spec.get(key), key)
    for key in ("directory", "candidate_root", "predecessor_archive"):
        if not isinstance(spec.get(key), str) or not Path(spec[key]).is_absolute():
            raise TaskflowError("quality " + key + " must be absolute")
    if not isinstance(spec.get("purpose"), str) or not spec["purpose"].strip():
        raise TaskflowError("quality purpose required")
    spec.setdefault("scope", "whole_package")
    if spec["scope"] not in ("whole_package", "affected"):
        raise TaskflowError("submit an explicit whole_package or affected quality task")
    if spec["scope"] == "whole_package" and spec.get("coverage") is not None:
        raise TaskflowError("whole quality must not claim inherited affected coverage")
    limit = spec.setdefault("correction_limit", 0)
    if type(limit) is not int or limit < 0:
        raise TaskflowError("correction_limit must be a nonnegative integer")
    ids = spec.setdefault("defect_ids", [])
    if not isinstance(ids, list) or len(ids) != len(set(ids)):
        raise TaskflowError("quality defect_ids must be an explicit unique list")
    for identity in ids:
        validate_id(identity, "quality defect ID")
        defect = store.get_defect(identity)
        if defect["state"] not in ("repaired", "closed") or defect.get("repair_candidate_id") != spec["candidate_id"]:
            raise TaskflowError("quality defect must identify this recorded actual repair")
    runner = spec.get("runner")
    if not isinstance(runner, dict) or runner.get("inputs"):
        raise TaskflowError("quality runner inputs are derived only from the exact declared sources")
    session = runner.get("role_session")
    if (not isinstance(session, dict) or session.get("mode") not in {"fresh_persistent", "resume_existing"}
            or not isinstance(session.get("home"), str)
            or not Path(session["home"]).is_absolute()):
        raise TaskflowError("quality requires fresh_persistent or an exact affected resume_existing role_session and own absolute home")
    resuming = session["mode"] == "resume_existing"
    if (resuming and (spec["scope"] != "affected" or not isinstance(session.get("previous"), dict))
            or not resuming and session.get("previous") is not None):
        raise TaskflowError("only an affected child may explicitly resume its exact parent QA")
    lineage = {"logical_role": "quality", "owner": spec["owner"], "task_id": spec["task_id"]}
    if runner.get("role_lineage") not in (None, lineage):
        raise TaskflowError("quality logical role, owner and task binding differs from its task")
    runner["role_lineage"] = lineage
    if runner.get("role_transition") is not None:
        raise TaskflowError("quality version transition is derived from its authenticated revision")
    listing = {"scope": spec["scope"], "index": "execution/public-inputs.json", "checkpoint": "public-responsibility-first.json"}
    if resuming:
        if runner.get("public_first_initial_listing") is not None:
            raise TaskflowError("informed QA recheck cannot claim a new blind public-first listing")
    else:
        if runner.get("public_first_initial_listing") not in (None, listing):
            raise TaskflowError("quality task requires the standard neutral public-first initial listing")
        runner["public_first_initial_listing"] = listing
    for key, default in REPORT_FILES.items():
        spec[key] = relative_path(spec.get(key, default))
        if spec[key] != default:
            raise TaskflowError("quality task uses the fixed shared delivery contract: " + default)
    if len({spec[key] for key in REPORT_FILES}) != len(REPORT_FILES):
        raise TaskflowError("quality delivery paths must be distinct")
    refs = validate_evidence(evidence)
    if not refs:
        raise TaskflowError("quality task requires actual authority/source evidence")
    spec["kind"] = KIND
    candidate, _, _ = _sources(store, spec)
    if spec["scope"] == "affected":
        from taskflow.execution_quality_coverage import coverage_plan, recheck_session

        recheck_session(coverage_plan(store, spec, candidate), spec)
    store.get_policy(spec["policy_id"])
    directory = Path(spec["directory"])
    with task_lock(directory, store=store):
        try:
            current = _get(store, spec["flow_id"])
        except TaskflowError:
            current = None
        if current is not None:
            if current["definition"] != spec or current["evidence"] != refs:
                raise TaskflowError("quality task cannot replace its definition or authority")
            return current
        state = {"status": "ready", "execution": "not_started", "delivery": "pending", "outcome": None,
                 "errors": [], "invocations": [_invocation(spec, 0)], "current_index": 0,
                 "corrections_used": 0, "candidate_id": spec["candidate_id"]}
        return store.register_flow(spec["flow_id"], spec, refs, state)


def _render(value: object, context: dict) -> object:
    if isinstance(value, str):
        def replacement(match):
            key = match.group(1)
            if key not in context:
                raise TaskflowError("unknown quality runner placeholder: " + key)
            return str(context[key])
        return re.sub(r"\{\{([a-zA-Z_][a-zA-Z_0-9]*)\}\}", replacement, value)
    if isinstance(value, list):
        return [_render(item, context) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            rendered_key = _render(key, context)
            if rendered_key in result:
                raise TaskflowError("quality runner keys collide after placeholder rendering: " + str(rendered_key))
            result[rendered_key] = _render(item, context)
        return result
    return value


def _prepare(store: ExecutionStore, record: dict) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    invocation = state["invocations"][state["current_index"]]
    candidate, files, _ = _sources(store, spec)
    coverage = None
    if spec["scope"] == "affected":
        from taskflow.execution_quality_coverage import coverage_plan

        coverage = coverage_plan(store, spec, candidate)
    root, work, scratch = (Path(invocation[key]) for key in ("root", "work", "scratch"))
    for path in (root, work, scratch):
        path.mkdir(parents=True, exist_ok=True)
    shared = Path(spec["directory"]) / "sources"
    public_index = {"schema_version": 1, "input_digest": candidate["input_digest"], "files": [
        {key: row[key] for key in ("path", "role", "sha256", "size")}
        for _, row in sorted(files.items()) if row["role"] in PUBLIC_ROLES]}
    if not public_index["files"]:
        raise TaskflowError("quality task has no captured public inputs")
    public_first = {"index": "execution/public-inputs.json", "checkpoint": "public-responsibility-first.json",
                    "input_digest": candidate["input_digest"],
                    "trace_nonce": sha256(canonical([spec["flow_id"], candidate_binding(candidate)]))[:24],
                    "review_scope": spec["scope"]}
    if coverage is not None:
        public_first["review_mode"] = coverage["plan"]["public_review_mode"]
    stable = {**candidate_binding(candidate), "public_first": public_first}
    _publish_once(shared / "current.json", stable)
    _publish_once(shared / "public-inputs.json", public_index)
    observed = observation_inputs(Path(spec["predecessor_archive"]))
    inputs = [{"path": row["source"], "kind": "public" if row["role"] in PUBLIC_ROLES else "teacher", "target": name}
              for name, row in sorted(files.items())]
    inputs += [{"path": str(shared / "current.json"), "kind": "source_metadata", "target": "execution/current.json"},
               {"path": str(shared / "public-inputs.json"), "kind": "source_metadata", "target": "execution/public-inputs.json"},
               {"path": str(Path(spec["candidate_root"]) / "task.json"), "kind": "source_metadata", "target": "execution/task.json"}, *observed]
    protocol = shared / "quality-protocol.md"
    _publish_copy(protocol, render_native_signoff_protocol().encode("utf-8"))
    inputs.append({"path": str(protocol), "kind": "operation", "target": "execution/quality-protocol.md"})
    if coverage is not None:
        from taskflow.execution_quality_coverage import coverage_inputs

        inputs += coverage_inputs(coverage, shared, _publish_once)
    targets = {"phase": "final", "scope": spec["scope"], "defect_ids": spec["defect_ids"], "candidate_binding": candidate_binding(candidate),
               "require_native": True, "native_protocol_sha256": NATIVE_SIGNOFF_PROTOCOL_SHA256,
               "candidate_paths": sorted(files), "observed_paths": [row["target"] for row in observed], "source_errors": []}
    _publish_once(shared / "quality-target-paths.json", targets)
    helper = Path(__file__).with_name("execution_quality_progress_contract.py")
    if not invocation["ordinal"] or not (shared / "quality-target-preflight.py").exists():
        _publish_copy(shared / "quality-target-preflight.py", _regular(helper))
    inputs += [{"path": str(shared / "quality-target-paths.json"), "kind": "source_metadata", "target": "execution/quality-target-paths.json"},
               {"path": str(shared / "quality-target-preflight.py"), "kind": "source_metadata", "target": "execution/quality-target-preflight.py"}]
    context = {**stable, **invocation, "runtime": invocation["runtime"], "inputs_root": str(Path(invocation["runtime"]) / "inputs"),
               "defect_ids": spec["defect_ids"]}
    if coverage is not None:
        context["repair_diff"] = {"available": True, "index": "execution/revision-diff.json"}
    resuming = spec["runner"]["role_session"]["mode"] == "resume_existing"
    runner = _render(copy.deepcopy(spec["runner"]), context)
    runner.update(role="quality", intent_id=invocation["intent_id"], policy_version=spec["policy_id"],
                  cwd=str(work), inputs=inputs, outputs=[spec[key] for key in REPORT_FILES] + ["public-responsibility-first.json"])
    runner.setdefault("output_trees", [])
    if resuming and not invocation["ordinal"]:
        from taskflow.execution_quality_coverage import recheck_session
        from taskflow.execution_role_session import input_changes

        source = recheck_session(coverage, spec)
        runner["role_transition"] = {
            "kind": "quality_recheck_v1", "source_binding": source["manifest"]["candidate_binding"],
            "target_binding": candidate_binding(candidate),
            "revision": {**coverage["revision"]["record"]["state"]["diff"],
                         **_info(Path(coverage["revision"]["record"]["state"]["diff"]["path"]))},
            "input_changes": input_changes(source["manifest"]["inputs"], inputs)}
        native = source["manifest"].get("native")
        if native and native["repository"] != runner["native_permissions"]["repository"]:
            runner["role_transition"]["read_root_transition"] = {
                "before": native["repository"], "after": runner["native_permissions"]["repository"]}
    if invocation["ordinal"]:
        runner.pop("public_first_initial_listing", None)
        runner["role_session"] = {"mode": "resume_existing", "home": spec["runner"]["role_session"]["home"],
                                  "previous": invocation["previous"]}
        sections = [{"kind": "operation", "text":
            "Continue only your original independent quality task in the same context. "
            "The prior delivery and its business claims remain original evidence. Correct only the reported contract errors, "
            "independently determine responsibility and preserve truthful blocking findings or uncertainty. "
            "Do not presume acceptance, rerun public reconstruction, repair candidate files or start another model. "
            "Sign this new delivery with its new intent_id; do not copy the prior intent signature. "
            "public-responsibility-first.json is seeded with your sealed original checkpoint; preserve it byte-for-byte, "
            "do not repeat or retrospectively claim a new blind public reading. "
            + json.dumps({"contract_errors": invocation["correction_errors"]}, ensure_ascii=False)}]
    else:
        sections = list(runner.get("request_sections", []))
        if resuming:
            from taskflow.execution_quality_contracts import new_caller_sections

            sections = new_caller_sections(source, spec["runner"].get("request_sections", []), sections)
            sections.append({"kind": "operation", "text":
                "Continue your exact original independent QA in this repaired child version. You have already seen teacher material; "
                "this is an informed affected recheck, never a new blind review. Preserve the original outcome, findings and signature. "
                "First locate and reread current affected public business units from execution/public-inputs.json; "
                "save public-responsibility-first.json with review_mode=informed_recheck, parent_checkpoint_sha256 equal to "
                "execution/parent-public-checkpoint.json and actual current source positions. Retain its actual successful tool readback "
                "PUBLIC_RESPONSIBILITY_FIRST_SHA256 before reading current teachers and revision evidence. "
                "In the report public_first add mode=informed_recheck and parent_checkpoint_sha256 with the same actual parent digest. "
                "The current checkpoint proves current rereading only. It does not establish a new blind context. "
                "Read complete necessary current units and changed originals even when you remember the old version."})
    if invocation["ordinal"] or resuming:
        sections.append({"kind": "operation", "text":
            "Keep the full independent-quality delivery protocol already supplied to this exact QA context. "
            "Use the current execution/current.json candidate binding and this invocation's new intent_id in "
            "execution_routing and issues.execution_binding; scope=" + spec["scope"] + ". "
            "Retain the current complete normative selection, truthful outcome, blocking issues, responsibilities, "
            "necessary public conditions and lawful methods. covered_defect_ids must match execution/quality-target-paths.json. "
            "Do not copy an old signature, change old findings or scores, repair candidate material, or treat an observed "
            "public-solver error as a candidate repair target. execution/quality-protocol.md retains the complete protocol "
            "for necessary rereading; do not repeat its body or read unchanged sections without a current need. "
            "The original supplied protocol is authenticated through the precise prior collection/request chain."})
    else:
        sections.append({"kind": "operation", "text": review_instructions(context, spec["scope"])})
    if invocation["ordinal"]:
        sections.append({"kind": "operation", "text":
            "Keep public_first tied to the seeded original checkpoint and original successful first-turn tool anchors. "
            "Do not manufacture new blind reads or retrospectively replace order evidence. Correct only the stated errors; "
            "the original signature and technical collection remain unchanged."})
    else:
        sections.append({"kind": "operation", "text":
            "In adoption-readiness.json retain public_first={checkpoint_sha256,trace_nonce,teacher_path}. "
            "Use trace_nonce=" + public_first["trace_nonce"] + ". Retain the initial successful tool output "
            "PUBLIC_RESPONSIBILITY_FIRST_SHA256=<actual SHA256> from reading back the saved checkpoint; "
            "do not repeat it now or restart the blind phase. "
            "Only afterward actually read a current teacher file; in that successful read response print "
            "TEACHER_SOURCE_PATH=<its locked target>, SHA256=<its actual SHA256>, and the same QUALITY_TRACE_NONCE. "
            "These are tool output correlation markers, not self-attestations; do not merely echo values without reading originals. "
            "During correction cite the original first-turn output anchors, not new reads. Missing or unsuccessful order evidence "
            "is unresolved; an echo or a later print cannot retrospectively establish a blind public stage. "
            "These anchors verify visible order only, never hidden context or tool-domain isolation."})
    if coverage is not None:
        from taskflow.execution_quality_coverage import coverage_instructions

        if not invocation["ordinal"]:
            sections.append({"kind": "operation", "text": coverage_instructions(coverage, compact=resuming)})
    command = shlex.join(["rtk", "proxy", runner.get("native_permissions", {}).get("python", sys.executable),
                         str(Path(invocation["runtime"]) / "inputs/execution/quality-target-preflight.py"),
                         "--context", str(Path(invocation["runtime"]) / "inputs/execution/quality-target-paths.json"),
                         "--intent-id", invocation["intent_id"],
                         "--directory", str(work)])
    sections.append({"kind": "operation", "text":
        "This invocation has intent_id=" + invocation["intent_id"] + "; write only " + str(work) + " and " + str(scratch)
        + ". Read locked inputs from " + context["inputs_root"] + ". Deliver " + ", ".join(runner["outputs"])
        + ". Before ending the turn run the read-only shared delivery preflight: " + command
        + ". It checks structure, not business acceptance. Preserve real unresolved findings."})
    runner["request_sections"] = sections
    work_spec = {"work": {"work_id": invocation["work_id"], "task_id": spec["task_id"], "candidate_id": candidate["candidate_id"],
                          "role": "quality", "policy_id": spec["policy_id"], "intent_id": invocation["intent_id"],
                          "purpose": spec["purpose"], "dispatch_owner": spec["owner"], "evidence": record["evidence"]}, "runner": runner}
    if runner.get("model_slot") is not None:
        work_spec["work"]["model_slot"] = runner["model_slot"]
    _publish_once(root / "spec.json", work_spec)
    try:
        existing = store.get_work(invocation["work_id"])
    except TaskflowError:
        existing = None
    if existing is None:
        enqueue_spec(store, work_spec, Path(invocation["runtime"]), resume_prepared=True, probe_native=True)
    elif any(existing[key] != value for key, value in {
            "intent_id": invocation["intent_id"], "candidate_id": candidate["candidate_id"],
            "runtime_path": invocation["runtime"], "policy_id": spec["policy_id"]}.items()):
        raise TaskflowError("quality work identity already belongs to another invocation")
    if invocation["ordinal"]:
        original = Path(state["invocations"][0]["runtime"]) / "artifacts/public-responsibility-first.json"
        _publish_copy(work / original.name, _regular(original))
        invocation["seeds"] = [evidence_ref(original, "same reviewer sealed original public checkpoint")]
    invocation["prepared"] = True
    state.update(status="ready", execution="not_started", delivery="pending", outcome=None, errors=[])
    return _save(store, record, state, "quality_task_prepared")


def prepare_quality_task(store: ExecutionStore, identity: str) -> dict:
    """Prepare the exact current invocation without starting a model."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _get(store, identity)
        if record["state"]["invocations"][record["state"]["current_index"]].get("prepared"):
            return record
        return _prepare(store, record)


def _collect(store: ExecutionStore, record: dict) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    current = state["invocations"][state["current_index"]]
    attempts = store.list_attempts(current["work_id"])
    if not attempts:
        return record
    if len(attempts) != 1:
        raise TaskflowError("quality invocation has more than one original attempt")
    attempt = attempts[0]
    if attempt["state"] in ACTIVE:
        attempt = reconcile(store, attempt["attempt_id"], resume_pending=False)
    current["attempt_id"] = attempt["attempt_id"]
    state["execution"] = "unknown" if attempt["state"] == "uncertain" else "running" if attempt["state"] in ACTIVE else attempt["state"]
    state["status"] = state["execution"]
    if attempt["state"] != "completed":
        return _save(store, record, state, "quality_task_execution_observed")
    candidate, _, predecessor = _sources(store, spec)
    root = Path(current["runtime"])
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    excluded = set(candidate.get("external_author_sids", [])) | {predecessor["cli_sid"]}
    if candidate.get("author_attempt"):
        excluded.add(store.get_attempt(candidate["author_attempt"])["cli_sid"])
    errors = []
    expected = None
    if current["ordinal"]:
        original = state["invocations"][0]
        original_attempt = store.get_attempt(original["attempt_id"])
        expected = original_attempt["cli_sid"]
    if attempt["cli_sid"] in excluded or (expected is not None and attempt["cli_sid"] != expected):
        errors.append(delivery_error("quality.independent_sid", "quality context is not its bound independent reviewer", correctable=False))
    archive = archive_full_attempt(root, Path(current["root"]) / "archive")
    coverage = None
    if spec["scope"] == "affected":
        from taskflow.execution_quality_coverage import coverage_plan, recheck_session

        coverage = coverage_plan(store, spec, candidate)
        excluded.update(coverage["parent_source"]["excluded_sids"])
        continuation = recheck_session(coverage, spec)
        if continuation is not None:
            # Only this exact prior QA is reusable; author and public contexts
            # are checked independently below and remain forbidden.
            excluded.discard(continuation["collection"]["sid"])
            excluded.update(candidate.get("external_author_sids", []))
            excluded.add(predecessor["cli_sid"])
            excluded.add(coverage["revision"]["collection"]["sid"])
            if attempt["cli_sid"] != continuation["collection"]["sid"]:
                errors.append(delivery_error("quality.recheck_sid", "affected recheck did not retain the precise original QA SID", correctable=False))
        if attempt["cli_sid"] in excluded:
            errors.append(delivery_error("quality.independent_sid", "affected reviewer reused an excluded historical context", correctable=False))
    result = inspect_delivery(candidate=candidate, attempt=attempt, root=root,
                archive=Path(spec["predecessor_archive"]), report_file=spec["report_file"], issues_file=spec["issues_file"],
                original_public_root=Path(state["invocations"][0]["runtime"]), scope=spec["scope"], coverage=coverage, defect_ids=spec["defect_ids"])
    errors.extend(result["errors"])
    if current["ordinal"] and _regular(root / "artifacts/public-responsibility-first.json") != _regular(
            Path(state["invocations"][0]["runtime"]) / "artifacts/public-responsibility-first.json"):
        errors.append(delivery_error("quality.public_first", "correction changed the sealed original public checkpoint", correctable=False))
    refs = {key: evidence_ref(root / "artifacts" / spec[key], "original quality " + key) for key in REPORT_FILES}
    state.update(execution="completed", status="completed", delivery="invalid" if errors else "valid",
                 outcome=result["outcome"], errors=errors)
    current.update(archive=archive, artifacts=refs, sid=collected["sid"],
                   delivery=state["delivery"], outcome=state["outcome"], errors=errors)
    if not errors:
        review_id = current["work_id"] + "-review"
        try:
            stored = store.get_review(review_id)
        except TaskflowError:
            parsed = result["parsed"]
            stored = store.record_review(review_id, candidate["candidate_id"], attempt["attempt_id"],
                verdict={"accepted": "accepted", "revision_required": "defects", "inconclusive": "inconclusive"}[parsed["outcome"]],
                scope=spec["flow_id"], evidence=list(refs.values()), whole_package=spec["scope"] == "whole_package",
                covered_defect_ids=parsed["covered_defect_ids"])
        current["review_id"] = stored["review_id"]
        defect_ids = []
        for finding in result["parsed"]["defects"]:
            defect_id = "quality-defect-" + sha256(canonical([current["work_id"], finding["id"]]))[:24]
            try:
                defect = store.get_defect(defect_id)
            except TaskflowError:
                defect = store.open_defect(defect_id, candidate["candidate_id"], stored["review_id"],
                    responsibility=finding["responsibility"], evidence=finding["evidence"], kind=finding["kind"])
            defect_ids.append(defect["defect_id"])
        current["defect_ids"] = defect_ids
        if result["outcome"] == "accepted":
            if spec["scope"] == "affected":
                record = _save(store, record, state, "quality_task_signature_received")
            closure_errors = []
            for identity in spec["defect_ids"]:
                defect = store.get_defect(identity)
                if defect["state"] == "closed" and defect.get("closure_review_id") == stored["review_id"]:
                    continue
                try:
                    store.close_defect(identity, stored["review_id"], evidence=list(refs.values()))
                except TaskflowError as error:
                    closure_errors.append({"defect_id": identity, "error": str(error)})
            if closure_errors:
                state["closure_errors"] = closure_errors
            else:
                state.pop("closure_errors", None)
            try:
                store.quality_gate(candidate["candidate_id"])
                state["quality_gate"] = {"accepted": True, "reasons": []}
            except TaskflowError as error:
                state["quality_gate"] = {"accepted": False, "reasons": [str(error)]}
    current["result"] = {key: state[key] for key in ("execution", "delivery", "outcome", "errors")}
    return _save(store, record, state, "quality_task_delivery_received")


def collect_quality_task(store: ExecutionStore, identity: str) -> dict:
    """Receive only the original attempt; invalid reports remain original evidence."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        return _collect(store, _get(store, identity))


def correct_quality_task(store: ExecutionStore, identity: str, evidence: list[dict]) -> dict:
    """Reserve one bounded same-reviewer correction, without executing a model."""
    refs = validate_evidence(evidence)
    if not refs:
        raise TaskflowError("quality correction requires actual authorization evidence")
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _get(store, identity)
        state = copy.deepcopy(record["state"])
        current = state["invocations"][state["current_index"]]
        if current.get("correction_evidence") == refs and not current.get("result"):
            return record
        record = _collect(store, record)
        state = copy.deepcopy(record["state"])
        current = state["invocations"][state["current_index"]]
        if (state["execution"] != "completed" or state["delivery"] != "invalid"
                or not state["errors"] or any(not error["correctable"] for error in state["errors"])):
            raise TaskflowError("only a completed persistent delivery with correctable contract errors can be corrected")
        if state["corrections_used"] >= record["definition"]["correction_limit"]:
            raise TaskflowError("quality correction allowance exhausted")
        root = Path(current["runtime"])
        _validated_collection(root, _validate(root))
        ordinal = state["current_index"] + 1
        following = _invocation(record["definition"], ordinal,
            previous={**evidence_ref(root / "collection.json", "exact completed prior quality turn"),
                      **_info(root / "collection.json")},
            correction_errors=copy.deepcopy(state["errors"]), correction_evidence=refs,
            source_index=state["current_index"])
        state["invocations"].append(following)
        state.update(current_index=ordinal, corrections_used=state["corrections_used"] + 1,
                     status="ready", execution="not_started", delivery="pending", outcome=None, errors=[])
        state.pop("quality_gate", None)
        return _save(store, record, state, "quality_task_correction_reserved")


def run_quality_task(store: ExecutionStore, identity: str, allow_model: bool = False) -> dict:
    """Run at most the current explicit invocation; never advances a business loop."""
    record = prepare_quality_task(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity))
        state = record["state"]
        if state["execution"] != "not_started":
            return record
        current = state["invocations"][state["current_index"]]
        if not allow_model:
            return record
        run_worker(store, "explicit-quality-task", allow_model, [current["work_id"]])
        return _collect(store, _get(store, identity))
