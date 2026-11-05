"""One explicit material revision on an independent copy, without a lifecycle.

The task binds a parent and a purpose. Codex edits its own copy and declares the
changes; Python authenticates the collected bytes and their complete difference.
It never chooses business changes, grants quality, or starts the next role.
"""

import copy
from pathlib import Path

from taskflow.contracts import (
    PUBLIC_ROLES, TaskflowError, canonical, relative_path, sha256, validate_id,
)
from taskflow.execution import capture_candidate, run_worker
from taskflow.execution_flow_contracts import repair_defect_ids, validate_repair
from taskflow.execution_quality_contracts import candidate_binding, candidate_files, delivery_error
from taskflow.execution_runner import (
    RunnerError, _info, _read, _regular, _role_session_source, _validate,
    archive_full_attempt,
)
from taskflow.execution_solver_contracts import completed_original, verified_full_archive
from taskflow.execution_iteration import validate_purpose_kind
from taskflow.execution_revision_contracts import (
    HISTORY_ROLES, history_inventory, observed_source_use, responsibility_analysis,
)
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.execution_tasks import (
    check_native_view, execution_state, get_task, invocation, original_attempt,
    prepare_work, publish_copy, publish_once, render, save_task, task_lock,
)
from taskflow.packages import check_task


KIND = "revision_task_v1"


def _get(store, identity: str) -> dict:
    record = get_task(store, identity, KIND)
    expected = invocation(record["definition"], "revision")
    if any(record["state"]["invocation"].get(key) != value for key, value in expected.items()):
        raise TaskflowError("revision invocation differs from its registered identity or paths")
    return record


def _parent(store, spec: dict) -> tuple[dict, dict]:
    parent = store.get_candidate(spec["candidate_id"])
    if parent["task_id"] != spec["task_id"]:
        raise TaskflowError("revision parent belongs to another task")
    files = candidate_files(parent, Path(spec["candidate_root"]))
    raw = _regular(Path(spec["candidate_root"]) / "task.json")
    refs = [ref for ref in parent["evidence"] if Path(ref["path"]).name == "task.json"]
    if len(refs) != 1 or refs[0]["sha256"] != sha256(raw):
        raise TaskflowError("revision parent declaration differs from its captured original")
    return parent, files


def _paths(spec: dict) -> None:
    for key in ("directory", "candidate_root"):
        value = spec.get(key)
        if (not isinstance(value, str) or not Path(value).is_absolute()
                or Path(value).resolve() != Path(value)):
            raise TaskflowError("revision " + key + " must be an absolute canonical ordinary path")
    directory, parent = Path(spec["directory"]), Path(spec["candidate_root"])
    if directory == parent or directory.is_relative_to(parent) or parent.is_relative_to(directory):
        raise TaskflowError("revision control directory and parent must be independent")


def register_revision_task(store, spec: dict, evidence: list[dict]) -> dict:
    """Register a concrete revision; candidate_id always names the parent input."""
    spec = copy.deepcopy(spec)
    if type(spec.get("schema_version")) is not int or spec["schema_version"] != 1:
        raise TaskflowError("revision schema_version must be 1")
    for key in ("flow_id", "task_id", "owner", "policy_id", "candidate_id", "revised_candidate_id"):
        validate_id(spec.get(key), key)
    if spec["revised_candidate_id"] == spec["candidate_id"]:
        raise TaskflowError("revision requires a distinct prospective candidate identity")
    _paths(spec)
    if not isinstance(spec.get("purpose"), str) or not spec["purpose"].strip():
        raise TaskflowError("revision purpose required")
    spec["purpose_kind"] = validate_purpose_kind(spec.get("purpose_kind"))
    validate_id(spec.get("author_id"), "logical author id")
    expected = spec.get("expected_changes")
    if not isinstance(expected, list):
        raise TaskflowError("revision expected_changes must explicitly be a list")
    seen = set()
    for change in expected:
        if not isinstance(change, dict):
            raise TaskflowError("revision expected change must be an object")
        name = relative_path(change.get("path"))
        if name in seen or not isinstance(change.get("reason"), str) or not change["reason"].strip():
            raise TaskflowError("revision expected changes require unique paths and reasons")
        seen.add(name)
    responsibilities = spec.get("responsibilities")
    if not isinstance(responsibilities, list) or not responsibilities:
        raise TaskflowError("revision requires explicit associated business responsibilities")
    ids = set()
    for row in responsibilities:
        if not isinstance(row, dict):
            raise TaskflowError("revision responsibility must be an object")
        validate_id(row.get("id"), "responsibility id")
        if row["id"] in ids or not isinstance(row.get("description"), str) or not row["description"].strip():
            raise TaskflowError("revision responsibilities require unique IDs and descriptions")
        ids.add(row["id"])
        if not isinstance(row.get("paths"), list) or not row["paths"]:
            raise TaskflowError("revision responsibility requires explicit associated paths")
        for name in row["paths"]:
            relative_path(name)
    defects = spec.setdefault("defect_ids", [])
    repairs = spec.setdefault("defect_repairs", [])
    if (not isinstance(defects, list) or any(not isinstance(item, str) for item in defects)
            or len(defects) != len(set(defects))
            or not set(defects) <= ids):
        raise TaskflowError("revision defect_ids must be unique actual assigned responsibility IDs")
    if not isinstance(repairs, list):
        raise TaskflowError("revision defect_repairs must explicitly state each business disposition")
    for row in repairs:
        if (not isinstance(row, dict) or set(row) != {"defect_id", "disposition"}
                or not isinstance(row["defect_id"], str) or not isinstance(row["disposition"], str)
                or row["defect_id"] not in defects
                or row["disposition"] not in {"material_repair", "private_repair", "scope_adjustment"}):
            raise TaskflowError("revision defect repair requires an assigned ID and explicit valid disposition")
    if len(repairs) != len(defects) or {row["defect_id"] for row in repairs} != set(defects):
        raise TaskflowError("revision defect repair dispositions must cover exactly the assigned IDs")
    if not isinstance(spec.get("sources"), list):
        raise TaskflowError("revision sources must explicitly list the available originals")
    sources = validate_evidence(spec["sources"]) if spec["sources"] else []
    diagnosis = spec.setdefault("diagnosis", {"mode": "material_revision"})
    if not isinstance(diagnosis, dict) or diagnosis.get("mode") not in {"post_run", "material_revision"}:
        raise TaskflowError("revision diagnosis must explicitly identify post_run or material_revision")
    targets = set()
    for source in sources:
        target = relative_path(source.get("target"))
        if target in targets or not target.startswith("history/"):
            raise TaskflowError("revision source targets must be unique paths under history/")
        targets.add(target)
        if source.get("source_role", "other") not in HISTORY_ROLES:
            raise TaskflowError("revision source_role must identify its actual history category")
        if "size" in source and source["size"] != _info(Path(source["path"]))["size"]:
            raise TaskflowError("revision source size differs from its original")
    spec["sources"] = sources
    diagnosis["history_inventory"] = history_inventory(spec)
    runner = spec.get("runner")
    if not isinstance(runner, dict) or runner.get("mode") not in {"native_role", "test_fixture"}:
        raise TaskflowError("revision requires an explicit native_role runner or neutral fixture")
    if runner.get("inputs") or runner.get("request_text"):
        raise TaskflowError("revision inputs are derived only from the declared original sources")
    if any(key in runner for key in ("resume_sid", "quality_session", "facts_attempt", "public_first_initial_listing")):
        raise TaskflowError("revision cannot restore a historical or other-role context")
    if any(not isinstance(row, dict) or row.get("kind") != "operation"
           for row in runner.get("request_sections", [])):
        raise TaskflowError("revision request sections must be typed operations")
    session = runner.get("role_session")
    if (not isinstance(session, dict) or session.get("mode") not in {"fresh_persistent", "fresh_ephemeral", "resume_existing"}
            or not isinstance(session.get("home"), str)
            or not Path(session["home"]).is_absolute()):
        raise TaskflowError("revision requires an explicit author session and absolute home")
    if session["mode"] == "resume_existing":
        validate_id(spec.get("source_revision_task_id"), "source revision task id")
        validate_evidence([session.get("previous")])
    elif (session.get("previous") is not None or spec.get("source_revision_task_id") is not None
            or spec.get("session_reason") not in {
                "initial_author", "original_unrecoverable", "original_ephemeral", "author_changed"}):
        raise TaskflowError("fresh revision requires an explicit original-context availability reason")
    home, parent_root = Path(session["home"]), Path(spec["candidate_root"])
    if (home.resolve() != home or home == parent_root or home.is_relative_to(parent_root)
            or parent_root.is_relative_to(home)):
        raise TaskflowError("revision session home must be independent of the parent material")
    refs = validate_evidence(evidence)
    spec["kind"] = KIND
    _parent(store, spec)
    _author_source(store, spec)
    store.get_policy(spec["policy_id"])
    with task_lock(Path(spec["directory"]), store=store):
        publish_once(Path(spec["directory"]) / "revision-definition.json", {"definition": spec, "evidence": refs})
        try:
            current = store.get_flow(spec["flow_id"])
        except TaskflowError:
            current = None
        if current is not None:
            if current["definition"] != spec or current["evidence"] != refs:
                raise TaskflowError("revision cannot replace its original definition or authority")
            return current
        _validate_defect_sources(store, spec)
        return store.register_flow(spec["flow_id"], spec, refs, {
            "status": "ready", "execution": "not_started", "delivery": "pending", "outcome": None,
            "errors": [], "candidate_id": spec["candidate_id"], "invocation": invocation(spec, "revision")})


def _validate_defect_sources(store, spec: dict) -> None:
    """Bind explicit open findings to this actual parent lineage, without classifying impact."""
    ancestors, identity = set(), spec["candidate_id"]
    while identity:
        if identity in ancestors:
            raise TaskflowError("revision parent lineage has a cycle")
        ancestors.add(identity)
        identity = store.get_candidate(identity).get("parent_id")
    for identity in spec["defect_ids"]:
        defect = store.get_defect(identity)
        if (defect["task_id"] != spec["task_id"] or defect["candidate_id"] not in ancestors
                or defect["state"] != "open"):
            raise TaskflowError("revision assigned defect is not open and applicable to its parent lineage")
        validate_evidence(defect["evidence"])


def _author_source(store, spec: dict) -> dict | None:
    """Authenticate only the explicitly named logical author's parent-producing turn."""
    session = spec["runner"]["role_session"]
    if session["mode"] != "resume_existing":
        parent = store.get_candidate(spec["candidate_id"])
        if (parent.get("author_attempt") or parent.get("external_author_sids")) and spec["session_reason"] == "initial_author":
            raise TaskflowError("an existing parent author requires continuation or a recorded fresh exception")
        if spec["session_reason"] != "initial_author" and (
                not isinstance(spec.get("session_reason_detail"), str) or not spec["session_reason_detail"].strip()):
            raise TaskflowError("fresh author exception requires its actual reason")
        if spec["session_reason"] == "original_ephemeral":
            if not parent.get("author_attempt"):
                raise TaskflowError("original_ephemeral requires a verifiable captured original; unknown sources require original_unrecoverable and detail")
            try:
                attempt = store.get_attempt(parent["author_attempt"])
                manifest = _validate(Path(attempt["runtime_path"]))
            except (TaskflowError, RunnerError, OSError, KeyError, TypeError, ValueError) as error:
                raise TaskflowError("original_ephemeral requires a verifiable original manifest; use original_unrecoverable and detail when unavailable") from error
            if (attempt.get("task_id") != spec["task_id"] or manifest.get("role") not in {"author", "repair"}
                    or (manifest.get("role_session") or {}).get("mode") != "fresh_ephemeral"):
                raise TaskflowError("original_ephemeral requires an actual fresh_ephemeral parent author; persistent context cannot be relabeled")
        return None
    source = validate_revision_source(store, spec["source_revision_task_id"])
    definition = source["record"]["definition"]
    current = source["record"]["state"]["invocation"]
    exact = Path(current["runtime"]) / "collection.json"
    previous = session["previous"]
    lineage = {"logical_role": "author", "owner": spec["author_id"], "task_id": spec["task_id"]}
    if (definition.get("author_id") != spec["author_id"]
            or definition["task_id"] != spec["task_id"] or definition["policy_id"] != spec["policy_id"]
            or source["candidate"]["candidate_id"] != spec["candidate_id"]
            or source["manifest"].get("role_lineage") != lineage
            or source["manifest"].get("role_session", {}).get("home") != session["home"]
            or source["manifest"].get("role_session", {}).get("mode") not in {"fresh_persistent", "resume_existing"}
            or previous["path"] != str(exact) or any(previous.get(key) != value for key, value in _info(exact).items())):
        raise TaskflowError("author continuation must retain its exact parent-producing collection, logical author, task, policy and home")
    return source


def _new_author_sections(source: dict, declared: list[dict], rendered: list[dict]) -> list[dict]:
    """Omit only rendered text actually supplied in this precise author context."""
    prior = source["record"]["definition"]["runner"].get("request_sections", [])
    if not any(section in prior for section in declared):
        return rendered
    root, manifest = Path(source["attempt"]["runtime_path"]), source["manifest"]
    sid, home = source["collection"]["sid"], manifest["role_session"]["home"]
    lineage, requests, seen = manifest.get("role_lineage"), [], set()
    while root not in seen:
        seen.add(root)
        if (manifest.get("role") != "repair" or manifest.get("role_lineage") != lineage
                or manifest.get("role_session", {}).get("home") != home
                or _info(root / "request.md") != manifest["request"]):
            raise TaskflowError("caller instruction source differs from its exact original author context")
        requests.append(_regular(root / "request.md"))
        session = manifest["role_session"]
        if session["mode"] != "resume_existing":
            break
        previous = _role_session_source(session["previous"])
        if previous["collection"]["sid"] != sid:
            raise TaskflowError("caller instruction source changed its original author SID")
        root, manifest = Path(previous["root"]), previous["manifest"]
    else:
        raise TaskflowError("caller instruction source chain contains a cycle")
    return [current for original, current in zip(declared, rendered)
            if original not in prior or not any(current["text"].encode("utf-8") in request for request in requests)]


def _candidate(root: Path, identity: str, parent: dict) -> dict:
    result = check_task(root, write_report=False)
    if result["errors"] or result["task_id"] != parent["task_id"]:
        raise TaskflowError("revised candidate structure invalid: " + "; ".join(result["errors"]))
    declaration = _read(root / "task.json")
    expected = {"task.json", *(row["path"] for row in declaration["files"])}
    actual = set()
    for path in root.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            continue
        _regular(path)
        if path.stat().st_nlink != 1:
            raise TaskflowError("revised material cannot share a writable hard link")
        actual.add(str(path.relative_to(root)))
    if actual != expected:
        raise TaskflowError("revised material tree differs from its complete declared file membership")
    files = [{**evidence_ref(root / row["path"], "actual revision file"),
              "size": _info(root / row["path"])["size"], "package_path": row["path"], "role": row["role"]}
             for row in declaration["files"]]
    return {"candidate_id": identity, "parent_id": parent["candidate_id"], "task_id": result["task_id"],
            **{key: result[key] for key in ("content_digest", "input_digest", "evaluation_digest")}, "files": files}


def revision_difference(parent: dict, candidate: dict, parent_root: Path, candidate_root: Path) -> dict:
    """Compute complete actual role/member/byte and declaration differences."""
    before = {row["package_path"]: row for row in parent["files"]}
    after = {row["package_path"]: row for row in candidate["files"]}
    def info(row):
        return None if row is None else {"role": row["role"], **_info(Path(row["path"]))}
    changes = []
    for name in sorted(set(before) | set(after)):
        old, new = info(before.get(name)), info(after.get(name))
        if old != new:
            changes.append({"path": name, "before": old, "after": new})
    old, new = _info(parent_root / "task.json"), _info(candidate_root / "task.json")
    return {"schema_version": 1, "kind": "material_revision_diff_v1",
            "parent_binding": candidate_binding(parent), "candidate_binding": candidate_binding(candidate),
            "changes": changes, "declaration_change": None if old == new else {"before": old, "after": new}}


def _assert_copy(parent: dict, files: dict, directory: Path) -> None:
    candidate_files(parent, directory)
    if (directory / "task.json").stat().st_nlink != 1:
        raise TaskflowError("revision parent declaration copy cannot share a writable hard link")
    for name, source in files.items():
        copied = directory / name
        if copied.stat().st_nlink != 1 or copied.stat().st_ino == Path(source["source"]).stat().st_ino:
            raise TaskflowError("revision parent copy cannot share writable filesystem files")


def _prepare(store, record: dict) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    parent, files = _parent(store, spec)
    author_source = _author_source(store, spec)
    current = state["invocation"]
    directory = Path(spec["directory"])
    original = directory / "parent"
    work = Path(current["work"])
    writable = work / "candidate"
    if current.get("prepared") or state["outcome"] == "no_change":
        _assert_copy(parent, files, original)
        return record
    declaration = _regular(Path(spec["candidate_root"]) / "task.json")
    for name, row in files.items():
        raw = _regular(Path(row["source"]))
        publish_copy(original / name, raw)
        publish_copy(writable / name, raw)
    publish_copy(original / "task.json", declaration)
    publish_copy(writable / "task.json", declaration)
    _assert_copy(parent, files, original)
    current["parent_root"] = str(original)
    current["candidate_root"] = str(writable)
    actual = _candidate(writable, spec["revised_candidate_id"], parent)
    diff = revision_difference(parent, actual, original, writable)
    if not spec["expected_changes"] and not diff["changes"] and diff["declaration_change"] is None:
        state.update(status="completed", execution="not_started", delivery="valid", outcome="no_change")
        return save_task(store, record, state, "revision_no_change")
    sources = validate_evidence(spec["sources"]) if spec["sources"] else []
    shared = directory / "sources"
    index = {"schema_version": 1, "parent_binding": candidate_binding(parent),
             "history_inventory": spec["diagnosis"]["history_inventory"],
             "candidate_files": [{"path": name, "role": row["role"], "sha256": row["sha256"], "size": row["size"]}
                                 for name, row in sorted(files.items())],
             "diagnosis_sources": [{"path": source["target"], "sha256": source["sha256"],
                                    "size": _info(Path(source["path"]))["size"], "location": source["location"],
                                    "source_role": source.get("source_role", "other")}
                                   for source in sources]}
    publish_once(shared / "original-index.json", index)
    assignment = {"parent_id": parent["candidate_id"], "candidate_id": spec["revised_candidate_id"],
                  "purpose": spec["purpose"], "expected_changes": spec["expected_changes"],
                  "purpose_kind": spec["purpose_kind"], "author_id": spec["author_id"],
                  "session_reason": spec.get("session_reason", "same_logical_author_continuation"),
                  "responsibilities": spec["responsibilities"], "diagnosis": spec["diagnosis"],
                  "defect_ids": spec["defect_ids"], "defect_repairs": spec["defect_repairs"],
                  "original_index": "execution/original-index.json"}
    publish_once(shared / "revision-brief.json", assignment)
    old_sources = ({row["target"]: row for row in author_source["manifest"]["inputs"]}
                   if author_source else {})
    compact = {
        "parent_id": parent["candidate_id"], "candidate_id": spec["revised_candidate_id"],
        "purpose": spec["purpose"], "purpose_kind": spec["purpose_kind"],
        "expected_changes": spec["expected_changes"], "responsibilities": spec["responsibilities"],
        "defect_ids": spec["defect_ids"], "defect_repairs": spec["defect_repairs"],
        "history_availability": [{"source_role": row["source_role"], "status": row["status"]}
                                 for row in spec["diagnosis"]["history_inventory"]],
        "current_originals": [{"path": "parent/" + name, "role": row["role"]} for name, row in sorted(files.items())],
        "new_evidence": [row["target"] for row in sources
                         if old_sources.get(row["target"], {}).get("sha256") != row["sha256"]],
        "parent_difference": "execution/parent-revision-diff.json" if author_source else None,
        "complete_original_index": "execution/original-index.json",
        "delivery_contract": "execution/revision-contract.md",
    }
    publish_once(shared / "revision-context.json", compact)
    context = {**candidate_binding(parent), **current, "candidate_root": str(writable),
               "revision_root": str(writable), "parent_root": str(original),
               "inputs_root": str(Path(current["runtime"]) / "inputs"),
               "revised_candidate_id": spec["revised_candidate_id"]}
    runner = render(copy.deepcopy(spec["runner"]), context)
    runner["role_lineage"] = {"logical_role": "author", "owner": spec["author_id"], "task_id": spec["task_id"]}
    runner.update(role="repair", intent_id=current["intent_id"], policy_version=spec["policy_id"],
        cwd=str(work), inputs=[
            {"path": row["source"], "kind": "public" if row["role"] in PUBLIC_ROLES else "teacher", "target": "parent/" + name}
            for name, row in sorted(files.items())] + [
            {"path": str(original / "task.json"), "kind": "source_metadata", "target": "execution/parent-task.json"},
            {"path": str(shared / "original-index.json"), "kind": "source_metadata", "target": "execution/original-index.json"},
            {"path": str(shared / "revision-brief.json"), "kind": "source_metadata", "target": "execution/revision-brief.json"},
            {"path": str(shared / "revision-context.json"), "kind": "source_metadata", "target": "execution/revision-context.json"},
            *[{"path": source["path"], "kind": "history", "target": source["target"]} for source in sources]],
        outputs=["repair-record.json"], output_trees=["candidate"])
    if author_source:
        from taskflow.execution_role_session import input_changes

        runner["inputs"].append({"path": author_source["record"]["state"]["diff"]["path"],
                                 "kind": "source_metadata", "target": "execution/parent-revision-diff.json"})
        runner["role_transition"] = {
            "kind": "author_revision_v1",
            "source_binding": author_source["manifest"]["candidate_binding"],
            "target_binding": candidate_binding(parent),
            "revision": {**author_source["record"]["state"]["diff"],
                         **_info(Path(author_source["record"]["state"]["diff"]["path"]))},
            "input_changes": input_changes(author_source["manifest"]["inputs"], runner["inputs"]),
        }
        if runner["mode"] == "native_role":
            runner["role_transition"]["read_root_transition"] = {
                "before": author_source["record"]["state"]["invocation"]["parent_root"],
                "after": str(original)}
    runner["seeded_output_trees"] = {"candidate": [
        {"path": "candidate/" + name, "input_target": "parent/" + name,
         "sha256": row["sha256"], "size": row["size"]} for name, row in sorted(files.items())] + [
        {"path": "candidate/task.json", "input_target": "execution/parent-task.json", **_info(original / "task.json")} ]}
    protocol = (
        "Revise only the necessary material in your independent work/candidate copy. "
        "Start with execution/revision-context.json under ../runtime/inputs; complete original-index.json remains available. "
        "Locate the relevant originals, then read the complete necessary business units; originals remain available. "
        "Use available complete prior final answer, facts and original review to diagnose correct work, lawful short methods, "
        "important omissions and material/reference/rubric/environment/model issues. Do not restrict diagnosis to lost points. "
        "History inventory distinguishes unknown, nonexistent, archived but not provided, and supplied originals. "
        "Do not invent unavailable requests, tool returns, code or intermediate work, and do not rerun a model to fill gaps. "
        "For each assigned responsibility, write responsibility_analysis with id, result=success/failure/mixed/unknown, "
        "public_work,processing,final_result,business_consequence; each unit has status=supported/unknown, summary, "
        "evidence=[{target,location}] pointing to actual typed originals. Include lawful successful methods and the major "
        "responsibilities they still cover, not only errors. next_action has disposition=changed/retained/unknown, reason, "
        "changed_paths identifying actual edits (empty for retention/unknown). A good method may remain valid. "
        "Missing evidence stays unknown; file presence or a score change is not processing analysis. "
        "Also write success_method_coverage={status:supported/unknown,summary,covered_responsibilities:[description], "
        "evidence:[{target,location}]} for the major responsibilities still covered by lawful successful methods, even "
        "when assigned changes concern only failures. Missing processing evidence remains unknown without erasing "
        "a supported successful final result. Do not require a failure, a changed method or a new model run. "
        "Optional source_usage=[{target,trace_lines,analysis_ids}] requires exact visible completed tool returns and linked "
        "responsibility IDs; provided originals alone are not evidence of use. Reread the current affected business units "
        "even when the old version remains in context. "
        "Business changes and their impact are your responsibility. Preserve necessary public facts and lawful solutions; "
        "do not prefill solutions, suppress lawful methods to lower scores, or change old scores/weights retrospectively. "
        "Edit files directly; unchanged files are already retained, so do not regenerate or repeat the package. "
        "Write repair-record.json with role=repair, parent_id, candidate_id, defect_ids matching the explicit responsibility IDs, "
        "needs_business_decision boolean, changes=[{path,before_sha256,after_sha256,reason,impact}], "
        "declaration_change=null or {before_sha256,after_sha256,reason,impact}, and impact_analysis={summary, "
        "recommended_scope,associated_paths}; recommended_scope is a nonempty human-readable recommendation, "
        "not an automatic quality-scope selection. changes lists all and only actual payload member/role/byte changes, "
        "including deleted members, with null SHA for absence; task.json changes belong only in declaration_change. "
        "If nothing changed, keep changes empty and declaration_change null. Report unresolved business choices truthfully. "
        "Do not self-sign independent quality or initiate another model. Keep the final response concise: changed paths, "
        "reasons and impact; no repetition of original reports or unchanged material.")
    publish_copy(shared / "revision-contract.md", protocol.encode("utf-8"))
    runner["inputs"].append({"path": str(shared / "revision-contract.md"), "kind": "source_metadata",
                             "target": "execution/revision-contract.md"})
    if author_source:
        declared_sections = spec["runner"].get("request_sections", [])
        current_sections = _new_author_sections(
            author_source, declared_sections, render(declared_sections, context))
        runner["request_sections"] = [*current_sections, {"kind": "operation", "text":
            "Continue this logical author's revision. Read ../runtime/inputs/execution/revision-context.json for this "
            "purpose, responsibilities, expected changes, parent difference and new evidence. Reread the current affected "
            "complete business units and linked materials; remembering the parent does not replace verification. "
            "Keep the established repair-record contract and lawful-success coverage. A good method may remain valid. "
            "Complete originals and the delivery contract remain available through the context index. Edit only work/candidate "
            "and return the concise repair record; do not repeat unchanged materials or historical reports."}]
        runner["role_transition"]["input_changes"] = input_changes(author_source["manifest"]["inputs"], runner["inputs"])
    else:
        runner["request_sections"] = [*runner.get("request_sections", []), {"kind": "operation", "text": protocol}]
    check_native_view(runner, original, control_root=directory, forbidden=[Path(spec["candidate_root"])])
    prepare_work(store, record, current, runner)
    current["prepared"] = True
    return save_task(store, record, state, "revision_task_prepared")


def prepare_revision_task(store, identity: str) -> dict:
    """Prepare an independent writable copy and at most one exact work item."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        return _prepare(store, _get(store, identity))


def _validate_author(path: Path, *, spec: dict, parent: dict, candidate: dict, diff: dict) -> dict:
    report = _read(path)
    ids = [row["id"] for row in spec["responsibilities"]]
    if report.get("role") != "repair" or report.get("parent_id") != parent["candidate_id"] or report.get("candidate_id") != candidate["candidate_id"]:
        raise TaskflowError("revision declaration does not identify its parent, candidate and author role")
    repair_defect_ids(report, ids)
    if report.get("needs_business_decision") is not False:
        raise TaskflowError("revision requires a business decision; original author conclusion retained")
    if diff["changes"]:
        validate_repair(path, parent=parent, candidate=candidate, defect_ids=ids)
    elif report.get("changes") != []:
        raise TaskflowError("revision declares payload changes that did not occur")
    for row in report.get("changes", []):
        for key in ("reason", "impact"):
            if not isinstance(row.get(key), str) or not row[key].strip():
                raise TaskflowError("every changed file requires the author's reason and impact")
    declared = report.get("declaration_change")
    actual = diff["declaration_change"]
    if actual is None:
        if declared is not None:
            raise TaskflowError("revision declares a task.json change that did not occur")
    elif (not isinstance(declared, dict)
            or any(declared.get(key + "_sha256") != actual[key]["sha256"] for key in ("before", "after"))
            or any(not isinstance(declared.get(key), str) or not declared[key].strip() for key in ("reason", "impact"))):
        raise TaskflowError("revision declaration omits or misidentifies actual task.json changes")
    impact = report.get("impact_analysis")
    if (not isinstance(impact, dict) or not isinstance(impact.get("summary"), str) or not impact["summary"].strip()
            or not isinstance(impact.get("recommended_scope"), str)
            or not impact["recommended_scope"].strip()
            or not isinstance(impact.get("associated_paths"), list)):
        raise TaskflowError("revision requires the author's concrete impact and recommended review scope")
    for name in impact["associated_paths"]:
        relative_path(name)
    responsibility_analysis(report, spec, diff, parent)
    return report


def _collect(store, record: dict) -> dict:
    spec, state = record["definition"], copy.deepcopy(record["state"])
    current = state["invocation"]
    if state["delivery"] == "valid" and state["status"] == "completed":
        return record
    attempt = original_attempt(store, current)
    if attempt is None:
        return record
    current["attempt_id"] = attempt["attempt_id"]
    state.update(execution=execution_state(attempt), status=execution_state(attempt))
    if state["execution"] != "completed":
        return save_task(store, record, state, "revision_execution_observed")
    errors = []
    try:
        parent, files = _parent(store, spec)
        original = Path(current["parent_root"])
        _assert_copy(parent, files, original)
        attempt, manifest, collection = completed_original(store, current, parent, "repair")
        if attempt["policy_id"] != spec["policy_id"]:
            raise TaskflowError("revision original differs from its declared policy")
        runtime = Path(current["runtime"])
        archive = Path(current["root"]) / "archive"
        current["archive"] = archive_full_attempt(runtime, archive)
        root = runtime / "artifacts/candidate"
        candidate = _candidate(root, spec["revised_candidate_id"], parent)
        diff = revision_difference(parent, candidate, original, root)
        diff["revision_task_id"] = record["flow_id"]
        report_path = runtime / "artifacts/repair-record.json"
        report = _validate_author(report_path, spec=spec, parent=parent, candidate=candidate, diff=diff)
        usage = observed_source_use(report, spec, runtime)
        diff["author_record"] = evidence_ref(report_path, "original author change and impact declaration")
        diff_path = Path(spec["directory"]) / "revision-diff.json"
        publish_once(diff_path, diff)
        state["diff"] = evidence_ref(diff_path, "complete actual material and declaration differences")
        if not diff["changes"] and diff["declaration_change"] is None:
            state.update(outcome="no_change", candidate_id=parent["candidate_id"])
        else:
            try:
                captured = store.get_candidate(spec["revised_candidate_id"])
            except TaskflowError:
                captured = capture_candidate(store, root, spec["revised_candidate_id"], parent["candidate_id"], attempt["attempt_id"])
            if (candidate_binding(captured) != candidate_binding(candidate)
                    or captured.get("author_attempt") != attempt["attempt_id"] or captured.get("parent_id") != parent["candidate_id"]):
                raise TaskflowError("revision candidate ID already identifies another original")
            state.update(outcome="revised", candidate_id=candidate["candidate_id"], revised_candidate_root=str(root))
            repaired = []
            for declared in spec["defect_repairs"]:
                defect = store.get_defect(declared["defect_id"])
                if defect["state"] == "open":
                    defect = store.record_repair(declared["defect_id"], candidate["candidate_id"],
                        evidence=[diff["author_record"], state["diff"]], disposition=declared["disposition"])
                if (defect["state"] not in {"repaired", "closed"} or defect.get("repair_candidate_id") != candidate["candidate_id"]
                        or defect.get("author_attempt") != attempt["attempt_id"] or defect.get("disposition") != declared["disposition"]):
                    raise TaskflowError("assigned defect repair already identifies another author, revision or disposition")
                repaired.append({"defect_id": defect["defect_id"], "repair_seq": defect["repair_seq"],
                                 "disposition": defect["disposition"]})
            state["defect_repairs"] = repaired
        current["sid"] = collection["sid"]
        state["impact_analysis"] = report["impact_analysis"]
        state["author_record"] = diff["author_record"]
        state["responsibility_analysis"] = report["responsibility_analysis"]
        state["success_method_coverage"] = report["success_method_coverage"]
        state["history_inventory"] = spec["diagnosis"]["history_inventory"]
        state["source_usage"] = usage
    except (TaskflowError, RunnerError, OSError, KeyError, TypeError, ValueError) as error:
        errors.append(delivery_error("revision.invalid_source_or_delivery", str(error), correctable=False))
        # Return exact observed differences for the role/coordinator, never a
        # rewritten report or an automatic correction invocation.
        if "diff" in locals():
            state["actual_difference"] = diff
    state.update(status="completed", execution="completed", delivery="invalid" if errors else "valid", errors=errors)
    return save_task(store, record, state, "revision_delivery_received")


def collect_revision_task(store, identity: str) -> dict:
    """Collect the one original attempt without starting follow-up work."""
    record = _get(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity))
        return _with_metrics(record)


def _with_metrics(record: dict) -> dict:
    """Expose derived CLI usage without modifying original state or claiming cost."""
    if record["state"]["execution"] not in {"completed", "failed", "unknown"}:
        return record
    try:
        from taskflow.stress_metrics import runtime_metrics

        report = runtime_metrics(Path(record["state"]["invocation"]["runtime"]))
        report["management_invocations"] = int(bool(record["state"]["invocation"].get("attempt_id")))
        path = Path(record["definition"]["directory"]) / ("revision-metrics-" + sha256(canonical(report))[:24] + ".json")
        publish_once(path, report)
        return {**record, "metrics": {"status": "available", **evidence_ref(path, "derived original CLI metrics; not a verified bill")}}
    except (TaskflowError, RunnerError, OSError, KeyError, TypeError, ValueError) as error:
        return {**record, "metrics": {"status": "unavailable", "error": str(error)}}


def run_revision_task(store, identity: str, allow_model: bool = False) -> dict:
    """Execute the explicitly prepared revision once; failed/unknown stays original."""
    record = prepare_revision_task(store, identity)
    with task_lock(Path(record["definition"]["directory"]), store=store):
        record = _collect(store, _get(store, identity))
        if record["state"]["execution"] == "not_started" and record["state"]["outcome"] != "no_change" and allow_model:
            _validate_defect_sources(store, record["definition"])
            run_worker(store, "explicit-revision-task", True, [record["state"]["invocation"]["work_id"]])
            record = _collect(store, _get(store, identity))
        return _with_metrics(record)


def status_revision_task(store, identity: str) -> dict:
    """Read the registered revision and original state without mutation."""
    return _get(store, identity)


def validate_revision_source(store, identity: str) -> dict:
    """Authenticate a completed revision for an independently submitted quality task."""
    record = _get(store, identity)
    spec, state = record["definition"], record["state"]
    if state.get("execution") != "completed" or state.get("delivery") != "valid" or state.get("outcome") != "revised":
        raise TaskflowError("quality requires an original valid completed material revision")
    parent, files = _parent(store, spec)
    current = state["invocation"]
    _assert_copy(parent, files, Path(current["parent_root"]))
    attempt, manifest, collection = completed_original(store, current, parent, "repair")
    archive = Path(current["root"]) / "archive"
    verified_full_archive(Path(current["runtime"]), archive)
    candidate = store.get_candidate(state["candidate_id"])
    root = Path(state["revised_candidate_root"])
    candidate_files(candidate, root)
    validate_evidence([state["diff"], state["author_record"]])
    expected = revision_difference(parent, candidate, Path(current["parent_root"]), root)
    expected.update(revision_task_id=record["flow_id"], author_record=state["author_record"])
    diff = _read(Path(state["diff"]["path"]))
    if canonical(expected) != canonical(diff) or candidate.get("author_attempt") != attempt["attempt_id"] or candidate.get("parent_id") != parent["candidate_id"]:
        raise TaskflowError("revision actual differences, ancestry or original author no longer match")
    report = _validate_author(Path(state["author_record"]["path"]), spec=spec, parent=parent, candidate=candidate, diff=diff)
    if observed_source_use(report, spec, Path(current["runtime"])) != state.get("source_usage"):
        raise TaskflowError("revision source-use evidence differs from its original intake")
    for declared in spec["defect_repairs"]:
        defect = store.get_defect(declared["defect_id"])
        if (defect["state"] not in {"repaired", "closed"} or defect.get("repair_candidate_id") != candidate["candidate_id"]
                or defect.get("author_attempt") != attempt["attempt_id"] or defect.get("disposition") != declared["disposition"]):
            raise TaskflowError("revision assigned defect lacks its actual author-bound repair record")
        validate_evidence(defect["repair_evidence"])
    return {"record": record, "parent": parent, "candidate": candidate, "root": root,
            "diff": diff, "attempt": attempt, "manifest": manifest, "collection": collection,
            "archive": archive, "repair_record": report}
