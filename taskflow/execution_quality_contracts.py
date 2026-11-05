"""Source and delivery checks for one explicitly submitted quality task.

This module authenticates declared sources and signatures. It neither selects
the next investment nor converts observations into candidate defects.
"""

import json
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, relative_path, sha256
from taskflow.execution_flow_contracts import (
    NATIVE_SIGNOFF_PROTOCOL_SHA256, parse_review, request_has_native_signoff_protocol, review_metadata_aliases,
)
from taskflow.execution_quality_progress_contract import (
    QualityContractError, check_issue_targets, nonblocking_observation, validate_final,
)
from taskflow.execution_runner import (
    _full_layout, _full_verify, _info, _quality_locked_request, _read, _regular, _role_session_source, _validate, _validated_collection, archive_attempt,
)
from taskflow.packages import check_task


BINDING_KEYS = ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")


def candidate_binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in BINDING_KEYS}


def candidate_files(candidate: dict, directory: Path) -> dict:
    """Verify the exact current declaration and every captured public/private file."""
    actual = check_task(directory, write_report=False)
    if actual["errors"] or any(actual[key] != candidate[key] for key in BINDING_KEYS[1:]):
        raise TaskflowError("quality candidate declaration differs from captured bytes")
    files = {}
    for entry in candidate["files"]:
        name = relative_path(entry["package_path"])
        observed = _info(Path(entry["path"]))
        if (name in files or observed["sha256"] != entry["sha256"]
                or ("size" in entry and observed["size"] != entry["size"])):
            raise TaskflowError("quality candidate file differs from captured bytes: " + name)
        files[name] = {"path": name, "source": entry["path"], "role": entry["role"], **observed}
    return files


def public_predecessor(store: object, spec: dict, candidate: dict) -> tuple[dict, dict]:
    """Authenticate one named completed public attempt and its exact full archive."""
    attempt = store.get_attempt(spec["predecessor_attempt"])
    work = store.get_work(attempt["work_id"])
    source = candidate
    if attempt["candidate_id"] != candidate["candidate_id"]:
        from taskflow.execution_public_predecessor import validate_public_reuse_reference

        if not spec.get("public_predecessor_reuse"):
            raise TaskflowError("quality public predecessor requires an explicit same-public applicability record")
        source = validate_public_reuse_reference(store, spec["public_predecessor_reuse"],
            owner=spec["owner"], task_id=spec["task_id"], attempt=attempt, target=candidate)
    root = Path(attempt["runtime_path"])
    manifest = _validate(root)
    if manifest.get("mode") in ("native_role", "test_fixture"):
        expected = []
        for row in source["files"]:
            if row["role"] not in PUBLIC_ROLES:
                continue
            observed = _info(Path(row["path"]))
            if observed["sha256"] != row["sha256"] or ("size" in row and row["size"] != observed["size"]):
                raise TaskflowError("public reconstruction source differs from its captured original bytes")
            expected.append((row["package_path"], "public", row["sha256"], observed["size"]))
        expected.sort()
        actual = sorted((row["target"], row["kind"], row["sha256"], row["size"])
                        for row in manifest["inputs"])
        if not expected or actual != expected:
            raise TaskflowError("public reconstruction must receive the complete exact public view with no metadata/history extras")
    collection = _validated_collection(root, manifest)
    if (attempt["state"] != "completed" or attempt["role"] != "quality"
            or attempt["candidate_id"] != source["candidate_id"]
            or attempt["task_id"] != candidate["task_id"]
            or manifest["candidate_binding"] != candidate_binding(source)
            or manifest["intent_id"] != attempt["intent_id"]
            or collection.get("status") != "completed"
            or collection.get("technical_accepted") is not True
            or not collection.get("sid") or collection["sid"] != attempt.get("cli_sid")
            or any(work[key] != attempt[key] for key in (
                "task_id", "candidate_id", "intent_id", "runtime_path", "policy_id", "request_digest"))):
        raise TaskflowError("quality public predecessor is not its exact completed original")
    if (manifest.get("mode") not in ("test_fixture", "existing_isolated")
            and manifest.get("model_slot") != "public_rebuild"):
        raise TaskflowError("quality predecessor must be an explicit public-rebuild role")
    if any(entry["kind"] in ("teacher", "history", "target") for entry in manifest["inputs"]):
        raise TaskflowError("quality public predecessor received private or historical inputs")
    archive = Path(spec["predecessor_archive"])
    if not archive.is_dir() or not (archive / "supplemental-intent.json").is_file():
        raise TaskflowError("quality predecessor requires its explicit completed full archive")
    # The normal archive verifier authenticates every member/directory against
    # the original runtime and durable binding; do not trust a self-consistent
    # replacement supplemental index inside the archive.
    archive_attempt(root, archive)
    intent = _read(root / "supplemental-intent.json")
    _full_verify(archive / "supplemental", intent, True)
    return attempt, manifest


def observation_inputs(archive: Path) -> list[dict]:
    """Expose only the explicit predecessor's complete work/scratch and trace."""
    inputs = []
    intent = _read(archive / "supplemental-intent.json")
    payloads, _ = _full_layout(intent)
    for area in ("work", "scratch"):
        alias = intent.get("aliases", {}).get(area)
        if area == "scratch" and alias is None:
            continue
        if not isinstance(alias, dict) or alias.get("source") not in intent["sources"]:
            raise TaskflowError("quality predecessor lacks a declared " + area + " archive alias")
        relative = alias["relative"]
        if relative != ".":
            relative = relative_path(relative)
        if any(name.startswith("indexed-files/") and row["source"] == alias["source"]
               and (relative == "." or Path(row["path"]).is_relative_to(Path(relative)))
               for name, row in payloads.items()):
            raise TaskflowError("quality predecessor has unsupported indexed " + area + " members; complete observations cannot be transported")
        source = archive / "supplemental/trees" / alias["source"] / relative
        if not source.is_dir() or source.is_symlink():
            raise TaskflowError("quality predecessor lacks full " + area + " archive")
        for path in sorted(source.rglob("*")):
            if path.is_dir() and not path.is_symlink():
                continue
            _regular(path)
            inputs.append({"path": str(path), "kind": "submission",
                           "target": "actual-public-" + area + "/" + str(path.relative_to(source))})
    trace = archive / "trace.jsonl"
    _regular(trace)
    inputs.append({"path": str(trace), "kind": "trace", "target": "actual-public-trace.jsonl"})
    return inputs


def observation_aliases(manifest: dict, root: Path, archive: Path) -> dict:
    aliases = {}
    for item in manifest["inputs"]:
        if item["kind"] not in ("submission", "trace"):
            continue
        path = Path(item["source"])
        if not path.is_relative_to(archive):
            raise TaskflowError("quality observation is outside its explicit predecessor archive")
        data = _regular(path)
        target = relative_path(item["target"])
        if (target in aliases or data != _regular(root / "inputs" / target)
                or _info(path) != {key: item[key] for key in ("sha256", "size")}):
            raise TaskflowError("quality observation differs from its locked original")
        aliases[target] = {"path": str(path), "sha256": sha256(data), "kind": item["kind"]}
    return aliases


def delivery_error(code: str, message: str, *, field: str = "delivery",
                   issue_id: str | None = None, correctable: bool = True) -> dict:
    return {"code": code, "field": field, "issue_id": issue_id,
            "message": message, "correctable": correctable}


def _native_sources(report: dict, issues: dict, candidate: dict, manifest: dict,
                    root: Path, metadata: dict, observed: dict, scope: str = "whole_package") -> None:
    """Verify the existing native signature shape and its actual source citations."""
    if not _original_native_protocol(root, manifest):
        raise TaskflowError("quality native signoff protocol is absent from its locked request")
    validate_final(report, issues, {
        "intent_id": manifest["intent_id"], "candidate_binding": candidate_binding(candidate),
        "require_native": True, "native_protocol_sha256": NATIVE_SIGNOFF_PROTOCOL_SHA256, "scope": scope})
    files = {entry["package_path"]: entry for entry in candidate["files"]}
    aliases = {**metadata, **observed}

    def citations(rows: list[dict], *, public_only: bool = False) -> None:
        for row in rows:
            name = relative_path(row["path"])
            source = files.get(name) or aliases.get(name)
            if source is None or (public_only and (name not in files or source["role"] not in PUBLIC_ROLES)):
                raise TaskflowError("native signoff citation is outside its current authorized source: " + name)
            info = _info(Path(source["path"]))
            entries = [entry for entry in manifest["inputs"] if entry["target"] == name]
            if (row["sha256"] != source["sha256"] or info["sha256"] != source["sha256"] or len(entries) != 1
                    or info != {key: entries[0][key] for key in ("sha256", "size")}):
                raise TaskflowError("native signoff citation SHA differs from its actual locked source: " + name)

    routing, signed = report["execution_routing"], report["native_signoff"]
    outcome, affected = routing["outcome"], signed["affected_paths"]
    if any(relative_path(name) not in files for name in affected):
        raise TaskflowError("native signoff affected paths are outside the current candidate")
    if scope == "whole_package" and outcome == "accepted" and (signed["coverage"] != "whole_package" or affected):
        raise TaskflowError("accepted whole quality cannot upgrade an excluded/unchanged scope")
    if signed["coverage"] == "unchanged_scope" and not affected:
        raise TaskflowError("unchanged_scope must retain its actual excluded candidate paths")
    for dimension in signed["dimensions"].values():
        citations(dimension["evidence"])
    observed_rows = issues["issues"]
    if outcome == "accepted" and any(not dimension["affirmed"] for dimension in signed["dimensions"].values()):
        if (not observed_rows or any(not nonblocking_observation(row, []) or row.get("blocking") is not False
                or row.get("needs_business_decision", False) is not False for row in observed_rows)):
            raise TaskflowError("false native affirmation needs explicit accepted nonblocking observations")
        links = {(row["path"], row["sha256"]) for issue in observed_rows for row in issue["evidence"]}
        for dimension in signed["dimensions"].values():
            if not dimension["affirmed"] and (not dimension["limitations"] or not links.intersection(
                    (row["path"], row["sha256"]) for row in dimension["evidence"])):
                raise TaskflowError("false native affirmation lacks its actual nonblocking observation and limitation")
    linked_blocks = set()
    for row in signed["responsibility_analysis"]:
        citations(row["public_evidence"], public_only=True)
        ids = set(row["blocking_issue_ids"])
        if not ids <= set(routing["blocking_issue_ids"]):
            raise TaskflowError("native responsibility refers to an unbound blocking issue")
        if (row["disposition"] == "blocking_defect") != bool(ids):
            raise TaskflowError("native responsibility disposition contradicts its blocking issue association")
        if outcome == "accepted" and row["disposition"] != "retained":
            raise TaskflowError("accepted native responsibility cannot retain unresolved or blocking disposition")
        linked_blocks.update(ids)
    if outcome == "revision_required" and linked_blocks != set(routing["blocking_issue_ids"]):
        raise TaskflowError("native responsibility did not associate all actual blocking findings")
    normative = signed["normative"]
    citations(normative["evidence"])
    selected = []
    for row in normative["files"]:
        name = relative_path(row["package_path"])
        source = files.get(name)
        if source is None or source["role"] in PUBLIC_ROLES or source["role"] != row["role"]:
            raise TaskflowError("native normative selection is not an actual current private source")
        info = _info(Path(source["path"]))
        if info != {key: row[key] for key in ("sha256", "size")} or source["sha256"] != row["sha256"]:
            raise TaskflowError("native normative selection differs from the current candidate bytes")
        selected.append(name)
    if len(selected) != len(set(selected)):
        raise TaskflowError("native normative selection duplicates a source")
    if normative["status"] == "determined" and not {"answer", "rubric"} <= {files[name]["role"] for name in selected}:
        raise TaskflowError("determined native normative selection requires actual answer and rubric sources")
    if outcome == "accepted" and normative["status"] != "determined":
        raise TaskflowError("accepted native quality has an unresolved normative choice")


def _original_native_protocol(root: Path, manifest: dict) -> bool:
    """Authenticate the protocol already delivered in this exact persistent QA.

    Older requests remain original evidence; this reads their exact bound chain
    for validation and never injects their text into the next model request.
    """
    seen = set()
    while root not in seen:
        seen.add(root)
        if request_has_native_signoff_protocol(_quality_locked_request(root, manifest)):
            return True
        session = manifest.get("role_session") or {}
        if session.get("mode") != "resume_existing":
            return False
        source = _role_session_source(session["previous"])
        original = source["manifest"]
        if (original.get("role") != "quality" or manifest.get("role") != "quality"
                or original.get("role_lineage") != manifest.get("role_lineage")
                or original.get("role_session", {}).get("home") != session["home"]
                or source["collection"]["sid"] != _validated_collection(root, manifest)["sid"]):
            raise TaskflowError("quality protocol source differs from its exact original QA context")
        root, manifest = Path(source["root"]), original
    raise TaskflowError("quality protocol source chain contains a cycle")


def new_caller_sections(source: dict, declared: list[dict], rendered: list[dict]) -> list[dict]:
    """Omit only exact caller sections already supplied to this actual QA.

    The immutable definition retains the complete declaration. A matching old
    declaration alone is insufficient: the rendered text must also occur in an
    authenticated request in the precise same-SID chain.
    """
    prior = source["quality"]["definition"]["runner"].get("request_sections", [])
    if not any(section in prior for section in declared):
        return rendered
    root, manifest = Path(source["attempt"]["runtime_path"]), source["manifest"]
    sid, home = source["collection"]["sid"], manifest["role_session"]["home"]
    lineage, requests, seen = manifest.get("role_lineage"), [], set()
    while root not in seen:
        seen.add(root)
        if (manifest.get("role") != "quality" or manifest.get("role_lineage") != lineage
                or manifest.get("role_session", {}).get("home") != home):
            raise TaskflowError("caller instruction source differs from its exact original QA context")
        requests.append(_quality_locked_request(root, manifest))
        session = manifest["role_session"]
        if session["mode"] != "resume_existing":
            break
        previous = _role_session_source(session["previous"])
        if previous["collection"]["sid"] != sid:
            raise TaskflowError("caller instruction source changed its original QA SID")
        root, manifest = Path(previous["root"]), previous["manifest"]
    else:
        raise TaskflowError("caller instruction source chain contains a cycle")
    return [current for original, current in zip(declared, rendered)
            if original not in prior or not any(current["text"].encode("utf-8") in request for request in requests)]


def _public_first_proof(root: Path, report: dict) -> None:
    """Authenticate explicit successful trace anchors, not hidden-context isolation."""
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    checkpoint = root / "artifacts/public-responsibility-first.json"
    info = _info(checkpoint)
    claim = report.get("public_first")
    if (collected["artifacts"].get(checkpoint.name) != info or not isinstance(claim, dict)
            or claim.get("checkpoint_sha256") != info["sha256"]):
        raise TaskflowError("public-first needs its sealed checkpoint and explicit original trace anchors")
    events = [json.loads(line) for line in _regular(root / "trace.jsonl").decode().splitlines() if line.strip()]
    completed = [(index, event.get("item", {})) for index, event in enumerate(events)
                 if event.get("type") == "item.completed" and event.get("item", {}).get("type") == "command_execution"]
    stable = _read(root / "inputs/execution/current.json")
    nonce = stable.get("public_first", {}).get("trace_nonce")
    if not isinstance(nonce, str) or not nonce or claim.get("trace_nonce") != nonce:
        raise TaskflowError("public-first trace correlation differs from the locked task")
    nonce_marker = "QUALITY_TRACE_NONCE=" + nonce

    def unique_item(markers: tuple[str, ...], *, correlate: bool = True) -> tuple[int, dict]:
        required = (*markers, nonce_marker) if correlate else markers
        matches = [(index, item) for index, item in completed
                   if all(marker in item.get("aggregated_output", "") for marker in required)]
        if len(matches) != 1:
            raise TaskflowError("public-first needs one unambiguous actual tool output anchor")
        index, item = matches[0]
        if item.get("exit_code") != 0 or item.get("status") != "completed":
            raise TaskflowError("public-first tool anchor did not complete successfully")
        return index, item

    marker = "PUBLIC_RESPONSIBILITY_FIRST_SHA256=" + info["sha256"]
    before, _ = unique_item((marker,), correlate=False)
    private = [entry for entry in manifest["inputs"] if entry["kind"] == "teacher"]
    source = next((entry for entry in private if entry["target"] == claim.get("teacher_path")), None)
    if source is None:
        raise TaskflowError("public-first informed read does not identify a locked current teacher source")
    after, teacher = unique_item(("TEACHER_SOURCE_PATH=" + source["target"], "SHA256=" + source["sha256"]))
    if before >= after:
        raise TaskflowError("public-first checkpoint readback must precede the cited informed read")

    def shows_source(item: dict, entry: dict) -> bool:
        output = item.get("aggregated_output", "")
        return entry["target"] in output and entry["sha256"] in output

    if not shows_source(teacher, source):
        raise TaskflowError("public-first informed item lacks its actual source path/SHA readback")
    if any(index <= before and any(shows_source(item, entry) for entry in private) for index, item in completed):
        raise TaskflowError("current teacher source was visibly read before the public-first checkpoint")


def _informed_recheck_proof(root: Path, report: dict, coverage: dict) -> None:
    """Authenticate current rereading without restamping a teacher-informed QA as blind."""
    _public_first_proof(root, report)
    claim = report["public_first"]
    checkpoint = _read(root / "artifacts/public-responsibility-first.json")
    original = coverage["parent_source"]["quality"]["state"]["invocations"][0]
    path = Path(original["runtime"]) / "artifacts/public-responsibility-first.json"
    expected = _info(path)["sha256"]
    if (claim.get("mode") != "informed_recheck" or claim.get("parent_checkpoint_sha256") != expected
            or checkpoint.get("review_mode") != "informed_recheck"
            or checkpoint.get("parent_checkpoint_sha256") != expected
            or _regular(root / "inputs/execution/parent-public-checkpoint.json") != _regular(path)):
        raise TaskflowError("resumed QA must retain its original checkpoint and disclose informed current rereading")


def inspect_delivery(*, candidate: dict, attempt: dict, root: Path, archive: Path,
                     report_file: str, issues_file: str, original_public_root: Path,
                     scope: str = "whole_package", coverage: dict | None = None, defect_ids: list | None = None) -> dict:
    """Return structural diagnostics without rewriting or weakening the original."""
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    if (manifest["candidate_binding"] != candidate_binding(candidate)
            or manifest["intent_id"] != attempt["intent_id"]
            or collected.get("sid") != attempt.get("cli_sid")):
        raise TaskflowError("quality delivery lost its original candidate/intent/SID binding")
    report, issues = root / "artifacts" / report_file, root / "artifacts" / issues_file
    errors = []
    outcome = None
    try:
        report_value, issue_value = _read(report), _read(issues)
        routing = report_value.get("execution_routing", {})
        outcome = routing.get("outcome") if isinstance(routing, dict) else None
        observed = observation_aliases(manifest, root, archive)
        files = {entry["package_path"] for entry in candidate["files"]}
        check_issue_targets(report_value, issue_value, {
            "candidate_paths": sorted(files), "observed_paths": sorted(observed), "source_errors": []})
        metadata = review_metadata_aliases(candidate, manifest, root)
        parsed = parse_review(report, issues, candidate=candidate, intent_id=attempt["intent_id"],
                              scope=scope,
                              metadata_aliases=metadata,
                              observed_aliases=observed)
        if defect_ids is not None and sorted(parsed["covered_defect_ids"]) != sorted(defect_ids):
            raise TaskflowError("quality original omits or invents explicitly assigned defect coverage")
        _native_sources(report_value, issue_value, candidate, manifest, root, metadata, observed, scope)
        if scope == "affected":
            from taskflow.execution_quality_coverage import validate_coverage_signature

            validate_coverage_signature(report_value, coverage, candidate, manifest, root)
    except QualityContractError as error:
        diagnostic = error.as_dict() if hasattr(error, "as_dict") else delivery_error("invalid_quality_contract", str(error))
        errors.append({**diagnostic, "correctable": True})
        parsed = None
    except (TaskflowError, OSError, ValueError, KeyError, TypeError) as error:
        if not errors:
            errors.append(delivery_error("invalid_quality_contract", str(error)))
        parsed = None
    if parsed is not None and outcome != "inconclusive":
        # Corrections cannot manufacture a blind reading after the original saw
        # teacher material. Always authenticate the first invocation's checkpoint.
        try:
            if coverage is not None and coverage["plan"]["public_review_mode"] == "informed_recheck":
                _informed_recheck_proof(original_public_root, report_value, coverage)
            else:
                _public_first_proof(original_public_root, report_value)
        except (TaskflowError, OSError, ValueError, KeyError) as error:
            errors.append(delivery_error("quality.public_first", str(error), correctable=False))
    return {"outcome": outcome, "parsed": parsed, "errors": errors}
