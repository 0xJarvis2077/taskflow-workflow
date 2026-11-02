"""Read a bound affected supplement without rewriting any original signature."""

import copy
import json
from pathlib import Path
import re

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, parse_json, relative_path, validate_id
from taskflow.execution_flow_contracts import _files
from taskflow.execution_runner import RunnerError, _info, _regular, _trace, _validate as _validate_attempt, _validated_collection


def _need(condition: bool, message: str) -> None:
    if not condition:
        raise TaskflowError("recheck supplement: " + message)


def _text(value: object, label: str) -> str:
    _need(isinstance(value, str) and bool(value.strip()), label + " must be nonempty text")
    return value


def _path(value: str) -> Path:
    path = Path(_text(value, "source path"))
    _need(path.is_absolute() and ".." not in path.parts
          and not any(item.is_symlink() for item in (path, *path.parents)), "unsafe source path")
    return path


def _ref(path: Path) -> dict:
    return {"path": str(path), **_info(path)}


def _reference(value: dict, expected: Path | None = None) -> tuple[dict, bytes]:
    _need(isinstance(value, dict), "a hash-bound source reference is required")
    path = _path(value.get("path"))
    _need(expected is None or path == expected, "source reference does not name the actual collected original")
    observed = _info(path)
    _need(observed["sha256"] == value.get("sha256")
          and ("size" not in value or observed["size"] == value["size"]), "source reference SHA/size drifted")
    return {"path": str(path), **observed}, _regular(path, limit=128 * 1024 * 1024)


def _binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}


def _ids(value: object, label: str) -> list[str]:
    _need(isinstance(value, list), label + " must be a list")
    _need(all(isinstance(item, str) for item in value) and len(value) == len(set(value)), label + " must be unique strings")
    return value


def _pointer(document: object, pointer: str) -> object:
    _need(isinstance(pointer, str) and pointer.startswith("/") and re.search(r"~(?:[^01]|$)", pointer) is None,
          "invalid signed JSON pointer")
    value = document
    try:
        for part in pointer[1:].split("/"):
            part = part.replace("~1", "/").replace("~0", "~")
            if isinstance(value, list):
                _need(re.fullmatch(r"0|[1-9][0-9]*", part) is not None, "invalid signed array index")
                value = value[int(part)]
            else:
                _need(isinstance(value, dict), "signed JSON pointer traverses a scalar")
                value = value[part]
    except (KeyError, IndexError) as error:
        raise TaskflowError("recheck supplement: signed JSON pointer does not exist") from error
    return value


def _managed(attempt: dict, candidate: dict) -> tuple[Path, dict, dict]:
    root = _path(attempt["runtime_path"])
    manifest = _validate_attempt(root)
    collected = _validated_collection(root, manifest)
    trace = _trace(root / "trace.jsonl", attempt.get("cli_sid"))
    _need(attempt.get("state") == "completed" and attempt.get("role") == "quality"
          and attempt.get("candidate_id") == candidate["candidate_id"]
          and type(attempt.get("process_exit")) is int and attempt["process_exit"] == 0
          and all(attempt.get(key) is True for key in ("response_complete", "artifacts_complete", "technical_accepted"))
          and manifest.get("role") == "quality" and manifest.get("mode") == "native_role"
          and manifest.get("nature") == "real_cli_attempt" and manifest.get("candidate_binding") == _binding(candidate)
          and manifest.get("intent_id") == attempt.get("intent_id")
          and manifest["request"]["sha256"] == attempt.get("request_digest")
          and manifest.get("model", {}).get("requested") == "gpt-6.1-sol"
          and manifest.get("model", {}).get("reasoning_effort") == "xhigh"
          and all(collected.get(key) is True for key in ("technical_accepted", "process_ended", "delivery_complete", "model_completed"))
          and trace["model_completed"] and collected.get("sid") == trace["sid"] == attempt.get("cli_sid"),
          "source is not its actual completed independent native quality attempt")
    return root, manifest, collected


def _validate(*, supplement: dict, attempt: dict, prior: dict, parent: dict, candidate: dict,
              closed_defect_ids: list[str], changed_paths: list[str], original_paths: set[str],
              parent_aliases: dict[str, str] | None) -> dict:
    _need(isinstance(supplement, dict) and set(supplement) == {
        "schema_version", "attempt_id", "report", "issues", "original_recheck_report", "base_recheck_id"}
        and type(supplement["schema_version"]) is int and supplement["schema_version"] == 1,
        "the exact typed supplement definition is required")
    validate_id(supplement["base_recheck_id"], "base recheck ID")
    closed = _ids(closed_defect_ids, "closed defect IDs")
    _need(bool(closed), "closed defect IDs are required")
    for identity in closed:
        validate_id(identity, "closed defect ID")
    changed = _ids(changed_paths, "changed paths")
    before, after = _files(parent), _files(candidate)
    _need(parent["task_id"] == candidate["task_id"] and candidate.get("parent_id") == parent["candidate_id"]
          and candidate["input_digest"] == parent["input_digest"] and set(before) == set(after)
          and all(before[key]["role"] == after[key]["role"] for key in before), "candidate lineage/public/declared role set differs")
    actual = {key for key in before if before[key]["sha256"] != after[key]["sha256"]}
    _need(actual and set(changed) == actual and all(after[key]["role"] not in PUBLIC_ROLES for key in changed),
          "changed paths must cover every and only actual private byte change")
    _need(isinstance(original_paths, set) and original_paths <= set(before), "original affected paths are not exact candidate paths")
    extras = actual - original_paths
    aliases = {} if parent_aliases is None else parent_aliases
    _need(isinstance(aliases, dict), "parent aliases must be explicitly authenticated by the caller")
    root, manifest, collection = _managed(attempt, candidate)
    prior_root, prior_manifest, prior_collection = _managed(prior["attempt"], candidate)
    _need(supplement["attempt_id"] == attempt.get("attempt_id")
          and attempt["attempt_id"] != prior["attempt"]["attempt_id"]
          and attempt["attempt_id"] not in {candidate.get("author_attempt"), parent.get("author_attempt")}
          and attempt["cli_sid"] != prior["attempt"]["cli_sid"]
          and attempt["cli_sid"] not in set(candidate.get("external_author_sids", [])) | set(parent.get("external_author_sids", []))
          and type(attempt.get("claimed_seq")) is int and type(prior["attempt"].get("last_seq")) is int
          and attempt["claimed_seq"] > prior["attempt"]["last_seq"], "supplement is not a new independent post-recheck identity")
    _need(_binding(prior["candidate"]) == _binding(candidate), "prior signed candidate differs")
    report_ref, raw = _reference(supplement["report"], root / "artifacts/adoption-readiness.json")
    issues_ref, issues_raw = _reference(supplement["issues"], root / "artifacts/issues.json")
    report = parse_json(raw, "original affected supplement")
    _need(type(report.get("schema_version")) is int and report["schema_version"] == 1,
          "original supplement has an unsupported report schema")
    _need(json.loads(issues_raw) == [], "original supplemental issues must be the actually collected empty array")
    for filename, reference in (("adoption-readiness.json", report_ref), ("issues.json", issues_ref)):
        _need(collection["artifacts"].get(filename) == {key: reference[key] for key in ("sha256", "size")}, "output is not sealed by actual collection")
    prior_ref, prior_raw = _reference(prior["report_ref"], prior_root / "artifacts/adoption-readiness.json")
    _need(prior_collection["artifacts"].get("adoption-readiness.json") == {key: prior_ref[key] for key in ("sha256", "size")}
          and canonical(parse_json(prior_raw)) == canonical(prior["report"]), "prior report is not its original collected signature")
    original_ref, original_raw = _reference(supplement["original_recheck_report"])
    _need(original_raw == prior_raw, "the supplied original recheck report differs from its actual collection")
    routing = report.get("execution_routing", {})
    _need(type(routing.get("schema_version")) is int and routing["schema_version"] == 1
          and routing.get("intent_id") == attempt["intent_id"] and routing.get("candidate_id") == candidate["candidate_id"]
          and routing.get("scope") == "affected" and routing.get("outcome") == "accepted"
          and routing.get("signer_role") == "independent_quality" and routing.get("independent_of_current_author") is True
          and routing.get("participated_in_production") is False and routing.get("blocking_issue_ids") == []
          and set(_ids(routing.get("covered_defect_ids"), "signed closed defect IDs")) == set(closed)
          and all(key not in routing or routing[key] == value for key, value in _binding(candidate).items()),
          "original supplement does not sign this exact affected closure")
    _need(report.get("parent_candidate_id") == parent["candidate_id"] and report.get("candidate_id") == candidate["candidate_id"]
          and report.get("task_id") == candidate["task_id"], "original supplemental parent/current identity differs")
    native = report.get("native_signoff", {})
    _need(type(native.get("schema_version")) is int and native["schema_version"] == 1
          and native.get("coverage") == "affected" and set(_ids(native.get("affected_paths"), "native affected paths")) == actual
          and set(native.get("dimensions", {})) == {"business", "artifact", "calculation"}
          and all(native["dimensions"][kind].get("affirmed") is True for kind in native["dimensions"])
          and "normative" not in native, "supplement cannot replace whole/native scope or normative selection")
    detail, coverage, reuse = report["defect_resolution"], report["coverage"], report["prior_judgment_reuse"]
    detail_ids = detail.get("defect_ids", [detail.get("defect_id")])
    _need(set(_ids(detail_ids, "resolution defect IDs")) == set(closed) and detail.get("status") == "resolved_in_affected_scope"
          and detail.get("remaining_blocking_issue_ids") == [], "supplemental defect resolution is not exact")
    _need(coverage.get("scope") == "affected"
          and set(_ids(coverage.get("original_defect_paths"), "original defect paths")) == original_paths
          and set(_ids(coverage.get("associated_extra_paths"), "associated extra paths")) == extras
          and set(_ids(detail.get("affected_paths"), "resolution affected paths")) == extras,
          "signed original/changed/necessary affected relations are incomplete")
    prior_report = prior["report"]
    _need(reuse.get("sha256") == prior_ref["sha256"] and reuse.get("candidate_id") == candidate["candidate_id"]
          and reuse.get("candidate_id_json_pointer") == "/execution_routing/candidate_id"
          and reuse.get("scope_json_pointer") == "/native_signoff/coverage"
          and _pointer(prior_report, reuse["candidate_id_json_pointer"]) == candidate["candidate_id"]
          and _pointer(prior_report, reuse["scope_json_pointer"]) == "affected"
          and reuse.get("supplied_copy_byte_identical") is True
          and reuse.get("historic_whole_package_judgment_reissued") is False
          and reuse.get("normative_allowlist_reselection_performed") is False, "prior judgment reuse has a foreign version/scope")
    prior_inputs = [row for row in manifest["inputs"] if row["source"] == reuse.get("path") and row["kind"] == "source_metadata"]
    _need(len(prior_inputs) == 1, "prior signed source is absent or ambiguous in actual inputs")
    staged_prior = root / "inputs" / prior_inputs[0]["target"]
    _need(reuse.get("supplied_copy_path") == str(staged_prior)
          and original_ref["path"] in {prior_ref["path"], reuse["path"]}
          and _regular(_path(reuse["path"]), limit=128 * 1024 * 1024) == prior_raw
          and _regular(staged_prior, limit=128 * 1024 * 1024) == prior_raw
          and all(prior_inputs[0][key] == prior_ref[key] for key in ("sha256", "size")), "prior source/staged/collected originals are not byte-identical")
    disposition_pointer = reuse["defect_disposition_json_pointer"]
    _need(re.fullmatch(r"/defect_recheck/(0|[1-9][0-9]*)/disposition", disposition_pointer) is not None
          and _pointer(prior_report, disposition_pointer) == "resolved_in_affected_scope"
          and _pointer(prior_report, disposition_pointer.rsplit("/", 1)[0])["defect_id"] in closed,
          "prior closure pointer is outside the actual signed defect")
    for kind in ("business", "artifact", "calculation"):
        pointer = "/native_signoff/dimensions/" + kind
        dimension_source = native["dimensions"][kind].get("reused_judgment", {})
        _need(reuse["dimension_judgment_json_pointers"].get(kind) == pointer
              and _pointer(prior_report, pointer).get("affirmed") is True
              and dimension_source.get("path") == reuse["path"] and dimension_source.get("sha256") == prior_ref["sha256"]
              and dimension_source.get("json_pointer") == pointer
              and dimension_source.get("candidate_id_pointer") == reuse["candidate_id_json_pointer"]
              and dimension_source.get("coverage_pointer") == reuse["scope_json_pointer"]
              and dimension_source.get("same_candidate_and_current_sha_verified") is True
              and dimension_source.get("fresh_suite_execution") is False,
              "prior dimension pointer is not its original affirmation")

    def current(row: dict) -> str:
        name = relative_path(row.get("package_path"))
        _need(name in after and row.get("path") == after[name]["path"], "current evidence path does not equal captured package source")
        matches = [item for item in manifest["inputs"] if item["target"] == name and item["source"] == row["path"]]
        observed = _info(_path(row["path"]))
        _need(len(matches) == 1 and matches[0]["kind"] == "teacher" and observed["sha256"] == row.get("sha256") == after[name]["sha256"]
              and all(matches[0][key] == observed[key] for key in ("sha256", "size"))
              and _info(root / "inputs" / name) == observed, "current source/staged evidence SHA/size is not exact")
        _text(row.get("location"), "current source locator")
        return name

    def old_source(value: str, name: str) -> str:
        path = _path(value)
        captured = before[name]["path"]
        _need(str(path) == captured or aliases.get(str(path)) == captured, "parent source alias is not explicitly authenticated")
        _need(_info(path) == _info(Path(captured)) and _regular(path, limit=128 * 1024 * 1024)
              == _regular(Path(captured), limit=128 * 1024 * 1024), "parent source alias does not contain captured original bytes")
        return str(path)

    reused = {}
    for index, row in enumerate(reuse["reused_file_evidence"]):
        name = relative_path(row["path"])
        _need(name in actual and name not in reused, "prior reuse paths are missing or ambiguous")
        pointer = row["json_pointer"]
        _need(re.fullmatch(r"/(?:defect_recheck/(?:0|[1-9][0-9]*)/evidence|native_signoff/dimensions/(?:business|artifact|calculation)/evidence)/(?:0|[1-9][0-9]*)", pointer) is not None,
              "reused evidence pointer is outside original affected evidence")
        original = _pointer(prior_report, pointer)
        _need(original.get("path") == name and original.get("sha256") == row.get("current_sha256") == after[name]["sha256"]
              and original.get("location") == row.get("original_location"), "reused evidence differs from its original signed pointer")
        reused[name] = (index, row, pointer, original)

    def provenance(row: dict, name: str) -> None:
        _need(name in reused and isinstance(row, dict), "unsigned prior evidence cannot be reused")
        _need(row.get("path") == reuse["path"] and row.get("sha256") == prior_ref["sha256"]
              and row.get("json_pointer") == reused[name][2]
              and row.get("candidate_id_pointer") == reuse["candidate_id_json_pointer"]
              and row.get("coverage_pointer") == reuse["scope_json_pointer"]
              and row.get("same_candidate_and_current_sha_verified") is True and row.get("fresh_suite_execution") is False,
              "reused evidence provenance is not the exact original source")

    fresh = report["fresh_record_readback"]
    fresh_name = current(fresh)
    _need(fresh_name in actual and fresh_name not in reused
          and fresh.get("judgment_origin") == "fresh_independent_record_readback_and_association_judgment"
          and fresh.get("fresh_suite_execution") is False, "missing actual fresh supplemental readback")
    _text(fresh.get("judgment"), "fresh independent judgment")
    fresh_old = old_source(fresh["parent_path"], fresh_name)
    _need(fresh.get("parent_sha256") == before[fresh_name]["sha256"], "fresh readback parent SHA differs")
    _need(set(reused) | {fresh_name} == actual
          and set(_ids(coverage.get("reused_judgment_paths"), "reused judgment paths")) == set(reused)
          and _ids(coverage.get("new_record_judgment_paths"), "fresh judgment paths") == [fresh_name]
          and set(_ids(coverage.get("fresh_old_new_necessity_judgment_paths"), "fresh association paths")) == extras,
          "reused/fresh/association judgments do not cover exact actual changes")
    pairs = {}
    for index, row in enumerate(coverage["changed_files"]):
        name = relative_path(row["path"])
        _need(name in actual and name not in pairs and row.get("role") == after[name]["role"]
              and row.get("old") == before[name]["sha256"] and row.get("new") == after[name]["sha256"]
              and row.get("new_source") == after[name]["path"] and row.get("old_bytes_verified") is True
              and row.get("fresh_necessary_association_verified") is (name in extras)
              and row.get("new_bytes_verified") is True and row.get("old_size_bytes") == _info(Path(before[name]["path"]))["size"]
              and row.get("new_size_bytes") == _info(Path(after[name]["path"]))["size"], "signed old/new coverage pair differs from actual bytes")
        old_source(row["old_source"], name)
        pairs[name] = (index, row)
    _need(set(pairs) == actual and pairs[fresh_name][1]["old_source"] == fresh_old, "coverage omits actual old/new changes")
    associations = {}
    for index, row in enumerate(detail["necessary_associations"]):
        name = relative_path(row["path"])
        _need(name in extras and name not in associations and row.get("current_path") == after[name]["path"]
              and row.get("parent_path") == pairs[name][1]["old_source"]
              and row.get("old_sha256") == before[name]["sha256"] and row.get("new_sha256") == after[name]["sha256"]
              and row.get("old_new_bytes_verified") is True and row.get("disposition") == "necessary_association_verified"
              and row.get("judgment_origin") == "fresh_old_new_and_necessity_examination", "unsigned or mismatched necessary association")
        for key in ("old_location", "new_location", "delta", "necessity"):
            _text(row.get(key), "necessary association " + key)
        if name in reused:
            provenance(row.get("prior_evidence_provenance"), name)
        else:
            _need(row.get("prior_evidence_provenance") is None and name == fresh_name, "fresh association cannot impersonate a historical judgment")
        associations[name] = (index, row)
    _need(set(associations) == extras, "necessary associations must cover every and only extra changed path")
    sources = {}

    def source(pointer: str, original_pointer: str, value: object, reference: dict = report_ref) -> None:
        sources.setdefault(pointer, []).append({"report": reference, "json_pointer": original_pointer, "actual_value": copy.deepcopy(value)})

    normalized = []
    names = set()
    for index, row in enumerate(detail["evidence"]):
        name = current(row)
        _need(name in actual and name not in names, "supplemental current evidence must cover unique changed paths")
        names.add(name)
        pointer = "/defect_resolution/evidence/" + str(index)
        source(pointer, pointer, row)
        if name in reused:
            _need(row.get("judgment_origin") == "reused_same_candidate_signed_affected_judgment", "reused evidence lacks its signed attribution")
            provenance(row.get("provenance"), name)
            _need(row["location"] == reused[name][3]["location"], "reused signed locator changed")
            source(pointer, reused[name][2], reused[name][3], prior_ref)
            source(pointer, "/prior_judgment_reuse/reused_file_evidence/" + str(reused[name][0]), reused[name][1])
        else:
            _need(name == fresh_name and canonical(row) == canonical(fresh), "fresh canonical evidence is not its actual signed readback")
            source(pointer, "/fresh_record_readback", fresh)
        normalized.append({**copy.deepcopy(row), "path": name})
    _need(names == actual, "current evidence omits an actual changed file")
    for index, name in enumerate(detail["affected_paths"]):
        pointer = "/defect_resolution/affected_paths/" + str(index)
        source(pointer, pointer, name)
        if name in associations:
            source(pointer, "/defect_resolution/necessary_associations/" + str(associations[name][0]), associations[name][1])
    for name, (index, row) in pairs.items():
        pointer = "/coverage/changed_files/" + str(index)
        source(pointer, pointer, row)
        if name in associations:
            source(pointer, "/defect_resolution/necessary_associations/" + str(associations[name][0]), associations[name][1])
    result = {"report": {"defect_resolution": {**copy.deepcopy(detail), "evidence": normalized}, "coverage": copy.deepcopy(coverage)},
        "sources": sources, "binding": {"definition": copy.deepcopy(supplement), "attempt_id": attempt["attempt_id"],
            "sid": attempt["cli_sid"], "candidate_binding": _binding(candidate), "scope": "affected", "whole_package": False,
            "report": report_ref, "issues": issues_ref, "manifest": _ref(root / "manifest.json"), "collection": _ref(root / "collection.json"),
            "request": _ref(root / "request.md"), "trace": _ref(root / "trace.jsonl"), "termination": _ref(root / "termination.json"),
            "prior": {"attempt_id": prior["attempt"]["attempt_id"], "sid": prior["attempt"]["cli_sid"],
                "report": prior_ref, "original_reference": original_ref, "manifest": _ref(prior_root / "manifest.json"),
                "collection": _ref(prior_root / "collection.json")}, "parent_aliases": dict(sorted(aliases.items()))}}
    _need(_info(Path(report_ref["path"])) == {key: report_ref[key] for key in ("sha256", "size")}
          and _info(Path(prior_ref["path"])) == {key: prior_ref[key] for key in ("sha256", "size")}, "original signature changed while projecting")
    return result


def validate_recheck_supplement(*, supplement: dict, attempt: dict, prior: dict, parent: dict, candidate: dict,
                                closed_defect_ids: list[str], changed_paths: list[str], original_paths: set[str],
                                parent_aliases: dict[str, str] | None = None) -> dict:
    """Project exact original affected evidence; never sign or call a subprocess."""
    try:
        return _validate(supplement=supplement, attempt=attempt, prior=prior, parent=parent, candidate=candidate,
                         closed_defect_ids=closed_defect_ids, changed_paths=changed_paths,
                         original_paths=original_paths, parent_aliases=parent_aliases)
    except TaskflowError:
        raise
    except (RunnerError, OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise TaskflowError("recheck supplement source/shape verification: " + str(error)) from error
