"""Mechanical handoffs to existing release, formal runner and raw-run records.

These adapters neither grant an experiment nor grade a package. The caller
records the exact investment decision and dispatches through the normal worker.
"""

import ast
import importlib.util
import json
from functools import wraps
from pathlib import Path
import re
import stat
import subprocess
from typing import Any
import uuid

from taskflow.contracts import (
    PUBLIC_ROLES, TaskflowError, atomic_directory, canonical, parse_json,
    relative_path, sha256, validate_id, write_json,
)
from taskflow.execution_runner import (
    RunnerError, _full_layout, _full_tree, _full_verify, _info, _publish, _read, _regular,
    _trace, _validate, _validated_collection,
)
from taskflow.packages import check_task, verify_export, verify_snapshot
from taskflow.records import import_run, verify_run


def _handoff_errors(function: Any) -> Any:
    @wraps(function)
    def checked(*args: Any, **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except TaskflowError:
            raise
        except (RunnerError, OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as error:
            raise TaskflowError(function.__name__ + ": " + str(error)) from error
    return checked


def _binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in (
        "candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest"
    )}


def _batch_quality_source(attempt: dict | None, manifest: dict, store: object | None,
                          *, coverage: str, offline: bool = False) -> bool:
    """Use the original confirmed quality slot; offline reads do not claim DB verification."""
    from taskflow.execution_batch_binding import validate_batch_runner, validate_batch_source

    bound = (validate_batch_runner(manifest, manifest=True) is not None if offline
             else validate_batch_source(attempt, manifest, store))
    if bound and (coverage not in {"whole_package", "unchanged_scope", "affected"}
                  or manifest["batch_binding"]["model_slot"] != ("recheck" if coverage == "affected" else "quality")):
        raise TaskflowError("native quality source batch slot differs from its signed coverage")
    return bound


def _ref(path: Path) -> dict:
    return {"path": str(path.absolute()), **_info(path)}


def _destination(path: Path) -> Path:
    path = Path(path).absolute()
    if ".." in path.parts or any(part.is_symlink() for part in (path, *path.parents)):
        raise TaskflowError("handoff destination has traversal or a symlink component: " + str(path))
    return path


def _checked_ref(value: dict) -> tuple[Path, bytes]:
    if not isinstance(value, dict) or not value.get("path") or not value.get("sha256"):
        raise TaskflowError("hash-bound source reference required")
    path = Path(value["path"]).absolute()
    raw = _regular(path, limit=128 * 1024 * 1024)
    if sha256(raw) != value["sha256"] or ("size" in value and len(raw) != value["size"]):
        raise TaskflowError("bound source changed: " + str(path))
    return path, raw


def _once(path: Path, value: dict) -> None:
    if path.exists():
        if _read(path) != value:
            raise TaskflowError("immutable handoff belongs to different evidence: " + str(path))
    else:
        _publish(path, value)


def _selected(sources: dict, selector: dict) -> dict:
    name = selector["source"]
    source = sources[name]
    if "lines" in selector:
        lines = source["raw"].decode("utf-8").splitlines()
        numbers = selector["lines"]
        if not isinstance(numbers, list) or not numbers or any(type(item) is not int or item < 1 or item > len(lines) for item in numbers):
            raise TaskflowError("quality projection requires existing explicit source lines")
        value = "\n".join(lines[number - 1] for number in numbers)
        locator = {"lines": numbers}
    else:
        pointer = selector.get("pointer")
        if not isinstance(pointer, str) or not pointer.startswith("/"):
            raise TaskflowError("quality projection needs a JSON pointer or explicit lines")
        value = source["json"]
        try:
            for part in pointer[1:].split("/"):
                part = part.replace("~1", "/").replace("~0", "~")
                value = value[int(part)] if isinstance(value, list) else value[part]
        except (KeyError, IndexError, TypeError, ValueError) as error:
            raise TaskflowError("quality projection source locator missing: " + name + pointer) from error
        locator = {"pointer": pointer}
    if "equals" in selector and canonical(value) != canonical(selector["equals"]):
        raise TaskflowError("quality projection explicit affirmation differs: " + name + str(locator))
    if selector.get("nonempty") is True and not value:
        raise TaskflowError("quality projection source evidence/limitation is empty: " + name + str(locator))
    return {"source": name, "reference": source["reference"], **locator, "actual_value": value}


def _projection_authority(candidate: dict, protocol_map: dict) -> dict:
    path, raw = _checked_ref(protocol_map["authorization"])
    authority = parse_json(raw, str(path))
    if not authority.get("controller"):
        raise TaskflowError("quality projection lacks an attributed controller")
    selector = protocol_map.get("authorization_item")
    if selector is None:
        if authority.get("task_id") != candidate["task_id"]:
            raise TaskflowError("quality projection mapping lacks this task's attributed authority")
    else:
        if (not isinstance(selector, dict) or set(selector) != {"task_id", "source_author_quality_flow"}
                or selector["task_id"] != candidate["task_id"]):
            raise TaskflowError("quality projection authority item does not bind the exact task and source flow")
        items = authority.get("items")
        matches = [item for item in items if isinstance(item, dict)
                   and all(item.get(key) == value for key, value in selector.items())] if isinstance(items, list) else []
        if len(matches) != 1:
            raise TaskflowError("quality projection authority needs one exact task/source-flow item")
    return authority


def _projection_data(store: object, candidate: dict, source_refs: dict, protocol_map: dict) -> dict:
    if any(protocol_map.get(key) is not None for key in ("candidate_acceptance", "repaired_candidate_acceptance")) and protocol_map.get("native_signoff") is None:
        raise TaskflowError("candidate routing acceptance requires its exact native signed handoff")
    if protocol_map.get("native_signoff") is not None:
        expected = native_quality_handoff(store, candidate_id=candidate["candidate_id"],
                                          authorization=protocol_map["authorization"],
                                          source_flow_id=protocol_map["native_signoff"]["source_flow_id"],
                                          packet_transport=protocol_map["native_signoff"].get("packet_transport"))
        if source_refs != expected["source_refs"] or protocol_map != expected["protocol_map"]:
            raise TaskflowError("native quality projection differs from its exact signed handoff")
    accepted = store.quality_gate(candidate["candidate_id"])
    if accepted.get("composite_quality_coverage"):
        return _composite_projection_data(store, candidate, source_refs, protocol_map, accepted)
    if not accepted.get("attempt_id"):
        raise TaskflowError("signed projection needs the current completed managed quality attempt")
    attempt = store.get_attempt(accepted["attempt_id"])
    root = Path(attempt["runtime_path"])
    manifest = _read(root / "manifest.json")
    if (attempt.get("state") != "completed" or attempt.get("role") != "quality"
            or attempt.get("candidate_id") != candidate["candidate_id"]
            or attempt.get("intent_id") != manifest.get("intent_id")
            or manifest.get("nature") != "real_cli_attempt" or manifest.get("candidate_binding") != _binding(candidate)):
        raise TaskflowError("projection source is not a real completed same-candidate independent quality")
    collection = _validated_collection(root, manifest)
    if not attempt.get("cli_sid") or collection.get("sid") != attempt["cli_sid"]:
        raise TaskflowError("projection source signer SID differs from actual collected quality")
    _projection_authority(candidate, protocol_map)
    delivered = {row["sha256"] for row in collection["artifacts"].values()}
    sources = {}
    for name, reference in source_refs.items():
        path, raw = _checked_ref(reference)
        if sha256(raw) not in delivered:
            raise TaskflowError("quality projection source was not delivered by its signed attempt: " + name)
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            value = None
        sources[name] = {"raw": raw, "json": value, "reference": _ref(path)}
    scope = _selected(sources, protocol_map["scope"])
    if scope["actual_value"] != "whole_package" or protocol_map["scope"].get("equals") != "whole_package":
        raise TaskflowError("quality projection cannot promote an affected source to whole_package")
    result = {}
    for kind, dimension in protocol_map["dimensions"].items():
        affirmations = dimension.get("affirmations", [])
        if not affirmations or not any("equals" in item for item in affirmations):
            raise TaskflowError("quality projection lacks an explicit affirmative field: " + kind)
        if not dimension.get("evidence") or not dimension.get("limitations"):
            raise TaskflowError("quality projection needs source evidence and limitations for " + kind)
        affirmative = [_selected(sources, item) for item in affirmations]
        evidence = [_selected(sources, item) for item in dimension["evidence"]]
        limitations = [_selected(sources, item) for item in dimension["limitations"]]
        chain = {"candidate_binding": _binding(candidate), "accepted_review_id": accepted["review_id"],
                 "attempt_id": attempt["attempt_id"], "cli_sid": attempt["cli_sid"],
                 "sources": {name: value["reference"] for name, value in sources.items()},
                 "protocol_map": protocol_map, "protocol_digest": sha256(canonical(protocol_map)),
                 "dimension": kind, "scope_source": scope, "affirmative_sources": affirmative}
        result[kind] = {"schema_version": 1, "kind": kind, "status": "passed", "scope": "whole_package",
                        "candidate_binding": _binding(candidate), "evidence": evidence, "limitations": limitations,
                        "derived_quality_projection": chain,
                        "signer": {"source": "original collected independent role", "cli_sid": attempt["cli_sid"]},
                        "interpretation": "Authorized mechanical projection of explicit original signed dimensions; no rerating or new independent signature."}
        _retain_native_acceptance(result[kind], protocol_map, kind)
    if not result:
        raise TaskflowError("quality projection needs explicit declared dimensions")
    return result


def _composite_projection_data(store: object, candidate: dict, source_refs: dict,
                               protocol_map: dict, accepted: dict) -> dict:
    """Project the exact original scopes and any validated bounded supplement."""
    graph = accepted["composite_quality_coverage"]
    if accepted.get("whole_package") is not False or accepted.get("snapshot_bound_acceptance") is not True:
        raise TaskflowError("composite projection cannot upgrade the fresh affected signature")
    allowed, signers = {}, {}
    for group, field in (("whole", "whole_source"), ("affected", "affected_source")):
        source = graph[field]
        attempt = store.get_attempt(source["attempt_id"])
        bound = store.get_candidate(source["candidate_id"])
        root = Path(attempt["runtime_path"])
        manifest = _read(root / "manifest.json")
        if (attempt.get("state") != "completed" or attempt.get("role") != "quality"
                or attempt.get("candidate_id") != source["candidate_id"]
                or attempt.get("cli_sid") != source["sid"] or not attempt.get("cli_sid")
                or manifest.get("intent_id") != attempt.get("intent_id")
                or manifest.get("nature") != "real_cli_attempt"
                or manifest.get("candidate_binding") != _binding(bound)):
            raise TaskflowError("composite projection source is not its actual completed independent signature")
        collection = _validated_collection(root, manifest)
        if (collection.get("sid") != attempt["cli_sid"] or collection.get("technical_accepted") is not True
                or collection.get("process_ended") is not True):
            raise TaskflowError("composite projection collection or signer differs")
        for name, info in collection["artifacts"].items():
            allowed[str(root / "artifacts" / name)] = (group, info)
        signers[group] = attempt
    supplement = _supplement_quality_source(store, candidate, graph)
    if supplement is not None:
        for name, reference in supplement["source_refs"].items():
            path, raw = _checked_ref(reference)
            allowed[str(path)] = ("supplement", {"sha256": sha256(raw), "size": len(raw)})
        signers["supplement"] = supplement["attempt"]
    if (graph["affected_source"]["candidate_id"] != candidate["candidate_id"]
            or graph["input_digest"] != candidate["input_digest"]
            or not graph["private_changes"] or not graph["closed_defect_ids"]):
        raise TaskflowError("composite projection lacks the current exact repaired coverage graph")
    _projection_authority(candidate, protocol_map)
    sources = {}
    for name, reference in source_refs.items():
        path, raw = _checked_ref(reference)
        original = allowed.get(str(path))
        if original is None or original[1] != {"sha256": sha256(raw), "size": len(raw)}:
            group_label = "two actual" if supplement is None else "explicit actual"
            raise TaskflowError("composite projection reference is outside its " + group_label + " signed artifact sets: " + name)
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            value = None
        sources[name] = {"raw": raw, "json": value, "reference": _ref(path), "group": original[0]}
    scope = _selected(sources, protocol_map["scope"])
    if (sources[scope["source"]]["group"] != "whole" or scope["actual_value"] != "whole_package"
            or protocol_map["scope"].get("equals") != "whole_package"):
        raise TaskflowError("composite scope must retain the old whole source, never promote fresh affected")
    fresh_name = next((name for name, value in sources.items()
                       if value["reference"]["path"] == graph["affected_source"]["report"]["path"]), None)
    if fresh_name is None:
        raise TaskflowError("composite projection lacks its actual fresh closure report")
    signed_ids = sources[fresh_name]["json"].get("execution_routing", {}).get("covered_defect_ids")
    if not isinstance(signed_ids, list) or set(signed_ids) != set(graph["closed_defect_ids"]):
        raise TaskflowError("composite closure does not cover its exact defect graph")
    closure = [_selected(sources, {"source": fresh_name, "pointer": pointer, "equals": expected})
               for pointer, expected in (("/execution_routing/outcome", "accepted"),
                                         ("/execution_routing/scope", "affected"),
                                         ("/execution_routing/covered_defect_ids", signed_ids))]
    result = {}
    for kind, dimension in protocol_map["dimensions"].items():
        affirmations = dimension.get("affirmations", [])
        if not affirmations or not dimension.get("evidence") or not dimension.get("limitations"):
            raise TaskflowError("composite dimension needs explicit old affirmation/current closure/evidence/limits: " + kind)
        affirmative = [_selected(sources, item) for item in affirmations]
        repaired = protocol_map.get("repaired_candidate_acceptance")
        old_positive = [row for row in affirmative if sources[row["source"]]["group"] == "whole"
                        and row["actual_value"] and row != scope]
        if repaired is not None:
            prior = repaired["prior_whole_findings"]
            selectors = [{"source": "whole_adoption", "pointer": "/native_signoff/dimensions/" + kind + "/affirmed",
                          "equals": prior["original_dimensions"][kind]["affirmed"]},
                         {"source": "whole_adoption", "pointer": "/execution_routing/outcome", "equals": "revision_required"},
                         {"source": "whole_adoption", "pointer": "/execution_routing/blocking_issue_ids", "equals": prior["blocking_issue_ids"]},
                         {"source": "adoption", "pointer": "/execution_routing/covered_defect_ids", "equals": signed_ids}]
            if (protocol_map.get("native_signoff") is None or repaired["composite_graph_sha256"] != sha256(canonical(graph))
                    or repaired["accepted_review_id"] != accepted["review_id"]
                    or any(selector not in affirmations for selector in selectors)):
                raise TaskflowError("repaired candidate projection lacks exact prior findings and current closure sources")
            old_positive = [_selected(sources, selector) for selector in selectors]
        fresh_positive = any(sources[row["source"]]["group"] == "affected"
                             and row.get("pointer") == "/execution_routing/outcome"
                             and row["actual_value"] == "accepted" and item.get("equals") == "accepted"
                             for row, item in zip(affirmative, affirmations))
        evidence = [_selected(sources, item) for item in dimension["evidence"]]
        if not old_positive or not fresh_positive or not any(
                sources[row["source"]]["group"] == "affected" and row["actual_value"] for row in evidence):
            raise TaskflowError("composite dimension omits its original affirmative or fresh changed-association evidence: " + kind)
        limitations = [_selected(sources, item) for item in dimension["limitations"]]
        if supplement is not None:
            positive = any(sources[row["source"]]["group"] == "supplement"
                           and row.get("pointer") == "/native_signoff/dimensions/" + kind + "/affirmed"
                           and row["actual_value"] is True and item.get("equals") is True
                           for row, item in zip(affirmative, affirmations))
            if (not positive or not any(sources[row["source"]]["group"] == "supplement"
                                        and row["actual_value"] for row in evidence)
                    or not any(sources.get(row.get("source"), {}).get("group") == "supplement"
                               for row in limitations)):
                raise TaskflowError("composite dimension omits its exact supplemental affirmation/evidence/limits: " + kind)
        limitations.append({"inherited_signed_source_limits": graph["source_limits"]})
        chain = {"candidate_binding": _binding(candidate), "accepted_review_id": accepted["review_id"],
                 "attempt_id": signers["affected"]["attempt_id"], "cli_sid": signers["affected"]["cli_sid"],
                 "sources": {name: value["reference"] for name, value in sources.items()},
                 "source_groups": {name: value["group"] for name, value in sources.items()},
                 "protocol_map": protocol_map, "protocol_digest": sha256(canonical(protocol_map)),
                 "dimension": kind, "scope_source": scope, "affirmative_sources": affirmative,
                 "closure_sources": closure, "composite_quality_coverage": graph, "whole_package": False}
        result[kind] = {"schema_version": 1, "kind": kind, "status": "passed",
                        "scope": "explicit_repaired_quality_coverage", "whole_package": False,
                        "candidate_binding": _binding(candidate), "evidence": evidence, "limitations": limitations,
                        "derived_quality_projection": chain, "composite_quality_coverage": graph,
                        "signer": {"source": ("original signed scopes and bounded supplement; no new whole-package signature"
                                               if supplement is not None else "two original signed scopes; no new whole-package signature"),
                                   "cli_sid": signers["affected"]["cli_sid"], "whole_source_cli_sid": signers["whole"]["cli_sid"]},
                        "interpretation": "Authorized mechanical projection of exact old whole scope and fresh affected closure; no rerating or new independent signature."}
        _retain_native_acceptance(result[kind], protocol_map, kind)
        _retain_repaired_acceptance(result[kind], protocol_map, kind)
    if not result:
        raise TaskflowError("composite projection needs explicit declared dimensions")
    return result


def _supplement_quality_source(store: object, candidate: dict, graph: dict) -> dict | None:
    """Transport an already validated third source without replacing its base."""
    supplemental = graph.get("recheck_supplement")
    if supplemental is None:
        return None
    definition = supplemental["definition"]
    if definition["base_recheck_id"] != graph["recheck_id"]:
        raise TaskflowError("native quality supplement has a different original affected review")
    attempt = store.get_attempt(definition["attempt_id"])
    root = Path(attempt["runtime_path"])
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    batch_source = _batch_quality_source(attempt, manifest, store, coverage="affected")
    if (attempt.get("state") != "completed" or attempt.get("role") != "quality"
            or attempt.get("candidate_id") != candidate["candidate_id"]
            or not attempt.get("cli_sid") or collection.get("sid") != attempt["cli_sid"]
            or attempt["cli_sid"] in {graph["whole_source"]["sid"], graph["affected_source"]["sid"]}
            or manifest.get("candidate_binding") != _binding(candidate)
            or manifest.get("intent_id") != attempt.get("intent_id")
            or manifest.get("mode") != "native_role" or manifest.get("nature") != "real_cli_attempt"
            or (not batch_source and (manifest.get("model", {}).get("requested") != "gpt-6.1-sol"
                                     or manifest.get("model", {}).get("reasoning_effort") != "xhigh"))
            or collection.get("technical_accepted") is not True or collection.get("process_ended") is not True):
        raise TaskflowError("native quality supplement is not its completed independent same-candidate source")
    refs = {}
    for name, filename in (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md")):
        path = root / "artifacts" / filename
        if collection["artifacts"].get(filename) != _info(path):
            raise TaskflowError("native quality supplement output differs from its actual collection")
        if name in {"adoption", "issues"}:
            expected = definition["report" if name == "adoption" else "issues"]
            _checked_ref(expected)
            if Path(expected["path"]) != path or expected["sha256"] != _info(path)["sha256"]:
                raise TaskflowError("native quality supplement source reference differs")
        refs["supplement_" + name] = _ref(path)
    return {"attempt": attempt, "source_refs": refs,
            "certification": {"attempt_id": attempt["attempt_id"], "sid": attempt["cli_sid"],
                              "candidate_binding": _binding(candidate), "scope": "affected",
                              "manifest": _ref(root / "manifest.json"), "collection": _ref(root / "collection.json"),
                              "original_affected_source_retained": graph["recheck_id"], "definition": definition}}


def _signoff_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError("native quality needs nonempty " + field)
    return value


def _signoff_strings(value: object, field: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise TaskflowError("native quality needs an explicit " + field + " list")
    return [_signoff_text(item, field) for item in value]


def _signoff_citations(rows: object, candidate: dict, manifest: dict, root: Path,
                       *, public_only: bool = False) -> None:
    if not isinstance(rows, list) or not rows:
        raise TaskflowError("native quality needs actual source citations")
    files = {item["package_path"]: item for item in candidate["files"]}
    for row in rows:
        if not isinstance(row, dict):
            raise TaskflowError("native quality citation must be an object")
        path = relative_path(row.get("path"))
        source = files.get(path)
        if source is not None:
            info = _info(Path(source["path"]))
            entries = [item for item in manifest["inputs"] if item["source"] == source["path"]
                       and item["target"] == path and all(item[key] == info[key] for key in ("sha256", "size"))]
            if (info["sha256"] != source["sha256"] or (public_only and source["role"] not in PUBLIC_ROLES)
                    or len(entries) != 1 or entries[0]["kind"] != ("public" if source["role"] in PUBLIC_ROLES else "teacher")):
                raise TaskflowError("native quality citation is not the actual current public/source bytes")
        else:
            inputs = [item for item in manifest["inputs"] if item["target"] == path]
            permitted = (path == "execution/task.json" and len(inputs) == 1
                         and inputs[0]["kind"] == "source_metadata") or (
                             len(inputs) == 1 and inputs[0]["kind"] in ("submission", "trace")
                             and (path.startswith(("actual-public-work/", "actual-public-scratch/"))
                                  or path == "actual-public-trace.jsonl"))
            if public_only or not permitted:
                raise TaskflowError("native quality citation is outside current candidate/public observation sources")
            info = _info(root / "inputs" / path)
            if info != {key: inputs[0][key] for key in ("sha256", "size")}:
                raise TaskflowError("native quality observed source changed")
            if path == "execution/task.json":
                declarations = [item for item in candidate.get("evidence", []) if item.get("location") == "explicit package declaration"]
                if (len(declarations) != 1 or inputs[0]["source"] != declarations[0]["path"]
                        or info["sha256"] != declarations[0]["sha256"]):
                    raise TaskflowError("native quality metadata citation has a foreign candidate declaration")
        if row.get("sha256") != info["sha256"]:
            raise TaskflowError("native quality citation SHA differs from actual source")
        _signoff_text(row.get("location"), "citation location")


def _accepted_observation_source(candidate: dict, attempt: dict, manifest: dict,
                                 root: Path, refs: dict, report: dict, coverage: str) -> dict:
    """Keep a scoped false affirmation distinct from accepted nonblocking routing."""
    from taskflow.execution_flow_contracts import _nonblocking_observation, parse_review

    routing, dimensions = report["execution_routing"], report["native_signoff"]["dimensions"]
    issues = _read(Path(refs["issues"]["path"]))
    rows = issues.get("issues")
    if (routing.get("outcome") != "accepted" or routing.get("blocking_issue_ids") != []
            or ("blocking_issue_ids" in issues and issues["blocking_issue_ids"] != [])
            or not isinstance(rows, list) or not rows):
        raise TaskflowError("native quality dimension has no explicit scoped affirmation or accepted nonblocking observations")
    for row in rows:
        if (not _nonblocking_observation(row, routing["blocking_issue_ids"]) or row.get("blocking") is not False
                or row.get("disposition") not in (None, "observed_unrepaired", "retained")
                or row.get("needs_business_decision", False) is not False):
            raise TaskflowError("native false affirmation cannot transport blocking or unresolved issues")
        _signoff_citations(row.get("evidence"), candidate, manifest, root)
    used = {citation["path"] for row in rows for citation in row["evidence"]}
    used.update(path for row in rows for path in row.get("affected_paths", []) if isinstance(path, str))
    metadata = None
    if "execution/task.json" in used:
        from taskflow.execution_flow_contracts import review_metadata_aliases

        metadata = review_metadata_aliases(candidate, manifest, root)
    observed = {entry["target"]: {"path": entry["source"], "sha256": entry["sha256"], "kind": entry["kind"]}
                for entry in manifest["inputs"] if entry["target"] in used
                and entry["kind"] in {"submission", "trace"}}
    parsed = parse_review(Path(refs["adoption"]["path"]), Path(refs["issues"]["path"]),
                          candidate=candidate, intent_id=attempt["intent_id"], scope=routing["scope"],
                          metadata_aliases=metadata, observed_aliases=observed)
    if parsed["outcome"] != "accepted" or parsed["defects"]:
        raise TaskflowError("native false affirmation has no accepted candidate routing")
    observation_sources = {(citation["path"], citation["sha256"]) for row in rows for citation in row["evidence"]}
    for kind, dimension in dimensions.items():
        if dimension["affirmed"] is False:
            _signoff_strings(dimension["limitations"], kind + " false-affirmation limitations", nonempty=True)
            if not observation_sources.intersection((citation["path"], citation["sha256"]) for citation in dimension["evidence"]):
                raise TaskflowError("native false affirmation is not linked to its explicit nonblocking observation: " + kind)
    return {"schema_version": 1, "kind": "accepted_candidate_with_nonblocking_observations",
            "candidate_binding": _binding(candidate), "outcome": "accepted", "scope": routing["scope"],
            "coverage": coverage, "blocking_issue_ids": [], "report": refs["adoption"], "issues": refs["issues"],
            "original_dimensions": dimensions,
            "observations": [{"reference": refs["issues"], "pointer": f"/issues/{index}", "actual_value": row}
                             for index, row in enumerate(rows)]}


def _retain_native_acceptance(report: dict, protocol: dict, kind: str) -> None:
    """Label the legacy freeze status without turning a source false into true."""
    acceptance = protocol.get("candidate_acceptance")
    if acceptance is None:
        return
    dimension = acceptance["original_dimensions"][kind]
    report.update(status_basis="candidate_routing_acceptance", candidate_acceptance=acceptance,
                  original_dimension={**dimension, "coverage": acceptance["coverage"],
                                      "source": acceptance["report"],
                                      "pointer": "/native_signoff/dimensions/" + kind},
                  nonblocking_observations=acceptance["observations"],
                  interpretation="Legacy passed records only current-candidate routing acceptance without blockers. "
                                 "It does not assert a true dimension affirmation or repair any original observation; "
                                 "the original booleans, scopes, evidence and limitations remain explicit.")


def _prior_native_scope(signoff: dict, required: list[str], blocking: list[str]) -> dict:
    """Bind limited negative observations to explicit original whole findings."""
    if signoff.get("coverage") == "whole_package":
        return {}
    if signoff.get("coverage") != "unchanged_scope":
        raise TaskflowError("prior findings require original whole or unchanged native coverage")
    excluded = _signoff_strings(signoff.get("affected_paths"), "prior excluded paths", nonempty=True)
    if len(set(excluded)) != len(excluded) or not set(excluded) <= set(required):
        raise TaskflowError("needs_business_decision: prior exclusions lack exact blocking findings")
    linked = set()
    for row in signoff["responsibility_analysis"]:
        ids = _signoff_strings(row.get("blocking_issue_ids"), "prior responsibility blocking IDs")
        if (row.get("disposition") not in {"retained", "blocking_defect"}
                or (row["disposition"] == "blocking_defect") != bool(ids)
                or not set(ids) <= set(blocking)):
            raise TaskflowError("needs_business_decision: prior responsibility remains unresolved")
        linked.update(ids)
    if linked != set(blocking):
        raise TaskflowError("needs_business_decision: prior findings lack explicit responsibility linkage")
    for kind, dimension in signoff["dimensions"].items():
        if dimension["affirmed"] is False and not set(excluded) & {row["path"] for row in dimension["evidence"]}:
            raise TaskflowError("needs_business_decision: prior false dimension lacks excluded finding evidence: " + kind)
    return {"affected_paths": excluded,
            "coverage_basis": _signoff_text(signoff.get("coverage_basis"), "prior coverage basis")}


def _whole_finding_locations(report: dict, required: list[str], manifest: dict,
                              root: Path, refs: dict, issues: list[dict], candidate: dict | None = None) -> dict:
    """Keep a negative whole review's explicit finding targets as original data."""
    signoff, routing = report["native_signoff"], report["execution_routing"]
    affected = signoff.get("affected_paths")
    if signoff.get("coverage") != "whole_package" or not affected:
        return {}
    paths = _signoff_strings(affected, "original whole finding paths", nonempty=True)
    blocking = _signoff_strings(routing.get("blocking_issue_ids"), "original whole blocking IDs", nonempty=True)
    if (routing.get("scope") != "whole_package" or routing.get("outcome") != "revision_required"
            or len(set(paths)) != len(paths) or set(paths) != set(required)
            or not any(row.get("affirmed") is False for row in signoff["dimensions"].values())):
        raise TaskflowError("negative whole finding paths require exact original blocking targets and false affirmations")
    targets = []
    for path in paths:
        path = relative_path(path)
        inputs = [row for row in manifest["inputs"] if row["target"] == path and row["kind"] in {"public", "teacher"}]
        if len(inputs) != 1:
            raise TaskflowError("original whole finding target has no unique original payload input")
        row = inputs[0]
        if candidate is not None:
            files = [item for item in candidate["files"] if item["package_path"] == path]
            if (len(files) != 1 or files[0]["path"] != row["source"] or files[0]["sha256"] != row["sha256"]
                    or row["kind"] != ("public" if files[0]["role"] in PUBLIC_ROLES else "teacher")):
                raise TaskflowError("original whole finding input is not its captured candidate payload")
        source = Path(row["source"])
        staged = root / "inputs" / path
        expected = {key: row[key] for key in ("sha256", "size")}
        if _info(source) != expected or _info(staged) != expected:
            raise TaskflowError("original whole finding source/staged bytes drifted")
        pointers = [{"reference": refs["issues"], "pointer": f"/issues/{index}/affected_paths",
                     "issue_id": issue["id"], "actual_value": issue["affected_paths"]}
                    for index, issue in enumerate(issues) if issue["id"] in blocking and path in issue["affected_paths"]]
        if not pointers:
            raise TaskflowError("original whole finding target has no signed blocking-issue pointer")
        targets.append({"package_path": path, "input": row, "source": _ref(source), "staged": _ref(staged),
                        "issue_targets": pointers})
    return {"affected_paths": paths, "coverage_basis": _signoff_text(signoff.get("coverage_basis"), "original whole coverage basis"),
            "finding_locations": {"schema_version": 1, "kind": "original_negative_whole_finding_locations",
                "coverage": "whole_package", "outcome": "revision_required", "report": refs["adoption"],
                "pointer": "/native_signoff/affected_paths", "actual_value": affected,
                "candidate_binding": manifest["candidate_binding"], "blocking_issue_ids": blocking, "targets": targets}}


def _prior_whole_findings(candidate: dict, attempt: dict, manifest: dict, root: Path,
                          refs: dict, report: dict) -> dict:
    """Read a real blocking whole review without relabeling its false dimensions."""
    from taskflow.execution_flow_contracts import parse_review, review_metadata_aliases

    routing, signoff = report["execution_routing"], report["native_signoff"]
    blocking = _signoff_strings(routing.get("blocking_issue_ids"), "prior whole blocking IDs", nonempty=True)
    issues = _read(Path(refs["issues"]["path"]))
    rows = issues.get("issues")
    if not isinstance(rows, list):
        raise TaskflowError("prior whole findings need their actual issue records")
    for row in rows:
        if not isinstance(row, dict):
            raise TaskflowError("prior whole finding must be an issue object")
        _signoff_citations(row.get("evidence"), candidate, manifest, root)
    used = {citation["path"] for row in rows for citation in row["evidence"]}
    used.update(path for row in rows for path in row.get("affected_paths", []) if isinstance(path, str))
    metadata = review_metadata_aliases(candidate, manifest, root) if "execution/task.json" in used else None
    observed = {entry["target"]: {"path": entry["source"], "sha256": entry["sha256"], "kind": entry["kind"]}
                for entry in manifest["inputs"] if entry["target"] in used and entry["kind"] in {"submission", "trace"}}
    parsed = parse_review(Path(refs["adoption"]["path"]), Path(refs["issues"]["path"]), candidate=candidate,
                          intent_id=attempt["intent_id"], scope="whole_package",
                          metadata_aliases=metadata, observed_aliases=observed)
    if (routing["outcome"] != "revision_required" or signoff["coverage"] not in {"whole_package", "unchanged_scope"}
            or not parsed["whole_package"] or parsed["outcome"] != "revision_required"
            or set(blocking) != {row["id"] for row in parsed["defects"]}):
        raise TaskflowError("prior whole source has no exact original blocking findings")
    required = sorted({path for row in parsed["defects"] for path in row["affected_paths"]})
    scope = _prior_native_scope(signoff, required, blocking)
    scope.update(_whole_finding_locations(report, required, manifest, root, refs, rows, candidate))
    return {"schema_version": 1, "kind": "original_whole_package_findings", "scope": "whole_package",
            "coverage": signoff["coverage"], "outcome": "revision_required", "blocking_issue_ids": blocking,
            "report": refs["adoption"], "issues": refs["issues"], "original_dimensions": signoff["dimensions"],
            "original_issues": [{"reference": refs["issues"], "pointer": f"/issues/{index}", "actual_value": row}
                                for index, row in enumerate(rows)],
            "required_associations": required, **scope}


def _repaired_candidate_source(candidate: dict, accepted: dict, graph: dict, whole: dict, fresh: dict) -> dict:
    """Transport the canonical gate's closure, not a positive old whole judgment."""
    prior = whole["prior_whole_findings"]
    if prior["coverage"] == "unchanged_scope" and any(
            dimension["affirmed"] is not True for dimension in fresh["signoff"]["dimensions"].values()):
        raise TaskflowError("needs_business_decision: limited prior findings need explicit current affected affirmations")
    closed = _signoff_strings(graph.get("closed_defect_ids"), "closed defects", nonempty=True)
    originals = graph.get("original_issue_ids")
    inherited = graph.get("inherited_closure_chains", [])
    if (accepted.get("whole_package") is not False or accepted.get("snapshot_bound_acceptance") is not True
            or accepted.get("attempt_id") != fresh["attempt"]["attempt_id"]
            or len(set(closed)) != len(closed) or not isinstance(originals, dict)
            or not set(originals) <= set(closed)
            or set(prior["blocking_issue_ids"]) != set(originals.values())
            or not isinstance(graph["whole_source"].get("scope"), str) or not graph["whole_source"]["scope"].strip()
            or graph["whole_source"].get("whole_package") is not True
            or graph["whole_source"].get("verdict") != "defects"
            or not isinstance(graph["affected_source"].get("scope"), str) or not graph["affected_source"]["scope"].strip()
            or graph["affected_source"].get("whole_package") is not False
            or fresh["report"]["execution_routing"].get("blocking_issue_ids") != []
            or set(fresh["report"]["execution_routing"].get("covered_defect_ids", [])) != set(closed)
            or not graph.get("whole_review_id") or graph.get("recheck_id") is None
            or not isinstance(inherited, list)):
        raise TaskflowError("repaired candidate requires the exact canonical prior-whole/current-affected closure graph")
    inherited_ids = []
    for chain in inherited:
        if (not isinstance(chain, dict) or type(chain.get("schema_version")) is not int or chain["schema_version"] != 1
                or chain.get("kind") != "whole_then_same_public_affected"
                or chain.get("repair_review_id") != graph["whole_review_id"]
                or chain.get("final_review_id") != graph["recheck_id"]):
            raise TaskflowError("inherited closure does not use these exact whole/current review anchors")
        inherited_ids.append(_signoff_text(chain.get("defect_id"), "inherited defect ID"))
    if (len(set(inherited_ids)) != len(inherited_ids) or set(inherited_ids) & set(originals)
            or set(inherited_ids) | set(originals) != set(closed)):
        raise TaskflowError("repaired candidate leaves an original or inherited blocking finding unclosed")
    if inherited and graph.get("coverage_mode") != "whole_findings_with_inherited_affected_closure_v1":
        raise TaskflowError("additional prior findings require the canonical inherited closure chains")
    if not set(prior["required_associations"]) <= set(fresh["signoff"]["affected_paths"]):
        raise TaskflowError("fresh affected signature omits a prior whole finding's necessary association")
    return {"schema_version": 1, "kind": "prior_whole_findings_with_verified_affected_closure",
            "candidate_binding": _binding(candidate), "accepted_review_id": accepted["review_id"],
            "whole_review_id": graph["whole_review_id"], "recheck_id": graph["recheck_id"],
            "prior_whole_findings": prior,
            "current_affected_closure": {"report": fresh["source_refs"]["adoption"], "issues": fresh["source_refs"]["issues"],
                "coverage": "affected", "scope": "affected", "outcome": "accepted",
                "covered_defect_ids": fresh["report"]["execution_routing"]["covered_defect_ids"],
                "original_dimensions": fresh["signoff"]["dimensions"]},
            "composite_graph_sha256": sha256(canonical(graph)), "private_changes": graph["private_changes"],
            "inherited_closure_chains": inherited}


def _retain_repaired_acceptance(report: dict, protocol: dict, kind: str) -> None:
    repaired = protocol.get("repaired_candidate_acceptance")
    if repaired is None:
        return
    report.update(status_basis="verified_repaired_candidate_acceptance", repaired_candidate_acceptance=repaired,
                  prior_dimension=repaired["prior_whole_findings"]["original_dimensions"][kind],
                  current_dimension=repaired["current_affected_closure"]["original_dimensions"][kind],
                  interpretation="Legacy passed records only canonical repaired-candidate acceptance from original whole findings "
                                 "and exact fresh affected closure. Prior whole false affirmations, blocking findings and limits "
                                 "remain unchanged; no unchanged-scope affirmation or new whole pass is inferred.")


def _native_quality_source(store: object, candidate: dict, attempt_id: str,
                           *, coverage: str, outcome: str, prior_whole_findings: bool = False,
                           offline: bool = False) -> dict:
    from taskflow.execution_flow_contracts import (
        NATIVE_SIGNOFF_PROTOCOL_SHA256, request_has_native_signoff_protocol,
    )

    if coverage not in {"whole_package", "unchanged_scope", "affected"}:
        raise TaskflowError("native quality has an unknown original coverage")
    attempt = store.get_attempt(attempt_id)
    root = Path(attempt["runtime_path"])
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    batch_source = _batch_quality_source(attempt, manifest, store, coverage=coverage, offline=offline)
    if (attempt.get("state") != "completed" or attempt.get("role") != "quality"
            or attempt.get("candidate_id") != candidate["candidate_id"]
            or manifest.get("candidate_binding") != _binding(candidate)
            or manifest.get("intent_id") != attempt.get("intent_id")
            or manifest.get("mode") != "native_role" or manifest.get("nature") != "real_cli_attempt"
            or (not batch_source and (manifest.get("model", {}).get("requested") != "gpt-6.1-sol"
                                     or manifest.get("model", {}).get("reasoning_effort") != "xhigh"))
            or collection.get("technical_accepted") is not True or collection.get("process_ended") is not True
            or not attempt.get("cli_sid") or collection.get("sid") != attempt["cli_sid"]):
        raise TaskflowError("native quality requires its actual completed independent Sol/xhigh candidate attempt")
    request = _regular(root / "request.md")
    if not request_has_native_signoff_protocol(request):
        raise TaskflowError("native quality protocol is absent from the actual locked request")
    refs = {}
    for name, filename in (("adoption", "adoption-readiness.json"), ("intake", "intake.md"), ("issues", "issues.json")):
        path = root / "artifacts" / filename
        if collection["artifacts"].get(filename) != _info(path):
            raise TaskflowError("native quality source is absent from the actual collection: " + filename)
        refs[name] = _ref(path)
    report = _read(Path(refs["adoption"]["path"]))
    routing, signed = report.get("execution_routing"), report.get("native_signoff")
    expected_scope = "affected" if coverage == "affected" else "whole_package"
    routing_binding = {key: value for key, value in _binding(candidate).items() if key != "task_id"}
    routing_binding["intent_id"] = attempt["intent_id"]
    if (not isinstance(routing, dict) or routing.get("scope") != expected_scope or routing.get("outcome") != outcome
            or type(routing.get("schema_version")) is not int or routing["schema_version"] != 1
            or any(routing.get(key) != value for key, value in routing_binding.items())
            or ("task_id" in routing and routing["task_id"] != candidate["task_id"])
            or routing.get("signer_role") != "independent_quality"
            or routing.get("independent_of_current_author") is not True or routing.get("participated_in_production") is not False
            or not isinstance(signed, dict) or type(signed.get("schema_version")) is not int or signed["schema_version"] != 1
            or signed.get("protocol_sha256") != NATIVE_SIGNOFF_PROTOCOL_SHA256 or signed.get("coverage") != coverage):
        raise TaskflowError("native quality signoff differs from the actual version, protocol or signed scope")
    affected = _signoff_strings(signed.get("affected_paths"), "affected_paths")
    files = {item["package_path"] for item in candidate["files"]}
    if len(set(affected)) != len(affected) or any(relative_path(path) not in files for path in affected):
        raise TaskflowError("native quality affected paths must name unique current candidate files")
    if ((coverage == "whole_package" and affected and not (prior_whole_findings and outcome == "revision_required"))
            or (coverage != "whole_package" and not affected)):
        raise TaskflowError("native quality coverage must explicitly delimit whole or affected files")
    _signoff_text(signed.get("coverage_basis"), "coverage basis")
    dimensions = signed.get("dimensions")
    if not isinstance(dimensions, dict) or set(dimensions) != {"business", "artifact", "calculation"}:
        raise TaskflowError("native quality needs exactly three explicit dimensions")
    for kind, value in dimensions.items():
        if (not isinstance(value, dict) or type(value.get("affirmed")) is not bool
                or (value["affirmed"] is False and outcome != "accepted" and not prior_whole_findings)):
            raise TaskflowError("native quality dimension has no explicit scoped affirmation: " + kind)
        _signoff_citations(value.get("evidence"), candidate, manifest, root)
        _signoff_strings(value.get("limitations"), kind + " limitations")
    analysis = signed.get("responsibility_analysis")
    if not isinstance(analysis, list) or not analysis:
        raise TaskflowError("native quality needs explicit public responsibility analysis")
    for row in analysis:
        if not isinstance(row, dict):
            raise TaskflowError("native quality responsibility entry must be an object")
        _signoff_citations(row.get("public_evidence"), candidate, manifest, root, public_only=True)
        for key in ("upstream_work_already_done", "remaining_judgment", "consequence"):
            _signoff_text(row.get(key), key)
        for key in ("retained_necessary_conditions", "legitimate_branches"):
            _signoff_strings(row.get(key), key, nonempty=True)
        issue_ids = _signoff_strings(row.get("blocking_issue_ids"), "responsibility blocking IDs")
        if (row.get("disposition") not in ("retained", "blocking_defect")
                or not set(issue_ids) <= set(routing.get("blocking_issue_ids", []))
                or (row["disposition"] == "blocking_defect") != bool(issue_ids)
                or (outcome == "accepted" and row["disposition"] != "retained")):
            raise TaskflowError("native quality has unresolved or conflicting responsibility disposition")
    material = {"candidate": candidate, "attempt": attempt, "manifest": manifest, "root": root,
            "report": report, "signoff": signed, "source_refs": refs,
            "certification": {"attempt_id": attempt_id, "cli_sid": attempt["cli_sid"],
                              "coverage": coverage, "report": refs["adoption"], "request": manifest["request"],
                              "manifest": _ref(root / "manifest.json"), "protocol_sha256": NATIVE_SIGNOFF_PROTOCOL_SHA256}}
    if prior_whole_findings:
        if coverage not in {"whole_package", "unchanged_scope"} or outcome != "revision_required":
            raise TaskflowError("prior whole findings cannot substitute another source scope or outcome")
        material["prior_whole_findings"] = _prior_whole_findings(candidate, attempt, manifest, root, refs, report)
        material["certification"]["prior_whole_findings"] = material["prior_whole_findings"]
    elif any(value["affirmed"] is False for value in dimensions.values()):
        material["candidate_acceptance"] = _accepted_observation_source(candidate, attempt, manifest, root, refs, report, coverage)
        material["certification"]["candidate_acceptance"] = material["candidate_acceptance"]
    return material


def _native_normative(material: dict) -> list[dict]:
    normative = material["signoff"].get("normative")
    if not isinstance(normative, dict) or normative.get("status") != "determined":
        raise TaskflowError("native normative scope needs the current independent signer's substantive choice")
    _signoff_citations(normative.get("evidence"), material["candidate"], material["manifest"], material["root"])
    _signoff_strings(normative.get("limitations"), "normative limitations")
    files = {item["package_path"]: item for item in material["candidate"]["files"]}
    result = []
    rows = normative.get("files")
    if not isinstance(rows, list) or not rows:
        raise TaskflowError("native normative scope must list exact current frozen references")
    for row in rows:
        if not isinstance(row, dict):
            raise TaskflowError("native normative source must be an object")
        path = relative_path(row.get("package_path"))
        source = files.get(path)
        if (source is None or source["role"] in PUBLIC_ROLES or row.get("role") != source["role"]
                or row.get("purpose") not in ("answer", "rubric", "reference", "legitimate_alternative")):
            raise TaskflowError("native normative selection is not an explicit current private reference")
        info = _info(Path(source["path"]))
        visible = [item for item in material["manifest"]["inputs"] if item["source"] == source["path"]
                   and item["target"] == path and item["kind"] == "teacher"
                   and all(item[key] == info[key] for key in ("sha256", "size"))]
        if (row.get("sha256") != source["sha256"] or info != {key: row.get(key) for key in ("sha256", "size")}
                or len(visible) != 1):
            raise TaskflowError("native normative selection SHA/size differs from the current candidate")
        result.append({"path": source["path"], "package_path": path, "role": source["role"], **info,
                       "purpose": row["purpose"], "basis": _signoff_text(row.get("basis"), "normative source basis")})
    if len({row["package_path"] for row in result}) != len(result) or not {"answer", "rubric"} <= {row["role"] for row in result}:
        raise TaskflowError("native normative selection must contain unique actual answer and rubric sources")
    return sorted(result, key=lambda row: row["package_path"])


@_handoff_errors
def native_quality_handoff(store: object, *, candidate_id: str, authorization: dict,
                           source_flow_id: str, packet_transport: dict | None = None) -> dict:
    """Read the same role's locked native signature; never select standards by prose."""
    candidate = store.get_candidate(candidate_id)
    selector = {"task_id": candidate["task_id"], "source_author_quality_flow": source_flow_id}
    authority = _projection_authority(candidate, {"authorization": authorization, "authorization_item": selector})
    item = next(row for row in authority["items"] if all(row.get(key) == value for key, value in selector.items()))
    parent = store.get_flow(source_flow_id)
    definition, state = parent["definition"], parent["state"]
    if item.get("source_author_flow_kind") not in (None, definition.get("kind")):
        raise TaskflowError("native authority source kind differs from the selected lifecycle")
    result = state.get("result", {})
    source_author = store.get_attempt(item["source_author_attempt"])
    author_binding = definition.get("source_author_binding", {})
    baseline_witness = None
    if definition.get("kind") == "baseline_quality_v1":
        from taskflow.execution_baseline_quality import baseline_experiment_source

        bound = baseline_experiment_source(store, {"definition": {
            "source_author_flow_id": source_flow_id, "task_id": candidate["task_id"],
            "owner": authority["controller"], "policy_id": definition["policy_id"]}, "state": {}},
            item, require_candidate=True)
        baseline_witness = bound["binding"]["baseline_source"]
    if (definition.get("kind") not in {"author_quality_v1", "baseline_quality_v1"} or definition.get("task_id") != candidate["task_id"]
            or definition.get("owner") != authority["controller"] or state.get("status") != "accepted"
            or state.get("pending_business_scope")
            or result.get("outcome") != "candidate_quality_accepted" or result.get("candidate_id") != candidate_id
            or state.get("child_flow_id") != result.get("child_flow_id")
            or definition.get("source_author_attempt") != item["source_author_attempt"]
            or author_binding.get("intent_id") != item["source_author_intent"]
            or any(source_author.get(key) != author_binding.get(key)
                   for key in ("task_id", "candidate_id", "intent_id", "work_id"))
            or source_author.get("state") != "completed" or source_author.get("cli_sid") != item["source_author_sid"]
            or (packet_transport is None and (
                definition.get("authorization", {}).get("path") != item["source_business_packet"]
                or definition.get("authorization", {}).get("sha256") != item["source_business_packet_sha256"]))):
        raise TaskflowError("native quality authority does not bind this actual accepted author parent")
    _checked_ref(definition["authorization"])
    packet_witness = None
    if packet_transport is not None:
        from taskflow.execution_author_packet import validate_author_packet_transport

        packet_witness = validate_author_packet_transport(packet_transport=packet_transport,
            authorization=authorization, source_flow_id=source_flow_id, store=store, require_complete=True)
    child = store.get_flow(result["child_flow_id"])
    child_state = child["state"]
    receipt_path = Path(result["child_receipt"])
    receipt = _read(receipt_path)
    if (child_state.get("status") != "accepted" or child_state.get("pending_business_scope")
            or child_state.get("candidate_id") != candidate_id
            or child["definition"].get("owner") != authority["controller"]
            or child["definition"].get("task_id") != candidate["task_id"]
            or child_state.get("candidate_root") != result.get("candidate_root")
            or child_state.get("accepted_review_id") != result.get("accepted_review_id")
            or child_state.get("receipt") != str(receipt_path)
            or receipt.get("flow_id") != child["flow_id"] or receipt.get("status") != "accepted"
            or receipt.get("candidate_id") != candidate_id or receipt.get("controller") != authority["controller"]
            or receipt.get("history") != child_state.get("history")):
        raise TaskflowError("native quality parent/child receipt or current candidate differs")
    root = Path(result["candidate_root"])
    declarations = [row for row in candidate.get("evidence", []) if row.get("location") == "explicit package declaration"]
    if (len(declarations) != 1 or declarations[0]["path"] != str(root / "task.json")
            or any(Path(row["path"]) != root / row["package_path"] for row in candidate["files"])):
        raise TaskflowError("native quality receipt candidate root is not its captured package")
    _checked_ref(declarations[0])
    store.assert_review_current(result["accepted_review_id"])
    accepted = store.quality_gate(candidate_id)
    graph = accepted.get("composite_quality_coverage")
    supplemental, repaired = None, None
    if graph:
        if graph["recheck_id"] != result["accepted_review_id"]:
            raise TaskflowError("native composite quality does not use the actual child recheck")
        old_candidate = store.get_candidate(graph["whole_source"]["candidate_id"])
        _, whole_raw = _checked_ref(graph["whole_source"]["report"])
        original_signoff = parse_json(whole_raw, "original whole quality source").get("native_signoff", {})
        coverage = original_signoff.get("coverage")
        finding_mode = coverage == "whole_package" or (coverage == "unchanged_scope" and any(
            dimension.get("affirmed") is False for dimension in original_signoff.get("dimensions", {}).values()))
        whole = _native_quality_source(store, old_candidate, graph["whole_source"]["attempt_id"],
                                       coverage=coverage, outcome="revision_required",
                                       prior_whole_findings=finding_mode)
        fresh = _native_quality_source(store, candidate, graph["affected_source"]["attempt_id"],
                                       coverage="affected", outcome="accepted")
        for material, field in ((whole, "whole_source"), (fresh, "affected_source")):
            original = graph[field]
            report_ref = material["source_refs"]["adoption"]
            if (original["sid"] != material["attempt"]["cli_sid"]
                    or any(original["report"].get(key) != report_ref[key] for key in ("path", "sha256"))):
                raise TaskflowError("native composite source differs from the actual signed coverage graph")
            if finding_mode and any(original.get("issues", {}).get(key) != material["source_refs"]["issues"][key]
                                    for key in ("path", "sha256")):
                raise TaskflowError("native prior-findings issues differ from the canonical signed coverage graph")
        excluded = set(whole["signoff"]["affected_paths"]) | {row["path"] for row in graph["private_changes"]}
        if not excluded <= set(fresh["signoff"]["affected_paths"]):
            raise TaskflowError("native affected signature leaves an excluded or changed association uncovered")
        if finding_mode:
            repaired = _repaired_candidate_source(candidate, accepted, graph, whole, fresh)
        sources = {"whole_" + name: ref for name, ref in whole["source_refs"].items()}
        sources.update(fresh["source_refs"])
        supplemental = _supplement_quality_source(store, candidate, graph)
        if supplemental is not None:
            sources.update(supplemental["source_refs"])
    else:
        if accepted["review_id"] != result["accepted_review_id"] or accepted.get("whole_package") is not True:
            raise TaskflowError("native quality requires exact whole acceptance or explicit repaired coverage")
        fresh = _native_quality_source(store, candidate, accepted["attempt_id"], coverage="whole_package", outcome="accepted")
        whole, sources = None, fresh["source_refs"]
    history = [row for row in child_state["history"] if row.get("kind") == "independent_review"
               and row.get("review_id") == result["accepted_review_id"]]
    if (len(history) != 1 or history[0].get("attempt_id") != fresh["attempt"]["attempt_id"]
            or history[0].get("candidate_id") != candidate_id or history[0].get("cli_sid") != fresh["attempt"]["cli_sid"]
            or history[0].get("outcome") != "accepted"):
        raise TaskflowError("native quality receipt does not identify the actual current independent signature")
    normative = _native_normative(fresh)
    dimensions = {}
    for kind in ("business", "artifact", "calculation"):
        base = "/native_signoff/dimensions/" + kind
        affirmed = [{"source": "adoption", "pointer": base + "/affirmed",
                     "equals": fresh["signoff"]["dimensions"][kind]["affirmed"]}]
        evidence = [{"source": "adoption", "pointer": base + "/evidence", "nonempty": True}]
        limits = [{"source": "adoption", "pointer": base + "/limitations"}]
        if whole:
            affirmed += [{"source": "whole_adoption", "pointer": base + "/affirmed", "equals": whole["signoff"]["dimensions"][kind]["affirmed"]},
                         {"source": "adoption", "pointer": "/execution_routing/outcome", "equals": "accepted"}]
            evidence.append({"source": "whole_adoption", "pointer": base + "/evidence", "nonempty": True})
            limits.append({"source": "whole_adoption", "pointer": base + "/limitations"})
        if supplemental is not None:
            affirmed.append({"source": "supplement_adoption", "pointer": base + "/affirmed", "equals": True})
            evidence.append({"source": "supplement_adoption", "pointer": base + "/evidence", "nonempty": True})
            limits.append({"source": "supplement_adoption", "pointer": base + "/limitations"})
        if repaired is not None:
            affirmed += [{"source": "whole_adoption", "pointer": "/execution_routing/outcome", "equals": "revision_required"},
                         {"source": "whole_adoption", "pointer": "/execution_routing/blocking_issue_ids", "equals": repaired["prior_whole_findings"]["blocking_issue_ids"]},
                         {"source": "adoption", "pointer": "/execution_routing/covered_defect_ids", "equals": fresh["report"]["execution_routing"]["covered_defect_ids"]}]
        dimensions[kind] = {"affirmations": affirmed, "evidence": evidence, "limitations": limits}
    protocol = {"authorization": authorization, "authorization_item": selector,
                "native_signoff": {"source_flow_id": source_flow_id,
                                    "protocol_sha256": fresh["signoff"]["protocol_sha256"]},
                "scope": {"source": "whole_adoption" if whole else "adoption",
                          "pointer": "/execution_routing/scope", "equals": "whole_package"},
                "dimensions": dimensions}
    if packet_witness is not None:
        protocol["native_signoff"]["packet_transport"] = packet_transport
    if fresh.get("candidate_acceptance") is not None:
        protocol["candidate_acceptance"] = fresh["candidate_acceptance"]
    if repaired is not None:
        protocol["repaired_candidate_acceptance"] = repaired
    certification = {"parent_flow_id": source_flow_id, "child_flow_id": child["flow_id"],
                     "child_receipt": _ref(receipt_path), "accepted_review_id": accepted["review_id"],
                     "fresh": fresh["certification"]}
    if baseline_witness is not None:
        certification["baseline_source"] = baseline_witness
        protocol["native_signoff"]["baseline_source"] = baseline_witness
    if packet_witness is not None:
        certification["author_packet_transport"] = packet_witness
    if whole:
        certification["whole"] = whole["certification"]
        certification["composite_quality_coverage"] = graph
    if repaired is not None:
        certification["repaired_candidate_acceptance"] = repaired
    if supplemental is not None:
        certification["supplement"] = supplemental["certification"]
    return {"source_refs": sources, "protocol_map": protocol,
            "scoring_source_allowlist": [row["package_path"] for row in normative],
            "normative_sources": normative, "certification": certification}


def _repaired_certificate_sources(native: dict, candidate: dict, material: dict) -> None:
    """Check frozen source transport offline; online admission still uses the gate."""
    repaired = native["protocol_map"].get("repaired_candidate_acceptance")
    certified = native["certification"].get("repaired_candidate_acceptance")
    if repaired is None and certified is None:
        return
    graph = native["certification"].get("composite_quality_coverage")
    if (repaired != certified or not isinstance(repaired, dict) or not isinstance(graph, dict)
            or repaired.get("composite_graph_sha256") != sha256(canonical(graph))
            or repaired.get("candidate_binding") != _binding(candidate)
            or repaired.get("accepted_review_id") != native["certification"]["accepted_review_id"]):
        raise TaskflowError("repaired normative certificate differs from its exact accepted graph")
    prior = repaired["prior_whole_findings"]
    whole = native["certification"]["whole"]
    manifest_path, _ = _checked_ref(whole["manifest"])
    root = manifest_path.parent
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    batch_source = _batch_quality_source(None, manifest, None, coverage=whole.get("coverage"), offline=True)
    if (whole.get("prior_whole_findings") != prior or whole.get("coverage") != prior.get("coverage")
            or manifest.get("role") != "quality" or manifest.get("mode") != "native_role"
            or manifest.get("nature") != "real_cli_attempt"
            or (not batch_source and (manifest.get("model", {}).get("requested") != "gpt-6.1-sol"
                                     or manifest.get("model", {}).get("reasoning_effort") != "xhigh"))
            or manifest.get("candidate_binding", {}).get("candidate_id") != graph["whole_source"]["candidate_id"]
            or collection.get("sid") != whole.get("cli_sid") or whole["cli_sid"] != graph["whole_source"]["sid"]
            or collection.get("technical_accepted") is not True or collection.get("process_ended") is not True):
        raise TaskflowError("repaired normative certificate replaced its actual prior whole source")
    for key, filename in (("report", "adoption-readiness.json"), ("issues", "issues.json")):
        actual = _ref(root / "artifacts" / filename)
        if (prior[key] != actual or collection["artifacts"].get(filename) != {k: actual[k] for k in ("sha256", "size")}
                or any(graph["whole_source"][key].get(k) != actual[k] for k in ("path", "sha256"))):
            raise TaskflowError("repaired normative certificate has foreign original findings")
    report = _read(Path(prior["report"]["path"]))
    issues = _read(Path(prior["issues"]["path"]))
    expected_prior = {"schema_version": 1, "kind": "original_whole_package_findings", "scope": "whole_package",
                      "coverage": report["native_signoff"]["coverage"], "outcome": "revision_required",
                      "blocking_issue_ids": report["execution_routing"]["blocking_issue_ids"],
                      "report": prior["report"], "issues": prior["issues"],
                      "original_dimensions": report["native_signoff"]["dimensions"],
                      "original_issues": [{"reference": prior["issues"], "pointer": f"/issues/{index}", "actual_value": row}
                                          for index, row in enumerate(issues["issues"])],
                      "required_associations": sorted({path for row in issues["issues"]
                          if row["id"] in report["execution_routing"]["blocking_issue_ids"] for path in row["affected_paths"]})}
    expected_prior.update(_prior_native_scope(report["native_signoff"], expected_prior["required_associations"],
                                              expected_prior["blocking_issue_ids"]))
    expected_prior.update(_whole_finding_locations(report, expected_prior["required_associations"], manifest, root,
        {"adoption": prior["report"], "issues": prior["issues"]}, issues["issues"]))
    if (report["execution_routing"].get("scope") != "whole_package"
            or report["execution_routing"].get("outcome") != "revision_required"
            or report["execution_routing"].get("blocking_issue_ids") != prior["blocking_issue_ids"]
            or report["native_signoff"].get("coverage") != whole["coverage"]
            or report["native_signoff"].get("dimensions") != prior["original_dimensions"]
            or prior != expected_prior):
        raise TaskflowError("repaired normative certificate altered prior verdicts or false dimensions")
    expected = {"report": material["source_refs"]["adoption"], "issues": material["source_refs"]["issues"],
                "coverage": "affected", "scope": "affected", "outcome": "accepted",
                "covered_defect_ids": material["report"]["execution_routing"]["covered_defect_ids"],
                "original_dimensions": material["signoff"]["dimensions"]}
    if repaired["current_affected_closure"] != expected:
        raise TaskflowError("repaired normative certificate altered the fresh affected closure")
    expected_repaired = _repaired_candidate_source(candidate,
        {"review_id": native["certification"]["accepted_review_id"], "attempt_id": material["attempt"]["attempt_id"],
         "whole_package": False, "snapshot_bound_acceptance": True}, graph,
        {"prior_whole_findings": expected_prior}, material)
    if repaired != expected_repaired:
        raise TaskflowError("repaired normative certificate changed its source or closure projection")


@_handoff_errors
def validate_normative_certification(*, candidate: dict, certification: dict,
                                     store: object | None = None) -> list[dict]:
    """Read an explicit native selection; never infer standards from file labels."""
    fields = {"schema_version", "kind", "candidate_binding", "source_author_flow_id", "authorization", "native_quality_handoff"}
    if (not isinstance(certification, dict) or set(certification) != fields
            or type(certification["schema_version"]) is not int or certification["schema_version"] != 1
            or certification["kind"] != "native_quality_handoff"
            or certification["candidate_binding"] != _binding(candidate)):
        raise TaskflowError("normative certification needs the exact typed current-candidate native handoff")
    flow_id = validate_id(certification["source_author_flow_id"], "source author flow ID")
    native = certification["native_quality_handoff"]
    if not isinstance(native, dict) or set(native) != {
            "source_refs", "protocol_map", "scoring_source_allowlist", "normative_sources", "certification"}:
        raise TaskflowError("normative certification lacks its complete native handoff")
    if store is not None:
        expected = native_quality_handoff(store, candidate_id=candidate["candidate_id"],
                                          authorization=certification["authorization"], source_flow_id=flow_id,
                                          packet_transport=native.get("protocol_map", {}).get("native_signoff", {}).get("packet_transport"))
        if canonical(expected) != canonical(native):
            raise TaskflowError("normative certification differs from the current stored native handoff")
    protocol = native["protocol_map"]
    expected_item = {"task_id": candidate["task_id"], "source_author_quality_flow": flow_id}
    if (protocol.get("authorization") != certification["authorization"]
            or protocol.get("authorization_item") != expected_item
            or protocol.get("native_signoff", {}).get("source_flow_id") != flow_id
            or native["certification"].get("parent_flow_id") != flow_id):
        raise TaskflowError("normative certification has a different authority or source-flow item")
    authority = _projection_authority(candidate, protocol)
    packet_transport = protocol.get("native_signoff", {}).get("packet_transport")
    if packet_transport is not None:
        from taskflow.execution_author_packet import validate_author_packet_transport

        witness = validate_author_packet_transport(packet_transport=packet_transport,
            authorization=certification["authorization"], source_flow_id=flow_id,
            store=store, require_complete=True)
        if native["certification"].get("author_packet_transport") != witness:
            raise TaskflowError("normative certification author packet transport differs from its actual source")
    elif native["certification"].get("author_packet_transport") is not None:
        raise TaskflowError("normative certification has no explicit author packet transport")
    fresh = native["certification"]["fresh"]
    manifest_path, _ = _checked_ref(fresh["manifest"])
    if manifest_path.name != "manifest.json":
        raise TaskflowError("normative certification must name the actual source manifest")
    root = _destination(manifest_path.parent)
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    sid = collected.get("sid")
    trace = _trace(root / "trace.jsonl", sid)
    termination = _read(root / "termination.json")
    if (not all(collected.get(key) is True for key in ("technical_accepted", "process_ended", "delivery_complete", "model_completed"))
            or not trace["model_completed"] or not sid or trace["sid"] != sid
            or collected.get("termination") != termination
            or type(termination.get("returncode")) is not int or termination["returncode"] != 0
            or fresh.get("cli_sid") != sid or manifest.get("candidate_binding") != _binding(candidate)
            or fresh.get("coverage") not in {"whole_package", "affected"}):
        raise TaskflowError("normative certification has no completed actual same-candidate source/trace")
    item = next(row for row in authority["items"] if isinstance(row, dict)
                and all(row.get(key) == value for key, value in expected_item.items()))
    if item.get("source_author_flow_kind") == "baseline_quality_v1":
        from taskflow.execution_baseline_quality import baseline_flow_id, validate_frozen_baseline_source

        proof = native["certification"].get("baseline_source")
        if (protocol.get("native_signoff", {}).get("baseline_source") != proof
                or item.get("source_baseline_binding") != (proof or {}).get("baseline_binding")
                or item.get("source_author_attempt") != (proof or {}).get("source_author_attempt")
                or item.get("source_author_sid") != (proof or {}).get("source_author_sid")
                or flow_id != baseline_flow_id((proof or {}).get("source_flow_id", ""))):
            raise TaskflowError("baseline normative source lacks its original research-ended provenance")
        validate_frozen_baseline_source(proof)
    elif native["certification"].get("baseline_source") is not None:
        raise TaskflowError("baseline source proof requires its explicit typed authority")
    if sid == item.get("source_author_sid") or sid in candidate.get("external_author_sids", []):
        raise TaskflowError("normative source reused a captured author identity")
    identity = validate_id(fresh["attempt_id"], "native source attempt ID")
    observed_attempt = {"attempt_id": identity, "state": "completed", "role": "quality",
                        "candidate_id": candidate["candidate_id"], "runtime_path": str(root),
                        "intent_id": manifest["intent_id"], "cli_sid": sid}

    class SealedSource:
        def get_attempt(self, attempt_id: str) -> dict:
            if attempt_id != identity:
                raise TaskflowError("normative source adapter cannot substitute another attempt")
            return observed_attempt

    material = _native_quality_source(SealedSource(), candidate, identity, coverage=fresh["coverage"], outcome="accepted", offline=True)
    if (material["certification"] != fresh
            or any(native["source_refs"].get(key) != value for key, value in material["source_refs"].items())):
        raise TaskflowError("normative certification source refs differ from their actual sealed signature")
    if material.get("candidate_acceptance") is not None:
        if protocol.get("candidate_acceptance") != material["candidate_acceptance"]:
            raise TaskflowError("normative certification changed its original false affirmation or nonblocking observations")
        for kind, dimension in material["signoff"]["dimensions"].items():
            selector = {"source": "adoption", "pointer": "/native_signoff/dimensions/" + kind + "/affirmed",
                        "equals": dimension["affirmed"]}
            if not protocol.get("dimensions", {}).get(kind, {}).get("affirmations", []) or protocol["dimensions"][kind]["affirmations"][0] != selector:
                raise TaskflowError("normative certification changed an original dimension selector")
    elif protocol.get("candidate_acceptance") is not None:
        raise TaskflowError("normative certification invented a candidate observation acceptance")
    _repaired_certificate_sources(native, candidate, material)
    selected = _native_normative(material)
    if (canonical(selected) != canonical(native["normative_sources"])
            or native["scoring_source_allowlist"] != sorted(row["package_path"] for row in selected)):
        raise TaskflowError("normative certification selection differs from the actual independent signature")
    return selected


@_handoff_errors
def projected_quality_reports(store: object, *, candidate_id: str, source_refs: dict,
                              protocol_map: dict, destination: Path) -> dict[str, Path]:
    """Preserve actual signed acceptance and scoped original dimension evidence."""
    candidate = store.get_candidate(candidate_id)
    reports = _projection_data(store, candidate, source_refs, protocol_map)
    destination = _destination(destination)
    for item in source_refs.values():
        source = Path(item["path"]).absolute()
        if destination.is_relative_to(source.parent) or source.is_relative_to(destination):
            raise TaskflowError("quality projections must be outside original signed source directories")
    destination.mkdir(parents=True, exist_ok=True)
    result = {}
    for kind, report in reports.items():
        validate_id(kind, "review kind")
        path = destination / (kind + ".json")
        _once(path, report)
        result[kind] = path
    return result


def _source_report(path: Path, spec: dict, candidate: dict, accepted: dict,
                   collected: set[str], store: object) -> tuple[bytes, dict]:
    raw = _regular(path)
    report = parse_json(raw, str(path))
    projection = report.get("derived_quality_projection")
    if projection is not None:
        expected = _projection_data(store, candidate, projection["sources"], projection["protocol_map"])
        if report != expected.get(spec["kind"]):
            raise TaskflowError("derived quality report no longer matches its actual signed source chain: " + spec["id"])
    if report.get("nature") == "test_fixture" or report.get("test_fixture") is True:
        raise TaskflowError("required release report is a fixture: " + spec["id"])
    if report.get("status") != "passed":
        raise TaskflowError("required release report has no explicit passed status: " + spec["id"])
    binding = report.get("candidate_binding", report.get("execution_binding", report))
    if any(binding.get(key) != value for key, value in _binding(candidate).items()):
        raise TaskflowError("required release report is not bound to the current candidate: " + spec["id"])
    composite = (projection is not None and accepted.get("composite_quality_coverage")
                 and projection.get("composite_quality_coverage") == accepted["composite_quality_coverage"]
                 and report.get("scope") == "explicit_repaired_quality_coverage" and report.get("whole_package") is False)
    if report.get("scope") != "whole_package" and not composite:
        raise TaskflowError("required release report lacks whole_package scope; affected is not promoted: " + spec["id"])
    kinds = report.get("covered_review_kinds", [report.get("kind")])
    if not isinstance(kinds, list) or spec["kind"] not in kinds:
        raise TaskflowError("required release report does not sign the declared review kind: " + spec["kind"])
    if not isinstance(report.get("limitations"), list) or not report.get("evidence"):
        raise TaskflowError("required release report lacks source evidence/limitations: " + spec["id"])
    if (projection is None and sha256(raw) not in collected
            and not any(item.get("sha256") == sha256(raw) for item in accepted.get("evidence", []))):
        raise TaskflowError("required release report is not evidence of the current accepted review: " + spec["id"])
    return raw, report


def _released(destination: Path, version: str, identity: dict) -> dict:
    receipt = _read(destination / "release-receipt.json")
    if receipt.get("binding") != identity:
        raise TaskflowError("existing release has a different candidate/source/report binding")
    for item in receipt["quality_evidence"]:
        _checked_ref(item)
    snapshot = destination / "versions" / version
    frozen = verify_snapshot(snapshot)
    if any(frozen[key] != identity["candidate_binding"][key] for key in
           ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("released snapshot differs from the accepted candidate")
    exports = {}
    for audience in ("solver", "evaluator"):
        path = destination / "exports" / (version + "-" + audience)
        export = verify_export(path)
        selected = [{**{key: item[key] for key in ("id", "path", "role", "sha256", "size")},
                     **({"empty_content": item["empty_content"]}
                        if audience == "evaluator" and "empty_content" in item else {})}
                    for item in frozen["files"] if audience == "evaluator" or item["role"] in PUBLIC_ROLES]
        if (export["input_digest"] != frozen["input_digest"]
                or export["task_id"] != frozen["task_id"] or export["version_id"] != version
                or export.get("audience") != audience or export.get("files") != selected):
            raise TaskflowError("released export differs from its frozen candidate")
        if audience == "evaluator" and any(export.get(key) != frozen[key] for key in
                                           ("scoring", "evaluation_digest", "snapshot_digest", "sources", "project")):
            raise TaskflowError("released evaluator private/scoring/snapshot binding differs")
        exports[audience] = {**export, "path": str(path)}
    return {"schema_version": 1, "candidate_binding": identity["candidate_binding"],
            "publication_root": str(destination), "snapshot": {**frozen, "path": str(snapshot)},
            **exports, "quality_evidence": receipt["quality_evidence"],
            "receipt": _ref(destination / "release-receipt.json")}


@_handoff_errors
def prepare_release(store: object, *, candidate_id: str, source_root: Path,
                    destination: Path, version: str, quality_reports: dict[str, Path]) -> dict:
    """Publish a detached exact candidate; preserve existing signed originals."""
    from taskflow.execution import release_candidate

    validate_id(version, "version")
    candidate = store.get_candidate(candidate_id)
    accepted = store.quality_gate(candidate_id)
    source_root, destination = Path(source_root).absolute(), _destination(destination)
    if (destination == source_root or destination.is_relative_to(source_root)
            or source_root.is_relative_to(destination) or destination.name != candidate["task_id"]):
        raise TaskflowError("release needs a separate publication root named for the task")
    actual = check_task(source_root, write_report=False)
    if actual["errors"] or any(actual[key] != candidate[key] for key in
                               ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("release source differs from the current registered candidate")
    declaration_raw = _regular(source_root / "task.json")
    declaration = parse_json(declaration_raw, "candidate task.json")
    required = {item["id"]: item for item in declaration["reviews"] if item["required"]}
    if set(quality_reports) != set(required):
        raise TaskflowError("quality_reports must cover exactly the required report IDs: " + ", ".join(required))
    collected = set()
    if accepted.get("attempt_id"):
        quality_attempt = store.get_attempt(accepted["attempt_id"])
        quality_root = Path(quality_attempt["runtime_path"])
        quality_manifest = _read(quality_root / "manifest.json")
        if (quality_attempt.get("state") != "completed" or quality_attempt.get("candidate_id") != candidate_id
                or quality_manifest.get("nature") != "real_cli_attempt"):
            raise TaskflowError("release quality source is not a completed real current-candidate review")
        collected = {item["sha256"] for item in _validated_collection(quality_root, quality_manifest)["artifacts"].values()}
    reports = {name: _source_report(Path(quality_reports[name]), spec, candidate, accepted, collected, store)
               for name, spec in required.items()}
    identity = {"candidate_binding": _binding(candidate), "source_root": str(source_root), "version": version,
                "task_json": {"sha256": sha256(declaration_raw), "size": len(declaration_raw)},
                "accepted_review_id": accepted["review_id"],
                "reports": {name: _ref(Path(quality_reports[name])) for name in required}}
    if destination.exists():
        return _released(destination, version, identity)
    with atomic_directory(destination) as stage:
        (stage / "task.json").write_bytes(declaration_raw)
        captured = {item["package_path"]: item for item in candidate["files"]}
        for item in declaration["files"]:
            name = relative_path(item["path"])
            raw = _regular(source_root / name, limit=128 * 1024 * 1024)
            if name not in captured or sha256(raw) != captured[name]["sha256"]:
                raise TaskflowError("declared release file differs from its captured candidate: " + name)
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        evidence = []
        for name, spec in required.items():
            raw, report = reports[name]
            original = Path("release-sources") / (name + ".json")
            (stage / original).parent.mkdir(parents=True, exist_ok=True)
            (stage / original).write_bytes(raw)
            projection = {"schema_version": 1, "review_id": name, "kind": spec["kind"], "status": "passed",
                          **{key: candidate[key] for key in ("content_digest", "input_digest", "evaluation_digest")},
                          "scope": report["scope"], "limitations": report["limitations"],
                          "evidence": report["evidence"], "original_report": report,
                          "signature_authority": identity["reports"][name],
                          "mechanical_projection": {"accepted_review_id": accepted["review_id"],
                                                    "candidate_binding": _binding(candidate),
                                                    "original_copy": str(destination / original)}}
            if report.get("candidate_acceptance") is not None:
                for field in ("status_basis", "candidate_acceptance", "original_dimension", "nonblocking_observations", "interpretation"):
                    projection[field] = report[field]
            if report.get("repaired_candidate_acceptance") is not None:
                for field in ("status_basis", "repaired_candidate_acceptance", "prior_dimension", "current_dimension", "interpretation"):
                    projection[field] = report[field]
            if accepted.get("composite_quality_coverage"):
                projection.update(whole_package=False, composite_quality_coverage=accepted["composite_quality_coverage"])
            target = stage / relative_path(spec["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            write_json(target, projection)
            evidence.append({"path": str(destination / original), "sha256": sha256(raw), "size": len(raw)})
        release_candidate(store, stage, candidate_id, version)
        after = check_task(source_root, write_report=False)
        if any(after[key] != candidate[key] for key in ("content_digest", "input_digest", "evaluation_digest")):
            raise TaskflowError("candidate changed during detached publication")
        for name in required:
            _checked_ref(identity["reports"][name])
        write_json(stage / "release-receipt.json", {"schema_version": 1, "binding": identity,
                                                  "quality_evidence": evidence,
                                                  "scope": "Mechanical transport of existing signed acceptance; no new business judgment."})
    return _released(destination, version, identity)


def _native_checks(runtime: Path, plan: dict, python: str) -> dict:
    """Exercise default Node/OpenSSL, denial, own writes, fork IPC and SQLite."""
    spec = importlib.util.spec_from_file_location("taskflow_formal_native_gate", runtime / "runtime_gate.py")
    if spec is None or spec.loader is None:
        raise TaskflowError("copied formal runtime gate is unavailable")
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    environments = [part for part in plan["argv"] if part.startswith("shell_environment_policy.set=")]
    paths = [match[1] for part in environments
             if (match := re.search(r'"?PATH"?\s*=\s*("(?:\\.|[^"\\])*")', part))]
    nodes = [Path(item) / "node" for encoded in paths for item in json.loads(encoded).split(":")
             if item.endswith("/node/bin")]
    if len(nodes) != 1:
        raise TaskflowError("formal baseline must bind one bundled default Node directory")
    node = nodes[0]
    scratch = Path(plan["root"]) / "solver-scratch"
    package = _read(Path(plan["cwd"]) / "package.json")
    member = next((item for item in package["files"] if item["role"] in PUBLIC_ROLES), None)
    if not member:
        raise TaskflowError("formal native probe needs an actual public member")
    token = "formal-native-" + uuid.uuid4().hex
    parameters = {"public": str(Path(plan["cwd"]) / member["path"]),
                  "private": str(Path(plan["private_directory"]) / "answer.md"),
                  "scratch": str(scratch), "token": token}
    code = r'''
const fs=require('node:fs'), crypto=require('node:crypto'), cp=require('node:child_process');
const {DatabaseSync}=require('node:sqlite'); const p=PARAMETERS;
const child=p.scratch+'/'+p.token+'.cjs', dbfile=p.scratch+'/'+p.token+'.sqlite';
let denied=false,errno=null;try{fs.readFileSync(p.private)}catch(e){denied=true;errno=e.code}
const result={node:process.version,openssl_conf:process.env.OPENSSL_CONF??null,private_denied:denied,deny_code:errno,
 public_sha256:crypto.createHash('sha256').update(fs.readFileSync(p.public)).digest('hex'),own_write:false,fork_ipc:false,sqlite:false};
(async()=>{try{
 const db=new DatabaseSync(dbfile);db.exec('CREATE TABLE fixture(value TEXT);INSERT INTO fixture VALUES (\'NEUTRAL\')');db.close();
 fs.writeFileSync(child,"const {DatabaseSync}=require('node:sqlite');const db=new DatabaseSync(process.argv[2]);const row=db.prepare('SELECT value FROM fixture').get();db.close();process.send({value:row.value});process.disconnect();");
 result.own_write=fs.readFileSync(child,'utf8').length>0;
 const value=await new Promise((resolve,reject)=>{const c=cp.fork(child,[dbfile],{execPath:process.execPath,silent:true});
  const timer=setTimeout(()=>{c.kill();reject(Error('fork timeout'))},5000);let message;
  c.on('message',m=>{message=m});c.on('error',reject);c.on('exit',status=>{clearTimeout(timer);status===0?resolve(message):reject(Error('fork exit '+status))});});
 result.fork_ipc=value?.value==='NEUTRAL';result.sqlite=result.fork_ipc;
 }finally{for(const file of [child,dbfile,dbfile+'-wal',dbfile+'-shm']){try{fs.unlinkSync(file)}catch(e){if(e.code!=='ENOENT')throw e}}}
 console.log(JSON.stringify(result));})().catch(e=>{console.error(e.stack);process.exitCode=1});
'''.replace("PARAMETERS", json.dumps(parameters))
    path = runtime / "normal-tools-probe.json"
    binding = {"plan_sha256": _info(runtime / "execution-plan.json")["sha256"],
               "node": _ref(node), "public_sha256": member["sha256"],
               "scope": "Current zero-model native profile; no provider identity or hidden payload claim."}
    if path.exists():
        report = _read(path)
        if report.get("binding") != binding:
            raise TaskflowError("existing formal native tool probe has different bound sources")
    else:
        argv = [*gate.shell_probe_base(plan), "--", "/usr/bin/env", "TMPDIR=" + str(scratch), str(node), "-e", code]
        process = subprocess.run(argv, cwd=plan["cwd"], capture_output=True, timeout=20, check=False)
        try:
            result = json.loads(process.stdout)
        except (ValueError, UnicodeError):
            result = {}
        report = {"binding": binding, "argv": argv, "returncode": process.returncode,
                  "stdout": process.stdout.decode(errors="replace"), "stderr": process.stderr.decode(errors="replace"),
                  "observed": result, "model_calls": 0, "external_model_requests": 0}
        _publish(path, report)
    result = report.get("observed", {})
    if (report.get("returncode") != 0 or "openssl_conf" not in result or result.get("openssl_conf") is not None
            or result.get("private_denied") is not True or result.get("deny_code") not in ("EPERM", "EACCES")
            or result.get("public_sha256") != member["sha256"]
            or any(result.get(key) is not True for key in ("own_write", "fork_ipc", "sqlite"))):
        raise TaskflowError("current formal default Node/private/own-write/fork/SQLite probe failed: " + str(path)
                            + " " + report.get("stderr", "")[-1500:] + " " + report.get("stdout", "")[-1500:])
    return _ref(path)


def validate_formal_authority(*, config: dict, candidate: dict) -> dict:
    """Resolve one hash-bound authority item without changing its launch gates."""
    path, raw = _checked_ref(config["authorization"])
    authority = parse_json(raw, str(path))
    if not isinstance(authority, dict):
        raise TaskflowError("formal authority must be an object")
    selector = config.get("authorization_item")
    if selector is not None:
        if (not isinstance(selector, dict)
                or set(selector) != {"task_id", "source_author_quality_flow"}
                or selector["task_id"] != candidate["task_id"]):
            raise TaskflowError("formal authority item must bind the exact task and source flow")
        items = authority.get("items")
        matches = [item for item in items if isinstance(item, dict)
                   and all(item.get(key) == value for key, value in selector.items())] if isinstance(items, list) else []
        if len(matches) != 1:
            raise TaskflowError("formal authority needs one exact task/source-flow item")
        item = matches[0]
        baseline = authority.get("formal_baseline", {})
        if not isinstance(baseline, dict):
            raise TaskflowError("formal baseline authority must be an object")
        authority = {**authority, "task_id": item["task_id"],
            "formal": {"first_run_id": item.get("first_run_id"),
                       "model": baseline.get("model_requested"),
                       "timeout_seconds": baseline.get("timeout_seconds"),
                       "baseline": {"path": baseline.get("path"), "sha256": baseline.get("sha256")}},
            "single_confirmation": {"run_id": item.get("conditional_one_confirmation_run_id")}}
    formal = authority.get("formal", {})
    if not isinstance(formal, dict) or not isinstance(authority.get("single_confirmation", {}), dict):
        raise TaskflowError("formal launch authority must contain typed launch fields")
    run_id = validate_id(config["run_id"], "run_id")
    allowed_runs = {formal.get("first_run_id"), authority.get("single_confirmation", {}).get("run_id")}
    if (authority.get("task_id") != candidate["task_id"] or run_id not in allowed_runs
            or not authority.get("controller") or formal.get("model") != config["model"]
            or formal.get("timeout_seconds") != config["timeout_seconds"]):
        raise TaskflowError("formal configuration is outside the explicit business packet")
    baseline = formal.get("baseline", {})
    baseline_sha = baseline.get("sha256") if isinstance(baseline, dict) else None
    if (not isinstance(baseline_sha, str) or len(baseline_sha) != 64
            or config["adapter"].get("files", {}).get("baseline-plan.json") != baseline_sha):
        raise TaskflowError("formal adapter baseline differs from authorized business packet")
    return authority


@_handoff_errors
def prepare_formal(*, config: dict, release: dict, candidate: dict, runtime: Path,
                   preparation_recovery: dict | None = None) -> dict:
    """Prepare and validate the existing formal adapter; never enqueue or execute."""
    runtime = _destination(runtime)
    recovered = (_validate_formal_preparation_recovery(config=config, release=release, candidate=candidate,
                 runtime=runtime, recovery=preparation_recovery, public_probe=True, allow_draft=True)
                 if preparation_recovery is not None else None)
    if release["candidate_binding"] != _binding(candidate):
        raise TaskflowError("formal release belongs to a different candidate")
    snapshot = Path(release["snapshot"]["path"])
    frozen = verify_snapshot(snapshot)
    if any(frozen[key] != candidate[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("formal frozen source differs from current candidate")
    adapter, _ = _checked_ref(config["adapter"])
    request, _ = _checked_ref(config["request"])
    authority = validate_formal_authority(config=config, candidate=candidate)
    authority_path = Path(config["authorization"]["path"])
    run_id = validate_id(config["run_id"], "run_id")
    files = config["adapter"].get("files", {})
    needed = {"runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json"}
    if set(files) != needed:
        raise TaskflowError("formal adapter requires the complete approved source/baseline hash set")
    for name, digest in files.items():
        _checked_ref({"path": str(adapter.parent / name), "sha256": digest})
    denied = [_checked_ref(item)[0] for item in config.get("deny_read_probes", [])]
    quality = [_checked_ref(item)[0] for item in release["quality_evidence"]]
    identity = {"config_digest": sha256(canonical(config)), "release_receipt": release["receipt"],
                "candidate_binding": _binding(candidate), "runtime": str(runtime)}
    intent = runtime.with_name(runtime.name + ".formal-intent.json")
    _once(intent, identity)
    python = str(Path(config["launcher_python"]).resolve())
    if not runtime.exists():
        argv = [python, str(adapter), "--solver", release["solver"]["path"],
                "--evaluation-directory", str(snapshot / "draft/evaluation"),
                "--run-id", run_id, "--request", str(request), "--runtime-directory", str(runtime),
                "--task-directory", release["publication_root"]]
        for path in quality:
            argv += ["--quality-evidence", str(path)]
        for path in denied:
            argv += ["--deny-read-probe", str(path)]
        result = subprocess.run(argv, capture_output=True, timeout=config.get("prepare_timeout_seconds", 300), check=False)
        if result.returncode:
            raise TaskflowError("existing formal preparation failed; original runtime/reservation retained: "
                                + result.stderr.decode(errors="replace")[-2000:])
    if not (runtime / "execution-plan.json").exists():
        raise TaskflowError("existing failed/incomplete formal preparation cannot be automatically repeated")
    plan = _read(runtime / "execution-plan.json")
    plan_info = _info(runtime / "execution-plan.json")
    if (_read(runtime / "plan-lock.json") != {"sha256": plan_info["sha256"]}
            or plan.get("run_id") != run_id or plan.get("requested_model") != config["model"]
            or plan.get("input_digest") != candidate["input_digest"]
            or plan.get("request_sha256") != config["request"]["sha256"]
            or plan.get("frozen_binding", {}).get("snapshot_digest") != frozen["snapshot_digest"]
            or plan.get("launcher_python") != python):
        raise TaskflowError("prepared formal plan belongs to different request/frozen/configuration bytes")
    for name in needed | {"prepare_diagnostic.py"}:
        expected = (recovered["source_change"]["executed"]["sha256"] if recovered is not None
                    else config["adapter"]["sha256"]) if name == "prepare_diagnostic.py" else files[name]
        if _info(runtime / name)["sha256"] != expected:
            raise TaskflowError("prepared formal source differs from its approved adapter: " + name)
    validation = subprocess.run([python, str(runtime / "execute_diagnostic.py")], capture_output=True, timeout=60, check=False)
    if validation.returncode:
        raise TaskflowError("existing formal validation failed: " + validation.stderr.decode(errors="replace")[-2000:])
    normal_tools = _native_checks(runtime, plan, python)
    # The business packet already authorizes this mechanical boundary. This is
    # an attributed program receipt, not a claim that the controller read it.
    acceptance = {"approved": True, "reviewer": "execution adapter on behalf of " + authority["controller"],
                  "plan_sha256": plan_info["sha256"], "input_digest": candidate["input_digest"],
                  "authorization": _ref(authority_path), "human_read_claimed": False,
                  "preparation_binding": identity, "normal_tools": normal_tools}
    _once(runtime / "launch-review.json", acceptance)
    runner = {"intent_id": config["intent_id"], "policy_version": config["policy_version"],
              "role": "solver", "mode": "existing_isolated", "model": config["model"],
              "reasoning_effort": config["reasoning_effort"],
              "reasoning_context": config.get("reasoning_context"),
              "reasoning_selection": config.get("reasoning_selection", "verified_client_default"),
              "timeout_seconds": config["timeout_seconds"], "cwd": plan["cwd"],
              "argv": [python, str(runtime / "execute_diagnostic.py"), "--execute"],
              "inputs": [], "request_sections": [], "outputs": config.get("outputs", []),
              "output_trees": config.get("output_trees", ["deliverables"]),
              "dependencies": [normal_tools["path"], _read(Path(normal_tools["path"]))["binding"]["node"]["path"]],
              "isolated": {"runtime": str(runtime), "flavor": "formal"}}
    return {"runner": runner, "release": release, "plan_path": str(runtime / "execution-plan.json"),
            "plan_sha256": plan_info["sha256"], "acceptance_path": str(runtime / "launch-review.json")}


def _recovery_reference(reference: dict, expected: Path | None = None) -> tuple[Path, bytes]:
    path = _destination(Path(reference["path"]))
    if expected is not None and path != expected:
        raise TaskflowError("formal recovery reference has a different controlled path")
    return _checked_ref(reference)


def _recovery_tree(path: Path, files: dict) -> None:
    tree = _full_tree(_destination(path))
    actual = {name: {key: row[key] for key in ("sha256", "size")} for name, row in tree["files"].items()}
    if tree["special"] or set(tree["directories"]) != {"."} or actual != files:
        raise TaskflowError("formal recovery original archive/retained membership or bytes drifted")


@_handoff_errors
def validate_formal_preparation_recovery(*, config: dict, release: dict, candidate: dict,
                                         runtime: Path, recovery: dict) -> dict:
    """Read the exact listener-EPERM recovery; never prepare, probe or launch."""
    return _validate_formal_preparation_recovery(config=config, release=release, candidate=candidate,
                                               runtime=runtime, recovery=recovery)


@_handoff_errors
def validate_formal_public_probe_recovery(*, config: dict, release: dict, candidate: dict,
                                          runtime: Path, recovery: dict) -> dict:
    """Validate an explicit same-run public-probe source correction and real probes."""
    return _validate_formal_preparation_recovery(config=config, release=release, candidate=candidate,
                                               runtime=runtime, recovery=recovery, public_probe=True)


def _public_probe_source_change(config: dict, runtime: Path, change: dict) -> dict:
    """Only replace the redundant public target; retain all other adapter bytes."""
    if (not isinstance(change, dict) or set(change) != {"kind", "original", "replacement", "executed"}
            or change["kind"] != "deterministic_public_probe_target_v1"):
        raise TaskflowError("formal public-probe recovery requires its exact source-change witness")
    original, raw = _recovery_reference(change["original"], Path(config["adapter"]["path"]))
    _checked_ref(config["adapter"])
    old = b"work / 'draft/input/prompt.md'"
    replacement = b"work / next(iter(plan['public_tree']))"
    try:
        parsed_source = ast.parse(raw)
    except SyntaxError as error:
        raise TaskflowError("formal public-probe original source is invalid") from error
    functions = [node for node in parsed_source.body if isinstance(node, ast.FunctionDef) and node.name == "shell_probes"]
    targets = [node.value for function in functions for node in function.body if isinstance(node, ast.Assign)
               and any(isinstance(target, ast.Name) and target.id == "targets" for target in node.targets)]
    first = (targets[0].elts[0].elts[1] if len(targets) == 1 and isinstance(targets[0], ast.List)
             and targets[0].elts and isinstance(targets[0].elts[0], ast.Tuple) and len(targets[0].elts[0].elts) == 4 else None)
    if (raw.count(old) != 1 or len(functions) != 1 or first is None
            or ast.get_source_segment(raw.decode("utf-8"), first) != old.decode("utf-8")):
        raise TaskflowError("formal public-probe source must have one original fixed first target expression")
    updated, updated_raw = _recovery_reference(change["replacement"])
    executed, executed_raw = _recovery_reference(change["executed"], runtime / "prepare_diagnostic.py")
    if (updated in (original, executed) or updated_raw != raw.replace(old, replacement)
            or executed_raw != updated_raw):
        raise TaskflowError("formal public-probe source differs beyond the one permitted target expression")
    for name, digest in config["adapter"]["files"].items():
        _checked_ref({"path": str(updated.parent / name), "sha256": digest})
    return {"prepare_diagnostic.py": sha256(updated_raw), **config["adapter"]["files"]}


def _validate_formal_preparation_recovery(*, config: dict, release: dict, candidate: dict,
                                         runtime: Path, recovery: dict, public_probe: bool = False,
                                         allow_draft: bool = False) -> dict:
    runtime = _destination(runtime)
    proof_path, raw = _recovery_reference(recovery)
    proof = parse_json(raw, str(proof_path))
    binding = {"config_digest": sha256(canonical(config)), "release_receipt": release["receipt"],
               "candidate_binding": _binding(candidate), "runtime": str(runtime)}
    run_id = validate_id(config["run_id"], "run_id")
    draft = (public_probe and allow_draft and proof.get("kind") == "public_probe_source_recovery_preparation"
             and proof.get("completed") is False)
    recovery_kind = ("public_probe_source_recovery_preparation" if draft else
                     "validated_zero_inference_public_probe_source_recovery" if public_probe
                     else "validated_zero_inference_host_preparation_recovery")
    if (type(proof.get("schema_version")) is not int or proof["schema_version"] != 1
            or proof.get("kind") != recovery_kind
            or proof.get("binding") != binding or proof.get("runtime") != str(runtime) or proof.get("run_id") != run_id
            or proof.get("completed") is not (not draft) or type(proof.get("model_calls")) is not int or proof["model_calls"] != 0):
        raise TaskflowError("formal recovery proof does not bind a completed zero-inference preparation")
    validate_formal_authority(config=config, candidate=candidate)
    if release["candidate_binding"] != _binding(candidate):
        raise TaskflowError("formal recovery release has a different candidate")
    snapshot_root = Path(release["snapshot"]["path"])
    frozen = verify_snapshot(snapshot_root)
    if any(frozen[key] != candidate[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("formal recovery snapshot differs from the original candidate")
    _checked_ref(release["receipt"])
    for reference in release["quality_evidence"]:
        _checked_ref(reference)
    _, intent_raw = _recovery_reference(proof["intent"], runtime.with_name(runtime.name + ".formal-intent.json"))
    if parse_json(intent_raw, "original formal intent") != binding:
        raise TaskflowError("formal recovery changed the original intent")
    reservation_path = Path(release["publication_root"]) / "reports/runtime-reservations" / (run_id + ".json")
    _, reservation_raw = _recovery_reference(proof["reservation"], reservation_path)
    reservation = parse_json(reservation_raw, "original run reservation")
    token_path = runtime.with_name(run_id + (".public-probe-preparation-recovery.json" if public_probe
                                           else ".host-preparation-recovery.json"))
    _, token_raw = _recovery_reference(proof["recovery_token"], token_path)
    token = parse_json(token_raw, "exclusive recovery token")
    if (type(token.get("schema_version")) is not int or token["schema_version"] != 1
            or token.get("kind") != ("exclusive_zero_inference_public_probe_source_recovery" if public_probe
                                    else "exclusive_zero_inference_preparation_recovery")
            or token.get("binding") != binding or token.get("run_id") != run_id or token.get("runtime") != str(runtime)
            or type(token.get("model_calls")) is not int or token["model_calls"] != 0):
        raise TaskflowError("formal recovery exclusive token belongs to different preparation")
    if public_probe and token.get("source_change") != proof.get("source_change"):
        raise TaskflowError("formal public-probe exclusive token has a different source correction")
    previous = proof["previous_runtime"]
    archive = _destination(Path(previous["path"]))
    retained = runtime.with_name(run_id + ".failed-preparation-" + sha256(token_raw)[:12])
    if (Path(previous["retained_path"]) != retained or archive in (runtime, retained)
            or any(archive.is_relative_to(path) or path.is_relative_to(archive) for path in (runtime, retained))):
        raise TaskflowError("formal recovery must retain the old runtime separately at its exact sibling")
    manifest_path, manifest_raw = _recovery_reference(previous["manifest"])
    if manifest_path.is_relative_to(archive) or manifest_path.is_relative_to(retained):
        raise TaskflowError("formal recovery archive manifest must remain outside the original ten files")
    files = parse_json(manifest_raw, "failed runtime manifest")["files"]
    expected_names = {"baseline-plan.json", "runtime_gate.py", "prepare_diagnostic.py", "execute_diagnostic.py", "file_evidence.py",
                      "request.md", "source-lock.json", "shell-probes.json", "preparation-process.json", "preparation-failure.json"}
    if public_probe:
        expected_names.remove("shell-probes.json")
    if not isinstance(files, dict) or set(files) != expected_names:
        raise TaskflowError("formal recovery requires exactly the original failed preparation files")
    _recovery_tree(archive, files)
    _recovery_tree(retained, files)
    adapter, _ = _checked_ref(config["adapter"])
    approved = {**config["adapter"]["files"], "prepare_diagnostic.py": config["adapter"]["sha256"]}
    if set(approved) != expected_names - {"request.md", "source-lock.json", "shell-probes.json", "preparation-process.json", "preparation-failure.json"}:
        raise TaskflowError("formal recovery needs all five approved source/baseline files")
    current_sources = (_public_probe_source_change(config, runtime, proof["source_change"])
                       if public_probe else approved)
    for name, digest in approved.items():
        for path in (adapter.parent / name, archive / name):
            _checked_ref({"path": str(path), "sha256": digest})
        _checked_ref({"path": str(runtime / name), "sha256": current_sources[name]})
    if public_probe:
        for name in expected_names:
            if stat.S_IMODE((archive / name).stat().st_mode) != stat.S_IMODE((retained / name).stat().st_mode):
                raise TaskflowError("formal public-probe retained file permissions drifted")
        for name in set(approved) | {"request.md"}:
            if stat.S_IMODE((archive / name).stat().st_mode) != stat.S_IMODE((runtime / name).stat().st_mode):
                raise TaskflowError("formal public-probe copied source/request permissions changed")
    _, request_raw = _checked_ref(config["request"])
    if _regular(archive / "request.md") != request_raw or _regular(runtime / "request.md") != request_raw:
        raise TaskflowError("formal recovery changed the original request")
    old_process = _read(archive / "preparation-process.json")
    old_failure = _read(archive / "preparation-failure.json")
    old_probe = _read(archive / "shell-probes.json") if not public_probe else None
    zero = lambda value: type(value) is int and value == 0
    if public_probe:
        prefix = "Negative probe is invalid without existing target: "
        message = old_failure.get("message", "")
        missing = Path(message[len(prefix):]) if isinstance(message, str) and message.startswith(prefix) else None
        if (old_failure.get("nature") != "preparation_failure_no_inference" or old_failure.get("run_id") != run_id
                or old_failure.get("type") != "ValueError" or missing is None or not missing.is_absolute()
                or tuple(missing.parts[-3:]) != ("draft", "input", "prompt.md")
                or missing.exists() or missing.is_symlink()
                or not all(zero(item.get("model_calls")) for item in (old_failure, old_process))
                or type(old_process.get("exit_code")) is not int or old_process["exit_code"] != 1
                or old_process.get("stdout") != "" or not isinstance(old_process.get("stderr"), str)
                or "ValueError: " + message not in old_process["stderr"]
                or "shell_probes(plan)" not in old_process["stderr"]):
            raise TaskflowError("formal public-probe recovery lacks the exact missing fixed-target zero-inference failure")
        old_work = missing.parents[2]
    else:
        if (old_failure.get("nature") != "preparation_failure_no_inference" or old_failure.get("run_id") != run_id
                or old_failure.get("type") != "PermissionError" or old_failure.get("message") != "[Errno 1] Operation not permitted"
                or not all(zero(item.get("model_calls")) for item in (old_failure, old_process, old_probe))
                or type(old_process.get("exit_code")) is not int or old_process["exit_code"] != 1
                or old_process.get("stdout") != "" or not isinstance(old_process.get("stderr"), str)
                or "listener.bind(address)" not in old_process["stderr"]
                or "PermissionError: [Errno 1] Operation not permitted" not in old_process["stderr"]
                or old_probe.get("nature") != "no_inference_existing_target_boundary_probes" or old_probe.get("passed") is not False
                or old_probe.get("tcp", {}).get("listener_created") is not False
                or old_probe.get("exception", {}).get("type") != "PermissionError"
                or old_probe.get("exception", {}).get("message") != "[Errno 1] Operation not permitted"):
            raise TaskflowError("formal recovery original failure is not the exact zero-inference host listener EPERM")
    python = str(Path(config["launcher_python"]).resolve())
    argv = [python, str(runtime / "prepare_diagnostic.py"), "--solver", release["solver"]["path"],
            "--evaluation-directory", str(snapshot_root / "draft/evaluation"), "--run-id", run_id,
            "--runtime-directory", str(runtime), "--task-directory", release["publication_root"], "--in-runtime"]
    for reference in release["quality_evidence"]:
        argv += ["--quality-evidence", reference["path"]]
    for reference in config.get("deny_read_probes", []):
        _checked_ref(reference)
        argv += ["--deny-read-probe", reference["path"]]
    original_argv = old_process.get("argv")
    if (not isinstance(original_argv, list) or not original_argv or not isinstance(original_argv[0], str)
            or str(Path(original_argv[0]).resolve()) != python or original_argv[1:] != argv[1:]):
        raise TaskflowError("formal recovery original argv differs from approved in-runtime preparation")
    _, process_raw = _recovery_reference(proof["recovery_process"])
    process = parse_json(process_raw, "recovery process")
    if (process.get("argv") != original_argv or type(process.get("returncode")) is not int or process["returncode"] != 0
            or not zero(process.get("model_calls")) or not isinstance(process.get("stdout"), str)
            or not isinstance(process.get("stderr"), str)):
        raise TaskflowError("formal recovery process did not complete the same zero-inference preparation")
    markers = ("execution-claim.json", "execution-attempt.json", "real-run", "initial-input-binding.json")
    if public_probe:
        markers += ("launch.json", "dispatch.json", "process.json", "termination.json", "collection.json", "trace.jsonl")
    for name in markers:
        if (runtime / name).exists() or (runtime / name).is_symlink():
            raise TaskflowError("formal recovery cannot accept an executed or uncertain model slot")
    _, plan_raw = _recovery_reference(proof["plan"], runtime / "execution-plan.json")
    plan = parse_json(plan_raw, "recovered formal plan")
    if (_read(runtime / "plan-lock.json") != {"sha256": sha256(plan_raw)}
            or plan.get("run_id") != run_id or plan.get("task_id") != candidate["task_id"]
            or plan.get("requested_model") != config["model"] or plan.get("timeout_seconds") != config["timeout_seconds"]
            or plan.get("input_digest") != candidate["input_digest"] or plan.get("request_sha256") != sha256(request_raw)
            or plan.get("launcher_python") != python or plan.get("run_reservation") != str(reservation_path)
            or plan.get("run_reservation_sha256") != sha256(reservation_raw)
            or plan.get("baseline_plan_sha256") != approved["baseline-plan.json"]
            or plan.get("source_solver") != release["solver"]["path"]
            or plan.get("private_directory") != str(snapshot_root / "draft/evaluation")
            or plan.get("frozen_binding", {}).get("snapshot_digest") != frozen["snapshot_digest"]
            or reservation.get("run_id") != run_id or reservation.get("runtime_directory") != str(runtime)
            or reservation.get("nature") != "preparation_only_zero_inference_not_a_model_attempt"
            or reservation.get("frozen_binding") != plan.get("frozen_binding")):
        raise TaskflowError("formal recovery plan/configuration/frozen/reservation binding differs")
    source_locks = [_read(path / "source-lock.json") for path in (archive, runtime)]
    for index, lock in enumerate(source_locks):
        if (lock.get("no_repository_dynamic_imports") is not True or not isinstance(lock.get("files"), list)
                or not lock["files"] or len({row["path"] for row in lock["files"]}) != len(lock["files"])):
            raise TaskflowError("formal recovery source lock is incomplete or ambiguous")
        by_path = {row["path"]: row for row in lock["files"]}
        for name, digest in (approved if index == 0 else current_sources).items():
            if by_path.get(str(runtime / name), {}).get("sha256") != digest:
                raise TaskflowError("formal recovery source lock omits its approved copied source")
        for reference in lock["files"]:
            path = Path(reference["path"])
            if public_probe and index == 0 and path.parent == runtime and path.name in approved:
                path = archive / path.name
            observed_source = _info(path)
            if (observed_source["sha256"] != reference.get("sha256")
                    or ("size" in reference and observed_source["size"] != reference["size"])):
                raise TaskflowError("formal recovery locked dependency bytes drifted")
    gate_path = runtime / "runtime_gate.py"
    namespace = {"__file__": str(gate_path), "__name__": "taskflow_readonly_formal_recovery_gate"}
    exec(compile(_regular(gate_path), str(gate_path), "exec"), namespace)
    namespace["validate"](plan, require_review=not draft)
    if public_probe:
        if namespace["public_tree"](old_work) != namespace["public_tree"](Path(release["solver"]["path"])):
            raise TaskflowError("formal public-probe original copied public bytes drifted")
        target = str(Path(plan["cwd"]) / next(iter(plan["public_tree"])))
        probe = _read(runtime / "shell-probes.json")
        targets = probe.get("host_existence_checked", [])
        if (not targets or targets[0].get("name") != "public" or targets[0].get("path") != target
                or targets[0].get("expected_readable") is not True
                or targets[0].get("expected_sha256") != plan["public_tree"][next(iter(plan["public_tree"]))]["sha256"]):
            raise TaskflowError("formal public-probe evidence does not bind the deterministic real public member")
    if draft:
        return {"schema_version": 1, "kind": recovery_kind, "completed": False, "source_change": proof["source_change"],
                "binding": binding, "run_id": run_id, "runtime": str(runtime), "model_calls": 0,
                **{key: proof[key] for key in ("plan", "reservation", "intent", "previous_runtime", "recovery_process")},
                "recovery": recovery}
    _, acceptance_raw = _recovery_reference(proof["acceptance"], runtime / "launch-review.json")
    acceptance = parse_json(acceptance_raw, "recovered formal acceptance")
    if (acceptance.get("approved") is not True or acceptance.get("preparation_binding") != binding
            or acceptance.get("plan_sha256") != sha256(plan_raw) or acceptance.get("input_digest") != candidate["input_digest"]):
        raise TaskflowError("formal recovery acceptance does not bind the unchanged preparation")
    authorization_path, _ = _checked_ref(acceptance["authorization"])
    if authorization_path != Path(config["authorization"]["path"]) or acceptance["authorization"]["sha256"] != config["authorization"]["sha256"]:
        raise TaskflowError("formal recovery acceptance has a different authority")
    _, normal_raw = _recovery_reference(acceptance["normal_tools"], runtime / "normal-tools-probe.json")
    normal = parse_json(normal_raw, "recovered normal tools")
    observed = normal.get("observed", {})
    package = _read(Path(plan["cwd"]) / "package.json")
    member = next(row for row in package["files"] if row["role"] in PUBLIC_ROLES)
    node = normal.get("binding", {}).get("node", {})
    environments = [part for part in plan["argv"] if part.startswith("shell_environment_policy.set=")]
    paths = [match[1] for part in environments
             if (match := re.search(r'"?PATH"?\s*=\s*("(?:\\.|[^"\\])*")', part))]
    nodes = [Path(item) / "node" for encoded in paths for item in json.loads(encoded).split(":")
             if item.endswith("/node/bin")]
    if (len(nodes) != 1 or node.get("path") != str(nodes[0])
            or _info(nodes[0]) != {key: node.get(key) for key in ("sha256", "size")}):
        raise TaskflowError("formal recovery normal-tools runtime bytes drifted")
    if (normal.get("binding", {}).get("plan_sha256") != sha256(plan_raw)
            or normal["binding"].get("public_sha256") != member["sha256"]
            or type(normal.get("returncode")) is not int or normal["returncode"] != 0
            or not all(zero(normal.get(key)) for key in ("model_calls", "external_model_requests"))
            or observed.get("public_sha256") != member["sha256"] or "openssl_conf" not in observed or observed["openssl_conf"] is not None
            or observed.get("private_denied") is not True or observed.get("deny_code") not in ("EPERM", "EACCES")
            or any(observed.get(key) is not True for key in ("own_write", "fork_ipc", "sqlite"))):
        raise TaskflowError("formal recovery recorded native normal-tool checks did not pass")
    return {**({"schema_version": 1, "kind": recovery_kind, "completed": True, "source_change": proof["source_change"]} if public_probe else {}),
            "binding": binding, "run_id": run_id, "runtime": str(runtime), "model_calls": 0,
            **{key: proof[key] for key in ("plan", "acceptance", "reservation", "intent", "previous_runtime", "recovery_process")},
            "recovery": recovery}


def _archive_manifest(archive: Path, attempt: dict) -> tuple[dict, dict, dict]:
    binding = _read(archive / "archive-binding.json")
    manifest = _read(archive / "manifest.json")
    if (attempt.get("state") not in ("completed", "failed", "cancelled")
            or attempt.get("role") != "solver" or manifest.get("role") != "solver"
            or manifest.get("mode") != "existing_isolated" or manifest.get("isolated", {}).get("flavor") != "formal"
            or manifest.get("intent_id") != attempt["intent_id"] or binding.get("intent_id") != attempt["intent_id"]
            or any(manifest.get("candidate_binding", {}).get(key) != attempt.get(key)
                   for key in ("candidate_id", "task_id", "content_digest", "input_digest"))):
        raise TaskflowError("formal original archive differs from its terminal solver attempt")
    tree = _full_tree(archive)
    observed = {name: {key: row[key] for key in ("size", "sha256")} for name, row in tree["files"].items()
                if name != "archive-binding.json"}
    directories = sorted(name.rstrip("/") for name in tree["directories"] if name != ".")
    if tree["special"] or observed != binding.get("files") or directories != binding.get("directories"):
        raise TaskflowError("formal archive file/directory membership or original bytes changed")
    if (_read(archive / "manifest-lock.json") != _info(archive / "manifest.json")
            or binding.get("collection") != _info(archive / "collection.json")):
        raise TaskflowError("formal archive manifest/collection seal differs")
    collection = _validated_collection(archive, manifest)
    intent = _read(archive / "supplemental-intent.json")
    if (intent.get("manifest") != _info(archive / "manifest.json")
            or intent.get("collection") != _info(archive / "collection.json")
            or intent.get("intent_id") != attempt["intent_id"]):
        raise TaskflowError("full original snapshot belongs to different formal evidence")
    _full_verify(archive / "supplemental", intent, True)
    return manifest, collection, intent


def _visible_trace(path: Path) -> tuple[dict, str | None]:
    raw = _regular(path, limit=128 * 1024 * 1024)
    messages, sids, terminals, errors, final = [], [], [], [], None
    offset = 0
    for number, line in enumerate(raw.splitlines(keepends=True), 1):
        try:
            item = json.loads(line)
            if item.get("type") == "thread.started":
                sids.append(item.get("thread_id"))
            if item.get("type") in ("turn.completed", "turn.failed", "error"):
                terminals.append(item["type"])
            message = item.get("item", {})
            if item.get("type") == "item.completed" and message.get("type") == "agent_message" and isinstance(message.get("text"), str):
                final = message["text"]
                messages.append({"line": number, "offset": offset, "text_sha256": sha256(final.encode())})
        except (ValueError, AttributeError):
            errors.append({"line": number, "offset": offset})
        offset += len(line)
    return {"thread_ids": sids, "terminal_events": terminals, "agent_messages": messages,
            "errors": errors, "scope": "Actual visible CLI events; no hidden reasoning inferred."}, final


def first_review_target(row: dict) -> str:
    """Preserve the original workspace layout in the reviewer's private staging."""
    validate_id(row.get("id"), "original artifact ID")
    if row.get("workspace"):
        value = "original/" + row["workspace"] + "/" + row["workspace_path"]
    elif row.get("root_id") and row.get("source_path"):
        value = "original/" + row["root_id"] + "/" + row["source_path"]
    else:
        value = "original/" + row["id"] + "/" + Path(row["incoming_path"]).name
    try:
        if any(part.startswith(".capture-") for part in value.split("/")):
            raise TaskflowError("reserved temporary namespace")
        return relative_path(value)
    except TaskflowError:
        return "original/" + row["id"] + "/payload.bin"


@_handoff_errors
def import_formal(*, attempt: dict, archive: Path, snapshot: Path, run_id: str) -> dict:
    """Transport a sealed full archive into the existing append-only run format."""
    archive, snapshot = Path(archive).absolute(), Path(snapshot).absolute()
    validate_id(run_id, "run_id")
    frozen = verify_snapshot(snapshot)
    # Store attempts carry input/content; the caller supplies the registered
    # evaluation digest as part of the exact candidate binding as well.
    manifest, collection, intent = _archive_manifest(archive, attempt)
    if any(frozen[key] != manifest["candidate_binding"][key] for key in
           ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("formal original and run snapshot are different candidates")
    payloads, _ = _full_layout(intent)
    locations = {(row["source"], row["path"]): "supplemental/" + name for name, row in payloads.items()}

    def alias(name: str, member: str) -> str:
        item = intent["aliases"][name]
        relative = member if item["relative"] == "." else item["relative"].rstrip("/") + "/" + member
        try:
            return locations[(item["source"], relative)]
        except KeyError as error:
            raise TaskflowError("full formal archive lacks original member: " + name + "/" + member) from error

    inner_plan = _read(archive / alias("inner", "execution-plan.json"))
    inner_attempt = _read(archive / alias("inner", "real-run/attempt.json"))
    observer = _read(archive / alias("inner", "real-run/file-change-evidence/summary.json"))
    trace_name, request_name = alias("inner", "real-run/trace.jsonl"), alias("inner", "real-run/request.md")
    if (inner_plan.get("run_id") != run_id or inner_attempt.get("run_id") != run_id
            or inner_plan.get("frozen_binding", {}).get("snapshot_digest") != frozen["snapshot_digest"]
            or inner_plan.get("input_digest") != frozen["input_digest"]
            or _info(archive / request_name)["sha256"] != inner_plan.get("request_sha256")):
        raise TaskflowError("formal inner originals differ from the run/frozen/request binding")
    if (not isinstance(inner_attempt.get("returncode"), int) or isinstance(inner_attempt["returncode"], bool)
            or observer.get("observer_stopped") is not True
            or observer.get("trace_bytes_at_stop") != _info(archive / trace_name)["size"]):
        raise TaskflowError("formal inner termination and stopped observer are not proved")
    trace, final = _visible_trace(archive / trace_name)
    if attempt.get("cli_sid") and trace["thread_ids"] != [attempt["cli_sid"]]:
        raise TaskflowError("formal original trace belongs to another CLI context")
    package = _read(archive / alias("work", "package.json"))
    actual_input = sha256(canonical(package["files"]))
    if (package.get("audience") != "solver" or package.get("task_id") != frozen["task_id"]
            or package.get("input_digest") != actual_input or actual_input != inner_plan["input_digest"]):
        raise TaskflowError("actual solver package public input digest differs")
    public = {item["path"] for item in package["files"]}
    artifacts, rows, visible = [], [], []
    originals = {**_read(archive / "archive-binding.json")["files"],
                 "archive-binding.json": _info(archive / "archive-binding.json")}
    work, scratch, inner = (intent["aliases"][name] for name in ("work", "scratch", "inner"))
    prefix = "" if work["relative"] == "." else work["relative"].rstrip("/") + "/"
    scratch_prefix = "" if scratch["relative"] == "." else scratch["relative"].rstrip("/") + "/"
    inner_prefix = "" if inner["relative"] == "." else inner["relative"].rstrip("/") + "/"
    for number, (name, expected) in enumerate(sorted(originals.items()), 1):
        artifact_id = f"raw_{number:06d}"
        kind, visibility = "evidence", "teacher_engineering_only"
        source = next((row for payload, row in payloads.items() if "supplemental/" + payload == name), None)
        if name == trace_name:
            kind, visibility = "trace", "actual_trace"
        elif name == request_name:
            visibility = "actual_request"
        elif source:
            if source["source"] == work["source"] and source["path"].startswith(prefix):
                member = source["path"][len(prefix):]
                if member == "package.json" or member in public:
                    visibility = "public"
                else:
                    kind, visibility = "answer", "submission"
            if source["source"] == scratch["source"] and source["path"].startswith(scratch_prefix):
                kind, visibility = "answer", "submission"
            if source["source"] == inner["source"] and source["path"].startswith(inner_prefix + "real-run/file-change-evidence/"):
                visibility = "visible_file_change_process_evidence"
        artifacts.append({"id": artifact_id, "path": "raw/" + name, "kind": kind})
        row = {"id": artifact_id, "archive_path": name, "incoming_path": "raw/" + name,
               "visibility": visibility, **expected}
        if source:
            row.update(root_id=source["source"], source_path=source["path"])
            if source["source"] == work["source"] and source["path"].startswith(prefix):
                row.update(workspace="work", workspace_path=source["path"][len(prefix):])
            elif source["source"] == scratch["source"] and source["path"].startswith(scratch_prefix):
                row.update(workspace="scratch", workspace_path=source["path"][len(scratch_prefix):])
        rows.append(row)
        if visibility != "teacher_engineering_only":
            visible.append(row)
    input_index = {"candidate_binding": manifest["candidate_binding"], "actual_input_digest": actual_input,
                   "frozen_input_digest": frozen["input_digest"], "files": package["files"],
                   "scope": "Hash-bound original solver package plus formal initial/post-run binding evidence."}
    identity = {"attempt_id": attempt["attempt_id"], "archive_binding": _info(archive / "archive-binding.json"),
                "snapshot_digest": frozen["snapshot_digest"], "run_id": run_id}
    incoming = _destination(snapshot.parent.parent / "incoming" / run_id)
    if incoming.is_relative_to(archive) or archive.is_relative_to(incoming):
        raise TaskflowError("formal incoming destination overlaps original archive")
    if not incoming.exists():
        with atomic_directory(incoming) as stage:
            for row in rows:
                target = stage / row["incoming_path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                raw = _regular(archive / row["archive_path"], limit=128 * 1024 * 1024)
                if {"sha256": sha256(raw), "size": len(raw)} != {key: row[key] for key in ("sha256", "size")}:
                    raise TaskflowError("formal archived original changed during import transport")
                target.write_bytes(raw)
            if final is not None:
                target = stage / "derived/visible-response.txt"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(final, encoding="utf-8")
                artifacts.append({"id": "visible_response", "path": "derived/visible-response.txt", "kind": "answer"})
                visible.append({"id": "visible_response", "incoming_path": "derived/visible-response.txt",
                                "root_id": "derived", "source_path": "visible-response.txt",
                                "visibility": "derived_actual_final_agent_message",
                                "derived_from": {"archive_path": trace_name, **trace["agent_messages"][-1]},
                                **_info(target)})
            membership = {}
            for name in ("work", "scratch"):
                item = intent["aliases"][name]
                tree = intent["snapshot"][item["source"]]
                prefix = "" if item["relative"] == "." else item["relative"].rstrip("/") + "/"
                membership[name] = {
                    "directories": [path[len(prefix):] or "." for path in tree["directories"] if path.startswith(prefix)],
                    "special": {path[len(prefix):]: {key: value for key, value in row.items() if key in ("kind", "mode")}
                                for path, row in tree["special"].items() if path.startswith(prefix)},
                    "scope": "Original own workspace membership; special objects are indexed and not followed."}
            member_path = "derived/workspace-membership.json"
            write_json(stage / member_path, membership)
            artifacts.append({"id": "workspace_membership", "path": member_path, "kind": "evidence"})
            visible.append({"id": "workspace_membership", "incoming_path": member_path,
                            "root_id": "derived", "source_path": "workspace-membership.json",
                            "visibility": "submission_workspace_membership", **_info(stage / member_path)})
            def optional_inner(member: str) -> tuple[str | None, dict | None]:
                source = intent["aliases"]["inner"]
                relative = member if source["relative"] == "." else source["relative"].rstrip("/") + "/" + member
                path = locations.get((source["source"], relative))
                return (path, _read(archive / path)) if path else (None, None)

            normal_path, normal = optional_inner("normal-tools-probe.json")
            readback_path, readback = optional_inner("post-run-input-readback.json")
            context_path, context = optional_inner("exec-context.json")
            observed = (normal or {}).get("observed", {})
            source_ids = {row["archive_path"]: row["id"] for row in rows}
            locators = {"manifest.json": ["/model", "/reasoning_selection"],
                        "collection.json": ["/technical_accepted", "/delivery_complete", "/termination"],
                        alias("inner", "real-run/attempt.json"): ["/returncode", "/status", "/exceptions"]}
            for source, pointers in ((normal_path, ["/observed", "/binding/plan_sha256"]),
                                     (readback_path, ["/matches", "/errors"]),
                                     (context_path, ["/model_calls", "/external_model_requests", "/observations/0/model", "/observations/0/reasoning"])):
                if source:
                    locators[source] = pointers
            configuration = {
                "schema_version": 1, "scope": "Actual local configuration/process observations; no served identity or business/quality judgment.",
                "input_digest": actual_input, "snapshot_digest": frozen["snapshot_digest"],
                "requested_model": manifest["model"]["requested"],
                "reasoning_selection": manifest.get("reasoning_selection"),
                "request_reasoning_override": manifest["model"].get("request_reasoning_override"),
                "client_resolved_model": manifest["model"].get("client_resolved"),
                "client_resolved_reasoning": manifest["model"].get("client_resolved_reasoning"),
                "served_model": manifest["model"].get("served"), "served_revision": manifest["model"].get("revision"),
                "served_identity_unknown": manifest["model"].get("served") is None,
                "process": {**{key: inner_attempt.get(key) for key in ("status", "returncode", "started_at", "ended_at")},
                            "exception_types": [item.get("type") for item in inner_attempt.get("exceptions", []) if isinstance(item, dict)],
                            "intervention_count": len(inner_attempt["interventions"]) if isinstance(inner_attempt.get("interventions"), list) else None},
                "technical_delivery": {key: collection.get(key) for key in
                                       ("process_ended", "model_completed", "delivery_complete", "technical_accepted")},
                "artifact_error_count": len(collection["artifact_errors"]) if isinstance(collection.get("artifact_errors"), list) else None,
                "input_readback_matches": (readback or {}).get("matches"),
                "normal_tools": {key: observed.get(key) for key in
                                 ("node", "openssl_conf", "private_denied", "deny_code", "public_sha256", "own_write", "fork_ipc", "sqlite")},
                "client_capture": {key: (context or {}).get(key) for key in ("model_calls", "external_model_requests", "passed")},
                "fact_sources": [{"artifact_id": source_ids[source], "locators": pointers, **_info(archive / source)}
                                 for source, pointers in locators.items()]}
            configuration_path = "derived/actual-configuration.json"
            write_json(stage / configuration_path, configuration)
            artifacts.append({"id": "actual_configuration", "path": configuration_path, "kind": "evidence"})
            visible.append({"id": "actual_configuration", "incoming_path": configuration_path,
                            "root_id": "derived", "source_path": "actual-configuration.json",
                            "visibility": "actual_configuration", **_info(stage / configuration_path)})
            indexes = {"source_index": {"files": rows, "original_archive": str(archive)},
                       "directories": {"sources": intent["snapshot"], "aliases": intent["aliases"]},
                       "response_index": trace, "input_readback": input_index,
                       "first_review_index": {"allowed_direct_artifacts": visible,
                                              "excluded": "Release, control, engineering, teacher and historical pointers are not first-review inputs."}}
            for name, value in indexes.items():
                path = "indexes/" + name + ".json"
                write_json(stage / path, value)
                artifacts.append({"id": name, "path": path, "kind": "evidence"})
            model = manifest["model"]
            status = "completed" if collection.get("technical_accepted") else inner_attempt.get("status")
            if status not in ("completed", "timeout", "cancelled", "failed"):
                status = "incomplete"
            record = {"schema_version": 1, "run_id": run_id, "nature": "observed_run", "status": status,
                      "actual_input_digest": actual_input,
                      "requested_config": {"model": model["requested"], "reasoning_effort": model["reasoning_effort"],
                                           "reasoning_selection": manifest.get("reasoning_selection"),
                                           "request_reasoning_override": model.get("request_reasoning_override"),
                                           "tools": {"source": "sealed actual formal argv and original capability probes"}},
                      "observed_config": {"model": model.get("served"), "served_model": model.get("served"),
                                          "served_revision": model.get("revision"), "client_resolved": model.get("client_resolved"),
                                          "client_resolved_reasoning": model.get("client_resolved_reasoning"),
                                          "client_resolution_scope": model.get("client_resolution_scope"),
                                          "tools": {"visible_calls": "original raw trace", "capabilities": "original formal probes"}},
                      "started_at": inner_attempt.get("started_at"), "ended_at": inner_attempt.get("ended_at"),
                      "interventions": inner_attempt.get("interventions"), "exceptions": inner_attempt.get("exceptions"),
                      "input_evidence": ["input_readback#actual_input_digest"],
                      "isolation_evidence": ["source_index#files"], "artifacts": artifacts,
                      "limitations": ["Unknown served identity is retained. Final snapshots do not reconstruct deleted transient bytes or hidden reasoning."]}
            write_json(stage / "run.json", record)
            expected = {item["id"]: {"source_path": item["path"], "kind": item.get("kind"),
                                    **_info(stage / item["path"])} for item in artifacts}
            write_json(stage / "handoff-binding.json", {"binding": identity,
                                                       "record": _info(stage / "run.json"), "artifacts": expected})
            _archive_manifest(archive, attempt)
    handoff = _read(incoming / "handoff-binding.json")
    if handoff.get("binding") != identity or handoff.get("record") != _info(incoming / "run.json"):
        raise TaskflowError("existing formal import belongs to another original attempt/archive")
    record = _read(incoming / "run.json")
    expected = {item["id"]: {"source_path": item["path"], "kind": item.get("kind"), **_info(incoming / item["path"])}
                for item in record["artifacts"]}
    if expected != handoff.get("artifacts"):
        raise TaskflowError("sealed incoming formal artifacts changed before native import")
    run_path = snapshot.parent.parent / "runs" / run_id
    if not run_path.exists():
        import_run(snapshot, incoming / "run.json")
    verified = verify_run(run_path)
    actual = {item["id"]: {key: item[key] for key in ("source_path", "kind", "size", "sha256")}
              for item in verified["artifacts"]}
    if (verified["snapshot_digest"] != frozen["snapshot_digest"] or actual != expected
            or _regular(run_path / "record.original.json") != _regular(incoming / "run.json")):
        raise TaskflowError("existing native run differs from fixed original record/artifact bytes")
    first = _read(incoming / "indexes/first_review_index.json")
    typed = [{"path": str(incoming / row["incoming_path"]), "sha256": row["sha256"], "size": row["size"],
              "kind": "trace" if row["visibility"] in ("actual_trace", "visible_file_change_process_evidence", "actual_configuration") else "submission",
              "target": first_review_target(row)}
             for row in first["allowed_direct_artifacts"]]
    return {"run_path": str(run_path), "run_digest": verified["run_digest"], "archive": str(archive),
            "classification": verified["classification"], "gaps": verified["gaps"],
            "first_review_inputs": typed, "first_review_index": str(incoming / "indexes/first_review_index.json")}
