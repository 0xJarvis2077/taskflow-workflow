"""Finite review delivery checks, also usable as a read-only same-turn CLI.

These checks authenticate report shape, declared references and arithmetic.
They never choose a score, rewrite a report or decide the business merits.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re


class AssessmentContractError(ValueError):
    """One deterministic delivery error; never an inferred review conclusion."""

    def __init__(self, message, *, code="invalid_assessment_delivery", field=None):
        super().__init__(message)
        self.details = {"code": code, "message": message}
        if field is not None:
            self.details["field"] = field

    def as_dict(self):
        return dict(self.details)


def require(condition, message, *, field=None):
    if not condition:
        raise AssessmentContractError(message, field=field)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def strings(value, name):
    require(isinstance(value, list) and all(text(item) for item in value),
            name + " must be a list of nonempty strings", field=name)


def number(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def relative(value):
    return (isinstance(value, str) and bool(value) and not value.startswith("/")
            and "\\" not in value and all(part not in ("", ".", "..") for part in value.split("/")))


def read_json(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "delivery must be an ordinary file")
    raw = path.read_bytes()
    require(len(raw) <= 16 * 1024 * 1024, "delivery file exceeds 16 MiB")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise AssessmentContractError("delivery is not valid UTF-8 JSON", field="delivery") from error


def _binding(document, context):
    require(isinstance(document, dict), "delivery must be an object")
    require(type(document.get("schema_version")) is int and document["schema_version"] == 1,
            "schema_version must be integer 1", field="schema_version")
    require(document.get("binding") == context["binding"],
            "delivery must sign this exact candidate, solver and invocation", field="binding")


def _source_map(context):
    sources = context["sources"]
    require(isinstance(sources, list) and bool(sources), "complete source index required")
    mapping = {}
    for row in sources:
        require(isinstance(row, dict) and relative(row.get("path"))
                and isinstance(row.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", row["sha256"])
                and type(row.get("size")) is int and row["size"] >= 0,
                "invalid locked source index")
        require(row["path"] not in mapping, "duplicate locked source target")
        root = Path(context["inputs_root"]).resolve()
        path = root / row["path"]
        require(path.resolve().is_relative_to(root) and path.is_file() and not path.is_symlink(),
                "locked source is not a staged ordinary file")
        raw = path.read_bytes()
        require(len(raw) == row["size"] and hashlib.sha256(raw).hexdigest() == row["sha256"],
                "staged source differs from its declared bytes")
        mapping[row["path"]] = {**row, "raw": raw}
    return mapping


def _location(value, source, field):
    require(text(value), "source location required", field=field)
    line = re.fullmatch(r"L([1-9][0-9]*)(?:-L([1-9][0-9]*))?", value)
    byte = re.fullmatch(r"B([0-9]+)-B([0-9]+)", value)
    if line:
        try:
            length = len(source["raw"].decode("utf-8").splitlines())
        except UnicodeDecodeError as error:
            raise AssessmentContractError("binary source requires a byte-range location", field=field) from error
        start, end = int(line[1]), int(line[2] or line[1])
        require(1 <= start <= end <= length, "line location is outside the actual source", field=field)
    elif byte:
        require(0 <= int(byte[1]) <= int(byte[2]) <= source["size"]
                and (int(byte[1]) < int(byte[2]) or source["size"] == 0),
                "byte-range location is outside the actual source", field=field)
    else:
        raise AssessmentContractError("location must be L1[-L2] or a half-open B0-B1 range", field=field)


def _citations(rows, sources, field):
    require(isinstance(rows, list) and bool(rows), "nonempty source citations required", field=field)
    for index, row in enumerate(rows):
        require(isinstance(row, dict) and text(row.get("path")) and text(row.get("location")),
                "source citation needs a precise nonempty location", field=field + "/" + str(index))
        source = sources.get(row.get("path"))
        require(source is not None and row.get("sha256") == source["sha256"],
                "citation is outside the locked stage sources or has another SHA256",
                field=field + "/" + str(index))
        _location(row["location"], source, field + "/" + str(index) + "/location")


def validate_facts(document, context):
    """Check full source membership and the role's original complete/incomplete statement."""
    _binding(document, context)
    require(set(document) == {"schema_version", "kind", "binding", "status", "coverage", "facts", "limitations"},
            "facts fields differ from the current contract")
    require(document["kind"] == "review_facts_v1", "wrong factual review kind", field="kind")
    require(document["status"] in ("complete", "incomplete"), "unknown factual review status", field="status")
    sources = _source_map(context)
    coverage = document["coverage"]
    require(isinstance(coverage, list), "coverage must be a list", field="coverage")
    seen = set()
    unresolved = False
    for index, row in enumerate(coverage):
        field = "coverage/" + str(index)
        require(isinstance(row, dict) and set(row) == {"path", "sha256", "status", "location", "limitation"},
                "coverage row fields differ", field=field)
        require(text(row["path"]), "coverage path must be a string", field=field + "/path")
        source = sources.get(row["path"])
        require(source is not None and row["path"] not in seen and row["sha256"] == source["sha256"],
                "coverage must identify each actual locked source exactly once", field=field)
        require(row["status"] in ("read", "inspected", "unresolved") and text(row["location"])
                and isinstance(row["limitation"], str), "coverage needs actual disposition and location", field=field)
        _location(row["location"], source, field + "/location")
        if row["status"] == "unresolved":
            unresolved = True
            require(text(row["limitation"]), "unresolved source needs its actual limitation", field=field)
        seen.add(row["path"])
    require(seen == set(sources), "coverage omits actual public or submission members", field="coverage")
    require(document["status"] != "complete" or not unresolved,
            "complete factual review cannot leave a source unread", field="status")
    facts = document["facts"]
    require(isinstance(facts, list), "facts must be a list", field="facts")
    ids = set()
    for index, row in enumerate(facts):
        field = "facts/" + str(index)
        require(isinstance(row, dict) and set(row) == {"id", "statement", "evidence"}
                and text(row["id"]) and row["id"] not in ids and text(row["statement"]),
                "fact needs a unique ID, original statement and citations", field=field)
        _citations(row["evidence"], sources, field + "/evidence")
        ids.add(row["id"])
    require(document["status"] != "complete" or bool(facts), "complete review needs original factual findings", field="facts")
    strings(document["limitations"], "limitations")
    return {"status": "structure_valid", "stage": "facts", "outcome": document["status"], "score_assigned": False}


def validate_score(document, context):
    """Check the model's original score against explicit frozen criterion parameters."""
    _binding(document, context)
    require(set(document) == {"schema_version", "kind", "binding", "outcome", "facts_sha256", "validity", "criteria", "total", "limitations"},
            "score fields differ from the current contract")
    require(document["kind"] == "review_score_v1", "wrong scoring review kind", field="kind")
    require(document["outcome"] in ("scored", "inconclusive"), "unknown scoring outcome", field="outcome")
    require(document["facts_sha256"] == context["facts_sha256"], "score cites different sealed facts", field="facts_sha256")
    sources = _source_map(context)
    validity = document["validity"]
    require(isinstance(validity, dict) and set(validity) == {"local_eligible", "reason", "evidence"}
            and (type(validity["local_eligible"]) is bool or validity["local_eligible"] is None)
            and text(validity["reason"]), "local eligibility must remain the reviewer's explicit conclusion", field="validity")
    _citations(validity["evidence"], sources, "validity/evidence")
    expected = {row["id"]: row for row in context["criteria"]}
    rows = document["criteria"]
    require(isinstance(rows, list), "criterion scores must be a list", field="criteria")
    seen, assigned = set(), []
    for index, row in enumerate(rows):
        field = "criteria/" + str(index)
        require(isinstance(row, dict) and set(row) == {"id", "points", "max_points", "reason", "evidence"}
                and text(row.get("id")) and row["id"] in expected and row["id"] not in seen and text(row.get("reason")),
                "criterion needs one original score and rationale", field=field)
        criterion = expected[row["id"]]
        require(number(row["max_points"]) and row["max_points"] == criterion["max_points"], "criterion maximum differs from frozen rubric", field=field)
        require(number(row["points"]) and 0 <= row["points"] <= criterion["max_points"],
                "criterion points must be finite and within its declared maximum", field=field)
        _citations(row["evidence"], sources, field + "/evidence")
        require(criterion["source"] in row["evidence"],
                "criterion score must cite its exact frozen rubric rule", field=field + "/evidence")
        projection = context.get("scoring_projection")
        if projection is not None:
            require(any(citation.get("path") == projection["path"] and citation.get("sha256") == projection["sha256"]
                        for citation in row["evidence"]),
                    "criterion score must cite the complete frozen scoring conditions", field=field + "/evidence")
        seen.add(row["id"])
        assigned.append(row["points"])
    if document["outcome"] == "scored":
        require(seen == set(expected), "original score omits frozen criteria", field="criteria")
        require(number(document["total"]) and math.isclose(document["total"], sum(assigned), rel_tol=0, abs_tol=1e-9),
                "original total does not equal the original criterion sum", field="total")
    else:
        require(document["total"] is None, "inconclusive review cannot manufacture a total", field="total")
        require(bool(document["limitations"]), "inconclusive review needs actual limitations", field="limitations")
    strings(document["limitations"], "limitations")
    validate_business_values(document, context, complete=True)
    return {"status": "structure_valid", "stage": "score", "outcome": document["outcome"], "score_assigned": False}


def business_payload(document, context):
    """Retain valid signed scalar values; never fill a missing score or select one."""
    require(isinstance(document, dict), "original scoring payload must be an object")
    result = {}
    if document.get("outcome") in ("scored", "inconclusive"):
        result["outcome"] = document["outcome"]
    validity = document.get("validity")
    if isinstance(validity, dict):
        retained = {}
        if "local_eligible" in validity and (type(validity["local_eligible"]) is bool or validity["local_eligible"] is None):
            retained["local_eligible"] = validity["local_eligible"]
        if text(validity.get("reason")):
            retained["reason"] = validity["reason"]
        if retained:
            result["validity"] = retained
    expected = {row["id"]: row["max_points"] for row in context["criteria"]}
    rows = document.get("criteria")
    retained = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict) or not text(row.get("id")) or row["id"] not in expected:
                continue
            identity = row["id"]
            fields = {}
            if number(row.get("max_points")) and row["max_points"] == expected[identity]:
                fields["max_points"] = row["max_points"]
            if number(row.get("points")) and 0 <= row["points"] <= expected[identity]:
                fields["points"] = row["points"]
            if text(row.get("reason")):
                fields["reason"] = row["reason"]
            prior = retained.setdefault(identity, {})
            if not all(key not in prior or prior[key] == value for key, value in fields.items()):
                raise AssessmentContractError("original delivery contains conflicting signed values for one criterion",
                    code="assessment.ambiguous_business_payload", field="criteria/" + identity)
            prior.update(fields)
        if retained:
            result["criteria"] = retained
        complete = (len(rows) == len(expected) and set(retained) == set(expected)
                    and all({"max_points", "points"} <= set(fields) for fields in retained.values()))
        if complete and number(document.get("total")) and math.isclose(
                document["total"], sum(fields["points"] for fields in retained.values()), rel_tol=0, abs_tol=1e-9):
            result["total"] = document["total"]
    return result


def accumulate_business_payload(original, document, context):
    """Append newly signed valid values without replacing an earlier signed one."""
    current = business_payload(document, context)
    result = json.loads(json.dumps(original))
    for key, value in current.items():
        if key == "criteria":
            criteria = result.setdefault("criteria", {})
            for identity, fields in value.items():
                retained = criteria.setdefault(identity, {})
                for field, scalar in fields.items():
                    retained.setdefault(field, scalar)
        elif key == "validity":
            retained = result.setdefault("validity", {})
            for field, scalar in value.items():
                retained.setdefault(field, scalar)
        else:
            result.setdefault(key, value)
    return result


def validate_business_values(document, context, *, complete=False):
    """Reject a changed signed value even if another delivery field is invalid."""
    locked = context.get("business_lock")
    if locked is None:
        return
    current = business_payload(document, context)

    def same(actual, key, original, field):
        if (complete or key in actual) and (key not in actual or actual[key] != original):
            raise AssessmentContractError("delivery correction changed an already signed business value",
                code="assessment.signed_business_changed", field=field)

    for key, value in locked.items():
        if key == "criteria":
            for identity, fields in value.items():
                for field, original in fields.items():
                    same(current.get("criteria", {}).get(identity, {}), field, original, "criteria/" + identity + "/" + field)
        elif key == "validity":
            for field, original in value.items():
                same(current.get("validity", {}), field, original, "validity/" + field)
        else:
            same(current, key, value, key)


def check_delivery(context, directory, *, intent_id=None, inputs_root=None):
    context = dict(context)
    if intent_id is not None:
        context["binding"] = {**context["binding"], "intent_id": intent_id}
    if inputs_root is not None:
        context["inputs_root"] = inputs_root
    stage = context["stage"]
    require(stage in ("facts", "score"), "unknown assessment stage")
    document = read_json(Path(directory) / ("facts.json" if stage == "facts" else "score.json"))
    return validate_facts(document, context) if stage == "facts" else validate_score(document, context)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--context")
    source.add_argument("--context-json")
    parser.add_argument("--directory", required=True)
    parser.add_argument("--intent-id")
    parser.add_argument("--inputs-root")
    args = parser.parse_args()
    try:
        context = read_json(args.context) if args.context else json.loads(args.context_json)
        print(json.dumps(check_delivery(context, args.directory, intent_id=args.intent_id, inputs_root=args.inputs_root)))
    except (ValueError, OSError, KeyError, TypeError) as error:
        details = error.as_dict() if isinstance(error, AssessmentContractError) else {"code": "invalid_assessment_delivery", "message": str(error)}
        parser.exit(2, json.dumps({"status": "invalid_assessment_delivery", "error": details, "score_assigned": False}) + "\n")


if __name__ == "__main__":
    main()
