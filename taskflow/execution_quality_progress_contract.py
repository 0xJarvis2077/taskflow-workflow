"""Finite serial-quality output structure and same-turn local preflight.

Standard library only: copied into the existing runtime, with no teacher/plan
reads, network, database, signing, normalization or delivery writes.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re

PROGRESS_KIND = "quality_read_progress_v1"
TOP_FIELDS = {"schema_version", "kind", "intent_id", "candidate_binding", "segment_index",
              "status", "pages", "facts", "remaining", "public_checkpoint"}
PAGE_FIELDS = {"id", "sha256", "disposition", "evidence", "association", "remaining"}
DISPOSITIONS = ("read", "same_bytes_this_sid", "not_current_dependency", "unresolved")
FINAL_FILES = ("intake.md", "issues.json", "adoption-readiness.json")
FIELD_CONTRACT = {
    "top": "exact TOP_FIELDS; version integer 1; kind/status fixed; intent/binding/index exact; facts/remaining/pages lists",
    "page": "exact PAGE_FIELDS; id/hash assigned strings; disposition one enum; evidence nonempty object list; association nonempty string; remaining string (legacy nonempty-string list preserved)",
    "evidence": "canonical nonempty path/location strings, extra observation fields allowed; legacy public/rich/split codecs remain source-checked by the original receiver",
    "unresolved": "requires a nonempty actual remaining duty; no read/acceptance claim is inferred",
    "public_first": "no private assigned pages; empty pages or indexed public observations; checkpoint object with sha256/tool_readback_location, optional path/sequence",
    "later_checkpoint": "canonical null; exact prior first-checkpoint references retained only through the original seal/byte/anchor validator",
    "boundaries": "progress_only cannot sign whole quality or create final files; final uses the original three-file whole protocol, not progress fields",
    "authority": "structure never proves reading, source truth, independence, normative selection or business acceptance; original IO/owned-context/signoff guards remain mandatory",
}


class QualityContractError(ValueError):
    """A deterministic delivery problem, never a business outcome."""

    def __init__(self, message, *, code="invalid_delivery_structure", field=None,
                 issue_id=None, affected_path=None):
        super().__init__(message)
        self.details = {"code": code, "message": message}
        for key, value in (("field", field), ("issue_id", issue_id),
                           ("affected_path", affected_path)):
            if value is not None:
                self.details[key] = value

    def as_dict(self):
        return dict(self.details)


def nonblocking_observation(row, blocking_ids):
    """Classify an actual issue without changing its signed kind or outcome."""
    if (not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"].strip()
            or row["id"] in blocking_ids):
        return False
    if row.get("kind") == "claim":
        return row.get("blocking", False) is False
    reason = row.get("nonblocking_reason")
    return (row.get("kind") in {"reference", "rubric"} and row.get("blocking") is False
            and row.get("status") == "open_nonblocking" and isinstance(reason, str) and bool(reason.strip())
            and row.get("needs_business_decision", False) is False
            and row.get("disposition") in (None, "observed_unrepaired", "retained"))


def target_path_error(row, blocking_ids, path, candidate_paths, observed_paths):
    """Return the receiver's repair-target error without making a quality judgment."""
    if path in observed_paths and (not nonblocking_observation(row, blocking_ids) or row.get("kind") != "claim"):
        return "Observed paths are only for nonblocking claims, not repair targets"
    if path not in candidate_paths and path not in observed_paths:
        return "Unknown affected candidate path: " + path
    return None


def check_issue_targets(report, issues, context):
    """Check only routing/target membership; source authentication stays with the receiver."""
    routing = report.get("execution_routing") if isinstance(report, dict) else None
    rows = issues.get("issues") if isinstance(issues, dict) else None
    require(isinstance(routing, dict) and isinstance(rows, list), "routing and issue list required")
    blocking = routing.get("blocking_issue_ids")
    strings(blocking, "blocking_issue_ids", unique=True)
    files, observed = context.get("candidate_paths"), context.get("observed_paths")
    source_errors = context.get("source_errors", [])
    strings(source_errors, "source_errors")
    require(not source_errors, "; ".join(source_errors))
    strings(files, "candidate_paths", unique=True)
    strings(observed, "observed_paths", unique=True)
    for index, row in enumerate(rows):
        require(isinstance(row, dict) and text(row.get("id")) and row.get("kind") in ("material", "reference", "rubric", "claim"),
                "issue identity/kind required")
        paths = row.get("affected_paths")
        strings(paths, "affected_paths", nonempty=True, unique=True)
        for path_index, path in enumerate(paths):
            error = target_path_error(row, blocking, path, files, observed)
            if error:
                raise QualityContractError(
                    error + "; issue=" + row["id"] + "; affected_path=" + path,
                    code=("observed_repair_target" if path in observed else "unknown_repair_target"),
                    field=f"/issues/{index}/affected_paths/{path_index}",
                    issue_id=row["id"], affected_path=path,
                )
    return {"status": "target_scope_valid", "quality_accepted": False,
            "limitations": "Only repair-target membership was checked; original source and business quality gates still apply."}


def require(value, message):
    if not value:
        raise QualityContractError(message)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def valid_remaining(value):
    return isinstance(value, str) or (isinstance(value, list) and all(text(x) for x in value))


def strings(value, name, *, nonempty=False, unique=False):
    require(isinstance(value, list) and (not nonempty or value) and all(text(x) for x in value), name + ": string list required")
    require(not unique or len(value) == len(set(value)), name + ": duplicate values")


def make_context(manifest, plan, public_files=None, request="", *, legacy=False):
    """Filter controller metadata; no private plan content is emitted to model."""
    session = manifest["quality_session"]
    index = session["index"]
    phase = session.get("phase", "progress") if legacy else session["phase"]
    assigned = []
    if phase == "progress" and index:
        ids = plan["turns"][index]["page_ids"]
        assigned = [p for p in plan["pages"] if p["id"] in ids]
    marker = re.search(r'<taskflow_native_signoff_protocol sha256="([0-9a-f]{64})">', request)
    return {"schema_version": 1, "phase": phase, "index": index, "delivery_mode": "legacy" if legacy else "canonical",
            "intent_id": manifest["intent_id"], "candidate_binding": manifest["candidate_binding"],
            "assigned_pages": assigned, "public_files": public_files or [],
            "work": manifest["cwd"], "outputs": manifest.get("outputs", []),
            "native_protocol_sha256": marker.group(1) if marker else None,
            "require_native": manifest.get("mode") == "native_role" and bool(marker)}


def template(context):
    """A phase-correct neutral example, never a prefilled reading verdict."""
    first = context["index"] == 0
    pages = []
    if not first and context["assigned_pages"]:
        p = context["assigned_pages"][0]
        pages = [{"id": p["id"], "sha256": p["sha256"], "disposition": "<choose one permitted disposition>",
                  "evidence": [{"path": p["path"], "location": "<actual visible source readback>"}],
                  "association": "<actual source/responsibility relation>",
                  "remaining": "<actual still-open duty; empty string only if none>"}]
    return {"schema_version": 1, "kind": PROGRESS_KIND, "intent_id": context["intent_id"],
            "candidate_binding": context["candidate_binding"], "segment_index": context["index"],
            "status": "progress_only", "pages": pages, "facts": [], "remaining": [],
            "public_checkpoint": {"sha256": "<actual saved checkpoint SHA256>",
                                  "tool_readback_location": "<actual visible SHA readback>"} if first else None}


def render(context):
    """One finite contract renders all field/type rules and the phase example."""
    if context["phase"] == "final":
        return ("This is the one ordinary final whole review, not progress. Deliver the existing intake.md, issues.json "
                "and adoption-readiness.json using the original execution_routing/native_signoff protocol already in this request. "
                "Choose accepted, revision_required or inconclusive truthfully; structure checks never choose an outcome. "
                "Preserve unresolved duties and actual source/coverage limits. Run the local final preflight before delivery.")
    return ("Finite serial-quality output contract (replace all neutral placeholders truthfully):\n"
            + json.dumps({"fields": FIELD_CONTRACT, "top_fields": sorted(TOP_FIELDS),
                          "page_fields": sorted(PAGE_FIELDS), "dispositions": DISPOSITIONS,
                          "example": template(context)}, ensure_ascii=False)
            + '\nEvidence must be a nonempty list of records, each with nonempty string fields "path" and "location"; rich observations may be additional fields. '
            + "Each page's remaining must be a string; The separate top-level remaining remains a list, and top-level facts remains a list. "
            + "Before delivery, check every evidence record and check every page's complete fields and types. "
            + ("Neutral complete page record (placeholders only) is shown in example.pages. " if context["index"] else "")
            + "Same-byte reuse needs actual earlier reading in this SID; nondependency claims need actual current-use evidence. "
            + ("First public_checkpoint must be the saved object and real visible SHA anchor, not null. Do not read private plan/queue in this first turn."
               if context["index"] == 0 else "For this later progress receipt public_checkpoint must be null; cite the sealed first checkpoint without replacing it."))


def validate_evidence_shape(entries, *, legacy=False):
    require(isinstance(entries, list) and entries and all(isinstance(e, dict) and e for e in entries),
            "evidence: nonempty object list required")
    if all(text(e.get("path")) and text(e.get("location")) for e in entries):
        return
    # New requested deliveries use canonical fields; old frozen originals retain
    # finite codecs whose remaining shape/source checks stay in the IO receiver.
    require(legacy, "evidence: new delivery requires nonempty path/location fields")
    require(any("plan_page" in e for e in entries) or any("body_page" in e for e in entries),
            "evidence: canonical path/location or a supported legacy locator required")


def validate_progress(result, context):
    require(isinstance(result, dict), "progress: object required")
    require(set(result) == TOP_FIELDS, "progress top fields differ: missing="
            + str(sorted(TOP_FIELDS - set(result))) + " extra=" + str(sorted(set(result) - TOP_FIELDS)))
    require(type(result["schema_version"]) is int and result["schema_version"] == 1,
            "schema_version: integer 1 required")
    for name, expected in (("kind", PROGRESS_KIND), ("status", "progress_only"),
                           ("intent_id", context["intent_id"]), ("candidate_binding", context["candidate_binding"])):
        require(result[name] == expected, name + ": differs from locked unfinished reading receipt")
    require(type(result["segment_index"]) is int and result["segment_index"] == context["index"],
            "segment_index: exact integer turn index required")
    for name in ("facts", "remaining", "pages"):
        require(isinstance(result[name], list), name + ": list required")
    require(all(isinstance(p, dict) for p in result["pages"]), "pages: every record must be an object")
    pages = result["pages"]
    if context["index"] == 0 and pages:
        fields = {"path", "role", "sha256", "size", "read", "sha256_verified", "size_verified"}
        expected = {p["path"]: p for p in context["public_files"]}
        seen = []
        for p in pages:
            require(set(p) == fields and text(p.get("path")) and p.get("role") in ("prompt", "input", "template")
                    and text(p.get("sha256")) and type(p.get("size")) is int and p["size"] >= 0
                    and text(p.get("read")) and type(p.get("sha256_verified")) is bool
                    and type(p.get("size_verified")) is bool, "public observation needs exact indexed identity and explicit reading claim")
            if expected:
                require(p["path"] in expected and all(p[k] == expected[p["path"]][k] for k in ("role", "sha256", "size")),
                        "public observation differs from indexed identity")
            seen.append(p["path"])
        require(len(seen) == len(set(seen)) and (not expected or set(seen) == set(expected)),
                "public observations must cover the complete public index exactly once")
    else:
        assigned = {p["id"]: p for p in context["assigned_pages"]}
        require(all(text(p.get("id")) for p in pages) and len(pages) == len(assigned)
                and {p["id"] for p in pages} == set(assigned),
                "every assigned page needs an explicit disposition, including unresolved pages")
        for index, p in enumerate(pages):
            require(set(p) == PAGE_FIELDS and p.get("sha256") == assigned[p["id"]]["sha256"]
                    and isinstance(p.get("disposition"), str) and p["disposition"] in DISPOSITIONS
                    and text(p.get("association")) and valid_remaining(p.get("remaining")),
                    "page disposition needs exact bytes and substantive evidence/association at pages[" + str(index) + "]")
            require(context.get("delivery_mode") == "legacy" or isinstance(p["remaining"], str),
                    "pages[" + str(index) + "].remaining: canonical output requires a string")
            validate_evidence_shape(p.get("evidence"), legacy=context.get("delivery_mode") == "legacy")
            if p["disposition"] == "unresolved":
                require(bool(p["remaining"].strip()) if isinstance(p["remaining"], str) else bool(p["remaining"]),
                        "unresolved page needs its actual unread responsibility")
    checkpoint = result["public_checkpoint"]
    if context["index"] == 0 or checkpoint is not None:
        require(isinstance(checkpoint, dict) and {"sha256", "tool_readback_location"} <= set(checkpoint)
                and set(checkpoint) <= {"sha256", "tool_readback_location", "path", "sequence"}
                and text(checkpoint.get("sha256")) and isinstance(checkpoint.get("tool_readback_location"), str)
                and bool(checkpoint["tool_readback_location"]), "fresh public-first original and visible readback required")
        for key in ("path", "sequence"):
            require(key not in checkpoint or text(checkpoint[key]), "checkpoint optional location/sequence must be text")
    return result


def citations(rows, name):
    require(isinstance(rows, list) and rows, name + ": nonempty citation list required")
    for row in rows:
        require(isinstance(row, dict) and text(row.get("path")) and text(row.get("location"))
                and isinstance(row.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", row["sha256"]),
                name + ": path/SHA256/location required")


def validate_final(report, issues, context):
    """Pure requested report shape; original source/coverage/business gates follow."""
    require(isinstance(report, dict) and isinstance(issues, dict), "final reports must be objects")
    routing = report.get("execution_routing")
    require(isinstance(routing, dict) and type(routing.get("schema_version")) is int
            and routing["schema_version"] == 1, "execution_routing.schema_version must be 1")
    expected = {"intent_id": context["intent_id"], **{k:v for k,v in context["candidate_binding"].items() if k != "task_id"}}
    for name, binding in (("execution_routing", routing), ("execution_binding", issues.get("execution_binding"))):
        require(isinstance(binding, dict) and all(binding.get(k) == v for k,v in expected.items())
                and ("task_id" not in binding or binding["task_id"] == context["candidate_binding"]["task_id"]), name + ": current binding required")
    require(routing.get("signer_role") == "independent_quality" and routing.get("independent_of_current_author") is True
            and routing.get("participated_in_production") is False and routing.get("scope") == context.get("scope", "whole_package"),
            "independent authorized role declaration required")
    outcome = routing.get("outcome")
    require(outcome in ("accepted", "revision_required", "inconclusive"), "unknown business routing outcome")
    strings(routing.get("blocking_issue_ids"), "blocking_issue_ids", unique=True)
    strings(routing.get("covered_defect_ids"), "covered_defect_ids", unique=True)
    if "defect_ids" in context:
        require(set(routing["covered_defect_ids"]) == set(context["defect_ids"]), "explicit assigned defect coverage differs")
    require(text(routing.get("reason")), "routing reason required")
    require(not (outcome == "accepted" and routing["blocking_issue_ids"])
            and not (outcome == "revision_required" and not routing["blocking_issue_ids"]), "routing/blocking IDs conflict")
    legacy = report.get("candidate_business_quality")
    if legacy is not None:
        outcomes = {"needs_author_revision":"revision_required", "failed":"revision_required",
                    "passed":"accepted", "accepted":"accepted", "revision_required":"revision_required",
                    "inconclusive":"inconclusive"}
        require(isinstance(legacy, dict) and isinstance(legacy.get("status"), str)
                and outcomes.get(legacy["status"]) == outcome, "legacy business status contradicts routing")
        if "blocking_issue_ids" in legacy:
            strings(legacy["blocking_issue_ids"], "legacy blocking IDs", unique=True)
            require(set(legacy["blocking_issue_ids"]) == set(routing["blocking_issue_ids"]), "legacy blocking IDs conflict")
    rows = issues.get("issues")
    require(isinstance(rows, list), "issues: list required")
    ids = []
    for row in rows:
        require(isinstance(row, dict) and text(row.get("id")) and row.get("kind") in ("material", "reference", "rubric", "claim")
                and text(row.get("responsibility")), "issue identity/kind/responsibility required")
        strings(row.get("affected_paths"), "affected_paths", nonempty=True, unique=True)
        citations(row.get("evidence"), "issue evidence")
        ids.append(row["id"])
    require(len(ids) == len(set(ids)) and set(routing["blocking_issue_ids"]) <= set(ids), "blocking ID has no unique issue record")
    signed = report.get("native_signoff")
    if signed is None and not context.get("require_native"):
        return
    require(isinstance(signed, dict) and type(signed.get("schema_version")) is int and signed["schema_version"] == 1
            and signed.get("protocol_sha256") == context.get("native_protocol_sha256")
            and signed.get("coverage") in (("affected",) if context.get("scope") == "affected" else ("whole_package", "unchanged_scope")), "native signoff version/protocol/coverage differs")
    strings(signed.get("affected_paths"), "affected_paths", unique=True)
    require(text(signed.get("coverage_basis")), "native coverage basis required")
    dimensions = signed.get("dimensions")
    require(isinstance(dimensions, dict) and set(dimensions) == {"business", "artifact", "calculation"}, "three native dimensions required")
    for value in dimensions.values():
        require(isinstance(value, dict) and type(value.get("affirmed")) is bool, "dimension affirmation must be boolean")
        citations(value.get("evidence"), "dimension evidence")
        strings(value.get("limitations"), "dimension limitations")
    normative = signed.get("normative")
    require(isinstance(normative, dict) and normative.get("status") in ("determined", "needs_business_decision")
            and isinstance(normative.get("files"), list), "normative declaration required")
    citations(normative.get("evidence"), "normative evidence")
    strings(normative.get("limitations"), "normative limitations")
    for row in normative["files"]:
        require(isinstance(row, dict) and all(text(row.get(k)) for k in ("package_path", "role", "basis", "sha256"))
                and type(row.get("size")) is int and row["size"] >= 0
                and row.get("purpose") in ("answer", "rubric", "reference", "legitimate_alternative"), "normative source shape differs")
    analysis = signed.get("responsibility_analysis")
    require(isinstance(analysis, list) and analysis, "responsibility analysis required")
    for row in analysis:
        require(isinstance(row, dict) and row.get("disposition") in ("retained", "blocking_defect", "unresolved")
                and all(text(row.get(k)) for k in ("upstream_work_already_done", "remaining_judgment", "consequence")), "responsibility shape differs")
        citations(row.get("public_evidence"), "public responsibility evidence")
        for key in ("retained_necessary_conditions", "legitimate_branches"):
            strings(row.get(key), key, nonempty=True)
        strings(row.get("blocking_issue_ids"), "responsibility blocking IDs")


def read_json(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= 16 * 1024 * 1024, "output file too large")
    return json.loads(raw)


def check_delivery(context, directory):
    directory = Path(directory)
    if context["phase"] == "progress":
        require(not any((directory / name).exists() for name in FINAL_FILES), "a progress turn must not issue a quality signoff")
        result = read_json(directory / ("reading-progress-" + str(context["index"]).zfill(3) + ".json"))
        validate_progress(result, context)
        if context["index"] == 0:
            path = directory / "public-responsibility-first.json"
            require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == result["public_checkpoint"]["sha256"],
                    "checkpoint file/SHA differs; visible tool readback is still required")
    else:
        require((directory / "intake.md").is_file(), "final intake.md missing")
        report = read_json(directory / "adoption-readiness.json")
        issues = read_json(directory / "issues.json")
        validate_final(report, issues, context)
        if "candidate_paths" in context or "observed_paths" in context:
            check_issue_targets(report, issues, context)
    return {"status": "structure_valid", "phase": context["phase"], "quality_accepted": False,
            "limitations": "Source bytes, actual readbacks, same-SID capture, normative/business signoff still require original gates."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--directory")
    parser.add_argument("--target-scope-only", action="store_true")
    parser.add_argument("--intent-id", help="current invocation identity; never changes report bytes")
    args = parser.parse_args()
    try:
        context = read_json(args.context)
        if args.intent_id is not None:
            require(text(args.intent_id), "current intent ID must be nonempty")
            context = {**context, "intent_id": args.intent_id}
        directory = Path(args.directory or context["work"])
        result = (check_issue_targets(read_json(directory / "adoption-readiness.json"),
                                      read_json(directory / "issues.json"), context)
                  if args.target_scope_only else check_delivery(context, directory))
        print(json.dumps(result))
    except (ValueError, OSError, KeyError, TypeError) as error:
        details = error.as_dict() if isinstance(error, QualityContractError) else {"code": "invalid_delivery_structure", "message": str(error)}
        parser.exit(2, json.dumps({"status": "invalid_delivery_structure", "error": details, "quality_accepted": False}) + "\n")


if __name__ == "__main__":
    main()
