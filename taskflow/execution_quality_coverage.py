"""Explicit revision coverage, authenticated by the normal independent signature.

Paths and byte differences establish coverage obligations; the reviewer decides
business units, necessary associations and whether the proposed scope isolates.
No historical signature is restamped and no whole-package verdict is composed.
"""

import json
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, relative_path, sha256
from taskflow.execution_quality_contracts import candidate_binding
from taskflow.execution_runner import _info, _read, _regular
from taskflow.execution_store import validate_evidence


def _paths(value, name: str, allowed: set[str], *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise TaskflowError(name + " requires an explicit path list")
    paths = [relative_path(item) for item in value]
    if len(paths) != len(set(paths)) or not set(paths) <= allowed:
        raise TaskflowError(name + " has duplicate or undeclared paths")
    return paths


def _units(value, name: str, allowed: set[str]) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise TaskflowError(name + " requires explicit business units/associations")
    result = []
    for row in value:
        if (not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"].strip()
                or not isinstance(row.get("responsibility"), str) or not row["responsibility"].strip()):
            raise TaskflowError(name + " needs an ID and concrete responsibility")
        result.append({"id": row["id"], "responsibility": row["responsibility"],
                       "paths": _paths(row.get("paths"), name + ".paths", allowed, nonempty=True)})
    if len({row["id"] for row in result}) != len(result):
        raise TaskflowError(name + " IDs must be unique")
    return result


def coverage_plan(store, spec: dict, candidate: dict) -> dict:
    """Verify the declared inheritance and actual revision before model dispatch."""
    from taskflow.execution_revision import validate_revision_source
    from taskflow.execution_solver_contracts import quality_source

    proposed = spec.get("coverage")
    if not isinstance(proposed, dict):
        raise TaskflowError("affected quality requires an explicit coverage proposal")
    revision = validate_revision_source(store, proposed.get("revision_task_id"))
    if (revision["candidate"]["candidate_id"] != candidate["candidate_id"]
            or str(revision["root"]) != spec["candidate_root"]):
        raise TaskflowError("affected quality revision source is not this exact candidate/root")
    if sorted(spec.get("defect_ids", [])) != sorted(revision["record"]["definition"].get("defect_ids", [])):
        raise TaskflowError("affected quality must retain the revision explicit defect IDs")
    parent = revision["parent"]
    previous = store.get_flow(proposed.get("parent_quality_task_id"))
    definition = previous["definition"]
    if definition.get("candidate_id") != parent["candidate_id"]:
        raise TaskflowError("inherited quality must identify the exact revision parent")
    source = quality_source(store, {"candidate_id": parent["candidate_id"], "task_id": parent["task_id"],
        "candidate_root": definition.get("candidate_root"), "quality_task_id": previous["flow_id"],
        "runner": {"mode": spec["runner"].get("mode")}}, _check_gate=False, _allow_findings=True)
    before = {row["package_path"]: row for row in parent["files"]}
    after = {row["package_path"]: row for row in candidate["files"]}
    allowed = set(before) | set(after)
    inherited = _paths(proposed.get("inherited_paths"), "inherited_paths", set(after))
    recheck = _paths(proposed.get("recheck_paths"), "recheck_paths", allowed, nonempty=True)
    if set(inherited) & set(recheck) or set(inherited) | set(recheck) != allowed:
        raise TaskflowError("inheritance and recheck must explicitly partition every current/removed member")
    for name in inherited:
        if name not in before or any(before[name].get(key) != after[name].get(key) for key in ("role", "sha256", "size")):
            raise TaskflowError("changed member cannot inherit old quality: " + name)
    author_impact = revision["repair_record"]["impact_analysis"]
    author_paths = _paths(author_impact["associated_paths"], "author associated responsibilities", allowed)
    if not set(author_paths) <= set(recheck):
        raise TaskflowError("affected proposal omits the original author associated responsibilities")
    old_report = _read(Path(source["attempt"]["runtime_path"]) / "artifacts" / definition["report_file"])
    native = old_report["native_signoff"]
    if native["coverage"] == "unchanged_scope":
        if not set(native["affected_paths"]) <= set(recheck):
            raise TaskflowError("parent excluded responsibilities must be reopened in the current recheck")
        if inherited and any(row["affirmed"] is not True for row in native["dimensions"].values()):
            raise TaskflowError("parent unchanged scope lacks actual affirmative dimensions")
    elif inherited and source["quality"]["state"]["outcome"] != "accepted":
        raise TaskflowError("parent findings do not provide a signed unchanged inheritance scope")
    units = _units(proposed.get("business_units"), "business_units", set(recheck))
    if set().union(*(set(row["paths"]) for row in units)) != set(recheck):
        raise TaskflowError("complete business units must cover the entire declared recheck")
    related = _units(proposed.get("related_checks"), "related_checks", set(recheck))
    reopened = proposed.get("reopened_evidence", [])
    if not isinstance(reopened, list):
        raise TaskflowError("reopened_evidence must be an explicit list")
    evidence = []
    for row in reopened:
        if not isinstance(row, dict) or not isinstance(row.get("responsibility"), str) or not row["responsibility"].strip():
            raise TaskflowError("new evidence must retain its concrete reopened responsibility")
        ref = validate_evidence([row.get("evidence")])[0]
        evidence.append({"evidence": ref, "responsibility": row["responsibility"],
            "paths": _paths(row.get("paths"), "new evidence reopened paths", set(recheck), nonempty=True)})
    declaration = revision["diff"].get("declaration_change")
    declaration_check = None
    if declaration is not None:
        declaration_check = _units([proposed.get("declaration_check")], "declaration_check", set(recheck))[0]
    plan = {"schema_version": 1, "revision_task_id": proposed["revision_task_id"],
        "parent_quality_task_id": previous["flow_id"], "parent_outcome": source["quality"]["state"]["outcome"],
        "public_review_mode": "informed_recheck" if spec["runner"]["role_session"]["mode"] == "resume_existing" else "fresh_public_first",
        "defect_ids": spec.get("defect_ids", []), "parent_binding": candidate_binding(parent),
        "current_binding": candidate_binding(candidate), "author_impact": author_impact, "inherited_paths": inherited,
        "recheck_paths": recheck, "business_units": units, "related_checks": related,
        "reopened_evidence": evidence, "declaration_change": declaration, "declaration_check": declaration_check, "diff_sha256": _info(Path(revision["record"]["state"]["diff"]["path"]))["sha256"]}
    return {"plan": plan, "plan_sha256": sha256(canonical(plan)), "revision": revision,
            "parent_source": source, "old_report": old_report}


def recheck_session(coverage: dict, spec: dict) -> dict | None:
    """Bind business reinspection to the precise original QA, never a public solver."""
    session = spec["runner"]["role_session"]
    if session["mode"] != "resume_existing":
        return None
    source = coverage["parent_source"]
    original = source["manifest"]
    root = Path(source["attempt"]["runtime_path"])
    previous = session.get("previous")
    expected = {"path": str(root / "collection.json"), **_info(root / "collection.json")}
    if (not isinstance(previous, dict) or any(previous.get(key) != value for key, value in expected.items())
            or source["quality"]["definition"]["owner"] != spec["owner"]
            or original.get("role_lineage") != spec["runner"]["role_lineage"]
            or original.get("role_session", {}).get("home") != session["home"]
            or original.get("role_session", {}).get("mode") not in {"fresh_persistent", "resume_existing"}):
        raise TaskflowError("affected resume requires its exact original persistent QA collection, owner and home")
    return source


def coverage_inputs(coverage: dict, shared: Path, publish) -> list[dict]:
    """Stage precise old/new locators and original private evidence for normal review."""
    revision, plan = coverage["revision"], coverage["plan"]
    parent = revision["parent"]
    files = {row["package_path"]: row for row in parent["files"]}
    inputs = []
    changed = []
    for row in revision["diff"]["changes"]:
        name = row["path"]
        old, new = row["before"], row["after"]
        previous = "execution/parent-inputs/" + name if old is not None else None
        if previous:
            inputs.append({"path": files[name]["path"], "kind": "history", "target": previous})
        changed.append({"path": name, "old_sha256": old["sha256"] if old else None,
            "new_sha256": new["sha256"] if new else None, "old_role": old["role"] if old else None,
            "new_role": new["role"] if new else None, "before_staged_path": previous,
            "current_staged_path": name if new else None})
    index = {"available": True, "parent_binding": plan["parent_binding"],
             "current_binding": plan["current_binding"], "changed": changed,
             "declaration_change": revision["diff"].get("declaration_change")}
    if plan["declaration_change"] is not None:
        previous = Path(revision["record"]["state"]["invocation"]["parent_root"]) / "task.json"
        if _info(previous) != plan["declaration_change"]["before"]:
            raise TaskflowError("parent declaration differs from actual revision source")
        inputs.append({"path": str(previous), "kind": "history", "target": "execution/parent-task.json"})
    values = {"revision-coverage.json": {**plan, "plan_sha256": coverage["plan_sha256"]},
              "revision-diff.json": index, "parent-quality-signoff.json": coverage["old_report"]}
    for name, value in values.items():
        path = shared / name
        publish(path, value)
        inputs.append({"path": str(path), "kind": "history", "target": "execution/" + name})
    author = revision["diff"]["author_record"]
    inputs.append({"path": author["path"], "kind": "history", "target": "execution/revision-author.json"})
    parent_source = coverage["parent_source"]
    parent_definition = parent_source["quality"]["definition"]
    parent_root = Path(parent_source["attempt"]["runtime_path"])
    inputs.append({"path": str(parent_root / "artifacts" / parent_definition["issues_file"]),
                   "kind": "history", "target": "execution/parent-quality-issues.json"})
    if plan["public_review_mode"] == "informed_recheck":
        original = parent_source["quality"]["state"]["invocations"][0]
        inputs.append({"path": str(Path(original["runtime"]) / "artifacts/public-responsibility-first.json"),
                       "kind": "history", "target": "execution/parent-public-checkpoint.json"})
    for index, row in enumerate(plan["reopened_evidence"]):
        inputs.append({"path": row["evidence"]["path"], "kind": "history", "target": f"execution/reopened/{index:03d}"})
    return inputs


def coverage_instructions(coverage: dict, *, compact: bool = False) -> str:
    plan = coverage["plan"]
    if compact:
        return (
            "Read execution/revision-coverage.json, revision-diff.json, parent-quality-signoff.json, "
            "parent-quality-issues.json and the actual old/current originals they index. Confirm complete current business units, "
            "cross-file checks, reopened evidence and actual differences. The parent outcome remains unchanged; absent affirmative "
            "old scope provides no inheritance. If influence cannot be isolated, sign inconclusive and request whole review. "
            "In adoption-readiness.json add revision_coverage with schema_version=1, revision_task_id, parent_quality_task_id "
            "and plan_sha256 copied from the locked plan, scope_confirmed boolean and concrete isolation_basis; "
            "checked_business_unit_ids/checked_related_check_ids cover all plan IDs, reopened_evidence_sha256 covers every "
            "new evidence source. business_unit_checks/related_check_evidence/reopened_check_evidence each use {id,evidence:[{path,sha256,location}]} "
            "and cite every declared complete path, including new evidence. native_signoff.coverage=affected and affected_paths "
            "equals every current recheck path. coverage.changed_files=[{path,old,new}] covers all actual payload changes "
            "with exact SHA or null; defect_resolution.evidence cites each current changed file or indexed deleted original. "
            "If declaration_change is present, retain coverage.declaration_change={old,new}, declaration_check_id and "
            "declaration_check_evidence in revision_coverage; read and cite both execution/parent-task.json and execution/task.json "
            "plus complete associated business units. Locations are actual L1[-L2] or half-open B0-Bn. "
            "Current rereading and this affected signature do not restamp the old signature or close unassigned blockers.")
    contract = {"schema_version": 1, "revision_task_id": plan["revision_task_id"],
        "parent_quality_task_id": plan["parent_quality_task_id"], "plan_sha256": coverage["plan_sha256"],
        "scope_confirmed": True, "isolation_basis": "Actual business explanation and limits; if not isolatable sign inconclusive and request whole review.",
        "checked_business_unit_ids": [row["id"] for row in plan["business_units"]],
        "checked_related_check_ids": [row["id"] for row in plan["related_checks"]],
        "reopened_evidence_sha256": [row["evidence"]["sha256"] for row in plan["reopened_evidence"]],
        "declaration_check_id": plan["declaration_check"]["id"] if plan["declaration_check"] else None,
        "declaration_check_evidence": ([{"id": plan["declaration_check"]["id"], "evidence": [{"path": "actual source", "sha256": "actual SHA", "location": "L1 or actual B0-Bn"}]}] if plan["declaration_check"] else []),
        "reopened_check_evidence": [{"id": row["evidence"]["sha256"], "evidence": [{"path": f"execution/reopened/{index:03d}", "sha256": row["evidence"]["sha256"], "location": "L1 or actual B0-Bn"}]} for index, row in enumerate(plan["reopened_evidence"])],
        "business_unit_checks": [{"id": row["id"], "evidence": [{"path": "actual source", "sha256": "actual SHA", "location": "L1 or actual B0-Bn"}]} for row in plan["business_units"]],
        "related_check_evidence": [{"id": row["id"], "evidence": [{"path": "actual source", "sha256": "actual SHA", "location": "L1 or actual B0-Bn"}]} for row in plan["related_checks"]]}
    return ("After the public checkpoint read execution/revision-coverage.json and the original sources it indexes. "
        "The author proposes scope; independently confirm complete business units, necessary cross-file relations and new evidence "
        "during this normal review. Expand the proposal with a new explicit whole task if influence cannot be isolated; "
        "do not accept merely because a file is unchanged. Read execution/parent-quality-issues.json and preserve every old finding "
        "and the actual parent outcome; a revision_required parent is not accepted. Keep old signatures with old candidate/SID/time. "
        "Do not copy old conclusions as a new whole signoff. In adoption-readiness.json retain revision_coverage="
        + json.dumps(contract, ensure_ascii=False)
        + ". Retain coverage.changed_files=[{path,old,new}] for every actual payload difference, using exact before/after SHA or null. "
        "native_signoff.affected_paths must cover all current recheck paths; cite the actual current or indexed deleted originals "
        "in defect_resolution.evidence for every changed member. Independently select the entire current normative allowlist. "
        "A confirmed scope and accepted affected result establish only this version's declared inheritance and recheck; "
        "unclosed blockers remain blockers. Each business_unit_checks and related_check_evidence entry must cite "
        "every declared path with actual SHA and L1[-L2] or half-open B0-Bn location. When declaration_change is present, actually read "
        "execution/parent-task.json and execution/task.json, retain coverage.declaration_change={old,new} "
        "with their exact hashes, and cite both in defect_resolution.evidence; independently check declaration_check "
        "against its complete associated business paths. Preserve actual uncertainty.")


def validate_coverage_signature(report: dict, coverage: dict, candidate: dict, manifest: dict, root: Path) -> None:
    """Check exact original signature/plan/diff association without inferring impact."""
    if not isinstance(coverage, dict):
        raise TaskflowError("affected quality has no authenticated coverage plan")
    plan = coverage["plan"]
    signed = report.get("revision_coverage")
    expected = {"schema_version": 1, "revision_task_id": plan["revision_task_id"],
        "parent_quality_task_id": plan["parent_quality_task_id"], "plan_sha256": coverage["plan_sha256"]}
    if (not isinstance(signed, dict) or any(signed.get(key) != value for key, value in expected.items())
            or type(signed.get("scope_confirmed")) is not bool or not isinstance(signed.get("isolation_basis"), str)
            or not signed["isolation_basis"].strip()):
        raise TaskflowError("affected original did not bind its exact inheritance/recheck scope")
    if report["execution_routing"]["outcome"] != "accepted":
        return
    if signed["scope_confirmed"] is not True:
        raise TaskflowError("unconfirmed influence cannot admit affected quality")
    for field, expected in (("checked_business_unit_ids", [row["id"] for row in plan["business_units"]]),
                            ("checked_related_check_ids", [row["id"] for row in plan["related_checks"]]),
                            ("reopened_evidence_sha256", [row["evidence"]["sha256"] for row in plan["reopened_evidence"]])):
        actual = signed.get(field)
        if not isinstance(actual, list) or sorted(actual) != sorted(expected):
            raise TaskflowError("affected signature omits a declared unit, association or reopened evidence: " + field)
    current_paths = {row["package_path"] for row in candidate["files"]}
    if set(report["native_signoff"]["affected_paths"]) != set(plan["recheck_paths"]) & current_paths:
        raise TaskflowError("affected native scope differs from complete current recheck responsibilities")
    expected_changes = [{"path": row["path"], "old": row["before"]["sha256"] if row["before"] else None,
                         "new": row["after"]["sha256"] if row["after"] else None}
                        for row in coverage["revision"]["diff"]["changes"]]
    changed = report.get("coverage", {}).get("changed_files")
    if not isinstance(changed, list) or sorted(changed, key=lambda row: row["path"]) != sorted(expected_changes, key=lambda row: row["path"]):
        raise TaskflowError("affected signed actual differences omit or alter payload changes")
    citations = report.get("defect_resolution", {}).get("evidence")
    if not isinstance(citations, list):
        raise TaskflowError("affected review needs original changed-member evidence")
    sources = {row["target"]: row for row in manifest["inputs"]}

    def position(row, source):
        from taskflow.execution_assessment_contracts import AssessmentContractError, _location

        try:
            _location(row["location"], {**source, "raw": _regular(root / "inputs" / row["path"])}, "affected evidence location")
        except AssessmentContractError as error:
            raise TaskflowError(str(error)) from error

    cited = set()
    for row in citations:
        if not isinstance(row, dict) or not isinstance(row.get("location"), str) or not row["location"].strip():
            raise TaskflowError("affected evidence needs exact source and location")
        name = relative_path(row.get("path"))
        source = sources.get(name)
        if source is None or row.get("sha256") != source["sha256"] or _info(root / "inputs" / name) != {key: source[key] for key in ("sha256", "size")}:
            raise TaskflowError("affected changed-member evidence differs from its locked original")
        position(row, source)
        cited.add((name, row["sha256"]))
    for row in expected_changes:
        name = row["path"] if row["new"] else "execution/parent-inputs/" + row["path"]
        if (name, row["new"] or row["old"]) not in cited:
            raise TaskflowError("affected original omitted actual changed-member evidence: " + row["path"])
    def unit_evidence(field: str, units: list[dict]) -> None:
        entries = signed.get(field)
        if not isinstance(entries, list) or any(not isinstance(row, dict) for row in entries):
            raise TaskflowError("affected unit/association original evidence required: " + field)
        mapped = {row.get("id"): row for row in entries}
        if len(mapped) != len(entries) or set(mapped) != {row["id"] for row in units}:
            raise TaskflowError("affected unit/association evidence IDs differ: " + field)
        for unit in units:
            positions = mapped[unit["id"]].get("evidence")
            if not isinstance(positions, list) or not positions:
                raise TaskflowError("affected unit/association needs original positions: " + unit["id"])
            covered = set()
            for citation in positions:
                if not isinstance(citation, dict) or not isinstance(citation.get("location"), str) or not citation["location"].strip():
                    raise TaskflowError("affected unit original location required")
                name = relative_path(citation.get("path"))
                source = sources.get(name)
                if source is None or citation.get("sha256") != source["sha256"] or _info(root / "inputs" / name) != {key: source[key] for key in ("sha256", "size")}:
                    raise TaskflowError("affected unit evidence differs from its locked original")
                position(citation, source)
                covered.add(name)
            expected = {name if name in sources else "execution/parent-inputs/" + name for name in unit["paths"]}
            if not expected <= covered:
                raise TaskflowError("affected unit evidence omitted a complete associated original: " + unit["id"])

    unit_evidence("business_unit_checks", plan["business_units"])
    unit_evidence("related_check_evidence", plan["related_checks"])
    unit_evidence("reopened_check_evidence", [{"id": row["evidence"]["sha256"],
        "paths": [f"execution/reopened/{index:03d}", *row["paths"]]} for index, row in enumerate(plan["reopened_evidence"])])
    declaration = plan["declaration_change"]
    if declaration is not None:
        expected = {"old": declaration["before"]["sha256"], "new": declaration["after"]["sha256"]}
        if (report.get("coverage", {}).get("declaration_change") != expected
                or signed.get("declaration_check_id") != plan["declaration_check"]["id"]
                or not {("execution/parent-task.json", expected["old"]), ("execution/task.json", expected["new"])} <= cited):
            raise TaskflowError("affected declaration/scoring change lacks exact old/current original and association check")
        unit_evidence("declaration_check_evidence", [plan["declaration_check"]])


class _ReadStore:
    """Narrow read-only adapter for validating sources under an existing DB gate."""

    def __init__(self, db):
        self.db = db

    def get_flow(self, identity):
        from taskflow.execution_store import ExecutionStore

        row = self.db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (identity,)).fetchone()
        return ExecutionStore._flow_value(row)

    def _get(self, table, identity):
        from taskflow.execution_store import ExecutionStore

        return ExecutionStore._load(self.db, table, identity)

    def get_candidate(self, identity):
        return self._get("candidates", identity)

    def get_attempt(self, identity):
        return self._get("attempts", identity)

    def get_work(self, identity):
        return self._get("work", identity)

    def get_review(self, identity):
        return self._get("reviews", identity)

    def get_defect(self, identity):
        return self._get("defects", identity)


def is_affected_quality_review(db, review: dict) -> bool:
    row = db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (review.get("scope"),)).fetchone()
    if row is None:
        return False
    definition = json.loads(row["definition"])
    return definition.get("kind") == "quality_task_v1" and definition.get("scope") == "affected"


def validate_affected_quality_gate(db, review: dict) -> dict:
    """Validate a current affected endpoint; the store still checks all blockers."""
    from taskflow.execution_solver_contracts import quality_source

    store = _ReadStore(db)
    record = store.get_flow(review["scope"])
    definition = record["definition"]
    source = quality_source(store, {"candidate_id": definition["candidate_id"], "task_id": definition["task_id"],
        "candidate_root": definition["candidate_root"], "quality_task_id": record["flow_id"],
        "runner": {"mode": definition["runner"].get("mode")}}, _check_gate=False)
    if source["review"] != review:
        raise TaskflowError("affected quality gate differs from its actual current original review")
    return source["review"]


def validate_affected_defect_closure(db, review: dict, defect: dict) -> None:
    """Admit one explicit repaired defect only through this normal current signoff."""
    from taskflow.execution_solver_contracts import quality_source

    validate_affected_quality_gate(db, review)
    store = _ReadStore(db)
    record = store.get_flow(review["scope"])
    definition = record["definition"]
    candidate = store.get_candidate(definition["candidate_id"])
    coverage = coverage_plan(store, definition, candidate)
    identity = defect["defect_id"]
    if (identity not in definition.get("defect_ids", []) or identity not in review.get("covered_defect_ids", [])
            or defect.get("repair_candidate_id") != candidate["candidate_id"]):
        raise TaskflowError("affected closure lacks the exact explicitly assigned repaired defect")
    original_review = store.get_review(defect["review_id"])
    original = store.get_flow(original_review["scope"])
    old_definition = original["definition"]
    source = quality_source(store, {"candidate_id": old_definition["candidate_id"], "task_id": old_definition["task_id"],
        "candidate_root": old_definition["candidate_root"], "quality_task_id": original["flow_id"],
        "runner": {"mode": definition["runner"].get("mode")}}, _check_gate=False, _allow_findings=True)
    if source["review"] != original_review:
        raise TaskflowError("affected closure original finding is not its exact independent signature")
    issues = _read(Path(source["attempt"]["runtime_path"]) / "artifacts" / old_definition["issues_file"])
    rows = [row for row in issues["issues"] if "quality-defect-" + sha256(canonical([source["attempt"]["work_id"], row["id"]]))[:24] == identity]
    if len(rows) != 1 or rows[0]["responsibility"] != defect["responsibility"]:
        raise TaskflowError("affected closure cannot authenticate the original issue responsibility")
    if not set(rows[0]["affected_paths"]) <= set(coverage["plan"]["recheck_paths"]):
        raise TaskflowError("affected closure omitted original defect responsibilities and associations")
