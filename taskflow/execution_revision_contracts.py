"""Evidence contracts for revision handoffs and responsibility-level analysis.

These checks preserve unknowns and validate citations, not the author's business
judgment. A source being present is deliberately distinct from observed use.
"""

import json
from pathlib import Path

from taskflow.contracts import TaskflowError, relative_path
from taskflow.execution_store import validate_evidence


HISTORY_ROLES = frozenset({
    "request", "tools", "tool_calls", "tool_results", "code", "intermediate", "answer", "facts", "review", "other",
})
HISTORY_STATES = frozenset({"unknown", "does_not_exist", "archived_not_provided", "provided"})
CHAIN_FIELDS = ("public_work", "processing", "final_result", "business_consequence")


def history_inventory(spec: dict) -> list[dict]:
    """Build the source index without treating unprovided history as nonexistent."""
    sources = spec["sources"]
    declared = spec.get("diagnosis", {}).get("history_inventory", [])
    if not isinstance(declared, list):
        raise TaskflowError("revision history_inventory must be a list")
    rows = {}
    for row in declared:
        if (not isinstance(row, dict) or not isinstance(row.get("source_role"), str)
                or row.get("source_role") not in HISTORY_ROLES
                or not isinstance(row.get("status"), str) or row.get("status") not in HISTORY_STATES
                or row["source_role"] in rows):
            raise TaskflowError("revision history inventory requires unique roles and explicit availability")
        if not isinstance(row.get("reason"), str) or not row["reason"].strip():
            raise TaskflowError("revision history availability requires a concrete reason")
        refs = row.get("originals", [])
        if not isinstance(refs, list):
            raise TaskflowError("revision history originals must be evidence references")
        if row["status"] == "archived_not_provided" and not refs:
            raise TaskflowError("archived history requires its precise original entry")
        if refs:
            validate_evidence(refs)
        if row["status"] in {"unknown", "does_not_exist"} and refs:
            raise TaskflowError("unknown or nonexistent history cannot claim archived originals")
        rows[row["source_role"]] = dict(row)
    for role in sorted(HISTORY_ROLES):
        supplied = [row for row in sources if row.get("source_role", "other") == role]
        if supplied:
            if role in rows and rows[role]["status"] != "provided":
                raise TaskflowError("history availability contradicts actually supplied sources")
            rows[role] = {"source_role": role, "status": "provided",
                          "reason": "Exact originals are included in this role's typed inputs.",
                          "targets": [row["target"] for row in supplied]}
        elif role in rows and rows[role]["status"] == "provided":
            raise TaskflowError("provided history requires actual typed source inputs")
        elif role not in rows:
            rows[role] = {"source_role": role, "status": "unknown",
                          "reason": "No availability evidence was supplied; do not reconstruct or rerun."}
    return [rows[role] for role in sorted(rows)]


def _citations(value: object, available: set[str], *, required: bool) -> None:
    if not isinstance(value, list) or (required and not value):
        raise TaskflowError("responsibility analysis requires original citations or an explicit unknown")
    for row in value:
        if (not isinstance(row, dict) or set(row) != {"target", "location"}
                or row.get("target") not in available
                or not isinstance(row.get("location"), str) or not row["location"].strip()):
            raise TaskflowError("responsibility evidence must locate an actual supplied original")


def responsibility_analysis(report: dict, spec: dict, diff: dict, parent: dict) -> list[dict]:
    """Require every assigned responsibility's result-to-action chain at intake."""
    rows = report.get("responsibility_analysis")
    expected = {row["id"] for row in spec["responsibilities"]}
    if (not isinstance(rows, list) or any(not isinstance(row, dict) or not isinstance(row.get("id"), str) for row in rows)
            or len(rows) != len(expected) or {row.get("id") for row in rows} != expected):
        raise TaskflowError("revision responsibility_analysis must cover exactly the assigned responsibilities")
    available = {"parent/" + row["package_path"] for row in parent["files"]}
    available.add("execution/parent-task.json")
    available.update(row["target"] for row in spec["sources"])
    changed = {row["path"] for row in diff["changes"]}
    if diff["declaration_change"] is not None:
        changed.add("task.json")
    for row in rows:
        if row.get("result") not in {"success", "failure", "mixed", "unknown"}:
            raise TaskflowError("responsibility result must distinguish successful, failed and unknown work")
        for field in CHAIN_FIELDS:
            unit = row.get(field)
            if (not isinstance(unit, dict) or unit.get("status") not in {"supported", "unknown"}
                    or not isinstance(unit.get("summary"), str) or not unit["summary"].strip()):
                raise TaskflowError("responsibility analysis requires every explicit evidence chain unit")
            _citations(unit.get("evidence"), available, required=unit["status"] == "supported")
        action = row.get("next_action")
        if (not isinstance(action, dict) or action.get("disposition") not in {"changed", "retained", "unknown"}
                or not isinstance(action.get("reason"), str) or not action["reason"].strip()
                or not isinstance(action.get("changed_paths"), list)):
            raise TaskflowError("responsibility next action requires an actual change or reasoned retention")
        paths = [relative_path(name) for name in action["changed_paths"]]
        if action["disposition"] == "changed":
            if not paths or not set(paths) <= changed:
                raise TaskflowError("responsibility action must identify only actual changed paths")
        elif paths:
            raise TaskflowError("retained or unknown actions cannot claim actual changes")
    coverage = report.get("success_method_coverage")
    if (not isinstance(coverage, dict) or coverage.get("status") not in {"supported", "unknown"}
            or not isinstance(coverage.get("summary"), str) or not coverage["summary"].strip()
            or not isinstance(coverage.get("covered_responsibilities"), list)
            or any(not isinstance(item, str) or not item.strip() for item in coverage["covered_responsibilities"])
            or (coverage["status"] == "supported" and not coverage["covered_responsibilities"])):
        raise TaskflowError("revision must separately analyze lawful successful methods and their major responsibility coverage, or preserve the unknown")
    _citations(coverage.get("evidence"), available, required=coverage["status"] == "supported")
    return rows


def observed_source_use(report: dict, spec: dict, runtime: Path) -> list[dict]:
    """Keep source availability separate from cited visible tool-use evidence."""
    rows = report.get("source_usage", [])
    if not isinstance(rows, list):
        raise TaskflowError("revision source_usage must be a list")
    sources = {row["target"]: row for row in spec["sources"]}
    raw_lines = (runtime / "trace.jsonl").read_text(encoding="utf-8").splitlines()
    result = []
    seen = set()
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get("target"), str) or row.get("target") not in sources
                or row["target"] in seen or not isinstance(row.get("trace_lines"), list)
                or not row["trace_lines"] or not isinstance(row.get("analysis_ids"), list)
                or not row["analysis_ids"] or any(not isinstance(item, str) for item in row["analysis_ids"])):
            raise TaskflowError("source use requires an actual source, visible tool lines and analysis links")
        analyses = {item["id"]: item for item in report["responsibility_analysis"]}
        if not set(row["analysis_ids"]) <= set(analyses):
            raise TaskflowError("source use refers to an unassigned analysis")
        if any(not any(citation.get("target") == row["target"]
                       for field in CHAIN_FIELDS
                       for citation in analyses[identity].get(field, {}).get("evidence", []))
               for identity in row["analysis_ids"]):
            raise TaskflowError("source use must cite that original in each linked analysis")
        for number in row["trace_lines"]:
            if type(number) is not int or not 1 <= number <= len(raw_lines):
                raise TaskflowError("source use cites no actual visible trace line")
            event = json.loads(raw_lines[number - 1])
            item = event.get("item", {})
            if (event.get("type") != "item.completed" or item.get("type") != "command_execution"
                    or item.get("exit_code") != 0 or not item.get("aggregated_output")
                    or row["target"] not in item.get("command", "")):
                raise TaskflowError("source use lacks a completed visible tool return for its exact input")
        seen.add(row["target"])
        result.append({**row, "status": "used", "basis": "visible_tool_return_and_role_analysis"})
    return result
