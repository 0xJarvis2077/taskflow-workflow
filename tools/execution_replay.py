"""Replay externally adjudicated history on copies; no new model or score."""

import argparse
import json
import shutil
from copy import deepcopy
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_store import ExecutionStore, evidence_ref
from tools.execution_simulation import fixture_policy, perform


def replay(manifest_path: Path, input_path: Path, scenario_path: Path, output: Path) -> dict:
    manifest = read_json(manifest_path)
    adjudication = read_json(input_path)
    scenario = read_json(scenario_path)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for entry in manifest["files"]:
        for key in ("source", "copy"):
            if sha256(Path(entry[key]).read_bytes()) != entry["sha256"]:
                raise TaskflowError(f"historical {key} bytes changed: {entry[key]}")
    histories = manifest["runs"]
    chosen = next(row for row in histories if row["version_id"] == adjudication["candidate_version"])
    reference_file = next(Path(row["copy"]) for row in manifest["files"]
                          if f"/runs/{chosen['run_id']}/snapshot.reference.json" in row["source"])
    reference = read_json(reference_file)
    task_id = reference["task_id"]
    before, after = output / "public-before", output / "public-after"
    for row in manifest["files"]:
        marker = f"/versions/{chosen['version_id']}/draft/input/"
        if marker in row["source"]:
            target = before / "draft/input" / row["source"].split(marker, 1)[1]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(Path(row["copy"]).read_bytes())
    shutil.copytree(before, after)
    changed = []
    for change in scenario["changes"]:
        path = after / change["path"]
        text = path.read_text(encoding="utf-8")
        for replacement in change["replacements"]:
            if text.count(replacement["before"]) != 1:
                raise TaskflowError("replay replacement must identify exactly one original span")
            text = text.replace(replacement["before"], replacement["after"])
        path.write_text(text, encoding="utf-8")
        if path.read_bytes() == (before / change["path"]).read_bytes():
            raise TaskflowError("material replay must actually change public bytes")
        for condition in change["must_retain"]:
            if condition not in text:
                raise TaskflowError("replay removed an explicit necessary condition")
        changed.append(change["path"])
    for path in before.rglob("*"):
        if path.is_file() and path.relative_to(before).as_posix() not in changed:
            if path.read_bytes() != (after / path.relative_to(before)).read_bytes():
                raise TaskflowError("unexpected replay mutation")
    store = ExecutionStore(output / "state.sqlite3")
    store.register_policy("fixture-v1", fixture_policy())
    refs = [evidence_ref(manifest_path, "read-only historical run/evaluation bindings")]
    def public_files(directory: Path) -> list[dict]:
        return [{**evidence_ref(directory / item["path"], "replay captured public file"),
                 "package_path": item["path"], "role": item["role"]}
                for item in reference["files"] if (directory / item["path"]).is_file()]

    for historical in histories:
        store.register_candidate(task_id, "history-" + historical["version_id"],
                                 **{key: historical[key] for key in ("input_digest", "content_digest", "evaluation_digest")}, evidence=refs,
                                 files=public_files(before) if historical == chosen else [])
    candidate_id = "history-" + chosen["version_id"]
    original_quality = perform(store, output, candidate_id, "quality", "prior-quality", {
        "report.json": json.dumps({"nature": "test_fixture", "verdict": "accepted", "scope": "synthetic historical acceptance for state replay only"})
    })
    prior_ref = [evidence_ref(Path(original_quality["runtime_path"]) / "artifacts/report.json", "synthetic prior verdict")]
    store.record_review("prior-review", candidate_id, original_quality["attempt_id"], verdict="accepted", scope="synthetic full scope", whole_package=True, evidence=prior_ref)
    store.quality_gate(candidate_id)
    external_quality = perform(store, output, candidate_id, "quality", "external-adjudication", {
        "findings.json": json.dumps({"nature": "test_fixture", "adjudication_source": str(input_path), "findings": adjudication["external_findings"]}, ensure_ascii=False)
    })
    external_ref = [evidence_ref(Path(external_quality["runtime_path"]) / "artifacts/findings.json", "externally confirmed finding; not autonomous detection")]
    store.record_review("external-review", candidate_id, external_quality["attempt_id"], verdict="defects", scope="external responsibility finding", whole_package=True, evidence=external_ref)
    store.open_defect("material-finding", candidate_id, "external-review", responsibility=scenario["responsibility"], evidence=external_ref)
    rejected = []
    try:
        store.quality_gate(candidate_id)
    except TaskflowError:
        rejected.append("same_bytes_reopened")
    document_attempt = perform(store, output, candidate_id, "repair", "report-only-repair", {"note.md": "Only report wording changed.\n"})
    store.register_candidate(task_id, "report-only", input_digest=chosen["input_digest"],
                             content_digest=sha256(b"replay report-only revision"), evaluation_digest=chosen["evaluation_digest"],
                             parent_id=candidate_id, author_attempt=document_attempt["attempt_id"],
                             files=public_files(before),
                             evidence=[evidence_ref(Path(document_attempt["runtime_path"]) / "artifacts/note.md", "report-only evidence")])
    try:
        store.record_repair("material-finding", "report-only", evidence=external_ref)
    except TaskflowError:
        rejected.append("report_only_material_closure")
    author = perform(store, output, candidate_id, "repair", "actual-copy-repair", {
        path: (after / path).read_text(encoding="utf-8") for path in changed
    })
    files = deepcopy(reference["files"])
    public = []
    for row in files:
        path = after / row["path"]
        if path.is_file():
            raw = path.read_bytes()
            row.update(sha256=sha256(raw), size=len(raw))
            public.append({key: row[key] for key in ("id", "path", "role", "size", "sha256")})
    revised_input = sha256(canonical(public))
    if revised_input == chosen["input_digest"]:
        raise TaskflowError("actual copy repair did not change input digest")
    repair_ref = [evidence_ref(after / path, "actual replay-only modified material") for path in changed]
    store.register_candidate(task_id, "replay-revised", input_digest=revised_input,
                             content_digest=sha256(canonical({"task_sha256": reference["task_sha256"], "files": files})),
                             evaluation_digest=chosen["evaluation_digest"], parent_id=candidate_id,
                             author_attempt=author["attempt_id"], evidence=repair_ref, files=public_files(after))
    store.record_repair("material-finding", "replay-revised", evidence=repair_ref)
    try:
        store.record_review("self-review", "replay-revised", author["attempt_id"], verdict="accepted", scope="author self-report", evidence=repair_ref, whole_package=True)
    except TaskflowError:
        rejected.append("author_self_review")
    recheck = perform(store, output, "replay-revised", "quality", "external-recheck", {
        "recheck.json": json.dumps({"nature": "test_fixture", "scope": "externally scripted whole-public-package association check", "changed": changed, "retained_conditions": scenario["changes"]}, ensure_ascii=False)
    })
    recheck_ref = [evidence_ref(Path(recheck["runtime_path"]) / "artifacts/recheck.json", "synthetic independent-context post-modification recheck")]
    store.record_review("recheck", "replay-revised", recheck["attempt_id"], verdict="accepted", scope="whole-package external scripted adjudication", evidence=recheck_ref, whole_package=True)
    store.close_defect("material-finding", "recheck", evidence=recheck_ref)
    store.quality_gate("replay-revised")
    for row in manifest["files"]:
        if any(sha256(Path(row[key]).read_bytes()) != row["sha256"] for key in ("source", "copy")):
            raise TaskflowError("historical evidence changed during replay")
    result = {"nature": "test_fixture_historical_execution_replay", "real_model_calls": 0,
              "rejected": rejected, "revised_gate": "accepted_external_scripted_recheck",
              "history_intact": True, "distinct_history": histories,
              "changed_copy_paths": changed, "unchanged_other_public_files": len(public) - len(changed),
              "old_review_did_not_transfer": True,
              "limitation": "A tests state enforcement using injected adjudication and mock role processes. It does not discover defects, adopt a repaired task, re-score history, or establish content correctness/difficulty.",
              "dashboard": store.dashboard()}
    write_json(output / "verification.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = replay(args.manifest, args.input, args.scenario, args.output)
    print(json.dumps({key: result[key] for key in ("nature", "rejected", "history_intact", "changed_copy_paths", "unchanged_other_public_files")}))
