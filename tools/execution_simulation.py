"""Run an observable neutral quality/experiment lifecycle without model calls."""

import json
import shutil
import sys
import uuid
from pathlib import Path

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate, enqueue_spec, release_candidate, run_worker
from taskflow.execution_runner import archive_attempt, seal_facts
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.packages import check_task, init_task
from taskflow.records import import_evaluation, import_run


def fixture_policy() -> dict:
    return {"concurrency": 6, "roles": {
        role: {"model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15}
        for role in ("author", "repair", "quality", "solver", "review-facts", "review-score", "research")
    }}


def make_package(root: Path) -> Path:
    task = root / "neutral"
    init_task(task, "neutral")
    data = read_json(task / "task.json")
    data.update(title="Neutral capacity decision", project={"name": "fixture", "phase": None, "rule_sources": ["fixture"]})
    data["sources"] = [{"id": "S1", "kind": "test_fixture", "reference": "neutral construction"}]
    data["scoring"].update(total=7, criteria=[{
        "id": "C1", "dimension": "result", "max_score": 7,
        "condition": "choose a feasible allocation using the stated capacity",
        "allowed": ["equivalent representation"], "tolerance": "exact", "evidence": ["prompt", "S1"]
    }])
    write_json(task / "task.json", data)
    (task / "draft/input/prompt.md").write_text("Capacity is 3. Choose an allocation of 2 units.\n", encoding="utf-8")
    (task / "draft/evaluation/answer.md").write_text("2 <= 3, feasible.\n", encoding="utf-8")
    (task / "draft/evaluation/rubric.md").write_text("C1: 7 points for a feasible, justified allocation.\n", encoding="utf-8")
    return task


def package_reviews(task: Path, source: Path) -> None:
    """Fixture-only bridge to the existing business/artifact report contracts."""
    check = check_task(task, write_report=False)
    for spec in read_json(task / "task.json")["reviews"]:
        write_json(task / spec["path"], {
            "schema_version": 1, "review_id": spec["id"], "kind": spec["kind"],
            "status": "passed", "content_digest": check["content_digest"],
            "input_digest": check["input_digest"], "scope": "neutral test_fixture only",
            "limitations": ["simulated quality input, no model or semantic certification"],
            "evidence": [str(source)]
        })


def perform(store: ExecutionStore, root: Path, candidate: str, role: str,
            name: str, payload: dict[str, str], *, sid: str | None = None,
            extra: dict | None = None, incomplete: bool = False) -> dict:
    """Use the same prepare/claim/worker path as an explicitly queued real role."""
    work = root / "work" / name
    work.mkdir(parents=True)
    payload_file = root / "payloads" / f"{name}.json"
    write_json(payload_file, payload)
    runner = {
        "intent_id": name, "policy_version": "fixture-v1", "role": role,
        "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture",
        "timeout_seconds": 15, "cwd": str(work),
        "argv": [str(Path(sys.executable).resolve()), str(Path(__file__).with_name("mock_role_cli.py").resolve()),
                 "--payload", str(payload_file), "--sid", sid or str(uuid.uuid4())],
        "inputs": [], "outputs": list(payload),
        "request_sections": [{"kind": "operation", "text": "Neutral test_fixture role; deliver the declared files."}],
        "dependencies": [str(payload_file), str(Path(__file__).with_name("mock_role_cli.py").resolve())]
    }
    if incomplete:
        runner["argv"].append("--incomplete")
    runner.update(extra or {})
    enqueue_spec(store, {"work": {
        "work_id": name, "task_id": store.get_candidate(candidate)["task_id"], "candidate_id": candidate,
        "role": role, "policy_id": "fixture-v1", "intent_id": name,
        "purpose": "neutral lifecycle verification", "modifies": role in ("author", "repair")
    }, "runner": runner}, root / "attempts" / name)
    return run_worker(store, "fixture-worker")


def simulate(output: Path) -> dict:
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=False)
    store = ExecutionStore(root / "state.sqlite3")
    store.register_policy("fixture-v1", fixture_policy())
    task = make_package(root)
    capture_candidate(store, task, "neutral-seed")
    author = perform(store, root, "neutral-seed", "author", "author-01", {
        "prompt.md": "Capacity is 3. Allocate 2 units. Draft recommendation: select container A. Decide the allocation.\n"
    })
    shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", task / "draft/input/prompt.md")
    capture_candidate(store, task, "neutral-v1", "neutral-seed", author["attempt_id"])
    quality = perform(store, root, "neutral-v1", "quality", "quality-01", {
        "review.json": json.dumps({"nature": "test_fixture", "finding": "unapproved recommendation performs the delegated choice", "preserve": ["capacity", "required units"]})
    })
    qref = [evidence_ref(Path(quality["runtime_path"]) / "artifacts/review.json", "fixture finding")]
    store.record_review("review-v1", "neutral-v1", quality["attempt_id"], verdict="defects", scope="entire neutral package", evidence=qref, whole_package=True)
    store.open_defect("D1", "neutral-v1", "review-v1", responsibility="restore allocation decision while preserving capacity", evidence=qref)
    blocked = []
    try:
        store.quality_gate("neutral-v1")
    except TaskflowError:
        blocked.append("unclosed_defect")
    repair = perform(store, root, "neutral-v1", "repair", "repair-01", {
        "prompt.md": "Capacity is 3. Container A and B are available. Allocate 2 units and justify your choice.\n"
    })
    shutil.copyfile(Path(repair["runtime_path"]) / "artifacts/prompt.md", task / "draft/input/prompt.md")
    capture_candidate(store, task, "neutral-v2", "neutral-v1", repair["attempt_id"])
    rref = [evidence_ref(task / "draft/input/prompt.md", "actual changed public input; capacity and demand retained")]
    store.record_repair("D1", "neutral-v2", evidence=rref)
    try:
        store.record_review("self-review", "neutral-v2", repair["attempt_id"], verdict="accepted", scope="self claim", evidence=rref, whole_package=True)
    except TaskflowError:
        blocked.append("author_self_review")
    recheck = perform(store, root, "neutral-v2", "quality", "quality-02", {
        "review.json": json.dumps({"nature": "test_fixture", "verdict": "accepted", "scope": "whole package and cross-file relations", "conditions_retained": ["capacity 3", "demand 2"]})
    })
    reref = [evidence_ref(Path(recheck["runtime_path"]) / "artifacts/review.json", "independent fixture whole-package recheck")]
    store.record_review("review-v2", "neutral-v2", recheck["attempt_id"], verdict="accepted", scope="whole package incl related private files", evidence=reref, whole_package=True)
    store.close_defect("D1", "review-v2", evidence=reref)
    package_reviews(task, Path(recheck["runtime_path"]) / "artifacts/review.json")
    released = release_candidate(store, task, "neutral-v2", "fixture-v2")
    store.record_decision("experiment-decision", "neutral", "neutral-v2", action="experiment", rationale="exercise lifecycle without a real model", evidence=reref,
                          experiment_purpose="neutral lifecycle verification", authorized=True)
    solver = perform(store, root, "neutral-v2", "solver", "solver-01", {
        "answer.md": "Allocate 2 units to A: 2 <= 3.\n"
    }, extra={"inputs": [{"path": str(task / "draft/input/prompt.md"), "kind": "public"}]})
    facts = perform(store, root, "neutral-v2", "review-facts", "facts-01", {
        "facts.md": "Entire submission: chooses A and 2 units, explicitly checks capacity 3.\n"
    }, extra={"inputs": [{"path": str(Path(solver["runtime_path"]) / "artifacts/answer.md"), "kind": "submission"}]})
    facts_root = Path(facts["runtime_path"])
    seal_facts(facts_root, facts_root / "artifacts/facts.md")
    scoring = perform(store, root, "neutral-v2", "review-score", "score-01", {
        "score.json": json.dumps({"criterion": "C1", "score": 7, "max": 7, "nature": "test_fixture"})
    }, sid=facts["cli_sid"], extra={
        "facts_attempt": str(facts_root), "resume_sid": facts["cli_sid"],
        "inputs": [{"path": str(task / "draft/evaluation/rubric.md"), "kind": "teacher"}]
    })
    snapshot = Path(released["snapshot"]["path"])
    record_source = root / "record-source"
    record_source.mkdir()
    shutil.copyfile(Path(solver["runtime_path"]) / "artifacts/answer.md", record_source / "answer.md")
    shutil.copyfile(Path(solver["runtime_path"]) / "trace.jsonl", record_source / "trace.jsonl")
    termination = read_json(Path(solver["runtime_path"]) / "termination.json")
    write_json(record_source / "run.json", {
        "run_id": "fixture-run-01", "nature": "test_fixture", "status": "completed",
        "actual_input_digest": released["snapshot"]["input_digest"],
        "requested_config": {"model": "fixture-cli", "tools": ["fixture file output"]},
        "observed_config": {"model": "fixture-cli", "tools": ["fixture file output"]},
        "started_at": termination["started_at"], "ended_at": termination["ended_at"],
        "interventions": [], "exceptions": [],
        "input_evidence": ["trace#mock-request-received"],
        "isolation_evidence": ["trace#fixture-only-no-native-isolation-claim"],
        "artifacts": [{"id": "answer", "path": "answer.md", "kind": "answer"},
                      {"id": "trace", "path": "trace.jsonl", "kind": "trace"}],
        "notes": ["simulated subprocess only; never a real model score"]
    })
    archived_run = import_run(snapshot, record_source / "run.json")
    write_json(record_source / "evaluation.json", {
        "evaluation_id": "fixture-eval-01", "kind": "original",
        "input_digest": released["snapshot"]["input_digest"],
        "evaluator": "test_fixture same-SID synthetic two-stage review",
        "review_status": "unreviewed", "total": 7,
        "items": [{"id": "C1", "score": 7, "evidence": ["answer#allocation"]}]
    })
    archived_evaluation = import_evaluation(Path(archived_run["path"]), record_source / "evaluation.json")
    raw = archive_attempt(Path(solver["runtime_path"]), root / "raw-archive/solver-01")
    store.record_decision("next-research", "neutral", "neutral-v2", action="research", rationale="fixture successful method observed; explore a separately evidenced unknown", evidence=reref)
    result = {
        "nature": "test_fixture", "real_model_calls": 0,
        "stages": {"author": author["state"], "quality": quality["state"], "repair": repair["state"],
                   "recheck": recheck["state"], "solver": solver["state"], "facts": facts["state"], "score": scoring["state"]},
        "rejected": blocked, "same_review_sid": facts["cli_sid"] == scoring["cli_sid"],
        "raw_archive": raw, "original_run": archived_run,
        "original_evaluation": archived_evaluation, "dashboard": store.dashboard(),
        "limitation": "neutral program-generated quality inputs test execution gates, not semantic detection or real-model difficulty"
    }
    write_json(root / "verification.json", result)
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    print(json.dumps(simulate(parser.parse_args().output), ensure_ascii=False, indent=2))
