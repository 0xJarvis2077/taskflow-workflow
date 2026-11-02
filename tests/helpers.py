"""Neutral task data, deliberately unrelated to accepted business samples."""

from pathlib import Path

from taskflow.contracts import read_json, write_json
from taskflow.packages import check_task, init_task


def make_task(root: Path, task_id: str = "neutral", total: int = 7) -> Path:
    task = root / task_id
    init_task(task, task_id)
    value = read_json(task / "task.json")
    value.update(title="Neutral allocation exercise", project={"name": "test", "phase": None, "rule_sources": ["fixture"]})
    value["sources"] = [{"id": "S1", "kind": "test_fixture", "reference": "constructed neutral data"}]
    value["scoring"].update(total=total, criteria=[{
        "id": "C1", "dimension": "result", "max_score": total,
        "condition": "correct result with evidence", "allowed": ["equivalent reasoning"],
        "tolerance": "exact", "evidence": ["prompt", "S1"],
    }])
    (task / "draft/input/prompt.md").write_text("Choose 2 units from capacity 3.\n", encoding="utf-8")
    (task / "draft/evaluation/answer.md").write_text("2 <= 3, feasible.\n", encoding="utf-8")
    (task / "draft/evaluation/rubric.md").write_text(f"C1: {total} points.\n", encoding="utf-8")
    write_json(task / "task.json", value)
    return task


def add_reviews(task: Path) -> dict:
    value = read_json(task / "task.json")
    check = check_task(task, write_report=False)
    for spec in value["reviews"]:
        write_json(task / spec["path"], {
            "schema_version": 1, "review_id": spec["id"], "kind": spec["kind"],
            "status": "passed", "content_digest": check["content_digest"],
            "input_digest": check["input_digest"], "scope": "test fixture only",
            "limitations": ["not a real business/model review"], "evidence": ["neutral fixture"],
        })
    return check
