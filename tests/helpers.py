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


def assert_legacy_recovery_retired(case, *, action="resume_unclaimed_public_preparation"):
    """Assert the removed recovery contract against unchanged actual fixture data."""
    from pathlib import Path
    import tempfile

    from taskflow.contracts import TaskflowError
    from taskflow.execution_lifecycle import resume_path_transport
    from taskflow.execution_store import ExecutionStore, evidence_ref

    store = getattr(case, "store", None)
    if store is None:
        for name in ("case", "fixture", "author"):
            store = getattr(getattr(case, name, None), "store", None)
            if store is not None:
                break
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary).resolve()
        if store is None:
            store = ExecutionStore(root / "state.sqlite3")
        original = root / "original-exception.json"
        original.write_text('{"status":"exception","simulation":true}\n', encoding="utf-8")
        old_bytes = original.read_bytes()
        before = (store.dashboard(), store.list_flows(), store.events())
        receipts = {}
        for record in before[1]:
            name = record["state"].get("receipt")
            if name and Path(name).is_file():
                receipts[name] = Path(name).read_bytes()
        with case.assertRaisesRegex(TaskflowError, "recovery is retired"):
            resume_path_transport(store, "historical-source", {
                "action": action, "reason": "Explicitly retired neutral recovery contract",
                "evidence": [evidence_ref(original, "original simulated exception")]})
        case.assertEqual(before, (store.dashboard(), store.list_flows(), store.events()))
        case.assertEqual(old_bytes, original.read_bytes())
        for name, raw in receipts.items():
            case.assertEqual(raw, Path(name).read_bytes())
