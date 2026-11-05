"""Read explicit solver/review evidence without advancing or repairing a task.

This is a report adapter, not another quality or grading gate. It authenticates
received source references; business statements and original scores stay intact.
"""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from taskflow.contracts import TaskflowError, canonical


_LIMIT = 32 * 1024 * 1024
_BINDING = ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TaskflowError("stress report source: " + message)


class _Sources:
    def __init__(self) -> None:
        self.references = {}
        self.documents = {}
        self.limitations = []

    def read(self, path: Path, location: str, expected: dict | None = None) -> dict:
        path = Path(path)
        _require(path.is_absolute() and path.resolve() == path and not path.is_symlink(),
                 "source must be an absolute ordinary path: " + str(path))
        if str(path) not in self.documents:
            _require(path.is_file() and path.stat().st_size <= _LIMIT,
                     "missing, non-file or oversized source: " + str(path))
            raw = path.read_bytes()
            _require(len(raw) <= _LIMIT, "source grew past size limit")
            value = json.loads(raw)
            _require(isinstance(value, dict), "source JSON must be an object")
            self.documents[str(path)] = value
            self.references[str(path)] = {
                "path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                "size": len(raw), "location": location,
            }
        actual = self.references[str(path)]
        if expected is not None:
            _require(expected.get("sha256") == actual["sha256"]
                     and ("size" not in expected or type(expected["size"]) is int
                          and expected["size"] == actual["size"]),
                     "source SHA/size changed: " + str(path))
        return self.documents[str(path)]

    def reference(self, value: dict, location: str, path: Path | None = None) -> dict:
        _require(isinstance(value, dict) and isinstance(value.get("path"), str)
                 and isinstance(value.get("sha256"), str), "malformed source reference")
        actual_path = Path(value["path"])
        _require(path is None or actual_path == path, "reference moved outside its original task")
        return self.read(actual_path, location, value)

    def optional(self, path: Path, location: str) -> dict | None:
        if not path.exists() and not path.is_symlink():
            self.limitations.append(location + "尚未保存；报告未推进原任务。")
            return None
        return self.read(path, location)


def _flow(connection: sqlite3.Connection, identity: str, kind: str) -> dict:
    row = connection.execute("SELECT * FROM execution_flows WHERE flow_id=?", (identity,)).fetchone()
    _require(row is not None, "unknown explicit task: " + identity)
    record = {"flow_id": row["flow_id"], "definition": json.loads(row["definition"]),
              "definition_digest": row["definition_digest"], "state": json.loads(row["state"]),
              "revision": row["revision"], "event_seq": row["event_seq"],
              "evidence": json.loads(row["evidence"])}
    definition = record["definition"]
    _require(isinstance(definition, dict) and isinstance(record["state"], dict)
             and definition.get("kind") == kind and definition.get("flow_id") == identity,
             "task kind or identity differs from its explicit registration")
    _require(hashlib.sha256(canonical(definition)).hexdigest() == record["definition_digest"],
             "registered definition digest changed")
    return record


def _runtime(record: dict, current: dict, stage: str) -> Path:
    ordinal = current.get("ordinal")
    _require(type(ordinal) is int and ordinal >= 0, "invocation ordinal is invalid")
    root = Path(record["definition"]["directory"]) / "invocations" / (stage + "-" + str(ordinal).zfill(3))
    digest = hashlib.sha256(canonical([record["flow_id"], stage, ordinal])).hexdigest()[:24]
    identity = stage + "-" + digest
    _require(current.get("stage") == stage and current.get("root") == str(root)
             and current.get("runtime") == str(root / "runtime")
             and current.get("intent_id") == identity and current.get("work_id") == identity,
             "invocation differs from its registered task/stage identity")
    return root / "runtime"


def _binding(document: dict, record: dict) -> None:
    binding = document.get("candidate_binding", document.get("binding"))
    definition = record["definition"]
    _require(isinstance(binding, dict)
             and all(binding.get(key) == definition[key] for key in ("candidate_id", "task_id")),
             "candidate/task binding differs from original")


def _manifest(reader: _Sources, record: dict, current: dict, stage: str) -> tuple[Path, dict | None, dict | None]:
    runtime = _runtime(record, current, stage)
    manifest = reader.optional(runtime / "manifest.json", stage + "锁定请求")
    collection = reader.optional(runtime / "collection.json", stage + "原始收件")
    if manifest is not None:
        _binding(manifest, record)
        _require(manifest.get("intent_id") == current["intent_id"]
                 and manifest.get("role") == ("solver" if stage == "solver" else "review-" + stage)
                 and manifest.get("policy_version") == record["definition"]["policy_id"],
                 "manifest belongs to a different role/intent/policy")
    if collection is not None:
        _require(manifest is not None and collection.get("intent_id") == current["intent_id"],
                 "collection belongs to a different invocation")
    return runtime, manifest, collection


def _attempt(connection, record, current, stage, manifest, collection) -> None:
    row = connection.execute("SELECT data FROM attempts WHERE id=?", (current.get("attempt_id"),)).fetchone()
    _require(row is not None, "received invocation has no original attempt")
    attempt = json.loads(row["data"])
    _require(isinstance(attempt, dict), "original attempt JSON must be an object")
    expected = {"attempt_id": current["attempt_id"], "work_id": current["work_id"],
                "intent_id": current["intent_id"], "runtime_path": current["runtime"],
                "task_id": record["definition"]["task_id"], "candidate_id": record["definition"]["candidate_id"],
                "policy_id": record["definition"]["policy_id"],
                "role": "solver" if stage == "solver" else "review-" + stage, "state": "completed"}
    _require(all(attempt.get(key) == value for key, value in expected.items())
             and collection.get("status") == "completed" and collection.get("technical_accepted") is True
             and attempt.get("cli_sid") == collection.get("sid") and bool(collection.get("sid"))
             and manifest.get("intent_id") == attempt["intent_id"],
             "received attempt/SID does not identify the completed original")


def _solver(reader: _Sources, connection, record: dict) -> dict:
    state, definition = record["state"], record["definition"]
    current = state["invocation"]
    runtime, manifest, collection = _manifest(reader, record, current, "solver")
    result = {"record": record, "runtime": str(runtime), "manifest": manifest,
              "collection": collection, "receipt": None}
    if state.get("delivery") != "valid" or state.get("execution") != "completed":
        reader.limitations.append("正式答卷尚未被原任务接收为完整有效交付。")
        return result
    receipt = reader.reference(state["receipt"], "正式答卷原始回执",
                               Path(definition["directory"]) / "solver-receipt.json")
    _attempt(connection, record, current, "solver", manifest, collection)
    _binding(receipt, record)
    _require(receipt.get("kind") == "solver_delivery_v1"
             and receipt.get("solver_task_id") == record["flow_id"]
             and receipt.get("attempt_id") == current["attempt_id"]
             and receipt.get("work_id") == current["work_id"]
             and receipt.get("intent_id") == current["intent_id"]
             and receipt.get("policy_id") == definition["policy_id"]
             and receipt.get("execution") == "completed" and receipt.get("delivery") == "valid"
             and receipt.get("quality_task_id") == definition["quality_task_id"]
             and receipt.get("decision_id") == definition["decision_id"]
             and receipt.get("sid") == collection["sid"]
             and receipt.get("candidate_binding") == manifest["candidate_binding"],
             "solver receipt belongs to another original")
    reader.reference(receipt["manifest"], "正式作答manifest", runtime / "manifest.json")
    reader.reference(receipt["collection"], "正式作答collection", runtime / "collection.json")
    result["receipt"] = receipt
    return result


def _facts(reader: _Sources, connection, record: dict, solver: dict) -> tuple[dict | None, dict | None, dict | None]:
    state = record["state"]
    entries = state.get("invocations", {}).get("facts", [])
    if not entries or state.get("results", {}).get("facts", {}).get("delivery") != "valid":
        reader.limitations.append("评阅事实尚未被原任务有效接收。")
        return None, None, None
    current = entries[-1]
    runtime, manifest, collection = _manifest(reader, record, current, "facts")
    facts = reader.reference(current["artifact"], "评阅者原始事实", runtime / "artifacts/facts.json")
    _binding(facts, record)
    _attempt(connection, record, current, "facts", manifest, collection)
    _require(solver["receipt"] is not None
             and manifest["candidate_binding"] == solver["manifest"]["candidate_binding"]
             and all(facts["binding"].get(key) == solver["manifest"]["candidate_binding"].get(key)
                     for key in _BINDING)
             and facts["binding"].get("solver_attempt_id") == solver["receipt"]["attempt_id"]
             and facts["binding"].get("solver_collection_sha256") == solver["receipt"]["collection"]["sha256"],
             "facts belong to another formal run")
    _require(facts.get("kind") == "review_facts_v1", "wrong factual document kind")
    seal = None
    if state.get("facts_seal"):
        seal = reader.reference(state["facts_seal"], "封存事实", runtime / "facts-seal.json")
        _require(seal.get("sid") == collection.get("sid")
                 and seal.get("intent_id") == manifest.get("intent_id")
                 and seal.get("candidate_binding") == manifest.get("candidate_binding")
                 and seal.get("path") == str(runtime / "artifacts/facts.json")
                 and seal.get("output_name") == "facts.json"
                 and facts.get("status") == "complete", "facts seal belongs to another reviewer or incomplete facts")
        reader.read(runtime / "manifest.json", "事实manifest", seal["manifest"])
        reader.read(runtime / "collection.json", "事实collection", seal["collection"])
        reader.read(runtime / "artifacts/facts.json", "封存事实原文", seal["facts"])
    return facts, seal, manifest


def _assessment(reader: _Sources, connection, record: dict, solver: dict) -> dict:
    definition, state = record["definition"], record["state"]
    original = solver["record"]["definition"]
    _require(definition.get("solver_task_id") == solver["record"]["flow_id"]
             and all(definition.get(key) == original[key] for key in ("task_id", "candidate_id", "candidate_root")),
             "assessment belongs to another solver/candidate")
    facts, seal, facts_manifest = _facts(reader, connection, record, solver)
    result = {"record": record, "facts": facts, "score": None, "receipt": None,
              "facts_manifest": facts_manifest, "score_manifest": None}
    if not state.get("score_receipt"):
        reader.limitations.append("尚无原任务接收的唯一原分；未采纳原始未接收评分文件。")
        return result
    _require(state.get("results", {}).get("score", {}).get("delivery") == "valid"
             and state["results"]["score"].get("execution") == "completed"
             and solver["receipt"] is not None and seal is not None,
             "original score receipt requires accepted solver, sealed facts and valid score delivery")
    current = state["invocations"]["score"][-1]
    runtime, manifest, collection = _manifest(reader, record, current, "score")
    _attempt(connection, record, current, "score", manifest, collection)
    receipt = reader.reference(state["score_receipt"], "唯一原评分回执",
                               Path(definition["directory"]) / "score-receipt.json")
    _require(receipt.get("kind") == "original_score_receipt_v1"
             and receipt.get("assessment_task_id") == record["flow_id"]
             and receipt.get("attempt_id") == current["attempt_id"]
             and receipt.get("sid") == collection["sid"] == seal["sid"]
             and receipt.get("facts_seal") == state["facts_seal"]
             and receipt.get("solver_receipt") == solver["record"]["state"]["receipt"],
             "score receipt belongs to another original reviewer/run")
    reader.reference(receipt["manifest"], "评分manifest", runtime / "manifest.json")
    reader.reference(receipt["collection"], "评分collection", runtime / "collection.json")
    score = reader.reference(receipt["score"], "原评阅者评分", runtime / "artifacts/score.json")
    _binding(score, record)
    binding = score.get("binding", {})
    _require(score.get("kind") == "review_score_v1" and binding == receipt.get("binding")
             and manifest["candidate_binding"] == solver["manifest"]["candidate_binding"]
             and all(binding.get(key) == manifest["candidate_binding"].get(key) for key in _BINDING)
             and binding.get("solver_attempt_id") == solver["receipt"]["attempt_id"]
             and binding.get("solver_collection_sha256") == solver["receipt"]["collection"]["sha256"]
             and score.get("facts_sha256") == seal["facts"]["sha256"]
             and all(canonical(score.get(key)) == canonical(receipt.get(key))
                     for key in ("total", "validity", "outcome")),
             "original score content or binding differs from received receipt")
    result.update(score=score, receipt=receipt, score_manifest=manifest)
    return result


def load_report_sources(database: Path, solver_id: str, assessment_id: str | None = None) -> dict:
    """Return a read-only SQLite snapshot and exact received source documents.

    Running and unreceived stages are visible as gaps. A present but corrupted
    source or cross-run binding is an error, never an invented accepted score.
    """
    database = Path(database)
    _require(database.is_absolute() and database.resolve() == database and database.is_file()
             and not database.is_symlink(), "database must be an existing canonical ordinary path")
    _require(isinstance(solver_id, str) and bool(solver_id), "solver identity required")
    reader = _Sources()
    try:
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            solver_record = _flow(connection, solver_id, "solver_task_v1")
            solver = _solver(reader, connection, solver_record)
            if assessment_id is None:
                rows = connection.execute(
                    "SELECT flow_id FROM execution_flows WHERE json_extract(definition,'$.kind')=? "
                    "AND json_extract(definition,'$.solver_task_id')=?",
                    ("assessment_task_v1", solver_id)).fetchall()
                _require(len(rows) <= 1, "multiple original assessments for the same solver")
                assessment_id = rows[0]["flow_id"] if rows else None
            assessment = None
            if assessment_id is not None:
                record = _flow(connection, assessment_id, "assessment_task_v1")
                assessment = _assessment(reader, connection, record, solver)
            else:
                reader.limitations.append("尚无该正式答卷的原评阅任务。")
            return {"solver": solver, "assessment": assessment,
                    "references": sorted(reader.references.values(), key=lambda row: row["path"]),
                    "limitations": reader.limitations}
    except TaskflowError:
        raise
    except (KeyError, TypeError, ValueError, sqlite3.Error, OSError) as error:
        raise TaskflowError("stress report source cannot be authenticated: " + str(error)) from error
