"""Read-only real SQLite/evidence fixtures; no model or runtime execution."""

import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical
from taskflow.stress_report_sources import load_report_sources


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value))
    return ref(path)


def info(path):
    raw = path.read_bytes()
    return {"sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}


def ref(path):
    return {"path": str(path), **info(path), "location": "neutral source fixture"}


class SourceCase:
    def __init__(self, root):
        self.root = root
        self.database = root / "state.sqlite3"
        self.connection = sqlite3.connect(self.database)
        self.connection.execute("CREATE TABLE execution_flows(flow_id TEXT PRIMARY KEY,definition TEXT,definition_digest TEXT,state TEXT,revision INT,event_seq INT,evidence TEXT)")
        self.connection.execute("CREATE TABLE attempts(id TEXT PRIMARY KEY,data TEXT)")
        self.binding = {"candidate_id": "neutral-candidate", "task_id": "neutral-task", "input_digest": "a" * 64,
                        "content_digest": "b" * 64, "evaluation_digest": "c" * 64}
        self.solver = self.record("solver", "solver_task_v1")
        self.solver["definition"].update(quality_task_id="quality", decision_id="decision")
        current, self.solver_manifest, self.solver_collection = self.invocation(self.solver, "solver", "solver-sid")
        self.solver["state"].update(invocation=current, execution="completed", delivery="valid")
        runtime = Path(current["runtime"])
        self.solver_receipt = {"kind": "solver_delivery_v1", "solver_task_id": "solver", "candidate_binding": self.binding,
            "attempt_id": current["attempt_id"], "sid": "solver-sid", "work_id": current["work_id"],
            "intent_id": current["intent_id"], "policy_id": "solver-policy", "quality_task_id": "quality",
            "decision_id": "decision", "execution": "completed", "delivery": "valid",
            "manifest": ref(runtime / "manifest.json"), "collection": ref(runtime / "collection.json")}
        self.solver["state"]["receipt"] = write(root / "solver/solver-receipt.json", self.solver_receipt)
        self.review = self.record("review", "assessment_task_v1")
        self.review["definition"]["solver_task_id"] = "solver"
        facts_current, _, _ = self.invocation(self.review, "facts", "review-sid")
        score_current, _, _ = self.invocation(self.review, "score", "review-sid")
        self.review["state"].update(invocations={"facts": [facts_current], "score": [score_current]},
            results={stage: {"delivery": "valid", "execution": "completed"} for stage in ("facts", "score")})
        self.review_binding = {**self.binding, "solver_attempt_id": current["attempt_id"],
                               "solver_collection_sha256": self.solver_receipt["collection"]["sha256"]}
        facts_runtime = Path(facts_current["runtime"])
        facts = {"kind": "review_facts_v1", "binding": self.review_binding, "status": "complete", "facts": []}
        facts_current["artifact"] = write(facts_runtime / "artifacts/facts.json", facts)
        seal = {"sid": "review-sid", "intent_id": facts_current["intent_id"], "candidate_binding": self.binding,
                "path": str(facts_runtime / "artifacts/facts.json"), "output_name": "facts.json",
                "facts": info(facts_runtime / "artifacts/facts.json"),
                "manifest": info(facts_runtime / "manifest.json"), "collection": info(facts_runtime / "collection.json")}
        self.review["state"]["facts_seal"] = write(facts_runtime / "facts-seal.json", seal)
        self.score = {"kind": "review_score_v1", "binding": self.review_binding,
            "outcome": "scored", "facts_sha256": seal["facts"]["sha256"], "total": 7,
            "validity": {"local_eligible": None, "reason": "neutral"},
            "criteria": [{"id": "criterion", "points": 7, "max_points": 10,
                          "reason": "original reviewer reason", "evidence": []}]}
        score_runtime = Path(score_current["runtime"])
        score_current["artifact"] = write(score_runtime / "artifacts/score.json", self.score)
        self.score_receipt = {"kind": "original_score_receipt_v1", "assessment_task_id": "review",
            "solver_receipt": self.solver["state"]["receipt"], "binding": self.review_binding,
            "facts_seal": self.review["state"]["facts_seal"], "sid": "review-sid",
            "attempt_id": score_current["attempt_id"], "manifest": ref(score_runtime / "manifest.json"),
            "collection": ref(score_runtime / "collection.json"), "score": score_current["artifact"],
            **{key: self.score[key] for key in ("outcome", "total", "validity")}}
        self.review["state"]["score_receipt"] = write(root / "review/score-receipt.json", self.score_receipt)
        self.save(self.solver)
        self.save(self.review)

    def record(self, identity, kind):
        return {"flow_id": identity, "definition": {"flow_id": identity, "kind": kind,
            "directory": str(self.root / identity), "candidate_root": str(self.root / "candidate"),
            "candidate_id": "neutral-candidate", "task_id": "neutral-task", "policy_id": identity + "-policy"}, "state": {}}

    def invocation(self, record, stage, sid):
        digest = hashlib.sha256(canonical([record["flow_id"], stage, 0])).hexdigest()[:24]
        identity = stage + "-" + digest
        root = Path(record["definition"]["directory"]) / "invocations" / (stage + "-000")
        runtime = root / "runtime"
        current = {"stage": stage, "ordinal": 0, "root": str(root), "runtime": str(runtime),
                   "intent_id": identity, "work_id": identity, "attempt_id": identity + "-attempt"}
        manifest = {"intent_id": identity, "candidate_binding": self.binding,
                    "policy_version": record["definition"]["policy_id"],
                    "role": "solver" if stage == "solver" else "review-" + stage}
        collection = {"intent_id": identity, "sid": sid, "technical_accepted": True, "status": "completed"}
        write(runtime / "manifest.json", manifest)
        write(runtime / "collection.json", collection)
        attempt = {"attempt_id": current["attempt_id"], "work_id": identity, "intent_id": identity,
                   "runtime_path": str(runtime), **{key: record["definition"][key] for key in ("task_id", "candidate_id", "policy_id")},
                   "role": manifest["role"], "state": "completed", "cli_sid": sid}
        self.connection.execute("INSERT INTO attempts VALUES(?,?)", (attempt["attempt_id"], json.dumps(attempt)))
        return current, manifest, collection

    def save(self, record):
        definition = canonical(record["definition"])
        self.connection.execute("INSERT OR REPLACE INTO execution_flows VALUES(?,?,?,?,?,?,?)",
            (record["flow_id"], definition.decode(), hashlib.sha256(definition).hexdigest(),
             canonical(record["state"]).decode(), 0, 1, "[]"))
        self.connection.commit()

    def read(self, assessment=None):
        return load_report_sources(self.database, "solver", assessment)

    def rewrite_receipt(self):
        path = self.root / "review/score-receipt.json"
        self.review["state"]["score_receipt"] = write(path, self.score_receipt)
        self.save(self.review)


class StressReportSourcesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.case = SourceCase(Path(self.temporary.name).resolve())
        self.addCleanup(self.case.connection.close)

    def test_reads_original_score_and_sources_without_mutating_database_or_files(self):
        case = self.case
        before = {str(p.relative_to(case.root)): p.read_bytes() for p in case.root.rglob("*") if p.is_file()}
        result = case.read()
        self.assertEqual(case.score, result["assessment"]["score"])
        self.assertEqual(case.solver_receipt, result["solver"]["receipt"])
        self.assertEqual("review-policy", result["assessment"]["record"]["definition"]["policy_id"])
        self.assertEqual("review-facts", result["assessment"]["facts_manifest"]["role"])
        self.assertEqual("review-score", result["assessment"]["score_manifest"]["role"])
        self.assertGreater(len(result["references"]), 7)
        self.assertEqual([], result["limitations"])
        self.assertEqual(before, {str(p.relative_to(case.root)): p.read_bytes() for p in case.root.rglob("*") if p.is_file()})

    def test_pending_or_invalid_score_does_not_read_unaccepted_raw_score(self):
        case = self.case
        del case.review["state"]["score_receipt"]
        case.review["state"]["results"]["score"]["delivery"] = "invalid"
        case.save(case.review)
        Path(case.review["state"]["invocations"]["score"][0]["artifact"]["path"]).write_text("not valid JSON")
        result = case.read()
        self.assertIsNone(result["assessment"]["score"])
        self.assertIsNone(result["assessment"]["receipt"])
        self.assertIsNone(result["assessment"]["score_manifest"])
        self.assertEqual("review-facts", result["assessment"]["facts_manifest"]["role"])
        self.assertTrue(result["limitations"])

    def test_pending_solver_and_absent_review_are_explicit(self):
        case = self.case
        case.connection.execute("DELETE FROM execution_flows WHERE flow_id='review'")
        case.solver["state"].update(execution="not_started", delivery="pending")
        for name in ("manifest.json", "collection.json"):
            (Path(case.solver["state"]["invocation"]["runtime"]) / name).unlink()
        case.save(case.solver)
        result = case.read()
        self.assertIsNone(result["solver"]["manifest"])
        self.assertIsNone(result["assessment"])
        self.assertGreaterEqual(len(result["limitations"]), 3)

    def test_report_connection_is_closed_on_success_and_error(self):
        actual_connect = sqlite3.connect
        connections = []

        def capture(*args, **kwargs):
            connection = actual_connect(*args, **kwargs)
            connections.append(connection)
            return connection

        with patch("taskflow.stress_report_sources.sqlite3.connect", side_effect=capture):
            self.case.read()
            with self.assertRaises(TaskflowError):
                self.case.read("unknown-review")
        self.assertEqual(2, len(connections))
        for connection in connections:
            with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                connection.execute("SELECT 1")

    def test_pending_facts_has_no_reviewer_manifest_or_score(self):
        case = self.case
        del case.review["state"]["score_receipt"]
        case.review["state"]["results"]["facts"]["delivery"] = "pending"
        case.save(case.review)
        result = case.read()["assessment"]
        self.assertIsNone(result["facts"])
        self.assertIsNone(result["facts_manifest"])
        self.assertIsNone(result["score_manifest"])

    def test_received_score_or_facts_tampering_is_rejected(self):
        for area in ("score", "facts"):
            with self.subTest(area=area):
                case = self.case
                artifact = Path(case.review["state"]["invocations"][area][0]["artifact"]["path"])
                original = artifact.read_bytes()
                artifact.write_text("{}")
                with self.assertRaisesRegex(TaskflowError, "SHA/size changed"):
                    case.read()
                artifact.write_bytes(original)

    def test_bad_definition_digest_and_wrong_kind_are_rejected(self):
        case = self.case
        case.connection.execute("UPDATE execution_flows SET definition_digest='wrong' WHERE flow_id='solver'")
        case.connection.commit()
        with self.assertRaisesRegex(TaskflowError, "definition digest"):
            case.read()
        case.save(case.solver)
        case.solver["definition"]["kind"] = "legacy_experiment"
        case.save(case.solver)
        with self.assertRaisesRegex(TaskflowError, "task kind"):
            case.read()

    def test_cross_solver_review_and_duplicate_review_are_rejected(self):
        case = self.case
        other = copy.deepcopy(case.review)
        other["definition"]["solver_task_id"] = "another-solver"
        case.save(other)
        with self.assertRaisesRegex(TaskflowError, "another solver"):
            case.read("review")
        case.save(case.review)
        other = copy.deepcopy(case.review)
        other["flow_id"] = other["definition"]["flow_id"] = "review-duplicate"
        case.save(other)
        with self.assertRaisesRegex(TaskflowError, "multiple original"):
            case.read()

    def test_cross_context_and_unaccepted_original_receipt_are_rejected(self):
        case = self.case
        case.score_receipt["sid"] = "another-sid"
        case.rewrite_receipt()
        with self.assertRaisesRegex(TaskflowError, "another original"):
            case.read()
        case.score_receipt["sid"] = "review-sid"
        case.review["state"]["results"]["score"]["delivery"] = "invalid"
        case.rewrite_receipt()
        with self.assertRaisesRegex(TaskflowError, "valid score delivery"):
            case.read()

    def test_manifest_path_attempt_or_reference_redirect_is_rejected(self):
        case = self.case
        current = case.solver["state"]["invocation"]
        current["runtime"] += "-different"
        case.save(case.solver)
        with self.assertRaisesRegex(TaskflowError, "invocation differs"):
            case.read()
        current["runtime"] = str(Path(current["root"]) / "runtime")
        case.save(case.solver)
        case.connection.execute("UPDATE attempts SET data=? WHERE id=?", ("{}", current["attempt_id"]))
        case.connection.commit()
        with self.assertRaisesRegex(TaskflowError, "attempt/SID"):
            case.read()

    def test_symlink_and_missing_database_never_create_database(self):
        case = self.case
        missing = case.root / "missing.sqlite3"
        with self.assertRaises(TaskflowError):
            load_report_sources(missing, "solver")
        self.assertFalse(missing.exists())
        link = case.root / "link.sqlite3"
        link.symlink_to(case.database)
        with self.assertRaises(TaskflowError):
            load_report_sources(link, "solver")
        receipt = Path(case.review["state"]["score_receipt"]["path"])
        original = receipt.with_name("original.json")
        receipt.rename(original)
        receipt.symlink_to(original)
        with self.assertRaisesRegex(TaskflowError, "ordinary path"):
            case.read()

    def test_structurally_broken_source_is_actionable_error(self):
        case = self.case
        manifest = Path(case.solver["state"]["invocation"]["runtime"]) / "manifest.json"
        manifest.write_text("[]")
        with self.assertRaisesRegex(TaskflowError, "JSON must be an object"):
            case.read()
        manifest.write_text("{")
        with self.assertRaisesRegex(TaskflowError, "cannot be authenticated"):
            case.read()


if __name__ == "__main__":
    unittest.main()
