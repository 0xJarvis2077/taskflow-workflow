"""Full neutral native intake/resume, using local fixture subprocesses only."""

import copy
from datetime import datetime, timezone
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow import execution, execution_review
from taskflow.contracts import PUBLIC_ROLES, TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from taskflow.records import import_run
from tests import test_execution_experiment as fixtures


class NamedSourceIntakeResumeTests(unittest.TestCase):
    receive_in_setup = True

    def import_named_fixture(self, *, attempt, archive, snapshot, run_id):
        case = self.case
        incoming = case.root / "incoming" / run_id
        incoming.mkdir(parents=True)
        manifest = read_json(archive / "manifest.json")
        shutil.copyfile(Path(manifest["cwd"]) / "answer.md", incoming / "answer.md")
        shutil.copyfile(archive / "trace.jsonl", incoming / "trace.jsonl")
        artifacts = [{"id": "answer", "path": "answer.md", "kind": "answer"},
                     {"id": "trace", "path": "trace.jsonl", "kind": "trace"}]
        for item in case.candidate["files"]:
            if item["role"] not in PUBLIC_ROLES:
                continue
            target = incoming / "capture/trees/work" / item["package_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item["path"], target)
            artifacts.append({"id": "public-" + target.name.replace(".", "-"),
                              "path": str(target.relative_to(incoming)), "kind": "evidence"})
        self.name = next(item["package_path"] for item in case.candidate["files"] if item["role"] in PUBLIC_ROLES)
        peer = incoming / "capture/trees/work/same-bytes/peer.txt"
        peer.parent.mkdir(parents=True)
        shutil.copyfile(incoming / "capture/trees/work" / self.name, peer)
        artifacts.append({"id": "same-byte-peer", "path": str(peer.relative_to(incoming)), "kind": "evidence"})
        rows = [{"id": item["id"], "incoming_path": item["path"],
                 "visibility": "actual_trace" if item["id"] == "trace" else "submission" if item["id"] == "answer" else "public",
                 **_info(incoming / item["path"])} for item in artifacts]
        index = incoming / "indexes/first_review_index.json"
        write_json(index, {"allowed_direct_artifacts": rows})
        artifacts.append({"id": "first_review_index", "path": "indexes/first_review_index.json", "kind": "evidence"})
        now = datetime.now(timezone.utc).isoformat()
        write_json(incoming / "run.json", {"schema_version": 1, "run_id": run_id, "nature": "test_fixture", "status": "completed",
            "actual_input_digest": case.candidate["input_digest"],
            "requested_config": {"model": "fixture-cli", "tools": [], "reasoning_selection": "verified_client_default", "request_reasoning_override": None},
            "observed_config": {"model": "fixture-cli", "tools": [], "client_resolved": "fixture-cli",
                "client_resolved_reasoning": {"effort": "fixture", "context": "all_turns"}},
            "started_at": now, "ended_at": now, "interventions": [], "exceptions": [],
            "input_evidence": [rows[-1]["id"] + "#public"], "isolation_evidence": ["trace#fixture-only"], "artifacts": artifacts})
        imported = import_run(snapshot, incoming / "run.json")
        inputs, _ = execution_review._first_inputs({"first_review_index": str(index)}, imported)
        return {"run_path": imported["path"], "run_digest": imported["run_digest"], "first_review_index": str(index), "first_review_inputs": inputs}

    def setUp(self):
        case = fixtures.ExperimentBoundaryTests("test_real_fixture_workers_traverse_import_facts_seal_score_and_original_intake")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        record = case.ready()
        execution.run_worker(self.store, "neutral-original", False, [record["state"]["current_work_id"]])
        with patch("taskflow.execution_experiment.import_formal", side_effect=self.import_named_fixture):
            record = case.advance()
        with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=case.fixture_role):
            record = case.advance()
        execution.run_worker(self.store, "neutral-facts", False, [record["state"]["current_work_id"]])
        record = case.advance()
        with patch("taskflow.execution_experiment.prepare_score_runner", side_effect=lambda **kwargs: case.fixture_role(scoring=True, **kwargs)):
            record = case.advance()
        execution.run_worker(self.store, "neutral-score", False, [record["state"]["current_work_id"]])
        record = case.advance()
        self.assertEqual("evaluation_intake", record["state"]["stage"])
        record = advance_lifecycle(self.store, record["flow_id"])
        record = advance_lifecycle(self.store, record["flow_id"])
        self.assertEqual({"stage": "evaluation_intake", "reason": "signed source has no unique same-byte native run artifact: " + self.name}, record["state"]["exception"])
        self.record = record
        state = record["state"]
        self.config = {"schema_version": 1, "mode": "managed_two_stage", "run_directory": state["imported_run"]["run_path"],
            "snapshot_directory": state["release"]["snapshot"]["path"], "evaluation_id": "fixture-eval-01",
            "facts_attempt": state["facts_attempt"]["runtime_path"], "facts_binding": state["facts_prepared"]["review_binding"],
            "score_attempt": state["score_attempt"]["runtime_path"], "score_binding": state["score_prepared"]["review_binding"],
            "expected_sid": state["facts_seal"]["sid"], "score_file": "score.json", "explanation_file": "explanation.md",
            "evidence": [record["definition"]["authorization"]], "source_path_map": {self.name: "capture/trees/work/" + self.name}}
        self.destination = Path(record["definition"]["directory"]) / "evaluations/fixture-01"
        if self.receive_in_setup:
            self.receive()

    def receive(self):
        self.result = execution_review.ingest_signed_evaluation(config=self.config, destination=self.destination)
        self.assertTrue(self.result["accepted"])
        self.resolution = {"action": "resume_validated_evaluation_intake", "reason": "Consume the already received named neutral original.",
            "evidence": [self.record["definition"]["authorization"]], "receipt": evidence_ref(Path(self.result["receipt"]), "existing exact neutral intake")}
        return self.result

    def test_existing_map_intake_resumes_then_ordinary_stage_uses_same_evaluation(self):
        board = self.store.dashboard()
        exception = Path(self.record["state"]["receipt"]).read_bytes()
        score_path = Path(self.config["score_attempt"]) / "artifacts/score.json"
        original = score_path.read_bytes()
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("no new worker")):
            resumed = resume_path_transport(self.store, self.record["flow_id"], self.resolution)
            self.assertEqual("evaluation_intake", resumed["state"]["stage"])
            self.assertEqual("active", resumed["state"]["status"])
            result = advance_lifecycle(self.store, self.record["flow_id"])
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(1, len(result["state"]["runs"]))
        self.assertEqual(board["attempts"], self.store.dashboard()["attempts"])
        self.assertEqual(board["decisions"], self.store.dashboard()["decisions"])
        self.assertEqual(original, score_path.read_bytes())
        self.assertEqual(exception, Path(self.record["state"]["receipt"]).read_bytes())
        evaluations = Path(self.config["run_directory"]) / "evaluations"
        self.assertEqual(["fixture-eval-01"], [path.name for path in evaluations.iterdir() if path.is_dir()])

    def test_wrong_sid_forged_receipt_changed_map_and_archive_are_rejected(self):
        receipt_path = Path(self.result["receipt"])
        original_receipt = read_json(receipt_path)
        for field, value in (("signer_sid", "wrong-sid"), ("source_sha256", "b" * 64), ("accepted", False)):
            write_json(receipt_path, {**original_receipt, field: value})
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                resume_path_transport(self.store, self.record["flow_id"], {**self.resolution,
                    "receipt": evidence_ref(receipt_path, "negative neutral receipt")})
        write_json(receipt_path, original_receipt)
        binding_path = self.destination / "binding.json"
        original_binding = read_json(binding_path)
        for mapping in ({"unknown": "capture/unknown"}, {self.name: "capture/trees/work/same-bytes/peer.txt"}):
            changed = copy.deepcopy(original_binding)
            changed["config"]["source_path_map"] = mapping
            write_json(binding_path, changed)
            with self.subTest(mapping=mapping), self.assertRaises(TaskflowError):
                resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        write_json(binding_path, original_binding)
        extra = self.destination / "full-archive/score/unexplained.txt"
        extra.write_text("neutral extra", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            resume_path_transport(self.store, self.record["flow_id"], self.resolution)
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
