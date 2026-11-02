"""A zero-model recovery ref belongs to its original formal run only.

The accepted-intake/observed-configuration seam is the existing explicit neutral
state-machine fixture. SQLite/CAS, eligibility, request preparation and exact
work enqueue are real. Native formal preparation uses a local fixture adapter;
this suite does not claim a real model, permission profile or signed business result.
"""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_experiment import _advance, _node, advance_experiment
from taskflow.execution_formal import prepare_formal
from taskflow.execution_runner import _validate
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as experiment_fixtures


class ConfirmationRecoveryScopeTests(unittest.TestCase):
    def setUp(self):
        self.case = experiment_fixtures.ExperimentBoundaryTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.result, self.observed = self.case.intake_state()
        self.store = self.case.store
        record = self.store.get_flow(self.case.record["flow_id"])
        release = record["state"]["release"]
        config = {**record["definition"]["technical"]["formal_config_base"], "run_id": "fixture-01",
                  "intent_id": _node(record, "solver") + "-intent", "policy_version": "fixture-v1"}
        runtime = Path(record["definition"]["technical"]["formal_runtime_root"]) / "fixture-01"
        self.proof = {"schema_version": 1, "kind": "validated_zero_inference_public_probe_source_recovery",
            "completed": True, "model_calls": 0, "run_id": "fixture-01", "runtime": str(runtime),
            "binding": {"config_digest": sha256(canonical(config)), "release_receipt": release["receipt"],
                        "candidate_binding": release["candidate_binding"], "runtime": str(runtime)}}
        # Only opaque prior recovery metadata is simulated here. Its completed
        # native/host proof admission is exercised by the existing recovery tests.
        self.proof_path = self.case.root / "original-run-recovery-header.json"
        write_json(self.proof_path, self.proof)
        self.reference = evidence_ref(self.proof_path, "neutral original-run recovery header; no model call")
        self.original_history = {"kind": "public_probe_formal_preparation_technical_resolution",
                                 "preparation": self.reference, "proof": copy.deepcopy(self.proof)}
        state = {**record["state"], "formal_preparation_recovery": self.reference,
                 "history": [*record["state"]["history"], self.original_history]}
        self.original = self.store.update_flow(record["flow_id"], record["revision"], state,
                                              {"kind": "neutral previously admitted first-run recovery"})
        original_paths = [self.proof_path, Path(record["state"]["formal_runtime"]) / "local-fixture.py",
                          Path(record["state"]["formal_outer"]) / "request.md",
                          Path(record["state"]["formal_outer"]) / "manifest.json", self.case.snapshot / "task.json"]
        self.assertTrue(all(path.is_file() for path in original_paths))
        self.original_bytes = {path: path.read_bytes() for path in original_paths}

    def accepted_intake(self):
        with patch("taskflow.execution_experiment.ingest_signed_evaluation", return_value=self.result), \
             patch("taskflow.execution_experiment.verify_run", return_value=self.observed):
            return self.case.advance()

    def test_eligible_confirmation_retires_only_active_ref_and_prepares_one_new_run_without_it(self):
        confirmation = self.accepted_intake()
        self.assertEqual("formal_prepare", confirmation["state"]["stage"])
        self.assertEqual(1, confirmation["state"]["run_index"])
        self.assertNotIn("formal_preparation_recovery", confirmation["state"])
        self.assertIn(self.original_history, confirmation["state"]["history"])
        retired = [row for row in confirmation["state"]["history"] if row["kind"] == "run_preparation_recovery_retired"]
        self.assertEqual([{"kind": "run_preparation_recovery_retired", "run_id": "fixture-01", "preparation": self.reference}], retired)
        calls = []
        def neutral_preparer(**kwargs):
            self.assertNotIn("preparation_recovery", kwargs)
            self.assertEqual("fixture-02", kwargs["config"]["run_id"])
            self.assertEqual(self.case.candidate, kwargs["candidate"])
            self.assertEqual(confirmation["state"]["release"], kwargs["release"])
            runtime = kwargs["runtime"]
            self.assertEqual(self.case.root / "formal/fixture-02", runtime)
            work = runtime / "work"
            work.mkdir(parents=True)
            script = runtime / "neutral-confirmation.py"
            script.write_text("# Prepared neutral confirmation; never executed.\n")
            calls.append(copy.deepcopy(kwargs))
            return {"runner": {"intent_id": kwargs["config"]["intent_id"], "policy_version": "fixture-v1", "role": "solver",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), str(script)], "dependencies": [str(script)],
                "inputs": [], "request_sections": [{"kind": "operation", "text": "Neutral confirmation ready request only."}], "outputs": ["answer.md"]}}
        with patch("taskflow.execution_experiment.prepare_formal", side_effect=neutral_preparer):
            ready = self.case.advance()
            first_wait = self.case.advance()
            again = self.case.advance()
        self.assertEqual(1, len(calls))
        self.assertEqual("formal_wait", ready["state"]["stage"])
        self.assertEqual(first_wait, again)
        self.assertEqual(ready["state"]["current_work_id"], again["state"]["current_work_id"])
        work = self.store.get_work(ready["state"]["current_work_id"])
        self.assertEqual("ready", work["state"])
        self.assertEqual([], self.store.list_attempts(work["work_id"]))
        runtime = Path(work["runtime_path"])
        manifest = _validate(runtime)
        self.assertEqual(manifest["request"]["sha256"], sha256((runtime / "request.md").read_bytes()))
        self.assertEqual("fixture-02", Path(manifest["cwd"]).parent.name)
        self.assertNotIn(self.reference["path"], (runtime / "request.md").read_text())
        for path, raw in self.original_bytes.items():
            self.assertEqual(raw, path.read_bytes())
        state = {**again["state"], "stage": "evaluation_intake"}
        self.store.update_flow(again["flow_id"], again["revision"], state, {"kind": "neutral confirmation receipt"})
        final = self.accepted_intake()
        self.assertEqual("accepted", final["state"]["status"])
        self.assertEqual(1, final["state"]["run_index"])
        self.assertIn("single_confirmation_already_used", final["state"]["result"]["confirmation_eligibility"]["reasons"])
        self.assertEqual(final, self.case.advance())
        self.assertEqual(2, len(final["state"]["runs"]))
        self.assertEqual([], self.store.list_attempts(work["work_id"]))

    def test_explicit_original_proof_still_cannot_authorize_second_run_preparation(self):
        confirmation = self.accepted_intake()
        config = {**confirmation["definition"]["technical"]["formal_config_base"], "run_id": "fixture-02",
                  "intent_id": _node(confirmation, "solver") + "-intent", "policy_version": "fixture-v1"}
        runtime = self.case.root / "formal/fixture-02"
        with self.assertRaisesRegex(TaskflowError, "proof does not bind"):
            prepare_formal(config=config, release=confirmation["state"]["release"], candidate=self.case.candidate,
                           runtime=runtime, preparation_recovery=self.reference)
        self.assertFalse(runtime.exists())
        self.assertEqual(self.proof, read_json(self.proof_path))
        self.assertEqual(confirmation, self.store.get_flow(confirmation["flow_id"]))

    def test_ineligible_original_does_not_clear_its_active_recovery_reference(self):
        self.result["local_profile_assessment"]["local_eligible"] = "unknown"
        final = self.accepted_intake()
        self.assertEqual("accepted", final["state"]["status"])
        self.assertEqual(0, final["state"]["run_index"])
        self.assertEqual(self.reference, final["state"]["formal_preparation_recovery"])
        self.assertIn(self.original_history, final["state"]["history"])
        self.assertFalse(final["state"]["result"]["confirmation_dispatched"])
        self.assertEqual([], [row for row in final["state"]["history"] if row["kind"] == "run_preparation_recovery_retired"])


if __name__ == "__main__":
    unittest.main()
