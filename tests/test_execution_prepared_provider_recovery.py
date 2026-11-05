"""Prepared provider metadata recovery uses frozen zero-model local fixtures."""

import copy
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_formal import _binding, _prepared_formal_plan, validate_prepared_formal_cli
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.packages import verify_snapshot
from tests import test_execution_external_formal_closure as closure_fixtures
from tests import test_execution_lifecycle as lifecycle_fixtures


class PreparedProviderRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = lifecycle_fixtures.FormalPreparationResumeTests()
        register = ExecutionStore.register_flow

        def configure(store, identity, definition, evidence, state):
            if definition.get("kind") == "experiment_v1":
                root = Path(definition["directory"]).parent
                source = root / "adapter-source"
                source.mkdir()
                provider_path = root / "provider.json"
                write_json(provider_path, {"nature": "test_fixture", "not_a_service": True})
                provider = evidence_ref(provider_path, "baseline provenance")
                for name in ("prepare_diagnostic.py", "runtime_gate.py", "execute_diagnostic.py", "file_evidence.py"):
                    (source / name).write_text("# Neutral fixture; never executed.\n", encoding="utf-8")
                write_json(source / "baseline-plan.json", {"native_provider_ref": provider})
                adapter = evidence_ref(source / "prepare_diagnostic.py", "approved fixture adapter")
                adapter["files"] = {name: sha256((source / name).read_bytes()) for name in
                                    ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")}
                request = root / "request.txt"
                request.write_text("Solve the public fixture.\n", encoding="utf-8")
                definition["technical"]["formal_config_base"].update(
                    adapter=adapter, native_provider={**provider, "location": "selected provenance"},
                    request=evidence_ref(request, "neutral request"), launcher_python=sys.executable)
            return register(store, identity, definition, evidence, state)

        with patch.object(ExecutionStore, "register_flow", configure):
            self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.case = self.fixture.case
        self.store = self.case.store
        record, self.receipt = self.fixture.failure()
        definition = record["definition"]
        self.runtime = Path(definition["technical"]["formal_runtime_root"]) / "fixture-01"
        self.runtime.mkdir(parents=True)
        self.config = {**definition["technical"]["formal_config_base"], "run_id": "fixture-01",
                       "intent_id": _node(record, "solver") + "-intent", "policy_version": definition["policy_id"]}
        source = Path(self.config["adapter"]["path"]).parent
        for path in source.iterdir():
            shutil.copyfile(path, self.runtime / path.name)
        self.frozen = verify_snapshot(self.case.snapshot)
        baseline = read_json(source / "baseline-plan.json")
        self.plan = {"run_id": "fixture-01", "requested_model": self.config["model"],
                     "input_digest": self.case.candidate["input_digest"],
                     "request_sha256": self.config["request"]["sha256"],
                     "frozen_binding": {"snapshot_digest": self.frozen["snapshot_digest"]},
                     "launcher_python": str(Path(sys.executable).resolve()),
                     "native_provider_ref": baseline["native_provider_ref"], "argv": [sys.executable]}
        self.save_plan()
        write_json(self.runtime / "preparation-process.json", {"exit_code": 0, "model_calls": 0})
        write_json(self.runtime / "exec-context.json", {"model_calls": 0, "external_model_requests": 0})
        write_json(self.runtime.with_name(self.runtime.name + ".formal-intent.json"), {
            "config_digest": sha256(canonical(self.config)), "release_receipt": record["state"]["release"]["receipt"],
            "candidate_binding": _binding(self.case.candidate), "runtime": str(self.runtime)})
        exception = {"stage": "formal_prepare", "reason": "formal plan custom provider differs from selected configuration"}
        write_json(self.receipt, {"flow_id": record["flow_id"], "status": "exception", "exception": exception})
        self.record = self.store.update_flow(record["flow_id"], record["revision"],
            {**record["state"], "exception": exception}, {"kind": "neutral_prepared_metadata_exception_fixture"})
        self.resolution = {**self.fixture.resolution, "action": "resume_prepared_formal_provider_metadata"}

    def save_plan(self):
        write_json(self.runtime / "execution-plan.json", self.plan)
        write_json(self.runtime / "plan-lock.json", {"sha256": sha256((self.runtime / "execution-plan.json").read_bytes())})

    def resume(self):
        with self.fixture.validate_authority(), patch("subprocess.run", side_effect=AssertionError("resume cannot execute")):
            return resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def test_verified_existing_plan_resumes_without_reprepare_then_closes_once(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_plan_reference_keeps_all_identity_fields_and_verified_actual_bytes(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_default_provider_and_invalid_reference_shapes_keep_plan_gate(self):
        config = {**self.config, "native_provider": None}
        self.plan["native_provider_ref"] = None
        self.save_plan()
        _prepared_formal_plan(config, self.case.candidate, self.frozen, self.runtime,
                              self.config["adapter"]["sha256"])
        for invalid in (None, "provider", []):
            self.plan["native_provider_ref"] = invalid
            self.save_plan()
            with self.subTest(invalid=invalid), self.assertRaises(TaskflowError):
                _prepared_formal_plan(self.config, self.case.candidate, self.frozen, self.runtime,
                                      self.config["adapter"]["sha256"])

    def test_existing_prepared_cli_validation_keeps_bound_plan_and_never_executes_model(self):
        normal = self.runtime / "normal-tools-probe.json"
        write_json(normal, {"model_calls": 0, "external_model_requests": 0})
        intent = read_json(self.runtime.with_name(self.runtime.name + ".formal-intent.json"))
        write_json(self.runtime / "launch-review.json", {
            "approved": True, "plan_sha256": sha256((self.runtime / "execution-plan.json").read_bytes()),
            "input_digest": self.case.candidate["input_digest"], "preparation_binding": intent,
            "normal_tools": evidence_ref(normal, "fixture zero-model check")})
        with self.fixture.validate_authority(), patch("subprocess.run", return_value=
                subprocess.CompletedProcess([], 0, b"", b"")) as validate:
            result = validate_prepared_formal_cli(config=self.config,
                release=self.record["state"]["release"], candidate=self.case.candidate, runtime=self.runtime)
        self.assertEqual(self.plan["argv"][0], result["cli"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(1, validate.call_count)
        self.assertNotIn("--execute", validate.call_args.args[0])
        self.assertEqual(str(self.runtime / "execute_diagnostic.py"), validate.call_args.args[0][-1])

    def test_existing_work_hold_changed_source_or_external_request_count_blocks_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_uncertain_evidence_nonzero_or_missing_counts_and_intent_drift_never_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
