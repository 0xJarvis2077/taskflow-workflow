"""Hash-bound single/multi-item authority tests; no model or native probes."""

import copy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _packet, advance_experiment
from taskflow.execution_formal import prepare_formal, validate_formal_authority
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment_author as author_fixtures
from tests import test_execution_formal as formal_fixtures


class AdapterBoundaryReached(RuntimeError):
    """Stop immediately after the real authorization and source-hash gates."""


class FormalAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        adapter = self.root / "adapter"
        adapter.mkdir()
        for name in ("prepare_diagnostic.py", "runtime_gate.py", "execute_diagnostic.py", "file_evidence.py"):
            (adapter / name).write_text("# Neutral source fixture; never executed.\n", encoding="utf-8")
        write_json(adapter / "baseline-plan.json", {"nature": "neutral baseline fixture; no model calls"})
        self.baseline = {"path": str(adapter / "baseline-plan.json"), **_info(adapter / "baseline-plan.json")}
        self.request = self.root / "request.md"
        self.request.write_text("Read the exact public neutral inputs and deliver the requested result.\n", encoding="utf-8")
        self.candidate = {"task_id": "neutral"}
        self.selector = {"task_id": "neutral", "source_author_quality_flow": "exact-author-quality"}
        self.item = {**self.selector, "first_run_id": "neutral-01", "conditional_one_confirmation_run_id": "neutral-02"}
        self.packet = {"schema_version": 1, "controller": "fixture-controller", "items": [self.item, {
            "task_id": "another-task", "source_author_quality_flow": "another-author-quality",
            "first_run_id": "foreign-01", "conditional_one_confirmation_run_id": "foreign-02"}],
            "formal_baseline": {**self.baseline, "model_requested": "gpt-5.6-terra", "reasoning_override": None,
                                "client_prior_resolution": "medium/all_turns; actual same profile verification required",
                                "timeout_seconds": 3600, "served_revision": None}}
        self.authority = self.root / "business.json"
        self.config = {"adapter": {"path": str(adapter / "prepare_diagnostic.py"), **_info(adapter / "prepare_diagnostic.py"),
            "files": {name: _info(adapter / name)["sha256"] for name in
                      ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")}},
            "request": {"path": str(self.request), **_info(self.request)}, "authorization_item": self.selector,
            "run_id": "neutral-01", "intent_id": "neutral-formal-intent", "policy_version": "fixture-v1",
            "launcher_python": sys.executable, "model": "gpt-5.6-terra", "timeout_seconds": 3600,
            "reasoning_effort": "medium", "reasoning_context": "all_turns", "reasoning_selection": "verified_client_default",
            "deny_read_probes": []}
        self.publish()

    def publish(self, packet=None):
        write_json(self.authority, self.packet if packet is None else packet)
        self.config["authorization"] = evidence_ref(self.authority, "neutral hash-bound formal authority")

    def resolve(self, config=None):
        return validate_formal_authority(config=self.config if config is None else config, candidate=self.candidate)

    def test_existing_single_packet_keeps_its_explicit_run_model_timeout_and_baseline(self):
        packet = {"controller": "fixture-controller", "task_id": "neutral",
                  "formal": {"first_run_id": "neutral-01", "model": "gpt-5.6-terra", "timeout_seconds": 3600,
                             "baseline": self.baseline}, "single_confirmation": {"run_id": "neutral-02"}}
        self.publish(packet)
        self.config.pop("authorization_item")
        self.assertEqual(packet, self.resolve())
        self.config["run_id"] = "neutral-02"
        self.assertEqual(packet, self.resolve())
        self.config["run_id"] = "neutral-03"
        with self.assertRaisesRegex(TaskflowError, "outside the explicit business packet"):
            self.resolve()

    def test_exact_multi_item_normalizes_only_the_original_bound_item_and_baseline(self):
        original = self.authority.read_bytes()
        normalized = self.resolve()
        self.assertEqual("neutral", normalized["task_id"])
        self.assertEqual({"first_run_id": "neutral-01", "model": "gpt-5.6-terra", "timeout_seconds": 3600,
                          "baseline": {"path": self.baseline["path"], "sha256": self.baseline["sha256"]}}, normalized["formal"])
        self.assertEqual({"run_id": "neutral-02"}, normalized["single_confirmation"])
        self.assertEqual(original, self.authority.read_bytes())
        self.assertEqual(self.selector, self.config["authorization_item"])
        self.config["run_id"] = "neutral-02"
        self.assertEqual("neutral-02", self.resolve()["single_confirmation"]["run_id"])

    def test_missing_wrong_or_ambiguous_selector_and_other_item_run_cannot_pass(self):
        cases = [None, {}, {"task_id": "other-task", "source_author_quality_flow": "exact-author-quality"},
                 {"task_id": "neutral", "source_author_quality_flow": "another-author-quality"},
                 {**self.selector, "extra": "not permitted"}]
        for selector in cases:
            config = copy.deepcopy(self.config)
            config.pop("authorization_item")
            if selector is not None:
                config["authorization_item"] = selector
            with self.subTest(selector=selector), self.assertRaises(TaskflowError):
                self.resolve(config)
        for run_id in ("foreign-01", "foreign-02", "neutral-03"):
            with self.subTest(run=run_id), self.assertRaisesRegex(TaskflowError, "outside the explicit business packet"):
                self.resolve({**self.config, "run_id": run_id})
        self.packet["items"].append(copy.deepcopy(self.item))
        self.publish()
        with self.assertRaisesRegex(TaskflowError, "one exact task/source-flow item"):
            self.resolve()

    def test_model_timeout_baseline_and_original_authority_sha_drift_are_rejected(self):
        for field, value in (("model", "another-model"), ("timeout_seconds", 3599)):
            with self.subTest(field=field), self.assertRaisesRegex(TaskflowError, "outside the explicit business packet"):
                self.resolve({**self.config, field: value})
        config = copy.deepcopy(self.config)
        config["adapter"]["files"]["baseline-plan.json"] = "f" * 64
        with self.assertRaisesRegex(TaskflowError, "baseline differs"):
            self.resolve(config)
        packet = copy.deepcopy(self.packet)
        packet["formal_baseline"]["sha256"] = "e" * 64
        self.publish(packet)
        with self.assertRaisesRegex(TaskflowError, "baseline differs"):
            self.resolve()
        self.publish()
        self.authority.write_bytes(self.authority.read_bytes() + b"\n")
        with self.assertRaisesRegex(TaskflowError, "bound source changed"):
            self.resolve()

    def test_real_prepare_boundary_reaches_only_post_authorization_adapter_subprocess(self):
        case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        released = case.release()
        runtime = self.root / "actual-prepare-boundary"
        with patch("taskflow.execution_formal.subprocess.run", side_effect=AdapterBoundaryReached("authorized adapter boundary")) as execute:
            with self.assertRaisesRegex(AdapterBoundaryReached, "authorized adapter boundary"):
                prepare_formal(config=self.config, release=released, candidate=case.candidate, runtime=runtime)
        execute.assert_called_once()
        argv = execute.call_args.args[0]
        self.assertIn(str(Path(self.config["adapter"]["path"])), argv)
        self.assertEqual("neutral-01", argv[argv.index("--run-id") + 1])
        self.assertEqual(released["solver"]["path"], argv[argv.index("--solver") + 1])
        self.assertFalse(runtime.exists())
        self.assertTrue(runtime.with_name(runtime.name + ".formal-intent.json").is_file())
        bad = copy.deepcopy(self.config)
        bad.pop("authorization_item")
        with patch("taskflow.execution_formal.subprocess.run", side_effect=AssertionError("must not reach adapter")) as execute:
            with self.assertRaisesRegex(TaskflowError, "outside the explicit business packet"):
                prepare_formal(config=bad, release=released, candidate=case.candidate, runtime=self.root / "rejected-prepare")
        execute.assert_not_called()

    def test_real_experiment_mapping_passes_only_immutable_item_selector_to_actual_validator(self):
        helper = author_fixtures.AuthorExperimentTests("test_actual_complete_candidate_binds_exact_whole_quality_child")
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        case, source, candidate = helper.candidate_source()
        definition = helper.definition(case, source)
        packet_path = Path(definition["authorization"]["path"])
        packet = read_json(packet_path)
        packet["formal_baseline"] = copy.deepcopy(self.packet["formal_baseline"])
        write_json(packet_path, packet)
        reference = evidence_ref(packet_path, "neutral full multi-item downstream authority")
        definition.update(authorization=reference, business_packet_ref=reference)
        base = copy.deepcopy(self.config)
        base.update(authorization=reference)
        for key in ("run_id", "intent_id", "policy_version", "authorization_item"):
            base.pop(key, None)
        definition["technical"].update(formal_config_base=base, formal_runtime_root=str(self.root / "experiment-formal"))
        normalized = _packet({"definition": definition})
        self.assertEqual(packet["items"][0]["first_run_id"], normalized["formal"]["first_run_id"])
        self.assertEqual(packet["items"][0]["conditional_one_confirmation_run_id"], normalized["single_confirmation"]["run_id"])
        with patch("taskflow.execution_experiment._native_handoff", side_effect=helper.native_interface_fixture):
            record = advance_experiment(case.store, helper.register(case, definition))
        self.assertEqual("release", record["state"]["stage"], record["state"])
        state = {**record["state"], "stage": "formal_prepare", "release": {"nature": "authority transport fixture only"}}
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_formal_authority_boundary"})
        captured = []

        def validate_then_stop(**kwargs):
            actual = kwargs["config"]
            self.assertEqual(definition["business_packet_item"], actual["authorization_item"])
            self.assertEqual(reference, actual["authorization"])
            resolved = validate_formal_authority(config=actual, candidate=kwargs["candidate"])
            self.assertEqual(normalized["formal"]["first_run_id"], actual["run_id"])
            self.assertEqual("gpt-5.6-terra", resolved["formal"]["model"])
            self.assertEqual(candidate, kwargs["candidate"])
            captured.append(actual)
            raise AdapterBoundaryReached("experiment called the actual authority validator")

        with patch("taskflow.execution_experiment.prepare_formal", side_effect=validate_then_stop):
            with self.assertRaisesRegex(AdapterBoundaryReached, "actual authority validator"):
                advance_experiment(case.store, record)
        self.assertEqual(1, len(captured))
        self.assertNotIn("authorization_item", definition["technical"]["formal_config_base"])
        self.assertEqual(3, len(case.store.dashboard()["attempts"]))
        self.assertFalse(any(work["role"] == "solver" for work in case.store.dashboard()["work"]))


if __name__ == "__main__":
    unittest.main()
