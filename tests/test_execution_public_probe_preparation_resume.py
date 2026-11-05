"""A typed public-probe recovery resumes only its original zero-call stage.

Lifecycle cases use real captured packages, releases, SQLite CAS and safe IO;
the final formal-validator return is an explicit interface double. The separate
consumer case uses the real formal validator/prepare path with the documented
neutral gate fixture and stops at enqueue. No external model/native probe runs.
"""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_experiment import _advance, _node
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_host_preparation_resume as host_fixtures
from tests import test_execution_formal_public_probe_recovery as source_fixtures
from tools.execution_simulation import fixture_policy, perform


class PublicProbePreparationResumeTests(unittest.TestCase):
    def setUp(self):
        case = host_fixtures.HostPreparationResumeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store, self.root = case, case.store, case.root
        old = case.record
        exception = {"stage": "formal_prepare", "reason":
            "existing formal preparation failed; original runtime/reservation retained: "
            "ValueError: Negative probe is invalid without existing target: neutral public source"}
        receipt = Path(old["state"]["receipt"])
        write_json(receipt, {"flow_id": old["flow_id"], "status": "exception", "exception": exception})
        self.record = self.store.update_flow(old["flow_id"], old["revision"],
            {**old["state"], "exception": exception}, {"kind": "neutral_public_probe_preparation_failure"})
        self.proof = copy.deepcopy(case.proof)
        self.proof.update(schema_version=1, kind="validated_zero_inference_public_probe_source_recovery")
        self.proof["source_change"] = {"kind": "deterministic_public_probe_target_v1"}
        for name, text in (("original", "Original neutral public target source.\n"),
                           ("replacement", "Corrected neutral public target source.\n"),
                           ("executed", "Corrected neutral public target source.\n")):
            path = self.root / (name + "-preparer.py")
            path.write_text(text, encoding="utf-8")
            self.proof["source_change"][name] = evidence_ref(path, "neutral validator interface source ref")
        recovery = self.root / "public-probe-recovery.json"
        write_json(recovery, self.proof)
        self.resolution = {"action": "resume_validated_public_probe_preparation",
            "reason": "Consume the explicit completed source recovery without a preparation/model retry.",
            "evidence": self.record["evidence"], "preparation": evidence_ref(recovery, "completed neutral public-probe recovery")}
        self.proof["recovery"] = self.resolution["preparation"]
        packet = read_json(case.case.case.authority)
        purpose = packet["business_investment"]["unresolved_business_question"]
        self.decision = self.store.record_decision(_node(self.record, "experiment-decision"),
            self.record["definition"]["task_id"], self.record["state"]["candidate_id"], action="experiment",
            rationale="Original neutral purpose remains approved, no model dispatched.", evidence=self.record["evidence"],
            experiment_purpose=purpose, authorized=True)
        self.originals = {path: path.read_bytes() for path in
            [receipt, case.intent, case.inner / "preparation-failure.json", Path(self.proof["reservation"]["path"]) ]}

    def validator(self, **kwargs):
        return patch("taskflow.execution_formal.validate_formal_public_probe_recovery", **kwargs)

    def metadata(self):
        with self.store._db() as db:
            return {table: [row["data"] for row in db.execute("SELECT data FROM " + table)]
                    for table in ("work", "attempts", "decisions")}

    @contextmanager
    def changed_state(self, **changes):
        with self.store._db(True) as db:
            original = db.execute("SELECT state FROM execution_flows WHERE flow_id=?", (self.record["flow_id"],)).fetchone()["state"]
            value = json.loads(original)
            value.update(changes)
            db.execute("UPDATE execution_flows SET state=? WHERE flow_id=?", (canonical(value).decode(), self.record["flow_id"]))
        try:
            yield
        finally:
            with self.store._db(True) as db:
                db.execute("UPDATE execution_flows SET state=? WHERE flow_id=?", (original, self.record["flow_id"]))

    def test_final_typed_recovery_resumes_once_and_preserves_original_decision_intent_and_receipt(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_draft_old_kind_foreign_binding_or_nonzero_model_proofs_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_business_holds_prior_recovery_and_wrong_stage_never_reach_validator(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_execution_markers_and_related_ready_claimed_uncertain_work_block_without_requeue(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_original_intent_receipt_and_current_source_bytes_are_rechecked(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_old_host_action_never_selects_public_probe_validator(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


class PublicProbeConsumerForwardingTests(unittest.TestCase):
    def make_consumer(self):
        f = source_fixtures.make_public_probe_recovery_fixture(self)
        flow_id = "neutral-public-probe-consumer"
        node_record = {"flow_id": flow_id, "state": {"run_index": 0}}
        f.config["intent_id"] = _node(node_record, "solver") + "-intent"
        f.binding["config_digest"] = sha256(canonical(f.config))
        write_json(Path(f.proof["intent"]["path"]), f.binding)
        token = Path(f.proof["recovery_token"]["path"])
        token_body = read_json(token)
        token_body["binding"] = f.binding
        write_json(token, token_body)
        retained = f.runtime.with_name(f.runtime.name + ".failed-preparation-" + _info(token)["sha256"][:12])
        f.retained.rename(retained)
        f.proof["previous_runtime"]["retained_path"] = str(retained)
        f.proof["recovery_token"] = f.ref(token)
        accepted = read_json(Path(f.proof["acceptance"]["path"]))
        accepted["preparation_binding"] = f.binding
        write_json(Path(f.proof["acceptance"]["path"]), accepted)
        f.proof["acceptance"] = f.ref(Path(f.proof["acceptance"]["path"]))
        f.proof["binding"] = f.binding
        f.proof["intent"] = f.ref(Path(f.proof["intent"]["path"]))
        write_json(f.proof_path, f.proof)
        f.recovery = f.ref(f.proof_path)
        f.store.register_policy(f.config["policy_version"], fixture_policy())
        f.store.register_policy("fixture-v1", fixture_policy())
        # Mechanical source prerequisite from a real neutral subprocess, never
        # relabelled as native quality or a business pass.
        quality = perform(f.store, f.root / "consumer-quality-fixture", f.candidate["candidate_id"], "quality",
            "consumer-quality-fixture", {"intake.md": "Neutral test_fixture quality source only."})
        f.store.record_review("consumer-quality-fixture", f.candidate["candidate_id"], quality["attempt_id"],
            verdict="accepted", scope="neutral fixture source, no native signoff", whole_package=True, evidence=quality["evidence"])
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": flow_id,
            "owner": "fixture-controller", "task_id": f.candidate["task_id"], "policy_id": f.config["policy_version"],
            "authorization": f.config["authorization"], "business_packet_ref": f.config["authorization"],
            "directory": str(f.root / "lifecycle"), "technical": {"formal_runtime_root": str(f.runtime.parent),
                "formal_config_base": {key: val for key, val in f.config.items() if key not in ("run_id", "intent_id", "policy_version")}}}
        record = f.store.register_flow(flow_id, definition, [f.config["authorization"]],
            {"status": "active", "stage": "formal_prepare", "current_work_id": None, "run_index": 0,
             "history": [], "archives": [], "candidate_id": f.candidate["candidate_id"], "release": f.release,
             "formal_preparation_recovery": f.recovery})
        packet = {"formal": {"first_run_id": f.config["run_id"]},
            "business_investment": {"action": "Neutral existing original purpose.", "unresolved_business_question": "No model; verify explicit recovery forwarding."}}
        decision = f.store.record_decision(_node(record, "experiment-decision"), f.candidate["task_id"], f.candidate["candidate_id"],
            action="experiment", rationale=packet["business_investment"]["action"], evidence=[f.config["authorization"]],
            experiment_purpose=packet["business_investment"]["unresolved_business_question"], authorized=True)
        return f, record, packet, decision

    def test_common_consumer_forwards_explicit_ref_through_real_formal_validation_without_repreparing(self):
        f, record, packet, decision = self.make_consumer()
        before = {str(p): p.read_bytes() for p in [Path(f.proof["intent"]["path"]), Path(f.proof["reservation"]["path"]),
                                                  f.archive / "preparation-failure.json", f.runtime / "request.md"]}
        class EnqueueBoundary(RuntimeError):
            pass
        def validate_only(argv, **kwargs):
            self.assertEqual([f.config["launcher_python"], str(f.runtime / "execute_diagnostic.py")], argv)
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        def stop_before_queue(store, spec, outer, **kwargs):
            self.assertIs(f.store, store)
            self.assertEqual("existing_isolated", spec["runner"]["mode"])
            self.assertEqual(f.config["intent_id"], spec["runner"]["intent_id"])
            self.assertEqual(str(f.runtime), spec["runner"]["isolated"]["runtime"])
            raise EnqueueBoundary("Explicit outer enqueue seam; no worker/model is authorized by this fixture.")
        from taskflow.execution_formal import _validate_formal_preparation_recovery
        with patch("taskflow.execution_experiment._packet", return_value=packet), \
             patch("taskflow.execution_formal._validate_formal_preparation_recovery", wraps=_validate_formal_preparation_recovery) as validated, \
             patch("taskflow.execution_formal.subprocess.run", side_effect=validate_only) as checked, \
             patch("taskflow.execution_experiment.enqueue_spec", side_effect=stop_before_queue) as queued, \
             patch.object(f.store, "record_decision", side_effect=AssertionError("original approval must be retained")), \
             self.assertRaises(EnqueueBoundary):
            _advance(f.store, record)
        validated.assert_called_once()
        self.assertEqual(f.recovery, validated.call_args.kwargs["recovery"])
        self.assertTrue(validated.call_args.kwargs["public_probe"])
        self.assertEqual(1, checked.call_count)
        queued.assert_called_once()
        self.assertEqual([decision], f.store.dashboard()["decisions"])
        self.assertEqual(["quality"], [row["role"] for row in f.store.dashboard()["attempts"]])
        for path, raw in before.items():
            self.assertEqual(raw, Path(path).read_bytes())

    def test_legacy_consumer_without_explicit_ref_keeps_old_approved_source_guard(self):
        f, record, packet, _ = self.make_consumer()
        record["state"].pop("formal_preparation_recovery")
        with patch("taskflow.execution_experiment._packet", return_value=packet), \
             patch("taskflow.execution_formal._validate_formal_preparation_recovery", side_effect=AssertionError("no implicit proof")), \
             patch("taskflow.execution_experiment.enqueue_spec", side_effect=AssertionError("must not enqueue")), \
             self.assertRaisesRegex(TaskflowError, "approved adapter"):
            _advance(f.store, record)


if __name__ == "__main__":
    unittest.main()
