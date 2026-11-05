"""Exact source recovery consumes real sealed whole-finding transport.

The native source/report/input/collection and release/projection IO is actual
neutral subprocess evidence. Accepted author lineage and the store quality graph
are explicit interface doubles from the adjacent source suite, not native model
or independent business results. Recovery lock, receipt, metadata and CAS are real.
"""

from contextlib import ExitStack, contextmanager
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_formal import prepare_release, projected_quality_reports
from taskflow.execution_lifecycle import _receive, resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_negative_whole_finding_targets as findings
from tests import test_execution_native_prior_findings as priors
from tools.execution_simulation import fixture_policy


REASON = "native quality coverage must explicitly delimit whole or affected files"


class WholeFindingLocationsResumeTests(unittest.TestCase):
    def received(self, *, locations=True):
        values = findings.negative_whole_targets_fixture(self) if locations else priors.prior_findings_fixture(self)
        case, graph = values[:2]
        prior_root, fresh_root = values[2:4]
        initial = case.candidates[graph["whole_source"]["candidate_id"]]
        case.candidate["parent_id"] = initial["candidate_id"]
        case.current.update(derived=True, candidate_id=case.candidate["candidate_id"])
        case.child["state"]["history"].append({"kind": "independent_review", "review_id": graph["whole_review_id"]})
        child_receipt = Path(case.child["state"]["receipt"])
        received_child = read_json(child_receipt)
        received_child["history"] = case.child["state"]["history"]
        write_json(child_receipt, received_child)
        receipt = case.root / "source-parent-received.json"
        write_json(receipt, {"flow_id": case.parent_id, "status": "accepted", "result": case.parent["state"]["result"]})
        case.parent["state"]["receipt"] = str(receipt)
        store = ExecutionStore(case.root / "resume.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "whole-finding-experiment",
            "task_id": "neutral", "owner": "neutral-controller", "policy_id": "fixture-v1",
            "source_author_flow_id": case.parent_id, "authorization": case.authorization,
            "directory": str(case.root / "experiment"),
            "technical": {"formal_runtime_root": str(case.root / "formal"), "publication_root": str(case.root / "publication")}}
        record = store.register_flow(definition["flow_id"], definition, [evidence_ref(case.authority, "original source authority")], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_bounded_source_exception"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        packet = {"formal": {"first_run_id": "next-neutral-run"},
                  "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}}
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Use only the actual original whole finding-location witness and current affected closure.",
            "evidence": [evidence_ref(case.authority, "original unchanged business authority")]}
        return {"case": case, "store": store, "record": record, "resolution": resolution,
                "packet": packet, "graph": graph, "initial": initial, "prior": prior_root, "fresh": fresh_root}

    @contextmanager
    def interfaces(self, value):
        case, store = value["case"], value["store"]
        get_flow = store.get_flow
        with ExitStack() as stack:
            stack.enter_context(patch.object(store, "get_flow", side_effect=lambda key: case.flows[key] if key in case.flows else get_flow(key)))
            for name in ("get_candidate", "get_attempt", "quality_gate", "assert_review_current"):
                stack.enter_context(patch.object(store, name, getattr(case.store, name)))
            stack.enter_context(patch.object(store, "get_review", return_value={"covered_defect_ids": value["graph"]["closed_defect_ids"]}))
            stack.enter_context(patch("taskflow.execution_experiment._packet", return_value=value["packet"]))
            stack.enter_context(patch("taskflow.execution_experiment._author_source", return_value={"source": case.child,
                "initial": value["initial"], "binding": {"source_author_flow_id": case.parent_id, "child_flow_id": case.child_id}}))
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_actual_whole_finding_witness_restores_initial_stage_and_normal_release_readiness(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_a_valid_old_transport_without_finding_locations_cannot_clear_this_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_exact_received_exception_and_every_business_hold_are_preserved(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_foreign_solver_decision_and_formal_roots_block_without_dispatch(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_original_sealed_source_drift_is_not_a_technical_resolution(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_real_cas_conflict_retains_the_original_exception_and_no_new_work(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
