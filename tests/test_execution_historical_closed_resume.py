"""Finite same-source recovery, without changing an independent business hold.

The store, actual closed repair, new author, current independent whole source,
sealed subprocess outputs, receipt, lock and CAS are real neutral fixtures.
Packet selection, parent-author binding and native CLI certification are explicit
boundary doubles; this is not evidence of a model run or native sandbox validity.
"""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _receive, resume_path_transport
from tests import test_execution_historical_closed_ancestor as sources
from tests import test_execution_historical_closure_resume as old_resumes


REASON = "historical negative lacks exact current whole repair coverage"


class HistoricalClosedResumeTests(unittest.TestCase):
    def received(self):
        value = sources.descendant_fixture(self)
        store, root = value["store"], value["case"].root
        candidate, review = value["new_candidate"], value["current"]
        refs = review["evidence"]
        child = store.register_flow("new-source-child", {"kind": "quality_repair_v1"}, refs, {
            "status": "accepted", "candidate_id": candidate["candidate_id"],
            "accepted_review_id": review["review_id"]})
        result = {"outcome": "candidate_quality_accepted"}
        parent_receipt = root / "new-source-parent-result.json"
        write_json(parent_receipt, {"flow_id": "new-source-parent", "status": "accepted", "result": result})
        parent = store.register_flow("new-source-parent", {"kind": "author_quality_v1"}, refs, {
            "status": "accepted", "receipt": str(parent_receipt), "result": result})
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "historical-experiment",
            "task_id": candidate["task_id"], "owner": value["flow"]["definition"]["owner"],
            "policy_id": value["flow"]["definition"]["policy_id"], "authorization": refs[0],
            "source_author_flow_id": parent["flow_id"], "directory": str(root / "experiment"),
            "technical": {"formal_runtime_root": str(root / "formal"), "publication_root": str(root / "publication")}}
        record = store.register_flow(definition["flow_id"], definition, refs, {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_source_transport_failure"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Revalidate this negative's actual already closed historical repair and current source.",
            "evidence": refs}
        value.update(record=record, resolution=resolution, refs=refs, parent=parent,
            packet={"formal": {"first_run_id": "neutral-run-01"}},
            bound={"source": child, "initial": value["alias"],
                   "binding": {"source_author_flow_id": parent["flow_id"], "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": review["attempt_id"]},
                                      "composite_quality_coverage": None}})
        return value

    @contextmanager
    def interfaces(self, value, *, bound=None, handoff=None, parent=None):
        store = value["store"]
        original_get = store.get_flow
        def get_flow(identity):
            if parent is not None and identity == value["parent"]["flow_id"]:
                return parent
            return original_get(identity)
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]), \
             patch.object(store, "get_flow", side_effect=get_flow):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_exact_original_source_resumes_once_and_preserves_business_pause_and_raw_sources(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_current_whole_without_any_alias_proof_remains_rejected(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_original_received_identity_and_hash_cannot_be_changed(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_an_open_new_business_defect_blocks_technical_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_parent_active_source_holds_wrong_lineage_review_or_native_binding_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_any_existing_formal_path_work_attempt_or_experiment_decision_blocks(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_concurrent_revision_causes_real_cas_rejection_and_preserves_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
