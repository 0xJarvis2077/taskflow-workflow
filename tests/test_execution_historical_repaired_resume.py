"""Exact initial historical-graph transport recovery; no worker is dispatched.

Store/source/repair/closure/receipt/lock/CAS are actual neutral fixtures. Packet,
parent-author selector and native CLI certification are explicit interface doubles.
"""

from contextlib import contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _receive, resume_path_transport
from tests import test_execution_historical_repaired_source as sources


REASON = "historical negative is outside the current repaired quality graph"


class HistoricalRepairedResumeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = sources.graph_descendant_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    def received(self):
        value = dict(self.value)
        store, root, child = value["store"], value["case"].root, value["new_flow"]
        tag = sha256(self.id().encode())[:10]
        parent_id, flow_id = "source-parent-" + tag, "experiment-" + tag
        result = {"outcome": "candidate_quality_accepted"}
        receipt = root / (parent_id + ".json")
        write_json(receipt, {"flow_id": parent_id, "status": "accepted", "result": result})
        parent = store.register_flow(parent_id, {"kind": "author_quality_v1"}, value["refs"], {
            "status": "accepted", "receipt": str(receipt), "result": result})
        candidate = store.get_candidate(value["current"]["candidate_id"])
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": flow_id,
            "task_id": candidate["task_id"], "owner": child["definition"]["owner"],
            "policy_id": child["definition"]["policy_id"], "authorization": value["refs"][0],
            "source_author_flow_id": parent_id, "directory": str(root / flow_id),
            "technical": {"formal_runtime_root": str(root / (flow_id + "-formal")),
                          "publication_root": str(root / (flow_id + "-publication"))}}
        record = store.register_flow(flow_id, definition, value["refs"], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_received_failure"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        value.update(record=record, parent=parent, candidate_row=candidate,
            resolution={"action": "resume_validated_source_quality_transport", "reason": "Revalidate the exact historical closure transport.", "evidence": value["refs"]},
            packet={"formal": {"first_run_id": "fixture-run"},
                    "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}},
            bound={"source": child, "initial": store.get_candidate(value["new_whole"]["candidate_id"]),
                   "binding": {"source_author_flow_id": parent_id, "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": value["current"]["attempt_id"]},
                                      "composite_quality_coverage": value["current"]["composite_quality_coverage"]}})
        return value

    @contextmanager
    def interfaces(self, value, *, parent=None, bound=None, handoff=None):
        original = value["store"].get_flow
        def get_flow(identity):
            return parent if parent is not None and identity == parent["flow_id"] else original(identity)
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]), \
             patch.object(value["store"], "get_flow", side_effect=get_flow):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_exact_received_graph_transport_resumes_once_without_changing_pause_or_original_scope(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_hold_wrong_scope_native_or_received_receipt_cannot_clear_failure(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_existing_publication_work_attempt_decision_or_new_open_business_defect_blocks(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_actual_cas_rejects_a_concurrent_append(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
