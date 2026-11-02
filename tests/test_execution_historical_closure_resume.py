"""Finite historical-freshness recovery keeps receipt, hold and CAS boundaries.

No safety IO or native validator is mocked. The positive actual native handoff
and recovery must additionally be exercised by root on the original sources.
"""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import _receive, _resume_source_quality_transport, resume_path_transport
from tests import test_execution_experiment_author as authors
from tests import test_execution_source_quality_transport as transports


HISTORICAL = "later independent rejection/conflict covers the accepted scope; old acceptance is not current"


class HistoricalResumeStateTests(unittest.TestCase):
    def test_only_exact_initial_received_zero_launch_state_can_reach_source_validation(self):
        record = {"definition": {"kind": "experiment_v1", "source_author_flow_id": "source", "owner": "owner"},
                  "state": {"status": "exception", "stage": "experiment_v1", "current_work_id": None,
                            "receipt": "original", "exception": {"stage": "experiment_v1", "reason": HISTORICAL}}}
        changes = [{"status": "active"}, {"status": "accepted", "stage": "complete"}, {"stage": "release"},
                   {"current_work_id": "running"}, {"receipt": None}, {"candidate_id": "selected"},
                   {"accepted_review_id": "selected"}, {"runs": ["run"]}, {"release": {"path": "publication"}},
                   {"run_index": 1}, {"pending_business_scope": {"reason": "Specific original business hold"}},
                   {"exception": {"stage": "experiment_v1", "reason": "A separate substantive conflict"}}]
        for change in changes:
            wrong = copy.deepcopy(record); wrong["state"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                # These invalid states must fail before any store or IO call.
                _resume_source_quality_transport(None, wrong, {}, [])


class HistoricalResumeReceiptTests(unittest.TestCase):
    def received(self):
        helper = authors.AuthorExperimentTests()
        case, source, initial = helper.candidate_source()
        self.addCleanup(helper.doCleanups)
        helper.root = case.root
        definition = helper.definition(case, source)
        definition["technical"].update(formal_runtime_root=str(case.root / "formal"),
                                        publication_root=str(case.root / "publication"))
        record, resolution = transports.SourceQualityTransportTests().exception(case.store, definition, HISTORICAL, case.root)
        state = dict(record["state"]); state.pop("receipt")
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_original_failure"})
        record = _receive(case.store, record)
        return case, source, initial, record, resolution

    def test_current_whole_without_exact_alias_and_graph_is_not_a_recovery(self):
        case, _, _, record, resolution = self.received()
        before = case.store.dashboard()
        with self.assertRaisesRegex(TaskflowError, "no closed repair through"):
            resume_path_transport(case.store, record["flow_id"], resolution)
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))
        self.assertEqual(before, case.store.dashboard())

    def test_original_receipt_history_owner_authority_and_source_cannot_be_changed(self):
        case, source, _, record, resolution = self.received()
        receipt = Path(record["state"]["receipt"])
        raw = receipt.read_bytes()
        for change in ({"controller": "foreign"}, {"history": [{"kind": "replacement"}]},
                       {"flow_id": "foreign"}, {"exception": {"stage": "experiment_v1", "reason": "different"}}):
            changed = read_json(receipt); changed.update(change); write_json(receipt, changed)
            try:
                with self.subTest(change=change), self.assertRaises(TaskflowError):
                    resume_path_transport(case.store, record["flow_id"], resolution)
            finally:
                receipt.write_bytes(raw)
        authority = Path(record["definition"]["authorization"]["path"])
        original = authority.read_bytes(); authority.write_bytes(original + b"drift")
        try:
            with self.assertRaises(TaskflowError):
                resume_path_transport(case.store, record["flow_id"], resolution)
        finally:
            authority.write_bytes(original)
        child = Path(source["state"]["result"]["child_receipt"])
        original = child.read_bytes(); changed = read_json(child); changed["candidate_id"] = "foreign"; write_json(child, changed)
        try:
            with self.assertRaises(TaskflowError):
                resume_path_transport(case.store, record["flow_id"], resolution)
        finally:
            child.write_bytes(original)
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))

    def test_existing_formal_paths_and_experiment_decision_prevent_recovery(self):
        case, _, initial, record, resolution = self.received()
        for name in ("formal_runtime_root", "publication_root"):
            path = Path(record["definition"]["technical"][name]); path.mkdir()
            try:
                with self.subTest(name=name), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    resume_path_transport(case.store, record["flow_id"], resolution)
            finally:
                path.rmdir()
        case.store.record_decision("preexisting", "neutral", initial["candidate_id"], action="experiment",
                                   rationale="A separate already authorized decision.", experiment_purpose="Other purpose.",
                                   authorized=True, evidence=case.refs)
        with self.assertRaisesRegex(TaskflowError, "existing formal"):
            resume_path_transport(case.store, record["flow_id"], resolution)
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))

    def test_stale_revision_is_rejected_by_the_existing_real_cas(self):
        case, _, _, record, _ = self.received()
        current = case.store.update_flow(record["flow_id"], record["revision"], record["state"], {"kind": "concurrent_neutral_append"})
        with self.assertRaises(TaskflowError):
            case.store.update_flow(record["flow_id"], record["revision"], record["state"], {"kind": "stale_append"})
        self.assertEqual(current, case.store.get_flow(record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
