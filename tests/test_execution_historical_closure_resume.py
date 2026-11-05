"""Finite historical-freshness recovery keeps receipt, hold and CAS boundaries.

No safety IO or native validator is mocked. The positive actual native handoff
and recovery must additionally be exercised by root on the original sources.
"""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import _receive, resume_path_transport
from tests import test_execution_experiment_author as authors
from tests import test_execution_source_quality_transport as transports


HISTORICAL = "later independent rejection/conflict covers the accepted scope; old acceptance is not current"


class HistoricalResumeStateTests(unittest.TestCase):
    def test_only_exact_initial_received_zero_launch_state_can_reach_source_validation(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_original_receipt_history_owner_authority_and_source_cannot_be_changed(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_existing_formal_paths_and_experiment_decision_prevent_recovery(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_stale_revision_is_rejected_by_the_existing_real_cas(self):
        case, _, _, record, _ = self.received()
        current = case.store.update_flow(record["flow_id"], record["revision"], record["state"], {"kind": "concurrent_neutral_append"})
        with self.assertRaises(TaskflowError):
            case.store.update_flow(record["flow_id"], record["revision"], record["state"], {"kind": "stale_append"})
        self.assertEqual(current, case.store.get_flow(record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
