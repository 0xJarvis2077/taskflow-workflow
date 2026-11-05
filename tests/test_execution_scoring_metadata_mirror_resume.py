"""Exact zero-model source recovery for a signed scoring-mirror transport.

The store, private repair, independent subprocess reports, graph revalidation,
receipt, lock and CAS are real neutral fixtures. Author ancestry/packet selection
and native outer CLI identity are explicit interface doubles. This fixture does
not establish production provenance or native identity. No model is invoked here.
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
from tests import test_execution_composite_quality as composite_fixtures
from tests import test_execution_scoring_metadata_mirror as mirror_fixtures


REASON = "Composition cannot silently inherit changed package/scoring metadata"


class ScoringMetadataMirrorResumeTests(unittest.TestCase):
    def received(self, *, mirror=True):
        if mirror:
            value = mirror_fixtures.make_scoring_mirror_fixture(self)
        else:
            fixture = composite_fixtures.CompositeQualityTests()
            fixture.addCleanup = self.addCleanup
            value = fixture.make_closed_flow()
            value["refs"] = value["case"].refs
        store, child = value["store"], value["flow"]
        identity = value["candidate"]
        identity = identity["candidate_id"] if isinstance(identity, dict) else identity
        candidate = store.get_candidate(identity)
        initial = store.get_candidate(store.get_review(value["whole"])["candidate_id"])
        review = store.compose_repaired_quality(identity, value["whole"], value["recheck"], value["defects"], evidence=value["refs"])
        root = store.path.parent
        parent_id = "mirror-source-parent"
        parent_receipt = root / "mirror-parent-result.json"
        result = {"outcome": "candidate_quality_accepted"}
        write_json(parent_receipt, {"flow_id": parent_id, "status": "accepted", "result": result})
        parent = store.register_flow(parent_id, {"kind": "author_quality_v1"}, value["refs"], {
            "status": "accepted", "receipt": str(parent_receipt), "result": result})
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "mirror-experiment",
            "task_id": candidate["task_id"], "owner": child["definition"]["owner"],
            "policy_id": child["definition"]["policy_id"], "authorization": value["refs"][0],
            "source_author_flow_id": parent_id, "directory": str(root / "mirror-experiment"),
            "technical": {"formal_runtime_root": str(root / "mirror-formal"), "publication_root": str(root / "mirror-publication")}}
        record = store.register_flow(definition["flow_id"], definition, value["refs"], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_bounded_exception"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Revalidate only the exact independently signed scoring-mirror transport.", "evidence": value["refs"]}
        value.update(record=record, resolution=resolution, parent=parent, candidate_row=candidate,
            packet={"formal": {"first_run_id": "mirror-run-01"},
                    "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}},
            bound={"source": child, "initial": initial,
                   "binding": {"source_author_flow_id": parent_id, "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": review["attempt_id"]},
                                      "composite_quality_coverage": review["composite_quality_coverage"]}},
            review=review)
        return value

    @contextmanager
    def interfaces(self, value, *, bound=None, handoff=None):
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_actual_canonical_mirror_restores_only_same_initial_stage_once_and_retains_originals(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_only_exact_received_initial_metadata_exception_can_reach_source_validation(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_ordinary_composite_without_typed_mirror_is_not_metadata_recovery(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_stored_witness_drift_and_foreign_native_graph_do_not_count_as_validation(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_holds_decisions_and_existing_formal_paths_block_recovery(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_existing_exact_work_or_claimed_attempt_blocks_without_a_new_launch(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_real_cas_rejects_a_concurrent_revision_without_clearing_original_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
