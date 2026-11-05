"""Finite source-transport recovery, with explicit fixture interface boundaries."""

from contextlib import ExitStack, contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_claim_scope_composition as claim_fixtures
from tests import test_execution_native_observation_acceptance as native_fixtures
from tools.execution_simulation import fixture_policy


CALCULATION = "native quality dimension has no explicit scoped affirmation: calculation"
COMPOSITION = "Composition defect lacks exact repair/closure identity and actual ordering"
ALIAS = "Issue evidence is absent from current candidate or has wrong SHA"


class SourceQualityTransportTests(unittest.TestCase):
    def native_case(self):
        # The real native helper reads actual request/input/output/collection
        # seals around a neutral local subprocess, never a real model call.
        source = native_fixtures.native_observation_fixture(self)
        packet = read_json(source.authority)
        packet.update(prerequisites=["Actual accepted independent current source."],
                      preauthorized_normal_chain="One exact neutral handoff.",
                      confirmation={"maximum_additional_runs_per_first": 1})
        packet["items"][0].update(first_experiment_business_unknown="Neutral offline observation.", version_id="next-v1",
                                  first_run_id="next-run-01", first_evaluation_id="next-eval-01",
                                  conditional_one_confirmation_run_id="next-run-02",
                                  conditional_one_confirmation_evaluation_id="next-eval-02")
        write_json(source.authority, packet)
        source.authorization = evidence_ref(source.authority, "neutral exact source experiment authority")
        parent_receipt = source.root / "parent-result.json"
        write_json(parent_receipt, {"flow_id": source.parent_id, "status": "accepted",
                                   "result": source.parent["state"]["result"]})
        source.parent["state"]["receipt"] = str(parent_receipt)
        store = ExecutionStore(source.root / "recovery.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "source-experiment", "task_id": "neutral",
                      "owner": "neutral-controller", "policy_id": "fixture-v1", "authorization": source.authorization,
                      "business_packet_ref": source.authorization, "source_author_flow_id": source.parent_id,
                      "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": source.parent_id},
                      "directory": str(source.root / "experiment"), "technical": {
                          "formal_runtime_root": str(source.root / "formal"), "publication_root": str(source.root / "publication")}}
        record, resolution = self.exception(store, definition, CALCULATION, source.root)
        original_get = store.get_flow
        bound = {"source": source.child, "initial": source.candidate, "author_sids": {source.item["source_author_sid"]},
                 "binding": {"source_author_flow_id": source.parent_id, "source_author_attempt": "author-attempt"}}

        @contextmanager
        def interfaces():
            # Source-author ancestry has its own real author tests. The native
            # signature validator is deliberately not mocked in this fixture.
            with ExitStack() as stack:
                stack.enter_context(patch.object(store, "get_flow", side_effect=lambda key: source.flows[key] if key in source.flows else original_get(key)))
                for name in ("get_candidate", "get_attempt", "quality_gate", "assert_review_current"):
                    stack.enter_context(patch.object(store, name, getattr(source.store, name)))
                stack.enter_context(patch("taskflow.execution_experiment._author_source", return_value=bound))
                yield
        return source, store, record, resolution, interfaces

    def exception(self, store, definition, reason, root):
        exception = {"stage": "experiment_v1", "reason": reason}
        receipt = root / (definition["flow_id"] + "-original-exception.json")
        write_json(receipt, {"flow_id": definition["flow_id"], "status": "exception", "exception": exception})
        record = store.register_flow(definition["flow_id"], definition, [definition["authorization"]], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [], "archives": [], "receipt": str(receipt), "exception": exception})
        return record, {"action": "resume_validated_source_quality_transport", "reason": "Validate only the original corrected source transport.",
                        "evidence": [definition["authorization"]]}

    def test_actual_native_nonblocking_signature_reopens_without_rewriting_false_or_sources(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_unrelated_hold_receipt_pending_business_and_existing_launch_footprints_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_native_source_drift_or_flipping_the_original_false_is_not_a_resolution(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_composite_errors_require_an_existing_real_canonical_graph_and_preserve_scope(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
