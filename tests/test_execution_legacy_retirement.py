"""Removed incident recovery and old real dispatch never mutate the ledger."""

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError
from taskflow.execution_lifecycle import assert_legacy_dispatch_allowed, run_lifecycle
from taskflow.execution_store import evidence_ref
from tests.helpers import assert_legacy_recovery_retired
from tests import test_execution as fixtures


OLD_ACTIONS = (
    "resume_validated_prepared_formal_cli", "resume_validated_engineering_public_archive",
    "resume_observed_author_completion", "resume_failed_public_capacity_once",
    "resume_resolved_public_capacity_dependency", "resume_author_packet_source_metadata_transport",
    "accept_path_only_candidate_transport", "accept_unsigned_author_placeholders",
    "resume_unclaimed_public_preparation", "resume_unclaimed_formal_preparation",
    "resume_unclaimed_formal_provider_metadata", "resume_prepared_formal_provider_metadata",
    "resume_accepted_formal_provider_metadata", "resume_resolved_quality_dependency",
    "resume_validated_source_quality_transport", "resume_unpublished_release_metadata",
    "resume_validated_evaluation_intake", "resume_unpublished_release_report_binding",
    "resume_validated_host_formal_preparation", "resume_validated_public_probe_preparation",
    "resume_validated_quality_composition", "resume_validated_score_preparation",
    "accept_declaration_only_candidate_transport", "resume_resolved_declaration_dependency",
    "resume_resolved_author_preparation_dependency",
)


class LegacyRetirementTests(unittest.TestCase):
    def test_every_old_resolution_refuses_without_changing_originals(self):
        for action in (*OLD_ACTIONS, "unknown-incident-action"):
            with self.subTest(action=action):
                assert_legacy_recovery_retired(self, action=action)

    def case(self):
        case = fixtures.ExecutionCLITests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def work(self, role="quality"):
        case = self.case()
        runtime, queued = case.enqueue("old-work", role)
        work = queued["work"]
        declaration = runtime / "manifest.json"
        manifest = json.loads(declaration.read_text(encoding="utf-8"))
        manifest["mode"] = "native_role"
        declaration.write_text(json.dumps(manifest), encoding="utf-8")
        return case, runtime, work

    def bind_old(self, case, work, kind="author_quality_v1"):
        return case.store.register_flow("old-source", {"kind": kind}, work["evidence"] or [
            evidence_ref(
                Path(work["runtime_path"]) / "manifest.json", "neutral non-fixture dispatch guard source")],
            {"status": "active", "current_work_id": work["work_id"]})

    def test_old_real_registered_work_is_refused_before_claim(self):
        case, runtime, work = self.work()
        self.bind_old(case, work)
        before = (case.store.events(), case.store.get_work(work["work_id"]), (runtime / "manifest.json").read_bytes())
        with patch("taskflow.execution_runner.run_attempt", side_effect=AssertionError("no old model")):
            with self.assertRaisesRegex(TaskflowError, "dispatch is retired"):
                execution.run_worker(case.store, "forbidden-old-worker", True, [work["work_id"]])
        self.assertEqual(before, (case.store.events(), case.store.get_work(work["work_id"]), (runtime / "manifest.json").read_bytes()))
        self.assertEqual([], case.store.list_attempts(work["work_id"]))

    def test_old_real_author_without_lifecycle_is_refused_before_claim(self):
        case, runtime, work = self.work("author")
        before = case.store.events()
        with self.assertRaisesRegex(TaskflowError, "dispatch is retired"):
            execution.run_worker(case.store, "forbidden-old-author", True, [work["work_id"]])
        self.assertEqual(before, case.store.events())
        self.assertEqual([], case.store.list_attempts(work["work_id"]))

    def test_legacy_run_refuses_nonfixture_without_consumer_receipt_or_state_change(self):
        case, _, work = self.work()
        record = self.bind_old(case, work)
        before = case.store.events()
        with self.assertRaisesRegex(TaskflowError, "execution is retired"):
            run_lifecycle(case.store, record["flow_id"], allow_model=True)
        self.assertEqual(before, case.store.events())
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))

    def test_explicit_public_work_remains_allowed_without_legacy_owner(self):
        case, _, work = self.work()
        with case.store._db() as database:
            assert_legacy_dispatch_allowed(database, work)

    def test_neutral_historical_simulation_can_still_dispatch(self):
        case = self.case()
        _, queued = case.enqueue("simulated-legacy", "quality")
        work = queued["work"]
        self.bind_old(case, work)
        result = execution.run_worker(case.store, "neutral-simulation", False, [work["work_id"]])
        self.assertEqual("completed", result["state"])

    def test_malformed_unrelated_flow_does_not_block_explicit_ready_work(self):
        case = self.case()
        first_runtime, first = case.enqueue("explicit-first", "quality")
        case.store.register_flow("unrelated-damaged", {"kind": "author_quality_v1"},
                                 [evidence_ref(first_runtime / "manifest.json", "neutral unrelated flow")], {"status": "ready"})
        variants = (("{", "{}"), ("[]", "{}"),
                    ('{"kind":"author_quality_v1"}', "{"),
                    ('{"kind":"author_quality_v1"}', "[]"),
                    ('{"kind":["opaque"]}', "{}"))
        for index, (definition, state) in enumerate(variants):
            with self.subTest(index=index):
                with case.store._db(True) as database:
                    database.execute("UPDATE execution_flows SET definition=?,state=? WHERE flow_id=?",
                                     (definition, state, "unrelated-damaged"))
                identity = "explicit-independent-" + str(index)
                _, queued = case.enqueue(identity, "quality")
                result = execution.run_worker(case.store, "independent-fixture", False, [identity])
                self.assertEqual("completed", result["state"])
                self.assertEqual(1, len(case.store.list_attempts(queued["work"]["work_id"])))


if __name__ == "__main__":
    unittest.main()
