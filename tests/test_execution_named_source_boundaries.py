"""First receipt checks run before publication; business holds stay unresolved.

These cases compose the original neutral subprocess fixture without inheriting
its tests. The source IO, source map validation, native evaluation import,
archive, intake and resume functions are not mocked. Existing fixture adapters
only replace external model roles with ordinary neutral subprocesses.
"""

import copy
from pathlib import Path
import unittest

from taskflow import execution_review
from taskflow.contracts import TaskflowError
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.records import verify_evaluation
from tests import test_execution_named_source_resume as fixtures


class NamedSourceBoundaryTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.NamedSourceIntakeResumeTests(
            "test_existing_map_intake_resumes_then_ordinary_stage_uses_same_evaluation")
        case.receive_in_setup = False
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        self.store = case.store
        self.native = Path(case.config["run_directory"]) / "evaluations" / case.config["evaluation_id"]
        self.assert_unreceived()

    def assert_unreceived(self):
        self.assertFalse(self.native.exists())
        self.assertFalse((self.case.destination / "receipt.json").exists())
        self.assertFalse((self.case.destination / "full-archive").exists())
        self.assertFalse((self.case.destination / "evaluation.original.json").exists())

    def originals(self):
        paths = [Path(self.case.record["state"]["receipt"])]
        for name in ("facts_attempt", "score_attempt"):
            root = Path(self.case.config[name])
            paths.extend(root / child for child in ("manifest.json", "manifest-lock.json", "collection.json",
                         "collection-lock.json", "request.md", "trace.jsonl", "termination.json"))
            paths.extend(path for path in (root / "facts-seal.json", root / "first-score-seal.json") if path.exists())
            paths.extend(path for path in (root / "artifacts").iterdir() if path.is_file())
        return {path: path.read_bytes() for path in paths}

    def test_first_same_byte_peer_and_unknown_map_reject_before_any_receipt_or_native_import(self):
        originals = self.originals()
        record = self.store.get_flow(self.case.record["flow_id"])
        board = self.store.dashboard()
        mappings = [
            {self.case.name: "capture/trees/work/same-bytes/peer.txt"},
            {**self.case.config["source_path_map"], "unknown-signed-source": "capture/trees/work/unknown.txt"},
        ]
        for mapping in mappings:
            self.assert_unreceived()
            config = {**self.case.config, "source_path_map": mapping}
            with self.subTest(mapping=mapping), self.assertRaises(TaskflowError):
                execution_review.ingest_signed_evaluation(config=config, destination=self.case.destination)
            self.assert_unreceived()
            self.assertEqual(record, self.store.get_flow(record["flow_id"]))
            self.assertEqual(board["attempts"], self.store.dashboard()["attempts"])
            self.assertEqual(board["decisions"], self.store.dashboard()["decisions"])
            self.assertEqual(originals, {path: path.read_bytes() for path in originals})

    def test_first_correct_map_is_received_once_with_originals_preserved(self):
        originals = self.originals()
        board = self.store.dashboard()
        before = self.store.get_flow(self.case.record["flow_id"])
        result = self.case.receive()
        self.assertTrue(result["accepted"])
        self.assertTrue(self.native.is_dir())
        self.assertTrue((self.case.destination / "receipt.json").is_file())
        self.assertTrue((self.case.destination / "full-archive/facts/archive-binding.json").is_file())
        self.assertTrue((self.case.destination / "full-archive/score/archive-binding.json").is_file())
        native = verify_evaluation(self.native)
        self.assertEqual("original", native["kind"])
        self.assertEqual([], native["validation_issues"])
        self.assertEqual(result, execution_review.ingest_signed_evaluation(
            config=self.case.config, destination=self.case.destination))
        self.assertEqual([self.native.name], sorted(path.name for path in self.native.parent.iterdir() if path.is_dir()))
        self.assertEqual(before, self.store.get_flow(before["flow_id"]))
        self.assertEqual(board["attempts"], self.store.dashboard()["attempts"])
        self.assertEqual(board["decisions"], self.store.dashboard()["decisions"])
        self.assertEqual(originals, {path: path.read_bytes() for path in originals})

    def test_source_map_resume_cannot_clear_pending_business_scope(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_managed_intake_preserves_its_existing_public_error_type(self):
        config = dict(self.case.config)
        del config["facts_attempt"]
        with self.assertRaises(TaskflowError):
            execution_review.ingest_signed_evaluation(config=config, destination=self.case.destination)
        self.assert_unreceived()


if __name__ == "__main__":
    unittest.main()
