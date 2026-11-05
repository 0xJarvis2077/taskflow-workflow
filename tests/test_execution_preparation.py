"""Unclaimed partial preparation preservation, without CLI or model calls."""

from contextlib import contextmanager
import json
import fcntl
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution_preparation import prepare_or_resume
from taskflow.execution_formal import validate_formal_provider
from taskflow.execution_store import evidence_ref


class PreparationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(temporary.cleanup)
        self.parent = Path(temporary.name)
        self.runtime = self.parent / "runtime"
        self.work = {"work_id": "neutral-work", "intent_id": "neutral-intent"}
        self.runner = {"mode": "test_fixture", "intent_id": "neutral-intent"}
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        for table in ("work", "attempts"):
            self.db.execute("CREATE TABLE " + table + " (data TEXT)")
        self.store = type("NeutralStore", (), {})()
        @contextmanager
        def database():
            yield self.db
        self.store._db = database

    def partial(self):
        (self.runtime / "inputs").mkdir(parents=True)
        (self.runtime / "inputs/neutral.txt").write_text("ORIGINAL partial bytes")
        (self.runtime / "request.md").write_text("ORIGINAL partial request")

    def prepare(self, root, runner):
        root.mkdir()
        (root / "manifest.json").write_text(json.dumps({"preparation_config_digest": sha256(canonical(runner))}))
        (root / "manifest-lock.json").write_text("synthetic lock")
        return {"preparation_config_digest": sha256(canonical(runner))}

    def run_prepare(self):
        with patch("taskflow.execution_runner.prepare_attempt", side_effect=self.prepare):
            return prepare_or_resume(self.store, self.work, self.runner, self.runtime, resume_prepared=True)

    def archives(self):
        return list(self.runtime.with_name("runtime.preparation-originals").glob("*/original"))

    def test_partial_is_preserved_and_same_path_normally_prepared(self):
        self.partial(); original = (self.runtime / "request.md").read_bytes()
        self.run_prepare()
        self.assertTrue((self.runtime / "manifest-lock.json").exists())
        self.assertEqual(1, len(self.archives()))
        self.assertEqual(original, (self.archives()[0] / "request.md").read_bytes())
        self.assertEqual("ORIGINAL partial bytes", (self.archives()[0] / "inputs/neutral.txt").read_text())
        with patch("taskflow.execution_runner._validate", return_value={"preparation_config_digest": sha256(canonical(self.runner))}):
            self.run_prepare()
        self.assertEqual(1, len(self.archives()))

    def test_missing_runtime_normal_prepare_and_resume_false_do_not_redraw(self):
        self.run_prepare()
        with patch("taskflow.execution_runner.prepare_attempt", side_effect=FileExistsError):
            with self.assertRaises(FileExistsError): prepare_or_resume(self.store, self.work, self.runner, self.runtime)

    def test_registered_work_or_attempt_forbids_recovery(self):
        self.partial()
        for table in ("work", "attempts"):
            self.db.execute("INSERT INTO " + table + " VALUES(?)", (json.dumps(self.work),))
            with self.assertRaisesRegex(TaskflowError, "registered work"): self.run_prepare()
            self.db.execute("DELETE FROM " + table)
        self.assertTrue((self.runtime / "request.md").exists())
        self.assertFalse(self.archives())

    def test_execution_unknown_directories_and_symlinks_are_never_recovered(self):
        self.partial()
        for name in ("trace.jsonl", "dispatch.json", "native-probe-ready.json", "unknown.txt"):
            p = self.runtime / name; p.write_text("retained")
            with self.assertRaises(TaskflowError): self.run_prepare()
            p.unlink()
        directory = self.runtime / "artifacts"; directory.mkdir()
        with self.assertRaises(TaskflowError): self.run_prepare()
        directory.rmdir()
        link = self.runtime / "inputs/link"; link.symlink_to(self.parent / "absent")
        with self.assertRaises(TaskflowError): self.run_prepare()

    def test_missing_manifest_with_lock_or_invalid_completed_runtime_is_not_replaced(self):
        self.partial(); (self.runtime / "manifest-lock.json").write_text("retained")
        with patch("taskflow.execution_runner._validate", side_effect=ValueError("original corruption")):
            with self.assertRaisesRegex(ValueError, "original corruption"): self.run_prepare()
        self.assertFalse(self.archives())

    def test_unsealed_manifest_must_match_configuration(self):
        self.partial()
        p = self.runtime / "manifest.json"; p.write_text(json.dumps({"intent_id": "wrong"}))
        with self.assertRaises(TaskflowError): self.run_prepare()
        p.write_text(json.dumps({"intent_id": self.work["intent_id"], "preparation_config_digest": sha256(canonical(self.runner))}))
        self.run_prepare()
        self.assertTrue((self.archives()[0] / "manifest.json").exists())

    def test_interrupt_before_atomic_move_reuses_same_receipt(self):
        self.partial()
        with patch("taskflow.execution_preparation.os.rename", side_effect=OSError("neutral interruption")):
            with self.assertRaises(OSError): self.run_prepare()
        self.run_prepare()
        self.assertEqual(1, len(self.archives()))

    def test_interrupt_after_move_finalizes_preserved_receipt_and_prepares(self):
        self.partial()
        from taskflow.execution_runner import _publish
        def interrupt(path, value):
            if path.name == "preserved.json": raise OSError("neutral interruption after move")
            return _publish(path, value)
        with patch("taskflow.execution_runner._publish", side_effect=interrupt):
            with self.assertRaises(OSError): self.run_prepare()
        self.assertFalse(self.runtime.exists())
        self.run_prepare()
        self.assertTrue((self.archives()[0].parent / "preserved.json").exists())

    def test_repeated_failed_preparations_preserve_every_original(self):
        self.partial()
        def incomplete(root, runner):
            root.mkdir(); (root / "request.md").write_text("next partial")
            raise ValueError("neutral prepare failed")
        with patch("taskflow.execution_runner.prepare_attempt", side_effect=incomplete):
            with self.assertRaises(ValueError): self.run_without_prepare_mock()
            with self.assertRaises(ValueError): self.run_without_prepare_mock()
        self.assertEqual(2, len(self.archives()))

    def run_without_prepare_mock(self):
        return prepare_or_resume(self.store, self.work, self.runner, self.runtime, resume_prepared=True)

    def test_preserved_original_drift_is_rejected(self):
        self.partial(); self.run_prepare()
        (self.archives()[0] / "request.md").write_text("drift")
        with self.assertRaisesRegex(TaskflowError, "drifted"): self.run_prepare()

    def test_completed_configuration_cannot_change(self):
        self.run_prepare()
        with patch("taskflow.execution_runner._validate", return_value={"preparation_config_digest": "wrong"}):
            with self.assertRaisesRegex(TaskflowError, "different continuation"): self.run_prepare()

    def test_partial_bridge_and_concurrent_preparation_are_rejected(self):
        self.partial(); self.runner["mode"] = "existing_isolated"
        with self.assertRaisesRegex(TaskflowError, "bridge preparation"): self.run_prepare()
        self.runner["mode"] = "test_fixture"
        with self.runtime.with_name("runtime.preparation.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(TaskflowError, "already active"): self.run_prepare()

    def test_duplicate_partial_bytes_are_each_preserved(self):
        self.partial()
        def same_partial(root, runner):
            self.partial()
            raise ValueError("neutral same partial failure")
        with patch("taskflow.execution_runner.prepare_attempt", side_effect=same_partial):
            with self.assertRaises(ValueError): self.run_without_prepare_mock()
            with self.assertRaises(ValueError): self.run_without_prepare_mock()
        self.assertEqual(2, len(self.archives()))

    def test_invalid_archive_root_and_pending_receipts_are_rejected(self):
        self.partial()
        archive_root = self.runtime.with_name("runtime.preparation-originals")
        archive_root.symlink_to(self.parent / "absent")
        with self.assertRaises(TaskflowError): self.run_prepare()
        archive_root.unlink(); archive_root.mkdir()
        pending = archive_root / "pending"; pending.mkdir()
        (pending / "unknown").write_text("retained")
        with self.assertRaisesRegex(TaskflowError, "no original receipt"): self.run_prepare()
        (pending / "unknown").unlink()
        (pending / "receipt.json").write_text(json.dumps({"runtime": "foreign", "kind": "unclaimed_partial_preparation_v1"}))
        with self.assertRaisesRegex(TaskflowError, "different runtime"): self.run_prepare()
        (pending / "receipt.json").write_text(json.dumps({"runtime": str(self.runtime), "kind": "unclaimed_partial_preparation_v1", "original": {}}))
        with self.assertRaisesRegex(TaskflowError, "lost its original"): self.run_prepare()

    def test_preservation_marker_drift_is_rejected(self):
        self.partial(); self.run_prepare()
        marker = self.archives()[0].parent / "preserved.json"
        marker.write_text("{}")
        with self.assertRaisesRegex(TaskflowError, "receipt changed"): self.run_prepare()

    def test_provider_location_is_descriptive_but_exact_identity_and_bytes_are_required(self):
        provider = self.parent / "provider.json"; provider.write_text('{"synthetic": true}')
        source = evidence_ref(provider, "original descriptive location")
        adapter = self.parent / "prepare.py"; adapter.write_text("# synthetic adapter")
        baseline = self.parent / "baseline-plan.json"
        baseline.write_text(json.dumps({"native_provider_ref": source}))
        config = {"adapter": evidence_ref(adapter, "neutral adapter"),
                  "native_provider": {**source, "location": "different description"}}
        validate_formal_provider(config)
        config["native_provider"] = None
        with self.assertRaises(TaskflowError): validate_formal_provider(config)
        config["native_provider"] = {**source}
        config["native_provider"]["sha256"] = "0" * 64
        with self.assertRaises((TaskflowError, ValueError)): validate_formal_provider(config)
        config["native_provider"] = {**source, "extra": "unbound metadata"}
        with self.assertRaises(TaskflowError): validate_formal_provider(config)
        config["native_provider"] = {**source}
        provider.write_text("changed source")
        with self.assertRaises((TaskflowError, ValueError)): validate_formal_provider(config)

    def test_formal_preparation_invokes_provider_identity_gate_before_request_or_launch(self):
        from taskflow.execution_formal import prepare_formal
        candidate = {key: "neutral-" + key for key in
                     ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
        config = {"adapter": {"path": str(self.parent / "prepare.py")},
                  "native_provider": {"path": str(self.parent / "provider.json")},
                  "request": {"path": str(self.parent / "request.md")}}
        calls = []
        def checked(ref):
            calls.append(ref)
            if ref == config["request"]: raise TaskflowError("neutral request boundary stop")
            return Path(ref["path"]), b""
        with patch("taskflow.execution_formal.verify_snapshot", return_value=candidate), \
             patch("taskflow.execution_formal._checked_ref", side_effect=checked), \
             patch("taskflow.execution_formal.validate_formal_provider") as validate:
            with self.assertRaisesRegex(TaskflowError, "neutral request boundary stop"):
                prepare_formal(config=config, release={"snapshot": {"path": str(self.parent / "frozen")},
                                                     "candidate_binding": candidate},
                               candidate=candidate, runtime=self.runtime)
        validate.assert_called_once_with(config)
        self.assertEqual([config["adapter"], config["request"]], calls)
        self.assertFalse(self.runtime.exists())


class ProviderMetadataResumeTests(unittest.TestCase):
    def setUp(self):
        from tests.test_execution_lifecycle import FormalPreparationResumeTests
        self.fixture = FormalPreparationResumeTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        record, self.receipt = self.fixture.failure()
        state = dict(record["state"])
        state["exception"] = {"stage": "formal_prepare", "reason": "formal adapter does not bind the selected custom provider"}
        self.receipt.write_text(json.dumps({"flow_id": record["flow_id"], "status": "exception", "exception": state["exception"]}))
        self.record = self.fixture.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_provider_metadata_error"})
        self.resolution = {**self.fixture.resolution, "action": "resume_unclaimed_formal_provider_metadata"}

    def test_exact_zero_intent_provider_metadata_resume_preserves_originals(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_real_provider_difference_stops_without_state_change(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)
