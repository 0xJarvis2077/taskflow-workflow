"""Same-packet factories retain prepared requests from an earlier prompt revision."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow import execution_author
from taskflow.execution_author import start_author_from_packet
from taskflow.execution_store import evidence_ref
from tests import test_execution_author_factory as fixture


OLD_REQUEST = ("Prior factory prompt: read protocol/current-author-packet.json and the staged actual originals. "
               "Return the five core outcome files and only an independently unsigned optional candidate.")


class AuthorFactoryReentryTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.AuthorFactoryTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.factory()

    def original_prompt(self, *, before_prepare=False, after_prepare=False):
        actual_publish = execution_author._publish_once

        def publish(path, value):
            if path == self.case.directory / "spec.json":
                # Model the prior factory renderer at its publication boundary;
                # the original spec, request, manifest and queued digest below
                # are prepared by the real existing implementation.
                value["runner"]["request_sections"] = [{"kind": "operation", "text": OLD_REQUEST}]
            return actual_publish(path, value)

        def enqueue(store, spec, runtime, **options):
            if before_prepare:
                raise TaskflowError("neutral stop before preparation")
            if after_prepare:
                with patch.object(store, "enqueue", side_effect=TaskflowError("neutral stop after preparation")):
                    return self.case.fixture_enqueue(store, spec, runtime, **options)
            return self.case.fixture_enqueue(store, spec, runtime, **options)

        with patch("taskflow.execution_author._publish_once", side_effect=publish), \
             patch("taskflow.execution_author.enqueue_spec", side_effect=enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("no models")):
            return start_author_from_packet(self.case.store, **self.case.kwargs())

    def reenter(self):
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.case.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("no models")):
            return start_author_from_packet(self.case.store, **self.case.kwargs())

    def locked_files(self):
        root = self.case.directory
        return {path: path.read_bytes() for path in (
            root / "factory-intent.json", root / "spec.json", root / "runtime/manifest.json",
            root / "runtime/manifest-lock.json", root / "runtime/request.md") if path.exists()}

    def test_same_packet_reentry_preserves_actual_old_prompt_and_ready_intent(self):
        first = self.original_prompt()
        before = self.locked_files()
        self.assertIn(OLD_REQUEST, (Path(first["runtime"]) / "request.md").read_text())
        self.assertNotIn("review_id=r.id", (Path(first["runtime"]) / "request.md").read_text())
        result = self.reenter()
        self.assertEqual(first, result)
        self.assertEqual(before, self.locked_files())
        self.assertEqual(1, len(self.case.native_specs))
        self.assertEqual(1, len(self.case.store.dashboard()["work"]))
        self.assertEqual([], self.case.store.dashboard()["attempts"])

    def test_same_packet_partial_preparation_keeps_old_spec_and_request(self):
        with self.assertRaisesRegex(TaskflowError, "after preparation"):
            self.original_prompt(after_prepare=True)
        before = self.locked_files()
        self.assertEqual([], self.case.store.dashboard()["work"])
        result = self.reenter()
        self.assertEqual("ready", result["state"])
        self.assertEqual(before, self.locked_files())
        self.assertEqual(1, len(self.case.store.dashboard()["work"]))
        self.assertEqual([], self.case.store.dashboard()["attempts"])

    def test_published_old_spec_without_runtime_uses_normal_prepare_once(self):
        with self.assertRaisesRegex(TaskflowError, "before preparation"):
            self.original_prompt(before_prepare=True)
        before = self.locked_files()
        self.assertFalse((self.case.directory / "runtime").exists())
        result = self.reenter()
        self.assertEqual("ready", result["state"])
        for path, raw in before.items():
            self.assertEqual(raw, path.read_bytes())
        self.assertIn(OLD_REQUEST, (Path(result["runtime"]) / "request.md").read_text())

    def test_non_request_spec_drift_is_rejected_without_replacing_the_runtime(self):
        self.original_prompt()
        path = self.case.directory / "spec.json"
        original_spec = read_json(path)
        locked = {key: value for key, value in self.locked_files().items() if key != path}
        for field in ("purpose", "timeout", "permissions", "outputs", "inputs"):
            value = copy.deepcopy(original_spec)
            if field == "purpose":
                value["work"]["purpose"] += " Changed intent"
            elif field == "timeout":
                value["runner"]["timeout_seconds"] += 1
            elif field == "permissions":
                value["runner"]["native_permissions"]["environment"]["EXTRA"] = "different"
            elif field == "outputs":
                value["runner"]["outputs"].append("other.md")
            else:
                value["runner"]["inputs"].append({"path": str(self.case.authority), "kind": "history", "target": "foreign.json"})
            write_json(path, value)
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.reenter()
            for name, raw in locked.items():
                self.assertEqual(raw, name.read_bytes())
        write_json(path, original_spec)

    def test_packet_or_baseline_change_cannot_reuse_an_old_request(self):
        self.original_prompt()
        before = self.locked_files()
        self.case.packet_data["author_work"] = "Different current business intent"
        write_json(self.case.packet, self.case.packet_data)
        self.case.packet_ref = evidence_ref(self.case.packet, "changed packet")
        with self.assertRaises(TaskflowError):
            self.reenter()
        self.assertEqual(before, self.locked_files())
        # Restore the exact authority and independently exercise source drift.
        self.case.packet_data["author_work"] = "CURRENT_NEW_RESEARCH"
        write_json(self.case.packet, self.case.packet_data)
        self.case.packet_ref = evidence_ref(self.case.packet, "new once-only research authority")
        baseline = self.case.task / "draft/evaluation/answer.md"
        baseline.write_bytes(baseline.read_bytes() + b"Current baseline drift")
        with self.assertRaises(TaskflowError):
            self.reenter()
        self.assertEqual(before, self.locked_files())

    def test_modified_published_sections_or_runtime_request_cannot_rebind_a_queued_intent(self):
        self.original_prompt()
        path = self.case.directory / "spec.json"
        spec = read_json(path)
        locked = self.locked_files()
        spec["runner"]["request_sections"] = [{"kind": "operation", "text": "Foreign replacement request"}]
        write_json(path, spec)
        with self.assertRaises(TaskflowError):
            self.reenter()
        path.write_bytes(locked[path])
        request = self.case.directory / "runtime/request.md"
        request.write_bytes(request.read_bytes() + b"Foreign unsealed bytes")
        with self.assertRaises(ValueError):
            self.reenter()
        self.assertEqual(1, len(self.case.store.dashboard()["work"]))
        self.assertEqual([], self.case.store.dashboard()["attempts"])

    def test_self_consistent_file_reseal_cannot_replace_the_queued_request_digest(self):
        first = self.original_prompt()
        runtime = Path(first["runtime"])
        path = self.case.directory / "spec.json"
        spec = read_json(path)
        replacement = "Foreign internally resealed request"
        spec["runner"]["request_sections"] = [{"kind": "operation", "text": replacement}]
        write_json(path, spec)
        request = runtime / "request.md"
        request.write_bytes(request.read_bytes().replace(OLD_REQUEST.encode(), replacement.encode()))
        manifest = read_json(runtime / "manifest.json")
        manifest["request"].update(sha256=sha256(request.read_bytes()), size=len(request.read_bytes()))
        write_json(runtime / "manifest.json", manifest)
        raw = (runtime / "manifest.json").read_bytes()
        write_json(runtime / "manifest-lock.json", {"sha256": sha256(raw), "size": len(raw)})
        with self.assertRaisesRegex(TaskflowError, "published original and locked digest"):
            self.reenter()
        self.assertEqual(1, len(self.case.store.dashboard()["work"]))
        self.assertEqual([], self.case.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
