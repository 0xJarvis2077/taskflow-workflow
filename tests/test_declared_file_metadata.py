"""Actual computed snapshot metadata overrides optional author assertions."""

import copy
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.packages import export_snapshot, freeze_task, verify_export, verify_snapshot
from tests.helpers import add_reviews, make_task


class DeclaredFileMetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.task = make_task(self.root)

    def freeze(self, *, declared=True):
        declaration = read_json(self.task / "task.json")
        if declared:
            for index, entry in enumerate(declaration["files"]):
                raw = (self.task / entry["path"]).read_bytes()
                entry.update(sha256=sha256(raw), bytes=len(raw), author_metadata={"source": "neutral original assertion"})
                if index == 1:
                    # Existing capture treats these optional fields as original
                    # declaration metadata, never as the measured payload hash.
                    entry.update(sha256="0" * 64, size=-1, bytes=999)
            write_json(self.task / "task.json", declaration)
        raw = (self.task / "task.json").read_bytes()
        add_reviews(self.task)
        snapshot = Path(freeze_task(self.task, "v1")["path"])
        return snapshot, declaration, raw

    def rewrite_manifest(self, snapshot, manifest):
        manifest["snapshot_digest"] = sha256(canonical({key: value for key, value in manifest.items() if key != "snapshot_digest"}))
        write_json(snapshot / "manifest.json", manifest)

    def test_optional_author_sha_bytes_and_metadata_survive_exact_round_trip(self):
        snapshot, declaration, raw = self.freeze()
        before = {path: path.read_bytes() for path in snapshot.rglob("*") if path.is_file()}
        manifest = verify_snapshot(snapshot)
        self.assertEqual(raw, (snapshot / "task.json").read_bytes())
        for source, captured in zip(declaration["files"], manifest["files"]):
            payload = (snapshot / source["path"]).read_bytes()
            self.assertEqual({**source, "size": len(payload), "sha256": sha256(payload)}, captured)
            self.assertEqual(source["bytes"], captured["bytes"])
            self.assertEqual(source["author_metadata"], captured["author_metadata"])
        self.assertNotEqual(declaration["files"][1]["sha256"], manifest["files"][1]["sha256"])
        for audience in ("solver", "evaluator"):
            exported = export_snapshot(snapshot, audience)
            self.assertEqual(exported["files"], verify_export(Path(exported["path"]))["files"])
            for entry in exported["files"]:
                payload = (Path(exported["path"]) / entry["path"]).read_bytes()
                self.assertEqual(sha256(payload), entry["sha256"])
                self.assertEqual(len(payload), entry["size"])
        self.assertEqual(before, {path: path.read_bytes() for path in snapshot.rglob("*") if path.is_file()})

    def test_wrong_payload_computed_hash_metadata_role_and_file_set_remain_rejected(self):
        snapshot, _, _ = self.freeze()
        original = read_json(snapshot / "manifest.json")
        payload = snapshot / original["files"][0]["path"]
        raw = payload.read_bytes()
        payload.write_bytes(b"!" + raw[1:])
        with self.assertRaisesRegex(TaskflowError, "integrity mismatch"):
            verify_snapshot(snapshot)
        payload.write_bytes(raw)
        for label in ("computed hash", "unknown metadata", "author bytes", "role", "missing file", "extra file", "order"):
            manifest = copy.deepcopy(original)
            if label == "computed hash":
                manifest["files"][0]["sha256"] = "0" * 64
            elif label == "unknown metadata":
                manifest["files"][0]["unrecorded"] = True
            elif label == "author bytes":
                manifest["files"][0]["bytes"] += 1
            elif label == "role":
                manifest["files"][0]["role"] = "verification"
            elif label == "missing file":
                manifest["files"].pop()
            elif label == "extra file":
                manifest["files"].append({**manifest["files"][0], "id": "foreign"})
            else:
                manifest["files"].reverse()
            self.rewrite_manifest(snapshot, manifest)
            with self.subTest(label=label), self.assertRaises(TaskflowError):
                verify_snapshot(snapshot)
        self.rewrite_manifest(snapshot, original)
        self.assertEqual(original, verify_snapshot(snapshot))

    def test_legacy_without_self_metadata_has_the_same_manifest_and_readback_bytes(self):
        snapshot, declaration, raw = self.freeze(declared=False)
        manifest = read_json(snapshot / "manifest.json")
        expected = [{**entry, "sha256": sha256((snapshot / entry["path"]).read_bytes()),
                     "size": len((snapshot / entry["path"]).read_bytes())} for entry in declaration["files"]]
        self.assertEqual(expected, manifest["files"])
        before = {path: path.read_bytes() for path in snapshot.rglob("*") if path.is_file()}
        self.assertEqual(manifest, verify_snapshot(snapshot))
        self.assertEqual(raw, (snapshot / "task.json").read_bytes())
        self.assertEqual(before, {path: path.read_bytes() for path in snapshot.rglob("*") if path.is_file()})

    def test_zero_size_and_original_numeric_metadata_cannot_be_replaced_by_boolean_false(self):
        declaration = read_json(self.task / "task.json")
        path = "draft/evaluation/zero.jsonl"
        (self.task / path).write_bytes(b"")
        declaration["files"].append({"id": "zero", "path": path, "role": "teacher", "bytes": 0,
            "sha256": sha256(b""), "numeric_metadata": 0,
            "empty_content": {"kind": "zero_record_jsonl", "reason": "Neutral zero-record control.", "evidence": ["S1"]}})
        write_json(self.task / "task.json", declaration)
        add_reviews(self.task)
        snapshot = Path(freeze_task(self.task, "v1")["path"])
        original = read_json(snapshot / "manifest.json")
        for key in ("size", "bytes", "numeric_metadata"):
            manifest = copy.deepcopy(original)
            manifest["files"][-1][key] = False
            self.rewrite_manifest(snapshot, manifest)
            with self.subTest(key=key), self.assertRaisesRegex(TaskflowError, "file manifest mismatch"):
                verify_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
