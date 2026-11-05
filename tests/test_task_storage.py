"""Single-report cold storage preserves originals on unsafe or failed operations."""

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools import task_storage as storage


class TaskStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.package = Path(self.temporary.name).resolve() / "package"
        self.package.mkdir()
        self.task = self.package / "task.json"
        self.task.write_text('{"files":[],"reviews":[]}', encoding="utf-8")
        self.relative = "reports/workbook/generated.json"
        self.original = self.package / self.relative
        self.original.parent.mkdir(parents=True)
        self.content = (b'{"cell":"A1","formula":"SUM(B1:B9)"}\n' * 3000)
        self.original.write_bytes(self.content)
        self.original.chmod(0o640)
        self.mtime = 1700000000123456789
        os.utime(self.original, ns=(self.mtime, self.mtime))
        self.compressed = self.package / ("cold-storage/" + self.relative + ".gz")
        self.metadata = self.compressed.with_name(self.compressed.name + ".json")

    def pack(self, **kwargs):
        return storage.pack_file(self.package, self.relative, **kwargs)

    def restore(self, **kwargs):
        return storage.restore_file(self.package, self.relative, **kwargs)

    def snapshot(self):
        return {str(p.relative_to(self.package)): (p.read_bytes(), p.stat().st_mtime_ns)
                for p in self.package.rglob("*") if p.is_file()}

    def test_roundtrip_metadata_audit_retention_and_idempotence(self):
        task_bytes = self.task.read_bytes()
        result = self.pack(apply=True)
        self.assertEqual("packed", result["status"])
        self.assertFalse(self.original.exists())
        self.assertLess(result["compressed_size"], result["original_size"])
        manifest = json.loads(self.metadata.read_bytes())
        self.assertEqual(hashlib.sha256(self.content).hexdigest(),
                         manifest["original"]["sha256"])
        self.assertEqual(self.relative, manifest["original"]["path"])
        self.assertIn("--apply", manifest["restore_command"])
        archived = self.snapshot()
        self.assertEqual("already_packed", self.pack(apply=True)["status"])
        self.assertEqual(archived, self.snapshot())

        self.assertEqual("restored", self.restore(apply=True)["status"])
        self.assertEqual(self.content, self.original.read_bytes())
        self.assertEqual(0o640, self.original.stat().st_mode & 0o7777)
        self.assertEqual(self.mtime, self.original.stat().st_mtime_ns)
        self.assertTrue(self.compressed.is_file())
        self.assertTrue(self.metadata.is_file())
        restored = self.snapshot()
        self.assertEqual("already_restored", self.restore(apply=True)["status"])
        self.assertEqual(restored, self.snapshot())
        self.assertEqual("packed", self.pack(apply=True)["status"])
        self.assertFalse(self.original.exists())
        self.assertEqual(task_bytes, self.task.read_bytes())

    def test_dryrun_and_cli_defaults_do_not_write(self):
        before = self.snapshot()
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(0, storage.main([
                "pack", "--package", str(self.package), "--path", self.relative]))
        self.assertFalse(json.loads(output.getvalue())["apply"])
        self.assertEqual("would_pack", self.pack()["status"])
        self.assertEqual(before, self.snapshot())
        self.assertFalse((self.package / "cold-storage").exists())
        self.pack(apply=True)
        before = self.snapshot()
        self.assertEqual("would_restore", self.restore()["status"])
        self.assertEqual("already_packed", self.pack()["status"])
        self.assertEqual(before, self.snapshot())
        self.assertFalse(self.original.exists())

    def test_incompressible_and_empty_files_keep_original(self):
        for content in (os.urandom(4096), b""):
            with self.subTest(size=len(content)):
                self.original.write_bytes(content)
                self.assertEqual("no_savings", self.pack(apply=True)["status"])
                self.assertEqual(content, self.original.read_bytes())
                self.assertFalse(self.compressed.exists())
                self.assertFalse(self.metadata.exists())
                self.assertFalse(list(self.compressed.parent.glob(".cold-storage-*")))

    def test_gzip_or_manifest_tampering_never_restores_or_deletes_source(self):
        self.pack(apply=True)
        original_gzip = self.compressed.read_bytes()
        original_manifest = self.metadata.read_bytes()
        for changed in ("gzip", "manifest", "duplicate_key", "boolean_schema"):
            with self.subTest(changed=changed):
                self.compressed.write_bytes(original_gzip)
                self.metadata.write_bytes(original_manifest)
                if changed == "gzip":
                    self.compressed.write_bytes(original_gzip[:-4])
                elif changed == "manifest":
                    value = json.loads(original_manifest)
                    value["original"]["path"] = "reports/elsewhere.json"
                    self.metadata.write_text(json.dumps(value), encoding="utf-8")
                elif changed == "duplicate_key":
                    self.metadata.write_bytes(b'{"kind":"one","kind":"two"}')
                else:
                    value = json.loads(original_manifest)
                    value["schema_version"] = True
                    self.metadata.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises((storage.StorageError, ValueError)):
                    self.restore(apply=True)
                self.assertFalse(self.original.exists())
                with self.assertRaises((storage.StorageError, ValueError)):
                    self.pack(apply=True)

    def test_false_size_or_hash_even_with_resealed_manifest_fails(self):
        self.pack(apply=True)
        value = json.loads(self.metadata.read_bytes())
        for size, sha in ((1, value["original"]["sha256"]),
                          (len(self.content), "0" * 64)):
            with self.subTest(size=size):
                original = {key: value["original"][key]
                            for key in ("sha256", "size", "mode", "mtime_ns")}
                original.update(size=size, sha256=sha)
                compressed = {key: value["compressed"][key] for key in ("sha256", "size")}
                forged = storage._manifest(self.package, self.relative, original, compressed)
                self.metadata.write_bytes(storage._canonical(forged))
                with self.assertRaisesRegex(storage.StorageError, "original size|SHA256/size"):
                    self.restore(apply=True)
                self.assertFalse(self.original.exists())

    def test_restore_refuses_existing_bytes_and_racing_overwrite(self):
        self.pack(apply=True)
        self.original.write_bytes(b"keep existing")
        with self.assertRaises(storage.StorageError):
            self.restore(apply=True)
        self.assertEqual(b"keep existing", self.original.read_bytes())
        self.original.unlink()
        publish = storage._publish

        def race(parent, temporary, name):
            self.original.write_bytes(b"racing writer")
            publish(parent, temporary, name)

        with patch.object(storage, "_publish", side_effect=race):
            with self.assertRaises(FileExistsError):
                self.restore(apply=True)
        self.assertEqual(b"racing writer", self.original.read_bytes())

    def test_unsafe_paths_roots_directories_and_links_are_rejected(self):
        for relative in ("../reports/x", "/reports/x", "reports//x", "reports/./x",
                         "reports/../task.json", "reports\\x", "draft/input/x",
                         "reports/runs/x", "reports/copy/versions/x", "reports/work/x"):
            with self.subTest(relative=relative), self.assertRaises(storage.StorageError):
                storage.pack_file(self.package, relative, apply=True)
        outside = self.package.parent / "outside"
        outside.write_bytes(b"external")
        self.original.unlink()
        self.original.symlink_to(outside)
        with self.assertRaises(OSError):
            self.pack(apply=True)
        self.original.unlink()
        self.original.mkdir()
        with self.assertRaises(storage.StorageError):
            self.pack(apply=True)
        self.original.rmdir()
        self.original.parent.rename(self.original.parent.with_name("moved"))
        self.original.parent.symlink_to(self.original.parent.with_name("moved"),
                                       target_is_directory=True)
        with self.assertRaises(OSError):
            self.pack(apply=True)
        alias = self.package.parent / "alias"
        alias.symlink_to(self.package, target_is_directory=True)
        with self.assertRaises(storage.StorageError):
            storage.pack_file(alias, self.relative, apply=True)
        self.assertEqual(b"external", outside.read_bytes())

    def test_fifo_is_rejected_without_blocking(self):
        self.original.unlink()
        os.mkfifo(self.original)
        with self.assertRaises(storage.StorageError):
            self.pack(apply=True)

    def test_declared_files_and_reviews_protected_for_both_operations(self):
        for key in ("files", "reviews"):
            with self.subTest(key=key):
                self.task.write_text(json.dumps({key: [{"path": self.relative}]}),
                                     encoding="utf-8")
                with self.assertRaisesRegex(storage.StorageError, "declares"):
                    self.pack(apply=True)
                with self.assertRaisesRegex(storage.StorageError, "declares"):
                    self.restore(apply=True)
                self.assertEqual(self.content, self.original.read_bytes())

    def test_source_change_verification_failure_and_publication_conflict_keep_source(self):
        inflate = storage._inflate

        def change_source(*args, **kwargs):
            inflate(*args, **kwargs)
            self.original.write_bytes(b"updated by another writer")

        with patch.object(storage, "_inflate", side_effect=change_source):
            with self.assertRaisesRegex(storage.StorageError, "source changed"):
                self.pack(apply=True)
        self.assertEqual(b"updated by another writer", self.original.read_bytes())
        self.assertFalse(self.compressed.exists())
        self.original.write_bytes(self.content)
        with patch.object(storage, "_inflate", side_effect=storage.StorageError("bad gzip")):
            with self.assertRaises(storage.StorageError):
                self.pack(apply=True)
        self.assertEqual(self.content, self.original.read_bytes())
        publish = storage._publish

        def conflicting_publish(parent, temporary, name):
            self.compressed.write_bytes(b"do not overwrite")
            publish(parent, temporary, name)

        with patch.object(storage, "_publish", side_effect=conflicting_publish):
            with self.assertRaises(FileExistsError):
                self.pack(apply=True)
        self.assertEqual(self.content, self.original.read_bytes())
        self.assertEqual(b"do not overwrite", self.compressed.read_bytes())
        with self.assertRaisesRegex(storage.StorageError, "incomplete"):
            self.pack(apply=True)

    def test_second_publication_failure_and_later_source_change_keep_source(self):
        publish = storage._publish
        calls = []

        def fail_second(parent, temporary, name):
            calls.append(name)
            if len(calls) == 2:
                raise OSError("manifest publication failed")
            publish(parent, temporary, name)

        with patch.object(storage, "_publish", side_effect=fail_second):
            with self.assertRaises(OSError):
                self.pack(apply=True)
        self.assertTrue(self.compressed.exists())
        self.assertFalse(self.metadata.exists())
        self.assertEqual(self.content, self.original.read_bytes())
        self.compressed.unlink()
        calls.clear()

        def change_after_publication(parent, temporary, name):
            publish(parent, temporary, name)
            calls.append(name)
            if len(calls) == 2:
                self.original.write_bytes(b"latest source")

        with patch.object(storage, "_publish", side_effect=change_after_publication):
            with self.assertRaisesRegex(storage.StorageError, "source changed"):
                self.pack(apply=True)
        self.assertEqual(b"latest source", self.original.read_bytes())
        self.assertTrue(self.metadata.exists())

    def test_task_or_archive_parent_changes_are_detected(self):
        inflate = storage._inflate

        def change_task(*args, **kwargs):
            inflate(*args, **kwargs)
            self.task.write_text('{"files":[],"reviews":[],"new":true}', encoding="utf-8")

        with patch.object(storage, "_inflate", side_effect=change_task):
            with self.assertRaisesRegex(storage.StorageError, "task.json changed"):
                self.pack(apply=True)
        self.assertEqual(self.content, self.original.read_bytes())
        (self.package / "cold-storage").rename(self.package / "saved-storage")
        (self.package / "cold-storage").symlink_to(self.package / "saved-storage",
                                                  target_is_directory=True)
        with self.assertRaises(OSError):
            self.pack(apply=True)

    def test_archive_directory_replacement_after_publication_never_removes_source(self):
        publish = storage._publish
        calls = []

        def move_archive(parent, temporary, name):
            publish(parent, temporary, name)
            calls.append(name)
            if len(calls) == 2:
                self.compressed.parent.rename(self.compressed.parent.with_name("moved"))

        with patch.object(storage, "_publish", side_effect=move_archive):
            with self.assertRaises((storage.StorageError, OSError)):
                self.pack(apply=True)
        self.assertEqual(self.content, self.original.read_bytes())
        self.assertTrue((self.compressed.parent.with_name("moved") /
                         self.compressed.name).is_file())

    def test_restore_recreates_only_required_report_parents(self):
        self.pack(apply=True)
        self.original.parent.rmdir()
        self.original.parent.parent.rmdir()
        self.assertEqual("would_restore", self.restore()["status"])
        self.assertFalse(self.original.parent.exists())
        self.assertEqual("restored", self.restore(apply=True)["status"])
        self.assertEqual(self.content, self.original.read_bytes())

    def test_crc_failure_with_matching_compressed_digest_is_rejected(self):
        self.pack(apply=True)
        value = json.loads(self.metadata.read_bytes())
        damaged = self.compressed.read_bytes()[:-4]
        self.compressed.write_bytes(damaged)
        original = {key: value["original"][key]
                    for key in ("sha256", "size", "mode", "mtime_ns")}
        compressed = {"sha256": hashlib.sha256(damaged).hexdigest(), "size": len(damaged)}
        self.metadata.write_bytes(storage._canonical(
            storage._manifest(self.package, self.relative, original, compressed)))
        with self.assertRaisesRegex(storage.StorageError, "gzip verification failed"):
            self.restore(apply=True)
        self.assertFalse(self.original.exists())

    def test_invalid_manifest_metadata_and_task_contract_fail_closed(self):
        self.pack(apply=True)
        value = json.loads(self.metadata.read_bytes())
        for key, replacement in (("mtime_ns", 2 ** 70), ("mode", 0o10000),
                                  ("size", True), ("sha256", "incorrect")):
            with self.subTest(key=key):
                changed = json.loads(json.dumps(value))
                changed["original"][key] = replacement
                core = {name: item for name, item in changed.items()
                        if name != "manifest_sha256"}
                changed["manifest_sha256"] = hashlib.sha256(storage._canonical(core)).hexdigest()
                self.metadata.write_text(json.dumps(changed), encoding="utf-8")
                with self.assertRaises(storage.StorageError):
                    self.restore(apply=True)
        for invalid in (b"[]", b'{"files":{}}', b'{"reviews":[{}]}'):
            self.task.write_bytes(invalid)
            with self.assertRaises(storage.StorageError):
                self.pack(apply=True)
        self.assertFalse(self.original.exists())

    def test_cli_processes_explicit_paths_independently_and_returns_json_errors(self):
        with redirect_stdout(io.StringIO()) as output:
            result = storage.main(["pack", "--package", str(self.package),
                                   "--path", self.relative, "--path", "runs/original.json",
                                   "--path", self.relative, "--apply"])
        self.assertEqual(2, result)
        receipt = json.loads(output.getvalue())
        self.assertEqual(["packed", "error"], [row["status"] for row in receipt["results"]])
        self.assertFalse(self.original.exists())
        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, storage.main(["restore", "--package", str(self.package),
                                             "--path", self.relative, "--apply"]))


if __name__ == "__main__":
    unittest.main()
