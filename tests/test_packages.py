import io
from contextlib import redirect_stderr, redirect_stdout
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from taskflow.contracts import (
    TaskflowError, atomic_directory, canonical, parse_json, read_json,
    relative_path, safe_read, sha256, validate_task, write_json,
)
from taskflow.__main__ import main
from taskflow.packages import (
    check_task, export_snapshot, freeze_task, init_task, verify_export, verify_snapshot,
)
from tests.helpers import add_reviews, make_task


class TaskCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.task = make_task(self.root)

    def update(self, change):
        task = read_json(self.task / "task.json")
        change(task)
        write_json(self.task / "task.json", task)

    def freeze(self):
        add_reviews(self.task)
        return Path(freeze_task(self.task, "v1")["path"])

    def test_draft_and_no_overwrite(self):
        draft = self.root / "draft"
        init_task(draft, "draft")
        self.assertEqual(check_task(draft, False)["status"], "failed")
        with self.assertRaises(TaskflowError):
            init_task(draft, "draft")
        with self.assertRaises(TaskflowError):
            init_task(self.root / "invalid", "../escape")

    def test_structure_report_is_separate_and_reports_do_not_self_hash(self):
        report = check_task(self.task)
        self.assertEqual(report["status"], "passed")
        self.assertTrue(Path(report["report_path"]).is_file())
        add_reviews(self.task)
        self.assertEqual(report["content_digest"], check_task(self.task, False)["content_digest"])

    def test_configured_scores(self):
        for total in (28, 100):
            with self.subTest(total=total):
                task = make_task(self.root, f"score{total}", total)
                self.assertEqual(check_task(task, False)["status"], "passed")
        self.update(lambda t: t["scoring"].update(total=99))
        self.assertIn("criteria sum", str(check_task(self.task, False)["errors"]))

    def test_invalid_contracts_are_located(self):
        original = read_json(self.task / "task.json")
        changes = [
            (lambda t: t.update(schema_version=2), "schema_version"),
            (lambda t: t["files"].append(t["files"][0]), "unique"),
            (lambda t: t["files"][0].update(path="../outside"), "unsafe"),
            (lambda t: t["files"][0].update(role="both"), "role"),
            (lambda t: t["files"][0].update(references=["answer"]), "private"),
            (lambda t: t["files"][0].update(references=["unknown"]), "unknown references"),
            (lambda t: t["scoring"].update(criteria=[]), "criteria"),
            (lambda t: t["scoring"]["criteria"][0].update(evidence=[]), "evidence"),
            (lambda t: t.update(reviews=[]), "required business"),
            (lambda t: t["reviews"][0].update(path="draft/evaluation/r.md"), "reports/"),
        ]
        for change, message in changes:
            task = json.loads(json.dumps(original))
            change(task)
            with self.subTest(message=message):
                self.assertIn(message, str(validate_task(task)))

    def test_missing_empty_symlink_and_hardlink(self):
        prompt = self.task / "draft/input/prompt.md"
        original = prompt.read_bytes()
        prompt.unlink()
        self.assertEqual(check_task(self.task, False)["status"], "failed")
        prompt.write_bytes(b"")
        self.assertIn("empty", str(check_task(self.task, False)["errors"]))
        prompt.unlink()
        prompt.symlink_to(self.task / "draft/evaluation/answer.md")
        self.assertIn("symlink", str(check_task(self.task, False)["errors"]))
        prompt.unlink()
        os.link(self.task / "draft/evaluation/answer.md", prompt)
        self.assertIn("public and private", str(check_task(self.task, False)["errors"]))
        prompt.unlink()
        prompt.write_bytes(original)
        self.assertEqual(check_task(self.task, False)["status"], "passed")

    def test_safe_path_and_json_boundaries(self):
        for path in ("/abs", "../up", "a/../up", "a//b", "./a", "a\\b", "a\x00b", "C:/test", ""):
            with self.subTest(path=path), self.assertRaises(TaskflowError):
                relative_path(path)
        for raw in (b"[]", b'{"a":1,"a":2}', b'{"a":NaN}', b"invalid"):
            with self.subTest(raw=raw), self.assertRaises(TaskflowError):
                parse_json(raw)
        with self.assertRaises(TaskflowError):
            safe_read(self.task, "checks")

    def test_freeze_needs_current_review_and_actual_config_bytes(self):
        with self.assertRaisesRegex(TaskflowError, "review"):
            freeze_task(self.task, "missing")
        self.assertFalse((self.task / "versions/missing").exists())
        add_reviews(self.task)
        prompt = self.task / "draft/input/prompt.md"
        prompt.write_text("changed", encoding="utf-8")
        self.assertEqual(check_task(self.task, False)["reviews"][0]["state"], "stale")
        with self.assertRaisesRegex(TaskflowError, "stale"):
            freeze_task(self.task, "stale")
        add_reviews(self.task)
        task_json = self.task / "task.json"
        task_json.write_bytes(task_json.read_bytes() + b"\n")
        with self.assertRaisesRegex(TaskflowError, "stale"):
            freeze_task(self.task, "config-bytes")

    def test_reports_identity_failed_invalid(self):
        for update, expected in (({"review_id": "other"}, "identity_mismatch"), ({"status": "failed"}, "failed"), ({"evidence": []}, "invalid_report"), ({"status": "unknown"}, "invalid_report")):
            add_reviews(self.task)
            path = self.task / "reports/business.json"
            report = read_json(path)
            report.update(update)
            write_json(path, report)
            self.assertEqual(check_task(self.task, False)["reviews"][0]["state"], expected)
            with self.assertRaises(TaskflowError):
                freeze_task(self.task, expected)

    def test_review_summary_preserves_limited_scope_and_stale_judgment(self):
        add_reviews(self.task)
        report_path = self.task / "reports/business.json"
        report = read_json(report_path)
        report["scope"] = "Only the neutral allocation arithmetic was checked"
        report["limitations"] = ["The supporting document layout was not reviewed"]
        report["evidence"] = ["prompt#line-1", "answer#line-1"]
        write_json(report_path, report)
        current = check_task(self.task, False)["reviews"][0]
        self.assertEqual(current["state"], "passed")
        self.assertEqual(current["reported_status"], "passed")
        self.assertEqual(current["scope"], report["scope"])
        self.assertEqual(current["limitations"], report["limitations"])
        self.assertEqual(current["evidence_count"], 2)
        self.assertNotIn("evidence", current)
        self.assertEqual(current["path"], "reports/business.json")
        frozen = freeze_task(self.task, "limited")
        self.assertEqual(verify_snapshot(Path(frozen["path"]))["version_id"], "limited")

        prompt = self.task / "draft/input/prompt.md"
        prompt.write_text("Choose 1 unit from capacity 3.\n", encoding="utf-8")
        stale = check_task(self.task, False)["reviews"][0]
        self.assertEqual(stale["state"], "stale")
        self.assertEqual(stale["reported_status"], "passed")
        self.assertEqual(stale["limitations"], report["limitations"])
        with self.assertRaisesRegex(TaskflowError, "stale"):
            freeze_task(self.task, "stale-limited")

    def test_snapshot_roundtrip_no_overwrite_and_live_changes(self):
        snapshot = self.freeze()
        manifest = verify_snapshot(snapshot)
        self.assertEqual(manifest["scoring"]["total"], 7)
        with self.assertRaises(TaskflowError):
            freeze_task(self.task, "v1")
        (self.task / "draft/input/prompt.md").write_text("new", encoding="utf-8")
        self.assertEqual(verify_snapshot(snapshot), manifest)

    def test_solver_export_explicit_and_evaluator_contains_input(self):
        snapshot = self.freeze()
        solver = export_snapshot(snapshot, "solver")
        root = Path(solver["path"])
        self.assertEqual(set(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()), {"draft/input/prompt.md", "package.json"})
        text = (root / "package.json").read_text()
        for private in ("scoring", "answer.md", "rubric", "record.md", "snapshot_digest"):
            self.assertNotIn(private, text)
        evaluator = export_snapshot(snapshot, "evaluator")
        self.assertTrue((Path(evaluator["path"]) / "draft/input/prompt.md").is_file())
        self.assertTrue((Path(evaluator["path"]) / "draft/evaluation/answer.md").is_file())
        with self.assertRaises(TaskflowError):
            export_snapshot(snapshot, "solver")
        with self.assertRaises(TaskflowError):
            export_snapshot(snapshot, "unknown")

    def test_snapshot_tamper_and_unlisted_files(self):
        snapshot = self.freeze()
        target = snapshot / "draft/input/prompt.md"
        data = target.read_bytes()
        target.write_bytes(data + b"tampered")
        with self.assertRaisesRegex(TaskflowError, "integrity"):
            export_snapshot(snapshot, "solver")
        self.assertFalse((self.task / "exports/v1-solver").exists())
        target.write_bytes(data)
        extra = snapshot / "secret.txt"
        extra.write_text("undeclared", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "unexpected"):
            verify_snapshot(snapshot)
        extra.unlink()
        path = snapshot / "manifest.json"
        manifest = read_json(path)
        manifest["version_id"] = "changed"
        write_json(path, manifest)
        with self.assertRaisesRegex(TaskflowError, "digest"):
            verify_snapshot(snapshot)

    def test_concurrent_freeze_only_one_publish(self):
        add_reviews(self.task)
        def attempt():
            try:
                freeze_task(self.task, "race")
                return "success"
            except TaskflowError:
                return "refused"
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: attempt(), range(4)))
        self.assertEqual(results.count("success"), 1)
        verify_snapshot(self.task / "versions/race")

    def test_interruption_cleans_unpublished_work_and_lock(self):
        destination = self.root / "interrupt"
        with self.assertRaises(KeyboardInterrupt):
            with atomic_directory(destination) as stage:
                (stage / "partial").write_bytes(b"partial")
                raise KeyboardInterrupt
        self.assertFalse(destination.exists())
        self.assertFalse(list(self.root.glob(".interrupt*")))
        add_reviews(self.task)
        with patch("taskflow.packages.verify_snapshot", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                freeze_task(self.task, "interrupt")
        self.assertFalse((self.task / "versions/interrupt").exists())

    def test_source_mutation_during_capture_is_rejected(self):
        original = safe_read
        prompt = self.task / "draft/input/prompt.md"
        count = 0
        def mutate(root, rel):
            nonlocal count
            data = original(root, rel)
            if rel == "draft/input/prompt.md":
                count += 1
                if count == 1:
                    prompt.write_bytes(data + b"changed")
            return data
        with patch("taskflow.packages.safe_read", side_effect=mutate):
            self.assertIn("source changed", str(check_task(self.task, False)["errors"]))

    def test_archive_members_and_nested_roles(self):
        def zipped(name, content):
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w") as archive:
                archive.writestr(name, content)
            return stream.getvalue()
        path = self.task / "draft/input/data.zip"
        path.write_bytes(zipped("inner.zip", zipped("table.csv", "a,b\n1,2\n")))
        entry = {"id": "archive", "path": "draft/input/data.zip", "role": "input", "references": [], "archive_members": [{"path": "inner.zip", "role": "input"}, {"path": "inner.zip!/table.csv", "role": "input"}]}
        self.update(lambda t: t["files"].append(entry))
        self.assertEqual(check_task(self.task, False)["status"], "passed")
        self.update(lambda t: t["files"][-1]["archive_members"][-1].update(role="answer"))
        self.assertIn("private member", str(check_task(self.task, False)["errors"]))
        for name in ("../escape", "/absolute"):
            path.write_bytes(zipped(name, b"bad"))
            self.assertIn("unsafe", str(check_task(self.task, False)["errors"]))
        path.write_bytes(b"not zip")
        self.assertIn("unreadable ZIP", str(check_task(self.task, False)["errors"]))

    def test_archive_resource_encryption_duplicate_and_symlink_failures(self):
        path = self.task / "draft/input/data.zip"
        self.update(lambda t: t["files"].append({"id": "data", "path": "draft/input/data.zip", "role": "input", "archive_members": []}))
        def write_entries(entries):
            with zipfile.ZipFile(path, "w") as archive:
                for name, data in entries:
                    archive.writestr(name, data)
        write_entries([("a", b"x"), ("b", b"y")])
        with patch("taskflow.packages.MAX_ARCHIVE_MEMBERS", 1):
            self.assertIn("limit", str(check_task(self.task, False)["errors"]))
        with patch("taskflow.packages.MAX_ARCHIVE_BYTES", 1):
            self.assertIn("limit", str(check_task(self.task, False)["errors"]))
        info = zipfile.ZipInfo("link")
        info.create_system = 3
        info.external_attr = 0o120777 << 16
        write_entries([(info, b"target")])
        self.assertIn("symlink", str(check_task(self.task, False)["errors"]))
        write_entries([("a", b"x")])
        data = bytearray(path.read_bytes())
        central = data.index(b"PK\x01\x02")
        data[central + 8] |= 1
        path.write_bytes(data)
        self.assertIn("encrypted", str(check_task(self.task, False)["errors"]))
        write_entries([("nested.rar", b"x")])
        self.assertIn("unsupported nested", str(check_task(self.task, False)["errors"]))
        write_entries([("inner.zip!/item.txt", b"ambiguous")])
        self.assertIn("reserved nested separator", str(check_task(self.task, False)["errors"]))
        write_entries([("a", b"file"), ("a/b", b"child")])
        self.assertIn("file/directory collision", str(check_task(self.task, False)["errors"]))

    def test_noncooperating_destination_creation_cannot_replace_directory(self):
        from taskflow.contracts import _publish_no_replace

        destination = self.root / "other-writer"
        def race(source, target):
            target.mkdir()
            _publish_no_replace(source, target)
        with patch("taskflow.contracts._publish_no_replace", side_effect=race):
            with self.assertRaises(TaskflowError):
                with atomic_directory(destination) as stage:
                    (stage / "ours").write_bytes(b"ours")
        self.assertTrue(destination.is_dir())
        self.assertEqual(list(destination.iterdir()), [])

    def test_check_report_binding_is_verified_independently_of_manifest_hash(self):
        snapshot = self.freeze()
        write_json(snapshot / "structural-check.json", {})
        manifest = read_json(snapshot / "manifest.json")
        raw = (snapshot / "structural-check.json").read_bytes()
        manifest["check"].update(size=len(raw), sha256=sha256(raw))
        del manifest["snapshot_digest"]
        manifest["snapshot_digest"] = sha256(canonical(manifest))
        write_json(snapshot / "manifest.json", manifest)
        with self.assertRaisesRegex(TaskflowError, "structural check"):
            verify_snapshot(snapshot)

    def test_public_template_and_received_package_digest(self):
        path = self.task / "draft/input/output-template.csv"
        path.write_text("name,quantity\n", encoding="utf-8")
        self.update(lambda t: t["files"].append({"id": "template", "path": "draft/input/output-template.csv", "role": "template", "references": ["prompt"]}))
        snapshot = self.freeze()
        exported = export_snapshot(snapshot, "solver")
        package = Path(exported["path"])
        files = []
        for entry in exported["files"]:
            data = (package / entry["path"]).read_bytes()
            files.append({**entry, "size": len(data), "sha256": sha256(data)})
        self.assertEqual(sha256(canonical(files)), exported["input_digest"])
        self.assertEqual([e["role"] for e in files], ["prompt", "template"])
        self.assertEqual(verify_export(package)["input_digest"], exported["input_digest"])
        template = package / "draft/input/output-template.csv"
        template.write_text("tampered", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "integrity"):
            verify_export(package)
        template.write_text("name,quantity\n", encoding="utf-8")
        metadata = read_json(package / "package.json")
        write_json(package / "package.json", {**metadata, "scoring": "private"})
        with self.assertRaisesRegex(TaskflowError, "private metadata"):
            verify_export(package)

    def test_snapshot_report_and_task_tampering(self):
        snapshot = self.freeze()
        for relative in ("reports/business.json", "task.json"):
            target = snapshot / relative
            before = target.read_bytes()
            target.write_bytes(before + b"\n")
            with self.subTest(relative=relative), self.assertRaises(TaskflowError):
                verify_snapshot(snapshot)
            target.write_bytes(before)
        target = snapshot / "reports/business.json"
        target.unlink()
        target.symlink_to(self.task / "reports/business.json")
        with self.assertRaisesRegex(TaskflowError, "symlink"):
            verify_snapshot(snapshot)

    def test_export_interruption_and_concurrent_source_change_do_not_publish(self):
        snapshot = self.freeze()
        original = safe_read
        def mutate(root, rel):
            data = original(root, rel)
            if root == snapshot and rel == "draft/input/prompt.md":
                (root / rel).write_bytes(data + b"changed")
            return data
        with patch("taskflow.packages.safe_read", side_effect=mutate):
            with self.assertRaises(TaskflowError):
                export_snapshot(snapshot, "solver")
        self.assertFalse((self.task / "exports/v1-solver").exists())

    def test_direct_cli_full_local_flow(self):
        def invoke(*args):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = main(list(args))
            self.assertEqual(code, 0, err.getvalue())
            return json.loads(out.getvalue())
        invoke("init", "cli-draft", "--root", str(self.root))
        invoke("check", str(self.task))
        add_reviews(self.task)
        snapshot = Path(invoke("freeze", str(self.task), "--version", "cli")["path"])
        invoke("export", str(snapshot), "--audience", "solver")
        record = self.root / "run.json"
        write_json(record, {"run_id": "cli-test", "nature": "test_fixture"})
        run = Path(invoke("run", "import", str(snapshot), "--record", str(record))["path"])
        write_json(self.root / "evaluation.json", {"evaluation_id": "original", "kind": "original", "nature": "test_fixture"})
        invoke("evaluation", "import", str(run), "--record", str(self.root / "evaluation.json"))
        self.assertEqual(invoke("status", str(self.task))["runs"][0]["nature"], "test_fixture")

    def test_hard_kill_staging_never_looks_published(self):
        destination = self.root / "killed"
        script = "from pathlib import Path; from taskflow.contracts import atomic_directory; import os;\nwith atomic_directory(Path(os.environ['TASKFLOW_TEST_DEST'])) as stage:\n (stage/'partial').write_bytes(b'x')\n os._exit(17)\n"
        env = {**os.environ, "TASKFLOW_TEST_DEST": str(destination)}
        result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True)
        self.assertEqual(result.returncode, 17)
        self.assertFalse(destination.exists())
        self.assertTrue((self.root / ".killed.lock").is_file())
        with self.assertRaisesRegex(TaskflowError, "locked"):
            with atomic_directory(destination):
                pass

    def test_cli_help_checks_and_exit_codes(self):
        def cli(*args):
            return subprocess.run([sys.executable, "-m", "taskflow", *args], capture_output=True, text=True)
        self.assertIn("evaluation", cli("--help").stdout)
        self.assertEqual(cli("check", str(self.task), "--no-write").returncode, 0)
        self.assertEqual(cli("freeze", str(self.task), "--version", "bad").returncode, 2)
        self.update(lambda t: t.update(title=""))
        self.assertEqual(cli("check", str(self.task)).returncode, 1)


if __name__ == "__main__":
    unittest.main()
