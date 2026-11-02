"""Explicit path transport preserves the completed author's actual file bodies."""

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

from taskflow import execution
from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_store import evidence_ref
from taskflow.packages import check_task
from tests import test_execution_optional_candidate_delivery as fixture


ADDITIONAL_DECLARATIONS = """
 value=json.loads((root/'task.json').read_text())
 for index,name in enumerate(('manifest.json','record.md','reference-seal.json')):
  path='record/'+name
  (root/path).parent.mkdir(parents=True,exist_ok=True)
  (root/path).write_text('Actual author private provenance '+name+'\\n')
  value['files'].append({'id':'provenance_'+str(index),'path':path,'role':'teacher','references':['S1']})
 (root/'task.json').write_text(json.dumps(value))
"""


class OptionalCandidateTransportTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.OptionalCandidateDeliveryTests(
            "test_full_archive_optional_candidate_extends_only_actual_declared_delivery")
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root, self.store = self.case.root, self.case.store
        script = fixture.SCRIPT.replace(
            " (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')",
            ADDITIONAL_DECLARATIONS + " (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')")
        self.case.script.write_text(script)
        self.attempt = self.case.run_author()
        self.case.archive_author()
        self.original = self.case.archive / "supplemental/trees/work/next-task/neutral"
        self.transported = self.root / "transported"
        shutil.copytree(self.original, self.transported)
        self.value = json.loads((self.original / "task.json").read_text())
        self.projected = json.loads(json.dumps(self.value))
        for row in self.projected["files"]:
            if row["path"].startswith("record/"):
                old = self.transported / row["path"]
                row["path"] = "draft/evaluation/author-provenance/" + old.name
                new = self.transported / row["path"]
                new.parent.mkdir(parents=True, exist_ok=True)
                old.rename(new)
        self.write_task(self.projected)
        proof = self.root / "transport-proof.json"
        proof.write_text('{"nature":"explicit neutral path-only transport authority"}')
        self.proof = [evidence_ref(proof, "explicit byte-preserving private path transport")]

    def write_task(self, value):
        (self.transported / "task.json").write_text(json.dumps(value))

    def adopt(self, **changes):
        return self.store.record_optional_candidate_delivery(
            self.attempt["attempt_id"], archive=self.case.archive,
            candidate_subpath="next-task/neutral", evidence=self.case.refs,
            transport_candidate=changes.get("transport_candidate", self.transported),
            transport_evidence=changes.get("transport_evidence", self.proof))

    def test_explicit_transport_binds_original_full_archive_and_actual_derived_candidate(self):
        self.assertEqual(3, len(check_task(self.original, write_report=False)["errors"]))
        self.assertEqual([], check_task(self.transported, write_report=False)["errors"])
        with self.assertRaisesRegex(TaskflowError, "incomplete"):
            self.case.adopt()
        original_manifest = (self.original / "task.json").read_bytes()
        result = self.adopt()
        delivery = result["supplemental_delivery"]
        transport = delivery["transport"]
        self.assertEqual(str(self.transported), delivery["candidate_root"])
        self.assertEqual(str(self.original), transport["source_candidate_root"])
        self.assertEqual(3, len(transport["mapping"]))
        self.assertEqual(self.proof, transport["evidence"])
        self.assertEqual(sha256(original_manifest), transport["source_task"]["sha256"])
        self.assertEqual(len(self.value["files"]) + 1, len(transport["source_files"]))
        self.assertEqual(len(self.projected["files"]) + 1, len(delivery["file_refs"]))
        for source, target in zip(self.value["files"], self.projected["files"]):
            self.assertEqual((self.original / source["path"]).read_bytes(),
                             (self.transported / target["path"]).read_bytes())
            if source["role"] in ("prompt", "input"):
                self.assertEqual(source["path"], target["path"])
        delivered = self.store._delivered_artifacts(result)
        self.assertIn(sha256((self.transported / "task.json").read_bytes()), delivered)
        child = execution.capture_candidate(
            self.store, self.transported, "transport-child", parent_id="parent",
            author_attempt=self.attempt["attempt_id"])
        self.assertEqual(self.attempt["attempt_id"], child["author_attempt"])
        self.assertEqual([], child["external_author_sids"])
        self.assertEqual(original_manifest, (self.original / "task.json").read_bytes())

    def test_old_caller_reuses_locked_transport_but_different_transport_or_proof_is_rejected(self):
        result = self.adopt()
        self.assertEqual(result, self.adopt())
        self.assertEqual(result, self.case.adopt())
        other = self.root / "different-transport"
        shutil.copytree(self.transported, other)
        with self.assertRaisesRegex(TaskflowError, "cannot replace"):
            self.adopt(transport_candidate=other)
        another = self.root / "other-proof.json"
        another.write_text('{"nature":"different evidence"}')
        with self.assertRaisesRegex(TaskflowError, "cannot replace"):
            self.adopt(transport_evidence=[evidence_ref(another, "different explicit source")])

    def test_changed_payload_public_or_private_cannot_be_transported(self):
        for name in ("draft/input/prompt.md", "draft/evaluation/author-provenance/manifest.json"):
            with self.subTest(name=name):
                path = self.transported / name
                raw = path.read_bytes()
                path.write_bytes(raw + b"Changed")
                with self.assertRaisesRegex(TaskflowError, "payload bytes differ"):
                    self.adopt()
                path.write_bytes(raw)

    def test_metadata_changes_beyond_permitted_paths_and_reordered_declarations_are_rejected(self):
        changes = [lambda value: value.update(title="Different title"),
                   lambda value: value["scoring"].update(total=True),
                   lambda value: value["files"][-1].update(role="verification"),
                   lambda value: value["files"].reverse()]
        for change in changes:
            value = json.loads(json.dumps(self.projected))
            change(value)
            self.write_task(value)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.adopt()
        self.write_task(self.projected)

    def test_public_path_or_non_record_private_path_changes_are_rejected(self):
        for name in ("draft/input/prompt.md", "draft/evaluation/answer.md"):
            value = json.loads(json.dumps(self.projected))
            row = next(row for row in value["files"] if row["path"] == name)
            old = self.transported / name
            row["path"] = "checks/moved-" + old.name
            new = self.transported / row["path"]
            new.parent.mkdir(exist_ok=True)
            old.rename(new)
            self.write_task(value)
            with self.subTest(name=name), self.assertRaisesRegex(TaskflowError, "only teacher record/"):
                self.adopt()
            new.rename(old)
        self.write_task(self.projected)

    def test_future_capture_and_old_retry_revalidate_source_target_and_evidence(self):
        result = self.adopt()
        path = self.transported / "draft/evaluation/author-provenance/record.md"
        raw = path.read_bytes()
        path.write_bytes(raw + b"Late drift")
        with self.assertRaises(TaskflowError):
            self.store._delivered_artifacts(result)
        with self.assertRaises(TaskflowError):
            self.case.adopt()
        path.write_bytes(raw)
        proof = Path(self.proof[0]["path"])
        proof_raw = proof.read_bytes()
        proof.write_bytes(proof_raw + b" ")
        with self.assertRaisesRegex(TaskflowError, "evidence changed"):
            self.store._delivered_artifacts(result)
        proof.write_bytes(proof_raw)
        source = self.case.work / "next-task/neutral/record/record.md"
        source.write_text("Late original work change")
        with self.assertRaisesRegex(TaskflowError, "terminal work"):
            self.store._delivered_artifacts(result)

    def test_source_structural_errors_beyond_private_record_paths_are_not_repaired(self):
        case = fixture.OptionalCandidateDeliveryTests(
            "test_full_archive_optional_candidate_extends_only_actual_declared_delivery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        extra = ADDITIONAL_DECLARATIONS.replace(
            " (root/'task.json').write_text", " value['title']=''\n (root/'task.json').write_text")
        case.script.write_text(fixture.SCRIPT.replace(
            " (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')", extra))
        attempt = case.run_author()
        case.archive_author()
        original = case.archive / "supplemental/trees/work/next-task/neutral"
        target = case.root / "transport"
        shutil.copytree(original, target)
        value = json.loads((target / "task.json").read_text())
        for row in value["files"]:
            if row["path"].startswith("record/"):
                source = target / row["path"]
                row["path"] = "checks/provenance/" + source.name
                destination = target / row["path"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                source.rename(destination)
        (target / "task.json").write_text(json.dumps(value))
        with self.assertRaisesRegex(TaskflowError, "errors beyond"):
            case.store.record_optional_candidate_delivery(
                attempt["attempt_id"], archive=case.archive, candidate_subpath="next-task/neutral",
                evidence=case.refs, transport_candidate=target, transport_evidence=self.proof)

    def test_transport_drift_during_reference_capture_is_rejected(self):
        target = self.transported / "draft/evaluation/author-provenance/manifest.json"
        original_ref = evidence_ref

        def changing_ref(path, location):
            if path == target:
                path.write_bytes(path.read_bytes() + b"Concurrent drift")
            return original_ref(path, location)

        with mock.patch("taskflow.execution_store.evidence_ref", side_effect=changing_ref):
            with self.assertRaisesRegex(TaskflowError, "changed during validation"):
                self.adopt()

    def test_special_transport_payloads_are_rejected_in_a_bounded_real_process(self):
        path = self.transported / "draft/evaluation/author-provenance/record.md"
        raw = path.read_bytes()
        arguments = {"attempt": self.attempt, "archive": str(self.case.archive),
                     "transport": str(self.transported), "evidence": self.proof}
        code = """
import json,sys
from pathlib import Path
from taskflow.contracts import TaskflowError
from taskflow.execution_store import ExecutionStore
args=json.loads(sys.argv[1])
try:
 ExecutionStore._optional_candidate_material(args['attempt'],Path(args['archive']),'next-task/neutral',transport_candidate=Path(args['transport']),transport_evidence=args['evidence'])
except TaskflowError:
 sys.exit(0)
sys.exit(1)
"""
        for kind in ("fifo", "symlink", "directory"):
            with self.subTest(kind=kind):
                path.unlink()
                if kind == "fifo":
                    os.mkfifo(path)
                elif kind == "symlink":
                    path.symlink_to(self.original / "record/record.md")
                else:
                    path.mkdir()
                result = subprocess.run([sys.executable, "-c", code, json.dumps(arguments)],
                                        cwd=Path(__file__).resolve().parents[1],
                                        capture_output=True, text=True, timeout=3)
                self.assertEqual(0, result.returncode, result.stderr)
                if kind == "directory":
                    path.rmdir()
                else:
                    path.unlink()
                path.write_bytes(raw)


if __name__ == "__main__":
    unittest.main()
