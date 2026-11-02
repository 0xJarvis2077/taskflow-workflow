"""Observed aliases remain within the exact archived public attempt."""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, sha256, write_json
from taskflow.execution_flow import _review_observations


class ObservationAliasTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.archive = self.root / "public-archive"
        self.runtime = self.root / "quality-runtime"
        self.source = self.archive / "supplemental/trees/inner/work/deliverables/report.json"
        self.target = "actual-public-work/deliverables/report.json"
        self.staged = self.runtime / "inputs" / self.target
        for p in (self.source, self.staged):
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('{"observation":"neutral"}')
        self.candidate = {"candidate_id": "neutral-current", "task_id": "neutral", "input_digest": "a" * 64,
                          "content_digest": "b" * 64, "evaluation_digest": "c" * 64}
        write_json(self.archive / "manifest.json", {"candidate_binding": self.candidate})
        self.record = {"state": {"public_archive": str(self.archive), "archives": [str(self.archive)]}}
        self.attempt = {"runtime_path": str(self.runtime), "intent_id": "neutral-review"}
        self.manifest = {"candidate_binding": self.candidate, "intent_id": "neutral-review", "inputs": [
            {"source": str(self.source), "target": self.target, "kind": "submission",
             "sha256": sha256(self.source.read_bytes()), "size": self.source.stat().st_size}]}

    def read(self, manifest=None):
        with patch("taskflow.execution_runner._validate", return_value=manifest or self.manifest):
            return _review_observations(self.record, self.candidate, self.attempt)

    def test_exact_manifest_source_and_staged_bytes_are_accepted(self):
        result = self.read()
        self.assertEqual(str(self.source), result[self.target]["path"])
        self.assertEqual("submission", result[self.target]["kind"])

    def test_foreign_same_bytes_cannot_be_relabelled_as_this_public_archive(self):
        foreign = self.root / "foreign.json"
        foreign.write_bytes(self.source.read_bytes())
        altered = copy.deepcopy(self.manifest)
        altered["inputs"][0]["source"] = str(foreign)
        with self.assertRaisesRegex(TaskflowError, "outside the exact"):
            self.read(altered)

    def test_changed_staged_bytes_or_another_public_version_are_rejected(self):
        self.staged.write_text("changed")
        with self.assertRaisesRegex(TaskflowError, "SHA differs"):
            self.read()
        self.staged.write_bytes(self.source.read_bytes())
        write_json(self.archive / "manifest.json", {"candidate_binding": {**self.candidate, "input_digest": "d" * 64}})
        with self.assertRaisesRegex(TaskflowError, "another public candidate"):
            self.read()

    def test_duplicate_alias_or_unregistered_archive_is_rejected(self):
        altered = copy.deepcopy(self.manifest)
        altered["inputs"].append(altered["inputs"][0])
        with self.assertRaisesRegex(TaskflowError, "ambiguous"):
            self.read(altered)
        self.record["state"]["archives"] = []
        with self.assertRaisesRegex(TaskflowError, "recorded provenance"):
            self.read()


if __name__ == "__main__":
    unittest.main()
