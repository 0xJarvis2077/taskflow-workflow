"""Future factory prompts render the current unsigned candidate contract."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import _candidate_structure, start_author_from_packet
from taskflow.execution_runner import _validate
from taskflow.execution_store import evidence_ref
from taskflow.packages import check_task
from tests import test_execution_author_factory as fixture


class AuthorOutputProtocolTests(unittest.TestCase):
    def setUp(self):
        # Compose only the established neutral fixture; do not inherit its tests.
        self.case = fixture.AuthorFactoryTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.factory()

    def render(self):
        # This uses actual prepare/enqueue/request/manifest. Only the native
        # host-probe seam is converted to test_fixture; no worker/model runs.
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.case.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("never run a model")):
            return start_author_from_packet(self.case.store, **self.case.kwargs())

    def placeholders(self):
        task = read_json(self.case.task / "task.json")
        provenance = "draft/evaluation/author-provenance/self-check.json"
        observation = {"role": "informed_author", "reason": "Neutral local observation.",
                       "summary": "Author-only checks, pending independent review.",
                       "independent": False, "author_checked": True}
        write_json(self.case.task / provenance, observation)
        task["files"].append({"id": "selfcheck", "path": provenance, "role": "teacher", "references": ["S1"]})
        for index, entry in enumerate(task["reviews"]):
            entry.update(id="pending_" + str(index), path="reports/pending-" + str(index) + ".json")
            write_json(self.case.task / entry["path"], {
                "schema_version": 1, "review_id": entry["id"], "kind": entry["kind"],
                "task_id": task["task_id"], "status": "inconclusive",
                "independent_quality_passed": False, "new_formal_score": None,
                "authority": "Unsigned author metadata only.", "scope": "Pending independent quality.",
                "limitations": ["Neutral author observations, no independent pass."],
                "evidence": [provenance], "local_checks": observation, "local_evidence": [provenance]})
        write_json(self.case.task / "task.json", task)
        return task, observation

    def test_actual_factory_request_and_manifest_lock_canonical_metadata_instructions(self):
        result = self.render()
        runtime = Path(result["runtime"])
        manifest = _validate(runtime)
        raw = (runtime / "request.md").read_bytes()
        request = raw.decode()
        for text in ("unique task.reviews[].id", "unique task.reviews[].path values under reports/",
                     "never in task.files", "review_id=r.id", "kind=r.kind", "task_id=task.task_id",
                     "status=inconclusive", "independent_quality_passed=false", "new_formal_score=null",
                     "scope, limitations, evidence, authority, local_checks or local_evidence",
                     "author self-check observations separately as declared private provenance",
                     "local_evidence is a unique list of current declared private relative paths",
                     "scoring.rubric_file", "whose role is rubric"):
            self.assertIn(text, request)
        self.assertEqual(sha256(raw), manifest["request"]["sha256"])
        self.assertEqual([], self.case.store.dashboard()["attempts"])

    def test_rendered_placeholder_contract_accepts_declared_ids_and_separate_self_checks(self):
        task, original_observation = self.placeholders()
        result = self.render()
        request = (Path(result["runtime"]) / "request.md").read_text()
        self.assertIn("review_id=r.id", request)
        check = _candidate_structure(self.case.task, "neutral")
        self.assertEqual([], check["errors"])
        payloads = {row["path"] for row in task["files"]}
        for entry in task["reviews"]:
            self.assertTrue(entry["path"].startswith("reports/"))
            self.assertNotIn(entry["path"], payloads)
            report = read_json(self.case.task / entry["path"])
            self.assertEqual(entry["id"], report["review_id"])
            self.assertEqual(entry["kind"], report["kind"])
            self.assertEqual(original_observation, report["local_checks"])
            self.assertEqual("inconclusive", report["status"])
            self.assertFalse(report["independent_quality_passed"])
        self.assertTrue(all(row["state"] != "passed" for row in check["reviews"]))

    def test_old_top_level_self_check_or_signed_fields_do_not_fit_the_consumer(self):
        task, observation = self.placeholders()
        path = self.case.task / task["reviews"][0]["path"]
        original = path.read_bytes()
        report = read_json(path)
        changes = [{key: value} for key, value in observation.items()]
        changes += [{"review_id": "foreign"}, {"status": "passed"},
                    {"independent_quality_passed": True}, {"new_formal_score": 1},
                    {"independent_signer": "self"}]
        for change in changes:
            write_json(path, {**report, **change})
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                _candidate_structure(self.case.task, "neutral")
        path.write_bytes(original)
        invalid = copy.deepcopy(task)
        entry = invalid["reviews"][0]
        entry["path"] = "checks/reports/self-check.json"
        write_json(self.case.task / entry["path"], report)
        invalid["files"].append({"id": "invalid_report_payload", "path": entry["path"],
                                 "role": "teacher", "references": ["S1"]})
        write_json(self.case.task / "task.json", invalid)
        self.assertTrue(check_task(self.case.task, write_report=False)["errors"])
        with self.assertRaises(TaskflowError):
            _candidate_structure(self.case.task, "neutral")

    def test_future_rendering_cannot_replace_an_existing_locked_request(self):
        result = self.render()
        root = self.case.directory
        paths = [root / "factory-intent.json", root / "spec.json",
                 Path(result["runtime"]) / "manifest.json", Path(result["runtime"]) / "request.md"]
        before = {path: path.read_bytes() for path in paths}
        self.case.packet_data["author_work"] = "Changed future business instruction"
        write_json(self.case.packet, self.case.packet_data)
        self.case.packet_ref = evidence_ref(self.case.packet, "changed packet cannot replace the existing lock")
        with self.assertRaisesRegex(TaskflowError, "published continuation evidence differs"):
            self.render()
        for path, raw in before.items():
            self.assertEqual(raw, path.read_bytes())
        self.assertEqual([], self.case.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
