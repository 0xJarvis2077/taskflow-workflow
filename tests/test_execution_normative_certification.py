"""Native-format byte-sealed fixtures around local subprocesses; zero models."""

import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow import execution_runner as runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import validate_normative_certification
from taskflow.execution_store import ExecutionStore
from tests import test_execution_formal as formal_tests


class NormativeCertificationTests(unittest.TestCase):
    def fixture(self):
        source = formal_tests.NativeQualityHandoffTests(
            "test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic")
        source.setUp()
        self.addCleanup(source.doCleanups)
        self.complete_source(source, source.material)
        return source

    def complete_source(self, source, root):
        """Use an actual neutral process for terminal trace and exit metadata."""
        sid = source.attempts[root.name]["cli_sid"]
        events = [{"type": "thread.started", "thread_id": sid},
                  {"type": "item.completed", "item": {"type": "agent_message", "text": "Neutral schema delivery; zero models."}},
                  {"type": "turn.completed"}]
        process = subprocess.run(
            [sys.executable, "-c", "import json,sys\nfor event in json.loads(sys.argv[1]): print(json.dumps(event))", json.dumps(events)],
            check=True, capture_output=True)
        (root / "trace.jsonl").write_bytes(process.stdout)
        write_json(root / "termination.json", {"returncode": process.returncode, "process_ended": True})
        collected = read_json(root / "collection.json")
        collected.update(delivery_complete=True, model_completed=True,
                         termination=read_json(root / "termination.json"))
        self.reseal(root, collected)

    @staticmethod
    def reseal(root, collected):
        # Fixture construction only; a production collection is never rewritten.
        (root / "collection-lock.json").unlink()
        (root / "collection.json").unlink()
        runner._save_collection(root, collected)

    @staticmethod
    def certification(source):
        return {"schema_version": 1, "kind": "native_quality_handoff",
                "candidate_binding": {key: source.candidate[key] for key in
                                      ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                "source_author_flow_id": source.parent_id, "authorization": source.authorization,
                "native_quality_handoff": source.handoff()}

    def test_online_and_offline_read_the_exact_signed_selection_without_writes(self):
        source = self.fixture()
        certification = self.certification(source)
        self.assertTrue(all("size" not in row for row in source.candidate["files"]))
        before = {str(path): path.read_bytes() for path in source.root.rglob("*") if path.is_file()}
        with patch("subprocess.run", side_effect=AssertionError("readonly validation launched a process")):
            offline = validate_normative_certification(candidate=source.candidate, certification=certification)
            online = validate_normative_certification(candidate=source.candidate, certification=certification, store=source.store)
        self.assertEqual(certification["native_quality_handoff"]["normative_sources"], offline)
        self.assertEqual(offline, online)
        self.assertEqual(["draft/evaluation/answer.md", "draft/evaluation/rubric.md"], [row["package_path"] for row in offline])
        self.assertTrue(all(row["size"] == Path(row["path"]).stat().st_size for row in offline))
        self.assertEqual(before, {str(path): path.read_bytes() for path in source.root.rglob("*") if path.is_file()})

    def test_typed_authority_current_bytes_and_signed_selection_cannot_be_substituted(self):
        source = self.fixture()
        original = self.certification(source)
        mutations = {
            "boolean schema": lambda cert: cert.update(schema_version=True),
            "extra untyped field": lambda cert: cert.update(accepted=True),
            "different candidate": lambda cert: cert["candidate_binding"].update(candidate_id="another-candidate"),
            "different flow": lambda cert: cert.update(source_author_flow_id="another-flow"),
            "different authority item": lambda cert: cert["native_quality_handoff"]["protocol_map"]["authorization_item"].update(task_id="another-task"),
            "unsigned history": lambda cert: cert["native_quality_handoff"]["scoring_source_allowlist"].append("draft/evaluation/research-history.md"),
            "selection sha": lambda cert: cert["native_quality_handoff"]["normative_sources"][0].update(sha256="0" * 64),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                certification = copy.deepcopy(original)
                mutate(certification)
                with self.assertRaises(TaskflowError):
                    validate_normative_certification(candidate=source.candidate, certification=certification)
        duplicate = source.root / "same-bytes-foreign-report.json"
        shutil.copyfile(source.material / "artifacts/adoption-readiness.json", duplicate)
        certification = copy.deepcopy(original)
        certification["native_quality_handoff"]["source_refs"]["adoption"] = {"path": str(duplicate), **runner._info(duplicate)}
        with self.assertRaisesRegex(TaskflowError, "source refs differ"):
            validate_normative_certification(candidate=source.candidate, certification=certification)
        certification = copy.deepcopy(original)
        certification["native_quality_handoff"]["certification"]["accepted_review_id"] = "other-review"
        with self.assertRaisesRegex(TaskflowError, "current stored native handoff"):
            validate_normative_certification(candidate=source.candidate, certification=certification, store=source.store)
        for field, value in (("sha256", "0" * 64), ("role", "teacher")):
            candidate = copy.deepcopy(source.candidate)
            next(row for row in candidate["files"] if row["role"] == "answer")[field] = value
            with self.subTest(candidate_field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=candidate, certification=original)

    def test_completed_flags_do_not_replace_actual_trace_process_or_source_locks(self):
        for label in ("delivery incomplete", "model incomplete", "nonzero process", "wrong sid", "missing terminal", "fixture role", "output drift", "unlocked protocol"):
            with self.subTest(label=label):
                source = self.fixture()
                certification = self.certification(source)
                root = source.material
                collected = read_json(root / "collection.json")
                if label == "delivery incomplete":
                    collected["delivery_complete"] = False
                elif label == "model incomplete":
                    collected["model_completed"] = False
                elif label == "nonzero process":
                    write_json(root / "termination.json", {"returncode": 1, "process_ended": True})
                    collected["termination"] = read_json(root / "termination.json")
                elif label == "wrong sid":
                    trace = (root / "trace.jsonl").read_text().replace(source.attempts[root.name]["cli_sid"], source.item["source_author_sid"])
                    (root / "trace.jsonl").write_text(trace)
                elif label == "missing terminal":
                    lines = (root / "trace.jsonl").read_text().splitlines()
                    (root / "trace.jsonl").write_text("\n".join(lines[:-1]) + "\n")
                elif label == "fixture role":
                    manifest = read_json(root / "manifest.json")
                    manifest["nature"] = "test_fixture"
                    write_json(root / "manifest.json", manifest)
                    write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
                    certification["native_quality_handoff"]["certification"]["fresh"]["manifest"].update(runner._info(root / "manifest.json"))
                elif label == "output drift":
                    (root / "artifacts/intake.md").write_text("Changed after sealing.")
                else:
                    (root / "request.md").write_text("Only a self-reported protocol SHA; no locked complete protocol.")
                if label not in {"output drift", "unlocked protocol"}:
                    self.reseal(root, collected)
                with self.assertRaises(TaskflowError):
                    validate_normative_certification(candidate=source.candidate, certification=certification)

    def test_fresh_affected_normative_choice_preserves_the_original_composite_scope(self):
        source = self.fixture()
        old_task = source.root / "old-task"
        shutil.copytree(source.task, old_task)
        (old_task / "draft/evaluation/rubric.md").write_text("Old private standard requiring repair.\n")
        old = capture_candidate(ExecutionStore(source.root / "old.sqlite3"), old_task, "native-old")
        source.candidates[old["candidate_id"]] = old
        old_root = source.source(old, "old-quality", coverage="unchanged_scope", outcome="revision_required")
        fresh_root = source.source(source.candidate, "fresh-quality", coverage="affected", outcome="accepted")
        self.complete_source(source, old_root)
        self.complete_source(source, fresh_root)
        source.bind_receipt("fresh-accepted", "fresh-quality")
        graph = {"recheck_id": "fresh-accepted",
                 "whole_source": {"candidate_id": old["candidate_id"], "attempt_id": "old-quality", "sid": source.attempts["old-quality"]["cli_sid"],
                                  "report": {"path": str(old_root / "artifacts/adoption-readiness.json"), **runner._info(old_root / "artifacts/adoption-readiness.json")}},
                 "affected_source": {"candidate_id": source.candidate["candidate_id"], "attempt_id": "fresh-quality", "sid": source.attempts["fresh-quality"]["cli_sid"],
                                     "report": {"path": str(fresh_root / "artifacts/adoption-readiness.json"), **runner._info(fresh_root / "artifacts/adoption-readiness.json")}},
                 "input_digest": source.candidate["input_digest"], "closed_defect_ids": ["closed-defect"],
                 "private_changes": [{"path": "draft/evaluation/rubric.md"}], "source_limits": {"whole": {}, "affected": {}}}
        source.current = {"review_id": "composed", "attempt_id": "fresh-quality", "whole_package": False,
                          "snapshot_bound_acceptance": True, "composite_quality_coverage": graph}
        certification = self.certification(source)
        original = copy.deepcopy(certification)
        offline = validate_normative_certification(candidate=source.candidate, certification=certification)
        self.assertEqual(offline, validate_normative_certification(candidate=source.candidate, certification=certification, store=source.store))
        self.assertEqual(original, certification)
        self.assertEqual("affected", certification["native_quality_handoff"]["certification"]["fresh"]["coverage"])
        self.assertEqual("unchanged_scope", certification["native_quality_handoff"]["certification"]["whole"]["coverage"])
        certification["native_quality_handoff"]["certification"]["fresh"]["coverage"] = "whole_package"
        with self.assertRaisesRegex(TaskflowError, "signed scope"):
            validate_normative_certification(candidate=source.candidate, certification=certification)


if __name__ == "__main__":
    unittest.main()
