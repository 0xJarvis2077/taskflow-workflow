"""Neutral local evidence fixtures; none of these are model performance results."""

import concurrent.futures
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.packages import check_task, freeze_task, init_task
from taskflow.records import (
    import_evaluation,
    import_run,
    status,
    verify_evaluation,
    verify_run,
)
from tests.helpers import add_reviews, make_task


class RecordTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.task = self.root / "task"
        self.snapshot = self.task / "versions" / "v1"
        self.snapshot.mkdir(parents=True)
        self.source = self.root / "observation"
        self.source.mkdir()
        (self.source / "answer.txt").write_text("neutral answer", encoding="utf-8")
        (self.source / "trace.txt").write_text("neutral evidence", encoding="utf-8")
        self.reference = {
            "task_id": "neutral",
            "version_id": "v1",
            "input_digest": "public-input-digest",
            "evaluation_digest": "rubric-digest",
            "snapshot_digest": "snapshot-digest",
            "scoring": {
                "total": 28,
                "criteria": [
                    {"id": "c1", "max_score": 12},
                    {"id": "c2", "max_score": 16},
                ],
            },
        }
        self.verify_patch = patch(
            "taskflow.records.verify_snapshot", return_value=self.reference
        )
        self.verify_patch.start()
        self.addCleanup(self.verify_patch.stop)
        self.record = {
            "run_id": "neutral-run",
            "nature": "test_fixture",
            "actual_input_digest": self.reference["input_digest"],
            "status": "completed",
            "requested_config": {"model": "fixture-model", "tools": []},
            "observed_config": {"model": "fixture-model", "tools": []},
            "started_at": "2026-01-01T00:00:00+00:00",
            "ended_at": "2026-01-01T00:00:01+00:00",
            "interventions": [],
            "exceptions": [],
            "input_evidence": ["trace#input-delivery"],
            "isolation_evidence": ["trace#environment-review"],
            "artifacts": [
                {"id": "answer", "path": "answer.txt", "kind": "answer"},
                {"id": "trace", "path": "trace.txt", "kind": "trace"},
            ],
        }

    def save_run(self, changes=None):
        record = deepcopy(self.record)
        if changes:
            record.update(changes)
        path = self.source / "run.json"
        write_json(path, record)
        return import_run(self.snapshot, path)

    def evaluation_record(self, **changes):
        record = {
            "evaluation_id": "e1",
            "kind": "original",
            "input_digest": self.reference["input_digest"],
            "evaluator": "fixture-reviewer",
            "review_status": "unreviewed",
            "items": [
                {"id": "c1", "score": 10, "evidence": ["answer#first"]},
                {"id": "c2", "score": 16, "evidence": ["answer#second"]},
            ],
            "total": 26,
        }
        record.update(changes)
        path = self.source / "evaluation.json"
        write_json(path, record)
        return path

    def test_complete_evidence_does_not_certify_model_or_isolation(self):
        result = self.save_run()
        run = Path(result["path"])
        self.assertEqual(result["classification"], "test_fixture")
        self.assertEqual(result["gaps"], [])
        self.assertEqual(result["model_validity"], "not_determined")
        self.assertIn("independent review", result["isolation"])
        self.assertEqual(
            (run / "record.original.json").read_bytes(),
            (self.source / "run.json").read_bytes(),
        )
        self.assertEqual(verify_run(run)["run_digest"], result["run_digest"])
        self.assertNotEqual(result["imported_at"], self.record["started_at"])

    def test_observation_classification_is_a_pure_evidence_rule(self):
        from taskflow.records import _classify_run, _run_gaps

        record = {**self.record, "nature": "observed_run"}
        artifacts = self.record["artifacts"]
        self.assertEqual(_run_gaps(record, self.reference, artifacts), [])
        self.assertEqual(_classify_run(record, self.reference, []), "evidence_complete")
        self.assertEqual(_classify_run(record, self.reference, ["gap"]), "unknown")
        self.assertEqual(
            _classify_run({**record, "actual_input_digest": "wrong"}, self.reference, []),
            "input_mismatch",
        )
        self.assertEqual(
            _classify_run({**record, "status": "timeout"}, self.reference, []),
            "incomplete",
        )
        unknown = {**record, "nature": [], "requested_config": {"model": [], "tools": 5}}
        self.assertTrue(any("nature" in gap for gap in _run_gaps(unknown, self.reference, artifacts)))

    def test_unknown_mismatch_and_incomplete_are_archived(self):
        for index, changes in enumerate(
            [
                {"actual_input_digest": None, "observed_config": None},
                {"actual_input_digest": {"unexpected": "object"}},
                {"status": "timeout"},
                {"nature": "test_fixture"},
                {"status": [], "started_at": 3},
                {"schema_version": 999},
            ]
        ):
            with self.subTest(changes=changes):
                changes["run_id"] = f"r{index}"
                result = self.save_run(changes)
                self.assertTrue(Path(result["path"]).is_dir())
                self.assertEqual(result["classification"], "test_fixture")
                if changes != {"nature": "test_fixture", "run_id": f"r{index}"}:
                    self.assertTrue(result["gaps"])
        self.assertEqual(
            verify_run(self.task / "runs" / "r1")["input_binding"], "mismatched"
        )
        self.assertEqual(
            verify_run(self.task / "runs" / "r2")["completion"], "timeout"
        )

    def test_missing_unknown_evidence_and_reversed_times_are_explicit(self):
        result = self.save_run(
            {
                "artifacts": None,
                "requested_config": {},
                "observed_config": {},
                "input_evidence": ["missing#position"],
                "isolation_evidence": None,
                "interventions": None,
                "exceptions": None,
                "ended_at": "2025-12-31T00:00:00+00:00",
            }
        )
        self.assertEqual(result["classification"], "test_fixture")
        self.assertIn("artifacts: missing or not a list", result["gaps"])
        self.assertTrue(any("earlier" in gap for gap in result["gaps"]))
        self.assertTrue(any("unknown" in gap for gap in result["gaps"]))

    def test_dangerous_missing_and_duplicate_artifacts_keep_other_evidence(self):
        (self.source / "link").symlink_to(self.source / "answer.txt")
        artifacts = self.record["artifacts"] + [
            {"id": "escape", "path": "../outside", "kind": "answer"},
            {"id": "missing", "path": "absent.txt"},
            {"id": "link", "path": "link"},
            {"id": "answer", "path": "answer.txt"},
            {"id": "bad", "path": None},
            "not-an-object",
        ]
        result = self.save_run({"artifacts": artifacts})
        self.assertEqual(len(result["artifacts"]), 2)
        self.assertEqual(len(result["gaps"]), 6)
        self.assertFalse((Path(result["path"]) / "outside").exists())

    def test_invalid_record_cannot_escape_or_publish(self):
        path = self.source / "run.json"
        for raw in (
            b"[]", b"not-json", b'{"run_id":"x", "run_id":"y"}',
            b'{"run_id":"x", "number":NaN}',
            b'{"run_id":"../escape"}',
        ):
            path.write_bytes(raw)
            with self.assertRaises(TaskflowError):
                import_run(self.snapshot, path)
        path.unlink()
        path.symlink_to(self.source / "answer.txt")
        with self.assertRaises(TaskflowError):
            import_run(self.snapshot, path)
        self.assertFalse((self.task / "runs").exists())

    def test_import_cannot_overwrite_and_detects_raw_and_artifact_tampering(self):
        result = self.save_run()
        run = Path(result["path"])
        old = (run / "record.original.json").read_bytes()
        with self.assertRaises(TaskflowError):
            self.save_run({"status": "failed"})
        self.assertEqual((run / "record.original.json").read_bytes(), old)
        artifact = run / result["artifacts"][0]["path"]
        artifact.write_text("changed", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            verify_run(run)
        artifact.write_text("neutral answer", encoding="utf-8")
        (run / "record.original.json").write_bytes(b"{}")
        with self.assertRaises(TaskflowError):
            verify_run(run)

    def test_manifest_tampering_fails(self):
        run = Path(self.save_run()["path"])
        value = json.loads((run / "run.json").read_text())
        value["nature"] = "changed"
        write_json(run / "run.json", value)
        with self.assertRaises(TaskflowError):
            verify_run(run)

    def test_interrupt_and_concurrency_do_not_publish_partial_run(self):
        write_json(self.source / "run.json", self.record)
        with patch("taskflow.records._capture_artifacts", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                import_run(self.snapshot, self.source / "run.json")
        self.assertEqual(list((self.task / "runs").iterdir()), [])

        def attempt(_):
            try:
                import_run(self.snapshot, self.source / "run.json")
                return True
            except TaskflowError:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            successes = list(pool.map(attempt, range(2)))
        self.assertEqual(sum(successes), 1)
        self.assertEqual(len(verify_run(self.task / "runs" / "neutral-run")["artifacts"]), 2)

    def test_snapshot_change_during_import_does_not_publish(self):
        write_json(self.source / "run.json", self.record)
        changed = {**self.reference, "snapshot_digest": "changed-during-import"}
        with patch("taskflow.records.verify_snapshot", side_effect=[self.reference, changed]):
            with self.assertRaises(TaskflowError):
                import_run(self.snapshot, self.source / "run.json")
        self.assertEqual(list((self.task / "runs").iterdir()), [])
        run = Path(self.save_run()["path"])
        with patch("taskflow.records.verify_snapshot", side_effect=[self.reference, changed]):
            with self.assertRaises(TaskflowError):
                import_evaluation(run, self.evaluation_record())
        self.assertEqual(list((run / "evaluations").iterdir()), [])

    def test_original_correction_and_new_standard_keep_distinct_judgments(self):
        run = Path(self.save_run({"nature": "test_fixture"})["path"])
        original = import_evaluation(run, self.evaluation_record())
        original_bytes = (Path(original["path"]) / "record.original.json").read_bytes()
        correction = import_evaluation(
            run,
            self.evaluation_record(
                evaluation_id="e2", kind="correction", parent_evaluation_id="e1"
            ),
        )
        self.assertEqual(correction["validation_issues"], [])
        self.assertEqual(original["nature"], "test_fixture")
        self.reference["evaluation_digest"] = "new-rubric"
        self.reference["input_digest"] = "new-input"
        revised = import_evaluation(
            run,
            self.evaluation_record(
                evaluation_id="e3", kind="rejudgment", parent_evaluation_id="e1",
                input_digest="public-input-digest",
            ),
        )
        self.assertEqual(revised["classification"], "exploratory")
        self.assertTrue(any("different public input" in x for x in revised["validation_issues"]))
        self.assertEqual(
            (Path(original["path"]) / "record.original.json").read_bytes(), original_bytes
        )
        self.assertEqual(verify_evaluation(Path(original["path"]))["kind"], "original")
        with self.assertRaises(TaskflowError):
            import_evaluation(run, self.evaluation_record())

    def test_evaluation_issues_are_saved_instead_of_valid_scores(self):
        run = Path(self.save_run()["path"])
        items = [
            {"id": "c1", "score": 99, "evidence": ["unseen#x"]},
            {"id": "c1", "score": 2},
            {"id": [], "score": 0},
            "invalid",
        ]
        result = import_evaluation(
            run,
            self.evaluation_record(
                items=items, total=99, evaluator=None, review_status=None,
                input_digest="wrong",
            ),
        )
        self.assertGreater(len(result["validation_issues"]), 6)
        self.assertTrue(Path(result["path"]).is_dir())
        with self.assertRaises(TaskflowError):
            import_evaluation(run, self.evaluation_record(kind="other"))

    def test_huge_evaluation_total_is_archived_with_validation_issue(self):
        run = Path(self.save_run()["path"])
        source = self.evaluation_record(total=10**400)
        original = source.read_bytes()
        result = import_evaluation(run, source)
        self.assertTrue(any("total:" in issue for issue in result["validation_issues"]))
        self.assertEqual((Path(result["path"]) / "record.original.json").read_bytes(), original)
        self.assertEqual(read_json(Path(result["path"]) / "record.original.json")["total"], 10**400)

    def test_concurrent_original_assessments_are_serialized_without_overwrite(self):
        run = Path(self.save_run()["path"])
        paths = []
        for evaluation_id in ("e1", "e2"):
            source = self.evaluation_record(evaluation_id=evaluation_id)
            target = self.source / f"{evaluation_id}.json"
            target.write_bytes(source.read_bytes())
            paths.append(target)
        barrier = Barrier(2)

        def import_together(source):
            barrier.wait(timeout=5)
            return import_evaluation(run, source)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(import_together, paths))
        clean = [result for result in results if not result["validation_issues"]]
        duplicate = [result for result in results if result["validation_issues"]]
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(duplicate), 1)
        self.assertTrue(any("already archived" in x for x in duplicate[0]["validation_issues"]))
        self.assertEqual(len(list((run / "evaluations").iterdir())), 2)
        self.assertTrue((run / ".evaluation-sequence").is_file())
        with patch("taskflow.records.check_task", return_value={"status": "passed"}):
            overview = status(self.task)
        self.assertEqual(overview["interrupted_operations"], [])
        self.assertEqual(verify_run(run)["nature"], "test_fixture")

    def test_evaluation_sequence_rejects_symlink_and_releases_after_interrupt(self):
        run = Path(self.save_run()["path"])
        source = self.evaluation_record()
        outside = self.source / "outside.txt"
        outside.write_bytes(b"unchanged")
        lock = run / ".evaluation-sequence"
        lock.symlink_to(outside)
        with self.assertRaises(TaskflowError):
            import_evaluation(run, source)
        self.assertEqual(outside.read_bytes(), b"unchanged")
        lock.unlink()
        with patch("taskflow.records._import_evaluation_locked", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                import_evaluation(run, source)
        self.assertTrue(lock.is_file())
        result = import_evaluation(run, source)
        self.assertEqual(result["validation_issues"], [])

    def test_different_standards_and_missing_parent_are_explicit(self):
        run = Path(self.save_run()["path"])
        import_evaluation(run, self.evaluation_record())
        duplicate = import_evaluation(run, self.evaluation_record(evaluation_id="e2"))
        self.assertTrue(any("already archived" in x for x in duplicate["validation_issues"]))
        unchanged = import_evaluation(
            run, self.evaluation_record(
                evaluation_id="e3", kind="rejudgment", parent_evaluation_id="e1"
            )
        )
        self.assertTrue(any("unchanged" in x for x in unchanged["validation_issues"]))
        missing = import_evaluation(
            run, self.evaluation_record(
                evaluation_id="e4", kind="correction", parent_evaluation_id="absent"
            )
        )
        self.assertTrue(any("parent_evaluation_id" in x for x in missing["validation_issues"]))
        self.reference["evaluation_digest"] = "new-rubric"
        changed = import_evaluation(
            run, self.evaluation_record(
                evaluation_id="e5", kind="correction", parent_evaluation_id="e1"
            )
        )
        self.assertTrue(any("rubric changed" in x for x in changed["validation_issues"]))
        self.reference["scoring"] = {
            "total": 100,
            "criteria": [{"id": "c1", "max_score": 40}, {"id": "c2", "max_score": 60}],
        }
        new_standard = import_evaluation(
            run, self.evaluation_record(
                evaluation_id="e6", kind="rejudgment", parent_evaluation_id="e1",
                items=[
                    {"id": "c1", "score": 30, "evidence": ["answer#first"]},
                    {"id": "c2", "score": 60, "evidence": ["answer#second"]},
                ],
                total=90,
            )
        )
        self.assertEqual(new_standard["validation_issues"], [])
        self.assertEqual(new_standard["classification"], "recorded")
        saved = read_json(Path(new_standard["path"]) / "record.original.json")
        self.assertEqual(saved["total"], 90)
        self.assertEqual(read_json(run / "evaluations/e1/record.original.json")["total"], 26)

    def test_evaluation_tampering_and_status_report_evidence_gaps(self):
        run = Path(self.save_run({"nature": "test_fixture"})["path"])
        evaluation = import_evaluation(run, self.evaluation_record())
        (Path(evaluation["path"]) / "record.original.json").write_bytes(b"{}")
        with self.assertRaises(TaskflowError):
            verify_evaluation(Path(evaluation["path"]))
        with patch("taskflow.records.check_task", return_value={"status": "passed"}):
            result = status(self.task)
        self.assertEqual(result["versions"][0]["integrity"], "verified")
        self.assertEqual(result["runs"][0]["nature"], "test_fixture")
        self.assertEqual(result["runs"][0]["evaluations"][0]["integrity"], "failed")
        self.assertIn("unknown", result["platform_status"])
        (run / "record.original.json").write_bytes(b"{}")
        with patch("taskflow.records.check_task", return_value={"status": "failed"}):
            with patch("taskflow.records.verify_snapshot", side_effect=TaskflowError("changed")):
                result = status(self.task)
        self.assertEqual(result["versions"][0]["integrity"], "failed")
        self.assertEqual(result["runs"][0]["integrity"], "failed")

    def test_status_marks_check_freshness_and_possible_interruption_without_deleting(self):
        reports = self.task / "reports"
        for name, digest in (("check-a", "current"), ("check-b", "old"), ("check-c", None)):
            write_json(reports / name / "report.json", {"content_digest": digest})
        write_json(reports / "check-d" / "report.json", {})
        (reports / "check-d" / "report.json").write_bytes(b"invalid-json")
        lock = self.task / "versions" / ".v2.lock"
        lock.write_text("test fixture lock", encoding="utf-8")
        staging = self.task / "runs" / ".future.staging-fixture"
        staging.mkdir(parents=True)
        with patch("taskflow.records.check_task", return_value={"content_digest": "current"}):
            result = status(self.task)
        self.assertEqual(
            [item["state"] for item in result["check_history"]],
            ["current", "stale", "invalid", "invalid"],
        )
        self.assertEqual(len(result["interrupted_operations"]), 2)
        self.assertTrue(lock.exists())
        self.assertTrue(staging.exists())
        with patch("taskflow.records.check_task", return_value={"status": "failed"}):
            result = status(self.task)
        self.assertEqual(result["check_history"][0]["state"], "unknown_current_input")

    def test_status_distinguishes_old_run_from_new_public_input_and_rejudgment(self):
        self.verify_patch.stop()
        task = make_task(self.root, "version-boundaries")
        add_reviews(task)
        first = freeze_task(task, "v1")
        self.snapshot = Path(first["path"])
        self.reference = first
        run = Path(self.save_run({"actual_input_digest": first["input_digest"]})["path"])
        items = [{"id": "C1", "score": 7, "evidence": ["answer#result"]}]
        import_evaluation(run, self.evaluation_record(items=items, total=7))

        (task / "draft/input/prompt.md").write_text(
            "Choose 1 unit from capacity 3.\n", encoding="utf-8"
        )
        (task / "draft/evaluation/rubric.md").write_text(
            "C1: 7 points for the revised public requirement.\n", encoding="utf-8"
        )
        add_reviews(task)
        second = freeze_task(task, "v2")
        import_evaluation(
            run,
            self.evaluation_record(
                evaluation_id="e2", kind="rejudgment", parent_evaluation_id="e1",
                rubric_snapshot=second["path"], items=items, total=7,
            ),
        )
        self.save_run({"run_id": "unknown-input", "actual_input_digest": None})
        overview = status(task)
        versions = {item["version_id"]: item for item in overview["versions"]}
        observed = {item["run_id"]: item for item in overview["runs"]}
        original = observed["neutral-run"]
        self.assertEqual(original["version_id"], "v1")
        self.assertEqual(original["input_binding"], "matched")
        self.assertEqual(original["completion"], "completed")
        self.assertEqual(original["snapshot_digest"], versions["v1"]["snapshot_digest"])
        self.assertEqual(original["actual_input_digest"], versions["v1"]["input_digest"])
        self.assertNotEqual(original["actual_input_digest"], versions["v2"]["input_digest"])
        self.assertEqual(original["model_validity"], "not_determined")
        self.assertEqual(original["nature"], "test_fixture")
        evaluations = {item["evaluation_id"]: item for item in original["evaluations"]}
        self.assertEqual(evaluations["e1"]["rubric_version_id"], "v1")
        revised = evaluations["e2"]
        self.assertEqual(revised["parent_evaluation_id"], "e1")
        self.assertEqual(revised["rubric_version_id"], "v2")
        self.assertEqual(revised["classification"], "exploratory")
        self.assertEqual(revised["evaluation_digest"], versions["v2"]["evaluation_digest"])
        self.assertEqual(revised["rubric_input_digest"], versions["v2"]["input_digest"])
        self.assertEqual(revised["actual_input_digest"], versions["v1"]["input_digest"])
        self.assertIsNone(observed["unknown-input"]["actual_input_digest"])
        self.assertEqual(observed["unknown-input"]["input_binding"], "unknown")
        self.assertEqual(observed["unknown-input"]["classification"], "test_fixture")

    def test_status_preserves_history_when_current_manifest_cannot_be_read(self):
        from taskflow.__main__ import main

        self.verify_patch.stop()
        task = make_task(self.root, "damaged-draft")
        add_reviews(task)
        first = freeze_task(task, "v1")
        self.snapshot = Path(first["path"])
        self.reference = first
        self.save_run({"actual_input_digest": first["input_digest"]})
        check_task(task)
        manifest = task / "task.json"
        manifest.write_bytes(b"{")
        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["status", str(task)])
        self.assertEqual(code, 0)
        overview = json.loads(output.getvalue())
        self.assertEqual(overview["check"]["status"], "failed")
        self.assertIn("task.json", overview["check"]["errors"][0])
        self.assertNotIn("content_digest", overview["check"])
        self.assertEqual(overview["check_history"][0]["state"], "unknown_current_input")
        self.assertEqual(overview["versions"][0]["integrity"], "verified")
        self.assertEqual(overview["runs"][0]["integrity"], "verified")
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            self.assertEqual(main(["check", str(task), "--no-write"]), 2)

        manifest.unlink()
        manifest.symlink_to(self.snapshot / "task.json")
        blocked = status(task)
        self.assertEqual(blocked["check"]["status"], "failed")
        self.assertIn("symlink", blocked["check"]["errors"][0].lower())
        self.assertEqual(blocked["versions"][0]["integrity"], "verified")

    def test_real_snapshot_round_trip_records_and_checks_original_integrity(self):
        self.verify_patch.stop()
        task = self.root / "real-task"
        init_task(task, "round-trip")
        manifest = read_json(task / "task.json")
        manifest["title"] = "Neutral local fixture"
        manifest["project"] = {"name": "Fixture", "phase": None, "rule_sources": ["local"]}
        manifest["scoring"] = {
            "total": 28,
            "rubric_file": "rubric",
            "criteria": [
                {
                    "id": criterion_id,
                    "dimension": "fixture",
                    "max_score": maximum,
                    "condition": "explicit neutral result",
                    "allowed": "neutral alternative",
                    "tolerance": "exact",
                    "evidence": ["prompt"],
                }
                for criterion_id, maximum in (("c1", 12), ("c2", 16))
            ],
        }
        write_json(task / "task.json", manifest)
        checked = check_task(task, write_report=False)
        self.assertEqual(checked["status"], "passed")
        for review in manifest["reviews"]:
            write_json(
                task / review["path"],
                {
                    "schema_version": 1,
                    "review_id": review["id"],
                    "kind": review["kind"],
                    "status": "passed",
                    "scope": "neutral fixture bytes only; no business conclusion",
                    "limitations": ["test fixture"],
                    "evidence": ["prompt"],
                    "content_digest": checked["content_digest"],
                    "input_digest": checked["input_digest"],
                },
            )
        frozen = freeze_task(task, "v1")
        self.reference = frozen
        self.snapshot = Path(frozen["path"])
        result = self.save_run({"actual_input_digest": frozen["input_digest"]})
        self.assertEqual(result["nature"], "test_fixture")
        self.assertEqual(result["input_binding"], "matched")
        self.assertEqual(result["gaps"], [])
        evaluation = import_evaluation(Path(result["path"]), self.evaluation_record())
        self.assertEqual(evaluation["validation_issues"], [])
        overview = status(task)
        self.assertEqual(overview["runs"][0]["integrity"], "verified")
        self.assertEqual(overview["versions"][0]["integrity"], "verified")
        (self.snapshot / "draft/input/prompt.md").write_text("changed", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            self.save_run({"run_id": "cannot-publish"})
        self.assertFalse((task / "runs" / "cannot-publish").exists())


if __name__ == "__main__":
    unittest.main()
