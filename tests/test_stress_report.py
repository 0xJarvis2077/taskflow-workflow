"""Derived teacher reports preserve originals and expose evidence gaps."""

import copy
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError
from taskflow.stress_report import assemble_report, build_report, render_markdown, write_report
from tests.test_stress_report_sources import SourceCase, info
from tools.stress_report import main


def measured():
    return {"completion": {"status": "completed", "timed_out": False}, "nature": "real_native",
        "requested_model": "neutral-model", "effort": "low", "cli_version": "neutral-cli",
        "timing": {"gross_seconds": 430},
        "counts": {"agent_messages": 13, "command_executions": 103, "tool_calls": 103, "file_changes": 12},
        "reconnections": 1, "limitations": [], "evidence": []}


class StressReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.case = SourceCase(self.root)
        self.addCleanup(self.case.connection.close)
        self.case.review["definition"]["criteria"] = [{"id": "criterion", "max_points": 10}]
        self.case.save(self.case.review)
        self.sources = self.case.read()
        self.sources["assessment"]["score"]["validity"]["local_eligible"] = True

    def rules(self):
        source = self.root / "selected-rules.md"
        source.write_text("Explicit neutral comparison convention.")
        return {"source": {"path": str(source), "sha256": info(source)["sha256"], "location": "L1"},
                "score_max": 8, "score_denominator": 10}

    def test_report_lists_original_scores_measurements_and_unknowns_without_mutation(self):
        original = copy.deepcopy(self.sources)
        report = assemble_report(self.sources, measured())
        self.assertEqual(7, report["evaluation"]["total"])
        self.assertEqual(10, report["evaluation"]["maximum"])
        self.assertEqual("not_configured", report["thresholds"]["status"])
        self.assertNotIn("m", report)
        self.assertEqual(1, report["stability"]["sample_count"])
        text = render_markdown(report)
        for value in ("7 / 10", "criterion", "103", "13", "教师访问／公开预填", "原评阅事实"):
            self.assertIn(value, text)
        self.assertEqual(original, self.sources)
        combined = json.dumps(report, ensure_ascii=False) + text
        for removed in ("effective_seconds", "effective_time", "step_count", "official_steps", "正式执行步数", "有效任务时间", "M："):
            self.assertNotIn(removed, combined)

    def test_no_review_or_unaccepted_score_never_becomes_zero(self):
        for review in (None, {**self.sources["assessment"], "score": None}):
            data = {**self.sources, "assessment": review}
            report = assemble_report(data, measured())
            self.assertIsNone(report["evaluation"]["total"])
            self.assertIn("尚无已接收原分", render_markdown(report))

    def test_inconclusive_score_and_original_nonhundred_denominator_survive(self):
        self.sources["assessment"]["score"].update(outcome="inconclusive", total=None)
        report = assemble_report(self.sources, measured())
        self.assertEqual("inconclusive", report["evaluation"]["status"])
        self.assertEqual(10, report["evaluation"]["maximum"])
        self.assertIsNone(report["evaluation"]["total"])

    def test_rules_compare_original_score_without_removed_runtime_metrics(self):
        rules = self.rules()
        report = assemble_report(self.sources, measured(), rules=rules)
        self.assertEqual("score_at_or_below_limit", report["thresholds"]["status"])
        rules["score_max"] = 6
        self.assertEqual("score_above_limit", assemble_report(self.sources, measured(), rules=rules)["thresholds"]["status"])
        rules["score_denominator"] = 100
        self.assertIsNone(assemble_report(self.sources, measured(), rules=rules)["thresholds"]["score"])

    def test_rules_require_actual_source_and_do_not_claim_fixture_eligibility(self):
        rules = self.rules()
        for key, value in (("score_max", True), ("score_max", float("inf")), ("score_denominator", 0), ("step_min", 50)):
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                assemble_report(self.sources, measured(), rules={**rules, key: value})
        with self.assertRaises(TaskflowError):
            assemble_report(self.sources, measured(), rules={})
        rules["source"]["sha256"] = "wrong"
        with self.assertRaises(TaskflowError):
            assemble_report(self.sources, measured(), rules=rules)
        rules = self.rules(); observed = measured(); observed["nature"] = "test_fixture"
        self.assertEqual("unknown", assemble_report(self.sources, observed, rules=rules)["thresholds"]["status"])

    def test_stability_excludes_different_versions_invalid_runs_and_duplicate_originals(self):
        report = assemble_report(self.sources, measured())
        peer = copy.deepcopy(self.sources); peer["solver"]["record"]["flow_id"] = "another-original"
        grade = copy.deepcopy(report["evaluation"]); grade["total"] = 9
        result = assemble_report(self.sources, measured(), peers=[(peer, measured(), grade)])
        self.assertEqual(2, result["stability"]["sample_count"])
        self.assertEqual(8, result["stability"]["mean"])
        self.assertIn("平均值", render_markdown(result))
        peer["solver"]["manifest"]["candidate_binding"]["content_digest"] = "new-version"
        result = assemble_report(self.sources, measured(), peers=[(peer, measured(), grade)])
        self.assertEqual(1, result["stability"]["sample_count"])
        self.assertEqual(1, len(result["stability"]["excluded"]))
        self.assertIn("未混算", render_markdown(result))
        with self.assertRaises(TaskflowError):
            assemble_report(self.sources, measured(), peers=[(self.sources, measured(), grade)])

    def test_stability_keeps_reviewer_tools_and_operation_changes_separate(self):
        grade = assemble_report(self.sources, measured())["evaluation"]
        for role, field in (("assessment", "facts_runner"), ("assessment", "score_runner"), ("solver", "runner")):
            with self.subTest(role=role, field=field):
                peer = copy.deepcopy(self.sources)
                peer["solver"]["record"]["flow_id"] = "peer"
                peer[role]["record"]["definition"][field] = {"model": "other-reviewer", "request_sections": [{"text": "changed operation"}]}
                result = assemble_report(self.sources, measured(), peers=[(peer, measured(), grade)])
                self.assertEqual(1, result["stability"]["sample_count"])
                self.assertEqual(1, len(result["stability"]["excluded"]))

    def test_write_is_idempotent_and_does_not_replace_corrupted_or_symlinked_snapshots(self):
        report = assemble_report(self.sources, measured()); out = self.root / "reports"
        first = write_report(out, report)
        changed_time = {**report, "generated_at": "later"}
        self.assertEqual(first, write_report(out, changed_time))
        self.assertNotIn("later", Path(first["json"]).read_text())
        Path(first["markdown"]).write_text("changed")
        with self.assertRaises(TaskflowError):write_report(out, report)
        link = self.root / "linked"; link.symlink_to(out)
        with self.assertRaises(TaskflowError):write_report(link, report)

    def test_output_cannot_contaminate_public_runtime_archives_or_original_membership(self):
        report = assemble_report(self.sources, measured())
        for protected in report["protected_source_roots"]:
            path = Path(protected)
            before = sorted(str(p) for p in path.rglob("*")) if path.exists() else []
            with self.subTest(path=protected), self.assertRaisesRegex(TaskflowError, "cannot enter original"):
                write_report(path, report)
            self.assertEqual(before, sorted(str(p) for p in path.rglob("*")) if path.exists() else [])

    def test_measurement_snapshot_change_is_not_mixed_with_authenticated_original(self):
        observation = measured()
        source = self.sources["references"][0]
        observation["evidence"] = [{**source, "sha256": "changed"}]
        with patch("taskflow.stress_report.runtime_metrics", return_value=observation):
            with self.assertRaisesRegex(TaskflowError, "changed between"):
                build_report(self.case.database, "solver")

    def test_real_readonly_build_and_cli_report_do_not_change_source_bytes(self):
        before = {str(p):p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        report = build_report(self.case.database, "solver")
        self.assertEqual(7, report["evaluation"]["total"])
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["--db", str(self.case.database), "--solver", "solver", "--output", str(self.root / "reports")])
        self.assertEqual(0, code)
        self.assertTrue(Path(json.loads(output.getvalue())["markdown"]).is_file())
        self.assertTrue(all(Path(p).read_bytes()==raw for p,raw in before.items()))
        with redirect_stderr(io.StringIO()):
            self.assertEqual(2, main(["--db", str(self.root / "missing.sqlite3"), "--solver", "solver", "--output", str(self.root / "reports")]))

    def test_auto_report_is_derived_and_cannot_turn_report_failure_into_score_failure(self):
        from taskflow.execution_assessment import _with_report
        store = type("ReadOnlyCase", (), {"path": self.case.database})()
        record = self.case.read()["assessment"]["record"]
        original = copy.deepcopy(record)
        result = _with_report(store, record, "score")
        self.assertEqual("available", result["report"]["status"])
        self.assertEqual(original, record)
        self.assertEqual(record, _with_report(store, record, "facts"))
        with patch("taskflow.stress_report.build_report", side_effect=TaskflowError("source gap")):
            result = _with_report(store, record, "score")
        self.assertEqual("unavailable", result["report"]["status"])
        self.assertEqual(original["state"], result["state"])

    def test_solver_reports_without_starting_review_and_report_errors_stay_derived(self):
        from taskflow.execution_solver import _with_report
        store = type("ReadOnlyCase", (), {"path": self.case.database})()
        record = self.case.read()["solver"]["record"]
        original = copy.deepcopy(record)
        self.assertEqual("available", _with_report(store, record)["report"]["status"])
        with patch("taskflow.stress_report.build_report", side_effect=RuntimeError("presentation problem")):
            failed = _with_report(store, record)
        self.assertEqual("unavailable", failed["report"]["status"])
        self.assertEqual(original["state"], failed["state"])
        record["state"]["execution"] = "running"
        self.assertEqual(record, _with_report(store, record))


if __name__ == "__main__":
    unittest.main()
