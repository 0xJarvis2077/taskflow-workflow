"""Offline rejected-signature continuation; all subprocess roles are fixtures."""

from pathlib import Path
import json
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, sha256, write_json
from taskflow.execution_quality_continuation import (
    continue_rejected_quality_workflow, inspect_rejected_quality_workflow,
)
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flow_tests


class RejectedQualityContinuationTests(unittest.TestCase):
    def setUp(self):
        self.helper = flow_tests.ContinuationTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.store, self.root = self.helper.store, self.helper.root
        self.definition = self.helper.definition(outcome="revision_required")
        self.definition["roles"]["quality"]["runner"]["public_first_initial_listing"] = {
            "scope": "whole_package", "index": "execution/public-inputs.json",
            "checkpoint": "public-responsibility-first.json"}
        self.observed = self.root / "independent-submission.md"
        self.observed.write_text("Neutral independent answer, not a candidate file.\n")
        self.aliases = {"actual-public-work/submission.md": {
            "kind": "submission", "path": str(self.observed), "sha256": sha256(self.observed.read_bytes())}}
        code = self.helper.script.read_text().replace(
            "name='draft/evaluation/answer.md';blocks=['PRIVATE-ONE'];defects=",
            "name='draft/evaluation/answer.md';blocks=['PRIVATE-ONE'];defects=")
        code = code.replace("route={'schema_version':1", "if a.outcome=='revision_required':\n  defects[0]['kind']='claim';defects[0]['affected_paths']=['actual-public-work/submission.md']\n route={'schema_version':1")
        code = code.replace("sid=str(uuid.uuid4())", "\nif '/nodes/1-quality/' in a.context:a.outcome='accepted'\nsid=str(uuid.uuid4())")
        self.helper.script.write_text(code)
        self.helper.register(self.definition)
        self.flow_id = self.definition["flow_id"]
        with patch("taskflow.execution_flow._review_observations", return_value=self.aliases):
            self.helper.run_flow()
        self.original = self.store.get_flow(self.flow_id)
        self.assertEqual("exception", self.original["state"]["status"])
        self.patch = patch("taskflow.execution_quality_continuation._review_observations", return_value=self.aliases)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.source = inspect_rejected_quality_workflow(self.store, self.flow_id)
        self.packet = {"schema_version": 1, "kind": "continue_rejected_quality_v1", "flow_id": self.flow_id,
                       "controller": self.definition["owner"], "scope": "whole_package",
                       "purpose": "Independently inspect current materials and rejected original.",
                       "source": self.source, "direct_user_authority": self.helper.refs[0]}
        self.packet_path = self.root / "authorization-packet.json"

    def authorize(self, packet=None):
        write_json(self.packet_path, packet or self.packet)
        return continue_rejected_quality_workflow(self.store, self.flow_id,
            authorization=evidence_ref(self.packet_path, "neutral fixture authorization"))

    def test_single_registration_keeps_old_signature_and_requires_new_review(self):
        original_receipt = Path(self.original["state"]["receipt"]).read_bytes()
        original_report = Path(self.source["evidence"][0]["path"]).read_bytes()
        with patch("subprocess.Popen", side_effect=AssertionError("registration must not launch")):
            result = self.authorize()
            self.assertEqual(result, self.authorize())
        self.assertEqual("active", result["state"]["status"])
        with self.assertRaises(TaskflowError):
            self.store.quality_gate(self.source["candidate_binding"]["candidate_id"])
        with patch("taskflow.execution_flow._review_observations", return_value=self.aliases):
            self.helper.run_flow()
        after = self.store.get_flow(self.flow_id)
        self.assertEqual("accepted", after["state"]["status"])
        review = self.store.get_review(after["state"]["accepted_review_id"])
        attempt = self.store.get_attempt(review["attempt_id"])
        self.assertNotEqual(self.source["cli_sid"], attempt["cli_sid"])
        self.assertEqual(original_receipt, Path(self.original["state"]["receipt"]).read_bytes())
        self.assertEqual(original_report, Path(self.source["evidence"][0]["path"]).read_bytes())

    def test_request_keeps_original_report_behind_public_first_checkpoint(self):
        from taskflow.execution_flow import advance_workflow

        self.authorize()
        prepared = advance_workflow(self.store, self.flow_id)
        runtime = Path(prepared["state"]["current_runtime"])
        request = (runtime / "request.md").read_text()
        self.assertIn("PUBLIC_RESPONSIBILITY_FIRST_SHA256=<实际SHA256>", request)
        self.assertNotIn("a rejected signed quality report", request)
        later = json.loads((runtime / "quality-later-inputs.v1.json").read_text())
        operations = "\n".join(item["text"] for item in later["request_sections"])
        self.assertIn("a rejected signed quality report", operations)
        self.assertLess(operations.index("complete the public-first checkpoint"),
                        operations.index('current["quality_continuation"]["purpose"]'))
        self.assertIn("print its actual SHA256 anchor", operations)
        self.assertIn("Do not infer acceptance", operations)
        self.assertEqual(Path(self.source["evidence"][0]["path"]).read_bytes(),
                         (runtime / "inputs/execution/previous-review-0.json").read_bytes())
        self.assertEqual(Path(self.source["evidence"][5]["path"]).read_bytes(),
                         (runtime / "inputs/execution/previous-review-2.md").read_bytes())
        self.assertEqual([], self.store.list_attempts(prepared["state"]["current_work_id"]))

    def test_parent_receives_only_new_accepted_signature(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_same_original_sid_cannot_sign_successor(self):
        from taskflow.execution_flow import _consume_review

        self.authorize()
        record = self.store.get_flow(self.flow_id)
        with self.assertRaisesRegex(TaskflowError, "reused"):
            _consume_review(self.store, record, {"cli_sid": self.source["cli_sid"]}, {})

    def test_no_recursive_continuation_of_same_source(self):
        self.authorize()
        altered = {**self.packet, "purpose": "An unauthorized second attempt."}
        with self.assertRaises(TaskflowError):
            self.authorize(altered)

    def test_prior_other_continuation_and_noninitial_step_are_ineligible(self):
        from taskflow.execution_quality_continuation import _rejected_original

        for state_change in ({"step": 1}, {"defects": [{"id": "prior"}]},
                             {"repaired_defects": ["prior"]},
                             {"history": [{"kind": "workflow_inconclusive_continued"}]},
                             {"history": [{"kind": "workflow_rejected_quality_continued"}]}):
            with self.subTest(state_change=state_change), self.assertRaises(TaskflowError):
                _rejected_original(self.store, {**self.original,
                    "state": {**self.original["state"], **state_change}})

    def test_changed_source_scope_or_authority_does_not_write(self):
        for changes in ({"scope": "affected"}, {"controller": "other"}, {"purpose": ""},
                        {"source": {**self.source, "attempt_id": "foreign"}}):
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                self.authorize({**self.packet, **changes})
            self.assertEqual(self.original, self.store.get_flow(self.flow_id))

    def test_original_changed_or_foreign_exception_fails_closed(self):
        report = Path(self.source["evidence"][0]["path"])
        report.write_text(report.read_text() + "\n")
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual(self.original, self.store.get_flow(self.flow_id))


if __name__ == "__main__":
    unittest.main()
