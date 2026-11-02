"""Markdown keeps current registered flows separate from older observations."""

import copy
import unittest

from taskflow.execution import dashboard_markdown


class ExecutionDashboardTests(unittest.TestCase):
    def flow(self, identity, task, kind, state, revision=3):
        return {
            "flow_id": identity, "revision": revision, "event_seq": 40 + revision,
            "definition": {"task_id": task, "kind": kind, "owner": "controller"},
            "state": state,
        }

    def row(self, text, identity):
        return next(line for line in text.splitlines() if line.startswith("| ") and identity in line)

    def test_current_work_uses_actual_attempt_instead_of_older_formal_snapshot(self):
        flow = self.flow("current-flow", "topic-alpha", "experiment_v1", {
            "status": "active", "stage": "facts_wait", "current_work_id": "facts-work",
            "formal_attempt": {"attempt_id": "old-solver", "state": "running"},
        }, revision=6)
        board = {"flows": [flow], "work": [{"work_id": "facts-work", "state": "running"}],
                 "attempts": [
                     {"attempt_id": "old-solver", "work_id": "solver-work", "state": "completed"},
                     {"attempt_id": "facts-attempt", "work_id": "facts-work", "state": "running",
                      "cli_sid": "facts-sid", "response_complete": False, "artifacts_complete": False,
                      "technical_accepted": False},
                 ]}
        before = copy.deepcopy(board)
        row = self.row(dashboard_markdown(board), "current-flow")
        for value in ("topic-alpha", "experiment_v1", "rev=6", "event=46", "active / facts_wait",
                      "current work=facts-work", "attempt=facts-attempt", "state=running", "sid=facts-sid",
                      "observe and collect existing attempt", "receipt=not recorded"):
            self.assertIn(value, row)
        self.assertNotIn("old-solver", row)
        self.assertEqual(before, board)

    def test_research_ended_keeps_original_author_and_requires_controller_decision(self):
        flow = self.flow("ended-flow", "topic-beta", "author_quality_v1", {
            "status": "accepted", "stage": "author_handoff", "current_work_id": None,
            "receipt": "/fixture/ended-result.json", "result": {"outcome": "research_ended"},
        })
        flow["definition"]["source_author_attempt"] = "source-author"
        row = self.row(dashboard_markdown({"flows": [flow], "attempts": [{
            "attempt_id": "source-author", "work_id": "author-work", "state": "completed",
            "cli_sid": "author-sid", "technical_accepted": True,
        }]}), "ended-flow")
        for value in ("accepted / author_handoff", "no current work", "source author attempt=source-author",
                      "state=completed", "outcome=research_ended", "/fixture/ended-result.json",
                      "controller: business decision for further work"):
            self.assertIn(value, row)
        self.assertNotIn("observe and collect", row)

    def test_exception_and_multiple_topics_remain_visible_beside_legacy_source(self):
        running = self.flow("running-flow", "topic-alpha", "experiment_v1", {
            "status": "active", "stage": "formal_wait", "current_work_id": "solver-work",
        }, revision=4)
        ended = self.flow("ended-flow", "topic-beta", "author_quality_v1", {
            "status": "accepted", "stage": "author_handoff", "result": {"outcome": "research_ended"},
            "receipt": "/fixture/ended.json",
        })
        stopped = self.flow("stopped-flow", "topic-gamma", None, {
            "status": "exception", "stage": "quality", "current_runtime": "/fixture/quality-runtime",
            "exception": {"reason": "specific | collection mismatch\nretain original"},
            "receipt": "/fixture/exception.json",
        }, revision=7)
        stopped["definition"]["roles"] = {"quality": {}}
        board = {"flows": [running, ended, stopped], "work": [{"work_id": "solver-work", "state": "running"}],
                 "attempts": [{"attempt_id": "failed-quality", "work_id": "quality-work", "state": "failed",
                               "runtime_path": "/fixture/quality-runtime"}],
                 "tasks": [{"task_id": "topic-alpha", "owner": "original-research-owner", "mode": "observed",
                            "current_state": "old-running",
                            "observation": {"observed_status": "old-running", "observed_at": "2026-10-01T10:00:00Z",
                                            "source": {"path": "/fixture/old-observation.json", "sha256": "a" * 64}}}]}
        text = dashboard_markdown(board)
        self.assertLess(text.index("Current registered flows"), text.index("Task observations and managed scopes"))
        self.assertIn("active / formal_wait", self.row(text, "running-flow"))
        self.assertIn("outcome=research_ended", self.row(text, "ended-flow"))
        row = self.row(text, "stopped-flow")
        for value in ("topic-gamma", "quality workflow (kind undeclared)", "rev=7", "exception / quality",
                      "attempt=failed-quality", "state=failed", "resolve recorded exception",
                      "specific \\| collection mismatch retain original", "/fixture/exception.json"):
            self.assertIn(value, row)
        legacy = self.row(text, "original-research-owner")
        for value in ("old-running", "2026-10-01T10:00:00Z", "/fixture/old-observation.json", "a" * 64):
            self.assertIn(value, legacy)

    def test_uncertain_and_undispatched_work_do_not_imply_a_new_attempt(self):
        flow = self.flow("waiting-flow", "topic-delta", "experiment_v1", {
            "status": "active", "stage": "formal_wait", "current_work_id": "one-work",
        })
        board = {"flows": [flow], "work": [{"work_id": "one-work", "state": "ready"}]}
        row = self.row(dashboard_markdown(board), "waiting-flow")
        self.assertIn("queue=ready", row)
        self.assertIn("no attempt recorded", row)
        self.assertIn("follow registered work state", row)
        board["attempts"] = [{"attempt_id": "same-attempt", "work_id": "one-work", "state": "uncertain"}]
        row = self.row(dashboard_markdown(board), "waiting-flow")
        self.assertIn("attempt=same-attempt", row)
        self.assertIn("reconcile existing attempt; no relaunch", row)


if __name__ == "__main__":
    unittest.main()
