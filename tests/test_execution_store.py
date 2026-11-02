"""State gates use neutral evidence and actual independent SQLite contenders."""

import concurrent.futures
import json
import subprocess
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_store import ExecutionStore, evidence_ref, validate_evidence


class ExecutionStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite")
        self.counter = 0
        self.policy = {
            "concurrency": 6,
            "roles": {
                role: {"model": "fixture-model", "reasoning_effort": "high", "timeout_seconds": 20}
                for role in ("author", "repair", "quality", "solver", "facts", "score", "research")
            },
        }
        self.store.register_policy("policy-1", self.policy)
        self.candidate("item-a", "candidate-a")

    def refs(self, text="attributed evidence"):
        self.counter += 1
        path = self.root / f"source-{self.counter}.txt"
        path.write_text(text, encoding="utf-8")
        return [evidence_ref(path, "whole file")]

    def candidate(self, task, identity, **kwargs):
        author = self.store.get_attempt(kwargs["author_attempt"]) if kwargs.get("author_attempt") else None
        file_ref = next((item for item in author["evidence"] if item.get("kind") == "delivery"), None) if author else None
        if file_ref is None:
            file_ref = self.refs("public bytes for " + identity)[0]
        files = [{**file_ref, "package_path": "draft/input/prompt.md", "role": "prompt"}]
        return self.store.register_candidate(
            task, identity,
            input_digest=kwargs.pop("input_digest", sha256(identity.encode())),
            content_digest=kwargs.pop("content_digest", sha256((identity + "-all").encode())),
            evaluation_digest=kwargs.pop("evaluation_digest", sha256((identity + "-private").encode())),
            evidence=self.refs(), files=kwargs.pop("files", files), **kwargs,
        )

    def enqueue(self, identity, role="author", task="item-a", candidate="candidate-a", **kwargs):
        return self.store.enqueue(
            identity, task, candidate, role, "policy-1",
            intent_id=kwargs.pop("intent_id", identity + "-intent"),
            purpose="neutral fixture intent", request_digest=sha256(b"request"),
            modifies=role in ("author", "repair"), **kwargs,
        )

    def complete(self, role="quality", candidate="candidate-a", sid=None, task="item-a"):
        self.counter += 1
        identity = f"work-{self.counter}"
        if role == "solver":
            self.store.record_decision(
                identity + "-approval", task, candidate, action="experiment",
                rationale="independent fixture confirmation of remaining unknown",
                experiment_purpose="neutral fixture intent", authorized=True,
                evidence=self.refs(),
            )
        self.enqueue(identity, role, task, candidate)
        attempt = self.store.claim("fixture-worker")
        self.assertEqual(identity, attempt["work_id"])
        identity = attempt["attempt_id"]
        self.store.mark_starting(identity)
        self.store.record_process(identity, 123, "fixture-start-identity", sid or f"fixture-context-{self.counter}")
        self.store.mark_collecting(identity)
        refs = self.refs("complete fixture output " + identity)
        if role in ("author", "repair"):
            refs[0]["kind"] = "delivery"
        return self.store.finish_attempt(
            identity, process_exit=0, response_complete=True,
            artifacts_complete=True, technical_accepted=True,
            evidence=refs,
        )

    def review(self, identity="review-a", candidate="candidate-a", verdict="accepted", attempt=None):
        attempt = attempt or self.complete(candidate=candidate)
        return self.store.record_review(
            identity, candidate, attempt["attempt_id"], verdict=verdict,
            scope="public package and private standard; linked responsibilities",
            whole_package=True, evidence=self.refs("independent scope judgment"),
        )

    def defect(self):
        self.review("review-defect", verdict="defects")
        return self.store.open_defect(
            "defect-a", "candidate-a", "review-defect",
            responsibility="derive disposition from source records", evidence=self.refs(),
        )

    def repaired(self):
        self.defect()
        author = self.complete("repair")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"])
        self.store.record_repair("defect-a", "candidate-b", evidence=self.refs("actual public diff and retained prerequisites"))

    def test_policy_versions_are_immutable_and_validate_config(self):
        with self.assertRaises(TaskflowError):
            self.store.register_policy("policy-1", self.policy)
        for config in ({}, {"concurrency": True, "roles": {}}, {"concurrency": 1, "roles": {"a": {}}}):
            with self.assertRaises(TaskflowError):
                self.store.register_policy("bad", config)
        self.assertEqual(sha256(json.dumps(self.policy, sort_keys=True, separators=(",", ":")).encode()), self.store.get_policy("policy-1")["digest"])

    def test_evidence_is_rechecked_not_keyword_scanned(self):
        refs = self.refs("lawful fixed commitment and arbitrary words")
        validate_evidence(refs)
        Path(refs[0]["path"]).write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "changed"):
            validate_evidence(refs)
        with self.assertRaises(TaskflowError):
            validate_evidence([])
        with self.assertRaises(TaskflowError):
            validate_evidence([{"path": str(self.root), "sha256": "x", "location": ""}])

    def test_concurrent_process_claims_are_unique(self):
        self.enqueue("one")
        script = (
            "import sys,json;from pathlib import Path;"
            "from taskflow.execution_store import ExecutionStore;"
            "print(json.dumps(ExecutionStore(Path(sys.argv[1])).claim(sys.argv[2])))"
        )
        def invoke(index):
            return json.loads(subprocess.check_output([sys.executable, "-c", script, str(self.store.path), str(index)], text=True))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(invoke, range(8)))
        winners = [result for result in results if result]
        self.assertEqual(1, len(winners))
        self.assertEqual("one", winners[0]["work_id"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_selected_work_ids_are_exact_under_concurrent_process_claims(self):
        for identity in ("unselected-first", "selected-one", "selected-two"):
            self.enqueue(identity, "research")
        script = (
            "import sys,json;from pathlib import Path;"
            "from taskflow.execution_store import ExecutionStore;"
            "print(json.dumps(ExecutionStore(Path(sys.argv[1])).claim(sys.argv[2],work_ids=json.loads(sys.argv[3]))))"
        )
        allowed = json.dumps(["selected-one", "selected-two"])
        def invoke(index):
            return json.loads(subprocess.check_output([sys.executable, "-c", script, str(self.store.path), str(index), allowed], text=True))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(invoke, range(8)))
        winners = [item for item in results if item]
        self.assertEqual(2, len(winners))
        self.assertEqual({"selected-one", "selected-two"}, {item["work_id"] for item in winners})
        self.assertEqual(2, len({item["attempt_id"] for item in winners}))
        self.assertEqual("ready", self.store.get_work("unselected-first")["state"])
        legacy = self.store.claim("legacy-default-worker")
        self.assertEqual("unselected-first", legacy["work_id"])

    def test_empty_selection_never_claims_and_none_keeps_legacy_behavior(self):
        self.enqueue("ready-work")
        before = self.store.events()
        self.assertIsNone(self.store.claim("empty-selection", work_ids=[]))
        self.assertEqual("ready", self.store.get_work("ready-work")["state"])
        self.assertEqual(before, self.store.events())
        self.assertEqual("ready-work", self.store.claim("default-selection", work_ids=None)["work_id"])

    def test_claim_selection_validation_and_global_active_gates_remain_effective(self):
        self.enqueue("first-writer")
        self.enqueue("second-writer")
        self.assertEqual("first-writer", self.store.claim("one", work_ids=["first-writer"])["work_id"])
        self.assertIsNone(self.store.claim("two", work_ids=["second-writer"]))
        for bad in ("second-writer", [None], ["../outside"]):
            with self.assertRaises(TaskflowError):
                self.store.claim("invalid", work_ids=bad)
        self.assertEqual("ready", self.store.get_work("second-writer")["state"])

    def test_selected_task_does_not_exclude_unselected_active_concurrency(self):
        self.store.register_policy("one-slot", {**self.policy, "concurrency": 1})
        self.store.enqueue("active-unselected", "item-a", "candidate-a", "research", "one-slot", intent_id="active-unselected-intent", purpose="unselected active work", request_digest=sha256(b"request"))
        self.enqueue("selected-ready", "research")
        self.store.claim("original-worker", work_ids=["active-unselected"])
        self.assertIsNone(self.store.claim("scoped-worker", work_ids=["selected-ready"]))
        self.assertEqual("ready", self.store.get_work("selected-ready")["state"])

    def test_one_writer_and_other_tasks_continue(self):
        self.enqueue("writer-a")
        self.enqueue("writer-a-next")
        self.candidate("item-b", "candidate-other")
        self.enqueue("writer-b", task="item-b", candidate="candidate-other")
        self.assertEqual("writer-a", self.store.claim("first")["work_id"])
        self.assertEqual("writer-b", self.store.claim("second")["work_id"])
        self.assertIsNone(self.store.claim("third"))

    def test_paused_single_task_does_not_block_other_ready_task(self):
        self.enqueue("blocked")
        self.candidate("item-b", "candidate-other")
        self.enqueue("available", task="item-b", candidate="candidate-other")
        self.store.set_control("task:item-a", "pause", "awaiting evidence")
        self.assertEqual("available", self.store.claim("worker")["work_id"])
        self.store.set_control("task:item-a", "resume", "evidence received")
        self.assertEqual("blocked", self.store.claim("worker")["work_id"])

    def test_global_concurrency_obeys_active_policy(self):
        policy = {**self.policy, "concurrency": 1}
        self.store.register_policy("policy-limited", policy)
        self.store.enqueue("limited", "item-a", "candidate-a", "research", "policy-limited", intent_id="limited-intent", purpose="check", request_digest=sha256(b"request"))
        self.enqueue("next", "research")
        self.store.claim("one")
        self.assertIsNone(self.store.claim("two"))

    def test_dependency_and_cancel_only_affect_ready_work(self):
        self.enqueue("parent", "research")
        self.enqueue("dependent", "research", dependencies=["parent"])
        attempt = self.store.claim("one")
        self.assertIsNone(self.store.claim("two"))
        self.store.set_control("all", "cancel", "cancel pending dispatch")
        self.assertEqual("claimed", self.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual("cancelled", self.store.get_work("dependent")["state"])

    def test_expired_claim_does_not_imply_invocation_did_not_happen(self):
        self.enqueue("first")
        attempt = self.store.claim("old-controller")
        self.store.mark_uncertain(attempt["attempt_id"], "controller disappeared before PID persisted")
        restarted = ExecutionStore(self.store.path)
        self.enqueue("new-author")
        self.assertIsNone(restarted.claim("restarted"))
        self.assertEqual("uncertain", restarted.get_attempt(attempt["attempt_id"])["state"])

    def test_recovery_requires_matching_process_start_identity(self):
        self.enqueue("first")
        attempt = self.store.claim("worker")
        identity = attempt["attempt_id"]
        self.store.mark_starting(identity)
        self.store.record_process(identity, 78, "started-one")
        self.store.mark_uncertain(identity, "controller interrupted")
        with self.assertRaisesRegex(TaskflowError, "identity mismatch"):
            self.store.reconcile(identity, {"process_state": "alive", "process_identity": "started-other", "evidence": self.refs()})
        restored = self.store.reconcile(identity, {"process_state": "alive", "process_identity": "started-one", "evidence": self.refs()})
        self.assertEqual("running", restored["state"])
        uncertain = self.store.reconcile(identity, {"process_state": "dead", "evidence": self.refs()})
        self.assertEqual("uncertain", uncertain["state"])

    def test_zero_exit_and_missing_artifact_or_response_never_complete(self):
        for response, artifacts in ((False, True), (True, False), (False, False)):
            self.counter += 1
            self.enqueue(f"incomplete-{self.counter}", "research")
            attempt = self.store.claim("worker")
            result = self.store.finish_attempt(attempt["attempt_id"], process_exit=0, response_complete=response, artifacts_complete=artifacts, technical_accepted=True, evidence=self.refs("exit 0 only"))
            self.assertEqual("failed", result["state"])
            self.assertEqual(response, result["response_complete"])
        self.assertEqual([], self.store.dashboard()["quality"][:0])

    def test_unknown_process_exit_keeps_recovery_slot(self):
        self.enqueue("unknown")
        attempt = self.store.claim("worker")
        result = self.store.finish_attempt(attempt["attempt_id"], process_exit=None, response_complete=True, artifacts_complete=True, technical_accepted=True, evidence=self.refs())
        self.assertEqual("uncertain", result["state"])
        self.enqueue("next")
        self.assertIsNone(self.store.claim("worker"))

    def test_terminal_attempt_and_intent_cannot_be_rewritten(self):
        attempt = self.complete("research")
        with self.assertRaises(TaskflowError):
            self.store.finish_attempt(attempt["attempt_id"], process_exit=0, response_complete=True, artifacts_complete=True, technical_accepted=True, evidence=self.refs())
        with self.assertRaises(TaskflowError):
            self.enqueue("another", "research", intent_id=attempt["intent_id"])

    def test_public_revision_invalidates_old_quality(self):
        self.review()
        self.store.quality_gate("candidate-a")
        author = self.complete("author")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "missing/stale"):
            self.store.quality_gate("candidate-b")
        with self.assertRaises(TaskflowError):
            self.enqueue("blocked-solver", "solver", candidate="candidate-b")

    def test_author_role_cannot_self_sign_and_same_sid_is_rejected(self):
        author = self.complete("author", sid="shared-context")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "completed quality"):
            self.review("self-review", "candidate-b", attempt=author)
        quality = self.complete("quality", "candidate-b", sid="shared-context")
        with self.assertRaisesRegex(TaskflowError, "author context"):
            self.review("pretend-independent", "candidate-b", attempt=quality)

    def test_unclosed_defect_blocks_original_responsibility(self):
        self.review()
        self.defect()
        with self.assertRaisesRegex(TaskflowError, "unclosed"):
            self.store.quality_gate("candidate-a")

    def test_actual_material_repair_and_independent_package_recheck_close(self):
        self.repaired()
        self.review("review-recheck", "candidate-b")
        closed = self.store.close_defect("defect-a", "review-recheck", evidence=self.refs("same responsibilities independently checked after actual revision"))
        self.assertEqual("closed", closed["state"])
        self.store.quality_gate("candidate-b")
        self.assertEqual("completed", self.complete("solver", "candidate-b")["state"])

    def test_report_only_change_cannot_close_material_defect(self):
        original = self.store.dashboard()["quality"][0]
        self.assertEqual("unresolved", original["state"])
        self.defect()
        author = self.complete("repair")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"], input_digest=sha256(b"candidate-a"))
        with self.assertRaisesRegex(TaskflowError, "report/template-only"):
            self.store.record_repair("defect-a", "candidate-b", evidence=self.refs("only report wording changed"))

    def test_review_must_follow_repair_and_cover_whole_package(self):
        self.repaired()
        attempt = self.complete("quality", "candidate-b")
        self.store.record_review("limited-review", "candidate-b", attempt["attempt_id"], verdict="accepted", scope="one changed line", whole_package=False, evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.close_defect("defect-a", "limited-review", evidence=self.refs())

    def test_new_finding_reopens_same_bytes(self):
        self.review()
        self.store.quality_gate("candidate-a")
        self.store.open_defect("new-finding", "candidate-a", "review-a", responsibility="newly found cross-file responsibility", evidence=self.refs())
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("candidate-a")
        self.assertEqual("unresolved", self.store.dashboard()["quality"][0]["state"])

    def test_same_version_authorized_confirmation_is_not_input_deduped(self):
        self.review()
        first = self.complete("solver")
        second = self.complete("solver")
        self.assertNotEqual(first["attempt_id"], second["attempt_id"])
        self.assertEqual(first["input_digest"], second["input_digest"])
        self.assertNotEqual(first["intent_id"], second["intent_id"])

    def test_solver_requires_authorized_candidate_and_exact_purpose(self):
        self.review()
        with self.assertRaisesRegex(TaskflowError, "authorized experiment"):
            self.enqueue("without-decision", "solver")
        self.store.record_decision("other-purpose", "item-a", "candidate-a", action="experiment", rationale="check another approved unknown", experiment_purpose="another purpose", authorized=True, evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "exact purpose"):
            self.enqueue("mismatched-purpose", "solver")

    def test_author_missing_modifies_flag_still_claims_writer_ownership(self):
        for identity in ("writer-one", "writer-two"):
            work = self.store.enqueue(identity, "item-a", "candidate-a", "author", "policy-1", intent_id=identity + "-intent", purpose="author revision", request_digest=sha256(b"request"))
            self.assertTrue(work["modifies"])
        self.store.claim("one")
        self.assertIsNone(self.store.claim("two"))

    def test_closed_later_revision_does_not_rehabilitate_original(self):
        self.repaired()
        self.review("review-original-again", "candidate-a")
        self.review("review-repaired", "candidate-b")
        self.store.close_defect("defect-a", "review-repaired", evidence=self.refs())
        self.store.quality_gate("candidate-b")
        with self.assertRaisesRegex(TaskflowError, "different/later revision"):
            self.store.quality_gate("candidate-a")

    def test_private_revision_preserves_old_attempt_and_requires_new_review(self):
        self.review()
        solver = self.complete("solver")
        original = self.store.get_candidate("candidate-a")
        author = self.complete("repair")
        self.candidate("item-a", "private-revision", parent_id="candidate-a", author_attempt=author["attempt_id"], input_digest=original["input_digest"])
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("private-revision")
        saved = self.store.get_attempt(solver["attempt_id"])
        self.assertEqual(solver, saved)
        self.assertEqual("candidate-a", saved["candidate_id"])

    def test_defect_kind_cannot_be_repaired_in_wrong_file_boundary(self):
        self.defect()
        author = self.complete("repair")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "public repair"):
            self.store.record_repair("defect-a", "candidate-b", disposition="private_repair", evidence=self.refs())

    def test_same_original_input_can_have_explicit_narrower_scope(self):
        self.defect()
        author = self.complete("repair")
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"], input_digest=self.store.get_candidate("candidate-a")["input_digest"])
        repair = self.store.record_repair("defect-a", "candidate-b", disposition="scope_adjustment", evidence=self.refs("approved upstream work retained and capability declaration narrowed"))
        self.assertEqual("scope_adjustment", repair["disposition"])
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("candidate-b")

    def test_dashboard_exposes_waiting_owner_and_observation_separately(self):
        self.enqueue("writing")
        self.enqueue("next-writing")
        self.store.claim("author-worker")
        self.store.observe("item-a", self.refs()[0], "historical-observation", "controller-a", "2026-11-01T00:00:00Z", {})
        task = self.store.dashboard()["tasks"][0]
        self.assertEqual("observed", task["mode"])
        self.assertEqual("controller-a", task["owner"])
        self.assertIn("scoped ownership receipt required", task["waiting"][0]["reason"])
        self.assertEqual("author", task["next_role"])
        self.assertEqual("claimed", task["active_attempts"][0]["state"])

    def test_next_investment_uses_evidence_not_automatic_score_threshold(self):
        self.review()
        for decision, action, rationale in (("high", "research", "successful method covers existing responsibility; investigate new unknown"), ("low", "retain", "effective performance archived; preserve current asset"), ("environment", "environment", "tool failure needs runtime repair")):
            self.store.record_decision(decision, "item-a", "candidate-a", action=action, rationale=rationale, evidence=self.refs())
        with self.assertRaises(TaskflowError):
            self.store.record_decision("bad-experiment", "item-a", "candidate-a", action="experiment", rationale="quality accepted alone", evidence=self.refs())
        self.store.record_decision("confirmation", "item-a", "candidate-a", action="experiment", rationale="confirm documented remaining business unknown", experiment_purpose="second independent same-version confirmation", authorized=True, evidence=self.refs())
        self.assertEqual(["research", "retain", "environment", "experiment"], [value["action"] for value in self.store.dashboard()["decisions"]])

    def test_observation_does_not_take_ownership_and_receipt_is_required(self):
        observation = self.store.observe("item-a", self.refs()[0], "author-running", "old-controller", "2026-11-01T00:00:00+00:00", {"scope": "read-only"})
        self.assertEqual("observed", observation["mode"])
        self.assertEqual([], self.store.dashboard()["ownership"])
        with self.assertRaises(TaskflowError):
            self.store.transfer_owner("item-a", "old-controller", "new-controller", [])
        with self.assertRaises(TaskflowError):
            self.store.transfer_owner("item-a", "wrong-owner", "new-controller", self.refs())
        transfer = self.store.transfer_owner("item-a", "old-controller", "new-controller", self.refs("controller receipt relinquishing new work"))
        self.assertEqual("managed_dispatch", transfer["mode"])
        self.assertEqual("new-controller", self.store.dashboard()["ownership"][0]["owner"])

    def test_work_item_handoff_does_not_take_whole_task_from_controller(self):
        self.store.observe("item-a", self.refs()[0], "author-complete", "controller", "2026-11-01T00:00:00Z", {})
        transfer = self.store.transfer_owner("item-a", "controller", "execution-layer", self.refs("controller releases only named next work"), work_id="next-quality")
        self.assertEqual("work_item", transfer["scope"])
        task = self.store.dashboard()["tasks"][0]
        self.assertEqual("managed_work_item", task["mode"])
        self.assertEqual("controller", task["owner"])
        self.assertEqual("next-quality", task["managed_work_items"][0]["work_id"])
        self.store.transfer_owner("item-a", "controller", "execution-layer", self.refs("independent later scope"), work_id="next-research")
        self.assertEqual(2, len(self.store.dashboard()["ownership"]))

    def test_active_managed_attempt_prevents_owner_transfer(self):
        self.store.observe("item-a", self.refs()[0], "observed", "old", "2026-11-01T00:00:00Z", {})
        self.store.transfer_owner("item-a", "old", "execution", self.refs("controller releases dispatch"), work_id="active")
        self.enqueue("active", dispatch_owner="execution")
        self.store.claim("worker")
        with self.assertRaisesRegex(TaskflowError, "unresolved managed attempt"):
            self.store.transfer_owner("item-a", "execution", "new", self.refs(), work_id="active")

    def test_observed_task_needs_matching_work_scope_before_dispatch(self):
        self.store.observe("item-a", self.refs()[0], "observed", "controller", "2026-11-01T00:00:00Z", {})
        with self.assertRaisesRegex(TaskflowError, "scoped ownership"):
            self.enqueue("not-transferred")
        self.store.transfer_owner("item-a", "controller", "engine", self.refs("release only this work"), work_id="allowed")
        with self.assertRaisesRegex(TaskflowError, "dispatch_owner"):
            self.enqueue("allowed", dispatch_owner="controller")
        self.enqueue("allowed", dispatch_owner="engine")
        with self.assertRaisesRegex(TaskflowError, "scoped ownership"):
            self.enqueue("another-work", dispatch_owner="engine")
        self.assertEqual("allowed", self.store.claim("engine-worker")["work_id"])

    def test_handoff_back_blocks_queued_intent_without_changing_inflight(self):
        self.store.observe("item-a", self.refs()[0], "observed", "controller", "2026-11-01T00:00:00Z", {})
        self.store.transfer_owner("item-a", "controller", "engine", self.refs("release research"), work_id="research-running")
        self.enqueue("research-running", "research", dispatch_owner="engine")
        running = self.store.claim("worker")
        self.store.transfer_owner("item-a", "controller", "engine", self.refs("release next work"), work_id="next-work")
        self.enqueue("next-work", "research", dispatch_owner="engine")
        self.store.transfer_owner("item-a", "engine", "controller", self.refs("return only queued next work"), work_id="next-work")
        self.assertIsNone(self.store.claim("worker"))
        self.assertEqual(running, self.store.get_attempt(running["attempt_id"]))
        self.assertEqual("ready", self.store.get_work("next-work")["state"])
        self.assertIn("ownership changed", self.store.dashboard()["tasks"][0]["waiting"][0]["reason"])

    def test_observation_added_after_queue_stops_unowned_future_claim(self):
        self.enqueue("queued")
        self.store.observe("item-a", self.refs()[0], "observed", "controller", "2026-11-01T00:00:00Z", {})
        self.assertIsNone(self.store.claim("unapproved-worker"))

    def test_return_future_dispatch_preserves_active_original_identity(self):
        self.store.observe("item-a", self.refs()[0], "observed", "controller", "2026-11-01T00:00:00Z", {})
        self.store.transfer_owner("item-a", "controller", "engine", self.refs("release future dispatch"))
        self.enqueue("running", "research", dispatch_owner="engine")
        running = self.store.claim("worker")
        self.enqueue("queued", "research", dispatch_owner="engine")
        self.store.transfer_owner("item-a", "engine", "controller", self.refs("return future dispatch, active attempt stays with engine"))
        self.assertEqual(running, self.store.get_attempt(running["attempt_id"]))
        self.assertIsNone(self.store.claim("engine-worker"))
        self.assertEqual("ready", self.store.get_work("queued")["state"])

    def test_late_historical_report_does_not_replace_newer_source_time(self):
        current = self.store.observe("item-a", self.refs("current source")[0], "review-running", "current-controller", "2026-11-01T12:00:00Z", {"reported_at": "2026-11-01T11:30:00Z"})
        old = self.store.observe("item-a", self.refs("late old source")[0], "author-running", "old-controller", "2026-11-01T13:00:00Z", {"reported_at": "2026-11-01T08:00:00Z"})
        board = self.store.dashboard()
        self.assertEqual("review-running", board["observations"][0]["observed_status"])
        self.assertEqual(current["event_seq"], board["tasks"][0]["observation"]["event_seq"])
        self.assertEqual(2, len(board["observation_history"]))
        self.assertEqual("2026-11-01T08:00:00Z", board["observation_history"][1]["details"]["reported_at"])
        self.assertGreater(old["event_seq"], current["event_seq"])
        transfer = self.store.transfer_owner("item-a", "current-controller", "engine", self.refs("current owner receipt"), work_id="next-quality")
        self.assertEqual("engine", transfer["owner"])

    def test_observation_timezone_order_and_tie_use_actual_instant_then_event(self):
        self.store.observe("item-a", self.refs()[0], "latest-instant", "controller", "2026-11-01T12:00:00+09:00", {})
        self.store.observe("item-a", self.refs()[0], "older-instant", "controller", "2026-11-01T04:00:00+02:00", {})
        self.assertEqual("latest-instant", self.store.dashboard()["observations"][0]["observed_status"])
        tied = self.store.observe("item-a", self.refs()[0], "same-instant-new-evidence", "controller", "2026-11-01T03:00:00Z", {})
        self.assertEqual(tied["event_seq"], self.store.dashboard()["observations"][0]["event_seq"])

    def test_invalid_observation_time_is_rejected_and_invalid_reported_time_falls_back(self):
        for value in ("now", "2026-11-01T12:00:00", "2026-99-01T12:00:00Z", "", 17):
            with self.assertRaises(TaskflowError):
                self.store.observe("item-a", self.refs()[0], "observed", "controller", value, {})
        self.assertEqual([], self.store.dashboard()["observation_history"])
        observation = self.store.observe("item-a", self.refs()[0], "current", "controller", "2026-11-01T12:00:00Z", {"reported_at": "unparseable original source timestamp"})
        self.assertEqual("observed_at_fallback", observation["time_basis"])
        self.assertEqual("2026-11-01T12:00:00+00:00", observation["effective_at"])
        self.assertIsNotNone(observation["time_issue"])

    def test_managed_running_attempt_takes_precedence_over_later_observation(self):
        self.enqueue("running", "research")
        running = self.store.claim("engine")
        self.store.observe("item-a", self.refs()[0], "complete", "controller", "2026-11-01T12:00:00Z", {})
        task = self.store.dashboard()["tasks"][0]
        self.assertEqual("claimed", task["current_state"])
        self.assertEqual("managed_attempt", task["current_state_source"])
        self.assertEqual("complete", task["observation"]["observed_status"])
        self.assertEqual(running, self.store.get_attempt(running["attempt_id"]))

    def external_trace(self, sid, terminal="turn.completed", another_start=None):
        self.counter += 1
        path = self.root / f"external-{self.counter}.jsonl"
        events = [{"type": "thread.started", "thread_id": sid}]
        if another_start:
            events.append({"type": "thread.started", "thread_id": another_start})
        events.append({"type": terminal})
        path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
        return evidence_ref(path, "external author actual complete trace")

    def test_external_author_boundary_reuses_actual_trace_without_fake_attempt(self):
        sid = str(uuid.uuid4())
        trace = self.external_trace(sid)
        candidate = self.store.register_candidate("item-a", "external-candidate", input_digest=sha256(b"external-public"), content_digest=sha256(b"external-content"), evaluation_digest=sha256(b"external-private"), parent_id="candidate-a", evidence=[trace], external_author_sids=[sid])
        self.assertEqual([sid], candidate["external_author_sids"])
        self.assertIsNone(candidate["author_attempt"])
        self.assertIsNone(candidate["external_provenance"][0]["engine_attempt_id"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        same_context = self.complete("quality", "external-candidate", sid=sid)
        with self.assertRaisesRegex(TaskflowError, "external author context"):
            self.review("false-independent", "external-candidate", attempt=same_context)
        self.review("actual-independent", "external-candidate")
        self.store.quality_gate("external-candidate")

    def test_external_author_rejects_claimed_sid_without_completed_matching_trace(self):
        sid = str(uuid.uuid4())
        for sids, evidence in (
            ([sid], self.refs("author said completed")),
            ([sid], [self.external_trace(str(uuid.uuid4()))]),
            ([sid], [self.external_trace(sid, "turn.failed")]),
            ([sid], [self.external_trace(sid, another_start=str(uuid.uuid4()))]),
            (["not-a-uuid"], self.refs()),
            ([sid, sid], [self.external_trace(sid)]),
        ):
            with self.assertRaises(TaskflowError):
                self.store.register_candidate("item-a", "rejected-external", input_digest=sha256(b"public"), content_digest=sha256(b"content"), evaluation_digest=sha256(b"private"), evidence=evidence, external_author_sids=sids)

    def test_descendant_review_rejects_original_external_author_context(self):
        sid = str(uuid.uuid4())
        self.store.register_candidate("item-a", "external-parent", input_digest=sha256(b"public"), content_digest=sha256(b"content"), evaluation_digest=sha256(b"private"), evidence=[self.external_trace(sid)], external_author_sids=[sid])
        author = self.complete("repair", "external-parent")
        self.candidate("item-a", "descendant", parent_id="external-parent", author_attempt=author["attempt_id"])
        quality = self.complete("quality", "descendant", sid=sid)
        with self.assertRaisesRegex(TaskflowError, "external author context"):
            self.review("false-descendant-review", "descendant", attempt=quality)

    def test_same_byte_alias_cannot_escape_new_finding_or_dispatch_solver(self):
        original = self.store.get_candidate("candidate-a")
        self.store.register_candidate("item-a", "alias", **{key: original[key] for key in ("input_digest", "content_digest", "evaluation_digest")}, files=original["files"], evidence=self.refs())
        self.review("accepted-original")
        self.review("accepted-alias", "alias")
        self.store.record_decision("alias-experiment", "item-a", "alias", action="experiment", rationale="independent confirmation", experiment_purpose="neutral fixture intent", authorized=True, evidence=self.refs())
        self.enqueue("alias-solver", "solver", candidate="alias")
        self.store.open_defect("reopened-same-bytes", "candidate-a", "accepted-original", responsibility="new cross-file finding", evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "unclosed"):
            self.store.quality_gate("alias")
        self.assertIsNone(self.store.claim("must-not-dispatch"))

    def test_pre_discovery_author_attempt_cannot_claim_later_external_revision(self):
        old_author = self.complete("author")
        self.defect()
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=old_author["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "predates finding"):
            self.store.record_repair("defect-a", "candidate-b", evidence=self.refs("later material was changed externally"))

    def test_new_author_cannot_claim_changed_bytes_absent_from_its_delivery(self):
        self.defect()
        author = self.complete("repair")
        unrelated = self.refs("later unrelated external file bytes")[0]
        with self.assertRaisesRegex(TaskflowError, "actually collected delivery"):
            self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"], files=[{**unrelated, "role": "prompt", "package_path": "draft/input/prompt.md"}])

    def test_new_work_delegation_inherits_current_future_dispatch_owner(self):
        self.store.observe("item-a", self.refs()[0], "observed", "old", "2026-11-01T00:00:00Z", {})
        self.store.transfer_owner("item-a", "old", "engine", self.refs("future dispatch receipt"))
        with self.assertRaisesRegex(TaskflowError, "previous_owner"):
            self.store.transfer_owner("item-a", "old", "delegate", self.refs("stale owner cannot delegate"), work_id="delegated-work")
        receipt = self.store.transfer_owner("item-a", "engine", "delegate", self.refs("current owner delegates"), work_id="delegated-work")
        self.assertEqual("engine", receipt["previous_owner"])
        self.enqueue("delegated-work", "research", dispatch_owner="delegate")
        self.assertEqual("delegated-work", self.store.claim("delegate")["work_id"])

    def test_normal_revision_without_defect_cannot_claim_unrelated_old_output(self):
        author = self.complete("author")
        externally_changed = self.refs("new external change not delivered by the author")[0]
        with self.assertRaisesRegex(TaskflowError, "actually collected delivery"):
            self.candidate("item-a", "unattributed-revision", parent_id="candidate-a", author_attempt=author["attempt_id"], files=[{**externally_changed, "package_path": "draft/input/prompt.md", "role": "prompt"}])

    def test_managed_author_without_parent_infers_real_parent_and_checks_delivery(self):
        author = self.complete("author")
        unrelated = self.refs("external bytes not authored by this attempt")[0]
        with self.assertRaisesRegex(TaskflowError, "actually collected delivery"):
            self.candidate("item-a", "no-parent-bypass", author_attempt=author["attempt_id"], files=[{**unrelated, "package_path": "draft/input/prompt.md", "role": "prompt"}])
        lawful = self.candidate("item-a", "inferred-revision", author_attempt=author["attempt_id"])
        self.assertEqual("candidate-a", lawful["parent_id"])

    def test_old_accepted_attempt_cannot_be_restamped_after_later_rejection(self):
        accepted = self.complete("quality")
        self.review("first-acceptance", attempt=accepted)
        self.review("new-rejection", verdict="inconclusive")
        with self.assertRaisesRegex(TaskflowError, "restamp"):
            self.review("restamped-acceptance", attempt=accepted)
        with self.assertRaisesRegex(TaskflowError, "later independent"):
            self.store.quality_gate("candidate-a")

    def test_delayed_old_first_registration_cannot_cover_newer_rejection(self):
        old = self.complete("quality")
        self.review("newer-rejection", verdict="defects")
        self.review("late-registration-of-old", attempt=old)
        with self.assertRaisesRegex(TaskflowError, "later independent"):
            self.store.quality_gate("candidate-a")

    def test_explicit_prior_authorized_repair_binds_purpose_source_and_delivery(self):
        finding = self.refs("external independent finding awaiting DB intake")
        self.store.record_decision("prior-repair-approval", "item-a", "candidate-a", action="repair", rationale="neutral fixture intent", authorized=True, evidence=finding)
        author = self.complete("repair")
        self.review("intake-of-existing-finding", verdict="defects")
        self.store.open_defect("prior-known-defect", "candidate-a", "intake-of-existing-finding", responsibility="previously authorized repair responsibility", evidence=finding)
        self.candidate("item-a", "candidate-b", parent_id="candidate-a", author_attempt=author["attempt_id"])
        repaired = self.store.record_repair("prior-known-defect", "candidate-b", prior_decision_id="prior-repair-approval", evidence=self.refs())
        self.assertEqual("prior-repair-approval", repaired["prior_decision_id"])

    def test_not_started_flag_without_bound_failure_receipt_remains_uncertain(self):
        self.enqueue("unknown-author")
        attempt = self.store.claim("worker")
        result = self.store.finish_attempt(attempt["attempt_id"], process_exit=None, process_state="not_started", response_complete=False, artifacts_complete=False, technical_accepted=False, evidence=self.refs("caller claims invocation never happened"))
        self.assertEqual("uncertain", result["state"])
        self.enqueue("next-author")
        self.assertIsNone(self.store.claim("worker"))

    def test_actual_bound_popen_failure_preserves_attempt_and_releases_writer(self):
        from taskflow.execution import enqueue_spec, run_worker

        executable = self.root / "not-executable"
        executable.write_text("deliberately nonexecutable fixture", encoding="utf-8")
        executable.chmod(0o600)
        cwd = self.root / "failed-author-work"
        cwd.mkdir()
        runtime = self.root / "failed-author-runtime"
        enqueue_spec(self.store, {"work": {
            "work_id": "failed-author", "task_id": "item-a", "candidate_id": "candidate-a",
            "role": "author", "policy_id": "policy-1", "intent_id": "failed-author-intent",
            "purpose": "explicit spawn-failure fixture",
        }, "runner": {
            "intent_id": "failed-author-intent", "policy_version": "policy-1", "role": "author",
            "mode": "test_fixture", "model": "fixture-model", "reasoning_effort": "high",
            "timeout_seconds": 20, "cwd": str(cwd), "argv": [str(executable)],
            "inputs": [], "outputs": ["delivery.md"],
            "request_sections": [{"kind": "operation", "text": "Neutral startup failure fixture"}],
        }}, runtime)
        failed = run_worker(self.store, "failure-worker")
        self.assertEqual("failed", failed["state"])
        self.assertEqual("not_started", failed["process_state"])
        self.assertIsNone(failed["process_exit"])
        self.assertFalse((runtime / "process.json").exists())
        self.enqueue("explicit-new-retry")
        next_attempt = self.store.claim("retry-worker")
        self.assertEqual("explicit-new-retry", next_attempt["work_id"])
        self.assertEqual(failed, self.store.get_attempt(failed["attempt_id"]))

    def external_review_fixture(self, report_overrides=None, attempt_overrides=None, spec_overrides=None):
        from taskflow.contracts import canonical, read_json, write_json
        from taskflow.packages import check_task, freeze_task
        from tools.execution_simulation import make_package, package_reviews

        root = self.root / "external-review-fixture"
        root.mkdir()
        task = make_package(root)
        check = check_task(task, write_report=False)
        files = read_json(task / "task.json")["files"]
        rows = [{"path": item["path"], "size": len((task / item["path"]).read_bytes()), "sha256": sha256((task / item["path"]).read_bytes())} for item in files]
        sid = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        report_path = root / "adoption-readiness.json"
        write_json(report_path, {
            "schema_version": 1, "signed_at_utc": (now - timedelta(seconds=3)).isoformat(),
            "signer": {"sid": sid, "role": "independent fixture quality", "independent_of_current_authoring": True},
            "candidate": {"root": str(task), **{key: check[key] for key in ("task_id", "input_digest", "content_digest", "evaluation_digest")}, "task_json_sha256": sha256((task / "task.json").read_bytes()), "independent_declared_files_fingerprint": sha256(canonical(sorted(rows, key=lambda item: item["path"])))},
            "quality_pass_within_declared_offline_scope": True, "blocking_issue_ids": [],
            "requires_author_repair": False, "quality_scope": "neutral offline fixture package only",
            "source_scope_limits": [{"id": "L1", "scope": "no production or difficulty claim"}],
            "business_quality": {"references": "current reference scope", "scoring": "current rubric scope", "public_responsibility": "current delegated responsibility scope"},
            **(report_overrides or {}),
        })
        trace_path = root / "trace.jsonl"
        trace_path.write_text(json.dumps({"type": "thread.started", "thread_id": sid}) + "\n" + json.dumps({"type": "turn.completed"}) + "\n", encoding="utf-8")
        write_json(root / "attempt.json", {"status": "process_exited", "returncode": 0, "exceptions": [], "nature": "independent_quality", "started_at": (now - timedelta(seconds=4)).isoformat(), "ended_at": (now - timedelta(seconds=2)).isoformat(), "requested_model": "fixture-model", "reasoning_effort": "high", **(attempt_overrides or {})})
        write_json(root / "spec.json", {"role": "independent_quality", "argv": ["never-executed-fixture-cli", "exec", "-m", "fixture-model", "-c", 'model_reasoning_effort="high"'], **(spec_overrides or {})})
        write_json(root / "archive-index.json", {"at": (now - timedelta(seconds=1)).isoformat(), "source_readback_matched": True, "files": [{"destination": str(path), "sha256": sha256(path.read_bytes())} for path in (report_path, trace_path, root / "attempt.json", root / "spec.json")]})
        package_reviews(task, report_path)
        business_path = task / "reports/business.json"
        business = read_json(business_path)
        business["signature_authority"] = {"sid": sid, "path": str(report_path), "sha256": sha256(report_path.read_bytes())}
        write_json(business_path, business)
        snapshot = Path(freeze_task(task, "fixture-v1")["path"])
        self.store.register_candidate("neutral", "external-reviewed", **{key: check[key] for key in ("input_digest", "content_digest", "evaluation_digest")}, evidence=[evidence_ref(snapshot / "manifest.json", "neutral frozen fixture")])
        arguments = {
            "snapshot_directory": str(snapshot), "policy_id": "policy-1",
            "report": evidence_ref(report_path, "existing independent scope signature"),
            "trace": evidence_ref(trace_path, "existing independent execution"),
            "runtime_attempt": evidence_ref(root / "attempt.json", "original process evidence"),
            "runtime_spec": evidence_ref(root / "spec.json", "original request configuration"),
            "archive_index": evidence_ref(root / "archive-index.json", "transport hash binding"),
        }
        return arguments

    def test_external_review_import_preserves_actual_scope_without_managed_attempt(self):
        arguments = self.external_review_fixture()
        imported = self.store.import_external_review("external-quality", "external-reviewed", **arguments)
        self.assertIsNone(imported["engine_attempt_id"])
        self.assertIsNone(imported["attempt_id"])
        self.assertFalse(imported["whole_package"])
        self.assertTrue(imported["snapshot_bound_acceptance"])
        self.assertEqual("neutral offline fixture package only", imported["scope"])
        self.assertEqual("no production or difficulty claim", imported["limitations"][0]["scope"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        self.assertEqual("external-quality", self.store.quality_gate("external-reviewed")["review_id"])
        with self.assertRaisesRegex(TaskflowError, "already has"):
            self.store.import_external_review("restamp", "external-reviewed", **arguments)

    def test_external_review_rejects_author_self_signature(self):
        arguments = self.external_review_fixture()
        sid = json.loads(Path(arguments["report"]["path"]).read_text())["signer"]["sid"]
        candidate = self.store.get_candidate("external-reviewed")
        self.store.register_candidate("neutral", "external-author-alias", **{key: candidate[key] for key in ("input_digest", "content_digest", "evaluation_digest")}, evidence=[arguments["trace"]], external_author_sids=[sid])
        with self.assertRaisesRegex(TaskflowError, "author context"):
            self.store.import_external_review("self-signed", "external-reviewed", **arguments)

    def test_external_review_rejects_later_new_finding_even_when_imported_late(self):
        arguments = self.external_review_fixture()
        self.store.import_external_review("external-quality", "external-reviewed", **arguments)
        self.store.open_defect("late-finding", "external-reviewed", "external-quality", responsibility="newly discovered relationship outside earlier signature", evidence=self.refs())
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("external-reviewed")

    def test_external_review_wrong_bytes_policy_or_trace_cannot_be_adopted(self):
        arguments = self.external_review_fixture()
        self.candidate("neutral", "different-candidate")
        with self.assertRaisesRegex(TaskflowError, "different candidate"):
            self.store.import_external_review("wrong-bytes", "different-candidate", **arguments)
        changed = dict(arguments)
        changed["trace"] = self.external_trace(str(uuid.uuid4()))
        with self.assertRaises(TaskflowError):
            self.store.import_external_review("wrong-sid", "external-reviewed", **changed)
        self.store.register_policy("different-policy", {"concurrency": 1, "roles": {"quality": {"model": "different-model", "reasoning_effort": "high", "timeout_seconds": 20}}})
        with self.assertRaisesRegex(TaskflowError, "configuration"):
            self.store.import_external_review("wrong-policy", "external-reviewed", **{**arguments, "policy_id": "different-policy"})

    def test_external_review_rejects_technical_intake_even_with_a_pass_claim(self):
        arguments = self.external_review_fixture(attempt_overrides={"nature": "technical_intake"}, spec_overrides={"role": "technical_intake"})
        with self.assertRaisesRegex(TaskflowError, "authorized quality role"):
            self.store.import_external_review("technical-is-not-quality", "external-reviewed", **arguments)

    def test_external_review_rejects_mock_attempt_and_unresolved_source(self):
        arguments = self.external_review_fixture(attempt_overrides={"nature": "test_fixture"}, spec_overrides={"role": "test_fixture"})
        with self.assertRaisesRegex(TaskflowError, "original completed role"):
            self.store.import_external_review("mock-is-not-quality", "external-reviewed", **arguments)

    def test_external_review_cannot_invent_signature_outside_runtime(self):
        arguments = self.external_review_fixture(report_overrides={"signed_at_utc": "2026-01-01T00:00:00Z"})
        with self.assertRaisesRegex(TaskflowError, "ordering"):
            self.store.import_external_review("out-of-order", "external-reviewed", **arguments)

    def prior_quality_source(self, arguments, **overrides):
        from taskflow.contracts import write_json

        report_path = Path(arguments["report"]["path"])
        prior = json.loads(report_path.read_text(encoding="utf-8"))
        prior["signer"] = {"sid": str(uuid.uuid4()), "role": "earlier independent full quality", "independent_of_current_authoring": True}
        prior["signed_at_utc"] = (datetime.fromisoformat(prior["signed_at_utc"]) - timedelta(seconds=10)).isoformat()
        prior["status"] = "not_ready_author_repair_required"
        prior["quality_pass_within_declared_offline_scope"] = False
        prior["requires_author_repair"] = True
        prior["blocking_issue_ids"] = ["D-earlier"]
        prior["affected_independent_recheck_completed"] = False
        prior["verification"] = {"unchanged_finite_groups_reused": 0}
        origin_role = overrides.pop("_origin_role", "independent_quality")
        prior.update(overrides)
        self.counter += 1
        path = report_path.with_name(f"prior-quality-{self.counter}.json")
        path.write_text(json.dumps(prior), encoding="utf-8")
        origin_root = report_path.parent / f"prior-origin-{self.counter}"
        origin_root.mkdir()
        trace = origin_root / "trace.jsonl"
        trace.write_text(json.dumps({"type": "thread.started", "thread_id": prior["signer"]["sid"]}) + "\n" + json.dumps({"type": "turn.completed"}) + "\n", encoding="utf-8")
        signed = datetime.fromisoformat(prior["signed_at_utc"])
        write_json(origin_root / "attempt.json", {"status": "process_exited", "returncode": 0, "exceptions": [], "nature": origin_role, "started_at": (signed - timedelta(seconds=1)).isoformat(), "ended_at": (signed + timedelta(seconds=1)).isoformat(), "requested_model": "fixture-model", "reasoning_effort": "high"})
        write_json(origin_root / "spec.json", {"role": origin_role, "argv": ["never-executed-fixture-cli", "exec", "-m", "fixture-model", "-c", 'model_reasoning_effort="high"']})
        write_json(origin_root / "archive-index.json", {"at": (signed + timedelta(seconds=2)).isoformat(), "source_readback_matched": True, "files": [{"destination": str(source), "sha256": sha256(source.read_bytes())} for source in (path, trace, origin_root / "attempt.json", origin_root / "spec.json")]})
        reference = evidence_ref(path, "actual earlier scoped quality record")
        reference["origin"] = {
            "trace": evidence_ref(trace, "original prior CLI trace"),
            "runtime_attempt": evidence_ref(origin_root / "attempt.json", "original prior process evidence"),
            "runtime_spec": evidence_ref(origin_root / "spec.json", "original prior role request"),
            "archive_index": evidence_ref(origin_root / "archive-index.json", "original prior archive binding"),
        }
        return reference

    def test_external_review_cannot_reuse_itself_through_path_hash_or_sid(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        report = Path(arguments["report"]["path"])
        copied = report.with_name("same-report-another-path.json")
        copied.write_bytes(report.read_bytes())
        same_sid = json.loads(report.read_text())["signer"]
        same_context = self.prior_quality_source(arguments, signer=same_sid)
        for support in (arguments["report"], evidence_ref(copied, "copied current report"), same_context):
            with self.assertRaisesRegex(TaskflowError, "cycle"):
                self.store.import_external_review("circular-source", "external-reviewed", **{**arguments, "supporting_evidence": [support]})

    def test_external_reuse_retains_earlier_failed_scope_without_demanding_new_pass(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        prior = self.prior_quality_source(arguments)
        imported = self.store.import_external_review("genuine-composed-scope", "external-reviewed", **{**arguments, "supporting_evidence": [prior]})
        self.assertEqual([prior], imported["supporting_evidence"])
        self.assertFalse(imported["whole_package"])
        self.assertEqual("genuine-composed-scope", self.store.quality_gate("external-reviewed")["review_id"])

    def test_external_reuse_rejects_later_time_wrong_fingerprint_or_missing_scope(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        current = json.loads(Path(arguments["report"]["path"]).read_text())
        bad_fingerprint = {**current["candidate"], "independent_declared_files_fingerprint": sha256(b"unrelated declaration")}
        bad_sources = [
            self.prior_quality_source(arguments, signed_at_utc=current["signed_at_utc"]),
            self.prior_quality_source(arguments, candidate=bad_fingerprint),
            self.prior_quality_source(arguments, quality_scope="", business_quality={}),
        ]
        for source in bad_sources:
            with self.assertRaises(TaskflowError):
                self.store.import_external_review("unusable-prior-source", "external-reviewed", **{**arguments, "supporting_evidence": [source]})

    def test_reused_quality_graph_rejects_indirect_cycle_to_current_report(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        prior = self.prior_quality_source(arguments, affected_independent_recheck_completed=True, verification={"unchanged_finite_groups_reused": 1}, supporting_evidence=[arguments["report"]])
        with self.assertRaisesRegex(TaskflowError, "cycle"):
            self.store.import_external_review("cyclic-origin", "external-reviewed", **{**arguments, "supporting_evidence": [prior]})

    def test_reused_quality_graph_rejects_missing_recursive_source_and_missing_runtime(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        absent = {"path": str(self.root / "missing-origin.json"), "sha256": sha256(b"absent origin"), "location": "declared required origin"}
        prior = self.prior_quality_source(arguments, affected_independent_recheck_completed=True, verification={"unchanged_finite_groups_reused": 1}, supporting_evidence=[absent])
        with self.assertRaisesRegex(TaskflowError, "missing earlier"):
            self.store.import_external_review("missing-recursive-origin", "external-reviewed", **{**arguments, "supporting_evidence": [prior]})
        missing_runtime = self.prior_quality_source(arguments)
        del missing_runtime["origin"]
        with self.assertRaisesRegex(TaskflowError, "original runtime"):
            self.store.import_external_review("missing-execution-origin", "external-reviewed", **{**arguments, "supporting_evidence": [missing_runtime]})

    def test_reused_quality_requires_business_scope_type_and_actual_quality_role(self):
        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        technical_text = self.prior_quality_source(arguments, business_quality="unresolved", quality_scope="technical integrity only; business quality unresolved")
        technical_role = self.prior_quality_source(arguments, _origin_role="technical_intake")
        for source in (technical_text, technical_role):
            with self.assertRaises(TaskflowError):
                self.store.import_external_review("technical-origin", "external-reviewed", **{**arguments, "supporting_evidence": [source]})

    def test_different_private_standard_requires_actual_affected_scope_mapping(self):
        from taskflow.contracts import canonical, read_json
        from taskflow.packages import check_task
        from tools.execution_simulation import make_package

        arguments = self.external_review_fixture(report_overrides={"affected_independent_recheck_completed": True, "verification": {"unchanged_finite_groups_reused": 1}, "closed_issue_ids": ["D-earlier"]})
        parent = self.root / "older-private-standard"
        parent.mkdir()
        task = make_package(parent)
        (task / "draft/evaluation/answer.md").write_text("an earlier private answer", encoding="utf-8")
        (task / "draft/evaluation/rubric.md").write_text("an earlier private standard", encoding="utf-8")
        checked = check_task(task, write_report=False)
        rows = [{"path": item["path"], "size": len((task / item["path"]).read_bytes()), "sha256": sha256((task / item["path"]).read_bytes())} for item in read_json(task / "task.json")["files"]]
        candidate = {"root": str(task), **{key: checked[key] for key in ("task_id", "input_digest", "content_digest", "evaluation_digest")}, "task_json_sha256": sha256((task / "task.json").read_bytes()), "independent_declared_files_fingerprint": sha256(canonical(sorted(rows, key=lambda item: item["path"])))}
        prior = self.prior_quality_source(arguments, candidate=candidate)
        with self.assertRaisesRegex(TaskflowError, "different private standard"):
            self.store.import_external_review("unmapped-private-version", "external-reviewed", **{**arguments, "supporting_evidence": [prior]})

    def test_dashboard_has_no_implied_score_or_platform_success(self):
        self.complete("research")
        board = self.store.dashboard()
        self.assertEqual("unknown", board["platform_status"])
        self.assertEqual("unresolved", board["quality"][0]["state"])
        self.assertEqual("completed", board["attempts"][0]["state"])
        self.assertEqual("requires_valid_experiment_evidence", board["difficulty_status"])
        events = self.store.events()
        self.assertEqual(list(range(1, len(events) + 1)), [event["seq"] for event in events])
        self.assertEqual([], self.store.events(events[-1]["seq"]))

    def test_flow_registration_and_restart_keep_one_sqlite_current_state(self):
        definition = {"policy_id": "policy-1", "initial_state": {"stage": "quality_pending"}, "task_id": "item-a"}
        registered = self.store.register_flow("flow-a", definition, self.refs())
        self.assertEqual(0, registered["revision"])
        self.assertEqual({"stage": "quality_pending"}, registered["state"])
        updated = self.store.update_flow("flow-a", 0, {"stage": "quality_running", "attempt_id": "observed-attempt"}, {"kind": "quality_dispatched", "evidence": self.refs()})
        restarted = ExecutionStore(self.store.path)
        self.assertEqual(updated, restarted.get_flow("flow-a"))
        self.assertEqual([updated], restarted.list_flows())
        self.assertEqual(registered["definition"], updated["definition"])
        self.assertEqual(registered["definition_digest"], updated["definition_digest"])
        self.assertEqual(1, updated["revision"])
        self.assertEqual("flow_updated", restarted.events(updated["event_seq"] - 1)[0]["kind"])

    def test_flow_cas_conflict_and_duplicate_definition_never_overwrite_or_add_event(self):
        first = self.store.register_flow("flow-a", {"policy_ids": ["policy-1"]}, self.refs(), {"stage": "initial"})
        self.store.update_flow("flow-a", 0, {"stage": "first"}, {"kind": "first_worker"})
        before = self.store.get_flow("flow-a")
        events_before = self.store.events()
        with self.assertRaisesRegex(TaskflowError, "revision conflict"):
            self.store.update_flow("flow-a", first["revision"], {"stage": "overwritten"}, {"kind": "stale_worker"})
        with self.assertRaises(TaskflowError):
            self.store.register_flow("flow-a", {"policy_id": "policy-1", "changed": True}, self.refs(), {})
        self.assertEqual(before, self.store.get_flow("flow-a"))
        self.assertEqual(events_before, self.store.events())

    def test_concurrent_flow_updates_have_one_winner(self):
        self.store.register_flow("flow-a", {"policy_id": "policy-1"}, self.refs(), {"stage": "initial"})
        def update(index):
            try:
                return self.store.update_flow("flow-a", 0, {"winner": index}, {"kind": "concurrent_worker"})
            except TaskflowError:
                return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as workers:
            outcomes = list(workers.map(update, range(8)))
        winners = [item for item in outcomes if item is not None]
        self.assertEqual(1, len(winners))
        self.assertEqual(winners[0], self.store.get_flow("flow-a"))
        self.assertEqual(1, len([item for item in self.store.events() if item["kind"] == "flow_updated"]))

    def test_flow_contract_and_policy_failures_leave_database_unchanged(self):
        before = self.store.events()
        for definition, initial in (([], {}), ({"policy_id": "missing"}, {}), ({"policy_ids": "policy-1"}, {}), ({}, [])):
            with self.assertRaises(TaskflowError):
                self.store.register_flow("bad-flow", definition, self.refs(), initial)
        self.assertEqual(before, self.store.events())
        self.assertEqual([], self.store.list_flows())
        with self.assertRaises(TaskflowError):
            self.store.get_flow("unknown")
        self.store.register_flow("flow-a", {}, self.refs(), {})
        for revision, state, event in ((True, {}, {"kind": "bad"}), (-1, {}, {"kind": "bad"}), (0, [], {"kind": "bad"}), (0, {}, {})):
            with self.assertRaises(TaskflowError):
                self.store.update_flow("flow-a", revision, state, event)
        self.assertEqual(0, self.store.get_flow("flow-a")["revision"])

    def test_review_and_defect_getters_return_existing_immutable_evidence(self):
        defect = self.defect()
        self.assertEqual(defect, self.store.get_defect("defect-a"))
        self.assertEqual("review-defect", self.store.get_review("review-defect")["review_id"])
        with self.assertRaises(TaskflowError):
            self.store.get_review("missing")
        with self.assertRaises(TaskflowError):
            self.store.get_defect("missing")

    def test_first_same_owner_future_dispatch_registration_preserves_scoped_and_inflight(self):
        self.store.observe("item-a", self.refs()[0], "quality-pending", "controller", "2026-11-01T00:00:00Z", {})
        self.store.transfer_owner("item-a", "controller", "engine", self.refs("existing named work handoff"), work_id="existing-role")
        self.enqueue("existing-role", "research", dispatch_owner="engine")
        active = self.store.claim("engine-worker")
        scoped_before = self.store.dashboard()["ownership"][0]
        registration = self.store.transfer_owner("item-a", "controller", "controller", self.refs("one authorized flow definition enables ordinary future dispatch"))
        self.assertEqual("observed_to_managed_future_dispatch", registration["transition_kind"])
        self.assertEqual(active, self.store.get_attempt(active["attempt_id"]))
        scoped_after = next(item for item in self.store.dashboard()["ownership"] if item.get("work_id") == "existing-role")
        self.assertEqual(scoped_before, scoped_after)
        self.enqueue("new-normal-role", "research", dispatch_owner="controller")
        self.assertEqual("new-normal-role", self.store.claim("ordinary-controller-worker")["work_id"])
        before = self.store.events()
        with self.assertRaisesRegex(TaskflowError, "restamp"):
            self.store.transfer_owner("item-a", "controller", "controller", self.refs())
        self.assertEqual(before, self.store.events())

    def test_same_owner_registration_rejects_wrong_owner_work_scope_and_missing_evidence(self):
        self.store.observe("item-a", self.refs()[0], "observed", "controller", "2026-11-01T00:00:00Z", {})
        with self.assertRaises(TaskflowError):
            self.store.transfer_owner("item-a", "wrong", "wrong", self.refs())
        with self.assertRaises(TaskflowError):
            self.store.transfer_owner("item-a", "controller", "controller", self.refs(), work_id="named-work")
        with self.assertRaises(TaskflowError):
            self.store.transfer_owner("item-a", "controller", "controller", [])
        self.assertEqual([], self.store.dashboard()["ownership"])

    def private_repair(self, *, change_public=False, record_now=True, additional_defect=False):
        self.review("private-defect-review", verdict="defects")
        self.store.open_defect("private-defect", "candidate-a", "private-defect-review", kind="rubric", responsibility="repair only the private standard while preserving public duties", evidence=self.refs())
        if additional_defect:
            self.store.open_defect("other-private-defect", "candidate-a", "private-defect-review", kind="reference", responsibility="another independent private responsibility", evidence=self.refs())
        author = self.complete("repair")
        original = self.store.get_candidate("candidate-a")
        delivered = next(item for item in author["evidence"] if item.get("kind") == "delivery")
        files = [*original["files"], {**delivered, "package_path": "draft/evaluation/rubric.md", "role": "rubric"}]
        self.candidate("item-a", "private-repaired", parent_id="candidate-a", author_attempt=author["attempt_id"], input_digest=sha256(b"changed-public") if change_public else original["input_digest"], files=files)
        if record_now:
            self.store.record_repair("private-defect", "private-repaired", disposition="private_repair", evidence=self.refs())
            if additional_defect:
                self.store.record_repair("other-private-defect", "private-repaired", disposition="private_repair", evidence=self.refs())
        return author

    def test_private_only_covered_recheck_closes_defect_without_formal_quality_upgrade(self):
        self.private_repair()
        independent = self.complete("quality", "private-repaired")
        review = self.store.record_review("private-recheck", "private-repaired", independent["attempt_id"], verdict="accepted", scope="the private rubric defect and necessary linked consequences", evidence=self.refs(), whole_package=False, covered_defect_ids=["private-defect"])
        closed = self.store.close_defect("private-defect", "private-recheck", evidence=self.refs())
        self.assertEqual("closed", closed["state"])
        self.assertFalse(review["whole_package"])
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.quality_gate("private-repaired")

    def test_private_partial_review_without_explicit_coverage_cannot_close(self):
        self.private_repair()
        independent = self.complete("quality", "private-repaired")
        self.store.record_review("uncovered-private-recheck", "private-repaired", independent["attempt_id"], verdict="accepted", scope="another private scope", evidence=self.refs(), whole_package=False)
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.close_defect("private-defect", "uncovered-private-recheck", evidence=self.refs())

    def test_changed_public_or_material_defect_still_requires_whole_package_review(self):
        self.private_repair(change_public=True)
        independent = self.complete("quality", "private-repaired")
        self.store.record_review("public-changed-partial", "private-repaired", independent["attempt_id"], verdict="accepted", scope="only private impact", evidence=self.refs(), covered_defect_ids=["private-defect"])
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.close_defect("private-defect", "public-changed-partial", evidence=self.refs())

    def test_material_scope_adjustment_with_same_public_still_requires_whole_package(self):
        self.defect()
        author = self.complete("repair")
        original = self.store.get_candidate("candidate-a")
        delivered = next(item for item in author["evidence"] if item.get("kind") == "delivery")
        self.candidate("item-a", "claim-adjusted", parent_id="candidate-a", author_attempt=author["attempt_id"], input_digest=original["input_digest"], files=[*original["files"], {**delivered, "package_path": "draft/evaluation/ownership.md", "role": "teacher"}])
        self.store.record_repair("defect-a", "claim-adjusted", disposition="scope_adjustment", evidence=self.refs())
        independent = self.complete("quality", "claim-adjusted")
        self.store.record_review("limited-material-claim", "claim-adjusted", independent["attempt_id"], verdict="accepted", scope="only capability claim adjustment", evidence=self.refs(), covered_defect_ids=["defect-a"])
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.close_defect("defect-a", "limited-material-claim", evidence=self.refs())

    def test_covered_recheck_rejects_wrong_revision_self_review_and_old_attempt(self):
        author = self.private_repair(record_now=False)
        old_quality = self.complete("quality", "private-repaired")
        self.store.record_repair("private-defect", "private-repaired", disposition="private_repair", evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "after the recorded"):
            self.store.record_review("old-private-recheck", "private-repaired", old_quality["attempt_id"], verdict="accepted", scope="old result restamped", evidence=self.refs(), covered_defect_ids=["private-defect"])
        wrong = self.complete("quality")
        with self.assertRaises(TaskflowError):
            self.store.record_review("wrong-revision", "candidate-a", wrong["attempt_id"], verdict="accepted", scope="wrong candidate", evidence=self.refs(), covered_defect_ids=["private-defect"])
        with self.assertRaises(TaskflowError):
            self.store.record_review("self-private-recheck", "private-repaired", author["attempt_id"], verdict="accepted", scope="author self claim", evidence=self.refs(), covered_defect_ids=["private-defect"])

    def test_covered_defect_list_requires_existing_ids_and_repaired_state(self):
        self.private_repair()
        independent = self.complete("quality", "private-repaired")
        for covered in (["missing"], ["private-defect", "private-defect"], "private-defect"):
            with self.assertRaises(TaskflowError):
                self.store.record_review("bad-coverage", "private-repaired", independent["attempt_id"], verdict="accepted", scope="invalid coverage metadata", evidence=self.refs(), covered_defect_ids=covered)

    def private_descendant(self, parent="private-repaired", identity="private-r2", change_public=False):
        author = self.complete("repair", parent)
        prior = self.store.get_candidate(parent)
        delivered = next(item for item in author["evidence"] if item.get("kind") == "delivery")
        files = [item for item in prior["files"] if item["package_path"] != "draft/evaluation/rubric.md"]
        files.append({**delivered, "package_path": "draft/evaluation/rubric.md", "role": "rubric"})
        return self.candidate("item-a", identity, parent_id=parent, author_attempt=author["attempt_id"], input_digest=sha256(identity.encode()) if change_public else prior["input_digest"], files=files)

    def test_final_descendant_explicitly_rechecks_and_closes_ancestor_and_new_defect(self):
        self.private_repair()
        first_recheck = self.complete("quality", "private-repaired")
        self.store.record_review("r1-found-new-defect", "private-repaired", first_recheck["attempt_id"], verdict="defects", scope="original repair plus newly located private issue", evidence=self.refs(), covered_defect_ids=["private-defect"])
        self.store.open_defect("new-private-defect", "private-repaired", "r1-found-new-defect", kind="reference", responsibility="genuinely new private issue", evidence=self.refs())
        self.private_descendant()
        self.store.record_repair("new-private-defect", "private-r2", disposition="private_repair", evidence=self.refs())
        final = self.complete("quality", "private-r2")
        self.store.record_review("final-r2-recheck", "private-r2", final["attempt_id"], verdict="accepted", scope="explicit original and new issue coverage on final current candidate", evidence=self.refs(), covered_defect_ids=["private-defect", "new-private-defect"])
        for identity in ["private-defect", "new-private-defect"]:
            self.assertEqual("closed", self.store.close_defect(identity, "final-r2-recheck", evidence=self.refs())["state"])
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("private-r2")

    def test_descendant_closure_rejects_branch_and_missing_explicit_coverage(self):
        self.private_repair()
        self.private_descendant(parent="candidate-a", identity="side-branch")
        branch_review = self.complete("quality", "side-branch")
        with self.assertRaisesRegex(TaskflowError, "ancestor"):
            self.store.record_review("branch-cannot-cover", "side-branch", branch_review["attempt_id"], verdict="accepted", scope="unrelated branch", evidence=self.refs(), covered_defect_ids=["private-defect"])
        self.private_descendant()
        final = self.complete("quality", "private-r2")
        self.store.record_review("whole-without-old-id", "private-r2", final["attempt_id"], verdict="accepted", scope="full current candidate but old defect not explicitly rechecked", whole_package=True, evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "explicit defect coverage"):
            self.store.close_defect("private-defect", "whole-without-old-id", evidence=self.refs())

    def test_descendant_private_partial_cannot_close_after_public_change(self):
        self.private_repair()
        self.private_descendant(change_public=True)
        final = self.complete("quality", "private-r2")
        self.store.record_review("r2-public-changed", "private-r2", final["attempt_id"], verdict="accepted", scope="private scope only", evidence=self.refs(), covered_defect_ids=["private-defect"])
        with self.assertRaisesRegex(TaskflowError, "whole-package"):
            self.store.close_defect("private-defect", "r2-public-changed", evidence=self.refs())

    def test_old_partial_acceptance_cannot_close_after_later_same_defect_rejection(self):
        self.private_repair()
        accepted = self.complete("quality", "private-repaired")
        self.store.record_review("old-partial-acceptance", "private-repaired", accepted["attempt_id"], verdict="accepted", scope="private defect", evidence=self.refs(), covered_defect_ids=["private-defect"])
        rejection = self.complete("quality", "private-repaired")
        self.store.record_review("later-private-rejection", "private-repaired", rejection["attempt_id"], verdict="defects", scope="same private defect remains", evidence=self.refs(), covered_defect_ids=["private-defect"])
        with self.assertRaisesRegex(TaskflowError, "later independent rejection"):
            self.store.close_defect("private-defect", "old-partial-acceptance", evidence=self.refs())
        self.assertEqual("repaired", self.store.get_defect("private-defect")["state"])

    def test_later_unrelated_private_rejection_does_not_block_covered_closure(self):
        self.private_repair(additional_defect=True)
        accepted = self.complete("quality", "private-repaired")
        self.store.record_review("specific-acceptance", "private-repaired", accepted["attempt_id"], verdict="accepted", scope="only first private defect", evidence=self.refs(), covered_defect_ids=["private-defect"])
        rejection = self.complete("quality", "private-repaired")
        self.store.record_review("other-scope-rejection", "private-repaired", rejection["attempt_id"], verdict="inconclusive", scope="only second private defect", evidence=self.refs(), covered_defect_ids=["other-private-defect"])
        self.assertEqual("closed", self.store.close_defect("private-defect", "specific-acceptance", evidence=self.refs())["state"])

    def test_late_registration_of_older_rejection_does_not_override_newer_actual_acceptance(self):
        self.private_repair()
        old_rejection = self.complete("quality", "private-repaired")
        acceptance = self.complete("quality", "private-repaired")
        self.store.record_review("newer-actual-acceptance", "private-repaired", acceptance["attempt_id"], verdict="accepted", scope="current private defect resolved", evidence=self.refs(), covered_defect_ids=["private-defect"])
        self.store.record_review("older-late-registration", "private-repaired", old_rejection["attempt_id"], verdict="defects", scope="older same-scope finding", evidence=self.refs(), covered_defect_ids=["private-defect"])
        self.assertEqual("closed", self.store.close_defect("private-defect", "newer-actual-acceptance", evidence=self.refs())["state"])

    def test_delayed_collection_uses_original_termination_time_to_block_old_acceptance(self):
        self.private_repair()
        self.enqueue("old-delayed-acceptance", "quality", candidate="private-repaired")
        old = self.store.claim("old-quality-worker")
        self.store.mark_starting(old["attempt_id"])
        self.store.record_process(old["attempt_id"], 123, "old-quality-start", "old-quality-context")
        actual_old_end = datetime.now(timezone.utc)
        later = self.complete("quality", "private-repaired")
        self.store.record_review("later-completed-rejection", "private-repaired", later["attempt_id"], verdict="inconclusive", scope="same defect under later evidence", evidence=self.refs(), covered_defect_ids=["private-defect"])
        runtime = self.root / "old-quality-receipt"
        runtime.mkdir()
        receipt = runtime / "collection.json"
        receipt.write_text(json.dumps({"intent_id": old["intent_id"], "sid": "old-quality-context", "termination": {"returncode": 0, "ended_at": actual_old_end.isoformat()}}), encoding="utf-8")
        finished = self.store.finish_attempt(old["attempt_id"], process_exit=0, response_complete=True, artifacts_complete=True, technical_accepted=True, evidence=[evidence_ref(receipt, "actual old CLI termination collected late")])
        self.assertGreater(finished["ended_at"], later["ended_at"])
        self.store.record_review("old-acceptance-collected-late", "private-repaired", old["attempt_id"], verdict="accepted", scope="older completion imported after the later rejection", evidence=self.refs(), covered_defect_ids=["private-defect"])
        with self.assertRaisesRegex(TaskflowError, "later independent rejection"):
            self.store.close_defect("private-defect", "old-acceptance-collected-late", evidence=self.refs())

    def scoped_acceptance(self, identity="scoped-acceptance", scope="flow-A.authorized-quality"):
        attempt = self.complete("quality")
        return self.store.record_review(identity, "candidate-a", attempt["attempt_id"], verdict="accepted", scope=scope, evidence=self.refs(), whole_package=False)

    def test_direct_accepted_review_without_repairs_is_guarded_against_newer_negative(self):
        accepted = self.scoped_acceptance()
        self.assertEqual(accepted, self.store.assert_review_current(accepted["review_id"]))
        with self.assertRaises(TaskflowError):
            self.store.quality_gate("candidate-a")
        newer = self.complete("quality")
        self.store.record_review("newer-direct-rejection", "candidate-a", newer["attempt_id"], verdict="defects", scope=accepted["scope"], evidence=self.refs())
        with self.assertRaisesRegex(TaskflowError, "later independent rejection"):
            self.store.assert_review_current(accepted["review_id"])

    def test_review_guard_preserves_unrelated_partial_scopes_and_validates_original_bytes(self):
        accepted = self.scoped_acceptance()
        unrelated = self.complete("quality")
        self.store.record_review("unrelated-scope-review", "candidate-a", unrelated["attempt_id"], verdict="inconclusive", scope="flow-B.separate-scope", evidence=self.refs())
        self.assertEqual(accepted, self.store.assert_review_current(accepted["review_id"]))
        Path(accepted["evidence"][0]["path"]).write_text("changed original role attestation", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "changed"):
            self.store.assert_review_current(accepted["review_id"])
        with self.assertRaisesRegex(TaskflowError, "accepted independent review"):
            self.store.assert_review_current("unrelated-scope-review")

    def test_review_guard_allows_explicit_disjoint_ids_even_with_same_registered_scope(self):
        self.private_repair(additional_defect=True)
        first = self.complete("quality", "private-repaired")
        accepted = self.store.record_review("first-specific-review", "private-repaired", first["attempt_id"], verdict="accepted", scope="registered-private-scope", evidence=self.refs(), covered_defect_ids=["private-defect"])
        second = self.complete("quality", "private-repaired")
        self.store.record_review("second-specific-review", "private-repaired", second["attempt_id"], verdict="defects", scope="registered-private-scope", evidence=self.refs(), covered_defect_ids=["other-private-defect"])
        self.assertEqual(accepted, self.store.assert_review_current(accepted["review_id"]))

    def test_flow_acceptance_rechecks_review_inside_cas_transaction_after_preflight(self):
        accepted = self.scoped_acceptance()
        registered = self.store.register_flow("acceptance-flow", {"policy_id": "policy-1"}, self.refs(), {"candidate_id": "candidate-a", "status": "receipt-pending"})
        self.store.assert_review_current(accepted["review_id"])
        # Another independent source arrives after the caller's preflight and
        # before the final state commit. This must invalidate that final commit.
        newer = self.complete("quality")
        self.store.record_review("arrived-before-commit", "candidate-a", newer["attempt_id"], verdict="inconclusive", scope=accepted["scope"], evidence=self.refs())
        before = self.store.events()
        with self.assertRaisesRegex(TaskflowError, "later independent rejection"):
            self.store.update_flow("acceptance-flow", 0, {"candidate_id": "candidate-a", "status": "accepted"}, {"kind": "workflow_result_received", "accepted_review_id": accepted["review_id"]})
        self.assertEqual(registered, self.store.get_flow("acceptance-flow"))
        self.assertEqual(before, self.store.events())

    def test_flow_acceptance_guard_binds_exact_candidate_and_retains_cas(self):
        accepted = self.scoped_acceptance()
        initial = self.store.register_flow("acceptance-flow", {"policy_id": "policy-1"}, self.refs(), {"candidate_id": "candidate-a", "status": "pending"})
        with self.assertRaisesRegex(TaskflowError, "exact candidate"):
            self.store.update_flow("acceptance-flow", 0, {"candidate_id": "different-candidate", "status": "accepted"}, {"kind": "workflow_result_received", "accepted_review_id": accepted["review_id"]})
        self.assertEqual(initial, self.store.get_flow("acceptance-flow"))
        committed = self.store.update_flow("acceptance-flow", 0, {"candidate_id": "candidate-a", "status": "accepted"}, {"kind": "workflow_result_received", "accepted_review_id": accepted["review_id"]})
        self.assertEqual(1, committed["revision"])
        self.assertEqual("accepted", committed["state"]["status"])
        before = self.store.events()
        with self.assertRaisesRegex(TaskflowError, "revision conflict"):
            self.store.update_flow("acceptance-flow", 0, {"candidate_id": "candidate-a", "status": "overwritten"}, {"kind": "stale_commit", "accepted_review_id": accepted["review_id"]})
        self.assertEqual(committed, self.store.get_flow("acceptance-flow"))
        self.assertEqual(before, self.store.events())


if __name__ == "__main__":
    unittest.main()
