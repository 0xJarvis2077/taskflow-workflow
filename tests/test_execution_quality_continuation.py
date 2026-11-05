"""Synthetic subprocess receipts exercise the normal continuation boundary."""

import json
import contextlib
import copy
import io
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_quality_continuation import (
    continue_inconclusive_workflow, inspect_inconclusive_workflow,
    resolved_inconclusive_dependency,
)
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flow_tests


class InconclusiveContinuationTests(unittest.TestCase):
    def setUp(self):
        self.helper = flow_tests.ContinuationTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.store, self.root = self.helper.store, self.helper.root
        self.definition = self.helper.definition(outcome="inconclusive")
        source = self.helper.script.read_text()
        source = source.replace(
            "sid=str(uuid.uuid4())", "\nif '/nodes/1-quality/' in a.context:a.outcome='accepted'\nsid=str(uuid.uuid4())")
        self.helper.script.write_text(source)
        self.helper.register(self.definition)
        self.helper.run_flow()
        self.identity = self.definition["flow_id"]
        self.original = self.store.get_flow(self.identity)
        self.source = inspect_inconclusive_workflow(self.store, self.identity)
        self.packet = {"schema_version": 1, "kind": "continue_inconclusive_quality_v1",
                       "flow_id": self.identity, "controller": self.definition["owner"],
                       "scope": "whole_package", "purpose": "Complete previously unread source scope.",
                       "source": self.source, "direct_user_authority": self.helper.refs[0]}
        self.packet_path = self.root / "continuation.json"

    def authorize(self, packet=None):
        write_json(self.packet_path, packet or self.packet)
        return continue_inconclusive_workflow(self.store, self.identity,
            authorization=evidence_ref(self.packet_path, "synthetic continuation authority"))

    def test_new_node_preserves_original_and_accepts_only_new_signature(self):
        attempts_before = self.store.dashboard()["attempts"]
        old_receipt = Path(self.original["state"]["receipt"]).read_bytes()
        with patch("subprocess.Popen", side_effect=AssertionError("authorization must not spawn")):
            result = self.authorize()
            replay = self.authorize()
        self.assertEqual(result, replay)
        self.assertEqual(attempts_before, self.store.dashboard()["attempts"])
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual(1, result["state"]["step"])
        self.assertEqual(self.original["state"]["candidate_id"], result["state"]["candidate_id"])
        self.assertEqual(old_receipt, Path(self.original["state"]["receipt"]).read_bytes())
        self.assertEqual("inconclusive", self.store.get_review(self.source["review_id"])["verdict"])
        with self.assertRaises(TaskflowError):
            self.store.quality_gate(result["state"]["candidate_id"])
        self.helper.run_flow()
        attempts = self.store.dashboard()["attempts"]
        new_attempt = next(row for row in attempts if row["attempt_id"] != self.source["attempt_id"])
        request = (Path(new_attempt["runtime_path"]) / "request.md").read_text()
        self.assertIn("one fresh independent whole review", request)
        self.assertIn("Do not infer acceptance", request)
        accepted = self.store.get_flow(self.identity)
        self.assertEqual("accepted", accepted["state"]["status"])
        self.assertNotEqual(self.source["review_id"], accepted["state"]["accepted_review_id"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))
        self.assertEqual("inconclusive", self.store.get_review(self.source["review_id"])["verdict"])
        self.assertEqual(old_receipt, Path(self.original["state"]["receipt"]).read_bytes())

    def test_purpose_entry_is_post_checkpoint_bounded_and_keeps_private_text_out_of_request(self):
        from taskflow.execution_flow import advance_workflow

        purpose = "PRIVATE_REMAINING_DUTY_SENTINEL: read the exact unresolved neutral grant and return dependencies."
        old_root = Path(self.store.get_attempt(self.source["attempt_id"])["runtime_path"])
        original_request = (old_root / "request.md").read_bytes()
        self.assertNotIn('current["quality_continuation"]["purpose"]', original_request.decode())
        self.authorize({**self.packet, "purpose": purpose})
        prepared = advance_workflow(self.store, self.identity)
        runtime = Path(prepared["state"]["current_runtime"])
        request = (runtime / "request.md").read_text()
        entry = 'current["quality_continuation"]["purpose"]'
        self.assertIn(entry, request)
        self.assertIn('current["public_first"], current["inputs_root"], current["work"] and current["scratch"]', request)
        self.assertIn("Do not output other current fields before the checkpoint", request)
        self.assertLess(request.index("compact pre-checkpoint routing metadata"), request.index("complete the public-first checkpoint"))
        self.assertIn("Only afterward use json.load on execution/current.json", request)
        self.assertIn("6000 UTF-8 bytes per tool response", request)
        self.assertIn("Do not dump the whole current object, task declaration or history", request)
        self.assertLess(request.index("complete the public-first checkpoint"), request.index(entry))
        self.assertLess(request.index("print its actual SHA256 anchor"), request.index(entry))
        self.assertNotIn(purpose, request)
        staged = json.loads((runtime / "inputs/execution/current.json").read_text())
        self.assertEqual(purpose, staged["quality_continuation"]["purpose"])
        self.assertEqual(self.source["candidate_binding"]["candidate_id"], staged["candidate_id"])
        self.assertEqual(original_request, (old_root / "request.md").read_bytes())
        self.assertEqual([], self.store.list_attempts(prepared["state"]["current_work_id"]))
        self.assertEqual(prepared, advance_workflow(self.store, self.identity))

    def test_binding_and_authority_mismatch_do_not_write(self):
        before = self.store.get_flow(self.identity)
        for changes in ({"controller": "foreign-controller"}, {"scope": "affected"},
                        {"purpose": ""}, {"source": {**self.source, "revision": 0}},
                        {"source": {**self.source, "attempt_id": "other-attempt"}}):
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                self.authorize({**self.packet, **changes})
            self.assertEqual(before, self.store.get_flow(self.identity))

    def test_original_artifact_drift_fails_closed(self):
        target = Path(self.source["evidence"][0]["path"])
        target.write_text(target.read_text() + "\n")
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual(self.original, self.store.get_flow(self.identity))

    def test_original_receipt_drift_fails_closed(self):
        target = Path(self.original["state"]["receipt"])
        data = json.loads(target.read_text()); data["exception"] = None
        write_json(target, data)
        with self.assertRaises(TaskflowError):
            self.authorize()

    def test_current_candidate_drift_fails_closed(self):
        candidate = self.store.get_candidate(self.source["candidate_binding"]["candidate_id"])
        Path(candidate["files"][0]["path"]).write_text("Changed source.")
        with self.assertRaises(TaskflowError):
            self.authorize()

    def test_reserved_new_node_does_not_get_overwritten(self):
        target = Path(self.definition["directory"]) / "nodes/1-quality"
        target.mkdir()
        with self.assertRaises(TaskflowError):
            self.authorize()

    def test_authority_drift_on_replay_fails_closed(self):
        self.authorize()
        self.helper.authority.write_text("changed synthetic authority")
        with self.assertRaises(TaskflowError):
            self.authorize()

    def test_frozen_dependency_drift_prevents_new_review(self):
        original_script = self.helper.script.read_text()
        self.helper.script.write_text(original_script.replace("a.outcome='accepted'", "a.outcome='inconclusive'"))
        # This intentionally changes a frozen dependency and must fail before any new model.
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_affected_inconclusive_whole_followup_closes_actual_repair(self):
        helper = flow_tests.ContinuationTests(); helper.setUp()
        self.addCleanup(helper.doCleanups)
        definition = helper.definition(identity="repaired", outcome="revision_required")
        definition["roles"]["recheck"]["runner"]["argv"][
            definition["roles"]["recheck"]["runner"]["argv"].index("--outcome") + 1] = "inconclusive"
        helper.script.write_text(helper.script.read_text().replace("sid=str(uuid.uuid4())",
            "\nif a.action=='quality' and '/nodes/0-quality/' not in a.context:a.outcome='accepted'\nsid=str(uuid.uuid4())"))
        helper.register(definition); helper.run_flow()
        identity = definition["flow_id"]
        original = helper.store.get_flow(identity)
        self.assertEqual("recheck", original["state"]["stage"])
        source = inspect_inconclusive_workflow(helper.store, identity)
        packet = {**self.packet, "flow_id": identity, "controller": definition["owner"],
                  "source": source, "direct_user_authority": helper.refs[0]}
        target = helper.root / "continue.json"; write_json(target, packet)
        continue_inconclusive_workflow(helper.store, identity,
            authorization=evidence_ref(target, "synthetic repaired continuation"))
        helper.run_flow()
        current = helper.store.get_flow(identity)
        self.assertEqual("accepted", current["state"]["status"])
        for defect_id in original["state"]["repaired_defects"]:
            defect = helper.store.get_defect(defect_id)
            self.assertEqual("closed", defect["state"])
            self.assertEqual(current["state"]["accepted_review_id"], defect["closure_review_id"])

    def test_pending_work_prevents_continuation(self):
        self.store.enqueue("pending-work", self.definition["task_id"],
            self.source["candidate_binding"]["candidate_id"], "quality", "fixture-v1",
            intent_id="pending-intent", purpose="synthetic pending sibling", request_digest="0" * 64,
            dispatch_owner=self.definition["owner"], evidence=self.helper.refs)
        with self.assertRaises(TaskflowError):
            self.authorize()

    def parent(self):
        return {"definition": {"kind": "author_quality_v1", **{
                    key: self.definition[key] for key in ("task_id", "owner", "policy_id")}},
                "state": {"child_flow_id": self.identity,
                          "candidate_id": self.definition["initial"]["candidate_id"]}}

    def previous(self):
        return {"stage": "child_quality", "reason": self.original["state"]["exception"],
                "child_receipt": self.original["state"]["receipt"]}

    def test_parent_dependency_waits_for_actual_new_whole_acceptance(self):
        self.authorize()
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(self.store, self.parent(), self.previous())
        self.helper.run_flow()
        proof = resolved_inconclusive_dependency(self.store, self.parent(), self.previous())
        self.assertEqual("accepted", proof["status"])
        self.assertEqual(self.source, proof["inconclusive_quality_continuation"]["source"])
        parent = self.parent(); parent["definition"]["owner"] = "foreign-controller"
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(self.store, parent, self.previous())
        previous = self.previous(); previous["reason"] = {"reason": "different exception"}
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(self.store, self.parent(), previous)

    def test_archive_drift_is_not_replaced(self):
        binding = Path(self.source["evidence"][4]["path"])
        original = json.loads(binding.read_text()); original["intent_id"] = "foreign-intent"
        write_json(binding, original)
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual("foreign-intent", json.loads(binding.read_text())["intent_id"])

    def test_retired_cli_does_not_dispatch_or_modify_existing_records(self):
        from taskflow import execution

        events = self.store.events()
        for command in ("flow-inconclusive-inspect", "flow-inconclusive-continue"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                execution.parser().parse_args([command, "--flow-id", self.identity])
            self.assertEqual(2, error.exception.code)
        self.assertEqual(events, self.store.events())
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_baseline_parent_reception_uses_existing_lifecycle_resolution(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_old_capacity_cursor_is_retained_only_in_original_event(self):
        state = dict(self.original["state"])
        state["repair_capacity_continuation"] = {"kind": "bounded_repair_capacity_v1", "maximum_additional_attempts": 10}
        state["repair_capacity_attempt_ordinal"] = 1
        old = self.store.update_flow(self.identity, self.original["revision"], state,
            {"kind": "synthetic_completed_capacity_cursor_fixture"})
        self.source = inspect_inconclusive_workflow(self.store, self.identity)
        self.packet["source"] = self.source
        result = self.authorize()
        self.assertNotIn("repair_capacity_continuation", result["state"])
        self.assertNotIn("repair_capacity_attempt_ordinal", result["state"])
        with self.store._db() as db:
            event = json.loads(db.execute("SELECT data FROM events WHERE seq=?", (old["event_seq"],)).fetchone()[0])
        self.assertEqual(10, event["state"]["repair_capacity_continuation"]["maximum_additional_attempts"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_claimed_sibling_prevents_continuation_without_reclaim(self):
        self.store.enqueue("claimed-work", self.definition["task_id"],
            self.source["candidate_binding"]["candidate_id"], "quality", "fixture-v1",
            intent_id="claimed-intent", purpose="synthetic uncertain sibling", request_digest="0" * 64,
            dispatch_owner=self.definition["owner"], evidence=self.helper.refs)
        claimed = self.store.claim("synthetic-worker", ["claimed-work"])
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual(claimed, self.store.get_attempt(claimed["attempt_id"]))
        self.assertEqual(self.original, self.store.get_flow(self.identity))

    def test_formal_ledger_footprint_without_work_row_still_blocks(self):
        # Deliberately incomplete synthetic ledger, never real task data.
        with self.store._db(True) as db:
            self.store._event(db, "dispatch_intent", {"task_id": self.definition["task_id"], "role": "solver"})
        with self.assertRaises(TaskflowError):
            self.authorize()
        self.assertEqual(self.original, self.store.get_flow(self.identity))

    def repaired_followup(self):
        helper = flow_tests.ContinuationTests(); helper.setUp()
        self.addCleanup(helper.doCleanups)
        definition = helper.definition(outcome="inconclusive")
        helper.script.write_text(helper.script.read_text().replace("sid=str(uuid.uuid4())",
            "\nif a.action=='quality' and '/nodes/1-quality/' in a.context:a.outcome='revision_required'\nsid=str(uuid.uuid4())"))
        helper.register(definition); helper.run_flow()
        original = helper.store.get_flow(definition["flow_id"])
        source = inspect_inconclusive_workflow(helper.store, definition["flow_id"])
        target = helper.root / "followup.json"
        write_json(target, {**self.packet, "flow_id": definition["flow_id"], "source": source,
                           "direct_user_authority": helper.refs[0]})
        continue_inconclusive_workflow(helper.store, definition["flow_id"],
            authorization=evidence_ref(target, "synthetic independently reproduced P1 regression"))
        helper.run_flow()
        child = helper.store.get_flow(definition["flow_id"])
        parent = {"definition": {"kind": "author_quality_v1", **{
            key: definition[key] for key in ("task_id", "owner", "policy_id")}},
            "state": {"child_flow_id": definition["flow_id"], "candidate_id": definition["initial"]["candidate_id"]}}
        previous = {"stage": "child_quality", "reason": original["state"]["exception"],
                    "child_receipt": original["state"]["receipt"]}
        self.assertEqual("accepted", child["state"]["status"])
        return helper, child, parent, previous

    def test_whole_defects_actual_repair_affected_acceptance_returns_to_parent(self):
        helper, child, parent, previous = self.repaired_followup()
        count = len(helper.store.dashboard()["attempts"])
        proof = resolved_inconclusive_dependency(helper.store, parent, previous)
        path = proof["inconclusive_quality_continuation"]["accepted_path"]
        self.assertEqual(1, len(path["repairs"]))
        self.assertEqual(child["state"]["candidate_id"], path["accepted_candidate_binding"]["candidate_id"])
        self.assertFalse(path["quality_gate_authorized"])
        self.assertNotEqual(path["whole_review_id"], path["final_review_id"])
        self.assertEqual(proof, resolved_inconclusive_dependency(helper.store, parent, previous))
        self.assertEqual(count, len(helper.store.dashboard()["attempts"]))
        # The synthetic route-only fixture has no canonical native composition;
        # parent transport must not turn it into formal whole acceptance.
        with self.assertRaises(TaskflowError):
            helper.store.quality_gate(child["state"]["candidate_id"])

    def test_repaired_followup_unclosed_responsibility_is_rejected(self):
        helper, child, parent, previous = self.repaired_followup()
        defect_id = child["state"]["repaired_defects"][0]
        defect = helper.store.get_defect(defect_id); defect["state"] = "repaired"
        with helper.store._db(True) as db:
            db.execute("UPDATE defects SET state='repaired',data=? WHERE id=?", (json.dumps(defect), defect_id))
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)

    def test_repaired_followup_wrong_ancestor_and_changed_payload_are_rejected(self):
        helper, child, parent, previous = self.repaired_followup()
        candidate = helper.store.get_candidate(child["state"]["candidate_id"])
        path = Path(candidate["files"][0]["path"]); before = path.read_bytes(); path.write_text("changed")
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)
        path.write_bytes(before)
        candidate["parent_id"] = None
        with helper.store._db(True) as db:
            db.execute("UPDATE candidates SET parent_id=NULL,data=? WHERE id=?", (json.dumps(candidate), candidate["candidate_id"]))
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)

    def test_repaired_followup_wrong_sid_and_original_whole_drift_are_rejected(self):
        helper, child, parent, previous = self.repaired_followup()
        review = helper.store.get_review(child["state"]["accepted_review_id"])
        attempt = helper.store.get_attempt(review["attempt_id"])
        changed = {**attempt, "cli_sid": "foreign-signer"}
        with helper.store._db(True) as db:
            db.execute("UPDATE attempts SET data=? WHERE id=?", (json.dumps(changed), attempt["attempt_id"]))
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)
        with helper.store._db(True) as db:
            db.execute("UPDATE attempts SET data=? WHERE id=?", (json.dumps(attempt), attempt["attempt_id"]))
        proof = resolved_inconclusive_dependency(helper.store, parent, previous)
        anchor_id = proof["inconclusive_quality_continuation"]["accepted_path"]["whole_review_id"]
        anchor = helper.store.get_attempt(helper.store.get_review(anchor_id)["attempt_id"])
        report = Path(anchor["runtime_path"]) / "artifacts/adoption-readiness.json"
        report.write_text(report.read_text() + "\n")
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)

    def finish_actual_parent(self, store, parent, authority):
        from taskflow.execution_flow import run_workflows
        from taskflow.execution_lifecycle import resume_path_transport, run_lifecycle

        child_id = parent["state"]["child_flow_id"]
        source = inspect_inconclusive_workflow(store, child_id)
        old_receipt = Path(parent["state"]["receipt"]).read_bytes()
        target = Path(parent["definition"]["directory"]).parent / "actual-parent-followup.json"
        write_json(target, {**self.packet, "flow_id": child_id,
                           "controller": parent["definition"]["owner"], "source": source,
                           "direct_user_authority": authority})
        continue_inconclusive_workflow(store, child_id,
            authorization=evidence_ref(target, "synthetic registered-parent P1 regression"))
        run_workflows(store, [child_id], allow_model=True, poll_seconds=.01)
        child = store.get_flow(child_id)
        self.assertEqual("accepted", child["state"]["status"], child["state"])
        self.assertEqual("recheck", child["state"]["stage"])
        resolution = {"action": "resume_resolved_quality_dependency",
                      "reason": "Receive this actual whole/repair/independent recheck path.", "evidence": [authority]}
        resume_path_transport(store, parent["flow_id"], resolution)
        accepted = run_lifecycle(store, parent["flow_id"], allow_model=True, poll_seconds=.01)
        self.assertEqual("accepted", accepted["state"]["status"], accepted["state"])
        self.assertEqual(child["state"]["candidate_id"], accepted["state"]["result"]["candidate_id"])
        self.assertEqual(child["state"]["accepted_review_id"], accepted["state"]["result"]["accepted_review_id"])
        count = len(store.dashboard()["attempts"])
        self.assertEqual(accepted, run_lifecycle(store, parent["flow_id"], allow_model=True, poll_seconds=.01))
        with self.assertRaises(TaskflowError):
            resume_path_transport(store, parent["flow_id"], resolution)
        self.assertEqual(count, len(store.dashboard()["attempts"]))
        self.assertEqual(old_receipt, Path(parent["state"]["receipt"]).read_bytes())
        self.assertEqual("inconclusive", store.get_review(source["review_id"])["verdict"])

    def test_registered_baseline_parent_runs_repair_recheck_and_receives_final_candidate(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_registered_author_parent_runs_repair_recheck_and_receives_final_candidate(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def custom_report_followup(self, report_name, issues_name, repair):
        helper = flow_tests.ContinuationTests(); helper.setUp()
        self.addCleanup(helper.doCleanups)
        definition = helper.definition(outcome="inconclusive")
        for name, role in definition["roles"].items():
            if name != "repair":
                role.update(report_file=report_name, issues_file=issues_name)
                role["runner"]["outputs"] = ["intake.md", issues_name, report_name]
        script = helper.script.read_text().replace("adoption-readiness.json", report_name).replace("issues.json", issues_name)
        outcome = "revision_required" if repair else "accepted"
        helper.script.write_text(script.replace("sid=str(uuid.uuid4())",
            "\nif a.action=='quality' and '/nodes/1-quality/' in a.context:a.outcome=" + repr(outcome) + "\nsid=str(uuid.uuid4())"))
        helper.register(definition); helper.run_flow()
        original = helper.store.get_flow(definition["flow_id"])
        source = inspect_inconclusive_workflow(helper.store, definition["flow_id"])
        target = helper.root / "custom-followup.json"
        write_json(target, {**self.packet, "flow_id": definition["flow_id"], "source": source,
                           "direct_user_authority": helper.refs[0]})
        continue_inconclusive_workflow(helper.store, definition["flow_id"], authorization=evidence_ref(target, "synthetic P2 regression"))
        helper.run_flow()
        child = helper.store.get_flow(definition["flow_id"])
        self.assertEqual("accepted", child["state"]["status"], child["state"])
        parent = {"definition": {"kind": "author_quality_v1", **{
            key: definition[key] for key in ("task_id", "owner", "policy_id")}},
            "state": {"child_flow_id": definition["flow_id"], "candidate_id": definition["initial"]["candidate_id"]}}
        previous = {"stage": "child_quality", "reason": original["state"]["exception"],
                    "child_receipt": original["state"]["receipt"]}
        return helper, child, parent, previous

    def test_declared_custom_and_nested_reports_support_whole_and_repaired_paths(self):
        for report, issues in [("custom-review.json", "custom-issues.json"), ("review/current.json", "review/issues.json")]:
            for repair in [False, True]:
                with self.subTest(report=report, repair=repair):
                    helper, child, parent, previous = self.custom_report_followup(report, issues, repair)
                    result = resolved_inconclusive_dependency(helper.store, parent, previous)
                    self.assertEqual(child["state"]["candidate_id"], result["inconclusive_quality_continuation"]["accepted_path"]["accepted_candidate_binding"]["candidate_id"])

    def test_missing_custom_report_is_not_replaced_by_default_file(self):
        helper, child, parent, previous = self.custom_report_followup("custom-review.json", "custom-issues.json", False)
        attempt = helper.store.get_attempt(helper.store.get_review(child["state"]["accepted_review_id"])["attempt_id"])
        artifacts = Path(attempt["runtime_path"]) / "artifacts"
        (artifacts / "custom-review.json").rename(artifacts / "adoption-readiness.json")
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)
        self.assertTrue((artifacts / "adoption-readiness.json").is_file())

    def test_custom_report_drift_and_path_escape_remain_rejected(self):
        from taskflow.execution_store import ExecutionStore
        helper, child, parent, previous = self.custom_report_followup("custom-review.json", "custom-issues.json", False)
        review = helper.store.get_review(child["state"]["accepted_review_id"])
        with helper.store._db() as db, self.assertRaises(TaskflowError):
            ExecutionStore._composition_review(db, review, "whole_package", report_file="../custom-review.json")
        attempt = helper.store.get_attempt(review["attempt_id"])
        report = Path(attempt["runtime_path"]) / "artifacts/custom-review.json"
        report.write_text(report.read_text() + "\n")
        with self.assertRaises(TaskflowError):
            resolved_inconclusive_dependency(helper.store, parent, previous)

    def test_default_path_keeps_three_argument_review_reader_compatibility(self):
        from taskflow.execution_store import ExecutionStore
        helper, child, parent, previous = self.repaired_followup()
        original = ExecutionStore._composition_review
        def legacy_reader(db, review, scope):
            return original(db, review, scope)
        with patch.object(ExecutionStore, "_composition_review", side_effect=legacy_reader):
            result = resolved_inconclusive_dependency(helper.store, parent, previous)
        self.assertEqual(child["state"]["accepted_review_id"], result["inconclusive_quality_continuation"]["accepted_review_id"])
