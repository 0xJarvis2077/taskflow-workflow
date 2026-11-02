"""Bounded coverage composes real neutral whole findings and affected closure."""

import json
import sys
import uuid
import unittest
from pathlib import Path

from taskflow import execution
from taskflow.contracts import TaskflowError
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flow_fixtures


class CompositeQualityTests(unittest.TestCase):
    def make_closed_flow(self, *, extra=False, association=True, bad_sha=False):
        case = flow_fixtures.ContinuationTests("test_running_predecessor_wait_is_stable_without_process_recovery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        definition = case.definition("neutral")
        baseline = definition["initial"]["candidate_root"]
        prefix = '''
import pathlib,json,hashlib
def signed_report(route,ctx,inputs):
 report={'execution_routing':route,'limits':['Unknown remote production and served identity remain unknown.']}
 if route['scope']=='affected':
  baseline=pathlib.Path(BASELINE)
  declaration=json.loads((baseline/'task.json').read_text())
  changed=[];evidence=[]
  for item in declaration['files']:
   name=item['path'];old=hashlib.sha256((baseline/name).read_bytes()).hexdigest();new=hashlib.sha256((inputs/name).read_bytes()).hexdigest()
   if old!=new:
    changed.append({'path':name,'old':old,'new':new})
    evidence.append({'path':name,'sha256':new,'location':'actual current private byte and necessary association'})
  affected=[x['path'] for x in changed]
  if not ASSOCIATION:
   affected=[x for x in affected if x=='draft/evaluation/answer.md']
   changed=[x for x in changed if x['path']=='draft/evaluation/answer.md']
  if BAD_SHA:evidence[-1]['sha256']='0'*64
  report['coverage']={'changed_files':changed}
  report['defect_resolution']={'affected_paths':affected,'evidence':evidence}
 return report
'''.replace("BASELINE", repr(baseline)).replace("ASSOCIATION", repr(association)).replace("BAD_SHA", repr(bad_sha))
        script = flow_fixtures.ROLE_FIXTURE.replace("{'execution_routing':route}", "signed_report(route,ctx,inputs)")
        script = script.replace("save(root/'task.json',task)",
                                "(root/'task.json').write_bytes((inputs/'execution/task.json').read_bytes())")
        if extra:
            extra_repair = """
 other='draft/evaluation/rubric.md';before_other=digest(root/other);(root/other).write_text('Actual associated private rubric clarification.\\n')
 record=json.loads(pathlib.Path('repair-record.json').read_text());record['changes'].append({'path':other,'before_sha256':before_other,'after_sha256':digest(root/other)});save('repair-record.json',record)
"""
            script = script.replace(" pathlib.Path('handoff.md').write_text", extra_repair + " pathlib.Path('handoff.md').write_text")
        case.script.write_text(prefix + script)
        case.register(definition)
        case.run_flow()
        flow = case.store.get_flow(definition["flow_id"])
        self.assertEqual("accepted", flow["state"]["status"])
        history = [row for row in flow["state"]["history"] if row["kind"] == "independent_review"]
        return {"case": case, "store": case.store, "flow": flow,
                "candidate": flow["state"]["candidate_id"], "whole": history[0]["review_id"],
                "recheck": flow["state"]["accepted_review_id"],
                "defects": flow["state"]["repaired_defects"], "refs": case.refs}

    def compose(self, value, **changes):
        return value["store"].compose_repaired_quality(
            changes.get("candidate_id", value["candidate"]), changes.get("whole_review_id", value["whole"]),
            changes.get("recheck_id", value["recheck"]), changes.get("closed_defect_ids", value["defects"]),
            evidence=value["refs"])

    def later_negative(self, value):
        store, root = value["store"], value["case"].root
        script = root / "negative.py"
        script.write_text("import json,pathlib\n"
                          "pathlib.Path('negative.json').write_text('{\\\"reason\\\":\\\"new neutral independent conflict\\\"}')\n"
                          "print(json.dumps({'type':'thread.started','thread_id':'" + str(uuid.uuid4()) + "'}))\n"
                          "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'New neutral conflict.'}}))\n"
                          "print(json.dumps({'type':'turn.completed'}))\n")
        work = root / "negative-work"
        work.mkdir()
        runtime = root / "negative-runtime"
        execution.enqueue_spec(store, {"work": {"work_id": "new-negative", "task_id": "neutral",
            "candidate_id": value["candidate"], "role": "quality", "policy_id": "fixture-v1",
            "intent_id": "new-negative-intent", "purpose": "neutral later independent conflict", "evidence": value["refs"]},
            "runner": {"mode": "test_fixture", "role": "quality", "intent_id": "new-negative-intent",
                       "policy_version": "fixture-v1", "model": "fixture-cli", "reasoning_effort": "fixture",
                       "timeout_seconds": 15, "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), str(script)],
                       "inputs": [], "dependencies": [str(script)], "outputs": ["negative.json"],
                       "request_sections": [{"kind": "operation", "text": "Inspect a neutral new conflict."}]}}, runtime)
        attempt = execution.run_worker(store, "neutral-negative")
        self.assertEqual("completed", attempt["state"])
        return store.record_review("later-conflict", value["candidate"], attempt["attempt_id"],
                                    verdict="inconclusive", scope="current related package", whole_package=True,
                                    evidence=[evidence_ref(runtime / "artifacts/negative.json", "actual neutral negative")])

    def test_exact_private_repair_composes_without_new_whole_signature(self):
        value = self.make_closed_flow()
        with self.assertRaises(TaskflowError):
            value["store"].quality_gate(value["candidate"])
        result = self.compose(value)
        self.assertFalse(result["whole_package"])
        self.assertTrue(result["snapshot_bound_acceptance"])
        self.assertTrue(result["derived"])
        graph = result["composite_quality_coverage"]
        self.assertEqual(value["whole"], graph["whole_review_id"])
        self.assertEqual(value["recheck"], graph["recheck_id"])
        self.assertEqual(1, len(graph["private_changes"]))
        self.assertTrue(graph["private_changes"][0]["original_issue_affected"])
        self.assertEqual(["Unknown remote production and served identity remain unknown."], graph["source_limits"]["whole"]["limits"])
        fresh = value["store"].get_review(value["recheck"])
        self.assertEqual(fresh["completed_seq"], result["completed_seq"])
        self.assertEqual(fresh["attempt_id"], result["attempt_id"])
        self.assertEqual(value["recheck"], value["store"].get_flow(value["flow"]["flow_id"])["state"]["accepted_review_id"])
        self.assertEqual(result, self.compose(value))
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))
        self.assertEqual(result, value["store"].assert_review_current(result["review_id"]))
        self.assertEqual([], value["store"].dashboard()["decisions"])

    def test_exact_signed_necessary_association_carries_original_pointer_and_sha(self):
        value = self.make_closed_flow(extra=True)
        result = self.compose(value)
        changes = result["composite_quality_coverage"]["private_changes"]
        extra = next(row for row in changes if not row["original_issue_affected"])
        self.assertEqual("draft/evaluation/rubric.md", extra["path"])
        self.assertEqual(["/defect_resolution/evidence/1", "/defect_resolution/affected_paths/1", "/coverage/changed_files/1"],
                         [row["json_pointer"] for row in extra["fresh_signed_evidence"]])
        self.assertEqual(extra["new_sha256"], extra["fresh_signed_evidence"][0]["actual_value"]["sha256"])

    def test_unlisted_private_change_does_not_inherit_old_whole_coverage(self):
        value = self.make_closed_flow(extra=True, association=False)
        with self.assertRaisesRegex(TaskflowError, "outside original issues"):
            self.compose(value)

    def test_signed_changed_path_without_current_sha_is_rejected(self):
        value = self.make_closed_flow(extra=True, bad_sha=True)
        with self.assertRaisesRegex(TaskflowError, "current SHA evidence"):
            self.compose(value)

    def test_exact_source_and_complete_closed_defect_set_are_required(self):
        value = self.make_closed_flow()
        for defects in ([], [*value["defects"], *value["defects"]], ["foreign-defect"]):
            with self.subTest(defects=defects), self.assertRaises(TaskflowError):
                self.compose(value, closed_defect_ids=defects)
        with self.assertRaises(TaskflowError):
            self.compose(value, whole_review_id=value["recheck"])
        with self.assertRaises(TaskflowError):
            self.compose(value, candidate_id=value["case"].store.get_review(value["whole"])["candidate_id"])

    def test_later_negative_before_composition_cannot_be_washed_by_new_record_time(self):
        value = self.make_closed_flow()
        self.later_negative(value)
        with self.assertRaisesRegex(TaskflowError, "later independent negative"):
            self.compose(value)

    def test_later_negative_blocks_existing_composite_and_idempotent_reacceptance(self):
        value = self.make_closed_flow()
        result = self.compose(value)
        self.later_negative(value)
        with self.assertRaises(TaskflowError):
            value["store"].quality_gate(value["candidate"])
        with self.assertRaises(TaskflowError):
            value["store"].assert_review_current(result["review_id"])
        with self.assertRaises(TaskflowError):
            self.compose(value)

    def test_old_original_evidence_drift_blocks_composite_gate(self):
        value = self.make_closed_flow()
        self.compose(value)
        review = value["store"].get_review(value["whole"])
        Path(review["evidence"][0]["path"]).write_text("Changed old signature after composition")
        with self.assertRaises((TaskflowError, ValueError)):
            value["store"].quality_gate(value["candidate"])


if __name__ == "__main__":
    unittest.main()
