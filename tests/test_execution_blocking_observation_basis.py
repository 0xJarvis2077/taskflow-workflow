"""Blocking private findings may use bound public observations as supplements.

All subprocesses are local neutral fixtures, not external model calls or native
permission proofs. Completed collections are produced by the actual runner.
"""

from pathlib import Path
import unittest

from taskflow import execution, execution_runner as runner
from taskflow.contracts import read_json, sha256
from taskflow.execution_flow import advance_workflow
from tests import test_execution_flow as flow_fixtures


QUALITY_FIXTURE = r'''
import argparse,hashlib,json,pathlib,sys,uuid
p=argparse.ArgumentParser()
for name in ('context','inputs','action','scope','outcome','delay','variant'):
 p.add_argument('--'+name)
a=p.parse_args();request=sys.stdin.read();inputs=pathlib.Path(a.inputs)
ctx=json.loads(pathlib.Path(a.context).read_text())
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def save(name,value):pathlib.Path(name).write_text(json.dumps(value))
print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
binding={k:ctx[k] for k in ['candidate_id','intent_id','content_digest','input_digest','evaluation_digest']}
observed='actual-public-work/delivery.txt'
rows=[]
for identity,kind,path in [('PRIVATE-REFERENCE','reference','draft/evaluation/answer.md'),('PRIVATE-RUBRIC','rubric','draft/evaluation/rubric.md')]:
 basis='execution/task.json' if a.variant=='metadata' else path
 row={'id':identity,'kind':kind,'blocking':True,'status':'open','responsibility':'Current private support must match the retained public responsibility.','affected_paths':[path],'evidence':[{'path':basis,'sha256':digest(inputs/basis),'location':'current field 1'},{'path':observed,'sha256':digest(inputs/observed),'location':'line 1'}]}
 rows.append(row)
blocks=[row['id'] for row in rows];outcome='revision_required'
if a.variant=='sole_observed':rows[0]['evidence']=rows[0]['evidence'][1:]
if a.variant=='target':rows[0]['affected_paths']=[observed]
if a.variant=='false_blocking':rows[0]['blocking']=False
if a.variant=='integer_blocking':rows[0]['blocking']=1
if a.variant=='missing_blocking':rows[0].pop('blocking')
if a.variant=='membership':blocks=blocks[1:]
if a.variant=='inconclusive':outcome='inconclusive'
if a.variant=='accepted':outcome='accepted'
if a.variant=='observed_sha':rows[0]['evidence'][1]['sha256']='0'*64
if a.variant=='current_sha':rows[0]['evidence'][0]['sha256']='0'*64
if a.variant=='empty_location':rows[0]['evidence'][1]['location']=' '
if a.variant in ('material','claim','unknown'):rows[0]['kind']=a.variant
route={'schema_version':1,**binding,'signer_role':'independent_quality','independent_of_current_author':True,'participated_in_production':False,'scope':a.scope,'outcome':outcome,'blocking_issue_ids':blocks,'covered_defect_ids':[],'reason':'Two concrete private findings require actual repair; observed delivery is only supplemental.'}
save('adoption-readiness.json',{'execution_routing':route,'candidate_business_quality':{'status':outcome},'native_signoff':{'dimensions':{'business':{'affirmed':False},'artifact':{'affirmed':True},'calculation':{'affirmed':False}}}})
save('issues.json',{'execution_binding':binding,'issues':rows})
pathlib.Path('intake.md').write_text('Neutral independent fixture: current private defects with supplemental public observations.')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Original fixture findings delivered.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
'''


class BlockingObservationBasisTests(unittest.TestCase):
    def setUp(self):
        self.case = flow_fixtures.ContinuationTests(
            "test_one_registration_runs_quality_actual_repair_recheck_receipt")
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.store = self.case.store
        self.script = self.case.root / "blocking-quality.py"
        self.script.write_text(QUALITY_FIXTURE, encoding="utf-8")
        self.serial = 0

    def completed_quality(self, variant="valid"):
        self.serial += 1
        definition = self.case.definition("bounded-" + str(self.serial))
        quality = definition["roles"]["quality"]
        quality["predecessor_inputs"] = [{
            "tree": "{{public_archive}}/supplemental/trees/work",
            "kind": "submission", "target": "actual-public-work"}]
        quality["runner"]["dependencies"].append(str(self.script))
        quality["runner"]["argv"][1] = str(self.script)
        quality["runner"]["argv"] += ["--variant", variant]
        predecessor = self.case.predecessor(definition, completed=True)
        record = self.case.register(definition)
        record = advance_workflow(self.store, record["flow_id"])
        self.assertTrue(record["state"]["initialized"])
        record = advance_workflow(self.store, record["flow_id"])
        self.assertEqual("active", record["state"]["status"])
        work_id = record["state"]["current_work_id"]
        execution.run_worker(self.store, "neutral-quality", True, [work_id])
        attempt = next(row for row in self.store.dashboard()["attempts"]
                       if row["work_id"] == work_id)
        self.assertEqual("completed", attempt["state"])
        self.assertNotEqual(predecessor["cli_sid"], attempt["cli_sid"])
        root = Path(attempt["runtime_path"])
        manifest = runner._validate(root)
        runner._validated_collection(root, manifest)
        originals = {name: sha256((root / "artifacts" / name).read_bytes())
                     for name in ("adoption-readiness.json", "issues.json")}
        return record, attempt, root, originals

    def consume(self, variant):
        record, attempt, root, originals = self.completed_quality(variant)
        received = advance_workflow(self.store, record["flow_id"])
        self.assertEqual(originals, {name: sha256((root / "artifacts" / name).read_bytes())
                                     for name in originals})
        return received, attempt, root

    def test_two_real_blocking_findings_are_stored_and_original_repair_is_prepared_once(self):
        record, attempt, root = self.consume("valid")
        state = record["state"]
        self.assertEqual("active", state["status"])
        self.assertEqual("repair", state["stage"])
        self.assertIsNone(state["current_work_id"])
        self.assertEqual(2, len(state["defects"]))
        self.assertEqual({"reference", "rubric"}, {row["kind"] for row in state["defects"]})
        source = read_json(root / "artifacts/adoption-readiness.json")
        self.assertEqual("revision_required", source["execution_routing"]["outcome"])
        self.assertEqual([False, True, False], [source["native_signoff"]["dimensions"][key]["affirmed"]
                                               for key in ("business", "artifact", "calculation")])
        review = self.store.get_review(state["defects"][0]["source_review_id"])
        self.assertEqual("defects", review["verdict"])
        self.assertTrue(review["whole_package"])
        for item in state["defects"]:
            defect = self.store.get_defect(item["defect_id"])
            self.assertEqual("open", defect["state"])
            self.assertEqual(attempt["attempt_id"], review["attempt_id"])
            self.assertTrue(any("/supplemental/trees/work/" in ref["path"] for ref in defect["evidence"]))
            self.assertTrue(all(not name.startswith("actual-public-") for name in item["affected_paths"]))
        prepared = advance_workflow(self.store, record["flow_id"])
        work_id = prepared["state"]["current_work_id"]
        self.assertEqual("ready", self.store.get_work(work_id)["state"])
        self.assertEqual("repair", self.store.get_work(work_id)["role"])
        self.assertFalse(any(row["work_id"] == work_id for row in self.store.dashboard()["attempts"]))
        self.assertEqual(prepared, advance_workflow(self.store, record["flow_id"]))
        repair_context = read_json(Path(prepared["state"]["current_runtime"]) / "inputs/execution/current.json")
        self.assertEqual({row["defect_id"] for row in state["defects"]}, set(repair_context["defect_ids"]))

    def test_bound_current_declaration_is_a_valid_basis_but_observed_never_is_sole_basis(self):
        record, _, _ = self.consume("metadata")
        self.assertEqual("repair", record["state"]["stage"])
        self.assertEqual(2, len(record["state"]["defects"]))
        record, _, _ = self.consume("sole_observed")
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("supplemental", record["state"]["exception"]["reason"])
        self.assertEqual([], record["state"]["defects"])

    def test_observed_target_and_ineligible_issue_kinds_remain_rejected(self):
        for variant in ("target", "material", "claim", "unknown"):
            with self.subTest(variant=variant):
                record, _, _ = self.consume(variant)
                self.assertEqual("exception", record["state"]["status"])
                self.assertEqual([], record["state"]["defects"])
                reason = record["state"]["exception"]["reason"]
                self.assertIn("Unknown issue kind" if variant == "unknown" else "Observed", reason)

    def test_true_blocking_membership_and_revision_required_outcome_are_all_required(self):
        for variant in ("false_blocking", "integer_blocking", "missing_blocking", "membership", "inconclusive", "accepted"):
            with self.subTest(variant=variant):
                record, _, _ = self.consume(variant)
                self.assertEqual("exception", record["state"]["status"])
                self.assertEqual([], record["state"]["defects"])
                reason = record["state"]["exception"]["reason"]
                self.assertIn("Accepted review cannot" if variant == "accepted" else "Observed evidence", reason)

    def test_actual_current_and_observed_sha_and_nonblank_locations_remain_required(self):
        for variant in ("observed_sha", "current_sha", "empty_location"):
            with self.subTest(variant=variant):
                record, _, _ = self.consume(variant)
                self.assertEqual("exception", record["state"]["status"])
                self.assertEqual([], record["state"]["defects"])
                reason = record["state"]["exception"]["reason"]
                self.assertIn("evidence location" if variant == "empty_location" else "wrong SHA", reason)


if __name__ == "__main__":
    unittest.main()
