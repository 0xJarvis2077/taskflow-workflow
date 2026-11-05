"""Explicit assessment stages with real local subprocesses, never model calls."""

import copy
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json
from taskflow import execution_assessment as assessment_module
from taskflow.execution_assessment import (
    _permissions, _sources,
    collect_assessment_task, correct_assessment_task, prepare_assessment_task,
    register_assessment_task, run_assessment_task, seal_assessment_facts,
)
from taskflow.execution_assessment_contracts import (
    AssessmentContractError, accumulate_business_payload, business_payload, check_delivery, validate_facts, validate_score,
)
from taskflow.execution_solver import register_solver_task, run_solver_task, validate_solver_source
from tests import test_execution_solver as solver_fixtures


REVIEWER = r'''
import hashlib,json,pathlib,re,subprocess,sys
request=sys.stdin.read();mode=sys.argv[1];root=pathlib.Path.cwd().parent
stage,ordinal=root.name.rsplit('-',1);ordinal=int(ordinal)
inputs=root/'runtime/inputs'
intent=re.search(r'This invocation intent_id=([^\.]+)\.',request)[1]
sid='00000000-0000-4000-8000-000000000003' if mode=='same-solver' else '00000000-0000-4000-8000-000000000005' if mode=='wrong-score' and stage=='score' else '00000000-0000-4000-8000-000000000004'
print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
def citation(row):
 raw=(inputs/row['path']).read_bytes()
 try: location='L1' if raw.decode().splitlines() else 'B0-B0'
 except UnicodeDecodeError: location='B0-B'+str(len(raw))
 return {'path':row['path'],'sha256':row['sha256'],'location':location}
def save(value):pathlib.Path(stage+'.json').write_text(json.dumps(value))
context=json.loads((inputs/'execution/review-context.json').read_text())
assert 'criteria' not in context and 'standards' not in context
if stage=='facts':
 assert not (inputs/'draft/evaluation/rubric.md').exists()
 assert not (inputs/'draft/evaluation/answer.md').exists()
 binding={**context['binding'],'intent_id':intent}
 coverage=[]
 for row in context['sources']:
  cite=citation(row);assert hashlib.sha256((inputs/row['path']).read_bytes()).hexdigest()==row['sha256']
  coverage.append({**cite,'status':'read','limitation':''})
 status='complete';limitations=[]
 if mode=='incomplete':
  status='incomplete';coverage[0].update(status='unresolved',limitation='Neutral fixture source not completely read.');limitations=['Actual incomplete fixture reading.']
 report={'schema_version':1,'kind':'review_facts_v1','binding':binding,'status':status,'coverage':coverage,
  'facts':[{'id':'F1','statement':'Neutral fixture original factual finding; no score or private standard.',
   'evidence':[citation(context['sources'][0])]}],'limitations':limitations}
 if mode=='bad-facts' and ordinal==0:report['coverage']=report['coverage'][:-1]
 if mode=='bad-type' and ordinal==0:report['coverage'][0]['path']=[]
 if mode=='bad-location':report['facts'][0]['evidence'][0]['location']='L999999'
 if mode!='missing':save(report)
 if mode=='bad-json' and ordinal==0:pathlib.Path('facts.json').write_text('{')
else:
 assert (inputs/'draft/evaluation/rubric.md').is_file()
 assert (inputs/'execution/sealed-facts.json').is_file()
 context=json.loads((pathlib.Path.cwd()/'.taskflow-assessment-context.json').read_text())
 binding={**context['binding'],'intent_id':intent}
 facts=next(row for row in context['sources'] if row['path']=='execution/sealed-facts.json')
 projection=next(row for row in context['sources'] if row['path']=='normative/task-scoring.json')
 criteria=[{'id':row['id'],'points':row['max_points'],'max_points':row['max_points'],
  'reason':'Original neutral fixture criterion judgment, never a real model score.',
  'evidence':[citation(facts),row['source'],citation(projection)]} for row in context['criteria']]
 outcome='inconclusive' if mode=='inconclusive' else 'scored'
 total=None if outcome=='inconclusive' else sum(row['points'] for row in criteria)
 report={'schema_version':1,'kind':'review_score_v1','binding':binding,'outcome':outcome,
  'facts_sha256':context['facts_sha256'],'validity':{'local_eligible':None,'reason':'Actual served identity remains unobserved.',
   'evidence':[citation(facts)]},'criteria':criteria,'total':total,
  'limitations':['Actual fixture uncertainty.'] if outcome=='inconclusive' else []}
 if mode in ('bad-score','same-turn','regrade') and ordinal==0:report['total']=total+1
 if mode=='regrade' and ordinal>0:
  report['criteria'][0]['points']-=1;report['total']=sum(row['points'] for row in report['criteria'])
 if mode!='missing':save(report)
 if mode=='bad-score-json':pathlib.Path('score.json').write_text('{')
 if mode=='same-turn':
  command=[sys.executable,str(inputs/'execution/assessment-preflight.py'),'--directory',str(pathlib.Path.cwd()),
   '--context',str(pathlib.Path.cwd()/'.taskflow-assessment-context.json')]
  wrong=subprocess.run(command,capture_output=True,text=True);assert wrong.returncode==2,(wrong.stdout,wrong.stderr)
  assert json.loads(wrong.stderr)['score_assigned'] is False
  report['total']=sum(row['points'] for row in criteria);save(report)
  correct=subprocess.run(command,capture_output=True,text=True);assert correct.returncode==0,(correct.stdout,correct.stderr)
  print(json.dumps({'type':'item.completed','item':{'type':'command_execution','status':'completed','exit_code':0,'aggregated_output':correct.stdout}}),flush=True)
if mode=='tamper-score-context' and stage=='score':
 pathlib.Path('.taskflow-assessment-context.json').write_text('{"changed":"A fixture cannot silently rewrite its locked context."}')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral local reviewer fixture completed.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
'''


class AssessmentTaskTests(unittest.TestCase):
    def setUp(self):
        self.case = solver_fixtures.SolverTaskTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root, self.store = self.case.root, self.case.store
        solver = self.case.register()
        self.solver = run_solver_task(self.store, solver["flow_id"], True)
        self.source = validate_solver_source(self.store, solver["flow_id"])
        self.script = self.root / "reviewer.py"
        self.script.write_text(REVIEWER, encoding="utf-8")

    def spec(self, mode="review", *, identity="assessment-neutral", corrections=0):
        home = self.root / (identity + "-home")
        home.mkdir(exist_ok=True)
        runner = {"mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
            "inputs": [], "request_sections": [], "dependencies": [str(self.script)],
            "argv": [str(Path(sys.executable).resolve()), str(self.script), mode],
            "role_session": {"mode": "fresh_persistent", "home": str(home), "previous": None}}
        rubric = next(row for row in self.source["candidate"]["files"] if row["role"] == "rubric")
        declared = read_json(self.case.case.task / "task.json")["scoring"]
        return {"schema_version": 1, "flow_id": identity, "task_id": "neutral", "owner": "fixture-coordinator",
            "policy_id": "fixture-v1", "directory": str(self.root / identity), "candidate_id": "candidate-neutral",
            "candidate_root": str(self.case.case.task), "solver_task_id": self.solver["flow_id"],
            "purpose": "Actual neutral fixture reviewer purpose, not model input.", "correction_limit": corrections,
            "standards": [row["package_path"] for row in self.source["candidate"]["files"] if row["role"] in ("answer", "rubric")],
            "criteria": [{"id": row["id"], "max_points": row["max_score"],
                "source": {"path": rubric["package_path"], "sha256": rubric["sha256"], "location": "L1"}}
                for row in declared["criteria"]], "facts_runner": runner, "score_runner": copy.deepcopy(runner)}

    def register(self, mode="review", **kwargs):
        return register_assessment_task(self.store, self.spec(mode, **kwargs), self.case.case.refs)

    def run_facts(self, mode="review", **kwargs):
        record = self.register(mode, **kwargs)
        return run_assessment_task(self.store, record["flow_id"], "facts", True)

    def ready_score(self, mode="review", **kwargs):
        facts = self.run_facts(mode, **kwargs)
        self.assertEqual("valid", facts["state"]["delivery"], facts["state"])
        return seal_assessment_facts(self.store, facts["flow_id"])

    @staticmethod
    def snapshot(root):
        return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}

    def test_explicit_facts_seal_same_sid_score_and_unique_original_receipt(self):
        registered = self.register()
        with self.assertRaisesRegex(TaskflowError, "sealed"):
            prepare_assessment_task(self.store, registered["flow_id"], "score")
        attempts = len(self.store.dashboard()["attempts"])
        dry = run_assessment_task(self.store, registered["flow_id"])
        self.assertEqual("not_started", dry["state"]["execution"])
        self.assertEqual(attempts, len(self.store.dashboard()["attempts"]))
        facts = run_assessment_task(self.store, registered["flow_id"], "facts", True)
        self.assertEqual(("completed", "valid", "complete"), tuple(facts["state"][key] for key in ("execution", "delivery", "outcome")), facts["state"])
        first = facts["state"]["invocations"]["facts"][-1]
        manifest = read_json(Path(first["runtime"]) / "manifest.json")
        self.assertFalse(any(row["kind"] == "teacher" for row in manifest["inputs"]))
        self.assertIn("actual-solver-work/unselected-original.txt", {row["target"] for row in manifest["inputs"]})
        self.assertIn("actual-solver-work/scratch/scratch-original.txt", {row["target"] for row in manifest["inputs"]})
        self.assertNotIn("criteria", (Path(first["runtime"]) / "inputs/execution/review-context.json").read_text())
        sealed = seal_assessment_facts(self.store, facts["flow_id"])
        self.assertEqual(sealed["revision"], seal_assessment_facts(self.store, facts["flow_id"])["revision"])
        original = self.snapshot(Path(first["runtime"]))
        result = run_assessment_task(self.store, facts["flow_id"], "score", True)
        self.assertEqual(("completed", "valid", "scored"), tuple(result["state"][key] for key in ("execution", "delivery", "outcome")), result["state"])
        second = result["state"]["invocations"]["score"][-1]
        score_manifest=read_json(Path(second["runtime"])/"manifest.json")
        self.assertIn("normative/task-scoring.json",{row["target"] for row in score_manifest["inputs"] if row["kind"]=="teacher"})
        self.assertEqual(read_json(self.case.case.task/"task.json")["scoring"],read_json(Path(second["runtime"])/"inputs/normative/task-scoring.json"))
        self.assertEqual(first["sid"], second["sid"])
        self.assertNotEqual(first["work_id"], second["work_id"])
        self.assertEqual(original, self.snapshot(Path(first["runtime"])))
        receipt = Path(result["state"]["score_receipt"]["path"])
        raw = receipt.read_bytes()
        count = len(self.store.dashboard()["attempts"])
        self.assertEqual(result["revision"], collect_assessment_task(self.store, result["flow_id"], "score")["revision"])
        run_assessment_task(self.store, result["flow_id"], "score", True)
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))
        self.assertEqual(raw, receipt.read_bytes())
        self.assertIsNone(read_json(receipt)["validity"]["local_eligible"])

    def test_incomplete_facts_and_inconclusive_score_remain_original_business_outcomes(self):
        facts = self.run_facts("incomplete")
        self.assertEqual(("valid", "incomplete"), (facts["state"]["delivery"], facts["state"]["outcome"]))
        with self.assertRaises(TaskflowError):seal_assessment_facts(self.store, facts["flow_id"])
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store, facts["flow_id"], "facts", self.case.case.refs)

    def test_inconclusive_original_scoring_is_not_replaced_by_a_computed_score(self):
        ready = self.ready_score("inconclusive")
        result = run_assessment_task(self.store, ready["flow_id"], "score", True)
        self.assertEqual(("valid", "inconclusive"), (result["state"]["delivery"], result["state"]["outcome"]))
        self.assertIsNone(read_json(Path(result["state"]["score_receipt"]["path"]))["total"])
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store, ready["flow_id"], "score", self.case.case.refs)

    def test_facts_contract_error_can_be_corrected_only_in_its_same_context(self):
        first = self.run_facts("bad-facts", corrections=1)
        self.assertEqual(("completed", "invalid", "complete"), tuple(first["state"][key] for key in ("execution", "delivery", "outcome")))
        original = first["state"]["invocations"]["facts"][-1]
        before = self.snapshot(Path(original["runtime"]))
        count = len(self.store.dashboard()["attempts"])
        ready = correct_assessment_task(self.store, first["flow_id"], "facts", self.case.case.refs)
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))
        self.assertEqual(ready["revision"], correct_assessment_task(self.store, first["flow_id"], "facts", self.case.case.refs)["revision"])
        result = run_assessment_task(self.store, first["flow_id"], "facts", True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"])
        corrected = result["state"]["invocations"]["facts"][-1]
        request = (Path(corrected["runtime"]) / "request.md").read_text()
        self.assertNotIn("Write facts.json with exactly", request)
        self.assertIn("actual contract errors", request)
        self.assertEqual(original["sid"], corrected["sid"])
        self.assertNotEqual(original["work_id"], corrected["work_id"])
        self.assertEqual(before, self.snapshot(Path(original["runtime"])))
        sealed = seal_assessment_facts(self.store, first["flow_id"])
        self.assertEqual(str(Path(corrected["runtime"]) / "facts-seal.json"), sealed["state"]["facts_seal"]["path"])
        score = run_assessment_task(self.store, first["flow_id"], "score", True)
        self.assertEqual("valid", score["state"]["delivery"], score["state"])

    def test_scoring_contract_error_keeps_original_and_uses_one_bounded_same_sid_correction(self):
        ready = self.ready_score("bad-score", corrections=1)
        first = run_assessment_task(self.store, ready["flow_id"], "score", True)
        self.assertEqual(("completed", "invalid", "scored"), tuple(first["state"][key] for key in ("execution", "delivery", "outcome")))
        original = first["state"]["invocations"]["score"][-1]
        before = self.snapshot(Path(original["runtime"]))
        self.assertNotIn("score_receipt", first["state"])
        corrected = correct_assessment_task(self.store, ready["flow_id"], "score", self.case.case.refs)
        result = run_assessment_task(self.store, corrected["flow_id"], "score", True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"])
        self.assertEqual(original["sid"], result["state"]["invocations"]["score"][-1]["sid"])
        request = (Path(result["state"]["invocations"]["score"][-1]["runtime"]) / "request.md").read_text()
        self.assertNotIn("Write score.json with exactly", request)
        self.assertNotIn("--context-json", request)
        self.assertIn("business_lock", request)
        self.assertEqual(before, self.snapshot(Path(original["runtime"])))
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store, ready["flow_id"], "score", self.case.case.refs)

    def test_same_turn_preflight_repair_does_not_create_an_additional_invocation(self):
        ready = self.ready_score("same-turn")
        result = run_assessment_task(self.store, ready["flow_id"], "score", True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"])
        self.assertEqual(1, len(result["state"]["invocations"]["score"]))

    def test_score_context_is_locked_without_duplicate_json_or_changed_typed_inputs(self):
        from taskflow.execution_runner import _info, prepare_attempt

        ready = self.ready_score()
        prepared = prepare_assessment_task(self.store, ready['flow_id'], 'score')
        current = prepared['state']['invocations']['score'][-1]
        root = Path(current['runtime'])
        context_path = Path(current['work']) / '.taskflow-assessment-context.json'
        context = read_json(context_path)
        manifest = read_json(root / 'manifest.json')
        after = (root / 'request.md').read_bytes()
        self.assertEqual(_info(context_path), manifest['dependencies'][str(context_path)])
        self.assertNotIn(b'--context-json', after)
        self.assertNotIn(json.dumps(context).encode(), after)
        self.assertFalse(any(row['target'].endswith('.taskflow-assessment-context.json') for row in manifest['inputs']))
        facts = prepared['state']['invocations']['facts'][-1]
        first_context = read_json(Path(facts['work']) / context_path.name)
        self.assertFalse({'criteria', 'standards', 'scoring'} & set(first_context))
        before = copy.deepcopy(read_json(Path(current['root']) / 'spec.json')['runner'])
        locator = ('Read the exact current binding, criteria, scoring and source locators from '
                   + str(context_path) + ' without rewriting that dependency. ')
        text = before['request_sections'][0]['text']
        self.assertIn(locator, text)
        text = text.replace(locator, 'Exact locked scoring parameters: ' + json.dumps(context, ensure_ascii=False) + '. ')
        text = text.replace(shlex.join(['--context', str(context_path)]),
                            shlex.join(['--context-json', json.dumps(context, ensure_ascii=False, separators=(',', ':'))]))
        before['request_sections'][0]['text'] = text
        before_root = self.root / 'score-before' / 'runtime'
        before_work = before_root.parent / 'work'
        before_work.mkdir(parents=True)
        before['cwd'] = str(before_work)
        prepare_attempt(before_root, before)
        old = (before_root / 'request.md').read_bytes()
        self.assertLess(len(after), len(old))
        self.request_comparison = {'nature': 'offline_score_render_comparison',
            'before_request_bytes': len(old), 'after_request_bytes': len(after),
            'saved_request_bytes': len(old) - len(after), 'real_model_calls': 0,
            'limitations': 'Request bytes only; no token, cache or bill saving is inferred.'}

    def test_context_dependency_is_revalidated_after_the_actual_fixture_turn(self):
        from taskflow.execution_runner import RunnerError

        ready = self.ready_score('tamper-score-context')
        with self.assertRaisesRegex(RunnerError, 'Locked executable/dependency changed'):
            run_assessment_task(self.store, ready['flow_id'], 'score', True)
        result = self.store.get_flow(ready['flow_id'])
        self.assertNotIn('score_receipt', result['state'])
        current = result['state']['invocations']['score'][-1]
        self.assertEqual(1, len(self.store.list_attempts(current['work_id'])))
        with self.assertRaisesRegex(RunnerError, 'Locked executable/dependency changed'):
            run_assessment_task(self.store, ready['flow_id'], 'score', True)
        self.assertEqual(1, len(self.store.list_attempts(current['work_id'])))

    def test_same_context_delivery_correction_cannot_be_used_as_a_regrade(self):
        ready=self.ready_score("regrade",corrections=1)
        invalid=run_assessment_task(self.store,ready["flow_id"],"score",True)
        self.assertEqual("invalid",invalid["state"]["delivery"])
        correct_assessment_task(self.store,ready["flow_id"],"score",self.case.case.refs)
        result=run_assessment_task(self.store,ready["flow_id"],"score",True)
        self.assertEqual("invalid",result["state"]["delivery"])
        self.assertEqual("assessment.signed_business_changed",result["state"]["errors"][0]["code"])
        self.assertFalse(result["state"]["errors"][0]["correctable"])
        self.assertNotIn("score_receipt",result["state"])

    def test_wrong_context_and_unknown_execution_never_become_a_score_or_relaunch(self):
        ready = self.ready_score("wrong-score")
        result = run_assessment_task(self.store, ready["flow_id"], "score", True)
        self.assertEqual("failed", result["state"]["execution"])
        self.assertNotIn("score_receipt", result["state"])
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store, ready["flow_id"], "score", self.case.case.refs)
        current = result["state"]["invocations"]["score"][-1]
        self.assertEqual(1, len(self.store.list_attempts(current["work_id"])))

    def test_reused_solver_context_is_a_noncorrectable_delivery_error(self):
        result = self.run_facts("same-solver", corrections=1)
        self.assertEqual("invalid", result["state"]["delivery"])
        self.assertFalse(result["state"]["errors"][0]["correctable"])
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store, result["flow_id"], "facts", self.case.case.refs)

    def test_registration_is_idempotent_and_one_original_reviewer_is_bound_to_the_solver(self):
        spec = self.spec()
        first = register_assessment_task(self.store, spec, self.case.case.refs)
        self.assertEqual(first["revision"], register_assessment_task(self.store, spec, self.case.case.refs)["revision"])
        changed = copy.deepcopy(spec);changed["purpose"] = "Different immutable purpose."
        with self.assertRaises(TaskflowError):register_assessment_task(self.store, changed, self.case.case.refs)
        with self.assertRaisesRegex(TaskflowError, "original assessment"):
            self.register(identity="another-assessment")

    def test_review_target_policy_is_explicit_and_preserves_original_solver_policy(self):
        self.store.register_policy("review-v2",copy.deepcopy(self.store.get_policy("fixture-v1")["config"]))
        spec=self.spec();spec["policy_id"]="review-v2"
        record=register_assessment_task(self.store,spec,self.case.case.refs)
        facts=run_assessment_task(self.store,record["flow_id"],"facts",True)
        self.assertEqual("valid",facts["state"]["delivery"],facts["state"])
        current=facts["state"]["invocations"]["facts"][-1]
        self.assertEqual("review-v2",read_json(Path(current["runtime"])/"manifest.json")["policy_version"])
        self.assertEqual("fixture-v1",self.source["manifest"]["policy_version"])
        seal_assessment_facts(self.store,record["flow_id"])
        scored=run_assessment_task(self.store,record["flow_id"],"score",True)
        self.assertEqual("valid",scored["state"]["delivery"],scored["state"])
        changed=copy.deepcopy(spec);changed["policy_id"]="fixture-v1"
        with self.assertRaises(TaskflowError):register_assessment_task(self.store,changed,self.case.case.refs)

    def test_frozen_criteria_profile_and_stage_source_guards_fail_before_a_model(self):
        mutations = [lambda spec: spec.update(schema_version=2),
            lambda spec: spec.update(correction_limit=-1),
            lambda spec: spec["criteria"][0].update(max_points=99),
            lambda spec: spec["criteria"][0]["source"].update(sha256="0"*64),
            lambda spec: spec["criteria"][0]["source"].update(location="L99999"),
            lambda spec: spec.update(standards=[]),
            lambda spec: spec["facts_runner"].update(request_sections=[{"kind":"operation","text":"private focus"}]),
            lambda spec: spec["facts_runner"].update(inputs=[{"kind":"teacher"}]),
            lambda spec: spec["score_runner"].update(model="foreign"),
            lambda spec: spec["facts_runner"]["role_session"].update(mode="fresh_ephemeral")]
        count = len(self.store.dashboard()["attempts"])
        for index, mutate in enumerate(mutations):
            spec = self.spec(identity="invalid-"+str(index));mutate(spec)
            with self.subTest(index=index), self.assertRaises((TaskflowError, ValueError)):
                register_assessment_task(self.store, spec, self.case.case.refs)
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))

    def test_unknown_factual_attempt_is_observed_without_redrawing(self):
        record = self.register()
        ready = prepare_assessment_task(self.store, record["flow_id"])
        current = ready["state"]["invocations"]["facts"][-1]
        attempt = self.store.claim("unknown-factual", [current["work_id"]])
        self.store.mark_uncertain(attempt["attempt_id"], "Neutral fixture unknown launch result.")
        with patch("taskflow.execution_tasks.reconcile", return_value=self.store.get_attempt(attempt["attempt_id"])):
            result = run_assessment_task(self.store, record["flow_id"], "facts", True)
        self.assertEqual("unknown", result["state"]["execution"])
        self.assertEqual(1, len(self.store.list_attempts(current["work_id"])))

    def test_missing_output_and_bad_location_are_not_fabricated_or_normalized(self):
        result = self.run_facts("missing")
        self.assertEqual("failed", result["state"]["execution"])
        self.assertEqual("pending", result["state"]["delivery"])

    def test_public_repository_contamination_and_owned_directory_aliases_fail_before_execution(self):
        record=self.register();directory=Path(record["definition"]["directory"])
        public=directory/"public";public.mkdir();bad=public/"private-reference.txt";bad.write_text("Unexpected private material.")
        count=len(self.store.dashboard()["attempts"])
        with self.assertRaises(TaskflowError):prepare_assessment_task(self.store,record["flow_id"])
        bad.unlink();ready=prepare_assessment_task(self.store,record["flow_id"])
        bad.write_text("Unexpected later material.")
        with self.assertRaises(TaskflowError):run_assessment_task(self.store,record["flow_id"],"facts",True)
        bad.unlink()
        current=ready["state"]["invocations"]["facts"][-1];scratch=Path(current["scratch"])
        scratch.rmdir();outside=self.root/"outside-scratch";outside.mkdir();scratch.symlink_to(outside,target_is_directory=True)
        with self.assertRaisesRegex(TaskflowError,"canonical"):prepare_assessment_task(self.store,record["flow_id"])
        self.assertEqual([],list(outside.iterdir()))
        self.assertEqual(count,len(self.store.dashboard()["attempts"]))

    def test_native_review_cannot_promote_a_simulated_solver_and_private_grants_are_positive(self):
        spec=self.spec()
        for stage in ("facts","score"):spec[stage+"_runner"]["mode"]="native_role"
        with self.assertRaisesRegex(TaskflowError,"simulated solver"):
            register_assessment_task(self.store,spec,self.case.case.refs)
        spec=self.spec();source=_sources(self.store,spec)
        directory=Path(spec["directory"]);public=directory/"public";public.mkdir()
        prompt=public/"public.txt";prompt.write_text("Neutral public-only view.")
        work=directory/"invocations/facts-000/work";scratch=work.parent/"scratch"
        work.mkdir(parents=True);scratch.mkdir()
        profile={":root":"deny",":minimal":"read",":tmpdir":"deny",":slash_tmp":"deny","/private/tmp":"deny",
            str(public):"read",str(work):"write",str(scratch):"write",spec["facts_runner"]["role_session"]["home"]:"deny"}
        runner={"mode":"native_role","cli":str(Path(sys.executable).resolve()),"cwd":str(work),
            "role_session":spec["facts_runner"]["role_session"],"native_permissions":{"filesystem":profile,"repository":str(public),
                "python":str(Path(sys.executable).resolve()),"scratch":str(scratch),"probe_sources":[str(prompt)],"readonly_probe":str(prompt)}}
        _permissions(runner,source,public,spec)
        for name in (str(directory),str(directory/"sources/task-scoring.json"),str(self.source["archive"]),str(self.root/"different-batch"),":home"):
            changed=copy.deepcopy(runner);changed["native_permissions"]["filesystem"][name]="read"
            with self.subTest(name=name),self.assertRaises(TaskflowError):_permissions(changed,source,public,spec)

    def test_explicit_frozen_normative_selection_cannot_be_expanded_or_silently_trimmed(self):
        spec=self.spec();spec["standards"]=spec["standards"][:-1]
        with self.assertRaisesRegex(TaskflowError,"exactly retain"):
            register_assessment_task(self.store,spec,self.case.case.refs)

    def test_facts_seal_crash_window_reuses_the_exact_existing_seal(self):
        facts=self.run_facts()
        actual_save=assessment_module.save_task
        def interrupted(store,record,state,event):
            if event=="assessment_facts_sealed":raise RuntimeError("after seal before DB")
            return actual_save(store,record,state,event)
        with patch("taskflow.execution_assessment.save_task",side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError,"before DB"):seal_assessment_facts(self.store,facts["flow_id"])
        current=facts["state"]["invocations"]["facts"][-1]
        seal=Path(current["runtime"])/"facts-seal.json";original=seal.read_bytes()
        result=seal_assessment_facts(self.store,facts["flow_id"])
        self.assertEqual(original,seal.read_bytes())
        self.assertEqual(str(seal),result["state"]["facts_seal"]["path"])

    def test_malformed_factual_json_and_field_types_are_correctable_delivery_errors(self):
        result=self.run_facts("bad-json",corrections=1)
        self.assertEqual("invalid",result["state"]["delivery"])
        self.assertTrue(result["state"]["errors"][0]["correctable"])
        correct_assessment_task(self.store,result["flow_id"],"facts",self.case.case.refs)
        fixed=run_assessment_task(self.store,result["flow_id"],"facts",True)
        self.assertEqual("valid",fixed["state"]["delivery"])

    def test_unparseable_score_keeps_its_original_and_cannot_be_replaced_by_unguarded_scores(self):
        ready=self.ready_score("bad-score-json",corrections=1)
        result=run_assessment_task(self.store,ready["flow_id"],"score",True)
        self.assertEqual("invalid",result["state"]["delivery"])
        self.assertFalse(result["state"]["errors"][0]["correctable"])
        count=len(self.store.dashboard()["attempts"])
        with self.assertRaises(TaskflowError):correct_assessment_task(self.store,result["flow_id"],"score",self.case.case.refs)
        self.assertEqual(count,len(self.store.dashboard()["attempts"]))


class AssessmentContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / "public.txt").write_text("Actual neutral source.\nSecond line.\n", encoding="utf-8")
        (self.root / "rubric.txt").write_text("C1: up to seven points.\n", encoding="utf-8")
        self.rows = [{"path": name, "sha256": hashlib.sha256((self.root/name).read_bytes()).hexdigest(),
                      "size": (self.root/name).stat().st_size} for name in ("public.txt", "rubric.txt")]
        self.binding = {"candidate_id":"neutral", "intent_id":"neutral-review", "solver_attempt_id":"original"}
        self.cite = {"path":"public.txt", "sha256":self.rows[0]["sha256"], "location":"L1"}
        self.rule = {"path":"rubric.txt", "sha256":self.rows[1]["sha256"], "location":"L1"}
        self.context = {"stage":"facts", "binding":self.binding, "sources":[self.rows[0]], "inputs_root":str(self.root)}
        self.facts = {"schema_version":1,"kind":"review_facts_v1","binding":self.binding,"status":"complete",
            "coverage":[{**self.cite,"status":"read","limitation":""}],
            "facts":[{"id":"F1","statement":"Original neutral finding.","evidence":[self.cite]}],"limitations":[]}
        self.score_context = {**self.context,"stage":"score","sources":self.rows,"facts_sha256":"f"*64,
                              "criteria":[{"id":"C1","max_points":7,"source":self.rule}]}
        self.score = {"schema_version":1,"kind":"review_score_v1","binding":self.binding,"outcome":"scored",
            "facts_sha256":"f"*64,"validity":{"local_eligible":None,"reason":"Actual uncertainty.","evidence":[self.cite]},
            "criteria":[{"id":"C1","points":5,"max_points":7,"reason":"Original criterion judgment.","evidence":[self.cite,self.rule]}],
            "total":5,"limitations":[]}

    def test_valid_source_citations_and_original_score_are_checked_without_assignment(self):
        self.assertEqual("complete", validate_facts(self.facts,self.context)["outcome"])
        self.assertEqual("scored", validate_score(self.score,self.score_context)["outcome"])
        self.assertFalse(validate_score(self.score,self.score_context)["score_assigned"])

    def test_factual_shape_binding_coverage_and_locations_fail_without_mutation(self):
        changes = [lambda d:d.update(schema_version=True),lambda d:d.update(kind="foreign"),
            lambda d:d.update(binding={}),lambda d:d.update(extra="score"),lambda d:d.update(status="passed"),
            lambda d:d.update(coverage=[]),lambda d:d["coverage"].append(copy.deepcopy(d["coverage"][0])),
            lambda d:d["coverage"][0].update(sha256="0"*64),
            lambda d:d["coverage"][0].update(path=[]),
            lambda d:d["coverage"][0].update(status="unresolved",limitation=""),
            lambda d:d["coverage"][0].update(status="unresolved",limitation="Actual remaining read."),
            lambda d:d["facts"][0]["evidence"][0].update(location="L999"),
            lambda d:d["facts"][0]["evidence"][0].update(location="B0-B0"),
            lambda d:d["facts"][0]["evidence"][0].update(location="made-up prose"),
            lambda d:d["facts"][0]["evidence"][0].update(path="private.txt"),
            lambda d:d["facts"][0]["evidence"][0].update(path=[]),
            lambda d:d.update(facts=[]),lambda d:d.update(limitations=[""])]
        original=copy.deepcopy(self.facts)
        for index,change in enumerate(changes):
            document=copy.deepcopy(self.facts);change(document);raw=copy.deepcopy(document)
            with self.subTest(index=index),self.assertRaises(AssessmentContractError):validate_facts(document,self.context)
            self.assertEqual(raw,document)
        self.assertEqual(original,self.facts)

    def test_source_byte_drift_and_unsafe_targets_cannot_be_cited(self):
        for field,value in (("path","../public.txt"),("path","/public.txt"),("sha256","x"),("size",-1)):
            context=copy.deepcopy(self.context);context["sources"][0][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(AssessmentContractError):validate_facts(self.facts,context)
        context=copy.deepcopy(self.context);context["sources"].append(copy.deepcopy(context["sources"][0]))
        with self.assertRaises(AssessmentContractError):validate_facts(self.facts,context)
        (self.root/"public.txt").write_text("Drifted original.")
        with self.assertRaises(AssessmentContractError):validate_facts(self.facts,self.context)

    def test_score_sum_criteria_rule_and_uncertainty_are_preserved(self):
        changes=[lambda d:d.update(total=6),lambda d:d.update(total=float("nan")),lambda d:d.update(total=10**400),
            lambda d:d.update(facts_sha256="0"*64),lambda d:d.update(outcome="accepted"),
            lambda d:d["criteria"][0].update(points=True),lambda d:d["criteria"][0].update(points=8),
            lambda d:d["criteria"][0].update(max_points=8),lambda d:d["criteria"][0].update(id="foreign"),
            lambda d:d["criteria"][0].update(id=[]),
            lambda d:d.update(criteria=[]),lambda d:d["criteria"][0].update(evidence=[self.cite]),
            lambda d:d["validity"].update(local_eligible="true"),
            lambda d:d["validity"].update(official=True),
            lambda d:d.update(outcome="inconclusive"),
            lambda d:d.update(outcome="inconclusive",total=None,limitations=[])]
        for index,change in enumerate(changes):
            document=copy.deepcopy(self.score);change(document);raw=copy.deepcopy(document)
            with self.subTest(index=index),self.assertRaises(AssessmentContractError):validate_score(document,self.score_context)
            self.assertEqual(raw,document)
        document={**self.score,"outcome":"inconclusive","total":None,"limitations":["Actual uncertainty."]}
        self.assertEqual("inconclusive",validate_score(document,self.score_context)["outcome"])

    def test_delivery_correction_cannot_regrade_already_signed_business_values(self):
        draft=copy.deepcopy(self.score);draft["total"]=6
        locked=business_payload(draft,self.score_context)
        self.assertNotIn("total",locked)
        context={**self.score_context,"business_lock":locked}
        self.assertEqual("scored",validate_score(self.score,context)["outcome"])
        changes=[lambda d:(d["criteria"][0].update(points=6),d.update(total=6)),
                 lambda d:d["criteria"][0].update(reason="A different business reason."),
                 lambda d:d["validity"].update(local_eligible=True),
                 lambda d:d["validity"].update(reason="New eligibility claim."),
                 lambda d:d.update(outcome="inconclusive",total=None,limitations=["New uncertainty."])]
        for index,change in enumerate(changes):
            document=copy.deepcopy(self.score);change(document)
            with self.subTest(index=index),self.assertRaises(AssessmentContractError):validate_score(document,context)
        correct_total=business_payload(self.score,self.score_context)
        self.assertEqual(5,correct_total["total"])
        conflict=copy.deepcopy(self.score);conflict["criteria"].append({**conflict["criteria"][0],"points":6})
        with self.assertRaises(AssessmentContractError):business_payload(conflict,self.score_context)
        bad_max=copy.deepcopy(self.score);bad_max["criteria"][0]["max_points"]=True
        self.assertNotIn("max_points",business_payload(bad_max,self.score_context)["criteria"]["C1"])

    def test_complete_frozen_scoring_projection_must_be_cited(self):
        context={**self.score_context,"scoring_projection":{"path":"rubric.txt","sha256":self.rows[1]["sha256"]}}
        self.assertEqual("scored",validate_score(self.score,context)["outcome"])
        document=copy.deepcopy(self.score);document["criteria"][0]["evidence"]=[self.cite]
        with self.assertRaises(AssessmentContractError):validate_score(document,context)

    def test_multiple_delivery_corrections_accumulate_new_signed_values_without_regrading(self):
        context=copy.deepcopy(self.score_context)
        context["criteria"].append({"id":"C2","max_points":7,"source":self.rule})
        first=copy.deepcopy(self.score)
        lock=business_payload(first,context)
        self.assertNotIn("total",lock)
        second=copy.deepcopy(first)
        second["criteria"].append({"id":"C2","points":3,"max_points":7,"reason":"Newly signed second criterion.","evidence":[self.cite,self.rule]})
        second["total"]=9
        combined=accumulate_business_payload(lock,second,context)
        self.assertEqual(3,combined["criteria"]["C2"]["points"])
        third=copy.deepcopy(second);third["criteria"][1]["points"]=0;third["total"]=5
        with self.assertRaises(AssessmentContractError):validate_score(third,{**context,"business_lock":combined})
        corrected=copy.deepcopy(second);corrected["total"]=8
        self.assertEqual("scored",validate_score(corrected,{**context,"business_lock":combined})["outcome"])

    def test_binary_and_empty_original_members_use_actual_byte_ranges(self):
        for raw,location in ((b"\xff\x00","B0-B2"),(b"","B0-B0")):
            (self.root/"binary.bin").write_bytes(raw)
            row={"path":"binary.bin","sha256":hashlib.sha256(raw).hexdigest(),"size":len(raw)}
            context={**self.context,"sources":[row]}
            cite={"path":row["path"],"sha256":row["sha256"],"location":location}
            facts={**self.facts,"coverage":[{**cite,"status":"inspected","limitation":""}],
                "facts":[{"id":"B1","statement":"Original binary member observed.","evidence":[cite]}]}
            self.assertEqual("complete",validate_facts(facts,context)["outcome"])
            facts["coverage"][0]["location"]="L1"
            with self.assertRaises(AssessmentContractError):validate_facts(facts,context)

    def test_read_only_cli_gives_machine_error_and_current_invocation_override(self):
        output=self.root/"delivery";output.mkdir()
        path=output/"facts.json";path.write_text(json.dumps(self.facts))
        helper=Path(__file__).resolve().parents[1]/"taskflow/execution_assessment_contracts.py"
        command=[sys.executable,str(helper),'--directory',str(output),'--context-json',json.dumps(self.context)]
        valid=subprocess.run(command,capture_output=True,text=True)
        self.assertEqual(0,valid.returncode,valid.stderr)
        raw=path.read_bytes()
        bad=subprocess.run(command+['--intent-id','new-invocation'],capture_output=True,text=True)
        self.assertEqual(2,bad.returncode)
        self.assertFalse(json.loads(bad.stderr)["score_assigned"])
        self.assertEqual(raw,path.read_bytes())


if __name__ == "__main__":
    unittest.main()
