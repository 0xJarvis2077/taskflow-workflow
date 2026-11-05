"""Explicit single-role quality tasks preserve each original turn and outcome."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_quality import (
    collect_quality_task, correct_quality_task, prepare_quality_task,
    register_quality_task, run_quality_task,
)
from taskflow.execution_runner import archive_full_attempt, prepare_attempt, run_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


ROLE = r'''
import hashlib,json,pathlib,re,subprocess,sys
mode=sys.argv[1];request=sys.stdin.read()
def save(name,value):
 p=pathlib.Path(name);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(value))
if mode.startswith('public'):
 print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000001'}),flush=True)
 pathlib.Path('delivery.txt').write_text('An independent public attempt with an ordinary mistake.')
 if mode=='public-special': pathlib.Path('.capture-original-observation').write_text('Original indexed observation; never silently omitted.')
else:
 location=re.search(r'Read locked inputs from ([^\n]+?)\. Deliver ',request)
 inputs=pathlib.Path(location[1]) if location else pathlib.Path.cwd().parent/'runtime/inputs'
 ctx=json.loads((inputs/'execution/current.json').read_text())
 ordinal=int(pathlib.Path.cwd().parent.name)
 sid='00000000-0000-4000-8000-000000000002' if mode!='wrong-sid' or ordinal==0 else '00000000-0000-4000-8000-000000000003'
 print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
 public=json.loads((inputs/'execution/public-inputs.json').read_text())['files']
 for row in public:
  assert hashlib.sha256((inputs/row['path']).read_bytes()).hexdigest()==row['sha256']
 save('public-responsibility-first.json',{'public_observation':'Actual neutral fixture public checkpoint.','sources':public})
 digest=hashlib.sha256(pathlib.Path('public-responsibility-first.json').read_bytes()).hexdigest()
 nonce=ctx['public_first']['trace_nonce']
 # The fixture reads its known current teacher source after the public checkpoint;
 # native runs must retain equivalent actual successful tool events.
 teacher_path='draft/evaluation/answer.md';teacher_sha=hashlib.sha256((inputs/teacher_path).read_bytes()).hexdigest()
 teacher_event={'type':'item.completed','item':{'id':'teacher-read-0','type':'command_execution','status':'completed','exit_code':0,
  'aggregated_output':'TEACHER_SOURCE_PATH='+teacher_path+' SHA256='+teacher_sha+' QUALITY_TRACE_NONCE='+nonce}}
 if mode=='teacher-first': print(json.dumps(teacher_event),flush=True)
 checkpoint_event={'type':'item.completed','item':{'id':'checkpoint-read-0','type':'command_execution','status':'completed',
  'exit_code':1 if mode=='failed-anchor' else 0,'aggregated_output':'PUBLIC_RESPONSIBILITY_FIRST_SHA256='+digest+' QUALITY_TRACE_NONCE='+nonce}}
 print(json.dumps(checkpoint_event),flush=True)
 if mode=='duplicate-anchor':
  checkpoint_event['item']['id']='checkpoint-duplicate';print(json.dumps(checkpoint_event),flush=True)
 if mode!='teacher-first': print(json.dumps(teacher_event),flush=True)
 if not location:
  later=json.loads((pathlib.Path.cwd().parent/'runtime/quality-later-inputs.v1.json').read_text())
  request+='\n'+'\n'.join(row['text'] for row in later['request_sections'])
 intent=re.search(r'This invocation has intent_id=([^;]+);',request)[1]
 binding={k:ctx[k] for k in ('candidate_id','content_digest','input_digest','evaluation_digest')};binding['intent_id']=intent
 outcome=mode if mode in ('revision_required','inconclusive') else 'accepted'
 defects=[];blocking=[]
 if (mode in ('invalid-first','wrong-sid','same-turn') and ordinal==0) or mode=='always-invalid':
  outcome='revision_required';blocking=['OBS-1'];target='actual-public-work/delivery.txt'
 elif mode=='revision_required':
  blocking=['MATERIAL-1'];target=next(x['path'] for x in json.loads((inputs/'execution/public-inputs.json').read_text())['files'])
 if blocking:
  basis=next(x['path'] for x in json.loads((inputs/'execution/public-inputs.json').read_text())['files'])
  defects=[{'id':blocking[0],'kind':'material','responsibility':'Neutral fixture responsibility requiring current evidence.',
    'affected_paths':[target],'evidence':[{'path':basis,'sha256':hashlib.sha256((inputs/basis).read_bytes()).hexdigest(),'location':'line 1'}]}]
 route={'schema_version':1,**binding,'signer_role':'independent_quality','independent_of_current_author':True,
  'participated_in_production':False,'scope':'whole_package','outcome':outcome,'blocking_issue_ids':blocking,
  'covered_defect_ids':[],'reason':'Original independent neutral fixture conclusion.'}
 basis=public[0]['path'];citation={'path':basis,'sha256':public[0]['sha256'],'location':'line 1'}
 declaration=json.loads((inputs/'execution/task.json').read_text())
 normative=[]
 for row in declaration['files']:
  if row['role'] in ('answer','rubric'):
   raw=(inputs/row['path']).read_bytes()
   normative.append({'package_path':row['path'],'role':row['role'],'basis':'Current explicit neutral fixture reference.',
    'sha256':hashlib.sha256(raw).hexdigest(),'size':len(raw),'purpose':row['role']})
 native={'schema_version':1,'protocol_sha256':(re.search(r'<taskflow_native_signoff_protocol sha256="([0-9a-f]+)"',request)[1] if '<taskflow_native_signoff_protocol ' in request else json.loads((inputs/'execution/quality-target-paths.json').read_text())['native_protocol_sha256']),
  'coverage':'whole_package','affected_paths':[],'coverage_basis':'Entire current neutral fixture package.',
  'dimensions':{k:{'affirmed':outcome!='revision_required','evidence':[citation],'limitations':[]} for k in ('business','artifact','calculation')},
  'normative':{'status':'determined','files':normative,'evidence':[citation],'limitations':[]},
  'responsibility_analysis':[{'public_evidence':[citation],'upstream_work_already_done':'Neutral ordinary public facts.',
   'remaining_judgment':'The independent role decides the actual consequence.','consequence':'Material business choice.',
   'retained_necessary_conditions':['Actual source facts.'],'legitimate_branches':['Equivalent lawful methods.'],
   'blocking_issue_ids':blocking,'disposition':'blocking_defect' if blocking else 'unresolved' if outcome=='inconclusive' else 'retained'}]}
 report={'execution_routing':route,'native_signoff':native,'public_first':{'checkpoint_sha256':digest,
  'trace_nonce':nonce,'teacher_path':teacher_path}}
 if mode=='missing-native': del report['native_signoff']
 if mode=='wrong-native': report['native_signoff']['normative']['files'][0]['sha256']='0'*64
 save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':defects})
 pathlib.Path('intake.md').write_text('Original neutral fixture examination; never a real quality result.')
 if mode=='same-turn':
  command=[sys.executable,str(inputs/'execution/quality-target-preflight.py'),'--context',str(inputs/'execution/quality-target-paths.json'),
   '--intent-id',intent,'--directory',str(pathlib.Path.cwd())]
  bad=subprocess.run(command,capture_output=True,text=True);assert bad.returncode==2,(bad.stdout,bad.stderr)
  diagnostic=json.loads(bad.stderr);assert diagnostic['quality_accepted'] is False
  print(json.dumps({'type':'item.completed','item':{'id':'preflight-bad','type':'command_execution','status':'completed','exit_code':2,'aggregated_output':bad.stderr}}),flush=True)
  # This role fixture corrects its own report, not candidate material or prior turns.
  route.update(outcome='accepted',blocking_issue_ids=[])
  for dimension in native['dimensions'].values(): dimension['affirmed']=True
  native['responsibility_analysis'][0].update(blocking_issue_ids=[],disposition='retained')
  save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':[]})
  good=subprocess.run(command,capture_output=True,text=True);assert good.returncode==0,(good.stdout,good.stderr)
  print(json.dumps({'type':'item.completed','item':{'id':'preflight-good','type':'command_execution','status':'completed','exit_code':0,'aggregated_output':good.stdout}}),flush=True)
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral fixture delivery.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
'''


class ExplicitQualityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.script = self.root / "role.py"
        self.script.write_text(ROLE, encoding="utf-8")
        authority = self.root / "authority.json"
        write_json(authority, {"nature": "test_fixture", "authorized": "single-role-quality-with-one-correction"})
        self.refs = [evidence_ref(authority, "neutral fixture authority")]
        self.task = make_task(self.root, "neutral")
        self.candidate = execution.capture_candidate(self.store, self.task, "candidate-neutral")
        work = self.root / "public-work"
        work.mkdir()
        runtime = self.root / "public-runtime"
        public_spec = {"work": {
            "work_id": "public", "task_id": "neutral", "candidate_id": self.candidate["candidate_id"],
            "intent_id": "public-intent", "role": "quality", "policy_id": "fixture-v1",
            "purpose": "independent neutral public observation", "evidence": self.refs},
            "runner": {"mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "role": "quality", "intent_id": "public-intent", "policy_version": "fixture-v1", "cwd": str(work),
                "inputs": [], "request_sections": [], "dependencies": [str(self.script)],
                "argv": [str(Path(sys.executable).resolve()), str(self.script), "public"], "outputs": ["delivery.txt"]}}
        public_spec["runner"].update(candidate_binding={key: self.candidate[key] for key in (
            "candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}, inputs=[
            {"path": row["path"], "kind": "public", "target": row["package_path"]}
            for row in self.candidate["files"] if row["role"] in ("prompt", "input", "template")])
        self.public_spec = copy.deepcopy(public_spec)
        manifest = prepare_attempt(runtime, public_spec["runner"])
        self.store.enqueue(**public_spec["work"], request_digest=manifest["request"]["sha256"], runtime_path=str(runtime))
        original = self.store.claim("fixture-public", ["public"])
        result = run_attempt(runtime)
        self.public = self.store.finish_attempt(original["attempt_id"], process_exit=0,
            response_complete=result["model_completed"], artifacts_complete=result["delivery_complete"],
            technical_accepted=result["technical_accepted"], cli_sid=result["sid"],
            evidence=[evidence_ref(runtime / "collection.json", "original public fixture collection")])
        self.archive = self.root / "public-archive"
        archive_full_attempt(runtime, self.archive)

    def spec(self, mode="invalid-first", *, corrections=1, identity="quality-neutral"):
        home = self.root / (identity + "-home")
        home.mkdir(exist_ok=True)
        return {"schema_version": 1, "flow_id": identity, "task_id": "neutral", "owner": "fixture-controller",
            "policy_id": "fixture-v1", "directory": str(self.root / identity), "candidate_id": self.candidate["candidate_id"],
            "candidate_root": str(self.task), "purpose": "One explicit neutral whole quality task.",
            "scope": "whole_package", "predecessor_attempt": self.public["attempt_id"],
            "predecessor_archive": str(self.archive), "correction_limit": corrections,
            "runner": {"mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "inputs": [], "request_sections": [], "dependencies": [str(self.script)],
                "argv": [str(Path(sys.executable).resolve()), str(self.script), mode],
                "role_session": {"mode": "fresh_persistent", "home": str(home), "previous": None}}}

    def register(self, mode="invalid-first", **kwargs):
        spec = self.spec(mode, **kwargs)
        return register_quality_task(self.store, spec, self.refs)

    def test_prepare_renders_nested_path_keys_and_values(self):
        spec = self.spec(identity="quality-render-paths")
        spec["runner"]["native_permissions"] = {"filesystem": {
            "{{runtime}}/inputs": "read", "{{work}}": "write", "{{scratch}}": "{{intent_id}}"}}
        record = register_quality_task(self.store, spec, self.refs)
        prepared = prepare_quality_task(self.store, record["flow_id"])
        invocation = prepared["state"]["invocations"][0]
        written = read_json(Path(invocation["root"]) / "spec.json")
        self.assertEqual({str(Path(invocation["runtime"]) / "inputs"): "read",
                          invocation["work"]: "write", invocation["scratch"]: invocation["intent_id"]},
                         written["runner"]["native_permissions"]["filesystem"])
        self.assertEqual(prepared["revision"], prepare_quality_task(self.store, record["flow_id"])["revision"])

    def test_prepare_rejects_unknown_and_colliding_rendered_keys(self):
        for name, filesystem, message in (
            ("unknown", {"{{unknown}}/inputs": "read"}, "unknown quality runner placeholder"),
            ("collision", {"{{work}}": "write", "{{root}}/work": "read"}, "keys collide"),
        ):
            with self.subTest(name=name):
                spec = self.spec(identity="quality-render-" + name)
                spec["runner"]["native_permissions"] = {"filesystem": filesystem}
                record = register_quality_task(self.store, spec, self.refs)
                with self.assertRaisesRegex(TaskflowError, message):
                    prepare_quality_task(self.store, record["flow_id"])
                invocation = record["state"]["invocations"][0]
                self.assertFalse((Path(invocation["root"]) / "spec.json").exists())

    def test_invalid_observation_target_then_same_context_correction(self):
        registered = self.register()
        self.assertEqual("not_started", registered["state"]["execution"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        result = run_quality_task(self.store, registered["flow_id"], True)
        self.assertEqual(("completed", "invalid", "revision_required"), tuple(result["state"][k] for k in ("execution", "delivery", "outcome")))
        error = result["state"]["errors"][0]
        self.assertIn("Observed paths", error["message"])
        original = result["state"]["invocations"][0]
        report = Path(original["runtime"]) / "artifacts/adoption-readiness.json"
        original_bytes = report.read_bytes()
        original_collection = (Path(original["runtime"]) / "collection.json").read_bytes()
        corrected = correct_quality_task(self.store, registered["flow_id"], self.refs)
        again = correct_quality_task(self.store, registered["flow_id"], self.refs)
        self.assertEqual(corrected["revision"], again["revision"])
        self.assertEqual(1, corrected["state"]["corrections_used"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))
        result = run_quality_task(self.store, registered["flow_id"], True)
        self.assertEqual(("completed", "valid", "accepted"), tuple(result["state"][k] for k in ("execution", "delivery", "outcome")))
        turns = result["state"]["invocations"]
        self.assertNotEqual(turns[0]["attempt_id"], turns[1]["attempt_id"])
        self.assertEqual(turns[0]["sid"], turns[1]["sid"])
        self.assertEqual(original_bytes, report.read_bytes())
        self.assertEqual(original_collection, (Path(original["runtime"]) / "collection.json").read_bytes())
        first = read_json(Path(turns[0]["runtime"]) / "manifest.json")
        second = read_json(Path(turns[1]["runtime"]) / "manifest.json")
        signature = lambda m: sorted((x["target"], x["kind"], x["sha256"], x["size"]) for x in m["inputs"])
        self.assertEqual(signature(first), signature(second))
        self.assertEqual(["public", turns[0]["work_id"], turns[1]["work_id"]], [x["work_id"] for x in self.store.dashboard()["work"]])

    def test_business_outcomes_return_without_repair_or_reconstruction(self):
        for outcome in ("revision_required", "inconclusive", "accepted"):
            with self.subTest(outcome=outcome):
                record = self.register(outcome, identity="quality-" + outcome)
                result = run_quality_task(self.store, record["flow_id"], True)
                self.assertEqual("valid", result["state"]["delivery"])
                self.assertEqual(outcome, result["state"]["outcome"])
                count = len(self.store.dashboard()["attempts"])
                run_quality_task(self.store, record["flow_id"], True)
                self.assertEqual(count, len(self.store.dashboard()["attempts"]))
                self.assertEqual(1, len(result["state"]["invocations"]))
                with self.assertRaises(TaskflowError):
                    correct_quality_task(self.store, record["flow_id"], self.refs)

    def test_zero_or_exhausted_correction_allowance_never_relaunches(self):
        for limit in (0, 1):
            with self.subTest(limit=limit):
                record = self.register("always-invalid", corrections=limit, identity="quality-limit-" + str(limit))
                result = run_quality_task(self.store, record["flow_id"], True)
                if limit:
                    correct_quality_task(self.store, record["flow_id"], self.refs)
                    result = run_quality_task(self.store, record["flow_id"], True)
                    self.assertEqual("invalid", result["state"]["delivery"])
                count = len(self.store.dashboard()["attempts"])
                with self.assertRaisesRegex(TaskflowError, "allowance exhausted"):
                    correct_quality_task(self.store, record["flow_id"], self.refs)
                self.assertEqual(count, len(self.store.dashboard()["attempts"]))

    def test_unknown_original_is_observed_and_never_requeued(self):
        record = self.register()
        prepared = prepare_quality_task(self.store, record["flow_id"])
        work_id = prepared["state"]["invocations"][0]["work_id"]
        attempt = self.store.claim("interrupted-fixture", [work_id])
        self.store.mark_uncertain(attempt["attempt_id"], "unknown original launch")
        result = run_quality_task(self.store, record["flow_id"], True)
        self.assertEqual("unknown", result["state"]["execution"])
        self.assertEqual("pending", result["state"]["delivery"])
        self.assertEqual(1, len(self.store.list_attempts(work_id)))
        with self.assertRaises(TaskflowError):
            correct_quality_task(self.store, record["flow_id"], self.refs)

    def test_wrong_sid_cannot_become_an_accepted_correction(self):
        record = self.register("wrong-sid")
        run_quality_task(self.store, record["flow_id"], True)
        correct_quality_task(self.store, record["flow_id"], self.refs)
        result = run_quality_task(self.store, record["flow_id"], True)
        self.assertNotEqual("valid", result["state"]["delivery"])
        self.assertNotEqual("accepted", result["state"]["outcome"])
        self.assertFalse(result["state"].get("quality_gate", {}).get("accepted", False))

    def test_source_drift_and_nonpersistent_configuration_are_rejected(self):
        spec = self.spec()
        bad = copy.deepcopy(spec)
        bad["runner"]["role_session"]["mode"] = "fresh_ephemeral"
        with self.assertRaisesRegex(TaskflowError, "fresh_persistent"):
            register_quality_task(self.store, bad, self.refs)
        (self.archive / "supplemental/trees/work/delivery.txt").write_text("Changed observation.", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            register_quality_task(self.store, spec, self.refs)
        self.assertEqual([], self.store.list_flows())

    def test_later_accepted_signature_does_not_close_original_blocking_defect(self):
        first = self.register("revision_required", identity="quality-negative")
        negative = run_quality_task(self.store, first["flow_id"], True)
        defects = negative["state"]["invocations"][0]["defect_ids"]
        self.assertEqual(1, len(defects))
        self.assertEqual("open", self.store.get_defect(defects[0])["state"])
        later = self.register("accepted", identity="quality-later")
        positive = run_quality_task(self.store, later["flow_id"], True)
        self.assertEqual(("valid", "accepted"), (positive["state"]["delivery"], positive["state"]["outcome"]))
        self.assertFalse(positive["state"]["quality_gate"]["accepted"])
        self.assertEqual("open", self.store.get_defect(defects[0])["state"])
        self.assertFalse(any(row["role"] == "repair" for row in self.store.dashboard()["work"]))

    def test_native_contract_and_public_first_failures_are_retained_as_invalid(self):
        for mode in ("missing-native", "wrong-native", "teacher-first", "failed-anchor", "duplicate-anchor"):
            with self.subTest(mode=mode):
                record = self.register(mode, identity="quality-" + mode)
                result = run_quality_task(self.store, record["flow_id"], True)
                self.assertEqual("completed", result["state"]["execution"])
                self.assertEqual("invalid", result["state"]["delivery"])
                self.assertEqual("accepted", result["state"]["outcome"])
                self.assertFalse(result["state"].get("quality_gate", {}).get("accepted", False))
                if mode in ("teacher-first", "failed-anchor", "duplicate-anchor"):
                    self.assertFalse(result["state"]["errors"][0]["correctable"])
                    with self.assertRaises(TaskflowError):
                        correct_quality_task(self.store, record["flow_id"], self.refs)

    def test_initial_neutral_listing_is_not_repeated_during_correction(self):
        spec = self.spec()
        spec["runner"]["public_first_initial_listing"] = {
            "scope": "whole_package", "index": "execution/public-inputs.json", "checkpoint": "public-responsibility-first.json"}
        record = register_quality_task(self.store, spec, self.refs)
        original = run_quality_task(self.store, record["flow_id"], True)
        self.assertEqual("invalid", original["state"]["delivery"])
        correct_quality_task(self.store, record["flow_id"], self.refs)
        result = run_quality_task(self.store, record["flow_id"], True)
        self.assertEqual(("valid", "accepted"), (result["state"]["delivery"], result["state"]["outcome"]))
        turns = result["state"]["invocations"]
        self.assertEqual(turns[0]["sid"], turns[1]["sid"])
        manifest = read_json(Path(turns[1]["runtime"]) / "manifest.json")
        self.assertNotIn("public_first_initial_listing", manifest)

    def test_role_can_use_the_shared_full_preflight_and_correct_in_one_turn(self):
        record = self.register("same-turn")
        result = run_quality_task(self.store, record["flow_id"], True)
        self.assertEqual(("completed", "valid", "accepted"), tuple(result["state"][key] for key in ("execution", "delivery", "outcome")))
        self.assertEqual(0, result["state"]["corrections_used"])
        self.assertEqual(1, len(result["state"]["invocations"]))
        invocation = result["state"]["invocations"][0]
        events = [json.loads(line) for line in (Path(invocation["runtime"]) / "trace.jsonl").read_text().splitlines()]
        preflight = [event["item"] for event in events if event.get("item", {}).get("id") in ("preflight-bad", "preflight-good")]
        self.assertEqual([2, 0], [event["exit_code"] for event in preflight])

    def test_indexed_predecessor_members_fail_closed_without_partial_observations(self):
        public_spec = copy.deepcopy(self.public_spec)
        runtime, work = self.root / "special-public-runtime", self.root / "special-public-work"
        work.mkdir()
        public_spec["work"].update(work_id="public-special", intent_id="public-special-intent")
        public_spec["runner"].update(intent_id="public-special-intent", cwd=str(work),
            argv=[str(Path(sys.executable).resolve()), str(self.script), "public-special"])
        manifest = prepare_attempt(runtime, public_spec["runner"])
        self.store.enqueue(**public_spec["work"], request_digest=manifest["request"]["sha256"], runtime_path=str(runtime))
        original = self.store.claim("fixture-special-public", ["public-special"])
        result = run_attempt(runtime)
        attempt = self.store.finish_attempt(original["attempt_id"], process_exit=0,
            response_complete=result["model_completed"], artifacts_complete=result["delivery_complete"],
            technical_accepted=result["technical_accepted"], cli_sid=result["sid"],
            evidence=[evidence_ref(runtime / "collection.json", "original indexed public fixture collection")])
        archive = self.root / "special-public-archive"
        archive_full_attempt(runtime, archive)
        original_bytes = (work / ".capture-original-observation").read_bytes()
        indexed = sorted((archive / "supplemental/indexed-files").rglob("*.bin"))
        self.assertEqual(1, len(indexed))
        spec = self.spec()
        spec.update(predecessor_attempt=attempt["attempt_id"], predecessor_archive=str(archive))
        record = register_quality_task(self.store, spec, self.refs)
        with self.assertRaisesRegex(TaskflowError, "unsupported indexed work members"):
            prepare_quality_task(self.store, record["flow_id"])
        self.assertEqual(original_bytes, indexed[0].read_bytes())
        self.assertEqual(original_bytes, (work / ".capture-original-observation").read_bytes())
        self.assertEqual([], self.store.list_attempts(record["state"]["invocations"][0]["work_id"]))

    def test_registration_and_no_model_run_are_idempotent(self):
        spec = self.spec("accepted")
        one = register_quality_task(self.store, spec, self.refs)
        two = register_quality_task(self.store, spec, self.refs)
        self.assertEqual(one, two)
        prepared = run_quality_task(self.store, one["flow_id"])
        self.assertEqual("not_started", prepared["state"]["execution"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(prepared, collect_quality_task(self.store, one["flow_id"]))


if __name__ == "__main__":
    unittest.main()
