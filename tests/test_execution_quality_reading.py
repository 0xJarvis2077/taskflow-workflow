"""Neutral real subprocess/SQLite turns; no model or production session."""

import copy
import json
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_flow import advance_workflow
from taskflow.execution_quality_continuation import continue_inconclusive_workflow, inspect_inconclusive_workflow
from taskflow.execution_quality_session import validate_session, validate_plan
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as fixtures


SERIAL_PREFIX = r"""
serial=ctx.get('serial_quality_reading')
if serial:
 store=pathlib.Path(STORE_PATH);store.mkdir(exist_ok=True)
 if serial['index']:
  previous=json.loads(pathlib.Path(serial['previous']['path']).read_text());sid=previous['sid']
  if (store/'changed-sid').exists():sid=str(uuid.uuid4())
 else:
  reused=store/'reuse-sid.txt';sid=reused.read_text() if reused.exists() else str(uuid.uuid4())
 original=store/('rollout-'+sid+'.jsonl')
 turn_id=str(uuid.uuid4())
 with original.open('a') as out:
  if original.stat().st_size==0:out.write(json.dumps({'type':'session_meta','payload':{'id':sid}})+'\n')
  out.write(json.dumps({'type':'event_msg','payload':{'type':'task_started','turn_id':turn_id}})+'\n')
  out.write(json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':request}],'internal_chat_message_metadata_passthrough':{'turn_id':turn_id,'content_item_kinds':['user.text']}}})+'\n')
  out.write(json.dumps({'type':'event_msg','payload':{'type':'item_completed','thread_id':sid,'turn_id':turn_id,'item':{'type':'UserMessage','content':[{'type':'text','text':request,'text_elements':[]}]}}})+'\n')
 def finish_session(text):
  with original.open('a') as out:
   out.write(json.dumps({'type':'event_msg','payload':{'type':'item_completed','thread_id':sid,'turn_id':turn_id,'item':{'type':'AgentMessage','content':[{'type':'Text','text':text}]}}})+'\n')
   out.write(json.dumps({'type':'event_msg','payload':{'type':'task_complete','turn_id':turn_id,'last_agent_message':text}})+'\n')
   if (store/'foreign-before-capture').exists():
    out.write(json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'FOREIGN'}]}})+'\n')
    out.write(json.dumps({'type':'event_msg','payload':{'type':'task_complete','turn_id':'foreign'}})+'\n')
 print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
 print(json.dumps({'type':'turn.started'}),flush=True)
 if serial['phase']=='progress':
  checkpoint=None
  if serial['index']==0:
   save('public-responsibility-first.json',{'input_digest':ctx['input_digest'],'nature':'synthetic public responsibility fixture'})
   anchor=digest(pathlib.Path('public-responsibility-first.json'))
   if not (store/'missing-anchor').exists():print(json.dumps({'type':'item.completed','item':{'type':'command_execution','aggregated_output':'PUBLIC_RESPONSIBILITY_FIRST_SHA256='+anchor}}),flush=True)
   checkpoint={'sha256':anchor,'tool_readback_location':'synthetic visible command response'}
  plan=json.loads(pathlib.Path(serial['plan']['path']).read_text());assigned=plan['turns'][serial['index']]['page_ids'];pages={p['id']:p for p in plan['pages']}
  dispositions=[]
  for pid in assigned:
   p=pages[pid];data=pathlib.Path(p['path']).read_bytes()
   assert hashlib.sha256(data).hexdigest()==p['sha256']
   dispositions.append({'id':pid,'sha256':p['sha256'],'disposition':'read','evidence':[{'path':p['path'],'location':'complete synthetic body read'}],'association':'Neutral required source matched to current responsibility.','remaining':''})
  if (store/'omit-page').exists() and assigned:dispositions=[]
  name='reading-progress-'+str(serial['index']).zfill(3)+'.json'
  save(name,{'schema_version':1,'kind':'quality_read_progress_v1','intent_id':ctx['intent_id'],'candidate_binding':{k:ctx[k] for k in ['candidate_id','task_id','input_digest','content_digest','evaluation_digest']},'segment_index':serial['index'],'status':'progress_only','pages':dispositions,'facts':['Neutral observed facts; not acceptance'],'remaining':[],'public_checkpoint':checkpoint})
  if (store/'premature-signoff').exists():save('adoption-readiness.json',{'outcome':'accepted'})
  finish_session('Synthetic progress only, no quality signoff.')
  print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Synthetic progress only, no quality signoff.'}}),flush=True)
  if not (store/'missing-turn').exists():print(json.dumps({'type':'turn.completed'}),flush=True)
  sys.exit(0)
 a.outcome='inconclusive' if (store/'final-inconclusive').exists() else 'accepted'
else:
 sid=str(uuid.uuid4());print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
"""


class SerialQualityReadingTests(unittest.TestCase):
    def setUp(self):
        self.helper=fixtures.ContinuationTests();self.helper.setUp();self.addCleanup(self.helper.doCleanups)
        self.store,self.root=self.helper.store,self.helper.root
        self.sessions=self.root/'synthetic-sessions';self.sessions.mkdir()
        prefix=SERIAL_PREFIX.replace('STORE_PATH',repr(str(self.sessions)))
        original=fixtures.ROLE_FIXTURE
        old="sid=str(uuid.uuid4());print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)"
        self.assertIn(old,original)
        original=original.replace(old,prefix)
        marker="print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral fixture delivery.'}}),flush=True)"
        original=original.replace(marker,"if serial:finish_session('Neutral fixture delivery.')\n"+marker)
        self.helper.script.write_text(original)
        self.definition=self.helper.definition(outcome='inconclusive')
        self.definition['roles']['quality']['runner']['fixture_session_store']=str(self.sessions)
        self.helper.register(self.definition);self.helper.run_flow()
        self.fid=self.definition['flow_id'];self.original=self.store.get_flow(self.fid)
        self.source=inspect_inconclusive_workflow(self.store,self.fid)
        page=self.root/'neutral-page.txt';page.write_text('A complete neutral necessary source.\n')
        import hashlib
        self.plan={'schema_version':1,'kind':'serial_whole_quality_review_v1','candidate_binding':self.source['candidate_binding'],
            'turns':[{'name':'public','kind':'public_first','instructions':'Read public originals and seal checkpoint.','page_ids':[]},
                     {'name':'read-source','kind':'read','instructions':'Read and associate the exact neutral source.','page_ids':['page-one']}],
            'pages':[{'id':'page-one','path':str(page),'sha256':hashlib.sha256(page.read_bytes()).hexdigest(),'size':page.stat().st_size,'source_refs':['neutral-original']}],
            'source_refs':[self.helper.refs[0]],'direct_user_authority':self.helper.refs[0],'max_progress_turns':2,'max_final_turns':1}
        self.plan_path=self.root/'reading-plan.json';write_json(self.plan_path,self.plan)
        self.plan_ref=evidence_ref(self.plan_path,'synthetic explicit finite reading plan')
        self.packet={'schema_version':1,'kind':'continue_inconclusive_quality_v1','flow_id':self.fid,'controller':self.definition['owner'],
            'scope':'whole_package','purpose':'One fresh same-SID serial whole review of exact neutral scope.',
            'source':self.source,'direct_user_authority':self.helper.refs[0],'serial_review_plan':self.plan_ref}

    def authorize(self):
        path=self.root/'continuation.json';write_json(path,self.packet)
        return continue_inconclusive_workflow(self.store,self.fid,authorization=evidence_ref(path,'synthetic exact continuation'))

    def first_progress(self):
        from taskflow.execution import run_worker
        self.authorize();prepared=advance_workflow(self.store,self.fid)
        self.assertEqual(0,len(self.store.list_attempts(prepared['state']['current_work_id'])))
        run_worker(self.store,'synthetic-serial-owner',True,[prepared['state']['current_work_id']])
        archived=advance_workflow(self.store,self.fid)
        received=advance_workflow(self.store,self.fid) if archived['state'].get('current_work_id') else archived
        return received

    def test_one_same_sid_three_turns_only_final_signs_and_reentry_is_idempotent(self):
        old_report=Path(self.source['evidence'][0]['path']).read_bytes()
        self.authorize();result=self.helper.run_flow([self.fid]);f=result['flows'][0]
        self.assertEqual('accepted',f['state']['status'],f['state'].get('exception'))
        reading=f['state']['serial_quality_reading'];self.assertEqual(2,reading['index']);self.assertEqual(2,len(reading['turns']))
        attempts=self.store.dashboard()['attempts'];new=[a for a in attempts if a['attempt_id']!=self.source['attempt_id']]
        self.assertEqual(3,len(new));self.assertEqual(1,len({a['cli_sid'] for a in new}));self.assertNotEqual(self.source['cli_sid'],new[0]['cli_sid'])
        self.assertEqual(old_report,Path(self.source['evidence'][0]['path']).read_bytes())
        with self.store._db() as db:self.assertEqual(2,db.execute('select count(*) from reviews').fetchone()[0])
        for entry in reading['turns']:
            self.assertFalse(entry['quality_accepted']);self.assertEqual('not_issued',entry['business_verdict'])
            self.assertTrue(Path(entry['seal']['path']).exists());self.assertTrue((Path(entry['archive'])/'archive-binding.json').exists())
        self.helper.run_flow([self.fid]);self.assertEqual(4,len(self.store.dashboard()['attempts']))
        final=next(a for a in new if Path(a['runtime_path']).parent.name=='1-quality')
        request=(Path(final['runtime_path'])/'request.md').read_text();self.assertNotIn('Input (',request)
        self.assertIn('one ordinary whole-package',request)
        self.assertIn('rtk proxy', request)
        self.assertIn('--target-scope-only', request)
        self.assertTrue((Path(final['runtime_path'])/'inputs/execution/quality-target-paths.json').exists())
        for progress in (a for a in new if a['attempt_id'] != final['attempt_id']):
            initial = (Path(progress['runtime_path'])/'request.md').read_text()
            self.assertNotIn('--target-scope-only', initial)
            self.assertFalse((Path(progress['runtime_path'])/'inputs/execution/quality-target-paths.json').exists())

    def test_external_context_append_stops_before_another_attempt(self):
        f=self.first_progress();self.assertEqual(1,f['state']['serial_quality_reading']['index'],f['state'].get('exception'))
        entry=f['state']['serial_quality_reading']['turns'][0];seal=json.loads(Path(entry['seal']['path']).read_text())
        with Path(seal['session_file']).open('a') as out:out.write(json.dumps({'type':'foreign-turn'})+'\n')
        result=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',result['state']['status']);self.assertIn('context changed',result['state']['exception']['reason'])
        self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_foreign_turn_after_collection_before_seal_is_not_adopted(self):
        from taskflow.execution import run_worker
        self.authorize();prepared=advance_workflow(self.store,self.fid)
        attempt=run_worker(self.store,'synthetic-owner',True,[prepared['state']['current_work_id']])
        self.assertEqual('completed',attempt['state'])
        path=next(self.sessions.glob('*'+attempt['cli_sid']+'*.jsonl'))
        with path.open('a') as out:
            out.write(json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'FOREIGN AFTER COLLECTION'}]}})+'\n')
            out.write(json.dumps({'type':'event_msg','payload':{'type':'task_complete','turn_id':'foreign'}})+'\n')
        f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('process-exit capture',f['state']['exception']['reason'])
        self.assertEqual(0,f['state']['serial_quality_reading']['index']);self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_foreign_turn_before_owned_capture_is_not_technically_accepted(self):
        (self.sessions/'foreign-before-capture').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertEqual(0,f['state']['serial_quality_reading']['index'])
        self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_premature_signoff_progress_is_not_a_review(self):
        (self.sessions/'premature-signoff').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('must not issue',f['state']['exception']['reason'])
        with self.store._db() as db:self.assertEqual(1,db.execute('select count(*) from reviews').fetchone()[0])

    def test_missing_turn_is_not_retried_or_consumed_as_progress(self):
        (self.sessions/'missing-turn').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertEqual(0,f['state']['serial_quality_reading']['index'])
        self.helper.run_flow([self.fid]);self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_resumed_wrong_sid_is_not_adopted(self):
        (self.sessions/'changed-sid').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertEqual(1,f['state']['serial_quality_reading']['index'])
        self.assertEqual(3,len(self.store.dashboard()['attempts']))

    def test_public_checkpoint_without_actual_visible_anchor_is_rejected(self):
        (self.sessions/'missing-anchor').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('actual visible tool readback',f['state']['exception']['reason'])
        self.assertEqual(0,f['state']['serial_quality_reading']['index'])

    def test_first_progress_cannot_reuse_an_old_signed_sid(self):
        (self.sessions/'reuse-sid.txt').write_text(self.source['cli_sid']);self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('reused a previous',f['state']['exception']['reason'])

    def test_page_omission_is_retained_as_protocol_failure_not_acceptance(self):
        (self.sessions/'omit-page').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('every assigned page',f['state']['exception']['reason'])
        self.assertEqual(1,f['state']['serial_quality_reading']['index'])
        self.assertEqual(3,len(self.store.dashboard()['attempts']))

    def test_final_unresolved_is_one_real_signature_and_no_extra_turn(self):
        (self.sessions/'final-inconclusive').touch();self.authorize();f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('signed inconclusive',f['state']['exception']['reason'])
        self.assertEqual(4,len(self.store.dashboard()['attempts']));self.helper.run_flow([self.fid]);self.assertEqual(4,len(self.store.dashboard()['attempts']))

    def test_current_source_and_plan_drift_reject(self):
        f=self.first_progress();self.assertEqual(1,f['state']['serial_quality_reading']['index'],f['state'].get('exception'))
        Path(self.plan['pages'][0]['path']).write_text('changed necessary source')
        f=self.helper.run_flow([self.fid])['flows'][0];self.assertEqual('exception',f['state']['status']);self.assertIn('page source bytes changed',f['state']['exception']['reason'])
        self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_previous_full_archive_tamper_stops_before_another_attempt(self):
        f=self.first_progress();self.assertEqual(1,f['state']['serial_quality_reading']['index'],f['state'].get('exception'))
        entry=f['state']['serial_quality_reading']['turns'][0]
        original=Path(entry['archive'])/'artifacts/reading-progress-000.json'
        original.write_text(original.read_text()+'\\nchanged archive')
        f=self.helper.run_flow([self.fid])['flows'][0]
        self.assertEqual('exception',f['state']['status']);self.assertIn('full archive',f['state']['exception']['reason'])
        self.assertEqual(2,len(self.store.dashboard()['attempts']))

    def test_model_workspace_or_policy_cannot_change_between_turns(self):
        f=self.first_progress();self.assertEqual(1,f['state']['serial_quality_reading']['index'],f['state'].get('exception'))
        previous=f['state']['serial_quality_reading']['turns'][0]['seal'];root=Path(previous['path']).parent;m=json.loads((root/'manifest.json').read_text())
        m['quality_session'].update(index=1,previous=previous);m['resume_sid']=f['state']['serial_quality_reading']['turns'][0]['cli_sid']
        validate_session(m,root=root,before_run=True)
        for field,value in [('policy_version','foreign-policy'),('cwd',str(self.root/'foreign'))]:
            changed=copy.deepcopy(m);changed[field]=value
            with self.subTest(field=field),self.assertRaises(TaskflowError):validate_session(changed,root=root,before_run=True)
        changed=copy.deepcopy(m);changed['model']['requested']='another-model'
        with self.assertRaises(TaskflowError):validate_session(changed,root=root,before_run=True)

    def test_native_store_role_and_freshness_guards_are_explicit(self):
        self.authorize();f=advance_workflow(self.store,self.fid);root=Path(f['state']['current_runtime'])
        m=json.loads((root/'manifest.json').read_text())
        native=copy.deepcopy(m);native['mode']='native_role'
        with self.assertRaisesRegex(TaskflowError,'session store differs'):validate_session(native)
        wrong=copy.deepcopy(m);wrong['role']='review-facts'
        with self.assertRaisesRegex(TaskflowError,'original quality role'):validate_session(wrong)
        wrong=copy.deepcopy(m);wrong['resume_sid']=self.source['cli_sid']
        with self.assertRaisesRegex(TaskflowError,'must start fresh'):validate_session(wrong)

    def test_no_implicit_extra_page_or_final_turn(self):
        changed=copy.deepcopy(self.plan);changed['turns'][1]['page_ids'].append('foreign')
        write_json(self.plan_path,changed)
        with self.assertRaises(TaskflowError):validate_plan(evidence_ref(self.plan_path,'synthetic rehashed invalid plan'),self.source['candidate_binding'])
        changed=copy.deepcopy(self.plan);changed['max_final_turns']=2;write_json(self.plan_path,changed)
        with self.assertRaises(TaskflowError):validate_plan(evidence_ref(self.plan_path,'synthetic rehashed invalid final budget'),self.source['candidate_binding'])


if __name__=='__main__':unittest.main()
