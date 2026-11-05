"""A fresh direct human decision authorizes one explicitly bound successor."""
import copy
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_author import _identity
from taskflow.execution_lifecycle import register_lifecycle
from taskflow.execution_runner import archive_full_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flows
from tests import test_execution_quality_external_state_recovery as prior

FAIL_EXTERNAL = """
if manifest['model']['requested']=='future-fixture-cli' and '/network-restored-quality/' in manifest['cwd']:
 print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
 message='stream disconnected before completion: error sending request'
 print(json.dumps({'type':'error','message':message}),flush=True)
 print(json.dumps({'type':'turn.failed','error':{'message':message}}),flush=True)
 sys.exit(1)
"""

class DirectHumanQualitySuccessorTests(unittest.TestCase):
 def setUp(self):
  case=prior.ExternalStateQualityRecoveryTests()
  with patch.object(flows,'ROLE_FIXTURE',FAIL_EXTERNAL+flows.ROLE_FIXTURE):case.setUp()
  self.addCleanup(case.doCleanups);self.case=case;self.store=case.store
  source=case.register();case.run_cli(source['flow_id']);self.source=self.store.get_flow(source['flow_id'])
  self.assertEqual('exception',self.source['state']['status']);child=self.store.get_flow(self.source['state']['child_flow_id'])
  failed=self.store.list_attempts(child['state']['exception']['work_id'])[-1];self.failed=failed
  archive_full_attempt(Path(failed['runtime_path']),Path(child['definition']['directory'])/'archives'/failed['attempt_id'])
  self.tech={'source_parent_flow_id':source['flow_id'],'source_parent_definition_digest':source['definition_digest'],'source_child_flow_id':child['flow_id'],'source_child_definition_digest':child['definition_digest'],'failed_attempt_id':failed['attempt_id']}
  self.definition=copy.deepcopy(self.source['definition']);self.definition.update(flow_id=_identity('neutral','quality-technical-'+failed['attempt_id']),directory=str(case.fixture.case.root/'direct-quality'))
  self.human={'schema_version':1,'authority_kind':'direct_human_instruction_in_q1_controller_chat','additional_quality_attempt_limit':1,'candidate_id':case.authority['candidate_binding']['candidate_id'],**{'candidate_'+k:case.authority['candidate_binding'][k] for k in ('input_digest','content_digest','evaluation_digest')},'quality_model':'future-fixture-cli/fixture, thinking enabled','sole_dispatcher':'fixture-controller','source_thread_id':'neutral-controller','human_quote':'I authorize one quality call and role materials.','scope':'same neutral candidate','provider_data_and_cost':'explicitly agreed'}
  self.human_path=case.fixture.case.root/'human.json';self.binding_path=case.fixture.case.root/'direct-binding.json';self.authority_path=case.fixture.case.root/'direct-authority.json';self.refresh()
 def refresh(self):
  write_json(self.human_path,self.human);event=evidence_ref(self.human_path,'explicit fresh human decision')
  write_json(self.binding_path,{'schema_version':1,'kind':'direct_human_quality_recovery_v1','external_state_event':event,'previous_technical_authority':self.source['definition']['technical_quality_successor']['authority']})
  self.definition['flow_id']=_identity('neutral','quality-human-'+event['sha256'])
  authority=copy.deepcopy(self.case.authority);authority.update(technical_source=self.tech,decision=event,external_state_recovery=evidence_ref(self.binding_path,'one direct decision'))
  write_json(self.authority_path,authority);self.definition['technical_quality_successor']={**self.tech,'authority':evidence_ref(self.authority_path,'one direct human allowance')}
 def register(self):return register_lifecycle(self.store,self.definition,[*self.case.fixture.case.refs,self.definition['technical_quality_successor']['authority']])
 def test_one_actual_fresh_successor_and_idempotency(self):
  before=len(self.store.dashboard()['attempts']);source=copy.deepcopy(self.source);flow=self.register();self.case.run_cli(flow['flow_id']);result=self.store.get_flow(flow['flow_id']);self.assertEqual('accepted',result['state']['status'],result);self.assertEqual(before+1,len(self.store.dashboard()['attempts']));self.assertEqual(5,len(result['state']['technical_quality_binding']['excluded_sids']));self.assertEqual(source,self.store.get_flow(source['flow_id']));self.register();self.case.run_cli(flow['flow_id']);self.assertEqual(before+1,len(self.store.dashboard()['attempts']))
 def test_same_human_bytes_concurrent_distinct_sources_have_one_sqlite_identity(self):
  original=copy.deepcopy(self.definition);alternate=copy.deepcopy(original)
  alternate['technical_quality_successor']['source_parent_flow_id']='different-parent'
  alternate['technical_quality_successor']['failed_attempt_id']='different-failed-attempt'
  alternate['directory'] += '-alternate'
  refs=[*self.case.fixture.case.refs,original['technical_quality_successor']['authority']]
  gate=threading.Barrier(2)
  def submit(definition):
   gate.wait()
   try:
    return ('registered',register_lifecycle(self.store,definition,refs))
   except Exception as exc:
    return ('rejected',type(exc).__name__)
  # Isolate the actual SQLite registration race; source validation is covered
  # by the end-to-end and negative cases, without fabricating another source.
  with patch('taskflow.execution_author.validate_technical_quality_successor',return_value=None):
   with ThreadPoolExecutor(max_workers=2) as pool:
    outcomes=list(pool.map(submit,(original,alternate)))
   self.assertEqual(['registered','rejected'],sorted(x[0] for x in outcomes))
   stored=self.store.get_flow(original['flow_id'])
   self.assertIn(stored['definition'],(original,alternate))
   self.assertEqual(stored,register_lifecycle(self.store,stored['definition'],refs))

 def test_wrong_human_binding_and_alias_rejected(self):
  for field,value in [('additional_quality_attempt_limit',True),('candidate_content_digest','wrong'),('quality_model','other/high, thinking enabled'),('sole_dispatcher','other')]:
   old=self.human[field];self.human[field]=value;self.refresh()
   with self.subTest(field=field),self.assertRaises(TaskflowError):self.register()
   self.human[field]=old;self.refresh()
  self.definition['flow_id']='alias'
  with self.assertRaises(TaskflowError):self.register()
