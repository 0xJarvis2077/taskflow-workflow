"""Neutral real subprocess/collection/SQLite fixtures, with no external model.

The approved isolated adapter is explicitly a local fixture: it copies only
public bytes and produces locked plans but does not claim OS probe coverage.
"""

import copy
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_author import _public_root, advance_author
from taskflow.execution_flow import _identity
from taskflow.execution_lifecycle import _lifecycle_lock, _receive, advance_lifecycle, register_lifecycle, resume_path_transport
from taskflow.execution_public_continuation import CAPACITY, KIND, REASON, validate_public_capacity_continuation
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_author as author_tests
from tests import test_execution_experiment_author as experiment_tests
from tests import test_execution_author_factory as factory_tests


ADAPTER = r'''
import json,pathlib,shutil,sys
sys.path.insert(0,REPOSITORY)
from taskflow.contracts import PUBLIC_ROLES,canonical
from taskflow.execution_runner import _info
from taskflow.packages import check_task

def prepare_draft(task,root,config,denied,timeout):
 root=pathlib.Path(root);root.mkdir(parents=True)
 for d in ('work','scratch','control'):(root/d).mkdir()
 declaration=json.loads((task/'task.json').read_text());actual=check_task(task,write_report=False)
 public=[]
 for row in declaration['files']:
  if row['role'] not in PUBLIC_ROLES:continue
  target=root/'work'/row['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(task/row['path'],target)
  public.append({'id':row['id'],'path':row['path'],'role':row['role'],**_info(target)})
 package={'input_digest':actual['input_digest'],'files':public}
 (root/'work/package.json').write_bytes(canonical(package)+b'\n')
 tree={row['path']:_info(root/'work'/row['path']) for row in public};tree['package.json']=_info(root/'work/package.json')
 plan={'root':str(root),'model':{'requested':config['model'],'reasoning_effort':config['reasoning_effort']},
       'public_tree':tree,'config':config,'denied':[{'path':str(p)} for p in [*denied,pathlib.Path(config['repository']),pathlib.Path(config['codex_home'])]],
       'source_binding':{'input_digest':actual['input_digest']}}
 (root/'plan.json').write_bytes(canonical(plan)+b'\n')
 (root/'plan-lock.json').write_bytes(canonical(_info(root/'plan.json')))
 (root/'source-lock.json').write_text('{}');(root/'prepared-lock.json').write_text('{}')
 (root/'request.md').write_text('Neutral strictly public reconstruction request.\n')
 (root/'runner.py').write_text(RUNNER)

def probe(root):
 (root/'probe.json').write_text('{"passed":true,"model_calls":0}')
 (root/'ready.json').write_bytes(canonical({'probe_file':'probe.json','probe':_info(root/'probe.json'),'source_lock':_info(root/'source-lock.json')}))

def validate(root, before_run=True):
 plan=json.loads((root/'plan.json').read_text())
 assert json.loads((root/'plan-lock.json').read_text())==_info(root/'plan.json')
 assert plan['public_tree']=={str(p.relative_to(root/'work')):_info(p) for p in (root/'work').rglob('*') if p.is_file() and 'deliverables' not in p.relative_to(root/'work').parts}
 return plan
'''

INNER_RUNNER = r'''
import json,pathlib,sys,uuid
root=pathlib.Path(__file__).parent
if sys.argv[1]=='validate':sys.exit(0)
plan=json.loads((root/'plan.json').read_text());marker=pathlib.Path(plan['config']['capacity_marker'])
failed=not marker.exists();marker.write_text('Actual neutral first capacity failure retained.')
events=[{'type':'thread.started','thread_id':str(uuid.uuid4())}]
if failed:
 events += [{'type':'error','message':CAPACITY},{'type':'turn.failed','error':{'message':CAPACITY}}]
else:
 p=root/'work/deliverables';p.mkdir(exist_ok=True);(p/'quality-note.md').write_text('Actual neutral public reconstruction output.')
 events += [{'type':'item.completed','item':{'type':'agent_message','text':'Actual neutral normal completion.'}},{'type':'turn.completed'}]
run=root/'run';run.mkdir();(run/'trace.jsonl').write_text('\n'.join(json.dumps(x) for x in events)+'\n')
(run/'request.md').write_bytes((root/'request.md').read_bytes())
(run/'attempt.json').write_text(json.dumps({'status':'incomplete' if failed else 'completed','returncode':1 if failed else 0}))
'''


def install_capacity_adapter(case, failure=CAPACITY):
    adapter = case.root / 'approved-neutral-public-adapter.py'
    runner = 'CAPACITY=' + repr(failure) + '\n' + INNER_RUNNER
    adapter.write_text('REPOSITORY=' + repr(str(Path(__file__).resolve().parents[1])) + '\nRUNNER=' + repr(runner) + '\n' + ADAPTER)
    cli = case.root / 'neutral-cli'
    cli.write_text('Explicit neutral CLI identity fixture; no external model.')
    template = read_json(case.template)
    role = template['workflows'][0]['definition']['roles']['public_rebuild']
    role['prepare'].update(adapter=str(adapter), sha256=sha256(adapter.read_bytes()), checks=[], config={
        'repository':str(case.root/'repository'), 'codex_home':str(case.root/'home'), 'cli':str(cli),
        'capacity_marker':str(case.root/'capacity-marker'), 'model':'OLD_MODEL','reasoning_effort':'OLD_EFFORT'})
    write_json(case.template, template)
    return adapter


def failed_public(owner, case, record):
    record = advance_author(case.store, record)
    record = advance_author(case.store, record)
    owner.addCleanup(shutil.rmtree, _public_root(record), True)
    record = advance_author(case.store, record)
    failed = execution.run_worker(case.store, 'neutral-first-public', True, [record['state']['public_work_id']])
    owner.assertEqual('failed', failed['state'])
    record = advance_lifecycle(case.store, record['flow_id'])
    owner.assertEqual('exception', record['state']['status'])
    record = advance_lifecycle(case.store, record['flow_id'])
    owner.assertTrue(record['state'].get('receipt'))
    return record, failed


def later_availability(owner, case, record, adapter):
    # A real later successful isolated CLI-format attempt on the baseline,
    # with the same immutable CLI/model/policy, is separate from this candidate.
    role = __import__('taskflow.execution_author',fromlist=['_roles'])._roles(case.store,record['definition'])['public_rebuild']
    inner = case.root/'availability-inner'
    specmod = __import__('importlib.util',fromlist=['spec_from_file_location'])
    ms=specmod.spec_from_file_location('neutral_availability_adapter',adapter);mod=specmod.module_from_spec(ms);ms.loader.exec_module(mod)
    mod.prepare_draft(case.task,inner,role['prepare']['config'],[],15);mod.probe(inner)
    available_root=case.root/'availability-runtime'
    execution.enqueue_spec(case.store,{'work':{'work_id':'later-availability','task_id':'neutral','candidate_id':'baseline',
        'role':'quality','policy_id':'fixture-v1','intent_id':'later-availability-intent','purpose':'Separate actual same-CLI normal completion',
        'dispatch_owner':'fixture-controller','evidence':case.refs},'runner':{'intent_id':'later-availability-intent','policy_version':'fixture-v1',
        'role':'quality','mode':'existing_isolated','model':role['runner']['model'],'reasoning_effort':role['runner']['reasoning_effort'],
        'argv':[str(Path(sys.executable).resolve())],
        'timeout_seconds':15,'cwd':str(inner/'work'),'isolated':{'runtime':str(inner),'flavor':'quality','python':str(Path(sys.executable).resolve())},
        'outputs':['deliverables/quality-note.md'],'inputs':[],'request_sections':[]}},available_root)
    available=execution.run_worker(case.store,'neutral-later-availability',True,['later-availability'])
    owner.assertEqual('completed',available['state'])
    return available


def make_capacity_fixture(owner, *, failure=CAPACITY):
    case = author_tests.AuthorSuccessorTests()
    case.setUp()
    owner.addCleanup(case.doCleanups)
    adapter = install_capacity_adapter(case, failure)
    case.run_source('candidate_complete')
    record, failed = failed_public(owner, case, case.register())
    candidate = case.store.get_candidate(record['state']['candidate_id'])
    available = later_availability(owner, case, record, adapter)
    return {'case':case,'store':case.store,'record':record,'failed':failed,'available':available,'candidate':candidate}


def make_real_aliased_capacity_fixture(owner):
    """Actual factory metadata input and shared alias adoption before failure."""
    from taskflow.execution_author import start_author_from_packet
    from taskflow.execution_author_packet import KIND as ALIAS_KIND, TARGET

    case=factory_tests.AuthorFactoryTests();case.setUp();owner.addCleanup(case.doCleanups)
    case.factory();adapter=install_capacity_adapter(case)
    case.config['role_template_ref']=evidence_ref(case.template,'bound neutral public role template')
    def enqueue(store,spec,runtime,**options):
        converted=copy.deepcopy(spec);converted['runner'].update(mode='test_fixture',
            argv=[str(Path(sys.executable).resolve()),str(case.script),str(case.task),'candidate_complete','0',str(Path(__file__).resolve().parents[1])])
        converted['runner'].pop('native_permissions',None)
        return execution.enqueue_spec(store,converted,runtime,**options)
    with patch('taskflow.execution_author.enqueue_spec',side_effect=enqueue):
        prepared=start_author_from_packet(case.store,**case.kwargs())
    case.attempt=execution.run_worker(case.store,'neutral-original-author',True,[prepared['work_id']])
    owner.assertEqual('completed',case.attempt['state'])
    binding={key:case.attempt[key] for key in ('work_id','intent_id','candidate_id','task_id')}
    definition={'schema_version':1,'kind':'author_quality_v1','flow_id':case.attempt['intent_id']+'-author-quality',
        'task_id':'neutral','owner':'fixture-controller','policy_id':'fixture-v1','directory':str(case.root/'parent'),
        'authorization':case.packet_ref,'source_author_attempt':case.attempt['attempt_id'],'source_author_binding':binding,
        'source_author_work':str(case.directory/'work'),'role_template_ref':case.config['role_template_ref'],
        'business_item':case.packet_data['items'][0],'author_contract':case.packet_data['output_contract'],
        'next':case.packet_data['preauthorized_normal_successors']}
    parent=register_lifecycle(case.store,definition,[case.packet_ref])
    lifecycle=case.directory/'lifecycle-spec.json';write_json(lifecycle,{'definition':definition,'evidence':[case.packet_ref]})
    helper=experiment_tests.AuthorExperimentTests();helper.root=case.root
    ed=helper.definition(case,parent);ed['technical'].update(formal_runtime_root=str(case.root/'formal'),publication_root=str(case.root/'publication'))
    authority=Path(ed['authorization']['path']);body=read_json(authority);staged=case.directory/'runtime/inputs'/TARGET
    body['items'][0]['source_business_packet']=str(staged);write_json(authority,body)
    er=evidence_ref(authority,'same immutable experiment purpose using actual staged packet')
    ed.update(authorization=er,business_packet_ref=er)
    ex=register_lifecycle(case.store,ed,[er]);ex=advance_lifecycle(case.store,ex['flow_id']);ex=advance_lifecycle(case.store,ex['flow_id'])
    owner.assertIn('original business packet',ex['state']['exception']['reason'])
    note=case.root/'explicit-source-metadata-adoption.json';write_json(note,{'controller':'fixture-controller','scope':'Exact original source/staged alias only; unchanged purpose.'})
    spec={'schema_version':1,'kind':ALIAS_KIND,'source_author_flow_id':parent['flow_id'],
        'source_author_attempt':case.attempt['attempt_id'],'authorization':er,
        'lifecycle_spec':evidence_ref(lifecycle,'original retained lifecycle'),
        'manifest':evidence_ref(case.directory/'runtime/manifest.json','actual original author manifest'),
        'source':case.packet_ref,'staged':evidence_ref(staged,'actual locked source metadata alias'),
        'evidence':[evidence_ref(note,'explicit metadata adoption')]}
    sidecar=case.root/'packet-transport.json';write_json(sidecar,spec);tr=evidence_ref(sidecar,'actual explicit packet transport')
    ex=resume_path_transport(case.store,ex['flow_id'],{'action':'resume_author_packet_source_metadata_transport',
        'reason':'Adopt exact real source/staged metadata alias before public role.','packet_transport':tr,
        'evidence':[tr,evidence_ref(note,'explicit metadata-only adoption')]})
    parent,failed=failed_public(owner,case,parent)
    ex=advance_lifecycle(case.store,ex['flow_id']);ex=advance_lifecycle(case.store,ex['flow_id'])
    owner.assertEqual('exception',ex['state']['status']);owner.assertIn('fresh public reconstruction failed',ex['state']['exception']['reason'])
    available=later_availability(owner,case,parent,adapter)
    return {'case':case,'store':case.store,'record':parent,'failed':failed,'available':available,
        'candidate':case.store.get_candidate(parent['state']['candidate_id']),'experiment':ex,'transport':tr,'transport_path':sidecar}


class PublicCapacityContinuationTests(unittest.TestCase):
    def resolution(self, value):
        return {'action':'resume_failed_public_capacity_once','reason':'One explained same-model public continuation after real later completion.',
            'failed_attempt_id':value['failed']['attempt_id'],'availability_attempt_id':value['available']['attempt_id'],
            'evidence':value['case'].refs}

    def fixture(self):
        return make_capacity_fixture(self)

    def validate(self,v):
        return validate_public_capacity_continuation(v['store'],v['record'],failed_attempt_id=v['failed']['attempt_id'],availability_attempt_id=v['available']['attempt_id'])

    @contextmanager
    def edited(self,v,table,identity,change):
        with v['store']._db(True) as db:
            raw=db.execute('SELECT data FROM '+table+' WHERE id=?',(identity,)).fetchone()['data'];row=json.loads(raw);change(row)
            db.execute('UPDATE '+table+' SET data=? WHERE id=?',(canonical(row).decode(),identity))
        try:yield
        finally:
            with v['store']._db(True) as db:db.execute('UPDATE '+table+' SET data=? WHERE id=?',(raw,identity))

    def test_real_failed_once_new_identity_normal_completion_and_no_author_repeat(self):
        v=self.fixture();s=v['store'];old=v['failed'];root=Path(old['runtime_path'])
        before={str(root/n):(root/n).read_bytes() for n in ('manifest.json','request.md','observed-trace.jsonl','collection.json')}
        original=s.get_attempt(old['attempt_id']);r=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        self.assertEqual('public_prepare',r['state']['stage']);self.assertEqual(original,s.get_attempt(old['attempt_id']))
        self.assertTrue((Path(r['state']['public_capacity_continuation']['failed_archive'])/'supplemental/receipt.json').exists())
        self.addCleanup(shutil.rmtree,_public_root(r),True)
        self.assertNotEqual(_public_root(v['record']),_public_root(r))
        wrong=copy.deepcopy(r);wrong['state']['public_capacity_continuation']['ordinal']=True
        with self.assertRaises(TaskflowError):_public_root(wrong)
        with self.assertRaises(TaskflowError):resume_path_transport(s,r['flow_id'],self.resolution(v))
        roles=__import__('taskflow.execution_author',fromlist=['_roles'])._roles(s,r['definition'])
        changed=copy.deepcopy(roles);changed['public_rebuild']['runner']['model']='other-unapproved-model'
        with patch('taskflow.execution_author._roles',return_value=changed),self.assertRaisesRegex(TaskflowError,'original approved role'):
            advance_author(s,r)
        ready=advance_author(s,r);work=ready['state']['public_work_id'];self.assertNotEqual(old['work_id'],work)
        again=advance_author(s,ready);self.assertEqual(work,again['state']['public_work_id'])
        new=execution.run_worker(s,'neutral-once-public',True,[work]);self.assertEqual('completed',new['state'])
        self.assertNotEqual(old['attempt_id'],new['attempt_id']);self.assertNotEqual(old['intent_id'],new['intent_id']);self.assertNotEqual(old['cli_sid'],new['cli_sid'])
        with self.edited(v,'attempts',new['attempt_id'],lambda x:x.update(cli_sid=old['cli_sid'])),self.assertRaisesRegex(TaskflowError,'original failed CLI'):
            advance_author(s,again)
        done=advance_author(s,again);self.assertEqual('child_quality',done['state']['stage'])
        self.assertEqual(v['case'].attempt['attempt_id'],done['definition']['source_author_attempt'])
        self.assertEqual(v['record']['definition_digest'],done['definition_digest'])
        self.assertEqual(before,{p:Path(p).read_bytes() for p in before})
        self.assertEqual(original,s.get_attempt(old['attempt_id']))

    def test_failure_identity_state_hold_receipt_lock_and_cas_fail_closed(self):
        v=self.fixture();s=v['store'];r=v['record'];before=s.events()
        with _lifecycle_lock(s,r['flow_id']),self.assertRaises(TaskflowError):resume_path_transport(s,r['flow_id'],self.resolution(v))
        for changes in ({'pending_business_scope':{}},{'status':'active'},{'stage':'public_prepare'},{'public_capacity_continuation':{'kind':KIND}}, {'child_flow_id':'already-child'}):
            fake={**r,'state':{**r['state'],**changes}}
            with self.subTest(changes=changes),self.assertRaises(TaskflowError):
                validate_public_capacity_continuation(s,fake,failed_attempt_id=v['failed']['attempt_id'],availability_attempt_id=v['available']['attempt_id'])
        with self.assertRaises(TaskflowError):validate_public_capacity_continuation(s,r,failed_attempt_id=v['available']['attempt_id'],availability_attempt_id=v['failed']['attempt_id'])
        actual=ExecutionStore.update_flow
        def conflict(self,identity,revision,state,event):return actual(self,identity,revision+1,state,event)
        with patch.object(ExecutionStore,'update_flow',new=conflict),self.assertRaises(TaskflowError):resume_path_transport(s,r['flow_id'],self.resolution(v))
        self.assertEqual(before,s.events());self.assertEqual(r,s.get_flow(r['flow_id']))

    def test_actual_failed_and_availability_bindings_cannot_be_relabelled(self):
        v=self.fixture()
        for identity,change in ((v['failed']['attempt_id'],lambda x:x.update(state='uncertain')),
            (v['failed']['attempt_id'],lambda x:x.update(cli_sid=v['case'].attempt['cli_sid'])),
            (v['available']['attempt_id'],lambda x:x.update(state='failed')),
            (v['available']['attempt_id'],lambda x:x.update(policy_digest='0'*64)),
            (v['available']['attempt_id'],lambda x:x.update(request_digest='0'*64))):
            with self.subTest(identity=identity),self.edited(v,'attempts',identity,change),self.assertRaises(TaskflowError):self.validate(v)
        r=v['record'];path=Path(r['state']['receipt']);raw=path.read_bytes()
        try:
            body=read_json(path);body['controller']='foreign';write_json(path,body)
            with self.assertRaises(TaskflowError):self.validate(v)
        finally:path.write_bytes(raw)

    def test_source_public_or_original_trace_byte_drift_is_rejected(self):
        v=self.fixture();p=Path(v['failed']['runtime_path'])/'observed-trace.jsonl';raw=p.read_bytes()
        try:
            p.write_bytes(raw.replace(CAPACITY.encode(),b'different actual failure'))
            with self.assertRaises(Exception):self.validate(v)
        finally:p.write_bytes(raw)
        source=next(x for x in v['candidate']['files'] if x['role']=='prompt');path=Path(source['path']);raw=path.read_bytes()
        try:
            path.write_bytes(raw+b'changed')
            with self.assertRaises(TaskflowError):self.validate(v)
        finally:path.write_bytes(raw)

    def test_existing_current_execution_or_formal_decision_is_not_replaced(self):
        v=self.fixture();s=v['store'];c=v['candidate']['candidate_id']
        # Adversarial preexisting metadata cannot be accepted as no footprint;
        # this does not manufacture an independent quality signature.
        with s._db(True) as db:
            row={'decision_id':'preexisting-formal-purpose','task_id':'neutral','candidate_id':c,'action':'experiment'}
            db.execute('INSERT INTO decisions VALUES(?,?,?)',(row['decision_id'],'neutral',canonical(row).decode()))
        with self.assertRaisesRegex(TaskflowError,'experiment decision'):self.validate(v)

    def test_noncapacity_failure_and_existing_other_candidate_execution_are_not_redrawn(self):
        v=make_capacity_fixture(self,failure='Actual distinct technical or business error')
        with self.assertRaisesRegex(TaskflowError,'exact capacity failure'):self.validate(v)
        v=self.fixture();s=v['store']
        s.enqueue('existing-other-quality','neutral',v['candidate']['candidate_id'],'quality','fixture-v1',
            intent_id='existing-other-quality-intent',purpose='Existing independent execution cannot be replaced.',
            request_digest='0'*64,evidence=v['case'].refs,dispatch_owner='fixture-controller')
        before=s.events()
        with self.assertRaisesRegex(TaskflowError,'another current-candidate execution'):self.validate(v)
        self.assertEqual(before,s.events())

    def dependent(self,v,identity='next-experiment'):
        helper=experiment_tests.AuthorExperimentTests();helper.root=v['case'].root
        definition=helper.definition(v['case'],v['record'])
        authority=read_json(Path(definition['authorization']['path']))
        authority['items'][0]['first_experiment_business_unknown']='Distinct neutral authorized purpose for '+identity
        path=helper.root/(identity+'-authority.json');write_json(path,authority)
        ref=evidence_ref(path,'exact distinct neutral experiment authority')
        definition.update(flow_id=identity,authorization=ref,business_packet_ref=ref,directory=str(helper.root/identity))
        definition['technical'].update(formal_runtime_root=str(helper.root/(identity+'-formal')),publication_root=str(helper.root/(identity+'-publication')))
        record=register_lifecycle(v['store'],definition,[definition['authorization']])
        record=advance_lifecycle(v['store'],record['flow_id'])
        self.assertEqual('exception',record['state']['status'])
        record=advance_lifecycle(v['store'],record['flow_id'])
        self.assertTrue(record['state'].get('receipt'))
        return record

    def dependency_resolution(self,v):
        return {'action':'resume_resolved_public_capacity_dependency','reason':'Exact propagated first-public capacity exception resolved once.',
            'evidence':v['case'].refs}

    def test_original_dependent_experiment_resumes_only_to_wait_same_source_and_pause_is_retained(self):
        v=self.fixture();s=v['store'];ex=self.dependent(v)
        from taskflow.execution_experiment import _node
        s.set_control('work:'+_node(ex,'solver'),'pause','The exact original first target purpose remains held.')
        with s._db() as db:controls=[tuple(x) for x in db.execute('SELECT * FROM controls')]
        before_ex=Path(ex['state']['receipt']).read_bytes();before=s.get_attempt(v['failed']['attempt_id'])
        with self.assertRaises(TaskflowError):resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        with _lifecycle_lock(s,source['flow_id']),self.assertRaises(TaskflowError):resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        resumed=resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        self.assertEqual('active',resumed['state']['status']);self.assertEqual('experiment_v1',resumed['state']['stage'])
        self.assertEqual(ex['definition_digest'],resumed['definition_digest'])
        self.assertEqual(ex['state']['exception'],resumed['state']['history'][-1]['previous_exception'])
        wait=advance_lifecycle(s,ex['flow_id']);self.assertEqual('source_author_quality_not_received',wait['state']['waiting']['reason'])
        self.assertEqual(before,s.get_attempt(before['attempt_id']))
        self.assertEqual(before_ex,Path(ex['state']['receipt']).read_bytes())
        with s._db() as db:self.assertEqual(controls,[tuple(x) for x in db.execute('SELECT * FROM controls')])
        with self.assertRaises(TaskflowError):resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))

    def test_dependency_business_hold_original_exception_and_formal_footprints_remain_strict(self):
        v=self.fixture();s=v['store'];ex=self.dependent(v)
        source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        original=s.events()
        from taskflow.execution_public_continuation import resume_public_capacity_dependency
        for field,value in (('pending_business_scope',{}),('exception',{'stage':'experiment_v1','reason':'Actual independent unresolved business hold'}),('current_work_id','already-dispatched')):
            forged={**ex,'state':{**ex['state'],field:value}}
            with self.subTest(field=field),self.assertRaises(TaskflowError):resume_public_capacity_dependency(s,forged,self.dependency_resolution(v),v['case'].refs)
        path=Path(ex['definition']['technical']['publication_root']);path.mkdir()
        try:
            with self.assertRaisesRegex(TaskflowError,'preparation/publication'):resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        finally:path.rmdir()
        self.assertEqual(original,s.events());self.assertEqual(ex,s.get_flow(ex['flow_id']))

    def test_dependent_source_original_event_and_cas_cannot_be_replaced(self):
        v=self.fixture();s=v['store'];ex=self.dependent(v)
        source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        state=copy.deepcopy(source['state']);state['public_capacity_continuation']['original_state_digest']='0'*64
        state['history'][-1]['binding']=state['public_capacity_continuation']
        from taskflow.execution_public_continuation import resume_public_capacity_dependency
        actual_get_flow=s.get_flow
        with patch.object(s,'get_flow',side_effect=lambda identity:{**source,'state':state} if identity==source['flow_id'] else actual_get_flow(identity)):
            with self.assertRaisesRegex(TaskflowError,'immutable event'):
                resume_public_capacity_dependency(s,ex,self.dependency_resolution(v),v['case'].refs)
        before=s.events();actual=ExecutionStore.update_flow
        def conflict(self,identity,revision,state,event):return actual(self,identity,revision+1,state,event)
        with patch.object(ExecutionStore,'update_flow',new=conflict),self.assertRaises(TaskflowError):
            resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        self.assertEqual(before,s.events());self.assertEqual(ex,s.get_flow(ex['flow_id']))

    def test_two_registered_different_purposes_do_not_share_one_public_continuation(self):
        v=self.fixture();self.dependent(v,'original-experiment');self.dependent(v,'different-purpose-experiment')
        before=v['store'].events()
        with self.assertRaisesRegex(TaskflowError,'ambiguous|unique dependent'):
            resume_path_transport(v['store'],v['record']['flow_id'],self.resolution(v))
        self.assertEqual(before,v['store'].events())

    def test_real_registered_source_metadata_alias_preserves_exact_dependent_purpose(self):
        v=make_real_aliased_capacity_fixture(self);s=v['store'];ex=v['experiment']
        original_ex=copy.deepcopy(ex);before_author=s.get_attempt(v['case'].attempt['attempt_id'])
        source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        resumed=resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        self.assertEqual('active',resumed['state']['status'])
        self.assertEqual(original_ex['definition_digest'],resumed['definition_digest'])
        self.assertEqual(v['transport'],resumed['state']['source_business_packet_transport'])
        self.assertEqual(before_author,s.get_attempt(before_author['attempt_id']))

    def test_parent_only_none_binding_cannot_adopt_a_later_new_experiment(self):
        v=self.fixture();s=v['store'];source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        self.assertIsNone(source['state']['public_capacity_continuation']['dependent_experiment'])
        helper=experiment_tests.AuthorExperimentTests();helper.root=v['case'].root
        definition=helper.definition(v['case'],source);definition['technical'].update(
            formal_runtime_root=str(helper.root/'late-formal'),publication_root=str(helper.root/'late-publication'))
        late=register_lifecycle(s,definition,[definition['authorization']])
        # Explicit adversarial late clone: the genuine old exception belonged
        # to the earlier source, not to a registered dependent at authorization.
        state={**late['state'],'status':'exception','exception':{'stage':'experiment_v1','reason':
            'source author-quality flow stopped with its original exception: '+str(v['record']['state']['exception'])}}
        late=s.update_flow(late['flow_id'],late['revision'],state,{'kind':'neutral_adversarial_late_exception_clone'})
        late=_receive(s,late);before=s.events()
        with self.assertRaisesRegex(TaskflowError,'uniquely bound original'):
            resume_path_transport(s,late['flow_id'],self.dependency_resolution(v))
        self.assertEqual(before,s.events());self.assertEqual(late,s.get_flow(late['flow_id']))

    def test_bound_dependent_identity_authority_event_and_alias_cannot_be_cloned(self):
        v=make_real_aliased_capacity_fixture(self);s=v['store'];ex=v['experiment']
        source=resume_path_transport(s,v['record']['flow_id'],self.resolution(v))
        proof=source['state']['public_capacity_continuation'];bound=proof['dependent_experiment']
        self.assertEqual(ex['flow_id'],bound['flow_id']);self.assertEqual(ex['definition_digest'],bound['definition_digest'])
        self.assertEqual(ex['definition']['authorization'],bound['authority'])
        self.assertEqual(ex['event_seq'],bound['received_event']['event_seq'])
        from taskflow.execution_author_packet import author_packet_matches
        from taskflow.execution_experiment import _packet
        self.assertFalse(author_packet_matches(s,parent=source,packet=_packet(ex),
            authorization=ex['definition']['authorization'],packet_transport=None))
        from taskflow.execution_public_continuation import resume_public_capacity_dependency
        for field,value in (('flow_id','unregistered-clone'),('definition_digest','0'*64),('event_seq',source['event_seq'])):
            fake={**ex,field:value}
            with self.subTest(field=field),self.assertRaises(TaskflowError):
                resume_public_capacity_dependency(s,fake,self.dependency_resolution(v),v['case'].refs)
        # An equal-SHA unrelated file is not the typed source/staged alias.
        raw=v['transport_path'].read_bytes();body=read_json(v['transport_path'])
        peer=v['case'].root/'same-bytes-foreign-packet.json';peer.write_bytes(Path(body['staged']['path']).read_bytes())
        body['staged']=evidence_ref(peer,'equal-byte unrelated source, not the locked target');write_json(v['transport_path'],body)
        try:
            with self.assertRaises(TaskflowError):author_packet_matches(s,parent=source,packet=_packet(ex),
                authorization=ex['definition']['authorization'],packet_transport=evidence_ref(v['transport_path'],'self-bound fake alias with foreign staged path'))
            with self.assertRaises(TaskflowError):resume_path_transport(s,ex['flow_id'],self.dependency_resolution(v))
        finally:v['transport_path'].write_bytes(raw)


if __name__=='__main__':unittest.main()
