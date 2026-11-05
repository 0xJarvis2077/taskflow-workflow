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


def failed_public(owner, case, record, *, prepare_only=False):
    record = advance_author(case.store, record)
    record = advance_author(case.store, record)
    owner.addCleanup(shutil.rmtree, _public_root(record), True)
    record = advance_author(case.store, record)
    if prepare_only:
        return record, None
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


def make_capacity_fixture(owner, *, failure=CAPACITY, prepare_only=False):
    case = author_tests.AuthorSuccessorTests()
    case.setUp()
    owner.addCleanup(case.doCleanups)
    adapter = install_capacity_adapter(case, failure)
    case.run_source('candidate_complete')
    record, failed = failed_public(owner, case, case.register(), prepare_only=prepare_only)
    if prepare_only:
        return {'case': case, 'store': case.store, 'record': record}
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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_failure_identity_state_hold_receipt_lock_and_cas_fail_closed(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_old_public_capacity_chain_is_retired_before_a_failure_is_created(self):
        value = make_capacity_fixture(self, failure=CAPACITY, prepare_only=True)
        store, record = value['store'], value['record']
        work_id = record['state']['public_work_id']
        work = store.get_work(work_id)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('existing_isolated', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (store.dashboard(), store.events(), store.list_flows())
        author = value['case'].attempt
        author_trace = Path(author['runtime_path']) / 'trace.jsonl'
        trace = author_trace.read_bytes()
        with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
            execution.run_worker(store, 'retired-public-worker', True, [work_id])
        self.assertEqual(before, (store.dashboard(), store.events(), store.list_flows()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual(trace, author_trace.read_bytes())
        self.assertEqual([], store.list_attempts(work_id))

    def test_retired_public_dispatch_preserves_source_and_original_trace(self):
        value = make_capacity_fixture(self, failure=CAPACITY, prepare_only=True)
        store, record = value['store'], value['record']
        work_id = record['state']['public_work_id']
        work = store.get_work(work_id)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('existing_isolated', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (store.dashboard(), store.events(), store.list_flows())
        author = value['case'].attempt
        author_trace = Path(author['runtime_path']) / 'trace.jsonl'
        trace = author_trace.read_bytes()
        with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
            execution.run_worker(store, 'retired-public-worker', True, [work_id])
        self.assertEqual(before, (store.dashboard(), store.events(), store.list_flows()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual(trace, author_trace.read_bytes())
        self.assertEqual([], store.list_attempts(work_id))

    def test_retired_public_dispatch_cannot_replace_existing_work(self):
        value = make_capacity_fixture(self, failure=CAPACITY, prepare_only=True)
        store, record = value['store'], value['record']
        work_id = record['state']['public_work_id']
        work = store.get_work(work_id)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('existing_isolated', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (store.dashboard(), store.events(), store.list_flows())
        author = value['case'].attempt
        author_trace = Path(author['runtime_path']) / 'trace.jsonl'
        trace = author_trace.read_bytes()
        with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
            execution.run_worker(store, 'retired-public-worker', True, [work_id])
        self.assertEqual(before, (store.dashboard(), store.events(), store.list_flows()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual(trace, author_trace.read_bytes())
        self.assertEqual([], store.list_attempts(work_id))

    def test_noncapacity_setting_cannot_restore_old_public_dispatch(self):
        value = make_capacity_fixture(self, failure='Actual distinct technical or business error', prepare_only=True)
        store, record = value['store'], value['record']
        work_id = record['state']['public_work_id']
        work = store.get_work(work_id)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('existing_isolated', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (store.dashboard(), store.events(), store.list_flows())
        author = value['case'].attempt
        author_trace = Path(author['runtime_path']) / 'trace.jsonl'
        trace = author_trace.read_bytes()
        with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
            execution.run_worker(store, 'retired-public-worker', True, [work_id])
        self.assertEqual(before, (store.dashboard(), store.events(), store.list_flows()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual(trace, author_trace.read_bytes())
        self.assertEqual([], store.list_attempts(work_id))

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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_dependency_business_hold_original_exception_and_formal_footprints_remain_strict(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_dependent_source_original_event_and_cas_cannot_be_replaced(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_two_registered_different_purposes_do_not_share_one_public_continuation(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_real_registered_source_metadata_alias_preserves_exact_dependent_purpose(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_parent_only_none_binding_cannot_adopt_a_later_new_experiment(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_bound_dependent_identity_authority_event_and_alias_cannot_be_cloned(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__=='__main__':unittest.main()
