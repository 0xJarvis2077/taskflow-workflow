"""Offline local fake-CLI chain; fake sandbox output is not isolation evidence."""

import copy
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import os
from pathlib import Path
from types import SimpleNamespace
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_author import _public_root
from taskflow.execution_batch import CHAT_ROLES, CLI_ROLES, ROLE_KEYS, confirmation_digests, freeze_batch_plan, load_batch_snapshot, role_config
from taskflow.execution_lifecycle import _lifecycle_lock, advance_lifecycle, register_lifecycle
from taskflow.execution_runner import RunnerError, _info, run_attempt
from taskflow.execution_store import ExecutionStore, evidence_ref
from taskflow.execution_flow import _binding, advance_workflow
from taskflow.execution_repair_capacity import CAPACITY, prepare_repair_capacity_budget
from tests.helpers import make_task
from tests.test_execution_batch import file_ref, make_confirmed_plan
from tests import test_execution_experiment_author as experiment_fixtures
from tools.execution_simulation import fixture_policy

FAKE_CLI = r"""#!/usr/bin/env python3
import ast,hashlib,json,pathlib,shlex,shutil,sys,uuid
args=sys.argv[1:]
if args==['--version']:
 print('neutral-local-fake-cli 1.0');sys.exit(0)
if args in (['catalog'],['--no-daemon','debug','models','--bundled']):
 print(pathlib.Path(__file__).with_name('catalog.json').read_text());sys.exit(0)
if 'sandbox' in args:
 shell=shlex.split(args[-1]);code=shell[-1]
 specs=ast.literal_eval(code.split('for spec in ',1)[1].split(':\n',1)[0])
 rows=[{'path':s['path'],'read':s['read'],'write':s['write'],**({'read_errno':1} if not s['read'] else {}),**({'write_errno':1} if not s['write'] else {}),**({'sha256':s['sha256']} if 'sha256' in s else {})} for s in specs]
 print(json.dumps(rows));sys.exit(0)
request=sys.stdin.read();sid=str(uuid.uuid4())
print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
if 'Role: author.' in request:
 paths=[line.split(': ',1)[1] for line in request.splitlines() if line.startswith('Input (source_metadata): ')]
 decl=next(pathlib.Path(p) for p in paths if p.endswith('/inputs/execution/task.json'))
 inputs=decl.parent.parent;task=json.loads(decl.read_text());root=pathlib.Path('next-task')/task['task_id'];root.mkdir(parents=True)
 (root/'task.json').write_bytes(decl.read_bytes())
 for row in task['files']:
  source=inputs/row['path'];target=root/row['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,target)
 (root/'draft/evaluation/answer.md').write_text('Neutral fake author material; not a model output.\n')
 sys.path.insert(0,__REPOSITORY__)
 from taskflow.packages import check_task
 checked=check_task(root,write_report=False)
 for row in task['reviews']:
  path=root/row['path'];path.parent.mkdir(parents=True,exist_ok=True)
  path.write_text(json.dumps({'schema_version':1,'review_id':row['id'],'kind':row['kind'],'status':'inconclusive','content_digest':checked['content_digest'],'input_digest':checked['input_digest'],'scope':'Neutral unreviewed fixture','limitations':['No model inference'],'evidence':['fixture author']}))
 handoff={'schema_version':1,'task_id':task['task_id'],'disposition':'candidate_complete','candidate_subpath':'next-task/'+task['task_id'],'reason':'Neutral fixture','needs_business_decision':False,'source_evidence':[{'path':str(decl),'location':'fixture input'}],'independent_quality_passed':False,'new_formal_score':None}
 for n in ('research.md','candidate-decision.md','handoff.md'):pathlib.Path(n).write_text('Neutral local simulation')
 pathlib.Path('handoff.json').write_text(json.dumps(handoff));pathlib.Path('issues.json').write_text('{"issues":[]}')
elif 'Role: quality.' in request:
 paths=[line.split(': ',1)[1] for line in request.splitlines() if line.startswith('Input (source_metadata): ')]
 current=next(pathlib.Path(p) for p in paths if p.endswith('/inputs/execution/current.json'));ctx=json.loads(current.read_text());inputs=current.parent.parent
 binding={k:ctx[k] for k in ('candidate_id','intent_id','content_digest','input_digest','evaluation_digest')}
 name='draft/evaluation/answer.md';blocks=['PRIVATE-ONE'];defects=[{'id':blocks[0],'kind':'reference','responsibility':'The private support differs from the published rule.','affected_paths':[name],'evidence':[{'path':name,'sha256':hashlib.sha256((inputs/name).read_bytes()).hexdigest(),'location':'line1'}]}]
 route={'schema_version':1,**binding,'signer_role':'independent_quality','independent_of_current_author':True,'participated_in_production':False,'scope':'whole_package','outcome':'revision_required','blocking_issue_ids':blocks,'covered_defect_ids':[],'reason':'Neutral fixture issue only.'}
 pathlib.Path('adoption-readiness.json').write_text(json.dumps({'execution_routing':route}));pathlib.Path('issues.json').write_text(json.dumps({'execution_binding':binding,'issues':defects}));pathlib.Path('intake.md').write_text('Neutral simulated review')
elif 'Role: repair.' in request:
 paths=[line.split(': ',1)[1] for line in request.splitlines() if line.startswith('Input (source_metadata): ')]
 current=next(pathlib.Path(p) for p in paths if p.endswith('/inputs/execution/current.json'));ctx=json.loads(current.read_text());inputs=current.parent.parent
 marker=pathlib.Path(__file__).with_name('repair-counter');number=int(marker.read_text())+1 if marker.exists() else 1;marker.write_text(str(number))
 config_path=pathlib.Path(__file__).with_name('repair-fixture-config.json');config=json.loads(config_path.read_text()) if config_path.exists() else {}
 if number <= config.get('capacity_failures',2) or number == config.get('noncapacity_at'):
  message='neutral noncapacity failure' if number == config.get('noncapacity_at') else __CAPACITY__
  print(json.dumps({'type':'error','message':message}),flush=True);print(json.dumps({'type':'turn.failed','error':{'message':message}}),flush=True);sys.exit(1)
 task=json.loads((inputs/'execution/task.json').read_text());root=pathlib.Path('revised-task')/ctx['task_id'];root.mkdir(parents=True)
 (root/'task.json').write_text(json.dumps(task))
 for row in task['files']:
  dest=root/row['path'];dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(inputs/row['path'],dest)
 changed='draft/evaluation/answer.md';before=hashlib.sha256((root/changed).read_bytes()).hexdigest();(root/changed).write_text('Neutral repaired material.\n');after=hashlib.sha256((root/changed).read_bytes()).hexdigest()
 pathlib.Path('repair-record.json').write_text(json.dumps({'parent_id':ctx['candidate_id'],'candidate_id':ctx['revised_candidate_id'],'defect_ids':ctx['defect_ids'],'needs_business_decision':False,'changes':[{'path':changed,'before_sha256':before,'after_sha256':after}]}));pathlib.Path('handoff.md').write_text('Neutral simulated repair')
else:
 pathlib.Path('deliverables').mkdir(exist_ok=True);pathlib.Path('deliverables/quality-note.md').write_text('Neutral public output; no model')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral local fake CLI only.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
""".replace('__REPOSITORY__',repr(str(Path(__file__).resolve().parents[1]))).replace('__CAPACITY__',repr(CAPACITY))


def fake_probe(root):
    """Explicitly fabricated local test double; never native isolation proof."""
    root=Path(root); manifest=_info(root/'manifest.json'); report=root/'native-probe-local-fake.json'
    write_json(report, {'passed':True,'model_calls':0,'manifest':manifest,'scope':'TEST DOUBLE: no permissions tested'})
    write_json(root/'native-probe-ready.json', {'manifest':manifest,'report_file':report.name,'report':_info(report),'model_calls':0})
    return {'passed':True, 'test_double':True}


class ConfirmedRepairCapacityFakeCliTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='taskflow-fake-confirmed-',dir='/private/tmp');self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.repo=self.base/'repository';self.repo.mkdir()
        self.home=self.base/'home';(self.home/'.codex').mkdir(parents=True)
        plan,path=make_confirmed_plan(self.base/'choices',mode='real_cli');plan['task_ids']=['neutral']
        cli=Path(plan['roles']['author']['runtime']['cli']);cli.write_text(FAKE_CLI);cli.chmod(0o755)
        catalog_path=cli.with_name('catalog.json')
        catalog={'models':[{'id':'other-provider/'+key,'slug':'other-provider/'+key,'efforts':['deep'],
                            'supported_reasoning_levels':[{'effort':'deep'}]}
                           for key in ROLE_KEYS if key in CLI_ROLES]}
        write_json(catalog_path,catalog)
        observed=subprocess.run([str(cli),'catalog'],capture_output=True,check=True)
        observed_catalog=cli.with_name('observed-catalog.json');observed_catalog.write_bytes(observed.stdout)
        self.assertEqual(catalog,json.loads(observed.stdout))
        for key in CLI_ROLES:
            runtime=plan['roles'][key]['runtime'];runtime['reasoning_context']=None
            support=Path(runtime['support_evidence']['path']);data=read_json(support)
            data.update(cli=file_ref(cli),output=file_ref(observed_catalog));data.pop('contexts_pointer',None)
            write_json(support,data);runtime['support_evidence']=file_ref(support)
        confirmed=confirmation_digests(plan)
        for key in CHAT_ROLES:
            ref=plan['role_bindings'][key]['configuration_evidence'];source=Path(ref['path']);data=read_json(source)
            data['configuration']['scope']=confirmed['scope'];write_json(source,data)
            plan['role_bindings'][key]['configuration_evidence']=file_ref(source)
        plan['user_model_confirmation'].update(confirmed);write_json(path,plan)
        snapshot_ref=freeze_batch_plan(path,self.base/'frozen')['snapshot'];snapshot=load_batch_snapshot(snapshot_ref)
        config={**fixture_policy(),'model_confirmation_required':True,'batch_snapshot':snapshot_ref,
                'model_slots':{key:role_config(snapshot,key) for key in ROLE_KEYS if key in CLI_ROLES and snapshot['roles'][key]['enabled']}}
        self.store=ExecutionStore(self.base/'state.sqlite3');self.store.register_policy('batch-v1',config)
        self.cli=cli;self.snapshot=snapshot
        self.task=make_task(self.repo,'neutral');task=read_json(self.task/'task.json')
        task['reviews'].append({'id':'calculation','kind':'calculation','path':'reports/calculation.json','required':True})
        write_json(self.task/'task.json',task);self.candidate=execution.capture_candidate(self.store,self.task,'seed')

    def permissions(self, work, scratch):
        return {'profile':'neutral_fixture','repository':str(self.repo),'scratch':str(scratch),'python':str(Path(sys.executable).resolve()),
                'filesystem':{str(self.repo):'read',str(work):'write',str(scratch):'write'},
                'readonly_probe':str(self.task/'task.json'),'probe_sources':[str(self.task/'task.json')]}

    def build_chain(self, *, prepare_only=False):
        work=self.base/'author-work';scratch=self.base/'author-scratch';work.mkdir();scratch.mkdir()
        required=['research.md','candidate-decision.md','handoff.md','handoff.json','issues.json']
        item={'task_id':'neutral','intent_id':'source-author-intent'}
        successors={'candidate_complete':'Neutral asset only','research_ended':'Archive result','needs_business_decision':'Wait'}
        packet={'controller':'fixture-controller','task_id':'neutral','batch_id':self.snapshot['batch_id'],'items':[item],
                'output_contract':{'required_outputs':required},'preauthorized_normal_successors':successors}
        packet_path=self.base/'packet.json';write_json(packet_path,packet);ref=evidence_ref(packet_path,'neutral simulated controller authority')
        inputs=[{'path':str(self.task/'task.json'),'kind':'source_metadata','target':'execution/task.json'},
                {'path':str(packet_path),'kind':'source_metadata','target':'protocol/current-author-packet.json'}]
        inputs += [{'path':row['path'],'kind':'public' if row['role'] in {'prompt','input','template'} else 'teacher','target':row['package_path']} for row in self.candidate['files']]
        setting=role_config(self.snapshot,'author')
        runner={**setting,'intent_id':'source-author-intent','policy_version':'batch-v1','role':'author','model_slot':'author','mode':'native_role',
                'cwd':str(work),'native_permissions':self.permissions(work,scratch),'inputs':inputs,'outputs':required,'request_sections':[]}
        spec={'work':{'work_id':'source-author','task_id':'neutral','candidate_id':'seed','role':'author','model_slot':'author','policy_id':'batch-v1','intent_id':'source-author-intent','purpose':'neutral local fixture','dispatch_owner':'fixture-controller','evidence':[ref]},'runner':runner}
        with patch('taskflow.execution_runner.probe_native_attempt',side_effect=fake_probe):
            prepared = execution.enqueue_spec(self.store,spec,self.base/'author-runtime',probe_native=True)
            if prepare_only:
                return prepared['work']
            author=execution.run_worker(self.store,'fixture-author',True,['source-author'])
        self.assertEqual('completed',author['state'],author)
        roles={}
        for name in ('quality','repair','recheck','recheck_public','public_rebuild'):
            role='repair' if name=='repair' else 'quality'
            stub={'role':role,'mode':'existing_isolated' if name=='public_rebuild' else 'native_role',
                  'model':'unused','reasoning_effort':'deep','timeout_seconds':10,'cwd':'{{work}}','inputs':[],
                  'request_sections':[],'outputs':['deliverables/quality-note.md'] if name=='public_rebuild' else ['intake.md','issues.json','adoption-readiness.json'],
                  'native_permissions':self.permissions('{{work}}','{{scratch}}')}
            roles[name]={'runner':stub,'purpose':'fixture','scope':'whole_package'}
            if name=='repair':
                stub['outputs']=['repair-record.json','handoff.md'];stub['output_trees']=['revised-task'];roles[name]['candidate_subpath']='revised-task/neutral'
            if name=='public_rebuild':
                source=Path(__file__).resolve().parents[1]/'tools/isolated_quality.py'
                roles[name]['prepare']={'adapter':str(source),'sha256':sha256(source.read_bytes()),'checks':[],
                     'launcher_python':str(Path(sys.executable).resolve()),'config':{'repository':str(self.repo),
                      'dependencies':str(Path(sys.executable).resolve().parent),'cli':str(self.cli),'rtk':str(Path(sys.executable).resolve()),
                      'python':str(Path(sys.executable).resolve()),'model':'unused','reasoning_effort':'deep'}}
        template=self.base/'roles.json';write_json(template,{'workflows':[{'definition':{'flow_id':'fixture-template','roles':roles}}]})
        definition={'schema_version':1,'kind':'author_quality_v1','flow_id':'fixture-parent','task_id':'neutral','owner':'fixture-controller',
                    'policy_id':'batch-v1','directory':str(self.base/'parent'),'authorization':ref,'source_author_attempt':author['attempt_id'],
                    'source_author_binding':{key:author[key] for key in ('work_id','intent_id','candidate_id','task_id')},
                    'source_author_work':str(work),'role_template_ref':{'path':str(template),'sha256':sha256(template.read_bytes()),'flow_id':'fixture-template'},
                    'business_item':item,'author_contract':packet['output_contract'],'next':successors}
        parent=register_lifecycle(self.store,definition,[ref]);external=_public_root(parent)
        self.addCleanup(lambda: shutil.rmtree(external,ignore_errors=True))
        with patch.object(Path,'home',return_value=self.home), patch('taskflow.execution_runner.probe_native_attempt',side_effect=fake_probe):
            for _ in range(20):
                parent=advance_lifecycle(self.store,parent['flow_id'])
                if parent['state'].get('current_work_id'):
                    execution.run_worker(self.store,'fixture-role',True,[parent['state']['current_work_id']])
                if parent['state']['status']=='exception' and parent['state'].get('receipt'): break
        self.assertEqual('exception',parent['state']['status'],parent['state'])
        child=self.store.get_flow(parent['state']['child_flow_id'])
        self.assertEqual('exception',child['state']['status'],child['state'])
        self.assertEqual('repair',child['state']['stage'])
        failed=self.store.list_attempts(child['state']['current_work_id'])[0]
        self.assertEqual('failed',failed['state'])
        experiment_fixture=experiment_fixtures.AuthorExperimentTests();experiment_fixture.root=self.base
        case=SimpleNamespace(root=self.base,store=self.store,attempt=author)
        dependent_definition=experiment_fixture.definition(case,parent)
        dependent_definition['policy_id']='batch-v1'
        dependent_definition['technical'].update(formal_runtime_root=str(self.base/'formal-unused'),publication_root=str(self.base/'publication-unused'))
        dependent=register_lifecycle(self.store,dependent_definition,[dependent_definition['authorization']])
        dependent=advance_lifecycle(self.store,dependent['flow_id']);dependent=advance_lifecycle(self.store,dependent['flow_id'])
        self.assertEqual('exception',dependent['state']['status'],dependent['state'])
        return parent,child,dependent,failed

    def prepare_chain(self):
        parent,child,dependent,failed=self.build_chain()
        self.original_receipts={Path(parent['state']['receipt']):Path(parent['state']['receipt']).read_bytes(),
                                Path(child['state']['receipt']):Path(child['state']['receipt']).read_bytes()}
        user=self.base/'simulated-user-message.json';write_json(user,{'origin':'SIMULATED_TEST_ONLY','message':'Allow up to 10 local fake retries'})
        auth=self.base/'retry-authority.json';write_json(auth,{'schema_version':1,'kind':'bounded_repair_capacity_v1','owner':'fixture-controller',
            'flow_id':child['flow_id'],'failed_attempt_id':failed['attempt_id'],'candidate_binding':_binding(self.store.get_candidate(child['state']['candidate_id'])),
            'policy_id':'batch-v1','action':'prepare_bounded_same_profile_repairs','maximum_additional_attempts':10,
            'direct_user_evidence':evidence_ref(user,'SIMULATED TEST ONLY')})
        resolution={'reason':'neutral test only','authority':evidence_ref(auth,'simulated authority'),'failed_attempt_id':failed['attempt_id']}
        prepared=prepare_repair_capacity_budget(self.store,parent['flow_id'],child['flow_id'],resolution)
        proof=prepared['state']['repair_capacity_continuation'];self.assertEqual(parent,self.store.get_flow(parent['flow_id']));self.assertEqual(dependent,self.store.get_flow(dependent['flow_id']))
        self.store.set_control('work:'+proof['new_work_id'],'resume','simulated authorized release')
        return parent,child,dependent,failed,proof

    def assert_originals_retained(self,parent,dependent,failed):
        self.assertEqual(parent,self.store.get_flow(parent['flow_id']))
        self.assertEqual(dependent,self.store.get_flow(dependent['flow_id']))
        self.assertEqual(self.original_receipts,{path:path.read_bytes() for path in self.original_receipts})
        self.assertEqual([failed],self.store.list_attempts(failed['work_id']))
        self.assertFalse([item for item in self.store.dashboard()['work'] if item['role'] in {'solver','review-facts','review-score'}])

    def test_confirmed_batch_old_native_chain_is_retired(self):
        write_json(self.cli.with_name('repair-fixture-config.json'), {})
        work = self.build_chain(prepare_only=True)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('native_role', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (self.store.dashboard(), self.store.events())
        with patch('taskflow.execution_runner.run_attempt', side_effect=AssertionError('retired native chain cannot run')):
            with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
                execution.run_worker(self.store, 'retired-old-author', True, [work['work_id']])
        self.assertEqual(before, (self.store.dashboard(), self.store.events()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual([], self.store.list_attempts(work['work_id']))
        self.assertFalse(self.cli.with_name('repair-counter').exists())

    def test_noncapacity_setting_cannot_restore_old_native_dispatch(self):
        write_json(self.cli.with_name('repair-fixture-config.json'), {'capacity_failures':1,'noncapacity_at':2})
        work = self.build_chain(prepare_only=True)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('native_role', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (self.store.dashboard(), self.store.events())
        with patch('taskflow.execution_runner.run_attempt', side_effect=AssertionError('retired native chain cannot run')):
            with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
                execution.run_worker(self.store, 'retired-old-author', True, [work['work_id']])
        self.assertEqual(before, (self.store.dashboard(), self.store.events()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual([], self.store.list_attempts(work['work_id']))
        self.assertFalse(self.cli.with_name('repair-counter').exists())

    def test_retry_allowance_cannot_restore_old_native_dispatch(self):
        write_json(self.cli.with_name('repair-fixture-config.json'), {'capacity_failures':11})
        work = self.build_chain(prepare_only=True)
        manifest = Path(work['runtime_path']) / 'manifest.json'
        self.assertEqual('native_role', read_json(manifest)['mode'])
        original = manifest.read_bytes()
        before = (self.store.dashboard(), self.store.events())
        with patch('taskflow.execution_runner.run_attempt', side_effect=AssertionError('retired native chain cannot run')):
            with self.assertRaisesRegex(TaskflowError, 'dispatch is retired'):
                execution.run_worker(self.store, 'retired-old-author', True, [work['work_id']])
        self.assertEqual(before, (self.store.dashboard(), self.store.events()))
        self.assertEqual(original, manifest.read_bytes())
        self.assertEqual([], self.store.list_attempts(work['work_id']))
        self.assertFalse(self.cli.with_name('repair-counter').exists())
