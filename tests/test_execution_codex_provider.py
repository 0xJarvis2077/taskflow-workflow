"""Role-local native provider selection; fake CLI metadata only, no services."""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError,write_json
from taskflow.execution_codex_provider import provider_settings, copy_provider_setting
from tests.test_execution_batch import file_ref as reference
from taskflow.execution_runner import prepare_attempt,_process_environment
from taskflow.execution_batch import confirmation_digests,ROLE_KEYS,check_batch_plan,freeze_batch_plan,role_config,policy_setting
from tests import test_execution_runner as runner_tests
from tests import test_execution_batch as batch_tests


class CodexProviderTests(unittest.TestCase):
    def setUp(self):
        self.case=runner_tests.ExecutionRunnerTests();self.case.setUp();self.addCleanup(self.case.doCleanups)
        self.root=self.case.base;self.home=self.root/'role-codex-home';self.home.mkdir()
        self.pack=self.root/'role-pack';self.pack.mkdir();(self.pack/'method.md').write_text('Neutral declared source method.')
        self.scratch=self.root/'scratch';self.scratch.mkdir()
        self.catalog=self.root/'catalog.json';write_json(self.catalog,{'models':[{'slug':'neutral','tool_mode':'direct','supported_reasoning_levels':[{'effort':'high'}]}]})
        self.doc=self.root/'provider-doc.json';write_json(self.doc,{'origin':'offline fixture; not provider availability'})
        self.selected={'kind':'codex_role_provider_v1','provider':'openai','base_url':'https://api.openai.com/v1',
            'wire_api':'responses','credential_env':'OPENAI_API_KEY','model':'neutral','reasoning_effort':'high',
            'cli':reference(self.case.script),'catalog':reference(self.catalog),'codex_home':str(self.home),
            'documentation':[reference(self.doc)]}
        self.profile=self.root/'native-provider.json';write_json(self.profile,self.selected)
        self.fs={':root':'deny',':minimal':'read',':tmpdir':'deny',':slash_tmp':'deny','/private/tmp':'deny',
                 str(self.home):'deny',str(self.pack):'read',str(self.case.work):'write',str(self.scratch):'write'}
        permissions={'profile':'neutral-native','repository':str(self.pack),'scratch':str(self.scratch),
            'python':str(Path(runner_tests.sys.executable).resolve()),'probe_sources':[str(self.pack/'method.md')],
            'readonly_probe':str(self.pack/'method.md'),'filesystem':self.fs}
        self.config={**self.case.config,'mode':'native_role','role':'content-inspector','cli':str(self.case.script),
                     'native_provider':reference(self.profile),'native_permissions':permissions}

    def capture(self,root,cli):
        evidence={'model_calls':0,'commands':[
            {'argv':[cli,'--version'],'returncode':0,'stdout':'neutral fake CLI'},
            {'argv':[cli,'--no-daemon','debug','models','--bundled'],'returncode':0,
             'stdout':json.dumps({'models':[{'slug':'neutral','supported_reasoning_levels':[{'effort':'high'}]}]})}]}
        write_json(root/'cli-evidence.json',evidence);return evidence

    def test_exact_native_provider_preserves_choice_and_standalone_worker_environment(self):
        with patch('tools.isolated_quality.capture_cli_evidence',side_effect=self.capture):
            manifest=prepare_attempt(self.case.root,self.config)
        argv=manifest['argv'];self.assertIn('model_provider="taskflow_external"',argv)
        self.assertIn('model_providers.taskflow_external.env_key="OPENAI_API_KEY"',argv)
        self.assertIn('permissions.neutral-native.network.enabled=false',argv)
        self.assertIn('shell_environment_policy.inherit="none"',argv)
        self.assertFalse(manifest['native']['network_proxy'])
        with patch.dict(os.environ,{'OPENAI_API_KEY':'synthetic-selected','DEEPSEEK_API_KEY':'synthetic-not-selected'}):
            env=_process_environment(manifest['native'])
        self.assertEqual('synthetic-selected',env['OPENAI_API_KEY']);self.assertNotIn('DEEPSEEK_API_KEY',env)
        self.assertEqual(str(self.home),env['CODEX_HOME'])
        self.assertNotIn('synthetic-selected',json.dumps(manifest))

    def test_provider_endpoint_model_effort_or_home_expansion_is_rejected(self):
        for field,value in [('base_url','https://example.invalid/v1'),('credential_env','OTHER_KEY'),
                            ('model','other'),('reasoning_effort','low')]:
            with self.subTest(field=field):
                selected={**self.selected,field:value};write_json(self.profile,selected)
                cfg={**self.config,'native_provider':reference(self.profile)}
                with self.assertRaises(TaskflowError):provider_settings(cfg,self.fs)
        write_json(self.profile,self.selected)
        with self.assertRaises(TaskflowError):provider_settings({**self.config,'native_provider':reference(self.profile)},
            {**self.fs,str(self.home):'read'})

    def test_content_inspector_rejects_teacher_input_before_runtime_creation(self):
        cfg={**self.config,'inputs':[{'path':str(self.doc),'kind':'teacher'}]}
        with self.assertRaisesRegex(Exception,'Forbidden teacher'):
            prepare_attempt(self.case.root,cfg)
        self.assertFalse(self.case.root.exists())

    def test_confirmed_content_slot_can_use_cli_without_changing_legacy_chat_slots(self):
        plan,path=batch_tests.make_confirmed_plan(self.root/'batch')
        row=copy.deepcopy(plan['roles']['author']);plan['roles']['content_inspector']=row;plan['role_bindings']['content_inspector']={'carrier':'cli'}
        # Reattribute the same actual structured support evidence to this logical slot.
        plan['user_model_confirmation'].update(confirmation_digests(plan));write_json(path,plan)
        self.assertTrue(check_batch_plan(path)['ready'])
        frozen=freeze_batch_plan(path,self.root/'frozen-content')
        snapshot=frozen['snapshot']
        policy={'model_confirmation_required':True,'batch_snapshot':snapshot,
                'model_slots':{key:role_config(frozen,key) for key,value in frozen['roles'].items()
                               if value['enabled'] and value['runtime']['carrier'] in {'cli','api'}}}
        self.assertEqual('cli',policy_setting(policy,'content-inspector','content_inspector','neutral-task')['carrier'])
        with self.assertRaises(TaskflowError):policy_setting(policy,'quality','content_inspector','neutral-task')

    def test_custom_provider_model_uses_its_catalog_not_bundled_models(self):
        selected = {**self.selected, "provider": "deepseek",
                    "base_url": "https://api.deepseek.com", "credential_env": "DEEPSEEK_API_KEY",
                    "model": "vendor-model", "reasoning_effort": "medium"}
        write_json(self.catalog, {"models": [{"slug": "vendor-model", "tool_mode": "direct",
                    "supported_reasoning_levels": [{"effort": "medium"}]}]})
        selected["catalog"] = reference(self.catalog)
        write_json(self.profile, selected)
        config = {**self.config, "model": "vendor-model", "reasoning_effort": "medium",
                  "native_provider": reference(self.profile)}
        with patch('tools.isolated_quality.capture_cli_evidence', side_effect=self.capture):
            manifest = prepare_attempt(self.case.root, config)
        self.assertEqual("vendor-model", manifest["model"]["requested"])
        self.assertEqual("medium", manifest["model"]["reasoning_effort"])
        self.assertIsNone(manifest["model"]["served"])
        self.assertEqual(reference(self.catalog), manifest["model"]["catalog_source"])
        self.assertIn('model_providers.taskflow_external.env_key="DEEPSEEK_API_KEY"', manifest['argv'])

    def test_custom_provider_still_requires_successful_original_cli_observation(self):
        def failed(root, cli):
            evidence = self.capture(root, cli)
            evidence['commands'][0]['returncode'] = 1
            write_json(root/'cli-evidence.json', evidence)
            return evidence
        with patch('tools.isolated_quality.capture_cli_evidence', side_effect=failed):
            with self.assertRaisesRegex(TaskflowError, 'Original Codex version'):
                prepare_attempt(self.case.root, self.config)

    def test_custom_provider_batch_freezes_declared_catalog_without_builtin_membership(self):
        plan, path = batch_tests.make_confirmed_plan(self.root/'native-batch')
        runtime = {"carrier": "cli", "provider": "openai", "cli": str(self.case.script),
                   "reasoning_selection": "explicit", "reasoning_effort": "high",
                   "reasoning_context": None, "timeout_seconds": 10,
                   "native_provider": reference(self.profile)}
        plan['roles']['content_inspector'] = {'enabled': True, 'model': 'neutral', 'runtime': runtime}
        plan['role_bindings']['content_inspector'] = {'carrier': 'cli'}
        plan['user_model_confirmation'].update(confirmation_digests(plan))
        write_json(path, plan)
        self.assertTrue(check_batch_plan(path)['ready'])
        frozen = freeze_batch_plan(path, self.root/'frozen-native-batch')
        sources = {(item['path'], item['sha256']) for item in frozen['evidence']}
        self.assertIn((str(self.catalog), reference(self.catalog)['sha256']), sources)
        self.assertIn((str(self.doc), reference(self.doc)['sha256']), sources)

    def test_optional_provider_setting_is_copied_and_stale_template_is_removed(self):
        original = {"native_provider": reference(self.profile)}
        target = {"model": "unchanged", "native_provider": {"path": "old"}}
        copy_provider_setting(target, original)
        self.assertEqual(original["native_provider"], target["native_provider"])
        target["native_provider"]["path"] = "different"
        self.assertEqual(str(self.profile), original["native_provider"]["path"])
        copy_provider_setting(target, {})
        self.assertEqual({"model": "unchanged"}, target)

    def test_compiler_selected_runtime_keeps_exact_provider_and_clears_absent_choice(self):
        from taskflow.execution_batch_factory import _selected

        target = {"native_provider": {"path": "obsolete"}, "timeout_seconds": 10}
        selected = {"model": "neutral", "native_provider": reference(self.profile)}
        _selected(target, selected)
        self.assertEqual(reference(self.profile), target["native_provider"])
        _selected(target, {"model": "neutral-other"})
        self.assertNotIn("native_provider", target)
        self.assertEqual("neutral-other", target["model"])

    def test_normal_facts_builder_preserves_selected_provider_home_declaration(self):
        from taskflow.execution_review import prepare_facts_runner
        from tests.test_execution_review import CleanReviewPreparationTests

        case = CleanReviewPreparationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        config = copy.deepcopy(case.clean_config)
        config['runner']['native_provider'] = reference(self.profile)
        captured = {}

        class ConfigurationCaptured(RuntimeError):
            pass

        def capture(store, spec, runtime, **kwargs):
            captured.update(spec['runner'])
            raise ConfigurationCaptured()

        with patch('taskflow.execution.enqueue_spec', side_effect=capture):
            with self.assertRaises(ConfigurationCaptured):
                prepare_facts_runner(store=case.store, config=config, runtime=case.runtime)
        self.assertEqual(reference(self.profile), captured['native_provider'])
        filesystem = captured['native_permissions']['filesystem']
        self.assertEqual('deny', filesystem['/private/tmp'])
        self.assertEqual('deny', filesystem[str(self.home)])


    def test_normal_batch_compile_delivers_exact_provider_refs_to_experiment_factory(self):
        from taskflow.contracts import read_json
        from taskflow.execution_batch_factory import compile_batch_configuration
        from tests.test_execution_batch_dispatch import BatchDispatchTests

        case = BatchDispatchTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.factory_infrastructure()
        expected = {}
        for slot in ('solver', 'reviewer'):
            model = case.plan['roles'][slot]['model']
            catalog = self.root/(slot+'-catalog.json')
            write_json(catalog, {'models': [{'slug': model, 'tool_mode': 'direct',
                                           'supported_reasoning_levels': [{'effort': 'deep'}]}]})
            provider = {**self.selected, 'model': model, 'reasoning_effort': 'deep',
                        'catalog': reference(catalog)}
            path = self.root/(slot+'-provider.json')
            write_json(path, provider)
            expected[slot] = reference(path)
            runtime = case.plan['roles'][slot]['runtime']
            runtime.update(provider='openai', cli=str(self.case.script), native_provider=expected[slot])
        case.plan['user_model_confirmation'].update(confirmation_digests(case.plan))
        write_json(case.plan_path, case.plan)
        result = compile_batch_configuration(case.plan_path, case.infra, case.root/'native-configuration')
        factory = read_json(Path(result['experiment_factory']['path']))
        self.assertEqual(expected['solver'], factory['technical']['formal_config_base']['native_provider'])
        self.assertEqual(expected['reviewer'], factory['technical']['review_runner']['native_provider'])
        self.assertFalse((case.root/'never.sqlite3').exists())

    def test_existing_public_prepare_transports_provider_without_model_or_probe(self):
        from tests.test_isolated_quality import IsolatedQualityTests
        from tools import isolated_quality

        case = IsolatedQualityTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        write_json(self.catalog, {'models': [{'slug': 'neutral-model', 'tool_mode': 'direct',
                    'supported_reasoning_levels': [{'effort': 'high'}]}]})
        selected = {**self.selected, 'model': 'neutral-model', 'cli': reference(case.bin/'cli'),
                    'catalog': reference(self.catalog)}
        write_json(self.profile, selected)
        case.config['native_provider'] = reference(self.profile)
        with patch.object(isolated_quality.subprocess, 'Popen') as launch:
            case.prepare()
            plan = isolated_quality.validate(case.root, require_probe=False)
        self.assertFalse(launch.called)
        self.assertEqual(reference(self.profile), plan['model']['native_provider_ref'])
        self.assertIn('model_providers.taskflow_external.env_key="OPENAI_API_KEY"', plan['argv'])
        self.assertFalse(any('request_max_retries=' in x or 'stream_max_retries=' in x for x in plan['argv']))
        self.assertEqual('codex_configured_default', plan['config']['codex_provider']['transport_retry'])
        source_paths={Path(x['path']).name for x in json.loads((case.root/'source-lock.json').read_text())['files']}
        self.assertTrue({'execution_codex_provider.py','execution_codex_retry.py'} <= source_paths)
        self.assertTrue((case.root/'codex_environment.py').is_file())
        self.assertFalse((case.root/'ready.json').exists())
        self.assertFalse((case.root/'claim.json').exists())

    def test_existing_formal_source_renderer_binds_provider_without_preparing_task(self):
        from tools.build_formal_profile_adapter import build_formal_profile_adapter, MEMBERS

        plan, path = batch_tests.make_confirmed_plan(self.root/'formal-batch')
        plan['roles']['solver'] = {'enabled': True, 'model': 'neutral', 'runtime': {
            'carrier': 'cli', 'provider': 'openai', 'cli': str(self.case.script),
            'reasoning_selection': 'explicit', 'reasoning_effort': 'high', 'reasoning_context': None,
            'timeout_seconds': 3600, 'native_provider': reference(self.profile)}}
        plan['user_model_confirmation'].update(confirmation_digests(plan))
        write_json(path, plan)
        frozen = freeze_batch_plan(path, self.root/'formal-confirmed')
        original = self.root/'neutral-formal-source'
        original.mkdir()
        (original/'runtime_gate.py').write_text("import json, sys\nfrom pathlib import Path\n"
            "HERE = Path(__file__).resolve().parent\nREQUESTED_MODEL = 'old-profile'\n"
            "def validate(plan):\n    argv = plan['argv']\n    return argv\n"
            "def matches(observation):\n    return (True\n"
            "            and observation.get('model') == REQUESTED_MODEL\n            )\n"
            "def source_manifest():\n    paths = set()\n"
            "    for module in list(sys.modules.values()):\n        pass\n    return paths\n")
        (original/'prepare_diagnostic.py').write_text("def plan():\n    return {\n"
            "            'reasoning_setting': 'not_explicitly_specified',\n"
            "            'served_model': None, 'served_revision': None,\n    }\n"
            "def check(observations):\n    if observations[0]['model'] != gate.REQUESTED_MODEL:\n"
            "        raise ValueError('model mismatch')\n")
        (original/'execute_diagnostic.py').write_text("import subprocess\n"
            "def execute(plan):\n    return subprocess.Popen(plan['argv'], "
            "stdout=stdout, stderr=stderr, start_new_session=True)\n")
        (original/'file_evidence.py').write_text("# Neutral file capture source fixture.\n")
        from tools.isolated_quality import inline_table
        write_json(original/'baseline-plan.json', {'argv': [str(self.case.script), '-c',
            'default_permissions="neutral-formal"', '-c', 'permissions.neutral-formal.filesystem='+inline_table(self.fs),
            'exec', '-m', 'old-profile']})
        source = reference(original/'prepare_diagnostic.py')
        source['files'] = {name: reference(original/name)['sha256'] for name in MEMBERS}
        destination = self.root/'formal-generated'
        result = build_formal_profile_adapter(adapter_ref=source, snapshot_ref=frozen['snapshot'], destination=destination,
                                              transport_retry_mode='codex_default_v1')
        baseline = json.loads((destination/'baseline-plan.json').read_text())
        self.assertEqual(reference(self.profile), baseline['native_provider_ref'])
        self.assertIn('model_providers.taskflow_external.env_key="OPENAI_API_KEY"', baseline['argv'])
        self.assertFalse(any('request_max_retries=' in x or 'stream_max_retries=' in x for x in baseline['argv']))
        self.assertEqual('codex_configured_default', baseline['codex_provider']['transport_retry'])
        self.assertTrue({'execution_codex_provider.py','execution_codex_retry.py'} <=
                        {Path(x['path']).name for x in baseline['codex_provider_sources']})
        self.assertIn('process_environment(', (destination/'execute_diagnostic.py').read_text())
        self.assertEqual(set(MEMBERS), set(result['files']))
        self.assertFalse((destination/'execution-plan.json').exists())
        self.assertEqual('neutral', baseline['requested_model'])

    def test_same_reviewer_provider_reference_is_required_for_score(self):
        from taskflow.execution_review import _review_batch_pair

        source = reference(self.profile)
        facts = {'model': {'native_provider_ref': source}}
        with patch('taskflow.execution_batch_binding.validate_batch_runner', return_value=None):
            self.assertFalse(_review_batch_pair(facts, {'native_provider': source}, manifest=False))
            with self.assertRaisesRegex(TaskflowError, 'same actual provider/profile'):
                _review_batch_pair(facts, {'native_provider': {**source, 'sha256': '0'*64}}, manifest=False)

    def text_only(self):
        schema = self.root/'response-schema.json'
        write_json(schema, {'type': 'object', 'properties': {'ok': {'type': 'boolean'}},
                            'required': ['ok'], 'additionalProperties': False})
        source = self.pack/'public.md'
        source.write_text('Complete neutral public input.\n第二行完整原文。\n', encoding='utf-8')
        return {**self.config, 'outputs': ['inspection.json'],
                'inputs': [{'path': str(source), 'kind': 'public', 'target': 'draft/input/public.md'},
                           {'path': str(self.pack/'method.md'), 'kind': 'skill', 'target': 'methods/method.md'}],
                'text_only_delivery': {'schema': reference(schema), 'output': 'inspection.json'}}

    def test_text_only_content_inlines_complete_sources_and_binds_trusted_capture(self):
        from taskflow.execution_runner import _validate
        config = self.text_only()
        with patch('tools.isolated_quality.capture_cli_evidence', side_effect=self.capture):
            manifest = prepare_attempt(self.case.root, config)
        self.assertTrue((self.case.root/'text-only-ready.json').is_file())
        self.assertFalse((self.case.root/'native-probe-ready.json').exists())
        self.assertEqual(2, json.loads((self.case.root/'inline-input-index.json').read_text())['input_count'])
        request = (self.case.root/'request.md').read_text()
        self.assertIn('1|Complete neutral public input.\n2|第二行完整原文。', request)
        self.assertIn('BEGIN COMPLETE INPUT methods/method.md', request)
        model = json.loads((self.case.root/'text-only-catalog.json').read_text())['models'][0]
        self.assertEqual('disabled', model['shell_type'])
        self.assertIsNone(model['apply_patch_tool_type'])
        self.assertIn('--output-schema', manifest['argv'])
        self.assertIn(str(self.case.work/'inspection.json'), manifest['argv'])
        _validate(self.case.root, before_run=True)

    def test_text_only_capability_cannot_downgrade_solver_or_use_shared_home(self):
        from taskflow.execution_runner import _validate
        config = self.text_only()
        with self.assertRaises(Exception):
            prepare_attempt(self.case.root, {**config, 'role': 'solver'})
        self.assertFalse(self.case.root.exists())
        with patch('tools.isolated_quality.capture_cli_evidence', side_effect=self.capture):
            prepare_attempt(self.case.root, config)
        (self.home/'existing-session').write_text('Not an independent empty role home.')
        with self.assertRaisesRegex(Exception, 'empty independent'):
            _validate(self.case.root, before_run=True)

    def test_normal_worker_collects_cli_structured_text_without_model_tools(self):
        from taskflow.execution_runner import run_attempt
        self.case.script.write_text('#!'+runner_tests.sys.executable+'\n'
            'import json,sys,pathlib\n'
            'request=sys.stdin.read()\n'
            'assert "BEGIN COMPLETE INPUT draft/input/public.md" in request\n'
            'output=pathlib.Path(sys.argv[sys.argv.index("-o")+1])\n'
            'output.write_text(json.dumps({"ok":True}))\n'
            'print(json.dumps({"type":"thread.started","thread_id":"00000000-0000-4000-8000-000000000017"}))\n'
            'print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"{\\"ok\\":true}"}}))\n'
            'print(json.dumps({"type":"turn.completed"}))\n', encoding='utf-8')
        self.case.script.chmod(0o755)
        write_json(self.profile, {**self.selected, 'cli': reference(self.case.script)})
        config = self.text_only()
        config['native_provider'] = reference(self.profile)
        with patch('tools.isolated_quality.capture_cli_evidence', side_effect=self.capture):
            prepare_attempt(self.case.root, config)
        with patch.dict(os.environ, {'OPENAI_API_KEY': 'synthetic-only-fixture'}):
            result = run_attempt(self.case.root)
        self.assertTrue(result['technical_accepted'], result)
        self.assertEqual({'ok': True}, json.loads((self.case.root/'artifacts/inspection.json').read_text()))
        self.assertTrue((self.case.root/'trace.jsonl').is_file())
