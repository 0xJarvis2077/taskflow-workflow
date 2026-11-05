"""New provider preparation leaves transport retries to Codex configuration."""

import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.execution_codex_retry import provider_settings_with_retry
from taskflow.execution_runner import _trace, prepare_attempt
from tests import test_execution_codex_provider as fixtures
from tests import test_execution_solver_profile as profile_fixtures
from taskflow.contracts import TaskflowError, write_json


class CodexRetryDefaultTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.CodexProviderTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_new_provider_preserves_transport_defaults_and_no_business_replay(self):
        args, provider = provider_settings_with_retry(self.case.config, self.case.fs)
        self.assertFalse(any('request_max_retries=' in item or 'stream_max_retries=' in item for item in args))
        self.assertEqual('codex_configured_default', provider['transport_retry'])
        self.assertFalse(provider['automatic_retry'])
        self.assertEqual('business_work_redispatch',provider['automatic_retry_scope'])
        self.assertIn('unobserved', provider['transport_retry_observation'])
        self.assertIn('permissions.neutral-native.network.enabled=false', args)
        self.assertIn('shell_environment_policy.inherit="none"', args)

    def test_missing_or_changed_legacy_options_fail_closed_and_no_provider_is_unchanged(self):
        self.assertEqual(([], None), provider_settings_with_retry({}, {}))
        with patch('taskflow.execution_codex_retry.provider_settings',return_value=(['model_providers.taskflow_external.request_max_retries=0'], {'provider':'fixture'})):
            with self.assertRaisesRegex(TaskflowError, 'retry overrides unavailable'):
                provider_settings_with_retry(self.case.config, self.case.fs)

    def test_new_formal_profile_rejects_preexisting_retry_overrides(self):
        from tools.build_formal_profile_adapter import _provider_baseline
        from tools.isolated_quality import inline_table
        baseline={'argv':[str(self.case.case.script),'-c','default_permissions="neutral-native"',
                 '-c','permissions.neutral-native.filesystem='+inline_table(self.case.fs),
                 '-c','model_providers.taskflow_external.stream_max_retries=0','exec','-m','neutral']}
        with self.assertRaisesRegex(TaskflowError,'source already overrides provider retries'):
            _provider_baseline(baseline,self.case.config,'codex_default_v1')

    def test_new_native_manifest_locks_exact_options_without_retry_override(self):
        with patch('tools.isolated_quality.capture_cli_evidence',side_effect=self.case.capture):
            manifest = prepare_attempt(self.case.case.root, self.case.config)
        self.assertFalse(any('request_max_retries=' in item or 'stream_max_retries=' in item for item in manifest['argv']))
        self.assertEqual('codex_configured_default',manifest['native']['provider']['transport_retry'])
        self.assertFalse(manifest['automatic_retry'])
        self.assertEqual('native_role',manifest['mode'])


class CodexReconnectTraceTests(unittest.TestCase):
    def test_only_exact_wait_progress_before_real_completion_is_nonterminal(self):
        sid = '12345678-1234-4567-8123-123456789012'
        start = {'type':'thread.started','thread_id':sid}
        message = {'type':'item.completed','item':{'type':'agent_message','text':'Final answer'}}
        wait = {'type':'error','message':'Reconnecting... waiting for network (Connection failed: error sending request)'}
        numbered = {'type':'error','message':'Reconnecting... 1/2 (transport interrupted)'}
        done = {'type':'turn.completed'}
        cases = [([start,wait,message,done],True,1),([start,numbered,message,done],True,1),
                 ([start,wait,message,{'type':'turn.failed'}],False,1),
                 ([start,{'type':'error','message':'Reconnecting... waiting for network (other)'},message,done],False,0),
                 ([start,message,done,wait],False,0),([start,wait,message,done,done],False,1)]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory).resolve()/'trace.jsonl'
            for events,expected,count in cases:
                path.write_text(''.join(json.dumps(row)+'\n' for row in events))
                with self.subTest(events=events):
                    observed=_trace(path,expected_sid=sid)
                    self.assertEqual(expected,observed['model_completed'])
                    self.assertEqual(count,len(observed['reconnect_progress']))
            observed=_trace(path,expected_sid='87654321-4321-4567-8123-123456789012')
            self.assertFalse(observed['model_completed'])


class RetryMigrationAuthorizationTests(unittest.TestCase):
    def test_new_retry_mode_requires_explicit_profile_authority_and_unknown_fails(self):
        from taskflow.execution_solver_profile import validated_profile
        case=profile_fixtures.SolverProfileTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        path=case.root/'new-adapter/profile-migration.json'
        original=json.loads(path.read_text())
        for mode in ('codex_default_v1','unrecognized'):
            write_json(path,{**original,'transport_retry_mode':mode})
            with self.subTest(mode=mode),self.assertRaisesRegex(TaskflowError,'transport retry mode'):
                validated_profile(case.ref)
        write_json(path,original)
        validated_profile(case.ref)


if __name__ == '__main__':
    unittest.main()
