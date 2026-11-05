"""Codex connectivity result intake; original event fixtures, no services."""
import json
import unittest
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from taskflow.contracts import write_json
from tests.test_execution_batch import file_ref
from tools.codex_connectivity import run

from tools.codex_connectivity import observed


class ConnectivityIntakeTests(unittest.TestCase):
    def events(self, middle):
        return '\n'.join(json.dumps(value) for value in [
            {'type': 'thread.started', 'thread_id': 'neutral-connection'},
            *middle,
            {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'CONNECTION_OK'}},
            {'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}},
        ])

    def test_startup_feature_warning_is_retained_without_hiding_completed_response(self):
        stdout = self.events([
            {'type': 'item.completed', 'item': {'type': 'error',
             'message': 'Under-development features enabled: skip_host_skill_discovery.'}},
            {'type': 'turn.started'},
        ])
        value = observed(stdout, 0)
        self.assertTrue(value['success'])
        self.assertEqual([], value['tool_events'])
        self.assertEqual([], value['errors'])
        self.assertEqual(1, len(value['startup_warnings']))

    def test_real_error_or_tool_activity_is_not_a_tool_free_success(self):
        for kind in ['error', 'command_execution']:
            with self.subTest(kind=kind):
                stdout = self.events([{'type': 'turn.started'},
                    {'type': 'item.completed', 'item': {'type': kind, 'message': 'failed'}}])
                self.assertFalse(observed(stdout, 0)['success'])
        self.assertFalse(observed(self.events([]), 1)['success'])
        self.assertFalse(observed('', 0)['success'])


class ConnectivityProcessTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='connectivity-fixture-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cli = self.root/'fake-codex'
        self.cli.write_text('# Synthetic executable fixture; never a service.')
        self.catalog = self.root/'catalog.json'
        write_json(self.catalog, {'models': [{'slug': 'neutral', 'supported_reasoning_levels': [{'effort': 'medium'}]}]})
        self.profile = self.root/'profile.json'
        self.selected = {'provider': 'openai', 'base_url': 'https://api.openai.com/v1',
            'credential_env': 'OPENAI_API_KEY', 'wire_api': 'responses', 'cli': file_ref(self.cli),
            'catalog': file_ref(self.catalog), 'model': 'neutral', 'reasoning_effort': 'medium'}
        write_json(self.profile, self.selected)
        self.env = patch.dict(os.environ, {'OPENAI_API_KEY': 'synthetic-key-for-fixture'}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def fake_process(self, *args, **kwargs):
        class Process:
            pid = 99999999
            returncode = 0
            def communicate(self, *args, **kwargs):
                value = '\n'.join(json.dumps(event) for event in [
                    {'type': 'thread.started', 'thread_id': 'neutral-sid'},
                    {'type': 'turn.started'},
                    {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'CONNECTION_OK'}},
                    {'type': 'turn.completed', 'usage': {'input_tokens': 1, 'output_tokens': 1}},
                ])
                return value.encode(), b'synthetic-key-for-fixture'
        self.assertNotIn('DEEPSEEK_API_KEY', kwargs['env'])
        self.assertIn('--disable', args[0])
        return Process()

    def test_preflight_and_one_success_preserve_originals_without_reissuing(self):
        output = self.root/'attempt'
        with patch('tools.codex_connectivity.subprocess.Popen', side_effect=self.fake_process) as launch:
            self.assertTrue(run(self.profile, output)['ready'])
            self.assertFalse(output.exists())
            self.assertFalse(launch.called)
            result = run(self.profile, output, execute=True)
            self.assertTrue(result['success'])
            self.assertTrue(result['credential_redacted'])
            with self.assertRaises(FileExistsError):run(self.profile, output, execute=True)
            self.assertEqual(1, launch.call_count)
        self.assertNotIn('synthetic-key-for-fixture', (output/'stderr.txt').read_text())
        self.assertTrue((output/'claim.json').is_file())
        self.assertTrue((output/'result.json').is_file())

    def test_launch_failure_gets_receipt_and_is_not_retried(self):
        output = self.root/'failed'
        with patch('tools.codex_connectivity.subprocess.Popen', side_effect=OSError('synthetic')) as launch:
            result = run(self.profile, output, execute=True)
            self.assertFalse(result['success'])
            self.assertEqual('OSError', result['launch_error'])
            with self.assertRaises(FileExistsError):run(self.profile, output, execute=True)
            self.assertEqual(1, launch.call_count)

    def test_missing_source_and_unapproved_endpoint_do_not_create_attempt(self):
        output = self.root/'invalid'
        write_json(self.profile, {**self.selected, 'base_url': 'https://example.invalid'})
        with self.assertRaises(ValueError):run(self.profile, output, execute=True)
        write_json(self.profile, self.selected)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):run(self.profile, output, execute=True)
        self.assertFalse(output.exists())

    def test_absent_provider_keeps_existing_default_process_environment(self):
        from taskflow.execution_codex_environment import process_environment
        self.assertIsNone(process_environment(None))
