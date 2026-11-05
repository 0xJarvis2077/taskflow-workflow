"""Normal startup environment selection; synthetic process only, no business."""
import os
import json
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.execution_codex_environment import process_environment
from tools.codex_managed_start import start


class ManagedStartTests(unittest.TestCase):
    def test_explicit_chatgpt_environment_preserves_runtime_and_proxy_names_only(self):
        fixture = {'PATH': '/neutral/bin', 'HOME': '/neutral/home', 'CODEX_HOME': '/neutral/codex',
                   'HTTPS_PROXY': 'https://proxy.invalid', 'SSL_CERT_FILE': '/neutral/ca.pem',
                   'OPENAI_API_KEY': 'synthetic', 'DEEPSEEK_API_KEY': 'synthetic',
                   'AZURE_OPENAI_API_KEY': 'synthetic', 'UNRELATED_TOKEN': 'synthetic'}
        with patch.dict(os.environ, fixture, clear=True):
            result = process_environment(None, authentication='chatgpt')
        self.assertEqual(set(fixture)-{'OPENAI_API_KEY','DEEPSEEK_API_KEY','AZURE_OPENAI_API_KEY','UNRELATED_TOKEN'}, set(result))
        with self.assertRaises(ValueError):process_environment({'credential_env':'KEY'}, authentication='chatgpt')

    def test_normal_entry_preflight_and_single_declared_start_never_changes_worker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = [sys.executable, '-m', 'taskflow', 'execution', '--db', str(root/'neutral.sqlite3'),
                       'quality', 'run', 'neutral', '--allow-model']
            class Process:pid=99999999
            captured = {}
            def spawn(argv, **kwargs):
                captured.update(kwargs)
                self.assertEqual(command, argv)
                return Process()
            receipt = root/'startup.json'
            with patch.dict(os.environ, {'HOME':str(root),'OPENAI_API_KEY':'synthetic'}, clear=True):
                with patch('tools.codex_managed_start.subprocess.Popen', side_effect=spawn) as launch:
                    self.assertFalse(start(command,cwd=Path(__file__).resolve().parents[1],receipt=receipt)['process_started'])
                    self.assertFalse(receipt.exists())
                    self.assertFalse(launch.called)
                    self.assertTrue(start(command,cwd=Path(__file__).resolve().parents[1],receipt=receipt,execute=True)['process_started'])
                    with self.assertRaises(FileExistsError):start(command,cwd=Path(__file__).resolve().parents[1],receipt=receipt,execute=True)
                    self.assertEqual(1, launch.call_count)
            self.assertEqual({'HOME':str(root)},captured['env'])
            self.assertFalse((root/'neutral.sqlite3').exists())
            self.assertNotIn('synthetic',receipt.read_text())

    def test_clean_environment_passes_through_three_normal_python_layers(self):
        leaf = 'import os,json;print(json.dumps(sorted(os.environ)))'
        worker = 'import subprocess,sys;subprocess.run([sys.executable,"-c",'+repr(leaf)+'],check=True)'
        consumer = 'import subprocess,sys;subprocess.run([sys.executable,"-c",'+repr(worker)+'],check=True)'
        with patch.dict(os.environ, {'HOME':'/neutral/home','PATH':os.environ.get('PATH',''),
                 'HTTPS_PROXY':'https://proxy.invalid','OPENAI_API_KEY':'synthetic','DEEPSEEK_API_KEY':'synthetic'}, clear=True):
            environment = process_environment(None, authentication='chatgpt')
        result = subprocess.run([sys.executable,'-c',consumer],env=environment,capture_output=True,text=True,check=True)
        names = json.loads(result.stdout)
        self.assertIn('HOME',names)
        self.assertIn('HTTPS_PROXY',names)
        self.assertNotIn('OPENAI_API_KEY',names)
        self.assertNotIn('DEEPSEEK_API_KEY',names)

    def test_start_boundary_rejects_other_commands_before_creating_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root/'not-started.json'
            with self.assertRaises(ValueError):
                start([sys.executable,'-c','print("neutral")'],cwd=root,receipt=receipt,execute=True)
            self.assertFalse(receipt.exists())
            command = [sys.executable,'-m','taskflow','execution','--help']
            with self.assertRaisesRegex(ValueError,'repository differs'):
                start(command,cwd=root,receipt=receipt,execute=True)
            self.assertFalse(receipt.exists())
