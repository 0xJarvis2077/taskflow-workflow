"""Same-turn target preflight shares intake policy without signing quality."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from taskflow.execution_flow import advance_workflow
from taskflow.execution_quality_progress_contract import check_issue_targets, QualityContractError
from tests import test_execution_flow as flow_tests


class TargetPreflightTests(unittest.TestCase):
    def setUp(self):
        self.context = {"candidate_paths": ["draft/input/rules.md", "draft/evaluation/answer.md"],
                        "observed_paths": ["actual-public-work/submission.md"]}
        self.base = {"id": "neutral", "kind": "claim", "affected_paths": ["actual-public-work/submission.md"]}

    def check(self, issue, blocked):
        return check_issue_targets({"execution_routing": {"blocking_issue_ids": blocked}}, {"issues": [issue]}, self.context)

    def test_blocking_observation_and_unknown_paths_fail_with_identity(self):
        with self.assertRaisesRegex(QualityContractError, "issue=neutral; affected_path=actual-public-work/submission.md"):
            self.check({**self.base, "blocking": True}, ["neutral"])
        with self.assertRaisesRegex(QualityContractError, "Unknown affected candidate path"):
            self.check({**self.base, "affected_paths": ["foreign.md"]}, [])

    def test_nonblocking_claim_and_real_candidate_reference_rubric_are_legal(self):
        self.assertFalse(self.check(self.base, [])['quality_accepted'])
        for kind in ("reference", "rubric"):
            for blocked in (True, False):
                issue = {**self.base, "kind": kind, "blocking": blocked,
                         "affected_paths": ["draft/evaluation/answer.md"]}
                self.assertFalse(self.check(issue, ["neutral"] if blocked else [])['quality_accepted'])
            with self.assertRaises(QualityContractError):
                self.check({**self.base, "kind": kind, "blocking": False,
                            "status": "open_nonblocking", "nonblocking_reason": "actual limit"}, [])

    def test_cli_is_read_only_and_reports_issue_path(self):
        from taskflow import execution_quality_progress_contract as contract

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); context = {**self.context, "work": str(root)}
            (root / 'context.json').write_text(json.dumps(context))
            (root / 'adoption-readiness.json').write_text(json.dumps({"execution_routing": {"blocking_issue_ids": ["neutral"]}}))
            (root / 'issues.json').write_text(json.dumps({"issues": [{**self.base, "blocking": True}]}))
            before = {p.name: p.read_bytes() for p in root.iterdir()}
            p = subprocess.run([sys.executable, contract.__file__, '--context', str(root / 'context.json'),
                                '--target-scope-only'], capture_output=True, text=True)
            self.assertEqual(2, p.returncode)
            self.assertIn('issue=neutral; affected_path=actual-public-work/submission.md', p.stderr)
            self.assertEqual(before, {p.name: p.read_bytes() for p in root.iterdir()})

    def test_new_quality_preparation_stages_readonly_helper_and_instructions(self):
        helper = flow_tests.ContinuationTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        definition = helper.definition(outcome='accepted'); helper.register(definition)
        advance_workflow(helper.store, definition['flow_id'])
        record = advance_workflow(helper.store, definition['flow_id'])
        runtime = Path(record['state']['current_runtime'])
        manifest = json.loads((runtime / 'manifest.json').read_text())
        targets = {entry['target']: entry for entry in manifest['inputs']}
        self.assertIn('execution/quality-target-preflight.py', targets)
        context = json.loads((runtime / 'inputs/execution/quality-target-paths.json').read_text())
        self.assertIn('draft/input/prompt.md', context['candidate_paths'])
        self.assertIn('--target-scope-only', (runtime / 'request.md').read_text())
        self.assertEqual([], helper.store.list_attempts(record['state']['current_work_id']))

    def test_predecessor_inventory_rejects_foreign_duplicate_and_unsupported_aliases(self):
        from taskflow.execution_flow import _target_preflight_context

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); archive = root / 'archive'; archive.mkdir()
            (archive / 'manifest.json').write_text(json.dumps({"candidate_binding": {"task_id": "t", "input_digest": "x"}}))
            original = archive / 'submission.md'; original.write_text('observed')
            candidate = {"task_id": "t", "input_digest": "x", "files": []}
            state = {"public_archive": str(archive), "archives": [str(archive)]}
            base = {"kind": "submission", "path": str(original), "target": "actual-public-work/submission.md"}
            valid = _target_preflight_context(candidate, state, [base], root)
            self.assertEqual([base['target']], valid['observed_paths'])
            self.assertEqual([], valid['source_errors'])
            for item, substring in (({**base, "path": str(root / 'foreign.md')}, 'outside the exact public archive'),
                                    ({**base, "target": "other/submission.md"}, 'Unsupported public-predecessor')):
                checked = _target_preflight_context(candidate, state, [item], root)
                self.assertTrue(any(substring in error for error in checked['source_errors']))
            duplicate = _target_preflight_context(candidate, state, [base, base], root)
            self.assertTrue(any('ambiguous' in error for error in duplicate['source_errors']))
            mismatch = _target_preflight_context({**candidate, 'input_digest': 'foreign'}, state, [base], root)
            self.assertTrue(any('another public candidate' in error for error in mismatch['source_errors']))
            payload = root / 'candidate.md'; payload.write_text('candidate')
            import hashlib
            conflict = {**candidate, 'files': [{'package_path': base['target'], 'path': str(payload), 'role': 'input',
                                                'sha256': hashlib.sha256(payload.read_bytes()).hexdigest()}]}
            checked = _target_preflight_context(conflict, state, [base], root)
            self.assertTrue(any('Unsupported public-predecessor' in error for error in checked['source_errors']))
            (archive / 'manifest.json').unlink()
            missing = _target_preflight_context(candidate, state, [base], root)
            self.assertTrue(any('archive manifest is unavailable' in error for error in missing['source_errors']))

    def test_public_first_initial_request_does_not_disclose_target_inventory_or_command(self):
        helper = flow_tests.ContinuationTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        definition = helper.definition(outcome='accepted')
        definition['roles']['quality']['runner']['public_first_initial_listing'] = {
            'scope': 'whole_package', 'index': 'execution/public-inputs.json',
            'checkpoint': 'public-responsibility-first.json'}
        helper.register(definition); advance_workflow(helper.store, definition['flow_id'])
        result = advance_workflow(helper.store, definition['flow_id'])
        runtime = Path(result['state']['current_runtime'])
        request = (runtime / 'request.md').read_text()
        later = json.loads((runtime / 'quality-later-inputs.v1.json').read_text())
        operations = '\n'.join(item['text'] for item in later['request_sections'])
        self.assertNotIn('quality-target-paths.json', request)
        self.assertNotIn('--target-scope-only', request)
        self.assertIn('PUBLIC_RESPONSIBILITY_FIRST_SHA256', request)
        self.assertIn('rtk proxy', operations)
        self.assertIn('--target-scope-only', operations)


if __name__ == '__main__':
    unittest.main()
