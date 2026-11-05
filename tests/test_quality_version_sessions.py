"""Offline QA rechecks use one actual fixture SID and distinct immutable turns."""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, read_json
from taskflow.execution_quality import correct_quality_task, prepare_quality_task, register_quality_task, run_quality_task
from taskflow.execution_revision import register_revision_task, run_revision_task, validate_revision_source
from taskflow.execution_runner import _info, prepare_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_quality as quality_fixture
from tests import test_execution_revision as revision_fixture
from tests import test_material_revision_quality as affected_fixture


RECHECK_ROLE = affected_fixture.AFFECTED_ROLE.replace(
    "ctx=json.loads((inputs/'execution/current.json').read_text())",
    "ctx=json.loads((inputs/'execution/current.json').read_text())\n requested_mode=mode\n mode='affected' if ctx['public_first']['review_scope']=='affected' else mode")
RECHECK_ROLE = RECHECK_ROLE.replace(
    "ordinal=int(pathlib.Path.cwd().parent.name)",
    "ordinal=int(pathlib.Path.cwd().parent.name)\n if requested_mode=='invalid-child' and mode=='affected' and ordinal==0: mode='affected-omit-related'")
RECHECK_ROLE = RECHECK_ROLE.replace("'kind':'material'", "'kind':'reference'")
RECHECK_ROLE = RECHECK_ROLE.replace(
    "sid='00000000-0000-4000-8000-000000000003' if mode.startswith('affected') else sid",
    "sid='00000000-0000-4000-8000-000000000002'")
RECHECK_ROLE = RECHECK_ROLE.replace(
    "save('public-responsibility-first.json',{'public_observation':'Actual neutral fixture public checkpoint.','sources':public})",
    "checkpoint={'public_observation':'Actual neutral fixture public checkpoint.','sources':public}\n "
    "if ctx['public_first'].get('review_mode')=='informed_recheck':\n  "
    "checkpoint.update(review_mode='informed_recheck',parent_checkpoint_sha256=hashlib.sha256((inputs/'execution/parent-public-checkpoint.json').read_bytes()).hexdigest())\n "
    "save('public-responsibility-first.json',checkpoint)")
RECHECK_ROLE = RECHECK_ROLE.replace(
    "if mode=='missing-native': del report['native_signoff']",
    "if ctx['public_first'].get('review_mode')=='informed_recheck':\n  "
    "report['public_first'].update(mode='informed_recheck',parent_checkpoint_sha256=checkpoint['parent_checkpoint_sha256'])\n "
    "if mode=='missing-native': del report['native_signoff']")


class QualityVersionSessionTests(unittest.TestCase):
    def setUp(self):
        self.case = quality_fixture.ExplicitQualityTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.script = self.case.root / 'persistent-qa.py'
        self.case.script.write_text(RECHECK_ROLE, encoding='utf-8')

    def prepare_chain(self, *, parent_outcome='accepted', fixture_mode=None, parent_unchanged=False, caller_sections=None):
        c = self.case
        if parent_unchanged:
            text = RECHECK_ROLE.replace(
                "if mode=='missing-native': del report['native_signoff']",
                "if mode=='revision_required':\n  native.update(coverage='unchanged_scope',affected_paths=['draft/input/prompt.md','draft/evaluation/answer.md'])\n "
                "if mode=='missing-native': del report['native_signoff']")
            c.script.write_text(text, encoding='utf-8')
        parent_spec = c.spec(fixture_mode or parent_outcome, corrections=0, identity='parent-qa')
        parent_spec['runner']['request_sections'] = copy.deepcopy(caller_sections or [])
        parent = register_quality_task(c.store, parent_spec, c.refs)
        self.parent = run_quality_task(c.store, parent['flow_id'], True)
        self.assertEqual('valid', self.parent['state']['delivery'], self.parent['state']['errors'])
        self.assertEqual(parent_outcome, self.parent['state']['outcome'])
        repair = revision_fixture.RevisionTaskTests()
        repair.root, repair.store, repair.task = c.root, c.store, c.task
        repair.script = c.root / 'revision-role.py'
        repair.script.write_text(revision_fixture.ROLE, encoding='utf-8')
        repair.history = c.root / 'prior-answer.txt'
        repair.history.write_text('Entire final original answer, retained as an offline fixture.', encoding='utf-8')
        spec = repair.spec('private', 'repair-child')
        spec['candidate_id'] = c.candidate['candidate_id']
        if parent_outcome == 'revision_required':
            defect = self.parent['state']['invocations'][0]['defect_ids'][0]
            spec['responsibilities'][0]['id'] = defect
            spec['defect_ids'] = [defect]
            spec['defect_repairs'] = [{'defect_id': defect, 'disposition': 'private_repair'}]
        revision = register_revision_task(c.store, spec, c.refs)
        self.revision = run_revision_task(c.store, revision['flow_id'], True)
        self.assertEqual('valid', self.revision['state']['delivery'], self.revision['state']['errors'])
        self.source = validate_revision_source(c.store, revision['flow_id'])
        helper = affected_fixture.AffectedRevisionQualityTests()
        helper.case, helper.parent_quality = c, self.parent
        helper.revision, helper.source, helper.target = self.revision, self.source, self.source['candidate']
        child = helper.spec(fixture_mode or parent_outcome, 'child-qa')
        child['defect_ids'] = list(spec.get('defect_ids', []))
        prior = self.parent['state']['invocations'][0]
        collection = Path(prior['runtime']) / 'collection.json'
        child['runner']['role_session'] = {
            'mode': 'resume_existing', 'home': parent_spec['runner']['role_session']['home'],
            'previous': {**evidence_ref(collection, 'exact completed parent QA'), **_info(collection)}}
        if parent_outcome == 'revision_required':
            paths = [row['package_path'] for row in self.source['candidate']['files']]
            child['coverage']['inherited_paths'] = []
            child['coverage']['recheck_paths'] = paths
            child['coverage']['business_units'][0]['paths'] = list(paths)
            child['coverage']['related_checks'][0]['paths'] = list(paths)
        return child

    def test_parent_child_recheck_keeps_sid_and_current_reread_without_claiming_blind(self):
        spec = self.prepare_chain()
        parent = self.parent['state']['invocations'][0]
        parent_bytes = (Path(parent['runtime']) / 'collection.json').read_bytes()
        record = register_quality_task(self.case.store, spec, self.case.refs)
        result = run_quality_task(self.case.store, record['flow_id'], True)
        self.assertEqual(('completed', 'valid', 'accepted'), tuple(result['state'][k] for k in ('execution', 'delivery', 'outcome')), result['state']['errors'])
        child = result['state']['invocations'][0]
        self.assertEqual(parent['sid'], child['sid'])
        self.assertNotEqual(parent['attempt_id'], child['attempt_id'])
        self.assertNotEqual(parent['work_id'], child['work_id'])
        self.assertEqual(parent_bytes, (Path(parent['runtime']) / 'collection.json').read_bytes())
        root = Path(child['runtime'])
        manifest = read_json(root / 'manifest.json')
        self.assertEqual('quality_recheck_v1', manifest['role_transition']['kind'])
        self.assertNotIn('public_first_initial_listing', manifest)
        report = read_json(root / 'artifacts/adoption-readiness.json')
        self.assertEqual('informed_recheck', report['public_first']['mode'])
        from taskflow.execution_quality_contracts import _informed_recheck_proof
        from taskflow.execution_quality_coverage import coverage_plan

        forged = copy.deepcopy(report)
        forged['public_first']['mode'] = 'fresh_blind'
        coverage = coverage_plan(self.case.store, result['definition'], self.source['candidate'])
        with self.assertRaisesRegex(TaskflowError, 'disclose informed'):
            _informed_recheck_proof(root, forged, coverage)
        self.assertEqual('accepted', read_json(root / 'inputs/execution/revision-coverage.json')['parent_outcome'])

    def test_revision_required_parent_is_valid_context_but_has_no_unearned_inheritance(self):
        spec = self.prepare_chain(parent_outcome='revision_required')
        forged = copy.deepcopy(spec)
        inherited = forged['coverage']['recheck_paths'].pop()
        forged['coverage']['inherited_paths'] = [inherited]
        with self.assertRaisesRegex(TaskflowError, 'parent findings'):
            register_quality_task(self.case.store, forged, self.case.refs)
        record = register_quality_task(self.case.store, spec, self.case.refs)
        result = run_quality_task(self.case.store, record['flow_id'], True)
        self.assertEqual('valid', result['state']['delivery'], result['state']['errors'])
        self.assertEqual('accepted', result['state']['outcome'])
        self.assertTrue(result['state']['quality_gate']['accepted'], result['state'].get('closure_errors'))
        self.assertEqual(self.parent['state']['invocations'][0]['sid'], result['state']['invocations'][0]['sid'])
        self.assertEqual('revision_required', self.case.store.get_flow(self.parent['flow_id'])['state']['outcome'])
        self.assertEqual('closed', self.case.store.get_defect(spec['defect_ids'][0])['state'])

    def test_revision_required_unaffirmed_old_scope_can_be_fully_rechecked(self):
        spec = self.prepare_chain(parent_outcome='revision_required', parent_unchanged=True)
        record = register_quality_task(self.case.store, spec, self.case.refs)
        result = run_quality_task(self.case.store, record['flow_id'], True)
        self.assertEqual('valid', result['state']['delivery'], result['state']['errors'])
        self.assertTrue(result['state']['quality_gate']['accepted'])
        self.assertEqual([], result['definition']['coverage']['inherited_paths'])
        self.assertEqual('revision_required', self.case.store.get_flow(self.parent['flow_id'])['state']['outcome'])

    def test_child_recheck_contract_correction_stays_same_version_and_same_qa(self):
        spec = self.prepare_chain(fixture_mode='invalid-child')
        spec['correction_limit'] = 1
        record = register_quality_task(self.case.store, spec, self.case.refs)
        first = run_quality_task(self.case.store, record['flow_id'], True)
        self.assertEqual('invalid', first['state']['delivery'])
        correct_quality_task(self.case.store, record['flow_id'], self.case.refs)
        result = run_quality_task(self.case.store, record['flow_id'], True)
        self.assertEqual('valid', result['state']['delivery'], result['state']['errors'])
        before, after = result['state']['invocations']
        self.assertEqual(before['sid'], after['sid'])
        manifest = read_json(Path(after['runtime']) / 'manifest.json')
        self.assertNotIn('role_transition', manifest)
        self.assertEqual('resume_existing', manifest['role_session']['mode'])
        self.assertEqual(self.source['candidate']['candidate_id'], manifest['candidate_binding']['candidate_id'])
        request = (Path(after['runtime']) / 'request.md').read_bytes()
        self.assertNotIn(b'<taskflow_native_signoff_protocol ', request)
        self.assertIn(b'contract_errors', request)

    def test_rendered_recheck_request_omits_repeated_protocol_and_is_smaller(self):
        from taskflow.execution_flow_contracts import review_instructions
        from taskflow.execution_quality_coverage import coverage_instructions, coverage_plan

        spec = self.prepare_chain()
        record = register_quality_task(self.case.store, spec, self.case.refs)
        current = prepare_quality_task(self.case.store, record['flow_id'])['state']['invocations'][0]
        root = Path(current['runtime'])
        after = (root / 'request.md').read_bytes()
        self.assertNotIn(b'<taskflow_native_signoff_protocol ', after)
        self.assertIn(b'informed affected recheck', after)
        self.assertIn(b'revision-diff.json', after)
        config = read_json(Path(current['root']) / 'spec.json')['runner']
        context = read_json(root / 'inputs/execution/current.json')
        context.update(intent_id=current['intent_id'], defect_ids=spec['defect_ids'],
                       repair_diff={'available': True, 'index': 'execution/revision-diff.json'})
        coverage = coverage_plan(self.case.store, record['definition'], self.source['candidate'])
        baseline = copy.deepcopy(config)
        baseline['candidate_binding'] = read_json(root / 'manifest.json')['candidate_binding']
        baseline['request_sections'] = [
            {'kind': 'operation', 'text': review_instructions(context, 'affected')}
            if row['text'].startswith('Keep the full independent-quality delivery protocol') else
            {'kind': 'operation', 'text': coverage_instructions(coverage)}
            if row['text'].startswith('Read execution/revision-coverage.json, revision-diff.json') else row
            for row in baseline['request_sections']]
        before_root = self.case.root / 'qa-before' / 'runtime'
        before_work = before_root.parent / 'work'
        before_work.mkdir(parents=True)
        baseline['cwd'] = str(before_work)
        prepare_attempt(before_root, baseline)
        before = (before_root / 'request.md').read_bytes()
        self.assertIn(b'<taskflow_native_signoff_protocol ', before)
        self.assertLess(len(after), len(before))
        self.request_comparison = {'nature': 'offline_render_comparison',
            'before_request_bytes': len(before), 'after_request_bytes': len(after),
            'saved_request_bytes': len(before) - len(after), 'real_model_calls': 0,
            'limitations': 'Rendered request bytes only; no token, cache or bill saving is inferred.'}

    def test_wrong_owner_collection_role_and_whole_resume_are_rejected_before_dispatch(self):
        spec = self.prepare_chain()
        for mutation in ('owner', 'collection', 'whole'):
            changed = copy.deepcopy(spec)
            if mutation == 'owner':
                changed['owner'] = 'another-qa'
            elif mutation == 'collection':
                path = Path(self.source['attempt']['runtime_path']) / 'collection.json'
                changed['runner']['role_session']['previous'] = {**evidence_ref(path, 'author cannot become QA'), **_info(path)}
            else:
                changed['scope'] = 'whole_package'
                changed.pop('coverage')
            before = len(self.case.store.dashboard()['attempts'])
            with self.subTest(mutation=mutation), self.assertRaises(TaskflowError):
                register_quality_task(self.case.store, changed, self.case.refs)
            self.assertEqual(before, len(self.case.store.dashboard()['attempts']))

    def test_rendered_recheck_keeps_new_sections_and_omits_only_supplied_exact_caller_skill(self):
        repeated = {'kind': 'operation', 'text': 'EXACT_QA_CALLER_SKILL_V1: check actual necessary business evidence.'}
        changed = {'kind': 'operation', 'text': 'EXACT_QA_CALLER_SKILL_V2: inspect the newly changed business obligation.'}
        spec = self.prepare_chain(caller_sections=[repeated])
        spec['runner']['request_sections'] = [repeated, changed]
        record = register_quality_task(self.case.store, spec, self.case.refs)
        prepared = prepare_quality_task(self.case.store, record['flow_id'])
        current = prepared['state']['invocations'][0]
        request = (Path(current['runtime']) / 'request.md').read_text()
        self.assertNotIn(repeated['text'], request)
        self.assertIn(changed['text'], request)
        self.assertEqual([repeated, changed], prepared['definition']['runner']['request_sections'])
        parent = self.parent['state']['invocations'][0]
        from taskflow.execution_runner import _quality_locked_request
        manifest = read_json(Path(parent['runtime']) / 'manifest.json')
        self.assertIn(repeated['text'].encode(), _quality_locked_request(Path(parent['runtime']), manifest))


if __name__ == '__main__':
    unittest.main()
