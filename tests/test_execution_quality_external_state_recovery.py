"""An explicit new changed-state decision does not reset a spent allowance."""

import contextlib
import copy
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_author import _identity, validate_technical_quality_successor
from taskflow.execution_lifecycle import register_lifecycle, run_lifecycle
from taskflow.execution_experiment import _author_source, _packet
from taskflow.execution_runner import RunnerError, archive_full_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as flows
from tests import test_execution_experiment_author as experiments
from tests import test_execution_quality_technical_successor as technical_fixtures


FAIL_FIRST_TECHNICAL = """
if manifest['model']['requested']=='future-fixture-cli' and '/technical-quality/' in manifest['cwd']:
 pathlib.Path('public-responsibility-first.json').write_text('OLD_FAILED_CHECKPOINT_SENTINEL')
 print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
 message='stream disconnected before completion: Transport error: network error: local changed-state fixture'
 print(json.dumps({'type':'error','message':message}),flush=True)
 print(json.dumps({'type':'turn.failed','error':{'message':message}}),flush=True)
 sys.exit(1)
"""


class ExternalStateQualityRecoveryTests(unittest.TestCase):
    def setUp(self):
        fixture = technical_fixtures.QualityTechnicalSuccessorTests()
        with patch.object(flows, 'ROLE_FIXTURE', FAIL_FIRST_TECHNICAL + flows.ROLE_FIXTURE):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.store = fixture.case.store
        first = fixture.register()
        self.run_legacy_backend(first['flow_id'])
        self.source = self.store.get_flow(first['flow_id'])
        self.assertEqual('exception', self.source['state']['status'])
        child = self.store.get_flow(self.source['state']['child_flow_id'])
        failed = self.store.list_attempts(child['state']['exception']['work_id'])[-1]
        self.failed = failed
        archive_full_attempt(Path(failed['runtime_path']), Path(child['definition']['directory']) / 'archives' / failed['attempt_id'])
        technical = {'source_parent_flow_id':first['flow_id'], 'source_parent_definition_digest':first['definition_digest'],
                     'source_child_flow_id':child['flow_id'], 'source_child_definition_digest':child['definition_digest'],
                     'failed_attempt_id':failed['attempt_id']}
        self.authority = copy.deepcopy(fixture.authority)
        self.authority['technical_source'] = technical
        self.event_path = fixture.case.root / 'changed-state-event.json'
        self.event = {'schema_version':1, 'maximum_new_quality_attempts':1,
                      'predecessor_failed_attempt':failed['attempt_id'], 'predecessor_sid':failed['cli_sid'],
                      **{k:v for k,v in self.authority['candidate_binding'].items() if k != 'task_id'},
                      'model':'future-fixture-cli', 'reasoning_effort':'fixture', 'thinking':'enabled',
                      'sole_dispatcher':'fixture-controller', 'source_thread_id':'neutral-controller',
                      'human_quote':'External condition reported changed.', 'evidence_limit':'Report only; no new probe.',
                      'decision_maker':'Existing controller; one additional bounded decision.',
                      'decision':'One fresh whole review after this specific failed predecessor.'}
        self.external_path = fixture.case.root / 'external-state-binding.json'
        self.authority_path = fixture.case.root / 'external-state-authority.json'
        self.definition = copy.deepcopy(self.source['definition'])
        self.definition.update(flow_id=_identity('neutral', 'quality-technical-' + failed['attempt_id']),
                               directory=str(fixture.case.root / 'network-restored-quality'))
        self.refresh()

    def refresh(self):
        write_json(self.event_path, self.event)
        event_ref = evidence_ref(self.event_path, 'Actual new explicit changed-state decision fixture')
        write_json(self.external_path, {'schema_version':1, 'kind':'explicit_external_state_quality_recovery_v1',
                   'external_state_event':event_ref,
                   'previous_technical_authority':self.source['definition']['technical_quality_successor']['authority']})
        self.authority.update(decision=event_ref, external_state_recovery=evidence_ref(self.external_path, 'Mechanical exact event and spent-allowance binding'))
        write_json(self.authority_path, self.authority)
        self.definition['technical_quality_successor'] = {**self.authority['technical_source'],
                    'authority':evidence_ref(self.authority_path, 'One new finite allowance; previous remains spent')}

    def register(self):
        return register_lifecycle(self.store, self.definition,
                    [*self.fixture.case.refs, self.definition['technical_quality_successor']['authority']])

    def run_legacy_backend(self, flow_id):
        return run_lifecycle(self.store, flow_id, allow_model=True, poll_seconds=.001)

    def run_cli(self, flow_id):
        """Historical fixture helper name; this never invokes the retired CLI."""
        return self.run_legacy_backend(flow_id)

    def test_same_policy_new_event_adds_one_fresh_review_preserves_all_ancestors_and_is_idempotent(self):
        before = len(self.store.dashboard()['attempts'])
        original = copy.deepcopy(self.source)
        result = self.register()
        proof = validate_technical_quality_successor(self.store, self.definition)
        self.assertIn(self.failed['cli_sid'], proof['excluded_sids'])
        self.assertEqual(4, len(proof['excluded_sids']))
        self.assertTrue(proof['previous_allowance_spent'])
        self.run_legacy_backend(result['flow_id'])
        current = self.store.get_flow(result['flow_id'])
        self.assertEqual('accepted', current['state']['status'], current)
        self.assertEqual(before+1, len(self.store.dashboard()['attempts']))
        self.assertEqual(original, self.store.get_flow(original['flow_id']))
        helper = experiments.AuthorExperimentTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        experiment = helper.definition(self.fixture.case, current)
        experiment['policy_id'] = 'fixture-future'
        packet = _packet({'definition':experiment})
        bound = _author_source(self.store, {'definition':experiment,'state':{}}, packet, require_candidate=True)
        self.assertEqual(self.fixture.case.attempt['attempt_id'], bound['binding']['source_author_attempt'])
        self.register()
        self.run_legacy_backend(result['flow_id'])
        self.assertEqual(before+1, len(self.store.dashboard()['attempts']))
        with self.assertRaises(TaskflowError):
            validate_technical_quality_successor(self.store, {**self.definition,'flow_id':'duplicate-event-alias'})

    def test_no_new_event_wrong_predecessor_or_partial_signature_are_rejected(self):
        del self.authority['external_state_recovery']
        write_json(self.authority_path,self.authority)
        self.definition['technical_quality_successor']['authority']=evidence_ref(self.authority_path,'No event')
        with self.assertRaisesRegex(TaskflowError,'no recursive successor'):
            self.register()
        self.refresh()
        for field,value in [('predecessor_failed_attempt',self.fixture.failed['attempt_id']),
                            ('predecessor_sid',self.fixture.failed['cli_sid']),
                            ('model','other-model'),('maximum_new_quality_attempts',True)]:
            old=self.event[field];self.event[field]=value;self.refresh()
            with self.subTest(field=field),self.assertRaises(TaskflowError):self.register()
            self.event[field]=old;self.refresh()
        partial=Path(self.failed['runtime_path'])/'artifacts/intake.md';partial.parent.mkdir(exist_ok=True)
        partial.write_text('Partial review cannot become an absent signoff.')
        with self.assertRaises((TaskflowError,RunnerError)):self.register()

    def test_changed_same_policy_model_or_reused_spent_event_is_rejected(self):
        original=self.store.get_policy
        def policy(identity):
            result=copy.deepcopy(original(identity))
            if identity=='fixture-future':result['config']['roles']['quality']['model']='changed-model'
            return result
        with patch.object(self.store,'get_policy',side_effect=policy),self.assertRaises(TaskflowError):self.register()
        self.authority['decision']=self.source['definition']['technical_quality_successor']['authority']
        write_json(self.authority_path,self.authority)
        self.definition['technical_quality_successor']['authority']=evidence_ref(self.authority_path,'Wrong event binding')
        with self.assertRaises(TaskflowError):self.register()


if __name__=='__main__':
    unittest.main()
