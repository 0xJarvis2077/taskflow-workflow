"""Offline continuation after a complete delivery whose model turn failed."""
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_author import _identity, validate_technical_quality_successor
from taskflow.execution_lifecycle import register_lifecycle
from taskflow.execution_runner import RunnerError, archive_full_attempt
from taskflow.execution_store import evidence_ref
from tests import test_direct_human_quality_successor as direct
from tests import test_execution_flow as flows


FAIL_WITH_DELIVERY = """
if manifest['model']['requested']=='future-fixture-cli' and '/direct-quality/' in manifest['cwd']:
 pathlib.Path('public-responsibility-first.json').write_text('FAILED_PRIVATE_CHECKPOINT')
 for name in ('intake.md','issues.json','adoption-readiness.json'):
  pathlib.Path(name).write_text('FAILED_PRIVATE_DELIVERY')
 print(json.dumps({'type':'thread.started','thread_id':str(uuid.uuid4())}),flush=True)
 message='stream disconnected before completion: error sending request'
 print(json.dumps({'type':'turn.failed','error':{'message':message}}),flush=True)
 sys.exit(1)
if '/fresh-after-complete-delivery/' in manifest['cwd']:
 assert not pathlib.Path('public-responsibility-first.json').exists()
 for path in pathlib.Path(sys.argv[sys.argv.index('--inputs')+1]).rglob('*'):
  if path.is_file():
   assert 'FAILED_PRIVATE_DELIVERY' not in path.read_text()
   assert 'FAILED_PRIVATE_CHECKPOINT' not in path.read_text()
"""


class FailedDeliverySuccessorTests(unittest.TestCase):
    def setUp(self):
        case = direct.DirectHumanQualitySuccessorTests()
        with patch.object(flows, 'ROLE_FIXTURE', FAIL_WITH_DELIVERY + flows.ROLE_FIXTURE):
            case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        self.store = case.store
        source = case.register()
        case.case.run_cli(source['flow_id'])
        self.source = self.store.get_flow(source['flow_id'])
        self.assertEqual('exception', self.source['state']['status'])
        child = self.store.get_flow(self.source['state']['child_flow_id'])
        self.failed = self.store.list_attempts(child['state']['exception']['work_id'])[-1]
        archive_full_attempt(Path(self.failed['runtime_path']),
                             Path(child['definition']['directory']) / 'archives' / self.failed['attempt_id'])
        self.tech = {'source_parent_flow_id': source['flow_id'],
                     'source_parent_definition_digest': source['definition_digest'],
                     'source_child_flow_id': child['flow_id'],
                     'source_child_definition_digest': child['definition_digest'],
                     'failed_attempt_id': self.failed['attempt_id']}
        root = case.case.fixture.case.root
        binding = case.case.authority['candidate_binding']
        self.human = {'schema_version': 1, 'authority_kind': 'direct_human_instruction_in_q1_controller_chat',
                      **{k: binding[k] for k in ('candidate_id', 'input_digest', 'content_digest', 'evaluation_digest')},
                      'sole_q1_dispatcher': 'fixture-controller', 'source_thread_id': 'neutral-controller',
                      'human_quote': 'I authorize a new independent quality check.',
                      'scope': 'one same-candidate review', 'after_quality': 'follow the quality result',
                      'quality': 'One new independent whole-quality continuation, future-fixture-cli/fixture, thinking enabled. Public-first own checkpoint with actual SHA readback, then teacher materials.',
                      'prior_explicit_data_and_cost_authorization': case.human_path.name}
        self.human_path = root / 'fresh-human.json'
        self.binding_path = root / 'failed-delivery-binding.json'
        self.authority_path = root / 'failed-delivery-authority.json'
        self.definition = copy.deepcopy(self.source['definition'])
        self.definition['directory'] = str(root / 'fresh-after-complete-delivery')
        self.refresh()

    def refresh(self):
        write_json(self.human_path, self.human)
        event = evidence_ref(self.human_path, 'fresh human fixture')
        write_json(self.binding_path, {'schema_version': 1,
                   'kind': 'direct_human_failed_complete_delivery_quality_recovery_v1',
                   'external_state_event': event,
                   'previous_technical_authority': self.source['definition']['technical_quality_successor']['authority'],
                   'prior_data_and_cost_authorization': evidence_ref(self.case.human_path, 'earlier human fixture')})
        self.definition['flow_id'] = _identity('neutral', 'quality-human-' + event['sha256'])
        authority = copy.deepcopy(self.case.case.authority)
        authority.update(technical_source=self.tech, decision=event,
                         external_state_recovery=evidence_ref(self.binding_path, 'failed delivery fixture'))
        write_json(self.authority_path, authority)
        self.definition['technical_quality_successor'] = {**self.tech,
            'authority': evidence_ref(self.authority_path, 'fresh bounded authorization')}

    def register(self):
        return register_lifecycle(self.store, self.definition,
            [*self.case.case.fixture.case.refs, self.definition['technical_quality_successor']['authority']])

    def test_new_independent_review_and_idempotent_registration(self):
        before = len(self.store.dashboard()['attempts'])
        old = copy.deepcopy(self.source)
        flow = self.register()
        self.assertEqual(flow, self.register())
        proof = validate_technical_quality_successor(self.store, self.definition)
        self.assertIn(self.failed['cli_sid'], proof['excluded_sids'])
        self.case.case.run_cli(flow['flow_id'])
        self.assertEqual('accepted', self.store.get_flow(flow['flow_id'])['state']['status'])
        self.assertEqual(before + 1, len(self.store.dashboard()['attempts']))
        self.assertEqual(old, self.store.get_flow(old['flow_id']))

    def test_wrong_human_or_changed_original_fails_closed(self):
        for key in ('content_digest', 'sole_q1_dispatcher', 'quality', 'prior_explicit_data_and_cost_authorization'):
            original = self.human[key]
            self.human[key] = 'incorrect'
            self.refresh()
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                self.register()
            self.human[key] = original
        self.refresh()
        (Path(self.failed['runtime_path']) / 'artifacts' / 'intake.md').write_text('modified')
        with self.assertRaises((TaskflowError, RunnerError)):
            self.register()


if __name__ == '__main__':
    unittest.main()
