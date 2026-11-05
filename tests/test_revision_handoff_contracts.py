"""Offline contracts distinguish history availability, use and business analysis."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError
from taskflow.execution_revision_contracts import (
    HISTORY_ROLES, history_inventory, observed_source_use, responsibility_analysis,
)
from taskflow.execution_store import evidence_ref


class RevisionHandoffContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.original = self.root / 'answer.txt'
        self.original.write_text('A complete lawful result with its processing evidence.\n')
        self.spec = {'sources': [{**evidence_ref(self.original, 'L1'), 'source_role':'answer',
                                 'target':'history/answer.txt'}],
                     'responsibilities':[{'id':'capacity','paths':['public.txt']}], 'diagnosis':{}}
        self.parent = {'files':[{'package_path':'public.txt'}]}
        self.diff = {'changes':[{'path':'reference.txt'}], 'declaration_change':None}
        def unit():
            return {'status':'supported', 'summary':'Concrete processing and consequence.',
                    'evidence':[{'target':'history/answer.txt', 'location':'L1'}]}
        self.report = {'responsibility_analysis':[{'id':'capacity','result':'success',
            **{key:unit() for key in ('public_work','processing','final_result','business_consequence')},
            'next_action':{'disposition':'retained','reason':'The lawful normal method remains applicable.',
                           'changed_paths':[]}}]}
        self.report['success_method_coverage'] = {'status':'supported',
            'summary':'The legal short method still covers the capacity and normal alternative decisions.',
            'covered_responsibilities':['Capacity feasibility and the normal alternative.'],
            'evidence':[{'target':'history/answer.txt','location':'L1'}]}

    def test_inventory_preserves_all_history_categories_and_does_not_infer_use(self):
        archive = evidence_ref(self.original, 'archive complete original entry')
        self.spec['diagnosis']['history_inventory'] = [
            {'source_role':'request','status':'archived_not_provided','reason':'Archive exists; this turn did not receive it.',
             'originals':[archive]},
            {'source_role':'code','status':'does_not_exist','reason':'The retained run had no code artifact.'}]
        rows = {row['source_role']:row for row in history_inventory(self.spec)}
        self.assertEqual(set(HISTORY_ROLES), set(rows))
        self.assertEqual('provided', rows['answer']['status'])
        self.assertEqual('archived_not_provided', rows['request']['status'])
        self.assertEqual('does_not_exist', rows['code']['status'])
        self.assertEqual('unknown', rows['intermediate']['status'])
        self.assertNotIn('used', {row['status'] for row in rows.values()})

    def test_inventory_rejects_unproved_or_contradictory_availability(self):
        bad = [None, [{'source_role':'request','status':'used','reason':'Read the logs.'}],
            [{'source_role':'answer','status':'does_not_exist','reason':'Pretend missing.'}],
            [{'source_role':'request','status':'provided','reason':'No actual typed input.'}],
            [{'source_role':'request','status':'archived_not_provided','reason':'No archive reference.'}],
            [{'source_role':'request','status':'unknown','reason':'Unknown','originals':[evidence_ref(self.original,'L1')]}],
            [{'source_role':'request','status':'unknown','reason':' '}],
            [{'source_role':'request','status':'unknown','reason':'Unknown','originals':None}],
            [{'source_role':'request','status':'unknown','reason':'Unknown'}]*2]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                spec = copy.deepcopy(self.spec)
                spec['diagnosis']['history_inventory'] = value
                history_inventory(spec)

    def test_successful_method_may_be_retained_and_unknowns_do_not_force_new_calls(self):
        self.assertEqual(self.report['responsibility_analysis'],
                         responsibility_analysis(self.report, self.spec, self.diff, self.parent))
        row = self.report['responsibility_analysis'][0]
        row['processing'] = {'status':'unknown','summary':'Only the successful final artifact remains.','evidence':[]}
        self.assertEqual('success', responsibility_analysis(self.report,self.spec,self.diff,self.parent)[0]['result'])
        row['result'] = 'unknown'
        for key in ('public_work','processing','final_result','business_consequence'):
            row[key] = {'status':'unknown','summary':'Original evidence is unavailable; no invented process.','evidence':[]}
        row['next_action'] = {'disposition':'unknown','reason':'Resolve the business question before another investment.',
                              'changed_paths':[]}
        self.assertEqual('unknown', responsibility_analysis(self.report,self.spec,self.diff,self.parent)[0]['result'])

    def test_business_chain_rejects_missing_units_fabricated_citations_and_unmade_changes(self):
        mutations = [lambda row: row.pop('processing'), lambda row: row.update(result='score_improved'),
            lambda row: row['public_work'].update(evidence=[]),
            lambda row: row['final_result'].update(evidence=[{'target':'foreign.txt','location':'L1'}]),
            lambda row: row['business_consequence'].update(summary=' '),
            lambda row: row['next_action'].update(disposition='changed',changed_paths=['unmodified.txt']),
            lambda row: row['next_action'].update(changed_paths=['reference.txt']),
            lambda row: row['next_action'].update(reason=' ')]
        for mutate in mutations:
            report = copy.deepcopy(self.report)
            mutate(report['responsibility_analysis'][0])
            with self.subTest(report=report), self.assertRaises(TaskflowError):
                responsibility_analysis(report,self.spec,self.diff,self.parent)
        for value in (None, [], [{'id':'foreign'}]):
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                responsibility_analysis({'responsibility_analysis':value},self.spec,self.diff,self.parent)
        for coverage in (None, {}, {'status':'supported','summary':'Read logs.','covered_responsibilities':[],'evidence':[]}):
            report = copy.deepcopy(self.report)
            report['success_method_coverage'] = coverage
            with self.subTest(coverage=coverage), self.assertRaises(TaskflowError):
                responsibility_analysis(report,self.spec,self.diff,self.parent)

    def test_actual_change_action_matches_diff_including_scoring_declaration(self):
        row = self.report['responsibility_analysis'][0]
        row['next_action'].update(disposition='changed',changed_paths=['reference.txt'])
        responsibility_analysis(self.report,self.spec,self.diff,self.parent)
        self.diff['declaration_change'] = {'before':{},'after':{}}
        row['next_action']['changed_paths'] = ['task.json']
        responsibility_analysis(self.report,self.spec,self.diff,self.parent)

    def test_used_requires_real_completed_visible_tool_return_and_analysis_link(self):
        event = {'type':'item.completed','item':{'type':'command_execution',
            'command':'cat history/answer.txt','exit_code':0,'aggregated_output':self.original.read_text()}}
        (self.root/'trace.jsonl').write_text(json.dumps(event)+'\n')
        self.assertEqual([], observed_source_use(self.report,self.spec,self.root))
        self.report['source_usage'] = [{'target':'history/answer.txt','trace_lines':[1],'analysis_ids':['capacity']}]
        self.assertEqual('used', observed_source_use(self.report,self.spec,self.root)[0]['status'])
        for mutation in ({'trace_lines':[2]}, {'trace_lines':[True]}, {'target':'history/foreign.txt'},
                         {'analysis_ids':['foreign']}, {'analysis_ids':[]}, {'trace_lines':[]}):
            report = copy.deepcopy(self.report)
            report['source_usage'][0].update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(TaskflowError):
                observed_source_use(report,self.spec,self.root)
        for item in ({'type':'agent_message','text':'Read the logs.'},
                     {**event['item'],'exit_code':1}, {**event['item'],'aggregated_output':''}):
            (self.root/'trace.jsonl').write_text(json.dumps({'type':'item.completed','item':item})+'\n')
            with self.subTest(item=item), self.assertRaises(TaskflowError):
                observed_source_use(self.report,self.spec,self.root)


if __name__ == '__main__':
    unittest.main()
