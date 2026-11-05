"""Explicit revision fixtures exercise the real queue and local CLI, never models."""

from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_revision import (
    collect_revision_task, prepare_revision_task, register_revision_task,
    run_revision_task, status_revision_task, validate_revision_source,
)
from taskflow.execution_runner import RunnerError, _check_seeded_outputs, _validate
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


ROLE = r'''
import hashlib,json,pathlib,sys
mode=sys.argv[1];request=sys.stdin.read()
print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000012'}),flush=True)
root=pathlib.Path.cwd();inputs=root.parent/'runtime/inputs'
brief=json.loads((inputs/'execution/revision-brief.json').read_text())
index=json.loads((inputs/'execution/original-index.json').read_text())
assert 'This is the entire prior final answer, not merely lost points.' not in request
assert (inputs/'history/answer.txt').is_file()
candidate=root/'candidate'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
before={r['path']:r for r in index['candidate_files']}
declaration_before=digest(candidate/'task.json')
if mode in ('private','missing','wrong-sha','decision','scope-text'):
 (candidate/'draft/evaluation/answer.md').write_text('2 <= 3. Retain this valid short method and clarify its applicability.\nRevision '+brief['candidate_id']+'\n')
elif mode=='public':
 (candidate/'draft/input/prompt.md').write_text('Choose 2 units from capacity 3; report the valid normal alternative.\n')
elif mode=='declaration':
 path=candidate/'task.json';data=json.loads(path.read_text())
 data['scoring']['criteria'][0]['condition']='A feasible result with explicit capacity evidence and lawful equivalents.'
 path.write_text(json.dumps(data))
elif mode=='extra':(candidate/'undeclared.md').write_text('An untracked material change must not disappear.')
elif mode=='failure':sys.exit(1)
changes=[]
for name,row in before.items():
 after=digest(candidate/name)
 if after!=row['sha256']:changes.append({'path':name,'before_sha256':row['sha256'],'after_sha256':after,
  'reason':'Clarify the assigned responsibility.','impact':'Read the linked reference and public constraint together.'})
if mode=='missing':changes=[]
if mode=='wrong-sha':changes[0]['before_sha256']='0'*64
declaration_after=digest(candidate/'task.json')
declaration=None if declaration_before==declaration_after else {'before_sha256':declaration_before,'after_sha256':declaration_after,
 'reason':'Clarify the prospective criterion.','impact':'Review the complete current standard; retain historical grades.'}
report={'role':'repair','parent_id':brief['parent_id'],'candidate_id':brief['candidate_id'],
 'defect_ids':[r['id'] for r in brief['responsibilities']],'needs_business_decision':mode=='decision','changes':changes,
 'declaration_change':declaration,'impact_analysis':{'summary':'The original lawful result is preserved.',
 'recommended_scope':'affected','associated_paths':['draft/input/prompt.md','draft/evaluation/answer.md']}}
def unit(summary,target):return {'status':'supported','summary':summary,'evidence':[{'target':target,'location':'L1'}]}
changed=[r['path'] for r in changes]+(['task.json'] if declaration is not None else [])
report['responsibility_analysis']=[{'id':r['id'],'result':'success',
 'public_work':unit('The public constraint gives capacity but leaves feasibility to the solver.','parent/draft/input/prompt.md'),
 'processing':unit('The prior final answer applies the capacity comparison lawfully.','history/answer.txt'),
 'final_result':unit('The entire prior answer retains its feasible normal solution.','history/answer.txt'),
 'business_consequence':unit('The lawful method remains applicable to the business constraint.','parent/draft/input/prompt.md'),
 'next_action':{'disposition':'changed' if changed else 'retained','reason':'Clarify applicability while retaining the valid normal method.',
 'changed_paths':changed}} for r in brief['responsibilities']]
report['success_method_coverage']={'status':'supported','summary':'The lawful short capacity method remains valid for feasible normal choices.',
 'covered_responsibilities':['Capacity feasibility and the normal lawful alternative.'],
 'evidence':[{'target':'history/answer.txt','location':'L1'}]}
if mode == 'scope-text': report['impact_analysis']['recommended_scope'] = 'Independent whole-package review and related checks.'
if mode == 'scope-empty': report['impact_analysis']['recommended_scope'] = '  '
if mode == 'scope-null': report['impact_analysis']['recommended_scope'] = None
if mode == 'scope-list': report['impact_analysis']['recommended_scope'] = ['whole_package']
(root/'repair-record.json').write_text(json.dumps(report))
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Changed files and impact are recorded; unchanged originals retained.'}}),flush=True)
print(json.dumps({'type':'turn.completed','usage':{'input_tokens':113,'cached_input_tokens':17,'output_tokens':29}}),flush=True)
'''


class RevisionTaskTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = ExecutionStore(self.root / "state.sqlite3")
        self.store.register_policy("fixture-v1", fixture_policy())
        self.task = make_task(self.root)
        self.parent = capture_candidate(self.store, self.task, "parent-v1")
        self.original = {p: (self.task / p).read_bytes() for p in ["task.json", *[row["package_path"] for row in self.parent["files"]]]}
        self.script = self.root / "revision-role.py"
        self.script.write_text(ROLE, encoding="utf-8")
        self.authority = self.root / "authority.json"
        write_json(self.authority, {"nature": "test_fixture", "purpose": "single explicit material revision"})
        self.refs = [evidence_ref(self.authority, "neutral test authority")]
        self.history = self.root / "prior-answer.txt"
        self.history.write_text("This is the entire prior final answer, not merely lost points.\n", encoding="utf-8")

    def spec(self, mode="private", identity="revision-one"):
        (self.root / (identity + "-home")).mkdir(exist_ok=True)
        return {"schema_version": 1, "flow_id": identity, "task_id": "neutral", "owner": "fixture-owner",
                "policy_id": "fixture-v1", "directory": str(self.root / identity),
                "candidate_id": "parent-v1", "candidate_root": str(self.task), "revised_candidate_id": identity + "-candidate",
                "purpose": "Clarify a known reference responsibility after diagnosis.",
                "purpose_kind": "quality_repair", "author_id": "fixture-author", "session_reason": "initial_author",
                "expected_changes": [{"path": "draft/evaluation/answer.md", "reason": "Clarify applicability."}],
                "responsibilities": [{"id": "R1", "description": "Reference/public constraint applicability.",
                                      "paths": ["draft/evaluation/answer.md", "draft/input/prompt.md"]}],
                "sources": [{**evidence_ref(self.history, "full original final answer"), "target": "history/answer.txt", "source_role": "answer"}],
                "runner": {"mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                           "inputs": [], "request_sections": [], "dependencies": [str(self.script)],
                           "argv": [str(Path(sys.executable).resolve()), str(self.script), mode],
                           "role_session": {"mode": "fresh_ephemeral", "home": str(self.root / (identity + "-home")), "previous": None}}}

    def register(self, mode="private", identity="revision-one"):
        return register_revision_task(self.store, self.spec(mode, identity), self.refs)

    def assert_parent_unchanged(self):
        for name, raw in self.original.items():
            self.assertEqual(raw, (self.task / name).read_bytes(), name)
        self.assertEqual(self.parent, self.store.get_candidate("parent-v1"))

    def test_copy_edit_capture_preserves_parent_and_only_changes_needed_file(self):
        record = self.register()
        prepared = prepare_revision_task(self.store, record["flow_id"])
        current = prepared["state"]["invocation"]
        clone = Path(current["candidate_root"])
        self.assertEqual(self.original["draft/evaluation/answer.md"], (clone / "draft/evaluation/answer.md").read_bytes())
        self.assertNotEqual((clone / "draft/evaluation/answer.md").stat().st_ino, (self.task / "draft/evaluation/answer.md").stat().st_ino)
        result = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual(("completed", "valid", "revised"), tuple(result["state"][key] for key in ("execution", "delivery", "outcome")))
        source = validate_revision_source(self.store, record["flow_id"])
        self.assertEqual(["draft/evaluation/answer.md"], [row["path"] for row in source["diff"]["changes"]])
        self.assertIsNone(source["diff"]["declaration_change"])
        self.assertEqual(self.parent["input_digest"], source["candidate"]["input_digest"])
        self.assertEqual("parent-v1", source["candidate"]["parent_id"])
        self.assertEqual(source["attempt"]["attempt_id"], source["candidate"]["author_attempt"])
        self.assertEqual(self.original["draft/input/prompt.md"], (source["root"] / "draft/input/prompt.md").read_bytes())
        metrics = read_json(Path(result["metrics"]["path"]))
        self.assertEqual(1, metrics["management_invocations"])
        self.assertEqual(113, metrics["usage_summary"]["input_tokens"])
        self.assertEqual(17, metrics["usage_summary"]["cached_input_tokens"])
        self.assertEqual(29, metrics["usage_summary"]["output_tokens"])
        self.assertIsNone(metrics["actual_billing"])
        self.assert_parent_unchanged()

    def test_scope_recommendation_is_nonempty_text_and_does_not_dispatch_quality(self):
        record = self.register('scope-text')
        result = run_revision_task(self.store, record['flow_id'], True)
        self.assertEqual('valid', result['state']['delivery'])
        self.assertEqual('revised', result['state']['outcome'])
        self.assertEqual('Independent whole-package review and related checks.',
                         result['state']['impact_analysis']['recommended_scope'])
        self.assertEqual(1, len(self.store.dashboard()['work']))

    def test_recollection_keeps_original_attempt_and_prior_invalid_event(self):
        record = self.register('scope-text')
        with patch('taskflow.execution_revision._validate_author', side_effect=TaskflowError('old scope rule')):
            rejected = run_revision_task(self.store, record['flow_id'], True)
        self.assertEqual('invalid', rejected['state']['delivery'])
        current = rejected['state']['invocation']
        attempt_id = current['attempt_id']
        report = Path(current['runtime']) / 'artifacts/repair-record.json'
        original = report.read_bytes()
        received = collect_revision_task(self.store, record['flow_id'])
        self.assertEqual('valid', received['state']['delivery'])
        self.assertEqual('revised', received['state']['outcome'])
        self.assertEqual(attempt_id, received['state']['invocation']['attempt_id'])
        self.assertEqual(original, report.read_bytes())
        self.assertEqual(1, len(self.store.list_attempts(current['work_id'])))
        states = [event['data']['state']['delivery'] for event in self.store.events()
                  if event['kind'] == 'flow_updated' and event['data'].get('flow_id') == record['flow_id']
                  and 'delivery' in event['data']['state']]
        self.assertIn('invalid', states)
        self.assertEqual('valid', states[-1])

    def test_scope_recommendation_rejects_empty_or_non_text(self):
        for mode in ('scope-empty', 'scope-null', 'scope-list'):
            with self.subTest(mode=mode):
                record = self.register(mode, mode)
                result = run_revision_task(self.store, record['flow_id'], True)
                self.assertEqual('invalid', result['state']['delivery'])
                self.assertIsNone(result['state']['outcome'])

    def test_no_requested_or_actual_change_creates_no_work_candidate_or_call(self):
        spec = self.spec("nochange")
        spec["expected_changes"] = []
        registered = register_revision_task(self.store, spec, self.refs)
        result = run_revision_task(self.store, registered["flow_id"], True)
        self.assertEqual(("not_started", "valid", "no_change"), tuple(result["state"][key] for key in ("execution", "delivery", "outcome")))
        self.assertEqual([], self.store.dashboard()["work"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        with self.assertRaises(TaskflowError):self.store.get_candidate(spec["revised_candidate_id"])
        self.assertEqual(result, collect_revision_task(self.store, registered["flow_id"]))
        self.assert_parent_unchanged()

    def test_actual_noop_after_role_does_not_manufacture_candidate(self):
        record = self.register("nochange")
        result = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual("no_change", result["state"]["outcome"])
        with self.assertRaises(TaskflowError):self.store.get_candidate(record["definition"]["revised_candidate_id"])
        self.assertEqual(1, len(self.store.list_attempts(result["state"]["invocation"]["work_id"])))

    def test_scoring_declaration_only_change_is_real_new_standard(self):
        record = self.register("declaration")
        result = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        source = validate_revision_source(self.store, record["flow_id"])
        self.assertEqual([], source["diff"]["changes"])
        self.assertIsNotNone(source["diff"]["declaration_change"])
        self.assertNotEqual(self.parent["evaluation_digest"], source["candidate"]["evaluation_digest"])
        self.assertEqual(self.parent["input_digest"], source["candidate"]["input_digest"])
        self.assert_parent_unchanged()

    def test_public_change_is_visible_in_actual_difference(self):
        record = self.register("public")
        run_revision_task(self.store, record["flow_id"], True)
        source = validate_revision_source(self.store, record["flow_id"])
        self.assertNotEqual(self.parent["input_digest"], source["candidate"]["input_digest"])
        self.assertEqual("draft/input/prompt.md", source["diff"]["changes"][0]["path"])

    def test_missing_or_wrong_author_declaration_returns_exact_difference_without_capture(self):
        for mode in ("missing", "wrong-sha", "decision"):
            with self.subTest(mode=mode):
                record = self.register(mode, "revision-" + mode)
                result = run_revision_task(self.store, record["flow_id"], True)
                self.assertEqual("invalid", result["state"]["delivery"])
                self.assertEqual("draft/evaluation/answer.md", result["state"]["actual_difference"]["changes"][0]["path"])
                self.assertTrue((Path(result["state"]["invocation"]["root"]) / "archive/archive-binding.json").is_file())
                with self.assertRaises(TaskflowError):self.store.get_candidate(record["definition"]["revised_candidate_id"])
                repeated = run_revision_task(self.store, record["flow_id"], True)
                self.assertEqual(1, len(self.store.list_attempts(repeated["state"]["invocation"]["work_id"])))
        self.assert_parent_unchanged()

    def test_undeclared_material_is_not_silently_ignored(self):
        record = self.register("extra")
        result = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual("invalid", result["state"]["delivery"])
        self.assertIn("complete declared file membership", result["state"]["errors"][0]["message"])

    def test_repeated_and_concurrent_operations_dispatch_once(self):
        spec = self.spec()
        registered = register_revision_task(self.store, spec, self.refs)
        self.assertEqual(registered, register_revision_task(self.store, spec, self.refs))
        prepared = prepare_revision_task(self.store, registered["flow_id"])
        self.assertEqual(prepared, prepare_revision_task(self.store, registered["flow_id"]))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: run_revision_task(self.store, registered["flow_id"], True), range(2)))
        self.assertTrue(all(row["state"]["delivery"] == "valid" for row in results))
        self.assertEqual(1, len(self.store.list_attempts(prepared["state"]["invocation"]["work_id"])))
        result = collect_revision_task(self.store, registered["flow_id"])
        self.assertEqual(result, collect_revision_task(self.store, registered["flow_id"]))
        self.assertEqual({key: value for key, value in result.items() if key != "metrics"}, status_revision_task(self.store, registered["flow_id"]))

    def test_failed_role_is_never_replayed(self):
        record = self.register("failure")
        result = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual("failed", result["state"]["execution"])
        repeated = run_revision_task(self.store, record["flow_id"], True)
        self.assertEqual(1, len(self.store.list_attempts(repeated["state"]["invocation"]["work_id"])))

    def test_original_mutation_and_shared_directories_are_rejected(self):
        spec = self.spec()
        for directory in (self.task, self.task / "revision", self.task.parent):
            invalid = copy.deepcopy(spec);invalid["directory"] = str(directory)
            with self.assertRaises(TaskflowError):register_revision_task(self.store, invalid, self.refs)
        record = self.register()
        (self.task / "draft/input/prompt.md").write_text("Changed outside the role.", encoding="utf-8")
        with self.assertRaises(TaskflowError):prepare_revision_task(self.store, record["flow_id"])

    def test_source_index_is_short_and_full_original_is_still_readable(self):
        record = self.register()
        prepared = prepare_revision_task(self.store, record["flow_id"])
        runtime = Path(prepared["state"]["invocation"]["runtime"])
        request = (runtime / "request.md").read_text(encoding="utf-8")
        self.assertNotIn(self.history.read_text(), request)
        self.assertEqual(self.history.read_bytes(), (runtime / "inputs/history/answer.txt").read_bytes())
        index = read_json(runtime / "inputs/execution/original-index.json")
        self.assertEqual("full original final answer", index["diagnosis_sources"][0]["location"])

    def continuation(self, first, identity="revision-two"):
        source = validate_revision_source(self.store, first['flow_id'])
        spec = self.spec(identity=identity)
        spec.update(candidate_id=source['candidate']['candidate_id'], candidate_root=str(source['root']),
                    source_revision_task_id=first['flow_id'])
        spec.pop('session_reason')
        runtime = Path(first['state']['invocation']['runtime'])
        original = first['definition']['runner']['role_session']
        from taskflow.execution_runner import _info
        spec['runner']['role_session'] = {'mode':'resume_existing', 'home':original['home'],
            'previous':{**evidence_ref(runtime/'collection.json', 'exact author source'), **_info(runtime/'collection.json')}}
        return spec

    def test_parent_child_author_reuses_exact_sid_with_new_invocation_and_complete_handoff(self):
        spec = self.spec()
        spec['runner']['role_session']['mode'] = 'fresh_persistent'
        repeated = {'kind':'operation','text':'A stable operation instruction remains in this author context. '*30}
        spec['runner']['request_sections'] = [repeated]
        first = register_revision_task(self.store, spec, self.refs)
        first = run_revision_task(self.store, first['flow_id'], True)
        original_collection = (Path(first['state']['invocation']['runtime'])/'collection.json').read_bytes()
        following = self.continuation(first)
        following['runner']['request_sections'] = [repeated, {'kind':'operation','text':'Current-only evidence needs verification.'}]
        registered = register_revision_task(self.store, following, self.refs)
        second = run_revision_task(self.store, registered['flow_id'], True)
        self.assertEqual('valid', second['state']['delivery'], second['state']['errors'])
        old, new = first['state']['invocation'], second['state']['invocation']
        self.assertEqual(old['sid'], new['sid'])
        self.assertNotEqual(old['attempt_id'], new['attempt_id'])
        self.assertNotEqual(old['work_id'], new['work_id'])
        self.assertEqual(original_collection, (Path(old['runtime'])/'collection.json').read_bytes())
        manifest = read_json(Path(new['runtime'])/'manifest.json')
        self.assertEqual('author_revision_v1', manifest['role_transition']['kind'])
        self.assertEqual('success', second['state']['responsibility_analysis'][0]['result'])
        self.assertEqual('changed', second['state']['responsibility_analysis'][0]['next_action']['disposition'])
        request = (Path(new['runtime'])/'request.md').read_text()
        self.assertIn('A good method may remain valid', request)
        self.assertIn('Current-only evidence needs verification.', request)
        self.assertNotIn(repeated['text'], request)
        self.assertNotIn('Write repair-record.json with role=repair', request)
        self.assertNotIn(self.history.read_text(), request)
        first_request = (Path(old['runtime'])/'request.md').read_bytes()
        self.assertLess(len(request.encode()), len(first_request))
        self.assertIn('Write repair-record.json with role=repair',
                      (Path(new['runtime'])/'inputs/execution/revision-contract.md').read_text())
        compact = read_json(Path(new['runtime'])/'inputs/execution/revision-context.json')
        self.assertEqual([], compact['new_evidence'])
        self.assertNotIn('sha256', json.dumps(compact))
        self.assertEqual('execution/parent-revision-diff.json', compact['parent_difference'])
        self.assertTrue((Path(new['runtime'])/'inputs/execution/parent-revision-diff.json').is_file())
        self.assertEqual(len(self.parent['files']), len([row for row in manifest['inputs'] if row['target'].startswith('parent/')]))
        third_spec = self.continuation(second, 'revision-three')
        third = register_revision_task(self.store, third_spec, self.refs)
        third = run_revision_task(self.store, third['flow_id'], True)
        self.assertEqual('valid', third['state']['delivery'], third['state']['errors'])
        self.assertEqual(old['sid'], third['state']['invocation']['sid'])
        self.assertNotEqual(new['attempt_id'], third['state']['invocation']['attempt_id'])

    def test_author_continuation_keeps_changed_rendered_instructions_and_deduplicates_ancestry(self):
        stable = {'kind': 'operation', 'text': 'Retain the ordinary valid method.'}
        templated = {'kind': 'operation', 'text':
                     'Current revision label {{revised_candidate_id}} uses parent {{parent_root}}.'}
        added = {'kind': 'operation', 'text': 'Verify this newly assigned evidence association.'}
        spec = self.spec()
        spec['runner']['role_session']['mode'] = 'fresh_persistent'
        spec['runner']['request_sections'] = [stable, templated]
        first = register_revision_task(self.store, spec, self.refs)
        first = run_revision_task(self.store, first['flow_id'], True)
        previous = first
        for identity in ('rendered-second', 'rendered-third'):
            following = self.continuation(previous, identity)
            following['runner']['request_sections'] = [stable, templated, added]
            registered = register_revision_task(self.store, following, self.refs)
            current = run_revision_task(self.store, registered['flow_id'], True)
            self.assertEqual('valid', current['state']['delivery'], current['state']['errors'])
            invocation = current['state']['invocation']
            request = (Path(invocation['runtime']) / 'request.md').read_text()
            self.assertIn('Current revision label ' + following['revised_candidate_id']
                          + ' uses parent ' + invocation['parent_root'] + '.', request)
            self.assertNotIn(stable['text'], request)
            if previous is first:
                self.assertIn(added['text'], request)
            else:
                self.assertNotIn(added['text'], request)
            self.assertEqual(first['state']['invocation']['sid'], invocation['sid'])
            previous = current

    def test_author_continuation_rejects_wrong_author_unrelated_parent_and_ephemeral(self):
        spec = self.spec()
        spec['runner']['role_session']['mode'] = 'fresh_persistent'
        first = register_revision_task(self.store, spec, self.refs)
        first = run_revision_task(self.store, first['flow_id'], True)
        for change in ({'author_id':'other-author'}, {'candidate_id':'parent-v1','candidate_root':str(self.task)}):
            invalid = self.continuation(first)
            invalid.update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, 'author continuation'):
                register_revision_task(self.store, invalid, self.refs)
        fresh = self.spec(identity='ephemeral-first')
        created = register_revision_task(self.store, fresh, self.refs)
        completed = run_revision_task(self.store, created['flow_id'], True)
        with self.assertRaisesRegex(TaskflowError, 'author continuation'):
            register_revision_task(self.store, self.continuation(completed, 'invalid-ephemeral'), self.refs)

    def test_fresh_exceptions_cannot_relabel_persistent_or_unknown_author_as_ephemeral(self):
        spec = self.spec()
        spec['runner']['role_session']['mode'] = 'fresh_persistent'
        first = register_revision_task(self.store, spec, self.refs)
        first = run_revision_task(self.store, first['flow_id'], True)
        source = validate_revision_source(self.store, first['flow_id'])
        def fresh(identity):
            result = self.spec(identity=identity)
            result.update(candidate_id=source['candidate']['candidate_id'], candidate_root=str(source['root']),
                          session_reason='original_ephemeral', session_reason_detail='Claim the original did not persist.')
            return result
        with self.assertRaisesRegex(TaskflowError, 'persistent context cannot be relabeled'):
            register_revision_task(self.store, fresh('false-ephemeral'), self.refs)
        missing_detail = fresh('missing-exception-detail')
        missing_detail.pop('session_reason_detail')
        with self.assertRaisesRegex(TaskflowError, 'actual reason'):
            register_revision_task(self.store, missing_detail, self.refs)
        unknown = self.spec(identity='unknown-original')
        unknown.update(session_reason='original_ephemeral',session_reason_detail='No captured manifest exists.')
        with self.assertRaisesRegex(TaskflowError, 'verifiable captured original'):
            register_revision_task(self.store, unknown, self.refs)
        valid = fresh('unrecoverable-original')
        valid.update(session_reason='original_unrecoverable',session_reason_detail='The original carrier is unavailable for a documented operational reason.')
        register_revision_task(self.store, valid, self.refs)

    def test_actual_ephemeral_original_allows_a_documented_fresh_exception(self):
        first = self.register()
        first = run_revision_task(self.store, first['flow_id'], True)
        source = validate_revision_source(self.store, first['flow_id'])
        spec = self.spec(identity='fresh-after-ephemeral')
        spec.update(candidate_id=source['candidate']['candidate_id'], candidate_root=str(source['root']),
                    session_reason='original_ephemeral',session_reason_detail='The exact captured parent author used fresh_ephemeral.')
        registered = register_revision_task(self.store, spec, self.refs)
        self.assertEqual('original_ephemeral', registered['definition']['session_reason'])

    def test_external_author_cannot_be_presented_as_initial_author(self):
        sid = '00000000-0000-4000-8000-000000000099'
        trace = self.root/'external-author.jsonl'
        trace.write_text(json.dumps({'type':'thread.started','thread_id':sid})+'\n'+json.dumps({'type':'turn.completed'})+'\n')
        external = capture_candidate(self.store,self.task,'external-parent',
            external_author_sids=[sid],external_author_traces=[trace])
        spec = self.spec(identity='external-initial')
        spec['candidate_id'] = external['candidate_id']
        with self.assertRaisesRegex(TaskflowError, 'existing parent author'):
            register_revision_task(self.store,spec,self.refs)

    def test_seed_rejects_post_preparation_mutation_hardlinks_and_extra_members(self):
        for mode in ("bytes", "hardlink", "extra"):
            with self.subTest(mode=mode):
                record = self.register(identity="seed-" + mode)
                prepared = prepare_revision_task(self.store, record["flow_id"])
                current = prepared["state"]["invocation"]
                clone = Path(current["candidate_root"])
                path = clone / "draft/input/prompt.md"
                if mode == "bytes":path.write_text("Changed after preparation.", encoding="utf-8")
                elif mode == "hardlink":
                    path.unlink();os.link(self.task / "draft/input/prompt.md", path)
                else:(clone / "extra.md").write_text("Undeclared seed injection.", encoding="utf-8")
                with self.assertRaises((RunnerError, TaskflowError)):
                    _validate(Path(current["runtime"]), before_run=True)
                self.assertFalse((Path(current["runtime"]) / "termination.json").exists())
        self.assert_parent_unchanged()

    def test_seed_cannot_be_supplied_by_other_role_or_claim_foreign_locked_bytes(self):
        record = self.register()
        prepared = prepare_revision_task(self.store, record["flow_id"])
        current = prepared["state"]["invocation"]
        manifest = read_json(Path(current["runtime"]) / "manifest.json")
        args = (Path(current["work"]), manifest["output_trees"], manifest["seeded_output_trees"])
        with self.assertRaises(RunnerError):_check_seeded_outputs(*args, "solver", manifest["inputs"])
        wrong = copy.deepcopy(manifest["seeded_output_trees"])
        wrong["candidate"][0]["sha256"] = "0" * 64
        with self.assertRaises(RunnerError):_check_seeded_outputs(args[0], args[1], wrong, "repair", manifest["inputs"])
        missing = copy.deepcopy(manifest["seeded_output_trees"])
        missing["candidate"].pop()
        with self.assertRaises(RunnerError):_check_seeded_outputs(args[0], args[1], missing, "repair", manifest["inputs"])

    def test_post_run_diagnosis_preserves_missing_history_as_unknown(self):
        spec = self.spec()
        spec["diagnosis"] = {"mode": "post_run"}
        record = register_revision_task(self.store, spec, self.refs)
        inventory = {row['source_role']: row for row in record['definition']['diagnosis']['history_inventory']}
        self.assertEqual('provided', inventory['answer']['status'])
        self.assertEqual('unknown', inventory['facts']['status'])
        self.assertEqual('unknown', inventory['review']['status'])
        self.assertEqual([], self.store.dashboard()['work'])
        spec = self.spec(identity='complete-post-run')
        spec['diagnosis'] = {'mode':'post_run'}
        spec["sources"] = [{**spec["sources"][0], "source_role": role, "target": "history/" + role + ".txt"}
                           for role in ("answer", "facts", "review")]
        record = register_revision_task(self.store, spec, self.refs)
        self.assertEqual("post_run", record["definition"]["diagnosis"]["mode"])

    def test_invalid_explicit_specs_are_rejected_before_queueing(self):
        spec = self.spec()
        with self.assertRaisesRegex(TaskflowError, "evidence: non-empty list"):
            register_revision_task(self.store, spec, [])
        cases = [
            {"schema_version": True}, {"revised_candidate_id": "parent-v1"}, {"directory": "relative"},
            {"purpose": ""}, {"expected_changes": None}, {"expected_changes": [None]},
            {"expected_changes": [{"path": "draft/input/prompt.md", "reason": ""}]},
            {"responsibilities": []}, {"responsibilities": [None]},
            {"responsibilities": [{"id": "R1", "description": "", "paths": ["task.json"]}]},
            {"responsibilities": [{"id": "R1", "description": "Actual responsibility.", "paths": []}]},
            {"diagnosis": {"mode": "guessed"}}, {"runner": {"mode": "old_author"}},
            {"defect_ids": [{}]}, {"defect_ids": ["R1"]},
            {"defect_repairs": [{"defect_id": "R1", "disposition": "private_repair"}]},
        ]
        for update in cases:
            with self.subTest(update=update):
                invalid = copy.deepcopy(spec);invalid.update(update)
                with self.assertRaises(TaskflowError):register_revision_task(self.store, invalid, self.refs)
        for update in ({"inputs": [{"path": "private"}]}, {"resume_sid": "old"},
                       {"request_sections": [{"kind": "teacher"}]},
                       {"role_session": {"mode": "resume_existing", "home": str(self.root / "home"), "previous": None}},
                       {"role_session": {"mode": "fresh_ephemeral", "home": str(self.task), "previous": None}}):
            with self.subTest(runner_update=update):
                invalid = copy.deepcopy(spec);invalid["runner"].update(update)
                with self.assertRaises(TaskflowError):register_revision_task(self.store, invalid, self.refs)
        for source_update in ({"target": "elsewhere/source"}, {"source_role": "guessed"}, {"size": 1}):
            with self.subTest(source_update=source_update):
                invalid = copy.deepcopy(spec);invalid["sources"][0].update(source_update)
                with self.assertRaises(TaskflowError):register_revision_task(self.store, invalid, self.refs)
        self.assertEqual([], self.store.dashboard()["work"])

    def test_original_definition_and_diff_cannot_be_overwritten_or_promoted(self):
        spec = self.spec()
        record = register_revision_task(self.store, spec, self.refs)
        changed = copy.deepcopy(spec);changed["purpose"] = "Another business purpose."
        with self.assertRaises(TaskflowError):register_revision_task(self.store, changed, self.refs)
        with self.assertRaises(TaskflowError):validate_revision_source(self.store, record["flow_id"])
        result = run_revision_task(self.store, record["flow_id"], True)
        path = Path(result["state"]["diff"]["path"])
        path.write_text('{"changes": []}', encoding="utf-8")
        with self.assertRaises(TaskflowError):validate_revision_source(self.store, record["flow_id"])

    def test_explicit_open_defect_is_repaired_once_and_never_self_closed(self):
        from tests.test_execution_quality import ExplicitQualityTests
        from taskflow.execution_quality import run_quality_task

        prior = ExplicitQualityTests()
        prior.setUp()
        self.addCleanup(prior.doCleanups)
        quality = prior.register("revision_required", corrections=0)
        quality = run_quality_task(prior.store, quality["flow_id"], True)
        identity = quality["state"]["invocations"][0]["defect_ids"][0]
        self.root, self.store, self.task = prior.root, prior.store, prior.task
        self.script = self.root / "revision-role.py"
        self.script.write_text(ROLE, encoding="utf-8")
        self.history = self.root / "history.txt"
        self.history.write_text("Complete prior answer/facts/review diagnostic original.", encoding="utf-8")
        spec = self.spec("public")
        spec["candidate_id"] = prior.candidate["candidate_id"]
        spec["responsibilities"][0]["id"] = identity
        spec.update(defect_ids=[identity], defect_repairs=[{"defect_id": identity, "disposition": "material_repair"}])
        registered = register_revision_task(prior.store, spec, prior.refs)
        result = run_revision_task(prior.store, registered["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        defect = prior.store.get_defect(identity)
        self.assertEqual("repaired", defect["state"])
        self.assertEqual(result["state"]["candidate_id"], defect["repair_candidate_id"])
        self.assertEqual(defect["repair_seq"], result["state"]["defect_repairs"][0]["repair_seq"])
        repeated = collect_revision_task(prior.store, registered["flow_id"])
        self.assertEqual(defect, prior.store.get_defect(identity))
        self.assertEqual(registered["definition"], register_revision_task(prior.store, spec, prior.refs)["definition"])
        source = validate_revision_source(prior.store, repeated["flow_id"])
        self.assertEqual(defect["author_attempt"], source["attempt"]["attempt_id"])
        with self.assertRaisesRegex(TaskflowError, "unclosed|independent quality"):
            prior.store.quality_gate(result["state"]["candidate_id"])

    def test_explicit_rubric_defect_accepts_actual_scoring_declaration_only_repair(self):
        from tests.test_execution_quality import ExplicitQualityTests
        from taskflow.execution_quality import run_quality_task

        prior = ExplicitQualityTests()
        prior.setUp()
        self.addCleanup(prior.doCleanups)
        quality = prior.register("accepted", corrections=0)
        quality = run_quality_task(prior.store, quality["flow_id"], True)
        current = quality["state"]["invocations"][0]
        identity = "prospective-criterion-omission"
        prior.store.open_defect(identity, prior.candidate["candidate_id"], current["review_id"],
            responsibility="Clarify a prospective criterion while preserving old original grades.",
            evidence=[evidence_ref(prior.task / "task.json", "scoring.criteria[0].condition")], kind="rubric")
        self.root, self.store, self.task = prior.root, prior.store, prior.task
        self.script = self.root / "revision-role.py"
        self.script.write_text(ROLE, encoding="utf-8")
        self.history = Path(prior.public["runtime_path"]) / "artifacts/delivery.txt"
        spec = self.spec("declaration")
        spec["candidate_id"] = prior.candidate["candidate_id"]
        spec["responsibilities"][0]["id"] = identity
        spec.update(defect_ids=[identity], defect_repairs=[{"defect_id": identity, "disposition": "private_repair"}],
                    diagnosis={"mode": "post_run"})
        original_review = Path(current["runtime"]) / "artifacts/adoption-readiness.json"
        original_facts = Path(current["runtime"]) / "artifacts/public-responsibility-first.json"
        spec["sources"] = [{**evidence_ref(path, "Complete neutral fixture " + role + " original"),
                            "target": "history/" + role + ".txt", "source_role": role}
                           for role, path in (("answer", self.history), ("facts", original_facts), ("review", original_review))]
        old = original_review.read_bytes()
        registered = register_revision_task(prior.store, spec, prior.refs)
        result = run_revision_task(prior.store, registered["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        source = validate_revision_source(prior.store, result["flow_id"])
        self.assertEqual([], source["diff"]["changes"])
        defect = prior.store.get_defect(identity)
        self.assertEqual("repaired", defect["state"])
        self.assertIsNotNone(defect.get("changed_declaration"))
        self.assertEqual([], defect["changed_files"])
        self.assertEqual(old, original_review.read_bytes())


if __name__ == "__main__":
    unittest.main()
