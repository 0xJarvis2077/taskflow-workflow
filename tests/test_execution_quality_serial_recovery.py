"""Neutral whole serial intake through interrupted successor preparation."""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution import run_worker
from taskflow.execution_flow import advance_workflow, resume_workflow
from taskflow.execution_quality_session import capability_signature, validate_session
from taskflow.execution_store import evidence_ref
from tests import test_execution_quality_public_progress as fixtures


class SerialRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SerialResumeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.store, self.fid = self.fixture.store, self.fixture.fid
        self.serial = self.fixture.fixture
        (self.serial.sessions / "public-observations").touch()

    def receive_original(self):
        stopped, attempt = self.fixture.completed_exception()
        root = Path(attempt["runtime_path"])
        preserved = {name: (root / name).read_bytes() for name in (
            "manifest.json", "manifest-lock.json", "collection.json", "trace.jsonl",
            "artifacts/reading-progress-000.json")}
        count = len(self.store.dashboard()["attempts"])
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("no launch")):
            self.fixture.resume()
            received = advance_workflow(self.store, self.fid)
        self.assertEqual(1, received["state"]["serial_quality_reading"]["index"])
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))
        self.assertEqual(attempt["attempt_id"], received["state"]["serial_quality_reading"]["turns"][0]["attempt_id"])
        progress = json.loads(preserved["artifacts/reading-progress-000.json"])
        self.assertTrue(progress["pages"])
        self.assertIn("path", progress["pages"][0])
        self.assertNotIn("id", progress["pages"][0])
        self.assertFalse(received["state"]["serial_quality_reading"]["turns"][0]["quality_accepted"])
        return attempt, root, preserved, count

    def prepare_successor_after_interruption(self):
        attempt, original, preserved, count = self.receive_original()
        real_validate = validate_session

        def interrupted(value, **kwargs):
            if kwargs.get("root") is not None and value["quality_session"]["index"] == 1:
                raise TaskflowError("Synthetic interruption before manifest publication")
            return real_validate(value, **kwargs)

        with patch("taskflow.execution_quality_session.validate_session", side_effect=interrupted):
            advance_workflow(self.store, self.fid)
        stopped = advance_workflow(self.store, self.fid)
        self.assertEqual("exception", stopped["state"]["status"])
        self.assertIsNone(stopped["state"]["current_work_id"])
        runtime = original.parent.parent / "001/runtime"
        self.assertTrue((runtime / "request.md").exists())
        self.assertFalse((runtime / "manifest.json").exists())
        partial = (runtime / "request.md").read_bytes()
        evidence = self.serial.root / "technical-repair.json"
        write_json(evidence, {"kind": "synthetic_partial_prepare_repair"})
        resolution = dict(reason="Neutral partial preparation repaired", evidence=[evidence_ref(evidence, "neutral recovery")])
        resume_workflow(self.store, self.fid, **resolution)
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("no dispatch during prepare")):
            prepared = advance_workflow(self.store, self.fid)
        self.assertEqual("active", prepared["state"]["status"], prepared["state"].get("exception"))
        self.assertEqual(str(runtime), prepared["state"]["current_runtime"])
        work_id = prepared["state"]["current_work_id"]
        self.assertEqual([], self.store.list_attempts(work_id))
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))
        manifest = json.loads((runtime / "manifest.json").read_text())
        self.assertEqual(attempt["cli_sid"], manifest["resume_sid"])
        archives = list(runtime.with_name("runtime.preparation-originals").glob("*/original"))
        self.assertEqual(1, len(archives))
        self.assertEqual(partial, (archives[0] / "request.md").read_bytes())
        entry = prepared["state"]["serial_quality_reading"]["turns"][0]
        seal = Path(entry["seal"]["path"])
        seal_bytes = seal.read_bytes()
        again = advance_workflow(self.store, self.fid)
        self.assertEqual(work_id, again["state"]["current_work_id"])
        self.assertEqual(seal_bytes, seal.read_bytes())
        self.assertEqual(1, len(list(runtime.with_name("runtime.preparation-originals").glob("*/original"))))
        for name, data in preserved.items():
            self.assertEqual(data, (original / name).read_bytes(), name)
        return prepared, attempt, count

    def test_original_public_progress_to_same_sid_successor_without_replay(self):
        prepared, original, count = self.prepare_successor_after_interruption()
        work = prepared["state"]["current_work_id"]
        successor = run_worker(self.store, "synthetic-reader", True, [work])
        self.assertEqual("completed", successor["state"])
        self.assertEqual(original["cli_sid"], successor["cli_sid"])
        advance_workflow(self.store, self.fid)
        received = advance_workflow(self.store, self.fid)
        self.assertEqual(2, received["state"]["serial_quality_reading"]["index"])
        self.assertEqual(count + 1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(1, len(self.store.list_attempts(original["work_id"])))

    def test_private_page_omission_after_recovery_never_becomes_acceptance(self):
        prepared, original, count = self.prepare_successor_after_interruption()
        (self.serial.sessions / "omit-page").touch()
        successor = run_worker(self.store, "synthetic-reader", True, [prepared["state"]["current_work_id"]])
        self.assertEqual("completed", successor["state"])
        advance_workflow(self.store, self.fid)
        stopped = advance_workflow(self.store, self.fid)
        self.assertEqual("exception", stopped["state"]["status"])
        self.assertIn("every assigned page", stopped["state"]["exception"]["reason"])
        self.assertEqual(1, stopped["state"]["serial_quality_reading"]["index"])
        self.assertEqual(count + 1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(1, len(self.store.list_attempts(original["work_id"])))


class EquivalentCapabilityPathTests(unittest.TestCase):
    def manifest(self, root):
        from tools.isolated_quality import inline_table
        filesystem = {"/middle-shared": "read", str(root / "inputs"): "read"}
        declaration = "permissions.neutral.filesystem=" + inline_table(filesystem)
        return {"mode": "native_role", "argv": ["/neutral/cli", "-c", declaration, "exec"],
                "model": {"requested": "neutral-model", "reasoning_effort": "low"},
                "native": {"profile": "neutral", "filesystem": filesystem,
                           "sandbox_argv": ["/neutral/cli", "-c", declaration]}}

    def test_input_placeholder_precedes_sorting_without_changing_original_signature(self):
        left_root, right_root = Path("/alpha/turn"), Path("/zulu/turn")
        left, right = self.manifest(left_root), self.manifest(right_root)
        right["native"]["provider"] = None
        before = copy.deepcopy(left)
        raw = capability_signature(left, left_root)
        self.assertEqual(capability_signature(left, left_root, normalize_representation=True),
                         capability_signature(right, right_root, normalize_representation=True))
        self.assertEqual(before, left)
        self.assertEqual(raw, capability_signature(left, left_root))
        expected = capability_signature(left, left_root, normalize_representation=True)
        for field, value in (("cwd", "/changed/work"), ("policy_version", "different-policy")):
            changed = copy.deepcopy(right); changed[field] = value
            self.assertNotEqual(expected, capability_signature(changed, right_root, normalize_representation=True))
        changed = copy.deepcopy(right); changed["model"]["requested"] = "other-model"
        self.assertNotEqual(expected, capability_signature(changed, right_root, normalize_representation=True))
        changed = copy.deepcopy(right); changed["argv"][0] = "/other/cli"
        self.assertNotEqual(expected, capability_signature(changed, right_root, normalize_representation=True))
        changed = copy.deepcopy(right); changed["model"]["reasoning_effort"] = "high"
        self.assertNotEqual(expected, capability_signature(changed, right_root, normalize_representation=True))
        changed = copy.deepcopy(right); changed["native"]["provider"] = {"name": "other"}
        self.assertNotEqual(expected, capability_signature(changed, right_root, normalize_representation=True))
        changed = copy.deepcopy(right); changed["native"]["filesystem"]["/middle-shared"] = "write"
        with self.assertRaises(TaskflowError):
            capability_signature(changed, right_root, normalize_representation=True)



class StructuredProgressTests(unittest.TestCase):
    def setUp(self):
        from taskflow import execution_quality_progress_contract as contract
        original_context = contract.make_context
        def legacy_context(*args, **kwargs):
            kwargs["legacy"] = True
            return original_context(*args, **kwargs)
        # These fixtures exercise already-frozen rich receipts, rather than
        # granting the new canonical preflight permission to accept aliases.
        context_patch = patch.object(contract, "make_context", side_effect=legacy_context)
        context_patch.start()
        self.addCleanup(context_patch.stop)
        from tests import test_execution_quality_reading as reading_fixtures
        prefix = reading_fixtures.SERIAL_PREFIX
        marker = "  name='reading-progress-'"
        body = r"""  if serial['index']:
   for entry in dispositions:
    page=pages[entry['id']]
    entry['evidence']=[{'plan_page':page['path'],'queue_record':str(pathlib.Path(STORE_PATH).parent/'neutral-queue.jsonl')+':1','queue_and_navigation_visible_tool_chunk':'synthetic-current','page_hash_verified_tool_chunk':'synthetic-current'}, {'resolved_original_sources':[{'ref':ref,'original':page['path']} for ref in page['source_refs']]}, {'body_read':'Synthetic complete assigned page body response'}]
   first=pathlib.Path(serial['previous']['path']).parent/'artifacts/reading-progress-000.json'
   checkpoint=json.loads(first.read_text())['public_checkpoint']
  with original.open('a') as out:
   out.write(json.dumps({'type':'response_item','payload':{'type':'custom_tool_call_output','output':[{'type':'input_text','text':json.dumps({'chunk_id':'synthetic-prior' if serial['index']==0 else 'synthetic-current','exit_code':0,'output':'Synthetic tool response, never real model evidence.'})}]}})+'\n')
"""
        with patch.object(reading_fixtures, "SERIAL_PREFIX", prefix.replace(marker, body + marker)):
            self.fixture = fixtures.SerialResumeTests()
            self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.serial = self.fixture.fixture
        self.store, self.fid = self.fixture.store, self.fixture.fid
        page = self.serial.plan['pages'][0]
        queue = self.serial.root / 'neutral-queue.jsonl'
        write_json(queue, {'id':page['id'],'page':Path(page['path']).name,
            'sha256':page['sha256'],'bytes':page['size'],'source_refs':page['source_refs']})
        # Queue is one JSON line, just like the locked navigation transport.
        queue.write_text(json.dumps(json.loads(queue.read_text())) + '\n')
        self.serial.plan['source_refs'].append(evidence_ref(queue, 'synthetic bound queue'))
        write_json(self.serial.plan_path, self.serial.plan)
        self.serial.packet['serial_review_plan'] = evidence_ref(self.serial.plan_path, 'synthetic bound serial plan')
        (self.serial.sessions / 'public-observations').touch()
        first = self.serial.first_progress()
        self.assertEqual(1, first['state']['serial_quality_reading']['index'])
        prepared = advance_workflow(self.store, self.fid)
        attempt = run_worker(self.store, 'synthetic-structured-reader', True,
                             [prepared['state']['current_work_id']])
        self.assertEqual('completed', attempt['state'])
        self.runtime = Path(attempt['runtime_path'])
        self.manifest = json.loads((self.runtime / 'manifest.json').read_text())
        self.artifact = self.runtime / 'artifacts/reading-progress-001.json'
        self.original = self.artifact.read_bytes()

    def parse(self, result):
        from taskflow.execution_quality_session import _progress
        from taskflow.execution_runner import _info
        try:
            write_json(self.artifact, result)
            return _progress(self.runtime, self.manifest, self.serial.plan,
                             {'artifacts':{'reading-progress-001.json':_info(self.artifact)}})
        finally:
            self.artifact.write_bytes(self.original)

    def test_locked_structured_observations_and_exact_first_checkpoint_receive_unchanged(self):
        result = json.loads(self.original)
        self.assertEqual(result, self.parse(result))
        before = len(self.store.dashboard()['attempts'])
        advance_workflow(self.store, self.fid)
        received = advance_workflow(self.store, self.fid)
        self.assertEqual(2, received['state']['serial_quality_reading']['index'])
        self.assertEqual(before, len(self.store.dashboard()['attempts']))
        self.assertEqual(self.original, self.artifact.read_bytes())
        self.assertFalse(received['state']['serial_quality_reading']['turns'][-1]['quality_accepted'])

    def test_same_byte_claim_requires_prior_owned_output_not_later_echo(self):
        result = json.loads(self.original)
        page = result['pages'][0]
        page['disposition'] = 'same_bytes_this_sid'
        page['evidence'].append({'prior_public_original':'synthetic prior public source',
                                'prior_actual_full_body_tool_chunk':'synthetic-prior'})
        self.assertEqual(result, self.parse(result))
        page['evidence'][-1]['prior_actual_full_body_tool_chunk'] = 'synthetic-current'
        with self.assertRaisesRegex(TaskflowError, 'prior same-SID readback'):
            self.parse(result)

    def test_missing_source_identity_tool_anchor_association_and_changed_checkpoint_reject(self):
        original = json.loads(self.original)
        cases = []
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['plan_page'] = '/foreign'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['queue_record'] = '/foreign:1'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['queue_record'] = changed['pages'][0]['evidence'][0]['queue_record'].rsplit(':',1)[0]+':99'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['page_hash_verified_tool_chunk'] = 'invented'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][1]['resolved_original_sources'] = []; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][-1] = {'body_read':''}; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['association'] = ''; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'] = []; cases.append(changed)
        changed = copy.deepcopy(original); changed['public_checkpoint']['tool_readback_location'] = 'changed'; cases.append(changed)
        for index, changed in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                self.parse(changed)

    def test_command_echo_cannot_supply_an_owned_tool_readback(self):
        from taskflow.execution_quality_session import _visible_readback_chunks
        path = self.serial.root / 'synthetic-output-locators.jsonl'
        echo = {'chunk_id':'echo','exit_code':0,'output':'fake'}
        rows = [{'type':'response_item','payload':{'type':'custom_tool_call','input':json.dumps(echo)}},
                {'type':'response_item','payload':{'type':'custom_tool_call_output','output':[
                    {'type':'input_text','text':json.dumps({'chunk_id':'actual','exit_code':0,'output':json.dumps(echo)})}]}}]
        path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        self.assertEqual({'actual'}, _visible_readback_chunks(path))

    def split_result(self):
        result = json.loads(self.original)
        expected = self.serial.plan['pages'][0]
        old = result['pages'][0]['evidence']
        result['pages'][0]['evidence'] = [
            {'body_page':old[0]['plan_page'], 'bytes':expected['size'],
             'actual_individual_body_read_exec_command_chunk':'synthetic-current'},
            {'plan_turn':'turns[1]', 'queue_record':old[0]['queue_record'],
             'navigation_queue_visible_read_chunk':'synthetic-current',
             'source_records':old[1]['resolved_original_sources'],
             'bindings_sha_check':'synthetic-current'}]
        return result

    def test_split_body_source_locators_bind_originals_without_rewriting(self):
        result = self.split_result()
        self.assertEqual(result, self.parse(result))
        self.assertEqual(self.original, self.artifact.read_bytes())
        # The producer's canonical format accepts additional observations.
        canonical = copy.deepcopy(result)
        canonical['pages'][0]['evidence'] = [{'path':self.serial.plan['pages'][0]['path'],
            'location':'Synthetic actual readback locator', 'observations':result['pages'][0]['evidence']}]
        self.assertEqual(canonical, self.parse(canonical))

    def test_split_false_body_read_size_turn_and_source_bindings_are_rejected(self):
        original = self.split_result()
        cases = []
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['bytes'] += 1; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['bytes'] = True; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][0]['actual_individual_body_read_exec_command_chunk'] = 'echo-only'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][1]['plan_turn'] = 'turns[9]'; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'][1]['source_records'] = []; cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'].append({'plan_page':self.serial.plan['pages'][0]['path']}); cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'].append(copy.deepcopy(changed['pages'][0]['evidence'][0])); cases.append(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['evidence'] = [{'arbitrary':{'nonempty':'not proof'}}]; cases.append(changed)
        for index, changed in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                self.parse(changed)

    def test_future_producer_explicit_schema_is_consumable_and_first_request_is_preserved(self):
        from taskflow.execution_runner import _info
        request = (self.runtime / 'request.md').read_text()
        self.assertIn('each with nonempty string fields "path" and "location"', request)
        self.assertIn('public_checkpoint must be null', request)
        self.assertIn('Before delivery, check every evidence record', request)
        previous = Path(self.manifest['quality_session']['previous']['path']).parent
        first_manifest = json.loads((previous / 'manifest.json').read_text())
        self.assertEqual(first_manifest['request'], _info(previous / 'request.md'))
        self.assertNotIn('Neutral evidence item schema', (previous / 'request.md').read_text())
        result = json.loads(self.original)
        result['public_checkpoint'] = None
        result['pages'][0]['evidence'] = [{'path':self.serial.plan['pages'][0]['path'],
                                         'location':'Synthetic actual visible source response'}]
        self.assertEqual(result, self.parse(result))
        for key in ('path','location'):
            changed = copy.deepcopy(result)
            changed['pages'][0]['evidence'][0][key] = ''
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                self.parse(changed)

    def test_orchestrated_body_locator_requires_owned_response_and_successful_bound_full_output(self):
        from taskflow.execution_quality_session import _split_body_output
        root = self.serial.root / 'synthetic-body-witness'
        root.mkdir()
        expected = self.serial.plan['pages'][0]
        body = Path(expected['path']).read_text()
        manifest = {'quality_session':{'plan':{'path':str(self.serial.plan_path)}}}
        page = {'id':expected['id']}
        header = f"PAGE {page['id']} chunk displayed exit 0"
        response = {'type':'response_item','payload':{'type':'custom_tool_call_output',
            'output':[{'type':'input_text','text':header},{'type':'input_text','text':body}]}}
        success = {'type':'item.completed','item':{'type':'command_execution','exit_code':0,
            'command':'read ' + expected['path'],'aggregated_output':body}}
        def write(response_row, trace_row):
            (root / 'quality-session-observed.jsonl').write_text(json.dumps(response_row)+'\n')
            (root / 'trace.jsonl').write_text(json.dumps(trace_row)+'\n')
        write(response, success)
        self.assertTrue(_split_body_output(root, manifest, page, expected, 'displayed'))
        by_plan = copy.deepcopy(success)
        by_plan['item']['command'] = str(self.serial.plan_path) + ' ["pages"][0]'
        write(response, by_plan)
        self.assertTrue(_split_body_output(root, manifest, page, expected, 'displayed'))
        cases = []
        changed = copy.deepcopy(success); changed['item']['exit_code'] = 1; cases.append((response,changed))
        changed = copy.deepcopy(success); changed['item']['exit_code'] = False; cases.append((response,changed))
        changed = copy.deepcopy(success); changed['item']['command'] = 'echo unrelated'; cases.append((response,changed))
        changed = copy.deepcopy(success); changed['item']['aggregated_output'] = 'missing body'; cases.append((response,changed))
        changed = copy.deepcopy(response); changed['payload']['type'] = 'custom_tool_call'; cases.append((changed,success))
        changed = copy.deepcopy(response); changed['payload']['output'][1]['text'] = 'metadata only'; cases.append((changed,success))
        for index, (response_row, trace_row) in enumerate(cases):
            write(response_row, trace_row)
            with self.subTest(index=index):
                self.assertFalse(_split_body_output(root, manifest, page, expected, 'displayed'))
        self.assertFalse(_split_body_output(root, manifest, page, expected, None))

    def test_page_remaining_lists_preserve_all_duties_without_resolving_them(self):
        original = json.loads(self.original)
        duties = ["Inspect one unresolved source responsibility.",
                  "Reconcile a distinct final-product relation."]
        for remaining in (duties, [duties[0]], [], ""):
            result = copy.deepcopy(original)
            result['pages'][0]['remaining'] = remaining
            self.assertEqual(result, self.parse(result))
            self.assertEqual(remaining, self.parse(result)['pages'][0]['remaining'])
        unresolved = copy.deepcopy(original)
        unresolved['pages'][0].update(disposition='unresolved', remaining=duties)
        self.assertEqual(duties, self.parse(unresolved)['pages'][0]['remaining'])
        for value in ([], "", " ", [""], [" "]):
            changed = copy.deepcopy(unresolved); changed['pages'][0]['remaining'] = value
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                self.parse(changed)
        self.assertEqual(self.original, self.artifact.read_bytes())

    def test_page_remaining_rejects_objects_nested_lists_and_missing_identity(self):
        original = json.loads(self.original)
        for value in (None, 1, False, {}, [None], [1], [{}], [["nested"]], ["real duty", False]):
            changed = copy.deepcopy(original); changed['pages'][0]['remaining'] = value
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                self.parse(changed)
        changed = copy.deepcopy(original); changed['pages'][0]['remaining'] = ["Still open"]
        changed['pages'][0]['sha256'] = 'wrong'
        with self.assertRaises(TaskflowError): self.parse(changed)
        changed = copy.deepcopy(original); del changed['pages'][0]['remaining']
        with self.assertRaises(TaskflowError): self.parse(changed)

    def test_future_page_schema_distinguishes_page_and_root_remaining_without_rewriting_first(self):
        from taskflow.execution_runner import _info
        request = (self.runtime / 'request.md').read_text()
        self.assertIn("Each page's remaining must be a string", request)
        self.assertIn('The separate top-level remaining remains a list', request)
        self.assertIn('top-level facts remains a list', request)
        self.assertIn('Neutral complete page record (placeholders only)', request)
        self.assertIn("check every page's complete fields and types", request)
        previous = Path(self.manifest['quality_session']['previous']['path']).parent
        first_manifest = json.loads((previous / 'manifest.json').read_text())
        self.assertEqual(first_manifest['request'], _info(previous / 'request.md'))
        self.assertNotIn('Neutral complete page record', (previous / 'request.md').read_text())

if __name__ == "__main__":
    unittest.main()
