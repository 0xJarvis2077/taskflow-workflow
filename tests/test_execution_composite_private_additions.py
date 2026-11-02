"""Actual private repair evidence additions keep exact declaration provenance."""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_flow_contracts import review_metadata_aliases
from taskflow.execution_runner import _validate, prepare_attempt
from tests import test_execution_composite_quality as composite_fixtures
from tests import test_execution_flow as flow_fixtures


def make_private_addition_fixture(testcase, *, role="teacher", association=True, explicit_old=True, scoring_change=False):
    case = composite_fixtures.CompositeQualityTests("test_exact_private_repair_composes_without_new_whole_signature")
    testcase.addCleanup(case.doCleanups)
    task_factory = flow_fixtures.make_task

    def declared_hashes(*args, **kwargs):
        root = task_factory(*args, **kwargs)
        declaration = read_json(root / "task.json")
        for row in declaration["files"]:
            raw = (root / row["path"]).read_bytes()
            row.update(sha256=sha256(raw), bytes=len(raw))
        write_json(root / "task.json", declaration)
        return root

    wrapper = """
_original_signed_report=signed_report
def signed_report(route,ctx,inputs):
 report=_original_signed_report(route,ctx,inputs)
 if route['scope']=='affected':
  name='draft/evaluation/controls/repair-proof.json';new=hashlib.sha256((inputs/name).read_bytes()).hexdigest()
  report['defect_resolution']['evidence'].append({'path':name,'sha256':new,'location':'all current neutral repair control observations'})
  if ASSOCIATION:
   report['defect_resolution']['affected_paths'].append(name)
   row={'path':name,'new':new}
   if EXPLICIT_OLD:row['old']=None
   report['coverage']['changed_files'].append(row)
 return report
""".replace("ASSOCIATION", repr(association)).replace("EXPLICIT_OLD", repr(explicit_old))
    addition = """
  added='draft/evaluation/controls/repair-proof.json';save(root/added,{'observations':['actual neutral repair control evidence']})
  declaration=json.loads((root/'task.json').read_text())
  declaration['files'].append({'id':'repair-proof','path':added,'role':ROLE,'references':['S1']})
  for row in declaration['files']:
   raw=(root/row['path']).read_bytes();row.update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
  if SCORING_CHANGE:declaration['scoring']['criteria'][0]['condition']='A changed rule must not inherit old whole quality.'
  save(root/'task.json',declaration)
  record=json.loads(pathlib.Path('repair-record.json').read_text());record['changes'].append({'path':added,'before_sha256':None,'after_sha256':digest(root/added)});save('repair-record.json',record)
""".replace("ROLE", repr(role)).replace("SCORING_CHANGE", repr(scoring_change))
    addition = "\n".join(line[1:] if line.startswith(" ") else line for line in addition.splitlines()) + "\n"
    script = wrapper + flow_fixtures.ROLE_FIXTURE
    script = script.replace(" pathlib.Path('handoff.md').write_text", addition + " pathlib.Path('handoff.md').write_text")
    script = script.replace(" route={'schema_version'", " if a.outcome=='revision_required':defects.append({'id':'META-OBS','kind':'claim','blocking':False,'responsibility':'Current declaration metadata observation only.','affected_paths':[name],'evidence':[{'path':'execution/task.json','sha256':digest(inputs/'execution/task.json'),'location':'actual declared scoring and file metadata'}]})\n route={'schema_version'")
    with patch.object(flow_fixtures, "make_task", side_effect=declared_hashes), patch.object(flow_fixtures, "ROLE_FIXTURE", script):
        return case.make_closed_flow()


class CompositePrivateAdditionTests(unittest.TestCase):
    def compose(self, value):
        return value["store"].compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
                                                       value["defects"], evidence=value["refs"])

    def test_metadata_alias_and_added_collected_validation_preserve_all_signed_sources(self):
        value = make_private_addition_fixture(self)
        result = self.compose(value)
        delta = result["composite_quality_coverage"]["declaration_delta"]
        self.assertEqual(1, len(delta["added_files"]))
        self.assertEqual(2, len(delta["file_metadata_updates"]))
        self.assertNotEqual(delta["original"]["sha256"], delta["current"]["sha256"])
        change = next(row for row in result["composite_quality_coverage"]["private_changes"] if row["old_sha256"] is None)
        self.assertTrue(change["fresh_signed_evidence"])
        self.assertEqual("teacher", change["role"])
        self.assertFalse(result["whole_package"])
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))

    def test_added_standard_scoring_change_or_missing_explicit_association_is_rejected(self):
        for options, reason in (({"role": "answer"}, "added private validation"),
                                ({"scoring_change": True}, "changed package/scoring metadata"),
                                ({"association": False}, "outside original issues"),
                                ({"explicit_old": False}, "outside original issues")):
            value = make_private_addition_fixture(self, **options)
            with self.subTest(options=options), self.assertRaisesRegex(TaskflowError, reason):
                self.compose(value)

    def test_parent_metadata_alias_never_substitutes_child_declaration_or_wrong_sha(self):
        value = make_private_addition_fixture(self)
        store = value["store"]
        whole = store.get_review(value["whole"])
        candidate = store.get_candidate(whole["candidate_id"])
        attempt = store.get_attempt(whole["attempt_id"])
        root = Path(attempt["runtime_path"])
        manifest = _validate(root)
        alias = review_metadata_aliases(candidate, manifest, root)
        original = next(row for row in candidate["evidence"] if row.get("location") == "explicit package declaration")
        self.assertEqual(original["sha256"], alias["execution/task.json"]["sha256"])
        with self.assertRaisesRegex(TaskflowError, "different candidate or intent"):
            review_metadata_aliases({**candidate, "input_digest": "0" * 64}, manifest, root)
        self.assertNotEqual(alias["execution/task.json"]["sha256"], next(row["sha256"] for row in
            store.get_candidate(value["candidate"])["evidence"] if row.get("location") == "explicit package declaration"))
        wrong = copy.deepcopy(candidate)
        next(row for row in wrong["evidence"] if row.get("location") == "explicit package declaration")["sha256"] = "0" * 64
        with self.assertRaisesRegex(TaskflowError, "source/staged bytes"):
            review_metadata_aliases(wrong, manifest, root)
        foreign = copy.deepcopy(candidate)
        next(row for row in foreign["evidence"] if row.get("location") == "explicit package declaration")["path"] = str(root / "foreign.json")
        with self.assertRaisesRegex(TaskflowError, "foreign source"):
            review_metadata_aliases(foreign, manifest, root)
        binding = {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
        for label, inputs in (("absent", []), ("wrong-kind", [{"path": original["path"], "kind": "teacher", "target": "execution/task.json"}])):
            runtime, cwd = value["case"].root / label, value["case"].root / (label + "-work")
            cwd.mkdir()
            prepare_attempt(runtime, {"intent_id": label, "policy_version": "fixture-v1", "role": "quality",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "candidate_binding": binding, "cwd": str(cwd), "argv": [sys.executable, "-c", "pass"],
                "inputs": inputs, "request_sections": [], "outputs": ["unused.json"]})
            prepared = _validate(runtime)
            if label == "absent":
                self.assertEqual({}, review_metadata_aliases(candidate, prepared, runtime))
            else:
                with self.assertRaisesRegex(TaskflowError, "missing or ambiguous"):
                    review_metadata_aliases(candidate, prepared, runtime)

