"""Explicit rubric-role transport uses sealed neutral authors, never models."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import _candidate_structure, advance_author
from taskflow.execution_store import evidence_ref
from taskflow.packages import check_task
from tests import test_execution_author as author_fixture
from tests import test_execution_optional_candidate_delivery as fixture


DECLARATION = """
 value=json.loads((root/'task.json').read_text())
 criteria=value['scoring']['criteria']
 second=dict(criteria[0]);second.update(id='C2',max_score=4,condition='Second neutral judgment.')
 criteria[0]['max_score']=3
 criteria.append(second)
 for item in criteria:item['public_evidence']=['draft/input/prompt.md']
 row=next(item for item in value['files'] if item['id']==value['scoring']['rubric_file'])
 (root/row['path']).unlink()
 row.update(path='draft/evaluation/rubric.json',role='teacher')
 rubric={'schema_version':1,'total':value['scoring']['total'],'criteria':[{key:item for key,item in criterion.items() if key!='evidence'} for criterion in criteria]}
 VARIANT
 (root/row['path']).write_text(json.dumps(rubric))
 if not any(item['kind']=='calculation' for item in value['reviews']):value['reviews'].append({'id':'calculation','kind':'calculation','path':'reports/calculation.json','required':True})
 (root/'task.json').write_text(json.dumps(value))
 for item in value['reviews']:
  path=root/item['path'];path.parent.mkdir(parents=True,exist_ok=True)
  path.write_text(json.dumps({'schema_version':1,'review_id':item['id'],'kind':item['kind'],'status':'inconclusive','independent_quality_passed':False,'new_formal_score':None,'local_evidence':['draft/evaluation/answer.md','draft/evaluation/rubric.json']}))
"""


def author_script(variant=""):
    return fixture.SCRIPT.replace(
        " (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')",
        DECLARATION.replace(" VARIANT", " " + variant))


class OptionalCandidateDeclarationTransportTests(unittest.TestCase):
    def setUp(self):
        self.material = self.make_material()
        self.case = self.material["case"]

    def make_material(self, variant=""):
        case = fixture.OptionalCandidateDeliveryTests(
            "test_full_archive_optional_candidate_extends_only_actual_declared_delivery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.script.write_text(author_script(variant))
        attempt = case.run_author()
        case.archive_author()
        original = case.archive / "supplemental/trees/work/next-task/neutral"
        target = case.root / "declaration-transport"
        shutil.copytree(original, target)
        declaration = read_json(target / "task.json")
        referenced = next(row for row in declaration["files"]
                          if row["id"] == declaration["scoring"]["rubric_file"])
        referenced["role"] = "rubric"
        write_json(target / "task.json", declaration)
        proof = case.root / "explicit-declaration-authority.json"
        write_json(proof, {"nature": "neutral explicit declaration-only technical transport",
                           "independent_quality_passed": False, "new_formal_score": None})
        specification = {"schema_version": 1, "kind": "referenced_rubric_role_v1",
                         "original": evidence_ref(original / "task.json", "sealed actual author declaration"),
                         "projected": evidence_ref(target / "task.json", "derived role label only"),
                         "evidence": [evidence_ref(proof, "explicit neutral transport authority")]}
        return {"case": case, "attempt": attempt, "original": original, "target": target,
                "declaration": declaration, "spec": specification}

    def adopt(self, material=None, specification=None, **options):
        material = material or self.material
        case = material["case"]
        return case.store.record_optional_candidate_delivery(
            material["attempt"]["attempt_id"], archive=case.archive,
            candidate_subpath="next-task/neutral", evidence=case.refs,
            declaration_transport=specification or material["spec"], **options)

    def rewrite(self, declaration):
        write_json(self.material["target"] / "task.json", declaration)
        specification = copy.deepcopy(self.material["spec"])
        specification["projected"] = evidence_ref(self.material["target"] / "task.json", "derived role label only")
        return specification

    def test_explicit_role_transport_preserves_raw_author_and_captures_managed_candidate(self):
        material, case = self.material, self.case
        original = material["original"]
        raw = {path: path.read_bytes() for path in
               [original / "task.json", *[case.runtime / name for name in
                ("manifest.json", "request.md", "collection.json", "collection-lock.json", "trace.jsonl")]]}
        self.assertEqual(["files: missing rubric role", "scoring.rubric_file: must reference a rubric file ID"],
                         check_task(original, write_report=False)["errors"])
        with self.assertRaisesRegex(TaskflowError, "incomplete"):
            case.adopt()
        result = self.adopt()
        delivery = result["supplemental_delivery"]
        transport = delivery["declaration_transport"]
        self.assertNotIn("transport", delivery)
        self.assertEqual(material["spec"], transport["definition"])
        self.assertEqual({"json_pointer": "/files/2/role", "file_id": "rubric",
                          "package_path": "draft/evaluation/rubric.json", "old": "teacher", "new": "rubric"},
                         transport["change"])
        self.assertEqual("teacher", transport["source_files"][3]["role"])
        self.assertEqual("rubric", delivery["file_refs"][3]["role"])
        self.assertTrue(transport["rubric_mirror"]["criteria_identical"])
        self.assertEqual([], _candidate_structure(material["target"], "neutral")["errors"])
        child = execution.capture_candidate(case.store, material["target"], "transport-child",
                                            parent_id="parent", author_attempt=result["attempt_id"])
        self.assertEqual(result["attempt_id"], child["author_attempt"])
        self.assertEqual([], child["external_author_sids"])
        self.assertEqual(result, case.adopt())  # Old caller reuses the locked, revalidated transport.
        self.assertEqual(result, self.adopt())
        for path, value in raw.items():
            self.assertEqual(value, path.read_bytes())
        for entry in material["declaration"]["files"]:
            self.assertEqual((original / entry["path"]).read_bytes(), (material["target"] / entry["path"]).read_bytes())
        for review in material["declaration"]["reviews"]:
            report = read_json(material["target"] / review["path"])
            self.assertEqual((original / review["path"]).read_bytes(), (material["target"] / review["path"]).read_bytes())
            self.assertEqual("inconclusive", report["status"])
            self.assertFalse(report["independent_quality_passed"])
            self.assertIsNone(report["new_formal_score"])

    def test_unsigned_local_evidence_cannot_claim_public_foreign_signed_or_duplicate_sources(self):
        target = self.material["target"]
        path = target / "reports/business.json"
        original = path.read_bytes()
        report = read_json(path)
        changes = [{"local_evidence": ["/absolute/private.md"]},
                   {"local_evidence": ["../private.md"]},
                   {"local_evidence": ["draft/input/prompt.md"]},
                   {"local_evidence": ["draft/evaluation/missing.md"]},
                   {"local_evidence": ["draft/evaluation/answer.md", "draft/evaluation/answer.md"]},
                   {"local_evidence": [{"path": "draft/evaluation/answer.md"}]},
                   {"local_evidence": "draft/evaluation/answer.md"},
                   {"sha256": "0" * 64}, {"content_digest": "0" * 64},
                   {"independent_quality_passed": True}, {"new_formal_score": 1},
                   {"independent_signer": "self"}, {"signed_at_utc": "claimed"},
                   {"status": "passed"}, {"review_id": "other"}]
        for change in changes:
            write_json(path, {**report, **change})
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                _candidate_structure(target, "neutral")
        path.write_bytes(original)
        self.assertEqual([], _candidate_structure(target, "neutral")["errors"])

    def test_transport_requires_exact_archive_source_and_typed_explicit_specification(self):
        other = self.case.root / "foreign-same-byte-task.json"
        shutil.copyfile(self.material["original"] / "task.json", other)
        cases = [
            {**self.material["spec"], "original": evidence_ref(other, "foreign identical source")},
            {**self.material["spec"], "schema_version": True},
            {**self.material["spec"], "kind": "path_only"},
            {**self.material["spec"], "undeclared": True},
            {**self.material["spec"], "evidence": []},
        ]
        for specification in cases:
            with self.subTest(specification=specification), self.assertRaises(TaskflowError):
                self.adopt(specification=specification)
        with self.assertRaisesRegex(TaskflowError, "cannot be combined"):
            self.adopt(transport_candidate=self.material["target"])
        with self.assertRaises(TaskflowError):
            self.case.store.record_optional_candidate_delivery(
                self.material["attempt"]["attempt_id"], archive=self.case.archive,
                candidate_subpath="next-task/neutral", evidence=self.case.refs,
                transport_candidate=self.material["target"], transport_evidence=self.material["spec"]["evidence"])

    def test_no_metadata_role_path_membership_or_scoring_change_can_hide_in_transport(self):
        changes = [lambda value: value.update(title="Different purpose"),
                   lambda value: value["scoring"].update(total=8),
                   lambda value: value["files"].reverse(),
                   lambda value: value["files"][0].update(role="teacher"),
                   lambda value: value["files"][1].update(role="teacher"),
                   lambda value: value["files"][2].update(id="other-rubric"),
                   lambda value: value["files"][2].update(path="checks/other.json"),
                   lambda value: value["files"][2].update(references=["S1"])]
        for change in changes:
            value = copy.deepcopy(self.material["declaration"])
            change(value)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.adopt(specification=self.rewrite(value))
        self.rewrite(self.material["declaration"])

    def test_only_already_referenced_private_rubric_with_complete_scoring_mirror_is_eligible(self):
        variants = ["rubric['total']=8",
                    "rubric['criteria'][1]['condition']='Different judgment.'",
                    "rubric['criteria'].reverse()",
                    "rubric['criteria'][0]['extra_norm']='New requirement'",
                    "rubric['schema_version']=True",
                    "value['title']=''",
                    "value['scoring']['rubric_file']='answer'"]
        for variant in variants:
            material = self.make_material(variant)
            with self.subTest(variant=variant), self.assertRaises(TaskflowError):
                self.adopt(material)

    def test_payload_or_evidence_drift_and_replacement_are_rejected_on_old_retry(self):
        result = self.adopt()
        for name in ("draft/input/prompt.md", "draft/evaluation/rubric.json", "task.json"):
            path = self.material["target"] / name
            raw = path.read_bytes()
            path.write_bytes(raw + b" ")
            with self.subTest(name=name), self.assertRaises(TaskflowError):
                self.case.store._delivered_artifacts(result)
            with self.assertRaises(TaskflowError):
                self.case.adopt()
            path.write_bytes(raw)
        proof = Path(self.material["spec"]["evidence"][0]["path"])
        raw = proof.read_bytes()
        proof.write_bytes(raw + b" ")
        with self.assertRaisesRegex(TaskflowError, "evidence changed"):
            self.case.adopt()
        proof.write_bytes(raw)
        other = self.case.root / "other-target"
        shutil.copytree(self.material["target"], other)
        specification = {**self.material["spec"], "projected": evidence_ref(other / "task.json", "different derived root")}
        with self.assertRaisesRegex(TaskflowError, "cannot replace"):
            self.adopt(specification=specification)
        (self.case.work / "next-task/neutral/draft/evaluation/rubric.json").write_text("Changed original")
        with self.assertRaisesRegex(TaskflowError, "terminal work"):
            self.case.store._delivered_artifacts(result)

    def test_special_projected_payload_is_rejected_by_bounded_real_process(self):
        path = self.material["target"] / "draft/evaluation/rubric.json"
        path.unlink()
        os.mkfifo(path)
        code = """
import json,sys
from pathlib import Path
from taskflow.contracts import TaskflowError
from taskflow.execution_store import ExecutionStore
args=json.loads(sys.argv[1])
try:
 ExecutionStore._optional_candidate_material(args['attempt'],Path(args['archive']),'next-task/neutral',declaration_transport=args['spec'])
except TaskflowError:
 sys.exit(0)
sys.exit(1)
"""
        arguments = {"attempt": self.material["attempt"], "archive": str(self.case.archive), "spec": self.material["spec"]}
        process = subprocess.run([sys.executable, "-c", code, json.dumps(arguments)],
                                 cwd=Path(__file__).resolve().parents[1], capture_output=True, timeout=3)
        self.assertEqual(0, process.returncode, process.stderr)

    def test_author_handoff_consumes_only_explicit_state_transport_and_preserves_source_attempt(self):
        case = author_fixture.AuthorSuccessorTests(
            "test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.script.write_text(author_script().replace("source,mode=sys.argv[1:]", "source,mode,*_=sys.argv[1:]"))
        original_attempt = case.run_source("candidate_complete")
        original_attempt = case.store.get_attempt(original_attempt["attempt_id"])
        record = advance_author(case.store, case.register())
        with self.assertRaisesRegex(TaskflowError, "incomplete"):
            advance_author(case.store, record)
        source = Path(record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        target = case.root / "explicit-declaration-candidate"
        shutil.copytree(source, target)
        declaration = read_json(target / "task.json")
        next(row for row in declaration["files"] if row["id"] == declaration["scoring"]["rubric_file"])["role"] = "rubric"
        write_json(target / "task.json", declaration)
        specification = {"schema_version": 1, "kind": "referenced_rubric_role_v1",
                         "original": evidence_ref(source / "task.json", "actual sealed original"),
                         "projected": evidence_ref(target / "task.json", "explicit derived label"), "evidence": case.refs}
        state = {**record["state"], "optional_candidate_declaration_transport": specification}
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_explicit_technical_resolution"})
        result = advance_author(case.store, record)
        self.assertEqual("public_prepare", result["state"]["stage"])
        self.assertEqual(str(target), result["state"]["candidate_root"])
        candidate = case.store.get_candidate(result["state"]["candidate_id"])
        self.assertEqual(original_attempt["attempt_id"], candidate["author_attempt"])
        updated = case.store.get_attempt(original_attempt["attempt_id"])
        self.assertGreater(updated["last_seq"], original_attempt["last_seq"])
        self.assertEqual({key: value for key, value in original_attempt.items() if key != "last_seq"},
                         {key: value for key, value in updated.items()
                          if key not in ("supplemental_delivery", "last_seq")})
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))
        self.assertIsNone(result["state"]["current_work_id"])


if __name__ == "__main__":
    unittest.main()
