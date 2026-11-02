"""Declared unsigned report layout transport from real neutral author archives."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import _candidate_structure, advance_author
from taskflow.execution_store import evidence_ref
from taskflow.packages import check_task
from tests import test_execution_author as author_fixture
from tests import test_execution_optional_candidate_delivery as fixture


SOURCE_REPORTS = """
 value=json.loads((root/'task.json').read_text())
 value['reviews'].append({'id':'calculation','kind':'calculation','path':'reports/calculation.json','required':True}) if not any(row['kind']=='calculation' for row in value['reviews']) else None
 for review in value['reviews']:
  review['path']='checks/reports/'+review['kind']+'.json'
  report={'schema_version':1,'kind':review['kind'],'status':'inconclusive','independent':False,'author_checked':True,'summary':'Neutral local author observations only.','evidence':['S1'],'independent_quality_passed':False,'formal_score':None}
  VARIANT
  path=root/review['path'];path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(report))
  value['files'].append({'id':'raw_'+review['id'],'path':review['path'],'role':'teacher','references':['S1']})
 zero='draft/evaluation/empty-events.jsonl'
 (root/zero).write_bytes(b'')
 value['files'].append({'id':'empty_events','path':zero,'role':'teacher','references':['S1']})
 (root/'task.json').write_text(json.dumps(value))
"""


def source_script(variant=""):
    return fixture.SCRIPT.replace(
        " (root/'unlisted-extra.txt').write_text('Not declared candidate delivery.')",
        SOURCE_REPORTS.replace("  VARIANT", "  " + variant))


def project_unsigned_reports(original, target, authority):
    shutil.copytree(original, target)
    declaration = read_json(target / "task.json")
    for review in declaration["reviews"]:
        old = review["path"]
        row = next(row for row in declaration["files"] if row["path"] == old)
        source = target / old
        raw, value = source.read_bytes(), read_json(source)
        row["path"] = "draft/evaluation/author-provenance/unsigned-source-reports/" + source.name
        new = target / row["path"]
        new.parent.mkdir(parents=True, exist_ok=True)
        source.rename(new)
        review["path"] = "reports/" + source.name
        write_json(target / review["path"], {
            "schema_version": 1, "review_id": review["id"], "kind": review["kind"],
            "status": "inconclusive", "task_id": declaration["task_id"],
            "independent_quality_passed": False, "new_formal_score": None,
            "authority": "Derived unsigned author metadata; independent quality remains pending.",
            "local_checks": {"original_author_report": value, "original_relative_path": old,
                             "original_sha256": sha256(raw)},
            "limitations": ["Constructed neutral local author evidence, not an independent pass."]})
    next(row for row in declaration["files"] if row["id"] == "empty_events")["empty_content"] = {
        "kind": "zero_record_jsonl", "reason": "The neutral control contains no events.", "evidence": ["S1"]}
    write_json(target / "task.json", declaration)
    return {"schema_version": 1, "kind": "declared_unsigned_report_layout_v1",
            "original": evidence_ref(original / "task.json", "exact original sealed author declaration"),
            "projected": evidence_ref(target / "task.json", "derived unsigned layout and explicit zero-record metadata"),
            "evidence": authority}


class OptionalCandidateReportLayoutTests(unittest.TestCase):
    def setUp(self):
        self.material = self.make_material()
        self.case = self.material["case"]

    def make_material(self, variant=""):
        case = fixture.OptionalCandidateDeliveryTests(
            "test_full_archive_optional_candidate_extends_only_actual_declared_delivery")
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.script.write_text(source_script(variant))
        attempt = case.run_author()
        case.archive_author()
        original = case.archive / "supplemental/trees/work/next-task/neutral"
        target = case.root / "unsigned-layout"
        specification = project_unsigned_reports(original, target, case.refs)
        return {"case": case, "attempt": attempt, "original": original, "target": target,
                "specification": specification, "declaration": read_json(target / "task.json")}

    def adopt(self, material=None, specification=None, **options):
        material = material or self.material
        case = material["case"]
        return case.store.record_optional_candidate_delivery(
            material["attempt"]["attempt_id"], archive=case.archive, candidate_subpath="next-task/neutral",
            evidence=case.refs, declaration_transport=specification or material["specification"], **options)

    def rewrite(self, value):
        write_json(self.material["target"] / "task.json", value)
        specification = copy.deepcopy(self.material["specification"])
        specification["projected"] = evidence_ref(self.material["target"] / "task.json", "explicit changed transport under test")
        return specification

    def test_full_archive_layout_preserves_original_and_captures_actual_managed_author(self):
        case, material = self.case, self.material
        original = material["original"]
        source_task = read_json(original / "task.json")
        snapshots = {path: path.read_bytes() for path in [original / "task.json",
                     *[original / row["path"] for row in source_task["files"]],
                     *[case.runtime / name for name in ("manifest.json", "request.md", "collection.json", "trace.jsonl")]]}
        with self.assertRaisesRegex(TaskflowError, "incomplete"):
            case.adopt()
        self.assertEqual([], check_task(material["target"], write_report=False)["errors"])
        result = self.adopt()
        delivery, transport = result["supplemental_delivery"], result["supplemental_delivery"]["declaration_transport"]
        projection = transport["projection"]
        self.assertEqual(material["specification"], transport["definition"])
        self.assertEqual(7, len(projection["declaration_changes"]))
        self.assertEqual(3, len(projection["unsigned_reports"]))
        self.assertEqual(1, len(projection["empty_content"]))
        self.assertEqual([], _candidate_structure(material["target"], "neutral")["errors"])
        for row in projection["unsigned_reports"]:
            self.assertTrue(row["original_json_preserved"])
            self.assertEqual(Path(row["original"]["path"]).read_bytes(), Path(row["relocated"]["path"]).read_bytes())
            self.assertEqual(read_json(Path(row["original"]["path"])), read_json(Path(row["wrapper"]["path"]))["local_checks"]["original_author_report"])
            self.assertNotIn(row["wrapper"]["sha256"], case.store._delivered_artifacts(result))
        child = execution.capture_candidate(case.store, material["target"], "layout-child",
                                            parent_id="parent", author_attempt=result["attempt_id"])
        self.assertEqual(result["attempt_id"], child["author_attempt"])
        self.assertEqual([], child["external_author_sids"])
        self.assertEqual(result, case.adopt())
        self.assertEqual(result, self.adopt())
        for path, raw in snapshots.items():
            self.assertEqual(raw, path.read_bytes())
        self.assertEqual(len(source_task["files"]) + 1, len(delivery["file_refs"]))

    def test_foreign_source_or_existing_transport_modes_cannot_authorize_layout(self):
        other = self.case.root / "same-raw-task.json"
        shutil.copyfile(self.material["original"] / "task.json", other)
        specifications = [{**self.material["specification"], "original": evidence_ref(other, "foreign identical metadata")},
                          {**self.material["specification"], "schema_version": True},
                          {**self.material["specification"], "kind": "referenced_rubric_role_v1"},
                          {**self.material["specification"], "extra": True}]
        for specification in specifications:
            with self.subTest(specification=specification), self.assertRaises(TaskflowError):
                self.adopt(specification=specification)
        with self.assertRaises(TaskflowError):
            self.adopt(transport_candidate=self.material["target"])

    def test_only_corresponding_report_paths_and_typed_zero_metadata_can_change(self):
        changes = [lambda value: value.update(title="Other business"),
                   lambda value: value["scoring"].update(total=8),
                   lambda value: value["files"].reverse(),
                   lambda value: value["files"][0].update(role="teacher"),
                   lambda value: value["files"][1].update(path="checks/moved-answer.md"),
                   lambda value: value["files"][4].update(role="verification"),
                   lambda value: value["files"][4].update(path="draft/evaluation/other-business.json"),
                   lambda value: value["reviews"][0].update(id="foreign"),
                   lambda value: value["reviews"][0].update(kind="artifact"),
                   lambda value: value["reviews"][0].update(required=False),
                   lambda value: value["reviews"][0].update(path=value["files"][4]["path"])]
        for change in changes:
            value = copy.deepcopy(self.material["declaration"])
            change(value)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.adopt(specification=self.rewrite(value))
        self.rewrite(self.material["declaration"])

    def test_original_reports_must_be_unsigned_and_belong_to_the_declared_kind(self):
        variants = ["report['status']='passed'", "report['independent']=True",
                    "report['independent_quality_passed']=True", "report['formal_score']=1",
                    "report['kind']='foreign'", "report['review_id']='foreign'",
                    "report['signed_at_utc']='claimed'", "report['schema_version']=True"]
        for variant in variants:
            material = self.make_material(variant)
            with self.subTest(variant=variant), self.assertRaises(TaskflowError):
                self.adopt(material)

    def test_wrapper_preserves_all_original_json_path_and_sha_without_signing(self):
        path = self.material["target"] / "reports/business.json"
        original = path.read_bytes()
        wrapper = read_json(path)
        changes = [lambda value: value.update(status="passed"),
                   lambda value: value.update(independent_quality_passed=True),
                   lambda value: value.update(new_formal_score=1),
                   lambda value: value.update(review_id="foreign"),
                   lambda value: value.update(task_id="foreign"),
                   lambda value: value.update(independent_signer="claimed"),
                   lambda value: value["local_checks"].update(original_relative_path="other.json"),
                   lambda value: value["local_checks"].update(original_sha256="0" * 64),
                   lambda value: value["local_checks"]["original_author_report"].update(summary="Changed original"),
                   lambda value: value["local_checks"]["original_author_report"].pop("evidence")]
        for change in changes:
            value = copy.deepcopy(wrapper)
            change(value)
            write_json(path, value)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.adopt()
        path.write_bytes(original)

    def test_empty_metadata_uses_core_contract_and_cannot_hide_nonempty_or_public_bytes(self):
        changes = [lambda value: value["files"][-1].pop("empty_content"),
                   lambda value: value["files"][-1]["empty_content"].update(evidence=["unknown"]),
                   lambda value: value["files"][-1]["empty_content"].update(evidence=["S1", "S1"]),
                   lambda value: value["files"][-1]["empty_content"].update(reason=" "),
                   lambda value: value["files"][-1]["empty_content"].update(kind="allow_empty"),
                   lambda value: value["files"][-1].update(role="answer"),
                   lambda value: value["files"][0].update(empty_content=value["files"][-1]["empty_content"])]
        for change in changes:
            value = copy.deepcopy(self.material["declaration"])
            change(value)
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                self.adopt(specification=self.rewrite(value))
        self.rewrite(self.material["declaration"])
        path = self.material["target"] / "draft/evaluation/empty-events.jsonl"
        path.write_text("{}\n")
        with self.assertRaises(TaskflowError):
            self.adopt()
        path.write_bytes(b"")

    def test_wrapper_and_payload_drift_reject_reuse_and_validation_time_mutation(self):
        result = self.adopt()
        for name in ("reports/business.json", "draft/input/prompt.md",
                     "draft/evaluation/author-provenance/unsigned-source-reports/business.json"):
            path = self.material["target"] / name
            raw = path.read_bytes()
            path.write_bytes(raw + b" ")
            with self.subTest(name=name), self.assertRaises(TaskflowError):
                self.case.store._delivered_artifacts(result)
            with self.assertRaises(TaskflowError):
                self.case.adopt()
            path.write_bytes(raw)
        original_ref = evidence_ref
        target = self.material["target"] / "reports/business.json"
        def mutate(path, location):
            if path == target:
                path.write_bytes(path.read_bytes() + b" ")
            return original_ref(path, location)
        with patch("taskflow.execution_store.evidence_ref", side_effect=mutate):
            with self.assertRaisesRegex(TaskflowError, "changed during validation"):
                self.adopt()

    def test_fifo_wrapper_returns_within_real_subprocess_timeout(self):
        path = self.material["target"] / "reports/business.json"
        path.unlink();os.mkfifo(path)
        code = """
import json,sys
from pathlib import Path
from taskflow.contracts import TaskflowError
from taskflow.execution_store import ExecutionStore
value=json.loads(sys.argv[1])
try:ExecutionStore._optional_candidate_material(value['attempt'],Path(value['archive']),'next-task/neutral',declaration_transport=value['spec'])
except TaskflowError:sys.exit(0)
sys.exit(1)
"""
        arguments = {"attempt": self.material["attempt"], "archive": str(self.case.archive), "spec": self.material["specification"]}
        process = subprocess.run([sys.executable, "-c", code, json.dumps(arguments)],
                                 cwd=Path(__file__).resolve().parents[1], capture_output=True, timeout=3)
        self.assertEqual(0, process.returncode, process.stderr)

    def test_author_state_handoff_keeps_original_actor_and_advances_only_to_public_prepare(self):
        case = author_fixture.AuthorSuccessorTests(
            "test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp();self.addCleanup(case.doCleanups)
        case.script.write_text(source_script().replace("source,mode=sys.argv[1:]", "source,mode,*_=sys.argv[1:]"))
        attempt = case.run_source("candidate_complete")
        record = advance_author(case.store, case.register())
        original = Path(record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        target = case.root / "explicit-layout-candidate"
        specification = project_unsigned_reports(original, target, case.refs)
        state = {**record["state"], "optional_candidate_declaration_transport": specification}
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_explicit_layout_resolution"})
        result = advance_author(case.store, record)
        self.assertEqual("public_prepare", result["state"]["stage"])
        self.assertEqual(str(target), result["state"]["candidate_root"])
        candidate = case.store.get_candidate(result["state"]["candidate_id"])
        self.assertEqual(attempt["attempt_id"], candidate["author_attempt"])
        self.assertIsNone(result["state"]["current_work_id"])
        self.assertEqual(1, len(case.store.dashboard()["attempts"]))


if __name__ == "__main__":
    unittest.main()
