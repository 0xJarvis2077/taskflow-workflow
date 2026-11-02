"""Signed rubric mirrors preserve actual scoring changes rather than inherit them.

Three real neutral runner subprocesses produce the original finding, actual
repair and fresh affected closure. They do not call an external model or prove
native permissions. The native source/projection reader is tested separately
from its real-CLI identity adapter, without modifying sealed role artifacts.
"""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow import execution
from taskflow.execution_flow_contracts import NATIVE_SIGNOFF_PROTOCOL_SHA256, parse_review
from taskflow.execution_formal import _native_normative, _prior_whole_findings, _repaired_candidate_source, _retain_repaired_acceptance
from taskflow.execution_runner import _validate, _validated_collection
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_flow as fixtures


def make_scoring_mirror_fixture(testcase, variant="valid"):
    case = fixtures.ContinuationTests("test_one_registration_runs_quality_actual_repair_recheck_receipt")
    case.setUp()
    testcase.addCleanup(case.doCleanups)
    definition = case.definition("mirror")
    task = Path(definition["initial"]["candidate_root"])
    declaration = read_json(task / "task.json")
    rules = copy.deepcopy(declaration["scoring"])
    if variant == "old_mismatch":rules["criteria"][0]["condition"] = "Old file differs from its original declaration."
    path = task / "draft/evaluation/standards.json"
    write_json(path, rules)
    declaration["files"].append({"id": "structured-rules", "path": "draft/evaluation/standards.json", "role": "teacher"})
    if variant == "multiple":
        write_json(task / "draft/evaluation/peer.json", rules)
        declaration["files"].append({"id": "other-structured-rules", "path": "draft/evaluation/peer.json", "role": "teacher"})
    write_json(task / "task.json", declaration)
    parent = capture_candidate(case.store, task, definition["initial"]["candidate_id"])
    prefix = r'''
import pathlib,json,hashlib
VARIANT=VARIANT_VALUE
PARENT=PARENT_VALUE
PROTOCOL=PROTOCOL_VALUE
BASELINE=pathlib.Path(BASELINE_VALUE)
def signed_report(route,ctx,inputs):
 declaration_source=inputs/'execution/task.json'
 if route['scope']=='affected' and VARIANT=='missing_alias':declaration_source=pathlib.Path(ctx['candidate_root'])/'task.json'
 if route['scope']=='affected' and VARIANT=='wrong_target':declaration_source=inputs/'execution/renamed-task.json'
 declaration=json.loads(declaration_source.read_text())
 evidence={'path':'draft/evaluation/standards.json','sha256':digest(inputs/'draft/evaluation/standards.json'),'location':'/criteria/0/condition'}
 selected=[]
 for name,role,purpose in [('draft/evaluation/answer.md','answer','answer'),('draft/evaluation/rubric.md','rubric','rubric'),('draft/evaluation/standards.json','teacher','rubric')]:
  selected.append({'package_path':name,'role':role,'purpose':purpose,'sha256':digest(inputs/name),'size':(inputs/name).stat().st_size,'basis':'This exact current source is independently selected for its stated function.'})
 if route['scope']=='affected' and VARIANT=='unapproved':selected[-1]['purpose']='reference'
 if route['scope']=='affected' and VARIANT=='role':selected[-1]['role']='rubric'
 if route['scope']=='affected' and VARIANT=='sha':selected[-1]['sha256']='0'*64
 if route['scope']=='affected' and VARIANT=='multiple':
  other='draft/evaluation/peer.json';selected.append({'package_path':other,'role':'teacher','purpose':'rubric','sha256':digest(inputs/other),'size':(inputs/other).stat().st_size,'basis':'Another explicitly selected source.'})
 affected=['draft/evaluation/standards.json']+(['draft/evaluation/peer.json'] if VARIANT=='multiple' else [])
 signoff={'schema_version':1,'protocol_sha256':PROTOCOL,'coverage':'affected' if route['scope']=='affected' else 'unchanged_scope','affected_paths':affected,'coverage_basis':'Only this actual private source correction and its necessary associations.',
 'dimensions':{kind:{'affirmed':True if route['scope']=='affected' else kind=='artifact','evidence':[evidence],'limitations':['Original limited private finding is retained.'] if route['scope']!='affected' else []} for kind in ('business','artifact','calculation')},
 'normative':{'status':'determined','files':selected,'evidence':[evidence],'limitations':['The declaration scoring is an exact transport of the selected structured rubric.']},
 'responsibility_analysis':[{'public_evidence':[{'path':'draft/input/prompt.md','sha256':digest(inputs/'draft/input/prompt.md'),'location':'line 1'}],'upstream_work_already_done':'Capacity is explicit.','remaining_judgment':'Retain the required feasible result.','consequence':'The result must satisfy the stated constraint.','retained_necessary_conditions':['Capacity remains explicit.'],'legitimate_branches':['Equivalent lawful reasoning remains valid.'],'disposition':'retained' if route['scope']=='affected' else 'blocking_defect','blocking_issue_ids':route['blocking_issue_ids']}]}
 report={'execution_routing':route,'native_signoff':signoff}
 if route['scope']=='affected' and VARIANT=='protocol':signoff['protocol_sha256']='0'*64
 if route['scope']=='affected' and VARIANT=='outside_scope':signoff['affected_paths']=[]
 if route['scope']=='affected':
  changed=[];refs=[]
  for item in declaration['files']:
   name=item['path'];old=digest(BASELINE/name);new=digest(inputs/name)
   if old!=new:changed.append({'path':name,'old':old,'new':new});refs.append({'path':name,'sha256':new,'location':'/criteria/0/condition'})
  report['coverage']={'parent_binding':PARENT,'current_binding':{k:ctx[k] for k in ('candidate_id','task_id','content_digest','input_digest','evaluation_digest')},'changed_files':changed}
  report['defect_resolution']={'affected_paths':[x['path'] for x in changed],'evidence':refs}
  if VARIANT!='missing_declaration':report['verification']={'evidence':[{'path':'execution/task.json','sha256':digest(declaration_source),'location':'/scoring'}]}
 return report
'''.replace("VARIANT_VALUE", repr(variant)).replace("PARENT_VALUE", repr({key: parent[key] for key in
        ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")})).replace(
        "PROTOCOL_VALUE", repr(NATIVE_SIGNOFF_PROTOCOL_SHA256)).replace("BASELINE_VALUE", repr(str(task)))
    script = fixtures.ROLE_FIXTURE.replace("{'execution_routing':route}", "signed_report(route,ctx,inputs)")
    script = script.replace("name='draft/evaluation/answer.md'", "name='draft/evaluation/standards.json'")
    old_change = "changed='draft/evaluation/answer.md';before=digest(root/changed);(root/changed).write_text('Actual repaired private support.\\n')"
    new_change = """changed='draft/evaluation/standards.json';before=digest(root/changed)
 rules=json.loads((root/changed).read_text());rules['criteria'][0]['condition']='The corrected required result with lawful alternatives.'
 save(root/changed,rules);task['scoring']=json.loads(json.dumps(rules))
 if VARIANT=='nonmirror':task['scoring']['criteria'][0]['condition']='An unsigned different condition.'
 if VARIANT=='extra_metadata':task['title']='A different unapproved package title'
 save(root/'task.json',task)"""
    script = script.replace(old_change, new_change)
    extra = """
 if VARIANT=='multiple':
  name='draft/evaluation/peer.json';old=digest(root/name);save(root/name,rules);record=json.loads(pathlib.Path('repair-record.json').read_text());record['changes'].append({'path':name,'before_sha256':old,'after_sha256':digest(root/name)});save('repair-record.json',record)
"""
    script = script.replace(" pathlib.Path('handoff.md').write_text", extra + " pathlib.Path('handoff.md').write_text")
    if variant == "multiple":
        script = script.replace("'affected_paths':[name]", "'affected_paths':[name,'draft/evaluation/peer.json']")
    case.script.write_text(prefix + script)
    case.register(definition)
    enqueue = execution.enqueue_spec
    def prepare_variant(store, specification, runtime, **kwargs):
        # Only mutate the neutral preparation specification before its first
        # manifest/request/collection exists. Never reseal delivered evidence.
        argv = specification["runner"].get("argv", [])
        affected_review = (specification["work"]["role"] == "quality" and "--scope" in argv
                           and argv[argv.index("--scope") + 1] == "affected")
        if affected_review and variant in {"missing_alias", "wrong_target"}:
            specification = copy.deepcopy(specification)
            inputs = specification["runner"]["inputs"]
            if variant == "missing_alias":
                specification["runner"]["inputs"] = [row for row in inputs if row["target"] != "execution/task.json"]
            else:
                next(row for row in inputs if row["target"] == "execution/task.json")["target"] = "execution/renamed-task.json"
        return enqueue(store, specification, runtime, **kwargs)
    with patch("taskflow.execution.enqueue_spec", side_effect=prepare_variant):
        case.run_flow([definition["flow_id"]])
    flow = case.store.get_flow(definition["flow_id"])
    history = [row for row in flow["state"]["history"] if row.get("kind") == "independent_review"]
    if variant in {"missing_alias", "wrong_target"}:
        # The normal factory correctly refuses the missing alias first. Keep
        # that exception; exercise the lower managed-review store API with the
        # same actual completed/sealed role, not a forged successful flow.
        testcase.assertEqual("exception", flow["state"]["status"])
        testcase.assertEqual("review declaration alias has a foreign source", flow["state"]["exception"]["reason"])
        work_id = flow["state"]["current_work_id"]
        attempt = next(row for row in case.store.dashboard()["attempts"] if row["work_id"] == work_id)
        testcase.assertEqual("completed", attempt["state"])
        runtime = Path(attempt["runtime_path"])
        _validated_collection(runtime, _validate(runtime))
        candidate = case.store.get_candidate(flow["state"]["candidate_id"])
        report, issues = runtime / "artifacts/adoption-readiness.json", runtime / "artifacts/issues.json"
        parsed = parse_review(report, issues, candidate=candidate, intent_id=attempt["intent_id"], scope="affected")
        testcase.assertEqual("accepted", parsed["outcome"])
        review = case.store.record_review("managed-alias-negative-recheck", candidate["candidate_id"], attempt["attempt_id"],
            verdict="accepted", scope="actual managed affected source without canonical alias", whole_package=False,
            covered_defect_ids=parsed["covered_defect_ids"], evidence=[evidence_ref(report, "actual sealed original role routing"),
                                                                   evidence_ref(issues, "same-role original issues")])
        for defect_id in flow["state"]["repaired_defects"]:
            case.store.close_defect(defect_id, review["review_id"], evidence=review["evidence"])
        recheck_id = review["review_id"]
    else:
        testcase.assertEqual("accepted", flow["state"]["status"])
        recheck_id = history[-1]["review_id"]
    return {"case": case, "store": case.store, "flow": flow, "candidate": flow["state"]["candidate_id"],
            "whole": history[0]["review_id"], "recheck": recheck_id,
            "defects": flow["state"]["repaired_defects"], "refs": case.refs, "parent": parent}


class ScoringMetadataMirrorTests(unittest.TestCase):
    def compose(self, value):
        return value["store"].compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
                                                      value["defects"], evidence=value["refs"])

    def test_real_repair_and_affected_closure_bind_exact_signed_scoring_objects(self):
        value = make_scoring_mirror_fixture(self)
        result = self.compose(value)
        witness = result["composite_quality_coverage"]["scoring_metadata_mirror"]
        self.assertEqual("deterministic_signed_scoring_mirror_v1", witness["kind"])
        self.assertEqual("/scoring/criteria/0/condition", witness["changes"][0]["json_pointer"])
        self.assertEqual("teacher", witness["source"]["role"])
        self.assertEqual("unchanged_scope", witness["original_scope"]["coverage"])
        self.assertEqual(witness["after"], read_json(Path(witness["source"]["current"]["path"])))
        self.assertEqual(witness["before"], read_json(Path(witness["source"]["original"]["path"])))
        alias = witness["declaration_alias"]
        self.assertEqual("execution/task.json", alias["manifest_input"]["target"])
        self.assertEqual("source_metadata", alias["manifest_input"]["kind"])
        self.assertEqual(witness["current_declaration"]["path"], alias["source"]["path"])
        self.assertEqual(alias["source"]["sha256"], alias["staged"]["sha256"])
        self.assertEqual(value["candidate"], alias["candidate_binding"]["candidate_id"])
        self.assertFalse(result["whole_package"])
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))
        self.assertEqual(result, value["store"].assert_review_current(result["review_id"]))
        self.assertEqual(result, self.compose(value))
        self.assertEqual("defects", value["store"].get_review(value["whole"])["verdict"])

    def test_unsigned_nonmirror_and_other_metadata_are_rejected(self):
        for variant, pattern in (("nonmirror", "one exact"), ("extra_metadata", "cannot silently"),
                                  ("unapproved", "one exact"), ("missing_declaration", "declaration citation")):
            with self.subTest(variant=variant):
                value = make_scoring_mirror_fixture(self, variant)
                with self.assertRaisesRegex(TaskflowError, pattern):self.compose(value)

    def test_wrong_role_sha_multiple_sources_and_old_object_mismatch_are_rejected(self):
        for variant, pattern in (("role", "explicit current private"), ("sha", "SHA/size"),
                                  ("multiple", "one exact"), ("old_mismatch", "Original scoring object"),
                                  ("protocol", "actual requested"), ("outside_scope", "outside the actual")):
            with self.subTest(variant=variant):
                value = make_scoring_mirror_fixture(self, variant)
                with self.assertRaisesRegex(TaskflowError, pattern):self.compose(value)

    def test_original_issue_responsibility_and_exact_metadata_boundary_are_required(self):
        value = make_scoring_mirror_fixture(self)
        accepted = self.compose(value)
        graph = accepted["composite_quality_coverage"]
        with value["store"]._db() as db:
            old = ExecutionStore._composition_review(db, value["store"].get_review(value["whole"]), "whole_package")
            fresh = ExecutionStore._composition_review(db, value["store"].get_review(value["recheck"]), "affected")
        mirror = graph["scoring_metadata_mirror"]
        before = read_json(Path(mirror["original_declaration"]["path"]))
        current = read_json(Path(mirror["current_declaration"]["path"]))
        # Pure reader argument checks, not forged successful managed signatures.
        for variant in ("same_scoring", "other_metadata", "not_original_issue", "missing_change"):
            with self.subTest(variant=variant):
                document, changes = copy.deepcopy(current), copy.deepcopy(graph["private_changes"])
                if variant == "same_scoring":document["scoring"] = copy.deepcopy(before["scoring"])
                elif variant == "other_metadata":document["title"] = "Unapproved independent metadata"
                elif variant == "not_original_issue":changes[0]["original_issue_affected"] = False
                else:changes = []
                with self.assertRaises(TaskflowError):
                    ExecutionStore._scoring_metadata_mirror(old, fresh, before, document,
                        mirror["original_declaration"], mirror["current_declaration"], changes)

    def test_canonical_witness_cannot_be_relabelled_or_repointed(self):
        value = make_scoring_mirror_fixture(self)
        result = self.compose(value)
        for field in ("after", "source", "signed_evidence", "declaration_alias"):
            forged = copy.deepcopy(result)
            witness = forged["composite_quality_coverage"]["scoring_metadata_mirror"]
            witness[field] = {} if field != "signed_evidence" else []
            with value["store"]._db() as db, self.assertRaisesRegex(TaskflowError, "changed or upgraded"):
                ExecutionStore._validate_repaired_quality(db, forged)

    def test_actual_managed_missing_or_renamed_alias_cannot_use_a_report_self_claim(self):
        for variant in ("missing_alias", "wrong_target"):
            with self.subTest(variant=variant):
                value = make_scoring_mirror_fixture(self, variant)
                fresh = value["store"].get_review(value["recheck"])
                attempt = value["store"].get_attempt(fresh["attempt_id"])
                root = Path(attempt["runtime_path"])
                manifest = _validate(root)
                self.assertFalse(any(row["target"] == "execution/task.json" for row in manifest["inputs"]))
                raw = read_json(root / "artifacts/adoption-readiness.json")
                claimed = raw["verification"]["evidence"][0]
                self.assertEqual("execution/task.json", claimed["path"])
                candidate = value["store"].get_candidate(value["candidate"])
                declaration = next(ref for ref in candidate["evidence"] if ref["location"] == "explicit package declaration")
                self.assertEqual(declaration["sha256"], claimed["sha256"])
                self.assertEqual("completed", attempt["state"])
                with self.assertRaisesRegex(TaskflowError, "actual locked current declaration alias"):
                    self.compose(value)

    def test_native_repaired_projection_preserves_whole_false_and_commits_full_graph(self):
        value = make_scoring_mirror_fixture(self)
        accepted = self.compose(value)
        graph = accepted["composite_quality_coverage"]
        with value["store"]._db() as db:
            whole = ExecutionStore._composition_review(db, value["store"].get_review(value["whole"]), "whole_package")
            fresh = ExecutionStore._composition_review(db, value["store"].get_review(value["recheck"]), "affected")
        for source in (whole, fresh):
            source["root"] = Path(source["attempt"]["runtime_path"])
            source["manifest"] = _validate(source["root"])
            source["signoff"] = source["report"]["native_signoff"]
            source["source_refs"] = {"adoption": source["report_ref"], "issues": source["issues_ref"]}
        whole["prior_whole_findings"] = _prior_whole_findings(whole["candidate"], whole["attempt"], whole["manifest"],
            whole["root"], whole["source_refs"], whole["report"])
        self.assertEqual(3, len(_native_normative(fresh)))
        projection = _repaired_candidate_source(fresh["candidate"], accepted, graph, whole, fresh)
        report = {}
        _retain_repaired_acceptance(report, {"repaired_candidate_acceptance": projection}, "business")
        self.assertFalse(report["prior_dimension"]["affirmed"])
        self.assertTrue(report["current_dimension"]["affirmed"])
        from taskflow.contracts import canonical, sha256
        self.assertEqual(sha256(canonical(graph)), projection["composite_graph_sha256"])
        self.assertEqual("verified_repaired_candidate_acceptance", report["status_basis"])


if __name__ == "__main__":
    unittest.main()
