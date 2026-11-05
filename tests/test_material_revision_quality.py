"""Material revision quality reuses authentic public observations without new calls."""

import copy
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_public_predecessor import BINDING, KIND
from taskflow.execution_quality import register_quality_task, run_quality_task
from taskflow.execution_quality_contracts import public_predecessor
from taskflow.execution_store import evidence_ref
from tests import test_execution_quality as quality_fixture


class RevisionPublicReuseTests(unittest.TestCase):
    def setUp(self):
        self.case = quality_fixture.ExplicitQualityTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        c = self.case
        self.original = c.store.get_attempt(c.public["attempt_id"])
        self.parent = c.candidate
        self.task = c.root / "private-revised"
        shutil.copytree(c.task, self.task)
        (self.task / "draft/evaluation/answer.md").write_text("Revised private reference only.\n", encoding="utf-8")
        self.target = execution.capture_candidate(c.store, self.task, "private-revision")
        self.spec = c.spec("accepted", corrections=0)
        self.spec.update(candidate_id=self.target["candidate_id"], candidate_root=str(self.task))
        self.packet = {"schema_version": 1, "kind": KIND, "controller": self.spec["owner"],
            "task_id": self.target["task_id"], "quality_transfer": False,
            "reason": "Private evidence changed; retain the original public observation identity.",
            "predecessor": {k: c.public[k] for k in ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id", "cli_sid")},
            "source_candidate_binding": {k: self.parent[k] for k in BINDING},
            "target_candidate_binding": {k: self.target[k] for k in BINDING}, "evidence": c.refs}
        self.publish()

    def publish(self):
        path = self.case.root / "same-public.json"
        write_json(path, self.packet)
        self.spec["public_predecessor_reuse"] = evidence_ref(path, "Exact public applicability; no new run or quality transfer")

    def test_private_revision_runs_only_one_fresh_quality_and_keeps_old_public_original(self):
        c = self.case
        before = len(c.store.dashboard()["attempts"])
        record = register_quality_task(c.store, self.spec, c.refs)
        result = run_quality_task(c.store, record["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"])
        self.assertEqual("accepted", result["state"]["outcome"])
        self.assertTrue(result["state"]["quality_gate"]["accepted"])
        self.assertEqual(before + 1, len(c.store.dashboard()["attempts"]))
        self.assertEqual(self.original, c.store.get_attempt(c.public["attempt_id"]))
        manifest = read_json(c.archive / "manifest.json")
        self.assertEqual(self.parent["candidate_id"], manifest["candidate_binding"]["candidate_id"])
        self.assertEqual(self.original["cli_sid"], read_json(c.archive / "collection.json")["sid"])

    def test_reuse_requires_explicit_current_applicability(self):
        self.spec.pop("public_predecessor_reuse")
        with self.assertRaisesRegex(TaskflowError, "explicit same-public"):
            register_quality_task(self.case.store, self.spec, self.case.refs)
        self.assertEqual([], self.case.store.list_flows())

    def test_optional_size_metadata_is_compared_using_actual_original_bytes(self):
        target = copy.deepcopy(self.target)
        for row in target["files"]:
            row["size"] = Path(row["path"]).stat().st_size
        attempt, _ = public_predecessor(self.case.store, self.spec, target)
        self.assertEqual(self.original, attempt)
        self.assertEqual(self.original, self.case.store.get_attempt(attempt["attempt_id"]))

    def test_member_path_role_bytes_and_size_each_prevent_false_reuse(self):
        actual = self.case.store.get_candidate
        for change in ({"package_path": "renamed.md"}, {"role": "template"}, {"sha256": "f" * 64}, {"size": 999}):
            changed = copy.deepcopy(self.target)
            row = next(row for row in changed["files"] if row["role"] == "prompt")
            row.update(change)
            with self.subTest(change=change), patch.object(self.case.store, "get_candidate", side_effect=lambda identity: changed if identity == changed["candidate_id"] else actual(identity)):
                with self.assertRaisesRegex(TaskflowError, "identical captured public bytes"):
                    public_predecessor(self.case.store, self.spec, changed)

    def test_changed_public_bytes_reject_even_with_updated_target_binding(self):
        row = next(row for row in self.target["files"] if row["role"] == "prompt")
        (self.task / row["package_path"]).write_text("Changed public facts.\n", encoding="utf-8")
        changed = execution.capture_candidate(self.case.store, self.task, "public-changed")
        self.spec.update(candidate_id=changed["candidate_id"])
        self.packet["target_candidate_binding"] = {k: changed[k] for k in BINDING}
        self.publish()
        with self.assertRaisesRegex(TaskflowError, "identical captured public bytes"):
            register_quality_task(self.case.store, self.spec, self.case.refs)

    def test_applicability_cannot_relabel_old_sid_or_transfer_quality(self):
        original = copy.deepcopy(self.packet)
        for change in ({"quality_transfer": True}, {"controller": "other"},
                       {"predecessor": {**self.packet["predecessor"], "cli_sid": "new-sid"}}):
            self.packet = {**original, **change}
            self.publish()
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                register_quality_task(self.case.store, self.spec, self.case.refs)


    def test_source_metadata_cannot_smuggle_old_answer_or_score_into_public_reconstruction(self):
        import taskflow.execution_quality_contracts as module

        c = self.case
        manifest = read_json(Path(c.public["runtime_path"]) / "manifest.json")
        private = next(row for row in self.parent["files"] if row["role"] == "answer")
        contaminated = {**manifest, "inputs": [*manifest["inputs"], {"target": "execution/old-score.json",
            "source": private["path"], "kind": "source_metadata", "sha256": private["sha256"], "size": Path(private["path"]).stat().st_size}]}
        # All candidate/member/attempt bindings remain the actual originals;
        # a metadata label alone must not grant a private exception.
        spec = c.spec("accepted")
        with patch.object(module, "_validate", return_value=contaminated), self.assertRaisesRegex(TaskflowError, "complete exact public view"):
            public_predecessor(c.store, spec, self.parent)
        missing = {**manifest, "inputs": manifest["inputs"][1:]}
        with patch.object(module, "_validate", return_value=missing), self.assertRaisesRegex(TaskflowError, "complete exact public view"):
            public_predecessor(c.store, spec, self.parent)

    def test_identical_public_members_may_reuse_when_private_declaration_ids_change_digest(self):
        c = self.case
        declaration = read_json(self.task / "task.json")
        row = next(row for row in declaration["files"] if row["role"] == "prompt")
        previous_id = row["id"]
        row["id"] = "public-logical-alias"
        for member in declaration["files"]:
            if "references" in member:
                member["references"] = ["public-logical-alias" if item == previous_id else item for item in member["references"]]
        for criterion in declaration["scoring"]["criteria"]:
            criterion["evidence"] = ["public-logical-alias" if item == previous_id else item for item in criterion["evidence"]]
        write_json(self.task / "task.json", declaration)
        self.target = execution.capture_candidate(c.store, self.task, "new-declaration")
        self.assertNotEqual(self.parent["input_digest"], self.target["input_digest"])
        self.packet["target_candidate_binding"] = {k: self.target[k] for k in BINDING}
        self.spec["candidate_id"] = self.target["candidate_id"]
        self.publish()
        registered = register_quality_task(c.store, self.spec, c.refs)
        result = run_quality_task(c.store, registered["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        self.assertEqual(self.original, c.store.get_attempt(c.public["attempt_id"]))

    def test_confirmed_public_role_slot_and_sealed_collection_are_required(self):
        import taskflow.execution_public_predecessor as module

        c = self.case
        attempt = {**c.public, "model_slot": "public_rebuild"}
        original = read_json(Path(c.public["runtime_path"]) / "manifest.json")
        manifest = {**original, "mode": "native_role"}
        with patch.object(module, "_validate", return_value=manifest), patch("taskflow.execution_batch_binding.validate_batch_source", return_value=True):
            source = module.validate_public_reuse_reference(c.store, self.spec["public_predecessor_reuse"],
                owner=self.spec["owner"], task_id="neutral", attempt=attempt, target=self.target)
            self.assertEqual(self.parent, source)
            collection = module._validated_collection(Path(attempt["runtime_path"]), manifest)
            with patch.object(module, "_validated_collection", return_value={**collection, "process_ended": False}):
                with self.assertRaisesRegex(TaskflowError, "sealed successful collection"):
                    module.validate_public_reuse_reference(c.store, self.spec["public_predecessor_reuse"],
                        owner=self.spec["owner"], task_id="neutral", attempt=attempt, target=self.target)

        with patch.object(module, "_validate", return_value={**original, "candidate_binding": {key: self.target[key] for key in BINDING}}):
            with self.assertRaisesRegex(TaskflowError, "source differs from original manifest"):
                module.validate_public_reuse_reference(c.store, self.spec["public_predecessor_reuse"],
                    owner=self.spec["owner"], task_id="neutral", attempt=c.public, target=self.target)


AFFECTED_ROLE = quality_fixture.ROLE.replace(
    "report={'execution_routing':route", "report={'execution_routing':route")
AFFECTED_ROLE = AFFECTED_ROLE.replace(
    "save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':defects})",
    r"""
 if mode.startswith('affected'):
  plan=json.loads((inputs/'execution/revision-coverage.json').read_text())
  diff=json.loads((inputs/'execution/revision-diff.json').read_text())
  route['scope']='affected';native['coverage']='affected'
  native['affected_paths']=[p for p in plan['recheck_paths'] if (inputs/p).is_file()]
  report['revision_coverage']={'schema_version':1,'revision_task_id':plan['revision_task_id'],
   'parent_quality_task_id':plan['parent_quality_task_id'],'plan_sha256':plan['plan_sha256'],
   'scope_confirmed':mode!='affected-unconfirmed','isolation_basis':'Checked complete applicability and public/reference relations.',
   'checked_business_unit_ids':[r['id'] for r in plan['business_units']],
   'checked_related_check_ids':[r['id'] for r in plan['related_checks']],
   'reopened_evidence_sha256':[r['evidence']['sha256'] for r in plan['reopened_evidence']]}
  def position(path):
   name=path if (inputs/path).is_file() else 'execution/parent-inputs/'+path
   return {'path':name,'sha256':hashlib.sha256((inputs/name).read_bytes()).hexdigest(),'location':'L1'}
  report['revision_coverage']['business_unit_checks']=[{'id':r['id'],'evidence':[position(p) for p in r['paths']]} for r in plan['business_units']]
  report['revision_coverage']['reopened_check_evidence']=[{'id':r['evidence']['sha256'],'evidence':[position('execution/reopened/'+str(i).zfill(3)), *[position(p) for p in r['paths']]]} for i,r in enumerate(plan['reopened_evidence'])]
  route['covered_defect_ids']=plan.get('defect_ids',[])
  report['revision_coverage']['related_check_evidence']=[{'id':r['id'],'evidence':[position(p) for p in r['paths']]} for r in plan['related_checks']]
  report['coverage']={'changed_files':[{'path':r['path'],'old':r['old_sha256'],'new':r['new_sha256']} for r in diff['changed']]}
  report['defect_resolution']={'evidence':[{'path':r['current_staged_path'] or r['before_staged_path'],
    'sha256':r['new_sha256'] or r['old_sha256'],'location':'L1'} for r in diff['changed']]}
  if plan.get('declaration_change'):
   report['revision_coverage']['declaration_check_id']=plan['declaration_check']['id']
   report['revision_coverage']['declaration_check_evidence']=[{'id':plan['declaration_check']['id'],'evidence':[position(p) for p in plan['declaration_check']['paths']]}]
   report['coverage']['declaration_change']={'old':plan['declaration_change']['before']['sha256'],'new':plan['declaration_change']['after']['sha256']}
   report['defect_resolution']['evidence'] += [position('execution/parent-task.json'),position('execution/task.json')]
  if mode=='affected-omit-diff':report['coverage']['changed_files']=[]
  if mode=='affected-omit-related':report['revision_coverage']['checked_related_check_ids']=[]
  if mode=='affected-omit-new-evidence':report['revision_coverage']['reopened_evidence_sha256']=[]
  if mode=='affected-omit-citation':report['defect_resolution']['evidence']=[]
  if mode=='affected-bad-location':report['defect_resolution']['evidence'][0]['location']='L999999'
 save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':defects})
""")
AFFECTED_ROLE = AFFECTED_ROLE.replace(
    "else '00000000-0000-4000-8000-000000000003'", "else '00000000-0000-4000-8000-000000000003'")
AFFECTED_ROLE = AFFECTED_ROLE.replace(
    "print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)",
    "sid='00000000-0000-4000-8000-000000000003' if mode.startswith('affected') else sid\n print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)")


class AffectedRevisionQualityTests(unittest.TestCase):
    def setUp(self):
        from tests import test_execution_revision as revision_fixture
        from taskflow.execution_revision import register_revision_task, run_revision_task, validate_revision_source

        self.case = quality_fixture.ExplicitQualityTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        c = self.case
        self.parent_quality = c.register("accepted", corrections=0, identity="parent-quality")
        self.parent_quality = run_quality_task(c.store, self.parent_quality["flow_id"], True)
        self.assertTrue(self.parent_quality["state"]["quality_gate"]["accepted"])
        self.revision_case = revision_fixture.RevisionTaskTests()
        self.revision_case.root = c.root
        self.revision_case.store = c.store
        self.revision_case.task = c.task
        self.revision_case.script = c.root / "revision-role.py"
        self.revision_case.script.write_text(revision_fixture.ROLE, encoding="utf-8")
        self.revision_case.history = c.root / "old-answer.txt"
        self.revision_case.history.write_text("Entire old diagnostic answer and score; teacher only.", encoding="utf-8")
        spec = self.revision_case.spec()
        spec["candidate_id"] = c.candidate["candidate_id"]
        self.revision = register_revision_task(c.store, spec, c.refs)
        self.revision = run_revision_task(c.store, spec["flow_id"], True)
        self.source = validate_revision_source(c.store, spec["flow_id"])
        c.script = c.root / "affected-role.py"
        c.script.write_text(AFFECTED_ROLE, encoding="utf-8")
        self.target = self.source["candidate"]
        self.original_parent = copy.deepcopy(c.candidate)
        self.original_parent_report = (Path(self.parent_quality["state"]["invocations"][0]["runtime"]) / "artifacts/adoption-readiness.json").read_bytes()

    def spec(self, mode="affected", identity="affected-quality"):
        c = self.case
        spec = c.spec(mode, corrections=0, identity=identity)
        spec.update(scope="affected", candidate_id=self.target["candidate_id"], candidate_root=str(self.source["root"]))
        packet = {"schema_version": 1, "kind": KIND, "controller": spec["owner"], "task_id": "neutral",
            "quality_transfer": False, "reason": "Private revision retains exact public observations.",
            "predecessor": {k: c.public[k] for k in ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id", "cli_sid")},
            "source_candidate_binding": {k: c.candidate[k] for k in BINDING},
            "target_candidate_binding": {k: self.target[k] for k in BINDING}, "evidence": c.refs}
        path = c.root / (identity + "-reuse.json")
        write_json(path, packet)
        spec["public_predecessor_reuse"] = evidence_ref(path, "Exact public identity only")
        recheck = ["draft/input/prompt.md", "draft/evaluation/answer.md"]
        spec["coverage"] = {"revision_task_id": self.revision["flow_id"],
            "parent_quality_task_id": self.parent_quality["flow_id"],
            "inherited_paths": [row["package_path"] for row in self.target["files"] if row["package_path"] not in recheck],
            "recheck_paths": recheck,
            "business_units": [{"id": "applicability", "responsibility": "Complete feasibility and lawful equivalents.", "paths": list(recheck)}],
            "related_checks": [{"id": "reference-public", "responsibility": "Reference must use the public capacity constraint.", "paths": list(recheck)}],
            "reopened_evidence": []}
        return spec

    def run_quality(self, spec):
        record = register_quality_task(self.case.store, spec, self.case.refs)
        return run_quality_task(self.case.store, record["flow_id"], True)

    def test_actual_revision_affected_quality_and_solver_source_keep_old_signatures(self):
        from taskflow.execution_solver_contracts import quality_source

        result = self.run_quality(self.spec())
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        self.assertTrue(result["state"]["quality_gate"]["accepted"], result["state"]["quality_gate"])
        source = quality_source(self.case.store, {"candidate_id": self.target["candidate_id"], "task_id": "neutral",
            "candidate_root": str(self.source["root"]), "quality_task_id": result["flow_id"], "runner": {"mode": "test_fixture"}})
        self.assertFalse(source["review"]["whole_package"])
        self.assertEqual("affected", source["quality"]["definition"]["scope"])
        self.assertEqual(self.original_parent, self.case.store.get_candidate(self.original_parent["candidate_id"]))
        report = Path(self.parent_quality["state"]["invocations"][0]["runtime"]) / "artifacts/adoption-readiness.json"
        self.assertEqual(self.original_parent_report, report.read_bytes())
        self.assertIn(self.source["attempt"]["cli_sid"], source["excluded_sids"])
        self.assertIn(self.parent_quality["state"]["invocations"][0]["sid"], source["excluded_sids"])

    def test_missing_diff_association_citation_or_scope_confirmation_is_rejected(self):
        for mode in ("affected-omit-diff", "affected-omit-related", "affected-omit-citation", "affected-unconfirmed", "affected-bad-location"):
            with self.subTest(mode=mode):
                result = self.run_quality(self.spec(mode, mode))
                self.assertEqual("invalid", result["state"]["delivery"])
                self.assertNotIn("quality_gate", result["state"])

    def test_new_evidence_about_unchanged_file_requires_reopen_and_fresh_signature(self):
        c = self.case
        evidence = c.root / "new-evidence.txt"
        evidence.write_text("New evidence about the unchanged criterion requires independent reconsideration.", encoding="utf-8")
        spec = self.spec()
        rubric = "draft/evaluation/rubric.md"
        spec["coverage"]["reopened_evidence"] = [{"evidence": evidence_ref(evidence, "new original evidence"),
            "responsibility": "Unchanged rubric has a newly exposed omission.", "paths": [rubric]}]
        with self.assertRaisesRegex(TaskflowError, "reopened paths"):
            register_quality_task(c.store, spec, c.refs)
        spec["coverage"]["inherited_paths"].remove(rubric)
        spec["coverage"]["recheck_paths"].append(rubric)
        spec["coverage"]["business_units"][0]["paths"].append(rubric)
        result = self.run_quality(spec)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        bad = self.spec("affected-omit-new-evidence", "omit-new-evidence")
        bad["coverage"] = spec["coverage"]
        result = self.run_quality(bad)
        self.assertEqual("invalid", result["state"]["delivery"])

    def test_changed_member_and_complete_related_business_unit_cannot_be_inherited(self):
        spec = self.spec()
        spec["coverage"]["inherited_paths"].append("draft/evaluation/answer.md")
        spec["coverage"]["recheck_paths"].remove("draft/evaluation/answer.md")
        with self.assertRaisesRegex(TaskflowError, "changed member cannot inherit"):
            register_quality_task(self.case.store, spec, self.case.refs)
        spec = self.spec()
        spec["coverage"]["business_units"][0]["paths"].remove("draft/input/prompt.md")
        with self.assertRaisesRegex(TaskflowError, "entire declared recheck"):
            register_quality_task(self.case.store, spec, self.case.refs)


    def test_new_solver_and_first_facts_receive_no_revision_history_or_old_score(self):
        from tests import test_execution_solver as solver_fixture
        from tests import test_execution_assessment as assessment_fixture
        from taskflow.execution_solver import register_solver_task, run_solver_task, validate_solver_source
        from taskflow.execution_assessment import register_assessment_task, run_assessment_task

        c = self.case
        quality = self.run_quality(self.spec())
        solver = solver_fixture.SolverTaskTests()
        solver.root, solver.store, solver.case, solver.quality = c.root, c.store, c, quality
        solver.purpose = "Explicit neutral revised solver request."
        solver.script = c.root / "new-solver.py"
        solver.script.write_text(solver_fixture.SOLVER.replace("00000000-0000-4000-8000-000000000003", "00000000-0000-4000-8000-000000000006"), encoding="utf-8")
        solver.decision = c.store.record_decision("new-solver-decision", "neutral", self.target["candidate_id"],
            action="experiment", rationale="One explicit offline fixture purpose.", evidence=c.refs,
            experiment_purpose=solver.purpose, authorized=True, intake=solver_fixture.experiment_intake(c.refs))
        spec = solver.spec()
        spec.update(candidate_id=self.target["candidate_id"], candidate_root=str(self.source["root"]))
        registered = register_solver_task(c.store, spec, c.refs)
        result = run_solver_task(c.store, registered["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"], result["state"])
        actual = validate_solver_source(c.store, result["flow_id"])
        self.assertTrue(all(row["kind"] == "public" for row in actual["manifest"]["inputs"]))
        reviewer = assessment_fixture.AssessmentTaskTests()
        reviewer.root, reviewer.store, reviewer.case = c.root, c.store, solver
        reviewer.solver, reviewer.source = result, actual
        reviewer.script = c.root / "new-reviewer.py"
        reviewer.script.write_text(assessment_fixture.REVIEWER, encoding="utf-8")
        review_spec = reviewer.spec()
        review_spec.update(candidate_id=self.target["candidate_id"], candidate_root=str(self.source["root"]))
        registered = register_assessment_task(c.store, review_spec, c.refs)
        facts = run_assessment_task(c.store, registered["flow_id"], "facts", True)
        self.assertEqual("valid", facts["state"]["delivery"], facts["state"])
        current = facts["state"]["invocations"]["facts"][-1]
        manifest = read_json(Path(current["runtime"]) / "manifest.json")
        targets = {row["target"] for row in manifest["inputs"]}
        self.assertTrue(any(name.startswith("actual-solver-work/") for name in targets))
        self.assertTrue(any(name.startswith("actual-solver-trace") for name in targets))
        self.assertFalse(any(row["kind"] in ("teacher", "history") for row in manifest["inputs"]))
        for invocation in (result["state"]["invocation"], current):
            runtime = Path(invocation["runtime"])
            payload = (runtime / "request.md").read_bytes()
            payload += b"".join(path.read_bytes() for path in (runtime / "inputs").rglob("*") if path.is_file())
            for forbidden in (b"Entire old diagnostic answer and score", b"revision_coverage", b"isolation_basis", b"revision-author", b"new-evidence"):
                self.assertNotIn(forbidden, payload)

    def test_later_blocker_on_unchanged_file_reopens_current_quality(self):
        from taskflow.execution_quality import collect_quality_task

        c = self.case
        accepted = self.run_quality(self.spec())
        negative = self.spec("revision_required", "later-negative")
        negative.update(scope="whole_package")
        negative.pop("coverage")
        result = self.run_quality(negative)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        self.assertTrue(result["state"]["invocations"][0]["defect_ids"])
        with self.assertRaisesRegex(TaskflowError, "unclosed responsibility defect|later independent review"):
            c.store.quality_gate(self.target["candidate_id"])
        current = collect_quality_task(c.store, accepted["flow_id"])
        self.assertFalse(current["state"]["quality_gate"]["accepted"])

    def _fresh_public_for_target(self):
        from taskflow.execution_runner import archive_full_attempt, prepare_attempt, run_attempt

        c = self.case
        public = copy.deepcopy(c.public_spec)
        work = c.root / "new-public-work"
        work.mkdir()
        runtime = c.root / "new-public-runtime"
        public["work"].update(work_id="new-public", intent_id="new-public-intent", candidate_id=self.target["candidate_id"])
        public["runner"].update(cwd=str(work), intent_id="new-public-intent", candidate_binding={k: self.target[k] for k in BINDING},
            inputs=[{"path": row["path"], "kind": "public", "target": row["package_path"]} for row in self.target["files"] if row["role"] in ("prompt", "input", "template")])
        manifest = prepare_attempt(runtime, public["runner"])
        c.store.enqueue(**public["work"], request_digest=manifest["request"]["sha256"], runtime_path=str(runtime))
        attempt = c.store.claim("fixture-public", ["new-public"])
        result = run_attempt(runtime)
        c.public = c.store.finish_attempt(attempt["attempt_id"], process_exit=0, response_complete=result["model_completed"],
            artifacts_complete=result["delivery_complete"], technical_accepted=result["technical_accepted"], cli_sid=result["sid"],
            evidence=[evidence_ref(runtime / "collection.json", "New public fixture original")])
        c.archive = c.root / "new-public-archive"
        archive_full_attempt(runtime, c.archive)

    def test_public_revision_needs_new_reconstruction_then_checks_complete_unit(self):
        from taskflow.execution_revision import register_revision_task, run_revision_task, validate_revision_source
        from taskflow.execution_runner import archive_full_attempt, prepare_attempt, run_attempt

        c = self.case
        spec = self.revision_case.spec("public", "public-revision")
        spec["candidate_id"] = c.candidate["candidate_id"]
        record = register_revision_task(c.store, spec, c.refs)
        self.revision = run_revision_task(c.store, record["flow_id"], True)
        self.source = validate_revision_source(c.store, record["flow_id"])
        self.target = self.source["candidate"]
        with self.assertRaisesRegex(TaskflowError, "identical captured public bytes"):
            register_quality_task(c.store, self.spec(), c.refs)
        self._fresh_public_for_target()
        spec = self.spec()
        spec.pop("public_predecessor_reuse")
        result = self.run_quality(spec)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        self.assertTrue(result["state"]["quality_gate"]["accepted"], result["state"]["quality_gate"])

    def test_declaration_only_revision_requires_old_new_originals_and_association(self):
        from taskflow.execution_revision import register_revision_task, run_revision_task, validate_revision_source

        c = self.case
        spec = self.revision_case.spec("declaration", "new-standard")
        spec["candidate_id"] = c.candidate["candidate_id"]
        record = register_revision_task(c.store, spec, c.refs)
        self.revision = run_revision_task(c.store, record["flow_id"], True)
        self.source = validate_revision_source(c.store, record["flow_id"])
        self.target = self.source["candidate"]
        quality = self.spec()
        with self.assertRaisesRegex(TaskflowError, "declaration_check"):
            register_quality_task(c.store, quality, c.refs)
        quality["coverage"]["declaration_check"] = {"id": "standard-change", "responsibility": "Review the changed prospective criterion with complete applicability evidence.",
            "paths": list(quality["coverage"]["recheck_paths"])}
        result = self.run_quality(quality)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        current = result["state"]["invocations"][0]
        manifest = read_json(Path(current["runtime"]) / "manifest.json")
        self.assertTrue({"execution/parent-task.json", "execution/task.json"} <= {row["target"] for row in manifest["inputs"]})
        self.assertTrue(result["state"]["quality_gate"]["accepted"], result["state"]["quality_gate"])


    def test_affected_initial_locates_units_without_repeating_all_public_members_or_private_scope(self):
        from taskflow.execution_quality import prepare_quality_task

        spec = self.spec()
        record = register_quality_task(self.case.store, spec, self.case.refs)
        prepared = prepare_quality_task(self.case.store, record["flow_id"])
        runtime = Path(prepared["state"]["invocations"][0]["runtime"])
        text = (runtime / "request.md").read_text(encoding="utf-8")
        self.assertIn("定位相关责任", text)
        self.assertIn("完整必要业务单元", text)
        self.assertNotIn("逐件读取全部公开原件", text)
        self.assertNotIn("Public input:", text)
        for private in ("revision-author", "revision-coverage", "applicability", "reference-public", "parent-quality-signoff"):
            self.assertNotIn(private, text)
        self.assertEqual("affected", read_json(runtime / "manifest.json")["public_first_initial_listing"]["scope"])
        self.assertTrue((runtime / "inputs/execution/revision-author.json").is_file())

    def _close_explicit_defect(self, kind, mode, disposition):
        from taskflow.execution_revision import register_revision_task, run_revision_task, validate_revision_source

        c = self.case
        script = c.root / "parent-finding.py"
        role = quality_fixture.ROLE.replace("'kind':'material'", "'kind':" + repr(kind))
        target = "draft/evaluation/answer.md" if kind == "reference" else "draft/input/prompt.md"
        role = role.replace("blocking=['MATERIAL-1'];target=next(x['path'] for x in json.loads((inputs/'execution/public-inputs.json').read_text())['files'])", "blocking=['MATERIAL-1'];target=" + repr(target))
        role = role.replace(" save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':defects})", " if mode=='revision_required':\n  native['coverage']='unchanged_scope';native['affected_paths']=['draft/input/prompt.md','draft/evaluation/answer.md']\n  for dimension in native['dimensions'].values():dimension['affirmed']=True\n save('adoption-readiness.json',report);save('issues.json',{'execution_binding':binding,'issues':defects})")
        script.write_text(role, encoding="utf-8")
        negative_spec = c.spec("revision_required", corrections=0, identity="parent-finding")
        negative_spec["runner"]["dependencies"] = [str(script)]
        negative_spec["runner"]["argv"][1] = str(script)
        negative = self.run_quality(negative_spec)
        self.assertEqual("valid", negative["state"]["delivery"], negative["state"]["errors"])
        identity = negative["state"]["invocations"][0]["defect_ids"][0]
        self.parent_quality = negative
        spec = self.revision_case.spec(mode, "explicit-defect-revision")
        spec["candidate_id"] = c.candidate["candidate_id"]
        spec["responsibilities"][0]["id"] = identity
        spec["defect_ids"] = [identity]
        spec["defect_repairs"] = [{"defect_id": identity, "disposition": disposition}]
        revision = register_revision_task(c.store, spec, c.refs)
        self.revision = run_revision_task(c.store, revision["flow_id"], True)
        self.source = validate_revision_source(c.store, revision["flow_id"])
        self.target = self.source["candidate"]
        self.assertEqual("repaired", c.store.get_defect(identity)["state"])
        if self.target["input_digest"] != c.candidate["input_digest"]:
            self._fresh_public_for_target()
        quality = self.spec()
        if c.public["candidate_id"] == self.target["candidate_id"]:
            quality.pop("public_predecessor_reuse")
        quality["defect_ids"] = [identity]
        result = self.run_quality(quality)
        self.assertEqual("valid", result["state"]["delivery"], result["state"]["errors"])
        self.assertTrue(result["state"]["quality_gate"]["accepted"], result["state"].get("closure_errors", result["state"]["quality_gate"]))
        defect = c.store.get_defect(identity)
        self.assertEqual("closed", defect["state"])
        self.assertEqual(result["state"]["invocations"][0]["review_id"], defect["closure_review_id"])
        original = c.store.get_review(negative["state"]["invocations"][0]["review_id"])
        self.assertEqual("defects", original["verdict"])
        self.assertEqual(c.candidate["candidate_id"], original["candidate_id"])

        from tests import test_execution_solver as solver_fixture
        from taskflow.execution_solver import register_solver_task, run_solver_task

        solver = solver_fixture.SolverTaskTests()
        solver.root, solver.store, solver.case, solver.quality = c.root, c.store, c, result
        solver.purpose = "Explicit repaired candidate solver fixture."
        solver.script = c.root / "repaired-solver.py"
        solver.script.write_text(solver_fixture.SOLVER.replace("00000000-0000-4000-8000-000000000003", "00000000-0000-4000-8000-000000000006"), encoding="utf-8")
        solver.decision = c.store.record_decision("repaired-solver-decision", "neutral", self.target["candidate_id"],
            action="experiment", rationale="Explicit repaired neutral fixture source.", evidence=c.refs,
            experiment_purpose=solver.purpose, authorized=True, intake=solver_fixture.experiment_intake(c.refs))
        spec = solver.spec()
        spec.update(candidate_id=self.target["candidate_id"], candidate_root=str(self.source["root"]))
        registered = register_solver_task(c.store, spec, c.refs)
        solved = run_solver_task(c.store, registered["flow_id"], True)
        self.assertEqual("valid", solved["state"]["delivery"], solved["state"])

    def test_explicit_private_defect_repair_closes_only_after_real_affected_original(self):
        self._close_explicit_defect("reference", "private", "private_repair")

    def test_explicit_material_defect_public_repair_closes_then_solver_uses_current_source(self):
        self._close_explicit_defect("material", "public", "material_repair")
