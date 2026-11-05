"""First-score evidence and native-shaped handoff fixtures; zero model calls."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_eligibility_evidence import first_score_evidence, safe_argv
from taskflow.execution_runner import _info, run_attempt
from taskflow.execution_review import (
    LOCAL_EXECUTION_TARGET, _local_execution_trace, _validate_local_execution_evidence,
    _managed_projection_context, _project, _review_source_alias,
    ingest_signed_evaluation, prepare_score_runner,
)
from taskflow.execution import enqueue_spec
from tests import test_execution_review as review_fixtures
from tools.execution_simulation import fixture_policy


class EligibilityEvidenceTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.directory = self.root / "run"; self.directory.mkdir()
        self.snapshot = self.root / "snapshot"; self.snapshot.mkdir()
        write_json(self.snapshot / "task.json", {"scoring":{"total":7,"criteria":[]}})
        (self.snapshot / "answer.md").write_text("Frozen answer.")
        write_json(self.snapshot / "manifest.json", {"files":[
            {"path":"public.txt","role":"input","sha256":"e"*64,"size":2},
            {"path":"unselected-private.md","role":"teacher","sha256":"9"*64,"size":1}]})
        self.source = self.root / "selected"; self.source.mkdir()
        (self.source / "answer.md").write_text("Frozen answer.")
        write_json(self.source / "scoring.json", {"total":7,"criteria":[]})
        self.inputs = [self.teacher(self.source / "answer.md", "normative/answer.md"),
                       self.teacher(self.source / "scoring.json", "normative/task-scoring.json")]
        argv = ["/opt/bin/codex", "--no-daemon", "-c", 'shell_environment_policy.set={"OPENAI_API_KEY":"SECRET_ENV"}',
                "exec", "-m", "neutral-model", "-c", 'model_reasoning_effort="low"', "--json", "-"]
        records = [
            ("plan", "execution-plan.json", {"argv":argv,"input_digest":"a"*64,
                "private_tree":{"SECRET_PRIVATE":"SECRET_CONTENT"},"headers":{"Authorization":"SECRET_HEADER"},
                "frozen_binding":{"snapshot_digest":"b"*64,"snapshot_manifest_sha256":"c"*64,"hidden":"SECRET_FROZEN"}}),
            ("lock", "plan-lock.json", {"sha256":"d"*64,"api_key":"SECRET_LOCK"}),
            ("readback", "post-run-input-readback.json", {"matches":True,"errors":["SECRET_ERROR"],
                "files":{"public.txt":{"sha256":"e"*64,"size":2,"content":"SECRET_BODY"}}}),
            ("context", "exec-context.json", {"observations":[{"model":"SECRET_CAPTURE"}],"stdout":"SECRET_STDOUT"}),
        ]
        artifacts, index = [], []
        for identity, name, value in records:
            path = self.directory / (identity + ".json"); write_json(path, value)
            row = {"id":identity,"path":path.name,**_info(path)}
            artifacts.append(row); index.append({**row,"root_id":"inner","source_path":name})
        write_json(self.directory / "source-index.json", {"files":index})
        write_json(self.directory / "directories.json", {"aliases":{"inner":{"source":"inner","relative":"."}}})
        for identity, name in (("source_index","source-index.json"),("directories","directories.json")):
            artifacts.append({"id":identity,"path":name,**_info(self.directory / name)})
        self.run = {"run_digest":"f"*64,"snapshot_digest":"b"*64,"snapshot_path":str(self.snapshot),"artifacts":artifacts}
        self.observed = datetime.now(timezone.utc).isoformat()

    def teacher(self, path, target):
        return {"source":str(path),"path":str(path),"kind":"teacher","target":target,**_info(path)}

    def build(self):
        return first_score_evidence(run=self.run,run_directory=self.directory,snapshot=self.snapshot,
            inputs=self.inputs,input_root=None,scoring_source_allowlist=["answer.md"],observed_at=self.observed)

    def test_finite_safe_view_has_original_anchors_and_current_scope_without_a_verdict(self):
        report = self.build()
        text = json.dumps(report)
        self.assertNotIn("SECRET_", text)
        self.assertNotIn("private_tree", text)
        self.assertNotIn("local_eligible", report)
        self.assertEqual({"plan","lock","readback"}, {r["artifact_id"] for r in report["retained_execution"]})
        plan = next(r for r in report["retained_execution"] if r["artifact_id"] == "plan")
        view = plan["fields"]["locked_command"]
        self.assertTrue(view["available"])
        self.assertIn('model_reasoning_effort="low"', view["safe_argv_view"])
        self.assertFalse(view["complete_argv_disclosed"])
        self.assertEqual("/argv", plan["source_json_pointers"][0])
        self.assertEqual("current_selected_source_readback_before_score_capture_not_historical_witness",
                         report["current_reference_comparison"]["observation_scope"])
        self.assertTrue(all(r["matches"] for r in report["current_reference_comparison"]["files"]))
        self.assertTrue(all("captured" not in r for r in report["current_reference_comparison"]["files"]))
        self.assertEqual(["public.txt"], [r["path"] for r in report["frozen_public_input"]["files"]])

    def test_opaque_or_credential_cli_never_copies_payload_and_missing_stays_missing(self):
        for argv in (["curl","-H","Authorization: SECRET_HTTP"], ["codex","exec","SECRET_PROMPT"],
                     ["codex","--api-key","SECRET_KEY"], [], {"argv":"SECRET_OBJECT"}):
            with self.subTest(argv=argv):
                result = safe_argv(argv)
                self.assertFalse(result["available"])
                self.assertNotIn("SECRET_", json.dumps(result))
        self.run["artifacts"] = []
        self.assertEqual([], self.build()["retained_execution"])

    def test_original_byte_drift_and_metadata_drift_are_rejected(self):
        (self.directory / "plan.json").write_text("{}")
        with self.assertRaisesRegex(TaskflowError,"bytes changed"): self.build()
        (self.directory / "source-index.json").write_text("{}")
        with self.assertRaisesRegex(TaskflowError,"metadata bytes changed"): self.build()

    def trace(self, runtime, runner, binding):
        clean = runtime.parent / (runtime.name + "-review") / "public-root"; clean.mkdir(parents=True,exist_ok=True)
        return _local_execution_trace(run=self.run,config={"run_directory":str(self.directory)},
            runner=runner,binding=binding,clean=clean,runtime=runtime,
            facts={"at":(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()})

    def runner(self):
        return {"inputs":[{"path":i["source"],"kind":"teacher","target":i["target"]} for i in self.inputs],
                "scoring_source_allowlist":["answer.md"],"request_sections":[],"dependencies":[]}

    def test_new_and_partial_preparation_reenter_without_new_time_or_inputs_legacy_unchanged(self):
        runtime = self.root / "runtime"; runner=self.runner(); binding=self.trace(runtime,runner,{})
        path=runtime.parent/(runtime.name+"-review")/"review-binding.json";write_json(path,binding);runtime.mkdir()
        first = Path(binding["local_execution_evidence"]["path"]).read_bytes()
        next_runner=self.runner(); self.assertEqual(binding,self.trace(runtime,next_runner,{}))
        self.assertEqual(first,Path(binding["local_execution_evidence"]["path"]).read_bytes())
        self.assertEqual(runner,next_runner)
        legacy=self.root/"legacy";legacy.mkdir();legacy_runner=self.runner()
        self.assertEqual({},self.trace(legacy,legacy_runner,{}))
        self.assertFalse(any(i["target"]==LOCAL_EXECUTION_TARGET for i in legacy_runner["inputs"]))

    def test_interrupt_after_evidence_before_binding_reuses_original_observation(self):
        runtime=self.root/"interrupted-runtime"
        first=self.trace(runtime,self.runner(),{})
        path=Path(first["local_execution_evidence"]["path"])
        original=path.read_bytes()
        self.assertFalse((runtime.parent/(runtime.name+"-review")/"review-binding.json").exists())
        second=self.trace(runtime,self.runner(),{})
        self.assertEqual(first,second)
        self.assertEqual(original,path.read_bytes())

    def prepared(self):
        runtime=self.root/"validate-runtime";runner=self.runner();binding=self.trace(runtime,runner,{})
        reference=binding["local_execution_evidence"]
        inputs=[{"source":i["path"],"kind":i["kind"],"target":i["target"],**_info(Path(i["path"]))} for i in runner["inputs"]]
        manifest={"inputs":inputs,"scoring_source_allowlist":["answer.md"],"created_at":datetime.now(timezone.utc).isoformat()}
        facts={"at":(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()}
        return runtime,reference,manifest,facts

    def test_intake_recomputes_originals_and_rejects_self_consistent_forgery_duplicate_or_time(self):
        _,ref,manifest,facts=self.prepared()
        validate=lambda r=ref,m=manifest: _validate_local_execution_evidence(r,manifest=m,run=self.run,
            run_directory=self.directory,snapshot={"snapshot_digest":self.run["snapshot_digest"]},facts=facts)
        validate()
        for change in ({"path":None},{"path":[]},{"sha256":True},{"size":True},{"size":-1}):
            with self.subTest(change=change),self.assertRaises(TaskflowError):validate({**ref,**change})
        future=copy.deepcopy(manifest);future["created_at"]=facts["at"]
        with self.assertRaisesRegex(TaskflowError,"time differs"):validate(m=future)
        duplicate=copy.deepcopy(manifest);duplicate["inputs"].append(copy.deepcopy(duplicate["inputs"][-1]))
        with self.assertRaisesRegex(TaskflowError,"input binding"):validate(m=duplicate)
        path=Path(ref["path"]);document=read_json(path);document["retained_execution"][0]["fields"]["input_digest"]="0"*64
        write_json(path,document);forged={**ref,**_info(path)};changed=copy.deepcopy(manifest);changed["inputs"][-1].update(_info(path))
        with self.assertRaisesRegex(TaskflowError,"exact retained"):validate(forged,changed)

    def test_new_qualification_alias_is_not_a_criterion_source(self):
        runtime,reference,manifest,facts=self.prepared();(runtime/"inputs").mkdir(parents=True)
        target=runtime/"inputs"/LOCAL_EXECUTION_TARGET;target.write_bytes(Path(reference["path"]).read_bytes())
        binding={"selector":{"path":str(self.root/"indexes/first.json")},"local_execution_evidence":reference}
        context=_managed_projection_context(runtime,manifest,binding,{"path":"/unchanged-facts"},{},[])
        self.assertEqual([],context["plan"]["sources"])
        result=_review_source_alias(LOCAL_EXECUTION_TARGET,{"type":"json_pointer","pointer":"/retained_execution"},context,self.run,{})
        self.assertEqual("local_execution_qualification_observations",result["source_type"])
        score={"max_score":7,"criteria":[{"id":"C1","max_score":7,"score":7,"reason":"unchanged",
            "public_evidence":[{"path":LOCAL_EXECUTION_TARGET,"locator":"/retained_execution"}]}]}
        with self.assertRaisesRegex(TaskflowError,"locked first-stage"):
            _project(score,context,{"run_directory":str(self.directory)},self.run,{"scoring":{"total":7,"criteria":[{"id":"C1","max_score":7}]}})


class NormalScoreEvidenceHandoffTests(unittest.TestCase):
    def test_normal_prepare_collect_intake_unknown_and_reentry_zero_model(self):
        case=review_fixtures.CleanReviewPreparationTests("test_managed_collected_score_preserves_original_once_and_full_archives")
        case.setUp();self.addCleanup(case.doCleanups)
        original,score=case.fixture_managed_score(local_assessment=True,review_aliases=True)
        config=copy.deepcopy(case.clean_config)
        config.update(facts_attempt=original["facts_attempt"],facts_binding=original["facts_binding"],
                      scoring_source_allowlist=["draft/evaluation/answer.md","draft/evaluation/rubric.md"])
        policy=fixture_policy()
        for setting in policy["roles"].values():setting.update(model="local-fixture",reasoning_effort="fixture",timeout_seconds=10)
        case.store.register_policy("qualification-view-fixture",policy)
        config["work"].update(work_id="qualification-view",intent_id="qualification-view-intent",role="review-score",policy_id="qualification-view-fixture")
        config["runner"].update(intent_id="qualification-view-intent",role="review-score",model="local-fixture",reasoning_effort="fixture",timeout_seconds=10,policy_version="qualification-view-fixture",outputs=["first-score.json","scoring-review.md"])
        runtime=case.root/"normal-qualification-runtime"
        score["experiment_validity"]["evidence"].append({"path":LOCAL_EXECUTION_TARGET,"locator":{"type":"json_pointer","pointer":"/retained_execution"}})
        script=case.root/"neutral-qualification-score.py"
        script.write_text("import json,pathlib\npathlib.Path('first-score.json').write_text("+repr(json.dumps(score))+")\npathlib.Path('scoring-review.md').write_text('Unchanged original fixture score; no model call.')\n"+
            "\n".join("print(json.dumps("+repr(e)+"))" for e in ({"type":"thread.started","thread_id":review_fixtures.SID},{"type":"item.completed","item":{"type":"agent_message","text":"Neutral fixture score saved; zero model calls."}},{"type":"turn.completed"})))
        def enqueue(store,spec,runtime,**kwargs):
            value=copy.deepcopy(spec);value["runner"].update(mode="test_fixture",argv=[sys.executable,str(script)])
            value["runner"].pop("native_permissions",None)
            return enqueue_spec(store,value,runtime)
        def profile(store,config,run,snapshot,candidate,directory):
            path=directory/"local-profile-definition.json";write_json(path,{"document_kind":"local-profile-definition","scope":"local_fixed_client_profile","execution_binding":score["execution_binding"]})
            return {"path":str(path),"target":"local-profile-definition.json",**_info(path)}
        def boundary(runtime,forbidden):write_json(runtime/"review-boundary-probe.json",{"manifest":_info(runtime/"manifest.json"),"model_calls":0})
        with patch("taskflow.execution.enqueue_spec",side_effect=enqueue),patch("taskflow.execution_review._profile_definition",side_effect=profile),patch("taskflow.execution_review._denied_probe",side_effect=boundary):
            prepared=prepare_score_runner(store=case.store,config=config,runtime=runtime)
            self.assertEqual(prepared,prepare_score_runner(store=case.store,config=config,runtime=runtime))
        self.assertEqual([],case.store.list_attempts(config["work"]["work_id"]))
        manifest=read_json(runtime/"manifest.json");self.assertEqual(review_fixtures.SID,manifest["resume_sid"])
        self.assertTrue(any(i["target"]==LOCAL_EXECUTION_TARGET and i["kind"]=="trace" for i in manifest["inputs"]))
        self.assertTrue(run_attempt(runtime)["technical_accepted"])
        intake={**original,"score_attempt":str(runtime),"score_binding":prepared["review_binding"],"evaluation_id":"qualification-view-original"}
        result=ingest_signed_evaluation(config=intake,destination=case.root/"qualification-view-intake")
        self.assertTrue(result["accepted"]);self.assertEqual(7,result["score"])
        self.assertEqual("unknown",result["local_profile_assessment"]["local_eligible"])
        self.assertTrue(result["local_profile_assessment"]["signed_binding_valid"])
        self.assertTrue(any(r.get("source_type")=="local_execution_qualification_observations" for r in result["local_profile_assessment"]["evidence"]))
        self.assertEqual("test_fixture",result["nature"])

        # A later eligibility-only correction inherits this exact first-score
        # view; its new scoring projection path must not reinterpret the origin.
        from taskflow.execution_review_supplement import KIND, reference_comparison
        from taskflow.records import verify_run
        from taskflow.packages import verify_snapshot
        from taskflow.execution_review import seal_existing_facts
        run=verify_run(case.run);snapshot=verify_snapshot(case.snapshot)
        facts=seal_existing_facts(config={"facts_attempt":original["facts_attempt"],"review_binding":original["facts_binding"]})["seal"]
        manifest=read_json(runtime/"manifest.json")
        parent_score={"path":str(runtime/"artifacts/first-score.json"),**_info(runtime/"artifacts/first-score.json")}
        comparison=case.root/"current-correction-comparison.json"
        write_json(comparison,reference_comparison(run=run,parent_manifest=manifest,parent_root=runtime,
            parent_score=parent_score,observed_at=datetime.now(timezone.utc).isoformat()))
        correction_runtime=case.root/"normal-view-correction-runtime"
        def ref(path):return {"path":str(path),**_info(path)}
        document={"schema_version":1,"kind":KIND,"run_digest":run["run_digest"],"snapshot_digest":snapshot["snapshot_digest"],
            "candidate_binding":manifest["candidate_binding"],"facts_seal_digest":sha256(canonical(facts)),"reviewer_sid":review_fixtures.SID,
            "parent_evaluation_id":intake["evaluation_id"],"parent_score_attempt":str(runtime),"parent_score_binding":ref(Path(prepared["review_binding"])),
            "parent_score":parent_score,"original_profile":ref(runtime/"inputs/local-profile-definition.json"),
            "members":[{"id":"comparison","origin":"current_reference_comparison","source":ref(comparison),"artifact_id":None}],
            "correction":{"evaluation_id":"view-correction","work_id":"view-correction-work","intent_id":"view-correction-intent","runtime":str(correction_runtime)}}
        supplement=case.root/"view-correction-manifest.json";write_json(supplement,document)
        corrected=copy.deepcopy(score);corrected["experiment_validity"]["reason"]="Additional current comparison; original score remains unchanged."
        script=case.root/"neutral-correction-score.py"
        script.write_text("import pathlib,json\npathlib.Path('first-score.json').write_text("+repr(json.dumps(corrected))+")\npathlib.Path('scoring-review.md').write_text('Eligibility-only fixture; unchanged criteria.')\n"+
            "\n".join("print(json.dumps("+repr(e)+"))" for e in ({"type":"thread.started","thread_id":review_fixtures.SID},{"type":"item.completed","item":{"type":"agent_message","text":"Neutral correction fixture saved."}},{"type":"turn.completed"})))
        correction_config=copy.deepcopy(config);correction_config["eligibility_correction"]=ref(supplement)
        correction_config["work"].update(work_id="view-correction-work",intent_id="view-correction-intent")
        correction_config["runner"].update(intent_id="view-correction-intent")
        with patch("taskflow.execution.enqueue_spec",side_effect=enqueue),patch("taskflow.execution_review._profile_definition",side_effect=profile),patch("taskflow.execution_review._denied_probe",side_effect=boundary):
            correction_prepared=prepare_score_runner(store=case.store,config=correction_config,runtime=correction_runtime)
        self.assertTrue(run_attempt(correction_runtime)["technical_accepted"])
        corrected_intake={**intake,"evaluation_id":"view-correction","eligibility_correction":ref(supplement),
                          "score_attempt":str(correction_runtime),"score_binding":correction_prepared["review_binding"]}
        received=ingest_signed_evaluation(config=corrected_intake,destination=case.root/"view-correction-intake")
        self.assertTrue(received["accepted"]);self.assertEqual("correction",received["kind"])
        self.assertEqual(7,received["score"]);self.assertEqual("unknown",received["local_profile_assessment"]["local_eligible"])
        self.assertTrue(received["local_profile_assessment"]["signed_binding_valid"])
        self.assertEqual(manifest["resume_sid"],read_json(correction_runtime/"manifest.json")["resume_sid"])


class ExistingBoundedControlRecipeTests(unittest.TestCase):
    def test_normal_claim_repause_then_claimed_worker_preserves_originals(self):
        from tests import test_execution_review_supplement as correction_fixtures
        from taskflow.execution import _run_worker_claimed
        case=correction_fixtures.EligibilityCorrectionTests()
        case.setUp();self.addCleanup(case.doCleanups)
        case.prepare()
        work_id=case.document["correction"]["work_id"]
        task_id=case.store.get_work(work_id)["task_id"]
        task_scope="task:"+task_id
        def task_control():
            with case.store._db() as db:
                return json.loads(db.execute("SELECT data FROM controls WHERE scope=?",(task_scope,)).fetchone()[0])
        case.store.set_control(task_scope,"pause","Neutral first-original task pause")
        before=task_control()
        case.store.set_control("work:"+work_id,"resume","Cannot override the task pause")
        self.assertIsNone(case.store.claim("neutral-correction-only",[work_id]))
        self.assertEqual([],case.store.list_attempts(work_id))
        with case.store._db() as db:
            seq=db.execute("SELECT MAX(seq) FROM events").fetchone()[0]
        try:
            case.store.set_control(task_scope,"resume","Explicit fixture controller authority for only the selected correction")
            attempt=case.store.claim("neutral-correction-only",[work_id])
        finally:
            case.store.set_control(task_scope,"pause",before["reason"])
        self.assertEqual(before,task_control())
        self.assertEqual(work_id,attempt["work_id"])
        with case.store._db() as db:
            events=[(kind,json.loads(data)) for kind,data in db.execute("SELECT kind,data FROM events WHERE seq>? ORDER BY seq",(seq,))]
        self.assertEqual(["control","claimed","control"],[kind for kind,_ in events])
        self.assertEqual("resume",events[0][1]["action"])
        self.assertEqual("pause",events[-1][1]["action"])
        def identity(pid):
            self.assertEqual("pause",task_control()["action"])
            return {"pid":pid,"start_identity":"explicit zero-model fixture identity"}
        with patch("taskflow.execution_runner._identity",side_effect=identity):
            result=_run_worker_claimed(case.store,attempt,True,None)
        self.assertEqual("completed",result["state"])
        self.assertEqual(before,task_control())
        self.assertEqual(1,len(case.store.list_attempts(work_id)))
        case.assert_originals()
