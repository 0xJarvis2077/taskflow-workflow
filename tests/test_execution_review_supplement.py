"""Offline managed correction fixtures; no native probe or external model calls."""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution import enqueue_spec
from taskflow.execution_runner import _info, prepare_attempt, run_attempt
from taskflow.execution_review import ingest_signed_evaluation, prepare_score_runner, seal_existing_facts
from taskflow.execution_review_supplement import KIND, reference_comparison, validate_output, validate_supplement
from taskflow.packages import verify_snapshot
from taskflow.records import import_run, verify_run
from tests import test_execution_review as fixtures
from tools.execution_simulation import fixture_policy


def ref(path):
    return {"path": str(path), **_info(path)}


class EligibilityCorrectionTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.CleanReviewPreparationTests("test_managed_collected_score_preserves_original_once_and_full_archives")
        case.setUp(); self.addCleanup(case.doCleanups)
        self.case, self.root, self.store = case, case.root, case.store
        launch = case.incoming / "retained-launch.json"
        write_json(launch, {"nature": "neutral original record fixture", "argv": ["neutral-cli", "exec"]})
        index = case.incoming / "source-index.json"
        write_json(index, {"files": [{"id": "retained_launch", "root_id": "inner", "source_path": "execution-plan.json", **_info(launch)}]})
        directories = case.incoming / "directories.json"
        write_json(directories, {"aliases": {"inner": {"source": "inner", "relative": "."}}})
        record = read_json(case.incoming / "clean-run.json")
        record["run_id"] = "fixture-supplement-run"
        record["artifacts"].extend([{"id": name, "path": str(path.relative_to(case.incoming)), "kind": "evidence"}
            for name, path in [("retained_launch", launch), ("source_index", index), ("directories", directories)]])
        write_json(case.incoming / "supplement-run.json", record)
        case.run = Path(import_run(case.snapshot, case.incoming / "supplement-run.json")["path"])
        case.clean_config["run_directory"] = str(case.run)
        self.profile = self.root / "original-profile.json"
        run = verify_run(case.run)
        write_json(self.profile, {"document_kind": "local-profile-definition", "scope": "local_fixed_client_profile",
            "execution_binding": {"run_digest": run["run_digest"], "snapshot_digest": run["snapshot_digest"],
                                  "input_digest": run["actual_input_digest"], "evaluation_digest": run["evaluation_digest"]},
            "judgment": {"limits": "Retained ordinary final work/scratch and visible trace/request; outbound payload remains unknown."}})
        original_prepare = prepare_attempt
        def parent_prepare(runtime, config):
            if config["role"] == "review-score":
                for item in config["inputs"]:
                    if item["kind"] == "teacher" and not item["target"].startswith("normative/"):
                        item["target"] = "normative/" + item["target"]
                binding_path = Path(config["dependencies"][0]); binding = read_json(binding_path)
                binding["local_profile_definition"] = ref(self.profile)
                write_json(binding_path, binding)
                config["inputs"].append({"path": str(self.profile), "kind": "trace", "target": "local-profile-definition.json"})
            return original_prepare(runtime, config)
        original_dumps = json.dumps
        def with_frozen_source(value, *args, **kwargs):
            if isinstance(value, dict) and "total_score" in value and "criteria" in value:
                value["frozen_sources"] = [{"path": str(self.root / "managed-score-attempt/inputs/normative/draft/evaluation/answer.md"),
                                            **_info(case.snapshot / "draft/evaluation/answer.md")}]
            return original_dumps(value, *args, **kwargs)
        with patch("tests.test_execution_review.prepare_attempt", side_effect=parent_prepare), \
             patch("tests.test_execution_review.json.dumps", side_effect=with_frozen_source):
            self.original_config, self.parent_score = case.fixture_managed_score(local_assessment=True, review_aliases=True)
        self.original = ingest_signed_evaluation(config=self.original_config, destination=self.root / "original-intake")
        self.parent_root = Path(self.original_config["score_attempt"])
        self.facts_root = Path(self.original_config["facts_attempt"])
        self.facts = seal_existing_facts(config={"facts_attempt": str(self.facts_root),
            "review_binding": self.original_config["facts_binding"]})["seal"]
        self.run = verify_run(case.run); self.snapshot = verify_snapshot(case.snapshot)
        self.parent_manifest = read_json(self.parent_root / "manifest.json")
        comparison = reference_comparison(run=self.run, parent_manifest=self.parent_manifest,
            parent_root=self.parent_root, parent_score=ref(self.parent_root / "artifacts/first-score.json"),
            observed_at=datetime.now(timezone.utc).isoformat())
        self.comparison = self.root / "comparison.json"; write_json(self.comparison, comparison)
        self.runtime = self.root / "correction-runtime"
        self.document = {"schema_version": 1, "kind": KIND, "run_digest": self.run["run_digest"],
            "snapshot_digest": self.run["snapshot_digest"], "candidate_binding": self.parent_manifest["candidate_binding"],
            "facts_seal_digest": sha256(canonical(self.facts)), "reviewer_sid": fixtures.SID,
            "parent_evaluation_id": self.original_config["evaluation_id"], "parent_score_attempt": str(self.parent_root),
            "parent_score_binding": ref(Path(self.original_config["score_binding"])),
            "parent_score": ref(self.parent_root / "artifacts/first-score.json"),
            "original_profile": ref(self.parent_root / "inputs/local-profile-definition.json"),
            "correction": {"evaluation_id": "eligibility-correction", "work_id": "eligibility-correction-work",
                           "intent_id": "eligibility-correction-intent", "runtime": str(self.runtime)},
            "members": [{"id": "frozen", "origin": "frozen_manifest", "artifact_id": None,
                         "source": ref(case.snapshot / "manifest.json")},
                        {"id": "comparison", "origin": "current_reference_comparison", "artifact_id": None,
                         "source": ref(self.comparison)},
                        {"id": "locked-launch", "origin": "run_artifact", "artifact_id": "retained_launch",
                         "source": ref(case.run / next(x["path"] for x in self.run["artifacts"] if x["id"] == "retained_launch"))}]}
        self.manifest = self.root / "supplement.json"; self.refresh()
        self.protected = {str(p): p.read_bytes() for p in [self.parent_root / "artifacts/first-score.json",
            Path(self.original["evaluation"]) / "record.original.json", Path(self.original["receipt"]),
            self.facts_root / "artifacts/facts.json", self.facts_root / "facts-seal.json"]}
        self.next_score = copy.deepcopy(self.parent_score)
        self.next_score["experiment_validity"].update(local_eligible="unknown", reason="The original scope remains unresolved.")
        self.next_score["experiment_validity"]["evidence"].append({
            "path": "supplemental-trace/member-comparison.json", "locator": {"type": "json_pointer", "pointer": "/files"}})

    def refresh(self):
        write_json(self.manifest, self.document)
        self.reference = ref(self.manifest)

    def validate(self):
        return validate_supplement(reference=self.reference, run=self.run, run_directory=self.case.run,
            snapshot=self.snapshot, facts=self.facts, facts_root=self.facts_root,
            candidate_binding=self.parent_manifest["candidate_binding"])

    def prepare(self):
        config = copy.deepcopy(self.case.clean_config)
        config.update(facts_attempt=str(self.facts_root), facts_binding=self.original_config["facts_binding"],
            scoring_source_allowlist=self.parent_manifest["scoring_source_allowlist"],
            eligibility_correction=self.reference)
        if getattr(self, "extra_teacher", False):
            config["scoring_source_allowlist"] = [*config["scoring_source_allowlist"], "draft/evaluation/author-notes.md"]
        if getattr(self, "injected_inputs", False):
            config["runner"]["inputs"] = [{"path":str(self.comparison),"kind":"trace","target":"other.json"}]
        config["work"].update(role="review-score", work_id="eligibility-correction-work",
            intent_id="eligibility-correction-intent", policy_id="correction-fixture-v1")
        config["runner"].update(role="review-score", intent_id="eligibility-correction-intent",
            policy_version="correction-fixture-v1", model="local-fixture", reasoning_effort="fixture", timeout_seconds=10,
            outputs=["first-score.json", "scoring-review.md"])
        policy = fixture_policy()
        for setting in policy["roles"].values():
            setting.update(model="local-fixture", reasoning_effort="fixture", timeout_seconds=10)
        if not getattr(self, "policy_registered", False):
            self.store.register_policy("correction-fixture-v1", policy)
            self.policy_registered = True
        self.prepared_spec = None
        def fixture_enqueue(store, spec, runtime, **kwargs):
            self.prepared_spec = copy.deepcopy(spec)
            value = copy.deepcopy(spec)
            script = self.root / "correction-fixture.py"
            script.write_text("import pathlib,json\npathlib.Path('first-score.json').write_text(" + repr(json.dumps(self.next_score)) + ")\n"
                "pathlib.Path('scoring-review.md').write_text('Separate eligibility-only correction; original score unchanged.')\n" +
                "\n".join("print(json.dumps(" + repr(event) + "))" for event in (
                    {"type":"thread.started","thread_id":fixtures.SID},
                    {"type":"item.completed","item":{"type":"agent_message","text":"Neutral fixture only."}},
                    {"type":"turn.completed"})))
            value["runner"].update(mode="test_fixture", argv=[sys.executable,str(script)])
            value["runner"].pop("native_permissions",None)
            return enqueue_spec(store,value,runtime)
        def profile(store, config, run, snapshot, candidate, directory):
            target=directory/"local-profile-definition.json";target.write_bytes(self.profile.read_bytes())
            return {**ref(target), "target": "local-profile-definition.json"}
        def denied(runtime, forbidden):
            write_json(runtime/"review-boundary-probe.json",{"manifest":_info(runtime/"manifest.json"),"model_calls":0})
        with patch("taskflow.execution.enqueue_spec",side_effect=fixture_enqueue), \
             patch("taskflow.execution_review._profile_definition",side_effect=profile), \
             patch("taskflow.execution_review._denied_probe",side_effect=denied):
            self.prepared=prepare_score_runner(store=self.store,config=config,runtime=self.runtime)
        return self.prepared

    def collect(self):
        self.prepare()
        self.assertTrue(run_attempt(self.runtime)["technical_accepted"])
        config={**self.original_config,"score_attempt":str(self.runtime),"score_binding":self.prepared["review_binding"],
                "evaluation_id":"eligibility-correction","eligibility_correction":self.reference}
        return config

    def assert_originals(self):
        self.assertEqual(self.protected,{p:Path(p).read_bytes() for p in self.protected})

    def test_managed_prepare_and_correction_intake_keep_parent_numbers_facts_and_sid(self):
        config=self.collect()
        manifest=read_json(self.runtime/"manifest.json")
        self.assertEqual(fixtures.SID,manifest["resume_sid"])
        self.assertTrue(all(i["kind"]=="trace" for i in manifest["inputs"] if i["target"].startswith("supplemental-trace/")))
        request=(self.runtime/"request.md").read_text()
        self.assertIn("no desired conclusion",request)
        self.assertIn("Copy the parent's complete score JSON unchanged except experiment_validity",request)
        result=ingest_signed_evaluation(config=config,destination=self.root/"correction-intake")
        self.assertEqual("correction",result["kind"])
        self.assertEqual(self.original_config["evaluation_id"],result["parent_evaluation_id"])
        self.assertEqual(self.original["score"],result["score"])
        self.assertEqual("unknown",result["local_profile_assessment"]["local_eligible"])
        self.assertEqual(self.next_score,read_json(Path(result["evaluation"])/"record.original.json")["signed_original_score"])
        self.assertEqual(result,ingest_signed_evaluation(config=config,destination=self.root/"correction-intake"))
        self.assert_originals()

    def test_new_references_rules_wrong_run_sid_or_parent_are_rejected_before_dispatch(self):
        original=copy.deepcopy(self.document)
        edits=[lambda d:d.update(reviewer_sid="foreign-sid"),lambda d:d.update(run_digest="0"*64),
               lambda d:d.update(facts_seal_digest="0"*64),lambda d:d.update(parent_evaluation_id="missing-parent"),
               lambda d:d.update(kind="generic_extra_inputs"),lambda d:d.update(new_rules="Accept this score"),
               lambda d:d["correction"].update(runtime=str(self.parent_root)),
               lambda d:d["members"].append({"id":"new-rule","origin":"run_artifact","artifact_id":"public",
                   "source":ref(self.case.snapshot/"draft/evaluation/author-notes.md")})]
        for edit in edits:
            self.document=copy.deepcopy(original);edit(self.document);self.refresh()
            with self.subTest(edit=edit),self.assertRaises((TaskflowError,OSError)):self.validate()
        self.assertEqual([],self.store.dashboard()["attempts"])
        self.assert_originals()

    def test_fabricated_current_comparison_hidden_text_duplicates_and_source_drift_reject(self):
        report=read_json(self.comparison);original=self.comparison.read_bytes()
        for key,value in [("new_instruction","Sign true"),("observation_scope","historical_witness"),("observed_at","2000-01-01T00:00:00+00:00")]:
            altered={**report,key:value};write_json(self.comparison,altered)
            self.document["members"][1]["source"]=ref(self.comparison);self.refresh()
            with self.subTest(key=key),self.assertRaises(TaskflowError):self.validate()
        self.comparison.write_bytes(original)
        self.document["members"][1]["source"]=ref(self.comparison);self.refresh()
        self.comparison.write_bytes(original+b" ")
        with self.assertRaisesRegex(TaskflowError,"bytes changed"):self.validate()
        self.comparison.write_bytes(original)
        self.document["members"].append(copy.deepcopy(self.document["members"][0]));self.refresh()
        with self.assertRaisesRegex(TaskflowError,"duplicate"):self.validate()
        self.assert_originals()

    def test_output_scope_rejects_changed_score_max_reason_artifacts_rules_or_facts(self):
        supplement=self.validate()
        for value in (True,False,"unknown"):
            score=copy.deepcopy(self.next_score);score["experiment_validity"]["local_eligible"]=value
            validate_output(score,supplement)
        edits=[lambda s:s.update(total_score=6),lambda s:s.update(max_score=8),
               lambda s:s["criteria"][0].update(score=6),lambda s:s["criteria"][0].update(reason="New grading"),
               lambda s:s.update(extra_facts=[]),lambda s:s["artifact_status"].update(artifacts_complete=False),
               lambda s:s["experiment_validity"].update(local_eligible=1),
               lambda s:s["experiment_validity"].update(served_identity_unknown_disclosed=False),
               lambda s:s["experiment_validity"].update(official_provider_identity=True)]
        for edit in edits:
            score=copy.deepcopy(self.next_score);edit(score)
            with self.subTest(edit=edit),self.assertRaises(TaskflowError):validate_output(score,supplement)

    def test_intake_rejects_changed_numbers_and_keeps_original_unique(self):
        self.next_score["total_score"]=6
        config=self.collect()
        with self.assertRaisesRegex(TaskflowError,"changed original score"):ingest_signed_evaluation(config=config,destination=self.root/"bad-intake")
        self.assertFalse((self.case.run/"evaluations/eligibility-correction").exists())
        self.assert_originals()

    def test_intake_requires_same_manifest_target_parent_and_citation_scope(self):
        config=self.collect()
        wrong={**config,"eligibility_correction":None}
        with self.assertRaisesRegex(TaskflowError,"supplemental handoff"):ingest_signed_evaluation(config=wrong,destination=self.root/"bad-intake")
        wrong={**config,"evaluation_id":self.original_config["evaluation_id"]}
        with self.assertRaisesRegex(TaskflowError,"overwrite parent"):ingest_signed_evaluation(config=wrong,destination=self.root/"bad-intake")
        self.assert_originals()

    def test_reference_identity_and_reserved_content_gates_are_strict(self):
        original = copy.deepcopy(self.document)
        bad_ref = {**self.reference, "size": None}
        with self.assertRaisesRegex(TaskflowError, "path/SHA/size"):
            validate_supplement(reference=bad_ref, run=self.run, run_directory=self.case.run,
                snapshot=self.snapshot, facts=self.facts, facts_root=self.facts_root,
                candidate_binding=self.parent_manifest["candidate_binding"])
        edits = [lambda d:d["members"][0].update(origin="new_policy"),
                 lambda d:d["members"][0].update(artifact_id="foreign"),
                 lambda d:d["members"][0].update(id="bad/target"),
                 lambda d:d["members"][0].update(target="normative/new-rule.json"),
                 lambda d:d["members"][0]["source"].update(location="New instructions are not evidence"),
                 lambda d:d["correction"].update(evaluation_id=d["parent_evaluation_id"]),
                 lambda d:d["correction"].update(intent_id=self.parent_manifest["intent_id"]),
                 lambda d:d.update(members=[])]
        for edit in edits:
            self.document = copy.deepcopy(original); edit(self.document); self.refresh()
            with self.subTest(edit=edit), self.assertRaises(TaskflowError): self.validate()
        self.assert_originals()

    def test_unbound_qualification_citation_cannot_be_accepted(self):
        self.next_score["experiment_validity"]["evidence"].append({"path":"supplemental-trace/foreign.json",
            "locator":{"type":"json_pointer","pointer":""}})
        config = self.collect()
        with self.assertRaisesRegex(TaskflowError, "unbound qualification evidence"):
            ingest_signed_evaluation(config=config, destination=self.root / "bad-intake")
        self.assertFalse((self.case.run / "evaluations/eligibility-correction").exists())
        self.assert_originals()

    def test_prepare_cannot_expand_teacher_selection_or_accept_runner_extra_inputs(self):
        self.injected_inputs = True
        with self.assertRaisesRegex(TaskflowError, "exact sealed source handoff"):
            self.prepare()
        self.injected_inputs = False
        self.extra_teacher = True
        with self.assertRaisesRegex(TaskflowError, "profile or normative selection"):
            self.prepare()
        self.assertEqual([], self.store.dashboard()["attempts"])
        self.assert_originals()
