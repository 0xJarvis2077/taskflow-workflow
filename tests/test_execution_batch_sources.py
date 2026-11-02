"""Original confirmed batch sources, with no external model calls.

Native source metadata uses the existing explicitly native-shaped schema fixture;
managed review intake uses real neutral facts/score subprocesses. Neither proves
real provider identity or OS permissions. Preparation probes/catalog are mocked
only at their explicit zero-model infrastructure boundaries.
"""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow import execution_formal as formal, execution_review as review, execution_runner as runner
from taskflow.execution_batch import CLI_ROLES, confirmation_digests, freeze_batch_plan, load_batch_snapshot, role_config
from taskflow.execution_batch_binding import make_batch_binding
from tests import test_execution_batch as batch_fixtures
from tests import test_execution_formal as formal_fixtures
from tests import test_execution_normative_certification as certificate_fixtures
from tests import test_execution_review as review_fixtures
from tests import test_execution_score_request as request_fixtures
from tests import test_execution_native_prior_findings as prior_fixtures
from tests import test_execution_formal_supplement as supplement_fixtures
from tools.execution_simulation import fixture_policy


def original_policy(root, *, task_id="neutral", policy_id="batch-original", batch_id="neutral-batch", mode="test_fixture"):
    root = Path(root)
    plan, path = batch_fixtures.make_confirmed_plan(root, mode=mode)
    plan.update(task_ids=[task_id], batch_id=batch_id)
    if mode == "real_cli":
        for role in plan["roles"].values():
            if role.get("enabled") and role["runtime"]["carrier"] == "cli":
                role["runtime"]["reasoning_context"] = None
    expected = confirmation_digests(plan)
    for binding in plan["role_bindings"].values():
        target = Path(binding["configuration_evidence"]["path"])
        value = read_json(target)
        value["configuration"]["scope"] = expected["scope"]
        write_json(target, value)
        binding["configuration_evidence"] = batch_fixtures.file_ref(target)
    choice = Path(plan["user_model_confirmation"]["source"]["path"])
    value = read_json(choice)
    value["structured_selection"] = expected["expanded_model_map"]
    write_json(choice, value)
    plan["user_model_confirmation"] = {"origin": "direct_user", "source": batch_fixtures.file_ref(choice), **expected}
    write_json(path, plan)
    frozen = freeze_batch_plan(path, root / "frozen")
    snapshot = load_batch_snapshot(frozen["snapshot"])
    config = {**fixture_policy(), "model_confirmation_required": True, "batch_snapshot": frozen["snapshot"],
        "model_slots": {key: role_config(snapshot, key) for key in CLI_ROLES if snapshot["roles"][key]["enabled"]}}
    return {"policy_id": policy_id, "digest": sha256(canonical(config)), "config": config}


def bind_manifest(manifest, policy, slot):
    manifest = copy.deepcopy(manifest)
    binding = make_batch_binding(policy, manifest["role"], slot, manifest["candidate_binding"]["task_id"])
    setting = binding["setting"]
    manifest.update(policy_version=policy["policy_id"], model_slot=slot, batch_binding=binding,
                    timeout_seconds=setting["timeout_seconds"], reasoning_selection=setting["reasoning_selection"],
                    reasoning_context=setting["reasoning_context"])
    manifest["model"].update(requested=setting["model"], reasoning_effort=setting["reasoning_effort"])
    if manifest["mode"] != "test_fixture":manifest["argv"] = [setting["cli"], "exec"]
    return manifest


class BatchSourceTests(unittest.TestCase):
    def source(self):
        case = formal_fixtures.NativeQualityHandoffTests(
            "test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic")
        case.setUp()
        self.addCleanup(case.doCleanups)
        policy = original_policy(case.root / "batch", mode="real_cli")
        case.store.get_policy = lambda identity: policy if identity == policy["policy_id"] else self.fail("latest policy lookup")
        self.bind_source(case, case.material, policy, "quality")
        return case, policy

    @staticmethod
    def bind_source(case, root, policy, slot):
        manifest = bind_manifest(read_json(root / "manifest.json"), policy, slot)
        write_json(root / "manifest.json", manifest)
        write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
        case.attempts[root.name].update(policy_id=policy["policy_id"], policy_digest=policy["digest"],
                                      task_id=case.candidate["task_id"], model_slot=slot)
        for name in ("collection.json", "collection-lock.json"):(root / name).unlink(missing_ok=True)
        case.seal(root)

    def test_confirmed_quality_and_recheck_use_their_original_model_slots(self):
        case, policy = self.source()
        first = formal._native_quality_source(case.store, case.candidate, "native-quality", coverage="whole_package", outcome="accepted")
        self.assertEqual("other-provider/quality", first["manifest"]["model"]["requested"])
        self.assertEqual("deep", first["manifest"]["model"]["reasoning_effort"])
        root = case.source(case.candidate, "new-recheck", coverage="affected", outcome="accepted")
        self.bind_source(case, root, policy, "recheck")
        current = formal._native_quality_source(case.store, case.candidate, "new-recheck", coverage="affected", outcome="accepted")
        self.assertEqual("other-provider/recheck", current["manifest"]["model"]["requested"])
        self.assertNotEqual(first["manifest"]["model"]["requested"], current["manifest"]["model"]["requested"])

    def test_public_slot_wrong_model_task_or_attempt_policy_cannot_sign_quality(self):
        for variant in ("public", "scope", "model", "task", "policy"):
            with self.subTest(variant=variant):
                case, policy = self.source()
                root = case.material
                if variant in {"public", "scope"}:self.bind_source(case, root, policy, "public_rebuild" if variant == "public" else "recheck")
                elif variant == "policy":case.attempts[root.name]["policy_digest"] = "0" * 64
                else:
                    manifest = read_json(root / "manifest.json")
                    if variant == "model":manifest["model"]["requested"] = "unconfirmed-provider/model"
                    else:manifest["batch_binding"]["task_id"] = "another-task"
                    write_json(root / "manifest.json", manifest)
                    write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
                    for name in ("collection.json", "collection-lock.json"):(root / name).unlink()
                    case.seal(root)
                with self.assertRaises(TaskflowError):
                    formal._native_quality_source(case.store, case.candidate, "native-quality", coverage="whole_package", outcome="accepted")

    def test_offline_certificate_uses_original_snapshot_without_latest_database_policy(self):
        case, policy = self.source()
        helper = certificate_fixtures.NormativeCertificationTests()
        helper.complete_source(case, case.material)
        certification = helper.certification(case)
        case.store.get_policy = lambda identity: self.fail("offline certificate consulted a policy database")
        selected = formal.validate_normative_certification(candidate=case.candidate, certification=certification)
        self.assertEqual(certification["native_quality_handoff"]["normative_sources"], selected)
        manifest = read_json(case.material / "manifest.json")
        snapshot = Path(manifest["batch_binding"]["policy_config"]["batch_snapshot"]["path"])
        snapshot.write_bytes(snapshot.read_bytes() + b"\n")
        with self.assertRaises(TaskflowError):
            formal.validate_normative_certification(candidate=case.candidate, certification=certification)

    def test_prior_whole_and_affected_batch_sources_keep_offline_repaired_scope(self):
        case, graph, old, fresh = prior_fixtures.prior_findings_fixture(self)
        policy = original_policy(case.root / "original-batch", mode="real_cli")
        case.store.get_policy = lambda identity: policy if identity == policy["policy_id"] else self.fail("foreign policy")
        self.bind_source(case, old, policy, "quality")
        self.bind_source(case, fresh, policy, "recheck")
        helper = certificate_fixtures.NormativeCertificationTests()
        helper.complete_source(case, old)
        helper.complete_source(case, fresh)
        certification = helper.certification(case)
        selected = formal.validate_normative_certification(candidate=case.candidate, certification=certification)
        repaired = certification["native_quality_handoff"]["protocol_map"]["repaired_candidate_acceptance"]
        self.assertFalse(repaired["prior_whole_findings"]["original_dimensions"]["business"]["affirmed"])
        self.assertEqual("affected", repaired["current_affected_closure"]["scope"])
        self.assertEqual(certification["native_quality_handoff"]["normative_sources"], selected)

    def test_supplement_uses_recheck_slot_without_replacing_legacy_sources(self):
        case = supplement_fixtures.FormalSupplementTests(
            "test_three_original_sources_project_and_release_without_scope_upgrade")
        case.setUp();self.addCleanup(case.doCleanups)
        policy = original_policy(case.root / "batch", mode="real_cli")
        manifest = bind_manifest(read_json(case.runtime / "manifest.json"), policy, "recheck")
        write_json(case.runtime / "manifest.json", manifest)
        case.attempt.update(policy_id=policy["policy_id"], policy_digest=policy["digest"], task_id="neutral", model_slot="recheck")
        case.store.get_policy = lambda identity: policy
        case.seal()
        # This existing supplement fixture explicitly doubles only _validate;
        # its actual source/collection/SID/reference checks remain enabled.
        with patch("taskflow.execution_formal._validate", side_effect=lambda root: read_json(Path(root) / "manifest.json")):
            source = formal._supplement_quality_source(case.store, case.candidate, case.graph)
        self.assertEqual(case.attempt["attempt_id"], source["attempt"]["attempt_id"])
        self.assertEqual("other-provider/recheck", manifest["model"]["requested"])

    def test_legacy_sources_keep_original_sol_guard(self):
        case = formal_fixtures.NativeQualityHandoffTests(
            "test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic")
        case.setUp();self.addCleanup(case.doCleanups)
        self.assertEqual(case.handoff(), case.handoff())
        manifest = read_json(case.material / "manifest.json")
        manifest["model"]["requested"] = "other-provider/quality"
        write_json(case.material / "manifest.json", manifest)
        write_json(case.material / "manifest-lock.json", runner._info(case.material / "manifest.json"))
        for name in ("collection.json", "collection-lock.json"):(case.material / name).unlink()
        case.seal(case.material)
        with self.assertRaisesRegex(TaskflowError, "Sol/xhigh"):
            formal._native_quality_source(case.store, case.candidate, "native-quality", coverage="whole_package", outcome="accepted")

    def review_case(self, *, mode="test_fixture"):
        case = review_fixtures.CleanReviewPreparationTests(
            "test_managed_collected_score_preserves_original_once_and_full_archives")
        case.setUp();self.addCleanup(case.doCleanups)
        return case, original_policy(case.root / "batch", mode=mode)

    def managed(self, case, policy, score_policy=None):
        original_prepare = review_fixtures.prepare_attempt
        def prepare(root, config):
            config = copy.deepcopy(config)
            current = score_policy if config["role"] == "review-score" and score_policy else policy
            binding = make_batch_binding(current, config["role"], "reviewer", "neutral")
            setting = binding["setting"]
            config.update(policy_version=current["policy_id"], model_slot="reviewer", batch_binding=binding,
                          model=setting["model"], reasoning_effort=setting["reasoning_effort"],
                          reasoning_selection=setting["reasoning_selection"], reasoning_context=setting["reasoning_context"],
                          timeout_seconds=setting["timeout_seconds"])
            return original_prepare(root, config)
        with patch("tests.test_execution_review.prepare_attempt", side_effect=prepare):
            return case.fixture_managed_score()

    def test_managed_same_sid_facts_and_score_accept_the_original_confirmed_reviewer(self):
        case, policy = self.review_case()
        config, score = self.managed(case, policy)
        facts_manifest = runner._validate(Path(config["facts_attempt"]))
        score_manifest = runner._validate(Path(config["score_attempt"]))
        self.assertTrue(review._review_batch_pair(facts_manifest, score_manifest))
        self.assertEqual("reviewer", facts_manifest["model_slot"])
        result = review.ingest_signed_evaluation(config=config, destination=case.root / "batch-intake")
        self.assertTrue(result["accepted"])
        original = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(score, original["signed_original_score"])
        self.assertIn("other-provider/reviewer/deep", original["evaluator"])
        self.assertEqual(result, review.ingest_signed_evaluation(config=config, destination=case.root / "batch-intake"))

    def test_managed_review_rejects_a_new_batch_even_with_the_same_model_and_sid(self):
        case, policy = self.review_case()
        later = original_policy(case.root / "later", policy_id="later-policy", batch_id="later-batch")
        with self.assertRaisesRegex(TaskflowError, "same original batch"):
            self.managed(case, policy, later)
        self.assertFalse((case.root / "managed-score-attempt/manifest.json").exists())

    def test_batch_pair_rejects_mixed_legacy_wrong_slot_and_changed_reasoning(self):
        case, policy = self.review_case()
        config, _ = self.managed(case, policy)
        facts, score = runner._validate(Path(config["facts_attempt"])), runner._validate(Path(config["score_attempt"]))
        self.assertTrue(review._review_batch_pair(facts, score))
        for variant in ("mixed", "slot", "reason", "phase"):
            changed = copy.deepcopy(score)
            if variant == "mixed":changed.pop("batch_binding");changed.pop("model_slot")
            elif variant == "slot":changed["model_slot"] = "quality"
            elif variant == "reason":changed["reasoning_context"] = "different-context"
            else:changed["role"] = "review-facts"
            with self.subTest(variant=variant), self.assertRaises(TaskflowError):review._review_batch_pair(facts, changed)

    @staticmethod
    def config(case, policy, role):
        config = copy.deepcopy(case.clean_config)
        setting = make_batch_binding(policy, role, "reviewer", "neutral")["setting"]
        suffix = "facts" if role == "review-facts" else "score"
        config["work"].update(policy_id=policy["policy_id"], role=role, work_id="batch-" + suffix, intent_id="batch-" + suffix)
        config["runner"].update(policy_version=policy["policy_id"], role=role, intent_id="batch-" + suffix,
            model=setting["model"], reasoning_effort=setting["reasoning_effort"], timeout_seconds=setting["timeout_seconds"],
            cli=setting["cli"], outputs=["facts.json"] if suffix == "facts" else ["first-score.json"])
        return config

    @staticmethod
    def catalog(root, cli):
        payload = {"models": [{"slug": "other-provider/reviewer", "supported_reasoning_levels": [{"effort": "deep"}]}]}
        evidence = {"model_calls": 0, "commands": [
            {"argv": [cli, "--version"], "returncode": 0, "stdout": "neutral fake catalog boundary"},
            {"argv": [cli, "--no-daemon", "debug", "models", "--bundled"], "returncode": 0, "stdout": json.dumps(payload)}]}
        runner._publish(root / "cli-evidence.json", evidence)
        return evidence

    def prepare(self, case, config, runtime, *, score=False):
        with patch("tools.isolated_quality.capture_cli_evidence", side_effect=self.catalog), \
                patch("taskflow.execution_runner.probe_native_attempt", side_effect=request_fixtures.FutureScoreRequestTests.native_probe), \
                patch("taskflow.execution_review._denied_probe", side_effect=request_fixtures.FutureScoreRequestTests.denied_probe), \
                patch("taskflow.execution.run_worker", side_effect=AssertionError("preparation must not start a model")):
            function = review.prepare_score_runner if score else review.prepare_facts_runner
            return function(store=case.store, config=config, runtime=runtime)

    def test_prepare_facts_binds_reviewer_without_rewriting_explicit_model_drift(self):
        case, policy = self.review_case(mode="real_cli")
        case.store.register_policy(policy["policy_id"], policy["config"])
        config = self.config(case, policy, "review-facts")
        wrong = copy.deepcopy(config);wrong["runner"]["model"] = "gpt-6.1-sol"
        with self.assertRaisesRegex(TaskflowError, "actual runner model"):
            self.prepare(case, wrong, case.root / "wrong-facts")
        result = self.prepare(case, config, case.root / "batch-facts-runtime")
        manifest = runner._validate(Path(result["runtime"]))
        self.assertEqual("reviewer", result["work"]["model_slot"])
        self.assertEqual("other-provider/reviewer", manifest["model"]["requested"])
        self.assertEqual(policy["digest"], manifest["batch_binding"]["policy_digest"])
        self.assertEqual(result, self.prepare(case, config, Path(result["runtime"])))
        self.assertEqual([], case.store.dashboard()["attempts"])

    def test_prepare_score_uses_the_original_reviewer_and_sealed_sid(self):
        case, policy = self.review_case(mode="real_cli")
        case.store.register_policy(policy["policy_id"], policy["config"])
        source, _ = self.managed(case, policy)
        config = self.config(case, policy, "review-score")
        config.update(facts_attempt=source["facts_attempt"], facts_binding=source["facts_binding"],
                      scoring_source_allowlist=["draft/evaluation/answer.md", "draft/evaluation/rubric.md"])
        result = self.prepare(case, config, case.root / "batch-score-runtime", score=True)
        manifest = runner._validate(Path(result["runtime"]))
        self.assertEqual("reviewer", manifest["model_slot"])
        self.assertEqual(review_fixtures.SID, manifest["resume_sid"])
        self.assertEqual(policy["digest"], manifest["batch_binding"]["policy_digest"])
        self.assertEqual([], case.store.dashboard()["attempts"])


if __name__ == '__main__':
    unittest.main()
