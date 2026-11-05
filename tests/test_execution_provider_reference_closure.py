"""Provider metadata boundaries and zero-model accepted-preparation recovery.

Native/batch records are explicit metadata fixtures, never provider executions.
The closing queue uses existing test_fixture subprocesses. Native report and
batch attachment adapters are mocked only in that synthetic queue fixture;
separate tests exercise the real provider and batch validators with actual files.
"""

import copy
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_batch import CLI_ROLES, confirmation_digests, freeze_batch_plan, role_config
from taskflow.execution_batch_binding import make_batch_binding, validate_batch_runner
from taskflow.execution_codex_provider import provider_references_equal
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_review import _review_batch_pair
from taskflow.execution_store import evidence_ref
from tests import test_execution_batch as batch_fixtures
from tests import test_execution_codex_provider as provider_fixtures
from tests import test_execution_external_formal_closure as closure_fixtures
from tests import test_execution_prepared_provider_recovery as prepared_fixtures


class ProviderReferenceBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.provider = provider_fixtures.CodexProviderTests()
        self.provider.setUp()
        self.addCleanup(self.provider.doCleanups)
        self.root = self.provider.root
        plan, path = batch_fixtures.make_confirmed_plan(self.root / "batch", mode="real_cli")
        self.refs = {}
        for slot in ("solver", "reviewer"):
            model = plan["roles"][slot]["model"]
            catalog = self.root / (slot + "-catalog.json")
            write_json(catalog, {"models": [{"slug": model, "tool_mode": "direct",
                                            "supported_reasoning_levels": [{"effort": "deep"}]}]})
            selected = {**self.provider.selected, "model": model, "reasoning_effort": "deep",
                        "catalog": batch_fixtures.file_ref(catalog)}
            source = self.root / (slot + "-provider.json")
            write_json(source, selected)
            self.refs[slot] = {**batch_fixtures.file_ref(source), "location": "frozen selection"}
            plan["roles"][slot]["runtime"].update(
                provider="openai", cli=str(self.provider.case.script), reasoning_context=None,
                native_provider=self.refs[slot])
        plan["user_model_confirmation"].update(confirmation_digests(plan))
        write_json(path, plan)
        frozen = freeze_batch_plan(path, self.root / "frozen")
        config = {"concurrency": 1, "model_confirmation_required": True,
                  "batch_snapshot": frozen["snapshot"],
                  "model_slots": {key: role_config(frozen, key) for key in CLI_ROLES},
                  "roles": {"solver": role_config(frozen, "solver"),
                            "review-facts": role_config(frozen, "reviewer")}}
        self.policy = {"policy_id": "neutral-provider-metadata", "config": config,
                       "digest": sha256(canonical(config))}

    def value(self, role="solver", slot="solver"):
        binding = make_batch_binding(self.policy, role, slot, "neutral-task")
        value = {**binding["setting"], "batch_binding": binding, "role": role, "model_slot": slot,
                 "policy_version": self.policy["policy_id"], "mode": "native_role",
                 "candidate_binding": {"task_id": "neutral-task"},
                 "native_provider": {**self.refs[slot], "location": "runner provenance"}}
        return value

    def test_runner_manifest_and_reviewer_pair_accept_only_descriptive_difference(self):
        value = self.value()
        self.assertIsNotNone(validate_batch_runner(value))
        manifest = {**value, "argv": [value["cli"]], "model": {
            "requested": value["model"], "reasoning_effort": value["reasoning_effort"],
            "native_provider_ref": {**self.refs["solver"], "location": "captured plan provenance"}}}
        self.assertIsNotNone(validate_batch_runner(manifest, manifest=True))
        facts = self.value("review-facts", "reviewer")
        facts_manifest = {**facts, "argv": [facts["cli"]], "model": {
            "requested": facts["model"], "reasoning_effort": facts["reasoning_effort"],
            "native_provider_ref": {**self.refs["reviewer"], "location": "facts provenance"}}}
        score = self.value("review-score", "reviewer")
        score["facts_attempt"] = str(self.root / "facts-metadata-only")
        with patch("taskflow.execution_runner._validate", return_value=facts_manifest):
            self.assertTrue(_review_batch_pair(facts_manifest, score, manifest=False))
        changed = copy.deepcopy(score)
        changed["native_provider"] = self.refs["solver"]
        with self.assertRaises(TaskflowError):
            _review_batch_pair(facts_manifest, changed, manifest=False)

    def test_path_size_hash_extra_fields_policy_and_actual_bytes_never_become_equivalent(self):
        value = self.value()
        original_policy = copy.deepcopy(value["batch_binding"])
        alias = self.root / "same-bytes-other-path.json"
        alias.write_bytes(Path(value["native_provider"]["path"]).read_bytes())
        for edit in ({"path": str(alias)}, {"size": 1}, {"sha256": "0" * 64}, {"extra": "unbound"}):
            changed = copy.deepcopy(value)
            changed["native_provider"].update(edit)
            with self.subTest(edit=edit), self.assertRaises(TaskflowError):
                validate_batch_runner(changed)
            self.assertEqual(original_policy, value["batch_binding"])
        changed = copy.deepcopy(value)
        changed["batch_binding"]["setting"]["native_provider"]["location"] = "changed frozen setting"
        with self.assertRaises(TaskflowError):
            validate_batch_runner(changed)
        Path(value["native_provider"]["path"]).write_text("changed source", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            validate_batch_runner(value)

    def test_shared_reference_helper_checks_optional_size_and_malformed_sources(self):
        ref = self.refs["solver"]
        small = {key: value for key, value in ref.items() if key != "size"}
        self.assertTrue(provider_references_equal(small, {**small, "location": "other"}))
        self.assertFalse(provider_references_equal(small, ref))
        self.assertTrue(provider_references_equal(None, None))
        for malformed in (None, [], {}, {"path": ref["path"]}, {**ref, "size": False},
                          {"path": "/missing-provider.json", "sha256": "0" * 64}):
            with self.subTest(malformed=malformed):
                self.assertFalse(provider_references_equal(ref, malformed))


class AcceptedProviderRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.prepared = prepared_fixtures.PreparedProviderRecoveryTests()
        self.prepared.setUp()
        self.addCleanup(self.prepared.doCleanups)
        self.case = self.prepared.case
        self.store = self.case.store
        self.runtime = self.prepared.runtime
        normal = self.runtime / "normal-tools-probe.json"
        write_json(normal, {"model_calls": 0, "external_model_requests": 0,
                            "nature": "synthetic report adapter; not native capability"})
        self.normal_ref = evidence_ref(normal, "synthetic cached-report adapter")
        identity = read_json(self.runtime.with_name(self.runtime.name + ".formal-intent.json"))
        # Production _ref does not include descriptive location.
        normal_ref = {key: value for key, value in self.normal_ref.items() if key != "location"}
        normal_ref["size"] = normal.stat().st_size
        self.normal_ref = normal_ref
        write_json(self.runtime / "launch-review.json", {
            "approved": True, "plan_sha256": sha256((self.runtime / "execution-plan.json").read_bytes()),
            "input_digest": self.case.candidate["input_digest"], "preparation_binding": identity,
            "normal_tools": normal_ref})
        original = self.store.get_flow(self.prepared.record["flow_id"])
        exception = {"stage": "formal_prepare", "reason": "native Codex provider differs from confirmed batch"}
        write_json(self.prepared.receipt, {"flow_id": original["flow_id"], "status": "exception", "exception": exception})
        self.record = self.store.update_flow(original["flow_id"], original["revision"],
            {**original["state"], "exception": exception}, {"kind": "synthetic_accepted_batch_reference_error"})
        self.resolution = {**self.prepared.resolution, "action": "resume_accepted_formal_provider_metadata"}

    def resume(self, *, normal=None):
        with self.prepared.fixture.validate_authority(), patch("subprocess.run", return_value=
                subprocess.CompletedProcess([], 0, b"", b"")) as validation, \
             patch("taskflow.execution_formal._native_checks", return_value=normal or self.normal_ref) as cached, \
             patch("taskflow.execution_batch_binding.make_batch_binding", return_value={"model_slot": "solver"}), \
             patch("taskflow.execution_batch_binding.validate_batch_runner"):
            result = resume_path_transport(self.store, self.record["flow_id"], self.resolution)
            self.assertEqual(1, validation.call_count)
            self.assertNotIn("--execute", validation.call_args.args[0])
            cached.assert_called_once()
            return result

    def test_original_acceptance_and_plan_preserved_then_fixture_closes_once(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_missing_or_changed_cached_acceptance_and_report_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
