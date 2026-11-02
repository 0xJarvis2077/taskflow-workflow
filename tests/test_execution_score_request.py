"""Future score instructions reach locked requests; no scoring CLI is started.

The fixture facts are written by the existing neutral subprocess collector.
Only the native permission/normal-tool and read-denial probes are replaced at
their explicit boundaries. Native request assembly, bundled CLI catalog reads,
source binding, SQLite enqueue and request/manifest locks remain real.
"""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution_experiment as experiment
from taskflow import execution_review as review
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_runner import _info, _validate
from taskflow.execution_store import evidence_ref
from taskflow.packages import verify_snapshot
from taskflow.records import verify_run
from tests import test_execution_review as fixtures


class FutureScoreRequestTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.CleanReviewPreparationTests(
            "test_score_prepared_inputs_are_only_normative_plus_scoring_projection")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        self.facts = case.fixture_facts(payload={"facts": [{"id": "F1", "observation": "Neutral complete original."}]})
        self.sealed = review.seal_existing_facts(config=self.facts)
        self.config = copy.deepcopy(case.clean_config)
        self.config.update(facts_attempt=self.facts["facts_attempt"], facts_binding=self.facts["review_binding"],
            scoring_source_allowlist=["draft/evaluation/answer.md", "draft/evaluation/rubric.md"])
        self.config["work"].update(role="review-score", work_id="canonical-score", intent_id="canonical-score-intent")
        self.config["runner"].update(role="review-score", intent_id="canonical-score-intent",
            outputs=["score.json", "explanation.md"], request_sections=self.sections())
        self.runtime = case.root / "canonical-score-runtime"

        # Supply a real bounded profile definition so all three JSON aliases
        # pass their own provenance checks before testing locator semantics.
        policy = copy.deepcopy(case.store.get_policy("review-fixture-v1")["config"])
        policy["roles"]["solver"].update(model="local-fixture", reasoning_effort="fixture", reasoning_context="all_turns")
        case.store.register_policy("canonical-score-policy", policy)
        authority = case.root / "canonical-score-authority.json"
        write_json(authority, {"controller": "fixture-controller", "task_id": "neutral",
            "formal": {"first_run_id": verify_run(case.run)["run_id"], "model": "local-fixture"}})
        reference = evidence_ref(authority, "neutral same-run local profile authority")
        self.config["work"].update(policy_id="canonical-score-policy", dispatch_owner="fixture-controller",
            evidence=[*self.config["work"]["evidence"], reference])
        self.config["runner"]["policy_version"] = "canonical-score-policy"
        self.config["local_profile_policy"] = {"schema_version": 1, "authorization": reference,
            "scope": "local_fixed_client_profile", "required_model": "local-fixture",
            "required_client_reasoning": {"effort": "fixture", "context": "all_turns"},
            "allow_served_identity_unknown": True, "requires_frozen_input_and_reference_match": True,
            "requires_complete_capture": True, "requires_no_substantive_dispute": True,
            "official_provider_identity_claimed": False}

    def sections(self):
        return experiment._score_request_sections({"run_path": str(self.case.run)}, self.case.candidate)

    @staticmethod
    def native_probe(runtime):
        report = {"passed": True, "model_calls": 0, "manifest": _info(runtime / "manifest.json"),
                  "nature": "mocked native OS probe boundary; no sandbox claim"}
        path = runtime / "native-probe-score-request-fixture.json"
        write_json(path, report)
        write_json(runtime / "native-probe-ready.json", {"manifest": report["manifest"],
                   "report_file": path.name, "report": _info(path)})
        return report

    @staticmethod
    def denied_probe(runtime, forbidden):
        report = {"model_calls": 0, "manifest": _info(runtime / "manifest.json"), "returncode": 0,
                  "nature": "mocked read-denial OS boundary; no permission-isolation claim",
                  "forbidden_reads_denied": [str(path) for path in forbidden]}
        write_json(runtime / "review-boundary-probe.json", report)
        return report

    def prepare(self):
        with patch("taskflow.execution_runner.probe_native_attempt", side_effect=self.native_probe) as native, \
                patch("taskflow.execution_review._denied_probe", side_effect=self.denied_probe) as denied, \
                patch("taskflow.execution.run_worker", side_effect=AssertionError("preparation cannot run a model")):
            result = review.prepare_score_runner(store=self.case.store, config=self.config, runtime=self.runtime)
        return result, native.call_count, denied.call_count

    def originals(self, result):
        paths = [self.runtime / name for name in ("request.md", "manifest.json", "manifest-lock.json",
                  "native-probe-ready.json", "native-probe-score-request-fixture.json", "review-binding.json",
                  "review-boundary-probe.json")]
        paths.extend([Path(result["review_binding"]), Path(self.facts["facts_attempt"]) / "facts-seal.json",
                      Path(self.facts["facts_attempt"]) / "artifacts/facts.json"])
        return {path: path.read_bytes() for path in paths}

    def test_future_helper_reaches_actual_native_request_and_manifest_lock(self):
        result, native, denied = self.prepare()
        self.assertEqual((1, 1), (native, denied))
        body = (self.runtime / "request.md").read_text()
        self.assertIn(self.config["runner"]["request_sections"][0]["text"], body)
        decoder, objects = json.JSONDecoder(), []
        for index, character in enumerate(body):
            if character == "{":
                try:
                    value, _ = decoder.raw_decode(body[index:])
                except ValueError:
                    continue
                if isinstance(value, dict) and "type" in value:
                    objects.append(value)
        for example in ({"type": "json_pointer", "pointer": "/criteria/0"},
                        {"type": "json_pointer", "pointer": ""},
                        {"type": "lines", "start": 1, "end": 3}):
            self.assertIn(example, objects)
        for field in ("public_evidence", "submission_evidence", "stage1_evidence",
                      "experiment_validity", "artifact_status", "reason", "locator"):
            self.assertIn(field, body)
        for rule in (
                "Every evidence list in criteria, experiment_validity and artifact_status uses the same typed locator contract",
                "one exact existing JSON Pointer", "including a root array",
                "/ denotes an empty object key, not the document root", "Escape ~ as ~0 and / within a key as ~1",
                "For UTF-8 text, CSV or JSONL", "existing inclusive 1-based range of integer line numbers",
                "The JSON aliases normative/task-scoring.json, sealed/facts.json and local-profile-definition.json require JSON Pointer locators",
                "Use these three aliases only in experiment_validity.evidence or artifact_status.evidence",
                "Criterion public_evidence and submission_evidence must cite the original locked first-stage sources",
                "typed locators do not expand source permissions",
                "stage1_evidence is retained in the signed score and does not replace required criterion public_evidence and submission_evidence",
                "exactly one pointer or one line range", "repeat the path in separate evidence items for multiple locations",
                "Put explanations in the existing reason field", "Do not combine multiple pointers with commas or and",
                "append prose to a locator", "No target scores or prior grades are supplied"):
            self.assertIn(rule, body)
        manifest = _validate(self.runtime)
        self.assertEqual("native_role", manifest["mode"])
        self.assertEqual(_info(self.runtime / "request.md"), manifest["request"])
        self.assertEqual(_info(self.runtime / "manifest.json"), read_json(self.runtime / "manifest-lock.json"))
        self.assertEqual(self.sealed["sid"], manifest["resume_sid"])
        self.assertEqual(self.sealed["seal"], manifest["facts_seal"])
        self.assertEqual("ready", result["work"]["state"])
        self.assertEqual([], self.case.store.dashboard()["attempts"])
        self.assertEqual({"--version", "--bundled"}, {
            command["argv"][-1] for command in read_json(self.runtime / "cli-evidence.json")["commands"]})

    def test_all_three_bound_json_aliases_accept_single_pointers_and_reject_lines(self):
        result, _, _ = self.prepare()
        manifest, binding = review._source_binding(self.runtime, Path(result["review_binding"]))
        context = {"manifest": manifest, "cwd": self.runtime / "inputs", "facts_seal": manifest["facts_seal"],
                   "local_profile_definition": binding["local_profile_definition"]}
        for path, pointer in (("normative/task-scoring.json", "/criteria/0"),
                              ("sealed/facts.json", "/facts/0"),
                              ("local-profile-definition.json", "/execution_binding")):
            for current in (pointer, ""):
                locator = {"type": "json_pointer", "pointer": current}
                with self.subTest(path=path, pointer=current):
                    value = review._review_source_alias(path, locator, context, verify_run(self.case.run),
                                                        verify_snapshot(self.case.snapshot))
                    self.assertEqual(current, value["json_pointer"])
                    self.assertEqual(locator, {"type": "json_pointer", "pointer": current})
            with self.subTest(path=path), self.assertRaises(TaskflowError):
                review._review_source_alias(path, {"type": "lines", "start": 1, "end": 1}, context,
                                            verify_run(self.case.run), verify_snapshot(self.case.snapshot))

    def test_typed_locations_roundtrip_real_files_without_splitting_literal_keys(self):
        source = self.case.root / "canonical.json"
        write_json(source, {"criteria": [{"id": "C1"}], "a/b": {"x~y": [11]}, "semi; comma,and": 12})
        for pointer in ("", "/criteria/0", "/a~1b/x~0y/0", "/semi; comma,and"):
            locator = {"type": "json_pointer", "pointer": pointer}
            before = copy.deepcopy(locator)
            self.assertEqual({"json_pointer": pointer}, review._review_locator(source, locator))
            self.assertEqual(before, locator)
        for pointer in ("/", "/criteria/0; /a~1b", "/missing", "/criteria/00"):
            with self.subTest(pointer=pointer), self.assertRaises(TaskflowError):
                review._review_locator(source, {"type": "json_pointer", "pointer": pointer})
        # '/' is a valid pointer to an explicitly present empty key, not root.
        write_json(source, {"": "an actual empty key"})
        self.assertEqual({"json_pointer": "/"}, review._review_locator(source, {"type": "json_pointer", "pointer": "/"}))
        for body in ("[1,2]\n", "null\n", "7\n"):
            source.write_text(body)
            self.assertEqual({"json_pointer": ""}, review._review_locator(source, {"type": "json_pointer", "pointer": ""}))
        for suffix, body in (("txt", "one\ntwo\nthree\n"), ("csv", "id,value\na,1\nb,2\n"),
                             ("jsonl", '{"a":1}\n{"a":2}\n{"a":3}\n')):
            target = self.case.root / ("ordinary." + suffix)
            target.write_text(body)
            self.assertEqual({"line_ranges": [[1, 3]]}, review._review_locator(
                target, {"type": "lines", "start": 1, "end": 3}))
            with self.assertRaises(TaskflowError):
                review._review_locator(target, {"type": "lines", "start": 1, "end": 4})

    def test_bound_local_alias_cannot_be_promoted_to_criterion_source_permission(self):
        result, _, _ = self.prepare()
        manifest, binding = review._source_binding(self.runtime, Path(result["review_binding"]))
        # Reconstruct the managed consumer's source classes from these actual
        # locked inputs. A trace input still needs a native run artifact match;
        # being readable is not enough to become criterion evidence.
        sources = [{"source": entry["source"], "destination": str(self.runtime / "inputs" / entry["target"]),
                    "sha256": entry["sha256"], "size": entry["size"]}
                   for entry in manifest["inputs"]
                   if entry["kind"] != "teacher" and entry["source"] != manifest["facts_seal"]["path"]]
        context = {"manifest": manifest, "cwd": self.runtime / "inputs", "facts_seal": manifest["facts_seal"],
                   "local_profile_definition": binding["local_profile_definition"],
                   "plan": {"sources": sources, "stage2_teacher": []}}
        run, snapshot = verify_run(self.case.run), verify_snapshot(self.case.snapshot)
        for path, pointer in (("normative/task-scoring.json", "/criteria/0"),
                              ("sealed/facts.json", "/facts/0"),
                              ("local-profile-definition.json", "/execution_binding")):
            locator = {"type": "json_pointer", "pointer": pointer}
            # The real alias-specific binding validates for local assessment.
            self.assertEqual(pointer, review._review_source_alias(path, locator, context, run, snapshot)["json_pointer"])
            evidence = {"path": path, "locator": locator}
            retained = {"stage1_evidence": [evidence]}
            self.assertEqual([], review._locators(retained, {}))
            self.assertEqual({"stage1_evidence": [evidence]}, retained)
            for field in ("public_evidence", "submission_evidence"):
                score = {"max_score": snapshot["scoring"]["total"], "criteria": [
                    {"id": row["id"], "max_score": row["max_score"], "score": 0,
                     "reason": "Neutral boundary test; no scoring or business finding.", field: [copy.deepcopy(evidence)]}
                    for row in snapshot["scoring"]["criteria"]]}
                original = copy.deepcopy(score)
                with self.subTest(path=path, field=field), self.assertRaisesRegex(
                        TaskflowError, "not a locked first-stage source|no unique same-byte native run artifact"):
                    review._project(score, context, {"run_directory": str(self.case.run)}, run, snapshot)
                self.assertEqual(original, score)

    def test_distinct_locations_use_distinct_entries_and_explanation_stays_in_reason(self):
        source = self.case.root / "two-locations.json"
        write_json(source, {"first": 1, "second": 2})
        entries = [{"path": source.name, "locator": {"type": "json_pointer", "pointer": pointer}}
                   for pointer in ("/first", "/second")]
        criterion = {"reason": "Both explicit observations support the same conclusion.", "public_evidence": entries}
        original = copy.deepcopy(criterion)
        locators = review._locators(criterion, {})
        self.assertEqual(2, len(locators))
        self.assertEqual([{"json_pointer": "/first"}, {"json_pointer": "/second"}],
                         [review._review_locator(self.case.root / path, locator) for path, locator in locators])
        self.assertEqual(original, criterion)
        for invalid in ({"type": "json_pointer", "pointer": "/first", "comment": "because"},
                        {"type": "json_pointer", "pointer": ["/first", "/second"]},
                        {"type": "lines", "start": True, "end": 3},
                        {"type": "lines", "start": 1, "end": 3, "reason": "not a location"}):
            with self.subTest(invalid=invalid), self.assertRaises(TaskflowError):
                review._normalized_locator(invalid)

    def test_existing_queued_work_reuses_old_locked_request_after_future_text_changes(self):
        result, _, _ = self.prepare()
        originals = self.originals(result)
        work, attempts = copy.deepcopy(self.case.store.dashboard()["work"]), self.case.store.dashboard()["attempts"]
        self.config["runner"]["request_sections"] = [{"kind": "operation", "text": "FUTURE_REQUEST_MUST_NOT_REPLACE_OLD_BYTES"}]
        again, native, denied = self.prepare()
        self.assertEqual(result, again)
        self.assertEqual((0, 0), (native, denied))
        self.assertEqual(work, self.case.store.dashboard()["work"])
        self.assertEqual(attempts, self.case.store.dashboard()["attempts"])
        self.assertEqual(originals, {path: path.read_bytes() for path in originals})
        self.assertNotIn("FUTURE_REQUEST_MUST_NOT_REPLACE_OLD_BYTES", (self.runtime / "request.md").read_text())

    def test_prepared_without_db_work_fails_closed_on_new_sections_and_keeps_old_bytes(self):
        with patch.object(self.case.store, "enqueue", side_effect=TaskflowError("neutral interruption before DB enqueue")):
            with self.assertRaisesRegex(TaskflowError, "neutral interruption"):
                self.prepare()
        self.assertEqual([], self.case.store.dashboard()["work"])
        binding = self.runtime.parent / (self.runtime.name + "-review") / "review-binding.json"
        originals = self.originals({"review_binding": str(binding)})
        self.config["runner"]["request_sections"] = [{"kind": "operation", "text": "NEW_FUTURE_TEXT_AFTER_PREPARATION"}]
        with self.assertRaisesRegex(TaskflowError, "prepared runtime belongs to a different continuation configuration"):
            self.prepare()
        self.assertEqual(originals, {path: path.read_bytes() for path in originals})
        self.assertEqual([], self.case.store.dashboard()["work"])
        self.assertEqual([], self.case.store.dashboard()["attempts"])


if __name__ == "__main__":
    unittest.main()
