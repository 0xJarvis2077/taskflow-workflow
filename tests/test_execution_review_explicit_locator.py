"""Explicit signed selections retain source text and validate the actual bytes.

Neutral files exercise the alias/run boundary; the managed fixture uses real
subprocess collection and same-SID facts sealing, with zero model calls.
"""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution_review as review
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_runner import _info
from tests import test_execution_review as fixtures


class ExplicitReviewLocatorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.target = self.root / "selection.json"
        self.facts = {"coverage": {"complete": True}, "evidence_inventory": [],
                      "facts": [{"id": "F01", "finding": "First neutral fact."},
                                {"id": "F02", "finding": "Second neutral fact."}]}
        write_json(self.target, self.facts)

    def resolve(self, locator, *, allow_facts=True):
        return review._review_explicit_locator(self.target, locator, expected=_info(self.target), allow_facts=allow_facts)[0]

    def test_all_pointers_and_unique_fact_ids_are_checked_without_rewriting_signed_text(self):
        locator = "JSON Pointers /coverage, /evidence_inventory; facts F02, F01"
        original = self.target.read_bytes()
        result = self.resolve(locator)
        self.assertEqual(["/coverage", "/evidence_inventory"], result["json_pointers"])
        self.assertEqual(["F02", "F01"], result["fact_ids"])
        self.assertEqual(["/facts/1", "/facts/0"], result["fact_pointers"])
        self.assertEqual(original, self.target.read_bytes())
        for raw, pointer in ((b'[{"x":1}]', "/0/x"), (b'{"a/b":{"x~y":1}}', "/a~1b/x~0y")):
            self.target.write_bytes(raw)
            self.assertEqual([pointer], self.resolve("JSON Pointers " + pointer)["json_pointers"])
        self.assertIsNone(review._review_explicit_locator(self.target, {"type": "json_pointer", "pointer": ""}, expected={}))
        self.assertIsNone(review._review_explicit_locator(self.target, "all 2 criteria", expected={}))

    def test_missing_duplicate_invalid_pointer_or_fact_cannot_be_guessed(self):
        invalid = ["JSON Pointers /coverage, /missing", "JSON Pointers /coverage, /coverage",
                   "JSON Pointers /coverage,/evidence_inventory", "JSON Pointers /bad~2token",
                   "JSON Pointers /coverage; facts F03", "JSON Pointers /coverage; facts F01, F01",
                   "JSON Pointers /coverage; facts F01 F02", "JSON Pointers /coverage; facts ",
                   "JSON Pointers /coverage; facts F01; facts F02"]
        for locator in invalid:
            with self.subTest(locator=locator), self.assertRaises(TaskflowError):self.resolve(locator)
        with self.assertRaises(TaskflowError):self.resolve("JSON Pointers /coverage; facts F01", allow_facts=False)
        for facts in (None, {}, [], [{"id": "F01"}, {"id": "F01"}], [{"fact_id": "F01"}], [{"id": 1}], [False]):
            write_json(self.target, {**self.facts, "facts": facts})
            with self.subTest(facts=facts), self.assertRaises(TaskflowError):self.resolve("JSON Pointers /coverage; facts F01")
        for body in (b'{"coverage":1,"coverage":2}', b'{"coverage":NaN}', b'null', b'{} {}'):
            self.target.write_bytes(body)
            with self.subTest(body=body), self.assertRaises(TaskflowError):self.resolve("JSON Pointers /coverage")

    def test_jsonl_explicit_ranges_complete_count_and_event_line(self):
        self.target.write_text('\n'.join(json.dumps(row) for row in (
            {"type": "item.completed"}, {"type": "item.completed"}, {"type": "turn.completed"})) + '\n')
        result = self.resolve("JSONL lines 1, 3")
        self.assertEqual([[1, 1], [3, 3]], result["line_ranges"])
        result = self.resolve("JSONL lines 1-3, especially final captures at lines 2-3")
        self.assertEqual([[2, 3]], result["highlighted_line_ranges"])
        result = self.resolve("all 3 JSONL records, including turn.completed at line 3")
        self.assertEqual(3, result["jsonl_record_count"])
        self.assertEqual("turn.completed", result["event_type"])
        invalid = ["JSONL lines 0", "JSONL lines 01", "JSONL lines 4", "JSONL lines 3-1",
                   "JSONL lines 1, 1", "JSONL lines 1-2, especially final captures at lines 2-3",
                   "JSONL lines 1-3, especially final captures at lines 3-2",
                   "all 2 JSONL records, including turn.completed at line 3",
                   "all 3 JSONL records, including turn.completed at line 2",
                   "all 4 JSONL records, including turn.completed at line 3",
                   "all 3 JSONL records, including arbitrary prose", "JSONL lines 1; anything"]
        for locator in invalid:
            with self.subTest(locator=locator), self.assertRaises(TaskflowError):self.resolve(locator)
        for raw in (b'{"type":1,"type":2}\n', b'[]\n', b'\n', b'{"x":Infinity}\n', b'\xff\n'):
            self.target.write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(TaskflowError):self.resolve("JSONL lines 1")

    def test_same_read_bytes_must_match_expected_sha_and_size(self):
        expected = _info(self.target)
        for key in ("sha256", "size"):
            changed = {**expected, key: "0" * 64 if key == "sha256" else expected["size"] + 1}
            with self.subTest(key=key), self.assertRaisesRegex(TaskflowError, "changed during locator"):
                review._review_explicit_locator(self.target, "JSON Pointers /coverage", expected=changed)

    def context(self):
        binding = {"run_digest": "r" * 64, "snapshot_digest": "s" * 64,
                   "input_digest": "i" * 64, "evaluation_digest": "e" * 64}
        self.run = {**binding, "artifacts": []}
        self.snapshot = {**binding, "files": []}
        definition = self.root / "definition.json"
        write_json(definition, {"document_kind": "local-profile-definition", "scope": "local_fixed_client_profile",
                               "execution_binding": binding, "actual_configuration": {}, "requirements": [], "judgment": {}})
        target = self.root / "inputs/local-profile-definition.json"
        target.parent.mkdir();target.write_bytes(definition.read_bytes())
        return {"cwd": target.parent, "manifest": {"inputs": [{"source": str(definition), "target": target.name,
                "kind": "trace", **_info(target)}]}, "local_profile_definition": {"path": str(definition), **_info(target)}}

    def test_registered_policy_alias_rejects_wrong_run_source_kind_or_byte_drift(self):
        context = self.context()
        locator = "JSON Pointers /execution_binding, /actual_configuration, /requirements, /judgment, /scope"
        result = review._review_source_alias("local-profile-definition.json", locator, context, self.run, self.snapshot)
        self.assertEqual(locator, result["locator"])
        self.assertEqual(5, len(result["json_pointers"]))
        for key in ("run_digest", "snapshot_digest", "input_digest", "evaluation_digest"):
            run, snapshot = copy.deepcopy(self.run), copy.deepcopy(self.snapshot)
            (run if key == "run_digest" else snapshot)[key] = "wrong"
            with self.subTest(binding=key), self.assertRaises(TaskflowError):
                review._review_source_alias("local-profile-definition.json", locator, context, run, snapshot)
        for key, value in (("kind", "teacher"), ("source", "/foreign/source"), ("sha256", "0" * 64), ("size", 1)):
            changed = copy.deepcopy(context);changed["manifest"]["inputs"][0][key] = value
            with self.subTest(field=key), self.assertRaises(TaskflowError):
                review._review_source_alias("local-profile-definition.json", locator, changed, self.run, self.snapshot)
        original_expect = review._expect
        def drift(path, expected):
            result = original_expect(path, expected)
            path.write_text('{"execution_binding":{},"scope":"wrong"}')
            return result
        with patch.object(review, "_expect", side_effect=drift), self.assertRaisesRegex(TaskflowError, "changed during locator"):
            review._review_source_alias("local-profile-definition.json", locator, context, self.run, self.snapshot)

    def test_direct_run_artifact_selection_cannot_escape_actual_run_mapping(self):
        context = self.context()
        source = context["cwd"] / "original/configuration.json"
        source.parent.mkdir();write_json(source, {"requested_model": "neutral", "served_model": None})
        entry = {"destination": str(source), "artifact_id": "actual_configuration", **_info(source)}
        context["plan"] = {"sources": [entry]}
        self.run["artifacts"] = [{"id": "actual_configuration", **_info(source)}]
        locator = "JSON Pointers /requested_model, /served_model"
        value = {"reason": "Only exact neutral source locators.", "evidence": [{"path": "original/configuration.json", "locator": locator}]}
        score = {"execution_binding": {key: self.run[key] for key in ("run_digest", "snapshot_digest", "input_digest", "evaluation_digest")},
            "experiment_validity": {**value, "local_profile": "current_registered_local_profile", "local_eligible": False,
                "served_identity_unknown_disclosed": True, "substantive_disputes": []},
            "artifact_status": {**value, "artifacts_complete": True}}
        original = copy.deepcopy(score)
        result = review._local_assessment(score, context, self.run, self.snapshot)
        self.assertTrue(result["signed_binding_valid"], result)
        self.assertEqual(2, len(result["evidence"]))
        self.assertEqual(["/requested_model", "/served_model"], result["evidence"][0]["json_pointers"])
        self.assertFalse(result["local_eligible"])
        for change in (lambda r: r["artifacts"][0].update(id="other-run"),
                       lambda r: r["artifacts"].append(copy.deepcopy(r["artifacts"][0])),
                       lambda r: r["artifacts"][0].update(sha256="0" * 64)):
            bad = copy.deepcopy(self.run);change(bad)
            self.assertFalse(review._local_assessment(score, context, bad, self.snapshot)["signed_binding_valid"])
        self.assertEqual(original, score)
        score["artifact_status"]["evidence"][0]["locator"] = "lines 1-1"
        self.assertTrue(review._local_assessment(score, context, self.run, self.snapshot)["signed_binding_valid"])
        score["artifact_status"]["evidence"][0]["locator"] = "lines 1-999"
        self.assertFalse(review._local_assessment(score, context, self.run, self.snapshot)["signed_binding_valid"])

    def test_managed_same_sid_facts_selection_preserves_original_score_and_every_reference(self):
        case = fixtures.CleanReviewPreparationTests("test_managed_collected_score_preserves_original_once_and_full_archives")
        case.setUp();self.addCleanup(case.doCleanups)
        original_facts = case.fixture_facts
        original_dumps = json.dumps
        def facts(*args, **kwargs):
            kwargs["payload"] = self.facts
            return original_facts(*args, **kwargs)
        locator = "JSON Pointers /coverage, /evidence_inventory; facts F01, F02"
        def serialize(value, *args, **kwargs):
            if isinstance(value, dict) and "total_score" in value and "criteria" in value:
                value["artifact_status"]["evidence"].append({"path": "sealed/facts.json", "locator": locator})
            return original_dumps(value, *args, **kwargs)
        with patch.object(case, "fixture_facts", side_effect=facts), patch.object(fixtures.json, "dumps", side_effect=serialize):
            config, score = case.fixture_managed_score(local_assessment=True)
        source = Path(config["score_attempt"]) / "artifacts/first-score.json"
        before = source.read_bytes()
        result = review.ingest_signed_evaluation(config=config, destination=case.root / "explicit-locator-adoption")
        self.assertTrue(result["accepted"])
        self.assertTrue(result["local_profile_assessment"]["signed_binding_valid"], result["local_profile_assessment"])
        row = result["local_profile_assessment"]["evidence"][-1]
        self.assertEqual(locator, row["locator"])
        self.assertEqual(["/facts/0", "/facts/1"], row["fact_pointers"])
        self.assertEqual(score, read_json(Path(result["evaluation"]) / "record.original.json")["signed_original_score"])
        self.assertEqual(before, source.read_bytes())


if __name__ == "__main__":
    unittest.main()
