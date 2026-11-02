"""Typed signed locators use neutral sealed managed reviews, never real scores."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow import execution_review
from taskflow.contracts import TaskflowError, read_json
from taskflow.records import verify_evaluation
from tests import test_execution_review as fixture


class TypedReviewLocatorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()

    def managed(self, mutate, *, local_assessment=False):
        # Compose the fixture helper instead of inheriting its entire test suite.
        case = fixture.CleanReviewPreparationTests(
            "test_managed_collected_score_preserves_original_once_and_full_archives")
        case.setUp()
        self.addCleanup(case.doCleanups)
        original_dumps = json.dumps
        observed = []

        def serialize(value, *args, **kwargs):
            if isinstance(value, dict) and "total_score" in value and "criteria" in value:
                mutate(value)
                observed.append(copy.deepcopy(value))
            return original_dumps(value, *args, **kwargs)

        # Change only the new neutral score before its actual subprocess writes
        # and seals it. No collected file, hash, original or lock is rewritten.
        with patch.object(fixture.json, "dumps", side_effect=serialize):
            config, score = case.fixture_managed_score(local_assessment=local_assessment)
        self.assertEqual([score], observed)
        return case, config, score

    def test_syntax_normalization_preserves_raw_objects_and_legacy_strings(self):
        values = [({"type": "lines", "start": 7, "end": 9}, "lines 7-9"),
                  ({"type": "lines", "start": 1, "end": 1}, "lines 1-1"),
                  ({"type": "json_pointer", "pointer": ""}, "json_pointer:"),
                  ({"type": "json_pointer", "pointer": "/a~1b/x~0y/0"}, "json_pointer:/a~1b/x~0y/0")]
        for raw, normalized in values:
            before = copy.deepcopy(raw)
            with self.subTest(raw=raw):
                self.assertEqual(normalized, execution_review._normalized_locator(raw))
                self.assertEqual(before, raw)
                row = {"public_evidence": [{"path": "current.json", "locator": raw}]}
                self.assertEqual([("current.json", before)], execution_review._locators(row, {}))
        for value in ("1-2", "第1行", "/scope; /execution_binding"):
            self.assertEqual(value, execution_review._normalized_locator(value))

    def test_malformed_typed_locators_are_rejected_without_coercion_or_unknown_fields(self):
        values = [None, [], {}, 1, "", " ",
                  {"type": "lines", "start": True, "end": 2},
                  {"type": "lines", "start": 1, "end": False},
                  {"type": "lines", "start": 1.0, "end": 2},
                  {"type": "lines", "start": "1", "end": 2},
                  {"type": "lines", "start": 0, "end": 2},
                  {"type": "lines", "start": 2, "end": 1},
                  {"type": "lines", "start": 1},
                  {"type": "lines", "start": 1, "end": 2, "extra": "ignored?"},
                  {"type": "json_pointer", "pointer": None},
                  {"type": "json_pointer", "pointer": "value"},
                  {"type": "json_pointer", "pointer": "#/value"},
                  {"type": "json_pointer", "pointer": "/bad~2escape"},
                  {"type": "json_pointer", "pointer": "/trailing~"},
                  {"type": "json_pointer", "pointer": "/value", "extra": True},
                  {"type": "other", "pointer": "/value"}]
        for value in values:
            with self.subTest(value=value), self.assertRaises(TaskflowError):
                execution_review._normalized_locator(value)

    def test_actual_bound_files_enforce_lines_json_nodes_and_single_pointer_semantics(self):
        text = self.root / "source.txt"
        text.write_text("first\nsecond\n")
        self.assertEqual({"line_ranges": [[1, 2]]}, execution_review._review_locator(
            text, {"type": "lines", "start": 1, "end": 2}))
        with self.assertRaises(TaskflowError):
            execution_review._review_locator(text, {"type": "lines", "start": 1, "end": 3})
        source = self.root / "source.json"
        source.write_text('{"a/b":{"x~y":[11]},"a; /b":23}')
        for pointer in ("", "/a~1b/x~0y/0", "/a; ~1b"):
            self.assertEqual({"json_pointer": pointer}, execution_review._review_locator(
                source, {"type": "json_pointer", "pointer": pointer}))
        for pointer in ("/missing", "/a~1b/x~0y/1", "/a~1b/x~0y/01", "/a~1b/x~0y/0/nope"):
            with self.subTest(pointer=pointer), self.assertRaises(TaskflowError):
                execution_review._review_locator(source, {"type": "json_pointer", "pointer": pointer})
        for body, pointer in (('[{"value":1}]', "/0/value"), ('null', ""), ('7', "")):
            source.write_text(body)
            self.assertEqual({"json_pointer": pointer}, execution_review._review_locator(
                source, {"type": "json_pointer", "pointer": pointer}))
        for body in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', '{} {}'):
            source.write_text(body)
            with self.subTest(body=body), self.assertRaises(TaskflowError):
                execution_review._review_locator(source, {"type": "json_pointer", "pointer": ""})

    def test_managed_ingest_preserves_typed_original_and_records_exact_projection_once(self):
        def typed(score):
            row = score["criteria"][0]
            row["public_evidence"][0]["locator"] = {"type": "lines", "start": 1, "end": 1}
            row["submission_evidence"][0]["locator"] = {"type": "lines", "start": 1, "end": 2}
            row["final_submission_evidence"] = [{"path": "original/actual_configuration/actual-configuration.json",
                "locator": {"type": "json_pointer", "pointer": "/requested_model"}}]
            score["experiment_validity"]["evidence"][0]["locator"] = {"type": "json_pointer", "pointer": "/requested_model"}
            score["artifact_status"]["evidence"][0]["locator"] = {"type": "lines", "start": 1, "end": 2}

        case, config, score = self.managed(typed, local_assessment=True)
        source = Path(config["score_attempt"]) / "artifacts/first-score.json"
        original = source.read_bytes()
        destination = case.root / "typed-locator-adoption"
        result = execution_review.ingest_signed_evaluation(config=config, destination=destination)
        self.assertTrue(result["accepted"])
        record = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(score, record["signed_original_score"])
        self.assertEqual(score["criteria"][0], record["items"][0]["signed_original_item"])
        self.assertEqual(score["total_score"], record["total"])
        bindings = read_json(destination / "evaluation-transport-verification.json")["bindings"]
        self.assertEqual([{"type": "lines", "start": 1, "end": 1},
                          {"type": "lines", "start": 1, "end": 2},
                          {"type": "json_pointer", "pointer": "/requested_model"}],
                         [row["signed_locator"] for row in bindings])
        self.assertEqual(["lines 1-1", "lines 1-2", "json_pointer:/requested_model"],
                         [row["projected_locator"] for row in bindings])
        self.assertEqual({"line_ranges": [[1, 2]]}, bindings[1]["location_validation"])
        self.assertEqual({"json_pointer": "/requested_model"}, bindings[2]["location_validation"])
        self.assertTrue(result["local_profile_assessment"]["signed_binding_valid"])
        self.assertEqual([], verify_evaluation(Path(result["evaluation"]))["validation_issues"])
        self.assertEqual(result, execution_review.ingest_signed_evaluation(config=config, destination=destination))
        self.assertEqual(original, source.read_bytes())

    def test_managed_invalid_typed_locator_cannot_create_native_original(self):
        for locator in ({"type": "lines", "start": 1, "end": 99},
                        {"type": "json_pointer", "pointer": "/missing"},
                        {"type": "lines", "start": True, "end": 1}):
            def invalid(score):
                score["criteria"][0]["submission_evidence"] = [{
                    "path": "original/actual_configuration/actual-configuration.json",
                    "locator": copy.deepcopy(locator)}]
            case, config, score = self.managed(invalid)
            with self.subTest(locator=locator), self.assertRaises(TaskflowError):
                execution_review.ingest_signed_evaluation(config=config, destination=case.root / "invalid-adoption")
            self.assertFalse((case.run / "evaluations/managed-original").exists())
            self.assertEqual(score, read_json(Path(config["score_attempt"]) / "artifacts/first-score.json"))

    def test_invalid_local_typed_pointer_preserves_score_but_cannot_sign_local_binding(self):
        def invalid_local(score):
            score["experiment_validity"]["evidence"][0]["locator"] = {"type": "json_pointer", "pointer": "/missing"}
        case, config, score = self.managed(invalid_local, local_assessment=True)
        result = execution_review.ingest_signed_evaluation(config=config, destination=case.root / "local-invalid-adoption")
        self.assertTrue(result["accepted"])
        self.assertFalse(result["local_profile_assessment"]["signed_binding_valid"])
        record = read_json(Path(result["evaluation"]) / "record.original.json")
        self.assertEqual(score, record["signed_original_score"])
        self.assertEqual(score["total_score"], record["total"])


if __name__ == "__main__":
    unittest.main()
