"""Neutral identity proofs; equal bytes never identify a named source."""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError
from taskflow.execution_review import (
    _managed_projection_config, _managed_projection_context, _received_source_map_config, _validate_named_source_map,
)


class NamedSourceMapProofTests(unittest.TestCase):
    def setUp(self):
        self.name = "draft/input/records/empty.json"
        self.origin = "capture/trees/work/" + self.name
        self.info = {"sha256": "a" * 64, "size": 3}
        self.config = {"source_path_map": {self.name: self.origin}}
        self.score = {"criteria": [{"public_evidence": [{"path": self.name, "locator": "/"}],
                                    "submission_evidence": [{"path": "original/answer.md", "locator": "1"}]}]}
        self.manifest = {"inputs": [
            {"target": self.name, "source": "/package/" + self.name, "kind": "public", **self.info},
            {"target": "original/answer.md", "source": "/incoming/answer.md", "kind": "submission", **self.info}]}
        self.facts_manifest = copy.deepcopy(self.manifest)
        self.binding = {"selector": {"path": "/incoming/indexes/first_review_index.json"}, "public_sources": [
            {"package_path": self.name, "path": "/package/" + self.name, "role": "input", **self.info}]}
        self.declaration = {"files": [{"path": self.name, "role": "input"}]}
        self.rows = [{"id": "empty-original", "incoming_path": self.origin, "visibility": "public", **self.info},
                     {"id": "same-bytes-other", "incoming_path": "capture/trees/work/other.json", "visibility": "public", **self.info},
                     {"id": "answer", "incoming_path": "answer.md", "visibility": "submission", **self.info}]
        self.run = {"artifacts": [{"id": row["id"], "source_path": row["incoming_path"], **self.info} for row in self.rows]}
        self.transport = {"all_signed_original_reasons_preserved": True, "root_business_rereview": False, "bindings": [
            {"signed_path": self.name, "signed_locator": "/", "artifact_id": "empty-original", "source_path": self.origin,
             "native_evidence": ["empty-original#/"], **self.info},
            {"signed_path": "original/answer.md", "signed_locator": "1", "artifact_id": "answer", "source_path": "answer.md",
             "native_evidence": ["answer#1"], **self.info}]}

    def validate(self):
        _validate_named_source_map(self.config, self.score, self.facts_manifest, self.manifest,
                                   self.binding, self.declaration, self.rows, self.run, self.transport)

    def test_exact_named_original_with_equal_byte_peer_is_valid(self):
        self.validate()
        self.score["criteria"][0]["public_evidence"][0]["locator"] = {"type": "lines", "start": 1, "end": 1}
        self.transport["bindings"][0].update(signed_locator={"type": "lines", "start": 1, "end": 1},
                                                native_evidence=["empty-original#lines 1-1"])
        self.validate()

    def test_unknown_empty_or_forged_map_is_rejected(self):
        for mapping in (None, {}, [], {"unknown": self.origin}, {self.name: ""},
                        {self.name: "capture/trees/work/other.json"}, {self.name: "../" + self.origin}):
            with self.subTest(mapping=mapping), self.assertRaises(TaskflowError):
                self.config["source_path_map"] = mapping
                self.validate()

    def test_changed_first_stage_source_or_teacher_scope_is_rejected(self):
        original = copy.deepcopy(self.facts_manifest)
        for field, value in (("source", "/foreign/empty.json"), ("kind", "teacher"), ("sha256", "b" * 64)):
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.facts_manifest = copy.deepcopy(original)
                self.facts_manifest["inputs"][0][field] = value
                self.validate()
        self.facts_manifest = copy.deepcopy(self.manifest)
        self.facts_manifest["inputs"][0]["kind"] = self.manifest["inputs"][0]["kind"] = "teacher"
        with self.assertRaises(TaskflowError):
            self.validate()

    def test_package_role_path_and_original_hash_are_required(self):
        original = copy.deepcopy(self.binding)
        for field, value in (("package_path", "other.json"), ("path", "/foreign/file"),
                             ("role", "teacher"), ("sha256", "b" * 64)):
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.binding = copy.deepcopy(original)
                self.binding["public_sources"][0][field] = value
                self.validate()
        self.binding = original
        self.declaration["files"][0]["role"] = "teacher"
        with self.assertRaises(TaskflowError):
            self.validate()

    def test_ambiguous_selector_or_native_name_and_byte_drift_are_rejected(self):
        original = copy.deepcopy(self.rows)
        self.rows.append(copy.deepcopy(self.rows[0]))
        with self.assertRaises(TaskflowError):
            self.validate()
        self.rows = original
        self.run["artifacts"].append(copy.deepcopy(self.run["artifacts"][0]))
        with self.assertRaises(TaskflowError):
            self.validate()
        self.run["artifacts"].pop()
        for field, value in (("sha256", "b" * 64), ("size", 4), ("id", "same-bytes-other")):
            previous = self.run["artifacts"][0][field]
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.run["artifacts"][0][field] = value
                self.validate()
            self.run["artifacts"][0][field] = previous

    def test_submission_cannot_be_remapped_and_every_citation_is_bound(self):
        self.config["source_path_map"]["original/answer.md"] = "answer.md"
        with self.assertRaises(TaskflowError):
            self.validate()
        del self.config["source_path_map"]["original/answer.md"]
        original = copy.deepcopy(self.transport)
        for field, value in (("signed_path", "unknown"), ("signed_locator", "wrong"),
                             ("artifact_id", "same-bytes-other"), ("source_path", "foreign"),
                             ("native_evidence", ["same-bytes-other#/"])):
            self.transport = copy.deepcopy(original)
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.transport["bindings"][0][field] = value
                self.validate()
        for field, value in (("bindings", original["bindings"][:1]), ("root_business_rereview", True),
                             ("all_signed_original_reasons_preserved", False)):
            self.transport = copy.deepcopy(original)
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                self.transport[field] = value
                self.validate()

    def test_context_retains_existing_direct_unique_and_ambiguous_behavior(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["inputs"].extend([
            {"source": "/sealed/facts.json", "target": "sealed/facts.json", "kind": "submission", **self.info},
            {"source": "/package/answer.md", "target": "normative/answer.md", "kind": "teacher", **self.info},
            {"source": "/package/unique", "target": "unique", "kind": "public", "sha256": "c" * 64, "size": 1}])
        rows = [*self.rows, {"id": "unique", "incoming_path": "unique", "visibility": "public", "sha256": "c" * 64, "size": 1}]
        context = _managed_projection_context(Path("/score"), manifest, self.binding,
                                              {"path": "/sealed/facts.json"}, {}, rows)
        sources = context["plan"]["sources"]
        self.assertNotIn("artifact_id", sources[0])
        self.assertEqual("answer", sources[1]["artifact_id"])
        self.assertEqual("unique", sources[2]["artifact_id"])
        self.assertEqual(1, len(context["plan"]["stage2_teacher"]))

    def test_ordinary_reentry_retains_only_exact_received_config(self):
        config = {"run_directory": "/run", "evaluation_id": "neutral", "expected_sid": "neutral-sid"}
        received = {**config, "source_path_map": self.config["source_path_map"]}
        destination = Path("/intake")
        receipt = {"accepted": True, "state": "accepted", "receipt": "/intake/receipt.json",
                   "run": "/run", "evaluation_id": "neutral"}
        self.assertEqual(received, _received_source_map_config(config, received, receipt, destination))
        for field, value in (("accepted", False), ("state", "waiting"), ("receipt", "/foreign/receipt.json"),
                             ("run", "/foreign"), ("evaluation_id", "foreign")):
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                _received_source_map_config(config, received, {**receipt, field: value}, destination)
        for field in config:
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                _received_source_map_config({**config, field: "foreign"}, received, receipt, destination)

    def test_replay_uses_the_same_evaluator_metadata_as_managed_intake(self):
        value = _managed_projection_config({}, "neutral-sid", {
            "model": {"requested": "fixture-cli", "reasoning_effort": "fixture"}}, {"nature": "test_fixture"})
        self.assertEqual("neutral-sid", value["expected_sid"])
        self.assertEqual("Independent fixture-cli/fixture same-SID two-stage original neutral-sid; nature=test_fixture",
                         value["evaluator_description"])


if __name__ == "__main__":
    unittest.main()
