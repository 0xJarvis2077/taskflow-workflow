"""Selection seam for store-validated third-source quality coverage.

The store gate and native handoff are explicit contract doubles here. Their
actual byte/graph validators have separate tests; no native proof is claimed.
"""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from taskflow.contracts import TaskflowError
from taskflow.execution_experiment import _source
from taskflow.execution_store import evidence_ref


class ExperimentSupplementSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.candidates = {
            "initial": {"candidate_id": "initial", "task_id": "neutral", "parent_id": None,
                        "input_digest": "1" * 64, "content_digest": "2" * 64, "evaluation_digest": "3" * 64},
            "current": {"candidate_id": "current", "task_id": "neutral", "parent_id": "initial",
                        "author_attempt": "repair", "input_digest": "1" * 64,
                        "content_digest": "4" * 64, "evaluation_digest": "5" * 64}}
        self.attempts, sources = {}, {}
        for identity in ("whole", "base", "supplement", "repair"):
            runtime = self.root / identity
            (runtime / "artifacts").mkdir(parents=True)
            self.attempts[identity] = {"attempt_id": identity, "runtime_path": str(runtime),
                "cli_sid": identity + "-sid", "state": "completed", "role": "quality",
                "candidate_id": "initial" if identity == "whole" else "current", "task_id": "neutral"}
            for key, name in (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md")):
                path = runtime / "artifacts" / name
                path.write_text(identity + " neutral original " + name)
                sources[(identity, key)] = evidence_ref(path, "neutral contract-double source")
        self.spec = {"schema_version": 1, "attempt_id": "supplement", "base_recheck_id": "base-review",
                     "report": sources[("supplement", "adoption")], "issues": sources[("supplement", "issues")],
                     "original_recheck_report": sources[("base", "adoption")]}
        self.review = {"review_id": "derived-supplement", "candidate_id": "current", "attempt_id": "base",
            "derived": True, "snapshot_bound_acceptance": True, "whole_package": False,
            "composite_quality_coverage": {"whole_review_id": "whole-review", "recheck_id": "base-review",
                "closed_defect_ids": ["defect-one"], "whole_source": {"candidate_id": "initial", "attempt_id": "whole"},
                "affected_source": {"candidate_id": "current", "attempt_id": "base", "report": sources[("base", "adoption")]},
                "recheck_supplement": {"definition": copy.deepcopy(self.spec), "transport": {"fixture_gate_contract": True}}}}
        self.reviews = {"whole-review": {"review_id": "whole-review", "candidate_id": "initial", "whole_package": True, "verdict": "defects"},
                        "base-review": {"review_id": "base-review", "candidate_id": "current", "whole_package": False,
                                        "verdict": "accepted", "covered_defect_ids": ["defect-one"]}}
        receipt = self.root / "source-receipt.json"
        receipt.write_text('{"nature":"neutral prevalidated source receipt seam"}')
        authority = self.root / "authority.json"
        authority.write_text('{"nature":"neutral exact supplement authority seam"}')
        self.source = {"flow_id": "source-child", "state": {"status": "accepted", "receipt": str(receipt),
            "candidate_id": "current", "candidate_root": str(self.root / "current"), "accepted_review_id": "base-review",
            "history": [{"review_id": "whole-review", "cli_sid": "whole-sid"}, {"review_id": "base-review", "cli_sid": "base-sid"}]}}
        self.record = {"flow_id": "experiment", "revision": 1,
            "definition": {"source_author_flow_id": "source-author", "task_id": "neutral", "technical": {},
                           "authorization": evidence_ref(authority, "neutral explicit source authority")},
            "state": {"status": "active", "stage": "experiment_v1", "history": [], "archives": [],
                      "quality_recheck_supplement": copy.deepcopy(self.spec),
                      "quality_recheck_supplement_review_id": "derived-supplement"}}
        self.bound = {"source": self.source, "initial": self.candidates["initial"],
                      "author_sids": {"author-sid", "public-sid"}, "binding": {"source_author_flow_id": "source-author"}}
        self.native = {"source_refs": {("whole_" if actor == "whole" else "supplement_" if actor == "supplement" else "") + name: reference
                                     for (actor, name), reference in sources.items() if actor in ("whole", "base", "supplement")}}
        self.gated, self.composed = copy.deepcopy(self.review), copy.deepcopy(self.review)
        self.store = Mock()
        self.store.assert_review_current = Mock()
        self.store.get_candidate.side_effect = lambda identity: copy.deepcopy(self.candidates[identity])
        self.store.get_review.side_effect = lambda identity: copy.deepcopy(self.reviews[identity])
        self.store.get_attempt.side_effect = lambda identity: copy.deepcopy(self.attempts[identity])
        self.store.quality_gate.side_effect = self.gate
        self.store.compose_repaired_quality.side_effect = self.compose
        self.store.update_flow.side_effect = lambda identity, revision, state, event: {
            **self.record, "revision": revision + 1, "state": copy.deepcopy(state)}

    def gate(self, candidate_id):
        self.assertEqual("current", candidate_id)
        if self.gated is None:
            raise TaskflowError("current whole-package independent quality review missing/stale")
        return copy.deepcopy(self.gated)

    def compose(self, *args, **kwargs):
        self.gated = copy.deepcopy(self.composed)
        return copy.deepcopy(self.composed)

    def consume(self):
        with patch("taskflow.execution_experiment._author_source", return_value=self.bound), \
             patch("taskflow.execution_experiment._native_handoff", return_value=self.native):
            return _source(self.store, self.record, {"quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}})

    def test_existing_validated_supplement_is_reused_without_default_recomposition(self):
        original_definition = copy.deepcopy(self.record["definition"])
        result = self.consume()
        self.store.compose_repaired_quality.assert_not_called()
        self.assertEqual("release", result["state"]["stage"])
        self.assertEqual("derived-supplement", result["state"]["accepted_review_id"])
        self.assertEqual("base-review", self.source["state"]["accepted_review_id"])
        self.assertEqual(original_definition, self.record["definition"])
        self.assertEqual(self.native["source_refs"], result["state"]["quality_sources"])
        self.assertIn("supplement-sid", result["state"]["excluded_sids"])
        self.store.claim.assert_not_called()

    def test_explicit_spec_is_passed_to_composition_when_no_derived_gate_exists(self):
        self.record["state"].pop("quality_recheck_supplement_review_id")
        self.gated = None
        result = self.consume()
        self.assertEqual(self.spec, self.store.compose_repaired_quality.call_args.kwargs["supplement"])
        self.assertEqual("derived-supplement", result["state"]["accepted_review_id"])
        self.assertEqual("base-review", self.source["state"]["accepted_review_id"])

    def test_authorized_existing_review_failure_cannot_fall_back_to_recomposition(self):
        self.gated = None
        with self.assertRaisesRegex(TaskflowError, "missing/stale"):
            self.consume()
        self.store.compose_repaired_quality.assert_not_called()
        self.store.update_flow.assert_not_called()

    def test_graph_candidate_reviews_closure_and_explicit_review_identity_must_match(self):
        mutations = [lambda value: value.update(candidate_id="foreign"),
                     lambda value: value.update(review_id="foreign-derived"),
                     lambda value: value["composite_quality_coverage"].update(whole_review_id="foreign-whole"),
                     lambda value: value["composite_quality_coverage"].update(recheck_id="foreign-base"),
                     lambda value: value["composite_quality_coverage"].update(closed_defect_ids=["foreign-defect"]),
                     lambda value: value["composite_quality_coverage"]["whole_source"].update(candidate_id="foreign-initial"),
                     lambda value: value["composite_quality_coverage"]["affected_source"].update(candidate_id="foreign-current")]
        for mutate in mutations:
            self.gated = copy.deepcopy(self.review)
            mutate(self.gated)
            with self.subTest(mutate=mutate), self.assertRaises(TaskflowError):
                self.consume()
        self.store.compose_repaired_quality.assert_not_called()
        self.store.update_flow.assert_not_called()

    def test_unapproved_or_different_supplement_and_wrong_actor_are_rejected(self):
        self.record["state"].pop("quality_recheck_supplement")
        self.record["state"].pop("quality_recheck_supplement_review_id")
        with self.assertRaisesRegex(TaskflowError, "explicit experiment-state authorization"):
            self.consume()
        self.record["state"]["quality_recheck_supplement"] = copy.deepcopy(self.spec)
        foreign = self.root / "different-report.json"
        foreign.write_text("another actual byte source")
        self.record["state"]["quality_recheck_supplement"]["report"] = evidence_ref(foreign, "different source")
        with self.assertRaisesRegex(TaskflowError, "exact authorized source"):
            self.consume()
        wrong_base_source = copy.deepcopy(self.spec)
        wrong_base_source["original_recheck_report"] = evidence_ref(foreign, "different original recheck source")
        self.record["state"]["quality_recheck_supplement"] = wrong_base_source
        self.gated["composite_quality_coverage"]["recheck_supplement"]["definition"] = copy.deepcopy(wrong_base_source)
        with self.assertRaisesRegex(TaskflowError, "exact authorized source"):
            self.consume()
        self.gated = copy.deepcopy(self.review)
        self.record["state"]["quality_recheck_supplement"] = copy.deepcopy(self.spec)
        self.record["state"]["quality_recheck_supplement"]["base_recheck_id"] = "foreign-base"
        with self.assertRaisesRegex(TaskflowError, "exact explicit"):
            self.consume()
        self.record["state"]["quality_recheck_supplement"] = copy.deepcopy(self.spec)
        self.attempts["supplement"]["candidate_id"] = "foreign-candidate"
        with self.assertRaisesRegex(TaskflowError, "supplement actor"):
            self.consume()
        self.store.update_flow.assert_not_called()

    def test_default_composition_retains_its_original_call_shape(self):
        self.record["state"].pop("quality_recheck_supplement")
        self.record["state"].pop("quality_recheck_supplement_review_id")
        self.composed["composite_quality_coverage"].pop("recheck_supplement")
        self.composed["review_id"] = "default-composed"
        self.native["source_refs"] = {key: value for key, value in self.native["source_refs"].items()
                                     if not key.startswith("supplement_")}
        self.gated = None
        result = self.consume()
        self.assertNotIn("supplement", self.store.compose_repaired_quality.call_args.kwargs)
        self.assertEqual("default-composed", result["state"]["accepted_review_id"])


if __name__ == "__main__":
    unittest.main()
