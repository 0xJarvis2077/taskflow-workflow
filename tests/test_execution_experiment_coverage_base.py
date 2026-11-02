"""Source selection unit boundary; store graph/native validators have real fixtures."""

import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from taskflow.contracts import TaskflowError
from taskflow.execution_experiment import _source
from taskflow.execution_store import evidence_ref


class ExperimentCoverageBaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidates = {"p0": {"candidate_id": "p0", "task_id": "neutral", "input_digest": "old"},
                           "p1": {"candidate_id": "p1", "parent_id": "p0", "task_id": "neutral", "input_digest": "new"},
                           "p2": {"candidate_id": "p2", "parent_id": "p1", "task_id": "neutral", "input_digest": "new"}}
        self.reviews = {
            "initial-whole": {"review_id": "initial-whole", "candidate_id": "p0", "whole_package": True, "verdict": "defects"},
            "repair-whole": {"review_id": "repair-whole", "candidate_id": "p1", "whole_package": True, "verdict": "defects"},
            "final-affected": {"review_id": "final-affected", "candidate_id": "p2", "whole_package": False,
                               "verdict": "accepted", "covered_defect_ids": ["old-duty", "new-duty"]}}
        self.receipt = self.root / "source-result.json"
        self.receipt.write_text("Neutral source receipt, tested source-author boundary only.")
        self.source = {"flow_id": "actual-child", "state": {"status": "accepted", "receipt": str(self.receipt),
            "candidate_id": "p2", "candidate_root": str(self.root / "candidate"), "accepted_review_id": "final-affected",
            "history": [{"review_id": identity, "cli_sid": identity + "-sid"} for identity in self.reviews]}}
        self.attempts = {}
        for identity in ("original-attempt", "final-attempt"):
            runtime = self.root / identity
            (runtime / "artifacts").mkdir(parents=True)
            for filename in ("adoption-readiness.json", "issues.json", "intake.md"):
                (runtime / "artifacts" / filename).write_text("Neutral interface bytes; no model or business signature.")
            self.attempts[identity] = {"attempt_id": identity, "runtime_path": str(runtime), "cli_sid": identity + "-sid"}
        authority = self.root / "authority.json"
        authority.write_text("Neutral already-validated packet interface.")
        self.record = {"flow_id": "source-experiment", "revision": 0, "definition": {"task_id": "neutral",
            "source_author_flow_id": "actual-parent", "authorization": evidence_ref(authority, "neutral interface"), "technical": {}},
            "state": {"stage": "experiment_v1", "history": [], "archives": []}}
        self.packet = {"quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}}
        self.bound = {"source": self.source, "initial": self.candidates["p0"], "author_sids": set(),
                      "binding": {"source_author_flow_id": "actual-parent"}}
        self.accepted = None
        def quality(candidate_id):
            if self.accepted is None:
                raise TaskflowError("current whole-package independent quality review missing/stale")
            return self.accepted
        def compose(candidate_id, whole_id, recheck_id, defects, **options):
            self.accepted = {"review_id": "canonical-composite", "candidate_id": candidate_id, "attempt_id": "final-attempt",
                "derived": True, "snapshot_bound_acceptance": True, "whole_package": False,
                "composite_quality_coverage": {"whole_review_id": whole_id, "recheck_id": recheck_id,
                    "whole_source": {"candidate_id": self.reviews[whole_id]["candidate_id"], "attempt_id": "original-attempt"},
                    "affected_source": {"candidate_id": candidate_id}, "closed_defect_ids": defects}}
            return self.accepted
        self.store = SimpleNamespace(get_candidate=lambda key: self.candidates[key], get_review=lambda key: self.reviews[key],
            get_attempt=lambda key: self.attempts[key], quality_gate=quality, compose_repaired_quality=Mock(side_effect=compose),
            assert_review_current=Mock(), update_flow=Mock(side_effect=lambda identity, rev, state, event:
                {**self.record, "revision": rev + 1, "state": state}))

    def consume(self):
        # This tests the caller's exact selection/delegation, not the separately
        # covered store acceptance or native role evidence validators.
        with patch("taskflow.execution_experiment._author_source", return_value=self.bound), \
             patch("taskflow.execution_experiment._native_handoff", return_value={"source_refs": {}, "nature": "explicit_interface_fixture"}):
            return _source(self.store, self.record, self.packet)

    def test_direct_parent_whole_is_selected_after_an_authorized_public_revision(self):
        before = copy.deepcopy(self.reviews)
        result = self.consume()
        self.assertEqual("release", result["state"]["stage"])
        self.assertEqual("p2", result["state"]["candidate_id"])
        self.store.compose_repaired_quality.assert_called_once()
        self.assertEqual(("p2", "repair-whole", "final-affected", ["old-duty", "new-duty"]),
                         self.store.compose_repaired_quality.call_args.args)
        self.assertEqual(before, self.reviews)
        self.assertEqual("defects", self.reviews["repair-whole"]["verdict"])
        self.assertFalse(self.accepted["whole_package"])

    def test_original_direct_parent_route_remains_compatible(self):
        self.candidates["p2"]["parent_id"] = "p0"
        self.candidates["p2"]["input_digest"] = "old"
        self.reviews.pop("repair-whole")
        self.source["state"]["history"] = [{"review_id": "initial-whole"}, {"review_id": "final-affected"}]
        self.consume()
        self.assertEqual("initial-whole", self.store.compose_repaired_quality.call_args.args[1])

    def test_initial_or_foreign_whole_ambiguity_and_override_do_not_expand_source_scope(self):
        original = copy.deepcopy((self.candidates, self.reviews, self.source, self.record))
        for label in ("missing parent whole", "foreign whole", "ambiguous", "explicit old base", "outside lineage"):
            self.candidates, self.reviews, self.source, self.record = copy.deepcopy(original)
            self.bound.update(source=self.source, initial=self.candidates["p0"])
            if label == "missing parent whole":
                self.reviews["repair-whole"]["whole_package"] = False
            elif label == "foreign whole":
                self.reviews["repair-whole"]["candidate_id"] = "foreign"
            elif label == "ambiguous":
                self.reviews["duplicate"] = {**self.reviews["repair-whole"], "review_id": "duplicate"}
                self.source["state"]["history"].append({"review_id": "duplicate"})
            elif label == "explicit old base":
                self.record["definition"]["technical"]["quality_coverage"] = {
                    "whole_review_id": "initial-whole", "recheck_id": "final-affected", "closed_defect_ids": ["old-duty", "new-duty"]}
            else:
                self.candidates["p1"].pop("parent_id")
            with self.subTest(label=label), self.assertRaises(TaskflowError):
                self.consume()
        self.store.compose_repaired_quality.assert_not_called()
        self.store.update_flow.assert_not_called()


if __name__ == "__main__":
    unittest.main()
