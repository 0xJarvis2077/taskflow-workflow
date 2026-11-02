"""Select only a flow's explicitly covered whole signature; no business judging."""

import copy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from taskflow.contracts import TaskflowError
from taskflow.execution_flow import _close_accepted_defect


class CoverageChainRoutingTests(unittest.TestCase):
    def setUp(self):
        self.candidates = {
            "p0": {"candidate_id": "p0", "input_digest": "old-public"},
            "p1": {"candidate_id": "p1", "parent_id": "p0", "input_digest": "repaired-public"},
            "p2": {"candidate_id": "p2", "parent_id": "p1", "input_digest": "repaired-public"}}
        self.defect = {"defect_id": "original-duty", "candidate_id": "p0", "repair_candidate_id": "p1", "kind": "material"}
        self.final = {"review_id": "final-affected", "candidate_id": "p2", "whole_package": False, "verdict": "accepted"}
        self.anchor = {"review_id": "whole-repair", "candidate_id": "p1", "attempt_id": "whole-attempt", "whole_package": True,
                       "verdict": "defects", "covered_defect_ids": ["original-duty"]}
        self.reviews = {self.anchor["review_id"]: self.anchor}
        self.attempts = {"whole-attempt": {"attempt_id": "whole-attempt", "candidate_id": "p1", "role": "quality",
                                           "state": "completed", "cli_sid": "original-independent-whole-sid"}}
        self.record = {"state": {"history": [{"kind": "independent_review", "review_id": "whole-repair", "attempt_id": "whole-attempt",
                                                "candidate_id": "p1", "cli_sid": "original-independent-whole-sid"}]}}
        self.evidence = [{"path": "neutral-final-source", "sha256": "neutral-schema-interface"}]
        self.store = SimpleNamespace(get_candidate=lambda key: self.candidates[key], get_review=lambda key: self.reviews[key],
                                     get_attempt=lambda key: self.attempts[key], close_defect=Mock(return_value={"route": "original"}),
                                     close_defect_with_coverage_chain=Mock(return_value={"route": "coverage_chain"}))

    def close(self):
        return _close_accepted_defect(self.store, self.record, self.defect, self.final, self.evidence)

    def test_material_or_changed_public_reference_uses_explicit_intermediate_whole_signature(self):
        original = copy.deepcopy((self.record, self.anchor, self.final, self.defect))
        self.assertEqual({"route": "coverage_chain"}, self.close())
        self.store.close_defect_with_coverage_chain.assert_called_once_with(
            "original-duty", "final-affected", repair_review_id="whole-repair", evidence=self.evidence)
        self.store.close_defect.assert_not_called()
        self.assertEqual(original, (self.record, self.anchor, self.final, self.defect))
        self.defect["kind"] = "rubric"
        self.store.close_defect_with_coverage_chain.reset_mock()
        self.assertEqual({"route": "coverage_chain"}, self.close())
        self.store.close_defect_with_coverage_chain.assert_called_once()
        self.assertEqual("defects", self.anchor["verdict"])
        self.assertFalse(self.final["whole_package"])

    def test_normal_same_public_private_closure_and_current_whole_keep_original_gate(self):
        self.defect.update(candidate_id="p1", repair_candidate_id="p2", kind="reference")
        self.assertEqual({"route": "original"}, self.close())
        self.store.close_defect.assert_called_once_with("original-duty", "final-affected", evidence=self.evidence)
        self.store.close_defect_with_coverage_chain.assert_not_called()
        self.store.close_defect.reset_mock()
        self.defect.update(candidate_id="p0", kind="material")
        self.final["whole_package"] = True
        self.assertEqual({"route": "original"}, self.close())
        self.store.close_defect.assert_called_once()

    def test_missing_ambiguous_foreign_or_mismatched_history_never_selects_a_chain(self):
        original = copy.deepcopy((self.candidates, self.record, self.anchor, self.attempts))
        for label in ("not covered", "different public", "wrong candidate branch", "duplicate signature", "foreign sid", "wrong actor", "no lineage", "cyclic lineage"):
            self.candidates, self.record, self.anchor, self.attempts = copy.deepcopy(original)
            self.reviews = {"whole-repair": self.anchor}
            if label == "not covered":
                self.anchor["covered_defect_ids"] = []
            elif label == "different public":
                self.candidates["p1"]["input_digest"] = "foreign-public"
            elif label == "wrong candidate branch":
                self.candidates["other"] = {"candidate_id": "other", "input_digest": "repaired-public"}
                self.anchor["candidate_id"] = "other"
            elif label == "duplicate signature":
                self.reviews["other-whole"] = {**self.anchor, "review_id": "other-whole"}
                self.record["state"]["history"].append({**self.record["state"]["history"][0], "review_id": "other-whole"})
            elif label == "foreign sid":
                self.record["state"]["history"][0]["cli_sid"] = "foreign-sid"
            elif label == "wrong actor":
                self.attempts["whole-attempt"]["role"] = "repair"
            elif label == "no lineage":
                self.candidates["p2"].pop("parent_id")
            else:
                self.candidates["p2"]["parent_id"] = "p2"
            with self.subTest(label=label), self.assertRaises(TaskflowError):
                self.close()
        self.store.close_defect.assert_not_called()
        self.store.close_defect_with_coverage_chain.assert_not_called()


if __name__ == "__main__":
    unittest.main()
