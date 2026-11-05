"""Delivery diagnostics are structured and cannot silently change a signoff."""

import copy
import unittest

from taskflow.execution_quality_progress_contract import (
    QualityContractError, check_issue_targets,
)


class QualityDiagnosticsTests(unittest.TestCase):
    def test_repair_scope_error_preserves_original_and_identifies_the_field(self):
        report = {"execution_routing": {"outcome": "revision_required",
                                         "blocking_issue_ids": ["neutral-finding"]}}
        issues = {"issues": [{"id": "neutral-finding", "kind": "claim", "blocking": True,
                               "affected_paths": ["actual-public-work/answer.md"]}]}
        context = {"candidate_paths": ["draft/input/rules.md"],
                   "observed_paths": ["actual-public-work/answer.md"]}
        before = copy.deepcopy((report, issues, context))
        with self.assertRaises(QualityContractError) as error:
            check_issue_targets(report, issues, context)
        detail = error.exception.as_dict()
        self.assertEqual("observed_repair_target", detail["code"])
        self.assertEqual("/issues/0/affected_paths/0", detail["field"])
        self.assertEqual("neutral-finding", detail["issue_id"])
        self.assertEqual("actual-public-work/answer.md", detail["affected_path"])
        detail["code"] = "caller-modification"
        self.assertEqual("observed_repair_target", error.exception.as_dict()["code"])
        self.assertEqual(before, (report, issues, context))

    def test_unknown_target_is_distinct_from_a_business_disagreement(self):
        with self.assertRaises(QualityContractError) as error:
            check_issue_targets(
                {"execution_routing": {"blocking_issue_ids": []}},
                {"issues": [{"id": "neutral", "kind": "material",
                             "affected_paths": ["outside.md"]}]},
                {"candidate_paths": ["rules.md"], "observed_paths": []},
            )
        self.assertEqual("unknown_repair_target", error.exception.as_dict()["code"])

    def test_schema_error_uses_stable_code_without_inventing_an_outcome(self):
        with self.assertRaises(QualityContractError) as error:
            check_issue_targets({}, {}, {})
        self.assertEqual("invalid_delivery_structure", error.exception.as_dict()["code"])
        self.assertNotIn("outcome", error.exception.as_dict())


if __name__ == "__main__":
    unittest.main()
