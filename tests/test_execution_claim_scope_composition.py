"""Compose only a signed private claim scope adjustment; no model calls."""

import copy
import sqlite3
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical
from taskflow.execution_store import ExecutionStore
from tests import test_execution_composite_quality as composite_fixtures
from tests import test_execution_flow as flow_fixtures


def make_claim_scope_fixture(testcase, *, extra=True, association=True, bad_sha=False, public_scope=False):
    case = composite_fixtures.CompositeQualityTests("test_exact_private_repair_composes_without_new_whole_signature")
    testcase.addCleanup(case.doCleanups)
    script = flow_fixtures.ROLE_FIXTURE.replace("'kind':'reference'", "'kind':'claim'")
    if public_scope:
        script = script.replace("'affected_paths':[name]", "'affected_paths':[name,'draft/input/prompt.md']")
    with patch.object(flow_fixtures, "ROLE_FIXTURE", script):
        return case.make_closed_flow(extra=extra, association=association, bad_sha=bad_sha)


class ClaimScopeCompositionTests(unittest.TestCase):
    def compose(self, value):
        return value["store"].compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
                                                       value["defects"], evidence=value["refs"])

    def changed_view(self, value, *, defect_changes=None, candidate_changes=None):
        # Deliberately invalid DB views are confined to memory. Sealed actors
        # and their actual signatures remain unchanged; these are negatives.
        view = sqlite3.connect(":memory:")
        view.row_factory = sqlite3.Row
        with value["store"]._db(False) as original:
            original.backup(view)
        if defect_changes:
            defect = {**value["store"].get_defect(value["defects"][0]), **defect_changes}
            view.execute("UPDATE defects SET data=? WHERE id=?", (canonical(defect).decode(), defect["defect_id"]))
        if candidate_changes:
            candidate = {**value["store"].get_candidate(value["candidate"]), **candidate_changes}
            view.execute("UPDATE candidates SET data=? WHERE id=?", (canonical(candidate).decode(), candidate["candidate_id"]))
        self.addCleanup(view.close)
        return view

    def material(self, view, value):
        return ExecutionStore._repaired_quality_material(view, value["candidate"], value["whole"], value["recheck"], value["defects"])

    def test_actual_private_claim_scope_adjustment_keeps_kind_and_affected_signature(self):
        value = make_claim_scope_fixture(self)
        defect = copy.deepcopy(value["store"].get_defect(value["defects"][0]))
        self.assertEqual("claim", defect["kind"])
        self.assertEqual("scope_adjustment", defect["disposition"])
        result = self.compose(value)
        graph = result["composite_quality_coverage"]
        self.assertFalse(result["whole_package"])
        self.assertTrue(result["snapshot_bound_acceptance"])
        self.assertEqual(value["recheck"], graph["recheck_id"])
        self.assertEqual(2, len(graph["private_changes"]))
        self.assertTrue(any(not row["original_issue_affected"] and row["fresh_signed_evidence"] for row in graph["private_changes"]))
        self.assertEqual(defect, value["store"].get_defect(value["defects"][0]))
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))
        self.assertEqual(value["recheck"], value["store"].get_flow(value["flow"]["flow_id"])["state"]["accepted_review_id"])

    def test_claim_with_public_affected_scope_is_not_blanket_admitted(self):
        value = make_claim_scope_fixture(self, public_scope=True)
        with self.assertRaisesRegex(TaskflowError, "original signed private affected"):
            self.compose(value)

    def test_unsupported_type_and_true_identity_ordering_errors_remain_distinct(self):
        value = make_claim_scope_fixture(self)
        for change in ({"kind": "material"}, {"kind": "environment"}, {"disposition": "private_repair"}):
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "does not support this defect kind/repair disposition"):
                self.material(self.changed_view(value, defect_changes=change), value)
        for change in ({"closure_review_id": value["whole"]}, {"author_attempt": "foreign-author"}, {"closed_seq": 0}):
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "identity and actual ordering"):
                self.material(self.changed_view(value, defect_changes=change), value)
        with self.assertRaisesRegex(TaskflowError, "signature is not bound"):
            self.material(self.changed_view(value, candidate_changes={"input_digest": "0" * 64}), value)

    def test_missing_association_current_sha_and_later_conflict_still_block(self):
        for options, message in (({"association": False}, "outside original issues"), ({"bad_sha": True}, "current SHA evidence")):
            value = make_claim_scope_fixture(self, **options)
            with self.subTest(options=options), self.assertRaisesRegex(TaskflowError, message):
                self.compose(value)
        value = make_claim_scope_fixture(self)
        case = composite_fixtures.CompositeQualityTests("test_later_negative_before_composition_cannot_be_washed_by_new_record_time")
        case.later_negative(value)
        with self.assertRaisesRegex(TaskflowError, "later independent negative"):
            self.compose(value)

