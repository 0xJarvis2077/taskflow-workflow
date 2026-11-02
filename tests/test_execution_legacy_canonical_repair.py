"""Keep canonical historical repair records and their actual byte bindings."""

import json
import sqlite3
import unittest
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical
from taskflow.execution_flow_contracts import validate_repair
from taskflow.execution_store import ExecutionStore
from tests import test_execution_flow_contracts as contract_fixtures
from tests.test_execution_coverage_chain_integration import make_coverage_chain_fixture


class LegacyCanonicalRepairTests(unittest.TestCase):
    def setUp(self):
        self.case = contract_fixtures.FlowContractTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def revision(self, private=False):
        contents = dict(self.case.contents)
        if private:
            contents.update({
                "draft/evaluation/check.json": ("verification", '{"observed":"before"}'),
                "draft/evaluation/context.md": ("teacher", "Original context."),
            })
            parent = self.case.make_candidate("private-parent", contents)
            changed = {**contents,
                "draft/evaluation/answer.md": ("answer", "A corrected feasible route."),
                "draft/evaluation/rubric.md": ("rubric", "Accept every supported alternative."),
                "draft/evaluation/check.json": ("verification", '{"observed":"after"}'),
                "draft/evaluation/context.md": ("teacher", "Corrected supporting context."),
            }
            identities = ["issue-b", "issue-a"]
            name = "private-child"
        else:
            parent = self.case.candidate
            changed = {**contents,
                "draft/input/prompt.md": ("prompt", "A revised complete request."),
                "draft/evaluation/rubric.md": ("rubric", "Accept supported equivalent methods."),
                "draft/evaluation/new.md": ("teacher", "New supporting evidence."),
            }
            del changed["draft/evaluation/answer.md"]
            identities = ["issue-a"]
            name = "public-child"
        child = self.case.make_candidate(name, changed, parent_id=parent["candidate_id"])
        before = {row["package_path"]: row for row in parent["files"]}
        after = {row["package_path"]: row for row in child["files"]}
        record = {
            "parent_id": parent["candidate_id"], "candidate_id": child["candidate_id"],
            "defect_ids": list(identities), "covered_defect_ids": list(reversed(identities)),
            "needs_business_decision": False,
            "changes": [{
                "path": name,
                "before_sha256": before[name]["sha256"] if name in before else None,
                "after_sha256": after[name]["sha256"] if name in after else None,
            } for name in sorted(set(before) | set(after))
                if (before.get(name, {}).get("sha256"), before.get(name, {}).get("role"))
                != (after.get(name, {}).get("sha256"), after.get(name, {}).get("role"))],
        }
        return parent, child, record, identities

    def validate(self, parent, child, record, identities):
        self.case.write(self.case.repair_path, record)
        return validate_repair(self.case.repair_path, parent=parent,
                               candidate=child, defect_ids=identities)

    def assert_original_preserved(self, private):
        parent, child, record, identities = self.revision(private)
        self.case.write(self.case.repair_path, record)
        original = self.case.repair_path.read_bytes()
        result = validate_repair(self.case.repair_path, parent=parent,
                                 candidate=child, defect_ids=identities)
        self.assertEqual(identities, result["defect_ids"])
        self.assertEqual(4, len(result["changes"]))
        self.assertNotIn("role", record)
        self.assertEqual(original, self.case.repair_path.read_bytes())
        self.assertEqual(record, json.loads(original))

    def test_private_canonical_and_redundant_alias_preserve_missing_role(self):
        self.assert_original_preserved(private=True)

    def test_public_canonical_and_redundant_alias_keep_all_actual_changes(self):
        self.assert_original_preserved(private=False)

    def test_alias_only_without_role_is_not_legacy_canonical_identity(self):
        parent, child, record, identities = self.revision()
        del record["defect_ids"]
        with self.assertRaisesRegex(TaskflowError, "repair role"):
            self.validate(parent, child, record, identities)

    def test_explicit_wrong_role_still_rejects_even_with_canonical_ids(self):
        parent, child, record, identities = self.revision()
        for role in (None, False, "author", "quality"):
            with self.subTest(role=role), self.assertRaises(TaskflowError):
                self.validate(parent, child, {**record, "role": role}, identities)

    def test_both_fields_must_independently_match_exact_unique_ids(self):
        parent, child, record, identities = self.revision(private=True)
        for field in ("defect_ids", "covered_defect_ids"):
            for value in (["foreign"], [*identities, identities[0]], [], None):
                altered = {**record, field: value}
                with self.subTest(field=field, value=value), self.assertRaises(TaskflowError):
                    self.validate(parent, child, altered, identities)

    def test_original_and_revised_source_byte_drift_remain_blocking(self):
        parent, child, record, identities = self.revision()
        for candidate in (parent, child):
            path = Path(next(row["path"] for row in candidate["files"]
                             if row["package_path"] == "draft/input/prompt.md"))
            original = path.read_bytes()
            path.write_bytes(original + b"\nUncaptured source drift.")
            try:
                with self.subTest(candidate=candidate["candidate_id"]), self.assertRaisesRegex(TaskflowError, "bytes changed"):
                    self.validate(parent, child, record, identities)
            finally:
                path.write_bytes(original)


class OriginalCoverageSourceGuards(unittest.TestCase):
    def memory_view(self, value):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        self.addCleanup(db.close)
        with value["store"]._db() as original:
            original.backup(db)
        return db

    def test_coverage_chain_requires_original_whole_blocking_finding(self):
        value = make_coverage_chain_fixture(self)
        original = value["store"].get_review(value["original_review"])
        defect = value["store"].get_defect(value["defects"][0])
        for change in ({"verdict": "accepted"}, {"whole_package": False}):
            db = self.memory_view(value)
            altered = {**original, **change}
            db.execute("UPDATE reviews SET data=? WHERE id=?",
                       (canonical(altered).decode(), original["review_id"]))
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "original actual whole blocking"):
                ExecutionStore._closure_coverage_chain_material(
                    db, defect, value["whole"], value["recheck"])
        self.assertEqual(original, value["store"].get_review(value["original_review"]))

    def test_completed_quality_source_needs_actual_nonempty_runtime(self):
        value = make_coverage_chain_fixture(self)
        review = value["store"].get_review(value["recheck"])
        original = value["store"].get_attempt(review["attempt_id"])
        for runtime in (None, "", 17):
            db = self.memory_view(value)
            altered = {**original, "runtime_path": runtime}
            db.execute("UPDATE attempts SET data=? WHERE id=?",
                       (canonical(altered).decode(), original["attempt_id"]))
            with self.subTest(runtime=runtime), self.assertRaises(TaskflowError):
                ExecutionStore._composition_review(db, review, "affected")
        self.assertEqual(original, value["store"].get_attempt(original["attempt_id"]))


if __name__ == "__main__":
    unittest.main()
