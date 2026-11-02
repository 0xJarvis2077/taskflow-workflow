"""Whole repair coverage closes retained findings without upgrading affected scope."""

import copy
import sqlite3
import unittest
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, read_json
from taskflow.execution_store import ExecutionStore
from tests import test_execution_composite_quality as composite_fixtures


class ClosureCoverageStoreTests(unittest.TestCase):
    def make_closed(self):
        from tests.test_execution_coverage_chain_integration import make_coverage_chain_fixture

        value = make_coverage_chain_fixture(self)
        store = value["store"]
        for identity in value["defects"]:
            store.close_defect_with_coverage_chain(identity, value["recheck"],
                repair_review_id=value["whole"], evidence=value["refs"])
        if store.get_defect(value["new_defect"])["state"] != "closed":
            store.close_defect(value["new_defect"], value["recheck"], evidence=value["refs"])
        return value

    def compose(self, value):
        return value["store"].compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
            value["all_defects"], evidence=value["refs"])

    def view(self, value, table=None, identity=None, changes=None):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        with value["store"]._db() as source:
            source.backup(db)
        self.addCleanup(db.close)
        if table:
            row = {**ExecutionStore._load(db, table, identity), **changes}
            db.execute("UPDATE " + table + " SET data=? WHERE id=?", (canonical(row).decode(), identity))
        return db

    def material(self, db, value, ids=None):
        return ExecutionStore._repaired_quality_material(db, value["candidate"], value["whole"], value["recheck"],
            value["all_defects"] if ids is None else ids)

    def test_actual_retained_closure_keeps_original_signatures_and_replays_the_graph(self):
        value = self.make_closed()
        store = value["store"]
        sources = [store.get_review(key) for key in (value["original_review"], value["whole"], value["recheck"])]
        actors = {row["attempt_id"]: store.get_attempt(row["attempt_id"]) for row in sources}
        raw = {ref["path"]: Path(ref["path"]).read_bytes() for row in sources for ref in row["evidence"]}
        for identity in value["defects"]:
            defect = store.get_defect(identity)
            chain = store.validate_defect_coverage_chain(identity)
            self.assertEqual("whole_then_same_public_affected", chain["kind"])
            self.assertEqual(value["whole"], chain["repair_review_id"])
            self.assertEqual(value["recheck"], chain["final_review_id"])
            self.assertEqual("defects", chain["whole_source"]["verdict"])
            self.assertEqual("resolved_retained", chain["affected_source"]["actual_value"]["disposition"])
            self.assertFalse(chain["affected_source"]["whole_package"])
            self.assertEqual(defect, store.close_defect_with_coverage_chain(identity, value["recheck"],
                repair_review_id=value["whole"], evidence=value["refs"]))
        result = self.compose(value)
        graph = result["composite_quality_coverage"]
        self.assertFalse(result["whole_package"])
        self.assertTrue(result["snapshot_bound_acceptance"])
        self.assertEqual("whole_findings_with_inherited_affected_closure_v1", graph["coverage_mode"])
        self.assertEqual(2, len(graph["inherited_closure_chains"]))
        self.assertEqual(set(value["all_defects"]), set(graph["closed_defect_ids"]))
        self.assertEqual({value["new_defect"]}, set(graph["original_issue_ids"]))
        self.assertEqual([value["new_defect"]], read_json(Path(graph["repair_record"]["path"]))["defect_ids"])
        self.assertTrue(all(row["role"] not in {"prompt", "input", "template"} for row in graph["private_changes"]))
        self.assertEqual(result, store.quality_gate(value["candidate"]))
        self.assertEqual(result, store.assert_review_current(result["review_id"]))
        self.assertEqual(result, self.compose(value))
        self.assertEqual(sources, [store.get_review(row["review_id"]) for row in sources])
        self.assertEqual(actors, {key: store.get_attempt(key) for key in actors})
        self.assertEqual(raw, {key: Path(key).read_bytes() for key in raw})
        self.assertEqual([], store.dashboard()["decisions"])

    def test_original_identity_ordering_and_typed_chain_cannot_be_replaced(self):
        value = self.make_closed()
        defect = value["store"].get_defect(value["defects"][0])
        for changes in ({"author_attempt": "foreign-author"}, {"repair_seq": 0},
                        {"responsibility": "An unsigned responsibility."}, {"closed_seq": 0},
                        {"closure_review_id": value["whole"]}):
            db = self.view(value, "defects", defect["defect_id"], changes)
            changed = ExecutionStore._load(db, "defects", defect["defect_id"])
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                ExecutionStore._closure_coverage_chain_material(db, changed, value["whole"], value["recheck"])
        for table, identity, changes in (
                ("reviews", value["whole"], {"covered_defect_ids": []}),
                ("reviews", value["recheck"], {"covered_defect_ids": value["defects"]}),
                ("candidates", value["candidate"], {"parent_id": value["original"]["candidate_id"]}),
                ("candidates", value["anchor_candidate"]["candidate_id"], {"input_digest": "0" * 64})):
            db = self.view(value, table, identity, changes)
            with self.subTest(table=table, changes=changes), self.assertRaises(TaskflowError):
                ExecutionStore._closure_coverage_chain_material(db, defect, value["whole"], value["recheck"])
        changed = copy.deepcopy(defect["closure_coverage_chain"])
        changed["whole_source"]["actual_value"]["limit"] = "A replacement limit is not the original signature."
        db = self.view(value, "defects", defect["defect_id"], {"closure_coverage_chain": changed})
        with self.assertRaisesRegex(TaskflowError, "differs from its actual current sources"):
            self.material(db, value)
        with self.assertRaisesRegex(TaskflowError, "cannot be replaced"):
            value["store"].close_defect_with_coverage_chain(defect["defect_id"], value["recheck"],
                repair_review_id=value["whole"], evidence=[value["refs"][0]])

    def test_inherited_ids_require_exact_closed_current_chains_and_all_own_findings(self):
        value = self.make_closed()
        old = value["store"].get_defect(value["defects"][0])
        for change in ({"closure_coverage_chain": None}, {"state": "repaired"},
                       {"closure_review_id": value["whole"]}):
            db = self.view(value, "defects", old["defect_id"], change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "typed whole/affected closure chain"):
                self.material(db, value)
        for ids in (value["defects"], [value["new_defect"]], [*value["all_defects"], "foreign-defect"]):
            with self.subTest(ids=ids), self.assertRaises(TaskflowError):
                self.material(self.view(value), value, ids)
        own = value["store"].get_defect(value["new_defect"])
        db = self.view(value, "defects", own["defect_id"], {"closure_review_id": value["whole"]})
        with self.assertRaisesRegex(TaskflowError, "identity and actual ordering"):
            self.material(db, value)
        with self.assertRaises(TaskflowError):
            value["store"].validate_defect_coverage_chain(value["new_defect"])

    def test_original_source_drift_and_later_actual_negative_reject_replay(self):
        value = self.make_closed()
        result = self.compose(value)
        original = value["store"].get_review(value["original_review"])
        path = Path(original["evidence"][0]["path"])
        path.write_bytes(path.read_bytes() + b"\nchanged after the sealed signature")
        with self.assertRaises(TaskflowError):
            value["store"].quality_gate(value["candidate"])
        with self.assertRaises(TaskflowError):
            value["store"].assert_review_current(result["review_id"])
        value = self.make_closed()
        self.compose(value)
        composite_fixtures.CompositeQualityTests("test_later_negative_before_composition_cannot_be_washed_by_new_record_time").later_negative(value)
        with self.assertRaises(TaskflowError):
            value["store"].quality_gate(value["candidate"])


if __name__ == "__main__":
    unittest.main()
