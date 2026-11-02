"""An old own repaired graph can precede a new actual whole signature.

Local sealed role subprocesses and original store/IO validators are real. These
fixtures certify no external model, native sandbox or business result.
"""

from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, canonical
from taskflow.execution import capture_candidate
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_historical_closure_freshness as fixtures
from tools.execution_simulation import perform


def mixed_endpoint_fixture(owner, *, foreign=False):
    value = fixtures.closure_fixture(owner)
    case, store = value["case"], value["store"]
    historical = value["accepted"]
    repaired = store.get_candidate(historical["candidate_id"])
    declaration = next(row for row in repaired["evidence"] if row["location"] == "explicit package declaration")
    baseline = case.root / "new-whole-baseline"
    shutil.copytree(Path(declaration["path"]).parent, baseline)
    alias = capture_candidate(store, baseline, "new-whole-baseline")
    author = perform(store, case.root / "new-author-role", alias["candidate_id"], "author", "new-author",
        {"prompt.md": "New neutral public commission; preserve capacity 3 and select 1 unit.\n"})
    owner.assertEqual("completed", author["state"])
    product = case.root / "new-author-product"
    shutil.copytree(baseline, product)
    shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", product / "draft/input/prompt.md")
    candidate = capture_candidate(store, product, "new-author-product", alias["candidate_id"], author["attempt_id"])
    extra_review = None
    if foreign:
        extra = perform(store, case.root / "unconsumed-negative", alias["candidate_id"], "quality", "unconsumed",
            {"conflict.md": "Actual independent unresolved conflict, not consumed by a repair."})
        extra_review = store.record_review("unconsumed", alias["candidate_id"], extra["attempt_id"],
            verdict="inconclusive", scope="whole_package", whole_package=True, evidence=extra["evidence"])
    intent = "current-independent-whole"
    binding = {"intent_id": intent, **{key: candidate[key] for key in
        ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
    routing = {"schema_version": 1, **binding, "signer_role": "independent_quality",
        "independent_of_current_author": True, "participated_in_production": False,
        "scope": "whole_package", "outcome": "accepted", "blocking_issue_ids": [],
        "covered_defect_ids": [], "reason": "New whole inspection; no claim to be the old repair or its closure."}
    signed = perform(store, case.root / "current-whole-role", candidate["candidate_id"], "quality", intent,
        {"adoption-readiness.json": json.dumps({"execution_routing": routing}),
         "issues.json": json.dumps({"execution_binding": binding, "issues": []}),
         "intake.md": "Actual neutral whole source, not a historical repair."},
        extra={"inputs": [{"path": str(product / "task.json"), "target": "execution/task.json", "kind": "source_metadata"}]})
    owner.assertEqual("completed", signed["state"])
    refs = [evidence_ref(Path(signed["runtime_path"]) / "artifacts" / name, "actual independent whole source")
            for name in ("adoption-readiness.json", "issues.json")]
    current = store.record_review("current-independent-whole", candidate["candidate_id"], signed["attempt_id"],
        verdict="accepted", scope="new independent whole", whole_package=True, evidence=refs)
    return {**value, "historical": historical, "negative": store.get_review(value["whole"]),
            "alias": alias, "current": current, "new_candidate": candidate, "new_author": author,
            "new_attempt": signed, "foreign": extra_review}


class MixedHistoricalSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = mixed_endpoint_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    @contextmanager
    def changed(self, table, identity, change):
        store = self.value["store"]
        with store._db(True) as db:
            raw = db.execute("SELECT data FROM " + table + " WHERE id=?", (identity,)).fetchone()["data"]
            value = json.loads(raw)
            change(value)
            db.execute("UPDATE " + table + " SET data=? WHERE id=?", (canonical(value).decode(), identity))
        try:
            yield
        finally:
            with store._db(True) as db:
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (raw, identity))

    def source(self, *, current=None, negative=None):
        value = self.value
        with value["store"]._db() as db:
            return ExecutionStore._historical_repaired_quality_source(
                db, negative or value["negative"], current or value["current"])

    def test_actual_mixed_endpoints_keep_both_scopes_and_original_records(self):
        value, store = self.value, self.value["store"]
        with store._db() as db:
            before = {table: [row["data"] for row in db.execute("SELECT data FROM " + table)]
                      for table in ("reviews", "defects", "attempts", "work")}
        witness = self.source()
        self.assertFalse(witness["historical_currentness_claimed"])
        self.assertEqual(value["historical"]["composite_quality_coverage"], witness["historical_graph"])
        self.assertNotIn("current_graph", witness)
        self.assertEqual("actual_independent_whole", witness["current_source"]["kind"])
        self.assertEqual(value["new_attempt"]["attempt_id"], witness["current_source"]["attempt_id"])
        self.assertIn(value["alias"]["candidate_id"], [row["candidate_id"] for row in witness["ancestor_aliases"]])
        self.assertEqual([], value["current"]["covered_defect_ids"])
        self.assertEqual(value["current"], store.quality_gate(value["new_candidate"]["candidate_id"], require_repair_alias=True))
        self.assertFalse(store.get_review(value["historical"]["review_id"])["whole_package"])
        self.assertTrue(store.get_review(value["current"]["review_id"])["whole_package"])
        with store._db() as db:
            self.assertEqual(before, {table: [row["data"] for row in db.execute("SELECT data FROM " + table)]
                                      for table in before})

    def test_current_endpoint_must_be_real_original_managed_whole_not_a_stored_summary(self):
        value = self.value
        for changes in ({"whole_package": False}, {"derived": True}, {"snapshot_bound_acceptance": True},
                        {"composite_quality_coverage": {"whole_review_id": "invented"}},
                        {"verdict": "inconclusive"}, {"attempt_id": None}, {"attempt_id": ""},
                        {"attempt_id": "external-only"}, {"completed_seq": None}, {"completed_seq": True}):
            with self.subTest(changes=changes):
                self.assertIsNone(self.source(current={**value["current"], **changes}))
        with self.assertRaisesRegex(TaskflowError, "persisted acceptance"):
            self.source(current={**value["current"], "scope": "unsigned replacement"})
        with self.changed("attempts", value["new_attempt"]["attempt_id"], lambda row: row.update(role="author")):
            with self.assertRaises(TaskflowError):
                self.source()

    def test_empty_derived_current_context_preserves_legacy_rejection(self):
        value = self.value
        with self.changed("reviews", value["current"]["review_id"], lambda row: row.update(
                derived=True, whole_package=False, snapshot_bound_acceptance=True, composite_quality_coverage={})):
            malformed = value["store"].get_review(value["current"]["review_id"])
            with value["store"]._db() as db:
                with self.assertRaisesRegex(TaskflowError, "later actual current canonical acceptance"):
                    ExecutionStore._review_acceptance_material(
                        db, value["historical"]["review_id"], current_acceptance=malformed)
            self.assertIsNone(self.source(current=malformed))

    def test_raw_current_context_rechecks_actual_routing_and_source_bytes(self):
        value = self.value
        with value["store"]._db() as db:
            actual = ExecutionStore._review_acceptance_material(
                db, value["historical"]["review_id"], current_acceptance=value["current"])
            self.assertEqual(value["historical"], actual)
            with self.assertRaisesRegex(TaskflowError, "actual derived"):
                ExecutionStore._review_acceptance_material(
                    db, value["recheck"], current_acceptance=value["current"])
        path = Path(value["current"]["evidence"][0]["path"])
        raw = path.read_bytes()
        try:
            path.write_bytes(raw + b"source drift")
            with self.assertRaises(TaskflowError):
                self.source()
        finally:
            path.write_bytes(raw)

    def test_historical_graph_must_be_unique_own_closed_source_and_exact_events(self):
        value, store = self.value, self.value["store"]
        for changes in ({"state": "repaired"}, {"closure_review_id": "foreign"},
                        {"repair_candidate_id": "foreign"}):
            with self.changed("defects", value["defects"][0], lambda row, c=changes: row.update(c)):
                self.assertIsNone(self.source())
        with self.changed("reviews", value["historical"]["review_id"],
                          lambda row: row["composite_quality_coverage"].update(private_changes=[])):
            with self.assertRaises(TaskflowError):
                self.source()
        with self.changed("defects", value["defects"][0], lambda row: row.update(closed_seq=row["repair_seq"])):
            with self.assertRaisesRegex(TaskflowError, "original repair/closure events"):
                self.source()
        with store._db(True) as db:
            duplicate = {**value["historical"], "review_id": "duplicate-own-graph"}
            db.execute("INSERT INTO reviews VALUES(?,?,?)",
                       (duplicate["review_id"], duplicate["candidate_id"], canonical(duplicate).decode()))
        try:
            self.assertIsNone(self.source())
        finally:
            with store._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id='duplicate-own-graph'")

    def test_alias_must_be_literal_complete_and_strictly_between_own_graph_and_current_claim(self):
        value = self.value
        unrelated = value["store"].get_candidate(value["negative"]["candidate_id"])["parent_id"]
        with self.changed("candidates", value["new_candidate"]["candidate_id"], lambda row: row.update(parent_id=unrelated)):
            self.assertIsNone(self.source())
        for seq in (value["historical"]["created_seq"], value["new_attempt"]["claimed_seq"]):
            with self.changed("candidates", value["alias"]["candidate_id"], lambda row, seq=seq: row.update(created_seq=seq)):
                self.assertIsNone(self.source())
        for changes in ({"evidence": []}, {"task_id": "foreign"}):
            with self.changed("candidates", value["alias"]["candidate_id"], lambda row, c=changes: row.update(c)):
                with self.assertRaises(TaskflowError):
                    self.source()
        with self.changed("candidates", value["alias"]["candidate_id"], lambda row: row["files"][0].update(role="foreign")):
            with self.assertRaisesRegex(TaskflowError, "complete members"):
                self.source()

    def test_alias_bytes_and_prior_recheck_are_not_replaced_by_matching_digest_metadata(self):
        value = self.value
        paths = [Path(value["alias"]["files"][0]["path"]),
                 Path(value["alias"]["evidence"][0]["path"]),
                 Path(value["historical"]["composite_quality_coverage"]["affected_source"]["report"]["path"])]
        for path in paths:
            raw = path.read_bytes()
            try:
                path.write_bytes(raw + b"drift")
                with self.subTest(path=path), self.assertRaises(TaskflowError):
                    self.source()
            finally:
                path.write_bytes(raw)

    def test_intervening_unconsumed_negative_and_newer_conflict_still_block(self):
        extra = mixed_endpoint_fixture(self, foreign=True)
        with extra["store"]._db() as db:
            with self.assertRaises(TaskflowError):
                ExecutionStore._historical_repaired_quality_source(db, extra["negative"], extra["current"])
        value, store = self.value, self.value["store"]
        later = perform(store, value["case"].root / "newer-conflict", value["new_candidate"]["candidate_id"],
            "quality", "newer-conflict", {"conflict.md": "Actual newer unresolved negative."})
        negative = store.record_review("newer-conflict", value["new_candidate"]["candidate_id"], later["attempt_id"],
            verdict="inconclusive", scope="whole_package", whole_package=True, evidence=later["evidence"])
        try:
            with self.assertRaises(TaskflowError):
                self.source()
            with self.assertRaises(TaskflowError):
                store.quality_gate(value["new_candidate"]["candidate_id"])
        finally:
            with store._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id=?", (negative["review_id"],))


if __name__ == "__main__":
    unittest.main()
