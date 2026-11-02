"""An old finding uses its own actual closure, never a new author's acceptance."""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, canonical, read_json
from taskflow.execution import capture_candidate
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_historical_closure_freshness as fixtures
from tools.execution_simulation import perform


def descendant_fixture(owner):
    value = fixtures.closure_fixture(owner, whole=True)
    case, store = value["case"], value["store"]
    historical = store.get_review(value["recheck"])
    repaired = store.get_candidate(historical["candidate_id"])
    declaration = next(x for x in repaired["evidence"] if x["location"] == "explicit package declaration")
    root = case.root / "new-author-baseline"
    shutil.copytree(Path(declaration["path"]).parent, root)
    alias = capture_candidate(store, root, "new-author-baseline")
    author = perform(store, case.root / "new-author-role", alias["candidate_id"], "author", "new-author",
                     {"prompt.md": "A new complete neutral commission; preserve capacity 3 and choose 1 unit.\n"})
    owner.assertEqual("completed", author["state"])
    product = case.root / "new-author-product"
    shutil.copytree(root, product)
    shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", product / "draft/input/prompt.md")
    candidate = capture_candidate(store, product, "new-author-product", alias["candidate_id"], author["attempt_id"])
    intent = "new-independent-whole"
    binding = {"intent_id": intent, **{key: candidate[key] for key in
               ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
    routing = {"schema_version": 1, **binding, "signer_role": "independent_quality",
               "independent_of_current_author": True, "participated_in_production": False,
               "scope": "whole_package", "outcome": "accepted", "blocking_issue_ids": [],
               "covered_defect_ids": [], "reason": "New neutral package independently inspected; no historical closure claimed."}
    fresh = perform(store, case.root / "new-whole-role", candidate["candidate_id"], "quality", intent,
                    {"adoption-readiness.json": json.dumps({"execution_routing": routing}),
                     "issues.json": json.dumps({"execution_binding": binding, "issues": []}),
                     "intake.md": "Neutral current whole source, not a historical repair."},
                    extra={"inputs": [{"path": str(product / "task.json"), "kind": "source_metadata",
                                       "target": "execution/task.json"}]})
    owner.assertEqual("completed", fresh["state"])
    refs = [evidence_ref(Path(fresh["runtime_path"]) / "artifacts" / name, "original neutral independent source")
            for name in ("adoption-readiness.json", "issues.json")]
    current = store.record_review("new-current-whole", candidate["candidate_id"], fresh["attempt_id"],
                                 verdict="accepted", scope="new separately authorized whole", whole_package=True,
                                 evidence=refs)
    negative = store.get_review(value["whole"])
    return {**value, "current": current, "new_candidate": candidate, "alias": alias,
            "historical": historical, "negative": negative, "new_author": author}


class HistoricalClosedAncestorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = descendant_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    def source(self, negative=None, current=None):
        v = self.value
        with v["store"]._db() as db:
            return ExecutionStore._historical_closed_quality_source(
                db, negative or v["negative"], current or v["current"])

    @contextmanager
    def changed_row(self, table, identity, change, *, capture_event=False):
        store = self.value["store"]
        with store._db(True) as db:
            original = db.execute("SELECT data FROM " + table + " WHERE id=?", (identity,)).fetchone()["data"]
            value = json.loads(original); change(value)
            db.execute("UPDATE " + table + " SET data=? WHERE id=?", (canonical(value).decode(), identity))
            event = None
            if capture_event:
                event = db.execute("SELECT data FROM events WHERE seq=?", (value["created_seq"],)).fetchone()["data"]
                db.execute("UPDATE events SET data=? WHERE seq=?", (canonical({key: item for key, item in value.items()
                           if key != "created_seq"}).decode(), value["created_seq"]))
        try:
            yield
        finally:
            with store._db(True) as db:
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (original, identity))
                if event is not None:
                    db.execute("UPDATE events SET data=? WHERE seq=?", (event, value["created_seq"]))

    def test_new_author_whole_reuses_only_the_negatives_actual_closed_ancestor(self):
        v, store = self.value, self.value["store"]
        store.set_control("work:future-neutral-solver", "pause", "Separate future business finding remains held.")
        before = store.dashboard()
        witness = self.source()
        self.assertEqual("verified_historical_closed_negative_ancestor", witness["kind"])
        self.assertEqual(v["historical"]["review_id"], witness["closure_review_id"])
        self.assertEqual(v["negative"]["review_id"], witness["negative_review_id"])
        self.assertIn(v["alias"]["candidate_id"], [x["candidate_id"] for x in witness["ancestor_aliases"]])
        self.assertEqual([], v["current"]["covered_defect_ids"])
        self.assertEqual("author", v["new_author"]["role"])
        self.assertEqual(v["current"], store.quality_gate(v["new_candidate"]["candidate_id"], require_repair_alias=True))
        self.assertEqual(before, store.dashboard())
        with store._db() as db:
            self.assertEqual("pause", json.loads(db.execute(
                "SELECT data FROM controls WHERE scope='work:future-neutral-solver'").fetchone()["data"])["action"])
            ExecutionStore._assert_consumed_quality_negative(db, v["negative"], v["historical"])
            with self.assertRaisesRegex(TaskflowError, "exact current whole repair"):
                ExecutionStore._assert_consumed_quality_negative(db, v["negative"], v["current"])

    def test_unclosed_multiple_closures_and_wrong_original_scope_do_not_select_a_source(self):
        v = self.value
        defect_id = v["defects"][0]
        with self.changed_row("defects", defect_id, lambda row: row.update(state="repaired")):
            self.assertIsNone(self.source())
            with self.assertRaises(TaskflowError):
                v["store"].quality_gate(v["new_candidate"]["candidate_id"])
        for changes in ({"verdict": "inconclusive"}, {"whole_package": False}):
            self.assertIsNone(self.source(negative={**v["negative"], **changes}))
        for changes in ({"whole_package": False}, {"derived": True}, {"snapshot_bound_acceptance": True}):
            self.assertIsNone(self.source(current={**v["current"], **changes}))
        with v["store"]._db(True) as db:
            original = json.loads(db.execute("SELECT data FROM defects WHERE id=?", (defect_id,)).fetchone()["data"])
            second = {**original, "defect_id": "foreign-closure", "closure_review_id": "another-closure"}
            db.execute("INSERT INTO defects VALUES(?,?,?,?)",
                       (second["defect_id"], second["task_id"], second["state"], canonical(second).decode()))
        try:
            self.assertIsNone(self.source())
        finally:
            with v["store"]._db(True) as db:
                db.execute("DELETE FROM defects WHERE id='foreign-closure'")

    def test_absent_defect_wrong_closure_and_order_do_not_match(self):
        v = self.value
        self.assertIsNone(self.source(negative={**v["negative"], "review_id": "no-actual-defects"}))
        self.assertIsNone(self.source(current=v["historical"]))
        self.assertIsNone(self.source(current={**v["current"], "completed_seq": v["historical"]["created_seq"]}))
        historical_id = v["historical"]["review_id"]
        for changes in ({"whole_package": False}, {"derived": True}, {"verdict": "inconclusive"}):
            with self.changed_row("reviews", historical_id, lambda row, c=changes: row.update(c)):
                self.assertIsNone(self.source())
        with self.changed_row("defects", v["defects"][0],
                              lambda row: row.update(repair_candidate_id="not-that-closed-candidate")):
            self.assertIsNone(self.source())

    def test_external_review_without_managed_completion_metadata_is_not_a_history_proof(self):
        v = self.value
        variants = [("completed_seq", None), ("completed_seq", True), ("completed_seq", False),
                    ("attempt_id", None), ("attempt_id", ""), ("attempt_id", True),
                    ("attempt_id", "not-a-managed-attempt")]
        for key in ("completed_seq", "attempt_id"):
            current = {name: item for name, item in v["current"].items() if name != key}
            with self.subTest(target="current", absent=key):
                self.assertIsNone(self.source(current=current))
            with self.changed_row("reviews", v["historical"]["review_id"], lambda row, key=key: row.pop(key)):
                with self.subTest(target="closure", absent=key):
                    self.assertIsNone(self.source())
        for key, value in variants:
            with self.subTest(target="current", key=key, value=value):
                self.assertIsNone(self.source(current={**v["current"], key: value}))
            with self.changed_row("reviews", v["historical"]["review_id"],
                                  lambda row, key=key, value=value: row.update({key: value})):
                with self.subTest(target="closure", key=key, value=value):
                    self.assertIsNone(self.source())

    def test_equal_hash_elsewhere_cannot_replace_an_actual_ancestor(self):
        v = self.value
        unrelated = v["store"].get_candidate(v["negative"]["candidate_id"])["parent_id"]
        # The real exact alias still exists in the database, outside this forged
        # candidate's literal parent chain. It cannot be picked by a hash search.
        with self.changed_row("candidates", v["new_candidate"]["candidate_id"],
                              lambda row: row.update(parent_id=unrelated)):
            with self.assertRaisesRegex(TaskflowError, "capture event"):
                self.source()
        with self.changed_row("candidates", v["alias"]["candidate_id"],
                              lambda row: row.update(task_id="foreign-task")):
            # Task identity is part of a valid actual alias, not merely three digests.
            with self.assertRaises(TaskflowError):
                self.source()

    def test_alias_declaration_member_metadata_and_payload_bytes_are_rechecked(self):
        v = self.value
        alias_id = v["alias"]["candidate_id"]
        with self.changed_row("candidates", alias_id, lambda row: row["files"][0].update(role="different-role"),
                              capture_event=True):
            with self.assertRaisesRegex(TaskflowError, "member path/id/role"):
                self.source()
        with self.changed_row("candidates", alias_id, lambda row: row.update(evidence=[]), capture_event=True):
            with self.assertRaisesRegex(TaskflowError, "captured declaration"):
                self.source()
        for path in [Path(v["alias"]["files"][0]["path"]),
                     Path(v["alias"]["evidence"][0]["path"])]:
            raw = path.read_bytes()
            try:
                path.write_bytes(raw + b"drift")
                with self.subTest(path=path.name), self.assertRaises(TaskflowError):
                    self.source()
            finally:
                path.write_bytes(raw)

    def test_forged_closed_events_current_source_and_later_negative_are_not_absorbed(self):
        v = self.value
        with self.changed_row("defects", v["defects"][0], lambda row: row.update(closed_seq=1)):
            with self.assertRaises(TaskflowError):
                self.source()
        current_report = Path(v["current"]["evidence"][0]["path"])
        raw = current_report.read_bytes()
        try:
            current_report.write_bytes(raw + b"drift")
            with self.assertRaises(TaskflowError):
                self.source()
        finally:
            current_report.write_bytes(raw)
        later = perform(v["store"], v["case"].root / "later-actual-negative",
                        v["new_candidate"]["candidate_id"], "quality", "later-negative",
                        {"conflict.md": "A later unresolved independent conflict."})
        signed = v["store"].record_review("later-unresolved", v["new_candidate"]["candidate_id"],
                    later["attempt_id"], verdict="inconclusive", scope="later whole",
                    whole_package=True, evidence=later["evidence"])
        try:
            with self.assertRaises(TaskflowError):
                self.source()
            with self.assertRaises(TaskflowError):
                v["store"].quality_gate(v["new_candidate"]["candidate_id"])
        finally:
            # This deliberate adversarial review is isolated to the disposable DB.
            with v["store"]._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id=?", (signed["review_id"],))


if __name__ == "__main__":
    unittest.main()

