"""Historical own closure crosses proven parent/alias routes, not hash guesses.

All roles are local neutral subprocess fixtures. No external model or production
SQLite is used; old signed records are never rewritten to claim new coverage.
"""

from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical
from taskflow.execution import capture_candidate
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.test_execution_historical_closed_ancestor import descendant_fixture
from tools.execution_simulation import perform


def transitive_fixture(owner):
    v = descendant_fixture(owner)
    case, store = v["case"], v["store"]
    source = next(row for row in v["new_candidate"]["evidence"]
                  if row["location"] == "explicit package declaration")
    baseline = case.root / "second-alias-baseline"
    shutil.copytree(Path(source["path"]).parent, baseline)
    alias = capture_candidate(store, baseline, "second-alias-baseline")
    author = perform(store, case.root / "second-author", alias["candidate_id"], "author", "second-author",
                     {"prompt.md": "Another complete neutral job: original capacity 3, select 2 units.\n"})
    owner.assertEqual("completed", author["state"])
    product = case.root / "second-author-product"
    shutil.copytree(baseline, product)
    shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", product / "draft/input/prompt.md")
    candidate = capture_candidate(store, product, "second-author-product", alias["candidate_id"], author["attempt_id"])
    intent = "second-independent-whole"
    binding = {"intent_id": intent, **{key: candidate[key] for key in
               ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
    route = {"schema_version": 1, **binding, "signer_role": "independent_quality",
             "independent_of_current_author": True, "participated_in_production": False,
             "scope": "whole_package", "outcome": "accepted", "blocking_issue_ids": [],
             "covered_defect_ids": [], "reason": "New current whole; no historical defect closure claimed."}
    fresh = perform(store, case.root / "second-quality", candidate["candidate_id"], "quality", intent,
                    {"adoption-readiness.json": json.dumps({"execution_routing": route}),
                     "issues.json": json.dumps({"execution_binding": binding, "issues": []}),
                     "intake.md": "Independent current neutral role; not the old repair."},
                    extra={"inputs": [{"path": str(product / "task.json"), "kind": "source_metadata",
                                       "target": "execution/task.json"}]})
    owner.assertEqual("completed", fresh["state"])
    refs = [evidence_ref(Path(fresh["runtime_path"]) / "artifacts" / name, "actual independent current whole")
            for name in ("adoption-readiness.json", "issues.json")]
    current = store.record_review("second-current-whole", candidate["candidate_id"], fresh["attempt_id"],
                                 verdict="accepted", whole_package=True, scope="whole_package", evidence=refs)
    return {**v, "intermediate": v["new_candidate"], "first_current": v["current"],
            "current": current, "new_candidate": candidate, "second_alias": alias, "second_author": author}


class TransitiveClosedQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = transitive_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    def source(self, current=None):
        v = self.value
        with v["store"]._db() as db:
            return ExecutionStore._historical_closed_quality_source(db, v["negative"], current or v["current"])

    @contextmanager
    def changed_row(self, table, identity, change, *, capture_event=False):
        store = self.value["store"]
        with store._db(True) as db:
            raw = db.execute("SELECT data FROM " + table + " WHERE id=?", (identity,)).fetchone()["data"]
            value = json.loads(raw)
            change(value)
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
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (raw, identity))
                if event is not None:
                    db.execute("UPDATE events SET data=? WHERE seq=?", (event, value["created_seq"]))

    def test_double_alias_route_reuses_only_original_own_closure_and_preserves_all_sources(self):
        v, store = self.value, self.value["store"]
        original = {"negative": store.get_review(v["negative"]["review_id"]),
                    "closure": store.get_review(v["historical"]["review_id"]),
                    "author": store.get_attempt(v["new_author"]["attempt_id"]),
                    "new_author": store.get_attempt(v["second_author"]["attempt_id"])}
        before = {Path(ref["path"]): Path(ref["path"]).read_bytes() for signed in
                  (v["negative"], v["historical"], v["current"]) for ref in signed["evidence"]}
        witness = self.source()
        self.assertEqual(v["historical"]["review_id"], witness["closure_review_id"])
        self.assertEqual(v["negative"]["review_id"], witness["negative_review_id"])
        route = witness["transitive_route"]
        self.assertEqual(["parent", "same_byte_alias", "parent", "same_byte_alias"],
                         [edge["kind"] for edge in route["edges"]])
        self.assertEqual([v["new_candidate"]["candidate_id"], v["second_alias"]["candidate_id"],
                          v["intermediate"]["candidate_id"], v["alias"]["candidate_id"],
                          v["historical"]["candidate_id"]], [row["candidate_id"] for row in route["nodes"]])
        self.assertTrue(all(row["members"] and row["declaration"]["sha256"] for row in route["nodes"]))
        self.assertEqual([], v["current"]["covered_defect_ids"])
        self.assertEqual(v["current"], store.quality_gate(v["new_candidate"]["candidate_id"], require_repair_alias=True))
        self.assertEqual(original["negative"], store.get_review(v["negative"]["review_id"]))
        self.assertEqual(original["closure"], store.get_review(v["historical"]["review_id"]))
        self.assertEqual(original["author"], store.get_attempt(v["new_author"]["attempt_id"]))
        self.assertEqual(original["new_author"], store.get_attempt(v["second_author"]["attempt_id"]))
        self.assertEqual(before, {path: path.read_bytes() for path in before})
        self.assertEqual(witness, self.source())

    def test_direct_alias_keeps_original_witness_shape(self):
        witness = self.source(current=self.value["first_current"])
        self.assertNotIn("transitive_route", witness)
        self.assertEqual(self.value["historical"]["review_id"], witness["closure_review_id"])

    def test_direct_route_also_requires_its_recorded_capture_events(self):
        v = self.value
        for identity in (v["alias"]["candidate_id"], v["intermediate"]["candidate_id"]):
            with self.subTest(identity=identity), self.changed_row(
                    "candidates", identity, lambda row: row.update(author_attempt="foreign")):
                with self.assertRaisesRegex(TaskflowError, "capture event"):
                    self.source(current=v["first_current"])

    def test_route_cannot_use_alias_captured_before_its_closure(self):
        v = self.value
        with v["store"]._db() as db:
            attempt = ExecutionStore._load(db, "attempts", v["first_current"]["attempt_id"])
            repaired = ExecutionStore._load(db, "candidates", v["historical"]["candidate_id"])
            route = ExecutionStore._closed_quality_alias_route(
                db, v["intermediate"], repaired, attempt["claimed_seq"], v["alias"]["created_seq"])
        self.assertIsNone(route)

    def test_equal_bytes_at_start_without_ancestor_are_not_a_route(self):
        v = self.value
        with v["store"]._db() as db:
            attempt = ExecutionStore._load(db, "attempts", v["first_current"]["attempt_id"])
            repaired = ExecutionStore._load(db, "candidates", v["historical"]["candidate_id"])
            route = ExecutionStore._closed_quality_alias_route(
                db, v["alias"], repaired, attempt["claimed_seq"], 0)
        self.assertIsNone(route)

    def test_intermediate_capture_event_digest_member_role_id_size_path_declaration_reject(self):
        v = self.value
        identity = v["intermediate"]["candidate_id"]
        mutations = [lambda row: row.update(evaluation_digest="0" * 64),
                     lambda row: row.update(evidence=[]),
                     lambda row: row["files"].pop(),
                     lambda row: row["files"][0].update(role="teacher"),
                     lambda row: row["files"][0].update(id="foreign"),
                     lambda row: row["files"][0].update(size=98765),
                     lambda row: row["files"][0].update(package_path="draft/input/foreign.md"),
                     lambda row: row["files"][0].update(path=v["alias"]["files"][0]["path"])]
        for index, mutation in enumerate(mutations):
            # Actual candidate_registered data is matched too, so the negative
            # tests exercise declaration/member validation beyond the event gate.
            with self.subTest(index=index), self.changed_row("candidates", identity, mutation, capture_event=True):
                try:
                    witness = self.source()
                except TaskflowError:
                    pass
                else:
                    self.assertIsNone(witness)
        with self.changed_row("candidates", identity, lambda row: row.update(author_attempt="foreign-captured-author")):
            with self.assertRaisesRegex(TaskflowError, "capture event"):
                self.source()

    def test_intermediate_actual_bytes_and_declaration_drift_reject(self):
        v = self.value
        for path in [Path(v["intermediate"]["files"][0]["path"]),
                     Path(v["intermediate"]["evidence"][0]["path"])]:
            raw = path.read_bytes()
            try:
                path.write_bytes(raw + b"drift")
                with self.subTest(path=path), self.assertRaises(TaskflowError):
                    self.source()
            finally:
                path.write_bytes(raw)

    def test_wrong_task_no_route_and_later_capture_do_not_prove_history(self):
        v = self.value
        for identity, change in [
            (v["second_alias"]["candidate_id"], lambda row: row.update(task_id="foreign")),
            (v["new_candidate"]["candidate_id"], lambda row: row.update(parent_id=None)),
            (v["intermediate"]["candidate_id"], lambda row: row.update(created_seq=v["current"]["completed_seq"])),
        ]:
            with self.subTest(identity=identity), self.changed_row("candidates", identity, change):
                try:
                    witness = self.source()
                except TaskflowError:
                    pass
                else:
                    self.assertIsNone(witness)

    def test_open_forged_closed_missing_coverage_and_late_closure_are_not_transport(self):
        v = self.value
        with self.changed_row("defects", v["defects"][0], lambda row: row.update(state="repaired")):
            self.assertIsNone(self.source())
        with self.changed_row("defects", v["defects"][0], lambda row: row.update(closed_seq=1)):
            with self.assertRaises(TaskflowError):
                self.source()
        with self.changed_row("reviews", v["historical"]["review_id"], lambda row: row.update(covered_defect_ids=[])):
            with self.assertRaises(TaskflowError):
                self.source()
        self.assertIsNone(self.source(current={**v["current"], "completed_seq": v["historical"]["created_seq"]}))
        for change in ({"whole_package": False}, {"derived": True}, {"snapshot_bound_acceptance": True}):
            self.assertIsNone(self.source(current={**v["current"], **change}))
        with self.changed_row("attempts", v["current"]["attempt_id"],
                              lambda row: row.update(cli_sid=v["second_author"]["cli_sid"])):
            with self.assertRaises(TaskflowError):
                self.source()

    def test_later_unclosed_independent_negative_is_still_blocking(self):
        v = self.value
        attempt = perform(v["store"], v["case"].root / "later-unclosed-negative", v["new_candidate"]["candidate_id"],
                          "quality", "later-unclosed", {"conflict.md": "A real later unresolved neutral conflict."})
        signed = v["store"].record_review("later-transitive-unclosed", v["new_candidate"]["candidate_id"],
                    attempt["attempt_id"], verdict="inconclusive", scope="whole_package", whole_package=True,
                    evidence=attempt["evidence"])
        try:
            with self.assertRaises(TaskflowError):
                self.source()
        finally:
            with v["store"]._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id=?", (signed["review_id"],))


class AlternateTransitiveRouteTests(unittest.TestCase):
    def setUp(self):
        from taskflow.packages import check_task

        self.optional = None
        original_capture = capture_candidate

        def add_competing_revision(store, root, name, *args, **kwargs):
            if name == "second-alias-baseline":
                checked = check_task(root, write_report=False)
                with store._db() as db:
                    rows = [json.loads(row["data"]) for row in db.execute(
                        "SELECT data FROM candidates WHERE task_id=?", (checked["task_id"],))]
                matching = [row for row in rows if row.get("parent_id") and all(
                    row[key] == checked[key] for key in
                    ("input_digest", "content_digest", "evaluation_digest"))]
                self.assertEqual(1, len(matching))
                source = matching[0]
                clone = Path(root).parent / "competing-revision"
                shutil.copytree(root, clone)
                self.optional = original_capture(store, clone, "aaa-competing-revision",
                                                 source["parent_id"], source["author_attempt"])
            return original_capture(store, root, name, *args, **kwargs)

        with patch(__name__ + ".capture_candidate", side_effect=add_competing_revision):
            self.value = transitive_fixture(self)

    def source(self):
        with self.value["store"]._db() as db:
            return ExecutionStore._historical_closed_quality_source(
                db, self.value["negative"], self.value["current"])

    @staticmethod
    def drift(candidate):
        path = Path(candidate["files"][0]["path"])
        path.write_bytes(path.read_bytes() + b"drift")

    def test_drifted_optional_branch_does_not_hide_valid_route(self):
        before = self.source()
        self.assertIn(self.optional["candidate_id"],
                      [node["candidate_id"] for node in before["transitive_route"]["nodes"]])
        self.drift(self.optional)
        witness = self.source()
        self.assertIn(self.value["intermediate"]["candidate_id"],
                      [node["candidate_id"] for node in witness["transitive_route"]["nodes"]])
        self.assertNotIn(self.optional["candidate_id"],
                         [node["candidate_id"] for node in witness["transitive_route"]["nodes"]])
        self.assertEqual(self.value["historical"]["review_id"], witness["closure_review_id"])

    def test_all_candidate_paths_invalid_still_rejects(self):
        self.drift(self.optional)
        self.drift(self.value["intermediate"])
        with self.assertRaises(TaskflowError):
            self.source()

    @staticmethod
    def remove_capture(candidate):
        declaration = next(row for row in candidate["evidence"]
                           if row["location"] == "explicit package declaration")
        shutil.rmtree(Path(declaration["path"]).parent)

    def test_missing_optional_capture_directory_does_not_hide_valid_route(self):
        self.remove_capture(self.optional)
        witness = self.source()
        self.assertIn(self.value["intermediate"]["candidate_id"],
                      [node["candidate_id"] for node in witness["transitive_route"]["nodes"]])

    def test_malformed_optional_capture_event_does_not_hide_valid_route(self):
        store = self.value["store"]
        with store._db(True) as db:
            db.execute("UPDATE events SET data=? WHERE seq=?", ("{broken", self.optional["created_seq"]))
        witness = self.source()
        self.assertIn(self.value["intermediate"]["candidate_id"],
                      [node["candidate_id"] for node in witness["transitive_route"]["nodes"]])

    def test_missing_optional_capture_field_does_not_hide_valid_route(self):
        store = self.value["store"]
        with store._db(True) as db:
            row = db.execute("SELECT data FROM candidates WHERE id=?", (self.optional["candidate_id"],)).fetchone()
            value = json.loads(row["data"])
            del value["files"]
            db.execute("UPDATE candidates SET data=? WHERE id=?",
                       (canonical(value).decode(), self.optional["candidate_id"]))
        witness = self.source()
        self.assertIn(self.value["intermediate"]["candidate_id"],
                      [node["candidate_id"] for node in witness["transitive_route"]["nodes"]])

    def test_missing_all_candidate_path_directories_still_rejects(self):
        self.remove_capture(self.optional)
        self.remove_capture(self.value["intermediate"])
        with self.assertRaises(TaskflowError):
            self.source()

    def test_missing_endpoint_directory_cannot_be_bypassed(self):
        self.remove_capture(self.value["new_candidate"])
        with self.assertRaises((TaskflowError, OSError)):
            self.source()

    def test_invalid_endpoint_cannot_be_bypassed_by_alternate(self):
        self.drift(self.value["new_candidate"])
        with self.assertRaises(TaskflowError):
            self.source()


if __name__ == "__main__":
    unittest.main()
