"""Historical graph authenticity is distinct from its current freshness.

Actual neutral role subprocesses, complete captures, repair manifests, SQLite
events and original validators are used. No source/IO/quality validator is
mocked, and these fixtures certify no model or business result.
"""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution import capture_candidate
from taskflow.execution_store import ExecutionStore
from tests import test_execution_historical_closure_freshness as fixtures
from tools.execution_simulation import perform


def graph_descendant_fixture(owner, *, foreign=False):
    value = fixtures.closure_fixture(owner)
    store, case = value["store"], value["case"]
    historical = value["accepted"]
    repaired = store.get_candidate(historical["candidate_id"])
    declaration = next(row for row in repaired["evidence"] if row["location"] == "explicit package declaration")
    baseline = case.root / "new-graph-baseline"
    shutil.copytree(Path(declaration["path"]).parent, baseline)
    alias = capture_candidate(store, baseline, "new-graph-baseline")
    author = perform(store, case.root / "graph-author-role", alias["candidate_id"], "author", "graph-new-author",
                     {"prompt.md": "A new complete neutral commission; retain capacity 3 and select 1 unit.\n"})
    owner.assertEqual("completed", author["state"])
    product = case.root / "new-graph-product"
    shutil.copytree(baseline, product)
    shutil.copyfile(Path(author["runtime_path"]) / "artifacts/prompt.md", product / "draft/input/prompt.md")
    candidate = capture_candidate(store, product, "new-graph-author", alias["candidate_id"], author["attempt_id"])
    foreign_review = None
    if foreign:
        extra = perform(store, case.root / "separate-original-conflict", alias["candidate_id"],
            "quality", "separate-original-conflict", {"conflict.md": "An independent unresolved original conflict."})
        foreign_review = store.record_review("separate-original-conflict", alias["candidate_id"], extra["attempt_id"],
            verdict="inconclusive", scope="whole_package", whole_package=True, evidence=extra["evidence"])
    definition = copy.deepcopy(value["flow"]["definition"])
    old_baseline = definition["initial"]["candidate_root"]
    script = case.root / "new-graph-role.py"
    old_script = Path(definition["roles"]["quality"]["runner"]["dependencies"][0])
    script.write_text(old_script.read_text().replace(old_baseline, str(product)).replace(
        "current actual corrected support.", "new descendant actual corrected support."), encoding="utf-8")
    definition.update(flow_id="new-graph-flow", directory=str(case.root / "new-graph-flow"))
    definition["initial"] = {"candidate_id": candidate["candidate_id"], "candidate_root": str(product)}
    for role in definition["roles"].values():
        role["runner"]["dependencies"] = [str(script)]
        role["runner"]["argv"][1] = str(script)
    case.register(definition)
    case.run_flow([definition["flow_id"]])
    flow = store.get_flow(definition["flow_id"])
    owner.assertEqual("accepted", flow["state"]["status"], flow["state"])
    history = [row for row in flow["state"]["history"] if row["kind"] == "independent_review"]
    current = store.compose_repaired_quality(flow["state"]["candidate_id"], history[0]["review_id"],
        flow["state"]["accepted_review_id"], flow["state"]["repaired_defects"], evidence=case.refs)
    return {**value, "historical": historical, "alias": alias, "author": author,
            "current": current, "new_flow": flow, "new_whole": store.get_review(history[0]["review_id"]),
            "negative": store.get_review(value["whole"]), "foreign": foreign_review}


class HistoricalRepairedSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = graph_descendant_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    @contextmanager
    def changed(self, table, identity, change):
        store = self.value["store"]
        with store._db(True) as db:
            raw = db.execute("SELECT data FROM " + table + " WHERE id=?", (identity,)).fetchone()["data"]
            value = json.loads(raw); change(value)
            db.execute("UPDATE " + table + " SET data=? WHERE id=?", (canonical(value).decode(), identity))
        try:
            yield
        finally:
            with store._db(True) as db:
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (raw, identity))

    def source(self, negative=None, current=None):
        value = self.value
        with value["store"]._db() as db:
            return ExecutionStore._historical_repaired_quality_source(
                db, negative or value["negative"], current or value["current"])

    def test_actual_two_graphs_keep_old_freshness_rejected_but_current_gate_uses_own_closed_source(self):
        value, store = self.value, self.value["store"]
        store.set_control("work:separately-paused-solver", "pause", "Independent business hold retained.")
        before = store.dashboard()
        with store._db() as db:
            controls = [dict(row) for row in db.execute("SELECT * FROM controls")]
        with self.assertRaises(TaskflowError):
            store.assert_review_current(value["historical"]["review_id"])
        self.assertEqual(value["current"], store.quality_gate(value["current"]["candidate_id"], require_repair_alias=True))
        self.assertFalse(store.get_review(value["historical"]["review_id"])["whole_package"])
        self.assertFalse(store.get_review(value["current"]["review_id"])["whole_package"])
        self.assertEqual(before, store.dashboard())
        with store._db() as db:
            self.assertEqual(controls, [dict(row) for row in db.execute("SELECT * FROM controls")])
        witness = self.source()
        self.assertEqual("verified_historical_repaired_graph_ancestor", witness["kind"])
        self.assertFalse(witness["historical_currentness_claimed"])
        self.assertEqual(value["historical"]["composite_quality_coverage"], witness["historical_graph"])
        self.assertEqual(value["current"]["composite_quality_coverage"], witness["current_graph"])
        self.assertIn(value["alias"]["candidate_id"], [row["candidate_id"] for row in witness["ancestor_aliases"]])

    def test_unclosed_nonwhole_unmanaged_or_foreign_scope_is_not_historical_proof(self):
        value = self.value
        for changes in ({"verdict": "inconclusive"}, {"whole_package": False}, {"review_id": "no-own-defects"}):
            self.assertIsNone(self.source(negative={**value["negative"], **changes}))
        for changes in ({"whole_package": True}, {"derived": False}, {"snapshot_bound_acceptance": False},
                        {"completed_seq": None}, {"completed_seq": True}, {"composite_quality_coverage": None}):
            self.assertIsNone(self.source(current={**value["current"], **changes}))
        self.assertIsNone(self.source(negative=value["new_whole"]))
        for changes in ({"state": "repaired"}, {"closure_review_id": None},
                        {"repair_candidate_id": "foreign-candidate"}):
            with self.changed("defects", value["defects"][0], lambda row, c=changes: row.update(c)):
                self.assertIsNone(self.source())
                with self.assertRaises(TaskflowError):
                    value["store"].quality_gate(value["current"]["candidate_id"], require_repair_alias=True)
        with self.changed("reviews", value["historical"]["review_id"],
                          lambda row: row.update(whole_package=True)):
            self.assertIsNone(self.source())
        with value["store"]._db(True) as db:
            duplicate = {**value["historical"], "review_id": "duplicate-own-closure"}
            db.execute("INSERT INTO reviews VALUES(?,?,?)",
                       (duplicate["review_id"], duplicate["candidate_id"], canonical(duplicate).decode()))
        try:
            self.assertIsNone(self.source())
        finally:
            with value["store"]._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id='duplicate-own-closure'")

    def test_only_literal_complete_alias_after_original_closure_before_new_source_can_bind(self):
        value = self.value
        unrelated = value["store"].get_candidate(value["negative"]["candidate_id"])["parent_id"]
        with self.changed("candidates", value["current"]["candidate_id"], lambda row: row.update(parent_id=unrelated)):
            self.assertIsNone(self.source())
        for seq in (value["historical"]["created_seq"], value["current"]["created_seq"]):
            with self.changed("candidates", value["alias"]["candidate_id"],
                              lambda row, seq=seq: row.update(created_seq=seq)):
                self.assertIsNone(self.source())
        with self.changed("defects", value["defects"][0],
                          lambda row: row.update(closed_seq=value["historical"]["created_seq"])):
            self.assertIsNone(self.source())
        with self.changed("reviews", value["historical"]["review_id"],
                          lambda row: row.update(created_seq=value["current"]["completed_seq"])):
            self.assertIsNone(self.source())
        with self.changed("candidates", value["alias"]["candidate_id"], lambda row: row.update(evidence=[])):
            with self.assertRaisesRegex(TaskflowError, "actual declaration"):
                self.source()
        with self.changed("candidates", value["alias"]["candidate_id"],
                          lambda row: row["files"][0].update(role="foreign-role")):
            with self.assertRaisesRegex(TaskflowError, "complete members"):
                self.source()
        for path in (Path(value["alias"]["files"][0]["path"]),
                     Path(value["alias"]["evidence"][0]["path"])):
            raw = path.read_bytes()
            try:
                path.write_bytes(raw + b"drift")
                with self.subTest(path=path.name), self.assertRaises(TaskflowError):
                    self.source()
            finally:
                path.write_bytes(raw)

    def test_old_graph_events_and_current_graph_cannot_be_replaced_by_stored_summaries(self):
        value = self.value
        for target in ("historical", "current"):
            with self.changed("reviews", value[target]["review_id"],
                              lambda row: row["composite_quality_coverage"].update(private_changes=[])):
                with self.subTest(target=target), self.assertRaises(TaskflowError):
                    self.source()
        with self.changed("defects", value["defects"][0], lambda row: row.update(closed_seq=row["repair_seq"])):
            with self.assertRaisesRegex(TaskflowError, "original repair/closure events"):
                self.source()
        for signed in (value["historical"], value["current"]):
            graph = signed["composite_quality_coverage"]
            path = Path(graph["affected_source"]["report"]["path"])
            raw = path.read_bytes()
            try:
                path.write_bytes(raw + b"drift")
                with self.subTest(review=signed["review_id"]), self.assertRaises(TaskflowError):
                    self.source()
            finally:
                path.write_bytes(raw)

    def test_context_is_actual_default_validated_current_graph_not_a_freshness_skip(self):
        value = self.value
        store, old = value["store"], value["historical"]
        with store._db() as db:
            with self.assertRaises(TaskflowError):
                ExecutionStore._review_acceptance_material(db, old["review_id"])
            for context in (True, {}, {**value["current"], "whole_package": True},
                            {**value["current"], "completed_seq": old["created_seq"]},
                            {**value["current"], "scope": "unsigned replacement scope"}):
                with self.subTest(context=context), self.assertRaises(TaskflowError):
                    ExecutionStore._review_acceptance_material(db, old["review_id"], current_acceptance=context)
            with self.assertRaisesRegex(TaskflowError, "actual derived"):
                ExecutionStore._review_acceptance_material(db, value["recheck"], current_acceptance=value["current"])
            actual = ExecutionStore._review_acceptance_material(
                db, old["review_id"], current_acceptance=value["current"])
            self.assertEqual(old, actual)
        # The historical source was authenticated only in this bounded context;
        # ordinary freshness and all persisted fields remain unchanged.
        with self.assertRaises(TaskflowError):
            store.assert_review_current(old["review_id"])

    def test_later_unclosed_and_foreign_negative_are_not_discarded(self):
        value, store = self.value, self.value["store"]
        extra = graph_descendant_fixture(self, foreign=True)
        with extra["store"]._db() as db:
            with self.assertRaisesRegex(TaskflowError, "not an earlier consumed whole finding"):
                ExecutionStore._historical_repaired_quality_source(db, extra["negative"], extra["current"])
        later = perform(store, value["case"].root / "later-unclosed-source", value["current"]["candidate_id"],
            "quality", "later-unclosed", {"conflict.md": "A new actual independent unresolved conflict."})
        signed = store.record_review("later-unclosed-negative", value["current"]["candidate_id"], later["attempt_id"],
            verdict="inconclusive", scope="whole_package", whole_package=True, evidence=later["evidence"])
        try:
            with self.assertRaises(TaskflowError):
                self.source()
            with self.assertRaises(TaskflowError):
                store.quality_gate(value["current"]["candidate_id"])
        finally:
            with store._db(True) as db:
                db.execute("DELETE FROM reviews WHERE id=?", (signed["review_id"],))


if __name__ == "__main__":
    unittest.main()
