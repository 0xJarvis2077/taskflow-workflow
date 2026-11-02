"""Historical closure reuse requires the exact current repair, never recency alone.

The integration fixtures use sealed local Python role subprocesses, SQLite and
captured packages. They do not replace safe IO, review validators or quality gates.
"""

from contextlib import closing
import copy
import json
from pathlib import Path
import shutil
import sqlite3
import unittest

from taskflow.contracts import TaskflowError
from taskflow.execution import capture_candidate
from taskflow.execution_store import ExecutionStore
from tests import test_execution_composite_quality as composite
from tests import test_execution_closure_coverage_store as chains
from tests import test_execution_flow as flows
from tools.execution_simulation import perform


def closure_fixture(owner, *, whole=False, intervening=False, chained=False):
    case = flows.ContinuationTests()
    prior = None
    if chained:
        helper = chains.ClosureCoverageStoreTests()
        prior = helper.make_closed()
        owner.addCleanup(helper.doCleanups)
        case.store, case.refs = prior["store"], prior["refs"]
        case.root = case.store.path.parent / "current-fixture"
        case.root.mkdir()
        case.script = case.root / "role.py"
        case.script.write_text(flows.ROLE_FIXTURE, encoding="utf-8")
    else:
        case.setUp()
        owner.addCleanup(case.doCleanups)
    base = case.definition("neutral")

    def definition(name, root, *, public=False):
        script = case.root / (name + "-role.py")
        prefix = '''
import pathlib,json,hashlib
def signed_report(route,ctx,inputs):
 report={'execution_routing':route,'limits':['Local mechanical fixture; no business certification.']}
 if route['scope']=='affected':
  baseline=pathlib.Path(BASELINE); declaration=json.loads((baseline/'task.json').read_text())
  changed=[];evidence=[]
  for item in declaration['files']:
   path=item['path'];old=digest(baseline/path);new=digest(inputs/path)
   if old!=new:
    changed.append({'path':path,'old':old,'new':new})
    evidence.append({'path':path,'sha256':new,'location':'actual current private repair'})
  report['coverage']={'changed_files':changed}
  report['defect_resolution']={'affected_paths':[row['path'] for row in changed],'evidence':evidence}
 return report
'''.replace("BASELINE", repr(str(root)))
        source = flows.ROLE_FIXTURE.replace("{'execution_routing':route}", "signed_report(route,ctx,inputs)")
        source = source.replace("save(root/'task.json',task)", "(root/'task.json').write_bytes((inputs/'execution/task.json').read_bytes())")
        source = source.replace("Actual repaired private support.", name + " actual corrected support.")
        if public:
            source = source.replace("draft/evaluation/answer.md", "draft/input/prompt.md").replace("'kind':'reference'", "'kind':'material'")
        script.write_text(prefix + source, encoding="utf-8")
        value = copy.deepcopy(base)
        value.update(flow_id=name, directory=str(case.root / (name + "-flow")))
        value["initial"] = {"candidate_id": name + "-initial", "candidate_root": str(root)}
        for role in value["roles"].values():
            role["runner"]["dependencies"] = [str(script)]
            role["runner"]["argv"][1] = str(script)
        if public:
            role = copy.deepcopy(value["roles"]["recheck"])
            role["scope"] = "whole_package"
            index = role["runner"]["argv"].index("--scope") + 1
            role["runner"]["argv"][index] = "whole_package"
            value["roles"]["recheck_public"] = role
            value["roles"]["public_rebuild"] = copy.deepcopy(role)
        return value

    if prior:
        candidate = case.store.get_candidate(prior["candidate"])
        declaration = next(row for row in candidate["evidence"] if row.get("location") == "explicit package declaration")
        archived = {"state": {"accepted_review_id": prior["recheck"], "candidate_root": str(Path(declaration["path"]).parent)}}
    else:
        old = definition("archival", Path(base["initial"]["candidate_root"]))
        case.register(old)
        case.run_flow([old["flow_id"]])
        archived = case.store.get_flow(old["flow_id"])
        owner.assertEqual("accepted", archived["state"]["status"], archived["state"])
    alias = case.root / "current-baseline"
    shutil.copytree(archived["state"]["candidate_root"], alias)
    current = definition("current", alias, public=whole)
    initial = capture_candidate(case.store, alias, current["initial"]["candidate_id"])
    if intervening:
        attempt = perform(case.store, case.root / "intervening", initial["candidate_id"], "quality", "unaccounted",
                          {"conflict.md": "A separate independent conflict, outside the consumed repair."})
        case.store.record_review("unaccounted", initial["candidate_id"], attempt["attempt_id"], verdict="inconclusive",
                                 whole_package=True, scope="whole_package", evidence=attempt["evidence"])
    case.register(current)
    case.run_flow([current["flow_id"]])
    flow = case.store.get_flow(current["flow_id"])
    owner.assertEqual("accepted", flow["state"]["status"], flow["state"])
    history = [row for row in flow["state"]["history"] if row["kind"] == "independent_review"]
    value = {"case": case, "store": case.store, "flow": flow, "candidate": flow["state"]["candidate_id"],
             "whole": history[0]["review_id"], "recheck": flow["state"]["accepted_review_id"],
             "defects": flow["state"]["repaired_defects"], "refs": case.refs, "archived": archived}
    if not whole and not intervening:
        value["accepted"] = case.store.compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
                                                                value["defects"], evidence=case.refs)
    elif whole:
        value["accepted"] = case.store.get_review(value["recheck"])
    return value


class ConsumedQualityDefectTests(unittest.TestCase):
    """Pure matching policy can also run on hosts denying directory-descriptor IO."""

    def material(self):
        negative = {"review_id": "finding", "candidate_id": "initial", "created_seq": 1}
        current = {"review_id": "accepted", "candidate_id": "repaired", "covered_defect_ids": ["defect"],
                   "created_seq": 10, "completed_seq": 9}
        original = {"candidate_id": "initial", "task_id": "neutral", "input_digest": "old", "evaluation_digest": "old"}
        candidate = {"candidate_id": "repaired", "parent_id": "initial", "task_id": "neutral", "author_attempt": "author",
                     "created_seq": 6, "input_digest": "new", "evaluation_digest": "new"}
        issue = {"id": "raw", "kind": "material", "responsibility": "Retain the stated condition.", "evidence": [{"path": "input"}]}
        defect = {**issue, "defect_id": "defect", "review_id": "finding", "candidate_id": "initial", "state": "closed",
                  "repair_candidate_id": "repaired", "closure_review_id": "accepted", "author_attempt": "author",
                  "disposition": "material_repair", "opened_seq": 2, "repair_seq": 7, "closed_seq": 11}
        author = {"attempt_id": "author", "state": "completed", "role": "repair", "candidate_id": "initial",
                  "claimed_seq": 3, "last_seq": 5, "cli_sid": "author-context"}
        fresh = {"claimed_seq": 8, "last_seq": 9, "cli_sid": "independent-context"}
        return [negative, current, candidate, [issue], [defect], author, fresh, original]

    def test_exact_all_findings_match_once(self):
        self.assertEqual(["defect"], ExecutionStore._consumed_quality_defect_ids(*self.material()))
        args = self.material()
        args[3][0]["kind"] = args[4][0]["kind"] = "reference"
        args[4][0]["disposition"] = "private_repair"
        self.assertEqual(["defect"], ExecutionStore._consumed_quality_defect_ids(*args))

    def test_wrong_identity_order_coverage_and_unclosed_records_reject(self):
        changes = [(1, "candidate_id", "other"), (1, "covered_defect_ids", []),
                   (1, "covered_defect_ids", ["defect", "defect"]), (2, "parent_id", "other"),
                   (2, "author_attempt", "foreign"), (2, "task_id", "foreign"), (5, "state", "failed"),
                   (5, "role", "author"), (5, "candidate_id", "other"), (5, "cli_sid", "independent-context"),
                   (6, "last_seq", 12), (7, "candidate_id", "other")]
        for index, key, value in changes:
            args = self.material(); args[index][key] = value
            with self.subTest(index=index, key=key), self.assertRaises(TaskflowError):
                ExecutionStore._consumed_quality_defect_ids(*args)
        for key, value in {"state": "repaired", "repair_candidate_id": "foreign", "closure_review_id": "foreign",
                           "author_attempt": "foreign", "candidate_id": "foreign", "review_id": "foreign",
                           "opened_seq": 4, "repair_seq": 8, "closed_seq": 9, "evidence": [],
                           "disposition": "private_repair"}.items():
            args = self.material(); args[4][0][key] = value
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                ExecutionStore._consumed_quality_defect_ids(*args)

    def test_empty_duplicate_and_uncovered_findings_reject(self):
        for index, value in ((4, []), (3, [])):
            args = self.material(); args[index] = value
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                ExecutionStore._consumed_quality_defect_ids(*args)
        for index in (3, 4):
            args = self.material(); args[index].append(copy.deepcopy(args[index][0]))
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                ExecutionStore._consumed_quality_defect_ids(*args)
        args = self.material()
        args[3].append({"id": "uncovered", "kind": "reference", "responsibility": "Another finding.", "evidence": []})
        with self.assertRaisesRegex(TaskflowError, "uncovered"):
            ExecutionStore._consumed_quality_defect_ids(*args)


class ClosedQualityEventTests(unittest.TestCase):
    def view(self, *, chained=False):
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.row_factory = sqlite3.Row
        db.execute("CREATE TABLE events(seq INTEGER PRIMARY KEY,kind TEXT,data TEXT)")
        row = {"defect_id": "neutral", "candidate_id": "original", "review_id": "finding",
               "repair_candidate_id": "repaired", "author_attempt": "author", "repair_seq": 1,
               "closed_seq": 2, "closure_review_id": "closure", "closure_evidence": [],
               "repair_evidence": [], "disposition": "private_repair", "changed_files": []}
        if chained:
            row["closure_coverage_chain"] = {"final_review_id": "closure", "kind": "whole_then_same_public_affected"}
        db.execute("INSERT INTO events VALUES(1,'defect_repaired',?)", (json.dumps(row),))
        db.execute("INSERT INTO events VALUES(2,?,?)", ("defect_closed_with_coverage_chain" if chained else "defect_closed", json.dumps(row)))
        return db, row

    def test_normal_and_chained_event_identity_is_required(self):
        for chained in (False, True):
            db, row = self.view(chained=chained)
            ExecutionStore._assert_closed_quality_record(db, row)
            for key, value in (("closed_seq", 999), ("repair_seq", 999), ("closure_review_id", "foreign"),
                               ("author_attempt", "foreign"), ("repair_candidate_id", "foreign")):
                with self.subTest(chained=chained, key=key), self.assertRaisesRegex(TaskflowError, "original repair/closure events"):
                    ExecutionStore._assert_closed_quality_record(db, {**row, key: value})

    def test_unrelated_event_cannot_prove_closure(self):
        db, row = self.view()
        db.execute("UPDATE events SET kind='unrelated' WHERE seq=2")
        with self.assertRaises(TaskflowError):
            ExecutionStore._assert_closed_quality_record(db, row)


class HistoricalClosureFreshnessTests(unittest.TestCase):
    def test_original_whole_affected_chain_is_revalidated_under_current_composite(self):
        value = closure_fixture(self, chained=True)
        accepted = value["store"].quality_gate(value["candidate"], require_repair_alias=True)
        self.assertEqual(value["accepted"], accepted)
        self.assertFalse(accepted["whole_package"])
        with self.assertRaisesRegex(TaskflowError, "later independent"):
            value["store"].assert_review_current(value["archived"]["state"]["accepted_review_id"])

    def test_exact_composite_and_whole_consume_only_their_original_negative(self):
        for whole in (False, True):
            with self.subTest(whole=whole):
                value = closure_fixture(self, whole=whole)
                store = value["store"]
                before = store.dashboard()
                with store._db() as db:
                    raw = {table: [row["data"] for row in db.execute("SELECT data FROM " + table)]
                           for table in ("candidates", "reviews", "defects", "attempts")}
                accepted = store.quality_gate(value["candidate"], require_repair_alias=True)
                self.assertEqual(value["accepted"], accepted)
                self.assertEqual(whole, accepted["whole_package"])
                self.assertEqual(accepted, store.assert_review_current(accepted["review_id"]))
                # Archival acceptance itself remains stale. Only reuse of its
                # authentic closure inside the current gate has bounded context.
                old = value["archived"]["state"]["accepted_review_id"]
                with self.assertRaisesRegex(TaskflowError, "later independent"):
                    store.assert_review_current(old)
                self.assertEqual(before, store.dashboard())
                with store._db() as db:
                    self.assertEqual(raw, {table: [row["data"] for row in db.execute("SELECT data FROM " + table)] for table in raw})

    def test_later_negative_cannot_be_washed_by_current_whole_or_composite(self):
        for whole in (False, True):
            value = closure_fixture(self, whole=whole)
            composite.CompositeQualityTests().later_negative(value)
            with self.subTest(whole=whole), self.assertRaises(TaskflowError):
                value["store"].quality_gate(value["candidate"])

    def test_unaccounted_intermediate_negative_still_blocks_current_whole(self):
        value = closure_fixture(self, whole=True, intervening=True)
        with self.assertRaisesRegex(TaskflowError, "earlier consumed whole"):
            value["store"].quality_gate(value["candidate"])

    def test_current_source_and_original_alias_bytes_are_revalidated(self):
        for whole in (False, True):
            value = closure_fixture(self, whole=whole)
            store = value["store"]
            alias = store.get_candidate(value["flow"]["definition"]["initial"]["candidate_id"])
            paths = [Path(alias["files"][0]["path"]), Path(store.get_review(value["recheck"])["evidence"][0]["path"])]
            for path in paths:
                raw = path.read_bytes(); path.write_bytes(raw + b"drift")
                try:
                    with self.subTest(whole=whole, path=path.name), self.assertRaises((TaskflowError, ValueError)):
                        store.quality_gate(value["candidate"])
                finally:
                    path.write_bytes(raw)

    def test_forged_unclosed_or_mismatched_current_closure_rejects(self):
        value = closure_fixture(self, whole=True)
        store = value["store"]
        # Corruption lives only in disposable SQLite views, never in the source.
        for change in ({"state": "repaired"}, {"closure_review_id": value["whole"]},
                       {"author_attempt": "foreign"}, {"repair_seq": 0}, {"closed_seq": 999999}):
            with store._db() as source:
                with closing(sqlite3.connect(":memory:")) as db, db:
                    db.row_factory = sqlite3.Row
                    source.backup(db)
                    defect = ExecutionStore._load(db, "defects", value["defects"][0])
                    defect.update(change)
                    db.execute("UPDATE defects SET data=? WHERE id=?", (json.dumps(defect), defect["defect_id"]))
                    with self.subTest(change=change), self.assertRaises(TaskflowError):
                        ExecutionStore._assert_quality(db, value["candidate"])

    def test_alias_members_and_current_graph_cannot_be_substituted(self):
        value = closure_fixture(self)
        store = value["store"]
        alias_id = value["flow"]["definition"]["initial"]["candidate_id"]
        changes = [("candidates", alias_id, "files", None),
                   ("reviews", value["accepted"]["review_id"], "composite_quality_coverage", None)]
        for table, identity, key, _ in changes:
            with store._db() as source, closing(sqlite3.connect(":memory:")) as db, db:
                db.row_factory = sqlite3.Row
                source.backup(db)
                row = ExecutionStore._load(db, table, identity)
                if key == "files":
                    row[key][0]["package_path"] = "draft/input/foreign.md"
                else:
                    row[key]["closed_defect_ids"].append(row[key]["closed_defect_ids"][0])
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (json.dumps(row), identity))
                with self.subTest(key=key), self.assertRaises(TaskflowError):
                    ExecutionStore._assert_quality(db, value["candidate"])


if __name__ == "__main__":
    unittest.main()
