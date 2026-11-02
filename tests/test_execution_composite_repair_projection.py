"""The same real repair projection remains explicit at composite acceptance."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, write_json
from taskflow.execution_store import evidence_ref
from tests import test_execution_repair_record_projection as fixtures


class CompositeRepairProjectionTests(unittest.TestCase):
    def setUp(self):
        case = fixtures.RepairRecordProjectionTests("test_explicit_projection_reuses_original_attempt_and_requires_fresh_recheck")
        case.setUp()
        self.addCleanup(case.doCleanups)
        prefix = '''
def composite_report(route,ctx,inputs):
 report={'execution_routing':route,'limits':['Neutral affected fixture; remote platform remains unknown.']}
 if route['scope']=='affected':
  name='draft/evaluation/answer.md'
  report['defect_resolution']={'affected_paths':[name],'evidence':[{'path':name,'sha256':digest(inputs/name),'location':'actual changed private source'}]}
 return report
'''
        script = prefix + fixtures.ROLE_FIXTURE_WITH_DECLARATION_COPY.replace(
            "{'execution_routing':route}", "composite_report(route,ctx,inputs)")
        with patch.object(fixtures, "ROLE_FIXTURE_WITH_DECLARATION_COPY", script):
            case.prepare_projection_exception()
        self.case, self.store = case, case.store
        self.originals = {path: path.read_bytes() for path in (case.original, case.runtime / "collection.json",
            case.runtime / "trace.jsonl", case.runtime / "manifest.json", Path(case.failed["state"]["receipt"]))}
        self.author = copy.deepcopy(case.attempt)
        case.resume()
        self.flow = case.case.run_flow([case.definition["flow_id"]])["flows"][0]
        self.assertEqual("accepted", self.flow["state"]["status"])
        reviews = [row for row in self.flow["state"]["history"] if row["kind"] == "independent_review"]
        self.whole, self.recheck = reviews[0]["review_id"], self.flow["state"]["accepted_review_id"]
        self.defects = self.flow["state"]["repaired_defects"]

    def compose(self):
        return self.store.compose_repaired_quality(self.flow["state"]["candidate_id"], self.whole, self.recheck,
                                                   self.defects, evidence=self.case.refs)

    def test_actual_repair_projection_fresh_recheck_and_composite_preserve_originals_and_scope(self):
        result = self.compose()
        graph = result["composite_quality_coverage"]
        self.assertFalse(result["whole_package"])
        self.assertEqual(self.recheck, graph["recheck_id"])
        self.assertEqual(self.author["attempt_id"], graph["repair_record_projection"]["attempt_id"])
        self.assertEqual(str(self.case.projected), graph["repair_record"]["path"])
        self.assertIn("derived", graph["repair_record"]["location"])
        self.assertEqual(self.case.pair()["original"], graph["repair_record_projection"]["original"])
        self.assertEqual(self.author, self.store.get_attempt(self.author["attempt_id"]))
        self.assertEqual(result, self.store.quality_gate(result["candidate_id"]))
        self.assertEqual(self.recheck, self.store.get_flow(self.flow["flow_id"])["state"]["accepted_review_id"])
        self.assertEqual(3, len(self.store.dashboard()["attempts"]))
        for path, raw in self.originals.items():
            self.assertEqual(raw, path.read_bytes())

    def test_forged_nested_projection_binding_is_rejected_without_new_attempt(self):
        defect = self.store.get_defect(self.defects[0])
        typed = next(row for row in defect["repair_evidence"] if row.get("kind") == "repair_record_projection")
        copy_path = self.case.root / "unrelated-projected-copy.json"
        copy_path.write_bytes(self.case.projected.read_bytes())
        typed["repair_record_projection"]["projected"] = evidence_ref(copy_path, "unrelated projected physical copy")
        # Corrupt only this neutral fixture's persisted attestation metadata.
        with self.store._db(True) as db:
            db.execute("UPDATE defects SET data=? WHERE id=?", (canonical(defect).decode(), defect["defect_id"]))
        with self.assertRaisesRegex(TaskflowError, "does not bind"):
            self.compose()
        self.assertEqual(3, len(self.store.dashboard()["attempts"]))

    def test_composite_gate_revalidates_projection_bytes_and_default_path_remains_strict(self):
        result = self.compose()
        raw = self.case.projected.read_bytes()
        value = read_json(self.case.projected)
        value["needs_business_decision"] = True
        write_json(self.case.projected, value)
        with self.assertRaises(TaskflowError):
            self.store.quality_gate(result["candidate_id"])
        self.case.projected.write_bytes(raw)
        self.assertEqual(result, self.store.quality_gate(result["candidate_id"]))
        defect = self.store.get_defect(self.defects[0])
        defect["repair_evidence"] = [row for row in defect["repair_evidence"] if row.get("kind") != "repair_record_projection"]
        with self.store._db(True) as db:
            db.execute("UPDATE defects SET data=? WHERE id=?", (canonical(defect).decode(), defect["defect_id"]))
        with self.assertRaisesRegex(TaskflowError, "actual differences"):
            self.store.quality_gate(result["candidate_id"])


if __name__ == "__main__":
    unittest.main()
