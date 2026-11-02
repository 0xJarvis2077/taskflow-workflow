"""Exact source recovery consumes real sealed whole-finding transport.

The native source/report/input/collection and release/projection IO is actual
neutral subprocess evidence. Accepted author lineage and the store quality graph
are explicit interface doubles from the adjacent source suite, not native model
or independent business results. Recovery lock, receipt, metadata and CAS are real.
"""

from contextlib import ExitStack, contextmanager
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_formal import prepare_release, projected_quality_reports
from taskflow.execution_lifecycle import _receive, _resume_source_quality_transport, resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_negative_whole_finding_targets as findings
from tests import test_execution_native_prior_findings as priors
from tools.execution_simulation import fixture_policy


REASON = "native quality coverage must explicitly delimit whole or affected files"


class WholeFindingLocationsResumeTests(unittest.TestCase):
    def received(self, *, locations=True):
        values = findings.negative_whole_targets_fixture(self) if locations else priors.prior_findings_fixture(self)
        case, graph = values[:2]
        prior_root, fresh_root = values[2:4]
        initial = case.candidates[graph["whole_source"]["candidate_id"]]
        case.candidate["parent_id"] = initial["candidate_id"]
        case.current.update(derived=True, candidate_id=case.candidate["candidate_id"])
        case.child["state"]["history"].append({"kind": "independent_review", "review_id": graph["whole_review_id"]})
        child_receipt = Path(case.child["state"]["receipt"])
        received_child = read_json(child_receipt)
        received_child["history"] = case.child["state"]["history"]
        write_json(child_receipt, received_child)
        receipt = case.root / "source-parent-received.json"
        write_json(receipt, {"flow_id": case.parent_id, "status": "accepted", "result": case.parent["state"]["result"]})
        case.parent["state"]["receipt"] = str(receipt)
        store = ExecutionStore(case.root / "resume.sqlite3")
        store.register_policy("fixture-v1", fixture_policy())
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "whole-finding-experiment",
            "task_id": "neutral", "owner": "neutral-controller", "policy_id": "fixture-v1",
            "source_author_flow_id": case.parent_id, "authorization": case.authorization,
            "directory": str(case.root / "experiment"),
            "technical": {"formal_runtime_root": str(case.root / "formal"), "publication_root": str(case.root / "publication")}}
        record = store.register_flow(definition["flow_id"], definition, [evidence_ref(case.authority, "original source authority")], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_bounded_source_exception"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        packet = {"formal": {"first_run_id": "next-neutral-run"},
                  "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}}
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Use only the actual original whole finding-location witness and current affected closure.",
            "evidence": [evidence_ref(case.authority, "original unchanged business authority")]}
        return {"case": case, "store": store, "record": record, "resolution": resolution,
                "packet": packet, "graph": graph, "initial": initial, "prior": prior_root, "fresh": fresh_root}

    @contextmanager
    def interfaces(self, value):
        case, store = value["case"], value["store"]
        get_flow = store.get_flow
        with ExitStack() as stack:
            stack.enter_context(patch.object(store, "get_flow", side_effect=lambda key: case.flows[key] if key in case.flows else get_flow(key)))
            for name in ("get_candidate", "get_attempt", "quality_gate", "assert_review_current"):
                stack.enter_context(patch.object(store, name, getattr(case.store, name)))
            stack.enter_context(patch.object(store, "get_review", return_value={"covered_defect_ids": value["graph"]["closed_defect_ids"]}))
            stack.enter_context(patch("taskflow.execution_experiment._packet", return_value=value["packet"]))
            stack.enter_context(patch("taskflow.execution_experiment._author_source", return_value={"source": case.child,
                "initial": value["initial"], "binding": {"source_author_flow_id": case.parent_id, "child_flow_id": case.child_id}}))
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_actual_whole_finding_witness_restores_initial_stage_and_normal_release_readiness(self):
        value = self.received()
        case, record, store = value["case"], value["record"], value["store"]
        paths = [Path(record["state"]["receipt"])] + [root / name for root in (value["prior"], value["fresh"])
            for name in ("manifest.json", "collection.json", "request.md", "artifacts/adoption-readiness.json", "artifacts/issues.json")]
        originals = {str(path): sha256(path.read_bytes()) for path in paths}
        with self.interfaces(value):
            result = self.resume(value)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                self.resume(value)
        self.assertEqual(record["revision"] + 1, result["revision"])
        self.assertEqual(record["definition_digest"], result["definition_digest"])
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        prior = result["state"]["history"][-1]["native_certification"]["whole"]["prior_whole_findings"]
        self.assertEqual("original_negative_whole_finding_locations", prior["finding_locations"]["kind"])
        self.assertFalse(prior["original_dimensions"]["business"]["affirmed"])
        self.assertTrue(prior["original_dimensions"]["artifact"]["affirmed"])
        self.assertFalse(prior["original_dimensions"]["calculation"]["affirmed"])
        self.assertEqual(record["state"]["exception"], result["state"]["history"][-1]["previous_exception"])
        self.assertEqual(record["state"]["receipt"], result["state"]["history"][-1]["previous_receipt"]["path"])
        self.assertFalse(result["state"]["history"][-1]["new_model_dispatched"])
        native = case.handoff()
        reports = projected_quality_reports(case.store, candidate_id=case.candidate["candidate_id"],
            source_refs=native["source_refs"], protocol_map=native["protocol_map"], destination=case.root / "projections")
        mapped = {row["id"]: reports[row["kind"]] for row in read_json(case.task / "task.json")["reviews"] if row["required"]}
        released = prepare_release(case.store, candidate_id=case.candidate["candidate_id"], source_root=case.task,
            destination=case.root / "normal-release/neutral", version="bounded-v1", quality_reports=mapped)
        self.assertEqual(case.candidate["input_digest"], released["snapshot"]["input_digest"])
        self.assertFalse(read_json(reports["business"])["whole_package"])
        self.assertFalse(read_json(reports["business"])["prior_dimension"]["affirmed"])
        self.assertEqual(originals, {str(path): sha256(path.read_bytes()) for path in paths})
        with store._db() as db:
            self.assertEqual([0, 0, 0], [db.execute("SELECT count(*) FROM " + table).fetchone()[0] for table in ("work", "attempts", "decisions")])

    def test_a_valid_old_transport_without_finding_locations_cannot_clear_this_exception(self):
        value = self.received(locations=False)
        self.assertNotIn("finding_locations", value["case"].handoff()["protocol_map"]["repaired_candidate_acceptance"]["prior_whole_findings"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "validated original whole finding locations"):
            self.resume(value)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_exact_received_exception_and_every_business_hold_are_preserved(self):
        record = {"definition": {"kind": "experiment_v1", "source_author_flow_id": "source"}, "state": {
            "status": "exception", "stage": "experiment_v1", "receipt": "old", "current_work_id": None,
            "exception": {"stage": "experiment_v1", "reason": REASON}}}
        for change in ({"pending_business_scope": {}}, {"pending_business_scope": False}, {"stage": "formal_prepare"},
                       {"current_work_id": "inflight"}, {"runs": ["existing"]},
                       {"exception": {"stage": "experiment_v1", "reason": REASON + " extra"}}):
            wrong = copy.deepcopy(record);wrong["state"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                _resume_source_quality_transport(None, wrong, {}, [])
        value = self.received()
        for source in (value["case"].parent, value["case"].child):
            source["state"]["pending_business_scope"] = {}
            with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "business hold"):
                self.resume(value)
            source["state"].pop("pending_business_scope")
        receipt = Path(value["record"]["state"]["receipt"]);raw=receipt.read_bytes()
        changed = read_json(receipt);changed["controller"] = "foreign-controller";write_json(receipt, changed)
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "history/hash"):
            self.resume(value)
        receipt.write_bytes(raw)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_foreign_solver_decision_and_formal_roots_block_without_dispatch(self):
        value = self.received();store=value["store"];candidate=value["case"].candidate
        for name in ("formal_runtime_root", "publication_root"):
            path=Path(value["record"]["definition"]["technical"][name]);path.mkdir()
            try:
                with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    self.resume(value)
            finally:path.rmdir()
        row={"work_id":"foreign-solver","candidate_id":candidate["candidate_id"],"role":"solver","state":"uncertain"}
        with store._db(True) as db:
            db.execute("INSERT INTO work VALUES(?,?,?,?,?,?)", ("foreign-solver", "neutral", candidate["candidate_id"], "foreign-intent", "uncertain", json.dumps(row)))
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        with store._db(True) as db:db.execute("DELETE FROM work WHERE id='foreign-solver'")
        with store._db(True) as db:
            db.execute("INSERT INTO decisions VALUES(?,?,?)", ("foreign-decision", "neutral", json.dumps({"decision_id":"foreign-decision", "candidate_id":candidate["candidate_id"], "action":"experiment"})))
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        self.assertEqual(value["record"], store.get_flow(value["record"]["flow_id"]))

    def test_original_sealed_source_drift_is_not_a_technical_resolution(self):
        value = self.received();path=value["prior"] / "artifacts/intake.md";raw=path.read_bytes();path.write_bytes(raw+b"drift")
        try:
            with self.interfaces(value), self.assertRaises((TaskflowError, ValueError)):
                self.resume(value)
        finally:path.write_bytes(raw)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_real_cas_conflict_retains_the_original_exception_and_no_new_work(self):
        value=self.received();store=value["store"];actual=store.update_flow
        def concurrent(flow_id, revision, state, event):
            if event.get("kind") == "lifecycle_source_quality_transport_resumed":
                existing=store.get_flow(flow_id);actual(flow_id,revision,existing["state"],{"kind":"concurrent-neutral-append"})
            return actual(flow_id,revision,state,event)
        with self.interfaces(value), patch.object(store,"update_flow",side_effect=concurrent), self.assertRaises(TaskflowError):
            self.resume(value)
        current=store.get_flow(value["record"]["flow_id"])
        self.assertEqual(value["record"]["revision"]+1,current["revision"])
        self.assertEqual(value["record"]["state"],current["state"])


if __name__ == "__main__":
    unittest.main()
