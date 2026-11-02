"""Finite same-source recovery, without changing an independent business hold.

The store, actual closed repair, new author, current independent whole source,
sealed subprocess outputs, receipt, lock and CAS are real neutral fixtures.
Packet selection, parent-author binding and native CLI certification are explicit
boundary doubles; this is not evidence of a model run or native sandbox validity.
"""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _receive, _resume_source_quality_transport, resume_path_transport
from tests import test_execution_historical_closed_ancestor as sources
from tests import test_execution_historical_closure_resume as old_resumes


REASON = "historical negative lacks exact current whole repair coverage"


class HistoricalClosedResumeTests(unittest.TestCase):
    def received(self):
        value = sources.descendant_fixture(self)
        store, root = value["store"], value["case"].root
        candidate, review = value["new_candidate"], value["current"]
        refs = review["evidence"]
        child = store.register_flow("new-source-child", {"kind": "quality_repair_v1"}, refs, {
            "status": "accepted", "candidate_id": candidate["candidate_id"],
            "accepted_review_id": review["review_id"]})
        result = {"outcome": "candidate_quality_accepted"}
        parent_receipt = root / "new-source-parent-result.json"
        write_json(parent_receipt, {"flow_id": "new-source-parent", "status": "accepted", "result": result})
        parent = store.register_flow("new-source-parent", {"kind": "author_quality_v1"}, refs, {
            "status": "accepted", "receipt": str(parent_receipt), "result": result})
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "historical-experiment",
            "task_id": candidate["task_id"], "owner": value["flow"]["definition"]["owner"],
            "policy_id": value["flow"]["definition"]["policy_id"], "authorization": refs[0],
            "source_author_flow_id": parent["flow_id"], "directory": str(root / "experiment"),
            "technical": {"formal_runtime_root": str(root / "formal"), "publication_root": str(root / "publication")}}
        record = store.register_flow(definition["flow_id"], definition, refs, {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_source_transport_failure"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Revalidate this negative's actual already closed historical repair and current source.",
            "evidence": refs}
        value.update(record=record, resolution=resolution, refs=refs, parent=parent,
            packet={"formal": {"first_run_id": "neutral-run-01"}},
            bound={"source": child, "initial": value["alias"],
                   "binding": {"source_author_flow_id": parent["flow_id"], "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": review["attempt_id"]},
                                      "composite_quality_coverage": None}})
        return value

    @contextmanager
    def interfaces(self, value, *, bound=None, handoff=None, parent=None):
        store = value["store"]
        original_get = store.get_flow
        def get_flow(identity):
            if parent is not None and identity == value["parent"]["flow_id"]:
                return parent
            return original_get(identity)
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]), \
             patch.object(store, "get_flow", side_effect=get_flow):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_exact_original_source_resumes_once_and_preserves_business_pause_and_raw_sources(self):
        value = self.received()
        store, record = value["store"], value["record"]
        work_id = _node(record, "solver")
        store.set_control("work:" + work_id, "pause", "Separate current business repair is still mandatory.")
        before = store.dashboard()
        with store._db() as db:
            controls = [dict(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")]
        paths = [Path(record["state"]["receipt"])]
        for review in (value["negative"], value["historical"], value["current"]):
            runtime = Path(store.get_attempt(review["attempt_id"])["runtime_path"])
            paths.extend(runtime / path for path in ("manifest.json", "collection.json", "artifacts/adoption-readiness.json", "artifacts/issues.json"))
        hashes = {path: sha256(path.read_bytes()) for path in paths}
        with self.interfaces(value):
            result = self.resume(value)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                self.resume(value)
        self.assertEqual(record["revision"] + 1, result["revision"])
        self.assertEqual(record["definition_digest"], result["definition_digest"])
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        self.assertNotIn("exception", result["state"])
        history = result["state"]["history"]
        self.assertEqual(record["state"]["history"], history[:-1])
        self.assertEqual(record["state"]["exception"], history[-1]["previous_exception"])
        self.assertEqual(str(paths[0]), history[-1]["previous_receipt"]["path"])
        self.assertFalse(history[-1]["new_model_dispatched"])
        self.assertEqual(value["current"]["review_id"], history[-1]["accepted_review_id"])
        self.assertEqual([], store.get_review(value["current"]["review_id"])["covered_defect_ids"])
        self.assertEqual(hashes, {path: sha256(path.read_bytes()) for path in paths})
        for key in ("work", "attempts", "decisions"):
            self.assertEqual(before[key], store.dashboard()[key])
        with store._db() as db:
            self.assertEqual(controls, [dict(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")])
        self.assertEqual("pause", json.loads(controls[0]["data"])["action"])

    def test_current_whole_without_any_alias_proof_remains_rejected(self):
        helper = old_resumes.HistoricalResumeReceiptTests()
        case, _, _, record, resolution = helper.received()
        self.addCleanup(helper.doCleanups)
        state = copy.deepcopy(record["state"])
        state.pop("receipt")
        state["exception"]["reason"] = REASON
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_original_reason"})
        record = _receive(case.store, record)
        before = case.store.dashboard()
        with self.assertRaisesRegex(TaskflowError, "no closed repair through"):
            resume_path_transport(case.store, record["flow_id"], resolution)
        self.assertEqual(record, case.store.get_flow(record["flow_id"]))
        self.assertEqual(before, case.store.dashboard())

    def test_original_received_identity_and_hash_cannot_be_changed(self):
        record = {"definition": {"kind": "experiment_v1", "source_author_flow_id": "source"},
            "state": {"status": "exception", "stage": "experiment_v1", "current_work_id": None,
                      "receipt": "original", "exception": {"stage": "experiment_v1", "reason": REASON}}}
        for change in ({"status": "active"}, {"stage": "release"}, {"current_work_id": "work"},
                       {"candidate_id": "selected"}, {"accepted_review_id": "review"}, {"runs": ["run"]},
                       {"release": {"path": "release"}}, {"run_index": 1}, {"pending_business_scope": {}},
                       {"pending_business_scope": False}, {"exception": {"stage": "experiment_v1", "reason": REASON + " other"}}):
            wrong = copy.deepcopy(record); wrong["state"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                _resume_source_quality_transport(None, wrong, {}, [])
        value = self.received()
        receipt = Path(value["record"]["state"]["receipt"])
        raw = receipt.read_bytes()
        for change in ({"flow_id": "foreign"}, {"controller": "foreign"},
                       {"history": [{"kind": "replacement"}]}, {"result": {"outcome": "replacement"}}):
            changed = read_json(receipt); changed.update(change); write_json(receipt, changed)
            try:
                with self.subTest(change=change), self.interfaces(value), self.assertRaises(TaskflowError):
                    self.resume(value)
            finally:
                receipt.write_bytes(raw)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_an_open_new_business_defect_blocks_technical_resume(self):
        value = self.received()
        store = value["store"]
        defect = store.open_defect("new-business-scope-finding", value["new_candidate"]["candidate_id"],
            value["current"]["review_id"], responsibility="The current public responsibility requires an actual repair.",
            evidence=value["refs"])
        before = store.dashboard()
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "unclosed responsibility"):
            self.resume(value)
        self.assertEqual("open", store.get_defect(defect["defect_id"])["state"])
        self.assertEqual(before, store.dashboard())
        self.assertEqual(value["record"], store.get_flow(value["record"]["flow_id"]))

    def test_parent_active_source_holds_wrong_lineage_review_or_native_binding_cannot_resume(self):
        value = self.received()
        parent = copy.deepcopy(value["parent"]); parent["state"]["status"] = "active"
        with self.interfaces(value, parent=parent), self.assertRaisesRegex(TaskflowError, "already accepted"):
            self.resume(value)
        for target in ("parent", "child"):
            for field, item in (("pending_business_scope", {}), ("pending_business_scope", False),
                                ("exception", {"reason": "A separate substantive repair is not complete."})):
                parent = copy.deepcopy(value["parent"]); bound = copy.deepcopy(value["bound"])
                source = parent if target == "parent" else bound["source"]
                source["state"][field] = item
                with self.subTest(target=target, field=field, item=item), self.interfaces(value, parent=parent, bound=bound), \
                     self.assertRaisesRegex(TaskflowError, "source business hold"):
                    self.resume(value)
        bound = copy.deepcopy(value["bound"])
        bound["initial"] = value["store"].get_candidate(value["negative"]["candidate_id"])
        with self.interfaces(value, bound=bound), self.assertRaisesRegex(TaskflowError, "authorized lineage"):
            self.resume(value)
        bound = copy.deepcopy(value["bound"]); bound["source"]["state"]["accepted_review_id"] = value["historical"]["review_id"]
        with self.interfaces(value, bound=bound), self.assertRaisesRegex(TaskflowError, "accepted original child"):
            self.resume(value)
        handoff = copy.deepcopy(value["handoff"]); handoff["certification"]["composite_quality_coverage"] = {"foreign": True}
        with self.interfaces(value, handoff=handoff), self.assertRaisesRegex(TaskflowError, "native certification differs"):
            self.resume(value)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_any_existing_formal_path_work_attempt_or_experiment_decision_blocks(self):
        value = self.received()
        store, record = value["store"], value["record"]
        for key in ("formal_runtime_root", "publication_root"):
            path = Path(record["definition"]["technical"][key]); path.mkdir()
            try:
                with self.subTest(path=key), self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    self.resume(value)
            finally:
                path.rmdir()
        candidate = value["new_candidate"]
        decision = store.record_decision("already-recorded-experiment", candidate["task_id"], candidate["candidate_id"],
            action="experiment", rationale="Previously authorized bounded intent.", experiment_purpose="Existing intent.",
            authorized=True, evidence=value["refs"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        # Remove only this adversarial fixture decision to isolate work/claim
        # footprint assertions; no production database is used by this module.
        with store._db(True) as db:
            db.execute("DELETE FROM decisions WHERE id=?", (decision["decision_id"],))
        work_id = _node(record, "solver")
        store.enqueue(work_id, candidate["task_id"], candidate["candidate_id"], "quality",
            record["definition"]["policy_id"], intent_id=work_id + "-intent", purpose="Preexisting footprint; never dispatch.",
            request_digest="0" * 64, evidence=value["refs"], dispatch_owner=record["definition"]["owner"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        attempt = store.claim("neutral-existing-worker", [work_id])
        self.assertEqual("claimed", attempt["state"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_concurrent_revision_causes_real_cas_rejection_and_preserves_exception(self):
        value = self.received()
        store = value["store"]
        original_update = store.update_flow
        def update(identity, revision, state, event):
            if event.get("kind") == "lifecycle_source_quality_transport_resumed":
                current = store.get_flow(identity)
                original_update(identity, revision, current["state"], {"kind": "neutral_concurrent_append"})
            return original_update(identity, revision, state, event)
        with self.interfaces(value), patch.object(store, "update_flow", side_effect=update), self.assertRaises(TaskflowError):
            self.resume(value)
        current = store.get_flow(value["record"]["flow_id"])
        self.assertEqual(value["record"]["revision"] + 1, current["revision"])
        self.assertEqual(value["record"]["state"], current["state"])


if __name__ == "__main__":
    unittest.main()
