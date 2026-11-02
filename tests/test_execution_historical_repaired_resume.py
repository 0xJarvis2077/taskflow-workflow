"""Exact initial historical-graph transport recovery; no worker is dispatched.

Store/source/repair/closure/receipt/lock/CAS are actual neutral fixtures. Packet,
parent-author selector and native CLI certification are explicit interface doubles.
"""

from contextlib import contextmanager
import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _receive, _resume_source_quality_transport, resume_path_transport
from tests import test_execution_historical_repaired_source as sources


REASON = "historical negative is outside the current repaired quality graph"


class HistoricalRepairedResumeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owner = unittest.TestCase()
        cls.value = sources.graph_descendant_fixture(cls.owner)
        cls.addClassCleanup(cls.owner.doCleanups)

    def received(self):
        value = dict(self.value)
        store, root, child = value["store"], value["case"].root, value["new_flow"]
        tag = sha256(self.id().encode())[:10]
        parent_id, flow_id = "source-parent-" + tag, "experiment-" + tag
        result = {"outcome": "candidate_quality_accepted"}
        receipt = root / (parent_id + ".json")
        write_json(receipt, {"flow_id": parent_id, "status": "accepted", "result": result})
        parent = store.register_flow(parent_id, {"kind": "author_quality_v1"}, value["refs"], {
            "status": "accepted", "receipt": str(receipt), "result": result})
        candidate = store.get_candidate(value["current"]["candidate_id"])
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": flow_id,
            "task_id": candidate["task_id"], "owner": child["definition"]["owner"],
            "policy_id": child["definition"]["policy_id"], "authorization": value["refs"][0],
            "source_author_flow_id": parent_id, "directory": str(root / flow_id),
            "technical": {"formal_runtime_root": str(root / (flow_id + "-formal")),
                          "publication_root": str(root / (flow_id + "-publication"))}}
        record = store.register_flow(flow_id, definition, value["refs"], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_received_failure"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        value.update(record=record, parent=parent, candidate_row=candidate,
            resolution={"action": "resume_validated_source_quality_transport", "reason": "Revalidate the exact historical closure transport.", "evidence": value["refs"]},
            packet={"formal": {"first_run_id": "fixture-run"},
                    "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}},
            bound={"source": child, "initial": store.get_candidate(value["new_whole"]["candidate_id"]),
                   "binding": {"source_author_flow_id": parent_id, "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": value["current"]["attempt_id"]},
                                      "composite_quality_coverage": value["current"]["composite_quality_coverage"]}})
        return value

    @contextmanager
    def interfaces(self, value, *, parent=None, bound=None, handoff=None):
        original = value["store"].get_flow
        def get_flow(identity):
            return parent if parent is not None and identity == parent["flow_id"] else original(identity)
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]), \
             patch.object(value["store"], "get_flow", side_effect=get_flow):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_exact_received_graph_transport_resumes_once_without_changing_pause_or_original_scope(self):
        value = self.received()
        store, record = value["store"], value["record"]
        work = _node(record, "solver")
        store.set_control("work:" + work, "pause", "Independent scope obligation remains paused.")
        before = store.dashboard()
        with store._db() as db:
            controls = [dict(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")]
        paths = [Path(record["state"]["receipt"])]
        for key in ("historical", "current"):
            graph = value[key]["composite_quality_coverage"]
            paths.extend(Path(graph[source][field]["path"]) for source in ("whole_source", "affected_source") for field in ("report", "issues"))
        hashes = {path: sha256(path.read_bytes()) for path in paths}
        with self.interfaces(value), patch.object(store, "dashboard", side_effect=AssertionError("No global quality dashboard for launch footprint.")):
            result = self.resume(value)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                self.resume(value)
        self.assertEqual(record["revision"] + 1, result["revision"])
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        item = result["state"]["history"][-1]
        self.assertFalse(item["new_model_dispatched"])
        self.assertEqual(value["current"]["review_id"], item["accepted_review_id"])
        self.assertEqual(value["current"]["composite_quality_coverage"], item["native_certification"]["composite_quality_coverage"])
        self.assertEqual(record["state"]["receipt"], item["previous_receipt"]["path"])
        self.assertEqual(hashes, {path: sha256(path.read_bytes()) for path in paths})
        for key in ("work", "attempts", "decisions"):
            self.assertEqual(before[key], store.dashboard()[key])
        with store._db() as db:
            self.assertEqual(controls, [dict(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")])
        with self.assertRaises(TaskflowError):
            store.assert_review_current(value["historical"]["review_id"])

    def test_source_hold_wrong_scope_native_or_received_receipt_cannot_clear_failure(self):
        value = self.received()
        for field, item in (("status", "active"), ("pending_business_scope", {}),
                            ("pending_business_scope", False), ("exception", {"reason": "Actual repair is pending."})):
            parent = copy.deepcopy(value["parent"]); parent["state"][field] = item
            with self.subTest(field=field, item=item), self.interfaces(value, parent=parent), self.assertRaises(TaskflowError):
                self.resume(value)
        bound = copy.deepcopy(value["bound"]); bound["source"]["state"]["accepted_review_id"] = value["recheck"]
        with self.interfaces(value, bound=bound), self.assertRaisesRegex(TaskflowError, "accepted original child"):
            self.resume(value)
        bound = copy.deepcopy(value["bound"]); bound["initial"] = value["store"].get_candidate(value["negative"]["candidate_id"])
        with self.interfaces(value, bound=bound), self.assertRaisesRegex(TaskflowError, "authorized lineage"):
            self.resume(value)
        handoff = copy.deepcopy(value["handoff"]); handoff["certification"]["composite_quality_coverage"] = value["historical"]["composite_quality_coverage"]
        with self.interfaces(value, handoff=handoff), self.assertRaisesRegex(TaskflowError, "native certification"):
            self.resume(value)
        receipt = Path(value["record"]["state"]["receipt"])
        raw = receipt.read_bytes()
        try:
            changed = read_json(receipt); changed["controller"] = "foreign"; write_json(receipt, changed)
            with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "history/hash"):
                self.resume(value)
        finally:
            receipt.write_bytes(raw)
        for change in ({"current_work_id": "inflight"}, {"stage": "formal_prepare"}, {"pending_business_scope": False},
                       {"pending_business_scope": {}}, {"release": {"path": "already"}}, {"run_index": 1}):
            record = copy.deepcopy(value["record"]); record["state"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                _resume_source_quality_transport(None, record, {}, [])
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_existing_publication_work_attempt_decision_or_new_open_business_defect_blocks(self):
        value = self.received()
        store, record, candidate = value["store"], value["record"], value["candidate_row"]
        for name in ("formal_runtime_root", "publication_root"):
            path = Path(record["definition"]["technical"][name]); path.mkdir()
            try:
                with self.subTest(name=name), self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    self.resume(value)
            finally:
                path.rmdir()
        decision = store.record_decision("preexisting-graph-experiment", candidate["task_id"], candidate["candidate_id"],
            action="experiment", rationale="Already bound separate intent.", experiment_purpose="Previous bounded purpose.",
            authorized=True, evidence=value["refs"])
        try:
            with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                self.resume(value)
        finally:
            with store._db(True) as db:
                db.execute("DELETE FROM decisions WHERE id=?", (decision["decision_id"],))
        work_id = _node(record, "solver")
        store.enqueue(work_id, candidate["task_id"], candidate["candidate_id"], "quality", record["definition"]["policy_id"],
            intent_id=work_id + "-intent", purpose="Existing neutral footprint; never run.", request_digest="0" * 64,
            evidence=value["refs"], dispatch_owner=record["definition"]["owner"])
        attempt = None
        try:
            with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                self.resume(value)
            attempt = store.claim("neutral-footprint-owner", [work_id])
            self.assertEqual("claimed", attempt["state"])
            with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                self.resume(value)
        finally:
            with store._db(True) as db:
                if attempt is not None:
                    db.execute("DELETE FROM attempts WHERE id=?", (attempt["attempt_id"],))
                db.execute("DELETE FROM work WHERE id=?", (work_id,))
        defect = store.open_defect("new-independent-business-obligation", candidate["candidate_id"],
            value["new_flow"]["state"]["accepted_review_id"], responsibility="Actual current responsibility needs another authorized repair.",
            evidence=value["refs"])
        try:
            with self.interfaces(value), self.assertRaises(TaskflowError):
                self.resume(value)
        finally:
            with store._db(True) as db:
                db.execute("DELETE FROM defects WHERE id=?", (defect["defect_id"],))
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_actual_cas_rejects_a_concurrent_append(self):
        value = self.received()
        store, record = value["store"], value["record"]
        original = store.update_flow
        def update(identity, revision, state, event):
            if event.get("kind") == "lifecycle_source_quality_transport_resumed":
                current = store.get_flow(identity)
                original(identity, revision, current["state"], {"kind": "concurrent_fixture_append"})
            return original(identity, revision, state, event)
        with self.interfaces(value), patch.object(store, "update_flow", side_effect=update), self.assertRaises(TaskflowError):
            self.resume(value)
        current = store.get_flow(record["flow_id"])
        self.assertEqual(record["revision"] + 1, current["revision"])
        self.assertEqual(record["state"], current["state"])


if __name__ == "__main__":
    unittest.main()
