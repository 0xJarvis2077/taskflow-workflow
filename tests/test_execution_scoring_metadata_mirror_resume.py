"""Exact zero-model source recovery for a signed scoring-mirror transport.

The store, private repair, independent subprocess reports, graph revalidation,
receipt, lock and CAS are real neutral fixtures. Author ancestry/packet selection
and native outer CLI identity are explicit interface doubles; their original
production evidence is verified separately by root. No model is invoked here.
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
from tests import test_execution_composite_quality as composite_fixtures
from tests import test_execution_scoring_metadata_mirror as mirror_fixtures


REASON = "Composition cannot silently inherit changed package/scoring metadata"


class ScoringMetadataMirrorResumeTests(unittest.TestCase):
    def received(self, *, mirror=True):
        if mirror:
            value = mirror_fixtures.make_scoring_mirror_fixture(self)
        else:
            fixture = composite_fixtures.CompositeQualityTests()
            fixture.addCleanup = self.addCleanup
            value = fixture.make_closed_flow()
            value["refs"] = value["case"].refs
        store, child = value["store"], value["flow"]
        identity = value["candidate"]
        identity = identity["candidate_id"] if isinstance(identity, dict) else identity
        candidate = store.get_candidate(identity)
        initial = store.get_candidate(store.get_review(value["whole"])["candidate_id"])
        review = store.compose_repaired_quality(identity, value["whole"], value["recheck"], value["defects"], evidence=value["refs"])
        root = store.path.parent
        parent_id = "mirror-source-parent"
        parent_receipt = root / "mirror-parent-result.json"
        result = {"outcome": "candidate_quality_accepted"}
        write_json(parent_receipt, {"flow_id": parent_id, "status": "accepted", "result": result})
        parent = store.register_flow(parent_id, {"kind": "author_quality_v1"}, value["refs"], {
            "status": "accepted", "receipt": str(parent_receipt), "result": result})
        definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": "mirror-experiment",
            "task_id": candidate["task_id"], "owner": child["definition"]["owner"],
            "policy_id": child["definition"]["policy_id"], "authorization": value["refs"][0],
            "source_author_flow_id": parent_id, "directory": str(root / "mirror-experiment"),
            "technical": {"formal_runtime_root": str(root / "mirror-formal"), "publication_root": str(root / "mirror-publication")}}
        record = store.register_flow(definition["flow_id"], definition, value["refs"], {
            "status": "exception", "stage": "experiment_v1", "current_work_id": None,
            "history": [{"kind": "original_bounded_exception"}], "archives": [],
            "exception": {"stage": "experiment_v1", "reason": REASON}})
        record = _receive(store, record)
        resolution = {"action": "resume_validated_source_quality_transport",
            "reason": "Revalidate only the exact independently signed scoring-mirror transport.", "evidence": value["refs"]}
        value.update(record=record, resolution=resolution, parent=parent, candidate_row=candidate,
            packet={"formal": {"first_run_id": "mirror-run-01"},
                    "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}},
            bound={"source": child, "initial": initial,
                   "binding": {"source_author_flow_id": parent_id, "child_flow_id": child["flow_id"]}},
            handoff={"certification": {"fresh": {"attempt_id": review["attempt_id"]},
                                      "composite_quality_coverage": review["composite_quality_coverage"]}},
            review=review)
        return value

    @contextmanager
    def interfaces(self, value, *, bound=None, handoff=None):
        with patch("taskflow.execution_experiment._packet", return_value=value["packet"]), \
             patch("taskflow.execution_experiment._author_source", return_value=bound or value["bound"]), \
             patch("taskflow.execution_experiment._native_handoff", return_value=handoff or value["handoff"]):
            yield

    def resume(self, value):
        return resume_path_transport(value["store"], value["record"]["flow_id"], value["resolution"])

    def test_actual_canonical_mirror_restores_only_same_initial_stage_once_and_retains_originals(self):
        value = self.received()
        store, record = value["store"], value["record"]
        graph = value["review"]["composite_quality_coverage"]
        self.assertEqual("deterministic_signed_scoring_mirror_v1", graph["scoring_metadata_mirror"]["kind"])
        paths = [Path(record["state"]["receipt"])]
        for identity in (value["whole"], value["recheck"]):
            runtime = Path(store.get_attempt(store.get_review(identity)["attempt_id"])["runtime_path"])
            paths += [runtime / name for name in ("manifest.json", "collection.json", "artifacts/adoption-readiness.json", "artifacts/issues.json")]
        original = {path: sha256(path.read_bytes()) for path in paths}
        board = store.dashboard()
        reviews = [store.get_review(identity) for identity in (value["whole"], value["recheck"], value["review"]["review_id"])]
        with self.interfaces(value):
            result = self.resume(value)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                self.resume(value)
        self.assertEqual(record["revision"] + 1, result["revision"])
        self.assertEqual(record["definition_digest"], result["definition_digest"])
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        self.assertNotIn("receipt", result["state"])
        self.assertNotIn("exception", result["state"])
        history = result["state"]["history"]
        self.assertEqual(record["state"]["history"], history[:-1])
        self.assertEqual(record["state"]["exception"], history[-1]["previous_exception"])
        self.assertEqual(record["state"]["receipt"], history[-1]["previous_receipt"]["path"])
        self.assertFalse(history[-1]["new_model_dispatched"])
        self.assertEqual(graph, history[-1]["native_certification"]["composite_quality_coverage"])
        for key in ("work", "attempts", "decisions"):
            self.assertEqual(board[key], store.dashboard()[key])
        self.assertEqual(reviews, [store.get_review(row["review_id"]) for row in reviews])
        self.assertEqual(original, {path: sha256(path.read_bytes()) for path in paths})

    def test_only_exact_received_initial_metadata_exception_can_reach_source_validation(self):
        record = {"definition": {"kind": "experiment_v1", "source_author_flow_id": "source"},
            "state": {"status": "exception", "stage": "experiment_v1", "current_work_id": None,
                      "receipt": "original", "exception": {"stage": "experiment_v1", "reason": REASON}}}
        for change in ({"status": "active"}, {"stage": "release"}, {"current_work_id": "inflight"},
                       {"candidate_id": "already-selected"}, {"runs": ["run"]}, {"run_index": 1},
                       {"pending_business_scope": {}}, {"pending_business_scope": False},
                       {"exception": {"stage": "experiment_v1", "reason": REASON + " other"}}):
            wrong = copy.deepcopy(record); wrong["state"].update(change)
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "exact received"):
                _resume_source_quality_transport(None, wrong, {}, [])
        value = self.received()
        receipt = Path(value["record"]["state"]["receipt"])
        original = receipt.read_bytes()
        payload = read_json(receipt);payload["controller"] = "foreign"
        write_json(receipt, payload)
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "history/hash"):
            self.resume(value)
        receipt.write_bytes(original)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_ordinary_composite_without_typed_mirror_is_not_metadata_recovery(self):
        value = self.received(mirror=False)
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "signed scoring metadata mirror"):
            self.resume(value)
        self.assertEqual(value["record"], value["store"].get_flow(value["record"]["flow_id"]))

    def test_stored_witness_drift_and_foreign_native_graph_do_not_count_as_validation(self):
        value = self.received()
        store, review = value["store"], value["review"]
        for variant in ("missing", "source", "declaration", "signed_evidence"):
            changed = copy.deepcopy(review)
            graph = changed["composite_quality_coverage"]
            if variant == "missing":graph.pop("scoring_metadata_mirror")
            elif variant == "source":graph["scoring_metadata_mirror"]["source"]["current"]["sha256"] = "0" * 64
            elif variant == "declaration":graph["scoring_metadata_mirror"]["current_declaration"]["sha256"] = "0" * 64
            else:graph["scoring_metadata_mirror"]["signed_evidence"] = []
            with store._db(True) as db:
                db.execute("UPDATE reviews SET data=? WHERE id=?", (json.dumps(changed), review["review_id"]))
            try:
                with self.subTest(variant=variant), self.interfaces(value), self.assertRaises(TaskflowError):
                    self.resume(value)
            finally:
                with store._db(True) as db:
                    db.execute("UPDATE reviews SET data=? WHERE id=?", (json.dumps(review), review["review_id"]))
        handoff = copy.deepcopy(value["handoff"])
        handoff["certification"]["composite_quality_coverage"]["scoring_metadata_mirror"]["changes"] = []
        with self.interfaces(value, handoff=handoff), self.assertRaisesRegex(TaskflowError, "native certification differs"):
            self.resume(value)
        self.assertEqual(value["record"], store.get_flow(value["record"]["flow_id"]))

    def test_source_holds_decisions_and_existing_formal_paths_block_recovery(self):
        value = self.received()
        store = value["store"]
        original_get = store.get_flow
        for target in ("parent", "child"):
            for field, hold in (("pending_business_scope", {}), ("pending_business_scope", False), ("exception", {"reason": "Separate business hold"})):
                parent = copy.deepcopy(value["parent"])
                bound = copy.deepcopy(value["bound"])
                source = parent if target == "parent" else bound["source"]
                source["state"][field] = hold
                with self.subTest(target=target, field=field, hold=hold), self.interfaces(value, bound=bound), \
                     patch.object(store, "get_flow", side_effect=lambda key: parent if key == parent["flow_id"] else original_get(key)), \
                     self.assertRaisesRegex(TaskflowError, "source business hold"):
                    self.resume(value)
        for name in ("formal_runtime_root", "publication_root"):
            path = Path(value["record"]["definition"]["technical"][name]);path.mkdir()
            try:
                with self.subTest(path=name), self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    self.resume(value)
            finally:path.rmdir()
        store.record_decision("existing-experiment", value["candidate_row"]["task_id"], value["candidate_row"]["candidate_id"],
            action="experiment", rationale="Separate preexisting bounded decision.", experiment_purpose="Previously authorized observation.", authorized=True, evidence=value["refs"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        self.assertEqual(value["record"], store.get_flow(value["record"]["flow_id"]))

    def test_existing_exact_work_or_claimed_attempt_blocks_without_a_new_launch(self):
        value = self.received()
        store, record = value["store"], value["record"]
        candidate = value["candidate_row"]
        work_id = _node(record, "solver")
        store.enqueue(work_id, candidate["task_id"], candidate["candidate_id"], "quality",
            record["definition"]["policy_id"], intent_id=work_id + "-intent",
            purpose="Preexisting neutral work footprint; never launch it.", request_digest="0" * 64,
            evidence=value["refs"], dispatch_owner=record["definition"]["owner"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        attempt = store.claim("preexisting-neutral-worker", [work_id])
        self.assertIsNotNone(attempt)
        self.assertEqual("claimed", attempt["state"])
        with self.interfaces(value), self.assertRaisesRegex(TaskflowError, "existing formal"):
            self.resume(value)
        self.assertEqual(record, store.get_flow(record["flow_id"]))

    def test_real_cas_rejects_a_concurrent_revision_without_clearing_original_exception(self):
        value = self.received()
        store = value["store"]
        original_update = store.update_flow
        def update(flow_id, revision, state, event):
            if event.get("kind") == "lifecycle_source_quality_transport_resumed":
                current = store.get_flow(flow_id)
                original_update(flow_id, revision, current["state"], {"kind": "concurrent-neutral-append"})
            return original_update(flow_id, revision, state, event)
        with self.interfaces(value), patch.object(store, "update_flow", side_effect=update), self.assertRaises(TaskflowError):
            self.resume(value)
        current = store.get_flow(value["record"]["flow_id"])
        self.assertEqual(value["record"]["revision"] + 1, current["revision"])
        self.assertEqual(value["record"]["state"], current["state"])


if __name__ == "__main__":
    unittest.main()
