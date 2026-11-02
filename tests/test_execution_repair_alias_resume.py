"""Real author intake and finite alias recovery; native protocol has own tests."""

import copy
import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from taskflow.execution import capture_candidate
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _node, advance_experiment
from taskflow.execution_lifecycle import _receive, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment_author as author_fixtures
from tests import test_execution_source_quality_transport as transport_fixtures
from tools.execution_simulation import perform


REPAIR_ALIAS = "defect was repaired only in a different/later revision"


class RepairAliasResumeTests(unittest.TestCase):
    def source(self, *, alias=True):
        helper = author_fixtures.AuthorExperimentTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        original_author = helper.author

        def with_repaired_source(*args, **kwargs):
            case, source = original_author(*args, **kwargs)
            store = case.store
            old_root, repaired_root = case.root / "old-package", case.root / "repaired-package"
            shutil.copytree(case.task, old_root)
            prompt = old_root / "draft/input/prompt.md"
            prompt.write_text("Obsolete neutral responsibility already performed upstream.\n", encoding="utf-8")
            capture_candidate(store, old_root, "old-source")
            finding = perform(store, case.root / "prior", "old-source", "quality", "old-finding", {"finding.md": "Neutral blocking finding."})
            refs = [evidence_ref(Path(finding["runtime_path"]) / "artifacts/finding.md", "original neutral finding")]
            store.record_review("old-whole", "old-source", finding["attempt_id"], verdict="defects", scope="whole neutral package", whole_package=True, evidence=refs)
            store.open_defect("old-defect", "old-source", "old-whole", responsibility="Leave the current responsibility to the recipient.", evidence=refs)
            original_prompt = case.task / "draft/input/prompt.md"
            author = perform(store, case.root / "prior", "old-source", "repair", "old-repair", {"prompt.md": original_prompt.read_text(encoding="utf-8")})
            shutil.copytree(case.task, repaired_root)
            repaired = capture_candidate(store, repaired_root, "old-repaired", "old-source", author["attempt_id"])
            store.record_repair("old-defect", repaired["candidate_id"], evidence=refs)
            recheck = perform(store, case.root / "prior", "old-repaired", "quality", "old-recheck", {"recheck.md": "Neutral independent original closure."})
            refs = [evidence_ref(Path(recheck["runtime_path"]) / "artifacts/recheck.md", "original neutral closure")]
            store.record_review("old-closure", "old-repaired", recheck["attempt_id"], verdict="accepted", scope="whole original repair", whole_package=True, evidence=refs)
            store.close_defect("old-defect", "old-closure", evidence=refs)
            baseline = store.get_candidate("baseline")
            self.assertEqual([baseline[key] for key in ("input_digest", "content_digest", "evaluation_digest")],
                             [repaired[key] for key in ("input_digest", "content_digest", "evaluation_digest")])
            return case, source

        if alias:
            with patch.object(helper, "author", side_effect=with_repaired_source):
                case, source, initial = helper.candidate_source()
        else:
            case, source, initial = helper.candidate_source()
        definition = helper.definition(case, source)
        definition["technical"].update(formal_runtime_root=str(case.root / "formal"), publication_root=str(case.root / "publication"))
        return helper, case, source, initial, definition

    def received(self, *, alias=True):
        helper, case, source, initial, definition = self.source(alias=alias)
        factory = transport_fixtures.SourceQualityTransportTests()
        record, resolution = factory.exception(case.store, definition, REPAIR_ALIAS, case.root)
        state = dict(record["state"])
        state.pop("receipt")
        state["history"] = [{"kind": "original_zero_launch_source_failure"}]
        record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_exception_fixture"})
        record = _receive(case.store, record)
        return helper, case, source, initial, record, resolution

    def native_interface(self, helper):
        # Only this interface is substituted. Real author/archive/child binding,
        # SQLite quality/alias gates and durable resume are exercised together.
        # Native manifest/report/collection validation has separate real tests.
        def handoff(store, record, candidate_id):
            value = helper.native_interface_fixture(store, record, candidate_id)
            value["certification"]["fresh"] = {"nature": "test_fixture_interface"}
            return value
        return patch("taskflow.execution_experiment._native_handoff", side_effect=handoff)

    def test_producer_accepts_current_whole_with_closed_repaired_baseline_copy(self):
        helper, case, source, initial, definition = self.source()
        record = helper.register(case, definition)
        before = case.store.dashboard()
        with self.native_interface(helper):
            result = advance_experiment(case.store, record)
        self.assertEqual("release", result["state"]["stage"], result["state"])
        self.assertEqual(initial["candidate_id"], result["state"]["candidate_id"])
        self.assertEqual(source["state"]["result"]["accepted_review_id"], result["state"]["accepted_review_id"])
        self.assertEqual(before["work"], case.store.dashboard()["work"])
        self.assertEqual(before["attempts"], case.store.dashboard()["attempts"])

    def test_received_resume_appends_and_preserves_all_original_bytes_and_records(self):
        helper, case, source, initial, record, resolution = self.received()
        store = case.store
        def originals():
            with store._db() as db:
                return {table: [json.loads(row["data"]) for row in db.execute("SELECT data FROM " + table + " ORDER BY rowid")]
                        for table in ("candidates", "reviews", "defects", "work", "attempts")}
        before = originals()
        paths = {Path(record["state"]["receipt"]), Path(source["state"]["receipt"]),
                 Path(source["state"]["result"]["child_receipt"])}
        for candidate in before["candidates"]:
            paths.update(Path(row["path"]) for row in candidate["files"] + candidate["evidence"])
        for row in before["reviews"]:
            paths.update(Path(ref["path"]) for ref in row["evidence"])
        raw = {path: path.read_bytes() for path in paths}
        with self.native_interface(helper), patch("taskflow.execution.run_worker", side_effect=AssertionError("zero launch")):
            result = resume_path_transport(store, record["flow_id"], resolution)
            with self.assertRaisesRegex(TaskflowError, "exact received"):
                resume_path_transport(store, record["flow_id"], resolution)
            advanced = advance_experiment(store, result)
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertEqual(record["state"]["history"], result["state"]["history"][:-1])
        self.assertEqual(record["state"]["exception"], result["state"]["history"][-1]["previous_exception"])
        self.assertFalse(result["state"]["history"][-1]["new_model_dispatched"])
        self.assertEqual("release", advanced["state"]["stage"])
        after = originals()
        for key in ("candidates", "reviews", "defects", "work", "attempts"):
            self.assertEqual(before[key], after[key], key)
        self.assertEqual(raw, {path: path.read_bytes() for path in paths})
        self.assertFalse(Path(record["definition"]["technical"]["publication_root"]).exists())

    def test_no_alias_unknown_business_complete_and_noninitial_states_reject(self):
        helper, case, _, _, record, resolution = self.received(alias=False)
        with self.native_interface(helper), self.assertRaisesRegex(TaskflowError, "no closed repair through"):
            resume_path_transport(case.store, record["flow_id"], resolution)
        original_get = case.store.get_flow
        changes = [{"status": "accepted", "stage": "complete"}, {"stage": "release"},
                   {"exception": {"stage": "experiment_v1", "reason": "unknown transport error"}},
                   {"pending_business_scope": {"reason": "specific unresolved choice"}},
                   {"current_work_id": "running"}, {"release": {"receipt": "published"}},
                   {"runs": [{"score": 1}]}, {"run_index": 1}]
        for change in changes:
            wrong = {**record, "state": {**record["state"], **change}}
            with self.subTest(change=change), patch.object(case.store, "get_flow", side_effect=lambda key: wrong if key == record["flow_id"] else original_get(key)), \
                 self.assertRaisesRegex(TaskflowError, "exact received"):
                resume_path_transport(case.store, record["flow_id"], resolution)
        self.assertEqual(record, original_get(record["flow_id"]))

    def test_source_receipt_whole_bytes_and_formal_footprints_reject_without_resume(self):
        helper, case, source, initial, record, resolution = self.received()
        store = case.store
        paths = [Path(record["state"]["receipt"]), Path(source["state"]["result"]["child_receipt"]),
                 Path(store.get_review(source["state"]["result"]["accepted_review_id"])["evidence"][0]["path"]),
                 Path(initial["files"][0]["path"])]
        for path in paths:
            raw = path.read_bytes()
            if path.suffix == ".json" and path != paths[-1]:
                value = read_json(path)
                value["flow_id"] = "another-flow"
                write_json(path, value)
            else:
                path.write_bytes(raw + b"changed")
            try:
                with self.subTest(path=path.name), self.native_interface(helper), self.assertRaises(TaskflowError):
                    resume_path_transport(store, record["flow_id"], resolution)
            finally:
                path.write_bytes(raw)
        receipt = paths[0]
        raw = receipt.read_bytes()
        changed = read_json(receipt)
        changed["history"].append({"kind": "substituted_history"})
        write_json(receipt, changed)
        try:
            with self.native_interface(helper), self.assertRaisesRegex(TaskflowError, "received history/hash"):
                resume_path_transport(store, record["flow_id"], resolution)
        finally:
            receipt.write_bytes(raw)
        current = store.quality_gate(initial["candidate_id"])
        with patch.object(store, "quality_gate", return_value={**current, "derived": True}), \
             self.assertRaisesRegex(TaskflowError, "original current independent whole"):
            resume_path_transport(store, record["flow_id"], resolution)
        formal = Path(record["definition"]["technical"]["formal_runtime_root"])
        outer = Path(record["definition"]["directory"]) / "runtimes" / _node(record, "solver")
        publication = Path(record["definition"]["technical"]["publication_root"])
        for path in (formal, outer, publication):
            path.mkdir(parents=True)
            try:
                with self.subTest(path=path.name), self.native_interface(helper), self.assertRaisesRegex(TaskflowError, "existing formal"):
                    resume_path_transport(store, record["flow_id"], resolution)
            finally:
                path.rmdir()
        board = store.dashboard()
        for kind in ("work", "attempts"):
            wrong = copy.deepcopy(board)
            wrong[kind].append({"intent_id": _node(record, "solver") + "-intent", "state": "failed"})
            with self.subTest(kind=kind), self.native_interface(helper), patch.object(store, "dashboard", return_value=wrong), \
                 self.assertRaisesRegex(TaskflowError, "existing formal"):
                resume_path_transport(store, record["flow_id"], resolution)
        self.assertEqual(record, store.get_flow(record["flow_id"]))


if __name__ == "__main__":
    unittest.main()
