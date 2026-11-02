"""Original sealed repair -> child -> author -> experiment, no model service.

Only the historical strict field-name error is injected. Author, public,
repair and recheck are real neutral Python subprocesses, with real manifests,
collections, archives, SQLite store, closure and experiment source consumers.
No permission or process-identity adapters are installed by these tests.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _author_source, _packet
from taskflow.execution_flow import (
    advance_workflow, resume_workflow,
)
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from tests import test_execution_experiment_author as author_fixtures
from tests import test_execution_flow as flow_fixtures
from tools.execution_simulation import perform


OLD_ERROR = "repair defect_ids: list required"
REPORT = """
def composite_report(route,ctx,inputs):
 report={'execution_routing':route,'limits':['Neutral fixture, no target model.']}
 if route['scope']=='affected':
  name='draft/evaluation/answer.md'
  report['defect_resolution']={'affected_paths':[name],'evidence':[{'path':name,'sha256':digest(inputs/name),'location':'actual changed private source'}]}
 return report
"""


class RepairIdentityResumeTests(unittest.TestCase):
    def setUp(self):
        helper = author_fixtures.AuthorExperimentTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        author, source = helper.author("candidate_complete", terminal=False)
        store = author.store
        source = advance_lifecycle(store, source["flow_id"])
        source = advance_lifecycle(store, source["flow_id"])
        self.assertEqual("public_prepare", source["state"]["stage"])
        initial = store.get_candidate(source["state"]["candidate_id"])
        public = perform(store, author.root / "public", initial["candidate_id"], "quality", "public-original", {
            "public.md": "Neutral independent original public reconstruction."})
        case = flow_fixtures.ContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        definition = case.definition("neutral")
        definition["initial"] = {
            "candidate_id": initial["candidate_id"], "candidate_root": source["state"]["candidate_root"],
            "predecessor_attempt": public["attempt_id"],
            "predecessor_binding": {key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")},
            "expected_digests": {key: initial[key] for key in ("input_digest", "content_digest", "evaluation_digest")}}
        script = REPORT + flow_fixtures.ROLE_FIXTURE.replace(
            "'defect_ids':ctx['defect_ids']", "'role':'repair','intent_id':ctx['intent_id'],'covered_defect_ids':ctx['defect_ids']").replace(
            "{'execution_routing':route}", "composite_report(route,ctx,inputs)").replace(
            "save(root/'task.json',task)",
            "shutil.copyfile(inputs/'execution/task.json',root/'task.json')")
        case.script.write_text(script, encoding="utf-8")
        case.store = store
        case.register(definition)
        # Reproduce the historical consumer error after the real original
        # delivery is sealed. No IO/security checks are substituted.
        with patch("taskflow.execution_flow_contracts.repair_defect_ids", side_effect=TaskflowError(OLD_ERROR)):
            failed = case.run_flow([definition["flow_id"]])["flows"][0]
        self.assertEqual("exception", failed["state"]["status"], failed["state"])
        self.assertEqual(OLD_ERROR, failed["state"]["exception"]["reason"])
        source = store.update_flow(source["flow_id"], source["revision"], {
            **source["state"], "stage": "public_result", "public_work_id": public["work_id"],
            "current_work_id": public["work_id"], "child_flow_id": failed["flow_id"]}, {"kind": "neutral_public_node_fixture"})
        for _ in range(3):
            source = advance_lifecycle(store, source["flow_id"])
        self.assertEqual("exception", source["state"]["status"])
        self.assertEqual(failed["state"]["exception"], source["state"]["exception"]["reason"])
        experiment_definition = helper.definition(author, source)
        experiment_definition["technical"].update(
            formal_runtime_root=str(author.root / "formal"), publication_root=str(author.root / "publication"))
        experiment = helper.register(author, experiment_definition)
        for _ in range(2):
            experiment = advance_lifecycle(store, experiment["flow_id"])
        self.assertEqual("exception", experiment["state"]["status"])
        self.case, self.store, self.child, self.parent, self.experiment = case, store, failed, source, experiment
        self.attempt = next(row for row in store.dashboard()["attempts"] if row["role"] == "repair")
        self.runtime = Path(self.attempt["runtime_path"])
        self.original = self.runtime / "artifacts/repair-record.json"
        self.record = read_json(self.original)
        self.assertNotIn("defect_ids", self.record)
        self.assertNotIn("task.json", [row["path"] for row in self.record["changes"]])
        self.resolution = {"action": "resume_resolved_quality_dependency", "reason": "Consume the original sealed repair defect identity.",
                           "evidence": case.refs}
        paths = [self.original, self.runtime / "manifest.json", self.runtime / "request.md", self.runtime / "collection.json",
                 self.runtime / "trace.jsonl", Path(failed["state"]["receipt"]), Path(source["state"]["receipt"]),
                 Path(experiment["state"]["receipt"])]
        self.originals = {path: path.read_bytes() for path in paths}

    def resume_child(self):
        return resume_workflow(self.store, self.child["flow_id"], reason=self.resolution["reason"], evidence=self.case.refs)

    def resume_parent(self):
        return resume_path_transport(self.store, self.parent["flow_id"], self.resolution)

    def resume_experiment(self):
        return resume_path_transport(self.store, self.experiment["flow_id"], self.resolution)

    def test_original_repair_resumes_all_dependencies_then_real_fresh_closure_and_source_consumer(self):
        before = self.store.dashboard()
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("resume must not launch")):
            child = self.resume_child()
            self.assertEqual(child, self.resume_child())
            parent = self.resume_parent()
            experiment = self.resume_experiment()
            child = advance_workflow(self.store, child["flow_id"])
        self.assertEqual("recheck", child["state"]["stage"])
        self.assertEqual("active", parent["state"]["status"])
        self.assertEqual("active", experiment["state"]["status"])
        for key in ("work", "attempts"):
            self.assertEqual(before[key], self.store.dashboard()[key], key)
        self.assertNotIn("accepted_review_id", child["state"])
        self.assertEqual("repair", self.store.get_attempt(self.attempt["attempt_id"])["role"])
        final = self.case.run_flow([child["flow_id"]])["flows"][0]
        self.assertEqual("accepted", final["state"]["status"], final["state"])
        histories = [row for row in final["state"]["history"] if row["kind"] == "independent_review"]
        composed = self.store.compose_repaired_quality(final["state"]["candidate_id"], histories[0]["review_id"],
            final["state"]["accepted_review_id"], final["state"]["repaired_defects"], evidence=self.case.refs)
        self.assertEqual(composed, self.store.quality_gate(final["state"]["candidate_id"]))
        self.assertEqual(str(self.original), composed["composite_quality_coverage"]["repair_record"]["path"])
        self.assertEqual(1, len([a for a in self.store.dashboard()["attempts"] if a["role"] == "repair"]))
        self.assertEqual(len(before["attempts"]) + 1, len(self.store.dashboard()["attempts"]))
        self.assertNotEqual(self.attempt["cli_sid"], self.store.get_attempt(composed["attempt_id"])["cli_sid"])
        for _ in range(2):
            parent = advance_lifecycle(self.store, parent["flow_id"])
        self.assertEqual("accepted", parent["state"]["status"])
        bound = _author_source(self.store, experiment, _packet(experiment))
        self.assertEqual(final["flow_id"], bound["source"]["flow_id"])
        self.assertEqual(self.originals, {path: path.read_bytes() for path in self.originals})
        self.assertEqual(self.attempt, self.store.get_attempt(self.attempt["attempt_id"]))
        self.assertFalse(Path(experiment["definition"]["technical"]["publication_root"]).exists())
        self.assertFalse(Path(experiment["definition"]["technical"]["formal_runtime_root"]).exists())

    def test_parent_and_experiment_need_the_exact_resolved_child_and_same_source(self):
        with self.assertRaises(TaskflowError):
            self.resume_parent()
        with self.assertRaises(TaskflowError):
            self.resume_experiment()
        self.resume_child()
        get_flow = self.store.get_flow
        child = get_flow(self.child["flow_id"])
        for change in ({"resolved_exceptions": []}, {"repair_defect_identities": {}}, {"last_resolution": {}},
                       {"pending_business_scope": {"reason": "new actual choice"}}, {"status": "exception"},
                       {"candidate_root": "/unrelated/source"}, {"defects": []},
                       {"current_runtime": "/unrelated/runtime"}, {"current_node_id": "foreign-node"}):
            wrong = {**child, "state": {**child["state"], **change}}
            with self.subTest(change=change), patch.object(self.store, "get_flow", side_effect=lambda key: wrong if key == child["flow_id"] else get_flow(key)), self.assertRaises(TaskflowError):
                self.resume_parent()
        self.assertEqual(self.parent, get_flow(self.parent["flow_id"]))

    def test_experiment_rejects_business_unknown_reason_complete_and_every_formal_footprint(self):
        self.resume_child()
        self.resume_parent()
        get_flow = self.store.get_flow
        for change in ({"pending_business_scope": {"reason": "new business choice"}}, {"release": {"receipt": "other"}},
                       {"runs": [{"score": 0}]}, {"run_index": 1}, {"status": "accepted", "stage": "complete"},
                       {"exception": {"stage": "experiment_v1", "reason": "Unknown failure"}}):
            wrong = {**self.experiment, "state": {**self.experiment["state"], **change}}
            with self.subTest(change=change), patch.object(self.store, "get_flow", side_effect=lambda key: wrong if key == self.experiment["flow_id"] else get_flow(key)), self.assertRaises(TaskflowError):
                self.resume_experiment()
        technical = self.experiment["definition"]["technical"]
        formal = Path(technical["formal_runtime_root"])
        from taskflow.execution_experiment import _node
        outer = Path(self.experiment["definition"]["directory"]) / "runtimes" / _node(self.experiment, "solver")
        for path in (Path(technical["publication_root"]), formal / "new-run-01",
                     formal / "new-run-01.formal-intent.json", outer):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("Existing immutable footprint.")
            try:
                with self.subTest(path=path), self.assertRaises(TaskflowError):
                    self.resume_experiment()
            finally:
                path.unlink()
        board = self.store.dashboard()
        for table in ("work", "attempts"):
            for identity in ({"work_id": _node(self.experiment, "solver")}, {"intent_id": _node(self.experiment, "solver") + "-intent"},
                             {"runtime_path": str(formal / "new-run-01")}):
                wrong = copy.deepcopy(board)
                wrong[table].append({"work_id": "foreign-work", "intent_id": "foreign-intent",
                                     "runtime_path": str(self.case.root / "foreign-runtime"), **identity})
                with self.subTest(table=table, identity=identity), patch.object(self.store, "dashboard", return_value=wrong), self.assertRaises(TaskflowError):
                    self.resume_experiment()
        self.assertEqual(self.experiment, get_flow(self.experiment["flow_id"]))

    def test_parent_requires_original_author_public_predecessor_and_candidate_source(self):
        self.resume_child()
        get_flow = self.store.get_flow
        for change in ({"public_attempt_id": "foreign-public"}, {"candidate_id": "foreign-candidate"},
                       {"candidate_root": "/foreign/source"}, {"child_flow_id": "foreign-child"},
                       {"author_archive": str(self.case.root / "foreign-archive")},
                       {"pending_business_scope": {"reason": "new source decision"}}):
            wrong = {**self.parent, "state": {**self.parent["state"], **change}}
            with self.subTest(change=change), patch.object(self.store, "get_flow", side_effect=lambda key: wrong if key == self.parent["flow_id"] else get_flow(key)), self.assertRaises((TaskflowError, OSError)):
                self.resume_parent()
        self.assertEqual(self.parent, get_flow(self.parent["flow_id"]))

    def test_original_seal_candidate_source_and_archive_drift_reject_before_resume(self):
        candidate = self.store.get_candidate(self.record["candidate_id"])
        archive = Path(self.child["definition"]["directory"]) / "archives" / self.attempt["attempt_id"]
        paths = [self.original, self.runtime / "collection.json", self.runtime / "manifest.json", self.runtime.parent / "current.json", archive / "artifacts/repair-record.json",
                 Path(candidate["files"][0]["path"]), Path(self.child["state"]["candidate_root"]) / "task.json"]
        for path in paths:
            raw = path.read_bytes()
            path.write_bytes(raw + b" changed")
            try:
                with self.subTest(path=path), self.assertRaises(TaskflowError):
                    self.resume_child()
            finally:
                path.write_bytes(raw)
        self.assertEqual(self.child, self.store.get_flow(self.child["flow_id"]))

    def test_wrong_actual_author_duplicate_attempt_foreign_node_and_new_hold_reject(self):
        original_lookup = self.store.list_attempts
        requested = []

        def changed_attempts(work_id, change=None, duplicate=False):
            requested.append(work_id)
            rows = original_lookup(work_id)
            if work_id != self.attempt["work_id"]:
                return rows
            self.assertEqual([self.attempt["attempt_id"]], [row["attempt_id"] for row in rows])
            rows = copy.deepcopy(rows)
            if duplicate:
                rows.append(copy.deepcopy(rows[0]))
            else:
                rows[0].update(change)
            return rows

        for change in ({"role": "quality"}, {"state": "running"}, {"state": "failed"}, {"process_exit": True},
                       {"candidate_id": "foreign"}, {"cli_sid": "foreign-sid"}, {"request_digest": "0" * 64}):
            requested.clear()
            with self.subTest(change=change), patch.object(self.store, "list_attempts", side_effect=lambda key: changed_attempts(key, change)), self.assertRaises(TaskflowError):
                self.resume_child()
            self.assertIn(self.attempt["work_id"], requested)
        requested.clear()
        with patch.object(self.store, "list_attempts", side_effect=lambda key: changed_attempts(key, duplicate=True)), self.assertRaises(TaskflowError):
            self.resume_child()
        self.assertIn(self.attempt["work_id"], requested)
        get_flow = self.store.get_flow
        for change in ({"pending_business_scope": {"reason": "new business choice"}}, {"current_node_id": "foreign"},
                       {"exception": {"reason": "Unknown decision", "work_id": self.attempt["work_id"]}}):
            wrong = {**self.child, "state": {**self.child["state"], **change}}
            with self.subTest(change=change), patch.object(self.store, "get_flow", side_effect=lambda key: wrong if key == self.child["flow_id"] else get_flow(key)), self.assertRaises(TaskflowError):
                # The generic resume API retains its historical unknown-reason
                # behavior; an unknown exception must never acquire this proof.
                if "exception" in change:
                    self.resume_parent()
                else:
                    self.resume_child()
        self.assertEqual(self.child, get_flow(self.child["flow_id"]))
