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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_parent_and_experiment_need_the_exact_resolved_child_and_same_source(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_experiment_rejects_business_unknown_reason_complete_and_every_formal_footprint(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_parent_requires_original_author_public_predecessor_and_candidate_source(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)
