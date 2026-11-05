"""Initial public provenance remains fixed when the ordinary phase cursor moves."""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, sha256, write_json
from taskflow.execution_engineering_source import engineering_source
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_runner import archive_full_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_engineering_source as fixture


class InitialPublicArchiveTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.EngineeringSourceTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.source = self.case.source

    def repair_archive(self):
        c, s = self.case, self.source
        script = s.root / "repair-phase.py"
        script.write_text("import json,pathlib\npathlib.Path('handoff.md').write_text('Neutral repair phase fixture only.')\n"
            "print(json.dumps({'type':'thread.started','thread_id':'00000000-0000-4000-8000-000000000077'}))\n"
            "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral repair original.'}}))\n"
            "print(json.dumps({'type':'turn.completed'}))\n")
        work, runtime = s.root / "repair-work", s.root / "repair-runtime"
        work.mkdir()
        execution.enqueue_spec(c.ledger, {"work": {"work_id": "repair-phase", "task_id": "neutral",
            "candidate_id": s.candidate["candidate_id"], "role": "repair", "policy_id": "fixture-v1",
            "intent_id": "repair-phase-intent", "purpose": "Neutral distinct phase artifact", "evidence": [c.authorization]},
            "runner": {"intent_id": "repair-phase-intent", "role": "repair", "policy_version": "fixture-v1",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), str(script)],
                "dependencies": [str(script)], "inputs": [], "request_sections": [], "outputs": ["handoff.md"]}}, runtime)
        attempt = execution.run_worker(c.ledger, "repair-phase-fixture", work_ids=["repair-phase"])
        archive = Path(s.child["definition"]["directory"]) / "archives" / attempt["attempt_id"]
        archive_full_attempt(runtime, archive)
        s.child["state"]["predecessor_archive"] = str(archive)
        return archive

    def test_actual_distinct_repair_cursor_does_not_rebind_initial_public_archive(self):
        c, s = self.case, self.source
        public_archive = Path(s.child["state"]["predecessor_archive"])
        repair_archive = self.repair_archive()
        before = {str(p): p.read_bytes() for root in (public_archive, repair_archive) for p in root.rglob("*") if p.is_file()}
        result = engineering_source(s.store, c.definition, c.item, require_candidate=True)
        proof = result["binding"]["engineering_source"]
        self.assertEqual(public_archive, Path(proof["public_manifest"]["path"]).parent)
        self.assertEqual(str(repair_archive), s.child["state"]["predecessor_archive"])
        self.assertNotEqual(public_archive, repair_archive)
        self.assertEqual(before, {str(p): p.read_bytes() for root in (public_archive, repair_archive) for p in root.rglob("*") if p.is_file()})

    def prepare_exception(self):
        c, s = self.case, self.source
        self.repair_archive()
        s.child["revision"] = 1
        s.child["definition_digest"] = sha256(canonical(s.child["definition"]))
        definition = copy.deepcopy(c.definition)
        definition.update(schema_version=1, directory=str(s.root / "experiment"))
        definition["technical"]["formal_runtime_root"] = str(s.root / "formal-runtime")
        Path(definition["directory"]).mkdir()
        receipt = Path(definition["directory"]) / "original-exception.json"
        exception = {"stage": "experiment_v1", "reason": "Archive destination belongs to different original evidence"}
        write_json(receipt, {"flow_id": definition["flow_id"], "status": "exception", "exception": exception})
        state = {"status": "exception", "stage": "experiment_v1", "current_work_id": None,
                 "history": [], "archives": [], "exception": exception, "receipt": str(receipt)}
        record = c.ledger.register_flow(definition["flow_id"], definition, [c.authorization], state)
        original_get = s.store.get_flow
        s.store.get_flow = lambda identity: c.ledger.get_flow(identity) if identity == record["flow_id"] else original_get(identity)
        s.store.path = c.ledger.path
        s.store.update_flow = c.ledger.update_flow
        s.store.dashboard = c.ledger.dashboard
        resolution = {"action": "resume_validated_engineering_public_archive", "reason": "Original public archive verified",
                      "evidence": [c.authorization]}
        return record, resolution, receipt

    def test_normal_resume_appends_same_experiment_evidence_without_new_role_or_source(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_other_failure_hold_source_override_or_existing_runtime_cannot_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_existing_solver_evidence_and_changed_source_cannot_be_cleared(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_corrupted_initial_public_archive_is_not_waived_by_valid_phase_archive(self):
        c, s = self.case, self.source
        archive = Path(s.child["state"]["predecessor_archive"])
        self.repair_archive()
        (archive / "collection.json").write_text("{}")
        with self.assertRaises(ValueError):
            engineering_source(s.store, c.definition, c.item, require_candidate=True)
