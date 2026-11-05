"""Resume one sealed rubric-role declaration transport without model calls."""

import copy
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import _author, _identity, advance_author
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as author_fixtures
from tests import test_execution_optional_candidate_declaration_transport as transport_fixtures


class DeclarationTransportResumeTests(unittest.TestCase):
    def setUp(self):
        case = author_fixtures.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        case.script.write_text(transport_fixtures.author_script().replace("source,mode=sys.argv[1:]", "source,mode,*_=sys.argv[1:]"))
        case.run_source("candidate_complete")
        self.attempt = self.store.get_attempt(case.attempt["attempt_id"])
        record = case.register()
        advance_lifecycle(self.store, record["flow_id"])
        advance_lifecycle(self.store, record["flow_id"])
        self.record = advance_lifecycle(self.store, record["flow_id"])
        self.source = Path(self.record["state"]["author_archive"]) / "supplemental/trees/work/next-task/neutral"
        self.target = case.root / "declaration-candidate"
        shutil.copytree(self.source, self.target)
        declaration = read_json(self.target / "task.json")
        next(row for row in declaration["files"] if row["id"] == declaration["scoring"]["rubric_file"])["role"] = "rubric"
        write_json(self.target / "task.json", declaration)
        self.declaration = declaration
        self.spec = {"schema_version": 1, "kind": "referenced_rubric_role_v1",
            "original": evidence_ref(self.source / "task.json", "actual sealed original author declaration"),
            "projected": evidence_ref(self.target / "task.json", "explicit derived rubric role label"), "evidence": case.refs}
        self.resolution = {"action": "accept_declaration_only_candidate_transport", "reason": "Use the already referenced unchanged private rubric.",
                           "declaration_transport": self.spec, "evidence": case.refs}
        self.originals = {path: path.read_bytes() for path in [self.source / "task.json", Path(self.record["state"]["receipt"]),
            *[case.runtime / name for name in ("manifest.json", "request.md", "collection.json", "collection-lock.json", "trace.jsonl")]]}

    def resume(self, resolution=None):
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("resume cannot dispatch")):
            return resume_path_transport(self.store, self.record["flow_id"], resolution or self.resolution)

    def rewritten(self, declaration):
        write_json(self.target / "task.json", declaration)
        spec = {**self.spec, "projected": evidence_ref(self.target / "task.json", "explicit derived rubric role label")}
        return {**self.resolution, "declaration_transport": spec}

    def downstream(self):
        flow_id = "declaration-experiment"
        authority = self.case.root / "downstream-authority.json"
        item = {"task_id": "neutral", "source_author_quality_flow": self.record["flow_id"],
            "source_author_intent": self.attempt["intent_id"], "source_author_attempt": self.attempt["attempt_id"],
            "source_author_sid": self.attempt["cli_sid"], "source_business_packet": self.case.refs[0]["path"],
            "source_business_packet_sha256": self.case.refs[0]["sha256"], "first_experiment_business_unknown": "Neutral authorized future question.",
            "version_id": "neutral-v1", "first_run_id": "neutral-01", "first_evaluation_id": "neutral-eval-01",
            "conditional_one_confirmation_run_id": "neutral-02", "conditional_one_confirmation_evaluation_id": "neutral-eval-02"}
        write_json(authority, {"schema_version": 1, "controller": "fixture-controller", "items": [item],
            "preauthorized_normal_chain": "Existing bounded chain only.", "prerequisites": ["Actual independent quality required."], "confirmation": {}})
        ref = evidence_ref(authority, "explicit neutral downstream authority")
        record = self.store.register_flow(flow_id, {"schema_version": 1, "flow_id": flow_id, "kind": "experiment_v1",
            "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1", "authorization": ref,
            "business_packet_ref": ref, "business_packet_item": {"task_id": "neutral", "source_author_quality_flow": self.record["flow_id"]},
            "source_author_flow_id": self.record["flow_id"], "directory": str(self.case.root / "experiment"),
            "technical": {"formal_runtime_root": str(self.case.root / "formal")}}, [ref],
            {"status": "exception", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": [],
             "exception": {"stage": "experiment_v1", "reason": "source author-quality flow stopped with its original exception: " + str(self.record["state"]["exception"])}})
        record = advance_lifecycle(self.store, record["flow_id"])
        resolution = {"action": "resume_resolved_declaration_dependency", "reason": "Wait on the same technically resumed source.", "evidence": [ref]}
        return record, resolution

    def test_explicit_resume_preserves_original_actor_then_normal_handler_captures_same_author(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_legacy_wrong_link_extra_role_new_payload_public_change_and_foreign_source_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_nonterminal_signed_or_changed_metadata_successor_and_business_hold_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_downstream_resumes_same_initial_stage_and_only_waits_for_existing_quality(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_downstream_rejects_unresolved_parent_drift_existing_intent_and_other_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

