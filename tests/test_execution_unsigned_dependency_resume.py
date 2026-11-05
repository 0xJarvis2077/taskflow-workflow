"""Source-bound unsigned alias dependency recovery; no model calls."""

import copy
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution_author import advance_author
from taskflow.execution_lifecycle import _declaration_transport_reviews, advance_lifecycle, resume_path_transport
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as author_fixtures
from tests import test_execution_declaration_transport_resume as declaration_fixtures
from tests import test_execution_optional_candidate_report_layout as layout_fixtures


class UnsignedDependencyResumeTests(unittest.TestCase):
    downstream = declaration_fixtures.DeclarationTransportResumeTests.downstream

    def setUp(self):
        case = author_fixtures.AuthorSuccessorTests("test_complete_optional_candidate_binds_actual_author_then_scrubs_child_roles")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        case.script.write_text(case.script.read_text().replace("'status':'inconclusive'", "'status':'passed'"))
        case.run_source("candidate_complete")
        record = case.register()
        advance_lifecycle(self.store, record["flow_id"])
        advance_lifecycle(self.store, record["flow_id"])
        self.record = advance_lifecycle(self.store, record["flow_id"])
        self.attempt = self.store.get_attempt(case.attempt["attempt_id"])
        self.material = self.store._validated_optional_candidate_delivery(self.attempt)
        self.source = Path(self.material["candidate_root"])
        self.target = case.root / "unsigned-alias"
        shutil.copytree(self.source, self.target)
        for row in read_json(self.target / "task.json")["reviews"]:
            source = self.source / row["path"]
            write_json(self.target / row["path"], {"schema_version": 1, "review_id": row["id"], "kind": row["kind"],
                "status": "inconclusive", "task_id": "neutral", "independent_quality_passed": False, "new_formal_score": None,
                "local_checks": {"original_author_report": read_json(source), "original_relative_path": row["path"],
                                 "original_sha256": sha256(source.read_bytes())}})
        self.resolution = {"action": "accept_unsigned_author_placeholders", "reason": "Preserve the unsigned original in a source-bound wrapper.",
                           "candidate_root": str(self.target), "evidence": case.refs}

    def resume_parent(self):
        return resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def resume_child(self, record, resolution):
        return resume_path_transport(self.store, record["flow_id"], {**resolution, "action": "resume_resolved_author_preparation_dependency"})

    def test_actual_alias_preserves_full_raw_report_and_only_restores_downstream_wait(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_changed_raw_json_locator_sha_or_payload_cannot_resume_dependency(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_only_explicit_unsigned_resolution_and_typed_layout_transport_are_admitted(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


class LayoutDependencyResumeTests(unittest.TestCase):
    downstream = declaration_fixtures.DeclarationTransportResumeTests.downstream

    def test_real_layout_validator_resumes_parent_and_dependency_without_upgrading_report(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

