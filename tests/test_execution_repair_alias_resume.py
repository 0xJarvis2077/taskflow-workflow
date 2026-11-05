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
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_no_alias_unknown_business_complete_and_noninitial_states_reject(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_source_receipt_whole_bytes_and_formal_footprints_reject_without_resume(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":
    unittest.main()
