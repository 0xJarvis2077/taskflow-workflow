"""Neutral canonical producer -> strict release, with no model or shared-state IO."""

import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow import execution_runner as runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_experiment import (
    _advance, _release_quality_reports, _required_release_reports,
)
from taskflow.execution_formal import prepare_release, projected_quality_reports
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task


KINDS = ("business", "artifact", "calculation")


def declaration(*, legacy=False, calculation=True):
    return {"reviews": [{"id": kind if legacy else "signed-" + str(index),
                         "kind": kind, "path": "reports/" + kind + ".json",
                         "required": kind != "calculation" or calculation}
                        for index, kind in enumerate(KINDS)]}


class RequiredReleaseSelectionTests(unittest.TestCase):
    def test_custom_ids_preserve_paths_and_do_not_mutate_inputs(self):
        task = declaration()
        reports = {kind: Path("projection") / (kind + ".json") for kind in KINDS}
        before = copy.deepcopy((task, reports))
        selected = _required_release_reports(task, reports)
        self.assertEqual({"signed-" + str(i): reports[kind] for i, kind in enumerate(KINDS)}, selected)
        self.assertTrue(all(selected["signed-" + str(i)] is reports[kind] for i, kind in enumerate(KINDS)))
        self.assertEqual(before, (task, reports))

    def test_legacy_selection_is_identical(self):
        reports = {kind: Path(kind + ".json") for kind in KINDS}
        self.assertEqual(reports, _required_release_reports(declaration(legacy=True), reports))

    def test_optional_is_not_required_or_an_ambiguity_and_calculation_is_conditional(self):
        task = declaration(calculation=False)
        task["reviews"].append({"id": "extra-optional", "kind": "business", "required": False})
        reports = {kind: Path(kind + ".json") for kind in KINDS}
        selected = _required_release_reports(task, reports)
        self.assertEqual({"signed-0", "signed-1"}, set(selected))
        reports.pop("calculation")
        self.assertEqual(selected, _required_release_reports(task, reports))

    def test_duplicate_missing_unknown_and_shared_paths_are_rejected(self):
        reports = {kind: Path(kind + ".json") for kind in KINDS}
        duplicate = declaration()
        duplicate["reviews"].append({"id": "second-signature", "kind": "business", "required": True})
        cases = ((duplicate, reports, "unique kind"),
                 (declaration(), {key: value for key, value in reports.items() if key != "artifact"}, "missing"),
                 (declaration(), {**reports, "unexpected": Path("extra.json")}, "unknown"),
                 (declaration(), {**reports, "artifact": reports["business"]}, "reuse"))
        for task, produced, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(TaskflowError, reason):
                _required_release_reports(task, produced)


class ReleaseFixture:
    """Actual sealed neutral source and projections; no subprocess or model."""

    def __init__(self, root, *, legacy=False, calculation=True, include_calculation=True, reviews=None):
        self.root = root
        self.task = make_task(root)
        task = read_json(self.task / "task.json")
        task["reviews"] = reviews if reviews is not None else declaration(legacy=legacy, calculation=calculation)["reviews"]
        write_json(self.task / "task.json", task)
        self.db = ExecutionStore(root / "state.sqlite3")
        self.candidate = capture_candidate(self.db, self.task, "neutral-current")
        self.binding = {key: self.candidate[key] for key in
                        ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
        source = root / "signed-source"
        (source / "artifacts").mkdir(parents=True)
        write_json(source / "artifacts/adoption.json", {"scope": "whole_package",
            "dimensions": {kind: True for kind in KINDS}, "limitations": ["Neutral protocol fixture; no business approval."]})
        (source / "artifacts/intake.md").write_text("Explicit neutral evidence.\n", encoding="utf-8")
        write_json(source / "artifacts/issues.json", {"issues": []})
        outputs = ["adoption.json", "intake.md", "issues.json"]
        manifest = {"intent_id": "neutral-quality", "role": "quality", "nature": "real_cli_attempt",
                    "candidate_binding": self.binding, "outputs": outputs}
        write_json(source / "manifest.json", manifest)
        self.attempt = {"attempt_id": "neutral-quality-attempt", "state": "completed", "role": "quality",
            "candidate_id": self.candidate["candidate_id"], "intent_id": manifest["intent_id"],
            "runtime_path": str(source), "cli_sid": "00000000-0000-4000-8000-000000000001"}
        runner._save_collection(source, {"intent_id": manifest["intent_id"], "sid": self.attempt["cli_sid"],
            "artifacts": {name: runner._info(source / "artifacts" / name) for name in outputs}})
        self.refs = {key: evidence_ref(source / "artifacts" / name, "neutral original signed-source fixture")
                     for key, name in (("adoption", "adoption.json"), ("intake", "intake.md"), ("issues", "issues.json"))}
        authority = root / "projection-authority.json"
        write_json(authority, {"task_id": "neutral", "controller": "neutral-controller"})
        self.authority = evidence_ref(authority, "neutral projection authority")
        self.protocol = {"authorization": self.authority,
            "scope": {"source": "adoption", "pointer": "/scope", "equals": "whole_package"},
            "dimensions": {kind: {"affirmations": [{"source": "adoption", "pointer": "/dimensions/" + kind, "equals": True}],
                "evidence": [{"source": "intake", "lines": [1, 1], "nonempty": True}],
                "limitations": [{"source": "adoption", "pointer": "/limitations", "nonempty": True}]}
                for kind in KINDS if include_calculation or kind != "calculation"}}
        self.accepted = {"review_id": "neutral-accepted", "attempt_id": self.attempt["attempt_id"], "whole_package": True}
        self.store = SimpleNamespace(get_candidate=lambda key: self.candidate, quality_gate=lambda key: self.accepted,
            get_attempt=lambda key: self.attempt, assert_review_current=lambda key: self.accepted)
        self.directory = root / "lifecycle"
        self.destination = root / "publication/neutral"
        self.produce()

    def produce(self):
        self.projections = projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"],
            source_refs=self.refs, protocol_map=self.protocol, destination=self.directory / "quality-projection")
        return self.projections

    def select(self, reports=None):
        return _release_quality_reports(self.store, candidate_id=self.candidate["candidate_id"],
            source_root=self.task, reports=self.projections if reports is None else reports)

    def release(self, reports=None):
        return prepare_release(self.store, candidate_id=self.candidate["candidate_id"], source_root=self.task,
            destination=self.destination, version="neutral-v1", quality_reports=self.select() if reports is None else reports)


class CanonicalReleaseBoundaryTests(unittest.TestCase):
    def fixture(self, **options):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return ReleaseFixture(Path(temporary.name).resolve(), **options)

    def test_actual_experiment_producer_to_strict_release_preserves_every_original(self):
        case = self.fixture()
        original = {str(path): path.read_bytes() for path in case.root.rglob("*") if path.is_file()}
        definition = {"kind": "experiment_v1", "flow_id": "neutral-experiment", "task_id": "neutral",
            "authorization": case.authority, "directory": str(case.directory),
            "technical": {"quality_projection_map": case.protocol, "publication_root": str(case.destination)}}
        record = {"flow_id": definition["flow_id"], "revision": 1, "definition": definition, "state": {
            "stage": "release", "candidate_id": case.candidate["candidate_id"], "candidate_root": str(case.task),
            "quality_sources": case.refs, "accepted_review_id": case.accepted["review_id"]}}
        packet = {"formal": {"first_run_id": "neutral-run"}, "release": {"version_id": "neutral-v1"}}
        case.store.list_flows = lambda: [record]
        case.store.update_flow = lambda identity, revision, state, event: {**record, "state": state}
        with patch("taskflow.execution_experiment._packet", return_value=packet), \
             patch("subprocess.Popen", side_effect=AssertionError("no processes")):
            released = _advance(case.store, record)
        self.assertEqual("formal_prepare", released["state"]["stage"])
        receipt = read_json(case.destination / "release-receipt.json")
        self.assertEqual({"signed-0", "signed-1", "signed-2"}, set(receipt["binding"]["reports"]))
        for name, raw in original.items():
            self.assertEqual(raw, Path(name).read_bytes(), name)
        for index, kind in enumerate(KINDS):
            self.assertEqual(case.projections[kind].read_bytes(),
                             (case.destination / "release-sources" / ("signed-" + str(index) + ".json")).read_bytes())
        self.assertEqual(released["state"]["release"], case.release())

    def test_strict_consumer_still_rejects_kind_keys_for_custom_ids(self):
        case = self.fixture()
        with self.assertRaisesRegex(TaskflowError, "exactly the required report IDs"):
            case.release(case.projections)
        self.assertFalse(case.destination.exists())

    def test_legacy_and_optional_projections_keep_bytes_and_optional_has_no_passed_output(self):
        for legacy, calculation in ((True, True), (False, False)):
            with self.subTest(legacy=legacy, calculation=calculation):
                case = self.fixture(legacy=legacy, calculation=calculation)
                before = {kind: path.read_bytes() for kind, path in case.projections.items()}
                case.release()
                for kind, raw in before.items():
                    self.assertEqual(raw, case.projections[kind].read_bytes())
                receipt = read_json(case.destination / "release-receipt.json")
                expected = set(KINDS) if legacy else {"signed-0", "signed-1"}
                self.assertEqual(expected, set(receipt["binding"]["reports"]))
                if not calculation:
                    self.assertFalse((case.destination / "reports/calculation.json").exists())

    def test_no_calculation_producer_is_needed_when_it_is_optional(self):
        case = self.fixture(calculation=False, include_calculation=False)
        self.assertEqual({"business", "artifact"}, set(case.projections))
        self.assertEqual({"signed-0", "signed-1"}, set(case.select()))
        case.release()

    def test_automatic_selection_rejects_duplicate_missing_and_unknown_canonical_dimensions(self):
        case = self.fixture()
        for produced in ({key: value for key, value in case.projections.items() if key != "artifact"},
                         {**case.projections, "unknown": case.projections["business"]}):
            with self.subTest(produced=list(produced)), self.assertRaises(TaskflowError):
                case.select(produced)
        reviews = declaration()["reviews"]
        reviews.append({"id": "another-business", "kind": "business", "required": True, "path": "reports/another.json"})
        duplicate = self.fixture(reviews=reviews)
        with self.assertRaisesRegex(TaskflowError, "unique kind"):
            duplicate.select()
        self.assertFalse(case.destination.exists())
        self.assertFalse(duplicate.destination.exists())

    def test_wrong_actual_kind_including_unselected_dimension_and_changed_source_are_rejected(self):
        case = self.fixture(calculation=False)
        reports = {**case.projections, "calculation": case.projections["business"]}
        with self.assertRaises(TaskflowError):
            case.select(reports)
        path = case.projections["calculation"]
        raw = path.read_bytes()
        changed = read_json(path)
        changed["kind"] = "artifact"
        write_json(path, changed)
        with self.assertRaises(TaskflowError):
            case.select()
        path.write_bytes(raw)
        (case.root / "signed-source/artifacts/intake.md").write_text("Changed after signature.\n", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            case.select()
        self.assertFalse(case.destination.exists())

    def test_declaration_candidate_binding_and_current_quality_drift_reject(self):
        case = self.fixture()
        original = (case.task / "task.json").read_bytes()
        task = read_json(case.task / "task.json")
        task["reviews"][0]["id"] = "changed-declaration"
        write_json(case.task / "task.json", task)
        with self.assertRaises(TaskflowError):
            case.select()
        (case.task / "task.json").write_bytes(original)
        for target, field, value in ((case.candidate, "input_digest", "0" * 64),
                                     (case.accepted, "review_id", "another-signature")):
            old = target[field]
            target[field] = value
            try:
                with self.assertRaises(TaskflowError):
                    case.select()
            finally:
                target[field] = old


if __name__ == "__main__":
    unittest.main()
