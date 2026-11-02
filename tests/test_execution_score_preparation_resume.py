"""Exact normative selection and an unclaimed score resume; no model calls.

Shared native certification and OS sandbox probes are mocked at their explicit
boundaries here. Original facts/score preparation fixtures use the real SQLite
ledger, subprocess collector, immutable manifests and facts seal.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution, execution_review
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_experiment import _work
from taskflow.execution_lifecycle import advance_lifecycle, resume_path_transport
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as experiment_fixtures
from tests import test_execution_review as review_fixtures


REASON = "scoring allowlist must contain this candidate's normative answer/rubric sources"


class CertifiedReviewSelectionTests(unittest.TestCase):
    def setUp(self):
        task_factory = review_fixtures.make_task

        def with_execution_summary(*args, **kwargs):
            task = task_factory(*args, **kwargs)
            path = task / "draft/evaluation/execution-summary.json"
            write_json(path, {"kind": "author execution summary; not a normative source"})
            declaration = read_json(task / "task.json")
            declaration["files"].append({"id": "summary", "path": "draft/evaluation/execution-summary.json", "role": "answer"})
            write_json(task / "task.json", declaration)
            return task

        case = review_fixtures.CleanReviewPreparationTests("test_score_prepared_inputs_are_only_normative_plus_scoring_projection")
        with patch("tests.test_execution_review.make_task", side_effect=with_execution_summary):
            case.setUp()
        self.addCleanup(case.doCleanups)
        self.case = case
        facts = case.fixture_facts()
        self.config = copy.deepcopy(case.clean_config)
        self.selected = [{**row, **_info(Path(row["path"]))} for row in case.candidate["files"] if row["package_path"] in
                         ("draft/evaluation/answer.md", "draft/evaluation/rubric.md")]
        self.config.update(facts_attempt=facts["facts_attempt"], facts_binding=facts["review_binding"],
                           scoring_source_allowlist=sorted(row["package_path"] for row in self.selected))
        self.config["work"].update(role="review-score", work_id="clean-score", intent_id="clean-score-intent")
        self.config["runner"].update(role="review-score", intent_id="clean-score-intent", outputs=["score.json"])
        self.certificate = {"schema_version": 1, "kind": "native_quality_handoff",
            "authorization": self.config["work"]["evidence"][0], "native_quality_handoff": {"fixture": "typed validator boundary"}}

    def prepare(self):
        case, runtime = self.case, self.case.root / "score-runtime"

        def enqueue(prepared, spec, target, **kwargs):
            target.mkdir()
            self.observed = copy.deepcopy(spec["runner"])
            prepared.enqueue(**spec["work"], runtime_path=str(target), dependencies=[], modifies=False)

        def probe(target, forbidden):
            write_json(target / "review-boundary-probe.json", {"fixture": "OS probe boundary only", "model_calls": 0})

        with patch("taskflow.execution.enqueue_spec", side_effect=enqueue), \
                patch("taskflow.execution_review._denied_probe", side_effect=probe), \
                patch("taskflow.execution_review._profile_definition", return_value=None):
            return execution_review.prepare_score_runner(store=case.store, config=self.config, runtime=runtime)

    def test_valid_certificate_keeps_exact_subset_and_transports_same_proof(self):
        self.config["normative_certification"] = self.certificate
        with patch("taskflow.execution_formal.validate_normative_certification", return_value=self.selected, create=True) as validate:
            result = self.prepare()
        self.assertEqual(self.case.store, validate.call_args.kwargs["store"])
        self.assertEqual(self.certificate, self.observed["scoring_projection"]["normative_certification"])
        self.assertEqual(self.certificate, read_json(Path(result["review_binding"]))["scoring_projection"]["normative_certification"])
        self.assertEqual(self.config["scoring_source_allowlist"], self.observed["scoring_source_allowlist"])
        self.assertFalse(any("execution-summary" in row["path"] for row in self.observed["inputs"]))

    def test_without_certificate_legacy_all_normative_guard_stays_closed(self):
        with self.assertRaisesRegex(TaskflowError, "normative answer/rubric"):
            self.prepare()
        self.assertFalse(any(row["role"] == "review-score" for row in self.case.store.dashboard()["work"]))

    def test_invalid_or_expanded_certificate_cannot_add_teacher_sources(self):
        self.config["normative_certification"] = self.certificate
        with patch("taskflow.execution_formal.validate_normative_certification", side_effect=TaskflowError("actual certificate mismatch"), create=True):
            with self.assertRaisesRegex(TaskflowError, "actual certificate mismatch"):
                self.prepare()
        self.config["scoring_source_allowlist"].append("draft/evaluation/author-notes.md")
        with patch("taskflow.execution_formal.validate_normative_certification", return_value=self.selected, create=True):
            with self.assertRaisesRegex(TaskflowError, "exact independently certified"):
                self.prepare()


class ScorePreparationResumeTests(unittest.TestCase):
    def setUp(self):
        case = experiment_fixtures.ExperimentBoundaryTests("test_real_fixture_workers_traverse_import_facts_seal_score_and_original_intake")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.case, self.store = case, case.store
        record = case.ready()
        execution.run_worker(case.store, "neutral-original", False, [record["state"]["current_work_id"]])
        with patch("taskflow.execution_experiment.import_formal", side_effect=case.import_original_fixture):
            record = case.advance()
        with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=case.fixture_role):
            record = case.advance()
        execution.run_worker(case.store, "neutral-facts", False, [record["state"]["current_work_id"]])
        record = case.advance()
        definition = copy.deepcopy(record["definition"])
        definition.update(flow_id="score-resume", source_author_flow_id="neutral-source-author")
        state = copy.deepcopy(record["state"])
        state.update(status="exception", exception={"stage": "score_prepare", "reason": REASON})
        self.record = self.store.register_flow("score-resume", definition, record["evidence"], state)
        self.record = advance_lifecycle(self.store, "score-resume")
        self.packet = read_json(case.authority)
        self.selected = [{**row, **_info(Path(row["path"]))} for row in case.candidate["files"] if row["role"] in {"answer", "rubric"}]
        self.native = {"scoring_source_allowlist": sorted(row["package_path"] for row in self.selected),
                       "normative_sources": [{**row, **_info(Path(row["path"]))} for row in self.selected]}
        self.certificate = {"schema_version": 1, "kind": "native_quality_handoff",
            "candidate_binding": {key: case.candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
            "authorization": definition["authorization"], "source_author_flow_id": definition["source_author_flow_id"],
            "native_quality_handoff": self.native}
        work = _work(self.store, self.record, "review-score", "Independent exact-run review-score", [state["facts_attempt"]["work_id"]])
        run_id = self.packet["formal"]["first_run_id"]
        runtime = Path(definition["technical"]["formal_runtime_root"]) / (".review-" + run_id) / "score/runtime"
        config = {"work": work, "run_directory": state["imported_run"]["run_path"],
            "first_review_index": state["imported_run"]["first_review_index"], "excluded_sids": state["excluded_sids"],
            "facts_attempt": state["facts_attempt"]["runtime_path"], "facts_binding": state["facts_prepared"]["review_binding"],
            "scoring_source_allowlist": self.native["scoring_source_allowlist"]}
        enqueue = execution.enqueue_spec
        enqueue_work = self.store.enqueue

        def with_locked_certificate(store, spec, root, **kwargs):
            spec["runner"]["scoring_projection"]["normative_certification"] = self.certificate
            old = Path(spec["runner"]["dependencies"][0])
            binding = read_json(old)
            binding["scoring_projection"]["normative_certification"] = self.certificate
            self.binding_path = runtime.parent / "runtime-review/review-binding.json"
            write_json(self.binding_path, binding)
            spec["runner"]["dependencies"][0] = str(self.binding_path)
            return enqueue(store, spec, root, **kwargs)

        def with_probe_receipt(**arguments):
            proof = runtime / "review-boundary-probe.json"
            write_json(proof, {"manifest": _info(runtime / "manifest.json"), "model_calls": 0, "returncode": 0})
            arguments["evidence"] = [*arguments["evidence"], evidence_ref(proof, "mocked OS probe, bound before ready enqueue")]
            return enqueue_work(**arguments)

        with patch("taskflow.execution.enqueue_spec", side_effect=with_locked_certificate), \
                patch.object(self.store, "enqueue", side_effect=with_probe_receipt), \
                patch("taskflow.execution_formal.validate_normative_certification", return_value=self.selected, create=True):
            self.prepared = case.fixture_role(store=self.store, config=config, runtime=runtime, scoring=True)
        self.prepared["review_binding"] = str(self.binding_path)
        self.prepared_path = case.root / "prepared-score.json"
        write_json(self.prepared_path, self.prepared)
        self.resolution = {"action": "resume_validated_score_preparation", "reason": "Consume the exact prepared normative subset.",
                           "evidence": [evidence_ref(case.authority, "explicit neutral technical recovery")],
                           "preparation": evidence_ref(self.prepared_path, "actual zero-model prepared ready work")}
        facts = Path(state["facts_attempt"]["runtime_path"])
        self.originals = {path: path.read_bytes() for path in [facts / "facts-seal.json", facts / "collection.json",
                          facts / "trace.jsonl", facts / "artifacts/facts.json", Path(self.record["state"]["receipt"])]}

    def resume(self):
        with patch("taskflow.execution_experiment._packet", return_value=self.packet), \
                patch("taskflow.execution_experiment._native_handoff", return_value=self.native), \
                patch("taskflow.execution_formal.validate_normative_certification", return_value=self.selected, create=True), \
                patch("taskflow.execution_review.seal_existing_facts", side_effect=AssertionError("resume must only read the existing seal")), \
                patch("taskflow.execution.run_worker", side_effect=AssertionError("resume cannot dispatch")):
            return resume_path_transport(self.store, self.record["flow_id"], self.resolution)

    def test_ready_same_sid_resume_preserves_originals_and_same_stage_without_claim(self):
        attempts = copy.deepcopy(self.store.dashboard()["attempts"])
        result = self.resume()
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("score_prepare", result["state"]["stage"])
        self.assertIsNone(result["state"]["current_work_id"])
        self.assertEqual(attempts, self.store.dashboard()["attempts"])
        self.assertEqual(self.record["definition"], result["definition"])
        for path, data in self.originals.items():
            self.assertEqual(data, path.read_bytes())

    def test_any_claimed_score_attempt_rejects_without_reset(self):
        attempt = self.store.claim("neutral-score-owner", [self.prepared["work"]["work_id"]])
        with self.assertRaisesRegex(TaskflowError, "score attempt already exists"):
            self.resume()
        self.assertEqual("claimed", self.store.get_attempt(attempt["attempt_id"])["state"])
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))

    def test_prepared_proof_or_existing_launch_marker_cannot_be_cleared(self):
        changed = copy.deepcopy(self.prepared)
        changed["run_digest"] = "0" * 64
        write_json(self.prepared_path, changed)
        self.resolution["preparation"] = evidence_ref(self.prepared_path, "wrong run proof")
        with self.assertRaisesRegex(TaskflowError, "original facts/run"):
            self.resume()
        write_json(self.prepared_path, self.prepared)
        self.resolution["preparation"] = evidence_ref(self.prepared_path, "actual ready proof")
        write_json(Path(self.prepared["runtime"]) / "dispatch.json", {"intent": "already uncertain"})
        with self.assertRaisesRegex(TaskflowError, "launch or collection"):
            self.resume()
        self.assertEqual(self.record, self.store.get_flow(self.record["flow_id"]))

