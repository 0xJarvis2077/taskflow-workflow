"""Early author checks and retained local observations; no external models."""

import contextlib
import copy
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import start_author_from_packet
from taskflow.execution_formal import _fixed_client_observations, import_formal
from taskflow.execution_store import evidence_ref
from tests import test_execution_author_factory as authors
from tests import test_execution_experiment as experiments
from tests import test_execution_formal as formals


class PreparationObservationTests(unittest.TestCase):
    def test_original_import_os_error_remains_a_handoff_error(self):
        with patch("taskflow.execution_formal.verify_snapshot", side_effect=OSError("retained archive unavailable")):
            with self.assertRaisesRegex(TaskflowError, "import_formal: retained archive unavailable"):
                import_formal(attempt={}, archive=Path("/unused/archive"),
                              snapshot=Path("/unused/snapshot"), run_id="neutral-run")

    def test_bad_successor_template_fails_before_author_intent_or_launch(self):
        case = authors.AuthorFactoryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.factory()
        roles = read_json(case.template)["workflows"][0]["definition"]["roles"]
        roles.pop("recheck")
        write_json(case.template, roles)
        case.config["role_template_ref"] = evidence_ref(case.template, "invalid fixture roles")
        with patch("taskflow.execution_author.enqueue_spec") as enqueue, \
             patch("taskflow.execution.run_worker") as worker:
            with self.assertRaisesRegex(TaskflowError, "complete authorized"):
                start_author_from_packet(case.store, **case.kwargs(allow_model=True))
        enqueue.assert_not_called()
        worker.assert_not_called()
        self.assertFalse(case.directory.exists())
        self.assertEqual([], case.store.dashboard()["attempts"])

    def test_projection_keeps_only_original_model_fields_and_exact_locators(self):
        source = {"artifact_id": "raw_000001", "sha256": "a" * 64, "size": 123}
        context = {"observations": [None, {"model": "different-model", "reasoning": {
            "effort": "low", "context": "all_turns", "headers": "PRIVATE_SENTINEL"},
            "input": "PRIVATE_SENTINEL", "api_key": "PRIVATE_SENTINEL"}]}
        projected = _fixed_client_observations(context, source)
        self.assertEqual([{"values": {"model": "different-model", "reasoning": {
            "effort": "low", "context": "all_turns"}}, "source": {**source, "locators": [
                "/observations/1/model", "/observations/1/reasoning/effort",
                "/observations/1/reasoning/context"]}}], projected)
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(projected))
        self.assertEqual([], _fixed_client_observations(context, None))
        self.assertEqual([], _fixed_client_observations({"observations": "unknown"}, source))
        self.assertEqual([], _fixed_client_observations({"observations": [{
            "model": {"secret": "PRIVATE_SENTINEL"}, "reasoning": {"effort": True}}]}, source))

    def test_actual_archive_import_projects_probe_values_without_eligibility_claim(self):
        case = formals.FormalHandoffTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        original_write = formals.write_json
        context = {"passed": True, "model_calls": 0, "external_model_requests": 0,
                   "observations": [{"model": "neutral-observed", "reasoning": {
                       "effort": "high", "context": None}, "input": "PRIVATE_SENTINEL"}]}

        def capture(path, value):
            return original_write(path, context if Path(path).name == "exec-context.json" else value)

        with patch.object(formals, "write_json", side_effect=capture):
            release, archive, attempt = case.formal_archive()
        result = import_formal(attempt=attempt, archive=archive,
                               snapshot=Path(release["snapshot"]["path"]), run_id="neutral-run")
        selected = next(row for row in result["first_review_inputs"]
                        if row["target"] == "original/derived/actual-configuration.json")
        configuration = read_json(Path(selected["path"]))
        projected = configuration["client_capture"]["observations"]
        self.assertEqual("neutral-observed", projected[0]["values"]["model"])
        self.assertEqual(1, configuration["client_capture"]["observation_count"])
        source = projected[0]["source"]
        raw_source = next(row for row in configuration["fact_sources"]
                          if row["artifact_id"] == source["artifact_id"])
        self.assertEqual({key: source[key] for key in ("sha256", "size")},
                         {key: raw_source[key] for key in ("sha256", "size")})
        self.assertTrue(set(source["locators"]) <= set(raw_source["locators"]))
        self.assertIsNone(configuration["served_model"])
        self.assertNotIn("local_eligible", configuration)
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(configuration))

    def test_retired_experiment_cli_preserves_original_state_without_a_dispatch(self):
        case = experiments.ExperimentBoundaryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.ready()
        before = copy.deepcopy(case.store.dashboard())
        with contextlib.redirect_stderr(io.StringIO()), patch(
            "taskflow.execution_experiment.import_formal", side_effect=AssertionError("retired lifecycle must not run")
        ):
            with self.assertRaises(SystemExit) as error:
                execution.main(["--db", str(case.store.path), "lifecycle-run",
                    "--flow-id", case.record["flow_id"], "--allow-model"])
            self.assertEqual(2, error.exception.code)
        self.assertEqual(before, case.store.dashboard())


if __name__ == "__main__":
    unittest.main()
