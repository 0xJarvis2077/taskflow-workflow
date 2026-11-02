"""Native factory specifications with real neutral worker/lifecycle subprocesses.

The enqueue seam converts only the executable permission probe into a neutral
fixture; these tests do not claim a real native profile or external model run.
"""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow import execution_lifecycle
from taskflow import execution_runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import start_author_from_packet
from taskflow.execution_store import evidence_ref
from tests import test_execution_author as source_fixtures


class AuthorFactoryTests(unittest.TestCase):
    setUp = source_fixtures.AuthorSuccessorTests.setUp

    def factory(self):
        old_root = self.root / "OLD_Q1_WORK"
        readonly = self.root / "README.md"
        readonly.write_text("Neutral writable-host read-only sandbox target.")
        template = {"work": {"work_id": "OLD_Q1_WORK", "intent_id": "OLD_Q1_INTENT", "candidate_id": "OLD_Q1_CANDIDATE"},
                    "runner": {"role": "author", "mode": "native_role", "cli": str(Path(sys.executable).resolve()),
                               "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                               "cwd": str(old_root / "work"), "dependencies": [str(self.script)],
                               "inputs": [{"path": "/OLD_Q1_HISTORY", "kind": "history", "target": "old-history.json"}],
                               "request_sections": [{"kind": "history", "text": "OLD_Q1_BUSINESS_SENTINEL"}],
                               "request_text": "OLD_Q1_REQUEST_TEXT_SENTINEL", "outputs": ["wrong-old-output.md"],
                               "native_permissions": {"profile": "informed-author", "repository": str(self.root),
                                   "python": str(Path(sys.executable).resolve()), "scratch": str(old_root / "scratch"),
                                   "readonly_probe": str(readonly),
                                   "probe_sources": [str(readonly), str(old_root / "runtime/inputs/execution/task.json")],
                                   "filesystem": {str(self.root): "read", str(old_root / "work"): "write", str(old_root / "scratch"): "write"},
                                   "environment": {"PATH": "/ordinary/native/tools"},
                                   "direct_network": {"tcp": "deny", "unix": "deny"}}}}
        path = self.root / "old-author-spec.json"
        write_json(path, template)
        self.template_ref = evidence_ref(path, "approved neutral native infrastructure schema")
        self.packet = self.root / "new-business-packet.json"
        self.packet_data = {"schema_version": 1, "controller": "fixture-controller", "author_work": "CURRENT_NEW_RESEARCH",
                            "permission": "Project read-only and own work/scratch write; retain normal tools.",
                            "items": [{"task_id": "neutral", "intent_id": "new-research-intent", "candidate_id": "new-research-baseline",
                                       "baseline_root": str(self.task), "direction": "CURRENT_NEW_DIRECTION", "why": "CURRENT_NEW_REASON",
                                       "most_relevant": [str(self.task / "draft/input/prompt.md")]}],
                            "output_contract": {"required_outputs": self.required,
                                                "handoff_json": {"schema_version": 1, "independent_quality_passed": False, "new_formal_score": None}},
                            "preauthorized_normal_successors": {"candidate_complete": "Complete independent research-quality asset; no target experiment.",
                                "research_ended": "Receive actual bounded research end.", "needs_business_decision": "Return the exact unresolved choice."}}
        write_json(self.packet, self.packet_data)
        self.packet_ref = evidence_ref(self.packet, "new once-only research authority")
        self.config = {"schema_version": 1, "template_spec_ref": self.template_ref,
                       "role_template_ref": evidence_ref(self.template, "conditional role infrastructure"),
                       "policy_id": "fixture-v1", "research_root": str(self.root / "research"),
                       "lifecycle_root": str(self.root / "lifecycles")}
        self.directory = self.root / "research/new-author"
        self.native_specs = []

    def kwargs(self, allow_model=False):
        return {"packet_ref": self.packet_ref, "template_spec_ref": self.template_ref,
                "directory": self.directory, "allow_model": allow_model, "factory_config": self.config}

    def fixture_enqueue(self, store, spec, runtime, **options):
        self.assertTrue(options["probe_native"])
        self.assertTrue(options["resume_prepared"])
        self.native_specs.append(copy.deepcopy(spec))
        converted = copy.deepcopy(spec)
        converted["runner"].update(mode="test_fixture", argv=[str(Path(sys.executable).resolve()), str(self.script),
            str(self.task), "research_ended", ".5", str(Path(__file__).resolve().parents[1])])
        converted["runner"].pop("native_permissions")
        return execution.enqueue_spec(store, converted, runtime, **options)

    def actual_fixture_process(self, root, on_process=None, **options):
        # Only macOS ps startup metadata is adapted; the program, worker,
        # collection, original archive and SQLite lifecycle are actual.
        def identity(value):
            on_process({**value, "start_identity": "neutral actual factory subprocess"})
        return self.actual_run(root, on_process=identity, **options)

    def test_ready_only_scrubs_old_business_inputs_and_rebinds_native_paths(self):
        self.factory()
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("ready-only must not claim")):
            first = start_author_from_packet(self.store, **self.kwargs())
            again = start_author_from_packet(self.store, **self.kwargs())
        self.assertEqual(first, again)
        self.assertEqual("ready", first["state"])
        self.assertEqual([], self.store.dashboard()["attempts"])
        self.assertEqual(1, len(self.native_specs))
        spec = self.native_specs[0]
        request = spec["runner"]["request_sections"][0]["text"]
        self.assertIn("CURRENT_NEW_DIRECTION", request)
        self.assertIn("Ordinary shell, Node, Python, SQL", request)
        self.assertIn("only under draft/ or checks/, or as the single root file record.md", request)
        self.assertIn("draft/evaluation/author-provenance/", request)
        self.assertNotIn("OLD_Q1", request)
        self.assertNotIn("request_text", spec["runner"])
        self.assertNotIn("history", {row["kind"] for row in spec["runner"]["inputs"]})
        permissions = spec["runner"]["native_permissions"]
        self.assertEqual(str(self.directory / "scratch"), permissions["scratch"])
        self.assertEqual("write", permissions["filesystem"][str(self.directory / "work")])
        self.assertEqual("read", permissions["filesystem"][str(self.root)])
        self.assertIn(str(self.directory / "runtime/inputs/execution/task.json"), permissions["probe_sources"])
        self.assertEqual("new-research-intent", spec["work"]["intent_id"])
        self.assertEqual("new-research-baseline", spec["work"]["candidate_id"])
        self.assertEqual([], spec["runner"]["output_trees"])

    def test_actual_claim_registers_successors_before_author_finishes_and_runs_once(self):
        self.factory()
        self.actual_run = execution_runner.run_attempt
        actual_register = execution_lifecycle.register_lifecycle
        registered_states = []

        def register(store, definition, evidence):
            observed = store.get_attempt(definition["source_author_attempt"])
            registered_states.append(observed["state"])
            if len(registered_states) == 1:
                self.assertIn(observed["state"], ("claimed", "starting", "running"))
            return actual_register(store, definition, evidence)

        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.fixture_enqueue), \
             patch("taskflow.execution_runner.run_attempt", side_effect=self.actual_fixture_process), \
             patch("taskflow.execution_lifecycle.register_lifecycle", side_effect=register), \
             patch("taskflow.execution_author.run_worker", wraps=execution.run_worker) as workers:
            result = start_author_from_packet(self.store, **self.kwargs(True))
            registration = read_json(self.directory / "lifecycle-registration.json")
            repeated = start_author_from_packet(self.store, **self.kwargs(True))
        self.assertEqual("accepted", result["state"])
        self.assertEqual("research_ended", result["lifecycle"]["state"]["result"]["outcome"])
        self.assertEqual(result["attempt_id"], repeated["attempt_id"])
        self.assertEqual(registration, read_json(self.directory / "lifecycle-registration.json"))
        self.assertIn(registration["source_state_at_registration"], ("claimed", "starting", "running"))
        self.assertEqual(1, workers.call_count)
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))
        self.assertEqual(1, len(self.store.list_flows()))
        self.assertTrue(Path(result["lifecycle"]["state"]["receipt"]).is_file())

    def test_changed_packet_cannot_redraw_ready_intent(self):
        self.factory()
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.fixture_enqueue):
            start_author_from_packet(self.store, **self.kwargs())
            self.packet_data["items"][0]["direction"] = "DIFFERENT_NEW_INTENT"
            write_json(self.packet, self.packet_data)
            self.packet_ref = evidence_ref(self.packet, "changed purported authority")
            with self.assertRaisesRegex(TaskflowError, "published continuation evidence differs"):
                start_author_from_packet(self.store, **self.kwargs())
        self.assertEqual(1, len(self.store.dashboard()["work"]))
        self.assertEqual([], self.store.dashboard()["attempts"])

    def test_missing_core_contract_is_rejected_before_any_work(self):
        self.factory()
        self.packet_data["output_contract"]["required_outputs"] = ["handoff.json"]
        write_json(self.packet, self.packet_data)
        self.packet_ref = evidence_ref(self.packet, "incomplete purported authority")
        with self.assertRaisesRegex(TaskflowError, "five-file"):
            start_author_from_packet(self.store, **self.kwargs())
        self.assertEqual([], self.store.dashboard()["work"])

    def test_observed_same_controller_receives_future_dispatch_once(self):
        self.factory()
        self.store.observe("neutral", self.packet_ref, "legacy asset retained", "fixture-controller",
                           "2026-11-02T00:00:00+00:00", {})
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.fixture_enqueue):
            first = start_author_from_packet(self.store, **self.kwargs())
            again = start_author_from_packet(self.store, **self.kwargs())
        ownership = self.store.dashboard()["ownership"]
        self.assertEqual(1, len(ownership))
        self.assertEqual("fixture-controller", ownership[0]["owner"])
        self.assertEqual("future_dispatch", ownership[0]["scope"])
        self.assertEqual(first, again)
        self.assertEqual(ownership[0]["event_seq"], self.store.get_work(first["work_id"])["ownership_event_seq"])

    def test_original_worker_failure_keeps_uncertain_attempt_and_never_relaunches(self):
        self.factory()
        def crash(store, worker_id, allow_model, work_ids):
            self.assertIsNotNone(store.claim(worker_id, work_ids))
            raise RuntimeError("neutral crash after durable claim")
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=crash) as workers:
            first = start_author_from_packet(self.store, **self.kwargs(True))
            again = start_author_from_packet(self.store, **self.kwargs(True))
        self.assertEqual("exception", first["state"])
        self.assertEqual(first["attempt_id"], again["attempt_id"])
        self.assertEqual("uncertain", self.store.get_attempt(first["attempt_id"])["state"])
        self.assertEqual(1, workers.call_count)
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))


if __name__ == "__main__":
    unittest.main()
