"""Same-attempt recovery preserves originals and feeds ordinary author consumers."""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError
from taskflow.execution_completion_observation import (
    author_collection, author_technically_complete, observation_material,
    received_dependencies, validated_observation,
)
from taskflow.execution_runner import (
    RunnerError, _author_host_executables, _info, _trace, _validate, archive_full_attempt,
)
from tests import test_execution_optional_candidate_delivery as fixture


class CompletionObservationTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.OptionalCandidateDeliveryTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def legacy_author(self):
        case = self.case
        source = case.script.read_text()
        source = source.replace("if mode!='no-turn':print", "print")
        source = source.replace("print(json.dumps({'type':'turn.completed'}))",
            "print(json.dumps({'type':'error','message':'Reconnecting... 2/3 (neutral transport interrupted)'}))\n"
            "print(json.dumps({'type':'turn.completed'}))")
        case.script.write_text(source)

        def legacy(path, expected_sid=None):
            current = _trace(path, expected_sid)
            if current.get("reconnect_progress"):
                return {**current, "model_completed": False, "errors": ["missing_or_failed_turn"],
                        "terminal_events": ["error", "turn.completed"]}
            return current

        with patch("taskflow.execution_runner._trace", side_effect=legacy):
            case.run_author("no-turn")
        self.assertEqual("failed", case.attempt["state"])
        return case.attempt

    def observe(self):
        case = self.case
        return case.store.observe_author_completion(case.attempt["attempt_id"], evidence=case.refs)

    def test_append_is_idempotent_and_normal_candidate_reception_uses_original_attempt(self):
        old = self.legacy_author()
        case = self.case
        originals = {name: (case.runtime / name).read_bytes() for name in
                     ("manifest.json", "request.md", "trace.jsonl", "termination.json",
                      "collection.json", "collection-lock.json")}
        result = self.observe()
        self.assertEqual(result, self.observe())
        self.assertEqual("failed", result["state"])
        self.assertFalse(result["technical_accepted"])
        self.assertFalse(result["response_complete"])
        self.assertTrue(author_technically_complete(result))
        self.assertFalse(author_technically_complete(old))
        self.assertEqual("failed", case.store.get_work(old["work_id"])["state"])
        self.assertEqual(1, len(case.store.list_attempts(old["work_id"])))
        archive_full_attempt(case.runtime, case.archive,
                             received_dependencies=received_dependencies(result))
        case.attempt = result
        updated = case.adopt()
        child = execution.capture_candidate(case.store, Path(updated["supplemental_delivery"]["candidate_root"]),
                    "child", parent_id="parent", author_attempt=old["attempt_id"])
        self.assertEqual(old["attempt_id"], child["author_attempt"])
        self.assertTrue(case.store._delivered_artifacts(updated))
        view = author_collection(updated, case.archive)
        self.assertTrue(view["technical_accepted"])
        self.assertEqual("same_attempt_transport_completion_v1", view["interpretation"])
        for name, raw in originals.items():
            self.assertEqual(raw, (case.runtime / name).read_bytes())
            self.assertEqual(raw, (case.archive / name).read_bytes())
        self.assertIsNone(case.store.claim("never-redraw"))

    def test_unterminated_turn_and_wrong_role_or_flags_never_gain_observation(self):
        case = self.case
        case.run_author("no-turn")
        with self.assertRaises(TaskflowError):
            self.observe()
        self.legacy_flags(case.attempt)

    def legacy_flags(self, attempt):
        for change in ({"role": "repair"}, {"state": "completed"}, {"process_exit": 1},
                       {"response_complete": True}, {"artifacts_complete": False},
                       {"technical_accepted": True}, {"cli_sid": None}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                observation_material({**attempt, **change})

    def test_receipt_binding_and_original_files_are_revalidated_on_consumption(self):
        self.legacy_author()
        result = self.observe()
        bad = copy.deepcopy(result)
        bad["completion_observation"]["identity"]["work_id"] = "foreign"
        with self.assertRaises(TaskflowError):
            validated_observation(bad)
        trace = self.case.runtime / "trace.jsonl"
        trace.write_text(trace.read_text() + "{}\n")
        with self.assertRaises(TaskflowError):
            author_technically_complete(result)

    def test_reception_evidence_cannot_be_replaced(self):
        self.legacy_author()
        self.observe()
        other = self.case.root / "other.json"
        other.write_text("{}")
        from taskflow.execution_store import evidence_ref

        with self.assertRaisesRegex(TaskflowError, "cannot replace"):
            self.case.store.observe_author_completion(self.case.attempt["attempt_id"],
                        evidence=[evidence_ref(other, "another reception")])

    def test_changed_non_host_dependency_is_rejected(self):
        self.legacy_author()
        self.case.script.write_text("changed source, not a host executable update")
        with self.assertRaisesRegex(TaskflowError, "changed input or dependency"):
            self.observe()

    def test_cli_update_is_historical_only_and_cannot_launch(self):
        self.legacy_author()
        case = self.case
        # The fixture CLI is the shared Python interpreter; never mutate it.
        original_info = _info
        executable = json.loads((case.runtime / "manifest.json").read_text())["argv"][0]

        def upgraded(path):
            result = original_info(path)
            return {"sha256": "a" * 64, "size": result["size"]} if str(path) == executable else result

        with patch("taskflow.execution_completion_observation._info", side_effect=upgraded), \
             patch("taskflow.execution_runner._info", side_effect=upgraded):
            result = self.observe()
            changes = received_dependencies(result)
            self.assertEqual({executable}, set(changes))
            _validate(case.runtime, received_dependencies=changes)
            with self.assertRaisesRegex(RunnerError, "cannot authorize a launch"):
                _validate(case.runtime, before_run=True, received_dependencies=changes)
        with self.assertRaises(TaskflowError):
            validated_observation(result)

    def test_native_host_binary_set_excludes_data_and_repository_code(self):
        base = "/neutral/runtime/dependencies"
        python = base + "/python/bin/python3.12"
        node = base + "/node/bin/node"
        manifest = {"argv": ["/neutral/codex"], "native": {
            "python": python, "filesystem": {base: "read", "/repository": "read"}},
            "dependencies": {node: {}, "/repository/bin/node": {}, base + "/data.json": {}}}
        self.assertEqual({"/neutral/codex", python, node}, _author_host_executables(manifest))

    def test_normal_author_lifecycle_resumes_only_its_exact_failed_source(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_cli_entry_appends_without_allow_model_flag(self):
        self.legacy_author()
        case = self.case
        record = case.root / "reception.json"
        record.write_text(json.dumps({"evidence": case.refs}))
        from contextlib import redirect_stdout
        import io

        from contextlib import redirect_stderr
        before = case.store.events()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            execution.main(["--db", str(case.store.path), "author-completion-observe",
                            "--attempt-id", case.attempt["attempt_id"], "--record", str(record)])
        self.assertEqual(2, error.exception.code)
        self.assertEqual(before, case.store.events())
        self.assertEqual("failed", case.store.get_attempt(case.attempt["attempt_id"])["state"])


class TransportTraceTests(unittest.TestCase):
    def test_only_bounded_preterminal_progress_plus_one_successful_turn_completes(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory).resolve() / "trace.jsonl"
            sid = "12345678-1234-4567-8123-123456789012"
            prefix = [{"type": "thread.started", "thread_id": sid},
                      {"type": "item.completed", "item": {"type": "agent_message", "text": "Complete"}}]
            progress = {"type": "error", "message": "Reconnecting... 2/3 (transport failure)"}
            end = {"type": "turn.completed"}
            for tail, accepted in (([progress, end], True), ([progress], False),
                    ([{"type": "error", "message": "403 Forbidden"}, end], False),
                    ([{"type": "error", "message": "Reconnecting... 4/3 (failure)"}, end], False),
                    ([{"type": "error", "message": "Reconnecting... 0/3 (failure)"}, end], False),
                    ([progress, {"type": "turn.failed"}, end], False),
                    ([end, progress], False), ([progress, end, end], False)):
                trace.write_text("\n".join(json.dumps(event) for event in prefix + tail))
                with self.subTest(tail=tail):
                    self.assertEqual(accepted, _trace(trace)["model_completed"])
            trace.write_text("not-json")
            self.assertFalse(_trace(trace)["model_completed"])
            self.assertFalse(_trace(Path(directory) / "missing")["model_completed"])
