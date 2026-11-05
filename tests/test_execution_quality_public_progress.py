"""Public-only observations and zero-model completed-turn recovery."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_runner import RunnerError, _info
from taskflow.execution_quality_session import _progress, capability_signature, validate_session
from taskflow.execution_flow import advance_workflow, resume_workflow
from tests import test_execution_quality_reading as fixtures


class PublicProgressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        (self.root / "artifacts").mkdir()
        (self.root / "inputs/execution").mkdir(parents=True)
        (self.root / "inputs/public.txt").write_text("Neutral complete public source.")
        source = {"path": "public.txt", "role": "input", **_info(self.root / "inputs/public.txt")}
        self.index = {"schema_version": 1, "input_digest": "neutral-input", "files": [source]}
        write_json(self.root / "inputs/execution/public-inputs.json", self.index)
        checkpoint = self.root / "artifacts/public-responsibility-first.json"
        checkpoint.write_text("Neutral public responsibility observation.")
        digest = _info(checkpoint)["sha256"]
        (self.root / "trace.jsonl").write_text(json.dumps({"type": "item.completed", "item": {
            "type": "command_execution", "aggregated_output": "PUBLIC_RESPONSIBILITY_FIRST_SHA256=" + digest}}))
        self.manifest = {"intent_id": "neutral-intent", "candidate_binding": {"input_digest": "neutral-input"},
                         "cwd": str(self.root / "work"), "quality_session": {"index": 0}, "inputs": [
                             {"target": "execution/public-inputs.json", "kind": "source_metadata",
                              **_info(self.root / "inputs/execution/public-inputs.json")},
                             {"target": "public.txt", "kind": "public", **_info(self.root / "inputs/public.txt")} ]}
        self.result = {"schema_version": 1, "kind": "quality_read_progress_v1", "intent_id": "neutral-intent",
                       "candidate_binding": self.manifest["candidate_binding"], "segment_index": 0,
                       "status": "progress_only", "pages": [{**source, "read": "full original",
                           "sha256_verified": True, "size_verified": True}], "facts": [], "remaining": [],
                       "public_checkpoint": {"sha256": digest, "tool_readback_location": "neutral actual response",
                           "path": str(self.root / "work/public-responsibility-first.json"),
                           "sequence": "Original self-reported ordering; not independently certified."}}
        self.plan = {"turns": [{"kind": "public_first", "page_ids": []}], "pages": []}

    def receive(self):
        name = "reading-progress-" + str(self.manifest["quality_session"]["index"]).zfill(3) + ".json"
        path = self.root / "artifacts" / name
        write_json(path, self.result)
        original = path.read_bytes()
        received = _progress(self.root, self.manifest, self.plan, {"artifacts": {name: _info(path)}})
        self.assertEqual(original, path.read_bytes())
        return received

    def test_public_observations_preserve_original_and_optional_checkpoint_locations(self):
        self.assertEqual(self.result, self.receive())
        self.result["pages"][0]["sha256_verified"] = False
        self.result["pages"][0]["read"] = "Unresolved reading claim"
        self.assertEqual(self.result, self.receive())

    def test_legacy_empty_first_pages_remains_valid(self):
        self.result["pages"] = []
        for key in ("path", "sequence"):
            self.result["public_checkpoint"].pop(key)
        self.assertEqual(self.result, self.receive())

    def test_identity_membership_and_shape_drift_are_rejected(self):
        original = copy.deepcopy(self.result)
        cases = [{"path": "private.txt"}, {"role": "teacher"}, {"sha256": "wrong"},
                 {"size": True}, {"read": ""}, {"sha256_verified": "true"}, {"extra": "unexpected"}]
        for change in cases:
            with self.subTest(change=change):
                self.result = copy.deepcopy(original)
                self.result["pages"][0].update(change)
                with self.assertRaises(TaskflowError): self.receive()
        self.result = copy.deepcopy(original)
        self.result["pages"] *= 2
        with self.assertRaises(TaskflowError): self.receive()

    def test_missing_public_member_and_changed_staged_bytes_are_rejected(self):
        self.index["files"].append({**self.index["files"][0], "path": "second.txt"})
        (self.root / "inputs/second.txt").write_bytes((self.root / "inputs/public.txt").read_bytes())
        self.manifest["inputs"].append({"target": "second.txt", "kind": "public", **_info(self.root / "inputs/second.txt")})
        index_path = self.root / "inputs/execution/public-inputs.json"
        write_json(index_path, self.index)
        self.manifest["inputs"][0].update(_info(index_path))
        with self.assertRaises(TaskflowError): self.receive()
        self.result["pages"].append({**self.result["pages"][0], "path": "second.txt"})
        self.assertEqual(self.result, self.receive())
        (self.root / "inputs/public.txt").write_text("Changed")
        with self.assertRaises(TaskflowError): self.receive()

    def test_index_lock_and_private_index_are_rejected(self):
        index_path = self.root / "inputs/execution/public-inputs.json"
        self.index["files"][0]["role"] = "teacher"
        write_json(index_path, self.index)
        with self.assertRaises(TaskflowError): self.receive()
        self.manifest["inputs"][0].update(_info(index_path))
        with self.assertRaises(TaskflowError): self.receive()

    def test_later_private_pages_cannot_be_replaced_by_public_observations(self):
        self.manifest["quality_session"]["index"] = 1
        self.result.update(segment_index=1, public_checkpoint=None)
        self.plan["turns"].append({"kind": "read", "page_ids": ["private-page"]})
        with self.assertRaisesRegex(TaskflowError, "every assigned page"): self.receive()

    def test_optional_checkpoint_fields_do_not_replace_hash_or_actual_tool_anchor(self):
        self.result["public_checkpoint"]["path"] = str(self.root / "other.json")
        with self.assertRaises(TaskflowError): self.receive()
        self.result["public_checkpoint"]["path"] = str(self.root / "work/public-responsibility-first.json")
        self.result["public_checkpoint"]["sequence"] = ""
        with self.assertRaises(TaskflowError): self.receive()
        self.result["public_checkpoint"]["sequence"] = "Claim only"
        (self.root / "trace.jsonl").write_text("")
        with self.assertRaisesRegex(TaskflowError, "actual visible"): self.receive()


class SerialResumeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SerialQualityReadingTests()
        line = "  name='reading-progress-'"
        extra = ("  if serial['index']==0 and (store/'public-observations').exists():\n"
                 "   index=json.loads((pathlib.Path(ctx['inputs_root'])/'execution/public-inputs.json').read_text())\n"
                 "   dispositions=[dict(p,read='full original',sha256_verified=True,size_verified=True) for p in index['files']]\n"
                 "   checkpoint.update(path=str(pathlib.Path.cwd()/'public-responsibility-first.json'),sequence='Synthetic public-only sequence claim')\n")
        self.assertIn(line, fixtures.SERIAL_PREFIX)
        with patch.object(fixtures, "SERIAL_PREFIX", fixtures.SERIAL_PREFIX.replace(line, extra + line)):
            self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.store = self.fixture.store
        self.fid = self.fixture.fid

    def completed_exception(self):
        from taskflow.execution import run_worker
        self.fixture.authorize()
        prepared = advance_workflow(self.store, self.fid)
        attempt = run_worker(self.store, "neutral-owner", True, [prepared["state"]["current_work_id"]])
        self.assertEqual("completed", attempt["state"])
        with patch("taskflow.execution_quality_session._progress", side_effect=TaskflowError("Neutral old parser rejection")):
            stopped = advance_workflow(self.store, self.fid)
        stopped = advance_workflow(self.store, self.fid)
        self.assertEqual("exception", stopped["state"]["status"])
        return stopped, attempt

    def resume(self):
        return resume_workflow(self.store, self.fid, reason="Neutral completed source compatibility repair",
                               evidence=[self.fixture.helper.refs[0]])

    def test_normal_resume_consumes_original_completed_serial_turn_without_launch(self):
        (self.fixture.sessions / "public-observations").touch()
        stopped, attempt = self.completed_exception()
        original_receipt = Path(stopped["state"]["receipt"]).read_bytes()
        runtime = Path(attempt["runtime_path"])
        preserved = {name: (runtime / name).read_bytes() for name in [
            "manifest.json", "manifest-lock.json", "request.md", "trace.jsonl", "collection.json",
            "artifacts/reading-progress-000.json", "execution_quality_capture.py"]}
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("must not launch")):
            active = self.resume()
            self.assertEqual("active", active["state"]["status"])
            received = advance_workflow(self.store, self.fid)
        self.assertEqual(1, received["state"]["serial_quality_reading"]["index"], received["state"].get("exception"))
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))
        entry = received["state"]["serial_quality_reading"]["turns"][0]
        self.assertEqual(attempt["attempt_id"], entry["attempt_id"])
        self.assertEqual(attempt["cli_sid"], entry["cli_sid"])
        self.assertFalse(entry["quality_accepted"])
        self.assertEqual(original_receipt, Path(stopped["state"]["receipt"]).read_bytes())
        for name, data in preserved.items(): self.assertEqual(data, (runtime / name).read_bytes(), name)
        self.assertEqual(1, len(received["state"]["resolved_exceptions"]))
        self.assertEqual("active", self.resume()["state"]["status"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_cursor_runtime_and_manifest_binding_mismatch_reject_without_mutation(self):
        stopped, attempt = self.completed_exception()
        baseline = copy.deepcopy(stopped["state"])
        for key, value in [("current_runtime", "/neutral/wrong"), ("current_node_id", "wrong-node")]:
            with self.subTest(key=key):
                state = copy.deepcopy(baseline); state[key] = value
                record = self.store.get_flow(self.fid)
                self.store.update_flow(self.fid, record["revision"], state, {"kind": "neutral_fixture_change"})
                before = self.store.get_flow(self.fid)
                with self.assertRaises(TaskflowError): self.resume()
                self.assertEqual(before, self.store.get_flow(self.fid))
        record = self.store.get_flow(self.fid)
        self.store.update_flow(self.fid, record["revision"], baseline, {"kind": "neutral_fixture_restore"})
        manifest = Path(attempt["runtime_path"]) / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
        with self.assertRaises((TaskflowError, RunnerError)): self.resume()

    def test_failed_serial_attempt_is_never_retried(self):
        (self.fixture.sessions / "missing-turn").touch()
        stopped = self.fixture.first_progress()
        self.assertEqual("exception", stopped["state"]["status"])
        before = self.store.get_flow(self.fid)
        with self.assertRaises(TaskflowError): self.resume()
        self.assertEqual(before, self.store.get_flow(self.fid))
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_unlaunched_serial_preflight_resumes_without_recreating_prior_turn(self):
        received = self.fixture.first_progress()
        self.assertEqual(1, received["state"]["serial_quality_reading"]["index"])
        seal_path = Path(received["state"]["serial_quality_reading"]["turns"][0]["seal"]["path"])
        original_seal = seal_path.read_bytes()
        with patch("taskflow.execution_flow._prepare_node", side_effect=TaskflowError("Neutral preflight compatibility failure")):
            advance_workflow(self.store, self.fid)
        stopped = advance_workflow(self.store, self.fid)
        self.assertIsNone(stopped["state"]["current_work_id"])
        resumed = self.resume()
        resolution = resumed["state"]["resolved_exceptions"][-1]["serial_quality_node"]
        self.assertEqual("retry_zero_model_preflight", resolution["action"])
        self.assertIsNone(resolution["attempt_id"])
        self.assertEqual(1, resolution["index"])
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("no launch")):
            prepared = advance_workflow(self.store, self.fid)
        self.assertEqual("active", prepared["state"]["status"], prepared["state"].get("exception"))
        self.assertIn("read-001", prepared["state"]["current_work_id"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))
        self.assertEqual(original_seal, seal_path.read_bytes())

    def test_unlaunched_preflight_foreign_runtime_is_rejected(self):
        received = self.fixture.first_progress()
        with patch("taskflow.execution_flow._prepare_node", side_effect=TaskflowError("Neutral preflight failure")):
            advance_workflow(self.store, self.fid)
        record = self.store.get_flow(self.fid)
        state = copy.deepcopy(record["state"])
        state["current_runtime"] = "/neutral/foreign-runtime"
        self.store.update_flow(self.fid, record["revision"], state, {"kind": "neutral_fixture_change"})
        before = self.store.get_flow(self.fid)
        with self.assertRaisesRegex(TaskflowError, "zero-model preflight"): self.resume()
        self.assertEqual(before, self.store.get_flow(self.fid))


class CapabilityRepresentationTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("/neutral/turn-zero/runtime")
        self.fs = {"/neutral/repository": "read", "/neutral/work": "write", str(self.root / "inputs"): "read"}
        from tools.isolated_quality import inline_table
        self.argument = "permissions.neutral.filesystem=" + inline_table(self.fs)
        self.manifest = {"mode": "native_role", "argv": ["neutral-cli", "-c", self.argument, "exec"],
                         "model": {"requested": "neutral-model", "reasoning_effort": "medium"},
                         "native": {"profile": "neutral", "filesystem": self.fs,
                                    "sandbox_argv": ["neutral-cli", "-c", self.argument]}}

    def signature(self, manifest, normalize=True):
        return capability_signature(manifest, self.root, normalize_representation=normalize)

    def test_absent_null_and_equivalent_table_order_keep_historical_signatures(self):
        from tools.isolated_quality import inline_table
        original = copy.deepcopy(self.manifest)
        raw_before = self.signature(original, False)
        changed = copy.deepcopy(original)
        changed["native"]["provider"] = None
        reordered = "permissions.neutral.filesystem=" + inline_table(dict(reversed(list(self.fs.items()))))
        changed["argv"][2] = reordered
        changed["native"]["sandbox_argv"][2] = reordered
        self.assertNotEqual(raw_before, self.signature(changed, False))
        self.assertEqual(self.signature(original), self.signature(changed))
        self.assertEqual(raw_before, self.signature(original, False))
        self.assertEqual(original, self.manifest)

    def test_real_capability_and_command_changes_remain_different(self):
        for change in ("provider", "model", "effort", "command", "permission"):
            with self.subTest(change=change):
                altered = copy.deepcopy(self.manifest)
                if change == "provider": altered["native"]["provider"] = {"name": "other-provider"}
                if change == "model": altered["model"]["requested"] = "other-model"
                if change == "effort": altered["model"]["reasoning_effort"] = "low"
                if change == "command": altered["argv"][0] = "other-cli"
                if change == "permission":
                    from tools.isolated_quality import inline_table
                    altered["native"]["filesystem"]["/neutral/repository"] = "write"
                    argument = "permissions.neutral.filesystem=" + inline_table(altered["native"]["filesystem"])
                    altered["argv"][2] = argument
                    altered["native"]["sandbox_argv"][2] = argument
                self.assertNotEqual(self.signature(self.manifest), self.signature(altered))

    def test_malformed_duplicate_or_unbound_filesystem_arguments_reject(self):
        cases = ['{"a"="read","a"="read"}', '{"a"=1}', '{"a":"read"}',
                 '{"a"="read",}', '{"a"="read";', '{}', '{"a"="read"}']
        for table in cases:
            with self.subTest(table=table):
                altered = copy.deepcopy(self.manifest)
                altered["argv"][2] = "permissions.neutral.filesystem=" + table
                with self.assertRaises((TaskflowError, ValueError)): self.signature(altered)
        altered = copy.deepcopy(self.manifest)
        altered["argv"].insert(2, self.argument)
        with self.assertRaises(TaskflowError): self.signature(altered)

    def test_session_validation_accepts_only_representational_drift_without_resigning(self):
        from tools.isolated_quality import inline_table
        with tempfile.TemporaryDirectory(dir="/private/tmp") as directory:
            root = Path(directory)
            session_file = root / "neutral-owned-session.jsonl"
            session_file.write_bytes(b"synthetic owned context")
            reference = {"path": str(root / "quality-reading-seal.json"), "sha256": "synthetic"}
            prior_session = {"schema_version": 1, "plan": {"synthetic": True}, "index": 0,
                             "phase": "progress", "previous": None, "flow_id": "neutral-flow",
                             "session_store": str(root)}
            before = copy.deepcopy(self.manifest)
            before.update(role="quality", candidate_binding={"synthetic": "binding"})
            previous = {"session": prior_session, "signature": capability_signature(before, root),
                        "sid": "neutral-sid", "session_file": str(session_file)}
            current = copy.deepcopy(before)
            current.update(resume_sid="neutral-sid", quality_session={**prior_session, "index": 1, "previous": reference})
            current["native"]["provider"] = None
            argument = "permissions.neutral.filesystem=" + inline_table(dict(reversed(list(self.fs.items()))))
            current["argv"][2] = argument
            current["native"]["sandbox_argv"][2] = argument
            preserved = copy.deepcopy(previous)
            with patch("taskflow.execution_quality_session.validate_plan", return_value=(None, {"turns": [{}, {}]})), \
                 patch("taskflow.execution_quality_session.read_seal", return_value=(reference, previous, before, session_file.read_bytes())), \
                 patch("taskflow.execution_quality_session.session_store", return_value=root):
                validate_session(current, root=root, before_run=True)
                current["model"]["requested"] = "different-model"
                with self.assertRaisesRegex(TaskflowError, "capabilities changed"):
                    validate_session(current, root=root, before_run=True)
            self.assertEqual(preserved, previous)
