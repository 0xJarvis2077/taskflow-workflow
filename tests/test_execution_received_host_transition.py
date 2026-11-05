"""Synthetic sealed native receipts; no executable or model is started."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution_quality_continuation import _received_host_dependencies
from taskflow.execution_runner import RunnerError, _collection_files, _info, _validate
from taskflow.execution_store import evidence_ref


class ReceivedHostTransitionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve() / "runtime"
        self.root.mkdir()
        self.host = self.root.parent / "host"
        self.host.mkdir()
        self.cli = self.host / "cli"
        self.python = self.host / "python3.99"
        self.node = self.host / "node"
        for path in (self.cli, self.python, self.node):
            path.write_bytes(b"\x7fELFsynthetic-original")
        self.policy = self.host / "policy.json"
        self.policy.write_text("{}")
        (self.root / "request.md").write_text("original request")
        (self.root / "cli-evidence.json").write_text("{}")
        self.manifest = {
            "intent_id": "original-intent", "mode": "native_role", "inputs": [],
            "argv": [str(self.cli), "-c", 'shell_environment_policy.set={"PATH"=' + json.dumps(str(self.host)) + '}'],
            "native": {"python": str(self.python)}, "model": {"catalog_evidence": _info(self.root / "cli-evidence.json")},
            "dependencies": {str(p): _info(p) for p in (self.cli, self.python, self.node, self.policy)},
            "request": _info(self.root / "request.md"), "outputs": []}
        write_json(self.root / "manifest.json", self.manifest)
        write_json(self.root / "manifest-lock.json", _info(self.root / "manifest.json"))
        write_json(self.root / "termination.json", {"intent_id": "original-intent", "returncode": 0, "timed_out": False})
        self.seal()
        self.authority = self.host / "authority.txt"
        self.authority.write_text("Synthetic explicit technical recovery authority")
        self.attempt = {"attempt_id": "original-attempt", "intent_id": "original-intent"}
        self.changes = {}
        for path in (self.cli, self.python, self.node):
            before = _info(path)
            path.write_bytes(b"\x7fELFsynthetic-updated")
            self.changes[str(path)] = {"before": before, "after": _info(path)}
        self.packet = {
            "schema_version": 1, "kind": "received_host_dependency_transition_v1", **self.attempt,
            "manifest": evidence_ref(self.root / "manifest.json", "synthetic original"),
            "collection": evidence_ref(self.root / "collection.json", "synthetic original"),
            "termination": evidence_ref(self.root / "termination.json", "synthetic original"),
            "dependencies": self.changes, "direct_user_authority": evidence_ref(self.authority, "synthetic authority")}

    def seal(self):
        write_json(self.root / "collection.json", {"intent_id": "original-intent", "status": "completed",
            "technical_accepted": True, "evidence_files": _collection_files(self.root), "artifacts": {}})
        write_json(self.root / "collection-lock.json", _info(self.root / "collection.json"))

    def transition(self, packet=None):
        path = self.host / "transition.json"
        write_json(path, packet or self.packet)
        return _received_host_dependencies(self.root, self.attempt, evidence_ref(path, "synthetic transition"))

    def test_explicit_received_observation_preserves_original_but_never_authorizes_launch(self):
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        reference, changes = self.transition()
        self.assertEqual(self.changes, changes)
        with self.assertRaisesRegex(RunnerError, "Locked executable"):
            _validate(self.root)
        self.assertEqual(self.manifest, _validate(self.root, received_dependencies=changes))
        with self.assertRaisesRegex(RunnerError, "cannot authorize a launch"):
            _validate(self.root, before_run=True, received_dependencies=changes)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_wrong_attempt_original_hash_and_unknown_dependency_are_rejected(self):
        for update in ({"attempt_id": "foreign"}, {"manifest": self.packet["collection"]},
                       {"dependencies": {str(self.policy): {"before": {}, "after": {}}}}):
            with self.subTest(update=update), self.assertRaises(TaskflowError):
                self.transition({**self.packet, **update})

    def test_script_disguised_as_host_binary_is_rejected(self):
        self.node.write_text("#!/bin/sh\nexit 0\n")
        changes = copy.deepcopy(self.changes)
        changes[str(self.node)]["after"] = _info(self.node)
        with self.assertRaisesRegex(TaskflowError, "binary executables"):
            self.transition({**self.packet, "dependencies": changes})

    def test_missing_extra_or_changed_observations_are_rejected(self):
        _, changes = self.transition()
        for observed in ({}, {str(self.cli): changes[str(self.cli)]},
                         {**changes, str(self.policy): {"before": {}, "after": {}}}):
            with self.subTest(observed=observed), self.assertRaises(RunnerError):
                _validate(self.root, received_dependencies=observed)
        self.python.write_bytes(b"\x7fELFanother-update")
        with self.assertRaises(TaskflowError):
            self.transition()

    def test_unlisted_policy_drift_and_original_request_drift_still_fail(self):
        _, changes = self.transition()
        self.policy.write_text('{"changed":true}')
        with self.assertRaisesRegex(RunnerError, "Locked executable"):
            _validate(self.root, received_dependencies=changes)
        self.policy.write_text("{}")
        (self.root / "request.md").write_text("changed request")
        with self.assertRaises(RunnerError):
            _validate(self.root, received_dependencies=changes)

    def test_seal_termination_and_artifact_tampering_cannot_be_recovered(self):
        _, changes = self.transition()
        write_json(self.root / "termination.json", {"intent_id": "foreign", "returncode": 0, "timed_out": False})
        with self.assertRaises(RunnerError):
            _validate(self.root, received_dependencies=changes)
        self.seal()
        with self.assertRaisesRegex(RunnerError, "successful terminal"):
            _validate(self.root, received_dependencies=changes)


if __name__ == "__main__":
    unittest.main()
