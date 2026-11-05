"""Deterministic opt-in task-local profile generation; no probes or model calls."""

import copy
from pathlib import Path
import runpy
import unittest

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_solver_profile import validated_profile
from taskflow.execution_store import evidence_ref
from tests import test_execution_solver_profile as fixtures
from tools.build_formal_profile_adapter import build_formal_profile_adapter


class TaskLocalProfileTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SolverProfileTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        source = self.fixture.adapter
        gate = source / "runtime_gate.py"
        gate.write_text(gate.read_text() + """
def shell_probe_base(plan):
    return []
def shell_probe_passed(report: dict, plan: dict) -> bool:
        required_denied = {'/unrelated-historical-target'}
        public = observed_public = set()
        specs = []
        base = shell_probe_base(plan)
        denied = {entry['path'] for entry in specs if entry.get('expected_readable') is False}
        return (report.get('nature') == 'no_inference_existing_target_boundary_probes'
                and observed_public == public and required_denied <= denied)
""")
        prepare = source / "prepare_diagnostic.py"
        prepare.write_text(prepare.read_text() + """
def probe(plan):
    targets = [('public', '/neutral/frozen-public', 'file', True)]
    for name, path, kind, expected in targets:
        pass
    return {'nature': 'no_inference_existing_target_boundary_probes'}
""")
        self.source = evidence_ref(prepare, "neutral approved source")
        self.source["files"] = {name: evidence_ref(source / name, "neutral member")["sha256"]
                                for name in self.fixture.original_adapter["files"]}
        self.original = {name: (source / name).read_bytes()
                         for name in ["prepare_diagnostic.py", *self.source["files"]]}
        self.adapter = build_formal_profile_adapter(
            adapter_ref=self.source, snapshot_ref=self.fixture.new["config"]["batch_snapshot"],
            destination=self.root / "local-adapter", probe_scope="task_local_v1")
        packet = read_json(self.fixture.packet)
        original_factory = read_json(Path(packet["source_factory"]["path"]))
        original_factory["technical"]["formal_config_base"]["adapter"] = self.source
        old_path = self.root / "source-factory-local.json"
        write_json(old_path, original_factory)
        factory = read_json(Path(packet["target_factory"]["path"]))
        factory["technical"]["formal_config_base"]["adapter"] = self.adapter
        new_path = self.root / "target-factory-local.json"
        write_json(new_path, factory)
        packet.update(source_factory=evidence_ref(old_path, "neutral source"),
                      target_factory=evidence_ref(new_path, "neutral target"),
                      probe_scope="task_local_v1")
        self.packet = self.root / "local-profile.json"
        write_json(self.packet, packet)

    def proof(self):
        return evidence_ref(self.packet, "neutral authorized local profile")

    def test_exact_generated_profile_passes_guard_without_changing_original(self):
        profile = validated_profile(self.proof())
        self.assertEqual("task_local_v1", profile["migration"]["probe_scope"])
        for name, original in self.original.items():
            self.assertEqual(original, (self.fixture.adapter / name).read_bytes())
        gate = runpy.run_path(str(self.root / "local-adapter/runtime_gate.py"))
        plan = {"cwd": "/neutral/work", "private_directory": "/neutral/private",
                "root": "/neutral/root", "public_tree": {"draft/input/data.csv": {}}}
        targets = gate["task_local_probe_targets"](plan)
        self.assertTrue(any(str(row[1]) == "/neutral/work/draft/input/data.csv" for row in targets))
        self.assertFalse(any("unrelated-historical" in str(row[1]) for row in targets))
        self.assertEqual(self.original["execute_diagnostic.py"],
                         (self.root / "local-adapter/execute_diagnostic.py").read_bytes())
        self.assertEqual(self.original["file_evidence.py"],
                         (self.root / "local-adapter/file_evidence.py").read_bytes())
        with self.assertRaises(ValueError):
            gate["task_local_probe_targets"]({**plan, "additional_deny_read_probes": ["/unrelated"]})
        for name in ("/absolute", "../outside"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                gate["task_local_probe_targets"]({**plan, "public_tree": {name: {}}})

    def test_packet_scope_mismatch_unknown_scope_and_tampered_sources_reject(self):
        original = read_json(self.packet)
        for scope in (None, "different"):
            changed = copy.deepcopy(original)
            if scope is None:
                changed.pop("probe_scope")
            else:
                changed["probe_scope"] = scope
            write_json(self.packet, changed)
            with self.assertRaisesRegex(TaskflowError, "probe scope"):
                validated_profile(self.proof())
        write_json(self.packet, original)
        with self.assertRaisesRegex(TaskflowError, "unsupported probe scope"):
            build_formal_profile_adapter(adapter_ref=self.source,
                snapshot_ref=self.fixture.new["config"]["batch_snapshot"],
                destination=self.root / "unsupported", probe_scope="unknown")
        self.assertFalse((self.root / "unsupported").exists())
        gate = self.root / "local-adapter/runtime_gate.py"
        gate.write_text(gate.read_text() + "# changed generated contract\n")
        with self.assertRaisesRegex(TaskflowError, "deterministic profile"):
            validated_profile(self.proof())


if __name__ == "__main__":
    unittest.main()
