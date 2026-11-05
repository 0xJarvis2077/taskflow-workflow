"""Only confirmed CLI metadata changes in a new immutable formal adapter."""

import copy
import json
from pathlib import Path
import shutil
import unittest

from taskflow.contracts import TaskflowError, canonical, read_json, write_json
from taskflow.execution_cli_baseline import refreshed_cli_baseline
from taskflow.execution_runtime_support import _frozen
from taskflow.execution_solver_profile import validated_profile
from taskflow.execution_store import evidence_ref
from tests import test_execution_runtime_support as fixture
from tools.build_formal_profile_adapter import build_formal_profile_adapter, rendered_profile_sources


class CliBaselineTests(unittest.TestCase):
    def setUp(self):
        self.host = fixture.RuntimeSupportTests()
        self.host.setUp()
        self.addCleanup(self.host.doCleanups)
        h = self.host
        self.root = h.case.root
        old_runtime = h.before["roles"]["solver"]["runtime"]
        proof = _frozen(h.old["config"]["batch_snapshot"], h.before, old_runtime["support_evidence"])
        source = self.root / "historical-adapter-with-cli"
        shutil.copytree(h.case.adapter, source)
        baseline = read_json(source / "baseline-plan.json")
        baseline["argv"][0] = old_runtime["cli"]
        baseline["observed_cli"] = {"path": old_runtime["cli"], "sha256": proof["cli"]["sha256"],
                                    "version": {"stdout": "Neutral historical fixture version"}}
        baseline["fixed_capabilities"] = {"scope": "unchanged neutral permissions and tools"}
        write_json(source / "baseline-plan.json", baseline)
        self.source_ref = {**evidence_ref(source / "prepare_diagnostic.py", "approved neutral historical source"),
                           "files": {name: evidence_ref(source / name, "neutral")["sha256"] for name in
                                     ("baseline-plan.json", "runtime_gate.py", "execute_diagnostic.py", "file_evidence.py")}}
        self.snapshot = h.new["config"]["batch_snapshot"]
        self.refresh = h.profile["runtime_support_refresh"]
        self.baseline = baseline

    def test_new_adapter_changes_only_cli_metadata_and_retains_exact_historical_observation(self):
        before, _ = rendered_profile_sources(adapter_ref=self.source_ref, snapshot_ref=self.snapshot)
        after, _ = rendered_profile_sources(adapter_ref=self.source_ref, snapshot_ref=self.snapshot,
                                             runtime_support_refresh=self.refresh)
        for name in set(before) - {"baseline-plan.json"}:
            self.assertEqual(before[name], after[name])
        a, z = json.loads(before["baseline-plan.json"]), json.loads(after["baseline-plan.json"])
        self.assertEqual(a["observed_cli"], z["historical_observed_cli"])
        self.assertEqual(a, {**{key: value for key, value in z.items() if key not in
                             {"historical_observed_cli", "runtime_support_refresh"}}, "observed_cli": a["observed_cli"]})
        self.assertNotIn("version", z["observed_cli"])
        self.assertEqual(self.refresh, z["runtime_support_refresh"])
        self.assertNotEqual(a["observed_cli"]["sha256"], z["observed_cli"]["sha256"])
        original = Path(self.source_ref["path"]).parent / "baseline-plan.json"
        self.assertEqual(self.baseline, read_json(original))
        destination = self.root / "refreshed-adapter"
        adapter = build_formal_profile_adapter(adapter_ref=self.source_ref, snapshot_ref=self.snapshot,
                    runtime_support_refresh=self.refresh, destination=destination)
        self.assertEqual(after["baseline-plan.json"], (destination / "baseline-plan.json").read_bytes())
        self.assertEqual(self.refresh, read_json(destination / "profile-migration.json")["runtime_support_refresh"])
        self.assertEqual(0, read_json(destination / "profile-migration.json")["model_calls"])
        self.assertFalse(read_json(destination / "profile-migration.json")["runtime_prepared"])

    def test_pinned_historical_executable_basename_is_preserved_not_reinterpreted(self):
        baseline = copy.deepcopy(self.baseline)
        baseline["argv"][0] = Path(baseline["observed_cli"]["path"]).name
        after = refreshed_cli_baseline(baseline, refresh=self.refresh, snapshot=self.snapshot)
        self.assertEqual(baseline["argv"], after["argv"])
        self.assertEqual(baseline["observed_cli"]["path"], after["observed_cli"]["path"])

    def test_full_profile_accepts_only_the_same_received_refresh_provenance(self):
        h = self.host
        adapter = build_formal_profile_adapter(adapter_ref=self.source_ref, snapshot_ref=self.snapshot,
                    runtime_support_refresh=self.refresh, destination=self.root / "profile-refreshed-adapter")
        source_factory = copy.deepcopy(h.case.old_factory)
        source_factory["technical"]["formal_config_base"]["adapter"] = self.source_ref
        source_path = self.root / "historical-factory.json"
        write_json(source_path, source_factory)
        target = read_json(Path(h.profile["target_factory"]["path"]))
        target["technical"]["formal_config_base"]["adapter"] = adapter
        target_path = self.root / "target-factory-refreshed-cli.json"
        write_json(target_path, target)
        packet = {**h.profile, "source_factory": evidence_ref(source_path, "approved neutral original factory"),
                  "target_factory": evidence_ref(target_path, "shared parameterized experiment preparation")}
        path = self.root / "current-cli-profile.json"
        write_json(path, packet)
        result = validated_profile(evidence_ref(path, "same profile plus confirmed current CLI baseline"))
        self.assertEqual(adapter, result["factory"]["technical"]["formal_config_base"]["adapter"])
        migration_path = Path(adapter["path"]).parent / "profile-migration.json"
        migration = read_json(migration_path)
        write_json(migration_path, {**migration, "runtime_support_refresh": h.profile["source_policy_record"]})
        with self.assertRaisesRegex(TaskflowError, "CLI refresh differs"):
            validated_profile(evidence_ref(path, "different received CLI support provenance"))
        write_json(migration_path, migration)
        packet.pop("runtime_support_refresh")
        write_json(path, packet)
        with self.assertRaises(TaskflowError):
            validated_profile(evidence_ref(path, "missing received CLI support provenance"))

    def test_wrong_snapshot_history_or_double_refresh_never_changes_the_baseline(self):
        original = copy.deepcopy(self.baseline)
        for change in ({"observed_cli": {**self.baseline["observed_cli"], "sha256": "f"*64}},
                       {"argv": ["/another-cli", "exec", "-m", "neutral"]},
                       {"historical_observed_cli": {"already": True}}):
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                refreshed_cli_baseline({**self.baseline, **change}, refresh=self.refresh, snapshot=self.snapshot)
        with self.assertRaisesRegex(TaskflowError, "another confirmed snapshot"):
            refreshed_cli_baseline(self.baseline, refresh=self.refresh, snapshot=self.host.old["config"]["batch_snapshot"])
        self.assertEqual(original, self.baseline)

    def test_confirmation_sources_must_be_actual_policy_registration_records(self):
        refresh = read_json(Path(self.refresh["path"]))
        record = read_json(Path(refresh["source_policy_record"]["path"]))
        record["operation"] = "enqueue"
        path = self.root / "wrong-source-record.json"
        write_json(path, record)
        refresh["source_policy_record"] = evidence_ref(path, "wrong source operation")
        path = self.root / "wrong-refresh.json"
        write_json(path, refresh)
        with self.assertRaisesRegex(TaskflowError, "policy registration sources"):
            refreshed_cli_baseline(self.baseline, refresh=evidence_ref(path, "invalid refresh source type"), snapshot=self.snapshot)

    def test_invalid_support_bytes_and_existing_destination_are_not_overwritten(self):
        updated = self.root / "already-present"
        updated.mkdir();(updated / "original.txt").write_text("retain")
        with self.assertRaises(TaskflowError):
            build_formal_profile_adapter(adapter_ref=self.source_ref, snapshot_ref=self.snapshot,
                runtime_support_refresh=self.refresh, destination=updated)
        self.assertEqual("retain", (updated / "original.txt").read_text())
        source = Path(self.source_ref["path"]).parent / "baseline-plan.json"
        source.write_text(source.read_text() + " ")
        with self.assertRaisesRegex(TaskflowError, "original source changed"):
            rendered_profile_sources(adapter_ref=self.source_ref, snapshot_ref=self.snapshot, runtime_support_refresh=self.refresh)
