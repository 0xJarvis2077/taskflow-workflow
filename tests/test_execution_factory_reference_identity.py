"""Original factory identity through the ordinary no-model CLI producer."""

import copy
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, write_json
from taskflow.execution import main
from taskflow.execution_solver_profile import source_policy_id, validated_profile
from taskflow.execution_store import evidence_ref, same_original_ref
from tests import test_execution_solver_profile as profile_tests


class FactoryReferenceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = profile_tests.SolverProfileTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_retired_factory_cli_cannot_register_or_launch_an_experiment(self):
        f = self.fixture
        before = f.store.list_flows()
        with patch("taskflow.execution_experiment_factory.register_lifecycle", side_effect=AssertionError("retired CLI")), patch(
                "taskflow.execution_experiment_factory.launch_lifecycle_consumer", side_effect=AssertionError("no consumer")), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                main(["--db", str(f.root / "state.sqlite3"), "batch-experiment-start"])
        self.assertEqual(before, f.store.list_flows())
        self.assertFalse((f.root / "formal").exists())

    def test_identity_retains_declared_metadata_and_checks_sources(self):
        f = self.fixture
        original = evidence_ref(f.packet, "original location")
        before = copy.deepcopy(original)
        self.assertTrue(same_original_ref(original, {**original, "location": "other location"}))
        self.assertEqual(before, original)
        for extra in ({"description": "extra"}, {"authority": "extra"}, {"size": f.packet.stat().st_size}):
            with self.subTest(extra=extra):
                self.assertFalse(same_original_ref(original, {**original, **extra}))
        sized = {**original, "size": f.packet.stat().st_size}
        self.assertTrue(same_original_ref(sized, {**sized, "location": "other"}))
        for size in (True, -1, 0, "100"):
            with self.subTest(size=size), self.assertRaises(TaskflowError):
                same_original_ref(original, {**original, "size": size})
        other = f.root / "same-bytes.json"
        other.write_bytes(f.packet.read_bytes())
        self.assertFalse(same_original_ref(original, evidence_ref(other, "same bytes different path")))
        with self.assertRaises(TaskflowError):
            same_original_ref(original, {**original, "sha256": "0" * 64})
        other.write_bytes(b"changed")
        with self.assertRaises(TaskflowError):
            same_original_ref(original, {**original, "path": str(other)})
        with self.assertRaises(TaskflowError):
            same_original_ref(original, {**original, "location": ""})

    def test_source_policy_rejects_changed_original_and_extra_authority(self):
        f = self.fixture
        profile = validated_profile(f.ref)
        reference = profile["packet"]["target_factory"]
        definition = {"task_id": "neutral-task", "policy_id": "updated-policy",
            "factory_config_ref": {**reference, "location": "CLI attribution"},
            "technical": copy.deepcopy(profile["factory"]["technical"]), "solver_profile_update": f.ref}
        self.assertEqual("original-policy", source_policy_id(f.store, definition))
        other = f.root / "copied-factory.json"
        other.write_bytes(Path(reference["path"]).read_bytes())
        for replacement in ({**reference, "authority": "extra"}, evidence_ref(other, "copy")):
            with self.subTest(replacement=replacement), self.assertRaisesRegex(TaskflowError, "experiment differs"):
                source_policy_id(f.store, {**definition, "factory_config_ref": replacement})
        changed = copy.deepcopy(definition)
        changed["technical"]["formal_config_base"]["adapter"]["location"] = "changed adapter binding"
        with self.assertRaisesRegex(TaskflowError, "experiment adapter differs"):
            source_policy_id(f.store, changed)

    def test_non_location_metadata_types_remain_exact(self):
        original = evidence_ref(self.fixture.packet, "source")
        for before, after in ((True, 1), (1, 1.0), ({"nested": [True]}, {"nested": [1]})):
            with self.subTest(before=before, after=after):
                self.assertFalse(same_original_ref({**original, "authority": before},
                                                  {**original, "authority": after, "location": "CLI"}))
