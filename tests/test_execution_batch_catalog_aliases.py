"""Synthetic app-server catalogs; no real CLI or model invocation."""

import json
from pathlib import Path
import tempfile
import unittest

from taskflow.execution_batch import CLI_ROLES, check_batch_plan, confirmation_digests
from tests.test_execution_batch import file_ref, make_confirmed_plan, write_json


class CatalogAliasTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.plan, self.path = make_confirmed_plan(self.root)
        self.catalog_path = self.root / "original-catalog.json"
        self.catalog = json.loads(self.catalog_path.read_bytes())
        for model in self.catalog["models"]:
            model["supportedReasoningEfforts"] = [
                {"reasoningEffort": effort} for effort in model.pop("efforts")
            ]
            model["defaultReasoningEffort"] = model.pop("default_effort")
        for role in CLI_ROLES:
            runtime = self.plan["roles"][role]["runtime"]
            runtime["reasoning_context"] = None
            support_path = self.root / (role + "-support.json")
            support = json.loads(support_path.read_bytes())
            parent = support["model_pointer"].rsplit("/", 1)[0]
            support["efforts_pointer"] = parent + "/supportedReasoningEfforts"
            support["default_effort_pointer"] = parent + "/defaultReasoningEffort"
            write_json(support_path, support)

    def check(self):
        write_json(self.catalog_path, self.catalog)
        for role in CLI_ROLES:
            support_path = self.root / (role + "-support.json")
            support = json.loads(support_path.read_bytes())
            support["output"] = file_ref(self.catalog_path)
            write_json(support_path, support)
            self.plan["roles"][role]["runtime"]["support_evidence"] = file_ref(support_path)
        self.plan["user_model_confirmation"].update(confirmation_digests(self.plan))
        write_json(self.path, self.plan)
        return check_batch_plan(self.path)

    def test_explicit_app_server_effort_objects(self):
        self.assertTrue(self.check()["ready"])

    def test_verified_default_and_frozen_round_trip(self):
        from taskflow.execution_batch import freeze_batch_plan, load_batch_snapshot

        for role in CLI_ROLES:
            self.plan["roles"][role]["runtime"].update(
                reasoning_selection="verified_client_default", reasoning_effort="minimal"
            )
        self.assertTrue(self.check()["ready"])
        frozen = freeze_batch_plan(self.path, self.root / "frozen")
        self.assertEqual("neutral-batch", load_batch_snapshot(frozen["snapshot"])["batch_id"])

    def test_conflicting_or_malformed_effort_aliases_are_rejected(self):
        for item in ({"effort": "minimal", "reasoningEffort": "deep"},
                     {"reasoningEffort": ["deep"]}, {"description": "deep"}):
            with self.subTest(item=item):
                self.catalog["models"][0]["supportedReasoningEfforts"] = [item]
                self.assertFalse(self.check()["ready"])

    def test_default_mismatch_is_rejected(self):
        self.plan["roles"]["author"]["runtime"]["reasoning_selection"] = "verified_client_default"
        self.assertFalse(self.check()["ready"])

    def test_aliases_do_not_allow_cross_model_capabilities(self):
        support_path = self.root / "author-support.json"
        support = json.loads(support_path.read_bytes())
        support["efforts_pointer"] = "/models/1/supportedReasoningEfforts"
        write_json(support_path, support)
        self.assertFalse(self.check()["ready"])
