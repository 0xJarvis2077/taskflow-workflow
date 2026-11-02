"""Pure registration receives one prepared original without running a model.

Native preparation is converted at the existing explicit fixture enqueue seam.
It does not certify a real native permission profile or external model identity.
"""

import copy
from contextlib import contextmanager
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_author import register_prepared_author_lifecycle, start_author_from_packet
from tests import test_execution_author_factory as fixtures
from tests import test_execution_author as source_fixtures


class AuthorRegistrationTests(unittest.TestCase):
    def prepared(self, *, observed=False, other_attempt=False, without_reference_locations=False):
        case = fixtures.AuthorFactoryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        if other_attempt:
            source_fixtures.AuthorSuccessorTests.prepare_source(case, "research_ended")
            case.attempt = execution.run_worker(case.store, "other-original-author")
        if observed:
            case.store.observe("neutral", case.refs[0], "received", "fixture-controller",
                               "2026-01-01T00:00:00Z", {})
        case.factory()
        if without_reference_locations:
            for key in ("template_spec_ref", "role_template_ref"):
                case.config[key] = {k: v for k, v in case.config[key].items() if k != "location"}
        with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue):
            result = start_author_from_packet(case.store, **case.kwargs(False))
        return case, result

    def register(self, case, attempt_id, config=None):
        return register_prepared_author_lifecycle(case.store, directory=case.directory,
            factory_config=config or case.config, attempt_id=attempt_id)

    @contextmanager
    def edited(self, path, transform):
        original, mode = path.read_bytes(), path.stat().st_mode & 0o777
        value = read_json(path)
        transform(value)
        path.chmod(0o600)
        write_json(path, value)
        try:
            yield
        finally:
            path.write_bytes(original)
            path.chmod(mode)

    def test_pure_registration_is_idempotent_and_does_not_prepare_or_consume(self):
        case, prepared = self.prepared()
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        paths = [case.directory / name for name in ["factory-intent.json", "spec.json", "runtime/request.md"]]
        original_bytes = {str(p): p.read_bytes() for p in paths}
        with patch("taskflow.execution_author.start_author_from_packet", side_effect=AssertionError("no factory reentry")), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("no dispatch")), \
             patch("taskflow.execution_author.enqueue_spec", side_effect=AssertionError("no preparation")), \
             patch("taskflow.execution_author._factory_owner", side_effect=AssertionError("no future-owner restamp")), \
             patch("taskflow.execution_lifecycle.run_lifecycle", side_effect=AssertionError("no consume")):
            flow = self.register(case, attempt["attempt_id"])
            registration = (case.directory / "lifecycle-registration.json").read_bytes()
            before = case.store.events()
            self.assertEqual(flow, self.register(case, attempt["attempt_id"]))
            self.assertEqual(before, case.store.events())
            self.assertEqual(registration, (case.directory / "lifecycle-registration.json").read_bytes())
        self.assertEqual(attempt, case.store.get_attempt(attempt["attempt_id"]))
        self.assertEqual(original_bytes, {str(p): p.read_bytes() for p in paths})
        self.assertEqual(attempt["attempt_id"], flow["definition"]["source_author_attempt"])
        self.assertEqual("active", flow["state"]["status"])
        self.assertFalse((case.directory / "runtime/dispatch.json").exists())

    def test_future_dispatch_owner_transfer_preserves_original_attempt_registration(self):
        case, prepared = self.prepared(observed=True)
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        case.store.transfer_owner("neutral", "fixture-controller", "next-controller", case.refs)
        before = case.store.get_attempt(attempt["attempt_id"])
        with patch.object(case.store, "dispatch_ownership_snapshot", side_effect=AssertionError("do not require a current future owner")):
            flow = self.register(case, attempt["attempt_id"])
        self.assertEqual("fixture-controller", flow["definition"]["owner"])
        self.assertEqual(before, case.store.get_attempt(attempt["attempt_id"]))
        self.assertEqual("next-controller", case.store.dispatch_ownership_snapshot("neutral")["ownership"]["owner"])

    def test_original_reference_without_location_remains_a_legal_factory(self):
        case, prepared = self.prepared(without_reference_locations=True)
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        original = (case.directory / "runtime/request.md").read_bytes()
        flow = self.register(case, attempt["attempt_id"])
        self.assertEqual(attempt["attempt_id"], flow["definition"]["source_author_attempt"])
        self.assertEqual(original, (case.directory / "runtime/request.md").read_bytes())

    def test_missing_or_foreign_actual_attempt_never_creates_a_lifecycle(self):
        case, prepared = self.prepared(other_attempt=True)
        original = case.store.get_attempt(case.attempt["attempt_id"])
        with self.assertRaises(TaskflowError):
            self.register(case, "unknown-attempt")
        actual = case.store.claim("original-neutral-author", [prepared["work_id"]])
        with self.assertRaisesRegex(TaskflowError, "exact original"):
            self.register(case, original["attempt_id"])
        self.assertEqual([], case.store.list_flows())
        self.assertEqual(actual, case.store.get_attempt(actual["attempt_id"]))
        self.assertFalse((case.directory / "lifecycle-spec.json").exists())

    def test_changed_factory_spec_request_or_source_cannot_register_the_original(self):
        case, prepared = self.prepared()
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        for path, change in [
            (case.directory / "factory-intent.json", lambda v: v.update(intent_id="another-intent")),
            (case.directory / "spec.json", lambda v: v["work"].update(purpose="another purpose")),
            (case.directory / "spec.json", lambda v: v["runner"]["request_sections"][0].update(text="changed request")),
            (case.directory / "spec.json", lambda v: v["runner"].update(model="another-model")),
            (case.packet, lambda v: v["items"][0].update(direction="another direction")),
        ]:
            with self.subTest(path=path, change=change), self.edited(path, change):
                before = case.store.events()
                with self.assertRaises(TaskflowError):
                    self.register(case, attempt["attempt_id"])
                self.assertEqual(before, case.store.events())
                self.assertEqual([], case.store.list_flows())
        changed = {**copy.deepcopy(case.config), "lifecycle_root": str(case.root / "foreign-lifecycle")}
        with self.assertRaisesRegex(TaskflowError, "configuration"):
            self.register(case, attempt["attempt_id"], changed)

    def test_existing_registration_receipt_cannot_be_rebound(self):
        case, prepared = self.prepared()
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        flow = self.register(case, attempt["attempt_id"])
        path = case.directory / "lifecycle-registration.json"
        with self.edited(path, lambda v: v.update(source_attempt_id="another-attempt")):
            before = case.store.events()
            with self.assertRaisesRegex(TaskflowError, "different original"):
                self.register(case, attempt["attempt_id"])
            self.assertEqual(before, case.store.events())
            self.assertEqual(flow, case.store.get_flow(flow["flow_id"]))


if __name__ == "__main__":
    unittest.main()
