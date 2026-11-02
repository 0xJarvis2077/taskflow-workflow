"""Registration's confirmed policy and native configuration binding only.

Native manifest validation is an explicit metadata boundary double in the
digest unit case; this does not certify a native profile or external model.
Other request/source/queued-work reads and the batch evidence are actual.
"""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256
from taskflow.execution_author import (
    _validated_prepared_author, register_prepared_author_lifecycle, start_author_from_packet,
)
from taskflow.execution_runner import _validate
from tests import test_execution_author_registration as registrations
from tests import test_execution_batch_author as batches


class AuthorRegistrationBindingTests(unittest.TestCase):
    def test_confirmed_batch_registers_its_actual_author_slot_without_dispatch(self):
        case = batches.BatchAuthorTests.author(self)
        with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue):
            prepared = start_author_from_packet(case.store, **case.kwargs(False))
        attempt = case.store.claim("confirmed-original-author", [prepared["work_id"]])
        flow = register_prepared_author_lifecycle(case.store, directory=case.directory,
            factory_config=case.config, attempt_id=attempt["attempt_id"])
        self.assertEqual("author", attempt["model_slot"])
        self.assertEqual("batch-v1", flow["definition"]["policy_id"])
        self.assertEqual(attempt["attempt_id"], flow["definition"]["source_author_attempt"])
        self.assertEqual(attempt, case.store.get_attempt(attempt["attempt_id"]))
        self.assertFalse((case.directory / "runtime/dispatch.json").exists())

    def test_native_configuration_digest_rejects_permission_or_digest_drift(self):
        case, prepared = registrations.AuthorRegistrationTests.prepared(self)
        runtime = Path(prepared["runtime"])
        spec = read_json(case.directory / "spec.json")
        native_view = copy.deepcopy(_validate(runtime))
        config = copy.deepcopy(spec["runner"])
        config["candidate_binding"] = native_view["candidate_binding"]
        native_view.update(mode="native_role", preparation_config_digest=sha256(canonical(config)))
        before = case.store.events()
        with patch("taskflow.execution_runner._validate", return_value=native_view):
            queued, candidate, actual = _validated_prepared_author(case.store, spec, runtime)
            self.assertEqual(prepared["work_id"], queued["work_id"])
            self.assertEqual(candidate["candidate_id"], actual["candidate_binding"]["candidate_id"])
            changed = copy.deepcopy(spec)
            changed["runner"]["native_permissions"]["profile"] = "another-profile"
            with self.assertRaisesRegex(TaskflowError, "complete published specification"):
                _validated_prepared_author(case.store, changed, runtime)
            native_view["preparation_config_digest"] = "0" * 64
            with self.assertRaisesRegex(TaskflowError, "complete published specification"):
                _validated_prepared_author(case.store, spec, runtime)
        self.assertEqual(before, case.store.events())


if __name__ == "__main__":
    unittest.main()
