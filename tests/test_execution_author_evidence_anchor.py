"""Prepared local files cannot replace the sources anchored by a queued work.

These are neutral real temporary SQLite/queued-request tests. The existing
fixture enqueue substitutes a test CLI for native permissions; no model runs.
"""

import copy
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_author import _validated_prepared_author, register_prepared_author_lifecycle
from tests import test_execution_author_registration as registrations


class PreparedAuthorEvidenceAnchorTests(unittest.TestCase):
    def test_same_byte_foreign_role_template_cannot_rebind_the_queued_original(self):
        case, prepared = registrations.AuthorRegistrationTests.prepared(self)
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        original_work = case.store.get_work(prepared["work_id"])
        runtime = Path(prepared["runtime"])
        locked = {name: (runtime / name).read_bytes() for name in
                  ("manifest.json", "manifest-lock.json", "request.md")}
        foreign = case.root / "same-byte-foreign-role-template.json"
        foreign.write_bytes(Path(case.config["role_template_ref"]["path"]).read_bytes())
        changed = copy.deepcopy(case.config)
        changed["role_template_ref"]["path"] = str(foreign)
        fint = read_json(case.directory / "factory-intent.json")
        fint["factory_config_digest"] = sha256(canonical(changed))
        spec = read_json(case.directory / "spec.json")
        spec["work"]["evidence"][2]["path"] = str(foreign)
        for name, value in (("factory-intent.json", fint), ("spec.json", spec)):
            path = case.directory / name
            path.chmod(0o600)
            write_json(path, value)
        before = case.store.events()
        with self.assertRaisesRegex(TaskflowError, "evidence.*original queued"):
            register_prepared_author_lifecycle(case.store, directory=case.directory,
                factory_config=changed, attempt_id=attempt["attempt_id"])
        self.assertEqual(before, case.store.events())
        self.assertEqual(original_work, case.store.get_work(prepared["work_id"]))
        self.assertEqual(attempt, case.store.get_attempt(attempt["attempt_id"]))
        self.assertEqual([], case.store.list_flows())
        self.assertEqual(locked, {name: (runtime / name).read_bytes() for name in locked})

    def test_evidence_order_type_and_reference_fields_are_not_normalized_away(self):
        case, prepared = registrations.AuthorRegistrationTests.prepared(self)
        runtime = Path(prepared["runtime"])
        original = read_json(case.directory / "spec.json")
        changes = [
            lambda refs: refs.reverse(),
            lambda refs: refs[0].update(location="another claimed provenance"),
            lambda refs: refs[0].update(size=True),
            lambda refs: refs.pop(),
        ]
        for change in changes:
            spec = copy.deepcopy(original)
            change(spec["work"]["evidence"])
            with self.subTest(change=change), self.assertRaisesRegex(TaskflowError, "evidence.*original queued"):
                _validated_prepared_author(case.store, spec, runtime)
        for value in (None, {}, "not references"):
            spec = copy.deepcopy(original)
            spec["work"]["evidence"] = value
            with self.subTest(value=value), self.assertRaisesRegex(TaskflowError, "evidence.*original queued"):
                _validated_prepared_author(case.store, spec, runtime)

    def test_original_no_location_factory_still_uses_its_queued_normalized_refs(self):
        case, prepared = registrations.AuthorRegistrationTests.prepared(self, without_reference_locations=True)
        attempt = case.store.claim("original-neutral-author", [prepared["work_id"]])
        request = Path(prepared["runtime"], "request.md").read_bytes()
        flow = register_prepared_author_lifecycle(case.store, directory=case.directory,
            factory_config=case.config, attempt_id=attempt["attempt_id"])
        self.assertEqual(attempt["attempt_id"], flow["definition"]["source_author_attempt"])
        self.assertEqual(request, Path(prepared["runtime"], "request.md").read_bytes())

    def test_original_os_parent_alias_keeps_the_store_evidence_contract(self):
        case, prepared = registrations.AuthorRegistrationTests.prepared(self)
        spec = read_json(case.directory / "spec.json")
        changed = False
        for entry in spec["work"]["evidence"]:
            for prefix in ("/private/tmp/", "/private/var/"):
                if entry["path"].startswith(prefix):
                    entry["path"] = entry["path"][len("/private"):]
                    changed = True
                    break
        if not changed:
            self.skipTest("host has no supported OS /tmp or /var parent alias")
        queued, _, _ = _validated_prepared_author(case.store, spec, Path(prepared["runtime"]))
        self.assertEqual(prepared["work_id"], queued["work_id"])


if __name__ == "__main__":
    unittest.main()
