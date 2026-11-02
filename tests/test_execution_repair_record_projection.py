"""Explicit declaration-copy projections keep raw repair evidence and fresh rechecks."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_flow import advance_workflow, resume_workflow
from taskflow.execution_flow_contracts import resolve_repair_record_projection, validate_repair
from taskflow.execution_store import evidence_ref
from tests import test_execution_flow as fixture


ROLE_FIXTURE_WITH_DECLARATION_COPY = fixture.ROLE_FIXTURE.replace(
    " save(root/'task.json',task)",
    " shutil.copyfile(inputs/'execution/task.json',root/'task.json')").replace(
    "'after_sha256':digest(root/changed)}]",
    "'after_sha256':digest(root/changed)},{'path':'task.json','before_sha256':None,"
    "'after_sha256':digest(root/'task.json'),'change_type':'added','reason':'Materialize the original declaration unchanged.'}]")


class RepairRecordProjectionTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.ContinuationTests("test_one_registration_runs_quality_actual_repair_recheck_receipt")
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root, self.store = self.case.root, self.case.store

    def prepare_projection_exception(self, *, alter_declaration=False, duplicate_metadata=False):
        script = ROLE_FIXTURE_WITH_DECLARATION_COPY
        if alter_declaration:
            script = script.replace(" changed='draft/evaluation/answer.md'",
                                    " task['title']='Changed delivered declaration';save(root/'task.json',task)\n changed='draft/evaluation/answer.md'")
        if duplicate_metadata:
            script = script.replace(" pathlib.Path('handoff.md')", " repair=json.loads(pathlib.Path('repair-record.json').read_text());repair['changes'].append(dict(repair['changes'][-1]));save('repair-record.json',repair)\n pathlib.Path('handoff.md')")
        self.case.script.write_text(script)
        self.definition = self.case.definition()
        self.case.register(self.definition)
        self.failed = self.case.run_flow([self.definition["flow_id"]])["flows"][0]
        self.assertEqual("exception", self.failed["state"]["status"])
        self.assertEqual("repair", self.failed["state"]["stage"])
        self.assertEqual("Repair changes duplicate or do not reflect actual differences",
                         self.failed["state"]["exception"]["reason"])
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(2, len(attempts))
        self.attempt = next(row for row in attempts if row["role"] == "repair")
        self.runtime = Path(self.attempt["runtime_path"])
        self.original = self.runtime / "artifacts/repair-record.json"
        self.raw_record = read_json(self.original)
        self.parent = self.store.get_candidate(self.raw_record["parent_id"])
        self.candidate = self.store.get_candidate(self.raw_record["candidate_id"])
        self.projected = self.root / "technical-projection/repair-record.payload-only.json"
        value = copy.deepcopy(self.raw_record)
        value["changes"] = [row for row in value["changes"] if row["path"] != "task.json"]
        write_json(self.projected, value)
        authority = self.root / "projection-authority.json"
        write_json(authority, {"nature": "neutral explicit declaration-copy technical projection", "fresh_recheck_required": True})
        self.refs = [evidence_ref(authority, "authorized exact projection; preserve the original and fresh recheck")]
        return self.failed

    def pair(self):
        return {"original": evidence_ref(self.original, "sealed actual repair author record"),
                "projected": evidence_ref(self.projected, "derived record excluding only unchanged task.json materialization")}

    def resume(self, **changes):
        return resume_workflow(self.store, self.definition["flow_id"], reason="Explicit neutral declaration-copy projection",
                               evidence=self.refs, repair_record_projection=changes.get("projection", self.pair()))

    def test_explicit_projection_reuses_original_attempt_and_requires_fresh_recheck(self):
        self.prepare_projection_exception()
        preserved = {path: path.read_bytes() for path in
                     [self.original, self.runtime / "manifest.json", self.runtime / "request.md",
                      self.runtime / "collection.json", self.runtime / "trace.jsonl",
                      Path(self.failed["state"]["receipt"]) ]}
        original_attempt = self.store.get_attempt(self.attempt["attempt_id"])
        with self.assertRaises(TaskflowError):
            validate_repair(self.original, parent=self.parent, candidate=self.candidate,
                            defect_ids=self.raw_record["defect_ids"])
        resumed = self.resume()
        self.assertEqual(resumed, self.resume())
        proof = resumed["state"]["repair_record_projections"][self.attempt["attempt_id"]]
        self.assertEqual(self.pair()["original"], proof["original"])
        self.assertEqual(self.pair()["projected"], proof["projected"])
        self.assertEqual(1, len(proof["validation"]["changes"]))
        self.assertEqual(proof["declaration_input"]["sha256"], proof["declaration_delivered"]["sha256"])
        locked = next(row for row in read_json(self.runtime / "manifest.json")["inputs"]
                      if row["target"] == "execution/task.json")
        self.assertEqual(locked["sha256"], proof["declaration_input"]["sha256"])
        received = advance_workflow(self.store, self.definition["flow_id"])
        self.assertEqual("recheck", received["state"]["stage"])
        self.assertEqual("active", received["state"]["status"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))
        final = self.case.run_flow([self.definition["flow_id"]])["flows"][0]
        self.assertEqual("accepted", final["state"]["status"], final["state"])
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(["quality", "repair", "quality"], [row["role"] for row in attempts])
        self.assertEqual(3, len({row["cli_sid"] for row in attempts}))
        self.assertEqual(original_attempt, self.store.get_attempt(self.attempt["attempt_id"]))
        defect = self.store.get_defect(final["state"]["repaired_defects"][0])
        self.assertEqual("closed", defect["state"])
        derived = [ref for ref in defect["repair_evidence"] if ref.get("kind") == "repair_record_projection"]
        self.assertEqual([self.pair()], [ref["repair_record_projection"] for ref in derived])
        self.assertEqual(str(self.projected), derived[0]["path"])
        for path, raw in preserved.items():
            self.assertEqual(raw, path.read_bytes())

    def test_without_explicit_projection_the_original_strict_error_remains(self):
        self.prepare_projection_exception()
        resume_workflow(self.store, self.definition["flow_id"], reason="Ordinary retry of consumption",
                        evidence=self.refs)
        failed = self.case.run_flow([self.definition["flow_id"]])["flows"][0]
        self.assertEqual("exception", failed["state"]["status"])
        self.assertEqual(self.failed["state"]["exception"], failed["state"]["exception"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))

    def test_projection_cannot_drop_payload_rewrite_fields_or_change_roles(self):
        self.prepare_projection_exception()
        base = read_json(self.projected)
        mutations = [lambda value: value.update(needs_business_decision=True),
                     lambda value: value["changes"].clear(),
                     lambda value: value["changes"][0].update(after_sha256="0" * 64),
                     lambda value: value["changes"][0].update(after_role="teacher")]
        for mutation in mutations:
            value = copy.deepcopy(base)
            mutation(value)
            write_json(self.projected, value)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(TaskflowError, "preserve every original field"):
                self.resume()
            self.assertEqual(self.failed["revision"], self.store.get_flow(self.definition["flow_id"])["revision"])

    def test_nonidentical_declaration_or_ambiguous_metadata_is_rejected(self):
        for option, expected in (("alter_declaration", "delivered declaration differs"),
                                 ("duplicate_metadata", "one unique task.json")):
            case = RepairRecordProjectionTests()
            case.setUp()
            try:
                case.prepare_projection_exception(**{option: True})
                with self.subTest(option=option), self.assertRaisesRegex(TaskflowError, expected):
                    case.resume()
            finally:
                case.doCleanups()

    def test_projection_requires_exact_completed_attempt_exception_and_collected_original(self):
        self.prepare_projection_exception()
        unrelated = self.root / "unrelated-original.json"
        write_json(unrelated, self.raw_record)
        pair = {**self.pair(), "original": evidence_ref(unrelated, "unrelated physical copy")}
        with self.assertRaisesRegex(TaskflowError, "exact sealed collected"):
            self.resume(projection=pair)
        for attempt_changes in ({"state": "running"}, {"state": "failed"}, {"role": "quality"}):
            with self.subTest(attempt_changes=attempt_changes), self.assertRaises(TaskflowError):
                resolve_repair_record_projection(self.pair(), attempt={**self.attempt, **attempt_changes},
                    parent=self.parent, candidate=self.candidate, defect_ids=self.raw_record["defect_ids"])
        current = self.store.get_flow(self.definition["flow_id"])
        with patch.object(self.store, "get_flow", return_value={**current, "state": {
            **current["state"], "exception": {"reason": "Other failure"}}}):
            with self.assertRaisesRegex(TaskflowError, "exact completed repair"):
                self.resume()

    def test_consumption_revalidates_projection_bytes_after_resume(self):
        self.prepare_projection_exception()
        self.resume()
        value = read_json(self.projected)
        value["changes"] = []
        write_json(self.projected, value)
        failed = advance_workflow(self.store, self.definition["flow_id"])
        self.assertEqual("exception", failed["state"]["status"])
        self.assertIn("evidence changed", failed["state"]["exception"]["reason"])
        self.assertEqual(2, len(self.store.dashboard()["attempts"]))


if __name__ == "__main__":
    unittest.main()
