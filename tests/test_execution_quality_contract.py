"""Finite structural matrix, real local preflight and frozen-runtime boundaries."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from taskflow import execution_quality_progress_contract as contract
from taskflow.execution_quality_session import _output_contract
from taskflow.execution_runner import _info
from tests import test_execution_preparation as preparation_tests


class QualityOutputContractTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.binding = {"task_id": "neutral-task", "candidate_id": "neutral-candidate", "candidate_digest": "a" * 64}
        self.manifest = {"intent_id": "neutral-intent", "candidate_binding": self.binding, "cwd": str(self.root),
                         "outputs": [], "quality_session": {"index": 1, "phase": "progress"}}
        self.plan = {"turns": [{"page_ids": []}, {"page_ids": ["page-1"]}],
                     "pages": [{"id": "page-1", "sha256": "b" * 64, "path": "private/page.txt"}]}
        self.context = contract.make_context(self.manifest, self.plan)
        self.result = contract.template(self.context)
        self.result["pages"][0].update(disposition="read", association="actual responsibility", remaining="")
        self.result["pages"][0]["evidence"] = [{"path": "private/page.txt", "location": "actual visible readback"}]

    def test_phase_templates_and_all_dispositions_keep_business_open(self):
        self.assertIsNone(self.result["public_checkpoint"])
        for disposition in contract.DISPOSITIONS:
            result = copy.deepcopy(self.result)
            result["pages"][0].update(disposition=disposition, remaining="actual still open duty")
            contract.validate_progress(result, self.context)
        first = contract.make_context({**self.manifest, "quality_session": {"index": 0, "phase": "progress"}}, self.plan)
        result = contract.template(first)
        self.assertIsInstance(result["public_checkpoint"], dict)
        self.assertEqual([], result["pages"])
        self.assertEqual([], first["assigned_pages"])
        self.assertIn("not null", contract.render(first))
        result["public_checkpoint"] = None
        with self.assertRaises(contract.QualityContractError): contract.validate_progress(result, first)

    def test_finite_top_field_type_matrix(self):
        variants = []
        for key in contract.TOP_FIELDS:
            changed = copy.deepcopy(self.result); del changed[key]; variants.append(changed)
        for key in ("schema_version", "segment_index"):
            for value in (None, True, "1", [], {}):
                changed = copy.deepcopy(self.result); changed[key] = value; variants.append(changed)
        for key in ("facts", "remaining", "pages"):
            for value in (None, True, "text", {}, 1):
                changed = copy.deepcopy(self.result); changed[key] = value; variants.append(changed)
        for key, value in (("kind", "quality_accepted"), ("status", "accepted"), ("intent_id", "foreign"),
                           ("candidate_binding", {}), ("extra", "field"), ("pages", [[]])):
            changed = copy.deepcopy(self.result); changed[key] = value; variants.append(changed)
        for index, changed in enumerate(variants):
            with self.subTest(index=index), self.assertRaises(contract.QualityContractError):
                contract.validate_progress(changed, self.context)

    def test_complete_page_matrix_and_no_incomplete_legacy_alias_in_new_delivery(self):
        variants = []
        for key in contract.PAGE_FIELDS:
            changed = copy.deepcopy(self.result); del changed["pages"][0][key]; variants.append(changed)
        for key, value in (("id", "foreign"), ("sha256", "c" * 64), ("association", ""),
                ("remaining", []), ("remaining", {}), ("disposition", {}), ("disposition", "accepted"),
                ("evidence", []), ("evidence", [{}]), ("evidence", [[{}]]),
                ("evidence", [{"plan_page": "incomplete"}]), ("evidence", [{"body_page": "incomplete"}]),
                ("evidence", [{"path": "p", "location": ""}]), ("extra", True)):
            changed = copy.deepcopy(self.result); changed["pages"][0][key] = value; variants.append(changed)
        changed = copy.deepcopy(self.result); changed["pages"][0]["disposition"] = "unresolved"; variants.append(changed)
        for index, changed in enumerate(variants):
            with self.subTest(index=index), self.assertRaises(contract.QualityContractError):
                contract.validate_progress(changed, self.context)
        legacy = {**self.context, "delivery_mode": "legacy"}
        changed = copy.deepcopy(self.result); changed["pages"][0]["remaining"] = ["preserved original duty"]
        contract.validate_progress(changed, legacy)

    def test_actual_local_cli_preflight_is_read_only_and_never_signs_quality(self):
        path = self.root / "reading-progress-001.json"
        path.write_text(json.dumps(self.result))
        context_path = self.root / "quality-output-contract.json"
        context_path.write_text(json.dumps(self.context))
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        process = subprocess.run([sys.executable, contract.__file__, "--context", str(context_path)], capture_output=True, text=True)
        self.assertEqual(0, process.returncode, process.stderr)
        self.assertFalse(json.loads(process.stdout)["quality_accepted"])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})
        self.result["pages"][0]["remaining"] = {}
        path.write_text(json.dumps(self.result))
        process = subprocess.run([sys.executable, contract.__file__, "--context", str(context_path)], capture_output=True, text=True)
        self.assertEqual(2, process.returncode)
        self.assertIn("pages[0]", process.stderr)
        path.write_text(json.dumps(contract.template(self.context)))
        (self.root / "adoption-readiness.json").write_text("{}")
        with self.assertRaisesRegex(contract.QualityContractError, "must not issue"):
            contract.check_delivery(self.context, self.root)

    def test_first_checkpoint_bytes_and_final_outcomes(self):
        first = contract.make_context({**self.manifest, "quality_session": {"index": 0, "phase": "progress"}}, self.plan)
        result = contract.template(first)
        checkpoint = self.root / "public-responsibility-first.json"; checkpoint.write_text("neutral original")
        result["public_checkpoint"] = {"sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(), "tool_readback_location": "visible readback"}
        (self.root / "reading-progress-000.json").write_text(json.dumps(result))
        self.assertFalse(contract.check_delivery(first, self.root)["quality_accepted"])
        checkpoint.write_text("changed")
        with self.assertRaisesRegex(contract.QualityContractError, "checkpoint file/SHA"):
            contract.check_delivery(first, self.root)
        final = {**self.context, "phase": "final"}
        binding = {"intent_id": self.manifest["intent_id"], **self.binding}
        for outcome in ("accepted", "revision_required", "inconclusive"):
            blocking = ["issue-1"] if outcome == "revision_required" else []
            issues = {"execution_binding": binding, "issues": []}
            if blocking:
                issues["issues"] = [{"id": "issue-1", "kind": "claim", "responsibility": "actual remaining judgment",
                    "affected_paths": ["public.txt"], "evidence": [{"path": "public.txt", "sha256": "d"*64, "location": "line 1"}]}]
            report = {"execution_routing": {**binding, "schema_version": 1, "signer_role": "independent_quality",
                "independent_of_current_author": True, "participated_in_production": False, "scope": "whole_package",
                "outcome": outcome, "blocking_issue_ids": blocking, "covered_defect_ids": [], "reason": "actual judgment"}}
            (self.root / "intake.md").write_text("actual whole review")
            (self.root / "issues.json").write_text(json.dumps(issues))
            (self.root / "adoption-readiness.json").write_text(json.dumps(report))
            self.assertFalse(contract.check_delivery(final, self.root)["quality_accepted"])
            with self.assertRaises(contract.QualityContractError):
                contract.validate_final(report, issues, {**final, "require_native": True})

    def test_bound_helper_and_context_cannot_fall_back_after_tampering(self):
        script = self.root / "execution_quality_progress_contract.py"; script.write_bytes(Path(contract.__file__).read_bytes())
        context_path = self.root / "quality-output-contract.json"; context_path.write_text(json.dumps(self.context))
        manifest = {**self.manifest, "dependencies": {str(p): _info(p) for p in (script, context_path)}}
        frozen, context = _output_contract(self.root, manifest, self.plan)
        frozen.validate_progress(self.result, context)
        context_path.write_text("{}")
        with self.assertRaisesRegex(Exception, "bytes changed"): _output_contract(self.root, manifest, self.plan)
        context_path.unlink()
        with self.assertRaises(Exception): _output_contract(self.root, manifest, self.plan)
        with self.assertRaisesRegex(Exception, "cannot fall back"): _output_contract(self.root, self.manifest, self.plan)

    def test_partial_helper_and_context_originals_are_preserved(self):
        fixture = preparation_tests.PreparationTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        fixture.partial()
        for name in ("execution_quality_progress_contract.py", "quality-output-contract.json"):
            (fixture.runtime / name).write_text("ORIGINAL " + name)
        fixture.run_prepare()
        self.assertEqual(1, len(fixture.archives()))
        for name in ("execution_quality_progress_contract.py", "quality-output-contract.json"):
            self.assertEqual("ORIGINAL " + name, (fixture.archives()[0] / name).read_text())

    def test_native_nonblocking_false_dimension_and_nested_shape_matrix(self):
        from tests.test_execution_native_observation_acceptance import native_observation_fixture
        source = native_observation_fixture(self)
        report = json.loads((source.material / "artifacts/adoption-readiness.json").read_text())
        issues = json.loads((source.material / "artifacts/issues.json").read_text())
        routing = report["execution_routing"]
        context = {"phase": "final", "intent_id": routing["intent_id"],
            "candidate_binding": {key: source.candidate[key] for key in
                ("task_id", "candidate_id", "content_digest", "input_digest", "evaluation_digest")},
            "require_native": True, "native_protocol_sha256": report["native_signoff"]["protocol_sha256"]}
        before = copy.deepcopy(report)
        contract.validate_final(report, issues, context)
        self.assertFalse(report["native_signoff"]["dimensions"]["calculation"]["affirmed"])
        self.assertEqual(before, report)
        mutations = [
            lambda r: r["native_signoff"].update(schema_version=True),
            lambda r: r["native_signoff"].update(protocol_sha256="0"*64),
            lambda r: r["native_signoff"].update(coverage="affected"),
            lambda r: r["native_signoff"]["dimensions"].pop("business"),
            lambda r: r["native_signoff"]["dimensions"]["calculation"].update(affirmed="false"),
            lambda r: r["native_signoff"]["dimensions"]["calculation"].update(evidence=[]),
            lambda r: r["native_signoff"]["dimensions"]["calculation"].update(limitations={}),
            lambda r: r["native_signoff"]["normative"].update(files={}),
            lambda r: r["native_signoff"]["normative"].update(evidence=[]),
            lambda r: r["native_signoff"]["normative"]["files"][0].update(size=True),
            lambda r: r["native_signoff"].update(responsibility_analysis=[]),
            lambda r: r["native_signoff"]["responsibility_analysis"][0].update(public_evidence=[]),
            lambda r: r["native_signoff"]["responsibility_analysis"][0].update(legitimate_branches=[]),
            lambda r: r["execution_routing"].update(outcome="quality_accepted"),
        ]
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(report); mutate(changed)
            with self.subTest(index=index), self.assertRaises(contract.QualityContractError):
                contract.validate_final(changed, issues, context)
