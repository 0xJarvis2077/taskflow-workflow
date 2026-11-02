"""Future informed guidance reaches actual requests without changing blind roles.

Real neutral SQLite/prepare/render boundaries are exercised. Native enqueue and
the isolated CLI catalog are explicit fixture seams, not permission/model proof.
The four scenarios below test transport; they do not sign business quality.
"""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow import execution_author
from taskflow.contracts import TaskflowError, sha256, write_json
from taskflow.execution_author import INFORMED_PUBLIC_MATERIAL_GUIDANCE, _roles, start_author_from_packet
from taskflow.execution_flow import _prepare_node
from taskflow.execution_runner import _validate, prepare_attempt
from taskflow.execution_store import evidence_ref
from tests import test_execution_author_factory as factories
from tests import test_execution_author_factory_reentry as reentries
from tests import test_isolated_quality as public_fixtures


class InformedMaterialGuidanceTests(unittest.TestCase):
    def case(self):
        case = factories.AuthorFactoryTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.factory()
        return case

    def test_future_author_actual_request_contains_the_shared_guidance(self):
        case = self.case()
        with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue), \
             patch("taskflow.execution_author.run_worker", side_effect=AssertionError("no models")):
            prepared = start_author_from_packet(case.store, **case.kwargs(False))
        runtime = Path(prepared["runtime"])
        request = (runtime / "request.md").read_text()
        manifest = _validate(runtime)
        self.assertEqual(1, request.count(INFORMED_PUBLIC_MATERIAL_GUIDANCE))
        self.assertEqual(manifest["request"]["sha256"], sha256(request.encode()))
        self.assertEqual(set(case.required), set(manifest["outputs"]))
        self.assertEqual([], case.store.list_attempts(prepared["work_id"]))
        self.assertNotIn("OLD_Q1", request)

    def test_quality_repair_recheck_real_node_preparation_locks_the_same_operation(self):
        case = self.case()
        common = {"task_id": "neutral", "policy_id": "fixture-v1",
                  "role_template_ref": case.config["role_template_ref"]}
        roles = _roles(case.store, common)
        public = case.root / "actual-public-archive"
        for name in ("work", "scratch"):
            (public / "supplemental/trees/inner" / name).mkdir(parents=True)
        (public / "observed-trace.jsonl").write_text('{"type":"neutral-retained-evidence"}\n')
        for index, stage in enumerate(("quality", "repair", "recheck", "recheck_public")):
            with self.subTest(stage=stage):
                definition = {**common, "flow_id": "future-informed-" + stage, "owner": "fixture-controller",
                              "directory": str(case.root / ("future-" + stage)), "roles": roles}
                state = {"status": "active", "stage": stage, "step": index,
                         "candidate_id": "baseline", "candidate_root": str(case.task),
                         "defects": [{"defect_id": "neutral-original-defect", "responsibility": "Neutral located responsibility"}]
                         if stage == "repair" else [],
                         "repaired_defects": [], "history": [], "public_archive": str(public)}
                record = case.store.register_flow(definition["flow_id"], definition, case.refs, initial_state=state)
                prepared = _prepare_node(case.store, record)
                runtime = Path(prepared["state"]["current_runtime"])
                manifest = _validate(runtime)
                request = (runtime / "request.md").read_text()
                self.assertEqual(1, request.count(INFORMED_PUBLIC_MATERIAL_GUIDANCE))
                self.assertEqual(manifest["request"]["sha256"], sha256(request.encode()))
                self.assertIn("Read the authoritative current package", request)
                self.assertEqual([], case.store.list_attempts(prepared["state"]["current_work_id"]))

    def test_old_queued_and_prepared_only_requests_are_reused_byte_for_byte(self):
        for partial in (False, True):
            with self.subTest(partial=partial):
                old = reentries.AuthorFactoryReentryTests()
                old.setUp()
                self.addCleanup(old.doCleanups)
                if partial:
                    with self.assertRaisesRegex(TaskflowError, "after preparation"):
                        old.original_prompt(after_prepare=True)
                else:
                    old.original_prompt()
                originals = old.locked_files()
                old.reenter()
                self.assertEqual(originals, old.locked_files())
                request = (old.case.directory / "runtime/request.md").read_text()
                self.assertIn(reentries.OLD_REQUEST, request)
                self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, request)

    def test_old_immutable_role_definitions_do_not_gain_new_teacher_text(self):
        case = self.case()
        definition = {"task_id": "neutral", "policy_id": "fixture-v1",
                      "role_template_ref": case.config["role_template_ref"]}
        with patch.object(execution_author, "INFORMED_PUBLIC_MATERIAL_GUIDANCE", "OLD_FROZEN_OPERATION"):
            old_roles = _roles(case.store, definition)
        old_roles["quality"]["predecessor_inputs"] = []
        frozen = case.store.register_flow("old-immutable-role-definition",
            {**definition, "roles": old_roles, "directory": str(case.root / "old-immutable"),
             "owner": "fixture-controller"}, case.refs, initial_state={"status": "active", "stage": "quality",
                "step": 0, "candidate_id": "baseline", "candidate_root": str(case.task),
                "defects": [], "repaired_defects": [], "history": []})
        before = copy.deepcopy(frozen)
        future = _roles(case.store, definition)
        self.assertIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, future["quality"]["runner"]["request_sections"][0]["text"])
        self.assertEqual(before, case.store.get_flow(frozen["flow_id"]))
        self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, str(before["definition"]["roles"]))
        first = _prepare_node(case.store, frozen)
        runtime = Path(first["state"]["current_runtime"])
        paths = [runtime / "request.md", runtime / "manifest.json", runtime.parent / "spec.json"]
        original = {path: path.read_bytes() for path in paths}
        _prepare_node(case.store, first)
        self.assertEqual(original, {path: path.read_bytes() for path in paths})
        request = (runtime / "request.md").read_text()
        self.assertIn("OLD_FROZEN_OPERATION", request)
        self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, request)

    def test_solver_facts_and_actual_isolated_public_request_do_not_get_teacher_guidance(self):
        case = self.case()
        roles = _roles(case.store, {"task_id": "neutral", "policy_id": "fixture-v1",
                                   "role_template_ref": case.config["role_template_ref"]})
        self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, str(roles["public_rebuild"]))
        for role in ("solver", "review-facts"):
            runtime, work = case.root / (role + "-runtime"), case.root / (role + "-work")
            work.mkdir()
            manifest = prepare_attempt(runtime, {"intent_id": role + "-intent", "policy_version": "fixture-v1",
                "role": role, "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture",
                "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), str(case.public_script)],
                "outputs": ["neutral.txt"], "inputs": [{"path": str(case.task / "draft/input/prompt.md"),
                    "kind": "public", "target": "draft/input/prompt.md"}],
                "request_sections": [{"kind": "operation", "text": "Use the current visible originals."}]})
            request = (runtime / "request.md").read_text()
            self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, request)
            self.assertEqual(manifest["request"]["sha256"], sha256(request.encode()))
        public = public_fixtures.IsolatedQualityTests()
        public.setUp()
        self.addCleanup(public.doCleanups)
        with patch("subprocess.Popen", side_effect=AssertionError("no model launch")):
            prepared = public.prepare()
        self.assertEqual(0, prepared["model_calls"])
        self.assertNotIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, (public.root / "request.md").read_text())

    def test_four_neutral_material_cases_reach_the_request_without_a_new_quality_verdict(self):
        # Each case is a teacher-side scenario, not an expected business answer.
        # Rendering cannot prove these duties were actually carried out.
        scenarios = (
            "Full ledger originals and stable account keys remain available; an author-prepared object allocation still needs independent derivation.",
            "The applicable rule formula and verification conditions are public; whether these particular records meet them must be checked against originals.",
            "A duly approved upstream allocation remains within its documented scope; its completed work is not claimed again as recipient reasoning.",
            "Two versioned source opinions remain visible with their dates and evidence; the recipient must reconcile applicability or retain supported conditional branches.",
        )
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                case = self.case()
                case.packet_data["items"][0]["direction"] = scenario
                write_json(case.packet, case.packet_data)
                case.packet_ref = evidence_ref(case.packet, "neutral responsibility scenario")
                with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue):
                    prepared = start_author_from_packet(case.store, **case.kwargs(False))
                request = (Path(prepared["runtime"]) / "request.md").read_text()
                self.assertIn(scenario, request)
                self.assertIn(INFORMED_PUBLIC_MATERIAL_GUIDANCE, request)
                self.assertEqual("ready", prepared["state"])
                self.assertEqual([], case.store.list_attempts(prepared["work_id"]))
                self.assertEqual([], case.store.list_flows())


if __name__ == "__main__":
    unittest.main()
