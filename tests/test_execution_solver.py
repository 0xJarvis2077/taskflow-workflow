"""One explicit solver uses real local subprocesses and no external models."""

import copy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json
from taskflow.execution_quality import run_quality_task
from taskflow.execution_runner import _full_layout
from taskflow.execution_solver import (
    collect_solver_task, prepare_solver_task, register_solver_task,
    run_solver_task, validate_solver_source,
)
from taskflow.execution_solver_contracts import check_solver_permissions
from tests import test_execution_quality as quality_fixtures


def experiment_intake(refs):
    return {"purpose_kind": "formal_experiment",
        "quality": {"conclusion": "accepted", "evidence": refs,
                    "limitations": "Neutral fixture signature; not actual business quality."},
        "difficulty": {"status": "unknown", "conclusion": "The remaining mechanism is unknown.", "evidence": [],
                       "limitations": "No model difficulty inference from offline fixtures."},
        "experiment": {"important_unknown": "Does the public evidence support the remaining responsibility?",
            "outcomes": [
                {"observation": "Complete lawful result", "next_action": "retain",
                 "reason": "Retain the valid method and its supported scope."},
                {"observation": "Responsibility remains unresolved", "next_action": "research",
                 "reason": "Inspect the actual handling evidence before another investment."}]}}


SOLVER = r'''
import json,pathlib,sys
request=sys.stdin.read()
mode=sys.argv[1]
root=pathlib.Path.cwd().parent
inputs=root/'runtime/inputs'
assert list(inputs.rglob('prompt.md'))
assert not list(inputs.rglob('answer.md'))
assert not list(inputs.rglob('rubric.md'))
assert 'One explicit neutral whole quality task.' not in request
assert 'One explicit solver purpose.' not in request
sid='00000000-0000-4000-8000-000000000002' if mode=='same-sid' else '00000000-0000-4000-8000-000000000003'
print(json.dumps({'type':'thread.started','thread_id':sid}),flush=True)
if mode!='missing':pathlib.Path('answer.md').write_text('2 <= 3. This is a neutral fixture answer, not a model score.')
pathlib.Path('unselected-original.txt').write_text('Every work file belongs to the actual complete attempt.')
# The neutral fixture has no native scratch alias; retain simulated scratch in
# its owned work tree. Real native scratch capture belongs to runner tests.
pathlib.Path('scratch').mkdir()
pathlib.Path('scratch/scratch-original.txt').write_text('Complete neutral fixture scratch original.')
print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral fixture answer complete.'}}),flush=True)
print(json.dumps({'type':'turn.completed'}),flush=True)
'''


class SolverTaskTests(unittest.TestCase):
    def setUp(self):
        self.case = quality_fixtures.ExplicitQualityTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root, self.store = self.case.root, self.case.store
        self.quality = self.case.register("accepted", identity="quality-current")
        self.quality = run_quality_task(self.store, self.quality["flow_id"], True)
        self.assertEqual("valid", self.quality["state"]["delivery"])
        self.script = self.root / "solver.py"
        self.script.write_text(SOLVER, encoding="utf-8")
        self.purpose = "One explicit solver purpose."
        self.decision = self.store.record_decision("solver-choice", "neutral", "candidate-neutral",
            action="experiment", rationale="Explicit neutral fixture solver choice.",
            evidence=self.case.refs, experiment_purpose=self.purpose, authorized=True,
            intake=experiment_intake(self.case.refs))

    def spec(self, mode="answer", identity="solver-neutral"):
        home = self.root / (identity + "-home")
        home.mkdir(exist_ok=True)
        return {"schema_version": 1, "flow_id": identity, "task_id": "neutral", "owner": "fixture-coordinator",
            "policy_id": "fixture-v1", "directory": str(self.root / identity),
            "candidate_id": "candidate-neutral", "candidate_root": str(self.case.task),
            "quality_task_id": self.quality["flow_id"], "decision_id": self.decision["decision_id"],
            "purpose": self.purpose, "runner": {"mode": "test_fixture", "model": "fixture-cli",
                "reasoning_effort": "fixture", "timeout_seconds": 15, "inputs": [], "request_sections": [],
                "outputs": ["answer.md"], "dependencies": [str(self.script)],
                "argv": [str(Path(sys.executable).resolve()), str(self.script), mode],
                "role_session": {"mode": "fresh_ephemeral", "home": str(home), "previous": None}}}

    def register(self, mode="answer", identity="solver-neutral"):
        return register_solver_task(self.store, self.spec(mode, identity), self.case.refs)

    def test_public_only_solver_complete_archive_and_receipt_are_not_a_score(self):
        record = self.register()
        before = len(self.store.dashboard()["attempts"])
        dry = run_solver_task(self.store, record["flow_id"])
        self.assertEqual("not_started", dry["state"]["execution"])
        self.assertEqual(before, len(self.store.dashboard()["attempts"]))
        result = run_solver_task(self.store, record["flow_id"], True)
        self.assertEqual(("completed", "valid", None), tuple(result["state"][key] for key in ("execution", "delivery", "outcome")))
        source = validate_solver_source(self.store, record["flow_id"])
        self.assertEqual("solver_delivery_v1", source["receipt"]["kind"])
        self.assertTrue(all(row["kind"] == "public" for row in source["manifest"]["inputs"]))
        self.assertTrue(all("evaluation" not in row["target"] for row in source["manifest"]["inputs"]))
        payloads, _ = _full_layout(read_json(source["archive"] / "supplemental-intent.json"))
        self.assertTrue(any(row["path"].endswith("unselected-original.txt") for row in payloads.values()))
        self.assertTrue(any(row["path"].endswith("scratch-original.txt") for row in payloads.values()))
        self.assertEqual(1, len(self.store.list_attempts(result["state"]["invocation"]["work_id"])))
        self.assertEqual(["quality_task_v1", "solver_task_v1"], [row["definition"]["kind"] for row in self.store.list_flows()])

    def test_registration_prepare_run_and_collect_are_idempotent(self):
        spec = self.spec()
        first = register_solver_task(self.store, spec, self.case.refs)
        again = register_solver_task(self.store, spec, self.case.refs)
        self.assertEqual(first["revision"], again["revision"])
        ready = prepare_solver_task(self.store, first["flow_id"])
        self.assertEqual(ready["revision"], prepare_solver_task(self.store, first["flow_id"])["revision"])
        result = run_solver_task(self.store, first["flow_id"], True)
        receipt = Path(result["state"]["receipt"]["path"])
        raw = receipt.read_bytes()
        count = len(self.store.dashboard()["attempts"])
        self.assertEqual(result["revision"], collect_solver_task(self.store, first["flow_id"])["revision"])
        run_solver_task(self.store, first["flow_id"], True)
        self.assertEqual(count, len(self.store.dashboard()["attempts"]))
        self.assertEqual(raw, receipt.read_bytes())

    def test_solver_unknown_is_observed_and_never_requeued(self):
        record = self.register()
        record = prepare_solver_task(self.store, record["flow_id"])
        current = record["state"]["invocation"]
        attempt = self.store.claim("uncertain-neutral", [current["work_id"]])
        self.store.mark_uncertain(attempt["attempt_id"], "fixture launch result is unknown")
        with patch("taskflow.execution_tasks.reconcile", return_value=self.store.get_attempt(attempt["attempt_id"])):
            result = run_solver_task(self.store, record["flow_id"], True)
        self.assertEqual("unknown", result["state"]["execution"])
        self.assertEqual(1, len(self.store.list_attempts(current["work_id"])))

    def test_missing_delivery_and_reused_quality_sid_never_create_reviewable_receipt(self):
        for mode, expected in (("missing", "failed"), ("same-sid", "completed")):
            record = self.register(mode, "solver-" + mode)
            result = run_solver_task(self.store, record["flow_id"], True)
            self.assertEqual(expected, result["state"]["execution"])
            self.assertNotEqual("valid", result["state"]["delivery"])
            self.assertFalse((Path(record["definition"]["directory"]) / "solver-receipt.json").exists())
            with self.assertRaises(TaskflowError):
                validate_solver_source(self.store, record["flow_id"])

    def test_unaccepted_or_foreign_quality_and_wrong_decision_are_rejected_before_work(self):
        for field, value in (("quality_task_id", "missing-quality"), ("purpose", "different purpose"),
                             ("decision_id", "missing-decision"), ("candidate_root", str(self.root))):
            spec = self.spec(identity="invalid-" + field)
            spec[field] = value
            count = len(self.store.dashboard()["work"])
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                register_solver_task(self.store, spec, self.case.refs)
            self.assertEqual(count, len(self.store.dashboard()["work"]))
        revision = self.case.register("revision_required", identity="quality-negative")
        run_quality_task(self.store, revision["flow_id"], True)
        declined = self.spec(identity="solver-declined-quality")
        declined["quality_task_id"] = revision["flow_id"]
        with self.assertRaises(TaskflowError):
            register_solver_task(self.store, declined, self.case.refs)
        with self.assertRaises(TaskflowError):
            self.register()

    def test_forbidden_solver_configuration_is_rejected_without_a_model(self):
        mutations = [lambda spec: spec.update(schema_version=2),
                     lambda spec: spec["runner"].update(inputs=[{"kind": "teacher"}]),
                     lambda spec: spec["runner"].update(resume_sid="foreign"),
                     lambda spec: spec["runner"].update(request_sections=[{"kind": "teacher", "text": "private"}]),
                     lambda spec: spec["runner"].update(outputs=[]),
                     lambda spec: spec["runner"]["role_session"].update(mode="resume_existing"),
                     lambda spec: spec.update(directory="relative"),
                     lambda spec: spec.update(purpose=""),
                     lambda spec: spec["runner"].update(mode="existing_isolated"),
                     lambda spec: spec["runner"].update(outputs=["../outside.md"])]
        for index, mutation in enumerate(mutations):
            spec = self.spec(identity="invalid-config-" + str(index))
            mutation(spec)
            with self.subTest(index=index), self.assertRaises(TaskflowError):
                register_solver_task(self.store, spec, self.case.refs)

    def test_authority_and_original_definition_cannot_be_substituted(self):
        spec = self.spec()
        with self.assertRaises(TaskflowError):
            register_solver_task(self.store, spec, [])
        original = register_solver_task(self.store, spec, self.case.refs)
        different = copy.deepcopy(spec)
        different["runner"]["outputs"] = ["different.md"]
        with self.assertRaises(TaskflowError):
            register_solver_task(self.store, different, self.case.refs)
        self.assertEqual(original["definition"], self.store.get_flow(original["flow_id"])["definition"])
        conflict = self.spec(identity="solver-conflict")
        self.store.register_flow(conflict["flow_id"], {"kind": "unrelated", "policy_id": "fixture-v1"}, self.case.refs, {})
        with self.assertRaises(TaskflowError):
            register_solver_task(self.store, conflict, self.case.refs)

    def test_current_candidate_and_quality_original_drift_are_rejected(self):
        record = self.register()
        current = self.quality["state"]["invocations"][self.quality["state"]["current_index"]]
        report = Path(current["runtime"]) / "artifacts/adoption-readiness.json"
        old = report.read_bytes()
        report.write_text("{}", encoding="utf-8")
        with self.assertRaises((TaskflowError, ValueError, OSError)):
            prepare_solver_task(self.store, record["flow_id"])
        report.write_bytes(old)
        public = self.case.task / "draft/input/prompt.md"
        public.write_text("Different actual task bytes.", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            prepare_solver_task(self.store, record["flow_id"])

    def test_receipt_or_full_archive_drift_cannot_be_used_as_a_review_source(self):
        record = self.register()
        result = run_solver_task(self.store, record["flow_id"], True)
        source = validate_solver_source(self.store, record["flow_id"])
        receipt = Path(result["state"]["receipt"]["path"])
        original = receipt.read_bytes()
        receipt.write_text("{}", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            validate_solver_source(self.store, record["flow_id"])
        receipt.write_bytes(original)
        archive = source["archive"] / "trace.jsonl"
        archive.write_bytes(archive.read_bytes() + b"\n")
        with self.assertRaises((TaskflowError, ValueError, OSError)):
            validate_solver_source(self.store, record["flow_id"])

    def test_role_policy_can_change_without_relabeling_original_quality(self):
        from tools.execution_simulation import fixture_policy
        self.store.register_policy("new-executor-policy", fixture_policy())
        spec = self.spec()
        spec["policy_id"] = "new-executor-policy"
        record = register_solver_task(self.store, spec, self.case.refs)
        result = run_solver_task(self.store, record["flow_id"], True)
        self.assertEqual("valid", result["state"]["delivery"])
        source = validate_solver_source(self.store, record["flow_id"])
        self.assertEqual("new-executor-policy", source["record"]["definition"]["policy_id"])
        self.assertEqual("fixture-v1", source["quality"]["definition"]["policy_id"])

    def test_native_solver_never_promotes_fixture_quality_to_real_business(self):
        from taskflow.execution_solver_contracts import quality_source
        spec = self.spec()
        spec["runner"]["mode"] = "native_role"
        with self.assertRaisesRegex(TaskflowError, "simulated quality"):
            quality_source(self.store, spec)

    def test_extra_public_repository_member_and_native_private_access_are_refused(self):
        record = self.register()
        ready = prepare_solver_task(self.store, record["flow_id"])
        public_root = Path(record["definition"]["directory"]) / "public"
        (public_root / "unexpected.txt").write_text("Undeclared material.", encoding="utf-8")
        with self.assertRaises(TaskflowError):
            run_solver_task(self.store, record["flow_id"], True)
        (public_root / "unexpected.txt").unlink()
        files = self.store.get_candidate("candidate-neutral")["files"]
        source_files = {row["package_path"]: {**row, "source": row["path"]} for row in files}
        baseline = {":root": "deny", ":minimal": "read", ":tmpdir": "deny",
                    ":slash_tmp": "deny", "/private/tmp": "deny", str(public_root): "read"}
        permissions = {"repository": str(public_root), "probe_sources": [str(next(public_root.rglob("prompt.md")))],
                       "readonly_probe": str(next(public_root.rglob("prompt.md"))),
                       "python": str(Path(sys.executable).resolve()), "scratch": ready["state"]["invocation"]["scratch"],
                       "filesystem": dict(baseline)}
        native_runner = {"mode": "native_role", "native_permissions": permissions,
                         "cwd": ready["state"]["invocation"]["work"], "cli": str(Path(sys.executable).resolve()),
                         "role_session": record["definition"]["runner"]["role_session"]}
        check_solver_permissions(native_runner, public_root,
                                 source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["filesystem"][str(self.case.task)] = "read"
        with self.assertRaises(TaskflowError):
            check_solver_permissions(native_runner, public_root,
                                     source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        for path in (self.case.task / "task.json", Path(self.quality["definition"]["directory"]) / "sources/current.json"):
            permissions["filesystem"] = {**baseline, str(path): "read"}
            with self.assertRaises(TaskflowError):
                check_solver_permissions(native_runner, public_root,
                    source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["filesystem"] = dict(baseline)
        permissions["repository"] = str(self.case.task)
        with self.assertRaises(TaskflowError):
            check_solver_permissions(native_runner, public_root,
                source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["repository"] = str(public_root)
        permissions["probe_sources"] = [str(next(public_root.rglob("prompt.md")))]
        permissions["filesystem"][":root"] = "read"
        with self.assertRaises(TaskflowError):
            check_solver_permissions(native_runner, public_root,
                source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["filesystem"] = dict(baseline)
        permissions["probe_sources"] = [str(self.case.task / "task.json")]
        with self.assertRaises(TaskflowError):
            check_solver_permissions(native_runner, public_root,
                source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["probe_sources"] = [str(next(public_root.rglob("prompt.md")))]
        for name in (str(public_root.parent), str(public_root.parent / "invocations/solver-000/spec.json"),
                     str(Path.home() / ".codex"), ":user_home", str(self.root / "other-batch-private")):
            permissions["filesystem"] = {**baseline, name: "read"}
            with self.subTest(forbidden_grant=name), self.assertRaises(TaskflowError):
                check_solver_permissions(native_runner, public_root,
                    source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        with self.assertRaises(TaskflowError):
            check_solver_permissions({"mode": "native_role", "native_permissions": []}, public_root,
                source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        permissions["filesystem"] = []
        with self.assertRaises(TaskflowError):
            check_solver_permissions(native_runner, public_root,
                source_files, Path(self.quality["definition"]["directory"]), self.case.task)
        self.assertEqual([], self.store.list_attempts(ready["state"]["invocation"]["work_id"]))


if __name__ == "__main__":
    unittest.main()
