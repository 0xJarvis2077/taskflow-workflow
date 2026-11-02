"""Neutral schema fixtures and actual subprocesses; no external model calls."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, sha256, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import (
    _native_checks, import_formal, native_quality_handoff, prepare_formal, prepare_release, projected_quality_reports,
)
from taskflow import execution_runner as runner
from taskflow.execution_store import ExecutionStore
from taskflow.packages import export_snapshot, freeze_task
from tests.helpers import add_reviews, make_task


SID = "00000000-0000-4000-8000-000000000001"


class FormalHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.task = make_task(self.root)
        database = ExecutionStore(self.root / "state.sqlite3")
        self.candidate = capture_candidate(database, self.task, "neutral-current")
        self.signed = self.root / "signed.json"
        self.report = {"schema_version": 1, "status": "passed", "scope": "whole_package",
                       "candidate_binding": {key: self.candidate[key] for key in
                                             ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                       "covered_review_kinds": [item["kind"] for item in read_json(self.task / "task.json")["reviews"]],
                       "limitations": ["Neutral schema fixture, not a business/model certification."],
                       "evidence": ["neutral source predicate"]}
        self.accepted = {"review_id": "neutral-schema-acceptance", "evidence": []}
        self.store = SimpleNamespace(get_candidate=lambda identity: self.candidate,
                                     quality_gate=lambda identity: self.accepted)
        self.reports = {item["id"]: self.signed for item in read_json(self.task / "task.json")["reviews"] if item["required"]}
        self.save_report()
        self.destination = self.root / "publication/neutral"

    def save_report(self):
        write_json(self.signed, self.report)
        self.accepted["evidence"] = [{"path": str(self.signed), "sha256": sha256(self.signed.read_bytes())}]

    def release(self):
        return prepare_release(self.store, candidate_id=self.candidate["candidate_id"], source_root=self.task,
                               destination=self.destination, version="neutral-v1", quality_reports=self.reports)

    def configured_formal_adapter(self):
        variable = "TASKFLOW_TEST_FORMAL_ADAPTER"
        if variable not in os.environ:
            self.skipTest(f"optional zero-model integration: set {variable} to a local formal adapter")
        value = os.environ[variable].strip()
        self.assertTrue(value, f"{variable} must name a nonempty local adapter directory")
        adapter = Path(value).expanduser().resolve()
        self.assertTrue(adapter.is_dir(), f"{variable} is not a directory: {adapter}")
        for name in ("baseline-plan.json", "runtime_gate.py", "prepare_diagnostic.py",
                     "execute_diagnostic.py", "file_evidence.py"):
            path = adapter / name
            self.assertTrue(path.is_file() and path.stat().st_size,
                            f"{variable} requires a nonempty file: {path}")
        try:
            baseline = read_json(adapter / "baseline-plan.json")
        except (OSError, ValueError, TaskflowError) as exc:
            self.fail(f"{variable} baseline-plan.json is not valid JSON: {exc}")
        observed_cli = baseline.get("observed_cli") if isinstance(baseline, dict) else None
        cli_path = observed_cli.get("path") if isinstance(observed_cli, dict) else None
        self.assertTrue(isinstance(cli_path, str) and cli_path.strip(),
                        f"{variable} baseline-plan.json requires observed_cli.path")
        cli = Path(cli_path).expanduser()
        self.assertTrue(cli.is_absolute() and cli.is_file() and os.access(cli, os.X_OK),
                        f"{variable} observed_cli.path must be an existing executable absolute path: {cli}")
        return adapter, baseline, cli

    def test_optional_formal_adapter_requires_explicit_nonempty_directory(self):
        with patch.dict(os.environ, {"TASKFLOW_TEST_FORMAL_ADAPTER": ""}):
            with self.assertRaisesRegex(AssertionError, "nonempty local adapter directory"):
                self.configured_formal_adapter()
        with patch.dict(os.environ, {"TASKFLOW_TEST_FORMAL_ADAPTER": str(self.root / "missing")}):
            with self.assertRaisesRegex(AssertionError, "is not a directory"):
                self.configured_formal_adapter()

    def test_optional_formal_adapter_rejects_incomplete_or_invalid_configuration(self):
        adapter = self.root / "optional-adapter"
        adapter.mkdir()
        with patch.dict(os.environ, {"TASKFLOW_TEST_FORMAL_ADAPTER": str(adapter)}):
            with self.assertRaisesRegex(AssertionError, "requires a nonempty file"):
                self.configured_formal_adapter()
            for name in ("baseline-plan.json", "runtime_gate.py", "prepare_diagnostic.py",
                         "execute_diagnostic.py", "file_evidence.py"):
                (adapter / name).write_text("neutral fixture", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "baseline-plan.json is not valid JSON"):
                self.configured_formal_adapter()
            write_json(adapter / "baseline-plan.json", {"observed_cli": {}})
            with self.assertRaisesRegex(AssertionError, "requires observed_cli.path"):
                self.configured_formal_adapter()
            write_json(adapter / "baseline-plan.json", {"observed_cli": {"path": str(adapter / "missing-cli")}})
            with self.assertRaisesRegex(AssertionError, "existing executable absolute path"):
                self.configured_formal_adapter()
            write_json(adapter / "baseline-plan.json", {"observed_cli": {"path": str(Path(sys.executable).resolve())}})
            selected, baseline, cli = self.configured_formal_adapter()
            self.assertEqual(adapter, selected)
            self.assertEqual(str(cli), baseline["observed_cli"]["path"])
            self.assertEqual(Path(sys.executable).resolve(), cli)

    def test_detached_release_preserves_candidate_and_reuses_only_exact_receipt(self):
        originals = {str(path.relative_to(self.task)): path.read_bytes() for path in self.task.rglob("*") if path.is_file()}
        first = self.release()
        inode = Path(first["snapshot"]["path"]).stat().st_ino
        self.assertEqual(first, self.release())
        self.assertEqual(inode, Path(first["snapshot"]["path"]).stat().st_ino)
        self.assertEqual(originals, {str(path.relative_to(self.task)): path.read_bytes() for path in self.task.rglob("*") if path.is_file()})
        self.assertEqual(self.candidate["input_digest"], first["solver"]["input_digest"])
        self.assertNotIn("scoring", first["solver"])
        self.report["evidence"] = ["different purported signature"]
        self.save_report()
        with self.assertRaisesRegex(TaskflowError, "different candidate/source/report"):
            self.release()

    def test_release_refuses_inconclusive_affected_fixture_foreign_or_unsigned_reports(self):
        original = copy.deepcopy(self.report)
        cases = [("status", "inconclusive", "explicit passed"),
                 ("scope", "affected", "whole_package"),
                 ("nature", "test_fixture", "fixture"),
                 ("covered_review_kinds", [], "review kind")]
        for key, value, message in cases:
            with self.subTest(key=key):
                self.report = {**original, key: value}
                self.save_report()
                with self.assertRaisesRegex(TaskflowError, message):
                    self.release()
        self.report = copy.deepcopy(original)
        self.report["candidate_binding"]["evaluation_digest"] = "f" * 64
        self.save_report()
        with self.assertRaisesRegex(TaskflowError, "current candidate"):
            self.release()
        self.report = original
        self.save_report()
        self.accepted["evidence"] = []
        with self.assertRaisesRegex(TaskflowError, "current accepted review"):
            self.release()
        self.assertFalse(self.destination.exists())

    def test_release_rejects_valid_same_public_export_from_different_private_snapshot(self):
        released = self.release()
        other = make_task(self.root / "other")
        (other / "draft/evaluation/answer.md").write_text("Different private answer with same public input.")
        add_reviews(other)
        frozen = freeze_task(other, "neutral-v1")
        exported = export_snapshot(Path(frozen["path"]), "evaluator")
        target = Path(released["evaluator"]["path"])
        shutil.rmtree(target)
        shutil.copytree(exported["path"], target)
        with self.assertRaisesRegex(TaskflowError, "export differs|private/scoring/snapshot"):
            self.release()

    def signed_source_protocol(self):
        root = self.root / "quality-original"
        (root / "artifacts").mkdir(parents=True)
        adoption = {"scope": "whole_package", "business": {"status": "accepted", "conclusion": "Explicit complete neutral responsibility conclusion."},
                    "artifact": {"readback": True, "coverage": "Complete neutral text/code readback."},
                    "calculation": {"exit_code": 0, "exact": True},
                    "limitations": ["Local neutral schema only; no production deployment or served identity claim."]}
        write_json(root / "artifacts/adoption.json", adoption)
        (root / "artifacts/intake.md").write_text("Explicit source responsibility finding.\nExplicit artifact and calculation evidence.\n")
        write_json(root / "artifacts/issues.json", {"blocking_issue_ids": []})
        manifest = {"intent_id": "quality-intent", "role": "quality", "nature": "real_cli_attempt",
                    "candidate_binding": self.report["candidate_binding"],
                    "outputs": ["adoption.json", "intake.md", "issues.json"]}
        write_json(root / "manifest.json", manifest)
        runner._save_collection(root, {"intent_id": "quality-intent", "sid": SID,
                                      "artifacts": {name: runner._info(root / "artifacts" / name) for name in manifest["outputs"]}})
        refs = {name: {"path": str(root / "artifacts" / filename), **runner._info(root / "artifacts" / filename)}
                for name, filename in (("adoption", "adoption.json"), ("intake", "intake.md"), ("issues", "issues.json"))}
        self.accepted.update(attempt_id="quality-attempt", evidence=list(refs.values()))
        attempt = {"attempt_id": "quality-attempt", "state": "completed", "role": "quality",
                   "candidate_id": self.candidate["candidate_id"], "runtime_path": str(root), "cli_sid": SID,
                   "intent_id": "quality-intent"}
        self.store.get_attempt = lambda identity: attempt
        authority = self.root / "projection-authority.json"
        write_json(authority, {"task_id": "neutral", "controller": "neutral schema fixture"})
        dimension = lambda pointer, expected: {
            "affirmations": [{"source": "adoption", "pointer": pointer, "equals": expected}],
            "evidence": [{"source": "intake", "lines": [1, 2], "nonempty": True}],
            "limitations": [{"source": "adoption", "pointer": "/limitations", "nonempty": True}]}
        protocol = {"authorization": {"path": str(authority), **runner._info(authority)},
                    "scope": {"source": "adoption", "pointer": "/scope", "equals": "whole_package"},
                    "dimensions": {"business": dimension("/business/status", "accepted"),
                                   "artifact": dimension("/artifact/readback", True),
                                   "calculation": dimension("/calculation/exact", True)}}
        return root, refs, protocol

    def test_explicit_dimension_projection_preserves_original_signature_and_is_revalidated(self):
        original, refs, protocol = self.signed_source_protocol()
        args = {"candidate_id": self.candidate["candidate_id"], "source_refs": refs,
                "protocol_map": protocol, "destination": self.root / "derived-quality"}
        reports = projected_quality_reports(self.store, **args)
        self.assertEqual(reports, projected_quality_reports(self.store, **args))
        business = read_json(reports["business"])
        self.assertEqual(SID, business["signer"]["cli_sid"])
        self.assertEqual("accepted", business["derived_quality_projection"]["affirmative_sources"][0]["actual_value"])
        self.assertNotIn("status", read_json(original / "artifacts/adoption.json"))
        self.reports = {spec["id"]: reports[spec["kind"]] for spec in read_json(self.task / "task.json")["reviews"] if spec["required"]}
        released = self.release()
        self.assertEqual(self.candidate["input_digest"], released["snapshot"]["input_digest"])
        (original / "artifacts/intake.md").write_text("Later changed purported evidence.")
        with self.assertRaisesRegex(TaskflowError, "drifted|changed"):
            self.release()

    def test_projection_refuses_missing_dimension_affirmation_or_affected_scope(self):
        original, refs, protocol = self.signed_source_protocol()
        altered = copy.deepcopy(protocol)
        altered["dimensions"]["artifact"]["affirmations"][0]["equals"] = False
        with self.assertRaisesRegex(TaskflowError, "explicit affirmation differs"):
            projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=refs,
                                      protocol_map=altered, destination=self.root / "bad-projection")
        altered = copy.deepcopy(protocol)
        altered["scope"]["equals"] = "affected"
        with self.assertRaisesRegex(TaskflowError, "affirmation differs|promote"):
            projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=refs,
                                      protocol_map=altered, destination=self.root / "bad-projection")
        self.assertFalse((self.root / "bad-projection").exists())

    def formal_archive(self, observer_stopped=True):
        release = self.release()
        inner = self.root / "formal-inner"
        inner.mkdir()
        root = self.root / "formal-root"
        work = root / "solver"
        shutil.copytree(release["solver"]["path"], work)
        (root / "solver-scratch").mkdir()
        for name in ("runtime_gate.py", "file_evidence.py", "source-lock.json", "baseline-plan.json"):
            (inner / name).write_text("{}")
        for name in ("shell-probes.json", "exec-context.json"):
            write_json(inner / name, {"passed": True})
        (inner / "request.md").write_text("Use the public package and deliver the requested result.")
        public_tree = {str(path.relative_to(work)): runner._info(path) for path in work.rglob("*") if path.is_file()}
        plan = {"cwd": str(work), "root": str(root), "run_id": "neutral-run", "task_id": "neutral",
                "requested_model": "neutral", "argv": ["codex", "-c", 'model_reasoning_effort="high"'],
                "launcher_python": sys.executable, "timeout_seconds": 3600,
                "input_digest": self.candidate["input_digest"], "public_tree": public_tree,
                "frozen_binding": {"snapshot_directory": release["snapshot"]["path"],
                                   "snapshot_digest": release["snapshot"]["snapshot_digest"],
                                   "taskflow_input_digest": self.candidate["input_digest"]},
                "source_lock_sha256": runner._info(inner / "source-lock.json")["sha256"],
                "request_sha256": runner._info(inner / "request.md")["sha256"],
                "probe_evidence_hashes": {name: runner._info(inner / name)["sha256"] for name in
                                          ("shell-probes.json", "exec-context.json")}}
        write_json(inner / "execution-plan.json", plan)
        plan_sha = runner._info(inner / "execution-plan.json")["sha256"]
        write_json(inner / "plan-lock.json", {"sha256": plan_sha})
        write_json(inner / "launch-review.json", {"approved": True, "reviewer": "neutral schema fixture",
                                                 "plan_sha256": plan_sha, "input_digest": self.candidate["input_digest"]})
        script = r'''
import json,os,pathlib,sys
r=pathlib.Path(__file__).parent
if '--execute' in sys.argv:
 out=r/'real-run';out.mkdir();(out/'file-change-evidence').mkdir()
 events=[{'type':'thread.started','thread_id':SID},{'type':'item.completed','item':{'type':'agent_message','text':'Visible neutral final response.'}},{'type':'turn.completed'}]
 trace='\n'.join(json.dumps(e) for e in events)+'\n'
 (out/'trace.jsonl').write_text(trace);(out/'request.md').write_bytes((r/'request.md').read_bytes())
 attempt={'run_id':'neutral-run','status':'process_exited','returncode':0,'exceptions':[],'interventions':[],'started_at':'2026-11-01T00:00:00+00:00','ended_at':'2026-11-01T00:00:01+00:00'}
 (out/'attempt.json').write_text(json.dumps(attempt));(r/'execution-attempt.json').write_text(json.dumps(attempt))
 (out/'file-change-evidence/summary.json').write_text(json.dumps({'observer_stopped':STOPPED,'trace_bytes_at_stop':len(trace.encode())}))
 (r/'post-run-input-readback.json').write_text(json.dumps({'matches':True}))
 pathlib.Path('deliverables/empty').mkdir(parents=True);pathlib.Path('deliverables/arbitrary-result.txt').write_text('Actual neutral subprocess delivery')
 pathlib.Path('working-code.py').write_text('print(2 <= 3)');pathlib.Path('scratch-note.tmp').write_text('temporary actual bytes')
 os.mkfifo('unused-pipe');pathlib.Path('scratch-link').symlink_to('/unread/teacher')
 (pathlib.Path(SCRATCH)/'temporary.bin').write_bytes(b'actual scratch bytes')
'''.replace("SID", repr(SID)).replace("STOPPED", repr(observer_stopped)).replace("SCRATCH", repr(str(root / "solver-scratch")))
        (inner / "execute_diagnostic.py").write_text(script)
        outer = self.root / "outer"
        config = {"intent_id": "neutral-intent", "policy_version": "neutral-policy", "role": "solver",
                  "mode": "existing_isolated", "model": "neutral", "reasoning_effort": "high",
                  "cwd": str(work), "timeout_seconds": 3600, "inputs": [], "outputs": [],
                  "argv": [sys.executable],
                  "output_trees": ["deliverables"], "request_sections": [],
                  "candidate_binding": {key: self.candidate[key] for key in
                                        ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                  "isolated": {"runtime": str(inner), "flavor": "formal"}}
        runner.prepare_attempt(outer, config)
        result = runner.run_attempt(outer)
        self.assertTrue(result["technical_accepted"], result)
        archive = self.root / "full-archive"
        runner.archive_full_attempt(outer, archive)
        attempt = {**self.candidate, "attempt_id": "neutral-attempt", "work_id": "neutral-work", "role": "solver",
                   "state": "completed", "intent_id": "neutral-intent", "runtime_path": str(outer), "cli_sid": SID}
        attempt.pop("evaluation_digest")  # Actual store attempts carry only input/content.
        return release, archive, attempt

    def test_actual_subprocess_full_original_import_is_exact_safe_and_idempotent(self):
        release, archive, attempt = self.formal_archive()
        args = {"attempt": attempt, "archive": archive, "snapshot": Path(release["snapshot"]["path"]), "run_id": "neutral-run"}
        first = import_formal(**args)
        self.assertEqual(first, import_formal(**args))
        self.assertEqual("unknown", first["classification"])
        native = read_json(Path(first["run_path"]) / "record.original.json")
        self.assertIsNone(native["observed_config"]["model"])
        first_review = read_json(Path(first["first_review_index"]))["allowed_direct_artifacts"]
        self.assertTrue(any(row.get("source_path") == "working-code.py" for row in first_review))
        self.assertTrue(any(row.get("source_path") == "temporary.bin" for row in first_review))
        self.assertTrue(any(row["id"] == "visible_response" and row.get("derived_from", {}).get("line") == 2 for row in first_review))
        self.assertTrue(all(row["visibility"] != "teacher_engineering_only" for row in first_review))
        self.assertEqual({"submission", "trace"}, {row["kind"] for row in first["first_review_inputs"]})
        self.assertIn("original/work/working-code.py", {row["target"] for row in first["first_review_inputs"]})
        self.assertIn("original/scratch/temporary.bin", {row["target"] for row in first["first_review_inputs"]})
        configuration_input = next(row for row in first["first_review_inputs"] if row["target"] == "original/derived/actual-configuration.json")
        self.assertEqual("trace", configuration_input["kind"])
        configuration = read_json(Path(configuration_input["path"]))
        self.assertIsNone(configuration["served_model"])
        self.assertTrue(configuration["served_identity_unknown"])
        self.assertEqual("neutral", configuration["requested_model"])
        self.assertTrue(configuration["technical_delivery"]["technical_accepted"])
        self.assertTrue(all(row["artifact_id"].startswith("raw_") and row["locators"] for row in configuration["fact_sources"]))
        self.assertNotIn("neutral-intent", json.dumps(configuration))
        self.assertNotIn("authorization", configuration)
        directories = read_json(Path(first["first_review_index"]).parent / "directories.json")
        work_tree = directories["sources"][directories["aliases"]["work"]["source"]]
        self.assertIn("deliverables/empty/", work_tree["directories"])
        self.assertEqual("fifo", work_tree["special"]["unused-pipe"]["kind"])
        original = archive / "observed-trace.jsonl"
        original.write_text("drift")
        with self.assertRaisesRegex(TaskflowError, "archive file/directory"):
            import_formal(**args)

    def test_original_import_requires_stopped_observer(self):
        release, archive, attempt = self.formal_archive(observer_stopped=False)
        with self.assertRaisesRegex(TaskflowError, "stopped observer"):
            import_formal(attempt=attempt, archive=archive, snapshot=Path(release["snapshot"]["path"]), run_id="neutral-run")
        self.assertFalse((self.destination / "runs/neutral-run").exists())

    def test_current_native_node_fork_sqlite_probe_uses_no_model_and_cleans_own_scratch(self):
        adapter, baseline, cli = self.configured_formal_adapter()
        release = self.release()
        runtime = self.root / "native-check"
        runtime.mkdir()
        shutil.copyfile(adapter / "runtime_gate.py", runtime / "runtime_gate.py")
        spec = importlib.util.spec_from_file_location("neutral_formal_native_fixture_gate", runtime / "runtime_gate.py")
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        root = self.root / "native-root"
        work = root / "solver"
        shutil.copytree(release["solver"]["path"], work)
        scratch = root / "solver-scratch"
        scratch.mkdir()
        argv, _ = gate.runtime_argv(baseline, str(root), str(cli))
        plan = {"argv": argv, "cwd": str(work), "root": str(root),
                "private_directory": str(Path(release["snapshot"]["path"]) / "draft/evaluation")}
        write_json(runtime / "execution-plan.json", plan)
        report = _native_checks(runtime, plan, sys.executable)
        observed = read_json(Path(report["path"]))
        self.assertEqual(0, observed["model_calls"])
        self.assertTrue(observed["observed"]["fork_ipc"])
        self.assertTrue(observed["observed"]["sqlite"])
        self.assertIsNone(observed["observed"]["openssl_conf"])
        self.assertEqual([], list(scratch.iterdir()))
        self.assertEqual(report, _native_checks(runtime, plan, sys.executable))

    def test_existing_formal_adapter_zero_model_prepare_returns_bound_enqueue_runner(self):
        adapter, baseline, _ = self.configured_formal_adapter()
        released = self.release()
        authority = self.root / "business-packet.json"
        baseline_sha = runner._info(adapter / "baseline-plan.json")["sha256"]
        write_json(authority, {"controller": "neutral-schema-fixture", "task_id": "neutral",
                               "formal": {"first_run_id": "neutral-formal", "model": "gpt-5.6-terra",
                                          "timeout_seconds": 3600, "baseline": {"sha256": baseline_sha}}})
        request = self.root / "request.md"
        request.write_text("Read the public neutral prompt and provide the requested neutral result.")
        config = {"adapter": {"path": str(adapter / "prepare_diagnostic.py"),
                              "sha256": runner._info(adapter / "prepare_diagnostic.py")["sha256"],
                              "files": {name: runner._info(adapter / name)["sha256"] for name in
                                        ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")}},
                  "authorization": {"path": str(authority), **runner._info(authority)},
                  "request": {"path": str(request), **runner._info(request)},
                  "run_id": "neutral-formal", "intent_id": "neutral-formal-intent", "policy_version": "neutral-v4",
                  "launcher_python": sys.executable, "model": "gpt-5.6-terra", "timeout_seconds": 3600,
                  "reasoning_effort": "medium", "reasoning_context": "all_turns",
                  "reasoning_selection": "verified_client_default", "deny_read_probes": []}
        runtime = self.root / "prepared-formal"
        prepared = prepare_formal(config=config, release=released, candidate=self.candidate, runtime=runtime)
        self.addCleanup(shutil.rmtree, Path(read_json(runtime / "execution-plan.json")["root"]))
        self.assertEqual(prepared, prepare_formal(config=config, release=released, candidate=self.candidate, runtime=runtime))
        self.assertFalse((runtime / "execution-claim.json").exists())
        self.assertEqual(0, read_json(runtime / "exec-context.json")["external_model_requests"])
        self.assertEqual(0, read_json(runtime / "normal-tools-probe.json")["model_calls"])
        outer_config = {**prepared["runner"], "candidate_binding": released["candidate_binding"]}
        manifest = runner.prepare_attempt(self.root / "prepared-outer", outer_config)
        self.assertEqual("gpt-5.6-terra", manifest["model"]["requested"])
        self.assertIsNone(manifest["model"]["served"])
        self.assertEqual("medium", manifest["model"]["client_resolved_reasoning"]["effort"])
        changed = {**config, "intent_id": "different-intent"}
        with self.assertRaisesRegex(TaskflowError, "immutable handoff"):
            prepare_formal(config=changed, release=released, candidate=self.candidate, runtime=runtime)


class NativeQualityHandoffTests(unittest.TestCase):
    """Native-shaped neutral receipts, with real request/output byte seals only."""

    def setUp(self):
        case = FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.root, self.task = case.root, case.task
        history = self.task / "draft/evaluation/research-history.md"
        history.write_text("Author research and calibration are not scoring standards.\n", encoding="utf-8")
        declaration = read_json(self.task / "task.json")
        declaration["files"].append({"id": "history", "path": history.relative_to(self.task).as_posix(), "role": "teacher"})
        write_json(self.task / "task.json", declaration)
        self.candidate = capture_candidate(ExecutionStore(self.root / "native-fixture.sqlite3"), self.task, "native-current")
        self.candidates = {self.candidate["candidate_id"]: self.candidate}
        author_binding = {"task_id": "neutral", "candidate_id": "author-baseline", "intent_id": "author-intent", "work_id": "author-work"}
        self.attempts = {"author-attempt": {"state": "completed", "cli_sid": "00000000-0000-4000-8000-000000000099", **author_binding}}
        self.parent_id, self.child_id = "native-parent", "native-child"
        original = self.root / "original-author-business.json"
        write_json(original, {"task_id": "neutral", "controller": "neutral-controller"})
        self.item = {"task_id": "neutral", "source_author_quality_flow": self.parent_id,
                     "source_author_attempt": "author-attempt", "source_author_intent": "author-intent",
                     "source_author_sid": self.attempts["author-attempt"]["cli_sid"],
                     "source_business_packet": str(original), "source_business_packet_sha256": runner._info(original)["sha256"]}
        self.authority = self.root / "six-authorized.json"
        write_json(self.authority, {"schema_version": 1, "controller": "neutral-controller", "items": [self.item]})
        self.authorization = {"path": str(self.authority), **runner._info(self.authority)}
        self.parent = {"flow_id": self.parent_id, "definition": {"kind": "author_quality_v1", "task_id": "neutral",
            "owner": "neutral-controller", "source_author_attempt": "author-attempt", "source_author_binding": author_binding,
            "authorization": {"path": str(original), **runner._info(original)}}, "state": {"status": "accepted", "child_flow_id": self.child_id}}
        self.child = {"flow_id": self.child_id, "definition": {"task_id": "neutral", "owner": "neutral-controller"},
                      "state": {"status": "accepted", "candidate_id": self.candidate["candidate_id"],
                                "candidate_root": str(self.task), "history": []}}
        self.flows = {self.parent_id: self.parent, self.child_id: self.child}
        self.current = {"review_id": "native-accepted", "attempt_id": "native-quality", "whole_package": True}
        self.store = SimpleNamespace(get_candidate=lambda key: self.candidates[key], get_attempt=lambda key: self.attempts[key],
            get_flow=lambda key: self.flows[key], quality_gate=lambda key: self.current,
            assert_review_current=lambda key: self.current)
        self.material = self.source(self.candidate, "native-quality", coverage="whole_package", outcome="accepted")
        self.bind_receipt("native-accepted")

    def source(self, candidate, identity, *, coverage, outcome):
        from taskflow.execution_flow_contracts import NATIVE_SIGNOFF_PROTOCOL_SHA256, render_native_signoff_protocol

        root = self.root / identity
        (root / "artifacts").mkdir(parents=True)
        (root / "inputs").mkdir()
        files = {row["package_path"]: row for row in candidate["files"]}
        prompt = next(row for row in candidate["files"] if row["role"] in {"prompt", "input", "template"})
        evidence = {"path": prompt["package_path"], "sha256": prompt["sha256"], "location": "line 1"}
        normative = [{"package_path": row["package_path"], **runner._info(Path(row["path"])),
                      "role": row["role"], "purpose": row["role"], "basis": "The current frozen normative answer or rubric."}
                     for row in candidate["files"] if row["role"] in {"answer", "rubric"}]
        affected = [] if coverage == "whole_package" else [row["package_path"] for row in candidate["files"] if row["role"] == "rubric"]
        issues = [] if outcome == "accepted" else ["reference-defect"]
        report = {"execution_routing": {"schema_version": 1, **{key: candidate[key] for key in
                   ("candidate_id", "content_digest", "input_digest", "evaluation_digest")},
                   "intent_id": identity + "-intent", "signer_role": "independent_quality", "independent_of_current_author": True,
                   "participated_in_production": False, "scope": "affected" if coverage == "affected" else "whole_package",
                   "outcome": outcome, "blocking_issue_ids": issues, "covered_defect_ids": ["closed-defect"] if coverage == "affected" else []},
                  "native_signoff": {"schema_version": 1, "protocol_sha256": NATIVE_SIGNOFF_PROTOCOL_SHA256,
                   "coverage": coverage, "affected_paths": affected, "coverage_basis": "Only the actual stated source scope.",
                   "dimensions": {kind: {"affirmed": True, "evidence": [evidence], "limitations": ["Neutral schema fixture only."]}
                                  for kind in ("business", "artifact", "calculation")},
                   "normative": {"status": "determined", "files": normative, "evidence": [evidence], "limitations": []},
                   "responsibility_analysis": [{"public_evidence": [evidence], "upstream_work_already_done": "Capacity is supplied.",
                    "remaining_judgment": "Select a feasible allocation.", "consequence": "The chosen quantity must satisfy capacity.",
                    "retained_necessary_conditions": ["Capacity 3 remains explicit."], "legitimate_branches": ["Equivalent comparisons are valid."],
                    "disposition": "retained" if outcome == "accepted" else "blocking_defect", "blocking_issue_ids": issues}]}}
        payload = {"adoption-readiness.json": report, "issues.json": {"issues": []}, "intake.md": "Neutral original source findings.\n"}
        script = "import json,pathlib,sys\nfor name,value in json.loads(sys.argv[1]).items():\n pathlib.Path('artifacts',name).write_text(json.dumps(value) if isinstance(value,dict) else value,encoding='utf-8')\n"
        subprocess.run([sys.executable, "-c", script, json.dumps(payload)], cwd=root, check=True, capture_output=True)
        (root / "request.md").write_text("Neutral schema exercise, zero model calls.\n" + render_native_signoff_protocol(), encoding="utf-8")
        inputs = []
        for path, row in files.items():
            target = root / "inputs" / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(Path(row["path"]).read_bytes())
            inputs.append({"source": row["path"], "target": path, "kind": "public" if row["role"] in {"prompt", "input", "template"} else "teacher",
                           **runner._info(target)})
        write_json(root / "cli-evidence.json", {"nature": "native-shaped neutral metadata fixture; no CLI model call"})
        manifest = {"intent_id": identity + "-intent", "role": "quality", "nature": "real_cli_attempt", "mode": "native_role",
                    "model": {"requested": "gpt-6.1-sol", "reasoning_effort": "xhigh", "catalog_evidence": runner._info(root / "cli-evidence.json")},
                    "candidate_binding": {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                    "request": runner._info(root / "request.md"), "inputs": inputs, "dependencies": {}, "outputs": list(payload)}
        write_json(root / "manifest.json", manifest)
        write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
        sid = SID if coverage != "affected" else "00000000-0000-4000-8000-000000000002"
        self.attempts[identity] = {"attempt_id": identity, "state": "completed", "role": "quality", "candidate_id": candidate["candidate_id"],
                                   "runtime_path": str(root), "cli_sid": sid, "intent_id": manifest["intent_id"]}
        self.seal(root)
        return root

    def seal(self, root):
        manifest = read_json(root / "manifest.json")
        runner._save_collection(root, {"intent_id": manifest["intent_id"], "sid": self.attempts[root.name]["cli_sid"],
            "technical_accepted": True, "process_ended": True,
            "artifacts": {name: runner._info(root / "artifacts" / name) for name in manifest["outputs"]}})

    def amend_source(self, mutate, root=None):
        root = root or self.material
        report = read_json(root / "artifacts/adoption-readiness.json")
        mutate(report)
        write_json(root / "artifacts/adoption-readiness.json", report)
        (root / "collection-lock.json").unlink()
        (root / "collection.json").unlink()
        self.seal(root)

    def amend_manifest(self, mutate):
        manifest = read_json(self.material / "manifest.json")
        mutate(manifest)
        write_json(self.material / "manifest.json", manifest)
        write_json(self.material / "manifest-lock.json", runner._info(self.material / "manifest.json"))
        (self.material / "collection-lock.json").unlink()
        (self.material / "collection.json").unlink()
        self.seal(self.material)

    def bind_receipt(self, review_id, attempt_id="native-quality"):
        self.child["state"]["accepted_review_id"] = review_id
        self.child["state"]["history"] = [{"kind": "independent_review", "review_id": review_id,
            "attempt_id": attempt_id, "candidate_id": self.candidate["candidate_id"],
            "cli_sid": self.attempts[attempt_id]["cli_sid"], "outcome": "accepted"}]
        path = self.root / "child-result.json"
        write_json(path, {"flow_id": self.child_id, "status": "accepted", "candidate_id": self.candidate["candidate_id"],
                          "controller": "neutral-controller", "history": self.child["state"]["history"]})
        self.child["state"]["receipt"] = str(path)
        self.parent["state"]["result"] = {"outcome": "candidate_quality_accepted", "candidate_id": self.candidate["candidate_id"],
            "candidate_root": str(self.task), "child_flow_id": self.child_id, "child_receipt": str(path), "accepted_review_id": review_id}

    def handoff(self):
        return native_quality_handoff(self.store, candidate_id=self.candidate["candidate_id"],
                                      authorization=self.authorization, source_flow_id=self.parent_id)

    def test_actual_protocol_three_dimensions_and_exact_normative_are_deterministic(self):
        handoff = self.handoff()
        self.assertEqual(handoff, self.handoff())
        self.assertEqual(["draft/evaluation/answer.md", "draft/evaluation/rubric.md"], handoff["scoring_source_allowlist"])
        self.assertFalse(any("history" in row["package_path"] for row in handoff["normative_sources"]))
        self.assertEqual({"business", "artifact", "calculation"}, set(handoff["protocol_map"]["dimensions"]))
        reports = projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=self.root / "native-projections")
        self.assertEqual("whole_package", read_json(reports["business"])["scope"])
        tampered = copy.deepcopy(handoff["protocol_map"])
        tampered["dimensions"]["business"]["affirmations"][0]["pointer"] = "/execution_routing/outcome"
        with self.assertRaisesRegex(TaskflowError, "exact signed handoff"):
            projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=handoff["source_refs"],
                                      protocol_map=tampered, destination=self.root / "tampered-projection")

    def test_hash_only_request_and_unlocked_request_cannot_certify_native_signature(self):
        path = self.material / "request.md"
        raw = path.read_bytes()
        path.write_bytes(b"hash-only protocol; the output still self-reports its SHA")
        with self.assertRaisesRegex(TaskflowError, "Actual request changed"):
            self.handoff()
        manifest = read_json(self.material / "manifest.json")
        manifest["request"] = runner._info(path)
        write_json(self.material / "manifest.json", manifest)
        write_json(self.material / "manifest-lock.json", runner._info(self.material / "manifest.json"))
        (self.material / "collection-lock.json").unlink()
        (self.material / "collection.json").unlink()
        self.seal(self.material)
        with self.assertRaisesRegex(TaskflowError, "absent from the actual locked request"):
            self.handoff()
        path.write_bytes(raw)

    def test_missing_false_dimension_and_public_responsibility_or_normative_drift_fail(self):
        original = read_json(self.material / "artifacts/adoption-readiness.json")
        cases = [
            (lambda r: r["native_signoff"]["dimensions"]["business"].update(affirmed=False), "scoped affirmation"),
            (lambda r: r["native_signoff"]["dimensions"].pop("artifact"), "three explicit dimensions"),
            (lambda r: r["native_signoff"].update(responsibility_analysis=[]), "responsibility analysis"),
            (lambda r: r["native_signoff"]["responsibility_analysis"][0].update(legitimate_branches=[]), "legitimate_branches"),
            (lambda r: r["native_signoff"]["responsibility_analysis"][0].update(disposition="unresolved"), "responsibility disposition"),
            (lambda r: r["native_signoff"]["normative"]["files"][0].update(sha256="f" * 64), "SHA/size"),
            (lambda r: r["native_signoff"]["normative"].update(status="needs_business_decision"), "substantive choice"),
            (lambda r: r["native_signoff"]["normative"]["files"].pop(), "answer and rubric"),
            (lambda r: r["native_signoff"]["normative"]["files"].append(copy.deepcopy(r["native_signoff"]["normative"]["files"][0])), "unique actual"),
            (lambda r: r["native_signoff"].update(protocol_sha256="f" * 64), "signed scope"),
            (lambda r: r["execution_routing"].update(task_id="other-task"), "signed scope"),
        ]
        for mutate, message in cases:
            with self.subTest(message=message):
                self.amend_source(lambda report: report.update(copy.deepcopy(original)))
                self.amend_source(mutate)
                with self.assertRaisesRegex(TaskflowError, message):
                    self.handoff()

    def test_authority_item_parent_child_and_receipt_cannot_be_substituted(self):
        original = self.authority.read_bytes()
        for items in ([], [self.item, self.item], [{**self.item, "source_author_quality_flow": "other-parent"}]):
            with self.subTest(items=len(items)):
                write_json(self.authority, {"controller": "neutral-controller", "items": items})
                self.authorization = {"path": str(self.authority), **runner._info(self.authority)}
                with self.assertRaisesRegex(TaskflowError, "one exact task/source-flow"):
                    self.handoff()
        self.authority.write_bytes(original)
        self.authorization = {"path": str(self.authority), **runner._info(self.authority)}
        self.parent["definition"]["source_author_attempt"] = "foreign-author"
        with self.assertRaisesRegex(TaskflowError, "actual accepted author parent"):
            self.handoff()
        self.parent["definition"]["source_author_attempt"] = "author-attempt"
        self.child["state"]["history"] = []
        with self.assertRaisesRegex(TaskflowError, "parent/child receipt"):
            self.handoff()

    def test_citations_bind_actual_metadata_observations_and_public_responsibility(self):
        declaration = self.task / "task.json"
        observed = self.root / "public-original.txt"
        observed.write_text("Actual neutral public reconstruction bytes.\n", encoding="utf-8")
        additions = [(declaration, "execution/task.json", "source_metadata"),
                     (observed, "actual-public-work/result.txt", "submission")]
        for source, target, kind in additions:
            staged = self.material / "inputs" / target
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_bytes(source.read_bytes())
        self.amend_manifest(lambda manifest: manifest["inputs"].extend(
            {"source": str(source), "target": target, "kind": kind, **runner._info(source)} for source, target, kind in additions))
        meta_ref = {"path": "execution/task.json", "sha256": runner._info(declaration)["sha256"], "location": "/files"}
        observed_ref = {"path": "actual-public-work/result.txt", "sha256": runner._info(observed)["sha256"], "location": "line 1"}
        self.amend_source(lambda report: report["native_signoff"]["dimensions"]["artifact"].update(evidence=[meta_ref, observed_ref]))
        self.assertTrue(self.handoff()["scoring_source_allowlist"])
        original = read_json(self.material / "artifacts/adoption-readiness.json")
        for reference in (meta_ref, {"path": "draft/evaluation/rubric.md", "sha256": runner._info(self.task / "draft/evaluation/rubric.md")["sha256"], "location": "line 1"}):
            self.amend_source(lambda report: report["native_signoff"]["responsibility_analysis"][0].update(public_evidence=[reference]))
            with self.assertRaisesRegex(TaskflowError, "current public|outside current"):
                self.handoff()
        self.amend_source(lambda report: report.update(original))
        self.amend_source(lambda report: report["native_signoff"]["dimensions"]["artifact"]["evidence"][0].update(sha256="f" * 64))
        with self.assertRaisesRegex(TaskflowError, "citation SHA"):
            self.handoff()
        self.amend_source(lambda report: report.update(original))
        self.amend_manifest(lambda manifest: next(item for item in manifest["inputs"] if item["target"] == "execution/task.json").update(source=str(self.root / "foreign-task.json")))
        with self.assertRaisesRegex(TaskflowError, "foreign candidate declaration"):
            self.handoff()

    def test_actual_quality_manifest_must_have_seen_each_normative_source(self):
        self.amend_manifest(lambda manifest: manifest.update(inputs=[item for item in manifest["inputs"]
                                                                   if item["target"] != "draft/evaluation/rubric.md"]))
        with self.assertRaisesRegex(TaskflowError, "native normative selection SHA/size"):
            self.handoff()

    def test_private_repair_uses_original_unchanged_and_fresh_affected_scope(self):
        old_task = self.root / "old-task"
        shutil.copytree(self.task, old_task)
        (old_task / "draft/evaluation/rubric.md").write_text("Old private standard needing the actual repair.\n")
        old = capture_candidate(ExecutionStore(self.root / "old.sqlite3"), old_task, "native-old")
        self.candidates[old["candidate_id"]] = old
        old_root = self.source(old, "old-quality", coverage="unchanged_scope", outcome="revision_required")
        fresh_root = self.source(self.candidate, "fresh-quality", coverage="affected", outcome="accepted")
        self.bind_receipt("fresh-accepted", "fresh-quality")
        graph = {"recheck_id": "fresh-accepted", "whole_source": {"candidate_id": old["candidate_id"], "attempt_id": "old-quality", "sid": SID,
                                   "report": {"path": str(old_root / "artifacts/adoption-readiness.json"), **runner._info(old_root / "artifacts/adoption-readiness.json")}},
                 "affected_source": {"candidate_id": self.candidate["candidate_id"], "attempt_id": "fresh-quality", "sid": self.attempts["fresh-quality"]["cli_sid"],
                                     "report": {"path": str(fresh_root / "artifacts/adoption-readiness.json"), **runner._info(fresh_root / "artifacts/adoption-readiness.json")}},
                 "input_digest": self.candidate["input_digest"], "closed_defect_ids": ["closed-defect"],
                 "private_changes": [{"path": "draft/evaluation/rubric.md"}], "source_limits": {"whole": {}, "affected": {}}}
        self.current = {"review_id": "composed", "attempt_id": "fresh-quality", "whole_package": False,
                        "snapshot_bound_acceptance": True, "composite_quality_coverage": graph}
        handoff = self.handoff()
        self.assertEqual({"fresh", "whole", "parent_flow_id", "child_flow_id", "child_receipt", "accepted_review_id", "composite_quality_coverage"}, set(handoff["certification"]))
        reports = projected_quality_reports(self.store, candidate_id=self.candidate["candidate_id"], source_refs=handoff["source_refs"],
                    protocol_map=handoff["protocol_map"], destination=self.root / "composite-native")
        result = read_json(reports["business"])
        self.assertFalse(result["whole_package"])
        self.assertEqual("explicit_repaired_quality_coverage", result["scope"])
        original = read_json(fresh_root / "artifacts/adoption-readiness.json")
        self.amend_source(lambda report: report["native_signoff"].update(affected_paths=["draft/evaluation/answer.md"]), fresh_root)
        graph["affected_source"]["report"].update(runner._info(fresh_root / "artifacts/adoption-readiness.json"))
        with self.assertRaisesRegex(TaskflowError, "association uncovered"):
            self.handoff()
        self.amend_source(lambda report: report.update(original), fresh_root)
        graph["affected_source"]["report"].update(runner._info(fresh_root / "artifacts/adoption-readiness.json"))
        self.amend_source(lambda report: report["native_signoff"].update(coverage="whole_package"), fresh_root)
        with self.assertRaisesRegex(TaskflowError, "signed scope"):
            self.handoff()


if __name__ == "__main__":
    unittest.main()
