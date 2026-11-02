"""Neutral retained preparation/source receipts; no CLI/model or OS-probe calls."""

import ast
import copy
import json
import os
import shlex
from pathlib import Path
import shutil
import stat
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution import capture_candidate
from taskflow.execution_formal import prepare_formal, validate_formal_public_probe_recovery
from taskflow.execution_runner import _info
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_formal as formal_fixtures


OLD_TARGET = b"work / 'draft/input/prompt.md'"
NEW_TARGET = b"work / next(iter(plan['public_tree']))"


def make_public_probe_recovery_fixture(owner):
    """Build actual frozen neutral bytes with a deliberately named fixture gate."""
    case = formal_fixtures.FormalHandoffTests("test_detached_release_preserves_candidate_and_reuses_only_exact_receipt")
    case.setUp()
    owner.addCleanup(case.doCleanups)
    task = read_json(case.task / "task.json")
    (case.task / "draft/input/prompt.md").rename(case.task / "draft/input/commission.md")
    for row in task["files"]:
        if row["path"] == "draft/input/prompt.md":
            row["path"] = "draft/input/commission.md"
    write_json(case.task / "task.json", task)
    store = ExecutionStore(case.root / "state.sqlite3")
    case.candidate = capture_candidate(store, case.task, "neutral-relocated-current")
    case.report["candidate_binding"] = {key: case.candidate[key] for key in case.report["candidate_binding"]}
    case.save_report()
    release = case.release()
    root = case.root
    runtime = root / "formal/neutral-01"
    runtime.mkdir(parents=True)
    incident = root / "incident"
    incident.mkdir()
    adapter = root / "approved-original-adapter"
    adapter.mkdir()
    original_source = ("from pathlib import Path\ndef shell_probes(plan):\n"
                       "    work = Path(plan['cwd'])\n"
                       "    targets = [('public', work / 'draft/input/prompt.md', 'file', True)]\n"
                       "    return targets\n")
    (adapter / "prepare_diagnostic.py").write_text(original_source, encoding="utf-8")
    # This is an explicit neutral gate double: it exercises actual receipt/file
    # transport and probe freshness, not native sandbox or provider attestation.
    gate = """import hashlib
import json
from pathlib import Path
def public_tree(root):
 root=Path(root)
 return {str(p.relative_to(root)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'size':p.stat().st_size}
         for p in sorted(root.rglob('*')) if p.is_file()}
def validate(plan, require_review=False):
 root=Path(__file__).parent
 if json.loads((root/'execution-plan.json').read_text())!=plan: raise ValueError('fixture plan drift')
 for name in ('shell-probes.json','exec-context.json'):
  data=(root/name).read_bytes()
  if hashlib.sha256(data).hexdigest()!=plan['probe_evidence_hashes'][name]: raise ValueError('fixture probe drift')
  if json.loads(data).get('passed') is not True: raise ValueError('fixture probe failed')
 if public_tree(plan['cwd'])!=plan['public_tree']: raise ValueError('fixture public drift')
 if require_review and json.loads((root/'launch-review.json').read_text()).get('approved') is not True:
  raise ValueError('fixture approval missing')
 return (root/'request.md').read_bytes(), None
"""
    (adapter / "runtime_gate.py").write_text(gate, encoding="utf-8")
    (adapter / "execute_diagnostic.py").write_text("# Explicit zero-process validation fixture.\n", encoding="utf-8")
    (adapter / "file_evidence.py").write_text("# Neutral file evidence fixture.\n", encoding="utf-8")
    write_json(adapter / "baseline-plan.json", {"nature": "neutral zero-model baseline fixture"})
    replacement = root / "deterministic-source-v4"
    shutil.copytree(adapter, replacement)
    (replacement / "prepare_diagnostic.py").write_bytes(original_source.encode().replace(OLD_TARGET, NEW_TARGET))
    request = root / "request.md"
    request.write_text("Read the actual declared public input and deliver the neutral result.\n", encoding="utf-8")
    ref = lambda path: evidence_ref(path, "neutral public-probe recovery fixture evidence")
    authority = root / "authority.json"
    write_json(authority, {"controller": "fixture-controller", "task_id": case.candidate["task_id"], "formal": {
        "first_run_id": "neutral-01", "model": "gpt-5.6-terra", "timeout_seconds": 3600,
        "baseline": {"sha256": _info(adapter / "baseline-plan.json")["sha256"]}}})
    source_names = ("runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json")
    config = {"model": "gpt-5.6-terra", "timeout_seconds": 3600, "run_id": "neutral-01", "intent_id": "neutral-intent",
              "reasoning_effort": "medium", "reasoning_context": "all_turns", "policy_version": "neutral-policy",
              "launcher_python": str(Path(sys.executable).resolve()), "deny_read_probes": [],
              "adapter": {**ref(adapter / "prepare_diagnostic.py"), "files": {name: _info(adapter / name)["sha256"] for name in source_names}},
              "request": ref(request), "authorization": ref(authority)}
    binding = {"config_digest": sha256(canonical(config)), "release_receipt": release["receipt"],
               "candidate_binding": release["candidate_binding"], "runtime": str(runtime)}
    intent = runtime.with_name(runtime.name + ".formal-intent.json")
    write_json(intent, binding)
    for path in adapter.iterdir():
        shutil.copy2(path, runtime / path.name)
    shutil.copy2(request, runtime / "request.md")
    names = [*source_names, "prepare_diagnostic.py"]
    write_json(runtime / "source-lock.json", {"no_repository_dynamic_imports": True,
        "files": [{"path": str(runtime / name), **_info(runtime / name)} for name in names]})
    original_work = root / "failed-context/solver"
    shutil.copytree(release["solver"]["path"], original_work)
    missing = original_work / "draft/input/prompt.md"
    message = "Negative probe is invalid without existing target: " + str(missing)
    argv = [config["launcher_python"], str(runtime / "prepare_diagnostic.py"), "--solver", release["solver"]["path"],
            "--evaluation-directory", str(Path(release["snapshot"]["path"]) / "draft/evaluation"), "--run-id", "neutral-01",
            "--runtime-directory", str(runtime), "--task-directory", release["publication_root"], "--in-runtime"]
    for item in release["quality_evidence"]:
        argv += ["--quality-evidence", item["path"]]
    write_json(runtime / "preparation-failure.json", {"nature": "preparation_failure_no_inference", "run_id": "neutral-01",
               "type": "ValueError", "message": message, "model_calls": 0})
    write_json(runtime / "preparation-process.json", {"argv": argv, "exit_code": 1, "stdout": "",
               "stderr": "shell_probes(plan)\nValueError: " + message, "model_calls": 0})
    archive = incident / "previous-runtime"
    shutil.copytree(runtime, archive, copy_function=shutil.copy2)
    change = {"kind": "deterministic_public_probe_target_v1", "original": ref(adapter / "prepare_diagnostic.py"),
              "replacement": ref(replacement / "prepare_diagnostic.py"), "executed": ref(replacement / "prepare_diagnostic.py")}
    shutil.copy2(replacement / "prepare_diagnostic.py", runtime / "prepare_diagnostic.py")
    change["executed"] = ref(runtime / "prepare_diagnostic.py")
    token = runtime.with_name(runtime.name + ".public-probe-preparation-recovery.json")
    write_json(token, {"schema_version": 1, "kind": "exclusive_zero_inference_public_probe_source_recovery", "binding": binding,
                      "run_id": "neutral-01", "runtime": str(runtime), "model_calls": 0, "source_change": change})
    retained = runtime.with_name(runtime.name + ".failed-preparation-" + _info(token)["sha256"][:12])
    shutil.copytree(archive, retained, copy_function=shutil.copy2)
    manifest = incident / "previous-runtime.manifest.json"
    write_json(manifest, {"files": {path.name: _info(path) for path in archive.iterdir()}})
    write_json(runtime / "source-lock.json", {"no_repository_dynamic_imports": True,
        "files": [{"path": str(runtime / name), **_info(runtime / name)} for name in names]})
    work = root / "recovered-context/solver"
    shutil.copytree(release["solver"]["path"], work)
    public = {str(path.relative_to(work)): _info(path) for path in sorted(work.rglob("*")) if path.is_file()}
    first = next(iter(public))
    write_json(runtime / "shell-probes.json", {"passed": True, "host_existence_checked": [
        {"name": "public", "path": str(work / first), "expected_readable": True, "expected_sha256": public[first]["sha256"]}]})
    write_json(runtime / "exec-context.json", {"passed": True, "model_calls": 0, "nature": "neutral fixture, no native probe executed"})
    node = root / "dependencies/node/bin/node"
    node.parent.mkdir(parents=True)
    node.write_text("Neutral dependency byte fixture.\n", encoding="utf-8")
    snapshot = read_json(Path(release["snapshot"]["path"]) / "manifest.json")
    frozen = {"snapshot_digest": snapshot["snapshot_digest"], "taskflow_input_digest": case.candidate["input_digest"]}
    reservation = Path(release["publication_root"]) / "reports/runtime-reservations/neutral-01.json"
    reservation.parent.mkdir(parents=True)
    write_json(reservation, {"run_id": "neutral-01", "runtime_directory": str(runtime), "frozen_binding": frozen,
                            "nature": "preparation_only_zero_inference_not_a_model_attempt"})
    plan = {"run_id": "neutral-01", "task_id": case.candidate["task_id"], "requested_model": "gpt-5.6-terra", "timeout_seconds": 3600,
        "input_digest": case.candidate["input_digest"], "request_sha256": _info(request)["sha256"], "launcher_python": config["launcher_python"],
        "run_reservation": str(reservation), "run_reservation_sha256": _info(reservation)["sha256"],
        "baseline_plan_sha256": config["adapter"]["files"]["baseline-plan.json"], "source_solver": release["solver"]["path"],
        "private_directory": str(Path(release["snapshot"]["path"]) / "draft/evaluation"), "frozen_binding": frozen,
        "cwd": str(work), "root": str(work.parent), "public_tree": public,
        "probe_evidence_hashes": {name: _info(runtime / name)["sha256"] for name in ("shell-probes.json", "exec-context.json")},
        "argv": ["neutral-cli", 'shell_environment_policy.set={"PATH"=' + json.dumps(str(node.parent)) + '}', "exec"]}
    write_json(runtime / "execution-plan.json", plan)
    write_json(runtime / "plan-lock.json", {"sha256": _info(runtime / "execution-plan.json")["sha256"]})
    normal = runtime / "normal-tools-probe.json"
    member = read_json(work / "package.json")["files"][0]
    write_json(normal, {"binding": {"plan_sha256": _info(runtime / "execution-plan.json")["sha256"],
        "node": {"path": str(node), **_info(node)}, "public_sha256": member["sha256"],
        "scope": "Current zero-model native profile; no provider identity or hidden payload claim."},
        "returncode": 0, "model_calls": 0, "external_model_requests": 0,
        "observed": {"openssl_conf": None, "private_denied": True, "deny_code": "EACCES", "public_sha256": member["sha256"],
                     "own_write": True, "fork_ipc": True, "sqlite": True}})
    acceptance = runtime / "launch-review.json"
    write_json(acceptance, {"approved": True, "reviewer": "execution adapter on behalf of fixture-controller",
        "plan_sha256": _info(runtime / "execution-plan.json")["sha256"], "input_digest": case.candidate["input_digest"],
        "preparation_binding": binding, "authorization": {"path": str(authority), **_info(authority)}, "human_read_claimed": False,
        "normal_tools": {"path": str(normal), **_info(normal)}})
    process = incident / "recovery-process.json"
    write_json(process, {"argv": argv, "returncode": 0, "stdout": "Neutral preparation complete.", "stderr": "", "model_calls": 0})
    proof = {"schema_version": 1, "kind": "validated_zero_inference_public_probe_source_recovery", "binding": binding,
        "run_id": "neutral-01", "runtime": str(runtime), "intent": ref(intent), "reservation": ref(reservation),
        "previous_runtime": {"path": str(archive), "manifest": ref(manifest), "retained_path": str(retained)},
        "recovery_token": ref(token), "recovery_process": ref(process), "plan": ref(runtime / "execution-plan.json"),
        "acceptance": ref(acceptance), "source_change": change, "completed": True, "model_calls": 0}
    proof_path = incident / "completed-recovery.json"
    write_json(proof_path, proof)
    return SimpleNamespace(case=case, store=store, root=root, task=case.task, candidate=case.candidate, release=release,
        runtime=runtime, incident=incident, config=config, binding=binding, archive=archive, retained=retained,
        adapter=adapter, replacement=replacement, proof=proof, proof_path=proof_path, ref=ref,
        recovery=ref(proof_path), plan=plan, original_work=original_work)


class FormalPublicProbeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = make_public_probe_recovery_fixture(self)

    def validate(self):
        f = self.fixture
        return validate_formal_public_probe_recovery(config=f.config, release=f.release, candidate=f.candidate,
                                                     runtime=f.runtime, recovery=f.ref(f.proof_path))

    def test_exact_source_correction_keeps_original_failure_and_validates_read_only(self):
        f = self.fixture
        before = {str(p): p.read_bytes() for p in f.root.rglob("*") if p.is_file()}
        with patch("taskflow.execution_formal.subprocess.run", side_effect=AssertionError("readonly validator spawned")):
            result = self.validate()
        self.assertEqual("validated_zero_inference_public_probe_source_recovery", result["kind"])
        self.assertEqual(f.binding, result["binding"])
        self.assertEqual(f.proof["source_change"], result["source_change"])
        self.assertEqual(9, len(list(f.archive.iterdir())))
        self.assertFalse((f.archive / "shell-probes.json").exists())
        self.assertNotEqual(f.config["adapter"]["sha256"], result["source_change"]["executed"]["sha256"])
        self.assertEqual(before, {str(p): p.read_bytes() for p in f.root.rglob("*") if p.is_file()})

    def test_explicit_draft_bootstraps_normal_acceptance_but_cannot_resume(self):
        f = self.fixture
        draft = {**f.proof, "kind": "public_probe_source_recovery_preparation", "completed": False}
        draft.pop("acceptance")
        path = f.incident / "preparation-draft.json"
        write_json(path, draft)
        with self.assertRaises(TaskflowError):
            validate_formal_public_probe_recovery(config=f.config, release=f.release, candidate=f.candidate,
                                                  runtime=f.runtime, recovery=f.ref(path))
        (f.runtime / "launch-review.json").unlink()
        with patch("taskflow.execution_formal.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout=b"", stderr=b"")) as process:
            prepared = prepare_formal(config=f.config, release=f.release, candidate=f.candidate, runtime=f.runtime,
                                      preparation_recovery=f.ref(path))
        self.assertEqual(1, process.call_count)
        self.assertTrue(read_json(Path(prepared["acceptance_path"]))["approved"])
        f.proof["acceptance"] = f.ref(Path(prepared["acceptance_path"]))
        write_json(f.proof_path, f.proof)
        self.assertEqual(f.binding, self.validate()["binding"])

    def test_rehashed_extra_code_and_decoy_original_are_not_authorized(self):
        f = self.fixture
        source = f.replacement / "prepare_diagnostic.py"
        source.write_bytes(source.read_bytes() + b"# extra unapproved behavior\n")
        shutil.copy2(source, f.runtime / "prepare_diagnostic.py")
        proof = copy.deepcopy(f.proof)
        proof["source_change"].update(replacement=f.ref(source), executed=f.ref(f.runtime / "prepare_diagnostic.py"))
        token_path = Path(proof["recovery_token"]["path"])
        token = read_json(token_path)
        token["source_change"] = proof["source_change"]
        write_json(token_path, token)
        new_retained = f.runtime.with_name(f.runtime.name + ".failed-preparation-" + _info(token_path)["sha256"][:12])
        f.retained.rename(new_retained)
        proof["recovery_token"] = f.ref(token_path)
        proof["previous_runtime"]["retained_path"] = str(new_retained)
        write_json(f.proof_path, proof)
        with self.assertRaisesRegex(TaskflowError, "one permitted target expression"):
            self.validate()
        decoy = f.root / "same-byte-original.py"
        shutil.copy2(f.adapter / "prepare_diagnostic.py", decoy)
        proof["source_change"]["original"] = f.ref(decoy)
        token["source_change"] = proof["source_change"]
        write_json(token_path, token)
        new_retained_again = f.runtime.with_name(f.runtime.name + ".failed-preparation-" + _info(token_path)["sha256"][:12])
        new_retained.rename(new_retained_again)
        proof["recovery_token"] = f.ref(token_path)
        proof["previous_runtime"]["retained_path"] = str(new_retained_again)
        write_json(f.proof_path, proof)
        with self.assertRaisesRegex(TaskflowError, "different controlled path"):
            self.validate()

    def test_unrelated_source_changes_missing_source_and_old_os_recovery_are_rejected(self):
        f = self.fixture
        for path in (f.replacement / "prepare_diagnostic.py", f.runtime / "prepare_diagnostic.py",
                     f.archive / "prepare_diagnostic.py", f.retained / "preparation-process.json"):
            old = path.read_bytes()
            with self.subTest(path=str(path.relative_to(f.root))):
                path.write_bytes(old + b"# foreign change\n")
                with self.assertRaises(TaskflowError):
                    self.validate()
                path.write_bytes(old)
        proof = copy.deepcopy(f.proof)
        proof["kind"] = "validated_zero_inference_host_preparation_recovery"
        write_json(f.proof_path, proof)
        with self.assertRaises(TaskflowError):
            self.validate()
        write_json(f.proof_path, f.proof)
        from taskflow.execution_formal import validate_formal_preparation_recovery
        with self.assertRaises(TaskflowError):
            validate_formal_preparation_recovery(config=f.config, release=f.release, candidate=f.candidate,
                                                 runtime=f.runtime, recovery=f.ref(f.proof_path))

    def test_failed_probes_private_permissions_public_drift_and_model_markers_are_rejected(self):
        f = self.fixture
        for name in ("shell-probes.json", "exec-context.json"):
            path = f.runtime / name
            old = path.read_bytes()
            with self.subTest(probe=name):
                write_json(path, {**read_json(path), "passed": False})
                with self.assertRaises(TaskflowError):
                    self.validate()
                path.write_bytes(old)
        path = f.runtime / "request.md"
        previous_mode = stat.S_IMODE(path.stat().st_mode)
        path.chmod(previous_mode ^ 0o100)
        with self.assertRaisesRegex(TaskflowError, "permissions"):
            self.validate()
        path.chmod(previous_mode)
        original = f.original_work / "draft/input/commission.md"
        original.write_bytes(original.read_bytes() + b"drift")
        with self.assertRaisesRegex(TaskflowError, "original copied public"):
            self.validate()
        shutil.copyfile(Path(f.release["solver"]["path"]) / "draft/input/commission.md", original)
        (f.runtime / "execution-claim.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "executed or uncertain"):
            self.validate()

    def test_normal_prepare_requires_explicit_completed_recovery_for_new_source(self):
        f = self.fixture
        with self.assertRaisesRegex(TaskflowError, "approved adapter"):
            prepare_formal(config=f.config, release=f.release, candidate=f.candidate, runtime=f.runtime)
        calls = []
        def validate_only(argv, **kwargs):
            calls.append(argv)
            self.assertEqual([f.config["launcher_python"], str(f.runtime / "execute_diagnostic.py")], argv)
            return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        with patch("taskflow.execution_formal.subprocess.run", side_effect=validate_only):
            result = prepare_formal(config=f.config, release=f.release, candidate=f.candidate, runtime=f.runtime,
                                    preparation_recovery=f.recovery)
        self.assertEqual(1, len(calls))
        self.assertEqual(f.config["intent_id"], result["runner"]["intent_id"])
        self.assertEqual(f.binding, read_json(f.runtime.with_name(f.runtime.name + ".formal-intent.json")))


class PublicProbeTargetTests(unittest.TestCase):
    """Opt-in private adapter source tests with named fake process/socket IO."""

    def setUp(self):
        variable = "TASKFLOW_TEST_PUBLIC_PROBE_ADAPTER_ROOT"
        if variable not in os.environ:
            self.skipTest("optional source-layout regression: set " + variable + " to the approved private adapter parent")
        self.adapters = Path(os.environ[variable]).resolve()
        self.assertTrue(self.adapters.is_dir(), "explicit adapter parent must exist")
        self.fixture = make_public_probe_recovery_fixture(self)

    def probe(self, version, *, private_readable=False, missing_private=False):
        f = self.fixture
        module = self.adapters / version / "prepare_diagnostic.py"
        source = ast.parse(module.read_bytes())
        functions = [n for n in source.body if isinstance(n, ast.FunctionDef) and n.name in {"shell_probes", "failure_details"}]
        probe_function = next(n for n in functions if n.name == "shell_probes")
        def private_target(suffix):
            matches = [node.value for node in ast.walk(probe_function) if isinstance(node, ast.Constant)
                       and isinstance(node.value, str) and node.value.endswith(suffix)]
            self.assertEqual(1, len(matches), "private adapter must have one explicit target for " + suffix)
            return matches[0]
        session_target, runtime_rules_target = private_target("/.codex/sessions"), private_target("/.codex/RTK.md")
        work = f.root / (version + "-probe/solver")
        shutil.copytree(f.release["solver"]["path"], work)
        private = f.root / (version + "-probe/private")
        private.mkdir()
        for name in ("answer.md", "rubric.md"):
            (private / name).write_text("NEUTRAL_PRIVATE", encoding="utf-8")
        if missing_private:
            (private / "answer.md").unlink()
        repo = f.root / "probe-repository"
        for name in ("tasks/saas-recovery-redesign/versions/design-v3/checks/recompute_policy.py", "knowledge/index.md"):
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("NEUTRAL", encoding="utf-8")
        baseline = f.root / "probe-baseline"
        baseline.mkdir(exist_ok=True)
        (baseline / "answer.md").write_text("NEUTRAL_OLD", encoding="utf-8")
        sessions = f.root / "probe-sessions"
        sessions.mkdir(exist_ok=True)
        shared = f.root / "probe-shared"
        shared.mkdir(exist_ok=True)
        rtk = f.root / "probe-RTK.md"
        rtk.write_text("NEUTRAL_RUNTIME", encoding="utf-8")
        redirects = {session_target: sessions, runtime_rules_target: rtk, "/private/tmp": shared}
        def fixture_path(value):
            return redirects.get(str(value), Path(value))
        plan = {"cwd": str(work), "root": str(work.parent), "private_directory": str(private),
                "public_tree": {str(p.relative_to(work)): _info(p) for p in sorted(work.rglob("*")) if p.is_file()}}
        gate_source = module.with_name("runtime_gate.py")
        gate_nodes = [n for n in ast.parse(gate_source.read_bytes()).body if isinstance(n, ast.FunctionDef)
                      and n.name in {"shell_probe_passed", "shell_writable_directories"}]
        gate_sessions = [node.value for function in gate_nodes for node in ast.walk(function)
                         if isinstance(node, ast.Constant) and isinstance(node.value, str)
                         and node.value.endswith("/.codex/sessions")]
        self.assertEqual([session_target], gate_sessions, "actual gate and preparer must bind the same unique target")
        # Remap the actual unchanged private sessions target into this fixture;
        # all permission predicates remain the actual gate implementation.
        class FixtureSessions(ast.NodeTransformer):
            def visit_Constant(self, node):
                if node.value == session_target:
                    return ast.copy_location(ast.Constant(value=str(sessions)), node)
                return node
        gate_tree = ast.fix_missing_locations(FixtureSessions().visit(ast.Module(body=gate_nodes, type_ignores=[])))
        gate_ns = {"json": json, "Path": Path, "shell_probe_base": lambda plan: ["neutral-cli"]}
        exec(compile(gate_tree, str(gate_source), "exec"), gate_ns)
        saved = []
        def save(path, report):
            saved.append(copy.deepcopy(report))
        gate = SimpleNamespace(HERE=work.parent, shell_probe_base=lambda plan: ["neutral-cli"],
            probe_binding=lambda plan: {"fixture": "no native execution"}, save=save,
            shell_writable_directories=gate_ns["shell_writable_directories"], shell_probe_passed=gate_ns["shell_probe_passed"])
        calls = []
        def neutral_process(argv, **kwargs):
            calls.append(argv)
            if argv[-2] == "-c" and argv[-1].startswith("import socket"):
                return SimpleNamespace(returncode=1, stdout="", stderr="Operation not permitted")
            code = shlex.split(argv[-1])[-1]
            loop = next(n for n in ast.parse(code).body if isinstance(n, ast.For))
            specs = ast.literal_eval(loop.iter)
            rows = [{"name": row["name"], "accessible": row["expected_readable"],
                     **({"sha256": row["expected_sha256"], "writable": True} if row["expected_sha256"] else {}),
                     **({"errno": 1} if not row["expected_readable"] else {})} for row in specs]
            if private_readable:
                next(row for row in rows if row["name"] == "reviewer_private")["accessible"] = True
            return SimpleNamespace(returncode=0, stdout=json.dumps(rows), stderr="")
        class NeutralSocket:
            def __init__(self, family):
                self.address = ("127.0.0.1", 12345)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def bind(self, address):
                self.address = address
            def listen(self, count):
                pass
            def getsockname(self):
                return self.address
        ns = {"Path": fixture_path, "gate": gate, "REPOSITORY": repo, "BASELINE": baseline,
              "RUNTIME": f.root / "neutral-runtime", "shlex": shlex, "json": json,
              "subprocess": SimpleNamespace(run=neutral_process),
              "socket": SimpleNamespace(AF_INET=2, AF_UNIX=1, socket=NeutralSocket)}
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(module), "exec"), ns)
        return ns["shell_probes"], plan, saved, calls

    def test_layout_without_prompt_uses_actual_first_member_and_keeps_all_permission_probes(self):
        probe, plan, saved, calls = self.probe("formal-public-probe-member-v4")
        self.assertFalse((Path(plan["cwd"]) / "draft/input/prompt.md").exists())
        probe(plan)
        self.assertEqual(3, len(calls))
        report = saved[0]
        self.assertTrue(report["passed"])
        specs = report["host_existence_checked"]
        self.assertEqual(str(Path(plan["cwd"]) / next(iter(plan["public_tree"]))), specs[0]["path"])
        public = {row["path"] for row in specs if row["name"].startswith("public_member_")}
        self.assertEqual({str(Path(plan["cwd"]) / name) for name in plan["public_tree"]}, public)
        self.assertTrue({"reviewer_private", "frozen_teacher", "frozen_rubric", "symlink_teacher", "sessions", "shared_tmp"}
                        <= {row["name"] for row in specs if row["expected_readable"] is False})
        old_probe, old_plan, old_saved, old_calls = self.probe("formal-explicit-skill-disable-v3")
        with self.assertRaisesRegex(ValueError, "prompt.md"):
            old_probe(old_plan)
        self.assertEqual([], old_calls)
        self.assertEqual([], old_saved)

    def test_missing_real_negative_or_readable_private_still_fails_closed(self):
        probe, plan, saved, calls = self.probe("formal-public-probe-member-v4", missing_private=True)
        with self.assertRaisesRegex(ValueError, "answer.md"):
            probe(plan)
        self.assertEqual([], calls)
        self.assertEqual([], saved)
        # A distinct context keeps the adapter's append-only control directory.
        self.fixture = make_public_probe_recovery_fixture(self)
        probe, plan, saved, calls = self.probe("formal-public-probe-member-v4", private_readable=True)
        with self.assertRaisesRegex(ValueError, "Boundary probe failed"):
            probe(plan)
        self.assertFalse(saved[0]["passed"])



if __name__ == "__main__":
    unittest.main()
