"""Native-shaped, real neutral subprocess deliveries; no external model calls."""

import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_recheck_supplement import validate_recheck_supplement
from taskflow.execution_runner import _info, _save_collection
from taskflow.execution_store import evidence_ref
from taskflow.packages import check_task
from tests.helpers import make_task


def _candidate(root, identity, parent=None):
    check = check_task(root, write_report=False)
    value = {key: check[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")}
    value.update(candidate_id=identity, files=[{"path": str(root / row["path"]), "package_path": row["path"],
        "role": row["role"], **_info(root / row["path"])} for row in read_json(root / "task.json")["files"]],
        evidence=[evidence_ref(root / "task.json", "explicit package declaration")])
    if parent:
        value["parent_id"] = parent["candidate_id"]
    return value


def _role(root, candidate, report, *, sid, claimed, last, prior_work=None):
    """Seal native-format metadata around an ordinary zero-model Python worker."""
    work, runtime = root / "work", root / "runtime"
    work.mkdir(parents=True)
    (runtime / "artifacts").mkdir(parents=True)
    (runtime / "inputs").mkdir()
    payload = root / "payload.json"
    write_json(payload, {"adoption-readiness.json": report, "issues.json": [] if prior_work else {"issues": []},
                         "intake.md": "Neutral original affected source. No business/model certification."})
    worker = root / "worker.py"
    worker.write_text("import json,pathlib,sys\n"
        "for name,value in json.loads(pathlib.Path(sys.argv[1]).read_text()).items():\n"
        " pathlib.Path(name).write_text(json.dumps(value) if not isinstance(value,str) else value)\n"
        + "\n".join("print(json.dumps(" + repr(event) + "))" for event in (
            {"type": "thread.started", "thread_id": sid},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "Actual neutral subprocess delivery."}},
            {"type": "turn.completed"})), encoding="utf-8")
    process = subprocess.run([sys.executable, str(worker), str(payload)], cwd=work, check=True, capture_output=True)
    for path in work.iterdir():
        shutil.copyfile(path, runtime / "artifacts" / path.name)
    (runtime / "trace.jsonl").write_bytes(process.stdout)
    write_json(runtime / "termination.json", {"returncode": process.returncode, "process_ended": True})
    (runtime / "request.md").write_text("Neutral native-shaped source fixture; no actual model call.", encoding="utf-8")
    write_json(runtime / "cli-evidence.json", {"nature": "constructed native-format schema metadata"})
    inputs = []
    for row in candidate["files"]:
        target = runtime / "inputs" / row["package_path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(row["path"], target)
        inputs.append({"source": row["path"], "target": row["package_path"],
                       "kind": "public" if row["role"] == "prompt" else "teacher", **_info(target)})
    if prior_work:
        target = runtime / "inputs/prior/adoption-readiness.json"
        target.parent.mkdir()
        shutil.copyfile(prior_work, target)
        inputs.append({"source": str(prior_work), "target": "prior/adoption-readiness.json", "kind": "source_metadata", **_info(target)})
    intent = report["execution_routing"]["intent_id"]
    manifest = {"intent_id": intent, "role": "quality", "mode": "native_role", "nature": "real_cli_attempt",
        "candidate_binding": {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
        "request": _info(runtime / "request.md"), "inputs": inputs, "dependencies": {str(path): _info(path) for path in (worker, payload)},
        "outputs": ["adoption-readiness.json", "issues.json", "intake.md"],
        "model": {"requested": "gpt-6.1-sol", "reasoning_effort": "xhigh", "catalog_evidence": _info(runtime / "cli-evidence.json")}}
    write_json(runtime / "manifest.json", manifest)
    write_json(runtime / "manifest-lock.json", _info(runtime / "manifest.json"))
    _save_collection(runtime, {"intent_id": intent, "sid": sid, "technical_accepted": True,
        "process_ended": True, "delivery_complete": True, "model_completed": True,
        "artifacts": {name: _info(runtime / "artifacts" / name) for name in manifest["outputs"]}})
    return {"attempt_id": root.name, "state": "completed", "role": "quality", "candidate_id": candidate["candidate_id"],
        "runtime_path": str(runtime), "intent_id": intent, "request_digest": manifest["request"]["sha256"],
        "cli_sid": sid, "process_exit": process.returncode, "response_complete": True, "artifacts_complete": True,
        "technical_accepted": True, "claimed_seq": claimed, "last_seq": last}


def make_supplement_fixture(testcase, mutate=None, *, reuse_sid=False, use_parent_alias=False):
    """Return all public-validator arguments plus paths; never modify old receipts."""
    temporary = tempfile.TemporaryDirectory()
    testcase.addCleanup(temporary.cleanup)
    root = Path(temporary.name).resolve()
    parent_root = make_task(root / "before")
    current_root = root / "after/neutral"
    shutil.copytree(parent_root, current_root)
    for name in ("draft/evaluation/answer.md", "draft/evaluation/rubric.md", "record.md"):
        with (current_root / name).open("a", encoding="utf-8") as stream:
            stream.write("\nActual private revision requiring an affected association.\n")
    parent = _candidate(parent_root, "parent")
    candidate = _candidate(current_root, "repaired", parent)
    before = {row["package_path"]: row for row in parent["files"]}
    after = {row["package_path"]: row for row in candidate["files"]}
    changed = ["draft/evaluation/answer.md", "draft/evaluation/rubric.md", "record.md"]
    original, extras = {changed[0]}, set(changed[1:])
    closed = ["neutral-defect"]
    prior_sid, new_sid = str(uuid.uuid4()), str(uuid.uuid4())
    prior_evidence = [{"path": name, "sha256": after[name]["sha256"], "location": "actual prior line 1"} for name in changed[:-1]]
    prior_report = {"execution_routing": {"candidate_id": candidate["candidate_id"]},
        "defect_recheck": [{"defect_id": closed[0], "disposition": "resolved_in_affected_scope", "evidence": prior_evidence}],
        "native_signoff": {"coverage": "affected", "dimensions": {
            kind: {"affirmed": True, "evidence": prior_evidence} for kind in ("business", "artifact", "calculation")}}}
    prior_report["execution_routing"]["intent_id"] = "prior-quality"
    prior_attempt = _role(root / "prior", candidate, prior_report, sid=prior_sid, claimed=10, last=20)
    prior_root = Path(prior_attempt["runtime_path"])
    prior_ref = evidence_ref(prior_root / "artifacts/adoption-readiness.json", "original collected prior signature")
    prior_work = prior_root.parent / "work/adoption-readiness.json"
    prior = {"attempt": prior_attempt, "candidate": candidate, "report": prior_report, "report_ref": prior_ref,
             "issues": {"issues": []}, "issues_ref": evidence_ref(prior_root / "artifacts/issues.json", "prior issues")}
    pointers = {changed[0]: "/defect_recheck/0/evidence/0", changed[1]: "/native_signoff/dimensions/artifact/evidence/1"}

    def provenance(pointer):
        return {"path": str(prior_work), "sha256": prior_ref["sha256"], "json_pointer": pointer,
            "candidate_id_pointer": "/execution_routing/candidate_id", "coverage_pointer": "/native_signoff/coverage",
            "same_candidate_and_current_sha_verified": True, "fresh_suite_execution": False}

    fresh = {"path": after[changed[-1]]["path"], "package_path": changed[-1], "sha256": after[changed[-1]]["sha256"],
        "location": "Current appended record and its unchanged original prefix.", "parent_path": before[changed[-1]]["path"],
        "parent_sha256": before[changed[-1]]["sha256"], "judgment_origin": "fresh_independent_record_readback_and_association_judgment",
        "judgment": "Actual current provenance correctly attributes the repair and prior signatures.", "fresh_suite_execution": False}
    evidence = [{"path": after[name]["path"], "package_path": name, "sha256": after[name]["sha256"],
                 "location": "actual prior line 1", "judgment_origin": "reused_same_candidate_signed_affected_judgment",
                 "provenance": provenance(pointers[name])} for name in changed[:-1]] + [fresh]
    pairs = [{"path": name, "role": after[name]["role"], "old": before[name]["sha256"], "new": after[name]["sha256"],
        "old_source": before[name]["path"], "new_source": after[name]["path"], "old_size_bytes": _info(Path(before[name]["path"]))["size"],
        "new_size_bytes": _info(Path(after[name]["path"]))["size"], "old_bytes_verified": True, "new_bytes_verified": True,
        "fresh_necessary_association_verified": name in extras} for name in changed]
    associations = [{"path": name, "current_path": after[name]["path"], "parent_path": before[name]["path"],
        "old_sha256": before[name]["sha256"], "new_sha256": after[name]["sha256"], "old_new_bytes_verified": True,
        "old_location": "parent line 1", "new_location": "current changed lines", "delta": "Actual private byte revision.",
        "necessity": "This exact private association supports the one independently closed responsibility.",
        "disposition": "necessary_association_verified", "judgment_origin": "fresh_old_new_and_necessity_examination",
        "prior_evidence_provenance": provenance(pointers[name]) if name in pointers else None} for name in changed[1:]]
    report = {"schema_version": 1, "task_id": "neutral", "candidate_id": candidate["candidate_id"], "parent_candidate_id": parent["candidate_id"],
        "execution_routing": {"schema_version": 1, "intent_id": "new-supplement", "candidate_id": candidate["candidate_id"],
            "signer_role": "independent_quality", "independent_of_current_author": True, "participated_in_production": False,
            "scope": "affected", "outcome": "accepted", "blocking_issue_ids": [], "covered_defect_ids": closed},
        "native_signoff": {"schema_version": 1, "coverage": "affected", "affected_paths": changed,
            "dimensions": {kind: {"affirmed": True, "reused_judgment": provenance("/native_signoff/dimensions/" + kind)}
                           for kind in ("business", "artifact", "calculation")}},
        "coverage": {"scope": "affected", "changed_files": pairs, "original_defect_paths": list(original),
            "associated_extra_paths": sorted(extras), "reused_judgment_paths": changed[:-1], "new_record_judgment_paths": [changed[-1]],
            "fresh_old_new_necessity_judgment_paths": sorted(extras)},
        "prior_judgment_reuse": {"path": str(prior_work), "sha256": prior_ref["sha256"], "candidate_id": candidate["candidate_id"],
            "candidate_id_json_pointer": "/execution_routing/candidate_id", "scope_json_pointer": "/native_signoff/coverage",
            "supplied_copy_path": str(root / "supplement/runtime/inputs/prior/adoption-readiness.json"), "supplied_copy_byte_identical": True,
            "historic_whole_package_judgment_reissued": False, "normative_allowlist_reselection_performed": False,
            "defect_disposition_json_pointer": "/defect_recheck/0/disposition",
            "dimension_judgment_json_pointers": {kind: "/native_signoff/dimensions/" + kind for kind in ("business", "artifact", "calculation")},
            "reused_file_evidence": [{"path": name, "current_sha256": after[name]["sha256"], "json_pointer": pointers[name],
                                      "original_location": "actual prior line 1"} for name in changed[:-1]]},
        "fresh_record_readback": fresh,
        "defect_resolution": {"defect_id": closed[0], "status": "resolved_in_affected_scope", "remaining_blocking_issue_ids": [],
                              "evidence": evidence, "affected_paths": sorted(extras), "necessary_associations": associations}}
    aliases = {}
    if use_parent_alias:
        source = root / "actual-parent-staged/record.md"
        source.parent.mkdir()
        shutil.copyfile(before[changed[-1]]["path"], source)
        aliases[str(source)] = before[changed[-1]]["path"]
        fresh["parent_path"] = str(source)
        next(row for row in pairs if row["path"] == changed[-1])["old_source"] = str(source)
        next(row for row in associations if row["path"] == changed[-1])["parent_path"] = str(source)
    if mutate:
        mutate(report)
    attempt = _role(root / "supplement", candidate, report, sid=prior_sid if reuse_sid else new_sid,
                    claimed=30, last=40, prior_work=prior_work)
    runtime = Path(attempt["runtime_path"])
    spec = {"schema_version": 1, "attempt_id": attempt["attempt_id"], "base_recheck_id": "prior-recheck",
        "report": evidence_ref(runtime / "artifacts/adoption-readiness.json", "actual new neutral affected signature"),
        "issues": evidence_ref(runtime / "artifacts/issues.json", "actual empty issues array"), "original_recheck_report": prior_ref}
    return {"arguments": {"supplement": spec, "attempt": attempt, "prior": prior, "parent": parent, "candidate": candidate,
                          "closed_defect_ids": closed, "changed_paths": changed, "original_paths": original, "parent_aliases": aliases},
            "root": root, "report": report, "runtime": runtime, "prior_root": prior_root}


class RecheckSupplementTests(unittest.TestCase):
    def test_new_actual_affected_source_projects_exact_bytes_with_original_pointer_graph(self):
        value = make_supplement_fixture(self)
        before = {str(path): path.read_bytes() for path in value["root"].rglob("*") if path.is_file()}
        with patch("subprocess.run", side_effect=AssertionError("validator called a process")) as process:
            projected = validate_recheck_supplement(**value["arguments"])
        process.assert_not_called()
        self.assertEqual("affected", projected["binding"]["scope"])
        self.assertFalse(projected["binding"]["whole_package"])
        self.assertNotIn("execution_routing", projected["report"])
        self.assertEqual(3, len(projected["report"]["defect_resolution"]["evidence"]))
        self.assertTrue(all(not Path(row["path"]).is_absolute() for row in projected["report"]["defect_resolution"]["evidence"]))
        self.assertEqual(8, len(projected["sources"]))
        reused = projected["sources"]["/defect_resolution/evidence/0"]
        self.assertEqual(value["report"]["defect_resolution"]["evidence"][0], reused[0]["actual_value"])
        self.assertEqual("/defect_recheck/0/evidence/0", reused[1]["json_pointer"])
        self.assertEqual(before, {str(path): path.read_bytes() for path in value["root"].rglob("*") if path.is_file()})

    def test_bad_current_path_sha_ambiguity_or_missing_payload_cannot_be_normalized(self):
        cases = [lambda report: report["defect_resolution"]["evidence"][0].update(path="/foreign/source.md"),
                 lambda report: report["defect_resolution"]["evidence"][0].update(sha256="f" * 64),
                 lambda report: report["defect_resolution"]["evidence"][0].update(package_path="../outside"),
                 lambda report: report["defect_resolution"]["evidence"].append(copy.deepcopy(report["defect_resolution"]["evidence"][0])),
                 lambda report: report["defect_resolution"]["evidence"].pop()]
        for mutate in cases:
            value = make_supplement_fixture(self, mutate)
            with self.subTest(mutate=mutate), self.assertRaises(TaskflowError):
                validate_recheck_supplement(**value["arguments"])

    def test_reuse_rejects_foreign_pointer_sha_candidate_scope_and_raw_history_mutation(self):
        cases = [lambda report: report["prior_judgment_reuse"]["reused_file_evidence"][0].update(json_pointer="/execution_routing"),
                 lambda report: report["prior_judgment_reuse"]["reused_file_evidence"].append(copy.deepcopy(report["prior_judgment_reuse"]["reused_file_evidence"][0])),
                 lambda report: report["prior_judgment_reuse"].update(sha256="f" * 64),
                 lambda report: report["prior_judgment_reuse"].update(candidate_id="foreign-candidate"),
                 lambda report: report["native_signoff"]["dimensions"]["business"]["reused_judgment"].update(json_pointer="/native_signoff/dimensions/artifact"),
                 lambda report: report["prior_judgment_reuse"].update(historic_whole_package_judgment_reissued=True)]
        for mutate in cases:
            value = make_supplement_fixture(self, mutate)
            with self.subTest(mutate=mutate), self.assertRaises(TaskflowError):
                validate_recheck_supplement(**value["arguments"])
        value = make_supplement_fixture(self)
        original = value["prior_root"] / "artifacts/adoption-readiness.json"
        original.write_bytes(original.read_bytes() + b"\n")
        with self.assertRaises(TaskflowError):
            validate_recheck_supplement(**value["arguments"])

    def test_closed_defects_independence_native_affirmations_and_associations_stay_strict(self):
        cases = [lambda report: report["execution_routing"].update(covered_defect_ids=["foreign-defect"]),
                 lambda report: report["execution_routing"].update(independent_of_current_author=False),
                 lambda report: report["execution_routing"].update(scope="whole_package"),
                 lambda report: report["native_signoff"]["dimensions"]["artifact"].update(affirmed=False),
                 lambda report: report["native_signoff"].update(normative={"files": []}),
                 lambda report: report["defect_resolution"]["necessary_associations"].pop(),
                 lambda report: report["defect_resolution"]["necessary_associations"][0].update(disposition="unresolved"),
                 lambda report: report["defect_resolution"]["necessary_associations"][0].update(old_sha256="f" * 64),
                 lambda report: report["coverage"]["changed_files"][0].update(old="f" * 64),
                 lambda report: report["defect_resolution"]["necessary_associations"][0].update(necessity="")]
        for mutate in cases:
            value = make_supplement_fixture(self, mutate)
            with self.subTest(mutate=mutate), self.assertRaises(TaskflowError):
                validate_recheck_supplement(**value["arguments"])
        value = make_supplement_fixture(self, reuse_sid=True)
        with self.assertRaisesRegex(TaskflowError, "independent post-recheck identity"):
            validate_recheck_supplement(**value["arguments"])
        value = make_supplement_fixture(self)
        value["arguments"]["attempt"]["state"] = "running"
        with self.assertRaisesRegex(TaskflowError, "actual completed independent"):
            validate_recheck_supplement(**value["arguments"])

    def test_parent_alias_requires_explicit_same_original_binding(self):
        value = make_supplement_fixture(self, use_parent_alias=True)
        arguments = value["arguments"]
        self.assertTrue(validate_recheck_supplement(**arguments)["report"])
        with self.assertRaisesRegex(TaskflowError, "parent source alias"):
            validate_recheck_supplement(**{**arguments, "parent_aliases": None})
        alias = next(iter(arguments["parent_aliases"]))
        with self.assertRaisesRegex(TaskflowError, "parent source alias"):
            validate_recheck_supplement(**{**arguments, "parent_aliases": {alias: "/foreign/captured"}})
        Path(alias).write_text("Different parent input bytes.", encoding="utf-8")
        with self.assertRaisesRegex(TaskflowError, "captured original bytes"):
            validate_recheck_supplement(**arguments)


if __name__ == "__main__":
    unittest.main()
