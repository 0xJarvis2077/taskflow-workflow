"""Real neutral workers across a public revision and its private continuation."""

from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from taskflow.contracts import TaskflowError, sha256, write_json
from taskflow.execution import capture_candidate, enqueue_spec, run_worker
from taskflow.execution_experiment import _source
from taskflow.execution_flow import _identity, advance_workflow, resume_workflow
from taskflow.execution_lifecycle import resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.helpers import make_task
from tools.execution_simulation import fixture_policy


def make_coverage_chain_fixture(testcase):
    temp = tempfile.TemporaryDirectory()
    testcase.addCleanup(temp.cleanup)
    root = Path(temp.name).resolve()
    store = ExecutionStore(root / "state.sqlite3")
    store.register_policy("fixture-v1", fixture_policy())
    task = make_task(root)
    original = capture_candidate(store, task, "p0")
    authority = root / "authority.json"
    write_json(authority, {"nature": "neutral schema fixture; zero models"})
    authority_refs = [evidence_ref(authority, "neutral continuation fixture")]
    flow_id, directory = "neutral-coverage-flow", root / "flow"
    cli = Path(__file__).resolve().parents[1] / "tools/mock_role_cli.py"

    def binding(candidate):
        return {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}

    def cite(candidate, path):
        row = next(item for item in candidate["files"] if item["package_path"] == path)
        return {"path": path, "sha256": row["sha256"], "location": "line 1, complete neutral payload"}

    def source_evidence(candidate, rows):
        paths = {row["package_path"]: row["path"] for row in candidate["files"]}
        return [{**row, "path": paths[row["path"]]} for row in rows]

    def actor(candidate, number, stage, payload):
        node = _identity(flow_id, f"{number}-{stage}")
        node_root = directory / "nodes" / f"{number}-{stage}"
        work = node_root / "work"
        work.mkdir(parents=True)
        payload_path = node_root / "payload.json"
        write_json(payload_path, payload)
        declaration = next(row for row in candidate["evidence"] if row.get("location") == "explicit package declaration")
        runner = {"intent_id": node, "policy_version": "fixture-v1", "role": "repair" if stage == "repair" else "quality",
            "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
            "cwd": str(work), "inputs": [{"path": declaration["path"], "kind": "source_metadata", "target": "execution/task.json"}],
            "outputs": list(payload), "dependencies": [str(cli), str(payload_path)],
            "request_sections": [{"kind": "operation", "text": "Neutral schema worker only; zero model calls."}],
            "argv": [str(Path(sys.executable).resolve()), str(cli), "--payload", str(payload_path), "--sid", str(uuid.uuid4())]}
        enqueue_spec(store, {"work": {"work_id": node, "task_id": "neutral", "candidate_id": candidate["candidate_id"],
            "role": runner["role"], "policy_id": "fixture-v1", "intent_id": node, "purpose": "neutral explicit continuation"},
            "runner": runner}, node_root / "runtime")
        return run_worker(store, "fixture-worker", True, [node])

    def quality(candidate, number, stage, defects, covered, extra=None):
        node = _identity(flow_id, f"{number}-{stage}")
        scope = "affected" if stage == "recheck" else "whole_package"
        route = {"schema_version": 1, **{key: value for key, value in binding(candidate).items() if key != "task_id"},
            "intent_id": node, "signer_role": "independent_quality", "independent_of_current_author": True,
            "participated_in_production": False, "scope": scope, "outcome": "revision_required" if defects else "accepted",
            "blocking_issue_ids": [row["id"] for row in defects], "covered_defect_ids": covered, "reason": "Only the signed neutral scope."}
        report = {"execution_routing": route, **(extra or {})}
        issues = {"execution_binding": {**{key: value for key, value in binding(candidate).items() if key != "task_id"}, "intent_id": node},
                  "issues": defects}
        attempt = actor(candidate, number, stage, {"adoption-readiness.json": json.dumps(report),
            "issues.json": json.dumps(issues), "intake.md": "Neutral independent routing, not business/model certification."})
        runtime = Path(attempt["runtime_path"])
        refs = [evidence_ref(runtime / "artifacts" / name, "actual original neutral role source")
                for name in ("adoption-readiness.json", "issues.json")]
        review = store.record_review(_identity(flow_id, node + "-review"), candidate["candidate_id"], attempt["attempt_id"],
            verdict="defects" if defects else "accepted", scope="neutral authorized quality", evidence=refs,
            whole_package=scope == "whole_package", covered_defect_ids=covered)
        return attempt, review, refs

    def issue(identity, kind, responsibility, candidate, path):
        return {"id": identity, "kind": kind, "responsibility": responsibility,
                "affected_paths": [path], "evidence": [cite(candidate, path)]}

    old_issues = [issue("OLD-M", "material", "Restore the public delegated choice.", original, "draft/input/prompt.md"),
                  issue("OLD-R", "reference", "Repair the private supporting explanation.", original, "draft/evaluation/answer.md")]
    first_attempt, first, _ = quality(original, 0, "quality", old_issues, [])
    old_ids = ["old-material", "old-reference"]
    for identity, row in zip(old_ids, old_issues):
        store.open_defect(identity, original["candidate_id"], first["review_id"], responsibility=row["responsibility"],
                          evidence=source_evidence(original, row["evidence"]), kind=row["kind"])

    def repair(parent, number, candidate_id, changes, defects):
        declaration = next(row for row in parent["evidence"] if row.get("location") == "explicit package declaration")
        payload = {"revised-task/neutral/task.json": Path(declaration["path"]).read_text()}
        for row in parent["files"]:
            payload["revised-task/neutral/" + row["package_path"]] = changes.get(row["package_path"], Path(row["path"]).read_text())
        old = {row["package_path"]: row for row in parent["files"]}
        record = {"parent_id": parent["candidate_id"], "candidate_id": candidate_id, "defect_ids": defects,
            "needs_business_decision": False, "changes": [{"path": path, "before_sha256": old[path]["sha256"],
                "after_sha256": sha256(text.encode())} for path, text in changes.items()]}
        payload.update({"repair-record.json": json.dumps(record), "handoff.md": "Actual neutral repaired delivery."})
        attempt = actor(parent, number, "repair", payload)
        source = Path(attempt["runtime_path"]) / "artifacts/revised-task/neutral"
        candidate = capture_candidate(store, source, candidate_id, parent["candidate_id"], attempt["attempt_id"])
        ref = [evidence_ref(Path(attempt["runtime_path"]) / "artifacts/repair-record.json", "actual complete managed repair")]
        for identity in defects:
            defect = store.get_defect(identity)
            disposition = "material_repair" if defect["kind"] == "material" else "private_repair"
            store.record_repair(identity, candidate_id, evidence=ref, disposition=disposition)
        return attempt, candidate

    first_repair, anchor = repair(original, 1, "p1", {
        "draft/input/prompt.md": "Capacity is 3. Independently choose 2 feasible units.\n",
        "draft/evaluation/answer.md": "The private support preserves the required independent choice.\n"}, old_ids)
    new_issue = issue("NEW-R", "reference", "Repair the remaining private rule support.", anchor, "draft/evaluation/rubric.md")
    anchor_rows = [{"defect_id": identity, "finding": "The specified responsibility was rechecked.",
        "evidence": [cite(anchor, "draft/input/prompt.md" if index == 0 else "draft/evaluation/answer.md")],
        "actual_coverage": "Actual complete neutral revised payload reviewed.", "limit": "Only this specified duty; a separate new finding remains."}
        for index, identity in enumerate(old_ids)]
    anchor_attempt, whole, _ = quality(anchor, 3, "recheck_public", [new_issue], old_ids, {"rechecked_defects": anchor_rows})
    new_id = "new-reference"
    store.open_defect(new_id, anchor["candidate_id"], whole["review_id"], responsibility=new_issue["responsibility"],
                      evidence=source_evidence(anchor, new_issue["evidence"]), kind=new_issue["kind"])
    second_repair, candidate = repair(anchor, 4, "p2", {"draft/evaluation/rubric.md": "C1 remains 7 points with actual private rule support.\n"}, [new_id])
    changed = "draft/evaluation/rubric.md"
    all_ids = [*old_ids, new_id]
    current_evidence = cite(candidate, changed)
    per_defect = [{"defect_id": identity, "disposition": "resolved_retained" if index < 2 else "resolved",
                  "evidence": [cite(candidate, path)]}
                 for index, (identity, path) in enumerate(zip(all_ids, ("draft/input/prompt.md", "draft/evaluation/answer.md", changed)))]
    final_extra = {"defect_resolution": {"affected_paths": [changed], "evidence": [current_evidence], "per_defect": per_defect},
                  "coverage": {"parent_binding": binding(anchor), "current_binding": binding(candidate),
                    "changed_files": [{"path": changed, "old": cite(anchor, changed)["sha256"], "new": current_evidence["sha256"]}]}}
    final_attempt, final, refs = quality(candidate, 5, "recheck", [], all_ids, final_extra)
    history = [{"kind": "independent_review", "attempt_id": first_attempt["attempt_id"], "review_id": first["review_id"],
                "candidate_id": original["candidate_id"], "cli_sid": first_attempt["cli_sid"], "outcome": "revision_required"},
               {"kind": "actual_repair", "attempt_id": first_repair["attempt_id"], "candidate_id": anchor["candidate_id"],
                "cli_sid": first_repair["cli_sid"], "public_changed": True},
               {"kind": "independent_review", "attempt_id": anchor_attempt["attempt_id"], "review_id": whole["review_id"],
                "candidate_id": anchor["candidate_id"], "cli_sid": anchor_attempt["cli_sid"], "outcome": "revision_required"},
               {"kind": "actual_repair", "attempt_id": second_repair["attempt_id"], "candidate_id": candidate["candidate_id"],
                "cli_sid": second_repair["cli_sid"], "public_changed": False}]
    definition = {"schema_version": 1, "flow_id": flow_id, "task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
        "directory": str(directory), "accepted_use": "Neutral research asset only, zero model claim.",
        "initial": {"candidate_id": original["candidate_id"], "candidate_root": str(task)},
        "roles": {name: {"runner": {"role": "repair" if name == "repair" else "quality"},
                          "scope": "whole_package" if name in {"quality", "recheck_public"} else "affected"}
                  for name in ("quality", "repair", "recheck_public", "recheck")}}
    exception = {"reason": "closure requires whole-package review or explicit covered same-public private recheck", "work_id": final_attempt["work_id"]}
    receipt = directory / "original-closure-exception.json"
    write_json(receipt, {"flow_id": flow_id, "status": "exception", "exception": exception})
    failed = store.register_flow(flow_id, definition, authority_refs, {"status": "exception", "stage": "recheck", "step": 5,
        "candidate_id": candidate["candidate_id"], "candidate_root": str(Path(candidate["evidence"][0]["path"]).parent),
        "current_node_id": final_attempt["intent_id"], "current_work_id": final_attempt["work_id"], "current_runtime": final_attempt["runtime_path"],
        "initialized": True, "history": history, "archives": [], "receipt": str(receipt), "exception": exception,
        "defects": [{**new_issue, "defect_id": new_id, "source_review_id": whole["review_id"]}],
        "repaired_defects": all_ids, "seen_findings": ["OLD-M", "OLD-R", "NEW-R"]})
    return {"store": store, "case": SimpleNamespace(root=root, store=store), "original": original,
            "anchor_candidate": anchor, "candidate": candidate["candidate_id"], "candidate_record": candidate,
            "whole": whole["review_id"], "original_review": first["review_id"], "recheck": final["review_id"], "final": final["review_id"],
            "defects": old_ids, "new_defect": new_id, "all_defects": all_ids, "refs": refs, "flow": failed,
            "final_attempt": final_attempt, "authority_refs": authority_refs}


class CoverageChainIntegrationTests(unittest.TestCase):
    def accept_child(self, value):
        store, failed = value["store"], value["flow"]
        resume_workflow(store, failed["flow_id"], reason="Only the existing exact signed coverage chain is now supported.", evidence=value["refs"])
        consumed = advance_workflow(store, failed["flow_id"])
        self.assertEqual("accepted", consumed["state"]["status"], consumed["state"].get("exception"))
        return advance_workflow(store, failed["flow_id"])

    def test_explicit_resume_consumes_original_completed_recheck_and_closes_all_actual_duties(self):
        value = make_coverage_chain_fixture(self)
        store, failed = value["store"], value["flow"]
        before = store.dashboard()["attempts"]
        original_receipt = Path(failed["state"]["receipt"]).read_bytes()
        accepted = self.accept_child(value)
        self.assertTrue(accepted["state"]["receipt"].endswith("result.json"))
        self.assertEqual(before, store.dashboard()["attempts"])
        self.assertEqual(original_receipt, Path(failed["state"]["receipt"]).read_bytes())
        for identity in value["defects"]:
            chain = store.validate_defect_coverage_chain(identity)
            self.assertEqual(value["whole"], chain["repair_review_id"])
            self.assertEqual(value["recheck"], chain["final_review_id"])
        self.assertNotIn("closure_coverage_chain", store.get_defect(value["new_defect"]))
        composed = store.compose_repaired_quality(value["candidate"], value["whole"], value["recheck"], value["all_defects"], evidence=value["refs"])
        self.assertFalse(composed["whole_package"])
        self.assertEqual(2, len(composed["composite_quality_coverage"]["inherited_closure_chains"]))
        self.assertEqual("defects", store.get_review(value["whole"])["verdict"])

    def test_parent_accepts_only_the_explicitly_resolved_original_completed_closure(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_real_graph_uses_the_current_direct_parent_whole_without_raising_its_verdict(self):
        value = make_coverage_chain_fixture(self)
        store, root = value["store"], value["case"].root
        child = self.accept_child(value)
        composite = store.compose_repaired_quality(value["candidate"], value["whole"], value["recheck"], value["all_defects"], evidence=value["refs"])
        definition = {"kind": "experiment_v1", "flow_id": "source-selection", "task_id": "neutral", "policy_id": "fixture-v1",
                      "source_author_flow_id": "actual-source-author", "technical": {}, "authorization": value["authority_refs"][0]}
        record = store.register_flow(definition["flow_id"], definition, value["authority_refs"], {
            "status": "active", "stage": "experiment_v1", "current_work_id": None, "history": [], "archives": []})
        bound = {"source": child, "initial": value["original"], "author_sids": {"original-source-author"},
                 "binding": {"source_author_flow_id": "actual-source-author"}}
        source_refs = {}
        for prefix, identity in (("", value["final_attempt"]["attempt_id"]), ("whole_", store.get_review(value["whole"])["attempt_id"])):
            artifacts = Path(store.get_attempt(identity)["runtime_path"]) / "artifacts"
            source_refs.update({prefix + name: evidence_ref(artifacts / filename, "actual neutral original source") for name, filename in
                                (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md"))})
        native_interface = {"source_refs": source_refs, "certification": {"composite_quality_coverage": composite["composite_quality_coverage"]},
                            "nature": "explicit native protocol interface fixture; role validators tested separately"}
        with patch("taskflow.execution_experiment._author_source", return_value=bound), \
             patch("taskflow.execution_experiment._native_handoff", return_value=native_interface):
            result = _source(store, record, {"quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True}})
        self.assertEqual("release", result["state"]["stage"])
        self.assertEqual(composite["review_id"], result["state"]["accepted_review_id"])
        self.assertEqual(value["anchor_candidate"]["candidate_id"], composite["composite_quality_coverage"]["whole_source"]["candidate_id"])
        self.assertEqual("defects", store.get_review(value["whole"])["verdict"])
        self.assertFalse(composite["whole_package"])
        self.assertFalse((root / "publication").exists())


if __name__ == "__main__":
    unittest.main()
