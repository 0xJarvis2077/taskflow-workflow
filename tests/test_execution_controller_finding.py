"""Actual neutral source/repair/public/recheck cycle; no real model or OS proof."""

import copy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_controller_finding import current_after_controller_reopen, reopen_controller_public_finding
from taskflow.execution_experiment import _node
from taskflow.execution_flow import _consumer_lock, _receipt, advance_workflow, register_workflow, run_workflows
from taskflow.execution_lifecycle import _lifecycle_lock, _receive, advance_lifecycle
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_author as author_fixtures
from tests import test_execution_flow as flow_fixtures
from tests import test_execution_composite_quality as composite_fixtures
from tests import test_execution_experiment_author as experiment_fixtures
from tools.execution_simulation import perform


def make_controller_finding_fixture(owner):
    author = author_fixtures.AuthorSuccessorTests()
    author.setUp()
    owner.addCleanup(author.doCleanups)
    author.run_source("candidate_complete")
    parent = author.register()
    parent = advance_lifecycle(author.store, parent["flow_id"])
    parent = advance_lifecycle(author.store, parent["flow_id"])
    owner.assertEqual("public_prepare", parent["state"]["stage"])
    candidate = author.store.get_candidate(parent["state"]["candidate_id"])
    chain = author.root / "normal-chain"
    chain.mkdir()
    script = chain / "role.py"
    raw = flow_fixtures.ROLE_FIXTURE.replace("if a.action=='repair':", "if a.action=='public':\n pathlib.Path('public.txt').write_text('Actual unscored neutral public delivery.')\nelif a.action=='repair':")
    raw = raw.replace("changed='draft/evaluation/answer.md';before=digest(root/changed);(root/changed).write_text('Actual repaired private support.\\n')",
                      "changed='draft/input/prompt.md';before=digest(root/changed);(root/changed).write_text('Establish the required choice from the retained capacity facts.\\n')")
    script.write_text(raw)
    definition = flow_fixtures.ContinuationTests.definition(SimpleNamespace(root=chain, script=script), "neutral", outcome="accepted")
    definition["initial"].update(candidate_id=candidate["candidate_id"], candidate_root=parent["state"]["candidate_root"])
    public = perform(author.store, author.root / "actual-first-public", candidate["candidate_id"], "quality", "independent-first-public", {"public.txt": "Actual initial unscored public reconstruction."})
    definition["initial"].update(predecessor_attempt=public["attempt_id"],
        predecessor_binding={key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")},
        expected_digests={key: candidate[key] for key in ("input_digest", "content_digest", "evaluation_digest")})
    public_role = copy.deepcopy(definition["roles"]["quality"])
    public_role["runner"]["outputs"] = ["public.txt"]
    args = public_role["runner"]["argv"]
    args[args.index("--action") + 1] = "public"
    definition["roles"]["public_rebuild"] = public_role
    definition["roles"]["recheck_public"] = copy.deepcopy(definition["roles"]["quality"])
    register_workflow(author.store, definition, author.refs)
    run_workflows(author.store, [definition["flow_id"]], allow_model=True, poll_seconds=.01)
    child = author.store.get_flow(definition["flow_id"])
    owner.assertEqual("accepted", child["state"]["status"])
    state = copy.deepcopy(parent["state"])
    state.update(stage="child_quality", current_work_id=None, child_flow_id=child["flow_id"],
                 public_attempt_id=public["attempt_id"], public_archive=child["state"]["public_archive"])
    parent = author.store.update_flow(parent["flow_id"], parent["revision"], state, {"kind": "neutral_adopted_actual_public_and_quality"})
    parent = advance_lifecycle(author.store, parent["flow_id"])
    parent = advance_lifecycle(author.store, parent["flow_id"])
    owner.assertEqual("accepted", parent["state"]["status"])
    experiment_fixture = experiment_fixtures.AuthorExperimentTests()
    experiment_fixture.root = author.root
    experiment_definition = experiment_fixture.definition(author, parent)
    experiment_definition.update(flow_id="held-original-experiment", directory=str(author.root / "held-experiment"))
    experiment_definition["technical"].update(formal_runtime_root=str(author.root / "held-formal"),
                                              publication_root=str(author.root / "held-publication"))
    experiment = author.store.register_flow("held-original-experiment", experiment_definition, author.refs, {
        "status": "exception", "stage": "experiment_v1", "current_work_id": None,
        "exception": {"reason": "Independent historical-negative transport remains unresolved"},
        "history": [], "archives": []})
    target = _node(experiment, "solver")
    author.store.set_control("work:" + target, "pause", "Known controller finding; only this future purpose is held")
    name = "draft/input/prompt.md"
    item = next(row for row in candidate["files"] if row["package_path"] == name)
    finding = author.root / "controller-finding.json"
    write_json(finding, {"schema_version": 1, "finding_id": "CONTROLLER-PUBLIC-ONE",
        "source_flow": parent["flow_id"], "candidate_id": candidate["candidate_id"],
        "candidate_root": child["state"]["candidate_root"], "experiment_flow": experiment["flow_id"],
        "source_role": "authorized independent content monitor", "source_thread_id": "neutral-controller-source",
        "exact_public_findings": [{"path": item["path"], "package_path": name, "sha256": item["sha256"],
                                   "quote": Path(item["path"]).read_text().strip()}],
        "monitor_requested_disposition": "Repair this actual public preclassification while retaining necessary capacity facts and lawful alternatives.",
        "root_business_decision": "This new current public finding must be resolved through actual repair and appropriate independent public/whole recheck before the held purpose."})
    args = {"parent_flow_id": parent["flow_id"], "controller": parent["definition"]["owner"],
        "finding_ref": evidence_ref(finding, "actual controller-received current public finding"),
        "expected_parent_revision": parent["revision"], "expected_child_revision": child["revision"],
        "expected_experiment_revision": experiment["revision"],
        "expected_candidate_binding": {key: candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
        "expected_review_id": child["state"]["accepted_review_id"]}
    return {"author": author, "store": author.store, "parent": parent, "child": child,
            "candidate": candidate, "experiment": experiment, "target": target, "finding": finding, "args": args}


class ControllerFindingTests(unittest.TestCase):
    def fixture(self):
        return make_controller_finding_fixture(self)

    def test_actual_normal_cycle_keeps_original_acceptance_and_versions_both_results(self):
        value = self.fixture()
        store, parent, child = value["store"], value["parent"], value["child"]
        original_review = store.get_review(child["state"]["accepted_review_id"])
        source = Path(store.get_attempt(original_review["attempt_id"])["runtime_path"])
        paths = [Path(parent["state"]["receipt"]), Path(child["state"]["receipt"]), value["finding"]]
        paths += [source / name for name in ("manifest.json", "request.md", "collection.json", "artifacts/adoption-readiness.json", "artifacts/issues.json")]
        before = {path: sha256(path.read_bytes()) for path in paths}
        result = reopen_controller_public_finding(store, **value["args"])
        self.assertEqual("reopened", result["status"])
        self.assertEqual("accepted", store.get_review(original_review["review_id"])["verdict"])
        self.assertEqual("authorized_controller_finding", result["defect"]["finding_origin"]["kind"])
        self.assertEqual(original_review["review_id"], result["defect"]["review_id"])
        self.assertEqual("repair", result["child"]["state"]["stage"])
        previous = result["child"]["state"]["history"][-1]["previous_acceptance"]
        self.assertNotIn("state", previous)
        self.assertEqual(sha256(canonical(child["state"])), previous["state_digest"])
        self.assertEqual(child["definition_digest"], result["child"]["definition_digest"])
        self.assertEqual(parent["definition_digest"], result["parent"]["definition_digest"])
        self.assertEqual(result["child"], _receipt(store, child))
        self.assertEqual(result["parent"], _receive(store, parent))
        events = store.events()
        self.assertEqual("already_registered", reopen_controller_public_finding(store, **value["args"])["status"])
        self.assertEqual(events, store.events())
        with self.assertRaises(TaskflowError):store.quality_gate(value["candidate"]["candidate_id"])
        run_workflows(store, [child["flow_id"]], allow_model=True, poll_seconds=.01)
        final_child = store.get_flow(child["flow_id"])
        self.assertEqual("accepted", final_child["state"]["status"], final_child["state"])
        self.assertTrue(Path(final_child["state"]["receipt"]).name.startswith("result-"))
        self.assertNotEqual(child["state"]["candidate_id"], final_child["state"]["candidate_id"])
        repaired = [row for row in final_child["state"]["history"] if row.get("kind") == "actual_repair"][-1]
        self.assertTrue(repaired["public_changed"])
        self.assertTrue(any(row.get("kind") == "public_reconstruction" for row in final_child["state"]["history"]))
        fresh = store.get_review(final_child["state"]["accepted_review_id"])
        self.assertTrue(fresh["whole_package"])
        self.assertNotEqual(original_review["attempt_id"], fresh["attempt_id"])
        defect = store.get_defect(result["defect"]["defect_id"])
        self.assertEqual("closed", defect["state"])
        self.assertEqual(fresh["review_id"], defect["closure_review_id"])
        with store._db() as db:ExecutionStore._assert_closed_quality_record(db, defect)
        final_parent = advance_lifecycle(store, parent["flow_id"])
        final_parent = advance_lifecycle(store, parent["flow_id"])
        self.assertEqual("accepted", final_parent["state"]["status"])
        self.assertTrue(Path(final_parent["state"]["receipt"]).name.startswith("result-"))
        self.assertEqual(final_child, _receipt(store, child))
        self.assertEqual(final_parent, _receive(store, parent))
        self.assertEqual(value["experiment"], store.get_flow(value["experiment"]["flow_id"]))
        self.assertFalse(any(row["work_id"] == value["target"] for row in store.dashboard()["attempts"]))
        self.assertEqual(before, {path: sha256(path.read_bytes()) for path in paths})
        composite_fixtures.CompositeQualityTests().later_negative({"store": store, "case": value["author"],
            "candidate": final_child["state"]["candidate_id"], "refs": value["author"].refs})
        with self.assertRaisesRegex(TaskflowError, "later independent"):_receipt(store, child)
        with self.assertRaisesRegex(TaskflowError, "later independent"):_receive(store, parent)

    def test_resumed_active_initial_experiment_stays_unchanged_and_held_during_source_reopen(self):
        value = self.fixture();store = value["store"]
        old = value["experiment"]
        state = copy.deepcopy(old["state"]);original_exception = state.pop("exception")
        state.update(status="active", history=[{"kind": "neutral_exact_technical_resume", "original_exception": original_exception}])
        current = store.update_flow(old["flow_id"], old["revision"], state, {"kind": "neutral_exact_technical_resume"})
        value["args"]["expected_experiment_revision"] = current["revision"]
        with store._db() as db:
            control = db.execute("SELECT data FROM controls WHERE scope=?", ("work:" + value["target"],)).fetchone()[0]
        result = reopen_controller_public_finding(store, **value["args"])
        self.assertEqual("reopened", result["status"])
        self.assertEqual(current, store.get_flow(current["flow_id"]))
        self.assertEqual(current, result["held_experiment"])
        self.assertFalse(any(row["work_id"] == value["target"] for row in store.dashboard()["attempts"]))
        with store._db() as db:
            self.assertEqual(control, db.execute("SELECT data FROM controls WHERE scope=?", ("work:" + value["target"],)).fetchone()[0])

    def test_owner_revision_original_receipt_and_candidate_binding_are_required(self):
        value = self.fixture()
        store = value["store"]
        for change in ({"controller": "foreign-controller"}, {"expected_parent_revision": 0},
                       {"expected_child_revision": 0}, {"expected_experiment_revision": 99},
                       {"expected_candidate_binding": {**value["args"]["expected_candidate_binding"], "input_digest": "0" * 64}}):
            events = store.events()
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                reopen_controller_public_finding(store, **{**value["args"], **change})
            self.assertEqual(events, store.events())
        path = Path(value["parent"]["state"]["receipt"])
        raw = path.read_bytes();payload = read_json(path);payload["controller"] = "foreign";write_json(path, payload)
        try:
            with self.assertRaisesRegex(TaskflowError, "receipt differs"):
                reopen_controller_public_finding(store, **value["args"])
        finally:path.write_bytes(raw)

    def test_foreign_same_bytes_private_targets_wrong_sha_and_absent_quotes_reject(self):
        value = self.fixture()
        original = read_json(value["finding"])
        for variant in ("foreign_path", "private", "sha", "quote", "source_flow"):
            payload = copy.deepcopy(original);row = payload["exact_public_findings"][0]
            if variant == "foreign_path":
                peer = value["author"].root / "same-byte-foreign.md";peer.write_bytes(Path(row["path"]).read_bytes());row["path"] = str(peer)
            elif variant == "private":
                item = next(row for row in value["candidate"]["files"] if row["role"] == "answer")
                row.update(path=item["path"], package_path=item["package_path"], sha256=item["sha256"], quote=Path(item["path"]).read_text().strip())
            elif variant == "sha":row["sha256"] = "0" * 64
            elif variant == "quote":row["quote"] = "No such current source sentence"
            else:payload["source_flow"] = "foreign-flow"
            write_json(value["finding"], payload)
            args = {**value["args"], "finding_ref": evidence_ref(value["finding"], "neutral deliberately invalid finding")}
            events = value["store"].events()
            with self.subTest(variant=variant), self.assertRaises(TaskflowError):
                reopen_controller_public_finding(value["store"], **args)
            self.assertEqual(events, value["store"].events())

    def test_target_pause_and_no_active_or_uncertain_source_role_are_required(self):
        value = self.fixture();store = value["store"]
        store.set_control("work:" + value["target"], "resume", "Neutral negative target is not held")
        with self.assertRaisesRegex(TaskflowError, "must remain paused"):
            reopen_controller_public_finding(store, **value["args"])
        store.set_control("work:" + value["target"], "pause", "Original exact purpose held again")
        store.enqueue("active-neutral-quality", "neutral", value["candidate"]["candidate_id"], "quality", "fixture-v1",
            intent_id="active-neutral-intent", purpose="Preexisting independent role, do not supersede it.", request_digest="0" * 64, evidence=value["author"].refs)
        attempt = store.claim("existing-neutral-worker", ["active-neutral-quality"])
        self.assertIsNotNone(attempt)
        with self.assertRaisesRegex(TaskflowError, "active or uncertain"):
            reopen_controller_public_finding(store, **value["args"])
        store.mark_uncertain(attempt["attempt_id"], "Original process observation unknown")
        with self.assertRaisesRegex(TaskflowError, "active or uncertain"):
            reopen_controller_public_finding(store, **value["args"])
        self.assertEqual(value["parent"], store.get_flow(value["parent"]["flow_id"]))

    def test_one_sqlite_transaction_rolls_back_defect_and_both_flow_changes(self):
        value = self.fixture();store = value["store"]
        before = store.events();actual = ExecutionStore.update_flow
        def fail_parent(self, identity, revision, state, event):
            if identity == value["parent"]["flow_id"] and event.get("kind") == "controller_finding_reopened":
                raise TaskflowError("Neutral storage failure after real child CAS")
            return actual(self, identity, revision, state, event)
        with patch.object(ExecutionStore, "update_flow", new=fail_parent), self.assertRaisesRegex(TaskflowError, "storage failure"):
            reopen_controller_public_finding(store, **value["args"])
        self.assertEqual(before, store.events())
        self.assertEqual(value["parent"], store.get_flow(value["parent"]["flow_id"]))
        self.assertEqual(value["child"], store.get_flow(value["child"]["flow_id"]))
        with store._db() as db:self.assertEqual(0, db.execute("SELECT count(*) FROM defects").fetchone()[0])

    def test_original_consumer_lock_and_unrelated_prior_receipt_cannot_be_bypassed(self):
        value = self.fixture();store = value["store"]
        before = store.events()
        for flow in (value["parent"], value["child"], value["experiment"]):
            with self.subTest(flow=flow["flow_id"]), _lifecycle_lock(store, flow["flow_id"]), \
                 self.assertRaisesRegex(TaskflowError, "already has a consumer"):
                reopen_controller_public_finding(store, **value["args"])
            self.assertEqual(before, store.events())
        with _consumer_lock(store), self.assertRaises(TaskflowError):
            reopen_controller_public_finding(store, **value["args"])
        self.assertEqual(before, store.events())
        child = value["child"]
        state = copy.deepcopy(child["state"])
        state.update(status="active", prior_receipts=[evidence_ref(Path(child["state"]["receipt"]), "unrelated copied old receipt")])
        store.update_flow(child["flow_id"], child["revision"], state, {"kind": "neutral-unrelated-state-change"})
        self.assertIsNone(current_after_controller_reopen(store, child))

    def test_preexisting_decision_and_actual_formal_paths_are_not_a_zero_launch_boundary(self):
        value = self.fixture();store = value["store"]
        technical = value["experiment"]["definition"]["technical"]
        for key in ("formal_runtime_root", "publication_root"):
            path = Path(technical[key]);path.mkdir()
            try:
                before = store.events()
                with self.subTest(path=key), self.assertRaisesRegex(TaskflowError, "publication or preparation"):
                    reopen_controller_public_finding(store, **value["args"])
                self.assertEqual(before, store.events())
            finally:path.rmdir()
        store.record_decision("preexisting-experiment-purpose", value["candidate"]["task_id"], value["candidate"]["candidate_id"],
            action="experiment", rationale="A preexisting separately authorized purpose.", experiment_purpose="Separate purpose before intake.",
            authorized=True, evidence=value["author"].refs)
        before = store.events()
        with self.assertRaisesRegex(TaskflowError, "existing experiment decision"):
            reopen_controller_public_finding(store, **value["args"])
        self.assertEqual(before, store.events())

    def test_preexisting_unresolved_responsibility_is_not_replaced_by_the_new_finding(self):
        value = self.fixture();store = value["store"]
        file = next(row for row in value["candidate"]["files"] if row["role"] == "prompt")
        store.open_defect("already-open", value["candidate"]["candidate_id"], value["args"]["expected_review_id"],
            responsibility="A separate current responsibility remains unresolved.",
            evidence=[evidence_ref(Path(file["path"]), "another actual current public responsibility")])
        before = store.events()
        with self.assertRaisesRegex(TaskflowError, "unresolved responsibility"):
            reopen_controller_public_finding(store, **value["args"])
        self.assertEqual(before, store.events())


if __name__ == '__main__':
    unittest.main()
