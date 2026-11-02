"""Original-public observations survive actual private repair and composition.

These are actual neutral runner subprocesses and sealed archives, not model or
native permission proofs. The native prior-source reader is exercised directly;
its outer real-CLI identity boundary is covered by the existing native suites.
"""

import copy
import json
from pathlib import Path
import unittest

from taskflow.contracts import TaskflowError, read_json, sha256
from taskflow.execution_formal import _prior_whole_findings
from taskflow.execution_runner import RunnerError, _info, _validate
from taskflow.execution_store import ExecutionStore
from tests import test_execution_blocking_observation_basis as basis_fixtures
from tests import test_execution_composite_quality as composite_fixtures
from tests.test_execution_flow import ROLE_FIXTURE


class BlockingObservationCompositionTests(unittest.TestCase):
    def closed(self):
        source = basis_fixtures.BlockingObservationBasisTests(
            "test_two_real_blocking_findings_are_stored_and_original_repair_is_prepared_once")
        source.setUp()
        self.addCleanup(source.doCleanups)
        # The fixture declares native coverage before its original subprocess;
        # no delivered report, manifest or collection is rewritten after sealing.
        raw = source.script.read_text()
        source.script.write_text(raw.replace("'native_signoff':{'dimensions'",
                                             "'native_signoff':{'coverage':'whole_package','dimensions'"))
        store, case = source.store, source.case
        # All role dependencies must be final BEFORE the first runner locks
        # them; the reusable fixture's first generated package is bounded-1.
        baseline = str(case.root / "bounded-1/task.json")
        prefix = '''
import pathlib,json,hashlib
def signed_report(route,ctx,inputs):
 report={'execution_routing':route}
 if route['scope']=='affected':
  baseline=pathlib.Path(BASELINE).parent
  declaration=json.loads((baseline/'task.json').read_text())
  changed=[];evidence=[]
  for item in declaration['files']:
   name=item['path'];old=hashlib.sha256((baseline/name).read_bytes()).hexdigest();new=hashlib.sha256((inputs/name).read_bytes()).hexdigest()
   if old!=new:
    changed.append({'path':name,'old':old,'new':new})
    evidence.append({'path':name,'sha256':new,'location':'actual current private correction'})
  report['coverage']={'changed_files':changed}
  report['defect_resolution']={'affected_paths':[x['path'] for x in changed],'evidence':evidence}
 return report
'''.replace("BASELINE", repr(baseline))
        repair = ROLE_FIXTURE.replace("{'execution_routing':route}", "signed_report(route,ctx,inputs)")
        repair = repair.replace("save(root/'task.json',task)",
                                "(root/'task.json').write_bytes((inputs/'execution/task.json').read_bytes())")
        extra = """
 other='draft/evaluation/rubric.md';before_other=digest(root/other);(root/other).write_text('Actual corrected private rubric.\\n')
 record=json.loads(pathlib.Path('repair-record.json').read_text());record['changes'].append({'path':other,'before_sha256':before_other,'after_sha256':digest(root/other)});save('repair-record.json',record)
"""
        repair = repair.replace(" pathlib.Path('handoff.md').write_text", extra + " pathlib.Path('handoff.md').write_text")
        case.script.write_text(prefix + repair)
        pending, attempt, runtime = source.consume("valid")
        self.assertEqual("repair", pending["state"]["stage"])
        parent = store.get_candidate(pending["state"]["candidate_id"])
        original = {path: sha256((runtime / path).read_bytes()) for path in
                    ("manifest.json", "request.md", "collection.json", "artifacts/adoption-readiness.json", "artifacts/issues.json")}
        case.run_flow([pending["flow_id"]])
        flow = store.get_flow(pending["flow_id"])
        self.assertEqual("accepted", flow["state"]["status"])
        history = [row for row in flow["state"]["history"] if row.get("kind") == "independent_review"]
        self.assertEqual(2, len(history))
        self.assertNotEqual(history[0]["cli_sid"], history[1]["cli_sid"])
        return {"store": store, "case": case, "flow": flow, "attempt": attempt, "root": runtime,
                "parent": parent, "original": original, "whole": history[0]["review_id"],
                "recheck": history[1]["review_id"], "candidate": flow["state"]["candidate_id"],
                "defects": flow["state"]["repaired_defects"]}

    def compose(self, value):
        return value["store"].compose_repaired_quality(value["candidate"], value["whole"], value["recheck"],
                    value["defects"], evidence=value["case"].refs)

    def state(self, value, mutate):
        record = value["store"].get_flow(value["flow"]["flow_id"])
        state = copy.deepcopy(record["state"])
        mutate(state)
        return value["store"].update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral-negative-metadata"})

    def test_real_repair_recheck_composition_and_native_prior_reader_reuse_original_public_archive(self):
        value = self.closed()
        # A later public cursor is deliberately unsuitable. Historical source
        # selection must retain the actual adopted predecessor, not this cursor.
        self.state(value, lambda state: state.update(public_archive=state["history"][-1]["archive"]))
        result = self.compose(value)
        self.assertFalse(result["whole_package"])
        self.assertEqual(2, len(result["composite_quality_coverage"]["private_changes"]))
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))
        self.assertEqual(result, value["store"].assert_review_current(result["review_id"]))
        with value["store"]._db() as db:
            original = ExecutionStore._composition_review(db, value["store"].get_review(value["whole"]), "whole_package")
        root = value["root"]
        native = _prior_whole_findings(value["parent"], value["attempt"], _validate(root), root,
            {"adoption": {"path": str(root / "artifacts/adoption-readiness.json"), **_info(root / "artifacts/adoption-readiness.json")},
             "issues": {"path": str(root / "artifacts/issues.json"), **_info(root / "artifacts/issues.json")}}, original["report"])
        self.assertEqual("revision_required", native["outcome"])
        self.assertEqual(["PRIVATE-REFERENCE", "PRIVATE-RUBRIC"], native["blocking_issue_ids"])
        self.assertEqual([False, True, False], [native["original_dimensions"][key]["affirmed"]
                                              for key in ("business", "artifact", "calculation")])
        self.assertEqual("defects", value["store"].get_review(value["whole"])["verdict"])
        self.assertEqual(value["original"], {path: sha256((root / path).read_bytes()) for path in value["original"]})

    def test_wrong_review_sid_and_duplicate_or_foreign_history_are_rejected(self):
        for variant in ("sid", "duplicate", "foreign"):
            with self.subTest(variant=variant):
                value = self.closed()
                def mutate(state):
                    signed = next(row for row in state["history"] if row.get("review_id") == value["whole"])
                    if variant == "sid":signed["cli_sid"] = "foreign-signer"
                    elif variant == "duplicate":state["history"].append(copy.deepcopy(signed))
                    else:state["history"].remove(signed)
                self.state(value, mutate)
                with self.assertRaisesRegex(TaskflowError, "history"):
                    self.compose(value)

    def test_copied_review_history_under_a_foreign_flow_cannot_supply_context(self):
        for preserve_scope in (False, True):
            with self.subTest(preserve_scope=preserve_scope):
                value = self.closed()
                original = value["store"].get_flow(value["flow"]["flow_id"])
                copied = copy.deepcopy(original["state"])
                definition = copy.deepcopy(original["definition"])
                definition["flow_id"] = "foreign-history-flow"
                if preserve_scope:
                    scope = value["store"].get_review(value["whole"])["scope"]
                    for role in definition["roles"].values():role["scope_id"] = scope
                value["store"].register_flow(definition["flow_id"], definition, value["case"].refs, copied)
                self.state(value, lambda state: state.update(history=[row for row in state["history"]
                                  if row.get("review_id") != value["whole"]]))
                with self.assertRaisesRegex(TaskflowError, "context"):
                    self.compose(value)

    def test_original_archive_membership_and_public_role_history_are_required(self):
        for variant in ("membership", "public_history", "archive_peer"):
            with self.subTest(variant=variant):
                value = self.closed()
                def mutate(state):
                    public = next(row for row in state["history"] if row.get("kind") == "adopted_predecessor")
                    if variant == "membership":state["archives"].remove(public["archive"])
                    elif variant == "public_history":public["kind"] = "unverified_archive"
                    else:
                        peer = value["case"].root / "same-byte-peer"
                        import shutil
                        shutil.copytree(public["archive"], peer)
                        state["archives"] = [str(peer) if path == public["archive"] else path for path in state["archives"]]
                        public["archive"] = str(peer)
                self.state(value, mutate)
                with self.assertRaisesRegex(TaskflowError, "public archive|public role"):
                    self.compose(value)

    def test_public_attempt_role_sid_and_foreign_owner_context_are_rejected(self):
        for variant in ("role", "public_sid", "owner"):
            with self.subTest(variant=variant):
                value = self.closed()
                with value["store"]._db(True) as db:
                    if variant == "owner":
                        row = db.execute("SELECT definition FROM execution_flows WHERE flow_id=?", (value["flow"]["flow_id"],)).fetchone()
                        definition = json.loads(row["definition"]);definition["owner"] = "foreign-controller"
                        db.execute("UPDATE execution_flows SET definition=? WHERE flow_id=?", (json.dumps(definition), value["flow"]["flow_id"]))
                    else:
                        public = next(row for row in value["flow"]["state"]["history"] if row.get("kind") == "adopted_predecessor")
                        row = db.execute("SELECT data FROM attempts WHERE id=?", (public["attempt_id"],)).fetchone()
                        attempt = json.loads(row["data"])
                        attempt["role" if variant == "role" else "cli_sid"] = "repair" if variant == "role" else value["attempt"]["cli_sid"]
                        db.execute("UPDATE attempts SET data=? WHERE id=?", (json.dumps(attempt), public["attempt_id"]))
                with self.assertRaisesRegex(TaskflowError, "foreign role|public role"):
                    self.compose(value)

    def test_locked_control_presence_kind_source_and_bytes_are_required(self):
        value = self.closed()
        root = value["root"]
        original = _validate(root)
        review = value["store"].get_review(value["whole"])
        issues = read_json(root / "artifacts/issues.json")
        # These are direct argument-boundary checks on the read-only resolver;
        # they do not relabel forged manifests as successful outer validation.
        for variant in ("missing", "kind", "foreign_source", "staged"):
            with self.subTest(variant=variant):
                manifest = copy.deepcopy(original)
                control = next(row for row in manifest["inputs"] if row["target"] == "execution/current.json")
                if variant == "missing":manifest["inputs"].remove(control)
                elif variant == "kind":control["kind"] = "teacher"
                elif variant == "foreign_source":
                    peer = value["case"].root / "same-byte-foreign-current.json"
                    peer.write_bytes(Path(control["source"]).read_bytes())
                    control["source"] = str(peer)
                else:
                    staged = root / "inputs/execution/current.json"
                    staged.chmod(0o644)
                    payload = read_json(staged);payload["flow_id"] = "foreign-source-flow"
                    staged.write_text(json.dumps(payload))
                with value["store"]._db() as db, self.assertRaisesRegex(TaskflowError, "context"):
                    ExecutionStore._composition_observations(db, review, value["parent"], value["attempt"], manifest, root, issues)

    def test_original_archive_source_and_actual_staged_bytes_are_rechecked(self):
        for variant in ("archive_manifest", "source", "staged"):
            with self.subTest(variant=variant):
                value = self.closed()
                manifest = read_json(value["root"] / "manifest.json")
                observed = next(row for row in manifest["inputs"] if row["kind"] == "submission")
                public = next(row for row in value["flow"]["state"]["history"] if row.get("kind") == "adopted_predecessor")
                path = (Path(public["archive"]) / "manifest.json" if variant == "archive_manifest" else
                        Path(observed["source"]) if variant == "source" else value["root"] / "inputs" / observed["target"])
                path.chmod(0o644)  # Deliberate neutral-fixture corruption of a staged read-only file.
                path.write_bytes(path.read_bytes() + b"\nchanged")
                with self.assertRaises((TaskflowError, RunnerError, ValueError)):
                    self.compose(value)

    def test_no_observation_composition_keeps_legacy_history_independence(self):
        fixture = composite_fixtures.CompositeQualityTests()
        fixture.addCleanup = self.addCleanup
        value = fixture.make_closed_flow()
        self.state(value, lambda state: state["history"].extend(copy.deepcopy(state["history"])))
        result = fixture.compose(value)
        self.assertFalse(result["whole_package"])
        self.assertEqual(result, value["store"].quality_gate(value["candidate"]))


if __name__ == "__main__":
    unittest.main()
