"""Actual neutral research ends, separate baseline quality; no provider calls."""

import copy
import contextlib
import io
import sys
import shutil
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution, execution_author, execution_runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_baseline_quality import (
    KIND, PACKET, baseline_flow_id, closed_research_source,
    start_baseline_quality_from_packet,
)
from taskflow.execution_flow import _identity
from taskflow.execution_lifecycle import advance_lifecycle, run_lifecycle
from taskflow.execution_experiment import _author_source
from taskflow.execution_store import evidence_ref
from tests import test_execution_batch_author as batches
from tests import test_execution_flow as flows
from tests import test_execution_normative_certification as certificates
from taskflow.execution_formal import validate_normative_certification
from taskflow.execution_baseline_quality import baseline_experiment_source, validate_frozen_baseline_source


class BaselineQualityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = batches.BatchAuthorTests()
        self.addCleanup(self.fixture.doCleanups)
        case = self.fixture.author()
        self.case, self.store, self.root = case, case.store, case.root
        case.actual_run = execution_runner.run_attempt
        with patch("taskflow.execution_author.enqueue_spec", side_effect=case.fixture_enqueue), \
                patch("taskflow.execution_runner.run_attempt", side_effect=case.actual_fixture_process):
            result = execution_author.start_author_from_packet(self.store, **case.kwargs(True))
        self.source = result["lifecycle"]
        self.assertEqual("research_ended", self.source["state"]["result"]["outcome"])
        self.material = closed_research_source(self.store, self.source["flow_id"])
        proof = self.material["proof"]
        self.packet = {"schema_version": 1, "kind": PACKET, "controller": "fixture-controller",
            "batch_id": self.material["batch"]["batch_id"], "no_new_author": True,
            "scope": "fresh_public_then_whole_quality", "formal_experiment": "separate_after_quality",
            "items": [{"task_id": "neutral", "source_author_quality_flow": self.source["flow_id"],
                "source_definition_digest": self.source["definition_digest"],
                "source_author_attempt": proof["source_author_attempt"], "source_author_sid": proof["source_author_sid"],
                "baseline_binding": proof["baseline_binding"], "source_terminal_receipt": proof["evidence"][0],
                "purpose": "Independently check the captured neutral baseline after research ended."}]}
        self.packet_path, self.factory_path = self.root / "baseline-purpose.json", self.root / "baseline-factory.json"
        write_json(self.packet_path, self.packet)
        write_json(self.factory_path, case.config)

    def start(self):
        return start_baseline_quality_from_packet(self.store,
            packet_ref=evidence_ref(self.packet_path, "SIMULATED baseline business choice"),
            factory_config_ref=evidence_ref(self.factory_path, "SIMULATED original batch infrastructure"))

    def run_quality(self, outcome="accepted", reused_sid=False):
        """Actual neutral subprocesses; public adapter transport is a test seam."""
        case = self.case
        role_script = self.root / "fresh-quality.py"
        script = flows.ROLE_FIXTURE
        if reused_sid:
            script = script.replace("sid=str(uuid.uuid4())", "sid=" + repr(self.material["author"]["cli_sid"]))
        role_script.write_text(script)
        original_roles = execution_author._roles
        original_enqueue = execution.enqueue_spec
        public_root = self.root / "neutral-public"
        (public_root / "work").mkdir(parents=True)

        def roles(store, definition):
            result = original_roles(store, definition)
            # The neutral public process has no adapter-owned inner trees.
            # Production still uses the original full archive transport.
            for role in result.values():
                role.pop("predecessor_inputs", None)
            return result

        def enqueue(store, spec, runtime, **kwargs):
            spec = copy.deepcopy(spec)
            runner = spec["runner"]
            if spec["work"]["model_slot"] == "public_rebuild":
                runner.update(mode="test_fixture", cwd=str(public_root / "work"),
                              argv=[str(Path(sys.executable).resolve()), str(case.public_script)])
                runner.pop("isolated", None)
            else:
                runner.update(mode="test_fixture", argv=[str(Path(sys.executable).resolve()), str(role_script),
                    "--context", str(runtime / "inputs/execution/current.json"),
                    "--inputs", str(runtime / "inputs"), "--action", "quality", "--scope", "whole_package",
                    "--outcome", outcome], outputs=["intake.md", "issues.json", "adoption-readiness.json"])
                runner["dependencies"].append(str(role_script))
            return original_enqueue(store, spec, runtime, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch("taskflow.execution_author._roles", side_effect=roles))
            stack.enter_context(patch("taskflow.execution_author._public_prepare", return_value={"runtime": str(public_root)}))
            stack.enter_context(patch("taskflow.execution_author.enqueue_spec", side_effect=enqueue))
            stack.enter_context(patch("taskflow.execution.enqueue_spec", side_effect=enqueue))
            stack.enter_context(patch("taskflow.execution_runner.run_attempt", side_effect=case.actual_fixture_process))
            record = self.start()
            self.addCleanup(shutil.rmtree, execution_author._public_root(record), True)
            return run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.01)

    def experiment_item(self, record):
        author = self.material["author"]
        return {"source_author_flow_kind": KIND,
            "source_baseline_binding": self.material["proof"]["baseline_binding"],
            "source_author_attempt": author["attempt_id"], "source_author_intent": author["intent_id"],
            "source_author_sid": author["cli_sid"], "source_business_packet": str(self.packet_path),
            "source_business_packet_sha256": record["definition"]["authorization"]["sha256"]}

    def experiment_record(self, record):
        return {"definition": {"source_author_flow_id": record["flow_id"],
            "task_id": "neutral", "owner": "fixture-controller", "policy_id": "batch-v1"},
            "state": {"history": []}}

    def test_actual_public_whole_chain_receives_baseline_without_author_delivery_or_old_quality(self):
        record = self.run_quality()
        self.assertEqual("accepted", record["state"]["status"], record["state"])
        result = record["state"]["result"]
        self.assertEqual("existing_captured_baseline", result["basis_kind"])
        self.assertFalse(result["candidate_created_by_source_author"])
        self.assertEqual(self.material["candidate"]["candidate_id"], result["candidate_id"])
        attempts = self.store.dashboard()["attempts"]
        self.assertEqual(["author", "public_rebuild", "quality"], [a["model_slot"] for a in attempts])
        self.assertEqual(3, len({a["cli_sid"] for a in attempts}))
        self.assertEqual(self.source, self.store.get_flow(self.source["flow_id"]))
        bound = _author_source(self.store, self.experiment_record(record), self.experiment_item(record))
        self.assertEqual(KIND, bound["binding"]["kind"])
        self.assertEqual(self.material["proof"], validate_frozen_baseline_source(bound["binding"]["baseline_source"]))
        self.assertEqual(record, self.start())
        self.assertEqual(3, len(self.store.dashboard()["attempts"]))

    def test_inconclusive_whole_quality_does_not_become_experiment_source(self):
        record = self.run_quality("inconclusive")
        self.assertEqual("exception", record["state"]["status"], record["state"])
        with self.assertRaisesRegex(TaskflowError, "unresolved"):
            baseline_experiment_source(self.store, self.experiment_record(record), self.experiment_item(record))
        self.assertEqual(3, len(self.store.dashboard()["attempts"]))

    def test_original_research_sid_cannot_sign_independent_quality(self):
        record = self.run_quality(reused_sid=True)
        self.assertEqual("exception", record["state"]["status"], record["state"])
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT COUNT(*) FROM reviews").fetchone()[0])

    def test_drift_after_registration_stops_before_preparation(self):
        record = self.start()
        path = self.case.task / "draft/input/prompt.md"
        path.write_text(path.read_text() + "changed baseline")
        with patch("taskflow.execution_author._public_prepare", side_effect=AssertionError("must not prepare")):
            record = advance_lifecycle(self.store, record["flow_id"])
        self.assertEqual("exception", record["state"]["status"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_native_boundary_transports_original_research_proof_and_offline_rejects_mixing_sources(self):
        # Native-shaped byte-sealed fixture only, no native CLI/model claim.
        record = self.start()
        helper = certificates.NormativeCertificationTests()
        self.addCleanup(helper.doCleanups)
        source = helper.fixture()
        proof = self.material["proof"]
        author = self.material["author"]
        old_id = source.parent_id
        source.parent_id = record["flow_id"]
        source.flows[source.parent_id] = source.flows.pop(old_id)
        source.parent["flow_id"] = source.parent_id
        source.parent["definition"].update(kind=KIND, owner="fixture-controller", policy_id="batch-v1",
            source_author_attempt=author["attempt_id"], source_author_binding=record["definition"]["source_author_binding"],
            authorization=record["definition"]["authorization"])
        source.attempts[author["attempt_id"]] = author
        source.child["definition"]["owner"] = "fixture-controller"
        receipt_path = Path(source.child["state"]["receipt"])
        body = read_json(receipt_path)
        body["controller"] = "fixture-controller"
        write_json(receipt_path, body)
        source.item.update(self.experiment_item(record), source_author_quality_flow=source.parent_id)
        write_json(source.authority, {"schema_version": 1, "controller": "fixture-controller", "items": [source.item]})
        source.authorization = evidence_ref(source.authority, "neutral native boundary fixture")
        with patch("taskflow.execution_baseline_quality.baseline_experiment_source", return_value={"binding": {
                "baseline_source": proof}}) as boundary:
            certification = helper.certification(source)
        self.assertTrue(boundary.call_args.kwargs["require_candidate"])
        native = certification["native_quality_handoff"]
        self.assertEqual(proof, native["certification"]["baseline_source"])
        self.assertEqual(native["normative_sources"], validate_normative_certification(candidate=source.candidate,
            certification=certification))
        for field in ("source_author_attempt", "source_author_sid", "baseline_binding", "source_flow_id"):
            bad = copy.deepcopy(certification)
            mutated = bad["native_quality_handoff"]["certification"]["baseline_source"]
            mutated[field] = "foreign" if field != "baseline_binding" else {}
            bad["native_quality_handoff"]["protocol_map"]["native_signoff"]["baseline_source"] = mutated
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                validate_normative_certification(candidate=source.candidate, certification=bad)

    def test_pending_quality_waits_without_work_and_requires_typed_source(self):
        record = self.start()
        definition = {**self.experiment_record(record)["definition"], "flow_id": "future-experiment"}
        experiment = self.store.register_flow("future-experiment", definition, [record["definition"]["authorization"]],
            {"status": "active", "stage": "experiment_v1", "history": [], "archives": [], "waiting": None})
        item = self.experiment_item(record)
        waiting = _author_source(self.store, experiment, item)["record"]
        self.assertEqual("source_baseline_quality_not_received", waiting["state"]["waiting"]["reason"])
        self.assertEqual(waiting, _author_source(self.store, waiting, item)["record"])
        for wrong in ({**item, "source_author_flow_kind": "author_quality_v1"},
                      {key: value for key, value in item.items() if key != "source_author_flow_kind"},
                      {**item, "source_baseline_binding": {}}):
            with self.assertRaises(TaskflowError):
                _author_source(self.store, waiting, wrong)
        with self.assertRaisesRegex(TaskflowError, "not yet accepted"):
            _author_source(self.store, waiting, item, require_candidate=True)
        self.assertEqual(1, len(self.store.dashboard()["work"]))

    def test_retired_baseline_cli_does_not_prepare_or_mutate(self):
        before = self.store.events()
        missing_db = self.root / "must-not-exist.sqlite3"
        with contextlib.redirect_stderr(io.StringIO()), patch("taskflow.execution_store.ExecutionStore") as store:
            with self.assertRaises(SystemExit) as error:
                execution.main(["--db", str(missing_db), "batch-baseline-quality-start", "--packet", str(self.packet_path), "--factory", str(self.factory_path)])
        self.assertEqual(2, error.exception.code)
        store.assert_not_called()
        self.assertFalse(missing_db.exists())
        self.assertEqual(before, self.store.events())

    def test_terminal_source_receipt_drift_cannot_prepare_a_successor(self):
        record = self.start()
        receipt = Path(self.material["proof"]["evidence"][0]["path"])
        data = read_json(receipt)
        data["result"]["candidate_created"] = True
        write_json(receipt, data)
        with patch("taskflow.execution_author._public_prepare", side_effect=AssertionError("changed original must stop")):
            stopped = advance_lifecycle(self.store, record["flow_id"])
        self.assertEqual("exception", stopped["state"]["status"])
        with self.assertRaises(TaskflowError):
            validate_frozen_baseline_source(self.material["proof"])
        self.assertEqual(1, len(self.store.dashboard()["attempts"]))

    def test_overlapping_or_aliased_output_cannot_write_into_original_evidence(self):
        factory = copy.deepcopy(self.case.config)
        for root in (self.case.task, Path(self.source["definition"]["directory"])):
            factory["lifecycle_root"] = str(root)
            write_json(self.factory_path, factory)
            with self.subTest(root=root), self.assertRaisesRegex(TaskflowError, "preserve original roots"):
                self.start()
        factory["lifecycle_root"] = str(self.root / "alias-output")
        alias = Path(factory["lifecycle_root"]) / baseline_flow_id(self.source["flow_id"])
        alias.parent.mkdir()
        alias.symlink_to(Path(self.source["definition"]["directory"]), target_is_directory=True)
        write_json(self.factory_path, factory)
        with self.assertRaisesRegex(TaskflowError, "preserve original roots"):
            self.start()
        self.assertEqual(1, len(self.store.list_flows()))

    def test_once_only_registration_preserves_original_author_and_does_not_prepare_or_dispatch(self):
        before = self.store.dashboard()
        protected = {r["path"]: Path(r["path"]).read_bytes() for r in self.material["proof"]["evidence"]}
        with patch("taskflow.execution_author._public_prepare", side_effect=AssertionError("register only")), \
                patch("taskflow.execution.run_worker", side_effect=AssertionError("no worker")):
            first, second = self.start(), self.start()
        self.assertEqual(first, second)
        self.assertEqual(KIND, first["definition"]["kind"])
        self.assertTrue(first["definition"]["simulation_only"])
        self.assertEqual(self.material["candidate"]["candidate_id"], first["state"]["candidate_id"])
        self.assertEqual("public_prepare", first["state"]["stage"])
        self.assertEqual(before["work"], self.store.dashboard()["work"])
        self.assertEqual(before["attempts"], self.store.dashboard()["attempts"])
        self.assertEqual(self.source, self.store.get_flow(self.source["flow_id"]))
        self.assertTrue(all(Path(p).read_bytes() == raw for p, raw in protected.items()))

    def test_wrong_source_baseline_batch_or_business_scope_rejects_without_a_successor(self):
        for changes in ({"batch_id": "foreign-batch"}, {"no_new_author": False}, {"scope": "skip_public"}):
            with self.subTest(changes=changes), self.assertRaises(TaskflowError):
                write_json(self.packet_path, {**self.packet, **changes})
                self.start()
        write_json(self.packet_path, self.packet)
        raw = self.packet["items"][0]
        for key, value in (("source_definition_digest", "0" * 64), ("source_author_sid", "foreign-sid"),
                           ("baseline_binding", {**raw["baseline_binding"], "content_digest": "0" * 64})):
            wrong = copy.deepcopy(self.packet)
            wrong["items"][0][key] = value
            write_json(self.packet_path, wrong)
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                self.start()
        self.assertEqual([self.source["flow_id"]], [r["flow_id"] for r in self.store.list_flows()])

    def test_concurrent_registration_and_changed_intent_do_not_create_another_chain(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.start(), range(2)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(2, len(self.store.list_flows()))
        self.packet["items"][0]["purpose"] = "A different unconfirmed business purpose."
        write_json(self.packet_path, self.packet)
        with self.assertRaisesRegex(TaskflowError, "cannot be replaced"):
            self.start()

    def test_long_original_ids_produce_distinct_work_and_child_identities(self):
        left, right = "same-prefix-" * 8 + "one", "same-prefix-" * 8 + "two"
        a, b = baseline_flow_id(left), baseline_flow_id(right)
        self.assertLess(len(a), 48)
        self.assertNotEqual(_identity(a, "public-work"), _identity(b, "public-work"))
        self.assertNotEqual(_identity(a, "quality-flow"), _identity(b, "quality-flow"))


if __name__ == "__main__":
    unittest.main()
