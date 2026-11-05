"""Explicit teacher links consume sealed neutral originals, never real models.

Original facts/score use the actual two-stage ingestion/seals/full archives.
The existing native author enqueue seam changes only execution/probe transport
to a real local fixture process; no native isolation or business pass is claimed.
"""

import copy
import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from taskflow import execution, execution_runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_lifecycle import advance_lifecycle, register_lifecycle, run_lifecycle
from taskflow.execution_store import evidence_ref
from taskflow.execution_teacher_successor import _predecessor, start_teacher_successors_from_packet
from tests import test_execution_author_factory as author_fixtures
from tests import test_execution_review as review_fixtures


def teacher_fixture(owner):
    author = author_fixtures.AuthorFactoryTests()
    author.setUp()
    owner.addCleanup(author.doCleanups)
    author.factory()
    review = review_fixtures.CleanReviewPreparationTests()
    review.setUp()
    owner.addCleanup(review.doCleanups)
    config, score = review.fixture_managed_score()
    predecessor_authority = author.root / "original-receive-authority.json"
    write_json(predecessor_authority, {"controller": "fixture-controller", "task_id": "neutral"})
    predecessor = register_lifecycle(author.store, {"schema_version": 1, "kind": "evaluation_tail_v1",
        "flow_id": "unrelated-to-any-receipt-path", "task_id": "neutral", "owner": "fixture-controller",
        "policy_id": "fixture-v1", "authorization": evidence_ref(predecessor_authority, "neutral actual original intake"),
        "directory": str(author.root / "predecessor-receive"), "review": config},
        [evidence_ref(predecessor_authority, "neutral actual original intake")])
    predecessor = advance_lifecycle(author.store, predecessor["flow_id"])
    predecessor = advance_lifecycle(author.store, predecessor["flow_id"])
    owner.assertEqual("accepted", predecessor["state"]["status"], predecessor["state"])
    accepted = predecessor["state"]["result"]
    owner.assertTrue(accepted["accepted"])
    owner.assertEqual("test_fixture", accepted["nature"])
    facts = Path(accepted["review_archive"]["facts"]["path"]) / "artifacts/facts.json"
    raw_score = Path(accepted["review_archive"]["score"]["path"]) / "artifacts/first-score.json"
    registration = author.root / "original-registration.json"
    write_json(registration, {"schema_version": 1, "kind": "neutral_controller_original_registration",
        "receipt": evidence_ref(Path(accepted["receipt"]), "accepted original receipt"),
        "sealed_facts": evidence_ref(facts, "actual first factual artifact"),
        "raw_score": evidence_ref(raw_score, "actual same-SID original score"),
        "evaluation_record": evidence_ref(Path(accepted["evaluation"]) / "record.original.json", "original source record"),
        "original": {"run_id": read_json(review.run / "run.json")["run_id"],
                     "source_sha256": accepted["source_sha256"], "record_digest": accepted["record_digest"]}})
    author.packet_data["items"][0]["most_relevant"] = [evidence_ref(facts, "actual original facts; preserve provenance and limits")]
    write_json(author.packet, author.packet_data)
    author.packet_ref = evidence_ref(author.packet, "original custom producer location retained")
    factory = author.root / "author-factory.json"
    write_json(factory, author.config)
    item = {"predecessor_run": read_json(review.run / "run.json")["run_id"],
        "predecessor_original_score": score["total_score"],
        "predecessor_receipt": evidence_ref(Path(accepted["receipt"]), "accepted original"),
        "registered_original": evidence_ref(registration, "actual controller receipt registration"),
        "author_intent": author.packet_data["items"][0]["intent_id"],
        "author_packet": evidence_ref(author.packet, "explicit next research packet")}
    bundle = author.root / "teacher-business.json"
    data = {"schema_version": 1, "kind": "explicit_bounded_post_original_teacher_business_successors",
        "owner": "fixture-controller", "batch_scope": "Explicit neutral existing-policy continuation, no score threshold.",
        "author_factory": evidence_ref(factory, "exact original factory"), "items": [item]}
    write_json(bundle, data)
    return author, review, predecessor, accepted, bundle, data


class TeacherSuccessorTests(unittest.TestCase):
    def setUp(self):
        self.author, self.review, self.predecessor, self.accepted, self.bundle, self.data = teacher_fixture(self)
        self.store = self.author.store
        self.directory = self.author.root / "teacher-links"

    def start(self, allow_model=False):
        return start_teacher_successors_from_packet(self.store, packet_ref=evidence_ref(self.bundle, "explicit neutral teacher successors"),
            directory=self.directory, allow_model=allow_model)

    def test_original_receive_fact_then_score_and_actual_new_author_request_are_bound_without_dispatch(self):
        before = self.store.list_attempts("neutral-does-not-exist")
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue), \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("tick cannot dispatch")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("no allow-model")):
            result = self.start()
        record = result["items"][0]["record"]
        self.assertEqual(self.predecessor["flow_id"], record["definition"]["predecessor"]["flow_id"])
        self.assertEqual("ready", self.store.get_work(record["state"]["current_work_id"])["state"])
        runtime = Path(record["state"]["prepared_author"]["runtime"])
        manifest = read_json(runtime / "manifest.json")
        actual_packet = next(row for row in manifest["inputs"] if row["target"] == "protocol/current-author-packet.json")
        self.assertEqual(evidence_ref(self.author.packet, "source")["sha256"], actual_packet["sha256"])
        body = (runtime / "request.md").read_text()
        self.assertIn("actual original facts; preserve provenance and limits", body)
        self.assertNotIn("OLD_Q1_BUSINESS_SENTINEL", body)
        self.assertEqual(before, self.store.list_attempts(record["definition"]["author_work_id"]))
        self.assertEqual(0, result["models_dispatched_by_preparation"])
        self.assertFalse(result["score_threshold_policy"])
        request = (runtime / "request.md").read_bytes()
        with patch("taskflow.execution_author.enqueue_spec", side_effect=AssertionError("must reuse ready request")):
            again = self.start()
        self.assertEqual(record["flow_id"], again["items"][0]["flow_id"])
        self.assertEqual(request, (runtime / "request.md").read_bytes())

    def test_preexisting_claimed_author_uses_original_ref_location_and_never_dispatches_or_launches(self):
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            original = self.author.kwargs()
            from taskflow.execution_author import start_author_from_packet
            prepared = start_author_from_packet(self.store, **original)
        attempt = self.store.claim("original-actual-owner", [prepared["work_id"]])
        directory = Path(prepared["factory_intent"]).parent
        protected = {p: p.read_bytes() for p in (directory / "factory-intent.json", directory / "spec.json", directory / "runtime/request.md")}
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("adoption must not dispatch")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("existing source must retain consumer")):
            result = self.start(allow_model=True)
        record = result["items"][0]["record"]
        self.assertEqual("accepted", record["state"]["status"])
        self.assertEqual("existing_managed_author_adopted", record["state"]["result"]["outcome"])
        self.assertEqual(attempt["attempt_id"], record["state"]["result"]["attempt_id"])
        self.assertEqual(self.author.packet_ref["location"], record["definition"]["author_packet"]["location"])
        self.assertEqual(attempt, self.store.get_attempt(attempt["attempt_id"]))
        for path, raw in protected.items():
            self.assertEqual(raw, path.read_bytes())

    def test_explicit_model_permission_uses_only_the_existing_normal_consumer_launcher(self):
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue), \
             patch("taskflow.execution_experiment_factory.launch_lifecycle_consumer", return_value={"nature": "not_spawned_fixture"}) as launch, \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("start must not directly call a model")):
            result = self.start(allow_model=True)
        launch.assert_called_once()
        self.assertEqual(result["items"][0]["flow_id"], launch.call_args.args[1]["flow_id"])
        self.assertNotIn("resume_dead", launch.call_args.kwargs)
        self.assertEqual([], self.store.list_attempts(result["items"][0]["record"]["definition"]["author_work_id"]))

    def test_a_current_teacher_consumer_lock_prevents_repreparation_or_a_new_launch(self):
        from taskflow.execution_lifecycle import _lifecycle_lock

        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            record = self.start()["items"][0]["record"]
        with _lifecycle_lock(self.store, record["flow_id"]), \
             patch("taskflow.execution_teacher_successor.start_author_from_packet", side_effect=AssertionError("original tick already owned")), \
             patch("taskflow.execution_experiment_factory.launch_lifecycle_consumer", side_effect=AssertionError("original consumer retained")):
            again = self.start(allow_model=True)
        self.assertEqual("consumer_lock_held", again["items"][0]["execution"])
        self.assertEqual(record, again["items"][0]["record"])

    def pending_original(self, stage):
        """Stop a real existing factory before work enqueue, retaining its intent."""
        from taskflow.execution_author import start_author_from_packet

        self.author.directory = Path(self.author.config["research_root"]) / self.author.packet_data["items"][0]["intent_id"]
        target = "taskflow.execution_author.check_task" if stage == "intent" else "taskflow.execution_author.enqueue_spec"
        with patch(target, side_effect=TaskflowError("neutral original preparation interrupted before work")):
            with self.assertRaisesRegex(TaskflowError, "preparation interrupted"):
                start_author_from_packet(self.store, **self.author.kwargs())
        return self.author.directory

    def test_existing_factory_intent_without_work_only_waits_for_its_original_owner(self):
        directory = self.pending_original("intent")
        intent = directory / "factory-intent.json"
        original = intent.read_bytes()
        self.assertFalse((directory / "spec.json").exists())
        with patch("taskflow.execution_teacher_successor.start_author_from_packet", side_effect=AssertionError("must not repeat original preparation")), \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("must not claim")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("must not launch a consumer")):
            first = self.start(allow_model=True)
            again = self.start(allow_model=True)
        record = first["items"][0]["record"]
        self.assertTrue(record["definition"]["preexisting_author"])
        self.assertEqual("active", record["state"]["status"])
        self.assertEqual("existing_author_preparation_not_received", record["state"]["waiting"]["reason"])
        self.assertIsNone(record["state"]["current_work_id"])
        self.assertNotIn("prepared_author", record["state"])
        self.assertEqual(record, again["items"][0]["record"])
        self.assertEqual(original, intent.read_bytes())
        self.assertEqual([], self.store.list_attempts(record["definition"]["author_work_id"]))
        self.assertIsNone(first["items"][0]["consumer"])
        from taskflow.execution_author import start_author_from_packet
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            prepared = start_author_from_packet(self.store, **self.author.kwargs())
        original_attempt = self.store.claim("original-factory-owner", [prepared["work_id"]])
        with patch("taskflow.execution_teacher_successor.start_author_from_packet", side_effect=AssertionError("link only adopts observed work")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("original consumer retained")):
            adopted = self.start(allow_model=True)["items"][0]["record"]
        self.assertEqual("accepted", adopted["state"]["status"])
        self.assertEqual(original_attempt["attempt_id"], adopted["state"]["result"]["attempt_id"])
        self.assertEqual(original, intent.read_bytes())

    def test_an_intent_appearing_between_registration_and_tick_cannot_be_reprepared(self):
        actual = advance_lifecycle
        def original_owner_arrives(store, flow_id):
            if not (Path(self.author.config["research_root"]) / self.author.packet_data["items"][0]["intent_id"] / "factory-intent.json").exists():
                self.pending_original("intent")
            return actual(store, flow_id)
        with patch("taskflow.execution_lifecycle.advance_lifecycle", side_effect=original_owner_arrives), \
             patch("taskflow.execution_teacher_successor.start_author_from_packet", side_effect=AssertionError("race must not reprepare")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("race must not launch")):
            result = self.start(allow_model=True)
        record = result["items"][0]["record"]
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("already owns an intent", record["state"]["exception"]["reason"])
        self.assertIsNone(record["state"]["current_work_id"])

    def test_existing_failed_preparation_with_spec_and_no_work_is_never_reprepared(self):
        directory = self.pending_original("spec")
        protected = {p: p.read_bytes() for p in (directory / "factory-intent.json", directory / "spec.json")}
        with patch("taskflow.execution_teacher_successor.start_author_from_packet", side_effect=AssertionError("failed preparation is not retry permission")), \
             patch("taskflow.execution.run_worker", side_effect=AssertionError("must not claim")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("must not launch")):
            record = self.start(allow_model=True)["items"][0]["record"]
        self.assertEqual("existing_author_preparation_not_received", record["state"]["waiting"]["reason"])
        for path, raw in protected.items():
            self.assertEqual(raw, path.read_bytes())
        self.assertFalse((directory / "runtime").exists())

    def test_existing_factory_intent_or_spec_must_have_exact_original_identity(self):
        directory = self.pending_original("spec")
        intent_path, spec_path = directory / "factory-intent.json", directory / "spec.json"
        intent, spec = read_json(intent_path), read_json(spec_path)
        peer = self.author.root / "same-byte-foreign-packet.json"
        shutil.copyfile(self.author.packet, peer)
        variants = [
            {**intent, "directory": str(self.author.root / "foreign")},
            {**intent, "factory_config_digest": "0" * 64},
            {**intent, "candidate_id": "foreign-candidate"},
            {**intent, "packet": evidence_ref(peer, "same SHA, wrong original packet")},
            {**intent, "template": intent["packet"]},
        ]
        for value in variants:
            write_json(intent_path, value)
            with self.subTest(identity=value), self.assertRaises(TaskflowError):
                self.start(allow_model=True)
        write_json(intent_path, intent)
        write_json(spec_path, {**spec, "work": {**spec["work"], "dispatch_owner": "foreign-owner"}})
        with self.assertRaisesRegex(TaskflowError, "foreign dispatch owner"):
            self.start(allow_model=True)
        self.assertFalse(self.directory.exists())

    def test_existing_ready_unknown_window_is_not_claimed_by_the_teacher_link(self):
        from taskflow.execution_author import start_author_from_packet
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            prepared = start_author_from_packet(self.store, **self.author.kwargs())
        with patch("taskflow.execution.run_worker", side_effect=AssertionError("unknown owner")), \
             patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("unknown owner")):
            result = self.start(allow_model=True)
        record = result["items"][0]["record"]
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("original owner must reconcile", record["state"]["exception"]["reason"])
        self.assertEqual("ready", self.store.get_work(prepared["work_id"])["state"])
        self.assertEqual([], self.store.list_attempts(prepared["work_id"]))

    def test_normal_allow_model_consumer_claims_one_local_author_then_follows_its_existing_research_end(self):
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            record = self.start()["items"][0]["record"]
        self.author.actual_run = execution_runner.run_attempt
        with patch("taskflow.execution_runner.run_attempt", side_effect=self.author.actual_fixture_process):
            result = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual("accepted", result["state"]["status"], result["state"])
        self.assertEqual("research_ended", result["state"]["result"]["author_result"]["outcome"])
        self.assertEqual(1, len(self.store.list_attempts(record["definition"]["author_work_id"])))
        again = run_lifecycle(self.store, record["flow_id"], allow_model=True, poll_seconds=.001)
        self.assertEqual(result, again)
        self.assertFalse(result["state"]["result"]["new_formal_experiment"])

    def test_unreceived_or_ambiguous_predecessor_cannot_prepare_a_successor(self):
        for status in ("active", "exception"):
            original = self.store.get_flow(self.predecessor["flow_id"])
            changed = self.store.update_flow(original["flow_id"], original["revision"],
                {**original["state"], "status": status}, {"kind": "neutral source not received"})
            with self.subTest(status=status), self.assertRaisesRegex(TaskflowError, "one exact received"):
                self.start()
            self.store.update_flow(changed["flow_id"], changed["revision"], original["state"], {"kind": "restore neutral source"})
        original = self.store.get_flow(self.predecessor["flow_id"])
        self.store.register_flow("second-ambiguous-source", {**original["definition"], "flow_id": "second-ambiguous-source"},
            original["evidence"], original["state"])
        with self.assertRaisesRegex(TaskflowError, "one exact received"):
            self.start()
        self.assertFalse(self.directory.exists())

    def test_wrong_stage_artifact_peer_sid_or_archive_bytes_cannot_become_accepted_original(self):
        registered = Path(self.data["items"][0]["registered_original"]["path"])
        source = read_json(registered)
        peer = self.author.root / "same-byte-peer.json"
        shutil.copyfile(source["raw_score"]["path"], peer)
        write_json(registered, {**source, "raw_score": evidence_ref(peer, "foreign same-byte peer")})
        self.data["items"][0]["registered_original"] = evidence_ref(registered, "changed registration")
        write_json(self.bundle, self.data)
        with self.assertRaisesRegex(TaskflowError, "actual declared sealed"):
            self.start()
        write_json(registered, source)
        self.data["items"][0]["registered_original"] = evidence_ref(registered, "restored registration")
        write_json(self.bundle, self.data)
        facts = Path(source["sealed_facts"]["path"])
        facts.write_bytes(facts.read_bytes() + b"drift")
        with self.assertRaises(TaskflowError):
            self.start()
        self.assertFalse(self.directory.exists())

    def test_a_receipt_sid_annotation_cannot_replace_the_actual_two_phase_seals(self):
        receipt_path = Path(self.accepted["receipt"])
        original = receipt_path.read_bytes()
        forged = read_json(receipt_path)
        forged["signer_sid"] = "00000000-0000-4000-8000-000000000999"
        write_json(receipt_path, forged)
        registration_path = Path(self.data["items"][0]["registered_original"]["path"])
        registration = read_json(registration_path)
        registration["receipt"] = evidence_ref(receipt_path, "changed source header")
        write_json(registration_path, registration)
        self.data["items"][0]["predecessor_receipt"] = evidence_ref(receipt_path, "changed source header")
        self.data["items"][0]["registered_original"] = evidence_ref(registration_path, "changed source header binding")
        write_json(self.bundle, self.data)
        try:
            with self.assertRaisesRegex(TaskflowError, "same sealed reviewer continuation"):
                self.start()
        finally:
            receipt_path.write_bytes(original)
        self.assertEqual([], self.author.native_specs)

    def test_one_author_intent_cannot_be_rebound_by_a_changed_bundle_or_unconfirmed_batch(self):
        with patch("taskflow.execution_author.enqueue_spec", side_effect=self.author.fixture_enqueue):
            self.start()
        self.data["batch_scope"] += " Changed authority is a separate immutable bundle."
        write_json(self.bundle, self.data)
        with self.assertRaisesRegex(TaskflowError, "another immutable"):
            self.start()
        self.assertEqual(1, len([row for row in self.store.list_flows() if row["definition"].get("kind") == "teacher_successor_v1"]))

    def test_an_unconfirmed_batch_factory_is_rejected_by_the_original_author_gate(self):
        factory_path = Path(self.data["author_factory"]["path"])
        factory = read_json(factory_path)
        factory["batch_snapshot"] = {"path": str(self.author.root / "unconfirmed.json"), "sha256": "0" * 64}
        write_json(factory_path, factory)
        self.data["author_factory"] = evidence_ref(factory_path, "unconfirmed fixture binding")
        write_json(self.bundle, self.data)
        with patch("taskflow.execution_experiment_factory.subprocess.Popen", side_effect=AssertionError("no confirmed batch")):
            result = self.start(allow_model=True)
        record = result["items"][0]["record"]
        self.assertEqual("exception", record["state"]["status"])
        self.assertIn("registered confirmed policy", record["state"]["exception"]["reason"])
        self.assertEqual([], self.author.native_specs)
        self.assertEqual([], self.store.list_attempts(record["definition"]["author_work_id"]))

    def test_a_new_batch_id_cannot_use_the_historical_factory_policy_as_confirmation(self):
        self.author.packet_data["batch_id"] = "new-unconfirmed-batch"
        write_json(self.author.packet, self.author.packet_data)
        self.data["items"][0]["author_packet"] = evidence_ref(self.author.packet, "new batch still needs its own model confirmation")
        write_json(self.bundle, self.data)
        with self.assertRaisesRegex(TaskflowError, "explicit confirmed batch factory"):
            self.start()
        self.assertEqual([], self.author.native_specs)
        self.assertFalse(self.directory.exists())

    def test_retired_teacher_cli_never_creates_a_successor(self):
        before = self.store.events()
        args = ["--db", str(self.store.path), "teacher-successor-start", "--packet", str(self.bundle), "--directory", str(self.directory)]
        with patch("taskflow.execution_teacher_successor.start_teacher_successors_from_packet") as started, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                execution.main(args)
        self.assertEqual(2, error.exception.code)
        started.assert_not_called()
        self.assertEqual(before, self.store.events())
        self.assertFalse(self.directory.exists())


if __name__ == "__main__":
    unittest.main()
