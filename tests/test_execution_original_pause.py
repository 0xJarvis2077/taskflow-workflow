"""Real SQLite and sealed neutral original intakes; no provider/model calls.

The existing fixture simulates quality/release/formal adapters, while actual
local subprocesses produce the first solver, facts, score, seals and intake.
All control/CAS behavior uses the actual integrated ExecutionStore method.
"""

import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_lifecycle import advance_lifecycle, register_lifecycle, run_lifecycle
from taskflow.execution_original_pause import (
    advance_original_score_pause, validate_original_pause_definition, validated_original,
)
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as experiments


def source_fixture(owner, *, score=7, stop=None, solver_prepare=None, original_import=None, allow_solver=False):
    case = experiments.ExperimentBoundaryTests()
    case.setUp()
    owner.addCleanup(case.doCleanups)
    if solver_prepare is not None:
        case.prepare = lambda **kwargs: solver_prepare(case, **kwargs)
    if stop == "unprepared":
        return case, case.store.get_flow(case.record["flow_id"])
    record = case.ready()
    execution.run_worker(case.store, "pause-neutral-solver", allow_solver, [record["state"]["current_work_id"]])
    importer = case.import_original_fixture if original_import is None else lambda **kwargs: original_import(case, **kwargs)
    with patch("taskflow.execution_experiment.import_formal", side_effect=importer):
        record = case.advance()
    with patch("taskflow.execution_experiment.prepare_facts_runner", side_effect=case.fixture_role):
        record = case.advance()
    if stop == "facts_wait":
        return case, record
    execution.run_worker(case.store, "pause-neutral-facts", False, [record["state"]["current_work_id"]])
    record = case.advance()
    original_write = experiments.write_json

    def score_payload(path, value):
        if Path(path).name == "payload.json" and "score.json" in value:
            raw = json.loads(value["score.json"])
            raw["total_score"] = score
            raw["criteria"][0]["score"] = score
            value = {**value, "score.json": json.dumps(raw)}
        original_write(path, value)

    with patch("taskflow.execution_experiment.prepare_score_runner", side_effect=lambda **kw: case.fixture_role(scoring=True, **kw)), \
            patch.object(experiments, "write_json", side_effect=score_payload):
        record = case.advance()
    if stop == "score_wait":
        return case, record
    execution.run_worker(case.store, "pause-neutral-score", False, [record["state"]["current_work_id"]])
    record = case.advance()
    record = case.advance()
    owner.assertEqual("accepted", record["state"]["status"], record["state"])
    owner.assertEqual(score, record["state"]["runs"][0]["score"])
    return case, record


def pause_definition(case, source, *, flow_id="one-original-pause", packet=None):
    if packet is None:
        packet = read_json(case.authority)
    purpose = packet["business_investment"]["unresolved_business_question"]
    authority_path = case.root / (flow_id + ".authorization.json")
    write_json(authority_path, {"schema_version": 1, "controller": source["definition"]["owner"],
        "task_id": source["definition"]["task_id"], "policy_id": source["definition"]["policy_id"],
        "source_flow_id": source["flow_id"], "source_definition_digest": source["definition_digest"],
        "first_run_purpose": purpose, "action": "pause_after_original_score",
        "round_scope": "this_round_original", "first_run_only": True})
    return {"schema_version": 1, "kind": "original_score_pause_v1", "flow_id": flow_id,
        "task_id": source["definition"]["task_id"], "owner": source["definition"]["owner"],
        "policy_id": source["definition"]["policy_id"], "source_flow_id": source["flow_id"],
        "source_definition_digest": source["definition_digest"], "first_run_purpose": purpose,
        "authorization": evidence_ref(authority_path, "explicit single-round controller task pause"),
        "directory": str(case.root / flow_id)}


def register_pause(case, source, **kwargs):
    definition = pause_definition(case, source, **kwargs)
    return register_lifecycle(case.store, definition, [definition["authorization"]])


def metadata(store):
    with store._db() as db:
        return {table: [tuple(row) for row in db.execute("SELECT * FROM " + table + " ORDER BY rowid")]
                for table in ("attempts", "work", "candidates", "reviews", "decisions")}


def controls(store):
    with store._db() as db:
        return [json.loads(row["data"]) for row in db.execute("SELECT data FROM controls")]


class OriginalScorePauseTests(unittest.TestCase):
    def test_high_and_low_original_scores_pause_once_with_originals_unchanged(self):
        for score in (0, 7):
            with self.subTest(score=score):
                case, source = source_fixture(self, score=score)
                follower = register_pause(case, source)
                before = metadata(case.store)
                proof = validated_original(case.store, source)
                protected = {row["path"]: Path(row["path"]).read_bytes() for row in proof["evidence"]}
                with patch("taskflow.execution.run_worker", side_effect=AssertionError("pause never dispatches")):
                    result = run_lifecycle(case.store, follower["flow_id"], allow_model=False, poll_seconds=0)
                self.assertEqual("accepted", result["state"]["status"])
                self.assertIsNone(result["state"]["current_work_id"])
                self.assertEqual("pause", controls(case.store)[0]["action"])
                self.assertEqual("task:neutral", controls(case.store)[0]["scope"])
                self.assertEqual(before, metadata(case.store))
                self.assertEqual(source, case.store.get_flow(source["flow_id"]))
                for path, raw in protected.items():
                    self.assertEqual(raw, Path(path).read_bytes())
                case.store.set_control("task:neutral", "resume", "Later explicit human decision.")
                self.assertEqual(result, advance_lifecycle(case.store, follower["flow_id"]))
                self.assertEqual("resume", controls(case.store)[0]["action"])

    def test_no_original_facts_or_score_yet_only_waits_and_never_pauses(self):
        for stop in ("unprepared", "facts_wait", "score_wait"):
            with self.subTest(stop=stop):
                case, source = source_fixture(self, stop=stop)
                follower = register_pause(case, source)
                before = metadata(case.store)
                result = advance_lifecycle(case.store, follower["flow_id"])
                self.assertEqual("first_original_not_received", result["state"]["waiting"])
                self.assertEqual("active", result["state"]["status"])
                self.assertEqual([], controls(case.store))
                self.assertEqual(before, metadata(case.store))
                self.assertEqual(result, advance_lifecycle(case.store, follower["flow_id"]))

    def test_wrong_source_owner_task_definition_authority_or_baseline_scope_is_rejected(self):
        case, source = source_fixture(self, stop="unprepared")
        definition = pause_definition(case, source)
        for key, value in (("source_flow_id", "source-flow"), ("owner", "foreign-owner"),
                           ("task_id", "foreign-task"), ("source_definition_digest", "0" * 64),
                           ("first_run_purpose", "A different investment.")):
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                validate_original_pause_definition(case.store, {**definition, key: value})
        path = Path(definition["authorization"]["path"])
        original = path.read_bytes()
        for change in ({"round_scope": "baseline_only"}, {"first_run_only": 1}, {"action": "after_score"}):
            body = json.loads(original); body.update(change); write_json(path, body)
            wrong = {**definition, "authorization": evidence_ref(path, "explicit wrong scope")}
            with self.subTest(change=change), self.assertRaises(TaskflowError):
                validate_original_pause_definition(case.store, wrong)
        path.write_bytes(original)
        path.write_bytes(original + b" ")
        with self.assertRaises(TaskflowError):
            validate_original_pause_definition(case.store, definition)
        path.write_bytes(original)
        source_authority = Path(source["definition"]["authorization"]["path"])
        source_raw = source_authority.read_bytes()
        source_authority.write_bytes(source_raw + b" ")
        with self.assertRaises(TaskflowError):
            validate_original_pause_definition(case.store, definition)
        source_authority.write_bytes(source_raw)
        self.assertEqual([], controls(case.store))

    def test_second_row_or_changed_core_cannot_replace_first_original(self):
        case, source = source_fixture(self)
        original = source["state"]["runs"][0]
        wrong = copy.deepcopy(source)
        wrong["state"]["runs"] = [{**original, "receipt": str(case.root / "second-receipt.json")}, original]
        with self.assertRaises(TaskflowError):
            validated_original(case.store, wrong)
        raw = Path(source["state"]["score_attempt"]["runtime_path"]) / "artifacts/score.json"
        saved = raw.read_bytes(); raw.write_bytes(saved + b" ")
        with self.assertRaises(TaskflowError):
            validated_original(case.store, source)
        raw.write_bytes(saved)
        attempt = source["state"]["facts_attempt"]
        with case.store._db() as db:
            saved = db.execute("SELECT data FROM attempts WHERE id=?", (attempt["attempt_id"],)).fetchone()["data"]
        for field, value in (("cli_sid", original["solver_sid"]), ("state", "uncertain"), ("dispatch_owner", "foreign")):
            bad = json.loads(saved); bad[field] = value
            with case.store._db(True) as db:
                db.execute("UPDATE attempts SET data=? WHERE id=?", (json.dumps(bad), attempt["attempt_id"]))
            try:
                with self.subTest(field=field), self.assertRaises(TaskflowError):
                    validated_original(case.store, source)
            finally:
                with case.store._db(True) as db:
                    db.execute("UPDATE attempts SET data=? WHERE id=?", (saved, attempt["attempt_id"]))
        self.assertEqual([], controls(case.store))

    def test_concurrent_same_plan_atomically_consumes_once_and_duplicate_plan_is_rejected(self):
        case, source = source_fixture(self)
        follower = register_pause(case, source)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: advance_original_score_pause(case.store, follower), range(2)))
        self.assertEqual(results[0], results[1])
        with case.store._db() as db:
            count = db.execute("SELECT count(*) FROM events WHERE kind='control'").fetchone()[0]
        self.assertEqual(1, count)
        self.assertEqual(1, case.store.get_flow(follower["flow_id"])["revision"])
        with self.assertRaises(TaskflowError):
            register_pause(case, source, flow_id="duplicate-pause")

    def test_concurrent_different_ids_cannot_register_two_followups_for_the_same_source(self):
        case, source = source_fixture(self, stop="unprepared")
        definitions = [pause_definition(case, source, flow_id="pause-" + name) for name in ("one", "two")]
        barrier = threading.Barrier(2)
        original = validate_original_pause_definition

        def simultaneously_validated(*args, **kwargs):
            result = original(*args, **kwargs)
            barrier.wait(timeout=10)
            return result

        def register(definition):
            try:
                return register_lifecycle(case.store, definition, [definition["authorization"]])
            except TaskflowError as error:
                return error

        with patch("taskflow.execution_original_pause.validate_original_pause_definition", side_effect=simultaneously_validated), \
                ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(register, definitions))
        self.assertEqual(1, sum(isinstance(result, dict) for result in results))
        self.assertEqual(1, sum(isinstance(result, TaskflowError) for result in results))
        matches = [row for row in case.store.list_flows() if row["definition"].get("kind") == "original_score_pause_v1"]
        self.assertEqual(1, len(matches))
        self.assertEqual([], controls(case.store))

    def test_retired_pause_consumer_cli_cannot_receive_or_change_originals(self):
        from taskflow import execution_experiment_factory as factory
        from taskflow.execution_experiment_factory import launch_lifecycle_consumer

        case, source = source_fixture(self)
        follower = register_pause(case, source)
        before = metadata(case.store)
        actual_popen, processes = factory.subprocess.Popen, []

        def record_process(*args, **kwargs):
            process = actual_popen(*args, **kwargs)
            if isinstance(args[0], list) and follower["flow_id"] in args[0]:
                processes.append(process)
            return process

        with patch.object(factory.subprocess, "Popen", side_effect=record_process):
            launched = launch_lifecycle_consumer(case.store, follower, Path(__file__).resolve().parents[1])
        self.assertTrue(launched["consumer_spawned"], launched)
        self.assertEqual(1, len(processes))
        self.assertEqual(2, processes[0].wait(timeout=30))
        self.assertEqual(follower, case.store.get_flow(follower["flow_id"]))
        self.assertEqual(before, metadata(case.store))
        self.assertEqual(source, case.store.get_flow(source["flow_id"]))
        self.assertEqual([], controls(case.store))
        directory = Path(follower["definition"]["directory"])
        intent = read_json(directory / "consumer-launch-intent.json")
        self.assertNotIn("--allow-model", intent["argv"])
        self.assertFalse(intent["allow_model"])
        self.assertIn("invalid choice: 'lifecycle-run'", (directory / "consumer.stderr.log").read_text())
        self.assertFalse((directory / "result.json").exists())

    def test_failed_commit_rolls_back_control_and_follower_together_and_source_cas_revalidates(self):
        case, source = source_fixture(self)
        follower = register_pause(case, source)
        with case.store._db(True) as db:
            db.execute("CREATE TRIGGER reject_pause BEFORE UPDATE ON execution_flows WHEN NEW.flow_id='one-original-pause' BEGIN SELECT RAISE(ABORT,'neutral rollback proof'); END")
        with self.assertRaises(TaskflowError):
            advance_original_score_pause(case.store, follower)
        self.assertEqual([], controls(case.store))
        self.assertEqual(follower, case.store.get_flow(follower["flow_id"]))
        with case.store._db(True) as db:
            db.execute("DROP TRIGGER reject_pause")
        accept = case.store.accept_original_score_pause

        def changed_source(*args, **kwargs):
            current = case.store.get_flow(source["flow_id"])
            case.store.update_flow(source["flow_id"], current["revision"], {**current["state"], "waiting": "neutral concurrent metadata tick"}, {"kind": "neutral source CAS drift"})
            return accept(*args, **kwargs)

        with patch.object(case.store, "accept_original_score_pause", side_effect=changed_source):
            self.assertEqual(follower, advance_original_score_pause(case.store, follower))
        self.assertEqual([], controls(case.store))
        self.assertEqual("accepted", advance_original_score_pause(case.store, follower)["state"]["status"])

    def test_eligible_confirmation_keeps_its_existing_solver1_pause_and_uses_only_first_receipt(self):
        from tests.test_execution_confirmation_preparation_resume import make_confirmation_recovery_fixture
        from taskflow.execution_experiment import _node

        f, source, packet, _ = make_confirmation_recovery_fixture(self)
        case = type("Fixture", (), {})()
        case.root, case.store = f.root, f.store
        second = _node(source, "solver")
        case.store.set_control("work:" + second, "pause", "Existing one-round control remains.")
        before = metadata(case.store)
        with patch("taskflow.execution_experiment._packet", return_value=packet):
            follower = register_pause(case, source, packet=packet)
            result = advance_original_score_pause(case.store, follower)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(1, case.store.get_flow(source["flow_id"])["state"]["run_index"])
        self.assertEqual(before, metadata(case.store))
        self.assertEqual({"task:neutral", "work:" + second}, {item["scope"] for item in controls(case.store)})


if __name__ == "__main__":
    unittest.main()
