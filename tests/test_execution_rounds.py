"""Bounded round ledger and real local fake workers; no provider calls."""

import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
import json
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_lifecycle import advance_lifecycle, register_lifecycle, run_lifecycle
from taskflow.execution_rounds import advance_round_plan, bind_round_from_packet, round_claim_scope, set_round_plan_mode, validate_round_plan
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as experiments
from tests import test_execution_original_pause as originals


def plan_definition(case, source, *, target=5, identity="neutral-round-plan", simulation=True, adopt=True):
    path = case.root / (identity + "-authority.json")
    user = case.root / (identity + "-user.txt")
    user.write_text("SIMULATED test authority; no actual user/model result.", encoding="utf-8")
    fields = {"flow_id": identity, "task_id": "neutral", "policy_id": "fixture-v1", "root_intent": identity + "-root",
              "batch_id": "neutral-batch", "target_rounds": target, "simulation_only": simulation,
              "first_experiment_id": source["flow_id"], "first_experiment_digest": source["definition_digest"],
              "adopt_existing_first": adopt}
    write_json(path, {"schema_version": 1, "kind": "bounded_original_rounds_authority_v1",
                     "controller": "fixture-controller", **fields,
                     "automatic_successors": True, "count_rule": "accepted_first_originals",
                     "preserve_first_inputs": True,
                     "user_authorization": {"origin": "test_fixture" if simulation else "direct_user",
                                            "source": evidence_ref(user, "SIMULATED authority")}})
    return {"schema_version": 1, "kind": "bounded_original_rounds_v1", "flow_id": identity,
            "owner": "fixture-controller", "directory": str(case.root / identity), **fields,
            "authorization": evidence_ref(path, "SIMULATED bounded plan"),
            "policy_digest": case.store.get_policy("fixture-v1")["digest"]}


class BoundedRoundTests(unittest.TestCase):
    def setUp(self):
        self.case, self.source = originals.source_fixture(self)
        self.source = advance_lifecycle(self.case.store, self.source["flow_id"])
        self.store = self.case.store

    def register(self, **kwargs):
        definition = plan_definition(self.case, self.source, **kwargs)
        return register_lifecycle(self.store, definition, [definition["authorization"]])

    def next_packet(self, plan, ordinal):
        packet = read_json(self.case.authority)
        packet.update(batch_id=plan["definition"]["batch_id"], bounded_round={
            "plan_id": plan["flow_id"], "ordinal": ordinal,
            "root_intent": plan["definition"]["root_intent"],
            "predecessor_digest": sha256(canonical(plan["state"]["completed"][-1])), "simulation_only": True})
        packet["business_investment"]["unresolved_business_question"] = "Neutral simulated investment " + str(ordinal)
        packet["formal"]["first_run_id"] = "neutral-original-" + str(ordinal)
        packet["independent_review"]["score_id"] = "neutral-score-" + str(ordinal)
        packet["single_confirmation"]["max_additional_attempts"] = 0
        path = self.case.root / ("round-" + str(ordinal) + "-packet.json")
        write_json(path, packet)
        config = self.case.root / "simulated-factory.json"
        write_json(config, {"nature": "test_fixture", "adapter": "explicit local factory boundary only"})
        return evidence_ref(path, "SIMULATED business packet"), evidence_ref(config, "SIMULATED factory")

    def simulated_factory(self, store, *, packet_ref, factory_config_ref, allow_model):
        self.assertFalse(allow_model)
        ordinal = read_json(Path(packet_ref["path"]))["bounded_round"]["ordinal"]
        definition = copy.deepcopy(self.case.definition)
        definition.update(flow_id="neutral-round-" + str(ordinal), authorization=packet_ref,
                          business_packet_ref=packet_ref, factory_config_ref=factory_config_ref,
                          directory=str(self.case.root / ("round-" + str(ordinal))))
        definition["technical"].update(publication_root=str(self.case.root / ("publication-" + str(ordinal))),
            formal_runtime_root=str(self.case.root / ("formal-" + str(ordinal))))
        child = register_lifecycle(store, definition, [packet_ref, factory_config_ref])
        return {"items": [{"flow_id": child["flow_id"]}]}

    def bind(self, plan, ordinal):
        packet, factory = self.next_packet(plan, ordinal)
        with patch("taskflow.execution_experiment_factory.start_experiments_from_packet", side_effect=self.simulated_factory):
            return bind_round_from_packet(self.store, plan["flow_id"], ordinal=ordinal,
                                          packet_ref=packet, factory_config_ref=factory)

    @contextmanager
    def simulated_adapters(self):
        """Fake only factory/release/native preparation; use real local workers/seals/intake."""
        solver_sid, reviewer_sid = str(uuid.uuid4()), str(uuid.uuid4())

        def project(store, *, candidate_id, source_refs, protocol_map, destination):
            self.case.fixture_projections = {}
            for kind in ("business", "artifact", "calculation"):
                path = destination / (kind + ".json")
                write_json(path, {"nature": "test_fixture", "kind": kind, "candidate_id": candidate_id})
                self.case.fixture_projections[kind] = path
            return self.case.fixture_projections

        def prepare(**kwargs):
            cwd = kwargs["runtime"] / "work"
            cwd.mkdir(parents=True)
            script = cwd.parent / "local-fixture.py"
            script.write_text("import pathlib,json\npathlib.Path('answer.md').write_text('Neutral simulation.')\n"
                              + "print(json.dumps({'type':'thread.started','thread_id':" + repr(solver_sid) + "}))\n"
                              + "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':'Neutral simulated delivery.'}}))\n"
                              + "print(json.dumps({'type':'turn.completed'}))\n", encoding="utf-8")
            return {"runner": {"intent_id": kwargs["config"]["intent_id"], "policy_version": "fixture-v1", "role": "solver",
                "mode": "test_fixture", "model": "fixture-cli", "reasoning_effort": "fixture", "timeout_seconds": 15,
                "cwd": str(cwd), "argv": [str(Path(sys.executable).resolve()), str(script)], "dependencies": [str(script)],
                "inputs": [], "request_sections": [{"kind": "operation", "text": "SIMULATED local solver."}], "outputs": ["answer.md"]}}

        enqueue = execution.enqueue_spec

        def role(*, scoring=False, **kwargs):
            config = {**kwargs["config"], "excluded_sids": [*kwargs["config"]["excluded_sids"], solver_sid]}

            def enqueue_with_sid(store, spec, runtime, **options):
                spec = copy.deepcopy(spec)
                argv = spec["runner"]["argv"]
                argv[argv.index("--sid") + 1] = reviewer_sid
                return enqueue(store, spec, runtime, **options)

            with patch.object(execution, "enqueue_spec", side_effect=enqueue_with_sid):
                return self.case.fixture_role(scoring=scoring, **{**kwargs, "config": config})

        with ExitStack() as stack:
            for name, effect in {
                "projected_quality_reports": project,
                "_release_quality_reports": lambda store, **kw: experiments._required_release_reports(
                    read_json(kw["source_root"] / "task.json"), kw["reports"]),
                "prepare_release": self.case.release, "prepare_formal": prepare,
                "import_formal": self.case.import_original_fixture,
                "prepare_facts_runner": role, "prepare_score_runner": lambda **kw: role(scoring=True, **kw),
            }.items():
                stack.enter_context(patch("taskflow.execution_experiment." + name, side_effect=effect))
            yield

    def test_real_plan_cannot_accept_unconfirmed_fixture_policy(self):
        with self.assertRaisesRegex(TaskflowError, "confirmed real batch"):
            self.register(simulation=False)

    def test_target_one_counts_only_simulation_and_preserves_first(self):
        plan = self.register(target=1)
        protected = {r["path"]: Path(r["path"]).read_bytes() for r in originals.validated_original(self.store, self.source)["evidence"]}
        first = self.store.get_flow(self.source["flow_id"])
        result = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertEqual(1, len(result["state"]["completed"]))
        self.assertEqual(0, result["state"]["real_original_count"])
        self.assertTrue(result["state"]["result"]["simulation_only"])
        self.assertEqual(result, advance_lifecycle(self.store, plan["flow_id"]))
        self.assertEqual(first, self.store.get_flow(self.source["flow_id"]))
        self.assertTrue(all(Path(p).read_bytes() == raw for p, raw in protected.items()))

    def test_missing_business_binding_waits_and_cannot_skip_an_ordinal(self):
        plan = self.register()
        record = advance_lifecycle(self.store, plan["flow_id"])
        self.assertEqual("next_business_binding_required", record["state"]["waiting"])
        record = advance_lifecycle(self.store, plan["flow_id"])
        self.assertEqual(1, len(record["state"]["completed"]))
        with self.assertRaisesRegex(TaskflowError, "round outside"):
            bind_round_from_packet(self.store, plan["flow_id"], ordinal=6,
                                   packet_ref={}, factory_config_ref={})

    def test_pause_and_cancel_do_not_change_original_controls_or_first(self):
        plan = self.register()
        first = self.store.get_flow(self.source["flow_id"])
        with self.store._db() as db:
            before = list(db.execute("SELECT * FROM controls"))
        set_round_plan_mode(self.store, plan["flow_id"], "paused", "SIMULATED pause")
        paused = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual("bounded_rounds_paused", paused["execution"])
        self.assertEqual([], paused["state"]["completed"])
        set_round_plan_mode(self.store, plan["flow_id"], "cancelled", "SIMULATED cancel")
        with self.assertRaisesRegex(TaskflowError, "cannot resume"):
            set_round_plan_mode(self.store, plan["flow_id"], "running", "SIMULATED resume")
        with self.store._db() as db:
            self.assertEqual(before, list(db.execute("SELECT * FROM controls")))
        self.assertEqual(first, self.store.get_flow(self.source["flow_id"]))

    def test_five_simulated_originals_resume_once_and_stop_without_sixth_work(self):
        plan = self.register()
        first = self.store.get_flow(self.source["flow_id"])
        protected = {r["path"]: Path(r["path"]).read_bytes() for r in originals.validated_original(self.store, first)["evidence"]}
        plan = advance_round_plan(self.store, plan)
        for ordinal in range(2, 6):
            plan = self.bind(plan, ordinal)
            with self.simulated_adapters():
                ready = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
                self.assertEqual("ready_requires_explicit_model_permission", ready["execution"], ready["state"])
                work_id = ready["state"]["current_work_id"]
                self.assertEqual("waiting", execution.run_worker(self.store, "unrelated-worker", False, [work_id])["state"])
                self.assertEqual([], self.store.list_attempts(work_id))
                paused = set_round_plan_mode(self.store, plan["flow_id"], "paused", "SIMULATED pause before claim")
                with round_claim_scope(self.store, paused, work_id):
                    self.assertIsNone(self.store.claim("paused-consumer", [work_id]))
                set_round_plan_mode(self.store, plan["flow_id"], "running", "SIMULATED resume")
                plan = run_lifecycle(self.store, plan["flow_id"], allow_model=True, poll_seconds=0)
            self.assertEqual(ordinal, len(plan["state"]["completed"]), [
                self.store.get_flow("neutral-round-" + str(ordinal))["state"].get("exception"),
                [{k: a.get(k) for k in ("state", "error", "reason", "last_error", "exception", "runtime_path")}
                 for a in self.store.list_attempts(work_id)]])
            self.assertEqual(0, plan["state"]["real_original_count"])
            self.assertEqual(first, self.store.get_flow(first["flow_id"]))
            self.assertTrue(all(Path(p).read_bytes() == raw for p, raw in protected.items()))
        self.assertEqual("accepted", plan["state"]["status"])
        before = originals.metadata(self.store)
        self.assertEqual(plan, run_lifecycle(self.store, plan["flow_id"], allow_model=True, poll_seconds=0))
        with self.assertRaisesRegex(TaskflowError, "round outside"):
            bind_round_from_packet(self.store, plan["flow_id"], ordinal=6, packet_ref={}, factory_config_ref={})
        self.assertEqual(before, originals.metadata(self.store))
        with self.store._db() as db:
            self.assertEqual(5, db.execute("SELECT count(*) FROM bounded_round_completions").fetchone()[0])
            self.assertEqual(12, db.execute("SELECT count(*) FROM bounded_round_work").fetchone()[0])
            self.assertEqual(12, db.execute("SELECT count(*) FROM events WHERE kind='bounded_round_launch_entered'").fetchone()[0])

    def test_concurrent_ticks_and_failed_commit_consume_original_once(self):
        plan = self.register()
        with self.store._db(True) as db:
            db.execute("CREATE TRIGGER reject_round BEFORE UPDATE ON execution_flows WHEN NEW.flow_id='neutral-round-plan' BEGIN SELECT RAISE(ABORT,'simulated rollback'); END")
        with self.assertRaises(TaskflowError):
            advance_round_plan(self.store, plan)
        with self.store._db(True) as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM bounded_round_completions").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT count(*) FROM bounded_round_sessions").fetchone()[0])
            db.execute("DROP TRIGGER reject_round")
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: advance_round_plan(self.store, plan), range(2)))
        self.assertEqual(1, len(results[-1]["state"]["completed"]))
        with self.store._db() as db:
            self.assertEqual(1, db.execute("SELECT count(*) FROM bounded_round_completions").fetchone()[0])

    def test_temporary_task_control_waits_then_resumes_without_rewriting_it(self):
        plan = self.register()
        for scope in ("all", "task:neutral"):
            self.store.set_control(scope, "pause", "SIMULATED existing control")
            result = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
            self.assertEqual("active", result["state"]["status"])
            self.assertEqual("existing_control_blocks_rounds", result["execution"])
            self.assertEqual([], result["state"]["completed"])
            self.store.set_control(scope, "resume", "SIMULATED explicit control recovery")
        result = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual(1, len(result["state"]["completed"]))

    def test_typed_authority_and_frozen_first_scope_are_required(self):
        definition = plan_definition(self.case, self.source)
        for key, value in (("kind", "experiment_v1"), ("target_rounds", True), ("target_rounds", 0),
                           ("simulation_only", 1), ("adopt_existing_first", 1),
                           ("policy_digest", "0" * 64), ("first_experiment_digest", "0" * 64),
                           ("root_intent", "foreign-intent")):
            with self.subTest(key=key, value=value), self.assertRaises(TaskflowError):
                validate_round_plan(self.store, {**definition, key: value})
        adopted = plan_definition(self.case, self.source, adopt=False)
        with self.assertRaisesRegex(TaskflowError, "explicit immutable adoption"):
            register_lifecycle(self.store, adopted, [adopted["authorization"]])

    def test_binding_is_idempotent_concurrent_and_rejects_changed_predecessor(self):
        plan = advance_round_plan(self.store, self.register())
        packet, factory = self.next_packet(plan, 2)
        call = lambda: bind_round_from_packet(self.store, plan["flow_id"], ordinal=2, packet_ref=packet, factory_config_ref=factory)
        with patch("taskflow.execution_experiment_factory.start_experiments_from_packet", side_effect=self.simulated_factory) as make:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: call(), range(2)))
            self.assertEqual(results[0], results[1])
            self.assertEqual(1, make.call_count)
        raw = read_json(Path(packet["path"]))
        raw["bounded_round"]["predecessor_digest"] = "0" * 64
        other = self.case.root / "changed-packet.json"
        write_json(other, raw)
        with self.assertRaisesRegex(TaskflowError, "cannot replace"):
            bind_round_from_packet(self.store, plan["flow_id"], ordinal=2,
                                   packet_ref=evidence_ref(other, "SIMULATED changed packet"), factory_config_ref=factory)

    def test_binding_cas_failure_rolls_back_child_and_reservations(self):
        plan = advance_round_plan(self.store, self.register())
        packet, factory = self.next_packet(plan, 2)
        with self.store._db(True) as db:
            db.execute("CREATE TRIGGER reject_bind BEFORE UPDATE ON execution_flows WHEN NEW.flow_id='neutral-round-plan' BEGIN SELECT RAISE(ABORT,'simulated binding rollback'); END")
        with patch("taskflow.execution_experiment_factory.start_experiments_from_packet", side_effect=self.simulated_factory), self.assertRaises(TaskflowError):
            bind_round_from_packet(self.store, plan["flow_id"], ordinal=2, packet_ref=packet, factory_config_ref=factory)
        self.assertFalse(any(f["flow_id"] == "neutral-round-2" for f in self.store.list_flows()))
        with self.store._db() as db:
            self.assertEqual(0, db.execute("SELECT count(*) FROM bounded_round_work").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT count(*) FROM bounded_round_bindings").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT count(*) FROM events WHERE kind='bounded_round_work_reserved'").fetchone()[0])

    def test_reserved_runtime_needs_guard_and_pause_between_claim_and_launch_blocks(self):
        from taskflow.execution_rounds import round_launch_guard
        from taskflow.execution_runner import RunnerError, run_attempt

        plan = self.bind(advance_round_plan(self.store, self.register()), 2)
        with self.simulated_adapters():
            ready = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        work_id = ready["state"]["current_work_id"]
        runtime = Path(self.store.get_work(work_id)["runtime_path"])
        with self.assertRaisesRegex(RunnerError, "guarded consumer"):
            run_attempt(runtime)
        self.assertFalse((runtime / "dispatch.json").exists())
        with round_claim_scope(self.store, ready, work_id):
            attempt = self.store.claim("guard-race-fixture", [work_id])
            self.store.mark_starting(attempt["attempt_id"])
            for scope in ("all", "task:neutral", "work:" + work_id):
                self.store.set_control(scope, "pause", "SIMULATED pause after claim")
                with self.assertRaisesRegex(TaskflowError, "no longer permits"):
                    with round_launch_guard(self.store, attempt):
                        self.fail("paused launch entered")
                self.store.set_control(scope, "resume", "SIMULATED restore")
            with round_launch_guard(self.store, attempt):
                # Entering admission releases the writer before process work.
                self.store.set_control("work:unrelated", "pause", "SIMULATED independent writer")
            with self.assertRaisesRegex(TaskflowError, "already entered"):
                with round_launch_guard(self.store, attempt):
                    self.fail("duplicate admission entered")
        self.assertFalse((runtime / "dispatch.json").exists())

    def test_changed_raw_score_terminal_or_ledger_cannot_be_counted(self):
        plan = self.register()
        terminal = Path(self.source["state"]["receipt"])
        raw = terminal.read_bytes()
        body = read_json(terminal)
        body["controller"] = "foreign-controller"
        write_json(terminal, body)
        with self.assertRaisesRegex(TaskflowError, "terminal original receipt"):
            advance_round_plan(self.store, plan)
        terminal.write_bytes(raw)
        plan = advance_round_plan(self.store, plan)
        with self.store._db(True) as db:
            db.execute("UPDATE bounded_round_completions SET run_digest=?", ("0" * 64,))
        with self.assertRaisesRegex(TaskflowError, "ledger identity"):
            advance_round_plan(self.store, plan)

    def test_lost_reservation_blocks_own_work_and_corrupt_other_ledger_leaves_unrelated_work(self):
        from taskflow.execution_rounds import round_claim_blocked

        plan = self.bind(advance_round_plan(self.store, self.register()), 2)
        with self.simulated_adapters():
            ready = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        work = self.store.get_work(ready["state"]["current_work_id"])
        with self.store._db(True) as db:
            db.execute("DELETE FROM bounded_round_work WHERE work_id=?", (work["work_id"],))
            with self.assertRaisesRegex(TaskflowError, "reservation missing"):
                round_claim_blocked(db, work)
            db.execute("DROP TABLE bounded_round_sessions")
            self.assertFalse(round_claim_blocked(db, {**work, "work_id": "unrelated-work"}))

    def test_bookkeeping_recovery_preserves_exception_and_completed_evidence(self):
        from taskflow.execution_rounds import resume_round_plan

        plan = self.register(target=1)
        terminal = Path(self.source["state"]["receipt"])
        original = terminal.read_bytes()
        value = read_json(terminal)
        value["controller"] = "foreign-controller"
        write_json(terminal, value)
        failed = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual("exception", failed["state"]["status"])
        prior = Path(failed["state"]["receipt"])
        prior_raw = prior.read_bytes()
        terminal.write_bytes(original)
        note = self.case.root / "simulated-repair.txt"
        note.write_text("SIMULATED restoration of original terminal bytes; no source/attempt reset.")
        recovered = resume_round_plan(self.store, plan["flow_id"], expected_revision=failed["revision"],
            controller="fixture-controller", resolution="Restore original receipt bytes.",
            evidence=[evidence_ref(note, "SIMULATED repair")])
        self.assertEqual("active", recovered["state"]["status"])
        self.assertEqual(original, terminal.read_bytes())
        result = run_lifecycle(self.store, plan["flow_id"], allow_model=False, poll_seconds=0)
        self.assertEqual("accepted", result["state"]["status"])
        self.assertNotEqual(str(prior), result["state"]["receipt"])
        self.assertEqual(prior_raw, prior.read_bytes())
        with self.assertRaisesRegex(TaskflowError, "revision/controller"):
            resume_round_plan(self.store, plan["flow_id"], expected_revision=failed["revision"],
                controller="fixture-controller", resolution="Replay stale recovery.", evidence=[evidence_ref(note, "SIMULATED repair")])

    def test_one_first_experiment_cannot_be_adopted_by_two_plans(self):
        self.register()
        with self.assertRaisesRegex(TaskflowError, "already belongs"):
            self.register(identity="other-round-plan")
        self.assertFalse(any(f["flow_id"] == "other-round-plan" for f in self.store.list_flows()))

    def test_corrupt_round_schema_does_not_block_unrelated_prepare_but_owned_work_fails_closed(self):
        from taskflow.execution_rounds import decorate_round_runner, round_claim_blocked, _schema

        plan = self.bind(advance_round_plan(self.store, self.register()), 2)
        with self.store._db(True) as db:
            work_id = db.execute("SELECT work_id FROM bounded_round_work LIMIT 1").fetchone()[0]
            db.execute("DROP TABLE bounded_round_work")
            for column in ("work_id", "data"):
                db.execute("CREATE TABLE bounded_round_work (" + column + " TEXT)")
                self.assertEqual({"role": "research"}, decorate_round_runner(db,
                    {"work_id": "unrelated-neutral-work", "role": "research"}, {"role": "research"}))
                self.assertFalse(round_claim_blocked(db, {"work_id": "unrelated-neutral-work"}))
                with self.assertRaisesRegex(TaskflowError, "malformed round ledger"):
                    round_claim_blocked(db, {"work_id": work_id})
                db.execute("DROP TABLE bounded_round_work")
            db.execute("CREATE TABLE bounded_round_work (work_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,data TEXT NOT NULL)")
            db.execute("DROP TABLE bounded_round_bindings")
            db.execute("CREATE TABLE bounded_round_bindings (plan_id TEXT,ordinal INTEGER,experiment_id TEXT,data TEXT)")
            with self.assertRaisesRegex(TaskflowError, "uniqueness constraints"):
                _schema(db)


if __name__ == "__main__":
    unittest.main()
