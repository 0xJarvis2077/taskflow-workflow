"""Bounded original-score rounds on the existing lifecycle and SQLite store.

Business coordinators supply each exact next investment through the existing
experiment factory. This module never invents business packets, changes a
locked first run, relaxes a quality gate, or starts a process from a tick.
"""

from contextlib import contextmanager
from contextvars import ContextVar
import json
import os
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, validate_id
from taskflow.execution_controller_finding import _TransactionStore
from taskflow.execution_store import ExecutionStore, evidence_ref, validate_evidence


KIND = "bounded_original_rounds_v1"
PROTOCOL = "<taskflow_bounded_original_round_v1>"
_CLAIM = ContextVar("bounded_original_round_claim", default=None)
_ROLES = ("solver", "review-facts", "review-score")


def managed_round_startup(store, record, directory):
    """Handshake after interpreter startup, before work or normal exit.

    On framework Python the launch executable can re-exec into Python.app.
    The parent must observe the settled child identity, not that transient PID.
    """
    from taskflow.execution_experiment_factory import _process_identity
    from taskflow.execution_flow import _publish_once

    paths = [directory / "consumer-launch-intent.json"]
    paths.extend(sorted((directory / "consumer-generations").glob("*/consumer-launch-intent.json")))
    if not paths[-1].exists():
        return
    intent_path = paths[-1]
    intent = read_json(intent_path)
    if not intent.get("normal_wait_handshake"):
        return
    _require(intent.get("flow_id") == record["flow_id"]
             and intent.get("definition_digest") == record["definition_digest"], "startup intent differs")
    process = _process_identity(os.getpid())
    _publish_once(intent_path.parent / ("consumer-started-" + str(os.getpid()) + ".json"), {
        "process": process, "launch_intent": evidence_ref(intent_path, "original consumer launch intent")})
    deadline = time.monotonic() + 5
    receipt_path = intent_path.parent / "consumer-launch.json"
    while not receipt_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    _require(receipt_path.exists(), "startup PID receipt missing; no round work permitted")
    receipt = read_json(receipt_path)
    _require(receipt.get("process", {}).get("pid") == os.getpid()
             and process.get("start_identity")
             and receipt["process"].get("start_identity") == process["start_identity"],
             "startup process identity is unproven; no round work permitted")


def record_normal_wait_exit(store, record):
    """Seal a managed consumer's normal wait while its lifecycle lock is held.

    Missing launcher receipts fail closed: this does not reconstruct a launch
    window or turn a direct/manual invocation into a managed consumer.
    """
    from taskflow.execution_experiment_factory import _consumer_launch_chain, _process_identity, _source_refs
    from taskflow.execution_flow import _publish_once

    repository = Path(__file__).resolve().parents[1]
    directory = Path(record["definition"]["directory"])
    if not (directory / "consumer-launch-intent.json").exists():
        return
    try:
        intent_path, _, intent, receipt, _ = _consumer_launch_chain(directory, record, store, repository)
    except (TaskflowError, OSError, ValueError, KeyError, TypeError) as exc:
        return {"recorded": False, "reason": str(exc)}
    process = _process_identity(os.getpid())
    original = receipt.get("process", {})
    if (not process.get("start_identity")
            or any(original.get(key) != process.get(key) for key in ("pid", "start_identity"))
            or intent["source_refs"] != _source_refs(repository)):
        return {"recorded": False, "reason": "consumer process identity or loaded source differs from launch",
                "identity_matches": all(original.get(key) == process.get(key) for key in ("pid", "start_identity")),
                "identity_observable": bool(process.get("start_identity")),
                "source_matches": intent["source_refs"] == _source_refs(repository)}
    _publish_once(intent_path.parent / "consumer-wait-exit.json", {
        "schema_version": 1, "kind": "bounded_round_normal_wait_v1",
        "flow_id": record["flow_id"], "definition_digest": record["definition_digest"],
        "process": original, "launch_intent": evidence_ref(intent_path, "original consumer launch intent"),
        "waiting": record["state"]["waiting"], "revision": record["revision"],
        "state_digest": sha256(canonical(record["state"])),
    })
    return {"recorded": True}


def validate_normal_wait_exit(store, record, intent_path, receipt, repository):
    """Require exact normal-wait provenance; caller proves process death and lock."""
    from taskflow.execution_experiment_factory import _source_refs

    ref = evidence_ref(intent_path.parent / "consumer-wait-exit.json", "proven normal bounded-round wait exit")
    proof = read_json(Path(ref["path"]))
    intent = read_json(intent_path)
    _require(proof.get("schema_version") == 1 and proof.get("kind") == "bounded_round_normal_wait_v1"
             and proof.get("flow_id") == record["flow_id"]
             and proof.get("definition_digest") == record["definition_digest"]
             and proof.get("process") == receipt.get("process")
             and proof.get("launch_intent") == evidence_ref(intent_path, "original consumer launch intent")
             and proof.get("waiting") in {"observing_locked_first_original", "next_business_binding_required"},
             "consumer has no exact normal wait exit proof")
    _require(intent["source_refs"] == _source_refs(repository), "normal wait source changed; technical recovery required")
    state = record["state"]
    _require(state.get("status") == "active" and state.get("mode") == "running"
             and not state.get("exception") and not state.get("pending_business_scope")
             and not state.get("current_work_id")
             and record["revision"] >= proof["revision"], "normal wait plan is no longer active")
    with store._db() as db:
        _schema(db)
        _controls(db, record["definition"])
        completed = _completed(db, record)
        _revalidate_completed(store, record, completed)
        pending = [item for item in _bindings(db, record["flow_id"])
                   if item["ordinal"] > len(completed) and item["owned"]]
        _require(not any(store.list_attempts(work) for item in pending
                         for work in item["work_ids"].values()),
                 "normal wait cannot restart a round with existing attempts")
    return {"normal_wait_exit": ref, "source_refs": intent["source_refs"]}


def _require(value, message):
    if not value:
        raise TaskflowError("bounded rounds: " + message)


def _schema(db, *, create=False):
    statements = (
        "CREATE TABLE IF NOT EXISTS bounded_round_bindings ("
        "plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,experiment_id TEXT NOT NULL UNIQUE,"
        "data TEXT NOT NULL,PRIMARY KEY(plan_id,ordinal))",
        "CREATE TABLE IF NOT EXISTS bounded_round_completions ("
        "plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,run_digest TEXT NOT NULL UNIQUE,"
        "record_digest TEXT NOT NULL UNIQUE,solver_attempt TEXT NOT NULL UNIQUE,"
        "data TEXT NOT NULL,PRIMARY KEY(plan_id,ordinal))",
        "CREATE TABLE IF NOT EXISTS bounded_round_sessions ("
        "sid TEXT PRIMARY KEY,plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS bounded_round_work ("
        "work_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
        "data TEXT NOT NULL)",
    )
    if create:
        for statement in statements:
            db.execute(statement)
    expected = {
        "bounded_round_bindings": ["plan_id", "ordinal", "experiment_id", "data"],
        "bounded_round_completions": ["plan_id", "ordinal", "run_digest", "record_digest", "solver_attempt", "data"],
        "bounded_round_sessions": ["sid", "plan_id", "ordinal"],
        "bounded_round_work": ["work_id", "plan_id", "ordinal", "data"],
    }
    for table, columns in expected.items():
        _require([row[1] for row in db.execute("PRAGMA table_info(" + table + ")")] == columns,
                 "missing or malformed round ledger")
    unique = {
        "bounded_round_bindings": {("plan_id", "ordinal"), ("experiment_id",)},
        "bounded_round_completions": {("plan_id", "ordinal"), ("run_digest",),
                                      ("record_digest",), ("solver_attempt",)},
        "bounded_round_sessions": {("sid",)}, "bounded_round_work": {("work_id",)},
    }
    for table, required in unique.items():
        actual = set()
        for index in db.execute("PRAGMA index_list(" + table + ")"):
            if index[2] and not index[4]:
                quoted = index[1].replace('"', '""')
                actual.add(tuple(row[2] for row in db.execute('PRAGMA index_info("' + quoted + '")')))
        _require(required <= actual, "round ledger uniqueness constraints changed")


def _reference(value):
    ref = validate_evidence([value])[0]
    return ref, read_json(Path(ref["path"]))


def validate_round_plan(store, definition):
    """Validate immutable scope and existing batch policy; no writes or calls."""
    from taskflow.execution_batch import validate_batch_policy

    _require(definition.get("kind") == KIND and type(definition.get("schema_version")) is int
             and definition["schema_version"] == 1, "unsupported round plan")
    keys = ("flow_id", "task_id", "owner", "policy_id", "root_intent", "batch_id")
    for key in keys:
        validate_id(definition.get(key), key)
    target = definition.get("target_rounds")
    _require(type(target) is int and target > 0, "positive integer target required")
    _require(type(definition.get("simulation_only")) is bool, "explicit simulation provenance required")
    _require(type(definition.get("adopt_existing_first")) is bool, "explicit first adoption choice required")
    ref, authority = _reference(definition["authorization"])
    _require(authority.get("kind") == "bounded_original_rounds_authority_v1"
             and type(authority.get("schema_version")) is int and authority["schema_version"] == 1
             and authority.get("controller") == definition["owner"]
             and authority.get("automatic_successors") is True
             and authority.get("count_rule") == "accepted_first_originals"
             and authority.get("preserve_first_inputs") is True,
             "explicit bounded original-round authority required")
    for key in ("flow_id", "task_id", "policy_id", "root_intent", "batch_id", "target_rounds",
                "simulation_only", "first_experiment_id", "first_experiment_digest", "adopt_existing_first"):
        _require(type(authority.get(key)) is type(definition.get(key))
                 and authority.get(key) == definition.get(key), "plan differs from authority: " + key)
    human = authority.get("user_authorization", {})
    _require(human.get("origin") == ("test_fixture" if definition["simulation_only"] else "direct_user"),
             "original user attribution required")
    validate_evidence([human.get("source")])
    policy = store.get_policy(definition["policy_id"])
    _require(policy["digest"] == definition.get("policy_digest"), "versioned policy changed")
    snapshot = validate_batch_policy(policy["config"])
    if not definition["simulation_only"]:
        _require(snapshot is not None and snapshot["execution_mode"] == "real_cli",
                 "real rounds require an existing confirmed real batch")
    if snapshot is not None:
        _require(snapshot["batch_id"] == definition["batch_id"]
                 and definition["task_id"] in snapshot["task_ids"], "batch scope differs")
        _require(not definition["simulation_only"] or snapshot["execution_mode"] == "test_fixture",
                 "simulated plan cannot consume a real batch")
    first = store.get_flow(definition["first_experiment_id"])
    _check_source(definition, first)
    _require(first["definition_digest"] == definition["first_experiment_digest"], "locked first definition changed")
    return ref


def _check_source(definition, source):
    actual = source["definition"]
    _require(actual.get("kind") == "experiment_v1"
             and sha256(canonical(actual)) == source["definition_digest"]
             and all(actual.get(key) == definition[key] for key in ("task_id", "owner", "policy_id")),
             "experiment identity/owner/task/policy differs")
    validate_evidence(source["evidence"])
    validate_evidence([actual["authorization"]])


def _bindings(db, identity):
    result = []
    for row in db.execute("SELECT * FROM bounded_round_bindings WHERE plan_id=? ORDER BY ordinal", (identity,)):
        data = json.loads(row["data"])
        _require(data["ordinal"] == row["ordinal"] and data["experiment_id"] == row["experiment_id"],
                 "binding ledger identity differs")
        result.append(data)
    return result


def _completed(db, record):
    rows = []
    for row in db.execute("SELECT * FROM bounded_round_completions WHERE plan_id=? ORDER BY ordinal", (record["flow_id"],)):
        data = json.loads(row["data"])
        proof = data["proof"]
        _require(data["ordinal"] == row["ordinal"]
                 and all(row[column] == proof[key] for column, key in (
                     ("run_digest", "run_digest"), ("record_digest", "record_digest"),
                     ("solver_attempt", "solver_attempt_id"))), "completion ledger identity differs")
        rows.append(data)
    _require([row["ordinal"] for row in rows] == list(range(1, len(rows) + 1))
             and len(rows) <= record["definition"]["target_rounds"]
             and rows == record["state"].get("completed"), "completion ledger/state differs")
    return rows


def _control_blocked(db, definition, work_id=None):
    scopes = ("all", "task:" + definition["task_id"], "work:" + work_id if work_id else "")
    actions = [json.loads(row["data"])["action"] for row in db.execute(
        "SELECT data FROM controls WHERE scope IN (?,?,?)", scopes)]
    _require(all(action in {"resume", "pause", "cancel"} for action in actions), "malformed existing control")
    return any(action != "resume" for action in actions)


def _controls(db, definition):
    _require(not _control_blocked(db, definition), "existing task/global control blocks rounds")


def register_round_plan(store, definition, refs):
    """Register one plan and its first observation atomically, without editing it."""
    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        validate_round_plan(tx, definition)
        _controls(db, definition)
        _schema(db, create=True)
        existing = next((r for r in tx.list_flows() if r["flow_id"] == definition["flow_id"]), None)
        if existing:
            _require(existing["definition_digest"] == sha256(canonical(definition))
                     and existing["evidence"] == refs, "plan cannot replace its original version")
            _completed(db, existing)
            return existing
        first = tx.get_flow(definition["first_experiment_id"])
        _require(definition["adopt_existing_first"] or not first["state"].get("runs"),
                 "completed first intake needs explicit immutable adoption authority")
        state = {"status": "active", "stage": KIND, "mode": "running", "current_work_id": None,
                 "completed": [], "history": [], "archives": [], "waiting": None,
                 "simulation_only": definition["simulation_only"], "real_original_count": 0}
        record = tx.register_flow(definition["flow_id"], definition, refs, state)
        binding = {"ordinal": 1, "experiment_id": first["flow_id"],
                   "definition_digest": first["definition_digest"], "predecessor_digest": None,
                   "simulation_only": definition["simulation_only"], "owned": False}
        try:
            db.execute("INSERT INTO bounded_round_bindings VALUES(?,?,?,?)",
                       (record["flow_id"], 1, first["flow_id"], canonical(binding).decode()))
        except sqlite3.IntegrityError as error:
            raise TaskflowError("bounded rounds: first experiment already belongs to a plan") from error
        tx._event(db, "bounded_round_first_observed", {"plan_id": record["flow_id"], **binding})
        return record


def _lineage(store, candidate_id, predecessor_id):
    seen = set()
    while candidate_id is not None:
        _require(candidate_id not in seen, "candidate lineage cycle")
        if candidate_id == predecessor_id:
            return
        seen.add(candidate_id)
        candidate_id = store.get_candidate(candidate_id).get("parent_id")
    raise TaskflowError("bounded rounds: next candidate is outside preceding candidate lineage")


def bind_round_from_packet(store, identity, *, ordinal, packet_ref, factory_config_ref):
    """Bind one exact next investment through the normal factory, atomically.

    The business coordinator supplies a packet after the preceding original is
    received. This function invents no direction, launches no consumer, and
    cannot register a round beyond the immutable target.
    """
    from taskflow.execution_experiment import _node, _packet, _source
    from taskflow.execution_experiment_factory import start_experiments_from_packet

    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        record = tx.get_flow(identity)
        definition = record["definition"]
        validate_round_plan(tx, definition)
        _schema(db)
        done = _completed(db, record)
        _require(type(ordinal) is int and 2 <= ordinal <= definition["target_rounds"], "round outside bounded target")
        ref, packet = _reference(packet_ref)
        factory = validate_evidence([factory_config_ref])[0]
        matches = [b for b in _bindings(db, identity) if b["ordinal"] == ordinal]
        if matches:
            _require(matches[0].get("packet") == ref and matches[0].get("factory") == factory,
                     "round cannot replace its original investment")
            return record
        _require(record["state"]["status"] == "active" and record["state"]["mode"] == "running",
                 "plan is stopped or paused")
        _controls(db, definition)
        _require(ordinal == len(done) + 1, "preceding original not received or round already bound")
        predecessor = done[-1]
        expected = {"plan_id": identity, "ordinal": ordinal, "root_intent": definition["root_intent"],
                    "predecessor_digest": sha256(canonical(predecessor)),
                    "simulation_only": definition["simulation_only"]}
        _require(canonical(packet.get("bounded_round")) == canonical(expected)
                 and packet.get("batch_id") == definition["batch_id"],
                 "next packet lacks its exact plan/round/predecessor/batch binding")
        _require("original_score_pause" not in packet, "new rounds cannot introduce a task-wide pause follower")
        _revalidate_completed(tx, record, done)
        prepared = start_experiments_from_packet(tx, packet_ref=ref, factory_config_ref=factory, allow_model=False)
        _require(len(prepared["items"]) == 1, "one exact next experiment required")
        item = prepared["items"][0]
        _require(item.get("status") != "attention",
                 "next experiment factory rejected packet: " + str(item.get("reason", "unspecified factory failure")))
        source = tx.get_flow(item["flow_id"])
        _check_source(definition, source)
        _require(source["definition"]["authorization"] == ref
                 and source["definition"].get("factory_config_ref") == factory,
                 "factory result differs from the exact next packet/configuration")
        _require(source["revision"] == 0 and source["state"].get("stage") == "experiment_v1"
                 and source["state"].get("status") == "active", "next experiment already has execution history")
        normalized = _packet(source)
        _require(normalized["single_confirmation"].get("max_additional_attempts") == 0,
                 "a development round must disable same-experiment confirmation")
        work_ids = {role: _node(source, role) for role in _ROLES}
        _require(not any(tx.list_attempts(work) or db.execute("SELECT 1 FROM work WHERE id=?", (work,)).fetchone()
                         for work in work_ids.values()), "next round already has managed work")
        source = _source(tx, source, normalized)
        _require(source["state"].get("stage") == "release", "next source quality is not accepted")
        _lineage(tx, source["state"]["candidate_id"], predecessor["proof"]["candidate_id"])
        excluded = sorted({sid for row in done for sid in (row["proof"]["solver_sid"], row["proof"]["signer_sid"])})
        source = tx.update_flow(source["flow_id"], source["revision"], {
            **source["state"], "excluded_sids": sorted(set(source["state"]["excluded_sids"]) | set(excluded))
        }, {"kind": "bounded_round_identity_exclusions", **expected})
        binding = {"ordinal": ordinal, "experiment_id": source["flow_id"],
                   "definition_digest": source["definition_digest"], "predecessor_digest": expected["predecessor_digest"],
                   "simulation_only": definition["simulation_only"], "owned": True,
                   "packet": ref, "factory": factory, "work_ids": work_ids}
        db.execute("INSERT INTO bounded_round_bindings VALUES(?,?,?,?)",
                   (identity, ordinal, source["flow_id"], canonical(binding).decode()))
        for role, work_id in work_ids.items():
            marker = {"plan_id": identity, "ordinal": ordinal, "experiment_id": source["flow_id"],
                      "definition_digest": source["definition_digest"], "work_id": work_id, "role": role}
            tx._event(db, "bounded_round_work_reserved", marker)
            db.execute("INSERT INTO bounded_round_work VALUES(?,?,?,?)",
                       (work_id, identity, ordinal, canonical(marker).decode()))
        return tx.update_flow(identity, record["revision"], {**record["state"], "waiting": None},
                              {"kind": "bounded_round_successor_bound", **expected, "experiment_id": source["flow_id"]})


def _proof(store, definition, source):
    from taskflow.execution_batch_binding import validate_batch_source
    from taskflow.execution_experiment import _local_configuration_reasons, _native_handoff, _node, _packet
    from taskflow.execution_original_pause import validated_original
    from taskflow.execution_runner import _read, _validate
    from taskflow.records import verify_run

    _check_source(definition, source)
    _require(source["state"].get("status") == "accepted" and source["state"].get("receipt"),
             "original lifecycle terminal receipt not received")
    original = validated_original(store, source)
    _require(original is not None, "original score not received")
    candidate_id = original["candidate_id"]
    quality = store.quality_gate(candidate_id)
    _require(quality["review_id"] == source["state"]["accepted_review_id"], "current quality differs")
    receipt = _read(Path(original["evidence"][0]["path"]))
    run = verify_run(Path(receipt["run"]))
    _require(run["completion"] == "completed" and run["input_binding"] == "matched", "original run is incomplete or unbound")
    if not definition["simulation_only"]:
        _require(receipt.get("nature") != "test_fixture" and not receipt.get("simulation_only"), "simulated score cannot count as real")
        _require(source["definition"].get("source_author_flow_id"), "real round requires its native author-quality source")
        _native_handoff(store, source, candidate_id)
        _require(not _local_configuration_reasons(source, receipt, run), "original local validity is not established")
    first = {**source, "state": {**source["state"], "run_index": 0}}
    for role in _ROLES:
        attempts = store.list_attempts(_node(first, role))
        _require(len(attempts) == 1, "one exact original phase required")
        attempt = attempts[0]
        manifest = _validate(Path(attempt["runtime_path"]))
        if definition["simulation_only"]:
            _require(manifest["mode"] == "test_fixture" and receipt["nature"] == "test_fixture",
                     "simulation plan only accepts fixture phases")
        else:
            _require(manifest["mode"] != "test_fixture"
                     and validate_batch_source(attempt, manifest, store) is True,
                     "native phase differs from confirmed batch")
    terminal = evidence_ref(Path(source["state"]["receipt"]), "original lifecycle terminal receipt")
    body = _read(Path(terminal["path"]))
    _require(body.get("flow_id") == source["flow_id"] and body.get("status") == "accepted"
             and body.get("controller") == source["definition"]["owner"]
             and body.get("history") == source["state"].get("history")
             and body.get("archives") == source["state"].get("archives")
             and body.get("result") == source["state"].get("result")
             and Path(terminal["path"]).parent == Path(source["definition"]["directory"]),
             "terminal original receipt differs")
    return {**original, "simulation_only": definition["simulation_only"], "terminal_receipt": terminal,
            "quality_review_id": quality["review_id"], "purpose": _packet(source)["business_investment"]["unresolved_business_question"]}


def _revalidate_completed(store, record, done):
    for entry in done:
        source = store.get_flow(entry["experiment_id"])
        _require(_proof(store, record["definition"], source) == entry["proof"], "previous original evidence changed")


def _save_wait(store, record, reason):
    state = {**record["state"], "waiting": reason, "current_work_id": None}
    if state == record["state"]:
        return record
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": "bounded_round_waiting", "reason": reason})


def advance_round_plan(store, record):
    """Advance one existing mechanical stage; never call a model or launcher."""
    from taskflow.execution_lifecycle import advance_lifecycle

    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        record = tx.get_flow(record["flow_id"])
        validate_round_plan(tx, record["definition"])
        _schema(db)
        done = _completed(db, record)
        if record["state"]["status"] == "accepted" or record["state"]["mode"] != "running":
            return record
        if _control_blocked(db, record["definition"]):
            return _save_wait(tx, record, "existing_control_blocks_rounds")
        _revalidate_completed(tx, record, done)
        ordinal = len(done) + 1
        bindings = [r for r in _bindings(db, record["flow_id"]) if r["ordinal"] == ordinal]
        if not bindings:
            return _save_wait(tx, record, "next_business_binding_required")
        binding = bindings[0]
        source = tx.get_flow(binding["experiment_id"])
        _require(source["definition_digest"] == binding["definition_digest"], "bound experiment changed")
        if source["state"].get("status") == "exception":
            return _save_wait(tx, record, "source_exception_requires_original_recovery")
        if source["state"].get("status") != "accepted" or not source["state"].get("receipt"):
            if not binding["owned"]:
                return _save_wait(tx, record, "observing_locked_first_original")
            source = advance_lifecycle(tx, source["flow_id"])
            current = source["state"].get("current_work_id")
            state = {**record["state"], "current_work_id": current, "waiting": "bound_round_in_progress"}
            return tx.update_flow(record["flow_id"], record["revision"], state,
                                  {"kind": "bounded_round_stage_followed", "ordinal": ordinal, "source_revision": source["revision"]})
        proof = _proof(tx, record["definition"], source)
        entry = {"ordinal": ordinal, "experiment_id": source["flow_id"], "proof": proof}
        try:
            db.execute("INSERT INTO bounded_round_completions VALUES(?,?,?,?,?,?)", (
                record["flow_id"], ordinal, proof["run_digest"], proof["record_digest"],
                proof["solver_attempt_id"], canonical(entry).decode()))
            for sid in (proof["solver_sid"], proof["signer_sid"]):
                db.execute("INSERT INTO bounded_round_sessions VALUES(?,?,?)", (sid, record["flow_id"], ordinal))
        except sqlite3.IntegrityError as error:
            raise TaskflowError("bounded rounds: original run/score/attempt/session already consumed") from error
        completed = [*done, entry]
        finished = len(completed) == record["definition"]["target_rounds"]
        state = {**record["state"], "completed": completed, "current_work_id": None,
                 "real_original_count": 0 if record["definition"]["simulation_only"] else len(completed),
                 "status": "accepted" if finished else "active", "waiting": None if finished else "next_business_binding_required"}
        if finished:
            state.update(stage="complete", result={"completed_rounds": len(completed),
                         "real_original_count": state["real_original_count"],
                         "simulation_only": record["definition"]["simulation_only"], "target_reached": True})
        return tx.update_flow(record["flow_id"], record["revision"], state,
                              {"kind": "bounded_round_original_counted", "ordinal": ordinal, "entry_digest": sha256(canonical(entry))})


def set_round_plan_mode(store, identity, mode, reason):
    """Pause/cancel only this plan's future claims; preserve running originals."""
    _require(mode in {"running", "paused", "cancelled"} and isinstance(reason, str) and reason.strip(), "explicit mode/reason required")
    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        record = tx.get_flow(identity)
        validate_round_plan(tx, record["definition"])
        _schema(db)
        _completed(db, record)
        if record["state"].get("mode") == mode:
            return record
        _require(record["state"].get("mode") != "cancelled" and record["state"]["status"] != "accepted", "stopped plan cannot resume")
        return tx.update_flow(identity, record["revision"], {**record["state"], "mode": mode},
                              {"kind": "bounded_round_mode", "mode": mode, "reason": reason})


def resume_round_plan(store, identity, *, expected_revision, controller, resolution, evidence):
    """Resume repaired plan bookkeeping only; never rewind a source or attempt.

    Normal pauses need no repair. A source exception must first be recovered
    through its existing lifecycle. Original exception receipts are retained.
    """
    _require(type(expected_revision) is int and expected_revision >= 0
             and isinstance(resolution, str) and resolution.strip(), "explicit recovery revision/reason required")
    refs = validate_evidence(evidence)
    _require(bool(refs), "recovery evidence required")
    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        record = tx.get_flow(identity)
        validate_round_plan(tx, record["definition"])
        _schema(db)
        done = _completed(db, record)
        state = record["state"]
        _require(record["revision"] == expected_revision and controller == record["definition"]["owner"],
                 "recovery revision/controller differs")
        _require(state["status"] == "exception" and state["mode"] != "cancelled"
                 and len(done) < record["definition"]["target_rounds"], "plan is not a recoverable bookkeeping exception")
        _controls(db, record["definition"])
        _revalidate_completed(tx, record, done)
        binding = next((b for b in _bindings(db, identity) if b["ordinal"] == len(done) + 1), None)
        if binding:
            source = tx.get_flow(binding["experiment_id"])
            _check_source(record["definition"], source)
            _require(source["definition_digest"] == binding["definition_digest"]
                     and source["state"]["status"] != "exception", "recover original source through its existing lifecycle first")
            if source["state"]["status"] == "accepted":
                _proof(tx, record["definition"], source)
        prior = evidence_ref(Path(state["receipt"]), "original bounded-plan exception retained") if state.get("receipt") else None
        changed = {**state, "status": "active", "waiting": None, "current_work_id": None,
                   "prior_receipts": [*state.get("prior_receipts", []), *([prior] if prior else [])],
                   "history": [*state["history"], {"kind": "bounded_round_bookkeeping_recovered",
                       "resolution": resolution, "evidence": refs, "previous_exception": state.get("exception"),
                       "previous_receipt": prior}]}
        changed.pop("exception", None)
        changed.pop("receipt", None)
        return tx.update_flow(identity, record["revision"], changed, {"kind": "bounded_round_bookkeeping_recovered"})


@contextmanager
def round_claim_scope(store, record, work_id):
    """Route one explicitly model-enabled consumer claim, not user authentication."""
    token = _CLAIM.set((str(Path(store.path).resolve()), record["flow_id"], work_id))
    try:
        yield
    finally:
        _CLAIM.reset(token)


def _work_reservation(db, work_id):
    """Locate ownership before requiring round-only schema invariants."""
    rows = db.execute("SELECT data FROM events WHERE kind='bounded_round_work_reserved' "
                      "AND CASE WHEN json_valid(data) THEN json_extract(data,'$.work_id')=? ELSE 0 END",
                      (work_id,)).fetchall()
    table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bounded_round_work'").fetchone()
    if not rows and not table:
        return None
    # A corrupt ledger for another plan must not disable unrelated workers.
    # The append-only per-work event still identifies owned work if its row
    # or the complete reservation table has been lost.
    reserved = None
    if table:
        columns = [r[1] for r in db.execute("PRAGMA table_info(bounded_round_work)")]
        if {"work_id", "data", "plan_id", "ordinal"} <= set(columns):
            reserved = db.execute("SELECT * FROM bounded_round_work WHERE work_id=?", (work_id,)).fetchone()
    if not rows and reserved is None:
        return None
    _schema(db)
    _require(len(rows) == 1, "missing or ambiguous round work reservation")
    marker = json.loads(rows[0]["data"])
    _require(reserved is not None and json.loads(reserved["data"]) == marker
             and marker["work_id"] == work_id and marker["plan_id"] == reserved["plan_id"]
             and marker["ordinal"] == reserved["ordinal"], "round work reservation missing or changed")
    return marker


def round_claim_blocked(db, work):
    """Fail closed for reserved work, including a lost/corrupt reservation row."""
    marker = _work_reservation(db, work["work_id"])
    if marker is None:
        return False
    main = next(r[2] for r in db.execute("PRAGMA database_list") if r[1] == "main")
    tx = _TransactionStore(SimpleNamespace(path=Path(main)), db)
    record = tx.get_flow(marker["plan_id"])
    validate_round_plan(tx, record["definition"])
    done = _completed(db, record)
    binding = [b for b in _bindings(db, record["flow_id"]) if b["ordinal"] == marker["ordinal"]]
    _require(len(binding) == 1 and binding[0]["owned"] is True
             and binding[0]["experiment_id"] == marker["experiment_id"]
             and binding[0]["work_ids"].get(marker["role"]) == work["work_id"], "bound round registry differs")
    if (record["state"]["status"] != "active" or record["state"]["mode"] != "running"
            or marker["ordinal"] != len(done) + 1
            or _CLAIM.get() != (str(Path(main).resolve()), record["flow_id"], work["work_id"])):
        return True
    if _control_blocked(db, record["definition"], work["work_id"]):
        return True
    _revalidate_completed(tx, record, done)
    source = tx.get_flow(marker["experiment_id"])
    _check_source(record["definition"], source)
    _require(source["definition_digest"] == marker["definition_digest"]
             and source["state"].get("current_work_id") == work["work_id"]
             and all(work.get(k) == source["definition"][k] for k in ("task_id", "policy_id"))
             and work.get("candidate_id") == source["state"]["candidate_id"] and work.get("role") == marker["role"],
             "reserved work differs from current bound round")
    quality = tx.quality_gate(work["candidate_id"])
    _require(quality["review_id"] == source["state"]["accepted_review_id"], "bound round quality changed before claim")
    from taskflow.execution_runner import _regular, _validate

    manifest = _validate(Path(work["runtime_path"]), before_run=True)
    _require(PROTOCOL.encode() in _regular(Path(work["runtime_path"]) / "request.md"),
             "reserved runtime lacks its immutable launch protocol")
    _require(not record["definition"]["simulation_only"] or manifest["mode"] == "test_fixture",
             "simulation cannot launch a native role")
    return False


def decorate_round_runner(db, work, runner):
    """Add the launch protocol before normal request/manifest freezing only."""
    marker = _work_reservation(db, work["work_id"])
    if marker is None:
        return runner
    _require(marker["role"] == work["role"], "reserved runner role differs")
    sections = list(runner.get("request_sections", []))
    sections.append({"kind": "operation", "text": PROTOCOL + "\nUse the registered bounded-round launch guard."})
    return {**runner, "request_sections": sections}


@contextmanager
def round_launch_guard(store, attempt):
    """Recheck and enter a launch once; never hold a writer for inference.

    Pause after this entry affects future launches, not this starting attempt.
    SQLite and process start are not atomic; uncertain attempts stay with their
    original worker and are never redrawn by this controller.
    """
    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        work = tx.get_work(attempt["work_id"])
        _require(not round_claim_blocked(db, work), "plan no longer permits this launch")
        _require(tx.get_attempt(attempt["attempt_id"])["state"] == "starting", "starting attempt required")
        old = db.execute("SELECT 1 FROM events WHERE kind='bounded_round_launch_entered' "
                         "AND json_extract(data,'$.attempt_id')=?", (attempt["attempt_id"],)).fetchone()
        _require(old is None, "original round launch already entered")
        tx._event(db, "bounded_round_launch_entered", {
            "attempt_id": attempt["attempt_id"], "work_id": work["work_id"], "claim_scope": list(_CLAIM.get())})
    yield
