"""One explained public capacity continuation; old terminal attempts stay intact."""

from datetime import datetime
import importlib.util
import json
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, sha256
from taskflow.execution_runner import _info, _read, _regular, _validate, _validated_collection
from taskflow.execution_store import evidence_ref, validate_evidence


REASON = "fresh public reconstruction failed or reused the author context"
KIND = "explained_public_capacity_continuation_v1"
CAPACITY = "Selected model is at capacity. Please try a different model."


def _require(ok, message):
    if not ok:
        raise TaskflowError("public capacity continuation: " + message)


def _time(collected):
    value = datetime.fromisoformat(collected["termination"]["ended_at"].replace("Z", "+00:00"))
    _require(value.tzinfo is not None, "actual termination needs a timezone")
    return value.timestamp()


def _profile(manifest):
    if manifest["mode"] == "existing_isolated":
        plan = _read(Path(manifest["isolated"]["runtime"]) / "plan.json")
        return plan["model"], Path(plan["config"]["cli"])
    _require(manifest["mode"] == "native_role", "availability must be an actual CLI execution")
    return manifest["model"], Path(manifest["argv"][0])


def _actual(store, attempt):
    from taskflow.execution import _check_work_binding

    root = Path(attempt["runtime_path"])
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    work = store.get_work(attempt["work_id"])
    candidate = store.get_candidate(attempt["candidate_id"])
    _check_work_binding(work, candidate, manifest)
    _require(all(attempt[key] == work[key] for key in
                 ("task_id", "candidate_id", "intent_id", "policy_id", "request_digest", "runtime_path"))
             and manifest["request"]["sha256"] == work["request_digest"]
             and attempt["policy_digest"] == store.get_policy(attempt["policy_id"])["digest"]
             and attempt.get("cli_sid") == collected.get("sid")
             and collected.get("process_ended") is True,
             "managed work/attempt differs from its sealed source")
    validate_evidence(attempt["evidence"])
    return root, manifest, collected


def _received(record):
    state = record["state"]
    path = Path(state["receipt"])
    raw = _read(path)
    _require(raw.get("flow_id") == record["flow_id"]
             and raw.get("controller") == record["definition"]["owner"]
             and raw.get("status") == "exception" and raw.get("exception") == state["exception"]
             and all(raw.get(key) == state.get(key) for key in ("history", "archives", "result"))
             and path.name == "exception-" + sha256(canonical(raw))[:16] + ".json",
             "original received exception differs")
    return evidence_ref(path, "original received exception retained")


def _received_event(store, record):
    """Bind a received original state to its actual append-only ledger event."""
    with store._db() as db:
        row = db.execute("SELECT kind,data FROM events WHERE seq=?", (record["event_seq"],)).fetchone()
    event = json.loads(row["data"]) if row is not None else {}
    _require(row is not None and row["kind"] == "flow_updated"
             and event.get("flow_id") == record["flow_id"] and event.get("revision") == record["revision"]
             and event.get("transition", {}).get("kind") == "lifecycle_result_received"
             and canonical(event.get("state")) == canonical(record["state"]),
             "original received exception lacks its actual source event")
    return {"flow_id": record["flow_id"], "event_seq": record["event_seq"],
            "revision": record["revision"], "event_sha256": sha256(canonical(event)),
            "state_sha256": sha256(canonical(record["state"]))}


def _no_formal(store, parent):
    """Read metadata only; neither a dashboard nor any model is needed."""
    from taskflow.execution_experiment import _node, _packet, _run_id

    candidate_id = parent["state"]["candidate_id"]
    with store._db() as db:
        works = [json.loads(row["data"]) for row in db.execute("SELECT data FROM work")]
        attempts = [json.loads(row["data"]) for row in db.execute("SELECT data FROM attempts")]
        decisions = [json.loads(row["data"]) for row in db.execute("SELECT data FROM decisions")]
    for row in decisions:
        _require(row.get("candidate_id") != candidate_id or row.get("action") != "experiment",
                 "candidate already has an experiment decision")
    for row in [*works, *attempts]:
        _require(row.get("candidate_id") != candidate_id or row.get("role") != "solver",
                 "candidate already has a formal execution")
    dependents = []
    for related in store.list_flows():
        definition, state = related["definition"], related["state"]
        if definition.get("source_author_flow_id") != parent["flow_id"]:
            continue
        dependents.append(related)
        _require(definition.get("kind") == "experiment_v1"
                 and state.get("pending_business_scope") is None
                 and state.get("current_work_id") is None
                 and state.get("stage") == "experiment_v1"
                 and not any(state.get(key) for key in
                     ("candidate_id", "native_quality_handoff", "release", "runs", "formal_runtime", "formal_outer")),
                 "related experiment has a hold or formal progress")
        packet = _packet(related)
        run_id, work_id = _run_id(related, packet), _node(related, "solver")
        technical = definition["technical"]
        inner = Path(technical["formal_runtime_root"]) / run_id
        paths = [Path(technical["formal_runtime_root"]), Path(technical["publication_root"]),
                 Path(definition["directory"]) / "runtimes" / work_id,
                 inner.with_name(inner.name + ".formal-intent.json")]
        _require(not any(path.exists() or path.is_symlink() for path in paths),
                 "related experiment has existing formal preparation/publication")
        _require(not any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
                         for row in [*works, *attempts])
                 and not any(row.get("decision_id") == _node(related, "experiment-decision") for row in decisions),
                 "related experiment has an existing exact work/decision")
    return dependents


def _dependent_binding(store, parent, author, related):
    """A once source recovery cannot adopt another original formal purpose."""
    from taskflow.execution_author_packet import author_packet_matches
    from taskflow.execution_experiment import _packet

    _require(len(related) <= 1, "ambiguous related experiments; a unique dependent is required")
    if not related:
        return None
    record = related[0]
    definition, state = record["definition"], record["state"]
    expected = "source author-quality flow stopped with its original exception: " + str(parent["state"]["exception"])
    _require(store.get_flow(record["flow_id"]) == record
             and all(definition.get(key) == parent["definition"][key] for key in ("task_id", "owner", "policy_id"))
             and state.get("status") == "exception" and state.get("stage") == "experiment_v1"
             and state.get("exception") == {"stage": "experiment_v1", "reason": expected}
             and state.get("pending_business_scope") is None and state.get("current_work_id") is None
             and state.get("receipt"), "unique dependent is not its original received source exception")
    packet = _packet(record)
    authority = validate_evidence([definition["authorization"]])[0]
    _require(author["attempt_id"] == packet["source_author_attempt"]
             and author["intent_id"] == packet["source_author_intent"]
             and author["cli_sid"] == packet["source_author_sid"]
             and author_packet_matches(store, parent=parent, packet=packet, authorization=authority,
                 packet_transport=state.get("source_business_packet_transport")),
             "unique dependent differs from its actual original author/authority/packet")
    return {"flow_id": record["flow_id"], "definition_digest": record["definition_digest"],
            "authority": authority, "original_received_exception": state["exception"],
            "original_receipt": _received(record), "received_event": _received_event(store, record),
            "source_received_event": _received_event(store, parent),
            "packet_sha256": sha256(canonical(packet)),
            "source_business_packet_transport": state.get("source_business_packet_transport")}


def validate_public_capacity_continuation(store, record, *, failed_attempt_id, availability_attempt_id):
    """Read original failed evidence and a later successful same-CLI execution."""
    from taskflow.execution_author import _author, _binding, _candidate_structure, _handoff, _roles
    from taskflow.execution_flow import _identity, _render

    definition, state = record["definition"], record["state"]
    _require(definition.get("kind") == "author_quality_v1" and state.get("status") == "exception"
             and state.get("stage") == "public_result"
             and state.get("exception") == {"stage": "public_result", "reason": REASON}
             and state.get("pending_business_scope") is None and state.get("receipt")
             and not state.get("public_capacity_continuation")
             and not any(state.get(key) for key in ("child_flow_id", "public_attempt_id", "public_archive", "result")),
             "exact received first-public failure without a business hold required")
    previous = _received(record)
    author, _ = _author(store, definition)
    _require(author["state"] == "completed" and state.get("author_archive"), "original author is not complete")
    _require(_handoff(Path(state["author_archive"]), definition, author)["disposition"] == "candidate_complete",
             "original author has no formed candidate")
    candidate = store.get_candidate(state["candidate_id"])
    check = _candidate_structure(Path(state["candidate_root"]), definition["task_id"])
    _require(candidate.get("author_attempt") == author["attempt_id"]
             and candidate.get("parent_id") == author["candidate_id"]
             and all(check[key] == candidate[key] for key in ("task_id", "input_digest", "content_digest", "evaluation_digest")),
             "actual frozen author candidate changed")
    validate_evidence(candidate["files"])
    failed = store.get_attempt(failed_attempt_id)
    _require(failed["state"] == "failed" and failed["role"] == "quality"
             and failed["candidate_id"] == candidate["candidate_id"]
             and failed["policy_id"] == definition["policy_id"]
             and failed["work_id"] == state["public_work_id"] == state["current_work_id"]
             == _identity(record["flow_id"], "public-work")
             and failed["intent_id"] == _identity(record["flow_id"], "public")
             and failed.get("cli_sid") and failed["cli_sid"] != author["cli_sid"],
             "failed original public attempt identity differs")
    root, manifest, collected = _actual(store, failed)
    _require(manifest["mode"] == "existing_isolated" and manifest["isolated"]["flavor"] == "quality"
             and collected.get("nature") == "real_cli_attempt"
             and collected.get("model_completed") is False and collected.get("delivery_complete") is False
             and collected.get("technical_accepted") is False
             and failed.get("response_complete") is False and failed.get("technical_accepted") is False
             and collected["termination"].get("timed_out") is False
             and type(collected["termination"].get("returncode")) is int
             and collected["termination"].get("returncode") == failed["process_exit"] == 0,
             "only terminal capacity failure, never timeout/uncertain/completion, can continue")
    events = [json.loads(line) for line in _regular(root / "observed-trace.jsonl").decode().splitlines() if line.strip()]
    failures = [row for row in events if row.get("type") == "turn.failed"]
    _require(len(failures) == 1 and failures[0].get("error", {}).get("message") == CAPACITY
             and not any(row.get("type") == "turn.completed" for row in events)
             and all(row.get("message") == CAPACITY for row in events if row.get("type") == "error"),
             "actual original trace does not show the exact capacity failure")
    role = _roles(store, definition)["public_rebuild"]
    adapter = Path(role["prepare"]["adapter"])
    _require(sha256(_regular(adapter)) == role["prepare"]["sha256"], "approved public adapter changed")
    module_spec = importlib.util.spec_from_file_location("taskflow_public_capacity_validator", adapter)
    _require(module_spec is not None and module_spec.loader is not None, "approved adapter unavailable")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    inner = Path(manifest["isolated"]["runtime"])
    plan = module.validate(inner, before_run=False)
    package = _read(inner / "work/package.json")
    public = {row["package_path"]: row for row in candidate["files"] if row["role"] in PUBLIC_ROLES}
    _require(package.get("input_digest") == candidate["input_digest"]
             and {row["path"] for row in package["files"]} == set(public)
             and set(plan["public_tree"]) == {*public, "package.json"}, "strict public membership differs")
    declaration = _read(Path(state["candidate_root"]) / "task.json")
    expected_public = [{"id": row["id"], "path": row["path"], "role": row["role"],
                        **_info(Path(public[row["path"]]["path"]))}
                       for row in declaration["files"] if row["role"] in PUBLIC_ROLES]
    _require(canonical(expected_public) == canonical(package["files"])
             and sha256(canonical(expected_public)) == candidate["input_digest"],
             "public package IDs/order/metadata differ from the frozen input digest")
    for row in package["files"]:
        actual = _info(inner / "work" / row["path"])
        _require(actual == _info(Path(public[row["path"]]["path"]))
                 and actual["sha256"] == row["sha256"] == public[row["path"]]["sha256"]
                 and row["role"] == public[row["path"]]["role"] and row["size"] == actual["size"],
                 "frozen public payload/role changed")
    denied = {row["path"] for row in plan["denied"]}
    _require({row["path"] for row in candidate["files"] if row["role"] not in PUBLIC_ROLES} <= denied
             and {plan["config"]["repository"], plan["config"]["codex_home"]} <= denied,
             "original strict public private/repository/home denial is incomplete")
    profile, cli = _profile(manifest)
    context = {**_binding(candidate), "flow_id": record["flow_id"], "node_id": failed["intent_id"],
               "intent_id": failed["intent_id"], "runtime": str(root), "work": str(root.parent / "work"),
               "scratch": str(root.parent / "scratch"), "candidate_root": state["candidate_root"],
               "inputs_root": str(root / "inputs")}
    expected_config = _render(role["prepare"]["config"], context)
    _require(all(plan["config"].get(key) == value for key, value in expected_config.items())
             and manifest["timeout_seconds"] == role["runner"]["timeout_seconds"],
             "original isolated configuration differs from the approved public role")
    available = store.get_attempt(availability_attempt_id)
    _, available_manifest, available_collected = _actual(store, available)
    available_profile, available_cli = _profile(available_manifest)
    _require(available["state"] == "completed" and available.get("cli_sid") != failed["cli_sid"]
             and available_collected.get("nature") == "real_cli_attempt"
             and available["policy_id"] == failed["policy_id"]
             and all(available_collected.get(key) is True for key in
                     ("technical_accepted", "model_completed", "delivery_complete", "process_ended"))
             and available_collected["termination"].get("returncode") == 0
             and _time(available_collected) > _time(collected)
             and all(profile.get(key) == available_profile.get(key) for key in ("requested", "reasoning_effort"))
             and cli.resolve() == available_cli.resolve() and _info(cli) == _info(available_cli)
             and profile["requested"] == role["runner"]["model"]
             and profile["reasoning_effort"] == role["runner"]["reasoning_effort"],
             "later actual same-model/CLI/policy completion is absent")
    child_id = _identity(record["flow_id"], "quality-flow")
    _require(not any(item["flow_id"] == child_id for item in store.list_flows()), "quality child already exists")
    with store._db() as db:
        works = [json.loads(row["data"]) for row in db.execute("SELECT data FROM work WHERE candidate_id=?", (candidate["candidate_id"],))]
    _require({row["work_id"] for row in works} == {failed["work_id"]}, "another current-candidate execution already exists")
    dependent = _dependent_binding(store, record, author, _no_formal(store, record))
    return {"schema_version": 1, "kind": KIND, "ordinal": 1,
            "flow_id": record["flow_id"], "definition_digest": record["definition_digest"],
            "original_revision": record["revision"], "original_event_seq": record["event_seq"],
            "original_state_digest": sha256(canonical(state)),
            "dependent_experiment": dependent,
            "source_author_attempt": author["attempt_id"], "source_author_sid": author["cli_sid"],
            "candidate_binding": _binding(candidate), "policy_id": definition["policy_id"],
            "failed_attempt_id": failed["attempt_id"], "failed_work_id": failed["work_id"], "failed_sid": failed["cli_sid"],
            "previous_receipt": previous, "public_role": role,
            "failed_manifest": evidence_ref(root / "manifest.json", "original failed public manifest"),
            "failed_collection": evidence_ref(root / "collection.json", "original sealed failed public collection"),
            "failed_trace": evidence_ref(root / "observed-trace.jsonl", "actual original capacity turn.failed"),
            "public_plan": evidence_ref(inner / "plan.json", "original strict public plan"),
            "availability_attempt_id": available["attempt_id"], "availability_sid": available["cli_sid"],
            "availability_runtime": available["runtime_path"], "availability_ended_at": available_collected["termination"]["ended_at"],
            "availability_manifest": evidence_ref(Path(available["runtime_path"]) / "manifest.json", "actual later same-CLI manifest"),
            "availability_collection": evidence_ref(Path(available["runtime_path"]) / "collection.json", "actual later same-CLI normal completion")}


def resume_public_capacity_once(store, record, resolution, refs):
    from taskflow.execution_lifecycle import _save
    from taskflow.execution_runner import archive_full_attempt

    proof = validate_public_capacity_continuation(store, record,
        failed_attempt_id=resolution["failed_attempt_id"], availability_attempt_id=resolution["availability_attempt_id"])
    original = store.get_attempt(proof["failed_attempt_id"])
    archive = archive_full_attempt(Path(original["runtime_path"]),
                                  Path(record["definition"]["directory"]) / "archives" / original["attempt_id"])
    state = dict(record["state"])
    proof["failed_archive"] = archive["path"]
    state["public_capacity_continuation"] = proof
    state["archives"] = [*state["archives"], archive["path"]]
    state["history"] = [*state["history"], {"kind": KIND, "resolution": resolution, "evidence": refs,
        "previous_exception": state["exception"], "previous_receipt": proof["previous_receipt"],
        "binding": proof, "new_model_dispatched": False}]
    state.update(status="active", stage="public_prepare", current_work_id=None, waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_public_capacity_continuation_authorized")


def resume_public_capacity_dependency(store, record, resolution, refs):
    """Clear only the exact received propagation after the parent once-resume."""
    from taskflow.execution_author import _author
    from taskflow.execution_author_packet import author_packet_matches
    from taskflow.execution_experiment import _packet
    from taskflow.execution_lifecycle import _save

    definition, state = record["definition"], record["state"]
    _require(definition.get("kind") == "experiment_v1" and definition.get("source_author_flow_id")
             and state.get("status") == "exception" and state.get("stage") == "experiment_v1"
             and state.get("current_work_id") is None and state.get("pending_business_scope") is None
             and not any(state.get(key) for key in ("candidate_id", "release", "runs", "native_quality_handoff")),
             "exact initial dependent exception without a hold required")
    previous = _received(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    ps, pd = parent["state"], parent["definition"]
    proof = ps.get("public_capacity_continuation") or {}
    _require(ps.get("status") == "active" and ps.get("stage") == "public_prepare"
             and ps.get("current_work_id") is None
             and ps.get("pending_business_scope") is None and ps.get("exception") is None
             and proof.get("kind") == KIND and proof.get("ordinal") == 1
             and type(proof.get("ordinal")) is int
             and proof.get("definition_digest") == parent["definition_digest"]
             and all(pd.get(key) == definition[key] for key in ("task_id", "owner", "policy_id")),
             "source has not resolved this exact original first-public failure")
    dependent = proof.get("dependent_experiment")
    _require(isinstance(dependent, dict) and dependent.get("flow_id") == record["flow_id"]
             and dependent.get("definition_digest") == record["definition_digest"]
             and dependent.get("authority") == validate_evidence([definition["authorization"]])[0]
             and dependent.get("original_received_exception") == state["exception"]
             and dependent.get("original_receipt") == previous
             and dependent.get("received_event") == _received_event(store, record),
             "dependent is not the uniquely bound original experiment/purpose/received exception")
    history = [x for x in ps["history"] if x.get("kind") == KIND]
    _require(len(history) == 1 and history[0].get("binding") == proof
             and history[0].get("previous_exception") == {"stage": "public_result", "reason": REASON},
             "source continuation lacks its unique original failure lineage")
    with store._db() as db:
        row = db.execute("SELECT kind,data FROM events WHERE seq=?", (proof["original_event_seq"],)).fetchone()
    event = json.loads(row["data"]) if row is not None else {}
    _require(row is not None and row["kind"] == "flow_updated" and event.get("flow_id") == parent["flow_id"]
             and event.get("revision") == proof["original_revision"]
             and sha256(canonical(event.get("state"))) == proof["original_state_digest"],
             "source original received state differs from its immutable event")
    original_parent = {**parent, "state": event["state"], "revision": proof["original_revision"],
                       "event_seq": proof["original_event_seq"]}
    rebuilt = validate_public_capacity_continuation(store, original_parent,
        failed_attempt_id=proof["failed_attempt_id"], availability_attempt_id=proof["availability_attempt_id"])
    _require(rebuilt == {key: value for key, value in proof.items() if key != "failed_archive"},
             "source continuation original failure/availability proof differs")
    original = _read(Path(proof["previous_receipt"]["path"]))
    validate_evidence([proof["previous_receipt"], proof["failed_collection"], proof["availability_collection"]])
    expected = "source author-quality flow stopped with its original exception: " + str(original["exception"])
    _require(state.get("exception") == {"stage": "experiment_v1", "reason": expected},
             "dependent exception is not this exact source failure propagation")
    packet = _packet(record)
    author, _ = _author(store, pd)
    _require(author["attempt_id"] == packet["source_author_attempt"] == proof["source_author_attempt"]
             and author["intent_id"] == packet["source_author_intent"]
             and author["cli_sid"] == packet["source_author_sid"] == proof["source_author_sid"]
             and author_packet_matches(store, parent=parent, packet=packet,
                 authorization=definition["authorization"],
                 packet_transport=state.get("source_business_packet_transport")),
             "dependent author/intent/SID/original purpose differs")
    _no_formal(store, parent)
    state["history"] = [*state["history"], {"kind": "resolved_public_capacity_dependency",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"],
        "previous_receipt": previous, "source_flow_id": parent["flow_id"], "source_revision": parent["revision"],
        "source_continuation": proof, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_public_capacity_dependency_resumed")
