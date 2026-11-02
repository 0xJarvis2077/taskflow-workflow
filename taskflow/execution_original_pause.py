"""One explicitly authorized original intake pauses its task, without a model.

The source is one immutable experiment definition, not a score threshold or a
task-name lookup. Original evidence is read outside the short atomic store CAS.
"""

from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, sha256, validate_id
from taskflow.execution_formal import _handoff_errors
from taskflow.execution_store import evidence_ref, validate_evidence


KIND = "original_score_pause_v1"


def original_pause_grant(packet: dict) -> dict | None:
    """Only a typed explicit grant opts new experiment packets into stopping."""
    if "original_score_pause" not in packet:
        return None
    grant = packet["original_score_pause"]
    if (not isinstance(grant, dict) or set(grant) - {"round_scope", "first_run_only", "authority_evidence"}
            or grant.get("round_scope") != "this_round_original"
            or type(grant.get("first_run_only")) is not bool or grant["first_run_only"] is not True):
        raise TaskflowError("original_score_pause requires the explicit typed first-original round grant")
    if "authority_evidence" in grant:
        validate_evidence([grant["authority_evidence"]])
    return grant


def prepare_original_pause(store: object, source: dict, repository: Path) -> dict:
    """Derive a single immutable control follower from its source's own grant."""
    from taskflow.execution_experiment import _node, _packet
    from taskflow.execution_experiment_factory import launch_lifecycle_consumer
    from taskflow.execution_flow import _identity, _publish_once
    from taskflow.execution_lifecycle import register_lifecycle
    from taskflow.execution_runner import _read

    source_ref = source["definition"]["authorization"]
    raw = _read(Path(validate_evidence([source_ref])[0]["path"]))
    grant = original_pause_grant(raw)
    if grant is None:
        raise TaskflowError("source has no explicit original_score_pause producer grant")
    definition = source["definition"]
    flow_id = _identity(source["flow_id"], "original-score-pause")
    authority = {"schema_version": 1, "kind": "derived_original_score_pause_authority_v1",
        "controller": definition["owner"], "task_id": definition["task_id"], "policy_id": definition["policy_id"],
        "source_flow_id": source["flow_id"], "source_definition_digest": source["definition_digest"],
        "first_run_purpose": _packet(source)["business_investment"]["unresolved_business_question"],
        "action": "pause_after_original_score", "round_scope": grant["round_scope"],
        "first_run_only": True, "source_packet": source_ref}
    path = Path(definition["directory"]) / "original-score-pause-authority.json"
    _publish_once(path, authority)
    authority_ref = evidence_ref(path, "mechanical adoption of the source's explicit first-original pause grant")
    follower = {"schema_version": 1, "kind": KIND, "flow_id": flow_id,
        **{key: authority[key] for key in ("task_id", "policy_id", "source_flow_id", "source_definition_digest", "first_run_purpose")},
        "owner": definition["owner"], "authorization": authority_ref,
        "directory": str(Path(definition["directory"]) / "original-score-pause")}
    refs = [authority_ref, source_ref]
    if "authority_evidence" in grant:
        refs.append(grant["authority_evidence"])
    try:
        record = register_lifecycle(store, follower, refs)
    except TaskflowError:
        # Concurrent identical registration may have committed first. Adopt only
        # those exact bytes and refs; a different plan remains a hard conflict.
        record = store.get_flow(flow_id)
        if record["definition_digest"] != sha256(canonical(follower)) or record["evidence"] != validate_evidence(refs):
            raise
    if record["state"]["status"] != "accepted":
        second = {**source, "state": {**source["state"], "run_index": 1}}
        record = store.prepare_original_pause_control(flow_id, record["definition_digest"],
            "work:" + _node(second, "solver"), "Explicit one-original round: prevent an old confirmation claim before first-original task pause.")
    consumer = launch_lifecycle_consumer(store, record, repository)
    return {"flow_id": flow_id, "definition_digest": record["definition_digest"],
            "control_consumer": consumer, "model_consumer": False}


def validate_original_pause_definition(store: object, definition: dict, *, check_unique: bool = True) -> dict:
    """Validate an explicit single-round source; never infer a baseline's scope."""
    from taskflow.execution_experiment import _packet
    from taskflow.execution_runner import _read

    for key in ("flow_id", "task_id", "owner", "policy_id", "source_flow_id"):
        validate_id(definition.get(key), key)
    purpose = definition.get("first_run_purpose")
    if (definition.get("kind") != KIND or not isinstance(purpose, str) or not purpose.strip()
            or not isinstance(definition.get("source_definition_digest"), str)):
        raise TaskflowError("original pause requires its exact first-run purpose and source definition")
    authority = validate_evidence([definition["authorization"]])[0]
    packet = _read(Path(authority["path"]))
    expected = {"controller": definition["owner"], "task_id": definition["task_id"],
                "policy_id": definition["policy_id"], "source_flow_id": definition["source_flow_id"],
                "source_definition_digest": definition["source_definition_digest"],
                "first_run_purpose": purpose, "action": "pause_after_original_score",
                "round_scope": "this_round_original", "first_run_only": True}
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or any(packet.get(key) != value for key, value in expected.items())
            or type(packet.get("first_run_only")) is not bool):
        raise TaskflowError("original pause is outside the explicit controller round authorization")
    source = store.get_flow(definition["source_flow_id"])
    actual = source["definition"]
    if (actual.get("kind") != "experiment_v1"
            or source["definition_digest"] != definition["source_definition_digest"]
            or sha256(canonical(actual)) != source["definition_digest"]
            or any(actual.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or _packet(source)["business_investment"]["unresolved_business_question"] != purpose):
        raise TaskflowError("original pause source, owner, task, policy or first purpose differs")
    if packet.get("kind") == "derived_original_score_pause_authority_v1":
        if packet.get("source_packet") != actual["authorization"]:
            raise TaskflowError("derived pause authority differs from the original source packet reference")
        raw = _read(Path(validate_evidence([packet["source_packet"]])[0]["path"]))
        if original_pause_grant(raw) is None:
            raise TaskflowError("derived pause authority has no original explicit packet grant")
    validate_evidence(source["evidence"])
    validate_evidence([actual["authorization"]])
    if check_unique:
        for other in store.list_flows():
            prior = other["definition"]
            if (other["flow_id"] != definition["flow_id"] and prior.get("kind") == KIND
                    and prior.get("source_flow_id") == definition["source_flow_id"]):
                raise TaskflowError("this original experiment already has its unique pause successor")
    return source


@_handoff_errors
def validated_original(store: object, source: dict) -> dict | None:
    """Read the first accepted native intake and its actual managed phases only."""
    from taskflow.execution import _check_work_binding
    from taskflow.execution_experiment import _node, _packet
    from taskflow.execution_review import _source_binding, _time
    from taskflow.execution_runner import _info, _read, _regular, _trace, _validate, _validated_collection, _validated_facts_seal
    from taskflow.execution_teacher_successor import _archive
    from taskflow.packages import verify_snapshot
    from taskflow.records import verify_evaluation, verify_run

    definition, state = source["definition"], source["state"]
    rows = state.get("runs", [])
    if not rows:
        return None
    if not isinstance(rows, list) or not isinstance(rows[0], dict):
        raise TaskflowError("original pause source has no typed first original intake")
    original = rows[0]
    packet = _packet(source)
    first_id = validate_id(packet["formal"]["first_run_id"], "first run ID")
    destination = Path(definition["directory"]) / "evaluations" / first_id
    receipt_path = destination / "receipt.json"
    receipt, binding = _read(receipt_path), _read(destination / "binding.json")
    if (receipt.get("accepted") is not True or receipt.get("state") != "accepted"
            or receipt.get("receipt") != str(receipt_path)
            or canonical(receipt) != canonical({key: value for key, value in original.items()
                                               if key not in {"solver_attempt_id", "solver_sid"}})):
        raise TaskflowError("source first row is not its fixed accepted original intake")
    run_path, evaluation_path = Path(receipt["run"]), Path(receipt["evaluation"])
    run, evaluation = verify_run(run_path), verify_evaluation(evaluation_path)
    snapshot = verify_snapshot(Path(binding["config"]["snapshot_directory"]))
    candidate = store.get_candidate(state["candidate_id"])
    if (run.get("run_id") != first_id or evaluation_path != run_path / "evaluations" / packet["independent_review"]["score_id"]
            or evaluation.get("kind") != "original" or evaluation.get("validation_issues")
            or any(row.get("run_digest") != run["run_digest"] for row in (receipt, binding, evaluation))
            or receipt.get("record_digest") != evaluation["record_digest"]
            or receipt.get("evaluation_id") != evaluation["evaluation_id"]
            or binding["config"].get("run_directory") != str(run_path)
            or binding["config"].get("evaluation_id") != packet["independent_review"]["score_id"]
            or run.get("snapshot_digest") != snapshot["snapshot_digest"]
            or any(snapshot.get(key) != candidate[key] for key in
                   ("task_id", "input_digest", "content_digest", "evaluation_digest"))
            or evaluation.get("evaluation_digest") != candidate["evaluation_digest"]
            or validate_evidence(binding["config"]["evidence"]) != validate_evidence([definition["authorization"]])):
        raise TaskflowError("first original run, evaluation or frozen candidate binding differs")
    first = {**source, "state": {**state, "run_index": 0}}
    phases, references = {}, [evidence_ref(receipt_path, "accepted first original intake"),
        evidence_ref(destination / "binding.json", "original intake immutable binding"),
        evidence_ref(evaluation_path / "record.original.json", "original signed evaluation")]
    for role, label in (("solver", "solver"), ("review-facts", "facts"), ("review-score", "score")):
        work_id = _node(first, role)
        attempts = store.list_attempts(work_id)
        if len(attempts) != 1:
            raise TaskflowError("first original requires one exact managed phase attempt")
        attempt, work = attempts[0], store.get_work(work_id)
        root = Path(attempt["runtime_path"])
        expected = {"work_id": work_id, "intent_id": work_id + "-intent", "state": "completed",
                    "task_id": candidate["task_id"], "candidate_id": candidate["candidate_id"],
                    "policy_id": definition["policy_id"], "dispatch_owner": definition["owner"], "role": role}
        manifest = _validate(root)
        collection = _validated_collection(root, manifest)
        _check_work_binding(attempt, candidate, manifest)
        trace_path = root / ("observed-trace.jsonl" if manifest["mode"] == "existing_isolated" else "trace.jsonl")
        if (any(row.get(key) != value for row in (work, attempt) for key, value in expected.items())
                or any(row.get("request_digest") != manifest["request"]["sha256"] for row in (work, attempt))
                or manifest.get("candidate_binding") != {key: candidate[key] for key in
                    ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
                or any(collection.get(key) is not True for key in
                       ("process_ended", "technical_accepted", "delivery_complete", "model_completed"))
                or collection.get("sid") != attempt.get("cli_sid")
                or _trace(trace_path, attempt["cli_sid"]).get("model_completed") is not True):
            raise TaskflowError("first original phase differs from its completed sealed managed attempt")
        if role == "solver":
            if (attempt["attempt_id"] != original.get("solver_attempt_id") or attempt["cli_sid"] != original.get("solver_sid")
                    or root != Path(definition["directory"]) / "runtimes" / work_id
                    or work.get("purpose") != packet["business_investment"]["unresolved_business_question"]):
                raise TaskflowError("first original solver identity or purpose differs")
            trace_info = _info(trace_path)
            if not any(all(row.get(key) == value for key, value in trace_info.items())
                       for row in run.get("artifacts", [])):
                raise TaskflowError("original native run does not contain this first solver trace")
        else:
            archived, archived_manifest, _ = _archive(receipt["review_archive"][label], role)
            part, phase_binding = _source_binding(root, Path(binding["config"][label + "_binding"]))
            if (root != Path(binding["config"][label + "_attempt"])
                    or archived != destination / "full-archive" / label
                    or attempt["cli_sid"] != receipt["signer_sid"]
                    or phase_binding["run_digest"] != run["run_digest"]
                    or part != manifest or archived_manifest != manifest
                    or any(_info(root / name) != _info(archived / name)
                           for name in ("manifest.json", "collection.json", "facts-seal.json" if label == "facts" else "first-score-seal.json"))):
                raise TaskflowError("first facts/score differs from its original sealed intake archive")
        phases[label] = (root, manifest, collection, attempt)
        references.extend(evidence_ref(root / name, "original managed " + label + " evidence")
                          for name in ("manifest.json", "collection.json"))
    facts_root, facts_manifest, _, facts_attempt = phases["facts"]
    score_root, score_manifest, score_collection, _ = phases["score"]
    facts = _validated_facts_seal(facts_root)
    score_seal = _read(score_root / "first-score-seal.json")
    native, raw_score = _read(evaluation_path / "record.original.json"), _read(Path(score_seal["score"]["path"]))
    if (facts_manifest.get("resume_sid") or facts["sid"] != receipt["signer_sid"]
            or facts["sid"] == original["solver_sid"] or score_manifest.get("resume_sid") != facts["sid"]
            or score_manifest.get("facts_seal") != facts or binding.get("facts") != facts
            or score_manifest.get("facts_attempt") != str(facts_root)
            or facts["facts"]["sha256"] != receipt["facts_sha256"]
            or score_seal.get("facts_sha256") != receipt["facts_sha256"]
            or score_seal.get("thread_id") != facts["sid"] or binding.get("score") != score_seal["score"]
            or score_seal["score"]["sha256"] != receipt["source_sha256"]
            or _info(Path(score_seal["score"]["path"])) != score_collection["artifacts"][binding["config"]["score_file"]]
            or _regular(destination / "evaluation.original.json") != _regular(evaluation_path / "record.original.json")
            or native.get("signed_original_score") != raw_score or native.get("total") != receipt["score"]
            or native.get("source_sha256") != receipt["source_sha256"]
            or not (_time(facts["at"]) <= _time(score_manifest["created_at"]) <= _time(score_collection["termination"]["started_at"]))):
        raise TaskflowError("first original same-SID facts/score seal or raw signed score differs")
    return {"schema_version": 1, "kind": "validated_first_original_for_task_pause",
            "source_flow_id": source["flow_id"], "source_definition_digest": source["definition_digest"],
            "source_revision": source["revision"], "source_state_digest": sha256(canonical(state)),
            "first_result_digest": sha256(canonical(original)), "run_id": first_id,
            "candidate_id": candidate["candidate_id"], "signer_sid": receipt["signer_sid"],
            "solver_attempt_id": original["solver_attempt_id"], "solver_sid": original["solver_sid"],
            "source_sha256": receipt["source_sha256"], "facts_sha256": receipt["facts_sha256"],
            "record_digest": receipt["record_digest"], "run_digest": receipt["run_digest"], "evidence": references}


def advance_original_score_pause(store: object, record: dict) -> dict:
    """Zero-model tick: wait for the first intake, then atomically pause once."""
    if record["state"]["status"] == "accepted":
        return record
    source = validate_original_pause_definition(store, record["definition"], check_unique=False)
    original = validated_original(store, source)
    if original is None:
        if record["state"].get("waiting") == "first_original_not_received":
            return record
        state = {**record["state"], "waiting": "first_original_not_received"}
        return store.update_flow(record["flow_id"], record["revision"], state,
                                 {"kind": "original_pause_waiting_first_intake"})
    return store.accept_original_score_pause(record["flow_id"], record["revision"],
        source_revision=source["revision"], source_state_digest=original["source_state_digest"],
        first_result_digest=original["first_result_digest"], original=original)
