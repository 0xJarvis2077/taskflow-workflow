"""Bind an explicit post-original business successor to existing author work.

No score threshold, new model role or scheduler lives here. A mechanical tick
only verifies accepted originals, prepares the normal author with allow=False,
or adopts an existing managed author. The lifecycle consumer owns model calls.
"""

import json
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, sha256, validate_id
from taskflow.execution_author import _factory_reference, _identity, start_author_from_packet
from taskflow.execution_runner import _full_tree, _full_verify, _info, _read, _validated_collection
from taskflow.execution_formal import _handoff_errors
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.execution_review import _time
from taskflow.records import verify_evaluation, verify_run


KIND = "teacher_successor_v1"


def _reference(value: dict, location: str) -> tuple[dict, dict]:
    source, payload = _factory_reference({**value, "location": value.get("location", location)}, location)
    if "size" in source and (type(source["size"]) is not int or source["size"] != _info(Path(source["path"]))["size"]):
        raise TaskflowError("source reference size differs from its actual bytes")
    return source, payload


def _archive(value: dict, role: str) -> tuple[Path, dict, dict]:
    root = Path(value["path"])
    binding = _read(root / "archive-binding.json")
    if any(binding.get(key) != value.get(key) for key in ("intent_id", "collection", "files", "directories")):
        raise TaskflowError("accepted original archive differs from its receipt binding")
    expected = {**binding["files"], "archive-binding.json": _info(root / "archive-binding.json")}
    tree = _full_tree(root)
    actual = {name: {key: row[key] for key in ("sha256", "size")} for name, row in tree["files"].items()}
    directories = sorted(name.rstrip("/") for name in tree["directories"] if name.rstrip("/") != ".")
    if tree["special"] or actual != expected or directories != binding["directories"]:
        raise TaskflowError("accepted original archive membership, file type or bytes changed")
    _full_verify(root / "supplemental", _read(root / "supplemental-intent.json"), True)
    manifest = _read(root / "manifest.json")
    collection = _validated_collection(root, manifest)
    if (manifest.get("role") != role or manifest.get("intent_id") != value["intent_id"]
            or collection.get("technical_accepted") is not True or collection.get("process_ended") is not True
            or _info(root / "collection.json") != value["collection"]):
        raise TaskflowError("accepted original lacks its actual completed sealed review role")
    return root, manifest, collection


def _receipt_rows(value: object) -> list[dict]:
    if isinstance(value, list):
        return [row for item in value for row in _receipt_rows(item)]
    if not isinstance(value, dict):
        return []
    rows = [value] if isinstance(value.get("receipt"), (str, dict)) else []
    return rows + [row for item in value.values() for row in _receipt_rows(item)]


@_handoff_errors
def _predecessor(store: object, item: dict, owner: str, policy_id: str) -> dict:
    receipt_ref, receipt = _reference(item["predecessor_receipt"], "accepted original facts/score intake")
    registered_ref, registered = _reference(item["registered_original"], "controller original registration, no rerating")
    if receipt.get("accepted") is not True or receipt.get("state") != "accepted":
        raise TaskflowError("teacher successor requires the accepted original intake")
    run = verify_run(Path(receipt["run"]))
    evaluation = verify_evaluation(Path(receipt["evaluation"]))
    if (run["run_id"] != item["predecessor_run"] or run["run_digest"] != receipt["run_digest"]
            or evaluation["record_digest"] != receipt["record_digest"]
            or evaluation["run_digest"] != receipt["run_digest"]
            or evaluation["evaluation_id"] != receipt["evaluation_id"]):
        raise TaskflowError("accepted original run/evaluation digest or identity differs")
    if item.get("predecessor_original_score", receipt["score"]) != receipt["score"]:
        raise TaskflowError("business handoff original-score observation differs; no rerating is allowed")
    for key in ("receipt", "raw_score", "sealed_facts", "evaluation_record"):
        validate_evidence([{**registered[key], "location": "registered original " + key}])
    if (any(registered["receipt"].get(key) != receipt_ref[key] for key in ("path", "sha256"))
            or Path(registered["evaluation_record"]["path"]) != Path(receipt["evaluation"]) / "record.original.json"
            or registered.get("original", {}).get("run_id") != run["run_id"]
            or any(registered["original"].get(key) != receipt[key] for key in ("source_sha256", "record_digest"))):
        raise TaskflowError("registered original is not this exact accepted receipt and source")
    facts_root, facts_manifest, facts = _archive(receipt["review_archive"]["facts"], "review-facts")
    score_root, score_manifest, score = _archive(receipt["review_archive"]["score"], "review-score")
    facts_ref, score_ref = registered["sealed_facts"], registered["raw_score"]
    for reference, root, collection in ((facts_ref, facts_root, facts), (score_ref, score_root, score)):
        path = Path(reference["path"])
        if (path.parent != root / "artifacts" or path.name not in collection["artifacts"]
                or _info(path) != collection["artifacts"][path.name]):
            raise TaskflowError("registered original is not an actual declared sealed review artifact")
    facts_seal = _read(facts_root / "facts-seal.json")
    score_seal = _read(score_root / "first-score-seal.json")
    sid = receipt["signer_sid"]
    phase = registered.get("same_sid_facts_then_score", {})
    if (registered.get("original", {}).get("registered_score", receipt["score"]) != receipt["score"]
            or registered.get("original", {}).get("signer_session", sid) != sid or phase.get("sid", sid) != sid
            or facts_manifest.get("policy_version") != policy_id or score_manifest.get("policy_version") != policy_id):
        raise TaskflowError("registered original identity, score observation or policy differs from actual review")
    for key, role, root in (("facts_attempt", "review-facts", facts_root), ("score_attempt", "review-score", score_root)):
        if key not in phase:
            continue
        if not isinstance(phase[key], str) or not phase[key]:
            raise TaskflowError("registered original has no exact managed review attempt identity")
        attempt = store.get_attempt(phase[key])
        manifest = facts_manifest if role == "review-facts" else score_manifest
        version = manifest["candidate_binding"]
        if (attempt.get("state") != "completed" or attempt.get("role") != role or attempt.get("cli_sid") != sid
                or attempt.get("intent_id") != manifest["intent_id"] or attempt.get("policy_id") != manifest["policy_version"]
                or attempt.get("candidate_id") != version["candidate_id"] or attempt.get("task_id") != version["task_id"]
                or _info(Path(attempt["runtime_path"]) / "manifest.json") != _info(root / "manifest.json")
                or _info(Path(attempt["runtime_path"]) / "collection.json") != _info(root / "collection.json")):
            raise TaskflowError("registered original managed attempt differs from its completed sealed source")
    if (not sid or facts.get("sid") != sid or score.get("sid") != sid
            or facts_seal.get("sid") != sid or score_seal.get("thread_id") != sid
            or score_manifest.get("resume_sid") != sid
            or any(facts_manifest.get(key) != score_manifest.get(key) for key in
                   ("model", "reasoning_effort", "reasoning_context", "reasoning_selection", "candidate_binding"))
            or facts_ref["sha256"] != receipt["facts_sha256"] or facts_seal.get("facts", {}).get("sha256") != facts_ref["sha256"]
            or score_ref["sha256"] != receipt["source_sha256"] or score_seal.get("score", {}).get("sha256") != score_ref["sha256"]
            or score_seal.get("facts_sha256") != facts_ref["sha256"]
            or any(facts_seal.get(key) != _info(facts_root / (key + ".json")) for key in ("manifest", "collection"))
            or any(score_seal.get(key) != _info(score_root / (key + ".json")) for key in ("manifest", "collection"))):
        raise TaskflowError("original facts/score are not the same sealed reviewer continuation")
    if not (_time(facts_seal["at"]) <= _time(score_manifest["created_at"])
            <= _time(score["termination"]["started_at"])):
        raise TaskflowError("original facts seal did not precede scoring preparation and launch")
    signature = {key: receipt[key] for key in
                 ("run", "evaluation_id", "source_sha256", "facts_sha256", "signer_sid", "record_digest", "run_digest")}
    matches = []
    for flow in store.list_flows():
        definition, state = flow["definition"], flow["state"]
        if definition.get("kind") not in {"experiment_v1", "evaluation_tail_v1"} or state.get("status") != "accepted" or not state.get("receipt"):
            continue
        for row in _receipt_rows([state.get("runs"), state.get("result")]):
            path = row["receipt"].get("path") if isinstance(row["receipt"], dict) else row["receipt"]
            if path == receipt_ref["path"] and row.get("accepted") is True and all(row.get(key) == value for key, value in signature.items()):
                matches.append(flow)
                break
    if len(matches) != 1:
        raise TaskflowError("accepted original must belong to one exact received predecessor lifecycle")
    flow = matches[0]
    if (flow["definition"].get("owner") != owner or flow["definition"].get("policy_id") != policy_id
            or flow["definition"].get("task_id") != run["task_id"]
            or registered.get("flow_id", flow["flow_id"]) != flow["flow_id"]):
        raise TaskflowError("teacher successor differs from its accepted original owner/task/policy")
    terminal = evidence_ref(Path(flow["state"]["receipt"]), "accepted predecessor lifecycle terminal receipt")
    return {"flow_id": flow["flow_id"], "definition_digest": flow["definition_digest"], "task_id": run["task_id"],
            "receipt": receipt_ref, "registration": registered_ref, "terminal_receipt": terminal,
            **signature, "nature": receipt["nature"], "facts": facts_ref, "score": score_ref,
            "facts_manifest": evidence_ref(facts_root / "manifest.json", "original factual role manifest"),
            "score_manifest": evidence_ref(score_root / "manifest.json", "same-context original scoring manifest")}


def _original_factory_packet(directory: Path, intent: str, author_ref: dict,
                             author_item: dict, factory: dict) -> dict:
    """Verify the original factory intent even before its work is queued."""
    if (not directory.is_absolute() or ".." in directory.parts
            or any(path.is_symlink() for path in (directory, *directory.parents))
            or not directory.is_relative_to(Path(factory["research_root"]).absolute())):
        raise TaskflowError("existing author preparation is outside its original factory directory")
    identity = _read(directory / "factory-intent.json")
    existing_ref, _ = _reference(identity["packet"], "original author packet identity")
    template_ref, _ = _reference(identity["template"], "original author infrastructure identity")
    expected_template, _ = _reference(factory["template_spec_ref"], "approved original author template")
    if (type(identity.get("schema_version")) is not int
            or any(existing_ref.get(key) != author_ref[key] for key in ("path", "sha256"))
            or any(template_ref.get(key) != expected_template[key] for key in ("path", "sha256"))):
        raise TaskflowError("existing author preparation has a foreign packet or template identity")
    expected = {"schema_version": 1, "packet": identity["packet"], "template": identity["template"],
                "factory_config_digest": sha256(canonical(factory)), "directory": str(directory),
                "task_id": author_item["task_id"], "intent_id": intent, "candidate_id": author_item["candidate_id"]}
    if canonical(identity) != canonical(expected):
        raise TaskflowError("existing author preparation differs from its exact original factory identity")
    if (directory / "spec.json").exists():
        spec = _read(directory / "spec.json")
        work, runner = spec["work"], spec["runner"]
        if (any(work.get(key) != expected[key] for key in ("task_id", "intent_id", "candidate_id"))
                or work.get("work_id") != author_item["task_id"] + "-" + intent
                or work.get("role") != "author" or work.get("policy_id") != factory["policy_id"]
                or runner.get("intent_id") != intent or runner.get("policy_version") != factory["policy_id"]
                or runner.get("role") != "author" or runner.get("cwd") != str(directory / "work")):
            raise TaskflowError("existing author preparation specification differs from its original factory identity")
    # Keep the actual original ref, including its optional location; re-labelling
    # would change the factory's immutable canonical identity.
    return validate_evidence([identity["packet"]])[0]


def _item(store: object, packet_ref: dict, packet: dict, item: dict, factory_ref: dict, factory: dict) -> dict:
    author_ref, author_packet = _reference(item["author_packet"], "once-only substantive author direction and normal successors")
    if (type(author_packet.get("schema_version")) is not int or author_packet["schema_version"] != 1
            or author_packet.get("controller") != packet["owner"] or not isinstance(author_packet.get("items"), list)
            or len(author_packet["items"]) != 1 or author_packet["items"][0].get("intent_id") != item["author_intent"]):
        raise TaskflowError("teacher successor needs its exact once-author packet and owner")
    # New work uses exactly the established CLI ref shape/location; existing
    # work below adopts its original canonical ref without re-labelling it.
    author_ref = evidence_ref(Path(author_ref["path"]), "once-only substantive author direction and normal successors")
    from taskflow.execution_batch import validate_batch_policy

    snapshot = validate_batch_policy(store.get_policy(factory["policy_id"])["config"])
    declared_batch = author_packet.get("batch_id", packet.get("batch_id"))
    if ((snapshot is None and declared_batch is not None)
            or (snapshot is not None and declared_batch != snapshot["batch_id"])):
        raise TaskflowError("a new batch teacher successor requires its explicit confirmed batch factory")
    author_item = author_packet["items"][0]
    intent = validate_id(item["author_intent"], "author intent")
    predecessor = _predecessor(store, item, packet["owner"], factory["policy_id"])
    if author_item["task_id"] != predecessor["task_id"]:
        raise TaskflowError("teacher successor author differs from its original task")
    work_id = validate_id(author_item["task_id"] + "-" + intent, "author work_id")
    with store._db() as db:
        prior = [json.loads(row["data"]) for row in db.execute("SELECT data FROM work WHERE intent_id=?", (intent,))]
    if len(prior) > 1 or (prior and prior[0]["work_id"] != work_id):
        raise TaskflowError("author intent has a foreign or ambiguous managed work identity")
    directory = Path(factory["research_root"]).absolute() / intent
    if prior:
        work = prior[0]
        directory = Path(work["runtime_path"]).parent
        if (work.get("role") != "author" or work.get("dispatch_owner") != packet["owner"]
                or work.get("policy_id") != factory["policy_id"] or work.get("candidate_id") != author_item["candidate_id"]):
            raise TaskflowError("existing author is not this exact original factory/packet/owner")
    intent_exists = (directory / "factory-intent.json").exists()
    if prior or intent_exists:
        author_ref = _original_factory_packet(directory, intent, author_ref, author_item, factory)
        if (directory / "spec.json").exists() and _read(directory / "spec.json")["work"].get("dispatch_owner") != packet["owner"]:
            raise TaskflowError("existing author preparation has a foreign dispatch owner")
    elif directory.exists() and any(directory.iterdir()):
        raise TaskflowError("author preparation directory contains unbound prior material")
    existing = [row for row in store.list_flows() if row["definition"].get("kind") == KIND
                and row["definition"].get("author_intent") == intent]
    preexisting = bool(prior) or intent_exists
    if existing:
        if len(existing) != 1 or any(existing[0]["definition"]["authorization"].get(key) != packet_ref[key] for key in ("path", "sha256")):
            raise TaskflowError("author intent already has another immutable teacher successor authorization")
        preexisting = existing[0]["definition"]["preexisting_author"]
    return {"author_intent": intent, "author_packet": author_ref, "factory": factory_ref,
            "author_directory": str(directory), "author_work_id": work_id,
            "preexisting_author": preexisting, "predecessor": predecessor, "item": item}


@_handoff_errors
def validate_teacher_successor_definition(store: object, definition: dict) -> None:
    """Keep direct lifecycle registration behind the same exact bundle binding."""
    source, packet = _reference(definition["authorization"], "explicit bounded post-original teacher business successors")
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or packet.get("kind") != "explicit_bounded_post_original_teacher_business_successors"
            or packet.get("owner") != definition["owner"] or not packet.get("batch_scope")):
        raise TaskflowError("teacher lifecycle is outside its explicit original business bundle")
    rows = [row for row in packet.get("items", []) if row.get("author_intent") == definition["author_intent"]]
    if len(rows) != 1 or rows[0] != definition["item"]:
        raise TaskflowError("teacher lifecycle item differs from its unique original authorization")
    factory_ref, factory = _reference(packet["author_factory"], "existing explicitly approved author factory")
    item = _item(store, source, packet, rows[0], factory_ref, factory)
    expected = {"schema_version": 1, "kind": KIND, "flow_id": _identity("teacher-successor", item["author_intent"]),
        "task_id": item["predecessor"]["task_id"], "owner": packet["owner"], "policy_id": factory["policy_id"],
        "directory": definition["directory"], "authorization": source, **item}
    if canonical(expected) != canonical(definition):
        raise TaskflowError("teacher lifecycle definition differs from the actual original author/predecessor")
    directory = Path(definition["directory"])
    source_root = Path(item["author_directory"])
    if (not directory.is_absolute() or any(path.is_symlink() for path in (directory, *directory.parents))
            or directory.is_relative_to(source_root) or source_root.is_relative_to(directory)):
        raise TaskflowError("teacher link receipts must be separate ordinary lifecycle storage")


@_handoff_errors
def start_teacher_successors_from_packet(store: object, *, packet_ref: dict, directory: Path,
                                        allow_model: bool = False) -> dict:
    """Register/prepare only this explicit bundle; ordinary consumers own calls."""
    from taskflow.execution_experiment_factory import launch_lifecycle_consumer
    from taskflow.execution_lifecycle import _lifecycle_lock, advance_lifecycle, register_lifecycle

    if type(allow_model) is not bool:
        raise TaskflowError("teacher successor model permission must be explicit")
    packet_source, packet = _reference(packet_ref, "explicit bounded post-original teacher business successors")
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or packet.get("kind") != "explicit_bounded_post_original_teacher_business_successors"
            or not isinstance(packet.get("items"), list) or not packet["items"] or not packet.get("batch_scope")):
        raise TaskflowError("an explicit bounded post-original business successor bundle is required")
    owner = validate_id(packet["owner"], "owner")
    factory_ref, factory = _reference(packet["author_factory"], "existing explicitly approved author factory")
    if any(not isinstance(item, dict) for item in packet["items"]):
        raise TaskflowError("teacher bundle items must be explicit author objects")
    intents = [item.get("author_intent") for item in packet["items"]]
    if any(not isinstance(item, str) or not item for item in intents) or len(intents) != len(set(intents)):
        raise TaskflowError("teacher bundle repeats an author intent")
    # Preflight every item before any new author preparation is made.
    items = [_item(store, packet_source, packet, item, factory_ref, factory) for item in packet["items"]]
    directory = Path(directory).absolute()
    results = []
    for item in items:
        identity = _identity("teacher-successor", item["author_intent"])
        definition = {"schema_version": 1, "kind": KIND, "flow_id": identity,
            "task_id": item["predecessor"]["task_id"], "owner": owner, "policy_id": factory["policy_id"],
            "directory": str(directory / identity), "authorization": packet_source, **item}
        for existing in store.list_flows():
            if (existing["definition"].get("kind") == KIND
                    and existing["definition"].get("author_intent") == item["author_intent"]
                    and (existing["flow_id"] != identity or canonical(existing["definition"]) != canonical(definition))):
                raise TaskflowError("author intent already belongs to another immutable teacher successor authorization")
        refs = [packet_source, factory_ref, item["author_packet"], item["predecessor"]["receipt"], item["predecessor"]["registration"]]
        record = register_lifecycle(store, definition, refs)
        owned = False
        try:
            with _lifecycle_lock(store, record["flow_id"]):
                record = advance_lifecycle(store, record["flow_id"])
                if record["state"]["status"] in {"accepted", "exception"}:
                    record = advance_lifecycle(store, record["flow_id"])
        except TaskflowError as exc:
            if str(exc) != "this lifecycle already has a consumer":
                raise
            record, owned = store.get_flow(record["flow_id"]), True
        launched = (launch_lifecycle_consumer(store, record, Path(__file__).resolve().parents[1], evidence=refs)
                    if allow_model and not owned and not item["preexisting_author"] and record["state"]["status"] == "active" else None)
        results.append({"flow_id": record["flow_id"], "record": record, "consumer": launched,
                        "execution": "consumer_lock_held" if owned else "original_successor_bound",
                        "models_dispatched_by_preparation": 0})
    return {"schema_version": 1, "items": results, "models_dispatched_by_preparation": 0,
            "score_threshold_policy": False, "new_formal_experiment_authorized": False}


@_handoff_errors
def advance_teacher_successor(store: object, record: dict) -> dict:
    """One zero-model tick; never invoke a worker or a model-enabled factory."""
    definition, state = record["definition"], dict(record["state"])
    if state.get("pending_business_scope") is not None:
        raise TaskflowError("teacher successor cannot clear a pending business scope decision")
    if definition.get("kind") != KIND:
        raise TaskflowError("teacher_successor_v1 lifecycle required")
    _, packet = _reference(definition["authorization"], "original teacher successor bundle")
    factory_ref, factory = _reference(definition["factory"], "same original author factory")
    if _predecessor(store, definition["item"], definition["owner"], definition["policy_id"]) != definition["predecessor"]:
        raise TaskflowError("original predecessor acceptance changed after teacher successor binding")
    directory = Path(definition["author_directory"])
    if not state.get("prepared_author"):
        if definition["preexisting_author"]:
            # An original factory may have published its immutable intent but
            # not yet queued a work. Neither that active window nor an earlier
            # preparation failure authorizes this link to prepare or retry it.
            with store._db() as db:
                row = db.execute("SELECT data FROM work WHERE id=?", (definition["author_work_id"],)).fetchone()
            if row is None:
                _, author_packet = _reference(definition["author_packet"], "original pending author packet")
                _original_factory_packet(directory, definition["author_intent"], definition["author_packet"],
                                         author_packet["items"][0], factory)
                state.update(stage="teacher_author", current_work_id=None,
                    waiting={"reason": "existing_author_preparation_not_received", "author_intent": definition["author_intent"],
                             "factory_intent": evidence_ref(directory / "factory-intent.json", "original owner preparation; never retried here")})
                return store.update_flow(record["flow_id"], record["revision"], state,
                    {"kind": "teacher_existing_author_preparation_waiting_without_model"}) if state != record["state"] else record
            # Pure registration revalidates the original queued spec/request and
            # actual attempt without entering preparation or future-owner rules.
            work = json.loads(row["data"])
            prepared = {"work_id": work["work_id"], "candidate_id": work["candidate_id"],
                "runtime": work["runtime_path"], "spec": str(directory / "spec.json"),
                "factory_intent": str(directory / "factory-intent.json"), "state": work["state"],
                "model_calls_by_factory_preparation": 0, "existing_managed_adoption": True}
        else:
            if (directory / "factory-intent.json").exists():
                raise TaskflowError("original author preparation already owns an intent before this tick; original owner must reconcile")
            prepared = start_author_from_packet(store, packet_ref=definition["author_packet"],
                template_spec_ref=factory["template_spec_ref"], directory=directory, allow_model=False, factory_config=factory)
        if prepared["work_id"] != definition["author_work_id"]:
            raise TaskflowError("normal author factory returned a different managed intent")
        state.update(prepared_author=prepared, stage="teacher_author", current_work_id=prepared["work_id"], waiting=None)
    work = store.get_work(definition["author_work_id"])
    attempts = store.list_attempts(work["work_id"])
    if len(attempts) > 1:
        raise TaskflowError("teacher successor cannot consume more than one original author attempt")
    if not attempts:
        if definition["preexisting_author"] or work["state"] != "ready":
            raise TaskflowError("preexisting or non-ready author has no observed managed attempt; original owner must reconcile")
        return store.update_flow(record["flow_id"], record["revision"], state, {"kind": "teacher_author_prepared_without_model"}) if state != record["state"] else record
    attempt = attempts[0]
    if attempt["state"] not in {"claimed", "starting", "running", "collecting", "completed"}:
        raise TaskflowError("original author failed/uncertain/cancelled; no automatic teacher retry")
    from taskflow.execution_author import register_prepared_author_lifecycle

    linked = register_prepared_author_lifecycle(store, directory=directory, factory_config=factory, attempt_id=attempt["attempt_id"])
    if definition["preexisting_author"]:
        state.update(status="accepted", stage="teacher_adoption", current_work_id=None, waiting=None,
            result={"outcome": "existing_managed_author_adopted", "author_flow_id": linked["flow_id"],
                "attempt_id": attempt["attempt_id"], "sid": attempt.get("cli_sid"), "actual_author_state": attempt["state"],
                "new_model_dispatched": False, "business_outcome_inferred": False, "new_formal_experiment": False})
    else:
        state.update(stage="teacher_author_quality", author_lifecycle_id=linked["flow_id"], current_work_id=None, waiting=None)
        if linked["state"]["status"] in {"accepted", "exception"} and linked["state"].get("receipt"):
            state.update(status=linked["state"]["status"], result={"outcome": "normal_author_successor_received",
                "author_flow_id": linked["flow_id"], "author_receipt": evidence_ref(Path(linked["state"]["receipt"]), "actual normal author successor outcome"),
                "author_result": linked["state"].get("result"), "business_outcome_inferred": False, "new_formal_experiment": False})
            if linked["state"]["status"] == "exception":
                state["exception"] = {"stage": "teacher_author_quality", "reason": "original normal author successor requires its owner", "source_exception": linked["state"].get("exception")}
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": "teacher_original_author_bound_without_model"}) if state != record["state"] else record
