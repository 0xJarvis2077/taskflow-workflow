"""Fresh quality of a captured baseline after an honest research-ended outcome.

The original author lifecycle remains terminal and unchanged. A separate,
once-only batch-bound purpose follows the normal public/quality cursor.
"""

import copy
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, read_json, sha256
from taskflow.execution_flow import _binding
from taskflow.execution_store import evidence_ref, validate_evidence


KIND = "baseline_quality_v1"
PACKET = "explicit_research_ended_baseline_quality_v1"


def _require(value, message):
    if not value:
        raise TaskflowError("baseline quality: " + message)


def baseline_flow_id(source_id: str) -> str:
    # Keep the complete identity inside _identity's 48-character prefix, so
    # long original IDs cannot collide again in public/quality child names.
    return "baselineq-" + sha256(source_id.encode())[:32]


def _same_ref(actual, expected):
    validate_evidence([actual, expected])
    return all(actual.get(key) == expected.get(key) for key in ("path", "sha256"))


def closed_research_source(store: object, identity: str) -> dict:
    """Read actual sealed provenance; never recover or advance the author."""
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_batch import validate_batch_policy
    from taskflow.execution_batch_binding import validate_batch_source
    from taskflow.execution_runner import _info, _regular, _validate
    from taskflow.packages import check_task

    source = store.get_flow(identity)
    definition, state = source["definition"], source["state"]
    _require(definition.get("kind") == "author_quality_v1"
             and source["definition_digest"] == sha256(canonical(definition))
             and state.get("status") == "accepted" and state.get("receipt")
             and state.get("stage") == "author_handoff" and state.get("current_work_id") is None
             and not state.get("pending_business_scope") and not state.get("child_flow_id")
             and not state.get("candidate_id"), "one received research-ended author required")
    validate_evidence(source["evidence"])
    validate_evidence([definition["authorization"]])
    author, _ = _author(store, definition)
    _require(author["state"] == "completed" and author.get("cli_sid")
             and author["policy_id"] == definition["policy_id"]
             and author.get("dispatch_owner") == definition["owner"], "original completed author identity differs")
    manifest = _validate(Path(author["runtime_path"]))
    _require(validate_batch_source(author, manifest, store) is True, "original author lacks its confirmed batch")
    policy = store.get_policy(definition["policy_id"])
    batch = validate_batch_policy(policy["config"])
    _require(batch is not None and definition["task_id"] in batch["task_ids"], "confirmed task scope required")
    simulation = batch["execution_mode"] == "test_fixture"
    _require((manifest["mode"] == "test_fixture") if simulation else
             (manifest["mode"] == "native_role" and manifest.get("nature") == "real_cli_attempt"),
             "author execution provenance differs from batch mode")
    handoff_path = Path(state["author_archive"]) / "artifacts/handoff.json"
    handoff = _handoff(Path(state["author_archive"]), definition, author)
    result = state.get("result", {})
    _require(handoff["disposition"] == "research_ended"
             and result.get("outcome") == "research_ended" and result.get("reason") == handoff["reason"]
             and result.get("source_evidence") == handoff["source_evidence"]
             and result.get("candidate_created") is False and result.get("new_model_dispatched") is False
             and result.get("formal_experiment") == "not_dispatched", "research outcome cannot be relabeled")
    receipt = read_json(Path(state["receipt"]))
    _require(receipt.get("flow_id") == identity and receipt.get("status") == "accepted"
             and receipt.get("controller") == definition["owner"] and receipt.get("result") == result
             and receipt.get("history") == state.get("history") and receipt.get("archives") == state.get("archives"),
             "original terminal receipt differs")
    candidate = store.get_candidate(author["candidate_id"])
    declarations = [r for r in candidate["evidence"] if r.get("location") == "explicit package declaration"]
    _require(len(declarations) == 1, "captured baseline declaration required")
    validate_evidence(candidate["evidence"])
    root = Path(declarations[0]["path"]).parent
    actual = check_task(root, write_report=False)
    _require(not actual["errors"] and all(actual[key] == candidate[key] for key in
             ("task_id", "content_digest", "input_digest", "evaluation_digest")), "captured baseline version changed")
    for row in candidate["files"]:
        _require(Path(row["path"]) == root / row["package_path"]
                 and _info(Path(row["path"]))["sha256"] == row["sha256"],
                 "captured baseline member changed")
    evidence = [evidence_ref(Path(state["receipt"]), "actual research-ended terminal receipt"),
        evidence_ref(handoff_path, "original research-ended handoff"),
        *[evidence_ref(Path(author["runtime_path"]) / name, "original author sealed provenance")
          for name in ("manifest.json", "manifest-lock.json", "collection.json", "collection-lock.json", "termination.json")],
        definition["authorization"], declarations[0]]
    # Read the source declaration as bytes as well as validating its evidence.
    _regular(Path(declarations[0]["path"]))
    proof = {"schema_version": 1, "kind": "research_ended_captured_baseline_source_v1",
        "source_flow_id": identity, "source_definition_digest": source["definition_digest"],
        "source_revision": source["revision"], "source_event_seq": source["event_seq"],
        "source_state_digest": sha256(canonical(state)),
        "source_author_attempt": author["attempt_id"], "source_author_sid": author["cli_sid"],
        "baseline_binding": _binding(candidate), "batch_id": batch["batch_id"],
        "policy_digest": policy["digest"], "simulation_only": simulation, "evidence": evidence}
    return {"source": source, "author": author, "candidate": candidate, "root": root,
            "policy": policy, "batch": batch, "proof": proof}


def _factory(store, reference, packet):
    from taskflow.execution_batch import validate_batch_policy

    ref = validate_evidence([reference])[0]
    factory = read_json(Path(ref["path"]))
    _require(type(factory.get("schema_version")) is int and factory["schema_version"] == 1,
             "versioned original factory required")
    policy = store.get_policy(factory["policy_id"])
    batch = validate_batch_policy(policy["config"])
    registration = validate_evidence([factory["policy_record_ref"]])[0]
    registered = read_json(Path(registration["path"]))
    _require(batch is not None and factory.get("batch_snapshot") == policy["config"]["batch_snapshot"]
             and registered == {"operation": "register_policy", "arguments": {
                 "policy_id": policy["policy_id"], "config": policy["config"]}}
             and packet.get("batch_id") == batch["batch_id"], "factory differs from original confirmed policy")
    return ref, factory


def validate_baseline_definition(store: object, definition: dict) -> dict:
    """Validate a separately authorized baseline purpose, including direct API use."""
    _require(definition.get("kind") == KIND, "typed baseline lifecycle required")
    packet_ref = validate_evidence([definition["authorization"]])[0]
    packet = read_json(Path(packet_ref["path"]))
    _require(type(packet.get("schema_version")) is int and packet["schema_version"] == 1
             and packet.get("kind") == PACKET and packet.get("controller") == definition["owner"]
             and packet.get("no_new_author") is True and packet.get("scope") == "fresh_public_then_whole_quality"
             and packet.get("formal_experiment") == "separate_after_quality"
             and isinstance(packet.get("items"), list) and len(packet["items"]) == 1,
             "explicit once-only public/whole-quality business purpose required")
    item = packet["items"][0]
    material = closed_research_source(store, definition["source_research_flow_id"])
    source, author, proof = material["source"], material["author"], material["proof"]
    _, factory = _factory(store, definition["factory_config_ref"], packet)
    _require(definition["flow_id"] == baseline_flow_id(source["flow_id"])
             and all(definition[key] == source["definition"][key] for key in ("task_id", "owner", "policy_id"))
             and factory["policy_id"] == definition["policy_id"]
             and definition["baseline_source"] == proof and definition["baseline_binding"] == proof["baseline_binding"]
             and definition["candidate_root"] == str(material["root"])
             and definition["simulation_only"] is proof["simulation_only"], "immutable baseline scope/source differs")
    _require(item.get("task_id") == definition["task_id"]
             and item.get("source_author_quality_flow") == source["flow_id"]
             and item.get("source_definition_digest") == source["definition_digest"]
             and item.get("source_author_attempt") == author["attempt_id"]
             and item.get("source_author_sid") == author["cli_sid"]
             and item.get("baseline_binding") == proof["baseline_binding"]
             and isinstance(item.get("purpose"), str) and bool(item["purpose"].strip())
             and _same_ref(item["source_terminal_receipt"], proof["evidence"][0]), "business item differs from actual baseline research")
    _require(definition["business_item"] == item
             and definition["next"] == {"candidate_complete": item["purpose"]}
             and definition["role_template_ref"] == source["definition"]["role_template_ref"]
             and _same_ref(factory["role_template_ref"], definition["role_template_ref"])
             and definition["source_author_attempt"] == author["attempt_id"]
             and definition["source_author_binding"] == source["definition"]["source_author_binding"],
             "quality infrastructure or original researcher differs")
    directory = Path(factory["lifecycle_root"]) / definition["flow_id"]
    original_roots = (material["root"].resolve(), Path(source["definition"]["directory"]).resolve())
    resolved = directory.resolve()
    _require(directory.is_absolute() and definition["directory"] == str(directory)
             and all(not resolved.is_relative_to(root) and not root.is_relative_to(resolved)
                     for root in original_roots), "new quality evidence must preserve original roots")
    from taskflow.execution_author import _roles

    _roles(store, definition)  # Require every used logical slot before registration.
    return material


def start_baseline_quality_from_packet(store: object, *, packet_ref: dict, factory_config_ref: dict,
                                       allow_model: bool = False) -> dict:
    """Register only unless explicitly consumed; never redraw an author or solver."""
    from taskflow.execution_lifecycle import register_lifecycle, run_lifecycle

    _require(type(allow_model) is bool, "explicit model-consumer flag required")
    packet_ref, factory_ref = validate_evidence([packet_ref, factory_config_ref])
    packet = read_json(Path(packet_ref["path"]))
    _require(isinstance(packet.get("items"), list) and len(packet["items"]) == 1, "one exact baseline item required")
    item = packet["items"][0]
    material = closed_research_source(store, item["source_author_quality_flow"])
    source = material["source"]
    _, factory = _factory(store, factory_ref, packet)
    identity = baseline_flow_id(source["flow_id"])
    definition = {"schema_version": 1, "kind": KIND, "flow_id": identity,
        **{key: source["definition"][key] for key in ("task_id", "owner", "policy_id")},
        "directory": str(Path(factory["lifecycle_root"]) / identity), "authorization": packet_ref,
        "factory_config_ref": factory_ref, "source_research_flow_id": source["flow_id"],
        "baseline_source": material["proof"], "baseline_binding": _binding(material["candidate"]),
        "candidate_root": str(material["root"]), "simulation_only": material["proof"]["simulation_only"],
        "role_template_ref": copy.deepcopy(source["definition"]["role_template_ref"]),
        "source_author_attempt": material["author"]["attempt_id"],
        "source_author_binding": copy.deepcopy(source["definition"]["source_author_binding"]),
        "business_item": item, "next": {"candidate_complete": item.get("purpose")}}
    record = register_lifecycle(store, definition, [packet_ref, factory_ref, *material["proof"]["evidence"]])
    return run_lifecycle(store, identity, allow_model=True) if allow_model else record


def register_baseline_quality(store: object, definition: dict, refs: list[dict]) -> dict:
    from taskflow.execution_controller_finding import _TransactionStore

    material = validate_baseline_definition(store, definition)
    state = {
        "status": "active", "stage": "public_prepare", "current_work_id": None,
        "candidate_id": material["candidate"]["candidate_id"], "candidate_root": str(material["root"]),
        "history": [{"kind": "captured_baseline_quality_authorized", "original_author_outcome": "research_ended",
                     "source_flow_id": material["source"]["flow_id"]}],
        "archives": [], "waiting": None, "simulation_only": definition["simulation_only"]}
    with store._db(True) as db:
        tx = _TransactionStore(store, db)
        _require(tx.get_flow(definition["source_research_flow_id"]) == material["source"],
                 "original source changed during registration")
        existing = db.execute("SELECT 1 FROM execution_flows WHERE flow_id=?", (definition["flow_id"],)).fetchone()
        if existing is not None:
            record = tx.get_flow(definition["flow_id"])
            _require(record["definition_digest"] == sha256(canonical(definition)) and record["evidence"] == refs,
                     "once-only baseline purpose cannot be replaced")
            return record
        return tx.register_flow(definition["flow_id"], definition, refs, state)


def advance_baseline_quality(store: object, record: dict) -> dict:
    from taskflow.execution_author import advance_candidate_quality

    material = validate_baseline_definition(store, record["definition"])
    return advance_candidate_quality(store, record, material["author"])


def baseline_experiment_source(store: object, record: dict, packet: dict, *, require_candidate: bool = False) -> dict:
    """An honest alternate source type for the existing batch experiment factory."""
    from taskflow.execution_experiment import _wait
    from taskflow.execution_batch_binding import validate_batch_source
    from taskflow.execution_runner import _validate

    parent = store.get_flow(record["definition"]["source_author_flow_id"])
    material = validate_baseline_definition(store, parent["definition"])
    definition, state = parent["definition"], parent["state"]
    author, initial = material["author"], material["candidate"]
    _require(packet.get("source_author_flow_kind") == KIND
             and packet.get("source_baseline_binding") == _binding(initial)
             and all(definition[key] == record["definition"][key] for key in ("task_id", "owner", "policy_id"))
             and packet.get("source_author_attempt") == author["attempt_id"]
             and packet.get("source_author_intent") == author["intent_id"]
             and packet.get("source_author_sid") == author["cli_sid"]
             and definition["authorization"]["path"] == packet.get("source_business_packet")
             and definition["authorization"]["sha256"] == packet.get("source_business_packet_sha256"),
             "experiment authority must name this exact baseline quality purpose/researcher")
    _require(state.get("status") != "exception" and not state.get("pending_business_scope"), "baseline quality is unresolved")
    if state.get("status") != "accepted" or not state.get("receipt"):
        _require(not require_candidate, "baseline quality not yet accepted")
        return {"record": _wait(store, record, "source_baseline_quality_not_received", source_flow_id=parent["flow_id"])}
    result = state.get("result", {})
    body = read_json(Path(state["receipt"]))
    _require(result.get("outcome") == "candidate_quality_accepted"
             and result.get("basis_kind") == "existing_captured_baseline"
             and result.get("candidate_created_by_source_author") is False
             and result.get("research_ended_source_flow_id") == material["source"]["flow_id"]
             and body.get("flow_id") == parent["flow_id"] and body.get("controller") == definition["owner"]
             and body.get("status") == "accepted" and body.get("result") == result
             and body.get("history") == state.get("history"), "baseline terminal result is not its actual independent quality")
    child = store.get_flow(state["child_flow_id"])
    public = store.get_attempt(state["public_attempt_id"])
    child_initial = child["definition"]["initial"]
    child_receipt = read_json(Path(child["state"]["receipt"]))
    public_manifest = _validate(Path(public["runtime_path"]))
    _require(result.get("child_flow_id") == child["flow_id"] and child["state"].get("status") == "accepted"
             and all(result.get(k) == child["state"].get(k) for k in
                     ("candidate_id", "candidate_root", "accepted_review_id"))
             and result.get("child_receipt") == child["state"].get("receipt")
             and all(child["definition"][k] == definition[k] for k in ("task_id", "owner", "policy_id"))
             and child_receipt.get("flow_id") == child["flow_id"] and child_receipt.get("status") == "accepted"
             and child_receipt.get("candidate_id") == result["candidate_id"]
             and child_receipt.get("controller") == definition["owner"]
             and child_receipt.get("history") == child["state"].get("history")
             and child_initial["candidate_id"] == initial["candidate_id"]
             and child_initial["expected_digests"] == {k: initial[k] for k in ("content_digest", "input_digest", "evaluation_digest")}
             and child_initial.get("excluded_review_sids") == [author["cli_sid"]]
             and public.get("state") == "completed" and public.get("role") == "quality"
             and public.get("candidate_id") == initial["candidate_id"] and public.get("cli_sid")
             and public.get("model_slot") == "public_rebuild" and public.get("policy_id") == definition["policy_id"]
             and public.get("task_id") == definition["task_id"] and public.get("dispatch_owner") == definition["owner"]
             and public.get("claimed_seq", 0) > material["source"]["event_seq"]
             and validate_batch_source(public, public_manifest, store) is True
             and public["cli_sid"] != author["cli_sid"]
             and child_initial.get("predecessor_attempt") == public["attempt_id"]
             and child_initial.get("predecessor_binding") == {k: public[k] for k in ("work_id", "intent_id", "candidate_id", "task_id")},
             "baseline quality child is not its exact fresh public/whole chain")
    return {"source": child, "initial": initial, "author_sids": {author["cli_sid"], public["cli_sid"]},
        "binding": {"kind": KIND, "source_author_flow_id": parent["flow_id"], "source_revision": parent["revision"],
                    "baseline_source": material["proof"],
                    "receipt": evidence_ref(Path(state["receipt"]), "actual baseline quality result"),
                    "child_flow_id": child["flow_id"],
                    "child_receipt": evidence_ref(Path(child["state"]["receipt"]), "actual fresh baseline quality child")}}


def validate_frozen_baseline_source(proof: dict) -> dict:
    """Offline provenance transport; online admission additionally checks the DB."""
    from taskflow.execution_runner import _info, _validate, _validated_collection

    _require(isinstance(proof, dict) and proof.get("kind") == "research_ended_captured_baseline_source_v1"
             and type(proof.get("schema_version")) is int and proof["schema_version"] == 1,
             "typed frozen research provenance required")
    refs = validate_evidence(proof["evidence"])
    _require(len(refs) == 9, "complete frozen original research evidence required")
    receipt, handoff = read_json(Path(refs[0]["path"])), read_json(Path(refs[1]["path"]))
    root = Path(refs[2]["path"]).parent
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    _require(Path(refs[2]["path"]).name == "manifest.json"
             and Path(refs[4]["path"]) == root / "collection.json"
             and collection.get("sid") == proof["source_author_sid"]
             and collection.get("technical_accepted") is True and collection.get("process_ended") is True
             and collection["artifacts"].get("handoff.json") == _info(Path(refs[1]["path"]))
             and manifest.get("candidate_binding") == proof["baseline_binding"]
             and receipt.get("flow_id") == proof["source_flow_id"] and receipt.get("status") == "accepted"
             and receipt.get("result", {}).get("outcome") == "research_ended"
             and receipt["result"].get("candidate_created") is False
             and receipt["result"].get("author_sid") == proof["source_author_sid"]
             and any(row.get("kind") == "source_author_original_received"
                     and row.get("attempt_id") == proof["source_author_attempt"]
                     and row.get("cli_sid") == proof["source_author_sid"]
                     for row in receipt.get("history", []))
             and handoff.get("disposition") == "research_ended" and handoff.get("candidate_subpath") is None
             and handoff.get("independent_quality_passed") is False and handoff.get("new_formal_score") is None,
             "frozen original research cannot become candidate delivery or quality")
    return proof
