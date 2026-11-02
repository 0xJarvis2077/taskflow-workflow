"""Fixed, preauthorized execution handoffs on the existing SQLite ledger.

This consumer owns no model service or scheduling database. It binds a single
business packet to existing runner/import operations and records their result.
"""

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import time
from typing import Iterator

from taskflow.contracts import TaskflowError, canonical, relative_path, sha256, validate_id
from taskflow.execution_flow import _publish_once
from taskflow.execution_author_packet import author_packet_matches
from taskflow.execution_formal import _handoff_errors
from taskflow.execution_store import ExecutionStore, validate_evidence


KINDS = {"evaluation_tail_v1", "experiment_v1", "author_quality_v1", "teacher_successor_v1", "original_score_pause_v1"}


def register_lifecycle(store: ExecutionStore, definition: dict, evidence: list[dict]) -> dict:
    """Persist a bounded, authorized handoff before its predecessor completes."""
    if (type(definition.get("schema_version")) is not int or definition["schema_version"] != 1
            or not isinstance(definition.get("kind"), str) or definition["kind"] not in KINDS):
        raise TaskflowError("unsupported execution lifecycle definition")
    for key in ("flow_id", "task_id", "owner", "policy_id"):
        validate_id(definition.get(key), key)
    if not Path(definition.get("directory", "")).is_absolute():
        raise TaskflowError("lifecycle directory must be absolute")
    refs = validate_evidence(evidence)
    authorization = validate_evidence([definition["authorization"]])[0]
    if not any(item["path"] == authorization["path"] and item["sha256"] == authorization["sha256"] for item in refs):
        raise TaskflowError("lifecycle authorization must be in its immutable evidence")
    from taskflow.contracts import read_json

    packet = read_json(Path(authorization["path"]))
    authority_owner = packet.get("owner") if definition["kind"] == "teacher_successor_v1" else packet.get("controller")
    if authority_owner != definition["owner"]:
        raise TaskflowError("lifecycle owner differs from business authority")
    if packet.get("task_id") and packet["task_id"] != definition["task_id"]:
        raise TaskflowError("lifecycle task differs from business authority")
    store.get_policy(definition["policy_id"])
    if definition["kind"] == "original_score_pause_v1":
        from taskflow.execution_original_pause import validate_original_pause_definition

        validate_original_pause_definition(store, definition)
    if definition["kind"] == "teacher_successor_v1":
        from taskflow.execution_teacher_successor import validate_teacher_successor_definition

        validate_teacher_successor_definition(store, definition)
    if definition["kind"] == "author_quality_v1":
        attempt = store.get_attempt(definition["source_author_attempt"])
        binding = {key: attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
        if (definition["source_author_binding"] != binding or attempt["task_id"] != definition["task_id"]
                or attempt["role"] != "author" or attempt["dispatch_owner"] != definition["owner"]):
            raise TaskflowError("author lifecycle is not bound to the exact authorized original author")
    if definition["kind"] == "experiment_v1":
        if definition.get("source_author_flow_id"):
            from taskflow.execution_experiment import _packet

            _packet({"definition": definition})
        elif packet.get("source_flow_id") != definition.get("source_flow_id"):
            raise TaskflowError("experiment lifecycle source differs from its explicit investment")
    existing = next((item for item in store.list_flows() if item["flow_id"] == definition["flow_id"]), None)
    if existing:
        if existing["definition_digest"] != sha256(canonical(definition)) or existing["evidence"] != refs:
            raise TaskflowError("lifecycle cannot replace its original definition or authorization")
        return existing
    state = {"status": "active", "stage": definition["kind"], "current_work_id": None,
             "history": [], "archives": [], "waiting": None}
    return store.register_flow(definition["flow_id"], definition, refs, state)


@contextmanager
def _lifecycle_lock(store: ExecutionStore, identity: str) -> Iterator[None]:
    # Per-intent exclusion permits disjoint already-running quality consumers;
    # all role claims still share the existing SQLite concurrency/ownership gate.
    path = store.path.with_name(store.path.name + ".lifecycle-" + identity + ".lock")
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskflowError("this lifecycle already has a consumer") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _save(store: ExecutionStore, record: dict, state: dict, kind: str) -> dict:
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": kind})


def _receive(store: ExecutionStore, record: dict) -> dict:
    from taskflow.execution_controller_finding import current_after_controller_reopen

    current = current_after_controller_reopen(store, record)
    if current is not None:
        return current
    state = dict(record["state"])
    if state.get("receipt"):
        return record
    payload = {"flow_id": record["flow_id"], "status": state["status"],
               "controller": record["definition"]["owner"], "human_read_claimed": False,
               "history": state["history"], "archives": state["archives"],
               "result": state.get("result"), "exception": state.get("exception"),
               "substantive_next_decision": "registered controller",
               "experiment_validity_inferred": False}
    if state["status"] == "accepted":
        name = "result-" + sha256(canonical(payload))[:16] + ".json" if state.get("prior_receipts") else "result.json"
    else:
        name = "exception-" + sha256(canonical(payload))[:16] + ".json"
    target = Path(record["definition"]["directory"]) / name
    _publish_once(target, payload)
    state["receipt"] = str(target)
    return _save(store, record, state, "lifecycle_result_received")


def _resume_unclaimed_formal(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from taskflow.execution_experiment import _native_handoff, _node, _packet, _run_id
    from taskflow.execution_formal import _checked_ref, validate_formal_authority
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import check_task, verify_snapshot

    definition, state = record["definition"], dict(record["state"])
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "formal_prepare" or state.get("current_work_id") or not state.get("receipt")
            or state.get("exception") != {"stage": "formal_prepare",
                                          "reason": "formal configuration is outside the explicit business packet"}):
        raise TaskflowError("formal resume requires the exact received, unclaimed authority preparation exception")
    packet = _packet(record)
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    intent_id = work_id + "-intent"
    inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    intent = inner.with_name(inner.name + ".formal-intent.json")
    dashboard = store.dashboard()
    if any(row.get("work_id") == work_id or row.get("intent_id") == intent_id
           or row.get("runtime_path") in (str(inner), str(outer))
           for row in [*dashboard["work"], *dashboard["attempts"]]):
        raise TaskflowError("formal work/attempt already exists; claimed, failed or uncertain calls cannot be redrawn")
    if any(path.exists() or path.is_symlink() for path in (inner, outer, intent)):
        raise TaskflowError("formal preparation/runtime/intent already exists; preserve the original instead of retrying")
    candidate = store.get_candidate(state["candidate_id"])
    binding = {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
    source = check_task(Path(state["candidate_root"]), write_report=False)
    release = state["release"]
    frozen = verify_snapshot(Path(release["snapshot"]["path"]))
    if (candidate["task_id"] != definition["task_id"] or release["candidate_binding"] != binding or source["errors"]
            or any(source[key] != candidate[key] or frozen[key] != candidate[key] for key in
                   ("task_id", "content_digest", "input_digest", "evaluation_digest"))):
        raise TaskflowError("formal resume candidate/source/release frozen version differs")
    accepted = store.quality_gate(candidate["candidate_id"])
    store.assert_review_current(state["accepted_review_id"])
    if accepted["review_id"] != state["accepted_review_id"]:
        raise TaskflowError("formal resume quality differs from the original accepted source")
    if definition.get("source_author_flow_id"):
        _native_handoff(store, record, candidate["candidate_id"])
    for reference in [release["receipt"], *release["quality_evidence"]]:
        _checked_ref(reference)
    received = _read(Path(release["receipt"]["path"]))
    locked = received.get("binding", {})
    if (locked.get("candidate_binding") != binding or locked.get("accepted_review_id") != state["accepted_review_id"]
            or locked.get("source_root") != state["candidate_root"] or locked.get("version") != packet["release"]["version_id"]):
        raise TaskflowError("formal resume release receipt differs from its exact source/quality/version")
    for reference in locked["reports"].values():
        _checked_ref(reference)
    config = {**definition["technical"]["formal_config_base"], "run_id": run_id,
              "intent_id": intent_id, "policy_version": definition["policy_id"]}
    if definition.get("source_author_flow_id"):
        config["authorization_item"] = definition["business_packet_item"]
    authority = validate_formal_authority(config=config, candidate=candidate)
    if authority.get("controller") != definition["owner"]:
        raise TaskflowError("formal resume authority belongs to another registered controller")
    prior = evidence_ref(Path(state["receipt"]), "original unclaimed formal preparation exception preserved")
    state["history"] = [*state["history"], {"kind": "formal_preparation_technical_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"], "previous_receipt": prior,
        "work_id": work_id, "intent_id": intent_id, "run_id": run_id,
        "candidate_binding": binding, "accepted_review_id": state["accepted_review_id"],
        "preparation_paths": {"inner": str(inner), "outer": str(outer), "intent": str(intent)},
        "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_unclaimed_formal_preparation_resumed")


def _resolved_projection_dependency(store: ExecutionStore, parent: dict, previous: dict, *, active_only: bool) -> dict:
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_flow import _predecessor
    from taskflow.execution_flow_contracts import resolve_repair_record_projection
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref

    definition, state = parent["definition"], parent["state"]
    reason = previous.get("reason")
    if (previous.get("stage") != "child_quality" or not isinstance(reason, dict)
            or reason.get("reason") != "Repair changes duplicate or do not reflect actual differences"
            or not previous.get("child_receipt") or not state.get("child_flow_id")):
        raise TaskflowError("dependency resume requires the exact propagated repair-record projection exception")
    author, _ = _author(store, definition)
    if author["state"] != "completed" or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
        raise TaskflowError("dependency resume requires the original completed candidate author, never a substantive decision")
    child = store.get_flow(state["child_flow_id"])
    child_definition, child_state = child["definition"], child["state"]
    allowed = {"active"} if active_only else {"active", "accepted"}
    if (child_state.get("status") not in allowed
            or any(child_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or child_definition["initial"]["candidate_id"] != state["candidate_id"]
            or child_definition["initial"]["candidate_root"] != state["candidate_root"]):
        raise TaskflowError("dependency child is not the same active/resolved authorized candidate")
    public, _ = _predecessor(store, child_definition)
    if (public["attempt_id"] != state.get("public_attempt_id") or public["state"] != "completed"
            or public["role"] != "quality" or not public.get("cli_sid") or public["cli_sid"] == author["cli_sid"]):
        raise TaskflowError("dependency child differs from the original fresh public predecessor")
    old_receipt = evidence_ref(Path(previous["child_receipt"]), "original child repair projection exception preserved")
    received = _read(Path(old_receipt["path"]))
    if (received.get("flow_id") != child["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != reason):
        raise TaskflowError("dependency original child receipt differs from its propagated exception")
    matches = [row for row in child_state.get("resolved_exceptions", [])
               if row.get("exception") == reason and row.get("receipt") == previous["child_receipt"]
               and isinstance(row.get("repair_record_projection"), dict)]
    if len(matches) != 1:
        raise TaskflowError("dependency has no unique explicit resolution for this exact repair projection")
    resolved, proof = matches[0], matches[0]["repair_record_projection"]
    validate_evidence(resolved["evidence"])
    pair = {key: proof[key] for key in ("original", "projected")}
    if (child_state.get("repair_record_projections", {}).get(proof["attempt_id"]) != proof
            or child_state.get("last_resolution", {}).get("repair_record_projection") != pair
            or proof["parent_id"] != state["candidate_id"]
            or child_state["candidate_id"] not in (proof["parent_id"], proof["candidate_id"])):
        raise TaskflowError("dependency resolution does not bind the current exact repair/candidate")
    repair = store.get_attempt(proof["attempt_id"])
    actual = resolve_repair_record_projection(pair, attempt=repair, parent=store.get_candidate(proof["parent_id"]),
        candidate=store.get_candidate(proof["candidate_id"]), defect_ids=proof["validation"]["defect_ids"])
    if actual != proof:
        raise TaskflowError("dependency repair projection differs from its immutable shared validation")
    if child_state.get("current_work_id"):
        current = child_state["current_work_id"]
        rows = [row for row in store.dashboard()["attempts"] if row["work_id"] == current]
        if (store.get_work(current)["state"] in {"uncertain", "failed", "cancelled"}
                or any(row["state"] in {"uncertain", "failed", "cancelled"} for row in rows)):
            raise TaskflowError("dependency has an unresolved uncertain/failed current attempt")
    return {"flow_id": child["flow_id"], "revision": child["revision"], "status": child_state["status"],
            "previous_child_receipt": old_receipt, "repair_record_projection": proof}


def _resolved_public_preparation_dependency(store: ExecutionStore, parent: dict, previous: dict) -> dict:
    """Reopen only an explicitly resolved, never-launched public runtime node."""
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_flow import _binding, _identity, _predecessor, _public_inner_runtime, _render
    from taskflow.execution_flow_contracts import validate_repair
    from taskflow.execution_runner import RunnerError, _read, _validate, _validated_collection
    from taskflow.execution_store import evidence_ref

    definition, state = parent["definition"], parent["state"]
    reason = previous.get("reason")
    if (previous.get("stage") != "child_quality" or reason != {
            "reason": "Runtime must be outside the repository", "work_id": None}
            or not previous.get("child_receipt") or not state.get("child_flow_id")):
        raise TaskflowError("dependency needs the exact zero-launch public runtime exception")
    author, _ = _author(store, definition)
    if author["state"] != "completed" or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
        raise TaskflowError("public runtime dependency requires its original completed candidate author")
    child = store.get_flow(state["child_flow_id"])
    child_definition, current = child["definition"], child["state"]
    if (current.get("status") != "active" or current.get("stage") != "public_rebuild"
            or current.get("current_work_id") or type(current.get("step")) is not int or current["step"] < 1
            or any(child_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or child_definition["initial"]["candidate_id"] != state["candidate_id"]
            or child_definition["initial"]["candidate_root"] != state["candidate_root"]):
        raise TaskflowError("public runtime dependency is not the same explicitly resumed unclaimed child")
    public, _ = _predecessor(store, child_definition)
    if (public["attempt_id"] != state.get("public_attempt_id") or public["state"] != "completed"
            or public["role"] != "quality" or not public.get("cli_sid") or public["cli_sid"] == author["cli_sid"]):
        raise TaskflowError("public runtime dependency differs from the original independent public predecessor")
    old_receipt = evidence_ref(Path(previous["child_receipt"]), "original public runtime exception retained")
    received = _read(Path(old_receipt["path"]))
    if received.get("flow_id") != child["flow_id"] or received.get("status") != "exception" or received.get("exception") != reason:
        raise TaskflowError("public runtime child receipt differs from the propagated original exception")
    resolutions = [row for row in current.get("resolved_exceptions", [])
                   if row.get("exception") == reason and row.get("receipt") == previous["child_receipt"]]
    if len(resolutions) != 1:
        raise TaskflowError("public runtime child has no unique explicit resolution for the original receipt")
    resolved = resolutions[0]
    refs = validate_evidence(resolved["evidence"])
    resolution = {"reason": resolved["reason"], "evidence": refs}
    if (not isinstance(resolved["reason"], str) or not resolved["reason"].strip()
            or current.get("last_resolution") != {"digest": sha256(canonical(resolution)), **resolution}
            or "repair_record_projection" in resolved):
        raise TaskflowError("public runtime resolution differs from the child's exact explicit technical receipt")
    label = f"{current['step']}-public_rebuild"
    node_id = _identity(child["flow_id"], label)
    root = Path(child_definition["directory"]) / "nodes" / label
    runtime = root / "runtime"
    role = child_definition["roles"]["public_rebuild"]
    candidate = store.get_candidate(current["candidate_id"])
    context_ref = evidence_ref(root / "current.json", "original failed public-node context retained")
    context = _read(root / "current.json")
    if (role["runner"].get("mode") != "existing_isolated"
            or any(context.get(key) != value for key, value in _binding(candidate).items())
            or context.get("node_id") != node_id or context.get("intent_id") != node_id
            or context.get("runtime") != str(runtime) or context.get("candidate_root") != current["candidate_root"]):
        raise TaskflowError("public runtime resolution has a foreign node, candidate or original context")
    inner = _public_inner_runtime(role, context)
    legacy = runtime.with_name(runtime.name + ".public")
    repository = Path(_render(role["prepare"]["config"], context)["repository"]).resolve()
    if (not legacy.resolve().is_relative_to(repository) or inner == legacy
            or inner.is_relative_to(repository) or repository.is_relative_to(inner)):
        raise TaskflowError("public runtime resolution does not repair the original repository overlap")
    board = store.dashboard()
    if (any(row.get("work_id") == node_id or row.get("intent_id") == node_id
            or row.get("runtime_path") in {str(runtime), str(legacy), str(inner)}
            for row in [*board["work"], *board["attempts"]])
            or any(path.exists() or path.is_symlink() for path in (root / "spec.json", runtime, legacy, inner))
            or root.is_symlink() or any(path.is_symlink() or (not path.is_dir() and path != root / "current.json")
                                      for path in root.rglob("*"))):
        raise TaskflowError("public runtime resolution has an existing node work, attempt, specification or launch evidence")
    repairs = [row for row in current["history"] if row.get("kind") == "actual_repair"]
    if not repairs or repairs[-1].get("candidate_id") != candidate["candidate_id"] or repairs[-1].get("public_changed") is not True:
        raise TaskflowError("public runtime node lacks its actual public-changing repair")
    originals = []
    for entry in current["history"]:
        if entry.get("kind") not in {"independent_review", "actual_repair"}:
            continue
        attempt = store.get_attempt(entry["attempt_id"])
        original_root = Path(attempt["runtime_path"])
        try:
            manifest = _validate(original_root)
            collected = _validated_collection(original_root, manifest)
        except RunnerError as error:
            raise TaskflowError("public runtime original sealed source invalid: " + str(error)) from error
        original_candidate = store.get_candidate(manifest["candidate_binding"]["candidate_id"])
        if (attempt["state"] != "completed" or manifest["candidate_binding"] != _binding(original_candidate)
                or manifest["intent_id"] != attempt["intent_id"] or collected.get("technical_accepted") is not True
                or collected.get("process_ended") is not True or collected.get("sid") != entry.get("cli_sid")
                or attempt.get("cli_sid") != entry.get("cli_sid")):
            raise TaskflowError("public runtime dependency original quality/repair source changed")
        if entry["kind"] == "independent_review":
            review = store.get_review(entry["review_id"])
            if review["attempt_id"] != attempt["attempt_id"] or review["candidate_id"] != original_candidate["candidate_id"]:
                raise TaskflowError("public runtime dependency original review identity changed")
            validate_evidence(review["evidence"])
        originals.append({"attempt_id": attempt["attempt_id"],
                          "collection": evidence_ref(original_root / "collection.json", "original sealed role collection")})
    repair = store.get_attempt(repairs[-1]["attempt_id"])
    repair_parent = store.get_candidate(candidate["parent_id"])
    if (candidate.get("author_attempt") != repair["attempt_id"] or repair["candidate_id"] != repair_parent["candidate_id"]
            or repair_parent["input_digest"] == candidate["input_digest"]):
        raise TaskflowError("public runtime dependency repair lineage or public change differs")
    validate_repair(Path(repair["runtime_path"]) / "artifacts" / child_definition["roles"]["repair"].get("repair_file", "repair-record.json"),
                    parent=repair_parent, candidate=candidate, defect_ids=[row["defect_id"] for row in current["defects"]])
    for identity in current["repaired_defects"]:
        defect = store.get_defect(identity)
        if defect["state"] != "repaired" or defect.get("repair_candidate_id") != candidate["candidate_id"]:
            raise TaskflowError("public runtime dependency may not close or substitute a repaired defect")
    proof = {"node_id": node_id, "candidate_id": candidate["candidate_id"], "context": context_ref,
             "original_inner": str(legacy), "corrected_inner": str(inner), "resolution": resolution,
             "original_collections": originals, "repaired_defect_ids": current["repaired_defects"]}
    return {"flow_id": child["flow_id"], "revision": child["revision"], "status": current["status"],
            "previous_child_receipt": old_receipt, "public_preparation_resolution": proof}


def _resolved_coverage_chain_dependency(store: ExecutionStore, parent: dict, previous: dict) -> dict:
    """Receive this child's completed closure chain, never an alternate model."""
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_flow import _predecessor
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref

    definition, state = parent["definition"], parent["state"]
    reason = previous.get("reason", {})
    if (previous.get("stage") != "child_quality" or state.get("pending_business_scope") or not isinstance(reason, dict)
            or reason != {"reason": "closure requires whole-package review or explicit covered same-public private recheck",
                          "work_id": reason.get("work_id")}
            or not isinstance(reason.get("work_id"), str) or not reason["work_id"]
            or not previous.get("child_receipt") or not state.get("child_flow_id")):
        raise TaskflowError("dependency needs the exact completed public-version closure exception")
    author, _ = _author(store, definition)
    if author["state"] != "completed" or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
        raise TaskflowError("closure dependency requires its original completed candidate author")
    child = store.get_flow(state["child_flow_id"])
    child_definition, current = child["definition"], child["state"]
    if (current.get("status") != "accepted" or current.get("current_work_id") or not current.get("receipt")
            or current.get("stage") not in {"recheck", "recheck_public"}
            or any(child_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or child_definition["initial"]["candidate_id"] != state["candidate_id"]
            or child_definition["initial"]["candidate_root"] != state["candidate_root"]):
        raise TaskflowError("closure dependency is not the same actually accepted child")
    public, _ = _predecessor(store, child_definition)
    if (public["attempt_id"] != state.get("public_attempt_id") or public["state"] != "completed"
            or public["role"] != "quality" or not public.get("cli_sid") or public["cli_sid"] == author["cli_sid"]):
        raise TaskflowError("closure dependency differs from its original independent public predecessor")
    original = evidence_ref(Path(previous["child_receipt"]), "original completed closure exception retained")
    received = _read(Path(original["path"]))
    if received.get("flow_id") != child["flow_id"] or received.get("status") != "exception" or received.get("exception") != reason:
        raise TaskflowError("closure dependency original child exception receipt differs")
    resolved = [row for row in current.get("resolved_exceptions", [])
                if row.get("exception") == reason and row.get("receipt") == previous["child_receipt"]]
    if len(resolved) != 1:
        raise TaskflowError("closure dependency has no unique explicit original exception resolution")
    refs = validate_evidence(resolved[0]["evidence"])
    resolution = {"reason": resolved[0]["reason"], "evidence": refs}
    if current.get("last_resolution") != {"digest": sha256(canonical(resolution)), **resolution}:
        raise TaskflowError("closure dependency differs from its exact explicit child resolution")
    receipt = evidence_ref(Path(current["receipt"]), "actual accepted child closure receipt")
    payload = _read(Path(receipt["path"]))
    if (payload.get("flow_id") != child["flow_id"] or payload.get("status") != "accepted"
            or payload.get("candidate_id") != current["candidate_id"] or payload.get("history") != current["history"]
            or payload.get("controller") != definition["owner"]):
        raise TaskflowError("closure dependency current child receipt differs from its actual result")
    review = store.assert_review_current(current["accepted_review_id"])
    attempt = store.get_attempt(review["attempt_id"])
    attempts = [row for row in store.dashboard()["attempts"] if row["work_id"] == reason["work_id"]]
    if (review["verdict"] != "accepted" or review["candidate_id"] != current["candidate_id"]
            or attempt["work_id"] != reason["work_id"] or attempt["state"] != "completed"
            or attempt["role"] != "quality" or attempt["candidate_id"] != current["candidate_id"]
            or len(attempts) != 1 or attempts[0]["attempt_id"] != attempt["attempt_id"]
            or set(review.get("covered_defect_ids", [])) != set(current["repaired_defects"])):
        raise TaskflowError("closure dependency substituted another completed review or defect scope")
    chains = []
    history = {entry.get("review_id") for entry in current["history"] if entry.get("kind") == "independent_review"}
    for identity in current["repaired_defects"]:
        defect = store.get_defect(identity)
        if defect["state"] != "closed" or defect.get("closure_review_id") != review["review_id"]:
            raise TaskflowError("closure dependency still has an unresolved or differently closed defect")
        validate_evidence(defect["closure_evidence"])
        if defect.get("closure_coverage_chain"):
            chain = store.validate_defect_coverage_chain(identity)
            if chain["final_review_id"] != review["review_id"] or chain["repair_review_id"] not in history:
                raise TaskflowError("closure dependency typed chain differs from its actual signed history")
            chains.append(chain)
    if not chains:
        raise TaskflowError("closure dependency needs its actual validated public-version coverage chain")
    proof = {"candidate_id": current["candidate_id"], "final_review_id": review["review_id"],
             "final_attempt_id": attempt["attempt_id"], "child_receipt": receipt,
             "resolution": resolution, "closure_coverage_chains": chains}
    return {"flow_id": child["flow_id"], "revision": child["revision"], "status": current["status"],
            "previous_child_receipt": original, "coverage_chain_resolution": proof}


def _resolved_repair_identity_dependency(store: ExecutionStore, parent: dict, previous: dict, *, active_only: bool) -> dict:
    """Consume only an explicit resolution of the original repair-name error."""
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_flow import _completed_repair_identity, _identity, _predecessor
    from taskflow.execution_runner import _read, _validate, _validated_collection
    from taskflow.execution_store import evidence_ref

    definition, state = parent["definition"], parent["state"]
    reason = previous.get("reason")
    if (previous.get("stage") != "child_quality" or not isinstance(reason, dict)
            or reason != {"reason": "repair defect_ids: list required", "work_id": reason.get("work_id")}
            or not isinstance(reason.get("work_id"), str) or not reason["work_id"]
            or state.get("pending_business_scope") or not previous.get("child_receipt")
            or not state.get("child_flow_id")):
        raise TaskflowError("dependency needs the exact resolved repair identity transport exception")
    author, _ = _author(store, definition)
    if author["state"] != "completed" or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
        raise TaskflowError("repair identity dependency requires its original completed candidate author")
    child = store.get_flow(state["child_flow_id"])
    child_definition, current = child["definition"], child["state"]
    allowed = {"active"} if active_only else {"active", "accepted"}
    if (current.get("status") not in allowed or current.get("pending_business_scope")
            or current.get("stage") not in {"repair", "recheck", "public_rebuild", "recheck_public"}
            or any(child_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or child_definition["initial"]["candidate_id"] != state["candidate_id"]
            or child_definition["initial"]["candidate_root"] != state["candidate_root"]):
        raise TaskflowError("repair identity dependency differs from its same authorized active/resolved child")
    public, _ = _predecessor(store, child_definition)
    if (public["attempt_id"] != state.get("public_attempt_id") or public["state"] != "completed"
            or public["role"] != "quality" or not public.get("cli_sid") or public["cli_sid"] == author["cli_sid"]):
        raise TaskflowError("repair identity dependency differs from its original independent public predecessor")
    public_root = Path(public["runtime_path"])
    collected = _validated_collection(public_root, _validate(public_root))
    if (collected.get("sid") != public["cli_sid"]
            or any(collected.get(key) is not True for key in ("technical_accepted", "process_ended", "delivery_complete"))):
        raise TaskflowError("repair identity dependency original public collection differs")
    original = evidence_ref(Path(previous["child_receipt"]), "original repair identity child exception retained")
    received = _read(Path(original["path"]))
    if (received.get("flow_id") != child["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != reason or received.get("candidate_id") != state["candidate_id"]):
        raise TaskflowError("repair identity dependency original child exception receipt differs")
    matches = [row for row in current.get("resolved_exceptions", [])
               if row.get("exception") == reason and row.get("receipt") == previous["child_receipt"]]
    if len(matches) != 1:
        raise TaskflowError("repair identity dependency lacks its unique explicit child resolution")
    resolved = matches[0]
    refs = validate_evidence(resolved["evidence"])
    resolution = {"reason": resolved["reason"], "evidence": refs}
    proof = resolved.get("repair_defect_identity")
    if (not isinstance(proof, dict) or not isinstance(resolved["reason"], str) or not resolved["reason"].strip()
            or current.get("last_resolution") != {"digest": sha256(canonical(resolution)), **resolution}
            or current.get("repair_defect_identities", {}).get(proof["attempt_id"]) != proof
            or proof["work_id"] != reason["work_id"] or proof["parent_id"] != state["candidate_id"]
            or proof != _completed_repair_identity(store, child, reason["work_id"])):
        raise TaskflowError("repair identity dependency differs from its exact sealed original resolution")
    if current.get("current_work_id"):
        work_id = current["current_work_id"]
        work = store.get_work(work_id)
        attempts = [row for row in store.dashboard()["attempts"] if row["work_id"] == work_id]
        runtime = str(Path(child_definition["directory"]) / "nodes" / f"{current['step']}-{current['stage']}" / "runtime")
        if (work["candidate_id"] != current["candidate_id"]
                or work_id != _identity(child["flow_id"], f"{current['step']}-{current['stage']}")
                or current.get("current_node_id") != work_id or current.get("current_runtime") != runtime
                or work.get("runtime_path") != runtime or work.get("task_id") != definition["task_id"]
                or work.get("intent_id") != work_id
                or work.get("role") != ("repair" if current["stage"] == "repair" else "quality")
                or work["state"] in {"uncertain", "failed", "cancelled"}
                or any(row["state"] in {"uncertain", "failed", "cancelled"}
                       or any(row.get(key) != work.get(key) for key in ("intent_id", "candidate_id", "runtime_path", "task_id", "role"))
                       for row in attempts)):
            raise TaskflowError("repair identity dependency has unresolved or foreign current work")
    return {"flow_id": child["flow_id"], "revision": child["revision"], "status": current["status"],
            "previous_child_receipt": original, "repair_defect_identity": proof}


def _resolved_quality_dependency(store: ExecutionStore, parent: dict, previous: dict, *, active_only: bool) -> dict:
    if isinstance(previous.get("reason"), dict) and previous["reason"].get("reason") == "repair defect_ids: list required":
        return _resolved_repair_identity_dependency(store, parent, previous, active_only=active_only)
    if previous.get("reason") == {"reason": "Runtime must be outside the repository", "work_id": None}:
        return _resolved_public_preparation_dependency(store, parent, previous)
    if isinstance(previous.get("reason"), dict) and previous["reason"].get("reason") == "closure requires whole-package review or explicit covered same-public private recheck":
        return _resolved_coverage_chain_dependency(store, parent, previous)
    return _resolved_projection_dependency(store, parent, previous, active_only=active_only)


def _resume_quality_dependency(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from taskflow.execution_author import _author
    from taskflow.execution_experiment import _node, _packet, _run_id
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    if state.get("status") != "exception" or state.get("current_work_id") or not state.get("receipt"):
        raise TaskflowError("dependency resume requires a received exception with no current work")
    if definition.get("kind") == "author_quality_v1" and state.get("stage") == "child_quality":
        dependency = _resolved_quality_dependency(store, record, state["exception"], active_only=True)
    elif definition.get("kind") == "experiment_v1" and state.get("stage") == "experiment_v1" and definition.get("source_author_flow_id"):
        packet = _packet(record)
        parent = store.get_flow(definition["source_author_flow_id"])
        source_definition, source = parent["definition"], parent["state"]
        entries = [row for row in source.get("history", []) if row.get("kind") == "quality_dependency_technical_resolution"]
        if (source_definition.get("kind") != "author_quality_v1" or source.get("status") not in {"active", "accepted"}
                or not entries or any(source_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
                or state["exception"] != {"stage": "experiment_v1", "reason":
                    "source author-quality flow stopped with its original exception: " + str(entries[-1]["previous_exception"])}):
            raise TaskflowError("experiment dependency is not its exact technically resumed source exception")
        author, _ = _author(store, source_definition)
        authority = validate_evidence([source_definition["authorization"]])[0]
        if (author["attempt_id"] != packet["source_author_attempt"] or author["intent_id"] != packet["source_author_intent"]
                or author["cli_sid"] != packet["source_author_sid"] or not author_packet_matches(store, parent=parent, packet=packet,
                    authorization=definition["authorization"], packet_transport=state.get("source_business_packet_transport"))):
            raise TaskflowError("experiment dependency source author/authority binding differs")
        dependency = _resolved_quality_dependency(store, parent, entries[-1]["previous_exception"], active_only=False)
        keys = [key for key in ("public_preparation_resolution", "repair_record_projection", "coverage_chain_resolution", "repair_defect_identity") if key in dependency]
        if len(keys) != 1 or entries[-1]["dependency_resolution"].get(keys[0]) != dependency[keys[0]]:
            raise TaskflowError("experiment dependency differs from the source's explicit technical resolution")
        validate_evidence([entries[-1]["previous_receipt"], *entries[-1]["evidence"]])
        if source["status"] == "accepted":
            child = store.get_flow(source["child_flow_id"])
            result = source.get("result", {})
            if (dependency["status"] != "accepted" or result.get("outcome") != "candidate_quality_accepted"
                    or result.get("child_flow_id") != child["flow_id"] or result.get("child_receipt") != child["state"].get("receipt")
                    or any(result.get(key) != child["state"].get(key) for key in ("candidate_id", "candidate_root", "accepted_review_id"))):
                raise TaskflowError("accepted source dependency differs from its current actual child result")
            store.assert_review_current(result["accepted_review_id"])
        run_id, work_id = _run_id(record, packet), _node(record, "solver")
        inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
        outer = Path(definition["directory"]) / "runtimes" / work_id
        dashboard = store.dashboard()
        if "repair_defect_identity" in dependency:
            publication = Path(definition["technical"]["publication_root"])
            if (state.get("pending_business_scope") or state.get("candidate_id") or state.get("accepted_review_id")
                    or state.get("runs") or state.get("release") or state.get("run_index", 0) != 0
                    or publication.exists() or publication.is_symlink()):
                raise TaskflowError("repair identity dependency requires an initial unpublished experiment without business holds")
        if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
                or row.get("runtime_path") in (str(inner), str(outer)) for row in [*dashboard["work"], *dashboard["attempts"]])
                or any(path.exists() or path.is_symlink() for path in (inner, outer, inner.with_name(inner.name + ".formal-intent.json")))):
            raise TaskflowError("experiment dependency has an existing solver work/attempt/runtime/intent")
        dependency = {**dependency, "source_author_flow_id": parent["flow_id"], "source_revision": parent["revision"]}
    else:
        raise TaskflowError("dependency resume is limited to original child-quality/source-author exceptions")
    prior = evidence_ref(Path(state["receipt"]), "original propagated quality dependency exception preserved")
    received = _read(Path(prior["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != state["exception"]):
        raise TaskflowError("propagated dependency receipt differs from this exact original exception")
    state["history"] = [*state["history"], {"kind": "quality_dependency_technical_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"], "previous_receipt": prior,
        "dependency_resolution": dependency, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_quality_dependency_resumed")


def _resume_source_quality_transport(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    """Consume a repaired source transport contract without creating acceptance."""
    import json

    from taskflow.execution_experiment import _author_source, _native_handoff, _node, _packet, _run_id
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    reason = state.get("exception", {}).get("reason")
    allowed = {"native quality dimension has no explicit scoped affirmation: calculation",
               "native quality dimension has no explicit scoped affirmation: business",
               "Composition defect lacks exact repair/closure identity and actual ordering",
               "Issue evidence is absent from current candidate or has wrong SHA",
               "defect was repaired only in a different/later revision",
               "later independent rejection/conflict covers the accepted scope; old acceptance is not current",
               "historical negative lacks exact current whole repair coverage",
               "historical negative is outside the current repaired quality graph",
               "Composition cannot silently inherit changed package/scoring metadata",
               "native quality coverage must explicitly delimit whole or affected files"}
    whole_finding_locations = reason == "native quality coverage must explicitly delimit whole or affected files"
    historical_closed_ancestor = reason == "historical negative lacks exact current whole repair coverage"
    historical_repaired_graph = reason == "historical negative is outside the current repaired quality graph"
    scoring_mirror = reason == "Composition cannot silently inherit changed package/scoring metadata"
    repair_alias = reason == "defect was repaired only in a different/later revision"
    historical_negative = reason == "later independent rejection/conflict covers the accepted scope; old acceptance is not current"
    limited_findings = reason == "native quality dimension has no explicit scoped affirmation: business"
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "experiment_v1" or state.get("current_work_id") or not state.get("receipt")
            or state.get("exception") != {"stage": "experiment_v1", "reason": reason}
            or not isinstance(reason, str) or reason not in allowed
            or not definition.get("source_author_flow_id") or state.get("pending_business_scope")
            or ((scoring_mirror or historical_closed_ancestor or historical_repaired_graph or whole_finding_locations) and state.get("pending_business_scope") is not None)
            or state.get("candidate_id") or state.get("accepted_review_id") or state.get("runs") or state.get("release")
            or state.get("run_index", 0) != 0):
        raise TaskflowError("source transport resume requires its exact received initial zero-launch exception")
    original = evidence_ref(Path(state["receipt"]), "original source-quality transport exception retained")
    received = _read(Path(original["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != state["exception"]):
        raise TaskflowError("source transport original exception receipt differs")
    if (repair_alias or historical_negative or limited_findings or scoring_mirror or historical_closed_ancestor or historical_repaired_graph or whole_finding_locations) and (received.get("controller") != definition["owner"]
            or any(received.get(key) != state.get(key) for key in ("history", "archives", "result"))
            or Path(original["path"]).name != "exception-" + sha256(canonical(received))[:16] + ".json"):
        raise TaskflowError("repair alias transport differs from its original received history/hash")
    packet = _packet(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    if (parent["state"].get("status") != "accepted" or not parent["state"].get("receipt")
            or parent["state"].get("result", {}).get("outcome") != "candidate_quality_accepted"):
        raise TaskflowError("source transport requires the same already accepted author-quality candidate")
    bound = _author_source(store, record, packet)
    if "record" in bound:
        raise TaskflowError("source transport cannot wait, alter or receive a different author outcome")
    child, initial = bound["source"], bound["initial"]
    if (historical_negative and any(flow["state"].get("pending_business_scope") for flow in (parent, child))
            or (scoring_mirror or historical_closed_ancestor or historical_repaired_graph or whole_finding_locations) and any(flow["state"].get("pending_business_scope") is not None
                                      or flow["state"].get("exception") is not None for flow in (parent, child))):
        raise TaskflowError("source transport cannot clear a source business hold")
    candidate = store.get_candidate(child["state"]["candidate_id"])
    current = candidate
    seen = set()
    while current["candidate_id"] != initial["candidate_id"]:
        if current["candidate_id"] in seen or not current.get("parent_id"):
            raise TaskflowError("source transport candidate is outside its original authorized lineage")
        seen.add(current["candidate_id"])
        current = store.get_candidate(current["parent_id"])
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    publication = Path(definition["technical"]["publication_root"])
    paths = (Path(definition["technical"]["formal_runtime_root"]), inner, outer, publication,
             inner.with_name(inner.name + ".formal-intent.json"), publication / "reports/runtime-reservations" / (run_id + ".json"))
    with store._db() as db:
        board = {table: [json.loads(row["data"]) for row in db.execute("SELECT data FROM " + table)]
                 for table in ("work", "attempts", "decisions")}
    if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
            or row.get("runtime_path") in {str(inner), str(outer)}
            or (row.get("candidate_id") == candidate["candidate_id"] and row.get("role") == "solver")
            for row in [*board["work"], *board["attempts"]])
            or any(path.exists() or path.is_symlink() for path in paths)
            or ((historical_negative or scoring_mirror or historical_closed_ancestor or historical_repaired_graph or whole_finding_locations) and any(row.get("decision_id") == _node(record, "experiment-decision")
                or (row.get("candidate_id") == candidate["candidate_id"] and row.get("action") == "experiment")
                for row in board["decisions"]))):
        raise TaskflowError("source transport has an existing formal publication, preparation, work or attempt")
    review = (store.quality_gate(candidate["candidate_id"], require_repair_alias=True) if repair_alias or historical_negative or historical_closed_ancestor or historical_repaired_graph
              else store.quality_gate(candidate["candidate_id"]))
    store.assert_review_current(review["review_id"])
    graph = review.get("composite_quality_coverage")
    source_review = graph["recheck_id"] if graph else review["review_id"]
    if source_review != child["state"]["accepted_review_id"]:
        raise TaskflowError("source transport current quality differs from the accepted original child")
    if repair_alias or historical_closed_ancestor:
        if (graph or review.get("whole_package") is not True or review.get("derived")
                or (historical_closed_ancestor and review.get("snapshot_bound_acceptance"))
                or review["candidate_id"] != candidate["candidate_id"]):
            raise TaskflowError("repair alias transport requires the original current independent whole acceptance")
    elif reason != "native quality dimension has no explicit scoped affirmation: calculation":
        gate = packet.get("quality_and_candidate_gate", {})
        history = {entry.get("review_id") for entry in child["state"].get("history", [])}
        if (not graph or review.get("derived") is not True or review.get("whole_package") is not False
                or review.get("snapshot_bound_acceptance") is not True or not gate.get("required")
                or not gate.get("no_implicit_experiment") or graph["whole_review_id"] not in history
                or graph["whole_source"]["candidate_id"] != initial["candidate_id"]
                or set(graph["closed_defect_ids"]) != set(store.get_review(source_review).get("covered_defect_ids", []))):
            raise TaskflowError("source transport needs its existing exact canonical repaired-quality coverage")
    if scoring_mirror:
        # The canonical gate rederived the full witness; bind this recovery to
        # that actual declaration/source before comparing the native graph.
        mirror = graph.get("scoring_metadata_mirror")
        mirror = mirror if isinstance(mirror, dict) else {}
        source = mirror.get("source", {})
        source = source if isinstance(source, dict) else {}
        selected = [row for row in candidate.get("files", []) if row.get("package_path") == source.get("package_path")]
        changed = [row for row in graph["private_changes"] if row.get("path") == source.get("package_path")]
        declarations = [row for row in candidate.get("evidence", []) if row.get("location") == "explicit package declaration"]
        current_decl, current_source = mirror.get("current_declaration", {}), source.get("current", {})
        delta = graph.get("declaration_delta", {})
        if (type(mirror.get("schema_version")) is not int or mirror["schema_version"] != 1
                or mirror.get("kind") != "deterministic_signed_scoring_mirror_v1" or mirror.get("json_pointer") != "/scoring"
                or not isinstance(mirror.get("before"), dict) or not isinstance(mirror.get("after"), dict)
                or not isinstance(mirror.get("changes"), list) or not mirror["changes"]
                or not isinstance(mirror.get("signed_evidence"), list) or not mirror["signed_evidence"]
                or not isinstance(delta, dict)
                or canonical(delta.get("original")) != canonical(mirror.get("original_declaration"))
                or canonical(delta.get("current")) != canonical(current_decl)
                or len(selected) != 1 or len(changed) != 1 or changed[0].get("original_issue_affected") is not True
                or source.get("role") != selected[0]["role"]
                or changed[0].get("new_sha256") != selected[0]["sha256"]
                or len(declarations) != 1 or not isinstance(current_decl, dict) or not isinstance(current_source, dict)
                or any(current_decl.get(key) != declarations[0][key] for key in ("path", "sha256"))
                or any(current_source.get(key) != selected[0][key] for key in ("path", "sha256"))):
            raise TaskflowError("source transport requires its current signed scoring metadata mirror")
    handoff = _native_handoff(store, record, candidate["candidate_id"])
    fresh = handoff["certification"]["fresh"]
    if reason == "native quality dimension has no explicit scoped affirmation: calculation":
        acceptance = fresh.get("candidate_acceptance", {})
        if (review.get("whole_package") is not True or graph
                or acceptance.get("kind") != "accepted_candidate_with_nonblocking_observations"
                or acceptance.get("original_dimensions", {}).get("calculation", {}).get("affirmed") is not False
                or acceptance.get("blocking_issue_ids") != [] or not acceptance.get("observations")
                or acceptance != handoff["protocol_map"].get("candidate_acceptance")):
            raise TaskflowError("source transport must preserve its original limited nonblocking calculation signature")
    elif handoff["certification"].get("composite_quality_coverage") != graph:
        raise TaskflowError("source transport native certification differs from the actual canonical coverage graph")
    if whole_finding_locations:
        repaired = handoff["certification"].get("repaired_candidate_acceptance", {})
        prior = repaired.get("prior_whole_findings", {})
        witness = prior.get("finding_locations", {})
        paths = prior.get("affected_paths", [])
        if (not graph or repaired != handoff["protocol_map"].get("repaired_candidate_acceptance")
                or prior != handoff["certification"].get("whole", {}).get("prior_whole_findings")
                or prior.get("coverage") != "whole_package" or prior.get("outcome") != "revision_required"
                or not prior.get("blocking_issue_ids") or not paths
                or type(witness.get("schema_version")) is not int or witness["schema_version"] != 1
                or witness.get("kind") != "original_negative_whole_finding_locations"
                or witness.get("coverage") != "whole_package" or witness.get("outcome") != "revision_required"
                or witness.get("pointer") != "/native_signoff/affected_paths"
                or witness.get("actual_value") != paths
                or witness.get("blocking_issue_ids") != prior["blocking_issue_ids"]
                or not isinstance(witness.get("targets"), list)
                or {row.get("package_path") for row in witness["targets"]} != set(paths)
                or any(witness.get("report", {}).get(key) != graph["whole_source"]["report"].get(key)
                       for key in ("path", "sha256"))):
            raise TaskflowError("source transport requires its exact validated original whole finding locations")
    if limited_findings:
        repaired = handoff["certification"].get("repaired_candidate_acceptance", {})
        prior = repaired.get("prior_whole_findings", {})
        if (not graph or prior.get("coverage") != "unchanged_scope"
                or prior.get("original_dimensions", {}).get("business", {}).get("affirmed") is not False
                or repaired != handoff["protocol_map"].get("repaired_candidate_acceptance")):
            raise TaskflowError("source transport requires its exact limited original business findings and affected closure")
    state["history"] = [*state.get("history", []), {"kind": "source_quality_transport_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"], "previous_receipt": original,
        "source_binding": bound["binding"], "candidate_id": candidate["candidate_id"],
        "accepted_review_id": review["review_id"], "native_certification": handoff["certification"],
        "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_source_quality_transport_resumed")



def _resume_unpublished_release_metadata(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    """Retry the first atomic commit only when failed metadata left no release."""
    from taskflow.execution_experiment import _author_source, _native_handoff, _node, _packet, _run_id
    from taskflow.execution_formal import _binding, _projection_data
    from taskflow.execution_runner import _info, _read, _regular
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import check_task

    definition, state = record["definition"], dict(record["state"])
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "release" or state.get("current_work_id") or not state.get("receipt")
            or state.get("exception") != {"stage": "release", "reason": "snapshot file manifest mismatch"}
            or not definition.get("source_author_flow_id") or state.get("pending_business_scope")
            or state.get("release") or state.get("runs") or state.get("run_index", 0) != 0):
        raise TaskflowError("release metadata resume requires the exact received unpublished initial release exception")
    original = evidence_ref(Path(state["receipt"]), "original unpublished metadata release exception retained")
    received = _read(Path(original["path"]))
    if received.get("flow_id") != record["flow_id"] or received.get("status") != "exception" or received.get("exception") != state["exception"]:
        raise TaskflowError("release metadata original exception receipt differs")
    packet = _packet(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    if parent["state"].get("status") != "accepted" or parent["state"].get("result", {}).get("outcome") != "candidate_quality_accepted":
        raise TaskflowError("release metadata source is no longer its accepted candidate outcome")
    bound = _author_source(store, record, packet)
    if "record" in bound or canonical(bound["binding"]) != canonical(state.get("author_source_binding")):
        raise TaskflowError("release metadata source author binding differs from its original intake")
    child = bound["source"]
    if any(state.get(key) != child["state"].get(key) for key in ("candidate_id", "candidate_root")):
        raise TaskflowError("release metadata source candidate differs from its accepted child")
    candidate = store.get_candidate(state["candidate_id"])
    accepted = store.quality_gate(candidate["candidate_id"])
    store.assert_review_current(accepted["review_id"])
    if accepted["review_id"] != state["accepted_review_id"]:
        raise TaskflowError("release metadata current quality differs from the originally bound signature")
    native = _native_handoff(store, record, candidate["candidate_id"])
    if (canonical(native) != canonical(state.get("native_quality_handoff"))
            or canonical(native["source_refs"]) != canonical(state["quality_sources"])):
        raise TaskflowError("release metadata original native source or raw evidence differs")
    source = Path(state["candidate_root"])
    actual = check_task(source, write_report=False)
    if actual["errors"] or any(actual[key] != candidate[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("release metadata current source bytes differ from the captured candidate")
    task_raw = _regular(source / "task.json")
    task = _read(source / "task.json")
    if not any("sha256" in entry or "size" in entry for entry in task["files"]):
        raise TaskflowError("release metadata exception has no original declared computed-metadata collision")
    projected = _projection_data(store, candidate, state["quality_sources"], native["protocol_map"])
    required = {entry["id"]: entry for entry in task["reviews"] if entry["required"]}
    if set(required) != set(projected):
        raise TaskflowError("release metadata required projection IDs differ from the failed original publication")
    reports = {}
    for identity in required:
        path = Path(definition["directory"]) / "quality-projection" / (identity + ".json")
        if _regular(path) != canonical(projected[identity]) + b"\n":
            raise TaskflowError("release metadata original projected source bytes differ")
        reports[identity] = {"path": str(path.absolute()), **_info(path)}
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    publication = Path(definition["technical"]["publication_root"])
    formal_root = Path(definition["technical"]["formal_runtime_root"])
    inner, outer = formal_root / run_id, Path(definition["directory"]) / "runtimes" / work_id
    paths = (publication, publication.with_name("." + publication.name + ".lock"), formal_root, inner, outer,
             inner.with_name(inner.name + ".formal-intent.json"), publication / "reports/runtime-reservations" / (run_id + ".json"))
    board = store.dashboard()
    if (any(path.exists() or path.is_symlink() for path in paths)
            or any(publication.parent.glob("." + publication.name + ".staging-*"))
            or any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
                   or row.get("runtime_path") in {str(inner), str(outer)}
                   or (row.get("candidate_id") == candidate["candidate_id"] and row.get("role") == "solver")
                   for row in [*board["work"], *board["attempts"]])):
        raise TaskflowError("release metadata retry has existing publication, staging, formal preparation, work or attempt")
    identity = {"candidate_binding": _binding(candidate), "source_root": str(source.absolute()),
                "version": packet["release"]["version_id"], "task_json": {"sha256": sha256(task_raw), "size": len(task_raw)},
                "accepted_review_id": accepted["review_id"], "reports": reports}
    state["history"] = [*state.get("history", []), {"kind": "unpublished_release_metadata_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"], "previous_receipt": original,
        "publication_root": str(publication), "first_publication_identity": identity,
        "previous_published_snapshot": False, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_unpublished_release_metadata_resumed")


@_handoff_errors
def _resume_unpublished_release_report_binding(store: ExecutionStore, record: dict, resolution: dict,
                                              refs: list[dict]) -> dict:
    """Restore only the original received ID-transport failure, without publication."""
    from taskflow.execution_experiment import (
        _author_source, _native_handoff, _node, _packet, _release_quality_reports, _run_id,
    )
    from taskflow.execution_formal import _binding, _projection_data
    from taskflow.execution_runner import _info, _read, _regular
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import check_task

    definition, state = record["definition"], dict(record["state"])
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "release" or state.get("current_work_id") or not state.get("receipt")
            or not definition.get("source_author_flow_id") or state.get("pending_business_scope")
            or state.get("waiting") or state.get("release") or state.get("runs")
            or state.get("run_index", 0) != 0
            or type(resolution.get("expected_revision")) is not int
            or resolution["expected_revision"] != record["revision"]):
        raise TaskflowError("report binding resume requires the exact received unpublished initial release revision")
    source = Path(state["candidate_root"])
    task = _read(source / "task.json")
    required = [entry for entry in task["reviews"] if entry["required"]]
    previous = {"stage": "release", "reason": "quality_reports must cover exactly the required report IDs: " +
                ", ".join(entry["id"] for entry in required)}
    if state.get("exception") != previous:
        raise TaskflowError("report binding resume requires the exact required-ID release exception")
    original = evidence_ref(Path(state["receipt"]), "original report-ID release exception retained")
    supplied = validate_evidence([resolution.get("original_receipt")])[0]
    if any(supplied.get(key) != original[key] for key in ("path", "sha256")):
        raise TaskflowError("report binding original receipt differs from the explicit recovery evidence")
    received = {"flow_id": record["flow_id"], "status": "exception", "controller": definition["owner"],
                "human_read_claimed": False, "history": state["history"], "archives": state["archives"],
                "result": state.get("result"), "exception": previous,
                "substantive_next_decision": "registered controller", "experiment_validity_inferred": False}
    receipt_path = Path(definition["directory"]) / ("exception-" + sha256(canonical(received))[:16] + ".json")
    if (Path(original["path"]) != receipt_path.absolute()
            or _regular(receipt_path) != canonical(received) + b"\n"):
        raise TaskflowError("report binding original exception receipt differs from its received state")
    packet = _packet(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    if (parent["state"].get("status") != "accepted" or parent["state"].get("pending_business_scope")
            or parent["state"].get("result", {}).get("outcome") != "candidate_quality_accepted"):
        raise TaskflowError("report binding source is no longer its accepted candidate outcome")
    bound = _author_source(store, record, packet, require_candidate=True)
    if "record" in bound or canonical(bound["binding"]) != canonical(state.get("author_source_binding")):
        raise TaskflowError("report binding source author binding differs from its original intake")
    child = bound["source"]["state"]
    if (child.get("status") != "accepted" or child.get("pending_business_scope")
            or any(state.get(key) != child.get(key) for key in ("candidate_id", "candidate_root"))):
        raise TaskflowError("report binding source candidate differs from its accepted child")
    candidate = store.get_candidate(state["candidate_id"])
    actual = check_task(source, write_report=False)
    if actual["errors"] or any(actual[key] != candidate[key] for key in
                               ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("report binding current source bytes differ from the captured candidate")
    accepted = store.quality_gate(candidate["candidate_id"])
    store.assert_review_current(accepted["review_id"])
    if accepted["review_id"] != state["accepted_review_id"]:
        raise TaskflowError("report binding quality differs from the originally bound signature")
    native = _native_handoff(store, record, candidate["candidate_id"])
    if (canonical(native) != canonical(state.get("native_quality_handoff"))
            or canonical(native["source_refs"]) != canonical(state["quality_sources"])):
        raise TaskflowError("report binding original native source or raw evidence differs")
    projected = _projection_data(store, candidate, state["quality_sources"], native["protocol_map"])
    if {entry["id"] for entry in required} == set(projected):
        raise TaskflowError("report binding recovery has no original kind-to-ID transport mismatch")
    paths = {kind: Path(definition["directory"]) / "quality-projection" / (kind + ".json") for kind in projected}
    reports = _release_quality_reports(store, candidate_id=candidate["candidate_id"], source_root=source, reports=paths)
    for kind, path in paths.items():
        if _regular(path) != canonical(projected[kind]) + b"\n":
            raise TaskflowError("report binding original projected source bytes differ")
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    publication = Path(definition["technical"]["publication_root"])
    formal_root = Path(definition["technical"]["formal_runtime_root"])
    inner, outer = formal_root / run_id, Path(definition["directory"]) / "runtimes" / work_id
    paths_to_check = (publication, publication.with_name("." + publication.name + ".lock"), formal_root, inner, outer,
                      inner.with_name(inner.name + ".formal-intent.json"),
                      publication / "reports/runtime-reservations" / (run_id + ".json"))
    board = store.dashboard()
    if (any(path.exists() or path.is_symlink() for path in paths_to_check)
            or any(publication.parent.glob("." + publication.name + ".staging-*"))
            or any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
                   or row.get("runtime_path") in {str(inner), str(outer)}
                   or (row.get("candidate_id") == candidate["candidate_id"] and row.get("role") == "solver")
                   for row in [*board["work"], *board["attempts"]])
            or any(row.get("candidate_id") == candidate["candidate_id"]
                   or row.get("decision_id") == _node(record, "experiment-decision") for row in board["decisions"])):
        raise TaskflowError("report binding recovery has existing publication, staging, formal, solver or decision evidence")
    identity = {"candidate_binding": _binding(candidate), "source_root": str(source.absolute()),
                "version": packet["release"]["version_id"], "task_json": _info(source / "task.json"),
                "accepted_review_id": accepted["review_id"],
                "reports": {key: {"path": str(path.absolute()), **_info(path)} for key, path in reports.items()}}
    state["history"] = [*state["history"], {"kind": "unpublished_release_report_binding_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": previous, "previous_receipt": original,
        "publication_root": str(publication), "first_publication_identity": identity,
        "required_kind_to_id": {entry["kind"]: entry["id"] for entry in required},
        "canonical_projections": {kind: {"path": str(path.absolute()), **_info(path)} for kind, path in paths.items()},
        "previous_published_snapshot": False, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_unpublished_release_report_binding_resumed")


def _resume_validated_intake(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from collections import Counter

    from taskflow.execution_experiment import _completed, _packet, _run_id
    from taskflow.execution_review import (
        _expect, _first_inputs, _locators, _managed_projection_config, _managed_projection_context,
        _normalized_locator, _project, _source_binding, _validate_named_source_map,
    )
    from taskflow.execution_runner import (
        _full_source_roots, _full_tree, _full_verify, _info, _read, _regular,
        _validated_collection, _validated_facts_seal,
    )
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import verify_snapshot
    from taskflow.records import verify_evaluation, verify_run

    definition, state = record["definition"], dict(record["state"])
    reason = state.get("exception", {}).get("reason", "")
    source_prefix = "signed source has no unique same-byte native run artifact: "
    source_name = reason.removeprefix(source_prefix) if isinstance(reason, str) and reason.startswith(source_prefix) else None
    source_exception = bool(source_name) and state.get("exception") == {"stage": "evaluation_intake", "reason": reason}
    if source_exception and state.get("pending_business_scope"):
        raise TaskflowError("source-map intake resume cannot clear a pending business scope decision")
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "evaluation_intake" or state.get("current_work_id") or not state.get("receipt")
            or (not source_exception and state.get("exception") != {
                "stage": "evaluation_intake", "reason": "signed evidence path/location is missing"})):
        raise TaskflowError("intake resume requires the exact received signed-locator exception")
    packet = _packet(record)
    run_id = _run_id(record, packet)
    evaluation_id = (packet["independent_review"]["score_id"] if state["run_index"] == 0 else packet["single_confirmation"]["score_id"])
    destination = Path(definition["directory"]) / "evaluations" / run_id
    receipt_ref = validate_evidence([resolution.get("receipt")])[0]
    if Path(receipt_ref["path"]) != destination / "receipt.json":
        raise TaskflowError("intake resume receipt is outside the original fixed destination")
    receipt, binding = _read(Path(receipt_ref["path"])), _read(destination / "binding.json")
    completed = {}
    for label, role in (("facts", "review-facts"), ("score", "review-score")):
        saved = state[label + "_attempt"]
        prepared = state[label + "_prepared"]
        temporary = {**record, "state": {**state, "current_work_id": saved["work_id"]}}
        attempt, _ = _completed(store, temporary)
        if (attempt is None or attempt["attempt_id"] != saved["attempt_id"] or attempt["role"] != role
                or prepared["work"]["work_id"] != attempt["work_id"]):
            raise TaskflowError("intake resume does not bind the original completed facts/score attempts")
        if source_exception:
            validate_evidence(saved["evidence"])
            validate_evidence(attempt["evidence"])
            validate_evidence(prepared["work"]["evidence"])
        root = Path(attempt["runtime_path"])
        manifest, source = _source_binding(root, Path(prepared["review_binding"]))
        collection = _validated_collection(root, manifest)
        if manifest["role"] != role or collection.get("process_ended") is not True:
            raise TaskflowError("intake resume original review collection is not complete")
        completed[label] = (attempt, root, manifest, source, collection)
    facts_attempt, facts_root, facts_manifest, facts_binding, _ = completed["facts"]
    score_attempt, score_root, score_manifest, score_binding, score_collection = completed["score"]
    facts = _validated_facts_seal(facts_root)
    score_seal = _read(score_root / "first-score-seal.json")
    if (facts != state["facts_seal"]["seal"] or facts_attempt["cli_sid"] != facts["sid"]
            or score_attempt["cli_sid"] != facts["sid"] or score_manifest.get("resume_sid") != facts["sid"]
            or score_manifest.get("facts_seal") != facts or score_binding.get("facts_seal") != facts
            or score_seal.get("thread_id") != facts["sid"] or score_seal.get("facts_sha256") != facts["facts"]["sha256"]
            or score_seal.get("manifest") != _info(score_root / "manifest.json")
            or score_seal.get("collection") != _info(score_root / "collection.json")):
        raise TaskflowError("intake resume facts/score SID or original seal differs")
    score_path = score_root / "artifacts/score.json"
    if (score_seal.get("score", {}).get("path") != str(score_path)
            or _info(score_path) != score_collection["artifacts"].get("score.json")
            or any(score_seal["score"][key] != _info(score_path)[key] for key in ("sha256", "size"))):
        raise TaskflowError("intake resume score is not the exact collected sealed original")
    run_path = Path(state["imported_run"]["run_path"])
    snapshot_path = Path(state["release"]["snapshot"]["path"])
    run, snapshot = verify_run(run_path), verify_snapshot(snapshot_path)
    expected_evaluation = run_path / "evaluations" / evaluation_id
    evaluation = verify_evaluation(expected_evaluation)
    if (receipt.get("accepted") is not True or receipt.get("state") != "accepted"
            or receipt.get("receipt") != str(destination / "receipt.json") or receipt.get("run") != str(run_path)
            or receipt.get("evaluation") != str(expected_evaluation) or receipt.get("evaluation_id") != evaluation_id
            or receipt.get("record_digest") != evaluation["record_digest"] or evaluation.get("kind") != "original"
            or evaluation.get("validation_issues") or receipt.get("signer_sid") != facts["sid"]
            or receipt.get("source_sha256") != score_seal["score"]["sha256"]
            or receipt.get("facts_sha256") != facts["facts"]["sha256"]
            or any(value != run["run_digest"] for value in (receipt.get("run_digest"), binding.get("run_digest"),
                   evaluation.get("run_digest"), facts_binding.get("run_digest"), score_binding.get("run_digest"), state["imported_run"].get("run_digest")))
            or run["snapshot_digest"] != snapshot["snapshot_digest"]
            or evaluation.get("evaluation_digest") != snapshot["evaluation_digest"]
            or any(score_manifest["candidate_binding"][key] != snapshot[key] for key in
                   ("task_id", "content_digest", "input_digest", "evaluation_digest"))):
        raise TaskflowError("intake resume receipt/native evaluation differs from the original run/score/facts/version")
    expected_config = {"mode": "managed_two_stage", "run_directory": str(run_path), "snapshot_directory": str(snapshot_path),
        "evaluation_id": evaluation_id, "facts_attempt": str(facts_root), "facts_binding": state["facts_prepared"]["review_binding"],
        "score_attempt": str(score_root), "score_binding": state["score_prepared"]["review_binding"], "expected_sid": facts["sid"],
        "score_file": "score.json", "explanation_file": "explanation.md"}
    if (any(binding["config"].get(key) != value for key, value in expected_config.items())
            or binding.get("facts") != facts or binding.get("score") != score_seal["score"]):
        raise TaskflowError("intake resume accepted transport config differs from the original handoff")
    native_original = _regular(expected_evaluation / "record.original.json")
    projected = _read(destination / "evaluation.original.json")
    raw_score = _read(score_path)
    if (native_original != _regular(destination / "evaluation.original.json")
            or projected.get("source_sha256") != score_seal["score"]["sha256"]
            or projected.get("signed_original_score") != raw_score):
        raise TaskflowError("intake resume projection does not preserve the complete original signed score")
    verification = _read(destination / "evaluation-transport-verification.json")
    if source_exception:
        config = binding["config"]
        if (facts_manifest.get("resume_sid") or facts["sid"] in facts_binding["excluded_sids"]
                or score_manifest.get("scoring_projection") != score_binding.get("scoring_projection")
                or score_manifest.get("scoring_source_allowlist") != score_binding.get("scoring_source_allowlist")):
            raise TaskflowError("intake source-map recovery changed the original SID or normative scope")
        if source_name not in config.get("source_path_map", {}):
            raise TaskflowError("intake source-map recovery does not resolve the received source")
        inputs, selector = _first_inputs({"first_review_index": facts_binding["selector"]["path"]}, run)
        if (inputs != facts_binding["first_review_inputs"] or inputs != score_binding["first_review_inputs"]
                or selector != facts_binding["selector"] or selector != score_binding["selector"]
                or facts_binding["public_sources"] != score_binding["public_sources"]):
            raise TaskflowError("intake source-map recovery changed the original first-stage scope")
        rows = _read(Path(selector["path"]))["allowed_direct_artifacts"]
        declaration = _read(snapshot_path / "task.json")
        _validate_named_source_map(config, raw_score, facts_manifest, score_manifest,
                                   score_binding, declaration, rows, run, verification)
        context = _managed_projection_context(score_root, score_manifest, score_binding, facts, score_seal, rows)
        replay_config = _managed_projection_config(config, facts["sid"], score_manifest, run)
        replay, locators = _project(raw_score, context, replay_config, run, snapshot)
        if (verification.get("bindings") != locators or any(projected.get(key) != value for key, value in replay.items())
                or receipt.get("score") != replay["total"]):
            raise TaskflowError("intake source-map projection differs from the accepted signed original")
        originals = [path for path in (run_path / "evaluations").glob("*/evaluation.json")
                     if verify_evaluation(path.parent)["kind"] == "original"]
        if originals != [expected_evaluation / "evaluation.json"]:
            raise TaskflowError("intake source-map recovery requires one unique native original evaluation")
        archives = binding.get("archives")
        if (not isinstance(archives, dict) or archives.get("path") != str(destination / "full-archive")
                or receipt.get("review_archive") != archives or projected.get("review_archive") != archives):
            raise TaskflowError("intake source-map recovery lacks its exact full archive binding")
        for label, root in (("facts", facts_root), ("score", score_root)):
            archive = destination / "full-archive" / label
            if archives[label].get("path") != str(archive) or archives[label].get("archived") is not True:
                raise TaskflowError("intake source-map recovery archive is outside the fixed destination")
            tree, archived = _full_tree(root), _full_tree(archive)
            files = {name: {key: item[key] for key in ("sha256", "size")} for name, item in tree["files"].items()}
            received = {name: {key: item[key] for key in ("sha256", "size")}
                        for name, item in archived["files"].items() if name != "archive-binding.json"}
            archive_binding = _read(archive / "archive-binding.json")
            directories = sorted(name.rstrip("/") for name in tree["directories"] if name != ".")
            if (files != received or tree["special"] or archived["special"]
                    or set(tree["directories"]) != set(archived["directories"])
                    or archive_binding != {"intent_id": completed[label][2]["intent_id"],
                        "collection": _info(root / "collection.json"), "files": files, "directories": directories}):
                raise TaskflowError("intake source-map recovery full archive membership or bytes changed")
            intent = _read(root / "supplemental-intent.json")
            roots, aliases = _full_source_roots(completed[label][2])
            if (intent["snapshot"] != {key: _full_tree(path) for key, path in roots.items()}
                    or intent["sources"] != {key: str(path) for key, path in roots.items()} or intent["aliases"] != aliases):
                raise TaskflowError("intake source-map recovery original full source scope changed")
            full = _full_verify(archive / "supplemental", intent, True)
            expected_archive = {"archived": True, "path": str(archive), **archive_binding,
                "supplemental": str(root / "supplemental"),
                "supplemental_receipt": _info(root / "supplemental/receipt.json"),
                "snapshot_digest": full["snapshot_digest"], "source_roots": intent["sources"],
                "regular_files_captured": sum(len(tree["files"]) for tree in intent["snapshot"].values()),
                "special_entries_indexed": sum(len(tree["special"]) for tree in intent["snapshot"].values()),
                "original_sources_retained": True, "automatic_model_retry": False, "scope": intent["scope"]}
            if archives[label] != expected_archive:
                raise TaskflowError("intake source-map recovery full archive snapshot differs")
        supporting = {"facts": {"path": str(destination / "full-archive/facts/artifacts" / facts["output_name"]), **facts["facts"]},
            "first_score": {"path": str(destination / "full-archive/score/artifacts/score.json"), **_info(score_path)},
            "scoring_review": {"path": str(destination / "full-archive/score/artifacts/explanation.md"),
                               **_info(score_root / "artifacts/explanation.md")},
            "seal": {"path": str(destination / "full-archive/score/first-score-seal.json"),
                     **_info(score_root / "first-score-seal.json")}}
        if projected.get("supporting_review") != supporting:
            raise TaskflowError("intake source-map recovery supporting originals differ")
        for item in supporting.values():
            _expect(Path(item["path"]), item)
    else:
        typed = [(path, canonical(locator)) for row in raw_score.get("criteria", raw_score.get("items", []))
                 for path, locator in _locators(row, raw_score) if isinstance(locator, dict)]
        actual = [row for row in verification.get("bindings", []) if isinstance(row.get("signed_locator"), dict)]
        if (not typed or verification.get("all_signed_original_reasons_preserved") is not True
                or verification.get("root_business_rereview") is not False
                or Counter(typed) != Counter((row.get("signed_path"), canonical(row["signed_locator"])) for row in actual)
                or any(row.get("projected_locator") != _normalized_locator(row["signed_locator"]) for row in actual)):
            raise TaskflowError("intake resume has no exact traceable typed-locator projection")
    prior = evidence_ref(Path(state["receipt"]), "original signed-locator intake exception preserved")
    original_exception = _read(Path(prior["path"]))
    if (original_exception.get("flow_id") != record["flow_id"] or original_exception.get("status") != "exception"
            or original_exception.get("exception") != state["exception"]):
        raise TaskflowError("intake resume original exception receipt differs")
    if source_exception:
        expected_exception = {"flow_id": record["flow_id"], "status": "exception", "controller": definition["owner"],
            "human_read_claimed": False, "history": state["history"], "archives": state["archives"],
            "result": state.get("result"), "exception": state["exception"],
            "substantive_next_decision": "registered controller", "experiment_validity_inferred": False}
        expected_path = Path(definition["directory"]) / ("exception-" + sha256(canonical(expected_exception))[:16] + ".json")
        if original_exception != expected_exception or Path(prior["path"]) != expected_path:
            raise TaskflowError("intake source-map recovery does not preserve the exact received exception")
    state["history"] = [*state["history"], {"kind": "validated_evaluation_intake_resolution", "resolution": resolution,
        "evidence": refs, "previous_exception": state["exception"], "previous_receipt": prior,
        "accepted_intake_receipt": receipt_ref, "source_sha256": score_seal["score"]["sha256"], "sid": facts["sid"],
        "transport_verification": evidence_ref(destination / "evaluation-transport-verification.json",
            "existing strict named-source transport" if source_exception else "existing strict typed-locator transport"),
        "native_evaluation": evidence_ref(expected_evaluation / "evaluation.json", "existing original native evaluation"),
        "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_validated_evaluation_intake_resumed")



@_handoff_errors
def _confirmation_first_run_footprint(store: ExecutionStore, record: dict, packet: dict, candidate: dict) -> dict:
    """Read the one accepted first run that may precede a zero-call confirmation."""
    from taskflow.execution import _check_work_binding
    from taskflow.execution_experiment import _confirmation_limit, _local_configuration_reasons, _node
    from taskflow.execution_review import _local_assessment, _managed_projection_context, _source_binding, _time
    from taskflow.execution_runner import _info, _read, _regular, _trace, _validate, _validated_collection, _validated_facts_seal
    from taskflow.execution_store import evidence_ref
    from taskflow.execution_teacher_successor import _archive
    from taskflow.packages import verify_snapshot
    from taskflow.records import verify_evaluation, verify_run

    definition, state = record["definition"], record["state"]
    rows = state.get("runs")
    if (type(state.get("run_index")) is not int or state["run_index"] != 1
            or state.get("confirmation_authorized") is not True
            or state.get("confirmation_basis", {}).get("eligible") is not True
            or type(packet.get("single_confirmation", {}).get("max_additional_attempts")) is not int
            or packet["single_confirmation"]["max_additional_attempts"] != 1
            or not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)):
        raise TaskflowError("confirmation recovery requires its one authorized accepted original run")
    original = rows[0]
    first = {**record, "state": {**state, "run_index": 0}}
    work_id = _node(first, "solver")
    work = store.get_work(work_id)
    attempts = store.list_attempts(work_id)
    if (len(attempts) != 1 or attempts[0].get("attempt_id") != original.get("solver_attempt_id")
            or not isinstance(original.get("solver_sid"), str) or not original["solver_sid"]):
        raise TaskflowError("confirmation recovery has no unique actual first solver attempt")
    attempt = attempts[0]
    runtime = Path(definition["directory"]) / "runtimes" / work_id
    expected = {"work_id": work_id, "intent_id": work_id + "-intent", "task_id": candidate["task_id"],
        "candidate_id": candidate["candidate_id"], "role": "solver", "policy_id": definition["policy_id"],
        "dispatch_owner": definition["owner"], "runtime_path": str(runtime), "state": "completed"}
    if (any(row.get(key) != value for row in (work, attempt) for key, value in expected.items())
            or attempt.get("cli_sid") != original["solver_sid"]
            or work.get("purpose") != packet["business_investment"]["unresolved_business_question"]
            or attempt.get("purpose") != work.get("purpose")):
        raise TaskflowError("confirmation recovery first solver identity, owner, purpose or completion differs")
    manifest = _validate(runtime)
    collected = _validated_collection(runtime, manifest)
    _check_work_binding(attempt, candidate, manifest)
    trace = _trace(runtime / "trace.jsonl", original["solver_sid"])
    if (any(collected.get(key) is not True for key in ("process_ended", "technical_accepted", "delivery_complete", "model_completed"))
            or collected.get("sid") != original["solver_sid"] or trace.get("model_completed") is not True
            or any(row.get("request_digest") != manifest["request"]["sha256"] for row in (work, attempt))
            or any(manifest["model"].get("requested" if key == "model" else key) != definition["technical"]["formal_config_base"][key]
                   for key in ("model", "reasoning_effort") if key in definition["technical"]["formal_config_base"])
            or manifest["candidate_binding"] != {key: candidate[key] for key in
                ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}):
        raise TaskflowError("confirmation recovery first solver lacks its complete sealed original delivery")
    formal_archive, _, _ = _archive(state["formal_archive"], "solver")
    if (formal_archive != Path(definition["directory"]) / "archives" / attempt["attempt_id"]
            or any(_info(runtime / name) != _info(formal_archive / name)
                   for name in ("manifest.json", "manifest-lock.json", "collection.json", "collection-lock.json"))):
        raise TaskflowError("confirmation recovery first solver differs from its full sealed original archive")
    decision_id = _node(first, "experiment-decision")
    decision = store.get_decision(decision_id)
    if (work.get("experiment_decision_id") != decision_id or attempt.get("experiment_decision_id") != decision_id
            or decision.get("candidate_id") != candidate["candidate_id"] or decision.get("task_id") != candidate["task_id"]
            or decision.get("action") != "experiment" or decision.get("authorized") is not True
            or decision.get("experiment_purpose") != work["purpose"]
            or validate_evidence(decision.get("evidence")) != validate_evidence([definition["authorization"]])):
        raise TaskflowError("confirmation recovery first run differs from its exact original authorized decision")
    first_run = validate_id(packet["formal"]["first_run_id"], "first run_id")
    destination = Path(definition["directory"]) / "evaluations" / first_run
    receipt_path = destination / "receipt.json"
    receipt, binding = _read(receipt_path), _read(destination / "binding.json")
    if (canonical({key: value for key, value in original.items() if key not in {"solver_attempt_id", "solver_sid"}}) != canonical(receipt)
            or receipt.get("accepted") is not True or receipt.get("state") != "accepted"
            or receipt.get("receipt") != str(receipt_path) or receipt.get("run") != state["imported_run"]["run_path"]):
        raise TaskflowError("confirmation recovery original row differs from its fixed accepted intake receipt")
    run_path = Path(receipt["run"])
    run, snapshot = verify_run(run_path), verify_snapshot(Path(state["release"]["snapshot"]["path"]))
    evaluation_path = run_path / "evaluations" / packet["independent_review"]["score_id"]
    evaluation = verify_evaluation(evaluation_path)
    if (run.get("run_id") != first_run or run.get("task_id") != candidate["task_id"]
            or receipt.get("evaluation") != str(evaluation_path) or receipt.get("evaluation_id") != evaluation["evaluation_id"]
            or evaluation.get("kind") != "original" or evaluation.get("validation_issues")
            or receipt.get("record_digest") != evaluation["record_digest"]
            or binding["config"].get("run_directory") != str(run_path)
            or binding["config"].get("snapshot_directory") != state["release"]["snapshot"]["path"]
            or binding["config"].get("evaluation_id") != packet["independent_review"]["score_id"]
            or validate_evidence(binding["config"].get("evidence")) != validate_evidence([definition["authorization"]])
            or any(value != run["run_digest"] for value in (receipt.get("run_digest"), binding.get("run_digest"), evaluation.get("run_digest")))
            or run.get("snapshot_digest") != snapshot["snapshot_digest"]
            or evaluation.get("evaluation_digest") != candidate["evaluation_digest"]
            or any(snapshot.get(key) != candidate[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest"))):
        raise TaskflowError("confirmation recovery original run/evaluation/frozen version differs")
    actual_trace = _info(runtime / "trace.jsonl")
    if not any(all(row.get(key) == value for key, value in actual_trace.items()) for row in run.get("artifacts", [])):
        raise TaskflowError("confirmation recovery native run lacks the actual first solver trace")
    phases = {}
    for label, role in (("facts", "review-facts"), ("score", "review-score")):
        saved, prepared = state[label + "_attempt"], state[label + "_prepared"]
        actual = store.get_attempt(saved["attempt_id"])
        roots = Path(actual["runtime_path"])
        part, source = _source_binding(roots, Path(prepared["review_binding"]))
        collection = _validated_collection(roots, part)
        _check_work_binding(actual, candidate, part)
        review_work = store.get_work(actual["work_id"])
        archive = receipt["review_archive"][label]
        archived_root, archived_manifest, _ = _archive(archive, role)
        if (actual.get("state") != "completed" or actual.get("role") != role or actual.get("cli_sid") != receipt.get("signer_sid")
                or actual.get("candidate_id") != candidate["candidate_id"] or actual.get("policy_id") != definition["policy_id"]
                or saved.get("work_id") != actual.get("work_id") or actual.get("work_id") != _node(first, role)
                or prepared.get("work", {}).get("work_id") != actual["work_id"]
                or any(actual.get(key) != review_work.get(key) for key in
                       ("work_id", "intent_id", "task_id", "candidate_id", "role", "policy_id", "dispatch_owner", "runtime_path", "request_digest"))
                or review_work.get("state") != "completed" or actual.get("dispatch_owner") != definition["owner"]
                or actual.get("intent_id") != actual["work_id"] + "-intent"
                or actual.get("request_digest") != part["request"]["sha256"]
                or roots != Path(binding["config"][label + "_attempt"])
                or _info(roots / "manifest.json") != _info(archived_root / "manifest.json")
                or _info(roots / "collection.json") != _info(archived_root / "collection.json")
                or source.get("run_digest") != run["run_digest"] or part["candidate_binding"] != manifest["candidate_binding"]
                or any(collection.get(key) is not True for key in ("process_ended", "technical_accepted", "delivery_complete", "model_completed"))
                or _trace(roots / "trace.jsonl", receipt["signer_sid"]).get("model_completed") is not True
                or Path(archive["path"]) != destination / "full-archive" / label):
            raise TaskflowError("confirmation recovery original facts/score managed source or sealed archive differs")
        seal_name = "facts-seal.json" if label == "facts" else "first-score-seal.json"
        if _info(roots / seal_name) != _info(archived_root / seal_name):
            raise TaskflowError("confirmation recovery original review seal bytes differ from the archive")
        phases[label] = (roots, part, source, collection)
    facts_root, facts_manifest, facts_binding, _ = phases["facts"]
    score_root, score_manifest, score_binding, score_collection = phases["score"]
    facts = _validated_facts_seal(facts_root)
    score_seal = _read(score_root / "first-score-seal.json")
    raw_score = _read(Path(score_seal["score"]["path"]))
    native = _read(evaluation_path / "record.original.json")
    if (facts.get("sid") != receipt["signer_sid"] or score_manifest.get("resume_sid") != facts["sid"]
            or facts["sid"] == original["solver_sid"] or facts["sid"] in facts_binding.get("excluded_sids", [])
            or facts_manifest.get("resume_sid")
            or score_seal.get("thread_id") != facts["sid"] or score_seal.get("facts_sha256") != receipt["facts_sha256"]
            or facts["facts"]["sha256"] != receipt["facts_sha256"] or binding.get("facts") != facts
            or binding.get("score") != score_seal["score"] or score_manifest.get("facts_seal") != facts
            or score_binding.get("facts_seal") != facts or score_manifest.get("facts_attempt") != str(facts_root)
            or score_seal.get("manifest") != _info(score_root / "manifest.json")
            or score_seal.get("collection") != _info(score_root / "collection.json")
            or _info(Path(score_seal["score"]["path"])) != score_collection["artifacts"].get(binding["config"].get("score_file", "first-score.json"))
            or score_seal["score"]["sha256"] != receipt["source_sha256"]
            or _regular(destination / "evaluation.original.json") != _regular(evaluation_path / "record.original.json")
            or native.get("signed_original_score") != raw_score or native.get("source_sha256") != receipt["source_sha256"]
            or native.get("total") != receipt["score"]
            or not (_time(facts["at"]) <= _time(score_manifest["created_at"]) <= _time(score_collection["termination"]["started_at"]))):
        raise TaskflowError("confirmation recovery original same-SID facts/score seals or signed evaluation differs")
    rows = _read(Path(facts_binding["selector"]["path"]))["allowed_direct_artifacts"]
    context = _managed_projection_context(score_root, score_manifest,
        {**score_binding, "selector": facts_binding["selector"]}, facts, score_seal, rows)
    if _local_assessment(raw_score, context, run, snapshot) != receipt.get("local_profile_assessment"):
        raise TaskflowError("confirmation recovery original local eligibility no longer matches its sealed signed evidence")
    maximum = _confirmation_limit(packet, _read(Path(state["release"]["snapshot"]["path"]) / "task.json")["scoring"]["total"])
    if (maximum is None or receipt["score"] > maximum or run.get("completion") != "completed"
            or run.get("input_binding") != "matched" or _local_configuration_reasons(record, receipt, run)):
        raise TaskflowError("confirmation recovery original does not satisfy its existing one-confirmation authorization")
    return {"work_id": work_id, "attempt_id": attempt["attempt_id"], "sid": attempt["cli_sid"],
            "receipt": evidence_ref(receipt_path, "validated first original intake before this confirmation"),
            "manifest": evidence_ref(runtime / "manifest.json", "exact completed first solver manifest"),
            "collection": evidence_ref(runtime / "collection.json", "exact completed first solver collection"),
            "run_digest": run["run_digest"], "record_digest": evaluation["record_digest"]}


def _resume_validated_host_preparation(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict], *,
                                       public_probe: bool = False) -> dict:
    from taskflow.execution_experiment import _native_handoff, _node, _packet, _run_id
    from taskflow.execution_formal import _checked_ref, validate_formal_preparation_recovery
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref
    from taskflow.packages import check_task, verify_snapshot

    definition, state = record["definition"], dict(record["state"])
    exception = state.get("exception", {})
    if public_probe and (state.get("pending_business_scope") is not None
                         or state.get("formal_preparation_recovery") is not None):
        raise TaskflowError("public-probe preparation resume cannot clear a business hold or replace a recovery")
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "formal_prepare" or state.get("current_work_id") or not state.get("receipt")
            or exception.get("stage") != "formal_prepare" or not isinstance(exception.get("reason"), str)
            or not exception["reason"].startswith("existing formal preparation failed; original runtime/reservation retained: ")):
        raise TaskflowError("host preparation resume requires the received original partial-preparation exception")
    packet = _packet(record)
    run_id, work_id = validate_id(_run_id(record, packet), "run_id"), _node(record, "solver")
    candidate = store.get_candidate(state["candidate_id"])
    inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    first_footprint = (_confirmation_first_run_footprint(store, record, packet, candidate)
                       if public_probe and state.get("run_index") == 1 else None)
    if public_probe:
        import json

        with store._db() as db:
            dashboard = {table: [json.loads(row["data"]) for row in db.execute(
                "SELECT data FROM " + table + (" WHERE id<>?" if first_footprint else ""),
                (first_footprint["work_id" if table == "work" else "attempt_id"],) if first_footprint else ())]
                         for table in ("work", "attempts")}
    else:
        dashboard = store.dashboard()
    if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
            or row.get("runtime_path") in (str(inner), str(outer))
            or (row.get("role") == "solver" and row.get("candidate_id") == candidate["candidate_id"])
            for row in [*dashboard["work"], *dashboard["attempts"]]) or outer.exists() or outer.is_symlink()):
        raise TaskflowError("host preparation has a related work/attempt/outer runtime; no duplicate dispatch")
    markers = ("execution-claim.json", "execution-attempt.json", "initial-input-binding.json", "real-run",
               "launch.json", "dispatch.json", "process.json", "termination.json", "collection.json", "trace.jsonl")
    if any((inner / name).exists() or (inner / name).is_symlink() for name in markers):
        raise TaskflowError("host preparation already has execution evidence; original call cannot be retried")
    release = state["release"]
    binding = {key: candidate[key] for key in ("candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")}
    source = check_task(Path(state["candidate_root"]), write_report=False)
    frozen = verify_snapshot(Path(release["snapshot"]["path"]))
    if (candidate["task_id"] != definition["task_id"] or release["candidate_binding"] != binding or source["errors"]
            or any(source[key] != candidate[key] or frozen[key] != candidate[key] for key in
                   ("task_id", "content_digest", "input_digest", "evaluation_digest"))):
        raise TaskflowError("host preparation source/release/candidate version differs")
    accepted = store.quality_gate(candidate["candidate_id"])
    store.assert_review_current(state["accepted_review_id"])
    if accepted["review_id"] != state["accepted_review_id"]:
        raise TaskflowError("host preparation current independent quality differs")
    if definition.get("source_author_flow_id"):
        _native_handoff(store, record, candidate["candidate_id"])
    for reference in [release["receipt"], *release["quality_evidence"]]:
        _checked_ref(reference)
    locked = _read(Path(release["receipt"]["path"])).get("binding", {})
    if (locked.get("candidate_binding") != binding or locked.get("accepted_review_id") != state["accepted_review_id"]
            or locked.get("source_root") != state["candidate_root"] or locked.get("version") != packet["release"]["version_id"]):
        raise TaskflowError("host preparation release receipt differs from its original accepted source")
    for reference in locked["reports"].values():
        _checked_ref(reference)
    config = {**definition["technical"]["formal_config_base"], "run_id": run_id,
              "intent_id": work_id + "-intent", "policy_version": definition["policy_id"]}
    if definition.get("source_author_flow_id"):
        config["authorization_item"] = definition["business_packet_item"]
    identity = {"config_digest": sha256(canonical(config)), "release_receipt": release["receipt"],
                "candidate_binding": binding, "runtime": str(inner)}
    intent = inner.with_name(inner.name + ".formal-intent.json")
    if _read(intent) != identity:
        raise TaskflowError("host preparation intent/config digest differs from the original run")
    recovery_path, _ = _checked_ref(resolution["preparation"])
    if public_probe:
        from taskflow.execution_formal import validate_formal_public_probe_recovery

        validator = validate_formal_public_probe_recovery
    else:
        validator = validate_formal_preparation_recovery
    proof = validator(config=config, release=release, candidate=candidate,
                      runtime=inner, recovery=resolution["preparation"])
    if (proof.get("binding") != identity or proof.get("run_id") != run_id or proof.get("runtime") != str(inner)
            or type(proof.get("model_calls")) is not int or proof["model_calls"] != 0):
        raise TaskflowError("host recovery proof does not bind this exact zero-model preparation")
    for name in ("plan", "acceptance", "reservation", "intent", "recovery_process"):
        _checked_ref(proof[name])
    if public_probe:
        source_change = proof.get("source_change", {})
        if (type(proof.get("schema_version")) is not int or proof["schema_version"] != 1
                or proof.get("kind") != "validated_zero_inference_public_probe_source_recovery"
                or not isinstance(source_change, dict)
                or source_change.get("kind") != "deterministic_public_probe_target_v1"
                or any(not isinstance(source_change.get(name), dict) for name in ("original", "replacement", "executed"))
                or not isinstance(proof.get("recovery"), dict)
                or any(proof["recovery"].get(key) != resolution["preparation"].get(key) for key in ("path", "sha256"))):
            raise TaskflowError("public-probe preparation requires its explicit typed source recovery proof")
        for name in ("original", "replacement", "executed"):
            _checked_ref(source_change[name])
    prior = evidence_ref(Path(state["receipt"]), "original failed host preparation exception preserved")
    received = _read(Path(prior["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != exception):
        raise TaskflowError("host preparation original exception receipt differs")
    history_kind = "validated_public_probe_formal_preparation_resolution" if public_probe else "validated_host_formal_preparation_resolution"
    state["history"] = [*state["history"], {"kind": history_kind, "resolution": resolution,
        "evidence": refs, "previous_exception": exception, "previous_receipt": prior,
        "preparation": evidence_ref(recovery_path, "validated same-run zero-model host preparation"),
        "preparation_validation": proof, "work_id": work_id, "run_id": run_id, "new_model_dispatched": False}]
    if first_footprint is not None:
        state["history"][-1]["completed_first_run_exclusion"] = first_footprint
    if public_probe:
        state["formal_preparation_recovery"] = dict(resolution["preparation"])
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    event = "lifecycle_validated_public_probe_preparation_resumed" if public_probe else "lifecycle_validated_host_formal_preparation_resumed"
    return _save(store, record, state, event)


def _resume_validated_composition(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from taskflow.execution_experiment import _author_source, _node, _packet, _run_id
    from taskflow.execution_runner import _read
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    if state.get("pending_business_scope"):
        raise TaskflowError("composition resume cannot clear a pending business scope decision")
    exception = state.get("exception", {})
    prefix = "Fresh recheck has no explicit current SHA evidence for changed path: "
    if (definition.get("kind") != "experiment_v1" or state.get("status") != "exception"
            or state.get("stage") != "experiment_v1" or state.get("current_work_id") or not state.get("receipt")
            or not definition.get("source_author_flow_id") or exception.get("stage") != "experiment_v1"
            or not isinstance(exception.get("reason"), str) or not exception["reason"].startswith(prefix)):
        raise TaskflowError("composition resume requires the exact received source changed-evidence exception")
    missing_path = relative_path(exception["reason"][len(prefix):])
    packet = _packet(record)
    author = store.get_flow(definition["source_author_flow_id"])
    if (author["state"].get("status") != "accepted" or not author["state"].get("receipt")
            or author["state"].get("result", {}).get("outcome") != "candidate_quality_accepted"):
        raise TaskflowError("composition source has not received its exact accepted author-quality result")
    bound = _author_source(store, record, packet)
    if "record" in bound:
        raise TaskflowError("composition source is not fully accepted")
    source = bound["source"]
    candidate_id = source["state"]["candidate_id"]
    accepted = store.quality_gate(candidate_id)
    review_id = validate_id(resolution.get("composite_review_id"), "composite_review_id")
    supplement = resolution.get("supplement")
    graph = accepted.get("composite_quality_coverage", {})
    current_recheck = store.get_review(source["state"]["accepted_review_id"])
    history = {row.get("review_id") for row in source["state"].get("history", [])}
    if (not isinstance(supplement, dict) or accepted.get("review_id") != review_id or accepted.get("derived") is not True
            or accepted.get("whole_package") is not False or accepted.get("snapshot_bound_acceptance") is not True
            or graph.get("recheck_supplement", {}).get("definition") != supplement
            or supplement.get("base_recheck_id") != current_recheck["review_id"]
            or graph.get("recheck_id") != current_recheck["review_id"] or graph.get("whole_review_id") not in history
            or graph.get("whole_source", {}).get("candidate_id") != bound["initial"]["candidate_id"]
            or set(graph.get("closed_defect_ids", [])) != set(current_recheck.get("covered_defect_ids", []))
            or not any(row.get("path") == missing_path and row.get("fresh_signed_evidence") for row in graph.get("private_changes", []))):
        raise TaskflowError("composition resume differs from the exact validated supplemented source coverage")
    store.assert_review_current(review_id)
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    dashboard = store.dashboard()
    if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
            or row.get("runtime_path") in (str(inner), str(outer))
            or (row.get("role") == "solver" and row.get("candidate_id") == candidate_id)
            for row in [*dashboard["work"], *dashboard["attempts"]])
            or any(path.exists() or path.is_symlink() for path in (inner, outer, inner.with_name(inner.name + ".formal-intent.json")))):
        raise TaskflowError("composition resume has an existing solver work/attempt/runtime/intent")
    prior = evidence_ref(Path(state["receipt"]), "original source composition exception preserved")
    received = _read(Path(prior["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != exception):
        raise TaskflowError("composition original exception receipt differs")
    state["history"] = [*state["history"], {"kind": "validated_quality_composition_resolution", "resolution": resolution,
        "evidence": refs, "previous_exception": exception, "previous_receipt": prior,
        "composite_review_id": review_id, "supplement": supplement,
        "source_flow_id": source["flow_id"], "source_revision": source["revision"], "new_model_dispatched": False}]
    state.update(status="active", waiting=None, quality_recheck_supplement=supplement,
                 quality_recheck_supplement_review_id=review_id)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_validated_quality_composition_resumed")


def _resume_validated_score_preparation(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from taskflow.execution import _check_work_binding
    from taskflow.execution_experiment import _completed, _native_handoff, _native_normative, _node, _packet, _run_id
    from taskflow.execution_review import _review_run, _source_binding
    from taskflow.execution_runner import _info, _read, _validated_facts_seal
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    if (definition.get("kind") != "experiment_v1" or not definition.get("source_author_flow_id")
            or state.get("status") != "exception" or state.get("stage") != "score_prepare"
            or state.get("current_work_id") or not state.get("receipt")
            or state.get("exception") != {"stage": "score_prepare", "reason":
                "scoring allowlist must contain this candidate's normative answer/rubric sources"}):
        raise TaskflowError("score preparation resume requires the exact received normative-selection exception")
    packet = _packet(record)
    run_id, work_id = _run_id(record, packet), _node(record, "review-score")
    runtime = Path(definition["technical"]["formal_runtime_root"]) / (".review-" + run_id) / "score/runtime"
    binding_path = runtime.parent / (runtime.name + "-review") / "review-binding.json"
    prepared_ref = validate_evidence([resolution.get("preparation")])[0]
    prepared = _read(Path(prepared_ref["path"]))
    work = store.get_work(work_id)
    facts_saved = state["facts_attempt"]
    temporary = {**record, "state": {**state, "current_work_id": facts_saved["work_id"]}}
    facts_attempt, _ = _completed(store, temporary)
    if (facts_attempt is None or facts_attempt["attempt_id"] != facts_saved["attempt_id"]
            or facts_attempt["role"] != "review-facts"
            or state["facts_prepared"]["work"]["work_id"] != facts_attempt["work_id"]):
        raise TaskflowError("score preparation resume needs the original completed factual attempt")
    facts_root = Path(facts_attempt["runtime_path"])
    facts_manifest, facts_binding = _source_binding(facts_root, Path(state["facts_prepared"]["review_binding"]))
    facts = _validated_facts_seal(facts_root)
    if (facts != state["facts_seal"]["seal"] or facts["sid"] != facts_attempt["cli_sid"]
            or facts["candidate_binding"] != facts_manifest["candidate_binding"]
            or facts_manifest.get("resume_sid") or facts["sid"] in state.get("excluded_sids", [])):
        raise TaskflowError("score preparation resume factual SID/seal/version differs")
    attempts = store.dashboard()["attempts"]
    if any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
           or row.get("runtime_path") == str(runtime)
           or (row.get("role") == "review-score" and row.get("cli_sid") == facts["sid"])
           for row in attempts):
        raise TaskflowError("score attempt already exists; the original SID cannot be dispatched twice")
    markers = ("dispatch.json", "process.json", "launcher.json", "supervisor.json", "trace.jsonl",
               "termination.json", "collection.json", "collection-lock.json", "first-score-seal.json")
    if any((runtime / name).exists() or (runtime / name).is_symlink() for name in markers):
        raise TaskflowError("score launch or collection evidence already exists; no preparation resume")
    if (prepared.get("runtime") != str(runtime) or prepared.get("review_binding") != str(binding_path)
            or prepared.get("work") != work or work["state"] != "ready"
            or work["role"] != "review-score" or work["intent_id"] != work_id + "-intent"
            or work["runtime_path"] != str(runtime) or work["dependencies"] != [facts_attempt["work_id"]]
            or work.get("dispatch_owner") != definition["owner"]
            or work["candidate_id"] != state["candidate_id"] or work["policy_id"] != definition["policy_id"]):
        raise TaskflowError("score preparation proof differs from the exact original unclaimed work")
    run, snapshot, candidate = _review_run({"run_directory": state["imported_run"]["run_path"], "work": work}, store)
    native = _native_handoff(store, record, candidate["candidate_id"])
    selected = _native_normative(Path(run["snapshot_path"]), _read(Path(run["snapshot_path"]) / "task.json"), native)
    manifest, binding = _source_binding(runtime, binding_path)
    _check_work_binding(work, candidate, manifest)
    certificate = manifest.get("scoring_projection", {}).get("normative_certification")
    if (not isinstance(certificate, dict) or certificate.get("native_quality_handoff") != native
            or certificate.get("source_author_flow_id") != definition["source_author_flow_id"]
            or certificate.get("authorization") != definition["authorization"]
            or binding.get("scoring_projection") != manifest.get("scoring_projection")
            or binding.get("facts_seal") != facts or manifest.get("facts_seal") != facts
            or manifest.get("resume_sid") != facts["sid"] or manifest.get("facts_attempt") != str(facts_root)
            or binding.get("first_review_inputs") != facts_binding["first_review_inputs"]
            or binding.get("scoring_source_allowlist") != sorted(selected)
            or manifest.get("scoring_source_allowlist") != sorted(selected)
            or prepared.get("run_digest") != run["run_digest"]
            or facts_binding["run_digest"] != run["run_digest"]
            or snapshot["snapshot_digest"] != run["snapshot_digest"]
            or str(Path(run["snapshot_path"])) != state["release"]["snapshot"]["path"]):
        raise TaskflowError("score preparation proof changed the original facts/run or certified normative selection")
    if run["nature"] != "test_fixture" and (manifest.get("mode") != "native_role"
            or manifest.get("nature") != "real_cli_attempt" or facts_manifest.get("mode") != "native_role"):
        raise TaskflowError("real scoring preparation requires its original native factual context")
    proof_path = runtime / "review-boundary-probe.json"
    proof = _read(proof_path)
    validate_evidence(work["evidence"])
    probe_ref = evidence_ref(proof_path, "original zero-model scoring boundary proof")
    if proof.get("manifest") != _info(runtime / "manifest.json") or proof.get("model_calls") != 0 or proof.get("returncode") != 0:
        raise TaskflowError("score preparation lacks its exact zero-model boundary proof")
    if not any(all(item.get(key) == probe_ref[key] for key in ("path", "sha256")) for item in work["evidence"]):
        raise TaskflowError("score boundary proof was not bound before the original work was enqueued")
    prior = evidence_ref(Path(state["receipt"]), "original normative-selection preparation exception preserved")
    original = _read(Path(prior["path"]))
    if (original.get("flow_id") != record["flow_id"] or original.get("status") != "exception"
            or original.get("exception") != state["exception"]):
        raise TaskflowError("score preparation original exception receipt differs")
    state["history"] = [*state["history"], {"kind": "validated_score_preparation_resolution", "resolution": resolution,
        "evidence": refs, "previous_exception": state["exception"], "previous_receipt": prior,
        "preparation": prepared_ref, "work_id": work_id, "facts_attempt_id": facts_attempt["attempt_id"],
        "facts_sid": facts["sid"], "facts_sha256": facts["facts"]["sha256"],
        "normative_certification": certificate, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_validated_score_preparation_resumed")


def _declaration_transport_reviews(material: dict, task_id: str) -> None:
    from taskflow.execution_author import _candidate_structure
    from taskflow.execution_runner import _read, _regular

    projection = material["declaration_transport"]
    target, source = Path(material["candidate_root"]), Path(projection["source_candidate_root"])
    _candidate_structure(target, task_id)
    kind = projection["definition"]["kind"]
    if kind == "referenced_rubric_role_v1":
        for row in _read(target / "task.json").get("reviews", []):
            path = relative_path(row["path"])
            if _regular(source / path) != _regular(target / path):
                raise TaskflowError("declaration transport cannot change unsigned author review metadata")
    elif kind == "declared_unsigned_report_layout_v1":
        reports = projection.get("projection", {}).get("unsigned_reports")
        if not isinstance(reports, list) or not reports or any(row.get("original_json_preserved") is not True for row in reports):
            raise TaskflowError("layout transport lacks its validated original unsigned report preservation")
        validate_evidence([row[key] for row in reports for key in ("original", "relocated", "wrapper")])
    else:
        raise TaskflowError("unsupported explicit candidate declaration transport")


def _unsigned_candidate_alias(material: dict, target: Path, task_id: str) -> dict:
    from taskflow.execution_author import _candidate_structure
    from taskflow.execution_runner import _info, _read, _regular
    from taskflow.execution_store import evidence_ref

    if not target.is_absolute():
        raise TaskflowError("unsigned candidate alias must be an absolute source-bound path")
    declaration = _read(target / "task.json")
    declared = {"task.json": "source_metadata", **{row["path"]: row["role"] for row in declaration["files"]}}
    expected = {row["package_path"]: row for row in material["file_refs"]}
    if set(declared) != set(expected):
        raise TaskflowError("unsigned candidate alias changes the original declaration membership")
    for path, role in declared.items():
        actual = _info(target / relative_path(path))
        if role != expected[path]["role"] or actual != {key: expected[path][key] for key in ("sha256", "size")}:
            raise TaskflowError("unsigned candidate alias changes an original declaration or payload byte")
    _candidate_structure(target, task_id)
    source, reports = Path(material["candidate_root"]), []
    for row in declaration.get("reviews", []):
        path = relative_path(row["path"])
        raw = _regular(source / path)
        original, wrapper = _read(source / path), _read(target / path)
        local = wrapper.get("local_checks")
        if (not isinstance(local, dict) or set(local) != {"original_author_report", "original_relative_path", "original_sha256"}
                or local["original_relative_path"] != path or local["original_sha256"] != sha256(raw)
                or canonical(local["original_author_report"]) != canonical(original)):
            raise TaskflowError("unsigned candidate alias does not preserve the exact original author report and locator")
        reports.append({"review_id": row["id"], "kind": row["kind"],
            "original": evidence_ref(source / path, "sealed original unsigned author report"),
            "wrapper": evidence_ref(target / path, "unsigned source-bound report wrapper")})
    return {"kind": "byte_identical_unsigned_candidate_alias_v1", "candidate_root": str(target),
            "source_candidate_root": str(source), "file_refs": material["file_refs"], "unsigned_reports": reports}


def _resume_declaration_transport(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    from taskflow.execution_author import _author, _candidate_structure, _handoff, _identity, _public_root
    from taskflow.execution_runner import _read, _regular
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    prefix = "Cannot adopt optional candidate delivery: Optional candidate is incomplete or belongs to another task"
    exception = state.get("exception", {})
    if (definition.get("kind") != "author_quality_v1" or state.get("status") != "exception"
            or state.get("stage") != "author_handoff" or state.get("current_work_id") or not state.get("receipt")
            or exception.get("stage") != "author_handoff" or not isinstance(exception.get("reason"), str)
            or not exception["reason"].startswith(prefix)
            or any(state.get(key) for key in ("candidate_id", "candidate_root", "public_work_id", "public_attempt_id",
                                             "public_archive", "child_flow_id", "technical_candidate_root"))):
        raise TaskflowError("declaration transport requires the exact received unclaimed optional-candidate exception")
    author, _ = _author(store, definition)
    if author["state"] != "completed" or not state.get("author_archive"):
        raise TaskflowError("declaration transport requires the same completed original author and full archive")
    dashboard = store.dashboard()
    candidate_id, public_id = _identity(record["flow_id"], "author-candidate"), _identity(record["flow_id"], "public-work")
    if (any(row.get("work_id") == public_id or row.get("candidate_id") == candidate_id
            for row in [*dashboard["work"], *dashboard["attempts"]])
            or any(row.get("candidate_id") == candidate_id or row.get("author_attempt") == author["attempt_id"]
                   for row in dashboard.get("candidates", []))
            or any(row["flow_id"] == _identity(record["flow_id"], "quality-flow") for row in store.list_flows())
            or any(path.exists() or path.is_symlink() for path in
                   (_public_root(record) / "runtime", Path(definition["directory"]) / "quality-definition.json",
                    Path(definition["directory"]) / "quality"))):
        raise TaskflowError("declaration transport cannot replace an existing candidate or public/quality successor")
    archive = Path(state["author_archive"])
    handoff = _handoff(archive, definition, author)
    specification = resolution.get("declaration_transport")
    material = store._optional_candidate_material(author, archive, handoff["candidate_subpath"],
                                                   declaration_transport=specification)
    projection = material.get("declaration_transport")
    if not projection or material["archive"] != str(archive):
        raise TaskflowError("declaration transport lacks its exact original author archive projection")
    _declaration_transport_reviews(material, author["task_id"])
    prior = evidence_ref(Path(state["receipt"]), "original optional-candidate declaration exception preserved")
    original = _read(Path(prior["path"]))
    if (original.get("flow_id") != record["flow_id"] or original.get("status") != "exception"
            or original.get("exception") != exception):
        raise TaskflowError("declaration transport original exception receipt differs")
    state["history"] = [*state["history"], {"kind": "optional_candidate_declaration_technical_resolution",
        "author_attempt": author["attempt_id"], "resolution": resolution, "evidence": refs,
        "previous_exception": exception, "previous_receipt": prior, "declaration_transport": projection,
        "candidate_binding": material["candidate_binding"], "new_author_dispatched": False}]
    state.update(status="active", waiting=None, optional_candidate_declaration_transport=projection["definition"])
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_optional_candidate_declaration_resumed")


def _resume_declaration_dependency(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict], *, unsigned_alias: bool = False) -> dict:
    from taskflow.execution_author import _author, _candidate_structure, _handoff
    from taskflow.execution_experiment import _node, _packet, _run_id
    from taskflow.execution_runner import _read, _regular
    from taskflow.execution_store import evidence_ref

    definition, state = record["definition"], dict(record["state"])
    if (definition.get("kind") != "experiment_v1" or not definition.get("source_author_flow_id")
            or state.get("status") != "exception" or state.get("stage") != "experiment_v1"
            or state.get("current_work_id") or not state.get("receipt") or state.get("pending_business_scope")
            or any(state.get(key) for key in ("candidate_id", "accepted_review_id", "release", "formal_attempt", "imported_run"))):
        raise TaskflowError("declaration dependency resume requires its exact initial received source exception")
    packet = _packet(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    source_definition, source = parent["definition"], parent["state"]
    if (source_definition.get("kind") != "author_quality_v1" or source.get("status") != "active"
            or source.get("stage") != "author_handoff" or source.get("current_work_id")
            or any(source.get(key) for key in ("candidate_id", "public_work_id", "public_attempt_id", "child_flow_id"))
            or source.get("exception") or source.get("pending_business_scope")
            or any(source_definition.get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))):
        raise TaskflowError("declaration dependency source is not the same technically resumed active author flow")
    history_kind = "author_preparation_technical_resolution" if unsigned_alias else "optional_candidate_declaration_technical_resolution"
    entries = [row for row in source.get("history", []) if row.get("kind") == history_kind
        and state.get("exception") == {"stage": "experiment_v1", "reason":
            "source author-quality flow stopped with its original exception: " + str(row.get("previous_exception"))}]
    if len(entries) != 1:
        raise TaskflowError("declaration dependency lacks one exact resolution of its original propagated exception")
    entry = entries[0]
    previous = entry["previous_exception"]
    prefixes = ("author candidate report must be explicitly unreviewed: ",
                "author candidate report must be current inconclusive or a typed unsigned placeholder: ") if unsigned_alias else (
                "Cannot adopt optional candidate delivery: Optional candidate is incomplete or belongs to another task",)
    source_action = "accept_unsigned_author_placeholders" if unsigned_alias else "accept_declaration_only_candidate_transport"
    if (previous.get("stage") != "author_handoff" or not isinstance(previous.get("reason"), str)
            or not previous["reason"].startswith(prefixes)
            or entry.get("resolution", {}).get("action") != source_action):
        raise TaskflowError("declaration dependency is not the explicit optional rubric-role transport")
    author, _ = _author(store, source_definition)
    authority = validate_evidence([source_definition["authorization"]])[0]
    if (author["state"] != "completed" or author["attempt_id"] != packet["source_author_attempt"]
            or author["intent_id"] != packet["source_author_intent"] or author["cli_sid"] != packet["source_author_sid"]
            or not author_packet_matches(store, parent=parent, packet=packet,
                authorization=definition["authorization"], packet_transport=state.get("source_business_packet_transport"))):
        raise TaskflowError("declaration dependency source author/authority binding differs")
    archive = Path(source["author_archive"])
    handoff = _handoff(archive, source_definition, author)
    if unsigned_alias:
        alias = source.get("technical_candidate_root")
        if (not alias or alias != entry["resolution"].get("candidate_root") or entry.get("author_attempt") != author["attempt_id"]):
            raise TaskflowError("unsigned dependency differs from its explicit same-author candidate alias")
        material = store._validated_optional_candidate_delivery(author)
        if material["archive"] != str(archive):
            raise TaskflowError("unsigned dependency belongs to a different original author archive")
        transport = _unsigned_candidate_alias(material, Path(alias), author["task_id"])
    else:
        specification = source.get("optional_candidate_declaration_transport")
        if (not specification or specification != entry.get("declaration_transport", {}).get("definition")
                or specification != entry["resolution"].get("declaration_transport")):
            raise TaskflowError("declaration dependency differs from its locked explicit source transport")
        material = store._optional_candidate_material(author, archive, handoff["candidate_subpath"], declaration_transport=specification)
        if material.get("declaration_transport") != entry["declaration_transport"]:
            raise TaskflowError("declaration dependency transport no longer matches its original source validation")
        _declaration_transport_reviews(material, author["task_id"])
        transport = material["declaration_transport"]
    validate_evidence([entry["previous_receipt"], *entry["evidence"]])
    parent_receipt = _read(Path(entry["previous_receipt"]["path"]))
    if parent_receipt.get("flow_id") != parent["flow_id"] or parent_receipt.get("exception") != previous:
        raise TaskflowError("declaration dependency original source receipt differs")
    run_id, work_id = _run_id(record, packet), _node(record, "solver")
    inner = Path(definition["technical"]["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    dashboard = store.dashboard()
    if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
            or row.get("runtime_path") in (str(inner), str(outer)) for row in [*dashboard["work"], *dashboard["attempts"]])
            or any(path.exists() or path.is_symlink() for path in (inner, outer, inner.with_name(inner.name + ".formal-intent.json")))):
        raise TaskflowError("declaration dependency has an existing solver work/attempt/runtime/intent")
    prior = evidence_ref(Path(state["receipt"]), "original propagated declaration dependency exception preserved")
    received = _read(Path(prior["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("status") != "exception"
            or received.get("exception") != state["exception"]):
        raise TaskflowError("declaration dependency original experiment receipt differs")
    state["history"] = [*state["history"], {"kind": "author_preparation_dependency_technical_resolution" if unsigned_alias else "declaration_dependency_technical_resolution",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"], "previous_receipt": prior,
        "source_author_flow_id": parent["flow_id"], "source_revision": parent["revision"],
        "source_previous_receipt": entry["previous_receipt"], "declaration_transport": transport,
        "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_declaration_dependency_resumed")


def _resume_author_packet_transport(store: ExecutionStore, record: dict, resolution: dict, refs: list[dict]) -> dict:
    """Adopt one exact locked source-metadata alias before any formal work."""
    import json

    from taskflow.execution_author_packet import validate_author_packet_transport
    from taskflow.execution_experiment import _author_source, _node, _packet, _run_id
    from taskflow.execution_store import evidence_ref
    from taskflow.execution_runner import _read

    definition, state = record["definition"], dict(record["state"])
    reason = "authorized author flow differs from its exact task/owner/policy/original business packet"
    if (definition.get("kind") != "experiment_v1" or not definition.get("source_author_flow_id")
            or state.get("status") != "exception" or state.get("stage") != "experiment_v1"
            or state.get("exception") != {"stage": "experiment_v1", "reason": reason}
            or state.get("pending_business_scope") is not None or state.get("current_work_id") is not None
            or not state.get("receipt") or state.get("run_index", 0) != 0
            or any(state.get(key) for key in ("candidate_id", "accepted_review_id", "release", "runs",
                "formal_attempt", "formal_runtime", "formal_outer", "imported_run", "native_quality_handoff",
                "source_business_packet_transport"))):
        raise TaskflowError("author packet resume requires its exact received initial zero-formal exception")
    original = evidence_ref(Path(state["receipt"]), "original author packet transport exception retained")
    received = _read(Path(original["path"]))
    if (received.get("flow_id") != record["flow_id"] or received.get("controller") != definition["owner"]
            or received.get("status") != "exception" or received.get("exception") != state["exception"]
            or any(received.get(key) != state.get(key) for key in ("history", "archives", "result"))
            or Path(original["path"]).name != "exception-" + sha256(canonical(received))[:16] + ".json"):
        raise TaskflowError("author packet resume differs from its original received exception")
    packet = _packet(record)
    parent = store.get_flow(definition["source_author_flow_id"])
    if (parent["definition"].get("kind") != "author_quality_v1"
            or any(parent["definition"].get(key) != definition[key] for key in ("task_id", "owner", "policy_id"))
            or parent["state"].get("status") not in {"active", "accepted"}
            or parent["state"].get("pending_business_scope") is not None
            or parent["state"].get("exception") is not None):
        raise TaskflowError("author packet resume cannot clear or replace its source business outcome")
    transport = validate_evidence([resolution["packet_transport"]])[0]
    if not any(row["path"] == transport["path"] and row["sha256"] == transport["sha256"] for row in refs):
        raise TaskflowError("author packet transport must be in the explicit resolution evidence")
    witness = validate_author_packet_transport(packet_transport=transport,
        authorization=definition["authorization"], source_flow_id=parent["flow_id"], store=store)
    work_id, run_id = _node(record, "solver"), _run_id(record, packet)
    technical = definition["technical"]
    inner = Path(technical["formal_runtime_root"]) / run_id
    outer = Path(definition["directory"]) / "runtimes" / work_id
    publication = Path(technical["publication_root"])
    footprints = (Path(technical["formal_runtime_root"]), inner, outer, publication,
                  inner.with_name(inner.name + ".formal-intent.json"),
                  publication / "reports/runtime-reservations" / (run_id + ".json"))
    if any(path.exists() or path.is_symlink() for path in footprints):
        raise TaskflowError("author packet resume has an existing formal preparation/publication")
    with store._db() as db:
        # Metadata only: do not invoke unrelated candidates' quality projections.
        # Keep all rows so an existing foreign-ID purpose cannot be filtered out.
        work = [json.loads(row["data"]) for row in db.execute("SELECT data FROM work")]
        attempts = [json.loads(row["data"]) for row in db.execute("SELECT data FROM attempts")]
        decisions = [json.loads(row["data"]) for row in db.execute("SELECT data FROM decisions")]
    source_candidates = {parent["state"]["candidate_id"]} if parent["state"].get("candidate_id") else set()
    if (parent["state"].get("status") == "accepted"
            and parent["state"].get("result", {}).get("outcome") == "candidate_quality_accepted"):
        effective = {**record, "state": {**state, "source_business_packet_transport": transport}}
        bound = _author_source(store, effective, packet, require_candidate=True)
        child_state = bound["source"]["state"]
        if child_state.get("pending_business_scope") is not None or child_state.get("exception") is not None:
            raise TaskflowError("author packet resume cannot clear its accepted child business hold")
        source_candidates.add(child_state["candidate_id"])
    if (any(row.get("work_id") == work_id or row.get("intent_id") == work_id + "-intent"
            or row.get("runtime_path") in {str(inner), str(outer)}
            or (row.get("candidate_id") in source_candidates and row.get("role") == "solver")
            for row in [*work, *attempts])
            or any(row.get("decision_id") == _node(record, "experiment-decision")
                   or (row.get("candidate_id") in source_candidates and row.get("action") == "experiment") for row in decisions)):
        raise TaskflowError("author packet resume has an existing formal work/attempt/decision")
    state["source_business_packet_transport"] = transport
    state["history"] = [*state.get("history", []), {"kind": "author_packet_source_metadata_transport",
        "resolution": resolution, "evidence": refs, "previous_exception": state["exception"],
        "previous_receipt": original, "source_revision": parent["revision"],
        "source_business_packet_transport": witness, "new_model_dispatched": False}]
    state.update(status="active", waiting=None)
    state.pop("exception", None)
    state.pop("receipt", None)
    return _save(store, record, state, "lifecycle_author_packet_transport_resumed")


def resume_path_transport(store: ExecutionStore, identity: str, resolution: dict) -> dict:
    """Resume verified unclaimed author/formal preparation, never a model call."""
    action = resolution.get("action")
    if (action not in {"resume_failed_public_capacity_once", "resume_resolved_public_capacity_dependency",
                       "resume_author_packet_source_metadata_transport", "accept_path_only_candidate_transport", "accept_unsigned_author_placeholders",
                       "resume_unclaimed_public_preparation", "resume_unclaimed_formal_preparation",
                       "resume_resolved_quality_dependency", "resume_validated_source_quality_transport", "resume_unpublished_release_metadata", "resume_validated_evaluation_intake",
                       "resume_unpublished_release_report_binding",
                        "resume_validated_host_formal_preparation", "resume_validated_public_probe_preparation", "resume_validated_quality_composition",
                        "resume_validated_score_preparation", "accept_declaration_only_candidate_transport",
                         "resume_resolved_declaration_dependency", "resume_resolved_author_preparation_dependency"}
            or not isinstance(resolution.get("reason"), str) or not resolution["reason"].strip()):
        raise TaskflowError("a specific path-only candidate transport resolution is required")
    refs = validate_evidence(resolution.get("evidence"))
    with _lifecycle_lock(store, identity):
        record = store.get_flow(identity)
        if action in {"resume_failed_public_capacity_once", "resume_resolved_public_capacity_dependency"}:
            from taskflow.execution_public_continuation import resume_public_capacity_once, resume_public_capacity_dependency

            if action == "resume_failed_public_capacity_once":
                return resume_public_capacity_once(store, record, resolution, refs)
            source_id = record["definition"].get("source_author_flow_id")
            if not source_id:
                raise TaskflowError("public capacity dependency requires its original author source")
            with _lifecycle_lock(store, source_id):
                return resume_public_capacity_dependency(store, record, resolution, refs)
        if action == "resume_author_packet_source_metadata_transport":
            return _resume_author_packet_transport(store, record, resolution, refs)
        if action == "resume_unclaimed_formal_preparation":
            return _resume_unclaimed_formal(store, record, resolution, refs)
        if action == "resume_resolved_quality_dependency":
            return _resume_quality_dependency(store, record, resolution, refs)
        if action == "resume_validated_source_quality_transport":
            return _resume_source_quality_transport(store, record, resolution, refs)
        if action == "resume_unpublished_release_report_binding":
            return _resume_unpublished_release_report_binding(store, record, resolution, refs)
        if action == "resume_unpublished_release_metadata":
            return _resume_unpublished_release_metadata(store, record, resolution, refs)
        if action == "resume_validated_evaluation_intake":
            return _resume_validated_intake(store, record, resolution, refs)
        if action == "resume_validated_host_formal_preparation":
            return _resume_validated_host_preparation(store, record, resolution, refs)
        if action == "resume_validated_public_probe_preparation":
            return _resume_validated_host_preparation(store, record, resolution, refs, public_probe=True)
        if action == "resume_validated_quality_composition":
            return _resume_validated_composition(store, record, resolution, refs)
        if action == "resume_validated_score_preparation":
            return _resume_validated_score_preparation(store, record, resolution, refs)
        if action == "accept_declaration_only_candidate_transport":
            return _resume_declaration_transport(store, record, resolution, refs)
        if action == "resume_resolved_declaration_dependency":
            return _resume_declaration_dependency(store, record, resolution, refs)
        if action == "resume_resolved_author_preparation_dependency":
            return _resume_declaration_dependency(store, record, resolution, refs, unsigned_alias=True)
        state = dict(record["state"])
        stage = state.get("stage")
        if (record["definition"].get("kind") != "author_quality_v1"
                or state.get("status") != "exception" or not state.get("receipt")
                or state.get("current_work_id") or stage not in {"author_handoff", "public_prepare"}
                or (stage == "author_handoff" and state.get("candidate_id"))
                or (stage == "public_prepare" and action != "resume_unclaimed_public_preparation")
                or (stage == "author_handoff" and action == "resume_unclaimed_public_preparation")):
            raise TaskflowError("resolution requires a terminal, unclaimed author preparation exception")
        attempt = store.get_attempt(record["definition"]["source_author_attempt"])
        material = store._validated_optional_candidate_delivery(attempt)
        if (attempt["state"] != "completed"
                or (action == "accept_path_only_candidate_transport" and not material.get("transport"))
                or material["archive"] != state.get("author_archive")):
            raise TaskflowError("path transport is not the completed source author's verified archive")
        if stage == "public_prepare":
            from taskflow.execution_author import _identity

            public_id = _identity(identity, "public-work")
            if any(row["work_id"] == public_id for row in store.dashboard()["work"]):
                raise TaskflowError("public work was already enqueued; this resolution cannot replace it")
        alias = resolution.get("candidate_root")
        if alias is not None:
            from taskflow.contracts import read_json, safe_read
            from taskflow.execution_author import _candidate_structure

            target = Path(alias)
            if not target.is_absolute():
                raise TaskflowError("technical candidate alias must be absolute")
            declaration = read_json(target / "task.json")
            declared = {"task.json": "source_metadata", **{row["path"]: row["role"] for row in declaration["files"]}}
            expected = {row["package_path"]: row for row in material["file_refs"]}
            if set(declared) != set(expected):
                raise TaskflowError("technical candidate alias declaration differs from its delivered source")
            for name, role in declared.items():
                raw = safe_read(target, name)
                if (role != expected[name]["role"] or sha256(raw) != expected[name]["sha256"]
                        or len(raw) != expected[name]["size"]):
                    raise TaskflowError("technical candidate alias changes a delivered declaration or payload")
            _candidate_structure(target, attempt["task_id"])
            state["technical_candidate_root"] = str(target)
        from taskflow.execution_store import evidence_ref

        prior = evidence_ref(Path(state["receipt"]), "original author handoff exception preserved")
        state["history"] = [*state["history"], {"kind": "author_preparation_technical_resolution",
            "author_attempt": attempt["attempt_id"], "resolution": resolution, "evidence": refs,
            "previous_exception": state.get("exception"), "previous_receipt": prior,
            "candidate_root": material["candidate_root"], "new_author_dispatched": False}]
        state.update(status="active", waiting=None)
        state.pop("exception", None)
        state.pop("receipt", None)
        return _save(store, record, state, "lifecycle_author_preparation_resumed")


def advance_lifecycle(store: ExecutionStore, identity: str) -> dict:
    """Advance one fixed mechanical handoff; errors stop only this intent."""
    record = store.get_flow(identity)
    if record["definition"].get("kind") not in KINDS:
        raise TaskflowError("this record is not an execution lifecycle; use its original entry")
    state = dict(record["state"])
    try:
        validate_evidence(record["evidence"])
        if state["status"] in ("accepted", "exception"):
            return _receive(store, record)
        kind = record["definition"]["kind"]
        if kind == "original_score_pause_v1":
            from taskflow.execution_original_pause import advance_original_score_pause

            return advance_original_score_pause(store, record)
        if kind == "teacher_successor_v1":
            from taskflow.execution_teacher_successor import advance_teacher_successor

            return advance_teacher_successor(store, record)
        if kind == "author_quality_v1":
            from taskflow.execution_author import advance_author

            return advance_author(store, record)
        if kind == "experiment_v1":
            from taskflow.execution_experiment import advance_experiment

            return advance_experiment(store, record)
        if kind == "evaluation_tail_v1":
            from taskflow.execution_review import ingest_signed_evaluation

            result = ingest_signed_evaluation(config=record["definition"]["review"],
                                              destination=Path(record["definition"]["directory"]) / "adoption")
            if not result.get("accepted"):
                waiting = {"stage": "signed_evaluation", "reason": result.get("reason"),
                           "state": result.get("state")}
                if waiting == state.get("waiting"):
                    return record
                state["waiting"] = waiting
                return _save(store, record, state, "lifecycle_waiting_original_evaluation")
            state.update(status="accepted", waiting=None, result=result)
            state["history"] = [*state["history"], {"kind": "signed_original_evaluation_received",
                                                    "evaluation_id": result["evaluation_id"],
                                                    "source_sha256": result["source_sha256"],
                                                    "signer_sid": result["signer_sid"]}]
            state["archives"] = [result["review_archive"]["path"]]
            return _save(store, record, state, "lifecycle_signed_original_adopted")
        raise TaskflowError("registered lifecycle has no installed handler: " + kind)
    except (TaskflowError, OSError, ValueError, KeyError) as exc:
        latest = store.get_flow(identity)
        failed = dict(latest["state"])
        failed.update(status="exception", exception={"stage": failed["stage"], "reason": str(exc)})
        return _save(store, latest, failed, "lifecycle_exception")


def run_lifecycle(store: ExecutionStore, identity: str, *, allow_model: bool = False,
                   poll_seconds: float = 1) -> dict:
    """Consume this exact registered intent; this function never redraws a call."""
    record = store.get_flow(identity)
    if record["definition"].get("kind") not in KINDS:
        raise TaskflowError("this record is not an execution lifecycle; use its original entry")
    with _lifecycle_lock(store, identity):
        directory = Path(record["definition"]["directory"])
        _publish_once(directory / ("consumer-" + str(os.getpid()) + ("-model.json" if allow_model else "-no-model.json")),
                      {"pid": os.getpid(), "flow_id": identity, "allow_model": allow_model})
        while True:
            record = advance_lifecycle(store, identity)
            state = record["state"]
            if state["status"] in ("accepted", "exception") and state.get("receipt"):
                return record
            if (record["definition"]["kind"] == "teacher_successor_v1"
                    and state.get("author_lifecycle_id")):
                # A mechanical tick never calls a model. Normal successor
                # execution remains in this explicit allow_model consumer path.
                try:
                    followed = run_lifecycle(store, state["author_lifecycle_id"], allow_model=allow_model,
                                             poll_seconds=poll_seconds)
                except TaskflowError as exc:
                    if str(exc) != "this lifecycle already has a consumer":
                        raise
                    return {**record, "execution": "existing_author_consumer_retained",
                            "author_lifecycle_id": state["author_lifecycle_id"]}
                if followed.get("execution") == "ready_requires_explicit_model_permission":
                    return {**record, "execution": "ready_requires_explicit_model_permission",
                            "author_lifecycle_id": followed["flow_id"]}
                continue
            if state.get("current_work_id"):
                work = store.get_work(state["current_work_id"])
                if work["state"] == "ready":
                    if not allow_model:
                        return {**record, "execution": "ready_requires_explicit_model_permission"}
                    from taskflow.execution import run_worker

                    try:
                        run_worker(store, "lifecycle-consumer", allow_model, [work["work_id"]])
                    except (TaskflowError, OSError, ValueError) as exc:
                        latest = store.get_flow(identity)
                        failed = dict(latest["state"])
                        failed.update(status="exception", exception={"stage": failed["stage"],
                                      "work_id": work["work_id"], "reason": str(exc)})
                        _save(store, latest, failed, "lifecycle_worker_exception")
            time.sleep(poll_seconds)
