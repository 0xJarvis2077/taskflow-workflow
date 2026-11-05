"""Fixed, preauthorized execution handoffs on the existing SQLite ledger.

This consumer owns no model service or scheduling database. It binds a single
business packet to existing runner/import operations and records their result.
"""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path

import time
from typing import Iterator

from taskflow.execution_completion_observation import author_technically_complete
from taskflow.contracts import TaskflowError, canonical, relative_path, sha256, validate_id
from taskflow.execution_flow import _publish_once
from taskflow.execution_author_packet import author_packet_matches
from taskflow.execution_formal import _handoff_errors
from taskflow.execution_store import ExecutionStore, validate_evidence


from taskflow.execution_legacy_guard import KINDS, assert_legacy_dispatch_allowed


def assert_legacy_simulation(store: ExecutionStore, work_id: str) -> None:
    """Only an explicitly prepared neutral fixture may exercise the old loop."""
    from taskflow.execution_runner import _read

    work = store.get_work(work_id)
    path = Path(work["runtime_path"]) / "manifest.json" if work.get("runtime_path") else None
    if path is None or not path.exists():
        return  # The existing worker retains the unprepared attempt as uncertain.
    if _read(path).get("mode") != "test_fixture":
        raise TaskflowError("legacy author/lifecycle execution is retired; only test_fixture simulation remains")


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
    if definition["kind"] == "bounded_original_rounds_v1":
        from taskflow.execution_rounds import register_round_plan

        return register_round_plan(store, definition, refs)
    if definition["kind"] == "baseline_quality_v1":
        from taskflow.execution_baseline_quality import register_baseline_quality

        return register_baseline_quality(store, definition, refs)
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
        if definition.get("technical_quality_successor") is not None:
            from taskflow.execution_author import validate_technical_quality_successor

            technical_authority = definition["technical_quality_successor"]["authority"]
            if not any(item["path"] == technical_authority["path"] and item["sha256"] == technical_authority["sha256"] for item in refs):
                raise TaskflowError("quality technical successor authority must be in its immutable evidence")
            validate_technical_quality_successor(store, definition)
    if definition["kind"] == "experiment_v1":
        if definition.get("source_author_flow_id") or definition.get("source_kind") == "engineering_candidate_quality_v1":
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
    if not author_technically_complete(author) or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
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
    if not author_technically_complete(author) or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
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
    if not author_technically_complete(author) or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
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
    if not author_technically_complete(author) or _handoff(Path(state["author_archive"]), definition, author)["disposition"] != "candidate_complete":
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
    child_id = parent["state"].get("child_flow_id")
    if child_id:
        child = store.get_flow(child_id)
        if any(row.get("kind") in {"workflow_inconclusive_continued", "workflow_rejected_quality_continued"}
               for row in child["state"].get("history", [])):
            from taskflow.execution_quality_continuation import resolved_inconclusive_dependency

            return resolved_inconclusive_dependency(store, parent, previous)
    if isinstance(previous.get("reason"), dict) and previous["reason"].get("reason") == "repair defect_ids: list required":
        return _resolved_repair_identity_dependency(store, parent, previous, active_only=active_only)
    if previous.get("reason") == {"reason": "Runtime must be outside the repository", "work_id": None}:
        return _resolved_public_preparation_dependency(store, parent, previous)
    if isinstance(previous.get("reason"), dict) and previous["reason"].get("reason") == "closure requires whole-package review or explicit covered same-public private recheck":
        return _resolved_coverage_chain_dependency(store, parent, previous)
    return _resolved_projection_dependency(store, parent, previous, active_only=active_only)









@_handoff_errors





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












def resume_path_transport(store: ExecutionStore, identity: str, resolution: dict) -> dict:
    """Reject removed incident recovery; archived records remain read-only."""
    raise TaskflowError("legacy lifecycle recovery is retired; submit a new explicit role task")


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
        if kind == "bounded_original_rounds_v1":
            from taskflow.execution_rounds import advance_round_plan

            return advance_round_plan(store, record)
        if kind == "baseline_quality_v1":
            from taskflow.execution_baseline_quality import advance_baseline_quality

            return advance_baseline_quality(store, record)
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
                   poll_seconds: float = 1, wait_for_round_binding: bool = False) -> dict:
    """Consume this exact registered intent; this function never redraws a call."""
    record = store.get_flow(identity)
    if record["definition"].get("kind") not in KINDS:
        raise TaskflowError("this record is not an execution lifecycle; use its original entry")
    source = record["definition"].get("source_author_binding")
    if isinstance(source, dict) and source.get("work_id"):
        assert_legacy_simulation(store, source["work_id"])
    if record["state"].get("current_work_id"):
        assert_legacy_simulation(store, record["state"]["current_work_id"])
    if type(wait_for_round_binding) is not bool or (wait_for_round_binding
            and record["definition"]["kind"] != "bounded_original_rounds_v1"):
        raise TaskflowError("persistent business-binding wait requires a bounded round plan")
    with _lifecycle_lock(store, identity):
        directory = Path(record["definition"]["directory"])
        _publish_once(directory / ("consumer-" + str(os.getpid()) + ("-model.json" if allow_model else "-no-model.json")),
                      {"pid": os.getpid(), "flow_id": identity, "allow_model": allow_model})
        if record["definition"]["kind"] == "bounded_original_rounds_v1":
            from taskflow.execution_rounds import managed_round_startup

            managed_round_startup(store, record, directory)
        while True:
            record = advance_lifecycle(store, identity)
            state = record["state"]
            bounded = record["definition"]["kind"] == "bounded_original_rounds_v1"
            if bounded and state.get("mode") != "running":
                return {**record, "execution": "bounded_rounds_" + state["mode"]}
            if bounded and not state.get("current_work_id") and state.get("waiting") in {
                    "next_business_binding_required", "observing_locked_first_original",
                    "source_exception_requires_original_recovery", "existing_control_blocks_rounds"}:
                if wait_for_round_binding and state["waiting"] in {
                        "next_business_binding_required", "observing_locked_first_original"}:
                    time.sleep(poll_seconds)
                    continue
                if state["waiting"] in {"next_business_binding_required", "observing_locked_first_original"}:
                    from taskflow.execution_rounds import record_normal_wait_exit

                    proof = record_normal_wait_exit(store, record)
                    if proof is not None:
                        return {**record, "execution": state["waiting"], "normal_wait_exit": proof}
                return {**record, "execution": state["waiting"]}
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
                    assert_legacy_simulation(store, work["work_id"])
                    if not allow_model:
                        return {**record, "execution": "ready_requires_explicit_model_permission"}
                    from taskflow.execution import run_worker

                    try:
                        if bounded:
                            from taskflow.execution_rounds import round_claim_scope

                            with round_claim_scope(store, record, work["work_id"]):
                                outcome = run_worker(store, "bounded-round-consumer", allow_model, [work["work_id"]])
                            if outcome.get("state") == "waiting":
                                return {**store.get_flow(identity), "execution": "bounded_round_claim_blocked"}
                        else:
                            run_worker(store, "lifecycle-consumer", allow_model, [work["work_id"]])
                    except (TaskflowError, OSError, ValueError) as exc:
                        latest = store.get_flow(identity)
                        failed = dict(latest["state"])
                        failed.update(status="exception", exception={"stage": failed["stage"],
                                      "work_id": work["work_id"], "reason": str(exc)})
                        _save(store, latest, failed, "lifecycle_worker_exception")
            time.sleep(poll_seconds)
