"""Bounded capacity recovery for a received native repair failure.

The CLI, workflow and worker integrations use this module to retain original
evidence, issue a paused successor, and guard each authorized retry.
"""

from contextlib import ExitStack, contextmanager
import json
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, relative_path, sha256, validate_id
from taskflow.execution_public_continuation import CAPACITY, _actual, _profile, _time
from taskflow.execution_runner import _info, _read, _regular, archive_full_attempt
from taskflow.execution_store import WorkClaimDataError, evidence_ref, validate_evidence

KIND = "bounded_repair_capacity_v1"


class RetryCapacityBusy(Exception):
    """A required original lifecycle is busy; nothing was claimed or started."""


def _require(ok, message):
    if not ok:
        raise TaskflowError("repair capacity continuation: " + message)


@contextmanager
def capacity_lifecycle_locks(store, proof):
    from taskflow.execution_lifecycle import _lifecycle_lock

    with ExitStack() as locks:
        try:
            identities = {proof["parent_flow_id"], *proof["dependent_revisions"]}
            for identity in sorted(identities):
                locks.enter_context(_lifecycle_lock(store, identity))
        except TaskflowError as error:
            if str(error) != "this lifecycle already has a consumer":
                raise
            raise RetryCapacityBusy("capacity source lifecycle is busy; no claim occurred") from error
        yield


def _validate_capacity_trace(raw, sid):
    """Require the original visible CLI thread to end only in capacity failure."""
    events = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
    _require(all(isinstance(row, dict) for row in events), "original trace contains a non-object event")
    starts = [row for row in events if row.get("type") == "thread.started"]
    turns = [row for row in events if row.get("type") == "turn.failed"]
    errors = [row for row in events if row.get("type") == "error"]
    _require(len(starts) == 1 and starts[0].get("thread_id") == sid
             and len(turns) == 1 and isinstance(turns[0].get("error"), dict)
             and turns[0]["error"].get("message") == CAPACITY
             and bool(errors) and all(row.get("message") == CAPACITY for row in errors)
             and not any(row.get("type") == "turn.completed" for row in events)
             and events[-1] == turns[0], "original trace does not prove its sole terminal capacity failure")


def _retry_work_id(record, proof, ordinal):
    from taskflow.execution_flow import _identity

    _require(type(ordinal) is int and 1 <= ordinal <= proof["maximum_additional_attempts"]
             and type(proof.get("initial_step")) is int, "retry ordinal is outside the explicit budget")
    return _identity(record["flow_id"], f"{proof['initial_step'] + ordinal}-repair")


def _validate_full_archive(archive, intent_id, original_root):
    from taskflow.execution_runner import _full_verify, _validated_collection

    archive = Path(archive)
    binding, intent = _read(archive / "archive-binding.json"), _read(archive / "supplemental-intent.json")
    _require(binding.get("intent_id") == intent.get("intent_id") == intent_id
             and binding.get("collection") == _info(archive / "collection.json")
             and intent.get("collection") == _info(archive / "collection.json")
             and intent.get("manifest") == _info(archive / "manifest.json")
             and _regular(archive / "manifest.json") == _regular(original_root / "manifest.json")
             and _regular(archive / "collection.json") == _regular(original_root / "collection.json"),
             "failed full archive binding differs")
    _validated_collection(archive, _read(archive / "manifest.json"))
    _require(isinstance(binding.get("files"), dict) and isinstance(binding.get("directories"), list),
             "failed archive lacks full member and directory binding")
    for path in archive.rglob("*"):
        _require(not path.is_symlink() and (path.is_file() or path.is_dir()),
                 "failed archive contains a symlink or special file")
    files = {str(path.relative_to(archive)) for path in archive.rglob("*") if path.is_file()}
    directories = sorted(str(path.relative_to(archive)) for path in archive.rglob("*") if path.is_dir())
    _require(files == set(binding["files"]) | {"archive-binding.json"} and directories == binding["directories"],
             "failed archive member or directory set changed")
    for path in original_root.rglob("*"):
        _require(not path.is_symlink() and (path.is_file() or path.is_dir()),
                 "failed original runtime contains a symlink or special file")
    originals = {str(path.relative_to(original_root)) for path in original_root.rglob("*") if path.is_file()}
    original_directories = sorted(str(path.relative_to(original_root)) for path in original_root.rglob("*") if path.is_dir())
    _require(originals == set(binding["files"]) and original_directories == binding["directories"],
             "failed original runtime member or directory set changed")
    for name, expected in binding["files"].items():
        relative = relative_path(name)
        _require(_info(archive / relative) == expected == _info(original_root / relative),
                 "failed archive or original member bytes changed")
    _full_verify(archive / "supplemental", intent, True)


def _validate_retry_manifest(store, record, proof, manifest, ordinal):
    from taskflow.execution_batch_binding import validate_batch_runner
    from taskflow.execution_flow import _identity
    from taskflow.execution_runner import _validate

    original = store.get_attempt(proof["failed_attempt_id"])
    original_root, old, _ = _actual(store, original)
    root = Path(record["definition"]["directory"]) / "nodes" / f"{proof['initial_step'] + ordinal}-repair" / "runtime"
    _require(_validate(root) == manifest, "retry manifest or sealed inputs differ")
    binding = validate_batch_runner(manifest, manifest=True)
    model, cli = _profile(manifest)
    old_model, old_cli = _profile(old)
    _require(binding is not None and manifest.get("mode") == "native_role" and manifest.get("resume_sid") is None
             and manifest.get("intent_id") == _retry_work_id(record, proof, ordinal) and manifest.get("role") == "repair"
             and manifest.get("candidate_binding") == proof["candidate_binding"]
             and binding.get("model_slot") == old.get("model_slot") and binding.get("policy_id") == original["policy_id"]
             and manifest.get("batch_binding", {}).get("setting") == old.get("batch_binding", {}).get("setting")
             and all(manifest.get(k) == old.get(k) for k in ("reasoning_selection", "reasoning_context", "timeout_seconds"))
             and all(model.get(k) == old_model.get(k) for k in ("requested", "reasoning_effort"))
             and cli.resolve() == old_cli.resolve() and _info(cli) == _info(old_cli),
             "new repair is not the same confirmed profile and fresh intent")
    old_context = _read(original_root.parent / "current.json")
    context = _read(root.parent / "current.json")
    _require(_regular(root.parent / "current.json") == _regular(root / "inputs/execution/current.json"),
             "retry current context differs from its captured input")
    expected_id = _retry_work_id(record, proof, ordinal)
    changed = {"node_id": expected_id, "intent_id": expected_id, "runtime": str(root),
               "work": str(root.parent / "work"), "scratch": str(root.parent / "scratch"),
               "inputs_root": str(root / "inputs"),
               "revised_candidate_id": _identity(record["flow_id"], expected_id + "-candidate")}
    _require(context == {**old_context, **changed}, "retry changes original assigned responsibility or source context")
    mapping = {str(old_context[key]): str(value) for key, value in changed.items() if old_context.get(key) is not None}

    def substitute(value):
        if isinstance(value, str):
            for source, target in sorted(mapping.items(), key=lambda row: len(row[0]), reverse=True):
                value = value.replace(source, target)
            return value
        if isinstance(value, list):
            return [substitute(item) for item in value]
        if isinstance(value, dict):
            return {substitute(key): substitute(item) for key, item in value.items()}
        return value

    old_inputs = [item for item in old["inputs"] if item.get("target") != "execution/current.json"]
    new_inputs = [item for item in manifest["inputs"] if item.get("target") != "execution/current.json"]
    _require(new_inputs == old_inputs and sum(item.get("target") == "execution/current.json" for item in manifest["inputs"]) == 1
             and all(manifest.get(key) == old.get(key) for key in ("outputs", "output_trees", "output_tree_contract_version"))
             and manifest.get("argv") == substitute(old.get("argv"))
             and manifest.get("native") == substitute(old.get("native"))
             and manifest.get("dependencies") == substitute(old.get("dependencies"))
             and _regular(root / "request.md") == substitute(_regular(original_root / "request.md").decode("utf-8")).encode("utf-8"),
             "retry request, inputs, permissions or output contract differ from the original")


def _failed_receipt(store, child):
    state = child["state"]
    path = Path(state["receipt"])
    raw = _read(path)
    _require(path.name == "exception-" + sha256(canonical(raw))[:16] + ".json"
             and raw.get("flow_id") == child["flow_id"] and raw.get("status") == "exception"
             and raw.get("controller") == child["definition"]["owner"]
             and all(raw.get(key) == state.get(key) for key in ("candidate_id", "history", "archives", "exception")),
             "original received child exception differs")
    with store._db() as db:
        event = db.execute("SELECT kind,data FROM events WHERE seq=?", (child["event_seq"],)).fetchone()
    body = json.loads(event["data"]) if event else {}
    _require(event is not None and event["kind"] == "flow_updated"
             and body.get("flow_id") == child["flow_id"] and body.get("revision") == child["revision"]
             and body.get("transition", {}).get("kind") == "workflow_result_received"
             and canonical(body.get("state")) == canonical(state), "child receipt event differs")
    return evidence_ref(path, "original failed repair exception retained")


def _validate_prepared_witness(store, child_id, proof):
    """Read back the original transition and complete failed archive."""
    with store._db() as db:
        rows = db.execute("SELECT data FROM events WHERE kind='flow_updated' AND seq> ?", (proof["child_event_seq"],)).fetchall()
    events = [json.loads(row["data"]) for row in rows]
    matches = [event for event in events if event.get("flow_id") == child_id
               and event.get("previous_revision") == proof["child_revision"]
               and event.get("revision") == proof["child_revision"] + 1
               and event.get("transition", {}).get("kind") == "workflow_repair_capacity_continuation_prepared"
               and event["transition"].get("binding") == proof
               and event.get("state", {}).get("repair_capacity_continuation") == proof]
    _require(len(matches) == 1, "one original preparation event is required")
    root = Path(store.get_attempt(proof["failed_attempt_id"])["runtime_path"])
    _validate_full_archive(proof["failed_archive"], proof["failed_work_id"], root)


def _proof(store, parent, child, failed_id, authority):
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_batch import policy_setting, validate_batch_policy
    from taskflow.execution_flow import _binding, _identity, _predecessor
    from taskflow.execution_flow_contracts import _ids
    from taskflow.packages import check_task

    pd, ps, cd, cs = parent["definition"], parent["state"], child["definition"], child["state"]
    _require(pd.get("kind") == "author_quality_v1" and ps.get("status") == "exception"
             and ps.get("stage") == "child_quality" and ps.get("receipt")
             and ps.get("pending_business_scope") is None and ps.get("child_flow_id") == child["flow_id"]
             and all(pd.get(k) == cd.get(k) for k in ("task_id", "owner", "policy_id")),
             "same authorized original parent and child required")
    _require(cd.get("initial", {}).get("candidate_id") == ps.get("candidate_id")
             and cd.get("initial", {}).get("candidate_root") == ps.get("candidate_root"),
             "child is not the original author candidate and root")
    dependents = [item for item in store.list_flows()
                  if item["definition"].get("source_author_flow_id") == parent["flow_id"]]
    _require(len(dependents) <= 1, "ambiguous dependent experiments cannot be guessed")
    for dependent in dependents:
        ds = dependent["state"]
        _require(dependent["definition"].get("kind") == "experiment_v1"
                 and ds.get("status") in {"active", "exception"}
                 and ds.get("stage") == "experiment_v1" and ds.get("current_work_id") is None
                 and (ds.get("status") != "exception" or ds.get("receipt"))
                 and (ds.get("status") != "active" or not ds.get("receipt"))
                 and not any(ds.get(k) for k in ("candidate_id", "native_quality_handoff", "release", "runs", "formal_runtime")),
                 "dependent experiment is not at its original blocked source boundary")
    _require(cs.get("status") == "exception" and cs.get("stage") == "repair"
             and cs.get("initialized") is True and cs.get("pending_business_scope") is None
             and cs.get("receipt") and cs.get("repair_capacity_continuation") is None
             and type(cs.get("step")) is int and cs["step"] >= 0,
             "exact unresolved original repair exception required")
    failed = store.get_attempt(failed_id)
    work_id = _identity(child["flow_id"], f"{cs['step']}-repair")
    _require(cs.get("current_node_id") == cs.get("current_work_id") == work_id == failed["work_id"]
             and cs.get("exception") == {"reason": "role technical failure/uncertainty retained: " + failed_id,
                                          "work_id": work_id}
             and ps.get("exception") == {"stage": "child_quality", "reason": cs["exception"],
                                           "child_receipt": cs["receipt"]},
             "source exception does not bind the exact failed node")
    prior = _failed_receipt(store, child)
    from taskflow.execution_public_continuation import _dependent_binding, _no_formal, _received, _received_event

    parent_ref = _received(parent)
    parent_event = _received_event(store, parent)
    author, _ = _author(store, pd)
    _require(author["state"] == "completed" and ps.get("author_archive")
             and _handoff(Path(ps["author_archive"]), pd, author)["disposition"] == "candidate_complete",
             "original author source is not complete")
    related = _no_formal(store, parent)
    if related and related[0]["state"].get("status") == "active":
        from taskflow.execution_author_packet import author_packet_matches
        from taskflow.execution_experiment import _packet

        dependent = related[0]
        dd, ds = dependent["definition"], dependent["state"]
        packet = _packet(dependent)
        experiment_authority = validate_evidence([dd["authorization"]])[0]
        _require(all(dd.get(key) == pd.get(key) for key in ("task_id", "owner", "policy_id"))
                 and author["attempt_id"] == packet["source_author_attempt"]
                 and author["intent_id"] == packet["source_author_intent"]
                 and author["cli_sid"] == packet["source_author_sid"]
                 and author_packet_matches(store, parent=parent, packet=packet, authorization=experiment_authority,
                                           packet_transport=ds.get("source_business_packet_transport")),
                 "active dependent is not the original authorized experiment")
        dependent_binding = {"flow_id": dependent["flow_id"], "definition_digest": dependent["definition_digest"],
                             "authority": experiment_authority, "packet_sha256": sha256(canonical(packet)),
                             "state": ds, "event_seq": dependent["event_seq"]}
    else:
        dependent_binding = _dependent_binding(store, parent, author, related)
    with store._db() as db:
        rows = [json.loads(row["data"]) for table in ("work", "attempts")
                for row in db.execute("SELECT data FROM " + table + " WHERE task_id=?", (pd["task_id"],))]
    _require(not any(row.get("role") in {"solver", "review-facts", "review-score"} for row in rows),
             "task has existing formal or evaluation work")
    public, _ = _predecessor(store, cd)
    _require(public["state"] == "completed" and public["attempt_id"] == ps.get("public_attempt_id")
             and public.get("cli_sid") and public["cli_sid"] != author.get("cli_sid"),
             "original public predecessor differs")
    candidate = store.get_candidate(cs["candidate_id"])
    check = check_task(Path(cs["candidate_root"]), write_report=False)
    _require(not check["errors"] and all(check[k] == candidate[k] for k in
                 ("task_id", "input_digest", "content_digest", "evaluation_digest")),
             "current candidate differs from its locked bytes")
    validate_evidence(candidate["files"])
    root, manifest, collected = _actual(store, failed)
    from taskflow.execution_batch_binding import validate_batch_source

    _require(validate_batch_source(failed, manifest, store) is True,
             "original repair lacks its confirmed batch source")
    _require(failed["state"] == "failed" and failed["role"] == "repair"
             and failed["candidate_id"] == candidate["candidate_id"] and failed["policy_id"] == pd["policy_id"]
             and failed["intent_id"] == work_id and failed.get("cli_sid")
             and failed["cli_sid"] != author.get("cli_sid")
             and failed["cli_sid"] not in {item.get("cli_sid") for item in cs.get("history", []) if item.get("cli_sid")}
             and manifest.get("mode") == "native_role" and collected.get("nature") == "real_cli_attempt"
             and collected.get("process_ended") is True and collected.get("model_completed") is False
             and collected.get("delivery_complete") is False and collected.get("technical_accepted") is False
             and collected["termination"].get("spawn_state") == "started"
             and collected["termination"].get("exception") is None
             and collected["termination"].get("timed_out") is False
             and type(collected["termination"].get("returncode")) is int
             and collected["termination"]["returncode"] == failed["process_exit"],
             "original is not a proved ended, incomplete native repair")
    _require(len(store.list_attempts(work_id)) == 1, "failed repair work has multiple attempts")
    _validate_capacity_trace(_regular(root / "trace.jsonl"), failed["cli_sid"])
    context_path = root.parent / "current.json"
    _require(_regular(context_path) == _regular(root / "inputs/execution/current.json"),
             "repair context differs from its locked input")
    context = _read(context_path)
    ids = _ids(context.get("defect_ids"), "repair defect ids", nonempty=True)
    _require(context.get("defects") == cs.get("defects") and set(ids) == {d["defect_id"] for d in cs["defects"]}
             and all(context.get(k) == v for k, v in _binding(candidate).items())
             and context.get("node_id") == context.get("intent_id") == work_id,
             "failed request does not bind current candidate and exact assigned defects")
    for issue in cs["defects"]:
        defect = store.get_defect(issue["defect_id"])
        _require(defect["state"] == "open" and defect["candidate_id"] == candidate["candidate_id"]
                 and defect["review_id"] == issue["source_review_id"]
                 and all(defect.get(k) == issue.get(k) for k in ("kind", "responsibility", "evidence")),
                 "assigned defect is not its original open independent finding")
    validate_evidence(cs.get("last_review_evidence", []))
    setting = policy_setting(store.get_policy(pd["policy_id"])["config"], "repair",
                             cd["roles"]["repair"]["runner"].get("model_slot"), pd["task_id"])
    validate_batch_policy(store.get_policy(pd["policy_id"])["config"])
    profile, cli = _profile(manifest)
    _require(manifest.get("model_slot") == cd["roles"]["repair"]["runner"].get("model_slot")
             and manifest.get("batch_binding", {}).get("setting") == setting
             and all(manifest.get(k) == setting.get(k) for k in
                     ("reasoning_selection", "reasoning_context", "timeout_seconds"))
             and cli.resolve() == Path(setting["cli"]).resolve()
             and profile.get("requested") == setting["model"]
             and profile.get("reasoning_effort") == setting["reasoning_effort"],
             "original repair profile differs from the confirmed batch")
    new_work = _identity(child["flow_id"], f"{cs['step'] + 1}-repair")
    _require(isinstance(authority, dict) and type(authority.get("schema_version")) is int
             and type(authority.get("maximum_additional_attempts")) is int
             and authority["maximum_additional_attempts"] > 0
             and set(authority) == {"schema_version", "kind", "owner", "flow_id",
             "failed_attempt_id", "candidate_binding", "policy_id", "action", "maximum_additional_attempts", "direct_user_evidence"},
             "bounded-retry authority has missing or unknown fields")
    direct_user_evidence = validate_evidence([authority["direct_user_evidence"]])[0]
    _require(authority == {"schema_version": 1, "kind": KIND, "owner": cd["owner"],
                           "flow_id": child["flow_id"], "failed_attempt_id": failed_id,
                           "candidate_binding": _binding(candidate), "policy_id": pd["policy_id"],
                           "action": "prepare_bounded_same_profile_repairs",
                           "maximum_additional_attempts": authority["maximum_additional_attempts"],
                           "direct_user_evidence": direct_user_evidence},
             "explicit exact bounded-retry authority differs")
    return {"schema_version": 1, "kind": KIND, "failed_attempt_id": failed_id, "failed_work_id": work_id,
            "failed_sid": failed["cli_sid"], "author_sid": author["cli_sid"],
            "candidate_binding": _binding(candidate), "defect_ids": ids,
            "maximum_additional_attempts": authority["maximum_additional_attempts"],
            "initial_step": cs["step"],
            "parent_flow_id": parent["flow_id"], "parent_revision": parent["revision"],
            "dependent_revisions": {item["flow_id"]: item["revision"] for item in dependents},
            "dependent_binding": dependent_binding,
            "child_revision": child["revision"], "child_event_seq": child["event_seq"],
            "previous_child_receipt": prior, "previous_parent_receipt": parent_ref, "parent_received_event": parent_event,
            "direct_user_evidence": direct_user_evidence,
            "failed_manifest": evidence_ref(root / "manifest.json", "original failed repair manifest"),
            "failed_collection": evidence_ref(root / "collection.json", "original failed repair collection"),
            "failed_trace": evidence_ref(root / "trace.jsonl", "original capacity error and failed turn"),
            "new_work_id": new_work, "new_model_dispatched": False}


def prepare_repair_capacity_budget(store, parent_id, child_id, resolution):
    """Prepare the first paused repair, with no model or experiment start."""
    from taskflow.execution_flow import _consumer_lock
    from taskflow.execution_lifecycle import _lifecycle_lock

    if not isinstance(resolution, dict) or set(resolution) != {"reason", "authority", "failed_attempt_id"}:
        raise TaskflowError("repair capacity continuation requires reason, authority and the exact failed attempt ID")
    if not isinstance(resolution["reason"], str) or not resolution["reason"].strip():
        raise TaskflowError("repair capacity continuation reason required")
    ref = validate_evidence([resolution["authority"]])[0]
    authority = _read(Path(ref["path"]))
    digest = sha256(canonical({**resolution, "authority": ref}))
    with ExitStack() as locks:
        locks.enter_context(_consumer_lock(store))
        dependent_ids = sorted(item["flow_id"] for item in store.list_flows()
                               if item["definition"].get("source_author_flow_id") == parent_id)
        for identity in sorted({parent_id, *dependent_ids}):
            locks.enter_context(_lifecycle_lock(store, identity))
        parent, child = store.get_flow(parent_id), store.get_flow(child_id)
        previous = child["state"].get("repair_capacity_continuation")
        if previous is not None:
            _require(previous.get("resolution_digest") == digest and previous.get("parent_flow_id") == parent_id,
                     "a different original continuation already exists")
            validate_evidence([previous["authority"], previous["direct_user_evidence"], previous["failed_manifest"],
                               previous["failed_collection"], previous["failed_trace"],
                               previous["previous_child_receipt"], previous["previous_parent_receipt"]])
            _validate_prepared_witness(store, child_id, previous)
            return child
        proof = _proof(store, parent, child, resolution["failed_attempt_id"], authority)
        archive = archive_full_attempt(Path(store.get_attempt(proof["failed_attempt_id"])["runtime_path"]),
                                       Path(child["definition"]["directory"]) / "archives" / proof["failed_attempt_id"])
        proof.update(failed_archive=archive["path"], authority=ref, resolution_digest=digest)
        state = dict(child["state"])
        if archive["path"] not in state["archives"]:
            state["archives"] = [*state["archives"], archive["path"]]
        state["history"] = [*state["history"], {"kind": KIND, "binding": proof,
                             "previous_exception": state["exception"], "new_model_dispatched": False}]
        state["resolved_exceptions"] = [*state.get("resolved_exceptions", []),
            {"exception": state["exception"], "receipt": state["receipt"], "repair_capacity_continuation": proof}]
        state.update(status="active", stage="repair", step=state["step"] + 1, current_work_id=None,
                     receipt_pending=False, repair_capacity_continuation=proof,
                     repair_capacity_attempt_ordinal=1)
        for key in ("exception", "receipt", "current_node_id", "current_runtime"):
            state.pop(key, None)
        scope = "work:" + proof["new_work_id"]
        from taskflow.execution_public_continuation import _no_formal

        _no_formal(store, parent)
        validate_evidence([proof["authority"], proof["direct_user_evidence"], proof["failed_manifest"],
                           proof["failed_collection"], proof["failed_trace"],
                           proof["previous_child_receipt"], proof["previous_parent_receipt"]])
        with store._db(True) as db:
            current = store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (child_id,)).fetchone())
            parent_now = store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (parent_id,)).fetchone())
            _require(current == child and parent_now == parent, "parent or child changed while validating original")
            for dependent_id, revision in proof["dependent_revisions"].items():
                dependent = store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (dependent_id,)).fetchone())
                _require(dependent["revision"] == revision and dependent["state"].get("status") in {"active", "exception"},
                         "dependent experiment changed or left its blocked source boundary")
            rows = db.execute("SELECT flow_id,definition FROM execution_flows").fetchall()
            exact = {row["flow_id"] for row in rows
                     if json.loads(row["definition"]).get("source_author_flow_id") == parent_id}
            _require(exact == set(proof["dependent_revisions"]), "dependent experiment set changed")
            formal = [json.loads(row["data"]) for table in ("work", "attempts")
                      for row in db.execute("SELECT data FROM " + table + " WHERE task_id=?", (child["definition"]["task_id"],))]
            _require(not any(row.get("role") in {"solver", "review-facts", "review-score"} for row in formal),
                     "formal work or attempt appeared while preparing recovery")
            _require(db.execute("SELECT 1 FROM work WHERE id=? OR intent_id=?", (proof["new_work_id"], proof["new_work_id"])).fetchone() is None
                     and db.execute("SELECT 1 FROM attempts WHERE work_id=?", (proof["new_work_id"],)).fetchone() is None,
                     "new repair was already prepared or attempted")
            active = db.execute("SELECT 1 FROM attempts WHERE task_id=? AND state IN ('claimed','starting','running','collecting','uncertain')", (child["definition"]["task_id"],)).fetchone()
            _require(active is None, "another same-task attempt is active or uncertain")
            old = db.execute("SELECT data FROM controls WHERE scope=?", (scope,)).fetchone()
            if old is not None:
                value = json.loads(old["data"])
                _require(value.get("scope") == scope and value.get("action") == "pause"
                         and value.get("applies_to") == "future_claims",
                         "future repair control already exists but is not paused")
            else:
                control = {"scope": scope, "action": "pause", "reason": "explicit bounded repair continuation awaits controlled release", "applies_to": "future_claims"}
                db.execute("INSERT INTO controls VALUES(?,?)", (scope, canonical(control).decode()))
                store._event(db, "control", control)
            transition = {"kind": "workflow_repair_capacity_continuation_prepared", "binding": proof,
                          "issued_work_id": proof["new_work_id"]}
            seq = store._event(db, "flow_updated", {"flow_id": child_id, "previous_revision": child["revision"],
                "revision": child["revision"] + 1, "state": state, "transition": transition})
            binding = {"flow_id": child_id, "proof_digest": sha256(canonical(proof)), "ordinal": 1,
                       "issued_event_seq": seq, "task_id": proof["candidate_binding"]["task_id"],
                       "candidate_id": proof["candidate_binding"]["candidate_id"], "role": "repair"}
            db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (proof["new_work_id"], canonical(binding).decode()))
            updated = db.execute("UPDATE execution_flows SET state=?,revision=?,event_seq=? WHERE flow_id=? AND revision=?",
                (canonical(state).decode(), child["revision"] + 1, seq, child_id, child["revision"]))
            _require(updated.rowcount == 1, "child CAS conflict")
            return store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (child_id,)).fetchone())


def validate_repair_capacity_result(store, record, attempt):
    """Each replacement must retain its exact source and a fresh identity."""
    proof = record["state"].get("repair_capacity_continuation")
    if proof is None:
        return
    _validate_prepared_witness(store, record["flow_id"], proof)
    ordinal = record["state"].get("repair_capacity_attempt_ordinal")
    expected_work_id = _retry_work_id(record, proof, ordinal)
    if attempt["work_id"] != expected_work_id:
        previous = [store.get_attempt(row["attempt_id"]) for row in record["state"].get("history", [])
                    if row.get("kind") == "actual_repair" and row.get("attempt_id")]
        previous = [row for row in previous if row["work_id"] == expected_work_id]
        _require(len(previous) == 1, "later repair lacks the original successful replacement")
        attempt = previous[0]
    root, manifest, collected = _actual(store, attempt)
    _validate_retry_manifest(store, record, proof, manifest, ordinal)
    original = store.get_attempt(proof["failed_attempt_id"])
    _, original_manifest, original_collection = _actual(store, original)
    model, cli = _profile(manifest)
    old_model, old_cli = _profile(original_manifest)
    prior_sids = {row.get("cli_sid") for row in record["state"].get("history", [])
                  if row.get("cli_sid") and row.get("attempt_id") != attempt["attempt_id"]}
    _require(attempt["work_id"] == attempt["intent_id"] == expected_work_id
             and attempt["role"] == "repair" and attempt["state"] == "completed"
             and attempt["candidate_id"] == proof["candidate_binding"]["candidate_id"]
             and attempt["policy_id"] == original["policy_id"] and attempt.get("cli_sid")
             and attempt["cli_sid"] != proof["failed_sid"]
             and attempt["cli_sid"] != proof["author_sid"] and manifest.get("resume_sid") is None
             and attempt["cli_sid"] not in prior_sids
             and manifest.get("mode") == "native_role" and collected.get("nature") == "real_cli_attempt"
             and all(collected.get(k) is True for k in ("technical_accepted", "model_completed", "delivery_complete", "process_ended"))
             and collected["termination"].get("returncode") == 0
             and _time(collected) > _time(original_collection)
             and cli.resolve() == old_cli.resolve() and _info(cli) == _info(old_cli)
             and all(model.get(k) == old_model.get(k) for k in ("requested", "reasoning_effort"))
             and len(store.list_attempts(expected_work_id)) == 1,
             "replacement result differs from the current fresh authorized repair")
    validate_evidence([proof["authority"], proof["failed_manifest"], proof["failed_collection"],
                       proof["failed_trace"],
                       proof["previous_child_receipt"], proof["previous_parent_receipt"], proof["direct_user_evidence"]])


def resolved_repair_capacity_dependency(store, parent, previous):
    """Recognize only the prepared, paused replacement before it is dispatched."""
    child_id = parent["state"].get("child_flow_id")
    child = store.get_flow(child_id)
    state = child["state"]
    proof = state.get("repair_capacity_continuation") or {}
    _validate_prepared_witness(store, child_id, proof)
    _require(state.get("status") == "active" and state.get("stage") == "repair"
             and state.get("pending_business_scope") is None and proof.get("kind") == KIND
             and proof.get("parent_flow_id") == parent["flow_id"]
             and previous == parent["state"].get("exception")
             and previous.get("child_receipt") == proof.get("previous_child_receipt", {}).get("path")
             and state.get("current_work_id") in (None, proof.get("new_work_id")),
             "the original dependency has no paused bounded-retry continuation")
    with store._db() as db:
        event = db.execute("SELECT kind,data FROM events WHERE seq=?", (proof.get("child_event_seq"),)).fetchone()
        control = db.execute("SELECT data FROM controls WHERE scope=?", ("work:" + proof["new_work_id"],)).fetchone()
        attempts = db.execute("SELECT 1 FROM attempts WHERE work_id=?", (proof["new_work_id"],)).fetchone()
    body = json.loads(event["data"]) if event else {}
    _require(event is not None and event["kind"] == "flow_updated"
             and body.get("flow_id") == child_id and body.get("revision") == proof.get("child_revision")
             and control is not None and json.loads(control["data"]).get("action") == "pause"
             and attempts is None, "original event, paused control or zero-attempt boundary differs")
    old_child = {**child, "revision": proof["child_revision"], "event_seq": proof["child_event_seq"], "state": body["state"]}
    authority = _read(Path(proof["authority"]["path"]))
    actual = _proof(store, parent, old_child, proof["failed_attempt_id"], authority)
    _require(all(proof.get(k) == v for k, v in actual.items()), "capacity source proof changed")
    validate_evidence([proof["authority"], proof["failed_manifest"], proof["failed_collection"],
                       proof["failed_trace"],
                       proof["previous_child_receipt"], proof["previous_parent_receipt"]])
    return {"flow_id": child_id, "revision": child["revision"], "status": state["status"],
            "repair_capacity_resolution": proof}


def validate_repair_capacity_before_dispatch(store, record, manifest=None):
    """Recheck original evidence before preparing or dispatching a retry.

    The workflow and worker call this while holding the relevant lifecycle
    locks. The worker retains those locks while the attempt is processed.
    """
    state = record["state"]
    proof = state.get("repair_capacity_continuation")
    if proof is None:
        return
    _validate_prepared_witness(store, record["flow_id"], proof)
    parent = store.get_flow(proof["parent_flow_id"])
    _require(parent["state"].get("status") == "exception" and parent["state"].get("stage") == "child_quality"
             and parent["state"].get("pending_business_scope") is None
             and parent["revision"] == proof["parent_revision"]
             and parent["event_seq"] == proof["parent_received_event"]["event_seq"]
             and parent["state"].get("receipt") == proof["previous_parent_receipt"]["path"]
             and parent["state"].get("child_flow_id") == record["flow_id"],
             "original parent exception or its received cursor changed")
    with store._db() as db:
        parent_event = db.execute("SELECT kind,data FROM events WHERE seq=?", (proof["parent_received_event"]["event_seq"],)).fetchone()
        child_event = db.execute("SELECT kind,data FROM events WHERE seq=?", (proof["child_event_seq"],)).fetchone()
    _require(parent_event is not None and child_event is not None
             and parent_event["kind"] == child_event["kind"] == "flow_updated", "original receipt events are missing")
    pbody, cbody = json.loads(parent_event["data"]), json.loads(child_event["data"])
    _require(pbody.get("flow_id") == parent["flow_id"] and cbody.get("flow_id") == record["flow_id"]
             and pbody.get("revision") == proof["parent_revision"] and cbody.get("revision") == proof["child_revision"],
             "original receipt event identities changed")
    old_parent = {**parent, "revision": proof["parent_revision"], "event_seq": proof["parent_received_event"]["event_seq"], "state": pbody["state"]}
    old_child = {**record, "revision": proof["child_revision"], "event_seq": proof["child_event_seq"], "state": cbody["state"]}
    authority = _read(Path(proof["authority"]["path"]))
    actual = _proof(store, old_parent, old_child, proof["failed_attempt_id"], authority)
    _require(all(proof.get(key) == value for key, value in actual.items()), "original capacity source changed")
    validate_evidence([proof["authority"], proof["direct_user_evidence"], proof["failed_manifest"],
                       proof["failed_collection"], proof["failed_trace"],
                       proof["previous_child_receipt"], proof["previous_parent_receipt"]])
    ordinal = state.get("repair_capacity_attempt_ordinal")
    _require(type(ordinal) is int and state.get("stage") == "repair"
             and state.get("step") == proof["initial_step"] + ordinal,
             "retry step does not match its authorized ordinal")
    expected_id = _retry_work_id(record, proof, ordinal)
    _require(state.get("current_work_id") in (None, expected_id), "current work differs from retry ordinal")
    prior = [row for row in state.get("history", []) if row.get("kind") == "repair_capacity_failed_retry"]
    _require([row.get("ordinal") for row in prior] == list(range(1, ordinal)),
             "retained failed attempts do not cover each previous retry")
    with store._db() as db:
        transitions = [json.loads(row["data"]) for row in db.execute(
            "SELECT data FROM events WHERE kind='flow_updated' AND seq> ?", (proof["child_event_seq"],))]
    used_sids = {proof["author_sid"], proof["failed_sid"]}
    used_sids.update(row.get("cli_sid") for row in state.get("history", [])
                     if row.get("kind") != "repair_capacity_failed_retry" and row.get("cli_sid"))
    for row in prior:
        matches = [event for event in transitions if event.get("flow_id") == record["flow_id"]
                   and event.get("transition", {}).get("kind") == "workflow_repair_capacity_failure_retained"
                   and event["transition"].get("failure") == row
                   and row in event.get("state", {}).get("history", [])
                   and event["state"].get("repair_capacity_attempt_ordinal") == row["ordinal"] + 1]
        _require(len(matches) == 1, "previous retry lacks its unique durable failure transition")
        validate_evidence([row["manifest"], row["collection"], row["trace"]])
        original = store.get_attempt(row["attempt_id"])
        root, original_manifest, original_collection = _actual(store, original)
        expected = _retry_work_id(record, proof, row["ordinal"])
        from taskflow.execution_batch_binding import validate_batch_source

        _require(validate_batch_source(original, original_manifest, store) is True
                 and original.get("work_id") == original.get("intent_id") == row.get("work_id") == expected
                 and original.get("state") == "failed" and original.get("cli_sid") == row.get("cli_sid")
                 and original.get("cli_sid") not in used_sids and len(store.list_attempts(expected)) == 1
                 and original_collection.get("process_ended") is True
                 and original_collection.get("model_completed") is False
                 and original_collection.get("delivery_complete") is False
                 and original_collection.get("technical_accepted") is False
                 and original_collection["termination"].get("spawn_state") == "started"
                 and original_collection["termination"].get("exception") is None
                 and original_collection["termination"].get("timed_out") is False
                 and type(original_collection["termination"].get("returncode")) is int
                 and original_collection["termination"]["returncode"] == original.get("process_exit"),
                 "a previous retry is not its sole ended capacity attempt")
        _validate_retry_manifest(store, record, proof, original_manifest, row["ordinal"])
        _validate_capacity_trace(_regular(root / "trace.jsonl"), original["cli_sid"])
        _validate_full_archive(row["archive"], expected, root)
        used_sids.add(original["cli_sid"])
    if manifest is not None:
        _validate_retry_manifest(store, record, proof, manifest, ordinal)


def advance_failed_capacity_retry(store, record, attempt):
    """Retain a proved capacity failure and advance one ordinal.

    A caller must hold the workflow consumer lock. The exact parent lifecycle
    lock is held until the transition commits. This function does not launch.
    """
    proof = record["state"].get("repair_capacity_continuation")
    _require(isinstance(proof, dict) and proof.get("parent_flow_id"), "original parent identity is missing")
    with capacity_lifecycle_locks(store, proof):
        return _advance_failed_capacity_retry_locked(store, record, attempt)


@contextmanager
def retry_claim_guard(store, worker_id, work_ids):
    """Acquire source locks before the bounded work is claimed.

    For a broad consumer, inspect eligible work in queue order; a damaged or
    locked retry cannot prevent an unrelated eligible item from being claimed.
    If no item can be claimed, surface the first damaged item's error.
    """
    if work_ids is not None and not isinstance(work_ids, list):
        raise TaskflowError("claim work_ids: list or None required")
    if work_ids is not None:
        for work_id in work_ids:
            validate_id(work_id, "claim work_id")
    errors = []
    with store._db() as db:
        global_control = db.execute("SELECT data FROM controls WHERE scope='all'").fetchone()
        if global_control is not None:
            try:
                global_action = json.loads(global_control["data"])["action"]
            except (ValueError, KeyError, TypeError) as error:
                raise TaskflowError("global claim control is malformed") from error
            if global_action != "resume":
                yield None, None
                return
        rows = []
        for row in db.execute("SELECT id,data FROM work WHERE state='ready' ORDER BY rowid"):
            if work_ids is not None and row["id"] not in work_ids:
                continue
            try:
                work = json.loads(row["data"])
                _require(isinstance(work, dict) and work.get("work_id") == row["id"],
                         "ready work identity differs from its storage key")
                if not store._blocked(db, work):
                    rows.append(work)
            except (TaskflowError, ValueError, KeyError, TypeError) as error:
                errors.append(TaskflowError("ready work or its scoped control is malformed: " + str(error)))
    for work in rows:
        try:
            with store._db() as db:
                binding = store._capacity_binding_for_work(db, work["work_id"])
                _require(work.get("repair_capacity_binding") == binding,
                         "ready work lost or changed its immutable retry issuing binding")
                from taskflow.execution_legacy_guard import assert_legacy_dispatch_allowed

                assert_legacy_dispatch_allowed(db, work)
                if binding is not None:
                    _require(work.get("role") == binding["role"] and work.get("task_id") == binding["task_id"]
                             and work.get("candidate_id") == binding["candidate_id"]
                             and work.get("intent_id") == work["work_id"],
                             "retry work role, task, candidate or intent differs from its issuing event")
                    marker = '"work_id":' + json.dumps(work["work_id"], ensure_ascii=False, separators=(",", ":"))
                    found = db.execute("SELECT data FROM events WHERE kind='dispatch_intent' AND instr(data,?)>0",
                                       (marker,)).fetchall()
                    try:
                        events = [json.loads(row["data"]) for row in found]
                    except (TypeError, ValueError) as error:
                        raise TaskflowError("retry dispatch-intent event is malformed") from error
                    _require(len(events) == 1 and canonical(events[0]) == canonical(work),
                             "retry work differs from its unique original dispatch-intent event")
        except TaskflowError as error:
            errors.append(error)
            continue
        if binding is None:
            try:
                attempt = store.claim(worker_id, [work["work_id"]])
            except WorkClaimDataError as error:
                errors.append(error)
                continue
            if attempt is not None:
                yield attempt, None
                return
            continue
        locks = ExitStack()
        try:
            record = store.get_flow(binding["flow_id"])
            proof = record["state"].get("repair_capacity_continuation")
            _require(isinstance(proof, dict) and sha256(canonical(proof)) == binding["proof_digest"]
                     and record["state"].get("status") == "active"
                     and record["state"].get("current_work_id") == work["work_id"]
                     and record["state"].get("repair_capacity_attempt_ordinal") == binding["ordinal"],
                     "retry work lacks its exact current durable flow proof")
            locks.enter_context(capacity_lifecycle_locks(store, proof))
        except RetryCapacityBusy:
            locks.close()
            continue
        except (TaskflowError, ValueError, KeyError, TypeError) as error:
            locks.close()
            errors.append(TaskflowError("repair retry flow cannot be validated: " + str(error)))
            continue
        with locks:
            try:
                latest = store.get_flow(record["flow_id"])
                _require(latest == record, "retry flow changed before its protected claim")
                from taskflow.execution_runner import _validate

                manifest = _validate(Path(work["runtime_path"]), before_run=True)
                validate_repair_capacity_before_dispatch(store, latest, manifest)
            except (TaskflowError, ValueError, OSError, KeyError) as error:
                errors.append(TaskflowError("repair retry evidence cannot be validated: " + str(error)))
                continue
            try:
                attempt = store.claim(worker_id, [work["work_id"]])
            except WorkClaimDataError as error:
                errors.append(error)
                continue
            if attempt is None:
                continue
            yield attempt, {"flow_id": record["flow_id"], "binding": binding}
            return
    if errors:
        raise errors[0]
    yield None, None


def _advance_failed_capacity_retry_locked(store, record, attempt):
    from taskflow.execution_batch_binding import validate_batch_source

    state = record["state"]
    proof = state.get("repair_capacity_continuation")
    _require(proof is not None and state.get("status") == "active", "no active repair retry budget")
    validate_repair_capacity_before_dispatch(store, record)
    ordinal = state["repair_capacity_attempt_ordinal"]
    expected = _retry_work_id(record, proof, ordinal)
    _require(state.get("current_work_id") == expected == state.get("current_node_id")
             and attempt.get("work_id") == attempt.get("intent_id") == expected
             and attempt.get("state") == "failed" and len(store.list_attempts(expected)) == 1,
             "retry is not the sole terminal attempt of this exact node")
    root, manifest, collected = _actual(store, attempt)
    _require(validate_batch_source(attempt, manifest, store) is True, "retry lost its confirmed batch source")
    _validate_retry_manifest(store, record, proof, manifest, ordinal)
    prior_sids = {proof["author_sid"], proof["failed_sid"]}
    prior_sids.update(row.get("cli_sid") for row in state.get("history", []) if row.get("cli_sid"))
    _require(attempt.get("cli_sid") and attempt["cli_sid"] not in prior_sids
             and collected.get("nature") == "real_cli_attempt" and collected.get("process_ended") is True
             and collected.get("model_completed") is False and collected.get("delivery_complete") is False
             and collected.get("technical_accepted") is False
             and collected["termination"].get("spawn_state") == "started"
             and collected["termination"].get("exception") is None
             and collected["termination"].get("timed_out") is False
             and type(collected["termination"].get("returncode")) is int
             and collected["termination"]["returncode"] == attempt.get("process_exit"),
             "retry failure is not a proved terminal incomplete original")
    _validate_capacity_trace(_regular(root / "trace.jsonl"), attempt["cli_sid"])
    archive = archive_full_attempt(root, Path(record["definition"]["directory"]) / "archives" / attempt["attempt_id"])
    failure = {"kind": "repair_capacity_failed_retry", "ordinal": ordinal, "attempt_id": attempt["attempt_id"],
               "work_id": expected, "cli_sid": attempt["cli_sid"], "archive": archive["path"],
               "manifest": evidence_ref(root / "manifest.json", "original retry manifest"),
               "collection": evidence_ref(root / "collection.json", "original retry termination and collection"),
               "trace": evidence_ref(root / "trace.jsonl", "original retry capacity failure")}
    next_state = dict(state)
    next_state["history"] = [*state["history"], failure]
    if archive["path"] not in next_state["archives"]:
        next_state["archives"] = [*next_state["archives"], archive["path"]]
    if ordinal == proof["maximum_additional_attempts"]:
        next_state.update(status="exception", receipt_pending=True,
                          exception={"reason": "authorized repair capacity retries exhausted", "work_id": expected})
    else:
        next_state.update(step=state["step"] + 1, current_work_id=None,
                          repair_capacity_attempt_ordinal=ordinal + 1)
        next_state.pop("current_node_id", None)
        next_state.pop("current_runtime", None)
    validate_repair_capacity_before_dispatch(store, record)
    with store._db(True) as db:
        current = store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (record["flow_id"],)).fetchone())
        parent = store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (proof["parent_flow_id"],)).fetchone())
        _require(current == record and parent["revision"] == proof["parent_revision"]
                 and parent["state"].get("status") == "exception"
                 and parent["event_seq"] == proof["parent_received_event"]["event_seq"],
                 "source parent or child changed before retaining retry")
        rows = db.execute("SELECT flow_id,definition,revision,state FROM execution_flows").fetchall()
        related = {row["flow_id"]: row for row in rows
                   if json.loads(row["definition"]).get("source_author_flow_id") == proof["parent_flow_id"]}
        _require(set(related) == set(proof["dependent_revisions"])
                 and all(related[key]["revision"] == revision for key, revision in proof["dependent_revisions"].items()),
                 "dependent experiment changed before retaining retry")
        formal = [json.loads(row["data"]) for table in ("work", "attempts")
                  for row in db.execute("SELECT data FROM " + table + " WHERE task_id=?", (record["definition"]["task_id"],))]
        _require(not any(row.get("role") in {"solver", "review-facts", "review-score"} for row in formal),
                 "formal work appeared while retaining retry")
        if ordinal < proof["maximum_additional_attempts"]:
            following = _retry_work_id(record, proof, ordinal + 1)
            _require(db.execute("SELECT 1 FROM work WHERE id=? OR intent_id=?", (following, following)).fetchone() is None
                     and db.execute("SELECT 1 FROM attempts WHERE work_id=?", (following,)).fetchone() is None,
                     "next retry intent already exists")
        transition = {"kind": "workflow_repair_capacity_failure_retained", "failure": failure}
        if ordinal < proof["maximum_additional_attempts"]:
            transition["issued_work_id"] = following
        seq = store._event(db, "flow_updated", {"flow_id": record["flow_id"], "previous_revision": record["revision"],
            "revision": record["revision"] + 1, "state": next_state, "transition": transition})
        if ordinal < proof["maximum_additional_attempts"]:
            binding = {"flow_id": record["flow_id"], "proof_digest": sha256(canonical(proof)), "ordinal": ordinal + 1,
                       "issued_event_seq": seq, "task_id": proof["candidate_binding"]["task_id"],
                       "candidate_id": proof["candidate_binding"]["candidate_id"], "role": "repair"}
            db.execute("INSERT INTO repair_capacity_issuance VALUES(?,?)", (following, canonical(binding).decode()))
        updated = db.execute("UPDATE execution_flows SET state=?,revision=?,event_seq=? WHERE flow_id=? AND revision=?",
            (canonical(next_state).decode(), record["revision"] + 1, seq, record["flow_id"], record["revision"]))
        _require(updated.rowcount == 1, "retry failure transition CAS conflict")
        return store._flow_value(db.execute("SELECT * FROM execution_flows WHERE flow_id=?", (record["flow_id"],)).fetchone())
