"""Explicit fresh whole reviews of a preserved, completed inconclusive node.

This advances the ordinary workflow, not an acceptance journal.  Only the new
independent role can issue a business verdict; normal review and defect gates
remain authoritative.  Locks coordinate cooperating local consumers and do
not protect against arbitrary database or filesystem writers.
"""

from contextlib import ExitStack
import copy
import json
from pathlib import Path
import re

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution_controller_finding import _TransactionStore
from taskflow.execution_flow import (
    _binding, _consumer_lock, _identity, _read, _review_metadata,
    _review_observations,
)
from taskflow.execution_flow_contracts import parse_review
from taskflow.execution_lifecycle import _lifecycle_lock
from taskflow.execution_runner import (
    RunnerError, _full_tree, _full_verify, _info, _validate, _validated_collection,
)
from taskflow.execution_store import ACTIVE, ExecutionStore, evidence_ref, validate_evidence


def _require(condition, message):
    if not condition:
        raise TaskflowError("quality continuation: " + message)


def _archive(root, destination, attempt):
    """Recheck the original full archive without publishing or replacing it."""
    binding = _read(destination / "archive-binding.json")
    tree, current = _full_tree(destination), _full_tree(root)
    files = {name: {key: value[key] for key in ("size", "sha256")}
             for name, value in tree["files"].items() if name != "archive-binding.json"}
    directories = sorted(name.rstrip("/") for name in tree["directories"] if name != ".")
    source_files = {name: {key: value[key] for key in ("size", "sha256")}
                    for name, value in current["files"].items()}
    source_directories = sorted(name.rstrip("/") for name in current["directories"] if name != ".")
    _require(not tree["special"] and not current["special"]
             and files == binding.get("files") == source_files
             and directories == binding.get("directories") == source_directories
             and binding.get("intent_id") == attempt["intent_id"]
             and binding.get("collection") == _info(root / "collection.json"),
             "original full archive identity, membership or bytes changed")
    intent = _read(destination / "supplemental-intent.json")
    _full_verify(destination / "supplemental", intent, True)
    return evidence_ref(destination / "archive-binding.json", "original full archive binding")


def _received_host_dependencies(root, attempt, reference):
    """Bind an explicit host update to one already received terminal original.

    This does not establish historical executable bytes or authorize a new
    launch. Code, policy, input and arbitrary dependency changes remain errors.
    """
    reference = validate_evidence([reference])[0]
    packet = _read(Path(reference["path"]))
    _require(set(packet) == {"schema_version", "kind", "attempt_id", "intent_id", "manifest",
                            "collection", "termination", "dependencies", "direct_user_authority"}
             and type(packet["schema_version"]) is int and packet["schema_version"] == 1
             and packet["kind"] == "received_host_dependency_transition_v1"
             and packet["attempt_id"] == attempt["attempt_id"]
             and packet["intent_id"] == attempt["intent_id"], "exact received host transition required")
    validate_evidence([packet["direct_user_authority"]])
    for field, name in (("manifest", "manifest.json"), ("collection", "collection.json"),
                        ("termination", "termination.json")):
        ref = validate_evidence([packet[field]])[0]
        _require(Path(ref["path"]) == root / name, "host transition original path differs")
    manifest = _read(root / "manifest.json")
    _require(manifest.get("mode") == "native_role", "host transition requires a native terminal original")
    allowed = {manifest["argv"][0], manifest["native"]["python"]}
    # Only standard binary runtimes on the original locked shell PATH, never
    # arbitrary scripts, policy sources, tools, worker or requested inputs.
    prefix = "shell_environment_policy.set="
    environments = [value[len(prefix):] for value in manifest["argv"]
                    if isinstance(value, str) and value.startswith(prefix)]
    _require(len(environments) == 1, "one original shell environment required")
    paths = re.findall(r'"PATH"\s*=\s*("(?:[^"\\]|\\.)*")', environments[0])
    _require(len(paths) == 1, "one original locked shell PATH required")
    directories = json.loads(paths[0]).split(":")
    allowed.update(name for name in manifest["dependencies"]
                   if (Path(name).name == "node" or re.fullmatch(r"python3(?:\.\d+)?", Path(name).name))
                   and str(Path(name).parent) in directories)
    changes = packet["dependencies"]
    _require(isinstance(changes, dict) and bool(changes) and set(changes) <= allowed,
             "only explicit original host runtime changes are supported")
    for name, item in changes.items():
        path = Path(name)
        _require(set(item) == {"before", "after"} and item["before"] == manifest["dependencies"].get(name)
                 and item["after"] != item["before"] and path.is_file() and not path.is_symlink()
                 and _info(path) == item["after"], "host transition dependency bytes differ")
        with path.open("rb") as handle:
            magic = handle.read(4)
        _require(magic in {b"\x7fELF", b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf",
                           b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"},
                 "host transition requires binary executables, never scripts")
    return reference, changes


def _original(store, record, host_transition=None):
    definition, state = record["definition"], record["state"]
    _require(not any(row.get("kind") == "workflow_rejected_quality_continued"
                     for row in state.get("history", [])),
             "rejected-signature successor cannot recursively request an inconclusive continuation")
    _require(definition.get("kind") in {None, "quality_repair_v1"}
             and state.get("status") == "exception"
             and state.get("stage") in {"quality", "recheck", "recheck_public"}
             and state.get("pending_business_scope") is None,
             "received inconclusive quality workflow required")
    _require(state.get("receipt") and state.get("current_node_id"), "original received node identity required")
    with store._db() as db:
        event = db.execute("SELECT kind,data FROM events WHERE seq=?", (record["event_seq"],)).fetchone()
    issued = json.loads(event["data"]) if event else {}
    _require(event is not None and event["kind"] == "flow_updated"
             and issued.get("flow_id") == record["flow_id"]
             and issued.get("revision") == record["revision"] and issued.get("state") == state
             and sha256(canonical(definition)) == record["definition_digest"],
             "original workflow issuer or immutable definition differs")
    work_id = state.get("current_work_id")
    attempts = store.list_attempts(work_id) if work_id else []
    _require(len(attempts) == 1 and attempts[0].get("state") == "completed"
             and attempts[0].get("role") == "quality",
             "one completed original quality attempt required")
    attempt = attempts[0]
    work = store.get_work(work_id)
    _require(all(attempt.get(key) == work.get(key) for key in (
        "work_id", "candidate_id", "intent_id", "task_id", "role", "policy_id", "runtime_path")),
        "original work and attempt bindings differ")
    candidate = store.get_candidate(state["candidate_id"])
    _require(attempt["candidate_id"] == candidate["candidate_id"]
             and attempt["task_id"] == definition["task_id"]
             and attempt.get("cli_sid"), "original candidate and independent SID required")
    root = Path(attempt["runtime_path"])
    changes = None
    if host_transition is not None:
        host_transition, changes = _received_host_dependencies(root, attempt, host_transition)
    try:
        manifest = _validate(root) if changes is None else _validate(root, received_dependencies=changes)
        collection = _validated_collection(root, manifest)
    except RunnerError as exc:
        raise TaskflowError("quality continuation: original sealed execution differs: " + str(exc)) from exc
    _require(collection.get("status") == "completed"
             and collection.get("technical_accepted") is True
             and collection.get("sid") == attempt["cli_sid"]
             and manifest["intent_id"] == attempt["intent_id"],
             "original sealed completed collection required")
    role = definition["roles"][state["stage"]]
    report = root / "artifacts" / role.get("report_file", "adoption-readiness.json")
    issues = root / "artifacts" / role.get("issues_file", "issues.json")
    received = {} if changes is None else {"received_dependencies": changes}
    verdict = parse_review(report, issues, candidate=candidate,
        intent_id=attempt["intent_id"], scope=role["scope"],
        metadata_aliases=_review_metadata(record, candidate, attempt, **received),
        observed_aliases=_review_observations(record, candidate, attempt, **received))
    review_id = _identity(record["flow_id"], state["current_node_id"] + "-review")
    review = store.get_review(review_id)
    with store._db() as db:
        event = db.execute("SELECT kind,data FROM events WHERE seq=?", (review["created_seq"],)).fetchone()
    _require(event is not None and event["kind"] == "review_recorded"
             and json.loads(event["data"]) == {
                 key: value for key, value in review.items() if key != "created_seq"},
             "original review issuer differs")
    _require(verdict["outcome"] == review["verdict"] == "inconclusive"
             and review["attempt_id"] == attempt["attempt_id"]
             and review["candidate_id"] == candidate["candidate_id"]
             and state.get("exception") == {
                 "reason": "independent role signed inconclusive: " + verdict["reason"],
                 "work_id": work_id}, "original native signature and received exception differ")
    receipt_ref = evidence_ref(Path(state["receipt"]), "preserved original inconclusive receipt")
    receipt = _read(Path(receipt_ref["path"]))
    _require(receipt.get("flow_id") == record["flow_id"]
             and receipt.get("status") == "exception"
             and receipt.get("exception") == state["exception"]
             and receipt.get("candidate_id") == candidate["candidate_id"]
             and receipt.get("history") == state["history"],
             "original workflow receipt differs")
    # _review_metadata verifies actual current package metadata; payload bytes
    # must also still match their captured candidate identities.
    for item in candidate.get("files", []):
        path = Path(item["path"])
        _require(not path.is_symlink() and path.is_file()
                 and sha256(path.read_bytes()) == item["sha256"],
                 "current candidate source changed: " + str(path))
    refs = [evidence_ref(report, "original independent inconclusive report"),
            evidence_ref(issues, "original independent issues"), receipt_ref,
            evidence_ref(root / "collection-lock.json", "original sealed collection identity")]
    destination = Path(definition["directory"]) / "archives" / attempt["attempt_id"]
    _require(str(destination) in state["archives"], "registered original full archive required")
    refs.append(_archive(root, destination, attempt))
    if host_transition is not None:
        refs.append(host_transition)
    source = {"flow_id": record["flow_id"], "revision": record["revision"],
            "definition_digest": record["definition_digest"],
            "candidate_binding": _binding(candidate), "attempt_id": attempt["attempt_id"],
            "cli_sid": attempt["cli_sid"], "review_id": review_id,
            "exception": state["exception"], "evidence": refs}
    if host_transition is not None:
        source["host_dependency_transition"] = host_transition
    return source


def inspect_inconclusive_workflow(store, flow_id: str, *, host_transition=None) -> dict:
    """Describe the exact original; no model, gate or ledger mutation."""
    with _lifecycle_lock(store, flow_id), _consumer_lock(store):
        return _original(store, store.get_flow(flow_id), host_transition)


REJECTED_OBSERVATION_SCOPE = "Observed paths are only for nonblocking claims, not repair targets"


def _rejected_original(store, record):
    """Verify the exact unreceived signed observation-scope exception, never its verdict."""
    definition, state = record["definition"], record["state"]
    _require(state.get("status") == "exception" and state.get("stage") == "quality"
             and state.get("step") == 0 and not state.get("defects") and not state.get("repaired_defects")
             and not any(row.get("kind") in {"workflow_rejected_quality_continued", "workflow_inconclusive_continued"}
                         for row in state.get("history", []))
             and state.get("pending_business_scope") is None and state.get("receipt")
             and state.get("exception") == {"reason": REJECTED_OBSERVATION_SCOPE,
                                             "work_id": state.get("current_work_id")},
             "exact rejected observation-scope exception required")
    with store._db() as db:
        event = db.execute("SELECT kind,data FROM events WHERE seq=?", (record["event_seq"],)).fetchone()
    issued = json.loads(event["data"]) if event else {}
    _require(event is not None and event["kind"] == "flow_updated"
             and issued.get("flow_id") == record["flow_id"]
             and issued.get("revision") == record["revision"] and issued.get("state") == state
             and sha256(canonical(definition)) == record["definition_digest"], "original workflow issuer differs")
    work_id = state["current_work_id"]
    attempts = store.list_attempts(work_id)
    _require(len(attempts) == 1 and attempts[0].get("state") == "completed"
             and attempts[0].get("role") == "quality" and attempts[0].get("cli_sid"),
             "one completed original quality attempt required")
    attempt, work = attempts[0], store.get_work(work_id)
    _require(all(attempt.get(key) == work.get(key) for key in
                 ("work_id", "candidate_id", "intent_id", "task_id", "role", "policy_id", "runtime_path")),
             "original work and attempt differ")
    with store._db() as db:
        rows = db.execute("SELECT data FROM reviews").fetchall()
    _require(not any(json.loads(row["data"]).get("attempt_id") == attempt["attempt_id"] for row in rows),
             "rejected original must not already be an accepted review record")
    candidate = store.get_candidate(state["candidate_id"])
    _require(attempt["candidate_id"] == candidate["candidate_id"]
             and attempt["task_id"] == definition["task_id"], "original candidate differs")
    role = definition["roles"]["quality"]
    _require(role.get("scope") == "whole_package"
             and role["runner"].get("public_first_initial_listing") == {
                 "scope": "whole_package", "index": "execution/public-inputs.json",
                 "checkpoint": "public-responsibility-first.json"}
             and not role["runner"].get("resume_sid"), "fresh public-first whole quality required")
    root = Path(attempt["runtime_path"])
    try:
        manifest = _validate(root)
        collected = _validated_collection(root, manifest)
    except RunnerError as exc:
        raise TaskflowError("quality continuation: original sealed execution differs: " + str(exc)) from exc
    _require(collected.get("status") == "completed" and collected.get("technical_accepted") is True
             and collected.get("sid") == attempt["cli_sid"] and manifest["intent_id"] == attempt["intent_id"],
             "original completed collection differs")
    report = root / "artifacts" / role.get("report_file", "adoption-readiness.json")
    issues = root / "artifacts" / role.get("issues_file", "issues.json")
    metadata = _review_metadata(record, candidate, attempt)
    observed = _review_observations(record, candidate, attempt)
    try:
        parse_review(report, issues, candidate=candidate, intent_id=attempt["intent_id"],
                     scope="whole_package", metadata_aliases=metadata, observed_aliases=observed)
    except TaskflowError as exc:
        _require(str(exc) == REJECTED_OBSERVATION_SCOPE, "original intake error changed")
    else:
        raise TaskflowError("quality continuation: original report is not rejected on the stated boundary")
    receipt_ref = evidence_ref(Path(state["receipt"]), "original rejected quality intake receipt")
    receipt = _read(Path(receipt_ref["path"]))
    _require(receipt.get("flow_id") == record["flow_id"] and receipt.get("status") == "exception"
             and receipt.get("exception") == state["exception"]
             and receipt.get("candidate_id") == candidate["candidate_id"]
             and receipt.get("history") == state["history"], "original rejection receipt differs")
    for item in candidate.get("files", []):
        path = Path(item["path"])
        _require(not path.is_symlink() and path.is_file() and sha256(path.read_bytes()) == item["sha256"],
                 "current candidate source changed: " + str(path))
    destination = Path(definition["directory"]) / "archives" / attempt["attempt_id"]
    _require(str(destination) in state["archives"], "registered original full archive required")
    refs = [evidence_ref(report, "original unreceived independent report"),
            evidence_ref(issues, "original unreceived independent issues"), receipt_ref,
            evidence_ref(root / "collection-lock.json", "original sealed collection identity"),
            _archive(root, destination, attempt),
            evidence_ref(root / "artifacts" / "intake.md", "original unreceived intake")]
    return {"flow_id": record["flow_id"], "revision": record["revision"],
            "definition_digest": record["definition_digest"], "candidate_binding": _binding(candidate),
            "attempt_id": attempt["attempt_id"], "cli_sid": attempt["cli_sid"],
            "exception": state["exception"], "evidence": refs}


def inspect_rejected_quality_workflow(store, flow_id: str) -> dict:
    with _lifecycle_lock(store, flow_id), _consumer_lock(store):
        return _rejected_original(store, store.get_flow(flow_id))


def continue_rejected_quality_workflow(store, flow_id: str, *, authorization: dict) -> dict:
    return continue_inconclusive_workflow(store, flow_id, authorization=authorization, rejected=True)


def continue_inconclusive_workflow(store, flow_id: str, *, authorization: dict, rejected=False) -> dict:
    """Authorize exactly one new whole-review node, without preparing a model.

    The packet binds the inspected source and controller plus a direct human
    authority reference. Replays return the original transition; a different
    source needs a different explicit packet. Old signatures are immutable.
    """
    reference = validate_evidence([authorization])[0]
    packet = _read(Path(reference["path"]))
    transition_kind = "workflow_rejected_quality_continued" if rejected else "workflow_inconclusive_continued"
    packet_kind = "continue_rejected_quality_v1" if rejected else "continue_inconclusive_quality_v1"
    _require((not rejected or "serial_review_plan" not in packet)
             and set(packet) - {"serial_review_plan"} == {"schema_version", "kind", "flow_id", "scope", "purpose", "controller", "source", "direct_user_authority"}
             and type(packet.get("schema_version")) is int and packet["schema_version"] == 1
             and packet.get("kind") == packet_kind
             and packet.get("flow_id") == flow_id
             and packet.get("scope") == "whole_package"
             and isinstance(packet.get("purpose"), str) and packet["purpose"].strip(),
             "explicit source-bound whole-review packet required")
    human = validate_evidence([packet["direct_user_authority"]])[0]
    with ExitStack() as locks:
        locks.enter_context(_lifecycle_lock(store, flow_id))
        locks.enter_context(_consumer_lock(store))
        with store._db(True) as db:
            tx = _TransactionStore(store, db)
            record = tx.get_flow(flow_id)
            _require(packet.get("controller") == record["definition"]["owner"],
                     "registered controller differs")
            prior = [row for row in record["state"].get("history", [])
                     if row.get("kind") == transition_kind
                     and row.get("authorization") == reference]
            if prior:
                _require(len(prior) == 1 and prior[0]["source"] == packet.get("source")
                         and prior[0]["direct_user_authority"] == human,
                         "conflicting previous continuation")
                validate_evidence([reference, human, *prior[0]["source"]["evidence"]])
                _require(prior[0].get("serial_review_plan") == packet.get("serial_review_plan"),
                         "conflicting previous serial reading plan")
                if packet.get("serial_review_plan") is not None:
                    from taskflow.execution_quality_session import validate_plan

                    validate_plan(packet["serial_review_plan"], prior[0]["source"]["candidate_binding"])
                return record
            if rejected:
                _require(not any(row.get("kind") in {"workflow_rejected_quality_continued", "workflow_inconclusive_continued"}
                                 for row in record["state"].get("history", [])),
                         "rejected-source continuation already used")
                source = _rejected_original(tx, record)
            else:
                source = _original(tx, record, packet.get("source", {}).get("host_dependency_transition"))
            _require(source == packet.get("source"), "inspected original source changed")
            task_id = record["definition"]["task_id"]
            for row in db.execute("SELECT data FROM attempts WHERE task_id=?", (task_id,)):
                attempt = json.loads(row["data"])
                _require(attempt.get("state") not in ACTIVE and attempt.get("role") != "solver",
                         "task has active/uncertain attempts or a formal solver footprint")
            for row in db.execute("SELECT data FROM work WHERE task_id=?", (task_id,)):
                work = json.loads(row["data"])
                _require(work.get("state") in {"completed", "failed", "cancelled"}
                         and work.get("role") != "solver",
                         "task has unresolved work or formal solver dispatch")
            for row in db.execute("SELECT data FROM events WHERE kind IN ('dispatch_intent','next_investment')"):
                event = json.loads(row["data"])
                if event.get("task_id") == task_id:
                    _require(event.get("role") != "solver" and event.get("action") != "experiment",
                             "task has a formal ledger footprint")
            definition, state = record["definition"], copy.deepcopy(record["state"])
            _require(definition["roles"]["quality"]["scope"] == "whole_package"
                     and not definition["roles"]["quality"]["runner"].get("resume_sid"),
                     "original fresh whole-quality role required")
            step = state["step"] + 1
            node_id = _identity(flow_id, str(step) + "-quality")
            node_root = Path(definition["directory"]) / "nodes" / (str(step) + "-quality")
            _require(not node_root.exists() and not node_root.is_symlink()
                     and db.execute("SELECT 1 FROM work WHERE id=? OR intent_id=?", (node_id, node_id)).fetchone() is None,
                     "new review node already exists")
            transition = {"kind": transition_kind, "source": source,
                          "authorization": reference, "direct_user_authority": human,
                          "new_work_id": node_id, "purpose": packet["purpose"],
                          "new_model_dispatched": False, "quality_accepted": False}
            state["history"].append(transition)
            state["prior_receipts"] = [*state.get("prior_receipts", []), source["evidence"][2]]
            state.update(status="active", stage="quality", step=step, current_work_id=None,
                         last_review_evidence=(source["evidence"][:2] + [source["evidence"][5]]
                                               if rejected else source["evidence"][:2]),
                         receipt_pending=False, waiting=None)
            state.pop("serial_quality_reading", None)
            if packet.get("serial_review_plan") is not None:
                from taskflow.execution_quality_reading import reading_state

                state["serial_quality_reading"] = reading_state(
                    packet["serial_review_plan"], source["candidate_binding"], step)
                transition["serial_review_plan"] = state["serial_quality_reading"]["plan"]
            # A completed capacity recovery belongs to its original failed
            # repair. Its journal/history survives; its cursor must not grant
            # retries for a later fresh review's unrelated repair.
            for key in ("exception", "receipt", "current_node_id", "current_runtime",
                        "repair_capacity_continuation", "repair_capacity_attempt_ordinal"):
                state.pop(key, None)
            validate_evidence([reference, human, *source["evidence"]])
            return tx.update_flow(flow_id, record["revision"], state, transition)


def _accepted_continuation_path(store, child, transition, review):
    """Revalidate the ordinary whole/repair/recheck path, not a formal gate."""
    state = child["state"]
    source = transition["source"]
    current = store.get_candidate(state["candidate_id"])
    for item in current.get("files", []):
        path = Path(item["path"])
        _require(not path.is_symlink() and path.is_file() and sha256(path.read_bytes()) == item["sha256"],
                 "accepted current candidate bytes changed")
    history = state["history"]
    originals = []
    finals = []
    for position, entry in enumerate(history):
        if entry.get("kind") != "independent_review":
            continue
        signed = store.get_review(entry["review_id"])
        attempt = store.get_attempt(signed["attempt_id"])
        if attempt["work_id"] == transition["new_work_id"]:
            originals.append((position, entry, signed, attempt))
        if signed["review_id"] == review["review_id"]:
            finals.append((position, entry, signed, attempt))
    _require(len(originals) == len(finals) == 1, "unique original whole and final review history required")
    start, anchor_entry, anchor, anchor_attempt = originals[0]
    end, final_entry, _, final_attempt = finals[0]
    for entry, signed, attempt in ((anchor_entry, anchor, anchor_attempt), (final_entry, review, final_attempt)):
        _require(entry.get("attempt_id") == attempt["attempt_id"]
                 and entry.get("cli_sid") == attempt["cli_sid"]
                 and entry.get("candidate_id") == signed["candidate_id"]
                 and attempt["state"] == "completed", "review history differs from its actual completed signer")
    _require(start <= end and anchor.get("whole_package") is True
             and anchor["candidate_id"] == source["candidate_binding"]["candidate_id"]
             and _binding(store.get_candidate(anchor["candidate_id"])) == source["candidate_binding"]
             and review["verdict"] == "accepted" and review["candidate_id"] == current["candidate_id"],
             "original whole or final accepted source differs")
    with store._db() as db:
        _declared_workflow_review(db, child, anchor, anchor_attempt)
        _declared_workflow_review(db, child, review, final_attempt)
        ancestors = ExecutionStore._ancestors(db, current["candidate_id"])
    _require(anchor["candidate_id"] in ancestors, "current candidate is outside the actual whole-review repair ancestry")
    repairs = []
    candidate = current
    while candidate["candidate_id"] != anchor["candidate_id"]:
        matches = [(position, row) for position, row in enumerate(history)
                   if row.get("kind") == "actual_repair" and row.get("candidate_id") == candidate["candidate_id"]]
        _require(len(matches) == 1 and start < matches[0][0] < end,
                 "each successor must be this workflow's actual intervening repair")
        position, entry = matches[0]
        attempt = store.get_attempt(entry["attempt_id"])
        _require(candidate.get("author_attempt") == attempt["attempt_id"]
                 and attempt["state"] == "completed" and attempt["role"] == "repair"
                 and attempt["candidate_id"] == candidate.get("parent_id")
                 and entry.get("cli_sid") == attempt["cli_sid"]
                 and anchor["created_seq"] < attempt["claimed_seq"]
                 and attempt["last_seq"] < final_attempt["claimed_seq"],
                 "successor is not the exact completed repair between these independent signatures")
        root = Path(attempt["runtime_path"])
        manifest = _validate(root)
        collected = _validated_collection(root, manifest)
        _require(collected.get("technical_accepted") is True and collected.get("sid") == attempt["cli_sid"],
                 "actual repair collection differs")
        archive = Path(child["definition"]["directory"]) / "archives" / attempt["attempt_id"]
        _require(str(archive) in state["archives"], "actual repair full archive missing")
        repairs.append({"candidate_id": candidate["candidate_id"], "attempt_id": attempt["attempt_id"],
                        "archive_binding": _archive(root, archive, attempt)})
        candidate = store.get_candidate(candidate["parent_id"])
    if repairs:
        _require(anchor["verdict"] == "defects" and start < end,
                 "a later repair must originate from this whole review's actual findings")
    else:
        _require(anchor == review and review.get("whole_package") is True,
                 "unchanged candidate requires its original accepted whole signature")
    closed = state.get("repaired_defects", [])
    _require(set(review.get("covered_defect_ids", [])) == set(closed),
             "final independent signature must explicitly cover every current repaired responsibility")
    with store._db() as db:
        for identity in closed:
            defect = store.get_defect(identity)
            _require(defect["state"] == "closed" and defect.get("closure_review_id") == review["review_id"]
                     and defect.get("repair_candidate_id") in ancestors,
                     "repaired responsibility lacks this exact current independent closure")
            ExecutionStore._assert_closed_quality_record(db, defect)
            validate_evidence([*defect["repair_evidence"], *defect["closure_evidence"]])
            if not review["whole_package"]:
                chain = defect.get("closure_coverage_chain")
                if chain:
                    actual = ExecutionStore._closure_coverage_chain_material(
                        db, defect, chain["repair_review_id"], review["review_id"])
                    _require(actual == chain, "ordinary whole/affected closure coverage chain changed")
                else:
                    original = store.get_candidate(defect["candidate_id"])
                    _require(defect["kind"] != "material"
                             and current["input_digest"] == original["input_digest"]
                             and final_attempt["claimed_seq"] > defect["repair_seq"],
                             "affected closure lacks original same-public private repair scope and ordering")
        if repairs:
            findings = [json.loads(row["data"])["defect_id"] for row in db.execute("SELECT data FROM defects")
                        if json.loads(row["data"]).get("review_id") == anchor["review_id"]]
            _require(findings and set(findings) <= set(closed), "whole findings are not explicitly repaired and closed")
    return {"whole_review_id": anchor["review_id"], "final_review_id": review["review_id"],
            "accepted_candidate_binding": _binding(current), "repairs": list(reversed(repairs)),
            "closed_defect_ids": list(closed), "quality_gate_authorized": False}


def _declared_workflow_review(db, child, review, attempt):
    """Use the immutable role's filenames, never a copied/default fallback."""
    definition = child["definition"]
    _require(sha256(canonical(definition)) == child["definition_digest"], "workflow role definition changed")
    root = Path(attempt["runtime_path"])
    try:
        parts = root.relative_to(Path(definition["directory"]) / "nodes").parts
    except ValueError as exc:
        raise TaskflowError("quality continuation: signer runtime is outside its workflow") from exc
    _require(len(parts) == 2 and parts[1] == "runtime" and "-" in parts[0],
             "exact original workflow node runtime required")
    stage = parts[0].split("-", 1)[1]
    role = definition["roles"].get(stage)
    expected_scope = "whole_package" if review["whole_package"] else "affected"
    _require(stage in {"quality", "recheck", "recheck_public"} and isinstance(role, dict)
             and role.get("scope") == expected_scope
             and attempt["intent_id"] == _identity(child["flow_id"], parts[0])
             and review["scope"] == role.get("scope_id", child["flow_id"] + ".authorized-quality"),
             "original node role, scope or intent differs")
    report_file = role.get("report_file", "adoption-readiness.json")
    issues_file = role.get("issues_file", "issues.json")
    if (report_file, issues_file) == ("adoption-readiness.json", "issues.json"):
        return ExecutionStore._composition_review(db, review, expected_scope)
    return ExecutionStore._composition_review(db, review, expected_scope,
        report_file=report_file, issues_file=issues_file)


def resolved_inconclusive_dependency(store, parent: dict, previous: dict) -> dict:
    """Receive the ordinary accepted whole or repaired path for this parent.

    Earlier technical exceptions may have propagated before the inconclusive
    node; their original child receipt must remain in the child's resolution
    history. This never changes the child's verdict or authorizes a solver.
    """
    definition, state = parent["definition"], parent["state"]
    _require(definition.get("kind") in {"author_quality_v1", "baseline_quality_v1"}
             and previous.get("stage") == "child_quality"
             and previous.get("child_receipt") and state.get("child_flow_id"),
             "original propagated child-quality exception required")
    child = store.get_flow(state["child_flow_id"])
    child_definition, current = child["definition"], child["state"]
    _require(current.get("status") == "accepted" and current.get("receipt")
             and all(child_definition.get(key) == definition.get(key) for key in ("task_id", "owner", "policy_id"))
             and child_definition["initial"]["candidate_id"] == state["candidate_id"],
             "same-owner original child must actually accept its review path")
    transitions = [row for row in current.get("history", [])
                   if row.get("kind") in {"workflow_inconclusive_continued", "workflow_rejected_quality_continued"}]
    review = store.assert_review_current(current["accepted_review_id"])
    _require(bool(transitions), "explicit original continuation required")
    transition = transitions[-1]
    source = transition["source"]
    try:
        path = _accepted_continuation_path(store, child, transition, review)
    except RunnerError as exc:
        raise TaskflowError("quality continuation: accepted repair path sealed execution differs: " + str(exc)) from exc
    prior = evidence_ref(Path(previous["child_receipt"]), "original propagated child exception")
    received = _read(Path(prior["path"]))
    _require(received.get("flow_id") == child["flow_id"]
             and received.get("status") == "exception"
             and received.get("exception") == previous.get("reason"),
             "parent's original child receipt differs")
    retained = [row.get("receipt") for row in current.get("resolved_exceptions", [])]
    retained += [row.get("path") for row in current.get("prior_receipts", [])]
    _require(prior["path"] in retained, "propagated child receipt was not retained by the continuation")
    with store._db() as db:
        events = db.execute("SELECT data FROM events WHERE kind='flow_updated' "
            "AND json_extract(data,'$.flow_id')=? "
            "AND json_extract(data,'$.transition.kind')=? "
            "AND json_extract(data,'$.transition.new_work_id')=?", (child["flow_id"], transition["kind"], transition["new_work_id"])).fetchall()
    _require(len(events) == 1 and json.loads(events[0]["data"])["transition"] == transition,
             "continuation issuer differs from its committed transition")
    validate_evidence([prior, transition["authorization"], transition["direct_user_authority"], *source["evidence"]])
    proof = {"source": source, "authorization": transition["authorization"],
             "new_work_id": transition["new_work_id"], "accepted_review_id": review["review_id"],
             "accepted_path": path}
    proof_key = ("rejected_quality_continuation" if transition["kind"] == "workflow_rejected_quality_continued"
                 else "inconclusive_quality_continuation")
    return {"flow_id": child["flow_id"], "revision": child["revision"], "status": "accepted",
            "previous_child_receipt": prior, proof_key: proof}
