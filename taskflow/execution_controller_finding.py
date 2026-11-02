"""Receive an authorized public finding at an accepted source's natural boundary.

The affected review remains its original accepted signature. The new defect is
explicitly attributed to the controller, and only the normal repair chain moves.
"""

from contextlib import ExitStack, contextmanager
import copy
import json
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, parse_json, read_json, relative_path, safe_read, sha256, validate_id
from taskflow.execution_store import ACTIVE, ExecutionStore, evidence_ref, validate_evidence


def _require(value, message):
    if not value:
        raise TaskflowError(message)


def _binding(candidate):
    return {key: candidate[key] for key in
            ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}


def _accepted_receipt(record):
    state = record["state"]
    _require(state.get("status") == "accepted" and state.get("current_work_id") is None
             and state.get("pending_business_scope") is None and state.get("exception") is None
             and state.get("receipt"), "controller finding requires a received accepted natural boundary")
    ref = evidence_ref(Path(state["receipt"]), "previous accepted result retained after controller finding")
    raw = read_json(Path(ref["path"]))
    _require(raw.get("flow_id") == record["flow_id"] and raw.get("status") == "accepted"
             and raw.get("controller") == record["definition"]["owner"]
             and raw.get("history") == state.get("history") and raw.get("archives") == state.get("archives"),
             "accepted controller finding source receipt differs from its original state")
    if record["definition"].get("kind") == "author_quality_v1":
        _require(raw.get("result") == state.get("result"), "accepted parent result receipt differs")
    else:
        _require(raw.get("candidate_id") == state.get("candidate_id"), "accepted child candidate receipt differs")
    return ref


class _TransactionStore(ExecutionStore):
    """Use existing public store checks inside one owner-held SQLite transaction."""

    def __init__(self, original, connection):
        self.path, self.connection = original.path, connection

    @contextmanager
    def _db(self, write=False):
        yield self.connection


def _prior_state(record, receipt, review_id):
    return {"flow_id": record["flow_id"], "revision": record["revision"],
            "definition_digest": record["definition_digest"], "receipt": receipt,
            "review_id": review_id, "event_seq": record["event_seq"],
            "state_digest": sha256(canonical(record["state"])),
            "closed_defect_ids": list(record["state"].get("repaired_defects", []))}


def reopen_controller_public_finding(store, *, parent_flow_id: str, controller: str,
                                     finding_ref: dict, expected_parent_revision: int,
                                     expected_child_revision: int, expected_experiment_revision: int,
                                     expected_candidate_binding: dict, expected_review_id: str) -> dict:
    """Atomically register one material finding and reopen its original source.

    The controller-received report must explicitly identify its source flow,
    candidate, held experiment and exact current public quotes. No model review,
    formal purpose, definition, request or existing result is replaced here.
    """
    from taskflow.execution_experiment import _node, _packet, _run_id
    from taskflow.execution_flow import _consumer_lock, _identity
    from taskflow.execution_lifecycle import _lifecycle_lock
    from taskflow.packages import check_task

    validate_id(parent_flow_id, "parent_flow_id")
    validate_id(controller, "controller")
    validate_id(expected_review_id, "expected_review_id")
    for revision in (expected_parent_revision, expected_child_revision, expected_experiment_revision):
        _require(type(revision) is int and revision >= 0, "explicit original revisions required")
    source = validate_evidence([finding_ref])[0]
    source_path = Path(source["path"])
    source_bytes = safe_read(Path(source_path.anchor), str(source_path.relative_to(source_path.anchor)))
    _require(sha256(source_bytes) == source["sha256"], "controller finding bytes changed before parsing")
    report = parse_json(source_bytes, "controller finding")
    _require(isinstance(report, dict) and type(report.get("schema_version")) is int and report["schema_version"] == 1,
             "controller finding schema_version=1 required")
    finding_id = validate_id(report.get("finding_id"), "controller finding_id")
    for key in ("root_business_decision", "monitor_requested_disposition", "source_role", "source_thread_id"):
        _require(isinstance(report.get(key), str) and report[key].strip(), "explicit controller finding attribution/disposition required")
    _require(report.get("source_flow") == parent_flow_id, "controller finding belongs to another source")
    experiment_id = validate_id(report.get("experiment_flow"), "held experiment_flow")
    original_parent = store.get_flow(parent_flow_id)
    child_id = original_parent["state"].get("child_flow_id")
    child_result = original_parent["state"].get("result", {}).get("child_flow_id")
    previous_child = any(row.get("kind") == "controller_finding_received"
                         and row.get("finding_origin", {}).get("child_flow_id") == child_id
                         for row in original_parent["state"].get("history", []))
    _require(isinstance(child_id, str) and (child_id == child_result or previous_child),
             "accepted parent has no exact original child")
    operation_id = "controller-finding-" + sha256(canonical({
        "parent_flow_id": parent_flow_id, "controller": controller,
        "finding": source, "candidate_binding": expected_candidate_binding,
        "review_id": expected_review_id}))[:24]
    with ExitStack() as locks:
        for identity in sorted({parent_flow_id, child_id, experiment_id}):
            locks.enter_context(_lifecycle_lock(store, identity))
        locks.enter_context(_consumer_lock(store))
        with store._db(True) as connection:
            tx = _TransactionStore(store, connection)
            parent, child, experiment = (tx.get_flow(identity) for identity in (parent_flow_id, child_id, experiment_id))
            existing = [row for row in child["state"].get("history", [])
                        if row.get("kind") == "controller_finding_received" and row.get("operation_id") == operation_id]
            if existing:
                _require(len(existing) == 1 and any(row.get("operation_id") == operation_id
                         for row in parent["state"].get("history", [])), "controller finding has incomplete prior registration")
                return {"status": "already_registered", "operation_id": operation_id,
                        "parent": parent, "child": child, "models_dispatched": 0}
            _require(parent["revision"] == expected_parent_revision and child["revision"] == expected_child_revision
                     and experiment["revision"] == expected_experiment_revision, "controller finding revision conflict")
            _require(parent["definition"].get("kind") == "author_quality_v1"
                     and all(flow["definition"].get("owner") == controller for flow in (parent, child, experiment))
                     and parent["definition"]["policy_id"] == child["definition"]["policy_id"]
                     and experiment["definition"].get("source_author_flow_id") == parent_flow_id,
                     "controller finding owner or source/experiment identity differs")
            previous_parent, previous_child = _accepted_receipt(parent), _accepted_receipt(child)
            roles = child["definition"].get("roles", {})
            _require({"repair", "public_rebuild", "recheck_public"} <= set(roles)
                     and roles["repair"].get("scope") == "affected"
                     and roles["recheck_public"].get("scope") == "whole_package",
                     "original child has no authorized material-repair/public/whole route")
            candidate = tx.get_candidate(child["state"]["candidate_id"])
            _require(all(flow["definition"].get("task_id") == candidate["task_id"]
                     for flow in (parent, child, experiment)), "controller source/child/experiment task scope differs")
            review_id = child["state"]["accepted_review_id"]
            _require(_binding(candidate) == expected_candidate_binding and review_id == expected_review_id
                     and report.get("candidate_id") == candidate["candidate_id"], "controller finding candidate/review binding differs")
            _require(parent["state"].get("result", {}).get("candidate_id") == candidate["candidate_id"]
                     and parent["state"]["result"].get("accepted_review_id") == review_id
                     and parent["state"]["result"].get("child_receipt") == child["state"]["receipt"],
                     "parent and original accepted child results disagree")
            current = tx.assert_review_current(review_id)
            _require(current["candidate_id"] == candidate["candidate_id"] and current.get("whole_package") is True
                     and not current.get("derived") and not current.get("composite_quality_coverage"),
                     "controller public finding requires its original current whole acceptance")
            ExecutionStore._composition_review(connection, current, "whole_package")
            scope = ExecutionStore._quality_scope(connection, candidate["candidate_id"])
            unresolved = [json.loads(row["data"]) for row in connection.execute(
                "SELECT data FROM defects WHERE task_id=?", (candidate["task_id"],))
                if json.loads(row["data"])["candidate_id"] in scope and json.loads(row["data"])["state"] != "closed"]
            _require(not unresolved, "source already has an unresolved responsibility")
            declaration = [row for row in candidate["evidence"] if row.get("location") == "explicit package declaration"]
            _require(len(declaration) == 1, "controller finding candidate needs its captured declaration")
            validate_evidence(declaration)
            root = Path(declaration[0]["path"]).parent
            _require(str(root) == report.get("candidate_root") == child["state"].get("candidate_root"), "controller finding has a foreign candidate root")
            checked = check_task(root, write_report=False)
            _require(not checked["errors"] and all(checked[key] == candidate[key]
                     for key in ("task_id", "input_digest", "content_digest", "evaluation_digest")), "controller finding candidate bytes changed")
            rows = report.get("exact_public_findings")
            _require(isinstance(rows, list) and rows, "exact current public finding rows required")
            files = {row["package_path"]: row for row in candidate["files"]}
            affected, evidence = [], []
            for row in rows:
                _require(isinstance(row, dict), "controller public finding row required")
                name = relative_path(row.get("package_path"))
                item = files.get(name)
                _require(item is not None and item["role"] in PUBLIC_ROLES
                         and row.get("path") == item["path"] == str(root / name)
                         and row.get("sha256") == item["sha256"] == sha256(Path(item["path"]).read_bytes()),
                         "controller finding public path/role/current SHA differs")
                quote = row.get("quote")
                _require(isinstance(quote, str) and quote.strip()
                         and quote.encode("utf-8") in Path(item["path"]).read_bytes(), "controller finding quote is absent from its exact current source")
                if name not in affected:
                    affected.append(name)
                evidence.append({"path": item["path"], "sha256": item["sha256"],
                                 "location": "controller finding " + finding_id + ": " + quote})
            held = experiment["state"]
            _require(experiment["definition"].get("kind") == "experiment_v1"
                     and held.get("status") in {"active", "exception"} and held.get("stage") == "experiment_v1"
                     and held.get("pending_business_scope") is None and held.get("current_work_id") is None
                     and not any(held.get(key) for key in ("candidate_id", "accepted_review_id", "runs", "release"))
                     and held.get("run_index", 0) == 0, "held experiment has passed its original initial boundary")
            target = _node(experiment, "solver")
            run_id = _run_id(experiment, _packet(experiment))
            technical = experiment["definition"]["technical"]
            inner = Path(technical["formal_runtime_root"]) / run_id
            publication = Path(technical["publication_root"])
            footprints = (Path(technical["formal_runtime_root"]), inner, publication,
                          Path(experiment["definition"]["directory"]) / "runtimes" / target,
                          inner.with_name(inner.name + ".formal-intent.json"),
                          publication / "reports/runtime-reservations" / (run_id + ".json"))
            _require(not any(path.exists() or path.is_symlink() for path in footprints),
                     "held experiment has an existing formal publication or preparation")
            control_row = connection.execute("SELECT data FROM controls WHERE scope=?", ("work:" + target,)).fetchone()
            _require(control_row is not None and json.loads(control_row["data"])["action"] == "pause", "exact target purpose must remain paused")
            # Inspect only this task/intent's ledger rows; dashboard quality
            # projections would reread unrelated tasks while owning a writer.
            board = {
                "work": [json.loads(row["data"]) for row in connection.execute(
                    "SELECT data FROM work WHERE task_id=? OR id=? OR intent_id=?",
                    (candidate["task_id"], target, target + "-intent"))],
                "attempts": [json.loads(row["data"]) for row in connection.execute(
                    "SELECT data FROM attempts WHERE task_id=? OR work_id=?", (candidate["task_id"], target))],
                "decisions": [json.loads(row["data"]) for row in connection.execute(
                    "SELECT data FROM decisions WHERE task_id=? OR id=?",
                    (candidate["task_id"], _node(experiment, "experiment-decision")))],
            }
            _require(not any(row.get("work_id") == target for row in board["attempts"]), "held target purpose already has an attempt")
            _require(not any(row.get("work_id") == target or row.get("intent_id") == target + "-intent"
                     or row.get("candidate_id") == candidate["candidate_id"] and row.get("role") == "solver"
                     for row in [*board["work"], *board["attempts"]]), "held purpose has existing formal work")
            _require(not any(row.get("decision_id") == _node(experiment, "experiment-decision")
                     or row.get("candidate_id") == candidate["candidate_id"] and row.get("action") == "experiment"
                     for row in board["decisions"]), "held purpose has an existing experiment decision")
            _require(not any(row.get("task_id") == candidate["task_id"] and row.get("state") in ACTIVE
                     for row in board["attempts"]), "source task still has an active or uncertain role")
            _require(not any(row.get("finding_origin", {}).get("finding_id") == finding_id
                     for row in (tx.get_defect(row["defect_id"]) for row in child["state"].get("defects", []))), "finding already registered under another source")
            authority = {**source, "location": "controller received explicit disposition /root_business_decision"}
            origin = {"schema_version": 1, "kind": "authorized_controller_finding", "finding_id": finding_id,
                      "controller": controller, "source_role": report["source_role"], "source_thread_id": report["source_thread_id"],
                      "source": source, "json_pointer": "/exact_public_findings", "authority": authority,
                      "candidate_binding": _binding(candidate), "affected_accepted_review_id": review_id,
                      "parent_flow_id": parent_flow_id, "child_flow_id": child_id,
                      "experiment_flow_id": experiment_id, "paused_target_work_id": target}
            evidence += [source, authority]
            defect_id = _identity(child_id, "controller-" + finding_id)
            defect = tx.open_defect(defect_id, candidate["candidate_id"], review_id,
                responsibility=report["monitor_requested_disposition"], evidence=evidence,
                kind="material", finding_origin=origin)
            for old in child["state"].get("repaired_defects", []):
                _require(tx.get_defect(old)["state"] == "closed", "prior cycle still has an unresolved responsibility")
            child_state, parent_state = copy.deepcopy(child["state"]), copy.deepcopy(parent["state"])
            previous = {"parent": _prior_state(parent, previous_parent, review_id),
                        "child": _prior_state(child, previous_child, review_id)}
            for record, state, label, receipt in ((parent, parent_state, "parent", previous_parent),
                                                   (child, child_state, "child", previous_child)):
                state["prior_receipts"] = [*state.get("prior_receipts", []), receipt]
                state["history"] = [*state.get("history", []), {"kind": "controller_finding_received",
                    "operation_id": operation_id, "finding_origin": origin, "previous_acceptance": previous[label],
                    "models_dispatched": 0}]
                state.update(status="active", current_work_id=None, waiting=None)
                state.pop("receipt", None)
                state.pop("result", None)
            child_state.update(stage="repair", step=child_state["step"] + 1, receipt_pending=False,
                repaired_defects=[], defects=[{"id": finding_id, "defect_id": defect_id, "source_review_id": review_id,
                    "kind": "material", "responsibility": report["monitor_requested_disposition"],
                    "affected_paths": affected, "evidence": evidence, "finding_origin": origin}],
                last_review_evidence=[*child_state.get("last_review_evidence", []), source])
            parent_state.update(stage="child_quality", child_flow_id=child_id)
            event = {"kind": "controller_finding_reopened", "operation_id": operation_id,
                     "finding_origin": origin, "evidence": [source, authority], "models_dispatched": 0}
            updated_child = tx.update_flow(child_id, expected_child_revision, child_state, event)
            updated_parent = tx.update_flow(parent_flow_id, expected_parent_revision, parent_state, event)
            _require(tx.get_flow(experiment_id) == experiment, "source reopen changed its held experiment")
            return {"status": "reopened", "operation_id": operation_id, "defect": defect,
                    "parent": updated_parent, "child": updated_child, "held_experiment": experiment,
                    "models_dispatched": 0}


def current_after_controller_reopen(store, record):
    """Recognize only an exact previous accepted cursor in a genuine reopen."""
    state = record["state"]
    if state.get("status") != "accepted" or not state.get("receipt"):
        return None
    current = store.get_flow(record["flow_id"])
    if current["revision"] == record["revision"]:
        return None
    if current["definition_digest"] != record["definition_digest"]:
        return None
    prior = [ref for ref in current["state"].get("prior_receipts", []) if ref.get("path") == state["receipt"]]
    if len(prior) != 1:
        return None
    validate_evidence(prior)
    matches = [row for row in current["state"].get("history", [])
               if row.get("kind") == "controller_finding_received"
               and row.get("previous_acceptance", {}).get("flow_id") == record["flow_id"]
               and row["previous_acceptance"].get("revision") == record["revision"]
               and row["previous_acceptance"].get("definition_digest") == record["definition_digest"]
               and row["previous_acceptance"].get("receipt") == prior[0]
               and row["previous_acceptance"].get("event_seq") == record["event_seq"]
               and row["previous_acceptance"].get("state_digest") == sha256(canonical(state))]
    if len(matches) != 1:
        return None
    match = matches[0]
    origin = match["finding_origin"]
    from taskflow.execution_flow import _identity

    with store._db() as db:
        defect = ExecutionStore._load(db, "defects", _identity(origin["child_flow_id"], "controller-" + origin["finding_id"]))
        opened = db.execute("SELECT kind,data FROM events WHERE seq=?", (defect["opened_seq"],)).fetchone()
        _require(origin.get("kind") == "authorized_controller_finding"
                 and origin.get("controller") == current["definition"]["owner"]
                 and record["flow_id"] in {origin["parent_flow_id"], origin["child_flow_id"]}
                 and canonical(defect.get("finding_origin")) == canonical(origin)
                 and defect["review_id"] == match["previous_acceptance"]["review_id"]
                 and opened is not None and opened["kind"] == "defect_opened"
                 and canonical(json.loads(opened["data"]).get("finding_origin")) == canonical(origin),
                 "prior accepted cursor lacks its actual controller finding defect/event")
        events = [json.loads(row["data"]) for row in db.execute(
            "SELECT data FROM events WHERE kind='flow_updated' AND seq>?", (record["event_seq"],))]
    _require(any(event.get("flow_id") == record["flow_id"] and event.get("previous_revision") == record["revision"]
                 and event.get("transition", {}).get("kind") == "controller_finding_reopened"
                 and event["transition"].get("operation_id") == match["operation_id"]
                 and match in event.get("state", {}).get("history", []) for event in events),
             "prior accepted receipt lacks its exact controller reopen event")
    if current["state"].get("status") == "accepted":
        review_id = current["state"].get("accepted_review_id") or current["state"].get("result", {}).get("accepted_review_id")
        _require(review_id, "new accepted result lacks its actual current review")
        store.assert_review_current(review_id)
    return current
