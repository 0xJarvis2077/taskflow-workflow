"""One packet authorizes a bounded experiment; the lifecycle owns its worker."""

import copy
from pathlib import Path

import re

from taskflow.execution_completion_observation import author_technically_complete
from taskflow.contracts import PUBLIC_ROLES, REVIEW_KINDS, TaskflowError, canonical, relative_path, sha256
from taskflow.execution import enqueue_spec
from taskflow.execution_flow import _get_or_none, _identity
from taskflow.execution_formal import _handoff_errors, import_formal, prepare_formal, prepare_release, projected_quality_reports
from taskflow.execution_review import (
    ingest_signed_evaluation, prepare_facts_runner, prepare_score_runner, seal_existing_facts,
)
from taskflow.execution_runner import (
    _info, _read, _validate, _validated_collection, archive_full_attempt,
)
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.records import verify_run


ACTIVE = {"claimed", "starting", "running", "collecting"}


def _save(store: object, record: dict, state: dict, kind: str, review_id: str | None = None) -> dict:
    event = {"kind": kind}
    if review_id:
        event["accepted_review_id"] = review_id
    return store.update_flow(record["flow_id"], record["revision"], state, event)


def _wait(store: object, record: dict, reason: str, **details) -> dict:
    waiting = {"stage": record["state"]["stage"], "reason": reason, **details}
    if record["state"].get("waiting") == waiting:
        return record
    state = {**record["state"], "waiting": waiting}
    return _save(store, record, state, "experiment_waiting")


def _node(record: dict, role: str) -> str:
    return _identity(record["flow_id"], role + "-" + str(record["state"].get("run_index", 0)))


def _native_source_flow(definition: dict) -> str | None:
    from taskflow.execution_engineering_source import KIND

    return (definition.get("source_flow_id") if definition.get("source_kind") == KIND
            else definition.get("source_author_flow_id"))


def _packet(record: dict) -> dict:
    definition = record["definition"]
    reference = validate_evidence([definition["business_packet_ref"]])[0]
    authorization = validate_evidence([definition["authorization"]])[0]
    if reference["path"] != authorization["path"] or reference["sha256"] != authorization["sha256"]:
        raise TaskflowError("experiment business packet differs from immutable lifecycle authorization")
    packet = _read(Path(reference["path"]))
    from taskflow.execution_engineering_source import KIND, engineering_packet

    if definition.get("source_kind") == KIND:
        return engineering_packet(definition, packet)
    if definition.get("source_author_flow_id"):
        selector = definition.get("business_packet_item")
        expected = {"task_id": definition["task_id"],
                    "source_author_quality_flow": definition["source_author_flow_id"]}
        if (definition.get("source_flow_id") is not None or selector != expected
                or type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
                or packet.get("controller") != definition["owner"] or not isinstance(packet.get("items"), list)):
            raise TaskflowError("experiment requires one exact authorized author-flow item selector")
        matches = [item for item in packet["items"] if isinstance(item, dict)
                   and all(item.get(key) == value for key, value in expected.items())]
        if len(matches) != 1:
            raise TaskflowError("experiment author-flow item is absent or ambiguous in its business authority")
        item = matches[0]
        fields = ("source_author_intent", "source_author_attempt", "source_author_sid", "source_business_packet",
                  "source_business_packet_sha256", "first_experiment_business_unknown", "version_id", "first_run_id",
                  "first_evaluation_id", "conditional_one_confirmation_run_id", "conditional_one_confirmation_evaluation_id")
        if (any(not isinstance(item.get(key), str) or not item[key].strip() for key in fields)
                or not packet.get("preauthorized_normal_chain") or not packet.get("prerequisites")):
            raise TaskflowError("experiment author-flow item lacks its exact purpose, original identity or normal-chain authority")
        confirmation = packet.get("confirmation", {})
        review = copy.deepcopy(definition["technical"].get("independent_review", {}))
        review["score_id"] = item["first_evaluation_id"]
        return {**packet, **item, "source_author_flow_id": definition["source_author_flow_id"],
                "business_investment": {"action": packet["preauthorized_normal_chain"],
                                        "unresolved_business_question": item["first_experiment_business_unknown"]},
                "release": {"version_id": item["version_id"]}, "formal": {"first_run_id": item["first_run_id"]},
                "independent_review": review,
                "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True},
                "single_confirmation": {"authorized_if": confirmation.get("only_if", ""),
                    "max_additional_attempts": confirmation.get("maximum_additional_runs_per_first"),
                    "run_id": item["conditional_one_confirmation_run_id"],
                    "score_id": item["conditional_one_confirmation_evaluation_id"]}}
    if (packet.get("controller") != definition["owner"] or packet.get("task_id") != definition["task_id"]
            or packet.get("source_flow_id") != definition["source_flow_id"]
            or not packet.get("business_investment", {}).get("unresolved_business_question")):
        raise TaskflowError("experiment has no exact task/controller/source/purpose authority")
    return packet


def _author_source(store: object, record: dict, packet: dict, *, require_candidate: bool = False) -> dict:
    """Bind a completed author outcome to its actual, unique accepted quality child."""
    from taskflow.execution_author import _author, _handoff
    from taskflow.execution_author_packet import author_packet_matches

    definition = record["definition"]
    author_flow = store.get_flow(definition["source_author_flow_id"])
    author_definition, state = author_flow["definition"], author_flow["state"]
    from taskflow.execution_solver_profile import source_policy_id
    source_policy = source_policy_id(store, definition)
    if packet.get("source_author_flow_kind") not in (None, author_definition.get("kind")):
        raise TaskflowError("experiment source kind differs from its explicitly selected lifecycle")
    if author_definition.get("kind") == "baseline_quality_v1":
        from taskflow.execution_baseline_quality import baseline_experiment_source

        return baseline_experiment_source(store, record, packet, require_candidate=require_candidate)
    if require_candidate and (state.get("status") != "accepted" or not state.get("receipt")
                              or state.get("pending_business_scope")
                              or state.get("result", {}).get("outcome") != "candidate_quality_accepted"):
        raise TaskflowError("release recovery requires the same accepted candidate source without waiting or decisions")
    original_authority = validate_evidence([author_definition["authorization"]])[0]
    if (author_definition.get("kind") != "author_quality_v1"
            or any(author_definition.get(key) != definition[key] for key in ("task_id", "owner"))
            or author_definition.get("policy_id") != source_policy
            or author_definition.get("source_author_attempt") != packet["source_author_attempt"]
            or not author_packet_matches(store, parent=author_flow, packet=packet,
                authorization=definition["authorization"],
                packet_transport=record["state"].get("source_business_packet_transport"))):
        raise TaskflowError("authorized author flow differs from its exact task/owner/policy/original business packet")
    author, _ = _author(store, author_definition)
    if (author["intent_id"] != packet["source_author_intent"]
            or (author.get("cli_sid") is not None and author["cli_sid"] != packet["source_author_sid"])):
        raise TaskflowError("authorized author attempt/intent/SID differs from its actual source")
    if state["status"] == "exception":
        raise TaskflowError("source author-quality flow stopped with its original exception: " +
                            str(state.get("exception") or state.get("result")))
    if state["status"] != "accepted" or not state.get("receipt"):
        return {"record": _wait(store, record, "source_author_quality_not_received",
                                source_author_flow_id=author_flow["flow_id"], source_status=state["status"])}
    if author.get("cli_sid") != packet["source_author_sid"]:
        raise TaskflowError("accepted author outcome lacks its exact original authorized SID")
    receipt = evidence_ref(Path(state["receipt"]), "exact terminal source author-quality result")
    received = _read(Path(receipt["path"]))
    if (received.get("flow_id") != author_flow["flow_id"] or received.get("status") != "accepted"
            or received.get("controller") != definition["owner"] or received.get("result") != state.get("result")):
        raise TaskflowError("source author-quality receipt differs from its actual terminal result")
    if not author_technically_complete(author) or not state.get("author_archive"):
        raise TaskflowError("accepted author outcome lacks its original completed author archive")
    handoff = _handoff(Path(state["author_archive"]), author_definition, author)
    result = state.get("result") or {}
    binding = {"source_author_flow_id": author_flow["flow_id"], "source_revision": author_flow["revision"],
               "source_author_attempt": author["attempt_id"], "source_author_sid": author["cli_sid"], "receipt": receipt}
    if handoff["disposition"] == "research_ended":
        if (result.get("outcome") != "research_ended" or result.get("reason") != handoff["reason"]
                or result.get("candidate_created") is not False or result.get("new_model_dispatched") is not False
                or result.get("formal_experiment") != "not_dispatched"):
            raise TaskflowError("research-ended author result conflicts with its original signed handoff")
        terminal = {**record["state"], "status": "accepted", "stage": "complete", "current_work_id": None,
                    "waiting": None, "author_source_binding": binding,
                    "result": {**result, "source_author_flow_id": author_flow["flow_id"], "source_receipt": receipt}}
        return {"record": _save(store, record, terminal, "experiment_author_research_ended")}
    if handoff["disposition"] != "candidate_complete" or result.get("outcome") != "candidate_quality_accepted":
        raise TaskflowError("source author has no complete independently accepted candidate; substantive decision unresolved")
    child_id = state.get("child_flow_id")
    if not child_id or result.get("child_flow_id") != child_id:
        raise TaskflowError("author result does not identify its exact registered quality child")
    child = store.get_flow(child_id)
    child_definition, child_state = child["definition"], child["state"]
    if (any(child_definition.get(key) != definition[key] for key in ("task_id", "owner"))
            or child_definition.get("policy_id") != source_policy
            or child_state.get("status") != "accepted" or not child_state.get("receipt")
            or any(result.get(key) != child_state.get(key) for key in ("candidate_id", "candidate_root", "accepted_review_id"))
            or result.get("child_receipt") != child_state["receipt"]):
        raise TaskflowError("author accepted result differs from its actual current quality child")
    child_receipt = evidence_ref(Path(child_state["receipt"]), "actual accepted child quality result")
    child_received = _read(Path(child_receipt["path"]))
    if (child_received.get("flow_id") != child_id or child_received.get("status") != "accepted"
            or child_received.get("candidate_id") != child_state["candidate_id"]
            or child_received.get("controller") != definition["owner"]
            or child_received.get("history") != child_state.get("history")):
        raise TaskflowError("accepted child receipt differs from its exact recorded candidate/history")
    initial_config = child_definition["initial"]
    initial = store.get_candidate(initial_config["candidate_id"])
    public = store.get_attempt(state["public_attempt_id"])
    if (initial["candidate_id"] != state.get("candidate_id") or initial.get("author_attempt") != author["attempt_id"]
            or initial.get("parent_id") != author["candidate_id"]
            or public["state"] != "completed" or public["role"] != "quality"
            or public["task_id"] != definition["task_id"] or public["candidate_id"] != initial["candidate_id"]
            or not public.get("cli_sid") or public["cli_sid"] == author["cli_sid"]
            or initial_config.get("predecessor_attempt") != public["attempt_id"]
            or initial_config.get("predecessor_binding") != {key: public[key] for key in
                                                            ("work_id", "intent_id", "candidate_id", "task_id")}):
        raise TaskflowError("quality child is not the actual fresh-public successor of this complete author candidate")
    if any(initial.get(key) != initial_config["expected_digests"].get(key) for key in
           ("content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("quality child initial candidate differs from its captured three digests")
    return {"source": child, "initial": initial, "author_sids": {author["cli_sid"], public["cli_sid"]},
            "binding": {**binding, "child_flow_id": child_id, "child_receipt": child_receipt}}


def _native_handoff(store: object, record: dict, candidate_id: str) -> dict:
    from taskflow.execution_formal import native_quality_handoff

    value = native_quality_handoff(store, candidate_id=candidate_id,
        authorization=record["definition"]["authorization"],
        source_flow_id=_native_source_flow(record["definition"]),
        packet_transport=record["state"].get("source_business_packet_transport"))
    if (record["state"].get("native_quality_handoff") is not None
            and value != record["state"]["native_quality_handoff"]):
        raise TaskflowError("current native quality/normative certification differs from the bound original handoff")
    return value


def _native_normative(snapshot: Path, declaration: dict, handoff: dict) -> list[str]:
    selected = handoff["scoring_source_allowlist"]
    sources = handoff["normative_sources"]
    if (not isinstance(selected, list) or not selected or any(not isinstance(item, str) for item in selected)
            or len(selected) != len(set(selected)) or not isinstance(sources, list)
            or len(sources) != len(selected) or {row["package_path"] for row in sources} != set(selected)):
        raise TaskflowError("native normative certification needs one exact source per allowed package path")
    declarations = {row["path"]: row for row in declaration["files"]}
    by_path = {row["package_path"]: row for row in sources}
    for value in selected:
        name = relative_path(value)
        signed = by_path[name]
        actual = _info(snapshot / name)
        if (name not in declarations or declarations[name]["role"] in PUBLIC_ROLES
                or signed["role"] != declarations[name]["role"]
                or any(actual[key] != signed[key] for key in ("sha256", "size"))):
            raise TaskflowError("frozen normative source differs from the exact independently certified private file")
    return list(selected)


def _confirmation_limit(packet: dict, total: int | float) -> float | None:
    condition = packet.get("single_confirmation", {}).get("authorized_if", "")
    bounds = re.findall(r"<=\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*([0-9]+(?:\.[0-9]+)?)", condition)
    if len(bounds) != 1:
        return None
    maximum, scale = map(float, bounds[0])
    if scale != total or not 0 <= maximum <= scale:
        raise TaskflowError("confirmation score condition differs from the frozen scoring scale")
    return maximum


def _local_configuration_reasons(record: dict, result: dict, observed: dict) -> list[str]:
    reasons = []
    signed = result.get("local_profile_assessment", {})
    if signed.get("signed_binding_valid") is not True:
        reasons.extend(signed.get("structural_reasons", ["signed_local_profile_assessment_missing"]))
    if signed.get("local_eligible") is not True:
        reasons.append("signed_local_eligible_" + str(signed.get("local_eligible", "unknown")))
    if signed.get("artifacts_complete") is not True:
        reasons.append("signed_artifact_completeness_not_true")
    if signed.get("served_identity_unknown_disclosed") is not True:
        reasons.append("served_identity_unknown_not_explicitly_disclosed")
    if signed.get("substantive_disputes") != []:
        reasons.append("signed_substantive_disputes_not_closed")
    if observed.get("model_validity") == "invalid":
        reasons.append("original_model_validity_explicitly_invalid")
    allowed_gaps = {"observed_config.model: unknown"}
    reasons.extend("original_run_gap: " + item for item in observed.get("gaps", []) if item not in allowed_gaps)
    original = _read(Path(result["run"]) / "record.original.json")
    requested, local = original.get("requested_config", {}), original.get("observed_config", {})
    config = record["definition"]["technical"]["formal_config_base"]
    if config.get("reasoning_selection") == "explicit":
        if (requested.get("model") != config["model"]
                or requested.get("reasoning_selection") != "explicit"
                or requested.get("request_reasoning_override") != config["reasoning_effort"]):
            reasons.append("actual_explicit_request_configuration_differs_from_registered_profile")
        return reasons
    if (requested.get("model") != config["model"]
            or requested.get("reasoning_selection") != config.get("reasoning_selection", "verified_client_default")
            or requested.get("request_reasoning_override") is not None
            or local.get("client_resolved") != config["model"]
            or local.get("client_resolved_reasoning") != {"effort": config["reasoning_effort"], "context": config["reasoning_context"]}):
        reasons.append("actual_local_client_configuration_not_proved_equal_to_registered_profile")
    return reasons


def _run_id(record: dict, packet: dict) -> str:
    index = record["state"].get("run_index", 0)
    if index == 0:
        return packet["formal"]["first_run_id"]
    if index == 1 and record["state"].get("confirmation_authorized") is True:
        return packet["single_confirmation"]["run_id"]
    raise TaskflowError("no authorization for another experimental attempt")


def _work(store: object, record: dict, role: str, purpose: str, dependencies=None) -> dict:
    definition, state = record["definition"], record["state"]
    return {"work_id": _node(record, role), "intent_id": _node(record, role) + "-intent",
            "task_id": definition["task_id"], "candidate_id": state["candidate_id"], "role": role,
            "policy_id": definition["policy_id"], "purpose": purpose, "request_digest": "0" * 64,
            "dispatch_owner": definition["owner"], "dependencies": dependencies or [],
            "evidence": [definition["authorization"]]}


def _completed(store: object, record: dict) -> tuple[dict | None, str]:
    from taskflow.execution_completion import reconcile_ended_attempt

    identity = record["state"]["current_work_id"]
    work = store.get_work(identity)
    attempts = store.list_attempts(identity)
    if not attempts:
        if work["state"] != "ready":
            raise TaskflowError("experiment work has no original attempt and is not ready")
        return None, "original_worker_not_started"
    if len(attempts) != 1:
        raise TaskflowError("one experiment intent has more than one original attempt")
    attempt = reconcile_ended_attempt(store, attempts[0])
    if attempt["state"] in ACTIVE:
        return None, "original_worker_owns_completion"
    if attempt["state"] != "completed":
        raise TaskflowError("original experiment role failed/uncertain/cancelled; no automatic redraw: " + attempt["attempt_id"])
    root = Path(attempt["runtime_path"])
    if not (root / "collection-lock.json").exists():
        return None, "original_collection_not_sealed"
    manifest = _validate(root)
    collection = _validated_collection(root, manifest)
    candidate = store.get_candidate(record["state"]["candidate_id"])
    binding = {key: candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
    if (manifest.get("candidate_binding") != binding or collection.get("sid") != attempt.get("cli_sid")
            or not collection.get("technical_accepted") or manifest["request"]["sha256"] != attempt["request_digest"]
            or manifest["intent_id"] != attempt["intent_id"] or manifest["policy_version"] != attempt["policy_id"]
            or any(work[key] != attempt[key] for key in ("task_id", "candidate_id", "intent_id", "role", "runtime_path", "policy_id"))):
        raise TaskflowError("experiment completed role original identity/version/source binding differs")
    if attempt["role"] == "solver" and attempt["cli_sid"] in record["state"].get("excluded_sids", []):
        raise TaskflowError("formal solver reused an author/teacher/prior experiment context")
    return attempt, "completed"


def _source_composite(review: dict, coverage: dict, candidate: dict, coverage_base: dict,
                      supplement: dict | None, authorized_id: str | None) -> None:
    """Keep the store-validated graph bound to this exact authorized source."""
    graph = review.get("composite_quality_coverage")
    expected_ids = coverage["closed_defect_ids"]
    if (not isinstance(graph, dict) or review.get("candidate_id") != candidate["candidate_id"]
            or review.get("derived") is not True or review.get("snapshot_bound_acceptance") is not True
            or review.get("whole_package") is not False
            or graph.get("whole_review_id") != coverage["whole_review_id"]
            or graph.get("recheck_id") != coverage["recheck_id"]
            or graph.get("whole_source", {}).get("candidate_id") != coverage_base["candidate_id"]
            or graph.get("affected_source", {}).get("candidate_id") != candidate["candidate_id"]
            or not isinstance(graph.get("closed_defect_ids"), list)
            or len(graph["closed_defect_ids"]) != len(set(graph["closed_defect_ids"]))
            or sorted(graph["closed_defect_ids"]) != sorted(expected_ids)
            or (authorized_id is not None and review.get("review_id") != authorized_id)):
        raise TaskflowError("validated composite quality differs from this exact source candidate/reviews/closure")
    third = graph.get("recheck_supplement")
    if supplement is None:
        if third is not None:
            raise TaskflowError("source supplement lacks its explicit experiment-state authorization")
    elif (not isinstance(third, dict) or canonical(third.get("definition")) != canonical(supplement)
          or supplement["base_recheck_id"] != graph["recheck_id"]
          or supplement["original_recheck_report"]["sha256"] != graph["affected_source"]["report"]["sha256"]):
        raise TaskflowError("validated composite supplement differs from its exact authorized source")


def _source(store: object, record: dict, packet: dict) -> dict:
    bound = None
    from taskflow.execution_engineering_source import KIND, engineering_source

    if record["definition"].get("source_kind") == KIND:
        bound = engineering_source(store, record["definition"], packet)
        source, initial = bound["source"], bound["initial"]
    elif record["definition"].get("source_author_flow_id"):
        bound = _author_source(store, record, packet)
        if "record" in bound:
            return bound["record"]
        source, initial = bound["source"], bound["initial"]
    else:
        source = store.get_flow(record["definition"]["source_flow_id"])
    if source["state"]["status"] == "exception":
        raise TaskflowError("source quality flow stopped with its original exception")
    if source["state"]["status"] != "accepted" or not source["state"].get("receipt"):
        return _wait(store, record, "source_quality_not_accepted", source_flow_id=source["flow_id"])
    candidate_id = source["state"]["candidate_id"]
    candidate = store.get_candidate(candidate_id)
    if bound is None:
        initial = store.get_candidate(packet["initial_candidate_id"])
        if any(initial[key] != value for key, value in packet["initial_expected_digests"].items()):
            raise TaskflowError("authorized initial candidate version changed")
    ancestor = candidate
    authors = set(bound.get("excluded_sids", bound.get("author_sids", []))) if bound else set()
    seen = set()
    while ancestor["candidate_id"] != initial["candidate_id"]:
        authors.update(ancestor.get("external_author_sids", []))
        if ancestor.get("author_attempt"):
            authors.add(store.get_attempt(ancestor["author_attempt"])["cli_sid"])
        if ancestor["candidate_id"] in seen or not ancestor.get("parent_id"):
            raise TaskflowError("accepted experiment candidate is outside the authorized source lineage")
        seen.add(ancestor["candidate_id"])
        ancestor = store.get_candidate(ancestor["parent_id"])
    authors.update(initial.get("external_author_sids", []))
    if initial.get("author_attempt"):
        authors.add(store.get_attempt(initial["author_attempt"])["cli_sid"])
    if candidate["task_id"] != record["definition"]["task_id"]:
        raise TaskflowError("experiment source candidate belongs to another task")
    coverage = record["definition"]["technical"].get("quality_coverage")
    coverage_base = initial
    if bound and coverage is None:
        current = store.get_review(source["state"]["accepted_review_id"])
        if current.get("whole_package") is False:
            originals = [store.get_review(identity) for identity in
                         {row["review_id"] for row in source["state"].get("history", []) if row.get("review_id")}]
            originals = [item for item in originals if item.get("candidate_id") == candidate.get("parent_id")
                         and item.get("whole_package") is True and item.get("verdict") == "defects"]
            if len(originals) != 1:
                raise TaskflowError("accepted affected child lacks one exact direct-parent whole finding for bounded composition")
            coverage = {"whole_review_id": originals[0]["review_id"], "recheck_id": current["review_id"],
                        "closed_defect_ids": current.get("covered_defect_ids", [])}
    supplement = record["state"].get("quality_recheck_supplement")
    authorized_id = record["state"].get("quality_recheck_supplement_review_id")
    if supplement is not None:
        fields = {"schema_version", "attempt_id", "report", "issues", "original_recheck_report", "base_recheck_id"}
        if (not isinstance(supplement, dict) or set(supplement) != fields
                or type(supplement.get("schema_version")) is not int or supplement["schema_version"] != 1
                or coverage is None or supplement["base_recheck_id"] != coverage["recheck_id"]):
            raise TaskflowError("experiment supplement requires its exact explicit repaired-quality source specification")
        validate_evidence([supplement[key] for key in ("report", "issues", "original_recheck_report")])
    elif authorized_id is not None:
        raise TaskflowError("experiment supplement review identity has no explicit supplement specification")
    review = None
    if coverage is not None:
        gate = packet.get("quality_and_candidate_gate", {})
        history = {row.get("review_id") for row in source["state"].get("history", [])}
        coverage_base = store.get_candidate(store.get_review(coverage["whole_review_id"])["candidate_id"])
        if (not isinstance(gate, dict) or not gate.get("required") or not gate.get("no_implicit_experiment")
                or coverage["recheck_id"] != source["state"]["accepted_review_id"]
                or coverage["whole_review_id"] not in history
                or coverage_base["candidate_id"] != candidate.get("parent_id")
                or coverage_base["candidate_id"] not in seen | {initial["candidate_id"]}):
            raise TaskflowError("composite quality sources differ from the explicitly authorized source flow")
        try:
            review = store.quality_gate(candidate_id)
        except TaskflowError:
            if authorized_id is not None:
                raise
        if review is None:
            options = {"supplement": supplement} if supplement is not None else {}
            store.compose_repaired_quality(candidate_id, coverage["whole_review_id"], coverage["recheck_id"],
                coverage["closed_defect_ids"], evidence=[record["definition"]["authorization"],
                    evidence_ref(Path(source["state"]["receipt"]), "actual closed source quality receipt")], **options)
            review = store.quality_gate(candidate_id)
        _source_composite(review, coverage, candidate, coverage_base, supplement, authorized_id)
    if review is None:
        review = store.quality_gate(candidate_id)
    graph = review.get("composite_quality_coverage")
    source_review = graph["recheck_id"] if graph else review["review_id"]
    if source_review != source["state"]["accepted_review_id"]:
        raise TaskflowError("source flow receipt differs from the current whole-quality signature")
    store.assert_review_current(review["review_id"])
    signer = store.get_attempt(review["attempt_id"])
    sources = {name: evidence_ref(Path(signer["runtime_path"]) / "artifacts" / filename,
                "actual current same-attempt quality source")
               for name, filename in (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md"))}
    if graph:
        original = store.get_attempt(graph["whole_source"]["attempt_id"])
        sources.update({"whole_" + name: evidence_ref(Path(original["runtime_path"]) / "artifacts" / filename,
                        "original whole-scope finding retained in explicit repaired quality coverage")
                        for name, filename in (("adoption", "adoption-readiness.json"), ("issues", "issues.json"), ("intake", "intake.md"))})
        authors.add(original["cli_sid"])
        if supplement is not None:
            actor = store.get_attempt(supplement["attempt_id"])
            if (actor.get("state") != "completed" or actor.get("role") != "quality"
                    or actor.get("candidate_id") != candidate_id or actor.get("task_id") != candidate["task_id"]
                    or not actor.get("cli_sid") or actor["cli_sid"] in {*authors, signer["cli_sid"]}):
                raise TaskflowError("supplement actor differs from this candidate's independent completed quality source")
            sources.update(supplement_adoption=supplement["report"], supplement_issues=supplement["issues"],
                           supplement_intake=evidence_ref(Path(actor["runtime_path"]) / "artifacts/intake.md",
                               "actual independent affected supplement intake"))
            authors.add(actor["cli_sid"])
    configured = record["definition"]["technical"].get("quality_projection_sources", {})
    if (graph or candidate_id == initial["candidate_id"]) and any(
            name not in sources or item["path"] != sources[name]["path"] or item["sha256"] != sources[name]["sha256"]
            for name, item in configured.items()):
        raise TaskflowError("registered quality sources differ from the accepted original attempt")
    state = {**record["state"], "stage": "release", "candidate_id": candidate_id,
             "candidate_root": source["state"]["candidate_root"], "accepted_review_id": review["review_id"],
             "quality_sources": sources, "run_index": 0, "runs": [], "waiting": None,
             "excluded_sids": sorted({*authors, signer["cli_sid"], *[item["cli_sid"] for item in source["state"].get("history", []) if item.get("cli_sid")]})}
    if bound:
        source_field = "engineering_source_binding" if record["definition"].get("source_kind") == KIND else "author_source_binding"
        state[source_field] = bound["binding"]
        state["native_quality_handoff"] = _native_handoff(store, record, candidate_id)
        state["quality_sources"] = state["native_quality_handoff"]["source_refs"]
    state["history"] = [*state["history"], {"kind": "accepted_source_bound", "source_flow_id": source["flow_id"],
                        "candidate_id": candidate_id, "review_id": review["review_id"], "source_receipt": source["state"]["receipt"]}]
    return _save(store, record, state, "experiment_source_bound", review["review_id"])


def _normative_paths(snapshot: Path, declaration: dict, review: dict) -> list[str]:
    """Select exactly configured IDs and explicit declared private path prefixes."""
    configured = review["teacher_file_ids"]
    normative = {item["id"]: item["path"] for item in declaration["files"]}
    if any(identity not in normative for identity in configured):
        raise TaskflowError("frozen repaired candidate lacks an authorized normative teacher ID")
    selected = [normative[identity] for identity in configured]
    if "teacher_path_prefixes" not in review:
        return selected
    prefixes = review["teacher_path_prefixes"]
    if (not isinstance(prefixes, list) or any(not isinstance(item, str) for item in prefixes)
            or len(prefixes) != len(set(prefixes))):
        raise TaskflowError("normative teacher prefixes must be an explicit unique list")
    if not prefixes:
        return selected
    for prefix in prefixes:
        if not isinstance(prefix, str) or not prefix.endswith("/"):
            raise TaskflowError("normative teacher prefix must be relative and end in slash")
        relative_path(prefix[:-1])
    from taskflow.packages import verify_snapshot

    frozen = verify_snapshot(snapshot)
    captured = {row["path"]: row for row in frozen["files"]}
    for item in declaration["files"]:
        name = relative_path(item["path"])
        if not any(name.startswith(prefix) for prefix in prefixes):
            continue
        if item["role"] in PUBLIC_ROLES:
            raise TaskflowError("normative teacher prefix includes a public file")
        actual = _info(snapshot / name)
        if name not in captured or any(actual[key] != captured[name][key] for key in ("sha256", "size")):
            raise TaskflowError("normative teacher prefix source differs from actual frozen bytes")
        if name not in selected:
            selected.append(name)
    return selected



def _score_request_sections(imported: dict, candidate: dict) -> list[dict]:
    """Render future scoring requests using the accepted evidence locator contract."""
    return [{"kind": "operation", "text": (
                "Continue your own sealed factual examination in this exact SID. Read only supplied same-version normative references, legitimate alternatives and normative/task-scoring.json. "
                "Preserve the sealed facts. Judge every frozen criterion fairly; do not add obligations, caps or deductions. "
                "Save score.json as {max_score,total_score,criteria:[{id,max_score,score,reason,public_evidence:[{path,locator}],submission_evidence:[{path,locator}],stage1_evidence:[{path,locator}]}]}. "
                "Use actual input-relative paths for every conclusion; retain all original reasons. "
                'Every evidence list in criteria, experiment_validity and artifact_status uses the same typed locator contract. '
                'For JSON, use locator={"type":"json_pointer","pointer":"/criteria/0"}, replacing the example with one exact existing JSON Pointer. '
                'Use locator={"type":"json_pointer","pointer":""} for the complete JSON document, including a root array; / denotes an empty object key, not the document root. '
                'Escape ~ as ~0 and / within a key as ~1. '
                'For UTF-8 text, CSV or JSONL, use locator={"type":"lines","start":1,"end":3}, replacing the example with an existing inclusive 1-based range of integer line numbers. '
                'The JSON aliases normative/task-scoring.json, sealed/facts.json and local-profile-definition.json require JSON Pointer locators. '
                'Use these three aliases only in experiment_validity.evidence or artifact_status.evidence. '
                'Criterion public_evidence and submission_evidence must cite the original locked first-stage sources; typed locators do not expand source permissions. '
                'stage1_evidence is retained in the signed score and does not replace required criterion public_evidence and submission_evidence. '
                'Each {path,locator} evidence item identifies exactly one pointer or one line range; repeat the path in separate evidence items for multiple locations. '
                'Put explanations in the existing reason field. Do not combine multiple pointers with commas or and, append prose to a locator, or use free-text locations such as whole result. '
                "Also sign execution_binding=" + canonical({"run_digest": verify_run(Path(imported["run_path"]))["run_digest"],
                    "snapshot_digest": verify_run(Path(imported["run_path"]))["snapshot_digest"],
                    "input_digest": candidate["input_digest"], "evaluation_digest": candidate["evaluation_digest"]}).decode() + ". "
                "Independently sign experiment_validity={local_profile:'current_registered_local_profile',local_eligible:true|false|'unknown',"
                "served_identity_unknown_disclosed:true,substantive_disputes:[],reason,evidence:[{path,locator}]} and "
                "artifact_status={artifacts_complete:true|false,reason,evidence:[{path,locator}]}. "
                "Use the neutral actual-configuration/termination/collection observations and complete actual submission. "
                "Unknown served identity is disclosed separately and does not establish official validity; local eligibility stays unknown unless the registered local profile is actually supported. "
                "Save the full independent scoring explanation in explanation.md. No target scores or prior grades are supplied.")}]


def _required_release_reports(declaration: dict, reports: dict[str, Path]) -> dict[str, Path]:
    """Select declared required IDs from canonical dimensions, without aliasing signatures."""
    if set(reports) - REVIEW_KINDS:
        raise TaskflowError("release projection contains an unknown review kind")
    required = [item for item in declaration["reviews"] if item["required"]]
    kinds = [item["kind"] for item in required]
    if len(kinds) != len(set(kinds)):
        raise TaskflowError("automatic release requires a unique kind for each required review ID")
    if set(kinds) - set(reports):
        raise TaskflowError("release projection is missing a required review kind")
    selected = {item["id"]: reports[item["kind"]] for item in required}
    paths = [Path(path).absolute() for path in selected.values()]
    if len(paths) != len(set(paths)):
        raise TaskflowError("automatic release cannot reuse one projection for distinct required kinds")
    return selected


@_handoff_errors
def _release_quality_reports(store: object, *, candidate_id: str, source_root: Path,
                             reports: dict[str, Path]) -> dict[str, Path]:
    """Revalidate canonical evidence and bind only unchanged captured required IDs."""
    from taskflow.execution_formal import _source_report

    candidate = store.get_candidate(candidate_id)
    declarations = [row for row in candidate.get("evidence", [])
                    if row.get("location") == "explicit package declaration"]
    if len(declarations) != 1 or declarations[0]["path"] != str(source_root.absolute() / "task.json"):
        raise TaskflowError("release report binding needs the captured source declaration")
    validate_evidence(declarations)
    selected = _required_release_reports(_read(source_root / "task.json"), reports)
    accepted = store.quality_gate(candidate_id)
    # Retained, unselected canonical dimensions are evidence, never optional signatures.
    for kind, path in reports.items():
        _, report = _source_report(Path(path), {"id": kind, "kind": kind}, candidate, accepted, set(), store)
        if report.get("kind") != kind or not isinstance(report.get("derived_quality_projection"), dict):
            raise TaskflowError("release projection key differs from its actual canonical signed kind")
    return selected


def _advance(store: object, record: dict) -> dict:
    definition, state = record["definition"], dict(record["state"])
    packet = _packet(record)
    authorization = definition["authorization"]
    for other in store.list_flows():
        reference = other["definition"].get("authorization", {})
        if (other["flow_id"] != record["flow_id"] and other["definition"].get("kind") == "experiment_v1"
                and reference.get("path") == authorization["path"] and reference.get("sha256") == authorization["sha256"]
                and (definition.get("business_packet_item") is None
                     or other["definition"].get("business_packet_item") is None
                     or other["definition"]["business_packet_item"] == definition["business_packet_item"])):
            raise TaskflowError("single experiment packet item is already registered under another lifecycle")
    stage = state["stage"]
    technical = definition["technical"]
    directory = Path(definition["directory"])
    if stage == "experiment_v1":
        return _source(store, record, packet)
    candidate = store.get_candidate(state["candidate_id"])
    run_id = _run_id(record, packet)
    if stage == "release":
        native = _native_handoff(store, record, state["candidate_id"]) if _native_source_flow(definition) else None
        reports = projected_quality_reports(store, candidate_id=state["candidate_id"], source_refs=state["quality_sources"],
                    protocol_map=native["protocol_map"] if native else technical["quality_projection_map"],
                    destination=directory / "quality-projection")
        reports = _release_quality_reports(store, candidate_id=state["candidate_id"],
                    source_root=Path(state["candidate_root"]), reports=reports)
        released = prepare_release(store, candidate_id=state["candidate_id"], source_root=Path(state["candidate_root"]),
                    destination=Path(technical["publication_root"]), version=packet["release"]["version_id"], quality_reports=reports)
        state.update(stage="formal_prepare", release=released, waiting=None)
        return _save(store, record, state, "experiment_exact_release_bound", state["accepted_review_id"])
    if stage == "formal_prepare":
        purpose = packet["business_investment"]["unresolved_business_question"]
        work = _work(store, record, "solver", purpose)
        decision_id = _node(record, "experiment-decision")
        decision = _get_or_none(store.get_decision, decision_id)
        decisions = [] if decision is None else [decision]
        if not decisions:
            store.record_decision(decision_id, definition["task_id"], state["candidate_id"], action="experiment",
                rationale=packet["business_investment"]["action"], evidence=[definition["authorization"]],
                experiment_purpose=purpose, authorized=True)
        elif any(decisions[0][key] != value for key, value in {"candidate_id": state["candidate_id"], "action": "experiment", "experiment_purpose": purpose, "authorized": True}.items()):
            raise TaskflowError("existing experiment decision differs from the single authorized purpose")
        config = {**technical["formal_config_base"], "run_id": run_id, "intent_id": work["intent_id"], "policy_version": definition["policy_id"]}
        if _native_source_flow(definition):
            config["authorization_item"] = definition["business_packet_item"]
        inner = Path(technical["formal_runtime_root"]) / run_id
        preparation = {"config": config, "release": state["release"], "candidate": candidate, "runtime": inner}
        if state.get("formal_preparation_recovery") is not None:
            preparation["preparation_recovery"] = state["formal_preparation_recovery"]
        prepared = prepare_formal(**preparation)
        outer = directory / "runtimes" / _node(record, "solver")
        existing = _get_or_none(store.get_work, work["work_id"])
        if existing:
            if (existing["intent_id"] != work["intent_id"] or existing["candidate_id"] != work["candidate_id"]
                    or existing["runtime_path"] != str(outer)):
                raise TaskflowError("existing formal intent belongs to another candidate/runtime")
        else:
            enqueue_spec(store, {"work": work, "runner": prepared["runner"]}, outer, resume_prepared=outer.exists())
        state.update(stage="formal_wait", current_work_id=work["work_id"], formal_runtime=str(inner), formal_outer=str(outer), waiting=None)
        return _save(store, record, state, "experiment_formal_ready")
    if stage in ("formal_wait", "facts_wait", "score_wait"):
        attempt, reason = _completed(store, record)
        if attempt is None:
            return _wait(store, record, reason, work_id=state["current_work_id"])
        if stage == "formal_wait":
            archived = archive_full_attempt(Path(attempt["runtime_path"]), directory / "archives" / attempt["attempt_id"])
            imported = import_formal(attempt=attempt, archive=Path(archived["path"]), snapshot=Path(state["release"]["snapshot"]["path"]), run_id=run_id)
            state.update(stage="facts_prepare", current_work_id=None, formal_attempt=attempt,
                         formal_archive=archived, imported_run=imported, waiting=None)
            state["archives"] = [*state["archives"], archived["path"]]
            return _save(store, record, state, "experiment_original_run_imported")
        if stage == "facts_wait":
            sealed = seal_existing_facts(config={"facts_attempt": attempt["runtime_path"], "review_binding": state["facts_prepared"]["review_binding"]})
            archived = archive_full_attempt(Path(attempt["runtime_path"]), directory / "archives" / attempt["attempt_id"])
            state.update(stage="score_prepare", current_work_id=None, facts_attempt=attempt, facts_seal=sealed, waiting=None)
            state["archives"] = [*state["archives"], archived["path"]]
            return _save(store, record, state, "experiment_original_facts_sealed")
        state.update(stage="evaluation_intake", current_work_id=None, score_attempt=attempt, waiting=None)
        return _save(store, record, state, "experiment_original_score_completed")
    if stage in ("facts_prepare", "score_prepare"):
        role = "review-facts" if stage == "facts_prepare" else "review-score"
        dependencies = [state["formal_attempt"]["work_id"]] if role == "review-facts" else [state["facts_attempt"]["work_id"]]
        work = _work(store, record, role, "Independent exact-run " + role, dependencies)
        runner = copy.deepcopy(technical["review_runner"])
        runner.update(role=role, intent_id=work["intent_id"], policy_version=definition["policy_id"])
        imported = state["imported_run"]
        config = {"work": work, "runner": runner, "run_directory": imported["run_path"],
                  "first_review_index": imported["first_review_index"], "first_review_inputs": imported["first_review_inputs"],
                  "real_repository": technical["real_repository"], "excluded_sids": state["excluded_sids"]}
        runtime = Path(technical["formal_runtime_root"]) / (".review-" + run_id) / ("facts" if role == "review-facts" else "score") / "runtime"
        if role == "review-facts":
            runner["outputs"] = ["facts.json"]
            prepared = prepare_facts_runner(store=store, config=config, runtime=runtime)
            state.update(stage="facts_wait", current_work_id=work["work_id"], facts_prepared=prepared, waiting=None)
        else:
            declaration = _read(Path(state["release"]["snapshot"]["path"]) / "task.json")
            if _native_source_flow(definition):
                native = _native_handoff(store, record, state["candidate_id"])
                normative = _native_normative(Path(state["release"]["snapshot"]["path"]), declaration, native)
            else:
                normative = _normative_paths(Path(state["release"]["snapshot"]["path"]), declaration,
                                             packet["independent_review"])
            runner["outputs"] = ["score.json", "explanation.md"]
            runner["request_sections"] = _score_request_sections(imported, candidate)
            config.update(facts_attempt=state["facts_attempt"]["runtime_path"], facts_binding=state["facts_prepared"]["review_binding"],
                          scoring_source_allowlist=normative)
            if _native_source_flow(definition):
                config["normative_certification"] = {
                    "schema_version": 1, "kind": "native_quality_handoff",
                    "candidate_binding": {key: candidate[key] for key in (
                        "candidate_id", "task_id", "content_digest", "input_digest", "evaluation_digest")},
                    ("source_quality_flow_id" if definition.get("source_kind") == "engineering_candidate_quality_v1"
                      else "source_author_flow_id"): _native_source_flow(definition),
                    "authorization": definition["authorization"], "native_quality_handoff": native}
            if "local_profile_policy" in technical:
                config["local_profile_policy"] = technical["local_profile_policy"]
            prepared = prepare_score_runner(store=store, config=config, runtime=runtime)
            state.update(stage="score_wait", current_work_id=work["work_id"], score_prepared=prepared, waiting=None)
        return _save(store, record, state, "experiment_" + role + "_ready")
    if stage == "evaluation_intake":
        evaluation_id = packet["independent_review"]["score_id"] if state["run_index"] == 0 else packet["single_confirmation"]["score_id"]
        config = {"schema_version": 1, "mode": "managed_two_stage", "run_directory": state["imported_run"]["run_path"],
                  "snapshot_directory": state["release"]["snapshot"]["path"], "evaluation_id": evaluation_id,
                  "facts_attempt": state["facts_attempt"]["runtime_path"], "facts_binding": state["facts_prepared"]["review_binding"],
                  "score_attempt": state["score_attempt"]["runtime_path"], "score_binding": state["score_prepared"]["review_binding"],
                  "expected_sid": state["facts_seal"]["sid"], "score_file": "score.json", "explanation_file": "explanation.md",
                  "evidence": [definition["authorization"]]}
        result = ingest_signed_evaluation(config=config, destination=directory / "evaluations" / run_id)
        if not result["accepted"]:
            return _wait(store, record, result["reason"])
        result = {**result, "solver_attempt_id": state["formal_attempt"]["attempt_id"],
                  "solver_sid": state["formal_attempt"]["cli_sid"]}
        runs = [*state["runs"], result]
        observed = verify_run(Path(result["run"]))
        total = _read(Path(state["release"]["snapshot"]["path"]) / "task.json")["scoring"]["total"]
        maximum = _confirmation_limit(packet, total)
        reasons = []
        if state["run_index"] != 0:
            reasons.append("single_confirmation_already_used")
        if maximum is None or result["score"] > maximum:
            reasons.append("first_signed_score_outside_authorized_confirmation_bound")
        if packet.get("single_confirmation", {}).get("max_additional_attempts") != 1:
            reasons.append("no_single_additional_attempt_authority")
        if observed.get("completion") != "completed":
            reasons.append("original_formal_run_not_completed")
        if observed.get("input_binding") != "matched":
            reasons.append("original_input_binding_not_matched")
        reasons.extend(_local_configuration_reasons(record, result, observed))
        eligible = not reasons
        eligibility = {"eligible": eligible, "reasons": reasons, "score_limit": maximum, "scoring_total": total}
        state["excluded_sids"] = sorted({*state["excluded_sids"], result["solver_sid"], result["signer_sid"]})
        state["history"] = [*state["history"], {"kind": "signed_original_evaluation_received", "run_id": run_id,
                            "evaluation_id": evaluation_id, "source_sha256": result["source_sha256"], "signer_sid": result["signer_sid"]}]
        state["archives"] = [*state["archives"], result["review_archive"]["path"]]
        if eligible:
            # A completed first-run recovery cannot authorize a different run.
            recovery = state.pop("formal_preparation_recovery", None)
            if recovery is not None:
                state["history"] = [*state["history"], {"kind": "run_preparation_recovery_retired",
                    "run_id": run_id, "preparation": recovery}]
            state.update(stage="formal_prepare", run_index=1, confirmation_authorized=True,
                         confirmation_basis=eligibility, runs=runs, waiting=None)
            return _save(store, record, state, "experiment_single_confirmation_authorized")
        state.update(status="accepted", stage="complete", runs=runs, waiting=None,
                     result={"runs": runs, "confirmation_dispatched": len(runs) > 1,
                             "confirmation_eligibility": eligibility, "platform_status": "unknown",
                             "model_validity": observed.get("model_validity"), "substantive_next_decision": "registered controller"})
        return _save(store, record, state, "experiment_original_chain_received")
    raise TaskflowError("unknown bounded experiment lifecycle stage: " + str(stage))


def advance_experiment(store: object, record: dict) -> dict:
    """Advance one durable stage; the calling lifecycle alone dispatches work."""
    if record["state"]["status"] in ("accepted", "exception"):
        return record
    try:
        validate_evidence(record["evidence"])
        return _advance(store, record)
    except (TaskflowError, OSError, ValueError, KeyError, TypeError) as exc:
        latest = store.get_flow(record["flow_id"])
        failed = {**latest["state"], "status": "exception",
                  "exception": {"stage": latest["state"]["stage"], "reason": str(exc)}}
        return _save(store, latest, failed, "experiment_exception")
