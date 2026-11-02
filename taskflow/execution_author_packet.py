"""Explicit source/staged transport for an unchanged author business packet.

The sidecar authorizes metadata transport, not a new business instruction. No
path is inferred from a basename, digest peer or a global ledger search.
"""

from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, parse_json, sha256
from taskflow.execution_runner import _info, _read, _regular, _trace, _validate, _validated_collection
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.packages import check_task


TARGET = "protocol/current-author-packet.json"
KIND = "author_packet_source_metadata_alias_v1"


def _require(value, message):
    if not value:
        raise TaskflowError("author packet transport: " + message)


def _ref(value):
    reference = validate_evidence([value])[0]
    actual = _info(Path(reference["path"]))
    _require(actual["sha256"] == reference["sha256"], "reference bytes changed")
    return {**reference, "size": actual["size"]}


def _same(left, right):
    return all(left.get(key) == right.get(key) for key in ("path", "sha256"))


def validate_author_packet_transport(*, packet_transport: dict, authorization: dict,
                                     source_flow_id: str, store=None,
                                     require_complete: bool = False) -> dict:
    """Rebuild a deterministic witness from exact locked originals, read only.

    Online validation additionally reads the real managed work/attempt/candidate.
    Offline use verifies the retained lifecycle specification and sealed runtime;
    it makes no claim to have independently queried the ledger.
    """
    reference = _ref(packet_transport)
    spec = parse_json(_regular(Path(reference["path"])), "author packet transport")
    fields = {"schema_version", "kind", "source_author_flow_id", "source_author_attempt",
              "authorization", "lifecycle_spec", "manifest", "source", "staged", "evidence"}
    _require(isinstance(spec, dict) and set(spec) == fields
             and type(spec["schema_version"]) is int and spec["schema_version"] == 1
             and spec["kind"] == KIND and spec["source_author_flow_id"] == source_flow_id,
             "exact typed alias sidecar required")
    refs = {key: _ref(spec[key]) for key in
            ("authorization", "lifecycle_spec", "manifest", "source", "staged")}
    authority_ref = _ref(authorization)
    _require(_same(refs["authorization"], authority_ref), "different original experiment authority")
    evidence = validate_evidence(spec["evidence"])
    _require(bool(evidence), "explicit metadata-adoption evidence required")
    lifecycle = _read(Path(refs["lifecycle_spec"]["path"]))
    definition = lifecycle.get("definition", {})
    _require(definition.get("flow_id") == source_flow_id
             and definition.get("kind") == "author_quality_v1"
             and definition.get("source_author_attempt") == spec["source_author_attempt"],
             "different retained original author definition")
    if store is not None:
        actual_parent = store.get_flow(source_flow_id)
        _require(canonical(actual_parent["definition"]) == canonical(definition),
                 "retained author definition differs from the actual parent")
    work_root = Path(definition["source_author_work"])
    runtime = work_root.parent / "runtime"
    _require(refs["lifecycle_spec"]["path"] == str(work_root.parent / "lifecycle-spec.json")
             and refs["manifest"]["path"] == str(runtime / "manifest.json"),
             "foreign lifecycle or runtime source")
    manifest = _validate(runtime)
    _require(_info(runtime / "manifest.json") == {key: refs["manifest"][key] for key in ("sha256", "size")},
             "manifest reference differs from its locked bytes")
    authority = _read(Path(authority_ref["path"]))
    matches = [row for row in authority.get("items", []) if isinstance(row, dict)
               and row.get("task_id") == definition["task_id"]
               and row.get("source_author_quality_flow") == source_flow_id]
    _require(authority.get("controller") == definition["owner"] and len(matches) == 1,
             "original authority has no unique same-owner author item")
    item = matches[0]
    binding = definition["source_author_binding"]
    source_authority = _ref(definition["authorization"])
    _require(item.get("source_author_attempt") == spec["source_author_attempt"]
             and item.get("source_author_intent") == binding["intent_id"]
             and isinstance(item.get("source_author_sid"), str) and bool(item["source_author_sid"])
             and _same(source_authority, refs["source"]), "author/intent/original source differs")
    entries = [row for row in manifest["inputs"] if row["target"] == TARGET]
    _require(len(entries) == 1 and entries[0]["kind"] == "source_metadata",
             "unique typed original packet input required")
    entry = entries[0]
    staged = runtime / "inputs" / TARGET
    _require(entry["source"] == source_authority["path"] == refs["source"]["path"]
             and refs["staged"]["path"] == item.get("source_business_packet") == str(staged)
             and item.get("source_business_packet_sha256") == source_authority["sha256"]
             == refs["source"]["sha256"] == refs["staged"]["sha256"] == entry["sha256"],
             "declared alias is not this exact source/staged input")
    info = {key: entry[key] for key in ("sha256", "size")}
    source_bytes, staged_bytes = _regular(Path(entry["source"])), _regular(staged)
    _require(source_bytes == staged_bytes and _info(staged) == _info(Path(entry["source"])) == info,
             "source/staged bytes or size differ")
    body = parse_json(source_bytes, "original author business packet")
    _require(body.get("controller") == definition["owner"]
             and body.get("items") == [definition["business_item"]]
             and body.get("output_contract") == definition["author_contract"]
             and body.get("preauthorized_normal_successors") == definition["next"],
             "original controller/business body or normal branches differ")
    business = definition["business_item"]
    baseline = check_task(Path(business["baseline_root"]), write_report=False)
    candidate_binding = manifest.get("candidate_binding", {})
    _require(not baseline["errors"]
             and business.get("task_id") == definition["task_id"] == binding["task_id"] == baseline["task_id"]
             and business.get("intent_id") == binding["intent_id"]
             and business.get("candidate_id") == binding["candidate_id"]
             and candidate_binding.get("candidate_id") == binding["candidate_id"]
             and all(candidate_binding.get(key) == baseline[key]
                     for key in ("task_id", "input_digest", "content_digest", "evaluation_digest"))
             and all(manifest.get(key) == value for key, value in {
                 "role": "author", "intent_id": binding["intent_id"],
                 "policy_version": definition["policy_id"], "cwd": str(work_root)}.items()),
             "original candidate/work/intent/policy binding differs")
    factory_spec = _read(work_root.parent / "spec.json")
    work = factory_spec.get("work", {})
    _require(all(work.get(key) == binding[key] for key in ("task_id", "candidate_id", "intent_id", "work_id"))
             and work.get("role") == "author" and work.get("policy_id") == definition["policy_id"]
             and work.get("dispatch_owner") == definition["owner"], "original factory work binding differs")
    if store is not None:
        from taskflow.execution import _check_work_binding
        from taskflow.execution_author import _author

        attempt, bound_manifest = _author(store, definition)
        managed_work = store.get_work(attempt["work_id"])
        candidate = store.get_candidate(attempt["candidate_id"])
        _check_work_binding(managed_work, candidate, bound_manifest)
        _require(bound_manifest == manifest and attempt["runtime_path"] == str(runtime)
                 and attempt.get("cli_sid") == item["source_author_sid"]
                 and attempt["state"] in {"running", "collecting", "completed"},
                 "actual managed author/CLI context differs or is unresolved")
    if require_complete:
        collected = _validated_collection(runtime, manifest)
        trace = _trace(runtime / "trace.jsonl", item["source_author_sid"])
        _require(all(collected.get(key) is True for key in
                     ("technical_accepted", "process_ended", "model_completed", "delivery_complete"))
                 and collected.get("sid") == item["source_author_sid"] and trace["model_completed"],
                 "complete sealed original author context required")
    return {"schema_version": 1, "kind": KIND, "transport": reference,
            "source_author_flow_id": source_flow_id, "source_author_attempt": spec["source_author_attempt"],
            "source_author_binding": binding, "source_author_sid": item["source_author_sid"],
            "candidate_binding": candidate_binding, "policy_id": definition["policy_id"],
            "definition_digest": sha256(canonical(definition)), "authorization": authority_ref,
            "lifecycle_spec": refs["lifecycle_spec"], "factory_spec": evidence_ref(work_root.parent / "spec.json", "original author factory specification"),
            "manifest": refs["manifest"], "request": evidence_ref(runtime / "request.md", "unchanged actual author request"),
            "manifest_input": entry, "source": refs["source"], "staged": refs["staged"],
            "evidence": evidence}


def author_packet_matches(store, *, parent: dict, packet: dict, authorization: dict,
                          packet_transport: dict | None = None) -> bool:
    """Keep legacy exact-path behavior; an alias needs the explicit sidecar."""
    original = parent["definition"]["authorization"]
    if packet_transport is None:
        return (original["path"] == packet["source_business_packet"]
                and original["sha256"] == packet["source_business_packet_sha256"])
    witness = validate_author_packet_transport(packet_transport=packet_transport, authorization=authorization,
        source_flow_id=parent["flow_id"], store=store)
    return (packet.get("source_business_packet") == witness["staged"]["path"]
            and packet.get("source_business_packet_sha256") == witness["staged"]["sha256"])
