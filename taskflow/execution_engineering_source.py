"""Honest engineering-root provenance for an existing public/quality/formal chain."""

from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_runner import (
    _full_tree, _full_verify, _info, _read, _validate, _validated_collection,
)
from taskflow.execution_store import evidence_ref, validate_evidence


KIND = "engineering_candidate_quality_v1"
BINDING = ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")
PUBLIC_IDENTITY = ("attempt_id", "work_id", "intent_id", "candidate_id", "task_id", "cli_sid")


def binding(candidate: dict) -> dict:
    return {key: candidate[key] for key in BINDING}


def engineering_packet(definition: dict, packet: dict) -> dict:
    """Resolve a separately authorized experiment item, with no author aliases."""
    import copy

    selector = {"task_id": definition["task_id"], "source_quality_flow": definition["source_flow_id"]}
    items = packet.get("items")
    matches = [row for row in items if isinstance(row, dict)
               and all(row.get(key) == value for key, value in selector.items())] if isinstance(items, list) else []
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or packet.get("controller") != definition["owner"] or len(matches) != 1
            or definition.get("business_packet_item") != selector
            or definition.get("source_author_flow_id") is not None):
        raise TaskflowError("Engineering experiment requires one exact non-author source authority item")
    item = matches[0]
    fields = ("first_experiment_business_unknown", "version_id", "first_run_id", "first_evaluation_id",
              "conditional_one_confirmation_run_id", "conditional_one_confirmation_evaluation_id")
    if (item.get("source_kind") != KIND or any(key.startswith("source_author") for key in item)
            or any(not isinstance(item.get(key), str) or not item[key].strip() for key in fields)
            or not packet.get("preauthorized_normal_chain") or not packet.get("prerequisites")):
        raise TaskflowError("Engineering experiment must preserve its real source and explicit normal-chain purpose")
    review = copy.deepcopy(definition["technical"].get("independent_review", {}))
    review["score_id"] = item["first_evaluation_id"]
    confirmation = packet.get("confirmation", {})
    return {**packet, **item,
            "business_investment": {"action": packet["preauthorized_normal_chain"],
                                    "unresolved_business_question": item["first_experiment_business_unknown"]},
            "release": {"version_id": item["version_id"]}, "formal": {"first_run_id": item["first_run_id"]},
            "independent_review": review, "quality_and_candidate_gate": {"required": True, "no_implicit_experiment": True},
            "single_confirmation": {"authorized_if": confirmation.get("only_if", ""),
                "max_additional_attempts": confirmation.get("maximum_additional_runs_per_first"),
                "run_id": item["conditional_one_confirmation_run_id"],
                "score_id": item["conditional_one_confirmation_evaluation_id"]}}


def engineering_source(store: object, definition: dict, item: dict, *, require_candidate: bool = False) -> dict:
    """Verify a real root and its unchanged public predecessor; never launch."""
    from taskflow.execution_flow import _predecessor
    from taskflow.execution_runner import archive_full_attempt
    from taskflow.execution_solver_profile import source_policy_id
    from taskflow.packages import check_task

    source_id = definition["source_flow_id"]
    source = store.get_flow(source_id)
    spec, state = source["definition"], source["state"]
    initial = store.get_candidate(spec["initial"]["candidate_id"])
    policy_id = source_policy_id(store, definition)
    if (definition.get("source_kind") != KIND or item.get("source_kind") != KIND
            or item.get("source_quality_flow") != source_id
            or spec.get("kind") is not None
            or any(spec.get(key) != definition[key] for key in ("task_id", "owner"))
            or spec.get("policy_id") != policy_id
            or initial.get("author_attempt") is not None or initial.get("parent_id") is not None
            or initial.get("external_author_sids")
            or item.get("initial_candidate_binding") != binding(initial)
            or spec["roles"]["quality"].get("scope") != "whole_package"
            or spec["roles"]["quality"]["runner"].get("resume_sid")
            or (spec["roles"]["quality"]["runner"].get("quality_session") or {}).get("previous")
            or spec["initial"].get("expected_digests") != {key: initial[key] for key in
                                                         ("input_digest", "content_digest", "evaluation_digest")}):
        raise TaskflowError("Engineering formal source requires its honest root and exact ordinary whole-quality flow")
    origin_ref = validate_evidence([item["engineering_origin"]])[0]
    origin = _read(Path(origin_ref["path"]))
    if (type(origin.get("schema_version")) is not int or origin["schema_version"] != 1
            or origin.get("kind") != "engineering_candidate_origin_v1"
            or origin.get("candidate_created_by") != "engineering"
            or origin.get("candidate_binding") != binding(initial)
            or "author_attempt" not in origin or origin["author_attempt"] is not None
            or "parent_id" not in origin or origin["parent_id"] is not None
            or not isinstance(origin.get("reason"), str) or not origin["reason"].strip()):
        raise TaskflowError("Engineering formal source lacks explicit engineering origin without invented author/parent")
    validate_evidence(origin.get("evidence"))
    check = check_task(Path(spec["initial"]["candidate_root"]), write_report=False)
    if check["errors"] or any(check[key] != initial[key] for key in BINDING if key != "candidate_id"):
        raise TaskflowError("Engineering source root bytes changed after capture")
    public, manifest = _predecessor(store, spec)
    if (item.get("public_predecessor") != {key: public[key] for key in PUBLIC_IDENTITY}
            or public.get("state") != "completed" or public.get("role") != "quality"
            or public.get("technical_accepted") is not True or not public.get("cli_sid")
            or public.get("response_complete") is not True or public.get("artifacts_complete") is not True
            or type(public.get("process_exit")) is not int or public["process_exit"] != 0
            or manifest.get("candidate_binding", {}).get("input_digest") != initial["input_digest"]):
        raise TaskflowError("Engineering formal source lacks its original completed same-public predecessor")
    if manifest.get("mode") != "test_fixture":
        from taskflow.execution_batch_binding import validate_batch_source

        if public.get("model_slot") != "public_rebuild" or not validate_batch_source(public, manifest, store):
            raise TaskflowError("Engineering predecessor is not its actual confirmed public role")
    if state.get("status") == "exception" or state.get("pending_business_scope"):
        raise TaskflowError("Engineering quality source has an unresolved exception or business scope")
    if require_candidate and (state.get("status") != "accepted" or not state.get("receipt")):
        raise TaskflowError("Engineering quality source has not been independently accepted")
    proof = None
    if state.get("status") == "accepted":
        receipt = validate_evidence([evidence_ref(Path(state["receipt"]), "actual ordinary quality flow result")])[0]
        body = _read(Path(receipt["path"]))
        if (body.get("flow_id") != source_id or body.get("status") != "accepted"
                or body.get("controller") != definition["owner"]
                or body.get("candidate_id") != state["candidate_id"] or body.get("history") != state.get("history")
                or any(row.get("cli_sid") == public["cli_sid"] for row in state.get("history", [])
                       if row.get("kind") == "independent_review")):
            raise TaskflowError("Engineering quality receipt differs from its actual fresh source")
        # The phase cursor moves to a repair/recheck archive. Initial public
        # provenance always belongs to the archive fixed by its original ID.
        archive = Path(spec["directory"]) / "archives" / public["attempt_id"]
        if not (archive / "archive-binding.json").is_file() or not (archive / "supplemental/receipt.json").is_file():
            raise TaskflowError("Engineering predecessor requires its existing complete full archive")
        archive_full_attempt(Path(public["runtime_path"]), archive)
        proof = {"schema_version": 1, "kind": KIND, "source_quality_flow": source_id,
                 "controller": definition["owner"], "initial_binding": binding(initial), "initial_declaration": evidence_ref(
                     Path(spec["initial"]["candidate_root"]) / "task.json", "actual captured engineering root declaration"),
                 "engineering_origin": origin_ref, "public_predecessor": {key: public[key] for key in PUBLIC_IDENTITY},
                 "public_source_binding": manifest["candidate_binding"],
                 "quality_receipt": receipt,
                 "public_manifest": evidence_ref(archive / "manifest.json", "original public manifest, original candidate"),
                 "public_collection": evidence_ref(archive / "collection.json", "original completed public collection"),
                 "public_archive_binding": evidence_ref(archive / "archive-binding.json", "complete public archive binding"),
                 "public_supplemental_receipt": evidence_ref(archive / "supplemental/receipt.json", "complete original work and scratch receipt")}
        validate_frozen_engineering_source(proof)
    return {"source": source, "initial": initial, "excluded_sids": {public["cli_sid"]},
            "binding": {"kind": KIND, "source_flow_id": source_id, "engineering_source": proof}}


def validate_frozen_engineering_source(proof: dict) -> None:
    """Verify the sealed public/engineering evidence during offline score intake."""
    if (not isinstance(proof, dict) or type(proof.get("schema_version")) is not int
            or proof["schema_version"] != 1 or proof.get("kind") != KIND):
        raise TaskflowError("Explicit frozen engineering source required")
    from taskflow.packages import check_task

    declaration = Path(proof["initial_declaration"]["path"])
    check = check_task(declaration.parent, write_report=False)
    if (declaration.name != "task.json" or check["errors"]
            or any(check[key] != proof["initial_binding"][key] for key in BINDING if key != "candidate_id")):
        raise TaskflowError("Frozen engineering initial bytes differ from their actual captured binding")
    names = ("initial_declaration", "engineering_origin", "quality_receipt", "public_manifest",
             "public_collection", "public_archive_binding", "public_supplemental_receipt")
    validate_evidence([proof[key] for key in names])
    archive = Path(proof["public_manifest"]["path"]).parent
    if (proof["public_collection"]["path"] != str(archive / "collection.json")
            or proof["public_archive_binding"]["path"] != str(archive / "archive-binding.json")
            or proof["public_supplemental_receipt"]["path"] != str(archive / "supplemental/receipt.json")):
        raise TaskflowError("Engineering public source does not retain one complete original archive")
    manifest = _validate(archive)
    collection = _validated_collection(archive, manifest)
    bound = _read(archive / "archive-binding.json")
    tree = _full_tree(archive)
    files = {name: {key: row[key] for key in ("sha256", "size")}
             for name, row in tree["files"].items() if name != "archive-binding.json"}
    directories = sorted(name.rstrip("/") for name in tree["directories"] if name != ".")
    origin = _read(Path(proof["engineering_origin"]["path"]))
    quality = _read(Path(proof["quality_receipt"]["path"]))
    if (tree["special"] or bound.get("files") != files or bound.get("directories") != directories
            or bound.get("intent_id") != proof["public_predecessor"]["intent_id"]
            or bound.get("collection") != _info(archive / "collection.json")
            or manifest.get("candidate_binding") != proof["public_source_binding"]
            or proof["public_source_binding"]["input_digest"] != proof["initial_binding"]["input_digest"]
            or collection.get("sid") != proof["public_predecessor"]["cli_sid"]
            or collection.get("intent_id") != proof["public_predecessor"]["intent_id"]
            or not all(collection.get(key) is True for key in
                       ("technical_accepted", "model_completed", "delivery_complete", "process_ended"))
            or origin.get("candidate_binding") != proof["initial_binding"]
            or origin.get("candidate_created_by") != "engineering" or origin.get("author_attempt") is not None
            or origin.get("parent_id") is not None
            or quality.get("flow_id") != proof["source_quality_flow"] or quality.get("status") != "accepted"
            or quality.get("controller") != proof["controller"]
            or any(row.get("cli_sid") == collection["sid"] for row in quality.get("history", [])
                   if row.get("kind") == "independent_review")):
        raise TaskflowError("Frozen engineering source differs from its actual original public/quality provenance")
    intent = _read(archive / "supplemental-intent.json")
    if (intent.get("intent_id") != proof["public_predecessor"]["intent_id"]
            or intent.get("manifest") != _info(archive / "manifest.json")
            or intent.get("collection") != _info(archive / "collection.json")):
        raise TaskflowError("Frozen engineering full archive differs from its original source binding")
    _full_verify(archive / "supplemental", intent, True)
