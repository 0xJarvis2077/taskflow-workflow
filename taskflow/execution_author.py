"""Preauthorized author outcomes and fresh public/quality handoffs.

This is a cursor adapter on the existing ledger. It never launches a model or
recovers an original worker that still owns its completion window.
"""

import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import time

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, parse_json, relative_path, sha256, validate_id
from taskflow.execution import (
    ReceiptEvidenceError, ReceiptResultUncertain, ReceiptWritePending, capture_candidate, enqueue_spec, run_worker,
)
from taskflow.execution_flow import (
    _binding, _get_or_none, _identity, _public_prepare, _publish_once, _render,
    advance_workflow, register_workflow,
)
from taskflow.execution_runner import _info, _publish, _read, _regular, _validated_collection, archive_full_attempt
from taskflow.execution_store import ACTIVE, evidence_ref, validate_evidence
from taskflow.packages import check_task


INFORMED_PUBLIC_MATERIAL_GUIDANCE = (
    "Public-material responsibility boundary for informed authors and teachers: authors and repairers keep the actual "
    "private job, reference and normal legitimate alternatives complete. Independent quality/recheck reviewers first "
    "form their own judgment from public originals, then compare the existing complete private work within their signed "
    "scope; they do not produce a replacement answer and self-sign it. Keep sufficient contemporaneously available original records, necessary "
    "rules and institutional conditions, field definitions and formulas, stable business keys, timing, permissions and "
    "association evidence. Reduce pre-solved analysis: leave the intended object allocation, relationship mapping, "
    "applicability and premise checks, evidence selection, method choice, case-specific arrangements and reconciliation "
    "of competing schemes to the recipient. Verification conditions are not this case's verification conclusion. "
    "If completeness, applicability or absence of cross-object interference is to be tested, supply the originals needed "
    "to verify or refute it rather than an unconditional guarantee.\n"
    "Preserve genuinely authorized upstream results with their actual source and scope, and deduct the work they already "
    "perform from claimed independent responsibility. When the task only executes given premises, state that limited "
    "scope honestly. Keep necessary facts, conditions, normal links and stable keys; do not create difficulty by deletion, "
    "disorder, artificial conflicts or forced lengthy methods. If a relevant relationship remains to be checked, provide "
    "its sufficient original evidence instead of merely deleting the conclusion or inventing a unique outcome. Keep "
    "sourced version differences and local opinions; verify their applicability against the originals and allow supported "
    "conditional branches instead of pre-filling a final reconciliation.\n"
    "In existing responsibility/issue records trace the exact original location and wording -> actual change or evidenced "
    "retention -> remaining judgment -> business consequence -> recheck of retained conditions and legitimate branches. "
    "Use the same original materials to verify what was really left to the recipient. This guidance adds no deliverable, "
    "score rule, approval layer or claim that a rendered instruction proves business quality."
)


def _factory_reference(reference: dict, label: str) -> tuple[dict, dict]:
    source = {**reference, "location": reference.get("location", label)}
    validated = validate_evidence([source])[0]
    return validated, _read(Path(validated["path"]))


def _factory_paths(value: object, replacements: dict[str, str]) -> object:
    if isinstance(value, dict):
        return {_factory_paths(key, replacements): _factory_paths(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_factory_paths(item, replacements) for item in value]
    if isinstance(value, str):
        for old, new in sorted(replacements.items(), key=lambda item: -len(item[0])):
            if value == old or value.startswith(old + "/"):
                return new + value[len(old):]
    return value


def _factory_owner(store: object, task_id: str, owner: str, refs: list[dict]) -> None:
    context = store.dispatch_ownership_snapshot(task_id)
    general = context["ownership"]
    if general is not None:
        if general["owner"] != owner:
            raise TaskflowError("author factory cannot take another controller's future dispatch ownership")
        return
    observed = context["observation"]
    if observed is not None:
        if observed["owner"] != owner:
            raise TaskflowError("author factory packet differs from the observed controller")
        store.transfer_owner(task_id, owner, owner, refs, scope="future_dispatch")


def start_author_from_packet(store: object, *, packet_ref: dict, template_spec_ref: dict,
                             directory: Path, allow_model: bool,
                             factory_config: dict | None = None,
                             retry_failed_probes: list[str] | None = None) -> dict:
    """Prepare one authorized author and consume its registered normal outcomes.

    A ready-only call never claims work. A later identical call can consume that
    exact intent, while a recorded attempt is always adopted without relaunch.
    """
    from taskflow.execution_batch import policy_setting, validate_batch_policy
    from taskflow.execution_lifecycle import run_lifecycle

    if type(allow_model) is not bool:
        raise TaskflowError("author factory allow_model must be an explicit boolean")
    if retry_failed_probes is not None and allow_model:
        raise TaskflowError("native probe recovery must not dispatch a model")
    directory = Path(directory).absolute()
    if factory_config is None:
        intent_path = directory / "factory-intent.json"
        if not intent_path.is_file() or intent_path.is_symlink():
            raise TaskflowError("new author intent requires an explicit factory configuration")
        original_factory = _read(intent_path)
        factory_config = _read(Path(__file__).resolve().parents[1] / "config/execution-author-factory-v1.json")
        if (not isinstance(original_factory, dict)
                or original_factory.get("factory_config_digest") != sha256(canonical(factory_config))):
            raise TaskflowError("original author factory configuration is no longer available; provide its exact binding")
    if not isinstance(factory_config, dict) or type(factory_config.get("schema_version")) is not int or factory_config["schema_version"] != 1:
        raise TaskflowError("author factory schema_version=1 required")
    packet_source, packet = _factory_reference(packet_ref, "current once-only author business packet")
    template_source, template = _factory_reference(template_spec_ref, "approved native author infrastructure template")
    configured_template = factory_config.get("template_spec_ref")
    if configured_template and any(configured_template.get(key) != template_source[key] for key in ("path", "sha256")):
        raise TaskflowError("author template differs from the approved factory configuration")
    role_source, _ = _factory_reference(factory_config["role_template_ref"], "approved conditional public/quality role infrastructure")
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or not isinstance(packet.get("items"), list) or len(packet["items"]) != 1):
        raise TaskflowError("one exact author business item is required per factory invocation")
    item = packet["items"][0]
    task_id = item["task_id"]
    intent_id = item["intent_id"]
    candidate_id = item["candidate_id"]
    owner = packet["controller"]
    for value in (task_id, intent_id, candidate_id, owner, factory_config["policy_id"]):
        validate_id(value, "author factory identity")
    policy = store.get_policy(factory_config["policy_id"])["config"]
    snapshot = validate_batch_policy(policy)
    if retry_failed_probes is not None and snapshot is None:
        raise TaskflowError("author probe recovery requires a confirmed batch policy")
    if snapshot is not None:
        if canonical(factory_config.get("batch_snapshot")) != canonical(policy["batch_snapshot"]):
            raise TaskflowError("author factory differs from the registered confirmed batch snapshot")
        if packet.get("batch_id") != snapshot["batch_id"]:
            raise TaskflowError("author packet differs from its explicit confirmed batch")
        if not isinstance(factory_config.get("policy_record_ref"), dict):
            raise TaskflowError("author batch factory needs its original policy record reference")
        _, registered = _factory_reference(factory_config["policy_record_ref"], "original confirmed batch policy record")
        if (registered.get("operation") != "register_policy"
                or registered.get("arguments", {}).get("policy_id") != factory_config["policy_id"]
                or canonical(registered.get("arguments", {}).get("config")) != canonical(policy)):
            raise TaskflowError("author factory differs from its original registered batch policy")
        setting = policy_setting(policy, "author", "author", task_id)
    else:
        if "batch_snapshot" in factory_config:
            raise TaskflowError("batch author factory requires its registered confirmed policy")
        setting = policy_setting(policy, "author")
    required = packet["output_contract"]["required_outputs"]
    if set(required) != {"research.md", "candidate-decision.md", "handoff.md", "handoff.json", "issues.json"}:
        raise TaskflowError("author packet must retain the complete five-file outcome contract")
    if not all(packet["preauthorized_normal_successors"].get(key) for key in
               ("candidate_complete", "research_ended", "needs_business_decision")):
        raise TaskflowError("author packet must authorize its three actual outcome branches")
    baseline = Path(item["baseline_root"]).absolute()
    if factory_config.get("research_root") and not directory.is_relative_to(Path(factory_config["research_root"]).absolute()):
        raise TaskflowError("author factory directory is outside the approved research root")
    if (".." in directory.parts or any(path.is_symlink() for path in (directory, *directory.parents))
            or directory.is_relative_to(baseline) or baseline.is_relative_to(directory)):
        raise TaskflowError("author factory needs a new separate ordinary work root")
    identity = {"schema_version": 1, "packet": packet_source, "template": template_source,
                "factory_config_digest": sha256(canonical(factory_config)), "directory": str(directory),
                "task_id": task_id, "intent_id": intent_id, "candidate_id": candidate_id}
    if directory.exists() and not (directory / "factory-intent.json").exists() and any(directory.iterdir()):
        raise TaskflowError("author factory directory contains unbound prior work")
    _publish_once(directory / "factory-intent.json", identity)
    actual = check_task(baseline, write_report=False)
    if actual["errors"] or actual["task_id"] != task_id:
        raise TaskflowError("author packet baseline is not a structurally complete same-task candidate")
    candidate = _get_or_none(store.get_candidate, candidate_id)
    if candidate is None:
        candidate = capture_candidate(store, baseline, candidate_id)
    if any(candidate[key] != actual[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("author factory baseline differs from the registered stable candidate")
    work, scratch, runtime = directory / "work", directory / "scratch", directory / "runtime"
    lifecycle_id = validate_id(intent_id + "-author-quality", "author lifecycle_id")
    lifecycle_root = Path(factory_config["lifecycle_root"]).absolute() / lifecycle_id
    if any(lifecycle_root.is_relative_to(path) or path.is_relative_to(lifecycle_root) for path in (work, scratch, runtime, baseline)):
        raise TaskflowError("author lifecycle archives must be separate from original author/baseline roots")
    work.mkdir(exist_ok=True)
    scratch.mkdir(exist_ok=True)
    original = template["runner"]
    if original.get("mode") != "native_role" or original.get("role") != "author":
        raise TaskflowError("author factory requires the previously verified native author template")
    runner = {key: copy.deepcopy(original[key]) for key in
              ("mode", "cli", "model", "reasoning_effort", "timeout_seconds", "native_permissions")}
    runner["dependencies"] = copy.deepcopy(original.get("dependencies", []))
    runner["reasoning_selection"] = original.get("reasoning_selection", "explicit")
    runner["native_permissions"] = _factory_paths(runner["native_permissions"], {
        original["cwd"]: str(work), original["native_permissions"]["scratch"]: str(scratch),
        str(Path(original["cwd"]).parent / "runtime"): str(runtime),
        "{{work}}": str(work), "{{scratch}}": str(scratch), "{{runtime}}": str(runtime)})
    runner.update(intent_id=intent_id, policy_version=factory_config["policy_id"], role="author", cwd=str(work),
                  outputs=list(required), output_trees=[], inputs=[
                      {"path": str(baseline / "task.json"), "kind": "source_metadata", "target": "execution/task.json"},
                      {"path": packet_source["path"], "kind": "source_metadata", "target": "protocol/current-author-packet.json"}])
    runner["inputs"] += [{"path": source["path"], "kind": "public" if source["role"] in PUBLIC_ROLES else "teacher",
                          "target": source["package_path"]} for source in candidate["files"]]
    if snapshot is not None:
        runner.update({key: setting[key] for key in ("cli", "model", "reasoning_effort", "timeout_seconds", "reasoning_selection", "reasoning_context")})
        runner["model_slot"] = "author"
    elif any(runner[key] != setting[key] for key in ("model", "reasoning_effort", "timeout_seconds")):
        raise TaskflowError("approved author template differs from the current versioned author policy")
    runner["request_sections"] = [{"kind": "operation", "text":
        "Act as a fresh independent informed research author. Read the current staged packet and necessary complete actual originals. "
        "The project is read-only; write only your own work and scratch. Keep normal professional tools and lawful alternatives. "
        "Do not start other model requests, model CLIs, agents or chats, change shared skills or old originals/scores, purchase services, submit externally or operate production. "
        "Ordinary shell, Node, Python, SQL and analysis tools remain allowed. "
        "First complete the private job/reference/legitimate alternatives/normal and wrong controls, then report the actual disposition; do not manufacture a candidate.\n"
        + INFORMED_PUBLIC_MATERIAL_GUIDANCE + "\n"
        + json.dumps({"author_work": packet["author_work"], "current_business_item": item,
                      "output_contract": packet["output_contract"],
                      "preauthorized_normal_successors": packet["preauthorized_normal_successors"],
                      "permission": packet.get("permission")}, ensure_ascii=False)
        + "\nDeliver all five declared files, including schema_version=1 handoff.json with exact task_id, "
          "disposition candidate_complete/research_ended/needs_business_decision, candidate_subpath next-task/" + task_id
        + " only when complete (otherwise null), a concrete reason/source_evidence, the boolean needs_business_decision, "
          "independent_quality_passed=false and new_formal_score=null. A candidate is optional, not a required output tree."}]
    runner["request_sections"][0]["text"] += (
        " Declare candidate task.json sources only under draft/ or checks/, or as the single root file record.md. "
        "Place nested author provenance under draft/evaluation/author-provenance/ or checks/. "
        "Use the current task.json contract for candidate review metadata: declare the required business, artifact and calculation reviews, "
        "with unique task.reviews[].id values and unique task.reviews[].path values under reports/. "
        "Review placeholder files belong only in task.reviews, never in task.files; keep their paths separate from declared payloads. "
        "For each matching task.reviews entry r, write its r.path with schema_version=1, review_id=r.id, kind=r.kind, "
        "task_id=task.task_id, status=inconclusive, independent_quality_passed=false and new_formal_score=null. "
        "These are unsigned author-stage placeholders, not independent quality results. "
        "Additional placeholder metadata may use scope, limitations, evidence, authority, local_checks or local_evidence; "
        "keep content_digest, input_digest, evaluation_digest, independent_signer, signed_at_utc and formal_score absent or null. "
        "Do not add top-level role, reason, summary, independent, author_checked or signature/score fields to placeholders. "
        "Save complete author self-check observations separately as declared private provenance under draft/evaluation/author-provenance/ or checks/; "
        "local_checks may retain their original JSON. Optional local_evidence is a unique list of current declared private relative paths, "
        "not public, absolute or historical source paths. "
        "Keep scoring.rubric_file pointing to the declared files[].id whose role is rubric. ")
    work_id = validate_id(task_id + "-" + intent_id, "author work_id")
    refs = [packet_source, template_source, role_source]
    spec = {"work": {"work_id": work_id, "task_id": task_id, "candidate_id": candidate_id, "role": "author",
                     "policy_id": factory_config["policy_id"], "intent_id": intent_id,
                     "purpose": item["direction"] + " " + item.get("why", ""), "modifies": True,
                     "dispatch_owner": owner, "evidence": refs}, "runner": runner}
    if snapshot is not None:
        spec["work"]["model_slot"] = "author"
    published = directory / "spec.json"
    if published.exists():
        previous = _read(published)
        previous_runner = previous.get("runner")
        if not isinstance(previous_runner, dict) or not isinstance(previous_runner.get("request_sections"), list):
            raise TaskflowError("published author specification has no typed original request")
        expected = {**spec, "runner": {**runner, "request_sections": previous_runner["request_sections"]}}
        if canonical(previous) != canonical(expected):
            raise TaskflowError("published author specification differs beyond its original request sections")
        # The factory identity above already locks the packet, template and
        # configuration. A prompt-code update cannot replace their old request.
        spec = previous
    with _author_prepare_lock(directory):
        _publish_once(directory / "spec.json", spec)
        _factory_owner(store, task_id, owner, refs)
        queued = _get_or_none(store.get_work, work_id)
        if retry_failed_probes is not None:
            _authorize_failed_author_probe(store, directory, runtime, work_id, retry_failed_probes, queued)
        if queued is None:
            options = {"retry_failed_probes": sorted(retry_failed_probes)} if retry_failed_probes is not None else {}
            queued = enqueue_spec(store, spec, runtime, resume_prepared=True, probe_native=True, **options)["work"]
        else:
            _validated_prepared_author(store, spec, runtime)
    result = {"work_id": work_id, "candidate_id": candidate_id, "runtime": str(runtime), "spec": str(directory / "spec.json"),
              "factory_intent": str(directory / "factory-intent.json"), "model_calls_by_factory_preparation": 0}
    if not allow_model:
        return {**result, "state": queued["state"], "execution": "ready_requires_explicit_model_permission"
                if queued["state"] == "ready" else "existing_intent_retained"}
    with ThreadPoolExecutor(max_workers=1) as pool:
        def initial_worker() -> dict:
            try:
                return run_worker(store, "author-factory", True, [work_id])
            except (ReceiptWritePending, ReceiptEvidenceError, ReceiptResultUncertain):
                raise
            except Exception as error:
                original = _active_attempt(store, work_id)
                if original is not None and original["state"] in ACTIVE:
                    store.mark_uncertain(original["attempt_id"], "original author worker stopped: " + str(error))
                raise

        attempt = _active_attempt(store, work_id)
        future = pool.submit(initial_worker) if attempt is None else None
        while attempt is None:
            if future.done():
                attempt = _active_attempt(store, work_id)
                if attempt is not None:
                    break
                future.result()
                if attempt is None:
                    if store.get_work(work_id)["state"] == "cancelled":
                        return {**result, "state": "cancelled", "reason": "unclaimed original author intent was cancelled"}
                    time.sleep(1)
                    future = pool.submit(initial_worker)
                    continue
                break
            time.sleep(.01)
            attempt = _active_attempt(store, work_id)
        registered = register_prepared_author_lifecycle(store, directory=directory,
            factory_config=factory_config, attempt_id=attempt["attempt_id"])
        received = run_lifecycle(store, lifecycle_id, allow_model=True)
        if future is not None:
            try:
                future.result()
            except (ReceiptWritePending, ReceiptEvidenceError, ReceiptResultUncertain):
                raise
            except Exception:
                if received["state"]["status"] != "exception":
                    raise
        return {**result, "state": received["state"]["status"], "attempt_id": attempt["attempt_id"],
                "lifecycle_id": lifecycle_id, "lifecycle": received}


@contextmanager
def _author_prepare_lock(directory: Path):
    """Exclude only another preparation of this same author intent."""
    with (directory / ".author-prepare.lock").open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskflowError("this author preparation is already active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _authorize_failed_author_probe(store: object, directory: Path, runtime: Path, work_id: str,
                                   supplied: list[str], queued: dict | None) -> None:
    """Bind one explicit zero-model retry to all original failed reports."""
    from taskflow.execution_runner import _validate

    if (not isinstance(supplied, list) or not supplied
            or any(not isinstance(value, str) or len(value) != 64
                   or any(char not in "0123456789abcdef" for char in value) for value in supplied)
            or len(set(supplied)) != len(supplied)):
        raise TaskflowError("native probe retry requires unique lowercase SHA256 values")
    if queued is not None or store.list_attempts(work_id) or (runtime / "native-probe-ready.json").exists():
        raise TaskflowError("native probe retry requires an unqueued author without attempts or a ready seal")
    manifest = _validate(runtime)
    if manifest.get("mode") != "native_role":
        raise TaskflowError("native probe retry requires the original native runtime")
    reports = []
    for path in sorted(runtime.glob("native-probe-*.json")):
        raw = _regular(path)
        value = parse_json(raw, "original native probe report")
        if (not isinstance(value, dict) or value.get("passed") is not False or value.get("model_calls") != 0
                or value.get("manifest") != _info(runtime / "manifest.json")):
            raise TaskflowError("native probe retry requires original failed zero-model reports for this manifest")
        reports.append({"path": str(path), "sha256": sha256(raw), "size": len(raw)})
    if sorted(item["sha256"] for item in reports) != sorted(supplied):
        raise TaskflowError("native probe retry SHA256 values must match all original failed reports")
    authorization = {"schema_version": 1, "kind": "explicit_author_failed_native_probe_retry",
                     "work_id": work_id, "runtime": str(runtime), "reports": reports,
                     "manifest": _info(runtime / "manifest.json"),
                     "factory_intent": _info(directory / "factory-intent.json"),
                     "spec": _info(directory / "spec.json"), "model_calls_authorized": 0}
    path = directory / "probe-retry-authorizations" / (sha256(canonical(authorization)) + ".json")
    try:
        _publish(path, authorization)
    except FileExistsError as exc:
        raise TaskflowError("this native probe retry authorization was already consumed") from exc


def _validated_prepared_author(store: object, spec: dict, runtime: Path) -> tuple[dict, dict, dict]:
    """Read the queued original and its already prepared request without dispatch."""
    from taskflow.execution import _check_work_binding
    from taskflow.execution_runner import _validate

    queued = store.get_work(spec["work"]["work_id"])
    candidate = store.get_candidate(spec["work"]["candidate_id"])
    if (any(queued[key] != spec["work"][key] for key in
            ("task_id", "candidate_id", "intent_id", "role", "policy_id", "purpose", "dispatch_owner"))
            or queued["runtime_path"] != str(runtime)
            or queued.get("model_slot") != spec["work"].get("model_slot")):
        raise TaskflowError("author factory work is already bound to a different intent")
    if (type(queued.get("evidence")) is not list or type(spec["work"].get("evidence")) is not list
            or canonical(queued["evidence"]) != canonical(validate_evidence(spec["work"]["evidence"]))):
        raise TaskflowError("prepared author evidence differs from its original queued sources")
    manifest = _validate(runtime)
    _check_work_binding(queued, candidate, manifest)
    if manifest["mode"] == "native_role":
        from taskflow.execution_batch_binding import make_batch_binding

        prepared_config = copy.deepcopy(spec["runner"])
        prepared_config["candidate_binding"] = _binding(candidate)
        policy = store.get_policy(queued["policy_id"])
        batch = make_batch_binding(policy, "author", queued.get("model_slot"), queued["task_id"])
        if batch is not None:
            prepared_config.update(model_slot=batch["model_slot"], batch_binding=batch)
        if (manifest.get("preparation_config_digest") is not None
                and manifest["preparation_config_digest"] != sha256(canonical(prepared_config))):
            raise TaskflowError("queued native author configuration differs from its complete published specification")
    expected_inputs = [(str(Path(entry["path"]).absolute()), entry["kind"],
                        entry.get("target", f"{index:04d}-{Path(entry['path']).name}"))
                       for index, entry in enumerate(spec["runner"]["inputs"])]
    if expected_inputs != [(entry["source"], entry["kind"], entry["target"]) for entry in manifest["inputs"]]:
        raise TaskflowError("queued author inputs differ from the published specification")
    if any(_info(Path(entry["source"])) != {key: entry[key] for key in ("sha256", "size")}
           for entry in manifest["inputs"]):
        raise TaskflowError("queued author inputs differ from their original source bytes")
    request = [f"Role: author. Intent: {queued['intent_id']}."]
    request += [f"Input ({entry['kind']}): {runtime / 'inputs' / entry['target']}" for entry in manifest["inputs"]]
    for entry in spec["runner"]["request_sections"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("text"), str) or not isinstance(entry.get("kind"), str):
            raise TaskflowError("published author request sections are not typed original text")
        request.append(entry["text"])
    if (queued["request_digest"] != manifest["request"]["sha256"]
            or _regular(runtime / "request.md") != ("\n\n".join(request) + "\n").encode("utf-8")):
        raise TaskflowError("queued author request differs from its published original and locked digest")
    return queued, candidate, manifest


def register_prepared_author_lifecycle(store: object, *, directory: Path,
                                      factory_config: dict, attempt_id: str) -> dict:
    """Register one prepared original author; never prepare, claim or consume it."""
    from taskflow.execution_batch import policy_setting, validate_batch_policy
    from taskflow.execution_lifecycle import register_lifecycle

    directory = Path(directory).absolute()
    if (type(factory_config.get("schema_version")) is not int or factory_config["schema_version"] != 1
            or ".." in directory.parts or any(path.is_symlink() for path in (directory, *directory.parents))):
        raise TaskflowError("prepared author needs its original ordinary factory directory/configuration")
    identity = _read(directory / "factory-intent.json")
    packet_source, packet = _factory_reference(identity["packet"], "original prepared author authority")
    template_source, template = _factory_reference(identity["template"], "original prepared author template")
    role_source, _ = _factory_reference(factory_config["role_template_ref"], "approved conditional public/quality role infrastructure")
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or not isinstance(packet.get("items"), list) or len(packet["items"]) != 1
            or template.get("runner", {}).get("mode") != "native_role"
            or template["runner"].get("role") != "author"):
        raise TaskflowError("prepared author requires its one original packet item/native template")
    item = packet["items"][0]
    task_id, intent_id, candidate_id, owner = item["task_id"], item["intent_id"], item["candidate_id"], packet["controller"]
    expected_identity = {"schema_version": 1, "packet": packet_source, "template": template_source,
        "factory_config_digest": sha256(canonical(factory_config)), "directory": str(directory),
        "task_id": task_id, "intent_id": intent_id, "candidate_id": candidate_id}
    if (canonical(identity) != canonical(expected_identity)
            or any(factory_config.get("template_spec_ref", template_source).get(key) != template_source[key]
                   for key in ("path", "sha256"))
            or factory_config.get("research_root") and not directory.is_relative_to(
                Path(factory_config["research_root"]).absolute())):
        raise TaskflowError("prepared author factory identity/configuration differs from the original")
    work_id = validate_id(task_id + "-" + intent_id, "author work_id")
    runtime, work = directory / "runtime", directory / "work"
    lifecycle_id = validate_id(intent_id + "-author-quality", "author lifecycle_id")
    lifecycle_root = Path(factory_config["lifecycle_root"]).absolute() / lifecycle_id
    if any(lifecycle_root.is_relative_to(path) or path.is_relative_to(lifecycle_root)
           for path in (work, directory / "scratch", runtime, Path(item["baseline_root"]).absolute())):
        raise TaskflowError("author lifecycle archives must be separate from original author/baseline roots")
    refs = [packet_source, template_source, role_source]
    spec = _read(directory / "spec.json")
    expected_work = {"work_id": work_id, "task_id": task_id, "candidate_id": candidate_id,
        "role": "author", "policy_id": factory_config["policy_id"], "intent_id": intent_id,
        "purpose": item["direction"] + " " + item.get("why", ""), "modifies": True,
        "dispatch_owner": owner, "evidence": refs}
    policy = store.get_policy(factory_config["policy_id"])
    snapshot = validate_batch_policy(policy["config"])
    if snapshot is not None:
        if (factory_config.get("batch_snapshot") != policy["config"]["batch_snapshot"]
                or packet.get("batch_id") != snapshot["batch_id"]):
            raise TaskflowError("prepared author differs from its confirmed batch")
        expected_work["model_slot"] = "author"
    elif "batch_snapshot" in factory_config:
        raise TaskflowError("prepared batch author requires its original registered policy")
    if canonical(spec["work"]) != canonical(expected_work):
        raise TaskflowError("prepared author specification differs from its exact original packet")
    queued, candidate, manifest = _validated_prepared_author(store, spec, runtime)
    expected_inputs = [{"path": str(Path(item["baseline_root"]).absolute() / "task.json"),
                        "kind": "source_metadata", "target": "execution/task.json"},
                       {"path": packet_source["path"], "kind": "source_metadata", "target": "protocol/current-author-packet.json"}]
    expected_inputs += [{"path": row["path"], "kind": "public" if row["role"] in PUBLIC_ROLES else "teacher",
                         "target": row["package_path"]} for row in candidate["files"]]
    setting = policy_setting(policy["config"], "author", expected_work.get("model_slot"), task_id)
    if (canonical(spec["runner"]["inputs"]) != canonical(expected_inputs)
            or manifest["cwd"] != str(work) or manifest["role"] != "author"
            or any(spec["runner"].get(key) != value for key, value in
                   (("cwd", str(work)), ("intent_id", intent_id), ("role", "author"),
                    ("policy_version", factory_config["policy_id"])))
            or set(manifest["outputs"]) != {"research.md", "candidate-decision.md", "handoff.md", "handoff.json", "issues.json"}
            or any(spec["runner"].get(key) != setting[key] for key in
                   ("model", "reasoning_effort", "timeout_seconds"))
            or any(manifest["model"][key] != setting[name] for key, name in
                   (("requested", "model"), ("reasoning_effort", "reasoning_effort")))
            or manifest["timeout_seconds"] != setting["timeout_seconds"]):
        raise TaskflowError("prepared author input/output/runtime policy differs from its original")
    attempt = store.get_attempt(attempt_id)
    originals = store.list_attempts(work_id)
    if (len(originals) != 1 or originals[0]["attempt_id"] != attempt_id
            or any(attempt.get(key) != queued.get(key) for key in
                   ("work_id", "intent_id", "task_id", "candidate_id", "role", "policy_id", "runtime_path",
                    "request_digest", "dispatch_owner", "ownership_event_seq", "model_slot"))):
        raise TaskflowError("prepared author registration needs its one exact original claimed attempt")
    definition = {"schema_version": 1, "kind": "author_quality_v1", "flow_id": lifecycle_id,
                  "task_id": task_id, "owner": owner, "policy_id": factory_config["policy_id"],
                  "directory": str(lifecycle_root), "authorization": packet_source,
                  "source_author_attempt": attempt_id,
                  "source_author_binding": {key: attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")},
                  "source_author_work": str(work), "role_template_ref": role_source,
                  "business_item": item, "author_contract": packet["output_contract"],
                  "next": packet["preauthorized_normal_successors"]}
    _publish_once(directory / "lifecycle-spec.json", {"definition": definition, "evidence": refs})
    registered = register_lifecycle(store, definition, refs)
    after_registration = store.get_attempt(attempt_id)
    registration_path = directory / "lifecycle-registration.json"
    registration_identity = {"flow_id": lifecycle_id, "definition_digest": registered["definition_digest"],
                             "source_attempt_id": attempt_id}
    if registration_path.exists():
        previous = _read(registration_path)
        if any(previous.get(key) != value for key, value in registration_identity.items()):
            raise TaskflowError("author lifecycle registration belongs to a different original attempt")
    else:
        _publish_once(registration_path, {**registration_identity,
                     "source_state_at_registration": attempt["state"],
                     "source_state_after_registration": after_registration["state"]})
    return registered


def _save(store: object, record: dict, state: dict, kind: str) -> dict:
    if state == record["state"]:
        return record
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": kind})


def _wait(store: object, record: dict, stage: str, state_name: str) -> dict:
    state = dict(record["state"])
    state["waiting"] = {"stage": stage, "state": state_name}
    return _save(store, record, state, "author_successor_waiting")


def _author(store: object, definition: dict) -> tuple[dict, dict]:
    attempt = store.get_attempt(definition["source_author_attempt"])
    identity = {key: attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
    work = store.get_work(attempt["work_id"])
    candidate = store.get_candidate(attempt["candidate_id"])
    if (identity != definition["source_author_binding"] or attempt["task_id"] != definition["task_id"]
            or attempt["role"] != "author"):
        raise TaskflowError("author successor is bound to a different original author intent")
    for key in ("task_id", "candidate_id", "intent_id", "runtime_path", "policy_id", "request_digest"):
        if work[key] != attempt[key]:
            raise TaskflowError("author work/attempt binding differs: " + key)
    root = Path(attempt["runtime_path"])
    manifest = _read(root / "manifest.json")
    business = definition["business_item"]
    if (business.get("task_id", definition["task_id"]) != definition["task_id"]
            or business.get("intent_id", attempt["intent_id"]) != attempt["intent_id"]):
        raise TaskflowError("author business packet item belongs to another source intent/task")
    if (_read(root / "manifest-lock.json") != _info(root / "manifest.json")
            or manifest.get("candidate_binding") != _binding(candidate)
            or manifest.get("cwd") != definition["source_author_work"]
            or manifest.get("intent_id") != attempt["intent_id"]
            or manifest.get("role") != "author" or manifest.get("policy_version") != attempt["policy_id"]
            or manifest.get("request", {}).get("sha256") != attempt["request_digest"]
            or set(manifest.get("outputs", [])) != set(definition["author_contract"]["required_outputs"])):
        raise TaskflowError("original author manifest differs from its bound work/candidate")
    if any(attempt[key] != candidate[key] or work[key] != candidate[key] for key in ("input_digest", "content_digest")):
        raise TaskflowError("original author candidate version differs from its work")
    return attempt, manifest


def _handoff(archive: Path, definition: dict, author: dict) -> dict:
    manifest = _read(archive / "manifest.json")
    collection = _validated_collection(archive, manifest)
    if (collection.get("technical_accepted") is not True or collection.get("process_ended") is not True
            or not author.get("cli_sid") or collection.get("sid") != author["cli_sid"]):
        raise TaskflowError("original author handoff lacks a completed same-SID technical collection")
    if "handoff.json" not in collection["artifacts"]:
        raise TaskflowError("original author did not collect its required handoff.json")
    value = _read(archive / "artifacts/handoff.json")
    dispositions = {"candidate_complete", "research_ended", "needs_business_decision"}
    if (type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or value.get("task_id") != definition["task_id"]
            or value.get("disposition") not in dispositions
            or value.get("independent_quality_passed") is not False
            or "new_formal_score" not in value or value["new_formal_score"] is not None
            or type(value.get("needs_business_decision")) is not bool
            or not isinstance(value.get("reason"), str) or not value["reason"].strip()
            or not value.get("source_evidence")):
        raise TaskflowError("original author handoff does not satisfy its signed outcome contract")
    complete = value["disposition"] == "candidate_complete"
    if (value.get("candidate_subpath") != ("next-task/" + definition["task_id"] if complete else None)
            or value["needs_business_decision"] != (value["disposition"] == "needs_business_decision")):
        raise TaskflowError("author disposition/candidate path/business-decision flag conflict")
    return value


def _template(definition: dict) -> dict:
    reference = definition["role_template_ref"]
    raw = _regular(Path(reference["path"]))
    if sha256(raw) != reference["sha256"]:
        raise TaskflowError("registered author successor role template changed")
    value = parse_json(raw, "author successor template")
    items = value.get("workflows", []) if isinstance(value, dict) else value
    candidates = [item.get("definition", item) for item in items]
    chosen = [item for item in candidates if
              (item.get("flow_id") == reference["flow_id"] if reference.get("flow_id") else
               item.get("roles", {}).get("quality", {}).get("scope") == "whole_package")]
    if len(chosen) != 1 or not all(name in chosen[0]["roles"] for name in
                                 ("quality", "repair", "recheck", "public_rebuild", "recheck_public")):
        raise TaskflowError("one complete authorized whole-quality/public-rebuild role template required")
    return chosen[0]


def _roles(store: object, definition: dict) -> dict:
    """Reuse infrastructure only; old task counts, scope text and paths are discarded."""
    from taskflow.execution_batch import policy_setting, validate_batch_policy

    template = _template(definition)
    roles = {}
    task_id = definition["task_id"]
    policy = store.get_policy(definition["policy_id"])["config"]
    snapshot = validate_batch_policy(policy)
    for name in ("quality", "repair", "recheck", "public_rebuild", "recheck_public"):
        old = template["roles"][name]
        runner = copy.deepcopy(old["runner"])
        role_name = "repair" if name == "repair" else "quality"
        slot = "quality" if name == "recheck_public" else name
        setting = policy_setting(policy, role_name, slot if snapshot is not None else None, task_id)
        for key in ("model", "reasoning_effort", "timeout_seconds"):
            runner[key] = setting[key]
        if snapshot is not None:
            runner.update({key: setting[key] for key in ("cli", "reasoning_selection", "reasoning_context")})
            runner["model_slot"] = slot
        runner["inputs"] = []
        runner["request_sections"] = []
        role = {"runner": runner, "purpose": "Preauthorized current-candidate " + name + "; research-quality asset only, no target experiment"}
        if name == "public_rebuild":
            if runner.get("mode") != "existing_isolated":
                raise TaskflowError("fresh public reconstruction must use the existing isolated adapter")
            role["prepare"] = copy.deepcopy(old["prepare"])
            role["prepare"]["config"].update(model=setting["model"], reasoning_effort=setting["reasoning_effort"])
            if snapshot is not None:
                role["prepare"]["config"].update({key: setting[key] for key in ("cli", "reasoning_selection", "reasoning_context")})
            runner["argv"] = [role["prepare"]["launcher_python"], "{{runtime}}.public/runner.py", "execute", "{{runtime}}.public"]
            runner["request_sections"] = [{"kind": "operation", "text":
                "Carrier only. The existing isolated adapter sends its locked neutral current-public reconstruction request."}]
        else:
            scope = "whole_package" if name in ("quality", "recheck_public") else "affected"
            role["scope"] = scope
            if name == "repair":
                role["candidate_subpath"] = "revised-task/" + task_id
                operation = ("Repair only current independently located blocking defects and necessary associations. "
                             "Preserve complete original professional responsibility, legitimate alternatives, normal tools and fair criteria. "
                             "Deliver the complete revised package under revised-task/" + task_id + ". "
                             "A substantive unresolved business choice must be declared, not invented. No target-model call or regrade.")
            else:
                operation = ("Act as a fresh independent informed quality reviewer for the bound current package. "
                             "Read every public original and form the responsibility judgment before the teacher design. "
                             "Examine the full current public/private source, complete original public reconstruction and trace/code, "
                             "normal legitimate alternatives and reference/scoring fairness, including necessary related files. "
                             "Sign only " + scope + "; do not repair and self-sign, promise difficulty, regrade old work or call a target model.")
                role["predecessor_inputs"] = [
                    {"tree": "{{public_archive}}/supplemental/trees/inner/work", "kind": "submission", "target": "actual-public-work"},
                    {"tree": "{{public_archive}}/supplemental/trees/inner/scratch", "kind": "submission", "target": "actual-public-scratch"},
                    {"path": "{{public_archive}}/observed-trace.jsonl", "kind": "trace", "target": "actual-public-trace.jsonl"}]
            runner["request_sections"] = [{"kind": "operation", "text": operation + "\n\n" + INFORMED_PUBLIC_MATERIAL_GUIDANCE}]
        roles[name] = role
        if definition.get("kind") == "baseline_quality_v1":
            # These are host-side immutable dependencies, never model inputs.
            refs = definition["baseline_source"]["evidence"]
            dependencies = list(runner.get("dependencies", []))
            runner["dependencies"] = list(dict.fromkeys([*dependencies,
                definition["authorization"]["path"], *[ref["path"] for ref in refs]]))
    return roles


def _candidate_structure(root: Path, task_id: str) -> dict:
    actual = check_task(root, write_report=False)
    if actual["errors"] or actual["task_id"] != task_id:
        raise TaskflowError("author candidate is not a structurally complete same-task package")
    declaration = _read(root / "task.json")
    private_paths = {entry["path"] for entry in declaration["files"] if entry["role"] not in PUBLIC_ROLES}
    for entry in declaration["files"]:
        name = relative_path(entry["path"])
        if not (name == "record.md" or name.startswith(("draft/", "checks/"))):
            raise TaskflowError("new author candidate source must use draft/, checks/ or the root file record.md: " + name)
    reviews = [item for item in actual["reviews"] if item["required"]]
    if not {"business", "artifact", "calculation"}.issubset({item["kind"] for item in reviews}):
        raise TaskflowError("new candidate requires explicit business/artifact/calculation inconclusive reports")
    for item in reviews:
        report = _read(root / relative_path(item["path"]))
        if (type(report.get("schema_version")) is not int or report["schema_version"] != 1
                or report.get("review_id") != item["id"] or report.get("kind") != item["kind"]
                or report.get("status") != "inconclusive"
                or ("task_id" in report and report["task_id"] != task_id)
                or ("independent_quality_passed" in report and report["independent_quality_passed"] is not False)
                or any(key in report and report[key] is not None for key in
                       ("new_formal_score", "formal_score", "independent_signer", "signed_at_utc"))):
            raise TaskflowError("author candidate report must be explicitly unreviewed: " + item["id"])
        allowed = {"schema_version", "review_id", "kind", "status", "task_id", "scope",
                   "content_digest", "input_digest", "evaluation_digest", "limitations", "evidence",
                   "authority", "local_checks", "local_evidence", "independent_quality_passed", "new_formal_score",
                   "independent_signer", "signed_at_utc", "formal_score"}
        if "local_evidence" in report:
            local = report["local_evidence"]
            if (not isinstance(local, list) or not all(isinstance(name, str) for name in local)
                    or len(local) != len(set(local)) or not set(report).issubset(allowed)
                    or report.get("independent_quality_passed") is not False
                    or "new_formal_score" not in report or report["new_formal_score"] is not None
                    or any(report.get(key) is not None for key in
                           ("content_digest", "input_digest", "evaluation_digest"))):
                raise TaskflowError("author local_evidence requires an explicitly unsigned placeholder: " + item["id"])
            for name in local:
                if relative_path(name) not in private_paths:
                    raise TaskflowError("author placeholder local_evidence must name current private declared files: " + item["id"])
        if item["state"] == "inconclusive":
            continue
        explicit_flags = (report.get("independent_quality_passed") is False
                          and "new_formal_score" in report and report["new_formal_score"] is None)
        unsigned_fields = ("content_digest", "input_digest", "evaluation_digest", "independent_signer",
                           "signed_at_utc", "formal_score")
        unsigned_nulls = all(key in report and report[key] is None for key in unsigned_fields)
        if (not set(report).issubset(allowed)
                or any(report.get(key) is not None for key in
                       ("content_digest", "input_digest", "evaluation_digest"))
                or not (explicit_flags or unsigned_nulls)):
            raise TaskflowError("author candidate report must be current inconclusive or a typed unsigned placeholder: " + item["id"])
    return actual


def _public_root(record: dict) -> Path:
    """Use one stable external runtime without changing prior repository evidence."""
    identity = {"flow_id": record["flow_id"], "definition_digest": record["definition_digest"]}
    if record.get("state", {}).get("public_capacity_continuation"):
        proof = record["state"]["public_capacity_continuation"]
        if (proof.get("kind") != "explained_public_capacity_continuation_v1" or type(proof.get("ordinal")) is not int or proof.get("ordinal") != 1
                or proof.get("flow_id") != record["flow_id"]
                or proof.get("definition_digest") != record["definition_digest"]):
            raise TaskflowError("public continuation identity differs from its original source")
        identity["continuation"] = 1
    return Path("/tmp").resolve() / ("tf-author-public-" + sha256(canonical(identity))[:24])


def _active_attempt(store: object, work_id: str) -> dict | None:
    attempts = store.list_attempts(work_id)
    return attempts[-1] if attempts else None


def advance_author(store: object, record: dict) -> dict:
    """Advance one registered author handoff; return a single exact work cursor."""
    definition, state = record["definition"], dict(record["state"])
    if definition.get("kind") != "author_quality_v1":
        raise TaskflowError("author_quality_v1 lifecycle required")
    validate_evidence(record["evidence"])
    if state["status"] != "active":
        return record
    from taskflow.execution_completion import reconcile_ended_attempt

    author, manifest = _author(store, definition)
    author = reconcile_ended_attempt(store, author)
    if author["state"] in ("claimed", "starting", "running", "collecting"):
        return _wait(store, record, "source_author", author["state"])
    if author["state"] != "completed":
        raise TaskflowError("original author failed/uncertain/cancelled; never redraw: " + author["state"])
    if not state.get("author_archive"):
        root = Path(author["runtime_path"])
        if not (root / "collection.json").exists() or not (root / "collection-lock.json").exists():
            return _wait(store, record, "source_author", "collection_not_sealed")
        archived = archive_full_attempt(root, Path(definition["directory"]) / "archives" / author["attempt_id"])
        state.update(author_archive=archived["path"], stage="author_handoff", waiting=None)
        state["archives"] = [*state["archives"], archived["path"]]
        state["history"] = [*state["history"], {"kind": "source_author_original_received", "attempt_id": author["attempt_id"],
                                               "cli_sid": author["cli_sid"], "archive": archived["path"]}]
        return _save(store, record, state, "author_original_archived")
    archive = Path(state["author_archive"])
    handoff = _handoff(archive, definition, author)
    if handoff["disposition"] != "candidate_complete":
        result = {"outcome": handoff["disposition"], "reason": handoff["reason"], "source_evidence": handoff["source_evidence"],
                  "author_archive": str(archive), "author_sid": author["cli_sid"], "candidate_created": False,
                  "new_model_dispatched": False, "formal_experiment": "not_dispatched"}
        state.update(status="exception" if handoff["needs_business_decision"] else "accepted", waiting=None,
                     current_work_id=None, result=result)
        if handoff["needs_business_decision"]:
            state["exception"] = {"stage": "author_handoff", "reason": handoff["reason"], "source_evidence": handoff["source_evidence"]}
        return _save(store, record, state, "author_actual_outcome_received")
    if not state.get("candidate_id"):
        delivery_options = {}
        if state.get("optional_candidate_declaration_transport") is not None:
            delivery_options["declaration_transport"] = state["optional_candidate_declaration_transport"]
        updated = store.record_optional_candidate_delivery(author["attempt_id"], archive=archive,
                    candidate_subpath=handoff["candidate_subpath"], evidence=[evidence_ref(archive / "artifacts/handoff.json", "actual optional candidate disposition")],
                    **delivery_options)
        root = Path(state.get("technical_candidate_root", updated["supplemental_delivery"]["candidate_root"]))
        _candidate_structure(root, definition["task_id"])
        candidate_id = _identity(record["flow_id"], "author-candidate")
        candidate = _get_or_none(store.get_candidate, candidate_id)
        if candidate is None:
            candidate = capture_candidate(store, root, candidate_id, parent_id=author["candidate_id"], author_attempt=author["attempt_id"])
        if _binding(candidate) != {"candidate_id": candidate_id, **{key: updated["supplemental_delivery"]["candidate_binding"][key]
                                                                  for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")}}:
            raise TaskflowError("registered author candidate differs from actual optional delivery")
        state.update(candidate_id=candidate_id, candidate_root=str(root), stage="public_prepare", waiting=None)
        return _save(store, record, state, "author_actual_candidate_bound")
    return advance_candidate_quality(store, record, author)


def advance_candidate_quality(store: object, record: dict, author: dict) -> dict:
    """Follow an already bound candidate through the existing public/quality chain."""
    from taskflow.execution_completion import reconcile_ended_attempt

    definition, state = record["definition"], dict(record["state"])

    candidate = store.get_candidate(state["candidate_id"])
    if state["stage"] == "public_prepare":
        role = _roles(store, definition)["public_rebuild"]
        continuation = state.get("public_capacity_continuation")
        if continuation is not None and canonical(role) != canonical(continuation["public_role"]):
            raise TaskflowError("public continuation configuration differs from its original approved role")
        root = _public_root(record)
        root.mkdir(parents=True, exist_ok=True)
        runtime = root / "runtime"
        node_id, work_id = _identity(record["flow_id"], "public"), _identity(record["flow_id"], "public-work")
        if continuation is not None:
            node_id = _identity(record["flow_id"], "public-capacity-once")
            work_id = _identity(record["flow_id"], "public-capacity-work-once")
        context = {**_binding(candidate), "flow_id": record["flow_id"], "node_id": node_id, "intent_id": node_id,
                   "runtime": str(runtime), "work": str(root / "work"), "scratch": str(root / "scratch"),
                   "candidate_root": state["candidate_root"], "inputs_root": str(runtime / "inputs"),
                   "private_paths": [item["path"] for item in candidate["files"] if item["role"] not in PUBLIC_ROLES]}
        existing = _get_or_none(store.get_work, work_id)
        if existing is None:
            config = _render(role["runner"], context)
            config.update(intent_id=node_id, policy_version=definition["policy_id"])
            config["isolated"] = _public_prepare(role, context)
            config["cwd"] = str(Path(config["isolated"]["runtime"]) / "work")
            spec = {"work": {"work_id": work_id, "task_id": definition["task_id"], "candidate_id": candidate["candidate_id"],
                              "role": "quality", "policy_id": definition["policy_id"], "intent_id": node_id,
                              "purpose": role["purpose"], "dispatch_owner": definition["owner"], "evidence": record["evidence"]},
                    "runner": config}
            if "model_slot" in config:
                spec["work"]["model_slot"] = config["model_slot"]
            _publish_once(root / "spec.json", spec)
            enqueue_spec(store, spec, runtime, resume_prepared=True)
        elif (existing["candidate_id"] != candidate["candidate_id"] or existing["intent_id"] != node_id
              or existing["runtime_path"] != str(runtime)):
            raise TaskflowError("fresh public work identity belongs to another launch")
        state.update(stage="public_result", public_work_id=work_id, current_work_id=work_id, waiting=None)
        return _save(store, record, state, "author_public_reconstruction_enqueued")
    if state["stage"] == "public_result":
        attempt = reconcile_ended_attempt(store, _active_attempt(store, state["public_work_id"]))
        if attempt is None or attempt["state"] in ACTIVE:
            if attempt is not None and attempt["state"] == "uncertain":
                raise TaskflowError("public reconstruction launch uncertain; original retained")
            return _wait(store, record, "public_reconstruction", attempt["state"] if attempt else "ready")
        if attempt["state"] != "completed" or not attempt.get("cli_sid") or attempt["cli_sid"] == author["cli_sid"]:
            raise TaskflowError("fresh public reconstruction failed or reused the author context")
        if (state.get("public_capacity_continuation") is not None
                and attempt["cli_sid"] == state["public_capacity_continuation"]["failed_sid"]):
            raise TaskflowError("public continuation reused the original failed CLI context")
        archived = archive_full_attempt(Path(attempt["runtime_path"]), Path(definition["directory"]) / "archives" / attempt["attempt_id"])
        state.update(public_attempt_id=attempt["attempt_id"], public_archive=archived["path"],
                     stage="child_quality", current_work_id=None, waiting=None)
        state["archives"] = [*state["archives"], archived["path"]]
        state["history"] = [*state["history"], {"kind": "public_reconstruction", "attempt_id": attempt["attempt_id"],
                                               "cli_sid": attempt["cli_sid"], "archive": archived["path"]}]
        return _save(store, record, state, "author_public_original_archived")
    child_id = state.get("child_flow_id") or _identity(record["flow_id"], "quality-flow")
    child = _get_or_none(store.get_flow, child_id)
    if child is None:
        public = store.get_attempt(state["public_attempt_id"])
        definition_child = {"schema_version": 1, "flow_id": child_id, "task_id": definition["task_id"],
                            "owner": definition["owner"], "policy_id": definition["policy_id"],
                            "directory": str(Path(definition["directory"]) / "quality"),
                            "accepted_use": definition["next"]["candidate_complete"],
                            "business_item": definition["business_item"], "normal_branches": definition["next"],
                            "initial": {"candidate_id": candidate["candidate_id"], "candidate_root": state["candidate_root"],
                                        "expected_digests": {key: candidate[key] for key in ("content_digest", "input_digest", "evaluation_digest")},
                                        "predecessor_attempt": public["attempt_id"],
                                        "predecessor_binding": {key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}},
                            "roles": _roles(store, definition)}
        if definition.get("kind") == "baseline_quality_v1":
            definition_child["initial"]["excluded_review_sids"] = [author["cli_sid"]]
        refs = [*record["evidence"], evidence_ref(Path(state["public_archive"]) / "archive-binding.json", "actual fresh public original")]
        _publish_once(Path(definition["directory"]) / "quality-definition.json", {"definition": definition_child, "evidence": refs})
        child = register_workflow(store, definition_child, refs)
    current = child["state"].get("current_work_id")
    active = reconcile_ended_attempt(store, _active_attempt(store, current)) if current else None
    if active is None or active["state"] not in ACTIVE:
        child = advance_workflow(store, child_id)
        if child["state"]["status"] in ("accepted", "exception") and not child["state"].get("receipt"):
            child = advance_workflow(store, child_id)
    elif active["state"] == "uncertain":
        raise TaskflowError("child quality launch uncertain; no replacement attempt")
    state.update(child_flow_id=child_id, current_work_id=child["state"].get("current_work_id"),
                 child_revision=child["revision"], waiting={"stage": "child_quality", "state": child["state"]["status"]})
    if child["state"]["status"] == "exception":
        state.update(status="exception", current_work_id=None, exception={"stage": "child_quality", "reason": child["state"].get("exception"),
                                                                         "child_receipt": child["state"].get("receipt")})
    elif child["state"]["status"] == "accepted":
        state.update(status="accepted", current_work_id=None, waiting=None,
                     result={"outcome": "candidate_quality_accepted", "scope": "research-quality asset only", "formal_experiment": "not_dispatched",
                             "candidate_id": child["state"]["candidate_id"], "candidate_root": child["state"]["candidate_root"],
                             "child_flow_id": child_id, "child_receipt": child["state"]["receipt"],
                             "accepted_review_id": child["state"]["accepted_review_id"]})
        if definition.get("kind") == "baseline_quality_v1":
            state["result"].update(basis_kind="existing_captured_baseline",
                candidate_created_by_source_author=False,
                research_ended_source_flow_id=definition["source_research_flow_id"])
    return _save(store, record, state, "author_child_quality_progress")
