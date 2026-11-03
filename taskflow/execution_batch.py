"""Explicit future-batch choices and immutable model-policy bindings.

This module verifies structured attribution and evidence bytes, never user prose.
It does not start a model, switch a chat or write an execution database.
"""

import copy
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

from taskflow.contracts import (
    TaskflowError, _publish_no_replace, canonical, parse_json, safe_read, sha256,
    validate_id,
)

ROLE_KEYS = (
    "coordinator", "author", "public_rebuild", "quality", "repair", "recheck",
    "solver", "reviewer", "content_inspector", "engineer", "health_auditor",
)
CHAT_ROLES = frozenset({"coordinator", "content_inspector", "engineer", "health_auditor"})
CLI_ROLES = frozenset(set(ROLE_KEYS) - CHAT_ROLES)
NATURAL_SLOTS = {"author": "author", "repair": "repair", "solver": "solver",
                 "reviewer": "reviewer", "review-facts": "reviewer", "review-score": "reviewer"}
LOW_ROLE_SLOTS = {**{key: {value} for key, value in NATURAL_SLOTS.items()},
                  "quality": {"quality", "public_rebuild", "recheck"}}


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise TaskflowError(label + ": explicit nonblank text required")
    return value


def _read(path: Path) -> bytes:
    path = Path(path).absolute()
    return safe_read(Path(path.anchor), str(path.relative_to(path.anchor)))


def _reference(reference: Any, captured: dict | None = None) -> tuple[dict, bytes]:
    if not isinstance(reference, dict):
        raise TaskflowError("evidence: path/SHA/size/location reference required")
    path = Path(_text(reference.get("path"), "evidence.path"))
    if not path.is_absolute():
        raise TaskflowError("evidence.path must be absolute")
    _text(reference.get("location"), "evidence.location")
    try:
        raw = _read(path) if captured is None else captured[(str(path), reference.get("sha256"))]
    except KeyError as error:
        raise TaskflowError("snapshot has no captured evidence: " + str(path)) from error
    if (reference.get("sha256") != sha256(raw) or type(reference.get("size")) is not int
            or reference["size"] != len(raw)):
        raise TaskflowError("evidence bytes differ: " + str(path))
    return copy.deepcopy(reference), raw


def _pointer(value: Any, pointer: Any) -> Any:
    if not isinstance(pointer, str) or (pointer and not pointer.startswith("/")):
        raise TaskflowError("evidence JSON pointer required")
    try:
        for raw in pointer.split("/")[1:] if pointer else []:
            if "~" in raw.replace("~1", "").replace("~0", ""):
                raise ValueError("bad escape")
            token = raw.replace("~1", "/").replace("~0", "~")
            if isinstance(value, list):
                if not token.isdigit() or (token != "0" and token.startswith("0")):
                    raise ValueError("bad array index")
                value = value[int(token)]
            else:
                value = value[token]
        return value
    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise TaskflowError("evidence JSON pointer does not locate an actual value") from error


def _scope(plan: dict) -> dict:
    batch = validate_id(plan.get("batch_id"), "batch_id")
    tasks = plan.get("task_ids")
    if not isinstance(tasks, list) or not tasks:
        raise TaskflowError("task_ids: nonempty unique task IDs required")
    for task in tasks:
        validate_id(task, "task_id")
    if len(tasks) != len(set(tasks)):
        raise TaskflowError("task_ids: nonempty unique task IDs required")
    return {"batch_id": batch, "task_ids": sorted(tasks),
            "role_keys": [key for key in ROLE_KEYS if plan["roles"][key]["enabled"]]}


def _runtime(role: str, runtime: Any) -> dict:
    if not isinstance(runtime, dict):
        raise TaskflowError(role + ".runtime: explicit runtime required")
    carrier = "chat" if role in CHAT_ROLES else "cli"
    allowed = {"carrier", "provider"} if carrier == "chat" else {
        "carrier", "provider", "cli", "reasoning_selection", "reasoning_effort",
        "reasoning_context", "timeout_seconds", "support_evidence"}
    if set(runtime) - allowed:
        raise TaskflowError(role + ".runtime has unknown configuration fields")
    if runtime.get("carrier") != carrier:
        raise TaskflowError(role + ".runtime.carrier must be " + carrier)
    result = {"carrier": carrier, "provider": _text(runtime.get("provider"), role + ".provider")}
    if carrier == "chat":
        return result
    cli = Path(_text(runtime.get("cli"), role + ".cli"))
    if not cli.is_absolute():
        raise TaskflowError(role + ".cli must be absolute")
    result["cli"] = str(cli)
    selection = runtime.get("reasoning_selection")
    if selection not in {"explicit", "verified_client_default"}:
        raise TaskflowError(role + ".reasoning_selection must be explicit or verified_client_default")
    result["reasoning_selection"] = selection
    result["reasoning_effort"] = _text(runtime.get("reasoning_effort"), role + ".reasoning_effort")
    context = runtime.get("reasoning_context")
    if context is not None:
        _text(context, role + ".reasoning_context")
    result["reasoning_context"] = context
    timeout = runtime.get("timeout_seconds")
    if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout) or timeout <= 0):
        raise TaskflowError(role + ".timeout_seconds must be positive and finite")
    result["timeout_seconds"] = timeout
    result["support_evidence"] = copy.deepcopy(runtime.get("support_evidence"))
    return result


def _roles(plan: dict) -> dict:
    roles = plan.get("roles")
    if not isinstance(roles, dict) or set(roles) != set(ROLE_KEYS):
        raise TaskflowError("roles must contain the eleven stable role keys exactly")
    result = {}
    for key in ROLE_KEYS:
        row = roles[key]
        if not isinstance(row, dict) or type(row.get("enabled")) is not bool:
            raise TaskflowError(key + ".enabled must be an explicit boolean")
        if not row["enabled"]:
            if row.get("model") is not None or row.get("runtime") is not None:
                raise TaskflowError(key + ": disabled role must keep model/runtime null")
            result[key] = {"enabled": False, "model": None, "runtime": None}
            continue
        model = _text(row.get("model"), key + ".model")
        if model.casefold() in {"auto", "default"}:
            raise TaskflowError(key + ".model: auto/default is not a user choice")
        result[key] = {"enabled": True, "model": model, "runtime": _runtime(key, row.get("runtime"))}
    return result


def confirmation_digests(plan: dict) -> dict:
    """Return the machine scope/map a controller must attribute to the user."""
    roles = _roles(plan)
    scope = _scope({**plan, "roles": roles})
    return {"scope": scope, "expanded_model_map": {k: v["model"] for k, v in roles.items() if v["enabled"]},
            "model_mapping_digest": sha256(canonical(roles)), "scope_digest": sha256(canonical(scope))}


def _catalog_effort(item: Any) -> str | None:
    """Read supported CLI catalog spellings without accepting conflicting aliases."""
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return None
    values = [item[key] for key in ("effort", "reasoningEffort") if key in item]
    if not values or any(not isinstance(value, str) or value != values[0] for value in values):
        return None
    return values[0]


def _support(role: str, row: dict, mode: str, refs: list[dict], captured: dict | None) -> None:
    runtime = row["runtime"]
    reference, raw = _reference(runtime["support_evidence"], captured)
    proof = parse_json(raw, role + " support evidence")
    origin = "test_fixture" if mode == "test_fixture" else "original_cli_observation"
    if (type(proof.get("schema_version")) is not int or proof["schema_version"] != 1
            or proof.get("origin") != origin or proof.get("kind") != "cli_model_support"
            or proof.get("provider") != runtime["provider"] or proof.get("model") != row["model"]
            or proof.get("supported") is not True or type(proof.get("returncode")) is not int
            or proof["returncode"] != 0):
        raise TaskflowError(role + ": original same-provider CLI support observation required")
    cli_ref, _ = _reference(proof.get("cli"), captured)
    if cli_ref["path"] != runtime["cli"]:
        raise TaskflowError(role + ": support observation belongs to another CLI")
    argv = proof.get("argv")
    if not isinstance(argv, list) or not argv or argv[0] != runtime["cli"] or not all(isinstance(x, str) for x in argv):
        raise TaskflowError(role + ": original CLI observation argv is missing")
    output_ref, output_raw = _reference(proof.get("output"), captured)
    output = parse_json(output_raw, role + " original CLI output")
    parent_pointer = _text(proof.get("model_pointer"), role + ".model_pointer").rsplit("/", 1)[0]
    explicit_context = runtime["reasoning_selection"] == "explicit" and runtime["reasoning_context"] is not None
    fields = ["efforts_pointer"] + (["contexts_pointer"] if explicit_context else [])
    if runtime["reasoning_selection"] == "verified_client_default":
        fields.append("default_effort_pointer")
    if any(not isinstance(proof.get(key), str) or proof[key].rsplit("/", 1)[0] != parent_pointer
           for key in fields):
        raise TaskflowError(role + ": capabilities must belong to the same selected model entry")
    if _pointer(output, proof.get("model_pointer")) != row["model"]:
        raise TaskflowError(role + ": model absent from original CLI output locator")
    efforts = _pointer(output, proof.get("efforts_pointer"))
    if not isinstance(efforts, list) or not any(
        _catalog_effort(item) == runtime["reasoning_effort"]
        for item in efforts
    ):
        raise TaskflowError(role + ": reasoning effort unsupported by original CLI output")
    if explicit_context:
        contexts = _pointer(output, proof.get("contexts_pointer"))
        if not isinstance(contexts, list) or runtime["reasoning_context"] not in contexts:
            raise TaskflowError(role + ": reasoning context unsupported by original CLI output")
    if runtime["reasoning_selection"] == "verified_client_default":
        default_pointer = proof.get("default_effort_pointer")
        legacy = default_pointer.rsplit("/", 1)[-1] not in {
            "default_reasoning_level", "default_effort", "defaultReasoningEffort"
        }
        if legacy:
            # Only captured snapshots can use the previous v1 support contract.
            # A marker in a live plan must never select historical validation.
            context_pointer = proof.get("contexts_pointer")
            if (captured is None or proof.get("observation_kind") != "same_argv_no_inference"
                    or runtime["reasoning_effort"] not in efforts
                    or (runtime["reasoning_context"] is not None and (
                        not isinstance(context_pointer, str)
                        or context_pointer.rsplit("/", 1)[0] != parent_pointer
                        or not isinstance(_pointer(output, context_pointer), list)
                        or runtime["reasoning_context"] not in _pointer(output, context_pointer)))):
                raise TaskflowError(role + ": frozen legacy client default evidence differs")
        if _pointer(output, default_pointer) != runtime["reasoning_effort"]:
            raise TaskflowError(role + ": catalog default effort differs from the expected client default")
    refs.extend([reference, cli_ref, output_ref])


def _chat(role: str, row: dict, binding: Any, scope: dict, refs: list[dict], captured: dict | None) -> dict:
    if not isinstance(binding, dict) or binding.get("carrier") != "chat":
        raise TaskflowError(role + ": actual chat binding remains unknown")
    identity = {"thread_id": validate_id(binding.get("thread_id"), role + ".thread_id"),
                "host": validate_id(binding.get("host"), role + ".host"), "model": row["model"]}
    if binding.get("actual_model") != row["model"]:
        raise TaskflowError(role + ": actual chat model differs or remains unknown")
    reference, raw = _reference(binding.get("configuration_evidence"), captured)
    observed = _pointer(parse_json(raw, role + " actual chat configuration"), binding.get("configuration_pointer"))
    if (not isinstance(observed, dict) or any(observed.get(k) != v for k, v in identity.items())
            or observed.get("role_key") != role or observed.get("scope") != scope
            or observed.get("origin") != "actual_chat_configuration"
            or observed.get("provider") != row["runtime"]["provider"]):
        raise TaskflowError(role + ": actual ID/model/provider/scope configuration evidence differs")
    refs.append(reference)
    return {"carrier": "chat", "thread_id": identity["thread_id"], "host": identity["host"],
            "actual_model": identity["model"], "configuration_evidence": reference,
            "configuration_pointer": binding["configuration_pointer"]}


def _check(plan: dict, captured: dict | None = None) -> dict:
    if type(plan.get("schema_version")) is not int or plan["schema_version"] != 1 or plan.get("architecture_version") != "high-value-task-v1":
        raise TaskflowError("schema_version=1/high-value-task-v1 required")
    roles = _roles(plan)
    digests = confirmation_digests(plan)
    mode = plan.get("execution_mode", "real_cli")
    if mode not in {"real_cli", "test_fixture"}:
        raise TaskflowError("execution_mode must be real_cli or test_fixture")
    confirm = plan.get("user_model_confirmation")
    if not isinstance(confirm, dict) or confirm.get("origin") != "direct_user":
        raise TaskflowError("user_model_confirmation: direct-user choice source missing")
    reference, _ = _reference(confirm.get("source"), captured)
    if any(confirm.get(key) != value for key, value in digests.items()):
        raise TaskflowError("user confirmation scope/model/runtime/enabled mapping differs")
    refs = [reference]
    bindings = plan.get("role_bindings", {})
    if not isinstance(bindings, dict) or any(key not in ROLE_KEYS for key in bindings):
        raise TaskflowError("role_bindings: stable role keys required; titles cannot route")
    chats = {}
    for key, row in roles.items():
        if not row["enabled"]:
            continue
        if key in CHAT_ROLES:
            chats[key] = _chat(key, row, bindings.get(key), digests["scope"], refs, captured)
        else:
            _support(key, row, mode, refs, captured)
            binding = bindings.get(key)
            if binding is not None and (not isinstance(binding, dict) or binding.get("carrier") != "cli"
                    or any(binding.get(k) is not None for k in ("work_id", "attempt_id", "sid"))):
                raise TaskflowError(key + ": batch preparation cannot predeclare CLI work/attempt/SID")
    return {"schema_version": 1, "architecture_version": plan["architecture_version"],
            "batch_id": digests["scope"]["batch_id"], "task_ids": digests["scope"]["task_ids"],
            "execution_mode": mode, "scope": digests["scope"], "roles": roles,
            "user_model_confirmation": copy.deepcopy(confirm), "chat_bindings": chats,
            **digests, "evidence": list({(r["path"], r["sha256"]): r for r in refs}.values())}


def check_batch_plan(plan_path: Path) -> dict:
    """Read only; report all missing model choices without preparing any work."""
    plan = parse_json(_read(plan_path), "batch plan")
    missing, unknown_chats = [], []
    roles = plan.get("roles", {})
    if isinstance(roles, dict):
        for key in ROLE_KEYS:
            row = roles.get(key)
            if isinstance(row, dict) and row.get("enabled") is True:
                model = row.get("model")
                if not isinstance(model, str) or not model.strip() or model.strip().casefold() in {"auto", "default"}:
                    missing.append(key)
                bindings = plan.get("role_bindings", {})
                if key in CHAT_ROLES and (not isinstance(bindings, dict) or not bindings.get(key)):
                    unknown_chats.append(key)
    try:
        normalized = _check(plan)
    except (TaskflowError, OSError, TypeError, KeyError, ValueError) as error:
        status = "awaiting_user_models" if missing or plan.get("user_model_confirmation") is None else (
            "awaiting_role_bindings" if unknown_chats else "invalid")
        return {"ready": False, "status": status, "missing_models": missing,
                "unknown_chat_roles": unknown_chats, "errors": [str(error)], "model_calls": 0, "database_writes": 0}
    return {"ready": True, "status": "ready", "missing_models": [], "unknown_chat_roles": [], "errors": [],
            "plan_digest": sha256(canonical(plan)), **normalized, "model_calls": 0, "database_writes": 0}


def _write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _snapshot_reference(path: Path) -> dict:
    raw = _read(path)
    return {"path": str(path.absolute()), "sha256": sha256(raw), "size": len(raw),
            "location": "immutable confirmed batch snapshot"}


def freeze_batch_plan(plan_path: Path, destination: Path) -> dict:
    """Publish a confirmed routing/evidence snapshot once, without execution."""
    checked = check_batch_plan(plan_path)
    if not checked["ready"]:
        raise TaskflowError("batch blocked: " + "; ".join(checked["errors"]) + "; missing models=" + ",".join(checked["missing_models"]))
    keys = ("schema_version", "architecture_version", "batch_id", "task_ids", "execution_mode", "scope", "roles",
            "user_model_confirmation", "chat_bindings", "expanded_model_map", "model_mapping_digest", "scope_digest", "evidence")
    snapshot = {key: checked[key] for key in keys}
    destination = Path(destination).absolute()
    if destination.is_symlink() or any(p.is_symlink() for p in destination.parents):
        raise TaskflowError("snapshot destination cannot use symlinks")
    manifest = destination / "batch-snapshot.json"
    if destination.exists():
        existing = load_batch_snapshot(_snapshot_reference(manifest))
        if any(existing.get(k) != v for k, v in snapshot.items()):
            raise TaskflowError("existing frozen batch differs; explicitly confirm a new version")
        return {"snapshot": _snapshot_reference(manifest), **existing}
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="." + destination.name + "-", dir=destination.parent))
    try:
        copies = []
        for index, source in enumerate(checked["evidence"]):
            _, raw = _reference(source)
            name = "evidence/" + str(index).zfill(4) + "-" + source["sha256"]
            (stage / "evidence").mkdir(exist_ok=True)
            _write_new(stage / name, raw)
            copies.append({"original": source, "relative_path": name})
        snapshot["evidence_copies"] = copies
        _write_new(stage / "batch-snapshot.json", canonical(snapshot) + b"\n")
        if _check(parse_json(_read(plan_path))) != {key: checked[key] for key in snapshot if key != "evidence_copies"}:
            raise TaskflowError("batch inputs changed while freezing")
        _publish_no_replace(stage, destination)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {"snapshot": _snapshot_reference(manifest), **load_batch_snapshot(_snapshot_reference(manifest))}


def _load_batch_snapshot(reference: dict) -> dict:
    ref, raw = _reference(reference)
    value = parse_json(raw, "batch snapshot")
    if (type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or value.get("architecture_version") != "high-value-task-v1"
            or not isinstance(value.get("evidence_copies"), list)):
        raise TaskflowError("invalid frozen batch snapshot")
    root, captured = Path(ref["path"]).parent, {}
    for item in value["evidence_copies"]:
        relative = item["relative_path"]
        if not isinstance(relative, str) or not relative.startswith("evidence/") or ".." in Path(relative).parts:
            raise TaskflowError("unsafe snapshot evidence path")
        original = item["original"]
        raw = safe_read(root, relative)
        if sha256(raw) != original["sha256"] or len(raw) != original["size"]:
            raise TaskflowError("frozen evidence copy changed")
        key = (original["path"], original["sha256"])
        if key in captured:
            raise TaskflowError("duplicate frozen evidence identity")
        captured[key] = raw
    plan = {"schema_version": value["schema_version"], "architecture_version": value["architecture_version"],
            "execution_mode": value.get("execution_mode"), "batch_id": value["scope"]["batch_id"],
            "task_ids": value["scope"]["task_ids"], "roles": value["roles"],
            "user_model_confirmation": value["user_model_confirmation"], "role_bindings": value["chat_bindings"]}
    checked = _check(plan, captured)
    if any(value.get(k) != v for k, v in checked.items()) or len(captured) != len(checked["evidence"]):
        raise TaskflowError("frozen confirmation/runtime/binding evidence differs")
    return value


def load_batch_snapshot(reference: dict) -> dict:
    """Revalidate frozen bytes and captured evidence, not current mutable sources."""
    try:
        return _load_batch_snapshot(reference)
    except TaskflowError:
        raise
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise TaskflowError("invalid frozen batch snapshot: " + str(error)) from error


def role_config(snapshot: dict, role_key: str) -> dict:
    """Resolve a stable logical slot; presentation names never route work."""
    if role_key not in ROLE_KEYS:
        raise TaskflowError("unknown batch role key")
    row = snapshot["roles"][role_key]
    if row["enabled"] is not True:
        raise TaskflowError("batch role is disabled: " + role_key)
    return {"model": row["model"], **copy.deepcopy(row["runtime"])}


def validate_batch_policy(config: dict) -> dict | None:
    """Legacy configs return None; any batch marker requires the whole binding."""
    if not any(k in config for k in ("model_confirmation_required", "batch_snapshot", "model_slots")):
        return None
    if config.get("model_confirmation_required") is not True or not isinstance(config.get("model_slots"), dict):
        raise TaskflowError("batch policy requires confirmation flag, snapshot and complete model_slots")
    snapshot = load_batch_snapshot(config.get("batch_snapshot"))
    expected = {k: role_config(snapshot, k) for k in ROLE_KEYS if k in CLI_ROLES and snapshot["roles"][k]["enabled"]}
    if config["model_slots"] != expected:
        raise TaskflowError("batch policy model_slots differ from the complete confirmed CLI map")
    return snapshot


def policy_setting(config: dict, low_role: str, model_slot: str | None = None,
                   task_id: str | None = None) -> dict:
    """Choose the unchanged legacy setting or an exact confirmed, scoped slot."""
    snapshot = validate_batch_policy(config)
    if snapshot is None:
        if model_slot is not None:
            raise TaskflowError("legacy policy cannot accept an unbound batch model_slot")
        return copy.deepcopy(config["roles"][low_role])
    if task_id is not None and task_id not in snapshot["scope"]["task_ids"]:
        raise TaskflowError("work task_id is outside confirmed batch scope")
    slot = model_slot or NATURAL_SLOTS.get(low_role)
    if slot not in LOW_ROLE_SLOTS.get(low_role, set()):
        raise TaskflowError("explicit compatible logical model_slot required for " + low_role)
    return role_config(snapshot, slot)

