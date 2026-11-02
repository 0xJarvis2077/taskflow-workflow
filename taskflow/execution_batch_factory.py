"""Compile confirmed models with explicitly supplied runtime infrastructure.

This is preparation on the existing execution layer, never a scheduler or a
model call. Infrastructure templates supply permissions and tools, not choices.
"""

import copy
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, read_json, sha256, validate_id
from taskflow.execution_batch import (
    check_batch_plan, freeze_batch_plan, load_batch_snapshot, role_config,
    validate_batch_policy,
)
from taskflow.execution_flow import _publish_once
from taskflow.execution_store import evidence_ref, validate_evidence


ROLE_SLOTS = {"author": "author", "repair": "repair", "quality": "quality", "solver": "solver",
              "review-facts": "reviewer", "review-score": "reviewer"}
RUNNER_FIELDS = ("model", "cli", "reasoning_effort", "reasoning_selection", "reasoning_context", "timeout_seconds")


def _selected(target: dict, setting: dict) -> None:
    for key in RUNNER_FIELDS:
        if key in setting:
            target[key] = copy.deepcopy(setting[key])
        elif key in {"reasoning_context", "cli"}:
            target.pop(key, None)


def compile_batch_configuration(plan: Path, infrastructure: Path, destination: Path) -> dict:
    """Freeze a new version without touching a database or historical factory."""
    checked = check_batch_plan(plan)
    if not checked["ready"]:
        raise TaskflowError("awaiting_user_models: " + "; ".join(checked["errors"]) + "; missing=" + ",".join(checked["missing_models"]))
    base = read_json(infrastructure)
    if not isinstance(base, dict) or base.get("schema_version") != 1:
        raise TaskflowError("batch infrastructure requires schema_version=1")
    identity = validate_id(base.get("policy_id"), "new batch policy_id")
    if not isinstance(base.get("policy"), dict):
        raise TaskflowError("batch infrastructure needs an explicit policy infrastructure object")
    policy = copy.deepcopy(base["policy"])
    if type(policy.get("concurrency")) is not int or policy["concurrency"] < 1:
        raise TaskflowError("batch infrastructure policy needs positive concurrency")
    for name in ("author_factory", "experiment_factory"):
        if name in base and not isinstance(base[name], dict):
            raise TaskflowError(name + " must be an explicit infrastructure object")
    slots = {key: role_config(checked, key) for key, row in checked["roles"].items()
             if row["enabled"] and row["runtime"]["carrier"] == "cli"}
    if "author_factory" in base:
        factory = base["author_factory"]
        if "author" not in slots or factory.get("schema_version") != 1:
            raise TaskflowError("author factory requires an explicitly enabled author and schema_version=1")
        validate_evidence([factory["template_spec_ref"], factory["role_template_ref"]])
        for key in ("research_root", "lifecycle_root"):
            if not isinstance(factory.get(key), str) or not Path(factory[key]).is_absolute():
                raise TaskflowError("author factory requires an explicit absolute " + key)
    if "experiment_factory" in base:
        factory = base["experiment_factory"]
        technical = factory.get("technical")
        if (not {"solver", "reviewer"} <= set(slots) or factory.get("schema_version") != 1
                or not isinstance(technical, dict)
                or not all(isinstance(technical.get(key), dict) for key in ("formal_config_base", "review_runner"))
                or (technical.get("local_profile_policy") is not None and not isinstance(technical["local_profile_policy"], dict))):
            raise TaskflowError("experiment factory requires enabled solver/reviewer and complete explicit infrastructure")
        for key in ("repository", "formal_runtime_root", "lifecycle_root", "publication_root"):
            if not isinstance(factory.get(key), str) or not Path(factory[key]).is_absolute():
                raise TaskflowError("experiment factory requires an explicit absolute " + key)
    destination = Path(destination).absolute()
    frozen = freeze_batch_plan(plan, destination / "confirmation")
    reference = frozen["snapshot"]
    snapshot = load_batch_snapshot(reference)
    if slots != {key: role_config(snapshot, key) for key in slots}:
        raise TaskflowError("batch choices changed between preflight and immutable snapshot")
    roles = {}
    for role, slot in ROLE_SLOTS.items():
        if slot in slots:
            roles[role] = {**policy.get("roles", {}).get(role, {}), **slots[slot]}
    if "quality" not in roles:
        alternatives = [slot for slot in ("public_rebuild", "recheck") if slot in slots]
        if alternatives:
            roles["quality"] = slots[alternatives[0]]
    policy.update(roles=roles, model_confirmation_required=True, batch_snapshot=reference, model_slots=slots,
                  batch_infrastructure=evidence_ref(Path(infrastructure).absolute(), "explicit model-independent runtime infrastructure"))
    validate_batch_policy(policy)
    record = {"operation": "register_policy", "arguments": {"policy_id": identity, "config": policy}}
    record_path = destination / "policy-record.json"
    _publish_once(record_path, record)
    record_ref = evidence_ref(record_path, "original new batch policy registration bytes")
    products = {}
    for kind in ("author", "experiment"):
        field = kind + "_factory"
        if field not in base:
            continue
        factory = copy.deepcopy(base[field])
        factory.update(policy_id=identity, batch_snapshot=reference, policy_record_ref=record_ref)
        if kind == "experiment":
            technical = factory["technical"]
            _selected(technical["formal_config_base"], slots["solver"])
            _selected(technical["review_runner"], slots["reviewer"])
            profile = technical.get("local_profile_policy")
            if profile is not None:
                profile["required_model"] = slots["solver"]["model"]
                profile["required_client_reasoning"] = {"effort": slots["solver"]["reasoning_effort"],
                                                         "context": slots["solver"].get("reasoning_context")}
        path = destination / (kind + "-factory.json")
        _publish_once(path, factory)
        products[kind + "_factory"] = evidence_ref(path, "confirmed batch " + kind + " infrastructure")
    result = {"schema_version": 1, "batch_id": snapshot["batch_id"], "snapshot": reference,
              "policy": record_ref, "policy_digest": sha256(canonical(policy)), **products,
              "model_calls": 0, "production_database_touched": False}
    _publish_once(destination / "configuration.json", result)
    return result


def checked_batch_factory(factory_path: Path, packet_path: Path) -> tuple[dict, dict]:
    """Preflight the new batch entry before creating a store or any runtime."""
    factory, packet = read_json(factory_path), read_json(packet_path)
    ref = validate_evidence([factory["policy_record_ref"]])[0]
    record = read_json(Path(ref["path"]))
    if record.get("operation") != "register_policy":
        raise TaskflowError("batch factory has no original policy registration record")
    policy = record["arguments"]
    snapshot = validate_batch_policy(policy["config"])
    if (snapshot is None or policy["policy_id"] != factory.get("policy_id")
            or canonical(policy["config"]["batch_snapshot"]) != canonical(factory.get("batch_snapshot"))):
        raise TaskflowError("batch factory differs from its original confirmed policy")
    if packet.get("batch_id") != snapshot["batch_id"]:
        raise TaskflowError("business packet batch_id differs from the user-confirmed batch")
    items = packet.get("items")
    if not isinstance(items, list) or not items:
        raise TaskflowError("batch factory needs explicit task business items")
    if any(item.get("task_id") not in snapshot["task_ids"] for item in items):
        raise TaskflowError("batch business task is outside user model confirmation scope")
    return factory, policy
