"""Validate a solver-only policy version when a new experiment keeps an old source.

No database mutation, new scheduler or retrofit of existing experiments.
"""

import copy
import json
from pathlib import Path

from taskflow.contracts import TaskflowError, validate_id
from taskflow.execution_batch import policy_setting, validate_batch_policy
from taskflow.execution_runner import _read, _regular
from taskflow.execution_store import same_original_ref, validate_evidence


KIND = "future_solver_profile_update_v1"
FIELDS = ("model", "cli", "reasoning_effort", "reasoning_selection", "reasoning_context", "timeout_seconds")


def _require(value, message):
    if not value:
        raise TaskflowError("solver profile: " + message)


def validated_profile(reference):
    """Verify frozen policies, confirmation, factory and deterministic adapter sources."""
    reference = validate_evidence([reference])[0]
    packet = _read(Path(reference["path"]))
    _require(type(packet.get("schema_version")) is int and packet["schema_version"] == 1 and packet.get("kind") == KIND
             and packet.get("changed_role") == "solver", "explicit solver-only update required")
    task = validate_id(packet["task_id"], "task_id")
    source_record, target_record, factory_ref, original_factory_ref = validate_evidence([
        packet["source_policy_record"], packet["target_policy_record"], packet["target_factory"], packet["source_factory"]])
    original = _read(Path(source_record["path"]))
    target = _read(Path(target_record["path"]))
    _require(original.get("operation") == target.get("operation") == "register_policy", "policy registration sources required")
    old, new = original["arguments"], target["arguments"]
    _require(old["policy_id"] != new["policy_id"], "new immutable policy version required")
    before, after = validate_batch_policy(old["config"]), validate_batch_policy(new["config"])
    _require(before is not None and after is not None and before["scope"] == after["scope"]
             and task in after["scope"]["task_ids"], "same confirmed task and batch scope required")
    from taskflow.execution_runtime_support import original_support, validate_support_refresh

    support_pairs = validate_support_refresh(packet, old, new, before, after)
    comparable_roles = copy.deepcopy(after["roles"])
    for key in comparable_roles:
        comparable_roles[key]["runtime"] = original_support(
            comparable_roles[key]["runtime"], before["roles"][key]["runtime"], support_pairs)
    _require(all(before["roles"][key] == comparable_roles[key] for key in before["roles"] if key != "solver"),
             "non-solver role configuration changed")
    # Version metadata changes with a new confirmation; operational limits do not.
    metadata = {"batch_snapshot", "batch_infrastructure", "model_slots", "roles"}
    _require({k:v for k,v in old["config"].items() if k not in metadata}
             == {k:v for k,v in new["config"].items() if k not in metadata}, "operational policy changed")
    _require({k:v for k,v in old["config"]["roles"].items() if k != "solver"}
             == {k:original_support(v, old["config"]["roles"][k], support_pairs)
                 for k,v in new["config"]["roles"].items() if k != "solver"}, "non-solver low-level policy changed")
    mutable_runtime = {"reasoning_effort", "reasoning_selection", "reasoning_context", "support_evidence"}
    _require({k:v for k,v in before["roles"]["solver"]["runtime"].items() if k not in mutable_runtime}
             == {k:v for k,v in after["roles"]["solver"]["runtime"].items() if k not in mutable_runtime},
             "solver carrier, provider, CLI or timeout changed")
    _require(new["config"]["roles"]["solver"]
             == {**old["config"]["roles"]["solver"], **new["config"]["model_slots"]["solver"]},
             "solver low-level capabilities changed")
    selected = policy_setting(new["config"], "solver", "solver", task)
    _require(selected["reasoning_selection"] == "explicit" and selected["reasoning_context"] is None,
             "explicit confirmed solver profile required")
    factory = _read(Path(factory_ref["path"]))
    _require(factory.get("policy_id") == new["policy_id"]
             and factory.get("policy_record_ref") == target_record
             and factory.get("batch_snapshot") == new["config"]["batch_snapshot"], "factory policy/version differs")
    original_factory = _read(Path(original_factory_ref["path"]))
    _require(original_factory["policy_id"] == old["policy_id"], "source factory policy differs")
    config = factory["technical"]["formal_config_base"]
    _require(all(config.get(key) == selected.get(key) for key in FIELDS), "formal configuration differs from confirmed solver slot")
    reviewer = policy_setting(new["config"], "review-facts", "reviewer", task)
    _require(all(factory["technical"]["review_runner"].get(key) == reviewer.get(key) for key in FIELDS),
             "reviewer configuration changed")
    original_config = original_factory["technical"]["formal_config_base"]
    allowed = set(FIELDS) | {"adapter"}
    _require({k:v for k,v in config.items() if k not in allowed}
             == {k:v for k,v in original_config.items() if k not in allowed}, "formal capabilities or authority changed")
    expected_factory = copy.deepcopy(original_factory)
    for key in ("policy_id", "policy_record_ref", "batch_snapshot"):
        expected_factory[key] = factory[key]
    expected_factory["technical"]["formal_config_base"].update(
        {key:selected.get(key) for key in FIELDS})
    expected_factory["technical"]["formal_config_base"]["adapter"] = config["adapter"]
    local_profile = expected_factory["technical"].get("local_profile_policy")
    if local_profile is not None:
        local_profile.update(required_model=selected["model"], required_client_reasoning={
            "effort":selected["reasoning_effort"], "context":selected["reasoning_context"]})
    _require(factory == expected_factory,
             "factory non-solver infrastructure, permissions, requests or capabilities changed")
    adapter = config["adapter"]
    migration = _read(Path(adapter["path"]).parent / "profile-migration.json")
    _require(migration.get("adapter") == adapter and migration.get("snapshot") == new["config"]["batch_snapshot"]
             and migration.get("source_adapter") == original_config["adapter"],
             "adapter profile migration provenance differs")
    from tools.build_formal_profile_adapter import rendered_profile_sources

    refresh = migration.get("runtime_support_refresh")
    _require(refresh is None or refresh == packet.get("runtime_support_refresh"),
             "adapter CLI refresh differs from received support metadata")
    probe_scope = migration.get("probe_scope")
    _require(probe_scope == packet.get("probe_scope"), "adapter probe scope differs from authorized profile")
    transport_retry_mode = migration.get("transport_retry_mode", "legacy_disabled")
    _require(transport_retry_mode in ("legacy_disabled", "codex_default_v1")
             and packet.get("transport_retry_mode", "legacy_disabled") == transport_retry_mode,
             "adapter transport retry mode differs from explicit profile authorization")
    expected, _ = rendered_profile_sources(adapter_ref=migration["source_adapter"], snapshot_ref=migration["snapshot"],
                                           runtime_support_refresh=refresh, probe_scope=probe_scope,
                                           transport_retry_mode=transport_retry_mode)
    for name, raw in expected.items():
        _require(_regular(Path(adapter["path"]).parent / name) == raw, "adapter differs from deterministic profile migration")
    return {"reference": reference, "packet": packet, "old": old, "new": new,
            "selected": selected, "factory": factory, "source_factory": original_factory, "migration": migration}



def source_policy_id(store, definition):
    """A new experiment may retain an exact old author/quality policy as source."""
    reference = definition.get("solver_profile_update")
    if reference is None:
        return definition["policy_id"]
    profile = validated_profile(reference)
    _require(definition["task_id"] == profile["packet"]["task_id"]
             and definition["policy_id"] == profile["new"]["policy_id"]
             and same_original_ref(definition["factory_config_ref"], profile["packet"]["target_factory"]),
             "experiment differs from its explicit source/target profile")
    for policy in (profile["old"], profile["new"]):
        _require(store.get_policy(policy["policy_id"])["config"] == policy["config"],
                 "registered source/target policy differs")
    _require(definition["technical"]["formal_config_base"]["adapter"]
             == profile["factory"]["technical"]["formal_config_base"]["adapter"],
             "experiment adapter differs from confirmed target factory")
    return profile["old"]["policy_id"]
