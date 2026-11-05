"""Bind a new batch runner to its original confirmed policy, including offline reads.

Legacy manifests have no batch binding and keep their original validation path.
This module does not select models, launch processes, or consult a latest policy.
"""

import copy
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution_codex_provider import provider_references_equal


SLOTS = {
    "author": {"author"}, "repair": {"repair"}, "quality": {"quality", "public_rebuild", "recheck"},
    "content-inspector": {"content_inspector"}, "solver": {"solver"}, "review-facts": {"reviewer"}, "review-score": {"reviewer"},
}


def make_batch_binding(policy: dict, role: str, model_slot: str | None, task_id: str) -> dict | None:
    """Capture the registered policy bytes rather than a mutable policy filename."""
    from taskflow.execution_batch import policy_setting, validate_batch_policy

    snapshot = validate_batch_policy(policy["config"])
    if snapshot is None:
        if model_slot is not None:
            raise TaskflowError("model_slot requires a confirmed batch policy")
        return None
    setting = policy_setting(policy["config"], role, model_slot, task_id)
    slot = model_slot or next(iter(SLOTS.get(role, set())))
    if slot not in SLOTS.get(role, set()):
        raise TaskflowError("model_slot is not valid for this execution role")
    if policy["digest"] != sha256(canonical(policy["config"])):
        raise TaskflowError("registered batch policy digest differs from its original bytes")
    return {"schema_version": 1, "policy_id": policy["policy_id"], "policy_digest": policy["digest"],
            "policy_config": copy.deepcopy(policy["config"]), "batch_id": snapshot["batch_id"],
            "task_id": task_id, "model_slot": slot, "setting": setting}


def validate_batch_runner(value: dict, *, manifest: bool = False) -> dict | None:
    """Verify an exact original confirmation and the actual requested runner values."""
    from taskflow.execution_batch import policy_setting, validate_batch_policy

    binding = value.get("batch_binding")
    if binding is None:
        if value.get("model_slot") is not None:
            raise TaskflowError("model_slot has no original batch policy binding")
        return None
    if not isinstance(binding, dict) or type(binding.get("schema_version")) is not int or binding["schema_version"] != 1:
        raise TaskflowError("invalid original batch runner binding")
    policy = binding.get("policy_config")
    if not isinstance(policy, dict) or sha256(canonical(policy)) != binding.get("policy_digest"):
        raise TaskflowError("batch runner original policy bytes changed")
    snapshot = validate_batch_policy(policy)
    role, slot = value.get("role"), binding.get("model_slot")
    if (snapshot is None or slot not in SLOTS.get(role, set())
            or value.get("model_slot") != slot or value.get("policy_version") != binding.get("policy_id")
            or snapshot["batch_id"] != binding.get("batch_id")):
        raise TaskflowError("batch runner role, slot, policy or batch differs")
    if snapshot["execution_mode"] == "test_fixture" and value.get("mode") != "test_fixture":
        raise TaskflowError("fixture batch confirmation cannot authorize a real model runner")
    public_adapter = value.get("mode") == "existing_isolated" and (value.get("isolated") or {}).get("flavor", "quality") == "quality"
    if value.get("mode") == "native_role" or public_adapter:
        if (value.get("reasoning_selection", "explicit") != "explicit"
                or value.get("reasoning_context") is not None):
            raise TaskflowError("selected reasoning mode/context is unsupported by this native/public adapter")
    setting = policy_setting(policy, role, slot, binding.get("task_id"))
    if canonical(setting) != canonical(binding.get("setting")):
        raise TaskflowError("batch runner setting differs from its original user confirmation")
    observed = value.get("model", {}) if manifest else value
    expected_native = setting.get("native_provider")
    actual_native = observed.get("native_provider_ref") if manifest else value.get("native_provider")
    if not provider_references_equal(expected_native, actual_native) or (expected_native is not None and not (value.get("mode") == "native_role" or public_adapter or (value.get("isolated") or {}).get("flavor") == "formal")):
        raise TaskflowError("native Codex provider differs from confirmed batch")
    model = observed.get("requested") if manifest else observed.get("model")
    if (model != setting["model"] or observed.get("reasoning_effort") != setting["reasoning_effort"]
            or value.get("timeout_seconds") != setting["timeout_seconds"]):
        raise TaskflowError("actual runner model, reasoning or timeout differs from the confirmed batch")
    for key in ("reasoning_selection", "reasoning_context"):
        if value.get(key, "explicit" if key == "reasoning_selection" else None) != setting.get(key, "explicit" if key == "reasoning_selection" else None):
            raise TaskflowError("actual runner reasoning selection differs from the confirmed batch")
    cli = setting.get("cli")
    if cli and value.get("mode") != "test_fixture":
        actual = (value.get("argv") or [None])[0] if manifest else value.get("cli")
        # The isolated adapter locks its actual CLI command in the inner plan.
        if manifest and value.get("mode") == "existing_isolated":
            from taskflow.contracts import read_json

            bridge = value.get("isolated") or {}
            name = "execution-plan.json" if bridge.get("flavor") == "formal" else "plan.json"
            plan = read_json(Path(bridge["runtime"]) / name)
            actual = (plan.get("argv") or [None])[0]
        if actual is None or Path(actual).resolve() != Path(cli).resolve():
            raise TaskflowError("actual runner CLI differs from the confirmed batch service")
    candidate = value.get("candidate_binding") or {}
    if candidate.get("task_id") != binding.get("task_id"):
        raise TaskflowError("batch runner candidate task is outside its confirmed scope")
    if role == "review-score":
        from taskflow.execution_runner import _validate

        facts = _validate(Path(value["facts_attempt"]))
        if facts.get("role") != "review-facts" or canonical(facts.get("batch_binding")) != canonical(binding):
            raise TaskflowError("facts and scoring must retain the same original batch reviewer model and policy")
    return binding


def validate_batch_source(attempt: dict, manifest: dict, store: object | None = None) -> bool:
    """Validate new source identity; return False only for an unbound legacy source."""
    binding = validate_batch_runner(manifest, manifest=True)
    if binding is None:
        if attempt.get("model_slot") is not None:
            raise TaskflowError("batch source lost its original runner binding")
        return False
    if any(attempt.get(key) != binding[key] for key in ("policy_id", "policy_digest", "task_id", "model_slot")):
        raise TaskflowError("batch source attempt differs from its original policy/slot/scope")
    if store is not None:
        registered = store.get_policy(binding["policy_id"])
        if registered["digest"] != binding["policy_digest"] or canonical(registered["config"]) != canonical(binding["policy_config"]):
            raise TaskflowError("batch source differs from the original registered versioned policy")
    return True
