"""Validate explicit same-catalog support evidence after a host CLI update."""

import copy
import json
from pathlib import Path

from taskflow.contracts import TaskflowError, sha256
from taskflow.execution_runner import _info, _read, _regular
from taskflow.execution_store import validate_evidence


KIND = "same_catalog_runtime_support_refresh_v1"


def _frozen(reference: dict, snapshot: dict, item: dict) -> dict:
    matches = [row for row in snapshot["evidence_copies"]
               if all(row["original"].get(key) == item.get(key) for key in ("path", "sha256", "size"))]
    if len(matches) != 1:
        raise TaskflowError("runtime support refresh: evidence not captured exactly once")
    raw = _regular(Path(reference["path"]).parent / matches[0]["relative_path"], limit=16 * 1024 * 1024)
    if sha256(raw) != item["sha256"] or len(raw) != item["size"]:
        raise TaskflowError("runtime support refresh: frozen evidence changed")
    return json.loads(raw)


def validate_support_refresh(profile: dict, old: dict, new: dict, before: dict, after: dict) -> list[tuple[dict, dict]]:
    """Permit only declared proof replacement, with unchanged catalog bytes."""
    reference = profile.get("runtime_support_refresh")
    if reference is None:
        return []
    reference = validate_evidence([reference])[0]
    packet = _read(Path(reference["path"]))
    if (type(packet.get("schema_version")) is not int or packet["schema_version"] != 1
            or packet.get("kind") != KIND
            or packet.get("source_policy_record") != profile["source_policy_record"]
            or packet.get("target_policy_record") != profile["target_policy_record"]
            or not isinstance(packet.get("reason"), str) or not packet["reason"].strip()):
        raise TaskflowError("runtime support refresh: exact source/target provenance required")
    changed = [key for key in before["roles"] if
               (before["roles"][key].get("runtime") or {}).get("support_evidence") !=
               (after["roles"][key].get("runtime") or {}).get("support_evidence")]
    if not changed or packet.get("role_keys") != sorted(changed):
        raise TaskflowError("runtime support refresh: exact changed support roles required")
    pairs, observations = [], []
    for key in changed:
        left, right = before["roles"][key], after["roles"][key]
        a, b = left["runtime"], right["runtime"]
        if (not left["enabled"] or not right["enabled"] or a.get("carrier") != "cli"
                or b.get("carrier") != "cli" or a.get("native_provider") or b.get("native_provider")
                or a.get("cli") != b.get("cli") or a.get("provider") != b.get("provider")):
            raise TaskflowError("runtime support refresh: carrier/provider/CLI cannot change")
        old_ref, new_ref = a["support_evidence"], b["support_evidence"]
        previous = _frozen(old["config"]["batch_snapshot"], before, old_ref)
        current = _frozen(new["config"]["batch_snapshot"], after, new_ref)
        if (previous["cli"]["path"] != current["cli"]["path"] or current["cli"]["path"] != b["cli"]
                or any(previous["output"][field] != current["output"][field] for field in ("sha256", "size"))
                or _info(Path(b["cli"])) != {field: current["cli"][field] for field in ("sha256", "size")}):
            raise TaskflowError("runtime support refresh: catalog bytes or current CLI differ")
        capture = validate_evidence([current["catalog_capture"]])[0]
        observed = _read(Path(capture["path"]))
        if (type(observed.get("model_calls")) is not int or observed["model_calls"] != 0
                or type(observed.get("exit_code")) is not int or observed["exit_code"] != 0
                or observed.get("cli") != {field: current["cli"][field] for field in ("path", "sha256", "size")}
                or observed.get("argv") != current["argv"]):
            raise TaskflowError("runtime support refresh: bound zero-model current CLI observation required")
        pairs.append((old_ref, new_ref))
        if capture not in observations:
            observations.append(capture)
    if packet.get("observations") != observations:
        raise TaskflowError("runtime support refresh: exact current host observations required")
    return pairs


def original_support(value: dict, previous: dict, pairs: list[tuple[dict, dict]]) -> dict:
    """Normalize only an explicitly validated proof replacement for equality checks."""
    result = copy.deepcopy(value)
    if not isinstance(result, dict) or not isinstance(previous, dict):
        return result
    pair = (previous.get("support_evidence"), result.get("support_evidence"))
    if pair in pairs:
        result["support_evidence"] = copy.deepcopy(pair[0])
    return result
