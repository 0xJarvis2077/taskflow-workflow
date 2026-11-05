"""Deterministic CLI-only baseline metadata refresh from confirmed host evidence."""

import copy
from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_batch import validate_batch_policy
from taskflow.execution_runner import _read
from taskflow.execution_runtime_support import _frozen, validate_support_refresh
from taskflow.execution_store import validate_evidence


def refreshed_cli_baseline(baseline: dict, *, refresh: dict, snapshot: dict) -> dict:
    refresh = validate_evidence([refresh])[0]
    packet = _read(Path(refresh["path"]))
    source, target = validate_evidence([packet["source_policy_record"], packet["target_policy_record"]])
    old_record, new_record = _read(Path(source["path"])), _read(Path(target["path"]))
    if old_record.get("operation") != "register_policy" or new_record.get("operation") != "register_policy":
        raise TaskflowError("CLI baseline refresh requires original policy registration sources")
    old, new = old_record["arguments"], new_record["arguments"]
    before, after = validate_batch_policy(old["config"]), validate_batch_policy(new["config"])
    if new["config"]["batch_snapshot"] != snapshot:
        raise TaskflowError("CLI baseline refresh belongs to another confirmed snapshot")
    validate_support_refresh({"runtime_support_refresh": refresh, "source_policy_record": source,
                              "target_policy_record": target}, old, new, before, after)
    a = before["roles"]["solver"]["runtime"]
    z = after["roles"]["solver"]["runtime"]
    previous = _frozen(old["config"]["batch_snapshot"], before, a["support_evidence"])
    current = _frozen(new["config"]["batch_snapshot"], after, z["support_evidence"])
    observed = baseline.get("observed_cli", {})
    # The pinned historical template may use a basename; preparation uses the
    # observed absolute CLI path. Preserve the template argv unchanged.
    if (observed.get("path") != previous["cli"]["path"] or observed.get("sha256") != previous["cli"]["sha256"]
            or not baseline.get("argv") or baseline["argv"][0] not in
               (previous["cli"]["path"], Path(previous["cli"]["path"]).name)
            or baseline.get("runtime_support_refresh") or baseline.get("historical_observed_cli")):
        raise TaskflowError("CLI baseline refresh does not match its exact historical baseline")
    result = copy.deepcopy(baseline)
    result.update(historical_observed_cli=copy.deepcopy(observed), runtime_support_refresh=refresh,
                  observed_cli={**{key: current["cli"][key] for key in ("path", "sha256", "size")},
                                "support_evidence": z["support_evidence"], "catalog_capture": current["catalog_capture"]})
    return result
