"""Read observable run measurements without judging difficulty or model ability.

This module does not execute, reconcile, or modify a run. Native CLI events are
reported in their own units; tool items are never converted into official steps.
"""

from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat


class MetricsError(ValueError):
    """A requested evidence path is unsafe or exceeds its bounded read size."""


def _bytes(path: Path, limit: int) -> bytes:
    path = path.absolute()
    directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:-1]:
            next_directory = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                     dir_fd=directory)
            os.close(directory)
            directory = next_directory
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=directory)
    except OSError as error:
        if isinstance(error, FileNotFoundError):
            raise
        raise MetricsError("evidence path must contain ordinary files/directories: " + str(path)) from error
    finally:
        os.close(directory)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise MetricsError("evidence is not an ordinary file: " + str(path))
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise MetricsError("evidence exceeds read size limit: " + str(path))
    return raw


def _reject_constant(value: str):
    raise ValueError("nonfinite JSON constant: " + value)


def _json(raw: bytes):
    return json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)


def _instant(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return result if result.utcoffset() is not None else None


def _reconnection(event: dict) -> bool:
    message = event.get("message")
    if event.get("type") != "error" or not isinstance(message, str):
        return False
    if message == "Reconnecting... waiting for network (Connection failed: error sending request)":
        return True
    match = re.fullmatch(r"Reconnecting\.\.\. ([1-9][0-9]*)/([1-9][0-9]*) \((.+)\)", message)
    return match is not None and int(match[1]) <= int(match[2])


def _trace(raw: bytes | None, limitations: list[str]) -> dict:
    if raw is None:
        return {"events": None, "parsed_events": None, "agent_messages": None,
                "command_executions": None, "file_changes": None, "tool_calls": None,
                "item_types": None, "duplicate_item_completions": [], "malformed_lines": [],
                "reconnections": None, "usage": [], "sids": [], "terminal_events": [],
                "timestamped_events": None}
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError:
        limitations.append("trace_invalid_utf8")
        return _trace(None, limitations)
    events, items = Counter(), Counter()
    seen, duplicates, malformed, usage, sids, terminals = {}, [], [], [], [], []
    reconnects = timestamps = 0
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            event = _json(line.encode("utf-8"))
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                raise ValueError("event type required")
        except (ValueError, UnicodeError):
            malformed.append(line_number)
            continue
        kind = event["type"]
        events[kind] += 1
        if _instant(event.get("timestamp")) or _instant(event.get("time")):
            timestamps += 1
        if _reconnection(event):
            reconnects += 1
        if kind == "thread.started":
            if isinstance(event.get("thread_id"), str):
                sids.append(event["thread_id"])
        if kind in {"turn.completed", "turn.failed"}:
            terminals.append(kind)
        if kind == "turn.completed" and "usage" in event:
            usage.append({"line": line_number, "value": event["usage"]})
        if kind != "item.completed":
            continue
        item = event.get("item")
        if not isinstance(item, dict) or not isinstance(item.get("type"), str):
            malformed.append(line_number)
            continue
        identity = item.get("id")
        if isinstance(identity, str) and identity:
            if identity in seen:
                duplicates.append({"id": identity, "first_line": seen[identity], "line": line_number})
                continue
            seen[identity] = line_number
        else:
            limitations.append("completed_items_without_ids_cannot_be_deduplicated")
        items[item["type"]] += 1
    if malformed:
        limitations.append("trace_contains_malformed_events_counts_are_partial")
    if duplicates:
        limitations.append("duplicate_completed_item_ids_counted_once")
    return {"events": dict(events), "parsed_events": sum(events.values()),
            "agent_messages": items["agent_message"], "command_executions": items["command_execution"],
            "file_changes": items["file_change"],
            "tool_calls": sum(items[name] for name in ("command_execution", "mcp_tool_call", "web_search")),
            "item_types": dict(items), "duplicate_item_completions": duplicates,
            "malformed_lines": malformed, "reconnections": reconnects, "usage": usage,
            "sids": sids, "terminal_events": terminals, "timestamped_events": timestamps}


def usage_summary(usage: list[dict], completed_turns: int | None) -> dict:
    """Project reported CLI counters; absent counters and billing stay unknown."""
    complete = bool(usage) and completed_turns == len(usage)
    totals = {}
    for key in ("input_tokens", "cached_input_tokens", "output_tokens"):
        values = [row.get("value", {}).get(key) if isinstance(row.get("value"), dict)
                  else None for row in usage]
        totals[key] = (sum(values) if complete and all(type(value) is int and value >= 0
                                                     for value in values) else None)
    return {"reported_turns": len(usage), "completed_turns": completed_turns,
            **totals, "unit": "CLI reported tokens", "actual_billing": None}


def runtime_metrics(runtime: Path) -> dict:
    """Return bounded read-only measurements and explicit unknowns for a run.

    ``gross_seconds`` is process wall time, including any reconnects or waits.
    ``tool_calls`` counts recognized completed command/MCP/web-search items.
    It excludes file-change items and is neither all LLM requests nor steps.
    """
    runtime = Path(runtime).absolute()
    raw, metadata, evidence, limitations = {}, {}, [], []
    for name in ("manifest.json", "collection.json", "termination.json", "trace.jsonl"):
        path = runtime / name
        try:
            value = _bytes(path, 128 * 1024 * 1024 if name == "trace.jsonl" else 16 * 1024 * 1024)
        except FileNotFoundError:
            raw[name] = None
            limitations.append("missing_" + name)
            continue
        raw[name] = value
        evidence.append({"path": str(path), "sha256": hashlib.sha256(value).hexdigest(), "size": len(value)})
        if name != "trace.jsonl":
            try:
                parsed = _json(value)
                if not isinstance(parsed, dict):
                    raise ValueError("metadata object required")
                metadata[name] = parsed
            except (ValueError, UnicodeError):
                limitations.append("invalid_" + name)
    manifest = metadata.get("manifest.json", {})
    collection = metadata.get("collection.json", {})
    termination = metadata.get("termination.json", {})
    if not termination and isinstance(collection.get("termination"), dict):
        termination = collection["termination"]
    elif termination and isinstance(collection.get("termination"), dict) and collection["termination"] != termination:
        limitations.append("collection_termination_differs_from_original")
    started, ended = termination.get("started_at"), termination.get("ended_at")
    first, last = _instant(started), _instant(ended)
    gross = None
    if first is not None and last is not None and last >= first:
        gross = (last - first).total_seconds()
    else:
        limitations.append("missing_invalid_or_reversed_process_times")
    measured = _trace(raw.get("trace.jsonl"), limitations)
    trace_sids = set(measured.pop("sids"))
    collected_sid = collection.get("sid")
    sid = collected_sid if isinstance(collected_sid, str) else None
    if len(trace_sids) > 1 or (sid is not None and trace_sids and trace_sids != {sid}):
        sid = None
        limitations.append("conflicting_session_ids")
    elif sid is None and len(trace_sids) == 1:
        sid = next(iter(trace_sids))
    mode = manifest.get("mode")
    mode = mode if isinstance(mode, str) else None
    model = manifest.get("model")
    model = model if isinstance(model, dict) else {}
    inputs = manifest.get("inputs")
    if isinstance(inputs, list) and all(isinstance(row, dict) and isinstance(row.get("kind"), str) for row in inputs):
        input_kinds = dict(Counter(row["kind"] for row in inputs))
    else:
        input_kinds = None
        limitations.append("input_kind_membership_unavailable")
    limitations.append("actual_billing_not_observed")
    return {"schema_version": 2, "kind": "observable_runtime_metrics_v2",
            "mode": mode,
            "nature": {"native_role": "real_native", "test_fixture": "test_fixture"}.get(mode, "other"),
            "requested_model": model.get("requested"), "effort": model.get("reasoning_effort"),
            "served": model.get("served"), "cli_version": model.get("cli_version"), "sid": sid,
            "completion": {"status": collection.get("status"),
                           "technical_accepted": collection.get("technical_accepted"),
                           "model_completed": collection.get("model_completed"),
                           "returncode": termination.get("returncode"),
                           "timed_out": termination.get("timed_out"),
                           "terminal_events": measured.pop("terminal_events")},
            "timing": {"started_at": started, "ended_at": ended, "gross_seconds": gross},
            "counts": {key: value for key, value in measured.items() if key not in ("reconnections", "usage")},
            "reconnections": measured["reconnections"], "usage": measured["usage"],
            "usage_summary": usage_summary(measured["usage"],
                measured["events"].get("turn.completed", 0) if measured["events"] is not None else None),
            "actual_billing": None, "inputs_by_kind": input_kinds, "evidence": evidence,
            "limitations": sorted(set(limitations))}
