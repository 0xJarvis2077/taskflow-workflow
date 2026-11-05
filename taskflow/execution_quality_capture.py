"""Pure parser for a single owned native CLI turn in a persisted increment.

This standard-library-only file is copied and hash-locked beside the standalone
supervisor. It does not infer comprehension or authenticate arbitrary host writes.
"""

import hashlib
import json
import uuid


def _require(value, message):
    if not value:
        raise ValueError("owned quality turn: " + message)


def _records(data):
    return [json.loads(line) for line in data.decode("utf-8").splitlines() if line.strip()]


def bind_increment(data, previous, request, trace, sid):
    """Bind one request/response/completion, rejecting additional user turns."""
    _require(isinstance(data, bytes) and isinstance(previous, bytes) and data.startswith(previous),
             "retained session prefix changed")
    _require(not previous or previous.endswith(b"\n"), "previous context is not a whole JSONL prefix")
    records, visible = _records(data), _records(trace)
    delta = _records(data[len(previous):])
    _require(records and records[0].get("type") == "session_meta"
             and records[0].get("payload", {}).get("id") == sid, "session original has a foreign SID")
    ids = [r.get("thread_id") for r in visible if r.get("type") == "thread.started"]
    starts = [r for r in visible if r.get("type") == "turn.started"]
    ends = [r.get("type") for r in visible if r.get("type") in {"turn.completed", "turn.failed", "error"}]
    messages = [r["item"].get("text") for r in visible
                if r.get("type") == "item.completed" and r.get("item", {}).get("type") == "agent_message"]
    _require(ids == [sid] and len(starts) == 1 and ends == ["turn.completed"]
             and messages and isinstance(messages[-1], str) and messages[-1].strip(),
             "one visible successful current turn required")
    started = [(i,r["payload"]) for i,r in enumerate(delta)
               if r.get("type") == "event_msg" and r.get("payload", {}).get("type") == "task_started"]
    completed = [(i,r["payload"]) for i,r in enumerate(delta)
                 if r.get("type") == "event_msg" and r.get("payload", {}).get("type") == "task_complete"]
    _require(len(started) == len(completed) == 1, "extra or missing persisted started/completed turn")
    begin, start = started[0]
    end, finish = completed[0]
    turn_id = start.get("turn_id")
    try:
        valid_id = str(uuid.UUID(turn_id)) == turn_id
    except (ValueError, TypeError, AttributeError):
        valid_id = False
    _require(valid_id and finish.get("turn_id") == turn_id and begin < end == len(delta) - 1
             and finish.get("last_agent_message") == messages[-1], "completion does not belong to this visible response")
    old_turns = {r.get("payload", {}).get("turn_id") for r in _records(previous)
                 if r.get("type") == "event_msg" and r.get("payload", {}).get("type") == "task_started"}
    _require(turn_id not in old_turns, "persisted turn identity was reused")
    actual_users, harness_kinds, completed_users, completed_agents = [], [], [], []
    for position, record in enumerate(delta):
        payload = record.get("payload", {})
        if record.get("type") == "event_msg":
            _require(payload.get("type") not in {"turn_aborted", "user_message"},
                     "unexpected external/legacy user or aborted turn")
            if payload.get("type") == "item_completed":
                _require(payload.get("thread_id") == sid and payload.get("turn_id") == turn_id,
                         "persisted item belongs to another SID or turn")
                item = payload.get("item", {})
                if item.get("type") == "UserMessage":
                    completed_users.append(item)
                elif item.get("type") == "AgentMessage":
                    completed_agents.append("".join(c.get("text", "") for c in item.get("content", [])
                                                    if c.get("type") == "Text"))
        if record.get("type") != "response_item" or payload.get("type") != "message" or payload.get("role") != "user":
            continue
        metadata = payload.get("internal_chat_message_metadata_passthrough")
        _require(isinstance(metadata, dict) and metadata.get("turn_id") == turn_id
                 and begin < position < end, "user context has no current owned-turn attribution")
        kinds = metadata.get("content_item_kinds")
        _require(isinstance(kinds, list) and bool(kinds), "unattributed extra user context")
        if kinds == ["user.text"]:
            actual_users.append(payload)
        else:
            allowed = {"agents_md.instructions", "environments.environment_context"}
            _require(set(kinds) <= allowed and (not previous or "agents_md.instructions" not in kinds),
                     "extra user instructions are not current native harness metadata")
            harness_kinds.extend(kinds)
    _require(len(harness_kinds) == len(set(harness_kinds)), "duplicate external harness/user additions")
    text = request.decode("utf-8")
    _require(len(actual_users) == len(completed_users) == 1
             and actual_users[0].get("content") == [{"type":"input_text", "text":text}]
             and completed_users[0].get("content") == [{"type":"text", "text":text, "text_elements":[]}],
             "persisted actual user request differs from the locked stdin request")
    _require(completed_agents == messages,
             "persisted assistant items differ from the actual visible responses")
    return {"schema_version":1, "kind":"owned_quality_turn_increment_v1", "sid":sid, "turn_id":turn_id,
            "request": {"size":len(request), "sha256":hashlib.sha256(request).hexdigest()},
            "trace": {"size":len(trace), "sha256":hashlib.sha256(trace).hexdigest()},
            "previous_bytes":len(previous), "captured_bytes":len(data), "increment_records":len(delta),
            "actual_user_messages":1, "completed_turns":1,
            "final_response_sha256":hashlib.sha256(messages[-1].encode()).hexdigest()}
