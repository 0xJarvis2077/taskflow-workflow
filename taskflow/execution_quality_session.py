"""Bounded unfinished quality turns, with immutable same-SID source seals.

A progress seal is a technical receipt. It is never a quality review or a
substitute for the final independent whole-package signoff.
"""

import json
import os
from pathlib import Path

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, relative_path, sha256
from taskflow.execution_store import evidence_ref, validate_evidence


PLAN_KIND = "serial_whole_quality_review_v1"
from taskflow.execution_quality_progress_contract import PROGRESS_KIND
SEAL_NAME = "quality-reading-seal.json"
BINDING = ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")


def progress_name(session):
    return "reading-progress-" + str(session["index"]).zfill(3) + ".json"


def _require(value, message):
    if not value:
        raise TaskflowError("serial quality reading: " + message)


def validate_plan(reference, binding):
    from taskflow.execution_runner import _read, _info

    _require(isinstance(binding, dict) and set(binding) == set(BINDING), "complete candidate binding required")
    reference = validate_evidence([reference])[0]
    plan = _read(Path(reference["path"]))
    _require(isinstance(plan, dict) and set(plan) == {"schema_version", "kind", "candidate_binding", "turns", "pages",
                          "source_refs", "direct_user_authority", "max_progress_turns", "max_final_turns"}
             and type(plan["schema_version"]) is int and plan["schema_version"] == 1
             and plan["kind"] == PLAN_KIND and plan["candidate_binding"] == binding,
             "exact current-candidate reading plan required")
    validate_evidence([plan["direct_user_authority"], *plan["source_refs"]])
    turns, pages = plan["turns"], plan["pages"]
    _require(isinstance(turns, list) and bool(turns) and isinstance(pages, list)
             and type(plan["max_progress_turns"]) is int and plan["max_progress_turns"] == len(turns)
             and type(plan["max_final_turns"]) is int and plan["max_final_turns"] == 1,
             "finite explicit progress turns and one final turn required")
    ids = []
    for index, turn in enumerate(turns):
        _require(isinstance(turn, dict) and set(turn) == {"name", "kind", "instructions", "page_ids"}
                 and isinstance(turn["name"], str) and bool(turn["name"])
                 and turn["kind"] == ("public_first" if index == 0 else "read")
                 and isinstance(turn["instructions"], str) and bool(turn["instructions"].strip())
                 and isinstance(turn["page_ids"], list)
                 and all(isinstance(pid, str) and pid for pid in turn["page_ids"]),
                 "ordered public-first/read turns required")
        _require(index != 0 or not turn["page_ids"], "first turn cannot assign private reading pages")
        ids.extend(turn["page_ids"])
    _require(len({t["name"] for t in turns}) == len(turns), "turn names must be unique")
    page_ids = []
    for page in pages:
        _require(isinstance(page, dict) and set(page) == {"id", "path", "sha256", "size", "source_refs"}
                 and isinstance(page["id"], str) and bool(page["id"])
                 and isinstance(page["source_refs"], list) and bool(page["source_refs"]),
                 "explicit page identity and source associations required")
        _require(type(page["size"]) is int and 0 <= page["size"] <= 6000
                 and _info(Path(page["path"])) == {k: page[k] for k in ("sha256", "size")},
                 "reading page source bytes changed")
        page_ids.append(page["id"])
    _require(len(page_ids) == len(set(page_ids)) and len(ids) == len(set(ids))
             and set(ids) == set(page_ids), "each reading page must have exactly one assigned turn")
    return reference, plan


def session_store():
    return (Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "sessions").resolve()


def _canonical_filesystem_arguments(argv, native, inputs_root):
    """Canonicalize only an exact generated string-table filesystem argument."""
    from tools.isolated_quality import inline_table

    prefix = "permissions." + native["profile"] + ".filesystem="
    matches = [index for index, argument in enumerate(argv) if argument.startswith(prefix)]
    _require(len(matches) == 1, "one native filesystem declaration required for equivalent capabilities")
    index = matches[0]
    table = argv[index][len(prefix):]
    _require(table.startswith("{") and table.endswith("}"), "explicit native filesystem table required")
    remaining = table[1:-1].strip()
    entries = {}
    decoder = json.JSONDecoder()
    while remaining:
        key, end = decoder.raw_decode(remaining)
        remaining = remaining[end:].lstrip()
        _require(remaining.startswith("="), "filesystem entry requires an explicit string value")
        value, end = decoder.raw_decode(remaining[1:].lstrip())
        _require(isinstance(key, str) and isinstance(value, str) and key not in entries,
                 "filesystem string entries must be unique")
        entries[key] = value
        remaining = remaining[1:].lstrip()[end:].lstrip()
        if remaining:
            _require(remaining.startswith(",") and bool(remaining[1:].strip()), "filesystem table separator is invalid")
            remaining = remaining[1:].lstrip()
    _require(entries == native["filesystem"], "filesystem command differs from declared native permissions")
    result = list(argv)
    normalized = {key.replace(str(inputs_root), "<SERIAL_TURN_INPUTS>"): item
                  for key, item in entries.items()}
    result[index] = prefix + inline_table(dict(sorted(normalized.items())))
    return result


def capability_signature(manifest, root, *, normalize_representation=False):
    """Only the per-turn staged-input pathname may vary, never a capability."""
    argv = manifest["argv"]
    prefix = argv[:argv.index("exec")] if manifest["mode"] == "native_role" else argv[:1]
    value = {key: manifest.get(key) for key in (
        "candidate_binding", "role", "mode", "policy_version", "cwd", "timeout_seconds",
        "reasoning_selection", "reasoning_context", "batch_binding", "model_slot", "native")}
    if normalize_representation and isinstance(value["native"], dict) and value["native"].get("provider") is None:
        value["native"] = {key: item for key, item in value["native"].items() if key != "provider"}
        prefix = _canonical_filesystem_arguments(prefix, value["native"], root / "inputs")
        value["native"]["sandbox_argv"] = _canonical_filesystem_arguments(value["native"]["sandbox_argv"], value["native"], root / "inputs")
    value["model"] = {key: manifest["model"].get(key) for key in ("requested", "reasoning_effort")}
    value["command_prefix"] = prefix
    normalized = canonical(value).replace(str(root / "inputs").encode(), b"<SERIAL_TURN_INPUTS>")
    return sha256(canonical(json.loads(normalized)) if normalize_representation else normalized)


def read_seal(reference):
    from taskflow.execution_runner import _read, _info, _regular, _validated_collection

    reference = validate_evidence([reference])[0]
    path = Path(reference["path"])
    _require(path.name == SEAL_NAME, "exact previous turn seal required")
    root = path.parent
    seal, manifest = _read(path), _read(root / "manifest.json")
    collected = _validated_collection(root, manifest)
    _require(_read(root / "manifest-lock.json") == _info(root / "manifest.json")
             and seal.get("schema_version") == 1 and seal.get("kind") == "quality_read_turn_seal_v1"
             and seal.get("manifest") == _info(root / "manifest.json")
             and seal.get("collection") == _info(root / "collection.json")
             and seal.get("sid") == collected.get("sid")
             and collected.get("status") == "completed" and collected.get("technical_accepted") is True
             and manifest.get("role") == "quality" and manifest.get("quality_session", {}).get("phase") == "progress"
             and seal.get("session") == manifest["quality_session"]
             and seal.get("signature") == capability_signature(manifest, root)
             and seal.get("snapshot") == _info(root / "session-prefix.jsonl"),
             "previous progress original, successful collection or session seal differs")
    for item in manifest["inputs"]:
        _require(_info(root / "inputs" / item["target"]) == {k:item[k] for k in ("size", "sha256")},
                 "previous turn staged inputs changed")
    name = progress_name(manifest["quality_session"])
    _require(seal.get("progress") == collected.get("artifacts", {}).get(name)
             and seal["progress"] == _info(root / "artifacts" / name),
             "previous sealed progress changed")
    _require(seal.get("owned_turn") == _info(root / "quality-session-observation.json")
             and seal["snapshot"] == _info(root / "quality-session-observed.jsonl"),
             "previous context no longer matches its original owned-turn capture")
    return reference, seal, manifest, _regular(root / "session-prefix.jsonl", limit=256 * 1024 * 1024)


def validate_session(value, *, root=None, before_run=False):
    """Validate native/fixture configuration or its immutable prepared manifest."""
    from taskflow.execution_runner import _regular

    session = value.get("quality_session")
    if session is None:
        return None
    _require(value.get("role") == "quality" and value.get("mode") in {"native_role", "test_fixture"},
             "reading sessions only extend the original quality role")
    _require(isinstance(session, dict) and set(session) == {"schema_version", "plan", "index", "phase", "previous", "flow_id", "session_store"}
             and type(session["schema_version"]) is int and session["schema_version"] == 1,
             "versioned quality session required")
    _, plan = validate_plan(session["plan"], value["candidate_binding"])
    index = session["index"]
    _require(type(index) is int and 0 <= index <= len(plan["turns"])
             and session["phase"] == ("progress" if index < len(plan["turns"]) else "final")
             and isinstance(session["flow_id"], str) and bool(session["flow_id"]),
             "turn is outside the finite authorized reading plan")
    if value["mode"] == "native_role":
        _require(Path(session["session_store"]) == session_store(), "native CLI session store differs")
    if index == 0:
        _require(session["previous"] is None and not value.get("resume_sid"),
                 "new whole review must start fresh; old inconclusive cannot resume")
    else:
        _, previous, manifest, snapshot = read_seal(session["previous"])
        prior = previous["session"]
        _require(prior["index"] == index - 1 and prior["plan"] == session["plan"]
                 and prior["flow_id"] == session["flow_id"]
                 and prior["session_store"] == session["session_store"]
                 and manifest["candidate_binding"] == value["candidate_binding"]
                 and value.get("resume_sid") == previous["sid"], "same unfinished review predecessor and SID required")
        if root is not None:
            # Keep each historical seal's original signature byte-for-byte.
            # Only null provider metadata and equivalent filesystem table order
            # may differ. Every actual capability and command stays bound.
            _require(capability_signature(value, root) == previous["signature"]
                     or capability_signature(value, root, normalize_representation=True)
                     == capability_signature(manifest, Path(session["previous"]["path"]).parent,
                                             normalize_representation=True),
                     "review model, policy, permission, workspace or command capabilities changed")
        actual = _regular(Path(previous["session_file"]), limit=256 * 1024 * 1024)
        _require(actual.startswith(snapshot) and (not before_run or actual == snapshot),
                 "persisted same-SID context changed outside the previous sealed turn")
    return plan


def _public_observations(root, manifest, observations):
    """Validate public source observations separately from assigned private pages.

    These are model reading claims, not proof of reading or business acceptance.
    The original receipt is retained unchanged.
    """
    from taskflow.execution_runner import _read, _info

    target = "execution/public-inputs.json"
    inputs = {item["target"]: item for item in manifest["inputs"]}
    index_path = root / "inputs" / target
    _require(target in inputs and _info(index_path) == {
        key: inputs[target][key] for key in ("sha256", "size")}, "public index differs from locked input")
    index = _read(index_path)
    _require(isinstance(index, dict) and set(index) == {"schema_version", "input_digest", "files"}
             and type(index["schema_version"]) is int and index["schema_version"] == 1
             and index["input_digest"] == manifest["candidate_binding"]["input_digest"]
             and isinstance(index["files"], list) and bool(index["files"]), "bound public index required")
    expected = {}
    for item in index["files"]:
        _require(isinstance(item, dict) and set(item) == {"path", "role", "sha256", "size"}
                 and isinstance(item["path"], str) and item["role"] in PUBLIC_ROLES
                 and type(item["size"]) is int and item["size"] >= 0, "public index row is invalid")
        name = relative_path(item["path"])
        staged = inputs.get(name)
        _require(name not in expected and staged is not None and staged["kind"] == "public"
                 and {key: staged[key] for key in ("sha256", "size")} == {
                     key: item[key] for key in ("sha256", "size")}
                 and _info(root / "inputs" / name) == {key: item[key] for key in ("sha256", "size")},
                 "public observation source differs from immutable staged bytes")
        expected[name] = item
    seen = set()
    for row in observations:
        _require(set(row) == {"path", "role", "sha256", "size", "read", "sha256_verified", "size_verified"}
                 and isinstance(row["path"], str) and row["path"] not in seen
                 and row["path"] in expected and type(row["size"]) is int
                 and {key: row[key] for key in ("path", "role", "sha256", "size")} == expected[row["path"]]
                 and isinstance(row["read"], str) and bool(row["read"].strip())
                 and type(row["sha256_verified"]) is bool and type(row["size_verified"]) is bool,
                 "public observation needs exact indexed identity and explicit reading claim")
        seen.add(row["path"])
    _require(seen == set(expected), "public observations must cover the complete public index exactly once")


def _visible_readback_chunks(path):
    """Locate actual successful tool-output IDs, excluding model echo/commands."""
    found = set()

    def visit(value):
        if isinstance(value, dict):
            if (isinstance(value.get("chunk_id"), str) and value.get("exit_code") == 0
                    and isinstance(value.get("output"), str)):
                found.add(value["chunk_id"])
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        payload = row.get("payload", {})
        if row.get("type") != "response_item" or payload.get("type") != "custom_tool_call_output":
            continue
        for block in payload.get("output", []):
            if not isinstance(block, dict) or not isinstance(block.get("text"), str):
                continue
            try:
                visit(json.loads(block["text"]))
            except ValueError:
                continue
    return found


def _split_body_output(root, manifest, page, expected, chunk):
    """Bind an orchestrated locator to owned output and an actual source read."""
    from taskflow.execution_runner import _read, _regular

    if not isinstance(chunk, str) or not chunk:
        return False
    body = _regular(Path(expected["path"])).decode("utf-8")
    capture = root / "quality-session-observed.jsonl"
    header = f"PAGE {page['id']} chunk {chunk} exit 0"
    witnessed = False
    for line in capture.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        payload = row.get("payload", {})
        if row.get("type") != "response_item" or payload.get("type") != "custom_tool_call_output":
            continue
        texts = [item["text"] for item in payload.get("output", [])
                 if isinstance(item, dict) and isinstance(item.get("text"), str)]
        if header in texts and body in "\n".join(texts):
            witnessed = True
            break
    if not witnessed:
        return False
    reference = manifest["quality_session"]["plan"]
    plan = _read(Path(reference["path"]))
    _require(expected in plan["pages"], "body read differs from its locked plan page")
    position = plan["pages"].index(expected)
    selection = '["pages"][' + str(position) + ']'
    for line in (root / "trace.jsonl").read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        item = event.get("item", {})
        command = item.get("command", "").replace('\\"', '"')
        bound = expected["path"] in command or (reference["path"] in command and selection in command)
        if (event.get("type") == "item.completed" and item.get("type") == "command_execution"
                and type(item.get("exit_code")) is int and item["exit_code"] == 0
                and bound and body in item.get("aggregated_output", "")):
            return True
    return False


def _page_evidence(root, manifest, page, expected):
    """Validate structured source observations without certifying comprehension."""
    from taskflow.execution_runner import _info, _read

    entries = page["evidence"]
    if all(isinstance(e, dict) and isinstance(e.get("path"), str) and e["path"].strip()
           and isinstance(e.get("location"), str) and e["location"].strip() for e in entries):
        return
    _require(all(isinstance(e, dict) and e for e in entries), "structured evidence needs explicit observations")
    anchors = [e for e in entries if "plan_page" in e]
    bodies = [e for e in entries if "body_page" in e]
    split_body_chunk = None
    if not anchors and bodies:
        bindings = [e for e in entries if "source_records" in e]
        _require(len(bodies) == len(bindings) == 1,
                 "split evidence needs one body and one source binding")
        body, binding = bodies[0], bindings[0]
        _require(type(body.get("bytes")) is int and body["bytes"] == expected["size"]
                 and binding.get("plan_turn") == f"turns[{manifest['quality_session']['index']}]",
                 "split evidence byte count or assigned turn differs")
        # A validation view of explicitly supplied fields; original observations
        # are returned untouched, with no invented path/location or readback.
        anchor = {"plan_page": body["body_page"], "queue_record": binding.get("queue_record"),
                  "queue_and_navigation_visible_tool_chunk": binding.get("navigation_queue_visible_read_chunk"),
                  "page_hash_verified_tool_chunk": binding.get("bindings_sha_check")}
        sources = [binding["source_records"]]
        split_body_chunk = body.get("actual_individual_body_read_exec_command_chunk")
    else:
        _require(len(anchors) == 1 and not bodies,
                 "structured evidence needs one unambiguous bound page locator")
        anchor = anchors[0]
        sources = [e["resolved_original_sources"] for e in entries if "resolved_original_sources" in e]
    _require(anchor.get("plan_page") == expected["path"] and isinstance(anchor.get("queue_record"), str),
             "structured evidence page differs from assigned original")
    parts = anchor["queue_record"].rsplit(":", 1)
    _require(len(parts) == 2 and parts[1].isdigit() and int(parts[1]) > 0,
             "structured evidence queue needs an exact original line")
    queue = Path(parts[0])
    _require(str(queue) in manifest["dependencies"] and _info(queue) == manifest["dependencies"][str(queue)],
             "structured evidence queue is not a locked original")
    rows = queue.read_text(encoding="utf-8").splitlines()
    index = int(parts[1]) - 1
    _require(index < len(rows), "structured evidence queue line is missing")
    row = json.loads(rows[index])
    _require(row.get("id") == page["id"] and row.get("sha256") == expected["sha256"]
             and row.get("bytes") == expected["size"] and row.get("page") == Path(expected["path"]).name
             and row.get("source_refs") == expected["source_refs"],
             "structured evidence queue identity or source associations differ")
    _require(len(sources) == 1 and isinstance(sources[0], list) and sources[0]
             and all(isinstance(e, dict) and isinstance(e.get("original"), str) and e["original"].strip()
                     and isinstance(e.get("ref"), str) for e in sources[0])
             and len(sources[0]) == len(expected["source_refs"])
             and {e["ref"] for e in sources[0]} == set(expected["source_refs"]),
             "structured evidence must retain every assigned original source association")
    observation = _read(root / "quality-session-observation.json")
    capture = root / "quality-session-observed.jsonl"
    _require(observation.get("snapshot") == _info(capture), "structured evidence owned capture differs")
    chunks = _visible_readback_chunks(capture)
    _require(all(isinstance(anchor.get(key), str) and anchor[key] in chunks for key in (
        "queue_and_navigation_visible_tool_chunk", "page_hash_verified_tool_chunk")),
        "structured evidence needs actual owned tool-output locators")
    if page["disposition"] == "same_bytes_this_sid":
        previous = manifest["quality_session"].get("previous")
        _require(previous is not None, "same-byte observation requires the sealed prior turn")
        _, _, _, _ = read_seal(previous)
        prior_chunks = _visible_readback_chunks(Path(previous["path"]).parent / "session-prefix.jsonl")
        witnesses = [e for e in entries if "prior_public_original" in e]
        _require(len(witnesses) == 1 and isinstance(witnesses[0]["prior_public_original"], str)
                 and witnesses[0]["prior_public_original"].strip()
                 and witnesses[0].get("prior_actual_full_body_tool_chunk") in prior_chunks,
                 "same-byte observation needs an actual prior same-SID readback")
    elif page["disposition"] == "read":
        if bodies:
            _require(isinstance(split_body_chunk, str) and (split_body_chunk in chunks
                     or _split_body_output(root, manifest, page, expected, split_body_chunk)),
                     "split evidence needs an actual owned body-read output")
        else:
            _require(any(isinstance(e.get("body_read"), str) and e["body_read"].strip() for e in entries),
                     "structured read evidence needs its explicit body-read location")


def _inherited_public_checkpoint(manifest, checkpoint, plan):
    """A later turn may cite, but cannot replace, the exact sealed first claim."""
    from taskflow.execution_runner import _read, _validated_collection

    reference = manifest["quality_session"].get("previous")
    expected_index = manifest["quality_session"]["index"] - 1
    while reference is not None:
        _, _, previous, _ = read_seal(reference)
        _require(previous["quality_session"]["index"] == expected_index,
                 "inherited checkpoint skipped a sealed prior turn")
        root = Path(reference["path"]).parent
        if expected_index == 0:
            result = _progress(root, previous, plan, _validated_collection(root, previous))
            _require(checkpoint == result["public_checkpoint"],
                     "later checkpoint differs from the sealed first public original")
            return
        reference = previous["quality_session"]["previous"]
        expected_index -= 1
    _require(False, "inherited checkpoint has no sealed first public original")


def _output_contract(root, manifest, plan):
    """Use the byte-locked standard-library program, never model attachments."""
    from types import ModuleType
    from taskflow.execution_runner import _info, _read, _regular
    from taskflow import execution_quality_progress_contract as legacy

    script = root / "execution_quality_progress_contract.py"
    context_path = root / "quality-output-contract.json"
    if str(script) in manifest.get("dependencies", {}):
        _require(_info(script) == manifest["dependencies"][str(script)]
                 and _info(context_path) == manifest["dependencies"].get(str(context_path)),
                 "output contract helper/context bytes changed")
        module = ModuleType("bound_quality_output_contract")
        exec(compile(_regular(script), str(script), "exec"), module.__dict__)
        context = _read(context_path)
        session = manifest["quality_session"]
        expected = [] if session["phase"] == "final" or session["index"] == 0 else [
            p for p in plan["pages"] if p["id"] in plan["turns"][session["index"]]["page_ids"]]
        _require(context.get("intent_id") == manifest["intent_id"]
                 and context.get("candidate_binding") == manifest["candidate_binding"]
                 and context.get("phase") == session["phase"] and context.get("index") == session["index"]
                 and context.get("assigned_pages") == expected, "output contract context differs from locked turn")
        return module, context
    _require(not script.exists() and not context_path.exists()
             and str(context_path) not in manifest.get("dependencies", {}),
             "declared output contract helper/context cannot fall back to legacy")
    index = root / "inputs/execution/public-inputs.json"
    files = _read(index).get("files", []) if index.exists() else []
    return legacy, legacy.make_context(manifest, plan, files, legacy=True)


def _progress(root, manifest, plan, collected):
    from taskflow.execution_runner import _read, _info

    session = manifest["quality_session"]
    name = progress_name(session)
    result = _read(root / "artifacts" / name)
    contract, context = _output_contract(root, manifest, plan)
    try:
        contract.validate_progress(result, context)
    except ValueError as error:
        _require(False, str(error))
    assigned = plan["turns"][session["index"]]["page_ids"]
    if session["index"] == 0 and not assigned and result["pages"]:
        _public_observations(root, manifest, result["pages"])
    if session["index"]:
        pages = {p["id"]: p for p in plan["pages"]}
        for page in result["pages"]:
            _page_evidence(root, manifest, page, pages[page["id"]])
    if session["index"] == 0:
        checkpoint = result["public_checkpoint"]
        _require(isinstance(checkpoint, dict)
                 and {"sha256", "tool_readback_location"} <= set(checkpoint)
                 and set(checkpoint) <= {"sha256", "tool_readback_location", "path", "sequence"}
                 and checkpoint["sha256"] == _info(root / "artifacts/public-responsibility-first.json")["sha256"]
                 and isinstance(checkpoint["tool_readback_location"], str) and bool(checkpoint["tool_readback_location"]),
                 "fresh public-first original and visible readback required")
        if "path" in checkpoint:
            _require(checkpoint["path"] == str(Path(manifest["cwd"]) / "public-responsibility-first.json"),
                     "public checkpoint path differs from original shared work")
        if "sequence" in checkpoint:
            _require(isinstance(checkpoint["sequence"], str) and bool(checkpoint["sequence"].strip()),
                     "public checkpoint sequence observation must be nonempty")
        events = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines() if line.strip()]
        marker = "PUBLIC_RESPONSIBILITY_FIRST_SHA256=" + checkpoint["sha256"]
        _require(any(event.get("type") == "item.completed"
                     and event.get("item", {}).get("type") == "command_execution"
                     and marker in event["item"].get("aggregated_output", "") for event in events),
                 "public-first checkpoint needs its actual visible tool readback")
    else:
        if result["public_checkpoint"] is not None:
            _inherited_public_checkpoint(manifest, result["public_checkpoint"], plan)
    _require(collected["artifacts"].get(name) == _info(root / "artifacts" / name),
             "progress is not the sealed original delivery")
    return result


def seal_turn(root):
    """Seal one completed turn before its full archive; no business signoff."""
    from taskflow.execution_runner import _read, _regular, _validate, _validated_collection, _info, _publish, _publish_bytes

    root = Path(root)
    manifest = _validate(root)
    session = manifest.get("quality_session")
    if session is None:
        return None
    path = root / SEAL_NAME
    if path.exists():
        # Final seals are never predecessors. Re-entry before receipt remains
        # idempotent without replacing a session snapshot with later bytes.
        saved = _read(path)
        _require(saved["manifest"] == _info(root / "manifest.json")
                 and saved["collection"] == _info(root / "collection.json")
                 and saved["session"] == session
                 and saved["snapshot"] == _info(root / "session-prefix.jsonl"), "existing turn seal changed")
        return evidence_ref(path, "original completed same-SID quality reading turn")
    plan = validate_session(manifest, root=root)
    collected = _validated_collection(root, manifest)
    _require(collected.get("status") == "completed" and collected.get("technical_accepted") is True
             and collected.get("sid"), "only a complete original turn can seal its unfinished context")
    progress = None
    if session["phase"] == "progress":
        progress = _progress(root, manifest, plan, collected)
        work = Path(manifest["cwd"])
        _require(not any((work / name).exists() for name in ("adoption-readiness.json", "issues.json", "intake.md")),
                 "a progress turn must not issue a quality signoff")
    if session["phase"] == "final" and str(root / "execution_quality_progress_contract.py") in manifest.get("dependencies", {}):
        contract, context = _output_contract(root, manifest, plan)
        if all(name in manifest["outputs"] for name in contract.FINAL_FILES):
            try:
                contract.check_delivery(context, root / "artifacts")
            except ValueError as error:
                _require(False, str(error))
    observed = _read(root / "quality-session-observation.json")
    data = _regular(root / "quality-session-observed.jsonl", limit=256 * 1024 * 1024)
    session_file = Path(observed["session_file"])
    _require(observed.get("sid") == collected["sid"] and observed.get("intent_id") == manifest["intent_id"]
             and observed.get("request") == manifest["request"]
             and observed.get("trace") == _info(root / "trace.jsonl")
             and observed.get("snapshot") == _info(root / "quality-session-observed.jsonl")
             and _regular(session_file, limit=256 * 1024 * 1024) == data,
             "persisted context changed after its owned-turn process-exit capture")
    meta = json.loads(data.splitlines()[0])
    _require(meta.get("type") == "session_meta" and meta.get("payload", {}).get("id") == collected["sid"],
             "persisted session original has a foreign identity")
    if session["previous"] is not None:
        _, previous, _, prefix = read_seal(session["previous"])
        _require(session_file == Path(previous["session_file"]) and data.startswith(prefix),
                 "same SID did not extend the exact retained session original")
    _publish_bytes(root / "session-prefix.jsonl", data)
    seal = {"schema_version":1, "kind":"quality_read_turn_seal_v1", "session":session,
            "manifest":_info(root / "manifest.json"), "collection":_info(root / "collection.json"),
            "sid":collected["sid"], "signature":capability_signature(manifest, root),
            "session_file":str(session_file), "snapshot":_info(root / "session-prefix.jsonl"),
            "owned_turn":_info(root / "quality-session-observation.json"), "turn_id":observed["turn_id"],
            "progress":_info(root / "artifacts" / progress_name(session)) if progress is not None else None,
            "quality_accepted":False, "limitations":["Same SID, retained prefix and visible turns prove local continuity, not semantic comprehension or served identity.",
                "Page dispositions are reviewer observations, never automatic quality acceptance."]}
    _publish(path, seal)
    return evidence_ref(path, "original completed same-SID quality reading turn")
