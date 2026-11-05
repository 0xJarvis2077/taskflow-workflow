"""Serial read turns inside one existing quality workflow and consumer."""

import copy
import json
import shlex
import sys
from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.execution_quality_session import (
    progress_name, read_seal, session_store, validate_plan,
)


def reading_state(reference, binding, step):
    reference, plan = validate_plan(reference, binding)
    return {"plan":reference, "candidate_binding":copy.deepcopy(binding), "step":step, "index":0, "turns":[]}


def layout(record):
    state = record["state"]
    configured = state.get("serial_quality_reading")
    if not configured or state["stage"] != "quality" or state["step"] != configured["step"]:
        return None
    binding = configured["candidate_binding"]
    # A repaired candidate is not silently substituted into an unfinished whole.
    if binding["candidate_id"] != state["candidate_id"] or binding["task_id"] != record["definition"]["task_id"]:
        raise TaskflowError("serial quality reading: unfinished whole changed its candidate")
    _, plan = validate_plan(configured["plan"], binding)
    index = configured["index"]
    if type(index) is not int or not 0 <= index <= len(plan["turns"]) or len(configured["turns"]) != index:
        raise TaskflowError("serial quality reading: progress cursor differs from its sealed turns")
    label = str(state["step"]) + "-quality"
    canonical = Path(record["definition"]["directory"]) / "nodes" / label
    final = index == len(plan["turns"])
    return {"plan":plan, "reference":configured["plan"], "index":index, "phase":"final" if final else "progress",
            "label":label if final else label + "-read-" + str(index).zfill(3),
            "root":canonical if final else canonical / "reading" / str(index).zfill(3),
            "work":canonical / "work", "scratch":canonical / "scratch", "canonical_label":label}


def context_metadata(record, active):
    result = {"plan":active["reference"], "index":active["index"], "phase":active["phase"],
              "maximum_progress_turns":len(active["plan"]["turns"]), "maximum_final_turns":1,
              "progress_is_not_quality_acceptance":True}
    if active["index"]:
        result["previous"] = record["state"]["serial_quality_reading"]["turns"][-1]["seal"]
    return result


def configure_runner(record, active, runner, context):
    from taskflow.execution_flow_contracts import public_first_instructions

    configured = record["state"]["serial_quality_reading"]
    previous = configured["turns"][-1]["seal"] if configured["turns"] else None
    session = {"schema_version":1, "plan":active["reference"], "index":active["index"],
               "phase":active["phase"], "previous":previous, "flow_id":record["flow_id"],
               "session_store":str(session_store())}
    if runner.get("mode") == "test_fixture":
        # Neutral tests must provide an isolated synthetic session store, never
        # claim a real CLI context. This field is not accepted for native roles.
        session["session_store"] = runner.pop("fixture_session_store")
    runner["quality_session"] = session
    if previous:
        _, seal, _, _ = read_seal(previous)
        runner["resume_sid"] = seal["sid"]
    else:
        runner.pop("resume_sid", None)
    dependencies = [active["reference"]["path"],
                    *[ref["path"] for ref in active["plan"]["source_refs"]],
                    active["plan"]["direct_user_authority"]["path"],
                    *[page["path"] for page in active["plan"]["pages"]]]
    for entry in configured["turns"]:
        prior_root = Path(entry["seal"]["path"]).parent
        dependencies.extend([entry["seal"]["path"], entry["archive_binding"]["path"],
                             str(prior_root / "session-prefix.jsonl")])
        from taskflow.execution_runner import _read

        original = _read(prior_root / "manifest.json")
        dependencies.extend(str(prior_root / name) for name in (
            "manifest.json", "manifest-lock.json", "request.md", "collection.json", "collection-lock.json",
            "termination.json", "trace.jsonl", "quality-session-observation.json", "quality-session-observed.jsonl",
            "execution_quality_capture.py"))
        dependencies.extend(str(prior_root / "artifacts" / name) for name in original["outputs"])
        dependencies.extend(str(prior_root / "inputs" / item["target"]) for item in original["inputs"])
        dependencies.extend(str(prior_root / name) for name in (
            "execution_quality_progress_contract.py", "quality-output-contract.json")
            if str(prior_root / name) in original.get("dependencies", {}))
    runner["dependencies"] = list(dict.fromkeys([*runner.get("dependencies", []), *dependencies]))
    from taskflow.execution_quality_progress_contract import make_context, render

    contract_root = Path(context["inputs_root"]).parent
    contract_manifest = {**runner, "candidate_binding": {k:context[k] for k in (
        "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
    command = shlex.join(["rtk", "proxy", runner.get("native_permissions", {}).get("python", sys.executable),
                          str(contract_root / "execution_quality_progress_contract.py"),
                          "--context", str(contract_root / "quality-output-contract.json")])
    preflight = ("Before ending this same model turn, run the existing local shell/Python tool: " + command
                 + ". It checks fields only. Correct delivery-format errors in this same turn without changing honest "
                 "reading/uncertainty/business conclusions. It never signs quality, verifies source truth or adds a model round. ")
    contract_context = make_context(contract_manifest, active["plan"])
    if active["phase"] == "final":
        runner["request_sections"].append({"kind":"operation", "text":
            "Continue this exact same independent SID's still-unsigned whole-package review. "
            "The original public-first checkpoint is retained in the shared work directory and first sealed reading turn; "
            "cite its actual original readback location. Do not claim this final turn started blind. "
            "Every reading-progress record is only this reviewer's intermediate source/association/unread facts, never a passed dimension. "
            "Inspect necessary originals and all unresolved duties, and issue the one ordinary whole-package accepted, revision_required or "
            "inconclusive signoff under the original protocol. The finite reading budget ends with this turn; no automatic additional turn is authorized."})
        runner["request_sections"].append({"kind":"operation", "text": render(contract_context) + " " + preflight})
        return
    name = progress_name(session)
    runner["outputs"] = [name] + (["public-responsibility-first.json"] if active["index"] == 0 else [])
    runner["output_trees"] = []
    turn = active["plan"]["turns"][active["index"]]
    text = ("This is one progress-only turn of a new independent whole review, not a quality signoff. "
            "Retain the same SID and shared work/scratch. Do not create intake.md, issues.json, adoption-readiness.json, "
            "accepted dimensions, a score, or any business acceptance. Prior inconclusive reviews and other authors' checks are not coverage. ")
    sections = []
    if active["index"] == 0:
        sections.append({"kind":"operation", "text":public_first_instructions(context)})
        text += ("Before any teacher, history, queue, plan or informed purpose read, use json.load on execution/current.json "
                 "and output only public_first, inputs_root, work and scratch. Read all current public originals, "
                 "save public-responsibility-first.json, read it back and print PUBLIC_RESPONSIBILITY_FIRST_SHA256=<actual SHA256>. "
                 "Then save the public-only progress record. Its public_checkpoint must contain the exact sha256 and "
                 "tool_readback_location. Do not open the private queue in this turn. ")
    else:
        text += ("The first turn completed and sealed the public-first checkpoint. "
                 "For this later progress receipt public_checkpoint must be null; cite the retained first checkpoint in "
                 "facts or evidence if needed, without overwriting it or claiming another blind public read. "
                 "Only now use json.load on execution/current.json "
                 'to read exactly current["quality_continuation"]["purpose"] and current["serial_quality_reading"], '
                 "in <=6000-byte responses without dumping other fields. Read the complete informed purpose. "
                 "Then read this turn's plan from "
                 + active["reference"]["path"] + ". Extract only turns[index] and its assigned page records, "
                 "in <=6000-byte responses; never dump the whole plan, navigation or current object. Assigned page IDs: " + json.dumps(turn["page_ids"]) + ". "
                 "Read each required body in a separate <=6000 UTF-8-byte tool response with adequate output-token allowance. "
                 "Inspect actual source uses, visible command/response/final-product associations and responsibilities; "
                 "resolve every source_refs association against the bound original navigation and queue records listed in source_refs, "
                 "reading only the relevant bounded records rather than dumping their entire inventories. "
                 "do not substitute hashes, counts, prior summaries or repeated sample arithmetic for the assigned reading. "
                 "Keep substantive source associations and all still-open responsibilities. "
                 "association states the actual responsibility/source/claim relation. Same-byte reuse requires the actual prior read in "
                 "this new SID, not an older signature. A nondependency disposition needs specific current-claim evidence; a teacher/history "
                 "label is insufficient. Unread pages remain unresolved with a concrete remaining duty; never invent reading or conclusions. ")
    text += ("This turn's authorized goal: " + turn["instructions"] + ". Save only " + name
             + ". " + render(contract_context) + " " + preflight
             + " A final message does not replace this file. Current immutable staged inputs: " + context["inputs_root"] + ".")
    sections.append({"kind":"operation", "text":text})
    runner["request_sections"] = sections
    runner.pop("request_text", None)


def receive_progress(store, record, attempt, archive, seal_ref):
    from taskflow.execution_flow import _save
    from taskflow.execution_runner import _read, _info

    active = layout(record)
    if active is None or active["phase"] != "progress":
        raise TaskflowError("serial quality reading: no current progress turn to receive")
    root = Path(attempt["runtime_path"])
    manifest = _read(root / "manifest.json")
    _, seal, _, _ = read_seal(seal_ref)
    session = manifest["quality_session"]
    if (session["index"] != active["index"] or session["plan"] != active["reference"]
            or session["flow_id"] != record["flow_id"] or seal["sid"] != attempt["cli_sid"]
            or attempt["state"] != "completed" or attempt["role"] != "quality"):
        raise TaskflowError("serial quality reading: original completed progress belongs to another turn")
    if active["index"] == 0:
        others = [row for row in store.dashboard()["attempts"] if row["attempt_id"] != attempt["attempt_id"]]
        if (seal["sid"] in record["definition"]["initial"].get("excluded_review_sids", [])
                or any(row.get("cli_sid") == seal["sid"] for row in others)):
            raise TaskflowError("serial quality reading: fresh independent whole reused a previous context")
    state = copy.deepcopy(record["state"])
    entry = {"kind":"quality_reading_progress", "attempt_id":attempt["attempt_id"], "cli_sid":seal["sid"],
             "index":active["index"], "seal":seal_ref, "archive":archive["path"],
             "archive_binding":evidence_ref(Path(archive["path"]) / "archive-binding.json", "original complete reading-turn archive"),
             "business_verdict":"not_issued", "quality_accepted":False}
    state["serial_quality_reading"]["turns"].append(entry)
    state["serial_quality_reading"]["index"] += 1
    state["history"].append(entry)
    state["current_work_id"] = None
    for key in ("current_runtime", "current_node_id"):
        state.pop(key, None)
    return _save(store, record, state, "workflow_quality_reading_progress_received")


def validate_history(store, record):
    """Recheck completed originals before preparing or dispatching a successor."""
    previous = record["state"]["serial_quality_reading"]["turns"]
    from taskflow.execution_quality_continuation import _archive

    for index, entry in enumerate(previous):
        original = store.get_attempt(entry["attempt_id"])
        if (entry["index"] != index or original["state"] != "completed" or original["role"] != "quality"
                or original["cli_sid"] != entry["cli_sid"] or original["candidate_id"] != record["state"]["candidate_id"]
                or original["policy_id"] != record["definition"]["policy_id"]
                or Path(entry["seal"]["path"]).parent != Path(original["runtime_path"])
                or entry["archive"] not in record["state"]["archives"]):
            raise TaskflowError("serial quality reading: previous turn lost its original database/role binding")
        validate_evidence([entry["seal"], entry["archive_binding"]])
        observed = _archive(Path(original["runtime_path"]), Path(entry["archive"]), original)
        if observed["path"] != entry["archive_binding"]["path"] or observed["sha256"] != entry["archive_binding"]["sha256"]:
            raise TaskflowError("serial quality reading: original full archive changed")


def validate_dispatch(store, attempt, manifest):
    """The existing worker must still own the exact committed workflow cursor."""
    session = manifest.get("quality_session")
    if session is None:
        return
    record = store.get_flow(session["flow_id"])
    active = layout(record)
    if (active is None or record["state"]["status"] != "active"
            or record["state"]["current_work_id"] != attempt["work_id"]
            or record["state"]["current_runtime"] != attempt["runtime_path"]
            or active["index"] != session["index"] or active["phase"] != session["phase"]
            or active["reference"] != session["plan"]):
        raise TaskflowError("serial quality reading: worker lost its committed finite workflow turn")
    validate_history(store, record)
    previous = record["state"]["serial_quality_reading"]["turns"]
    expected = previous[-1]["seal"] if previous else None
    if session["previous"] != expected:
        raise TaskflowError("serial quality reading: previous turn differs from its workflow receipt")
