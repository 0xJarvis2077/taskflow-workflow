"""Transport sealed independent reviews without judging or changing their scores."""

from datetime import datetime, timezone
import copy
from functools import wraps
import json
from pathlib import Path
import re
import subprocess

from taskflow.contracts import (
    PUBLIC_ROLES, TaskflowError, atomic_directory, canonical, parse_json,
    safe_read, sha256, validate_id,
)
from taskflow.execution_runner import (
    _full_tree, _info, _publish_bytes, _read, _regular, _trace, _validate,
    _validated_collection, _validated_facts_seal, seal_facts,
    _relative, RunnerError,
    archive_full_attempt,
)
from taskflow.execution_codex_provider import provider_references_equal
from taskflow.execution_store import evidence_ref, validate_evidence
from taskflow.packages import verify_snapshot
from taskflow.records import (
    _evaluation_issues, _evaluation_sequence, _import_evaluation_locked,
    verify_evaluation, verify_run,
)


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("missing timezone")
        return parsed
    except (AttributeError, TypeError, ValueError) as exc:
        raise TaskflowError("review requires timezone-aware source times") from exc


def _expect(path: Path, expected: dict) -> dict:
    observed = _info(path)
    if observed != {key: expected.get(key) for key in ("sha256", "size")}:
        raise TaskflowError("sealed review byte drift: " + str(path))
    return observed


def _publish(path: Path, value: dict) -> None:
    data = canonical(value) + b"\n"
    if path.exists():
        if _regular(path) != data:
            raise TaskflowError("review receiving identity already has different bytes: " + str(path))
        return
    _publish_bytes(path, data)


def _terminal(review: Path, stage: int) -> dict | None:
    path = review / f"stage{stage}-run/attempt.json"
    if not path.exists():
        return None
    attempt = _read(path)
    if not attempt.get("ended_at") and attempt.get("returncode") is None:
        return None
    if (attempt.get("stage") != stage or attempt.get("status") != "process_exited"
            or type(attempt.get("returncode")) is not int or attempt["returncode"] != 0
            or attempt.get("exceptions") != [] or attempt.get("input_readback_passed") is not True
            or attempt.get("output_snapshots", {}).get("errors") != []):
        raise TaskflowError(f"stage{stage} has no successful original terminal/input receipt")
    if _time(attempt["ended_at"]) < _time(attempt["started_at"]):
        raise TaskflowError("review terminal time precedes launch")
    return attempt


def _snapshot_output(review: Path, attempt: dict, source: Path, expected: dict) -> None:
    name = source.name
    rows = attempt["output_snapshots"].get("files", [])
    captured = [row for row in rows if row.get("path") == name]
    if len(captured) != 1 or captured[0].get("stable_during_read") is not True:
        raise TaskflowError("review output lacks its stable original process snapshot: " + name)
    _expect(source, expected)
    _expect(review / f"stage{attempt['stage']}-run/output-snapshots" / name, expected)
    if any(captured[0].get(key) != expected.get(key) for key in ("sha256", "size")):
        raise TaskflowError("review output snapshot does not match its first seal: " + name)


def _validate_legacy(config: dict, review: Path, run: dict, snapshot: dict) -> dict:
    plan = _read(review / "launch-plan.json")
    _expect(review / "launch-plan.json", _read(review / "launch-plan-lock.json"))
    snapshot_root = Path(config["snapshot_directory"]).absolute()
    cwd, runtime = Path(plan["cwd"]).absolute(), Path(plan["root"]).absolute()
    if (plan.get("task_id") != run["task_id"] or plan.get("version_id") != snapshot["version_id"]
            or plan.get("submission_id") != run["run_id"]
            or plan.get("snapshot_digest") != snapshot["snapshot_digest"]
            or run["snapshot_digest"] != snapshot["snapshot_digest"]
            or run["evaluation_digest"] != snapshot["evaluation_digest"]
            or run.get("actual_input_digest") != snapshot["input_digest"]
            or not cwd.is_relative_to(runtime)):
        raise TaskflowError("review/run/frozen public and scoring standards differ")
    _expect(snapshot_root / "manifest.json", plan["snapshot_manifest"])
    for item in [*plan.get("request_files", {}).values(), plan["neutral_rules"], plan["visible_manifest"]]:
        _expect(Path(item["path"]), item)
    declaration = _read(snapshot_root / "task.json")
    expected_files = {str(snapshot_root / row["path"]): _info(snapshot_root / row["path"])
                      for row in declaration["files"]}
    required_files = {str(snapshot_root / row["path"]) for row in declaration["files"]
                      if row["role"] in PUBLIC_ROLES | {"answer", "reference", "rubric"}}
    teacher_files = {}
    scoring_found = False
    for item in plan["stage2_teacher"]:
        source, target = Path(item["source"]).absolute(), Path(item["destination"]).absolute()
        if not target.is_relative_to(runtime / "teacher"):
            raise TaskflowError("frozen teacher destination is outside this review")
        _expect(target, item)
        if item.get("source_json_pointer") == "/scoring":
            if source != snapshot_root / "task.json":
                raise TaskflowError("teacher scoring projection comes from another package")
            _expect(source, item["source_info"])
            projected = _read(target)
            if projected not in (declaration["scoring"], {"scoring": declaration["scoring"]}):
                raise TaskflowError("teacher task scoring differs from frozen criteria")
            scoring_found = True
        elif str(source) in expected_files and _info(source) == expected_files[str(source)]:
            _expect(source, item)
            teacher_files[str(source)] = _info(source)
        else:
            raise TaskflowError("teacher source is not an exact declared frozen file")
    if not required_files.issubset(teacher_files) or not scoring_found:
        raise TaskflowError("review did not lock same-version public/reference/rubric files and scoring")
    for item in plan["sources"]:
        _expect(Path(item["destination"]), item)
        _expect(Path(item["source"]), item.get("source_info", item))
        source = Path(item["source"]).absolute()
        public_sources = {snapshot_root / row["path"] for row in declaration["files"] if row["role"] in PUBLIC_ROLES}
        if source.is_relative_to(snapshot_root) and source not in public_sources:
            raise TaskflowError("first factual review received a frozen teacher source")
    first, second = _terminal(review, 1), _terminal(review, 2)
    if first is None or second is None:
        raise TaskflowError("sealed score lacks both original terminal attempts")
    facts = _read(review / "stage1-facts-seal.json")
    seal = _read(review / "first-score-seal.json")
    sid = facts.get("thread_id")
    if (sid != config["expected_sid"] or seal.get("thread_id") != sid
            or seal.get("facts_sha256") != facts.get("sha256")
            or sid in config.get("excluded_sids", [])):
        raise TaskflowError("review SID is different, reused or not bound to its sealed facts")
    if config.get("expected_facts_sha256", facts["sha256"]) != facts["sha256"]:
        raise TaskflowError("facts differ from the controller's original handoff")
    if config.get("expected_score_sha256", seal["score"]["sha256"]) != seal["score"]["sha256"]:
        raise TaskflowError("score differs from the controller's sealed source binding")
    for stage in (1, 2):
        trace = _trace(review / f"stage{stage}-run/trace.jsonl", sid)
        if not trace["model_completed"]:
            raise TaskflowError(f"stage{stage} original trace is incomplete or its SID changed")
        actual = _read(review / f"stage{stage}-run/argv.json")["argv"]
        expected = plan["stage1_argv"] if stage == 1 else [
            sid if part == "<EXACT_NEW_STAGE1_SESSION_ID>" else part for part in plan["stage2_argv_template"]
        ]
        if actual != expected or actual.count("-m") != 1 or actual[actual.index("-m") + 1] != "gpt-6.1-sol":
            raise TaskflowError("review actual command differs from its locked independent model/SID")
        if (actual.count('model_reasoning_effort="xhigh"') != 1 or "--ephemeral" in actual
                or "fork" in actual or (stage == 1 and "resume" in actual)
                or (stage == 2 and (actual.count("resume") != 1 or sid not in actual))):
            raise TaskflowError("review facts creation or same-context scoring command differs")
        _expect(review / f"stage{stage}-run/request.md", plan["request_files"][str(stage)])
    if not (_time(first["ended_at"]) <= _time(facts["sealed_at"]) <= _time(second["started_at"])
            <= _time(second["ended_at"]) <= _time(seal["sealed_at"])):
        raise TaskflowError("facts were not sealed before original scoring began")
    facts_path = Path(facts["original_path"])
    if facts_path != cwd / "stage1-output/facts-review.md":
        raise TaskflowError("facts seal refers to another review workspace")
    _snapshot_output(review, first, facts_path, facts)
    for path in (review / "stage1-facts-review.md", cwd / "sealed/facts-review.md"):
        _expect(path, facts)
    if _read(cwd / "sealed/facts-seal.json") != facts:
        raise TaskflowError("visible facts seal changed")
    _expect(review / "stage1-run/trace.jsonl", facts["stage1_trace"])
    if facts["stage1_request"] != plan["request_files"]["1"]:
        raise TaskflowError("facts request binding changed")
    for key, name in (("score", "first-score.json"), ("explanation", "scoring-review.md")):
        path = Path(seal[key]["path"])
        if path != cwd / "stage2-output" / name:
            raise TaskflowError("first score seal refers to another review workspace")
        _snapshot_output(review, second, path, seal[key])
    return {"plan": plan, "facts": facts, "seal": seal, "runtime": runtime, "cwd": cwd}


def _archive(roots: dict[str, Path], destination: Path) -> dict:
    for source in roots.values():
        if source.is_relative_to(destination) or destination.is_relative_to(source):
            raise TaskflowError("review source and archive destination overlap")
    snapshot = {key: _full_tree(path) for key, path in roots.items()}
    binding = {"schema_version": 1, "sources": {key: str(path) for key, path in roots.items()}, "snapshot": snapshot,
               "scope": "All terminal ordinary files and empty directories; special entries indexed without following. Deleted transient bytes remain unobserved."}
    if destination.exists():
        if _read(destination / "manifest.json") != binding:
            raise TaskflowError("original review source membership or bytes changed after archival")
    else:
        if snapshot != {key: _full_tree(path) for key, path in roots.items()}:
            raise TaskflowError("review source changed during initial archive observation")
        with atomic_directory(destination) as stage:
            _publish(stage / "manifest.json", binding)
            for key, tree in snapshot.items():
                target = stage / "trees" / key
                target.mkdir(parents=True)
                for name in tree["directories"]:
                    (target / name).mkdir(parents=True, exist_ok=True)
                for name, expected in tree["files"].items():
                    data = _regular(roots[key] / name, limit=128 * 1024 * 1024)
                    if {"sha256": sha256(data), "size": len(data)} != {k: expected[k] for k in ("sha256", "size")}:
                        raise TaskflowError("review source changed while archiving: " + name)
                    output = target / name
                    _publish_bytes(output, data)
                    output.chmod(expected["mode"])
                for name, mode in tree["directories"].items():
                    (target / name).chmod(mode)
            if snapshot != {key: _full_tree(path) for key, path in roots.items()}:
                raise TaskflowError("review source changed before archive publication")
    for key, expected in snapshot.items():
        actual = _full_tree(destination / "trees" / key)
        if actual["files"] != expected["files"] or actual["directories"] != expected["directories"] or actual["special"]:
            raise TaskflowError("archived review bytes or directory membership changed")
    if set(path.name for path in destination.iterdir()) != {"manifest.json", "trees"}:
        raise TaskflowError("review archive has unexplained additional entries")
    if set(path.name for path in (destination / "trees").iterdir()) != set(roots):
        raise TaskflowError("review archive has an unexplained source tree")
    return {"path": str(destination), "sha256": sha256(canonical(binding)),
            "files": sum(len(tree["files"]) for tree in snapshot.values()),
            "special_entries": sum(len(tree["special"]) for tree in snapshot.values())}


def _normalized_locator(locator: str | dict) -> str:
    """Project a signed locator's syntax without changing its source object."""
    if isinstance(locator, str) and locator.strip():
        return locator
    if isinstance(locator, dict):
        kind = locator.get("type")
        if kind == "lines" and set(locator) == {"type", "start", "end"}:
            first, last = locator["start"], locator["end"]
            if type(first) is int and type(last) is int and 1 <= first <= last:
                return f"lines {first}-{last}"
        elif kind == "json_pointer" and set(locator) == {"type", "pointer"}:
            pointer = locator["pointer"]
            if (isinstance(pointer, str) and (pointer == "" or pointer.startswith("/"))
                    and re.search(r"~(?:[^01]|$)", pointer) is None):
                return "json_pointer:" + pointer
    raise TaskflowError("signed evidence locator has an invalid typed location")


def _locators(row: dict, score: dict) -> list[tuple[str, str | dict]]:
    found = []
    for key in ("public_evidence", "submission_evidence", "public_responsibility_evidence", "final_submission_evidence"):
        values = row.get(key, [])
        if not isinstance(values, list):
            raise TaskflowError("signed criterion evidence must be a list")
        for value in values:
            if isinstance(value, dict):
                path, locator = value.get("path"), value.get("locator")
            elif isinstance(value, str) and ":" in value:
                path, locator = value.rsplit(":", 1)
                path = score.get("evidence_aliases", {}).get(path, path)
            else:
                raise TaskflowError("signed evidence locator has no explicit path/location")
            if not isinstance(path, str) or not path or not isinstance(locator, (str, dict)) or not locator:
                raise TaskflowError("signed evidence path/location is missing")
            _normalized_locator(locator)
            found.append((path, locator))
    return found


def _project(score: dict, context: dict, config: dict, run: dict, snapshot: dict) -> tuple[dict, list[dict]]:
    supplement = context.get("eligibility_correction")
    if supplement is not None:
        from taskflow.execution_review_supplement import validate_output

        validate_output(score, supplement)
    criteria = score.get("criteria", score.get("items"))
    if not isinstance(criteria, list):
        raise TaskflowError("signed first score has no criterion list")
    expected = {row["id"]: row for row in snapshot["scoring"]["criteria"]}
    if len(criteria) != len(expected) or {row.get("id") for row in criteria} != set(expected):
        raise TaskflowError("signed score criterion IDs differ from the frozen rubric")
    if score.get("max_score") != snapshot["scoring"]["total"]:
        raise TaskflowError("signed score maximum differs from the frozen rubric")
    teacher_sources = context["plan"]["stage2_teacher"]
    if supplement is not None:
        parent_root = Path(supplement["document"]["parent_score_attempt"])
        teacher_sources = [{**item, "destination": str(parent_root / "inputs" / item["target"])}
                           for item in supplement["parent_manifest"]["inputs"] if item["kind"] == "teacher"]
    for source in score.get("frozen_sources", []):
        matching = [item for item in teacher_sources if item["destination"] == source.get("path")]
        if len(matching) != 1:
            raise TaskflowError("signed frozen source is not part of the locked standards")
        _expect(Path(source["path"]), source)
        _expect(Path(source["path"]), matching[0])
    items, bindings = [], []
    for row in criteria:
        if row.get("max_score") != expected[row["id"]]["max_score"]:
            raise TaskflowError("signed criterion weight differs from frozen rubric: " + row["id"])
        if not isinstance(row.get("reason"), str) or not row["reason"].strip():
            raise TaskflowError("signed original criterion reason is missing")
        refs = []
        for name, signed_locator in _locators(row, score):
            locator = _normalized_locator(signed_locator)
            source = Path(context["cwd"]) / name
            declared = [item for item in context["plan"]["sources"] if item["destination"] == str(source)]
            if len(declared) != 1:
                raise TaskflowError("signed evidence was not a locked first-stage source: " + name)
            info = _expect(source, declared[0])
            matches = [item for item in run["artifacts"] if all(item[key] == info[key] for key in ("sha256", "size"))]
            mapped = config.get("source_path_map", {}).get(name)
            if declared[0].get("artifact_id"):
                matches = [item for item in matches if item["id"] == declared[0]["artifact_id"]]
            elif mapped:
                matches = [item for item in matches if item["source_path"] == mapped]
            else:
                origin = declared[0]["source"]
                direct = [item for item in matches if origin.endswith("/" + item["source_path"])]
                hints = {name, name.removeprefix("submission/"), "work/" + name}
                matches = direct or [item for item in matches if item["source_path"] in hints]
            if len(matches) != 1:
                raise TaskflowError("signed source has no unique same-byte native run artifact: " + name)
            artifact = matches[0]
            raw = safe_read(Path(config["run_directory"]), artifact["path"])
            try:
                line_count = len(raw.decode("utf-8").splitlines())
            except UnicodeError:
                line_count = None
            line_spec = re.fullmatch(r"[0-9,，、–-]+", locator)
            chinese = re.search(r"第([0-9,，、–-]+)行", locator)
            if line_spec or chinese:
                numbers = [int(value) for value in re.findall(r"\d+", chinese.group(1) if chinese else locator)]
                if line_count is None or not numbers or min(numbers) < 1 or max(numbers) > line_count:
                    raise TaskflowError("signed locator is outside the actual source lines: " + name + ":" + locator)
            location = {}
            if isinstance(signed_locator, dict):
                if {"sha256": sha256(raw), "size": len(raw)} != info:
                    raise TaskflowError("signed native source changed during locator validation: " + name)
                location = _review_locator(source, signed_locator, raw=raw)
            native = artifact["id"] + "#" + locator
            refs.append(native)
            bindings.append({"signed_path": name, "signed_locator": signed_locator,
                             "artifact_id": artifact["id"], "source_path": artifact["source_path"],
                             **info, "line_count": line_count, "native_evidence": [native],
                             **({"projected_locator": locator, "location_validation": location}
                                if isinstance(signed_locator, dict) else {})})
        items.append({"id": row["id"], "score": row["score"], "reason": row["reason"],
                      "evidence": list(dict.fromkeys(refs)), "signed_original_item": row})
    record = {"schema_version": 1, "evaluation_id": config["evaluation_id"], "kind": "original",
              "input_digest": run["actual_input_digest"], "rubric_snapshot": config["snapshot_directory"],
              "review_status": "reviewed", "evaluator": config.get("evaluator_description", "Independent gpt-6.1-sol/xhigh same-SID two-stage signed original " + config["expected_sid"]),
              "items": items, "total": score.get("total_score"), "local_result_only": True,
              "signed_original_score": score, "source_sha256": context["seal"]["score"]["sha256"]}
    if supplement is not None:
        record.update(kind="correction", parent_evaluation_id=supplement["document"]["parent_evaluation_id"],
                      correction_scope="eligibility_only_unchanged_score",
                      supplemental_trace_manifest=supplement["reference"])
    issues = _evaluation_issues(record, snapshot, run)
    if issues:
        raise TaskflowError("original score transport validation: " + "; ".join(issues))
    return record, bindings


def ingest_signed_evaluation(*, config: dict, destination: Path) -> dict:
    """Receive a terminal legacy two-stage score once, preserving its judgment."""
    if isinstance(config, dict) and config.get("mode") == "managed_two_stage":
        return _ingest_managed(config=config, destination=destination)
    if config.get("eligibility_correction") is not None:
        raise TaskflowError("eligibility correction requires the managed same-SID score handoff")
    if not isinstance(config, dict) or config.get("schema_version", 1) != 1:
        raise TaskflowError("signed evaluation config schema is invalid")
    for key in ("review_directory", "run_directory", "snapshot_directory", "evaluation_id", "expected_sid"):
        if not isinstance(config.get(key), str) or not config[key]:
            raise TaskflowError("signed evaluation config missing: " + key)
    if any(not Path(config[key]).is_absolute() for key in ("review_directory", "run_directory", "snapshot_directory")):
        raise TaskflowError("signed evaluation source directories must be absolute")
    validate_id(config["evaluation_id"], "evaluation_id")
    validate_evidence(config.get("evidence", []))
    review, destination = Path(config["review_directory"]).absolute(), Path(destination).absolute()
    if _terminal(review, 2) is None:
        return {"accepted": False, "state": "waiting", "reason": "original_stage2_not_terminal"}
    if not (review / "first-score-seal.json").exists():
        return {"accepted": False, "state": "waiting", "reason": "original_first_score_not_sealed"}
    run = verify_run(Path(config["run_directory"]))
    snapshot = verify_snapshot(Path(config["snapshot_directory"]))
    context = _validate_legacy(config, review, run, snapshot)
    for item in run["artifacts"]:
        if item.get("kind") == "trace":
            solver = _trace(Path(config["run_directory"]) / item["path"])
            if solver.get("sid") == config["expected_sid"]:
                raise TaskflowError("independent review reused the solver context")
    score_path = Path(context["seal"]["score"]["path"])
    raw_score = _regular(score_path)
    if {"sha256": sha256(raw_score), "size": len(raw_score)} != {key: context["seal"]["score"][key] for key in ("sha256", "size")}:
        raise TaskflowError("sealed score changed while loading original judgment")
    score = parse_json(raw_score, str(score_path))
    record, bindings = _project(score, context, config, run, snapshot)
    archived = _archive({"launcher": review, "runtime": context["runtime"]}, destination / "full-archive")
    archive_root = Path(archived["path"])
    supporting = {}
    for key, source, expected in (
            ("facts", Path(context["facts"]["original_path"]), context["facts"]),
            ("first_score", score_path, context["seal"]["score"]),
            ("scoring_review", Path(context["seal"]["explanation"]["path"]), context["seal"]["explanation"])):
        target = archive_root / "trees/runtime" / source.relative_to(context["runtime"])
        _expect(target, expected)
        supporting[key] = {"path": str(target), **_info(target)}
    supporting["seal"] = {"path": str(archive_root / "trees/launcher/first-score-seal.json"),
                          **_info(archive_root / "trees/launcher/first-score-seal.json")}
    # The archive is the immutable receiving identity; validate the live source
    # once more before importing, so a changed writer cannot race the handoff.
    if _validate_legacy(config, review, run, snapshot) != context:
        raise TaskflowError("review source binding changed during original-score transport")
    record.update(supporting_review=supporting, review_archive=archived)
    identity = {"config": config, "run_digest": run["run_digest"], "snapshot_digest": snapshot["snapshot_digest"],
                "source_sha256": context["seal"]["score"]["sha256"], "archive": archived}
    _publish(destination / "binding.json", identity)
    _publish(destination / "evaluation-transport-verification.json", {
        "nature": "mechanical signed-original score transport; no regrading", "bindings": bindings,
        "all_signed_original_reasons_preserved": True, "root_business_rereview": False})
    _publish(destination / "evaluation.original.json", record)
    evaluation_path = Path(config["run_directory"]) / "evaluations" / config["evaluation_id"]
    # Use the native importer's own per-run sequence lock, including the unique
    # original check. Different evaluation IDs cannot race into two originals.
    with _evaluation_sequence(Path(config["run_directory"])):
        if evaluation_path.exists():
            evaluated = verify_evaluation(evaluation_path)
            if safe_read(evaluation_path, "record.original.json") != _regular(destination / "evaluation.original.json"):
                raise TaskflowError("fixed evaluation identity already contains a different original score transport")
        else:
            for path in (Path(config["run_directory"]) / "evaluations").glob("*/evaluation.json"):
                if verify_evaluation(path.parent)["kind"] == "original":
                    raise TaskflowError("another original judgment already exists; no second original will be created")
            evaluated = _import_evaluation_locked(Path(config["run_directory"]), destination / "evaluation.original.json")
    if (evaluated["validation_issues"] or evaluated["kind"] != "original"
            or evaluated["run_digest"] != run["run_digest"]
            or evaluated["evaluation_digest"] != snapshot["evaluation_digest"]):
        raise TaskflowError("native original evaluation did not pass exact-run/frozen-standard transport validation")
    result = {"accepted": True, "state": "accepted", "receipt": str(destination / "receipt.json"),
              "run": config["run_directory"], "run_digest": run["run_digest"],
              "evaluation": str(evaluation_path), "evaluation_id": config["evaluation_id"],
              "record_digest": evaluated["record_digest"], "source_sha256": context["seal"]["score"]["sha256"],
              "facts_sha256": context["facts"]["sha256"], "signer_sid": config["expected_sid"],
              "score": record["total"], "review_archive": archived,
              "nature": run["nature"], "model_validity": run["model_validity"],
              "run_classification": run["classification"], "local_result_only": True, "primary_rerating": False}
    _publish(destination / "receipt.json", result)
    return result


def _review_run(config: dict, store: object) -> tuple[dict, dict, dict]:
    run = verify_run(Path(config["run_directory"]))
    snapshot = verify_snapshot(Path(run["snapshot_path"]))
    candidate = store.get_candidate(config["work"]["candidate_id"])
    if (candidate["task_id"] != run["task_id"]
            or run.get("actual_input_digest") != snapshot["input_digest"]
            or any(candidate[key] != snapshot[key] for key in ("input_digest", "content_digest", "evaluation_digest"))):
        raise TaskflowError("factual/scoring handoff differs from the exact run and frozen standards")
    return run, snapshot, candidate


def _first_inputs(config: dict, run: dict) -> tuple[list[dict], dict]:
    from taskflow.execution_formal import first_review_target

    selector = Path(config["first_review_index"]).absolute()
    index = _read(selector)
    selectors = [item for item in run["artifacts"] if item.get("id") == "first_review_index"
                 and item.get("source_path") == "indexes/first_review_index.json"]
    if len(selectors) != 1:
        raise TaskflowError("native run lacks its immutable first-review selection")
    _expect(selector, selectors[0])
    root = selector.parent.parent
    inputs = []
    visible = {"public", "submission", "actual_request", "actual_trace",
               "visible_file_change_process_evidence", "derived_actual_final_agent_message",
               "submission_workspace_membership", "actual_configuration"}
    for row in index.get("allowed_direct_artifacts", []):
        if row.get("visibility") not in visible:
            raise TaskflowError("first-review selector contains a teacher/control or unknown source")
        source = root / _relative(row["incoming_path"])
        validate_id(row["id"], "first-review artifact ID")
        _expect(source, row)
        artifacts = [item for item in run["artifacts"] if item["id"] == row["id"]
                     and all(item[key] == row[key] for key in ("sha256", "size"))]
        if len(artifacts) != 1:
            raise TaskflowError("first-review selector is not bound to the actual imported run")
        kind = "trace" if row["visibility"] in ("actual_trace", "visible_file_change_process_evidence", "actual_configuration") else "submission"
        inputs.append({"path": str(source), "kind": kind,
                       "target": first_review_target(row)})
    if not inputs or not any(item["kind"] == "trace" for item in inputs):
        raise TaskflowError("first factual review needs the actual complete run selection and trace")
    supplied = config.get("first_review_inputs")
    if supplied is not None:
        observed = [{key: item[key] for key in ("path", "kind", "target")} for item in supplied]
        if observed != inputs:
            raise TaskflowError("first-review inputs differ from the sealed visible selector")
    return inputs, {"path": str(selector), **_info(selector)}


def _source_binding(root: Path, binding_path: Path) -> tuple[dict, dict]:
    manifest = _validate(root)
    binding = _read(binding_path)
    if manifest["dependencies"].get(str(binding_path)) != _info(binding_path):
        raise TaskflowError("review run/source handoff is not an immutable runner dependency")
    run = verify_run(Path(binding["run_directory"]))
    if (run["run_digest"] != binding["run_digest"]
            or manifest.get("candidate_binding") != binding["candidate_binding"]):
        raise TaskflowError("review facts belong to a different run/candidate")
    return manifest, binding


def _preparation_errors(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (RunnerError, OSError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, TaskflowError):
                raise
            raise TaskflowError("review preparation evidence: " + str(exc)) from exc
    return checked


@_preparation_errors
def seal_existing_facts(*, config: dict) -> dict:
    """Seal only actual collected stage-one facts, never launch another review."""
    root, binding_path = Path(config["facts_attempt"]).absolute(), Path(config["review_binding"]).absolute()
    manifest, binding = _source_binding(root, binding_path)
    collection = _validated_collection(root, manifest)
    if manifest["role"] != "review-facts" or manifest.get("resume_sid"):
        raise TaskflowError("facts must originate in a new independent factual-review context")
    sid = collection.get("sid")
    if not sid or sid in binding["excluded_sids"]:
        raise TaskflowError("facts context reused an author/solver/teacher-informed SID")
    expected = {(item["path"], item["kind"], _info(Path(item["path"]))["sha256"])
                for item in binding["first_review_inputs"]}
    expected.update((item["path"], "public", item["sha256"]) for item in binding["public_sources"])
    actual = {(item["source"], item["kind"], item["sha256"]) for item in manifest["inputs"]}
    if actual != expected:
        raise TaskflowError("facts inputs differ from the exact run's public/submission/trace handoff")
    path = root / "artifacts" / config.get("facts_file", "facts.json")
    if (root / "facts-seal.json").exists():
        seal = _validated_facts_seal(root)
        if Path(seal["path"]) != path:
            raise TaskflowError("existing facts seal identifies a different output")
    else:
        seal = seal_facts(root, path)
    return {"facts_attempt": str(root), "review_binding": str(binding_path),
            "run_digest": binding["run_digest"], "seal": seal, "sid": sid}


def _denied_probe(runtime: Path, forbidden: list[Path]) -> dict:
    manifest = _validate(runtime)
    native = manifest.get("native")
    if manifest["mode"] != "native_role" or not native:
        raise TaskflowError("clean review preparation requires the actual native sandbox")
    paths = sorted({str(path.absolute()) for path in forbidden})
    for path in paths:
        _info(Path(path))
    code = "import os,errno,json\nrows=[]\n"
    code += "for path in " + repr(paths) + ":\n"
    code += " try:\n  fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK);os.close(fd)\n  raise AssertionError('teacher/project source readable')\n"
    code += " except OSError as exc:\n  assert exc.errno in (errno.EPERM,errno.EACCES);rows.append(path)\n"
    code += "print(json.dumps(rows))\n"
    argv = [*native["sandbox_argv"], "--", native["python"], "-B", "-c", code]
    result = subprocess.run(argv, cwd=manifest["cwd"], capture_output=True, text=True,
                            timeout=30, check=False)
    if result.returncode != 0 or json.loads(result.stdout) != paths:
        raise TaskflowError("actual native first-review teacher/project read denial did not pass")
    proof = {"schema_version": 1, "manifest": _info(runtime / "manifest.json"),
             "forbidden_reads_denied": paths, "model_calls": 0,
             "returncode": result.returncode, "argv": argv}
    _publish(runtime / "review-boundary-probe.json", proof)
    return proof


def _dependency_read(path: Path, runner: dict, permissions: dict) -> bool:
    """Only named tool/library installations can extend the blind filesystem."""
    roots = [Path(value) for value in ("/System/Library", "/usr/lib", "/usr/bin", "/bin", "/sbin",
                                      "/usr/sbin", "/opt/homebrew/Cellar", "/Library/Developer")]
    python = Path(permissions["python"]).resolve()
    roots.append(python.parents[1])
    roots.extend(parent for parent in python.parents if parent.name == "dependencies")
    cli = Path(runner["cli"]).resolve()
    roots.append(cli)
    roots.extend(parent for parent in cli.parents if parent.suffix == ".app")
    if any(path == Path(name) or path.is_relative_to(Path(name)) for name in ("/private/tmp", "/tmp", "/private/var/folders")):
        return False
    return any(path == root or path.is_relative_to(root) for root in roots)


def _profile_definition(store: object, config: dict, run: dict, snapshot: dict,
                        candidate: dict, directory: Path) -> dict | None:
    policy = config.get("local_profile_policy")
    if policy is None:
        return None
    if (not isinstance(policy, dict) or type(policy.get("schema_version")) is not int
            or policy["schema_version"] != 1 or policy.get("scope") != "local_fixed_client_profile"
            or any(policy.get(key) is not True for key in (
                "allow_served_identity_unknown", "requires_frozen_input_and_reference_match",
                "requires_complete_capture", "requires_no_substantive_dispute"))
            or policy.get("official_provider_identity_claimed") is not False):
        raise TaskflowError("local profile definition requires the explicit bounded execution policy")
    authority = validate_evidence([policy["authorization"]])[0]
    if not any(item.get("path") == authority["path"] and item.get("sha256") == authority["sha256"]
               for item in validate_evidence(config["work"]["evidence"])):
        raise TaskflowError("local profile policy authority is not this work's registered packet")
    packet = _read(Path(authority["path"]))
    scoped = packet
    if isinstance(packet.get("items"), list):
        selected = [item for item in packet["items"] if isinstance(item, dict)
                    and item.get("task_id") == candidate["task_id"]
                    and run["run_id"] in {item.get("first_run_id"), item.get("conditional_one_confirmation_run_id")}]
        if len(selected) != 1:
            raise TaskflowError("local profile policy needs one exact authorized task/run item")
        scoped = {"task_id": selected[0]["task_id"],
                  "formal": {"first_run_id": selected[0].get("first_run_id"),
                             "model": packet.get("formal_baseline", {}).get("model_requested")},
                  "single_confirmation": {"run_id": selected[0].get("conditional_one_confirmation_run_id")}}
    permitted_runs = {scoped.get("formal", {}).get("first_run_id"), scoped.get("single_confirmation", {}).get("run_id")}
    if (scoped.get("task_id") != candidate["task_id"]
            or packet.get("controller") != config["work"].get("dispatch_owner")
            or run["run_id"] not in permitted_runs):
        raise TaskflowError("local profile policy task/run/controller differs from the registered authority")
    solver = store.get_policy(config["work"]["policy_id"])["config"]["roles"]["solver"]
    reasoning = {"effort": solver["reasoning_effort"], "context": solver.get("reasoning_context")}
    if (policy.get("required_model") != solver["model"]
            or scoped.get("formal", {}).get("model") != solver["model"]
            or policy.get("required_client_reasoning") != reasoning
            or (not all(isinstance(value, str) and value for value in reasoning.values())
                and not (solver.get("reasoning_selection") == "explicit" and reasoning["context"] is None))):
        raise TaskflowError("local profile policy differs from the registered target model/client configuration")
    actual = [item for item in run["artifacts"] if item["id"] == "actual_configuration"]
    if len(actual) != 1:
        raise TaskflowError("local profile policy needs this run's sealed actual-configuration evidence")
    document = {"schema_version": 1, "document_kind": "local-profile-definition",
        "scope": "local_fixed_client_profile",
        "execution_binding": {"run_digest": run["run_digest"], "snapshot_digest": snapshot["snapshot_digest"],
                              "input_digest": snapshot["input_digest"], "evaluation_digest": snapshot["evaluation_digest"]},
        "actual_configuration": {key: actual[0][key] for key in ("id", "sha256", "size")},
        "requirements": {"required_model": policy["required_model"], "required_client_reasoning": reasoning,
            "requires_frozen_input_and_reference_match": True, "requires_complete_capture": True,
            "requires_no_substantive_dispute": True, "allow_served_identity_unknown": True,
            "official_provider_identity_claimed": False},
        "judgment": {"local_profile": "current_registered_local_profile", "local_eligible_values": [True, False, "unknown"],
            "instruction": "Judge the measured local configuration, frozen input/reference binding, captured actual work and visible process evidence. Sign true only when these requirements are supported and no substantive dispute remains; use false for a demonstrated mismatch and unknown for an unresolved requirement, with actual reasons and locators.",
            "limits": "Served identity may remain unknown and must be disclosed. Local parser/probe observations are not official provider identity or a complete outbound production payload capture. Complete capture concerns the retained ordinary final work/scratch, declared delivery, visible trace/request and recorded file-change evidence; deleted transient bytes are not reconstructed. These execution rules add no task grading obligations."}}
    if solver.get("reasoning_selection") == "explicit":
        document["requirements"].update(reasoning_selection="explicit",
            required_request_reasoning_override=solver["reasoning_effort"])
        document["judgment"]["limits"] += (" This profile uses an explicit reasoning request. Check the actual "
            "requested model/effort, locked command and retained local capture; client-default resolution "
            "may remain unknown and is not inferred. Served identity remains separate.")
    document["judgment"]["artifact_completeness"] = "Completeness concerns the run and retained evidence collection, not whether every business delivery or answer is correct. Judge business correctness only under the unchanged task rubric."
    document["judgment"]["input_reference_match"] = "Match means that the supplied and captured input/reference bytes agree with the frozen version and are readable under the registered profile. The model choosing to read less, omit a business delivery or make a business error is a scoring fact, not by itself an invalid experiment. A real provision failure, permission mismatch or environment/capture break may separately support false or unknown with actual evidence."
    path = directory / "local-profile-definition.json"
    _publish(path, document)
    return {"path": str(path), "target": "local-profile-definition.json", **_info(path),
            "authority": authority, "policy_sha256": sha256(canonical(policy))}



LOCAL_EXECUTION_TARGET = "local-profile-execution-evidence.json"


def _validate_local_execution_evidence(reference: dict, *, manifest: dict, run: dict,
                                       run_directory: Path, snapshot: dict, facts: dict,
                                       source_manifest: dict | None = None) -> dict:
    from taskflow.execution_eligibility_evidence import first_score_evidence

    def _require(value, message):
        if not value:
            raise TaskflowError(message)
    _require(isinstance(reference, dict) and set(reference) == {"path", "sha256", "size", "target"}
             and reference["target"] == LOCAL_EXECUTION_TARGET and isinstance(reference["path"], str)
             and isinstance(reference["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", reference["sha256"])
             and type(reference["size"]) is int and reference["size"] >= 0,
             "local execution evidence reference differs")
    path = Path(reference["path"])
    _require(path.is_absolute() and type(reference["size"]) is int, "local execution evidence needs an absolute original and integer size")
    _require(run["snapshot_digest"] == snapshot["snapshot_digest"], "local execution evidence frozen version differs")
    _expect(path, reference)
    document = _read(path)
    current = document.get("current_reference_comparison")
    _require(isinstance(current, dict), "local execution evidence current readback object required")
    observed = current.get("observed_at")
    origin = source_manifest if source_manifest is not None else manifest
    _require(isinstance(observed, str) and _time(facts["at"]) <= _time(observed) <= _time(origin["created_at"]),
             "local execution evidence current-readback time differs")
    expected = first_score_evidence(run=run, run_directory=run_directory,
        snapshot=Path(run["snapshot_path"]), inputs=origin["inputs"], input_root=None,
        scoring_source_allowlist=origin["scoring_source_allowlist"], observed_at=observed)
    _require(canonical(document) == canonical(expected), "local execution observations differ from exact retained sources")
    entries = [item for item in manifest["inputs"]
               if item["source"] == str(path) or item["target"] == LOCAL_EXECUTION_TARGET]
    _require(len(entries) == 1 and entries[0]["source"] == str(path)
             and entries[0]["target"] == LOCAL_EXECUTION_TARGET and entries[0]["kind"] == "trace"
             and all(entries[0][k] == reference[k] for k in ("sha256", "size")),
             "local execution evidence input binding differs")
    return document


def _local_execution_trace(*, run: dict, config: dict, runner: dict, binding: dict,
                           clean: Path, runtime: Path, facts: dict) -> dict:
    from taskflow.execution_eligibility_evidence import first_score_evidence

    # Prepared legacy originals cannot acquire new input or a new request on reentry.
    old_binding = _read(runtime.parent / (runtime.name + "-review") / "review-binding.json") if (
        runtime.parent / (runtime.name + "-review") / "review-binding.json").exists() else {}
    previous = old_binding.get("local_execution_evidence")
    if runtime.exists() and previous is None:
        return binding
    path = clean / LOCAL_EXECUTION_TARGET
    retained = _read(path) if path.exists() else None
    current = retained.get("current_reference_comparison") if retained is not None else None
    if retained is not None and not isinstance(current, dict):
        raise TaskflowError("partial local execution evidence has no current-readback object")
    observed = current.get("observed_at") if current is not None else datetime.now(timezone.utc).isoformat()
    if not isinstance(observed, str) or not _time(facts["at"]) <= _time(observed) <= datetime.now(timezone.utc):
        raise TaskflowError("partial local execution evidence time differs from the original facts")
    sources = [{"source": item["path"], **item, **_info(Path(item["path"]))}
               for item in runner["inputs"] if item["kind"] == "teacher"]
    document = first_score_evidence(run=run, run_directory=Path(config["run_directory"]),
        snapshot=Path(run["snapshot_path"]), inputs=sources, input_root=None,
        scoring_source_allowlist=runner["scoring_source_allowlist"], observed_at=observed)
    if previous is not None:
        if previous.get("path") != str(path) or previous.get("target") != LOCAL_EXECUTION_TARGET:
            raise TaskflowError("prepared local execution evidence original path differs")
        _expect(path, previous)
    if retained is not None:
        if canonical(retained) != canonical(document):
            raise TaskflowError("prepared local execution observations changed on reentry")
    else:
        _publish(path, document)
    reference = {"path": str(path), **_info(path), "target": LOCAL_EXECUTION_TARGET}
    runner["inputs"].append({"path": str(path), "kind": "trace", "target": LOCAL_EXECUTION_TARGET})
    runner["dependencies"] = [*runner.get("dependencies", []),
        *[item["source"]["path"] for item in document["retained_execution"]],
        document["frozen_public_input"]["manifest"]["path"]]
    runner["request_sections"].append({"kind":"operation", "text":
        "Use local-profile-execution-evidence.json only for experiment_validity/artifact_status local-profile evidence. "
        "It contains bounded retained launch/input observations and an explicitly current selected-source/frozen comparison, "
        "not a historical independent witness or a production payload capture. The safe command view redacts opaque configuration; "
        "missing observations stay missing. Keep actual_configuration as the capture/model/reasoning source; do not infer served identity. "
        "Do not cite this qualification trace as a grading criterion or change task obligations. Sign true, false or unknown independently."})
    return {**binding, "local_execution_evidence": reference}

def _review_batch_pair(facts_manifest: dict, score: dict, *, manifest: bool = True) -> bool:
    """Keep both stages on their original confirmed reviewer snapshot."""
    from taskflow.execution_batch_binding import validate_batch_runner

    first_provider = facts_manifest.get("model", {}).get("native_provider_ref")
    second_provider = score.get("model", {}).get("native_provider_ref") if manifest else score.get("native_provider")
    if not provider_references_equal(first_provider, second_provider):
        raise TaskflowError("facts and score must retain the same actual provider/profile reference")
    first = validate_batch_runner(facts_manifest, manifest=True)
    second = validate_batch_runner(score, manifest=manifest)
    if first is None and second is None:
        return False
    keys = ("policy_id", "policy_digest", "policy_config", "batch_id", "task_id", "model_slot")
    if (first is None or second is None or facts_manifest.get("role") != "review-facts" or score.get("role") != "review-score"
            or first["model_slot"] != "reviewer" or second["model_slot"] != "reviewer"
            or canonical({key: first[key] for key in keys}) != canonical({key: second[key] for key in keys})):
        raise TaskflowError("review stages require the same original batch policy and reviewer slot")
    for key in ("model", "reasoning_effort", "reasoning_selection", "reasoning_context", "cli", "native_provider"):
        if first["setting"].get(key) != second["setting"].get(key):
            raise TaskflowError("review stages changed the original confirmed reviewer configuration")
    return True


def _prepare_review(*, store: object, config: dict, runtime: Path, role: str) -> dict:
    from taskflow.execution import enqueue_spec
    from taskflow.execution_batch_binding import make_batch_binding, validate_batch_runner

    config = copy.deepcopy(config)
    runtime = Path(runtime).absolute()
    work, runner = config["work"], config["runner"]
    if work["role"] != role or runner["role"] != role or runner.get("mode", "native_role") != "native_role":
        raise TaskflowError("review preparation needs the exact native factual/scoring role")
    if runner.get("inputs"):
        raise TaskflowError("review inputs come only from the exact sealed source handoff")
    if role != "review-score" and config.get("eligibility_correction") is not None:
        raise TaskflowError("supplemental eligibility evidence is only for an original reviewer's score correction")
    run, snapshot, candidate = _review_run(config, store)
    supplied_slots = {value for value in (work.get("model_slot"), runner.get("model_slot")) if value is not None}
    if len(supplied_slots) > 1:
        raise TaskflowError("review work and runner have different model slots")
    original_batch = make_batch_binding(store.get_policy(work["policy_id"]), role,
        next(iter(supplied_slots), None), candidate["task_id"])
    if original_batch is not None:
        if original_batch["model_slot"] != "reviewer":
            raise TaskflowError("facts and scoring require the original reviewer model slot")
        if runner.get("batch_binding") is not None and canonical(runner["batch_binding"]) != canonical(original_batch):
            raise TaskflowError("review runner has a different original batch binding")
        runner.update(model_slot="reviewer", batch_binding=original_batch)
        work["model_slot"] = "reviewer"
        expected_binding = {key: candidate[key] for key in (
            "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}
        if runner.get("candidate_binding") is not None and runner["candidate_binding"] != expected_binding:
            raise TaskflowError("review runner has a different candidate binding")
        runner["candidate_binding"] = expected_binding
        if role == "review-score":
            if runner.get("facts_attempt") is not None and runner["facts_attempt"] != config["facts_attempt"]:
                raise TaskflowError("review runner has a different original facts source")
            runner["facts_attempt"] = config["facts_attempt"]
        for key in ("reasoning_selection", "reasoning_context"):
            if key not in runner and key in original_batch["setting"]:
                runner[key] = original_batch["setting"][key]
        validate_batch_runner(runner)
    repository = Path(config["real_repository"]).resolve()
    base = runtime.parent / (runtime.name + "-review")
    clean, cwd, scratch = base / "public-root", base / "work", base / "scratch"
    for path in (clean, cwd, scratch):
        path.mkdir(parents=True, exist_ok=True)
    probe = clean / "boundary-probe.txt"
    if not probe.exists():
        _publish_bytes(probe, b"Neutral read-only review boundary probe.\n")
    permissions = copy.deepcopy(runner["native_permissions"])
    filesystem = {":root": "deny", ":minimal": "read", ":tmpdir": "deny", ":slash_tmp": "deny",
                  str(repository): "deny", str(clean): "read", str(cwd): "write", str(scratch): "write"}
    reads = config.get("runtime_read_paths")
    if reads is None:
        reads = [name for name, access in permissions.get("filesystem", {}).items()
                 if access == "read" and not name.startswith(":") and _dependency_read(Path(name).resolve(), runner, permissions)]
    for path in reads:
        resolved = Path(path).resolve()
        if (repository.is_relative_to(resolved) or resolved.is_relative_to(repository)
                or base.is_relative_to(resolved) or resolved.is_relative_to(base)
                or runtime.is_relative_to(resolved) or resolved.is_relative_to(runtime)
                or not _dependency_read(resolved, runner, permissions)):
            raise TaskflowError("runtime reads cannot expose the real project or review metadata")
        filesystem[str(resolved)] = "read"
    if runner.get("native_provider") is not None:
        from taskflow.execution_codex_provider import _checked

        provider = _read(_checked(runner["native_provider"]))
        filesystem["/private/tmp"] = "deny"
        filesystem[str(Path(provider["codex_home"]).resolve())] = "deny"
    permissions.update(repository=str(clean), scratch=str(scratch), filesystem=filesystem,
                       readonly_probe=str(probe), probe_sources=[str(probe)])
    runner.update(cwd=str(cwd), native_permissions=permissions)
    binding_path = base / "review-binding.json"
    if role == "review-score" and config.get("local_profile_policy") is not None and runtime.exists():
        if not binding_path.exists() or not _read(binding_path).get("local_profile_definition"):
            raise TaskflowError("an existing scoring runtime cannot acquire a new local-profile policy")
    excluded = set(config.get("excluded_sids", []))
    for item in run["artifacts"]:
        if item.get("kind") == "trace":
            sid = _trace(Path(config["run_directory"]) / item["path"]).get("sid")
            if sid:
                excluded.add(sid)
    cursor = candidate
    while cursor:
        excluded.update(cursor.get("external_author_sids", []))
        if cursor.get("author_attempt"):
            excluded.add(store.get_attempt(cursor["author_attempt"])["cli_sid"])
        cursor = store.get_candidate(cursor["parent_id"]) if cursor.get("parent_id") else None
    if role == "review-facts":
        inputs, selector = _first_inputs(config, run)
        binding = {"schema_version": 1, "run_directory": config["run_directory"], "run_digest": run["run_digest"],
                   "candidate_binding": {key: candidate[key] for key in (
                       "candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
                   "first_review_inputs": inputs, "selector": selector,
                   "public_sources": [item for item in candidate["files"] if item["role"] in PUBLIC_ROLES],
                   "excluded_sids": sorted(value for value in excluded if value)}
        runner["inputs"] = inputs
    else:
        sealed = seal_existing_facts(config={"facts_attempt": config["facts_attempt"],
                                             "review_binding": config["facts_binding"],
                                             "facts_file": config.get("facts_file", "facts.json")})
        facts_manifest, binding = _source_binding(Path(config["facts_attempt"]), Path(config["facts_binding"]))
        _review_batch_pair(facts_manifest, runner, manifest=False)
        if run["nature"] != "test_fixture" and facts_manifest["mode"] != "native_role":
            raise TaskflowError("a fixture facts context cannot become a real scoring continuation")
        if binding["run_digest"] != run["run_digest"] or binding["candidate_binding"]["candidate_id"] != candidate["candidate_id"]:
            raise TaskflowError("scoring cannot replace the facts stage's actual run/candidate")
        supplied = config["scoring_source_allowlist"]
        normative = [item for item in candidate["files"] if item["role"] in {"answer", "reference", "rubric"}]
        actual = set(supplied)
        allowed = {item["package_path"]: item for item in candidate["files"] if item["role"] not in PUBLIC_ROLES}
        certification = config.get("normative_certification")
        if certification is None:
            if not {item["package_path"] for item in normative}.issubset(actual) or not actual.issubset(allowed):
                raise TaskflowError("scoring allowlist must contain this candidate's normative answer/rubric sources")
        else:
            from taskflow.execution_formal import validate_normative_certification

            if not isinstance(certification, dict):
                raise TaskflowError("normative certification must be a typed source handoff")
            authority = validate_evidence([certification.get("authorization")])[0]
            if not any(all(item.get(key) == authority[key] for key in ("path", "sha256"))
                       for item in work.get("evidence", [])):
                raise TaskflowError("normative certification authority is outside this scoring work")
            certified = validate_normative_certification(candidate=candidate, certification=certification, store=store)
            if (not isinstance(supplied, list) or len(supplied) != len(actual)
                    or actual != {item["package_path"] for item in certified} or not actual.issubset(allowed)):
                raise TaskflowError("scoring allowlist differs from the exact independently certified normative sources")
        declarations = []
        for evidence in candidate["evidence"]:
            if Path(evidence["path"]).name == "task.json":
                raw = _regular(Path(evidence["path"]))
                source = parse_json(raw, evidence["path"])
                if (sha256(raw) == evidence["sha256"] and source.get("task_id") == candidate["task_id"]
                        and source.get("scoring") == snapshot["scoring"]):
                    declarations.append((evidence, source))
        if len(declarations) != 1:
            raise TaskflowError("scoring needs the candidate's unique actual declaration and frozen criteria")
        normative_root = base / "normative"
        normative_root.mkdir(exist_ok=True)
        projected = normative_root / "task-scoring.json"
        _publish(projected, declarations[0][1]["scoring"])
        projection = {"source": declarations[0][0]["path"], "source_sha256": declarations[0][0]["sha256"],
                      "path": str(projected), "target": "normative/task-scoring.json", "sha256": _info(projected)["sha256"]}
        if certification is not None:
            projection["normative_certification"] = certification
        runner.update(facts_attempt=config["facts_attempt"], resume_sid=sealed["sid"],
                      scoring_source_allowlist=sorted(actual), scoring_projection=projection)
        runner["inputs"] = [*binding["first_review_inputs"],
                            {"path": sealed["seal"]["path"], "kind": "submission", "target": "sealed/facts.json"},
                            *[{"path": allowed[path]["path"], "kind": "teacher", "target": "normative/" + path}
                              for path in sorted(actual)],
                            {"path": str(projected), "kind": "teacher", "target": projection["target"]}]
        binding = {**binding, "facts_seal": sealed["seal"], "scoring_source_allowlist": sorted(actual),
                   "scoring_sources": [allowed[path] for path in sorted(actual)], "scoring_projection": projection}
        definition = _profile_definition(store, config, run, snapshot, candidate, clean)
        if definition is not None:
            runner["inputs"].append({"path": definition["path"], "kind": "trace", "target": definition["target"]})
            binding["local_profile_definition"] = definition
            runner["request_sections"] = [*runner.get("request_sections", []), {"kind": "operation", "text":
                "Use local-profile-definition.json only for the explicit local execution eligibility judgment. It binds this actual run and frozen standard; preserve unknown when evidence is unresolved and do not turn these execution requirements into task grading criteria."}]
        if definition is not None and config.get("eligibility_correction") is None:
            binding = _local_execution_trace(run=run, config=config, runner=runner,
                binding=binding, clean=clean, runtime=runtime, facts=sealed["seal"])
        if config.get("eligibility_correction") is not None:
            from taskflow.execution_review_supplement import validate_supplement

            supplement = validate_supplement(reference=config["eligibility_correction"], run=run,
                run_directory=Path(config["run_directory"]), snapshot=snapshot, facts=sealed["seal"],
                facts_root=Path(config["facts_attempt"]).absolute(), candidate_binding=binding["candidate_binding"])
            _review_batch_pair(facts_manifest, supplement["parent_manifest"])
            parent_manifest = supplement["parent_manifest"]
            if (runner["model"] != parent_manifest["model"]["requested"]
                    or runner["reasoning_effort"] != parent_manifest["model"]["reasoning_effort"]
                    or runner["timeout_seconds"] != parent_manifest["timeout_seconds"]
                    or not provider_references_equal(runner.get("native_provider"), parent_manifest["model"].get("native_provider_ref"))):
                raise TaskflowError("eligibility correction must retain the original reviewer configuration")
            correction = supplement["document"]["correction"]
            if (correction["work_id"] != work["work_id"] or correction["intent_id"] != work["intent_id"]
                    or correction["runtime"] != str(runtime)
                    or definition is None or _info(Path(definition["path"])) !=
                    {key: supplement["original_profile"][key] for key in ("sha256", "size")}
                    or binding["scoring_source_allowlist"] != supplement["parent_binding"]["scoring_source_allowlist"]
                    or binding["scoring_sources"] != supplement["parent_binding"]["scoring_sources"]
                    or runner["scoring_projection"].get("normative_certification") !=
                    supplement["parent_manifest"]["scoring_projection"].get("normative_certification")):
                raise TaskflowError("eligibility correction cannot replace the original profile or normative selection")
            runner["inputs"].extend(supplement["inputs"])
            parent_local = supplement["parent_binding"].get("local_execution_evidence")
            if parent_local is not None:
                _validate_local_execution_evidence(parent_local, manifest=parent_manifest, run=run,
                    run_directory=Path(config["run_directory"]), snapshot=snapshot, facts=sealed["seal"])
                runner["inputs"].append({"path":parent_local["path"], "kind":"trace", "target":LOCAL_EXECUTION_TARGET})
                binding["local_execution_evidence"] = parent_local
            binding["eligibility_correction"] = config["eligibility_correction"]
            binding["supplemental_trace_inputs"] = supplement["inputs"]
            runner["request_sections"].append({"kind": "operation", "text":
                "This is one eligibility-only correction to the original parent named in supplemental-trace/manifest.json. "
                "Read supplemental-trace/parent-score.json and the unchanged local-profile-definition.json; "
                "interpret its original requirements and capture limits independently. Supplemental members are trace evidence, "
                "not new facts, references, rules or grading criteria. Distinguish retained original records from comparisons "
                "performed now; they do not create historical independent observations or complete outbound captures. "
                "Copy the parent's complete score JSON unchanged except experiment_validity. Preserve every criterion, reason, "
                "evidence, numeric score, total, artifact_status and all other fields exactly. Sign local_eligible true, false "
                "or unknown with typed evidence locators and remaining limits; within experiment_validity only local_eligible, "
                "reason and evidence may change. Preserve the original profile, scope and served-identity disclosure; "
                "no desired conclusion is supplied. "
                "Do not run a solver/provider probe or repeat this correction. The original score and facts remain sealed."})
    _publish(binding_path, binding)
    runner["dependencies"] = [*runner.get("dependencies", []), str(binding_path)]
    work["evidence"] = [*work.get("evidence", []), evidence_ref(binding_path, "immutable same-run review source handoff")]
    spec = {"work": work, "runner": runner}
    try:
        existing = store.get_work(work["work_id"])
    except TaskflowError as exc:
        if "unknown" not in str(exc):
            raise
        existing = None
    if existing is not None:
        manifest, actual_binding = _source_binding(runtime, binding_path)
        if (actual_binding != binding or existing["runtime_path"] != str(runtime)
                or any(existing[key] != work[key] for key in ("intent_id", "candidate_id", "role", "policy_id"))
                or existing["request_digest"] != manifest["request"]["sha256"]):
            raise TaskflowError("existing review work belongs to another immutable handoff")
        validate_evidence(existing["evidence"])
        proof = _read(runtime / "review-boundary-probe.json")
        if proof.get("manifest") != _info(runtime / "manifest.json") or proof.get("model_calls") != 0:
            raise TaskflowError("existing review lacks its same-profile zero-model read-boundary proof")
        return {"work": existing, "runtime": str(runtime), "review_binding": str(binding_path), "run_digest": run["run_digest"]}

    class Preparation:
        def __getattr__(self, name):
            return getattr(store, name)

        def enqueue(self, **arguments):
            self.arguments = arguments
            return arguments

    prepared = Preparation()
    enqueue_spec(prepared, spec, runtime, resume_prepared=runtime.exists(), probe_native=True)
    _publish(runtime / "review-binding.json", binding)
    declaration = _read(Path(run["snapshot_path"]) / "task.json")
    forbidden = [Path(run["snapshot_path"]) / item["path"] for item in declaration["files"] if item["role"] not in PUBLIC_ROLES]
    _denied_probe(runtime, [*forbidden, repository / "AGENTS.md", binding_path, runtime / "review-binding.json",
                            *[Path(path) for path in config.get("metadata_deny_paths", [])]])
    prepared.arguments["evidence"] = [*prepared.arguments["evidence"], evidence_ref(
        runtime / "review-boundary-probe.json", "actual same-profile project/teacher reads denied without model calls")]
    return {"work": store.enqueue(**prepared.arguments), "runtime": str(runtime),
            "review_binding": str(binding_path), "run_digest": run["run_digest"]}


@_preparation_errors
def prepare_facts_runner(*, store: object, config: dict, runtime: Path) -> dict:
    """Prepare/probe and enqueue one clean factual role; never start its worker."""
    return _prepare_review(store=store, config=config, runtime=runtime, role="review-facts")


@_preparation_errors
def prepare_score_runner(*, store: object, config: dict, runtime: Path) -> dict:
    """Prepare the same SID's exact-run scoring continuation after facts seal."""
    return _prepare_review(store=store, config=config, runtime=runtime, role="review-score")


def _json_pointer(document: object, locator: str) -> str:
    pointer = locator.removeprefix("json_pointer:")
    if pointer.startswith("#"):
        pointer = pointer[1:]
    if pointer == "" and locator in {"json_pointer:", "#"}:
        return pointer
    if not pointer.startswith("/") or re.search(r"~(?:[^01]|$)", pointer):
        raise TaskflowError("review alias requires an explicit valid JSON pointer")
    value = document
    try:
        for token in pointer[1:].split("/"):
            token = token.replace("~1", "/").replace("~0", "~")
            if isinstance(value, list):
                if re.fullmatch(r"0|[1-9][0-9]*", token) is None:
                    raise TaskflowError("review JSON pointer has an invalid array index")
                value = value[int(token)]
            elif isinstance(value, dict):
                value = value[token]
            else:
                raise TaskflowError("review JSON pointer traverses a scalar")
    except (KeyError, IndexError) as exc:
        raise TaskflowError("review JSON pointer does not exist in the bound source") from exc
    return pointer


def _review_locator(target: Path, locator: str | dict, *, pointers_only: bool = False,
                    raw: bytes | None = None) -> dict:
    typed = isinstance(locator, dict)
    locator = _normalized_locator(locator)
    if locator.startswith(("/", "#", "json_pointer:")):
        raw = _regular(target) if raw is None else raw
        # A typed locator denotes exactly one pointer, including semicolons in
        # keys. The envelope uses the existing strict JSON parser for any root.
        document = (parse_json(b'{"source":' + raw + b'}', str(target))["source"]
                    if typed else parse_json(raw, str(target)))
        pointers = ([_json_pointer(document, locator)] if typed else
                    [_json_pointer(document, item.strip()) for item in locator.split(";")])
        return {"json_pointer": pointers[0]} if len(pointers) == 1 else {"json_pointers": pointers}
    if not pointers_only and locator == "entire document":
        return {"locator_type": "entire_document"}
    if not pointers_only and re.fullmatch(r"lines? [1-9][0-9]*(?:-[1-9][0-9]*)?(?: and [1-9][0-9]*(?:-[1-9][0-9]*)?)*", locator):
        raw = _regular(target) if raw is None else raw
        try:
            count = len(raw.decode("utf-8").splitlines())
        except UnicodeError as exc:
            raise TaskflowError("review line locator requires a UTF-8 source") from exc
        ranges = []
        for item in re.sub(r"^lines? ", "", locator).split(" and "):
            start, _, end = item.partition("-")
            first, last = int(start), int(end or start)
            if not 1 <= first <= last <= count:
                raise TaskflowError("review line locator does not exist in the bound source")
            ranges.append([first, last])
        return {"line_ranges": ranges}
    raise TaskflowError("review source requires explicit JSON pointers, line ranges or the entire document")


def _review_explicit_locator(target: Path, locator: str | dict, *, expected: dict,
                             allow_facts: bool = False) -> tuple[dict, bytes] | None:
    """Transport only the explicit pointer/fact and JSONL selection grammars."""
    if not isinstance(locator, str) or not locator.startswith(("JSON Pointers ", "JSONL lines ", "all ")):
        return None
    if locator.startswith("all ") and "JSONL records" not in locator:
        return None
    raw = _regular(target)
    if {"sha256": sha256(raw), "size": len(raw)} != {key: expected[key] for key in ("sha256", "size")}:
        raise TaskflowError("review explicit source changed during locator validation")
    if locator.startswith("JSON Pointers "):
        parts = locator.removeprefix("JSON Pointers ").split("; facts ")
        pointers = parts[0].split(", ")
        if (len(parts) > 2 or len(set(pointers)) != len(pointers)
                or any(re.fullmatch(r"/[^\s,;]*", value) is None for value in pointers)):
            raise TaskflowError("review explicit JSON pointer list is malformed")
        document = parse_json(b'{"source":' + raw + b'}', str(target))["source"]
        location = {"locator_type": "explicit_json_pointers",
                    "json_pointers": [_json_pointer(document, value) for value in pointers]}
        if len(parts) == 2:
            ids = parts[1].split(", ")
            facts = document.get("facts") if isinstance(document, dict) else None
            if (not allow_facts or len(set(ids)) != len(ids)
                    or any(re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", value) is None for value in ids)
                    or not isinstance(facts, list) or not facts
                    or any(not isinstance(row, dict) or not isinstance(row.get("id"), str)
                           or not row["id"].strip() for row in facts)):
                raise TaskflowError("review fact IDs require an explicit sealed-facts list")
            actual = {row["id"]: index for index, row in enumerate(facts)}
            if len(actual) != len(facts) or not set(ids) <= set(actual):
                raise TaskflowError("review fact ID is absent or nonunique in sealed facts")
            location.update(fact_ids=ids, fact_pointers=[f"/facts/{actual[value]}" for value in ids])
        return location, raw
    selection = re.fullmatch(r"JSONL lines ([1-9][0-9]*(?:-[1-9][0-9]*)?(?:, [1-9][0-9]*(?:-[1-9][0-9]*)?)*)"
                             r"(?:, especially final captures at lines ([1-9][0-9]*(?:-[1-9][0-9]*)?))?", locator)
    complete = re.fullmatch(r"all ([1-9][0-9]*) JSONL records, including turn\.completed at line ([1-9][0-9]*)", locator)
    if selection is None and complete is None:
        raise TaskflowError("review explicit JSONL locator is malformed")
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise TaskflowError("review JSONL locator requires UTF-8 records") from exc
    ranges = []
    selected = []
    values = selection.group(1).split(", ") if selection else ["1-" + complete.group(1)]
    for value in values:
        first, _, last = value.partition("-")
        first, last = int(first), int(last or first)
        if not 1 <= first <= last <= len(lines):
            raise TaskflowError("review JSONL range is outside the bound records")
        ranges.append([first, last])
        selected.extend(range(first, last + 1))
    if len(set(selected)) != len(selected):
        raise TaskflowError("review JSONL selections overlap")
    records = {line: parse_json(lines[line - 1].encode("utf-8"), str(target) + f":{line}") for line in selected}
    location = {"locator_type": "explicit_jsonl_lines", "line_ranges": ranges}
    if selection and selection.group(2):
        first, _, last = selection.group(2).partition("-")
        first, last = int(first), int(last or first)
        if not first <= last or not set(range(first, last + 1)) <= set(selected):
            raise TaskflowError("review highlighted JSONL lines are outside the selection")
        location["highlighted_line_ranges"] = [[first, last]]
    if complete:
        event_line = int(complete.group(2))
        if (len(lines) != int(complete.group(1)) or event_line not in records
                or records[event_line].get("type") != "turn.completed"):
            raise TaskflowError("review complete JSONL count or turn.completed line differs")
        location.update(locator_type="entire_jsonl_records", jsonl_record_count=len(lines),
                        event_line=event_line, event_type="turn.completed")
    return location, raw


def _review_source_alias(path: str, locator: str | dict, context: dict, run: dict, snapshot: dict) -> dict | None:
    signed_locator = locator
    locator = _normalized_locator(locator)
    aliases = {"normative/task-scoring.json": "frozen_scoring_projection",
               "sealed/facts.json": "sealed_review_facts",
               "local-profile-definition.json": "registered_local_profile_definition"}
    manifest = context["manifest"]
    if path == LOCAL_EXECUTION_TARGET:
        reference = context.get("local_execution_evidence")
        if reference is None:
            raise TaskflowError("local execution evidence was not bound to this scoring handoff")
        entries = [item for item in manifest["inputs"] if item["target"] == path]
        if len(entries) != 1 or entries[0]["kind"] != "trace" or entries[0]["source"] != reference["path"]:
            raise TaskflowError("local execution evidence alias differs from its actual trace")
        target = context["cwd"] / path
        observed = _expect(target, reference)
        location = _review_locator(target, signed_locator, raw=_regular(target))
        return {"source_type":"local_execution_qualification_observations", "review_source":path,
                "path":str(target), "locator":locator, **location, **observed}
    supplement = context.get("eligibility_correction")
    if supplement is not None and path.startswith("supplemental-trace/"):
        entries = [item for item in manifest["inputs"] if item["target"] == path]
        expected = [item for item in supplement["inputs"] if item["target"] == path]
        if len(entries) != 1 or len(expected) != 1 or entries[0]["kind"] != "trace" or entries[0]["source"] != expected[0]["path"]:
            raise TaskflowError("supplemental citation is outside the exact correction manifest")
        target = context["cwd"] / path
        observed = _expect(target, entries[0])
        location = _review_locator(target, signed_locator, raw=_regular(target))
        return {"source_type": "same_run_supplemental_trace", "review_source": path,
                "path": str(target), "locator": locator, **location, **observed}
    package_path = path.removeprefix("normative/") if path.startswith("normative/") else None
    selected_normative = package_path in manifest.get("scoring_source_allowlist", [])
    if selected_normative:
        aliases[path] = "selected_frozen_normative"
    if path not in aliases:
        return None
    entries = [item for item in manifest["inputs"] if item["target"] == path]
    if len(entries) != 1:
        raise TaskflowError("review alias is absent or ambiguous in the actual scoring manifest")
    entry = entries[0]
    target = context["cwd"] / path
    observed = _expect(target, entry)
    explicit = _review_explicit_locator(target, signed_locator, expected=observed,
                                        allow_facts=path == "sealed/facts.json")
    if selected_normative:
        declared = [item for item in snapshot["files"] if item["path"] == package_path and item["role"] not in PUBLIC_ROLES]
        expected_source = Path(manifest["scoring_projection"]["source"]).parent / package_path
        if (len(declared) != 1 or entry["kind"] != "teacher" or entry["source"] != str(expected_source)
                or any(declared[0][key] != observed[key] for key in ("sha256", "size"))
                or any(manifest["candidate_binding"][key] != snapshot[key]
                       for key in ("task_id", "content_digest", "input_digest", "evaluation_digest"))):
            raise TaskflowError("review normative source is not selected from the same frozen version")
        _expect(expected_source, observed)
        location = _review_array_locator(target, signed_locator, expected=observed)
        if location is None:
            location = explicit[0] if explicit else _review_locator(target, signed_locator)
    elif path == "normative/task-scoring.json":
        document = parse_json(explicit[1] if explicit else _regular(target), str(target))
        projection = manifest["scoring_projection"]
        source = _regular(Path(projection["source"]))
        declaration = parse_json(source, projection["source"])
        if (entry["kind"] != "teacher" or entry["source"] != projection["path"]
                or entry["sha256"] != projection["sha256"] or projection["target"] != path
                or sha256(source) != projection["source_sha256"]
                or declaration["task_id"] != snapshot["task_id"]
                or document != snapshot["scoring"] or document != declaration["scoring"]):
            raise TaskflowError("review alias is not the exact frozen scoring projection")
    elif path == "sealed/facts.json":
        seal = _validated_facts_seal(Path(manifest["facts_attempt"]))
        if (seal != context["facts_seal"] or seal != manifest["facts_seal"]
                or manifest["resume_sid"] != seal["sid"] or seal["candidate_binding"] != manifest["candidate_binding"]
                or entry["kind"] != "submission" or entry["source"] != seal["path"]
                or observed != seal["facts"]):
            raise TaskflowError("review alias is not the actual same-SID sealed facts")
    elif path == "local-profile-definition.json":
        document = parse_json(explicit[1] if explicit else _regular(target), str(target))
        definition = context["local_profile_definition"]
        expected = {"run_digest": run["run_digest"], "snapshot_digest": snapshot["snapshot_digest"],
                    "input_digest": snapshot["input_digest"], "evaluation_digest": snapshot["evaluation_digest"]}
        if (entry["kind"] != "trace" or entry["source"] != definition["path"]
                or observed != {key: definition[key] for key in ("sha256", "size")}
                or document.get("document_kind") != "local-profile-definition"
                or document.get("scope") != "local_fixed_client_profile" or document.get("execution_binding") != expected):
            raise TaskflowError("review policy alias is not bound to this exact run and frozen version")
    if not selected_normative:
        location = explicit[0] if explicit else _review_locator(target, signed_locator, pointers_only=True)
    return {"source_type": aliases[path], "review_source": path, "path": str(target),
            "locator": locator, **location, **observed}


def _review_array_locator(target: Path, locator: str | dict, *, expected: dict) -> dict | None:
    """Resolve an explicit complete criterion count only after frozen binding."""
    match = re.fullmatch(r"all ([1-9][0-9]*) criteria", locator) if isinstance(locator, str) else None
    if match is None:
        return None
    raw = _regular(target)
    if {"sha256": sha256(raw), "size": len(raw)} != {key: expected[key] for key in ("sha256", "size")}:
        raise TaskflowError("review complete-criteria source changed during locator validation")
    document = parse_json(b'{"source":' + raw + b'}', str(target))["source"]
    if not isinstance(document, list) or str(len(document)) != match.group(1):
        raise TaskflowError("review complete-criteria locator differs from the actual JSON array")
    return {"json_pointer": "", "json_array_length": len(document), "locator_type": "entire_json_array"}


def _local_assessment(score: dict, context: dict, run: dict, snapshot: dict) -> dict:
    """Transport the existing signer's local verdict; absence stays unknown."""
    expected = {"run_digest": run["run_digest"], "snapshot_digest": snapshot["snapshot_digest"],
                "input_digest": snapshot["input_digest"], "evaluation_digest": snapshot["evaluation_digest"]}
    signed = score.get("execution_binding", {})
    reasons = []
    if not isinstance(signed, dict) or any(signed.get(key) != value for key, value in expected.items()):
        reasons.append("signed_local_assessment_has_no_exact_run_snapshot_input_evaluation_binding")
    validity, artifacts = score.get("experiment_validity"), score.get("artifact_status")
    native_evidence = []
    for name, value in (("experiment_validity", validity), ("artifact_status", artifacts)):
        if not isinstance(value, dict):
            reasons.append(name + "_not_signed")
            continue
        if not isinstance(value.get("reason"), str) or not value["reason"].strip() or not value.get("evidence"):
            reasons.append(name + "_reason_or_locators_missing")
            continue
        section_ids = []
        try:
            for path, signed_locator in _locators({"public_evidence": value["evidence"]}, score):
                locator = _normalized_locator(signed_locator)
                alias = _review_source_alias(path, signed_locator, context, run, snapshot)
                if alias is not None:
                    native_evidence.append({"section": name, **alias,
                        **({"signed_locator": signed_locator} if isinstance(signed_locator, dict) else {})})
                    continue
                target = str(context["cwd"] / path)
                sources = [item for item in context["plan"]["sources"] if item["destination"] == target]
                if len(sources) != 1:
                    raise TaskflowError("unbound signed local-assessment source")
                source = sources[0]
                info = _expect(Path(target), source)
                matches = [item for item in run["artifacts"] if item.get("id") == source.get("artifact_id")
                           and all(item[key] == info[key] for key in ("sha256", "size"))]
                if len(matches) != 1:
                    raise TaskflowError("local-assessment source is not this actual run's bound artifact")
                explicit = _review_explicit_locator(Path(target), signed_locator, expected=info)
                legacy_lines = locator.replace(", ", " and ") if isinstance(signed_locator, str) else ""
                if re.fullmatch(r"lines? [1-9][0-9]*(?:-[1-9][0-9]*)?(?: and [1-9][0-9]*(?:-[1-9][0-9]*)?)*", legacy_lines):
                    raw = _regular(Path(target))
                    if {"sha256": sha256(raw), "size": len(raw)} != info:
                        raise TaskflowError("review line source changed during locator validation")
                    _review_locator(Path(target), legacy_lines, raw=raw)
                location = (explicit[0] if explicit else _review_locator(Path(target), signed_locator)
                            if isinstance(signed_locator, dict)
                            or locator.startswith(("/", "#", "json_pointer:")) else {})
                native_evidence.append({"section": name, "artifact_id": matches[0]["id"],
                                        "locator": locator, **location, **info,
                    **({"signed_locator": signed_locator} if isinstance(signed_locator, dict) else {})})
                section_ids.append(matches[0]["id"])
            if name == "experiment_validity" and "actual_configuration" not in section_ids:
                reasons.append("signed_local_profile_has_no_actual_configuration_locator")
        except (TaskflowError, KeyError, TypeError, ValueError):
            reasons.append(name + "_locators_not_bound_to_actual_run")
    validity = validity if isinstance(validity, dict) else {}
    artifacts = artifacts if isinstance(artifacts, dict) else {}
    if validity.get("local_profile") != "current_registered_local_profile":
        reasons.append("signed_local_profile_scope_missing")
    eligible = validity.get("local_eligible", "unknown")
    if eligible is not True and eligible is not False and eligible != "unknown":
        reasons.append("signed_local_eligible_is_not_three_state")
        eligible = "unknown"
    disputes = validity.get("substantive_disputes")
    if not isinstance(disputes, list):
        reasons.append("signed_substantive_disputes_unresolved")
        disputes = ["unknown"]
    return {"signed_binding_valid": not reasons, "structural_reasons": reasons,
            "local_eligible": eligible, "artifacts_complete": artifacts.get("artifacts_complete"),
            "served_identity_unknown_disclosed": validity.get("served_identity_unknown_disclosed"),
            "substantive_disputes": disputes, "evidence": native_evidence,
            "authority": "original same-SID second-stage score; no program validity judgment"}


def _managed_projection_context(score_root: Path, manifest: dict, binding: dict,
                                facts: dict, seal: dict, rows: list[dict]) -> dict:
    selected = {str(Path(binding["selector"]["path"]).parent.parent / row["incoming_path"]): row for row in rows}
    sources, teacher = [], []
    for item in manifest["inputs"]:
        target = str(score_root / "inputs" / item["target"])
        if item["kind"] == "teacher":
            teacher.append({**item, "destination": target})
        elif item["source"] != facts["path"] and item["target"] != LOCAL_EXECUTION_TARGET:
            row = {"source": item["source"], "destination": target, "sha256": item["sha256"], "size": item["size"]}
            if item["source"] in selected:
                row["artifact_id"] = selected[item["source"]]["id"]
            elif item["kind"] == "public":
                public = [entry for entry in rows if entry["visibility"] == "public"
                          and entry["sha256"] == item["sha256"] and entry["size"] == item["size"]]
                if len(public) == 1:
                    row["artifact_id"] = public[0]["id"]
            sources.append(row)
    return {"cwd": score_root / "inputs", "seal": seal, "manifest": manifest, "facts_seal": facts,
            "local_profile_definition": binding.get("local_profile_definition"),
            "local_execution_evidence": binding.get("local_execution_evidence"),
            "plan": {"sources": sources, "stage2_teacher": teacher}}


def _validate_named_source_map(config: dict, score: dict, facts_manifest: dict,
                               score_manifest: dict, binding: dict, declaration: dict,
                               rows: list[dict], run: dict, transport: dict) -> None:
    """Check identity, not hash aliases, on an already validated intake's proof."""
    mapping = config.get("source_path_map")
    citations = [item for criterion in score.get("criteria", score.get("items", []))
                 for item in _locators(criterion, score)]
    names = {name for name, _ in citations}
    if (not isinstance(mapping, dict) or not mapping or not set(mapping).issubset(names)
            or any(not isinstance(value, str) or not value for value in mapping.values())):
        raise TaskflowError("intake source map has unknown or missing signed sources")
    public = {item["path"]: item for item in declaration["files"] if item["role"] in PUBLIC_ROLES}
    incoming = Path(binding["selector"]["path"]).parent.parent
    expected = []
    for name, locator in citations:
        inputs = [[item for item in manifest["inputs"] if item["target"] == name]
                  for manifest in (facts_manifest, score_manifest)]
        if (any(len(items) != 1 for items in inputs) or inputs[0] != inputs[1]
                or inputs[0][0]["kind"] not in {"public", "submission", "trace"}):
            raise TaskflowError("intake source is not the same locked first-stage source")
        item = inputs[0][0]
        if item["kind"] == "public":
            originals = [entry for entry in binding["public_sources"] if entry.get("package_path") == name]
            if (name not in public or len(originals) != 1 or originals[0]["path"] != item["source"]
                    or originals[0]["role"] != public[name]["role"] or originals[0]["sha256"] != item["sha256"]):
                raise TaskflowError("intake source is not an exact declared public package file")
            selected = [entry for entry in rows if entry["visibility"] == "public"
                        and entry["incoming_path"].endswith("/" + name)]
        else:
            if name in mapping:
                raise TaskflowError("intake source map cannot remap submission or trace scope")
            selected = [entry for entry in rows if str(incoming / entry["incoming_path"]) == item["source"]]
        if len(selected) != 1:
            raise TaskflowError("intake source has no unique named first-stage selector entry")
        selected = selected[0]
        artifacts = [entry for entry in run["artifacts"] if entry["source_path"] == selected["incoming_path"]]
        if (len(artifacts) != 1 or artifacts[0]["id"] != selected["id"]
                or any(artifacts[0][key] != selected[key] or selected[key] != item[key] for key in ("sha256", "size"))
                or (name in mapping and mapping[name] != selected["incoming_path"])):
            raise TaskflowError("intake source map lacks a unique same-name same-byte native original")
        artifact = artifacts[0]
        expected.append({"signed_path": name, "signed_locator": locator, "artifact_id": artifact["id"],
                         "source_path": artifact["source_path"], "sha256": item["sha256"], "size": item["size"],
                         "native_evidence": [artifact["id"] + "#" + _normalized_locator(locator)]})
    actual = transport.get("bindings", [])
    if (transport.get("all_signed_original_reasons_preserved") is not True
            or transport.get("root_business_rereview") is not False
            or not isinstance(actual, list) or any(not isinstance(row, dict) for row in actual)
            or len(actual) != len(expected)
            or any({key: observed.get(key) for key in wanted} != wanted for observed, wanted in zip(actual, expected))):
        raise TaskflowError("intake source map transport does not bind every original signed citation")


def _received_source_map_config(config: dict, received: dict, receipt: dict, destination: Path) -> dict:
    """Retain a map only for the exact ordinary re-entry of a received intake."""
    if (config != {key: value for key, value in received.items() if key != "source_path_map"}
            or receipt.get("accepted") is not True or receipt.get("state") != "accepted"
            or receipt.get("receipt") != str(destination / "receipt.json")
            or receipt.get("run") != config["run_directory"]
            or receipt.get("evaluation_id") != config["evaluation_id"]):
        raise TaskflowError("received source-map intake config differs from ordinary stage re-entry")
    return received


def _managed_projection_config(config: dict, sid: str, manifest: dict, run: dict) -> dict:
    return {**config, "expected_sid": sid, "evaluator_description": (
        "Independent " + str(manifest["model"]["requested"]) + "/" + str(manifest["model"]["reasoning_effort"])
        + (" same-SID eligibility-only correction " if config.get("eligibility_correction") else " same-SID two-stage original ")
        + sid + "; nature=" + run["nature"])}


@_preparation_errors
def _ingest_managed(*, config: dict, destination: Path) -> dict:
    """Use the managed collector's originals instead of a legacy launcher schema."""
    validate_evidence(config["evidence"])
    destination = Path(destination).absolute()
    # Ordinary stage re-entry supplies its original config. Retain only the
    # explicit map of an already received intake, then revalidate it normally.
    if "source_path_map" not in config and (destination / "receipt.json").exists():
        received = _read(destination / "binding.json")["config"]
        if received.get("source_path_map"):
            config = _received_source_map_config(config, received, _read(destination / "receipt.json"), destination)
    facts_root, score_root = Path(config["facts_attempt"]).absolute(), Path(config["score_attempt"]).absolute()
    if not (score_root / "collection-lock.json").exists():
        return {"accepted": False, "state": "waiting", "reason": "original_core_score_not_collected"}
    facts_manifest, binding = _source_binding(facts_root, Path(config["facts_binding"]).absolute())
    facts = seal_existing_facts(config={"facts_attempt": str(facts_root), "review_binding": config["facts_binding"],
                                       "facts_file": config.get("facts_file", "facts.json")})["seal"]
    manifest, score_binding = _source_binding(score_root, Path(config["score_binding"]).absolute())
    collection = _validated_collection(score_root, manifest)
    run = verify_run(Path(config["run_directory"]))
    snapshot = verify_snapshot(Path(config["snapshot_directory"]))
    supplement = None
    reference = config.get("eligibility_correction")
    if reference != score_binding.get("eligibility_correction"):
        raise TaskflowError("managed eligibility correction differs from its locked supplemental handoff")
    if reference is not None:
        from taskflow.execution_review_supplement import validate_supplement

        supplement = validate_supplement(reference=reference, run=run,
            run_directory=Path(config["run_directory"]), snapshot=snapshot, facts=facts,
            facts_root=facts_root, candidate_binding=manifest["candidate_binding"])
        _review_batch_pair(facts_manifest, supplement["parent_manifest"])
        correction = supplement["document"]["correction"]
        if (correction["evaluation_id"] != config["evaluation_id"]
                or correction["runtime"] != str(score_root) or correction["intent_id"] != manifest["intent_id"]
                or score_root == Path(supplement["document"]["parent_score_attempt"])
                or config["evaluation_id"] == supplement["document"]["parent_evaluation_id"]
                or score_binding.get("supplemental_trace_inputs") != supplement["inputs"]
                or score_binding.get("scoring_sources") != supplement["parent_binding"]["scoring_sources"]
                or score_binding.get("scoring_source_allowlist") != supplement["parent_binding"]["scoring_source_allowlist"]
                or score_binding.get("local_profile_definition", {}).get("sha256") != supplement["original_profile"]["sha256"]
                or manifest["scoring_projection"].get("normative_certification") !=
                supplement["parent_manifest"]["scoring_projection"].get("normative_certification")):
            raise TaskflowError("eligibility correction cannot overwrite parent or change profile/normative sources")
    models = [(item.get("model", {}).get("requested"), item.get("model", {}).get("reasoning_effort"))
              for item in (facts_manifest, manifest)]
    if models[0] != models[1]:
        raise TaskflowError("two-stage original review changed its requested model/reasoning configuration")
    batch_review = _review_batch_pair(facts_manifest, manifest)
    if run["nature"] != "test_fixture" and any(
            item.get("mode") != "native_role" or item.get("nature") != "real_cli_attempt"
            or (not batch_review and (item.get("model", {}).get("requested") != "gpt-6.1-sol"
                                      or item.get("model", {}).get("reasoning_effort") != "xhigh"))
            for item in (facts_manifest, manifest)):
        raise TaskflowError("observed run requires actual native real gpt-6.1-sol/xhigh review originals; fixtures cannot sign it")
    if (manifest["role"] != "review-score" or not collection.get("technical_accepted")
            or manifest.get("facts_seal") != facts or Path(manifest["facts_attempt"]) != facts_root
            or manifest.get("resume_sid") != facts["sid"] or collection.get("sid") != facts["sid"]
            or binding["run_digest"] != run["run_digest"]
            or score_binding["run_digest"] != binding["run_digest"]
            or score_binding.get("first_review_inputs") != binding["first_review_inputs"]
            or score_binding.get("facts_seal") != facts
            or score_binding.get("scoring_projection") != manifest.get("scoring_projection")
            or score_binding.get("scoring_source_allowlist") != manifest.get("scoring_source_allowlist")
            or manifest["candidate_binding"] != facts_manifest["candidate_binding"]
            or any(manifest["candidate_binding"][key] != snapshot[key] for key in (
                "task_id", "input_digest", "content_digest", "evaluation_digest"))
            or run["snapshot_digest"] != snapshot["snapshot_digest"]
            or run["actual_input_digest"] != snapshot["input_digest"]):
        raise TaskflowError("managed score/facts SID, actual run or frozen standards differ")
    expected_inputs = {(item["path"], item["kind"], _info(Path(item["path"]))["sha256"])
                       for item in binding["first_review_inputs"]}
    expected_inputs.update((item["path"], "public", item["sha256"]) for item in binding["public_sources"])
    expected_inputs.add((facts["path"], "submission", facts["facts"]["sha256"]))
    expected_inputs.update((item["path"], "teacher", item["sha256"]) for item in score_binding["scoring_sources"])
    expected_inputs.add((score_binding["scoring_projection"]["path"], "teacher", score_binding["scoring_projection"]["sha256"]))
    if score_binding.get("local_profile_definition"):
        definition = score_binding["local_profile_definition"]
        expected_inputs.add((definition["path"], "trace", definition["sha256"]))
    local_execution = score_binding.get("local_execution_evidence")
    if local_execution is not None:
        origin = None
        if supplement is not None:
            if local_execution != supplement["parent_binding"].get("local_execution_evidence"):
                raise TaskflowError("eligibility correction cannot replace original local execution evidence")
            origin = supplement["parent_manifest"]
        _validate_local_execution_evidence(local_execution, manifest=manifest, run=run,
            run_directory=Path(config["run_directory"]), snapshot=snapshot, facts=facts,
            source_manifest=origin)
        expected_inputs.add((local_execution["path"], "trace", local_execution["sha256"]))
    elif any(item["target"] == LOCAL_EXECUTION_TARGET for item in manifest["inputs"]):
        raise TaskflowError("undeclared local execution evidence input")
    if supplement is not None:
        expected_inputs.update((item["path"], "trace", _info(Path(item["path"]))["sha256"])
                              for item in supplement["inputs"])
        expected_extra = {(item["path"], "trace", item["target"]) for item in supplement["inputs"]}
        extra_sources = {item["path"] for item in supplement["inputs"]}
        actual_extra = [(item["source"], item["kind"], item["target"]) for item in manifest["inputs"]
                        if item["source"] in extra_sources or item["target"].startswith("supplemental-trace/")]
        if len(actual_extra) != len(expected_extra) or set(actual_extra) != expected_extra:
            raise TaskflowError("managed supplemental input namespace differs from its independent manifest")
    if {(item["source"], item["kind"], item["sha256"]) for item in manifest["inputs"]} != expected_inputs:
        raise TaskflowError("managed scoring inputs differ from the exact facts/run/normative handoff")
    # Reconstruct provenance from the actual frozen declaration and bytes, not
    # the caller's self-described expected scoring_sources.
    projection = manifest.get("scoring_projection")
    if not isinstance(projection, dict):
        raise TaskflowError("managed scoring needs its exact normative scoring projection")
    declaration_path = Path(projection["source"])
    frozen_root = Path(config["snapshot_directory"])
    declaration_raw = _regular(declaration_path)
    if declaration_raw != _regular(frozen_root / "task.json") or sha256(declaration_raw) != projection["source_sha256"]:
        raise TaskflowError("managed scoring declaration differs from the actual frozen candidate")
    declaration = parse_json(declaration_raw, str(declaration_path))
    true_candidate = dict(manifest["candidate_binding"])
    true_candidate["evidence"] = [{"path": str(declaration_path), "sha256": sha256(declaration_raw),
                                   "location": "explicit package declaration"}]
    true_candidate["files"] = []
    for item in declaration["files"]:
        source = declaration_path.parent / item["path"]
        expected_file = _info(frozen_root / item["path"])
        _expect(source, expected_file)
        true_candidate["files"].append({"path": str(source), "package_path": item["path"], "role": item["role"], **expected_file})
    from taskflow.execution import _check_work_binding

    _check_work_binding({"intent_id": manifest["intent_id"], "role": "review-score", "policy_id": manifest["policy_version"]},
                        true_candidate, manifest)
    if config.get("expected_sid", facts["sid"]) != facts["sid"]:
        raise TaskflowError("managed scoring SID differs from the registered facts handoff")
    if not (_time(facts["at"]) <= _time(manifest["created_at"]) <= _time(collection["termination"]["started_at"])):
        raise TaskflowError("managed facts seal did not precede scoring preparation and launch")
    score_name, explanation_name = config.get("score_file", "first-score.json"), config.get("explanation_file", "scoring-review.md")
    score_path, explanation_path = score_root / "artifacts" / _relative(score_name), score_root / "artifacts" / _relative(explanation_name)
    for name, path in ((score_name, score_path), (explanation_name, explanation_path)):
        if name not in collection["artifacts"]:
            raise TaskflowError("managed score lacks its actual collected original: " + name)
        _expect(path, collection["artifacts"][name])
    seal = {"nature": "first_independent_score_before_author_comparison",
            "sealed_at": collection["termination"]["ended_at"], "thread_id": facts["sid"],
            "facts_sha256": facts["facts"]["sha256"],
            "score": {"path": str(score_path), **_info(score_path)},
            "explanation": {"path": str(explanation_path), **_info(explanation_path)},
            "manifest": _info(score_root / "manifest.json"), "collection": _info(score_root / "collection.json")}
    if supplement is not None:
        seal["nature"] = "same_sid_eligibility_correction_after_original_score"
    _publish(score_root / "first-score-seal.json", seal)
    rows = _read(Path(binding["selector"]["path"]))["allowed_direct_artifacts"]
    _expect(Path(binding["selector"]["path"]), binding["selector"])
    context = _managed_projection_context(score_root, manifest,
        {**score_binding, "selector": binding["selector"]}, facts, seal, rows)
    if supplement is not None:
        context["eligibility_correction"] = supplement
    projected_config = _managed_projection_config(config, facts["sid"], manifest, run)
    raw = _regular(score_path)
    if {"sha256": sha256(raw), "size": len(raw)} != collection["artifacts"][score_name]:
        raise TaskflowError("managed original score changed while reading")
    score = parse_json(raw, str(score_path))
    record, locators = _project(score, context, projected_config, run, snapshot)
    if config.get("source_path_map"):
        _validate_named_source_map(config, score, facts_manifest, manifest, score_binding, declaration, rows, run,
            {"bindings": locators, "all_signed_original_reasons_preserved": True, "root_business_rereview": False})
    local_assessment = _local_assessment(score, context, run, snapshot)
    if supplement is not None:
        local_assessment["authority"] = "same-SID eligibility-only correction; no program validity judgment"
        if not local_assessment["signed_binding_valid"]:
            raise TaskflowError("eligibility correction has unbound qualification evidence: "
                                + ", ".join(local_assessment["structural_reasons"]))
    archive_root = destination / "full-archive"
    facts_archive = archive_full_attempt(facts_root, archive_root / "facts")
    score_archive = archive_full_attempt(score_root, archive_root / "score")
    for source, archived, expected in ((facts["path"], Path(facts_archive["path"]) / "artifacts" / facts["output_name"], facts["facts"]),
                                       (str(score_path), Path(score_archive["path"]) / "artifacts" / score_name, seal["score"]),
                                       (str(explanation_path), Path(score_archive["path"]) / "artifacts" / explanation_name, seal["explanation"])):
        _expect(Path(source), expected)
        _expect(archived, expected)
    archives = {"path": str(archive_root), "facts": facts_archive, "score": score_archive}
    record.update(supporting_review={"facts": {"path": str(Path(facts_archive["path"]) / "artifacts" / facts["output_name"]), **facts["facts"]},
                                    "first_score": {"path": str(Path(score_archive["path"]) / "artifacts" / score_name), **_info(score_path)},
                                    "scoring_review": {"path": str(Path(score_archive["path"]) / "artifacts" / explanation_name), **_info(explanation_path)},
                                    "seal": {"path": str(Path(score_archive["path"]) / "first-score-seal.json"), **_info(score_root / "first-score-seal.json")}},
                  review_archive=archives)
    _publish(destination / "binding.json", {"config": config, "run_digest": run["run_digest"],
                                            "score": seal["score"], "facts": facts, "archives": archives})
    _publish(destination / "evaluation-transport-verification.json", {"bindings": locators,
               "all_signed_original_reasons_preserved": True, "root_business_rereview": False})
    _publish(destination / "evaluation.original.json", record)
    evaluation_path = Path(config["run_directory"]) / "evaluations" / config["evaluation_id"]
    with _evaluation_sequence(Path(config["run_directory"])):
        if evaluation_path.exists():
            evaluation = verify_evaluation(evaluation_path)
            if safe_read(evaluation_path, "record.original.json") != _regular(destination / "evaluation.original.json"):
                raise TaskflowError("fixed evaluation identity already contains different managed score transport")
        else:
            for path in (Path(config["run_directory"]) / "evaluations").glob("*/evaluation.json"):
                if record["kind"] == "original" and verify_evaluation(path.parent)["kind"] == "original":
                    raise TaskflowError("another original judgment already exists")
            evaluation = _import_evaluation_locked(Path(config["run_directory"]), destination / "evaluation.original.json")
    if (evaluation["validation_issues"] or evaluation["kind"] != record["kind"]
            or evaluation["run_digest"] != run["run_digest"] or evaluation["evaluation_digest"] != snapshot["evaluation_digest"]):
        raise TaskflowError("managed original evaluation did not pass exact-run/frozen-standard validation")
    result = {"accepted": True, "state": "accepted", "receipt": str(destination / "receipt.json"),
              "run": config["run_directory"], "run_digest": run["run_digest"], "evaluation": str(evaluation_path),
              "evaluation_id": config["evaluation_id"], "record_digest": evaluation["record_digest"],
              "source_sha256": seal["score"]["sha256"], "facts_sha256": facts["facts"]["sha256"],
              "signer_sid": facts["sid"], "score": record["total"], "review_archive": archives,
              "nature": run["nature"], "model_validity": run["model_validity"], "run_classification": run["classification"],
              "local_profile_assessment": local_assessment,
              "local_result_only": True, "primary_rerating": False}
    if supplement is not None:
        result.update(kind="correction", parent_evaluation_id=record["parent_evaluation_id"],
                      correction_scope="eligibility_only_unchanged_score")
    _publish(destination / "receipt.json", result)
    return result
