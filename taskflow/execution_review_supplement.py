"""Finite same-run trace evidence for an unchanged-score eligibility correction.

This is a review handoff policy, not a dispatcher or another evaluation engine.
"""

from datetime import datetime, timezone
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, parse_json, sha256, validate_id
from taskflow.execution_runner import _info, _regular, _validate, _validated_collection
from taskflow.records import verify_evaluation
from taskflow.execution_eligibility_evidence import (
    EXECUTION_MEMBERS as EXECUTION_MEMBERS, reference_files, retained_sources,
)


KIND = "same_run_eligibility_correction_v1"
PREFIX = "supplemental-trace/"



def _read(path: Path) -> dict:
    value = parse_json(_regular(path), str(path))
    if not isinstance(value, dict):
        raise TaskflowError("eligibility supplement requires JSON objects")
    return value


def _reference(value: dict, expected: Path | None = None, *, allow_location: bool = False) -> Path:
    fields = {"path", "sha256", "size"} | ({"location"} if allow_location else set())
    if (not isinstance(value, dict) or set(value) - fields
            or not {"path", "sha256", "size"} <= set(value)
            or not isinstance(value["path"], str) or not Path(value["path"]).is_absolute()
            or type(value["size"]) is not int):
        raise TaskflowError("eligibility supplement needs explicit path/SHA/size references")
    path = Path(value["path"])
    if expected is not None and path != expected:
        raise TaskflowError("eligibility supplement source is outside its exact retained identity")
    if _info(path) != {key: value[key] for key in ("sha256", "size")}:
        raise TaskflowError("eligibility supplement source bytes changed")
    return path


def _ref(path: Path) -> dict:
    return {"path": str(path), **_info(path)}


def reference_comparison(*, run: dict, parent_manifest: dict,
                         parent_root: Path, parent_score: dict, observed_at: str) -> dict:
    """Describe a current byte comparison; never imply a historical read witness."""
    files = reference_files(snapshot=Path(run["snapshot_path"]), inputs=parent_manifest["inputs"],
        input_root=parent_root / "inputs", scoring_source_allowlist=parent_manifest["scoring_source_allowlist"])
    return {"schema_version": 1, "kind": "current_retained_reference_comparison_v1",
            "observation_scope": "current_readback_not_historical_independent_witness",
            "observed_at": observed_at, "run_digest": run["run_digest"],
            "snapshot_digest": run["snapshot_digest"], "parent_score": parent_score,
            "files": files}


def validate_supplement(*, reference: dict, run: dict, run_directory: Path,
                        snapshot: dict, facts: dict, facts_root: Path,
                        candidate_binding: dict) -> dict:
    """Validate the finite manifest and originals before copying any extra input."""
    path = _reference(reference, allow_location=True)
    document = _read(path)
    fields = {"schema_version", "kind", "run_digest", "snapshot_digest", "candidate_binding",
              "facts_seal_digest", "reviewer_sid", "parent_evaluation_id", "parent_score_attempt",
              "parent_score_binding", "parent_score", "original_profile", "members", "correction"}
    if (set(document) != fields or type(document["schema_version"]) is not int
            or document["schema_version"] != 1 or document["kind"] != KIND
            or document["run_digest"] != run["run_digest"]
            or document["snapshot_digest"] != snapshot["snapshot_digest"]
            or document["candidate_binding"] != candidate_binding
            or document["facts_seal_digest"] != sha256(canonical(facts))
            or document["reviewer_sid"] != facts["sid"]):
        raise TaskflowError("eligibility supplement differs from its original run/facts/SID/frozen binding")
    parent_id = validate_id(document["parent_evaluation_id"], "parent_evaluation_id")
    correction = document["correction"]
    if not isinstance(correction, dict) or set(correction) != {"evaluation_id", "work_id", "intent_id", "runtime"}:
        raise TaskflowError("eligibility correction requires one explicit new evaluation/work/intent/runtime")
    for key in ("evaluation_id", "work_id", "intent_id"):
        validate_id(correction[key], key)
    if (correction["evaluation_id"] == parent_id or not isinstance(correction["runtime"], str)
            or not Path(correction["runtime"]).is_absolute()):
        raise TaskflowError("eligibility correction must append a new evaluation and runtime")
    evaluation = run_directory / "evaluations" / parent_id
    parent = verify_evaluation(evaluation)
    parent_record = _read(evaluation / "record.original.json")
    if (parent["kind"] != "original" or parent["validation_issues"]
            or parent["run_digest"] != run["run_digest"]
            or parent["evaluation_digest"] != snapshot["evaluation_digest"]):
        raise TaskflowError("eligibility correction needs the same run's accepted original parent and rubric")
    parent_root = Path(document["parent_score_attempt"])
    if not parent_root.is_absolute():
        raise TaskflowError("eligibility correction requires an absolute original scoring runtime")
    parent_manifest = _validate(parent_root)
    if (Path(correction["runtime"]) == parent_root or correction["intent_id"] == parent_manifest["intent_id"]):
        raise TaskflowError("eligibility correction cannot reuse or overwrite the parent's runtime/intent")
    collection = _validated_collection(parent_root, parent_manifest)
    binding_path = _reference(document["parent_score_binding"])
    # The original collector locked this binding as an explicit dependency.
    if parent_manifest["dependencies"].get(str(binding_path), {}).get("sha256") != document["parent_score_binding"]["sha256"]:
        raise TaskflowError("parent scoring binding is not its original locked dependency")
    binding = _read(binding_path)
    if (parent_manifest["role"] != "review-score" or collection.get("technical_accepted") is not True
            or collection.get("sid") != facts["sid"] or parent_manifest.get("resume_sid") != facts["sid"]
            or parent_manifest.get("facts_seal") != facts
            or Path(parent_manifest["facts_attempt"]) != facts_root
            or parent_manifest["candidate_binding"] != candidate_binding
            or binding["run_digest"] != run["run_digest"] or binding.get("facts_seal") != facts):
        raise TaskflowError("eligibility supplement parent does not retain the same actual reviewer/facts/run")
    score_path = _reference(document["parent_score"])
    captured = [name for name, row in collection["artifacts"].items()
                if parent_root / "artifacts" / name == score_path
                and row == {key: document["parent_score"][key] for key in ("sha256", "size")}]
    score = _read(score_path)
    if (len(captured) != 1 or parent_record.get("source_sha256") != document["parent_score"]["sha256"]
            or parent_record.get("signed_original_score") != score):
        raise TaskflowError("eligibility supplement score is not the parent's exact collected original")
    profile = binding.get("local_profile_definition", {})
    profile_path = _reference(document["original_profile"], parent_root / "inputs/local-profile-definition.json")
    if _info(profile_path) != {key: profile.get(key) for key in ("sha256", "size")}:
        raise TaskflowError("eligibility supplement must retain the exact original local profile")
    allowed = {item["artifact_id"]: Path(item["source"]["path"])
               for item in retained_sources(run, run_directory)}
    members = document["members"]
    if not isinstance(members, list) or not members or len(members) > 32:
        raise TaskflowError("eligibility supplement requires a finite nonempty member list")
    inputs = [{"path": str(path), "kind": "trace", "target": PREFIX + "manifest.json"},
              {"path": str(score_path), "kind": "trace", "target": PREFIX + "parent-score.json"}]
    targets = {item["target"] for item in inputs}
    originals = set()
    for member in members:
        if not isinstance(member, dict) or set(member) != {"id", "origin", "source", "artifact_id"}:
            raise TaskflowError("eligibility supplement members have only identity/origin/source fields")
        name = validate_id(member["id"], "supplement member id")
        origin = member["origin"]
        if origin == "run_artifact":
            expected_path = allowed.get(member["artifact_id"])
            if expected_path is None:
                raise TaskflowError("supplement is not an allowed original execution artifact of this run")
        elif origin == "frozen_manifest":
            expected_path = Path(run["snapshot_path"]) / "manifest.json"
        elif origin == "current_reference_comparison":
            expected_path = None
            declared = member["source"].get("path") if isinstance(member["source"], dict) else None
            if not isinstance(declared, str) or Path(declared).parent != path.parent:
                raise TaskflowError("current comparison must be an explicit member beside its finite manifest")
        else:
            raise TaskflowError("eligibility supplement origin is unsupported; no extra references/rules/credentials")
        source = _reference(member["source"], expected_path)
        if source.suffix not in {".json", ".jsonl", ".txt", ".md"}:
            raise TaskflowError("eligibility supplement is retained trace/data, not executable/reference code")
        target = PREFIX + "member-" + name + source.suffix
        if target in targets or str(source) in originals:
            raise TaskflowError("eligibility supplement has duplicate members")
        targets.add(target)
        originals.add(str(source))
        if origin == "frozen_manifest":
            if member["artifact_id"] is not None:
                raise TaskflowError("supplement frozen record must be the exact original snapshot manifest")
        elif origin == "current_reference_comparison":
            report = _read(source)
            observed = report.get("observed_at")
            instant = datetime.fromisoformat(observed.replace("Z", "+00:00")) if isinstance(observed, str) else None
            end = datetime.fromisoformat(collection["termination"]["ended_at"].replace("Z", "+00:00"))
            if (member["artifact_id"] is not None or instant is None or instant.tzinfo is None
                    or not end <= instant <= datetime.now(timezone.utc)):
                raise TaskflowError("supplement comparison must be explicitly observed after the original score")
            expected = reference_comparison(run=run,
                parent_manifest=parent_manifest, parent_root=parent_root,
                parent_score=document["parent_score"], observed_at=observed)
            if report != expected:
                raise TaskflowError("supplement comparison differs from exact retained/frozen/captured members")
        inputs.append({"path": str(source), "kind": "trace", "target": target})
    return {"reference": reference, "document": document, "inputs": inputs,
            "parent_score": score, "parent_manifest": parent_manifest,
            "parent_binding": binding, "original_profile": _ref(profile_path)}


def validate_output(score: dict, supplement: dict) -> None:
    """Eligibility-only corrections preserve every other signed field verbatim."""
    parent = supplement["parent_score"]
    if (not isinstance(score, dict) or set(score) != set(parent)
            or canonical({key: value for key, value in score.items() if key != "experiment_validity"})
            != canonical({key: value for key, value in parent.items() if key != "experiment_validity"})):
        raise TaskflowError("eligibility correction changed original score/rubric/criteria/facts or another signed field")
    value = score.get("experiment_validity")
    mutable = {"local_eligible", "reason", "evidence"}
    original = parent.get("experiment_validity", {})
    if (not isinstance(value, dict) or set(value) != set(original)
            or canonical({key: item for key, item in value.items() if key not in mutable})
            != canonical({key: item for key, item in original.items() if key not in mutable})):
        raise TaskflowError("eligibility correction may change only local_eligible, qualification reason and evidence")
    if (not isinstance(value, dict) or not isinstance(value.get("reason"), str) or not value["reason"].strip()
            or not value.get("evidence") or value.get("local_profile") != parent["experiment_validity"].get("local_profile")
            or value.get("local_eligible") not in (True, False, "unknown")
            or type(value.get("local_eligible")) not in (bool, str)):
        raise TaskflowError("eligibility correction requires an independently signed three-state local assessment")
