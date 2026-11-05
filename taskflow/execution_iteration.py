"""Validate coordinator intake in the existing next-investment record.

These are completeness and provenance checks. Business choices remain explicit;
neither a score nor a lawful method selects a successor in Python.
"""

import copy

from taskflow.contracts import TaskflowError


PURPOSE_KINDS = frozenset({
    "quality_repair", "mechanism_diagnosis", "business_reconstruction", "formal_experiment",
})
NEXT_ACTIONS = frozenset({"research", "repair", "reconstruct", "experiment", "retain", "environment"})


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError(label + " requires a concrete nonempty explanation")
    return value


def validate_purpose_kind(value: object) -> str:
    """Require an explicit purpose category without deriving it from prose."""
    if not isinstance(value, str) or value not in PURPOSE_KINDS:
        raise TaskflowError("purpose_kind must explicitly identify repair, diagnosis, reconstruction or experiment")
    return value


def validate_intake(value: object, *, action: str) -> dict:
    """Keep quality, difficulty evidence and the explicit next choice distinct."""
    from taskflow.execution_store import validate_evidence

    if (not isinstance(value, dict)
            or set(value) != {"purpose_kind", "quality", "difficulty", "experiment"}):
        raise TaskflowError("next-investment intake requires purpose_kind, quality, difficulty and experiment")
    result = copy.deepcopy(value)
    validate_purpose_kind(result["purpose_kind"])
    quality = result["quality"]
    if (not isinstance(quality, dict) or set(quality) != {"conclusion", "evidence", "limitations"}
            or not isinstance(quality.get("conclusion"), str)
            or quality.get("conclusion") not in {"accepted", "revision_required", "inconclusive", "unknown"}):
        raise TaskflowError("intake quality must retain its actual conclusion, evidence and limitations")
    _text(quality["limitations"], "quality limitations")
    if not isinstance(quality["evidence"], list):
        raise TaskflowError("quality evidence must be an explicit list")
    if quality["evidence"]:
        quality["evidence"] = validate_evidence(quality["evidence"])
    elif quality["conclusion"] != "unknown":
        raise TaskflowError("a known quality conclusion requires original evidence")
    difficulty = result["difficulty"]
    if (not isinstance(difficulty, dict) or set(difficulty) != {"status", "conclusion", "evidence", "limitations"}
            or not isinstance(difficulty.get("status"), str)
            or difficulty["status"] not in {"unknown", "observed", "inferred"}):
        raise TaskflowError("intake difficulty must retain evidence and its practical limits separately")
    _text(difficulty["conclusion"], "difficulty conclusion")
    _text(difficulty["limitations"], "difficulty limitations")
    if not isinstance(difficulty["evidence"], list):
        raise TaskflowError("difficulty evidence must explicitly list originals; absence remains unknown")
    if difficulty["evidence"]:
        difficulty["evidence"] = validate_evidence(difficulty["evidence"])
    elif difficulty["status"] != "unknown":
        raise TaskflowError("difficulty without original evidence must remain explicitly unknown")
    experiment = result["experiment"]
    if action != "experiment":
        if experiment is not None:
            raise TaskflowError("non-experiment intake must not imply an authorized formal call")
        return result
    if result["purpose_kind"] != "formal_experiment" or quality["conclusion"] != "accepted":
        raise TaskflowError("formal experiment intake requires an explicit experiment purpose and accepted quality")
    if (not isinstance(experiment, dict) or set(experiment) != {"important_unknown", "outcomes"}
            or not isinstance(experiment["outcomes"], list) or len(experiment["outcomes"]) < 2):
        raise TaskflowError("formal call requires an important unknown and at least two consequential outcomes")
    _text(experiment["important_unknown"], "important unknown")
    observations, choices = set(), set()
    for row in experiment["outcomes"]:
        if not isinstance(row, dict) or set(row) != {"observation", "next_action", "reason"}:
            raise TaskflowError("each experiment outcome requires observation, next_action and reason")
        observations.add(_text(row["observation"], "outcome observation"))
        _text(row["reason"], "outcome consequence")
        if not isinstance(row["next_action"], str) or row["next_action"] not in NEXT_ACTIONS:
            raise TaskflowError("outcome next_action must use an existing next-investment action")
        choices.add((row["next_action"], row["reason"]))
    if len(observations) != len(experiment["outcomes"]) or len(choices) < 2:
        raise TaskflowError("different experiment results must have distinguishable subsequent choices")
    return result


def explicit_solver_decision(decision: dict) -> dict:
    """Reject quality-only dispatch at the current solver entry, before enqueue."""
    if decision.get("action") != "experiment" or decision.get("authorized") is not True:
        raise TaskflowError("formal solver requires the coordinator's exact authorized decision")
    return validate_intake(decision.get("intake"), action="experiment")
