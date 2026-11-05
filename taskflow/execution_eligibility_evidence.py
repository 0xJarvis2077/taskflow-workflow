"""Finite retained execution observations for the first local-profile assessment.

No model calls or eligibility decision. Unknown configuration values, environment,
stdout, headers and payloads are not projected. Reference comparisons are current
readbacks, distinct from retained execution observations.
"""
from pathlib import Path
import re

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, parse_json, sha256
from taskflow.execution_runner import _info, _regular

EXECUTION_MEMBERS = {
    "execution-plan.json", "real-run/execution-plan.json", "plan-lock.json",
    "source-lock.json", "launch-review.json", "execution-claim.json",
    "execution-attempt.json", "exec-context.json", "normal-tools-probe.json",
    "shell-probes.json", "post-run-input-readback.json", "real-run/trace.jsonl",
    "real-run/attempt.json", "real-run/request.md",
}
NORMAL_MEMBERS = {"execution-plan.json", "real-run/execution-plan.json", "plan-lock.json",
                  "execution-claim.json", "post-run-input-readback.json"}
DIGEST = re.compile(r"[0-9a-f]{64}")


def _read(path: Path) -> dict:
    result = parse_json(_regular(path), str(path))
    if not isinstance(result, dict):
        raise TaskflowError("retained execution evidence requires JSON objects")
    return result


def _ref(path: Path) -> dict:
    return {"path": str(path), **_info(path)}


def retained_sources(run: dict, run_directory: Path) -> list[dict]:
    """Reuse the correction's exact original artifact/inner-source whitelist."""
    artifacts = {row["id"]: row for row in run["artifacts"]}
    result = []
    if "source_index" not in artifacts or "directories" not in artifacts:
        return result
    for name in ("source_index", "directories"):
        relative = Path(artifacts[name]["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise TaskflowError("retained source metadata path is outside this run")
        if _info(run_directory / relative) != {key: artifacts[name][key] for key in ("sha256", "size")}:
            raise TaskflowError("retained source metadata bytes changed")
    index = _read(run_directory / artifacts["source_index"]["path"])
    aliases = _read(run_directory / artifacts["directories"]["path"])["aliases"]
    inner = aliases["inner"]
    prefix = "" if inner["relative"] == "." else inner["relative"].rstrip("/") + "/"
    allowed_names = {prefix + name for name in EXECUTION_MEMBERS}
    for row in index["files"]:
        if (row.get("root_id") == inner["source"]
                and row.get("source_path") in allowed_names):
            artifact = artifacts.get(row["id"])
            if artifact and all(artifact[key] == row[key] for key in ("sha256", "size")):
                relative = Path(artifact["path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise TaskflowError("retained execution artifact is outside this run")
                result.append({"artifact_id": row["id"], "source_path": row["source_path"][len(prefix):],
                               "source": {"path": str(run_directory / artifact["path"]),
                                          **{key: artifact[key] for key in ("sha256", "size")}}})
    return result


def reference_files(*, snapshot: Path, inputs: list[dict], input_root: Path | None,
                    scoring_source_allowlist: list[str]) -> list[dict]:
    """Exact current byte comparison; shared with the unchanged correction codec."""
    files = []
    for item in sorted(inputs, key=lambda row: row["target"]):
        if item["kind"] != "teacher":
            continue
        target = item["target"]
        source = Path(item["source"])
        captured = input_root / target if input_root is not None else source
        if _info(captured) != {key: item[key] for key in ("sha256", "size")}:
            raise TaskflowError("parent captured normative bytes changed")
        if target == "normative/task-scoring.json":
            frozen = snapshot / "task.json"
            matches = _read(captured) == _read(frozen)["scoring"] == _read(source)
            comparison = "scoring_json_projection"
        else:
            package_path = target.removeprefix("normative/")
            if not target.startswith("normative/") or package_path not in scoring_source_allowlist:
                raise TaskflowError("parent normative selection has an unexpected target")
            frozen = snapshot / package_path
            matches = _regular(source) == _regular(frozen) == _regular(captured)
            comparison = "exact_bytes"
        files.append({"target": target, "source": _ref(source), "frozen": _ref(frozen),
                      **({"captured": _ref(captured)} if input_root is not None else {}),
                      "comparison": comparison, "matches": matches})
    return files


def safe_argv(argv: object) -> dict:
    """A finite CLI view; never copy opaque configuration or a free-form payload."""
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
        return {"available": False}
    result = {"argv_sha256": sha256(canonical(argv)), "argument_count": len(argv)}
    if len(argv) > 256 or Path(argv[0]).name not in {"codex", "codex-cli"}:
        return {**result, "available": False}
    switches = {"--no-daemon", "--ignore-rules", "--ignore-user-config", "--ephemeral",
                "--skip-git-repo-check", "--json", "--full-auto",
                "--dangerously-bypass-approvals-and-sandbox", "--"}
    values = {"-m", "--model", "-a", "--ask-for-approval", "--color", "--sandbox", "-s",
              "--enable", "--disable"}
    paths = {"-C", "--cd", "-o", "--output-last-message", "--profile", "-p"}
    view, redacted = [Path(argv[0]).name], ["/argv/0"] if argv[0] != Path(argv[0]).name else []
    i = 1
    while i < len(argv):
        item = argv[i]
        if item in switches or item in {"exec", "e", "resume", "-"}:
            view.append(item)
            i += 1
            continue
        if item in values | paths | {"-c", "--config"} and i + 1 < len(argv):
            value = argv[i + 1]
            view.append(item)
            if item in values and re.fullmatch(r"[A-Za-z0-9_./:-]{1,128}", value) and not re.search(
                    r"(?i)(api.?key|secret|password|bearer|token|sk-[A-Za-z0-9])", value):
                view.append(value)
            elif item in {"-c", "--config"} and re.fullmatch(
                    r'model_reasoning_effort=(?:"(?:none|minimal|low|medium|high|xhigh|ultra)"|none|minimal|low|medium|high|xhigh|ultra)', value):
                view.append(value)
            else:
                key = value.partition("=")[0] if item in {"-c", "--config"} else ""
                view.append(key + "=<retained value not projected>" if re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", key) else "<retained value not projected>")
                redacted.append("/argv/" + str(i + 1))
            i += 2
            continue
        return {**result, "available": False}
    return {**result, "available": True, "safe_argv_view": view, "redacted_json_pointers": redacted,
            "complete_argv_disclosed": not redacted}


def retained_observations(run: dict, run_directory: Path) -> list[dict]:
    observations = []
    for item in retained_sources(run, run_directory):
        name = item["source_path"]
        if name not in NORMAL_MEMBERS:
            continue
        source = Path(item["source"]["path"])
        if _info(source) != {key: item["source"][key] for key in ("sha256", "size")}:
            raise TaskflowError("retained execution source bytes changed")
        document = _read(source)
        fields, pointers = {}, []
        if name.endswith("execution-plan.json"):
            fields["locked_command"] = safe_argv(document.get("argv"))
            pointers.append("/argv")
            for key in ("input_digest", "public_tree_digest", "request_sha256", "baseline_plan_sha256", "source_lock_sha256"):
                if isinstance(document.get(key), str) and DIGEST.fullmatch(document[key]):
                    fields[key] = document[key]
                    pointers.append("/" + key)
            frozen = document.get("frozen_binding")
            if isinstance(frozen, dict):
                fields["frozen_binding"] = {key: value for key, value in frozen.items()
                    if key in {"snapshot_digest", "snapshot_manifest_sha256", "taskflow_input_digest"}
                    and isinstance(value, str) and DIGEST.fullmatch(value)}
                pointers.append("/frozen_binding")
        elif name == "post-run-input-readback.json":
            if type(document.get("matches")) is bool:
                fields["matches"] = document["matches"]
                pointers.append("/matches")
            if isinstance(document.get("errors"), list):
                fields["error_count"] = len(document["errors"])
                pointers.append("/errors")
            files = document.get("files")
            if isinstance(files, dict):
                fields["files"] = {path: {key: value for key, value in row.items()
                    if (key == "sha256" and isinstance(value, str) and DIGEST.fullmatch(value))
                    or (key == "size" and type(value) is int and value >= 0)}
                    for path, row in files.items() if isinstance(row, dict)}
                pointers.append("/files")
        else:
            for key in ("sha256", "plan_sha256", "launcher_source_sha256"):
                if isinstance(document.get(key), str) and DIGEST.fullmatch(document[key]):
                    fields[key] = document[key]
                    pointers.append("/" + key)
        observations.append({"artifact_id": item["artifact_id"], "source_path": name,
            "source": item["source"], "source_json_pointers": pointers, "fields": fields})
    return observations


def first_score_evidence(*, run: dict, run_directory: Path, snapshot: Path,
                         inputs: list[dict], input_root: Path | None,
                         scoring_source_allowlist: list[str], observed_at: str) -> dict:
    return {"schema_version": 1, "kind": "first_score_local_execution_evidence_v1",
            "run_digest": run["run_digest"], "snapshot_digest": run["snapshot_digest"],
            "scope": "Qualification trace only; not task criteria, production payload, served identity or an eligibility verdict.",
            "retained_execution": retained_observations(run, run_directory),
            "frozen_public_input": {"manifest": _ref(snapshot / "manifest.json"),
                "files": [{key: row[key] for key in ("path", "role", "sha256", "size")}
                          for row in _read(snapshot / "manifest.json")["files"] if row["role"] in PUBLIC_ROLES]},
            "current_reference_comparison": {"observed_at": observed_at,
                "observation_scope": "current_selected_source_readback_before_score_capture_not_historical_witness",
                "files": reference_files(snapshot=snapshot, inputs=inputs, input_root=input_root,
                                         scoring_source_allowlist=scoring_source_allowlist)}}
