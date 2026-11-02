"""Single-attempt CLI adapter; evidence files are immutable, not queue state.

The supervisor is copied at preparation and can run without importing the project.
No business judgement, score threshold, or automatic model retry lives here.
"""

import hashlib
import errno
import json
import os
import re
from pathlib import Path
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from typing import Callable


_DETACHED: dict[str, subprocess.Popen] = {}


class RunnerError(ValueError):
    """Unsafe handoff or inconsistent execution evidence."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(path: Path) -> dict:
    value = json.loads(_regular(path).decode("utf-8"))
    if not isinstance(value, dict):
        raise RunnerError(f"Expected JSON object: {path}")
    return value


def _publish_bytes(path: Path, data: bytes) -> None:
    """Publish once; interrupted temporary bytes never become an artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".capture-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _publish(path: Path, value: object) -> None:
    _publish_bytes(path, _bytes(value) + b"\n")


def _open_regular(path: Path) -> int:
    path = Path(path).absolute()
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:-1]:
            next_descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                      dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        # A FIFO without a writer must reach fstat instead of hanging in open.
        result = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=descriptor)
    except OSError as error:
        if error.errno in (errno.ELOOP, errno.ENOTDIR):
            raise RunnerError(f"Unsafe symlink or non-directory in evidence path: {path}") from error
        raise
    finally:
        os.close(descriptor)
    if not stat.S_ISREG(os.fstat(result).st_mode):
        os.close(result)
        raise RunnerError(f"Not a regular file: {path}")
    return result


def _regular(path: Path, limit: int = 16 * 1024 * 1024) -> bytes:
    with os.fdopen(_open_regular(path), "rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise RunnerError(f"Evidence file exceeds size limit: {path}")
    return data


def _relative(value: str) -> str:
    if (not isinstance(value, str) or not value or "\\" in value
            or Path(value).is_absolute()
            or any(part in ("", ".", "..") or part.startswith(".capture-")
                   for part in value.split("/"))):
        raise RunnerError(f"Unsafe relative file: {value!r}")
    return value


def _info(path: Path) -> dict:
    # CLI executables can be much larger than the bounded input/artifact limit.
    digest, size = hashlib.sha256(), 0
    with os.fdopen(_open_regular(path), "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
    return {"sha256": digest.hexdigest(), "size": size}


def _selection(config: dict) -> None:
    for name in ("intent_id", "policy_version", "model", "reasoning_effort"):
        value = config.get(name)
        if (not isinstance(value, str) or not value.strip() or value != value.strip()
                or value.lower() in ("auto", "default")):
            raise RunnerError(f"Explicit {name} is required")
    timeout = config.get("timeout_seconds", 3600)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise RunnerError("timeout_seconds must be positive")


def _verified_client_default(plan: dict, runtime: Path, config: dict) -> dict:
    """Verify the existing zero-inference parser capture against the same argv.

    This proves local client parsing under the fixed responder. It is neither a
    production payload capture nor proof of the remote model identity.
    """
    report = _read(runtime / "exec-context.json")
    overrides = report.get("only_extra_argv")
    if (not isinstance(overrides, list) or len(overrides) != 10
            or any(not isinstance(value, str) for value in overrides)
            or overrides[::2] != ["-c"] * 5
            or overrides[1:4:2] != ['model_provider="audit-fixed"',
                                   'model_providers.audit-fixed.name="Local no inference fixture"']
            or re.fullmatch(r'model_providers\.audit-fixed\.base_url="http://127\.0\.0\.1:[0-9]+/v1"', overrides[5]) is None
            or overrides[7:] != ['model_providers.audit-fixed.wire_api="responses"', "-c",
                                   "model_providers.audit-fixed.requires_openai_auth=false"]):
        raise RunnerError("Client-default proof must use only the known local fixed-responder overrides")
    argv = plan["argv"]
    if argv.count("exec") != 1:
        raise RunnerError("Client-default proof needs one exact exec command")
    split = argv.index("exec")
    settings = [part.split("=", 1)[1] for part in argv if part.startswith("default_permissions=")]
    if len(settings) != 1:
        raise RunnerError("Client-default proof must bind one actual permissions profile")
    profile = json.loads(settings[0])
    core = {key: value for key, value in plan.items() if key != "probe_evidence_hashes"}
    expected_binding = {"plan_core_sha256": _digest(_bytes(core)), "cwd": plan["cwd"],
                        "profile": profile, "argv_sha256": _digest(_bytes(argv)),
                        "public_tree_digest": plan["public_tree_digest"],
                        "input_digest": plan["input_digest"], "request_sha256": plan["request_sha256"]}
    observations = report.get("observations")
    if (report.get("passed") is not True
            or report.get("nature") != "current_local_fixed_exec_context_not_production_payload"
            or type(report.get("exit_code")) is not int or report["exit_code"] != 0
            or report.get("exception")
            or any(type(report.get(name)) is not int or report[name] != 0
                   for name in ("model_calls", "external_model_requests", "tool_calls_returned"))
            or report.get("public_tree_unchanged") is not True
            or report.get("argv") != argv[:split] + overrides + argv[split:]
            or report.get("bound") != expected_binding
            or not isinstance(observations, list) or len(observations) != 1
            or not isinstance(observations[0], dict)):
        raise RunnerError("Client-default parser capture is absent, changed, unbound or used inference")
    observation = observations[0]
    inputs, reasoning = observation.get("input"), observation.get("reasoning")
    expected_context = config.get("reasoning_context")
    if (not isinstance(expected_context, str) or not expected_context
            or observation.get("path") != "/v1/responses"
            or observation.get("model") != config["model"]
            or not isinstance(reasoning, dict)
            or reasoning.get("effort") != config["reasoning_effort"]
            or reasoning.get("context") != expected_context
            or not isinstance(inputs, list) or not inputs or not isinstance(inputs[-1], dict)
            or inputs[-1].get("role") != "user"
            or inputs[-1].get("content") != [{"type": "input_text", "text": _regular(runtime / "request.md").decode("utf-8")}]):
        raise RunnerError("Client-default model, reasoning or actual request does not match versioned policy")
    return {"client_resolved": observation["model"], "client_resolved_reasoning": reasoning,
            "client_resolution_evidence": _info(runtime / "exec-context.json"),
            "client_resolution_scope": "Local zero-inference fixed-responder parser capture under the same argv; production payload and served identity unobserved."}


def _native_command(config: dict, root: Path) -> tuple[list[str], dict, dict]:
    # Shared settings are used by both actual tools and the no-model probe.
    from tools.isolated_quality import capture_cli_evidence, inline_table, selection_metadata
    cli = str(Path(config["cli"]).resolve())
    evidence = capture_cli_evidence(root, cli)
    model, version = selection_metadata({**config, "cli": cli}, evidence)
    permissions = config.get("native_permissions")
    if not isinstance(permissions, dict):
        raise RunnerError("Native roles require an explicit versioned permissions profile")
    profile = permissions.get("profile")
    if not isinstance(profile, str) or re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", profile) is None:
        raise RunnerError("Native permissions profile name is invalid")
    repository = Path(permissions["repository"]).resolve()
    cwd, scratch = Path(config["cwd"]).resolve(), Path(permissions["scratch"]).resolve()
    python = str(Path(permissions["python"]).resolve())
    filesystem = dict(permissions.get("filesystem", {}))
    if (not repository.is_dir() or not cwd.is_dir() or not scratch.is_dir()
            or filesystem.get(str(repository)) != "read"
            or filesystem.get(str(cwd)) != "write" or filesystem.get(str(scratch)) != "write"
            or any(value not in {"read", "write", "deny"} for value in filesystem.values())
            or any(value == "write" and key not in {str(cwd), str(scratch)}
                   for key, value in filesystem.items())
            or any(value == "deny" and not key.startswith(":")
                   and Path(key).is_relative_to(repository) for key, value in filesystem.items())):
        raise RunnerError("Native roles need the full project read-only and only own work/scratch writable")
    readonly = Path(permissions["readonly_probe"]).resolve()
    sources = [str(Path(name).resolve()) for name in permissions.get("probe_sources", [])]
    if not sources or not readonly.is_relative_to(repository):
        raise RunnerError("Native probe needs real project read sources and a read-only project target")
    for name in [*sources, str(readonly)]:
        _info(Path(name))
    filesystem[str(root / "inputs")] = "read"
    environment = {**permissions.get("environment", {}), "TMPDIR": str(scratch),
                   "TMPPREFIX": str(scratch / "zsh"), "PYTHONDONTWRITEBYTECODE": "1"}
    settings = ["default_permissions=" + json.dumps(profile),
                f"permissions.{profile}.filesystem=" + inline_table(filesystem),
                f"permissions.{profile}.network.enabled=true",
                f'permissions.{profile}.network.domains={{"*"="allow"}}',
                "features.network_proxy=true", 'web_search="live"',
                'shell_environment_policy.inherit="core"',
                "shell_environment_policy.set=" + inline_table(environment),
                "allow_login_shell=false", "apps._default.enabled=false", "agents.enabled=false",
                "model_reasoning_effort=" + json.dumps(config["reasoning_effort"])]
    disabled = config.get("disabled_skill_files", [])
    if not isinstance(disabled, list) or any(not isinstance(name, str) for name in disabled):
        raise RunnerError("disabled_skill_files must list exact local skill sources")
    if disabled:
        for name in disabled:
            _info(Path(name))
        settings.append("skills.config=[" + ",".join(
            inline_table({"path": name, "enabled": False}) for name in disabled) + "]")
    options = [part for setting in settings for part in ("-c", setting)]
    for feature in ("multi_agent", "apps", "plugins", "memories", "hooks", "skill_search"):
        options += ["--disable", feature]
    options += ["--enable", "skip_host_skill_discovery"]
    argv = [cli, "--no-daemon", "-a", "never", *options, "-C", str(cwd), "exec"]
    if config["role"] == "review-score":
        argv += ["resume", "--ignore-user-config", "--ignore-rules", "--skip-git-repo-check",
                 "--json", "-m", config["model"], config["resume_sid"], "-"]
    else:
        argv += ["--ignore-user-config", "--ignore-rules", "--skip-git-repo-check",
                 "--json", "-m", config["model"]]
        if config["role"] != "review-facts":
            argv += ["--ephemeral"]
        argv += ["-"]
    direct = permissions.get("direct_network", {"tcp": "deny", "unix": "deny"})
    if (not isinstance(direct, dict) or set(direct) != {"tcp", "unix"}
            or any(value not in {"allow", "deny"} for value in direct.values())):
        raise RunnerError("Native direct-network probe expectations must be explicit allow/deny observations")
    native = {"profile": profile, "filesystem": filesystem, "repository": str(repository),
              "scratch": str(scratch), "python": python, "probe_sources": sources,
              "readonly_probe": str(readonly), "network": {"enabled": True, "domains": {"*": "allow"}},
              "web_search": "live", "network_proxy": True, "direct_network": direct,
              "sandbox_argv": [cli, "sandbox", "-P", profile, "--include-managed-config",
                               *options, "-C", str(cwd)]}
    return argv, {**model, "cli_version": version,
                  "catalog_evidence": _info(root / "cli-evidence.json")}, native


def prepare_attempt(root: Path, config: dict) -> dict:
    """Lock a new request and adapter, without making a model call.

    inputs are typed source records: {path, kind, target?}. Blind role requests
    are generated from operation sections and public/submission/trace inputs.
    Type labels are an enforceable provenance contract, not semantic detection.
    """
    from taskflow.execution_batch_binding import validate_batch_runner

    root = Path(root).absolute()
    validate_batch_runner(config)
    _selection(config)
    role = config.get("role")
    roles = {"research", "author", "repair", "quality", "code-review",
             "calibration", "solver", "review-facts", "review-score"}
    if role not in roles:
        raise RunnerError("Unknown execution role")
    mode = config.get("mode", "native_role")
    reasoning_selection = config.get("reasoning_selection", "explicit")
    if reasoning_selection not in {"explicit", "verified_client_default"}:
        raise RunnerError("Unknown versioned reasoning selection")
    if reasoning_selection == "verified_client_default" and mode != "existing_isolated":
        raise RunnerError("Verified client defaults are only supported by the existing formal runner")
    if mode not in {"test_fixture", "native_role", "existing_isolated"}:
        raise RunnerError("Unknown runner mode")
    if role == "solver" and mode == "native_role":
        raise RunnerError("Solver requires the existing isolated runner and actual probe")
    candidate_binding = config.get("candidate_binding")
    if candidate_binding is not None and not isinstance(candidate_binding, dict):
        raise RunnerError("candidate_binding must be an object when supplied")
    if role == "review-score":
        seal_root = Path(config.get("facts_attempt", "")).absolute()
        seal = _validated_facts_seal(seal_root)
        if (seal.get("sid") != config.get("resume_sid")
                or seal.get("candidate_binding") != candidate_binding):
            raise RunnerError("Scoring requires sealed facts in the same review SID and candidate binding")
    elif config.get("scoring_source_allowlist") is not None or config.get("scoring_projection") is not None:
        raise RunnerError("Only review-score can select normative scoring sources")
    allowed = ({"public", "operation"} if role == "solver" else
               {"public", "submission", "trace", "operation", "teacher"} if role == "review-score" else
               {"public", "submission", "trace", "operation"})
    constrained = role in {"solver", "review-facts", "review-score"}
    inputs = config.get("inputs", [])
    sections = config.get("request_sections", [])
    if not isinstance(inputs, list) or not isinstance(sections, list):
        raise RunnerError("inputs and request_sections must be lists")
    for entry in [*inputs, *sections]:
        if not isinstance(entry, dict) or not isinstance(entry.get("kind"), str):
            raise RunnerError("Every input/request section needs a provenance kind")
        if constrained and entry["kind"] not in allowed:
            raise RunnerError(f"Forbidden {entry['kind']} in {role} request")
    # Arbitrary text is only supported for teacher-informed roles.
    if constrained and config.get("request_text"):
        raise RunnerError("Solver and review requests require typed operation sections")
    root.mkdir(parents=True, exist_ok=False)
    (root / "inputs").mkdir()
    # Dispatch identifiers may encode teacher-side experiment intent. Keep them
    # in the manifest rather than the solver or first factual review request.
    request = ([f"Role: {role}."] if constrained else
               [f"Role: {role}. Intent: {config['intent_id']}."])
    locked_inputs = []
    for index, entry in enumerate(inputs):
        source = Path(entry["path"]).absolute()
        target = _relative(entry.get("target", f"{index:04d}-{source.name}"))
        destination = root / "inputs" / target
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as handle:
            handle.write(_regular(source))
        destination.chmod(0o444)
        locked_inputs.append({"source": str(source), "kind": entry["kind"],
                              "target": target, **_info(destination)})
        request.append(f"Input ({entry['kind']}): {destination}")
    for entry in sections:
        if not isinstance(entry.get("text"), str):
            raise RunnerError("Request section text must be a string")
        request.append(entry["text"])
    if config.get("request_text"):
        request.append(config["request_text"])
    (root / "request.md").write_text("\n\n".join(request) + "\n", encoding="utf-8")
    worker = root / "worker.py"
    worker.write_bytes(Path(__file__).read_bytes())
    native = None
    if mode == "native_role":
        argv, model, native = _native_command(config, root)
    else:
        argv = config.get("argv")
        model = {"requested": config["model"], "reasoning_effort": config["reasoning_effort"],
                 "client_resolved": None, "served": None, "revision": None}
    if not isinstance(argv, list) or not argv or any(not isinstance(x, str) for x in argv):
        raise RunnerError("argv must be a nonempty string list")
    cwd = Path(config.get("cwd", root)).absolute()
    if not cwd.is_dir():
        raise RunnerError("cwd must exist")
    outputs = [_relative(name) for name in config.get("outputs", [])]
    output_trees = [_relative(name) for name in config.get("output_trees", [])]
    if not outputs and not output_trees:
        raise RunnerError("At least one required output file or output directory must be declared")
    if len(set(output_trees)) != len(output_trees) or any(
            left != right and right.startswith(left + "/") for left in output_trees for right in output_trees):
        raise RunnerError("Required output directories cannot duplicate or nest")
    if any((cwd / name).exists() or (cwd / name).is_symlink() for name in [*outputs, *output_trees]):
        raise RunnerError("Required outputs must be new attempt-specific delivery paths")
    bridge = config.get("isolated", {})
    if mode == "existing_isolated":
        # Bind the bridge to the existing copied implementation, never to an
        # arbitrary caller-supplied "validate=true" command.
        runtime = Path(bridge.get("runtime", "")).resolve()
        flavor = bridge.get("flavor", "quality")
        if flavor == "quality":
            if reasoning_selection != "explicit":
                raise RunnerError("The existing quality runner requires explicit reasoning")
            plan = _read(runtime / "plan.json")
            ready = _read(runtime / "ready.json")
            probe_name = _relative(ready.get("probe_file", ""))
            if (ready.get("probe") != _info(runtime / probe_name)
                    or _read(runtime / probe_name).get("passed") is not True
                    or ready.get("source_lock") != _info(runtime / "source-lock.json")):
                raise RunnerError("Existing solver probe is absent, stale or unsuccessful")
            model = plan["model"]
            expected_cwd = str(runtime / "work")
            lock_names = ["runner.py", "plan.json", "plan-lock.json", "source-lock.json",
                          "prepared-lock.json", "ready.json", probe_name]
            python = str(Path(bridge.get("python", sys.executable)).resolve())
            argv = [python, str(runtime / "runner.py"), "execute", str(runtime)]
            validation = [python, str(runtime / "runner.py"), "validate", str(runtime)]
            trace_path = runtime / "run/trace.jsonl"
            attempt_path = runtime / "run/attempt.json"
        elif flavor == "formal":
            plan = _read(runtime / "execution-plan.json")
            plan_sha = _info(runtime / "execution-plan.json")["sha256"]
            if _read(runtime / "plan-lock.json").get("sha256") != plan_sha:
                raise RunnerError("Formal execution plan digest changed")
            if plan.get("source_lock_sha256") != _info(runtime / "source-lock.json")["sha256"]:
                raise RunnerError("Formal source lock digest changed")
            review = _read(runtime / "launch-review.json")
            if (review.get("approved") is not True or not review.get("reviewer")
                    or review.get("plan_sha256") != plan_sha
                    or review.get("input_digest") != plan.get("input_digest")):
                raise RunnerError("Formal launch approval is absent or belongs to another plan")
            if (not isinstance(plan.get("frozen_binding"), dict)
                    or plan["frozen_binding"].get("taskflow_input_digest") != plan.get("input_digest")
                    or not plan.get("input_digest")):
                raise RunnerError("Formal frozen input version binding is absent")
            for probe_name in ("shell-probes.json", "exec-context.json"):
                if (_read(runtime / probe_name).get("passed") is not True
                        or plan.get("probe_evidence_hashes", {}).get(probe_name)
                        != _info(runtime / probe_name)["sha256"]):
                    raise RunnerError("Formal native/context probe failed or drifted")
            if _info(runtime / "request.md")["sha256"] != plan.get("request_sha256"):
                raise RunnerError("Formal actual request differs from the locked plan")
            if plan.get("timeout_seconds") != config.get("timeout_seconds", 3600):
                raise RunnerError("Formal execution timeout differs from policy")
            expected_cwd = plan["cwd"]
            settings = [part.split("=", 1)[1] for part in plan["argv"]
                        if part.startswith("model_reasoning_effort=")]
            client = {"client_resolved": None}
            if reasoning_selection == "verified_client_default":
                if settings:
                    raise RunnerError("Verified client default must preserve the absence of a reasoning override")
                client = _verified_client_default(plan, runtime, config)
            elif len(settings) != 1 or json.loads(settings[0]) != config["reasoning_effort"]:
                raise RunnerError("Formal runner must bind one explicit reasoning setting")
            model = {"requested": plan["requested_model"],
                     "reasoning_effort": config["reasoning_effort"],
                     "reasoning_selection": reasoning_selection,
                     "request_reasoning_override": None if reasoning_selection == "verified_client_default" else config["reasoning_effort"],
                     **client, "served": None, "revision": None}
            lock_names = ["execute_diagnostic.py", "runtime_gate.py", "file_evidence.py",
                          "execution-plan.json", "plan-lock.json", "source-lock.json",
                          "baseline-plan.json", "shell-probes.json", "exec-context.json",
                          "launch-review.json"]
            python = str(Path(plan["launcher_python"]).resolve())
            argv = [python, str(runtime / "execute_diagnostic.py"), "--execute"]
            validation = [python, str(runtime / "execute_diagnostic.py")]
            trace_path = runtime / "real-run/trace.jsonl"
            attempt_path = runtime / "real-run/attempt.json"
        else:
            raise RunnerError("Unknown existing isolated runner flavor")
        if (model["requested"] != config["model"]
                or model["reasoning_effort"] != config["reasoning_effort"]
                or expected_cwd != str(cwd)):
            raise RunnerError("Existing solver model or cwd differs from the locked plan")
        bridge = {"runtime": str(runtime), "flavor": flavor,
                  "lock_names": lock_names, "validate_argv": validation,
                  "trace_path": str(trace_path), "attempt_path": str(attempt_path),
                  "actual_request": str(runtime / "request.md"),
                  "actual_request_info": _info(runtime / "request.md"),
                  "source_binding": {"frozen_binding": plan.get("frozen_binding"),
                                     "input_digest": plan.get("input_digest"),
                                     "source_binding": plan.get("source_binding")}}
    dependencies = [Path(argv[0]).resolve(), Path(sys.executable).resolve(), worker]
    dependencies += [Path(name).absolute() for name in config.get("dependencies", [])]
    dependencies += [Path(name).absolute() for name in config.get("disabled_skill_files", [])]
    if config.get("batch_binding") is not None:
        from taskflow.execution_batch import load_batch_snapshot

        reference = config["batch_binding"]["policy_config"]["batch_snapshot"]
        confirmed = load_batch_snapshot(reference)
        snapshot = Path(reference["path"])
        dependencies += [snapshot, *[snapshot.parent / item["relative_path"] for item in confirmed["evidence_copies"]]]
    if native:
        dependencies.append(Path(native["python"]))
    if mode == "existing_isolated":
        dependencies += [Path(bridge["runtime"]) / name for name in bridge["lock_names"]]
    manifest = {"schema_version": 1, "created_at": _now(), "intent_id": config["intent_id"],
                "preparation_config_digest": _digest(_bytes(config)),
                "policy_version": config["policy_version"], "role": role, "mode": mode,
                "candidate_binding": candidate_binding,
                "nature": "test_fixture" if mode == "test_fixture" else "real_cli_attempt",
                "model": model, "argv": argv, "cwd": str(cwd), "inputs": locked_inputs,
                "outputs": outputs, "output_trees": output_trees,
                "output_tree_contract_version": 2 if output_trees else 1,
                "request": _info(root / "request.md"),
                "dependencies": {str(path): _info(path) for path in dependencies},
                "timeout_seconds": config.get("timeout_seconds", 3600),
                "reasoning_selection": reasoning_selection,
                "reasoning_context": config.get("reasoning_context"),
                "automatic_retry": False, "isolated": bridge, "native": native,
                "resume_sid": config.get("resume_sid"),
                "facts_seal": seal if role == "review-score" else None,
                "facts_attempt": str(seal_root) if role == "review-score" else None,
                "facts_seal_digest": _info(seal_root / "facts-seal.json") if role == "review-score" else None,
                "scoring_source_allowlist": config.get("scoring_source_allowlist"),
                "scoring_projection": config.get("scoring_projection"),
                "limitations": ["Type/provenance checks do not prove absence of semantic leakage.",
                                "No exactly-once guarantee across external process launch.",
                                "Business quality, formal validity and difficulty remain separate."]}
    if config.get("batch_binding") is not None:
        manifest["batch_binding"] = config["batch_binding"]
        manifest["model_slot"] = config["model_slot"]
        validate_batch_runner(manifest, manifest=True)
    _publish(root / "manifest.json", manifest)
    _publish(root / "manifest-lock.json", _info(root / "manifest.json"))
    return manifest


def _validate(root: Path, before_run: bool = False) -> dict:
    manifest = _read(root / "manifest.json")
    if _read(root / "manifest-lock.json") != _info(root / "manifest.json"):
        raise RunnerError("Manifest changed after preparation")
    if manifest["request"] != _info(root / "request.md"):
        raise RunnerError("Actual request changed after preparation")
    for entry in manifest["inputs"]:
        observed = _info(root / "inputs" / entry["target"])
        if observed != {key: entry[key] for key in ("sha256", "size")}:
            raise RunnerError("Locked input changed after preparation")
    for name, expected in manifest["dependencies"].items():
        if _info(Path(name)) != expected:
            raise RunnerError(f"Locked executable/dependency changed: {name}")
    if manifest.get("facts_seal"):
        source = Path(manifest["facts_attempt"])
        seal = _validated_facts_seal(source)
        if (seal != manifest["facts_seal"]
                or _info(source / "facts-seal.json") != manifest["facts_seal_digest"]):
            raise RunnerError("Sealed facts or their source binding changed before scoring")
    if manifest["mode"] == "native_role":
        if manifest["model"]["catalog_evidence"] != _info(root / "cli-evidence.json"):
            raise RunnerError("Native model catalog/version evidence changed")
    if manifest["mode"] == "native_role" and (before_run or (root / "native-probe-ready.json").exists()):
        ready = _read(root / "native-probe-ready.json")
        if ready.get("manifest") != _info(root / "manifest.json"):
            raise RunnerError("Native probe belongs to another request or permissions profile")
        report_path = root / _relative(ready["report_file"])
        report = _read(report_path)
        if (ready.get("report") != _info(report_path) or report.get("passed") is not True
                or report.get("model_calls") != 0 or report.get("manifest") != ready["manifest"]):
            raise RunnerError("Native permissions/normal-tool probe failed or changed")
    if __package__:
        # The locked standalone supervisor checks every captured snapshot and
        # evidence byte as dependencies above. Its parent performs semantic
        # policy validation before dispatch; the supervisor has no repo imports.
        from taskflow.execution_batch_binding import validate_batch_runner

        validate_batch_runner(manifest, manifest=True)
    if manifest["mode"] == "existing_isolated" and before_run:
        bridge = manifest["isolated"]
        if _info(Path(bridge["actual_request"])) != bridge["actual_request_info"]:
            raise RunnerError("Existing solver actual request changed")
        result = subprocess.run(bridge["validate_argv"],
                                capture_output=True, timeout=30, check=False)
        if result.returncode:
            raise RunnerError("Existing isolated runner validation rejected launch")
    return manifest


def probe_native_attempt(root: Path) -> dict:
    """Probe actual native filesystem and local TCP/UNIX access, without a model."""
    import select

    root = Path(root).absolute()
    manifest = _validate(root)
    native = manifest.get("native")
    if manifest["mode"] != "native_role" or not native:
        raise RunnerError("Only a native role uses this probe")
    token = uuid.uuid4().hex
    report_name = "native-probe-" + token + ".json"
    report = {"at": _now(), "manifest": _info(root / "manifest.json"), "model_calls": 0,
              "passed": False, "scope": "Listed project reads, read-only project target, own directory writes and configured direct TCP/UNIX boundaries; managed network/web configuration is explicit, but no external web/model call is made."}
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    unix_name = "np-" + token[:12] + ".sock"
    unix_path = Path(native["scratch"]) / unix_name
    helper, unix_identity = None, None
    helper_stdout, helper_stderr, helper_collected = "", "", False
    helper_code = """import json,os,socket,stat,sys
name=sys.argv[1]
listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
owned=None
try:
 listener.bind(name)
 info=os.lstat(name);owned=(info.st_dev,info.st_ino)
 listener.listen(1)
 print(json.dumps({'ready':True}),flush=True)
 action=sys.stdin.readline().strip()
 if action=='accept':
  listener.settimeout(3)
  connection,_=listener.accept()
  with connection:
   connection.settimeout(3)
   assert connection.recv(6)==b'NATIVE','incomplete native UNIX handshake'
  print(json.dumps({'accepted':True}),flush=True)
 elif action=='close':
  print(json.dumps({'closed':True}),flush=True)
 else:
  raise ValueError('unknown native listener control')
finally:
 listener.close()
 if owned is not None:
  try:
   info=os.lstat(name)
   if stat.S_ISSOCK(info.st_mode) and (info.st_dev,info.st_ino)==owned:os.unlink(name)
  except FileNotFoundError:pass
"""
    try:
        tcp.bind(("127.0.0.1", 0))
        tcp.listen(1)
        tcp.settimeout(2)
        # AF_UNIX limits the bind/connect argument, not the filesystem's cwd.
        # A child owns cwd so simultaneous probes never change the parent cwd.
        helper_argv = [sys.executable, "-B", "-u", "-c", helper_code, unix_name]
        helper = subprocess.Popen(helper_argv, cwd=native["scratch"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True, start_new_session=True)
        report["unix_listener"] = {"argv": helper_argv, "cwd": native["scratch"],
                                   "basename": unix_name, "pid": helper.pid,
                                   "scratch_path_bytes": len(os.fsencode(native["scratch"])),
                                   "bind_argument_bytes": len(os.fsencode(unix_name)),
                                   "model_calls": 0, "ready": False}
        readable, _, _ = select.select([helper.stdout], [], [], 5)
        if not readable:
            raise RunnerError("Native UNIX listener did not become ready")
        ready_line = helper.stdout.readline(4096)
        if json.loads(ready_line) != {"ready": True}:
            raise RunnerError("Native UNIX listener readiness handshake failed")
        unix_info = unix_path.lstat()
        if not stat.S_ISSOCK(unix_info.st_mode):
            raise RunnerError("Native UNIX listener did not create its socket")
        unix_identity = (unix_info.st_dev, unix_info.st_ino)
        report["unix_listener"].update(ready=True, readiness_stdout=ready_line)
        sources = {name: _info(Path(name)) for name in native["probe_sources"]}
        code = "import os,errno,pathlib,json,hashlib,socket\nrows=[]\n"
        code += "for name,expected in " + repr(sources) + ".items():\n"
        code += " data=pathlib.Path(name).read_bytes();assert {'sha256':hashlib.sha256(data).hexdigest(),'size':len(data)}==expected;rows.append('read:'+name)\n"
        code += "try:\n fd=os.open(" + repr(native["readonly_probe"]) + ",os.O_RDWR|os.O_NONBLOCK);os.close(fd)\n raise AssertionError('Project writable')\n"
        code += "except OSError as e:\n assert e.errno in (errno.EPERM,errno.EACCES);rows.append('project_write_denied')\n"
        code += "for directory in " + repr([manifest["cwd"], native["scratch"]]) + ":\n"
        code += " p=pathlib.Path(directory)/" + repr("np-" + token + ".txt") + ";p.write_text('NEUTRAL');assert p.read_text()=='NEUTRAL';p.unlink();rows.append('write:'+directory)\n"
        for kind, family, address in (("tcp", "socket.AF_INET", tcp.getsockname()),
                                      ("unix", "socket.AF_UNIX", unix_name)):
            if kind == "unix":
                code += "os.chdir(" + repr(native["scratch"]) + ")\n"
            code += "s=socket.socket(" + family + ");s.settimeout(2)\n"
            code += "try:\n s.connect(" + repr(address) + ");s.sendall(b'NATIVE');observed='allow'\n"
            code += "except OSError as e:\n assert e.errno in (errno.EPERM,errno.EACCES);observed='deny'\nfinally:s.close()\n"
            code += "assert observed==" + repr(native["direct_network"][kind]) + ";rows.append(" + repr(kind + ":") + "+observed)\n"
        code += "print(json.dumps(rows))\n"
        argv = [*native["sandbox_argv"], "--", native["python"], "-B", "-c", code]
        result = subprocess.run(argv, cwd=manifest["cwd"], capture_output=True, text=True,
                                timeout=30, check=False)
        report.update(argv=argv, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        expected = [*("read:" + name for name in sources), "project_write_denied",
                    "write:" + manifest["cwd"], "write:" + native["scratch"],
                    "tcp:" + native["direct_network"]["tcp"], "unix:" + native["direct_network"]["unix"]]
        checks = json.loads(result.stdout) if result.returncode == 0 else None
        if result.returncode == 0 and checks == expected:
            if native["direct_network"]["tcp"] == "allow":
                connection, _ = tcp.accept()
                with connection:
                    connection.settimeout(2)
                    if connection.recv(6) != b"NATIVE":
                        raise RunnerError("Native TCP probe handshake was incomplete")
            action = "accept" if native["direct_network"]["unix"] == "allow" else "close"
            helper_stdout, helper_stderr = helper.communicate(action + "\n", timeout=5)
            helper_collected = True
            wanted = {"accepted": True} if action == "accept" else {"closed": True}
            if helper.returncode != 0 or json.loads(helper_stdout) != wanted:
                raise RunnerError("Native UNIX listener completion handshake failed")
            report["passed"] = True
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        report["exception"] = {"type": type(error).__name__, "message": str(error)}
    finally:
        tcp.close()
        if helper is not None:
            if helper.poll() is None:
                try:
                    helper_stdout, helper_stderr = helper.communicate("close\n", timeout=3)
                    helper_collected = True
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    helper.terminate()
                    try:
                        helper.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        helper.kill()
                        helper.wait(timeout=3)
            if not helper_collected:
                try:
                    helper_stdout, helper_stderr = helper.communicate(timeout=3)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    for pipe in (helper.stdin, helper.stdout, helper.stderr):
                        if pipe is not None:
                            pipe.close()
            report["unix_listener"].update(returncode=helper.returncode,
                                             stdout=helper_stdout, stderr=helper_stderr)
        if unix_identity is not None:
            try:
                current = unix_path.lstat()
                if stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == unix_identity:
                    unix_path.unlink()
            except FileNotFoundError:
                pass
    _publish(root / report_name, report)
    if report["passed"]:
        _publish(root / "native-probe-ready.json", {"manifest": report["manifest"],
                 "report_file": report_name, "report": _info(root / report_name)})
    return report


def _identity(pid: int) -> dict:
    result = {"pid": pid, "observed_at": _now(), "start_identity": None}
    proc = Path(f"/proc/{pid}/stat")
    if proc.exists():
        try:
            result["start_identity"] = proc.read_text().split(") ", 1)[1].split()[19]
            result["identity_source"] = "proc_start_ticks"
            return result
        except (OSError, IndexError):
            pass
    try:
        check = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "lstart=", "-o", "command="],
                               capture_output=True, text=True, timeout=5, check=False)
        if check.returncode == 0 and check.stdout.strip():
            result["start_identity"] = check.stdout.strip()
            result["identity_source"] = "ps_lstart_command"
    except (OSError, subprocess.TimeoutExpired):
        pass
    return result


def _worker(root: Path) -> None:
    manifest = _validate(root)
    _publish(root / "supervisor.json", {**_identity(os.getpid()), "intent_id": manifest["intent_id"]})
    termination = {"intent_id": manifest["intent_id"], "started_at": _now(),
                   "returncode": None, "exception": None, "timed_out": False,
                   "spawn_state": "unknown", "spawn_failure_origin": None}
    process, spawn_entered = None, False
    try:
        with (root / "trace.jsonl").open("xb") as stdout, (root / "stderr.txt").open("xb") as stderr:
            spawn_entered = True
            process = subprocess.Popen(manifest["argv"], cwd=manifest["cwd"],
                                       stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
            termination["spawn_state"] = "started"
            _publish(root / "process.json", {**_identity(process.pid),
                                            "intent_id": manifest["intent_id"]})
            try:
                process.communicate((root / "request.md").read_bytes(),
                                    timeout=manifest["timeout_seconds"] +
                                    (30 if manifest["mode"] == "existing_isolated" else 0))
            except subprocess.TimeoutExpired:
                termination["timed_out"] = True
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            termination["returncode"] = process.returncode
    except OSError as error:
        termination["exception"] = {"type": type(error).__name__, "message": str(error)}
        if spawn_entered and process is None:
            termination.update(spawn_state="not_started", spawn_failure_origin="cli_popen")
    termination["ended_at"] = _now()
    _publish(root / "termination.json", termination)


def run_attempt(root: Path, fault: str | None = None,
                on_process: Callable[[dict], None] | None = None) -> dict:
    """Persist intent before launch; an occupied intent is never relaunched."""
    root = Path(root).absolute()
    manifest = _validate(root, before_run=True)
    _publish(root / "dispatch.json", {"intent_id": manifest["intent_id"], "at": _now(),
                                      "manifest": _info(root / "manifest.json")})
    if fault == "before_start":
        return reconcile_attempt(root)
    with (root / "supervisor-stdout.txt").open("xb") as stdout, (root / "supervisor-stderr.txt").open("xb") as stderr:
        try:
            process = subprocess.Popen([sys.executable, str(root / "worker.py"), "--worker", str(root)],
                                       stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
        except OSError as error:
            _publish(root / "termination.json", {"intent_id": manifest["intent_id"],
                       "started_at": _now(), "ended_at": _now(), "returncode": None,
                       "exception": {"type": type(error).__name__, "message": str(error)},
                       "timed_out": False, "spawn_state": "not_started",
                       "spawn_failure_origin": "supervisor_popen"})
            return reconcile_attempt(root)
    if fault in {"after_start", "before_pid"}:
        _DETACHED[str(root)] = process
        return {"status": "uncertain", "automatic_retry": False}
    _publish(root / "launcher.json", _identity(process.pid))
    deadline = time.monotonic() + manifest["timeout_seconds"] + 60
    reported = False
    while process.poll() is None:
        if not reported and (root / "process.json").exists():
            if on_process is not None:
                on_process(_read(root / "process.json"))
            reported = True
        if time.monotonic() >= deadline:
            raise RunnerError("Supervisor ended without a termination receipt; reconcile required")
        time.sleep(.02)
    if not reported and (root / "process.json").exists() and on_process is not None:
        on_process(_read(root / "process.json"))
    return reconcile_attempt(root, fault="collecting" if fault == "collecting" else None)


def _trace(path: Path, expected_sid: str | None = None) -> dict:
    ids, terminals, messages, errors = [], [], [], []
    if not path.exists():
        return {"model_completed": False, "sid": None, "errors": ["missing_trace"]}
    try:
        lines = _regular(path, limit=128 * 1024 * 1024).decode("utf-8").splitlines()
    except (OSError, RunnerError, UnicodeError) as error:
        return {"model_completed": False, "sid": None,
                "errors": ["unsafe_or_unreadable_trace:" + type(error).__name__]}
    for number, line in enumerate(lines, 1):
        try:
            event = json.loads(line)
            if event.get("type") == "thread.started":
                ids.append(event.get("thread_id"))
            if event.get("type") in {"turn.completed", "turn.failed", "error"}:
                terminals.append(event["type"])
            item = event.get("item", {})
            if event.get("type") == "item.completed" and item.get("type") == "agent_message":
                messages.append(item.get("text", ""))
        except (ValueError, AttributeError):
            errors.append(f"invalid_event:{number}")
    sid = ids[0] if len(ids) == 1 else None
    try:
        if not sid or str(uuid.UUID(sid)) != sid:
            raise ValueError("sid")
    except (ValueError, TypeError, AttributeError):
        errors.append("not_one_valid_sid")
    if expected_sid and sid != expected_sid:
        errors.append("review_sid_changed")
    if terminals != ["turn.completed"]:
        errors.append("missing_or_failed_turn")
    if not messages or not messages[-1].strip():
        errors.append("missing_final_response")
    return {"model_completed": not errors, "sid": sid, "terminal_events": terminals,
            "errors": errors, "final_response": messages[-1] if messages else None}


def _output_tree_files(cwd: Path, trees: list[str]) -> tuple[dict, dict, list[dict]]:
    """List untrusted output directories through no-follow directory handles."""
    result, directories, errors = {}, {}, []
    for tree in trees:
        members, tree_directories, nonempty, visited = [], [], 0, 0
        result[tree], directories[tree] = members, tree_directories
        def walk(directory: int, prefix: str) -> None:
            nonlocal nonempty, visited
            if prefix.count("/") > 128:
                raise RunnerError("Output directory nesting exceeds capture bound")
            with os.scandir(directory) as entries:
                for entry in sorted(entries, key=lambda row: row.name):
                    name = _relative(prefix + "/" + entry.name)
                    visited += 1
                    if visited > 2000:
                        raise RunnerError("Output directory exceeds bounded artifact count")
                    observed = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(observed.st_mode):
                        child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                        dir_fd=directory)
                        try:
                            tree_directories.append(name)
                            walk(child, name)
                        finally:
                            os.close(child)
                    elif stat.S_ISREG(observed.st_mode):
                        members.append(name)
                        nonempty += observed.st_size > 0
                    else:
                        errors.append({"path": name, "error": "Output directory contains a symlink or special file"})
        descriptor = None
        try:
            root = cwd / tree
            descriptor = os.open(root.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            for part in root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            tree_directories.append(tree)
            walk(descriptor, tree)
            if not nonempty:
                errors.append({"path": tree, "error": "Required output directory has no nonempty regular files"})
        except (OSError, RunnerError) as error:
            errors.append({"path": tree, "error": str(error)})
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return result, directories, errors


def _artifact_directories(root: Path) -> list[str]:
    artifacts = root / "artifacts"
    if not artifacts.exists():
        return []
    return sorted(["artifacts", *[str(path.relative_to(root)) for path in artifacts.rglob("*")
                                  if path.is_dir() and not path.is_symlink()
                                  and not path.name.startswith(".capture-")]])


def _allowed_output(manifest: dict, name: str) -> bool:
    return (name in manifest["outputs"]
            or any(name.startswith(tree + "/") for tree in manifest.get("output_trees", [])))


def _collection_files(root: Path) -> dict:
    names = ["manifest.json", "manifest-lock.json", "request.md", "dispatch.json",
             "supervisor.json", "launcher.json", "process.json", "termination.json",
             "trace.jsonl", "stderr.txt", "collection-sources.json",
             "observed-trace.jsonl", "observed-request.md", "observed-attempt.json",
             "observed-input-readback.json"]
    if (root / "native-probe-ready.json").exists():
        names += ["native-probe-ready.json", _relative(_read(root / "native-probe-ready.json")["report_file"])]
    if (root / "cli-evidence.json").exists():
        names.append("cli-evidence.json")
    files = {name: _info(root / name) for name in names if (root / name).exists()}
    artifacts = root / "artifacts"
    if artifacts.exists():
        for source in artifacts.rglob("*"):
            if source.is_file() and not source.name.startswith(".capture-"):
                files[str(source.relative_to(root))] = _info(source)
    return files


def _save_collection(root: Path, result: dict) -> dict:
    result["evidence_files"] = _collection_files(root)
    result["artifact_directories"] = _artifact_directories(root)
    data = _bytes(result) + b"\n"
    expected = {"sha256": _digest(data), "size": len(data)}
    lock = root / "collection-lock.json"
    if lock.exists():
        if _read(lock) != expected:
            raise RunnerError("Interrupted collection conclusion no longer matches its bound evidence")
    else:
        _publish(lock, expected)
    _publish_bytes(root / "collection.json", data)
    return result


def _validated_collection(root: Path, manifest: dict) -> dict:
    result = _read(root / "collection.json")
    if (not (root / "collection-lock.json").exists()
            or _read(root / "collection-lock.json") != _info(root / "collection.json")):
        raise RunnerError("Collection conclusion has no matching immutable digest; original requires audit")
    if result.get("intent_id") != manifest["intent_id"] or not isinstance(result.get("evidence_files"), dict):
        raise RunnerError("Collection evidence binding is missing or belongs to another intent")
    if ("artifact_directories" in result or manifest.get("output_tree_contract_version", 1) >= 2):
        if result.get("artifact_directories") != _artifact_directories(root):
            raise RunnerError("Collected output directory structure drifted")
    current = _collection_files(root)
    if current != result["evidence_files"]:
        raise RunnerError("Collected artifact, trace or original process evidence drifted")
    for name, expected in result.get("artifacts", {}).items():
        if not _allowed_output(manifest, name) or current.get("artifacts/" + name) != expected:
            raise RunnerError("Collection contains an unbound output artifact")
    return result


def reconcile_attempt(root: Path, fault: str | None = None) -> dict:
    """Observe original process and artifacts. Never starts any process/model."""
    root = Path(root).absolute()
    manifest = _validate(root)
    detached = _DETACHED.get(str(root))
    if detached is not None and detached.poll() is not None:
        detached.wait()
        del _DETACHED[str(root)]
    if (root / "collection.json").exists():
        return _validated_collection(root, manifest)
    if not (root / "termination.json").exists():
        identities = [_read(root / name) for name in ("process.json", "supervisor.json", "launcher.json")
                      if (root / name).exists()]
        live = any(identity.get("start_identity") is not None and
                   _identity(identity["pid"])["start_identity"] == identity["start_identity"]
                   for identity in identities)
        return {"status": "running" if live else "uncertain", "automatic_retry": False,
                "process_ended": False, "reason": "No bound termination receipt; lease expiry proves no absence."}
    termination = _read(root / "termination.json")
    if termination.get("intent_id") != manifest["intent_id"]:
        raise RunnerError("Termination receipt belongs to another dispatch intent")
    if termination.get("spawn_state") == "not_started":
        if ((root / "process.json").exists() or termination.get("returncode") is not None
                or termination.get("spawn_failure_origin") not in {"cli_popen", "supervisor_popen"}
                or not isinstance(termination.get("exception"), dict)):
            raise RunnerError("Conflicting evidence cannot prove the target execution never started")
        return _save_collection(root, {
            "intent_id": manifest["intent_id"], "nature": manifest["nature"],
            "status": "not_started", "process_state": "not_started", "process_ended": True,
            "model_completed": False, "delivery_complete": False, "technical_accepted": False,
            "termination": termination, "artifacts": {}, "artifact_errors": [], "sid": None,
            "business_quality": "unresolved", "formal_experiment_validity": "unresolved",
            "difficulty_target": "unresolved", "platform_status": "unverified",
            "automatic_retry": False})
    if termination.get("returncode") is None:
        return {"status": "uncertain", "process_ended": False, "automatic_retry": False,
                "reason": "Termination note does not prove the target process exited or never started."}
    tree_members, tree_directories, tree_errors = _output_tree_files(
        Path(manifest["cwd"]), manifest.get("output_trees", []))
    sources = {}
    names = [*manifest["outputs"], *(name for members in tree_members.values() for name in members)]
    for name in names:
        sources["artifacts/" + name] = str(Path(manifest["cwd"]) / name)
    if manifest["mode"] == "existing_isolated":
        sources.update({"observed-trace.jsonl": manifest["isolated"]["trace_path"],
                        "observed-request.md": manifest["isolated"]["actual_request"],
                        "observed-attempt.json": manifest["isolated"]["attempt_path"]})
        if manifest["isolated"]["flavor"] == "formal":
            sources["observed-input-readback.json"] = str(Path(manifest["isolated"]["runtime"]) / "post-run-input-readback.json")
    index_path = root / "collection-sources.json"
    if not index_path.exists():
        index = {"intent_id": manifest["intent_id"], "files": {},
                 "output_tree_members": tree_members, "output_tree_directories": tree_directories,
                 "output_tree_errors": tree_errors}
        for target, name in sources.items():
            try:
                index["files"][target] = {"source": name, **_info(Path(name))}
            except (OSError, RunnerError) as error:
                index["files"][target] = {"source": name, "error": str(error)}
        _publish(index_path, index)
    index = _read(index_path)
    if index.get("intent_id") != manifest["intent_id"]:
        raise RunnerError("Collection source index belongs to another attempt")
    if (set(index["files"]) != set(sources)
            or index.get("output_tree_members", {}) != tree_members
            or index.get("output_tree_directories", {}) != tree_directories
            or index.get("output_tree_errors", []) != tree_errors):
        raise RunnerError("Original output directory membership changed after interrupted collection")
    artifacts, errors = {}, list(tree_errors)
    for names in tree_directories.values():
        for name in names:
            destination = root / "artifacts" / name
            if destination.is_symlink() or (destination.exists() and not destination.is_dir()):
                raise RunnerError("Captured output directory path is unsafe")
            destination.mkdir(parents=True, exist_ok=True)
    for target, expected in index["files"].items():
        source = Path(sources[target])
        if expected["source"] != str(source):
            raise RunnerError("Collection source path changed")
        if "error" in expected:
            errors.append({"path": target, "error": expected["error"]})
            continue
        try:
            limit = 128 * 1024 * 1024 if target.startswith("observed-") else 16 * 1024 * 1024
            data = _regular(source, limit=limit)
            observed = {"sha256": _digest(data), "size": len(data)}
            if observed != {key: expected[key] for key in ("sha256", "size")}:
                raise RunnerError("Original source changed after interrupted collection")
            if not data and (not target.startswith("artifacts/")
                             or target.removeprefix("artifacts/") in manifest["outputs"]):
                raise RunnerError("Empty required delivery")
            destination = root / target
            if destination.exists():
                if _info(destination) != observed:
                    raise RunnerError("Published collected evidence changed")
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                if fault == "collecting":
                    # This is a real partial write in the controlled temporary
                    # namespace; recovery must use the already-bound source.
                    (destination.parent / ".capture-interrupted").write_bytes(data[:max(1, len(data) // 2)])
                    return {"status": "collecting", "automatic_retry": False}
                _publish_bytes(destination, data)
            if target.startswith("artifacts/"):
                artifacts[target.removeprefix("artifacts/")] = observed
        except (OSError, RunnerError) as error:
            errors.append({"path": target, "error": str(error)})
    for partial in root.rglob(".capture-*"):
        if partial.is_file() and not partial.is_symlink():
            partial.unlink()
    trace_path = root / ("observed-trace.jsonl" if manifest["mode"] == "existing_isolated" else "trace.jsonl")
    trace = _trace(trace_path, manifest.get("resume_sid"))
    isolated_receipt = None
    if manifest["mode"] == "existing_isolated":
        try:
            isolated_receipt = _read(Path(manifest["isolated"]["attempt_path"]))
            if manifest["isolated"]["flavor"] == "quality":
                valid = isolated_receipt.get("status") == "completed"
            else:
                readback = _read(Path(manifest["isolated"]["runtime"]) / "post-run-input-readback.json")
                valid = (isolated_receipt.get("status") == "process_exited"
                         and isolated_receipt.get("returncode") == 0
                         and not isolated_receipt.get("exceptions")
                         and readback.get("matches") is True)
            if not valid:
                errors.append({"path": "isolated-attempt", "error": "Existing runner did not accept the attempt"})
        except (OSError, ValueError) as error:
            errors.append({"path": "isolated-attempt", "error": str(error)})
    accepted = (termination["returncode"] == 0 and not termination["timed_out"]
                and trace["model_completed"] and not errors)
    result = {"intent_id": manifest["intent_id"], "nature": manifest["nature"],
              "status": "completed" if accepted else "incomplete", "process_ended": True,
              **trace, "delivery_complete": not errors, "technical_accepted": accepted,
              "termination": termination, "artifacts": artifacts, "artifact_errors": errors,
              "isolated_receipt": isolated_receipt,
              "business_quality": "unresolved", "formal_experiment_validity": "unresolved",
              "difficulty_target": "unresolved", "platform_status": "unverified",
              "automatic_retry": False}
    return _save_collection(root, result)


def _validated_facts_seal(root: Path) -> dict:
    manifest = _validate(root)
    collected = _validated_collection(root, manifest)
    seal = _read(root / "facts-seal.json")
    name = _relative(seal.get("output_name", ""))
    path = root / "artifacts" / name
    if (manifest["role"] != "review-facts" or not collected.get("technical_accepted")
            or seal.get("sid") != collected.get("sid")
            or seal.get("intent_id") != manifest["intent_id"]
            or seal.get("candidate_binding") != manifest.get("candidate_binding")
            or seal.get("path") != str(path)
            or not _allowed_output(manifest, name) or name not in collected["artifacts"]
            or seal.get("facts") != collected["artifacts"][name]
            or _info(path) != seal["facts"]
            or seal.get("collection") != _info(root / "collection.json")
            or seal.get("manifest") != _info(root / "manifest.json")):
        raise RunnerError("Facts seal is not bound to the collected CLI facts and source attempt")
    return seal


def seal_facts(root: Path, facts_path: Path) -> dict:
    """Seal complete review facts before any same-context scoring handoff."""
    root = Path(root).absolute()
    manifest = _validate(root)
    collected = reconcile_attempt(root)
    if manifest["role"] != "review-facts" or not collected.get("technical_accepted"):
        raise RunnerError("Only a technically complete facts attempt can be sealed")
    facts_path = Path(facts_path).absolute()
    try:
        name = str(facts_path.relative_to(root / "artifacts"))
    except ValueError as error:
        raise RunnerError("Facts must be an actual collected CLI output artifact") from error
    _relative(name)
    if (not _allowed_output(manifest, name) or name not in collected["artifacts"]
            or _info(facts_path) != collected["artifacts"][name]
            or not _regular(facts_path)):
        raise RunnerError("Facts are not bound to the actual collected CLI output")
    seal = {"at": _now(), "sid": collected["sid"], "path": str(facts_path),
            "output_name": name, "facts": collected["artifacts"][name],
            "collection": _info(root / "collection.json"),
            "manifest": _info(root / "manifest.json"), "intent_id": manifest["intent_id"],
            "candidate_binding": manifest.get("candidate_binding")}
    _publish(root / "facts-seal.json", seal)
    return seal


def archive_attempt(root: Path, destination: Path, fault: str | None = None) -> dict:
    """Resume each atomic copy; source or published evidence is never overwritten."""
    root, destination = Path(root).absolute(), Path(destination).absolute()
    result = reconcile_attempt(root)
    if not (root / "collection.json").exists():
        raise RunnerError("Uncertain/running attempt cannot be archived as complete evidence")
    files = {str(source.relative_to(root)): _info(source) for source in root.rglob("*")
             if source.is_file() and not source.name.startswith(".capture-")}
    directories = sorted(str(path.relative_to(root)) for path in root.rglob("*")
                         if path.is_dir() and not path.is_symlink()
                         and not path.name.startswith(".capture-"))
    binding = {"intent_id": result["intent_id"], "collection": _info(root / "collection.json"),
               "files": files, "directories": directories}
    if destination.exists():
        existing = _read(destination / "archive-binding.json")
        expected_binding = binding
        if "directories" not in existing and _read(root / "manifest.json").get("output_tree_contract_version", 1) < 2:
            expected_binding = {key: value for key, value in binding.items() if key != "directories"}
        if existing != expected_binding:
            raise RunnerError("Archive destination belongs to different original evidence")
        if "directories" in existing:
            observed_directories = sorted(str(path.relative_to(destination)) for path in destination.rglob("*")
                                          if path.is_dir() and not path.is_symlink()
                                          and not path.name.startswith(".capture-"))
            if observed_directories != directories:
                raise RunnerError("Published archive directory structure changed")
            _validated_collection(destination, _read(destination / "manifest.json"))
            for name in directories:
                member = destination / name
                if member.is_symlink() or not member.is_dir():
                    raise RunnerError("Published archive directory structure changed")
        for name, expected in files.items():
            if _info(destination / name) != expected:
                raise RunnerError("Published archive evidence changed")
        return {"archived": True, "path": str(destination), **binding}
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = destination.with_name(destination.name + ".staging")
    if not stage.exists():
        stage.mkdir()
    index = stage / "archive-binding.json"
    if not index.exists():
        # A crash between mkdir and first atomic receipt can only leave the
        # controlled temporary namespace; any unexplained bytes need inspection.
        if any(path.is_file() and not path.name.startswith(".capture-") for path in stage.rglob("*")):
            raise RunnerError("Unbound archive staging contains unexplained files")
        _publish(index, binding)
    if _read(index) != binding:
        raise RunnerError("Interrupted archive source changed or belongs to another attempt")
    for name in directories:
        member = stage / name
        if member.is_symlink() or (member.exists() and not member.is_dir()):
            raise RunnerError("Staged archive directory path is unsafe")
        member.mkdir(parents=True, exist_ok=True)
    for name, expected in files.items():
        source, target = root / name, stage / name
        if _info(source) != expected:
            raise RunnerError("Source changed during archive publication")
        if target.exists():
            if _info(target) != expected:
                raise RunnerError("Published archive member changed")
            continue
        data = _regular(source, limit=128 * 1024 * 1024)
        if {"sha256": _digest(data), "size": len(data)} != expected:
            raise RunnerError("Source changed during archive read")
        target.parent.mkdir(parents=True, exist_ok=True)
        if fault == "archiving":
            (target.parent / ".capture-interrupted").write_bytes(data[:max(1, len(data) // 2)])
            return {"archived": False, "status": "archiving", **binding}
        _publish_bytes(target, data)
    for partial in stage.rglob(".capture-*"):
        if partial.is_file() and not partial.is_symlink():
            partial.unlink()
    # A source can change after initial reconciliation or after an earlier
    # member was copied. Validate against the sealed collection, then check the
    # complete source/copy set again immediately before atomic publication.
    _validated_collection(root, _read(root / "manifest.json"))
    _validated_collection(stage, _read(stage / "manifest.json"))
    for name, expected in files.items():
        if _info(root / name) != expected or _info(stage / name) != expected:
            raise RunnerError("Source or staged archive changed before publication")
    for location in (root, stage):
        observed_directories = sorted(str(path.relative_to(location)) for path in location.rglob("*")
                                      if path.is_dir() and not path.is_symlink()
                                      and not path.name.startswith(".capture-"))
        if observed_directories != directories:
            raise RunnerError("Source or staged archive directory structure changed before publication")
    stage.rename(destination)
    return {"archived": True, "path": str(destination), **binding}


def _full_directory(path: Path) -> int:
    """Open a source directory through no-follow directory descriptors."""
    path = Path(path).absolute()
    if ".." in path.parts:
        raise RunnerError("Parent traversal is not an archive source")
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _full_tree(root: Path) -> dict:
    """Index final bytes, directory membership and nonregular types, never links."""
    files, directories, special = {}, {}, {}

    def walk(descriptor: int, prefix: str) -> None:
        directories[prefix or "."] = stat.S_IMODE(os.fstat(descriptor).st_mode)
        with os.scandir(descriptor) as entries:
            for entry in sorted(entries, key=lambda value: value.name):
                name = prefix + entry.name
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                    dir_fd=descriptor)
                    try:
                        walk(child, name + "/")
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    if info.st_size > 128 * 1024 * 1024:
                        raise RunnerError(f"Full archive member exceeds existing 128 MiB limit: {root / name}")
                    files[name] = {**_info(root / name), "mode": stat.S_IMODE(info.st_mode)}
                else:
                    kind = ("symlink" if stat.S_ISLNK(info.st_mode) else
                            "fifo" if stat.S_ISFIFO(info.st_mode) else
                            "socket" if stat.S_ISSOCK(info.st_mode) else
                            "character" if stat.S_ISCHR(info.st_mode) else
                            "block" if stat.S_ISBLK(info.st_mode) else "other")
                    row = {"kind": kind, "mode": stat.S_IMODE(info.st_mode)}
                    if kind == "symlink":
                        row["target"] = os.readlink(entry.name, dir_fd=descriptor)
                    special[name] = row
    descriptor = _full_directory(root)
    try:
        walk(descriptor, "")
    finally:
        os.close(descriptor)
    return {"files": files, "directories": directories, "special": special}


def _full_source_roots(manifest: dict) -> tuple[dict[str, Path], dict]:
    proposed = {"work": Path(manifest["cwd"]).absolute()}
    if manifest.get("native"):
        proposed["scratch"] = Path(manifest["native"]["scratch"]).absolute()
    if manifest["mode"] == "existing_isolated":
        inner = Path(manifest["isolated"]["runtime"]).absolute()
        proposed["inner"] = inner
        if manifest["isolated"]["flavor"] == "quality":
            proposed["inner_work"] = inner / "work"
            proposed["scratch"] = inner / "scratch"
        else:
            plan = _read(inner / "execution-plan.json")
            proposed["scratch"] = Path(plan["root"]) / "solver-scratch"
    roots, aliases = {}, {}
    for name, path in sorted(proposed.items(), key=lambda item: (len(item[1].parts), item[0])):
        descriptor = _full_directory(path)
        os.close(descriptor)
        covering = next((key for key, root in roots.items() if path.is_relative_to(root)), None)
        if covering is None:
            roots[name] = path
            aliases[name] = {"source": name, "relative": ".", "path": str(path)}
        else:
            aliases[name] = {"source": covering, "relative": str(path.relative_to(roots[covering])) or ".",
                             "path": str(path)}
    return roots, aliases


def _full_layout(intent: dict) -> tuple[dict, list[str]]:
    payloads, directories = {}, {"."}

    def parents(name: str) -> None:
        path = Path(name).parent
        while str(path) != ".":
            directories.add(path.as_posix())
            path = path.parent

    for source, tree in intent["snapshot"].items():
        for name in tree["directories"]:
            relative = f"trees/{source}" + ("" if name == "." else "/" + name.rstrip("/"))
            try:
                _relative(relative)
            except RunnerError:
                # Unsafe/reserved names retain exact directory membership in the
                # manifest; payloads use indexed paths instead of losing bytes.
                continue
            directories.add(relative)
            parents(relative + "/member")
        for index, (name, expected) in enumerate(sorted(tree["files"].items()), 1):
            relative = f"trees/{source}/{name}"
            try:
                _relative(relative)
            except RunnerError:
                relative = f"indexed-files/{source}/{index:06d}.bin"
            payloads[relative] = {"source": source, "path": name,
                                  "size": expected["size"], "sha256": expected["sha256"]}
            parents(relative)
    return payloads, sorted(directories)


def _full_verify(location: Path, intent: dict, require_receipt: bool) -> dict:
    if _read(location / "manifest.json") != intent:
        raise RunnerError("Supplemental snapshot manifest differs from its durable intent")
    payloads, directories = _full_layout(intent)
    expected_files = {name: {key: row[key] for key in ("size", "sha256")}
                      for name, row in payloads.items()}
    expected_files["manifest.json"] = _info(location / "manifest.json")
    receipt = {"schema_version": 1, "snapshot_digest": _digest(_bytes(intent)),
               "collection": intent["collection"], "manifest": expected_files["manifest.json"],
               "files": expected_files, "directories": directories}
    if require_receipt:
        if _read(location / "receipt.json") != receipt:
            raise RunnerError("Supplemental completion receipt differs from the bound snapshot")
        expected_files = {**expected_files, "receipt.json": _info(location / "receipt.json")}
    observed = _full_tree(location)
    actual_files = {name: {key: row[key] for key in ("size", "sha256")}
                    for name, row in observed["files"].items()}
    actual_dirs = sorted(name.rstrip("/") for name in observed["directories"])
    if observed["special"] or actual_files != expected_files or actual_dirs != directories:
        raise RunnerError("Supplemental payload, directory membership or file type drifted")
    return receipt


def archive_full_attempt(root: Path, destination: Path) -> dict:
    """Seal terminal work/scratch/inner originals, then use the normal archive.

    A durable source index is the recovery identity. It is never regenerated to
    accept changed bytes, and this function never launches or retries a model.
    """
    from taskflow.contracts import _publish_no_replace

    root, destination = Path(root).absolute(), Path(destination).absolute()
    if ".." in root.parts or ".." in destination.parts:
        raise RunnerError("Archive roots cannot contain parent traversal")
    manifest = _validate(root)
    result = reconcile_attempt(root)
    if not result.get("process_ended") or not (root / "collection.json").exists():
        raise RunnerError("Full archive requires proved termination and a sealed collection")
    _validated_collection(root, manifest)
    sources, aliases = _full_source_roots(manifest)

    def overlap(left: Path, right: Path) -> bool:
        return left.is_relative_to(right) or right.is_relative_to(left)

    if overlap(root, destination) or any(overlap(path, root) or overlap(path, destination)
                                         for path in sources.values()):
        raise RunnerError("Full archive source, runtime and destination roots overlap")
    # Check existing destination ancestors without following aliases into another
    # source tree. The no-replace publication remains authoritative for races.
    cursor = destination
    while cursor != cursor.parent:
        if cursor.is_symlink():
            raise RunnerError("Archive destination has a symlink component")
        cursor = cursor.parent
    source_paths = {key: str(path) for key, path in sources.items()}
    intent_path = root / "supplemental-intent.json"
    binding = {"manifest": _info(root / "manifest.json"), "collection": _info(root / "collection.json"),
               "intent_id": manifest["intent_id"], "sources": source_paths, "aliases": aliases}
    observed = {key: _full_tree(path) for key, path in sources.items()}
    if intent_path.exists():
        intent = _read(intent_path)
        if any(intent.get(key) != value for key, value in binding.items()) or intent.get("snapshot") != observed:
            raise RunnerError("Original full-archive sources changed after their durable snapshot intent")
    else:
        if observed != {key: _full_tree(path) for key, path in sources.items()}:
            raise RunnerError("Full-archive sources changed during initial observations")
        intent = {"schema_version": 1, **binding, "snapshot": observed,
                  "limits": {"file_bytes": 128 * 1024 * 1024},
                  "scope": "Terminal ordinary-file bytes and full directory/special-type membership. Links/special files are indexed, not followed. Deleted transient files and every historical write are not reconstructed."}
        _publish(intent_path, intent)
    final, stage = root / "supplemental", root / "supplemental.staging"
    if final.exists() or final.is_symlink():
        receipt = _full_verify(final, intent, True)
    else:
        if stage.is_symlink():
            raise RunnerError("Supplemental staging must not be a symlink")
        stage.mkdir(exist_ok=True)
        index = stage / "manifest.json"
        if not index.exists():
            if any(path.is_file() and not path.name.startswith(".capture-") for path in stage.rglob("*")):
                raise RunnerError("Unbound supplemental staging contains unexplained originals")
            _publish(index, intent)
        elif _read(index) != intent:
            raise RunnerError("Supplemental staging belongs to another source snapshot")
        payloads, directories = _full_layout(intent)
        for name in directories:
            if name != ".":
                target = stage / name
                if target.is_symlink() or (target.exists() and not target.is_dir()):
                    raise RunnerError("Supplemental directory path is unsafe")
                target.mkdir(parents=True, exist_ok=True)
        for name, row in payloads.items():
            target = stage / name
            expected = {key: row[key] for key in ("size", "sha256")}
            if target.exists():
                if _info(target) != expected:
                    raise RunnerError("Previously published supplemental bytes changed")
                continue
            data = _regular(sources[row["source"]] / row["path"], limit=128 * 1024 * 1024)
            if {"size": len(data), "sha256": _digest(data)} != expected:
                raise RunnerError("Original full-archive bytes changed during capture")
            _publish_bytes(target, data)
        for partial in stage.rglob(".capture-*"):
            if partial.is_file() and not partial.is_symlink():
                partial.unlink()
        if (root / "collection.json").exists():
            _validated_collection(root, manifest)
        if intent["snapshot"] != {key: _full_tree(path) for key, path in sources.items()}:
            raise RunnerError("Original source membership or bytes changed before supplemental publication")
        if (stage / "receipt.json").exists():
            receipt = _full_verify(stage, intent, True)
        else:
            receipt = _full_verify(stage, intent, False)
            _publish(stage / "receipt.json", receipt)
        _full_verify(stage, intent, True)
        _publish_no_replace(stage, final)
    if intent["snapshot"] != {key: _full_tree(path) for key, path in sources.items()}:
        raise RunnerError("Original sources changed before final archive publication")
    # Keep the complete archive unpublished under a sibling name until the
    # external work/scratch sources have been checked after archive copying too.
    pending = destination.with_name(destination.name + ".full-pending")
    if overlap(root, pending) or any(overlap(path, pending) for path in sources.values()):
        raise RunnerError("Full archive pending destination overlaps an original source")
    if pending.is_symlink():
        raise RunnerError("Full archive pending destination must not be a symlink")
    if destination.exists() or destination.is_symlink():
        archived = archive_attempt(root, destination)
        if intent["snapshot"] != {key: _full_tree(path) for key, path in sources.items()}:
            raise RunnerError("Original sources changed while verifying the completed full archive")
        _full_verify(final, intent, True)
    else:
        archived = archive_attempt(root, pending)
        if intent["snapshot"] != {key: _full_tree(path) for key, path in sources.items()}:
            raise RunnerError("Original sources changed during final archive copying")
        _full_verify(final, intent, True)
        _validated_collection(root, manifest)
        _publish_no_replace(pending, destination)
        archived["path"] = str(destination)
    return {**archived, "supplemental": str(final), "supplemental_receipt": _info(final / "receipt.json"),
            "snapshot_digest": receipt["snapshot_digest"], "source_roots": source_paths,
            "regular_files_captured": sum(len(tree["files"]) for tree in intent["snapshot"].values()),
            "special_entries_indexed": sum(len(tree["special"]) for tree in intent["snapshot"].values()),
            "original_sources_retained": True, "automatic_model_retry": False,
            "scope": intent["scope"]}


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit("This is an internal supervisor; use python3 -m taskflow execution")
    _worker(Path(sys.argv[2]))
