"""Prepare, probe and explicitly execute one isolated public quality solution.

Only prepare imports taskflow. The copied runner is standalone and locked before
native probes or inference. Neither preparation nor probing calls a model.
"""

import argparse
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import signal
import stat
import subprocess
import sys
import time
import uuid


PROFILE = "public-quality"
PUBLIC_ROLES = frozenset({"prompt", "input", "template"})
MAX_FILE = 16 * 1024 * 1024
MAX_TOTAL = 128 * 1024 * 1024
MAX_FILES = 2000
MAX_DEPENDENCY = 1024 * 1024 * 1024
NEUTRAL_RTK_SHA256 = "49c368c302c6f63d089f4c1085b242fc50fe22ce5bc34dada6478083000e7c6f"
REQUEST = (
    "请仅依据当前目录 package.json 所列公开题面和附件，完整完成题面要求的业务成果。"
    "将题面要求的全部交付保存到 deliverables/，临时分析文件可写入 {scratch}。"
    "公开输入只读；可使用正常文件、shell、Python、SQL和分析工具。"
    "另外在 deliverables/quality-note.md 简要记录实际读取范围、发现的题设矛盾、"
    "缺失条件或无法确定的事项；没有发现可如实写无。不要评分或预测难度。"
    "不要求披露隐藏思维链。生产变更和真实对外消息不在本次离线委托内。"
    "最终答复列出交付路径。\n"
)


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def metadata(data: bytes) -> dict:
    return {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def relative(value: str) -> str:
    if (not value or "\x00" in value or "\\" in value
            or PurePosixPath(value).is_absolute()
            or any(part in ("", ".", "..") for part in value.split("/"))):
        raise ValueError("Unsafe relative member")
    return value


def read_regular(root: Path, member: str, limit: int = MAX_FILE) -> bytes:
    """Read a bounded ordinary file through no-follow directory descriptors."""
    parts = PurePosixPath(relative(member)).parts
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        child = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                        dir_fd=descriptor)
        with os.fdopen(child, "rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
                raise ValueError("Expected bounded ordinary file")
            data = handle.read(limit + 1)
            after = os.fstat(handle.fileno())
            if (len(data) > limit or before.st_size != len(data)
                    or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise ValueError("File changed during bounded read")
            return data
    finally:
        os.close(descriptor)


def info(path: Path) -> dict:
    """Hash trusted dependencies without loading a large executable into RAM."""
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=parent)
        with os.fdopen(descriptor, "rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_DEPENDENCY:
                raise ValueError("Expected bounded ordinary dependency")
            digest, count = hashlib.sha256(), 0
            while chunk := handle.read(1024 * 1024):
                count += len(chunk)
                if count > MAX_DEPENDENCY:
                    raise ValueError("Dependency exceeds lock limit")
                digest.update(chunk)
            after = os.fstat(handle.fileno())
            if (count != before.st_size or (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise ValueError("Dependency changed during hash")
            return {"sha256": digest.hexdigest(), "size": count}
    finally:
        os.close(parent)


def expect(path: Path, expected: dict) -> None:
    if info(path) != {key: expected[key] for key in ("sha256", "size")}:
        raise ValueError(f"Locked bytes changed: {path}")


def load(path: Path) -> dict:
    value = json.loads(read_regular(path.parent, path.name).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("Expected JSON object")
    return value


def save(path: Path, value: object) -> None:
    with path.open("xb") as handle:
        handle.write(canonical(value) + b"\n")


def file_tree(root: Path, excluded: set[str] | None = None) -> dict:
    """Bound traversal without following directory or file symlinks."""
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Expected ordinary directory")
    rows, total = {}, 0
    for parent, directories, filenames in os.walk(root, followlinks=False):
        parent_path = Path(parent)
        for name in directories + filenames:
            if (parent_path / name).is_symlink():
                raise ValueError("Symlink in captured tree")
        directories[:] = [name for name in directories if
                           (parent_path / name).relative_to(root).as_posix()
                           not in (excluded or set())]
        for name in filenames:
            member = (parent_path / name).relative_to(root).as_posix()
            data = read_regular(root, member)
            total += len(data)
            if len(rows) >= MAX_FILES or total > MAX_TOTAL:
                raise ValueError("Tree exceeds capture limits")
            rows[member] = metadata(data)
    return rows


def inline_table(values: dict[str, str]) -> str:
    return "{" + ",".join(json.dumps(key) + "=" + json.dumps(value)
                          for key, value in values.items()) + "}"


def instruction_context(root: Path, codex_home: Path,
                        neutral_rtk_sha256: str) -> dict:
    """Track absence as well as bytes of files the CLI could preload."""
    candidates = {directory / name for directory in
                  [root / "work", root, *root.parents, codex_home]
                  for name in ("AGENTS.md", "AGENTS.override.md")}
    candidates.add(codex_home / "RTK.md")
    context = {}
    for path in sorted(candidates):
        if path.is_symlink():
            raise ValueError("Automatic instruction path is a symlink")
        if not path.exists():
            context[str(path)] = None
            continue
        observed = info(path)
        if path == codex_home / "RTK.md":
            if observed["sha256"] != neutral_rtk_sha256:
                raise ValueError("RTK content does not match the explicitly approved neutral digest")
        elif path.parent == codex_home:
            text = read_regular(codex_home, path.name).decode("utf-8").strip()
            if text and text != "@" + str(codex_home / "RTK.md"):
                raise ValueError("Global AGENTS must be absent or a neutral RTK reference")
        else:
            raise ValueError("Unexpected ancestor agent instructions")
        context[str(path)] = observed
    return context


def requested_selection(config: dict) -> tuple[str, str]:
    """Require literal selections; never fall back to a client default."""
    values = tuple(config.get(key) for key in ("model", "reasoning_effort"))
    if any(not isinstance(value, str) or not value.strip() or value != value.strip()
           or value.lower() in ("auto", "default") for value in values):
        raise ValueError("Explicit nonempty model and reasoning effort are required; no defaults")
    return values


def capture_cli_evidence(root: Path, cli: str) -> dict:
    """Retain local version/catalog evidence, including unsuccessful commands."""
    evidence = {"model_calls": 0, "commands": []}
    for suffix in (["--version"], ["--no-daemon", "debug", "models", "--bundled"]):
        argv = [cli, *suffix]
        row = {"argv": argv}
        try:
            result = subprocess.run(argv, capture_output=True, text=True,
                                    timeout=30, check=False)
            row.update(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        except (OSError, subprocess.TimeoutExpired) as error:
            row["exception"] = type(error).__name__
        evidence["commands"].append(row)
    save(root / "cli-evidence.json", evidence)
    return evidence


def selection_metadata(config: dict, evidence: dict) -> tuple[dict, str]:
    """Validate against this CLI's bundled catalog, not an inferred identity."""
    model, effort = requested_selection(config)
    rows = evidence.get("commands", [])
    expected = [[config["cli"], "--version"],
                [config["cli"], "--no-daemon", "debug", "models", "--bundled"]]
    if (evidence.get("model_calls") != 0 or len(rows) != 2
            or any(row.get("argv") != argv or row.get("returncode") != 0
                   for row, argv in zip(rows, expected))
            or not isinstance(rows[0].get("stdout"), str) or not rows[0]["stdout"].strip()):
        raise ValueError("CLI version and bundled catalog evidence must succeed")
    try:
        catalog = json.loads(rows[1]["stdout"])
        matches = [entry for entry in catalog["models"] if entry["slug"] == model]
        supported = [entry["effort"] for entry in matches[0]["supported_reasoning_levels"]]
    except (ValueError, TypeError, KeyError, IndexError) as error:
        raise ValueError("Model is absent from a usable local bundled catalog") from error
    if len(matches) != 1 or effort not in supported:
        raise ValueError("Model/reasoning effort pair is not supported by the local bundled catalog")
    return ({"requested": model, "reasoning_effort": effort,
             "selection": "Explicit -m and -c model_reasoning_effort; local bundled catalog checked.",
             "client_resolved": None,
             "client_resolution_scope": (
                 "Unavailable: no no-inference dump of the same exec's resolved "
                 "configuration; catalog membership is not request parsing."),
             "served": None, "revision": None}, rows[0]["stdout"].strip())


def commands(root: Path, config: dict) -> tuple[list[str], list[str]]:
    """Use the same native profile for shell probes and generated tool actions."""
    model, effort = requested_selection(config)
    work, scratch = root / "work", root / "scratch"
    filesystem = {
        ":root": "deny", ":minimal": "read", ":tmpdir": "deny",
        ":slash_tmp": "deny", "/private/tmp": "deny",
        config["dependencies"]: "read", config["rtk"]: "read",
        str(work): "read", str(work / "deliverables"): "write",
        str(scratch): "write", config["repository"]: "deny",
        config["codex_home"]: "deny",
    }
    if config.get("neutral_rtk"):
        filesystem[config["neutral_rtk"]] = "read"
    path = ":".join([str(Path(config["rtk"]).parent),
                     str(Path(config["python"]).parent),
                     str(Path(config["dependencies"]) / "node/bin"),
                     "/usr/bin", "/bin", "/usr/sbin", "/sbin"])
    settings = [
        f'default_permissions="{PROFILE}"',
        f"permissions.{PROFILE}.filesystem=" + inline_table(filesystem),
        f"permissions.{PROFILE}.network.enabled=true",
        f'permissions.{PROFILE}.network.domains={{"*"="allow"}}',
        "features.network_proxy=true", 'shell_environment_policy.inherit="core"',
        "shell_environment_policy.ignore_default_excludes=false",
        "shell_environment_policy.set=" + inline_table({
            "PATH": path, "TMPDIR": str(scratch), "TMPPREFIX": str(scratch / "zsh"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }),
        'web_search="live"', "apps._default.enabled=false", "agents.enabled=false",
        "allow_login_shell=false",
    ]
    options = [part for setting in settings for part in ("-c", setting)]
    for feature in ("apps", "plugins", "hooks", "memories", "computer_use",
                    "browser_use", "browser_use_external", "in_app_browser",
                    "multi_agent", "skill_search"):
        options.extend(["--disable", feature])
    options.extend(["--enable", "skip_host_skill_discovery"])
    execution = [config["cli"], "--no-daemon", "-a", "never", *options,
                 "-C", str(work), "exec", "--ignore-user-config", "--ignore-rules",
                 "--ephemeral", "--skip-git-repo-check", "-m", model,
                 "-c", "model_reasoning_effort=" + json.dumps(effort), "--json", "-"]
    sandbox = [config["cli"], "sandbox", "-P", PROFILE,
               "--include-managed-config", *options, "-C", str(work)]
    return execution, sandbox


def prepare(solver: Path, snapshot: Path, root: Path, config: dict,
            denied: list[Path] | None = None, timeout: int = 3600,
            neutral_rtk_sha256: str = NEUTRAL_RTK_SHA256) -> dict:
    """Bind a verified frozen solver export, copying no teacher content."""
    from taskflow.packages import verify_export, verify_snapshot

    if solver.is_symlink() or snapshot.is_symlink():
        raise ValueError("Symlink input/runtime roots are not allowed")
    solver, snapshot = solver.resolve(), snapshot.resolve()
    package = verify_export(solver)
    frozen = verify_snapshot(snapshot)
    public = [{key: entry[key] for key in ("id", "path", "role", "sha256", "size")}
              for entry in frozen["files"] if entry["role"] in PUBLIC_ROLES]
    if package["audience"] != "solver":
        raise ValueError("Only solver exports are accepted")
    if (package["files"] != public or any(package[key] != frozen[key]
                                        for key in ("task_id", "version_id", "input_digest"))):
        raise ValueError("Solver export does not match the frozen version")
    inputs = {member: read_regular(solver, member)
              for member in [entry["path"] for entry in public] + ["package.json"]}
    binding = {"kind": "frozen", "task_id": package["task_id"],
               "version_id": package["version_id"], "input_digest": package["input_digest"],
               "files": [{"path": str(snapshot / "manifest.json"), **info(snapshot / "manifest.json")},
                         *[{"path": str(solver / member), **metadata(data)}
                           for member, data in inputs.items()]]}
    private_paths = [snapshot / entry["path"] for entry in frozen["files"]
                     if entry["role"] not in PUBLIC_ROLES]
    return prepare_public(package, inputs, binding, private_paths, root, config,
                          denied, timeout, neutral_rtk_sha256)


def prepare_draft(task_root: Path, root: Path, config: dict,
                  denied: list[Path] | None = None, timeout: int = 3600,
                  neutral_rtk_sha256: str = NEUTRAL_RTK_SHA256) -> dict:
    """Capture a structural draft candidate without requiring passed reviews."""
    from taskflow.contracts import parse_json, safe_read
    from taskflow.packages import check_task

    if task_root.is_symlink():
        raise ValueError("Symlink task root is not allowed")
    task_root = task_root.resolve()
    before = check_task(task_root, write_report=False)
    if before["status"] != "passed":
        raise ValueError("Draft structural capture failed; see taskflow check errors")
    raw = safe_read(task_root, "task.json")
    task = parse_json(raw, "task.json")
    captured = {entry["path"]: safe_read(task_root, entry["path"])
                for entry in task["files"]}
    all_metadata = [{**entry, **metadata(captured[entry["path"]])} for entry in task["files"]]
    public = [{key: entry[key] for key in ("id", "path", "role", "size", "sha256")}
              for entry in all_metadata if entry["role"] in PUBLIC_ROLES]
    content_digest = hashlib.sha256(canonical({"task_sha256": metadata(raw)["sha256"],
                                               "files": all_metadata})).hexdigest()
    input_digest = hashlib.sha256(canonical(public)).hexdigest()
    after = check_task(task_root, write_report=False)
    if (safe_read(task_root, "task.json") != raw or after["status"] != "passed"
            or any(before.get(key) != after.get(key) or after.get(key) != expected
                   for key, expected in (("content_digest", content_digest),
                                         ("input_digest", input_digest)))):
        raise ValueError("Draft changed between structural capture and public copy")
    candidate_id = "candidate-" + input_digest[:16]
    package = {"schema_version": 1, "audience": "solver", "task_id": task["task_id"],
               "version_id": candidate_id, "input_digest": input_digest, "files": public}
    inputs = {entry["path"]: captured[entry["path"]] for entry in public}
    inputs["package.json"] = canonical(package) + b"\n"
    binding = {"kind": "draft_candidate", "task_id": task["task_id"],
               "candidate_id": candidate_id, "content_digest": content_digest,
               "input_digest": input_digest, "reviews_required_for_preparation": False,
               "files": [{"path": str(task_root / "task.json"), **metadata(raw)},
                         *[{"path": str(task_root / entry["path"]),
                            **metadata(captured[entry["path"]])} for entry in task["files"]]],
               "scope": "Candidate source freshness includes declared private bytes; copied input binding includes only public bytes."}
    private_paths = [task_root / entry["path"] for entry in task["files"]
                     if entry["role"] not in PUBLIC_ROLES]
    return prepare_public(package, inputs, binding, private_paths, root, config,
                          denied, timeout, neutral_rtk_sha256)


def source_freshness(binding: dict) -> dict:
    """Report current source drift separately from the copied model input."""
    changed = []
    for entry in binding["files"]:
        try:
            expect(Path(entry["path"]), entry)
        except (OSError, ValueError):
            changed.append(entry["path"])
    return {"state": "stale" if changed else "matched", "source_kind": binding["kind"],
            "changed": changed,
            "scope": "Source freshness only; stale source does not imply the copied public input changed."}


def prepare_public(package: dict, inputs: dict[str, bytes], binding: dict,
                   private_paths: list[Path], root: Path, config: dict,
                   denied: list[Path] | None, timeout: int,
                   neutral_rtk_sha256: str) -> dict:
    """Own the common copy, permissions and lock policy for both source modes."""
    model, effort = requested_selection(config)
    if root.is_symlink():
        raise ValueError("Symlink runtime root is not allowed")
    root = root.parent.resolve() / root.name
    repository = Path(config["repository"]).resolve()
    if root.is_relative_to(repository) or repository.is_relative_to(root):
        raise ValueError("Runtime must be outside the repository")
    if not 1 <= timeout <= 3600:
        raise ValueError("Timeout must be 1..3600 seconds")
    config = {key: str(Path(config[key]).resolve()) for key in
              ("cli", "python", "dependencies", "rtk", "repository")}
    config.update(model=model, reasoning_effort=effort)
    config["repository"] = str(repository)
    config["codex_home"] = str(Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).resolve())
    protected = [repository, root, Path(config["codex_home"])]
    for key in ("dependencies", "rtk", "python", "cli"):
        value = Path(config[key])
        if (not value.exists() or value == Path("/")
                or any(value.is_relative_to(path) or path.is_relative_to(value)
                       for path in protected)):
            raise ValueError(f"Unsafe or missing runtime dependency: {key}")
    public = package["files"]
    for entry in public:
        parts = PurePosixPath(entry["path"]).parts
        if (parts[-1] in ("AGENTS.md", "AGENTS.override.md")
                or any(part in (".git", ".codex", ".agents") for part in parts)):
            raise ValueError("Public input uses an automatic instruction/configuration name")
    if source_freshness(binding)["state"] != "matched":
        raise ValueError("Source changed before isolated preparation")
    codex_home = Path(config["codex_home"])
    global_rules = codex_home / "AGENTS.md"
    neutral_rtk = codex_home / "RTK.md"
    config["neutral_rtk"] = str(neutral_rtk) if neutral_rtk.is_file() else None
    instructions = instruction_context(root, codex_home, neutral_rtk_sha256)
    root.mkdir()
    work = root / "work"
    for directory in (work, work / "deliverables", root / "scratch", root / "control"):
        directory.mkdir()
    evidence = capture_cli_evidence(root, config["cli"])
    model_metadata, cli_version = selection_metadata(config, evidence)
    for member, data in inputs.items():
        relative(member)
        if len(data) > MAX_FILE:
            raise ValueError("Public member exceeds capture limit")
        target = work / member
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as handle:
            handle.write(data)
        # Keep POSIX owner-write enabled: later O_RDWR refusal must come from
        # native policy, not a chmod-only substitute for isolation.
        target.chmod(0o644)
    (root / "control/denied.txt").write_text("NEUTRAL_CONTROL\n", encoding="utf-8")
    request = REQUEST.format(scratch=root / "scratch").encode("utf-8")
    (root / "request.md").write_bytes(request)
    (root / "runner.py").write_bytes(Path(__file__).read_bytes())
    actual_denied = list(private_paths)
    actual_denied += [repository, codex_home, root / "control/denied.txt"]
    actual_denied += [path for path in (repository / "knowledge/index.md",
                                       codex_home / "sessions", codex_home / "config.toml")
                      if path.exists()]
    old_run = next(repository.glob("tasks/*/runs/*/run.json"), None)
    if old_run is not None:
        actual_denied.append(old_run)
    actual_denied += denied or []
    denied_specs = []
    for path in dict.fromkeys(actual_denied):
        if path.is_symlink() or not path.exists():
            raise ValueError("Deny probes require existing non-symlink targets")
        denied_specs.append({"path": str(path.resolve()),
                             "kind": "directory" if path.is_dir() else "file"})
    argv, sandbox = commands(root, config)
    plan = {
        "schema_version": 1, "nature": "isolated_public_quality_not_difficulty_run",
        "created_at": now(), "root": str(root), "config": config,
        "source_binding": binding,
        "public_tree": file_tree(work, {"deliverables"}),
        "request": metadata(request), "argv": argv, "sandbox_argv": sandbox,
        "instruction_context": instructions, "neutral_rtk_sha256": neutral_rtk_sha256,
        "denied": denied_specs, "timeout_seconds": timeout,
        "model": model_metadata,
        "cli": {"version": cli_version, "evidence": info(root / "cli-evidence.json")},
        "automatic_retry": False,
        "limitations": ["Native probes cover tested filesystem operations, not every possible OS bypass.",
                        "Raw CLI stdout is not the full production payload or every original tool response.",
                        "Quality solution evidence is not target-model difficulty or platform approval."],
    }
    save(root / "plan.json", plan)
    save(root / "plan-lock.json", info(root / "plan.json"))
    paths = {root / "runner.py", root / "request.md", root / "plan.json",
             root / "plan-lock.json", root / "cli-evidence.json", Path(sys.executable).resolve(),
             Path(config["cli"]), Path(config["python"]), Path(config["rtk"])}
    paths.update(path for path in (global_rules, neutral_rtk) if path.is_file())
    for module in list(sys.modules.values()):
        name = getattr(module, "__file__", None)
        if name and Path(name).suffix in (".py", ".so") and Path(name).is_file():
            paths.add(Path(name).resolve())
    # Mutable repository modules are preparation-only; execute never imports them.
    paths = {path for path in paths if not path.is_relative_to(repository)}
    source_lock = {"python": str(Path(sys.executable).resolve()),
                   "files": [{"path": str(path), **info(path)} for path in sorted(paths)]}
    save(root / "source-lock.json", source_lock)
    save(root / "prepared-lock.json", info(root / "source-lock.json"))
    if source_freshness(binding)["state"] != "matched":
        raise ValueError("Source changed during isolated preparation")
    return {"prepared": True, "root": str(root), "public_files": len(public),
            "source_kind": binding["kind"], "model_calls": 0,
            "runner": str(root / "runner.py")}


def validate(root: Path, require_probe: bool = True, before_run: bool = True) -> dict:
    """Fail closed on missing evidence, source drift or changed public membership."""
    expect(root / "plan.json", load(root / "plan-lock.json"))
    expect(root / "source-lock.json", load(root / "prepared-lock.json"))
    plan = load(root / "plan.json")
    if plan["root"] != str(root.resolve()):
        raise ValueError("Runtime root moved")
    expect(root / "cli-evidence.json", plan["cli"]["evidence"])
    model_metadata, cli_version = selection_metadata(plan["config"], load(root / "cli-evidence.json"))
    if plan.get("model") != model_metadata or plan["cli"].get("version") != cli_version:
        raise ValueError("Model or CLI metadata differs from explicit request/evidence")
    current_instructions = instruction_context(root, Path(plan["config"]["codex_home"]),
                                               plan["neutral_rtk_sha256"])
    if current_instructions != plan["instruction_context"]:
        raise ValueError("Automatic instruction files appeared, disappeared or changed")
    source_lock = load(root / "source-lock.json")
    if str(Path(sys.executable).resolve()) != source_lock["python"]:
        raise ValueError("Use the locked launcher Python")
    for entry in source_lock["files"]:
        expect(Path(entry["path"]), entry)
    if before_run and source_freshness(plan["source_binding"])["state"] != "matched":
        raise ValueError("Candidate source is stale; copied input may still match its captured version")
    if file_tree(root / "work", {"deliverables"}) != plan["public_tree"]:
        raise ValueError("Public input bytes or membership changed")
    if any(stat.S_IMODE((root / "work" / member).stat().st_mode) != 0o644
           for member in plan["public_tree"]):
        raise ValueError("Public probe targets must retain owner-write POSIX mode")
    if before_run and (file_tree(root / "work/deliverables") or file_tree(root / "scratch")):
        raise ValueError("Output and scratch must start empty")
    if (root / "request.md").read_bytes() != REQUEST.format(scratch=root / "scratch").encode("utf-8"):
        raise ValueError("Request is not the neutral public quality request")
    argv, sandbox = commands(root, plan["config"])
    if plan["argv"] != argv or plan["sandbox_argv"] != sandbox:
        raise ValueError("Command or permissions differ from the supported boundary")
    if require_probe:
        readiness = load(root / "ready.json")
        expect(root / readiness["probe_file"], readiness["probe"])
        report = load(root / readiness["probe_file"])
        if report.get("passed") is not True or report.get("model_calls") != 0:
            raise ValueError("Native probe did not pass")
        expect(root / "source-lock.json", readiness["source_lock"])
    return plan


def probe(root: Path) -> dict:
    """Exercise real native shell/Python permissions without inference."""
    plan = validate(root, require_probe=False)
    if (root / "ready.json").exists() or (root / "claim.json").exists():
        raise ValueError("This runtime is already probed or claimed")
    for entry in plan["denied"]:
        path = Path(entry["path"])
        if path.is_symlink() or not path.exists() or path.is_dir() != (entry["kind"] == "directory"):
            raise ValueError("Deny probe target is absent or changed type")
    link = root / "work/denied-symlink"
    link.symlink_to(root / "control/denied.txt")
    specs = [{"path": str(root / "work" / member), "kind": "file",
              "read": True, "write": False, **entry}
             for member, entry in plan["public_tree"].items()]
    specs += [{**entry, "read": False, "write": False} for entry in plan["denied"]]
    specs.append({"path": str(link), "kind": "file", "read": False, "write": False})
    specs.append({"path": str(root / "work"), "kind": "directory",
                  "read": True, "write": False})
    code = '''import hashlib,json,os,pathlib
rows=[]
for spec in SPECS:
    row={'path':spec['path']}
    try:
        p=pathlib.Path(spec['path'])
        if spec['kind']=='directory':
            next(p.iterdir(),None)
        else:
            with p.open('rb') as handle:
                data=handle.read() if spec['read'] else handle.read(1)
            if spec.get('sha256'): row['sha256']=hashlib.sha256(data).hexdigest()
        row['read']=True
    except OSError as error: row.update(read=False,read_errno=error.errno)
    try:
        if spec['kind']=='file':
            fd=os.open(spec['path'],os.O_RDWR);os.close(fd);row['write']=True
        else:
            target=pathlib.Path(spec['path'])/'_neutral_denied_write_probe'
            with target.open('x') as handle: handle.write('NEUTRAL')
            target.unlink();row['write']=True
    except OSError as error: row.update(write=False,write_errno=error.errno)
    rows.append(row)
for directory in WRITABLE:
    path=pathlib.Path(directory)/'_neutral_probe'
    with path.open('x') as handle: handle.write('NEUTRAL')
    assert path.read_text()=='NEUTRAL';path.unlink()
print(json.dumps(rows))
'''.replace("SPECS", repr(specs)).replace("WRITABLE", repr([
        str(root / "work/deliverables"), str(root / "scratch")]))
    shell = shlex.quote(plan["config"]["python"]) + " -c " + shlex.quote(code)
    argv = [*plan["sandbox_argv"], "--", "/bin/zsh", "-c", shell]
    report = {"created_at": now(), "argv": argv, "specs": specs,
              "model_calls": 0, "passed": False}
    try:
        result = subprocess.run(argv, cwd=root / "work", capture_output=True,
                                text=True, timeout=50, check=False)
        report.update(exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        rows = json.loads(result.stdout) if result.returncode == 0 else []
        report["passed"] = (len(rows) == len(specs) and all(
            row.get("path") == spec["path"] and row.get("read") == spec["read"]
            and row.get("write") == spec["write"]
            and (spec["read"] or row.get("read_errno") in (errno.EPERM, errno.EACCES))
            and (spec["write"] or row.get("write_errno") in (errno.EPERM, errno.EACCES))
            and (not spec.get("sha256") or row.get("sha256") == spec["sha256"])
            for row, spec in zip(rows, specs)))
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        report["exception"] = type(error).__name__
    finally:
        link.unlink()
    name = "native-probe-" + uuid.uuid4().hex + ".json"
    save(root / name, report)
    if not report["passed"]:
        raise ValueError(f"Native permissions probe failed; see {root / name}")
    validate(root, require_probe=False)
    save(root / "ready.json", {"probe_file": name, "probe": info(root / name),
                               "source_lock": info(root / "source-lock.json"),
                               "model_calls": 0})
    return {"probed": True, "checks": len(specs), "model_calls": 0,
            "evidence": str(root / name)}


def snapshot_outputs(root: Path, output: Path) -> dict:
    """Retain final ordinary outputs; refuse symlinks, special or oversized files."""
    files = file_tree(root / "work/deliverables")
    output.mkdir()
    for member, expected in files.items():
        data = read_regular(root / "work/deliverables", member)
        if metadata(data) != expected:
            raise ValueError("Output changed during snapshot")
        target = output / member
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as handle:
            handle.write(data)
    return files


def trace_summary(path: Path) -> tuple[dict, str | None]:
    """Distinguish successful process exit from a completed visible model turn."""
    identifiers, messages, issues, terminal = [], [], [], []
    with path.open("rb") as handle:
        for number, raw in enumerate(handle, 1):
            try:
                event = json.loads(raw)
                if not isinstance(event, dict):
                    raise ValueError("event is not an object")
                kind = event.get("type")
                if kind == "thread.started":
                    identifiers.append(event.get("thread_id"))
                if kind in ("turn.completed", "turn.failed", "error"):
                    terminal.append(kind)
                item = event.get("item", {})
                if (kind == "item.completed" and isinstance(item, dict)
                        and item.get("type") == "agent_message"
                        and isinstance(item.get("text"), str) and item["text"].strip()):
                    messages.append(item["text"])
            except (ValueError, TypeError):
                issues.append({"line": number, "issue": "invalid_json_event"})
    try:
        if len(identifiers) != 1 or str(uuid.UUID(identifiers[0])) != identifiers[0]:
            raise ValueError("Expected one fresh thread identifier")
    except (ValueError, TypeError, AttributeError):
        issues.append({"issue": "not_one_fresh_thread_id"})
    if terminal != ["turn.completed"]:
        issues.append({"issue": "missing_or_failed_completed_turn"})
    if not messages:
        issues.append({"issue": "missing_final_agent_message"})
    return ({"complete": not issues, "thread_id": identifiers[0] if len(identifiers) == 1 else None,
             "terminal_events": terminal, "issues": issues,
             "scope": "Visible CLI events only; no served identity or hidden reasoning inferred."},
            messages[-1] if messages else None)


def execute(root: Path) -> dict:
    """Claim one attempt, preserve raw output and never automatically retry."""
    plan = validate(root)
    save(root / "claim.json", {"claimed_at": now(), "plan": info(root / "plan.json"),
                               "source_lock": info(root / "source-lock.json"),
                               "automatic_retry": False})
    output = root / "run"
    output.mkdir()
    request = read_regular(root, "request.md")
    (output / "request.md").write_bytes(request)
    save(output / "argv.json", {"argv": plan["argv"], "cwd": str(root / "work")})
    attempt = {"nature": plan["nature"], "started_at": now(), "status": "launch_failed",
               "returncode": None, "exceptions": [], "model": plan["model"],
               "cli": plan["cli"],
               "source_kind": plan["source_binding"]["kind"],
               "automatic_retry": False, "limitations": plan["limitations"]}
    started = time.monotonic()
    try:
        with (output / "trace.jsonl").open("xb") as stdout, (output / "stderr.txt").open("xb") as stderr:
            process = subprocess.Popen(plan["argv"], cwd=root / "work", stdin=subprocess.PIPE,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            attempt["status"] = "process_exited"
            try:
                process.communicate(request, timeout=plan["timeout_seconds"])
            except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                attempt["status"] = "timeout" if isinstance(error, subprocess.TimeoutExpired) else "cancelled"
                attempt["exceptions"].append(type(error).__name__)
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=10)
                except ProcessLookupError:
                    pass
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            attempt["returncode"] = process.returncode
    except (OSError, KeyboardInterrupt) as error:
        attempt["exceptions"].append(type(error).__name__)
    finally:
        attempt["ended_at"] = now()
        attempt["elapsed_monotonic_seconds"] = round(time.monotonic() - started, 6)
        try:
            validate(root, before_run=False)
            attempt["input_readback"] = "matched"
        except (OSError, ValueError) as error:
            attempt["input_readback"] = "failed"
            attempt["exceptions"].append(type(error).__name__)
        attempt["source_freshness"] = source_freshness(plan["source_binding"])
        attempt["current_source_eligible"] = (attempt["source_freshness"]["state"] == "matched"
                                               and attempt.get("input_readback") == "matched")
        try:
            attempt["outputs"] = snapshot_outputs(root, output / "artifacts")
        except (OSError, ValueError) as error:
            attempt["output_capture_error"] = type(error).__name__
        summary, answer = trace_summary(output / "trace.jsonl")
        save(output / "trace-summary.json", summary)
        attempt["thread_id"] = summary["thread_id"]
        attempt["visible_turn_complete"] = summary["complete"]
        if answer is not None:
            (output / "answer.md").write_text(answer, encoding="utf-8")
            attempt["answer"] = info(output / "answer.md")
        if attempt["status"] == "process_exited":
            attempt["status"] = ("completed" if attempt["returncode"] == 0 and summary["complete"]
                                 and attempt.get("input_readback") == "matched"
                                 and "output_capture_error" not in attempt else "incomplete")
        save(output / "attempt.json", attempt)
    return attempt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    for action_name, source_names in (("prepare", ("solver", "snapshot")),
                                      ("prepare-draft", ("task",))):
        prepare_parser = actions.add_parser(action_name, help="No inference")
        for name in (*source_names, "output", "cli", "python", "dependencies", "rtk", "repository"):
            prepare_parser.add_argument("--" + name, type=Path, required=True)
        prepare_parser.add_argument("--model", required=True,
                                    help="Explicit model slug in the local CLI bundled catalog")
        prepare_parser.add_argument("--reasoning-effort", required=True,
                                    help="Explicit reasoning level supported by that catalog model")
        prepare_parser.add_argument("--deny-read", type=Path, action="append", default=[])
        prepare_parser.add_argument("--timeout", type=int, default=3600)
        prepare_parser.add_argument("--neutral-rtk-sha256", default=NEUTRAL_RTK_SHA256,
                                    help="Digest of an explicitly inspected neutral RTK policy")
    for name in ("probe", "validate", "execute"):
        action = actions.add_parser(name, help="One explicit model attempt" if name == "execute" else "No inference")
        action.add_argument("root", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action in ("prepare", "prepare-draft"):
            config = {key: str(getattr(args, key)) for key in
                      ("cli", "python", "dependencies", "rtk", "repository")}
            config.update(model=args.model, reasoning_effort=args.reasoning_effort)
            if args.action == "prepare-draft":
                result = prepare_draft(args.task, args.output, config, args.deny_read,
                                       args.timeout, args.neutral_rtk_sha256)
            else:
                result = prepare(args.solver, args.snapshot, args.output, config, args.deny_read,
                                 args.timeout, args.neutral_rtk_sha256)
        else:
            root = args.root.resolve()
            if args.action == "validate":
                plan = validate(root)
                result = {"validated": True, "model_calls": 0,
                          "source_freshness": source_freshness(plan["source_binding"])}
            else:
                result = probe(root) if args.action == "probe" else execute(root)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(json.dumps({"error": type(error).__name__, "message": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    raise SystemExit(main())
