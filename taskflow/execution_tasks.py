"""Small shared primitives for explicit role tasks on the existing queue.

No role sequencing, business decisions, model loop or automatic retry lives here.
"""
from contextlib import ExitStack, contextmanager
import copy
import fcntl
from pathlib import Path
import re
import shutil
import sys

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution import enqueue_spec, reconcile
from taskflow.execution_runner import _publish, _publish_bytes, _read, _regular, _validate

ACTIVE = frozenset({"claimed", "starting", "running", "collecting", "uncertain"})


def get_task(store, identity: str, kind: str) -> dict:
    record = store.get_flow(identity)
    if record["definition"].get("kind") != kind:
        raise TaskflowError("wrong explicit role task kind: " + identity)
    return record


@contextmanager
def task_lock(directory: Path, *, store=None):
    """Serialize one role task and share the existing source-install barrier."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "role-task.lock"
    paths = []
    if store is not None:
        paths.append((Path(str(store.path) + ".continuation.lock"), fcntl.LOCK_SH))
    paths.append((path, fcntl.LOCK_EX))
    with ExitStack() as stack:
        handles = []
        for selected, mode in paths:
            if selected.is_symlink():
                raise TaskflowError("role task lock must be an ordinary file")
            handle = stack.enter_context(selected.open("a+b"))
            fcntl.flock(handle, mode)
            handles.append(handle)
        try:
            yield
        finally:
            for handle in reversed(handles):
                fcntl.flock(handle, fcntl.LOCK_UN)


def save_task(store, record: dict, state: dict, event: str) -> dict:
    if state == record["state"]:
        return record
    return store.update_flow(record["flow_id"], record["revision"], state, {"kind": event})


def publish_once(path: Path, value: object) -> None:
    if path.exists():
        if canonical(_read(path)) != canonical(value):
            raise TaskflowError("role task original cannot be replaced: " + str(path))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        _publish(path, value)


def publish_copy(path: Path, raw: bytes) -> None:
    if path.exists():
        if _regular(path) != raw:
            raise TaskflowError("role task source cannot be replaced: " + str(path))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        _publish_bytes(path, raw)


def invocation(spec: dict, stage: str, ordinal: int = 0, **extra) -> dict:
    identity = stage + "-" + sha256(canonical([spec["flow_id"], stage, ordinal]))[:24]
    root = Path(spec["directory"]) / "invocations" / (stage + "-" + str(ordinal).zfill(3))
    return {"stage": stage, "ordinal": ordinal, "work_id": identity, "intent_id": identity,
            "root": str(root), "runtime": str(root / "runtime"), "work": str(root / "work"),
            "scratch": str(root / "scratch"), **extra}


def render(value: object, context: dict) -> object:
    if isinstance(value, str):
        def replace(match):
            key = match.group(1)
            if key not in context:
                raise TaskflowError("unknown role runner placeholder: " + key)
            return str(context[key])
        return re.sub(r"\{\{([a-zA-Z_][a-zA-Z_0-9]*)\}\}", replace, value)
    if isinstance(value, list):
        return [render(item, context) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            selected = render(key, context) if isinstance(key, str) else key
            if selected in result:
                raise TaskflowError("role runner keys collide after rendering: " + str(selected))
            result[selected] = render(item, context)
        return result
    return value


def prepare_work(store, record: dict, current: dict, runner: dict, *, evidence=None) -> dict:
    """Prepare one explicitly compiled invocation; never executes a model."""
    spec = record["definition"]
    work = {"work_id": current["work_id"], "intent_id": current["intent_id"],
            "task_id": spec["task_id"], "candidate_id": spec["candidate_id"],
            "role": runner["role"], "policy_id": spec["policy_id"], "purpose": spec["purpose"],
            "dispatch_owner": spec["owner"],
            "evidence": copy.deepcopy(record["evidence"] if evidence is None else evidence)}
    if runner.get("model_slot") is not None:
        work["model_slot"] = runner["model_slot"]
    plan = {"work": work, "runner": runner}
    publish_once(Path(current["root"]) / "spec.json", plan)
    try:
        existing = store.get_work(current["work_id"])
    except TaskflowError:
        existing = None
    if existing is not None:
        expected = {**work, "runtime_path": current["runtime"]}
        for key in ("work_id", "intent_id", "task_id", "candidate_id", "role", "policy_id",
                    "purpose", "dispatch_owner", "runtime_path", "model_slot"):
            if existing.get(key) != expected.get(key):
                raise TaskflowError("existing work differs from the explicit invocation: " + key)
        manifest = _validate(Path(current["runtime"]))
        if (manifest["intent_id"] != current["intent_id"]
                or manifest["request"]["sha256"] != existing["request_digest"]):
            raise TaskflowError("existing work request differs from its locked invocation")
        return {"work": existing, "manifest": manifest}
    return enqueue_spec(store, plan, Path(current["runtime"]), resume_prepared=True, probe_native=True)


def original_attempt(store, current: dict, *, observe: bool = True) -> dict | None:
    attempts = store.list_attempts(current["work_id"])
    if len(attempts) > 1:
        raise TaskflowError("explicit invocation has more than one original attempt")
    if not attempts:
        return None
    attempt = attempts[0]
    if observe and attempt["state"] in ACTIVE:
        return reconcile(store, attempt["attempt_id"], resume_pending=False)
    return attempt


def execution_state(attempt: dict) -> str:
    if attempt["state"] == "uncertain":
        return "unknown"
    if attempt["state"] in ACTIVE:
        return "running"
    return attempt["state"]


def check_native_view(runner: dict, public_root: Path, *, control_root: Path,
                      forbidden: list[Path]) -> None:
    """Restrict model tools to this role's view and actual installed runtimes."""
    if runner.get("mode") != "native_role":
        return
    permissions = runner.get("native_permissions")
    if not isinstance(permissions, dict) or not isinstance(permissions.get("filesystem"), dict):
        raise TaskflowError("native role requires an explicit restrictive filesystem object")
    filesystem = permissions["filesystem"]
    baseline = {":root": "deny", ":minimal": "read", ":tmpdir": "deny", ":slash_tmp": "deny", "/private/tmp": "deny"}
    if any(filesystem.get(key) != value for key, value in baseline.items()):
        raise TaskflowError("native role filesystem must deny undeclared host and temporary paths")
    declared_work = Path(runner["cwd"])
    declared_scratch = Path(permissions.get("scratch", ""))
    if any(not path.is_absolute() or path.resolve() != path
           for path in (public_root, control_root, declared_work, declared_scratch)):
        raise TaskflowError("native role paths must be canonical task-owned ordinary directories")
    public_root, control_root = public_root.resolve(), control_root.resolve()
    if (declared_scratch != declared_work.parent / "scratch"
            or not declared_work.is_relative_to(control_root)):
        raise TaskflowError("native scratch must be this exact invocation's owned scratch")
    if Path(permissions.get("repository", "")).resolve() != public_root:
        raise TaskflowError("native role repository must be its own public-only material view")
    python = Path(permissions.get("python", "")).resolve()
    if python != Path(sys.executable).resolve():
        raise TaskflowError("native role tool Python must be the explicit current management interpreter")
    work, scratch = Path(runner["cwd"]).resolve(), Path(permissions["scratch"]).resolve()
    owned = (public_root, work, scratch, work.parent / "runtime/inputs")
    runtime = {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve()}
    for prefix in tuple(runtime):
        if prefix.parent.name == "Versions" and prefix.parent.parent.name == "Python.framework":
            framework = prefix.parent.parent
            runtime.add(framework.parent.parent if framework.parent.name == "Frameworks" else framework)
    unsafe = {Path("/"), Path.home().resolve(), Path("/private/tmp")}
    runtime -= unsafe
    executable_files = {python, Path(runner["cli"]).resolve()}
    rtk = shutil.which("rtk")
    if rtk:
        executable_files.add(Path(rtk).resolve())
    denied = [path.resolve() for path in forbidden]
    denied.extend((Path.home().resolve() / ".codex", Path(runner["role_session"]["home"]).resolve()))
    for name, access in filesystem.items():
        if not isinstance(name, str) or access not in {"deny", "read", "write"}:
            raise TaskflowError("native filesystem entries must have explicit supported access")
        if name.startswith(":"):
            if name not in baseline:
                raise TaskflowError("native role cannot grant an unknown special filesystem scope")
            continue
        if access == "deny":
            continue
        granted = Path(name).resolve()
        if any(granted == path or granted.is_relative_to(path) or path.is_relative_to(granted) for path in denied):
            raise TaskflowError("native tools can read private/control source material")
        if access == "write":
            if granted not in (work, scratch):
                raise TaskflowError("native role can write only its own work and scratch")
            continue
        if granted == control_root or control_root.is_relative_to(granted):
            raise TaskflowError("native tools cannot read the task's control directory")
        if any(granted == path or granted.is_relative_to(path) for path in owned):
            continue
        if granted in executable_files or any(granted == path or granted.is_relative_to(path) for path in runtime):
            continue
        raise TaskflowError("native read grant is outside the declared role view and actual runtime")
    probes = permissions.get("probe_sources")
    if (not isinstance(probes, list) or not probes or any(not Path(name).resolve().is_relative_to(public_root) for name in probes)
            or not Path(permissions.get("readonly_probe", "")).resolve().is_relative_to(public_root)):
        raise TaskflowError("native role probes must use actual public-only view sources")


def brief_task_receipt(record: dict, *, database: Path, operation: str) -> dict:
    """Project a model-facing receipt; leave stored and API records untouched."""
    state = record.get("state", {})
    summary = {key: state[key] for key in (
        "status", "execution", "delivery", "outcome", "candidate_id", "quality_gate",
    ) if key in state}
    errors = state.get("errors", [])
    summary["error_count"] = len(errors)
    summary["errors"] = [{key: row[key] for key in ("code", "field", "message", "correctable") if key in row}
                         for row in errors[:3]]
    for error in summary["errors"]:
        if isinstance(error.get("message"), str) and len(error["message"]) > 500:
            error["message"] = error["message"][:500] + "… [expand --full]"
    if "results" in state:
        summary["results"] = {stage: {key: result[key] for key in ("execution", "delivery", "outcome") if key in result}
                              for stage, result in state["results"].items()}
    if "invocation" in state:
        invocations = [state["invocation"]]
    elif isinstance(state.get("invocations"), list):
        entries = state["invocations"]
        invocations = [entries[state.get("current_index", 0)]] if entries else []
    else:
        invocations = [entries[-1] for entries in state.get("invocations", {}).values() if entries]
    current = [{key: item[key] for key in ("work_id", "attempt_id", "sid", "runtime") if key in item}
               for item in invocations]
    for source, target in zip(invocations, current):
        archive = source.get("archive")
        if archive:
            target["archive"] = archive.get("path") if isinstance(archive, dict) else archive
    originals = {key: state[key]["path"] for key in (
        "receipt", "diff", "author_record", "facts_seal", "score_receipt",
    ) if isinstance(state.get(key), dict) and "path" in state[key]}
    identity = record.get("flow_id")
    return {"view": "brief", "flow_id": identity, "revision": record.get("revision"),
        "state": summary, "invocations": current, "originals": originals,
        "details": {"argv": ["python3", "-m", "taskflow", "execution", "--db", str(database),
                              operation, "status", identity, "--full"],
                    "note": "Full machine record and all original bytes remain available; no model call."}}
