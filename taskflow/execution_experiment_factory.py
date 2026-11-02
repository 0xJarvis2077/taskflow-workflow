"""Register a packet once and detach only its ordinary existing consumers."""

from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterator

from taskflow.contracts import TaskflowError, canonical, sha256, validate_id
from taskflow.execution_flow import _identity, _publish_once
from taskflow.execution_lifecycle import _lifecycle_lock, register_lifecycle
from taskflow.execution_runner import _identity as _process_identity
from taskflow.execution_runner import _publish, _read
from taskflow.execution_store import evidence_ref, validate_evidence


@contextmanager
def _bootstrap_lock(directory: Path) -> Iterator[None]:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "consumer-bootstrap.lock").open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskflowError("another bootstrap owns this lifecycle; no duplicate launch") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _process_status(identity: dict) -> tuple[str, str]:
    if not isinstance(identity, dict):
        return "unknown", "original consumer PID identity is missing"
    pid = identity.get("pid")
    if type(pid) is not int or pid <= 0:
        return "unknown", "original consumer PID is missing"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "dead", "original consumer PID no longer exists"
    except PermissionError:
        return "unknown", "original consumer PID cannot be observed"
    proc = Path(f"/proc/{pid}/stat")
    if proc.exists():
        try:
            if proc.read_text().split(") ", 1)[1].split()[0] in ("Z", "X"):
                return "dead", "original consumer is a terminated unreaped process"
        except (OSError, IndexError):
            return "unknown", "original consumer process state is unreadable"
    observed = _process_identity(pid)
    if not identity.get("start_identity") or not observed.get("start_identity"):
        return "unknown", "original consumer start identity is not proven"
    if identity["start_identity"] != observed["start_identity"]:
        return "dead", "original consumer PID now belongs to a different process"
    return "alive", "original PID and start identity match"


def _source_refs(repository: Path) -> list[dict]:
    return [evidence_ref(path, "consumer bootstrap source inventory")
            for path in sorted((repository / "taskflow").glob("*.py"))]


def _exclusive_intent(path: Path, value: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(canonical(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def launch_lifecycle_consumer(store: object, record: dict, repository: Path, *,
                              evidence: list[dict] | None = None) -> dict:
    """Launch one registered lifecycle once; uncertain launches never redraw."""
    repository = Path(repository).resolve()
    if repository != Path(__file__).resolve().parents[1]:
        raise TaskflowError("consumer repository differs from the running execution package")
    refs = validate_evidence([*record["evidence"], *(evidence or [])])
    current = store.get_flow(record["flow_id"])
    if current["definition_digest"] != record["definition_digest"]:
        raise TaskflowError("consumer record differs from its registered immutable definition")
    if current["definition"].get("kind") not in ("experiment_v1", "author_quality_v1", "evaluation_tail_v1"):
        raise TaskflowError("consumer launch requires an existing execution lifecycle")
    directory = Path(current["definition"]["directory"])
    base = {"flow_id": current["flow_id"], "revision": current["revision"],
            "lifecycle_status": current["state"]["status"], "consumer_spawned": False}
    if current["state"]["status"] in ("accepted", "exception"):
        receipt = current["state"].get("receipt")
        if receipt:
            return {**base, "status": "terminal_reused", "receipt": evidence_ref(Path(receipt), "existing terminal lifecycle result")}
        return {**base, "status": "attention", "reason": "terminal lifecycle has no received result yet; no relaunch"}
    with _bootstrap_lock(directory):
        intent_path, receipt_path = directory / "consumer-launch-intent.json", directory / "consumer-launch.json"
        if intent_path.exists():
            intent = _read(intent_path)
            if (not isinstance(intent, dict) or intent.get("flow_id") != current["flow_id"]
                    or intent.get("definition_digest") != current["definition_digest"]
                    or intent.get("database") != str(store.path)):
                return {**base, "status": "attention", "reason": "original consumer launch intent binding differs"}
            if not receipt_path.exists():
                return {**base, "status": "attention", "reason": "original launch intent has no PID receipt; no relaunch",
                        "launch_intent": evidence_ref(intent_path, "original launch intent retained")}
            launched = _read(receipt_path)
            expected = evidence_ref(intent_path, "original consumer launch intent")
            if not isinstance(launched, dict) or launched.get("launch_intent") != expected:
                return {**base, "status": "attention", "reason": "original consumer PID receipt does not bind its launch intent"}
            validate_evidence(intent["evidence"])
            validate_evidence(intent["source_refs"])
            status, reason = _process_status(launched.get("process", {}))
            return {**base, "status": "alive_reused" if status == "alive" else "attention",
                    "reason": reason, "process_status": status, "process": launched.get("process"),
                    "launch_receipt": evidence_ref(receipt_path, "original consumer PID receipt")}
        try:
            with _lifecycle_lock(store, current["flow_id"]):
                pass
        except TaskflowError:
            return {**base, "status": "attention", "reason": "existing lifecycle consumer owns its lock; no second launch"}
        stdout_path, stderr_path = directory / "consumer.stdout.log", directory / "consumer.stderr.log"
        argv = [str(Path(sys.executable).resolve()), "-m", "taskflow", "execution", "--db", str(store.path),
                "lifecycle-run", "--flow-id", current["flow_id"], "--allow-model"]
        intent = {"schema_version": 1, "flow_id": current["flow_id"], "definition_digest": current["definition_digest"],
                  "database": str(store.path), "argv": argv, "cwd": str(repository), "allow_model": True,
                  "created_at": datetime.now(timezone.utc).isoformat(), "launcher_pid": os.getpid(),
                  "stdout": str(stdout_path), "stderr": str(stderr_path), "evidence": refs,
                  "source_refs": _source_refs(repository)}
        _exclusive_intent(intent_path, intent)
        process = None
        try:
            with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
                process = subprocess.Popen(argv, cwd=repository, stdin=subprocess.DEVNULL,
                                           stdout=stdout, stderr=stderr, start_new_session=True)
            identity = _process_identity(process.pid)
            try:
                pgid = os.getpgid(process.pid)
            except ProcessLookupError:
                pgid = None
            receipt = {"schema_version": 1, "flow_id": current["flow_id"],
                       "launch_intent": evidence_ref(intent_path, "original consumer launch intent"),
                       "process": identity, "pgid": pgid, "launcher_pid": os.getpid(),
                       "stdout": str(stdout_path), "stderr": str(stderr_path)}
            _publish(receipt_path, receipt)
        except (OSError, ValueError) as exc:
            _publish_once(directory / "consumer-launch-error.json", {"flow_id": current["flow_id"],
                          "reason": str(exc), "spawned_pid": process.pid if process is not None else None,
                          "original_intent_retained": True, "automatic_retry": False})
            return {**base, "status": "attention", "consumer_spawned": process is not None,
                    "spawned_pid": process.pid if process is not None else None,
                    "reason": "consumer launch could not be fully recorded: " + str(exc),
                    "launch_intent": evidence_ref(intent_path, "original launch intent retained")}
        return {**base, "status": "started" if identity.get("start_identity") else "attention",
                "reason": "consumer spawned" if identity.get("start_identity") else "consumer spawned but start identity is unproven; no automatic relaunch",
                "consumer_spawned": True, "process": identity, "pgid": pgid,
                "launch_receipt": evidence_ref(receipt_path, "original consumer PID receipt")}


def start_experiments_from_packet(store: object, *, packet_ref: dict, factory_config_ref: dict,
                                  allow_model: bool = False) -> dict:
    """Register each exact authorized item, then optionally start ordinary consumers."""
    from taskflow.execution_batch import policy_setting, validate_batch_policy

    if type(allow_model) is not bool:
        raise TaskflowError("allow_model must be an explicit boolean")
    packet_ref, config_ref = validate_evidence([packet_ref, factory_config_ref])
    packet, config = _read(Path(packet_ref["path"])), _read(Path(config_ref["path"]))
    if type(config.get("schema_version")) is not int or config["schema_version"] != 1:
        raise TaskflowError("unsupported experiment factory configuration")
    policy = validate_id(config.get("policy_id"), "policy_id")
    registered = store.get_policy(policy)["config"]
    snapshot = validate_batch_policy(registered)
    if snapshot is not None:
        if canonical(config.get("batch_snapshot")) != canonical(registered["batch_snapshot"]):
            raise TaskflowError("experiment factory differs from the registered confirmed batch snapshot")
        if packet.get("batch_id") != snapshot["batch_id"]:
            raise TaskflowError("experiment packet differs from its explicit confirmed batch")
        source = validate_evidence([config.get("policy_record_ref")])[0]
        registration = _read(Path(source["path"]))
        if (registration.get("operation") != "register_policy"
                or registration.get("arguments", {}).get("policy_id") != policy
                or canonical(registration.get("arguments", {}).get("config")) != canonical(registered)):
            raise TaskflowError("experiment factory differs from its original registered batch policy")
    elif "batch_snapshot" in config:
        raise TaskflowError("batch experiment factory requires its registered confirmed policy")
    paths = {}
    for key in ("repository", "lifecycle_root", "formal_runtime_root", "publication_root"):
        path = Path(config.get(key, ""))
        if not path.is_absolute():
            raise TaskflowError("experiment factory path must be absolute: " + key)
        paths[key] = path
    common = config.get("technical")
    if (not isinstance(common, dict) or common.get("real_repository") != str(paths["repository"])
            or not isinstance(common.get("formal_config_base"), dict) or not isinstance(common.get("review_runner"), dict)):
        raise TaskflowError("experiment factory needs the shared formal/review configuration and exact repository")
    items = packet.get("items")
    if not isinstance(items, list) or not items:
        raise TaskflowError("experiment factory requires explicit authorized packet items")
    seen, definitions = set(), []
    for item in items:
        if not isinstance(item, dict):
            raise TaskflowError("experiment factory item must be an object")
        task_id = validate_id(item.get("task_id"), "task_id")
        if snapshot is not None:
            for low_role, slot, carrier in (("solver", "solver", common["formal_config_base"]),
                                            ("review-facts", "reviewer", common["review_runner"])):
                setting = policy_setting(registered, low_role, slot, task_id)
                if (any(carrier.get(key) != setting[key] for key in ("cli", "model", "reasoning_effort", "timeout_seconds"))
                        or any(carrier.get(key, "explicit" if key == "reasoning_selection" else None) != setting[key]
                               for key in ("reasoning_selection", "reasoning_context"))):
                    raise TaskflowError("experiment factory runtime differs from its confirmed " + slot + " slot")
        source_id = validate_id(item.get("source_author_quality_flow"), "source_author_quality_flow")
        selector = {"task_id": task_id, "source_author_quality_flow": source_id}
        identity = (task_id, source_id)
        if identity in seen:
            raise TaskflowError("experiment factory packet contains duplicate item selectors")
        seen.add(identity)
        flow_id = _identity(source_id, "experiment-" + sha256(source_id.encode())[:10])
        technical = copy.deepcopy(common)
        technical["formal_config_base"]["authorization"] = packet_ref
        if "local_profile_policy" in technical:
            technical["local_profile_policy"]["authorization"] = packet_ref
        technical.update(formal_runtime_root=str(paths["formal_runtime_root"] / flow_id),
                         publication_root=str(paths["publication_root"] / flow_id / task_id))
        definitions.append({"schema_version": 1, "kind": "experiment_v1", "flow_id": flow_id,
            "task_id": task_id, "owner": packet.get("controller"), "policy_id": policy,
            "authorization": packet_ref, "business_packet_ref": packet_ref, "factory_config_ref": config_ref,
            "directory": str(paths["lifecycle_root"] / flow_id), "source_author_flow_id": source_id,
            "business_packet_item": selector, "technical": technical})
    results = []
    for definition in definitions:
        try:
            record = register_lifecycle(store, definition, [packet_ref, config_ref])
            if allow_model:
                result = launch_lifecycle_consumer(store, record, paths["repository"], evidence=[config_ref])
            else:
                result = {"flow_id": record["flow_id"], "revision": record["revision"],
                          "status": "registered", "consumer_spawned": False}
            results.append({"task_id": definition["task_id"], **result})
        except (TaskflowError, OSError, ValueError, KeyError, TypeError) as exc:
            results.append({"task_id": definition["task_id"], "flow_id": definition["flow_id"],
                            "status": "attention", "consumer_spawned": False, "reason": str(exc)})
    return {"schema_version": 1, "packet_ref": packet_ref, "factory_config_ref": config_ref,
            "allow_model": allow_model, "items": results,
            "consumers_started": sum(row["consumer_spawned"] for row in results),
            "attention": any(row["status"] == "attention" for row in results)}
