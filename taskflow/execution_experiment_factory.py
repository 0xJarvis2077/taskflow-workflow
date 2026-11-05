"""Register a packet once and detach only its ordinary existing consumers."""

from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Iterator

from taskflow.contracts import TaskflowError, canonical, sha256, validate_id
from taskflow.execution_flow import _identity, _publish_once
from taskflow.execution_lifecycle import KINDS, _lifecycle_lock, register_lifecycle
from taskflow.execution_runner import _identity as _process_identity
from taskflow.execution_runner import _info, _publish, _read
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


def _consumer_argv(store: object, record: dict) -> list[str]:
    argv = [str(Path(sys.executable).resolve()), "-m", "taskflow", "execution", "--db", str(store.path),
            "lifecycle-run", "--flow-id", record["flow_id"]]
    return argv if record["definition"].get("kind") == "original_score_pause_v1" else [*argv, "--allow-model"]


def _consumer_definition(record: dict) -> dict:
    return {"flow_id": record["flow_id"], "definition_digest": record["definition_digest"],
            **{key: record["definition"].get(key) for key in ("kind", "task_id", "owner", "policy_id")}}


def _consumer_launch_chain(directory: Path, current: dict, store: object, repository: Path) -> tuple[Path, Path, dict, dict, int]:
    """Read every retained launch generation; never jump an unrecorded window."""
    root_intent, root_receipt = directory / "consumer-launch-intent.json", directory / "consumer-launch.json"
    generations = directory / "consumer-generations"
    paths = [(root_intent, root_receipt)]
    if generations.exists():
        if generations.is_symlink() or not generations.is_dir():
            raise TaskflowError("consumer generation root is not a controlled directory")
        entries = sorted(generations.iterdir())
        for number, entry in enumerate(entries, 1):
            if entry.name != f"{number:06d}" or entry.is_symlink() or not entry.is_dir():
                raise TaskflowError("consumer generation membership is ambiguous")
            paths.append((entry / "consumer-launch-intent.json", entry / "consumer-launch.json"))
    previous = None
    for generation, (intent_path, receipt_path) in enumerate(paths):
        if not intent_path.exists() or not receipt_path.exists():
            raise TaskflowError("original launch intent has no PID receipt; no relaunch")
        intent, receipt = _read(intent_path), _read(receipt_path)
        argv = _consumer_argv(store, current)
        if (not isinstance(intent, dict) or intent.get("flow_id") != current["flow_id"]
                or intent.get("definition_digest") != current["definition_digest"]
                or intent.get("database") != str(store.path) or intent.get("cwd") != str(repository)
                or intent.get("allow_model") is not ("--allow-model" in argv) or intent.get("argv") != argv
                or intent.get("stdout") != str(intent_path.parent / "consumer.stdout.log")
                or intent.get("stderr") != str(intent_path.parent / "consumer.stderr.log")):
            raise TaskflowError("original consumer launch intent binding differs")
        if (not isinstance(receipt, dict) or receipt.get("flow_id") != current["flow_id"]
                or receipt.get("launch_intent") != evidence_ref(intent_path, "original consumer launch intent")
                or receipt.get("stdout") != intent["stdout"] or receipt.get("stderr") != intent["stderr"]):
            raise TaskflowError("original consumer PID receipt does not bind its launch intent")
        process = receipt.get("process", {})
        if (not isinstance(process, dict) or type(process.get("pid")) is not int or process["pid"] <= 0
                or not isinstance(process.get("start_identity"), str) or not process["start_identity"].strip()):
            raise TaskflowError("consumer original proven start identity is missing; no relaunch")
        refs = validate_evidence(intent["evidence"])
        if not {(ref["path"], ref["sha256"]) for ref in current["evidence"]} <= {(ref["path"], ref["sha256"]) for ref in refs}:
            raise TaskflowError("consumer original business authority is not retained")
        # Source observations describe the code loaded at that earlier launch.
        # A later reviewed publication must not reinterpret them as current bytes.
        inventory = intent.get("source_refs")
        if not isinstance(inventory, list) or not inventory:
            raise TaskflowError("consumer original source inventory is missing")
        historical_paths = set()
        for item in inventory:
            if (not isinstance(item, dict) or not isinstance(item.get("path"), str)
                    or not Path(item["path"]).is_absolute() or ".." in Path(item["path"]).parts
                    or item["path"] in historical_paths or not isinstance(item.get("sha256"), str)
                    or len(item["sha256"]) != 64 or not isinstance(item.get("location"), str) or not item["location"].strip()):
                raise TaskflowError("consumer original historical source inventory is malformed")
            historical_paths.add(item["path"])
        if generation:
            if (intent.get("generation") != generation or intent.get("definition_binding") != _consumer_definition(current)
                    or intent.get("previous_launch") != previous
                    or intent.get("original_launch") != {"intent": evidence_ref(root_intent, "original consumer launch intent"),
                                                         "receipt": evidence_ref(root_receipt, "original consumer PID receipt")}):
                raise TaskflowError("consumer generation does not bind the exact preceding/original launch")
        previous = {"intent": evidence_ref(intent_path, "preceding consumer launch intent"),
                    "receipt": evidence_ref(receipt_path, "preceding consumer PID receipt")}
    return paths[-1][0], paths[-1][1], intent, receipt, len(paths) - 1


def _consumer_recovery(repository: Path, recovery: dict | None) -> dict:
    """Verify an explicit reviewed publication plus the complete current code list."""
    if not isinstance(recovery, dict) or set(recovery) != {"publication", "source_refs"}:
        raise TaskflowError("dead consumer recovery requires explicit publication and current source refs")
    publication = validate_evidence([recovery["publication"]])[0]
    proof = _read(Path(publication["path"]))
    if (not isinstance(proof, dict) or type(proof.get("schema_version")) is not int or proof["schema_version"] != 1
            or proof.get("kind") != "validated_execution_source_publication" or proof.get("accepted") is not True):
        raise TaskflowError("consumer recovery publication is not a validated execution source publication")
    paths = {}
    for name in ("before", "after"):
        rows = proof.get(name)
        if not isinstance(rows, list) or not rows:
            raise TaskflowError("consumer source publication requires before/after byte manifests")
        paths[name] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise TaskflowError("consumer source publication file binding must be an object")
            path = Path(row.get("path", ""))
            if (not path.is_absolute() or ".." in path.parts or any(part.is_symlink() for part in (path, *path.parents))
                    or not path.is_relative_to(repository) or str(path) in paths[name]
                    or type(row.get("size")) is not int or row["size"] < 0
                    or not isinstance(row.get("sha256"), str) or len(row["sha256"]) != 64):
                raise TaskflowError("consumer source publication has an unsafe/ambiguous file binding")
            if name == "after":
                if _info(path) != {key: row[key] for key in ("sha256", "size")}:
                    raise TaskflowError("consumer source publication differs from current published bytes")
            elif "preserved_source" in row:
                preserved = validate_evidence([row["preserved_source"]])[0]
                if _info(Path(preserved["path"])) != {key: row[key] for key in ("sha256", "size")}:
                    raise TaskflowError("consumer source publication preserved old bytes differ")
            paths[name][str(path)] = row
    if not paths["before"].keys() <= paths["after"].keys() or str(Path(__file__).resolve()) not in paths["after"]:
        raise TaskflowError("consumer source publication omits its launcher or drops a prior file")
    publication_evidence = validate_evidence(proof.get("evidence"))
    sources = validate_evidence(recovery["source_refs"])
    expected = _source_refs(repository)
    actual_pairs = [(row["path"], row["sha256"]) for row in sources]
    if len(actual_pairs) != len(set(actual_pairs)) or set(actual_pairs) != {(row["path"], row["sha256"]) for row in expected}:
        raise TaskflowError("consumer recovery source inventory is not the complete exact current taskflow source set")
    for row in sources:
        if "size" in row and (type(row["size"]) is not int or _info(Path(row["path"]))["size"] != row["size"]):
            raise TaskflowError("consumer recovery current source size differs")
    return {"publication": publication, "source_refs": sources, "publication_evidence": publication_evidence}


def launch_lifecycle_consumer(store: object, record: dict, repository: Path, *,
                              evidence: list[dict] | None = None, resume_dead: bool = False,
                              recovery: dict | None = None, resume_normal_wait: bool = False) -> dict:
    """Launch once, or explicitly append a generation for a proven dead consumer."""
    if type(resume_dead) is not bool or (recovery is not None and not resume_dead):
        raise TaskflowError("consumer recovery requires explicit resume_dead=True")
    if type(resume_normal_wait) is not bool or (resume_normal_wait and resume_dead):
        raise TaskflowError("normal wait continuation cannot also be technical recovery")
    repository = Path(repository).resolve()
    if repository != Path(__file__).resolve().parents[1]:
        raise TaskflowError("consumer repository differs from the running execution package")
    refs = validate_evidence([*record["evidence"], *(evidence or [])])
    current = store.get_flow(record["flow_id"])
    if (current["definition_digest"] != record["definition_digest"]
            or current["definition_digest"] != sha256(canonical(current["definition"]))):
        raise TaskflowError("consumer record differs from its registered immutable definition")
    if current["definition"].get("kind") not in KINDS:
        raise TaskflowError("consumer launch requires an existing execution lifecycle")
    if resume_normal_wait:
        from taskflow.execution_rounds import validate_round_plan

        validate_round_plan(store, current["definition"])
    directory = Path(current["definition"]["directory"])
    base = {"flow_id": current["flow_id"], "revision": current["revision"],
            "lifecycle_status": current["state"]["status"], "consumer_spawned": False}
    if current["state"]["status"] in ("accepted", "exception"):
        receipt = current["state"].get("receipt")
        if receipt:
            return {**base, "status": "terminal_reused", "receipt": evidence_ref(Path(receipt), "existing terminal lifecycle result")}
        return {**base, "status": "attention", "reason": "terminal lifecycle has no received result yet; no relaunch"}
    if resume_dead and (current["state"].get("pending_business_scope") is not None
                        or current["state"].get("exception") is not None):
        return {**base, "status": "attention", "reason": "consumer recovery cannot bypass a pending business scope or exception"}
    with _bootstrap_lock(directory):
        intent_path, receipt_path = directory / "consumer-launch-intent.json", directory / "consumer-launch.json"
        generation, previous, original, recovered = 0, None, None, None
        if intent_path.exists():
            try:
                latest_intent, latest_receipt, intent, launched, generation = _consumer_launch_chain(
                    directory, current, store, repository)
            except (TaskflowError, OSError, ValueError, KeyError, TypeError) as error:
                return {**base, "status": "attention", "reason": str(error)}
            identity = launched.get("process", {})
            status, reason = _process_status(identity)
            if not (resume_dead or resume_normal_wait) or status != "dead":
                return {**base, "status": "alive_reused" if status == "alive" else "attention",
                        "reason": reason, "process_status": status, "process": identity,
                        "launch_receipt": evidence_ref(latest_receipt, "original consumer PID receipt")}
            if not isinstance(identity, dict) or not isinstance(identity.get("start_identity"), str) or not identity["start_identity"].strip():
                return {**base, "status": "attention", "reason": "dead consumer has no original proven start identity; no relaunch"}
            if resume_normal_wait:
                from taskflow.execution_rounds import validate_normal_wait_exit

                recovered = validate_normal_wait_exit(store, current, latest_intent, launched, repository)
                refs = validate_evidence([*refs, recovered["normal_wait_exit"]])
            else:
                recovered = _consumer_recovery(repository, recovery)
                refs = validate_evidence([*refs, recovered["publication"], *recovered["publication_evidence"]])
            previous = {"intent": evidence_ref(latest_intent, "preceding consumer launch intent"),
                        "receipt": evidence_ref(latest_receipt, "preceding consumer PID receipt")}
            original = {"intent": evidence_ref(intent_path, "original consumer launch intent"),
                        "receipt": evidence_ref(receipt_path, "original consumer PID receipt")}
            generation += 1
        elif resume_dead or receipt_path.exists() or (directory / "consumer-generations").exists():
            return {**base, "status": "attention", "reason": "dead consumer recovery has no original launch intent; no launch"}
        try:
            with _lifecycle_lock(store, current["flow_id"]):
                if (resume_dead or resume_normal_wait) and canonical(store.get_flow(current["flow_id"])) != canonical(current):
                    return {**base, "status": "attention", "reason": "consumer lifecycle changed during recovery validation"}
        except TaskflowError:
            return {**base, "status": "attention", "reason": "existing lifecycle consumer owns its lock; no second launch"}
        launch_directory = directory
        if generation:
            base = {**base, "generation": generation}
            # The bootstrap lock serializes this exact chain; an empty directory
            # after a crash is an unknown generation, never permission to skip it.
            (directory / "consumer-generations").mkdir(exist_ok=True)
            launch_directory = directory / "consumer-generations" / f"{generation:06d}"
            launch_directory.mkdir()
            intent_path, receipt_path = launch_directory / "consumer-launch-intent.json", launch_directory / "consumer-launch.json"
        stdout_path, stderr_path = launch_directory / "consumer.stdout.log", launch_directory / "consumer.stderr.log"
        argv = _consumer_argv(store, current)
        intent = {"schema_version": 1, "flow_id": current["flow_id"], "definition_digest": current["definition_digest"],
                  "database": str(store.path), "argv": argv, "cwd": str(repository), "allow_model": "--allow-model" in argv,
                  "created_at": datetime.now(timezone.utc).isoformat(), "launcher_pid": os.getpid(),
                  "stdout": str(stdout_path), "stderr": str(stderr_path), "evidence": refs,
                  "source_refs": _source_refs(repository)}
        if resume_normal_wait:
            intent["normal_wait_handshake"] = True
        if generation:
            if not resume_normal_wait and canonical(_consumer_recovery(repository, recovery)) != canonical(recovered):
                raise TaskflowError("consumer recovery source publication changed before launch")
            intent.update(generation=generation, previous_launch=previous, original_launch=original,
                          definition_binding=_consumer_definition(current),
                          recovery=recovered if resume_normal_wait else {
                              key: recovered[key] for key in ("publication", "source_refs")},
                          old_source_refs_are_historical=True)
        _exclusive_intent(intent_path, intent)
        process = None
        try:
            with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
                process = subprocess.Popen(argv, cwd=repository, stdin=subprocess.DEVNULL,
                                           stdout=stdout, stderr=stderr, start_new_session=True)
            identity = _process_identity(process.pid)
            if resume_normal_wait:
                started_path = launch_directory / ("consumer-started-" + str(process.pid) + ".json")
                deadline = time.monotonic() + 5
                while not started_path.exists() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        break
                    time.sleep(0.01)
                if started_path.exists():
                    started = _read(started_path)
                    observed = _process_identity(process.pid)
                    child = started.get("process", {})
                    if (started.get("launch_intent") == evidence_ref(intent_path, "original consumer launch intent")
                            and child.get("pid") == process.pid and child.get("start_identity")
                            and child.get("start_identity") == observed.get("start_identity")):
                        identity = observed
                    else:
                        identity = {"pid": process.pid, "start_identity": None}
                else:
                    identity = {"pid": process.pid, "start_identity": None}
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
            _publish_once(launch_directory / "consumer-launch-error.json", {"flow_id": current["flow_id"],
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
    from taskflow.execution_original_pause import original_pause_grant, prepare_original_pause

    pause_grant = original_pause_grant(packet)
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
        from taskflow.execution_engineering_source import KIND as ENGINEERING_SOURCE

        engineering = item.get("source_kind") == ENGINEERING_SOURCE
        source_field = "source_quality_flow" if engineering else "source_author_quality_flow"
        source_id = validate_id(item.get(source_field), source_field)
        selector = {"task_id": task_id, source_field: source_id}
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
            "directory": str(paths["lifecycle_root"] / flow_id),
            **({"source_flow_id": source_id, "source_kind": ENGINEERING_SOURCE} if engineering
               else {"source_author_flow_id": source_id}),
            "business_packet_item": selector, "technical": technical,
            **({"solver_profile_update": packet["solver_profile_update"]} if packet.get("solver_profile_update") is not None else {})})
        if packet.get("solver_profile_update") is not None:
            from taskflow.execution_solver_profile import source_policy_id
            source_policy_id(store, definitions[-1])
    results = []
    for definition in definitions:
        try:
            record = register_lifecycle(store, definition, [packet_ref, config_ref])
            pause = prepare_original_pause(store, record, paths["repository"]) if pause_grant is not None else None
            if pause is not None and pause["control_consumer"]["status"] == "attention":
                results.append({"task_id": definition["task_id"], "flow_id": record["flow_id"],
                    "status": "attention", "consumer_spawned": False,
                    "reason": "explicit original-pause consumer needs attention before a model consumer starts",
                    "original_pause": pause})
                continue
            if allow_model:
                result = launch_lifecycle_consumer(store, record, paths["repository"], evidence=[config_ref])
            else:
                result = {"flow_id": record["flow_id"], "revision": record["revision"],
                          "status": "registered", "consumer_spawned": False}
            results.append({"task_id": definition["task_id"], **result,
                            **({"original_pause": pause} if pause is not None else {})})
        except (TaskflowError, OSError, ValueError, KeyError, TypeError) as exc:
            results.append({"task_id": definition["task_id"], "flow_id": definition["flow_id"],
                            "status": "attention", "consumer_spawned": False, "reason": str(exc)})
    return {"schema_version": 1, "packet_ref": packet_ref, "factory_config_ref": config_ref,
            "allow_model": allow_model, "items": results,
            "consumers_started": sum(row["consumer_spawned"] for row in results),
            "attention": any(row["status"] == "attention" for row in results),
            **({"control_consumers_started": sum(row.get("original_pause", {}).get("control_consumer", {}).get("consumer_spawned", False)
                                                for row in results)} if pause_grant is not None else {})}
