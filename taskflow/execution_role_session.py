"""Explicit Codex role sessions and single-successor claims.

This module is copied beside the standalone worker. It does not discover
sessions, reconstruct context, authorize follow-ups or schedule model work.
"""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Callable


MODES = frozenset({"fresh_persistent", "fresh_ephemeral", "resume_existing"})
REVIEW_ROLES = frozenset({"review-facts", "review-score"})
SEALED_FACTS_TARGET = "execution/sealed-facts.json"
BINDING_NAME = ".taskflow-role-binding.json"
CLAIMS_NAME = ".taskflow-role-invocations"
LOCK_NAME = ".taskflow-role-session.lock"


class RoleSessionError(ValueError):
    """A declared role session cannot use the claimed original context."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise RoleSessionError("role session evidence must be an ordinary file")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RoleSessionError("role session evidence must be a JSON object")
    return value


def _write_once(path: Path, value: dict) -> None:
    # An exclusive no-follow open makes a home/previous claim non-replaceable.
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(_canonical(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def _home(value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise RoleSessionError("role session home must be an absolute task-owned directory")
    home = Path(value)
    if not home.is_absolute() or home.resolve() != home or not home.is_dir():
        raise RoleSessionError("role session home must be an absolute ordinary directory")
    if home in {Path("/"), Path.home().resolve(), (Path.home() / ".codex").resolve()}:
        raise RoleSessionError("role session cannot use the global Codex home")
    return home


def _reference(reference: object) -> Path:
    if (not isinstance(reference, dict)
            or not {"path", "sha256", "size"}.issubset(reference)
            or set(reference) - {"path", "sha256", "size", "location"}
            or not isinstance(reference["path"], str)
            or type(reference["size"]) is not int):
        raise RoleSessionError("review run binding requires exact ordinary-file references")
    path = Path(reference["path"])
    if (not path.is_absolute() or str(path.resolve()) != reference["path"]
            or path.is_symlink() or not path.is_file()):
        raise RoleSessionError("review run binding requires canonical ordinary-file references")
    raw = path.read_bytes()
    if len(raw) != reference["size"] or hashlib.sha256(raw).hexdigest() != reference["sha256"]:
        raise RoleSessionError("review run binding dependency changed")
    return path


def _run_dependencies(config: dict) -> list[str]:
    binding = config.get("run_binding")
    if config["role"] not in REVIEW_ROLES:
        if binding is not None:
            raise RoleSessionError("only reviewer stages may declare a review run binding")
        return []
    if (not isinstance(binding, dict) or set(binding) != {"source", "teacher_inputs"}
            or not isinstance(binding["teacher_inputs"], list)):
        raise RoleSessionError("reviewer requires its frozen source and teacher run binding")
    source = _reference(binding["source"])
    dependencies = [str(source)]
    targets = set()
    teachers = set()
    for item in binding["teacher_inputs"]:
        if (not isinstance(item, dict)
                or not {"path", "target", "kind", "sha256", "size"}.issubset(item)
                or set(item) - {"path", "target", "kind", "sha256", "size", "location"}
                or item.get("kind") != "teacher" or not isinstance(item["target"], str)):
            raise RoleSessionError("review teacher inputs must declare exact typed frozen originals")
        target = item["target"]
        if (not target or Path(target).is_absolute() or "\\" in target
                or any(part in {"", ".", ".."} for part in target.split("/"))
                or target == SEALED_FACTS_TARGET or target in targets):
            raise RoleSessionError("review teacher input targets must be unique safe paths")
        original = _reference({key: value for key, value in item.items()
                               if key not in {"target", "kind"}})
        targets.add(target)
        teachers.add(str(original))
        dependencies.append(str(original))
    for item in config.get("inputs", []):
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise RoleSessionError("review typed inputs must identify their original paths")
        original = str(Path(item["path"]).resolve())
        if original == str(source):
            raise RoleSessionError("manager review run binding cannot be a model input")
        if config["role"] == "review-facts" and (
                original in teachers or item.get("target") == SEALED_FACTS_TARGET):
            raise RoleSessionError("facts cannot receive a frozen teacher or prior sealed-facts input")
    return dependencies


def _run_identity(binding: dict | None) -> dict | None:
    if binding is None:
        return None
    return {"source": {key: value for key, value in binding["source"].items()
                       if key != "location"},
            "teacher_inputs": [{key: value for key, value in item.items() if key != "location"}
                               for item in binding["teacher_inputs"]]}


def _input_map(inputs: list[dict]) -> dict[str, dict]:
    if not isinstance(inputs, list):
        raise RoleSessionError("version transition inputs must be a typed list")
    result = {}
    for item in inputs:
        if (not isinstance(item, dict) or not isinstance(item.get("target"), str)
                or not isinstance(item.get("kind"), str)):
            raise RoleSessionError("version transition requires typed input targets")
        target = item["target"]
        if target in result:
            raise RoleSessionError("version transition input targets must be unique")
        if "sha256" in item and "size" in item:
            value = {key: item[key] for key in ("kind", "sha256", "size")}
        else:
            path = Path(item["path"])
            if path.is_symlink() or not path.is_file():
                raise RoleSessionError("version transition input must be an ordinary file")
            raw = path.read_bytes()
            value = {"kind": item["kind"], "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}
        result[target] = value
    return result


def input_changes(before: list[dict], after: list[dict]) -> list[dict]:
    """Describe every actual typed input change, for a versioned dispatch proof."""
    old, new = _input_map(before), _input_map(after)
    return [{"target": target, "before": old.get(target), "after": new.get(target)}
            for target in sorted(set(old) | set(new)) if old.get(target) != new.get(target)]


def _lineage(config: dict) -> None:
    value = config.get("role_lineage")
    if value is None:
        return
    role = {"repair": "author", "quality": "quality"}.get(config.get("role"))
    binding = config.get("candidate_binding")
    if (not isinstance(value, dict) or set(value) != {"logical_role", "owner", "task_id"}
            or value.get("logical_role") != role or role is None
            or any(not isinstance(item, str) or not item.strip() for item in value.values())
            or not isinstance(binding, dict) or value["task_id"] != binding.get("task_id")):
        raise RoleSessionError("role lineage requires the exact logical author or QA and task")


def _transition_reference(config: dict) -> list[str]:
    transition = config.get("role_transition")
    if transition is None:
        return []
    required = {"kind", "source_binding", "target_binding", "revision", "input_changes"}
    if (not isinstance(transition, dict) or not required.issubset(transition)
            or set(transition) - required - {"read_root_transition"}
            or transition.get("kind") != {"repair": "author_revision_v1", "quality": "quality_recheck_v1"}.get(config.get("role"))
            or config.get("role_lineage") is None
            or config.get("role_session", {}).get("mode") != "resume_existing"):
        raise RoleSessionError("version migration requires an explicit author revision or QA recheck")
    path = _reference(transition["revision"])
    diff = _read(path)
    keys = {"candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest"}
    for key in ("source_binding", "target_binding"):
        value = transition[key]
        if (not isinstance(value, dict) or set(value) != keys
                or any(not isinstance(item, str) or not item for item in value.values())):
            raise RoleSessionError("version transition requires exact source and target candidate bindings")
    if (diff.get("kind") != "material_revision_diff_v1"
            or diff.get("parent_binding") != transition["source_binding"]
            or diff.get("candidate_binding") != transition["target_binding"]):
        raise RoleSessionError("version transition differs from its original parent-child revision")
    return [str(path)]


def declaration(config: dict, source_loader: Callable[[dict], dict]) -> dict | None:
    """Validate the explicit declaration and derive only its original SID."""
    session = config.get("role_session")
    if session is None:
        return None
    role = config.get("role")
    if (role not in {"quality", "solver", "repair", *REVIEW_ROLES}
            or config.get("mode", "native_role") not in {"native_role", "test_fixture"}
            or config.get("quality_session") is not None
            or not isinstance(session, dict) or set(session) != {"mode", "home", "previous"}
            or not isinstance(session.get("mode"), str) or session["mode"] not in MODES):
        raise RoleSessionError("unsupported explicit Codex role session declaration")
    if role == "solver" and (session["mode"] == "resume_existing" or config.get("isolated")):
        raise RoleSessionError("native solver requires a fresh independent role session")
    if role == "repair" and session["mode"] == "resume_existing" and config.get("role_transition") is None:
        raise RoleSessionError("author continuation requires its explicit parent-child version transition")
    if role == "review-facts" and session["mode"] == "fresh_ephemeral":
        raise RoleSessionError("facts require a persistent independent reviewer")
    if role == "review-score" and session["mode"] != "resume_existing":
        raise RoleSessionError("score requires the precise original facts reviewer continuation")
    if role != "quality":
        sections, inputs = config.get("request_sections", []), config.get("inputs", [])
        if not isinstance(sections, list) or not isinstance(inputs, list):
            raise RoleSessionError("native solver/reviewer inputs and instructions must be lists")
        if any(not isinstance(section, dict) or section.get("kind") != "operation" for section in sections):
            raise RoleSessionError("native solver/reviewer instructions require operation sections")
        allowed = {"public"} if role == "solver" else {"public", "submission", "trace", "operation"}
        if role == "repair":
            allowed = {"public", "teacher", "history", "source_metadata"}
        if role == "review-score":
            allowed.add("teacher")
        if any(not isinstance(item, dict) or item.get("kind") not in allowed for item in inputs):
            raise RoleSessionError("forbidden material in explicit solver/reviewer role inputs")
    if role in REVIEW_ROLES and (
            config.get("outputs") != ["facts.json" if role == "review-facts" else "score.json"]
            or config.get("output_trees")):
        raise RoleSessionError("reviewer stages require their one declared facts.json or score.json")
    _lineage(config)
    run_dependencies = _run_dependencies(config) + _transition_reference(config)
    home = _home(session["home"])
    if config.get("candidate_binding") is None:
        raise RoleSessionError("role session requires its complete candidate binding")
    binding = config["candidate_binding"]
    required = {"candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest"}
    if (not isinstance(binding, dict) or set(binding) != required
            or any(not isinstance(value, str) or not value for value in binding.values())):
        raise RoleSessionError("role session requires its complete candidate binding")
    for name in (config.get("cwd"), config.get("native_permissions", {}).get("scratch")):
        if name:
            path = Path(name).resolve()
            if home.is_relative_to(path) or path.is_relative_to(home):
                raise RoleSessionError("role home must be separate from model work and scratch")
    previous = session["previous"]
    if session["mode"] != "resume_existing":
        if previous is not None or config.get("resume_sid") is not None:
            raise RoleSessionError("fresh role session cannot supply a previous context or SID")
        if any(home.iterdir()):
            raise RoleSessionError("fresh role session requires an empty task-owned home")
        return {"declaration": session, "resume_sid": None, "source": None,
                "run_dependencies": run_dependencies}
    source = source_loader(previous)
    original = source["manifest"]
    prior_session = original.get("role_session") or {}
    required_roles = REVIEW_ROLES if role == "review-score" else {role}
    if (original.get("role") not in required_roles
            or prior_session.get("mode") not in {"fresh_persistent", "resume_existing"}
            or prior_session.get("home") != str(home)
            or "--ephemeral" in original["argv"]):
        raise RoleSessionError("resume requires the original persistent role and home")
    if role == "review-score" and not source.get("facts_seal"):
        raise RoleSessionError("score requires actual sealed facts before reviewer continuation")
    sid = source["collection"]["sid"]
    if config.get("resume_sid") not in (None, sid):
        raise RoleSessionError("explicit resume SID differs from the original collection")
    return {"declaration": session, "resume_sid": sid, "source": source,
            "run_dependencies": run_dependencies}


def _replace_paths(value: object, paths: dict[str, str]) -> object:
    if isinstance(value, str):
        for original in sorted(paths, key=len, reverse=True):
            if value == original or value.startswith(original + "/"):
                value = paths[original] + value[len(original):]
            value = re.sub(r'(?<=")' + re.escape(original) + r'(?=/|")',
                           lambda match: paths[original], value)
        return value
    if isinstance(value, list):
        return [_replace_paths(item, paths) for item in value]
    if isinstance(value, dict):
        return {_replace_paths(key, paths): _replace_paths(item, paths) for key, item in value.items()}
    return value


def identity(manifest: dict, root: Path) -> dict:
    """Bind role capabilities; only own input/work/scratch locations may move."""
    native = manifest.get("native")
    paths = {str(root / "inputs"): "<ROLE_INPUTS>", manifest["cwd"]: "<ROLE_WORK>"}
    if native:
        paths[native["scratch"]] = "<ROLE_SCRATCH>"
        filesystem_option = "permissions." + native["profile"] + ".filesystem="
        # The structured filesystem map below binds these exact generated rules.
        def command(arguments):
            return [filesystem_option + "<ROLE_FILESYSTEM>" if item.startswith(filesystem_option) else item
                    for item in arguments]
        capability = {**native, "sandbox_argv": command(native["sandbox_argv"])}
        prefix = manifest["argv"][:manifest["argv"].index("exec")]
        prefix = command(prefix)
    else:
        capability = None
        prefix = manifest["argv"]
    provider = manifest["model"].get("native_provider_ref")
    if isinstance(provider, dict):
        provider = {key: value for key, value in provider.items() if key != "location"}
    executables = {str(Path(manifest["argv"][0]).resolve())}
    if native:
        executables.add(native["python"])
    reviewer = manifest["role"] in REVIEW_ROLES
    inputs = manifest["inputs"]
    if manifest["role"] == "review-score":
        inputs = [item for item in inputs
                  if item["kind"] != "teacher" and item["target"] != SEALED_FACTS_TARGET]
    result = {
        "candidate_binding": manifest["candidate_binding"],
        "role": "reviewer" if reviewer else manifest["role"],
        "mode": manifest["mode"], "policy_version": manifest["policy_version"],
        "model": {key: manifest["model"].get(key) for key in ("requested", "reasoning_effort")},
        "provider": provider, "native": _replace_paths(capability, paths),
        "command": _replace_paths(prefix, paths),
        "runtime_executables": {name: manifest.get("dependencies", {}).get(name)
                                for name in sorted(executables)},
        "runtime_helpers": {name: manifest.get("dependencies", {}).get(str(root / name))
                            for name in ("worker.py", "codex_environment.py", "role_session.py")},
        "reasoning_selection": manifest.get("reasoning_selection"),
        "reasoning_context": manifest.get("reasoning_context"),
        "batch_binding": manifest.get("batch_binding"), "model_slot": manifest.get("model_slot"),
        "inputs": sorted([{key: item[key] for key in ("target", "kind", "sha256", "size")}
                          for item in inputs], key=lambda item: item["target"]),
        "outputs": ["<REVIEWER_STAGE_OUTPUT>"] if reviewer else manifest["outputs"],
        "output_trees": manifest.get("output_trees", []),
    }
    if reviewer:
        result["run_binding"] = _run_identity(manifest.get("run_binding"))
    if manifest.get("role_lineage") is not None:
        result["role_lineage"] = manifest["role_lineage"]
    return result


def _version_transition(manifest: dict, root: Path, source: dict) -> None:
    """Allow only authenticated parent-child input evolution in one logical role."""
    original = source["manifest"]
    _transition_reference(original)
    old = identity(original, Path(source["root"]))
    new = identity(manifest, root)
    transition = manifest.get("role_transition")
    if transition is None:
        if old != new:
            raise RoleSessionError("resume changed candidate, policy, model, provider, inputs or capabilities")
        return
    _lineage(manifest)
    _transition_reference(manifest)
    before, after = transition["source_binding"], transition["target_binding"]
    if (before != original["candidate_binding"] or after != manifest["candidate_binding"]
            or before.get("task_id") != after.get("task_id")
            or before.get("candidate_id") == after.get("candidate_id")
            or original.get("role_lineage") != manifest.get("role_lineage")
            or original["role"] != manifest["role"]):
        raise RoleSessionError("version transition requires the same logical role and task with actual distinct parent and child")
    if transition["input_changes"] != input_changes(original["inputs"], manifest["inputs"]):
        raise RoleSessionError("version transition must declare all and only actual typed input changes")
    _payload_transition(manifest, original, _read(_reference(transition["revision"])))
    allowed = ({"public", "teacher", "history", "source_metadata"} if manifest["role"] == "repair"
               else {"public", "teacher", "history", "source_metadata", "submission", "trace", "operation", "observation"})
    if any(item["kind"] not in allowed for item in manifest["inputs"]):
        raise RoleSessionError("version transition introduced materials outside its role boundary")
    roots = transition.get("read_root_transition")
    if roots is not None:
        if not isinstance(roots, dict) or set(roots) != {"before", "after"}:
            raise RoleSessionError("read-root transition requires the exact before and after repository")
        for label, item, runtime in (("before", original, Path(source["root"])), ("after", manifest, root)):
            path = Path(roots[label])
            native = item.get("native") or {}
            if (not path.is_absolute() or path.resolve() != path or not path.is_dir()
                    or native.get("repository") != str(path)
                    or native.get("filesystem", {}).get(str(path)) != "read"
                    or any(path == owned or path.is_relative_to(owned) or owned.is_relative_to(path)
                           for owned in (Path(item["cwd"]), Path(item["role_session"]["home"]), runtime))):
                raise RoleSessionError("version transition can relocate only its independent read-only repository")
        old = _replace_paths(old, {roots["before"]: "<VERSION_READ_ROOT>"})
        new = _replace_paths(new, {roots["after"]: "<VERSION_READ_ROOT>"})
    for item in (old, new):
        item.pop("candidate_binding")
        item.pop("inputs")
    if old != new:
        raise RoleSessionError("version transition changed policy, model, provider, logical role or capabilities")


def _payload_info(value: dict | None) -> dict | None:
    if value is None:
        return None
    public = {"prompt", "input", "template"}
    if (not isinstance(value, dict) or set(value) != {"role", "sha256", "size"}
            or not isinstance(value.get("role"), str)
            or value["role"] not in public | {"answer", "rubric", "teacher", "verification"}):
        raise RoleSessionError("version transition payload role is malformed")
    return {"kind": "public" if value["role"] in public else "teacher",
            "sha256": value["sha256"], "size": value["size"]}


def _payload_transition(manifest: dict, original: dict, diff: dict) -> None:
    """A version proof cannot authorize unrelated teacher files or relabel inputs."""
    changes = diff.get("changes")
    if not isinstance(changes, list):
        raise RoleSessionError("version transition requires the actual payload difference")
    prefix = "parent/" if manifest["role"] == "repair" else ""
    rows = {}
    for row in changes:
        if not isinstance(row, dict) or not isinstance(row.get("path"), str):
            raise RoleSessionError("version transition payload difference is malformed")
        target = prefix + row["path"]
        if target in rows:
            raise RoleSessionError("version transition payload difference duplicates a path")
        rows[target] = row
    before, after = _input_map(original["inputs"]), _input_map(manifest["inputs"])
    for target, row in rows.items():
        for label, inputs in (("before", before), ("after", after)):
            expected = _payload_info(row.get(label))
            if inputs.get(target) != expected:
                raise RoleSessionError("version transition public or teacher input does not match its actual role and bytes")
    for row in input_changes(original["inputs"], manifest["inputs"]):
        old, new = row["before"], row["after"]
        if not any(item is not None and item["kind"] in {"public", "teacher"} for item in (old, new)):
            if old is not None and new is not None and old["kind"] != new["kind"]:
                raise RoleSessionError("version transition cannot relabel historical or operational input kinds")
            continue
        actual = rows.get(row["target"])
        if actual is None:
            raise RoleSessionError("version transition introduced public or teacher material outside the actual revision")
        for label, item in (("before", old), ("after", new)):
            expected = _payload_info(actual.get(label))
            if expected != item:
                raise RoleSessionError("version transition public or teacher input does not match its actual role and bytes")


def _review_transition(manifest: dict, source: dict) -> None:
    if manifest["role"] != "review-score":
        return
    original = source["manifest"]
    seal = source.get("facts_seal")
    if (original["role"] not in REVIEW_ROLES or not seal
            or manifest.get("facts_attempt") != source.get("facts_attempt")
            or manifest.get("facts_seal") != seal
            or _run_identity(original.get("run_binding")) != _run_identity(manifest.get("run_binding"))):
        raise RoleSessionError("score must retain the exact sealed facts and original formal run binding")
    expected = [{key: item[key] for key in ("target", "kind", "sha256", "size")}
                for item in original["inputs"]]
    if original["role"] == "review-facts":
        expected += [{key: item[key] for key in ("target", "kind", "sha256", "size")}
                     for item in manifest["run_binding"]["teacher_inputs"]]
        expected.append({"target": SEALED_FACTS_TARGET, "kind": "submission", **seal["facts"]})
    actual = [{key: item[key] for key in ("target", "kind", "sha256", "size")}
              for item in manifest["inputs"]]
    if sorted(expected, key=lambda item: item["target"]) != sorted(actual, key=lambda item: item["target"]):
        raise RoleSessionError(
            "score inputs must be exactly the original facts view, frozen teacher and sealed facts")
    sealed = [item for item in manifest["inputs"] if item["target"] == SEALED_FACTS_TARGET]
    if len(sealed) != 1 or sealed[0]["source"] != seal["path"]:
        raise RoleSessionError("score sealed-facts input must come from the original sealed artifact")
    teachers = {item["target"]: item for item in manifest["run_binding"]["teacher_inputs"]}
    for item in manifest["inputs"]:
        if item["kind"] == "teacher" and item["source"] != teachers[item["target"]]["path"]:
            raise RoleSessionError("score teacher input must come from its frozen original")


def bind(root: Path, manifest: dict, prepared: dict) -> dict:
    """Reserve a fresh home or bind a new invocation to one completed original."""
    session = prepared["declaration"]
    home = _home(session["home"])
    native = manifest.get("native")
    if native:
        if native["filesystem"].get(str(home)) != "deny":
            raise RoleSessionError("model tools must be denied access to the role home")
        provider = native.get("provider")
        if provider and provider["codex_home"] != str(home):
            raise RoleSessionError("provider Codex home differs from the declared role home")
    current = identity(manifest, root)
    source = prepared["source"]
    marker = home / BINDING_NAME
    if source:
        original = source["manifest"]
        if (manifest["intent_id"] == original["intent_id"] or root == Path(source["root"])
                or manifest["cwd"] == original["cwd"]):
            raise RoleSessionError("resume requires a distinct invocation, runtime and work directory")
        _review_transition(manifest, source)
        _version_transition(manifest, root, source)
        original_binding = original.get("role_session_binding") or {}
        if (original_binding.get("identity") != _digest(identity(original, Path(source["root"])))
                or _read(marker) != original_binding.get("home_binding")):
            raise RoleSessionError("original role home binding changed")
        home_binding = _read(marker)
    else:
        if any(home.iterdir()):
            raise RoleSessionError("fresh role session home was already reserved")
        home_binding = {"kind": "codex_role_home_v1", "identity": _digest(current),
                        "origin_intent": manifest["intent_id"], "origin_runtime": str(root)}
        _write_once(marker, home_binding)
    return {"schema_version": 1, "identity": _digest(current), "home_binding": home_binding,
            "invocation": {"intent_id": manifest["intent_id"], "runtime": str(root),
                           "previous": session["previous"], "resume_sid": prepared["resume_sid"]}}


def validate(root: Path, manifest: dict, source_loader: Callable[[dict], dict], *,
             before_run: bool = False) -> None:
    """Check the bound home and original evidence without finding session files."""
    session = manifest.get("role_session")
    if session is None:
        return
    home = _home(session["home"])
    binding = manifest["role_session_binding"]
    if (binding.get("identity") != _digest(identity(manifest, root))
            or _read(home / BINDING_NAME) != binding.get("home_binding")
            or binding.get("invocation") != {"intent_id": manifest["intent_id"], "runtime": str(root),
                "previous": session["previous"], "resume_sid": manifest.get("resume_sid")}):
        raise RoleSessionError("role session invocation or home binding changed")
    if session["mode"] == "resume_existing":
        source = source_loader(session["previous"])
        _review_transition(manifest, source)
        _version_transition(manifest, root, source)
        if (source["collection"]["sid"] != manifest.get("resume_sid")
                or source["manifest"].get("role_session_binding", {}).get("identity")
                   != _digest(identity(source["manifest"], Path(source["root"])))
                or source["manifest"].get("role_session_binding", {}).get("home_binding") != binding.get("home_binding")):
            raise RoleSessionError("role session original completed collection changed")
    elif before_run and {path.name for path in home.iterdir()} != {BINDING_NAME}:
        raise RoleSessionError("fresh role home changed before its first invocation")


def claim(root: Path, manifest: dict) -> None:
    """Consume this original once; a crashed/unknown launch stays occupied."""
    session = manifest.get("role_session")
    if session is None:
        return
    home = _home(session["home"])
    claims = home / CLAIMS_NAME
    claims.mkdir(exist_ok=True)
    if claims.is_symlink() or claims.resolve() != claims:
        raise RoleSessionError("role invocation claims must be an ordinary directory")
    previous = session["previous"]
    # A transport path/alias cannot make one original context consumable twice.
    key = ({"collection_sha256": previous["sha256"], "collection_size": previous["size"],
            "sid": manifest["resume_sid"]} if previous else
           {"origin": manifest["role_session_binding"]["home_binding"]})
    try:
        _write_once(claims / (_digest(key) + ".json"), manifest["role_session_binding"]["invocation"])
    except FileExistsError as error:
        raise RoleSessionError("original role context already has an occupied invocation") from error


@contextmanager
def execution_lock(manifest: dict):
    """Hold the role-home mutex for the entire actual CLI process lifetime."""
    session = manifest.get("role_session")
    if session is None:
        yield
        return
    path = _home(session["home"]) / LOCK_NAME
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RoleSessionError("role home already has an active invocation") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
