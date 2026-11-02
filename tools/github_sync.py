"""Validate and synchronize only public files through a private Git mirror.

The default is a local preview. ``--push`` additionally runs the full offline
test suite, commits in the mirror and performs a non-forced verified push.
"""

import argparse
import ast
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit


class SyncError(ValueError):
    """A publication boundary or operational check did not pass."""


ROOT_FILES = {".gitignore", "AGENTS.md", "README.md", "pyproject.toml", "LICENSE", "NOTICE"}
REQUIRED_FILES = {".gitignore", "AGENTS.md", "README.md", "pyproject.toml",
                  "skills/high-value-task-workflow/SKILL.md", "knowledge/index.md"}
SENSITIVE = {
    "personal_home": r"/" + r"Users/[^/\s`\"']+",
    "private_meeting": r"https?://[^\s)\"']*(?:feishu\.cn|larkoffice\.com)",
    "chat_identity": r"\b01[a-f0-9]{6}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}\b",
    "private_key": r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
    "credential": r"(?<![A-Za-z0-9_-])(?:sk-(?:proj-)?[A-Za-z0-9_-]{24,}|gh[pousr]_[A-Za-z0-9]{24,}|github_pat_[A-Za-z0-9_]{24,})",
}


def allowed_path(name: str) -> bool:
    """Apply an independent allowlist even if Git ignore rules are widened."""
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts or "\\" in name:
        return False
    if name.endswith((".local.md", "LOCAL.md")):
        return False
    if len(p.parts) == 1:
        return name in ROOT_FILES
    if len(p.parts) == 2:
        return ((p.parts[0] in {"taskflow", "tools", "tests"} and p.suffix == ".py")
                or (p.parts[0] == "docs" and p.suffix == ".md")
                or name in {"knowledge/index.md", "knowledge/ingestion.md",
                            "knowledge/open-questions.template.md"})
    if len(p.parts) == 3:
        return ((p.parts[:2] == ("config", "examples") and p.suffix == ".json")
                or (p.parts[0] == "knowledge" and p.parts[1] in
                    {"methods", "experience", "policy-notes", "templates"}
                    and (p.suffix == ".md" or name == "knowledge/templates/task-batch-plan.template.json"))
                or name == "skills/high-value-task-workflow/SKILL.md")
    return False


def _run(argv: list[str], *, cwd: Path | None = None,
         env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    if Path(argv[0]).name == "git":
        env = {key: value for key, value in (env if env is not None else os.environ).items()
               if not key.startswith("GIT_")}
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, timeout=120)
    if check and result.returncode:
        raise SyncError(f"command_failed:{Path(argv[0]).name}:{result.returncode}")
    return result


def _git(mirror: Path, *args: str, check: bool = True) -> str:
    return _run(["git", "-c", "core.hooksPath=" + os.devnull, "-C", str(mirror), *args],
                check=check).stdout.decode("utf-8").strip()


def _anchors(text: str) -> set[str]:
    found = set(re.findall(r'<a[^>]+(?:id|name)=["\']([^"\']+)', text))
    counts = {}
    for line in text.splitlines():
        m = re.match(r"^#{1,6}\s+(.+?)(?:\s+#+)?$", line)
        if m:
            slug = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", m[1])
            slug = re.sub(r"[^\w\- ]", "", slug.lower()).replace(" ", "-")
            count = counts.get(slug, 0)
            counts[slug] = count + 1
            found.add(slug if not count else f"{slug}-{count}")
    return found


def collect_public(source: Path) -> dict[str, bytes]:
    """Read the exact ignored-filtered public tree; never follow symlinks."""
    with tempfile.TemporaryDirectory(prefix="taskflow-public-enumeration-") as tmp:
        _run(["git", "-c", "init.defaultBranch=main", "init", "--quiet", tmp])
        result = _run(["git", "--git-dir", str(Path(tmp) / ".git"), "--work-tree",
                       str(source), "ls-files", "--others", "--exclude-standard", "-z"])
    names = sorted(x for x in result.stdout.decode("utf-8").split("\0") if x)
    if len(names) > 2000 or not REQUIRED_FILES.issubset(names):
        raise SyncError("public_tree_incomplete_or_oversized")
    payloads = {}
    total_bytes = 0
    for name in names:
        if not allowed_path(name):
            raise SyncError(f"outside_public_scope:{name}")
        path = source / name
        ancestors = [path, *(source / part for part in PurePosixPath(name).parents if str(part) != ".")]
        if any(p.is_symlink() for p in ancestors):
            raise SyncError(f"symlink:{name}")
        if not path.is_file() or path.stat().st_size > 5 * 1024 * 1024:
            raise SyncError(f"invalid_public_file:{name}")
        total_bytes += path.stat().st_size
        if total_bytes > 50 * 1024 * 1024:
            raise SyncError("public_tree_bytes_exceeded")
        data = path.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SyncError(f"non_utf8:{name}") from exc
        for kind, pattern in SENSITIVE.items():
            if re.search(pattern, text):
                raise SyncError(f"sensitive_content:{name}:{kind}")
        if str(Path.home()) != "/" and str(Path.home()) in text:
            raise SyncError(f"sensitive_content:{name}:current_home")
        try:
            if path.suffix == ".py":
                ast.parse(text, filename=name, feature_version=(3, 10))
            elif path.suffix == ".json":
                json.loads(text)
        except (SyntaxError, ValueError) as exc:
            raise SyncError(f"invalid_syntax:{name}") from exc
        payloads[name] = data
    for name, data in payloads.items():
        if not name.endswith(".md"):
            continue
        for target in re.findall(r"\]\(([^)]+)\)", data.decode("utf-8")):
            if urlsplit(target).scheme:
                continue
            relative, _, fragment = target.partition("#")
            dest = (source / name).parent / unquote(relative) if relative else source / name
            dest = dest.resolve()
            if not dest.is_relative_to(source):
                raise SyncError(f"outside_link:{name}")
            key = dest.relative_to(source).as_posix()
            if key not in payloads or (fragment and key.endswith(".md") and
                    unquote(fragment) not in _anchors(payloads[key].decode("utf-8"))):
                raise SyncError(f"broken_link:{name}")
    return payloads


def _digest(payloads: dict[str, bytes]) -> str:
    manifest = [(name, hashlib.sha256(data).hexdigest()) for name, data in sorted(payloads.items())]
    return hashlib.sha256(json.dumps(manifest, separators=(",", ":")).encode()).hexdigest()


def _save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise SyncError("unsafe_state_file")
    with tempfile.NamedTemporaryFile(dir=path.parent, mode="w", encoding="utf-8", delete=False) as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        temp = Path(f.name)
    os.replace(temp, path)


def load_config(path: Path) -> dict:
    """Validate local deployment configuration, without selecting an account."""
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise SyncError("invalid_config")
    repository = config.get("repository")
    if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9][A-Za-z0-9_.-]*", repository):
        raise SyncError("invalid_repository")
    branch = config.setdefault("branch", "main")
    if not isinstance(branch, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./-]*", branch) or ".." in branch or "//" in branch:
        raise SyncError("invalid_branch")
    identity = config.get("identity", {})
    if not isinstance(identity, dict) or any(key not in {"name", "email"} or
            not isinstance(value, str) or not value.strip() or "\n" in value
            for key, value in identity.items()):
        raise SyncError("invalid_identity")
    for name in ("source", "mirror", "state_file"):
        value = config.get(name)
        if not isinstance(value, str) or not value.strip():
            raise SyncError(f"missing_config:{name}")
        p = Path(value).expanduser()
        if p.is_symlink():
            raise SyncError(f"unsafe_config_path:{name}")
        config[name] = p.resolve()
    source, mirror = config["source"], config["mirror"]
    if not source.is_dir() or source == mirror or source.is_relative_to(mirror):
        raise SyncError("unsafe_mirror_scope")
    for p in (mirror, config["state_file"]):
        if p.is_relative_to(source) and (allowed_path(p.relative_to(source).as_posix()) or
                p.relative_to(source).parts[0] in {"taskflow", "tools", "tests", "docs", "skills", "knowledge", "config", ".git"}):
            raise SyncError("operational_path_inside_public_scope")
    return config


def _test_snapshot(snapshot: Path, log: Path) -> dict:
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    for key in ("TASKFLOW_TEST_FORMAL_ADAPTER", "TASKFLOW_TEST_PUBLIC_PROBE_ADAPTER_ROOT", "PYTHONPATH", "PYTHONHOME"):
        env.pop(key, None)
    try:
        result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-q"],
                                cwd=snapshot, env=env, capture_output=True, timeout=1800)
    except subprocess.TimeoutExpired as exc:
        log.write_bytes((exc.stdout or b"") + (exc.stderr or b""))
        raise SyncError("tests_timed_out") from exc
    log.write_bytes(result.stdout + result.stderr)
    match = re.search(rb"Ran (\d+) tests?", result.stderr)
    if result.returncode or not match or int(match[1]) == 0:
        raise SyncError("tests_failed_or_empty")
    return {"tests": int(match[1]), "log": str(log)}


def _check_origin(config: dict) -> None:
    repo = config["repository"]
    expected = {f"https://github.com/{repo}.git", f"https://github.com/{repo}",
                f"git@github.com:{repo}.git"}
    for flags in (("--all",), ("--push", "--all")):
        urls = _git(config["mirror"], "remote", "get-url", *flags, "origin").splitlines()
        if not urls or any(url not in expected for url in urls):
            raise SyncError("origin_mismatch")


def _mirror_ready(config: dict) -> tuple[str | None, str | None]:
    mirror, repo, branch = config["mirror"], config["repository"], config["branch"]
    gitdir = mirror / ".git"
    if gitdir.is_symlink() or (gitdir.exists() and not gitdir.is_dir()):
        raise SyncError("mirror_external_gitdir")
    if not (mirror / ".git").exists():
        if mirror.exists() and any(mirror.iterdir()):
            raise SyncError("mirror_not_empty")
        mirror.mkdir(parents=True, exist_ok=True)
        _run(["git", "-c", "init.defaultBranch=" + branch, "init", "--quiet", str(mirror)])
        _git(mirror, "remote", "add", "origin", f"https://github.com/{repo}.git")
        _git(mirror, "config", "credential.helper", "")
        _git(mirror, "config", "--add", "credential.helper", "!gh auth git-credential")
        _git(mirror, "config", "core.autocrlf", "false")
        for key, value in config.get("identity", {}).items():
            if key not in {"name", "email"} or not isinstance(value, str) or "\n" in value:
                raise SyncError("invalid_identity")
            _git(mirror, "config", "user." + key, value)
    actual_gitdir = Path(_git(mirror, "rev-parse", "--absolute-git-dir")).resolve()
    common = Path(_git(mirror, "rev-parse", "--git-common-dir"))
    common = (mirror / common).resolve() if not common.is_absolute() else common.resolve()
    if (actual_gitdir != gitdir.resolve() or common != gitdir.resolve() or
            Path(_git(mirror, "rev-parse", "--show-toplevel")).resolve() != mirror):
        raise SyncError("mirror_external_gitdir")
    _check_origin(config)
    if _git(mirror, "status", "--porcelain", "--untracked-files=all", "--ignored=matching"):
        raise SyncError("mirror_has_local_changes")
    local = _git(mirror, "rev-parse", "--verify", "HEAD", check=False) or None
    if local and _git(mirror, "symbolic-ref", "--short", "HEAD", check=False) != branch:
        raise SyncError("mirror_branch_mismatch")
    remote_line = _git(mirror, "ls-remote", "--heads", "origin", "refs/heads/" + branch)
    return local, remote_line.split()[0] if remote_line else None


def _push(config: dict, commit: str) -> None:
    mirror, branch = config["mirror"], config["branch"]
    _check_origin(config)
    _git(mirror, "push", "--no-follow-tags", "origin", commit + ":refs/heads/" + branch)
    observed = _git(mirror, "ls-remote", "--heads", "origin", "refs/heads/" + branch)
    if not observed or observed.split()[0] != commit:
        raise SyncError("remote_verification_failed")


def _stage(config: dict, snapshot: Path, payloads: dict[str, bytes]) -> str:
    mirror = config["mirror"]
    tracked = _git(mirror, "ls-files", "-z").split("\0")
    for name in filter(None, tracked):
        if not allowed_path(name):
            raise SyncError(f"unsafe_tracked_file:{name}")
        if name not in payloads:
            (mirror / name).unlink()
    for name in payloads:
        dest = mirror / name
        ancestors = [dest, *(mirror / part for part in PurePosixPath(name).parents if str(part) != ".")]
        if any(p.is_symlink() for p in ancestors):
            raise SyncError(f"mirror_symlink:{name}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(snapshot / name, dest)
    _git(mirror, "add", "--all")
    names = set(filter(None, _git(mirror, "ls-files", "-z").split("\0")))
    if names != set(payloads):
        raise SyncError("staged_members_mismatch")
    for name, data in payloads.items():
        actual = _run(["git", "-C", str(mirror), "show", ":" + name]).stdout
        if actual != data:
            raise SyncError(f"staged_bytes_changed:{name}")
    _git(mirror, "-c", "core.hooksPath=" + os.devnull, "commit", "--no-gpg-sign", "-m",
         "Sync verified workflow and knowledge updates")
    return _git(mirror, "rev-parse", "HEAD")


def synchronize(config: dict, *, push: bool = False) -> dict:
    """Publish a tested stable snapshot, preserving a failed push's commit."""
    source, state_file = config["source"], config["state_file"]
    payloads = collect_public(source)
    digest = _digest(payloads)
    state = json.loads(state_file.read_text()) if state_file.exists() else {}
    if not push:
        return {"status": "preview", "files": len(payloads), "digest": digest,
                "changed": state.get("synced_digest") != digest, "tests_run": False}
    state_file.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_file.with_suffix(state_file.suffix + ".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SyncError("sync_already_running") from exc
        state = json.loads(state_file.read_text()) if state_file.exists() else {}
        try:
            local, remote = _mirror_ready(config)
            pending = state.get("pending_commit")
            if pending:
                if pending != local or _git(config["mirror"], "rev-parse", "HEAD^{tree}") != state.get("pending_tree"):
                    raise SyncError("pending_commit_mismatch")
                if remote not in (state.get("pending_parent"), pending):
                    raise SyncError("remote_head_changed")
                if remote != pending:
                    _push(config, pending)
                state.update(synced_digest=state.pop("pending_digest"), synced_commit=pending)
                for key in ("pending_commit", "pending_parent", "pending_tree"):
                    state.pop(key, None)
                state["last_status"] = "pushed_pending_commit"
                _save(state_file, state)
                return {"status": "pushed_pending_commit", "commit": pending,
                        "source_has_newer_changes": state["synced_digest"] != digest}
            if local != remote:
                raise SyncError("remote_head_changed")
            if local and state.get("synced_commit") == local and state.get("synced_digest") == digest:
                return {"status": "no_changes", "commit": local}
            with tempfile.TemporaryDirectory(prefix="taskflow-publish-snapshot-") as tmp:
                snapshot = Path(tmp)
                for name, data in payloads.items():
                    target = snapshot / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                log = state_file.parent / ("tests-" + str(time.time_ns()) + ".log")
                test_result = _test_snapshot(snapshot, log)
                if _digest(collect_public(source)) != digest:
                    raise SyncError("source_changed_during_validation")
                commit = _stage(config, snapshot, payloads)
            state.update(pending_commit=commit, pending_parent=remote,
                         pending_tree=_git(config["mirror"], "rev-parse", "HEAD^{tree}"),
                         pending_digest=digest, tests=test_result)
            _save(state_file, state)
            _push(config, commit)
            state.update(synced_digest=digest, synced_commit=commit, last_status="pushed")
            for key in ("pending_commit", "pending_parent", "pending_tree", "pending_digest"):
                state.pop(key, None)
            _save(state_file, state)
            return {"status": "pushed", "commit": commit, "files": len(payloads), **test_result}
        except (SyncError, OSError, subprocess.TimeoutExpired) as exc:
            state.update(last_status="failed", last_error=str(exc) if isinstance(exc, SyncError) else type(exc).__name__)
            _save(state_file, state)
            raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--push", action="store_true", help="Test, commit and push; otherwise preview only")
    args = parser.parse_args(argv)
    try:
        result = synchronize(load_config(args.config), push=args.push)
    except (SyncError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc) if isinstance(exc, SyncError) else type(exc).__name__}))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
