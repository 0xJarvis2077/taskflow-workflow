"""Preserve unclaimed interrupted preparation before normal same-path re-entry."""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, sha256


PREPARATION_FILES = {
    "request.md", "worker.py", "cli-evidence.json", "execution_quality_capture.py",
    "execution_quality_progress_contract.py", "quality-output-contract.json",
    "codex_environment.py", "quality-later-inputs.v1.json", "response-schema.json",
    "text-only-catalog.json", "inline-input-index.json", "manifest.json",
}


def _record(path, value):
    from taskflow.execution_runner import _publish, _read

    if path.exists():
        if _read(path) != value:
            raise TaskflowError("partial preparation receipt changed")
    else:
        _publish(path, value)


def _inventory(root):
    from taskflow.execution_runner import _info

    if root.is_symlink() or not root.is_dir():
        raise TaskflowError("partial preparation requires a plain runtime directory")
    files, directories = {}, []
    for directory, children, names in os.walk(root, followlinks=False):
        for name in [*children, *names]:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if path.is_symlink():
                raise TaskflowError("partial preparation contains a symlink")
            if path.is_dir():
                if relative != "inputs" and not relative.startswith("inputs/"):
                    raise TaskflowError("partial preparation contains non-staging directory: " + relative)
                directories.append(relative)
            else:
                if not relative.startswith("inputs/") and relative not in PREPARATION_FILES:
                    raise TaskflowError("partial preparation contains sealed, execution or unknown evidence: " + relative)
                files[relative] = _info(path)
    return {"files": files, "directories": sorted(directories)}


def _unclaimed(store, work, runtime):
    with store._db() as db:
        for table in ("work", "attempts"):
            for row in db.execute("SELECT data FROM " + table):
                value = json.loads(row[0])
                if (value.get("work_id") == work["work_id"] or value.get("intent_id") == work["intent_id"]
                        or value.get("runtime_path") == str(runtime)):
                    raise TaskflowError("partial preparation already has registered work or an attempt; preserve original")


def _finish_archives(runtime):
    from taskflow.execution_runner import _read

    archive_root = runtime.with_name(runtime.name + ".preparation-originals")
    if archive_root.is_symlink():
        raise TaskflowError("partial preparation archive cannot be a symlink")
    if not archive_root.exists():
        return
    for archive in archive_root.iterdir():
        if archive.is_symlink() or not archive.is_dir():
            raise TaskflowError("partial preparation archive is invalid")
        receipt = archive / "receipt.json"
        if not receipt.exists():
            if list(archive.iterdir()):
                raise TaskflowError("partial preparation archive has no original receipt")
            continue
        record = _read(receipt)
        if record.get("runtime") != str(runtime) or record.get("kind") != "unclaimed_partial_preparation_v1":
            raise TaskflowError("partial preparation receipt has a different runtime")
        saved = archive / "original"
        if saved.exists():
            if _inventory(saved) != record["original"]:
                raise TaskflowError("preserved preparation original drifted")
            _record(archive / "preserved.json", {"record_sha256": sha256(canonical(record)), "original_verified": True})
        elif not runtime.exists() or _inventory(runtime) != record["original"]:
            raise TaskflowError("interrupted preparation preservation lost its original")


def _preserve_partial(runtime, work, runner):
    from taskflow.execution_runner import _read

    inventory = _inventory(runtime)
    if "manifest.json" in inventory["files"]:
        manifest = _read(runtime / "manifest.json")
        if (manifest.get("preparation_config_digest") != sha256(canonical(runner))
                or manifest.get("intent_id") != work["intent_id"]):
            raise TaskflowError("unsealed partial manifest belongs to different preparation")
    record = {"schema_version": 1, "kind": "unclaimed_partial_preparation_v1",
              "runtime": str(runtime), "work_id": work["work_id"], "intent_id": work["intent_id"],
              "configuration_sha256": sha256(canonical(runner)), "original": inventory,
              "model_retry": False}
    archive_root = runtime.with_name(runtime.name + ".preparation-originals")
    if archive_root.is_symlink():
        raise TaskflowError("partial preparation archive cannot be a symlink")
    archive_root.mkdir(exist_ok=True)
    digest = sha256(canonical(record))
    ordinal = 0
    while True:
        archive = archive_root / (digest + "-" + str(ordinal).zfill(3))
        if archive.is_symlink():
            raise TaskflowError("partial preparation receipt cannot be a symlink")
        if not archive.exists():
            archive.mkdir()
        saved = archive / "original"
        if not saved.exists():
            break
        if _read(archive / "receipt.json") != record or _inventory(saved) != inventory:
            raise TaskflowError("preserved preparation original drifted")
        _record(archive / "preserved.json", {"record_sha256": digest, "original_verified": True})
        ordinal += 1
    _record(archive / "receipt.json", record)
    os.rename(runtime, saved)
    if _inventory(saved) != inventory:
        raise TaskflowError("partial preparation changed during preservation")
    _record(archive / "preserved.json", {"record_sha256": digest, "original_verified": True})


@contextmanager
def _preparation_lock(runtime):
    runtime.parent.mkdir(parents=True, exist_ok=True)
    path = runtime.with_name(runtime.name + ".preparation.lock")
    if path.is_symlink():
        raise TaskflowError("preparation lock cannot be a symlink")
    with path.open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TaskflowError("runtime preparation is already active") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def prepare_or_resume(store, work, runner, runtime, *, resume_prepared=False):
    """Re-enter only an unclaimed unsealed preparation; never retry an attempt."""
    from taskflow.execution_runner import _validate, prepare_attempt

    runtime = Path(runtime).absolute()
    with _preparation_lock(runtime):
        _finish_archives(runtime)
        if runtime.exists() and resume_prepared:
            if not (runtime / "manifest-lock.json").exists():
                _unclaimed(store, work, runtime)
                if runner.get("mode", "native_role") not in {"native_role", "test_fixture"}:
                    raise TaskflowError("partial isolated bridge preparation requires its original owner recovery")
                _preserve_partial(runtime, work, runner)
            else:
                manifest = _validate(runtime)
                if manifest.get("preparation_config_digest") != sha256(canonical(runner)):
                    raise TaskflowError("prepared runtime belongs to a different continuation configuration")
                return manifest
        return prepare_attempt(runtime, runner)
