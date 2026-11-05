"""Explicit, reversible cold storage for individually selected report files.

This does not make compressed reports transparent to existing readers. Restore a
file before running a script that reads its original path. No task, frozen input,
run, evaluation or historical manifest is rewritten.
"""

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
from typing import BinaryIO, Iterator
import uuid
import zlib


CHUNK = 1024 * 1024
PROTECTED = {"versions", "runs", "exports", "draft", "checks", "runtime",
             "work", "scratch", "archive"}
KIND = "task_report_cold_storage_v1"


class StorageError(ValueError):
    """A report or its recovery record cannot be used safely."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _unique(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise StorageError("duplicate JSON key: " + key)
        result[key] = value
    return result


def _json(raw: bytes) -> dict:
    value = json.loads(raw, object_pairs_hook=_unique)
    if not isinstance(value, dict):
        raise StorageError("expected a JSON object")
    _canonical(value)
    return value


def _relative(value: str) -> str:
    if (not isinstance(value, str) or not value or "\\" in value
            or "\x00" in value or ":" in value.split("/")[0]
            or any(part in {"", ".", ".."} for part in value.split("/"))):
        raise StorageError("unsafe relative path: " + repr(value))
    return value


def _report_path(value: str) -> str:
    _relative(value)
    parts = value.split("/")
    if len(parts) < 2 or parts[0] != "reports" or PROTECTED.intersection(parts):
        raise StorageError("only unprotected reports/ files may enter cold storage")
    return value


def _identity(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _directory_identity(info: os.stat_result) -> tuple:
    return info.st_dev, info.st_ino


@contextmanager
def _parent(root: int, relative: str, *, create: bool = False) -> Iterator[tuple[int, str]]:
    parts = _relative(relative).split("/")
    descriptor = os.dup(root)
    try:
        for part in parts[:-1]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor, parts[-1]
    finally:
        os.close(descriptor)


@contextmanager
def _regular(parent: int, name: str) -> Iterator[BinaryIO]:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=parent)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise StorageError("not an ordinary file: " + name)
    with os.fdopen(descriptor, "rb") as source:
        yield source


def _exists(root: int, relative: str) -> bool:
    try:
        with _parent(root, relative) as (parent, name):
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def _digest(source: BinaryIO) -> dict:
    source.seek(0)
    digest = hashlib.sha256()
    size = 0
    for block in iter(lambda: source.read(CHUNK), b""):
        size += len(block)
        digest.update(block)
    return {"sha256": digest.hexdigest(), "size": size}


def _assert_file(parent: int, name: str, source: BinaryIO,
                 original: os.stat_result) -> None:
    current = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (_identity(original) != _identity(current)
            or _identity(original) != _identity(os.fstat(source.fileno()))):
        raise StorageError("source changed during operation: " + name)


def _task(root: int) -> tuple[set[str], str]:
    with _regular(root, "task.json") as source:
        raw = source.read(4 * CHUNK + 1)
        if len(raw) > 4 * CHUNK:
            raise StorageError("task.json exceeds the metadata limit")
    task = _json(raw)
    declared = set()
    for key in ("files", "reviews"):
        entries = task.get(key, [])
        if not isinstance(entries, list):
            raise StorageError("task.json " + key + " must be a list")
        for entry in entries:
            if not isinstance(entry, dict) or "path" not in entry:
                raise StorageError("task.json contains an invalid declared path")
            declared.add(_relative(entry["path"]))
    return declared, hashlib.sha256(raw).hexdigest()


@contextmanager
def _package(package: Path, relative: str) -> Iterator[tuple[Path, int, str]]:
    relative = _report_path(relative)
    package = Path(package).absolute()
    if (".." in package.parts or package.resolve() != package
            or any(path.is_symlink() for path in (package, *package.parents))):
        raise StorageError("package must have an ordinary, real directory path")
    root = os.open(package, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        declared, task_digest = _task(root)
        if relative in declared:
            raise StorageError("task.json declares this file; cold storage is forbidden")
        yield package, root, task_digest
    finally:
        os.close(root)


def _guard(package: Path, root: int, task_digest: str,
           relative: str, parent: int) -> None:
    if (package.resolve() != package
            or _directory_identity(package.stat()) != _directory_identity(os.fstat(root))):
        raise StorageError("package directory changed during operation")
    if _task(root)[1] != task_digest:
        raise StorageError("task.json changed during operation")
    with _parent(root, relative) as (current, _):
        if _directory_identity(os.fstat(current)) != _directory_identity(os.fstat(parent)):
            raise StorageError("parent directory changed during operation")


def _locations(relative: str) -> tuple[str, str]:
    compressed = "cold-storage/" + relative + ".gz"
    return compressed, compressed + ".json"


def _manifest(package: Path, relative: str, original: dict, compressed: dict) -> dict:
    argv = ["python3", "-m", "tools.task_storage", "restore", "--package",
            str(package), "--path", relative, "--apply"]
    result = {"schema_version": 1, "kind": KIND, "package": str(package),
              "original": {"path": relative, **original},
              "compressed": {"path": _locations(relative)[0], "format": "gzip",
                             "level": 6, **compressed},
              "restore_argv": argv, "restore_command": shlex.join(argv)}
    return {**result, "manifest_sha256": hashlib.sha256(_canonical(result)).hexdigest()}


def _read_manifest(parent: int, name: str, package: Path, relative: str) -> dict:
    with _regular(parent, name) as source:
        raw = source.read(64 * 1024 + 1)
        if len(raw) > 64 * 1024:
            raise StorageError("cold-storage manifest exceeds the metadata limit")
    value = _json(raw)
    core = {key: item for key, item in value.items() if key != "manifest_sha256"}
    if value.get("manifest_sha256") != hashlib.sha256(_canonical(core)).hexdigest():
        raise StorageError("cold-storage manifest digest changed")
    original, compressed = value.get("original"), value.get("compressed")
    if not isinstance(original, dict) or not isinstance(compressed, dict):
        raise StorageError("invalid cold-storage manifest")
    for entry in (original, compressed):
        if (not isinstance(entry.get("sha256"), str)
                or not re.fullmatch("[0-9a-f]{64}", entry["sha256"])
                or type(entry.get("size")) is not int or not 0 <= entry["size"] < 2 ** 63):
            raise StorageError("invalid cold-storage size or SHA256")
    if (type(original.get("mode")) is not int or not 0 <= original["mode"] <= 0o7777
            or type(original.get("mtime_ns")) is not int
            or not -(2 ** 63) <= original["mtime_ns"] < 2 ** 63
            or set(original) != {"path", "sha256", "size", "mode", "mtime_ns"}
            or set(compressed) != {"path", "sha256", "size", "format", "level"}):
        raise StorageError("invalid original metadata")
    expected = _manifest(package, relative,
                         {key: original[key] for key in ("sha256", "size", "mode", "mtime_ns")},
                         {key: compressed[key] for key in ("sha256", "size")})
    if value != expected:
        raise StorageError("cold-storage manifest identity or digest changed")
    return value


def _inflate(source: BinaryIO, expected: dict, output: BinaryIO | None = None) -> None:
    source.seek(0)
    digest, size = hashlib.sha256(), 0
    try:
        with gzip.GzipFile(fileobj=source, mode="rb") as uncompressed:
            while True:
                block = uncompressed.read(min(CHUNK, expected["size"] - size + 1))
                if not block:
                    break
                size += len(block)
                if size > expected["size"]:
                    raise StorageError("gzip expands past the original size")
                digest.update(block)
                if output is not None:
                    output.write(block)
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise StorageError("gzip verification failed: " + str(exc)) from exc
    if size != expected["size"] or digest.hexdigest() != expected["sha256"]:
        raise StorageError("gzip does not restore the original SHA256/size")


def _verify_archive(parent: int, name: str, package: Path, relative: str,
                    output: BinaryIO | None = None) -> dict:
    manifest = _read_manifest(parent, name + ".json", package, relative)
    with _regular(parent, name) as source:
        original = os.fstat(source.fileno())
        actual = _digest(source)
        if actual != {key: manifest["compressed"][key] for key in ("sha256", "size")}:
            raise StorageError("compressed SHA256/size changed")
        _inflate(source, manifest["original"], output)
        _assert_file(parent, name, source, original)
    return manifest


@contextmanager
def _temporary(parent: int) -> Iterator[tuple[str, BinaryIO]]:
    name = ".cold-storage-" + uuid.uuid4().hex + ".tmp"
    descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=parent)
    try:
        with os.fdopen(descriptor, "w+b") as output:
            yield name, output
    finally:
        try:
            os.unlink(name, dir_fd=parent)
        except FileNotFoundError:
            pass


def _publish(parent: int, temporary: str, name: str) -> None:
    # Same-directory hard-link publication is atomic and never replaces a name.
    os.link(temporary, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)


def _source_matches(parent: int, name: str, original: dict) -> None:
    with _regular(parent, name) as source:
        info = os.fstat(source.fileno())
        if (_digest(source) != {key: original[key] for key in ("sha256", "size")}
                or stat.S_IMODE(info.st_mode) != original["mode"]
                or info.st_mtime_ns != original["mtime_ns"]):
            raise StorageError("existing original differs from the cold-storage record")
        _assert_file(parent, name, source, info)


def _receipt(relative: str, status: str, manifest: dict) -> dict:
    return {"path": relative, "status": status,
            "original_size": manifest["original"]["size"],
            "compressed_size": manifest["compressed"]["size"],
            "manifest": _locations(relative)[1]}


def pack_file(package: Path, relative: str, *, apply: bool = False) -> dict:
    """Verify one selected report; replace it with checked cold storage on apply."""
    with _package(package, relative) as (package, root, task_digest):
        compressed, metadata = _locations(relative)
        existing = (_exists(root, compressed), _exists(root, metadata))
        if any(existing):
            if not all(existing):
                raise StorageError("incomplete cold-storage entry; original is retained")
            with _parent(root, compressed) as (archive_parent, archive_name):
                manifest = _verify_archive(archive_parent, archive_name, package, relative)
                if not _exists(root, relative):
                    return _receipt(relative, "already_packed", manifest)
                with _parent(root, relative) as (parent, name):
                    _source_matches(parent, name, manifest["original"])
                    if apply:
                        _guard(package, root, task_digest, relative, parent)
                        _source_matches(parent, name, manifest["original"])
                        _guard(package, root, task_digest, compressed, archive_parent)
                        os.unlink(name, dir_fd=parent)
            return _receipt(relative, "packed" if apply else "would_pack", manifest)
        with _parent(root, relative) as (parent, name), _regular(parent, name) as source:
            original_stat = os.fstat(source.fileno())
            original = {**_digest(source), "mode": stat.S_IMODE(original_stat.st_mode),
                        "mtime_ns": original_stat.st_mtime_ns}
            _assert_file(parent, name, source, original_stat)
            if not apply:
                return {"path": relative, "status": "would_pack", "original_size": original["size"]}
            _guard(package, root, task_digest, relative, parent)
            with _parent(root, compressed, create=True) as (archive_parent, archive_name):
                with _temporary(archive_parent) as (temporary, output):
                    source.seek(0)
                    with gzip.GzipFile(filename="", fileobj=output, mode="wb",
                                       compresslevel=6, mtime=0) as archive:
                        for block in iter(lambda: source.read(CHUNK), b""):
                            archive.write(block)
                    output.flush()
                    os.fsync(output.fileno())
                    compressed_info = _digest(output)
                    _inflate(output, original)
                    manifest = _manifest(package, relative, original, compressed_info)
                    manifest_bytes = _canonical(manifest) + b"\n"
                    if compressed_info["size"] + len(manifest_bytes) >= original["size"]:
                        return _receipt(relative, "no_savings", manifest)
                    _assert_file(parent, name, source, original_stat)
                    if _digest(source) != {key: original[key] for key in ("sha256", "size")}:
                        raise StorageError("source bytes changed during compression")
                    _guard(package, root, task_digest, compressed, archive_parent)
                    with _temporary(archive_parent) as (metadata_temp, metadata_output):
                        metadata_output.write(manifest_bytes)
                        metadata_output.flush()
                        os.fsync(metadata_output.fileno())
                        _publish(archive_parent, temporary, archive_name)
                        _publish(archive_parent, metadata_temp, archive_name + ".json")
                    _verify_archive(archive_parent, archive_name, package, relative)
                    _guard(package, root, task_digest, relative, parent)
                    _assert_file(parent, name, source, original_stat)
                    if _digest(source) != {key: original[key] for key in ("sha256", "size")}:
                        raise StorageError("source bytes changed before removal")
                    _guard(package, root, task_digest, compressed, archive_parent)
                    _assert_file(parent, name, source, original_stat)
                    os.unlink(name, dir_fd=parent)
                    return _receipt(relative, "packed", manifest)


def restore_file(package: Path, relative: str, *, apply: bool = False) -> dict:
    """Restore checked bytes without overwriting; retain compressed audit records."""
    with _package(package, relative) as (package, root, task_digest):
        compressed, _ = _locations(relative)
        with _parent(root, compressed) as (archive_parent, archive_name):
            if _exists(root, relative):
                manifest = _verify_archive(archive_parent, archive_name, package, relative)
                with _parent(root, relative) as (parent, name):
                    _source_matches(parent, name, manifest["original"])
                return _receipt(relative, "already_restored", manifest)
            if not apply:
                manifest = _verify_archive(archive_parent, archive_name, package, relative)
                return _receipt(relative, "would_restore", manifest)
            with _parent(root, relative, create=True) as (parent, name):
                with _temporary(parent) as (temporary, output):
                    manifest = _verify_archive(archive_parent, archive_name, package, relative, output)
                    output.flush()
                    original = manifest["original"]
                    os.fchmod(output.fileno(), original["mode"])
                    os.utime(output.fileno(), ns=(original["mtime_ns"], original["mtime_ns"]))
                    os.fsync(output.fileno())
                    _guard(package, root, task_digest, relative, parent)
                    _publish(parent, temporary, name)
                return _receipt(relative, "restored", manifest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("pack", "restore"))
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--path", action="append", required=True, dest="paths")
    parser.add_argument("--apply", action="store_true", help="apply; default is read-only")
    args = parser.parse_args(argv)
    results = []
    for relative in dict.fromkeys(args.paths):
        try:
            operation = pack_file if args.operation == "pack" else restore_file
            results.append(operation(args.package, relative, apply=args.apply))
        except (StorageError, OSError, UnicodeError, ValueError) as exc:
            results.append({"path": relative, "status": "error", "error": str(exc)})
    failed = any(row["status"] == "error" for row in results)
    print(json.dumps({"operation": args.operation, "apply": args.apply,
                      "results": results}, ensure_ascii=False, sort_keys=True))
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
