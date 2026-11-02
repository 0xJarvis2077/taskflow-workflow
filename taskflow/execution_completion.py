"""Receive a terminated original attempt at normal consumer boundaries."""

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import stat
from typing import Iterator

from taskflow.contracts import TaskflowError
from taskflow.execution_store import ACTIVE


@contextmanager
def completion_owner(root: Path) -> Iterator[bool]:
    """Exclude a second collector, including threads in the same process."""
    root = Path(root).absolute()
    lock = root.with_name(root.name + ".completion-owner.lock")
    descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise TaskflowError("completion owner lock must be an ordinary file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def reconcile_ended_attempt(store: object, attempt: dict | None) -> dict | None:
    """Collect the same attempt only when its original termination exists.

    The canonical reconciler shares the worker's completion-owner mutex and owns
    receipt validation, collection seals and the terminal database transition;
    absence, elapsed time or a process lookup never authorizes another call.
    """
    if attempt is None or attempt["state"] not in ACTIVE:
        return attempt
    termination = Path(attempt["runtime_path"]) / "termination.json"
    try:
        metadata = termination.lstat()
    except FileNotFoundError:
        return attempt
    if not stat.S_ISREG(metadata.st_mode):
        raise TaskflowError("original termination evidence must be an ordinary file")
    from taskflow.execution import reconcile

    return reconcile(store, attempt["attempt_id"])
