"""
Safe file opening, fixed-byte mutexes, and durable metadata replacement.
"""

from __future__ import annotations

import errno
import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .exceptions import BackendError, ConfigurationError

if os.name == "nt":
    import msvcrt
else:
    import fcntl

__author__ = "Rohan B. Dalton"


def validate_component(value: str) -> None:
    if not isinstance(value, str) or not value or "/" in value or "\\" in value:
        raise ConfigurationError("file names and namespaces must be nonempty path components")


def open_file(path: Path, *, create: bool = True):
    if path.is_symlink():
        raise ValueError("lock path must not be a symlink")
    flags = os.O_RDWR | (os.O_CREAT if create else 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("lock path must be a regular file with no hard links")
        return os.fdopen(fd, "r+", encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise


def lock_file(handle, *, blocking: bool) -> bool:
    if os.name == "nt":
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write("\0")
            handle.flush()
        handle.seek(0)
        mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
        try:
            msvcrt.locking(handle.fileno(), mode, 1)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                return False
            raise
    else:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(handle.fileno(), flags)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                return False
            raise
    return True


def unlock_file(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def file_mutex(path: Path):
    with open_file(path) as fh:
        if not lock_file(fh, blocking=True):
            raise BackendError("Could not acquire file metadata mutex")
        try:
            yield
        finally:
            unlock_file(fh)


def read_file(path: Path) -> str | None:
    try:
        with open_file(path, create=False) as fh:
            return fh.read()
    except FileNotFoundError:
        return None


def replace_file(path: Path, data: str) -> None:
    """
    Replace metadata while a separate, stable mutex file is locked.
    """
    if path.is_symlink():
        raise ValueError("lock path must not be a symlink")
    fd, temporary = tempfile.mkstemp(prefix=".mithril-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
