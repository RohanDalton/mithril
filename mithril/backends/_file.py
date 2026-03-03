"""
File-based backend for single-host locking.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from .._fencing import FileFencingTokenProvider
from .._types import LockMode, Result

__author__ = "Rohan B. Dalton"


class FileBackend:
    """
    File backend for inter-process locks on a single host.
    """

    def __init__(self, directory: str | Path, *, namespace: str = "mithril") -> None:
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._namespace = namespace
        self._fencing = FileFencingTokenProvider(self._directory, namespace=namespace)
        self._held: dict[str, tuple[Any, str, float]] = {}

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | None:
        """
        Attempt to acquire the file lock.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        if mode == LockMode.SHARED:
            return self._acquire_shared(lock_name, owner_id, ttl_seconds, epoch)
        if lock_name in self._held:
            return None
        path = self._lock_path(lock_name)
        fh = self._open_lock_file(path)
        try:
            if not self._lock_file(fh, blocking=False):
                return None
            if self._active_readers_exist(lock_name):
                return None
            previous = self._read_last_entry(fh)
            next_epoch = (previous.get("epoch", 0) if previous else 0) + 1
            now = time.monotonic()
            expire_at = now + ttl_seconds
            token = self._fencing.next_token(lock_name)
            entry = {
                "owner": owner_id,
                "epoch": next_epoch if epoch <= 0 else epoch,
                "token": token,
                "acquired_at": now,
                "expire_at": expire_at,
            }
            self._append_entry(fh, entry)
            self._held[lock_name] = (fh, owner_id, expire_at)
            return token, entry["epoch"]
        finally:
            if lock_name not in self._held:
                self._unlock_file(fh)
                fh.close()

    def release(
        self, lock_name: str, owner_id: str, mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        """
        Release the file lock if owned by the caller.
        """
        if mode == LockMode.SHARED:
            return self._release_shared(lock_name, owner_id)
        path = self._lock_path(lock_name)
        if not path.exists():
            return Result.NOT_FOUND
        held = self._held.get(lock_name)
        fh = held[0] if held else self._open_lock_file(path)
        released = False
        try:
            if held is None and not self._lock_file(fh, blocking=True):
                return Result.WRONG_OWNER
            current = self._read_last_entry(fh)
            if not current or current.get("owner") != owner_id:
                return Result.WRONG_OWNER
            current["released_at"] = time.monotonic()
            self._append_entry(fh, current)
            released = True
            return Result.SUCCESS
        finally:
            if held is None:
                self._unlock_file(fh)
                fh.close()
            elif released:
                self._unlock_file(fh)
                fh.close()
                self._held.pop(lock_name, None)

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Renew the file lock TTL if owned by the caller.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        if mode == LockMode.SHARED:
            return self._renew_shared(lock_name, owner_id, ttl_seconds)
        path = self._lock_path(lock_name)
        if not path.exists():
            return Result.NOT_FOUND

        if held := self._held.get(lock_name):
            fh = held[0]
        else:
            fh = self._open_lock_file(path)

        try:
            if held is None and not self._lock_file(fh, blocking=True):
                return Result.WRONG_OWNER
            current = self._read_last_entry(fh)
            if not current or current.get("owner") != owner_id:
                return Result.WRONG_OWNER
            now = time.monotonic()
            expire_at = now + ttl_seconds
            current["expire_at"] = expire_at
            self._append_entry(fh, current)
            if held is not None:
                self._held[lock_name] = (fh, owner_id, expire_at)
            return Result.SUCCESS
        finally:
            if held is None:
                self._unlock_file(fh)
                fh.close()

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if the lock is held.
        """
        held = self._held.get(lock_name)
        if held is not None:
            if self._is_expired({"expire_at": held[2]}):
                self._unlock_file(held[0])
                held[0].close()
                self._held.pop(lock_name, None)
                return None
            return held[1]
        path = self._lock_path(lock_name)
        if not path.exists():
            return None
        fh = self._open_lock_file(path)
        try:
            if not self._lock_file(fh, blocking=False):
                return None
            current = self._read_last_entry(fh)
            if not current:
                return None
            if self._is_expired(current):
                return None
            return str(current.get("owner"))
        finally:
            self._unlock_file(fh)
            fh.close()

    def is_locked(self, lock_name: str) -> bool:
        """
        Return True if the lock is currently held.
        """
        held = self._held.get(lock_name)
        if held is not None:
            if self._is_expired({"expire_at": held[2]}):
                self._unlock_file(held[0])
                held[0].close()
                self._held.pop(lock_name, None)
                return False
            return True
        path = self._lock_path(lock_name)
        if not path.exists():
            return self._shared_readers_exist(lock_name)
        fh = self._open_lock_file(path)
        try:
            locked = not self._lock_file(fh, blocking=False)
            return locked
        finally:
            self._unlock_file(fh)
            fh.close()

    def _acquire_shared(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
    ) -> tuple[int, int] | None:
        writer_path = self._lock_path(lock_name)
        writer_fh = self._open_lock_file(writer_path)
        try:
            if not self._lock_file(writer_fh, blocking=False):
                return None
            readers_path = self._readers_path(lock_name)
            readers_fh = self._open_lock_file(readers_path)
            try:
                if not self._lock_file(readers_fh, blocking=True):
                    return None
                now = time.monotonic()
                readers = self._load_readers(readers_fh)
                readers = self._prune_readers(readers, now)
                entry = readers.get(owner_id)
                count = entry["count"] if entry else 0
                readers[owner_id] = {"count": count + 1, "expire_at": now + ttl_seconds}
                self._write_readers(readers_fh, readers)
                token = self._fencing.next_token(lock_name)
                return token, epoch
            finally:
                self._unlock_file(readers_fh)
                readers_fh.close()
        finally:
            self._unlock_file(writer_fh)
            writer_fh.close()

    def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        readers_path = self._readers_path(lock_name)
        if not readers_path.exists():
            return Result.NOT_FOUND
        fh = self._open_lock_file(readers_path)
        try:
            if not self._lock_file(fh, blocking=True):
                return Result.WRONG_OWNER
            now = time.monotonic()
            readers = self._load_readers(fh)
            readers = self._prune_readers(readers, now)
            entry = readers.get(owner_id)
            if entry is None:
                return Result.WRONG_OWNER if readers else Result.NOT_FOUND
            if entry["count"] > 1:
                entry["count"] -= 1
                readers[owner_id] = entry
            else:
                readers.pop(owner_id, None)
            self._write_readers(fh, readers)
            return Result.SUCCESS
        finally:
            self._unlock_file(fh)
            fh.close()

    def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        readers_path = self._readers_path(lock_name)
        if not readers_path.exists():
            return Result.NOT_FOUND
        fh = self._open_lock_file(readers_path)
        try:
            if not self._lock_file(fh, blocking=True):
                return Result.WRONG_OWNER
            now = time.monotonic()
            readers = self._load_readers(fh)
            readers = self._prune_readers(readers, now)
            entry = readers.get(owner_id)
            if entry is None:
                return Result.WRONG_OWNER if readers else Result.NOT_FOUND
            entry["expire_at"] = now + ttl_seconds
            readers[owner_id] = entry
            self._write_readers(fh, readers)
            return Result.SUCCESS
        finally:
            self._unlock_file(fh)
            fh.close()

    def _active_readers_exist(self, lock_name: str) -> bool:
        """Check for active readers while already holding the writer flock."""
        readers_path = self._readers_path(lock_name)
        if not readers_path.exists():
            return False
        fh = self._open_lock_file(readers_path)
        try:
            if not self._lock_file(fh, blocking=True):
                return True
            readers = self._load_readers(fh)
            readers = self._prune_readers(readers, time.monotonic())
            self._write_readers(fh, readers)
            return bool(readers)
        finally:
            self._unlock_file(fh)
            fh.close()

    def _shared_readers_exist(self, lock_name: str) -> bool:
        readers_path = self._readers_path(lock_name)
        if not readers_path.exists():
            return False
        fh = self._open_lock_file(readers_path)
        try:
            if not self._lock_file(fh, blocking=False):
                return True
            readers = self._load_readers(fh)
            readers = self._prune_readers(readers, time.monotonic())
            self._write_readers(fh, readers)
            return bool(readers)
        finally:
            self._unlock_file(fh)
            fh.close()

    def _readers_path(self, lock_name: str) -> Path:
        filename = f"{self._namespace}.{lock_name}.readers"
        return self._directory / filename

    def _load_readers(self, fh) -> dict[str, dict[str, float]]:
        fh.seek(0)
        data = fh.read().strip()
        if not data:
            return {}
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return {}

    def _write_readers(self, fh, readers: dict[str, dict[str, float]]) -> None:
        fh.seek(0)
        fh.write(json.dumps(readers, separators=(",", ":")))
        fh.truncate()
        fh.flush()
        os.fsync(fh.fileno())

    def _prune_readers(
        self, readers: dict[str, dict[str, float]], now: float
    ) -> dict[str, dict[str, float]]:
        return {
            owner: entry
            for owner, entry in readers.items()
            if float(entry.get("expire_at", 0)) > now
        }

    def _lock_path(self, lock_name: str) -> Path:
        if os.sep in lock_name or (os.altsep and os.altsep in lock_name):
            raise ValueError("lock_name must not contain path separators")
        filename = f"{self._namespace}.{lock_name}.lock"
        return self._directory / filename

    def _open_lock_file(self, path: Path):
        if path.exists() and path.is_symlink():
            raise ValueError("lock path must not be a symlink")
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        return os.fdopen(fd, "r+", encoding="utf-8")

    def _append_entry(self, fh, entry: dict[str, Any]) -> None:
        fh.seek(0, os.SEEK_END)
        fh.write(json.dumps(entry, separators=(",", ":")) + "\n")
        fh.flush()
        os.fsync(fh.fileno())

    def _read_last_entry(self, fh) -> dict[str, Any] | None:
        fh.seek(0)
        lines = [line for line in fh.read().splitlines() if line.strip()]
        if not lines:
            return None
        try:
            return json.loads(lines[-1])
        except json.JSONDecodeError:
            return None

    def _is_expired(self, entry: dict[str, Any]) -> bool:
        expire_at = entry.get("expire_at")
        if expire_at is None:
            return False
        try:
            return time.monotonic() > float(expire_at)
        except (TypeError, ValueError):
            return False

    def _lock_file(self, fh, *, blocking: bool) -> bool:
        if os.name == "nt":
            import msvcrt

            self._ensure_lock_byte(fh)
            mode = msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK
            try:
                msvcrt.locking(fh.fileno(), mode, 1)
                return True
            except OSError:
                return False
        else:
            import fcntl

            flags = fcntl.LOCK_EX
            if not blocking:
                flags |= fcntl.LOCK_NB
            try:
                fcntl.flock(fh.fileno(), flags)
                return True
            except OSError:
                return False

    def _unlock_file(self, fh) -> None:
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl

            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass

    def _ensure_lock_byte(self, fh) -> None:
        fh.seek(0, os.SEEK_END)
        if fh.tell() == 0:
            fh.write("0\n")
            fh.flush()


if __name__ == "__main__":
    pass
