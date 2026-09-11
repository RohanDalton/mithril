"""
File leases with OS-protected metadata and per-holder crash detection.

A lease may expire while its process is still alive. Downstream writes must use
fencing tokens, just as with the network backends. Drain holders before upgrading
from the original append-only metadata format.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .._fencing import FileFencingTokenProvider
from .._file_io import (
    file_mutex,
    lock_file,
    open_file,
    read_file,
    replace_file,
    unlock_file,
    validate_component,
)
from .._types import LockMode, Result, validate_duration
from ..exceptions import BackendError
from ._redis_common import validate_mode

__author__ = "Rohan B. Dalton"


class FileBackend:
    """
    Single-host leases with expiry, fencing, and OS-based process-crash detection.
    """

    def __init__(self, directory: str | Path, *, namespace: str = "mithril") -> None:
        validate_component(namespace)
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._namespace = namespace
        self._fencing = FileFencingTokenProvider(self._directory, namespace=namespace)
        self._mutex = threading.RLock()
        self._held: dict[tuple[str, str], tuple[Any, float]] = {}

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | None:
        """
        Atomically acquire a lease and issue a persistent fencing token and epoch.
        """
        validate_duration(ttl_seconds, "ttl_seconds")
        validate_mode(mode)
        self._reap_local()
        key = (lock_name, owner_id)
        with self._mutex:
            already_held = key in self._held
            try:
                with self._transaction(lock_name) as state:
                    if state["writer"] or (mode == LockMode.EXCLUSIVE and state["readers"]):
                        return None
                    if key not in self._held:
                        handle = open_file(self._owner_path(lock_name, owner_id))
                        if not lock_file(handle, blocking=False):
                            handle.close()
                            return None
                        self._held[key] = (handle, time.monotonic() + ttl_seconds)
                    token = self._fencing.next_token(lock_name)
                    now = time.monotonic()
                    entry = {
                        "owner": owner_id,
                        "expire_at": now + ttl_seconds,
                        "token": token,
                        "epoch": token,
                        "acquired_at": now,
                    }
                    if mode == LockMode.SHARED:
                        previous = state["readers"].get(owner_id)
                        entry["count"] = previous["count"] + 1 if previous else 1
                        state["readers"][owner_id] = entry
                    else:
                        state["writer"] = entry
                    self._held[key] = (self._held[key][0], entry["expire_at"])
                    return token, token
            except BaseException:
                if not already_held:
                    self._drop(lock_name, owner_id)
                raise

    def release(
        self, lock_name: str, owner_id: str, mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        """
        Release only a live lease owned by the caller.
        """
        validate_mode(mode)
        with self._transaction(lock_name) as state:
            if mode == LockMode.EXCLUSIVE:
                if not (entry := state["writer"]):
                    return Result.NOT_FOUND
                if entry["owner"] != owner_id:
                    return Result.WRONG_OWNER
                state["writer"] = None
            else:
                if not (entry := state["readers"].get(owner_id)):
                    return Result.WRONG_OWNER if state["readers"] else Result.NOT_FOUND
                if entry["count"] > 1:
                    entry["count"] -= 1
                    return Result.SUCCESS
                del state["readers"][owner_id]
            self._drop(lock_name, owner_id)
            return Result.SUCCESS

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Refresh a live lease without reviving expired ownership.
        """
        validate_mode(mode)
        validate_duration(ttl_seconds, "ttl_seconds")
        with self._transaction(lock_name) as state:
            entry = (
                state["writer"]
                if mode == LockMode.EXCLUSIVE
                else state["readers"].get(owner_id)
            )
            if entry is None:
                return Result.WRONG_OWNER if state["readers"] else Result.NOT_FOUND
            if entry["owner"] != owner_id:
                return Result.WRONG_OWNER
            entry["expire_at"] = time.monotonic() + ttl_seconds
            if held := self._held.get((lock_name, owner_id)):
                self._held[(lock_name, owner_id)] = (held[0], entry["expire_at"])
            return Result.SUCCESS

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return the exclusive owner, or None for an unlocked/shared lock.
        """
        with self._transaction(lock_name) as state:
            return entry["owner"] if (entry := state["writer"]) else None

    def is_locked(self, lock_name: str) -> bool:
        """
        Return whether an exclusive owner or any live reader exists.
        """
        with self._transaction(lock_name) as state:
            return bool(state["writer"] or state["readers"])

    @contextmanager
    def _transaction(self, lock_name: str):
        path = self._lock_path(lock_name)
        with self._mutex, file_mutex(path.with_suffix(".lock.guard")):
            if (data := read_file(path)) is None:
                state = {"writer": None, "readers": {}}
            else:
                try:
                    state = json.loads(data)
                    if set(state) != {"writer", "readers"} or not isinstance(
                        state["readers"], dict
                    ):
                        raise ValueError("invalid lease metadata")
                except (ValueError, TypeError) as exc:
                    raise BackendError(f"Invalid file lease metadata: {path}") from exc
            self._prune(lock_name, state)
            for held_name, owner in list(self._held):
                if held_name == lock_name and owner not in state["readers"]:
                    if not state["writer"] or state["writer"]["owner"] != owner:
                        self._drop(lock_name, owner)
            yield state
            replace_file(path, json.dumps(state, separators=(",", ":")))

    def _prune(self, lock_name: str, state: dict) -> None:
        now = time.monotonic()
        if entry := state["writer"]:
            if entry["expire_at"] <= now or not self._alive(lock_name, entry["owner"]):
                self._drop(lock_name, entry["owner"])
                state["writer"] = None
        for owner_id, entry in list(state["readers"].items()):
            if entry["expire_at"] <= now or not self._alive(lock_name, owner_id):
                self._drop(lock_name, owner_id)
                del state["readers"][owner_id]

    def _alive(self, lock_name: str, owner_id: str) -> bool:
        if (lock_name, owner_id) in self._held:
            return True
        try:
            with open_file(self._owner_path(lock_name, owner_id), create=False) as handle:
                if not lock_file(handle, blocking=False):
                    return True
                unlock_file(handle)
                return False
        except FileNotFoundError:
            return False

    def _drop(self, lock_name: str, owner_id: str) -> None:
        if held := self._held.pop((lock_name, owner_id), None):
            unlock_file(held[0])
            held[0].close()
        # A different process may still have its expired handle open on Windows.
        try:
            self._owner_path(lock_name, owner_id).unlink(missing_ok=True)
        except PermissionError:
            pass

    def _reap_local(self) -> None:
        with self._mutex:
            names = {
                name
                for (name, owner), held in self._held.items()
                if held[1] <= time.monotonic()
            }
            for name in names:
                with self._transaction(name) as state:
                    for held_name, owner in list(self._held):
                        if held_name == name and owner not in state["readers"]:
                            if not state["writer"] or state["writer"]["owner"] != owner:
                                self._drop(name, owner)

    def _lock_path(self, lock_name: str) -> Path:
        validate_component(lock_name)
        return self._directory / f"{self._namespace}.{lock_name}.lock"

    def _owner_path(self, lock_name: str, owner_id: str) -> Path:
        validate_component(lock_name)
        digest = hashlib.sha256(owner_id.encode("utf-8")).hexdigest()
        return self._directory / f"{self._namespace}.{lock_name}.{digest}.owner"


if __name__ == "__main__":
    pass
