"""
Fencing token providers for Mithril.
"""

from __future__ import annotations

import os
import threading
from typing import Any
from pathlib import Path
from typing import Protocol

__author__ = "Rohan B. Dalton"


class FencingTokenProvider(Protocol):
    """
    Protocol for fencing token providers.
    """

    def next_token(self, lock_name: str) -> int: ...

    def current_token(self, lock_name: str) -> int: ...


class InMemoryFencingTokenProvider:
    """
    Thread-safe in-memory fencing token provider.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: dict[str, int] = {}

    def next_token(self, lock_name: str) -> int:
        with self._lock:
            current = self._tokens.get(lock_name, 0)
            next_value = current + 1
            self._tokens[lock_name] = next_value
            return next_value

    def current_token(self, lock_name: str) -> int:
        with self._lock:
            return self._tokens.get(lock_name, 0)


class FileFencingTokenProvider:
    """
    File-backed fencing token provider.
    """

    def __init__(self, directory: str | Path, namespace: str = "mithril") -> None:
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._namespace = namespace

    def next_token(self, lock_name: str) -> int:
        path = self._path_for(lock_name)
        path.touch(exist_ok=True)
        with path.open("r+", encoding="ascii") as fh:
            self._lock_file(fh)
            try:
                current = self._read_counter(fh)
                next_value = current + 1
                self._write_counter(fh, next_value)
                return next_value
            finally:
                self._unlock_file(fh)

    def current_token(self, lock_name: str) -> int:
        if not (path := self._path_for(lock_name)).exists():
            return 0
        with path.open("r+", encoding="ascii") as fh:
            self._lock_file(fh)
            try:
                return self._read_counter(fh)
            finally:
                self._unlock_file(fh)

    def _path_for(self, lock_name: str) -> Path:
        if os.sep in lock_name or (os.altsep and os.altsep in lock_name):
            raise ValueError("lock_name must not contain path separators")
        filename = f"{self._namespace}.{lock_name}.fence"
        return self._directory / filename

    @staticmethod
    def _read_counter(handle) -> int:
        handle.seek(0)
        if not (data := handle.read().strip()):
            return 0
        try:
            return int(data, 10)
        except ValueError:
            return 0

    @staticmethod
    def _write_counter(handle, value: int) -> None:
        handle.seek(0)
        handle.write(str(value))
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())

    @staticmethod
    def _lock_file(handle) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write("0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)

    @staticmethod
    def _unlock_file(handle) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class FencingGuard:
    """
    Helper for downstream systems to validate fencing tokens.
    """

    def validate_and_update(self, lock_name: str, token: int) -> bool:
        """
        Validate a fencing token and update the stored max.
        """
        raise NotImplementedError("Provide a backend-specific implementation")


class RedisFencingGuard(FencingGuard):
    """
    Redis-backed fencing token guard.
    """

    _SCRIPT = """
    local current = redis.call('GET', KEYS[1])
    if not current then
        redis.call('SET', KEYS[1], ARGV[1])
        return 1
    end
    if tonumber(ARGV[1]) > tonumber(current) then
        redis.call('SET', KEYS[1], ARGV[1])
        return 1
    end
    return 0
    """

    def __init__(self, client: Any, *, namespace: str = "mithril") -> None:
        self._client = client
        self._namespace = namespace
        self._script = client.register_script(self._SCRIPT)

    def validate_and_update(self, lock_name: str, token: int) -> bool:
        """
        Validate a fencing token and update the stored max.
        """
        key = self._key(lock_name)
        result = self._script(keys=[key], args=[token])
        return bool(result)

    def _key(self, lock_name: str) -> str:
        if not self._namespace:
            return f"fence:{lock_name}:max"
        return f"{self._namespace}:fence:{lock_name}:max"


if __name__ == "__main__":
    pass
