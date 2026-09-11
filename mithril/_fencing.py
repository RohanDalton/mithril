"""
Fencing token providers for Mithril.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

from ._file_io import file_mutex, read_file, replace_file, validate_component
from .exceptions import FencingTokenError

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
    Durable counters protected by a separate OS mutex and atomic replacement.
    """

    def __init__(self, directory: str | Path, namespace: str = "mithril") -> None:
        validate_component(namespace)
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._namespace = namespace

    def next_token(self, lock_name: str) -> int:
        path = self._path_for(lock_name)
        with file_mutex(path.with_suffix(".fence.guard")):
            current = self._read_counter(path)
            replace_file(path, str(value := current + 1))
            return value

    def current_token(self, lock_name: str) -> int:
        path = self._path_for(lock_name)
        with file_mutex(path.with_suffix(".fence.guard")):
            return self._read_counter(path)

    def _path_for(self, lock_name: str) -> Path:
        validate_component(lock_name)
        return self._directory / f"{self._namespace}.{lock_name}.fence"

    @staticmethod
    def _read_counter(path: Path) -> int:
        if (data := read_file(path)) is None:
            return 0
        if not data.isascii() or not data.isdecimal():
            raise FencingTokenError(f"Invalid fencing counter: {path}")
        return int(data)


class FencingGuard:
    """
    Helper for downstream systems to validate fencing tokens.
    """

    def validate_and_update(self, lock_name: str, token: int) -> bool:
        """
        Validate a fencing token and update the stored max.
        """
        raise NotImplementedError("Provide a backend-specific implementation")


class RedisFencingTokenProvider:
    """
    Explicit fencing authority with persistent counters on one Redis service.

    The service must preserve its counters across failures. Configure bounded
    client timeouts; availability of this authority is required by Redlock.
    """

    _SCRIPT = """
    redis.call('INCR', KEYS[1])
    redis.call('PERSIST', KEYS[1])
    return redis.call('GET', KEYS[1])
    """

    def __init__(self, client: Any, *, namespace: str = "mithril") -> None:
        self._client = client
        self._namespace = namespace
        self._script = client.register_script(self._SCRIPT)

    def _key(self, lock_name: str) -> str:
        return f"{quote(self._namespace, safe='')}:authority:{quote(lock_name, safe='')}"

    def next_token(self, lock_name: str) -> int:
        return int(self._script(keys=[self._key(lock_name)]))

    def current_token(self, lock_name: str) -> int:
        return int(self._client.get(self._key(lock_name)) or 0)


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
