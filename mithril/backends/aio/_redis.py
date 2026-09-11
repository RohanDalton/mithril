"""
Async Redis backend for Mithril.
"""

from __future__ import annotations

import time
from typing import Any

from ..._types import LockMode, Result
from .. import _redis_scripts
from .._redis_common import RedisKeys, ttl_milliseconds, validate_mode

__author__ = "Rohan B. Dalton"

_ACQUIRE_SCRIPT = _redis_scripts.ACQUIRE
_RELEASE_SCRIPT = _redis_scripts.RELEASE
_RENEW_SCRIPT = _redis_scripts.RENEW
_ACQUIRE_READ_SCRIPT = _redis_scripts.ACQUIRE_READ
_RELEASE_READ_SCRIPT = _redis_scripts.RELEASE_READ
_RENEW_READ_SCRIPT = _redis_scripts.RENEW_READ


class AsyncRedisBackend(RedisKeys):
    """
    Async Redis backend that enforces atomic lock mutations.
    """

    def __init__(
        self,
        client: Any,
        *,
        namespace: str = "",
        signal_ttl: float = 5.0,
    ) -> None:
        ttl_milliseconds(signal_ttl)
        self._client = client
        self._namespace = namespace
        self._signal_ttl = signal_ttl
        self._acquire_script = client.register_script(_ACQUIRE_SCRIPT)
        self._release_script = client.register_script(_RELEASE_SCRIPT)
        self._renew_script = client.register_script(_RENEW_SCRIPT)
        self._acquire_read_script = client.register_script(_ACQUIRE_READ_SCRIPT)
        self._release_read_script = client.register_script(_RELEASE_READ_SCRIPT)
        self._renew_read_script = client.register_script(_RENEW_READ_SCRIPT)
        self._is_locked_script = client.register_script(_redis_scripts.IS_LOCKED)

    async def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
        acquired_at: float | None = None,
    ) -> tuple[int, int] | None:
        """
        Acquire the lock and return (fencing_token, epoch) on success.
        """
        validate_mode(mode)
        ttl_ms = ttl_milliseconds(ttl_seconds)
        if acquired_at is None:
            acquired_at = time.monotonic()
        keys = [
            self._lock_key(lock_name),
            self._fence_key(lock_name),
            self._signal_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
        ]
        args = [owner_id, ttl_ms, epoch, acquired_at]
        if mode == LockMode.SHARED:
            result = await self._acquire_read_script(keys=keys, args=args)
        else:
            result = await self._acquire_script(keys=keys, args=args)
        if not result or int(result[0]) == -1:
            return None
        return int(result[0]), int(result[1])

    async def release(
        self,
        lock_name: str,
        owner_id: str,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Release the lock, returning Result.SUCCESS on success.
        """
        validate_mode(mode)
        signal_ttl_ms = int(self._signal_ttl * 1000)
        signal_ttl_ms = max(signal_ttl_ms, 1)
        keys = [
            self._lock_key(lock_name),
            self._signal_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
        ]
        args = [owner_id, signal_ttl_ms]
        if mode == LockMode.SHARED:
            return Result(int(await self._release_read_script(keys=keys, args=args)))
        return Result(int(await self._release_script(keys=keys, args=args)))

    async def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Renew the lock TTL, returning Result.SUCCESS on success.
        """
        validate_mode(mode)
        ttl_ms = ttl_milliseconds(ttl_seconds)
        keys = [
            self._lock_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
            self._fence_key(lock_name),
        ]
        args = [owner_id, ttl_ms]
        if mode == LockMode.SHARED:
            value = await self._renew_read_script(keys=keys, args=args)
        else:
            value = await self._renew_script(keys=keys, args=args)

        return Result(int(value))

    async def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if the lock is held.
        """
        if value := await self._client.get(self._lock_key(lock_name)):
            if isinstance(value, bytes):
                return value.decode("utf-8")
            else:
                return str(value)
        else:
            return None

    async def is_locked(self, lock_name: str) -> bool:
        """
        Return True if an exclusive owner or an unexpired reader exists.
        """
        return bool(
            await self._is_locked_script(
                keys=[
                    self._lock_key(lock_name),
                    self._readers_key(lock_name),
                    self._readers_total_key(lock_name),
                ]
            )
        )

    async def wait_signal(self, lock_name: str, max_wait: float) -> bool:
        """
        Block until a release signal is received or timeout occurs.
        """
        if max_wait <= 0:
            return False
        result = await self._client.blpop(self._signal_key(lock_name), timeout=max_wait)
        return result is not None


if __name__ == "__main__":
    pass
