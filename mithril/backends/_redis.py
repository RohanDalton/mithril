"""
Redis single-instance backend for Mithril.
"""

from __future__ import annotations

import time
from typing import Any

from .._types import LockMode, Result

__author__ = "Rohan B. Dalton"

_ACQUIRE_SCRIPT = """
local acquired = false
if not redis.call('GET', KEYS[6]) then
    acquired = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
end
if not acquired then
    return {-1, -1}
end
local token = redis.call('INCR', KEYS[2])
local fence_ttl = math.max(tonumber(ARGV[2]) * 5, 86400000)
if redis.call('PTTL', KEYS[2]) < fence_ttl then
    redis.call('PEXPIRE', KEYS[2], fence_ttl)
end
redis.call('HSET', KEYS[4], 'owner', ARGV[1], 'epoch', ARGV[3],
           'token', token, 'acquired_at', ARGV[4])
redis.call('PEXPIRE', KEYS[4], ARGV[2])
return {token, tonumber(ARGV[3])}
"""

_RELEASE_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current == false then
    return 2
end
if current ~= ARGV[1] then
    return 1
end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[3])
redis.call('DEL', KEYS[2])
redis.call('LPUSH', KEYS[2], 1)
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 0
"""

_RENEW_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then
    return 1
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
    return 2
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[2])
local fence_ttl = math.max(tonumber(ARGV[2]) * 5, 86400000)
if redis.call('PTTL', KEYS[5]) < fence_ttl then
    redis.call('PEXPIRE', KEYS[5], fence_ttl)
end
return 0
"""

_ACQUIRE_READ_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return {-1, -1}
end
local token = redis.call('INCR', KEYS[2])
redis.call('HINCRBY', KEYS[5], ARGV[1], 1)
redis.call('INCR', KEYS[6])
local ttl_ms = tonumber(ARGV[2])
local fence_ttl = math.max(ttl_ms * 5, 86400000)
if redis.call('PTTL', KEYS[2]) < fence_ttl then
    redis.call('PEXPIRE', KEYS[2], fence_ttl)
end
if redis.call('PTTL', KEYS[5]) < ttl_ms then
    redis.call('PEXPIRE', KEYS[5], ttl_ms)
end
if redis.call('PTTL', KEYS[6]) < ttl_ms then
    redis.call('PEXPIRE', KEYS[6], ttl_ms)
end
return {token, tonumber(ARGV[3])}
"""

_RELEASE_READ_SCRIPT = """
local count = redis.call('HGET', KEYS[4], ARGV[1])
if not count then
    return 1
end
count = tonumber(count)
if count <= 1 then
    redis.call('HDEL', KEYS[4], ARGV[1])
else
    redis.call('HINCRBY', KEYS[4], ARGV[1], -1)
end
local total = redis.call('GET', KEYS[5])
if not total then
    return 2
end
total = tonumber(total) - 1
if total <= 0 then
    redis.call('DEL', KEYS[4])
    redis.call('DEL', KEYS[5])
    redis.call('DEL', KEYS[2])
    redis.call('LPUSH', KEYS[2], 1)
    redis.call('PEXPIRE', KEYS[2], ARGV[2])
else
    redis.call('SET', KEYS[5], total)
end
return 0
"""

_RENEW_READ_SCRIPT = """
local count = redis.call('HGET', KEYS[3], ARGV[1])
if not count then
    return 1
end
local ttl_ms = tonumber(ARGV[2])
local fence_ttl = math.max(ttl_ms * 5, 86400000)
if redis.call('PTTL', KEYS[3]) < ttl_ms then
    redis.call('PEXPIRE', KEYS[3], ttl_ms)
end
if redis.call('PTTL', KEYS[4]) < ttl_ms then
    redis.call('PEXPIRE', KEYS[4], ttl_ms)
end
if redis.call('PTTL', KEYS[5]) < fence_ttl then
    redis.call('PEXPIRE', KEYS[5], fence_ttl)
end
return 0
"""


class RedisBackend:
    """
    Redis backend that enforces atomic lock mutations.
    """

    def __init__(
        self,
        client: Any,
        *,
        namespace: str = "",
        signal_ttl: float = 5.0,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._signal_ttl = signal_ttl
        self._acquire_script = client.register_script(_ACQUIRE_SCRIPT)
        self._release_script = client.register_script(_RELEASE_SCRIPT)
        self._renew_script = client.register_script(_RENEW_SCRIPT)
        self._acquire_read_script = client.register_script(_ACQUIRE_READ_SCRIPT)
        self._release_read_script = client.register_script(_RELEASE_READ_SCRIPT)
        self._renew_read_script = client.register_script(_RENEW_READ_SCRIPT)

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
        mode: LockMode = LockMode.EXCLUSIVE,
        acquired_at: float | None = None,
    ) -> tuple[int, int] | None:
        """
        Acquire the lock and return (fencing_token, epoch) on success.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        ttl_ms = int(ttl_seconds * 1000)
        ttl_ms = max(ttl_ms, 1)
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
            result = self._acquire_read_script(keys=keys, args=args)
        else:
            result = self._acquire_script(keys=keys, args=args)
        if not result or result[0] == -1:
            return None
        return int(result[0]), int(result[1])

    def release(
        self, lock_name: str, owner_id: str, mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        """
        Release the lock, returning Result.SUCCESS on success.
        """
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
            return Result(int(self._release_read_script(keys=keys, args=args)))
        return Result(int(self._release_script(keys=keys, args=args)))

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Renew the lock TTL, returning Result.SUCCESS on success.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        ttl_ms = int(ttl_seconds * 1000)
        ttl_ms = max(ttl_ms, 1)
        keys = [
            self._lock_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
            self._fence_key(lock_name),
        ]
        args = [owner_id, ttl_ms]
        if mode == LockMode.SHARED:
            return Result(int(self._renew_read_script(keys=keys, args=args)))
        return Result(int(self._renew_script(keys=keys, args=args)))

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if the lock is held.
        """
        if value := self._client.get(self._lock_key(lock_name)):
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return str(value)
        else:
            return None

    def is_locked(self, lock_name: str) -> bool:
        """
        Return True if the lock key exists.
        """
        return bool(
            self._client.exists(self._lock_key(lock_name))
            or self._client.exists(self._readers_total_key(lock_name))
        )

    def wait_signal(self, lock_name: str, max_wait: float) -> bool:
        """
        Block until a release signal is received or timeout occurs.
        """
        result = self._client.blpop(self._signal_key(lock_name), timeout=max_wait)
        return result is not None

    def _lock_key(self, lock_name: str) -> str:
        return self._namespaced(f"lock:{lock_name}")

    def _fence_key(self, lock_name: str) -> str:
        return self._namespaced(f"fence:{lock_name}")

    def _signal_key(self, lock_name: str) -> str:
        return self._namespaced(f"signal:{lock_name}")

    def _meta_key(self, lock_name: str) -> str:
        return self._namespaced(f"lock:{lock_name}:meta")

    def _readers_key(self, lock_name: str) -> str:
        return self._namespaced(f"lock:{lock_name}:readers")

    def _readers_total_key(self, lock_name: str) -> str:
        return self._namespaced(f"lock:{lock_name}:readers:total")

    def _namespaced(self, key: str) -> str:
        if not self._namespace:
            return key
        return f"{self._namespace}:{key}"


if __name__ == "__main__":
    pass
