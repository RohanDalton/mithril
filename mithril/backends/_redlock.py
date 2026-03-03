"""
Redlock backend for multiple independent Redis instances.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Iterable

from redis.exceptions import RedisError

from ..exceptions import ConfigurationError
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


@dataclasses.dataclass(frozen=True)
class RedlockConfig:
    """
    Configuration for Redlock backend.
    """

    instances: list[Any]
    quorum: int | None = None
    retry_count: int = 3
    retry_delay: float = 0.2
    clock_drift_factor: float = 0.01
    per_instance_timeout: float = 0.05

    def __post_init__(self) -> None:
        if not self.instances:
            raise ConfigurationError("instances must not be empty")
        if self.quorum is not None and self.quorum <= 0:
            raise ConfigurationError("quorum must be > 0")
        if self.retry_count < 0:
            raise ConfigurationError("retry_count must be >= 0")
        if self.retry_delay < 0:
            raise ConfigurationError("retry_delay must be >= 0")
        if self.clock_drift_factor < 0:
            raise ConfigurationError("clock_drift_factor must be >= 0")
        if self.per_instance_timeout <= 0:
            raise ConfigurationError("per_instance_timeout must be > 0")

    def effective_quorum(self) -> int:
        if self.quorum is not None:
            return self.quorum
        return len(self.instances) // 2 + 1


class RedlockBackend:
    """
    Redlock backend using a majority quorum.
    """

    def __init__(
        self,
        config: RedlockConfig,
        *,
        namespace: str = "",
        signal_ttl: float = 5.0,
    ) -> None:
        self._config = config
        self._namespace = namespace
        self._signal_ttl = signal_ttl
        self._scripts = {
            id(client): (
                client.register_script(_ACQUIRE_SCRIPT),
                client.register_script(_RELEASE_SCRIPT),
                client.register_script(_RENEW_SCRIPT),
                client.register_script(_ACQUIRE_READ_SCRIPT),
                client.register_script(_RELEASE_READ_SCRIPT),
                client.register_script(_RENEW_READ_SCRIPT),
            )
            for client in config.instances
        }

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
        Acquire the lock using Redlock, returning (fencing_token, epoch).
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        ttl_ms = int(ttl_seconds * 1000)
        ttl_ms = max(ttl_ms, 1)
        quorum = self._config.effective_quorum()

        for _ in range(self._config.retry_count + 1):
            start = time.monotonic()
            successes: list[tuple[Any, int]] = []
            tokens: list[int] = []
            for client in self._config.instances:
                if self._try_acquire(
                    client,
                    lock_name,
                    owner_id,
                    ttl_ms,
                    epoch,
                    acquired_at,
                    mode,
                    successes,
                    tokens,
                ):
                    pass

            elapsed = time.monotonic() - start
            drift = ttl_seconds * self._config.clock_drift_factor
            validity = ttl_seconds - elapsed - drift
            if len(successes) >= quorum and validity > 0:
                token = max(tokens) if tokens else 0
                return token, epoch

            self._release_all(
                lock_name,
                owner_id,
                mode,
                clients=[client for client, _ in successes],
            )
            if self._config.retry_delay > 0:
                time.sleep(self._config.retry_delay)
        return None

    def release(
        self, lock_name: str, owner_id: str, mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        """
        Release the lock across all instances.
        """
        results = []
        for client in self._config.instances:
            try:
                result = self._release_instance(client, lock_name, owner_id, mode)
                results.append(result)
            except Exception:
                continue
        return _combine_release_results(results)

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Renew the lock across all instances.
        """
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        ttl_ms = int(ttl_seconds * 1000)
        ttl_ms = max(ttl_ms, 1)
        quorum = self._config.effective_quorum()
        successes = 0
        for client in self._config.instances:
            try:
                result = self._renew_instance(client, lock_name, owner_id, ttl_ms, mode)
                if result == 0:
                    successes += 1
            except Exception:
                continue
        return Result.SUCCESS if successes >= quorum else Result.WRONG_OWNER

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if any instance reports one.
        """
        for client in self._config.instances:
            try:
                value = client.get(self._lock_key(lock_name))
                if value is None:
                    continue
                if isinstance(value, bytes):
                    return value.decode("utf-8")
                return str(value)
            except Exception:
                continue
        return None

    def is_locked(self, lock_name: str) -> bool:
        """
        Return True if any instance reports a lock.
        """
        for client in self._config.instances:
            try:
                if client.exists(self._lock_key(lock_name)) or client.exists(
                    self._readers_total_key(lock_name)
                ):
                    return True
            except Exception:
                continue
        return False

    def _try_acquire(
        self,
        client: Any,
        lock_name: str,
        owner_id: str,
        ttl_ms: int,
        epoch: int,
        acquired_at: float | None,
        mode: LockMode,
        successes: list[tuple[Any, int]],
        tokens: list[int],
    ) -> bool:
        keys = [
            self._lock_key(lock_name),
            self._fence_key(lock_name),
            self._signal_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
        ]
        if acquired_at is None:
            acquired_at = time.monotonic()
        args = [owner_id, ttl_ms, epoch, acquired_at]
        scripts = self._scripts[id(client)]
        if mode == LockMode.SHARED:
            acquire_script = scripts[3]
        else:
            acquire_script = scripts[0]

        try:
            result = acquire_script(keys=keys, args=args)
        except RedisError:
            return False
        else:
            if not result or result[0] == -1:
                return False

        token_value = int(result[0])
        successes.append((client, token_value))
        tokens.append(token_value)
        return True

    def _release_instance(
        self,
        client: Any,
        lock_name: str,
        owner_id: str,
        mode: LockMode,
    ) -> int:
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
        _, release_script, _, _, release_read_script, _ = self._scripts[id(client)]
        if mode == LockMode.SHARED:
            return int(release_read_script(keys=keys, args=args))
        return int(release_script(keys=keys, args=args))

    def _renew_instance(
        self,
        client: Any,
        lock_name: str,
        owner_id: str,
        ttl_ms: int,
        mode: LockMode,
    ) -> int:
        keys = [
            self._lock_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
            self._fence_key(lock_name),
        ]
        args = [owner_id, ttl_ms]
        _, _, renew_script, _, _, renew_read_script = self._scripts[id(client)]
        if mode == LockMode.SHARED:
            return int(renew_read_script(keys=keys, args=args))
        return int(renew_script(keys=keys, args=args))

    def _release_all(
        self,
        lock_name: str,
        owner_id: str,
        mode: LockMode,
        clients: Iterable[Any] | None = None,
    ) -> None:
        if clients is None:
            clients = self._config.instances
        for client in clients:
            try:
                self._release_instance(client, lock_name, owner_id, mode)
            except RedisError:
                continue

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


def _combine_release_results(results: Iterable[int]) -> Result:
    results_list = list(results)
    if not results_list:
        return Result.NOT_FOUND
    if any(result == 0 for result in results_list):
        return Result.SUCCESS
    if any(result == 1 for result in results_list):
        return Result.WRONG_OWNER
    return Result.NOT_FOUND


if __name__ == "__main__":
    pass
