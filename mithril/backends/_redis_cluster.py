"""
Redis Cluster leases with replication checks on the acquisition connection.

WAIT improves durability but cannot make Redis failover strongly consistent.
Fencing authorities and downstream conditional writes must preserve their state.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote

from redis import RedisCluster
from redis.exceptions import AskError, MovedError

from .._types import LockMode, Result, validate_duration
from ..exceptions import ConfigurationError
from ._redis import RedisBackend
from ._redis_common import ttl_milliseconds, validate_mode

__author__ = "Rohan B. Dalton"


class RedisClusterBackend(RedisBackend):
    """
    Cluster keys share an escaped hash tag; safe mutations pin one primary connection.
    """

    def __init__(
        self,
        client: Any,
        *,
        namespace: str = "",
        signal_ttl: float = 5.0,
        safety_level: str = "fast",
        wait_replicas: int | None = None,
        wait_timeout: float = 0.2,
    ) -> None:
        if safety_level not in {"fast", "safe", "paranoid"}:
            raise ConfigurationError("safety_level must be fast, safe, or paranoid")
        if safety_level != "fast" and (
            not isinstance(wait_replicas, int)
            or isinstance(wait_replicas, bool)
            or wait_replicas < 1
        ):
            raise ConfigurationError("safe/paranoid require a positive wait_replicas count")
        validate_duration(wait_timeout, "wait_timeout")
        super().__init__(client, namespace=namespace, signal_ttl=signal_ttl)
        self._safety_level = safety_level
        self._wait_replicas = wait_replicas
        self._wait_timeout = wait_timeout

    def _key(self, kind: str, lock_name: str) -> str:
        if not isinstance(lock_name, str) or not lock_name:
            raise ConfigurationError("lock_name must be a nonempty string")
        tag = quote(lock_name, safe="")
        if self._namespace:
            tag = f"{quote(self._namespace, safe='')}:{tag}"
        return f"{{{tag}}}:{kind}"

    @contextmanager
    def _connection(self, lock_name: str):
        client = self._client
        if isinstance(client, RedisCluster):
            node = client.get_node_from_key(self._lock_key(lock_name))
            client = client.get_redis_connection(node)
        with client.client() as connection:
            yield connection

    def _wait_for_replicas(self, connection) -> bool:
        replicas = self._wait_replicas
        if self._safety_level == "paranoid":
            replicas = max(replicas, int(connection.info("replication")["connected_slaves"]))
        acknowledged = connection.execute_command(
            "WAIT", replicas, ttl_milliseconds(self._wait_timeout)
        )
        return int(acknowledged) >= replicas

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
        acquired_at: float | None = None,
    ) -> tuple[int, int] | None:
        """
        Acquire and require replication acknowledgments before reporting success.
        """
        if self._safety_level == "fast":
            return super().acquire(lock_name, owner_id, ttl_seconds, epoch, mode, acquired_at)
        validate_mode(mode)
        ttl_ms = ttl_milliseconds(ttl_seconds)
        keys = [
            self._lock_key(lock_name),
            self._fence_key(lock_name),
            self._signal_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
        ]
        script = self._acquire_read_script if mode == LockMode.SHARED else self._acquire_script
        try:
            with self._connection(lock_name) as connection:
                result = script(
                    keys=keys,
                    args=[owner_id, ttl_ms, epoch, time.monotonic()],
                    client=connection,
                )
                if not result or int(result[0]) == -1:
                    return None
                if self._wait_for_replicas(connection):
                    return int(result[0]), int(result[1])
        except (MovedError, AskError):
            # The cluster client re-routes the owner-checked cleanup. Retry via
            # the facade rather than accepting an unverified redirected lease.
            self.release(lock_name, owner_id, mode)
            return None
        except BaseException:
            self.release(lock_name, owner_id, mode)
            raise
        self.release(lock_name, owner_id, mode)
        return None

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Renew and confirm replication on the same primary connection.
        """
        if self._safety_level == "fast":
            return super().renew(lock_name, owner_id, ttl_seconds, mode)
        validate_mode(mode)
        ttl_ms = ttl_milliseconds(ttl_seconds)
        keys = [
            self._lock_key(lock_name),
            self._meta_key(lock_name),
            self._readers_key(lock_name),
            self._readers_total_key(lock_name),
            self._fence_key(lock_name),
        ]
        script = self._renew_read_script if mode == LockMode.SHARED else self._renew_script
        try:
            with self._connection(lock_name) as connection:
                result = Result(
                    int(script(keys=keys, args=[owner_id, ttl_ms], client=connection))
                )
                if result != Result.SUCCESS:
                    return result
                if self._wait_for_replicas(connection):
                    return result
        except BaseException:
            self.release(lock_name, owner_id, mode)
            raise
        self.release(lock_name, owner_id, mode)
        return Result.NOT_FOUND


if __name__ == "__main__":
    pass
