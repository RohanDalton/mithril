"""
Redlock quorum leases with an explicitly supplied, persistent fencing authority.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Sequence

from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

from .._fencing import FencingTokenProvider
from .._types import AcquisitionResult, LockMode, RenewalResult, Result, validate_duration
from ..exceptions import BackendUnavailableError, ConfigurationError, FencingTokenError
from ._redis import RedisBackend
from ._redis_common import RedisKeys, validate_mode

__author__ = "Rohan B. Dalton"


@dataclasses.dataclass(frozen=True)
class RedlockConfig:
    """
    Quorum configuration. Fencing requires an independent persistent authority.

    Never derive fencing tokens from the maximum of independent node counters.
    InMemoryFencingTokenProvider is suitable only for isolated tests.
    """

    instances: Sequence[Any]
    quorum: int | None = None
    retry_count: int = 3
    retry_delay: float = 0.2
    clock_drift_factor: float = 0.01
    per_instance_timeout: float = 0.05
    fencing_provider: FencingTokenProvider | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "instances", tuple(self.instances))
        if not self.instances or len({id(client) for client in self.instances}) != len(
            self.instances
        ):
            raise ConfigurationError("instances must contain distinct Redis clients")
        if (
            not isinstance(self.retry_count, int)
            or isinstance(self.retry_count, bool)
            or self.retry_count < 0
        ):
            raise ConfigurationError("retry_count must be a nonnegative integer")
        validate_duration(self.retry_delay, "retry_delay", allow_zero=True)
        validate_duration(self.clock_drift_factor, "clock_drift_factor", allow_zero=True)
        validate_duration(self.per_instance_timeout, "per_instance_timeout")
        if self.clock_drift_factor >= 1:
            raise ConfigurationError("clock_drift_factor must be < 1")
        quorum = self.effective_quorum()
        if (
            not isinstance(quorum, int)
            or isinstance(quorum, bool)
            or not len(self.instances) // 2 < quorum <= len(self.instances)
        ):
            raise ConfigurationError("quorum must be a strict majority of instances")
        if self.fencing_provider is None or not callable(
            getattr(self.fencing_provider, "next_token", None)
        ):
            raise ConfigurationError("Redlock requires a persistent fencing_provider")

    def effective_quorum(self) -> int:
        return self.quorum if self.quorum is not None else len(self.instances) // 2 + 1


class RedlockBackend(RedisKeys):
    """
    Quorum leases with bounded Redis I/O and externally ordered fencing tokens.
    """

    def __init__(
        self, config: RedlockConfig, *, namespace: str = "", signal_ttl: float = 5.0
    ) -> None:
        self._config = config
        self._namespace = namespace
        self._owned_clients = []
        self._backends = []
        for client in config.instances:
            if isinstance(client, Redis):
                params = dict(client.connection_pool.connection_kwargs)
                for name in ("socket_timeout", "socket_connect_timeout"):
                    params[name] = min(
                        params.get(name) or config.per_instance_timeout,
                        config.per_instance_timeout,
                    )
                params["retry"] = Retry(NoBackoff(), 0)
                pool = ConnectionPool(
                    connection_class=client.connection_pool.connection_class, **params
                )
                client = Redis(connection_pool=pool)
                self._owned_clients.append(client)
            self._backends.append(
                RedisBackend(client, namespace=namespace, signal_ttl=signal_ttl)
            )

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
        acquired_at: float | None = None,
    ) -> AcquisitionResult | None:
        """
        Acquire a majority and return its conservative validity deadline.
        """
        validate_duration(ttl_seconds, "ttl_seconds")
        validate_mode(mode)
        for attempt in range(self._config.retry_count + 1):
            started = time.monotonic()
            successes = 0
            try:
                for backend in self._backends:
                    try:
                        if backend.acquire(lock_name, owner_id, ttl_seconds, epoch, mode):
                            successes += 1
                    except RedisError:
                        continue
                deadline = (
                    started + ttl_seconds * (1 - self._config.clock_drift_factor) - 0.002
                )
                if (
                    successes >= self._config.effective_quorum()
                    and time.monotonic() < deadline
                ):
                    token = self._config.fencing_provider.next_token(
                        self._fence_key(lock_name)
                    )
                    if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
                        raise FencingTokenError("Fencing authority returned an invalid token")
                    if time.monotonic() < deadline:
                        return AcquisitionResult(token, token, deadline)
            except BaseException:
                self._release_all(lock_name, owner_id, mode)
                raise
            self._release_all(lock_name, owner_id, mode)
            if attempt < self._config.retry_count:
                time.sleep(self._config.retry_delay)
        return None

    def _release_all(self, lock_name: str, owner_id: str, mode: LockMode) -> list[Result]:
        results = []
        for backend in self._backends:
            try:
                results.append(backend.release(lock_name, owner_id, mode))
            except RedisError:
                continue
        return results

    def release(
        self, lock_name: str, owner_id: str, mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        """
        Attempt every node, including nodes whose acquisition reply was lost.
        """
        validate_mode(mode)
        results = self._release_all(lock_name, owner_id, mode)
        cleared = sum(result in (Result.SUCCESS, Result.NOT_FOUND) for result in results)
        if (
            Result.SUCCESS in results
            and cleared > len(self._backends) - self._config.effective_quorum()
        ):
            return Result.SUCCESS
        if Result.WRONG_OWNER in results:
            return Result.WRONG_OWNER
        if cleared == len(self._backends):
            return Result.NOT_FOUND
        raise BackendUnavailableError("Could not confirm quorum release")

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> RenewalResult | Result:
        """
        Renew a majority within the new TTL, accounting for elapsed time and drift.
        """
        validate_duration(ttl_seconds, "ttl_seconds")
        validate_mode(mode)
        started = time.monotonic()
        successes = 0
        for backend in self._backends:
            try:
                if backend.renew(lock_name, owner_id, ttl_seconds, mode) == Result.SUCCESS:
                    successes += 1
            except RedisError:
                continue
        deadline = started + ttl_seconds * (1 - self._config.clock_drift_factor) - 0.002
        if successes >= self._config.effective_quorum() and time.monotonic() < deadline:
            return RenewalResult(deadline)
        self._release_all(lock_name, owner_id, mode)
        return Result.WRONG_OWNER

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return an exclusive owner only when a majority reports that same owner.
        """
        owners: dict[str, int] = {}
        for backend in self._backends:
            try:
                if owner := backend.get_owner(lock_name):
                    owners[owner] = owners.get(owner, 0) + 1
                    if owners[owner] >= self._config.effective_quorum():
                        return owner
            except RedisError:
                continue
        return None

    def is_locked(self, lock_name: str) -> bool:
        """
        Conservatively report whether any reachable node holds the lease.
        """
        reachable = False
        for backend in self._backends:
            try:
                if backend.is_locked(lock_name):
                    return True
                reachable = True
            except RedisError:
                continue
        if not reachable:
            raise BackendUnavailableError("No Redlock node is reachable")
        return False

    def close(self) -> None:
        """
        Close only connection pools created by this backend.
        """
        for client in self._owned_clients:
            client.close()
            client.connection_pool.disconnect()


if __name__ == "__main__":
    pass
