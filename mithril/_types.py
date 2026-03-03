"""
Core types and protocols for Mithril locks.
"""

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Protocol

from .exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


@dataclasses.dataclass(frozen=True)
class LockToken:
    """
    Token returned on successful lock acquisition.
    """

    lock_name: str
    owner_id: str
    fencing_token: int
    epoch: int
    acquired_at: float
    expire_at: float | None


class LockState(Enum):
    """
    State machine for a lock instance.
    """

    UNLOCKED = "unlocked"
    ACQUIRED = "acquired"
    EXPIRED = "expired"
    RELEASED = "released"


class LockMode(Enum):
    """
    Lock acquisition mode.
    """

    EXCLUSIVE = "exclusive"
    SHARED = "shared"


class Result(Enum):
    """
    Result of a lock release or renew operation.
    """

    SUCCESS = 0
    WRONG_OWNER = 1
    NOT_FOUND = 2


class LockBackend(Protocol):
    """
    Backend interface for lock operations.
    """

    def acquire(
        self,
        key: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | None: ...

    def release(
        self,
        key: str,
        owner_id: str,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result: ...

    def renew(
        self,
        key: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result: ...

    def get_owner(self, key: str) -> str | None: ...

    def is_locked(self, key: str) -> bool: ...


@dataclasses.dataclass(frozen=True)
class LockConfig:
    """
    Per-lock configuration.
    """

    name: str
    namespace: str = ""
    ttl: float | None = 30.0
    timeout: float = 10.0
    retry_interval: float = 0.1
    retry_jitter: float = 0.05
    auto_renewal: bool = False
    renewal_interval: float | None = None
    strict: bool = True
    mode: LockMode = LockMode.EXCLUSIVE
    debug: bool = False

    def __post_init__(self) -> None:
        if self.ttl is not None and self.ttl <= 0:
            raise ConfigurationError("ttl must be > 0 when provided")
        if self.timeout < 0:
            raise ConfigurationError("timeout must be >= 0")
        if self.retry_interval <= 0:
            raise ConfigurationError("retry_interval must be > 0")
        if self.auto_renewal and self.ttl is None:
            raise ConfigurationError("auto_renewal requires ttl to be set")


__all__ = [
    "LockToken",
    "LockBackend",
    "LockState",
    "LockMode",
    "LockConfig",
    "Result",
]

if __name__ == "__main__":
    pass
