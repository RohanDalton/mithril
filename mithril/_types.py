"""
Core types and protocols for Mithril locks.
"""

from __future__ import annotations

import dataclasses
import inspect
import math
from enum import Enum
from typing import Any, Protocol

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
    ACQUIRING = "acquiring"
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


@dataclasses.dataclass(frozen=True)
class AcquisitionResult:
    """
    Backend acquisition with a conservative monotonic validity deadline.
    """

    fencing_token: int
    epoch: int
    expire_at: float


@dataclasses.dataclass(frozen=True)
class RenewalResult:
    """
    Successful renewal with a backend-specific monotonic validity deadline.
    """

    expire_at: float


class LockBackend(Protocol):
    """
    Backend interface for lock operations.
    """

    def acquire(
        self,
        key: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | AcquisitionResult | None: ...

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
    ) -> Result | RenewalResult: ...

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
        if not isinstance(self.name, str) or not self.name:
            raise ConfigurationError("name must be a nonempty string")
        if not isinstance(self.namespace, str):
            raise ConfigurationError("namespace must be a string")
        if self.ttl is not None:
            validate_duration(self.ttl, "ttl")
        validate_duration(self.timeout, "timeout", allow_zero=True)
        validate_duration(self.retry_interval, "retry_interval")
        validate_duration(self.retry_jitter, "retry_jitter", allow_zero=True)
        if not isinstance(self.mode, LockMode):
            raise ConfigurationError("mode must be a LockMode")
        for name in ("auto_renewal", "strict", "debug"):
            if not isinstance(getattr(self, name), bool):
                raise ConfigurationError(f"{name} must be a bool")
        if self.auto_renewal and self.ttl is None:
            raise ConfigurationError("auto_renewal requires ttl to be set")
        if self.renewal_interval is not None:
            validate_duration(self.renewal_interval, "renewal_interval")
            if self.ttl is None or self.renewal_interval * 1.1 >= self.ttl:
                raise ConfigurationError("renewal_interval including jitter must be < ttl")


def validate_duration(value: float, name: str, *, allow_zero: bool = False) -> None:
    """
    Reject invalid, nonfinite, or negative durations before backend mutation.
    """
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        or (value == 0 and not allow_zero)
    ):
        bound = ">= 0" if allow_zero else "> 0"
        raise ConfigurationError(f"{name} must be finite and {bound}")


def validate_backend(backend: Any, *, asynchronous: bool = False) -> None:
    """
    Validate the mutation interface without requiring a particular backend class.
    """
    for name in ("acquire", "release", "renew"):
        if not callable(method := getattr(backend, name, None)):
            raise ConfigurationError(f"backend must implement {name}()")
        if inspect.iscoroutinefunction(method) != asynchronous:
            kind = "asynchronous" if asynchronous else "synchronous"
            raise ConfigurationError(f"backend.{name} must be {kind}")


__all__ = [
    "LockToken",
    "LockBackend",
    "LockState",
    "LockMode",
    "LockConfig",
    "Result",
    "AcquisitionResult",
    "RenewalResult",
]

if __name__ == "__main__":
    pass
