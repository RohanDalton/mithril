"""
Public API for Mithril.
"""

from __future__ import annotations

from ._types import LockConfig, LockMode, LockState, LockToken, Result
from ._observability import LockHooks, configure_logging, get_logger, set_hooks
from .lock import Lock
from .exceptions import (
    AlreadyAcquiredError,
    AutoRenewalStoppedError,
    BackendError,
    BackendTimeoutError,
    BackendUnavailableError,
    ConfigurationError,
    FencingTokenError,
    LockAcquisitionError,
    LockExpiredError,
    LockReleaseError,
    LockRenewalError,
    LockTimeoutError,
    MithrilError,
    NotAcquiredError,
    RenewalFailedError,
    StaleTokenError,
)

__author__ = "Rohan B. Dalton"

__all__ = [
    "LockConfig",
    "LockMode",
    "LockState",
    "LockToken",
    "Result",
    "Lock",
    "LockHooks",
    "configure_logging",
    "get_logger",
    "set_hooks",
    "MithrilError",
    "LockAcquisitionError",
    "LockTimeoutError",
    "AlreadyAcquiredError",
    "LockReleaseError",
    "NotAcquiredError",
    "LockExpiredError",
    "LockRenewalError",
    "RenewalFailedError",
    "AutoRenewalStoppedError",
    "FencingTokenError",
    "StaleTokenError",
    "BackendError",
    "BackendUnavailableError",
    "BackendTimeoutError",
    "ConfigurationError",
]

if __name__ == "__main__":
    pass
