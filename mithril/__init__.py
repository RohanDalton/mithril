"""
Public API for Mithril.
"""

from __future__ import annotations

from .__version__ import __version__
from ._fencing import FencingTokenProvider, FileFencingTokenProvider, RedisFencingTokenProvider
from ._observability import LockHooks, configure_logging, get_logger, set_hooks
from ._types import LockConfig, LockMode, LockState, LockToken, Result
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
from .lock import Lock

__author__ = "Rohan B. Dalton"

__all__ = [
    "__version__",
    "FencingTokenProvider",
    "FileFencingTokenProvider",
    "RedisFencingTokenProvider",
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
