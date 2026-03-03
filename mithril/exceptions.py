"""
Exception hierarchy for Mithril locks.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"


class MithrilError(Exception):
    """
    Base class for all Mithril errors.
    """


class LockAcquisitionError(MithrilError):
    """
    Base class for lock acquisition failures.
    """


class LockTimeoutError(LockAcquisitionError):
    """
    Timed out waiting to acquire a lock.
    """


class AlreadyAcquiredError(LockAcquisitionError):
    """
    This Lock instance already holds the lock.
    """


class LockReleaseError(MithrilError):
    """
    Base class for lock release failures.
    """


class NotAcquiredError(LockReleaseError):
    """
    Tried to release a lock not held by this instance.
    """


class LockExpiredError(LockReleaseError):
    """
    Lock TTL expired before release was called.
    """


class LockRenewalError(MithrilError):
    """
    Base class for lock renewal failures.
    """


class RenewalFailedError(LockRenewalError):
    """
    Renewal failed (lock expired or taken by another).
    """


class AutoRenewalStoppedError(LockRenewalError):
    """
    Background renewal thread/task died.
    """


class FencingTokenError(MithrilError):
    """
    Base class for fencing token errors.
    """


class StaleTokenError(FencingTokenError):
    """
    Fencing token is out of date.
    """


class BackendError(MithrilError):
    """
    Underlying backend communication failure.
    """


class BackendUnavailableError(BackendError):
    """
    Cannot reach backend.
    """


class BackendTimeoutError(BackendError):
    """
    Backend operation timed out.
    """


class ConfigurationError(MithrilError):
    """
    Invalid configuration.
    """


__all__ = [
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
