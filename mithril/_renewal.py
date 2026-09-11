"""
Auto-renewal engine for Mithril locks.
"""

from __future__ import annotations

import random
import threading
import weakref
from typing import Callable

import structlog

from ._observability import log_safely
from ._types import LockBackend, LockMode, Result

__author__ = "Rohan B. Dalton"

_LOGGER = structlog.get_logger(__name__)


class LockRenewer:
    """
    Background thread that renews a lock's TTL periodically.
    """

    def __init__(
        self,
        lock_ref: weakref.ref,
        backend: LockBackend,
        key: str,
        owner_id: str,
        ttl: float,
        interval: float,
        jitter_fraction: float = 0.1,
        on_renewal_failed: Callable[[], None] | None = None,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> None:
        self._lock_ref = lock_ref
        self._backend = backend
        self._key = key
        self._owner_id = owner_id
        self._ttl = ttl
        self._interval = interval
        self._jitter_fraction = jitter_fraction
        self._on_renewal_failed = on_renewal_failed
        self._mode = mode
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"Mithril-renewer-{key}",
        )

    def start(self) -> None:
        """
        Start the renewal thread.
        """
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the renewal thread and wait for shutdown.
        """
        self._stop_event.set()
        if threading.current_thread() is self._thread:
            return
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            log_safely(
                _LOGGER,
                "warning",
                "Renewal thread for %s did not stop within %ss",
                self._key,
                timeout,
            )

    def _run(self) -> None:
        while not self._stop_event.wait(timeout=self._interval_with_jitter()):
            if (lock := self._lock_ref()) is None:
                break
            try:
                if renew := getattr(lock, "_renew_automatically", None):
                    renew(self._ttl)
                    continue
                result = self._backend.renew(self._key, self._owner_id, self._ttl, self._mode)
                if not _renewal_succeeded(result):
                    self._notify_failure(lock)
                    break
            except Exception:
                log_safely(_LOGGER, "exception", "Renewal failed for %s", self._key)
                self._notify_failure(lock)
                break
            finally:
                del lock

    def _interval_with_jitter(self) -> float:
        base = self._interval
        jitter = base * self._jitter_fraction
        value = base + random.uniform(-jitter, jitter)
        return max(self._interval * 0.1, value)

    def _notify_failure(self, lock) -> None:
        if self._on_renewal_failed is not None:
            self._on_renewal_failed()
        else:
            lock._on_renewal_failed()


def _renewal_succeeded(result: Result) -> bool:
    return result == Result.SUCCESS


if __name__ == "__main__":
    pass
