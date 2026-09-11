"""
Async auto-renewal engine for Mithril locks.
"""

from __future__ import annotations

import asyncio
import random
import weakref
from collections.abc import Awaitable
from typing import Callable

import structlog

from .._observability import log_safely
from .._types import LockBackend, LockMode, Result

__author__ = "Rohan B. Dalton"

_LOGGER = structlog.get_logger(__name__)


class AsyncLockRenewer:
    """
    asyncio.Task that renews a lock's TTL periodically.
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
        on_renewal_failed: Callable[[], Awaitable[None]] | None = None,
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
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """
        Start the renewal task.
        """
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"Mithril-renewer-{self._key}")

    async def stop(self) -> None:
        """
        Cancel the renewal task and wait for completion.
        """
        if self._task:
            if asyncio.current_task() is self._task:
                return
            else:
                self._task.cancel()

            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        else:
            return None

    async def _run(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval_with_jitter())
                if lock := self._lock_ref():
                    try:
                        if renew := getattr(lock, "_renew_automatically", None):
                            await renew(self._ttl)
                            continue
                        result = await self._backend.renew(
                            self._key, self._owner_id, self._ttl, self._mode
                        )
                        if not self._renewal_succeeded(result):
                            await self._notify_failure(lock)
                            break
                    except Exception:
                        log_safely(
                            _LOGGER, "exception", "Async renewal failed for %s", self._key
                        )
                        await self._notify_failure(lock)
                        break
                    finally:
                        del lock
                else:
                    break
        except asyncio.CancelledError:
            pass

    def _interval_with_jitter(self) -> float:
        base = self._interval
        jitter = base * self._jitter_fraction
        value = base + random.uniform(-jitter, jitter)
        return max(self._interval * 0.1, value)

    async def _notify_failure(self, lock) -> None:
        if self._on_renewal_failed is not None:
            await self._on_renewal_failed()
        else:
            # noinspection PyProtectedMember
            result = lock._on_renewal_failed()
            if asyncio.iscoroutine(result):
                await result

    @staticmethod
    def _renewal_succeeded(result: Result) -> bool:
        return result == Result.SUCCESS


if __name__ == "__main__":
    pass
