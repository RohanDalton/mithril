"""
Async distributed lock facade for Mithril.
"""

from __future__ import annotations

import atexit
import asyncio
import base64
import os
import time
import traceback
import weakref
from typing import Any, Callable

from .._types import LockConfig, LockMode, LockState, LockToken, Result
from .._observability import LockEvent, LockHooks, get_hooks, get_logger, record_event
from ..exceptions import (
    AlreadyAcquiredError,
    ConfigurationError,
    LockExpiredError,
    LockTimeoutError,
    NotAcquiredError,
    RenewalFailedError,
)
from ._renewal import AsyncLockRenewer
from ._retry import AsyncRetryPolicy

__author__ = "Rohan B. Dalton"

_ACTIVE_LOCKS: "weakref.WeakSet[AsyncLock]" = weakref.WeakSet()


async def _release_held_locks() -> None:
    for lock in list(_ACTIVE_LOCKS):
        try:
            if lock.state == LockState.ACQUIRED:
                await lock.release()
        except Exception:
            pass


def _atexit_cleanup() -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            asyncio.run(_release_held_locks())
        except Exception:
            pass
    else:
        try:
            loop.create_task(_release_held_locks())
        except Exception:
            pass


atexit.register(_atexit_cleanup)


class AsyncLock:
    """
    Async distributed lock with fencing tokens.
    """

    def __init__(
        self,
        name: str,
        *,
        backend: Any,
        namespace: str = "",
        ttl: float = 30.0,
        timeout: float = 10.0,
        retry_interval: float = 0.1,
        retry_jitter: float = 0.05,
        auto_renewal: bool = False,
        renewal_interval: float | None = None,
        strict: bool = True,
        mode: LockMode = LockMode.EXCLUSIVE,
        reentrant: bool = False,
        debug: bool = False,
        hooks: LockHooks | None = None,
    ) -> None:
        self._config = LockConfig(
            name=name,
            namespace=namespace,
            ttl=ttl,
            timeout=timeout,
            retry_interval=retry_interval,
            retry_jitter=retry_jitter,
            auto_renewal=auto_renewal,
            renewal_interval=renewal_interval,
            strict=strict,
            mode=mode,
            debug=debug,
        )
        self._backend = backend
        self._state = LockState.UNLOCKED
        self._owner_id: str | None = None
        self._token: LockToken | None = None
        self._renewal: AsyncLockRenewer | None = None
        self._epoch = 0
        self._mode: LockMode | None = None
        self._reentrant = reentrant
        self._reentrant_count = 0
        self._hooks = hooks
        self._logger = get_logger().bind(
            lock_name=self._full_name(),
            namespace=self._config.namespace,
            backend=type(self._backend).__name__,
        )
        self._acquire_stack: list[str] | None = None

    async def acquire(
        self,
        blocking: bool = True,
        timeout: float | None = None,
        mode: LockMode | None = None,
    ) -> LockToken:
        """
        Acquire the lock and return a LockToken.
        """
        desired_mode = mode or self._config.mode
        if self._state == LockState.ACQUIRED:
            if self._reentrant and self._mode == desired_mode:
                self._reentrant_count += 1
                return self._token  # type: ignore[return-value]
            raise AlreadyAcquiredError("Lock already acquired")
        if self._state in (LockState.RELEASED, LockState.EXPIRED):
            raise ConfigurationError("Lock instances are single-use")

        if (ttl := self._config.ttl) is None:
            raise ConfigurationError("ttl must be set for acquisition")

        timeout_value = self._config.timeout if timeout is None else timeout
        if not blocking:
            timeout_value = 0

        self._owner_id = _generate_owner_id()
        lock_name = self._full_name()
        self._epoch += 1

        wait_func: Callable[[float], Any] | None = None
        if hasattr(self._backend, "wait_signal"):

            async def wait_func(delay: float) -> bool:
                return await self._backend.wait_signal(lock_name, delay)

        retry = AsyncRetryPolicy(
            timeout=timeout_value,
            base_delay=self._config.retry_interval,
            max_delay=max(self._config.retry_interval, ttl),
            jitter=self._config.retry_jitter,
        )
        start = time.monotonic()
        async for _attempt in retry.attempts(wait_func=wait_func):
            result = await self._backend.acquire(
                lock_name, self._owner_id, ttl, self._epoch, desired_mode
            )
            if result is not None:
                token_value, epoch = result
                acquired_at = time.monotonic()
                expire_at = acquired_at + ttl
                self._token = LockToken(
                    lock_name=lock_name,
                    owner_id=self._owner_id,
                    fencing_token=token_value,
                    epoch=epoch,
                    acquired_at=acquired_at,
                    expire_at=expire_at,
                )
                self._state = LockState.ACQUIRED
                self._mode = desired_mode
                self._reentrant_count = 1
                _ACTIVE_LOCKS.add(self)
                await self._start_renewal()
                if self._config.debug:
                    self._acquire_stack = traceback.format_stack()
                elapsed = time.monotonic() - start
                self._logger.info(
                    "lock_acquired",
                    owner_id=self._owner_id,
                    fencing_token=token_value,
                    epoch=epoch,
                    mode=desired_mode.value,
                    ttl=ttl,
                    elapsed=elapsed,
                )
                self._emit_event("acquire", elapsed=elapsed, ttl=ttl)
                return self._token

        elapsed = time.monotonic() - start
        self._logger.warning(
            "lock_timeout",
            owner_id=self._owner_id,
            mode=desired_mode.value,
            ttl=ttl,
            timeout=timeout_value,
            elapsed=elapsed,
        )
        self._emit_event("timeout", elapsed=elapsed, ttl=ttl, timeout=timeout_value)
        raise LockTimeoutError(f"Timed out after {elapsed:.3f}s")

    async def release(self) -> None:
        """
        Release the lock if owned by this instance.
        """
        if self._state == LockState.EXPIRED:
            raise LockExpiredError("Lock already expired")
        if self._state != LockState.ACQUIRED:
            raise NotAcquiredError("Lock not acquired")
        if self._reentrant and self._reentrant_count > 1:
            self._reentrant_count -= 1
            return
        if self._token and self._token.expire_at is not None:
            if time.monotonic() > self._token.expire_at:
                self._state = LockState.EXPIRED
                await self._stop_renewal()
                self._logger.warning(
                    "lock_expired",
                    owner_id=self._owner_id,
                    mode=self._mode.value if self._mode else None,
                )
                self._emit_event("expired", ttl=self._config.ttl)
                raise LockExpiredError("Lock expired before release")

        result = await self._backend.release(
            self._full_name(), self._owner_id or "", self._mode
        )
        if not _release_succeeded(result):
            self._logger.warning(
                "lock_release_failed",
                owner_id=self._owner_id,
                mode=self._mode.value if self._mode else None,
            )
            raise NotAcquiredError("Lock not owned by this instance")
        self._state = LockState.RELEASED
        await self._stop_renewal()
        _ACTIVE_LOCKS.discard(self)
        self._reentrant_count = 0
        self._logger.info(
            "lock_released",
            owner_id=self._owner_id,
            mode=self._mode.value if self._mode else None,
        )
        self._emit_event("release", ttl=self._config.ttl)

    async def renew(self, ttl: float | None = None) -> None:
        """
        Manually renew the lock's TTL.
        """
        if self._state == LockState.EXPIRED:
            raise LockExpiredError("Lock already expired")
        if self._state != LockState.ACQUIRED:
            raise NotAcquiredError("Lock not acquired")

        if (ttl_value := self._config.ttl if ttl is None else ttl) is None or ttl_value <= 0:
            raise ConfigurationError("ttl must be > 0")

        result = await self._backend.renew(
            self._full_name(), self._owner_id or "", ttl_value, self._mode
        )
        if not _renewal_succeeded(result):
            self._state = LockState.EXPIRED
            self._logger.warning(
                "lock_expired",
                owner_id=self._owner_id,
                mode=self._mode.value if self._mode else None,
            )
            self._emit_event("expired", ttl=ttl_value)
            raise RenewalFailedError("Renewal failed")

        now = time.monotonic()
        expire_at = now + ttl_value
        if self._token is not None:
            self._token = LockToken(
                lock_name=self._token.lock_name,
                owner_id=self._token.owner_id,
                fencing_token=self._token.fencing_token,
                epoch=self._token.epoch,
                acquired_at=self._token.acquired_at,
                expire_at=expire_at,
            )
        self._logger.info(
            "lock_renewed",
            owner_id=self._owner_id,
            mode=self._mode.value if self._mode else None,
            ttl=ttl_value,
        )
        self._emit_event("renew", ttl=ttl_value)

    @property
    def token(self) -> LockToken | None:
        """
        Return the current lock token if held.
        """
        return self._token

    @property
    def state(self) -> LockState:
        """
        Return the current lock state.
        """
        return self._state

    @property
    def owner_id(self) -> str | None:
        """
        Return the owner id for this lock instance.
        """
        return self._owner_id

    @property
    def time_remaining(self) -> float | None:
        """
        Return time remaining before expiry, if acquired and expiring.
        """
        if self._state != LockState.ACQUIRED or (token := self._token) is None:
            return None
        if token.expire_at is None:
            return None
        return max(0.0, token.expire_at - time.monotonic())

    @property
    def acquire_stack(self) -> list[str] | None:
        """
        Return the acquisition call stack when debug mode is enabled.
        """
        return list(self._acquire_stack) if self._acquire_stack is not None else None

    async def __aenter__(self) -> LockToken:
        return await self.acquire()

    async def __aexit__(self, *exc_info) -> None:
        if self._state == LockState.ACQUIRED:
            await self.release()

    def _full_name(self) -> str:
        if not self._config.namespace:
            return self._config.name
        return f"{self._config.namespace}:{self._config.name}"

    async def _start_renewal(self) -> None:
        if not self._config.auto_renewal:
            return
        if (ttl := self._config.ttl) is None:
            raise ConfigurationError("auto_renewal requires ttl to be set")
        interval = self._config.renewal_interval or (ttl * 2 / 3)
        self._renewal = AsyncLockRenewer(
            weakref.ref(self),
            self._backend,
            key=self._full_name(),
            owner_id=self._owner_id or "",
            ttl=ttl,
            interval=interval,
            mode=self._mode or LockMode.EXCLUSIVE,
        )
        self._renewal.start()

    async def _stop_renewal(self) -> None:
        if self._renewal is None:
            return
        await self._renewal.stop()
        self._renewal = None

    def _on_renewal_failed(self) -> None:
        self._state = LockState.EXPIRED
        self._reentrant_count = 0
        self._logger.warning(
            "lock_expired",
            owner_id=self._owner_id,
            mode=self._mode.value if self._mode else None,
        )
        self._emit_event("expired", ttl=self._config.ttl)

    def _emit_event(
        self,
        event: str,
        *,
        elapsed: float | None = None,
        ttl: float | None = None,
        timeout: float | None = None,
    ) -> None:
        token = self._token
        record_event(
            LockEvent(
                event=event,
                lock_name=self._full_name(),
                owner_id=self._owner_id,
                mode=self._mode,
                fencing_token=token.fencing_token if token else None,
                epoch=token.epoch if token else None,
                ttl=ttl,
                timeout=timeout,
                elapsed=elapsed,
                state=self._state,
                backend=type(self._backend).__name__,
                namespace=self._config.namespace,
            ),
            hooks=self._hooks or get_hooks(),
        )


def _generate_owner_id() -> str:
    raw = os.urandom(18)
    return base64.b64encode(raw).decode("ascii")


def _release_succeeded(result: Result) -> bool:
    return result == Result.SUCCESS


def _renewal_succeeded(result: Result) -> bool:
    return result == Result.SUCCESS


if __name__ == "__main__":
    pass
