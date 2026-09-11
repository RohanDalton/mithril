"""
User-facing distributed lock facade for Mithril.
"""

from __future__ import annotations

import atexit
import base64
import os
import threading
import time
import traceback
import weakref
from typing import Callable

from ._observability import (
    LockEvent,
    LockHooks,
    get_hooks,
    get_logger,
    log_safely,
    record_event,
)
from ._renewal import LockRenewer
from ._retry import RetryPolicy
from ._types import (
    AcquisitionResult,
    LockBackend,
    LockConfig,
    LockMode,
    LockState,
    LockToken,
    RenewalResult,
    Result,
    validate_backend,
    validate_duration,
)
from .exceptions import (
    AlreadyAcquiredError,
    ConfigurationError,
    LockExpiredError,
    LockTimeoutError,
    NotAcquiredError,
    RenewalFailedError,
)

__author__ = "Rohan B. Dalton"

_ACTIVE_LOCKS: "weakref.WeakSet[Lock]" = weakref.WeakSet()


def _release_held_locks() -> None:
    for lock in list(_ACTIVE_LOCKS):
        try:
            if lock.state in (LockState.ACQUIRED, LockState.EXPIRED):
                lock.release()
        except Exception:
            pass


atexit.register(_release_held_locks)


class Lock:
    """
    A distributed lock with fencing tokens and optional auto-renewal.
    """

    def __init__(
        self,
        name: str,
        *,
        backend: LockBackend,
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
        if strict:
            validate_backend(backend)
        self._state_lock = threading.Lock()
        self._operation_lock = threading.RLock()
        self._state = LockState.UNLOCKED
        self._owner_id: str | None = None
        self._token: LockToken | None = None
        self._renewal: LockRenewer | None = None
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

    def acquire(
        self,
        blocking: bool = True,
        timeout: float | None = None,
        mode: LockMode | None = None,
    ) -> LockToken:
        """
        Acquire once, rejecting overlapping acquisition on the same instance.
        """
        if not self._operation_lock.acquire(blocking=False):
            raise AlreadyAcquiredError("Another operation is in progress")
        try:
            return self._acquire(blocking, timeout, mode)
        except BaseException:
            if self._state == LockState.ACQUIRING:
                self._cleanup()
                self._state = LockState.UNLOCKED
            raise
        finally:
            self._operation_lock.release()

    def _acquire(
        self,
        blocking: bool = True,
        timeout: float | None = None,
        mode: LockMode | None = None,
    ) -> LockToken:
        """
        Acquire the lock and return a LockToken.
        """
        self._expire_if_needed()
        desired_mode = self._config.mode if mode is None else mode
        if not isinstance(desired_mode, LockMode):
            raise ConfigurationError("mode must be a LockMode")
        with self._state_lock:
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
        validate_duration(timeout_value, "timeout", allow_zero=True)
        if not blocking:
            timeout_value = 0

        self._owner_id = _generate_owner_id()
        lock_name = self._full_name()
        self._mode = desired_mode
        self._state = LockState.ACQUIRING

        wait_func: Callable[[float], bool] | None = None
        if hasattr(self._backend, "wait_signal"):

            def wait_func(delay: float) -> bool:
                return self._backend.wait_signal(lock_name, delay)

        retry = RetryPolicy(
            timeout=timeout_value,
            base_delay=self._config.retry_interval,
            max_delay=max(self._config.retry_interval, ttl),
            jitter=self._config.retry_jitter,
        )
        start = time.monotonic()
        for _attempt in retry.attempts(wait_func=wait_func):
            acquired_at = time.monotonic()
            result = self._backend.acquire(
                lock_name, self._owner_id, ttl, self._epoch, desired_mode
            )
            if result is not None:
                expire_at = acquired_at + ttl
                if isinstance(result, AcquisitionResult):
                    token_value, epoch = result.fencing_token, result.epoch
                    expire_at = min(expire_at, result.expire_at)
                else:
                    token_value, epoch = result
                if time.monotonic() >= expire_at:
                    self._cleanup()
                    self._state = LockState.EXPIRED
                    raise LockTimeoutError("Lease expired before acquisition completed")
                self._token = LockToken(
                    lock_name=lock_name,
                    owner_id=self._owner_id,
                    fencing_token=token_value,
                    epoch=epoch,
                    acquired_at=acquired_at,
                    expire_at=expire_at,
                )
                with self._state_lock:
                    self._state = LockState.ACQUIRED
                    self._mode = desired_mode
                    self._reentrant_count = 1
                _ACTIVE_LOCKS.add(self)
                try:
                    self._start_renewal()
                except BaseException:
                    self._cleanup()
                    self._state = LockState.EXPIRED
                    raise
                if self._config.debug:
                    self._acquire_stack = traceback.format_stack()
                elapsed = time.monotonic() - start
                log_safely(
                    self._logger,
                    "info",
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
        log_safely(
            self._logger,
            "warning",
            "lock_timeout",
            owner_id=self._owner_id,
            mode=desired_mode.value,
            ttl=ttl,
            timeout=timeout_value,
            elapsed=elapsed,
        )
        self._emit_event("timeout", elapsed=elapsed, ttl=ttl, timeout=timeout_value)
        raise LockTimeoutError(f"Timed out after {elapsed:.3f}s")

    def release(self) -> None:
        """
        Release ownership and stop renewal, including cleanup of expired leases.
        """
        with self._operation_lock:
            self._release()

    def _release(self) -> None:
        """
        Release the lock if owned by this instance.
        """
        self._expire_if_needed()
        if self._state == LockState.EXPIRED:
            self._stop_renewal()
            self._cleanup()
            _ACTIVE_LOCKS.discard(self)
            raise LockExpiredError("Lock already expired")
        with self._state_lock:
            if self._state == LockState.EXPIRED:
                raise LockExpiredError("Lock already expired")
            if self._state != LockState.ACQUIRED:
                raise NotAcquiredError("Lock not acquired")
            if self._reentrant and self._reentrant_count > 1:
                self._reentrant_count -= 1
                return
        self._stop_renewal()
        result = self._backend.release(self._full_name(), self._owner_id or "", self._mode)
        if not _release_succeeded(result):
            self._on_renewal_failed()
            log_safely(
                self._logger,
                "warning",
                "lock_release_failed",
                owner_id=self._owner_id,
                mode=self._mode.value if self._mode else None,
            )
            raise NotAcquiredError("Lock not owned by this instance")
        with self._state_lock:
            self._state = LockState.RELEASED
            self._reentrant_count = 0
        self._stop_renewal()
        _ACTIVE_LOCKS.discard(self)
        log_safely(
            self._logger,
            "info",
            "lock_released",
            owner_id=self._owner_id,
            mode=self._mode.value if self._mode else None,
        )
        self._emit_event("release", ttl=self._config.ttl)

    def renew(self, ttl: float | None = None) -> None:
        """
        Renew a live lease and refresh its conservative local deadline.
        """
        with self._operation_lock:
            self._renew(ttl)

    def _renew(self, ttl: float | None = None) -> None:
        """
        Manually renew the lock's TTL.
        """
        self._expire_if_needed()
        with self._state_lock:
            if self._state == LockState.EXPIRED:
                raise LockExpiredError("Lock already expired")
            if self._state != LockState.ACQUIRED:
                raise NotAcquiredError("Lock not acquired")

        if (ttl_value := self._config.ttl if ttl is None else ttl) is None:
            raise ConfigurationError("ttl must be > 0")
        validate_duration(ttl_value, "ttl")
        if self._renewal is not None and self._renewal._interval * 1.1 >= ttl_value:
            raise ConfigurationError(
                "ttl must exceed the auto-renewal interval including jitter"
            )

        started = time.monotonic()
        try:
            result = self._backend.renew(
                self._full_name(), self._owner_id or "", ttl_value, self._mode
            )
        except BaseException:
            self._on_renewal_failed()
            self._stop_renewal()
            raise
        expire_at = started + ttl_value
        if isinstance(result, RenewalResult):
            expire_at = min(expire_at, result.expire_at)
        if (
            not _renewal_succeeded(result)
            or time.monotonic() >= expire_at
            or self._state != LockState.ACQUIRED
        ):
            with self._state_lock:
                self._state = LockState.EXPIRED
            log_safely(
                self._logger,
                "warning",
                "lock_expired",
                owner_id=self._owner_id,
                mode=self._mode.value if self._mode else None,
            )
            self._emit_event("expired", ttl=ttl_value)
            self._stop_renewal()
            raise RenewalFailedError("Renewal failed")
        if self._token is not None:
            self._token = LockToken(
                lock_name=self._token.lock_name,
                owner_id=self._token.owner_id,
                fencing_token=self._token.fencing_token,
                epoch=self._token.epoch,
                acquired_at=self._token.acquired_at,
                expire_at=expire_at,
            )
        log_safely(
            self._logger,
            "info",
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
        return self._token if self.state == LockState.ACQUIRED else None

    @property
    def state(self) -> LockState:
        """
        Return the current lock state.
        """
        self._expire_if_needed()
        with self._state_lock:
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
        if self.state != LockState.ACQUIRED or self._token is None:
            return None
        if self._token.expire_at is None:
            return None
        return max(0.0, self._token.expire_at - time.monotonic())

    @property
    def acquire_stack(self) -> list[str] | None:
        """
        Return the acquisition call stack when debug mode is enabled.
        """
        return list(self._acquire_stack) if self._acquire_stack is not None else None

    def __enter__(self) -> LockToken:
        return self.acquire()

    def __exit__(self, *exc_info) -> None:
        if self.state in (LockState.ACQUIRED, LockState.EXPIRED):
            try:
                self.release()
            except Exception:
                if not exc_info or exc_info[0] is None:
                    raise

    def _full_name(self) -> str:
        if not self._config.namespace:
            return self._config.name
        return f"{self._config.namespace}:{self._config.name}"

    def _start_renewal(self) -> None:
        if not self._config.auto_renewal:
            return
        if (ttl := self._config.ttl) is None:
            raise ConfigurationError("auto_renewal requires ttl to be set")
        interval = self._config.renewal_interval or (ttl * 2 / 3)
        self._renewal = LockRenewer(
            weakref.ref(self),
            self._backend,
            key=self._full_name(),
            owner_id=self._owner_id or "",
            ttl=ttl,
            interval=interval,
            mode=self._mode or LockMode.EXCLUSIVE,
        )
        self._renewal.start()

    def _stop_renewal(self) -> None:
        if self._renewal is None:
            return
        self._renewal.stop()
        self._renewal = None

    def _on_renewal_failed(self) -> None:
        with self._state_lock:
            if self._state != LockState.ACQUIRED:
                return
            self._state = LockState.EXPIRED
            self._reentrant_count = 0
        log_safely(
            self._logger,
            "warning",
            "lock_expired",
            owner_id=self._owner_id,
            mode=self._mode.value if self._mode else None,
        )
        self._emit_event("expired", ttl=self._config.ttl)

    def _expire_if_needed(self) -> None:
        with self._state_lock:
            expired = (
                self._state == LockState.ACQUIRED
                and self._token is not None
                and self._token.expire_at is not None
                and time.monotonic() >= self._token.expire_at
            )
            if expired:
                self._state = LockState.EXPIRED
                self._reentrant_count = 0
        if expired:
            self._emit_event("expired", ttl=self._config.ttl)

    def _renew_automatically(self, ttl: float) -> None:
        if self._operation_lock.acquire(blocking=False):
            try:
                self.renew(ttl)
            finally:
                self._operation_lock.release()

    def _cleanup(self) -> None:
        if self._owner_id is not None:
            try:
                self._backend.release(self._full_name(), self._owner_id, self._mode)
            except Exception:
                log_safely(
                    self._logger, "exception", "lock_cleanup_failed", owner_id=self._owner_id
                )

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


def _renewal_succeeded(result: Result | RenewalResult) -> bool:
    return isinstance(result, RenewalResult) or result == Result.SUCCESS


if __name__ == "__main__":
    pass
