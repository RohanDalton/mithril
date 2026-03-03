import time

import pytest

from mithril.lock import Lock
from mithril._types import LockMode, LockState, LockToken
from mithril._observability import LockHooks
from mithril.exceptions import (
    AlreadyAcquiredError,
    ConfigurationError,
    LockExpiredError,
    LockTimeoutError,
    NotAcquiredError,
    RenewalFailedError,
)

__author__ = "Rohan B. Dalton"


class FakeBackend:
    def __init__(self) -> None:
        self.owner: str | None = None
        self.token = 0
        self.epoch = 0
        self.fail_renew = False
        self.acquire_calls = 0
        self.release_calls = 0
        self.renew_calls = 0

    def acquire(
        self,
        key: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
        mode=LockMode.EXCLUSIVE,
    ):
        self.acquire_calls += 1
        if self.owner is not None:
            return None
        self.owner = owner_id
        self.token += 1
        self.epoch = epoch
        return self.token, self.epoch

    def release(self, key: str, owner_id: str, mode=LockMode.EXCLUSIVE):
        from mithril._types import Result

        self.release_calls += 1
        if self.owner is None:
            return Result.NOT_FOUND
        if self.owner != owner_id:
            return Result.WRONG_OWNER
        self.owner = None
        return Result.SUCCESS

    def renew(self, key: str, owner_id: str, ttl_seconds: float, mode=LockMode.EXCLUSIVE):
        from mithril._types import Result

        self.renew_calls += 1
        if self.owner != owner_id:
            return Result.WRONG_OWNER
        if self.fail_renew:
            return Result.WRONG_OWNER
        return Result.SUCCESS


def test_lock_acquire_returns_token() -> None:
    """
    GIVEN: A lock with a fake backend
    WHEN: acquire succeeds
    THEN: A LockToken is returned
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    token = lock.acquire()

    assert isinstance(token, LockToken)
    assert token.fencing_token == 1
    assert lock.state == LockState.ACQUIRED


def test_lock_double_acquire_raises() -> None:
    """
    GIVEN: A lock already acquired
    WHEN: acquire is called again
    THEN: AlreadyAcquiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    with pytest.raises(AlreadyAcquiredError):
        lock.acquire()


def test_lock_release_success() -> None:
    """
    GIVEN: An acquired lock
    WHEN: release is called
    THEN: The lock transitions to RELEASED
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    lock.release()

    assert lock.state == LockState.RELEASED


def test_release_without_acquire_raises() -> None:
    """
    GIVEN: A lock not acquired
    WHEN: release is called
    THEN: NotAcquiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    with pytest.raises(NotAcquiredError):
        lock.release()


def test_renew_failure_marks_expired() -> None:
    """
    GIVEN: An acquired lock and failing backend renewal
    WHEN: renew is called
    THEN: RenewalFailedError is raised and state is EXPIRED
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    backend.fail_renew = True
    with pytest.raises(RenewalFailedError):
        lock.renew()

    assert lock.state == LockState.EXPIRED


def test_expired_release_raises() -> None:
    """
    GIVEN: An acquired lock that has expired
    WHEN: release is called
    THEN: LockExpiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=0.01, timeout=0.1)
    lock.acquire()
    time.sleep(0.02)
    with pytest.raises(LockExpiredError):
        lock.release()


def test_timeout_non_blocking_raises() -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called with blocking False
    THEN: LockTimeoutError is raised
    """
    backend = FakeBackend()
    lock1 = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock2 = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock1.acquire()
    with pytest.raises(LockTimeoutError):
        lock2.acquire(blocking=False)


def test_lock_single_use_after_release() -> None:
    """
    GIVEN: A lock that has been released
    WHEN: acquire is called again on the same instance
    THEN: ConfigurationError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    lock.release()
    with pytest.raises(ConfigurationError):
        lock.acquire()


def test_reentrant_lock_acquire_release() -> None:
    """
    GIVEN: A reentrant lock
    WHEN: acquire is called multiple times
    THEN: Backend acquire is called once and release occurs at final release
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1, reentrant=True)
    token1 = lock.acquire()
    token2 = lock.acquire()

    assert token1 is token2
    assert backend.acquire_calls == 1

    lock.release()
    assert backend.release_calls == 0
    lock.release()
    assert backend.release_calls == 1


def test_auto_renewal_invokes_backend() -> None:
    """
    GIVEN: A lock with auto-renewal enabled
    WHEN: The lock is held for longer than the renewal interval
    THEN: The backend renew method is invoked
    """
    backend = FakeBackend()
    lock = Lock(
        "ring",
        backend=backend,
        ttl=0.2,
        timeout=0.1,
        auto_renewal=True,
        renewal_interval=0.05,
    )
    lock.acquire()
    time.sleep(0.12)
    assert backend.renew_calls > 0
    lock.release()


def test_context_manager_returns_token_and_releases() -> None:
    """
    GIVEN: A lock used as a context manager
    WHEN: The with block completes
    THEN: __enter__ returns a LockToken and state is RELEASED after exit
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    with lock as token:
        assert isinstance(token, LockToken)
        assert lock.state == LockState.ACQUIRED
    assert lock.state == LockState.RELEASED


def test_context_manager_no_release_when_not_acquired() -> None:
    """
    GIVEN: A lock whose state is not ACQUIRED
    WHEN: __exit__ is called
    THEN: release is not invoked on the backend
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.__exit__(None, None, None)
    assert backend.release_calls == 0


def test_renew_success_refreshes_expire_at() -> None:
    """
    GIVEN: An acquired lock
    WHEN: renew is called
    THEN: The token's expire_at is updated to a later time
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    old_expire = lock.token.expire_at
    time.sleep(0.05)
    lock.renew()
    assert lock.token.expire_at > old_expire


def test_renew_without_acquire_raises() -> None:
    """
    GIVEN: A lock that has not been acquired
    WHEN: renew is called
    THEN: NotAcquiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    with pytest.raises(NotAcquiredError):
        lock.renew()


def test_renew_on_expired_lock_raises() -> None:
    """
    GIVEN: An acquired lock that has transitioned to EXPIRED
    WHEN: renew is called
    THEN: LockExpiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    lock._on_renewal_failed()
    with pytest.raises(LockExpiredError):
        lock.renew()


def test_release_after_renewal_failure_raises_expired() -> None:
    """
    GIVEN: An acquired lock where _on_renewal_failed was called
    WHEN: release is called
    THEN: LockExpiredError is raised
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock.acquire()
    lock._on_renewal_failed()
    with pytest.raises(LockExpiredError):
        lock.release()


def test_properties_before_and_after_acquire() -> None:
    """
    GIVEN: A lock before and after acquisition
    WHEN: owner_id, time_remaining, and acquire_stack are accessed
    THEN: They return None before acquire and populated values after
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1)

    assert lock.owner_id is None
    assert lock.time_remaining is None
    assert lock.acquire_stack is None

    lock.acquire()
    assert lock.owner_id is not None
    assert lock.time_remaining is not None
    assert lock.time_remaining > 0


def test_debug_mode_captures_stack() -> None:
    """
    GIVEN: A lock with debug=True
    WHEN: acquire is called
    THEN: acquire_stack contains the call stack
    """
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1, debug=True)
    lock.acquire()
    stack = lock.acquire_stack
    assert stack is not None
    assert len(stack) > 0


def test_hooks_called_on_acquire_release_renew() -> None:
    """
    GIVEN: A lock with custom LockHooks
    WHEN: acquire, renew, and release are called
    THEN: The corresponding hook callbacks are invoked
    """
    events = []
    hooks = LockHooks(
        on_acquire=lambda e: events.append("acquire"),
        on_release=lambda e: events.append("release"),
        on_renewal=lambda e: events.append("renew"),
        on_timeout=lambda e: events.append("timeout"),
        on_expired=lambda e: events.append("expired"),
    )
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1, hooks=hooks)
    lock.acquire()
    assert "acquire" in events

    lock.renew()
    assert "renew" in events

    lock.release()
    assert "release" in events


def test_hooks_called_on_timeout() -> None:
    """
    GIVEN: A lock with a timeout hook
    WHEN: acquire times out
    THEN: The timeout hook is invoked
    """
    events = []
    hooks = LockHooks(on_timeout=lambda e: events.append("timeout"))
    backend = FakeBackend()
    backend.owner = "blocker"
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.01, hooks=hooks)
    with pytest.raises(LockTimeoutError):
        lock.acquire()
    assert "timeout" in events


def test_hooks_called_on_expired() -> None:
    """
    GIVEN: A lock with an expired hook
    WHEN: _on_renewal_failed is triggered
    THEN: The expired hook is invoked
    """
    events = []
    hooks = LockHooks(on_expired=lambda e: events.append("expired"))
    backend = FakeBackend()
    lock = Lock("ring", backend=backend, ttl=1.0, timeout=0.1, hooks=hooks)
    lock.acquire()
    lock._on_renewal_failed()
    assert "expired" in events


if __name__ == "__main__":
    pass
