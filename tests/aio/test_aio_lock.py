import asyncio

import pytest

from mithril.aio.lock import AsyncLock
from mithril._types import LockMode, LockState, LockToken
from mithril.exceptions import (
    AlreadyAcquiredError,
    ConfigurationError,
    LockExpiredError,
    LockTimeoutError,
    NotAcquiredError,
    RenewalFailedError,
)

__author__ = "Rohan B. Dalton"


class AsyncFakeBackend:
    def __init__(self) -> None:
        self.owner: str | None = None
        self.token = 0
        self.epoch = 0
        self.fail_renew = False
        self.acquire_calls = 0
        self.release_calls = 0
        self.renew_calls = 0

    async def acquire(
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

    async def release(self, key: str, owner_id: str, mode=LockMode.EXCLUSIVE):
        from mithril._types import Result

        self.release_calls += 1
        if self.owner is None:
            return Result.NOT_FOUND
        if self.owner != owner_id:
            return Result.WRONG_OWNER
        self.owner = None
        return Result.SUCCESS

    async def renew(
        self, key: str, owner_id: str, ttl_seconds: float, mode=LockMode.EXCLUSIVE
    ):
        from mithril._types import Result

        self.renew_calls += 1
        if self.owner != owner_id:
            return Result.WRONG_OWNER
        if self.fail_renew:
            return Result.WRONG_OWNER
        return Result.SUCCESS


@pytest.mark.asyncio
async def test_async_lock_acquire_returns_token() -> None:
    """
    GIVEN: An async lock with a fake backend
    WHEN: acquire succeeds
    THEN: A LockToken is returned
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    token = await lock.acquire()

    assert isinstance(token, LockToken)
    assert token.fencing_token == 1
    assert lock.state == LockState.ACQUIRED


@pytest.mark.asyncio
async def test_async_lock_double_acquire_raises() -> None:
    """
    GIVEN: An async lock already acquired
    WHEN: acquire is called again
    THEN: AlreadyAcquiredError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    with pytest.raises(AlreadyAcquiredError):
        await lock.acquire()


@pytest.mark.asyncio
async def test_async_lock_release_success() -> None:
    """
    GIVEN: An acquired async lock
    WHEN: release is called
    THEN: The lock transitions to RELEASED
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    await lock.release()

    assert lock.state == LockState.RELEASED


@pytest.mark.asyncio
async def test_async_release_without_acquire_raises() -> None:
    """
    GIVEN: An async lock not acquired
    WHEN: release is called
    THEN: NotAcquiredError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    with pytest.raises(NotAcquiredError):
        await lock.release()


@pytest.mark.asyncio
async def test_async_renew_failure_marks_expired() -> None:
    """
    GIVEN: An acquired async lock and failing backend renewal
    WHEN: renew is called
    THEN: RenewalFailedError is raised and state is EXPIRED
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    backend.fail_renew = True
    with pytest.raises(RenewalFailedError):
        await lock.renew()

    assert lock.state == LockState.EXPIRED


@pytest.mark.asyncio
async def test_async_expired_release_raises() -> None:
    """
    GIVEN: An acquired async lock that has expired
    WHEN: release is called
    THEN: LockExpiredError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=0.01, timeout=0.1)
    await lock.acquire()
    await asyncio.sleep(0.02)
    with pytest.raises(LockExpiredError):
        await lock.release()


@pytest.mark.asyncio
async def test_async_timeout_non_blocking_raises() -> None:
    """
    GIVEN: An async lock held by another owner
    WHEN: acquire is called with blocking False
    THEN: LockTimeoutError is raised
    """

    backend = AsyncFakeBackend()
    lock1 = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    lock2 = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock1.acquire()
    with pytest.raises(LockTimeoutError):
        await lock2.acquire(blocking=False)


@pytest.mark.asyncio
async def test_async_lock_single_use_after_release() -> None:
    """
    GIVEN: An async lock that has been released
    WHEN: acquire is called again on the same instance
    THEN: ConfigurationError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    await lock.release()
    with pytest.raises(ConfigurationError):
        await lock.acquire()


@pytest.mark.asyncio
async def test_async_reentrant_lock_acquire_release() -> None:
    """
    GIVEN: A reentrant async lock
    WHEN: acquire is called multiple times
    THEN: Backend acquire is called once and release occurs at final release
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1, reentrant=True)
    token1 = await lock.acquire()
    token2 = await lock.acquire()

    assert token1 is token2
    assert backend.acquire_calls == 1

    await lock.release()
    assert backend.release_calls == 0
    await lock.release()
    assert backend.release_calls == 1


@pytest.mark.asyncio
async def test_async_context_manager_returns_token() -> None:
    """
    GIVEN: An async lock used as an async context manager
    WHEN: The async with block completes
    THEN: __aenter__ returns a LockToken and state is RELEASED after exit
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    async with lock as token:
        assert isinstance(token, LockToken)
        assert lock.state == LockState.ACQUIRED
    assert lock.state == LockState.RELEASED


@pytest.mark.asyncio
async def test_async_context_manager_no_release_when_not_acquired() -> None:
    """
    GIVEN: An async lock whose state is not ACQUIRED
    WHEN: __aexit__ is called
    THEN: release is not invoked on the backend
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.__aexit__(None, None, None)
    assert backend.release_calls == 0


@pytest.mark.asyncio
async def test_async_renew_success_refreshes_expire_at() -> None:
    """
    GIVEN: An acquired async lock
    WHEN: renew is called
    THEN: The token's expire_at is updated to a later time
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    old_expire = lock.token.expire_at
    await asyncio.sleep(0.05)
    await lock.renew()
    assert lock.token.expire_at > old_expire


@pytest.mark.asyncio
async def test_async_renew_without_acquire_raises() -> None:
    """
    GIVEN: An async lock that has not been acquired
    WHEN: renew is called
    THEN: NotAcquiredError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    with pytest.raises(NotAcquiredError):
        await lock.renew()


@pytest.mark.asyncio
async def test_async_renew_on_expired_lock_raises() -> None:
    """
    GIVEN: An acquired async lock that has transitioned to EXPIRED
    WHEN: renew is called
    THEN: LockExpiredError is raised
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)
    await lock.acquire()
    lock._on_renewal_failed()
    with pytest.raises(LockExpiredError):
        await lock.renew()


@pytest.mark.asyncio
async def test_async_properties_owner_id_and_time_remaining() -> None:
    """
    GIVEN: An async lock before and after acquisition
    WHEN: owner_id and time_remaining are accessed
    THEN: They return None before acquire and populated values after
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock("ring", backend=backend, ttl=1.0, timeout=0.1)

    assert lock.owner_id is None
    assert lock.time_remaining is None

    await lock.acquire()
    assert lock.owner_id is not None
    assert lock.time_remaining is not None
    assert lock.time_remaining > 0


@pytest.mark.asyncio
async def test_async_auto_renewal_invokes_backend() -> None:
    """
    GIVEN: An async lock with auto-renewal enabled
    WHEN: The lock is held for longer than the renewal interval
    THEN: The backend renew method is invoked
    """

    backend = AsyncFakeBackend()
    lock = AsyncLock(
        "ring",
        backend=backend,
        ttl=0.2,
        timeout=0.1,
        auto_renewal=True,
        renewal_interval=0.05,
    )
    await lock.acquire()
    await asyncio.sleep(0.12)
    assert backend.renew_calls > 0
    await lock.release()


if __name__ == "__main__":
    pass
