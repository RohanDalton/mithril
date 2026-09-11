import asyncio

import pytest

from mithril import LockState
from mithril.aio import AsyncLock
from mithril.exceptions import AlreadyAcquiredError, ConfigurationError, LockExpiredError
from tests.aio.test_aio_lock import AsyncFakeBackend
from tests.test_lock import FakeBackend


async def test_nonblocking_async_acquisition():
    """
    GIVEN: An uncontended async lock
    WHEN: Acquisition is nonblocking
    THEN: The backend is called once and returns ownership
    """
    backend = AsyncFakeBackend()
    lock = AsyncLock("free", backend=backend)
    await lock.acquire(blocking=False)
    assert backend.acquire_calls == 1
    await lock.release()
    assert lock.token is None


async def test_async_renewal_refreshes_local_expiry():
    """
    GIVEN: An async lock with automatic renewal
    WHEN: It is held beyond its original TTL
    THEN: The deadline is refreshed and context cleanup succeeds
    """
    backend = AsyncFakeBackend()
    lock = AsyncLock(
        "renew", backend=backend, ttl=0.3, auto_renewal=True, renewal_interval=0.05
    )
    async with lock as token:
        await asyncio.sleep(0.65)
        assert lock.token.expire_at > token.expire_at
    assert backend.owner is None
    assert lock._renewal is None


async def test_concurrent_async_acquisition_preserves_identity(monkeypatch):
    """
    GIVEN: An async acquisition waiting for a response
    WHEN: Another task acquires the same facade
    THEN: It is rejected without overwriting the original owner's identity
    """
    backend = AsyncFakeBackend()
    entered, respond = asyncio.Event(), asyncio.Event()
    acquire = backend.acquire

    async def delayed(*args):
        result = await acquire(*args)
        entered.set()
        await respond.wait()
        return result

    monkeypatch.setattr(backend, "acquire", delayed)
    lock = AsyncLock("race", backend=backend)
    task = asyncio.create_task(lock.acquire())
    await entered.wait()
    try:
        with pytest.raises(AlreadyAcquiredError):
            await lock.acquire()
    finally:
        respond.set()
    assert (await task).owner_id == backend.owner
    await lock.release()


async def test_cancelled_acquisition_cleans_up(monkeypatch):
    """
    GIVEN: A backend committed acquisition before its response arrived
    WHEN: The acquiring task is cancelled
    THEN: The owner-checked cleanup removes the otherwise orphaned lease
    """
    backend = AsyncFakeBackend()
    entered = asyncio.Event()
    acquire = backend.acquire

    async def delayed(*args):
        await acquire(*args)
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(backend, "acquire", delayed)
    lock = AsyncLock("cancel", backend=backend)
    task = asyncio.create_task(lock.acquire())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.owner is None
    assert lock.state == LockState.UNLOCKED


@pytest.mark.parametrize("cancellations", [1, 3])
async def test_cancelled_release_completes_cleanup(monkeypatch, cancellations):
    """
    GIVEN: Release is awaiting the backend
    WHEN: The releasing caller is cancelled, potentially more than once
    THEN: Cleanup completes before cancellation propagates
    """
    backend = AsyncFakeBackend()
    entered, respond = asyncio.Event(), asyncio.Event()
    release = backend.release

    async def delayed(*args):
        entered.set()
        await respond.wait()
        return await release(*args)

    monkeypatch.setattr(backend, "release", delayed)
    lock = AsyncLock("cancel", backend=backend)
    await lock.acquire()
    task = asyncio.create_task(lock.release())
    await entered.wait()
    for _ in range(cancellations):
        task.cancel()
        await asyncio.sleep(0)
    respond.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert backend.owner is None


async def test_async_context_surfaces_ownership_loss():
    """
    GIVEN: An async context loses ownership
    WHEN: Its body otherwise completes successfully
    THEN: Context exit raises LockExpiredError
    """
    lock = AsyncLock("lost", backend=AsyncFakeBackend())
    with pytest.raises(LockExpiredError):
        async with lock:
            lock._on_renewal_failed()


def test_async_strict_validation_rejects_sync_backend():
    """
    GIVEN: A synchronous backend
    WHEN: It is supplied to a strict async lock
    THEN: A configuration error is raised before entering an async code path
    """
    with pytest.raises(ConfigurationError, match="asynchronous"):
        AsyncLock("invalid", backend=FakeBackend())
