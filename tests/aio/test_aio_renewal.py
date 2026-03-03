import asyncio
import weakref

import pytest

from mithril.aio._renewal import AsyncLockRenewer

__author__ = "Rohan B. Dalton"


class AsyncFakeBackend:
    def __init__(self, results: list[bool]) -> None:
        self._results = results
        self.calls: int = 0

    async def renew(self, _key: str, _owner_id: str, _ttl: float) -> bool:
        self.calls += 1
        if self._results:
            return self._results.pop(0)
        return True


class AsyncFakeLock:
    def __init__(self) -> None:
        self.failed = False

    def _on_renewal_failed(self) -> None:
        self.failed = True


@pytest.mark.asyncio
async def test_async_renewal_failure_notifies_lock() -> None:
    """
    GIVEN: An async renewer with failing backend
    WHEN: The renewal task runs
    THEN: The lock is notified of failure
    """

    lock = AsyncFakeLock()
    backend = AsyncFakeBackend([False])
    renewer = AsyncLockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
    )
    renewer.start()

    deadline = asyncio.get_running_loop().time() + 1.0
    while not lock.failed and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)

    await renewer.stop()
    assert lock.failed is True


@pytest.mark.asyncio
async def test_async_renewal_stop_cancels_task() -> None:
    """
    GIVEN: An async renewer running in the background
    WHEN: stop is called
    THEN: The task is cancelled cleanly
    """

    lock = AsyncFakeLock()
    backend = AsyncFakeBackend([True, True])
    renewer = AsyncLockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.1,
    )
    renewer.start()
    await asyncio.sleep(0.02)
    await renewer.stop()


class AsyncRaisingBackend:
    """Backend that raises on renew."""

    async def renew(self, _key: str, _owner_id: str, _ttl: float):
        raise RuntimeError("connection lost")


@pytest.mark.asyncio
async def test_async_renewal_exception_triggers_failure() -> None:
    """
    GIVEN: An async backend that raises an exception during renewal
    WHEN: The renewal task runs
    THEN: The lock is notified of failure
    """

    lock = AsyncFakeLock()
    backend = AsyncRaisingBackend()
    renewer = AsyncLockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
    )
    renewer.start()

    deadline = asyncio.get_running_loop().time() + 1.0
    while not lock.failed and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)

    await renewer.stop()
    assert lock.failed is True


if __name__ == "__main__":
    pass
