import asyncio

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from mithril._types import LockMode, Result
from mithril.aio import AsyncLock
from mithril.backends.aio._postgres import AsyncPostgresBackend

__author__ = "Rohan B. Dalton"

_PG_URL = "postgresql+asyncpg://mithril:mithril@localhost:5532/mithril"


@pytest_asyncio.fixture()
async def async_pg_engine():
    """
    GIVEN: A local Postgres instance for testing
    WHEN: An async engine is requested
    THEN: A healthy engine with a clean schema is returned
    """
    engine = create_async_engine(_PG_URL)
    async with engine.connect() as conn:
        backend = AsyncPostgresBackend(conn)
        await backend.initialize()
    async with engine.connect() as conn:
        async with conn.begin():
            await conn.execute(text("DELETE FROM mithril_locks"))
            await conn.execute(text("DELETE FROM mithril_readers"))
            await conn.execute(text("DELETE FROM mithril_fencing"))
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture()
async def backend(async_pg_engine):
    """
    GIVEN: An async Postgres engine
    WHEN: A backend is created
    THEN: The backend is ready for use with clean state
    """
    conn = await async_pg_engine.connect()
    backend = AsyncPostgresBackend(conn)
    yield backend
    await conn.close()


@pytest.mark.asyncio
async def test_acquire_returns_token_and_epoch(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: An unlocked Postgres key
    WHEN: acquire is called
    THEN: A fencing token and epoch are returned
    """
    result = await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token, epoch = result
    assert token >= 1
    assert epoch == 1


@pytest.mark.asyncio
async def test_acquire_fails_when_lock_held(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called again
    THEN: The acquisition fails
    """
    assert await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert await backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None


@pytest.mark.asyncio
async def test_release_requires_owner(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts release
    THEN: Release reports wrong-owner
    """
    assert await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert await backend.release("lock", "other") == Result.WRONG_OWNER
    assert await backend.release("lock", "owner") == Result.SUCCESS


@pytest.mark.asyncio
async def test_renew_requires_owner(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts renewal
    THEN: Renewal reports wrong-owner
    """
    assert await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert await backend.renew("lock", "other", ttl_seconds=5.0) == Result.WRONG_OWNER
    assert await backend.renew("lock", "owner", ttl_seconds=5.0) == Result.SUCCESS


@pytest.mark.asyncio
async def test_shared_allows_multiple_readers(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: An async Postgres backend with shared mode
    WHEN: Two readers acquire the same lock
    THEN: Both acquisitions succeed
    """
    r1 = await backend.acquire(
        "lock", "reader1", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED
    )
    r2 = await backend.acquire(
        "lock", "reader2", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED
    )
    assert r1 is not None
    assert r2 is not None
    assert await backend.release("lock", "reader1", mode=LockMode.SHARED) == Result.SUCCESS
    assert await backend.release("lock", "reader2", mode=LockMode.SHARED) == Result.SUCCESS


@pytest.mark.asyncio
async def test_writer_blocks_readers(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A writer lock is held
    WHEN: A reader attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=5.0, epoch=1, mode=LockMode.EXCLUSIVE
        )
        is not None
    )
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
        is None
    )


@pytest.mark.asyncio
async def test_readers_block_writer(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: Readers hold a shared lock
    WHEN: A writer attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=5.0, epoch=2, mode=LockMode.EXCLUSIVE
        )
        is None
    )


@pytest.mark.asyncio
async def test_shared_reentrant_count_blocks_writer_until_released(
    backend: AsyncPostgresBackend,
) -> None:
    """
    GIVEN: A shared reader acquires the lock twice
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer is blocked until both shared releases occur
    """
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
        is not None
    )

    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=5.0, epoch=3, mode=LockMode.EXCLUSIVE
        )
        is None
    )

    assert await backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=5.0, epoch=4, mode=LockMode.EXCLUSIVE
        )
        is None
    )

    assert await backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=5.0, epoch=5, mode=LockMode.EXCLUSIVE
        )
        is not None
    )
    assert await backend.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


@pytest.mark.asyncio
async def test_shared_expiry_allows_writer(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A shared reader that expires
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer can acquire the lock
    """
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=0.2, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    await asyncio.sleep(0.35)
    assert (
        await backend.acquire(
            "lock", "writer", ttl_seconds=1.0, epoch=2, mode=LockMode.EXCLUSIVE
        )
        is not None
    )
    assert await backend.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


@pytest.mark.asyncio
async def test_shared_release_wrong_owner(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A shared lock held by another reader
    WHEN: A wrong owner attempts release
    THEN: Release reports not-found
    """
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    actual = await backend.release("lock", "other", mode=LockMode.SHARED)
    assert actual == Result.NOT_FOUND
    assert await backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


@pytest.mark.asyncio
async def test_get_owner_and_is_locked(backend: AsyncPostgresBackend) -> None:
    """
    GIVEN: A lock acquired by an owner
    WHEN: get_owner and is_locked are queried
    THEN: They report the correct state before and after release
    """
    assert await backend.get_owner("lock") is None
    assert await backend.is_locked("lock") is False

    await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert await backend.get_owner("lock") == "owner"
    assert await backend.is_locked("lock") is True

    await backend.release("lock", "owner")
    assert await backend.get_owner("lock") is None
    assert await backend.is_locked("lock") is False


@pytest.mark.asyncio
async def test_failed_acquire_does_not_increment_fencing_token(
    backend: AsyncPostgresBackend,
) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner fails to acquire
    THEN: The fencing token is not incremented
    """
    result = await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token_after_acquire = result[0]

    for _ in range(5):
        assert await backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None

    await backend.release("lock", "owner")
    result2 = await backend.acquire("lock", "owner2", ttl_seconds=5.0, epoch=3)
    assert result2 is not None
    token_after_reacquire = result2[0]

    assert token_after_reacquire == token_after_acquire + 1


@pytest.mark.asyncio
async def test_failed_shared_acquire_does_not_increment_fencing_token(
    backend: AsyncPostgresBackend,
) -> None:
    """
    GIVEN: An exclusive lock held by one owner
    WHEN: A shared acquire fails
    THEN: The fencing token is not incremented
    """
    result = await backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token_after_acquire = result[0]

    for _ in range(5):
        assert (
            await backend.acquire(
                "lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED
            )
            is None
        )

    await backend.release("lock", "writer")
    result2 = await backend.acquire("lock", "owner2", ttl_seconds=5.0, epoch=3)
    assert result2 is not None
    token_after_reacquire = result2[0]

    assert token_after_reacquire == token_after_acquire + 1


async def test_shared_backend_has_independent_transactions(backend):
    """
    GIVEN: One async Postgres backend shared by concurrent tasks
    WHEN: Independent locks are acquired and released
    THEN: Connections are isolated and epochs persist between facade instances
    """

    async def use_lock(number):
        name = f"concurrent-{number}"
        async with AsyncLock(name, backend=backend, ttl=5) as first:
            assert first.epoch == 1
        async with AsyncLock(name, backend=backend, ttl=5) as second:
            assert second.epoch > first.epoch

    await asyncio.gather(*(use_lock(number) for number in range(20)))


if __name__ == "__main__":
    pass
