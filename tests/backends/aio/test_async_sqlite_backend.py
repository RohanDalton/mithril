import asyncio

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from mithril._types import LockMode, Result
from mithril.backends.aio._sqlite import AsyncSQLiteBackend

__author__ = "Rohan B. Dalton"


@pytest_asyncio.fixture()
async def backend(tmp_path):
    """
    GIVEN: A temporary SQLite database
    WHEN: An async backend is created
    THEN: The backend is ready for use with clean state
    """
    db_path = tmp_path / "mithril.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    conn = await engine.connect()
    b = AsyncSQLiteBackend(conn)
    await b.initialize()
    yield b
    await conn.close()
    await engine.dispose()


@pytest.mark.asyncio
async def test_acquire_returns_token_and_epoch(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: An unlocked SQLite key
    WHEN: acquire is called
    THEN: A fencing token and epoch are returned
    """
    result = await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token, epoch = result
    assert token >= 1
    assert epoch == 1


@pytest.mark.asyncio
async def test_acquire_fails_when_lock_held(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called again
    THEN: The acquisition fails
    """
    assert await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert await backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None


@pytest.mark.asyncio
async def test_readers_block_writer(backend: AsyncSQLiteBackend) -> None:
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
async def test_writer_blocks_readers(backend: AsyncSQLiteBackend) -> None:
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
async def test_shared_reentrant_count_blocks_writer_until_released(
    backend: AsyncSQLiteBackend,
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
async def test_shared_expiry_allows_writer(backend: AsyncSQLiteBackend) -> None:
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
async def test_shared_release_wrong_owner(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by another reader
    WHEN: A wrong owner attempts release
    THEN: Release reports not-found
    """
    assert (
        await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert await backend.release("lock", "other", mode=LockMode.SHARED) == Result.NOT_FOUND
    assert await backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


@pytest.mark.asyncio
async def test_failed_acquire_does_not_increment_fencing_token(
    backend: AsyncSQLiteBackend,
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
    backend: AsyncSQLiteBackend,
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


@pytest.mark.asyncio
async def test_renew_exclusive_success(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock held by an owner
    WHEN: renew is called with the same owner
    THEN: Result.SUCCESS is returned and the lock is still held
    """
    await backend.acquire("lock", "owner", ttl_seconds=0.3, epoch=1)
    assert await backend.renew("lock", "owner", ttl_seconds=5.0) == Result.SUCCESS
    assert await backend.is_locked("lock") is True


@pytest.mark.asyncio
async def test_renew_exclusive_wrong_owner(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock held by owner A
    WHEN: owner B attempts to renew
    THEN: Result.WRONG_OWNER is returned
    """
    await backend.acquire("lock", "A", ttl_seconds=5.0, epoch=1)
    assert await backend.renew("lock", "B", ttl_seconds=5.0) == Result.WRONG_OWNER


@pytest.mark.asyncio
async def test_renew_exclusive_expired(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock with a short TTL that has expired
    WHEN: renew is called
    THEN: Result.NOT_FOUND is returned
    """
    await backend.acquire("lock", "owner", ttl_seconds=0.2, epoch=1)
    await asyncio.sleep(0.35)
    assert await backend.renew("lock", "owner", ttl_seconds=5.0) == Result.NOT_FOUND


@pytest.mark.asyncio
async def test_release_exclusive_not_found(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: No lock exists
    WHEN: release is called
    THEN: Result.NOT_FOUND is returned
    """
    assert await backend.release("lock", "owner") == Result.NOT_FOUND


@pytest.mark.asyncio
async def test_shared_renew_success(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by a reader
    WHEN: renew is called by the same reader
    THEN: Result.SUCCESS is returned
    """
    await backend.acquire("lock", "reader", ttl_seconds=0.3, epoch=1, mode=LockMode.SHARED)
    assert (
        await backend.renew("lock", "reader", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.SUCCESS
    )
    assert await backend.is_locked("lock") is True


@pytest.mark.asyncio
async def test_shared_renew_wrong_owner(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by one reader
    WHEN: A different owner attempts to renew
    THEN: Result.NOT_FOUND is returned (no reader entry for that owner)
    """
    await backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    assert (
        await backend.renew("lock", "other", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.NOT_FOUND
    )


@pytest.mark.asyncio
async def test_release_requires_owner(backend: AsyncSQLiteBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts release
    THEN: Release reports wrong-owner
    """
    assert await backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert await backend.release("lock", "other") == Result.WRONG_OWNER
    assert await backend.release("lock", "owner") == Result.SUCCESS


if __name__ == "__main__":
    pass
