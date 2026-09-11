import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from mithril._types import LockMode, Result
from mithril.backends._sqlite import _SYNC_FENCING_SQL, SQLiteBackend
from mithril.exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


@pytest.fixture()
def backend(tmp_path):
    """
    GIVEN: A temporary SQLite database
    WHEN: A backend is created
    THEN: The backend is ready for use with clean state
    """
    db_path = tmp_path / "mithril.db"
    engine = create_engine(f"sqlite:///{db_path}")
    conn = engine.connect()
    b = SQLiteBackend(conn)
    yield b
    conn.close()
    engine.dispose()


def test_acquire_returns_token_and_epoch(backend: SQLiteBackend) -> None:
    """
    GIVEN: An unlocked SQLite key
    WHEN: acquire is called
    THEN: A fencing token and epoch are returned
    """
    result = backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token, epoch = result
    assert token >= 1
    assert epoch == 1


def test_acquire_fails_when_lock_held(backend: SQLiteBackend) -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called again
    THEN: The acquisition fails
    """
    assert backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None


def test_readers_block_writer(backend: SQLiteBackend) -> None:
    """
    GIVEN: Readers hold a shared lock
    WHEN: A writer attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=2, mode=LockMode.EXCLUSIVE)
        is None
    )


def test_writer_blocks_readers(backend: SQLiteBackend) -> None:
    """
    GIVEN: A writer lock is held
    WHEN: A reader attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=1, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
        is None
    )


def test_shared_reentrant_count_blocks_writer_until_released(
    backend: SQLiteBackend,
) -> None:
    """
    GIVEN: A shared reader acquires the lock twice
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer is blocked until both shared releases occur
    """
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
        is not None
    )

    assert (
        backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=3, mode=LockMode.EXCLUSIVE)
        is None
    )

    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=4, mode=LockMode.EXCLUSIVE)
        is None
    )

    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=5, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert backend.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_expiry_allows_writer(backend: SQLiteBackend) -> None:
    """
    GIVEN: A shared reader that expires
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer can acquire the lock
    """
    assert (
        backend.acquire("lock", "reader", ttl_seconds=0.2, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    time.sleep(0.35)
    assert (
        backend.acquire("lock", "writer", ttl_seconds=1.0, epoch=2, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert backend.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_release_wrong_owner_reports_not_owner(backend: SQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by another reader
    WHEN: A wrong owner attempts release
    THEN: Release reports not-owner
    """
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert backend.release("lock", "other", mode=LockMode.SHARED) == Result.NOT_FOUND
    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


def test_failed_acquire_does_not_increment_fencing_token(backend: SQLiteBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner fails to acquire
    THEN: The fencing token is not incremented
    """
    result = backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token_after_acquire = result[0]

    for _ in range(5):
        assert backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None

    backend.release("lock", "owner")
    result2 = backend.acquire("lock", "owner2", ttl_seconds=5.0, epoch=3)
    assert result2 is not None
    token_after_reacquire = result2[0]

    assert token_after_reacquire == token_after_acquire + 1


def test_renew_exclusive_success(backend: SQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock held by an owner
    WHEN: renew is called with the same owner
    THEN: Result.SUCCESS is returned and the lock is still held
    """
    backend.acquire("lock", "owner", ttl_seconds=0.3, epoch=1)
    assert backend.renew("lock", "owner", ttl_seconds=5.0) == Result.SUCCESS
    assert backend.is_locked("lock") is True


def test_renew_exclusive_wrong_owner(backend: SQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock held by owner A
    WHEN: owner B attempts to renew
    THEN: Result.WRONG_OWNER is returned
    """
    backend.acquire("lock", "A", ttl_seconds=5.0, epoch=1)
    assert backend.renew("lock", "B", ttl_seconds=5.0) == Result.WRONG_OWNER


def test_renew_exclusive_expired(backend: SQLiteBackend) -> None:
    """
    GIVEN: An exclusive lock with a short TTL that has expired
    WHEN: renew is called
    THEN: Result.NOT_FOUND is returned
    """
    backend.acquire("lock", "owner", ttl_seconds=0.2, epoch=1)
    time.sleep(0.35)
    assert backend.renew("lock", "owner", ttl_seconds=5.0) == Result.NOT_FOUND


def test_release_exclusive_not_found(backend: SQLiteBackend) -> None:
    """
    GIVEN: No lock exists
    WHEN: release is called
    THEN: Result.NOT_FOUND is returned
    """
    assert backend.release("lock", "owner") == Result.NOT_FOUND


def test_shared_renew_success(backend: SQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by a reader
    WHEN: renew is called by the same reader
    THEN: Result.SUCCESS is returned
    """
    backend.acquire("lock", "reader", ttl_seconds=0.3, epoch=1, mode=LockMode.SHARED)
    assert (
        backend.renew("lock", "reader", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.SUCCESS
    )
    assert backend.is_locked("lock") is True


def test_shared_renew_wrong_owner(backend: SQLiteBackend) -> None:
    """
    GIVEN: A shared lock held by one reader
    WHEN: A different owner attempts to renew
    THEN: Result.NOT_FOUND is returned (no reader entry for that owner)
    """
    backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    assert (
        backend.renew("lock", "other", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.NOT_FOUND
    )


def test_failed_shared_acquire_does_not_increment_fencing_token(
    backend: SQLiteBackend,
) -> None:
    """
    GIVEN: An exclusive lock held by one owner
    WHEN: A shared acquire fails
    THEN: The fencing token is not incremented
    """
    result = backend.acquire("lock", "writer", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token_after_acquire = result[0]

    for _ in range(5):
        assert (
            backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
            is None
        )

    backend.release("lock", "writer")
    result2 = backend.acquire("lock", "owner2", ttl_seconds=5.0, epoch=3)
    assert result2 is not None
    token_after_reacquire = result2[0]

    assert token_after_reacquire == token_after_acquire + 1


def test_failed_fencing_update_rolls_back_acquisition(backend, monkeypatch):
    """
    GIVEN: A failure after the lease insert but before fencing synchronization
    WHEN: The acquisition transaction rolls back
    THEN: Neither ownership nor the fencing counter is committed
    """
    execute = Connection.execute

    def fail_fencing(connection, statement, *args, **kwargs):
        if statement is _SYNC_FENCING_SQL:
            raise RuntimeError("fencing failed")
        return execute(connection, statement, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Connection, "execute", fail_fencing)
        with pytest.raises(RuntimeError, match="fencing failed"):
            backend.acquire("lock", "first", 5)
    assert backend.get_owner("lock") is None
    assert backend.acquire("lock", "second", 5) == (1, 1)


def test_shared_backend_supports_concurrent_connections(backend):
    """
    GIVEN: One database backend shared by multiple threads
    WHEN: Independent locks are acquired and released concurrently
    THEN: Each operation has its own transaction and fencing advances per name
    """

    def acquire_and_release(number):
        name = f"lock-{number}"
        assert backend.acquire(name, "first", 5) == (1, 1)
        assert backend.release(name, "first") == Result.SUCCESS
        assert backend.acquire(name, "second", 5) == (2, 2)
        assert backend.release(name, "second") == Result.SUCCESS

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(acquire_and_release, range(20)))


def test_unsafe_in_memory_pool_is_rejected():
    """
    GIVEN: An in-memory SQLite engine with thread-local connections
    WHEN: A database backend is constructed
    THEN: Configuration fails instead of silently using different lock databases
    """
    engine = create_engine("sqlite://")
    try:
        with pytest.raises(ConfigurationError, match="independent-connection"):
            SQLiteBackend(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    pass
