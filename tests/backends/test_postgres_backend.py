import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, text

from mithril import Lock, LockState
from mithril._types import LockMode, Result
from mithril.backends._postgres import PostgresBackend

__author__ = "Rohan B. Dalton"

_PG_URL = "postgresql://mithril:mithril@localhost:5532/mithril"


@pytest.fixture()
def pg_engine():
    """
    GIVEN: A local Postgres instance for testing
    WHEN: An engine is requested
    THEN: A healthy engine with a clean schema is returned
    """
    engine = create_engine(_PG_URL)
    with engine.connect() as conn:
        PostgresBackend(conn)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("DELETE FROM mithril_locks"))
            conn.execute(text("DELETE FROM mithril_readers"))
            conn.execute(text("DELETE FROM mithril_fencing"))
    yield engine
    engine.dispose()


@pytest.fixture()
def backend(pg_engine):
    """
    GIVEN: A Postgres engine
    WHEN: A backend is created
    THEN: The backend is ready for use with clean state
    """
    conn = pg_engine.connect()
    backend = PostgresBackend(conn)
    yield backend
    conn.close()


def test_acquire_returns_token_and_epoch(backend: PostgresBackend) -> None:
    """
    GIVEN: An unlocked Postgres key
    WHEN: acquire is called
    THEN: A fencing token and epoch are returned
    """
    result = backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert result is not None
    token, epoch = result
    assert token >= 1
    assert epoch == 1


def test_acquire_fails_when_lock_held(backend: PostgresBackend) -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called again
    THEN: The acquisition fails
    """
    assert backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert backend.acquire("lock", "other", ttl_seconds=5.0, epoch=2) is None


def test_release_requires_owner(backend: PostgresBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts release
    THEN: Release reports not-owner
    """
    assert backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert backend.release("lock", "other") == Result.WRONG_OWNER
    assert backend.release("lock", "owner") == Result.SUCCESS


def test_renew_requires_owner(backend: PostgresBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts renewal
    THEN: Renewal reports not-owner
    """
    assert backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1) is not None
    assert backend.renew("lock", "other", ttl_seconds=5.0) == Result.WRONG_OWNER
    assert backend.renew("lock", "owner", ttl_seconds=5.0) == Result.SUCCESS


def test_shared_allows_multiple_readers(backend: PostgresBackend) -> None:
    """
    GIVEN: A Postgres backend with shared mode
    WHEN: Two readers acquire the same lock
    THEN: Both acquisitions succeed
    """
    r1 = backend.acquire("lock", "reader1", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    r2 = backend.acquire("lock", "reader2", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
    assert r1 is not None
    assert r2 is not None
    assert backend.release("lock", "reader1", mode=LockMode.SHARED) == Result.SUCCESS
    assert backend.release("lock", "reader2", mode=LockMode.SHARED) == Result.SUCCESS


def test_writer_blocks_readers(backend: PostgresBackend) -> None:
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


def test_readers_block_writer(backend: PostgresBackend) -> None:
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


def test_shared_reentrant_count_blocks_writer_until_released(
    backend: PostgresBackend,
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


def test_shared_expiry_allows_writer(backend: PostgresBackend) -> None:
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


def test_shared_release_wrong_owner_reports_not_owner(backend: PostgresBackend) -> None:
    """
    GIVEN: A shared lock held by another reader
    WHEN: A wrong owner attempts release
    THEN: Release reports not-owner
    """
    assert (
        backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    actual = backend.release("lock", "other", mode=LockMode.SHARED)
    assert actual == Result.NOT_FOUND
    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


def test_get_owner_and_is_locked(backend: PostgresBackend) -> None:
    """
    GIVEN: A lock acquired by an owner
    WHEN: get_owner and is_locked are queried
    THEN: They report the correct state before and after release
    """
    assert backend.get_owner("lock") is None
    assert backend.is_locked("lock") is False

    backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert backend.get_owner("lock") == "owner"
    assert backend.is_locked("lock") is True

    backend.release("lock", "owner")
    assert backend.get_owner("lock") is None
    assert backend.is_locked("lock") is False


def test_isolation_level_is_serializable(pg_engine) -> None:
    """
    GIVEN: A PostgresBackend
    WHEN: The transaction isolation level is queried
    THEN: Private transactions are serializable without modifying the caller
    """
    conn = pg_engine.connect()
    original = conn.get_isolation_level()
    backend = PostgresBackend(conn)
    with backend._transaction() as transaction:
        result = transaction.execute(text("SHOW transaction_isolation"))
        level = result.scalar()
    assert conn.get_isolation_level() == original
    conn.close()
    assert level == "serializable"


def test_concurrent_writer_and_reader_cannot_both_succeed(pg_engine) -> None:
    """
    GIVEN: Two connections racing to acquire exclusive and shared locks
    WHEN: Both attempt acquisition concurrently over many iterations
    THEN: Both never succeed simultaneously

    This is the regression test for the shared-lock race condition
    under READ COMMITTED isolation. Under SERIALIZABLE, PostgreSQL's
    SSI detects the rw-antidependency cycle between the writer
    (reads mithril_readers, writes mithril_locks) and the reader
    (reads mithril_locks, writes mithril_readers) and aborts one
    transaction, which the backend converts to a failed acquisition.
    """
    violations = []
    iterations = 50

    for idx in range(iterations):
        with pg_engine.connect() as cleanup:
            with cleanup.begin():
                cleanup.execute(text("DELETE FROM mithril_locks"))
                cleanup.execute(text("DELETE FROM mithril_readers"))
                cleanup.execute(text("DELETE FROM mithril_fencing"))

        writer_conn = pg_engine.connect()
        reader_conn = pg_engine.connect()
        writer_backend = PostgresBackend(writer_conn)
        reader_backend = PostgresBackend(reader_conn)

        results = [None, None]
        errors = []
        barrier = threading.Barrier(2, timeout=5)

        def writer() -> None:
            try:
                barrier.wait()
                results[0] = writer_backend.acquire(
                    "lock",
                    "writer",
                    ttl_seconds=5.0,
                    epoch=1,
                    mode=LockMode.EXCLUSIVE,
                )
            except Exception as exc:
                errors.append(exc)

        def reader() -> None:
            try:
                barrier.wait()
                results[1] = reader_backend.acquire(
                    "lock",
                    "reader",
                    ttl_seconds=5.0,
                    epoch=1,
                    mode=LockMode.SHARED,
                )
            except Exception as exc:
                errors.append(exc)

        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)
        writer_thread.start()
        reader_thread.start()
        writer_thread.join(timeout=5)
        reader_thread.join(timeout=5)

        assert not errors

        if results[0] is not None and results[1] is not None:
            violations.append(idx)

        if results[0] is not None:
            writer_backend.release("lock", "writer", LockMode.EXCLUSIVE)
        if results[1] is not None:
            reader_backend.release("lock", "reader", LockMode.SHARED)

        writer_conn.close()
        reader_conn.close()

    assert not violations, (
        f"Writer and reader both acquired the lock in "
        f"{len(violations)}/{iterations} iterations"
    )


def test_failed_acquire_does_not_increment_fencing_token(backend: PostgresBackend) -> None:
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


def test_failed_shared_acquire_does_not_increment_fencing_token(
    backend: PostgresBackend,
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


def test_shared_backend_has_independent_transactions(backend):
    """
    GIVEN: One Postgres backend shared by concurrent threads
    WHEN: Independent locks are acquired and released
    THEN: Connections are isolated and epochs persist between facade instances
    """

    def use_lock(number):
        name = f"concurrent-{number}"
        with Lock(name, backend=backend, ttl=5) as first:
            assert first.epoch == 1
        with Lock(name, backend=backend, ttl=5) as second:
            assert second.epoch > first.epoch

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(use_lock, range(20)))


def test_postgres_auto_renewal_uses_independent_connection(backend):
    """
    GIVEN: A Postgres backend with automatic renewal on a background thread
    WHEN: The lock outlives its initial TTL
    THEN: Renewal refreshes both backend ownership and local state
    """
    lock = Lock(
        "renew-thread", backend=backend, ttl=0.5, auto_renewal=True, renewal_interval=0.1
    )
    with lock:
        time.sleep(0.8)
        assert lock.state == LockState.ACQUIRED
        assert backend.get_owner("renew-thread") == lock.owner_id
    assert backend.get_owner("renew-thread") is None


if __name__ == "__main__":
    pass
