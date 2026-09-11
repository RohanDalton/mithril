import time

import pytest
import redis

from mithril._types import LockMode, Result
from mithril.backends._redis import RedisBackend

__author__ = "Rohan B. Dalton"


@pytest.fixture()
def redis_client() -> redis.Redis:
    """
    GIVEN: A local Redis instance for testing
    WHEN: A client is requested
    THEN: A healthy client is returned
    """
    client = redis.Redis(host="localhost", port=6379, db=15, decode_responses=True)
    client.ping()
    client.flushdb()
    return client


@pytest.fixture()
def backend(redis_client: redis.Redis) -> RedisBackend:
    """
    GIVEN: A Redis client
    WHEN: A RedisBackend is created
    THEN: The backend is ready for use
    """
    return RedisBackend(redis_client, namespace="test")


def test_acquire_returns_token_and_epoch(backend: RedisBackend) -> None:
    """
    GIVEN: An unlocked Redis key
    WHEN: acquire is called
    THEN: A fencing token and epoch are returned
    """
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.23)
    assert result is not None
    token, epoch = result
    assert token == 1
    assert epoch == 1


def test_acquire_fails_when_lock_held(backend: RedisBackend) -> None:
    """
    GIVEN: A lock held by another owner
    WHEN: acquire is called again
    THEN: The acquisition fails
    """
    assert backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1) is not None
    assert backend.acquire("lock", "other", ttl_seconds=1.0, epoch=2) is None


def test_release_requires_owner(backend: RedisBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts release
    THEN: Release reports not-owner
    """
    assert backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1) is not None
    assert backend.release("lock", "other") == Result.WRONG_OWNER
    assert backend.release("lock", "owner") == Result.SUCCESS


def test_renew_requires_owner(backend: RedisBackend) -> None:
    """
    GIVEN: A lock held by one owner
    WHEN: Another owner attempts renewal
    THEN: Renewal reports not-owner
    """
    assert backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1) is not None
    assert backend.renew("lock", "other", ttl_seconds=1.0) == Result.WRONG_OWNER
    assert backend.renew("lock", "owner", ttl_seconds=1.0) == Result.SUCCESS


def test_release_signals_waiters(backend: RedisBackend) -> None:
    """
    GIVEN: A lock held and a waiter blocked on signal
    WHEN: The lock is released
    THEN: The waiter is signaled
    """
    assert backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1) is not None
    assert backend.wait_signal("lock", max_wait=0.01) is False
    assert backend.release("lock", "owner") == Result.SUCCESS
    assert backend.wait_signal("lock", max_wait=1.0) is True


def test_lock_expires_and_can_be_reacquired(backend: RedisBackend) -> None:
    """
    GIVEN: A lock with a short TTL
    WHEN: The TTL expires
    THEN: Another owner can acquire the lock
    """
    assert backend.acquire("lock", "owner", ttl_seconds=0.1, epoch=1) is not None
    time.sleep(0.15)
    assert backend.acquire("lock", "other", ttl_seconds=1.0, epoch=2) is not None


def test_redis_shared_allows_multiple_readers(backend: RedisBackend) -> None:
    """
    GIVEN: A Redis backend with shared mode
    WHEN: Two readers acquire the same lock
    THEN: Both acquisitions succeed
    """
    result1 = backend.acquire(
        "lock", "reader1", ttl_seconds=1.0, epoch=1, mode=LockMode.SHARED
    )
    result2 = backend.acquire(
        "lock", "reader2", ttl_seconds=1.0, epoch=2, mode=LockMode.SHARED
    )

    assert result1 is not None
    assert result2 is not None
    assert backend.release("lock", "reader1", mode=LockMode.SHARED) == Result.SUCCESS
    assert backend.release("lock", "reader2", mode=LockMode.SHARED) == Result.SUCCESS


def test_redis_writer_blocks_readers(backend: RedisBackend) -> None:
    """
    GIVEN: A writer lock is held
    WHEN: A reader attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        backend.acquire("lock", "writer", ttl_seconds=1.0, epoch=1, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert (
        backend.acquire("lock", "reader", ttl_seconds=1.0, epoch=2, mode=LockMode.SHARED)
        is None
    )


def test_redis_readers_block_writer(backend: RedisBackend) -> None:
    """
    GIVEN: Readers hold a shared lock
    WHEN: A writer attempts to acquire
    THEN: The acquisition fails
    """
    assert (
        backend.acquire("lock", "reader1", ttl_seconds=1.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        backend.acquire("lock", "reader2", ttl_seconds=1.0, epoch=2, mode=LockMode.SHARED)
        is not None
    )
    assert (
        backend.acquire("lock", "writer", ttl_seconds=1.0, epoch=3, mode=LockMode.EXCLUSIVE)
        is None
    )


if __name__ == "__main__":
    pass
