import asyncio

import pytest
import redis.asyncio as redis

from mithril._types import Result
from mithril.backends.aio._redis import AsyncRedisBackend

__author__ = "Rohan B. Dalton"


@pytest.mark.asyncio
async def test_async_redis_backend_acquire_release() -> None:
    """
    GIVEN: An async Redis backend
    WHEN: acquire and release are called
    THEN: The lock is acquired and released successfully
    """

    client = redis.Redis(host="localhost", port=6379, db=15, decode_responses=True)
    await client.flushdb()
    backend = AsyncRedisBackend(client, namespace="test")
    result = await backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)
    assert result is not None
    assert await backend.is_locked("lock") is True
    assert await backend.release("lock", "owner") == Result.SUCCESS


@pytest.mark.asyncio
async def test_async_redis_backend_ttl_expiry() -> None:
    """
    GIVEN: An async Redis backend with a short TTL
    WHEN: The TTL expires
    THEN: Another owner can acquire the lock
    """

    client = redis.Redis(host="localhost", port=6379, db=15, decode_responses=True)
    await client.flushdb()
    backend = AsyncRedisBackend(client, namespace="test")
    result = await backend.acquire("lock", "owner", ttl_seconds=0.1, epoch=1)
    assert result is not None
    await asyncio.sleep(0.15)
    assert await backend.acquire("lock", "other", ttl_seconds=1.0, epoch=2) is not None


if __name__ == "__main__":
    pass
