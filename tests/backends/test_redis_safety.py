import asyncio
import inspect
import uuid

import pytest
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from mithril._types import LockMode, Result
from mithril.backends._redis import RedisBackend
from mithril.backends._redis_cluster import RedisClusterBackend
from mithril.backends.aio._redis import AsyncRedisBackend


async def resolve(result):
    return await result if inspect.isawaitable(result) else result


@pytest.fixture(params=["sync", "async", "cluster"])
async def backend(request):
    """
    GIVEN: A test Redis service and an isolated key namespace
    WHEN: A sync, async, or cluster-layout backend is requested
    THEN: Tests exercise real Lua scripts without touching other namespaces
    """
    namespace = f"safety-{uuid.uuid4().hex}"
    client_class = AsyncRedis if request.param == "async" else Redis
    client = client_class(host="localhost", port=6379, db=15, decode_responses=True)
    backend_class = {
        "sync": RedisBackend,
        "async": AsyncRedisBackend,
        "cluster": RedisClusterBackend,
    }[request.param]
    backend = backend_class(client, namespace=namespace, signal_ttl=0.05)
    try:
        yield backend
    finally:
        keys = await resolve(client.keys(f"*{namespace}*"))
        if keys:
            await resolve(client.delete(*keys))
        await resolve(client.aclose() if request.param == "async" else client.close())


@pytest.mark.parametrize("operation", ["acquire", "renew", "release"])
async def test_short_reader_cannot_shorten_long_reader(backend, operation):
    """
    GIVEN: A long-lived shared reader
    WHEN: A shorter reader is acquired, renewed, or released
    THEN: Aggregate reader metadata retains the long reader's full TTL
    """
    assert await resolve(backend.acquire("lock", "long", 10, mode=LockMode.SHARED))
    assert await resolve(backend.acquire("lock", "short", 1, mode=LockMode.SHARED))
    if operation == "renew":
        assert (
            await resolve(backend.renew("lock", "short", 0.05, LockMode.SHARED))
            == Result.SUCCESS
        )
    elif operation == "release":
        assert (
            await resolve(backend.release("lock", "short", LockMode.SHARED)) == Result.SUCCESS
        )
    assert await resolve(backend._client.pttl(backend._readers_key("lock"))) > 8000
    assert await resolve(backend._client.pttl(backend._readers_total_key("lock"))) > 8000
    assert await resolve(backend.acquire("lock", "writer", 1)) is None


async def test_partial_shared_release_preserves_expiry(backend):
    """
    GIVEN: Two short-lived readers
    WHEN: One releases and the other expires
    THEN: Shared metadata expires and a writer can acquire
    """
    for owner in ("first", "second"):
        assert await resolve(backend.acquire("lock", owner, 0.1, mode=LockMode.SHARED))
    assert await resolve(backend.release("lock", "first", LockMode.SHARED)) == Result.SUCCESS
    assert 0 < await resolve(backend._client.pttl(backend._readers_total_key("lock"))) <= 100
    await asyncio.sleep(0.15)
    assert await resolve(backend.acquire("lock", "writer", 1))


async def test_expired_reader_cannot_be_revived(backend):
    """
    GIVEN: An expired reader alongside another live reader
    WHEN: The expired owner attempts renewal
    THEN: Renewal fails without affecting the live reader
    """
    assert await resolve(backend.acquire("lock", "long", 10, mode=LockMode.SHARED))
    assert await resolve(backend.acquire("lock", "short", 0.05, mode=LockMode.SHARED))
    await asyncio.sleep(0.1)
    assert await resolve(backend.renew("lock", "short", 10, LockMode.SHARED)) != Result.SUCCESS
    assert await resolve(backend.release("lock", "long", LockMode.SHARED)) == Result.SUCCESS
    assert await resolve(backend.acquire("lock", "writer", 1))


async def test_fencing_and_epoch_are_persistent(backend):
    """
    GIVEN: A previously issued fencing token and a fresh caller-supplied epoch
    WHEN: The named lock is released and reacquired
    THEN: Persistent fencing and backend-issued epochs increase together
    """
    assert await resolve(backend.acquire("lock", "first", 1, epoch=0)) == (1, 1)
    assert await resolve(backend._client.pttl(backend._fence_key("lock"))) == -1
    assert await resolve(backend.release("lock", "first")) == Result.SUCCESS
    assert await resolve(backend.acquire("lock", "second", 1, epoch=0)) == (2, 2)
    assert await resolve(backend.renew("lock", "second", 1)) == Result.SUCCESS
    assert await resolve(backend._client.pttl(backend._fence_key("lock"))) == -1


async def test_lock_names_cannot_alias_metadata(backend):
    """
    GIVEN: Lock names resembling internal Redis metadata keys
    WHEN: Each logical lock is acquired and released independently
    THEN: No acquisition or cleanup modifies another lock
    """
    names = ["lock", "lock:meta", "lock:readers", "fence:lock", "{lock}", "%7Block%7D"]
    for name in names:
        assert await resolve(backend.acquire(name, name, 10)) == (1, 1)
    assert await resolve(backend.release("lock", "lock")) == Result.SUCCESS
    for name in names[1:]:
        assert await resolve(backend.get_owner(name)) == name


async def test_fencing_preserves_large_integer_precision(backend):
    """
    GIVEN: A fencing counter beyond Lua's exact floating-point integer range
    WHEN: Two successive acquisitions issue tokens
    THEN: Tokens remain exact and strictly increasing
    """
    token = 2**53
    await resolve(backend._client.set(backend._fence_key("lock"), token))
    assert await resolve(backend.acquire("lock", "first", 1)) == (token + 1, token + 1)
    assert await resolve(backend.release("lock", "first")) == Result.SUCCESS
    assert await resolve(backend.acquire("lock", "second", 1)) == (token + 2, token + 2)
