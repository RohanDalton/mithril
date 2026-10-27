# Mithril

Mithril provides distributed locks with fencing tokens, ownership checks, TTLs,
and opt-in automatic renewal. It includes synchronous and asynchronous APIs,
Redis, Redis Cluster, Redlock, PostgreSQL, SQLite, and local file backends.

## Installation

```sh
python -m pip install mithril-locks
```

The distribution is named `mithril-locks`; Python imports use `mithril`.
Python 3.9 or newer is required. Database drivers are available as extras:

```sh
python -m pip install 'mithril-locks[postgres]'
python -m pip install 'mithril-locks[async-postgres]'
python -m pip install 'mithril-locks[async-sqlite]'
```

## Usage

```python
from redis import Redis

from mithril import Lock
from mithril.backends import RedisBackend

client = Redis.from_url("redis://localhost:6379/0", socket_timeout=5, socket_connect_timeout=5)
backend = RedisBackend(client)

with Lock("payment", backend=backend, ttl=30, timeout=10) as token:
    print(token.fencing_token)
    # Pass the fencing token to your protected resource with each write.
```

Context managers release the lock on exit and return a `LockToken`. Lock objects
are single-use. For work that may exceed the TTL, pass `auto_renewal=True`.

The async API uses `mithril.aio.AsyncLock`,
`mithril.backends.aio.AsyncRedisBackend`, and a `redis.asyncio.Redis` client:

```python
import asyncio

from redis.asyncio import Redis

from mithril.aio import AsyncLock
from mithril.backends.aio import AsyncRedisBackend


async def main():
    async with Redis.from_url(
        "redis://localhost:6379/0", socket_timeout=5, socket_connect_timeout=5
    ) as client:
        backend = AsyncRedisBackend(client)
        async with AsyncLock("payment", backend=backend, ttl=30) as token:
            print(token.fencing_token)


asyncio.run(main())
```

## Fencing and durability

Your downstream resource must atomically reject writes carrying a fencing token
older than the highest token it has accepted. A TTL alone cannot stop a delayed
holder from writing after its lease expires. Fencing counters must be durable:
Redis failover, eviction, or restoration of an older snapshot can invalidate
ordering guarantees. Redlock requires an explicit shared fencing authority.
File locks are for processes on the same host.

## Documentation

The [user guide](docs/source/index.rst) explains Mithril's current behavior using
the failure-oriented approach of Martin Kleppmann's *Designing Data-Intensive
Applications*. Start with:

- [Fencing tokens](docs/source/fencing.rst): when to use them, atomic enforcement,
  repeated writes, and a runnable SQL example.
- [Choosing a backend](docs/source/backends.rst): recommendations and the
  durability, failover, and availability assumptions of each implementation.
- [Designing for failures](docs/source/concepts.rst): invariants, leases, and
  alternatives to distributed locking.
- [Operating Mithril](docs/source/operations.rst): renewal, timeouts, recovery,
  observability, and failure testing.
- [API and lifecycle](docs/source/api.rst) and [sources](docs/source/references.rst).

Build the HTML documentation from a checkout:

```sh
pixi run -e dev python -m sphinx -W --keep-going -b html docs/source docs/build/html
```

Open `docs/build/html/index.html`. Run the local fencing demonstration with
`pixi run -e dev python docs/examples/fenced_snapshot.py`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
