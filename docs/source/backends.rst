Choosing a backend
==================

Choose by the failure you can tolerate
--------------------------------------

These recommendations are an engineering assessment of the implementation and
the deployment assumptions below, not performance benchmarks or a guarantee
of safety for every configuration.

1. **One database owns the invariant:** first use its native transaction or
   conditional update. If long-running coordination is also needed and you
   operate PostgreSQL, consider ``PostgresBackend`` with resource-side fencing.
2. **Duplicate work is acceptable:** ``RedisBackend`` is a straightforward
   choice when Redis is already available. Explicitly decide whether losing a
   lock or regressing a cache value during failover is tolerable.
3. **Workers run on one host:** choose ``SQLiteBackend`` for SQL-managed lease
   state or ``FileBackend`` for directory-based coordination. Both need stable
   local storage and correct destination enforcement for consequential writes.
4. **You already operate Redis Cluster:** use ``RedisClusterBackend`` for slot
   routing and optional replication acknowledgement. Sharding helps distribute
   different lock names; it does not remove contention on one hot name.
5. **You specifically need a Redlock quorum:** use ``RedlockBackend`` only after
   assessing its timing assumptions and the separate fencing authority. More
   Redis nodes do not by themselves provide a consensus-backed token history.
6. **You require coordination backed by a consensus service:** this checkout
   has no etcd or ZooKeeper backend. Select another integration or implement
   and verify an adapter; do not treat a planned backend as available.

Implemented capabilities
-------------------------

All built-in backends support exclusive and shared modes, ownership-checked
mutations, positive TTLs, and backend-issued epochs. Shared mode allows multiple
readers; it does not authorize concurrent mutation of one fenced resource.

.. list-table:: Backend comparison
   :header-rows: 1
   :widths: 18 16 24 42

   * - Backend
     - API
     - Token storage
     - Main deployment constraint
   * - Redis
     - Sync, async
     - Persistent Redis counter per lock
     - Counter eviction, rollback, and asynchronous failover can break history
   * - Redis Cluster
     - Sync
     - Counter on the lock's shard
     - ``WAIT`` improves replication confidence, not strong consistency
   * - Redlock
     - Sync
     - Required shared ``FencingTokenProvider``
     - Quorum timing assumptions plus authority availability and durability
   * - PostgreSQL
     - Sync, async
     - ``mithril_fencing`` table
     - Preserve committed history through failover; fence business writes
   * - SQLite
     - Sync, async
     - ``mithril_fencing`` in a local database
     - One host; independent connections; database-wide write contention
   * - File
     - Sync
     - Adjacent ``.fence`` files
     - One host; persistent trusted directory; leases can expire while alive

Redis
-----

Import ``RedisBackend`` from ``mithril.backends`` or ``AsyncRedisBackend`` from
``mithril.backends.aio``. Both accept ``client``, ``namespace=""``, and
``signal_ttl=5.0``. The async version expects ``redis.asyncio.Redis``.

Acquisition and fencing issuance run in a Lua script. Release and renewal also
use scripts and check the owner. Release signals waiters through a short-lived
list; the facade uses ``BLPOP`` to wait instead of only sleeping. Signalling
does not promise FIFO fairness, broadcast wakeups, or a fixed wakeup latency.

Fencing counters persist beyond the lease TTL. Keep them out of disposable
cache eviction policies and cleanup jobs. An expired lease can be removed; its
ordering history must survive. Lua atomicity prevents command interleaving but
does not establish disk durability or lossless replica promotion.

Redis documents that asynchronous replication can lose a lock during promotion.
Its `locking guide
<https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/>`_
also describes the timing assumptions behind Redlock. If your invariant relies
on an unbroken counter history, assess that history through crashes, failover,
and backup restoration, not just ordinary acquire/release tests.

Redis Cluster
-------------

.. code-block:: python

   from redis import RedisCluster
   from mithril.backends import RedisClusterBackend

   client = RedisCluster(
       host="redis-cluster", port=6379,
       socket_timeout=1, socket_connect_timeout=1,
   )
   backend = RedisClusterBackend(
       client,
       namespace="jobs",
       safety_level="safe",
       wait_replicas=1,
       wait_timeout=0.2,
   )

All keys for one logical lock share an escaped hash tag so the scripts operate
within one slot. The available replication policies are:

.. list-table:: Replication policy
   :header-rows: 1
   :widths: 20 80

   * - Setting
     - Current behavior
   * - ``fast`` (default)
     - No ``WAIT`` acknowledgement step
   * - ``safe``
     - Requires an explicit positive ``wait_replicas``; waits for that count
   * - ``paranoid``
     - Requires an explicit positive ``wait_replicas``; waits for the greater
       of that count and the primary's reported connected replica count

In ``safe`` and ``paranoid`` modes, acquisition and renewal pin the mutation
and ``WAIT`` to the same primary connection. Insufficient acknowledgements
cause owner-checked cleanup and refusal of that lease. Explicit acquisition
``MOVED``/``ASK`` errors on this path trigger cleanup and a failed attempt so
the facade can retry; a redirection is not accepted as proof of ownership.

``paranoid`` does not discover an authoritative inventory of all replicas:
disconnected replicas may be absent from the reported connected count.
Configure ``wait_replicas`` according to the intended topology and availability
tradeoff.

Redis states that `WAIT <https://redis.io/docs/latest/commands/wait/>`_ does not
make Redis strongly consistent and that acknowledged writes may still be lost
on failover. Its acknowledgements concern preceding writes on that connection.
The Mithril policy names describe replication checks, not correctness grades.

Redlock
-------

Mithril attempts acquisition across independent Redis instances, requires a
strict majority by default, and issues the public token from a shared authority
after reaching quorum. Release attempts every instance, including those that
may have accepted an acquisition whose response was lost.

The remaining validity is bounded by:

.. code-block:: text

   remaining = TTL - elapsed - TTL * clock_drift_factor - 0.002 seconds

The elapsed time includes the fencing authority request. An acquisition that
finishes outside this window is discarded. This is a timing model, not a proof
that arbitrary pauses or partitions cannot produce overlapping holders.

.. code-block:: python

   from redis import Redis
   from mithril import RedisFencingTokenProvider
   from mithril.backends import RedlockBackend, RedlockConfig

   # Illustrative deployment names. Provision these services separately.
   clients = [
       Redis(host=host, socket_timeout=0.05, socket_connect_timeout=0.05)
       for host in ("redis-a", "redis-b", "redis-c")
   ]
   authority_client = Redis(
       host="fencing-authority", socket_timeout=0.1, socket_connect_timeout=0.1,
   )
   provider = RedisFencingTokenProvider(authority_client, namespace="jobs")
   backend = RedlockBackend(
       RedlockConfig(instances=clients, fencing_provider=provider, retry_count=0),
       namespace="jobs",
   )
   # Use Lock(..., backend=backend), then call backend.close() at shutdown.
   # Close the caller-owned clients and authority_client separately.

The example's Redis authority is conditional on its deployment preserving all
issued counters. It is not automatically a durable consensus authority.
Supply another implementation of the public ``FencingTokenProvider`` protocol
when your failure model demands stronger issuance guarantees. An in-memory
counter cannot coordinate separate processes or survive a restart.

All participants must share the same authority, namespace, independent server
set, and quorum policy. Distinct client objects pointing at one Redis server
are not independent instances. The constructor checks client identity, not
physical failure-domain independence.

Taking the maximum of independent Redis node counters is insufficient. For
example, a grant from nodes A/B/C could observe counters 100/1/1 and return 100;
a later grant from B/C/D could observe 2/2/1 and return 2. Intersecting quorums
alone do not propagate the previous maximum. Mithril therefore requires the
shared authority instead of deriving public tokens from node maxima.

The authority is an additional availability dependency: a Redis majority is
insufficient if it cannot issue a token. Its ordering is also separate from
quorum ownership; the authority API does not atomically verify Redis ownership.
Use downstream fencing and validate the complete protocol's timing assumptions.

``RedlockConfig`` defaults are ``retry_count=3`` (four attempts),
``retry_delay=0.2``, ``clock_drift_factor=0.01``, and
``per_instance_timeout=0.05``. These retries occur inside a facade attempt;
the example disables internal retries to leave retry scheduling to ``Lock``.
Mithril creates bounded-timeout pools for concrete synchronous Redis clients.
Custom client implementations must enforce their own I/O limits.

PostgreSQL
----------

.. code-block:: python

   import os
   from sqlalchemy import create_engine
   from mithril.backends import PostgresBackend

   engine = create_engine(
       os.environ["MITHRIL_DATABASE_URL"],  # postgresql+psycopg2://...
       pool_timeout=2,
       connect_args={
           "connect_timeout": 2,
           "options": "-c statement_timeout=2000 -c lock_timeout=1000",
       },
   )
   backend = PostgresBackend(engine)
   # Reuse the backend; engine.dispose() after all leases have been released.

The constructor creates ``mithril_locks``, ``mithril_readers``, and
``mithril_fencing`` if absent. Arrange initialization permissions before serving
traffic. These tables hold leases and counters; the implementation does not
use PostgreSQL advisory locks.

The backend uses serializable transactions for lock operations and persistent
per-name counters. Acquisition transaction conflicts become failed attempts;
other supported operation conflicts have a bounded retry path. The lease is
not a database row lock held for the entire Python context.

The backend's transaction ends independently of any business transaction. Even
if both use the same database, an ordinary update inside ``with Lock(...)``
does not check the fencing token. Add the resource predicate from :doc:`fencing`.

PostgreSQL documents potential loss of committed transactions when using
asynchronous standby promotion. Synchronous replication changes the durability
and availability tradeoff; the failover procedure must select an appropriate
standby and exclude the old primary. See `PostgreSQL replication
<https://www.postgresql.org/docs/current/warm-standby.html>`_. Durable token
history is a deployment property, not a consequence of choosing the Python
backend class.

Async database initialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Async database backends require an explicit initialization call:

.. code-block:: python

   import asyncio
   import os
   from sqlalchemy.ext.asyncio import create_async_engine
   from mithril.aio import AsyncLock
   from mithril.backends.aio import AsyncPostgresBackend

   async def main():
       engine = create_async_engine(
           os.environ["MITHRIL_ASYNC_DATABASE_URL"],  # postgresql+asyncpg://...
           pool_timeout=2,
           connect_args={"timeout": 2, "command_timeout": 2},
       )
       try:
           backend = AsyncPostgresBackend(engine)
           await backend.initialize()
           async with AsyncLock("report-42", backend=backend) as token:
               print(token.fencing_token)
       finally:
           await engine.dispose()

   asyncio.run(main())

SQLite
------

.. code-block:: python

   from sqlalchemy import create_engine
   from mithril.backends import SQLiteBackend

   engine = create_engine("sqlite:///mithril.sqlite3", connect_args={"timeout": 1})
   backend = SQLiteBackend(engine)

Use one file-backed database shared by processes on the same host. The backend
enables WAL, serializes mutation transactions with ``BEGIN IMMEDIATE``, and
persists the fence table. ``StaticPool`` and ``SingletonThreadPool`` are
rejected because concurrent leases require independent connections. Do not use
an in-memory database for a persistent coordination service.

SQLite WAL requires processes to be on one host and is unsuitable for a shared
database over a network filesystem. See `SQLite WAL
<https://www.sqlite.org/wal.html>`_. Expect write contention across different
lock names because SQLite serializes writers; test this with your workload.

For async use, create an engine with ``sqlite+aiosqlite:///mithril.sqlite3``,
construct ``AsyncSQLiteBackend``, and await ``initialize()``. Mithril's renewal
uses an asyncio task, but aiosqlite itself uses a worker thread per connection.
An async SQLite application is therefore not thread-free. See
`SQLAlchemy's aiosqlite documentation
<https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#aiosqlite>`_.

File leases
-----------

.. code-block:: python

   from mithril.backends import FileBackend

   backend = FileBackend("/var/lib/my-app/leases", namespace="reports")

Provision a trusted, persistent local directory writable by the participating
processes. Names and namespaces must be nonempty path components without
slashes or backslashes. Choose names portable to your operating system.

Mithril uses OS locks for metadata synchronization and per-owner crash
detection, and atomic replacement for metadata and counter files. The code has
POSIX and Windows paths; validate the intended platform and filesystem in your
deployment. Parent-directory integrity remains an operator responsibility.

This backend implements **leases**, not an indefinitely held OS mutex around
your business operation. A replacement can acquire after TTL expiry while the
old process remains alive. Fencing is still necessary when stale effects
matter. All facades currently require a positive TTL, including file leases.

Keep ``.fence`` counters permanently for active resource identities. Do not
remove live guard files: replacing a mutex file can make processes synchronize
on different inodes. The directory must not be an ephemeral per-container
volume if those containers are supposed to coordinate with each other.
