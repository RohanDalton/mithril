Getting started
===============

Installation
------------

The distribution is ``mithril-locks``; imports use ``mithril``. The package
declares Python 3.9 or newer.

.. code-block:: sh

   python -m pip install mithril-locks

Redis, SQLAlchemy, structlog, and the OpenTelemetry API are dependencies.
Database drivers can be installed with extras:

.. code-block:: sh

   python -m pip install 'mithril-locks[postgres]'
   python -m pip install 'mithril-locks[async-postgres]'
   python -m pip install 'mithril-locks[async-sqlite]'

These select psycopg2, asyncpg, and aiosqlite respectively. Synchronous SQLite
uses Python's sqlite3 driver; the backend's SQL requires SQLite 3.35 or newer
for `RETURNING <https://www.sqlite.org/lang_returning.html>`_. Installing Mithril
does not start a Redis or PostgreSQL server.

A Redis lease
-------------

This example connects to a local Redis service and prints the acquisition's
token. It performs no protected application write.

.. code-block:: python

   from redis import Redis
   from mithril import Lock
   from mithril.backends import RedisBackend

   with Redis.from_url(
       "redis://localhost:6379/0",
       socket_timeout=1,
       socket_connect_timeout=1,
   ) as client:
       backend = RedisBackend(client, namespace="mithril-demo")
       with Lock("report-42", backend=backend, ttl=30, timeout=5) as token:
           print(token.lock_name, token.fencing_token)

``with`` returns an immutable ``LockToken`` and releases ownership on exit.
``ttl`` limits the lifetime of each lease; ``timeout`` controls acquisition
retries. They serve different purposes. Backend calls have their own I/O
timeouts and may outlast the retry budget.

All participants must agree on the backend service, database, backend namespace,
and logical lock name. Reuse clients and backends across operations, but create
a new ``Lock`` for each completed lease. A released or expired facade cannot be
acquired again.

For an actual application write, continue to :doc:`fencing`. Redis persistence
and failover assumptions are covered in :doc:`backends`.

Nonblocking acquisition
-----------------------

``blocking=False`` makes one backend attempt. Contention raises
``LockTimeoutError``; it does not return ``False``. The attempt still performs
I/O.

.. code-block:: python

   from mithril import Lock, LockTimeoutError

   def try_report(backend):
       lock = Lock("report-42", backend=backend, ttl=30)
       try:
           token = lock.acquire(blocking=False)
       except LockTimeoutError:
           return None
       try:
           return token.fencing_token  # Demonstration only: no application write.
       finally:
           lock.release()

This catches acquisition contention separately from errors raised by the work
or release. A release failure does not undo work already committed.

Async Redis
-----------

Use an async backend with ``AsyncLock``. Do not pass a synchronous Redis client
or synchronous backend to it.

.. code-block:: python

   import asyncio
   from redis.asyncio import Redis
   from mithril.aio import AsyncLock
   from mithril.backends.aio import AsyncRedisBackend

   async def main():
       async with Redis.from_url(
           "redis://localhost:6379/0",
           socket_timeout=1,
           socket_connect_timeout=1,
       ) as client:
           backend = AsyncRedisBackend(client, namespace="mithril-demo")
           async with AsyncLock("report-42", backend=backend, ttl=30) as token:
               print(token.fencing_token)

   asyncio.run(main())

Automatic renewal
-----------------

Set ``auto_renewal=True`` for work expected to exceed one TTL. The default
renewal interval is two-thirds of the TTL, with up to ten percent jitter in
either direction. The synchronous facade uses a daemon thread; the asynchronous
facade uses an asyncio task.

.. code-block:: python

   lock = Lock("report-42", backend=backend, ttl=30, auto_renewal=True)
   with lock as token:
       print(token.fencing_token, lock.time_remaining)

Renewal extends a live lease without changing its fencing token. It cannot
guarantee progress during a process pause, blocked event loop, or network
outage. The resource must still reject obsolete writes. Failure handling and
manual renewal are explained in :doc:`operations`.

A local example without a server
--------------------------------

The :ref:`fenced-snapshot-example` uses a temporary SQLite database to issue
tokens, accept a newer publication, and reject an obsolete publication. It can
be run directly from the repository and leaves no persistent demo database.
