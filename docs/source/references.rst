Sources and further reading
============================

Designing Data-Intensive Applications
-------------------------------------

Martin Kleppmann, *Designing Data-Intensive Applications*, first edition,
O'Reilly, 2017. The author's `book website <https://dataintensive.net/>`_
provides publication information and links to legitimate editions.

Use the discussions of replication, transactions, unreliable networks and
clocks, process pauses, and coordination to examine your application's failure
model. This guide applies those themes to Mithril's source and to original
application examples. It does not reproduce book passages or imply author
endorsement of Mithril.

The second edition is by Martin Kleppmann and Chris Riccomini. The publisher's
`table of contents
<https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/>`_
locates Replication in Chapter 6, Transactions in Chapter 8, The Trouble with
Distributed Systems in Chapter 9, and Consistency and Consensus in Chapter 10.
Chapter numbering differs between editions; use section names when following
references from other articles.

.. list-table:: Reading questions for a Mithril design review
   :header-rows: 1
   :widths: 30 70

   * - Topic
     - Question to answer in your system
   * - Replication and recovery
     - Can promotion or restore erase an issued token or accepted generation?
   * - Transaction isolation
     - Which operation atomically checks the fence and applies the effect?
   * - Network delays and process pauses
     - What can a delayed worker still do after its local lease ends?
   * - Ordering and coordination
     - Which authority orders generations for this particular resource?
   * - End-to-end correctness
     - Do all write paths and downstream systems enforce the same invariant?

Distributed locking analysis
------------------------------

Martin Kleppmann, `How to do distributed locking
<https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>`_,
8 February 2016. This public primary source explains the distinction between
efficiency and correctness, stale lease holders, resource-side fencing, and
his critique of Redlock's timing assumptions. Read it alongside the
`Redis locking guide
<https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/>`_,
which presents the Redis algorithm and its assumptions. Their different
positions should not be collapsed into a claim that a quorum automatically
solves correctness.

Backend primary documentation
-------------------------------

* `Redis WAIT <https://redis.io/docs/latest/commands/wait/>`_: what replica
  acknowledgements mean, their connection scope, and their consistency limits.
* `PostgreSQL transaction isolation
  <https://www.postgresql.org/docs/current/transaction-iso.html>`_: conditional
  updates, isolation guarantees, and serialization failures.
* `PostgreSQL standby servers and replication
  <https://www.postgresql.org/docs/current/warm-standby.html>`_: asynchronous
  loss windows and synchronous replication tradeoffs.
* `SQLite write-ahead logging <https://www.sqlite.org/wal.html>`_: writer
  concurrency and the same-host requirement.
* `SQLAlchemy aiosqlite
  <https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#aiosqlite>`_: async
  integration, connection pooling, and the driver's background threads.

Implementation evidence
-------------------------

Behavior in this guide was checked against the repository's implementation:

* ``mithril/lock.py`` and ``mithril/aio/lock.py``: lifecycle and local deadlines.
* ``mithril/_types.py``: validation, tokens, modes, and backend results.
* ``mithril/_fencing.py``: providers and the limits of the private guard helper.
* ``mithril/backends/_redis_scripts.py``: Redis ownership mutations.
* ``mithril/backends/_redis_cluster.py``: same-connection replication checks.
* ``mithril/backends/_redlock.py``: quorum attempts and external token issuance.
* ``mithril/backends/_database.py``, ``_postgres.py``, and ``_sqlite.py``:
  transaction and counter storage behavior.
* ``mithril/backends/_file.py`` and ``mithril/_file_io.py``: local leases and
  persistent counters.
* ``mithril/_observability.py``: logging, callbacks, and metric instruments.

Backend recommendations are this guide's assessment of those mechanisms and
their stated assumptions. A local test or a documentation build is not evidence
that a deployment survives arbitrary partitions, failovers, or data loss.
