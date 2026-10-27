Fencing tokens in practice
==========================

When to enforce a token
------------------------

Enforce fencing when an obsolete lease holder could overwrite or otherwise
invalidate work from a later holder. Examples include publication of a derived
artifact, updates to a shared job checkpoint, and commands issued by a replaced
coordinator. Every path that can make the protected change must participate.

A token can be unnecessary when duplicate work and its outputs are explicitly
acceptable, or when a database transaction, version comparison, or receiver's
deduplication rule already enforces the complete invariant. Decide this from
the effect of a failure, rather than from expected task duration.

Mithril always returns a token on successful acquisition. Using it is an
application responsibility: neither a context manager nor automatic renewal
causes the destination to inspect it.

A delayed publication
----------------------

Consider two report workers using the same logical lock:

.. code-block:: text

   Worker A                  Lock service           Report destination
   acquire ----------------> grants token 71
   compute report
   pause ...                 lease 71 expires

   Worker B
   acquire ----------------> grants token 72
   publish(token=72) ------------------------------> store result; max=72

   Worker A resumes
   publish(token=71) ------------------------------> reject: 71 < 72

The important event at the destination is acceptance of 72. Merely issuing 72
at the lock service does not notify the report table. Before the destination
has observed 72, it may still accept 71. Fencing orders accepted effects; it
does not prove that a lease is currently live or that no overlapping computation
exists. If ownership must be established at the destination before reading or
computing, explicitly register the new generation there first.

The destination's contract
---------------------------

A resource needs durable state recording its accepted generation. Define its
operation so that comparison, modification, and advancement of that generation
form one atomic unit. For example, a single SQL statement can compare a fence
column while updating a report row.

The complete contract includes:

1. All acquisitions for the resource use one ordered token history. Tokens
   cannot be reused after a crash, restore, backend migration, or rename.
2. All protected writes supply the token and address the correct resource.
3. The destination performs the comparison and write atomically, with durable
   retention of the accepted generation.
4. Rejected writes stop or restart from newly read application state. They do
   not retry with the predicate removed or an invented larger token.
5. Authentication prevents untrusted callers from forging arbitrarily large
   tokens. A fencing number is not a security credential.

The exact comparison depends on the operation:

.. list-table:: Choosing an acceptance rule
   :header-rows: 1
   :widths: 24 35 41

   * - Rule
     - Appropriate use
     - Consequence
   * - ``incoming > stored``
     - One accepted publication per acquisition
     - Equal-token retries are refused; recover the outcome separately
   * - ``incoming >= stored``
     - Multiple operations by the same generation
     - Repeated or reordered operations within that generation remain possible
   * - ``incoming == active_generation``
     - Explicit generation registration followed by operations
     - Registration must advance monotonically; every write checks equality

For repeated writes, add operation IDs for deduplication and, when necessary,
sequence numbers for order within a generation. Allowing equality without
these rules can apply a retried increment twice. Rejecting equality everywhere
can instead make the second legitimate write of one lease fail.

.. _fenced-snapshot-example:

Executable example: one publication per lease
---------------------------------------------

The example below uses Mithril's SQLite backend and a separate application
table in the same temporary database. Each publication replaces an entire
snapshot supplied by the caller; it is not a read-modify-write computation
over an earlier report value. The predicate and snapshot replacement are one
statement. The unique row key also scopes the fence.

Run it from a checkout with Mithril installed:

.. code-block:: sh

   python docs/examples/fenced_snapshot.py

.. literalinclude:: ../examples/fenced_snapshot.py
   :language: python

The demonstration prints the following lines in addition to Mithril's logs:

.. code-block:: text

   Accepted tokens: 1, 2
   Rejected obsolete token: 1
   Final snapshot: second publication

The final call deliberately reuses A's saved token after its lease ended, to
demonstrate destination enforcement independently of local facade checks.
The same destination predicate rejects A after a TTL takeover once B's larger
token has been accepted. This example tests that predicate, not a distributed
failover deployment.

``StaleTokenError`` is explicitly raised by this application code. Mithril
cannot automatically raise it for an external SQL statement. A zero-row result
may also mean the application resource is missing; this example creates it
first. Production code should define missing-resource behavior separately.

For PostgreSQL, the same conditional ``UPDATE ... RETURNING`` pattern works
with a ``BIGINT`` fence column. Treat serialization failures as retries of the
whole application transaction. Verify replication and recovery assumptions for
both the issuer and destination.

Read-modify-write and multiple resources
----------------------------------------

For computations that depend on existing mutable data, a fresh fencing token
does not prove that the inputs are fresh. One possible protocol is:

1. Acquire the Mithril lease.
2. Atomically advance the resource's active generation to this token, refusing
   tokens below or equal to the previous generation.
3. Read the input and its application version after registration.
4. Compute outside the database transaction if appropriate.
5. Atomically write only if both the active generation still equals this token
   and the input version still matches; advance the application version.

Once step 2 commits, earlier generations cannot modify a resource whose writes
all check the active generation. A worker may fail after registering without
publishing; the recovery protocol must tolerate this. A later holder may
register and invalidate the current worker while it computes.

If one lease protects several rows, use a persistent guard row for that
resource and lock/check it in the same transaction as all affected writes.
Checking one row's fence does not protect a different row. A transaction
covering both is needed for an all-or-nothing multi-row invariant.

Likewise, a request spanning a database and an external service is not made
atomic by sharing a token. Use an outbox, durable workflow, or other recovery
protocol, and require the external destination's own concurrency or idempotency
guarantee.

Why a separate guard call is insufficient
-----------------------------------------

This pattern has a race:

.. code-block:: text

   guard.validate_and_update(name, token)
   # The caller can pause here while another generation passes the guard.
   unrelated_database.write(value)

Mithril contains ``FencingGuard`` and ``RedisFencingGuard`` in the private
``mithril._fencing`` module. The base class is an unimplemented extension point;
the Redis helper atomically advances only a Redis maximum with a strict ``>``
comparison. It does not include a business write. It also uses Lua numeric
conversion for comparisons, so it is unsuitable for exact ordering of arbitrary
64-bit integers beyond the exact range of IEEE-754 doubles. It is not exported
as a complete public resource adapter.

For Redis-resident business data, implement a script that validates and performs
the business mutation together, with exact integer handling. For SQL data,
use a conditional statement or transaction in that database. A separate check
against Redis cannot authorize a later SQL write atomically.

Fencing is not exactly-once execution
-------------------------------------

A lease can expire after a payment service accepted a request but before the
worker received the response. A replacement holder has a higher token, yet
must not repeat the charge under a new logical operation identity. Preserve a
business idempotency key across attempts and reconcile uncertain outcomes.

If a destination cannot validate tokens or an equivalent version/idempotency
condition, adding a token to the request does nothing by itself. Choose a
different protocol or explicitly accept the remaining failure mode.
