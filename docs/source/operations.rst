Operating Mithril
=================

Choose separate time budgets
-----------------------------

Treat the following as independent controls:

* **Acquisition timeout:** how long the facade schedules retries under
  contention. It cannot interrupt a synchronous driver call already running.
* **Backend timeouts:** pool checkout, connection establishment, command
  execution, and driver retries. Configure these on the supplied client or
  engine; Mithril's ``timeout`` does not configure them.
* **Lease TTL:** how long ownership remains valid without renewal. Longer TTLs
  slow takeover after an abandoned lease; shorter TTLs increase sensitivity to
  latency and scheduling delays.
* **Work deadline:** the application's budget for computation, publication,
  retries, and cleanup. Mithril does not enforce a deadline on the context body.

Choose TTL and renewal headroom from observed latency and scheduling behavior.
No finite TTL makes an unbounded pause impossible. Keep correctness at the
resource boundary even when operational measurements look comfortable.

Renewal details
---------------

Automatic renewal is opt-in. ``renewal_interval=None`` selects ``ttl * 2/3``;
the renewers apply ten percent jitter. A custom interval must satisfy
``renewal_interval * 1.1 < ttl``. Leave additional time for the backend call.

Manual renewal uses ``lock.renew(ttl=...)`` or
``await lock.renew(ttl=...)``. It refreshes a live lease; it cannot resurrect
one that has expired. Changing the TTL for a manual renewal does not change
the constructor's configured TTL or the background interval. Subsequent
automatic renewals continue to use the original TTL. A shortened manual TTL
must still exceed the existing automatic interval including jitter.

Each successful renewal replaces the immutable ``lock.token`` snapshot with
one containing a refreshed expiry. A token saved from ``with lock as token``
retains its original expiry timestamp, even though its fence value remains the
correct generation. Inspect ``lock.time_remaining`` for current local timing.

The sync renewal thread uses a weak reference to the facade and has a bounded
join in its stop method. That join bound does not bound all of ``release()``:
release can wait for another operation and can perform backend I/O. Async
renewal is an asyncio task; blocking the event loop prevents timely renewal.

On renewal failure, the facade transitions to ``EXPIRED``. Application code
must stop initiating work, preserve enough context to reconcile already-issued
effects, and let destination checks reject obsolete requests. Hooks report
failure but do not forcibly cancel the application.

Exceptions and uncertain outcomes
----------------------------------

.. list-table:: Responding to failures
   :header-rows: 1
   :widths: 27 33 40

   * - Observation
     - Meaning
     - Application response
   * - ``LockTimeoutError`` on acquire
     - No usable lease returned; contention or validity budget exhausted
     - Back off, reschedule, or report contention; do not perform protected work
   * - Backend/driver error on acquire
     - Ownership outcome may be uncertain
     - Do not proceed; allow owner-checked cleanup and TTL recovery
   * - ``RenewalFailedError`` or ``LockExpiredError``
     - The facade can no longer use this lease
     - Stop new effects; reconcile pending ones; create a new facade for a new attempt
   * - ``NotAcquiredError`` on release
     - No usable ownership or backend refused this owner
     - Do not delete the lock key manually; inspect the operation's outcome
   * - Release I/O error after work
     - Business work may have committed; cleanup is uncertain
     - Reconcile by operation ID before retrying business work
   * - Destination refuses token
     - Obsolete/used generation, or another failed application predicate
     - Discard stale results or re-read under new ownership

The exception hierarchy is documented in :doc:`api`. Current backends may
propagate Redis, SQLAlchemy, or OS exceptions directly; they are not all
normalized into ``BackendError``. Handle the relevant driver exceptions at
your application boundary as well. Never interpret a communication error as
proof that a remote mutation did not happen.

On normal context exit, release errors propagate. If the body already raised
an exception, the context manager attempts cleanup while preserving that body
exception. Logs and destination state can therefore be needed to investigate
cleanup failure.

Cancellation and shutdown
-------------------------

Async release shields owner-checked cleanup from caller cancellation and then
propagates cancellation. Bounded backend I/O is necessary for cleanup to finish
promptly. Cancellation cannot retract a request already accepted by a remote
service.

During a planned shutdown, stop accepting jobs, finish or cancel active work,
release leases, and only then close clients and engines. For async applications,
do this before closing the event loop. Exit handlers are best effort and do
not run after every kind of process termination. TTLs remain necessary.

For Redlock, ``backend.close()`` closes the private bounded-timeout pools it
created. The caller still owns the supplied clients and fencing authority.
Other backends generally use caller-managed clients or engines.

Naming is part of the protocol
-------------------------------

``Lock(name="daily", namespace="reports", ...)`` passes the full logical name
``reports:daily`` to the backend. The lock-level namespace is a string prefix;
choose an unambiguous naming convention. For example, namespace ``a`` with
name ``b:c`` collides with namespace ``a:b`` and name ``c``.

Redis and file backends have an additional backend namespace. SQL backends use
the full logical name in their tables. ``token.lock_name`` includes the facade
namespace, but does not include an additional backend namespace. When using it
as an application resource ID, ensure that different authorities cannot
silently target the same resource.

Renaming a lock, changing a namespace, moving to a new database, or changing
the Redlock authority creates a new counter identity unless explicitly
migrated. It is not just a cosmetic configuration change.

Recovery without counter rollback
----------------------------------

Keep two histories: the issuer's highest issued token and the destination's
accepted generation. Deleting either can invalidate the protocol. Restoring
an old issuer snapshot can also strand new work below the destination's
maximum, even when the destination continues refusing stale writes correctly.

A recovery procedure should:

1. Stop issuance and protected writes, and exclude old holders and in-flight
   requests from the recovered generation's write path.
2. Recover a counter floor at least as high as every previously issued token
   that can still arrive, and every destination's accepted maximum. The
   destination alone may not know tokens that were issued but never used.
3. Restore or migrate issuer and destination state without reducing that
   floor, and ensure the next token is strictly larger.
4. Verify the restored state and only then resume coordinated writers.

If the floor cannot be established, use an explicitly designed new resource
generation with destination-enforced rejection of old-generation requests.
Mithril's numeric ``epoch`` is not a built-in recovery-generation protocol.
Do not reset both counters to zero and assume waiting one TTL discards delayed
application messages.

For existing deployments, read the repository's
:download:`upgrade notes <../../mithril/UPGRADING.md>`
alongside the version being installed. In particular, older Redis reader
metadata and file metadata formats require a drained migration, while fence
counters must be preserved. The same notes are in ``mithril/UPGRADING.md``
in this checkout.

Logging and metrics
-------------------

Mithril uses structlog and OpenTelemetry instruments. Configure your logging
pipeline and metrics provider/exporter in the application. The OpenTelemetry
API dependency alone does not install an exporter. ``configure_logging``
passes arguments to ``structlog.configure`` and affects that global pipeline.

Per-lock or global hooks can observe lifecycle events:

.. code-block:: python

   from mithril import LockHooks, set_hooks

   def on_expired(event):
       # Keep this callback fast; enqueue diagnostics for other code to process.
       print("lease lost", event.lock_name, event.fencing_token)

   set_hooks(LockHooks(on_expired=on_expired))
   # Alternatively: Lock(..., hooks=LockHooks(on_expired=on_expired))

Hooks are ordinary synchronous callbacks even on the async path. They may run
from the sync renewal thread, so shared callback state must be thread-safe.
Do not perform blocking network calls, re-enter lock operations, or place
correctness enforcement in hooks. Observability exceptions are caught; a hook
failure may also prevent recording that event's metric.

Available instruments are:

* ``mithril.lock.acquire.total``
* ``mithril.lock.release.total``
* ``mithril.lock.renew.total``
* ``mithril.lock.timeout.total``
* ``mithril.lock.expired.total``
* ``mithril.lock.acquire.latency`` (seconds)

Metric attributes include ``lock_name``, ``mode``, ``backend``, and ``namespace``.
Unbounded per-resource names can create high metric cardinality. Apply exporter
or SDK aggregation policies suitable for your application. Keep sensitive
values out of lock names and logs.

``debug=True`` records the acquisition stack, available through
``lock.acquire_stack``. Correlate that stack with owner ID, fence, acquisition
latency, renewal failures, and destination refusals. ``state`` and
``time_remaining`` are local observations, not remote ownership checks.

Test the invariant at the destination
--------------------------------------

A concurrency test that observes only lock acquisition is insufficient for
an application that relies on fenced writes. Include these scenarios in a
deployment-specific test suite:

.. list-table:: Failure experiments
   :header-rows: 1
   :widths: 48 52

   * - Injected fault
     - Required observation
   * - Pause A beyond TTL; let B acquire and commit; resume A
     - A's business write is rejected and B's result remains intact
   * - Delay A's already-issued write until after B's commit
     - Destination ordering holds even though A checked its lease earlier
   * - Lose acquisition or publication replies
     - No work without a returned usable lease; business retries reconcile by ID
   * - Disconnect holder from lock service while destination stays reachable
     - Renewal loss stops new work; stale requests cannot overwrite newer effects
   * - Fail over or restore issuer and destination
     - Counters and accepted maxima do not regress or get reused
   * - Retry one operation with the same token
     - Equality/deduplication behavior matches the documented application contract
   * - Cancel an async task during acquisition, work, and release
     - Cleanup is attempted; expiry recovers abandoned ownership
   * - Crash a file-lock process or contend on SQLite from separate processes
     - Replacement progress and persistent token ordering match expectations

Test with the actual server, persistence settings, drivers, filesystem, and
failover mechanism used in production. The included local example validates
one SQL predicate; it does not certify a deployment's failure model.
