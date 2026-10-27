API and lifecycle
=================

Public imports
--------------

.. code-block:: python

   from mithril import Lock, LockConfig, LockMode, LockState, LockToken, LockHooks
   from mithril.aio import AsyncLock
   from mithril.backends import (
       RedisBackend, RedisClusterBackend, RedlockBackend, RedlockConfig,
       PostgresBackend, SQLiteBackend, FileBackend,
   )
   from mithril.backends.aio import (
       AsyncRedisBackend, AsyncPostgresBackend, AsyncSQLiteBackend,
   )

There is no public global ``configure()`` factory in this version. Construct a
backend explicitly and pass it to each facade. ``LockConfig`` represents
configuration and validation; the facade accepts individual keyword arguments,
not a ``config=`` parameter.

Facade options
--------------

``Lock`` and ``AsyncLock`` accept the same constructor options:

.. list-table:: Constructor configuration
   :header-rows: 1
   :widths: 23 19 58

   * - Parameter
     - Default
     - Meaning
   * - ``name``
     - Required
     - Nonempty logical name
   * - ``backend``
     - Required keyword
     - Matching sync or async backend
   * - ``namespace``
     - ``""``
     - Logical name prefix, separated by a colon
   * - ``ttl``
     - ``30.0``
     - Positive finite lease duration in seconds
   * - ``timeout``
     - ``10.0``
     - Nonnegative finite acquisition retry budget
   * - ``retry_interval``
     - ``0.1``
     - Positive base delay for exponential backoff
   * - ``retry_jitter``
     - ``0.05``
     - Nonnegative retry jitter in seconds
   * - ``auto_renewal``
     - ``False``
     - Start a background renewer after acquisition
   * - ``renewal_interval``
     - ``None``
     - Uses ``ttl * 2/3``; explicit interval plus jitter must fit within TTL
   * - ``strict``
     - ``True``
     - Validate mutation methods and sync/async compatibility
   * - ``mode``
     - ``LockMode.EXCLUSIVE``
     - Exclusive holder or shared readers
   * - ``reentrant``
     - ``False``
     - Permit nested acquisitions of this same facade in the same mode
   * - ``debug``
     - ``False``
     - Capture the acquisition call stack
   * - ``hooks``
     - ``None``
     - Per-lock callbacks; otherwise global hooks apply

Durations reject booleans, non-numeric values, infinities, and NaN. Although
``LockConfig`` can represent ``ttl=None`` without automatic renewal, actual
facade acquisition rejects it. There is no public no-TTL lease mode.
``strict=False`` skips interface checks, not ownership or expiry checks, and
does not certify that a custom backend is safe.

Methods and local state
------------------------

``acquire(blocking=True, timeout=None, mode=None)``
   Returns a ``LockToken``. ``timeout=None`` uses the constructor budget;
   ``blocking=False`` selects one attempt. ``mode=None`` uses the constructor
   mode. Contention raises ``LockTimeoutError``. The async method is awaited.

``release()``
   Releases this owner and stops automatic renewal. Releasing an expired
   facade attempts cleanup and raises ``LockExpiredError``. Releasing a
   never-acquired/already-released facade raises ``NotAcquiredError``. A backend
   ownership refusal also raises ``NotAcquiredError`` and marks ownership lost.

``renew(ttl=None)``
   Renews current ownership, using the constructor TTL unless overridden.
   Refused or too-late renewal raises ``RenewalFailedError``; a lease already
   known to be expired raises ``LockExpiredError``. Communication errors can
   propagate from the driver and mark the facade expired.

``token``
   Current immutable token while acquired, otherwise ``None``. A saved token
   remains a Python object after release; that does not authorize further work.

``state``
   Local ``LockState``, with expiry checked against the conservative deadline
   when accessed. Does not contact the backend.

``time_remaining``
   Remaining local lease seconds, or ``None`` when not acquired. Does not
   measure replica state or authorize an application write.

``owner_id``
   The acquisition's random identifier, which can remain available for
   diagnostics after ownership ends.

``acquire_stack``
   A copy of the captured stack when debug mode is enabled, otherwise ``None``.

.. code-block:: text

   UNLOCKED -> ACQUIRING -> ACQUIRED -> RELEASED
                   |           |
                   |           +----> EXPIRED
                   +-> UNLOCKED (ordinary failed acquisition/cleanup)
                   +-> EXPIRED  (lease returned too late or renewal startup fails)

``RELEASED`` and ``EXPIRED`` are terminal states. A failed acquisition that
returns the facade to ``UNLOCKED`` can be retried. Concurrent acquisition
through the same facade is rejected with ``AlreadyAcquiredError``.

Both context manager forms return ``LockToken``, not the facade. Retain the
facade in a separate variable when you need to inspect state or renew manually.

Shared and reentrant use
-------------------------

``LockMode.SHARED`` permits multiple readers and excludes an exclusive holder.
It does not supply a consistent snapshot of application data. Shared holders
receive distinct tokens; using the greatest token as a universal read permission
would incorrectly invalidate other legitimate shared readers. Use shared mode
for read-only work and define application consistency independently.

There is no atomic shared-to-exclusive upgrade. Release and reacquire a new
exclusive facade, then re-read data because it may have changed in the gap.
Fairness and starvation freedom are not guaranteed.

``reentrant=True`` increments a local nesting count for the same facade and
mode, returning the same token. The final matching release ends the lease.
Reentrancy does not acquire a new generation or renew the TTL. It is not scoped
to a thread or task identity: do not share a reentrant facade across independent
workers and expect it to serialize them.

Token fields
------------

.. autoclass:: mithril.LockToken
   :members:

``lock_name`` is the full logical name, ``owner_id`` identifies ownership,
``fencing_token`` and ``epoch`` come from the backend, and ``acquired_at`` and
``expire_at`` use the local monotonic clock. The dataclass is frozen.

Fencing providers
-----------------

``FencingTokenProvider`` is publicly exported from ``mithril`` and defines
``next_token(lock_name) -> int`` and ``current_token(lock_name) -> int``.
Implementations used across processes must persist and serialize issuance.
Gaps are acceptable; reused or decreasing tokens are not.

``RedisFencingTokenProvider(client, namespace="mithril")``
   Uses a per-name Redis counter with no expiry. Redis deployment durability
   still determines whether history survives failure.

``FileFencingTokenProvider(directory, namespace="mithril")``
   Uses local files, OS synchronization, and atomic replacement. Only processes
   sharing the same trusted local directory share the counter.

``current_token`` observes issuance without incrementing. It is not a
resource-side atomic write guard or proof that its token holder is still live.
Generating a token separately from acquisition does not make it a valid
ownership generation. Redlock's explicitly configured provider is used as part
of that backend's protocol; ordinary facades have no ``fencing_provider=``
argument.

Custom backend protocol
------------------------

The protocol is defined in ``mithril._types``. Implement these methods, using
coroutine methods for an async backend:

.. code-block:: text

   acquire(key, owner_id, ttl_seconds, epoch=0, mode=LockMode.EXCLUSIVE)
       -> (fencing_token, epoch) | AcquisitionResult | None
   release(key, owner_id, mode=LockMode.EXCLUSIVE) -> Result
   renew(key, owner_id, ttl_seconds, mode=LockMode.EXCLUSIVE)
       -> Result | RenewalResult
   get_owner(key) -> str | None
   is_locked(key) -> bool

``Result`` is publicly exported with ``SUCCESS``, ``WRONG_OWNER``, and
``NOT_FOUND``. Return ``None`` for acquisition contention. Do not return a bool
for mutation results. ``AcquisitionResult`` and ``RenewalResult`` allow a backend
to report a shorter conservative monotonic expiry; the facade never extends
validity beyond its own pre-call TTL calculation.

``wait_signal(key, max_wait) -> bool`` is an optional sync/async callback used
instead of retry sleep. Bound its I/O and avoid interpreting a zero wait as an
infinite block. ``get_owner`` reports exclusive ownership in the built-in
backends; ``None`` does not prove there are no shared holders.

Custom adapters must atomically verify ownership for mutations, issue durable
ordered tokens, refuse renewal of expired ownership, and implement explicit
failure/cancellation behavior. The built-in ``epoch`` argument is retained for
compatibility but does not override backend-issued epochs.

Exceptions
----------

All the following are exported from ``mithril`` as well as
``mithril.exceptions``. See :doc:`operations` for driver-error handling.

.. automodule:: mithril.exceptions
   :members:
   :show-inheritance:
