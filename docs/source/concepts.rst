Designing for failures
========================

Start with an invariant
-----------------------

Before choosing a backend, describe the condition that must remain true. For
example: an older report worker must never replace a publication from a newer
worker; a reservation must not make available inventory negative; or a payment
request must be applied at most once.

These are different requirements. A fenced publication can solve the first.
A conditional database update can solve the second. The third needs durable
deduplication at the payment boundary. A distributed lock can reduce concurrent
attempts in all three cases, but it does not implement all three invariants.

Kleppmann distinguishes locks that save duplicate work from locks on which
correctness depends. He explains why pauses and delayed messages can outlive a
lease, and why a resource needs an increasing token to reject obsolete holders.
These are the starting points of this guide. See his
`distributed locking analysis <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>`_.

Apply that distinction to your own workload:

.. list-table:: What happens if two workers proceed?
   :header-rows: 1
   :widths: 30 35 35

   * - Workload
     - Consequence
     - Required boundary
   * - Recompute a disposable cache entry
     - Extra CPU; possibly an older cache value
     - Decide whether both costs are acceptable; version the value if needed
   * - Publish a persistent report
     - An obsolete worker can replace a newer result
     - Atomic fenced publication or equivalent version check
   * - Reserve stock
     - Over-allocation
     - Transactional inventory constraint; locking may reduce contention
   * - Send a payment request
     - A duplicate external effect
     - Provider-enforced idempotency and reconciliation
   * - Elect a job coordinator
     - Two coordinators can issue commands
     - Fence commands at every recipient that must exclude an old leader

Mithril's two boundaries
--------------------------

At the **coordination boundary**, the backend grants ownership, records a TTL,
issues a token, and checks ownership on release and renewal. At the
**resource boundary**, application code must enforce what the token permits.
Mithril does not intercept your SQL, object writes, messages, or HTTP calls.

The implementation uses atomic backend operations for ownership transitions:
Redis scripts, database transactions, or OS-protected file metadata. That
atomicity concerns the lock record. It does not extend automatically to a
separate operation performed inside a Python context manager.

Mithril's database backends also use independent transactions. Passing an
SQLAlchemy connection does not enlist lock acquisition in the transaction
that updates your business tables.

What each identifier means
--------------------------

``owner_id``
   A random identifier for an acquisition, used to authorize release and
   renewal. Random identifiers do not establish acquisition order.

``fencing_token``
   A positive, backend-issued ordering value for the lock's identity. Its
   guarantee depends on preserving the issuer's history. Compare tokens only
   within the same resource's authority and naming scheme.

``epoch``
   The built-in backends currently set this to the same persistent sequence
   value as the fencing token. It is not a separately managed disaster-recovery
   generation, and does not repair a counter reset.

``acquired_at`` and ``expire_at``
   Local monotonic-clock values. They help the facade conservatively reject
   expired ownership. They are not UTC timestamps or portable deadlines to
   compare across machines.

Safety, progress, and time
--------------------------

For a fenced report, the safety property is that a token below the destination's
accepted maximum cannot modify the report. The progress requirement is that a
healthy replacement worker can eventually acquire and publish after a failure.
Lease expiry supports progress, while resource-side validation enforces safety.

Mithril starts its local validity window before backend acquisition and renewal
I/O. Slow responses consume the useful lease interval. Redlock subtracts an
additional drift allowance. These measures reduce accidental use of an expired
lease, but an already-issued application request can still arrive late.

Reading ``lock.state``, calling ``get_owner()``, or checking ``time_remaining``
is an observation. None makes a later write atomic with the observation.
Renewal failure marks the facade expired; it does not terminate arbitrary user
code. Treat local state as a reason to stop work, never as the only proof that
a later write is authorized.

A backend timeout leaves an uncertain outcome: the mutation might have reached
the server even though its reply did not reach the caller. Mithril attempts
owner-checked cleanup on acquisition failures; cleanup itself can fail. TTLs
bound abandoned leases, and application-level idempotency resolves uncertain
business outcomes.

Ordering is stronger than uniqueness
-------------------------------------

A UUID can distinguish two holders but cannot tell the destination which
holder superseded the other. Independent counters or wall-clock timestamps
can similarly produce values that do not follow the intended ownership order.
The fencing authority must serialize issuance for a resource and preserve that
order across the failures the application claims to tolerate.

For a handoff protocol, decide where a new generation becomes authoritative.
An atomic counter is an ordered issuance point; a destination's generation
registration is an enforcement point. They are different operations unless
the backend and application explicitly combine them. In particular, allocating
a fresh token after performing stale work does not make that work current.

Replication, serializable transactions, and consensus address different parts
of this problem. Transaction isolation can protect local lock-table changes;
replication can retain copies; a coordination protocol determines which history
survives competing leaders. Review the complete path rather than assuming that
one of these properties supplies all the others. Mithril's backend comparison
states the particular mechanisms it actually uses.

Prefer a narrower primitive when it expresses the invariant
-----------------------------------------------------------

If all relevant data is in one database, first consider a constraint,
transaction, row lock, or conditional update. For example, the following
application SQL enforces a stock invariant directly:

.. code-block:: sql

   UPDATE inventory
   SET available = available - :quantity
   WHERE sku = :sku AND available >= :quantity
   RETURNING available;

Validate that ``quantity`` is positive and treat no returned row as refusal.
No Mithril lease is required for this one-statement invariant. Multi-row
invariants require an appropriate transaction and isolation level.
PostgreSQL's `isolation documentation
<https://www.postgresql.org/docs/current/transaction-iso.html>`_ describes the
guarantees and retry requirements of its isolation levels.

An optimistic version comparison is useful when a worker computes from a
previously read value. A fencing token orders ownership; a record version
checks that the input value has not changed. Some workflows need both.

Use Mithril when coordination spans work outside one short transaction, such
as expensive computation or scheduling across processes, and keep the final
invariant at a boundary that can enforce it.
