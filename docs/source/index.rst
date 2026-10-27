Mithril documentation
=====================

Mithril coordinates Python workers using leases, ownership checks, fencing
tokens, and optional automatic renewal. It provides synchronous and asynchronous
APIs with Redis, Redis Cluster, Redlock, PostgreSQL, SQLite, and local file
backends. The package is currently classified as **alpha**.

**Protect the effect, not just the lock.** A worker can continue running after
its lease expires. For work where a stale write would violate correctness, the
destination must atomically enforce a fencing token or another suitable
concurrency constraint. Issuing a token alone does not enforce it.

This guide applies the failure-oriented approach of Martin Kleppmann's
*Designing Data-Intensive Applications* to Mithril. It is original documentation
of this implementation, with application examples and explicit deployment
assumptions; it is not an adaptation of the book's text. See :doc:`references`
for a reading guide and primary sources.

Where to start
--------------

* :doc:`quickstart`: installation, synchronous and asynchronous usage.
* :doc:`concepts`: the correctness boundary and alternatives to locking.
* :doc:`fencing`: when tokens are necessary and how to enforce them.
* :doc:`backends`: choose a backend for your workload and failure model.
* :doc:`operations`: renewal, failure recovery, monitoring, and testing.
* :doc:`api`: configuration, lifecycle, and API reference.

The documentation describes the source in this checkout. Etcd, Kubernetes
Lease, and Django integration are design goals, not implemented backends or
integrations in this version. No hosted infrastructure is provisioned by Mithril.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   quickstart
   concepts
   fencing
   backends
   operations
   api
   references

Building this documentation
---------------------------

From a checkout with its development environment installed:

.. code-block:: sh

   pixi run -e dev python -m sphinx -W --keep-going -b html docs/source docs/build/html

Open ``docs/build/html/index.html``. With another environment, install the
project and Sphinx, then run the same ``python -m sphinx`` command.
