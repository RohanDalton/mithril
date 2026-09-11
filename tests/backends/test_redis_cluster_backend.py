from contextlib import contextmanager

import pytest
from redis.exceptions import MovedError

from mithril._types import Result
from mithril.backends import _redis_scripts
from mithril.backends._redis_cluster import RedisClusterBackend
from mithril.exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


class FakeScript:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def __call__(self, *, keys, args=None, client=None):
        self.calls.append({"keys": keys, "args": args, "client": client})
        return self.result


class FakeClient:
    def __init__(self, acquire_result=(1, 1)):
        self.acquire_script = FakeScript(acquire_result)
        self.release_script = FakeScript(0)
        self.renew_script = FakeScript(0)
        self.wait_calls = []
        self.blpop_calls = []

    def register_script(self, _script):
        if _script == _redis_scripts.ACQUIRE:
            return self.acquire_script
        if _script == _redis_scripts.RELEASE:
            return self.release_script
        return self.renew_script

    @contextmanager
    def client(self):
        yield self

    def execute_command(self, *args):
        self.wait_calls.append(args)
        return 1

    def blpop(self, key, timeout):
        self.blpop_calls.append((key, timeout))

    def get(self, _key):
        return None

    def exists(self, _key):
        return 0


def test_cluster_backend_requires_wait_replicas_for_safe() -> None:
    """
    GIVEN: A RedisClusterBackend with safety_level safe
    WHEN: wait_replicas is not provided
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedisClusterBackend(FakeClient(), safety_level="safe")


def test_cluster_backend_waits_for_replicas() -> None:
    """
    GIVEN: A RedisClusterBackend with safety_level safe
    WHEN: acquire succeeds
    THEN: WAIT is issued with configured replicas
    """
    client = FakeClient(acquire_result=(1, 1))
    backend = RedisClusterBackend(client, safety_level="safe", wait_replicas=1)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert result == (1, 1)
    assert client.wait_calls
    assert client.wait_calls[0][0] == "WAIT"


def test_cluster_backend_fast_does_not_wait() -> None:
    """
    GIVEN: A RedisClusterBackend with safety_level fast
    WHEN: acquire succeeds
    THEN: WAIT is not called
    """
    client = FakeClient(acquire_result=(1, 1))
    backend = RedisClusterBackend(client, safety_level="fast")
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert result == (1, 1)
    assert client.wait_calls == []


def test_cluster_backend_uses_hash_slot_tag() -> None:
    """
    GIVEN: A RedisClusterBackend with namespace
    WHEN: acquire is called
    THEN: Keys share the same hash slot tag
    """
    client = FakeClient(acquire_result=(1, 1))
    backend = RedisClusterBackend(client, namespace="ns", safety_level="fast")
    backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    keys = client.acquire_script.calls[0]["keys"]
    assert keys[0].startswith("{ns:lock}:lock")
    assert keys[1].startswith("{ns:lock}:fence")
    assert keys[2].startswith("{ns:lock}:signal")
    assert keys[3].startswith("{ns:lock}:meta")


def test_insufficient_replication_cleans_up_acquisition(monkeypatch):
    """
    GIVEN: A successful lease write without sufficient replica acknowledgments
    WHEN: Safe acquisition checks WAIT
    THEN: It releases the unconfirmed lease and reports failure
    """
    client = FakeClient()
    monkeypatch.setattr(client, "execute_command", lambda *args: 0)
    backend = RedisClusterBackend(client, safety_level="safe", wait_replicas=1)
    assert backend.acquire("lock", "owner", 5) is None
    assert len(client.release_script.calls) == 1


def test_wait_and_script_share_the_pinned_connection(monkeypatch):
    """
    GIVEN: A pool whose dedicated client differs from the parent client
    WHEN: Safe acquisition runs its script and WAIT
    THEN: Both commands use the dedicated connection
    """
    client = FakeClient()
    connection = FakeClient()

    @contextmanager
    def pinned_client():
        yield connection

    monkeypatch.setattr(client, "client", pinned_client)
    backend = RedisClusterBackend(client, safety_level="safe", wait_replicas=1)
    assert backend.acquire("lock", "owner", 5) == (1, 1)
    assert client.acquire_script.calls[0]["client"] is connection
    assert connection.wait_calls
    assert client.wait_calls == []


def test_failed_renewal_replication_releases_lease(monkeypatch):
    """
    GIVEN: A renewed lease whose replication cannot be confirmed
    WHEN: Safe renewal checks WAIT
    THEN: It fails closed and cleans up ownership
    """
    client = FakeClient()
    monkeypatch.setattr(client, "execute_command", lambda *args: 0)
    backend = RedisClusterBackend(client, safety_level="safe", wait_replicas=1)
    assert backend.renew("lock", "owner", 5) == Result.NOT_FOUND
    assert client.release_script.calls


def test_redirected_acquisition_is_not_accepted(monkeypatch):
    """
    GIVEN: The primary redirects a pinned acquisition during slot migration
    WHEN: Safe acquisition encounters MOVED
    THEN: Ownership is cleaned up and acquisition is retried by the facade
    """
    client = FakeClient()
    backend = RedisClusterBackend(client, safety_level="safe", wait_replicas=1)

    def moved(**kwargs):
        raise MovedError("1 127.0.0.1:7001")

    monkeypatch.setattr(backend, "_acquire_script", moved)
    assert backend.acquire("lock", "owner", 5) is None
    assert client.release_script.calls
    assert client.wait_calls == []


if __name__ == "__main__":
    pass
