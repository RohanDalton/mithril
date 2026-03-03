import pytest

from mithril.backends._redis_cluster import RedisClusterBackend
from mithril.exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


class FakeScript:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def __call__(self, *, keys, args):
        self.calls.append({"keys": keys, "args": args})
        return self.result


class FakeClient:
    def __init__(self, acquire_result=(1, 1)):
        self.acquire_script = FakeScript(acquire_result)
        self.release_script = FakeScript(0)
        self.renew_script = FakeScript(0)
        self.wait_calls = []
        self.blpop_calls = []

    def register_script(self, _script):
        if _script.strip().startswith("local acquired"):
            return self.acquire_script
        if "LPUSH" in _script:
            return self.release_script
        return self.renew_script

    def execute_command(self, *args):
        self.wait_calls.append(args)
        return 1

    def blpop(self, key, timeout):
        self.blpop_calls.append((key, timeout))
        return None

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


if __name__ == "__main__":
    pass
