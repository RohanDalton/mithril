import time

import pytest
from redis import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError

from mithril._fencing import InMemoryFencingTokenProvider
from mithril._types import AcquisitionResult, LockMode, RenewalResult, Result
from mithril.backends import _redis_scripts
from mithril.backends._redlock import RedlockBackend, RedlockConfig
from mithril.exceptions import ConfigurationError

__author__ = "Rohan B. Dalton"


class FakeScript:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def __call__(self, *, keys, args=None, client=None):
        self.calls.append({"keys": keys, "args": args})
        return self.result


class FakeClient:
    def __init__(self, acquire_result=(1, 1), release_result=0, renew_result=0):
        self.acquire_script = FakeScript(acquire_result)
        self.release_script = FakeScript(release_result)
        self.renew_script = FakeScript(renew_result)
        self.get_calls = []
        self.exists_calls = []

    def register_script(self, _script):
        if _script == _redis_scripts.ACQUIRE:
            return self.acquire_script
        if _script == _redis_scripts.RELEASE:
            return self.release_script
        if _script == _redis_scripts.IS_LOCKED:
            return lambda **kwargs: self.exists("lock")
        return self.renew_script

    def get(self, key):
        self.get_calls.append(key)
        return None

    def exists(self, key):
        self.exists_calls.append(key)
        return 0


def test_redlock_config_validates_instances() -> None:
    """
    GIVEN: An empty RedlockConfig
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(fencing_provider=InMemoryFencingTokenProvider(), instances=[])


def test_redlock_acquire_succeeds_with_quorum(monkeypatch) -> None:
    """
    GIVEN: A Redlock backend with quorum clients available
    WHEN: acquire is called
    THEN: The authoritative fencing token is returned
    """
    clients = [
        FakeClient(acquire_result=(1, 1)),
        FakeClient(acquire_result=(3, 1)),
        FakeClient(acquire_result=(-1, -1)),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    monkeypatch.setattr(time, "monotonic", lambda: 0.0)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert isinstance(result, AcquisitionResult)
    assert result.fencing_token == 1
    assert result.epoch == 1


def test_redlock_acquire_releases_on_failure() -> None:
    """
    GIVEN: A Redlock backend without quorum
    WHEN: acquire fails
    THEN: Every attempted instance is released
    """
    clients = [
        FakeClient(acquire_result=(1, 1)),
        FakeClient(acquire_result=(-1, -1)),
        FakeClient(acquire_result=(-1, -1)),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert result is None
    assert all(client.release_script.calls for client in clients)


def test_redlock_renew_requires_quorum() -> None:
    """
    GIVEN: A Redlock backend with partial renew success
    WHEN: renew is called
    THEN: Non-quorum results fail
    """
    clients = [
        FakeClient(renew_result=0),
        FakeClient(renew_result=1),
        FakeClient(renew_result=1),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    assert backend.renew("lock", "owner", ttl_seconds=1.0) == Result.WRONG_OWNER


def test_redlock_release_combines_results() -> None:
    """
    GIVEN: A Redlock backend with mixed release results
    WHEN: release is called
    THEN: Success is reported when the remaining nodes cannot retain a quorum
    """
    clients = [
        FakeClient(release_result=1),
        FakeClient(release_result=0),
        FakeClient(release_result=2),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    assert backend.release("lock", "owner") == Result.SUCCESS


def test_redlock_config_validates_negative_retry_delay() -> None:
    """
    GIVEN: A RedlockConfig with negative retry_delay
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(
            fencing_provider=InMemoryFencingTokenProvider(),
            instances=[FakeClient()],
            retry_delay=-1.0,
        )


def test_redlock_config_validates_negative_clock_drift() -> None:
    """
    GIVEN: A RedlockConfig with negative clock_drift_factor
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(
            fencing_provider=InMemoryFencingTokenProvider(),
            instances=[FakeClient()],
            clock_drift_factor=-0.01,
        )


def test_redlock_config_validates_non_positive_per_instance_timeout() -> None:
    """
    GIVEN: A RedlockConfig with per_instance_timeout <= 0
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(
            fencing_provider=InMemoryFencingTokenProvider(),
            instances=[FakeClient()],
            per_instance_timeout=0,
        )
    with pytest.raises(ConfigurationError):
        RedlockConfig(
            fencing_provider=InMemoryFencingTokenProvider(),
            instances=[FakeClient()],
            per_instance_timeout=-0.05,
        )


def test_redlock_acquire_fails_when_majority_down() -> None:
    """
    GIVEN: A Redlock backend where the majority of instances fail to acquire
    WHEN: acquire is called
    THEN: None is returned
    """
    clients = [
        FakeClient(acquire_result=(-1, -1)),
        FakeClient(acquire_result=(-1, -1)),
        FakeClient(acquire_result=(1, 1)),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
        retry_delay=0,
    )
    backend = RedlockBackend(config)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)
    assert result is None


def test_redlock_get_owner_returns_owner() -> None:
    """
    GIVEN: A Redlock backend where a majority reports one owner
    WHEN: get_owner is called
    THEN: The owner id string is returned
    """
    clients = [FakeClient(), FakeClient(), FakeClient()]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    clients[0].get = lambda key: b"owner_abc"
    clients[1].get = lambda key: b"owner_abc"
    assert backend.get_owner("lock") == "owner_abc"


def test_redlock_is_locked_true_and_false() -> None:
    """
    GIVEN: A Redlock backend
    WHEN: An instance reports the key exists or not
    THEN: is_locked returns True or False accordingly
    """
    clients = [FakeClient(), FakeClient(), FakeClient()]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    assert backend.is_locked("lock") is False

    clients[0].exists = lambda key: 1
    assert backend.is_locked("lock") is True


def test_redlock_release_all_not_found() -> None:
    """
    GIVEN: A Redlock backend where all instances report NOT_FOUND
    WHEN: release is called
    THEN: Result.NOT_FOUND is returned
    """
    clients = [
        FakeClient(release_result=2),
        FakeClient(release_result=2),
        FakeClient(release_result=2),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)
    assert backend.release("lock", "owner") == Result.NOT_FOUND


class OrderedFakeClient:
    """
    FakeClient that registers scripts in exact order matching RedlockBackend.
    """

    def __init__(self, script_results):
        """
        :param script_results: list of 6 results for
            [acquire, release, renew, acquire_read, release_read, renew_read]
        """
        self._scripts = [FakeScript(r) for r in script_results]
        self._idx = 0

    def register_script(self, _script):
        if _script == _redis_scripts.IS_LOCKED:
            return lambda **kwargs: False
        script = self._scripts[self._idx]
        self._idx += 1
        return script

    def get(self, key):
        return None

    def exists(self, key):
        return 0


def test_redlock_shared_acquire_release_renew() -> None:
    """
    GIVEN: A Redlock backend in SHARED mode
    WHEN: acquire, renew, and release are called
    THEN: They succeed with quorum
    """
    # script_results: [acquire, release, renew, acquire_read, release_read, renew_read]
    clients = [
        OrderedFakeClient([None, None, None, (1, 1), 0, 0]),
        OrderedFakeClient([None, None, None, (3, 1), 0, 0]),
        OrderedFakeClient([None, None, None, (-1, -1), 0, 0]),
    ]
    config = RedlockConfig(
        fencing_provider=InMemoryFencingTokenProvider(),
        instances=clients,
        quorum=2,
        retry_count=0,
    )
    backend = RedlockBackend(config)

    result = backend.acquire(
        "lock",
        "reader",
        ttl_seconds=1.0,
        epoch=1,
        mode=LockMode.SHARED,
        acquired_at=1.0,
    )
    assert result is not None
    assert isinstance(result, AcquisitionResult)
    assert result.fencing_token == 1
    assert result.epoch == 1

    assert isinstance(
        backend.renew("lock", "reader", ttl_seconds=1.0, mode=LockMode.SHARED),
        RenewalResult,
    )
    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


def test_redlock_requires_an_explicit_fencing_authority():
    """
    GIVEN: Independent Redis nodes without a shared fencing authority
    WHEN: Redlock is configured
    THEN: Configuration fails instead of promising monotonic local maxima
    """
    with pytest.raises(ConfigurationError, match="fencing_provider"):
        RedlockConfig(instances=[FakeClient() for _ in range(3)])


def test_authority_orders_tokens_across_changing_quorums():
    """
    GIVEN: Successive quorums with decreasing maxima of independent counters
    WHEN: Both quorums issue fencing tokens
    THEN: The shared authority still produces strictly increasing tokens
    """
    clients = [
        FakeClient(acquire_result=(100, 1)),
        FakeClient(),
        FakeClient(acquire_result=(-1, -1)),
    ]
    provider = InMemoryFencingTokenProvider()
    backend = RedlockBackend(
        RedlockConfig(instances=clients, fencing_provider=provider, retry_count=0)
    )
    assert (first := backend.acquire("lock", "first", 5))
    assert backend.release("lock", "first") == Result.SUCCESS
    clients[0].acquire_script.result = (-1, -1)
    clients[1].acquire_script.result = (2, 2)
    clients[2].acquire_script.result = (1, 1)
    assert (second := backend.acquire("lock", "second", 5))
    assert second.fencing_token > first.fencing_token


def test_lost_acquire_reply_is_included_in_cleanup(monkeypatch):
    """
    GIVEN: An acquisition that may have committed before its response timed out
    WHEN: The remaining nodes cannot form a quorum
    THEN: Cleanup includes the node with the lost response
    """
    clients = [
        FakeClient(),
        FakeClient(acquire_result=(-1, -1)),
        FakeClient(acquire_result=(-1, -1)),
    ]
    backend = RedlockBackend(
        RedlockConfig(
            instances=clients, fencing_provider=InMemoryFencingTokenProvider(), retry_count=0
        )
    )

    def lost_reply(*args, **kwargs):
        raise RedisTimeoutError("response lost")

    monkeypatch.setattr(backend._backends[0], "acquire", lost_reply)
    assert backend.acquire("lock", "owner", 5) is None
    assert all(client.release_script.calls for client in clients)


def test_slow_quorum_renewal_fails_closed(monkeypatch):
    """
    GIVEN: Successful renewals whose responses consume the entire TTL
    WHEN: Redlock evaluates the renewed lease's validity
    THEN: It rejects the expired quorum and releases every node
    """
    clients = [FakeClient() for _ in range(3)]
    backend = RedlockBackend(
        RedlockConfig(
            instances=clients, fencing_provider=InMemoryFencingTokenProvider(), retry_count=0
        )
    )
    ticks = iter([0.0, 2.0])
    monkeypatch.setattr(time, "monotonic", lambda: next(ticks))
    assert backend.renew("lock", "owner", 1) == Result.WRONG_OWNER
    assert all(client.release_script.calls for client in clients)


def test_redlock_bounds_private_connection_timeouts():
    """
    GIVEN: Caller-owned clients without socket timeouts
    WHEN: Redlock creates its per-instance connections
    THEN: Private connections have bounded timeouts without modifying the callers
    """
    clients = [Redis(port=6379 + number) for number in range(3)]
    backend = RedlockBackend(
        RedlockConfig(
            instances=clients,
            fencing_provider=InMemoryFencingTokenProvider(),
            per_instance_timeout=0.025,
        )
    )
    try:
        for client in backend._owned_clients:
            params = client.connection_pool.connection_kwargs
            assert params["socket_timeout"] == 0.025
            assert params["socket_connect_timeout"] == 0.025
        assert all(
            client.connection_pool.connection_kwargs["socket_timeout"] is None
            for client in clients
        )
    finally:
        backend.close()
        for client in clients:
            client.close()


if __name__ == "__main__":
    pass
