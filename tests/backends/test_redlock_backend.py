import time

import pytest

from mithril._types import Result
from mithril.backends._redlock import RedlockBackend, RedlockConfig
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
    def __init__(self, acquire_result=(1, 1), release_result=0, renew_result=0):
        self.acquire_script = FakeScript(acquire_result)
        self.release_script = FakeScript(release_result)
        self.renew_script = FakeScript(renew_result)
        self.get_calls = []
        self.exists_calls = []

    def register_script(self, _script):
        if _script.strip().startswith("local acquired"):
            return self.acquire_script
        if "LPUSH" in _script:
            return self.release_script
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
        RedlockConfig(instances=[])


def test_redlock_acquire_succeeds_with_quorum(monkeypatch) -> None:
    """
    GIVEN: A Redlock backend with quorum clients available
    WHEN: acquire is called
    THEN: The max fencing token is returned
    """
    clients = [
        FakeClient(acquire_result=(1, 1)),
        FakeClient(acquire_result=(3, 1)),
        FakeClient(acquire_result=(-1, -1)),
    ]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
    backend = RedlockBackend(config)

    monkeypatch.setattr(time, "monotonic", lambda: 0.0)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert result == (3, 1)


def test_redlock_acquire_releases_on_failure() -> None:
    """
    GIVEN: A Redlock backend without quorum
    WHEN: acquire fails
    THEN: Successful instances are released
    """
    clients = [
        FakeClient(acquire_result=(1, 1)),
        FakeClient(acquire_result=(-1, -1)),
        FakeClient(acquire_result=(-1, -1)),
    ]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
    backend = RedlockBackend(config)

    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)

    assert result is None
    assert clients[0].release_script.calls


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
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
    backend = RedlockBackend(config)

    assert backend.renew("lock", "owner", ttl_seconds=1.0) == Result.WRONG_OWNER


def test_redlock_release_combines_results() -> None:
    """
    GIVEN: A Redlock backend with mixed release results
    WHEN: release is called
    THEN: Success is reported if any instance succeeds
    """
    clients = [
        FakeClient(release_result=1),
        FakeClient(release_result=0),
        FakeClient(release_result=2),
    ]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
    backend = RedlockBackend(config)

    assert backend.release("lock", "owner") == Result.SUCCESS


def test_redlock_config_validates_negative_retry_delay() -> None:
    """
    GIVEN: A RedlockConfig with negative retry_delay
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(instances=[FakeClient()], retry_delay=-1.0)


def test_redlock_config_validates_negative_clock_drift() -> None:
    """
    GIVEN: A RedlockConfig with negative clock_drift_factor
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(instances=[FakeClient()], clock_drift_factor=-0.01)


def test_redlock_config_validates_non_positive_per_instance_timeout() -> None:
    """
    GIVEN: A RedlockConfig with per_instance_timeout <= 0
    WHEN: It is constructed
    THEN: ConfigurationError is raised
    """
    with pytest.raises(ConfigurationError):
        RedlockConfig(instances=[FakeClient()], per_instance_timeout=0)
    with pytest.raises(ConfigurationError):
        RedlockConfig(instances=[FakeClient()], per_instance_timeout=-0.05)


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
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0, retry_delay=0)
    backend = RedlockBackend(config)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1, acquired_at=1.0)
    assert result is None


def test_redlock_get_owner_returns_owner() -> None:
    """
    GIVEN: A Redlock backend where an instance reports an owner
    WHEN: get_owner is called
    THEN: The owner id string is returned
    """
    clients = [FakeClient(), FakeClient(), FakeClient()]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
    backend = RedlockBackend(config)

    clients[0].get = lambda key: b"owner_abc"
    assert backend.get_owner("lock") == "owner_abc"


def test_redlock_is_locked_true_and_false() -> None:
    """
    GIVEN: A Redlock backend
    WHEN: An instance reports the key exists or not
    THEN: is_locked returns True or False accordingly
    """
    clients = [FakeClient(), FakeClient(), FakeClient()]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
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
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
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
    from mithril._types import LockMode

    # script_results: [acquire, release, renew, acquire_read, release_read, renew_read]
    clients = [
        OrderedFakeClient([None, None, None, (1, 1), 0, 0]),
        OrderedFakeClient([None, None, None, (3, 1), 0, 0]),
        OrderedFakeClient([None, None, None, (-1, -1), 0, 0]),
    ]
    config = RedlockConfig(instances=clients, quorum=2, retry_count=0)
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
    assert result == (3, 1)

    assert (
        backend.renew("lock", "reader", ttl_seconds=1.0, mode=LockMode.SHARED)
        == Result.SUCCESS
    )
    assert backend.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


if __name__ == "__main__":
    pass
