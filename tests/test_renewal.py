import gc
import time
import weakref

from mithril._renewal import LockRenewer
from mithril._types import LockMode, Result

__author__ = "Rohan B. Dalton"


class FakeBackend:
    def __init__(self, results: list[Result]) -> None:
        self._results = results
        self.calls: int = 0

    def renew(
        self, _key: str, _owner_id: str, _ttl: float, _mode: LockMode = LockMode.EXCLUSIVE
    ) -> Result:
        self.calls += 1
        if self._results:
            return self._results.pop(0)
        return Result.SUCCESS


class FakeLock:
    def __init__(self) -> None:
        self.failed = False

    def _on_renewal_failed(self) -> None:
        self.failed = True


def test_renewal_failure_notifies_lock() -> None:
    """
    GIVEN: A backend that fails renewal
    WHEN: The renewal thread runs
    THEN: The lock is notified of failure
    """
    lock = FakeLock()
    backend = FakeBackend([Result.WRONG_OWNER])
    renewer = LockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
    )
    renewer.start()

    deadline = time.time() + 1.0
    while not lock.failed and time.time() < deadline:
        time.sleep(0.01)

    renewer.stop()
    assert lock.failed is True


def test_renewal_stops_when_lock_is_collected() -> None:
    """
    GIVEN: A lock that is garbage collected
    WHEN: The renewer wakes up
    THEN: It exits without further renewals
    """
    backend = FakeBackend([Result.SUCCESS, Result.SUCCESS, Result.SUCCESS])
    lock = FakeLock()
    renewer = LockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
    )
    renewer.start()
    time.sleep(0.05)

    calls_before = backend.calls
    del lock
    gc.collect()
    time.sleep(0.05)

    renewer.stop()
    assert backend.calls == calls_before


def test_renewal_thread_is_daemon_and_stops_quickly() -> None:
    """
    GIVEN: A renewal thread with a long interval
    WHEN: stop is invoked
    THEN: The thread is a daemon and returns promptly
    """
    lock = FakeLock()
    backend = FakeBackend([Result.SUCCESS])
    renewer = LockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=5.0,
    )
    assert renewer._thread.daemon is True
    renewer.start()

    start = time.time()
    renewer.stop(timeout=0.5)
    assert time.time() - start < 1.0


def test_custom_on_renewal_failed_callback() -> None:
    """
    GIVEN: A renewer with a custom on_renewal_failed callback
    WHEN: The backend fails renewal
    THEN: The custom callback is invoked instead of lock._on_renewal_failed
    """
    callback_called = []

    def my_callback():
        callback_called.append(True)

    lock = FakeLock()
    backend = FakeBackend([Result.WRONG_OWNER])
    renewer = LockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
        on_renewal_failed=my_callback,
    )
    renewer.start()

    deadline = time.time() + 1.0
    while not callback_called and time.time() < deadline:
        time.sleep(0.01)

    renewer.stop()
    assert callback_called
    assert not lock.failed


class RaisingBackend:
    """Backend that raises on renew."""

    def renew(
        self, _key: str, _owner_id: str, _ttl: float, _mode: LockMode = LockMode.EXCLUSIVE
    ):
        raise RuntimeError("connection lost")


def test_renewal_exception_triggers_failure() -> None:
    """
    GIVEN: A backend that raises an exception during renewal
    WHEN: The renewal thread runs
    THEN: The lock is notified of failure
    """
    lock = FakeLock()
    backend = RaisingBackend()
    renewer = LockRenewer(
        weakref.ref(lock),
        backend,
        key="lock",
        owner_id="owner",
        ttl=1.0,
        interval=0.01,
    )
    renewer.start()

    deadline = time.time() + 1.0
    while not lock.failed and time.time() < deadline:
        time.sleep(0.01)

    renewer.stop()
    assert lock.failed is True


if __name__ == "__main__":
    pass
