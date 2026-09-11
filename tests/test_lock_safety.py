import threading
import time

import pytest

from mithril import Lock, LockHooks, LockState, _observability
from mithril._types import LockConfig
from mithril.exceptions import (
    AlreadyAcquiredError,
    ConfigurationError,
    LockExpiredError,
    LockTimeoutError,
    RenewalFailedError,
)
from tests.test_lock import FakeBackend


def test_nonblocking_acquires_an_uncontended_lock():
    """
    GIVEN: A free lock and the real monotonic clock
    WHEN: A nonblocking acquisition is requested
    THEN: The backend is tried exactly once and ownership is returned
    """
    backend = FakeBackend()
    lock = Lock("free", backend=backend)
    token = lock.acquire(blocking=False)
    assert backend.acquire_calls == 1
    assert token.owner_id == backend.owner
    lock.release()


def test_auto_renewal_refreshes_local_expiry():
    """
    GIVEN: A lock with automatic renewal
    WHEN: It is held beyond its original TTL
    THEN: Its local deadline is refreshed and release succeeds
    """
    backend = FakeBackend()
    lock = Lock("renew", backend=backend, ttl=0.3, auto_renewal=True, renewal_interval=0.05)
    with lock as token:
        time.sleep(0.65)
        assert backend.renew_calls > 0
        assert lock.token.expire_at > token.expire_at
        assert lock.time_remaining > 0
    assert backend.owner is None


def test_late_acquisition_response_is_rejected(monkeypatch):
    """
    GIVEN: A backend response delayed beyond the lease TTL
    WHEN: Acquisition finishes
    THEN: The facade rejects and cleans up the expired lease
    """
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    backend = FakeBackend()
    acquire = backend.acquire

    def delayed(*args):
        result = acquire(*args)
        now[0] += 2.0
        return result

    monkeypatch.setattr(backend, "acquire", delayed)
    lock = Lock("late", backend=backend, ttl=1.0)
    with pytest.raises(LockTimeoutError):
        lock.acquire()
    assert backend.owner is None
    assert lock.token is None


def test_late_renewal_response_is_rejected(monkeypatch):
    """
    GIVEN: A backend renewal response delayed beyond the new TTL
    WHEN: Manual renewal completes
    THEN: The facade reports lost ownership without extending local validity
    """
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    backend = FakeBackend()
    lock = Lock("late", backend=backend, ttl=1.0)
    lock.acquire()
    renew = backend.renew

    def delayed(*args):
        result = renew(*args)
        now[0] += 2.0
        return result

    monkeypatch.setattr(backend, "renew", delayed)
    with pytest.raises(RenewalFailedError):
        lock.renew()
    assert lock.state == LockState.EXPIRED
    with pytest.raises(LockExpiredError):
        lock.release()
    assert backend.owner is None


def test_concurrent_acquire_preserves_owner(monkeypatch):
    """
    GIVEN: An acquisition awaiting its backend response
    WHEN: A second thread tries the same facade
    THEN: The second call is rejected without changing the first owner's identity
    """
    backend = FakeBackend()
    entered = threading.Event()
    respond = threading.Event()
    acquire = backend.acquire

    def delayed(*args):
        result = acquire(*args)
        entered.set()
        respond.wait(5)
        return result

    monkeypatch.setattr(backend, "acquire", delayed)
    lock = Lock("race", backend=backend)
    tokens = []
    thread = threading.Thread(target=lambda: tokens.append(lock.acquire()))
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(AlreadyAcquiredError):
            lock.acquire()
    finally:
        respond.set()
        thread.join(5)
    assert not thread.is_alive()
    assert tokens[0].owner_id == backend.owner
    lock.release()


def test_observability_failure_does_not_leak_lock():
    """
    GIVEN: A metrics callback that raises
    WHEN: A lock context is entered and exited
    THEN: Ownership is acquired and released normally
    """

    def failed(event):
        raise RuntimeError("metrics unavailable")

    backend = FakeBackend()
    with Lock("hooks", backend=backend, hooks=LockHooks(on_acquire=failed)):
        assert backend.owner is not None
    assert backend.owner is None


def test_context_surfaces_ownership_loss():
    """
    GIVEN: Background renewal reports lost ownership
    WHEN: An otherwise successful context exits
    THEN: LockExpiredError is raised and local resources are cleaned up
    """
    backend = FakeBackend()
    lock = Lock("lost", backend=backend)
    with pytest.raises(LockExpiredError):
        with lock:
            lock._on_renewal_failed()
    assert backend.owner is None


def test_expired_context_preserves_body_exception():
    """
    GIVEN: A context loses ownership and its body raises
    WHEN: Context cleanup also reports expiry
    THEN: The original body exception is preserved
    """
    lock = Lock("lost", backend=FakeBackend())
    with pytest.raises(KeyError):
        with lock:
            lock._on_renewal_failed()
            raise KeyError("body failed")


def test_state_and_token_notice_expiry(monkeypatch):
    """
    GIVEN: A lease whose monotonic deadline has passed
    WHEN: Its public state and token are inspected
    THEN: It reports expiry and exposes no current token
    """
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    lock = Lock("expiry", backend=FakeBackend(), ttl=1.0)
    lock.acquire()
    now[0] = 2.0
    assert lock.state == LockState.EXPIRED
    assert lock.token is None
    assert lock.time_remaining is None
    with pytest.raises(LockExpiredError):
        lock.release()


@pytest.mark.parametrize(
    "settings",
    [
        {"ttl": float("nan")},
        {"ttl": float("inf")},
        {"ttl": True},
        {"timeout": float("nan")},
        {"retry_interval": float("inf")},
        {"retry_jitter": -1},
        {"renewal_interval": 0},
        {"renewal_interval": -1},
        {"ttl": 1.0, "renewal_interval": 0.95},
        {"mode": "exclusive"},
        {"name": ""},
        {"namespace": None},
    ],
)
def test_invalid_configuration_fails_before_acquisition(settings):
    """
    GIVEN: Invalid duration or ownership configuration
    WHEN: A lock configuration is constructed
    THEN: A typed configuration error is raised before any backend mutation
    """
    with pytest.raises(ConfigurationError):
        LockConfig(**{"name": "invalid", **settings})


def test_strict_backend_validation_rejects_missing_methods():
    """
    GIVEN: An object without the backend mutation interface
    WHEN: A strict lock is constructed
    THEN: ConfigurationError identifies the missing interface
    """
    with pytest.raises(ConfigurationError, match="acquire"):
        Lock("invalid", backend=object())


def test_logging_failure_cannot_leak_ownership(monkeypatch):
    """
    GIVEN: A logging processor that raises during lifecycle notifications
    WHEN: A lock is used as a context manager
    THEN: Logging errors cannot interrupt acquisition or release
    """
    backend = FakeBackend()
    lock = Lock("logging", backend=backend)

    def fail(*args, **kwargs):
        raise RuntimeError("logger failed")

    monkeypatch.setattr(lock._logger, "info", fail)
    monkeypatch.setattr(_observability._LOGGER, "exception", fail)
    with lock:
        assert backend.owner == lock.owner_id
    assert backend.owner is None
    assert lock.state == LockState.RELEASED


def test_manual_renewal_cannot_undercut_background_schedule():
    """
    GIVEN: A lock with a fixed automatic renewal interval
    WHEN: Manual renewal requests a TTL shorter than that interval
    THEN: Validation rejects the TTL without changing backend ownership
    """
    backend = FakeBackend()
    lock = Lock("renewal", backend=backend, ttl=10, auto_renewal=True, renewal_interval=1)
    with lock:
        with pytest.raises(ConfigurationError, match="interval"):
            lock.renew(ttl=0.5)
        assert backend.renew_calls == 0
