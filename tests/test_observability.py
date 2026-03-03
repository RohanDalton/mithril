import pytest

from mithril._observability import (
    LockEvent,
    LockHooks,
    get_hooks,
    record_event,
    set_hooks,
)
from mithril._types import LockMode, LockState

__author__ = "Rohan B. Dalton"


def _make_event(event: str) -> LockEvent:
    return LockEvent(
        event=event,
        lock_name="ring",
        owner_id="owner",
        mode=LockMode.EXCLUSIVE,
        fencing_token=1,
        epoch=1,
        ttl=30.0,
        timeout=None,
        elapsed=0.01,
        state=LockState.ACQUIRED,
        backend="FakeBackend",
        namespace="",
    )


def test_record_event_dispatches_acquire() -> None:
    """
    GIVEN: A LockHooks with on_acquire set
    WHEN: record_event is called with an acquire event
    THEN: The on_acquire callback receives the event
    """
    received = []
    hooks = LockHooks(on_acquire=lambda e: received.append(e))
    event = _make_event("acquire")
    record_event(event, hooks=hooks)
    assert len(received) == 1
    assert received[0].event == "acquire"


def test_record_event_dispatches_release() -> None:
    """
    GIVEN: A LockHooks with on_release set
    WHEN:  record_event is called with a release event
    THEN:  The on_release callback receives the event
    """
    received = []
    hooks = LockHooks(on_release=lambda e: received.append(e))
    record_event(_make_event("release"), hooks=hooks)
    assert len(received) == 1
    assert received[0].event == "release"


def test_record_event_dispatches_renew() -> None:
    """
    GIVEN: A LockHooks with on_renewal set
    WHEN:  record_event is called with a renew event
    THEN:  The on_renewal callback receives the event
    """
    received = []
    hooks = LockHooks(on_renewal=lambda e: received.append(e))
    record_event(_make_event("renew"), hooks=hooks)
    assert len(received) == 1
    assert received[0].event == "renew"


def test_record_event_dispatches_timeout() -> None:
    """
    GIVEN: A LockHooks with on_timeout set
    WHEN:  record_event is called with a timeout event
    THEN:  The on_timeout callback receives the event
    """
    received = []
    hooks = LockHooks(on_timeout=lambda e: received.append(e))
    record_event(_make_event("timeout"), hooks=hooks)
    assert len(received) == 1
    assert received[0].event == "timeout"


def test_record_event_dispatches_expired() -> None:
    """
    GIVEN: A LockHooks with on_expired set
    WHEN:  record_event is called with an expired event
    THEN:  The on_expired callback receives the event
    """
    received = []
    hooks = LockHooks(on_expired=lambda e: received.append(e))
    record_event(_make_event("expired"), hooks=hooks)
    assert len(received) == 1
    assert received[0].event == "expired"


def test_set_hooks_get_hooks_round_trip() -> None:
    """
    GIVEN: Custom LockHooks
    WHEN: set_hooks is called then get_hooks is called
    THEN: The same hooks object is returned
    """
    original = get_hooks()
    try:
        hooks = LockHooks(on_acquire=lambda e: None)
        set_hooks(hooks)
        assert get_hooks() is hooks
    finally:
        set_hooks(original)


def test_set_hooks_none_resets_to_default() -> None:
    """
    GIVEN: Custom hooks have been set
    WHEN: set_hooks(None) is called
    THEN: get_hooks returns a default LockHooks with no callbacks
    """
    original = get_hooks()
    try:
        set_hooks(LockHooks(on_acquire=lambda e: None))
        set_hooks(None)
        hooks = get_hooks()
        assert hooks.on_acquire is None
        assert hooks.on_release is None
        assert hooks.on_renewal is None
        assert hooks.on_timeout is None
        assert hooks.on_expired is None
    finally:
        set_hooks(original)


if __name__ == "__main__":
    pass
