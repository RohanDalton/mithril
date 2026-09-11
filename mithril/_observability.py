"""
Observability helpers for Mithril: structured logging and metrics hooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import structlog
from opentelemetry.metrics import get_meter

from ._types import LockMode, LockState

__author__ = "Rohan B. Dalton"


@dataclass(frozen=True)
class LockEvent:
    """
    Snapshot of a lock lifecycle event.
    """

    event: str
    lock_name: str
    owner_id: str | None
    mode: LockMode | None
    fencing_token: int | None
    epoch: int | None
    ttl: float | None
    timeout: float | None
    elapsed: float | None
    state: LockState | None
    backend: str | None
    namespace: str | None


@dataclass(frozen=True)
class LockHooks:
    """
    Optional callbacks for lifecycle events.
    """

    on_acquire: Callable[[LockEvent], None] | None = None
    on_release: Callable[[LockEvent], None] | None = None
    on_renewal: Callable[[LockEvent], None] | None = None
    on_timeout: Callable[[LockEvent], None] | None = None
    on_expired: Callable[[LockEvent], None] | None = None


_HOOKS = LockHooks()
_LOGGER = structlog.get_logger("mithril")
_METER = get_meter("mithril")

_ACQUIRE_COUNTER = _METER.create_counter(
    "mithril.lock.acquire.total", description="Total lock acquisitions", unit="1"
)
_RELEASE_COUNTER = _METER.create_counter(
    "mithril.lock.release.total", description="Total lock releases", unit="1"
)
_RENEW_COUNTER = _METER.create_counter(
    "mithril.lock.renew.total", description="Total lock renewals", unit="1"
)
_TIMEOUT_COUNTER = _METER.create_counter(
    "mithril.lock.timeout.total", description="Total lock acquisition timeouts", unit="1"
)
_EXPIRED_COUNTER = _METER.create_counter(
    "mithril.lock.expired.total", description="Total lock expirations", unit="1"
)
_ACQUIRE_LATENCY = _METER.create_histogram(
    "mithril.lock.acquire.latency", description="Lock acquisition latency", unit="s"
)


def configure_logging(**kwargs: Any) -> None:
    """
    Configure structlog for Mithril consumers.

    kwargs are passed directly to structlog.configure.
    """

    structlog.configure(**kwargs)


def set_hooks(hooks: LockHooks | None) -> None:
    """
    Set global lock hooks. Pass None to clear.
    """

    global _HOOKS
    _HOOKS = hooks or LockHooks()


def get_hooks() -> LockHooks:
    """
    Return the global lock hooks.
    """

    return _HOOKS


def get_logger() -> structlog.BoundLogger:
    """
    Return the default Mithril logger.
    """

    return _LOGGER


def record_event(event: LockEvent, hooks: LockHooks | None = None) -> None:
    """
    Invoke hooks and record OpenTelemetry metrics.
    """

    try:
        _record_event(event, hooks)
    except Exception:
        log_safely(_LOGGER, "exception", "lock_observability_failed", lock_event=event.event)


def log_safely(logger, level: str, event: str, *args, **kwargs) -> None:
    """
    Logging failures must never interrupt ownership transitions or cleanup.
    """
    try:
        getattr(logger, level)(event, *args, **kwargs)
    except Exception:
        # Retrying through the same broken logging pipeline would fail again.
        pass


def _record_event(event: LockEvent, hooks: LockHooks | None = None) -> None:
    hooks = hooks or _HOOKS
    if event.event == "acquire":
        if hooks.on_acquire is not None:
            hooks.on_acquire(event)
        _ACQUIRE_COUNTER.add(1, _attributes(event))
        if event.elapsed is not None:
            _ACQUIRE_LATENCY.record(event.elapsed, _attributes(event))
    elif event.event == "release":
        if hooks.on_release is not None:
            hooks.on_release(event)
        _RELEASE_COUNTER.add(1, _attributes(event))
    elif event.event == "renew":
        if hooks.on_renewal is not None:
            hooks.on_renewal(event)
        _RENEW_COUNTER.add(1, _attributes(event))
    elif event.event == "timeout":
        if hooks.on_timeout is not None:
            hooks.on_timeout(event)
        _TIMEOUT_COUNTER.add(1, _attributes(event))
    elif event.event == "expired":
        if hooks.on_expired is not None:
            hooks.on_expired(event)
        _EXPIRED_COUNTER.add(1, _attributes(event))


def _attributes(event: LockEvent) -> dict[str, Any]:
    return {
        "lock_name": event.lock_name,
        "mode": event.mode.value if event.mode else None,
        "backend": event.backend,
        "namespace": event.namespace or "",
    }


if __name__ == "__main__":
    pass
