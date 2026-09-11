import random

import pytest

from mithril._retry import RetryPolicy

__author__ = "Rohan B. Dalton"


class FakeClock:
    def __init__(self) -> None:
        self._now = 0.0

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    def wait(self, seconds: float) -> bool:
        self.advance(seconds)
        return False


def test_timeout_zero_yields_one_attempt() -> None:
    """
    GIVEN: A retry policy with zero timeout
    WHEN: attempts are iterated
    THEN: Exactly one attempt is yielded
    """
    clock = FakeClock()
    policy = RetryPolicy(timeout=0, base_delay=0.1, max_delay=1.0, jitter=0, clock=clock.now)
    attempts = list(policy.attempts(wait_func=clock.wait))

    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].delay == 0


def test_max_delay_caps_backoff() -> None:
    """
    GIVEN: A retry policy with exponential backoff
    WHEN: Delays grow beyond max_delay
    THEN: The delay is capped at max_delay
    """
    clock = FakeClock()
    policy = RetryPolicy(timeout=10, base_delay=1, max_delay=2, jitter=0, clock=clock.now)
    attempts = []
    for attempt in policy.attempts(wait_func=clock.wait):
        attempts.append(attempt)
        if len(attempts) == 4:
            break

    delays = [attempt.delay for attempt in attempts]
    assert delays[0] == 0
    assert delays[1] == 1
    assert delays[2] == 2
    assert delays[3] == 2


def test_jitter_is_applied_to_delay(monkeypatch) -> None:
    """
    GIVEN: A retry policy with jitter configured
    WHEN: The retry delay is computed
    THEN: The jitter value is added to the delay
    """
    clock = FakeClock()
    monkeypatch.setattr(random, "uniform", lambda _a, _b: 0.5)
    policy = RetryPolicy(timeout=5, base_delay=1, max_delay=10, jitter=0.5, clock=clock.now)
    attempts = list(policy.attempts(wait_func=clock.wait))

    assert attempts[1].delay == pytest.approx(1.5)


def test_wait_func_receives_remaining_time() -> None:
    """
    GIVEN: A retry policy with limited timeout
    WHEN: wait_func is used for delays
    THEN: The wait durations never exceed remaining time
    """
    clock = FakeClock()
    waits: list[float] = []

    def wait_func(seconds: float) -> bool:
        waits.append(seconds)
        return clock.wait(seconds)

    policy = RetryPolicy(timeout=1.0, base_delay=0.8, max_delay=1.0, jitter=0, clock=clock.now)
    list(policy.attempts(wait_func=wait_func))

    assert waits[0] == pytest.approx(0.8)
    assert waits[1] == pytest.approx(0.2)
    assert all(wait <= 1.0 for wait in waits)


def test_backend_time_counts_toward_retry_deadline():
    """
    GIVEN: An acquisition attempt consumes nearly the entire timeout
    WHEN: The retry engine waits after that attempt
    THEN: It uses the remaining budget after the attempt instead of stale timing
    """
    clock = FakeClock()
    policy = RetryPolicy(timeout=1, base_delay=0.8, max_delay=1, jitter=0, clock=clock.now)
    attempts = policy.attempts(wait_func=clock.wait)
    next(attempts)
    clock.advance(0.9)
    assert list(attempts) == []
    assert clock.now() == pytest.approx(1.0)


def test_jitter_cannot_exceed_max_delay(monkeypatch):
    """
    GIVEN: Jitter larger than the configured maximum retry delay
    WHEN: Backoff delays are calculated
    THEN: Every delay still respects the maximum
    """
    clock = FakeClock()
    monkeypatch.setattr(random, "uniform", lambda *args: 10)
    policy = RetryPolicy(timeout=3, base_delay=0.5, max_delay=1, jitter=10, clock=clock.now)
    attempts = list(policy.attempts(wait_func=clock.wait))
    assert all(attempt.delay <= 1 for attempt in attempts)


if __name__ == "__main__":
    pass
