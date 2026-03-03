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


if __name__ == "__main__":
    pass
