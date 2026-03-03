import asyncio

import pytest

from mithril.aio._retry import AsyncRetryPolicy

__author__ = "Rohan B. Dalton"


class FakeClock:
    def __init__(self) -> None:
        self._now = 0.0

    def now(self) -> float:
        return self._now

    async def wait(self, seconds: float) -> bool:
        self._now += seconds
        return False


@pytest.mark.asyncio
async def test_async_timeout_zero_yields_one_attempt() -> None:
    """
    GIVEN: An async retry policy with zero timeout
    WHEN: attempts are iterated
    THEN: Exactly one attempt is yielded
    """

    clock = FakeClock()
    policy = AsyncRetryPolicy(
        timeout=0,
        base_delay=0.1,
        max_delay=1.0,
        jitter=0,
        clock=clock.now,
    )
    attempts = [attempt async for attempt in policy.attempts(wait_func=clock.wait)]
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1


@pytest.mark.asyncio
async def test_async_wait_func_receives_remaining_time() -> None:
    """
    GIVEN: An async retry policy with limited timeout
    WHEN: wait_func is used for delays
    THEN: The wait durations never exceed remaining time
    """

    clock = FakeClock()
    waits: list[float] = []

    async def wait_func(seconds: float) -> bool:
        waits.append(seconds)
        return await clock.wait(seconds)

    policy = AsyncRetryPolicy(
        timeout=1.0,
        base_delay=0.8,
        max_delay=1.0,
        jitter=0,
        clock=clock.now,
    )
    _ = [attempt async for attempt in policy.attempts(wait_func=wait_func)]
    assert waits[0] == pytest.approx(0.8)
    assert waits[1] == pytest.approx(0.2)
    assert all(wait <= 1.0 for wait in waits)


if __name__ == "__main__":
    pass
