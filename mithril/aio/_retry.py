"""
Async retry and backoff utilities for Mithril.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable
from typing import Callable

from .._retry import RetryAttempt
from .._types import validate_duration

__author__ = "Rohan B. Dalton"


class AsyncRetryPolicy:
    """
    Async retry with exponential backoff and jitter.
    """

    def __init__(
        self,
        timeout: float,
        base_delay: float,
        max_delay: float,
        jitter: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        validate_duration(timeout, "timeout", allow_zero=True)
        validate_duration(base_delay, "base_delay")
        validate_duration(max_delay, "max_delay")
        validate_duration(jitter, "jitter", allow_zero=True)
        self._timeout = timeout
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._clock = clock

    async def attempts(
        self,
        wait_func: Callable[[float], Awaitable[bool]] | None = None,
    ) -> AsyncIterator[RetryAttempt]:
        """
        Yield RetryAttempt objects until the timeout is exceeded.
        """
        start = self._clock()
        attempt = 0
        delay = 0.0
        backoff = min(self._base_delay, self._max_delay)
        while True:
            now = self._clock()
            elapsed = now - start
            remaining = max(0.0, self._timeout - elapsed)
            if attempt and remaining <= 0:
                break

            attempt += 1
            yield RetryAttempt(
                attempt_number=attempt,
                elapsed=elapsed,
                remaining=remaining,
                delay=delay,
            )

            if (remaining := self._timeout - (self._clock() - start)) <= 0:
                break

            delay = min(self._max_delay, backoff + random.uniform(0, self._jitter), remaining)
            backoff = min(self._max_delay, backoff * 2)
            if delay <= 0:
                continue

            if wait_func is None:
                await asyncio.sleep(delay)
            else:
                await wait_func(delay)


if __name__ == "__main__":
    pass
