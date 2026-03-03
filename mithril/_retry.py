"""
Retry and backoff utilities for Mithril.
"""

from __future__ import annotations

import dataclasses
import random
import time
from typing import Callable, Iterator

__author__ = "Rohan B. Dalton"


@dataclasses.dataclass(frozen=True)
class RetryAttempt:
    """
    Metadata for a single retry attempt.
    """

    attempt_number: int
    elapsed: float
    remaining: float
    delay: float


class RetryPolicy:
    """
    Configurable retry with exponential backoff and jitter.
    """

    def __init__(
        self,
        timeout: float,
        base_delay: float,
        max_delay: float,
        jitter: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout < 0:
            raise ValueError("timeout must be >= 0")
        if base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if max_delay <= 0:
            raise ValueError("max_delay must be > 0")
        if jitter < 0:
            raise ValueError("jitter must be >= 0")
        self._timeout = timeout
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._clock = clock

    def attempts(
        self,
        wait_func: Callable[[float], bool] | None = None,
    ) -> Iterator[RetryAttempt]:
        """
        Yield RetryAttempt objects until the timeout is exceeded.
        """

        start = self._clock()
        attempt = 0
        delay = 0.0
        while True:
            now = self._clock()
            elapsed = now - start
            remaining = self._timeout - elapsed
            if remaining < 0:
                break

            attempt += 1
            yield RetryAttempt(
                attempt_number=attempt,
                elapsed=elapsed,
                remaining=remaining,
                delay=delay,
            )

            if remaining <= 0:
                break

            backoff = self._base_delay * (2 ** (attempt - 1))
            delay = min(self._max_delay, backoff) + random.uniform(0, self._jitter)
            if delay > remaining:
                delay = remaining
            if delay <= 0:
                continue
            if wait_func is None:
                time.sleep(delay)
            else:
                wait_func(delay)


if __name__ == "__main__":
    pass
