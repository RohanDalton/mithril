import threading

import pytest
import redis

from mithril._fencing import (
    FileFencingTokenProvider,
    InMemoryFencingTokenProvider,
    RedisFencingGuard,
)

__author__ = "Rohan B. Dalton"


def test_in_memory_tokens_are_monotonic_under_concurrency() -> None:
    """
    GIVEN: Many threads contending for the same lock name
    WHEN: Each thread requests tokens repeatedly
    THEN: All issued tokens are unique and strictly increasing
    """
    provider = InMemoryFencingTokenProvider()
    lock_name = "anduril"
    tokens: list[int] = []
    tokens_lock = threading.Lock()

    def worker() -> None:
        local_tokens = [provider.next_token(lock_name) for _ in range(100)]
        with tokens_lock:
            tokens.extend(local_tokens)

    threads = [threading.Thread(target=worker) for _ in range(100)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(tokens) == 10000
    assert len(set(tokens)) == 10000
    assert sorted(tokens) == list(range(1, 10001))


def test_current_token_does_not_increment() -> None:
    """
    GIVEN: A fencing token provider with issued tokens
    WHEN: current_token is called
    THEN: The token is reported without incrementing
    """
    provider = InMemoryFencingTokenProvider()
    lock_name = "glamdring"

    assert provider.current_token(lock_name) == 0
    assert provider.next_token(lock_name) == 1
    assert provider.current_token(lock_name) == 1
    assert provider.next_token(lock_name) == 2
    assert provider.current_token(lock_name) == 2


def test_file_provider_persists_tokens_between_instances(tmp_path) -> None:
    """
    GIVEN: A file-backed fencing token provider
    WHEN: A new instance reads the same lock name
    THEN: The highest token is preserved across instances
    """
    provider = FileFencingTokenProvider(tmp_path, namespace="mithril")
    lock_name = "sting"

    assert provider.next_token(lock_name) == 1
    assert provider.next_token(lock_name) == 2

    second = FileFencingTokenProvider(tmp_path, namespace="mithril")
    assert second.current_token(lock_name) == 2
    assert second.next_token(lock_name) == 3


@pytest.fixture()
def redis_guard() -> RedisFencingGuard:
    """
    GIVEN: A local Redis instance for fencing guard tests
    WHEN: A guard is created
    THEN: The Redis fencing guard is ready to use
    """
    client = redis.Redis(host="localhost", port=6379, db=15, decode_responses=True)
    client.flushdb()
    return RedisFencingGuard(client, namespace="test")


def test_redis_fencing_guard_accepts_monotonic_tokens(redis_guard: RedisFencingGuard) -> None:
    """
    GIVEN: A Redis fencing guard
    WHEN: Tokens are increasing
    THEN: Each token is accepted and stored
    """
    assert redis_guard.validate_and_update("lock", 1) is True
    assert redis_guard.validate_and_update("lock", 2) is True
    assert redis_guard.validate_and_update("lock", 3) is True


def test_redis_fencing_guard_rejects_stale_tokens(redis_guard: RedisFencingGuard) -> None:
    """
    GIVEN: A Redis fencing guard with a stored max token
    WHEN: A stale token is provided
    THEN: The token is rejected
    """
    assert redis_guard.validate_and_update("lock", 5) is True
    assert redis_guard.validate_and_update("lock", 4) is False


if __name__ == "__main__":
    pass
