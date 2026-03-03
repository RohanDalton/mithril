import multiprocessing
import time

import pytest

from mithril._types import LockMode, Result
from mithril.backends._file import FileBackend

__author__ = "Rohan B. Dalton"


def test_file_backend_acquire_release(tmp_path) -> None:
    """
    GIVEN: A file backend and a lock name
    WHEN: acquire and release are called
    THEN: The lock is acquired and released successfully
    """
    backend = FileBackend(tmp_path)
    result = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)
    assert result is not None

    assert backend.is_locked("lock") is True
    assert backend.release("lock", "owner") == Result.SUCCESS
    assert backend.is_locked("lock") is False


def test_file_backend_rejects_wrong_owner(tmp_path) -> None:
    """
    GIVEN: A lock owned by one process
    WHEN: Another owner attempts release
    THEN: The release fails
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)
    assert backend.release("lock", "other") == Result.WRONG_OWNER


def test_file_backend_ttl_expiry(tmp_path) -> None:
    """
    GIVEN: A lock with a short TTL
    WHEN: The TTL expires
    THEN: The lock is considered not owned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "owner", ttl_seconds=0.05, epoch=1)
    time.sleep(0.08)
    assert backend.get_owner("lock") is None


def test_file_backend_fencing_tokens_increment(tmp_path) -> None:
    """
    GIVEN: A file backend
    WHEN: Multiple acquisitions occur
    THEN: Fencing tokens are monotonically increasing
    """
    backend = FileBackend(tmp_path)
    token1, _epoch1 = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)
    backend.release("lock", "owner")
    token2, _epoch2 = backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=2)

    assert token2 > token1


def test_file_backend_rejects_symlink_path(tmp_path) -> None:
    """
    GIVEN: A lock path that is a symlink
    WHEN: acquire is called
    THEN: A ValueError is raised
    """
    backend = FileBackend(tmp_path)
    target = tmp_path / "target.lock"
    target.write_text("data")
    link = tmp_path / "mithril.lock.lock"
    link.symlink_to(target)

    with pytest.raises(ValueError):
        backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)


def _worker_hold_lock(
    directory: str, started: multiprocessing.Event, release: multiprocessing.Event, queue
) -> None:
    backend = FileBackend(directory)
    result = backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    queue.put(("acquired", result is not None))
    started.set()
    release.wait(5.0)
    backend.release("lock", "owner")


def _worker_contend_lock(
    directory: str,
    started: multiprocessing.Event,
    release: multiprocessing.Event,
    queue,
) -> None:
    backend = FileBackend(directory)
    started.wait(5.0)
    first = backend.acquire("lock", "other", ttl_seconds=1.0, epoch=1)
    queue.put(("first_attempt", first))
    release.wait(5.0)
    second = None
    deadline = time.time() + 2.0
    while second is None and time.time() < deadline:
        second = backend.acquire("lock", "other", ttl_seconds=1.0, epoch=2)
        if second is None:
            time.sleep(0.01)
    queue.put(("second_attempt", second))
    if second is not None:
        backend.release("lock", "other")


def test_file_backend_multiprocess_contention(tmp_path) -> None:
    """
    GIVEN: Two processes contending for the same lock
    WHEN: One holds the lock and then releases it
    THEN: The other cannot acquire until after release
    """
    directory = str(tmp_path)
    started = multiprocessing.Event()
    release = multiprocessing.Event()
    queue = multiprocessing.Queue()

    holder = multiprocessing.Process(
        target=_worker_hold_lock,
        args=(directory, started, release, queue),
    )
    contender = multiprocessing.Process(
        target=_worker_contend_lock,
        args=(directory, started, release, queue),
    )

    holder.start()
    contender.start()

    label, acquired = queue.get(timeout=5.0)
    assert label == "acquired"
    assert acquired is True

    label, first_attempt = queue.get(timeout=5.0)
    assert label == "first_attempt"
    assert first_attempt is None

    release.set()

    label, second_attempt = queue.get(timeout=5.0)
    assert label == "second_attempt"
    assert second_attempt is not None

    holder.join(timeout=5.0)
    contender.join(timeout=5.0)
    assert holder.exitcode == 0
    assert contender.exitcode == 0


def test_exclusive_acquire_blocked_by_shared_readers(tmp_path) -> None:
    """
    GIVEN: A shared reader holds the lock
    WHEN: An exclusive acquire is attempted
    THEN: The exclusive acquire fails
    """
    reader = FileBackend(tmp_path)
    writer = FileBackend(tmp_path)

    r = reader.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    assert r is not None

    w = writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=2, mode=LockMode.EXCLUSIVE)
    assert w is None

    assert reader.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS


def test_shared_acquire_blocked_by_exclusive_writer(tmp_path) -> None:
    """
    GIVEN: An exclusive writer holds the lock
    WHEN: A shared acquire is attempted
    THEN: The shared acquire fails
    """
    writer = FileBackend(tmp_path)
    reader = FileBackend(tmp_path)

    w = writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=1, mode=LockMode.EXCLUSIVE)
    assert w is not None

    r = reader.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
    assert r is None

    assert writer.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_allows_multiple_readers(tmp_path) -> None:
    """
    GIVEN: A file backend
    WHEN: Two readers acquire shared locks
    THEN: Both acquisitions succeed
    """
    r1 = FileBackend(tmp_path)
    r2 = FileBackend(tmp_path)

    res1 = r1.acquire("lock", "reader1", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    res2 = r2.acquire("lock", "reader2", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
    assert res1 is not None
    assert res2 is not None

    assert r1.release("lock", "reader1", mode=LockMode.SHARED) == Result.SUCCESS
    assert r2.release("lock", "reader2", mode=LockMode.SHARED) == Result.SUCCESS


def test_exclusive_succeeds_after_readers_release(tmp_path) -> None:
    """
    GIVEN: Shared readers that have released
    WHEN: An exclusive acquire is attempted
    THEN: The exclusive acquire succeeds
    """
    reader = FileBackend(tmp_path)
    writer = FileBackend(tmp_path)

    r = reader.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    assert r is not None
    assert reader.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS

    w = writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=2, mode=LockMode.EXCLUSIVE)
    assert w is not None

    assert writer.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_reentrant_count_blocks_writer_until_released(tmp_path) -> None:
    """
    GIVEN: A shared reader acquires the same lock twice
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer is blocked until both shared releases occur
    """
    reader = FileBackend(tmp_path)
    writer = FileBackend(tmp_path)

    r1 = reader.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    r2 = reader.acquire("lock", "reader", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
    assert r1 is not None
    assert r2 is not None

    assert (
        writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=3, mode=LockMode.EXCLUSIVE)
        is None
    )

    assert reader.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=4, mode=LockMode.EXCLUSIVE)
        is None
    )

    assert reader.release("lock", "reader", mode=LockMode.SHARED) == Result.SUCCESS
    assert (
        writer.acquire("lock", "writer", ttl_seconds=5.0, epoch=5, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert writer.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_expiry_allows_writer(tmp_path) -> None:
    """
    GIVEN: A shared reader with an expired TTL
    WHEN: An exclusive writer attempts acquisition
    THEN: The writer can acquire the lock
    """
    reader = FileBackend(tmp_path)
    writer = FileBackend(tmp_path)

    assert (
        reader.acquire("lock", "reader", ttl_seconds=0.05, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    time.sleep(0.08)
    assert (
        writer.acquire("lock", "writer", ttl_seconds=1.0, epoch=2, mode=LockMode.EXCLUSIVE)
        is not None
    )
    assert writer.release("lock", "writer", mode=LockMode.EXCLUSIVE) == Result.SUCCESS


def test_shared_release_wrong_owner_reports_not_owner(tmp_path) -> None:
    """
    GIVEN: A shared lock held by another reader
    WHEN: A wrong owner attempts release
    THEN: Release reports not-owner
    """
    reader1 = FileBackend(tmp_path)
    reader2 = FileBackend(tmp_path)

    assert (
        reader1.acquire("lock", "reader1", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
        is not None
    )
    assert (
        reader2.acquire("lock", "reader2", ttl_seconds=5.0, epoch=2, mode=LockMode.SHARED)
        is not None
    )

    assert reader1.release("lock", "other", mode=LockMode.SHARED) == Result.WRONG_OWNER

    assert reader1.release("lock", "reader1", mode=LockMode.SHARED) == Result.SUCCESS
    assert reader2.release("lock", "reader2", mode=LockMode.SHARED) == Result.SUCCESS


def test_file_renew_success(tmp_path) -> None:
    """
    GIVEN: An acquired file lock
    WHEN: renew is called by the same owner
    THEN: Result.SUCCESS is returned and the lock is still held
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "owner", ttl_seconds=1.0, epoch=1)
    assert backend.renew("lock", "owner", ttl_seconds=5.0) == Result.SUCCESS
    assert backend.is_locked("lock") is True


def test_file_renew_wrong_owner(tmp_path) -> None:
    """
    GIVEN: A lock acquired by owner A
    WHEN: owner B attempts to renew
    THEN: Result.WRONG_OWNER is returned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "A", ttl_seconds=1.0, epoch=1)
    assert backend.renew("lock", "B", ttl_seconds=5.0) == Result.WRONG_OWNER


def test_file_renew_nonexistent(tmp_path) -> None:
    """
    GIVEN: No lock has been acquired
    WHEN: renew is called
    THEN: Result.NOT_FOUND is returned
    """
    backend = FileBackend(tmp_path)
    assert backend.renew("lock", "owner", ttl_seconds=5.0) == Result.NOT_FOUND


def test_file_release_nonexistent(tmp_path) -> None:
    """
    GIVEN: No lock file exists
    WHEN: release is called
    THEN: Result.NOT_FOUND is returned
    """
    backend = FileBackend(tmp_path)
    assert backend.release("lock", "owner") == Result.NOT_FOUND


def test_file_get_owner_returns_owner(tmp_path) -> None:
    """
    GIVEN: A lock acquired by a known owner
    WHEN: get_owner is called
    THEN: The owner id is returned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert backend.get_owner("lock") == "owner"


def test_file_get_owner_none_when_expired(tmp_path) -> None:
    """
    GIVEN: A lock with a short TTL that has expired
    WHEN: get_owner is called
    THEN: None is returned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "owner", ttl_seconds=0.05, epoch=1)
    time.sleep(0.08)
    assert backend.get_owner("lock") is None


def test_file_is_locked_true_then_false(tmp_path) -> None:
    """
    GIVEN: A file backend
    WHEN: A lock is acquired then released
    THEN: is_locked reports True then False
    """
    backend = FileBackend(tmp_path)
    assert backend.is_locked("lock") is False
    backend.acquire("lock", "owner", ttl_seconds=5.0, epoch=1)
    assert backend.is_locked("lock") is True
    backend.release("lock", "owner")
    assert backend.is_locked("lock") is False


def test_file_shared_renew_success(tmp_path) -> None:
    """
    GIVEN: A shared lock acquired by a reader
    WHEN: renew is called by the same reader
    THEN: Result.SUCCESS is returned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "reader", ttl_seconds=1.0, epoch=1, mode=LockMode.SHARED)
    assert (
        backend.renew("lock", "reader", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.SUCCESS
    )


def test_file_shared_renew_wrong_owner(tmp_path) -> None:
    """
    GIVEN: A shared lock held by one reader
    WHEN: Another owner attempts to renew
    THEN: Result.WRONG_OWNER is returned
    """
    backend = FileBackend(tmp_path)
    backend.acquire("lock", "reader", ttl_seconds=5.0, epoch=1, mode=LockMode.SHARED)
    assert (
        backend.renew("lock", "other", ttl_seconds=5.0, mode=LockMode.SHARED)
        == Result.WRONG_OWNER
    )


if __name__ == "__main__":
    pass
