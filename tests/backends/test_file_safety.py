import multiprocessing
import os
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from mithril import Lock, _file_io
from mithril._fencing import FileFencingTokenProvider
from mithril._types import LockMode, Result
from mithril.backends._file import FileBackend
from mithril.exceptions import FencingTokenError, LockExpiredError


def _hold_until_terminated(directory, ready):
    backend = FileBackend(directory)
    backend.acquire("lock", "child", 30)
    ready.set()
    time.sleep(30)


def test_expired_holder_does_not_prevent_takeover(tmp_path, monkeypatch):
    """
    GIVEN: A file lease whose holder remains alive past its TTL
    WHEN: Another backend acquires without the old holder inspecting its state
    THEN: Ownership transfers and stale cleanup cannot release the new holder
    """
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    first = FileBackend(tmp_path)
    second = FileBackend(tmp_path)
    lock = Lock("lock", backend=first, ttl=1)
    token = lock.acquire()
    now[0] = 2.0
    assert (replacement := second.acquire("lock", "second", 10))
    assert replacement[0] > token.fencing_token
    with pytest.raises(LockExpiredError):
        lock.release()
    assert first._held == {}
    assert second.get_owner("lock") == "second"
    assert second.release("lock", "second") == Result.SUCCESS


def test_process_crash_releases_file_lease(tmp_path):
    """
    GIVEN: A child process holding a long-lived file lease
    WHEN: The process is terminated before its TTL expires
    THEN: Another process can acquire immediately with a newer fencing token
    """
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_until_terminated, args=(tmp_path, ready))
    process.start()
    try:
        assert ready.wait(10)
        backend = FileBackend(tmp_path)
        assert backend.acquire("lock", "parent", 10) is None
        process.terminate()
        process.join(10)
        assert not process.is_alive()
        assert (result := backend.acquire("lock", "parent", 10))
        assert result == (2, 2)
        assert backend.release("lock", "parent") == Result.SUCCESS
    finally:
        if process.is_alive():
            process.terminate()
        process.join(10)


def test_shared_introspection_and_wrong_mode_cleanup(tmp_path):
    """
    GIVEN: A shared file lease
    WHEN: Introspection and an exclusive release are attempted
    THEN: The reader remains locked and retains its liveness handle
    """
    backend = FileBackend(tmp_path)
    assert backend.acquire("lock", "reader", 5, mode=LockMode.SHARED)
    assert backend.is_locked("lock")
    assert backend.release("lock", "reader") == Result.NOT_FOUND
    assert FileBackend(tmp_path).is_locked("lock")
    assert backend.release("lock", "reader", LockMode.SHARED) == Result.SUCCESS


def test_fencing_rejects_symlink(tmp_path):
    """
    GIVEN: A fencing path replaced by a symlink to another file
    WHEN: A new token is requested
    THEN: The request fails without modifying the target
    """
    target = tmp_path / "target"
    target.write_text("41")
    (tmp_path / "mithril.lock.fence").symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        FileFencingTokenProvider(tmp_path).next_token("lock")
    assert target.read_text() == "41"


@pytest.mark.parametrize("contents", ["", "broken", "-1", "1.5"])
def test_corrupt_fencing_counter_fails_closed(tmp_path, contents):
    """
    GIVEN: A corrupted persistent fencing counter
    WHEN: The next token is requested
    THEN: The provider raises instead of restarting the counter
    """
    path = tmp_path / "mithril.lock.fence"
    path.write_text(contents)
    provider = FileFencingTokenProvider(tmp_path)
    with pytest.raises(FencingTokenError):
        provider.next_token("lock")
    assert path.read_text() == contents


def test_fencing_failed_replace_preserves_counter(tmp_path, monkeypatch):
    """
    GIVEN: A persisted fencing counter
    WHEN: Atomic replacement fails while issuing the next token
    THEN: The old counter remains intact and can be safely retried
    """
    provider = FileFencingTokenProvider(tmp_path)
    assert provider.next_token("lock") == 1

    def fail_replace(*args):
        raise OSError("disk failure")

    with monkeypatch.context() as patch:
        patch.setattr(_file_io.os, "replace", fail_replace)
        with pytest.raises(OSError, match="disk failure"):
            provider.next_token("lock")
    assert provider.current_token("lock") == 1
    assert provider.next_token("lock") == 2


def test_fencing_serializes_independent_providers(tmp_path):
    """
    GIVEN: Independent providers sharing one persistent counter
    WHEN: Threads request tokens concurrently
    THEN: Every token is unique and a new provider observes the final counter
    """

    def next_token(_):
        return FileFencingTokenProvider(tmp_path).next_token("lock")

    with ThreadPoolExecutor(max_workers=8) as executor:
        tokens = list(executor.map(next_token, range(100)))
    assert sorted(tokens) == list(range(1, 101))
    assert FileFencingTokenProvider(tmp_path).current_token("lock") == 100


def test_windows_mutex_always_locks_first_byte(tmp_path, monkeypatch):
    """
    GIVEN: A growing file and the Windows locking implementation
    WHEN: The mutex is locked and unlocked after seeking to the end
    THEN: Both operations target byte zero
    """
    offsets = []

    def locking(fd, mode, length):
        offsets.append(os.lseek(fd, 0, os.SEEK_CUR))
        assert length == 1

    windows = SimpleNamespace(LK_LOCK=1, LK_NBLCK=2, LK_UNLCK=3, locking=locking)
    monkeypatch.setattr(_file_io, "msvcrt", windows, raising=False)
    monkeypatch.setattr(_file_io, "os", SimpleNamespace(name="nt", SEEK_END=os.SEEK_END))
    with (tmp_path / "mutex").open("w+") as handle:
        handle.write("metadata")
        handle.flush()
        assert _file_io.lock_file(handle, blocking=False)
        handle.seek(0, os.SEEK_END)
        _file_io.unlock_file(handle)
    assert offsets == [0, 0]
