from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import ClassVar

from sqlalchemy import TextClause, text

from .._types import LockMode, Result

__author__ = "Rohan B. Dalton"


class DatabaseQueries:
    ACQUIRE_EXCLUSIVE = text("")
    ACQUIRE_SHARED = text("")
    IS_LOCKED: ClassVar[list[TextClause]] = []
    RELEASE_EXCLUSIVE = text("")
    RELEASE_SHARED: ClassVar[TextClause | list[TextClause]] = text("")
    RENEW_EXCLUSIVE = text("")
    RENEW_SHARED = text("")
    SCHEMA: ClassVar[list[TextClause]] = []
    SELECT_OWNER = text("")


class DatabaseBackend(ABC):
    _QUERIES: ClassVar[DatabaseQueries] = DatabaseQueries

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._ensure_schema()

    @abstractmethod
    def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        raise NotImplementedError

    @abstractmethod
    def _acquire_shared(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        raise NotImplementedError()

    @abstractmethod
    def _release_exclusive(self, lock_name: str, owner_id: str) -> Result:
        raise NotImplementedError()

    @abstractmethod
    def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        raise NotImplementedError()

    @abstractmethod
    def _renew_exclusive(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        raise NotImplementedError()

    @abstractmethod
    def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        raise NotImplementedError()

    def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | None:
        """
        Acquire the lock and return (fencing_token, epoch) on success.
        """
        if mode == LockMode.EXCLUSIVE:
            if ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be > 0")
            return self._acquire_exclusive(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds, epoch=epoch
            )
        else:
            return self._acquire_shared(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds, epoch=epoch
            )

    def is_locked(self, lock_name: str) -> bool:
        """
        Return True if the lock is currently held.
        """
        params = {"lock_name": lock_name}
        with self._transaction() as transaction:
            for query in self._QUERIES.IS_LOCKED:
                if self._fetchone(query, params, transaction=transaction):
                    return True
        return False

    def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if the lock is held.
        """
        params = {"lock_name": lock_name}
        if row := self._fetchone(self._QUERIES.SELECT_OWNER, params):
            return str(row[0])
        else:
            return None

    def release(
        self,
        lock_name: str,
        owner_id: str,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Release the lock, returning Result.SUCCESS on success.
        """
        if mode == LockMode.EXCLUSIVE:
            return self._release_exclusive(lock_name=lock_name, owner_id=owner_id)
        else:
            return self._release_shared(lock_name=lock_name, owner_id=owner_id)

    def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")

        if mode == LockMode.EXCLUSIVE:
            return self._renew_exclusive(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds
            )
        else:
            return self._renew_shared(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds
            )

    def _ensure_schema(self) -> None:
        self._execute_many(self._QUERIES.SCHEMA)

    def _execute_many(self, statements: Iterable[str]) -> None:
        with self._connection.begin():
            for statement in statements:
                self._connection.execute(text(statement))

    def _fetchone(
        self,
        statement: Any,
        params: dict[str, Any],
        transaction: Any | None = None,
    ) -> tuple[Any, ...] | None:
        if transaction is None:
            with self._connection.begin():
                result = self._connection.execute(statement, params)
                return result.fetchone()
        result = transaction.execute(statement, params)
        return result.fetchone()

    @contextmanager
    def _transaction(self) -> Iterator[Any]:
        with self._connection.begin():
            yield self._connection


if __name__ == "__main__":
    pass
