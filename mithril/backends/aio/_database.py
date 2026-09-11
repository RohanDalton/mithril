from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from ... import LockMode
from ..._types import Result, validate_duration
from .._database import DatabaseBackend, validate_database_pool
from .._redis_common import validate_mode

__author__ = "Rohan B. Dalton"


class AsyncDatabaseBackend(DatabaseBackend):
    def __init__(self, connection: Any) -> None:
        # Skip the sync _ensure_schema() call from DatabaseBackend.__init__.
        # Callers must use ``await backend.initialize()`` instead.
        self._connection = connection
        self._engine = connection if isinstance(connection, AsyncEngine) else connection.engine
        validate_database_pool(self._engine.sync_engine)

    async def initialize(self) -> None:
        """
        Create the schema tables. Must be awaited after construction.
        """
        await self._execute_many(self._QUERIES.SCHEMA)

    @abstractmethod
    async def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        raise NotImplementedError

    @abstractmethod
    async def _acquire_shared(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        raise NotImplementedError()

    @abstractmethod
    async def _release_exclusive(self, lock_name: str, owner_id: str) -> Result:
        raise NotImplementedError()

    @abstractmethod
    async def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        raise NotImplementedError()

    @abstractmethod
    async def _renew_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float
    ) -> Result:
        raise NotImplementedError()

    @abstractmethod
    async def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        raise NotImplementedError()

    async def acquire(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int = 0,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> tuple[int, int] | None:
        """
        Acquire the lock and return (fencing_token, epoch) on success.
        """
        validate_mode(mode)
        validate_duration(ttl_seconds, "ttl_seconds")
        if mode == LockMode.EXCLUSIVE:
            return await self._acquire_exclusive(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds, epoch=epoch
            )
        else:
            return await self._acquire_shared(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds, epoch=epoch
            )

    async def is_locked(self, lock_name: str) -> bool:
        """
        Return True if the lock is currently held.
        """
        params = {"lock_name": lock_name}
        async with self._transaction() as transaction:
            for query in self._QUERIES.IS_LOCKED:
                if await self._fetchone(query, params, transaction=transaction):
                    return True
        return False

    async def get_owner(self, lock_name: str) -> str | None:
        """
        Return the current owner id if the lock is held.
        """
        params = {"lock_name": lock_name}
        if row := await self._fetchone(self._QUERIES.SELECT_OWNER, params):
            return str(row[0])
        else:
            return None

    async def release(
        self,
        lock_name: str,
        owner_id: str,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        """
        Release the lock, returning Result.SUCCESS on success.
        """
        validate_mode(mode)
        if mode == LockMode.EXCLUSIVE:
            return await self._release_exclusive(lock_name=lock_name, owner_id=owner_id)
        else:
            return await self._release_shared(lock_name=lock_name, owner_id=owner_id)

    async def renew(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        mode: LockMode = LockMode.EXCLUSIVE,
    ) -> Result:
        validate_mode(mode)
        validate_duration(ttl_seconds, "ttl_seconds")

        if mode == LockMode.EXCLUSIVE:
            return await self._renew_exclusive(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds
            )
        else:
            return await self._renew_shared(
                lock_name=lock_name, owner_id=owner_id, ttl_seconds=ttl_seconds
            )

    async def _fetchone(
        self,
        statement: Any,
        params: dict[str, Any],
        transaction: Any | None = None,
    ) -> tuple[Any, ...] | None:
        if transaction is None:
            async with self._transaction() as connection:
                result = await connection.execute(statement, params)
                return result.fetchone()
        result = await transaction.execute(statement, params)
        return result.fetchone()

    @asynccontextmanager
    async def _transaction(self) -> Iterator[Any]:
        async with self._engine.connect() as connection:
            if self._isolation_level:
                connection = await connection.execution_options(
                    isolation_level=self._isolation_level
                )
            async with connection.begin():
                if connection.dialect.name == "sqlite":
                    await connection.exec_driver_sql("BEGIN IMMEDIATE")
                yield connection


if __name__ == "__main__":
    pass
