"""
Async Postgres backend for Mithril.
"""

from __future__ import annotations

from typing import Iterable

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from ..._types import Result
from .._postgres import PostgresQueries, _SERIALIZATION_FAILURE
from ._database import AsyncDatabaseBackend

__author__ = "Rohan B. Dalton"


class AsyncPostgresBackend(AsyncDatabaseBackend):
    """
    Async Postgres backend for distributed locks.
    """

    _QUERIES = PostgresQueries

    async def initialize(self) -> None:
        """
        Set SERIALIZABLE isolation and create the schema tables.
        """
        self._connection = await self._connection.execution_options(
            isolation_level="SERIALIZABLE"
        )
        await self._execute_many(self._QUERIES.SCHEMA)

    @staticmethod
    def _is_serialization_failure(exc: OperationalError) -> bool:
        """
        Return True if the exception is a Postgres serialization failure.
        """
        if orig := exc.orig:
            if code := getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None):
                return code == _SERIALIZATION_FAILURE
        return False

    async def _do_acquire(self, query, params) -> tuple[int, int] | None:
        try:
            if row := await self._fetchone(query, params):
                return int(row[0]), int(row[1])
            else:
                return None
        except OperationalError as exc:
            if self._is_serialization_failure(exc):
                return None
            raise

    async def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        # asyncpg requires str for ``(:ttl_seconds || ' seconds')::interval``
        # but int for ``epoch`` (INSERT into BIGINT column).
        params = {
            "lock_name": lock_name,
            "owner_id": owner_id,
            "ttl_seconds": str(ttl_seconds),
            "epoch": epoch,
        }
        return await self._do_acquire(self._QUERIES.ACQUIRE_EXCLUSIVE, params)

    async def _acquire_shared(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        # asyncpg requires str for both ``ttl_seconds`` and ``epoch`` here:
        # ``epoch`` appears in a bare ``SELECT :epoch`` without column context.
        params = {
            "lock_name": lock_name,
            "owner_id": owner_id,
            "ttl_seconds": str(ttl_seconds),
            "epoch": str(epoch),
        }
        return await self._do_acquire(self._QUERIES.ACQUIRE_SHARED, params)

    async def _release_exclusive(self, lock_name: str, owner_id: str) -> Result:
        query = self._QUERIES.RELEASE_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id}
        if row := await self._fetchone(query, params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND

    async def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        query = self._QUERIES.RELEASE_SHARED
        params = {"lock_name": lock_name, "owner_id": owner_id}
        if row := await self._fetchone(query, params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND

    async def _renew(
        self, statement, lock_name: str, owner_id: str, ttl_seconds: float
    ) -> Result:
        params = (
            {"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": str(ttl_seconds)},
        )
        if row := await self._fetchone(statement=statement, params=params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND

    async def _renew_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float
    ) -> Result:
        statement = self._QUERIES.RENEW_EXCLUSIVE
        return await self._renew(
            statement=statement,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
        )

    async def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_SHARED
        return await self._renew(
            statement=statement,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
        )

    async def _execute_many(self, statements: Iterable[str]) -> None:
        async with self._connection.begin():
            for statement in statements:
                await self._connection.execute(text(statement))


if __name__ == "__main__":
    pass
