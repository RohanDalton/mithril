"""
Async Postgres backend for Mithril.
"""

from __future__ import annotations

from typing import Iterable

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from ..._types import Result
from ...aio._retry import AsyncRetryPolicy
from ...exceptions import BackendTimeoutError
from .._postgres import PostgresQueries, _is_transaction_conflict
from ._database import AsyncDatabaseBackend

__author__ = "Rohan B. Dalton"


class AsyncPostgresBackend(AsyncDatabaseBackend):
    """
    Async Postgres backend for distributed locks.
    """

    _QUERIES = PostgresQueries
    _isolation_level = "SERIALIZABLE"

    async def initialize(self) -> None:
        """
        Set SERIALIZABLE isolation and create the schema tables.
        """
        await self._execute_many(self._QUERIES.SCHEMA)

    async def _fetchone(self, statement, params, transaction=None):
        """
        Retry rolled-back release, renewal, and introspection transactions.
        """
        if transaction is not None or statement in (
            self._QUERIES.ACQUIRE_EXCLUSIVE,
            self._QUERIES.ACQUIRE_SHARED,
        ):
            return await super()._fetchone(statement, params, transaction)
        retry = AsyncRetryPolicy(timeout=1, base_delay=0.001, max_delay=0.025, jitter=0.001)
        failure = None
        async for _ in retry.attempts():
            try:
                return await super()._fetchone(statement, params)
            except DBAPIError as exc:
                if not _is_transaction_conflict(exc):
                    raise
                failure = exc
        raise BackendTimeoutError(
            "Postgres transaction conflicts exceeded the retry budget"
        ) from failure

    async def _do_acquire(self, query, params) -> tuple[int, int] | None:
        try:
            if row := await self._fetchone(query, params):
                return int(row[0]), int(row[1])
            else:
                return None
        except DBAPIError as exc:
            if _is_transaction_conflict(exc):
                return None
            raise

    async def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        # asyncpg requires str for ``(:ttl_seconds || ' seconds')::interval``
        params = {
            "lock_name": lock_name,
            "owner_id": owner_id,
            "ttl_seconds": str(ttl_seconds),
        }
        return await self._do_acquire(self._QUERIES.ACQUIRE_EXCLUSIVE, params)

    async def _acquire_shared(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        # asyncpg infers a string parameter from the interval expression.
        params = {
            "lock_name": lock_name,
            "owner_id": owner_id,
            "ttl_seconds": str(ttl_seconds),
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
        async with self._engine.begin() as connection:
            for statement in statements:
                await connection.execute(text(statement))


if __name__ == "__main__":
    pass
