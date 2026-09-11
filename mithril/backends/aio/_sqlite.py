"""
Async SQLite backend for Mithril.
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from ..._types import Result
from .._sqlite import _INCREMENT_FENCING_SQL, _SYNC_FENCING_SQL, SQLiteQueries, _is_busy
from ._database import AsyncDatabaseBackend

__author__ = "Rohan B. Dalton"


class AsyncSQLiteBackend(AsyncDatabaseBackend):
    """
    Async SQLite backend for single-host distributed locks.
    """

    _QUERIES = SQLiteQueries

    async def _acquire_exclusive(
        self, lock_name: str, owner_id: int, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        try:
            async with self._transaction() as transaction:
                statement = self._QUERIES.ACQUIRE_EXCLUSIVE
                params = {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "ttl_seconds": ttl_seconds,
                    "epoch": epoch,
                }
                if row := await self._fetchone(
                    statement=statement, params=params, transaction=transaction
                ):
                    fencing_token, epoch = int(row[0]), int(row[1])
                    statement = _SYNC_FENCING_SQL
                    params = ({"lock_name": lock_name, "token": fencing_token},)
                    await transaction.execute(statement, params)
                    return fencing_token, epoch
                else:
                    return None
        except OperationalError as exc:
            if _is_busy(exc):
                return None
            raise

    async def _acquire_shared(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
    ) -> tuple[int, int] | None:
        try:
            async with self._transaction() as transaction:
                query = self._QUERIES.ACQUIRE_SHARED
                params = {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "ttl_seconds": ttl_seconds,
                    "epoch": epoch,
                }
                if await self._fetchone(query, params, transaction):
                    query = _INCREMENT_FENCING_SQL
                    params = {"lock_name": lock_name}
                    row = await self._fetchone(query, params, transaction)
                    return int(row[0]), int(row[0])
                else:
                    return None
        except OperationalError as exc:
            if _is_busy(exc):
                return None
            raise

    async def _release_exclusive(self, lock_name: str, owner_id: int) -> Result:
        statement = self._QUERIES.RELEASE_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id}
        async with self._transaction() as transaction:
            result = await transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                statement = text("SELECT 1 FROM mithril_locks WHERE name = :lock_name")
                if await self._fetchone(
                    statement=statement, params=params, transaction=transaction
                ):
                    return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND

    async def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        params = {"lock_name": lock_name, "owner_id": owner_id}
        statements = self._QUERIES.RELEASE_SHARED
        dml_statements = statements[:2]
        async with self._transaction() as transaction:
            for dml_statement in dml_statements:
                result = await transaction.execute(dml_statement, params)
                if result.rowcount:
                    return Result.SUCCESS
            select = statements[2]
            if await self._fetchone(statement=select, params=params, transaction=transaction):
                return Result.WRONG_OWNER
            else:
                return Result.NOT_FOUND

    async def _renew_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float
    ) -> Result:
        statement = self._QUERIES.RENEW_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": ttl_seconds}
        async with self._transaction() as transaction:
            result = await transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                query = text("SELECT owner FROM mithril_locks WHERE name = :lock_name")
                if row := await self._fetchone(query, params=params, transaction=transaction):
                    if row[0] == owner_id:
                        return Result.NOT_FOUND
                    else:
                        return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND

    async def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_SHARED
        params = {"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": ttl_seconds}
        async with self._transaction() as transaction:
            result = await transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                # TODO: Dedupe this query
                query = text(
                    "SELECT 1 FROM mithril_readers "
                    "WHERE name = :lock_name AND owner = :owner_id"
                )
                if await self._fetchone(query, params=params, transaction=transaction):
                    return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND

    async def _execute_many(self, statements: Iterable[str]) -> None:
        async with self._engine.begin() as connection:
            for statement in statements:
                await connection.execute(text(statement))


if __name__ == "__main__":
    pass
