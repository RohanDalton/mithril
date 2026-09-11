"""
SQLite backend for Mithril.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from .._types import Result
from ._database import DatabaseBackend

__author__ = "Rohan B. Dalton"


_NOW_EXPR = "(julianday('now') - 2440587.5) * 86400.0"
_SQLITE_BUSY = 5


def _is_busy(exc: OperationalError) -> bool:
    """
    Return True if the exception is a SQLite BUSY error.
    """
    if orig := exc.orig:
        if code := getattr(orig, "sqlite_errorcode", None):
            return code == _SQLITE_BUSY
        return "database is locked" in str(orig)
    return False


# Release / renew operations are handled with separate statements in Python
# because Python's sqlite3 module does not support DML (INSERT/UPDATE/DELETE)
# inside CTEs.
class SQLiteQueries:
    # Exclusive acquire – single DML main statement (INSERT into mithril_locks).
    # The fencing token is computed inline; the fencing table is synced
    # afterwards in Python (see _SYNC_FENCING_SQL).
    ACQUIRE_EXCLUSIVE = text(
        f"""
WITH now(now_ts) AS (SELECT {_NOW_EXPR})
INSERT INTO mithril_locks(name, owner, fencing_token, epoch, acquired_at, expire_at)
SELECT :lock_name, :owner_id,
       COALESCE((SELECT token FROM mithril_fencing WHERE name = :lock_name), 0) + 1,
       COALESCE((SELECT token FROM mithril_fencing WHERE name = :lock_name), 0) + 1,
       now.now_ts, now.now_ts + :ttl_seconds
FROM now
WHERE NOT EXISTS (
    SELECT 1 FROM mithril_locks
    WHERE name = :lock_name AND expire_at > (SELECT now_ts FROM now)
)
AND NOT EXISTS (
    SELECT 1 FROM mithril_readers
    WHERE name = :lock_name AND expire_at > (SELECT now_ts FROM now)
)
ON CONFLICT(name) DO UPDATE SET
    owner = excluded.owner,
    fencing_token = excluded.fencing_token,
    epoch = excluded.epoch,
    acquired_at = excluded.acquired_at,
    expire_at = excluded.expire_at
WHERE mithril_locks.expire_at <= excluded.acquired_at
  AND NOT EXISTS (
    SELECT 1 FROM mithril_readers
    WHERE name = :lock_name AND expire_at > excluded.acquired_at
  )
RETURNING fencing_token, epoch
"""
    )
    ACQUIRE_SHARED = text(
        f"""
    WITH now(now_ts) AS (SELECT {_NOW_EXPR})
    INSERT INTO mithril_readers(name, owner, count, expire_at)
    SELECT :lock_name, :owner_id, 1, now.now_ts + :ttl_seconds
    FROM now
    WHERE NOT EXISTS (
        SELECT 1 FROM mithril_locks
        WHERE name = :lock_name AND expire_at > now.now_ts
    )
    ON CONFLICT(name, owner) DO UPDATE SET
        count = CASE
            WHEN mithril_readers.expire_at <= (SELECT now_ts FROM now) THEN 1
            ELSE mithril_readers.count + 1
        END,
        expire_at = (SELECT now_ts FROM now) + :ttl_seconds
    RETURNING 1
    """
    )
    IS_LOCKED = [
        text(
            f"SELECT 1 FROM mithril_locks WHERE name = :lock_name AND expire_at > {_NOW_EXPR}"
        ),
        text(
            f"SELECT 1 FROM mithril_readers WHERE name = :lock_name AND expire_at > {_NOW_EXPR}"
        ),
    ]
    RELEASE_EXCLUSIVE = text(
        "DELETE FROM mithril_locks WHERE name = :lock_name AND owner = :owner_id"
    )
    RELEASE_SHARED = [
        text(
            "UPDATE mithril_readers SET count = count - 1 "
            "WHERE name = :lock_name AND owner = :owner_id AND count > 1"
        ),
        text(
            f"DELETE FROM mithril_readers "
            f"WHERE name = :lock_name AND owner = :owner_id "
            f"AND (count <= 1 OR expire_at <= {_NOW_EXPR})"
        ),
        text("SELECT 1 FROM mithril_readers WHERE name = :lock_name AND owner = :owner_id"),
    ]
    RENEW_EXCLUSIVE = text(
        f"UPDATE mithril_locks "
        f"SET expire_at = {_NOW_EXPR} + :ttl_seconds "
        f"WHERE name = :lock_name AND owner = :owner_id "
        f"AND expire_at > {_NOW_EXPR}"
    )
    RENEW_SHARED = text(
        f"UPDATE mithril_readers "
        f"SET expire_at = {_NOW_EXPR} + :ttl_seconds "
        f"WHERE name = :lock_name AND owner = :owner_id "
        f"AND expire_at > {_NOW_EXPR}"
    )
    SCHEMA = [
        """
        PRAGMA journal_mode=WAL
        """,
        """
        CREATE TABLE IF NOT EXISTS mithril_locks (
            name TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            fencing_token INTEGER NOT NULL,
            epoch INTEGER NOT NULL,
            acquired_at REAL NOT NULL,
            expire_at REAL NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mithril_fencing (
            name TEXT PRIMARY KEY,
            token INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mithril_readers (
            name TEXT NOT NULL,
            owner TEXT NOT NULL,
            count INTEGER NOT NULL,
            expire_at REAL NOT NULL,
            PRIMARY KEY (name, owner)
        )
        """,
    ]

    SELECT_OWNER = text(
        f"SELECT owner FROM mithril_locks WHERE name = :lock_name AND expire_at > {_NOW_EXPR}"
    )


# Sync fencing table after a successful exclusive acquire.
_SYNC_FENCING_SQL = text(
    """
INSERT INTO mithril_fencing(name, token) VALUES (:lock_name, :token)
ON CONFLICT(name) DO UPDATE SET token = :token
"""
)

# Increment fencing token after a successful shared acquire.
_INCREMENT_FENCING_SQL = text(
    """
INSERT INTO mithril_fencing(name, token) VALUES (:lock_name, 1)
ON CONFLICT(name) DO UPDATE SET token = mithril_fencing.token + 1
RETURNING token
"""
)


class SQLiteBackend(DatabaseBackend):
    """
    SQLite backend for single-host distributed locks.
    """

    _QUERIES = SQLiteQueries

    def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        try:
            with self._transaction() as transaction:
                statement = self._QUERIES.ACQUIRE_EXCLUSIVE
                params = {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "ttl_seconds": ttl_seconds,
                    "epoch": epoch,
                }
                if row := self._fetchone(
                    statement=statement, params=params, transaction=transaction
                ):
                    fencing_token, epoch = int(row[0]), int(row[1])
                    statement = _SYNC_FENCING_SQL
                    params = ({"lock_name": lock_name, "token": fencing_token},)
                    transaction.execute(statement, params)
                    return fencing_token, epoch
                else:
                    return None
        except OperationalError as exc:
            if _is_busy(exc):
                return None
            raise

    def _acquire_shared(
        self,
        lock_name: str,
        owner_id: str,
        ttl_seconds: float,
        epoch: int,
    ) -> tuple[int, int] | None:
        try:
            with self._transaction() as transaction:
                query = self._QUERIES.ACQUIRE_SHARED
                params = {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "ttl_seconds": ttl_seconds,
                    "epoch": epoch,
                }
                if self._fetchone(query, params, transaction):
                    query = _INCREMENT_FENCING_SQL
                    params = {"lock_name": lock_name}
                    row = self._fetchone(query, params, transaction)
                    return int(row[0]), int(row[0])
                else:
                    return None
        except OperationalError as exc:
            if _is_busy(exc):
                return None
            raise

    def _release_exclusive(self, lock_name: str, owner_id: int) -> Result:
        statement = self._QUERIES.RELEASE_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id}
        with self._transaction() as transaction:
            result = transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                statement = text("SELECT 1 FROM mithril_locks WHERE name = :lock_name")
                if self._fetchone(statement=statement, params=params, transaction=transaction):
                    return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND

    def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        params = {"lock_name": lock_name, "owner_id": owner_id}
        statements = self._QUERIES.RELEASE_SHARED
        dml_statements = statements[:2]
        with self._transaction() as transaction:
            for dml_statement in dml_statements:
                result = transaction.execute(dml_statement, params)
                if result.rowcount:
                    return Result.SUCCESS
            select = statements[2]
            if self._fetchone(statement=select, params=params, transaction=transaction):
                return Result.WRONG_OWNER
            else:
                return Result.NOT_FOUND

    def _renew_exclusive(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": ttl_seconds}
        with self._transaction() as transaction:
            result = transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                query = text("SELECT owner FROM mithril_locks WHERE name = :lock_name")
                if row := self._fetchone(query, params=params, transaction=transaction):
                    if row[0] == owner_id:
                        return Result.NOT_FOUND
                    else:
                        return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND

    def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_SHARED
        params = {"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": ttl_seconds}
        with self._transaction() as transaction:
            result = transaction.execute(statement, params)
            if result.rowcount:
                return Result.SUCCESS
            else:
                # TODO: Dedupe this query
                query = text(
                    "SELECT 1 FROM mithril_readers "
                    "WHERE name = :lock_name AND owner = :owner_id"
                )
                if self._fetchone(query, params=params, transaction=transaction):
                    return Result.WRONG_OWNER
                else:
                    return Result.NOT_FOUND


if __name__ == "__main__":
    pass
