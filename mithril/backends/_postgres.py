"""
Postgres backend for Mithril.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from .._retry import RetryPolicy
from .._types import Result
from ..exceptions import BackendTimeoutError
from ._database import DatabaseBackend

__author__ = "Rohan B. Dalton"


def _is_transaction_conflict(exc: BaseException) -> bool:
    """
    Recognize rolled-back conflicts, including asyncpg commit-error wrappers.
    """
    while exc is not None:
        if (getattr(exc, "sqlstate", None) or getattr(exc, "pgcode", None)) in {
            "40001",
            "40P01",
        }:
            return True
        exc = getattr(exc, "orig", None) or exc.__cause__
    return False


class PostgresQueries:
    ACQUIRE_EXCLUSIVE = text("""
WITH token AS (
    INSERT INTO mithril_fencing(name, token)
    VALUES (:lock_name, 1)
    ON CONFLICT (name) DO UPDATE SET token = CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM mithril_locks
            WHERE name = :lock_name AND expire_at > NOW()
        )
        AND NOT EXISTS (
            SELECT 1 FROM mithril_readers
            WHERE name = :lock_name AND expire_at > NOW()
        )
        THEN mithril_fencing.token + 1
        ELSE mithril_fencing.token
    END
    RETURNING token
),
upsert AS (
    INSERT INTO mithril_locks(name, owner, fencing_token, epoch, acquired_at, expire_at)
    SELECT :lock_name, :owner_id, token.token, token.token, NOW(),
           NOW() + (:ttl_seconds || ' seconds')::interval
    FROM token
    WHERE NOT EXISTS (
        SELECT 1 FROM mithril_readers
        WHERE name = :lock_name AND expire_at > NOW()
    )
    ON CONFLICT (name) DO UPDATE SET
        owner = EXCLUDED.owner,
        fencing_token = EXCLUDED.fencing_token,
        epoch = EXCLUDED.epoch,
        acquired_at = EXCLUDED.acquired_at,
        expire_at = EXCLUDED.expire_at
    WHERE mithril_locks.expire_at <= NOW()
      AND NOT EXISTS (
        SELECT 1 FROM mithril_readers
        WHERE name = :lock_name AND expire_at > NOW()
      )
    RETURNING fencing_token, epoch
)
SELECT fencing_token, epoch FROM upsert
""")
    ACQUIRE_SHARED = text("""
WITH token AS (
    INSERT INTO mithril_fencing(name, token)
    VALUES (:lock_name, 1)
    ON CONFLICT (name) DO UPDATE SET token = CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM mithril_locks
            WHERE name = :lock_name AND expire_at > NOW()
        )
        THEN mithril_fencing.token + 1
        ELSE mithril_fencing.token
    END
    RETURNING token
),
reader_upsert AS (
    INSERT INTO mithril_readers(name, owner, count, expire_at)
    SELECT :lock_name, :owner_id, 1, NOW() + (:ttl_seconds || ' seconds')::interval
    WHERE NOT EXISTS (
        SELECT 1 FROM mithril_locks
        WHERE name = :lock_name AND expire_at > NOW()
    )
    ON CONFLICT (name, owner) DO UPDATE SET
        count = CASE
            WHEN mithril_readers.expire_at <= NOW() THEN 1
            ELSE mithril_readers.count + 1
        END,
        expire_at = NOW() + (:ttl_seconds || ' seconds')::interval
    RETURNING 1
)
SELECT token.token, token.token FROM token WHERE EXISTS (SELECT 1 FROM reader_upsert)
""")
    IS_LOCKED = [
        text("SELECT 1 FROM mithril_locks WHERE name = :lock_name AND expire_at > NOW()"),
        text("SELECT 1 FROM mithril_readers WHERE name = :lock_name AND expire_at > NOW()"),
    ]
    RELEASE_EXCLUSIVE = text(
        """
WITH current AS (
    SELECT owner FROM mithril_locks WHERE name = :lock_name
),
deleted AS (
    DELETE FROM mithril_locks WHERE name = :lock_name AND owner = :owner_id RETURNING 1
)
SELECT CASE
    WHEN EXISTS(SELECT 1 FROM deleted) THEN 0
    WHEN EXISTS(SELECT 1 FROM current) THEN 1
    ELSE 2
END
"""
    )
    RELEASE_SHARED = text(
        """
WITH current AS (
    SELECT count, expire_at FROM mithril_readers
    WHERE name = :lock_name AND owner = :owner_id
),
updated AS (
    UPDATE mithril_readers
    SET count = count - 1
    WHERE name = :lock_name AND owner = :owner_id AND count > 1 AND expire_at > NOW()
    RETURNING 1
),
deleted AS (
    DELETE FROM mithril_readers
    WHERE name = :lock_name AND owner = :owner_id
      AND (count <= 1 OR expire_at <= NOW())
    RETURNING 1
)
SELECT CASE
    WHEN EXISTS(SELECT 1 FROM updated) OR EXISTS(SELECT 1 FROM deleted) THEN 0
    WHEN EXISTS(SELECT 1 FROM current) THEN 1
    ELSE 2
END
"""
    )
    RENEW_EXCLUSIVE = text(
        """
    WITH current AS (
        SELECT owner, expire_at FROM mithril_locks WHERE name = :lock_name
    ),
    updated AS (
        UPDATE mithril_locks
        SET expire_at = NOW() + (:ttl_seconds || ' seconds')::interval
        WHERE name = :lock_name AND owner = :owner_id AND expire_at > NOW()
        RETURNING 1
    )
    SELECT CASE
        WHEN EXISTS(SELECT 1 FROM updated) THEN 0
        WHEN EXISTS(SELECT 1 FROM current) THEN
            CASE WHEN (SELECT owner FROM current) != :owner_id THEN 1 ELSE 2 END
        ELSE 2
    END
    """
    )
    RENEW_SHARED = text(
        """
    WITH updated AS (
        UPDATE mithril_readers
        SET expire_at = NOW() + (:ttl_seconds || ' seconds')::interval
        WHERE name = :lock_name AND owner = :owner_id AND expire_at > NOW()
        RETURNING 1
    ),
    current AS (
        SELECT count FROM mithril_readers WHERE name = :lock_name AND owner = :owner_id
    )
    SELECT CASE
        WHEN EXISTS(SELECT 1 FROM updated) THEN 0
        WHEN EXISTS(SELECT 1 FROM current) THEN 1
        ELSE 2
    END
    """
    )
    SCHEMA = [
        """
        CREATE TABLE IF NOT EXISTS mithril_locks (
            name TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            fencing_token BIGINT NOT NULL,
            epoch BIGINT NOT NULL,
            acquired_at TIMESTAMPTZ NOT NULL,
            expire_at TIMESTAMPTZ NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mithril_fencing (
            name TEXT PRIMARY KEY,
            token BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mithril_readers (
            name TEXT NOT NULL,
            owner TEXT NOT NULL,
            count BIGINT NOT NULL,
            expire_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (name, owner)
        )
        """,
    ]
    SELECT_OWNER = text(
        "SELECT owner FROM mithril_locks WHERE name = :lock_name AND expire_at > NOW()"
    )


class PostgresBackend(DatabaseBackend):
    """
    Postgres backend for distributed locks.
    """

    _QUERIES = PostgresQueries
    _isolation_level = "SERIALIZABLE"

    def _fetchone(self, statement, params, transaction=None):
        if transaction is not None or statement in (
            self._QUERIES.ACQUIRE_EXCLUSIVE,
            self._QUERIES.ACQUIRE_SHARED,
        ):
            return super()._fetchone(statement, params, transaction)
        retry = RetryPolicy(timeout=1, base_delay=0.001, max_delay=0.025, jitter=0.001)
        failure = None
        for _ in retry.attempts():
            try:
                return super()._fetchone(statement, params)
            except DBAPIError as exc:
                if not _is_transaction_conflict(exc):
                    raise
                failure = exc
        raise BackendTimeoutError(
            "Postgres transaction conflicts exceeded the retry budget"
        ) from failure

    def _renew(self, statement, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        params = ({"lock_name": lock_name, "owner_id": owner_id, "ttl_seconds": ttl_seconds},)
        if row := self._fetchone(statement=statement, params=params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND

    def _renew_exclusive(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_EXCLUSIVE
        return self._renew(
            statement=statement,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
        )

    def _renew_shared(self, lock_name: str, owner_id: str, ttl_seconds: float) -> Result:
        statement = self._QUERIES.RENEW_SHARED
        return self._renew(
            statement=statement,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
        )

    def _acquire(
        self, query, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        params = {
            "lock_name": lock_name,
            "owner_id": owner_id,
            "ttl_seconds": ttl_seconds,
            "epoch": epoch,
        }
        try:
            if row := self._fetchone(query, params):
                return int(row[0]), int(row[1])
            else:
                return None
        except DBAPIError as exc:
            if _is_transaction_conflict(exc):
                return None
            raise

    def _acquire_exclusive(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        query = self._QUERIES.ACQUIRE_EXCLUSIVE
        return self._acquire(
            query=query,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
            epoch=epoch,
        )

    def _acquire_shared(
        self, lock_name: str, owner_id: str, ttl_seconds: float, epoch: int
    ) -> tuple[int, int] | None:
        query = self._QUERIES.ACQUIRE_SHARED
        return self._acquire(
            query=query,
            lock_name=lock_name,
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
            epoch=epoch,
        )

    def _release_exclusive(self, lock_name: str, owner_id: str) -> Result:
        query = self._QUERIES.RELEASE_EXCLUSIVE
        params = {"lock_name": lock_name, "owner_id": owner_id}
        if row := self._fetchone(query, params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND

    def _release_shared(self, lock_name: str, owner_id: str) -> Result:
        query = self._QUERIES.RELEASE_SHARED
        params = {"lock_name": lock_name, "owner_id": owner_id}
        if row := self._fetchone(query, params):
            return Result(int(row[0]))
        else:
            return Result.NOT_FOUND


if __name__ == "__main__":
    pass
