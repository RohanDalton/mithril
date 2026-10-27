"""Demonstrate a fenced publication using only a temporary local SQLite file."""

from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import create_engine, text

from mithril import Lock, StaleTokenError
from mithril.backends import SQLiteBackend


def publish_snapshot(engine, token, snapshot):
    """Accept at most one publication per token for this resource."""
    with engine.begin() as connection:
        accepted = connection.execute(
            text(
                "UPDATE reports SET snapshot = :snapshot, fence = :token "
                "WHERE resource_id = :resource_id AND fence < :token "
                "RETURNING resource_id"
            ),
            {
                "resource_id": token.lock_name,
                "token": token.fencing_token,
                "snapshot": snapshot,
            },
        ).scalar_one_or_none()
        if accepted is None:
            raise StaleTokenError("Publication refused: obsolete or already used token")


def main():
    with TemporaryDirectory(prefix="mithril-fencing-") as directory:
        database = Path(directory) / "demo.sqlite3"
        engine = create_engine(f"sqlite:///{database}", connect_args={"timeout": 1})
        try:
            backend = SQLiteBackend(engine)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE TABLE reports (resource_id TEXT PRIMARY KEY, "
                        "snapshot TEXT NOT NULL, fence INTEGER NOT NULL DEFAULT 0)"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO reports(resource_id, snapshot) "
                        "VALUES ('reports:daily', 'initial')"
                    )
                )

            with Lock("daily", namespace="reports", backend=backend) as first:
                publish_snapshot(engine, first, "first publication")

            with Lock("daily", namespace="reports", backend=backend) as second:
                publish_snapshot(engine, second, "second publication")

            print(f"Accepted tokens: {first.fencing_token}, {second.fencing_token}")
            try:
                publish_snapshot(engine, first, "obsolete publication")
            except StaleTokenError:
                print(f"Rejected obsolete token: {first.fencing_token}")
            else:
                raise RuntimeError("The destination accepted an obsolete token")

            with engine.connect() as connection:
                snapshot = connection.execute(
                    text("SELECT snapshot FROM reports WHERE resource_id = 'reports:daily'")
                ).scalar_one()
            if snapshot != "second publication":
                raise RuntimeError("The accepted snapshot changed unexpectedly")
            print(f"Final snapshot: {snapshot}")
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
