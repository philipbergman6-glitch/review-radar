"""One place to open a PostgreSQL connection with the project's credentials."""
from __future__ import annotations

import psycopg

from src.common import config as C


def dsn() -> str:
    return (f"host={C.PG_HOST} port={C.PG_PORT} dbname={C.PG_DB} "
            f"user={C.PG_USER} password={C.PG_PASSWORD}")


def connect(*, autocommit: bool = False) -> psycopg.Connection:
    """A connection whose context manager commits on success and rolls back on error."""
    return psycopg.connect(dsn(), connect_timeout=10, autocommit=autocommit)
