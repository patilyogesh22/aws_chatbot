"""
app/db.py

PostgreSQL connection pool for long-running FastAPI/API process.

Use in FastAPI routes and services.
For one-off scripts like migrations, normal psycopg2.connect is still fine.
"""

from contextlib import contextmanager
from psycopg2 import pool

from app.config import PG_DSN


_pool = None


def get_pool():
    global _pool

    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=PG_DSN,
        )

    return _pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    if conn:
        get_pool().putconn(conn)


@contextmanager
def get_db_connection():
    conn = get_conn()

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_conn(conn)


def close_pool():
    global _pool

    if _pool is not None:
        _pool.closeall()
        _pool = None
