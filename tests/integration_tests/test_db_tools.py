"""Integration tests for db_tools — requires real database credentials."""

import os

import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def pg_credentials() -> dict[str, str]:
    """Load Postgres credentials from environment."""
    required = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        pytest.skip(f"Missing env vars: {', '.join(missing)}")

    return {
        "host": os.environ["POSTGRES_HOST"],
        "db": os.environ["POSTGRES_DB"],
        "usr": os.environ["POSTGRES_USER"],
        "pwd": os.environ["POSTGRES_PASSWORD"],
        "port": os.environ.get("POSTGRES_PORT", "5432"),
    }


def test_postgres_connection(pg_credentials: dict[str, str]) -> None:
    """Test real Postgres connection."""
    from fusetools.db_tools import Postgres

    cursor, conn = Postgres.con_postgres(**pg_credentials)
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result == (1,)
    conn.close()
