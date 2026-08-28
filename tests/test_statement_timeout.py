import asyncio
import uuid
from collections.abc import AsyncGenerator

import psycopg
import pytest

from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import generated_package

_INSERT_SQL = "INSERT INTO users (id, username) VALUES ($1, $2)"
_STREAM_SQL = "SELECT id FROM users ORDER BY username"
_SLEEP_SQL = "SELECT 1 AS n FROM pg_sleep(1)"

generated_package(
    "statement_timeout",
    schema=SCHEMA_SQL,
    queries="""
        from tests.generated.statement_timeout.testdb import testdb_sql

        testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
        testdb_sql("SELECT id FROM users ORDER BY username")
        testdb_sql("SELECT 1 AS n FROM pg_sleep(1)")
    """,
    pool_options={"kwargs": {"options": "-c statement_timeout=200"}},
)

from tests.generated.statement_timeout import testdb


@pytest.fixture(autouse=True)
async def use_generated_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("statement_timeout"):
        yield


async def test_statement_timeout_cancels_a_slow_statement() -> None:
    mod = testdb

    with pytest.raises(psycopg.errors.QueryCanceled):
        await mod.testdb_sql(_SLEEP_SQL).query_single_row()


async def test_statement_timeout_does_not_cancel_a_slow_stream() -> None:
    mod = testdb

    for i in range(5):
        await mod.testdb_sql(_INSERT_SQL).execute(uuid.uuid4(), f"user_{i}")

    streamed: list[uuid.UUID] = []
    async with mod.testdb_sql(_STREAM_SQL).query_stream() as rows:
        async for row in rows:
            await asyncio.sleep(0.1)
            streamed.append(row)

    assert len(streamed) == 5
