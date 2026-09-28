import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator

import psycopg
import psycopg.errors
import pytest

from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import generated_package

generated_package(
    "transaction_state",
    schema=SCHEMA_SQL,
    queries="""
        from tests.generated.transaction_state.testdb import testdb_sql

        testdb_sql("INSERT INTO users (id, username) VALUES ($1, 'streamed')")
        testdb_sql("SELECT id FROM users")
    """,
)

from tests.generated.transaction_state import testdb


@pytest.fixture(autouse=True)
async def use_generated_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("transaction_state"):
        yield


async def test_is_in_transaction_follows_transaction_status() -> None:
    assert not await testdb.testdb_is_in_transaction()

    async with testdb.testdb_connection():
        assert not await testdb.testdb_is_in_transaction()

        async with testdb.testdb_transaction():
            assert await testdb.testdb_is_in_transaction()

            async with testdb.testdb_transaction():
                assert await testdb.testdb_is_in_transaction()

            assert await testdb.testdb_is_in_transaction()

        assert not await testdb.testdb_is_in_transaction()

    assert not await testdb.testdb_is_in_transaction()


async def test_is_in_transaction_is_true_in_failed_transaction() -> None:
    async with testdb.testdb_connection() as conn:
        async with testdb.testdb_transaction():
            with contextlib.suppress(psycopg.errors.DivisionByZero):
                await conn.execute("SELECT 1 / 0")
            assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
            assert await testdb.testdb_is_in_transaction()

        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        assert not await testdb.testdb_is_in_transaction()


async def statement_in_progress(
    conn: psycopg.AsyncConnection[object],
) -> asyncio.Task[object]:
    task = asyncio.create_task(conn.execute("SELECT pg_sleep(0.2)"))
    await asyncio.sleep(0)
    assert conn.info.transaction_status == psycopg.pq.TransactionStatus.ACTIVE
    return task


async def test_is_in_transaction_waits_for_statement_in_transaction() -> None:
    async with testdb.testdb_transaction(), testdb.testdb_connection() as conn:
        concurrent = await statement_in_progress(conn)
        assert await testdb.testdb_is_in_transaction()
        assert concurrent.done()


async def test_is_in_transaction_waits_for_statement_on_bare_connection() -> None:
    async with testdb.testdb_connection() as conn:
        concurrent = await statement_in_progress(conn)
        assert not await testdb.testdb_is_in_transaction()
        assert concurrent.done()


async def test_is_in_transaction_refuses_to_guess_on_closed_connection() -> None:
    async with testdb.testdb_connection() as conn:
        await conn.close()
        with pytest.raises(psycopg.InterfaceError, match="UNKNOWN"):
            await testdb.testdb_is_in_transaction()


async def test_is_in_transaction_is_true_while_stream_is_iterated() -> None:
    insert = "INSERT INTO users (id, username) VALUES ($1, 'streamed')"
    await testdb.testdb_sql(insert).execute(uuid.uuid4())

    async with testdb.testdb_sql("SELECT id FROM users").query_stream() as rows:
        observed = [await testdb.testdb_is_in_transaction() async for _ in rows]

    assert observed == [True]
    assert not await testdb.testdb_is_in_transaction()
