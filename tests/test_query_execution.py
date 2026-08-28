import contextlib
import uuid
from collections.abc import AsyncGenerator

import psycopg
import psycopg.errors
import pytest

from iron_sql import runtime
from iron_sql.runtime import NoRowsError
from iron_sql.runtime import TooManyRowsError
from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import generated_package

generated_package(
    "query_execution",
    schema=SCHEMA_SQL,
    queries='''
        from tests.generated.query_execution.testdb import testdb_sql

        testdb_sql("SELECT id FROM users ORDER BY created_at")
        testdb_sql("SELECT * FROM users WHERE id = $1")
        testdb_sql("SELECT id, username FROM users WHERE id=$1", row_type="UserMini")
        testdb_sql("INSERT INTO users (id, username, is_active) VALUES ($1, $2, $3)")
        testdb_sql("SELECT id, username, is_active FROM users WHERE id = $1")
        testdb_sql("""INSERT INTO users (id, username, metadata)
        VALUES ($1, $2, $3) RETURNING metadata""")
        testdb_sql("INSERT INTO json_payloads (payload) VALUES ($1) RETURNING payload")
        testdb_sql("SELECT 1")
        testdb_sql("SELECT * FROM users WHERE username = $1")
        testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
        testdb_sql("INSERT INTO users (id, username) VALUES ($1, 'tx_user')")
        testdb_sql("SELECT count(*) as cnt FROM users WHERE username = 'tx_user'")
        testdb_sql("SELECT username FROM users WHERE id = $1")
        testdb_sql("INSERT INTO users (id, username) VALUES ($1, 'rollback_user')")
        testdb_sql("SELECT count(*) as cnt FROM users WHERE username = 'rollback_user'")
    ''',
)

from tests.generated.query_execution import testdb


@pytest.fixture(autouse=True)
async def use_generated_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("query_execution"):
        yield


async def test_result_shapes() -> None:
    get_users_sql = "SELECT id FROM users ORDER BY created_at"

    get_user_full_sql = "SELECT * FROM users WHERE id = $1"

    get_user_mini_sql = "SELECT id, username FROM users WHERE id=$1"
    mod = testdb

    id1 = uuid.uuid4()
    id2 = uuid.uuid4()

    async with mod.testdb_connection() as conn:
        await conn.execute("INSERT INTO users (id, username) VALUES (%s, 'u1')", (id1,))
        await conn.execute("INSERT INTO users (id, username) VALUES (%s, 'u2')", (id2,))

    rows = await mod.testdb_sql(get_users_sql).query_all_rows()
    assert len(rows) == 2
    assert isinstance(rows[0], uuid.UUID)

    user = await mod.testdb_sql(get_user_full_sql).query_single_row(id1)

    assert type(user).__name__ == "TestdbUser"
    assert user.id == id1

    mini = await mod.testdb_sql(
        get_user_mini_sql, row_type="UserMini"
    ).query_single_row(id1)
    assert type(mini).__name__ == "UserMini"
    assert mini.id == id1
    assert mini.username == "u1"


async def test_basic_execution() -> None:
    insert_sql = "INSERT INTO users (id, username, is_active) VALUES ($1, $2, $3)"
    select_sql = "SELECT id, username, is_active FROM users WHERE id = $1"

    mod = testdb

    uid = uuid.uuid4()

    await mod.testdb_sql(insert_sql).execute(uid, "testuser", True)

    row = await mod.testdb_sql(select_sql).query_single_row(uid)

    assert row.id == uid
    assert row.username == "testuser"
    assert row.is_active is True


async def test_jsonb_roundtrip() -> None:
    sql = """INSERT INTO users (id, username, metadata)
VALUES ($1, $2, $3) RETURNING metadata"""
    mod = testdb
    uid = uuid.uuid4()
    data = {"key": "value", "list": [1, 2], "nested": {"a": 1}}

    res = await mod.testdb_sql(sql).query_single_row(uid, "json_user", data)
    assert res == data


async def test_json_roundtrip() -> None:
    insert_sql = "INSERT INTO json_payloads (payload) VALUES ($1) RETURNING payload"
    mod = testdb
    data = {"key": "value", "list": [1, 2]}

    res = await mod.testdb_sql(insert_sql).query_single_row(data)
    assert res == data


def test_unknown_statement_dispatch() -> None:
    mod = testdb
    with pytest.raises(KeyError, match="Unknown statement"):
        mod.testdb_sql("SELECT 42")


async def test_runtime_context_pool() -> None:
    mod = testdb

    # Nested connection reuse
    async with mod.testdb_connection() as c1, mod.testdb_connection() as c2:
        assert c1 is c2

    await mod.testdb_sql("SELECT 1").query_single_row()

    pool = mod.TESTDB_POOL
    old_inner = pool.psycopg_pool
    await pool.close()

    await mod.testdb_sql("SELECT 1").query_single_row()
    assert pool.psycopg_pool is not old_inner

    pool.psycopg_pool.get_stats()


async def test_runtime_errors() -> None:
    select_sql = "SELECT * FROM users WHERE username = $1"
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"

    mod = testdb
    uid1 = uuid.uuid4()
    uid2 = uuid.uuid4()

    with pytest.raises(NoRowsError):
        await mod.testdb_sql(select_sql).query_single_row("missing")

    res = await mod.testdb_sql(select_sql).query_optional_row("missing")
    assert res is None

    await mod.testdb_sql(insert_sql).execute(uid1, "duplicate")
    await mod.testdb_sql(insert_sql).execute(uid2, "duplicate")

    with pytest.raises(TooManyRowsError):
        await mod.testdb_sql(select_sql).query_single_row("duplicate")

    with pytest.raises(TooManyRowsError):
        await mod.testdb_sql(select_sql).query_optional_row("duplicate")


async def test_transaction_commit() -> None:
    insert = "INSERT INTO users (id, username) VALUES ($1, 'tx_user')"
    select = "SELECT count(*) as cnt FROM users WHERE username = 'tx_user'"

    mod = testdb
    uid = uuid.uuid4()

    async with mod.testdb_transaction():
        await mod.testdb_sql(insert).execute(uid)

    row = await mod.testdb_sql(select).query_single_row()
    assert row == 1


async def test_ensure_transaction_error_state() -> None:
    mod = testdb

    async with mod.testdb_connection() as conn:
        await conn.execute("BEGIN")
        with contextlib.suppress(psycopg.errors.DivisionByZero):
            await conn.execute("SELECT 1 / 0")

        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR

        with pytest.raises(psycopg.InterfaceError, match="INERROR"):
            async with runtime._ensure_transaction(conn):  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                pass


async def test_with_connection() -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT username FROM users WHERE id = $1"

    mod = testdb
    uid = uuid.uuid4()

    async with mod.TESTDB_POOL.connection() as conn:
        async with conn.transaction():
            await (
                mod
                .testdb_sql(insert_sql)
                .with_connection(conn)
                .execute(uid, "explicit")
            )

        row = (
            await mod.testdb_sql(select_sql).with_connection(conn).query_single_row(uid)
        )
        assert row == "explicit"

    # Original query object is not affected
    row2 = await mod.testdb_sql(select_sql).query_single_row(uid)
    assert row2 == "explicit"


async def test_transaction_rollback() -> None:
    insert = "INSERT INTO users (id, username) VALUES ($1, 'rollback_user')"
    select = "SELECT count(*) as cnt FROM users WHERE username = 'rollback_user'"

    mod = testdb
    uid = uuid.uuid4()

    try:
        async with mod.testdb_transaction():
            await mod.testdb_sql(insert).execute(uid)
            raise RuntimeError  # noqa: TRY301
    except RuntimeError:
        pass

    row = await mod.testdb_sql(select).query_single_row()
    assert row == 0
