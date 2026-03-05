import contextlib
import uuid

import psycopg
import psycopg.errors
import pytest

from iron_sql import runtime
from iron_sql.runtime import NoRowsError
from iron_sql.runtime import TooManyRowsError
from tests.conftest import ProjectBuilder


async def test_result_shapes(test_project: ProjectBuilder) -> None:
    get_users_sql = "SELECT id FROM users ORDER BY created_at"
    test_project.add_query("get_users", get_users_sql)

    get_user_full_sql = "SELECT * FROM users WHERE id = $1"
    test_project.add_query("get_user_full", get_user_full_sql)

    get_user_mini_sql = "SELECT id, username FROM users WHERE id=$1"
    test_project.add_query(
        "get_user_mini",
        get_user_mini_sql,
        row_type="UserMini",
    )

    mod = test_project.generate()

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


async def test_basic_execution(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username, is_active) VALUES ($1, $2, $3)"
    select_sql = "SELECT id, username, is_active FROM users WHERE id = $1"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    uid = uuid.uuid4()

    await mod.testdb_sql(insert_sql).execute(uid, "testuser", True)

    row = await mod.testdb_sql(select_sql).query_single_row(uid)

    assert row.id == uid
    assert row.username == "testuser"
    assert row.is_active is True


async def test_jsonb_roundtrip(test_project: ProjectBuilder) -> None:
    sql = (
        "INSERT INTO users (id, username, metadata) "
        "VALUES ($1, $2, $3) RETURNING metadata"
    )
    test_project.add_query("q", sql)

    mod = test_project.generate()
    uid = uuid.uuid4()
    data = {"key": "value", "list": [1, 2], "nested": {"a": 1}}

    res = await mod.testdb_sql(sql).query_single_row(uid, "json_user", data)
    assert res == data


async def test_json_roundtrip(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO json_payloads (payload) VALUES ($1) RETURNING payload"
    test_project.add_query("q", insert_sql)

    mod = test_project.generate()
    data = {"key": "value", "list": [1, 2]}

    res = await mod.testdb_sql(insert_sql).query_single_row(data)
    assert res == data


def test_unknown_statement_dispatch(test_project: ProjectBuilder) -> None:
    test_project.add_query("q1", "SELECT 1")
    mod = test_project.generate()
    with pytest.raises(KeyError, match="Unknown statement"):
        mod.testdb_sql("SELECT 42")


async def test_runtime_context_pool(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    mod = test_project.generate()

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


async def test_runtime_errors(test_project: ProjectBuilder) -> None:
    select_sql = "SELECT * FROM users WHERE username = $1"
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"

    test_project.add_query("sel", select_sql)
    test_project.add_query("ins", insert_sql)

    mod = test_project.generate()
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


async def test_transaction_commit(test_project: ProjectBuilder) -> None:
    insert = "INSERT INTO users (id, username) VALUES ($1, 'tx_user')"
    select = "SELECT count(*) as cnt FROM users WHERE username = 'tx_user'"

    test_project.add_query("i", insert)
    test_project.add_query("s", select)

    mod = test_project.generate()
    uid = uuid.uuid4()

    async with mod.testdb_transaction():
        await mod.testdb_sql(insert).execute(uid)

    row = await mod.testdb_sql(select).query_single_row()
    assert row == 1


async def test_ensure_transaction_error_state(test_project: ProjectBuilder) -> None:
    test_project.add_query("sel", "SELECT 1")

    mod = test_project.generate()

    async with mod.testdb_connection() as conn:
        await conn.execute("BEGIN")
        with contextlib.suppress(psycopg.errors.DivisionByZero):
            await conn.execute("SELECT 1 / 0")

        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR

        with pytest.raises(psycopg.InterfaceError, match="INERROR"):
            async with runtime._ensure_transaction(conn):  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                pass


async def test_with_connection(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT username FROM users WHERE id = $1"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()
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


async def test_transaction_rollback(test_project: ProjectBuilder) -> None:
    insert = "INSERT INTO users (id, username) VALUES ($1, 'rollback_user')"
    select = "SELECT count(*) as cnt FROM users WHERE username = 'rollback_user'"

    test_project.add_query("i", insert)
    test_project.add_query("s", select)

    mod = test_project.generate()
    uid = uuid.uuid4()

    try:
        async with mod.testdb_transaction():
            await mod.testdb_sql(insert).execute(uid)
            raise RuntimeError  # noqa: TRY301
    except RuntimeError:
        pass

    row = await mod.testdb_sql(select).query_single_row()
    assert row == 0
