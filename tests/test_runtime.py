import uuid

import pytest

from iron_sql.runtime import NoRowsError
from iron_sql.runtime import TooManyRowsError
from tests.conftest import ProjectBuilder


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


async def test_runtime_errors(test_project: ProjectBuilder):
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


async def test_transaction_commit(test_project: ProjectBuilder):
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


async def test_transaction_rollback(test_project: ProjectBuilder):
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
