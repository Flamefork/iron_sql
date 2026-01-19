import uuid

import pytest

from iron_sql.runtime import NoRowsError
from iron_sql.runtime import TooManyRowsError
from tests.conftest import ProjectBuilder


async def test_codegen_e2e(test_project: ProjectBuilder):
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


async def test_jsonb_roundtrip(test_project: ProjectBuilder):
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
