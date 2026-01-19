import uuid

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


async def test_basic_execution(test_project: ProjectBuilder):
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
