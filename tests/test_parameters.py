import inspect
import uuid

from tests.conftest import ProjectBuilder


async def test_parameters_named(test_project: ProjectBuilder) -> None:
    insert_sql = (
        "INSERT INTO users (id, username, is_active) VALUES (@id, @username, @active)"
    )
    test_project.add_query("insert_user", insert_sql)
    mod = test_project.generate()

    uid = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(id=uid, username="e1_user", active=True)

    sig = inspect.signature(mod.testdb_sql(insert_sql).__class__.execute)
    params = list(sig.parameters.values())
    assert params[1].kind == inspect.Parameter.KEYWORD_ONLY


async def test_parameters_mixed(test_project: ProjectBuilder) -> None:
    select_mixed_sql = "SELECT id FROM users WHERE id = $1 AND username = @username"
    test_project.add_query("select_mixed", select_mixed_sql)
    mod = test_project.generate()

    uid = uuid.uuid4()
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO users (id, username) VALUES (%s, 'e1_user')", (uid,)
        )

    row = await mod.testdb_sql(select_mixed_sql).query_single_row(
        uid, username="e1_user"
    )
    assert row == uid

    sig_mixed = inspect.signature(
        mod.testdb_sql(select_mixed_sql).__class__.query_single_row
    )
    params_mixed = list(sig_mixed.parameters.values())
    # 0=self, 1=param_1 (POSITIONAL_OR_KEYWORD), 2=username (KEYWORD_ONLY)
    assert params_mixed[1].kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert params_mixed[2].kind == inspect.Parameter.KEYWORD_ONLY


async def test_parameters_optional(test_project: ProjectBuilder) -> None:
    select_opt_sql = "SELECT count(*) FROM users WHERE username = @u?"
    test_project.add_query("select_opt", select_opt_sql)
    mod = test_project.generate()

    uid = uuid.uuid4()
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO users (id, username) VALUES (%s, 'e1_user')", (uid,)
        )

    c1 = await mod.testdb_sql(select_opt_sql).query_single_row(u=None)
    assert c1 == 0

    c2 = await mod.testdb_sql(select_opt_sql).query_single_row(u="e1_user")
    assert c2 == 1


async def test_parameters_dedup(test_project: ProjectBuilder) -> None:
    select_dedup_sql = "SELECT count(*) FROM users WHERE id = $1 OR id = $2"
    test_project.add_query("select_dedup", select_dedup_sql)
    mod = test_project.generate()

    uid = uuid.uuid4()
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO users (id, username) VALUES (%s, 'e1_user')", (uid,)
        )

    sig_dedup = inspect.signature(
        mod.testdb_sql(select_dedup_sql).__class__.query_single_row
    )
    param_names = list(sig_dedup.parameters.keys())
    assert "id" in param_names
    c3 = await mod.testdb_sql(select_dedup_sql).query_single_row(uid, uid)
    assert c3 == 1
