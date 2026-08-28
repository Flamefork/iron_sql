import inspect
import uuid
from collections.abc import AsyncGenerator

import pytest

from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import generated_package

generated_package(
    "query_parameters",
    schema=SCHEMA_SQL,
    queries='''
        from tests.generated.query_parameters.testdb import testdb_sql

        testdb_sql("""INSERT INTO users (id, username, is_active)
        VALUES (@id, @username, @active)""")
        testdb_sql("SELECT id FROM users WHERE id = $1 AND username = @username")
        testdb_sql("SELECT count(*) FROM users WHERE username = @u?")
        testdb_sql("SELECT count(*) FROM users WHERE id = $1 OR id = $2")
    ''',
)

from tests.generated.query_parameters import testdb


@pytest.fixture(autouse=True)
async def use_generated_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("query_parameters"):
        yield


async def test_parameters_named() -> None:
    insert_sql = """INSERT INTO users (id, username, is_active)
VALUES (@id, @username, @active)"""
    mod = testdb

    uid = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(id=uid, username="e1_user", active=True)

    sig = inspect.signature(mod.testdb_sql(insert_sql).__class__.execute)
    params = list(sig.parameters.values())
    assert params[1].kind == inspect.Parameter.KEYWORD_ONLY


async def test_parameters_mixed() -> None:
    select_mixed_sql = "SELECT id FROM users WHERE id = $1 AND username = @username"
    mod = testdb

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


async def test_parameters_optional() -> None:
    select_opt_sql = "SELECT count(*) FROM users WHERE username = @u?"
    mod = testdb

    uid = uuid.uuid4()
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO users (id, username) VALUES (%s, 'e1_user')", (uid,)
        )

    c1 = await mod.testdb_sql(select_opt_sql).query_single_row(u=None)
    assert c1 == 0

    c2 = await mod.testdb_sql(select_opt_sql).query_single_row(u="e1_user")
    assert c2 == 1


async def test_parameters_dedup() -> None:
    select_dedup_sql = "SELECT count(*) FROM users WHERE id = $1 OR id = $2"
    mod = testdb

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
