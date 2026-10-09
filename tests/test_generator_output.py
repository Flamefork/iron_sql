# Packages under tests/generated are generated once per process, so mutmut's
# generator mutants reach only tests that run the generator themselves. These
# tests cover, through test_project, the module shapes those packages exercise.

from __future__ import annotations

import shutil
import sys
import textwrap
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from typing import cast

import pytest

from iron_sql.codegen import render_sql_module
from tests.conftest import basedpyright_report
from tests.json_models import UserMetadata
from tests.test_type_system import generated_class
from tests.test_type_system import generated_connection
from tests.test_type_system import module_value
from tests.test_type_system import query_single_row
from tests.test_type_system import sql_query

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from collections.abc import Awaitable
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager
    from types import ModuleType

    from tests.conftest import ProjectBuilder

type RowStream = AbstractAsyncContextManager[AsyncIterator[object]]

INSERT_USER = """
    INSERT INTO users (id, username, email, metadata)
    VALUES (%s, %s, 'user@example.com', '{"key": "k", "value": "v"}')
"""


async def insert_user(module: ModuleType, uid: uuid.UUID, username: str) -> None:
    async with generated_connection(module) as conn:
        await conn.execute(INSERT_USER, (uid, username))


def query_method(query: object, name: str) -> Callable[..., Awaitable[object]]:
    return cast("Callable[..., Awaitable[object]]", getattr(query, name))


def stream_method(query: object) -> Callable[..., RowStream]:
    method_name = "query_stream"
    return cast("Callable[..., RowStream]", getattr(query, method_name))


async def test_every_query_method_returns_rows(test_project: ProjectBuilder) -> None:
    stmt = "SELECT username FROM users WHERE id = $1"
    test_project.add_query("get_username", stmt)
    mod = test_project.generate()
    uid = uuid.uuid4()
    await insert_user(mod, uid, "alice")

    query = sql_query(mod, stmt)
    assert await query_method(query, "query_all_rows")(uid) == ["alice"]
    assert await query_method(query, "query_single_row")(uid) == "alice"
    assert await query_method(query, "query_optional_row")(uid) == "alice"
    assert await query_method(query, "query_optional_row")(uuid.uuid4()) is None
    async with stream_method(query)(uid) as rows:
        assert [row async for row in rows] == ["alice"]


async def test_module_with_several_row_classes(test_project: ProjectBuilder) -> None:
    users_stmt = "SELECT * FROM users WHERE id = $1"
    posts_stmt = "SELECT * FROM posts WHERE user_id = $1"
    test_project.add_query("get_user", users_stmt)
    test_project.add_query("get_posts", posts_stmt)
    mod = test_project.generate()
    uid = uuid.uuid4()
    await insert_user(mod, uid, "alice")
    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO posts (user_id, title) VALUES (%s, 'hello')", (uid,)
        )

    user = await query_single_row(mod, users_stmt, uid)
    post = await query_single_row(mod, posts_stmt, uid)

    assert type(user) is generated_class(mod, "TestdbUser")
    assert type(post) is generated_class(mod, "TestdbPost")


def test_single_component_module_name(test_project: ProjectBuilder) -> None:
    (test_project.app_dir / "config.py").write_text(
        f'DSN = "{test_project.dsn}"\n', encoding="utf-8"
    )
    test_project.add_query("q", "SELECT 1 AS value")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    render_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name="testdb",
        dsn_expr=f"{test_project.app_pkg}.config:DSN",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    ).write()

    generated_path = test_project.src_path / "testdb.py"
    compile(generated_path.read_text(encoding="utf-8"), str(generated_path), "exec")


async def test_listen_session_receives_notifications(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT 1 AS value")
    mod = test_project.generate()
    listen_session = cast(
        "Callable[[str], AbstractAsyncContextManager[AsyncIterator[str]]]",
        module_value(mod, "testdb_listen_session"),
    )
    notify = cast(
        "Callable[[str, str], Awaitable[None]]", module_value(mod, "testdb_notify")
    )

    async with listen_session("generator_output") as payloads:
        await notify("generator_output", "hello")
        assert await anext(payloads) == "hello"


async def test_repeated_parameter_columns_get_distinct_names(
    test_project: ProjectBuilder,
) -> None:
    stmt = "SELECT count(*) FROM users WHERE id = $1 OR id = $2"
    test_project.add_query("count_users", stmt)
    mod = test_project.generate()
    first, second = uuid.uuid4(), uuid.uuid4()
    await insert_user(mod, first, "alice")
    await insert_user(mod, second, "bob")

    assert await query_single_row(mod, stmt, id=first, id2=second) == 2


async def test_named_parameters_are_keyword_only(test_project: ProjectBuilder) -> None:
    stmt = "SELECT count(*) FROM users WHERE username = @username"
    test_project.add_query("count_by_username", stmt)
    mod = test_project.generate()
    await insert_user(mod, uuid.uuid4(), "alice")

    assert await query_single_row(mod, stmt, username="alice") == 1
    with pytest.raises(TypeError, match="positional argument"):
        await query_single_row(mod, stmt, "alice")


async def test_nullable_scalar_result_checks_type(test_project: ProjectBuilder) -> None:
    stmt = "SELECT email FROM users WHERE id = $1"
    test_project.add_query("get_email", stmt)
    mod = test_project.generate(type_overrides={"text": "int"})
    uid = uuid.uuid4()
    await insert_user(mod, uid, "alice")

    with pytest.raises(TypeError, match="Expected scalar of type <class 'int'>"):
        await query_single_row(mod, stmt, uid)


async def test_row_class_validates_json_model_column(
    test_project: ProjectBuilder,
) -> None:
    stmt = "SELECT * FROM users WHERE id = $1"
    test_project.add_query("get_user", stmt)
    mod = test_project.generate(
        json_model_overrides={"users.metadata": "tests.json_models:UserMetadata"}
    )
    uid = uuid.uuid4()
    await insert_user(mod, uid, "alice")

    user = await query_single_row(mod, stmt, uid)

    fields = cast("dict[str, object]", vars(user))
    assert fields["metadata"] == UserMetadata(key="k", value="v")


def test_generated_api_types(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        textwrap.dedent(f"""
        import uuid
        from collections.abc import AsyncIterator
        from contextlib import AbstractAsyncContextManager
        from typing import assert_type
        from typing import reveal_type

        from {test_project.app_pkg} import testdb as api
        from {test_project.app_pkg}.testdb import testdb_sql


        async def check(uid: uuid.UUID) -> None:
            query = testdb_sql("SELECT username FROM users WHERE id = $1")
            reveal_type(query)
            assert_type(await query.query_all_rows(uid), list[str])
            assert_type(await query.query_single_row(uid), str)
            assert_type(await query.query_optional_row(uid), str | None)
            assert_type(
                query.query_stream(uid),
                AbstractAsyncContextManager[AsyncIterator[str]],
            )

            summary = testdb_sql(
                "SELECT id, username FROM users", row_type="UserSummary"
            )
            assert_type(await summary.query_single_row(), api.UserSummary)

            status = testdb_sql("SELECT $1::user_status AS status")
            assert_type(
                await status.query_single_row(api.TestdbUserStatus.ACTIVE),
                api.TestdbUserStatus,
            )
        """)
    )
    test_project.generate()
    shutil.copy(
        Path(__file__).parent.parent / "pyproject.toml",
        test_project.src_path / "pyproject.toml",
    )

    report = basedpyright_report(
        test_project.src_path, project_root=test_project.src_path
    )

    errors = [
        f"{item.file.name}:{item.range.start.line + 1}: {item.message}"
        for item in report.general_diagnostics
        if item.severity == "error"
    ]
    assert errors == []
    revealed = [
        item
        for item in report.general_diagnostics
        if item.severity == "information" and item.file.name == "queries.py"
    ]
    assert len(revealed) == 1
