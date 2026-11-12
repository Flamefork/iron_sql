import re
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pydantic
import pytest

from iron_sql.codegen import SQLGenerationError
from iron_sql.codegen.sqlc import Column
from iron_sql.codegen.sqlc import run_sqlc
from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import ProjectBuilder
from tests.conftest import generated_package
from tests.json_models import Post
from tests.json_models import UserMetadata
from tests.json_models import UserSummary

_JSON_AGG_SUBQUERY = (
    "SELECT users.id,"
    " (SELECT json_agg(posts) FROM posts WHERE posts.user_id = users.id) AS posts"
    " FROM users"
)

_JSONB_AGG_SUBQUERY = (
    "SELECT users.id,"
    " (SELECT jsonb_agg(posts) FROM posts WHERE posts.user_id = users.id) AS posts"
    " FROM users"
)

_COALESCED_JSON_AGG_SUBQUERY = (
    "SELECT users.id,"
    " coalesce("
    "(SELECT json_agg(posts) FROM posts WHERE posts.user_id = users.id),"
    " '[]'::json) AS posts"
    " FROM users"
)

_TO_JSONB = "SELECT to_jsonb(users) AS user_json FROM users"


def sqlc_column(schema_path: Path, sql: str, name: str, *, dsn: str | None) -> Column:
    result, _ = run_sqlc(schema_path, [("q", sql)], dsn=dsn)
    assert result.error is None
    (query,) = result.queries
    return next(column for column in query.columns if column.name == name)


def test_json_agg_subquery_is_typed_json_without_a_table(
    pg_test_dsn: str, schema_path: Path
) -> None:
    column = sqlc_column(schema_path, _JSON_AGG_SUBQUERY, "posts", dsn=pg_test_dsn)

    assert column.pg_type_name == "json"
    assert column.table is None
    assert column.not_null


def test_jsonb_agg_subquery_is_typed_jsonb(pg_test_dsn: str, schema_path: Path) -> None:
    column = sqlc_column(schema_path, _JSONB_AGG_SUBQUERY, "posts", dsn=pg_test_dsn)

    assert column.pg_type_name == "jsonb"
    assert column.table is None
    assert column.not_null


def test_to_jsonb_is_typed_jsonb_without_a_table(
    pg_test_dsn: str, schema_path: Path
) -> None:
    column = sqlc_column(schema_path, _TO_JSONB, "user_json", dsn=pg_test_dsn)

    assert column.pg_type_name == "jsonb"
    assert column.table is None
    assert column.not_null


def test_coalesced_json_agg_subquery_becomes_nullable(
    pg_test_dsn: str, schema_path: Path
) -> None:
    column = sqlc_column(
        schema_path, _COALESCED_JSON_AGG_SUBQUERY, "posts", dsn=pg_test_dsn
    )

    assert column.pg_type_name == "json"
    assert column.table is None
    assert not column.not_null


def test_coalesced_json_agg_subquery_loses_its_type_without_a_database(
    schema_path: Path,
) -> None:
    column = sqlc_column(schema_path, _COALESCED_JSON_AGG_SUBQUERY, "posts", dsn=None)

    assert column.pg_type_name == "any"
    assert not column.not_null


generated_package(
    "computed_json",
    schema=SCHEMA_SQL,
    queries='''
        import uuid
        from typing import assert_type

        from tests.generated.computed_json import testdb as api
        from tests.generated.computed_json.testdb import testdb_sql
        from tests.json_models import Labels
        from tests.json_models import PostList
        from tests.json_models import PostSequence
        from tests.json_models import UserMetadata
        from tests.json_models import UserSummary


        async def insert_user(uid: uuid.UUID, metadata: UserMetadata) -> None:
            await testdb_sql(
                "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
            ).execute(uid, "author", metadata)


        async def insert_post(uid: uuid.UUID, title: str) -> None:
            await testdb_sql(
                "INSERT INTO posts (user_id, title) VALUES ($1, $2)"
            ).execute(uid, title)


        async def user_posts(uid: uuid.UUID) -> api.UserPosts:
            row = await testdb_sql(
                """SELECT users.id, coalesce(
                    (SELECT json_agg(posts ORDER BY posts.id)
                    FROM posts WHERE posts.user_id = users.id),
                    '[]'::json) AS posts
                FROM users WHERE users.id = $1""",
                row_type="UserPosts",
                json_model_overrides={"posts": "tests.json_models:PostList"},
            ).query_single_row(uid)
            assert_type(row.posts, PostList | None)
            return row


        async def posts(uid: uuid.UUID) -> PostList | None:
            return await testdb_sql(
                """SELECT coalesce(
                    (SELECT json_agg(posts ORDER BY posts.id)
                    FROM posts WHERE posts.user_id = $1),
                    '[]'::json) AS posts""",
                json_model_overrides={"posts": "tests.json_models:PostList"},
            ).query_single_row(uid)


        async def user_post_sequence(uid: uuid.UUID) -> PostSequence:
            row = await testdb_sql(
                """SELECT users.id,
                    (SELECT jsonb_agg(posts ORDER BY posts.id)
                    FROM posts WHERE posts.user_id = users.id) AS posts
                FROM users WHERE users.id = $1""",
                json_model_overrides={"posts": "tests.json_models:PostSequence"},
            ).query_single_row(uid)
            return row.posts


        async def post_sequence(uid: uuid.UUID) -> PostSequence:
            return await testdb_sql(
                """SELECT (SELECT jsonb_agg(posts ORDER BY posts.id)
                FROM posts WHERE posts.user_id = $1) AS posts""",
                json_model_overrides={"posts": "tests.json_models:PostSequence"},
            ).query_single_row(uid)


        async def user_summary(uid: uuid.UUID) -> UserSummary:
            return await testdb_sql(
                "SELECT to_jsonb(users) AS user_json FROM users WHERE id = $1",
                json_model_overrides={"user_json": "tests.json_models:UserSummary"},
            ).query_single_row(uid)


        async def labels(uid: uuid.UUID) -> tuple[Labels, str]:
            row = await testdb_sql(
                """SELECT users.id,
                    json_build_object('username', users.username) AS labels,
                    to_jsonb(users.username) AS username_json
                FROM users WHERE users.id = $1""",
                json_model_overrides={
                    "username_json": "builtins:str",
                    "labels": "tests.json_models:Labels",
                },
            ).query_single_row(uid)
            return row.labels, row.username_json


        async def metadata_by_query(uid: uuid.UUID) -> Labels | None:
            row = await testdb_sql(
                "SELECT id, metadata FROM users WHERE id = $1",
                json_model_overrides={"metadata": "tests.json_models:Labels"},
            ).query_single_row(uid)
            return row.metadata


        async def metadata_by_table(uid: uuid.UUID) -> UserMetadata | None:
            row = await testdb_sql(
                "SELECT * FROM users WHERE id = $1", json_model_overrides={}
            ).query_single_row(uid)
            return row.metadata


        async def misshapen_posts() -> PostList:
            return await testdb_sql(
                """SELECT '{"not": "a list"}'::jsonb AS posts""",
                json_model_overrides={"posts": "tests.json_models:PostList"},
            ).query_single_row()
    ''',
    json_model_overrides={"users.metadata": "tests.json_models:UserMetadata"},
)

from tests.generated.computed_json import queries as computed_json


@pytest.fixture
async def use_computed_json_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("computed_json"):
        yield


async def _insert_user_with_posts(*titles: str) -> uuid.UUID:
    uid = uuid.uuid4()
    await computed_json.insert_user(uid, UserMetadata(key="k", value="v"))
    for title in titles:
        await computed_json.insert_post(uid, title)
    return uid


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_types_json_agg_with_a_list_alias() -> None:
    uid = await _insert_user_with_posts("first", "second")

    row = await computed_json.user_posts(uid)
    scalar = await computed_json.posts(uid)

    assert row.id == uid
    assert row.posts is not None
    assert [(type(post), post.title) for post in row.posts] == [
        (Post, "first"),
        (Post, "second"),
    ]
    assert scalar == row.posts


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_types_json_agg_with_a_pep695_alias() -> None:
    uid = await _insert_user_with_posts("only")

    row_posts = await computed_json.user_post_sequence(uid)
    scalar = await computed_json.post_sequence(uid)

    assert [(type(post), post.title) for post in row_posts] == [(Post, "only")]
    assert scalar == row_posts


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_types_to_jsonb_with_a_model_class() -> None:
    uid = await _insert_user_with_posts()

    assert await computed_json.user_summary(uid) == UserSummary(
        id=uid, username="author"
    )


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_reads_a_dict_alias_and_a_json_string() -> None:
    uid = await _insert_user_with_posts()

    assert await computed_json.labels(uid) == ({"username": "author"}, "author")


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_beats_the_table_override() -> None:
    uid = await _insert_user_with_posts()

    assert await computed_json.metadata_by_query(uid) == {"key": "k", "value": "v"}
    assert await computed_json.metadata_by_table(uid) == UserMetadata(
        key="k", value="v"
    )


@pytest.mark.usefixtures("use_computed_json_database")
async def test_query_override_rejects_json_of_another_shape() -> None:
    with pytest.raises(pydantic.ValidationError, match="list_type"):
        await computed_json.misshapen_posts()


def _queries_source(*calls: str) -> str:
    return "\n".join([
        "from typing import Any",
        "def testdb_sql(q: str, **kwargs: Any) -> Any: ...",
        "",
        *calls,
        "",
    ])


def _overrides_call(sql: str, overrides: str) -> str:
    return f"testdb_sql({sql!r}, json_model_overrides={overrides})"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            "OVERRIDES",
            (
                "json_model_overrides must be a dict literal with string literal "
                "keys and values"
            ),
        ),
        (
            '{"posts": PostList}',
            "json_model_overrides['posts'] must be a string literal 'module:Name'",
        ),
        ('{1: "tests.json_models:PostList"}', "json_model_overrides keys must be"),
        ("{**OVERRIDES}", "json_model_overrides keys must be"),
        (
            '{"posts": "tests.json_models:PostList", "posts": "builtins:str"}',
            "json_model_overrides repeats key 'posts'; give each result column once",
        ),
        (
            '{"posts": "tests.json_models.PostList"}',
            (
                "json_model_overrides['posts']='tests.json_models.PostList': "
                "expected a 'module:Name' value"
            ),
        ),
        (
            '{"posts": "tests.json_models:"}',
            (
                "json_model_overrides['posts']='tests.json_models:': "
                "expected a 'module:Name' value"
            ),
        ),
    ],
)
def test_scanner_rejects_malformed_query_json_model_overrides(
    test_project: ProjectBuilder, overrides: str, message: str
) -> None:
    test_project.set_queries_source(
        _queries_source(_overrides_call("SELECT metadata FROM users", overrides))
    )

    with pytest.raises(SQLGenerationError, match=re.escape(message)) as exc_info:
        test_project.generate_no_import()

    assert "queries.py:4:1: " in str(exc_info.value)


def test_scanner_rejects_repeated_keyword_arguments(
    test_project: ProjectBuilder,
) -> None:
    call = (
        'testdb_sql("SELECT metadata FROM users",'
        " json_model_overrides={}, json_model_overrides={})"
    )
    test_project.set_queries_source(_queries_source(call))

    with pytest.raises(
        SQLGenerationError,
        match=re.escape(
            "queries.py:4:1: keyword argument 'json_model_overrides' repeated"
        ),
    ):
        test_project.generate_no_import()


_JSON_TYPES_REQUIREMENT = (
    "a JSON model override needs a column of type json, jsonb, text, varchar"
)


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "SELECT id, metadata FROM users",
            (
                "the query has no result column 'posts'; use one of its result "
                "columns: id, metadata"
            ),
        ),
        (
            "UPDATE users SET metadata = NULL",
            (
                "the query has no result column 'posts'; the query returns no "
                "result columns, remove the key"
            ),
        ),
        (
            "SELECT metadata AS posts, metadata AS posts FROM users",
            (
                "2 result columns are named 'posts'; give each of them a distinct "
                "alias with AS"
            ),
        ),
        (
            "SELECT id AS posts FROM users",
            f"result column 'posts' has type 'uuid'; {_JSON_TYPES_REQUIREMENT}",
        ),
        (
            "SELECT payloads AS posts FROM jsonb_arrays",
            f"result column 'posts' has type 'jsonb[]'; {_JSON_TYPES_REQUIREMENT}",
        ),
    ],
)
def test_query_json_model_overrides_reject_unfit_result_columns(
    test_project: ProjectBuilder, sql: str, message: str
) -> None:
    test_project.set_queries_source(
        _queries_source(_overrides_call(sql, '{"posts": "tests.json_models:PostList"}'))
    )

    with pytest.raises(
        SQLGenerationError,
        match=re.escape(f"queries.py:4: json_model_overrides['posts']: {message}"),
    ):
        test_project.generate_no_import()


def test_query_json_model_override_rejects_a_column_sqlc_could_not_type(
    test_project: ProjectBuilder,
) -> None:
    test_project.dsn = ""
    test_project.set_queries_source(
        _queries_source(
            _overrides_call(
                _COALESCED_JSON_AGG_SUBQUERY, '{"posts": "tests.json_models:PostList"}'
            )
        )
    )
    message = (
        "queries.py:4: json_model_overrides['posts']: sqlc could not infer the "
        "type of result column 'posts'; give the expression a type, for example "
        "a cast to json or jsonb, or generate with a DSN"
    )

    with pytest.raises(SQLGenerationError, match=re.escape(message)):
        test_project.generate_no_import()


def test_same_sql_with_different_json_model_overrides_is_rejected(
    test_project: ProjectBuilder,
) -> None:
    sql = "SELECT metadata FROM users"
    test_project.set_queries_source(
        _queries_source(
            _overrides_call(sql, '{"metadata": "tests.json_models:Labels"}'),
            f"testdb_sql({sql!r})",
        )
    )

    with pytest.raises(SQLGenerationError) as exc_info:
        test_project.generate_no_import()

    assert re.fullmatch(
        r"json_model_overrides conflict for the same SQL: \S+/queries\.py:4 has "
        + re.escape("{'metadata': 'tests.json_models:Labels'}, ")
        + r"\S+/queries\.py:5 has \{\}; "
        + re.escape(
            "pass the same json_model_overrides at every call site of this SQL"
        ),
        str(exc_info.value),
    )


def test_same_sql_with_reordered_json_model_overrides_is_allowed(
    test_project: ProjectBuilder,
) -> None:
    sql = "SELECT metadata, to_jsonb(username) AS name_json FROM users"
    test_project.set_queries_source(
        _queries_source(
            _overrides_call(
                sql,
                '{"metadata": "tests.json_models:Labels", "name_json": "builtins:str"}',
            ),
            _overrides_call(
                sql,
                '{"name_json": "builtins:str", "metadata": "tests.json_models:Labels"}',
            ),
        )
    )

    test_project.generate()
