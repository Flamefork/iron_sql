from pathlib import Path

from iron_sql.codegen.sqlc import Column
from iron_sql.codegen.sqlc import run_sqlc

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
