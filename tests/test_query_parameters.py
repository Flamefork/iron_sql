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
        testdb_sql("SELECT count(*) FROM users WHERE username = @u?")
        testdb_sql("SELECT count(*) FROM users WHERE id = $1 OR id = $2")
        testdb_sql("SELECT @x::text::int4")
        testdb_sql("SELECT @a::int4 + @b::int4")
        testdb_sql("SELECT @user::text")
        testdb_sql("SELECT @Name::text = @name::text")
        testdb_sql(r"""SELECT 'hi @a sqlc.arg(x)' || $$ @b $$ || $t$ @c? $t$
        || E'\\' @d' || "@col" || @value::text /* @e /* @f? */ */
        FROM (SELECT '!' AS "@col") AS sub -- @g, $1""")
        testdb_sql("SELECT $1::text /* see @note? */")
        testdb_sql("SELECT $💰$ hi @a $💰$ || /* c */@value::text")
        testdb_sql("SELECT -- c\\r@value?::int4")
        testdb_sql("SELECT $tag$hello$tag$ || @value?::text")
        testdb_sql("SELECT @a\u0305::text")
        testdb_sql("SELECT @value::text AS v$1")
        testdb_sql("SELECT @doc?|array['a', 'b']")
        testdb_sql("""SELECT ARRAY[1]<@ARRAY[1, 2]
        AND to_tsvector('cat dog')@@to_tsquery('cat')
        AND 'user@example.com' LIKE '%@example.com'""")
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


async def test_parameter_with_two_casts() -> None:
    assert (
        await testdb.testdb_sql("SELECT @x::text::int4").query_single_row(x="42") == 42
    )


async def test_cast_parameters_in_one_expression() -> None:
    query = testdb.testdb_sql("SELECT @a::int4 + @b::int4")
    assert await query.query_single_row(a=1, b=2) == 3


async def test_parameter_named_after_sql_keyword() -> None:
    query = testdb.testdb_sql("SELECT @user::text")
    assert await query.query_single_row(user="u") == "u"

    sig = inspect.signature(query.__class__.query_single_row)
    assert sig.parameters["user"].kind == inspect.Parameter.KEYWORD_ONLY


async def test_parameter_names_fold_to_lower_case() -> None:
    query = testdb.testdb_sql("SELECT @Name::text = @name::text")

    sig = inspect.signature(query.__class__.query_single_row)
    assert list(sig.parameters) == ["self", "name"]
    assert await query.query_single_row(name="n") is True


async def test_at_sign_in_operators_and_literals_is_not_a_parameter() -> None:
    query = testdb.testdb_sql("""SELECT ARRAY[1]<@ARRAY[1, 2]
AND to_tsvector('cat dog')@@to_tsquery('cat')
AND 'user@example.com' LIKE '%@example.com'""")

    sig = inspect.signature(query.__class__.query_single_row)
    assert list(sig.parameters) == ["self"]
    assert await query.query_single_row() is True


async def test_at_signs_outside_query_code_are_not_parameters() -> None:
    query = testdb.testdb_sql(r"""SELECT 'hi @a sqlc.arg(x)' || $$ @b $$ || $t$ @c? $t$
|| E'\' @d' || "@col" || @value::text /* @e /* @f? */ */
FROM (SELECT '!' AS "@col") AS sub -- @g, $1""")

    sig = inspect.signature(query.__class__.query_single_row)
    assert list(sig.parameters) == ["self", "value"]
    assert sig.parameters["value"].kind == inspect.Parameter.KEYWORD_ONLY
    assert await query.query_single_row(value="?") == "hi @a sqlc.arg(x) @b  @c? ' @d!?"


async def test_positional_query_with_at_sign_in_comment() -> None:
    query = testdb.testdb_sql("SELECT $1::text /* see @note? */")
    assert await query.query_single_row("x") == "x"


async def test_at_sign_in_non_ascii_dollar_quote_and_after_comment() -> None:
    query = testdb.testdb_sql("SELECT $💰$ hi @a $💰$ || /* c */@value::text")

    sig = inspect.signature(query.__class__.query_single_row)
    assert list(sig.parameters) == ["self", "value"]
    assert await query.query_single_row(value="!") == " hi @a !"


async def test_line_comment_ends_at_carriage_return() -> None:
    query = testdb.testdb_sql("SELECT -- c\r@value?::int4")
    assert await query.query_single_row(value=5) == 5


async def test_dollar_quote_tag_ends_before_dollar() -> None:
    query = testdb.testdb_sql("SELECT $tag$hello$tag$ || @value?::text")
    assert await query.query_single_row(value="!") == "hello!"


async def test_parameter_name_takes_any_non_ascii_character() -> None:
    query = testdb.testdb_sql("SELECT @a\u0305::text")

    sig = inspect.signature(query.__class__.query_single_row)
    assert list(sig.parameters) == ["self", "a\u0305"]
    assert await query.query_single_row(a̅="x") == "x"


async def test_dollar_digit_inside_name_is_not_positional() -> None:
    query = testdb.testdb_sql("SELECT @value::text AS v$1")
    assert await query.query_single_row(value="x") == "x"


async def test_question_mark_glued_to_operator_is_part_of_it() -> None:
    query = testdb.testdb_sql("SELECT @doc?|array['a', 'b']")
    assert await query.query_single_row(doc={"b": 1}) is True
