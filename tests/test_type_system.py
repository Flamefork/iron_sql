import datetime
import decimal
import inspect
import ipaddress
import warnings
from enum import StrEnum
from typing import Any
from typing import LiteralString
from typing import get_args

import pytest

from iron_sql.codegen import UnknownSQLTypeWarning
from iron_sql.runtime import ConnectionPool
from tests.conftest import ProjectBuilder


def query_signature(query: Any) -> inspect.Signature:
    return inspect.signature(query.__class__.query_single_row)


def annotation_types(annotation: Any) -> set[Any]:
    args = get_args(annotation)
    return {a for a in args if a is not type(None)} if args else {annotation}


def assert_return_types(query: Any, expected: set[type]) -> None:
    assert annotation_types(query_signature(query).return_annotation) == expected


async def test_enum_generation(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "get_enum",
        "SELECT 'active'::user_status as status",
    )

    mod = test_project.generate()

    assert hasattr(mod, "TestdbUserStatus")
    enum_cls = mod.TestdbUserStatus
    assert issubclass(enum_cls, StrEnum)

    assert enum_cls.ACTIVE == "active"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    assert enum_cls.INACTIVE == "inactive"  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

    row = await mod.testdb_sql(
        "SELECT 'active'::user_status as status"
    ).query_single_row()
    assert isinstance(row, enum_cls)
    assert row == enum_cls.ACTIVE  # pyright: ignore[reportAttributeAccessIssue]
    assert row == "active"  # StrEnum acts as str


async def test_enum_parameter(test_project: ProjectBuilder) -> None:
    sql = "SELECT $1::user_status as status"
    test_project.add_query("echo_status", sql)

    mod = test_project.generate()
    enum_cls = mod.TestdbUserStatus

    idx = await mod.testdb_sql(sql).query_single_row(enum_cls.ACTIVE)
    assert idx == enum_cls.ACTIVE

    idx = await mod.testdb_sql(sql).query_single_row("active")
    assert idx == enum_cls.ACTIVE


async def test_entity_generation_with_enum(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TABLE enum_test_table (
        id SERIAL PRIMARY KEY,
        status user_status NOT NULL,
        tags user_status[]
    );
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_enum_entity", "SELECT * FROM enum_test_table")

    mod = test_project.generate()

    enum_cls = mod.TestdbUserStatus
    entity_cls = mod.TestdbEnumTestTable

    assert entity_cls.__annotations__["status"] is enum_cls

    tags_annotation = entity_cls.__annotations__["tags"]
    tag_args = get_args(tags_annotation)
    # Sequence[EnumCls] | None — unwrap the union
    seq_type = next(a for a in tag_args if a is not type(None))
    (inner_type,) = get_args(seq_type)
    assert inner_type is enum_cls


async def test_single_array_column_result_roundtrip(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE array_results (
        id SERIAL PRIMARY KEY,
        scores int[] NOT NULL,
        extra int[]
    );
    """)

    not_null_sql = "SELECT scores FROM array_results WHERE id = $1"
    nullable_sql = "SELECT extra FROM array_results WHERE id = $1"
    test_project.add_query("get_scores", not_null_sql)
    test_project.add_query("get_extra", nullable_sql)

    mod = test_project.generate()

    generated = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    ).read_text()
    assert "runtime.typed_array_row(int, not_null=True)" in generated
    assert "runtime.typed_array_row(int, not_null=False)" in generated

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO array_results (id, scores, extra) VALUES (%s, %s, %s)",
            (1, [1, 2, 3], None),
        )
        await conn.execute(
            "INSERT INTO array_results (id, scores, extra) VALUES (%s, %s, %s)",
            (2, [4], [5, 6]),
        )

    assert await mod.testdb_sql(not_null_sql).query_single_row(1) == [1, 2, 3]
    assert await mod.testdb_sql(nullable_sql).query_single_row(1) is None
    assert await mod.testdb_sql(nullable_sql).query_single_row(2) == [5, 6]


async def test_enum_resolves_to_instances_in_all_result_positions(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE enum_results (
        id SERIAL PRIMARY KEY,
        status user_status NOT NULL,
        tags user_status[] NOT NULL
    );
    """)

    multi_sql = "SELECT id, status, tags FROM enum_results WHERE id = $1"
    scalar_sql = "SELECT status FROM enum_results WHERE id = $1"
    array_sql = "SELECT tags FROM enum_results WHERE id = $1"
    test_project.add_query("get_row", multi_sql)
    test_project.add_query("get_status", scalar_sql)
    test_project.add_query("get_tags", array_sql)

    mod = test_project.generate()
    status_enum = mod.TestdbUserStatus

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO enum_results (id, status, tags) VALUES (%s, %s, %s)",
            (1, status_enum.ACTIVE, [status_enum.ACTIVE, status_enum.INACTIVE]),
        )

    row = await mod.testdb_sql(multi_sql).query_single_row(1)
    assert row.status is status_enum.ACTIVE
    assert row.tags == [status_enum.ACTIVE, status_enum.INACTIVE]
    assert all(isinstance(tag, status_enum) for tag in row.tags)

    assert await mod.testdb_sql(scalar_sql).query_single_row(1) is status_enum.ACTIVE

    tags = await mod.testdb_sql(array_sql).query_single_row(1)
    assert tags == [status_enum.ACTIVE, status_enum.INACTIVE]
    assert all(isinstance(tag, status_enum) for tag in tags)


async def test_nullable_enum_resolves_or_returns_none(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE nullable_enum (
        id SERIAL PRIMARY KEY,
        status user_status,
        tags user_status[]
    );
    """)

    scalar_sql = "SELECT status FROM nullable_enum WHERE id = $1"
    array_sql = "SELECT tags FROM nullable_enum WHERE id = $1"
    test_project.add_query("get_status", scalar_sql)
    test_project.add_query("get_tags", array_sql)

    mod = test_project.generate()
    status_enum = mod.TestdbUserStatus

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO nullable_enum (id, status, tags) VALUES (%s, %s, %s)",
            (1, None, None),
        )
        await conn.execute(
            "INSERT INTO nullable_enum (id, status, tags) VALUES (%s, %s, %s)",
            (2, status_enum.ACTIVE, [status_enum.INACTIVE]),
        )

    assert await mod.testdb_sql(scalar_sql).query_single_row(1) is None
    assert await mod.testdb_sql(scalar_sql).query_single_row(2) is status_enum.ACTIVE
    assert await mod.testdb_sql(array_sql).query_single_row(1) is None
    assert await mod.testdb_sql(array_sql).query_single_row(2) == [status_enum.INACTIVE]


async def test_normalized_enum_label_roundtrips(test_project: ProjectBuilder) -> None:
    await test_project.extend_schema("""
    CREATE TYPE task_phase AS ENUM ('in-progress', '2fa');
    CREATE TABLE phase_table (
        id SERIAL PRIMARY KEY,
        phase task_phase NOT NULL
    );
    """)

    select_sql = "SELECT phase FROM phase_table WHERE id = $1"
    test_project.add_query("get_phase", select_sql)

    mod = test_project.generate()
    phase_enum = mod.TestdbTaskPhase

    # Member names are normalized, but values keep the raw PostgreSQL labels —
    # so registration must map by value, not by member name.
    assert phase_enum.IN_PROGRESS == "in-progress"
    assert phase_enum.NUM2FA == "2fa"

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO phase_table (id, phase) VALUES (%s, %s)",
            (1, phase_enum.IN_PROGRESS),
        )

    assert (
        await mod.testdb_sql(select_sql).query_single_row(1) is phase_enum.IN_PROGRESS
    )


async def test_empty_enum_array_returns_empty_list(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE empty_enum_array (
        id SERIAL PRIMARY KEY,
        tags user_status[] NOT NULL
    );
    """)

    select_sql = "SELECT tags FROM empty_enum_array WHERE id = $1"
    test_project.add_query("get_tags", select_sql)

    mod = test_project.generate()

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO empty_enum_array (id, tags) VALUES (1, '{}'::user_status[])"
        )

    assert await mod.testdb_sql(select_sql).query_single_row(1) == []


async def test_distinct_enums_register_independently(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TYPE color AS ENUM ('red', 'green');
    CREATE TABLE multi_enum (
        id SERIAL PRIMARY KEY,
        status user_status NOT NULL,
        color color NOT NULL
    );
    """)

    select_sql = "SELECT status, color FROM multi_enum WHERE id = $1"
    test_project.add_query("get_multi", select_sql)

    mod = test_project.generate()
    status_enum = mod.TestdbUserStatus
    color_enum = mod.TestdbColor

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO multi_enum (id, status, color) VALUES (%s, %s, %s)",
            (1, status_enum.ACTIVE, color_enum.RED),
        )

    row = await mod.testdb_sql(select_sql).query_single_row(1)
    assert row.status is status_enum.ACTIVE
    assert row.color is color_enum.RED


async def test_scalar_enum_query_requires_registered_connection(
    test_project: ProjectBuilder,
) -> None:
    select_sql = "SELECT 'active'::user_status AS status"
    test_project.add_query("get_status", select_sql)

    mod = test_project.generate()

    # A pool without enum_types — its connections are not enum-aware.
    unregistered_pool = ConnectionPool(test_project.dsn)
    try:
        async with unregistered_pool.connection() as conn:
            query = mod.testdb_sql(select_sql).with_connection(conn)
            with pytest.raises(TypeError, match="Expected scalar of type"):
                await query.query_single_row()
    finally:
        await unregistered_pool.close()


async def test_nullable_union_scalar_returns_none(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE network (
        id SERIAL PRIMARY KEY,
        addr inet
    );
    """)

    select_sql = "SELECT addr FROM network WHERE id = $1"
    test_project.add_query("get_addr", select_sql)

    mod = test_project.generate()

    generated = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    ).read_text()
    assert "runtime.typed_value_row(not_null=False)" in generated

    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO network (id, addr) VALUES (1, NULL), (2, '10.0.0.1')"
        )

    assert await mod.testdb_sql(select_sql).query_single_row(1) is None
    assert await mod.testdb_sql(select_sql).query_single_row(2) == ipaddress.ip_address(
        "10.0.0.1"
    )


async def test_unused_enum_skipped(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TYPE unused_enum AS ENUM ('a', 'b');
    """

    await test_project.extend_schema(extra_schema)

    mod = test_project.generate()

    assert not hasattr(mod, "TestdbUnusedEnum")


async def test_enum_naming_normalization(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TYPE "camelCaseEnum" AS ENUM ('a');
    CREATE TYPE "SCREAMING_ENUM" AS ENUM ('b');
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_camel", 'SELECT NULL::"camelCaseEnum" as c')
    test_project.add_query("get_screaming", 'SELECT NULL::"SCREAMING_ENUM" as s')

    mod = test_project.generate()

    assert hasattr(mod, "TestdbCamelCaseEnum")
    assert hasattr(mod, "TestdbScreamingEnum")


async def test_enum_value_name_normalization(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TYPE weird_enum AS ENUM ('1st', 'foo-bar', 'foo_bar');
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_weird_enum", "SELECT '1st'::weird_enum as status")

    mod = test_project.generate()

    enum_cls = mod.TestdbWeirdEnum
    assert enum_cls.NUM1ST == "1st"
    assert enum_cls.FOO_BAR == "foo-bar"
    assert enum_cls.FOO_BAR_2 == "foo_bar"

    assert len(enum_cls.__members__) == 3


async def test_enum_empty_label_value(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TYPE empty_label_enum AS ENUM ('', 'present');
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query(
        "get_empty_label_enum", "SELECT ''::empty_label_enum as status"
    )

    mod = test_project.generate()

    enum_cls = mod.TestdbEmptyLabelEnum
    assert {member.value for member in enum_cls} == {"", "present"}
    assert len(enum_cls.__members__) == 2


async def test_cross_schema_enum_type_annotation(
    test_project: ProjectBuilder,
) -> None:
    extra_schema = """
    CREATE SCHEMA extra_schema;
    CREATE TYPE extra_schema.mood AS ENUM ('happy', 'sad');
    CREATE TABLE cross_schema_enum_table (
        id SERIAL PRIMARY KEY,
        mood extra_schema.mood NOT NULL
    );
    """

    await test_project.extend_schema(extra_schema)

    select_sql = "SELECT * FROM cross_schema_enum_table"
    test_project.add_query("get_cross_enum", select_sql)

    mod = test_project.generate()

    enum_cls = mod.TestdbMood
    entity_cls = mod.TestdbCrossSchemaEnumTable

    assert entity_cls.__annotations__["mood"] is enum_cls

    # The enum lives in a non-public schema, so its type is registered by its
    # schema-qualified name.
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO cross_schema_enum_table (mood) VALUES (%s)",
            (enum_cls.HAPPY,),
        )

    row = await mod.testdb_sql(select_sql).query_single_row()
    assert row.mood is enum_cls.HAPPY


async def test_pg_catalog_type_does_not_break_generation(
    test_project: ProjectBuilder,
) -> None:
    sql = "SELECT 1::oid as oid"
    test_project.add_query("get_oid", sql)

    mod = test_project.generate()

    row = await mod.testdb_sql(sql).query_single_row()
    assert row == 1


def test_pg_catalog_does_not_trigger_warnings(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("get_user", "SELECT * FROM users")

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        test_project.generate()

    unknown_type_warnings = [
        w for w in warning_messages if issubclass(w.category, UnknownSQLTypeWarning)
    ]
    assert not unknown_type_warnings


async def test_unknown_sql_type_warns_and_maps_to_object(
    test_project: ProjectBuilder,
) -> None:
    extra_schema = """
    CREATE TYPE unknown_composite AS (x integer);
    CREATE TABLE unknown_table (
        id SERIAL PRIMARY KEY,
        val unknown_composite NOT NULL
    );
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_unknown", "SELECT * FROM unknown_table")

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        mod = test_project.generate()

    unknown_type_warnings = [
        w for w in warning_messages if issubclass(w.category, UnknownSQLTypeWarning)
    ]
    assert unknown_type_warnings
    assert "unknown_composite" in str(unknown_type_warnings[0].message)

    entity_cls = mod.TestdbUnknownTable
    assert entity_cls.__annotations__["val"] is object


async def test_unknown_sql_type_can_be_promoted_to_error(
    test_project: ProjectBuilder,
) -> None:
    extra_schema = """
    CREATE TYPE unknown_composite AS (x integer);
    CREATE TABLE unknown_table (
        id SERIAL PRIMARY KEY,
        val unknown_composite NOT NULL
    );
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_unknown", "SELECT * FROM unknown_table")

    with warnings.catch_warnings():
        warnings.simplefilter("error", UnknownSQLTypeWarning)
        with pytest.raises(UnknownSQLTypeWarning, match="unknown_composite"):
            test_project.generate()


async def test_table_column_enum_not_in_query_is_skipped(
    test_project: ProjectBuilder,
) -> None:
    extra_schema = """
    CREATE TYPE table_only_status AS ENUM ('pending', 'processed');
    CREATE TABLE status_log (
        id SERIAL PRIMARY KEY,
        status table_only_status NOT NULL
    );
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_users", "SELECT * FROM users")

    mod = test_project.generate()

    assert not hasattr(mod, "TestdbTableOnlyStatus")


async def test_standard_type_mapping_network(test_project: ProjectBuilder) -> None:
    inet_stmt = "SELECT '10.0.0.1/24'::inet as v"
    cidr_stmt = "SELECT '10.0.0.0/24'::cidr as v"

    test_project.add_query("get_inet", inet_stmt)
    test_project.add_query("get_cidr", cidr_stmt)

    mod = test_project.generate()

    inet_types = {
        ipaddress.IPv4Address,
        ipaddress.IPv6Address,
        ipaddress.IPv4Interface,
        ipaddress.IPv6Interface,
    }
    inet_query = mod.testdb_sql(inet_stmt)
    assert_return_types(inet_query, inet_types)
    assert isinstance(await inet_query.query_single_row(), tuple(inet_types))

    cidr_types = {ipaddress.IPv4Network, ipaddress.IPv6Network}
    cidr_query = mod.testdb_sql(cidr_stmt)
    assert_return_types(cidr_query, cidr_types)
    assert isinstance(await cidr_query.query_single_row(), tuple(cidr_types))


async def test_standard_type_mapping_interval(test_project: ProjectBuilder) -> None:
    interval_stmt = "SELECT INTERVAL '1 day 2 hours' as v"

    test_project.add_query("get_interval", interval_stmt)

    mod = test_project.generate()

    interval_query = mod.testdb_sql(interval_stmt)
    assert_return_types(interval_query, {datetime.timedelta})
    assert isinstance(await interval_query.query_single_row(), datetime.timedelta)


async def test_standard_type_mapping_text_variants(
    test_project: ProjectBuilder,
) -> None:
    bpchar_stmt = "SELECT 'x'::bpchar as v"
    char_stmt = "SELECT 'y'::\"char\" as v"
    name_stmt = "SELECT 'hello'::name as v"

    test_project.add_query("get_bpchar", bpchar_stmt)
    test_project.add_query("get_char", char_stmt)
    test_project.add_query("get_name", name_stmt)

    mod = test_project.generate()

    for stmt in (bpchar_stmt, char_stmt, name_stmt):
        q = mod.testdb_sql(stmt)
        assert_return_types(q, {str})
        assert isinstance(await q.query_single_row(), str)


async def test_type_overrides_suppress_unknown_warning_and_override_annotation(
    test_project: ProjectBuilder,
) -> None:
    extra_schema = """
    CREATE DOMAIN custom_int AS integer;
    """

    await test_project.extend_schema(extra_schema)

    stmt = "SELECT 1::custom_int as v"
    test_project.add_query("get_custom_int", stmt)

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        mod = test_project.generate(type_overrides={"custom_int": "int"})

    unknown_type_warnings = [
        w for w in warning_messages if issubclass(w.category, UnknownSQLTypeWarning)
    ]
    assert not unknown_type_warnings

    q = mod.testdb_sql(stmt)
    assert_return_types(q, {int})

    val = await q.query_single_row()
    assert val == 1
    assert isinstance(val, int)


_ANALYZER_VOCABULARY_SCHEMA = """
    CREATE TABLE vocabulary (
        f8 double precision,
        f4 real,
        i2 smallint,
        i4 integer,
        i8 bigint,
        ss smallserial,
        s serial,
        bs bigserial,
        s2 serial2,
        s4 serial4,
        s8 serial8,
        b boolean,
        num numeric(10, 2),
        v character varying(20),
        ch character(3),
        ts timestamp without time zone,
        tstz timestamp with time zone,
        tm time without time zone,
        tmtz time with time zone,
        c "char"
    );
"""

_ANALYZER_VOCABULARY_CASES = [
    ("f8", "min", float),
    ("f4", "min", float),
    ("i2", "min", int),
    ("i4", "min", int),
    ("i8", "min", int),
    ("ss", "min", int),
    ("s", "min", int),
    ("bs", "min", int),
    ("s2", "min", int),
    ("s4", "min", int),
    ("s8", "min", int),
    ("b", "bool_and", bool),
    ("num", "min", decimal.Decimal),
    ("v", "min", str),
    ("ch", "min", str),
    ("ts", "min", datetime.datetime),
    ("tstz", "min", datetime.datetime),
    ("tm", "min", datetime.time),
    ("tmtz", "min", datetime.time),
    ("c", "min", str),
]


async def test_analyzer_type_names_map_like_static_ones(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema(_ANALYZER_VOCABULARY_SCHEMA)

    static = {
        col: f"SELECT vocabulary.{col} as v FROM vocabulary"
        for col, _, _ in _ANALYZER_VOCABULARY_CASES
    }
    derived = {
        col: f"SELECT {agg}(vocabulary.{col}) as v FROM vocabulary"
        for col, agg, _ in _ANALYZER_VOCABULARY_CASES
    }
    parametrized = {
        col: (
            f"SELECT vocabulary.{col} as v FROM vocabulary WHERE vocabulary.{col} = @p"
        )
        for col, _, _ in _ANALYZER_VOCABULARY_CASES
    }
    for axis, statements in (
        ("static", static),
        ("derived", derived),
        ("param", parametrized),
    ):
        for col, stmt in statements.items():
            test_project.add_query(f"get_{col}_{axis}", stmt)

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        mod = test_project.generate()

    assert not [
        str(w.message)
        for w in warning_messages
        if issubclass(w.category, UnknownSQLTypeWarning)
    ]

    for col, _, py_type in _ANALYZER_VOCABULARY_CASES:
        for axis, statements in (("static", static), ("derived", derived)):
            returned = query_signature(
                mod.testdb_sql(statements[col])
            ).return_annotation
            assert annotation_types(returned) == {py_type}, (col, axis)

        param = query_signature(mod.testdb_sql(parametrized[col])).parameters["p"]
        assert annotation_types(param.annotation) == {py_type}, (col, "param")


def test_type_overrides_ignored_when_no_queries(
    test_project: ProjectBuilder,
) -> None:
    mod = test_project.generate(type_overrides={"jsonb": "str"})

    with pytest.raises(KeyError, match="Unknown statement"):
        mod.testdb_sql("SELECT * FROM users")


_SHADOWING_ENUM_NAMES: list[LiteralString] = [
    "boolean",
    "bigint",
    "integer",
    "real",
    "text",
    "varchar",
]


@pytest.mark.parametrize("type_name", _SHADOWING_ENUM_NAMES)
async def test_enum_named_after_builtin_type_wins(
    test_project: ProjectBuilder, type_name: LiteralString
) -> None:
    # Schema-qualified on purpose. For a name that is also a real typname (text) an
    # unqualified reference hands the column back to pg_catalog, so qualifying is the
    # only way to get a column whose type really is the user-defined one.
    table = f"shadow_{type_name}"
    await test_project.extend_schema(f"""
    CREATE TYPE public."{type_name}" AS ENUM ('first', 'second');
    CREATE TABLE {table} (
        id integer PRIMARY KEY,
        v public."{type_name}" NOT NULL
    );
    """)

    stmt = f"SELECT {table}.v as v FROM {table}"
    test_project.add_query("get_v", stmt)

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        mod = test_project.generate()

    assert not [
        str(w.message)
        for w in warning_messages
        if issubclass(w.category, UnknownSQLTypeWarning)
    ]

    enum_cls = getattr(mod, f"Testdb{type_name.capitalize()}")
    assert_return_types(mod.testdb_sql(stmt), {enum_cls})

    async with mod.testdb_connection() as conn:
        await conn.execute(f"INSERT INTO {table} (id, v) VALUES (1, 'first')")

    assert await mod.testdb_sql(stmt).query_single_row() is enum_cls.FIRST


async def test_type_overrides_apply_to_analyzer_type_name(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE readings (
        value double precision NOT NULL
    );
    """)

    stmt = "SELECT min(readings.value) as v FROM readings"
    test_project.add_query("get_min", stmt)

    mod = test_project.generate(type_overrides={"float8": "decimal.Decimal"})

    assert_return_types(mod.testdb_sql(stmt), {decimal.Decimal})


async def test_quoted_enum_name_resolves_in_parameter_position(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TYPE "camelCaseEnum" AS ENUM ('first', 'second');
    CREATE TABLE camel (
        id integer PRIMARY KEY,
        v "camelCaseEnum" NOT NULL
    );
    """)

    stmt = "SELECT camel.id as v FROM camel WHERE camel.v = @p"
    test_project.add_query("get_by_v", stmt)

    with warnings.catch_warnings(record=True) as warning_messages:
        warnings.simplefilter("always", UnknownSQLTypeWarning)
        mod = test_project.generate()

    assert not [
        str(w.message)
        for w in warning_messages
        if issubclass(w.category, UnknownSQLTypeWarning)
    ]

    enum_cls = mod.TestdbCamelCaseEnum

    param = query_signature(mod.testdb_sql(stmt)).parameters["p"]
    assert annotation_types(param.annotation) == {enum_cls}

    async with mod.testdb_connection() as conn:
        await conn.execute("INSERT INTO camel (id, v) VALUES (1, 'first')")

    assert await mod.testdb_sql(stmt).query_single_row(p=enum_cls.FIRST) == 1


async def test_type_overrides_apply_to_qualified_domain(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE DOMAIN public."integer" AS text;
    CREATE TABLE labels (
        id integer PRIMARY KEY,
        v public."integer" NOT NULL
    );
    """)

    stmt = "SELECT labels.v as v FROM labels"
    test_project.add_query("get_v", stmt)

    mod = test_project.generate(type_overrides={"integer": "str"})

    assert_return_types(mod.testdb_sql(stmt), {str})

    async with mod.testdb_connection() as conn:
        await conn.execute("INSERT INTO labels (id, v) VALUES (1, 'label')")

    assert await mod.testdb_sql(stmt).query_single_row() == "label"


async def test_type_overrides_reject_unused_type_name(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema("""
    CREATE TABLE measurements (
        value double precision NOT NULL
    );
    """)

    test_project.add_query(
        "get_value", "SELECT measurements.value as v FROM measurements"
    )

    with pytest.raises(ValueError, match="double precision") as excinfo:
        test_project.generate(type_overrides={"double precision": "float"})

    assert "float8" in str(excinfo.value)
