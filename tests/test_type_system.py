from __future__ import annotations

import datetime
import decimal
import inspect
import ipaddress
import warnings
from enum import StrEnum
from typing import TYPE_CHECKING
from typing import LiteralString
from typing import cast
from typing import get_args

import pytest

from iron_sql.codegen import UnknownSQLTypeWarning
from iron_sql.runtime import ConnectionPool

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from collections.abc import Callable
    from collections.abc import Set as AbstractSet
    from contextlib import AbstractAsyncContextManager
    from types import ModuleType

    import psycopg

    from tests.conftest import ProjectBuilder


def module_value(module: ModuleType, name: str) -> object:
    return cast("dict[str, object]", vars(module))[name]


def generated_enum(module: ModuleType, name: str) -> type[StrEnum]:
    value = module_value(module, name)
    if not inspect.isclass(value) or not issubclass(value, StrEnum):
        msg = f"{module.__name__}.{name} is not a StrEnum"
        raise TypeError(msg)
    return value


def generated_class(module: ModuleType, name: str) -> type[object]:
    value = module_value(module, name)
    if not inspect.isclass(value):
        msg = f"{module.__name__}.{name} is not a class"
        raise TypeError(msg)
    return value


def sql_query(module: ModuleType, sql: str) -> object:
    dispatcher = cast("Callable[[str], object]", module_value(module, "testdb_sql"))
    return dispatcher(sql)


async def query_single_row(
    module: ModuleType,
    sql: str,
    *params: object,
    **named_params: object,
) -> object:
    return await query_single_row_for(sql_query(module, sql), *params, **named_params)


async def query_single_row_for(
    query: object, *params: object, **named_params: object
) -> object:
    method_name = "query_single_row"
    method = cast("Callable[..., Awaitable[object]]", getattr(query, method_name))
    return await method(*params, **named_params)


def generated_connection(
    module: ModuleType,
) -> AbstractAsyncContextManager[psycopg.AsyncConnection[object]]:
    factory = cast(
        "Callable[[], AbstractAsyncContextManager[psycopg.AsyncConnection[object]]]",
        module_value(module, "testdb_connection"),
    )
    return factory()


def query_signature(query: object) -> inspect.Signature:
    method = cast("object", vars(type(query))["query_single_row"])
    if not callable(method):
        msg = f"{type(query).__name__}.query_single_row is not callable"
        raise TypeError(msg)
    return inspect.signature(method)


def annotation_types(annotation: object) -> set[object]:
    args = cast("tuple[object, ...]", get_args(annotation))
    return {item for item in args if item is not type(None)} if args else {annotation}


def assert_return_types(query: object, expected: AbstractSet[object]) -> None:
    returned = cast("object", query_signature(query).return_annotation)
    assert annotation_types(returned) == expected


async def test_enum_generation(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "get_enum",
        "SELECT 'active'::user_status as status",
    )

    mod = test_project.generate()

    enum_cls = generated_enum(mod, "TestdbUserStatus")

    active = enum_cls["ACTIVE"]
    inactive = enum_cls["INACTIVE"]
    assert active == "active"
    assert inactive == "inactive"

    row = await query_single_row(mod, "SELECT 'active'::user_status as status")
    assert isinstance(row, enum_cls)
    assert row == active
    assert row == "active"  # StrEnum acts as str


async def test_enum_parameter(test_project: ProjectBuilder) -> None:
    sql = "SELECT $1::user_status as status"
    test_project.add_query("echo_status", sql)

    mod = test_project.generate()
    enum_cls = generated_enum(mod, "TestdbUserStatus")
    active = enum_cls["ACTIVE"]

    idx = await query_single_row(mod, sql, active)
    assert idx == active

    idx = await query_single_row(mod, sql, "active")
    assert idx == active


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

    enum_cls = generated_enum(mod, "TestdbUserStatus")
    entity_cls = generated_class(mod, "TestdbEnumTestTable")
    annotations = cast("dict[str, object]", inspect.get_annotations(entity_cls))

    assert annotations["status"] is enum_cls

    tags_annotation = annotations["tags"]
    tag_args = cast("tuple[object, ...]", get_args(tags_annotation))
    # Sequence[EnumCls] | None — unwrap the union
    seq_type = next(item for item in tag_args if item is not type(None))
    (inner_type,) = cast("tuple[object]", get_args(seq_type))
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
    assert "runtime.typed_array_row(builtins.int, not_null=True)" in generated
    assert "runtime.typed_array_row(builtins.int, not_null=False)" in generated

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO array_results (id, scores, extra) VALUES (%s, %s, %s)",
            (1, [1, 2, 3], None),
        )
        await conn.execute(
            "INSERT INTO array_results (id, scores, extra) VALUES (%s, %s, %s)",
            (2, [4], [5, 6]),
        )

    assert await query_single_row(mod, not_null_sql, 1) == [1, 2, 3]
    assert await query_single_row(mod, nullable_sql, 1) is None
    assert await query_single_row(mod, nullable_sql, 2) == [5, 6]


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
    status_enum = generated_enum(mod, "TestdbUserStatus")
    active = status_enum["ACTIVE"]
    inactive = status_enum["INACTIVE"]

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO enum_results (id, status, tags) VALUES (%s, %s, %s)",
            (1, active, [active, inactive]),
        )

    row = await query_single_row(mod, multi_sql, 1)
    fields = cast("dict[str, object]", vars(row))
    assert fields["status"] is active
    tags_value = fields["tags"]
    assert isinstance(tags_value, list)
    tags_list = cast("list[object]", tags_value)
    assert tags_list == [active, inactive]
    assert all(isinstance(tag, status_enum) for tag in tags_list)

    assert await query_single_row(mod, scalar_sql, 1) is active

    tags = await query_single_row(mod, array_sql, 1)
    assert isinstance(tags, list)
    tag_list = cast("list[object]", tags)
    assert tag_list == [active, inactive]
    assert all(isinstance(tag, status_enum) for tag in tag_list)


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
    status_enum = generated_enum(mod, "TestdbUserStatus")
    active = status_enum["ACTIVE"]
    inactive = status_enum["INACTIVE"]

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO nullable_enum (id, status, tags) VALUES (%s, %s, %s)",
            (1, None, None),
        )
        await conn.execute(
            "INSERT INTO nullable_enum (id, status, tags) VALUES (%s, %s, %s)",
            (2, active, [inactive]),
        )

    assert await query_single_row(mod, scalar_sql, 1) is None
    assert await query_single_row(mod, scalar_sql, 2) is active
    assert await query_single_row(mod, array_sql, 1) is None
    assert await query_single_row(mod, array_sql, 2) == [inactive]


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
    phase_enum = generated_enum(mod, "TestdbTaskPhase")
    in_progress = phase_enum["IN_PROGRESS"]

    # Member names are normalized, but values keep the raw PostgreSQL labels —
    # so registration must map by value, not by member name.
    assert in_progress == "in-progress"
    assert phase_enum["NUM2FA"] == "2fa"

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO phase_table (id, phase) VALUES (%s, %s)",
            (1, in_progress),
        )

    assert await query_single_row(mod, select_sql, 1) is in_progress


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

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO empty_enum_array (id, tags) VALUES (1, '{}'::user_status[])"
        )

    assert await query_single_row(mod, select_sql, 1) == []


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
    status_enum = generated_enum(mod, "TestdbUserStatus")
    color_enum = generated_enum(mod, "TestdbColor")
    active = status_enum["ACTIVE"]
    red = color_enum["RED"]

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO multi_enum (id, status, color) VALUES (%s, %s, %s)",
            (1, active, red),
        )

    row = await query_single_row(mod, select_sql, 1)
    fields = cast("dict[str, object]", vars(row))
    assert fields["status"] is active
    assert fields["color"] is red


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
            query = sql_query(mod, select_sql)
            with_connection_name = "with_connection"
            with_connection = cast(
                "Callable[[psycopg.AsyncConnection[object]], object]",
                getattr(query, with_connection_name),
            )
            bound_query = with_connection(conn)
            with pytest.raises(TypeError, match="Expected scalar of type"):
                await query_single_row_for(bound_query)
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

    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO network (id, addr) VALUES (1, NULL), (2, '10.0.0.1')"
        )

    assert await query_single_row(mod, select_sql, 1) is None
    assert await query_single_row(mod, select_sql, 2) == ipaddress.ip_address(
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

    enum_cls = generated_enum(mod, "TestdbWeirdEnum")
    assert enum_cls["NUM1ST"] == "1st"
    assert enum_cls["FOO_BAR"] == "foo-bar"
    assert enum_cls["FOO_BAR_2"] == "foo_bar"

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

    enum_cls = generated_enum(mod, "TestdbEmptyLabelEnum")
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

    enum_cls = generated_enum(mod, "TestdbMood")
    entity_cls = generated_class(mod, "TestdbCrossSchemaEnumTable")
    annotations = cast("dict[str, object]", inspect.get_annotations(entity_cls))
    happy = enum_cls["HAPPY"]

    assert annotations["mood"] is enum_cls

    # The enum lives in a non-public schema, so its type is registered by its
    # schema-qualified name.
    async with generated_connection(mod) as conn:
        await conn.execute(
            "INSERT INTO cross_schema_enum_table (mood) VALUES (%s)",
            (happy,),
        )

    row = await query_single_row(mod, select_sql)
    fields = cast("dict[str, object]", vars(row))
    assert fields["mood"] is happy


async def test_pg_catalog_type_does_not_break_generation(
    test_project: ProjectBuilder,
) -> None:
    sql = "SELECT 1::oid as oid"
    test_project.add_query("get_oid", sql)

    mod = test_project.generate()

    row = await query_single_row(mod, sql)
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

    entity_cls = generated_class(mod, "TestdbUnknownTable")
    annotations = cast("dict[str, object]", inspect.get_annotations(entity_cls))
    assert annotations["val"] is object


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
    inet_query = sql_query(mod, inet_stmt)
    assert_return_types(inet_query, inet_types)
    assert isinstance(await query_single_row_for(inet_query), tuple(inet_types))

    cidr_types = {ipaddress.IPv4Network, ipaddress.IPv6Network}
    cidr_query = sql_query(mod, cidr_stmt)
    assert_return_types(cidr_query, cidr_types)
    assert isinstance(await query_single_row_for(cidr_query), tuple(cidr_types))


async def test_standard_type_mapping_interval(test_project: ProjectBuilder) -> None:
    interval_stmt = "SELECT INTERVAL '1 day 2 hours' as v"

    test_project.add_query("get_interval", interval_stmt)

    mod = test_project.generate()

    interval_query = sql_query(mod, interval_stmt)
    assert_return_types(interval_query, {datetime.timedelta})
    assert isinstance(await query_single_row_for(interval_query), datetime.timedelta)


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
        q = sql_query(mod, stmt)
        assert_return_types(q, {str})
        assert isinstance(await query_single_row_for(q), str)


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

    q = sql_query(mod, stmt)
    assert_return_types(q, {int})

    val = await query_single_row_for(q)
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
            returned = cast(
                "object",
                query_signature(sql_query(mod, statements[col])).return_annotation,
            )
            assert annotation_types(returned) == {py_type}, (col, axis)

        param = query_signature(sql_query(mod, parametrized[col])).parameters["p"]
        annotation = cast("object", param.annotation)
        assert annotation_types(annotation) == {py_type}, (col, "param")


def test_type_overrides_ignored_when_no_queries(
    test_project: ProjectBuilder,
) -> None:
    mod = test_project.generate(type_overrides={"jsonb": "str"})

    with pytest.raises(KeyError, match="Unknown statement"):
        sql_query(mod, "SELECT * FROM users")


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

    enum_cls = generated_enum(mod, f"Testdb{type_name.capitalize()}")
    first = enum_cls["FIRST"]
    assert_return_types(sql_query(mod, stmt), {enum_cls})

    async with generated_connection(mod) as conn:
        await conn.execute(f"INSERT INTO {table} (id, v) VALUES (1, 'first')")

    assert await query_single_row(mod, stmt) is first


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

    assert_return_types(sql_query(mod, stmt), {decimal.Decimal})


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

    enum_cls = generated_enum(mod, "TestdbCamelCaseEnum")
    first = enum_cls["FIRST"]

    param = query_signature(sql_query(mod, stmt)).parameters["p"]
    annotation = cast("object", param.annotation)
    assert annotation_types(annotation) == {enum_cls}

    async with generated_connection(mod) as conn:
        await conn.execute("INSERT INTO camel (id, v) VALUES (1, 'first')")

    assert await query_single_row(mod, stmt, p=first) == 1


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

    assert_return_types(sql_query(mod, stmt), {str})

    async with generated_connection(mod) as conn:
        await conn.execute("INSERT INTO labels (id, v) VALUES (1, 'label')")

    assert await query_single_row(mod, stmt) == "label"


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


@pytest.mark.parametrize("expression", ["bad-name", "missing.Type"])
def test_type_overrides_reject_invalid_python_expression(
    test_project: ProjectBuilder,
    expression: str,
) -> None:
    test_project.add_query("q", "SELECT 1::float8 AS value")

    with pytest.raises(ValueError, match="type_overrides expression"):
        test_project.generate_no_import(type_overrides={"float8": expression})


def test_type_overrides_qualify_builtin_names(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT 1::int4 AS value")

    changed, _ = test_project.generate_checked(type_overrides={"int4": "int"})
    assert changed is True
    generated = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    ).read_text(encoding="utf-8")

    assert "runtime.typed_scalar_row(builtins.int, not_null=True)" in generated
