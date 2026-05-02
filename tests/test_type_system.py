import datetime
import ipaddress
import warnings
from enum import StrEnum
from typing import Any
from typing import cast
from typing import get_args

import pytest

from iron_sql.codegen import UnknownSQLTypeWarning
from tests.conftest import ProjectBuilder


def assert_return_types(query: Any, expected: set[type]) -> None:
    ret = cast(Any, type(query)).query_single_row.__annotations__["return"]
    ret_args = get_args(ret)
    actual: set[Any] = (
        {a for a in ret_args if a is not type(None)} if ret_args else {ret}
    )
    assert actual == expected


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

    test_project.add_query("get_cross_enum", "SELECT * FROM cross_schema_enum_table")

    mod = test_project.generate()

    enum_cls = mod.TestdbMood
    entity_cls = mod.TestdbCrossSchemaEnumTable

    assert entity_cls.__annotations__["mood"] is enum_cls


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
