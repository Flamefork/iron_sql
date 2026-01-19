import keyword
from enum import StrEnum

from tests.conftest import ProjectBuilder


async def test_enum_generation(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "get_enum",
        "SELECT 'active'::user_status as status",
    )

    mod = test_project.generate()

    assert hasattr(mod, "TestdbUserStatus")
    enum_cls = mod.TestdbUserStatus
    assert issubclass(enum_cls, StrEnum)

    assert enum_cls.ACTIVE == "active"  # pyright: ignore[reportAttributeAccessIssue]
    assert enum_cls.INACTIVE == "inactive"  # pyright: ignore[reportAttributeAccessIssue]

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

    type_name = (
        enum_cls.split(".")[-1] if isinstance(enum_cls, str) else enum_cls.__name__
    )
    expected = f"Sequence[{type_name}]"
    annotation_str = str(entity_cls.__annotations__["tags"]).replace(
        "testapp_test_entity_generation_with_enum.testdb.", ""
    )
    assert expected in annotation_str
    assert (enum_cls if isinstance(enum_cls, str) else enum_cls.__name__) in str(
        entity_cls.__annotations__["tags"]
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

    assert hasattr(mod, "TestdbCamelcaseenum")
    assert hasattr(mod, "TestdbScreamingEnum")


async def test_enum_value_name_normalization(test_project: ProjectBuilder) -> None:
    extra_schema = """
    CREATE TYPE weird_enum AS ENUM ('1st', 'foo-bar', 'foo_bar');
    """

    await test_project.extend_schema(extra_schema)

    test_project.add_query("get_weird_enum", "SELECT '1st'::weird_enum as status")

    mod = test_project.generate()

    enum_cls = mod.TestdbWeirdEnum
    expected = {"1st", "foo-bar", "foo_bar"}

    assert {member.value for member in enum_cls} == expected
    assert len(enum_cls.__members__) == len(expected)
    for name in enum_cls.__members__:
        assert name.isidentifier()
        assert not keyword.iskeyword(name.lower())


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
