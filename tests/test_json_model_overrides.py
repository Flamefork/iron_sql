import uuid
from collections.abc import AsyncGenerator

import pydantic
import pytest

from tests.conftest import SCHEMA_SQL
from tests.conftest import GeneratedTestDB
from tests.conftest import ProjectBuilder
from tests.conftest import generated_package
from tests.json_models import Tag
from tests.json_models import UserMetadata

generated_package(
    "json_users_metadata",
    schema=SCHEMA_SQL,
    queries="""
        from tests.generated.json_users_metadata.testdb import testdb_sql

        testdb_sql("INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)")
        testdb_sql("SELECT * FROM users WHERE id = $1")
        testdb_sql("SELECT metadata FROM users WHERE id = $1")
        testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
    """,
    json_model_overrides={
        "users.metadata": "tests.json_models:UserMetadata",
    },
)
generated_package(
    "json_payload",
    schema=SCHEMA_SQL,
    queries='''
        from tests.generated.json_payload.testdb import testdb_sql

        testdb_sql("""INSERT INTO json_payloads (payload) VALUES ($1)
        RETURNING id, payload""")
    ''',
    json_model_overrides={
        "json_payloads.payload": "tests.json_models:UserMetadata",
    },
)
generated_package(
    "json_text",
    schema=f"""{SCHEMA_SQL}
        CREATE TABLE text_json (
            id SERIAL PRIMARY KEY,
            data TEXT NOT NULL
        );
    """,
    queries="""
        from tests.generated.json_text.testdb import testdb_sql

        testdb_sql("INSERT INTO text_json (data) VALUES ($1) RETURNING id, data")
    """,
    json_model_overrides={"text_json.data": "tests.json_models:UserMetadata"},
)
generated_package(
    "json_varchar",
    schema=f"""{SCHEMA_SQL}
        CREATE TABLE varchar_json (
            id SERIAL PRIMARY KEY,
            data VARCHAR NOT NULL
        );
    """,
    queries="""
        from tests.generated.json_varchar.testdb import testdb_sql

        testdb_sql("INSERT INTO varchar_json (data) VALUES ($1) RETURNING id, data")
    """,
    json_model_overrides={"varchar_json.data": "tests.json_models:UserMetadata"},
)
generated_package(
    "json_tag_list",
    schema=f"""{SCHEMA_SQL}
        CREATE TABLE tagged_items (
            id SERIAL PRIMARY KEY,
            tags JSONB NOT NULL
        );
    """,
    queries="""
        from tests.generated.json_tag_list.testdb import testdb_sql

        testdb_sql("INSERT INTO tagged_items (tags) VALUES ($1) RETURNING id, tags")
    """,
    json_model_overrides={"tagged_items.tags": "tests.json_models:TagList"},
)
generated_package(
    "json_priority",
    schema=SCHEMA_SQL,
    queries="""
        from tests.generated.json_priority.testdb import testdb_sql

        testdb_sql("SELECT * FROM users WHERE id = $1")
        testdb_sql("INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)")
    """,
    type_overrides={"jsonb": "str"},
    json_model_overrides={
        "users.metadata": "tests.json_models:UserMetadata",
    },
)

from tests.generated.json_payload import testdb as json_payload
from tests.generated.json_priority import testdb as json_priority
from tests.generated.json_tag_list import testdb as json_tag_list
from tests.generated.json_text import testdb as json_text
from tests.generated.json_users_metadata import testdb as json_users_metadata
from tests.generated.json_varchar import testdb as json_varchar


@pytest.fixture
async def use_users_metadata_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_users_metadata"):
        yield


@pytest.fixture
async def use_json_payload_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_payload"):
        yield


@pytest.fixture
async def use_json_text_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_text"):
        yield


@pytest.fixture
async def use_json_varchar_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_varchar"):
        yield


@pytest.fixture
async def use_json_tag_list_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_tag_list"):
        yield


@pytest.fixture
async def use_json_priority_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("json_priority"):
        yield


@pytest.mark.usefixtures("use_users_metadata_database")
async def test_jsonb_override_read_valid() -> None:
    insert_sql = "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
    select_sql = "SELECT * FROM users WHERE id = $1"

    mod = json_users_metadata

    uid = uuid.uuid4()
    data = UserMetadata(key="lang", value="en")

    await mod.testdb_sql(insert_sql).execute(uid, "u1", data)

    row = await mod.testdb_sql(select_sql).query_single_row(uid)
    assert isinstance(row.metadata, UserMetadata)
    assert row.metadata.key == "lang"
    assert row.metadata.value == "en"


@pytest.mark.usefixtures("use_users_metadata_database")
async def test_jsonb_override_read_invalid() -> None:
    select_sql = "SELECT * FROM users WHERE id = $1"

    mod = json_users_metadata

    uid = uuid.uuid4()
    async with mod.testdb_connection() as conn:
        await conn.execute(
            "INSERT INTO users (id, username, metadata) VALUES (%s, %s, %s)",
            (uid, "u1", '{"bad_field": 123}'),
        )

    with pytest.raises(pydantic.ValidationError):
        await mod.testdb_sql(select_sql).query_single_row(uid)


@pytest.mark.usefixtures("use_json_payload_database")
async def test_json_override_read_write() -> None:
    insert_sql = """INSERT INTO json_payloads (payload) VALUES ($1)
RETURNING id, payload"""
    mod = json_payload

    data = UserMetadata(key="theme", value="dark")
    row = await mod.testdb_sql(insert_sql).query_single_row(data)
    assert isinstance(row.payload, UserMetadata)
    assert row.payload == data


@pytest.mark.usefixtures("use_json_text_database")
async def test_text_override_read_write() -> None:
    insert_sql = "INSERT INTO text_json (data) VALUES ($1) RETURNING id, data"
    mod = json_text

    data = UserMetadata(key="mode", value="light")
    row = await mod.testdb_sql(insert_sql).query_single_row(data)
    assert isinstance(row.data, UserMetadata)
    assert row.data == data


@pytest.mark.usefixtures("use_json_varchar_database")
async def test_varchar_override_read_write() -> None:
    insert_sql = "INSERT INTO varchar_json (data) VALUES ($1) RETURNING id, data"
    mod = json_varchar

    data = UserMetadata(key="mode", value="compact")
    row = await mod.testdb_sql(insert_sql).query_single_row(data)
    assert isinstance(row.data, UserMetadata)
    assert row.data == data


@pytest.mark.usefixtures("use_users_metadata_database")
async def test_nullable_jsonb_override() -> None:
    select_sql = "SELECT * FROM users WHERE id = $1"
    insert_sql = "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"

    mod = json_users_metadata

    uid_with = uuid.uuid4()
    uid_without = uuid.uuid4()

    data = UserMetadata(key="k", value="v")
    await mod.testdb_sql(insert_sql).execute(uid_with, "with_meta", data)
    await mod.testdb_sql(insert_sql).execute(uid_without, "no_meta", None)

    row_with = await mod.testdb_sql(select_sql).query_single_row(uid_with)
    assert isinstance(row_with.metadata, UserMetadata)
    assert row_with.metadata == data

    row_without = await mod.testdb_sql(select_sql).query_single_row(uid_without)
    assert row_without.metadata is None


@pytest.mark.usefixtures("use_json_tag_list_database")
async def test_list_model_override() -> None:
    insert_sql = "INSERT INTO tagged_items (tags) VALUES ($1) RETURNING id, tags"
    mod = json_tag_list

    tags = [Tag(name="python", color="blue"), Tag(name="rust", color="orange")]
    row = await mod.testdb_sql(insert_sql).query_single_row(tags)
    row_tags: list[Tag] = row.tags
    assert isinstance(row_tags, list)
    assert len(row_tags) == 2
    assert isinstance(row_tags[0], Tag)
    assert row_tags[0].name == "python"


@pytest.mark.usefixtures("use_users_metadata_database")
async def test_scalar_result_override() -> None:
    insert_sql = "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
    select_sql = "SELECT metadata FROM users WHERE id = $1"

    mod = json_users_metadata

    uid = uuid.uuid4()
    data = UserMetadata(key="s", value="v")
    await mod.testdb_sql(insert_sql).execute(uid, "scalar_user", data)

    result = await mod.testdb_sql(select_sql).query_single_row(uid)
    assert isinstance(result, UserMetadata)
    assert result.key == "s"


@pytest.mark.usefixtures("use_users_metadata_database")
async def test_scalar_nullable_override_none() -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT metadata FROM users WHERE id = $1"

    mod = json_users_metadata

    uid = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid, "no_meta")

    result = await mod.testdb_sql(select_sql).query_optional_row(uid)
    assert result is None


def test_invalid_config_key_without_dot(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    with pytest.raises(ValueError, match=r"must be 'table\.column'"):
        test_project.generate_no_import(
            json_model_overrides={
                "users_metadata": "tests.json_models:UserMetadata",
            },
        )


def test_invalid_config_value_without_colon(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    with pytest.raises(ValueError, match="must be 'module:Class'"):
        test_project.generate_no_import(
            json_model_overrides={
                "users.metadata": "tests.json_models.UserMetadata",
            },
        )


def test_invalid_config_nonexistent_table(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    with pytest.raises(ValueError, match="table 'no_such_table' not found"):
        test_project.generate_no_import(
            json_model_overrides={
                "no_such_table.col": "tests.json_models:UserMetadata",
            },
        )


def test_invalid_config_nonexistent_column(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    with pytest.raises(ValueError, match="column 'no_col' not found in table 'users'"):
        test_project.generate_no_import(
            json_model_overrides={
                "users.no_col": "tests.json_models:UserMetadata",
            },
        )


def test_invalid_config_non_json_column(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    with pytest.raises(ValueError, match="has type 'bool'"):
        test_project.generate_no_import(
            json_model_overrides={
                "users.is_active": "tests.json_models:UserMetadata",
            },
        )


@pytest.mark.usefixtures("use_json_priority_database")
async def test_json_model_overrides_priority_over_type_overrides() -> None:
    select_sql = "SELECT * FROM users WHERE id = $1"
    insert_sql = "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"

    mod = json_priority

    uid = uuid.uuid4()
    data = UserMetadata(key="p", value="q")
    await mod.testdb_sql(insert_sql).execute(uid, "prio_user", data)

    row = await mod.testdb_sql(select_sql).query_single_row(uid)
    assert isinstance(row.metadata, UserMetadata)
    assert row.metadata == data


def test_json_model_overrides_qualify_same_class_name_by_module(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "json_models_a.py").write_text(
        """
from pydantic import BaseModel


class Payload(BaseModel):
    value_a: str
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (test_project.app_dir / "json_models_b.py").write_text(
        """
from pydantic import BaseModel


class Payload(BaseModel):
    value_b: int
""".strip()
        + "\n",
        encoding="utf-8",
    )

    test_project.add_query("sel_user", "SELECT metadata FROM users WHERE id = $1")
    test_project.add_query(
        "sel_payload",
        "SELECT payload FROM json_payloads WHERE id = $1",
    )

    module_a = f"{test_project.app_pkg}.json_models_a"
    module_b = f"{test_project.app_pkg}.json_models_b"
    changed, _ = test_project.generate_checked(
        json_model_overrides={
            "users.metadata": f"{module_a}:Payload",
            "json_payloads.payload": f"{module_b}:Payload",
        },
    )
    assert changed is True

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text(encoding="utf-8")

    assert f"import {module_a}" in generated
    assert f"import {module_b}" in generated
    assert f"{module_a}.Payload" in generated
    assert f"{module_b}.Payload" in generated


def test_json_model_overrides_order_does_not_change_generated_module(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query(
        "sel",
        """SELECT users.metadata, json_payloads.payload
FROM users CROSS JOIN json_payloads WHERE users.id = $1""",
    )
    overrides = {
        "users.metadata": "tests.json_models:UserMetadata",
        "json_payloads.payload": "tests.json_models:TagList",
    }

    first_changed, _ = test_project.generate_checked(json_model_overrides=overrides)
    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    first_source = generated_path.read_bytes()
    assert first_source.count(b"import tests.json_models\n") == 1
    assert b"tests.json_models.UserMetadata" in first_source
    assert b"tests.json_models.TagList" in first_source
    assert b"_iron_sql_json" not in first_source

    second_changed, _ = test_project.generate_checked(
        json_model_overrides=dict(reversed(overrides.items())),
    )

    assert first_changed is True
    assert second_changed is False
    assert generated_path.read_bytes() == first_source
