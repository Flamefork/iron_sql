import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from dataclasses import fields

import pytest

from tests.conftest import GeneratedTestDB
from tests.conftest import generated_package
from tests.json_models import UserMetadata

RESULT_MATRIX_PACKAGE = "result_matrix"


@dataclass(kw_only=True, frozen=True)
class ResultCell:
    name: str
    sql_definition: str
    python_type: str


RESULT_CELLS = (
    ResultCell(
        name="integer_required",
        sql_definition="INTEGER NOT NULL",
        python_type="int",
    ),
    ResultCell(
        name="integer_optional",
        sql_definition="INTEGER",
        python_type="int | None",
    ),
    ResultCell(
        name="mood_required",
        sql_definition="result_mood NOT NULL",
        python_type="api.TestdbResultMood",
    ),
    ResultCell(
        name="mood_optional",
        sql_definition="result_mood",
        python_type="api.TestdbResultMood | None",
    ),
    ResultCell(
        name="numbers_required",
        sql_definition="INTEGER[] NOT NULL",
        python_type="Sequence[int]",
    ),
    ResultCell(
        name="numbers_optional",
        sql_definition="INTEGER[]",
        python_type="Sequence[int] | None",
    ),
    ResultCell(
        name="payload_required",
        sql_definition="JSONB NOT NULL",
        python_type="UserMetadata",
    ),
    ResultCell(
        name="payload_optional",
        sql_definition="JSONB",
        python_type="UserMetadata | None",
    ),
)

INSERT_SQL = """INSERT INTO result_values (
    id,
    integer_required,
    integer_optional,
    mood_required,
    mood_optional,
    numbers_required,
    numbers_optional,
    payload_required,
    payload_optional
) VALUES (
    @row_id,
    @integer_required,
    @integer_optional?,
    @mood_required,
    @mood_optional?,
    @numbers_required,
    @numbers_optional?,
    @payload_required,
    @payload_optional?
)"""
TABLE_SQL = "SELECT * FROM result_values WHERE id = @row_id"
TABLE_REUSE_SQL = "SELECT result_values.* FROM result_values WHERE id = 1"
ANONYMOUS_SQL = """SELECT
    integer_required AS required_value,
    mood_optional AS optional_status
FROM result_values
WHERE id = @row_id"""
ANONYMOUS_REUSE_SQL = """SELECT
    integer_required AS required_value,
    mood_optional AS optional_status
FROM result_values
WHERE id = 1"""
EXPLICIT_SQL = """SELECT
    numbers_required,
    payload_optional
FROM result_values
WHERE id = @row_id"""
EXPLICIT_REUSE_SQL = """SELECT
    numbers_required,
    payload_optional
FROM result_values
WHERE id = 1"""
SCALAR_SQLS = {
    cell.name: f"SELECT {cell.name} FROM result_values WHERE id = @row_id"
    for cell in RESULT_CELLS
}


def result_matrix_schema() -> str:
    columns = ",\n".join(
        f"            {cell.name} {cell.sql_definition}" for cell in RESULT_CELLS
    )
    return f"""CREATE TYPE result_mood AS ENUM ('happy', 'calm');

        CREATE TABLE result_values (
            id INTEGER PRIMARY KEY,
{columns}
        );
    """


def result_matrix_queries() -> str:
    parameters = ",\n    ".join(
        f"{cell.name}: {cell.python_type}" for cell in RESULT_CELLS
    )
    arguments = "\n".join(
        f"            {cell.name}={cell.name}," for cell in RESULT_CELLS
    )
    scalar_assertions = "\n".join(
        f"""    assert_type(
        await testdb_sql(
            {json.dumps(SCALAR_SQLS[cell.name])}
        ).query_single_row(row_id=row_id),
        {cell.python_type},
    )"""
        for cell in RESULT_CELLS
    )
    anonymous_field_assertions = """    assert_type(anonymous.required_value, int)
    assert_type(anonymous.optional_status, api.TestdbResultMood | None)"""
    insert_literal = f'"""{INSERT_SQL}"""'
    anonymous_literal = f'"""{ANONYMOUS_SQL}"""'
    anonymous_reuse_literal = f'"""{ANONYMOUS_REUSE_SQL}"""'
    explicit_literal = f'"""{EXPLICIT_SQL}"""'
    explicit_reuse_literal = f'"""{EXPLICIT_REUSE_SQL}"""'
    return f"""from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import assert_type

from tests.generated.result_matrix import testdb as api
from tests.generated.result_matrix.testdb import testdb_sql
from tests.json_models import UserMetadata


async def check(
    row_id: int,
    {parameters},
) -> None:
    assert_type(
        await testdb_sql(
            {insert_literal}
        ).execute(
            row_id=row_id,
{arguments}
        ),
        None,
    )
{scalar_assertions}

    scalar = testdb_sql({json.dumps(SCALAR_SQLS["integer_required"])})
    assert_type(await scalar.query_all_rows(row_id=row_id), list[int])
    assert_type(await scalar.query_optional_row(row_id=row_id), int | None)
    assert_type(
        scalar.query_stream(row_id=row_id),
        AbstractAsyncContextManager[AsyncIterator[int]],
    )

    table = testdb_sql({json.dumps(TABLE_SQL)})
    assert_type(await table.query_all_rows(row_id=row_id), list[api.TestdbResultValue])
    assert_type(await table.query_single_row(row_id=row_id), api.TestdbResultValue)
    assert_type(
        await table.query_optional_row(row_id=row_id),
        api.TestdbResultValue | None,
    )
    assert_type(
        table.query_stream(row_id=row_id),
        AbstractAsyncContextManager[AsyncIterator[api.TestdbResultValue]],
    )
    assert_type(
        await testdb_sql(
            {json.dumps(TABLE_REUSE_SQL)}
        ).query_single_row(),
        api.TestdbResultValue,
    )

    anonymous = await testdb_sql(
        {anonymous_literal}
    ).query_single_row(row_id=row_id)
{anonymous_field_assertions}
    anonymous_reuse = await testdb_sql(
        {anonymous_reuse_literal}
    ).query_single_row()
    assert_type(anonymous_reuse.required_value, int)
    assert_type(
        anonymous_reuse.optional_status,
        api.TestdbResultMood | None,
    )

    explicit = await testdb_sql(
        {explicit_literal},
        row_type="ResultMatrixRow",
    ).query_single_row(row_id=row_id)
    assert_type(explicit, api.ResultMatrixRow)
    assert_type(explicit.numbers_required, Sequence[int])
    assert_type(explicit.payload_optional, UserMetadata | None)
    assert_type(
        await testdb_sql(
            {explicit_reuse_literal},
            row_type="ResultMatrixRow",
        ).query_single_row(),
        api.ResultMatrixRow,
    )
"""


generated_package(
    RESULT_MATRIX_PACKAGE,
    schema=result_matrix_schema(),
    queries=result_matrix_queries(),
    json_model_overrides={
        "result_values.payload_required": "tests.json_models:UserMetadata",
        "result_values.payload_optional": "tests.json_models:UserMetadata",
    },
)

from tests.generated.result_matrix import testdb

RUNTIME_VALUES: dict[int, dict[str, object]] = {
    1: {
        "integer_required": 17,
        "integer_optional": None,
        "mood_required": testdb.TestdbResultMood.HAPPY,
        "mood_optional": None,
        "numbers_required": [2, 3, 5],
        "numbers_optional": None,
        "payload_required": UserMetadata(key="required", value="value"),
        "payload_optional": None,
    },
    2: {
        "integer_required": 19,
        "integer_optional": 23,
        "mood_required": testdb.TestdbResultMood.CALM,
        "mood_optional": testdb.TestdbResultMood.HAPPY,
        "numbers_required": [7, 11],
        "numbers_optional": [13, 17],
        "payload_required": UserMetadata(key="required-populated", value="value"),
        "payload_optional": UserMetadata(key="optional-populated", value="value"),
    },
}


@pytest.fixture(autouse=True)
async def use_generated_database(
    generated_test_db: GeneratedTestDB,
) -> AsyncGenerator[None]:
    async with generated_test_db("result_matrix"):
        yield


async def insert_runtime_values() -> None:
    await testdb.testdb_sql(INSERT_SQL).execute(
        row_id=1,
        integer_required=17,
        integer_optional=None,
        mood_required=testdb.TestdbResultMood.HAPPY,
        mood_optional=None,
        numbers_required=[2, 3, 5],
        numbers_optional=None,
        payload_required=UserMetadata(key="required", value="value"),
        payload_optional=None,
    )
    await testdb.testdb_sql(INSERT_SQL).execute(
        row_id=2,
        integer_required=19,
        integer_optional=23,
        mood_required=testdb.TestdbResultMood.CALM,
        mood_optional=testdb.TestdbResultMood.HAPPY,
        numbers_required=[7, 11],
        numbers_optional=[13, 17],
        payload_required=UserMetadata(key="required-populated", value="value"),
        payload_optional=UserMetadata(key="optional-populated", value="value"),
    )


async def test_every_result_cell_survives_named_parameter_round_trip() -> None:
    assert len(RESULT_CELLS) == 8
    assert len(RUNTIME_VALUES) == 2
    assert all(
        set(values) == {cell.name for cell in RESULT_CELLS}
        for values in RUNTIME_VALUES.values()
    )

    await insert_runtime_values()
    for row_id, expected in RUNTIME_VALUES.items():
        row = await testdb.testdb_sql(TABLE_SQL).query_single_row(row_id=row_id)

        assert len(fields(row)) == len(RESULT_CELLS) + 1
        assert {field.name for field in fields(row)} == {
            "id",
            *(cell.name for cell in RESULT_CELLS),
        }
        assert row.id == row_id
        for cell in RESULT_CELLS:
            assert getattr(row, cell.name) == expected[cell.name]


async def test_scalar_and_structured_methods_use_the_same_runtime_values() -> None:
    await insert_runtime_values()

    for row_id, expected in RUNTIME_VALUES.items():
        actual = {
            "integer_required": await testdb.testdb_sql(
                "SELECT integer_required FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "integer_optional": await testdb.testdb_sql(
                "SELECT integer_optional FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "mood_required": await testdb.testdb_sql(
                "SELECT mood_required FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "mood_optional": await testdb.testdb_sql(
                "SELECT mood_optional FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "numbers_required": await testdb.testdb_sql(
                "SELECT numbers_required FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "numbers_optional": await testdb.testdb_sql(
                "SELECT numbers_optional FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "payload_required": await testdb.testdb_sql(
                "SELECT payload_required FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
            "payload_optional": await testdb.testdb_sql(
                "SELECT payload_optional FROM result_values WHERE id = @row_id"
            ).query_single_row(row_id=row_id),
        }
        assert actual == expected

    scalar = testdb.testdb_sql(
        "SELECT integer_required FROM result_values WHERE id = @row_id"
    )
    assert await scalar.query_all_rows(row_id=1) == [
        RUNTIME_VALUES[1]["integer_required"]
    ]
    assert (
        await scalar.query_optional_row(row_id=1)
        == RUNTIME_VALUES[1]["integer_required"]
    )
    async with scalar.query_stream(row_id=1) as stream:
        assert [value async for value in stream] == [
            RUNTIME_VALUES[1]["integer_required"]
        ]

    table = testdb.testdb_sql(TABLE_SQL)
    all_rows = await table.query_all_rows(row_id=1)
    assert len(all_rows) == 1
    assert await table.query_optional_row(row_id=1) == all_rows[0]
    async with table.query_stream(row_id=1) as stream:
        assert [row async for row in stream] == all_rows


async def test_structured_result_specs_reuse_entities() -> None:
    await insert_runtime_values()

    table = await testdb.testdb_sql(TABLE_SQL).query_single_row(row_id=1)
    table_reuse = await testdb.testdb_sql(TABLE_REUSE_SQL).query_single_row()
    anonymous = await testdb.testdb_sql(ANONYMOUS_SQL).query_single_row(row_id=1)
    anonymous_reuse = await testdb.testdb_sql(ANONYMOUS_REUSE_SQL).query_single_row()
    explicit = await testdb.testdb_sql(
        EXPLICIT_SQL,
        row_type="ResultMatrixRow",
    ).query_single_row(row_id=1)
    explicit_reuse = await testdb.testdb_sql(
        EXPLICIT_REUSE_SQL,
        row_type="ResultMatrixRow",
    ).query_single_row()

    assert type(table) is testdb.TestdbResultValue
    assert type(table_reuse) is type(table)
    assert type(anonymous_reuse) is type(anonymous)
    assert type(explicit) is testdb.ResultMatrixRow
    assert type(explicit_reuse) is type(explicit)
    assert len(fields(anonymous)) == 2
    assert len(fields(explicit)) == 2
