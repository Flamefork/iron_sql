from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import assert_type

from tests.generated.result_matrix import testdb as api
from tests.generated.result_matrix.testdb import testdb_sql
from tests.json_models import UserMetadata


async def check(
    row_id: int,
    integer_required: int,
    integer_optional: int | None,
    mood_required: api.TestdbResultMood,
    mood_optional: api.TestdbResultMood | None,
    numbers_required: Sequence[int],
    numbers_optional: Sequence[int] | None,
    payload_required: UserMetadata,
    payload_optional: UserMetadata | None,
) -> None:
    assert_type(
        await testdb_sql(
            """INSERT INTO result_values (
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
        ).execute(
            row_id=row_id,
            integer_required=integer_required,
            integer_optional=integer_optional,
            mood_required=mood_required,
            mood_optional=mood_optional,
            numbers_required=numbers_required,
            numbers_optional=numbers_optional,
            payload_required=payload_required,
            payload_optional=payload_optional,
        ),
        None,
    )
    assert_type(
        await testdb_sql(
            "SELECT integer_required FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        int,
    )
    assert_type(
        await testdb_sql(
            "SELECT integer_optional FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        int | None,
    )
    assert_type(
        await testdb_sql(
            "SELECT mood_required FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        api.TestdbResultMood,
    )
    assert_type(
        await testdb_sql(
            "SELECT mood_optional FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        api.TestdbResultMood | None,
    )
    assert_type(
        await testdb_sql(
            "SELECT numbers_required FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        Sequence[int],
    )
    assert_type(
        await testdb_sql(
            "SELECT numbers_optional FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        Sequence[int] | None,
    )
    assert_type(
        await testdb_sql(
            "SELECT payload_required FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        UserMetadata,
    )
    assert_type(
        await testdb_sql(
            "SELECT payload_optional FROM result_values WHERE id = @row_id"
        ).query_single_row(row_id=row_id),
        UserMetadata | None,
    )

    scalar = testdb_sql("SELECT integer_required FROM result_values WHERE id = @row_id")
    assert_type(await scalar.query_all_rows(row_id=row_id), list[int])
    assert_type(await scalar.query_optional_row(row_id=row_id), int | None)
    assert_type(
        scalar.query_stream(row_id=row_id),
        AbstractAsyncContextManager[AsyncIterator[int]],
    )

    table = testdb_sql("SELECT * FROM result_values WHERE id = @row_id")
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
            "SELECT result_values.* FROM result_values WHERE id = 1"
        ).query_single_row(),
        api.TestdbResultValue,
    )

    anonymous = await testdb_sql(
        """SELECT
    integer_required AS required_value,
    mood_optional AS optional_status
FROM result_values
WHERE id = @row_id"""
    ).query_single_row(row_id=row_id)
    assert_type(anonymous.required_value, int)
    assert_type(anonymous.optional_status, api.TestdbResultMood | None)
    anonymous_reuse = await testdb_sql(
        """SELECT
    integer_required AS required_value,
    mood_optional AS optional_status
FROM result_values
WHERE id = 1"""
    ).query_single_row()
    assert_type(anonymous_reuse.required_value, int)
    assert_type(
        anonymous_reuse.optional_status,
        api.TestdbResultMood | None,
    )

    explicit = await testdb_sql(
        """SELECT
    numbers_required,
    payload_optional
FROM result_values
WHERE id = @row_id""",
        row_type="ResultMatrixRow",
    ).query_single_row(row_id=row_id)
    assert_type(explicit, api.ResultMatrixRow)
    assert_type(explicit.numbers_required, Sequence[int])
    assert_type(explicit.payload_optional, UserMetadata | None)
    assert_type(
        await testdb_sql(
            """SELECT
    numbers_required,
    payload_optional
FROM result_values
WHERE id = 1""",
            row_type="ResultMatrixRow",
        ).query_single_row(),
        api.ResultMatrixRow,
    )
