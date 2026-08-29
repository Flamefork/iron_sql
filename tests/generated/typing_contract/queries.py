import uuid
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import assert_type
from typing import reveal_type

from tests.generated.typing_contract import testdb as api
from tests.generated.typing_contract.testdb import testdb_sql
from tests.json_models import UserMetadata


async def check(uid: uuid.UUID, metadata: UserMetadata) -> None:
    command = testdb_sql(
        "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
    )
    assert_type(await command.execute(uid, "name", metadata), None)

    scalar = testdb_sql("SELECT id FROM users ORDER BY created_at")
    assert_type(await scalar.query_all_rows(), list[uuid.UUID])
    assert_type(await scalar.query_single_row(), uuid.UUID)
    assert_type(await scalar.query_optional_row(), uuid.UUID | None)
    assert_type(
        scalar.query_stream(),
        AbstractAsyncContextManager[AsyncIterator[uuid.UUID]],
    )

    nullable_query = testdb_sql("SELECT email FROM users WHERE id = $1")
    assert_type(await nullable_query.query_single_row(uid), str | None)

    assert_type(
        await testdb_sql(
            "SELECT id FROM users WHERE id = $1 AND username = @username"
        ).query_single_row(uid, username="name"),
        uuid.UUID,
    )

    enum_query = testdb_sql("SELECT $1::user_status as status")
    assert_type(
        await enum_query.query_single_row(api.TestdbUserStatus.ACTIVE),
        api.TestdbUserStatus,
    )

    array_query = testdb_sql("SELECT @values::int[] as values")
    assert_type(
        await array_query.query_single_row(values=[1, 2]),
        Sequence[int],
    )

    json_query = testdb_sql("SELECT metadata FROM users WHERE id = $1")
    assert_type(
        await json_query.query_single_row(uid),
        UserMetadata | None,
    )

    user_query = testdb_sql("SELECT * FROM users WHERE id = $1")
    user = await user_query.query_single_row(uid)
    assert_type(user, api.TestdbUser)
    assert_type(user.metadata, UserMetadata | None)

    anonymous = await testdb_sql(
        "SELECT id, is_active FROM users WHERE id = $1"
    ).query_single_row(uid)
    assert_type(anonymous.id, uuid.UUID)
    assert_type(anonymous.is_active, bool)
    reveal_type(anonymous)

    explicit = testdb_sql(
        "SELECT id, username FROM users",
        row_type="UserSummary",
    )
    assert_type(await explicit.query_single_row(), api.UserSummary)
