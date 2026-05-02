import uuid
from typing import Any

import pytest

from tests.conftest import ProjectBuilder


def test_query_stream_api_shape(test_project: ProjectBuilder) -> None:
    select_sql = "SELECT id, username FROM users ORDER BY created_at"
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"

    test_project.add_query("sel", select_sql)
    test_project.add_query("ins", insert_sql)

    mod = test_project.generate()

    assert hasattr(mod.testdb_sql(select_sql), "query_stream")
    assert not hasattr(mod.testdb_sql(insert_sql), "query_stream")


async def test_query_stream_roundtrip(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    uid1, uid2 = uuid.uuid4(), uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid1, "user1")
    await mod.testdb_sql(insert_sql).execute(uid2, "user2")

    async with mod.testdb_sql(select_sql).query_stream() as stream:
        streamed = [row async for row in stream]

    all_rows = await mod.testdb_sql(select_sql).query_all_rows()

    assert streamed == all_rows


async def test_query_stream_with_concurrent_queries(
    test_project: ProjectBuilder,
) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"
    update_sql = "UPDATE users SET is_active = $1 WHERE id = $2"
    count_sql = "SELECT count(*) as cnt FROM users WHERE is_active = false"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)
    test_project.add_query("upd", update_sql)
    test_project.add_query("cnt", count_sql)

    mod = test_project.generate()

    ids = [uuid.uuid4() for _ in range(5)]
    for i, uid in enumerate(ids):
        await mod.testdb_sql(insert_sql).execute(uid, f"user{i}")

    async with (
        mod.testdb_transaction(),
        mod.testdb_sql(select_sql).query_stream() as stream,
    ):
        async for row in stream:
            await mod.testdb_sql(update_sql).execute(False, row.id)

    count = await mod.testdb_sql(count_sql).query_single_row()
    assert count == 5


async def test_query_stream_early_exit(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"
    count_sql = "SELECT count(*) as cnt FROM users"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)
    test_project.add_query("cnt", count_sql)

    mod = test_project.generate()

    for i in range(10):
        await mod.testdb_sql(insert_sql).execute(uuid.uuid4(), f"user{i}")

    collected: list[Any] = []
    async with mod.testdb_sql(select_sql).query_stream() as stream:
        async for row in stream:
            collected.append(row)
            if len(collected) == 3:
                break

    assert len(collected) == 3

    count = await mod.testdb_sql(count_sql).query_single_row()
    assert count == 10


async def test_query_stream_scalar(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id FROM users ORDER BY created_at"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    ids = [uuid.uuid4() for _ in range(3)]
    for i, uid in enumerate(ids):
        await mod.testdb_sql(insert_sql).execute(uid, f"user{i}")

    async with mod.testdb_sql(select_sql).query_stream() as stream:
        streamed = [row async for row in stream]
    all_rows = await mod.testdb_sql(select_sql).query_all_rows()

    assert streamed == all_rows


async def test_query_stream_empty(test_project: ProjectBuilder) -> None:
    select_sql = "SELECT id, username FROM users ORDER BY created_at"

    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    async with mod.testdb_sql(select_sql).query_stream() as stream:
        streamed = [row async for row in stream]
    assert streamed == []


async def test_query_stream_with_params(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username, is_active) VALUES ($1, $2, $3)"
    select_sql = (
        "SELECT id, username FROM users WHERE is_active = $1 ORDER BY created_at"
    )

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    uid1, uid2, uid3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid1, "active1", True)
    await mod.testdb_sql(insert_sql).execute(uid2, "inactive", False)
    await mod.testdb_sql(insert_sql).execute(uid3, "active2", True)

    async with mod.testdb_sql(select_sql).query_stream(True) as stream:
        streamed = [row async for row in stream]
    all_rows = await mod.testdb_sql(select_sql).query_all_rows(True)

    assert len(streamed) == 2
    assert streamed == all_rows


async def test_query_stream_exception_cleanup(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    for i in range(3):
        await mod.testdb_sql(insert_sql).execute(uuid.uuid4(), f"user{i}")

    msg = "boom"
    with pytest.raises(ValueError, match=msg):  # noqa: PT012
        async with mod.testdb_sql(select_sql).query_stream() as stream:
            async for _row in stream:
                raise ValueError(msg)

    rows = await mod.testdb_sql(select_sql).query_all_rows()
    assert len(rows) == 3


async def test_query_stream_parallel_cursors(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)

    mod = test_project.generate()

    ids = [uuid.uuid4() for _ in range(4)]
    for i, uid in enumerate(ids):
        await mod.testdb_sql(insert_sql).execute(uid, f"user{i}")

    async with (
        mod.testdb_transaction(),
        mod.testdb_sql(select_sql).query_stream() as stream1,
        mod.testdb_sql(select_sql).query_stream() as stream2,
    ):
        rows1 = [row async for row in stream1]
        rows2 = [row async for row in stream2]

    assert rows1 == rows2
    assert len(rows1) == 4


async def test_query_stream_write_after(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"
    count_sql = "SELECT count(*) as cnt FROM users"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)
    test_project.add_query("cnt", count_sql)

    mod = test_project.generate()

    uid1 = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid1, "before_stream")

    async with mod.testdb_sql(select_sql).query_stream() as stream:
        _ = [row async for row in stream]

    uid2 = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid2, "after_stream")

    count = await mod.testdb_sql(count_sql).query_single_row()
    assert count == 2


async def test_query_stream_inside_rollback(test_project: ProjectBuilder) -> None:
    insert_sql = "INSERT INTO users (id, username) VALUES ($1, $2)"
    select_sql = "SELECT id, username FROM users ORDER BY created_at"
    count_sql = "SELECT count(*) as cnt FROM users"

    test_project.add_query("ins", insert_sql)
    test_project.add_query("sel", select_sql)
    test_project.add_query("cnt", count_sql)

    mod = test_project.generate()

    uid = uuid.uuid4()
    await mod.testdb_sql(insert_sql).execute(uid, "existing")

    try:
        async with mod.testdb_transaction():
            await mod.testdb_sql(insert_sql).execute(uuid.uuid4(), "will_rollback")

            async with mod.testdb_sql(select_sql).query_stream() as stream:
                streamed = [row async for row in stream]
            assert len(streamed) == 2

            raise RuntimeError  # noqa: TRY301
    except RuntimeError:
        pass

    count = await mod.testdb_sql(count_sql).query_single_row()
    assert count == 1
