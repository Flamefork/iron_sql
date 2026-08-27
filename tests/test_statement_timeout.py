import asyncio
import uuid
from typing import Any

import psycopg
import pytest

from tests.conftest import ProjectBuilder

_POOL_OPTIONS: dict[str, Any] = {"kwargs": {"options": "-c statement_timeout=200"}}

_INSERT_SQL = "INSERT INTO users (id, username) VALUES ($1, $2)"
_STREAM_SQL = "SELECT id FROM users ORDER BY username"
_SLEEP_SQL = "SELECT 1 AS n FROM pg_sleep(1)"


async def test_statement_timeout_cancels_a_slow_statement(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("sleep", _SLEEP_SQL)
    mod = test_project.generate(pool_options=_POOL_OPTIONS)

    with pytest.raises(psycopg.errors.QueryCanceled):
        await mod.testdb_sql(_SLEEP_SQL).query_single_row()


async def test_statement_timeout_does_not_cancel_a_slow_stream(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("ins", _INSERT_SQL)
    test_project.add_query("stream", _STREAM_SQL)
    mod = test_project.generate(pool_options=_POOL_OPTIONS)

    for i in range(5):
        await mod.testdb_sql(_INSERT_SQL).execute(uuid.uuid4(), f"user_{i}")

    streamed: list[uuid.UUID] = []
    async with mod.testdb_sql(_STREAM_SQL).query_stream() as rows:
        async for row in rows:
            await asyncio.sleep(0.1)
            streamed.append(row)

    assert len(streamed) == 5
