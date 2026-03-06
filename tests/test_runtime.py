import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from iron_sql.runtime import ConnectionPool
from iron_sql.runtime import TooManyRowsError
from iron_sql.runtime import get_one_row_or_none
from iron_sql.runtime import json_validated
from iron_sql.runtime import typed_scalar_row
from tests.json_models import UserMetadata

# =============================================================================
# json_validated decorator
# =============================================================================


def test_json_validated_applies_validation() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata
        other: int

    row = Row(metadata='{"key": "lang", "value": "en"}', other=42)  # type: ignore[reportArgumentType]
    assert isinstance(row.metadata, UserMetadata)
    assert row.metadata.key == "lang"
    assert row.other == 42


def test_json_validated_chains_existing_post_init() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata
        extra: str = ""

        def __post_init__(self) -> None:
            self.extra = "post_init_ran"

    row = Row(metadata={"key": "k", "value": "v"}, extra="ignored")  # type: ignore[reportArgumentType]
    assert isinstance(row.metadata, UserMetadata)
    assert row.extra == "post_init_ran"


def test_json_validated_skips_none() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata | None

    row = Row(metadata=None)
    assert row.metadata is None


# =============================================================================
# ConnectionPool utilities
# =============================================================================


async def test_pool_check_and_await(pool: ConnectionPool) -> None:
    await pool.check()
    await pool.await_connections()


async def test_pool_context_manager(pg_dsn: str) -> None:
    async with ConnectionPool(pg_dsn) as p:
        await p.check()


# Workaround for https://github.com/psycopg/psycopg/issues/1275
async def test_pool_connection_reraises_cancelled_error_swallowed_by_pool(
    pool: ConnectionPool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = False
    exited = False

    @asynccontextmanager
    async def fake_connection():  # noqa: RUF029
        nonlocal entered, exited
        entered = True
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        try:
            yield object()
        finally:
            exited = True

    monkeypatch.setattr(pool.psycopg_pool, "open", AsyncMock())
    monkeypatch.setattr(pool.psycopg_pool, "connection", fake_connection)

    async def probe() -> None:
        async with pool.connection() as conn:
            _ = conn

    with pytest.raises(asyncio.CancelledError):
        await asyncio.create_task(probe())

    assert entered
    assert exited


def test_get_one_row_or_none_too_many() -> None:
    with pytest.raises(TooManyRowsError):
        get_one_row_or_none([1, 2])


async def test_typed_scalar_row_not_null_raises_on_none(
    pool: ConnectionPool,
) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_scalar_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT NULL::int")
        with pytest.raises(TypeError, match="Expected non-null value"):
            await cur.fetchone()


async def test_typed_scalar_row_type_mismatch(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_scalar_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT 'not an int'::text")
        with pytest.raises(TypeError, match="Expected scalar of type <class 'int'>"):
            await cur.fetchone()
