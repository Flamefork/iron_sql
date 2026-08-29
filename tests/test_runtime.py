from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING
from typing import cast
from unittest.mock import AsyncMock

import pytest

from iron_sql.runtime import ConnectionPool
from iron_sql.runtime import TooManyRowsError
from iron_sql.runtime import get_one_row_or_none
from iron_sql.runtime import json_validated
from iron_sql.runtime import register_enums
from iron_sql.runtime import typed_array_row
from iron_sql.runtime import typed_json_scalar_row
from iron_sql.runtime import typed_scalar_row
from iron_sql.runtime import typed_value_row
from tests.json_models import UserMetadata

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    import psycopg
    from psycopg.rows import BaseRowFactory

    from tests.conftest import ProjectBuilder

# =============================================================================
# json_validated decorator
# =============================================================================


def test_json_validated_applies_validation() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata
        other: int

    raw_row = cast("Callable[..., Row]", Row)
    row = raw_row(metadata='{"key": "lang", "value": "en"}', other=42)
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

    raw_row = cast("Callable[..., Row]", Row)
    row = raw_row(metadata={"key": "k", "value": "v"}, extra="ignored")
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
    async def fake_connection():
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


async def test_typed_json_scalar_row_validates_model(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(
            row_factory=typed_json_scalar_row(UserMetadata, not_null=True)
        ) as cur,
    ):
        await cur.execute('SELECT \'{"key": "k", "value": "v"}\'::jsonb')
        value = await cur.fetchone()

    assert value == UserMetadata(key="k", value="v")


async def test_pool_forwards_pool_options(pg_dsn: str) -> None:
    async with ConnectionPool(pg_dsn, pool_options={"min_size": 1, "max_size": 2}) as p:
        assert p.psycopg_pool.min_size == 1
        assert p.psycopg_pool.max_size == 2
        await p.check()


async def test_pool_preserves_application_name_from_pool_options_kwargs() -> None:
    pool = ConnectionPool(
        "postgresql://example.invalid/db",
        pool_options={"kwargs": {"application_name": "from-pool-options"}},
    )
    kwargs = pool.psycopg_pool.kwargs
    assert isinstance(kwargs, dict)
    assert kwargs["application_name"] == "from-pool-options"
    await pool.psycopg_pool.close()


async def test_pool_explicit_application_name_overrides_pool_options() -> None:
    pool = ConnectionPool(
        "postgresql://example.invalid/db",
        application_name="explicit",
        pool_options={"kwargs": {"application_name": "from-pool-options"}},
    )
    kwargs = pool.psycopg_pool.kwargs
    assert isinstance(kwargs, dict)
    assert kwargs["application_name"] == "explicit"
    await pool.psycopg_pool.close()


async def test_typed_scalar_row_type_mismatch(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_scalar_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT 'not an int'::text")
        with pytest.raises(TypeError, match="Expected scalar of type <class 'int'>"):
            await cur.fetchone()


async def test_typed_scalar_row_int_array(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_array_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT ARRAY[1, 2, 3]::int[]")
        row = await cur.fetchone()
        assert row == [1, 2, 3]


async def test_typed_value_row_not_null_raises_on_none(pool: ConnectionPool) -> None:
    row_factory: BaseRowFactory[object] = typed_value_row(not_null=True)
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=row_factory) as cur,
    ):
        await cur.execute("SELECT NULL::int")
        with pytest.raises(TypeError, match="Expected non-null value"):
            await cur.fetchone()


async def test_typed_array_row_not_null_raises_on_none(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_array_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT NULL::int[]")
        with pytest.raises(TypeError, match="Expected non-null value"):
            await cur.fetchone()


async def test_typed_scalar_row_array_type_mismatch(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        conn.cursor(row_factory=typed_array_row(str, not_null=True)) as cur,
    ):
        await cur.execute("SELECT 1::int")
        with pytest.raises(TypeError, match="Expected scalar of type"):
            await cur.fetchone()


async def test_register_enums_fails_fast_on_missing_type(
    pool: ConnectionPool,
) -> None:
    class Missing(StrEnum):
        A = "a"

    async with pool.connection() as conn:
        with pytest.raises(RuntimeError, match="not found in database"):
            await register_enums(conn, [("nonexistent_enum", Missing)])


async def test_pool_enum_types_compose_with_user_configure(
    test_project: ProjectBuilder,
) -> None:
    class Status(StrEnum):
        ACTIVE = "active"
        INACTIVE = "inactive"

    user_configure_calls = 0

    async def user_configure(conn: psycopg.AsyncConnection[Any]) -> None:
        nonlocal user_configure_calls
        user_configure_calls += 1
        await conn.execute("SELECT 1")

    pool = ConnectionPool(
        test_project.dsn,
        enum_types=[("user_status", Status)],
        pool_options={"configure": user_configure, "min_size": 1, "max_size": 1},
    )
    try:
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT 'active'::user_status")
            row = await cur.fetchone()
            assert row is not None
            assert isinstance(row[0], Status)
        assert user_configure_calls == 1
    finally:
        await pool.close()


async def test_register_enums_on_externally_supplied_connection(
    test_project: ProjectBuilder,
) -> None:
    class Status(StrEnum):
        ACTIVE = "active"
        INACTIVE = "inactive"

    pool = ConnectionPool(test_project.dsn)
    try:
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT 'active'::user_status")
            before = await cur.fetchone()
            assert before is not None
            assert not isinstance(before[0], Status)

            await register_enums(conn, [("user_status", Status)])

            cur = await conn.execute("SELECT 'active'::user_status")
            after = await cur.fetchone()
            assert after is not None
            assert isinstance(after[0], Status)
    finally:
        await pool.close()
