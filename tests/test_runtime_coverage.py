from collections.abc import AsyncIterator

import pytest

from iron_sql.runtime import ConnectionPool
from iron_sql.runtime import TooManyRowsError
from iron_sql.runtime import get_one_row_or_none
from iron_sql.runtime import typed_scalar_row


@pytest.fixture
async def async_pool(pg_dsn: str) -> AsyncIterator[ConnectionPool]:
    p = ConnectionPool(pg_dsn, name="test_pool")
    yield p
    await p.close()


async def test_pool_check_and_await(async_pool: ConnectionPool) -> None:
    await async_pool.check()
    await async_pool.await_connections()


async def test_pool_context_manager(pg_dsn: str) -> None:
    async with ConnectionPool(pg_dsn) as p:
        await p.check()


def test_get_one_row_or_none_too_many() -> None:
    with pytest.raises(TooManyRowsError):
        get_one_row_or_none([1, 2])


async def test_typed_scalar_row_type_mismatch(async_pool: ConnectionPool) -> None:
    async with (
        async_pool.connection() as conn,
        conn.cursor(row_factory=typed_scalar_row(int, not_null=True)) as cur,
    ):
        await cur.execute("SELECT 'not an int'::text")
        with pytest.raises(TypeError, match="Expected scalar of type <class 'int'>"):
            await cur.fetchone()
