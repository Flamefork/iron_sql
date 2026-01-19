from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Any
from typing import Literal
from typing import Self
from typing import overload

import psycopg
import psycopg.rows
import psycopg_pool


class NoRowsError(Exception):
    pass


class TooManyRowsError(Exception):
    pass


class ConnectionPool:
    def __init__(
        self,
        conninfo: str,
        *,
        name: str | None = None,
        application_name: str | None = None,
    ) -> None:
        self.conninfo = conninfo
        self.name = name
        self.application_name = application_name
        self._init_psycopg_pool()

    async def close(self) -> None:
        await self.psycopg_pool.close()
        self._init_psycopg_pool()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def await_connections(self) -> None:
        await self.psycopg_pool.open(wait=True)

    async def check(self) -> None:
        await self.psycopg_pool.open()
        await self.psycopg_pool.check()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection]:
        await self.psycopg_pool.open()
        async with self.psycopg_pool.connection() as conn:
            yield conn

    def _init_psycopg_pool(self) -> None:
        self.psycopg_pool = psycopg_pool.AsyncConnectionPool(
            self.conninfo,
            open=False,
            name=self.name,
            kwargs={
                "application_name": self.application_name,
                # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
                "autocommit": True,
            },
        )

    @asynccontextmanager
    async def connection_in_context(
        self, context_var: ContextVar[psycopg.AsyncConnection | None]
    ) -> AsyncIterator[psycopg.AsyncConnection]:
        conn = context_var.get()
        if conn is not None:
            yield conn
            return
        async with self.connection() as conn:
            token = context_var.set(conn)
            try:
                yield conn
            finally:
                context_var.reset(token)


def get_one_row[T](rows: list[T]) -> T:
    if len(rows) == 0:
        raise NoRowsError
    if len(rows) > 1:
        raise TooManyRowsError
    return rows[0]


def get_one_row_or_none[T](rows: list[T]) -> T | None:
    if len(rows) == 0:
        return None
    if len(rows) > 1:
        raise TooManyRowsError
    return rows[0]


@overload
def typed_scalar_row[T](
    typ: type[T], *, not_null: Literal[True]
) -> psycopg.rows.BaseRowFactory[T]: ...


@overload
def typed_scalar_row[T](
    typ: type[T], *, not_null: Literal[False]
) -> psycopg.rows.BaseRowFactory[T | None]: ...


def typed_scalar_row[T](
    typ: type[T], *, not_null: bool
) -> psycopg.rows.BaseRowFactory[T | None]:
    def typed_scalar_row_(cursor) -> psycopg.rows.RowMaker[T | None]:
        scalar_row_ = psycopg.rows.scalar_row(cursor)

        def typed_scalar_row__(values: Sequence[Any]) -> T | None:
            val = scalar_row_(values)
            if not not_null and val is None:
                return None
            if not isinstance(val, typ):
                if issubclass(typ, Enum):
                    return typ(val)
                msg = f"Expected scalar of type {typ}, got {type(val)}"
                raise TypeError(msg)
            return val

        return typed_scalar_row__

    return typed_scalar_row_
