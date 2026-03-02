import contextlib
import itertools
import types
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Callable
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
import psycopg.sql
import psycopg.types.json
import psycopg_pool
from pydantic import TypeAdapter

_adapter_cache: dict[object, TypeAdapter[object]] = {}


def get_adapter(typ: object) -> TypeAdapter[object]:
    if typ not in _adapter_cache:
        _adapter_cache[typ] = TypeAdapter(typ)
    return _adapter_cache[typ]


class NoRowsError(Exception):
    pass


class TooManyRowsError(Exception):
    pass


@asynccontextmanager
async def listen(
    conn: psycopg.AsyncConnection, channel: str
) -> AsyncIterator[AsyncGenerator[str]]:
    _validate_channel(channel)
    if await _has_active_listen_subscriptions(conn):
        msg = "listen() requires a connection without active LISTEN subscriptions"
        raise RuntimeError(msg)
    await execute_listen(conn, channel)

    async def _payloads() -> AsyncGenerator[str]:
        async for notify_msg in conn.notifies():
            yield notify_msg.payload

    gen = _payloads()
    try:
        yield gen
    finally:
        with contextlib.suppress(psycopg.OperationalError, psycopg.InterfaceError):
            await gen.aclose()
        with contextlib.suppress(psycopg.OperationalError, psycopg.InterfaceError):
            await execute_unlisten(conn, channel)


async def notify(conn: psycopg.AsyncConnection, channel: str, payload: str) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("NOTIFY {}, {}").format(
            psycopg.sql.Identifier(channel),
            psycopg.sql.Literal(payload),
        )
    )


async def execute_listen(conn: psycopg.AsyncConnection, channel: str) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel))
    )


async def execute_unlisten(conn: psycopg.AsyncConnection, channel: str) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("UNLISTEN {}").format(psycopg.sql.Identifier(channel))
    )


async def _has_active_listen_subscriptions(conn: psycopg.AsyncConnection) -> bool:
    async with conn.cursor() as cur:
        await cur.execute("SELECT EXISTS (SELECT FROM pg_listening_channels())")
        row = await cur.fetchone()
    if row is None:
        msg = "Expected a single boolean row from active LISTEN check"
        raise RuntimeError(msg)
    return bool(row[0])


def _validate_channel(name: str) -> None:
    if not name:
        msg = "Channel name must not be empty"
        raise ValueError(msg)


_cursor_seq = itertools.count()


def next_cursor_name() -> str:
    return f"_c{next(_cursor_seq)}"


@asynccontextmanager
async def ensure_transaction(conn: psycopg.AsyncConnection) -> AsyncIterator[None]:
    match conn.info.transaction_status:
        case psycopg.pq.TransactionStatus.IDLE:
            async with conn.transaction():
                yield
        case psycopg.pq.TransactionStatus.INTRANS:
            yield
        case status:
            msg = f"Cannot use server-side cursor: connection is in {status.name} state"
            raise psycopg.InterfaceError(msg)


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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
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


def validate_json_field(typ: object, value: object) -> object:
    if value is None:
        return None
    adapter = get_adapter(typ)
    if isinstance(value, str | bytes):
        return adapter.validate_json(value)
    return adapter.validate_python(value)


def json_validated(**json_fields: object):
    def decorator[T](cls: type[T]) -> type[T]:
        original = getattr(cls, "__post_init__", None)

        def __post_init__(self: object) -> None:  # noqa: N807
            if original is not None:
                original(self)
            for name, typ in json_fields.items():
                setattr(self, name, validate_json_field(typ, getattr(self, name)))

        cls.__post_init__ = __post_init__  # type: ignore[attr-defined]
        return cls

    return decorator


def serialize_json_param(typ: object, value: object, db_type: str) -> object:
    if value is None:
        return None
    adapter = get_adapter(typ)
    match db_type:
        case "json":
            return psycopg.types.json.Json(adapter.dump_python(value, mode="json"))
        case "jsonb":
            return psycopg.types.json.Jsonb(adapter.dump_python(value, mode="json"))
        case _:
            return adapter.dump_json(value).decode()


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
    typ: type[T],
    *,
    not_null: Literal[True],
    validate: Callable[[object], T] | None = None,
) -> psycopg.rows.BaseRowFactory[T]: ...


@overload
def typed_scalar_row[T](
    typ: type[T],
    *,
    not_null: Literal[False],
    validate: Callable[[object], T] | None = None,
) -> psycopg.rows.BaseRowFactory[T | None]: ...


def typed_scalar_row[T](
    typ: type[T], *, not_null: bool, validate: Callable[[object], T] | None = None
) -> psycopg.rows.BaseRowFactory[T | None]:
    def typed_scalar_row_(cursor) -> psycopg.rows.RowMaker[T | None]:
        scalar_row_ = psycopg.rows.scalar_row(cursor)

        def typed_scalar_row__(values: Sequence[Any]) -> T | None:
            val = scalar_row_(values)
            if val is None:
                if not_null:
                    msg = "Expected non-null value, got None"
                    raise TypeError(msg)
                return None
            if validate:
                return validate(val)
            if not isinstance(val, typ):
                if issubclass(typ, Enum):
                    return typ(val)
                msg = f"Expected scalar of type {typ}, got {type(val)}"
                raise TypeError(msg)
            return val

        return typed_scalar_row__

    return typed_scalar_row_
