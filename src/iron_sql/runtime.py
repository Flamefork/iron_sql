import asyncio
import contextlib
import itertools
import types
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Self
from typing import TypedDict
from typing import overload

import psycopg
import psycopg.abc
import psycopg.rows
import psycopg.sql
import psycopg.types.json
import psycopg_pool
from psycopg._cursor_base import BaseCursor
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
) -> AsyncGenerator[AsyncGenerator[str]]:
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


def _next_cursor_name() -> str:
    return f"_c{next(_cursor_seq)}"


@asynccontextmanager
async def _ensure_transaction(conn: psycopg.AsyncConnection) -> AsyncGenerator[None]:
    match conn.info.transaction_status:
        case psycopg.pq.TransactionStatus.IDLE:
            async with conn.transaction():
                yield
        case psycopg.pq.TransactionStatus.INTRANS:
            yield
        case status:
            msg = f"Cannot use server-side cursor: connection is in {status.name} state"
            raise psycopg.InterfaceError(msg)


class Query[T]:
    _stmt: ClassVar[psycopg.sql.SQL]
    _row_factory: psycopg.rows.BaseRowFactory[T]
    _connection_factory: Callable[
        [], contextlib.AbstractAsyncContextManager[psycopg.AsyncConnection]
    ]

    def with_connection(self, connection: psycopg.AsyncConnection) -> Self:
        q = self.__class__()
        q._connection_factory = lambda: contextlib.nullcontext(connection)  # noqa: SLF001
        return q

    @asynccontextmanager
    async def _client_cursor(
        self, params: psycopg.abc.Params | None
    ) -> AsyncGenerator[psycopg.AsyncRawCursor[T]]:
        async with (
            self._connection_factory() as conn,
            psycopg.AsyncRawCursor(conn, row_factory=self._row_factory) as cur,
        ):
            await cur.execute(self._stmt, params)
            yield cur

    @asynccontextmanager
    async def _server_cursor(
        self, params: psycopg.abc.Params | None
    ) -> AsyncGenerator[psycopg.AsyncRawServerCursor[T]]:
        async with (
            self._connection_factory() as conn,
            _ensure_transaction(conn),
            psycopg.AsyncRawServerCursor(
                conn, row_factory=self._row_factory, name=_next_cursor_name()
            ) as cur,
        ):
            await cur.execute(self._stmt, params)
            yield cur


class PoolOptions(TypedDict, total=False):
    min_size: int
    max_size: int | None
    timeout: float
    max_waiting: int
    max_lifetime: float
    max_idle: float
    reconnect_timeout: float
    num_workers: int
    kwargs: dict[str, Any]
    configure: Callable[[psycopg.AsyncConnection[Any]], Awaitable[None]]
    check: Callable[[psycopg.AsyncConnection[Any]], Awaitable[None]]
    reset: Callable[[psycopg.AsyncConnection[Any]], Awaitable[None]]
    reconnect_failed: Callable[[psycopg_pool.AsyncConnectionPool[Any]], Awaitable[None]]


class ConnectionPool:
    def __init__(
        self,
        conninfo: str,
        *,
        name: str | None = None,
        application_name: str | None = None,
        pool_options: PoolOptions | None = None,
    ) -> None:
        self.conninfo = conninfo
        self.name = name
        self.application_name = application_name
        self.pool_options = pool_options or {}
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
    async def connection(self) -> AsyncGenerator[psycopg.AsyncConnection]:
        task = asyncio.current_task()
        cancelling_before = 0 if task is None else task.cancelling()
        await self.psycopg_pool.open()
        async with self.psycopg_pool.connection() as conn:
            # Workaround for https://github.com/psycopg/psycopg/issues/1275
            if task is not None and task.cancelling() > cancelling_before:
                raise asyncio.CancelledError
            yield conn

    def _init_psycopg_pool(self) -> None:
        user_kwargs: dict[str, Any] = self.pool_options.get("kwargs", {})
        forwarded: dict[str, Any] = {
            k: v for k, v in self.pool_options.items() if k != "kwargs"
        }
        conn_kwargs = {
            **user_kwargs,
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
            "autocommit": True,
        }
        if self.application_name is not None:
            conn_kwargs["application_name"] = self.application_name
        self.psycopg_pool = psycopg_pool.AsyncConnectionPool(
            self.conninfo,
            **forwarded,
            open=False,
            name=self.name,
            kwargs=conn_kwargs,
        )

    @asynccontextmanager
    async def connection_in_context(
        self, context_var: ContextVar[psycopg.AsyncConnection | None]
    ) -> AsyncGenerator[psycopg.AsyncConnection]:
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
    def typed_scalar_row_(
        cursor: BaseCursor[Any, Any],
    ) -> psycopg.rows.RowMaker[T | None]:
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
