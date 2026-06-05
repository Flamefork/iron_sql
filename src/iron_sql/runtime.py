import asyncio
import contextlib
import functools
import itertools
import types
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from enum import StrEnum
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Self
from typing import TypedDict
from typing import TypeGuard
from typing import overload

import psycopg
import psycopg.abc
import psycopg.rows
import psycopg.sql
import psycopg.types.enum
import psycopg_pool
from psycopg._cursor_base import BaseCursor
from pydantic import TypeAdapter


@functools.cache
def get_adapter(typ: object) -> TypeAdapter[Any]:
    return TypeAdapter(typ)


class NoRowsError(Exception):
    pass


class TooManyRowsError(Exception):
    pass


@asynccontextmanager
async def listen(
    conn: psycopg.AsyncConnection[Any], channel: str
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


async def notify(
    conn: psycopg.AsyncConnection[Any], channel: str, payload: str
) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("NOTIFY {}, {}").format(
            psycopg.sql.Identifier(channel),
            psycopg.sql.Literal(payload),
        )
    )


async def execute_listen(conn: psycopg.AsyncConnection[Any], channel: str) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel))
    )


async def execute_unlisten(conn: psycopg.AsyncConnection[Any], channel: str) -> None:
    _validate_channel(channel)
    await conn.execute(
        psycopg.sql.SQL("UNLISTEN {}").format(psycopg.sql.Identifier(channel))
    )


async def _has_active_listen_subscriptions(conn: psycopg.AsyncConnection[Any]) -> bool:
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
async def _ensure_transaction(
    conn: psycopg.AsyncConnection[Any],
) -> AsyncGenerator[None]:
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
        [], contextlib.AbstractAsyncContextManager[psycopg.AsyncConnection[Any]]
    ]

    def with_connection(self, connection: psycopg.AsyncConnection[Any]) -> Self:
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


async def register_enums(
    conn: psycopg.AsyncConnection[Any],
    enum_types: Sequence[tuple[str, type[StrEnum]]],
) -> None:
    for pg_name, enum_cls in enum_types:
        info = await psycopg.types.enum.EnumInfo.fetch(conn, pg_name)
        if info is None:
            msg = f"Enum type {pg_name!r} not found in database"
            raise RuntimeError(msg)
        psycopg.types.enum.register_enum(
            info,
            conn,
            enum_cls,
            mapping=[(member, member.value) for member in enum_cls],
        )


def _enum_configure(
    enum_types: Sequence[tuple[str, type[StrEnum]]],
    user_configure: Callable[[psycopg.AsyncConnection[Any]], Awaitable[None]] | None,
) -> Callable[[psycopg.AsyncConnection[Any]], Awaitable[None]]:
    async def configure(conn: psycopg.AsyncConnection[Any]) -> None:
        await register_enums(conn, enum_types)
        if user_configure is not None:
            await user_configure(conn)

    return configure


class ConnectionPool:
    def __init__(
        self,
        conninfo: str,
        *,
        name: str | None = None,
        application_name: str | None = None,
        pool_options: PoolOptions | None = None,
        enum_types: Sequence[tuple[str, type[StrEnum]]] = (),
    ) -> None:
        self.conninfo = conninfo
        self.name = name
        self.application_name = application_name
        self.pool_options = pool_options or {}
        self.enum_types = enum_types
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
    async def connection(self) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
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
        if self.enum_types:
            forwarded["configure"] = _enum_configure(
                self.enum_types, forwarded.get("configure")
            )
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
        self, context_var: ContextVar[psycopg.AsyncConnection[Any] | None]
    ) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
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


def validate_json_field[T](typ: type[T], value: object) -> T:
    adapter = get_adapter(typ)
    if isinstance(value, str | bytes):
        return adapter.validate_json(value)
    return adapter.validate_python(value)


def json_validated[T](**json_fields: object) -> Callable[[type[T]], type[T]]:
    def decorator(cls: type[T]) -> type[T]:
        original_post_init = getattr(cls, "__post_init__", None)

        def __post_init__(self: object) -> None:  # noqa: N807
            if original_post_init is not None:
                original_post_init(self)
            for name, typ in json_fields.items():
                current = getattr(self, name)
                if current is None:
                    continue
                setattr(self, name, validate_json_field(typ, current))  # pyright: ignore[reportArgumentType]

        setattr(cls, "__post_init__", __post_init__)  # noqa: B010
        return cls

    return decorator


def dump_json_value(typ: object, value: object) -> object:
    adapter = get_adapter(typ)
    return adapter.dump_python(value, mode="json")


def dump_json_text(typ: object, value: object) -> str:
    adapter = get_adapter(typ)
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
            return _check_scalar_type(val, typ)

        return typed_scalar_row__

    return typed_scalar_row_


@overload
def typed_value_row[T](
    *,
    not_null: Literal[True],
) -> psycopg.rows.BaseRowFactory[T]: ...


@overload
def typed_value_row[T](
    *,
    not_null: Literal[False],
) -> psycopg.rows.BaseRowFactory[T | None]: ...


def typed_value_row[T](*, not_null: bool) -> psycopg.rows.BaseRowFactory[T | None]:
    def typed_value_row_(
        cursor: BaseCursor[Any, Any],
    ) -> psycopg.rows.RowMaker[T | None]:
        scalar_row_ = psycopg.rows.scalar_row(cursor)

        def typed_value_row__(values: Sequence[Any]) -> T | None:
            val = scalar_row_(values)
            if val is None:
                if not_null:
                    msg = "Expected non-null value, got None"
                    raise TypeError(msg)
                return None
            return val

        return typed_value_row__

    return typed_value_row_


@overload
def typed_array_row[T](
    elem_typ: type[T],
    *,
    not_null: Literal[True],
) -> psycopg.rows.BaseRowFactory[list[T]]: ...


@overload
def typed_array_row[T](
    elem_typ: type[T],
    *,
    not_null: Literal[False],
) -> psycopg.rows.BaseRowFactory[list[T] | None]: ...


def typed_array_row[T](
    elem_typ: type[T], *, not_null: bool
) -> psycopg.rows.BaseRowFactory[list[T] | None]:
    def typed_array_row_(
        cursor: BaseCursor[Any, Any],
    ) -> psycopg.rows.RowMaker[list[T] | None]:
        scalar_row_ = psycopg.rows.scalar_row(cursor)

        def typed_array_row__(values: Sequence[Any]) -> list[T] | None:
            val = scalar_row_(values)
            if val is None:
                if not_null:
                    msg = "Expected non-null value, got None"
                    raise TypeError(msg)
                return None
            if not _is_object_list(val):
                msg = f"Expected scalar of type list[{elem_typ}], got {type(val)}"
                raise TypeError(msg)
            return [_check_scalar_type(v, elem_typ) for v in val]

        return typed_array_row__

    return typed_array_row_


def _check_scalar_type[T](val: object, typ: type[T]) -> T:
    if _is_instance(val, typ):
        return val
    msg = f"Expected scalar of type {typ}, got {type(val)}"
    raise TypeError(msg)


def _is_instance[T](val: object, typ: type[T]) -> TypeGuard[T]:
    return isinstance(val, typ)


def _is_object_list(val: object) -> TypeGuard[list[object]]:
    return isinstance(val, list)
