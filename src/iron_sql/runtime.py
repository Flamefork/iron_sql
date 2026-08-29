import asyncio
import contextlib
import functools
import inspect
import itertools
import logging
import time
import types
import weakref
from collections import defaultdict
from collections import deque
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Sequence
from contextlib import asynccontextmanager
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Self
from typing import TypedDict
from typing import TypeGuard
from typing import cast
from typing import overload

import psycopg
import psycopg.abc
import psycopg.rows
import psycopg.sql
import psycopg.types.enum
import psycopg_pool
from psycopg._cursor_base import BaseCursor
from pydantic import TypeAdapter

logger = logging.getLogger(__name__)

type ConnectionFactory = Callable[
    [], contextlib.AbstractAsyncContextManager[psycopg.AsyncConnection[Any]]
]
type AsyncPoolFactory = Callable[
    ..., psycopg_pool.AsyncConnectionPool[psycopg.AsyncConnection[Any]]
]


@functools.cache
def get_adapter(typ: object) -> TypeAdapter[object]:
    return cast("TypeAdapter[object]", TypeAdapter(typ))


class NoRowsError(Exception):
    pass


class TooManyRowsError(Exception):
    pass


class RepeatedQueryError(Exception):
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
        row = cast("tuple[object]", await cur.fetchone())
    value = row[0]
    return bool(value)


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


@dataclass
class _TaskExecutions:
    timestamps: defaultdict[type["Query[Any]"], deque[float]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    reported: set[type["Query[Any]"]] = field(default_factory=set)


@dataclass
class _ActiveDetection:
    executions: int
    within_seconds: float
    strict: bool
    by_task: weakref.WeakKeyDictionary[asyncio.Task[Any], _TaskExecutions] = field(
        default_factory=weakref.WeakKeyDictionary
    )


@dataclass
class _DetectionSlot:
    active: _ActiveDetection | None = None


_detection_slot = _DetectionSlot()

_MIN_EXECUTIONS = 2

_MAX_STATEMENT_CHARS = 120


@contextmanager
def detect_sql_repeats(
    *, executions: int = 10, within_seconds: float = 1.0, strict: bool = False
) -> Generator[None]:
    if executions < _MIN_EXECUTIONS:
        msg = f"executions must be at least {_MIN_EXECUTIONS}, got: {executions}"
        raise ValueError(msg)
    if within_seconds <= 0:
        msg = f"within_seconds must be positive, got: {within_seconds}"
        raise ValueError(msg)
    if _detection_slot.active is not None:
        msg = "detect_sql_repeats is already active"
        raise RuntimeError(msg)

    _detection_slot.active = _ActiveDetection(
        executions=executions, within_seconds=within_seconds, strict=strict
    )
    try:
        yield
    finally:
        _detection_slot.active = None


def _statement_summary(stmt: psycopg.sql.SQL) -> str:
    text = " ".join(stmt.as_string(None).split())
    if len(text) <= _MAX_STATEMENT_CHARS:
        return text
    return text[:_MAX_STATEMENT_CHARS] + "..."


def _record_execution(
    query_cls: "type[Query[Any]]",
    stmt: psycopg.sql.SQL,
    locations: tuple[str, ...],
) -> None:
    active = _detection_slot.active
    if active is None:
        return
    task = asyncio.current_task()
    missing_task_msg = "Query repeat detection requires a running asyncio task"
    if task is None:
        raise AssertionError(missing_task_msg)

    task_executions = active.by_task.setdefault(task, _TaskExecutions())
    if query_cls in task_executions.reported:
        return

    now = time.monotonic()
    timestamps = task_executions.timestamps[query_cls]
    timestamps.append(now)
    cutoff = now - active.within_seconds
    while timestamps[0] < cutoff:
        timestamps.popleft()
    if len(timestamps) < active.executions:
        return

    task_executions.reported.add(query_cls)
    message = (
        f"Repeated query at {', '.join(locations)}: {_statement_summary(stmt)} "
        f"executed {len(timestamps)} times within {active.within_seconds}s "
        f"in a single asyncio task"
    )
    timestamps.clear()
    if active.strict:
        raise RepeatedQueryError(message)
    logger.warning(message)


class Query[T]:
    _locations: ClassVar[tuple[str, ...]]
    _stmt: ClassVar[psycopg.sql.SQL]
    _row_factory: psycopg.rows.BaseRowFactory[T]
    _connection_factory: Callable[
        [], contextlib.AbstractAsyncContextManager[psycopg.AsyncConnection[Any]]
    ]

    def __init__(self) -> None:
        self._row_factory = cast(
            "psycopg.rows.BaseRowFactory[T]",
            inspect.getattr_static(type(self), "_row_factory"),
        )
        self._connection_factory = cast(
            "ConnectionFactory",
            inspect.getattr_static(type(self), "_connection_factory"),
        )

    def with_connection(self, connection: psycopg.AsyncConnection[Any]) -> Self:
        q = self.__class__()
        q._connection_factory = lambda: contextlib.nullcontext(connection)  # noqa: SLF001
        return q

    @asynccontextmanager
    async def _client_cursor(
        self, params: psycopg.abc.Params | None
    ) -> AsyncGenerator[psycopg.AsyncRawCursor[T]]:
        _record_execution(type(self), self._stmt, self._locations)
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
        _record_execution(type(self), self._stmt, self._locations)
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
        pool_factory = cast(
            "AsyncPoolFactory",
            psycopg_pool.AsyncConnectionPool,
        )
        pool = pool_factory(
            self.conninfo,
            **forwarded,
            open=False,
            name=self.name,
            kwargs=conn_kwargs,
        )
        self.psycopg_pool = pool

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
        validated = adapter.validate_json(value)
    else:
        validated = adapter.validate_python(value)
    return cast("T", validated)


def json_validated[T](**json_fields: object) -> Callable[[type[T]], type[T]]:
    def decorator(cls: type[T]) -> type[T]:
        original_post_init = cast(
            "Callable[[object], None] | None", getattr(cls, "__post_init__", None)
        )

        def __post_init__(self: object) -> None:  # noqa: N807
            if original_post_init is not None:
                original_post_init(self)
            for name, typ in json_fields.items():
                current = cast("object", getattr(self, name))
                if current is None:
                    continue
                field_type = cast("type[object]", typ)
                setattr(self, name, validate_json_field(field_type, current))

        setattr(cls, "__post_init__", __post_init__)  # noqa: B010
        return cls

    return decorator


def dump_json_value(typ: object, value: object) -> object:
    adapter = get_adapter(typ)
    return cast("object", adapter.dump_python(value, mode="json"))


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
        scalar_row_ = cast(
            "psycopg.rows.RowMaker[object]", psycopg.rows.scalar_row(cursor)
        )

        def typed_scalar_row__(values: Sequence[object]) -> T | None:
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
def typed_json_scalar_row[T](
    typ: type[T],
    *,
    not_null: Literal[True],
) -> psycopg.rows.BaseRowFactory[T]: ...


@overload
def typed_json_scalar_row[T](
    typ: type[T],
    *,
    not_null: Literal[False],
) -> psycopg.rows.BaseRowFactory[T | None]: ...


def typed_json_scalar_row[T](
    typ: type[T], *, not_null: bool
) -> psycopg.rows.BaseRowFactory[T | None]:
    def validate(value: object) -> T:
        return validate_json_field(typ, value)

    return typed_scalar_row(typ, not_null=not_null, validate=validate)


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
        scalar_row_ = cast(
            "psycopg.rows.RowMaker[object]", psycopg.rows.scalar_row(cursor)
        )

        def typed_value_row__(values: Sequence[object]) -> T | None:
            val = scalar_row_(values)
            if val is None:
                if not_null:
                    msg = "Expected non-null value, got None"
                    raise TypeError(msg)
                return None
            return cast("T", val)

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
        scalar_row_ = cast(
            "psycopg.rows.RowMaker[object]", psycopg.rows.scalar_row(cursor)
        )

        def typed_array_row__(values: Sequence[object]) -> list[T] | None:
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
