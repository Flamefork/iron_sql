import ast
import dataclasses
import hashlib
import importlib
import logging
import warnings
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import inflection
from pydantic import alias_generators

from iron_sql.codegen.sqlc import Catalog
from iron_sql.codegen.sqlc import Column
from iron_sql.codegen.sqlc import Enum
from iron_sql.codegen.sqlc import Query
from iron_sql.codegen.sqlc import SQLCResult
from iron_sql.codegen.sqlc import run_sqlc
from iron_sql.codegen.util import indent_block
from iron_sql.codegen.util import write_if_changed

logger = logging.getLogger(__name__)


@dataclass(kw_only=True, frozen=True)
class ColumnSpec:
    name: str
    table: str
    py_type: str
    json_type: str | None = None


@dataclass(kw_only=True, frozen=True)
class ParamSpec:
    name: str
    py_type: str
    is_named: bool
    db_type: str
    not_null: bool
    is_array: bool
    json_type: str | None = None

    def __post_init__(self) -> None:
        if self.db_type == "jsonb" and self.is_array:
            msg = "Unsupported column type: jsonb[]"
            raise TypeError(msg)

    @property
    def serialized_expr(self) -> str:
        if self.json_type:
            return f"runtime.serialize_json_param({self.json_type}, {self.name}, {self.db_type!r})"  # noqa: E501

        match self.db_type:
            case "json":
                expr = f"psycopg.types.json.Json({self.name})"
            case "jsonb":
                expr = f"psycopg.types.json.Jsonb({self.name})"
            case _:
                return self.name

        if not self.not_null:
            return f"{expr} if {self.name} is not None else None"
        return expr


class UnknownSQLTypeWarning(UserWarning):
    pass


_SQL_TYPE_MAP: dict[str, str] = {
    "bool": "bool",
    "boolean": "bool",
    "int2": "int",
    "int4": "int",
    "int8": "int",
    "smallint": "int",
    "integer": "int",
    "bigint": "int",
    "serial": "int",
    "bigserial": "int",
    "oid": "int",
    "float4": "float",
    "float8": "float",
    "numeric": "decimal.Decimal",
    "varchar": "str",
    "text": "str",
    "bpchar": "str",
    "char": "str",
    "name": "str",
    "bytea": "bytes",
    "json": "object",
    "jsonb": "object",
    "inet": "ipaddress.IPv4Address | ipaddress.IPv6Address | ipaddress.IPv4Interface | ipaddress.IPv6Interface",  # noqa: E501
    "cidr": "ipaddress.IPv4Network | ipaddress.IPv6Network",
    "date": "datetime.date",
    "time": "datetime.time",
    "timetz": "datetime.time",
    "timestamp": "datetime.datetime",
    "timestamptz": "datetime.datetime",
    "interval": "datetime.timedelta",
    "uuid": "uuid.UUID",
    "any": "object",
    "anyelement": "object",
}


@dataclass(kw_only=True, frozen=True)
class TypeResolver:
    catalog: Catalog
    package_name: str
    to_pascal_fn: Callable[[str], str]
    to_snake_fn: Callable[[str], str]
    type_overrides: dict[str, str]
    json_col_overrides: dict[tuple[str, str], str]

    def column_spec(self, column: Column) -> ColumnSpec:
        _, py_type, json_type = self._resolve(column)
        return ColumnSpec(
            name=column.name,
            table=column.table.name if column.table else "unknown",
            py_type=py_type,
            json_type=json_type,
        )

    def param_spec(self, column: Column, name: str, *, is_named: bool) -> ParamSpec:
        db_type, py_type, json_type = self._resolve(column)
        return ParamSpec(
            name=name,
            py_type=py_type,
            is_named=is_named,
            db_type=db_type,
            not_null=column.not_null,
            is_array=column.is_array,
            json_type=json_type,
        )

    def _resolve(self, column: Column) -> tuple[str, str, str | None]:
        db_type = column.type.name.removeprefix("pg_catalog.")

        json_type = None
        if column.table is not None:
            col_name = column.original_name or column.name
            json_type = self.json_col_overrides.get((column.table.name, col_name))

        if json_type:
            py_type = json_type
        elif db_type in self.type_overrides:
            py_type = self.type_overrides[db_type]
        elif db_type in _SQL_TYPE_MAP:
            py_type = _SQL_TYPE_MAP[db_type]
        elif self.catalog.schema_by_ref(column.type).has_enum(db_type):
            py_type = (
                self.to_pascal_fn(f"{self.package_name}_{self.to_snake_fn(db_type)}")
                if self.package_name
                else "str"
            )
        else:
            warnings.warn(
                f"Unknown SQL type: {db_type}, mapped to 'object'",
                category=UnknownSQLTypeWarning,
                stacklevel=1,
            )
            py_type = "object"

        if column.is_array:
            py_type = f"Sequence[{py_type}]"

        if not column.not_null:
            py_type += " | None"

        return db_type, py_type, json_type


def collect_used_enums(sqlc_res: SQLCResult) -> set[tuple[str, str]]:
    return {
        (schema.name, col.type.name)
        for col in (
            *(c for q in sqlc_res.queries for c in q.columns),
            *(p.column for q in sqlc_res.queries for p in q.params),
        )
        for schema in (sqlc_res.catalog.schema_by_ref(col.type),)
        if schema.has_enum(col.type.name)
    }


def generate_sql_package(  # noqa: PLR0913, PLR0914
    *,
    schema_path: Path,
    package_full_name: str,
    dsn_import: str,
    application_name: str | None = None,
    type_overrides: dict[str, str] | None = None,
    json_model_overrides: dict[str, str] | None = None,
    to_pascal_fn: Callable[[str], str] = alias_generators.to_pascal,
    to_snake_fn: Callable[[str], str] = alias_generators.to_snake,
    debug_path: Path | None = None,
    src_path: Path = Path(),
    tempdir_path: Path | None = None,
) -> bool:
    dsn_import_package, dsn_import_path = dsn_import.split(":")

    package_name = package_full_name.split(".")[-1]  # noqa: PLC0207
    sql_fn_name = f"{package_name}_sql"

    target_package_path = src_path / f"{package_full_name.replace('.', '/')}.py"

    queries = list(find_all_queries(src_path, sql_fn_name))
    validate_stmt_has_single_row_type(queries)
    all_locations: defaultdict[str, list[str]] = defaultdict(list)
    first_occurrence: dict[str, CodeQuery] = {}
    for q in queries:
        all_locations[q.name].append(q.location)
        if q.name not in first_occurrence:
            first_occurrence[q.name] = q

    queries = sorted(
        first_occurrence.values(),
        key=lambda q: (q.file, q.lineno),
    )

    dsn_package = importlib.import_module(dsn_import_package)
    dsn = eval(dsn_import_path, vars(dsn_package))  # noqa: S307

    sqlc_res = run_sqlc(
        src_path / schema_path,
        [(q.name, q.stmt) for q in queries],
        dsn=dsn,
        debug_path=debug_path,
        tempdir_path=tempdir_path,
    )

    if sqlc_res.error:
        logger.error("Error running SQLC:\n%s", sqlc_res.error)
        return False

    json_import_block = ""
    json_col_overrides: dict[tuple[str, str], str] = {}

    if json_model_overrides:
        json_compatible_types = {"json", "jsonb", "text", "varchar"}
        col_types = {
            (table.rel.name, column.name): column.type.name.removeprefix("pg_catalog.")
            for schema in sqlc_res.catalog.schemas
            for table in schema.tables
            for column in table.columns
        }
        tables = {table for table, _ in col_types}

        parsed: dict[tuple[str, str], tuple[str, str]] = {}
        for key, import_path in json_model_overrides.items():
            table_name, sep, col_name = key.partition(".")
            if not sep:
                msg = f"json_model_overrides key must be 'table.column', got: {key!r}"
                raise ValueError(msg)
            if table_name not in tables:
                msg = f"json_model_overrides: table {table_name!r} not found in catalog"
                raise ValueError(msg)
            if (table_name, col_name) not in col_types:
                msg = (
                    f"json_model_overrides: column {col_name!r} "
                    f"not found in table {table_name!r}"
                )
                raise ValueError(msg)

            db_type = col_types[table_name, col_name]
            if db_type not in json_compatible_types:
                msg = (
                    f"json_model_overrides: column "
                    f"{table_name}.{col_name} has type "
                    f"{db_type!r}, expected one of "
                    f"{json_compatible_types}"
                )
                raise ValueError(msg)

            module_path, sep, class_name = import_path.partition(":")
            if not sep:
                msg = (
                    "json_model_overrides value must be "
                    f"'module:Class', got: {import_path!r}"
                )
                raise ValueError(msg)

            parsed[table_name, col_name] = (module_path, class_name)

        modules = sorted({module for module, _ in parsed.values()})
        json_import_block = "\n" + "\n".join(f"import {m}" for m in modules)
        json_col_overrides = {
            key: f"{module}.{cls}" for key, (module, cls) in parsed.items()
        }

    resolver = TypeResolver(
        catalog=sqlc_res.catalog,
        package_name=package_name,
        to_pascal_fn=to_pascal_fn,
        to_snake_fn=to_snake_fn,
        type_overrides=type_overrides or {},
        json_col_overrides=json_col_overrides,
    )

    ordered_entities, result_types = map_entities(
        sqlc_res.queries,
        sqlc_res.used_schemas(),
        queries,
        resolver,
    )

    entities = [render_entity(e.name, e.column_specs) for e in ordered_entities]

    used_enums = collect_used_enums(sqlc_res)

    enums = [
        render_enum_class(e, package_name, to_pascal_fn, to_snake_fn)
        for schema in sqlc_res.catalog.schemas
        for e in schema.enums
        if (schema.name, e.name) in used_enums
    ]

    query_order = {q.name: i for i, q in enumerate(queries)}
    sqlc_queries = sorted(sqlc_res.queries, key=lambda q: query_order[q.name])

    query_classes = [
        render_query_class(
            q.name,
            q.text,
            [
                resolver.param_spec(
                    p.column,
                    p.column.name or f"param_{p.number}",
                    is_named=p.column.is_named_param,
                )
                for p in q.params
            ],
            result_types[q.name],
            len(q.columns),
            (
                resolver.column_spec(q.columns[0]).json_type
                if len(q.columns) == 1
                else None
            ),
            all_locations[q.name],
        )
        for q in sqlc_queries
    ]

    query_overloads = [
        render_query_overload(sql_fn_name, q.name, q.stmt, q.row_type) for q in queries
    ]

    query_dict_entries = [render_query_dict_entry(q.name, q.stmt) for q in queries]

    new_content = render_package(
        dsn_import_package,
        dsn_import_path,
        package_name,
        sql_fn_name,
        sorted(entities),
        sorted(enums),
        query_classes,
        query_overloads,
        query_dict_entries,
        application_name,
        json_import_block,
    )
    changed = write_if_changed(target_package_path, new_content + "\n")
    if changed:
        logger.info(f"Generated SQL package {package_full_name}")
    return changed


def render_package(  # noqa: PLR0913, PLR0917
    dsn_import_package: str,
    dsn_import_path: str,
    package_name: str,
    sql_fn_name: str,
    entities: list[str],
    enums: list[str],
    query_classes: list[str],
    query_overloads: list[str],
    query_dict_entries: list[str],
    application_name: str | None = None,
    json_import_block: str = "",
):
    return f"""

# Code generated by iron_sql, DO NOT EDIT.

# fmt: off
# pyright: reportUnusedImport=false
# ruff: noqa

import datetime
import decimal
import ipaddress
import uuid
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar
from typing import Literal
from typing import overload

import psycopg
import psycopg.abc
import psycopg.rows
import psycopg.sql
import psycopg.types.json

from iron_sql import runtime

from {dsn_import_package} import {dsn_import_path.split(".", maxsplit=1)[0]}
{json_import_block}

{package_name.upper()}_POOL = runtime.ConnectionPool(
    {dsn_import_path},
    name="{package_name}",
    application_name={application_name!r},
)

_{package_name}_connection = ContextVar[psycopg.AsyncConnection | None](
    "_{package_name}_connection",
    default=None,
)


@asynccontextmanager
async def {package_name}_connection() -> AsyncIterator[psycopg.AsyncConnection]:
    async with {package_name.upper()}_POOL.connection_in_context(
        _{package_name}_connection
    ) as conn:
        yield conn


@asynccontextmanager
async def {package_name}_transaction() -> AsyncIterator[None]:
    async with {package_name}_connection() as conn, conn.transaction():
        yield


@asynccontextmanager
async def {package_name}_listen_session(
    channel: str,
) -> AsyncIterator[AsyncGenerator[str]]:
    async with {package_name.upper()}_POOL.connection() as conn:
        async with runtime.listen(conn, channel) as payloads:
            yield payloads


async def {package_name}_notify(channel: str, payload: str = "") -> None:
    async with {package_name}_connection() as conn:
        await runtime.notify(conn, channel, payload)


{"\n\n\n".join(enums)}


{"\n\n\n".join(entities)}


class Query[T]:
    _stmt: ClassVar[psycopg.sql.SQL]
    _row_factory: psycopg.rows.BaseRowFactory[T]

    @asynccontextmanager
    async def _client_cursor(self, params: psycopg.abc.Params | None):
        async with (
            {package_name}_connection() as conn,
            psycopg.AsyncRawCursor(conn, row_factory=self._row_factory) as cur,
        ):
            await cur.execute(self._stmt, params)
            yield cur

    @asynccontextmanager
    async def _server_cursor(self, params: psycopg.abc.Params | None):
        async with (
            {package_name}_connection() as conn,
            runtime.ensure_transaction(conn),
            psycopg.AsyncRawServerCursor(conn, row_factory=self._row_factory, name=runtime.next_cursor_name()) as cur,
        ):
            await cur.execute(self._stmt, params)
            yield cur


{"\n\n\n".join(query_classes)}


_QUERIES: dict[str, type[Query]] = {{
    {(",\n    ").join(query_dict_entries)}
}}


{"\n".join(query_overloads)}
@overload
def {sql_fn_name}(stmt: str) -> Query: ...


def {sql_fn_name}(stmt: str, row_type: str | None = None) -> Query:
    if stmt in _QUERIES:
        return _QUERIES[stmt]()
    msg = f"Unknown statement: {{stmt!r}}"
    raise KeyError(msg)

    """.strip()  # noqa: E501


def render_enum_class(
    enum: Enum,
    package_name: str,
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str],
) -> str:
    class_name = to_pascal_fn(f"{package_name}_{to_snake_fn(enum.name)}")
    members = []
    seen_names: dict[str, int] = {}

    for val in enum.vals:
        name = to_snake_fn(val).upper()
        name = "".join(c if c.isalnum() else "_" for c in name)
        name = name.strip("_") or "EMPTY"
        if name[0].isdigit():
            name = "NUM" + name
        if name in seen_names:
            seen_names[name] += 1
            name = f"{name}_{seen_names[name]}"
        else:
            seen_names[name] = 1
        members.append(f'{name} = "{val}"')

    return f"""

class {class_name}(StrEnum):
    {indent_block("\n".join(members), "    ")}

    """.strip()


def render_entity(name: str, columns: tuple[ColumnSpec, ...]) -> str:
    fields = "\n    ".join(f"{c.name}: {c.py_type}" for c in columns)
    json_cols = [(c.name, c.json_type) for c in columns if c.json_type]
    validated = ""
    if json_cols:
        args = ", ".join(f"{n}={jt}" for n, jt in json_cols)
        validated = f"\n@runtime.json_validated({args})"

    return f"""

@dataclass(kw_only=True){validated}
class {name}:
    {fields}

    """.strip()


def deduplicate_params(params: list[ParamSpec]) -> list[ParamSpec]:
    seen = defaultdict(int)
    result: list[ParamSpec] = []
    for param in params:
        seen[param.name] += 1
        new_name = (
            f"{param.name}{seen[param.name]}" if seen[param.name] > 1 else param.name
        )
        result.append(dataclasses.replace(param, name=new_name))
    return result


def render_query_class(
    query_name: str,
    stmt: str,
    query_params: list[ParamSpec],
    result: str,
    columns_num: int,
    scalar_json_type: str | None,
    locations: list[str],
) -> str:
    query_params = deduplicate_params(query_params)

    match query_params:
        case []:
            params_arg = "None"
        case [param]:
            params_arg = f"({param.serialized_expr},)"
        case params:
            params_arg = f"({', '.join(p.serialized_expr for p in params)})"

    query_fn_params = [f"{p.name}: {p.py_type}" for p in query_params]
    first_named_param_idx = next(
        (i for i, p in enumerate(query_params) if p.is_named), -1
    )
    if first_named_param_idx >= 0:
        query_fn_params.insert(first_named_param_idx, "*")
    query_fn_params.insert(0, "self")

    base_result = result.removesuffix(" | None")

    if columns_num == 0:
        row_factory = "psycopg.rows.scalar_row"
    elif columns_num == 1:
        not_null_str = "True" if not result.endswith(" | None") else "False"
        validate_arg = (
            f", validate=lambda _v: runtime.validate_json_field({scalar_json_type}, _v)"
            if scalar_json_type
            else ""
        )
        row_factory = (
            f"runtime.typed_scalar_row"
            f"({base_result}, not_null={not_null_str}{validate_arg})"
        )
    else:
        row_factory = f"psycopg.rows.class_row({result})"

    if columns_num > 0:
        methods = f"""

async def query_all_rows({", ".join(query_fn_params)}) -> list[{result}]:
    async with self._client_cursor({params_arg}) as cur:
        return await cur.fetchall()

async def query_single_row({", ".join(query_fn_params)}) -> {result}:
    async with self._client_cursor({params_arg}) as cur:
        return runtime.get_one_row(await cur.fetchmany(2))

async def query_optional_row({", ".join(query_fn_params)}) -> {base_result} | None:
    async with self._client_cursor({params_arg}) as cur:
        return runtime.get_one_row_or_none(await cur.fetchmany(2))

def query_stream({", ".join(query_fn_params)}) -> AbstractAsyncContextManager[AsyncIterator[{result}]]:
    return self._server_cursor({params_arg})

        """.strip()  # noqa: E501
    else:
        methods = f"""

async def execute({", ".join(query_fn_params)}) -> None:
    async with self._client_cursor({params_arg}):
        pass

        """.strip()

    return f"""

class {query_name}(Query[{result}]):
    # See: {", ".join(locations)}
    _stmt = psycopg.sql.SQL({stmt!r})
    _row_factory = staticmethod({row_factory})

    {indent_block(methods, "    ")}

    """.strip()


def render_query_overload(
    sql_fn_name: str, query_name: str, stmt: str, row_type: str | None
) -> str:
    result_arg = ""
    if row_type:
        result_arg = f", row_type: Literal[{row_type!r}]"

    return f"""

@overload
def {sql_fn_name}(stmt: Literal[{stmt!r}]{result_arg}) -> {query_name}: ...

    """.strip()


def render_query_dict_entry(query_name: str, stmt: str) -> str:
    return f"{stmt!r}: {query_name}"


@dataclass(kw_only=True, frozen=True)
class CodeQuery:
    stmt: str
    row_type: str | None
    file: Path
    lineno: int

    @property
    def name(self) -> str:
        md5_hash = hashlib.md5(self.stmt.encode(), usedforsecurity=False).hexdigest()
        return f"Query_{md5_hash}{'_' + self.row_type if self.row_type else ''}"

    @property
    def location(self) -> str:
        return f"{self.file}:{self.lineno}"


@dataclass(kw_only=True, frozen=True)
class SQLEntity:
    resolver: TypeResolver
    set_name: str | None
    table_name: str | None
    columns: tuple[Column, ...]

    @property
    def name(self) -> str:
        if self.set_name:
            return self.set_name
        if self.table_name:
            return self.resolver.to_pascal_fn(
                f"{self.resolver.package_name}_{inflection.singularize(self.table_name)}"
            )
        hash_base = repr(self.column_specs)
        md5_hash = hashlib.md5(hash_base.encode(), usedforsecurity=False).hexdigest()
        return f"QueryResult_{md5_hash}"

    @property
    def column_specs(self) -> tuple[ColumnSpec, ...]:
        return tuple(self.resolver.column_spec(c) for c in self.columns)


def map_entities(
    queries_from_sqlc: tuple[Query, ...],
    used_schemas: tuple[str, ...],
    queries_from_code: list[CodeQuery],
    resolver: TypeResolver,
) -> tuple[list[SQLEntity], dict[str, str]]:
    row_types = {q.name: q.row_type for q in queries_from_code}

    table_entities = [
        SQLEntity(
            resolver=resolver,
            set_name=None,
            table_name=t.rel.name,
            columns=t.columns,
        )
        for sch in used_schemas
        for t in resolver.catalog.schema_by_name(sch).tables
    ]
    specs_to_entities = {e.column_specs: e for e in table_entities}

    for q in queries_from_sqlc:
        if row_types[q.name] and not q.columns:
            msg = f"Query has row_type={row_types[q.name]} but no result"
            raise ValueError(msg)
        if row_types[q.name] and len(q.columns) == 1:
            msg = f"Query has row_type={row_types[q.name]} but only one column"
            raise ValueError(msg)

    query_result_entities = {
        q.name: SQLEntity(
            resolver=resolver,
            set_name=row_types[q.name],
            table_name=None,
            columns=q.columns,
        )
        for q in queries_from_sqlc
        if len(q.columns) > 1
    }

    unique_entities = {
        e.column_specs: specs_to_entities.get(e.column_specs, e)
        for e in query_result_entities.values()
    }
    ordered_entities = sorted(
        unique_entities.values(),
        key=lambda e: (e.table_name is None, e.table_name or ""),
    )

    result_types = {}
    for q in queries_from_sqlc:
        if len(q.columns) == 0:
            result_types[q.name] = "None"
        elif len(q.columns) == 1:
            result_types[q.name] = resolver.column_spec(q.columns[0]).py_type
        else:
            column_specs = query_result_entities[q.name].column_specs
            result_types[q.name] = unique_entities[column_specs].name

    return ordered_entities, result_types


def find_fn_calls(
    root_path: Path, fn_name: str
) -> Iterator[tuple[Path, int, ast.Call]]:
    for path in root_path.glob("**/*.py"):
        content = path.read_text(encoding="utf-8")
        if fn_name not in content:
            continue
        try:
            tree = ast.parse(content, filename=str(path))
        except SyntaxError as exc:
            msg = f"Failed to parse {path}: {exc.msg} (line {exc.lineno})"
            raise SyntaxError(msg) from exc
        for node in ast.walk(tree):
            match node:
                case ast.Call(func=ast.Name(id=id)) if id == fn_name:
                    yield path, node.lineno, node
                case _:
                    pass


def find_all_queries(src_path: Path, sql_fn_name: str) -> Iterator[CodeQuery]:
    for file, lineno, node in find_fn_calls(src_path, sql_fn_name):
        relative_path = file.relative_to(src_path)

        stmt_arg = node.args[0]
        if (
            len(node.args) != 1
            or not isinstance(stmt_arg, ast.Constant)
            or not isinstance(stmt_arg.value, str)
        ):
            msg = (
                f"Invalid positional arguments for {sql_fn_name} "
                f"at {relative_path}:{lineno}, "
                "expected a single string literal"
            )
            raise TypeError(msg)

        stmt = stmt_arg.value

        row_type = None
        for kw in node.keywords:
            if not isinstance(kw.value, ast.Constant) or not isinstance(
                kw.value.value, str
            ):
                msg = (
                    f"Invalid keyword argument {kw.arg} for {sql_fn_name} "
                    f"at {relative_path}:{lineno}, expected a string literal"
                )
                raise TypeError(msg)
            if kw.arg == "row_type":
                row_type = kw.value.value
                break

        yield CodeQuery(
            stmt=stmt,
            row_type=row_type,
            file=relative_path,
            lineno=lineno,
        )


def validate_stmt_has_single_row_type(queries: list[CodeQuery]) -> None:
    first_by_stmt: dict[str, CodeQuery] = {}
    for query in queries:
        if query.stmt in first_by_stmt:
            first = first_by_stmt[query.stmt]
            if query.row_type != first.row_type:
                msg = (
                    f"row_type conflict: {first.location} has {first.row_type!r},"
                    f" {query.location} has {query.row_type!r}"
                )
                raise ValueError(msg)
        else:
            first_by_stmt[query.stmt] = query
