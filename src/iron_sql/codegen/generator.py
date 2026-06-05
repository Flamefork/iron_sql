import ast
import dataclasses
import hashlib
import importlib
import logging
import re
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
    element_py_type: str | None = None
    json_type: str | None = None


_JSON_PARAM_DUMPERS = {
    "json": "runtime.dump_json_value",
    "jsonb": "runtime.dump_json_value",
    "text": "runtime.dump_json_text",
    "varchar": "runtime.dump_json_text",
}


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
        expr = self.name
        wraps_value = False
        if self.json_type:
            dump_fn = _JSON_PARAM_DUMPERS[self.db_type]
            expr = f"{dump_fn}({self.json_type}, {self.name})"
            wraps_value = True
        match self.db_type:
            case "json":
                expr = f"psycopg.types.json.Json({expr})"
                wraps_value = True
            case "jsonb":
                expr = f"psycopg.types.json.Jsonb({expr})"
                wraps_value = True
            case _:
                pass
        if wraps_value and not self.not_null:
            expr = f"{expr} if {self.name} is not None else None"
        return expr


class UnknownSQLTypeWarning(UserWarning):
    pass


@dataclass(kw_only=True, frozen=True)
class ModuleExprRef:
    module_name: str
    module_expr: str

    @classmethod
    def parse(cls, value: str) -> "ModuleExprRef":
        module_name, sep, module_expr = value.partition(":")
        if not sep:
            msg = f"module expression must be 'module:expr', got: {value!r}"
            raise ValueError(msg)
        match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", module_expr)
        if match is None:
            msg = f"module expression must start with identifier, got: {module_expr!r}"
            raise ValueError(msg)
        return cls(module_name=module_name, module_expr=module_expr)

    @property
    def import_name(self) -> str:
        match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.module_expr)
        if match is None:
            msg = (
                "module expression must start with identifier, "
                f"got: {self.module_expr!r}"
            )
            raise ValueError(msg)
        return match.group()

    def evaluate[T](self, *, expected_type: type[T]) -> T:
        mod = importlib.import_module(self.module_name)
        value = eval(self.module_expr, vars(mod))  # noqa: S307
        if not isinstance(value, expected_type):
            msg = (
                f"module expression {self.module_name}:{self.module_expr} "
                f"must evaluate to "
                f"{expected_type.__name__}, got: {type(value).__name__}"
            )
            raise TypeError(msg)
        return value


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
    module_name: str
    to_pascal_fn: Callable[[str], str]
    to_snake_fn: Callable[[str], str]
    type_overrides: dict[str, str]
    json_column_type_overrides: dict[tuple[str, str], str]

    def column_spec(self, column: Column) -> ColumnSpec:
        _, py_type, element_py_type, json_type = self._resolve(column)
        return ColumnSpec(
            name=column.name,
            table=column.table.name if column.table else "unknown",
            py_type=py_type,
            element_py_type=element_py_type,
            json_type=json_type,
        )

    def param_spec(self, column: Column, name: str, *, is_named: bool) -> ParamSpec:
        db_type, py_type, _, json_type = self._resolve(column)
        return ParamSpec(
            name=name,
            py_type=py_type,
            is_named=is_named,
            db_type=db_type,
            not_null=column.not_null,
            is_array=column.is_array,
            json_type=json_type,
        )

    def _resolve(self, column: Column) -> tuple[str, str, str | None, str | None]:
        db_type = column.type.name.removeprefix("pg_catalog.")

        json_type = None
        if column.table is not None:
            col_name = column.original_name or column.name
            json_type = self.json_column_type_overrides.get((
                column.table.name,
                col_name,
            ))

        if json_type:
            py_type = json_type
        elif db_type in self.type_overrides:
            py_type = self.type_overrides[db_type]
        elif db_type in _SQL_TYPE_MAP:
            py_type = _SQL_TYPE_MAP[db_type]
        elif self.catalog.schema_by_ref(column.type).has_enum(db_type):
            py_type = (
                self.to_pascal_fn(f"{self.module_name}_{self.to_snake_fn(db_type)}")
                if self.module_name
                else "str"
            )
        else:
            warnings.warn(
                f"Unknown SQL type: {db_type}, mapped to 'object'",
                category=UnknownSQLTypeWarning,
                stacklevel=1,
            )
            py_type = "object"

        element_py_type = None
        if column.is_array:
            element_py_type = py_type
            py_type = f"Sequence[{py_type}]"

        if not column.not_null:
            py_type += " | None"

        return db_type, py_type, element_py_type, json_type


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


def map_sqlc_error(
    error: str,
    block_starts: list[tuple[int, str]],
    query_locations_by_name: dict[str, list[str]],
) -> str:
    def replace(m: re.Match[str]) -> str:
        line = int(m.group(1))
        name = next((n for start, n in reversed(block_starts) if start <= line), None)
        if name is None:
            return m.group(0)
        locations = query_locations_by_name.get(name)
        if not locations:
            return m.group(0)
        return f"{', '.join(locations)}:"

    return re.sub(r"queries\.sql:(\d+)(?::\d+)?:", replace, error)


def generate_sql_module(  # noqa: PLR0913, PLR0914
    *,
    schema_path: Path,
    module_full_name: str,
    dsn_expr: str,
    pool_options_expr: str | None = None,
    application_name: str | None = None,
    type_overrides: dict[str, str] | None = None,
    json_model_overrides: dict[str, str] | None = None,
    to_pascal_fn: Callable[[str], str] = alias_generators.to_pascal,
    to_snake_fn: Callable[[str], str] = alias_generators.to_snake,
    debug_path: Path | None = None,
    src_path: Path = Path(),
    tempdir_path: Path | None = None,
) -> bool:
    module_name = module_full_name.rsplit(".", maxsplit=1)[-1]
    sql_fn_name = f"{module_name}_sql"

    queries, query_locations_by_name = collect_queries(src_path, sql_fn_name)

    dsn_ref = ModuleExprRef.parse(dsn_expr)
    dsn = dsn_ref.evaluate(expected_type=str)
    pool_options_ref = (
        ModuleExprRef.parse(pool_options_expr)
        if pool_options_expr is not None
        else None
    )
    if pool_options_ref is not None:
        pool_options_ref.evaluate(expected_type=dict)

    sqlc_res, block_starts = run_sqlc(
        src_path / schema_path,
        [(q.name, q.sql) for q in queries],
        dsn=dsn,
        debug_path=debug_path,
        tempdir_path=tempdir_path,
    )

    if sqlc_res.error:
        mapped = map_sqlc_error(sqlc_res.error, block_starts, query_locations_by_name)
        logger.error(f"Error running SQLC:\n{mapped}")
        return False

    json_import_block, json_column_type_overrides = resolve_json_model_overrides(
        json_model_overrides or {}, sqlc_res.catalog
    )

    resolver = TypeResolver(
        catalog=sqlc_res.catalog,
        module_name=module_name,
        to_pascal_fn=to_pascal_fn,
        to_snake_fn=to_snake_fn,
        type_overrides=type_overrides or {},
        json_column_type_overrides=json_column_type_overrides,
    )

    ordered_entities, query_result_types = build_entities(
        sqlc_res.queries,
        sqlc_res.used_schemas(),
        queries,
        resolver,
    )

    entities = sorted(render_entity(e.name, e.column_specs) for e in ordered_entities)

    used_enums = collect_used_enums(sqlc_res)

    enum_specs = [
        (schema, e)
        for schema in sqlc_res.catalog.schemas
        for e in schema.enums
        if (schema.name, e.name) in used_enums
    ]

    enums = sorted(
        render_enum_class(e, module_name, to_pascal_fn, to_snake_fn)
        for _, e in enum_specs
    )

    enum_registry = sorted(
        (
            f"{schema.name}.{e.name}",
            enum_class_name(e.name, module_name, to_pascal_fn, to_snake_fn),
        )
        for schema, e in enum_specs
    )

    query_classes = render_query_classes(
        sqlc_res.queries, queries, resolver, query_result_types, query_locations_by_name
    )

    query_overloads = [
        render_query_overload(sql_fn_name, q.name, q.sql, q.row_type) for q in queries
    ]

    query_dict_entries = [render_query_dict_entry(q.name, q.sql) for q in queries]

    target_module_path = src_path / f"{module_full_name.replace('.', '/')}.py"

    new_content = render_module(
        dsn_ref,
        module_name,
        sql_fn_name,
        entities,
        enums,
        enum_registry,
        query_classes,
        query_overloads,
        query_dict_entries,
        application_name,
        json_import_block,
        pool_options_ref,
    )
    changed = write_if_changed(target_module_path, new_content + "\n")
    if changed:
        logger.info(f"Generated SQL module {module_full_name}")
    return changed


def collect_queries(
    src_path: Path, sql_fn_name: str
) -> tuple[list["CodeQuery"], defaultdict[str, list[str]]]:
    raw = list(find_all_queries(src_path, sql_fn_name))
    validate_sql_has_single_row_type(raw)
    query_locations_by_name: defaultdict[str, list[str]] = defaultdict(list)
    first_occurrence: dict[str, CodeQuery] = {}
    for q in raw:
        query_locations_by_name[q.name].append(q.location)
        if q.name not in first_occurrence:
            first_occurrence[q.name] = q
    queries = sorted(first_occurrence.values(), key=lambda q: (q.file, q.lineno))
    return queries, query_locations_by_name


def render_query_classes(
    sqlc_queries: tuple[Query, ...],
    queries: list["CodeQuery"],
    resolver: TypeResolver,
    query_result_types: dict[str, str],
    query_locations_by_name: defaultdict[str, list[str]],
) -> list[str]:
    query_order = {q.name: i for i, q in enumerate(queries)}
    return [
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
            query_result_types[q.name],
            tuple(resolver.column_spec(column) for column in q.columns),
            query_locations_by_name[q.name],
        )
        for q in sorted(sqlc_queries, key=lambda q: query_order[q.name])
    ]


def resolve_json_model_overrides(
    overrides: dict[str, str], catalog: Catalog
) -> tuple[str, dict[tuple[str, str], str]]:
    if not overrides:
        return "", {}

    json_compatible_types = set(_JSON_PARAM_DUMPERS)
    col_types = {
        (table.rel.name, column.name): column.type.name.removeprefix("pg_catalog.")
        for schema in catalog.schemas
        for table in schema.tables
        for column in table.columns
    }
    tables = {table for table, _ in col_types}

    parsed: dict[tuple[str, str], tuple[str, str]] = {}
    for key, import_path in overrides.items():
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
    import_block = "\n" + "\n".join(f"import {m}" for m in modules)
    col_overrides = {key: f"{module}.{cls}" for key, (module, cls) in parsed.items()}
    return import_block, col_overrides


def render_module(  # noqa: PLR0913, PLR0917
    dsn_ref: ModuleExprRef,
    module_name: str,
    sql_fn_name: str,
    entities: list[str],
    enums: list[str],
    enum_registry: list[tuple[str, str]],
    query_classes: list[str],
    query_overloads: list[str],
    query_dict_entries: list[str],
    application_name: str | None = None,
    json_import_block: str = "",
    pool_options_ref: ModuleExprRef | None = None,
) -> str:
    imports = [f"from {dsn_ref.module_name} import {dsn_ref.import_name}"]
    pool_args = [
        dsn_ref.module_expr,
        f'name="{module_name}"',
        f"application_name={application_name!r}",
    ]
    if pool_options_ref is not None:
        imports.append(
            f"from {pool_options_ref.module_name} import {pool_options_ref.import_name}"
        )
        pool_args.append(f"pool_options={pool_options_ref.module_expr}")

    if json_import_block:
        imports.extend(json_import_block.strip().splitlines())

    imports_block = "\n".join(imports)

    pre_pool_blocks: list[str] = []
    if enums:
        pre_pool_blocks.append("\n\n\n".join(enums))
    if enum_registry:
        registry_entries = ",\n    ".join(
            f'("{pg_name}", {class_name})' for pg_name, class_name in enum_registry
        )
        registry_type = "list[tuple[str, type[StrEnum]]]"
        pre_pool_blocks.append(
            f"ENUM_TYPES: {registry_type} = [\n    {registry_entries},\n]"
        )
        pool_args.append("enum_types=ENUM_TYPES")
    pre_pool_section = "".join(f"{block}\n\n\n" for block in pre_pool_blocks)

    pool_args_str = ",\n    ".join(pool_args)

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
from typing import Any
from typing import Literal
from typing import overload

import psycopg
import psycopg.rows
import psycopg.sql
import psycopg.types.json

from iron_sql import runtime

{imports_block}


{pre_pool_section}{module_name.upper()}_POOL = runtime.ConnectionPool(
    {pool_args_str},
)

_{module_name}_connection = ContextVar[psycopg.AsyncConnection | None](
    "_{module_name}_connection",
    default=None,
)


@asynccontextmanager
async def {module_name}_connection() -> AsyncGenerator[psycopg.AsyncConnection]:
    async with {module_name.upper()}_POOL.connection_in_context(_{module_name}_connection) as conn:
        yield conn


@asynccontextmanager
async def {module_name}_transaction() -> AsyncGenerator[None]:
    async with {module_name}_connection() as conn, conn.transaction():
        yield


@asynccontextmanager
async def {module_name}_listen_session(
    channel: str,
) -> AsyncGenerator[AsyncGenerator[str]]:
    async with {module_name.upper()}_POOL.connection() as conn:
        async with runtime.listen(conn, channel) as payloads:
            yield payloads


async def {module_name}_notify(channel: str, payload: str = "") -> None:
    async with {module_name}_connection() as conn:
        await runtime.notify(conn, channel, payload)


{"\n\n\n".join(entities)}


class Query[T](runtime.Query[T]):
    _connection_factory = staticmethod({module_name}_connection)


{"\n\n\n".join(query_classes)}


_QUERIES: dict[str, type[Query[Any]]] = {{
    {(",\n    ").join(query_dict_entries)}
}}


{"\n".join(query_overloads)}
@overload
def {sql_fn_name}(sql: str) -> Query[Any]: ...


def {sql_fn_name}(sql: str, row_type: str | None = None) -> Query[Any]:
    if sql in _QUERIES:
        return _QUERIES[sql]()
    msg = f"Unknown statement: {{sql!r}}"
    raise KeyError(msg)

    """.strip()  # noqa: E501


def enum_class_name(
    enum_name: str,
    module_name: str,
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str],
) -> str:
    return to_pascal_fn(f"{module_name}_{to_snake_fn(enum_name)}")


def render_enum_class(
    enum: Enum,
    module_name: str,
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str],
) -> str:
    class_name = enum_class_name(enum.name, module_name, to_pascal_fn, to_snake_fn)
    members: list[str] = []
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
    seen: defaultdict[str, int] = defaultdict(int)
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
    sql: str,
    query_params: list[ParamSpec],
    result: str,
    result_columns: tuple[ColumnSpec, ...],
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
    row_factory = render_row_factory(result, result_columns)

    if result_columns:
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
    _stmt = psycopg.sql.SQL({sql!r})
    _row_factory = staticmethod({row_factory})

    {indent_block(methods, "    ")}

    """.strip()


def render_row_factory(result: str, columns: tuple[ColumnSpec, ...]) -> str:
    match columns:
        case ():
            return "psycopg.rows.scalar_row"
        case (column,):
            return render_scalar_row_factory(result, column)
        case _:
            return f"psycopg.rows.class_row({result})"


def render_scalar_row_factory(result: str, column: ColumnSpec) -> str:
    base_result = result.removesuffix(" | None")
    not_null = "True" if not result.endswith(" | None") else "False"

    if column.element_py_type is not None:
        return f"runtime.typed_array_row({column.element_py_type}, not_null={not_null})"

    validate_arg = (
        f", validate=lambda _v: runtime.validate_json_field({column.json_type}, _v)"
        if column.json_type
        else ""
    )
    if " | " in base_result and not validate_arg:
        return f"runtime.typed_value_row(not_null={not_null})"

    return f"runtime.typed_scalar_row({base_result}, not_null={not_null}{validate_arg})"


def render_query_overload(
    sql_fn_name: str, query_name: str, sql: str, row_type: str | None
) -> str:
    result_arg = ""
    if row_type:
        result_arg = f", row_type: Literal[{row_type!r}]"

    return f"""

@overload
def {sql_fn_name}(sql: Literal[{sql!r}]{result_arg}) -> {query_name}: ...

    """.strip()


def render_query_dict_entry(query_name: str, sql: str) -> str:
    return f"{sql!r}: {query_name}"


@dataclass(kw_only=True, frozen=True)
class CodeQuery:
    sql: str
    row_type: str | None
    file: Path
    lineno: int

    @property
    def name(self) -> str:
        md5_hash = hashlib.md5(self.sql.encode(), usedforsecurity=False).hexdigest()
        return f"Query_{md5_hash}{'_' + self.row_type if self.row_type else ''}"

    @property
    def location(self) -> str:
        return f"{self.file}:{self.lineno}"


@dataclass(kw_only=True, frozen=True)
class SQLEntity:
    resolver: TypeResolver
    explicit_name: str | None
    table_name: str | None
    columns: tuple[Column, ...]

    @property
    def name(self) -> str:
        if self.explicit_name:
            return self.explicit_name
        if self.table_name:
            return self.resolver.to_pascal_fn(
                f"{self.resolver.module_name}_{inflection.singularize(self.table_name)}"
            )
        hash_base = repr(self.column_specs)
        md5_hash = hashlib.md5(hash_base.encode(), usedforsecurity=False).hexdigest()
        return f"QueryResult_{md5_hash}"

    @property
    def column_specs(self) -> tuple[ColumnSpec, ...]:
        return tuple(self.resolver.column_spec(c) for c in self.columns)


def build_entities(
    queries_from_sqlc: tuple[Query, ...],
    used_schemas: tuple[str, ...],
    queries_from_code: list[CodeQuery],
    resolver: TypeResolver,
) -> tuple[list[SQLEntity], dict[str, str]]:
    row_types = {q.name: q.row_type for q in queries_from_code}

    table_entities = [
        SQLEntity(
            resolver=resolver,
            explicit_name=None,
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
            explicit_name=row_types[q.name],
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

    query_result_types: dict[str, str] = {}
    for q in queries_from_sqlc:
        if len(q.columns) == 0:
            query_result_types[q.name] = "None"
        elif len(q.columns) == 1:
            query_result_types[q.name] = resolver.column_spec(q.columns[0]).py_type
        else:
            column_specs = query_result_entities[q.name].column_specs
            query_result_types[q.name] = unique_entities[column_specs].name

    return ordered_entities, query_result_types


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

        sql_arg = node.args[0]
        if (
            len(node.args) != 1
            or not isinstance(sql_arg, ast.Constant)
            or not isinstance(sql_arg.value, str)
        ):
            msg = (
                f"Invalid positional arguments for {sql_fn_name} "
                f"at {relative_path}:{lineno}, "
                "expected a single string literal"
            )
            raise TypeError(msg)

        sql = sql_arg.value

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
            sql=sql,
            row_type=row_type,
            file=relative_path,
            lineno=lineno,
        )


def validate_sql_has_single_row_type(queries: list[CodeQuery]) -> None:
    first_by_sql: dict[str, CodeQuery] = {}
    for query in queries:
        if query.sql in first_by_sql:
            first = first_by_sql[query.sql]
            if query.row_type != first.row_type:
                msg = (
                    f"row_type conflict: {first.location} has {first.row_type!r},"
                    f" {query.location} has {query.row_type!r}"
                )
                raise ValueError(msg)
        else:
            first_by_sql[query.sql] = query
