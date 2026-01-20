import ast
import dataclasses
import hashlib
import importlib
import logging
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from operator import attrgetter
from pathlib import Path

import inflection
from pydantic import alias_generators

from iron_sql.sqlc import Catalog
from iron_sql.sqlc import Column
from iron_sql.sqlc import Enum
from iron_sql.sqlc import Query
from iron_sql.sqlc import SQLCResult
from iron_sql.sqlc import run_sqlc

logger = logging.getLogger(__name__)


@dataclass(kw_only=True, frozen=True)
class ColumnPySpec:
    name: str
    table: str
    db_type: str
    not_null: bool
    is_array: bool
    py_type: str


def _collect_used_enums(sqlc_res: SQLCResult) -> set[tuple[str, str]]:
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
    to_pascal_fn=alias_generators.to_pascal,
    to_snake_fn=alias_generators.to_snake,
    debug_path: Path | None = None,
    src_path: Path = Path(),
    sqlc_path: Path | None = None,
    tempdir_path: Path | None = None,
    sqlc_command: list[str] | None = None,
) -> bool:
    """Generate a typed SQL package from schema and queries.

    Args:
        schema_path: Path to the Postgres schema SQL file (relative to src_path)
        package_full_name: Target module name (e.g., "myapp.mydatabase")
        dsn_import: Import path to DSN string (e.g.,
            "myapp.config:CONFIG.db_url")
        application_name: Optional application name for connection pool
        to_pascal_fn: Function to convert names to PascalCase (default:
            pydantic's to_pascal)
        to_snake_fn: Function to convert names to snake_case (default:
            pydantic's to_snake)
        debug_path: Optional path to save sqlc inputs for inspection
        src_path: Base source path for scanning queries (default: Path())
        sqlc_path: Optional path to sqlc binary if not in PATH
        tempdir_path: Optional path for temporary file generation
        sqlc_command: Optional command prefix to run sqlc

    Returns:
        True if the package was generated or modified, False otherwise
    """
    dsn_import_package, dsn_import_path = dsn_import.split(":")

    package_name = package_full_name.split(".")[-1]  # noqa: PLC0207
    sql_fn_name = f"{package_name}_sql"

    target_package_path = src_path / f"{package_full_name.replace('.', '/')}.py"

    queries = list(find_all_queries(src_path, sql_fn_name))
    queries = list({q.name: q for q in queries}.values())

    dsn = attrgetter(dsn_import_path)(importlib.import_module(dsn_import_package))

    sqlc_res = run_sqlc(
        src_path / schema_path,
        [(q.name, q.stmt) for q in queries],
        dsn=dsn,
        debug_path=debug_path,
        sqlc_path=sqlc_path,
        tempdir_path=tempdir_path,
        sqlc_command=sqlc_command,
    )

    if sqlc_res.error:
        logger.error("Error running SQLC:\n%s", sqlc_res.error)
        return False

    ordered_entities, result_types = map_entities(
        package_name,
        sqlc_res.queries,
        sqlc_res.catalog,
        sqlc_res.used_schemas(),
        queries,
        to_pascal_fn,
    )

    entities = [render_entity(e.name, e.column_specs) for e in ordered_entities]

    used_enums = _collect_used_enums(sqlc_res)

    enums = [
        render_enum_class(e, package_name, to_pascal_fn, to_snake_fn)
        for schema in sqlc_res.catalog.schemas
        for e in schema.enums
        if (schema.name, e.name) in used_enums
    ]

    query_classes = [
        render_query_class(
            q.name,
            q.text,
            package_name,
            [
                (
                    column_py_spec(
                        p.column,
                        sqlc_res.catalog,
                        package_name,
                        to_pascal_fn,
                        to_snake_fn,
                        p.number,
                    ),
                    p.column.is_named_param,
                )
                for p in q.params
            ],
            result_types[q.name],
            len(q.columns),
        )
        for q in sqlc_res.queries
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
        sorted(query_classes),
        sorted(query_overloads),
        sorted(query_dict_entries),
        application_name,
    )
    changed = write_if_changed(target_package_path, new_content + "\n")
    if changed:
        logger.info(f"Generated SQL package {package_full_name}")
    return changed


def render_package(
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
):
    return f"""

# Code generated by iron_sql, DO NOT EDIT.

# fmt: off
# pyright: reportUnusedImport=false
# ruff: noqa: A002
# ruff: noqa: ARG001
# ruff: noqa: C901
# ruff: noqa: E303
# ruff: noqa: E501
# ruff: noqa: F401
# ruff: noqa: FBT001
# ruff: noqa: I001
# ruff: noqa: N801
# ruff: noqa: PLR0912
# ruff: noqa: PLR0913
# ruff: noqa: PLR0917
# ruff: noqa: Q000
# ruff: noqa: RUF100

import datetime
import decimal
import uuid
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal
from typing import overload

import psycopg
import psycopg.rows
from psycopg.types import json as pgjson

from iron_sql import runtime

from {dsn_import_package} import {dsn_import_path.split(".", maxsplit=1)[0]}

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


{"\n\n\n".join(enums)}


{"\n\n\n".join(entities)}


class Query:
    pass


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

    """.strip()


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


def render_entity(
    name: str,
    columns: tuple[ColumnPySpec, ...],
) -> str:
    return f"""

@dataclass(kw_only=True)
class {name}:
    {"\n    ".join(f"{c.name}: {c.py_type}" for c in columns)}

    """.strip()


def deduplicate_params(
    params: list[tuple[ColumnPySpec, bool]],
) -> list[tuple[ColumnPySpec, bool]]:
    seen = defaultdict(int)
    result: list[tuple[ColumnPySpec, bool]] = []
    for column, is_named in params:
        seen[column.name] += 1
        new_name = (
            f"{column.name}{seen[column.name]}"
            if seen[column.name] > 1
            else column.name
        )
        new_column = dataclasses.replace(column, name=new_name)
        result.append((new_column, is_named))
    return result


def serialized_arg(column: ColumnPySpec) -> str:
    match column:
        case ColumnPySpec(db_type="json", not_null=True):
            msg = "Unsupported column type: json"
            raise TypeError(msg)
        case ColumnPySpec(db_type="jsonb", is_array=True):
            msg = "Unsupported column type: jsonb[]"
            raise TypeError(msg)
        case ColumnPySpec(db_type="jsonb", not_null=True, name=name):
            return f"pgjson.Jsonb({name})"
        case ColumnPySpec(db_type="jsonb", not_null=False, name=name):
            return f"pgjson.Jsonb({name}) if {name} is not None else None"
        case ColumnPySpec(name=name):
            return name


def render_query_class(
    query_name: str,
    stmt: str,
    package_name: str,
    query_params: list[tuple[ColumnPySpec, bool]],
    result: str,
    columns_num: int,
) -> str:
    query_params = deduplicate_params(query_params)

    match [column for column, _ in query_params]:
        case []:
            params_arg = "None"
        case [column]:
            params_arg = f"({serialized_arg(column)},)"
        case columns:
            params_arg = f"({', '.join(serialized_arg(column) for column in columns)})"

    query_fn_params = [f"{column.name}: {column.py_type}" for column, _ in query_params]
    first_named_param_idx = next(
        (i for i, (_, is_named_param) in enumerate(query_params) if is_named_param), -1
    )
    if first_named_param_idx >= 0:
        query_fn_params.insert(first_named_param_idx, "*")
    query_fn_params.insert(0, "self")

    base_result = result.removesuffix(" | None")

    if columns_num == 0:
        row_factory = "psycopg.rows.scalar_row"
    elif columns_num == 1:
        if result.endswith(" | None"):
            row_factory = f"runtime.typed_scalar_row({base_result}, not_null=False)"
        else:
            row_factory = f"runtime.typed_scalar_row({base_result}, not_null=True)"
    else:
        row_factory = f"psycopg.rows.class_row({result})"

    if columns_num > 0:
        methods = f"""

async def query_all_rows({", ".join(query_fn_params)}) -> list[{result}]:
    async with self._execute({params_arg}) as cur:
        return await cur.fetchall()

async def query_single_row({", ".join(query_fn_params)}) -> {result}:
    async with self._execute({params_arg}) as cur:
        return runtime.get_one_row(await cur.fetchmany(2))

async def query_optional_row({", ".join(query_fn_params)}) -> {base_result} | None:
    async with self._execute({params_arg}) as cur:
        return runtime.get_one_row_or_none(await cur.fetchmany(2))

        """.strip()
    else:
        methods = f"""

async def execute({", ".join(query_fn_params)}) -> None:
    async with self._execute({params_arg}):
        pass

        """.strip()

    return f"""

class {query_name}(Query):
    @asynccontextmanager
    async def _execute(self, params) -> AsyncIterator[psycopg.AsyncRawCursor[{result}]]:
        stmt = {stmt!r}
        async with (
            {package_name}_connection() as conn,
            psycopg.AsyncRawCursor(conn, row_factory={row_factory}) as cur,
        ):
            await cur.execute(stmt, params)
            yield cur

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


@dataclass(kw_only=True)
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


@dataclass(kw_only=True)
class SQLEntity:
    package_name: str
    set_name: str | None
    table_name: str | None
    columns: list[Column]
    catalog: Catalog = dataclasses.field(repr=False)
    to_pascal_fn: Callable[[str], str]
    to_snake_fn: Callable[[str], str] = inflection.underscore

    @property
    def name(self) -> str:
        if self.set_name:
            return self.set_name
        if self.table_name:
            return self.to_pascal_fn(
                f"{self.package_name}_{inflection.singularize(self.table_name)}"
            )
        hash_base = repr(self.column_specs)
        md5_hash = hashlib.md5(hash_base.encode(), usedforsecurity=False).hexdigest()
        return f"QueryResult_{md5_hash}"

    @property
    def column_specs(self) -> tuple[ColumnPySpec, ...]:
        return tuple(
            column_py_spec(
                c, self.catalog, self.package_name, self.to_pascal_fn, self.to_snake_fn
            )
            for c in self.columns
        )


def map_entities(
    package_name: str,
    queries_from_sqlc: list[Query],
    catalog: Catalog,
    used_schemas: list[str],
    queries_from_code: list[CodeQuery],
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str] = inflection.underscore,
):
    row_types = {q.name: q.row_type for q in queries_from_code}

    table_entities = [
        SQLEntity(
            package_name=package_name,
            set_name=None,
            table_name=t.rel.name,
            columns=t.columns,
            catalog=catalog,
            to_pascal_fn=to_pascal_fn,
            to_snake_fn=to_snake_fn,
        )
        for sch in used_schemas
        for t in catalog.schema_by_name(sch).tables
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
            package_name=package_name,
            set_name=row_types[q.name],
            table_name=None,
            columns=q.columns,
            catalog=catalog,
            to_pascal_fn=to_pascal_fn,
            to_snake_fn=to_snake_fn,
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
            result_types[q.name] = column_py_spec(
                q.columns[0], catalog, package_name, to_pascal_fn, to_snake_fn
            ).py_type
        else:
            column_spec = query_result_entities[q.name].column_specs
            result_types[q.name] = unique_entities[column_spec].name

    return ordered_entities, result_types


def column_py_spec(  # noqa: C901, PLR0912
    column: Column,
    catalog: Catalog,
    package_name: str,
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str] = inflection.underscore,
    number: int = 0,
) -> ColumnPySpec:
    db_type = column.type.name.removeprefix("pg_catalog.")
    match db_type:
        case "bool" | "boolean":
            py_type = "bool"
        case (
            "int2"
            | "int4"
            | "int8"
            | "smallint"
            | "integer"
            | "bigint"
            | "serial"
            | "bigserial"
        ):
            py_type = "int"
        case "float4" | "float8":
            py_type = "float"
        case "numeric":
            py_type = "decimal.Decimal"
        case "varchar" | "text":
            py_type = "str"
        case "bytea":
            py_type = "bytes"
        case "json" | "jsonb":
            py_type = "object"
        case "date":
            py_type = "datetime.date"
        case "time" | "timetz":
            py_type = "datetime.time"
        case "timestamp" | "timestamptz":
            py_type = "datetime.datetime"
        case "uuid":
            py_type = "uuid.UUID"
        case "any" | "anyelement":
            py_type = "object"
        case enum if catalog.schema_by_ref(column.type).has_enum(enum):
            py_type = (
                to_pascal_fn(f"{package_name}_{to_snake_fn(enum)}")
                if package_name
                else "str"
            )
        case _:
            logger.warning(f"Unknown SQL type: {column.type.name} ({column.name})")
            py_type = "object"

    if column.is_array:
        py_type = f"Sequence[{py_type}]"

    if not column.not_null:
        py_type += " | None"

    return ColumnPySpec(
        name=column.name or f"param_{number}",
        table=column.table.name if column.table else "unknown",
        db_type=db_type,
        not_null=column.not_null,
        is_array=column.is_array,
        py_type=py_type,
    )


def find_fn_calls(
    root_path: Path, fn_name: str
) -> Iterator[tuple[Path, int, ast.Call]]:
    for path in root_path.glob("**/*.py"):
        content = path.read_text(encoding="utf-8")
        if fn_name not in content:
            continue
        for node in ast.walk(ast.parse(content, filename=str(path))):
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


def indent_block(block: str, indent: str) -> str:
    return "\n".join(
        indent + line if i > 0 and line.strip() else line
        for i, line in enumerate(block.split("\n"))
    )


def write_if_changed(path: Path, new_content: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_content = path.read_text(encoding="utf-8") if path.exists() else None
    if existing_content == new_content:
        return False
    path.write_text(new_content, encoding="utf-8")
    path.touch()
    return True
