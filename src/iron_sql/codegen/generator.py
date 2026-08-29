import ast
import builtins
import dataclasses
import hashlib
import importlib
import io
import json
import keyword
import logging
import re
import symtable
import tokenize
import unicodedata
import warnings
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from typing import override

import inflection
from psycopg.sql import Identifier
from pydantic import alias_generators

from iron_sql.codegen.sqlc import Catalog
from iron_sql.codegen.sqlc import Column
from iron_sql.codegen.sqlc import Enum
from iron_sql.codegen.sqlc import Query
from iron_sql.codegen.sqlc import Schema
from iron_sql.codegen.sqlc import SQLCResult
from iron_sql.codegen.sqlc import run_sqlc
from iron_sql.codegen.util import indent_block
from iron_sql.codegen.util import write_if_changed

logger = logging.getLogger(__name__)

_DEFAULT_SRC_PATH = Path()


@dataclass(kw_only=True, frozen=True)
class JSONModelRef:
    module_path: str
    class_path: str
    origin: str = dataclasses.field(compare=False, repr=False)

    @property
    def expression(self) -> str:
        return f"{self.module_path}.{self.class_path}"

    @property
    def import_statement(self) -> str:
        return f"import {self.module_path}"

    @property
    def binding(self) -> str:
        return self.module_path.partition(".")[0]


@dataclass(kw_only=True, frozen=True)
class NameOrigin:
    name: str
    origin: str
    locations: tuple[str, ...]


@dataclass(kw_only=True, frozen=True)
class ModuleImportSpec:
    source: str
    binding: str
    origin: str


@dataclass(kw_only=True, frozen=True)
class ColumnSpec:
    name: str
    table: str
    py_type: str
    element_py_type: str | None = None
    json_model: JSONModelRef | None = None


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
    json_model: JSONModelRef | None = None

    def __post_init__(self) -> None:
        if self.db_type == "jsonb" and self.is_array:
            msg = "Unsupported column type: jsonb[]"
            raise TypeError(msg)

    @property
    def serialized_expr(self) -> str:
        expr = self.name
        wraps_value = False
        if self.json_model is not None:
            dump_fn = _JSON_PARAM_DUMPERS[self.db_type]
            expr = f"{dump_fn}({self.json_model.expression}, {self.name})"
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


class SQLGenerationError(ValueError):
    pass


@dataclass(kw_only=True, frozen=True)
class ModuleExprRef:
    module_name: str
    module_expr: str
    import_names: tuple[str, ...]
    module_bindings: tuple[str, ...]
    source_spellings: tuple[str, ...]

    @classmethod
    def parse(cls, value: str) -> "ModuleExprRef":
        module_name, sep, module_expr = value.partition(":")
        if not sep:
            msg = f"module expression must be 'module:expr', got: {value!r}"
            raise ValueError(msg)
        issues = dotted_path_name_issues(
            "module expression module path",
            module_name,
            f"module expression {value!r}",
            (),
        )
        raise_generated_name_issues(issues)
        try:
            expression_source = f"_iron_sql_value = ({module_expr})"
            compile(expression_source, "<module expression>", "exec")
            parsed = ast.parse(module_expr, "<module expression>", mode="eval")
            expression_table = symtable.symtable(
                expression_source,
                "<module expression>",
                "exec",
            )
        except SyntaxError as exc:
            msg = f"invalid module expression {module_expr!r}: {exc.msg}"
            raise ValueError(msg) from exc
        module_binding_spellings = tuple(
            dict.fromkeys(module_expression_binding_spellings(parsed, module_expr))
        )
        module_binding_names = {
            unicodedata.normalize("NFKC", name) for name in module_binding_spellings
        }
        external_reads = tuple(
            dict.fromkeys(
                name
                for table in walk_symbol_tables(expression_table)
                for name in expression_table_reads(table)
                if name != "_iron_sql_value" and name not in module_binding_names
            )
        )
        module = importlib.import_module(module_name)
        module_imports = tuple(name for name in external_reads if name in vars(module))
        source_spellings = tuple(
            dict.fromkeys((
                *module_binding_spellings,
                *expression_name_spellings(parsed, module_expr, set(module_imports)),
            ))
        )
        return cls(
            module_name=module_name,
            module_expr=module_expr,
            import_names=module_imports,
            module_bindings=module_binding_spellings,
            source_spellings=source_spellings,
        )

    def evaluate[T](self, *, expected_type: type[T]) -> T:
        mod = importlib.import_module(self.module_name)
        value = cast("object", eval(self.module_expr, vars(mod)))  # noqa: S307
        if not isinstance(value, expected_type):
            msg = (
                f"module expression {self.module_name}:{self.module_expr} "
                f"must evaluate to "
                f"{expected_type.__name__}, got: {type(value).__name__}"
            )
            raise TypeError(msg)
        return value


_SQL_TYPE_MAP: dict[str, str] = {
    "bool": "builtins.bool",
    "int2": "builtins.int",
    "int4": "builtins.int",
    "int8": "builtins.int",
    "oid": "builtins.int",
    "float4": "builtins.float",
    "float8": "builtins.float",
    "numeric": "decimal.Decimal",
    "varchar": "builtins.str",
    "text": "builtins.str",
    "bpchar": "builtins.str",
    "char": "builtins.str",
    "name": "builtins.str",
    "bytea": "builtins.bytes",
    "json": "builtins.object",
    "jsonb": "builtins.object",
    "inet": "ipaddress.IPv4Address | ipaddress.IPv6Address | ipaddress.IPv4Interface | ipaddress.IPv6Interface",  # noqa: E501
    "cidr": "ipaddress.IPv4Network | ipaddress.IPv6Network",
    "date": "datetime.date",
    "time": "datetime.time",
    "timetz": "datetime.time",
    "timestamp": "datetime.datetime",
    "timestamptz": "datetime.datetime",
    "interval": "datetime.timedelta",
    "uuid": "uuid.UUID",
    "any": "builtins.object",
    "anyelement": "builtins.object",
}


def canonical_type_name(column: Column, catalog: Catalog) -> str:
    # Identity before mapping: a user-defined type never loses to a built-in that
    # happens to share a spelling. A name qualified with a user schema is one by
    # construction - sqlc reports built-ins either under pg_catalog or, from the live
    # database, with no schema at all. Otherwise only the catalog can tell, and it
    # carries enums and composite types but no domains.
    name = column.pg_type_name
    if column.type.schema_name not in {"", "pg_catalog"}:
        return name
    schema = catalog.schema_by_ref(column.type)
    if schema.has_enum(name) or schema.has_composite(name):
        return name
    return column.pg_builtin_type_name


@dataclass(kw_only=True, frozen=True)
class TypeResolver:
    catalog: Catalog
    module_name: str
    to_pascal_fn: Callable[[str], str]
    to_snake_fn: Callable[[str], str]
    type_overrides: dict[str, str]
    json_column_type_overrides: dict[tuple[str, str], JSONModelRef]

    def column_spec(self, column: Column) -> ColumnSpec:
        _, py_type, element_py_type, json_model = self._resolve(column)
        return ColumnSpec(
            name=column.name,
            table=column.table.name if column.table else "unknown",
            py_type=py_type,
            element_py_type=element_py_type,
            json_model=json_model,
        )

    def param_spec(self, column: Column, name: str, *, is_named: bool) -> ParamSpec:
        db_type, py_type, _, json_model = self._resolve(column)
        return ParamSpec(
            name=name,
            py_type=py_type,
            is_named=is_named,
            db_type=db_type,
            not_null=column.not_null,
            is_array=column.is_array,
            json_model=json_model,
        )

    def _resolve(
        self, column: Column
    ) -> tuple[str, str, str | None, JSONModelRef | None]:
        db_type = canonical_type_name(column, self.catalog)

        json_model = None
        if column.table is not None:
            col_name = column.original_name or column.name
            json_model = self.json_column_type_overrides.get((
                column.table.name,
                col_name,
            ))

        if json_model is not None:
            py_type = json_model.expression
        elif db_type in self.type_overrides:
            py_type = self.type_overrides[db_type]
        elif self.catalog.schema_by_ref(column.type).has_enum(db_type):
            py_type = (
                self.to_pascal_fn(f"{self.module_name}_{self.to_snake_fn(db_type)}")
                if self.module_name
                else "builtins.str"
            )
        elif db_type in _SQL_TYPE_MAP:
            py_type = _SQL_TYPE_MAP[db_type]
        else:
            warnings.warn(
                f"Unknown SQL type: {db_type}, mapped to 'object'",
                category=UnknownSQLTypeWarning,
                stacklevel=1,
            )
            py_type = "builtins.object"

        element_py_type = None
        if column.is_array:
            element_py_type = py_type
            py_type = f"Sequence[{py_type}]"

        if not column.not_null:
            py_type += " | None"

        return db_type, py_type, element_py_type, json_model


def all_columns(sqlc_res: SQLCResult) -> Iterator[Column]:
    for query in sqlc_res.queries:
        yield from query.columns
        yield from (param.column for param in query.params)


def collect_used_enums(sqlc_res: SQLCResult) -> set[tuple[str, str]]:
    return {
        (schema.name, name)
        for col in all_columns(sqlc_res)
        for name in (canonical_type_name(col, sqlc_res.catalog),)
        for schema in (sqlc_res.catalog.schema_by_ref(col.type),)
        if schema.has_enum(name)
    }


def validate_type_overrides(overrides: dict[str, str], sqlc_res: SQLCResult) -> None:
    # Without queries there are no column types at all, so every key would look
    # unused and the report would degenerate into an empty list.
    if not sqlc_res.queries:
        return

    used_types = {
        canonical_type_name(col, sqlc_res.catalog) for col in all_columns(sqlc_res)
    }
    unused = sorted(set(overrides) - used_types)
    if unused:
        msg = (
            f"type_overrides: no query column or parameter has type "
            f"{', '.join(unused)}; types in use: {', '.join(sorted(used_types))}"
        )
        raise ValueError(msg)


class BuiltinNameQualifier(ast.NodeTransformer):
    @override
    def visit_Name(self, node: ast.Name) -> ast.expr:
        if isinstance(node.ctx, ast.Load) and hasattr(builtins, node.id):
            return ast.copy_location(
                ast.Attribute(
                    value=ast.Name(id="builtins", ctx=ast.Load()),
                    attr=node.id,
                    ctx=node.ctx,
                ),
                node,
            )
        return node


def normalize_type_override_expression(expression: str) -> str:
    try:
        parsed = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        msg = f"invalid type_overrides expression {expression!r}: {exc.msg}"
        raise ValueError(msg) from exc
    normalized = cast("ast.Expression", BuiltinNameQualifier().visit(parsed))
    ast.fix_missing_locations(normalized)
    roots = {
        node.id
        for node in ast.walk(normalized)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    available = {spec.binding for spec in _FIXED_MODULE_IMPORT_SPECS}
    unavailable = sorted(roots - available)
    if unavailable:
        msg = (
            f"type_overrides expression {expression!r} reads unavailable generated "
            f"module bindings: {', '.join(unavailable)}"
        )
        raise ValueError(msg)
    return ast.unparse(normalized)


def map_sqlc_error(
    error: str,
    block_starts: list[tuple[int, str]],
    query_locations_by_name: dict[str, list[str]],
) -> str:
    def replace(m: re.Match[str]) -> str:
        line = int(m.group(1))
        name = next((n for start, n in reversed(block_starts) if start <= line), None)
        missing_block_msg = (
            f"SQLC error line {line} precedes every generated query block"
        )
        if name is None:
            raise AssertionError(missing_block_msg)
        locations = query_locations_by_name[name]
        missing_locations_msg = f"SQLC query {name!r} has no source locations"
        if not locations:
            raise AssertionError(missing_locations_msg)
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
    src_path: Path = _DEFAULT_SRC_PATH,
    tempdir_path: Path | None = None,
) -> bool:
    module_name = module_full_name.rsplit(".", maxsplit=1)[-1]
    sql_fn_name = f"{module_name}_sql"

    discovered = collect_queries(src_path, sql_fn_name)
    queries = discovered.queries
    query_locations_by_name = discovered.query_locations_by_name

    if debug_path is not None:
        write_skipped_dirs(debug_path, discovered.skipped)

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
        msg = f"Error running SQLC:\n{mapped}"
        raise SQLGenerationError(msg)

    normalized_type_overrides = {
        db_type: normalize_type_override_expression(expression)
        for db_type, expression in (type_overrides or {}).items()
    }
    validate_type_overrides(normalized_type_overrides, sqlc_res)

    json_models, json_column_type_overrides = resolve_json_model_overrides(
        json_model_overrides or {}, sqlc_res.catalog
    )

    resolver = TypeResolver(
        catalog=sqlc_res.catalog,
        module_name=module_name,
        to_pascal_fn=to_pascal_fn,
        to_snake_fn=to_snake_fn,
        type_overrides=normalized_type_overrides,
        json_column_type_overrides=json_column_type_overrides,
    )

    ordered_entities, query_result_types = build_entities(
        sqlc_res.queries,
        sqlc_res.used_schemas(),
        queries,
        resolver,
    )

    query_params_by_name = {
        query.name: deduplicate_params([
            resolver.param_spec(
                param.column,
                param.column.name or f"param_{param.number}",
                is_named=param.column.is_named_param,
            )
            for param in query.params
        ])
        for query in sqlc_res.queries
    }
    query_result_columns_by_name = {
        query.name: tuple(resolver.column_spec(column) for column in query.columns)
        for query in sqlc_res.queries
    }
    code_queries_by_name = {query.name: query for query in queries}
    query_render_specs_by_name = {
        query.name: build_query_class_render_spec(
            code_queries_by_name[query.name].class_name,
            query.text,
            query_params_by_name[query.name],
            query_result_types[query.name],
            query_result_columns_by_name[query.name],
            tuple(query_locations_by_name[query.name]),
        )
        for query in sqlc_res.queries
    }

    used_enums = collect_used_enums(sqlc_res)

    enum_specs = tuple(
        EnumSpec(
            schema=schema,
            enum=enum,
            class_name=enum_class_name(
                enum.name,
                module_name,
                to_pascal_fn,
                to_snake_fn,
            ),
            members=tuple(prepare_enum_members(enum, to_snake_fn)),
        )
        for schema in sqlc_res.catalog.schemas
        for enum in schema.enums
        if (schema.name, enum.name) in used_enums
    )
    query_order = {query.name: index for index, query in enumerate(queries)}
    query_specs = tuple(
        GeneratedQuerySpec(
            source=code_queries_by_name[query.name],
            sql=query.text,
            result_type=query_result_types[query.name],
            result_columns=query_result_columns_by_name[query.name],
            render_spec=query_render_specs_by_name[query.name],
            locations=tuple(query_locations_by_name[query.name]),
        )
        for query in sorted(sqlc_res.queries, key=lambda item: query_order[item.name])
    )
    module_spec = GeneratedModuleSpec(
        module_full_name=module_full_name,
        module_name=module_name,
        sql_fn_name=sql_fn_name,
        dsn_ref=dsn_ref,
        pool_options_ref=pool_options_ref,
        json_models=json_models,
        imports=build_module_import_specs(dsn_ref, pool_options_ref, json_models),
        queries=query_specs,
        entities=tuple(ordered_entities),
        enums=enum_specs,
    )
    validate_generated_names(module_spec)

    entities = sorted(render_entity(e.name, e.column_specs) for e in ordered_entities)

    enums = sorted(
        render_enum_class(enum.class_name, list(enum.members)) for enum in enum_specs
    )

    enum_registry = sorted(
        (
            Identifier(enum.schema.name, enum.enum.name).as_string(None),
            enum.class_name,
        )
        for enum in enum_specs
    )

    query_classes = render_query_classes(query_specs)

    query_overloads = [
        render_query_overload(sql_fn_name, q.class_name, q.sql, q.row_type)
        for q in queries
    ]

    query_dict_entries = [render_query_dict_entry(q.class_name, q.sql) for q in queries]

    target_module_path = src_path / f"{module_full_name.replace('.', '/')}.py"

    new_content = render_module(
        module_spec,
        entities,
        enums,
        enum_registry,
        query_classes,
        query_overloads,
        query_dict_entries,
        application_name,
    )
    compile(new_content, target_module_path.as_posix(), "exec")
    changed = write_if_changed(target_module_path, new_content + "\n")
    if changed:
        logger.info(f"Generated SQL module {module_full_name}")
    return changed


def collect_queries(src_path: Path, sql_fn_name: str) -> "DiscoveredQueries":
    scanned = walk_scanned_tree(src_path)
    raw = list(find_all_queries(src_path, scanned.files, sql_fn_name))
    validate_sql_has_single_row_type(raw)
    query_locations_by_name: defaultdict[str, list[str]] = defaultdict(list)
    first_occurrence: dict[str, CodeQuery] = {}
    for q in raw:
        query_locations_by_name[q.name].append(q.location)
        if q.name not in first_occurrence:
            first_occurrence[q.name] = q
    queries = sorted(first_occurrence.values(), key=lambda q: (q.file, q.lineno))
    return DiscoveredQueries(
        queries=queries,
        query_locations_by_name=query_locations_by_name,
        skipped=list(scanned.skipped),
    )


def write_skipped_dirs(debug_path: Path, skipped: list["SkippedDir"]) -> None:
    # The directories the walk refused to enter, with the reason it did.
    # Without this file, a tree left alone on purpose and a tree lost by the scan
    # both appear as statements that are absent from the generated module.
    #
    # The report contains the path and the reason, but no details about contents.
    # Counting files under a refused directory would perform the walk refused above.
    debug_path.mkdir(parents=True, exist_ok=True)
    (debug_path / "skipped_dirs.json").write_text(
        json.dumps(
            [{"location": entry.location, "reason": entry.reason} for entry in skipped],
            indent=2,
        ),
        encoding="utf-8",
    )


def render_query_classes(queries: tuple["GeneratedQuerySpec", ...]) -> list[str]:
    return [render_query_class_spec(query.render_spec) for query in queries]


def resolve_json_model_overrides(
    overrides: dict[str, str], catalog: Catalog
) -> tuple[tuple[JSONModelRef, ...], dict[tuple[str, str], JSONModelRef]]:
    if not overrides:
        return (), {}

    json_compatible_types = set(_JSON_PARAM_DUMPERS)
    col_types = {
        (table.rel.name, column.name): canonical_type_name(column, catalog)
        for schema in catalog.schemas
        for table in schema.tables
        for column in table.columns
    }
    tables = {table for table, _ in col_types}

    parsed: dict[tuple[str, str], tuple[str, str, str]] = {}
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

        origin = f"json_model_overrides[{key!r}] = {import_path!r}"
        parsed[table_name, col_name] = (module_path, class_name, origin)

    import_paths = sorted({(module, cls) for module, cls, _ in parsed.values()})
    origins_by_path: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
    for module_path, class_path, origin in parsed.values():
        origins_by_path[module_path, class_path].append(origin)
    refs_by_path = {
        (module_path, class_path): JSONModelRef(
            module_path=module_path,
            class_path=class_path,
            origin="; ".join(origins_by_path[module_path, class_path]),
        )
        for module_path, class_path in import_paths
    }
    column_refs = {
        key: refs_by_path[module_path, class_path]
        for key, (module_path, class_path, _) in parsed.items()
    }
    return tuple(refs_by_path[path] for path in import_paths), column_refs


_FIXED_MODULE_IMPORT_SPECS = (
    ModuleImportSpec(
        source="import builtins", binding="builtins", origin="generated import builtins"
    ),
    ModuleImportSpec(
        source="import datetime", binding="datetime", origin="generated import datetime"
    ),
    ModuleImportSpec(
        source="import decimal", binding="decimal", origin="generated import decimal"
    ),
    ModuleImportSpec(
        source="import ipaddress",
        binding="ipaddress",
        origin="generated import ipaddress",
    ),
    ModuleImportSpec(
        source="import uuid", binding="uuid", origin="generated import uuid"
    ),
    ModuleImportSpec(
        source="from collections.abc import AsyncGenerator",
        binding="AsyncGenerator",
        origin="generated import AsyncGenerator",
    ),
    ModuleImportSpec(
        source="from collections.abc import AsyncIterator",
        binding="AsyncIterator",
        origin="generated import AsyncIterator",
    ),
    ModuleImportSpec(
        source="from collections.abc import Sequence",
        binding="Sequence",
        origin="generated import Sequence",
    ),
    ModuleImportSpec(
        source="from contextlib import AbstractAsyncContextManager",
        binding="AbstractAsyncContextManager",
        origin="generated import AbstractAsyncContextManager",
    ),
    ModuleImportSpec(
        source="from contextlib import asynccontextmanager",
        binding="asynccontextmanager",
        origin="generated import asynccontextmanager",
    ),
    ModuleImportSpec(
        source="from contextvars import ContextVar",
        binding="ContextVar",
        origin="generated import ContextVar",
    ),
    ModuleImportSpec(
        source="from dataclasses import dataclass",
        binding="dataclass",
        origin="generated import dataclass",
    ),
    ModuleImportSpec(
        source="from enum import StrEnum",
        binding="StrEnum",
        origin="generated import StrEnum",
    ),
    ModuleImportSpec(
        source="from typing import Any", binding="Any", origin="generated import Any"
    ),
    ModuleImportSpec(
        source="from typing import Literal",
        binding="Literal",
        origin="generated import Literal",
    ),
    ModuleImportSpec(
        source="from typing import overload",
        binding="overload",
        origin="generated import overload",
    ),
    ModuleImportSpec(
        source="import psycopg", binding="psycopg", origin="generated psycopg imports"
    ),
    ModuleImportSpec(
        source="import psycopg.rows",
        binding="psycopg",
        origin="generated psycopg imports",
    ),
    ModuleImportSpec(
        source="import psycopg.sql",
        binding="psycopg",
        origin="generated psycopg imports",
    ),
    ModuleImportSpec(
        source="import psycopg.types.json",
        binding="psycopg",
        origin="generated psycopg imports",
    ),
    ModuleImportSpec(
        source="from iron_sql import runtime",
        binding="runtime",
        origin="generated import runtime",
    ),
)

_QUERY_METHOD_EXTERNAL_READS: dict[str, tuple[str, ...]] = {
    "execute": (),
    "query_all_rows": (),
    "query_single_row": ("runtime",),
    "query_optional_row": ("runtime",),
    "query_stream": (),
}


def query_method_required_external_reads(method_name: str) -> tuple[str, ...]:
    return _QUERY_METHOD_EXTERNAL_READS[method_name]


def build_module_import_specs(
    dsn_ref: ModuleExprRef,
    pool_options_ref: ModuleExprRef | None,
    json_models: tuple[JSONModelRef, ...],
) -> tuple[ModuleImportSpec, ...]:
    specs = list(_FIXED_MODULE_IMPORT_SPECS)
    for ref, label in (
        (dsn_ref, "dsn expression import"),
        (pool_options_ref, "pool options expression import"),
    ):
        if ref is None:
            continue
        specs.extend(
            ModuleImportSpec(
                source=f"from {ref.module_name} import {name}",
                binding=name,
                origin=f"{label} from {ref.module_name}:{name}",
            )
            for name in ref.import_names
        )
    json_origins_by_binding = {
        model.binding: "; ".join(
            ref.origin for ref in json_models if ref.binding == model.binding
        )
        for model in json_models
    }
    specs.extend(
        ModuleImportSpec(
            source=model.import_statement,
            binding=model.binding,
            origin=json_origins_by_binding[model.binding],
        )
        for model in json_models
    )
    merged: dict[tuple[str, str], ModuleImportSpec] = {}
    for spec in specs:
        key = (spec.source, spec.binding)
        previous = merged.get(key)
        if previous is None:
            merged[key] = spec
            continue
        origins = tuple(dict.fromkeys((previous.origin, spec.origin)))
        merged[key] = dataclasses.replace(previous, origin="; ".join(origins))
    return tuple(merged.values())


@dataclass(kw_only=True, frozen=True)
class FunctionScopeSpec:
    class_name: str
    function_name: str
    parameters: tuple[NameOrigin, ...]
    locals: tuple[NameOrigin, ...]
    external_reads: tuple[NameOrigin, ...]

    @property
    def label(self) -> str:
        return f"method {self.class_name}.{self.function_name}"


@dataclass(kw_only=True, frozen=True)
class ClassBodyStepSpec:
    source: str
    binding: NameOrigin
    eager_reads: tuple[NameOrigin, ...]
    function_scope: FunctionScopeSpec | None = None


@dataclass(kw_only=True, frozen=True)
class QueryClassRenderSpec:
    class_name: str
    result_type: str
    locations: tuple[str, ...]
    steps: tuple[ClassBodyStepSpec, ...]


@dataclass(kw_only=True, frozen=True)
class EnumSpec:
    schema: Schema
    enum: Enum
    class_name: str
    members: tuple[tuple[str, str], ...]


@dataclass(kw_only=True, frozen=True)
class GeneratedQuerySpec:
    source: "CodeQuery"
    sql: str
    result_type: str
    result_columns: tuple[ColumnSpec, ...]
    render_spec: QueryClassRenderSpec
    locations: tuple[str, ...]


@dataclass(kw_only=True, frozen=True)
class GeneratedModuleSpec:
    module_full_name: str
    module_name: str
    sql_fn_name: str
    dsn_ref: ModuleExprRef
    pool_options_ref: ModuleExprRef | None
    json_models: tuple[JSONModelRef, ...]
    imports: tuple[ModuleImportSpec, ...]
    queries: tuple[GeneratedQuerySpec, ...]
    entities: tuple["SQLEntity", ...]
    enums: tuple[EnumSpec, ...]

    @property
    def all_locations(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                location for query in self.queries for location in query.locations
            )
        )


def query_method_external_reads(
    method_name: str, query_params: list[ParamSpec]
) -> tuple[str, ...]:
    reads = set(query_method_required_external_reads(method_name))
    for param in query_params:
        if param.json_model is not None:
            reads.add("runtime")
            reads.add(param.json_model.binding)
        if param.db_type in {"json", "jsonb"}:
            reads.add("psycopg")
    return tuple(sorted(reads))


def name_claim_issues(
    scope: str,
    claims: list[NameOrigin],
    *,
    class_name: str | None = None,
) -> list[tuple[str, str, str]]:
    issues: list[tuple[str, str, str]] = []
    claims_by_name: defaultdict[str, list[NameOrigin]] = defaultdict(list)
    for claim in claims:
        name = claim.name
        normalized_name = unicodedata.normalize("NFKC", name)
        binding_name = (
            mangle_class_name(class_name, normalized_name)
            if class_name is not None
            else normalized_name
        )
        claims_by_name[binding_name].append(claim)

        context = (
            f"origin: {claim.origin}; SQL call sites: "
            f"{', '.join(claim.locations) or 'none'}"
        )
        if not name.isidentifier():
            issues.append((
                scope,
                binding_name,
                f"{scope}: {name!r} is not a valid Python identifier; {context}",
            ))
        elif keyword.iskeyword(normalized_name):
            if name == normalized_name:
                description = f"{name!r} is a Python keyword"
            else:
                description = (
                    f"{name!r} normalizes to Python keyword {normalized_name!r}"
                )
            issues.append((
                scope,
                binding_name,
                f"{scope}: {description}; {context}",
            ))
        elif name != normalized_name:
            description = f"{name!r} is normalized by Python to {normalized_name!r}"
            issues.append((
                scope,
                binding_name,
                f"{scope}: {description}; {context}",
            ))
        elif name != binding_name:
            description = f"{name!r} is mangled by Python to {binding_name!r}"
            issues.append((
                scope,
                binding_name,
                f"{scope}: {description}; {context}",
            ))

    for binding_name, name_claims in claims_by_name.items():
        origins = "; ".join(claim.origin for claim in name_claims)
        locations = tuple(
            dict.fromkeys(
                location for claim in name_claims for location in claim.locations
            )
        )
        location_text = ", ".join(locations) or "none"
        context = f"origins: {origins}; SQL call sites: {location_text}"
        if len(name_claims) > 1:
            spellings = tuple(dict.fromkeys(claim.name for claim in name_claims))
            if len(spellings) == 1:
                description = f"{spellings[0]!r} is claimed more than once"
            else:
                rendered_spellings = ", ".join(repr(name) for name in spellings)
                description = (
                    f"{rendered_spellings} resolve to Python binding {binding_name!r}"
                )
            issues.append((
                scope,
                binding_name,
                f"{scope}: {description}; {context}",
            ))
    return issues


def mangle_class_name(class_name: str, name: str) -> str:
    if not name.startswith("__") or name.endswith("__"):
        return name
    normalized_class_name = unicodedata.normalize("NFKC", class_name).lstrip("_")
    if not normalized_class_name:
        return name
    return f"_{normalized_class_name}{name}"


def dotted_path_name_issues(
    scope: str,
    path: str,
    origin: str,
    locations: tuple[str, ...],
) -> list[tuple[str, str, str]]:
    return [
        issue
        for index, component in enumerate(path.split("."), start=1)
        for issue in name_claim_issues(
            f"{scope} component {index}",
            [NameOrigin(name=component, origin=origin, locations=locations)],
        )
    ]


def raise_generated_name_issues(issues: list[tuple[str, str, str]]) -> None:
    if not issues:
        return
    details = "\n".join(f"- {message}" for _, _, message in sorted(issues))
    msg = f"Invalid generated Python names:\n{details}"
    raise ValueError(msg)


def module_binding_claims(
    *,
    module_name: str,
    sql_fn_name: str,
    imports: tuple[ModuleImportSpec, ...],
    dsn_ref: ModuleExprRef,
    pool_options_ref: ModuleExprRef | None,
    has_enums: bool,
    locations: tuple[str, ...],
) -> list[NameOrigin]:
    claims = [
        NameOrigin(
            name=binding,
            origin=origin,
            locations=locations,
        )
        for binding, origin in dict.fromkeys(
            (spec.binding, spec.origin) for spec in imports
        )
    ]
    claims.extend(
        NameOrigin(
            name=name,
            origin=f"binding created by dsn expression {dsn_ref.module_expr!r}",
            locations=locations,
        )
        for name in dsn_ref.module_bindings
    )
    if pool_options_ref is not None:
        claims.extend(
            NameOrigin(
                name=name,
                origin=(
                    "binding created by pool options expression "
                    f"{pool_options_ref.module_expr!r}"
                ),
                locations=locations,
            )
            for name in pool_options_ref.module_bindings
        )
    claims.extend(
        NameOrigin(
            name=name,
            origin="implicit module binding created by Python import machinery",
            locations=locations,
        )
        for name in (
            "__annotations__",
            "__builtins__",
            "__cached__",
            "__doc__",
            "__file__",
            "__loader__",
            "__name__",
            "__package__",
            "__spec__",
        )
    )
    claims.extend(
        NameOrigin(name=name, origin=origin, locations=locations)
        for name, origin in (
            (f"{module_name.upper()}_POOL", "generated connection pool"),
            (
                f"_{module_name}_connection",
                "generated connection context variable",
            ),
            (f"{module_name}_connection", "generated connection helper"),
            (f"{module_name}_transaction", "generated transaction helper"),
            (f"{module_name}_listen_session", "generated listen helper"),
            (f"{module_name}_notify", "generated notify helper"),
            ("Query", "generated query base class"),
            ("_QUERIES", "generated query registry"),
            (sql_fn_name, "generated SQL lookup function"),
        )
    )
    if has_enums:
        claims.append(
            NameOrigin(
                name="ENUM_TYPES",
                origin="generated enum registry",
                locations=locations,
            )
        )
    return claims


def query_method_scope_specs(
    class_name: str,
    query_params: list[ParamSpec],
    result_columns: tuple[ColumnSpec, ...],
    locations: tuple[str, ...],
) -> tuple[FunctionScopeSpec, ...]:
    method_names = (
        (
            "query_all_rows",
            "query_single_row",
            "query_optional_row",
            "query_stream",
        )
        if result_columns
        else ("execute",)
    )
    cursor_methods = {
        "query_all_rows",
        "query_single_row",
        "query_optional_row",
    }
    return tuple(
        FunctionScopeSpec(
            class_name=class_name,
            function_name=method_name,
            parameters=(
                NameOrigin(
                    name="self",
                    origin="generated method receiver",
                    locations=locations,
                ),
                *(
                    NameOrigin(
                        name=param.name,
                        origin=f"query parameter {index}",
                        locations=locations,
                    )
                    for index, param in enumerate(query_params, start=1)
                ),
            ),
            locals=(
                (
                    NameOrigin(
                        name="cur",
                        origin="generated cursor local",
                        locations=locations,
                    ),
                )
                if method_name in cursor_methods
                else ()
            ),
            external_reads=tuple(
                NameOrigin(
                    name=name,
                    origin="generated method body external read",
                    locations=locations,
                )
                for name in query_method_external_reads(method_name, query_params)
            ),
        )
        for method_name in method_names
    )


def function_scope_name_issues(
    scope: FunctionScopeSpec,
) -> list[tuple[str, str, str]]:
    issues = name_claim_issues(
        scope.label,
        list(scope.parameters),
        class_name=scope.class_name,
    )
    issues.extend(
        name_claim_issues(
            scope.label,
            [*scope.locals, *scope.external_reads],
            class_name=scope.class_name,
        )
    )
    parameter_bindings = {
        mangle_class_name(
            scope.class_name,
            unicodedata.normalize("NFKC", parameter.name),
        ): parameter
        for parameter in scope.parameters
    }
    local_bindings = {
        mangle_class_name(
            scope.class_name,
            unicodedata.normalize("NFKC", local.name),
        ): local
        for local in scope.locals
    }
    for read in scope.external_reads:
        binding_name = mangle_class_name(
            scope.class_name,
            unicodedata.normalize("NFKC", read.name),
        )
        shadow = parameter_bindings.get(binding_name) or local_bindings.get(
            binding_name
        )
        if shadow is None:
            continue
        locations = tuple(dict.fromkeys((*shadow.locations, *read.locations)))
        context = (
            f"origins: {shadow.origin}; {read.origin}; SQL call sites: "
            f"{', '.join(locations) or 'none'}"
        )
        issues.append((
            scope.label,
            binding_name,
            f"{scope.label}: {read.name!r} is claimed more than once; {context}",
        ))
    return issues


def query_class_render_name_issues(
    spec: QueryClassRenderSpec,
) -> list[tuple[str, str, str]]:
    scope = f"class {spec.class_name!r}"
    issues: list[tuple[str, str, str]] = []
    previous_bindings: dict[str, NameOrigin] = {}
    for step in spec.steps:
        issues.extend(
            name_claim_issues(
                scope,
                [step.binding],
                class_name=spec.class_name,
            )
        )
        for read in step.eager_reads:
            binding_name = mangle_class_name(
                spec.class_name,
                unicodedata.normalize("NFKC", read.name),
            )
            shadow = previous_bindings.get(binding_name)
            if shadow is None:
                continue
            locations = tuple(dict.fromkeys((*shadow.locations, *read.locations)))
            context = (
                f"origins: {shadow.origin}; {read.origin}; SQL call sites: "
                f"{', '.join(locations) or 'none'}"
            )
            issues.append((
                scope,
                binding_name,
                f"{scope}: {read.name!r} is claimed more than once; {context}",
            ))
        normalized_binding = unicodedata.normalize("NFKC", step.binding.name)
        previous_bindings[mangle_class_name(spec.class_name, normalized_binding)] = (
            step.binding
        )
        if step.function_scope is not None:
            issues.extend(function_scope_name_issues(step.function_scope))
    return issues


def entity_name_analysis(
    module: GeneratedModuleSpec,
) -> tuple[list[NameOrigin], list[tuple[str, str, str]]]:
    module_claims: list[NameOrigin] = []
    issues: list[tuple[str, str, str]] = []
    entity_locations_by_name: defaultdict[str, list[str]] = defaultdict(list)
    for query in module.queries:
        entity_locations_by_name[query.result_type].extend(query.locations)

    for entity in module.entities:
        locations = tuple(entity_locations_by_name[entity.name]) or module.all_locations
        origin = (
            f"generated query result entity for row_type {entity.explicit_name!r}"
            if entity.explicit_name is not None
            else f"generated entity for table {entity.table_name!r}"
        )
        module_claims.append(
            NameOrigin(name=entity.name, origin=origin, locations=locations)
        )
        field_claims = [
            NameOrigin(
                name=column.name,
                origin=f"generated result field {index}",
                locations=locations,
            )
            for index, column in enumerate(entity.column_specs, start=1)
        ]
        issues.extend(
            name_claim_issues(
                f"class {entity.name!r}",
                field_claims,
                class_name=entity.name,
            )
        )
        issues.extend(result_protocol_field_issues(entity.name, field_claims))
    return module_claims, issues


def result_protocol_field_issues(
    class_name: str, field_claims: list[NameOrigin]
) -> list[tuple[str, str, str]]:
    issues: list[tuple[str, str, str]] = []
    for field_claim in field_claims:
        normalized_name = unicodedata.normalize("NFKC", field_claim.name)
        if not (normalized_name.startswith("__") and normalized_name.endswith("__")):
            continue
        location_text = ", ".join(field_claim.locations) or "none"
        description = f"{field_claim.name!r} is reserved for Python protocol attributes"
        context = f"origin: {field_claim.origin}; SQL call sites: {location_text}"
        scope = f"class {class_name!r}"
        issues.append((
            scope,
            normalized_name,
            f"{scope}: {description}; {context}",
        ))
    return issues


def enum_name_analysis(
    module: GeneratedModuleSpec,
) -> tuple[list[NameOrigin], list[tuple[str, str, str]]]:
    module_claims: list[NameOrigin] = []
    issues: list[tuple[str, str, str]] = []
    for enum in module.enums:
        module_claims.append(
            NameOrigin(
                name=enum.class_name,
                origin=f"generated enum for {enum.schema.name}.{enum.enum.name}",
                locations=module.all_locations,
            )
        )
        issues.extend(
            name_claim_issues(
                f"class {enum.class_name!r}",
                [
                    NameOrigin(
                        name=name,
                        origin=f"generated enum member for label {value!r}",
                        locations=module.all_locations,
                    )
                    for name, value in enum.members
                ],
                class_name=enum.class_name,
            )
        )
    return module_claims, issues


def query_name_claims(module: GeneratedModuleSpec) -> list[NameOrigin]:
    claims: list[NameOrigin] = []
    for query in module.queries:
        claims.append(
            NameOrigin(
                name=query.source.class_name,
                origin=f"generated query class for {query.source.sql!r}",
                locations=query.locations,
            )
        )
        if query.source.row_type and query.result_type != query.source.row_type:
            claims.append(
                NameOrigin(
                    name=query.source.row_type,
                    origin=f"row_type for {query.source.sql!r}",
                    locations=query.locations,
                )
            )
    return claims


def validate_generated_names(module: GeneratedModuleSpec) -> None:
    issues: list[tuple[str, str, str]] = []

    all_locations = module.all_locations
    issues.extend(
        dotted_path_name_issues(
            "output module path",
            module.module_full_name,
            "module_full_name",
            all_locations,
        )
    )
    module_claims = module_binding_claims(
        module_name=module.module_name,
        sql_fn_name=module.sql_fn_name,
        imports=module.imports,
        dsn_ref=module.dsn_ref,
        pool_options_ref=module.pool_options_ref,
        has_enums=bool(module.enums),
        locations=all_locations,
    )
    for ref, label in (
        (module.dsn_ref, "dsn expression"),
        (module.pool_options_ref, "pool options expression"),
    ):
        if ref is None:
            continue
        for spelling in ref.source_spellings:
            issues.extend(
                name_claim_issues(
                    label,
                    [
                        NameOrigin(
                            name=spelling,
                            origin=f"module expression {ref.module_expr!r}",
                            locations=all_locations,
                        )
                    ],
                )
            )
    for json_model in module.json_models:
        issues.extend(
            dotted_path_name_issues(
                "JSON model override module path",
                json_model.module_path,
                json_model.origin,
                all_locations,
            )
        )
        issues.extend(
            dotted_path_name_issues(
                "JSON model override class path",
                json_model.class_path,
                json_model.origin,
                all_locations,
            )
        )

    entity_claims, entity_issues = entity_name_analysis(module)
    enum_claims, enum_issues = enum_name_analysis(module)
    module_claims.extend(entity_claims)
    module_claims.extend(enum_claims)
    module_claims.extend(query_name_claims(module))
    issues.extend(entity_issues)
    issues.extend(enum_issues)

    issues.extend(
        name_claim_issues(f"module {module.module_full_name!r}", module_claims)
    )

    for query in module.queries:
        issues.extend(query_class_render_name_issues(query.render_spec))

    raise_generated_name_issues(issues)


def walk_symbol_tables(
    table: symtable.SymbolTable,
) -> Iterator[symtable.SymbolTable]:
    yield table
    for child in table.get_children():
        yield from walk_symbol_tables(child)


def expression_table_reads(table: symtable.SymbolTable) -> tuple[str, ...]:
    return tuple(
        symbol.get_name()
        for symbol in table.get_symbols()
        if symbol.is_referenced()
        and (symbol.is_global() or symbol.is_free())
        and not symbol.is_assigned()
        and not symbol.is_parameter()
    )


def module_expression_binding_spellings(
    expression: ast.Expression,
    source: str,
) -> Iterator[str]:
    def walk(node: ast.AST, *, nested_function: bool) -> Iterator[str]:
        match node:
            case ast.Lambda(args=args, body=body):
                for default in (*args.defaults, *args.kw_defaults):
                    if default is not None:
                        yield from walk(default, nested_function=nested_function)
                yield from walk(body, nested_function=True)
            case ast.NamedExpr(target=ast.Name()) if not nested_function:
                spelling = ast.get_source_segment(source, node.target)
                missing_spelling_msg = (
                    f"Cannot recover module expression binding from {source!r}"
                )
                if spelling is None:
                    raise AssertionError(missing_spelling_msg)
                yield spelling
                yield from walk(node.value, nested_function=nested_function)
            case _:
                for child in ast.iter_child_nodes(node):
                    yield from walk(child, nested_function=nested_function)

    yield from walk(expression.body, nested_function=False)


def expression_name_spellings(
    expression: ast.Expression,
    source: str,
    names: set[str],
) -> Iterator[str]:
    for node in ast.walk(expression):
        if not isinstance(node, ast.Name) or not isinstance(node.ctx, ast.Load):
            continue
        if node.id not in names:
            continue
        spelling = ast.get_source_segment(source, node)
        missing_spelling_msg = f"Cannot recover module expression name from {source!r}"
        if spelling is None:
            raise AssertionError(missing_spelling_msg)
        yield spelling


def render_module(
    module: GeneratedModuleSpec,
    entities: list[str],
    enums: list[str],
    enum_registry: list[tuple[str, str]],
    query_classes: list[str],
    query_overloads: list[str],
    query_dict_entries: list[str],
    application_name: str | None = None,
) -> str:
    dsn_ref = module.dsn_ref
    module_name = module.module_name
    sql_fn_name = module.sql_fn_name
    pool_options_ref = module.pool_options_ref
    pool_args = [
        dsn_ref.module_expr,
        f'name="{module_name}"',
        f"application_name={application_name!r}",
    ]
    if pool_options_ref is not None:
        pool_args.append(f"pool_options={pool_options_ref.module_expr}")

    imports_block = "\n".join(spec.source for spec in module.imports)

    pre_pool_blocks: list[str] = []
    if enums:
        pre_pool_blocks.append("\n\n\n".join(enums))
    if enum_registry:
        registry_entries = ",\n    ".join(
            f"({pg_name!r}, {class_name})" for pg_name, class_name in enum_registry
        )
        registry_type = (
            "builtins.list[builtins.tuple[builtins.str, builtins.type[StrEnum]]]"
        )
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
# pyright: reportUnusedParameter=false
# ruff: noqa

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
    channel: builtins.str,
) -> AsyncGenerator[AsyncGenerator[builtins.str]]:
    async with {module_name.upper()}_POOL.connection() as conn:
        async with runtime.listen(conn, channel) as payloads:
            yield payloads


async def {module_name}_notify(
    channel: builtins.str,
    payload: builtins.str = "",
) -> None:
    async with {module_name}_connection() as conn:
        await runtime.notify(conn, channel, payload)


{"\n\n\n".join(entities)}


class Query[T](runtime.Query[T]):
    _connection_factory = builtins.staticmethod({module_name}_connection)


{"\n\n\n".join(query_classes)}


_QUERIES: builtins.dict[builtins.str, builtins.type[Query[Any]]] = {{
    {(",\n    ").join(query_dict_entries)}
}}


{"\n".join(query_overloads)}
@overload
def {sql_fn_name}(sql: builtins.str) -> Query[Any]: ...


def {sql_fn_name}(
    sql: builtins.str,
    row_type: builtins.str | None = None,
) -> Query[Any]:
    if sql in _QUERIES:
        return _QUERIES[sql]()
    msg = f"Unknown statement: {{sql!r}}"
    raise builtins.KeyError(msg)

    """.strip()  # noqa: E501


def enum_class_name(
    enum_name: str,
    module_name: str,
    to_pascal_fn: Callable[[str], str],
    to_snake_fn: Callable[[str], str],
) -> str:
    return to_pascal_fn(f"{module_name}_{to_snake_fn(enum_name)}")


def prepare_enum_members(
    enum: Enum,
    to_snake_fn: Callable[[str], str],
) -> list[tuple[str, str]]:
    members: list[tuple[str, str]] = []
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
        members.append((name, val))

    return members


def render_enum_class(class_name: str, members: list[tuple[str, str]]) -> str:
    rendered_members = "\n".join(f'{name} = "{value}"' for name, value in members)

    return f"""

class {class_name}(StrEnum):
    {indent_block(rendered_members, "    ")}

    """.strip()


def render_entity(name: str, columns: tuple[ColumnSpec, ...]) -> str:
    fields = "\n    ".join(f"{c.name}: {c.py_type}" for c in columns)
    json_cols = [
        (column.name, column.json_model.expression)
        for column in columns
        if column.json_model is not None
    ]
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
    spec = build_query_class_render_spec(
        query_name,
        sql,
        query_params,
        result,
        result_columns,
        tuple(locations),
    )
    return render_query_class_spec(spec)


def build_query_class_render_spec(
    query_name: str,
    sql: str,
    query_params: list[ParamSpec],
    result: str,
    result_columns: tuple[ColumnSpec, ...],
    locations: tuple[str, ...],
) -> QueryClassRenderSpec:
    function_scopes = query_method_scope_specs(
        query_name,
        query_params,
        result_columns,
        locations,
    )
    params_arg, query_fn_signature = render_query_parameters(query_params)

    row_factory = render_row_factory(result, result_columns)
    parameter_type_expressions = tuple(param.py_type for param in query_params)

    function_names = tuple(scope.function_name for scope in function_scopes)
    unsupported_method_set_msg = (
        f"Unsupported generated query method set: {function_names!r}"
    )
    method_sources: tuple[str, ...]
    method_eager_expressions: tuple[tuple[str, ...], ...]
    if function_names == (
        "query_all_rows",
        "query_single_row",
        "query_optional_row",
        "query_stream",
    ):
        cursor_name = function_scopes[0].locals[0].name
        method_sources = (
            "\n".join((
                render_method_header(
                    "query_all_rows",
                    query_fn_signature,
                    f"builtins.list[{result}]",
                ),
                f"    async with self._client_cursor({params_arg}) as {cursor_name}:",
                f"        return await {cursor_name}.fetchall()",
            )),
            "\n".join((
                render_method_header("query_single_row", query_fn_signature, result),
                f"    async with self._client_cursor({params_arg}) as {cursor_name}:",
                f"        return runtime.get_one_row(await {cursor_name}.fetchmany(2))",
            )),
            "\n".join((
                render_method_header(
                    "query_optional_row",
                    query_fn_signature,
                    f"{result.removesuffix(' | None')} | None",
                ),
                f"    async with self._client_cursor({params_arg}) as {cursor_name}:",
                "        return {function}(await {cursor}.fetchmany(2))".format(
                    function="runtime.get_one_row_or_none",
                    cursor=cursor_name,
                ),
            )),
            "\n".join((
                render_method_header(
                    "query_stream",
                    query_fn_signature,
                    f"AbstractAsyncContextManager[AsyncIterator[{result}]]",
                    is_async=False,
                ),
                f"    return self._server_cursor({params_arg})",
            )),
        )
        method_eager_expressions = (
            (*parameter_type_expressions, f"builtins.list[{result}]"),
            (*parameter_type_expressions, result),
            (
                *parameter_type_expressions,
                f"{result.removesuffix(' | None')} | None",
            ),
            (
                *parameter_type_expressions,
                f"AbstractAsyncContextManager[AsyncIterator[{result}]]",
            ),
        )
    elif function_names == ("execute",):
        method_sources = (
            "\n".join((
                render_method_header("execute", query_fn_signature, "None"),
                f"    async with self._client_cursor({params_arg}):",
                "        pass",
            )),
        )
        method_eager_expressions = (parameter_type_expressions,)
    else:
        raise AssertionError(unsupported_method_set_msg)

    fixed_sources = (
        (
            "_locations",
            f"_locations = {locations!r}",
            "generated locations binding",
            (),
        ),
        (
            "_stmt",
            f"_stmt = psycopg.sql.SQL({sql!r})",
            "generated statement binding",
            ("psycopg",),
        ),
        (
            "_row_factory",
            f"_row_factory = builtins.staticmethod({row_factory})",
            "generated row factory binding",
            ("builtins", row_factory),
        ),
    )
    steps = [
        class_body_step_spec(
            source=source,
            binding=name,
            binding_origin=origin,
            locations=locations,
            eager_read_expressions=eager_read_expressions,
        )
        for name, source, origin, eager_read_expressions in fixed_sources
    ]
    steps.extend(
        class_body_step_spec(
            source=source,
            binding=scope.function_name,
            binding_origin=f"generated query method {scope.function_name}",
            locations=locations,
            eager_read_expressions=eager_read_expressions,
            function_scope=scope,
        )
        for source, scope, eager_read_expressions in zip(
            method_sources,
            function_scopes,
            method_eager_expressions,
            strict=True,
        )
    )
    return QueryClassRenderSpec(
        class_name=query_name,
        result_type=result,
        locations=locations,
        steps=tuple(steps),
    )


def render_query_parameters(query_params: list[ParamSpec]) -> tuple[str, str]:
    match query_params:
        case []:
            params_arg = "None"
        case [param]:
            params_arg = f"({param.serialized_expr},)"
        case params:
            params_arg = f"({', '.join(p.serialized_expr for p in params)})"

    signature_parts = [f"{param.name}: {param.py_type}" for param in query_params]
    first_named = next(
        (index for index, param in enumerate(query_params) if param.is_named), -1
    )
    if first_named >= 0:
        signature_parts.insert(first_named, "*")
    signature_parts.insert(0, "self")
    return params_arg, ", ".join(signature_parts)


def render_method_header(
    name: str,
    parameters: str,
    return_type: str,
    *,
    is_async: bool = True,
) -> str:
    prefix = "async def" if is_async else "def"
    return f"{prefix} {name}({parameters}) -> {return_type}:"


def class_body_step_spec(
    *,
    source: str,
    binding: str,
    binding_origin: str,
    locations: tuple[str, ...],
    eager_read_expressions: tuple[str, ...],
    function_scope: FunctionScopeSpec | None = None,
) -> ClassBodyStepSpec:
    return ClassBodyStepSpec(
        source=source,
        binding=NameOrigin(
            name=binding,
            origin=binding_origin,
            locations=locations,
        ),
        eager_reads=tuple(
            NameOrigin(
                name=name,
                origin=f"eager read while defining {binding!r}",
                locations=locations,
            )
            for name in dict.fromkeys(
                name
                for expression in eager_read_expressions
                for name in expression_root_names(expression)
            )
        ),
        function_scope=function_scope,
    )


def expression_root_names(source: str) -> Iterator[str]:
    previous_operator = ""
    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    for token in tokens:
        if token.type == tokenize.NAME:
            if previous_operator != "." and not keyword.iskeyword(token.string):
                yield token.string
            previous_operator = ""
        elif token.type == tokenize.OP:
            previous_operator = token.string


def render_query_class_spec(spec: QueryClassRenderSpec) -> str:
    body = "\n\n".join(step.source for step in spec.steps)
    return f"""

class {spec.class_name}(Query[{spec.result_type}]):
    {indent_block(body, "    ")}

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

    if column.json_model is not None:
        return (
            "runtime.typed_json_scalar_row("
            f"{column.json_model.expression}, not_null={not_null})"
        )

    if " | " in base_result:
        return f"runtime.typed_value_row(not_null={not_null})"

    return f"runtime.typed_scalar_row({base_result}, not_null={not_null})"


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
        return f"Query_{md5_hash}"

    @property
    def class_name(self) -> str:
        md5_hash = hashlib.md5(self.sql.encode(), usedforsecurity=False).hexdigest()
        return f"Query_{md5_hash}{'_' + self.row_type if self.row_type else ''}"

    @property
    def location(self) -> str:
        return f"{self.file}:{self.lineno}"


@dataclass(kw_only=True, frozen=True)
class SkippedDir:
    # A directory the walk refused to enter, with the reason it did.
    # The record distinguishes a tree left alone on purpose from a lost tree.
    location: str
    reason: str


@dataclass(kw_only=True, frozen=True)
class ScannedTree:
    files: tuple[Path, ...]
    skipped: tuple[SkippedDir, ...]


@dataclass(kw_only=True)
class DiscoveredQueries:
    queries: list[CodeQuery]
    query_locations_by_name: defaultdict[str, list[str]]
    skipped: list[SkippedDir]


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


def _skip_reason(directory: Path) -> str | None:
    # A hidden directory is a tool or VCS store, not source owned by the project.
    # A directory with pyvenv.cfg is an environment installation (PEP 405).
    # Check the name first so a hidden tree does not cause an unnecessary stat.
    if directory.name.startswith("."):
        return "hidden directory"
    if (directory / "pyvenv.cfg").is_file():
        return "virtual environment"
    return None


def walk_scanned_tree(src_path: Path) -> ScannedTree:
    # Path.walk permits directory pruning before the scan reads a refused tree.
    # With follow_symlinks=False, directory links arrive with the file names.
    # Record those links without entering their targets, and scan file links as files.
    #
    # Only child directories pass through _skip_reason. The scan root can have any
    # name because the caller explicitly selected it as the source root.
    files: list[Path] = []
    skipped: list[SkippedDir] = []
    for directory, subdirs, names in src_path.walk():
        kept: list[str] = []
        for name in subdirs:
            reason = _skip_reason(directory / name)
            if reason is None:
                kept.append(name)
            else:
                skipped.append(
                    SkippedDir(
                        location=str((directory / name).relative_to(src_path)),
                        reason=reason,
                    )
                )
        subdirs[:] = kept
        for name in names:
            path = directory / name
            if path.is_symlink() and path.is_dir():
                skipped.append(
                    SkippedDir(
                        location=str(path.relative_to(src_path)),
                        reason="symbolic link",
                    )
                )
            elif name.endswith(".py"):
                files.append(path)

    # Sort the full tree because a top-down walk yields root files before nested files.
    # Every generated location and diagnosis inherits this canonical order.
    return ScannedTree(
        files=tuple(sorted(files)),
        skipped=tuple(sorted(skipped, key=lambda entry: entry.location)),
    )


def find_fn_calls(
    files: tuple[Path, ...], fn_name: str
) -> Iterator[tuple[Path, int, ast.Call]]:
    for path in files:
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


def find_all_queries(
    src_path: Path, files: tuple[Path, ...], sql_fn_name: str
) -> Iterator[CodeQuery]:
    for file, lineno, node in find_fn_calls(files, sql_fn_name):
        relative_path = file.relative_to(src_path)

        if len(node.args) != 1:
            msg = (
                f"Invalid positional arguments for {sql_fn_name} "
                f"at {relative_path}:{lineno}, "
                "expected a single string literal"
            )
            raise TypeError(msg)
        sql_arg = node.args[0]
        if not isinstance(sql_arg, ast.Constant) or not isinstance(sql_arg.value, str):
            msg = (
                f"Invalid positional arguments for {sql_fn_name} "
                f"at {relative_path}:{lineno}, "
                "expected a single string literal"
            )
            raise TypeError(msg)

        sql = sql_arg.value

        row_type = None
        if len(node.keywords) > 1:
            msg = (
                f"Invalid keyword arguments for {sql_fn_name} "
                f"at {relative_path}:{lineno}, "
                "expected at most one row_type string literal"
            )
            raise TypeError(msg)
        for kw in node.keywords:
            if kw.arg != "row_type":
                argument_name = "**kwargs" if kw.arg is None else repr(kw.arg)
                msg = (
                    f"Invalid keyword argument {argument_name} for {sql_fn_name} "
                    f"at {relative_path}:{lineno}, expected row_type"
                )
                raise TypeError(msg)
            if not isinstance(kw.value, ast.Constant) or not isinstance(
                kw.value.value, str
            ):
                msg = (
                    f"Invalid keyword argument {kw.arg} for {sql_fn_name} "
                    f"at {relative_path}:{lineno}, expected a string literal"
                )
                raise TypeError(msg)
            row_type = kw.value.value

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
