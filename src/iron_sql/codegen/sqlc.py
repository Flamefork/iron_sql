import json
import re
import shutil
import subprocess  # noqa: S404
import tempfile
import textwrap
from collections.abc import Sequence
from pathlib import Path

import pydantic
import sqlc
from pydantic import ConfigDict


class CatalogReference(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)
    catalog: str
    schema_name: str = pydantic.Field(..., alias="schema")
    name: str


# Spellings of built-in types, folded onto the internal pg_catalog name. One sqlc run
# reports type names from two sources at once, depending on where the column sits in
# the query. Names taken from the catalog built by parsing the schema are internal
# pg_catalog ones (float8, varchar), except for the serial shorthands, which pass
# through verbatim and name no type at all. Names resolved against the live database
# are SQL standard spellings (double precision, integer).
#
# Only spellings observed in the output are listed: sqlc rewrites most of the standard
# ones back to pg_catalog names itself (character varying, timestamp with time zone),
# so entries for those would never match. test_analyzer_type_names_map_like_static_ones
# fails loudly if that ever stops being true.
_PG_TYPE_ALIASES: dict[str, str] = {
    "bigint": "int8",
    "bigserial": "int8",
    "boolean": "bool",
    "double precision": "float8",
    "integer": "int4",
    "real": "float4",
    "serial": "int4",
    "serial2": "int2",
    "serial4": "int4",
    "serial8": "int8",
    "smallint": "int2",
    "smallserial": "int2",
}


class Column(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    not_null: bool
    is_array: bool
    comment: str
    length: int
    is_named_param: bool
    is_func_call: bool
    scope: str
    table: CatalogReference | None
    table_alias: str
    type: CatalogReference
    is_sqlc_slice: bool
    embed_table: None
    original_name: str
    unsigned: bool
    array_dims: int

    @property
    def pg_type_name(self) -> str:
        return self.type.name.removeprefix("pg_catalog.").strip('"')

    @property
    def pg_builtin_type_name(self) -> str:
        name = self.type.name.removeprefix("pg_catalog.")
        # A quoted name is an identifier the database had to escape, never one of the
        # spellings PostgreSQL writes for a type of its own.
        if name.startswith('"'):
            return name.strip('"')
        return _PG_TYPE_ALIASES.get(name, name)


class Table(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    rel: CatalogReference
    columns: tuple[Column, ...]
    comment: str


class Enum(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    vals: tuple[str, ...]
    comment: str


class CompositeType(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    comment: str


class Schema(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    comment: str
    name: str
    tables: tuple[Table, ...]
    enums: tuple[Enum, ...]
    composite_types: tuple[CompositeType, ...]

    def has_enum(self, name: str) -> bool:
        return any(e.name == name for e in self.enums)

    def has_composite(self, name: str) -> bool:
        return any(c.name == name for c in self.composite_types)


class Catalog(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    default_schema: str
    name: str
    schemas: tuple[Schema, ...]

    def schema_by_name(self, name: str) -> Schema:
        missing_schema_msg = f"Schema not found: {name}"
        for schema in self.schemas:
            if schema.name == name:
                return schema
        raise AssertionError(missing_schema_msg)

    def schema_by_ref(self, ref: CatalogReference) -> Schema:
        return self.schema_by_name(ref.schema_name or self.default_schema)


class QueryParameter(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    number: int
    column: Column


class Query(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    text: str
    name: str
    cmd: str
    columns: tuple[Column, ...]
    params: tuple[QueryParameter, ...]


class SQLCResult(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    error: str | None = None
    catalog: Catalog
    queries: tuple[Query, ...]

    def used_schemas(self) -> tuple[str, ...]:
        result = {
            c.table.schema_name
            for q in self.queries
            for c in q.columns
            if c.table is not None
        }
        if "" in result:
            result.remove("")
            result.add(self.catalog.default_schema)
        catalog_schema_names = {s.name for s in self.catalog.schemas}
        return tuple(s for s in result if s in catalog_schema_names)


def run_sqlc(
    schema_path: Path,
    queries: Sequence[tuple[str, str]],
    *,
    dsn: str | None,
    debug_path: Path | None = None,
    tempdir_path: Path | None = None,
) -> tuple[SQLCResult, list[tuple[int, str]]]:
    if not schema_path.exists():
        msg = f"Schema file not found: {schema_path}"
        raise ValueError(msg)

    if not queries:
        return SQLCResult(
            catalog=Catalog(default_schema="", name="", schemas=()),
            queries=(),
        ), []

    queries = list({q[0]: q for q in queries}.values())

    with tempfile.TemporaryDirectory(
        dir=str(tempdir_path) if tempdir_path else None
    ) as tempdir:
        queries_path = Path(tempdir) / "queries.sql"
        block_starts: list[tuple[int, str]] = []
        blocks: list[str] = []
        current_line = 1
        for name, sql in queries:
            block = f"-- name: {name} :exec\n{preprocess_sql(sql)};"
            block_starts.append((current_line, name))
            current_line += block.count("\n") + 2
            blocks.append(block)
        queries_path.write_text("\n\n".join(blocks), encoding="utf-8")

        (Path(tempdir) / "schema.sql").symlink_to(schema_path.absolute())

        config_path = Path(tempdir) / "sqlc.json"
        sqlc_config = {
            "version": "2",
            "sql": [
                {
                    "schema": "schema.sql",
                    "queries": ["queries.sql"],
                    "engine": "postgresql",
                    "database": {"uri": dsn} if dsn else None,
                    "gen": {"json": {"out": ".", "filename": "out.json"}},
                }
            ],
        }
        config_path.write_text(json.dumps(sqlc_config, indent=2), encoding="utf-8")

        cmd = [sqlc.get_binary_path(), "generate", "--file", str(config_path.resolve())]

        sqlc_run_result = subprocess.run(  # noqa: S603
            cmd,
            capture_output=True,
            check=False,
        )

        json_out_path = Path(tempdir) / "out.json"

        if debug_path:
            debug_path.absolute().mkdir(parents=True, exist_ok=True)
            shutil.copy(queries_path, debug_path)
            shutil.copy(schema_path, debug_path / "schema.sql")
            shutil.copy(config_path, debug_path)
            if json_out_path.exists():
                shutil.copy(json_out_path, debug_path)
            elif (debug_path / "out.json").exists():
                (debug_path / "out.json").unlink()

        if not json_out_path.exists():
            return SQLCResult(
                error=sqlc_run_result.stderr.decode().strip(),
                catalog=Catalog(default_schema="", name="", schemas=()),
                queries=(),
            ), block_starts
        return SQLCResult.model_validate_json(
            json_out_path.read_text(encoding="utf-8")
        ), block_starts


def preprocess_sql(sql: str) -> str:
    sql = re.sub(r"@(\w+)\?", r"sqlc.narg('\1')", sql)
    return textwrap.dedent(sql).strip()
