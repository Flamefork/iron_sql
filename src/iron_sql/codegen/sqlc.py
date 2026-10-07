import json
import re
import shutil
import string
import subprocess  # noqa: S404
import tempfile
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
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

    def used_tables(self) -> tuple[Table, ...]:
        used = {
            (c.table.schema_name or self.catalog.default_schema, c.table.name)
            for q in self.queries
            for c in q.columns
            if c.table is not None
        }
        return tuple(
            t
            for s in self.catalog.schemas
            for t in s.tables
            if (s.name, t.rel.name) in used
        )


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
            # The semicolon goes on its own line: a query ending in a line comment
            # would swallow it.
            block = f"-- name: {name} :exec\n{preprocess_sql(sql)}\n;"
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


# Character classes of the PostgreSQL lexer (src/backend/parser/scan.l). It works on
# bytes and takes every byte of a multibyte character as a letter, so any non-ASCII
# character counts. dolq_start is the same class as ident_start.
_IDENT_START = r"A-Za-z_\x80-\U0010ffff"
_IDENT_CONT = rf"{_IDENT_START}0-9$"
_DOLQ_CONT = rf"{_IDENT_START}0-9"
_SPACE = r" \t\n\r\f\v"

# Left to sqlc, `@name` is PostgreSQL's prefix operator `@` applied to whatever the
# grammar binds to it, and sqlc takes that whole operand as the parameter name:
# `@x::text::int4` comes out as broken SQL and `@a::int4 + @b::int4` as one parameter
# named "+aint4@bint4". A sqlc.arg() call has no such ambiguity.
# PostgreSQL reads a run of operator characters as one operator, so `@` and `?` mark
# a parameter only as operators of their own: `<@ARRAY[1]` and `@x?-1` hold the
# operators `<@` and `?-`. An `@` right after an identifier character is part of a
# name. The patterns see only the code span being searched: `/* c */@x` is a
# parameter.
_OPERATOR_CHARS = r"~!@#^&|`?+\-*/%<>="
_NAMED_PARAM = re.compile(
    rf"""
    (?<![{_IDENT_CONT}{_OPERATOR_CHARS}])@
    ([{_IDENT_START}][{_IDENT_CONT}]*)
    (\?(?![{_OPERATOR_CHARS}]))?
    """,
    re.VERBOSE,
)
# Operators that took in the `@` before a name or the `?` after one. Valid SQL when
# they are real operators, so they only explain a query that sqlc rejects.
_OPERATOR_BEFORE_NAME = re.compile(
    rf"(?<![{_OPERATOR_CHARS}])([{_OPERATOR_CHARS}]+@)([{_IDENT_START}][{_IDENT_CONT}]*)"
)
_OPERATOR_AFTER_NAME = re.compile(
    rf"""
    (?<![{_IDENT_CONT}{_OPERATOR_CHARS}])@
    ([{_IDENT_START}][{_IDENT_CONT}]*)
    (\?[{_OPERATOR_CHARS}]+)
    """,
    re.VERBOSE,
)
# Searched in code with unquoted names folded: the check catches accidental calls,
# not ones split by a comment or spelled with quoted names.
_HANDWRITTEN_PARAM = re.compile(
    rf"(?<![{_IDENT_CONT}])sqlc[{_SPACE}]*\.[{_SPACE}]*(?:arg|narg|slice)[{_SPACE}]*\("
)
_POSITIONAL_PARAM = re.compile(rf"(?<![{_IDENT_CONT}])\$[0-9]")
# PostgreSQL folds unquoted identifiers to lower case, ASCII letters only.
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)
# Text that is not query code: string literals, quoted identifiers, dollar-quoted
# strings and comments. An unterminated one runs to the end, where sqlc rejects it.
# An E prefix marks an escape string and `$` opens a dollar quote only when it
# starts a token, not inside a name. A dollar quote tag cannot contain `$`, and
# cannot start with a digit, which would be a positional parameter. A line comment
# ends at CR as well as at LF. An E'...' string continued by a literal on the next
# line is scanned as a standard string; a `\'` there ends it early and sqlc rejects
# the query.
_NON_CODE = re.compile(
    rf"""
    (?<![{_IDENT_CONT}])[eE]'(?:[^'\\]|''|\\.)*(?:'|\Z)
    | '(?:[^']|'')*(?:'|\Z)
    | "(?:[^"]|"")*(?:"|\Z)
    | --[^\n\r]*
    | (?<![{_IDENT_CONT}])\$(?:[{_IDENT_START}][{_DOLQ_CONT}]*)?\$
    | /\*
    """,
    re.VERBOSE | re.DOTALL,
)


def _skip_block_comment(sql: str, start: int) -> int:
    # Block comments nest in PostgreSQL.
    depth = 0
    for match in re.finditer(r"/\*|\*/", sql[start:]):
        depth += 1 if match[0] == "/*" else -1
        if depth == 0:
            return start + match.end()
    return len(sql)


def _code_spans(sql: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    code_start = pos = 0
    while (match := _NON_CODE.search(sql, pos)) is not None:
        spans.append((code_start, match.start()))
        if match[0] == "/*":
            end = _skip_block_comment(sql, match.start())
        elif match[0].startswith("$"):
            close = sql.find(match[0], match.end())
            end = len(sql) if close < 0 else close + len(match[0])
        else:
            end = match.end()
        code_start = pos = end
    spans.append((code_start, len(sql)))
    return spans


@dataclass(kw_only=True, frozen=True)
class NamedParam:
    name: str
    nullable: bool


def _named_param(match: re.Match[str]) -> NamedParam:
    return NamedParam(
        name=match[1].translate(_ASCII_LOWER), nullable=match[2] is not None
    )


def _render_named_param(match: re.Match[str]) -> str:
    param = _named_param(match)
    fn = "narg" if param.nullable else "arg"
    return f"sqlc.{fn}('{param.name}')"


def named_params(sql: str) -> tuple[NamedParam, ...]:
    return tuple(
        _named_param(match)
        for start, end in _code_spans(sql)
        for match in _NAMED_PARAM.finditer(sql[start:end])
    )


def has_handwritten_params(sql: str) -> bool:
    return any(
        _HANDWRITTEN_PARAM.search(sql[start:end].translate(_ASCII_LOWER))
        for start, end in _code_spans(sql)
    )


def has_positional_params(sql: str) -> bool:
    return any(
        _POSITIONAL_PARAM.search(sql[start:end]) for start, end in _code_spans(sql)
    )


def _operator_before_name_hint(match: re.Match[str]) -> str:
    operator, name = match[1], match[2]
    return (
        f"PostgreSQL reads `{operator}` in `{operator}{name}` as one operator. "
        f"If @{name} is a parameter, write `{operator[:-1]} @{name}`."
    )


def _operator_after_name_hint(match: re.Match[str]) -> str:
    name, operator = match[1], match[2]
    return (
        f"PostgreSQL reads `{operator}` in `@{name}{operator}` as one operator. "
        f"If `?` marks @{name} as optional, write `@{name}? {operator[1:]}`."
    )


def operator_hints(sql: str) -> list[str]:
    hints: list[tuple[int, str]] = []
    for start, end in _code_spans(sql):
        code = sql[start:end]
        hints.extend(
            (start + match.start(), _operator_before_name_hint(match))
            for match in _OPERATOR_BEFORE_NAME.finditer(code)
        )
        hints.extend(
            (start + match.start(), _operator_after_name_hint(match))
            for match in _OPERATOR_AFTER_NAME.finditer(code)
        )
    return list(dict.fromkeys(hint for _, hint in sorted(hints)))


def preprocess_sql(sql: str) -> str:
    parts: list[str] = []
    pos = 0
    for start, end in _code_spans(sql):
        code = _NAMED_PARAM.sub(_render_named_param, sql[start:end])
        parts.extend((sql[pos:start], code))
        pos = end
    return textwrap.dedent("".join(parts)).strip()
