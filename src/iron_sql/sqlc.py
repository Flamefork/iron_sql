import json
import re
import shutil
import subprocess  # noqa: S404
import tempfile
import textwrap
from pathlib import Path

import pydantic


class CatalogReference(pydantic.BaseModel):
    catalog: str
    schema_name: str = pydantic.Field(..., alias="schema")
    name: str


class Column(pydantic.BaseModel):
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


class Table(pydantic.BaseModel):
    rel: CatalogReference
    columns: list[Column]
    comment: str


class Enum(pydantic.BaseModel):
    name: str
    vals: list[str]
    comment: str


class CompositeType(pydantic.BaseModel):
    name: str
    comment: str


class Schema(pydantic.BaseModel):
    comment: str
    name: str
    tables: list[Table]
    enums: list[Enum]
    composite_types: list[CompositeType]

    def has_enum(self, name: str) -> bool:
        return any(e.name == name for e in self.enums)


class Catalog(pydantic.BaseModel):
    default_schema: str
    name: str
    schemas: list[Schema]

    def schema_by_name(self, name: str) -> Schema:
        for schema in self.schemas:
            if schema.name == name:
                return schema
        msg = f"Schema not found: {name}"
        raise ValueError(msg)

    def schema_by_ref(self, ref: CatalogReference) -> Schema:
        return self.schema_by_name(ref.schema_name or self.default_schema)


class QueryParameter(pydantic.BaseModel):
    number: int
    column: Column


class Query(pydantic.BaseModel):
    text: str
    name: str
    cmd: str
    columns: list[Column]
    params: list[QueryParameter]


class SQLCResult(pydantic.BaseModel):
    error: str | None = None
    catalog: Catalog
    queries: list[Query]

    def used_schemas(self) -> list[str]:
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
        return [s for s in result if s in catalog_schema_names]


def _resolve_sqlc_command(
    sqlc_path: Path | None,
    sqlc_command: list[str] | None,
) -> list[str]:
    if sqlc_command is not None:
        if sqlc_path is not None:
            msg = "sqlc_command and sqlc_path are mutually exclusive"
            raise ValueError(msg)
        if not sqlc_command:
            msg = "sqlc_command must not be empty"
            raise ValueError(msg)
        return sqlc_command

    if sqlc_path is None:
        discovered_path = shutil.which("sqlc")
        if discovered_path is None:
            msg = "sqlc not found in PATH"
            raise FileNotFoundError(msg)
        sqlc_path = Path(discovered_path)
    if not sqlc_path.exists():
        msg = f"sqlc not found at {sqlc_path}"
        raise FileNotFoundError(msg)

    return [str(sqlc_path)]


def run_sqlc(
    schema_path: Path,
    queries: list[tuple[str, str]],
    *,
    dsn: str | None,
    debug_path: Path | None = None,
    sqlc_path: Path | None = None,
    tempdir_path: Path | None = None,
    sqlc_command: list[str] | None = None,
) -> SQLCResult:
    if not schema_path.exists():
        msg = f"Schema file not found: {schema_path}"
        raise ValueError(msg)

    if not queries:
        return SQLCResult(
            catalog=Catalog(default_schema="", name="", schemas=[]),
            queries=[],
        )

    queries = list({q[0]: q for q in queries}.values())
    cmd_prefix = _resolve_sqlc_command(sqlc_path, sqlc_command)

    with tempfile.TemporaryDirectory(
        dir=str(tempdir_path) if tempdir_path else None
    ) as tempdir:
        queries_path = Path(tempdir) / "queries.sql"
        queries_path.write_text(
            "\n\n".join(
                f"-- name: {name} :exec\n{preprocess_sql(stmt)};"
                for name, stmt in queries
            ),
            encoding="utf-8",
        )

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

        cmd = [*cmd_prefix, "generate", "--file", str(config_path.resolve())]

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
                catalog=Catalog(default_schema="", name="", schemas=[]),
                queries=[],
            )
        return SQLCResult.model_validate_json(json_out_path.read_text(encoding="utf-8"))


def preprocess_sql(stmt: str) -> str:
    stmt = re.sub(r"@(\w+)\?", r"sqlc.narg('\1')", stmt)
    return textwrap.dedent(stmt).strip()
