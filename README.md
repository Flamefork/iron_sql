# iron_sql

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![main](https://github.com/Flamefork/iron_sql/actions/workflows/main.yml/badge.svg)](https://github.com/Flamefork/iron_sql/actions/workflows/main.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/iron-sql)](https://pypi.org/project/iron-sql/)


`iron_sql` is a typed SQL code generator and async runtime for PostgreSQL. Write SQL where you use it, run `generate_sql_package`, and get a module with typed dataclasses, query helpers, and pooled connections without hand-written boilerplate.

## Installation

```bash
pip install iron-sql             # runtime only (psycopg + psycopg-pool + pydantic)
pip install iron-sql[codegen]    # + inflection for code generation
```

The `sqlc` binary is bundled automatically via the `sqlc` Python package.

## Key Features
- **Query discovery.** `generate_sql_package` scans your codebase for calls like `<package>_sql("SELECT ...")`, runs `sqlc` for type analysis, and emits a typed module.
- **Strong typing.** Generated dataclasses and method signatures flow through your IDE and type checker.
- **Async runtime.** Built on `psycopg` v3 with pooled connections, context-based connection reuse, and transaction helpers.
- **Safe by default.** Helper methods enforce expected row counts instead of returning silent `None`.

## Package Layout
- `runtime.py` -- async `ConnectionPool`, row helpers (`get_one_row`, `typed_scalar_row`), JSON validation decorators.
- `codegen/generator.py` -- query discovery, type resolution, module rendering.
- `codegen/sqlc.py` -- wraps the `sqlc` CLI and models its JSON output.
- `codegen/util.py` -- shared codegen utilities (`indent_block`, `write_if_changed`).

## Getting Started
1. **Add a schema file.** A Postgres DDL dump, e.g. `db/schema.sql`.
2. **Write queries where they live.** Import the future helper and use SQL literals inline:
   ```python
   from myapp.db.mydb import mydb_sql

   user = await mydb_sql(
       "SELECT id, username, email, created_at FROM users WHERE id = @user_id"
   ).query_single_row(user_id=uid)
   ```
   Named parameters use `@param` (required) or `@param?` (optional, expands to `sqlc.narg`). Positional `$1` works too.
3. **Generate the client module.**
   ```python
   from pathlib import Path

   from iron_sql.codegen import generate_sql_package

   generate_sql_package(
       schema_path=Path("schema.sql"),
       package_full_name="myapp.db.mydb",
       dsn_import="myapp.config:DSN",
       src_path=Path("."),
   )
   ```
   This writes `myapp/db/mydb.py` containing:
   - a connection pool singleton,
   - `*_connection()` and `*_transaction()` context managers,
   - `*_listen_session(channel)` and `*_notify(channel, payload="")` helpers,
   - dataclasses for multi-column results (deduplicated by table),
   - `StrEnum` classes for PostgreSQL enums,
   - a query class per statement with typed methods,
   - overloads for the `*_sql()` helper so editors infer return types.

## Customization
- **Type overrides.** `type_overrides={"custom_type": "int"}` maps database type names to Python type strings.
- **JSON model overrides.** `json_model_overrides={"users.metadata": "myapp.models:UserMeta"}` adds Pydantic validation for JSON/JSONB columns.
- **Naming conventions.** Supply `to_pascal_fn` and `to_snake_fn` callables to control generated names.
- **DSN configuration.** `dsn_import` is written verbatim into the generated module; point it at a config variable, env var lookup, or function call.
- **Debug artifacts.** Pass `debug_path` to save sqlc inputs and outputs for inspection.

## Runtime Highlights
- `ConnectionPool` opens lazily and reopens after `close()`, with `ContextVar`-based connection reuse for nested contexts.
- `*_listen_session()` uses a dedicated pooled connection and doesn't reuse `ContextVar` transaction connections.
- `query_single_row()` raises `NoRowsError`; `query_optional_row()` returns `None`. Both raise `TooManyRowsError` on 2+ rows.
- JSONB params are sent with `pgjson.Jsonb`; JSON with `pgjson.Json`. Scalar row factories validate types at runtime.
- `json_validated` decorator applies Pydantic model validation to dataclass fields on construction.

## Example

The [`example/`](example/) directory contains a complete working setup: a PostgreSQL schema, generation script with testcontainers, and sample query definitions. See [`example/generate.py`](example/generate.py) for the codegen call and [`example/myapp/main.py`](example/myapp/main.py) for query usage.

## Validation and Troubleshooting
- Errors identify the file and line where the problematic statement lives.
- Unknown SQL types map to `object` and emit `UnknownSQLTypeWarning` (promotable to error with `warnings.filterwarnings`).
- Statements with the same SQL but conflicting `row_type` values are rejected at generation time.
