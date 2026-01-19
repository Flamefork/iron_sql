# iron_sql

iron_sql keeps SQL close to Python call sites while giving you typed, async query helpers. You write SQL once, keep it in version control, and get generated clients that match your schema without hand-written boilerplate.

## Why use it
- SQL-first workflow: write queries where they are used; no ORM layer to fight.
- Strong typing: generated dataclasses and method signatures flow through your IDE and type checker.
- Async-ready: built on `psycopg` with pooled connections and transaction helpers.
- Safe-by-default: helper methods enforce expected row counts instead of returning silent `None`.

## Quick start
1. Install `iron_sql`, `psycopg`, `psycopg-pool`, and `pydantic`.
2. Install [`sqlc` v2](https://docs.sqlc.dev/en/latest/overview/install.html) and ensure it is available in your PATH.
3. Add a Postgres schema dump, for example `db/mydatabase_schema.sql`.
4. Call `generate_sql_package(schema_path=..., package_full_name=..., dsn_import=...)` from a small script or task. The generator scans your code (defaults to current directory), runs `sqlc`, and writes a module such as `myapp/db/mydatabase.py`.

## Authoring queries
- Use the package helper for your DB, e.g. `mydatabase_sql("select ...")`. The SQL string must be a literal so the generator can find it.
- Named parameters:
  - Required: `@param`
  - Optional: `@param?` (expands to `sqlc.narg('param')`)
  - Positional placeholders (`$1`) stay as-is.
- Multi-column results can opt into a custom dataclass with `row_type="MyResult"`. Single-column queries return a scalar type; statements without results expose `execute()`.

## Using generated clients
- `*_sql("...")` returns a query object with methods derived from the result shape:
  - `execute()` when no rows are returned.
  - `query_all_rows()`, `query_single_row()`, `query_optional_row()` for result sets.
- `*_connection()` yields a pooled `psycopg.AsyncConnection`; `*_transaction()` wraps it in a transaction context.
- JSONB params are sent with `pgjson.Jsonb`; scalar row factories validate types and raise when they do not match.

## Adding another database package
Provide the schema file and DSN import string, then call `generate_sql_package()` with:
- `schema_path`: path to the schema SQL file (relative to `src_path`).
- `package_full_name`: target module, e.g. `myapp.db`.
- `dsn_import`: import path to a DSN string, e.g. `myapp.config:CONFIG.db_url.value`.
- `src_path`: optional base source path for scanning queries (defaults current directory).
- `sqlc_path`: optional path to the sqlc binary if not in PATH (e.g., `Path("/custom/bin/sqlc")`).
- `tempdir_path`: optional path for temporary file generation (useful for Docker mounts).
- Optional `application_name`, `debug_path`, and `to_pascal_fn` if you need naming overrides or want to keep `sqlc` inputs for inspection.
