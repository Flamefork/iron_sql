# iron_sql

iron_sql keeps SQL close to Python call sites while giving you typed, async query helpers. You write SQL once, keep it in version control, and get generated clients that match your schema without hand-written boilerplate.

## Why use it
- SQL-first workflow: write queries where they are used; no ORM layer to fight.
- Strong typing: generated dataclasses and method signatures flow through your IDE and type checker.
- Async-ready: built on `psycopg` with pooled connections and transaction helpers.
- Safe-by-default: helper methods enforce expected row counts instead of returning silent `None`.

## Quick start
1. Install `iron_sql`, `psycopg`, `psycopg-pool`, `orjson`, and `pydantic`.
2. Install [`sqlc` v2](https://docs.sqlc.dev/en/latest/overview/install.html) and ensure `/usr/local/bin/sqlc` is in PATH.
3. Add a Postgres schema dump, for example `db/adept_schema.sql`.
4. Call `generate_sql_package(schema_path=..., package_full_name=..., dsn_import=...)` from a small script or task. The generator scans your code, runs `sqlc`, and writes a module such as `adept/db/adept.py`.

## Authoring queries
- Use the package helper for your DB, e.g. `adept_sql("select ...")`. The SQL string must be a literal so the generator can find it.
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
- `schema_path`: path to the schema SQL file.
- `package_full_name`: target module, e.g. `adept.db.analytics`.
- `dsn_import`: import path to a DSN string, e.g. `adept.config:CONFIG.analytics_db_url.value`.
- Optional `application_name`, `debug_path`, and `to_pascal_fn` if you need naming overrides or want to keep `sqlc` inputs for inspection.
