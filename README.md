# iron_sql

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![main](https://github.com/Flamefork/iron_sql/actions/workflows/main.yml/badge.svg)](https://github.com/Flamefork/iron_sql/actions/workflows/main.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/iron-sql)](https://pypi.org/project/iron-sql/)


`iron_sql` is a typed SQL code generator and async runtime for PostgreSQL. Write SQL where you use it, run `generate_sql_module`, and get a module with typed dataclasses, query helpers, and pooled connections without hand-written boilerplate.

## Installation

```bash
pip install iron-sql             # runtime only (psycopg + psycopg-pool + pydantic)
pip install iron-sql[codegen]    # + inflection for code generation
```

The `sqlc` binary is bundled automatically via the `sqlc` Python package.

## Key Features
- **Query discovery.** `generate_sql_module` scans your codebase for calls like `<module>_sql("SELECT ...")`, runs `sqlc` for type analysis, and emits a typed module.
- **Strong typing.** Generated dataclasses and method signatures flow through your IDE and type checker.
- **Async runtime.** Built on `psycopg` v3 with pooled connections, context-based connection reuse, and transaction helpers.
- **Streaming.** `query_stream()` uses server-side cursors for memory-efficient iteration over large result sets.
- **Safe by default.** Helper methods enforce expected row counts instead of returning silent `None`.

## Design Constraints

Every query is a static SQL literal at its call site. That one constraint is where the guarantees come from: `sqlc` can type the statement ahead of time, generated `Literal` overloads hand your editor the exact result type, and the SQL that runs is the SQL you read. Nothing assembles a statement at runtime, so every statement text is fixed at generation time and appears verbatim in the generated module, next to the call sites that use it. That makes a hot `pg_stat_statements` entry easy to trace back -- though the mapping is not one-to-one: PostgreSQL folds statements that differ only in literal constants into a single entry, and splits one statement across users, databases, and `search_path`.

Non-goals, by construction:
- **SQL fragment composition.** No shared `WHERE` snippets stitched together before execution.
- **Dynamic query assembly.** Conditional filters belong in the SQL itself (`sqlc.narg('status')::task_status IS NULL OR status = @status?`), not in Python string building.
- **Lazy relations and object graphs.** Nothing loads on attribute access; related rows come from a query you wrote.

A 1+N pattern is therefore never implicit: it is always a loop in your own code around a statement you can read. [`detect_sql_repeats()`](#detecting-accidental-1n) reports one when you write it by accident.

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

   from iron_sql.codegen import generate_sql_module

   generate_sql_module(
       schema_path=Path("schema.sql"),
       module_full_name="myapp.db.mydb",
       dsn_expr="myapp.config:DSN",
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
- **Type overrides.** `type_overrides={"float8": "decimal.Decimal"}` maps database type names to Python type strings. For built-in types the key is the PostgreSQL internal name (`float8`, `varchar`, `timestamptz`), not the SQL-standard spelling (`double precision`, `character varying`); for a user-defined enum, domain or extension type it is the type name as declared (`custom_int`, `citext`). A key that no query column or parameter uses raises `ValueError` listing the type names actually in use.
- **JSON model overrides.** `json_model_overrides={"users.metadata": "myapp.models:UserMeta"}` adds Pydantic validation for JSON/JSONB columns.
- **Naming conventions.** Supply `to_pascal_fn` and `to_snake_fn` callables to control generated names.
- **Connection settings.** `dsn_expr` and `pool_options_expr` are written verbatim into the generated module; point them at config variables, env var lookups, or function calls.
- **Session settings and timeouts.** `PoolOptions["kwargs"]` reaches every pooled connection, so libpq `options` carries server-side settings: `{"kwargs": {"options": "-c statement_timeout=5000 -c lock_timeout=1000"}}`. `statement_timeout` bounds a single statement, not a query method: a `query_stream()` that iterates for longer than the timeout is unaffected, because `DECLARE` and each `FETCH` count separately. Consider `idle_in_transaction_session_timeout` alongside them to bound transactions that stay open while the application is busy elsewhere.
- **Debug artifacts.** Pass `debug_path` to save sqlc inputs and outputs for inspection.

## Runtime Highlights
- `ConnectionPool` opens lazily and reopens after `close()`, with `ContextVar`-based connection reuse for nested contexts.
- `*_listen_session()` uses a dedicated pooled connection and doesn't reuse `ContextVar` transaction connections.
- `query_single_row()` raises `NoRowsError`; `query_optional_row()` returns `None`. Both raise `TooManyRowsError` on 2+ rows.
- `query_stream()` returns an async context manager yielding an `AsyncGenerator`; uses server-side cursors with automatic transaction management.
- JSONB params are sent with `psycopg.types.json.Jsonb`; JSON with `psycopg.types.json.Json`. Scalar row factories validate types at runtime.
- `json_validated` decorator applies Pydantic model validation to dataclass fields on construction.
- `detect_sql_repeats()` reports accidental 1+N loops; see [below](#detecting-accidental-1n).

## Detecting Accidental 1+N

`detect_sql_repeats()` watches for one statement executing many times in quick succession inside a single asyncio task -- the shape of a query sitting in a loop. Wrap the part of the process you want watched:

```python
from contextlib import asynccontextmanager

from iron_sql import detect_sql_repeats

@asynccontextmanager                       # e.g. an ASGI lifespan in development
async def lifespan(app):
    with detect_sql_repeats(executions=10, within_seconds=1.0):
        yield
```

```python
@pytest.fixture(autouse=True)              # or a fixture that fails the run in CI
def no_repeated_queries():
    with detect_sql_repeats(executions=5, within_seconds=10.0, strict=True):
        yield
```

`executions` is how many runs of the same statement already look like a loop; `within_seconds` is the sliding window they must fall into. Both default to a deliberately quiet `executions=10, within_seconds=1.0`. Tune them to your code rather than to these numbers: set `executions` just above the largest batch you legitimately run in a loop (inserting a handful of rows one statement at a time is a normal pattern this cannot tell apart), and set `within_seconds` to the rough duration of one logical operation -- an HTTP request, one worker job. A window that is too wide merges neighbouring operations into a false report; one that is too narrow misses a slow loop whose every query waits on the network.

- Each repeated statement is reported once per task, as a `logging` warning naming the call sites and the statement itself, collapsed to one line and truncated. `strict=True` raises `RepeatedQueryError` instead, which is what you want in CI.
- Counting is per asyncio task, so a hundred concurrent handlers running the same query once each never trip it -- only a loop inside one of them does.
- Detection is off unless a block is active, and the block cannot be nested: entering a second one raises `RuntimeError`.

## Example

The [`example/`](example/) directory contains a complete working setup: a PostgreSQL schema, generation script with testcontainers, and sample query definitions. See [`example/generate.py`](example/generate.py) for the codegen call and [`example/main.py`](example/main.py) for query usage.

## Validation and Troubleshooting
- Errors identify the file and line where the problematic statement lives.
- Unknown SQL types map to `object` and emit `UnknownSQLTypeWarning` (promotable to error with `warnings.filterwarnings`).
- Statements with the same SQL but conflicting `row_type` values are rejected at generation time.
- A user-defined type may reuse a standard SQL spelling of a built-in (`integer`, `real`). Enums and composite types are recognised as user-defined either way. A domain or extension type is recognised only where the column references it schema-qualified (`v public."integer"`), because an unqualified reference reaches iron_sql spelled exactly like the built-in; such a column resolves to the built-in, and `type_overrides` cannot name it.
