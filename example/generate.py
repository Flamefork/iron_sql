import os
from pathlib import Path

import psycopg
from testcontainers.postgres import (  # pyright: ignore[reportMissingTypeStubs]
    PostgresContainer,
)

from iron_sql.codegen import generate_sql_module


def init_db(dsn: str, schema_path: Path):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(schema_path.read_text(encoding="utf-8"))  # pyright: ignore[reportCallIssue, reportArgumentType]


def generate_db_module(dsn: str, schema_path: Path, src_path: Path) -> bool:
    # For example.config:DSN
    os.environ["DATABASE_URL"] = dsn

    return generate_sql_module(
        schema_path=schema_path,
        module_full_name="db.mydb",
        dsn_expr="example.config:DSN",
        pool_options_expr="example.config:POOL_OPTIONS",
        src_path=src_path,
        json_model_overrides={
            "projects.settings": "example.models:ProjectSettings",
            "tasks.metadata": "example.models:TaskMetadata",
        },
    )


example_dir = Path(__file__).parent
schema_path = Path("schema.sql")
src_path = example_dir

if __name__ == "__main__":
    with PostgresContainer("postgres:17-alpine") as postgres:
        dsn = postgres.get_connection_url(driver=None)
        init_db(dsn, src_path / schema_path)
        changed = generate_db_module(dsn, schema_path, src_path)
        print("Updated SQL module:", changed)
