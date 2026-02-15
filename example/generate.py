import os
from pathlib import Path

import psycopg
from testcontainers.postgres import PostgresContainer

from iron_sql.codegen import generate_sql_package
from iron_sql_dev import SqlcContainer

example_dir = Path(__file__).parent

with PostgresContainer("postgres:17-alpine") as postgres:
    dsn = postgres.get_connection_url(driver=None)
    os.environ["DATABASE_URL"] = dsn

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((example_dir / "schema.sql").read_text())

    sqlc = SqlcContainer()
    sqlc.start(example_dir)
    try:
        generate_sql_package(
            schema_path=Path("schema.sql"),
            package_full_name="myapp.db.mydb",
            dsn_import="myapp.config:DSN",
            src_path=example_dir,
            sqlc_command=sqlc.sqlc_command(),
            tempdir_path=example_dir,
            json_model_overrides={
                "projects.settings": "myapp.models:ProjectSettings",
                "tasks.metadata": "myapp.models:TaskMetadata",
            },
        )
    finally:
        sqlc.stop()
