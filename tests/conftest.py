import asyncio
import importlib
import shutil
import sys
import textwrap
from collections.abc import AsyncIterator
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from iron_sql import generate_sql_package
from tests.sqlc_testcontainers import SqlcContainer

SCHEMA_SQL = """
    CREATE TYPE user_status AS ENUM ('active', 'inactive');

    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT,
        is_active BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        metadata JSONB
    );

    CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        user_id UUID NOT NULL REFERENCES users(id),
        title TEXT NOT NULL,
        content TEXT,
        published BOOLEAN NOT NULL DEFAULT false
    );

    CREATE TABLE IF NOT EXISTS json_payloads (
        id SERIAL PRIMARY KEY,
        payload JSON NOT NULL
    );

    CREATE TABLE IF NOT EXISTS jsonb_arrays (
        id SERIAL PRIMARY KEY,
        payloads JSONB[] NOT NULL
    );
"""


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    postgres = PostgresContainer("postgres:17-alpine")
    postgres.start()
    try:
        yield postgres
    finally:
        postgres.stop()


@pytest.fixture(scope="session")
def pg_dsn(pg_container: PostgresContainer) -> str:
    url = pg_container.get_connection_url(driver="psycopg")
    return url.replace("+psycopg", "")


@pytest.fixture(scope="session")
def schema_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    temp_dir = tmp_path_factory.mktemp("data")
    path = temp_dir / "schema.sql"
    path.write_text(SCHEMA_SQL, encoding="utf-8")
    return path


def _apply_schema(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
            cur.execute("GRANT ALL ON SCHEMA public TO public")
            cur.execute(SCHEMA_SQL)


def _cleanup_data(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE posts, users, json_payloads, jsonb_arrays "
                "RESTART IDENTITY CASCADE"
            )


@pytest.fixture(scope="session")
def _apply_schema_once(pg_dsn: str) -> None:  # pyright: ignore[reportUnusedFunction]
    _apply_schema(pg_dsn)


@pytest.fixture(scope="session")
def containerized_sqlc(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[SqlcContainer]:
    sqlc = SqlcContainer()
    sqlc.start(tmp_path_factory.getbasetemp())
    try:
        yield sqlc
    finally:
        sqlc.stop()


async def close_generated_pools(module: Any) -> None:
    for name in dir(module):
        if name.endswith("_POOL"):
            pool = getattr(module, name)
            if hasattr(pool, "close"):
                await pool.close()


class ProjectBuilder:
    def __init__(
        self,
        root: Path,
        dsn: str,
        test_name: str,
        schema_path: Path,
        sqlc: SqlcContainer,
    ):
        self.root = root
        self.dsn = dsn
        self.test_name = test_name
        self.schema_path = schema_path
        self._sqlc = sqlc
        self.pkg_name = f"testapp_{test_name}.testdb"
        self.src_path = root / "src"
        self.app_pkg = f"testapp_{test_name}"
        self.app_dir = self.src_path / self.app_pkg
        self.queries: list[tuple[str, str, dict[str, Any]]] = []
        self.generated_modules: list[Any] = []
        self.queries_source: str | None = None

        self.app_dir.mkdir(parents=True, exist_ok=True)
        (self.app_dir / "__init__.py").touch()

        schema_src = self.schema_path.absolute()
        schema_dest = self.src_path / "schema.sql"
        schema_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(schema_src, schema_dest)

    def set_queries_source(self, source: str) -> None:
        self.queries_source = textwrap.dedent(source)

    def add_query(self, name: str, sql: str, **kwargs: Any) -> None:
        self.queries.append((name, sql, kwargs))

    def generate_no_import(self) -> bool:
        (self.app_dir / "config.py").write_text(
            f'DSN = "{self.dsn}"\n', encoding="utf-8"
        )

        if self.queries_source is not None:
            (self.app_dir / "queries.py").write_text(
                self.queries_source, encoding="utf-8"
            )
        else:
            lines = ["from typing import Any"]
            lines.extend(["def testdb_sql(q: str, **kwargs: Any) -> Any: ...", ""])

            for name, sql, kwargs in self.queries:
                args = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
                call_args = f'"""{sql}"""'
                if args:
                    call_args += f", {args}"

                if name:
                    lines.append(f"{name} = testdb_sql({call_args})")
                else:
                    lines.append(f"testdb_sql({call_args})")

            (self.app_dir / "queries.py").write_text("\n".join(lines), encoding="utf-8")

        if str(self.src_path) not in sys.path:
            sys.path.insert(0, str(self.src_path))
        importlib.invalidate_caches()

        return generate_sql_package(
            schema_path=Path("schema.sql"),
            package_full_name=self.pkg_name,
            dsn_import=f"{self.app_pkg}.config:DSN",
            src_path=self.src_path,
            tempdir_path=self.src_path,
            sqlc_command=self._sqlc.sqlc_command(),
        )

    def generate(self) -> Any:
        self.generate_no_import()
        mod = importlib.import_module(self.pkg_name)
        self.generated_modules.append(mod)
        return mod


@pytest.fixture
async def test_project(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    pg_dsn: str,
    schema_path: Path,
    containerized_sqlc: SqlcContainer,
    _apply_schema_once: None,  # noqa: PT019
) -> AsyncIterator[ProjectBuilder]:
    clean_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    builder = ProjectBuilder(
        tmp_path, pg_dsn, clean_name, schema_path, containerized_sqlc
    )
    yield builder
    for mod in builder.generated_modules:
        await close_generated_pools(mod)
        sys.modules.pop(builder.pkg_name, None)
    await asyncio.to_thread(_cleanup_data, pg_dsn)
