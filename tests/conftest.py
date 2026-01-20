import importlib
import shutil
import sys
import textwrap
import uuid
from collections.abc import AsyncIterator
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from testcontainers.postgres import PostgresContainer

from iron_sql import generate_sql_package
from tests.sqlc_testcontainers import SqlcContainer

# =============================================================================
# PostgreSQL Container & Connection
# =============================================================================


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:17-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def pg_dsn(pg_container: PostgresContainer) -> str:
    return pg_container.get_connection_url(driver=None)


# =============================================================================
# Schema Management
# =============================================================================

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
def schema_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    temp_dir = tmp_path_factory.mktemp("data")
    path = temp_dir / "schema.sql"
    path.write_text(SCHEMA_SQL, encoding="utf-8")
    return path


@pytest.fixture(scope="session")
def pg_template_db(pg_dsn: str) -> str:
    template_name = "iron_sql_template"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(template_name))
        )
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(template_name)))

    base_dsn = pg_dsn.rsplit("/", 1)[0]
    template_dsn = f"{base_dsn}/{template_name}"

    with psycopg.connect(template_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
        cur.execute(SCHEMA_SQL)

    return template_name


@pytest.fixture
def pg_test_dsn(pg_dsn: str, pg_template_db: str) -> Iterator[str]:
    dbname = f"t_{uuid.uuid4().hex}"

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(dbname), sql.Identifier(pg_template_db)
            )
        )

    base_dsn = pg_dsn.rsplit("/", 1)[0]
    test_dsn = f"{base_dsn}/{dbname}"

    yield test_dsn

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(dbname)
            )
        )


# =============================================================================
# SQLC Code Generation
# =============================================================================


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


# =============================================================================
# Test Project Builder
# =============================================================================


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

    async def extend_schema(self, sql_str: LiteralString) -> None:
        with (self.src_path / "schema.sql").open("a", encoding="utf-8") as f:
            f.write("\n" + sql_str)

        async with (
            await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            await cur.execute(sql_str)

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

        importlib.invalidate_caches()
        sys.modules.pop(self.pkg_name, None)

        mod = importlib.import_module(self.pkg_name)
        self.generated_modules.append(mod)
        return mod


@pytest.fixture
async def test_project(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    pg_test_dsn: str,
    schema_path: Path,
    containerized_sqlc: SqlcContainer,
) -> AsyncIterator[ProjectBuilder]:
    clean_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    builder = ProjectBuilder(
        tmp_path, pg_test_dsn, clean_name, schema_path, containerized_sqlc
    )

    # Snapshot state before test
    before_modules = set(sys.modules)
    before_path = list(sys.path)

    yield builder

    # Teardown
    for module in builder.generated_modules:
        for name in dir(module):
            if name.endswith("_POOL"):
                pool = getattr(module, name)
                if hasattr(pool, "close"):
                    await pool.close()

    # Restore sys.path
    if sys.path != before_path:
        sys.path[:] = before_path

    # Clean up sys.modules
    new_modules = set(sys.modules) - before_modules
    for mod_name in new_modules:
        if mod_name.startswith("testapp_"):
            sys.modules.pop(mod_name, None)
