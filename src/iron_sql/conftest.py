import importlib
import shutil
import sys
from collections.abc import AsyncIterator
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from iron_sql import generate_sql_package
from iron_sql.testing import SqlcShim

SCHEMA_SQL = """
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


def _reset_db(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
            cur.execute("GRANT ALL ON SCHEMA public TO public")
            cur.execute(SCHEMA_SQL)


@pytest.fixture(scope="session")
def _apply_schema_once(pg_dsn: str) -> None:  # pyright: ignore[reportUnusedFunction]
    _reset_db(pg_dsn)


@pytest.fixture(scope="session")
def containerized_sqlc(tmp_path_factory: pytest.TempPathFactory) -> SqlcShim:
    return SqlcShim(tmp_path_factory.mktemp("sqlc_bin"))


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
        sqlc: SqlcShim,
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
        self.queries: list[tuple[str, str]] = []
        self.generated_modules: list[Any] = []

        self.app_dir.mkdir(parents=True, exist_ok=True)
        (self.app_dir / "__init__.py").touch()

        schema_src = self.schema_path.absolute()
        schema_dest = self.src_path / "schema.sql"
        schema_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(schema_src, schema_dest)

    def add_query(self, name: str, sql: str) -> None:
        self.queries.append((name, sql))

    def generate(self) -> Any:
        (self.app_dir / "config.py").write_text(
            f'DSN = "{self.dsn}"\n', encoding="utf-8"
        )

        lines = ["from typing import Any"]
        lines.extend(["def testdb_sql(q: str, **kwargs: Any) -> Any: ...", ""])

        for name, sql in self.queries:
            if name:
                lines.append(f'{name} = testdb_sql("""{sql}""")')
            else:
                lines.append(f'testdb_sql("""{sql}""")')

        (self.app_dir / "queries.py").write_text("\n".join(lines), encoding="utf-8")

        if str(self.src_path) not in sys.path:
            sys.path.insert(0, str(self.src_path))
        importlib.invalidate_caches()

        with self._sqlc.env_context(self.src_path):
            generate_sql_package(
                schema_path=Path("schema.sql"),
                package_full_name=self.pkg_name,
                dsn_import=f"{self.app_pkg}.config:DSN",
                src_path=self.src_path,
                sqlc_path=self._sqlc.path,
                tempdir_path=self.src_path,
            )

        mod = importlib.import_module(self.pkg_name)
        self.generated_modules.append(mod)
        return mod


@pytest.fixture
async def test_project(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    pg_dsn: str,
    schema_path: Path,
    containerized_sqlc: SqlcShim,
    _apply_schema_once: None,  # noqa: PT019
) -> AsyncIterator[ProjectBuilder]:
    clean_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    builder = ProjectBuilder(
        tmp_path, pg_dsn, clean_name, schema_path, containerized_sqlc
    )
    yield builder
    for mod in builder.generated_modules:
        await close_generated_pools(mod)
    _reset_db(pg_dsn)
