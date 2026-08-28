import importlib
import shutil
import sys
import textwrap
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from pydantic import alias_generators
from testcontainers.postgres import (  # pyright: ignore[reportMissingTypeStubs]
    PostgresContainer,
)

from iron_sql.codegen import generate_sql_module
from iron_sql.runtime import ConnectionPool
from tests.generated_oracles import assert_generated_module_contract

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

    yield f"{base_dsn}/{dbname}"

    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(dbname)
            )
        )


# =============================================================================
# Test Project Builder
# =============================================================================


@pytest.fixture
async def pool(pg_dsn: str) -> AsyncGenerator[ConnectionPool]:
    p = ConnectionPool(pg_dsn, name="test_pool")
    yield p
    await p.close()


class ProjectBuilder:
    def __init__(
        self,
        root: Path,
        dsn: str,
        test_name: str,
        schema_path: Path,
    ) -> None:
        self.root = root
        self.dsn = dsn
        self.test_name = test_name
        self.schema_path = schema_path
        self.module_full_name = f"testapp_{test_name}.testdb"
        self.src_path = root / "src"
        self.app_pkg = f"testapp_{test_name}"
        self.app_dir = self.src_path / self.app_pkg
        self.queries: list[tuple[str, str, dict[str, Any]]] = []
        self.generated_modules: list[ModuleType] = []
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

    def write_queries(self) -> None:
        if self.queries_source is not None:
            (self.app_dir / "queries.py").write_text(
                self.queries_source, encoding="utf-8"
            )
            return

        lines = ["from typing import Any"]
        lines.extend(["def testdb_sql(q: str, **kwargs: Any) -> Any: ...", ""])
        for name, query_sql, kwargs in self.queries:
            args = ", ".join(f"{key}={value!r}" for key, value in kwargs.items())
            call_args = f'"""{query_sql}"""'
            if args:
                call_args += f", {args}"

            if name:
                lines.append(f"{name} = testdb_sql({call_args})")
            else:
                lines.append(f"testdb_sql({call_args})")
        (self.app_dir / "queries.py").write_text("\n".join(lines), encoding="utf-8")

    def generate_no_import(
        self,
        *,
        type_overrides: dict[str, str] | None = None,
        json_model_overrides: dict[str, str] | None = None,
        pool_options: dict[str, Any] | None = None,
        to_pascal_fn: Callable[[str], str] = alias_generators.to_pascal,
    ) -> bool:
        config_lines = [f'DSN = "{self.dsn}"']
        if pool_options is not None:
            config_lines.append(f"POOL_OPTIONS = {pool_options!r}")
        (self.app_dir / "config.py").write_text(
            "\n".join(config_lines) + "\n", encoding="utf-8"
        )

        self.write_queries()

        if str(self.src_path) not in sys.path:
            sys.path.insert(0, str(self.src_path))

        changed = generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=self.module_full_name,
            dsn_expr=f"{self.app_pkg}.config:DSN",
            pool_options_expr=(
                f"{self.app_pkg}.config:POOL_OPTIONS"
                if pool_options is not None
                else None
            ),
            src_path=self.src_path,
            tempdir_path=self.src_path,
            type_overrides=type_overrides,
            json_model_overrides=json_model_overrides,
            to_pascal_fn=to_pascal_fn,
        )
        target_path = self.src_path / f"{self.module_full_name.replace('.', '/')}.py"
        if target_path.exists():
            source = target_path.read_text(encoding="utf-8")
            compile(source, target_path.as_posix(), "exec")
        return changed

    def generate(
        self,
        *,
        type_overrides: dict[str, str] | None = None,
        json_model_overrides: dict[str, str] | None = None,
        pool_options: dict[str, Any] | None = None,
    ) -> ModuleType:
        _, module = self.generate_checked(
            type_overrides=type_overrides,
            json_model_overrides=json_model_overrides,
            pool_options=pool_options,
        )
        return module

    def generate_checked(
        self,
        *,
        type_overrides: dict[str, str] | None = None,
        json_model_overrides: dict[str, str] | None = None,
        pool_options: dict[str, Any] | None = None,
        to_pascal_fn: Callable[[str], str] = alias_generators.to_pascal,
    ) -> tuple[bool, ModuleType]:
        changed = self.generate_no_import(
            type_overrides=type_overrides,
            json_model_overrides=json_model_overrides,
            pool_options=pool_options,
            to_pascal_fn=to_pascal_fn,
        )

        return changed, self.import_generated()

    def import_generated(self) -> ModuleType:
        importlib.invalidate_caches()
        sys.modules.pop(self.module_full_name, None)

        module = importlib.import_module(self.module_full_name)
        assert_generated_module_contract(module)
        self.generated_modules.append(module)
        return module


@pytest.fixture
async def test_project(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    pg_test_dsn: str,
    schema_path: Path,
) -> AsyncGenerator[ProjectBuilder]:
    node_name = str(request.node.name)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    clean_name = "".join(
        char if char.isascii() and (char.isalnum() or char == "_") else "_"
        for char in node_name
    )
    builder = ProjectBuilder(tmp_path, pg_test_dsn, clean_name, schema_path)

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
