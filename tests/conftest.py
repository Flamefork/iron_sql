import contextlib
import importlib
import json
import shutil
import subprocess
import sys
import textwrap
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any
from typing import LiteralString
from typing import cast

import psycopg
import pytest
from psycopg import sql
from pydantic import BaseModel
from pydantic import Field
from pydantic import TypeAdapter
from pydantic import ValidationError
from pydantic import alias_generators
from testcontainers.postgres import (  # pyright: ignore[reportMissingTypeStubs]
    PostgresContainer,
)

from iron_sql.codegen import generate_sql_module
from iron_sql.runtime import ConnectionPool
from tests.generated_oracles import assert_generated_module_contract


class DiagnosticPosition(BaseModel):
    line: int


class DiagnosticRange(BaseModel):
    start: DiagnosticPosition


class Diagnostic(BaseModel):
    file: Path
    severity: str
    message: str
    range: DiagnosticRange
    rule: str | None = None


class DiagnosticSummary(BaseModel):
    files_analyzed: int = Field(alias="filesAnalyzed")
    error_count: int = Field(alias="errorCount")
    warning_count: int = Field(alias="warningCount")


class BasedPyrightReport(BaseModel):
    general_diagnostics: list[Diagnostic] = Field(alias="generalDiagnostics")
    summary: DiagnosticSummary


def basedpyright_report(*check_paths: Path) -> BasedPyrightReport:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "basedpyright",
            "--outputjson",
            *(str(check_path) for check_path in check_paths),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=Path(__file__).parent.parent,
    )
    try:
        return TypeAdapter(BasedPyrightReport).validate_json(completed.stdout)
    except ValidationError as exc:
        msg = (
            f"basedpyright JSON does not match the expected schema "
            f"(exit code {completed.returncode}): {exc}\n"
            f"stdout={completed.stdout!r}\nstderr={completed.stderr!r}"
        )
        pytest.fail(msg)


@dataclass(kw_only=True, frozen=True)
class GeneratedPackage:
    name: str
    root: Path
    type_overrides: dict[str, str] | None
    json_model_overrides: dict[str, str] | None
    pool_options: dict[str, object] | None


_GENERATED_PACKAGES: dict[str, GeneratedPackage] = {}


def generated_package(
    name: str,
    *,
    schema: str,
    queries: str,
    type_overrides: dict[str, str] | None = None,
    json_model_overrides: dict[str, str] | None = None,
    pool_options: dict[str, object] | None = None,
) -> None:
    root = Path(__file__).parent / "generated" / name
    root.mkdir(parents=True, exist_ok=True)
    (root / "__init__.py").write_text("", encoding="utf-8")
    (root / "schema.sql").write_text(
        textwrap.dedent(schema).lstrip("\n"), encoding="utf-8"
    )
    settings = 'DSN = ""\n'
    if pool_options is not None:
        settings = (
            "from iron_sql import PoolOptions\n\n"
            + settings
            + f"POOL_OPTIONS: PoolOptions = {json.dumps(pool_options)}\n"
        )
    (root / "settings.py").write_text(settings, encoding="utf-8")
    (root / "queries.py").write_text(
        textwrap.dedent(queries).lstrip("\n"), encoding="utf-8"
    )
    _GENERATED_PACKAGES[name] = GeneratedPackage(
        name=name,
        root=root,
        type_overrides=type_overrides,
        json_model_overrides=json_model_overrides,
        pool_options=pool_options,
    )


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


@pytest.fixture(scope="session", autouse=True)
def regenerate_generated_packages(pg_dsn: str) -> Iterator[None]:
    database_name = "iron_sql_generated"
    base_dsn = pg_dsn.rsplit("/", 1)[0]
    generated_dsn = f"{base_dsn}/{database_name}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(database_name)
            )
        )
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))

    for package in _GENERATED_PACKAGES.values():
        with (
            psycopg.connect(generated_dsn, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
            cur.execute("CREATE SCHEMA public")
            cur.execute("GRANT ALL ON SCHEMA public TO public")
            schema = cast(
                "LiteralString",
                (package.root / "schema.sql").read_text(encoding="utf-8"),
            )
            cur.execute(schema)

        settings_path = package.root / "settings.py"
        settings = f"DSN = {generated_dsn!r}\n"
        if package.pool_options is not None:
            settings = (
                "from iron_sql import PoolOptions\n\n"
                + settings
                + f"POOL_OPTIONS: PoolOptions = {json.dumps(package.pool_options)}\n"
            )
        settings_path.write_text(settings, encoding="utf-8")
        importlib.invalidate_caches()
        settings_module_name = f"tests.generated.{package.name}.settings"
        settings_module = importlib.import_module(settings_module_name)
        importlib.reload(settings_module)
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name="testdb",
            dsn_expr=f"{settings_module_name}:DSN",
            pool_options_expr=(
                f"{settings_module_name}:POOL_OPTIONS"
                if package.pool_options is not None
                else None
            ),
            src_path=package.root,
            tempdir_path=package.root,
            type_overrides=package.type_overrides,
            json_model_overrides=package.json_model_overrides,
        )
        committed_settings = 'DSN = ""\n'
        if package.pool_options is not None:
            committed_settings = (
                "from iron_sql import PoolOptions\n\n"
                + committed_settings
                + f"POOL_OPTIONS: PoolOptions = {json.dumps(package.pool_options)}\n"
            )
        settings_path.write_text(committed_settings, encoding="utf-8")
        importlib.invalidate_caches()
        importlib.reload(settings_module)
        module = importlib.import_module(f"tests.generated.{package.name}.testdb")
        importlib.reload(module)
        queries_module = importlib.import_module(
            f"tests.generated.{package.name}.queries"
        )
        importlib.reload(queries_module)
        assert_generated_module_contract(module)

    try:
        yield
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )


type GeneratedTestDB = Callable[[str], contextlib.AbstractAsyncContextManager[None]]


@pytest.fixture
def generated_test_db(pg_dsn: str) -> GeneratedTestDB:
    @asynccontextmanager
    async def use(package_name: str) -> AsyncGenerator[None]:
        database_name = f"g_{uuid.uuid4().hex}"
        base_dsn = pg_dsn.rsplit("/", 1)[0]
        test_dsn = f"{base_dsn}/{database_name}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
            )

        package = _GENERATED_PACKAGES[package_name]
        async with (
            await psycopg.AsyncConnection.connect(test_dsn, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            schema = cast(
                "LiteralString",
                (package.root / "schema.sql").read_text(encoding="utf-8"),
            )
            await cur.execute(schema)

        module = importlib.import_module(f"tests.generated.{package_name}.testdb")
        namespace = cast("dict[str, object]", vars(module))
        pool_name = "TESTDB_POOL"
        original_pool = namespace[pool_name]
        if not isinstance(original_pool, ConnectionPool):
            msg = f"{module.__name__}.{pool_name} is not a ConnectionPool"
            raise TypeError(msg)
        replacement_pool = ConnectionPool(
            test_dsn,
            name=original_pool.name,
            application_name=original_pool.application_name,
            pool_options=original_pool.pool_options,
            enum_types=original_pool.enum_types,
        )
        namespace[pool_name] = replacement_pool
        try:
            yield
        finally:
            await replacement_pool.close()
            namespace[pool_name] = original_pool
            with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(database_name)
                    )
                )

    return use


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
        self.queries: list[tuple[str, str, dict[str, object]]] = []
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

    def add_query(self, name: str, sql: str, **kwargs: object) -> None:
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
        debug_path: Path | None = None,
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
            debug_path=debug_path,
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
        debug_path: Path | None = None,
    ) -> ModuleType:
        _, module = self.generate_checked(
            type_overrides=type_overrides,
            json_model_overrides=json_model_overrides,
            pool_options=pool_options,
            debug_path=debug_path,
        )
        return module

    def generate_checked(
        self,
        *,
        type_overrides: dict[str, str] | None = None,
        json_model_overrides: dict[str, str] | None = None,
        pool_options: dict[str, Any] | None = None,
        to_pascal_fn: Callable[[str], str] = alias_generators.to_pascal,
        debug_path: Path | None = None,
    ) -> tuple[bool, ModuleType]:
        changed = self.generate_no_import(
            type_overrides=type_overrides,
            json_model_overrides=json_model_overrides,
            pool_options=pool_options,
            to_pascal_fn=to_pascal_fn,
            debug_path=debug_path,
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
        namespace = cast("dict[str, object]", vars(module))
        for name, value in namespace.items():
            if name.endswith("_POOL") and isinstance(value, ConnectionPool):
                await value.close()

    # Restore sys.path
    if sys.path != before_path:
        sys.path[:] = before_path

    # Clean up sys.modules
    new_modules = set(sys.modules) - before_modules
    for mod_name in new_modules:
        if mod_name.startswith("testapp_"):
            sys.modules.pop(mod_name, None)
