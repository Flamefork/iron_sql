import os
from pathlib import Path

import pytest

from iron_sql.sqlc import run_sqlc


def test_run_sqlc_exclusive_args(tmp_path: Path) -> None:
    schema = tmp_path / "schema.sql"
    schema.touch()
    with pytest.raises(
        ValueError, match="sqlc_command and sqlc_path are mutually exclusive"
    ):
        run_sqlc(
            schema_path=schema,
            queries=[("q", "SELECT 1")],
            dsn=None,
            sqlc_path=Path("/bin/sqlc"),
            sqlc_command=["docker", "run"],
        )


def test_run_sqlc_empty_command(tmp_path: Path) -> None:
    schema = tmp_path / "schema.sql"
    schema.touch()
    with pytest.raises(ValueError, match="sqlc_command must not be empty"):
        run_sqlc(
            schema_path=schema,
            queries=[("q", "SELECT 1")],
            dsn=None,
            sqlc_path=None,
            sqlc_command=[],
        )


def test_run_sqlc_not_found_in_path(tmp_path: Path) -> None:
    schema = tmp_path / "schema.sql"
    schema.touch()
    original_path = os.environ.get("PATH", "")
    os.environ["PATH"] = ""
    try:
        with pytest.raises(FileNotFoundError, match="sqlc not found in PATH"):
            run_sqlc(
                schema_path=schema,
                queries=[("q", "SELECT 1")],
                dsn=None,
                sqlc_path=None,
                sqlc_command=None,
            )
    finally:
        os.environ["PATH"] = original_path


def test_run_sqlc_explicit_path_not_exists(tmp_path: Path) -> None:
    schema = tmp_path / "schema.sql"
    schema.touch()
    with pytest.raises(FileNotFoundError, match="sqlc not found at /does/not/exist"):
        run_sqlc(
            schema_path=schema,
            queries=[("q", "SELECT 1")],
            dsn=None,
            sqlc_path=Path("/does/not/exist"),
            sqlc_command=None,
        )


def test_run_sqlc_missing_schema() -> None:
    with pytest.raises(ValueError, match="Schema file not found"):
        run_sqlc(
            schema_path=Path("nonexistent.sql"),
            queries=[],
            dsn="postgres://",
        )


def test_run_sqlc_no_queries() -> None:
    schema_path = Path("schema.sql")
    schema_path.touch()
    try:
        result = run_sqlc(
            schema_path=schema_path,
            queries=[],
            dsn="postgres://",
        )
        assert result.queries == []
        assert result.catalog.schemas == []
    finally:
        schema_path.unlink()
