from pathlib import Path

import pytest

from iron_sql.codegen.sqlc import run_sqlc


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
        assert result.queries == ()
        assert result.catalog.schemas == ()
    finally:
        schema_path.unlink()
