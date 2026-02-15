import sys
from pathlib import Path

import pytest

from iron_sql import generate_sql_package
from tests.conftest import ProjectBuilder


def test_scanner_rejects_non_literal_sql(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """
        from typing import Any
        def testdb_sql(q: str, **kwargs: Any) -> Any: ...

        SQL = "SELECT 1"
        q = testdb_sql(SQL)
        """
    )
    with pytest.raises(TypeError, match="expected a single string literal"):
        test_project.generate_no_import()


def test_scanner_rejects_non_literal_row_type(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """
        from typing import Any
        def testdb_sql(q: str, **kwargs: Any) -> Any: ...

        RT = "UserMini"
        q = testdb_sql("SELECT 1", row_type=RT)
        """
    )
    with pytest.raises(TypeError, match="expected a string literal"):
        test_project.generate_no_import()


def test_scanner_rejects_wrong_call_shape(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """
        from typing import Any
        def testdb_sql(q: str, **kwargs: Any) -> Any: ...

        testdb_sql("SELECT 1", "extra")
        """
    )
    with pytest.raises(TypeError, match="expected a single string literal"):
        test_project.generate_no_import()


def test_scanner_rejects_same_stmt_different_row_type(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("SELECT id, username FROM users", row_type="UserMini")
q2 = testdb_sql("SELECT id, username FROM users")
"""
    )

    with pytest.raises(ValueError, match=r"row_type conflict \(existing='UserMini'\)"):
        test_project.generate_no_import()


def test_sqlc_failure_returns_false(test_project: ProjectBuilder) -> None:
    test_project.add_query("bad_query", "SELEC FROM users")
    assert test_project.generate_no_import() is False


def test_result_shapes_validation_error_zero_cols(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "insert_bad", "INSERT INTO users (id, username) VALUES ($1, $2)", row_type="Bad"
    )
    with pytest.raises(ValueError, match="Query has row_type=Bad but no result"):
        test_project.generate_no_import()


def test_result_shapes_validation_error_one_col(test_project: ProjectBuilder) -> None:
    test_project.add_query("select_bad", "SELECT id FROM users", row_type="Bad2")
    with pytest.raises(ValueError, match="Query has row_type=Bad2 but only one column"):
        test_project.generate_no_import()


def test_json_param_generates_successfully(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "insert_json", "INSERT INTO json_payloads (payload) VALUES ($1)"
    )
    assert test_project.generate_no_import() is True


def test_unsupported_param_types_array(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "bad_jsonb_array", "INSERT INTO jsonb_arrays (payloads) VALUES ($1)"
    )
    with pytest.raises(TypeError, match=r"Unsupported column type: jsonb\[\]"):
        test_project.generate_no_import()


def test_generator_is_idempotent(test_project: ProjectBuilder) -> None:
    assert test_project.generate_no_import() is True
    assert test_project.generate_no_import() is False


def test_generator_valid_explicit_row_type(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """
        from typing import Any
        def testdb_sql(q: str, **kwargs: Any) -> Any: ...

        RT = "UserMini"
        q = testdb_sql("SELECT id, username FROM users", row_type="UserMini")
        """
    )
    assert test_project.generate_no_import() is True


async def test_special_types_params(test_project: ProjectBuilder) -> None:
    await test_project.extend_schema(
        """
        CREATE TABLE special_types (
            id uuid PRIMARY KEY,
            d date NOT NULL,
            t time NOT NULL,
            ts timestamp NOT NULL,
            b boolean NOT NULL,
            j jsonb
        );
        """
    )
    test_project.add_query(
        "insert_special",
        "INSERT INTO special_types (id, d, t, ts, b, j) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
    )
    assert test_project.generate_no_import() is True


def test_dsn_import_with_function_call(test_project: ProjectBuilder) -> None:
    (test_project.app_dir / "config.py").write_text(
        f"""
class Config:
    def __init__(self, dsn: str):
        self._dsn = dsn
    def get_dsn(self) -> str:
        return self._dsn

CONFIG = Config("{test_project.dsn}")
""",
        encoding="utf-8",
    )

    test_project.add_query("q", "SELECT 1 as value")

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    generate_sql_package(
        schema_path=Path("schema.sql"),
        package_full_name=test_project.pkg_name,
        dsn_import=f"{test_project.app_pkg}.config:CONFIG.get_dsn()",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
        sqlc_command=test_project.sqlc.sqlc_command(),
    )

    generated_path = (
        test_project.src_path / f"{test_project.pkg_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert "CONFIG.get_dsn()" in generated
