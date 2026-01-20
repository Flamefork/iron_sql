import pytest

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


def test_unsupported_param_types_json(test_project: ProjectBuilder) -> None:
    test_project.add_query(
        "bad_json", "INSERT INTO json_payloads (payload) VALUES ($1)"
    )
    with pytest.raises(TypeError, match="Unsupported column type: json"):
        test_project.generate_no_import()


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
