import importlib.resources
import logging
import re
import sys
from ast import parse
from pathlib import Path

import pytest

from iron_sql.codegen import generate_sql_module
from iron_sql.codegen.generator import ModuleExprRef
from iron_sql.codegen.generator import ParamSpec
from iron_sql.codegen.sqlc import run_sqlc
from tests.conftest import ProjectBuilder

_USER_METADATA_TYPE = "tests.json_models.UserMetadata"
_JSON_METADATA_EXPR = f"runtime.dump_json_value({_USER_METADATA_TYPE}, metadata)"
_TEXT_METADATA_EXPR = f"runtime.dump_json_text({_USER_METADATA_TYPE}, metadata)"
_NULLABLE_JSONB_METADATA_EXPR = (
    f"psycopg.types.json.Jsonb({_JSON_METADATA_EXPR}) if metadata is not None else None"
)


def test_source_locations(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("SELECT id FROM users")
q2 = testdb_sql("SELECT username FROM users")
q3 = testdb_sql("SELECT id FROM users")
"""
    )
    test_project.generate_no_import()

    generated = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    ).read_text()

    location_lines = [
        [int(x) for x in re.findall(r":(\d+)", locations)]
        for locations in re.findall(r"_locations = (.+)", generated)
    ]
    assert location_lines == [[4, 6], [5]]


def test_scanner_rejects_syntax_error(test_project: ProjectBuilder) -> None:
    test_project.set_queries_source(
        """
        testdb_sql(
        def broken(
        """
    )
    with pytest.raises(SyntaxError, match=r"Failed to parse .+queries\.py"):
        test_project.generate_no_import()


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

    with pytest.raises(
        ValueError, match=r"row_type conflict: .+:4 has 'UserMini', .+:5 has None"
    ):
        test_project.generate_no_import()


def test_same_stmt_same_row_type_is_allowed(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("SELECT id, username FROM users", row_type="UserMini")
q2 = testdb_sql("SELECT id, username FROM users", row_type="UserMini")
"""
    )

    assert test_project.generate_no_import() is True


def test_sqlc_failure_returns_false(test_project: ProjectBuilder) -> None:
    test_project.add_query("bad_query", "SELEC FROM users")
    assert test_project.generate_no_import() is False


def test_sqlc_error_maps_to_source_location(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("SELECT id FROM users")
q2 = testdb_sql("SELECT nonexistent_column FROM users")
q3 = testdb_sql("SELECT nonexistent_column FROM users")
"""
    )
    with caplog.at_level(logging.ERROR, logger="iron_sql.codegen.generator"):
        result = test_project.generate_no_import()

    assert result is False
    assert "queries.sql" not in caplog.text
    assert "queries.py:5" in caplog.text
    assert "queries.py:6" in caplog.text


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


@pytest.mark.parametrize(
    ("db_type", "expected"),
    [
        (
            "json",
            f"psycopg.types.json.Json({_JSON_METADATA_EXPR})",
        ),
        (
            "jsonb",
            f"psycopg.types.json.Jsonb({_JSON_METADATA_EXPR})",
        ),
        ("text", _TEXT_METADATA_EXPR),
        ("varchar", _TEXT_METADATA_EXPR),
    ],
)
def test_json_model_param_serialized_expr_uses_direct_db_type_mapping(
    db_type: str, expected: str
) -> None:
    assert (
        ParamSpec(
            name="metadata",
            py_type=_USER_METADATA_TYPE,
            is_named=False,
            db_type=db_type,
            not_null=True,
            is_array=False,
            json_type=_USER_METADATA_TYPE,
        ).serialized_expr
        == expected
    )


def test_nullable_json_model_param_serialized_expr_keeps_none() -> None:
    assert (
        ParamSpec(
            name="metadata",
            py_type="tests.json_models.UserMetadata | None",
            is_named=False,
            db_type="jsonb",
            not_null=False,
            is_array=False,
            json_type=_USER_METADATA_TYPE,
        ).serialized_expr
        == _NULLABLE_JSONB_METADATA_EXPR
    )


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


def test_module_expr_ref_parse_and_evaluate(test_project: ProjectBuilder) -> None:
    (test_project.app_dir / "config.py").write_text(
        f"""DSN = "{test_project.dsn}"

def get_dsn() -> str:
    return DSN
""",
        encoding="utf-8",
    )

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    expr_ref = ModuleExprRef.parse(f"{test_project.app_pkg}.config:get_dsn()")

    assert expr_ref.module_name == f"{test_project.app_pkg}.config"
    assert expr_ref.module_expr == "get_dsn()"
    assert expr_ref.import_name == "get_dsn"
    assert expr_ref.evaluate(expected_type=str) == test_project.dsn


def test_dsn_expr_with_function_call(test_project: ProjectBuilder) -> None:
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

    generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:CONFIG.get_dsn()",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert "CONFIG.get_dsn()" in generated


def test_dsn_expr_with_factory_call_generates_valid_python(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f"""DSN = "{test_project.dsn}"

def get_dsn() -> str:
    return DSN
""",
        encoding="utf-8",
    )

    test_project.add_query("q", "SELECT 1 as value")

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:get_dsn()",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert f"from {test_project.app_pkg}.config import get_dsn" in generated
    assert "get_dsn()" in generated
    parse(generated)


def test_pool_options_expr(test_project: ProjectBuilder) -> None:
    config = (
        f'DSN = "{test_project.dsn}"\nPOOL_OPTIONS = {{"min_size": 1, "max_size": 5}}\n'
    )
    (test_project.app_dir / "config.py").write_text(config, encoding="utf-8")

    test_project.add_query("q", "SELECT 1 as value")

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:DSN",
        pool_options_expr=f"{test_project.app_pkg}.config:POOL_OPTIONS",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert f"from {test_project.app_pkg}.config import POOL_OPTIONS" in generated
    assert "pool_options=POOL_OPTIONS" in generated


def test_pool_options_expr_factory_generates_valid_python(
    test_project: ProjectBuilder,
) -> None:
    config = f"""DSN = "{test_project.dsn}"

def get_pool_options() -> dict[str, object]:
    return {{"min_size": 1, "max_size": 5}}
"""
    (test_project.app_dir / "config.py").write_text(config, encoding="utf-8")

    test_project.add_query("q", "SELECT 1 as value")

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:DSN",
        pool_options_expr=f"{test_project.app_pkg}.config:get_pool_options()",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert f"from {test_project.app_pkg}.config import get_pool_options" in generated
    assert "pool_options=get_pool_options()" in generated
    parse(generated)


def test_pool_options_expr_invalid_fails_during_generation(
    test_project: ProjectBuilder,
) -> None:
    config = f'DSN = "{test_project.dsn}"\n'
    (test_project.app_dir / "config.py").write_text(config, encoding="utf-8")

    test_project.add_query("q", "SELECT 1 as value")

    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    with pytest.raises(NameError, match="MISSING_POOL_OPTIONS"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=f"{test_project.app_pkg}.config:DSN",
            pool_options_expr=f"{test_project.app_pkg}.config:MISSING_POOL_OPTIONS",
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    assert not generated_path.exists()


def test_pool_options_expr_not_set(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1 as value")
    test_project.generate_no_import()

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text()
    assert "**" not in generated


def test_package_is_marked_as_typed() -> None:
    assert importlib.resources.files("iron_sql").joinpath("py.typed").is_file()


def test_run_sqlc_missing_schema() -> None:
    with pytest.raises(ValueError, match="Schema file not found"):
        run_sqlc(
            schema_path=Path("nonexistent.sql"),
            queries=[],
            dsn="postgres://",
        )


def test_run_sqlc_no_queries(tmp_path: Path) -> None:
    schema_path = tmp_path / "schema.sql"
    schema_path.touch()
    result, block_starts = run_sqlc(
        schema_path=schema_path,
        queries=[],
        dsn="postgres://",
    )
    assert result.queries == ()
    assert result.catalog.schemas == ()
    assert block_starts == []
