from __future__ import annotations

import ast
import builtins
import inspect
import symtable
import sys
import uuid
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING
from typing import cast

import pytest
from pydantic import BaseModel
from pydantic import alias_generators

from iron_sql.codegen import generate_sql_module
from iron_sql.codegen.generator import ColumnSpec
from iron_sql.codegen.generator import JSONModelRef
from iron_sql.codegen.generator import ParamSpec
from iron_sql.codegen.generator import mangle_class_name
from iron_sql.codegen.generator import query_method_scope_specs
from iron_sql.codegen.generator import render_query_class
from tests.generated_oracles import assert_query_source_namespaces

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager
    from typing import Any

    import psycopg

    from tests.conftest import ProjectBuilder

_USER_METADATA_REF = JSONModelRef(
    module_path="tests",
    class_path="json_models.UserMetadata",
    origin="test JSON model",
)


def write_json_model_module(test_project: ProjectBuilder, root: str) -> None:
    package = test_project.src_path / root
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (package / "models.py").write_text(
        """from pydantic import BaseModel

class Payload(BaseModel):
    key: str
    value: str
""",
        encoding="utf-8",
    )


def test_parameter_keyword_and_receiver_collision(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("UPDATE users SET username = @class")
q2 = testdb_sql("UPDATE users SET username = @self")
"""
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert message.startswith("Invalid generated Python names:\n")
    assert "'class' is a Python keyword" in message
    assert "'self' is claimed more than once" in message
    assert "queries.py:4" in message
    assert "queries.py:5" in message


@pytest.mark.parametrize(
    ("sql", "overrides", "name", "method"),
    [
        (
            "SELECT id FROM users WHERE username = @runtime",
            None,
            "runtime",
            "query_single_row",
        ),
        (
            "UPDATE json_payloads SET payload = @psycopg",
            None,
            "psycopg",
            "execute",
        ),
    ],
)
def test_parameter_collides_only_with_external_names_read_by_method(
    test_project: ProjectBuilder,
    sql: str,
    overrides: dict[str, str] | None,
    name: str,
    method: str,
) -> None:
    test_project.add_query("q", sql)

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(json_model_overrides=overrides)

    message = str(exc_info.value)
    assert "method Query_" in message
    assert f".{method}: {name!r} is claimed more than once" in message
    assert "generated method body external read" in message
    assert "queries.py:4" in message


def test_json_module_root_cannot_replace_generated_module_binding(
    test_project: ProjectBuilder,
) -> None:
    write_json_model_module(test_project, "runtime")
    test_project.add_query("q", "UPDATE users SET metadata = $1")

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(
            json_model_overrides={"users.metadata": "runtime.models:Payload"}
        )

    message = str(exc_info.value)
    assert "module" in message
    assert "'runtime' is claimed more than once" in message
    assert "generated import runtime" in message
    assert "json_model_overrides['users.metadata']" in message


def test_json_module_root_cannot_be_shadowed_by_parameter(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "UPDATE users SET metadata = @tests")

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(
            json_model_overrides={"users.metadata": "tests.json_models:UserMetadata"}
        )

    message = str(exc_info.value)
    assert "method Query_" in message
    assert ".execute: 'tests' is claimed more than once" in message
    assert "generated method body external read" in message
    assert "queries.py:4" in message


def test_same_names_are_safe_in_independent_scopes(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("UPDATE users SET username = @runtime WHERE id = @item_id")
q2 = testdb_sql("SELECT id FROM users WHERE username = @psycopg")
q3 = testdb_sql("UPDATE users SET username = @tests WHERE id = @item_id")
q4 = testdb_sql("SELECT username AS self, email AS runtime, id AS psycopg FROM users")
"""
    )

    changed, _ = test_project.generate_checked()
    assert changed is True


def test_json_module_root_cannot_be_shadowed_by_cursor_local(
    test_project: ProjectBuilder,
) -> None:
    write_json_model_module(test_project, "cur")
    sql = "UPDATE users SET metadata = @payload WHERE id = @id RETURNING id"
    test_project.add_query("q", sql)

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(
            json_model_overrides={"users.metadata": "cur.models:Payload"}
        )

    message = str(exc_info.value)
    assert "method Query_" in message
    assert "'cur' is claimed more than once" in message
    assert "generated cursor local" in message
    assert "generated method body external read" in message


async def test_scalar_json_module_named_like_old_lambda_parameter(
    test_project: ProjectBuilder,
) -> None:
    write_json_model_module(test_project, "_v")
    sql = "SELECT metadata FROM users WHERE id = @id"
    test_project.add_query("q", sql)
    module = test_project.generate(
        json_model_overrides={"users.metadata": "_v.models:Payload"}
    )
    namespace = cast("dict[str, object]", vars(module))
    connection_factory = cast(
        "Callable[[], AbstractAsyncContextManager[psycopg.AsyncConnection[Any]]]",
        namespace["testdb_connection"],
    )
    testdb_sql = cast("Callable[..., object]", namespace["testdb_sql"])
    user_id = uuid.uuid4()
    async with connection_factory() as connection:
        await connection.execute(
            "INSERT INTO users (id, username, metadata) VALUES (%s, %s, %s)",
            (user_id, "lambda-root", '{"key": "k", "value": "v"}'),
        )

    query = testdb_sql(sql)
    query_single_row_name = "query_single_row"
    query_single_row = cast(
        "Callable[..., Awaitable[object]]", getattr(query, query_single_row_name)
    )
    value = await query_single_row(id=user_id)
    assert isinstance(value, BaseModel)
    fields = cast("dict[str, object]", vars(value))
    assert fields["key"] == "k"
    assert fields["value"] == "v"


def test_parameter_named_cur_remains_valid(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT id FROM users WHERE username = @cur")

    changed, _ = test_project.generate_checked()
    assert changed is True


def test_module_implicit_annotations_binding_is_reserved(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query(
        "q",
        "SELECT id, username FROM users",
        row_type="__annotations__",
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "'__annotations__' is claimed more than once" in message
    assert "implicit module binding" in message
    assert "queries.py:4" in message


def test_result_field_keyword_non_identifier_and_duplicate(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q = testdb_sql(
    '''SELECT username AS "class", email AS "bad-name",
    id AS duplicate, username AS duplicate FROM users''',
    row_type="BrokenRow",
)
"""
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "class 'BrokenRow': 'class' is a Python keyword" in message
    assert "class 'BrokenRow': 'bad-name' is not a valid Python identifier" in message
    assert "class 'BrokenRow': 'duplicate' is claimed more than once" in message
    assert "queries.py:4" in message


def test_row_type_is_validated_independently_of_shape_deduplication(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("SELECT * FROM users", row_type="bad-name")
q2 = testdb_sql("SELECT users.* FROM users", row_type="Query")
"""
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "module" in message
    assert "'bad-name' is not a valid Python identifier" in message
    assert "'Query' is claimed more than once" in message
    assert "row_type for 'SELECT * FROM users'" in message
    assert "row_type for 'SELECT users.* FROM users'" in message
    assert "queries.py:4" in message
    assert "queries.py:5" in message


def test_custom_class_names_validate_entities_and_enums(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("entity", "SELECT * FROM users")
    test_project.add_query("enum", "SELECT 'active'::user_status AS status")

    def custom_to_pascal(value: str) -> str:
        if value.endswith("_user"):
            return "bad-name"
        if value.endswith("_user_status"):
            return "Query"
        return alias_generators.to_pascal(value)

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(to_pascal_fn=custom_to_pascal)

    message = str(exc_info.value)
    assert "'bad-name' is not a valid Python identifier" in message
    assert "generated entity for table 'users'" in message
    assert "'Query' is claimed more than once" in message
    assert "generated enum for public.user_status" in message
    assert "generated query base class" in message
    assert "queries.py:4" in message


def test_all_name_errors_are_reported_without_creating_target(
    test_project: ProjectBuilder,
) -> None:
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q1 = testdb_sql("UPDATE users SET username = @class")
q2 = testdb_sql(
    '''SELECT username AS "bad-name", email AS duplicate,
    username AS duplicate FROM users''',
    row_type="Query",
)
"""
    )
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "'class' is a Python keyword" in message
    assert "'bad-name' is not a valid Python identifier" in message
    assert "'duplicate' is claimed more than once" in message
    assert "'Query' is claimed more than once" in message
    assert not target_path.exists()


def test_name_error_does_not_update_existing_target(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT id FROM users")
    changed, _ = test_project.generate_checked()
    assert changed is True
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    original = target_path.read_text(encoding="utf-8")
    test_project.set_queries_source(
        """\
from typing import Any
def testdb_sql(q: str, **kwargs: Any) -> Any: ...

q = testdb_sql(
    "SELECT id, username FROM users",
    row_type="_stmt",
)
"""
    )

    with pytest.raises(ValueError, match=r"^Invalid generated Python names:"):
        test_project.generate_no_import()

    assert target_path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE users SET username = @K WHERE email = @K",
        "UPDATE users SET username = @K",
        "UPDATE users SET username = @ｃｌａｓｓ",
        'SELECT username AS "K", email AS "K" FROM users',
        'SELECT username AS "K", email FROM users',
    ],
)
def test_nfkc_unstable_generated_names_are_rejected(
    test_project: ProjectBuilder,
    sql: str,
) -> None:
    test_project.add_query("q", sql)

    with pytest.raises(ValueError, match=r"^Invalid generated Python names:"):
        test_project.generate_no_import()


def test_nfkc_collision_reports_original_spellings(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", 'SELECT username AS "K", email AS "K" FROM users')

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "'K'" in message
    assert "'K'" in message
    assert "queries.py:4" in message


def test_nfkc_unstable_row_type_is_rejected(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query(
        "q",
        "SELECT id, username FROM users",
        row_type="K",
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "'K' is normalized by Python to 'K'" in message
    assert "row_type" in message
    assert "queries.py:4" in message


def test_nfkc_unstable_custom_class_names_are_rejected(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("entity", "SELECT * FROM users")
    test_project.add_query("enum", "SELECT 'active'::user_status AS status")

    def custom_to_pascal(value: str) -> str:
        if value.endswith("_user"):
            return "K"
        if value.endswith("_user_status"):
            return "Ｃｌａｓｓ"
        return alias_generators.to_pascal(value)

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(to_pascal_fn=custom_to_pascal)

    message = str(exc_info.value)
    assert "'K' is normalized by Python to 'K'" in message
    assert "'Ｃｌａｓｓ' normalizes to Python keyword 'Class'" not in message
    assert "'Ｃｌａｓｓ' is normalized by Python to 'Class'" in message
    assert "generated entity for table 'users'" in message
    assert "generated enum for public.user_status" in message


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE users SET username = @__value",
        'SELECT username AS "__value", email FROM users',
        'SELECT username AS "__value__", email FROM users',
        'SELECT username AS "__annotations__", email FROM users',
    ],
)
def test_class_scope_unsafe_names_are_rejected(
    test_project: ProjectBuilder,
    sql: str,
) -> None:
    test_project.add_query("q", sql)

    with pytest.raises(ValueError, match=r"^Invalid generated Python names:"):
        test_project.generate_no_import()


def test_non_mangled_dunder_parameter_is_allowed(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "UPDATE users SET username = @__value__")

    changed, _ = test_project.generate_checked()
    assert changed is True


@pytest.mark.parametrize(
    "class_name", ["GeneratedQuery", "_GeneratedQuery", "___", "K"]
)
def test_class_binding_matches_python_symtable(class_name: str) -> None:
    source = f"class {class_name}:\n    def method(self, __value): ...\n"
    module_table = symtable.symtable(source, "generated.py", "exec")
    (class_table,) = module_table.get_children()
    (method_table,) = class_table.get_children()
    assert isinstance(method_table, symtable.Function)

    assert method_table.get_parameters() == (
        "self",
        mangle_class_name(class_name, "__value"),
    )


def test_json_override_validates_every_dotted_path_component(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT 1")

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import(
            json_model_overrides={
                "users.metadata": "tests.bad-models:Payload",
                "json_payloads.payload": "tests.models:class",
            }
        )

    message = str(exc_info.value)
    assert "'bad-models' is not a valid Python identifier" in message
    assert "'class' is a Python keyword" in message
    assert "queries.py:4" in message


@pytest.mark.parametrize("expression_kind", ["dsn", "pool_options"])
def test_module_expression_validates_every_module_component(
    test_project: ProjectBuilder,
    expression_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_project.add_query("q", "SELECT 1")
    changed, _ = test_project.generate_checked()
    assert changed is True
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    original = target_path.read_text(encoding="utf-8")

    invalid_module = ModuleType("bad-model")
    vars(invalid_module)["DSN"] = test_project.dsn
    vars(invalid_module)["POOL_OPTIONS"] = {}
    monkeypatch.setitem(sys.modules, invalid_module.__name__, invalid_module)

    dsn_expr = (
        "bad-model:DSN"
        if expression_kind == "dsn"
        else f"{test_project.app_pkg}.config:DSN"
    )
    pool_options_expr = (
        "bad-model:POOL_OPTIONS" if expression_kind == "pool_options" else None
    )
    with pytest.raises(ValueError, match="'bad-model'"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=dsn_expr,
            pool_options_expr=pool_options_expr,
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )

    assert target_path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize("row_type", ["list", "dict", "staticmethod"])
def test_generated_class_may_use_scaffold_builtin_name(
    test_project: ProjectBuilder,
    row_type: str,
) -> None:
    test_project.add_query(
        "q",
        "SELECT id, username FROM users",
        row_type=row_type,
    )

    module = test_project.generate()

    generated_class = cast("object", vars(module)[row_type])
    assert inspect.isclass(generated_class)
    assert generated_class.__name__ == row_type


async def test_generated_enum_members_are_validated_after_rendering_names(
    test_project: ProjectBuilder,
) -> None:
    await test_project.extend_schema(
        "CREATE TYPE unicode_enum AS ENUM ('²', '2', '２');"
    )
    test_project.add_query("q", "SELECT '²'::unicode_enum AS value")
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "'NUM²' is not a valid Python identifier" in message
    assert "'NUM２' is normalized by Python to 'NUM2'" in message
    assert "'NUM2', 'NUM２' resolve to Python binding 'NUM2'" in message
    assert "queries.py:4" in message
    assert not target_path.exists()


def test_output_module_validates_every_dotted_path_component(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT 1")
    changed, _ = test_project.generate_checked()
    assert changed is True
    (test_project.app_dir / "queries.py").write_text(
        """from typing import Any
def class_sql(q: str, **kwargs: Any) -> Any: ...

q = class_sql("SELECT 1")
""",
        encoding="utf-8",
    )
    invalid_module_name = f"{test_project.app_pkg}.class"
    target_path = test_project.src_path / f"{invalid_module_name.replace('.', '/')}.py"

    with pytest.raises(
        ValueError, match=r"^Invalid generated Python names:"
    ) as exc_info:
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=invalid_module_name,
            dsn_expr=f"{test_project.app_pkg}.config:DSN",
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )

    message = str(exc_info.value)
    assert "output module path component 2: 'class' is a Python keyword" in message
    assert "queries.py:4" in message
    assert not target_path.exists()


def test_renderer_owned_builtin_reads_are_qualified(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query(
        "q",
        """SELECT id, username, metadata, is_active,
        'active'::user_status AS status, 1::int4 AS count,
        1.0::float8 AS ratio, decode('', 'hex') AS payload
        FROM users""",
    )
    changed, _ = test_project.generate_checked()
    assert changed is True
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    tree = ast.parse(target_path.read_text(encoding="utf-8"))

    builtin_reads = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
        and isinstance(node.ctx, ast.Load)
        and hasattr(builtins, node.id)
    }

    assert builtin_reads == set()


@pytest.mark.parametrize(
    ("params", "result", "columns"),
    [
        ([], "None", ()),
        (
            [
                ParamSpec(
                    name="value",
                    py_type="str",
                    is_named=False,
                    db_type="text",
                    not_null=True,
                    is_array=False,
                )
            ],
            "ResultRow",
            (
                ColumnSpec(name="left", table="items", py_type="str"),
                ColumnSpec(name="right", table="items", py_type="str"),
            ),
        ),
        (
            [
                ParamSpec(
                    name="payload",
                    py_type="tests.json_models.UserMetadata",
                    is_named=False,
                    db_type="jsonb",
                    not_null=True,
                    is_array=False,
                    json_model=_USER_METADATA_REF,
                )
            ],
            "ResultRow",
            (
                ColumnSpec(name="left", table="items", py_type="str"),
                ColumnSpec(name="right", table="items", py_type="str"),
            ),
        ),
    ],
)
def test_query_method_external_read_claims_match_symtable(
    params: list[ParamSpec],
    result: str,
    columns: tuple[ColumnSpec, ...],
) -> None:
    scopes = query_method_scope_specs(
        "GeneratedQuery",
        params,
        columns,
        ("queries.py:4",),
    )
    source = render_query_class(
        "GeneratedQuery",
        "SELECT 1",
        params,
        result,
        columns,
        ["queries.py:4"],
    )
    module_table = symtable.symtable(source, Path("generated.py").as_posix(), "exec")
    (class_table,) = module_table.get_children()

    scopes_by_name = {scope.function_name: scope for scope in scopes}
    for method_table in class_table.get_children():
        assert isinstance(method_table, symtable.Function)
        scope = scopes_by_name[method_table.get_name()]
        expected_parameters = tuple(item.name for item in scope.parameters)
        assert method_table.get_parameters() == expected_parameters
        assert set(method_table.get_locals()) == {
            *expected_parameters,
            *(item.name for item in scope.locals),
        }
        actual = {
            symbol.get_name()
            for symbol in method_table.get_symbols()
            if symbol.is_referenced() and (symbol.is_global() or symbol.is_free())
        }
        expected = {item.name for item in scope.external_reads}
        assert actual == expected
        assert all(
            child.get_name() != "lambda" for child in method_table.get_children()
        )


def test_module_expression_nested_lambda_is_user_owned(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f"""DSN = "{test_project.dsn}"

def choose(factory: object) -> str:
    return factory()
""",
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    changed = generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:choose(lambda: DSN)",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )
    test_project.import_generated()

    assert changed is True


@pytest.mark.parametrize(
    "row_type",
    [
        "_locations",
        "_stmt",
        "_row_factory",
        "query_all_rows",
        "query_single_row",
        "query_optional_row",
    ],
)
def test_earlier_query_class_binding_cannot_shadow_result_type(
    test_project: ProjectBuilder,
    row_type: str,
) -> None:
    test_project.add_query(
        "q",
        "SELECT id, username FROM users",
        row_type=row_type,
    )
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )

    with pytest.raises(ValueError, match=f"class .*{row_type!r}") as exc_info:
        test_project.generate_no_import()

    message = str(exc_info.value)
    assert "origins: generated" in message
    assert "eager read while defining" in message
    assert "queries.py:4" in message
    assert not target_path.exists()


def test_last_query_stream_binding_can_match_result_type(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query(
        "q",
        "SELECT id, username FROM users",
        row_type="query_stream",
    )

    module = test_project.generate()

    query_stream = cast("object", vars(module)["query_stream"])
    assert inspect.isclass(query_stream)
    assert query_stream.__name__ == "query_stream"


def test_module_expression_walrus_binding_conflict_is_rejected_before_write(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    (test_project.app_dir / "config.py").write_text(
        f'DSN = "{test_project.dsn}"\n',
        encoding="utf-8",
    )
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))
    target_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )

    with pytest.raises(ValueError, match="TESTDB_POOL"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=(f"{test_project.app_pkg}.config:(TESTDB_POOL := DSN)"),
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )

    assert not target_path.exists()


@pytest.mark.parametrize(
    ("method_body", "message"),
    [
        ("extra = 1", "locals"),
        ("return decimal", "reads unclaimed decimal"),
        ("return (lambda: None)()", "unknown nested scopes lambda"),
    ],
)
def test_generated_namespace_oracle_rejects_renderer_drift(
    method_body: str,
    message: str,
) -> None:
    source = f"""class Query_Test:
    _locations = ()
    _stmt = None
    _row_factory = None

    async def execute(self):
        {method_body}
"""

    with pytest.raises(AssertionError, match=message):
        assert_query_source_namespaces(source, ("Query_Test",))


def test_module_expressions_preserve_nested_user_scopes(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f"""D = ["{test_project.dsn}"]
P = [{{"min_size": 1}}]

def c(factory):
    return factory()

def a(function, value):
    return function(value)

def f(values):
    return values[0]
""",
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))
    dsn_expression = "c(lambda: a(lambda Query: Query, f([runtime for runtime in D])))"
    pool_expression = "c(lambda: a(lambda Query: Query, f([psycopg for psycopg in P])))"

    changed = generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:{dsn_expression}",
        pool_options_expr=f"{test_project.app_pkg}.config:{pool_expression}",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )
    test_project.import_generated()
    generated = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    ).read_text(encoding="utf-8")

    assert changed is True
    assert dsn_expression in generated
    assert pool_expression in generated
    assert f"from {test_project.app_pkg}.config import Query" not in generated
    assert f"from {test_project.app_pkg}.config import runtime" not in generated
    assert f"from {test_project.app_pkg}.config import psycopg" not in generated


def test_safe_module_expression_walrus_binding_is_emitted(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f'DSN = "{test_project.dsn}"\n',
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    changed = generate_sql_module(
        schema_path=Path("schema.sql"),
        module_full_name=test_project.module_full_name,
        dsn_expr=f"{test_project.app_pkg}.config:(selected_dsn := DSN)",
        src_path=test_project.src_path,
        tempdir_path=test_project.src_path,
    )
    module = test_project.import_generated()

    assert changed is True
    assert vars(module)["selected_dsn"] == test_project.dsn


def test_module_expression_binding_conflicts_with_second_expression(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f'DSN = "{test_project.dsn}"\nPOOL_OPTIONS = {{}}\n',
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    with pytest.raises(ValueError, match="selected"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=f"{test_project.app_pkg}.config:(selected := DSN)",
            pool_options_expr=(
                f"{test_project.app_pkg}.config:(selected := POOL_OPTIONS)"
            ),
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )


def test_module_expression_binding_reports_original_nfkc_spelling(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f'DSN = "{test_project.dsn}"\n',
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    with pytest.raises(ValueError, match="'K' is normalized by Python to 'K'"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=f"{test_project.app_pkg}.config:(K := DSN)",
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )


def test_module_expression_read_reports_original_nfkc_spelling(
    test_project: ProjectBuilder,
) -> None:
    (test_project.app_dir / "config.py").write_text(
        f'K = "{test_project.dsn}"\n',
        encoding="utf-8",
    )
    test_project.add_query("q", "SELECT 1")
    test_project.write_queries()
    if str(test_project.src_path) not in sys.path:
        sys.path.insert(0, str(test_project.src_path))

    with pytest.raises(ValueError, match="'K' is normalized by Python to 'K'"):
        generate_sql_module(
            schema_path=Path("schema.sql"),
            module_full_name=test_project.module_full_name,
            dsn_expr=f"{test_project.app_pkg}.config:K",
            src_path=test_project.src_path,
            tempdir_path=test_project.src_path,
        )
