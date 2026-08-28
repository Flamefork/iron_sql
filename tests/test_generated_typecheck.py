import textwrap
from pathlib import Path

from tests.conftest import SCHEMA_SQL
from tests.conftest import BasedPyrightReport
from tests.conftest import Diagnostic
from tests.conftest import basedpyright_report
from tests.conftest import generated_package

generated_package(
    "typing_contract",
    schema=SCHEMA_SQL,
    queries="""
        from tests.generated.typing_contract.testdb import testdb_sql

        testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
        testdb_sql("SELECT id FROM users ORDER BY created_at")
        testdb_sql("SELECT id, username FROM users WHERE id = $1")
        testdb_sql("SELECT 'active'::user_status as status")
        testdb_sql("SELECT email FROM users WHERE id = $1")
    """,
)

GENERATED = Path(__file__).parent / "generated"

CHECK_SOURCES = {
    "valid.py": """
        import uuid

        from tests.generated.json_users_metadata import testdb as json_api
        from tests.generated.typing_contract import testdb as api


        async def check(uid: uuid.UUID) -> None:
            executed = await api.testdb_sql(
                "INSERT INTO users (id, username) VALUES ($1, $2)"
            ).execute(uid, "name")
            reveal_type(executed)
            scalar = await api.testdb_sql(
                "SELECT id FROM users ORDER BY created_at"
            ).query_single_row()
            reveal_type(scalar)
            structured = await api.testdb_sql(
                "SELECT id, username FROM users WHERE id = $1"
            ).query_single_row(uid)
            reveal_type(structured)
            enum_value = await api.testdb_sql(
                "SELECT 'active'::user_status as status"
            ).query_single_row()
            reveal_type(enum_value)
            nullable = await api.testdb_sql(
                "SELECT email FROM users WHERE id = $1"
            ).query_single_row(uid)
            reveal_type(nullable)
            json_model = await json_api.testdb_sql(
                "SELECT metadata FROM users WHERE id = $1"
            ).query_single_row(uid)
            reveal_type(json_model)


        def dynamic(sql: str) -> None:
            reveal_type(api.testdb_sql(sql))
    """,
    "invalid.py": """
        import uuid

        from tests.generated.typing_contract import testdb as api


        async def check(uid: uuid.UUID) -> None:
            await api.testdb_sql(
                "INSERT INTO users (id, username) VALUES ($1, $2)"
            ).execute("not-a-uuid", "name")
            await api.testdb_sql(
                "SELECT id FROM users ORDER BY created_at"
            ).execute()
            await api.testdb_sql(
                "INSERT INTO users (id, username) VALUES ($1, $2)"
            ).query_single_row(uid, "name")
            api.testdb_sql(
                "SELECT id, username FROM users WHERE id = $1",
                row_type="Wrong",
            )
    """,
}


def diagnostics_for(report: BasedPyrightReport, check_file: Path) -> list[Diagnostic]:
    resolved = check_file.resolve()
    return [
        diagnostic
        for diagnostic in report.general_diagnostics
        if diagnostic.file == resolved
    ]


def test_generated_typing_contract(tmp_path: Path) -> None:
    check_paths: dict[str, Path] = {}
    for name, source in CHECK_SOURCES.items():
        path = tmp_path / name
        path.write_text(textwrap.dedent(source).lstrip("\n"), encoding="utf-8")
        check_paths[name] = path

    report = basedpyright_report(GENERATED, tmp_path)
    generated_errors = [
        diagnostic
        for diagnostic in report.general_diagnostics
        if diagnostic.severity == "error"
        and diagnostic.file.is_relative_to(GENERATED.resolve())
    ]
    assert generated_errors == [], "\n".join(
        f"{item.file}:{item.range.start.line + 1}: {item.message}"
        for item in generated_errors
    )
    generated_files = len(list(GENERATED.glob("*/*.py")))
    assert report.summary.files_analyzed >= generated_files + len(check_paths)

    valid = diagnostics_for(report, check_paths["valid.py"])
    assert [item for item in valid if item.severity == "error"] == []
    assert [item.message for item in valid if item.severity == "information"] == [
        'Type of "executed" is "None"',
        'Type of "scalar" is "UUID"',
        'Type of "structured" is "QueryResult_85b3f3336318688059f120dc7d00bb56"',
        'Type of "enum_value" is "TestdbUserStatus"',
        'Type of "nullable" is "str | None"',
        'Type of "json_model" is "UserMetadata | None"',
        'Type of "api.testdb_sql(sql)" is "Query[Any]"',
    ]

    invalid = [
        item
        for item in diagnostics_for(report, check_paths["invalid.py"])
        if item.severity == "error" and item.rule != "reportUnknownMemberType"
    ]
    assert [item.range.start.line for item in invalid] == [8, 11, 14, 15]
    assert [item.rule for item in invalid] == [
        "reportArgumentType",
        "reportAttributeAccessIssue",
        "reportAttributeAccessIssue",
        "reportCallIssue",
    ]
