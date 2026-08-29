import re
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
        import uuid
        from collections.abc import AsyncIterator
        from collections.abc import Sequence
        from contextlib import AbstractAsyncContextManager
        from typing import assert_type
        from typing import reveal_type

        from tests.generated.typing_contract import testdb as api
        from tests.generated.typing_contract.testdb import testdb_sql
        from tests.json_models import UserMetadata


        async def check(uid: uuid.UUID, metadata: UserMetadata) -> None:
            command = testdb_sql(
                "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
            )
            assert_type(await command.execute(uid, "name", metadata), None)

            scalar = testdb_sql("SELECT id FROM users ORDER BY created_at")
            assert_type(await scalar.query_all_rows(), list[uuid.UUID])
            assert_type(await scalar.query_single_row(), uuid.UUID)
            assert_type(await scalar.query_optional_row(), uuid.UUID | None)
            assert_type(
                scalar.query_stream(),
                AbstractAsyncContextManager[AsyncIterator[uuid.UUID]],
            )

            nullable_query = testdb_sql("SELECT email FROM users WHERE id = $1")
            assert_type(await nullable_query.query_single_row(uid), str | None)

            assert_type(
                await testdb_sql(
                    "SELECT id FROM users WHERE id = $1 AND username = @username"
                ).query_single_row(uid, username="name"),
                uuid.UUID,
            )

            enum_query = testdb_sql("SELECT $1::user_status as status")
            assert_type(
                await enum_query.query_single_row(api.TestdbUserStatus.ACTIVE),
                api.TestdbUserStatus,
            )

            array_query = testdb_sql("SELECT @values::int[] as values")
            assert_type(
                await array_query.query_single_row(values=[1, 2]),
                Sequence[int],
            )

            json_query = testdb_sql("SELECT metadata FROM users WHERE id = $1")
            assert_type(
                await json_query.query_single_row(uid),
                UserMetadata | None,
            )

            user_query = testdb_sql("SELECT * FROM users WHERE id = $1")
            user = await user_query.query_single_row(uid)
            assert_type(user, api.TestdbUser)
            assert_type(user.metadata, UserMetadata | None)

            anonymous = await testdb_sql(
                "SELECT id, is_active FROM users WHERE id = $1"
            ).query_single_row(uid)
            assert_type(anonymous.id, uuid.UUID)
            assert_type(anonymous.is_active, bool)
            reveal_type(anonymous)

            explicit = testdb_sql(
                "SELECT id, username FROM users",
                row_type="UserSummary",
            )
            assert_type(await explicit.query_single_row(), api.UserSummary)
    """,
    json_model_overrides={
        "users.metadata": "tests.json_models:UserMetadata",
    },
)

GENERATED = Path(__file__).parent / "generated"

CHECK_SOURCES = {
    "dynamic.py": """
        from typing import Any
        from typing import assert_type

        from tests.generated.typing_contract import testdb as api


        def check(sql: str) -> None:
            assert_type(api.testdb_sql(sql), api.Query[Any])
    """,
    "invalid.py": """
        import uuid

        from tests.generated.typing_contract import testdb as api


        async def check(uid: uuid.UUID) -> None:
            await api.testdb_sql(
                "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
            ).execute("not-a-uuid", "name", None)
            await api.testdb_sql(
                "SELECT id FROM users WHERE id = $1 AND username = @username"
            ).query_single_row(uid)
            await api.testdb_sql(
                "SELECT id FROM users ORDER BY created_at"
            ).execute()
            await api.testdb_sql(
                "INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)"
            ).query_single_row(uid, "name", None)
            api.testdb_sql(
                "SELECT id, username FROM users",
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

    dynamic = diagnostics_for(report, check_paths["dynamic.py"])
    assert [item for item in dynamic if item.severity == "error"] == []

    generated_queries = diagnostics_for(
        report,
        GENERATED / "typing_contract" / "queries.py",
    )
    information = [item for item in generated_queries if item.severity == "information"]
    assert len(information) == 1
    assert re.fullmatch(
        r'Type of "anonymous" is "QueryResult_[0-9a-f]{32}"',
        information[0].message,
    )

    invalid = [
        item
        for item in diagnostics_for(report, check_paths["invalid.py"])
        if item.severity == "error"
    ]
    assert [(item.range.start.line, item.rule) for item in invalid] == [
        (8, "reportArgumentType"),
        (9, "reportCallIssue"),
        (12, "reportUnknownMemberType"),
        (14, "reportAttributeAccessIssue"),
        (15, "reportUnknownMemberType"),
        (17, "reportAttributeAccessIssue"),
        (20, "reportArgumentType"),
    ]
    assert [
        item for item in report.general_diagnostics if item.severity == "error"
    ] == invalid
