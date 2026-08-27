import asyncio
from typing import Any

import pytest

from iron_sql import RepeatedQueryError
from iron_sql import detect_sql_repeats
from tests.conftest import ProjectBuilder

_COUNT_SQL = "SELECT count(*) FROM users"
_STREAM_SQL = "SELECT id FROM users ORDER BY username"


def detector_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.message for r in caplog.records if r.name == "iron_sql.runtime"]


def generated_module(test_project: ProjectBuilder) -> Any:
    test_project.add_query("count", _COUNT_SQL)
    test_project.add_query("stream", _STREAM_SQL)
    return test_project.generate()


async def test_warns_on_query_repeated_in_a_single_task(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    with detect_sql_repeats(executions=3, within_seconds=10.0):
        for _ in range(5):
            await mod.testdb_sql(_COUNT_SQL).query_single_row()

    assert len(detector_warnings(caplog)) == 1
    message = detector_warnings(caplog)[0]
    assert _COUNT_SQL in message
    assert "queries.py:" in message
    assert "executed 3 times" in message
    assert "within 10.0s" in message


async def test_multiline_statements_stay_distinguishable(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    first_sql = "SELECT\n    users.id\nFROM users"
    second_sql = "SELECT\n    users.username\nFROM users"
    test_project.add_query("first", first_sql)
    test_project.add_query("second", second_sql)
    mod = test_project.generate()

    with detect_sql_repeats(executions=2, within_seconds=10.0):
        for _ in range(2):
            await mod.testdb_sql(first_sql).query_all_rows()
        for _ in range(2):
            await mod.testdb_sql(second_sql).query_all_rows()

    first_message, second_message = detector_warnings(caplog)
    assert "SELECT users.id FROM users" in first_message
    assert "SELECT users.username FROM users" in second_message


async def test_long_statements_are_truncated(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    columns = ", ".join(f"users.username AS name_{i}" for i in range(20))
    long_sql = f"SELECT {columns} FROM users"
    test_project.add_query("long", long_sql)
    mod = test_project.generate()

    with detect_sql_repeats(executions=2, within_seconds=10.0):
        for _ in range(2):
            await mod.testdb_sql(long_sql).query_all_rows()

    message = detector_warnings(caplog)[0]
    assert "SELECT users.username AS name_0," in message
    assert "name_19" not in message
    assert "..." in message


async def test_warns_on_repeated_stream(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    with detect_sql_repeats(executions=2, within_seconds=10.0):
        for _ in range(2):
            async with mod.testdb_sql(_STREAM_SQL).query_stream() as rows:
                async for _row in rows:
                    pass

    assert len(detector_warnings(caplog)) == 1
    assert _STREAM_SQL in detector_warnings(caplog)[0]


async def test_does_not_warn_across_concurrent_tasks(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    async def query_once() -> None:
        await mod.testdb_sql(_COUNT_SQL).query_single_row()

    with detect_sql_repeats(executions=2, within_seconds=10.0):
        await asyncio.gather(*(query_once() for _ in range(4)))

    assert detector_warnings(caplog) == []


async def test_strict_mode_raises(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    with detect_sql_repeats(executions=2, within_seconds=10.0, strict=True):
        await mod.testdb_sql(_COUNT_SQL).query_single_row()

        with pytest.raises(RepeatedQueryError, match="Repeated query"):
            await mod.testdb_sql(_COUNT_SQL).query_single_row()

    assert detector_warnings(caplog) == []


async def test_executions_older_than_the_window_are_forgotten(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    with detect_sql_repeats(executions=3, within_seconds=0.05):
        for _ in range(4):
            await mod.testdb_sql(_COUNT_SQL).query_single_row()
            await asyncio.sleep(0.06)

    assert detector_warnings(caplog) == []


async def test_nothing_is_counted_outside_the_block(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    with detect_sql_repeats(executions=2, within_seconds=10.0):
        await mod.testdb_sql(_COUNT_SQL).query_single_row()

    for _ in range(5):
        await mod.testdb_sql(_COUNT_SQL).query_single_row()

    assert detector_warnings(caplog) == []


async def test_counting_restarts_in_a_new_block(
    test_project: ProjectBuilder, caplog: pytest.LogCaptureFixture
) -> None:
    mod = generated_module(test_project)

    for _ in range(2):
        with detect_sql_repeats(executions=2, within_seconds=10.0):
            await mod.testdb_sql(_COUNT_SQL).query_single_row()

    assert detector_warnings(caplog) == []


def test_nested_detection_is_rejected() -> None:
    with (
        detect_sql_repeats(),
        pytest.raises(RuntimeError, match="already active"),
        detect_sql_repeats(),
    ):
        pass


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"executions": 1}, "executions must be at least 2"),
        ({"executions": 0}, "executions must be at least 2"),
        ({"within_seconds": 0.0}, "within_seconds must be positive"),
        ({"within_seconds": -1.0}, "within_seconds must be positive"),
    ],
)
def test_rejects_meaningless_configuration(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match), detect_sql_repeats(**kwargs):
        pass
