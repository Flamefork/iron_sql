import asyncio
from collections.abc import AsyncIterator

import psycopg
import pytest

from iron_sql.runtime import ConnectionPool
from iron_sql.runtime import execute_listen
from iron_sql.runtime import execute_unlisten
from iron_sql.runtime import listen
from iron_sql.runtime import notify
from tests.conftest import ProjectBuilder

# =============================================================================
# Unit test: codegen
# =============================================================================


def test_codegen_listen_notify_helpers(test_project: ProjectBuilder) -> None:
    test_project.add_query("q", "SELECT 1")
    mod = test_project.generate()

    assert hasattr(mod, "testdb_listen_session")
    assert not hasattr(mod, "testdb_listen")
    assert hasattr(mod, "testdb_notify")


async def test_generated_listen_session_does_not_block_context_queries(
    test_project: ProjectBuilder,
) -> None:
    stmt = "SELECT 1"
    test_project.add_query("q", stmt)
    mod = test_project.generate()
    channel = "test_codegen_listen_session"

    async with (
        mod.testdb_connection(),
        mod.testdb_listen_session(channel) as payloads,
    ):
        await mod.testdb_notify(channel, "hello")
        async with asyncio.timeout(5):
            payload = await anext(payloads)
        assert payload == "hello"

        async with asyncio.timeout(2):
            row = await mod.testdb_sql(stmt).query_single_row()
        assert row == 1


# =============================================================================
# Integration tests: execute_listen / execute_unlisten
# =============================================================================


@pytest.fixture
async def pool(pg_dsn: str) -> AsyncIterator[ConnectionPool]:
    p = ConnectionPool(pg_dsn, name="listen_notify_test")
    yield p
    await p.close()


async def test_execute_listen_unlisten(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        await execute_listen(conn, "test_raw_channel")
        await notify(conn, "test_raw_channel", "raw_hello")

        async with asyncio.timeout(5):
            async for n in conn.notifies():
                assert n.channel == "test_raw_channel"
                assert n.payload == "raw_hello"
                break

        await execute_unlisten(conn, "test_raw_channel")


# =============================================================================
# Integration tests: runtime.listen() context manager
# =============================================================================


async def test_listen_context_manager(pool: ConnectionPool) -> None:
    async with pool.connection() as conn, listen(conn, "test_listen_cm") as payloads:
        await conn.execute("SELECT pg_notify('test_listen_cm', 'cm_hello')")
        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == "cm_hello"
                break


async def test_listen_break_stops_iteration(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as listen_conn,
        listen(listen_conn, "test_break") as payloads,
    ):
        async with pool.connection() as send_conn:
            for i in range(3):
                await notify(send_conn, "test_break", str(i))

        received = []
        async with asyncio.timeout(5):
            async for value in payloads:
                received.append(value)
                if value == "1":
                    break

        assert received == ["0", "1"]


async def test_listen_unlisten_on_exit(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        async with listen(conn, "test_cleanup") as payloads:
            await conn.execute("SELECT pg_notify('test_cleanup', 'inside')")
            async with asyncio.timeout(5):
                async for value in payloads:
                    assert value == "inside"
                    break

        await conn.execute("SELECT pg_notify('test_cleanup', 'after_exit')")
        notifications = [n async for n in conn.notifies(timeout=0.5)]
        assert notifications == []


async def test_listen_rejects_connection_with_active_listen_subscriptions(
    pool: ConnectionPool,
) -> None:
    channel = "test_existing_subscription"
    async with pool.connection() as listen_conn:
        await execute_listen(listen_conn, channel)

        with pytest.raises(
            RuntimeError,
            match=(
                "listen\\(\\) requires a connection without active LISTEN subscriptions"
            ),
        ):
            async with listen(listen_conn, channel):
                pass

        async with pool.connection() as send_conn:
            await notify(send_conn, channel, "still_subscribed")

        async with asyncio.timeout(5):
            async for n in listen_conn.notifies():
                assert n.channel == channel
                assert n.payload == "still_subscribed"
                break

        await execute_unlisten(listen_conn, channel)

    async with (
        pool.connection() as listen_conn,
        listen(listen_conn, "test_single_listener_guard") as payloads,
    ):
        with pytest.raises(
            RuntimeError,
            match=(
                "listen\\(\\) requires a connection without active LISTEN subscriptions"
            ),
        ):
            async with listen(listen_conn, "test_single_listener_guard_2"):
                pass

        async with pool.connection() as send_conn:
            await notify(send_conn, "test_single_listener_guard", "still_alive")

        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == "still_alive"
                break


async def test_listen_special_channel_name(pool: ConnectionPool) -> None:
    channel = "my-channel.with.dots"
    async with pool.connection() as conn, listen(conn, channel) as payloads:
        await notify(conn, channel, "hello")
        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == "hello"
                break


# =============================================================================
# Integration tests: notify
# =============================================================================


async def test_notify_json_str(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as conn,
        listen(conn, "test_json_channel") as payloads,
    ):
        await conn.execute(
            """SELECT pg_notify('test_json_channel', '{"id": 7, "name": "test"}')"""
        )
        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == '{"id": 7, "name": "test"}'
                break


async def test_notify_empty_channel_raises(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        with pytest.raises(ValueError, match="must not be empty"):
            await notify(conn, "", "payload")


# =============================================================================
# Integration tests: transactional notify
# =============================================================================


async def test_notify_inside_committed_transaction(pool: ConnectionPool) -> None:
    async with (
        pool.connection() as listen_conn,
        listen(listen_conn, "test_tx_commit") as payloads,
    ):
        async with pool.connection() as conn, conn.transaction():
            await notify(conn, "test_tx_commit", "42")

        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == "42"
                break


async def test_notify_inside_rolled_back_transaction(
    pool: ConnectionPool,
) -> None:
    async with (
        pool.connection() as listen_conn,
        listen(listen_conn, "test_tx_rollback") as payloads,
    ):
        try:
            async with pool.connection() as conn, conn.transaction():
                await notify(conn, "test_tx_rollback", "99")
                msg = "force rollback"
                raise RuntimeError(msg)  # noqa: TRY301
        except RuntimeError:
            pass

        async with pool.connection() as conn:
            await notify(conn, "test_tx_rollback", "0")

        async with asyncio.timeout(5):
            async for value in payloads:
                assert value == "0"
                break


# =============================================================================
# Validation tests
# =============================================================================


async def test_execute_listen_empty_channel_raises(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        with pytest.raises(ValueError, match="must not be empty"):
            await execute_listen(conn, "")


async def test_execute_unlisten_empty_channel_raises(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        with pytest.raises(ValueError, match="must not be empty"):
            await execute_unlisten(conn, "")


async def test_listen_empty_channel_raises(pool: ConnectionPool) -> None:
    async with pool.connection() as conn:
        with pytest.raises(ValueError, match="must not be empty"):
            async with listen(conn, ""):
                pass


# =============================================================================
# Error preservation tests
# =============================================================================


async def test_listen_context_preserves_body_error_on_dead_connection(
    pool: ConnectionPool, pg_dsn: str
) -> None:
    class AppError(Exception):
        pass

    async with pool.connection() as conn:
        with pytest.raises(AppError):  # noqa: PT012
            async with listen(conn, "test_preserve_err") as _payloads:
                pid = conn.pgconn.backend_pid
                async with await psycopg.AsyncConnection.connect(
                    pg_dsn, autocommit=True
                ) as killer:
                    await killer.execute("SELECT pg_terminate_backend(%s)", (pid,))
                await asyncio.sleep(0.2)
                raise AppError
