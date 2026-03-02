import asyncio
import uuid
from datetime import date

from example.db.mydb import MydbTaskPriority
from example.db.mydb import MydbTaskStatus
from example.db.mydb import mydb_listen_session
from example.db.mydb import mydb_notify
from example.db.mydb import mydb_sql
from example.db.mydb import mydb_transaction
from example.models import ProjectSettings
from example.models import TaskMetadata
from iron_sql import NoRowsError


async def main() -> None:
    user_id = uuid.uuid4()
    project_id = uuid.uuid4()

    # --- Transaction: create user + project atomically ---
    async with mydb_transaction():
        await mydb_sql(
            """
            INSERT INTO users (id, username, email)
            VALUES (@id, @username, @email)
            """
        ).execute(
            id=user_id,
            username="alice",
            email="alice@example.com",
        )
        await mydb_sql(
            """
            INSERT INTO projects (id, name, owner_id, settings)
            VALUES (@id, @name, @owner_id, @settings)
            """
        ).execute(
            id=project_id,
            name="iron_sql",
            owner_id=user_id,
            settings=ProjectSettings(
                default_priority="high",
                enable_notifications=False,
            ),
        )

    # --- Create tasks with enums, optional params, JSON metadata ---
    task1_id = uuid.uuid4()
    await mydb_sql(
        """
        INSERT INTO tasks (id, project_id, title, priority, assignee_id, metadata, due_date)
        VALUES (@id, @project_id, @title, @priority, @assignee_id?, @metadata?, @due_date?)
        """
    ).execute(
        id=task1_id,
        project_id=project_id,
        title="Set up CI",
        priority=MydbTaskPriority.HIGH,
        assignee_id=user_id,
        metadata=TaskMetadata(tags=["infra", "ci"], estimated_hours=4.0),
        due_date=date(2026, 3, 1),
    )

    await mydb_sql(
        """
        INSERT INTO tasks (id, project_id, title, priority, assignee_id, metadata, due_date)
        VALUES (@id, @project_id, @title, @priority, @assignee_id?, @metadata?, @due_date?)
        """
    ).execute(
        id=uuid.uuid4(),
        project_id=project_id,
        title="Write README",
        priority=MydbTaskPriority.MEDIUM,
        assignee_id=None,
        metadata=None,
        due_date=None,
    )

    # --- Update task status with enum ---
    await mydb_sql("UPDATE tasks SET status = @status WHERE id = @task_id").execute(
        task_id=task1_id,
        status=MydbTaskStatus.IN_PROGRESS,
    )

    # --- List all users ---
    users = await mydb_sql(
        "SELECT id, username, email, created_at FROM users ORDER BY created_at"
    ).query_all_rows()
    print(f"Users: {len(users)}")

    # --- Get single user ---
    user = await mydb_sql(
        "SELECT id, username, email, created_at FROM users WHERE id = @user_id"
    ).query_single_row(user_id=user_id)
    print(f"User: {user.username} ({user.email})")

    # --- List tasks with optional status filter ---
    all_tasks = await mydb_sql(
        """
        SELECT id, project_id, assignee_id, title, status, priority, metadata, due_date, created_at
        FROM tasks
        WHERE project_id = @project_id AND (sqlc.narg('status')::task_status IS NULL OR status = @status?)
        """
    ).query_all_rows(
        project_id=project_id,
        status=None,
    )
    print(f"All tasks: {len(all_tasks)}")

    open_tasks = await mydb_sql(
        """
        SELECT id, project_id, assignee_id, title, status, priority, metadata, due_date, created_at
        FROM tasks
        WHERE project_id = @project_id AND (sqlc.narg('status')::task_status IS NULL OR status = @status?)
        """
    ).query_all_rows(
        project_id=project_id,
        status=MydbTaskStatus.OPEN,
    )
    print(f"Open tasks: {len(open_tasks)}")

    # --- Custom row_type: count by status ---
    counts = await mydb_sql(
        """
        SELECT status, count(*) AS task_count
        FROM tasks WHERE project_id = @project_id
        GROUP BY status ORDER BY status
        """,
        row_type="TaskStatusCount",
    ).query_all_rows(project_id=project_id)
    for row in counts:
        print(f"  {row.status}: {row.task_count}")

    # --- Scalar query: get task ID by title ---
    found_id = await mydb_sql(
        "SELECT id FROM tasks WHERE project_id = @project_id AND title = @title"
    ).query_single_row(
        project_id=project_id,
        title="Set up CI",
    )
    print(f"Found task ID: {found_id}")

    # --- Error handling: NoRowsError ---
    try:
        await mydb_sql(
            "SELECT id, username, email, created_at FROM users WHERE id = @user_id"
        ).query_single_row(user_id=uuid.uuid4())
    except NoRowsError:
        print("User not found (expected)")

    # --- LISTEN/NOTIFY ---
    async with mydb_listen_session("task_updates") as task_ids:
        await mydb_notify("task_updates", str(task1_id))

        async with asyncio.timeout(5):
            async for task_id in task_ids:
                print(f"Received: {task_id}")
                break


asyncio.run(main())
