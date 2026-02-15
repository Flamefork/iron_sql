from pydantic import BaseModel


class ProjectSettings(BaseModel):
    default_priority: str = "medium"
    enable_notifications: bool = True


class TaskMetadata(BaseModel):
    tags: list[str] = []
    estimated_hours: float | None = None
