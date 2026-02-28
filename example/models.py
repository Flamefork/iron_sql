from pydantic import BaseModel
from pydantic import Field


class ProjectSettings(BaseModel):
    default_priority: str = "medium"
    enable_notifications: bool = True


class TaskMetadata(BaseModel):
    tags: list[str] = Field(default_factory=list)
    estimated_hours: float | None = None
