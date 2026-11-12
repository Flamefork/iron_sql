import uuid

from pydantic import BaseModel


class UserMetadata(BaseModel):
    key: str
    value: str


class Tag(BaseModel):
    name: str
    color: str


TagList = list[Tag]


class Post(BaseModel):
    id: int
    title: str


PostList = list[Post]

type PostSequence = list[Post]


class UserSummary(BaseModel):
    id: uuid.UUID
    username: str


Labels = dict[str, str]
