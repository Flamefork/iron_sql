from pydantic import BaseModel


class UserMetadata(BaseModel):
    key: str
    value: str


class Tag(BaseModel):
    name: str
    color: str


TagList = list[Tag]
