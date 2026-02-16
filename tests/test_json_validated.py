from dataclasses import dataclass

from iron_sql.runtime import json_validated
from tests.json_models import UserMetadata


def test_json_validated_applies_validation() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata
        other: int

    row = Row(metadata='{"key": "lang", "value": "en"}', other=42)  # type: ignore[reportArgumentType]
    assert isinstance(row.metadata, UserMetadata)
    assert row.metadata.key == "lang"
    assert row.other == 42


def test_json_validated_chains_existing_post_init() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata
        extra: str = ""

        def __post_init__(self) -> None:
            self.extra = "post_init_ran"

    row = Row(metadata={"key": "k", "value": "v"}, extra="ignored")  # type: ignore[reportArgumentType]
    assert isinstance(row.metadata, UserMetadata)
    assert row.extra == "post_init_ran"


def test_json_validated_skips_none() -> None:
    @dataclass(kw_only=True)
    @json_validated(metadata=UserMetadata)
    class Row:
        metadata: UserMetadata | None

    row = Row(metadata=None)
    assert row.metadata is None
