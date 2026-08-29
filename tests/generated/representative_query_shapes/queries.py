from collections.abc import Sequence
from typing import assert_type

from tests.generated.representative_query_shapes import testdb as api
from tests.generated.representative_query_shapes.testdb import testdb_sql


async def check(
    group_id: int,
    ids: Sequence[int],
    ranks: Sequence[int],
    section_ids: Sequence[int] | None,
    stream_id: int,
) -> None:
    ordered_item = await testdb_sql(
        """WITH item_tags AS (
            SELECT DISTINCT ON (ordered_items.id)
                ordered_items.id,
                array_agg(expanded_tags.tag ORDER BY expanded_tags.tag)
                    FILTER (WHERE expanded_tags.tag IS NOT NULL) AS tags
            FROM ordered_items
            LEFT JOIN LATERAL
                jsonb_array_elements_text(ordered_items.tags)
                    AS expanded_tags(tag)
                ON TRUE
            WHERE ordered_items.group_id = @group_id
            GROUP BY ordered_items.id, ordered_items.order_index
            ORDER BY ordered_items.id, ordered_items.order_index
        )
        SELECT
            ordered_items.id,
            item_tags.tags
        FROM ordered_items
        LEFT JOIN item_tags ON item_tags.id = ordered_items.id
        WHERE ordered_items.group_id = @group_id
        ORDER BY ordered_items.order_index"""
    ).query_single_row(group_id=group_id)
    assert_type(ordered_item.id, int)
    assert_type(ordered_item.tags, Sequence[str] | None)

    resources = await testdb_sql(
        """WITH requested_resources AS (
            SELECT unnest(@ids::int[]) AS id
        )
        SELECT
            resources.id,
            owners.name AS owner_name
        FROM requested_resources
        JOIN resources ON resources.id = requested_resources.id
        JOIN owners ON owners.id = resources.owner_id
        WHERE resources.rank = ANY(@ranks::int[])
        ORDER BY resources.id""",
        row_type="ResourceSummary",
    ).query_all_rows(ids=ids, ranks=ranks)
    assert_type(resources, list[api.ResourceSummary])

    section_count = await testdb_sql(
        """SELECT
            memberships.section_id,
            count(*) AS member_count
        FROM members
        JOIN memberships ON memberships.member_id = members.id
        JOIN sections ON sections.id = memberships.section_id
        WHERE memberships.section_id = ANY(@section_ids?::int[])
        GROUP BY memberships.section_id"""
    ).query_single_row(section_ids=section_ids)
    assert_type(section_count.section_id, int | None)
    assert_type(section_count.member_count, int)

    payloads = await testdb_sql(
        """SELECT payload
        FROM event_log
        WHERE stream_id = @stream_id
        ORDER BY id"""
    ).query_all_rows(stream_id=stream_id)
    assert_type(payloads, list[bytes])
