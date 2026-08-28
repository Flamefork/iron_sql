from tests.generated.json_tag_list.testdb import testdb_sql

testdb_sql("INSERT INTO tagged_items (tags) VALUES ($1) RETURNING id, tags")
