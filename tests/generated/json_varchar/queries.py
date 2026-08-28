from tests.generated.json_varchar.testdb import testdb_sql

testdb_sql("INSERT INTO varchar_json (data) VALUES ($1) RETURNING id, data")
