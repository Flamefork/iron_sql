from tests.generated.json_text.testdb import testdb_sql

testdb_sql("INSERT INTO text_json (data) VALUES ($1) RETURNING id, data")
