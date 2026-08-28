from tests.generated.json_priority.testdb import testdb_sql

testdb_sql("SELECT * FROM users WHERE id = $1")
testdb_sql("INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)")
