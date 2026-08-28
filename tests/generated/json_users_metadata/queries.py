from tests.generated.json_users_metadata.testdb import testdb_sql

testdb_sql("INSERT INTO users (id, username, metadata) VALUES ($1, $2, $3)")
testdb_sql("SELECT * FROM users WHERE id = $1")
testdb_sql("SELECT metadata FROM users WHERE id = $1")
testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
