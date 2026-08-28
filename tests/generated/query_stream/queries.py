from tests.generated.query_stream.testdb import testdb_sql

testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
testdb_sql("SELECT id, username FROM users ORDER BY created_at")
testdb_sql("SELECT id FROM users ORDER BY created_at")
testdb_sql("UPDATE users SET is_active = $1 WHERE id = $2")
testdb_sql("SELECT count(*) as cnt FROM users WHERE is_active = false")
testdb_sql("SELECT count(*) as cnt FROM users")
testdb_sql("INSERT INTO users (id, username, is_active) VALUES ($1, $2, $3)")
testdb_sql("SELECT id, username FROM users WHERE is_active = $1 ORDER BY created_at")
