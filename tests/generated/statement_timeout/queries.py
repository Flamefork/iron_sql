from tests.generated.statement_timeout.testdb import testdb_sql

testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
testdb_sql("SELECT id FROM users ORDER BY username")
testdb_sql("SELECT 1 AS n FROM pg_sleep(1)")
