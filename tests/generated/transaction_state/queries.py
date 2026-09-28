from tests.generated.transaction_state.testdb import testdb_sql

testdb_sql("INSERT INTO users (id, username) VALUES ($1, 'streamed')")
testdb_sql("SELECT id FROM users")
