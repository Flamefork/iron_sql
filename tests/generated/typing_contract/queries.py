from tests.generated.typing_contract.testdb import testdb_sql

testdb_sql("INSERT INTO users (id, username) VALUES ($1, $2)")
testdb_sql("SELECT id FROM users ORDER BY created_at")
testdb_sql("SELECT id, username FROM users WHERE id = $1")
testdb_sql("SELECT 'active'::user_status as status")
testdb_sql("SELECT email FROM users WHERE id = $1")
