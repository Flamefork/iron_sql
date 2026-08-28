from tests.generated.query_parameters.testdb import testdb_sql

testdb_sql("""INSERT INTO users (id, username, is_active)
VALUES (@id, @username, @active)""")
testdb_sql("SELECT id FROM users WHERE id = $1 AND username = @username")
testdb_sql("SELECT count(*) FROM users WHERE username = @u?")
testdb_sql("SELECT count(*) FROM users WHERE id = $1 OR id = $2")
