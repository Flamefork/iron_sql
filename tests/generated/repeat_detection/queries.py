from tests.generated.repeat_detection.testdb import testdb_sql

testdb_sql("SELECT count(*) FROM users")
testdb_sql("SELECT id FROM users ORDER BY username")
testdb_sql("""SELECT
    users.id
FROM users""")
testdb_sql("""SELECT
    users.username
FROM users""")
testdb_sql("""SELECT users.username AS name_0,
    users.username AS name_1,
    users.username AS name_2,
    users.username AS name_3,
    users.username AS name_4
FROM users""")
