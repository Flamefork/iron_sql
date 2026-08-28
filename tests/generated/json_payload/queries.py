from tests.generated.json_payload.testdb import testdb_sql

testdb_sql("""INSERT INTO json_payloads (payload) VALUES ($1)
RETURNING id, payload""")
