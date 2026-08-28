from iron_sql import PoolOptions

DSN = ""
POOL_OPTIONS: PoolOptions = {"kwargs": {"options": "-c statement_timeout=200"}}
