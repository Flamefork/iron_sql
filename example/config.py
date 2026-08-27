import os

from iron_sql import PoolOptions

DSN = os.environ.get("DATABASE_URL", "")
POOL_OPTIONS: PoolOptions = {
    "min_size": 1,
    "max_size": 10,
    "timeout": 15.0,
    "kwargs": {"options": "-c statement_timeout=5000 -c lock_timeout=1000"},
}
