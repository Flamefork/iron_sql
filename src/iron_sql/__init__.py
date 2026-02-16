"""iron_sql: Typed SQL client generator for Python."""

from iron_sql.runtime import NoRowsError
from iron_sql.runtime import TooManyRowsError

__all__ = [
    "NoRowsError",
    "TooManyRowsError",
]
