import datetime
from typing import Callable

# 1A) write each converter as a simple function
def to_string(s: str) -> str:
    return s

def to_int(s: str) -> int:
    try: return int(s)
    except ValueError: raise ValueError(f"Invalid integer: {s}")

def to_float(s: str) -> float:
    try: return float(s)
    except ValueError: raise ValueError(f"Invalid float: {s}")

def to_bool(s: str) -> bool:
    if s == "TRUE": return True
    if s == "FALSE": return False
    raise ValueError(f"Invalid boolean: {s}")

def to_date(s: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date (YYYY‑MM‑DD): {s}")

def to_null(s: str):
    if s == "null": return None
    raise ValueError(f"Invalid null: {s}")


# 1B) map every alias to the right function
_TYPE_CONVERTERS: dict[str, Callable[[str], any]] = {}

# helper to register multiple names
def register(fn, *aliases):
    for a in aliases:
        _TYPE_CONVERTERS[a.upper()] = fn

register(to_string,  "VARCHAR", "TEXT", "CHAR")
register(to_int,     "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT")
register(to_float,   "FLOAT", "DOUBLE", "DECIMAL", "DEC")
register(to_bool,    "BOOLEAN", "BOOL")
register(to_date,    "DATE", "DATETIME", "TIMESTAMP")
register(to_null,    "NULL")

def convert_datatype(data: str, dtype: str) -> any:
    try:
        converter = _TYPE_CONVERTERS[dtype.upper()]
    except KeyError:
        raise ValueError(f"Unknown type {dtype!r}")
    return converter(data)

def prepare_converters(column_types: list[str]) -> list[Callable[[str], any]]:
    converters: list[Callable[[str], any]] = []
    for idx, dtype in enumerate(column_types):
        key = dtype.upper()
        if key not in _TYPE_CONVERTERS:
            raise ValueError(f"Unknown column type at index {idx}: {dtype!r}")
        converters.append(_TYPE_CONVERTERS[key])
    return converters
