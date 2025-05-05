"""
mydbapi – a PEP‑249‑compliant HTTP‑based DB‑API driver
"""

__version__ = "0.1.0"

# PEP‑249 module globals
apilevel    = "2.0"      # supported DB‑API level
threadsafety = 1         # connections may be shared, cursors not
paramstyle  = "named"    # we use JSON {"sql": "...", "db": "..."}  

# Convenience imports
from .connection import (connect, InterfaceError, DatabaseError, OperationalError, 
    ProgrammingError, NotSupportedError,
    AuthenticationError)

# What users get when they do `import mydbapi`:
__all__ = [
    "connect",
    "OperationalError",
    "NotSupportedError",
    "DatabaseError",
    "AuthenticationError",
    "ProgrammingError",
    "InterfaceError",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "__version__",
]
