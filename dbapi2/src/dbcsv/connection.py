import json
import time
import requests
from typing import Any, Iterator, List, Optional, Tuple

# PEP‑249 globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "named"

# Exceptions
class Error(Exception): ...
class DatabaseError(Error): ...
class ProgrammingError(Error): ...


def connect(
    base_url: str,
    username: str,
    password: str,
    db:   str,
    timeout:  float = 5.0,
) -> "Connection":
    """
    DB‑API connect: authenticate against /auth/connect to get token + schema.
    """
    base = base_url.rstrip("/")
    resp = requests.post(
        f"{base}/auth/connect",
        data={"username": username, "password": password, "db": db},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise DatabaseError(f"Connect failed: {resp.status_code} {resp.text}")
    tok = resp.json()
    return Connection(base, tok["access_token"], db, timeout)


class Connection:
    def __init__(self, base: str, access_token: str, db: str,timeout: float):
        self._base = base
        self._access_token = access_token
        self._timeout = timeout
        self._db = db

    def cursor(self) -> "Cursor":
        return Cursor(self)
    
    def close(self):
        # No persistent connection to close
        pass


class Cursor:
    # … __init__ unchanged …
    def __init__(self, conn: Connection):
        self._conn = conn
        self._row_gen: Optional[Iterator[Tuple[Any,...]]] = None
        self.description = None
        self.rowcount: int = -1
        self.itersize: int = 5000

    def execute(self, operation: str, parameters: Any = None):
        """
        Send SQL to /query/sql/, using the schema from self._conn.
        Raises:
          ProgrammingError for any 4xx (validation) failure,
          DatabaseError    for any 5xx or network failure.
        """
        url = f"{self._conn._base}/query/sql/"
        payload = {"sql": operation, "db": self._conn._db}
        headers = {"Authorization": f"Bearer {self._conn._access_token}"}

        try:
            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=self._conn._timeout,
                stream=True,
            )
        except requests.RequestException as e:
            # network or timeout
            raise DatabaseError(f"Network error during query: {e}") from e

        # handle HTTP errors
        if 400 <= resp.status_code < 500:
            # client/validation error
            detail = resp.json().get("detail", resp.text)
            raise ProgrammingError(f"Query failed ({resp.status_code}): {detail}")
        if resp.status_code >= 500:
            detail = resp.json().get("detail", resp.text) if resp.headers.get("content-type","").startswith("application/json") else resp.text
            raise DatabaseError(f"Server error ({resp.status_code}): {detail}")

        # success → set up row generator
        self._row_gen = self._make_row_generator(resp)
        self.rowcount = -1
        return self

    def _make_row_generator(self, resp: requests.Response) -> Iterator[Tuple[Any,...]]:
        """
        NDJSON or JSON‑batch stream.  Any JSON errors become ProgrammingError.
        """
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise ProgrammingError(f"Invalid JSON in response: {e}") from e

            if not isinstance(data, list):
                raise ProgrammingError(f"Unexpected payload: {data!r}")

            # batch or single-row?
            if data and isinstance(data[0], list):
                for row in data:
                    yield tuple(row)
            else:
                yield tuple(data)
                
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        try:
            row = next(self._row_gen)  # type: ignore
        except StopIteration:
            return None
        # track rowcount if desired
        self.rowcount = (self.rowcount or 0) + 1
        return row

    def fetchmany(self, size: int = 100) -> List[Tuple[Any, ...]]:
        rows = []
        for _ in range(size):
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self.rowcount = (self.rowcount or 0) + self.itersize
        return self.fetchmany(self.itersize)
    
    def __iter__(self):
        return self

    def __next__(self):
        # pull one row at a time, but under‑the‑hood we batch
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        self._row_gen = None