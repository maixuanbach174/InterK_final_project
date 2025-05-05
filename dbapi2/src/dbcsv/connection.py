import json
import time
from urllib.parse import urlparse
import requests
from requests.exceptions import ChunkedEncodingError, ReadTimeout, ConnectionError
from typing import Any, Iterator, List, Optional, Tuple

# PEP‑249 globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "named"

# Exceptions
class Error(Exception): ...
class DatabaseError(Error): ...
class ProgrammingError(Error): ...


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface
    rather than the database itself. (e.g., misuse of the DB-API, driver bugs)
    """

    pass


# Subclasses of DatabaseError


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range, etc.
    """

    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation
    and not necessarily under the programmer's control, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction
    could not be processed, a memory allocation error occurred during
    processing, etc.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails, duplicate key, etc.
    """

    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    This may indicate a bug in the database itself or the driver.
    """

    pass

class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is
    not supported by the database or driver, e.g. requesting a .rollback() on a
    connection that does not support transaction or has transactions turned off.
    """

    pass


class AuthenticationError(DatabaseError):
    """
    Exception raised when token validation error occurs, like the user is not authenticated,
    or the refresh endpoint is not online.
    """

    pass

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
        self.itersize: int = 1_000_000
        self._timeout = 10.0

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
                timeout=self._timeout,
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
        try:
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
        except (ChunkedEncodingError, ReadTimeout, ConnectionError):
            return
        finally:
            resp.close()
                
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


def validate_dsn_url(dsn: str) -> str:
    """
    Validate if dsn comply to format https://localhost:PORT/<schema_snake_case>.
    Returns schema_snake_case
    Throw InterfaceError if fails
    """
    parsed = urlparse(dsn)

    # if parsed.scheme not in ["http", "https"]:
    #     raise InterfaceError("DSN must be http or https")

    if parsed.port is not None and not (1 <= parsed.port <= 65535):
        raise InterfaceError("port number is not valid (not in range 1‑65535)")

    if (
        parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise InterfaceError("DSN must not contain query, fragment, user/password…")

    parts = dsn.rsplit("/", 1)
    url, schema = parts[0], parts[1]

    return schema, url

def login(url: str, username: str, password: str, db: str) -> str:
    data = {
        "username": username,
        "password": password,
        "db": db
    }

    try:
        r = requests.post(f"{url}/auth/connect", data=data, timeout=5)
        r.raise_for_status()  # 4xx/5xx → HTTPError
        return r.json()["access_token"]
    except requests.HTTPError as exc:
        err_json = exc.response.json()  # FastAPI return JSON: {"detail": "..."}
        detail = err_json.get("detail", exc.response.text)
        raise OperationalError(detail)
    except requests.RequestException as exc:
        # Internet error/timeout/etc.
        raise exc

def connect(
    dsn: str,
    user: Optional[str],
    password: Optional[str],
) -> Connection:
    """
    Initializes a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are database dependent.

    E.g. a connect could look like this: connect(dsn='https://localhost:1234/schema', user='guido', password='1234')
    """

    # Validate url correctness
    db, url = validate_dsn_url(dsn)

    # Request to /login endpoint of dsn to get JWT token. Catches exception if user doesn't exist in database
    token = login(url, user, password, db)

    # Create connection
    conn = Connection(url, token, db, 30)

    return conn


